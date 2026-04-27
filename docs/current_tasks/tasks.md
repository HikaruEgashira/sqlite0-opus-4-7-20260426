# Current Tasks

最新の実装計画。完了したタスクは `docs/memory/` または ADR に統合してから削除する (CLAUDE.md "Content Lifecycle Rules" 参照)。

## Active Phase: Phase 3 — Pager + SQLite3 file-format 互換 storage

ADR-0004 で Phase 順序を改訂 (Pager を VDBE より先行)。ADR-0005 が Phase 3 の境界を定義する。

直近の実装スライス (Iter24): Pager 着手前に **Cursor 抽象** を導入し、AST evaluator を `Database.tables.get().rows` 直参照から Cursor 経由に切替える (ADR-0004 §3)。これにより Iter25 で Pager を導入したとき、evaluator 側の修正範囲が cursor 実装の追加だけで済む。

各 Iter ごとに `tests/differential/cases.txt` を増やし、`bash tests/differential/run.sh` を緑にすること。

### Phase 3a: Cursor 抽象の導入 (Pager 着手前の refactor)

- [x] Iter24.A: `cursor.zig` 新設、`Cursor` + `VTable` + in-memory `TableCursor` を実装。`engine_from.resolveSource` の `.table_ref` 経路を `TableCursor` + `materializeRows` 経由に切替。差分テスト 787/787 緑を維持。Phase 3b で `BtreeCursor` を追加するとき call site は無修正。
- ~~Iter24.B/C~~: 削除。理由: DML の write-side cursor API (`delete_current` / `update_column_at_current`) は Pager B-tree mechanics に dictate されるため Phase 3b/c で再起票する (Iter26.A 参照)。correlated subquery の outer-frame 所有関係も page-eviction model 確定後でないと設計できない (Iter25.B に absorb)。

### Phase 3b: Pager + read-only B-tree (SQLite3 .db 読み込み)

- [x] Iter25.A: `pager.zig` 新設。`Pager.open(allocator, file_path)` / `getPage(n)` / `close()`。PAGE_SIZE=4096, LRU 16 page (test では `cache_capacity` 直接書き換え可)。`std.c.open/pread/flock` 直接使用 (std.Io threading は別 ADR で再評価)。Errors: `DatabaseLocked` / `IoError` を `ops.Error` に追加。7 unit tests 緑 (open/cache hit/LRU evict/promote/lock contention/page 0 reject/missing file).
- Iter25.B (sub-iterations): SQLite3 .db read pipeline. Advisor split:
  - [x] Iter25.B.1: `record.zig` — `decodeVarint` / `serialTypeBodyLen` / `decodeColumn` / `decodeRecord`。Pure logic, no Pager 依存。Hand-constructed test 23本 (varint 1/2/9-byte, integer 1/2/3/4/6/8 byte sign-extend, REAL IEEE 754 BE, TEXT/BLOB borrow source, multi-column record, malformed → IoError).
  - [x] Iter25.B.2: `btree.zig` 新設、leaf-table page (type 0x0d) parser。`parsePageHeader(page, header_offset)` + `parseLeafTablePage(alloc, page, header_offset, usable_size) → []LeafTableCell{rowid,record_bytes}`。`header_offset` は page 1 (= 100) と他 page (= 0) を区別。overflow record (payload > usable_size−35) は `Error.IoError` で fail loud。Pager 依存なし (caller が `getPage()` を渡す)。9 unit tests 緑 (leaf header / interior right_child / invalid type / short buffer / corrupt content_start / empty cells / 3 cells in rowid order / page 1 offset 100 / overflow rejection / OOB cell ptr / non-leaf rejection)。
  - [x] Iter25.B.3: `btree.zig` に `parseInteriorTablePage` 追加 (interior table 0x05、cells = (left_child u32 BE, key varint))。`pageHeaderOffset(page_no)` で page 1 = 100 を統一処理。`btree_walk.zig` 新設 (`TableLeafWalker.init/next/deinit`) で Pager + btree end-to-end traversal。interior cells を stack frame に copy して LRU eviction 安全。`btree_test_util.zig` に `buildLeafTablePage` / `buildInteriorTablePage` を抽出 (btree.zig を 500行未満に保ちつつ btree_walk テストでも再利用)。8 unit tests (interior parse / leaf rejection / pageHeaderOffset / root-is-leaf with offset 100 / interior root → 3 leaves / nested 2-level → 4 leaves / index-page rejection)。Pager end-to-end が初めて exercise される地点。差分 787/787 緑。
  - [x] Iter25.B.4: `btree_cursor.zig` 新設 (`BtreeCursor.open(arena, *Pager, root_page, column_names) → Cursor`)。`TableLeafWalker` を wrap し、leaf cell ごとに `record.decodeRecord` → TEXT/BLOB を arena に dupe して unified Value lifetime contract を満たす。これが OuterFrame.current_row 問題の解。`cursor.zig` top doc を「Unified Value lifetime contract」セクションで再定義。column index ≥ decoded.len は `Value.null` を返す (ALTER TABLE ADD COLUMN 由来の short record 対応)。`rewind()` は walker を tear-down→re-init。6 unit tests (empty B-tree / 3-row walk / **TEXT survives page eviction (cache_capacity=1, 2 leaves)** / rewind / short-record→NULL trailing / column-before-rewind→SyntaxError)。差分 787/787 緑。
  - [x] Iter25.B.5 + C (merged per advisor): `schema.zig` 新設で `sqlite_schema` (page 1) を `TableLeafWalker` 経由で scan、`record.decodeRecord` で 5列を decode、`stmt_ddl.parseCreateTableStatement` で sql 列を再解析、`type='table'` のみ `Database.registerTable` してから `t.root_page = rootpage` を patch。`Database.openFile(allocator, path)` 追加 (Pager 所有を Database に移管、`deinit` で flock 解放)。`Table.root_page: u32 = 0` (in-memory sentinel) を追加し `engine_from.resolveSource` の `.table_ref` で fork (`!= 0` なら `BtreeCursor`、それ以外は `TableCursor`)。`main.zig` に `-file <path>` flag。`tests/differential/run_file.sh` + `file_cases.txt` 新設、`sqlite3 fix.db "..."` で fixture 生成 → 両方の engine が同じ stdout を出すこと。**12/12 file-differential cases 緑** (single-page select / WHERE / ORDER BY / COUNT / 算術 / 500行多page B-tree count / 多page WHERE / 2-table JOIN / GROUP BY SUM / NULL / REAL IEEE754 BE / BLOB)。差分 787/787 緑、unit tests 全緑。

### Phase 3c: 書き込み path

advisor split (Iter26.A は scope が広すぎたため細分化):

- [x] Iter26.guard: file-mode DB に対する DDL/DML を `Error.ReadOnlyDatabase` で reject。Iter25.B.5+C 完了直後の silent in-memory shadow state を防ぐ (registerTable / executeInsert / executeDelete / executeUpdate を `assertWritable(db)` で gate)。SELECT は引き続き許可。Unit test: file-mode DB が 4 種の write すべて拒否し SELECT は通る。
- [x] Iter26.A.0: `Pager.writePage(page_no, bytes)` 単独 primitive。`std.c.pwrite` + cache write-through (LRU 内なら in-place memcpy + head 昇格、それ以外は head に挿入)。**fsync 無し** — Phase 3c に transaction semantics が無く、durability story は Phase 4 (WAL/ADR-0007) が一括で扱うべき。3 unit tests (close→reopen round-trip / page 0 + wrong-len rejection / cache write-through pointer identity)。差分 787/787 + file-differential 12/12 緑。
- [x] Iter26.A.1: insert-into-leaf-with-room (split 無し)。3 新規モジュール:
  - `record_encode.zig`: `serialTypeForValue` (literal 8/9 含む)、`encodeColumnBody`、`encodeRecord` ([]Value → bytes)。decode/encode round-trip for null/int(全 7 型)/real/text/blob 全部 unit test 緑。
  - `btree_insert.zig`: `insertLeafTableCell(page, header_offset, usable_size, rowid, record_bytes) → ok | page_full`。pure mutation、cell pointer 配列を rowid 順に shift (`std.mem.copyBackwards`)、content area を後ろから縮める、header の cell_count + cell_content_area を更新。duplicate rowid → IoError。overflow > usable-35 → IoError。6 unit tests (empty / out-of-order / append / duplicate / page_full / page 1 offset 100)。
  - `engine_dml.executeInsert` 内に `executeInsertFile` を追加。`t.root_page != 0` で dispatch、ローカル ArenaAllocator で work buffer + encoded record を管理 (db.execute は per-statement arena が無いため leak 防止に必須)。current max rowid を leaf scan で求めて auto-assign、`Pager.writePage` で 1 回コミット。multi-page B-tree / page split は Error.UnsupportedFeature。
  - harness 拡張: `run_file.sh` に MUTATE+VERIFY ペアを追加 (sqlite3 で fixture を 2 部複製→sqlite0 と sqlite3 でそれぞれ MUTATE→sqlite3 で VERIFY を 2 部 → diff、加えて sqlite0 側の copy に `PRAGMA integrity_check` を実行)。
  - 結果: file-differential 17/17 緑 (12 既存 read + 5 INSERT MUTATE/VERIFY)。`PRAGMA integrity_check` 全部 ok = sqlite0 が書いたバイトが sqlite3 から見て構文的に正しい。差分 787/787 緑、unit tests 全緑。
- [x] Iter26.A.2: DELETE / UPDATE for file-mode tables via "rebuild leaf page from scratch" primitive (sidesteps freeblock chain bookkeeping). `btree_insert.rebuildLeafTablePage` (single-leaf root only) + `engine_dml_file.zig` extracted to keep `engine_dml.zig` under 500 lines. `Pager.usableSize()` derives the real usable area from file-header byte 20 (sqlite3 default = 12 reserved bytes) — INSERT path retroactively fixed to honor it too. Survivor record bytes are duped out of the working page before rebuild (write-while-aliasing bug surfaced by `DELETE WHERE` test). UPDATE evaluates all RHS into a scratch array first, then splices — mirrors in-memory shape so `UPDATE t SET a = a+10, b = a` evaluates b against the OLD a (advisor caught this; discriminating test added). 27/27 file-differential cases green (including 4 DELETE + 6 UPDATE + integrity_check on every sqlite0-mutated copy). 5 new btree_insert unit tests (round-trip, empty, gap-zeroed, page-1 offset, overflow rejection). Differential 787/787 + unit tests all green.
- [x] Iter26.A.3: CREATE TABLE 永続化。3 つの primitive を組み合わせて pure sqlite0 で fixture 生成 → cold reopen で読み出し可能に。
  - `Pager.allocatePage()`: 末尾に zero-page 1枚を sparse extend (`pwrite` past EOF) し、page 1 header offset 28..31 の dbsize を u32 BE で +1 して新 page_no を返す。dbsize=0 (malformed) は `Error.IoError`。
  - `schema.appendSchemaRow(p, type, name, tbl_name, rootpage, sql)`: 5列レコードを `record_encode.encodeRecord` し、page 1 (header_offset=100) に `btree_insert.insertLeafTableCell` で挿入 → `Pager.writePage(1, ...)` で commit。single-page sqlite_schema 専用 (page split は Iter26.B 配下で対応)。
  - `engine_ddl_file.executeCreateTableFile(db, parsed)`: allocatePage → `rebuildLeafTablePage(empty)` で leaf header 初期化 → writePage → appendSchemaRow → `db.registerTable` 後に `t.root_page` を patch。`stmt_ddl.ParsedCreateTable.source_text` を新設し、parser で `p.src[stmt_start..stmt_end]` を保存して sqlite_schema の sql 列に書き戻す。
  - module split: pager.zig が allocatePage 追加で 567 行に膨らんだため、`pager_write_test.zig` (writePage + allocatePage tests) と `test_db_util.zig` (4 モジュールに散らばっていた `makeTempPath` / `unlinkPath` / `writeFixture` / `writePages` を統合) を抽出し全 `*.zig` を 500 行未満に維持。
  - 4 新規 file-differential cases: CREATE+sqlite_schema 確認 / CREATE+INSERT+SELECT / 型保持 / multi-CREATE batch。31/31 file-differential 緑。cold reopen round-trip (`sqlite0 CREATE → close → sqlite0 SELECT` で `1|hi`) を手動でも検証。差分 787/787 + unit tests 全緑。
- Iter26.B (sub-iterations): page split。advisor split (B 単発は scope が広すぎる):
  - [x] Iter26.B.1: balance-deeper。leaf root が full → root_page を stable に保ったまま 2 leaf child に cells を分配、root を interior に書き換え (single divider cell + right_child)。新規 module `btree_split.zig` (3 primitive: `splitLeafCells` midpoint split / `classifyForLeaf` fits/needs_split/oversize_record の 3-way 分類 / `balanceDeeperRoot` IO-含めた orchestration + `writeInteriorTablePage` interior page encoder)。`engine_dml_file.executeInsertFile` を「既存 cells + 新 cells を merge してから classify」shape に書き換え、`.needs_split` 経路を `balanceDeeperRoot` にルーティング。crash 順序は子 leaf → 新 interior root の順 (旧 leaf 内容が最後まで残る最も無害な失敗形)。Page 1 root (sqlite_schema 巨大化) は dbsize race を避けるため `Error.UnsupportedFeature` で reject。harness 拡張: `run_file.sh` 全 MUTATE case で sqlite0 round-trip verify を追加 (SKIP_RT で sqlite_schema 直接読みの 3 case のみ opt-out) — 既存 27 MUTATE case にも regression 網が張られた。3 新規 file-differential cases (100 行 single-INSERT で count(*)=100 / 両 leaf 跨ぐ rowid 検索 / 両 leaf の record bytes 復元) + 6 新 unit tests (split 中点 / 0/1 cell reject / fits 判定 / oversize record / needs_split 判定 / writeInterior round-trip / empty cells)。File-differential 34/34 + differential 787/787 + unit 全緑。
  - 既知の宿題 (B.2 スコープ): ~~midpoint split → byte-cumulative split に置換~~ → Iter26.B.2.c で対応。
  - [x] Iter26.B.2: non-root leaf split。interior root (B.1 後の depth-1 tree) で rightmost leaf が overflow → L_new + R_new を allocate、cells を midpoint 分割、親 root に新 InteriorCell を追加して right_child を R_new に差し替え、OLD right child を `Pager.freePage` で freelist 入り。新規 primitive:
    - `Pager.freePage(page_no)`: file header bytes [32..36] (trunk page no) + [36..40] (count) を更新。空 freelist (`cur_trunk == 0`) は page を新 trunk に zero-fill 昇格、既存 trunk があれば leaf array 末尾に追加。trunk leaf array が満杯 (~1019 leaves on 4084 usable) は B.3 scope のため `Error.UnsupportedFeature`。write 順序は trunk first / page 1 last (B.1 と同じ parent-last invariant)。3 unit tests (empty freelist promote / append-to-existing-trunk / page 0/1 reject)。
    - `btree_split.classifyForInterior(cells, header_offset, usable_size)`: 12-byte interior header + 4-byte left_child + varint(key) per cell の総和で `fits` / `needs_split` を返す pure predicate。`splitRightmostLeaf` の親 fit 事前検査で使用 — needs_split = recursive interior split (B.3) なので fail-fast。2 unit tests (small fits / 1000 cells needs_split)。
    - `btree_split.splitRightmostLeaf(pager, root_page_no, old_right_child, parent_cells, all_combined)`: orchestration。allocatePage × 2 → classifyForInterior pre-check → child 2 枚を rebuildLeafTablePage で書き → 親 root を新 interior content で `writeInteriorTablePage` → freePage(OLD) の順。crash window は B.1 と同 grade (parent-last)。
    - `engine_dml_file.executeInsertFile` を `insertIntoLeafRoot` / `insertIntoInteriorRoot` に dispatch (root header の page_type で fork)。interior path は parent → rightmost leaf を walk、combined cells を classifyForLeaf し `.fits` なら leaf rebuild、`.needs_split` なら splitRightmostLeaf、`.oversize` は B.3。
    - 4 新規 file-differential cases (100+50 で count=150 / 3 leaf 跨ぐ rowid 検索 / 跨 split record 復元 / .fits 経路 symmetry: 100 行 sqlite3-prebalanced fixture + 1 行 INSERT)。File-differential 38/38 + differential 787/787 + unit 全緑。
  - 既知の宿題 (B.3 スコープ): `splitRightmostLeaf` は L_new + R_new を `classifyForInterior` 検査の前に allocate するため、親 overflow が発生すると 2 page が orphan + dbsize bumped で残る。B.2 fixture では絶対に発火しないが、B.3 で recursive interior split に進化させる際に allocate 順を見直す。
  - [x] Iter26.B.2.b: file-mode DELETE / UPDATE for interior-root tables。INSERT/DELETE/UPDATE の対称性を回復 — B.2 完了直後は INSERT のみ multi-page 対応で DELETE/UPDATE は leaf-root only という非対称が残っていた。`engine_dml_file` を unified per-leaf walker に refactor:
    - `collectLeafPages(a, pager, root_page) → []u32`: depth-0 (leaf root) → `[root]`、depth-1 (interior root) → `parent.cells.left_child[*] + parent.right_child`。深さ 2 以上は `Error.UnsupportedFeature` (B.3 scope)。
    - `ModifyOp` tagged union (`.delete{where}` / `.update{where, assignments, indices}`): DELETE と UPDATE の per-leaf 決定ロジックを 1 つのループに統合。
    - `modifyOneLeaf`: 既存の per-leaf rebuild を抽出、ModifyOp で behaviour を fork。survivor / re-encoded cell の dupe-before-rebuild は維持。
    - `modifyAllLeaves`: collectLeafPages → modifyOneLeaf を per-page で呼ぶ。各 leaf 独立に rebuild → writePage、合計 changed 行数を返す。
    - 公開 API (`executeDeleteFile` / `executeUpdateFile`) shape は無変化、内部だけ refactor。empty leaf は許容 (sqlite3 traversal + integrity_check が tolerate)。per-leaf size 増の split は Iter26.B.3 で対応。
    - 4 新規 file-differential cases (DELETE 跨 leaf / DELETE 1 leaf 全消去で empty leaf survival / UPDATE 跨 leaf / UPDATE all rows for full coverage)。`engine_dml_file.zig` は 408 → 448 行 (500 行 discipline 内)。File-differential 42/42 + differential 787/787 + unit 全緑。
  - [x] Iter26.B.2.c: byte-cumulative split。B.1 の docstring に明示してあった homework — `splitLeafCells` の positional midpoint を byte-cumulative midpoint に置換。各 cell の byte cost (`2 + payload-varint + rowid-varint + record bytes`) を累積し、`total/2` を初めて超える index で分割。1 cell が dominant な場合でも両 half が non-empty になるよう clamp。uniform cell では既存と同じ split を返すため B.1 fixture は無変化。`btree_split.zig` から unit tests を `btree_split_test.zig` に切り出し (370 + 163 行で 500 行 discipline 維持)。Discriminating unit test (3 tiny + 1 giant cell) で positional split (50/50 → right 側 overflow) と byte split (3/1 → right 側に giant 1個) の挙動差を検証。File-differential 42/42 + differential 787/787 + unit 全緑。
  - [ ] Iter26.B.3: recursive interior split。親 interior が full のときの再帰 (B.2 を一般化)。
- [ ] Iter26.C: overflow page chain (record が usable_size−35 を超えるケース)。
- [ ] Iter26.refactor: Cursor write-side API (`delete_current` / `update_column_at_current` / `insert`) — file-mode write が落ち着いてから in-memory DML 経路も Cursor 経由に統一 (元 Iter24.B 由来)。

### 残課題 (低優先, Phase 2 由来)

- [ ] strftime の `'now'` modifier (std.Io を Database / EvalContext に通すリファクタ要; Cursor 抽象を入れるタイミングで一緒に対応するか別 ADR か Iter25 着手前に判断)

## Backlog (Phase 4 以降)

- Phase 4: Transaction + WAL (rollback / journal / atomic commit) — ADR-0007
- Phase 5: CLI 完全互換 (`.tables` / `.schema` / EXPLAIN / `-batch` モード) — ADR-0006
- Phase 6: Index (CREATE INDEX), JOIN optimization, ANALYZE — 起票判断は Pager 完了時
- Phase 7: Automated Reasoning (TLA+/Alloy/Lean) によるトランザクション安全性証明
- VDBE bytecode 移行: ADR-0004 §5 のトリガ条件成立時に再評価 (起票せず deferred)

## Conventions

- 1イテレーションの完了条件: `zig build && zig build test && bash tests/differential/run.sh` がすべて緑、git status clean。
- 500行を超えそうなファイルは追加実装前に分割する。
- 完了タスクはこのファイルから削除し、必要なら memory/ADRへ昇格させる。

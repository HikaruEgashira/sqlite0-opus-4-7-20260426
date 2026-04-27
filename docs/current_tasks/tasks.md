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

- [ ] Iter26.A: B-tree page 更新 + page split + sqlite_schema への INSERT。**Cursor write-side API (`delete_current` / `update_column_at_current` / `insert`) を導入し、`engine_dml.executeDelete` / `executeUpdate` を Cursor 経由に切替** (Iter24.B 由来の作業をここで吸収)。CREATE TABLE / INSERT / DELETE / UPDATE が persistent に。**rollback はまだ無い** (Phase 4 で WAL)。
- [ ] Iter26.B: page split overflow (record が page サイズを超えるケース)。

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

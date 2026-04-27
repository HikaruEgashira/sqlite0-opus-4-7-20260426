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
  - [ ] Iter25.B.4: `BtreeCursor` を `cursor.zig` に追加 (vtable 経由)。**OuterFrame.current_row 所有関係を BtreeCursor lifetime contract に統合**。
  - [ ] Iter25.B.5: CLI `-file <path>` 追加 + `tests/differential/run_file.sh` 新設。**`sqlite3 fixture.db` で fixture 生成 → sqlite0 で読む moment of truth**。fixture は harness が shell で生成し Zig 側は I/O のみ (Io threading は引き続き deferred)。
- [ ] Iter25.C: `schema.zig` 新設。`sqlite_schema` 経由で `Database.tables` populate。

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

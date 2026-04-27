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
- [ ] Iter25.B: `btree.zig` 新設。Table B-tree の cell parser + traversal。`BtreeCursor` を `cursor.zig` に追加。CLI に `-file <path>` 追加。差分ハーネス `run_file.sh` 新設。**OuterFrame.current_row 所有関係を BtreeCursor lifetime contract に統合**。
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

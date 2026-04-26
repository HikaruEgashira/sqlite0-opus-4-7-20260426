# Current Tasks

最新の実装計画。完了したタスクは `docs/memory/` または ADR に統合してから削除する (CLAUDE.md "Content Lifecycle Rules" 参照)。

## Active Phase: Phase 2 — `Database` struct とインメモリ行ストア

ADR-0003 に基づき、state を持つ `Database` オブジェクト + multi-statement 実行 + `CREATE TABLE` / `INSERT` / `SELECT FROM t` の縦串を通す。

### 次の縦スライス (ADR-0003 の Phase 2 移行)
- [ ] Iter14.A: `database.zig` 新設、`Database` struct + `Database.execute` (multi-statement loop)。`exec.execute` は thin wrapper に置換、`main.zig` は `-c` / REPL で `Database` を1個共有。`SELECT 1; SELECT 2` 系の差分ケース追加、既存 286 ケース非回帰確認。
- [ ] Iter14.B: `CREATE TABLE name (col [type], ...)` パース・実行 (型注釈破棄)。空テーブルを `Database.tables` に登録。
- [ ] Iter14.C: `INSERT INTO t VALUES (...) [, (...)]` と `SELECT ... FROM t [WHERE ...]`。`FromSource` を `(VALUES ...)` から `Database.tables` まで拡張、`*` 展開も table columns で動かす。
- [ ] Iter14.D (任意): `INSERT INTO t (c1, c2) VALUES (...)` の column list 指定 + `INSERT INTO t SELECT ...`。stmt.zig が 500 行を超えそうなら `ddl.zig` / `dml.zig` を抽出。

各 Iter ごとに `tests/differential/cases.txt` を増やし、`bash tests/differential/run.sh` を緑にすること。

### Phase 1 残タスク (Phase 2 と並行可)
- [ ] Iter9: `random()`, `printf()`/`format()`, `strftime()` (時刻関数の入口)
- [ ] Iter12: `SELECT <expr> AS <alias>` (列リネーム — Result の列名表示用)
- [ ] Iter13: `LIKE` / `GLOB` / `REGEXP` 演算子

## Backlog (Phase 3以降)

- Phase 3: VDBE (バイトコードに移行) — ADR-0004 で再設計
- Phase 4: Pager + B-tree (ファイル永続化, SQLite3ファイル形式互換) — ADR-0005
- Phase 5: トランザクション + WAL
- Phase 6: JOIN, GROUP BY, ORDER BY
- Phase 7: Automated Reasoning (TLA+/Alloy/Lean) によるトランザクション安全性証明
- CLI 完全互換 (`.tables` / `.schema` / エラーメッセージ形式) — ADR-0006

## Conventions

- 1イテレーションの完了条件: `zig build && zig build test && bash tests/differential/run.sh` がすべて緑、git status clean。
- 500行を超えそうなファイルは追加実装前に分割する。
- 完了タスクはこのファイルから削除し、必要なら memory/ADRへ昇格させる。

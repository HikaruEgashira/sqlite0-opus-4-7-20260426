# Current Tasks

最新の実装計画。完了したタスクは `docs/memory/` または ADR に統合してから削除する (CLAUDE.md "Content Lifecycle Rules" 参照)。

## Active Phase: Phase 2 — `Database` struct とインメモリ行ストア

ADR-0003 に基づき、state を持つ `Database` オブジェクト + multi-statement 実行 + `CREATE TABLE` / `INSERT` / `SELECT FROM t` の縦串を通す。

### Phase 2 の状態
- Iter14.A〜D は完了 (`Database` struct / multi-statement / `CREATE TABLE` / `INSERT` / `SELECT FROM t` / `INSERT (col-list)` / `INSERT ... SELECT` / `CREATE TABLE` の duplicate column 検出)。
- 次は Phase 1 の残 (Iter9, Iter12) または Phase 3 (VDBE) のどちらかへ。

各 Iter ごとに `tests/differential/cases.txt` を増やし、`bash tests/differential/run.sh` を緑にすること。

### Phase 1 残タスク (Phase 2 と並行可)
- [ ] Iter9: `random()`, `printf()`/`format()`, `strftime()` (時刻関数の入口)
- [ ] Iter12: `SELECT <expr> AS <alias>` (列リネーム — Result の列名表示用)

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

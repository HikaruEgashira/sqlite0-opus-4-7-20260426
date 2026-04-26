# Current Tasks

最新の実装計画。完了したタスクは `docs/memory/` または ADR に統合してから削除する (CLAUDE.md "Content Lifecycle Rules" 参照)。

## Active Phase: Phase 2 — `Database` struct とインメモリ行ストア

ADR-0003 に基づき、state を持つ `Database` オブジェクト + multi-statement 実行 + `CREATE TABLE` / `INSERT` / `SELECT FROM t` の縦串を通す。

### Phase 2 の状態
- Iter14.A〜D は完了 (`Database` struct / multi-statement / `CREATE TABLE` / `INSERT` / `SELECT FROM t` / `INSERT (col-list)` / `INSERT ... SELECT` / `CREATE TABLE` の duplicate column 検出)。
- 次は Phase 1 の残 (Iter9, Iter12) または Phase 3 (VDBE) のどちらかへ。

各 Iter ごとに `tests/differential/cases.txt` を増やし、`bash tests/differential/run.sh` を緑にすること。

### Phase 2 拡張 (実用 SQL の縦串)
- [x] Iter15: `ORDER BY <expr> [ASC|DESC]` + `LIMIT N [OFFSET M]`
- [x] Iter16: `DISTINCT`
- [x] Iter17: 集約関数 (count, sum, min, max, avg, total) + `GROUP BY` / `HAVING`
- [x] Iter18: `DELETE FROM t [WHERE ...]` / `UPDATE t SET col = expr [WHERE ...]`

### 残課題 (低優先)
- [x] Iter17.A: `count(DISTINCT x)` / `sum(DISTINCT x)` / `avg(DISTINCT x)` / min/max/total + DISTINCT
- [x] Iter19.A: comma-FROM (Cartesian) + qualified column refs (`t.x` / `t.*`) + table aliases
- [x] Iter19.B: `CROSS JOIN` / `INNER JOIN ... ON` / `JOIN ... ON` keywords
- [x] Iter19.C: `LEFT [OUTER] JOIN` (per-boundary ON + NULL padding for unmatched left rows)
- [x] Iter20: `UNION` / `UNION ALL` / `INTERSECT` / `EXCEPT` (chain-level ORDER BY/LIMIT, dedup-replace-last semantics, column-count mismatch error)
- [x] Iter21: `FROM (SELECT ...) [AS alias]` 部分問合せ (column-name 由来は alias > bare-ref > columnN; star は内側 cartesian 展開; nested / setop / aggregate / LEFT JOIN 連動を確認)
- [x] Iter22.A: `EvalContext` に `?*Database` を追加し、SELECT/DML 全 evalExpr 経路に通す (refactor only)
- [x] Iter22.B: スカラサブクエリ `(SELECT ...)` を式位置で受理 (空→NULL / 多列→error / 多行→先頭行; sqlite3 互換)
- [ ] strftime の `'now'` modifier (std.Io を Database / EvalContext に通すリファクタ要)
- [ ] strftime の `'+N days'` 等の date math modifier
- [ ] strftime の `%s` (Unix epoch) / `%J` (Julian day)
- [ ] `SELECT *` ambiguity detection across duplicate-alias FROM (e.g. `FROM a t, a t`)
- [ ] Iter20 拡張: setop chain での ORDER BY <name>/<expr> 対応 (現在は position-only)
- [ ] Iter21 拡張: 任意 expression 列の合成名は現在 `columnN` を使用 (sqlite3 はソーステキストを使用); 仕様乖離あり、`alias` または bare ref で命名するワークアラウンド要
- [ ] Iter22 拡張: `IN (SELECT ...)` / `EXISTS (SELECT ...)` (subquery in WHERE)
- [ ] Iter22 拡張: `INSERT INTO t VALUES ((SELECT ...))` (parser-time VALUES に Database を渡す要)
- [ ] Iter22 拡張: correlated subquery (外側 row への参照 — EvalContext から outer rows を流す要)

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

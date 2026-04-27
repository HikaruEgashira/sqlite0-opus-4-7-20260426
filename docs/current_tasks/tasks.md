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
- [x] Iter22.C: `IN (SELECT ...)` / `EXISTS (SELECT ...)` (空→ left 無視 0 / NULL 三値論理 / EXISTS は column-count 無視; `applyIn` の empty 早期 0 化は既存 `NULL IN ()` バグの修正も兼ねる)
- [x] Iter22.D: correlated subqueries (`EvalContext.outer_frames` を 14 EvalContext サイトに通す + DML EvalContext に `column_qualifiers = [table_name]` を植える + `eval_column.evalColumnRef` が innermost-out フォールバック; correlated `EXISTS` / `IN (SELECT)` / scalar in SELECT-list を sqlite3 互換で実装。FROM-clause subquery の correlation は対象外)
- [ ] strftime の `'now'` modifier (std.Io を Database / EvalContext に通すリファクタ要)
- [x] strftime の `'+N days'` 等の date math modifier (`±N <unit>` の seconds/minutes/hours/days/months/years; `start of day/month/year`; chain対応; sign 任意; sqlite3互換: `+0.5 month`=15日, `+0.1 year`=36.5日, `+1 month` から日 overflow は JD 経由で renormalise)
- [x] strftime の `%s` (Unix epoch) / `%J` (Julian day) (純粋に DateTime→数値; `{d}` shortest-unique decimal が sqlite3 `%.16g` と一致)
- [x] `SELECT *` ambiguity detection across duplicate-alias FROM (e.g. `FROM a t, a t` → SyntaxError; multi-star は許容; `validateStarExpansion` を `executeOneSelect` に組込み)
- [x] Iter20 拡張: setop chain での ORDER BY <name> 対応 (leftmost branch の projection を case-insensitive で解決; 任意 expr / 修飾名 / 不明な name は SyntaxError; `column1` 合成名は依然 sqlite3 と差異あり)
- [ ] Iter21 拡張: 任意 expression 列の合成名は現在 `columnN` を使用 (sqlite3 はソーステキストを使用); 仕様乖離あり、`alias` または bare ref で命名するワークアラウンド要
- [x] Iter22.E: `INSERT INTO t VALUES ((SELECT ...))` および `VALUES ((SELECT ...))` (Parser に `?*Database` 追加, dispatchOne で per-statement に設定; FROM `(VALUES ...)` 内のサブクエリも同経路で対応; FROM-clause 自体の `(SELECT ...)` ではなく VALUES tuple 内の subquery 限定)
- [ ] Iter22 拡張: correlated **FROM-clause** subquery (cart は materialise 一度きり — outer 駆動で再実行する仕組みが要)

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

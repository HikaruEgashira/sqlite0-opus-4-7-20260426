# Current Tasks

最新の実装計画。完了したタスクは `docs/memory/` または ADR に統合してから削除する (CLAUDE.md "Content Lifecycle Rules" 参照)。

## Active Phase: Phase 1 — Scalar SELECT

縦串で `SELECT <expr>` を通す。テーブル/FROM句はまだ無い。

### 完了
- [x] Iter1: `SELECT <int>`, 算術演算, `NULL`, 文字列リテラル
- [x] Iter1.5: SQLite互換 `%g` float formatting (15 sig digits, scientific境界 `[-4, 14]`)
- [x] Iter2: 比較・論理演算 (`=`, `!=`, `<>`, `<`, `<=`, `>`, `>=`, `AND`, `OR`, `NOT`, `IS [NOT]`)
- [x] Iter3: `BETWEEN`, `[NOT] IN (...)`, `IS [NOT] DISTINCT FROM`
- [x] Iter4: `CASE WHEN ... THEN ... ELSE ... END` (searched & simple form)
- [x] Iter5: 文字列連結 `||`, 関数呼び出し構文, `length`/`lower`/`upper`/`substr`/`abs`/`coalesce`/`ifnull`/`nullif`/`typeof`, applyArithのテキスト→数値coerce
- [x] Iter6: `round`/`min`/`max` (scalar variadic), `replace`/`hex`/`quote`/`trim`/`ltrim`/`rtrim`/`instr`/`char`/`unicode`
- [x] Iter7: top-level `VALUES (e, ...) [, (...)]` で複数行Result
- [x] ADR-0001: nostd解釈・Zig 0.16.0採用
- [x] ADR-0002: AST移行計画 (Phase 2準備, eager parseExpr→ Expr tree-walker)
- [x] `ops.zig` / `parser.zig` / `funcs.zig` / `func_util.zig` / `funcs_text.zig` / `stmt.zig` 抽出 (500行ルール遵守の継続的分割)

### 次の縦スライス (ADR-0002 の AST 移行)
- [x] Iter8.A: `ast.zig` / `eval.zig` 新設、`parsePrimary` / `parseAddSub` を AST 化 (他は literal でラップ継続)
- [ ] Iter8.B: `parseMulDiv` / `parseConcat` / `parseUnary` / `parseEquality` / `parseComparison` を AST 化
- [ ] Iter8.C: `parseNot` / `parseAnd` / `parseOr` / `parseCase` / `parseBetween` / `parseInList` / `parseFunctionCall` を AST 化、parser.zig から ops.zig 呼び出しが消える
- [ ] Iter8.D: `column_ref` + `EvalContext.current_row` 実装、`SELECT x FROM (VALUES ...)` / `SELECT x+1 FROM (VALUES ...)` を通す

### Phase 1 残タスク (AST 移行と独立)
- [ ] Iter9: `random()`, `printf()`/`format()`, `strftime()` (時刻関数の入口)

各 Iter ごとに `tests/differential/cases.txt` を増やし、`bash tests/differential/run.sh` を緑にすること。

## Backlog (Phase 2以降)

- Phase 2: テーブル定義 (`CREATE TABLE`), インメモリ行ストア (`INSERT`/`SELECT * FROM t`)
- Phase 3: VDBE (バイトコードに移行)
- Phase 4: Pager + B-tree (ファイル永続化, SQLite3ファイル形式互換)
- Phase 5: トランザクション + WAL
- Phase 6: JOIN, GROUP BY, ORDER BY
- Phase 7: Automated Reasoning (TLA+/Alloy/Lean) によるトランザクション安全性証明

## Conventions

- 1イテレーションの完了条件: `zig build && zig build test && bash tests/differential/run.sh` がすべて緑、git status clean。
- 500行を超えそうなファイルは追加実装前に分割する。
- 完了タスクはこのファイルから削除し、必要なら memory/ADRへ昇格させる。

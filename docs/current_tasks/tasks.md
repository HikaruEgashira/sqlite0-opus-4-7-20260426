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
- [x] ADR-0001: nostd解釈・Zig 0.16.0採用
- [x] `ops.zig` 抽出 (型変換・算術・比較ヘルパーを `exec.zig` から分離)
- [x] `parser.zig` 抽出 (Parser構造体を `exec.zig` から分離、500行ルール遵守)

### 次の縦スライス
- [ ] Iter5: 文字列連結 `||`, 文字列関数 `length()`, `lower()`, `upper()`, `substr()`, `||` 連結
- [ ] Iter3: CASE式 / IS NULL / BETWEEN / IN
- [ ] Iter4: 文字列関数 `length()`, `substr()`, `lower()`, `upper()`, `||` 連結
- [ ] Iter5: 数値関数 `abs()`, `round()`, `min()`, `max()`
- [ ] Iter6: `printf` / `format` 関数

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

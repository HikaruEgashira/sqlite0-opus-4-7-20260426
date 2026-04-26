# ADR-0002: 式評価のAST移行 (Phase 2準備)

- Status: Accepted
- Date: 2026-04-26
- Supersedes part of: ADR-0001 §初期アーキテクチャ (eager tree-walk)

## Context

ADR-0001は「縦に薄くスライス」する戦略のもと、Iter1〜Iter7で `parser.zig` を eager tree-walking 評価器として実装した。`parseExpr` の戻り型は `Value` であり、各 `parse*` メソッドは「トークンを消費する」と同時に「`ops.zig` を呼び出して値を畳み込む」という二重責務を持つ。

```zig
// packages/sqlite0/src/parser.zig:52
pub fn parseExpr(self: *Parser) Error!Value { ... }

// packages/sqlite0/src/parser.zig:56-67
fn parseOr(self: *Parser) Error!Value {
    var left = try self.parseAnd();
    while (self.cur.kind == .keyword_or) {
        self.advance();
        const right = try self.parseAnd();
        ...
        left = ops.logicalOr(left, right);   // ← 評価が parser に焼き込まれている
    }
    return left;
}
```

この設計は Iter1〜7 の Phase 1 (FROM句なしのスカラー SELECT) には十分機能する — むしろ AST を介在させない分シンプルで、差分テスト 253/253 を高速に通せた。しかし **Phase 2 で `CREATE TABLE` / `SELECT col FROM t` を導入する瞬間に破綻する**。

### なぜ破綻するか

列参照 (`column_ref`) は **同じ式が、同じパース時点で、行ごとに異なる値を返す** ことを要求する。

```sql
SELECT x FROM (VALUES (1), (2), (3)) WHERE x > 1;
-- 期待: 2, 3 (2行)
```

このクエリで `x > 1` という式は1回パースされ、3回 (各タプルで1回) 評価される必要がある。一方 `x` の値は行ごとに違う。現在の `parseExpr` は「パース時に Value を返す」契約なので、

- パース時点では `x` の値を知らない (FROM が走っていない)
- 評価対象を保存する手段がない (Value だけ返してパーサ状態は破棄される)

の二重の理由で、構造的に列参照を扱えない。**「parse=eval を分離する」リファクタリングは Phase 2 に不可避な前提である**。

### Iter8 (`SELECT * FROM (VALUES ...)`) との関係

`tasks.md` の次の縦スライスは Iter8: `SELECT * FROM (VALUES ...)` であった。これは「ストレージは要らないが FROM 句と `*` 展開が要る」最小スライスとして良い目印となるが、`*` は単に列名のリストを行に展開するだけなので **AST 化なしでも動かせる可能性はある** (parseExpr を呼ばずに stmt.zig レベルで列をコピーする)。

しかし Iter8.5 として `SELECT x+1 FROM (VALUES (1),(2))` を書いた瞬間に **行コンテキストを必要とする式** が出現する。このため Iter8 を AST 移行と合わせて行うのが最も整合的である。

## Decision

### 1. AST tree-walking interpreter を採用する (VDBE は Phase 3 以降)

選択肢は2つあった:

| 方式 | 利点 | 欠点 |
|------|------|------|
| **AST tree-walk** (採用) | 実装単純、`Expr` 型を1つ足すだけ。差分テストでカバーできる範囲が直線的に増える。 | 解釈実行が遅い (Phase 6 以降の JOIN/GROUP で問題化) |
| VDBE bytecode (本物の SQLite) | Phase 4 (Pager) との整合性が高い。最終形としては正解。 | レジスタ割付・命令セット設計・デバッグ用途の disasm まで初期投資が大きい。差分テストが緑になるまでの距離が遠い。 |

CLAUDE.md の `Principal` は「Differential Testingで動作保証」「コード品質を最高水準」を要求しており、**性能の優先度は低い**。Phase 1 の方針 (薄い縦スライス) を維持しつつ列参照を導入できる AST tree-walk を採る。VDBE は Phase 3 で改めて ADR を切って移行する。

### 2. `Expr` ノード型の形

`packages/sqlite0/src/ast.zig` に新設する。

```zig
pub const Expr = union(enum) {
    literal: Value,                                  // INT/REAL/TEXT/NULL/BLOB
    column_ref: ColumnRef,                           // Phase 2 で実体化
    unary: struct { op: UnaryOp, operand: *Expr },
    binary: struct { op: BinaryOp, left: *Expr, right: *Expr },
    concat: struct { left: *Expr, right: *Expr },
    is_check: struct { left: *Expr, right: *Expr, negated: bool, distinct: bool },
    between: struct { value: *Expr, lo: *Expr, hi: *Expr, negated: bool },
    in_list: struct { value: *Expr, items: []*Expr, negated: bool },
    case_expr: struct { scrutinee: ?*Expr, branches: []CaseBranch, else_branch: ?*Expr },
    func_call: struct { name: []const u8, args: []*Expr },
};

pub const ColumnRef = struct {
    /// Phase 2 段階では「タプル中のインデックス」のみで解決する。
    /// 後で table_alias / qualified name の resolver を被せる。
    index: u16,
};

pub const BinaryOp = enum { add, sub, mul, div, mod, eq, neq, lt, le, gt, ge, logical_and, logical_or };
pub const UnaryOp = enum { negate, logical_not };

pub const CaseBranch = struct { when: *Expr, then: *Expr };
```

ノードはすべて `allocator.create(Expr)` で確保し、Statement の deinit で再帰的に解放する (ArenaAllocator を Statement スコープで使うのが衛生的)。

### 3. パーサと評価器の責務分離

```
parser.zig    : Parser.parseExpr() *Expr   ← トークン → AST のみ
eval.zig      : evalExpr(ctx, expr) Value  ← 行コンテキストを受けて値を返す
ops.zig       : 既存のまま (eval.zig から呼ばれる純粋関数)
stmt.zig      : Parser + Evaluator を組み合わせて Result を生成
```

`EvalContext` は最初は空 struct で良い (Phase 1 互換: 列参照なしの式は context 不要)。Phase 2 で `current_row: []const Value` を持たせる。

```zig
pub const EvalContext = struct {
    allocator: std.mem.Allocator,
    current_row: []const Value = &.{},
};
```

### 4. 移行戦略: 全面カットオーバー (段階的並走はしない)

候補は2つあった:

- **A. 並走 (両方の経路を保持し、列参照が要る式だけ AST 経路を使う)**
- **B. 全面カットオーバー (`parseExpr` を一気に AST 化)**

**B を採る**。理由:

- 並走は parser.zig/eval.zig の二重メンテを生み、500行ルールに反して肥大化しやすい
- 差分テスト 253 ケースが安全網として機能するため、一括書き換えのリスクは限定的
- 並走分岐ロジック自体が技術負債になる (CLAUDE.md "Secure By Design": 1行1行に意味を持たせる)

ただしカットオーバーは1コミットで全部やるのではなく、以下の **AST 移行イテレーション** に区切る:

- **Iter8.A**: `Expr` 型と `eval.zig` を新設、`parsePrimary` / `parseAddSub` のみ AST 化、それ以外は AST に literal でラップしたまま継続。差分テスト緑を維持。
- **Iter8.B**: `parseMulDiv` / `parseConcat` / `parseUnary` / `parseEquality` / `parseComparison` を AST 化。
- **Iter8.C**: `parseNot` / `parseAnd` / `parseOr` / `parseCase` / `parseBetween` / `parseInList` / `parseFunctionCall` を AST 化。Parser から ops.zig 呼び出しが消える。
- **Iter8.D**: `column_ref` / `EvalContext.current_row` を実装、`SELECT x FROM (VALUES ...)` を通す。

各サブイテレーションで `zig build && zig build test && bash tests/differential/run.sh` 緑を保つ。

### 5. 500行ルールへの影響

新規ファイル: `ast.zig` (型定義のみ、~80行見込み)、`eval.zig` (評価器、~250行見込み)。
parser.zig は逆に **小さくなる** (ops.zig 呼び出しが eval.zig に移るため)。Iter8.C 終了時点で parser.zig は ~300行に収まる見込み。

### 6. メモリ管理

Statement パース全体を ArenaAllocator で囲み、`Result` 生成後に arena を解放する設計に切り替える。理由:

- AST ノードの個別解放はリーク発生源になりやすい
- 一回のパース→評価→結果生成は短命
- Result が握る Value のみ、長命 allocator (caller) で `dupeValue` して返す

`stmt.zig` の `parseStatement` シグネチャを以下に変える:

```zig
pub fn parseStatement(allocator: std.mem.Allocator, sql: []const u8) ![][]Value {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    // parser/AST/evaluator は arena.allocator() を使う
    // 結果として得た Value は allocator (引数の長命側) で dupeValue する
}
```

## Consequences

### 利点

- Phase 2 (`CREATE TABLE` / `INSERT` / `SELECT FROM t`) の前提が整う
- 式の評価が parser から分離され、subquery / correlated query / aggregation など Phase 6 以降の機能を AST 変換で書けるようになる
- VDBE 移行時 (Phase 3 ADR-0003 予定) は AST → bytecode の codegen を1モジュール書くだけで済む

### コスト

- Iter8 の作業量が増える (4サブイテレーション)
- Iter1〜7 で書いた parser.zig の eager 評価コードが大半捨てられる (ops.zig は再利用)
- パース毎の allocation が増える (AST ノードのために arena が必要)

### 後続 ADR の予告

- **ADR-0003**: VDBE bytecode への移行 (Phase 3 に書く)
- **ADR-0004**: Pager + B-tree とストレージ層の境界 (Phase 4)

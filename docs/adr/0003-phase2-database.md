# ADR-0003: Phase 2 — `Database` struct と multi-statement 実行

- Status: Accepted
- Date: 2026-04-26
- Builds on: ADR-0002 (AST tree-walking interpreter)

## Context

Phase 1 (Iter1〜Iter11) で `SELECT <expr>` と `VALUES (...)` および `(VALUES ...)` を FROM source とした列参照・WHERE・`*` 展開が動くようになった。差分テスト 286/286、unit 101/101 が緑、全ファイル 500 行以下。

Phase 2 の目標は **persistent (今は in-memory) なテーブルを介したラウンドトリップ**:

```sql
CREATE TABLE t(x, y);
INSERT INTO t VALUES (1, 'a'), (2, 'b');
SELECT * FROM t WHERE x > 1;
-- 期待: 2|b
```

これを実装するには、現在の API には2つの構造的ギャップがある。

### ギャップ1: state を持つ実行コンテキストがない

現在の `exec.execute(allocator, sql)` は **1ステートメント = 1呼び出し** で、状態は呼び出し間に持ち越されない。`CREATE TABLE` で作ったテーブルを次の `INSERT` が見られない。

```zig
// packages/sqlite0/src/exec.zig:29
pub fn execute(allocator: std.mem.Allocator, sql: []const u8) !Result {
    const raw_rows = try stmt.parseStatement(allocator, sql);  // 単発
    ...
}
```

### ギャップ2: CLI と差分ハーネスが multi-statement を要求する

`tests/differential/run.sh:34` は `sqlite3 :memory: "$line"` で参照値を取り、`./sqlite0 -c "$line"` で実装値を取る。sqlite3 は1行に複数ステートメントを書ける:

```bash
$ sqlite3 :memory: "SELECT 1; SELECT 2"
1
2
$ sqlite3 :memory: "CREATE TABLE t(x); INSERT INTO t VALUES (1); SELECT * FROM t"
1
$ sqlite3 :memory: "SELECT 1; SELECT bad(); SELECT 2"; echo "exit=$?"
1                                       # ← 先行 SELECT は出る
Error: in prepare, no such function: bad
exit=1                                  # ← エラー以降の SELECT は走らない
```

ハーネスはこの行をそのまま `./sqlite0 -c "$line"` に渡す。Phase 2 のテストケース (`CREATE TABLE t(x); INSERT INTO t VALUES (1); SELECT * FROM t`) を1行で書くのは必須要件であり、**multi-statement のセマンティクスをハーネスに合わせて事前確定する** のが ADR の本題である。

事後的に発見すると既存 286 ケースの再回帰テストが必要になり、CLI ↔ Database ↔ stmt の3層を一度に手戻りすることになる。

## Decision

### 1. `Database` struct を新設し、state を持つ実行 API に切り替える

```zig
// packages/sqlite0/src/database.zig (新設)
pub const Database = struct {
    allocator: std.mem.Allocator,
    tables: std.StringHashMapUnmanaged(Table) = .{},

    pub fn init(allocator: std.mem.Allocator) Database;
    pub fn deinit(self: *Database) void;

    /// Execute one or more semicolon-separated statements against `self`.
    /// Returns one `StatementResult` per statement that ran.
    ///
    /// **All-or-nothing**: Zig の error union は partial 結果 + エラーを同時に
    /// は返せない。エラー発生時はそれまでに集めた `StatementResult` を解放して
    /// エラーだけ返す。すなわち sqlite3 が「エラー前の SELECT 出力は残す」
    /// 振る舞いとはバイト一致しない。差分ハーネスは §5 で示すとおり stderr
    /// 経路で `<error>` 同士に潰れて一致するため Phase 2 のテストは緑になる。
    /// 部分出力の正確性は単体テスト側で担保する (§5 既知ギャップ)。
    /// バイト一致が必要になる日 (ADR-0006) は `executeOne` ストリーミング API
    /// に置き換える。
    pub fn execute(self: *Database, sql: []const u8) !ExecResult;
};

pub const ExecResult = struct {
    statements: []StatementResult,
    allocator: std.mem.Allocator,
    pub fn deinit(self: *ExecResult) void;
};

pub const StatementResult = union(enum) {
    create_table,                  // no rows
    insert: struct { rowcount: u64 },
    select: Rows,                  // []Row, columns are anonymous in Phase 2
    values: Rows,
};
```

`exec.zig` は段階的に縮退する: 既存の `pub fn execute(allocator, sql) !Result` は `Database.init` → `Database.execute` → 最初の SELECT 結果を抽出 する thin wrapper にする (= Phase 1 互換)。テストコードの大半はそのまま走る。Phase 2 完了時に `exec.zig` を削除し `database.zig` に一本化する。

### 2. テーブルストア (`Table`) は ArrayList ベースの最小実装

```zig
pub const Table = struct {
    columns: [][]const u8,            // 正規化済み (lower-case) 名のスライス
    rows: std.ArrayListUnmanaged([]Value) = .empty,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *Table) void;
};
```

設計判断:

- **型 affinity は持たない**。SQLite3 の dynamic typing に倣い、`CREATE TABLE t(x INTEGER, y TEXT)` の型注釈は **パースして捨てる**。Phase 2 では Value をそのまま格納し、列ごとの affinity 適用は ADR-0004 (storage layer) 以降で実装する。
- **行は `ArrayList([]Value)`**。各行は `[]Value`、 `Value` 中の TEXT/BLOB は dupe 済みで Table 所有。Phase 4 (Pager) でこの所有関係が page-buffer と入れ替わる。
- **テーブル名/列名はパース時に lower-case に正規化** して `StringHashMapUnmanaged` のキーに使う。SQLite3 の case-insensitive 比較に合わせる。`current_row` 経由の列参照ルックアップ (`eval.evalColumnRef`) は既に `func_util.eqlIgnoreCase` を使うので変更不要。
- **constraint (PRIMARY KEY / NOT NULL / UNIQUE) は持たない**。Phase 5 (transaction & integrity) で改めて入れる。

### 3. Multi-statement は stream-parse で処理する (pre-split しない)

候補は2つあった:

| 方式 | 利点 | 欠点 |
|------|------|------|
| **A. SQL文字列を `;` で pre-split** | 実装が直線的 | 文字列リテラル内 `;` の正しい扱いには再 lex が必要 (二重トークナイズ) |
| **B. 既存 Parser のまま EOF まで stream-parse** | lex/parser を再利用、文字列内 `;` も自然に扱える | dispatch ループの責務が `Database.execute` 側に出る |

**B を採る**。理由:

- A は SQLite3 互換のため `;` ↔ 文字列の境界判定が必要で、これは事実上 lexer の re-implementation
- 既存の `lex.zig` は `;` を `.semicolon` トークンとして既に出している。Parser は `parseStatement` 後に `.semicolon` を消費する処理を既に持つ (`stmt.zig:80`)
- B は `Parser` を1回作り、`while (p.cur.kind != .eof) parseOneStatement(p, db)` というシンプルなループになる

```zig
// database.zig の execute 概要
pub fn execute(self: *Database, sql: []const u8) !ExecResult {
    var p = Parser.init(self.allocator, sql);
    var statements: std.ArrayListUnmanaged(StatementResult) = .empty;
    errdefer freeStatementResults(self.allocator, &statements);
    while (p.cur.kind != .eof) {
        if (p.cur.kind == .semicolon) { p.advance(); continue; }   // empty stmt
        const sr = try dispatchOne(self, &p);                       // CREATE/INSERT/SELECT/VALUES
        try statements.append(self.allocator, sr);
        if (p.cur.kind == .semicolon) p.advance();
    }
    return .{ .statements = try statements.toOwnedSlice(self.allocator), .allocator = self.allocator };
}
```

これで `;;` (empty 中間) や trailing `;` も sqlite3 と同じく許容される。

### 4. CLI 出力契約: SELECT/VALUES のみが stdout、エラーは stderr で非 0 exit

sqlite3 の動作 (実測; `Context` セクション参照):

- `CREATE TABLE` / `INSERT` は **何も出力しない**
- `SELECT` / `VALUES` は1行ずつ `|` 区切りで出力
- multi-statement の途中でエラーが起きると、それまでに完了したステートメントの出力は stdout に残り、エラー以降のステートメントは実行されない
- exit code はエラー時 1、正常時 0

`main.zig` の `runSql` は `Database.execute` の戻り値を順次イテレートし、`StatementResult.select` / `.values` のときだけ `writeRows` する。`.create_table` / `.insert` は無音。エラーは `Database.execute` が返した位置で打ち切り、stderr に書いて非 0 で抜ける。

```zig
// main.zig (Phase 2 形)
fn runSql(db: *Database, sql: []const u8, stdout: *Writer, stderr: *Writer) !u8 {
    var result = db.execute(sql) catch |err| {
        try stderr.print("error: {s}\n", .{@errorName(err)});
        return 1;
    };
    defer result.deinit();
    for (result.statements) |s| switch (s) {
        .create_table, .insert => {},
        .select, .values => |rows| try writeRows(stdout, rows),
    };
    return 0;
}
```

### 5. 差分ハーネスは無変更で Phase 2 をカバーできる

`tests/differential/run.sh` の現状:

```bash
expected="$(sqlite3 :memory: "$line" 2>&1)" || expected="<error>"
actual="$("$SQLITE0" -c "$line" 2>&1)" || actual="<error>"
[[ "$expected" == "$actual" ]]
```

ここで多重ステートメント1行をそのまま両エンジンに渡せば良い。ハーネス側の変更は不要。**ハーネス契約は ADR-0003 で確定し、Phase 2 以降のいかなる仕様変更もこの契約を破ってはならない**。

ただし2点の既知ギャップを記録する:

- **エラーメッセージのバイト一致は保証しない**。sqlite3 は `Error: in prepare, no such function: bad` と多行で書くが、sqlite0 は `error: SyntaxError` 形式。両方 `2>&1` され `|| expected="<error>"` に置換されるため、現状のハーネスではエラー有無のみが比較対象。エラー分岐をテストする場合は両方が非 0 exit すれば一致する。これは ADR-0006 (CLI 完全互換) でメッセージ形式まで一致させる。
- **エラー前の部分出力**は sqlite3 が stdout に残すが、sqlite0 の `Database.execute` は §1 で all-or-nothing に決めたためエラー時に部分出力を捨てる。これは Zig の error union が partial 結果 + エラーを同時に返せない制約からくるトレードオフで、ADR-0006 (ストリーミング `executeOne` API) で解消する。差分ハーネス上は両方とも非 0 exit で `<error>` に潰れて一致するため Phase 2 のテストは緑になる。部分出力の正確性は単体テスト側で担保する。

### 6. Phase 2 サブイテレーション

ADR-0002 の Iter8.A〜D と同様、4段階で段階的にカットオーバーする。各サブで `zig build && zig build test && bash tests/differential/run.sh` 緑を維持する。

- **Iter14.A**: `database.zig` を新設、`Database` struct + `Database.execute` (multi-statement loop) を実装。**テーブル機能はまだ無い** — `dispatchOne` は SELECT/VALUES しか受けない。**ArenaAllocator を `dispatchOne` 冒頭で起こし、AST と中間行を arena から取る (§8 を Iter14.A 時点で完全実装)**。`StatementResult` に詰める Value は `db.allocator` へ dupe して arena teardown を超えて生存させる — dupe boundary は1箇所 (`extractStatementResult` 仮称) に集約し、use-after-free を防ぐ。`exec.execute` は `Database` を経由する thin wrapper に置換。`main.zig` は `-c` と REPL で `Database` を1個作って共有。差分ケースに `SELECT 1; SELECT 2` 系を追加し、既存 286 ケースの非回帰を確認。
- **Iter14.B**: `CREATE TABLE name (col [type], ...)` をパース・実行 (型注釈は破棄)。`Database.tables` に空テーブルを登録。差分ケース: `CREATE TABLE t(x); SELECT 1` (=> `1`)、`CREATE TABLE t(x, y)` (=> 出力なし)。
- **Iter14.C**: `INSERT INTO t VALUES (...) [, (...)]` と `SELECT ... FROM t [WHERE ...]`。`FromSource` の生成元を `(VALUES ...)` から `Database.tables.get(name)` に拡張。`*` 展開は table columns に対しても動く。差分ケース: ADR の Context にある fixture 一式。
- **Iter14.D** (任意): `INSERT INTO t (c1, c2) VALUES (...)` の column list 指定形式と、`INSERT INTO t SELECT ...` (subquery insertion)。Iter14.C 完了時点で stmt.zig が肥大化していれば、`ddl.zig` (CREATE) / `dml.zig` (INSERT) を抽出する。

### 7. 500 行ルールへの影響予測

```
新設:  database.zig         ~150 行 (Database/Table/dispatch ループ)
増加:  stmt.zig 408 → 480-ish (Iter14.B/C で CREATE/INSERT パース追加)
変更:  exec.zig 160 →  90 (thin wrapper 化、Iter14.A 終了後)
変更:  main.zig 109 → 130 (Database lifecycle)
```

stmt.zig が 500 行を超えそうなら Iter14.D 着手前に `ddl.zig` を切り出す。

### 8. メモリ管理

`Database` は **caller の long-lived allocator (gpa)** を保持し、Table の rows もそこから確保する。`Database.deinit` で全テーブルを再帰解放。

各 `Database.execute` 呼び出し内で生成される一時 AST と中間行は、**Statement 単位の ArenaAllocator** で囲むことを ADR-0002 §6 で予告した。Phase 2 でこれを完全実装する: `dispatchOne` 冒頭で `var arena = ArenaAllocator.init(db.allocator)` し、AST/中間行はそこから取り、SELECT 結果の Value だけ `db.allocator` に dupe して `StatementResult.select` に詰める。INSERT 時は dupe 先が `Table.allocator` (= db.allocator) になるだけ。

これにより AST のリーク発生源 (`Expr.deinit` の手動再帰) が消え、Phase 3 (VDBE) への移行で AST を bytecode にコンパイル後即捨てる動線が自然に出る。

## Consequences

### 利点

- Phase 2 の3要素 (CREATE / INSERT / SELECT FROM t) が ADR で予め整合された API 上に乗る
- multi-statement の振る舞いを実装前に確定したことで、差分テストハーネスは Phase 2 を通じて無変更
- `Database` を1つの object で表現する設計は、Phase 4 (Pager) で `Database` が `Pager` を内包する形に自然拡張できる
- ArenaAllocator 統一でメモリ管理コードが減る

### コスト

- `exec.zig` の thin wrapper 化期間中、API が二重存在する (Iter14.A 終了で解消)
- Phase 1 で書いた `parseStatement` の戻り型 `[][]Value` は `StatementResult` に置換され、callers (テスト含む) が広範囲に修正対象
- ArenaAllocator 切り替えは「Phase 2 まとめての作業」になり、Iter14.A の作業量が増える (差分テスト緑を維持しながら memory model を切り替えるため、bug surface が大きい)

### 後続 ADR の予告

- **ADR-0004**: VDBE bytecode 移行 (ADR-0002 §後続予告に従い、AST → bytecode codegen 1モジュール追加)
- **ADR-0005**: Pager + B-tree とストレージ層境界 (Phase 4)
- **ADR-0006**: CLI 完全互換 (`.tables` / `.schema` / エラーメッセージ形式 / `-batch` モード)

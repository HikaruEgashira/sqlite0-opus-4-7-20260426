# ADR-0004: Phase 順序再編 — Pager (ADR-0005) を VDBE より先行させる

- Status: Accepted
- Date: 2026-04-27
- Supersedes (partial):
  - ADR-0002 §後続予告 "ADR-0003: VDBE bytecode への移行 (Phase 3 に書く)"
  - ADR-0003 §後続予告 "ADR-0004: VDBE bytecode 移行 (ADR-0002 §後続予告に従い、AST → bytecode codegen 1モジュール追加)"

## Context

ADR-0001/0002/0003 は phase 順序を「Phase 2 = `Database` + 縦串 → Phase 3 = VDBE bytecode → Phase 4 = Pager + B-tree」と予告した。Phase 2 が Iter14〜Iter23 で完了した時点で再評価し、**当初の順序を覆す** 判断を本ADRで明文化する。

### Phase 2 完了時点の事実

- `packages/sqlite0/src/database.zig`: `Database` struct は `tables: StringHashMapUnmanaged(Table)` で in-memory storage を保持し、`execute()` は per-statement `ArenaAllocator` を起こす規律で動いている。
- `packages/sqlite0/src/eval.zig`: AST tree-walking evaluator は `EvalContext` に `current_row` / `columns` / `column_qualifiers` / `agg_values` / `db: ?*Database` / `outer_frames: []const OuterFrame` を持ち、SELECT/DML/correlated subquery/aggregate/setop chain/CAST まで網羅している。
- `tests/differential/run.sh`: 787/787 緑。`zig build test` も緑。Iter1 時点 (253ケース) から 3.1× の差分カバレッジが AST tree-walk の上で達成されている。
- 全 `packages/sqlite0/src/*.zig` が 500 行以下 (最大 stmt.zig 478 行)。

### VDBE migration を Phase 3 とした当初の根拠

ADR-0002 §後続予告は SQLite3 本家の VDBE 採用に倣ってバイトコード化を Phase 3 に置いた。ADR-0003 §後続予告は ArenaAllocator 統一を VDBE 移行への "自然な動線" として予告した。両者は **「いずれ VDBE が要る」という前提** で順序を決めていた。

### 再評価で見えた事実

- **VDBE が今すぐ必要となる forcing function が存在しない。** 想定されていた driver:
  1. *性能*: ADR-0002 §1 で性能優先度を低と定義済み。差分テスト緑が品質基準であり、tree-walk のオーバーヘッドは気にしない。
  2. *Pager との整合性*: 当該 Pager (ADR-0005) はまだ実装されていない。「VDBE が cursor 指向で Pager に自然嵌合する」予測は **どちらも未実装な段階での推測** であり、Pager 側の cursor API を先に確定させた方が VDBE opcode 集合の制約を後付けで決められる。
  3. *EXPLAIN compatibility*: ADR-0006 (CLI 完全互換) の領域。Phase 3 起点では遅すぎず早すぎず — Pager 実装後に再判断できる。
  4. *Correlated subquery*: Iter22.D の `outer_frames` slice 連鎖は確かに手動 thread だが、VDBE で register frame を作ったとしても "correlated reference を outer cursor に解決" という構造的問題は残る。bytecode は表現が変わるだけで難しさは変わらない。
- **逆に Pager は forcing function が明確である。** persistent storage が無いとそもそも DB ではない。SQLite3 ファイル形式互換 (CLAUDE.md Principal: "SQLite3と完全に互換性のある") は file format を持たない限り検証不能。
- **AST evaluator は今後も 1〜2 phase は host VM として戦える。** Phase 4 (Pager) で `Database.tables.get(name)` の参照経路が `Pager → B-tree page → row iteration` に変わるとき、evaluator 側は **行イテレータ抽象 (Cursor)** を1段被せれば対応できる。VDBE への一括移行は不要。

## Decision

### 1. Phase 順序を改訂する (positive reordering)

```
旧: Phase 2 (Database)  → Phase 3 (VDBE)        → Phase 4 (Pager)
新: Phase 2 (Database)  → Phase 4 (Pager)*      → 〔VDBE 再評価〕
                          *本 ADR では便宜上 Phase 3 と再番号付けする
```

これは「VDBE をやめる」ではなく「順序を覆す」決定である。VDBE は将来の選択肢として残るが、**ADR-0005 (Pager) と ADR-0006 (CLI互換) が完了した後** に必要性を再評価する。

新しい phase 順序:

| Phase | 内容 | ADR |
|------|------|-----|
| Phase 1 | スカラ SELECT (Iter1〜Iter11, AST 導入含む) | ADR-0001, ADR-0002 |
| Phase 2 | `Database` struct + in-memory tables (Iter14〜Iter23) | ADR-0003 |
| Phase 3 | **Pager + SQLite3 file format** | ADR-0005 (本 ADR と同時起票) |
| Phase 4 | Transaction + WAL | ADR-0007 (起票はPager完了後) |
| Phase 5 | CLI 完全互換 (`.tables` / `.schema` / EXPLAIN) | ADR-0006 |
| Phase 6 (任意) | VDBE bytecode への移行 | (起票判断は Phase 5 完了時) |

### 2. AST evaluator が消費する Cursor contract を定義する

`Database.tables.get(name).rows` への直接参照は Phase 4 (Pager) で破綻する。Pager 統合の **設計境界** を本 ADR 段階で確定し、Phase 3 着手時に evaluator 側が再設計対象にならないようにする。

```zig
// packages/sqlite0/src/cursor.zig (Phase 3 で新設予定)
pub const Cursor = struct {
    /// Implementation pointer + vtable. tagged-union ではなく opaque pointer
    /// にしておき、Phase 3a (in-memory ArrayList backend) と Phase 3b
    /// (Pager backend) を実装の追加だけで切り替えられるようにする。
    impl: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// 行ヘッダの位置に巻き戻す。EOF からの再開も含む。
        rewind: *const fn (impl: *anyopaque) Error!void,
        /// 次の行に進む。EOF なら is_eof() が true を返すようにする。
        next: *const fn (impl: *anyopaque) Error!void,
        /// 現在行が EOF (rewind 直後 + 全行消費後) かを返す。
        is_eof: *const fn (impl: *anyopaque) bool,
        /// 列番号 (0-based) で現在行から Value を取り出す。返値の TEXT/BLOB
        /// バイトは cursor 所有 — caller は次の next()/rewind() までの
        /// 範囲でのみ参照可能。長命化するなら caller が dupe する。
        column: *const fn (impl: *anyopaque, idx: usize) Error!Value,
        /// テーブル列名スライス。lifetime は Database と同じ。
        columns: *const fn (impl: *anyopaque) []const []const u8,
    };
};
```

設計判断:

- **vtable で抽象化、tagged-union で書かない。** Phase 3a (現 `Database.tables` を Cursor 実装で包む) と Phase 3b (Pager 直結 Cursor) を **同一の evaluator コードが透過に扱う** 必要がある。tagged-union だと evaluator 側に switch が必要で、Phase 3b 投入時に `select.zig` / `engine_*.zig` 全域を再修正することになる。
- **lifetime は cursor 所有、caller dupe responsible.** 現在の `Database.tables.get(name).rows` は静的に長命だが、Pager backend では page eviction で無効化される。境界を ADR で確定しておけば Phase 4 でも変えなくて済む。
- **`columns()` は lifetime = Database。** 列定義は schema であり page ではない。Pager backend でも `sqlite_schema` テーブルから1度読んだ後は `Database` allocator で抱えるべき。
- **I/O abstraction (`std.Io`) はこの段階では入れない。** Phase 3a は purely in-memory で I/O 不要。Phase 3b の Pager 起票時に `Cursor.vtable` 拡張するか、別の `PagerHandle` を被せるかは Phase 4 の判断とする。

### 3. Iter24 (Phase 3a 着手) の最小スライス

VDBE 移行のような大きな refactor は **しない**。Phase 3a は cursor 抽象を入れるだけ:

- **Iter24.A**: `cursor.zig` を新設、in-memory `Table` を Cursor で包む `TableCursor` を実装。`engine_from.cartesianFromSources` の table-name 解決経路を `Cursor` 経由に切替 (現状の `lookupTable → table.rows.items` を `cursor.rewind / next / column` に置換)。**差分テスト 787/787 緑を維持**。
- **Iter24.B**: DML 経路 (`engine_dml.executeDelete` / `executeUpdate`) を Cursor 経由に切替。INSERT は schema-side の追記なので Cursor は不要 (引き続き `Table.rows.append`)。
- **Iter24.C** (任意): correlated subquery の outer-frame 経路で参照される行も Cursor 由来にする。`OuterFrame.current_row: []const Value` の所有関係を contract として固める。

各サブで `zig build && zig build test && bash tests/differential/run.sh` 緑を維持する。Cursor 抽象の追加で差分テストの結果が変わってはならない。

### 4. Phase 3 = Pager (ADR-0005) の起票

本 ADR と同 commit で `docs/adr/0005-phase3-pager.md` を起票する。ADR-0005 はファイル形式・page サイズ・B-tree 構造・WAL との関係 (Phase 4) の **境界** を確定する。詳細は ADR-0005 本文を参照。

「ADR-0004 単独で deferral を宣言し、後続 ADR を書かない」のは **しない** — 本 ADR は phase ordering 決定であり、ADR-0005 が同時に起票されてはじめて Ralph loop の anchor として機能する。

### 5. VDBE 再評価のトリガ条件

将来 VDBE 移行を再検討する forcing function を予め列挙する:

1. **ADR-0006 (EXPLAIN 互換)**: sqlite3 の `EXPLAIN SELECT ...` 出力は VDBE opcode 列なので、bytecode を持たないとバイト一致できない。差分ハーネスが EXPLAIN を扱う段階で再評価。
2. **JOIN/GROUP BY の性能限界 (Phase 6 想定)**: Hash JOIN / Sort-Merge JOIN が tree-walk では実装上厳しくなった時点。
3. **Prepared statement caching**: 本物の SQLite3 prepared statement と互換 API を出すとき、bytecode の方が serialise 可能で reuse しやすい。

このいずれもが「現時点で必要」とは言えない。

### 6. 500 行ルールへの影響予測

```
新設:  cursor.zig          ~80 行 (Cursor + VTable + TableCursor)
変更:  engine_from.zig 197 → ~210 行 (cursor 経由化)
変更:  engine_dml.zig  238 → ~250 行 (DELETE/UPDATE cursor 経由化)
```

500 行超は発生しない見込み。

## Consequences

### 利点

- Phase 3 の意味付けが明確になる (Pager = persistent storage = "DB たる所以")
- VDBE 投資を Pager 完了まで遅らせることで、cursor API の制約を実装後に決められる (= 後付けより制約が少ない)
- AST evaluator は Phase 3〜5 を通じて host VM として残り、787 ケースの安全網がそのまま継承される
- ADR-0002 §後続予告と ADR-0003 §後続予告の予測が改訂される ことを明示することで、docs の整合性が保たれる
- Ralph loop は次の anchor (Iter24.A: Cursor 抽象 → Pager) を持てる

### コスト

- Phase 1〜2 の ADR と本 ADR の間で「VDBE が次」という従前の予告が無効化されるため、過去 ADR を読む人は本 ADR に到達するまで誤解の余地がある (各 ADR 末尾に supersede note を置くまでには至らない — 本 ADR の冒頭 "Supersedes (partial)" がそれを担う)
- AST evaluator が長期化することで、`outer_frames` 等の手動 thread コードが Phase 4 まで残る
- VDBE 設計の経験値を得る機会が後ろに倒れる (将来 ADR で初学者になりやすい)

### 後続 ADR の予告 (本 ADR で改訂)

- **ADR-0005**: Pager + B-tree (Phase 3) — 本 ADR と同時起票
- **ADR-0006**: CLI 完全互換 (Phase 5) — Pager 完了後に起票
- **ADR-0007**: Transaction + WAL (Phase 4) — Pager 完了後に起票
- VDBE 移行の ADR は番号を予約せず、§5 のトリガ条件のいずれかが成立したときに起票する

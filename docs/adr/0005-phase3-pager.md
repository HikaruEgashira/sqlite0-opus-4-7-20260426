# ADR-0005: Phase 3 — Pager + SQLite3 file-format 互換 storage layer

- Status: Accepted
- Date: 2026-04-27
- Builds on: ADR-0003 (Phase 2 `Database` struct), ADR-0004 (Phase 順序改訂; Pager を VDBE より先行)

## Context

ADR-0004 で Phase 順序を「Phase 3 = Pager」に改訂した。本 ADR は Phase 3 の **境界** を確定する。実装イテレーション (Iter25.A〜) の細部より、**何を Pager に押し込み、何をその上層 (`Database` / cursor / evaluator) に残すか** の責務分割を先に決める。

CLAUDE.md Principal は「SQLite3 と完全に互換性のある」を要請しており、これは ファイル形式 互換まで含む。Pager 設計の自由度は **読み書きするバイト列が SQLite3 と一致しなければならない** という制約で大きく狭まる — むしろこの制約があるおかげで多くの設計判断は SQLite3 source/file-format spec に既に答えがある。

### Phase 3 が解く課題

1. **persistent storage**: 現在 `Database.tables` は in-memory `StringHashMapUnmanaged`。プロセス終了で全消失。
2. **SQLite3 .db ファイルの読み書き**: 既存 sqlite3 で作った `.db` を sqlite0 で開いて差分テストできるようにする (差分ハーネスの将来拡張)。
3. **`sqlite_schema` テーブル**: SQLite3 のスキーマは `sqlite_schema` (旧 `sqlite_master`) という meta-table に CREATE 文として格納される。`Database.tables` の HashMap は ファイル化したときに この meta-table と同期する必要がある。
4. **page cache 方針**: メモリに常駐させる page 数の上限と eviction policy。Phase 3 段階では LRU 単純実装で十分。
5. **lock 戦略**: 本 ADR では single-writer/multi-reader を扱わず、**single-process exclusive lock** で済ませる。WAL/journal 等の concurrent control は Phase 4 (ADR-0007: Transaction + WAL) で改めて定義する。

### Phase 3 が解かない課題

- **トランザクション**: `BEGIN; ... COMMIT;` の構文と semantics は Phase 4。Phase 3 段階では「statement 1個 = implicit auto-commit、ただし atomic は保証しない」レベル。
- **WAL ジャーナル**: rollback journal も WAL も Phase 4。Phase 3 では rollback 不可 (= 中断時はファイル不整合のリスクがある) という割り切り。
- **VDBE bytecode**: ADR-0004 で先送り済み。
- **Index (CREATE INDEX)**: Phase 3 範囲外。テーブルスキャンのみ。

## Decision

### 1. ファイル形式は SQLite3 と byte-for-byte 互換

参照仕様: <https://www.sqlite.org/fileformat.html>。実装の具体的拘束:

- **Page size 4096 (デフォルト)**。`PRAGMA page_size` は Phase 3 では受け付けない (固定)。
- **Header 100 byte** + Page 1 (sqlite_schema の B-tree root) という layout を厳守。
- **Big-endian integer encoding** (sqlite3 の慣行)。
- **Variable-length integer (varint)** は SQLite3 独自エンコード (1〜9 byte; high-bit が continuation)。
- **Record format**: header (varint で型タグ列) + body (実データ)。NULL/INTEGER/REAL/TEXT/BLOB の 5 型。

Phase 3a/b/c で段階的に対応する (§3 サブイテレーション参照)。

### 2. モジュール構成と責務分割

```
packages/sqlite0/src/
  pager.zig       page-level read/write + page cache (LRU) + file lock
  btree.zig       B-tree page parser/writer (table B-tree のみ; index は Phase 6)
  schema.zig      sqlite_schema meta-table の同期 (CREATE/DROP の永続化)
  cursor.zig      ADR-0004 §2 で予告済み Cursor; Phase 3a で in-memory 実装、
                  Phase 3b で B-tree backed 実装を追加
  database.zig    変更: tables HashMap を schema.zig 経由に切替
                  (key=name → cursor factory)
```

責務境界:

- `pager.zig` は **page 番号 → 4096-byte buffer** の単位でしか考えない。「これがどんな B-tree node か」は知らない。
- `btree.zig` は page 内 のレイアウト (header / cell pointer array / cells) と、page を跨ぐ child 参照の解決を担う。Pager から page を受け取り、cursor が要求する row へ変換して返す。
- `schema.zig` は `sqlite_schema` テーブル (= 特別な root page 1 の table B-tree) の読み書きと、`Database.tables` 等の in-memory schema state との同期を担う。
- `cursor.zig` (ADR-0004 §2) は evaluator から見た **唯一の row source 抽象**。Phase 3a の `TableCursor` (in-memory `Table.rows` を包む) と Phase 3b の `BtreeCursor` (`btree.zig` 経由) を **同一の vtable で** 出す。

### 3. Phase 3 サブイテレーション

ADR-0003 §6 の Iter14.A〜D に倣い、4段階で段階的にカットオーバーする。各サブで `zig build && zig build test && bash tests/differential/run.sh` 緑を維持する。

- **Iter24.A〜C** (ADR-0004 §3 で予告済み): Cursor 抽象を入れ、in-memory Table を Cursor 経由で消費する形に evaluator を切替。**この段階ではファイルは存在しない** — purely in-memory のまま、cursor 抽象だけ被せる。
- **Iter25.A**: `pager.zig` を新設。`init(allocator, file_path)` でファイル open、`getPage(n)` で 4096-byte buffer を返す read-only API のみ。LRU cache 16 page。lock は kernel `flock` で exclusive。**まだ btree も schema も無い** — page level の I/O だけ動かし、unit test で sqlite3 で生成した `.db` の page 1 (header) を正しく読めることを確認する。
- **Iter25.B**: `btree.zig` を新設。Table B-tree の cell parser と「row id → record」 traversal を実装。`BtreeCursor` を `cursor.zig` に追加。**まだ書き込みは実装しない** — read-only。差分ケース: sqlite3 で生成した `.db` を `sqlite0 -file` で開いて `SELECT * FROM t` が一致する。
- **Iter25.C**: `schema.zig` を新設。`sqlite_schema` を Pager 経由で読み、`Database.tables` を populate。CLI に `-file <path>` オプションを追加して fixture を読む差分テストを enable。
- **Iter26.A**: 書き込み path (B-tree page 更新, page split, sqlite_schema への INSERT)。CREATE TABLE / INSERT / DELETE / UPDATE が persistent に。**rollback はまだ無い** — クラッシュ時の整合性は保証しない (Phase 4 で WAL を入れる)。
- **Iter26.B**: page split の overflow 処理 (record が page サイズを超えるケース)。

5 イテレーション分の作業量。VDBE 移行 (ADR-0004 で deferral) より大きいが、CLAUDE.md Principal の「SQLite3 と完全に互換性のある」を満たすには **必須** であり、後ろに倒せない。

### 4. 差分テストハーネスの拡張

`tests/differential/run.sh` の現状契約は ADR-0003 §5 で確定済み — `sqlite3 :memory:` と `sqlite0 -c` を比較する。Phase 3 で **ファイル経由の差分** を追加する:

```bash
# tests/differential/run_file.sh (新設)
# fixture .db を sqlite3 で生成し、sqlite0 -file で開いて結果を比較
sqlite3 fixture.db "$create_and_insert"
expected="$(sqlite3 fixture.db "$query")"
actual="$(sqlite0 -file fixture.db "$query")"
[[ "$expected" == "$actual" ]]
```

ハーネス自体の追加は Iter25.B (read-only path) 完了時。Iter25.A 段階では unit test の page-level 検証だけで足りる。

### 5. メモリ管理 (Pager との合流)

ADR-0003 §8 の per-statement `ArenaAllocator` 規律は維持する。Pager は **`db.allocator` から page buffer を取り、Pager.deinit で開放する** — statement arena には混ぜない (page は statement を跨いで生きる)。

Cursor が返す Value の TEXT/BLOB バイトは **page buffer 領域を借用** している。caller が statement 跨ぎで持ちたい場合は arena ではなく long-lived allocator に dupe する。これは ADR-0004 §2 の Cursor contract (lifetime = cursor 所有 / 次の next() まで) と整合する。

`engine.dispatchOne` の dupe boundary `dupeRowsToLongLived` は Phase 3 でも変わらない — 出力の `StatementResult` は引き続き `db.allocator` 所有で arena teardown を超えて生存する。

### 6. lock 戦略 (Phase 3 範囲)

- **single-writer**: open 時に `flock(fd, LOCK_EX)`。複数プロセスが同じ `.db` を開けないことで concurrency 問題を回避する。
- **error 報告**: lock 取得失敗は `Error.DatabaseLocked` (新設) を返す。CLI は exit 5 (sqlite3 互換)。
- **lock 解放**: `Database.deinit` で `flock(fd, LOCK_UN) → close(fd)`。

これは「最も単純で安全」な戦略であり、**WAL / journal が無い Phase 3 では正しい**。Phase 4 で multi-reader/single-writer に切り替える。

### 7. 500 行ルールへの影響予測

```
新設:  pager.zig          ~250 行 (LRU cache + file I/O + lock)
新設:  btree.zig          ~350 行 (cell parser + traversal + write)
新設:  schema.zig         ~150 行 (sqlite_schema 同期)
新設:  cursor.zig         ~80 行  (vtable + TableCursor; Iter24)
変更:  database.zig 420 → ~440 行 (Pager との接続; tables → schema 経由)
変更:  engine_from.zig 197 → ~210 行 (cursor 経由化; Iter24 で大半済む)
```

`btree.zig` は 500 行に近づくため、Iter25.B 時点で `btree_read.zig` / `btree_write.zig` への分割を **着手前に** 計画する。

## Consequences

### 利点

- Phase 3 完了時点で sqlite0 は **本物の DB** になる (ファイル形式互換 + persistent storage)
- ADR-0004 で deferral した VDBE の必要性を、Pager cursor が動く環境で再評価できる (現実のデータで)
- AST evaluator は Cursor 抽象を介して in-memory / on-disk を透過に扱う — Phase 4 (WAL) で更に backend が増えても evaluator 不変

### コスト

- 5 イテレーション分の集中作業 (Iter24.A〜26.B)。Phase 2 の Iter14〜23 (10 イテレーション) より少ないが、各イテレーションは bug surface が大きい (file I/O, page layout, lock)
- sqlite3 file format spec の精読が必要 (実装着手前に Iter25.A で 100-byte header の bit 単位の理解が必須)
- ファイル経由の差分テストは sqlite3 CLI を 1 fixture あたり呼び出すため遅くなる — `run_file.sh` は CI で別並列に回す

### 後続 ADR の予告

- **ADR-0006**: CLI 完全互換 (`.tables` / `.schema` / EXPLAIN / `-batch` モード) — Phase 5
- **ADR-0007**: Transaction + WAL — Phase 4 (本 ADR の rollback 欠如を解消)
- **ADR-0008+** (任意): `CREATE INDEX` (Index B-tree) — Phase 6 ANALYZE/JOIN optimization と一緒
- VDBE 移行は ADR-0004 §5 のトリガ条件成立時に起票

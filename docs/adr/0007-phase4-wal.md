# ADR-0007: Phase 4 — Write-Ahead Log (WAL) と Transaction semantics

- Status: Proposed
- Date: 2026-04-27
- Builds on: ADR-0005 (Phase 3 Pager + file format)、ADR-0004 (Phase 順序)

## Context

Phase 3 (ADR-0005) で sqlite0 は SQLite3 ファイル形式互換の persistent storage を獲得したが、**rollback も atomic commit も無い**。書き込みは `Pager.writePage` で in-place mutation し、複数 page にまたがる更新 (page split, balance-deeper, overflow chain alloc) は parent-last invariant でクラッシュ時にも sqlite3 が読めるバイトを残すという「best-effort durability」しか持たない。

Phase 4 は Principal「SQLite3 と完全に互換性のある Enterprise Grade」のうち **rollback / atomic commit / 安全な multi-reader-single-writer concurrency** を満たす。WAL は SQLite3 推奨 (`PRAGMA journal_mode=WAL`) であり、rollback journal より concurrency が良い。Phase 4 は **WAL を採用** し、rollback journal は実装しない (sqlite3 互換のため open 時の `-journal` ファイル検出は最低限ハンドルする)。

参照仕様:
- <https://sqlite.org/walformat.html> — WAL ファイル / frame / checksum / -shm
- <https://sqlite.org/wal.html> — WAL の動作モデル / checkpoint
- <https://sqlite.org/pragma.html#pragma_journal_mode>

### Phase 4 が解く課題

1. **atomic commit**: 1 statement (= 1 implicit transaction) が複数 page を変更しても、commit point 直前のクラッシュは「全部適用」か「全部未適用」のいずれかになる。
2. **rollback**: `BEGIN; ... ROLLBACK;` の構文と semantics。WAL なら未 commit frame を破棄するだけ。
3. **multi-reader-single-writer**: WAL があれば reader は WAL frame を見て snapshot を読み、writer は WAL に append するだけ。読者は writer をブロックしない。
4. **WAL file format 互換**: sqlite3 で `PRAGMA journal_mode=WAL` した DB を sqlite0 で開く / 書く / checkpoint する。byte-for-byte の WAL frame layout 互換。
5. **recovery**: クラッシュ後の cold open 時に WAL を scan し、最後の commit frame までを apply、それ以降は破棄。

### Phase 4 が解かない課題

- **新規 DB の DELETE mode への書き込み**: Phase 4 完了後、sqlite0 が **新規作成** する DB は WAL mode (header bytes 18/19 = 2/2)。既存 DELETE-mode DB は (§1.5 参照) read 時に `-journal` recovery + read-only fallback で開けるが、書き込みは行えない (rollback journal の write 経路は実装しない)。sqlite0 が書き込む DELETE-mode DB が必要なら明示的な `PRAGMA journal_mode=WAL` で WAL に格上げするのが推奨される (Phase 5 以降で pragma の set 側を実装)。
- **`PRAGMA wal_autocheckpoint`**: デフォルト 1000 frame で auto checkpoint するが、Phase 4 では明示的な `PRAGMA wal_checkpoint` または close 時 checkpoint のみ実装。閾値ベース auto は後続。
- **WAL2 (BEGIN CONCURRENT)**: 実験的拡張 — 範囲外。
- **shm のクロスプロセス共有**: sqlite3 は `-shm` を mmap して frame index をプロセス間で共有する。Phase 4 では **single-process exclusive lock** (Phase 3 の戦略を継続) を保ち、shm はインメモリ index として再構築のみ (= `PRAGMA locking_mode=EXCLUSIVE` 相当)。クロスプロセス共有は Phase 5 (CLI 完全互換) 以降の課題。
- **savepoint / nested transaction**: Phase 4 は flat transaction のみ。
- **underfull rebalance** (B.4 scope): Phase 4 は durability/atomicity を解くが、structural な「empty leaf at depth ≥ 2 = SQLITE_CORRUPT」spec rule は引き続き B.3.f の `Error.UnsupportedFeature` で fail-loud のまま。本物の解決 (sqlite3 `balance_quick`/`balance_nonroot` 相当) は Phase 4 完了後の B.4 で扱う。

## Decision

### 1. WAL ファイル形式は SQLite3 と byte-for-byte 互換

`<https://sqlite.org/walformat.html>` 厳守:

- ファイル名: `<dbname>-wal`、main DB と同一ディレクトリ。
- **WAL header (32 byte)**:
  - bytes [0..4]   magic — `0x377f0682` (BE checksum) または `0x377f0683` (LE checksum)。**page bytes 自体の order ではなく、checksum 算出時に 32-bit word をどちらの byte order で消費するか** を表す (sqlite3 は host endianness を選ぶ)。sqlite0 は portable のため **常に `0x377f0682` (BE checksum)** を採用。Page bytes は WAL frame 内でも main file と同じ byte-identical 形式 (big-endian integer encoding) で書く。
  - bytes [4..8]   file format version (`3007000`)
  - bytes [8..12]  page size (PAGE_SIZE)
  - bytes [12..16] checkpoint sequence number (open ごとに +1)
  - bytes [16..20] salt-1 (random per checkpoint)
  - bytes [20..24] salt-2 (random per checkpoint)
  - bytes [24..28] checksum-1 over [0..24]
  - bytes [28..32] checksum-2 over [0..24]
- **Frame (24 + PAGE_SIZE bytes)**:
  - bytes [0..4]   page number
  - bytes [4..8]   commit_size (0 = mid-transaction frame; >0 = commit frame, value = post-commit dbsize in pages)
  - bytes [8..12]  salt-1 copy from header
  - bytes [12..16] salt-2 copy from header
  - bytes [16..20] cumulative checksum-1
  - bytes [20..24] cumulative checksum-2
  - bytes [24..]   page bytes (PAGE_SIZE)
- **Checksum algo**: SQLite3 独自 (`s0 += (s0<<3) + byte`; pairwise sum over 8-byte chunks)。仕様 §4.4。32-bit word を BE/LE どちらで読むかが magic で分岐。

### 1.5. Journal mode on open: 既存ファイルの mode を尊重 (option (c))

新規 DB を作るときと既存 DB を開くときで挙動が違う:

- **新規 DB**: `Database.openFile` (= 内部 `Pager.open`) が新規作成パスで header bytes 18/19 を `(2, 2)` (= WAL) に設定して書き出す。Phase 4 完了後の sqlite0 が新規作成する DB は常に WAL モード。
- **既存 DB**: `Database.openFile` で header bytes 18/19 を読み:
  - `(1, 1)` (legacy / DELETE journal mode): **read 経路は対応**。`<dbname>-journal` が存在する場合は rollback journal の recovery (= journal の hot/cold 判定 + rollback or 削除) を最低限実装し、commit 状態の main file をそのまま読む。**書き込みは `Error.UnsupportedFeature`** (`-journal` を書く path は実装しない)。Phase 5 で `PRAGMA journal_mode=WAL` の set 側を実装したら DELETE → WAL 格上げ経由で書けるようになる予定。
  - `(2, 2)` (WAL): フル機能 (read + write + checkpoint)。`-wal` が存在すれば §4 の recovery 経路で apply。
  - その他: `Error.IoError` (sqlite3 でも未知 mode は corrupt)。

これにより **既存 `file_cases.txt` の全 fixture (sqlite3 が DELETE mode で生成) は引き続き read 互換** で、WAL fixture は新規 directive `WAL_SETUP:` (§6) で別途追加する。`Database.openFile` はこの mode bit を `db.journal_mode: enum {delete_ro, wal}` に保持し、DML 経路が `delete_ro` への write 試行を `Error.ReadOnlyDatabase` で reject する。

`-journal` recovery の最小実装 (Iter27.0 の subtask): `<dbname>-journal` を `Database.openFile` 時に検出し、(a) header magic + page count が valid なら main file への rollback (各 page を pwrite で復元) → journal 削除、(b) corrupt なら hot journal として扱い journal 削除のみ。これは sqlite3 の `pager.c::pager_playback` の最小サブセット。本格的な journal write は実装しない。

### 1.6. fsync 方針 (PRAGMA synchronous=FULL 相当)

ADR-0005 §6 で fsync を Phase 4 に deferral していた件をここで確定させる。Phase 4 の "Enterprise Grade data integrity" は in-memory atomicity ではなく **on-disk durability** を意味するため、commit point で必ず fsync を発行する。

- **commit frame 書き込み時**: `pwrite(wal_fd, commit_frame)` の **後で `fsync(wal_fd)` を呼んでから** `Database.execute` を return する。commit point は「commit frame が durably on disk」であって「page cache に commit frame 相当が乗った」ではない。これが無いと電源喪失で commit 済みデータが消えうる。
- **checkpoint 時**: 全 frame を main file に pwrite した **後 `fsync(main_fd)`、その後で WAL truncate**。順序: pwrite frames → fsync main → truncate WAL。fsync を skip すると checkpoint 中のクラッシュで「main は古い、WAL は消えた」状態になり data loss。
- **デフォルト動作**: sqlite3 `PRAGMA synchronous=FULL` 相当 (= 全 commit で fsync)。`PRAGMA synchronous=NORMAL/OFF` は Phase 5 以降で実装、Phase 4 では FULL hardcode。
- **範囲外 (Phase 5+)**:
  - darwin の `F_FULLFSYNC` (fsync より強い順序保証だが Linux で portable でない) — `synchronous=EXTRA` 相当として後続。
  - 親ディレクトリの fsync (= ファイル名 entry の durability) — 新規 DB / -wal 初回作成時のみ optional として扱う。

実装ポイント: `wal.zig` に `appendCommitFrame(wal_fd, frame_bytes)` ヘルパーを置き、(a) pwrite (b) fsync の 2 ステップを atomic な単位として扱う。call site (= `Database.execute` 終端) からは 1 関数呼び出し。テスト: fsync 失敗を mock するのは難しいので、**Iter27.B の差分テストで sqlite0 commit 後の `-wal` を sqlite3 で読み戻し、両者が同一 byte であること** で間接的に "writePage が disk に届いた" を検証する。

### 2. WAL Index (shm) はインメモリ再構築 (EXCLUSIVE mode 相当)

sqlite3 は `-shm` ファイルを mmap し、複数プロセスから WAL frame index (page no → frame index) を共有する。Phase 4 では:

- `<dbname>-shm` ファイルは **作らない** (sqlite3 が後から開いたとき自動再構築する)。
- WAL index は `Pager` 内の `std.HashMap(u32, FrameInfo)` でメモリに保持。
- Pager.open 時に WAL を scan し、commit frame ごとに index を rebuild。
- 結果として **同一プロセス内** での multi-reader-single-writer のみサポート。クロスプロセスは引き続き flock(EX)。

これは sqlite3 の `PRAGMA locking_mode=EXCLUSIVE` の挙動と同じ。Phase 5 以降で本物の shm を入れる。

### 3. モジュール構成と責務分割

```
packages/sqlite0/src/
  wal.zig          WAL ファイル format I/O (header / frame / checksum)
  wal_index.zig    インメモリ WAL frame index (page_no → frame_offset, snapshot tracking)
  wal_recovery.zig open 時の WAL scan + rebuild + truncate-on-corrupt
  wal_checkpoint.zig WAL → main file への drain
  pager.zig        変更: getPage が WAL index を先に lookup、writePage が WAL append に切替
  database.zig     変更: BEGIN/COMMIT/ROLLBACK の transaction state machine
```

責務境界:

- **`wal.zig`** は frame/header の純粋 encoder/decoder。Pager とは独立に unit test 可能。
- **`wal_index.zig`** は in-memory map。Pager から `lookupFrame(page_no) → ?frame_offset` で問い合わせる。
- **`wal_recovery.zig`** は Pager.open の hook。`-wal` が存在すれば scan、commit frame までを index に登録。それ以降の partial frame は破棄 (= file truncate or 無視)。
- **`wal_checkpoint.zig`** は drain 専用。WAL の各 frame を main file に pwrite し、WAL を 0-byte truncate (or restart)。Pager が write を抱えていないタイミングでしか呼べない。
- **`pager.zig`**: getPage は (1) WAL index lookup → (2) main file fallback。writePage は (1) WAL に frame append → (2) WAL index 更新。Page cache は WAL frame と main file の両方を mix で保持できるが、frame info を tag する必要あり。
- **`database.zig`**: 暗黙トランザクション (auto-commit) は statement 1個ごとに WAL に commit frame を打つ。明示トランザクション (BEGIN/COMMIT) は commit frame を COMMIT まで遅延、ROLLBACK は WAL を BEGIN 前の長さに truncate。

### 4. Phase 4 サブイテレーション (Iter27.0〜E)

advisor 推奨の incremental shape。各サブで `zig build && zig build test && bash tests/differential/run.sh && bash tests/differential/run_file.sh` 緑を維持する。

- **Iter27.0: Journal-mode-on-open detect + DELETE-journal recovery (purely additive)**。§1.5 の最小実装のうち **read 側のみ**。`Database.openFile` で header bytes 18/19 を読み `journal_mode` を保持。`(1,1)` で `-journal` 存在時は rollback (各 page を pwrite で復元) → journal 削除、または hot journal として削除。**この iter では `delete_ro` write enforcement は入れない** — write 経路は今まで通り in-place mutation を続ける (= 既存 53 cases の MUTATE は不変)。WAL bit (`(2,2)`) の検出も同時に入れるが、まだ write 経路はそれを使わない。差分: 既存 `file_cases.txt` 53 cases が全部緑 (read/write どちらも不変)、新規 read-only fixture で `-journal` ありの DB を recovery して読める。
- **Iter27.A: WAL read-side**。`wal.zig` (header/frame parser + checksum verify) と `wal_recovery.zig` (open 時 scan) を実装。`Pager.getPage` が `wal_index.lookupFrame` を先に呼び、ヒットすれば WAL frame の page bytes を返す。**書き込みは未対応** — sqlite0 は WAL を読むだけ。差分: sqlite3 で `journal_mode=WAL` 状態にし、INSERT 後に checkpoint せずに残った `-wal` を sqlite0 で読み、SELECT が一致する。`run_file.sh` に `WAL_SETUP:` directive を追加 (sqlite3 で `journal_mode=WAL` を有効化し、INSERT 後に checkpoint せずに -wal を残すヘルパー)。
- **Iter27.B: WAL write-side + journal-mode write dispatch**。新規 DB を WAL mode (`(2,2)`) で作成するように `Database.openFile` を切り替え。`Pager.writePage` を「WAL frame append + index 更新」に切替。auto-commit 1 statement = 1 frame batch、最後の frame に commit_size を立てる。`Pager.allocatePage` も WAL 経由で dbsize 更新を frame として記録 (page 1 frame として)。**ここで初めて Iter27.0 で検出した DELETE mode への write が `Error.ReadOnlyDatabase` で reject される** — write target を「in-place」から「WAL frame」に *swap* するタイミングで DELETE-mode write を遮断するので、中間的な regression window は無い。差分: 既存 `file_cases.txt` の MUTATE 群は `WAL_SETUP:` (新規) で WAL mode に変えた fixture を使うように書き換える。WAL mode の MUTATE 後 `PRAGMA integrity_check` 前に sqlite0 側で auto-checkpoint するフックを `run_file.sh` に追加 (Iter27.C と一部統合)。
- **Iter27.C: Checkpoint**。`PRAGMA wal_checkpoint(PASSIVE)` を実装。WAL 全 frame を main file に pwrite → `fsync(main_fd)` → WAL `truncate(0)` (順序は §1.6)。Database.deinit 時に自動 checkpoint (close-on-checkpoint)。**Page cache の WAL-sourced entry は checkpoint 後 invalidate** して次の `getPage` で main file から再読込 (re-tag より単純で正しい)。差分: sqlite0 で書いて checkpoint した main file が sqlite3 で読めて WAL は空。
- **Iter27.D: Recovery hardening**。partial frame (checksum 不一致 or salt 不一致) の安全な破棄。WAL header 破損時の bail-out。`flock(EX)` を `flock(SH)` + writer 1 体制にせず、まずは EXCLUSIVE のまま robustness だけ追加 (multi-reader 対応は Phase 5 以降)。差分: 中断シナリオを fixture script で再現し、open 後の SELECT が pre-WAL 状態と一致。
- **Iter27.E: BEGIN/COMMIT/ROLLBACK + journal_mode pragma (read-only)**。明示トランザクションの構文と semantics。COMMIT で WAL に commit frame、ROLLBACK で WAL を BEGIN 直前長に truncate。`PRAGMA journal_mode` は現在の mode (`wal` or `delete`) を返す read-only として実装 (set は Phase 5)。差分: BEGIN+INSERT+ROLLBACK で行が消える; BEGIN+INSERT+COMMIT で残る。

6 イテレーション (Iter27.0〜E)。Phase 3 の Iter25.A〜26.C より少し大きいが、Iter27.0 は journal-mode dispatch という基礎工事で工数は限定的。

### 5. Pager.writePage 経路の互換 shim

Phase 3 の全コード (`btree_insert.rebuildLeafTablePage` 後の `pager.writePage`、`balanceDeeperRoot` の child + parent write、`splitInteriorPage` の L+R write、`Pager.allocatePage` の zero page write、`Pager.freePage` の trunk + page1 update) は **API 不変** で WAL 経路に切り替わる。

- `Pager.writePage(page_no, bytes)` が WAL frame append に切り替わる。caller は知らない。
- 暗黙トランザクションの commit point は **statement 終端** (= `Database.execute` から戻る瞬間)。Pager 内に `pendingFrames: ArrayList` を持ち、`Database.execute` が完了したら最後の frame に commit_size を立てて flush。途中のエラーは `pendingFrames` を drop。
- B.3.f の deferred-free pattern は WAL 上では「commit frame 後に freelist 更新 frame を打つ」と等価になる — atomic commit に統合される。**ただし B.3.f が拒否する「empty leaf at depth ≥ 2」自体は spec 上の structural rule なので WAL では解けない** (B.4 underfull rebalance 待ち)。
- balanceDeeperRoot / balanceDeeperInterior の parent-last invariant は WAL 上でも有効 (frame 順序が再生順序を決めるため)。むしろ WAL では「commit frame が無いと何も適用されない」ため atomicity は WAL が保証してくれる — Phase 3 の orphan-page tolerance が「全 frame が atomic」に格上げされる。

### 6. 差分テストハーネスの拡張

`tests/differential/run_file.sh` に WAL 用 directive を追加:

```
# 既存
SETUP: <SQL>
MUTATE: <SQL>
VERIFY: <SQL>

# 新設 (Iter27.A〜)
SETUP: <SQL>
WAL_SETUP: <SQL>   # PRAGMA journal_mode=WAL; <SQL> を sqlite3 で実行し、checkpoint せずに -wal を残す
MUTATE: <SQL>      # sqlite0 で実行 (WAL 経路)
VERIFY: <SQL>      # sqlite3 と sqlite0 両方で実行
```

Iter27.A はまず WAL_SETUP + read-only QUERY、Iter27.B 以降で WAL_SETUP + MUTATE + VERIFY。

`run_file.sh` の現状は `cp fixture_a fixture_b` で main DB ファイルだけを複製しているが、WAL_SETUP を持つ case では **sidecar (`-wal`, `-shm`, Iter27.0 後は `-journal` も) を一緒に cp する** ように `cp_with_sidecars()` ヘルパーを導入する (Iter27.0 で先行投入)。これを忘れると WAL fixture の MUTATE が片側だけ WAL を見て差分が破綻する。

### 7. メモリ管理

- WAL frame の page bytes は Pager の page cache に **一級市民として** 載る (frame index に「これは WAL frame N から来た」タグを付ける)。
- `pendingFrames` は `db.allocator` から alloc し、commit / abort 時に開放。
- Recovery で scan した frame info は `wal_index` に残るが、scan 中に alloc した一時 buffer は scan 終了で deinit。

### 8. 500 行ルールへの影響予測

```
新設:  wal.zig             ~250 行 (header / frame encoder/decoder + checksum)
新設:  wal_index.zig       ~150 行 (HashMap + snapshot tracking)
新設:  wal_recovery.zig    ~200 行 (open scan + truncate)
新設:  wal_checkpoint.zig  ~150 行 (drain + truncate)
変更:  pager.zig 491 → ~480 行 (getPage に WAL lookup 追加、writePage を frame append に切替、size 圧縮のため LRU 部を pager_cache.zig に分割)
新設:  pager_cache.zig     ~150 行 (LRU 部分を pager.zig から抽出)
変更:  database.zig 455 → ~470 行 (BEGIN/COMMIT/ROLLBACK + Database.deinit checkpoint)
```

`pager.zig` は WAL 統合で行数が伸びるため Iter27.A 着手前に LRU 部を `pager_cache.zig` に切り出す (CLAUDE.md "Module Splitting Rules")。

## Consequences

### 利点

- Phase 4 完了時点で sqlite0 は **rollback / atomic commit / multi-reader concurrency** を獲得し、Enterprise Grade DB として最低限の data integrity を満たす。
- B.3.f の「parent-last invariant + best-effort durability」が atomic commit に置き換わり、orphan page / partial freelist update といった残存窓が消える。
- sqlite3 で WAL モード運用している既存 DB を sqlite0 で安全に開ける。
- Phase 5 (CLI 完全互換) で `PRAGMA journal_mode` の集約点が既にあるため迅速に対応可能。

### コスト

- 5 イテレーション。Phase 3 と同等。WAL frame format / checksum / recovery は仕様の bit 単位互換が要求されるため bug surface が大きい。
- Pager の API 不変だが内部 invariant が大きく変わる (page cache が WAL frame と main file の mix を扱う)。既存 unit test の page-level 検証は WAL fixture でも通ることを確認。
- shm 未対応のため複数プロセスから同 DB を開けない (= sqlite3 の `EXCLUSIVE` 相当)。Phase 5 以降で shm 対応。

### 後続 ADR の予告

- **ADR-0008** (任意): shm + multi-process locking — Phase 5 で CLI 完全互換と一緒に。
- **ADR-0009** (任意): `PRAGMA journal_mode=DELETE` (rollback journal) — sqlite3 デフォルトとの互換性が必要になったとき。
- **ADR-0010+** (任意): `CREATE INDEX` (Index B-tree) — Phase 6 と一緒。

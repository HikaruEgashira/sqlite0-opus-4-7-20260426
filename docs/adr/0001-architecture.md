# ADR-0001: nostd解釈・ツールチェーン・初期アーキテクチャ

- Status: Accepted
- Date: 2026-04-26

## Context

CLAUDE.mdは以下を要請している。
- SQLite3互換のEnterprise Gradeデータベース
- Differential Testingで動作保証
- Automated Reasoningで形式的に安全性を証明
- 実装はZig nostdで行う

「Zig nostd」は曖昧な要件である。厳密な解釈はfreestandingビルド (allocator/IO/fmtを自前実装) だが、データベースとして`mmap`/`pread`/ファイルロック等を使う以上、freestandingは現実的でない。

## Decision

### nostd解釈

Zig nostdを以下の規律として運用する。

1. **Zig std libraryは利用可** — allocator/IO/fmtの再実装は行わない。
2. **外部Cライブラリは原則禁止** — libc bindingsは利用してよいが、SQLite/zlib/openssl等の機能依存ライブラリには依存しない。
3. **明示的allocator受け渡し** — グローバルallocatorに依存せず、すべての関数は`std.mem.Allocator`を引数で受け取る。
4. **freestanding-friendly** — `std.heap.PageAllocator`等のOS依存allocatorを直接使わず、呼び出し側からallocatorを注入する設計を保つ。

### ツールチェーン

- **Zig 0.16.0** を採用 (`mise.toml`で固定)。
  - macOS 26 SDKを同梱しており、ホストでネイティブビルド可能。
  - 0.15.x はホスト上で `build_zcu` のリンクに失敗する (libSystem stubが古い)。
- Build runner targetを`os_version_min = 14.0`にして、当面は古いmacOSでも動く成果物を出す。
- 差分テストの参照実装はシステムの`sqlite3` CLI。

### 初期アーキテクチャ

「縦に薄くスライスして毎イテレーション差分テストを通す」方針を採る。横展開 (Tokenizer→Parser→VDBE→Pager) を順番に積み上げると、差分テストが走らない期間が長くなりRalph loopの観測価値を失う。

- Iter1: `SELECT <expr>` (FROM句なし、テーブルなし) を通す最小縦串
- Iter以降: 列・テーブル定義・ストレージ・JOINと順次広げる

### モジュール構成

```
packages/sqlite0/src/
  root.zig    public API (re-exports)
  main.zig    REPL/CLI entry
  value.zig   Value union (NULL/INTEGER/REAL/TEXT/BLOB)
  lex.zig     Tokenizer
  exec.zig    Parser + Evaluator + Result
```

500行ルール (CLAUDE.md) を遵守し、新機能追加時に`exec.zig`を以下に分割する見込み:
- `parser.zig` (構文解析)
- `eval.zig` (式評価)
- `vdbe.zig` (Phase 2以降の仮想マシン)

## Consequences

- 差分テストは1イテレーション目から動く。
- Zig 0.16のI/O API (Init/Io抽象) を初期から取り込むため、後の移行コストがない。
- nostdの厳密解釈は将来別ADRで切る (例: WASMターゲット、組込み利用)。

# sqlite0

## Principal

- SQLite3と完全に互換性のあるEnterprise Gradeなデータベースです。
- Diffential Testingによって動作を保証します。
- Automated Reasoningを用いて形式的に安全性を証明します。
- 実装はZig nostdで行います。それ以外（形式手法やドキュメンテーションなど）は自由に選択して良い。
- trunk based developmentを採用します。
- コード品質は常に最高水準に保ち、適宜リファクタリングを行います。
- 全てを自律的に判断してください。

## Common Commands

```bash
# Build
zig build

# Run REPL
zig build run

# Unit tests
zig build test

# Differential tests against SQLite3
bash tests/differential/run.sh
```

## Content Lifecycle Rules

各ドキュメントは追加だけでなく、定期的に削除・整理する。

### 削除トリガー
1. 実装完了: `docs/ideas` の完了済みアイテムを削除する
2. ADR統合: 後続のADRで意思決定が更新された場合、古いADRの内容を新しいADRに統合して古いものを削除する
3. 知見の陳腐化: `docs/memory` の内容がコードから自明になった場合、または対象コードが削除された場合は削除する
4. スタブ禁止: 1-2行の中身のないファイルは作成しない。具体的な内容がある場合のみファイルを作成する

### 実施タイミング
- 新しいセッション開始時に `docs/` 配下を確認し、不要なファイルを削除する
- コミット前に不要なコンテンツがないか確認する

## Module Splitting Rules

### Trigger: When to split

1. 500行ルール: 1ファイルが500行を超えたら、次の機能追加前に分割する。既存の500行超ファイルは段階的に分割する。**Zig source / Markdown / テスト fixture (cases.txt 等) すべてに同じルールを適用する。**
2. 2責務ルール: 1ファイルに2つ以上の独立した責務（例: 式評価とJOIN実行）が含まれたら分割する。
3. 新機能は既存巨大ファイルに追加しない: 500行超のファイルに新しいpub fnを追加する場合、まず関連コードを別モジュールに抽出してから追加する。

### Boundary: How to decide where to split

- 入力/出力の型で分ける: 同じ型を受け取り同じ型を返す関数群は1モジュールにまとめる
- 呼び出し方向で分ける: AがBを呼ぶがBはAを呼ばないなら、Bは別モジュールにできる
- テスト容易性で分ける: 単体テストを書くとき、モジュール単独でテストできるのが理想
- テスト fixture / Markdown は機能 / iteration 範囲ごとに分ける。ファイル名は zero-padded prefix (`01_`, `02_` …) で順序を保つ。

### Verification

新機能をコミットする前に以下を実行し、500行超のファイルがないことを確認する:

```bash
wc -l packages/*/src/*.zig tests/differential/cases/*.txt docs/**/*.md docs/*.md 2>/dev/null
```

## Current Phase

Check `docs/current_tasks/tasks.md` for active work. Development follows phased approach documented in ADRs.

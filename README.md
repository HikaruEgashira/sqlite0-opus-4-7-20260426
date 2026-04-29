# sqlite0

SQLite3 と完全互換の Enterprise Grade データベース。Zig nostd で実装。Differential Testing で挙動を保証。詳細は `CLAUDE.md` と `docs/adr/`。

```bash
zig build              # build
zig build run          # REPL
zig build test         # unit tests
bash tests/differential/run.sh       # in-memory diff vs sqlite3
bash tests/differential/run_file.sh  # file-mode diff vs sqlite3
```

---

# Development Report — 2026-04-29

直近 50 commit (`Iter30.R` → `Iter31.AK`) の指標を集計し、開発曲線が **長尾化 (long-tail)** しているか可視化する。本リポジトリは autonomous loop で駆動されているため、velocity の絶対値ではなく "どこに掘り続けているか" のシグネチャを観測する必要がある。

## 1. コミット履歴の俯瞰

| 指標 | 値 |
|---|---|
| 解析対象 commits | 50 (最古: `75ba13e` 2026-04-28 08:04 / 最新: `2b14422` 2026-04-29 01:03) |
| 期間 | 約 17 時間 (1 営業日に相当) |
| 総挿入行 | 8,174 |
| 総削除行 | 4,726 |
| ブランチ | `main` のみ |
| タグ | 0 (CLAUDE.md `Workflow` の "tag push で trusted publishing" が未発火) |
| 直 push to main | 50 / 50 (PR 0 件、レビュー 0 件) |
| 履歴上の最古 ADR は 2026-04-26 だが、対応する commit は git に存在しない (Iter1〜29 の lineage が消失) |

## 2. 時間帯別コミット密度 (1h バケット)

```
04-28 08 ██████████  10        ← Iter30.R-Z + Iter31.A
04-28 09 ██████       6        ← Iter31.B-E (+ test infra split)
04-28 10 ███████      7        ← Iter31.F-L
04-28 11 ██           2
04-28 12 ███          3        ← COLLATE 系 (3 連)
04-28 13 █            1
04-28 14 ████         4        ← Iter31.S-V
04-28 15 ███          3        ← Iter31.W-Y (PRAGMA)
04-28 16 ·            0  ──┐
04-28 17 █            1    │ 5h ギャップ (operator 不在?)
04-28 18-21 ·         0  ──┘
04-28 22 ████         4        ← Iter31.Z-AB (CTE 着手)
04-28 23 ████         4        ← Iter31.AC-AF
04-29 00 ████         4        ← Iter31.AG-AJ
04-29 01 █            1        ← Iter31.AK
```

ピークは午前 (10/h)、昼〜夕方は減衰、夜間 22h 以降に第二波。**5h の空白** 以降に話題が「printf/datetime/collate/pragma 細部」から「CTE / 制約」へ突如ジャンプしており、ロードマップ駆動でなく差分テスト駆動 (= 落ちたものから順に修正) であるシグネチャ。

## 3. Iter ラベル長尾分布

```
Iter30.  R S T U V W X Y Z                                 (9 letters)
Iter31.  A B C D E F G H I J K L M N _ _ _ R S T U V W X Y Z
                                              ↑ O,P,Q skip — feat() commit に化けて消失
Iter31.  AA AB AC AD AE AF AG AH AI AJ AK                  (拡張 11 letters)
─────────────────────────────────────────────────────────
Iter31 計 34 サブイテレーション (A→Z 22 + AA→AK 11 + skip 補正)
```

**Iter32 への遷移が起きていない**。Iter31 がアルファベット 1 周を終え 2 周目 (`AA-AK`) に突入している = 「次のフェーズ」が定義できないまま既存差分テストの取り残しを掘り続けている状態。

## 4. コミット規模の分布 (insertions+deletions)

```
<10 lines      ··                                0   合計0%
10-29 lines    ████████                          8   16%
30-99 lines    ███████████████████████          23   46%   ← 中央値帯
100-299 lines  ███████████                      11   22%
300-999 lines  ███████                           7   14%
≥1000 lines    █                                 1    2%
```

中央値帯が 30-99 行 = **微修正の塊**。最大は `ca8ad07` (test infra: cases.txt split) で 3093+/3082- = リファクタの自己ローテートが含まれる。Iter31.AK (UPDATE 側 column-level CHECK) が 67 行で済んでいるのは 31.AJ (INSERT 側 225 行) と非対称で、**最小実装で着地している懸念**。

## 5. 主題分類ヒートマップ (50 commits 分)

```
text / datetime / printf  ████████████████████  19   38%   ← 最大カテゴリ
set/order/distinct/nulls  ████████               8   16%
CTE                       ██████                 6   12%
constraints / conflict    ████                   4    8%
refactor / infra split    ███                    3    6%
PRAGMA                    ███                    3    6%
explicit bugfix           ██                     2    4%
limit/offset              █                      1    2%
LIKE / GLOB               █                      1    2%
collation (postfix op)    █                      1    2%
CLI / REPL                █                      1    2%
```

**40% 弱が文字列/日付/printf の挙動 quirk** を 1 件ずつ後追い修正している。これは sqlite3 の挙動モデルを保持しないまま empirical chase をしている証拠で、**ロングテールの本体**はここ。

## 6. ファイル更新ヒートマップ

```
docs/current_tasks/tasks.md                  ███████████████ 45  ← 90% の commit が触る
tests/differential/cases/08_iter31.txt       ███████          21
packages/sqlite0/src/engine.zig              ████             13
tests/differential/cases.txt (legacy)        ████             12
tests/differential/cases/07_iter30_31.txt    ███              11
packages/sqlite0/src/stmt.zig                ███              11
tests/differential/cases/10_iter31_cte_dml   ██                6
packages/sqlite0/src/select_post.zig         ██                6
packages/sqlite0/src/engine_setop.zig        ██                6
packages/sqlite0/src/engine_dml.zig          ██                6
packages/sqlite0/src/aggregate.zig           ██                6
─────────────────────────────────────────────────
btree*.zig / pager*.zig / wal*.zig / record*.zig         0 commits
engine_dml_file.zig                                       1 commit
engine_ddl_file.zig                                       0 commits
```

直近 50 commit で **Pager / B-tree / WAL / file-mode write path に手が入っていない**。`engine_dml_file.zig` ですら 1 件 (refactor 1 件) のみ。一方、CHECK / UNIQUE / OR IGNORE などの **制約系 5 commit はすべて in-memory パスにのみ実装** されており、永続化パスとの間に **silent divergence** が生じている。

## 7. テスト fixture vs ソース成長率

| 領域 | 直近 50 commit 内の追加行 | HEAD の総行数 |
|---|---|---|
| `tests/differential/cases/*.txt` | +1,084 (07-10 のみ; 01-06 は pre-history) | 3,784 |
| `packages/sqlite0/src/*.zig` | (注: 多くは既存ファイル更新) | 26,384 |
| `tests/differential/cases/`の**file-mode 用**fixture | 0 | (run_file.sh 経由のみ) |

差分テスト fixture 側だけが太り続け、**file-mode 経路への新規 fixture は 50 commit 中ゼロ**。テスト形跡の偏りが in-memory への偏りと一致する。

## 8. ロングテール指標 — まとめ

| 指標 | 観測 | 解釈 |
|---|---|---|
| Iter31 サブレター数 | **34** | 「次の Iter」が定義できず細分化に逃げている |
| 主題で最頻のカテゴリ | text/datetime/printf 38% | 仕様モデル不在の quirk chase |
| 制約系の実装場所 | 100% in-memory | 本番経路 (file-mode) との 二重実装ドリフト |
| btree/pager/wal 触れ度 | 0 commits | Phase 3 後半 / Phase 4 (WAL) 停滞 |
| 中央値コミット規模 | 30-99 行 | tail 修正サイズ |
| 直 push to main | 50 / 50 | レビュー無し、segfault が 30 分間 main に存在 (`Iter31.AI`) |
| `tasks.md` 更新頻度 | 45 / 50 commit | "deferred" が本文に埋没、Issue 化なし |
| tags / releases | 0 | 「実装後 release で動作確認」契約が未履行 |

## 9. 推測される技術負債と次の一手

1. **Critical — file-mode の制約欠落**: `Iter31.AF`〜`AK` で実装した UNIQUE/CHECK/IGNORE/REPLACE は in-memory 限定。永続 DB に整合性違反データを silent に書ける。次イテで `engine_dml_file` / `engine_dml_insert_file` への port を最優先で行う。
2. **Critical — レビュー無しで segfault が main 経由**: `Iter31.AI` が main 上に約 30 分滞在。`zig build test` が unsafe 入力を踏んでいない。fuzz / sanitizer ビルドを CI で常時走らせる必要がある。
3. **High — CI 不在 + 履歴消失**: GitHub Actions 設定なし、tag 0、Iter1〜29 の commit 喪失。trunk-based であるほど、CI が唯一の防衛線になる。最低: `zig build test` + `bash tests/differential/run.sh` + `bash tests/differential/run_file.sh` を PR で必須に。
4. **High — Iter のロングテール脱出**: Iter31.AL に進む前に「Iter31 を閉じる条件」を ADR で固定する。例: 「現在の cases/*.txt を 100% 緑かつ Pager 経路もカバー」。閉じない限り Iter32 (= Phase 4 着手) に進めない。
5. **Medium — 主題 38% が printf/datetime quirk**: sqlite3 の `printf` / `strftime` / `date()` のリファレンス実装挙動表を一度作り、quirk を表駆動で消化する。1 commit = 1 quirk の鎖を断つ。
6. **Medium — Phase 4 (WAL) 凍結**: `wal*.zig` 8 ファイルが存在するが 50 commit 中変更ゼロ。ADR-0007 が現行コードに wire されていない (`pager.zig` の `fsync 無し` は意図的に WAL 待ちのため)。Phase 4 着手しないと durability は永久に未保証のまま。
7. **Low — `tasks.md` から deferred を Issue に外出し**: 「Iter26.B.3 で対応」「Phase 4 で吸収」のような期限付き宿題を GitHub Issues / Project に転記する。本文埋没を解消。

## 10. 観測手法 (再現用)

```bash
# 全 commit を 1 行 1 commit に正規化
GIT_PAGER=cat git log --pretty=tformat:"COMMIT %h %ai %s" --shortstat -n 50 |
  awk '/^COMMIT / {h=$2;ts=$3" "$4;m="";for(i=6;i<=NF;i++)m=m" "$i;next}
       /files? changed/ {ins=del=0;for(i=2;i<=NF;i++){if($i~/insertion/)ins=$(i-1);if($i~/deletion/)del=$(i-1)}
                         print h"|"ts"|"$1"|"ins"|"del"|"m}'

# ファイル更新ヒートマップ
git --no-pager log --name-only --pretty=tformat:"" -n 50 | awk 'NF' | sort | uniq -c | sort -rn

# Iter ラベル抽出
git log --pretty=format:"%s" | grep -oE 'Iter[0-9]+\.[A-Z]+' | sort -u
```

> 本セクションは autonomous な開発レポートとして自動生成された。次回の更新タイミングで、上記 7 つの宿題が Issue 化されているか / file-mode に制約 port が入ったか / `wal*.zig` が wire されたかを差分で再評価する。

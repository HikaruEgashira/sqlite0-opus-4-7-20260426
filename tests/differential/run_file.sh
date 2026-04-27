#!/usr/bin/env bash
# File-mode differential testing (Iter25.B.5+C, ADR-0005 §4): the
# *moment of truth* — sqlite3 generates a real .db file, sqlite0 opens
# it via the Pager + sqlite_schema scanner, and a SELECT must produce
# byte-identical output.
#
# Each case in `file_cases.txt` is a SETUP/QUERY pair separated by a
# blank line. SETUP populates a fresh sqlite3 file; QUERY runs against
# both engines and the outputs are diffed.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
SQLITE0="${ROOT_DIR}/zig-out/bin/sqlite0"
CASES="${ROOT_DIR}/tests/differential/file_cases.txt"

if [[ ! -x "$SQLITE0" ]]; then
  echo "error: $SQLITE0 not found. Run 'zig build' first." >&2
  exit 2
fi
if [[ ! -f "$CASES" ]]; then
  echo "error: $CASES not found." >&2
  exit 2
fi
if ! command -v sqlite3 >/dev/null 2>&1; then
  echo "error: sqlite3 (reference) not found in PATH." >&2
  exit 2
fi

WORK_DIR="$(mktemp -d -t sqlite0-fileharness-XXXXXX)"
trap 'rm -rf "$WORK_DIR"' EXIT

pass=0
fail=0
fails=()
total=0

run_query_case() {
  local setup="$1"
  local query="$2"
  local wal_setup="$3"
  total=$((total + 1))

  local fixture="${WORK_DIR}/case-${total}.db"
  rm -f "$fixture" "${fixture}-journal" "${fixture}-wal" "${fixture}-shm"

  if ! sqlite3 "$fixture" "$setup" 2>/dev/null; then
    fail=$((fail + 1))
    fails+=("SETUP failed (case $total): $setup")
    return
  fi

  # WAL_SETUP runs after SETUP under `.dbconfig no_ckpt_on_close on` so
  # the -wal sidecar persists after sqlite3 exits (Iter27.A read-side
  # fixture mechanism — see ADR-0007 §6 and the spike in Iter27.A).
  # Without no_ckpt_on_close, sqlite3 truncates -wal to 0 bytes on
  # close and there's nothing for sqlite0 to recover.
  if [[ -n "$wal_setup" ]]; then
    if ! sqlite3 -cmd '.dbconfig no_ckpt_on_close on' "$fixture" "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; $wal_setup" >/dev/null 2>&1; then
      fail=$((fail + 1))
      fails+=("WAL_SETUP failed (case $total): $wal_setup")
      return
    fi
  fi

  local expected actual
  expected="$(sqlite3 "$fixture" "$query" 2>&1)" || expected="<error>"
  actual="$("$SQLITE0" -file "$fixture" -c "$query" 2>&1)" || actual="<error>"

  if [[ "$expected" == "$actual" ]]; then
    pass=$((pass + 1))
  else
    fail=$((fail + 1))
    fails+=("Case $total (QUERY)"$'\n'"  SETUP:    $setup"$'\n'"  WAL_SETUP: $wal_setup"$'\n'"  QUERY:    $query"$'\n'"  expected: $expected"$'\n'"  actual:   $actual")
  fi
}

# Write-mutation case: SETUP → clone fixture → sqlite0 mutates A,
# sqlite3 mutates B → run VERIFY against both → diff outputs.
# Also runs `PRAGMA integrity_check` on A (the sqlite0-mutated copy)
# to catch byte-format corruption that VERIFY alone might not surface.
# When `skip_rt` is "0", additionally re-runs VERIFY through sqlite0 on
# fixture A to close the round-trip (sqlite0 must re-read its own
# writes). Set SKIP_RT: 1 in a case to opt out — needed when VERIFY
# uses queries sqlite0 doesn't yet support (e.g. `sqlite_schema` reads,
# which Iter26.A.3 cases rely on but the engine doesn't expose as a
# user table).
run_mutate_case() {
  local setup="$1"
  local mutate="$2"
  local verify="$3"
  local skip_rt="$4"
  total=$((total + 1))

  local fixture_a="${WORK_DIR}/case-${total}-a.db"
  local fixture_b="${WORK_DIR}/case-${total}-b.db"
  rm -f "$fixture_a" "$fixture_b" "${fixture_a}-journal" "${fixture_b}-journal"

  if ! sqlite3 "$fixture_a" "$setup" 2>/dev/null; then
    fail=$((fail + 1)); fails+=("SETUP failed (case $total): $setup"); return
  fi
  cp "$fixture_a" "$fixture_b"

  if ! "$SQLITE0" -file "$fixture_a" -c "$mutate" >/dev/null 2>&1; then
    fail=$((fail + 1)); fails+=("Case $total (MUTATE) sqlite0 failed: $mutate"); return
  fi
  if ! sqlite3 "$fixture_b" "$mutate" >/dev/null 2>&1; then
    fail=$((fail + 1)); fails+=("Case $total (MUTATE) sqlite3 failed: $mutate"); return
  fi

  local actual expected sqlite0_roundtrip
  actual="$(sqlite3 "$fixture_a" "$verify" 2>&1)" || actual="<verify-error>"
  expected="$(sqlite3 "$fixture_b" "$verify" 2>&1)" || expected="<verify-error>"
  # Read-back through sqlite0 closes the round-trip: sqlite0 must be
  # able to read the bytes it just wrote. Iter26.B.1 (balance-deeper)
  # is the first iteration that makes this non-trivial — earlier
  # iterations only mutated single-leaf-root tables that any reader
  # could already handle. Skipped only for cases whose VERIFY uses
  # `sqlite_schema` directly, which sqlite0 doesn't yet expose as a
  # queryable user table.
  if [[ "$skip_rt" != "1" ]]; then
    sqlite0_roundtrip="$("$SQLITE0" -file "$fixture_a" -c "$verify" 2>&1)" || sqlite0_roundtrip="<sqlite0-verify-error>"
  else
    sqlite0_roundtrip="$expected"
  fi

  local integrity
  integrity="$(sqlite3 "$fixture_a" 'PRAGMA integrity_check' 2>&1)" || integrity="<integrity-error>"

  if [[ "$expected" == "$actual" && "$integrity" == "ok" && "$expected" == "$sqlite0_roundtrip" ]]; then
    pass=$((pass + 1))
  else
    fail=$((fail + 1))
    fails+=("Case $total (MUTATE)"$'\n'"  SETUP:     $setup"$'\n'"  MUTATE:    $mutate"$'\n'"  VERIFY:    $verify"$'\n'"  expected:  $expected"$'\n'"  actual:    $actual"$'\n'"  sqlite0_rt: $sqlite0_roundtrip"$'\n'"  integrity: $integrity")
  fi
}

setup=""
query=""
wal_setup=""
mutate=""
verify=""
skip_rt="0"
flush_case() {
  if [[ -n "$setup" ]]; then
    if [[ -n "$query" ]]; then
      run_query_case "$setup" "$query" "$wal_setup"
    elif [[ -n "$mutate" && -n "$verify" ]]; then
      run_mutate_case "$setup" "$mutate" "$verify" "$skip_rt"
    fi
  fi
  setup=""; query=""; wal_setup=""; mutate=""; verify=""; skip_rt="0"
}

while IFS= read -r line || [[ -n "$line" ]]; do
  case "$line" in
    '#'*) continue ;;
    '') flush_case ;;
    'SETUP: '*)     setup="${line#SETUP: }" ;;
    'WAL_SETUP: '*) wal_setup="${line#WAL_SETUP: }" ;;
    'QUERY: '*)     query="${line#QUERY: }" ;;
    'MUTATE: '*)    mutate="${line#MUTATE: }" ;;
    'VERIFY: '*)    verify="${line#VERIFY: }" ;;
    'SKIP_RT: '*)   skip_rt="${line#SKIP_RT: }" ;;
    *)
      echo "error: unrecognised line in $CASES: $line" >&2
      exit 2
      ;;
  esac
done < "$CASES"
flush_case

echo "file-differential: ${pass}/${total} passed (${fail} failed)"
if (( fail > 0 )); then
  echo
  printf 'FAILURES:\n'
  for f in "${fails[@]}"; do
    printf '%s\n\n' "$f"
  done
  exit 1
fi

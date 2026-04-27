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

run_case() {
  local setup="$1"
  local query="$2"
  total=$((total + 1))

  local fixture="${WORK_DIR}/case-${total}.db"
  rm -f "$fixture" "${fixture}-journal" "${fixture}-wal" "${fixture}-shm"

  # Build the fixture. sqlite3 exits before we touch the file with
  # sqlite0, so the flock contention discussed in ADR-0005 §3 doesn't
  # bite. Default journal mode is rollback; no -wal/-shm files appear.
  if ! sqlite3 "$fixture" "$setup" 2>/dev/null; then
    fail=$((fail + 1))
    fails+=("SETUP failed (case $total): $setup")
    return
  fi

  local expected actual
  expected="$(sqlite3 "$fixture" "$query" 2>&1)" || expected="<error>"
  actual="$("$SQLITE0" -file "$fixture" -c "$query" 2>&1)" || actual="<error>"

  if [[ "$expected" == "$actual" ]]; then
    pass=$((pass + 1))
  else
    fail=$((fail + 1))
    fails+=("Case $total"$'\n'"  SETUP:    $setup"$'\n'"  QUERY:    $query"$'\n'"  expected: $expected"$'\n'"  actual:   $actual")
  fi
}

setup=""
query=""
flush_case() {
  if [[ -n "$setup" && -n "$query" ]]; then
    run_case "$setup" "$query"
  fi
  setup=""
  query=""
}

while IFS= read -r line || [[ -n "$line" ]]; do
  case "$line" in
    '#'*) continue ;;
    '') flush_case ;;
    'SETUP: '*) setup="${line#SETUP: }" ;;
    'QUERY: '*) query="${line#QUERY: }" ;;
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

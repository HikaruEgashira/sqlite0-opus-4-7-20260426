#!/usr/bin/env bash
# Differential testing: compare sqlite0 against SQLite3 reference.
# Each line in `cases.txt` is a SQL statement that both engines must agree on.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
SQLITE0="${ROOT_DIR}/zig-out/bin/sqlite0"
CASES="${ROOT_DIR}/tests/differential/cases.txt"

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

pass=0
fail=0
fails=()
total=0

while IFS= read -r line || [[ -n "$line" ]]; do
  case "$line" in
    ''|'#'*) continue ;;
  esac
  total=$((total + 1))

  expected="$(sqlite3 :memory: "$line" 2>&1)" || expected="<error>"
  actual="$("$SQLITE0" -c "$line" 2>&1)" || actual="<error>"

  if [[ "$expected" == "$actual" ]]; then
    pass=$((pass + 1))
  else
    fail=$((fail + 1))
    fails+=("SQL: $line"$'\n'"  expected: $expected"$'\n'"  actual:   $actual")
  fi
done < "$CASES"

echo "differential: ${pass}/${total} passed (${fail} failed)"
if (( fail > 0 )); then
  echo
  printf 'FAILURES:\n'
  for f in "${fails[@]}"; do
    printf '%s\n\n' "$f"
  done
  exit 1
fi

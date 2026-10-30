#!/usr/bin/env bash
# Check that soxai_data behaves the same on two Python versions. Not part of CI.
#
# For each version a uv venv is created under .compat/venv-<version>, the package is
# installed with its latest compatible dependencies, and the following is collected
# into .compat/<timestamp>/<version>/:
#
#   1. offline.xml     : the offline test suite (also measured with branch coverage)
#   2. integration.xml : the integration tests (skipped without credentials)
#   3. snapshot.json   : the outputs of snapshot.py, compared value by value
#   4. warnings.xml    : the offline suite with DeprecationWarning / FutureWarning as errors
#
# The exit code of every pytest run is kept next to its report as <name>.exit, so that a
# run that crashed before writing a report cannot pass for a clean one.
#
# compare.py then prints the differences and exits with 1 if there are any.
#
# usage: scripts/compat/run_compat_check.sh [BASE_VERSION] [NEW_VERSION]
#        (defaults: 3.11 3.14, the versions CI tests. both have to satisfy requires-python.
#        SOXAI_API_TOKEN, SOXAI_UID and optionally SOXAI_API_URL enable the live checks)

set -uo pipefail

BASE_VERSION="${1:-3.11}"
NEW_VERSION="${2:-3.14}"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
OUT="$ROOT/.compat/$(date +%Y%m%d_%H%M%S)"

cd "$ROOT" || exit 1

for version in "$BASE_VERSION" "$NEW_VERSION"; do
  venv="$ROOT/.compat/venv-$version"
  result="$OUT/$version"
  py="$venv/bin/python"
  mkdir -p "$result"
  echo "=== python $version ==="

  if [ ! -x "$py" ]; then
    uv venv --python "$version" "$venv" || exit 1
  fi
  # --upgrade so that each version gets the newest dependencies it supports, the same
  # as a fresh CI run would
  uv pip install --python "$py" --upgrade -e ".[dev]" coverage || exit 1
  uv pip freeze --python "$py" > "$result/freeze.txt"

  echo "--- 1. offline tests (with coverage)"
  "$py" -m coverage run --branch --source soxai_data --data-file "$result/.coverage" \
    -m pytest test -q -p no:cacheprovider --junitxml "$result/offline.xml" > "$result/offline.log" 2>&1
  echo $? > "$result/offline.exit"
  "$py" -m coverage report -m --data-file "$result/.coverage" > "$result/coverage.txt"
  tail -n 1 "$result/offline.log"

  echo "--- 2. integration tests"
  "$py" -m pytest test -m integration -q -rs -p no:cacheprovider \
    --junitxml "$result/integration.xml" > "$result/integration.log" 2>&1
  echo $? > "$result/integration.exit"
  tail -n 1 "$result/integration.log"

  echo "--- 3. snapshot"
  "$py" scripts/compat/snapshot.py --out "$result/snapshot.json"

  echo "--- 4. offline tests with warnings as errors"
  "$py" -m pytest test -q -p no:cacheprovider \
    -W error::DeprecationWarning -W error::FutureWarning \
    --junitxml "$result/warnings.xml" > "$result/warnings.log" 2>&1
  echo $? > "$result/warnings.exit"
  tail -n 1 "$result/warnings.log"
done

echo
python3 scripts/compat/compare.py "$OUT/$BASE_VERSION" "$OUT/$NEW_VERSION" | tee "$OUT/report.txt"
status=${PIPESTATUS[0]}
echo
echo "coverage ($NEW_VERSION): $OUT/$NEW_VERSION/coverage.txt"
echo "report: $OUT/report.txt"
exit "$status"

#!/usr/bin/env bash
set -e

COVERAGE_BIN="${COVERAGE_BIN:-.venv/bin/coverage}"
if [ ! -x "$COVERAGE_BIN" ]; then
    COVERAGE_BIN="${COVERAGE_BIN_FALLBACK:-coverage}"
fi

# First run: use .env
ENV_FILE=.env_fms17 "$COVERAGE_BIN" run --data-file=.coverage.env_fms17 \
    -m unittest discover -s tests -t tests

## 2nd run: use .env2
ENV_FILE=.env_fms22 "$COVERAGE_BIN" run --data-file=.coverage.env_fms22 \
    -m unittest discover -s tests -t tests

"$COVERAGE_BIN" combine .coverage.env_fms17 .coverage.env_fms22
"$COVERAGE_BIN" report -m
"$COVERAGE_BIN" html  # optional: generates htmlcov/index.html
"$COVERAGE_BIN" xml -o coverage.xml

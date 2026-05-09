#!/usr/bin/env bash
set -euo pipefail

ROOT="/Users/chenyunwen/proj/postgres"
PSQL="${PSQL:-$ROOT/installed/bin/psql}"
HOST="${HOST:-/tmp}"
PORT="${PORT:-5432}"
DB="${DB:-tpch}"
SQL_FILE="${SQL_FILE:-$ROOT/contrib/pg_volvec/sql/q6_tpch_pg_volvec.sql}"

if [[ ! -x "$PSQL" ]]; then
  printf 'psql binary not found or not executable: %s\n' "$PSQL" >&2
  exit 1
fi

if [[ ! -f "$SQL_FILE" ]]; then
  printf 'SQL file not found: %s\n' "$SQL_FILE" >&2
  exit 1
fi

exec "$PSQL" -h "$HOST" -p "$PORT" -d "$DB" -v ON_ERROR_STOP=1 -f "$SQL_FILE"

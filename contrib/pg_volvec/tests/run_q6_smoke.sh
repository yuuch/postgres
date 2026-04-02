#!/bin/sh
set -eu

PSQL_BIN="${PGVEC_PSQL:-/Users/chenyunwen/proj/postgres/19dev_install/bin/psql}"
PGHOST_VALUE="${PGVEC_HOST:-/Users/chenyunwen/proj/postgres/19dev_install/run}"
PGPORT_VALUE="${PGVEC_PORT:-55439}"
PGDATABASE_VALUE="${PGVEC_DB:-postgres}"
PGUSER_VALUE="${PGVEC_USER:-postgres}"
SQL_FILE="${PGVEC_SQL:-/Users/chenyunwen/proj/postgres/contrib/pg_vec/tests/q6_smoke.sql}"
EXPECTED="${PGVEC_EXPECTED:-16.0000}"

RESULT="$("$PSQL_BIN" \
  -h "$PGHOST_VALUE" \
  -p "$PGPORT_VALUE" \
  -U "$PGUSER_VALUE" \
  -d "$PGDATABASE_VALUE" \
  -X \
  -v ON_ERROR_STOP=1 \
  -f "$SQL_FILE")"

RESULT="$(printf '%s' "$RESULT" | tr -d '\r' | tail -n 1)"

if [ "$RESULT" != "$EXPECTED" ]; then
  printf 'pg_vec Q6 smoke failed: expected %s but got %s\n' "$EXPECTED" "$RESULT" >&2
  exit 1
fi

printf 'pg_vec Q6 smoke ok: %s\n' "$RESULT"

#!/bin/zsh

set -euo pipefail

if [[ $# -lt 4 ]]; then
  echo "usage: $0 <psql_bin> <host> <port> <db> [query ...]" >&2
  exit 1
fi

PSQL_BIN=$1
HOST=$2
PORT=$3
DB=$4
shift 4

SCRIPT_DIR=/Users/chenyunwen/proj/postgres/tpch-dbgen

if [[ $# -gt 0 ]]; then
  QUERIES=("$@")
else
  QUERIES=(q1 q6 q14)
fi

run_one() {
  local query_name=$1
  local mode=$2
  local tmp_sql
  local raw_output
  local exec_time

  tmp_sql=$(mktemp /tmp/pgvec_bench.XXXXXX)

  {
    printf "LOAD \$\$pg_vec\$\$;\n"
    printf "SET client_min_messages=error;\n"
    printf "SET max_parallel_workers_per_gather=0;\n"
    printf "SET max_parallel_workers=0;\n"
    printf "SET jit=off;\n"
    printf "SET pg_vec.enabled=%s;\n" "$mode"
    printf "SET pg_vec.trace_hooks=off;\n"
    printf "EXPLAIN (ANALYZE, TIMING OFF, BUFFERS OFF, SUMMARY ON)\n"
    cat "$SCRIPT_DIR/$query_name.sql"
  } > "$tmp_sql"

  raw_output=$("$PSQL_BIN" \
    -h "$HOST" \
    -p "$PORT" \
    -d "$DB" \
    -v ON_ERROR_STOP=1 \
    -qAtf "$tmp_sql" 2>&1)

  exec_time=$(printf "%s\n" "$raw_output" |
    sed -n 's/^Execution Time: //p' |
    tail -n 1)

  if [[ -z "$exec_time" ]]; then
    printf "%s\n" "$raw_output" >&2
  else
    printf "%s\n" "$exec_time"
  fi

  rm -f "$tmp_sql"
}

for query_name in "${QUERIES[@]}"; do
  echo "== $query_name =="
  for mode in off on off on off on; do
    printf "%s\t%s\n" "$mode" "$(run_one "$query_name" "$mode")"
  done
done

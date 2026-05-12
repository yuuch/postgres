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

SCRIPT_DIR=/Users/chenyunwen/proj/postgres/contrib/pg_carbon/tests/tpch

if [[ $# -gt 0 ]]; then
  QUERIES=("$@")
else
  QUERIES=(q1 q3 q6 q12 q14 q19)
fi

run_one() {
  local query_name=$1
  local mode=$2
  local tmp_sql
  local raw_output
  local exec_time

  tmp_sql=$(mktemp /tmp/pgvec_bench.XXXXXX)

  {
    printf "LOAD 'pg_vec';\n"
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
    tail -n 1 |
    sed 's/ ms$//')

  rm -f "$tmp_sql"

  if [[ -z "$exec_time" ]]; then
    printf "%s\n" "$raw_output" >&2
    return 1
  fi

  printf "%s\n" "$exec_time"
}

median_of_three() {
  printf "%s\n" "$@" | sort -g | sed -n '2p'
}

summary_line() {
  local off_med=$1
  local on_med=$2

  awk -v off="$off_med" -v on="$on_med" 'BEGIN {
    diff = (off - on) / off * 100.0;
    if (diff >= 0)
      printf "%.3f\t%.3f\tfaster %.1f%%\n", off, on, diff;
    else
      printf "%.3f\t%.3f\tslower %.1f%%\n", off, on, -diff;
  }'
}

for query_name in "${QUERIES[@]}"; do
  off_times=()
  on_times=()

  echo "== $query_name =="
  for mode in off on off on off on; do
    t=$(run_one "$query_name" "$mode")
    printf "%s\t%s\n" "$mode" "$t"
    if [[ "$mode" == off ]]; then
      off_times+=("$t")
    else
      on_times+=("$t")
    fi
  done

  off_med=$(median_of_three "${off_times[@]}")
  on_med=$(median_of_three "${on_times[@]}")
  printf "median\t"
  summary_line "$off_med" "$on_med"
  echo
done

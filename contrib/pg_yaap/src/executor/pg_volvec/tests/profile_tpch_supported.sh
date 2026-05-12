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
OUT_DIR=/Users/chenyunwen/proj/postgres/contrib/pg_vec/tests
FG_DIR=/Users/chenyunwen/proj/postgres/FlameGraph

queries=("$@")
if [[ ${#queries[@]} -eq 0 ]]; then
  queries=(q12 q14 q19)
fi

for q in "${queries[@]}"; do
  sql=$(mktemp /tmp/pgvec_profile.XXXXXX)
  out=$(mktemp /tmp/pgvec_profile_out.XXXXXX)
  sample_txt="$OUT_DIR/${q}_pgvec_on.sample.txt"
  folded_txt="$OUT_DIR/${q}_pgvec_on.folded.txt"
  flame_svg="$OUT_DIR/${q}_pgvec_on.flame.svg"

  {
    printf "LOAD 'pg_vec';\n"
    printf "SET client_min_messages=error;\n"
    printf "SET max_parallel_workers_per_gather=0;\n"
    printf "SET max_parallel_workers=0;\n"
    printf "SET jit=off;\n"
    printf "SET pg_vec.enabled=on;\n"
    printf "SELECT pg_backend_pid();\n"
    printf "SELECT pg_sleep(1);\n"
    printf "EXPLAIN (ANALYZE, TIMING OFF, BUFFERS OFF, SUMMARY ON)\n"
    cat "$SCRIPT_DIR/${q}.sql"
  } > "$sql"

  "$PSQL_BIN" -h "$HOST" -p "$PORT" -d "$DB" -v ON_ERROR_STOP=1 -qAtf "$sql" > "$out" 2>&1 &
  psql_pid=$!
  backend_pid=""

  for _ in $(seq 1 200); do
    if [[ -s "$out" ]]; then
      backend_pid=$(head -n 1 "$out" | tr -d '[:space:]')
      if [[ "$backend_pid" =~ ^[0-9]+$ ]]; then
        break
      fi
    fi
    sleep 0.05
  done

  if [[ -z "$backend_pid" ]]; then
    echo "failed to capture backend pid for $q" >&2
    cat "$out" >&2 || true
    wait "$psql_pid" || true
    rm -f "$sql" "$out"
    exit 1
  fi

  /usr/bin/sample "$backend_pid" 5 10 -mayDie -file "$sample_txt" >/dev/null 2>&1 || true
  wait "$psql_pid"

  awk -f "$FG_DIR/stackcollapse-sample.awk" "$sample_txt" > "$folded_txt"
  perl "$FG_DIR/flamegraph.pl" "$folded_txt" > "$flame_svg"

  exec_time=$(sed -n 's/^Execution Time: //p' "$out" | tail -n 1 | sed 's/ ms$//')
  printf "%s\t%s\t%s\t%s\n" "$q" "$exec_time" "$sample_txt" "$flame_svg"

  rm -f "$sql" "$out"
done

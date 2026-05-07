#!/bin/bash
set -euo pipefail

PSQL="/Users/chenyunwen/proj/postgres/installed/bin/psql"
HOST="/tmp"
PORT="5432"
DB="tpch"
SQL_DIR="/Users/chenyunwen/proj/postgres/contrib/pg_volvec/profiles/xctrace_parallel_samples_20260413_121843"

QUERIES=(1 3 4 5 6 7 8 9 10 11 12 13 14 15 16 18 19 20 22)

OUT_DIR="/Users/chenyunwen/proj/postgres/contrib/pg_volvec/profiles/bench_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$OUT_DIR"

get_time_limit() {
  case "$1" in
    18|20) echo 120 ;;
    8|9|12|13) echo 60 ;;
    *) echo 45 ;;
  esac
}

# Extract just the query from the profiling SQL (after EXPLAIN line)
extract_query() {
  local qnum="$1"
  local src="$SQL_DIR/q${qnum}/q${qnum}.sql"
  # Get everything after the EXPLAIN line
  sed -n '/^EXPLAIN (ANALYZE,/,$ p' "$src" | sed '1d'
}

run_query() {
  local mode="$1"
  local qnum="$2"
  local query
  query=$(extract_query "$qnum")
  if [[ -z "$query" ]]; then
    echo "SKIP"
    return
  fi

  local tlim
  tlim=$(get_time_limit "$qnum")
  local wrapped="$OUT_DIR/${mode}_q${qnum}.sql"

  # Q15 needs revenue0 view setup
  local q15_setup=""
  if [[ "$qnum" == "15" ]]; then
    q15_setup="DROP VIEW IF EXISTS revenue0;
CREATE VIEW revenue0 (supplier_no, total_revenue) AS
  SELECT l_suppkey, sum(l_extendedprice * (1 - l_discount))
  FROM lineitem
  WHERE l_shipdate >= date '1996-01-01'
    AND l_shipdate < date '1996-01-01' + interval '3' month
  GROUP BY l_suppkey;;"
  fi

  {
    if [[ "$mode" == "native" ]]; then
      echo "SET pg_volvec.enabled=off;"
      echo "SET max_parallel_workers_per_gather=0;"
      echo "SET max_parallel_workers=0;"
      echo "SET min_parallel_table_scan_size='1000GB';"
      echo "SET parallel_setup_cost=1000000000;"
      echo "SET parallel_tuple_cost=1000000000;"
    elif [[ "$mode" == "serial" ]]; then
      echo "LOAD 'pg_volvec';"
      echo "SET client_min_messages=error;"
      echo "SET jit=off;"
      echo "SET enable_eager_aggregate=off;"
      echo "SET duckdb.force_execution=off;"
      echo "SET pg_volvec.enabled=on;"
      echo "SET pg_volvec.parallel=off;"
      echo "SET max_parallel_workers_per_gather=0;"
      echo "SET max_parallel_workers=0;"
      echo "SET min_parallel_table_scan_size='1000GB';"
      echo "SET parallel_setup_cost=1000000000;"
      echo "SET parallel_tuple_cost=1000000000;"
    else
      echo "LOAD 'pg_volvec';"
      echo "SET client_min_messages=error;"
      echo "SET jit=off;"
      echo "SET enable_eager_aggregate=off;"
      echo "SET duckdb.force_execution=off;"
      echo "SET pg_volvec.enabled=on;"
      echo "SET pg_volvec.parallel=on;"
      echo "SET pg_volvec.parallel_max_workers=4;"
      echo "SET pg_volvec.parallel_leader_participation=off;"
      echo "SET max_parallel_workers=8;"
      echo "SET max_parallel_workers_per_gather=0;"
    fi

    if [[ -n "$q15_setup" ]]; then
      echo "$q15_setup"
    fi

    echo "EXPLAIN ANALYZE"
    echo "$query"
    if [[ "$qnum" == "15" ]]; then
      echo "DROP VIEW IF EXISTS revenue0;"
    fi
  } > "$wrapped"

  local result
  result=$("$PSQL" -h "$HOST" -p "$PORT" -d "$DB" \
    -v ON_ERROR_STOP=1 -Atf "$wrapped" 2>&1) || true

  local exec_time=""
  exec_time=$(echo "$result" | grep -E '^Execution Time:' | tail -1 | sed 's/Execution Time: //' | sed 's/ ms$//') || true
  if [[ -z "$exec_time" ]]; then
    exec_time="ERROR"
  fi

  echo "$exec_time"
}

echo "=== pg_volvec Benchmark ==="
echo "Output: $OUT_DIR"
echo ""

printf "%-6s %12s %12s %12s\n" "Query" "Native(ms)" "Serial(ms)" "Parallel(ms)"
printf "%-6s %12s %12s %12s\n" "-----" "----------" "----------" "------------"

> "$OUT_DIR/results.txt"

for q in "${QUERIES[@]}"; do
  line=$(printf "%-6s" "Q${q}")

  native_t=$(run_query "native" "$q")
  line=$(printf "%s %12s" "$line" "$native_t")

  serial_t=$(run_query "serial" "$q")
  line=$(printf "%s %12s" "$line" "$serial_t")

  parallel_t=$(run_query "parallel" "$q")
  line=$(printf "%s %12s" "$line" "$parallel_t")

  echo "$line" | tee -a "$OUT_DIR/results.txt"
done

echo ""
echo "=== Done ==="
echo "Results: $OUT_DIR/results.txt"

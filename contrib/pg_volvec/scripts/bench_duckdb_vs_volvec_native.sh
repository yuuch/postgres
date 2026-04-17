#!/bin/bash
# Three-way TPC-H benchmark: PG native vs pg_duckdb vs pg_volvec
set -uo pipefail

PSQL="/Users/chenyunwen/proj/postgres/installed/bin/psql"
QUERIES_DIR="/Users/chenyunwen/proj/postgres/contrib/pg_volvec/tpch_queries"
DB_NAME="tpch"

# Check if directory exists
if [ ! -d "$QUERIES_DIR" ]; then
    echo "Error: Directory $QUERIES_DIR does not exist."
    exit 1
fi

cd "$QUERIES_DIR" || exit 1
TS=$(date +%Y%m%d_%H%M%S)
OUT="/Users/chenyunwen/proj/postgres/contrib/pg_volvec/tpch_perf_three_way_duckdb_${TS}.tsv"

# Skipping 2 and 21 as noted in your process-parallel checkpoint if needed, but array contains all 22
QUERIES=(1 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22)
RUNS=3
TIMEOUT_SEC=180 # Plotted as 180s timeout in your snapshot

echo -e "query\tpg_native_s\tpg_native_status\tpg_duckdb_s\tpg_duckdb_status\tpg_volvec_s\tpg_volvec_status" > "$OUT"

build_sql() {
    local qnum=$1
    local mode=$2
    local tmp_sql
    tmp_sql=$(mktemp /tmp/tpch_bench_XXXXXX)

    if [[ "$mode" == "native" ]]; then
        {
            printf "SET client_min_messages=error;\n"
            printf "SET max_parallel_workers_per_gather=14;\n"
            printf "SET max_parallel_workers=14;\n"
            printf "SET jit=off;\n"
            # Ensure volvec and duckdb are off
            printf "SET pg_volvec.enabled=off;\n"
            printf "SET duckdb.execution=off;\n" 2>/dev/null || true
            cat "q${qnum}.sql"
        } > "$tmp_sql"
    elif [[ "$mode" == "duckdb" ]]; then
        {
            printf "SET client_min_messages=error;\n"
            printf "LOAD 'pg_duckdb';\n"
            # DuckDB settings based on pg_duckdb README
            printf "SET duckdb.execution=on;\n"
            printf "SET max_parallel_workers=14;\n"
            printf "SET max_parallel_workers_per_gather=14;\n"
            printf "SET duckdb.max_workers_per_postgres_scan=14;\n"
            printf "SET duckdb.threads_for_postgres_scan=14;\n"
            printf "SET pg_volvec.enabled=off;\n"
            cat "q${qnum}.sql"
        } > "$tmp_sql"
    elif [[ "$mode" == "volvec" ]]; then
        {
            printf "SET client_min_messages=error;\n"
            printf "LOAD 'pg_volvec';\n"
            # pg_volvec specific tuning for process-parallel based on morsel design
            printf "SET max_parallel_workers_per_gather=0;\n"
            printf "SET max_parallel_workers=14;\n"
            printf "SET pg_volvec.enabled=on;\n"
            printf "SET pg_volvec.parallel=on;\n"
            printf "SET pg_volvec.parallel_max_workers=14;\n"
            printf "SET duckdb.execution=off;\n" 2>/dev/null || true
            # For Q3, disable eager aggregate to match the volvec lowering shape
            if [[ "$qnum" == "3" ]]; then
                printf "SET enable_eager_aggregate=off;\n"
            fi
            # For Q12, hash-join process-parallel check (from notes, may need parallel_leader_participation=off)
            if [[ "$qnum" == "12" ]]; then
                printf "SET parallel_leader_participation=off;\n"
            fi
            cat "q${qnum}.sql"
        } > "$tmp_sql"
    fi
    echo "$tmp_sql"
}

run_one_query() {
    local tmp_sql=$1
    local start_ms end_ms elapsed_ms
    start_ms=$(python3 -c "import time; print(int(time.time()*1000))")

    local raw_output rc
    local tmp_out
    tmp_out=$(mktemp /tmp/tpch_query_XXXXXX)

    # Run psql in background, capture output + exit code
    ( "$PSQL" -h /tmp -p 5432 -d "$DB_NAME" \
        -v ON_ERROR_STOP=1 -qAt -f "$tmp_sql" > "$tmp_out" 2>&1; echo $? > "${tmp_out}.rc" ) &
    local psql_pid=$!

    # Wait with timeout
    local waited=0
    while kill -0 "$psql_pid" 2>/dev/null; do
        sleep 1
        waited=$(( waited + 1 ))
        if [[ $waited -ge $TIMEOUT_SEC ]]; then
            # Timeout hit
            kill -9 "$psql_pid" 2>/dev/null
            wait "$psql_pid" 2>/dev/null
            rm -f "$tmp_out" "${tmp_out}.rc"
            # Return max timeout in ms
            echo "$((TIMEOUT_SEC * 1000))"
            return
        fi
    done
    wait "$psql_pid" 2>/dev/null
    end_ms=$(python3 -c "import time; print(int(time.time()*1000))")
    elapsed_ms=$(( end_ms - start_ms ))
    rc=$(cat "${tmp_out}.rc" 2>/dev/null || echo "1")
    raw_output=$(cat "$tmp_out" 2>/dev/null)
    rm -f "$tmp_out" "${tmp_out}.rc"

    if [[ $rc -ne 0 ]]; then
        echo "error"
        return
    fi

    echo "$elapsed_ms"
}

median_of_3() {
    echo -e "$1\n$2\n$3" | sort -n | sed -n '2p'
}

ms_to_s() {
    if [[ "$1" == "error" ]]; then
        echo "error"
    else
        python3 -c "v=$1; print(f'{v/1000:.3f}')"
    fi
}

for q in "${QUERIES[@]}"; do
    echo "--- Q${q} ---"

    sql_native=$(build_sql "$q" "native")
    sql_duckdb=$(build_sql "$q" "duckdb")
    sql_volvec=$(build_sql "$q" "volvec")

    pg_times=()
    duckdb_times=()
    volvec_times=()

    for run in $(seq 1 $RUNS); do
        printf "  run %d: " "$run"

        t=$(run_one_query "$sql_native")
        printf "PG=%s " "$t"
        pg_times+=("$t")

        t=$(run_one_query "$sql_duckdb")
        printf "DuckDB=%s " "$t"
        duckdb_times+=("$t")

        t=$(run_one_query "$sql_volvec")
        printf "VolVec=%s\n" "$t"
        volvec_times+=("$t")
    done

    pg_med=$(median_of_3 "${pg_times[0]}" "${pg_times[1]}" "${pg_times[2]}")
    duckdb_med=$(median_of_3 "${duckdb_times[0]}" "${duckdb_times[1]}" "${duckdb_times[2]}")
    volvec_med=$(median_of_3 "${volvec_times[0]}" "${volvec_times[1]}" "${volvec_times[2]}")

    pg_sec=$(ms_to_s "$pg_med")
    duckdb_sec=$(ms_to_s "$duckdb_med")
    volvec_sec=$(ms_to_s "$volvec_med")

    pg_status="ok"; [[ "$pg_med" == "error" ]] && pg_status="error"
    duckdb_status="ok"; [[ "$duckdb_med" == "error" ]] && duckdb_status="error"
    volvec_status="ok"; [[ "$volvec_med" == "error" ]] && volvec_status="error"

    echo -e "q${q}\t${pg_sec}\t${pg_status}\t${duckdb_sec}\t${duckdb_status}\t${volvec_sec}\t${volvec_status}" >> "$OUT"

    printf "  Median: PG: %ss  |  DuckDB: %ss  |  VolVec: %ss\n" \
        "$pg_sec" "$duckdb_sec" "$volvec_sec"

    rm -f "$sql_native" "$sql_duckdb" "$sql_volvec"
done

echo ""
echo "Results saved to: $OUT"
echo ""
column -t -s$'\t' "$OUT"
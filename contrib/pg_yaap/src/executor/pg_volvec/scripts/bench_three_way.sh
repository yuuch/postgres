#!/bin/bash
# Three-way TPC-H benchmark: PG native (6 parallel) vs pg_volvec parallel vs pg_volvec serial
set -uo pipefail

PSQL="/Users/chenyunwen/proj/postgres/installed/bin/psql"
QUERIES_DIR="/Users/chenyunwen/proj/postgres/contrib/pg_volvec/tpch_queries"
cd "$QUERIES_DIR"
TS=$(date +%Y%m%d_%H%M%S)
OUT="/Users/chenyunwen/proj/postgres/contrib/pg_volvec/tpch_perf_three_way_${TS}.tsv"

QUERIES=(1 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22)
RUNS=3
TIMEOUT_SEC=300

echo -e "query\tpg_parallel_6s\tpg_status\tpg_volvec_parallel_s\tpg_volvec_parallel_status\tpg_volvec_serial_s\tpg_volvec_serial_status" > "$OUT"

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
            printf "SET pg_volvec.enabled=off;\n"
            cat "q${qnum}.sql"
        } > "$tmp_sql"
    elif [[ "$mode" == "volvec_parallel" ]]; then
        {
            printf "SET client_min_messages=error;\n"
            printf "SET max_parallel_workers_per_gather=0;\n"
            printf "SET max_parallel_maintenance_workers=0;\n"
            printf "SET min_parallel_table_scan_size='1000GB';\n"
            printf "SET min_parallel_index_scan_size='1000GB';\n"
            printf "SET parallel_setup_cost=1000000000;\n"
            printf "SET parallel_tuple_cost=1000000000;\n"
            printf "SET jit=off;\n"
            printf "SET pg_volvec.enabled=on;\n"
            printf "SET pg_volvec.parallel=on;\n"
            printf "SET pg_volvec.trace_hooks=off;\n"
            printf "SET pg_volvec.parallel_max_workers=14;\n"
            printf "SET max_parallel_workers=14;\n"
            cat "q${qnum}.sql"
        } > "$tmp_sql"
    else
        {
            printf "SET client_min_messages=error;\n"
            printf "SET max_parallel_workers_per_gather=0;\n"
            printf "SET max_parallel_workers=0;\n"
            printf "SET max_parallel_maintenance_workers=0;\n"
            printf "SET min_parallel_table_scan_size='1000GB';\n"
            printf "SET min_parallel_index_scan_size='1000GB';\n"
            printf "SET parallel_setup_cost=1000000000;\n"
            printf "SET parallel_tuple_cost=1000000000;\n"
            printf "SET jit=off;\n"
            printf "SET pg_volvec.enabled=on;\n"
            printf "SET pg_volvec.trace_hooks=off;\n"
            printf "SET pg_volvec.parallel_max_workers=0;\n"
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
    ( "$PSQL" -h /tmp -p 5432 -d tpch \
        -v ON_ERROR_STOP=1 -qAt -f "$tmp_sql" > "$tmp_out" 2>&1; echo $? > "${tmp_out}.rc" ) &
    local psql_pid=$!

    # Wait with timeout
    local waited=0
    while kill -0 "$psql_pid" 2>/dev/null; do
        sleep 1
        waited=$(( waited + 1 ))
        if [[ $waited -ge $TIMEOUT_SEC ]]; then
            kill -9 "$psql_pid" 2>/dev/null
            wait "$psql_pid" 2>/dev/null
            rm -f "$tmp_out" "${tmp_out}.rc"
            end_ms=$(python3 -c "import time; print(int(time.time()*1000))")
            elapsed_ms=$(( end_ms - start_ms ))
            echo "180000"
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
    sql_volvec_par=$(build_sql "$q" "volvec_parallel")
    sql_volvec_ser=$(build_sql "$q" "volvec_serial")

    pg_times=()
    volvec_par_times=()
    volvec_ser_times=()

    for run in $(seq 1 $RUNS); do
        printf "  run %d: " "$run"

        t=$(run_one_query "$sql_native")
        printf "PG=%s " "$t"
        pg_times+=("$t")

        t=$(run_one_query "$sql_volvec_par")
        printf "VP=%s " "$t"
        volvec_par_times+=("$t")

        t=$(run_one_query "$sql_volvec_ser")
        printf "VS=%s\n" "$t"
        volvec_ser_times+=("$t")
    done

    pg_med=$(median_of_3 "${pg_times[0]}" "${pg_times[1]}" "${pg_times[2]}")
    volvec_par_med=$(median_of_3 "${volvec_par_times[0]}" "${volvec_par_times[1]}" "${volvec_par_times[2]}")
    volvec_ser_med=$(median_of_3 "${volvec_ser_times[0]}" "${volvec_ser_times[1]}" "${volvec_ser_times[2]}")

    pg_sec=$(ms_to_s "$pg_med")
    volvec_par_sec=$(ms_to_s "$volvec_par_med")
    volvec_ser_sec=$(ms_to_s "$volvec_ser_med")

    pg_status="ok"; [[ "$pg_med" == "error" ]] && pg_status="error"
    vp_status="ok"; [[ "$volvec_par_med" == "error" ]] && vp_status="error"
    vs_status="ok"; [[ "$volvec_ser_med" == "error" ]] && vs_status="error"

    echo -e "q${q}\t${pg_sec}\t${pg_status}\t${volvec_par_sec}\t${vp_status}\t${volvec_ser_sec}\t${vs_status}" >> "$OUT"

    printf "  PG(6): %ss  |  volvec(par): %ss  |  volvec(ser): %ss\n" \
        "$pg_sec" "$volvec_par_sec" "$volvec_ser_sec"

    rm -f "$sql_native" "$sql_volvec_par" "$sql_volvec_ser"
done

echo ""
echo "Results: $OUT"
echo ""
column -t -s$'\t' "$OUT"

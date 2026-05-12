#!/bin/bash

set -uo pipefail

ROOT="/Users/chenyunwen/proj/postgres"
PSQL="$ROOT/installed/bin/psql"
QUERY_DIR="$ROOT/contrib/pg_volvec/tpch_queries"
OUT_DIR="$ROOT/contrib/pg_volvec/benchmarks"
DB_NAME="tpch"

RUNS="${RUNS:-3}"
TIMEOUT_SEC="${TIMEOUT_SEC:-300}"
PG_WORKERS="${PG_WORKERS:-14}"
VOLVEC_WORKERS="${VOLVEC_WORKERS:-14}"
TRACE_EXECUTION_PATH="${TRACE_EXECUTION_PATH:-off}"

QUERIES=(1 3 5 6 7 8 9 10 12 14)
TS=$(date +%Y%m%d_%H%M%S)
OUT_TSV="$OUT_DIR/tpch_pg_vs_volvec_${TS}.tsv"
OUT_LOG="$OUT_DIR/tpch_pg_vs_volvec_${TS}.log"

mkdir -p "$OUT_DIR"

echo -e "query\tmode\trun1_ms\trun2_ms\trun3_ms\tmedian_ms\tmedian_s\tstatus" > "$OUT_TSV"

log() {
    printf '%s\n' "$*" | tee -a "$OUT_LOG"
}

build_sql() {
    local qnum="$1"
    local mode="$2"
    local tmp_sql
    tmp_sql=$(mktemp -t pgvv_bench)

    {
        printf "SET client_min_messages=error;\n"
        printf "SET statement_timeout='%ss';\n" "$TIMEOUT_SEC"
        printf "SET jit=off;\n"

        if [[ "$mode" == "pg_parallel" ]]; then
            printf "SET pg_volvec.enabled=off;\n"
            printf "SET max_parallel_workers_per_gather=%s;\n" "$PG_WORKERS"
            printf "SET max_parallel_workers=%s;\n" "$PG_WORKERS"
        elif [[ "$mode" == "volvec_parallel" ]]; then
            printf "LOAD 'pg_volvec';\n"
            printf "SET pg_volvec.enabled=on;\n"
            printf "SET pg_volvec.parallel=on;\n"
            printf "SET pg_volvec.parallel_max_workers=%s;\n" "$VOLVEC_WORKERS"
            printf "SET pg_volvec.parallel_leader_participation=off;\n"
            printf "SET pg_volvec.trace_hooks=off;\n"
            printf "SET pg_volvec.trace_execution_path=%s;\n" "$TRACE_EXECUTION_PATH"
            printf "SET enable_eager_aggregate=off;\n"
            printf "SET max_parallel_workers_per_gather=0;\n"
            printf "SET max_parallel_workers=%s;\n" "$VOLVEC_WORKERS"
            printf "SET max_parallel_maintenance_workers=0;\n"
            printf "SET min_parallel_table_scan_size='1000GB';\n"
            printf "SET min_parallel_index_scan_size='1000GB';\n"
            printf "SET parallel_setup_cost=1000000000;\n"
            printf "SET parallel_tuple_cost=1000000000;\n"
        else
            printf 'Unsupported mode: %s\n' "$mode" >&2
            rm -f "$tmp_sql"
            return 1
        fi

        cat "$QUERY_DIR/q${qnum}.sql"
    } > "$tmp_sql"

    printf '%s\n' "$tmp_sql"
}

run_one_query() {
    local tmp_sql="$1"
    local tmp_out
    local start_ms end_ms elapsed_ms
    local timeout_ms
    tmp_out=$(mktemp -t pgvv_query)
    start_ms=$(python3 -c "import time; print(int(time.time()*1000))")
    timeout_ms=$((TIMEOUT_SEC * 1000))

    ( "$PSQL" -h /tmp -p 5432 -d "$DB_NAME" -v ON_ERROR_STOP=1 -qAt -f "$tmp_sql" > "$tmp_out" 2>&1; echo $? > "${tmp_out}.rc" ) &
    local psql_pid=$!
    while kill -0 "$psql_pid" 2>/dev/null; do
        sleep 0.05
        end_ms=$(python3 -c "import time; print(int(time.time()*1000))")
        if [[ $(( end_ms - start_ms )) -ge $timeout_ms ]]; then
            kill -9 "$psql_pid" 2>/dev/null || true
            wait "$psql_pid" 2>/dev/null || true
            printf '%s\t%s\n' "$timeout_ms" "timeout"
            rm -f "$tmp_out" "${tmp_out}.rc"
            return
        fi
    done

    wait "$psql_pid" 2>/dev/null || true
    end_ms=$(python3 -c "import time; print(int(time.time()*1000))")
    elapsed_ms=$(( end_ms - start_ms ))
    local rc
    local raw_output
    rc=$(cat "${tmp_out}.rc" 2>/dev/null || echo 1)
    raw_output=$(cat "$tmp_out" 2>/dev/null || true)
    rm -f "$tmp_out" "${tmp_out}.rc"

    if [[ "$rc" -ne 0 ]]; then
        if [[ "$raw_output" == *"canceling statement due to statement timeout"* ]]; then
            printf '%s\t%s\n' "$elapsed_ms" "timeout"
        else
            printf '%s\t%s\n' "$elapsed_ms" "error"
        fi
        return
    fi

    printf '%s\t%s\n' "$elapsed_ms" "ok"
}

median_of_3() {
    printf '%s\n%s\n%s\n' "$1" "$2" "$3" | sort -n | sed -n '2p'
}

ms_to_s() {
    python3 -c "v=int($1); print(f'{v/1000:.3f}')"
}

median_status() {
    local m="$1"
    shift
    local timeout_ms=$((TIMEOUT_SEC * 1000))
    local timeout_count=0
    local error_count=0
    local item
    for item in "$@"; do
        if [[ "$item" == "timeout" ]]; then
            timeout_count=$(( timeout_count + 1 ))
        elif [[ "$item" == "error" ]]; then
            error_count=$(( error_count + 1 ))
        fi
    done

    if [[ $timeout_count -ge 2 && "$m" -ge $timeout_ms ]]; then
        printf 'timeout\n'
    elif [[ $error_count -ge 2 ]]; then
        printf 'error\n'
    elif [[ $timeout_count -gt 0 ]]; then
        printf 'partial_timeout\n'
    elif [[ $error_count -gt 0 ]]; then
        printf 'partial_error\n'
    else
        printf 'ok\n'
    fi
}

run_mode() {
    local mode="$1"
    local q

    for q in "${QUERIES[@]}"; do
        log "--- q${q} ${mode} ---"
        sql_file=$(build_sql "$q" "$mode")

        times=()
        statuses=()

        for run in $(seq 1 "$RUNS"); do
            result=$(run_one_query "$sql_file")
            t=$(printf '%s' "$result" | cut -f1)
            st=$(printf '%s' "$result" | cut -f2)
            times+=("$t")
            statuses+=("$st")
            log "q${q} ${mode} run${run}: ${t} ms (${st})"
        done

        median_ms=$(median_of_3 "${times[0]}" "${times[1]}" "${times[2]}")
        median_s=$(ms_to_s "$median_ms")
        status=$(median_status "$median_ms" "${statuses[@]}")

        echo -e "q${q}\t${mode}\t${times[0]}\t${times[1]}\t${times[2]}\t${median_ms}\t${median_s}\t${status}" >> "$OUT_TSV"
        log "q${q} ${mode} median: ${median_ms} ms (${median_s} s) status=${status}"

        rm -f "$sql_file"
    done
}

run_mode pg_parallel
run_mode volvec_parallel

log ""
log "Results TSV: $OUT_TSV"
log "Results LOG: $OUT_LOG"

column -t -s $'\t' "$OUT_TSV"

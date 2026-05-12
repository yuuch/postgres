#!/usr/bin/env bash

set -euo pipefail

ROOT="/Users/chenyunwen/proj/postgres"
PSQL="${PSQL:-$ROOT/installed/bin/psql}"
QUERY_DIR="${QUERY_DIR:-$ROOT/contrib/pg_volvec/tpch_queries}"
OUT_DIR="${OUT_DIR:-$ROOT/contrib/pg_volvec/benchmarks}"
DB_NAME="${DB_NAME:-tpch}"
HOST="${HOST:-/tmp}"
PORT="${PORT:-5432}"
RUNS="${RUNS:-5}"
TIMEOUT_SEC="${TIMEOUT_SEC:-300}"
VOLVEC_WORKERS="${VOLVEC_WORKERS:-12}"
TRACE_EXECUTION_PATH="${TRACE_EXECUTION_PATH:-off}"

if [[ $# -lt 1 ]]; then
	printf 'usage: %s <query_num>\n' "$0" >&2
	exit 1
fi

QNUM="${1#q}"
if [[ ! "$QNUM" =~ ^[0-9]+$ ]]; then
	printf 'invalid query number: %s\n' "$1" >&2
	exit 1
fi

SQL_FILE="$QUERY_DIR/q${QNUM}.sql"
if [[ ! -f "$SQL_FILE" ]]; then
	printf 'SQL file not found: %s\n' "$SQL_FILE" >&2
	exit 1
fi

if [[ ! -x "$PSQL" ]]; then
	printf 'psql binary not found or not executable: %s\n' "$PSQL" >&2
	exit 1
fi

mkdir -p "$OUT_DIR"

TS="$(date +%Y%m%d_%H%M%S)"
OUT_TSV="$OUT_DIR/bench_tpch_q${QNUM}_${TS}.tsv"
OUT_LOG="$OUT_DIR/bench_tpch_q${QNUM}_${TS}.log"

log() {
	printf '%s\n' "$*" | tee -a "$OUT_LOG"
}

build_sql() {
	local tmp_sql
	tmp_sql=$(mktemp -t pgvv_tpch)
	{
		printf 'SET client_min_messages=error;\n'
		printf "SET statement_timeout='%ss';\n" "$TIMEOUT_SEC"
		printf 'SET jit=off;\n'
		printf "LOAD 'pg_volvec';\n"
		printf 'SET pg_volvec.enabled=on;\n'
		printf 'SET pg_volvec.parallel=on;\n'
		printf 'SET pg_volvec.parallel_leader_participation=off;\n'
		printf 'SET pg_volvec.trace_hooks=off;\n'
		printf 'SET pg_volvec.trace_execution_path=%s;\n' "$TRACE_EXECUTION_PATH"
		printf 'SET pg_volvec.parallel_max_workers=%s;\n' "$VOLVEC_WORKERS"
		printf 'SET enable_eager_aggregate=off;\n'
		printf 'SET max_parallel_workers_per_gather=0;\n'
		printf 'SET max_parallel_workers=%s;\n' "$VOLVEC_WORKERS"
		printf 'SET max_parallel_maintenance_workers=0;\n'
		printf "SET min_parallel_table_scan_size='1000GB';\n"
		printf "SET min_parallel_index_scan_size='1000GB';\n"
		printf 'SET parallel_setup_cost=1000000000;\n'
		printf 'SET parallel_tuple_cost=1000000000;\n'
		cat "$SQL_FILE"
	} > "$tmp_sql"
	printf '%s\n' "$tmp_sql"
}

run_one_query() {
	local tmp_sql="$1"
	local tmp_out
	local start_ms end_ms elapsed_ms timeout_ms rc raw_output
	tmp_out=$(mktemp -t pgvv_query)
	start_ms=$(python3 -c 'import time; print(int(time.time()*1000))')
	timeout_ms=$((TIMEOUT_SEC * 1000))

	( "$PSQL" -h "$HOST" -p "$PORT" -d "$DB_NAME" -v ON_ERROR_STOP=1 -qAt -f "$tmp_sql" > "$tmp_out" 2>&1; echo $? > "${tmp_out}.rc" ) &
	local psql_pid=$!
	while kill -0 "$psql_pid" 2>/dev/null; do
		sleep 0.05
		end_ms=$(python3 -c 'import time; print(int(time.time()*1000))')
		if [[ $(( end_ms - start_ms )) -ge "$timeout_ms" ]]; then
			kill -9 "$psql_pid" 2>/dev/null || true
			wait "$psql_pid" 2>/dev/null || true
			printf '%s\t%s\n' "$timeout_ms" "timeout"
			rm -f "$tmp_out" "${tmp_out}.rc"
			return
		fi
	done

	wait "$psql_pid" 2>/dev/null || true
	end_ms=$(python3 -c 'import time; print(int(time.time()*1000))')
	elapsed_ms=$(( end_ms - start_ms ))
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

median_of_5() {
	printf '%s\n%s\n%s\n%s\n%s\n' "$1" "$2" "$3" "$4" "$5" | sort -n | sed -n '3p'
}

ms_to_s() {
	python3 -c "v=int($1); print(f'{v/1000:.3f}')"
}

median_status() {
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

	if [[ $timeout_count -ge 3 ]]; then
		printf 'timeout\n'
	elif [[ $error_count -ge 3 ]]; then
		printf 'error\n'
	elif [[ $timeout_count -gt 0 ]]; then
		printf 'partial_timeout\n'
	elif [[ $error_count -gt 0 ]]; then
		printf 'partial_error\n'
	else
		printf 'ok\n'
	fi
}

tmp_sql="$(build_sql)"
trap 'rm -f "$tmp_sql"' EXIT

echo -e "query\trun1_ms\trun2_ms\trun3_ms\trun4_ms\trun5_ms\tmedian_ms\tmedian_s\tstatus" > "$OUT_TSV"
log "--- q${QNUM} ---"

times=()
statuses=()
for run in $(seq 1 "$RUNS"); do
	result=$(run_one_query "$tmp_sql")
	t=$(printf '%s' "$result" | cut -f1)
	st=$(printf '%s' "$result" | cut -f2)
	times+=("$t")
	statuses+=("$st")
	log "q${QNUM} run${run}: ${t} ms (${st})"
done

if [[ ${#times[@]} -ne 5 ]]; then
	printf 'this script expects RUNS=5 for median_of_5, got %s\n' "${#times[@]}" >&2
	exit 1
fi

median_ms=$(median_of_5 "${times[0]}" "${times[1]}" "${times[2]}" "${times[3]}" "${times[4]}")
median_s=$(ms_to_s "$median_ms")
status=$(median_status "${statuses[@]}")

log "q${QNUM} median: ${median_ms} ms (${median_s} s) status=${status}"
log "Results TSV: $OUT_TSV"
log "Results LOG: $OUT_LOG"

column -t -s $'\t' "$OUT_TSV"

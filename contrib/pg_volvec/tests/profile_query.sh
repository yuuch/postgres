#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="/Users/chenyunwen/proj/postgres"
DEFAULT_PSQL="$ROOT_DIR/installed/bin/psql"
DEFAULT_TPCH_DIR="$ROOT_DIR/tpch-dbgen"
DEFAULT_FG_DIR="$ROOT_DIR/FlameGraph"
DEFAULT_OUT_DIR="$ROOT_DIR/contrib/pg_volvec/tests/profiles"

psql_bin="$DEFAULT_PSQL"
host="/tmp"
port="5432"
db="tpch"
query_file=""
query_name=""
tpch_dir="$DEFAULT_TPCH_DIR"
fg_dir="$DEFAULT_FG_DIR"
out_dir="$DEFAULT_OUT_DIR"
label=""
title=""
profiler="auto"
enable="on"
trace_hooks="off"
client_min_messages="error"
duration_sec="5"
interval_ms="1"
sleep_before_query="1"
explain_analyze="off"

usage() {
  cat <<'EOF'
usage: profile_query.sh [options]

Required:
  --query-file PATH          Path to a SQL file to profile
  --query-name NAME          Query name resolved from tpch-dbgen, e.g. q1 or q6

Optional:
  --psql PATH                psql binary
  --host HOST                PostgreSQL host or socket directory (default: /tmp)
  --port PORT                PostgreSQL port (default: 5432)
  --db DBNAME                Database name (default: tpch)
  --tpch-dir PATH            Directory used with --query-name
  --out-dir PATH             Output directory for profiling artifacts
  --label NAME               Output label, default: <query>_pg_volvec_<on|off>
  --title TEXT               SVG title, default derived from label
  --profiler auto|sample|dtrace
  --enable on|off            Value for pg_volvec.enabled (default: on)
  --trace-hooks on|off       Value for pg_volvec.trace_hooks (default: off)
  --client-min-messages LVL  PostgreSQL client_min_messages (default: error)
  --duration SEC             Profile duration in seconds (default: 5)
  --interval-ms MS           Sampling interval in milliseconds (default: 1)
  --sleep-before-query SEC   Delay after printing backend PID (default: 1)
  --explain-analyze          Prefix query with EXPLAIN (ANALYZE, TIMING OFF, BUFFERS OFF, SUMMARY ON)
  --help                     Show this message

Examples:
  profile_query.sh --query-name q1 --db tpch --enable on
  profile_query.sh --query-name q6 --db tpch --enable off --profiler sample
  profile_query.sh --query-file /tmp/test.sql --db postgres --enable off
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --query-file)
      query_file="${2:-}"
      shift 2
      ;;
    --query-name)
      query_name="${2:-}"
      shift 2
      ;;
    --psql)
      psql_bin="${2:-}"
      shift 2
      ;;
    --host)
      host="${2:-}"
      shift 2
      ;;
    --port)
      port="${2:-}"
      shift 2
      ;;
    --db)
      db="${2:-}"
      shift 2
      ;;
    --tpch-dir)
      tpch_dir="${2:-}"
      shift 2
      ;;
    --out-dir)
      out_dir="${2:-}"
      shift 2
      ;;
    --label)
      label="${2:-}"
      shift 2
      ;;
    --title)
      title="${2:-}"
      shift 2
      ;;
    --profiler)
      profiler="${2:-}"
      shift 2
      ;;
    --enable)
      enable="${2:-}"
      shift 2
      ;;
    --trace-hooks)
      trace_hooks="${2:-}"
      shift 2
      ;;
    --client-min-messages)
      client_min_messages="${2:-}"
      shift 2
      ;;
    --duration)
      duration_sec="${2:-}"
      shift 2
      ;;
    --interval-ms)
      interval_ms="${2:-}"
      shift 2
      ;;
    --sleep-before-query)
      sleep_before_query="${2:-}"
      shift 2
      ;;
    --explain-analyze)
      explain_analyze="on"
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -n "$query_name" && -n "$query_file" ]]; then
  echo "use either --query-name or --query-file, not both" >&2
  exit 1
fi

if [[ -z "$query_name" && -z "$query_file" ]]; then
  echo "one of --query-name or --query-file is required" >&2
  exit 1
fi

if [[ -n "$query_name" ]]; then
  query_name="${query_name%.sql}"
  query_file="$tpch_dir/${query_name}.sql"
fi

if [[ ! -f "$query_file" ]]; then
  echo "query file not found: $query_file" >&2
  exit 1
fi

if [[ ! -x "$psql_bin" ]]; then
  echo "psql binary not found or not executable: $psql_bin" >&2
  exit 1
fi

if [[ ! -d "$fg_dir" ]]; then
  echo "FlameGraph directory not found: $fg_dir" >&2
  exit 1
fi

if [[ ! -f "$fg_dir/stackcollapse-sample.awk" ]]; then
  echo "missing stackcollapse-sample.awk in $fg_dir" >&2
  exit 1
fi

if [[ ! -f "$fg_dir/stackcollapse.pl" ]]; then
  echo "missing stackcollapse.pl in $fg_dir" >&2
  exit 1
fi

if [[ ! -f "$fg_dir/flamegraph.pl" ]]; then
  echo "missing flamegraph.pl in $fg_dir" >&2
  exit 1
fi

case "$profiler" in
  auto|sample|dtrace) ;;
  *)
    echo "invalid profiler: $profiler" >&2
    exit 1
    ;;
esac

case "$enable" in
  on|off) ;;
  *)
    echo "invalid --enable value: $enable" >&2
    exit 1
    ;;
esac

case "$trace_hooks" in
  on|off) ;;
  *)
    echo "invalid --trace-hooks value: $trace_hooks" >&2
    exit 1
    ;;
esac

mkdir -p "$out_dir"

base_name="$(basename "$query_file" .sql)"
if [[ -z "$label" ]]; then
  label="${base_name}_pg_volvec_${enable}"
fi

if [[ -z "$title" ]]; then
  title="pg_volvec ${label}"
fi

wrapped_sql="$out_dir/${label}.wrapped.sql"
psql_out="$out_dir/${label}.psql.out.txt"
profiler_log="$out_dir/${label}.profiler.log"
folded_txt="$out_dir/${label}.folded.txt"
flame_svg="$out_dir/${label}.flame.svg"
summary_txt="$out_dir/${label}.summary.txt"
dtrace_txt="$out_dir/${label}.dtrace.txt"
sample_txt="$out_dir/${label}.sample.txt"

can_use_dtrace() {
  /usr/sbin/dtrace -n 'BEGIN { exit(0); }' >/dev/null 2>&1
}

select_profiler() {
  case "$profiler" in
    sample)
      printf '%s\n' "sample"
      ;;
    dtrace)
      if can_use_dtrace; then
        printf '%s\n' "dtrace"
      else
        echo "dtrace requested but unavailable on this machine" >&2
        exit 1
      fi
      ;;
    auto)
      if can_use_dtrace; then
        printf '%s\n' "dtrace"
      else
        printf '%s\n' "sample"
      fi
      ;;
  esac
}

write_wrapped_sql() {
  {
    printf "LOAD 'llvmjit';\n"
    printf "LOAD 'pg_volvec';\n"
    printf "SET client_min_messages = %s;\n" "$client_min_messages"
    printf "SET max_parallel_workers_per_gather = 0;\n"
    printf "SET max_parallel_workers = 0;\n"
    printf "SET min_parallel_table_scan_size = '1000GB';\n"
    printf "SET parallel_setup_cost = 1000000000;\n"
    printf "SET parallel_tuple_cost = 1000000000;\n"
    printf "SET pg_volvec.enabled = %s;\n" "$enable"
    printf "SET pg_volvec.trace_hooks = %s;\n" "$trace_hooks"
    printf "SELECT pg_backend_pid();\n"
    printf "SELECT pg_sleep(%s);\n" "$sleep_before_query"
    if [[ "$explain_analyze" == "on" ]]; then
      printf "EXPLAIN (ANALYZE, TIMING OFF, BUFFERS OFF, SUMMARY ON)\n"
    fi
    cat "$query_file"
    printf "\n"
  } > "$wrapped_sql"
}

wait_for_backend_pid() {
  local attempts=200
  local pid=""
  for _ in $(seq 1 "$attempts"); do
    if [[ -s "$psql_out" ]]; then
      pid="$(head -n 1 "$psql_out" | tr -d '[:space:]')"
      if [[ "$pid" =~ ^[0-9]+$ ]]; then
        printf '%s\n' "$pid"
        return 0
      fi
    fi
    sleep 0.05
  done
  return 1
}

run_sample_profiler() {
  : > "$profiler_log"
  /usr/bin/sample "$1" "$duration_sec" "$interval_ms" -mayDie -file "$sample_txt" \
    >/dev/null 2>>"$profiler_log" || true
}

run_dtrace_profiler() {
  local hz
  hz=$((1000 / interval_ms))
  if (( hz < 1 )); then
    hz=1
  fi
  if (( hz > 997 )); then
    hz=997
  fi
  : > "$profiler_log"
  /usr/sbin/dtrace -x ustackframes=100 \
    -n "profile-${hz} /pid == $1/ { @[ustack()] = count(); } tick-${duration_sec}s { exit(0); }" \
    -o "$dtrace_txt" >>"$profiler_log" 2>&1 || true
}

fold_profile() {
  local chosen="$1"
  if [[ "$chosen" == "sample" ]]; then
    awk -f "$fg_dir/stackcollapse-sample.awk" "$sample_txt" > "$folded_txt"
  else
    perl "$fg_dir/stackcollapse.pl" "$dtrace_txt" > "$folded_txt"
  fi
}

generate_summary() {
  local chosen="$1"
  local psql_rc="$2"
  local exec_time=""
  if [[ "$explain_analyze" == "on" ]]; then
    exec_time="$(sed -n 's/^Execution Time: //p' "$psql_out" | tail -n 1)"
  fi

  {
    printf "label: %s\n" "$label"
    printf "query_file: %s\n" "$query_file"
    printf "wrapped_sql: %s\n" "$wrapped_sql"
    printf "database: %s\n" "$db"
    printf "host: %s\n" "$host"
    printf "port: %s\n" "$port"
    printf "pg_volvec.enabled: %s\n" "$enable"
    printf "pg_volvec.trace_hooks: %s\n" "$trace_hooks"
    printf "profiler: %s\n" "$chosen"
    printf "backend_pid: %s\n" "$backend_pid"
    printf "duration_sec: %s\n" "$duration_sec"
    printf "interval_ms: %s\n" "$interval_ms"
    printf "psql_rc: %s\n" "$psql_rc"
    if [[ -n "$exec_time" ]]; then
      printf "execution_time: %s\n" "$exec_time"
    fi
    printf "\nTop leaf symbols:\n"
    awk '
      {
        count = $NF
        $NF = ""
        sub(/[[:space:]]+$/, "", $0)
        n = split($0, frames, ";")
        leaf = frames[n]
        if (leaf == "")
          leaf = "[root]"
        totals[leaf] += count
      }
      END {
        for (leaf in totals)
          printf "%d\t%s\n", totals[leaf], leaf
      }
    ' "$folded_txt" | sort -nr | head -n 20
    printf "\nTop folded stacks:\n"
    sort -k2,2nr "$folded_txt" | head -n 20
  } > "$summary_txt"
}

write_wrapped_sql
chosen_profiler="$(select_profiler)"
rm -f "$psql_out" "$profiler_log" "$folded_txt" "$flame_svg" "$summary_txt" "$sample_txt" "$dtrace_txt"

"$psql_bin" -h "$host" -p "$port" -d "$db" -v ON_ERROR_STOP=1 -qAtf "$wrapped_sql" > "$psql_out" 2>&1 &
psql_pid=$!
backend_pid=""

if ! backend_pid="$(wait_for_backend_pid)"; then
  echo "failed to capture backend pid; see $psql_out" >&2
  wait "$psql_pid" || true
  exit 1
fi

if [[ "$chosen_profiler" == "sample" ]]; then
  run_sample_profiler "$backend_pid"
else
  run_dtrace_profiler "$backend_pid"
fi

psql_rc=0
if ! wait "$psql_pid"; then
  psql_rc=$?
fi

fold_profile "$chosen_profiler"
perl "$fg_dir/flamegraph.pl" --title "$title" "$folded_txt" > "$flame_svg"
generate_summary "$chosen_profiler" "$psql_rc"

printf "label=%s\n" "$label"
printf "profiler=%s\n" "$chosen_profiler"
printf "backend_pid=%s\n" "$backend_pid"
printf "wrapped_sql=%s\n" "$wrapped_sql"
printf "psql_out=%s\n" "$psql_out"
if [[ "$chosen_profiler" == "sample" ]]; then
  printf "raw_profile=%s\n" "$sample_txt"
else
  printf "raw_profile=%s\n" "$dtrace_txt"
fi
printf "folded=%s\n" "$folded_txt"
printf "flame_svg=%s\n" "$flame_svg"
printf "summary=%s\n" "$summary_txt"

exit "$psql_rc"

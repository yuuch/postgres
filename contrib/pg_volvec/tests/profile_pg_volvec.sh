#!/usr/bin/env bash
# profile_pg_volvec.sh - end-to-end driver:
#   1) get a fresh backend PID via psql
#   2) attach DTrace (preferred) OR /usr/bin/sample (fallback) to it
#   3) run canonical Q1 in that same backend so the profile is non-empty
#   4) detach; print the report path
#
# DTrace on macOS Apple Silicon needs SIP relaxed for DTrace
# (`csrutil enable --without dtrace`) AND sudo. If DTrace privileges are
# missing we fall back to /usr/bin/sample which works without SIP changes.
set -euo pipefail

PGBIN="${PGBIN:-./installed/bin}"
PGDATA="${PGDATA:-$HOME/data/pg_tpch}"
DB="${DB:-tpch}"
SOCK="${SOCK:-/tmp}"
PORT="${PORT:-5432}"
SQL="${SQL:-contrib/pg_volvec/tests/q1_canonical.sql}"
DSCRIPT="contrib/pg_volvec/tests/pg_volvec_bottleneck.d"
OUT="/tmp/pg_volvec_bottleneck_$(date +%Y%m%d_%H%M%S)"

mkdir -p "$OUT"
echo "[+] output dir: $OUT"

# Open a long-lived psql session via coproc so the backend PID stays stable
# across attach + workload run. We use a FIFO for the SQL stream.
FIFO="$OUT/sql.fifo"
mkfifo "$FIFO"

"$PGBIN/psql" -h "$SOCK" -p "$PORT" -d "$DB" -X -q -At -f "$FIFO" \
    > "$OUT/psql.out" 2> "$OUT/psql.err" &
PSQL_PID=$!
exec 9>"$FIFO"

echo "SELECT pg_backend_pid();" >&9
sleep 0.5
BACKEND_PID="$(head -n1 "$OUT/psql.out" | tr -dc '0-9')"
if [[ -z "$BACKEND_PID" ]]; then
    echo "[!] failed to capture backend pid; psql.err:" >&2
    cat "$OUT/psql.err" >&2
    exec 9>&-; wait $PSQL_PID 2>/dev/null || true
    exit 1
fi
echo "[+] backend pid: $BACKEND_PID"

PROFILER=""
if command -v dtrace >/dev/null 2>&1; then
    if sudo -n dtrace -V >/dev/null 2>&1; then
        PROFILER="dtrace"
    fi
fi
if [[ -z "$PROFILER" && -x /usr/bin/sample ]]; then
    PROFILER="sample"
fi
[[ -z "$PROFILER" ]] && { echo "[!] neither dtrace nor sample available"; exit 1; }
echo "[+] profiler: $PROFILER"

if [[ "$PROFILER" == "dtrace" ]]; then
    sudo dtrace -p "$BACKEND_PID" -s "$DSCRIPT" -o "$OUT/dtrace.out" &
    PROF_PID=$!
    sleep 1
else
    /usr/bin/sample "$BACKEND_PID" 30 -mayDie -file "$OUT/sample.leader.out" >/dev/null 2>&1 &
    PROF_PID=$!
    (
        for i in 1 2 3 4 5 6 7 8 9 10; do
            sleep 0.4
            for wpid in $(pgrep -f 'pg_volvec.*worker' 2>/dev/null); do
                [[ -f "$OUT/sample.w${wpid}.out" ]] && continue
                /usr/bin/sample "$wpid" 20 -mayDie -file "$OUT/sample.w${wpid}.out" >/dev/null 2>&1 &
            done
        done
        wait
    ) &
    WORKER_WATCH_PID=$!
fi

echo "[+] running $SQL"
cat "$SQL" >&9
echo "SELECT 1;" >&9
sleep 0.3

if [[ "$PROFILER" == "dtrace" ]]; then
    sudo kill -INT "$PROF_PID" 2>/dev/null || true
fi
wait "$PROF_PID" 2>/dev/null || true
[[ -n "${WORKER_WATCH_PID:-}" ]] && wait "$WORKER_WATCH_PID" 2>/dev/null || true

echo "\\q" >&9
exec 9>&-
wait $PSQL_PID 2>/dev/null || true
rm -f "$FIFO"

echo "[+] done."
echo "[+] reports:"
ls -la "$OUT"

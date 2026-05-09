#!/usr/bin/env bash
set -euo pipefail

PGBIN="${PGBIN:-./installed/bin}"
DB="${DB:-tpch}"
SOCK="${SOCK:-/tmp}"
PORT="${PORT:-5432}"
SQL="${SQL:-contrib/pg_volvec/tests/q1_canonical.sql}"
FG="${FG:-$HOME/proj/postgres/FlameGraph}"
OUT="${OUT:-/tmp/pg_volvec_flame_$(date +%Y%m%d_%H%M%S)}"
DUR_S="${DUR_S:-10}"
mkdir -p "$OUT"
echo "[+] OUT=$OUT  duration=${DUR_S}s"

FIFO="$OUT/sql.fifo"
mkfifo "$FIFO"
"$PGBIN/psql" -h "$SOCK" -p "$PORT" -d "$DB" -X -q -At -f "$FIFO" \
    > "$OUT/psql.out" 2> "$OUT/psql.err" &
PSQL_PID=$!
exec 9>"$FIFO"

echo "SELECT pg_backend_pid();" >&9
sleep 0.4
LEADER="$(head -n1 "$OUT/psql.out" | tr -dc '0-9')"
[[ -z "$LEADER" ]] && { echo "[!] no leader pid"; exit 1; }
echo "[+] leader=$LEADER"

DUR_S=$(( DUR_S + 0 ))
/usr/bin/sample "$LEADER" "$DUR_S" 1 -mayDie -file "$OUT/sample.leader.txt" >/dev/null 2>&1 &
LEADER_SAMP=$!

(
    seen=""
    end=$((SECONDS + DUR_S))
    while (( SECONDS < end )); do
        for wpid in $(pgrep -f 'pg_volvec worker' 2>/dev/null); do
            [[ ",$seen," == *",$wpid,"* ]] && continue
            seen="$seen,$wpid"
            remaining=$(( end - SECONDS ))
            (( remaining < 1 )) && remaining=1
            /usr/bin/sample "$wpid" "$remaining" 1 -mayDie \
                -file "$OUT/sample.w${wpid}.txt" >/dev/null 2>&1 &
        done
        sleep 0.2
    done
    wait
) &
WATCHER=$!

(
    while kill -0 "$LEADER_SAMP" 2>/dev/null; do
        cat "$SQL" >&9
        echo "SELECT 1;" >&9
        sleep 0.1
    done
) &
WORKLOAD=$!

wait "$LEADER_SAMP" 2>/dev/null || true
wait "$WATCHER" 2>/dev/null || true
kill "$WORKLOAD" 2>/dev/null || true
wait "$WORKLOAD" 2>/dev/null || true

echo "\\q" >&9
exec 9>&-
wait $PSQL_PID 2>/dev/null || true
rm -f "$FIFO"

echo "[+] samples captured:"
ls -la "$OUT"/sample.*.txt

COLLAPSED="$OUT/collapsed.folded"
: > "$COLLAPSED"
for f in "$OUT"/sample.*.txt; do
    "$FG/stackcollapse-sample.awk" "$f" >> "$COLLAPSED"
done

echo "[+] collapsed lines: $(wc -l < "$COLLAPSED")"

"$FG/flamegraph.pl" \
    --title "pg_volvec Q1 SF=10 (leader+workers, ${DUR_S}s window)" \
    --subtitle "samples merged across leader + parallel workers" \
    --colors=java \
    --width 1400 \
    --countname "samples" \
    "$COLLAPSED" > "$OUT/flame.svg"

PGVV_FOLDED="$OUT/collapsed.pgvolvec.folded"
grep -E '(pg_volvec|llvmjit_)' "$COLLAPSED" > "$PGVV_FOLDED" || true
if [[ -s "$PGVV_FOLDED" ]]; then
    "$FG/flamegraph.pl" \
        --title "pg_volvec Q1 SF=10 (pg_volvec frames only)" \
        --colors=java --width 1400 --countname samples \
        "$PGVV_FOLDED" > "$OUT/flame.pgvolvec.svg"
fi

echo "[+] flamegraphs:"
ls -la "$OUT"/*.svg

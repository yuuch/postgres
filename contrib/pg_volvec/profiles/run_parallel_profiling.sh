#!/usr/bin/env bash
set -euo pipefail

ROOT="/Users/chenyunwen/proj/postgres"
OUT_DIR="$ROOT/contrib/pg_volvec/profiles/xctrace_parallel_analysis_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$OUT_DIR"

PSQL="$ROOT/installed/bin/psql"
HOST="/tmp"
PORT="5432"
DB="tpch"
FG_DIR="$ROOT/FlameGraph"
TPCH_SQL="$ROOT/tpch-dbgen"

# Queries to profile (exclude Q2, Q21)
QUERIES=(1 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 22)

# Time limits (generous, in seconds) based on known execution times
declare -A TIME_LIMITS=(
  [1]=10 [3]=15 [4]=20 [5]=10 [6]=8
  [7]=10 [8]=12 [9]=20 [10]=15 [11]=5
  [12]=12 [13]=12 [14]=10 [15]=15 [16]=5
  [17]=25 [18]=30 [19]=10 [20]=50 [22]=5
)

make_wrapped_sql() {
  local qnum="$1"
  local qfile="$TPCH_SQL/${qnum}.sql"
  if [[ ! -f "$qfile" ]]; then
    qfile="$TPCH_SQL/q${qnum}.sql"
  fi
  if [[ ! -f "$qfile" ]]; then
    echo "ERROR: SQL file for q${qnum} not found" >&2
    return 1
  fi

  cat > "$OUT_DIR/q${qnum}.sql" <<SQLWRAP
LOAD 'pg_volvec';
SET client_min_messages=error;
SET statement_timeout='180s';
SET jit=off;
SET enable_eager_aggregate=off;
SET duckdb.force_execution=off;
SET pg_volvec.enabled=on;
SET pg_volvec.trace_hooks=off;
SET max_parallel_workers_per_gather=0;
SET max_parallel_workers=8;
SET pg_volvec.parallel=on;
SET pg_volvec.parallel_max_workers=4;
SET pg_volvec.parallel_leader_participation=off;
EXPLAIN (ANALYZE, TIMING OFF, BUFFERS OFF, SUMMARY ON)
$(cat "$qfile")
SQLWRAP
}

profile_query() {
  local qnum="$1"
  local qdir="$OUT_DIR/q${qnum}"
  mkdir -p "$qdir"
  local tlim="${TIME_LIMITS[$qnum]:-30}"

  make_wrapped_sql "$qnum" || return 1

  # Start xctrace
  xctrace record \
    --template "Time Profiler" \
    --all-processes \
    --time-limit "${tlim}s" \
    --output "$qdir/q${qnum}.trace" \
    &>/dev/null &
  local xtrace_pid=$!
  sleep 2

  # Run query
  "$PSQL" -h "$HOST" -p "$PORT" -d "$DB" \
    -v ON_ERROR_STOP=1 -qAtf "$qdir/../q${qnum}.sql" \
    > "$qdir/psql.out" 2>"$qdir/psql.err" &
  local psql_pid=$!

  # Wait for query to finish
  local rc=0
  if ! wait $psql_pid; then
    rc=$?
  fi

  # Stop xctrace
  sleep 1
  kill -INT $xtrace_pid 2>/dev/null || true
  wait $xtrace_pid 2>/dev/null || true

  # Extract exec time
  local exec_time=""
  exec_time=$(sed -n 's/^Execution Time: //p' "$qdir/psql.out" | tail -n 1 | sed 's/ ms$//') || true
  mkdir -p "$qdir/per_pid"

  # Export TOC
  xctrace export --input "$qdir/q${qnum}.trace" --toc > "$qdir/toc.xml" 2>/dev/null || true

  # Export time-profile
  xctrace export --input "$qdir/q${qnum}.trace" --xpath "//sample" > "$qdir/time-profile.xml" 2>/dev/null || true

  # Generate folded stacks
  python3 <<'PYEOF'
import xml.etree.ElementTree as ET
import os, re

qnum = os.environ.get("QNUM", "1")
qdir = os.environ.get("QDIR", "")
toc_path = os.path.join(qdir, "toc.xml")
tp_path = os.path.join(qdir, "time-profile.xml")
folded_path = os.path.join(qdir, f"q{qnum}.xctrace.postgres.folded.txt")
top_stacks_path = os.path.join(qdir, "top_stacks.txt")
stage_summary_path = os.path.join(qdir, "stage_summary.tsv")
per_pid_dir = os.path.join(qdir, "per_pid")

# Get postgres PIDs
postgres_pids = set()
process_names = {}
try:
    tree = ET.parse(toc_path)
    for p in tree.iter("process"):
        name = p.get("name", "")
        pid = p.get("pid", "")
        if pid:
            process_names[pid] = name
        if "postgres" in name.lower() or "postmaster" in name.lower():
            postgres_pids.add(pid)
except:
    pass

# Parse time-profile
stack_counts = {}
category_counts = {"expr": 0, "deform": 0, "io": 0, "hash_join": 0, "sort": 0, "agg": 0, "sync_wait": 0, "other": 0}
pid_category_counts = {}
pid_stack_counts = {}
pid_roles = {}

def process_role(symbols):
    joined = " ".join(s.lower() for s in symbols)
    if "parallelworkermain" in joined or "backgroundworkermain" in joined:
        return "worker"
    if "postgresmain" in joined:
        return "leader"
    return "unknown"

def safe_name(text):
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", text).strip("_") or "unknown"

try:
    tree = ET.parse(tp_path)
    for sample in tree.iter("sample"):
        pid_elem = sample.find("pid")
        pid = pid_elem.text if pid_elem is not None else ""
        if pid not in postgres_pids:
            continue

        count_elem = sample.find("count")
        count = int(count_elem.text) if count_elem is not None else 1

        backtrace = sample.find("backtrace")
        if backtrace is None:
            continue
        frames = []
        for thread in backtrace:
            for frame in thread.iter("frame"):
                sym = frame.find("symbol")
                if sym is not None and sym.text:
                    frames.append(sym.text)
        if not frames:
            continue

        stack = ";".join(reversed(frames))
        stack_counts[stack] = stack_counts.get(stack, 0) + count
        pid_stack_counts.setdefault(pid, {})
        pid_stack_counts[pid][stack] = pid_stack_counts[pid].get(stack, 0) + count

        # Categorize by leaf symbol
        leaf = frames[0]
        leaf_lower = leaf.lower()

        # Category classification
        cat = "other"
        if any(kw in leaf_lower for kw in ["varexprprogram::eval", "stephaswideconst", "readregisterwide", "getregisternumericwidth", "getwideintreg"]):
            cat = "expr"
        elif any(kw in leaf_lower for kw in ["datachunkdeformer::deform", "fastgetattr", "nocachegetattr", "heap_getattr", "heapgettup"]):
            cat = "deform"
        elif any(kw in leaf_lower for kw in ["bufferalloc", "bufmgr", "read_stream", "bufferread", "smgrread"]):
            cat = "io"
        elif any(kw in leaf_lower for kw in ["hashjoin", "hashbuild", "hashprobe", "hashtable"]):
            cat = "hash_join"
        elif any(kw in leaf_lower for kw in ["vectorsort", "vecsort", "tuplesort"]):
            cat = "sort"
        elif any(kw in leaf_lower for kw in ["aggstate::", "agg_state", "consume_batch", "update_group_accum"]):
            cat = "agg"
        elif any(kw in leaf_lower for kw in ["latch", "wait", "sync"]):
            cat = "sync_wait"

        category_counts[cat] = category_counts.get(cat, 0) + count

        # Per-PID tracking
        if pid not in pid_category_counts:
            pid_category_counts[pid] = {}
        pid_category_counts[pid][cat] = pid_category_counts[pid].get(cat, 0) + count
        pid_roles.setdefault(pid, process_role(frames))
except Exception as e:
    print(f"Error parsing: {e}")

total_samples = sum(category_counts.values())

# Write folded stacks
with open(folded_path, "w") as f:
    for stack, cnt in sorted(stack_counts.items(), key=lambda x: -x[1]):
        f.write(f"{cnt} {stack}\n")

# Write top stacks
with open(top_stacks_path, "w") as f:
    for stack, cnt in sorted(stack_counts.items(), key=lambda x: -x[1])[:50]:
        f.write(f"{cnt}\t{stack}\n")

os.makedirs(per_pid_dir, exist_ok=True)
for pid, stacks in pid_stack_counts.items():
    name = safe_name(process_names.get(pid, pid))
    per_pid_path = os.path.join(per_pid_dir, f"pid_{pid}_{name}.folded.txt")
    with open(per_pid_path, "w") as f:
        for stack, cnt in sorted(stacks.items(), key=lambda x: -x[1]):
            f.write(f"{cnt} {stack}\n")

with open(stage_summary_path, "w") as f:
    f.write("scope\tid_or_name\trole\tcategory\tsamples\tpct\n")
    for cat in sorted(category_counts.keys(), key=lambda x: -category_counts[x]):
        if category_counts[cat] > 0:
            pct = (category_counts[cat] / total_samples * 100) if total_samples > 0 else 0
            f.write(f"all\tpostgres\tall\t{cat}\t{category_counts[cat]}\t{pct:.2f}\n")
    for pid in sorted(pid_category_counts.keys(), key=lambda x: -sum(pid_category_counts[x].values())):
        pc = pid_category_counts[pid]
        pid_total = sum(pc.values())
        role = pid_roles.get(pid, "unknown")
        name = process_names.get(pid, pid).replace("\t", " ")
        for cat in sorted(pc.keys(), key=lambda x: -pc[x]):
            pct = (pc[cat] / pid_total * 100) if pid_total > 0 else 0
            f.write(f"pid\t{pid}:{name}\t{role}\t{cat}\t{pc[cat]}\t{pct:.2f}\n")

# Print summary
print(f"total_samples\t{total_samples}")
for cat in sorted(category_counts.keys(), key=lambda x: -category_counts[x]):
    if category_counts[cat] > 0:
        pct = (category_counts[cat] / total_samples * 100) if total_samples > 0 else 0
        print(f"{cat}\t{category_counts[cat]}\t{pct:.2f}")

print("pid\ttop_category\ttop_pct")
for pid in sorted(pid_category_counts.keys(), key=lambda x: -sum(pid_category_counts[x].values())):
    pc = pid_category_counts[pid]
    pid_total = sum(pc.values())
    top_cat = max(pc, key=pc.get)
    top_pct = (pc[top_cat] / pid_total * 100) if pid_total > 0 else 0
    print(f"{pid}\t{top_cat}\t{top_pct:.2f}")
PYEOF
}

echo "=== pg_volvec parallel xctrace profiling ==="
echo "Output: $OUT_DIR"
echo ""

for q in "${QUERIES[@]}"; do
  echo "--- Q${q} ---"
  export QNUM="$q"
  export QDIR="$OUT_DIR/q${q}"

  # Run profiling
  if ! profile_query "$q"; then
    echo "FAILED: Q${q}" >&2
    continue
  fi

  # Generate flame graph
  if [[ -f "$OUT_DIR/q${q}/q${q}.xctrace.postgres.folded.txt" ]]; then
    perl "$FG_DIR/flamegraph.pl" \
      --title "pg_volvec q${q} (xctrace, parallel)" \
      "$OUT_DIR/q${q}/q${q}.xctrace.postgres.folded.txt" \
      > "$OUT_DIR/q${q}/q${q}.xctrace.postgres.flame.svg" 2>/dev/null
    echo "Flame graph generated for Q${q}"
  fi

  for folded in "$OUT_DIR/q${q}/per_pid"/*.folded.txt; do
    if [[ ! -f "$folded" ]]; then
      continue
    fi
    base="${folded%.folded.txt}"
    perl "$FG_DIR/flamegraph.pl" \
      --title "pg_volvec q${q} per-pid" \
      --subtitle "$(basename "$base")" \
      "$folded" \
      > "${base}.flame.svg" 2>/dev/null
  done

  echo ""
done

# Generate summary TSV
echo "=== Generating summary ==="
{
  printf "query\ttotal_samples\texpr_pct\tdeform_pct\tio_pct\thash_join_pct\tsort_pct\tagg_pct\tsync_wait_pct\tother_pct\n"
  for q in "${QUERIES[@]}"; do
    summary_file="$OUT_DIR/q${q}/summary.txt"
    if [[ -f "$summary_file" ]]; then
      echo "q${q}"
    fi
  done
} > "$OUT_DIR/summary.tsv"

echo "=== Done ==="
echo "Results in: $OUT_DIR"

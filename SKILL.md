---
name: pg-volvec-dev
description: Build, install, run, test, benchmark, and debug the pg_volvec executor prototype in /Users/chenyunwen/proj/postgres. Use when working in contrib/pg_volvec, reproducing or fixing Q1/Q6/Q10 behavior, running Meson builds into /Users/chenyunwen/proj/postgres/installed, starting ~/data/pg_tpch, or investigating regressions around page-wise scan, tuple deform/JIT deform, HashJoin, and executor hooks.
---

# pg_volvec Dev

## Workspace

- Work from `/Users/chenyunwen/proj/postgres`.
- Treat `contrib/pg_volvec` as the active extension directory.
- Treat `build/` as the active Meson build directory.
- Treat `installed/` as the active install prefix.
- Read `/Users/chenyunwen/proj/postgres/contrib/pg_volvec/LOCAL_RUNBOOK.md` first if you need the latest verified local commands or runtime notes.

## Build And Install

Use the top-level Meson build, not the older `build_pg_*` directories.

If `build/` does not exist, create it with:

```bash
meson setup build \
  --prefix=/Users/chenyunwen/proj/postgres/installed \
  -Dllvm=enabled \
  --buildtype=debugoptimized
```

For normal development, run:

```bash
meson compile -C build pg_volvec
meson install -C build --only-changed
```

Expect the main installed artifacts at:

- `/Users/chenyunwen/proj/postgres/installed/lib/pg_volvec.dylib`
- `/Users/chenyunwen/proj/postgres/installed/lib/pg_volvec.so`
- `/Users/chenyunwen/proj/postgres/installed/share/extension/pg_volvec.control`
- `/Users/chenyunwen/proj/postgres/installed/share/extension/pg_volvec--1.0.sql`

## Start And Connect

Use the existing TPCH instance in `~/data/pg_tpch`.

Start:

```bash
./installed/bin/pg_ctl -D ~/data/pg_tpch -l ~/data/pg_tpch/logfile start
```

Stop:

```bash
./installed/bin/pg_ctl -D ~/data/pg_tpch stop
```

Check status:

```bash
./installed/bin/pg_ctl -D ~/data/pg_tpch status
./installed/bin/pg_isready -h /tmp -p 5432
```

Connect:

```bash
./installed/bin/psql -h /tmp -p 5432 -d postgres
./installed/bin/psql -h /tmp -p 5432 -d tpch
```

Confirm preload and GUC registration with:

```bash
./installed/bin/psql -h /tmp -p 5432 -d postgres -Atqc \
  "SHOW shared_preload_libraries; SHOW pg_volvec.enabled;"
```

## Get pg_duckdb Explain Plans

Use this when you want the **DuckDB CustomScan plan** as a reference for
`pg_yaap` plan shape comparisons. The target output is:

```text
Custom Scan (DuckDBScan)
  DuckDB Execution Plan:
  ...
```

If you only see a normal PostgreSQL `Hash Join` / `Gather` / `Seq Scan` plan,
then the pg_duckdb planner hook is not active for that backend yet.

### One-time local runtime setup

The current local checkout already has the extension SQL/control files, but the
runtime libraries may need to be copied into `installed/lib/`:

```bash
cp /Users/chenyunwen/proj/postgres/contrib/pg_duckdb/pg_duckdb.dylib \
  /Users/chenyunwen/proj/postgres/installed/lib/pg_duckdb.dylib
cp /Users/chenyunwen/proj/postgres/contrib/pg_duckdb/third_party/duckdb/build/release/src/libduckdb.dylib \
  /Users/chenyunwen/proj/postgres/installed/lib/libduckdb.dylib
```

Without `libduckdb.dylib`, PostgreSQL startup fails with a message like:

```text
could not load library ".../pg_duckdb.dylib": Library not loaded: @rpath/libduckdb.dylib
```

### Start the cluster with pg_duckdb preloaded

`pg_duckdb` must be loaded through `shared_preload_libraries`. In this local
environment, the running cluster is often started with a command-line override,
so `postgresql.conf` may be ignored even if it already contains
`shared_preload_libraries = 'pg_duckdb'`.

Use:

```bash
./installed/bin/pg_ctl -D ~/data/pg_tpch restart -m fast \
  -o "-c shared_preload_libraries=pg_duckdb" \
  -l ~/data/pg_tpch/logfile
```

Verify that the backend really picked it up:

```bash
./installed/bin/psql -X -P pager=off -h /tmp -p 5432 -d tpch -c \
  "SELECT name, setting, source FROM pg_settings WHERE name IN ('shared_preload_libraries','duckdb.force_execution');"
```

Expected:

```text
 shared_preload_libraries | pg_duckdb | command line
 duckdb.force_execution   | off       | default
```

If `shared_preload_libraries` is empty with source `command line`, inspect the
actual postmaster command:

```bash
./installed/bin/pg_ctl -D ~/data/pg_tpch status
cat ~/data/pg_tpch/postmaster.opts
```

### Get the actual DuckDB plan

After the backend is preloaded correctly, use `duckdb.force_execution = true`
and plain `EXPLAIN`:

```bash
sql=$(mktemp)
printf "SET duckdb.force_execution = true;\nEXPLAIN " > "$sql"
cat contrib/pg_carbon/tests/tpch/q8.sql >> "$sql"
./installed/bin/psql -X -P pager=off -h /tmp -p 5432 -d tpch -v ON_ERROR_STOP=1 -f "$sql"
rm -f "$sql"
```

Repeat with `q7.sql`, `q8.sql`, `q9.sql`, etc. The output should start with:

```text
Custom Scan (DuckDBScan)
  DuckDB Execution Plan:
```

This is the plan to compare against `pg_yaap`'s optimizer physical plan. If the
shape differs materially, prefer aligning `pg_yaap` optimizer behavior with the
DuckDB/pg_duckdb plan before patching executor lowering.

## Test Flow

**CRITICAL**: `max_parallel_workers = 0` MUST be set for pg_volvec to take over query execution. When PostgreSQL's native parallelism is enabled, the planner wraps the plan with `Gather` nodes and parallel-aware scan nodes that pg_volvec's `is_supported_plan` does not recognize. Without this setting, queries silently fall back to PostgreSQL's native executor and pg_volvec trace hooks will never fire. This applies even when testing pg_volvec's own parallel execution — pg_volvec uses its own BackgroundWorker system, independent of PostgreSQL's Gather/parallel scan machinery.

```sql
SET max_parallel_workers_per_gather = 0;
SET max_parallel_workers = 0;
SET min_parallel_table_scan_size = '1000GB';
SET parallel_setup_cost = 1000000000;
SET parallel_tuple_cost = 1000000000;
```

Use these entry points:

- Q1, closer to current supported shape: `contrib/pg_volvec/test_q1_no_parallel.sql`
- Q1, original form with `ORDER BY`: `contrib/pg_volvec/test_q1_10g.sql`
- Q6 on TPCH data: `contrib/pg_volvec/test_q6_10g.sql`
- Small local scripts: `contrib/pg_volvec/sql/q1.sql` and `contrib/pg_volvec/sql/q6.sql`

Before assuming `pg_volvec` is active, inspect the plan shape:

```sql
EXPLAIN (COSTS OFF) ...;
```

Enable hook logging when isolating offload behavior:

```sql
SET client_min_messages = LOG;
SET pg_volvec.trace_hooks = on;
```

## Benchmarking

Use `contrib/pg_volvec/scripts/bench_tpch_pg_vs_volvec.sh` for the current
TPC-H comparison matrix (Q1/Q5/Q6/Q7/Q8/Q9/Q10/Q12/Q14).

Basic usage:

```bash
cd /Users/chenyunwen/proj/postgres
RUNS=3 TIMEOUT_SEC=600 PG_WORKERS=14 VOLVEC_WORKERS=14 TRACE_EXECUTION_PATH=off \
  contrib/pg_volvec/scripts/bench_tpch_pg_vs_volvec.sh
```

Key environment variables:

```bash
RUNS=3
TIMEOUT_SEC=600
PG_WORKERS=14
VOLVEC_WORKERS=14
TRACE_EXECUTION_PATH=off
```

The script connects to `tpch` on `/tmp:5432`, runs each query in both
`pg_parallel` and `volvec_parallel` modes, and writes timestamped artifacts to:

- `contrib/pg_volvec/benchmarks/tpch_pg_vs_volvec_<timestamp>.tsv`
- `contrib/pg_volvec/benchmarks/tpch_pg_vs_volvec_<timestamp>.log`

Use the generated TSV as the benchmark record to compare against the latest
checked-in baseline under `contrib/pg_volvec/benchmarks/`.

## Profiling

### Recommended: xctrace --all-processes (captures leader + parallel workers)

Use `xctrace` with `--all-processes` to capture the full system Time Profiler. This is the **only** reliable way to profile pg_volvec parallel execution because pg_volvec parallel workers are spawned by postmaster as independent processes (not children of the query leader), so attaching to a single backend PID will miss worker samples.

**Step 1: Start xctrace recording in background**

```bash
mkdir -p /tmp/xctrace_profile
/usr/bin/xctrace record \
  --template 'Time Profiler' \
  --all-processes \
  --time-limit 240s \
  --output "/tmp/xctrace_profile/q6.trace" \
  --no-prompt &
XCTRACE_PID=$!
```

**Step 2: Run the query in another shell**

```bash
./installed/bin/psql -h /tmp -p 5432 -d tpch -v ON_ERROR_STOP=1 \
  -c "LOAD 'pg_volvec'; SET application_name='pg_volvec_xctrace'; SET jit=off; SET pg_volvec.enabled=on; SET pg_volvec.parallel=on; SET pg_volvec.parallel_max_workers=4; SET max_parallel_workers=8; SET max_parallel_workers_per_gather=0;" \
  -f contrib/pg_volvec/sql/q6.sql
```

**Step 3: After query completes, signal xctrace to save**

```bash
kill -INT $XCTRACE_PID
wait $XCTRACE_PID
```

**Step 4: Export symbolicated time-profile data**

```bash
/usr/bin/xctrace export \
  --input "/tmp/xctrace_profile/q6.trace" \
  --xpath "/trace-toc/run[@number='1']/data/table[@schema='time-profile']" \
  > "/tmp/xctrace_profile/q6.time-profile.xml"
```

**Step 5: Filter by postgres PIDs and convert to folded format**

From the XML, extract stacks belonging to `postgres` processes (leader + all workers), then fold:

```bash
# The XML contains <time-profile> rows with call stacks.
# Extract only postgres process samples, then collapse into folded format.
# Use a script to parse the XML, grouping by process name="postgres".
```

**Step 6: Generate flame graph**

```bash
perl /Users/chenyunwen/proj/postgres/FlameGraph/flamegraph.pl \
  --title "pg_volvec q6 xctrace" \
  "/tmp/xctrace_profile/q6.xctrace.postgres.folded" \
  > "/tmp/xctrace_profile/q6.xctrace.postgres.flame.svg"
```

Profile output directory: `contrib/pg_volvec/profiles/xctrace_parallel_samples_YYYYMMDD_HHMMSS/`

### Fallback: sample -> folded -> svg (single-threaded only)

For **non-parallel** queries only, `sample` on a single backend PID works:

```bash
/usr/bin/sample "$backend_pid" 5 1 -mayDie -file /tmp/pg_volvec.sample.txt
awk -f /Users/chenyunwen/proj/postgres/FlameGraph/stackcollapse-sample.awk \
  /tmp/pg_volvec.sample.txt > /tmp/pg_volvec.folded.txt
perl /Users/chenyunwen/proj/postgres/FlameGraph/flamegraph.pl \
  --title "pg_volvec backend" \
  /tmp/pg_volvec.folded.txt > /tmp/pg_volvec.flame.svg
```

### DTrace status on this machine

As of 2026-04-02, `dtrace` exists but is not usable as the current user. A direct probe returns:

- `system integrity protection is on, some features will not be available`
- `DTrace requires additional privileges`

So do not assume DTrace is available in normal development sessions. Treat it as an optional path that only works if the machine is configured with the needed privileges.

### Fast crash capture with LLDB

When a specific SQL reliably crashes a backend, it is often faster to attach `lldb` to that backend before running the statement than to infer the failure from logs afterward.

Recommended flow:

1. Open a dedicated `psql` session to the target database.
2. In that same session, run `SELECT pg_backend_pid();` and keep the session open.
3. In another shell, attach LLDB to that PID:

```bash
lldb -p <backend_pid>
```

4. At the LLDB prompt, allow the backend to continue:

```text
(lldb) process continue
```

5. Back in the original `psql` session, run the crashing SQL.
6. When the backend faults, LLDB will stop on the signal and immediately show the active frame.
7. Collect the stack before doing anything else:

```text
(lldb) bt
(lldb) thread backtrace all
(lldb) frame variable
```

This is especially useful for `pg_volvec` regressions because it quickly tells you whether the crash is in:

- page-wise scan plumbing
- tuple deform / JIT deform setup
- expression lowering
- result materialization back into PostgreSQL slots

For current Q1 work, prefer this over repeated blind reruns. If the crash lands in JIT-generated or deform-related code, inspect these files first:

- `contrib/pg_volvec/src/engine/executor.cpp`
- `contrib/pg_volvec/src/engine/llvmjit_deform_datachunk.cpp`
- `contrib/pg_volvec/src/engine/data_chunk_deform.hpp`

### Recommended output set

Keep all three files:

- raw sample text: `*.sample.txt`
- collapsed stacks: `*.folded.txt`
- human-readable flame graph: `*.flame.svg`

For query-specific profiling, use names like:

- `q1_pg_volvec_on.sample.txt`
- `q1_pg_volvec_on.folded.txt`
- `q1_pg_volvec_on.flame.svg`

### How to read the flame graph

- Ignore the very top frames like `dyld\`start` or process entry wrappers; they are just the root of the stack.
- Look for the widest non-root frames first; width corresponds to sampled time.
- For `pg_volvec`, pay special attention to frames containing:
  - `pg_volvec`
  - `llvmjit`
  - `ExecInitVecPlan`
  - `VecSeqScanState`
  - `DataChunkDeformer`
  - `HeapTupleSatisfiesVisibility`
  - tuple deform helpers
- If most width stays in generic PostgreSQL scan / visibility code, the vector path may not be active or may be dominated by page-wise scan overhead.
- If width clusters in JIT deform code, inspect `llvmjit_deform_datachunk.cpp` and the page-wise scan feed path together.

### Optional DTrace path

Only try this if the environment has the necessary privileges:

```bash
dtrace -x ustackframes=100 \
  -n 'profile-997 /pid == $target/ { @[ustack()] = count(); } tick-5s { exit(0); }' \
  -p "$backend_pid" > /tmp/pg_volvec.dtrace.txt
```

If that works, convert the output with FlameGraph tooling appropriate for the emitted format before generating the SVG. On this machine today, this path is blocked, so default back to `sample`.

## Current Boundaries

- Only root plans shaped as `Agg` or `SeqScan` are considered for offload.
- `Sort`, `Join`, `Materialize`, and similar wrappers are not offloaded yet.
- `Gather` nodes (produced when `max_parallel_workers > 0`) prevent offload entirely — always set `max_parallel_workers = 0` before testing.
- Qual compilation currently handles `Var`, `Const`, and `OpExpr`; multi-clause `BoolExpr` paths are fragile.
- Q1 with `ORDER BY` often becomes `Sort -> HashAggregate -> Seq Scan`, so it may not actually run through `pg_volvec`.
- Q6 matches the intended `Aggregate -> SeqScan` shape, so crashes there are strong signals about the active executor path.

## Debug Priorities

Treat current Q1 regressions as likely related to the page-wise scan and JIT deform work. The user reported Q1 used to run before Gemini changed that area.

Prioritize these files when debugging that regression:

- `contrib/pg_volvec/src/engine/executor.cpp`
- `contrib/pg_volvec/src/engine/llvmjit_deform_datachunk.cpp`
- `contrib/pg_volvec/src/engine/data_chunk_deform.hpp`

Use this sequence:

1. Reproduce on a tiny table with `contrib/pg_volvec/sql/q1.sql` or a stripped Q1 without `ORDER BY`.
2. Re-run with `pg_volvec.trace_hooks = on` and `EXPLAIN (COSTS OFF)` to verify whether the vector path is active.
3. Reproduce on `~/data/pg_tpch` only after the small repro is stable.
4. If a backend crashes, inspect `~/data/pg_tpch/logfile` and `~/data/pg_tpch/crash_log`.
5. If the cluster enters recovery mode, wait for it to accept connections again before continuing.

Keep in mind that Q6 instability may still involve expression lowering issues, but Q1 regressions are currently a good place to scrutinize the page-wise scan and JIT deform changes first.

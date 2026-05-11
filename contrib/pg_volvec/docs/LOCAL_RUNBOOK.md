# pg_volvec Local Runbook

> ⚠️ **STALE — Pre-greenfield (2026-04-17).** Most claims here are invalid post M-FRAME-MIN (`fd9a8aaf326`). For the current build/run/test commands see `contrib/pg_volvec/README.md` and `contrib/pg_volvec/AGENTS.md`.

Last refreshed: `2026-04-17`

This is the local development runbook for the `contrib/pg_volvec` prototype in
the PostgreSQL tree.

## 1. Workspace And Local Instance

Verified local setup:

- workspace root: `~/proj/postgres`
- extension directory: `contrib/pg_volvec`
- Meson build dir: `build`
- install prefix: `installed`
- local TPCH instance: `~/data/pg_tpch`
- socket: `/tmp`
- port: `5432`

Key code entry points:

- hook bridge: `contrib/pg_volvec/src/bridge/pg_volvec.c`
- query execution bridge: `contrib/pg_volvec/src/bridge/execute.cpp`
- vector operator layer: `contrib/pg_volvec/src/engine/exec/`
- parallel runtime: `contrib/pg_volvec/src/engine/parallel_runtime.cpp`
- expression JIT: `contrib/pg_volvec/src/engine/llvmjit_expr.cpp`
- deform JIT: `contrib/pg_volvec/src/engine/llvmjit_deform_datachunk.cpp`

## 2. Meson Build And Install

If `build/` does not exist yet:

```bash
meson setup build \
  --prefix="$(pwd)/installed" \
  -Dllvm=enabled \
  --buildtype=debugoptimized
```

Normal incremental build:

```bash
CCACHE_DISABLE=1 PATH=/opt/homebrew/bin:$PATH meson compile -C build pg_volvec
```

Install:

```bash
CCACHE_DISABLE=1 PATH=/opt/homebrew/bin:$PATH meson install -C build --only-changed
```

After every `meson install`, restart PostgreSQL before testing:

```bash
./installed/bin/pg_ctl -D ~/data/pg_tpch restart -m fast -l ~/data/pg_tpch/logfile
```

This restart is part of the normal workflow. Do not assume a backend will pick
up a freshly installed `pg_volvec` binary without it.

Useful health check:

```bash
./installed/bin/pg_isready -h /tmp -p 5432
```

## 3. Start, Stop, And Connect

Start:

```bash
./installed/bin/pg_ctl -D ~/data/pg_tpch -l ~/data/pg_tpch/logfile start
```

Stop:

```bash
./installed/bin/pg_ctl -D ~/data/pg_tpch stop
```

Status:

```bash
./installed/bin/pg_ctl -D ~/data/pg_tpch status
./installed/bin/pg_isready -h /tmp -p 5432
```

Connect:

```bash
./installed/bin/psql -h /tmp -p 5432 -d postgres
./installed/bin/psql -h /tmp -p 5432 -d tpch
```

Quick preload/GUC sanity:

```bash
./installed/bin/psql -h /tmp -p 5432 -d postgres -Atqc \
  "SHOW shared_preload_libraries; SHOW pg_volvec.enabled;"
```

## 4. Useful GUCs

Current user-facing `pg_volvec` GUCs:

- `pg_volvec.enabled`
- `pg_volvec.trace_hooks`
- `pg_volvec.trace_execution_path`
- `pg_volvec.jit_deform`
- `pg_volvec.parallel`
- `pg_volvec.parallel_max_workers`
- `pg_volvec.parallel_min_relation_blocks`
- `pg_volvec.parallel_leader_participation`
- `pg_volvec.parallel_experimental_hash_pipeline`
- `pg_volvec.profile`

SeqScan parallel work distribution is now a block pool: each worker's local
read stream callback atomically claims the next heap block from shared
`next_block` until `total_blocks` is exhausted. There is no user-facing morsel
size GUC; `pg_volvec.parallel_morsel_nblocks` was removed with the old
block-range scheduler.

Most common serial validation session:

```sql
LOAD 'llvmjit';
LOAD 'pg_volvec';
SET pg_volvec.enabled = on;
SET pg_volvec.parallel = off;
SET pg_volvec.trace_hooks = on;
SET jit = on;
SET max_parallel_workers = 0;
SET max_parallel_workers_per_gather = 0;
SET min_parallel_table_scan_size = '1000GB';
SET parallel_setup_cost = 1000000000;
SET parallel_tuple_cost = 1000000000;
```

Most common `pg_volvec` process-parallel session:

```sql
LOAD 'llvmjit';
LOAD 'pg_volvec';
SET pg_volvec.enabled = on;
SET pg_volvec.parallel = on;
SET pg_volvec.parallel_max_workers = 4;
SET pg_volvec.profile = off;
SET max_parallel_workers = 8;
SET max_parallel_workers_per_gather = 4;
SET pg_volvec.trace_hooks = on;
SET jit = on;
```

Useful query-specific notes:

- Q3: set `enable_eager_aggregate = off`
- Q12: `SET pg_volvec.parallel_leader_participation = off` is often a cleaner
  validation mode
- Q11: current process-parallel path is correct, but nested hash-build still
  falls back to a leader-built shared hash bridge

## 5. Smoke And Query Entry Points

Standalone smoke scripts:

- `contrib/pg_volvec/tests/standalone/test_q1_no_parallel.sql`
- `contrib/pg_volvec/tests/standalone/test_q1_10g.sql`
- `contrib/pg_volvec/tests/standalone/test_q6_10g.sql`
- `contrib/pg_volvec/tests/standalone/test_q1_trace.sql`

TPC-H query files used for most real runs:

- `contrib/pg_volvec/tpch_queries/q1.sql`
- `contrib/pg_volvec/tpch_queries/q6.sql`
- `contrib/pg_volvec/tpch_queries/q11.sql`
- `contrib/pg_volvec/tpch_queries/q12.sql`
- `contrib/pg_volvec/tpch_queries/q17.sql`

Examples:

```bash
./installed/bin/psql -h /tmp -p 5432 -d tpch \
  -f contrib/pg_volvec/tests/standalone/test_q1_no_parallel.sql
```

```bash
./installed/bin/psql -h /tmp -p 5432 -d tpch \
  -f contrib/pg_volvec/tests/standalone/test_q1_10g.sql
```

```bash
./installed/bin/psql -h /tmp -p 5432 -d tpch \
  -f contrib/pg_volvec/tests/standalone/test_q6_10g.sql
```

Direct TPCH query run:

```bash
./installed/bin/psql -h /tmp -p 5432 -d tpch \
  -f contrib/pg_volvec/tpch_queries/q11.sql
```

Before assuming `pg_volvec` is active, inspect the plan shape:

```sql
EXPLAIN (COSTS OFF, VERBOSE) ...;
```

## 6. Benchmark Workflow

Checked-in benchmark scripts:

- `contrib/pg_volvec/scripts/bench_three_way.sh`
- `contrib/pg_volvec/scripts/bench_duckdb_vs_volvec_native.sh`
- `contrib/pg_volvec/tests/bench_tpch_supported.sh`
- `contrib/pg_volvec/tests/bench_tpch_local.sh`

Main checked-in benchmark artifacts:

- `contrib/pg_volvec/benchmarks/tpch_perf_snapshot.tsv`
- `contrib/pg_volvec/benchmarks/tpch_perf_snapshot.svg`
- `contrib/pg_volvec/benchmarks/tpch_perf_pg_parallel14_vs_pg_volvec_parallel14_20260414_170932.tsv`
- `contrib/pg_volvec/benchmarks/tpch_perf_pg_parallel14_vs_pg_volvec_parallel14_20260414_170932.svg`
- `contrib/pg_volvec/benchmarks/tpch_pg_vs_volvec_20260511_174551.tsv`
- `contrib/pg_volvec/benchmarks/tpch_pg_vs_volvec_20260511_174551.log`

Two benchmark modes matter:

### Fair executor/engine comparison

- PostgreSQL parallel disabled for all engines
- compare native PG, `pg_duckdb`, and `pg_volvec`
- this is the best apples-to-apples view of executor/engine quality

### Native PG parallel vs `pg_volvec` parallel

- native PostgreSQL gets its own `Gather`/parallel plan
- `pg_volvec` gets `pg_volvec.parallel=on`
- useful for understanding whether `pg_volvec`'s current process runtime is
  competitive on each query shape

When benchmarking:

- keep only one client running the measured query
- median-of-3 is the default checkpoint method
- use a hard timeout bucket for long-tail queries

## 7. Profiling

Default path: `sample -> folded -> flamegraph`.

```bash
/usr/bin/sample "$backend_pid" 5 1 -mayDie -file /tmp/pg_volvec.sample.txt
awk -f ./FlameGraph/stackcollapse-sample.awk \
  /tmp/pg_volvec.sample.txt > /tmp/pg_volvec.folded.txt
perl ./FlameGraph/flamegraph.pl \
  --title "pg_volvec backend" \
  /tmp/pg_volvec.folded.txt > /tmp/pg_volvec.flame.svg
```

Preferred output set:

- raw sample text
- folded stacks
- flame SVG
- short note on top stacks / conclusion

To capture the backend PID before a long query:

```sql
LOAD 'llvmjit';
LOAD 'pg_volvec';
SET pg_volvec.enabled = on;
SELECT pg_backend_pid();
SELECT pg_sleep(1);
... heavy query ...
```

The repository also contains `xctrace`-based captured profiles under
`contrib/pg_volvec/profiles/` from earlier investigations. They are useful as
 reference material but are not required for the normal profiling loop.

## 8. Fast Crash Debugging With LLDB

When a SQL reliably crashes a backend, attach to that backend before executing
the statement.

In the target `psql` session:

```sql
SELECT pg_backend_pid();
```

In another shell:

```bash
lldb -p <backend_pid>
```

Continue in LLDB:

```text
(lldb) process continue
```

Back in `psql`, run the crashing SQL. When it faults, immediately collect:

```text
(lldb) bt
(lldb) thread backtrace all
(lldb) frame variable
```

This is the fastest way to isolate regressions in:

- page-wise scan plumbing
- tuple deform / deform JIT
- expression lowering / JIT
- slot materialization
- parallel worker setup / teardown

## 9. Current Coverage Snapshot

Fully verified offloaded TPCH queries:

- Q1
- Q3
- Q4
- Q5
- Q6
- Q7
- Q8
- Q9
- Q10
- Q11
- Q12
- Q13
- Q14
- Q15
- Q16
- Q18
- Q19
- Q20
- Q22

Offloaded with narrower validation:

- Q2
- Q17
- Q21

Current process-parallel notes worth remembering:

- Q3 needs `enable_eager_aggregate = off`
- Q4 works through the process-worker path
- Q11 is correct but still uses a safe leader-built shared hash bridge on the
  nested build chain
- Q12 is correct, but worker-local hash build is still duplicated
- Q16 supports grouped `COUNT(DISTINCT int-like)` merge
- Q17 is correct on the live dataset in the current process-worker path, but
  the full original-query native diff is still limited by native PG timeout

## 10. Current Limitations

- `Wide128` expression JIT is not implemented
- sort is still first-cut in-memory only
- nested hash-build chains are not yet fully worker-shared
- broad semi/anti join execution is not implemented
- `Materialize` / rescan-heavy shape coverage is still limited
- Q21 is intentionally not the default next executor target

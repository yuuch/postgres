# pg_volvec

`pg_volvec` is a PostgreSQL extension prototype that keeps PostgreSQL planning
unchanged and offloads supported OLAP plan subtrees into a vectorized executor.

> **⚠️ Phase notice — M-Q1-PERF / M-Q6-RESTORE in progress.**
> The previous broad TPC-H prototype (Q1–Q22, HashJoin, MergeJoin,
> SubqueryScan, the legacy serial vector executor under `src/engine/exec/`,
> and the legacy morsel parallel runtime) was **intentionally deleted** in
> commits `53ac06adcb7` (step 1) and `fd9a8aaf326` (step 2) and replaced
> by a DuckDB-style `PhysicalOperator` + `MetaPipeline` runtime. As of HEAD
> `6c344eb036d` plus the current uncommitted work, the new runtime is
> plumbed end-to-end: **SeqScan → HashAgg → Order → OutputSink** runs in
> leader+workers, cross-process descriptor publish/load works, and the
> leader drains the global TDC after FINALIZE. Q1 remains the primary
> performance target (`M-Q1-PERF`), Q6 `SeqScan -> HashAgg` is restored for
> the canonical TPC-H filter/`SUM(extprice*discount)` path, and the current
> benchmark matrix also includes Q3 for exploratory coverage. Anything in
> `docs/` dated `2026-04-17`
> describes the pre-greenfield codebase and should not be trusted; see
> `docs/ARCHITECTURE_DEVIATIONS.md` for the diff vs the reference
> architecture in `pg_duckdb_architecture.md`.

## Current runtime architecture

- PostgreSQL still owns parsing, rewriting, planning, snapshots, and result
  delivery.
- `pg_volvec` hooks `ExecutorStart` / `ExecutorRun` / `ExecutorFinish` /
  `ExecutorEnd` and, for admitted plan subtrees, replaces execution with a
  `DataChunk`-oriented columnar engine.
- Plans are translated into a `pg_volvec::pipeline::PhysicalOperator` tree
  (DuckDB-faithful unified `Source/Operator/Sink` base, see
  `src/engine/parallel/pipeline/physical_operator.hpp`).
- The tree is sliced into `MetaPipeline` chains at blocking operators
  (`HashAggregate`, `Order`) and dispatched as Tasks over a DSM `TaskQueue`
  to PostgreSQL parallel bgworkers.
- SeqScan uses page-wise block claiming plus **tuple deform JIT**
  (`llvmjit_deform_datachunk.cpp`) to decode heap tuples directly into typed
  column arrays.
- Expression evaluation lowers to a linear IR (`expr.cpp`) and, when
  supported, compiles to fused LLVM row loops (`llvmjit_expr.cpp`).
- TPC-H-style `NUMERIC(15,2)` runs as scaled `int64` in the hot path with
  widened accumulation for aggregation. `Wide128` exact numerics stay on the
  interpreter path (no JIT, by design, for correctness).

## Current Status

Status refreshed: `2026-05-12` (HEAD `6c344eb036d` + uncommitted runtime fixes; includes the current scan tracing / AggDelta cleanup work and the latest benchmark refresh)

### What runs through `pg_volvec` today

For Q1, `Translator::Translate` now produces a real `PhysicalOperator` tree,
the bundle slices into 3 `MetaPipeline` chains (`P0=[Order→Output]`,
`P1=[HashAgg→Order]`, `P2=[SeqScan→HashAgg]`), workers attach DSA, drain
`DsmTaskQueue`, write `DataChunk` into the global TDC, and the leader runs
FINALIZE → drains the TDC into the `DestReceiver`. Q6's canonical `SeqScan ->
plain Agg` shape also runs through this path when `pg_volvec.parallel=on`;
its multi-clause date and numeric filters are represented by `QualDescriptor`
and evaluated in `PhysicalSeqScan` before projecting
`l_extendedprice * l_discount` into `SUM_NUMERIC`. Q10 is the current join-
heavy validated path, and Q3 is now part of the benchmark matrix for
exploratory coverage. See `AGENTS.md` for the detailed bug ledger.

Current checked-in benchmark coverage is Q1 / Q3 / Q5 / Q6 / Q7 / Q8 / Q9 /
Q10 / Q12 / Q14 via `contrib/pg_volvec/scripts/bench_tpch_pg_vs_volvec.sh`.
The validated core remains Q1 / Q6 / Q10; Q3 is benchmarked but not yet
treated as a primary validation target.

Latest PG-vs-VolVec comparison (`benchmarks/tpch_pg_vs_volvec_20260511_184037.tsv`, 2026-05-11 18:40:37):

| query | PG parallel | VolVec parallel | speedup |
|---|---:|---:|---:|
| Q1 | 3.057 s | 1.461 s | 2.09x |
| Q5 | 3.353 s | 2.091 s | 1.60x |
| Q6 | 1.399 s | 0.964 s | 1.45x |
| Q7 | 1.757 s | 1.684 s | 1.04x |
| Q8 | 3.853 s | 1.677 s | 2.30x |
| Q9 | 7.572 s | 3.562 s | 2.13x |
| Q10 | 2.841 s | 1.760 s | 1.61x |
| Q12 | 1.815 s | 1.302 s | 1.39x |
| Q14 | 1.249 s | 1.221 s | 1.02x |

Best current win in that snapshot is Q8 at ~2.30x; Q1 is ~2.09x.

### Current implementation line: `M-FRAME-MIN`

Sequential rebuild on top of the greenfield deletion:

| Step | Status | Commit |
|------|--------|--------|
| Step 1: aggressive teardown of legacy executor + parallel runtime | ✅ done | `53ac06adcb7` |
| Step 2: extract `Translator` + close namespace leak | ✅ done | `fd9a8aaf326` |
| Step 3: `MetaPipeline` runtime (Event DAG, `Task`, DSM `TaskQueue`, `TaskScheduler`) | 🚧 plumbed end-to-end; closing Bug G + Bug I | `6c344eb036d` (last) |
| Step 4: Q1 shape-matcher inside `Translator::TranslatePlan` | ✅ done (Q1 plumbed) | uncommitted |

### Current workstreams

- **`M-Q1-PERF`** — continue driving Q1 through the new pipeline runtime;
  perf tracked in `perf/q1_p3x_progression.md` and the checked-in benchmark
  artifacts under `benchmarks/`.
- **`M-Q6-RESTORE`** — keep the canonical Q6 `SeqScan -> plain Agg` path
  stable with date/numeric filter conjunctions.
- **Q10 / join-heavy validation** — keep the validated join-fed path healthy
  while the runtime matures.
- **Q3 benchmark coverage** — included in the current benchmark matrix for
  exploratory comparison, but not yet a primary validation target.

### Out of scope (intentionally deleted, do not reintroduce)

HashJoin, MergeJoin, SubqueryScan, the legacy `src/engine/exec/` vector
operators (`Vec{SeqScan,Filter,Agg,Project,Sort,Limit}State`), the legacy
parallel runtime (`parallel_runtime.cpp`, `runtime_*.{cpp,hpp,inc}`,
`ParallelPipelineRole/Desc/Driver/Sink`, `TaskKind`, all `PIPELINE_DSM_KEY_*`
of the previous era), `LoweredPipeline`, `WorkerPipelineExecutor`,
`ParallelAggPartialState`, `partial_agg_op.{hpp,cpp}`, `agg_sink.{hpp,cpp}`,
`seq_scan_source.{hpp,cpp}`, `pipeline_lowering.{hpp,cpp}`, `q1_translator.*`.

Q2 / Q4 / Q11 / Q13 / Q15 / Q16 / Q17 / Q18 / Q19 / Q20 / Q21 / Q22 are all
out of scope for the current phase. Q3 is benchmarked, but still treated as
exploratory coverage rather than a primary supported shape.

## Build and install

Use PostgreSQL's top-level Meson build.

If `build/` does not exist yet:

```bash
meson setup build \
  --prefix="$(pwd)/installed" \
  -Dllvm=enabled \
  --buildtype=debugoptimized
```

Normal development cycle:

```bash
CCACHE_DISABLE=1 PATH=/opt/homebrew/bin:$PATH meson compile -C build pg_volvec
CCACHE_DISABLE=1 PATH=/opt/homebrew/bin:$PATH meson install -C build --only-changed
./installed/bin/pg_ctl -D ~/data/pg_tpch restart -m fast -l ~/data/pg_tpch/logfile
```

After every `meson install`, **always** restart PostgreSQL before testing.
The backend will not pick up a freshly installed `pg_volvec.so` on its own.

## Test and benchmark

```bash
# Regress (smoke + q1 + q6 only) — invoked via the suite name
meson test -C build --suite pg_volvec
# NOTE: `meson test -C build pg_volvec` (bare test name) currently does NOT
# match. expected/*.out is stale vs HEAD (it pre-dates the WARNING fall-through
# and the harness's echo-mode change); test failures are intentional until
# M-Q1-PERF.

# Manual on the 10G TPC-H instance (set pg_volvec.parallel=on for runtime path)
./installed/bin/psql -h /tmp -p 5432 -d tpch -f contrib/pg_volvec/sql/q1.sql
./installed/bin/psql -h /tmp -p 5432 -d tpch -f contrib/pg_volvec/sql/q6.sql
```

For Q6 and other TPC-H repros, enable pg_volvec's runtime explicitly and
disable PostgreSQL's own parallel executor unless the test is explicitly about
PG parallel plan shapes. This keeps `Gather`/parallel scan out of the
PostgreSQL plan and isolates the `pg_volvec` runtime behavior:

```sql
SET pg_volvec.enabled = on;
SET pg_volvec.parallel = on;
SET max_parallel_workers_per_gather = 0;
SET max_parallel_workers = 0;
SET min_parallel_table_scan_size = '1000GB';
SET parallel_setup_cost = 1000000000;
SET parallel_tuple_cost = 1000000000;
```

Expected results today:

- Q1 on the small fixture: 2 rows (`A/F`, `B/O`).
- Q6 on 10G TPC-H: `1230113636.0101` through the pg_volvec runtime when
  `pg_volvec.parallel=on` and PostgreSQL parallelism is disabled as above.

Current benchmark artifact:

- `benchmarks/tpch_pg_vs_volvec_20260511_184037.tsv` (2026-05-11 18:40:37)

## Project layout

```
contrib/pg_volvec/
  src/bridge/                        # PG hook integration, GUCs, query-state HTAB
    pg_volvec.c                      # _PG_init, GUC table, hook chaining
    state.{c,h}                      # PgVolVecQueryState HTAB + admission filter
    execute.{cpp,h}                  # Translator dispatch + pipeline run
    AGENTS.md
  src/engine/core/                   # DataChunk, types, memory, DSA bridge,
                                     #   robin-hood adapter (foundation layer)
    AGENTS.md
  src/engine/expr/                   # Expression IR header (expr.hpp)
  src/engine/                        # expr.cpp (lowering+interpreter), JIT pair,
                                     #   volvec_engine.hpp
  src/engine/parallel/               # Container only; no source files
    AGENTS.md
  src/engine/parallel/pipeline/      # PhysicalOperator IR + (in-flight) MetaPipeline
                                     #   runtime — the only runtime
    AGENTS.md
  sql/, expected/                    # Regress: smoke + q1 + q6 only
  docs/                              # Mostly pre-greenfield; see banners
  third_party/                       # Vendored robin-hood-hashing
  perf/                              # Q1 perf progression notes
```

## Built sources (per `meson.build`)

22 active translation units:

- C bridge: `src/bridge/pg_volvec.c`, `src/bridge/state.c`
- C++ bridge: `src/bridge/execute.cpp`
- Expression: `src/engine/expr.cpp`
- Pipeline IR + MetaPipeline runtime (15):
  `src/engine/parallel/pipeline/{translator,physical_seq_scan,
  physical_hash_aggregate,physical_order,physical_projection,
  output_sink,pipeline_leader,pipeline_worker_main,
  physical_operator,meta_pipeline,pipeline_descriptor,
  event,pipeline_run_event,pipeline_combine_event,
  pipeline_finalize_event,task,dsm_task_queue,task_scheduler,
  aggregate_hash_table,tuple_data_collection,tuple_data_layout,
  tuple_data_ops}.cpp`
- Core DSA: `src/engine/core/parallel_dsa_bridge.cpp`
- LLVM JIT pair (when `llvm.found()`):
  `src/engine/llvmjit_deform_datachunk.cpp`,
  `src/engine/llvmjit_expr.cpp`

`src/engine/llvmjit_deform_datachunk.h` (note `.h`, not `.hpp`) is a stale
duplicate decl of `pg_volvec_try_compile_jit_expr` from `expr/expr.hpp`;
slated for cleanup at JIT-wiring time.

## Docs

Authoritative (refreshed for HEAD):

- [AGENTS.md](AGENTS.md) — top-level knowledge base (refreshed `2026-04-30`, HEAD `6c344eb036d` + uncommitted runtime)
- [src/bridge/AGENTS.md](src/bridge/AGENTS.md) — hook layer, GUCs,
  admission, dispatch
- [src/engine/parallel/pipeline/AGENTS.md](src/engine/parallel/pipeline/AGENTS.md)
  — PhysicalOperator IR + Translator + MetaPipeline runtime (Bug A–H ledger)
- [src/engine/parallel/AGENTS.md](src/engine/parallel/AGENTS.md) — pipeline
  container conventions
- [src/engine/core/AGENTS.md](src/engine/core/AGENTS.md) — `DataChunk`,
  types, memory, DSA bridge
- [docs/ARCHITECTURE_DEVIATIONS.md](docs/ARCHITECTURE_DEVIATIONS.md) — diff
  between current implementation and the reference architecture in
  `pg_duckdb_architecture.md` (D1–D16 + open Bug G/I)
- [docs/PIPELINE_PORT_PLAN.md](docs/PIPELINE_PORT_PLAN.md) — M-FRAME-MIN
  port plan (mirror of `.sisyphus/plans/pipeline-port-plan.md`)
- [docs/GLOBAL_LOCAL_STATE_DESIGN.md](docs/GLOBAL_LOCAL_STATE_DESIGN.md) —
  Global/Local state design (mirror of
  `.sisyphus/plans/global-local-state-design.md`)

Pre-greenfield (kept for reference, banner-marked `STALE`):

- [docs/DESIGN.md](docs/DESIGN.md)
- [docs/ROADMAP.md](docs/ROADMAP.md)
- [docs/LOCAL_RUNBOOK.md](docs/LOCAL_RUNBOOK.md)
- [docs/jit_deform_datachunk.md](docs/jit_deform_datachunk.md)
- [docs/llvmjit_expr.md](docs/llvmjit_expr.md)
- [docs/page-wise-scan.md](docs/page-wise-scan.md)

Plans live in `.sisyphus/plans/{pipeline-port-plan,global-local-state-design}.md`
(source of truth); `docs/` mirrors are kept in sync at milestone boundaries.

## License

PostgreSQL License.

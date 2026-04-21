# pg_volvec

`pg_volvec` is a PostgreSQL extension prototype that keeps PostgreSQL planning
unchanged and offloads supported OLAP plan subtrees into a vectorized
executor.

## What It Is

- PostgreSQL still owns parsing, rewriting, planning, snapshots, and result
  delivery.
- `pg_volvec` hooks `ExecutorStart` / `ExecutorRun` / `ExecutorEnd` and
  replaces supported plan regions with a `DataChunk`-oriented columnar engine.
- Scan hot paths use tuple deform JIT to decode heap tuples directly into
  typed column arrays.
- Expression evaluation lowers to a linear IR and, when supported, compiles to
  fused LLVM row loops so intermediate vectors do not have to be materialized.
- TPC-H-style `NUMERIC(15,2)` values run as scaled `int64` in the hot path,
  with widened accumulation for aggregation.
- Wider exact numeric expressions use a correctness-first `Wide128` path.
  That path is intentionally interpreter-only today.
- Process-parallel execution is lowered into a pipeline DAG plus
  morsel-driven runtime, while PostgreSQL's own planner remains the source of
  truth for the physical plan shape.

In short: PostgreSQL planner on top, `pg_volvec` vector executor underneath,
with JIT on both tuple deform and expression evaluation.

## Current Status

Status refreshed: `2026-04-17`

Fully verified offloaded TPC-H queries:

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

Current executor surface:

- `SeqScan`
- `Filter`
- grouped / ungrouped `Agg`
- in-memory final `Sort`
- constant-count `Limit`
- `HashJoin`
- current hash-backed right/left outer join subset
- `SubqueryScan`
- `MergeJoin`-planned shapes through a hash-backed fallback
- current Q22-style right-anti-planned shapes through a hash-backed fallback

Important current boundaries:

- `Wide128` exact numeric expression programs do not use expression JIT yet.
- int-like comparison opcodes and numeric division currently stay on the
  interpreter path for correctness.
- `VecSortState` is still a first-cut in-memory single-run sort.
- Process-parallel execution is live on the validated query family, but nested
  hash-build dependency chains may still fall back to a leader-built shared
  hash bridge. Q11 is the important example.
- Q21 is no longer the default next executor target. The dominant local pain
  point there looks more like PostgreSQL planner quality than a missing
  executor primitive.

## Benchmark Artifacts

### Fair Three-Way Baseline

Checked-in artifact:

- [benchmarks/tpch_perf_snapshot.svg](benchmarks/tpch_perf_snapshot.svg)
- [benchmarks/tpch_perf_snapshot.tsv](benchmarks/tpch_perf_snapshot.tsv)

This sweep is the current broad three-way comparison across PostgreSQL,
`pg_duckdb`, and `pg_volvec`.

- Run date: `2026-04-09`
- Method: median of 3 runs
- PostgreSQL parallel query: disabled for all three engines
- Timeout bucket: `180s`

Quick read:

- Across the direct `OK vs OK vs OK` comparisons, `pg_volvec` is fastest more
  often than either native PostgreSQL or `pg_duckdb`.
- `pg_volvec` is especially strong on Q1 / Q5 / Q6 / Q7 / Q8 / Q11 / Q12 /
  Q14 / Q18 / Q19 / Q22 in this fair no-PG-parallel setting.
- Q2 remains incomplete for `pg_volvec` in this sweep.

![TPC-H timing comparison](benchmarks/tpch_perf_snapshot.svg)

### PG Parallel vs pg_volvec Parallel

Checked-in artifact:

- [benchmarks/tpch_perf_pg_parallel14_vs_pg_volvec_parallel14_20260414_170932.svg](benchmarks/tpch_perf_pg_parallel14_vs_pg_volvec_parallel14_20260414_170932.svg)
- [benchmarks/tpch_perf_pg_parallel14_vs_pg_volvec_parallel14_20260414_170932.tsv](benchmarks/tpch_perf_pg_parallel14_vs_pg_volvec_parallel14_20260414_170932.tsv)

This sweep compares native PostgreSQL with `14` parallel workers against
`pg_volvec`'s own process-parallel runtime with `14` workers.

- Run date: `2026-04-14`
- Method: median of 3 runs
- This is not the same fairness criterion as the three-way baseline above
- It is useful for identifying where `pg_volvec`'s current parallel runtime is
  already competitive and where worker setup / hash-build strategy still
  loses to PostgreSQL's native parallel executor

Notable outcomes in this checkpoint:

- `pg_volvec` clearly wins on Q1 / Q5 / Q9 / Q16 / Q17 / Q20 / Q21.
- Native PostgreSQL still wins on Q3 / Q4 / Q6 / Q7 / Q8 / Q10 / Q11 / Q12 /
  Q13 / Q14 / Q15 / Q18 / Q22.
- Q4 and Q12 are the clearest reminders that current process-worker lowering
  is functionally broad but not yet performance-closed.

### Latest PostgreSQL Parallel Baseline

Checked-in artifact:

- [benchmarks/tpch_supported_twice_20260421_154310.tsv](benchmarks/tpch_supported_twice_20260421_154310.tsv)

This is the latest checked-in native PostgreSQL parallel baseline snapshot in
the `bench_supported_twice` format.

- Run date: `2026-04-21`
- Engine: native PostgreSQL parallel
- Source artifact mode: `pg_parallel`
- Measurement column: `best_ms`
- Note: this artifact captures the PostgreSQL side only; `q20` hit statement
  timeout in this run

| Query | Best ms | Status |
|---|---:|---|
| Q1 | 4110 | ok |
| Q3 | 3062 | ok |
| Q4 | 3069 | ok |
| Q5 | 2044 | ok |
| Q6 | 2057 | ok |
| Q7 | 2044 | ok |
| Q8 | 4077 | ok |
| Q9 | 8095 | ok |
| Q10 | 3049 | ok |
| Q11 | 1040 | ok |
| Q12 | 2039 | ok |
| Q13 | 12097 | ok |
| Q14 | 2034 | ok |
| Q15 | 4056 | ok |
| Q16 | 1026 | ok |
| Q18 | 57860 | ok |
| Q19 | 3050 | ok |
| Q20 | 360402 | error (statement timeout) |
| Q22 | 1035 | ok |

## Build And Install

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

After every `meson install`, restart PostgreSQL before testing. Do not assume
the backend will pick up a freshly installed `pg_volvec` binary without a
restart.

## Project Layout

- `src/bridge/`: PostgreSQL hook integration, query-state registry, and
  tuple/materialization handoff
- `src/engine/core/`: low-level shared primitives such as `DataChunk`,
  allocators, and serialized hash metadata
- `src/engine/expr/` plus `src/engine/expr.cpp`: expression IR surface plus
  lowering/interpreter
- `src/engine/exec/`: vector operator and plan-state layer (`SeqScan`, `Agg`,
  `HashJoin`, `Sort`, `Filter`, `Limit`, and helpers), built as separate
  translation units
- `src/engine/parallel/` plus `src/engine/parallel_runtime.cpp`: pipeline
  lowering and process-parallel scheduler/runtime
- `src/engine/llvmjit_expr.cpp`: expression JIT
- `src/engine/llvmjit_deform_datachunk.cpp`: tuple deform JIT
- `src/engine/executor.cpp`: reference file kept in-tree, but no longer built
  by Meson

## Docs

- [docs/LOCAL_RUNBOOK.md](docs/LOCAL_RUNBOOK.md): local build, install,
  startup, smoke, benchmark, and profiling workflow
- [docs/DESIGN.md](docs/DESIGN.md): current executor architecture
- [docs/module_split.md](docs/module_split.md): module split record
- [docs/morsel_parallel_design.md](docs/morsel_parallel_design.md): parallel
  runtime model and current boundaries
- [docs/parallel_plan_normalization.md](docs/parallel_plan_normalization.md):
  how accepted PostgreSQL parallel plans are normalized into `pg_volvec`
- [docs/llvmjit_expr.md](docs/llvmjit_expr.md): expression JIT notes
- [docs/jit_deform_datachunk.md](docs/jit_deform_datachunk.md): deform JIT
  notes
- [docs/vecSortDesign.md](docs/vecSortDesign.md): current sort design
- [docs/page-wise-scan.md](docs/page-wise-scan.md): scan/read-stream notes
- [docs/ROADMAP.md](docs/ROADMAP.md): medium-term direction
- [docs/TODO.md](docs/TODO.md): immediate actionable work

## License

PostgreSQL License.

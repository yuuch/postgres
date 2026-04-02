# pg_volvec: PostgreSQL Vectorized Volcanic Executor

`pg_volvec` is a PostgreSQL extension prototype that offloads a narrow but useful class of OLAP plans to a vectorized executor. It keeps PostgreSQL planning unchanged, hooks `ExecutorStart` / `ExecutorRun`, and executes supported subtrees in a `DataChunk`-oriented engine.

## Verified Status

The current code path is no longer just a skeleton. As of April 2, 2026, the following have been verified on the local `~/data/pg_tpch` instance:

- Single-table `SeqScan -> optional qual -> Agg` shapes run in `pg_volvec`.
- `Sort -> Agg -> SeqScan` is now supported for the current Q1 shape.
- TPC-H Q1 with and without `ORDER BY` runs correctly and is offloaded.
- TPC-H Q6 runs correctly and is offloaded.
- Tuple deform JIT works without manually `LOAD 'llvmjit'`; the provider is auto-loaded when needed.
- Expression JIT is wired in and generates fused row loops instead of materializing intermediate vector temporaries.
- `NUMERIC(15,2)` hot paths use scaled `int64`, while aggregation uses widened integer accumulation.
- A first columnar `VecSortState` exists for in-memory final-result sorting.

For the workspace-validated build, install, startup, test, profile, and benchmark commands, see [LOCAL_RUNBOOK.md](LOCAL_RUNBOOK.md).

## Highlights

### Vectorized execution

Operators exchange `DataChunk` batches instead of row-at-a-time `TupleTableSlot` values. This reduces executor overhead and keeps hot loops regular enough for LLVM optimization.

### Pruned row-to-column deform

The scan node no longer deforms a fixed prefix of attributes unconditionally. It derives the needed attribute set from scan quals and upper targetlists, builds a pruned `DeformProgram`, and only materializes the columns the rest of the plan needs.

### JIT deform to `DataChunk`

`HeapTupleHeader` values are decoded directly into typed `DataChunk` arrays through LLVM-generated code. The current implementation supports the TPC-H-relevant scalar mix used by Q1 and Q6, including fixed-point numeric decoding and string-prefix materialization for grouping keys.

### Fused expression JIT

`VecExprProgram` still uses a linear step IR, but the hot path no longer interprets every step. For supported programs, LLVM emits a fused loop that computes the final result directly for each active row. Intermediate values stay in SSA temporaries instead of being written to `tmp[]` arrays.

### Vectorized final sort

For the current full-Q1 shape, `pg_volvec` now keeps the aggregated result columnar, materializes dense sort-owned chunks, sorts row references indirectly by extracted key lanes, and gathers the final ordered output back into `DataChunk`s. The first cut is an in-memory single-run sort aimed at the top-level Q1 `ORDER BY`.

### Fixed-point numeric execution

For TPC-H-style `NUMERIC(15,2)` values, the executor uses scaled `int64` inputs and widened arithmetic for aggregation. This removes the old `numeric_float8_no_overflow()` bottleneck from the hot scan/deform path.

## Local Performance Notes

These are local, hot-cache measurements on the developer machine with parallel query disabled in the session. They are useful as engineering checkpoints, not as broad product claims.

- Q6, 3 alternating runs:
  - native PostgreSQL average: `3.72s`
  - `pg_volvec` average: `2.88s`
  - speedup: about `1.29x`
- Q1 no-order supported shape, 3 alternating runs:
  - native PostgreSQL average: `21.83s`
  - `pg_volvec` average: `4.87s`
  - speedup: about `4.48x`
- Q1 full SQL form with `ORDER BY`, 1 local hot-cache run:
  - native PostgreSQL: `21.16s`
  - `pg_volvec`: `5.74s`
  - speedup: about `3.69x`

The newest Q6 flame graph also shows the expression interpreter hotspot largely disappearing; the backend is now primarily I/O-bound on that query.

## Build And Install

Use PostgreSQL's top-level Meson build, not an ad-hoc extension-local build.

```bash
meson setup build \
  --prefix=/Users/chenyunwen/proj/postgres/installed \
  -Dllvm=enabled \
  --buildtype=debugoptimized

meson compile -C build pg_volvec
meson install -C build --only-changed
```

## Project Layout

- `src/bridge/`: PostgreSQL hook integration and result handoff
- `src/engine/executor.cpp`: vectorized plan initialization and scan / filter / agg operators
- `src/engine/expr.cpp`: expression lowering and interpreter
- `src/engine/llvmjit_expr.cpp`: fused expression JIT
- `src/engine/llvmjit_deform_datachunk.cpp`: tuple deform JIT
- `src/engine/executor.cpp`: also contains the current `VecSortState` implementation
- `tests/`: helper scripts for local benchmarking and profiling

## Additional Docs

- [LOCAL_RUNBOOK.md](LOCAL_RUNBOOK.md): validated local commands and latest progress snapshot
- [DESIGN.md](DESIGN.md): current architecture overview
- [llvmjit_expr.md](llvmjit_expr.md): current expression JIT design
- [jit_deform_datachunk.md](jit_deform_datachunk.md): current deform JIT design
- [vecSortDesign.md](vecSortDesign.md): current vectorized sort design
- [page-wise-scan.md](page-wise-scan.md): page-wise scan design and current status
- [TODO.md](TODO.md): next engineering steps

## License

PostgreSQL License.

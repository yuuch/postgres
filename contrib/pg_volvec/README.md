# pg_volvec: PostgreSQL Vectorized Volcanic Executor

`pg_volvec` is a PostgreSQL extension prototype that offloads a narrow but useful class of OLAP plans to a vectorized executor. It keeps PostgreSQL planning unchanged, hooks `ExecutorStart` / `ExecutorRun`, and executes supported subtrees in a `DataChunk`-oriented engine.

## Verified Status

The current code path is no longer just a skeleton. As of April 3, 2026, the following have been verified on the local `~/data/pg_tpch` instance:

- Single-table `SeqScan -> optional qual -> Agg` shapes run in `pg_volvec`.
- `Sort`, `Limit`, `Hash Join`, and `SubqueryScan` wrappers are live in the current offload path.
- `MergeJoin`-planned shapes can now be intercepted through a temporary hash-join-backed fallback.
- Q22-style right-anti plans can now be intercepted through the current hash-backed anti path.
- Tuple deform JIT works without manually `LOAD 'llvmjit'`; the provider is auto-loaded when needed.
- Expression JIT is wired in and generates fused row loops instead of materializing intermediate vector temporaries.
- `NUMERIC(15,2)` hot paths use scaled `int64`, while aggregation uses widened integer accumulation.
- A first columnar `VecSortState` exists for in-memory final-result sorting.
- Single-column `count(distinct ...)` for the currently validated scalar-key cases is live and verified through Q16.
- Correlated scalar aggregate lookup is no longer limited to `Agg <- SeqScan`; Q2-style lookup over `Agg <- HashJoin` is now in place.

### Fully verified offloaded TPC-H queries

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
- Q14
- Q15
- Q16
- Q18
- Q19
- Q22

### Offloaded with narrower validation so far

- Q2
  - confirmed to offload on `~/data/pg_tpch`
  - a full native diff on the current live dataset is still pending because the native query is slow
- Q17
  - confirmed to offload on `~/data/pg_tpch`
  - exact semantics were checked on a reduced reproducer, while a full native TPCH-side diff is still pending

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

### Join coverage

`pg_volvec` now supports the current validated inner-join wave through a first vectorized `VecHashJoinState`. For queries planned as `MergeJoin`, the current implementation reuses that hash-join execution path to get the query offloaded and correct, while a true vectorized merge-join kernel remains future work. Q22-style anti paths are also still running through a hash-backed fallback rather than a dedicated anti-join kernel.

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

## Next Likely Targets

The next most practical TPC-H target is Q20. On the current local `tpch` instance, the remaining blockers are now much clearer:

- a hash-backed `Nested Loop` / `Semi Join` execution path, or a real vectorized nested-loop family
- multi-key correlated scalar lookup, because the `0.5 * sum(l_quantity)` subquery is correlated on both `ps_partkey` and `ps_suppkey`

Two other caveats matter for the current roadmap:

- Q13 and Q21 are blocked by the live `tpch` schema itself, not just executor coverage. The local `orders` table is missing columns such as `o_comment` and `o_orderstatus`, and the local `customer` table is also trimmed.
- `count(distinct ...)` is no longer an all-or-nothing blocker, but broader distinct coverage beyond the currently validated scalar-key cases is still future work.

## License

PostgreSQL License.

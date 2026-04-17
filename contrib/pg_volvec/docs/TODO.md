# pg_volvec TODO

Status refreshed: `2026-04-17`

## Verified Query Set

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

## Immediate Priorities

### 1. Parallel hash-build correctness and model closure

- [ ] Replace the current Q11-style leader-built shared hash bridge fallback
  with a proper pipeline-specific worker build model where practical.
- [ ] Do not reintroduce the unsafe "pick the target `HashJoin` subtree as the
  worker root" experiment. Nested build chains need upstream bridge attachment,
  not blind subtree rooting.
- [ ] Keep the current safe fallback in place until the worker-side model is
  truly pipeline-specific.
- [ ] Reduce worker-local plan rebuild and init overhead in process-parallel
  mode.

### 2. Coverage closure

- [ ] Q2: preserve the current no-crash behavior and close more of the live
  dataset validation gap where possible.
- [ ] Q17: close the remaining full original-query validation gap if native PG
  runtime makes that practical.
- [ ] Q21: keep it explicitly documented as parked unless a future planner or
  rewrite path makes it a better executor target.

### 3. Performance closure on validated queries

- [ ] Q4 parallel regression: profile first, then decide whether the issue is
  worker setup, hash-build strategy, or plan-shape mismatch.
- [ ] Q12 parallel slowdown: keep cutting string-heavy join/agg/sort overhead
  once the next benchmark sweep confirms the regression shape.
- [ ] Q10/Q14 follow-up: continue reducing wide string payload movement only if
  a fresh benchmark says it is still worth it.
- [ ] Q1/Q6 follow-up: keep optimizing only against fresh flame graphs.

## JIT And Expression Work

- [ ] Add a `dense + no-null` expression kernel.
- [ ] Re-enable int-like comparison JIT only after focused regression coverage
  proves widening and scaled-compare semantics are correct.
- [ ] Re-enable numeric-division JIT only after emitted scale constants are
  validated against the interpreter path.
- [ ] Fuse `sum(expr)` / `avg(expr)` into the aggregate update loop.
- [ ] Improve runtime visibility of expression/deform JIT success and fallback
  in logs and `EXPLAIN`.

## Scan And Deform Work

- [ ] Add stronger fixed-layout / no-null deform specializations.
- [ ] Explore page-level deform fusion where it pays without destabilizing scan
  correctness.
- [ ] Decide how late materialization should interact with the current deform
  pipeline.
- [ ] Keep reducing string-arena traffic where filters only need short-lived
  predicate evaluation.

## Join, Distinct, And Semantics

- [ ] Broaden `HashJoin` beyond the current validated inner + first outer-join
  subset.
- [ ] Decide when `MergeJoin` should keep using the temporary hash-backed
  fallback versus needing a real vectorized merge kernel.
- [ ] Add real semi/anti join support instead of relying on planner rewrites or
  hash-backed fallback.
- [ ] Broaden `COUNT(DISTINCT ...)` beyond the current validated grouped
  single-column scalar-key path.
- [ ] Expand exact fixed-point coverage beyond the current TPC-H-centric
  `NUMERIC(15,2)` path.
- [ ] Broaden string predicate and string sort coverage beyond the current
  validated subset.

## Quality And Tooling

- [ ] Keep the checked-in benchmark snapshots current after meaningful runtime
  changes.
- [ ] Add repeatable regression coverage for JIT-on and JIT-off correctness.
- [ ] Improve fallback behavior reporting when a plan or expression is rejected.
- [ ] Keep docs and runbook synchronized with the real local workflow.

## Current Parallel Notes

- [ ] Q3 currently needs `enable_eager_aggregate = off`.
- [ ] Q4 lowers through the process-worker path.
- [ ] Q11 is correct, but nested hash-build still falls back to a leader-built
  shared hash bridge.
- [ ] Q12 is correct, but worker-local hash-build is still duplicated.
- [ ] Q16 supports grouped `COUNT(DISTINCT int-like)` merge in the validated
  path.
- [ ] Q17 is correct in the current process-worker path on the live dataset,
  but full native closure is still incomplete.

## Already Landed

- [x] Query-driven scan pruning and per-side join pruning
- [x] Deform JIT auto-load for `llvmjit`
- [x] Expression JIT wired into the real execution path
- [x] In-memory vectorized final sort
- [x] Heap `read_stream` integration for the scan path
- [x] Grouped `COUNT(DISTINCT int-like)` partial merge path for Q16
- [x] Safe process-parallel bad-shape guard for small probe / huge build cases

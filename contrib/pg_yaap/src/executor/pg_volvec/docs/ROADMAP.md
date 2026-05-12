# pg_volvec Roadmap

> ⚠️ **STALE — Pre-greenfield (2026-04-17).** Most claims here are invalid post M-FRAME-MIN (`fd9a8aaf326`). For the current state see `contrib/pg_volvec/AGENTS.md` and `docs/PIPELINE_PORT_PLAN.md`.

Status refreshed: `2026-04-17`

This document captures the medium-term direction after the current module
split, process-parallel bring-up, and broad TPC-H coverage wave.

## Current Baseline

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

Current capabilities:

- vectorized `SeqScan`, `Filter`, `Agg`, `Sort`, `Limit`, `HashJoin`
- query-driven scan pruning and per-side join pruning
- deform JIT with automatic `llvmjit` provider load
- expression JIT with fused loops on the currently safe opcode family
- exact numeric hot path for TPC-H-style `NUMERIC(15,2)`
- owned string storage across join/agg/sort/output
- process-based pipeline DAG; this stale doc predates the current block-pool
  SeqScan runtime where workers claim heap blocks through a local read stream
  callback

Current important limits:

- `Wide128` expression JIT is missing
- sort is still single-run in-memory only
- nested hash-build dependency chains are not yet fully worker-shared
- broad semi/anti join execution is missing
- `Materialize` / rescan-heavy coverage is still thin
- Q21 is mostly a planner-quality warning sign, not the default next executor
  project

## Roadmap Principles

1. Keep PostgreSQL planning unchanged.
2. Prefer safe fallback over clever partial-offload accidents.
3. Finish validation and closure on supported shapes before chasing generic
   completeness.
4. Put optimization work behind profiling evidence.
5. Keep memory lifetime and JIT lifetime aligned with PostgreSQL cleanup.

## Phase 1: Stabilize The Current Parallel Runtime

### Goal

Turn the current process-parallel runtime from "broad and correct on the
validated set" into "predictable and explainable".

### Main work

- replace duplicated worker-local hash-build behavior where a shared bridge is
  the right answer
- teach nested hash-build chains to initialize pipeline-specific worker state
  instead of guessing from a whole-root or ad-hoc subtree
- reduce worker setup and local plan rebuild overhead
- improve observability for accepted/rejected parallel shapes
- keep bad-shape guards honest with benchmark evidence

### Why this matters

Current Q11 is the clearest example: correctness is fine, but nested build
dependency chains still need a safer and more complete worker-side model.

## Phase 2: Coverage Closure

### Goal

Close the remaining "narrow validation" cases without reopening previously
stabilized shapes.

### Main work

- Q2: keep the no-crash guarantee and close more of the validation gap where
  practical
- Q17: close the remaining full original-query validation gap if native PG
  runtime allows it
- Q21: keep documented as parked unless a future planner or rewrite path makes
  it a better executor target

### Exit condition

The extension should have an explicit, documented answer for every TPC-H query:

- fully verified
- narrower validation but understood
- intentionally deferred for planner-quality reasons

## Phase 3: Performance Pass On The Validated Set

### Goal

Make the supported set faster before broadening semantics again.

### Highest-value targets

- Q1
- Q6
- Q10
- Q12
- Q14
- Q18

### Likely work items

- dense + no-null expression kernels
- aggregate argument fusion
- stronger fixed-layout / no-null deform specializations
- less string-arena traffic in filter-heavy paths
- less worker setup overhead in process-parallel mode

## Phase 4: Broader Join And Distinct Semantics

### Goal

Broaden semantic coverage only after the current executor surface is stable and
well-profiled.

### Main work

- broader outer-join support
- real semi/anti join execution
- less reliance on hash-backed fallback for `MergeJoin`-planned shapes
- broader `COUNT(DISTINCT ...)` coverage
- stronger subquery and materialization-boundary handling

## Phase 5: Second-Cut Sort And Spill Story

### Goal

Move beyond the current "good enough for final TPCH sorts" implementation.

### Main work

- multi-run sort
- merge phase improvements
- optional spill story when it becomes necessary
- broader string sort coverage

## Cross-Cutting Themes

### Fallback robustness

Rejected shapes should fail with precise reasons instead of mysterious null
returns.

### Memory lifetime discipline

Continue moving shared containers and query-lifetime state toward PostgreSQL
`MemoryContext` ownership.

### Observability

We should always be able to answer:

- why the plan was accepted or rejected
- whether deform JIT was used
- whether expression JIT was used
- where time moved after an optimization

### Benchmark discipline

Every meaningful executor change should come with:

- correctness check against native PostgreSQL
- at least one benchmark on a supported shape
- profiling evidence if the result is slower than expected

## Recommended Next Step

If work resumes immediately after this checkpoint, the highest-value next slice
is:

1. stabilize nested parallel hash-build behavior
2. keep Q2 / Q17 / Q21 status explicit and documented
3. then return to performance work on the already-validated set

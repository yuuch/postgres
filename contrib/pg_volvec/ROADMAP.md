# pg_volvec TPC-H Roadmap

This document describes a realistic path from the current `pg_volvec` prototype to broader TPC-H coverage. It is grounded in the local `~/data/pg_tpch` verification state as of April 3, 2026.

## Current Baseline

### Verified today

- Supported plan shapes:
  - `SeqScan -> optional qual -> Agg`
  - `Limit -> Sort -> Agg`
  - inner `HashJoin` chains
  - `SubqueryScan`
  - `MergeJoin`-planned shapes through a temporary hash-backed fallback
- Verified offloaded queries:
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
  - Q19
- Verified capabilities:
  - vectorized `SeqScan`, `Filter`, grouped `Agg`
  - first-cut vectorized in-memory `Sort`
  - first-cut vectorized `HashJoin`
  - query-driven scan pruning and per-side join pruning
  - deform JIT with automatic `llvmjit` provider load
  - expression JIT with fused row loops
  - fixed-point `NUMERIC(15,2)` hot path
  - owned-string storage across join/agg/sort/output

### Local checkpoints

These numbers are local engineering checkpoints, not broad claims:

- Q6, 3 alternating runs:
  - native PostgreSQL average: `3.72s`
  - `pg_volvec` average: `2.88s`
  - about `1.29x`
- Q1 no-order, 3 alternating runs:
  - native PostgreSQL average: `21.83s`
  - `pg_volvec` average: `4.87s`
  - about `4.48x`
- Q1 full SQL form with `ORDER BY`, 1 local hot-cache run:
  - native PostgreSQL: `21.16s`
  - `pg_volvec`: `5.74s`
  - about `3.69x`
- Q14, alternating local runs after join pruning:
  - native PostgreSQL average: `4.72s`
  - `pg_volvec` average: `3.83s`
  - about `1.23x`

### Main current limitations

- No true outer join support
- No true semi / anti join execution
- No `count(distinct ...)`
- No `Materialize` or broader plan-node coverage around rescan-heavy shapes
- No direct aggregate fusion of `sum(expr)` / `avg(expr)`
- Current `Sort` is still a first-cut single-run in-memory implementation
- `MergeJoin` is still executed through a hash-backed fallback, not a true merge kernel
- Grouped aggregation still has some width limits and rewrite special cases

## Roadmap Principles

1. Unlock real query shapes before chasing generic completeness.
2. Keep fallback to native PostgreSQL safe and boring.
3. Favor specialization of the hot single-table path before broadening plan coverage.
4. Only add features that actually help TPC-H. For example, window functions are not on the critical path because TPC-H does not require them.

## Phase 1: Finish The Current Multi-Query Core

### Goal

Turn the current working wave into a cleaner, better-instrumented base before the next capability jump.

### Tasks

- [ ] Add a `dense + no-null` expression JIT kernel.
- [ ] Fuse aggregate argument evaluation into aggregate update for `sum(expr)` / `avg(expr)`.
- [ ] Improve runtime visibility of expr/deform JIT success and fallback reasons.
- [ ] Add a repeatable correctness and benchmark harness for the verified query set.
- [ ] Keep tightening the scan/deform path where it is still I/O-adjacent but CPU-visible.

### TPC-H impact

- Makes Q1/Q6/Q14/Q12-style paths more stable and easier to extend.
- Reduces the amount of one-off query-specific cleanup before the next wave.

## Phase 2: Q18

### Goal

Unlock Q18 as the next best planner shape after the current validated set.

### Why Q18 is next

With parallel disabled, Q18 already plans into a shape close to the current engine:

- `Limit`
- `GroupAggregate`
- `Sort`
- inner `HashJoin` chain
- `HashAggregate` subquery on `lineitem`

It avoids the bigger new capability jumps needed by outer-join and distinct-heavy queries.

### Tasks

- [ ] Lift grouped aggregation beyond the current 4-key `VecGroupKey` limit.
- [ ] Validate the `HashAggregate` subquery with `sum(l_quantity) > 300`.
- [ ] Make sure the top `Limit -> Sort -> GroupAggregate -> HashJoin` stack works without query-specific hacks.

### TPC-H impact

- Q18 becomes the next concrete query unlocked.

## Phase 3: Outer Join And Distinct Wave

### Goal

Unlock the remaining families blocked mainly by outer joins, distinct, and anti-join logic.

### Candidate queries

- Q13
- Q16
- Q17
- Q20
- Q21
- Q22

### Required features

- outer join support
- semi / anti join support
- stronger subquery support
- `count(distinct ...)`

## Phase 4: Broaden Boolean Logic And Join Semantics

### Goal

Handle queries whose main difficulty is not the join operator alone, but large composite predicates.

### Why this deserves its own phase

Queries like Q19 are easy to underestimate. The main problem is not just the join. It is the large `OR`-of-conjunctions predicate shape.

### Candidate queries

- Q19
- residual hard cases inside Q16 / Q20 / Q21

### Required features

- broader `BoolExpr` support, especially `OR`
- better predicate normalization / lowering
- more complex join filter execution

## Phase 5: Remaining Coverage

### Goal

Close the remaining gaps after Q18, outer joins, distinct, and stronger subquery support are in place.

### Likely remaining needs

- more plan-node coverage around materialization boundaries
- better handling of multi-way join pipelines
- planner/offload heuristics to avoid choosing a bad partial offload

### Candidate queries

- Q2
- Q13
- Q16
- Q22

## Cross-Cutting Priorities

These matter in every phase:

### 1. Fallback robustness

The bridge must reject unsupported subtrees cleanly and predictably. A safe fallback is more important than a clever partial offload that occasionally breaks.

### 2. Memory lifetime discipline

The core containers are already moving toward PostgreSQL `MemoryContext` allocation. Keep that direction and avoid introducing side systems that fight PostgreSQL lifetime rules.

### 3. Observability

We need good answers to:

- why a plan was accepted or rejected
- whether deform JIT was used
- whether expr JIT was used
- where time moved after each optimization

### 4. Benchmark discipline

Every new capability should come with:

- correctness check against native PostgreSQL
- at least one local benchmark on a supported TPCH shape
- a flame graph if the result is slower than expected

## Non-Goals For Now

These may matter later, but they should not distort the near-term roadmap:

- window functions
- generic SQL completeness
- parallel execution support inside `Gather`
- broad type-system coverage beyond what the next TPC-H milestones need

## Recommended Next Step

If work resumes immediately after this document, the highest-value next milestone is Q18:

1. extend grouped aggregation beyond four key columns
2. validate the `sum(l_quantity) > 300` aggregate subquery path
3. keep the existing `Limit -> Sort -> GroupAggregate -> HashJoin` stack composable

That ordering stays close to the next real query unlock instead of drifting into abstract executor work.

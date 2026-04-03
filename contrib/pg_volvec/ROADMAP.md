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
  - current Q22-style right-anti-planned shapes through a hash-backed fallback
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
  - Q16
  - Q18
  - Q19
  - Q22
- Offloaded with narrower validation so far:
  - Q2
  - Q17
- Verified capabilities:
  - vectorized `SeqScan`, `Filter`, grouped `Agg`
  - first-cut vectorized in-memory `Sort`
  - first-cut vectorized `HashJoin`
  - query-driven scan pruning and per-side join pruning
  - deform JIT with automatic `llvmjit` provider load
  - expression JIT with fused row loops
  - fixed-point `NUMERIC(15,2)` hot path
  - owned-string storage across join/agg/sort/output
  - single-column `count(distinct ...)` on the currently validated scalar-key path
  - correlated scalar lookup beyond `Agg <- SeqScan`, including the current Q2-style `Agg <- HashJoin` case

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
- No true nested-loop-based semi / anti join execution
- No broad `count(distinct ...)` coverage beyond the current validated scalar-key cases
- No `Materialize` or broader plan-node coverage around rescan-heavy shapes
- No direct aggregate fusion of `sum(expr)` / `avg(expr)`
- Current `Sort` is still a first-cut single-run in-memory implementation
- `MergeJoin` is still executed through a hash-backed fallback, not a true merge kernel
- Q13 and Q21 are currently blocked by the live `tpch` schema missing columns in `orders` / `customer`

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

## Phase 2: Q20 And Schema Repair

### Goal

Unlock the next practical query family after the current validated set, and remove environment blockers for the remaining outer-join wave.

### Why this is next

Q18 is already done. The next real query gap is Q20, whose remaining blockers are now narrow enough to name precisely:

- a hash-backed `Nested Loop` / `Semi Join` execution path, or a real vectorized nested-loop family
- multi-key correlated scalar lookup because the scalar subquery depends on both `ps_partkey` and `ps_suppkey`

In parallel, Q13 and Q21 cannot be used as honest executor milestones until the local `tpch` schema is repaired.

### Tasks

- [ ] Add a hash-backed `Nested Loop` / `Semi Join` path suitable for Q20, or a real vectorized nested-loop family.
- [ ] Extend correlated scalar lookup to multi-key correlation.
- [ ] Repair the local `orders` / `customer` TPCH schema so Q13 and Q21 are meaningful targets again.

### TPC-H impact

- Q20 becomes the next concrete query unlock.
- Q13 / Q21 stop being blocked by environment drift.

## Phase 3: Outer Join And Distinct Wave

### Goal

Unlock the remaining families blocked mainly by outer joins, distinct, and anti-join logic.

### Candidate queries

- Q13
- Q20
- Q21

### Required features

- outer join support
- semi / anti join support
- stronger subquery support
- broader `count(distinct ...)`

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

- Q2 full native diff closure on the live dataset
- Q17 full native TPCH-side diff closure
- residual hard cases after Q20 / Q13 / Q21

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

If work resumes immediately after this document, the highest-value next milestone is Q20:

1. add a hash-backed `Nested Loop` / `Semi Join` path, or a real vectorized nested-loop family
2. extend correlated scalar lookup to multi-key correlation
3. repair the local TPCH schema so Q13 and Q21 stop being blocked by environment drift

That ordering stays close to the next real query unlock instead of drifting into abstract executor work.

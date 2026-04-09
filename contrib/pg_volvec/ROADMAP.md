# pg_volvec TPC-H Roadmap

This document describes a realistic path from the current `pg_volvec` prototype to broader TPC-H coverage and better performance on the validated set. It is grounded in the local `~/data/pg_tpch` verification state as of April 8, 2026.

## Current Baseline

### Verified today

- Supported plan shapes:
  - `SeqScan -> optional qual -> Agg`
  - `Limit -> Sort -> Agg`
  - inner `HashJoin` chains
  - first-cut hash-backed right/left outer join family
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
  - Q13
  - Q14
  - Q15
  - Q16
  - Q18
  - Q19
  - Q22
- Offloaded with narrower validation so far:
  - Q2
  - Q17
  - Q21
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

- Outer join support is currently limited to the validated Q13-style hash right/left subset
- No true nested-loop-based semi / anti join execution
- No broad `count(distinct ...)` coverage beyond the current validated scalar-key cases
- No `Materialize` or broader plan-node coverage around rescan-heavy shapes
- No direct aggregate fusion of `sum(expr)` / `avg(expr)`
- Current `Sort` is still a first-cut single-run in-memory implementation
- `MergeJoin` is still executed through a hash-backed fallback, not a true merge kernel
- Q21 can now offload and complete in the local prototype, but it is not a near-term executor milestone because the current many-table join plus sublink shape is dominated by PostgreSQL planner quality

## Roadmap Principles

1. Unlock real query shapes before chasing generic completeness.
2. Keep fallback to native PostgreSQL safe and boring.
3. Favor specialization of the hot single-table path before broadening plan coverage.
4. Only add features that actually help TPC-H. For example, window functions are not on the critical path because TPC-H does not require them.
5. When a query is mainly limited by PostgreSQL planning quality, prefer planner-aware offload heuristics or validation notes over deeper executor work.

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

## Phase 2: Performance, Validation, And Planner-Aware Triage

### Goal

Make the validated set faster, easier to benchmark, and easier to reason about before taking on more executor complexity.

### Why this is next

The current prototype already covers most of the practically reachable local TPCH set. The highest-value next work is therefore no longer a single uncovered query. It is:

- improving the performance of the already verified set
- closing the remaining validation gaps for Q2 / Q17
- keeping Q21 documented as an offloaded-but-deprioritized shape whose local pain point is planner quality rather than a missing executor primitive

### Tasks

- [ ] Add a repeatable correctness and benchmark harness for the verified query set.
- [ ] Keep improving Q1 / Q6 / Q10 / Q12 / Q14 performance where flame graphs still show value.
- [ ] Close the remaining full-diff validation gap for Q2 / Q17 where practical.
- [ ] Add planner-aware offload heuristics so obviously bad partial-offload shapes can stay on native PostgreSQL.
- [ ] Re-validate Q13 as join/filter rewrites expand.
- [ ] Keep Q21 documented as a parked query shape unless a future planner or rewrite path makes it a better executor target.

### TPC-H impact

- Makes the validated set more useful as an engineering platform.
- Turns Q2 / Q17 into cleanly closed cases instead of permanent “almost done” entries.
- Avoids over-investing in a Q21 path whose local bottleneck is primarily planning quality.

## Phase 3: Outer Join And Distinct Wave

### Goal

Unlock the remaining families blocked mainly by outer joins, distinct, and anti-join logic.

### Candidate queries

- residual hard cases in richer semi/anti join compositions
- broader distinct-heavy shapes
- Q21 only if a future planner shape or rewrite makes it a better executor target

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
- residual hard cases inside Q16 and later semi/anti join work

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
- residual hard cases after Q13 and the remaining semi/anti wave

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

If work resumes immediately after this document, the highest-value next milestone is performance and validation discipline on the already supported set:

1. build the repeatable benchmark and correctness harness for the verified queries
2. keep optimizing the hottest validated paths, especially Q1 / Q6 / Q10 / Q12 / Q14
3. close the remaining validation gaps for Q2 / Q17
4. treat Q21 as a planner-quality warning sign, not as the default next executor project

That ordering keeps engineering effort tied to measurable wins instead of overfitting the executor to one especially poor local plan.

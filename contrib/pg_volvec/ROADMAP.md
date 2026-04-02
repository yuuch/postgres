# pg_volvec TPC-H Roadmap

This document describes a realistic path from the current `pg_volvec` prototype to broader TPC-H coverage. It is intentionally grounded in the code and benchmarks verified on April 2, 2026, rather than in an idealized end state.

## Current Baseline

### Verified today

- Supported plan shapes:
  - `SeqScan -> optional qual -> Agg`
  - `Sort -> Agg -> SeqScan`
- Verified offloaded queries:
  - Q1 without `ORDER BY`
  - Q1 with `ORDER BY`
  - Q6
- Verified capabilities:
  - vectorized `SeqScan`, `Filter`, `HashAgg`
  - first-cut vectorized in-memory `Sort`
  - query-driven column pruning
  - deform JIT with automatic `llvmjit` provider load
  - expression JIT with fused row loops
  - fixed-point `NUMERIC(15,2)` hot path

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

### Main current limitations

- No `Hash Join`
- No `Semi Join` / `Anti Join`
- No support for broader plan nodes such as `Materialize`, `Limit`, `Gather`
- No direct aggregate fusion of `sum(expr)` / `avg(expr)`
- Current `Sort` is still a first-cut single-run in-memory implementation
- Expression engine support is still narrow outside the current Q1 / Q6 path

## Roadmap Principles

1. Unlock real query shapes before chasing generic completeness.
2. Keep fallback to native PostgreSQL safe and boring.
3. Favor specialization of the hot single-table path before broadening plan coverage.
4. Only add features that actually help TPC-H. For example, window functions are not on the critical path because TPC-H does not require them.

## Phase 1: Finish the Single-Table Fast Path

### Goal

Turn the current Q1 / Q6 path from "works and is fast on the happy path" into a stable base for broader coverage.

### Why this comes first

The single-table path is already paying off. The next best returns still come from making that path simpler, faster, and easier to compose with later operators.

### Tasks

- [ ] Add a `dense + no-null` expression JIT kernel.
- [ ] Fuse aggregate argument evaluation into aggregate update for `sum(expr)` / `avg(expr)`.
- [ ] Improve runtime visibility of expr/deform JIT success and fallback reasons.
- [ ] Add a repeatable correctness and benchmark harness for Q1 / Q6.
- [ ] Keep tightening the scan/deform path where it is still I/O-adjacent but CPU-visible.

### TPC-H impact

- Q1 no-order and Q6 get more robust and faster.
- This phase does not unlock many new queries by itself, but it makes every later phase easier.

## Phase 2: Full Q1 Status

### Goal

Offload the standard TPC-H Q1 shape, including the final `ORDER BY`.

### Real blockers

This milestone is now reached with a first-cut vectorized final sort. The main
follow-up gaps are:

- broadening sort beyond the current top-level in-memory path
- supporting richer string ordering than the current Q1-friendly short-key path
- adding memory-bounded multi-run sort and merge if needed

### Tasks

- [x] Implement vectorized final-result `Sort` for the current Q1 shape.
- [x] Keep sorting inside `pg_volvec` instead of handing pre-aggregated rows back to PostgreSQL.
- [ ] Generalize `Sort` to multi-run / spill-capable execution if larger result sets require it.

### TPC-H impact

- Full Q1 is now unlocked.

## Phase 3: First Join Wave

### Goal

Unlock the simplest and most valuable two-table TPC-H queries.

### Required engine features

- inner `Hash Join`
- broader expression support:
  - `CASE`
  - basic `LIKE 'prefix%'`
  - `IN` over constant lists
- more robust boolean expression support, especially wider predicate trees

### Candidate queries

- Q3
- Q10
- Q12
- Q14

### Query-specific notes

- Q12 and Q14 are not just "join queries". They also require control-flow expressions.
- Q14 needs `LIKE 'PROMO%'`.
- Q12 needs `CASE` and more boolean logic.

### Tasks

- [ ] Implement vectorized inner `Hash Join`.
- [ ] Add `CASE` lowering and execution.
- [ ] Add simple prefix `LIKE`.
- [ ] Add constant-list `IN`.

## Phase 4: Broaden Boolean Logic and Multi-Clause Join Filters

### Goal

Handle queries whose main difficulty is not the join operator alone, but large composite predicates.

### Why this deserves its own phase

Queries like Q19 are easy to underestimate. The main problem is not just the join. It is the large `OR`-of-conjunctions predicate shape.

### Candidate queries

- Q19
- parts of Q5 / Q7 / Q8 / Q9 depending on join progress

### Required features

- broader `BoolExpr` support, especially `OR`
- better predicate normalization / lowering
- more complex join filter execution

### Tasks

- [ ] Add proper `BoolExpr OR` support to the expression engine and JIT.
- [ ] Decide whether to normalize large disjunctions before lowering.
- [ ] Fuse probe-side filters more tightly into the join loop where practical.

## Phase 5: Semi / Anti Join and EXISTS Family

### Goal

Unlock the TPC-H queries that depend on `EXISTS`, `NOT EXISTS`, and related subquery shapes.

### Candidate queries

- Q4
- Q17
- Q20
- Q21

### Required features

- semi join
- anti join
- subquery-to-join style lowering in the supported offload path
- likely `Limit` for some top-level shapes

### Tasks

- [ ] Implement `Semi Join`.
- [ ] Implement `Anti Join`.
- [ ] Add support for more subquery-driven boolean patterns.
- [ ] Add `Limit` where it is structurally required.

## Phase 6: Remaining TPC-H Coverage

### Goal

Close the remaining gaps after inner joins, boolean logic, and semi/anti joins are in place.

### Likely remaining needs

- outer joins
- more plan-node coverage around materialization boundaries
- better handling of multi-way join pipelines
- planner/offload heuristics to avoid choosing a bad partial offload

### Candidate queries

- Q2
- Q5
- Q7
- Q8
- Q9
- Q11
- Q13
- Q15
- Q16
- Q18
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

If work resumes immediately after this document, the highest-value next milestone is:

1. inner `Hash Join`
2. `CASE` / prefix `LIKE`
3. broader `BoolExpr OR` support

That ordering keeps the roadmap close to actual TPC-H unlocks instead of drifting into abstract executor work.

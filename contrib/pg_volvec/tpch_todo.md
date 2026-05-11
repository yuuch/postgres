# TPC-H Coverage Todo (Active Runtime)

> Goal: track the **real** TPC-H progress of the current `pg_volvec` pipeline runtime, not the deleted legacy prototype, and propose the next implementation order **without breaking the current architecture** and **without hard-coding any single query**.

## Guardrails

- Keep the current `PhysicalOperator + MetaPipeline + descriptor-published DSA payload` architecture.
- Do **not** reintroduce the deleted legacy executor / morsel runtime.
- Do **not** add query-specific hacks.
- Extend reusable operator families, lowering rules, layouts, and expression support so more queries lower naturally.
- Treat `Q2`, `Q17`, `Q20`, `Q21` as **parked for now** because the current PostgreSQL plans are poor; revisit only after optimizer work changes the playing field.

## Source of truth

When documents disagree, use this order:

1. `contrib/pg_volvec/AGENTS.md`
2. `contrib/pg_volvec/q10_milestone.md`
3. live benchmark scripts/results under `contrib/pg_volvec/scripts/` and `contrib/pg_volvec/benchmarks/`
4. current translator/runtime code under `src/engine/parallel/pipeline/`
5. older phase notes such as `README.md` / `translator.hpp` comments only as lagging context

Important discrepancy today:

- `README.md` still says only Q1/Q6 are in scope and marks Q10+ as out of scope.
- `translator.hpp` still says the join slice is not recursive yet.
- But `AGENTS.md`, `q10_milestone.md`, the live translator/runtime code, and current benchmarks show the active runtime has already moved forward to a validated Q10 path and a benchmarked Q14 path.

So this file follows the **live implementation/evidence**, not the older scope note.

## Current capability snapshot

The current active runtime already has these reusable capability families:

- `SeqScan` with typed decode and filter/projection lowering
- `HashAggregate` / `PerfectHashAggregate`
- inner `HashJoin`
- `Sort` (single-thread, in-memory)
- `Projection`
- generic output materialization through `OutputSink`
- string-ref row-store support for join/group/order/output

What is still notably missing or still narrow:

- semi/anti join families (`EXISTS`, `NOT EXISTS`, `IN`, `NOT IN` lowered as join forms)
- outer join family
- explicit reusable `SubqueryScan` / derived-table lowering family
- broader expression coverage (`extract(year ...)`, richer `CASE`, broader string predicates)
- `MIN/MAX` and `COUNT(DISTINCT ...)`
- stronger join probe batching / SIMD / materialization efficiency
- external/spill-ready aggregation/sort behavior

## Status legend

- **Validated**: correctness or milestone evidence exists in current docs/logs.
- **Benchmarked**: current benchmark script/log evidence exists.
- **Runnable but not yet fully validated**: evidence suggests it runs, but correctness is not yet documented as a stable target.
- **Unverified**: query SQL exists, but there is no fresh proof it currently lowers/runs correctly.
- **Parked**: intentionally deferred for now.

## Query-by-query status

| Query | Status | Evidence / reasoning | Main gap if not done |
|---|---|---|---|
| Q1 | **Validated + Benchmarked** | `AGENTS.md` and `README.md` document end-to-end correctness/perf; benchmarked in `bench_tpch_pg_vs_volvec.sh` and the refreshed `tpch_pg_vs_volvec_20260511_174551.tsv` | Keep stable; continue perf work only |
| Q2 | **Parked** | SQL exists, but shape uses scalar subquery with `min(ps_supplycost)` and current plan quality is poor | Better optimizer + reusable subquery/semi-join style support |
| Q3 | **Unverified but near-term** | SQL is a pure inner-join + group + order shape in this repo (no `LIMIT` in current file) | More robust multi-join admission/coverage; validate grouped join chain at scale |
| Q4 | **Unverified, later family** | Uses `EXISTS` subquery over `lineitem` | Reusable semi-join / exists lowering |
| Q5 | **Unverified but near-term** | Pure multi-inner-join + group + order, structurally close to Q10 family | Wider join-chain admission, more generic join-fed agg validation |
| Q6 | **Validated + Benchmarked** | `README.md` expected result + `AGENTS.md` scope + benchmark TSV | Keep stable |
| Q7 | **Unverified, medium-term** | Derived-table / subquery form plus `extract(year from l_shipdate)` | Reusable subquery/derived-table lowering + date extract expression support |
| Q8 | **Unverified, medium-term** | Derived-table + `extract(year ...)` + `CASE` + many joins | Same as Q7, plus broader projection expr coverage |
| Q9 | **Unverified, medium-term** | Derived-table + `extract(year ...)` + arithmetic over join outputs | Same as Q7, plus broader numeric expression coverage |
| Q10 | **Validated + Benchmarked** | `AGENTS.md` says Q10 runnable/validated; `q10_milestone.md` records grouped row count `381105` matching native; benchmarked in current script/TSV | Keep stable; use as join-heavy regression target |
| Q11 | **Unverified, later family** | Uses grouped `HAVING` with scalar subquery threshold | Reusable subquery + having/subplan support |
| Q12 | **Unverified but near/medium-term** | Inner join + grouped aggregates + `CASE` over string equality/inequality; explicitly cited in older planning notes and future unlocks | Generic `CASE` lowering over join-fed string columns |
| Q13 | **Unverified, later family** | Uses `LEFT OUTER JOIN` and grouped derived table | Outer join family + derived-table lowering |
| Q14 | **Benchmarked / Runnable, not yet promoted to validated target** | Current benchmark script runs it; refreshed benchmark TSV shows `ok` for PG-vs-volvec | Need explicit correctness validation and more hash-join probe speed |
| Q15 | **Unverified, later family** | Uses `CREATE VIEW`, then scalar subquery with `max(total_revenue)` | View/subquery normalization and reusable scalar-subquery support |
| Q16 | **Unverified, later family** | Uses `count(distinct ps_suppkey)` and `NOT IN` subquery | Distinct aggregate + anti-join / `NOT IN` support |
| Q17 | **Parked** | Correlated scalar subquery and current PG plan quality is poor | Better optimizer + reusable correlated-subquery support |
| Q18 | **Unverified, later family** | Uses `IN (subquery with group by having)` then join+group+order | Semi-join / grouped subquery lowering |
| Q19 | **Unverified but near-term** | Pure join + OR-heavy filter + single `SUM`; explicitly named in `q10_milestone.md` as a join-family unlock | Broader OR-heavy filter lowering + join probe speed |
| Q20 | **Parked** | Nested `IN` + scalar aggregate subquery and current PG plan quality is poor | Better optimizer + reusable nested subquery / semi-join support |
| Q21 | **Parked** | `EXISTS` + `NOT EXISTS` + current PG plan quality is poor | Better optimizer + semi/anti join family |
| Q22 | **Unverified, later family** | Derived table + scalar subquery + `NOT EXISTS` | Derived-table + scalar subquery + anti-join support |

## Evidence-backed queries today

### Definitely validated/benchmarked

- **Q1**
- **Q6**
- **Q10**

### Definitely benchmarked in the current script

- **Q14**

Current benchmark driver:

- `contrib/pg_volvec/scripts/bench_tpch_pg_vs_volvec.sh`

Current benchmark set:

- `contrib/pg_volvec/benchmarks/tpch_pg_vs_volvec_20260511_174551.tsv`

That TSV currently records:

- Q1: ok
- Q6: ok
- Q10: ok
- Q14: ok

Important nuance:

- Q14 has **run/benchmark evidence**, but it does **not** yet have the same level of correctness/milestone documentation as Q1/Q6/Q10.
- So it should be treated as **runnable + benchmarked**, not yet as a fully validated coverage claim.

## Recommended implementation order

This order is based on **reusable capability expansion**, not on query-specific patching.

### Tier 0 — keep the proven path stable

Regression anchors:

1. Q1
2. Q6
3. Q10
4. Q14

Reason:

- Q1/Q6 cover scan/filter/agg fundamentals.
- Q10 covers the generic join-heavy path.
- Q14 is the simplest measurable join-heavy benchmark for hash-join probe work.

### Tier 1 — next best ROI, same operator family

Recommended order:

1. **Q3**
2. **Q5**
3. **Q19**

Why:

- They stay in the current family: inner joins + filter/projection + aggregate/order.
- They add coverage without forcing outer joins or semi/anti joins.
- Q19 is especially useful as a join/filter stress case after Q14.

Reusable work likely needed here:

- stronger multi-join admission and recursive lowering robustness
- broader expression/filter coverage for OR-heavy predicates
- continued hash-join probe/materialization optimization

### Tier 2 — same broad family, but expression coverage gets harder

Recommended order:

1. **Q12**
2. **Q7**
3. **Q9**
4. **Q8**

Why:

- Q12 is still an inner-join/grouped aggregation query, but needs more generic `CASE` lowering.
- Q7/Q8/Q9 add derived-table style structure and `extract(year ...)`, which should be solved as reusable expression/subquery capabilities.

Reusable work likely needed here:

- generic `CASE` lowering over join-fed columns
- `extract(year from date)` / date-part expression support
- reusable derived-table / `SubqueryScan` lowering rather than per-query special cases

### Tier 3 — later operator-family expansion

Recommended order:

1. **Q11**
2. **Q18**
3. **Q22**
4. **Q15**
5. **Q16**
6. **Q4**
7. **Q13**

Why:

- These require reusable support for one or more of:
  - scalar subqueries
  - grouped subqueries
  - semi/anti joins
  - outer joins
  - `COUNT(DISTINCT ...)`
  - view/derived-table normalization

They should come **after** the inner-join/group-agg family is solid, because they introduce new operator families rather than just broader coverage of the current one.

### Tier 4 — explicitly parked for now

- **Q2**
- **Q17**
- **Q20**
- **Q21**

Reason:

- Current PostgreSQL plans are poor enough that forcing them now is not a good use of time.
- These are also exactly the queries that would want better optimizer cooperation and/or more advanced reusable subquery/semi/anti-join support.

## Recommended work items by capability, not by query

### A. Keep current family healthy

- Maintain Q1/Q6/Q10/Q14 regression coverage.
- Keep benchmark script green.
- Continue fixing join probe / materialization hot spots without changing the pipeline model.

### B. Finish the inner-join/group/order family

Target unlocks: Q3, Q5, Q19, then Q12.

Needed reusable work:

- broader recursive multi-join lowering
- more generic join-fed Agg/Sort admission
- broader boolean filter lowering (`OR`, larger disjunction trees, richer string predicates)
- stronger expression coverage for grouped output/projection

### C. Add derived-table / subquery building blocks

Target unlocks: Q7, Q8, Q9, then later Q11/Q18/Q22/Q15.

Needed reusable work:

- explicit `SubqueryScan` / derived-table lowering path
- generic projection-expression widening (`extract`, broader arithmetic, richer `CASE`)
- stable schema/column lineage across deeper nested plans

### D. Add new join families only after the above is stable

Target unlocks: Q4, Q13, Q16, and eventually the parked set.

Needed reusable work:

- semi join
- anti join
- outer join
- distinct aggregate support where required

## Suggested next concrete sequence

If the goal is to maximize progress without breaking architecture, the next sequence should be:

1. **Stabilize and keep benchmarking Q1/Q6/Q10/Q14**
2. **Push the reusable inner-join/group/order family to cover Q3**
3. **Then extend the same family to Q5**
4. **Then cover Q19 as a join/filter stress query**
5. **Then add generic `CASE` support needed for Q12**
6. **Only after that, move into derived-table / `extract(year ...)` queries (Q7/Q9/Q8)**
7. **Leave Q2/Q17/Q20/Q21 parked until optimizer work changes the cost/plan situation**

## Short summary

- Real, evidence-backed current coverage is **Q1 / Q6 / Q10**, plus **Q14 benchmarked**.
- The best next non-hacky expansion is **Q3 → Q5 → Q19 → Q12**.
- After that, the next reusable family is **derived-table / `extract(year ...)` support** for **Q7 / Q9 / Q8**.
- Queries that fundamentally need new operator families (`EXISTS`, `NOT EXISTS`, outer join, distinct aggregate, scalar/grouped subqueries) should come later.
- `Q2 / Q17 / Q20 / Q21` are intentionally **parked** for now.

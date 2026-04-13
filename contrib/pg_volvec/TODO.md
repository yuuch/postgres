# pg_volvec TODO

## Current State

- Direct mapping from PostgreSQL `Plan` nodes to vectorized operators is now working for a broader inner-query subset, not just the early single-table path.
- Functional operators/wrappers in the current offload path:
  - `SeqScan`
  - `Filter`
  - `HashAgg` / grouped aggregation
  - in-memory final `Sort`
  - constant-count `Limit`
  - `HashJoin`
  - current hash-backed right/left outer join family
  - `SubqueryScan`
  - `MergeJoin`-planned shapes via a temporary hash-join-backed fallback
  - current Q22-style right-anti-planned shapes via a hash-backed fallback
- Column pruning is implemented for scans and for per-side join materialization.
- LLVM JIT deform is live, auto-loads the provider when needed, and now supports owned string storage too.
- LLVM expression JIT is live and replaces the interpreter on supported programs.
- Exact numeric values are width-aware: precision `<= 18` uses scaled `int64`; precision `> 18` uses the `Wide128` path. `Wide128` expression programs are deliberately not sent through expression JIT yet.
- Chunk-owned string storage is in place for correctness across join/agg/sort/output paths.
- Fixed-point `NUMERIC(15,2)` hot paths use scaled `int64`, while aggregation uses widened accumulation.
- Aggregation grouping is typed for integer/date/string keys instead of assuming string-only group keys.
- Single-column `count(distinct ...)` on the currently validated scalar-key path is live and was exercised by Q16.
- Correlated scalar lookup now works not only for `Agg <- SeqScan`, but also for the current Q2-style `Agg <- HashJoin` path.
- Fully verified offloaded TPC-H queries on `~/data/pg_tpch`:
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
- Offloaded with narrower validation so far:
  - Q2
  - Q17
  - Q21

## Near-Term Roadmap

### Coverage-First SQL Target

- [ ] Coverage-first policy: finish TPC-H SQL coverage/validation closure before starting the next query-specific performance pass.
- [ ] Next coverage target: Q2 no-crash coverage on the live dataset. PostgreSQL's local plan is poor around subquery flattening, so the short-term bar is that Q2 can run with `pg_volvec` enabled until timeout/cancel without crashing the backend; full timing/diff validation can wait.
- [x] Q2 no-crash checkpoint: on 2026-04-12, process-parallel settings attempted plan initialization, hit `hash join filter expression compilation failed`, safely fell back to PostgreSQL executor, then `statement_timeout=60s` canceled cleanly; after the Q17 numeric fix, the same 60s no-crash smoke still canceled cleanly with `/tmp:5432` accepting connections afterward.
- [x] Q17 live-dataset correctness checkpoint: on 2026-04-12, process-parallel lowering now carries the worker context into the correlated lookup aggregate instead of trying to initialize the native correlated `SubPlan` in the worker estate. Four-worker `pg_volvec` returned `3295493.51285714`, matching the native rewrite reference `3295493.512857142857` within float8 display precision.
- [x] Q17 numeric fix note: correlated `0.2 * avg(l_quantity)` now reads `NumericAvgPair` values as scaled average values inside expression evaluation, and numeric division (`INT64_DIV_FLOAT8`) stays on the interpreter path until the LLVM expression JIT scale constants are revalidated.
- [x] Process-parallel benchmark checkpoint: on 2026-04-12, the skip-Q2/Q21 median-of-3 sweep completed without `pg_volvec` crashes. Q17 was native PG `180s timeout`, `pg_duckdb` `15.364s`, and `pg_volvec` `14.601s`; across the 18 direct all-OK comparisons, `pg_volvec` was fastest on 11 with geometric mean speedup `1.55x` vs native PG.
- [x] Process-parallel bad-shape guard: on 2026-04-13, `HashProbeSource` process-parallel execution now skips when the largest hash-build dependency is more than `4x` the selected probe source. This avoids Q14/Q10 shapes where workers scan a small source while redundantly building a much larger local hash-join subtree. Spot checks: Q14 falls back to leader-only at about `3.01s-3.11s`; Q10 falls back at about `4.65s-4.82s`; Q17 still launches `4/4` workers and returns `3295493.51285714`.
- [ ] Final coverage decision: Q21 should get either a minimal correctness closure or an explicit documented exclusion. Keep it last because the local shape is dominated by PostgreSQL planner quality on many-table joins plus sublinks, not an obvious missing executor primitive.
- [ ] After Q21 coverage is settled, rerun the skip-Q2/Q21 benchmark snapshot and refresh the README numbers. Q10/Q14 have a post-sweep guard fix; Q12 did not reproduce as a bad process-parallel choice in the 2026-04-13 hot-cache spot check (`parallel=off` about `3.996s`, `parallel=on` about `3.834s`).

### 1. Stabilize The Current Query Wave

- [ ] Re-check Q2 / Q17 / Q20 after each capability bump to keep the correlated-subquery path honest.
- [ ] Re-check Q13 after future join/filter rewrites so the new outer-join path stays honest.
- [ ] Decide how much more Q21-specific work is justified once planner quality, not executor coverage, is the dominant issue on the local many-join-plus-sublink plan.

### 2. Join And Subquery Coverage

- [ ] Broaden `HashJoin` beyond the current validated inner + first outer-join subset.
- [ ] Support richer join filters on top of hash keys.
- [ ] Decide when `MergeJoin` should keep using the temporary hash fallback versus needing a real vectorized merge kernel.
- [ ] Add real semi/anti join support instead of depending on planner rewrites or hash-backed fallbacks.
- [ ] Broaden outer-join-planned shapes beyond the current Q13-style right/left hash-join subset.
- [ ] Prefer planner-aware offload heuristics over deeper executor work when a query is mainly hurt by a bad PostgreSQL join or sublink plan.

### 3. Expression And Aggregation Fusion

- [ ] Re-enable expression JIT for int-like comparisons only after adding focused regression coverage for `INT2/INT4/INT8` widening and scaled numeric compare. Until then, `VecExprProgram::try_compile_jit()` keeps these predicates on the interpreter path for correctness.
- [ ] Re-enable expression JIT for numeric division only after adding coverage that proves adjusted aggregate output scales are reflected in emitted LLVM constants; Q17 currently depends on the interpreter path for `sum(numeric) / 7.0` correctness.
- [ ] Add a `dense + no-null` specialized expression kernel.
- [ ] Fuse `sum(expr)` / `avg(expr)` into the aggregate update loop to avoid writing final expression result buffers.
- [ ] Add better observability for expression JIT success / fallback in logs and `EXPLAIN`.
- [ ] Reduce regroup/project rewrite special cases around grouped aggregates.

### 4. Scan Path Improvements

- [ ] Replace the old scan-only parallel idea with a real pipeline DAG plus morsel-driven scheduler.
  - Parallel work should be scheduled as `pipeline x morsel` tasks with explicit pipeline-breaker dependencies.
  - The first target should be `SeqScan/Filter/Project -> PartialAgg`, then grouped agg, then `HashJoin` as `HashBuildSource -> HashBuildFinalize -> HashProbe`, then sort/merge.
- [ ] Parallel coverage note: Q3 currently needs `enable_eager_aggregate=off` because PostgreSQL's eager aggregate plan introduces non-simple partial/final aggregate nodes that `pg_volvec` intentionally rejects.
- [ ] Parallel coverage note: Q4 now lowers through the process-worker path by keeping the bridge-produced `HashAggregate(lineitem)` on the build side and using the morsel-driven aggregate source instead of rejecting bridge-produced HashJoin input.
- [ ] Parallel coverage note: Q11 now runs through the process-worker path with worker-side partial agg preserving HAVING `Aggref` slots but deferring the InitPlan-dependent HAVING filter to the leader-side merged plan.
- [ ] Parallel coverage note: Q12 process-worker execution is correct with worker-side JIT disabled; with the current duplicate local hash-build model, `parallel_leader_participation=off` lets workers actually own the source scan instead of the leader consuming it first.
- [ ] Parallel coverage note: Q16 now supports the process-worker path for grouped `COUNT(DISTINCT int-like)` by exporting per-group distinct values through the file-backed partial state and unioning them during leader merge.
- [ ] Parallel coverage note: Q18 now returns the same 100 rows as native PG after keeping int-like comparison predicates off expression JIT and fixing ungrouped aggregate empty-input semantics; latest fair run was `pg_volvec` `12.69s` vs native PG `26.51s` with PG parallel disabled.
- [ ] Parallel coverage note: Q9 now lowers into a multi-pipeline process-worker DAG with hash-build/finalize/probe plus aggregate finalize stages; latest sanity run produced 175 rows matching native PG, `pg_volvec` `12.97s` vs native PG `14.56s` with PG parallel disabled.
- [ ] Add stronger `no-null` / fixed-layout deform specializations.
- [ ] Explore page-level deform fusion instead of tuple-at-a-time JIT calls.
- [ ] Decide how late materialization should interact with the current deform pipeline.
- [ ] Keep cutting string-arena traffic where filters only need short-lived predicate evaluation.

### 5. Type And Predicate Coverage

- [ ] Expand exact fixed-point coverage beyond the current TPC-H-centric `NUMERIC(15,2)` path.
- [ ] Add more scalar type support in the expression engine.
- [ ] Generalize string predicate support beyond the current equality / inequality / prefix / contains / constant-array path.
- [ ] Broaden `ScalarArrayOpExpr` coverage beyond the current constant-array subset.
- [ ] Extend boolean and pattern support needed by the remaining TPC-H queries.
- [ ] Generalize string sort coverage beyond the current in-memory owned-string path.
- [ ] Broaden `count(distinct ...)` beyond the current validated single-column scalar-key cases.

### 6. Quality

- [ ] Build a repeatable local benchmark harness for the verified TPC-H set.
- [ ] Add regression coverage for JIT-on and JIT-off correctness.
- [ ] Improve fallback behavior reporting when a plan or expression is rejected.
- [ ] Record Q21 as either minimally covered or explicitly excluded for planner-quality reasons after Q2/Q17 coverage closure.

### 7. Deferred Query-Specific Optimization

- [ ] Q14 follow-up: consider pushing `p_type LIKE 'PROMO%'` into a build-side flag so the join payload does not need to carry a string ref.
- [ ] Q14 follow-up: keep investigating scan/read-path cost now that hash-build materialization is no longer the dominant hotspot.
- [ ] Q10 follow-up after coverage closure: sample first, then reduce wide customer string payload movement and aggregate/sort materialization. Repeated worker setup for the small-customer-probe process-parallel shape is now guarded off.
- [ ] Q12 follow-up: continue reducing string-heavy join/agg/sort overhead only if a fresh full benchmark shows the post-string-deform-JIT small lead has regressed.
- [x] Scan I/O checkpoint: `VecSeqScanState` now reuses heap `read_stream` for asynchronous prefetch instead of hand-rolled `ReadBufferExtended()` stepping.
- [x] Q14 optimization checkpoint: per-side join pruning plus compact inner payload storage reduced deform targets from `16/9` to `4/2`.
- [x] Q14 optimization checkpoint: local alternating benchmark moved from roughly `4.72s` native vs `5.84s-6.10s` `pg_volvec` to about `4.72s` native vs `3.83s` `pg_volvec`.

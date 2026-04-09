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

- [ ] Add a `dense + no-null` specialized expression kernel.
- [ ] Fuse `sum(expr)` / `avg(expr)` into the aggregate update loop to avoid writing final expression result buffers.
- [ ] Add better observability for expression JIT success / fallback in logs and `EXPLAIN`.
- [ ] Reduce regroup/project rewrite special cases around grouped aggregates.

### 4. Scan Path Improvements

- [ ] **Parallel Deform Workers** remain deferred for now.
  - Fresh flame graphs show scan I/O, expression evaluation, and string-heavy join paths dominating far ahead of deform itself.
  - The current `DataChunk` / `MemoryContext` / owned-string model also does not map cleanly to the older shared-`DataChunk` sketch in `parallel_deformer.md`.
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
- [ ] Record Q21 as an offloaded-but-deprioritized shape so future work does not treat it as the default next executor milestone.

### 7. Deferred Query-Specific Optimization

- [ ] Q14 follow-up: consider pushing `p_type LIKE 'PROMO%'` into a build-side flag so the join payload does not need to carry a string ref.
- [ ] Q14 follow-up: keep investigating scan/read-path cost now that hash-build materialization is no longer the dominant hotspot.
- [ ] Q10/Q12 follow-up: reduce string-heavy join/agg/sort overhead now that correctness is in place.
- [x] Scan I/O checkpoint: `VecSeqScanState` now reuses heap `read_stream` for asynchronous prefetch instead of hand-rolled `ReadBufferExtended()` stepping.
- [x] Q14 optimization checkpoint: per-side join pruning plus compact inner payload storage reduced deform targets from `16/9` to `4/2`.
- [x] Q14 optimization checkpoint: local alternating benchmark moved from roughly `4.72s` native vs `5.84s-6.10s` `pg_volvec` to about `4.72s` native vs `3.83s` `pg_volvec`.

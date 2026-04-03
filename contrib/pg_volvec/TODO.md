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
  - Q14
  - Q15
  - Q16
  - Q18
  - Q19
  - Q22
- Offloaded with narrower validation so far:
  - Q2
  - Q17

## Near-Term Roadmap

### 1. Finish The Remaining Query Wave

- [ ] Q20: add a hash-backed `Nested Loop` / `Semi Join` execution path, or a true vectorized nested-loop family.
- [ ] Q20: support multi-key correlated scalar lookup, because the current subquery is correlated on both `ps_partkey` and `ps_suppkey`.
- [ ] Re-check Q2 / Q17 / Q20 after each capability bump to keep the correlated-subquery path honest.
- [ ] Restore the missing local `orders` / `customer` TPCH columns needed to make Q13 and Q21 meaningful targets again.

### 2. Join And Subquery Coverage

- [ ] Broaden `HashJoin` beyond the current validated inner-join subset.
- [ ] Support richer join filters on top of hash keys.
- [ ] Decide when `MergeJoin` should keep using the temporary hash fallback versus needing a real vectorized merge kernel.
- [ ] Add `Materialize` handling where planner output requires it.
- [ ] Add real semi/anti join support instead of depending on planner rewrites or hash-backed fallbacks.
- [ ] Support outer-join-planned shapes, starting from the Q13-style right/left outer join family once the local schema is repaired.

### 3. Expression And Aggregation Fusion

- [ ] Add a `dense + no-null` specialized expression kernel.
- [ ] Fuse `sum(expr)` / `avg(expr)` into the aggregate update loop to avoid writing final expression result buffers.
- [ ] Add better observability for expression JIT success / fallback in logs and `EXPLAIN`.
- [ ] Reduce regroup/project rewrite special cases around grouped aggregates.

### 4. Scan Path Improvements

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

### 7. Deferred Query-Specific Optimization

- [ ] Q14 follow-up: consider pushing `p_type LIKE 'PROMO%'` into a build-side flag so the join payload does not need to carry a string ref.
- [ ] Q14 follow-up: keep investigating scan/read-path cost now that hash-build materialization is no longer the dominant hotspot.
- [ ] Q10/Q12 follow-up: reduce string-heavy join/agg/sort overhead now that correctness is in place.
- [x] Q14 optimization checkpoint: per-side join pruning plus compact inner payload storage reduced deform targets from `16/9` to `4/2`.
- [x] Q14 optimization checkpoint: local alternating benchmark moved from roughly `4.72s` native vs `5.84s-6.10s` `pg_volvec` to about `4.72s` native vs `3.83s` `pg_volvec`.

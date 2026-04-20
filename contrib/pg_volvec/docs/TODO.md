# pg_volvec TODO

Status refreshed: `2026-04-18`

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

### 0. QueryScheduler Coverage Rollout

Target: every benchmarkable TPC-H query should run correctly through the
QueryScheduler parallel path. Native PostgreSQL or pg_volvec serial fallback is
not an acceptable end state for this workstream; fallback is only temporary
evidence that a specific QueryScheduler blocker still exists. Update this
section after each fix lands.

- [x] Batch A: add/keep correctness evidence for traced QueryScheduler paths
  and remove the Q4/Q18 QueryScheduler false positives. These queries no longer
  return empty or wrong QueryScheduler results. This is not a completion state:
  Q4/Q18 must still be moved back onto QueryScheduler by implementing
  aggregate-bridge hash-build as a real parallel stage.
- [x] Batch B: fix Q17 QueryScheduler worker block-range initialization
  (`pg_volvec query scheduler worker could not configure aggregate input block
  range`). Fixed by forwarding morsel block-range configuration through lookup
  wrapper plan states; Q17 now reaches `path=query_scheduler` and matches the
  pg_volvec serial result `3295493.51285714`.
- [ ] Batch C: deferred. Q15 uses `CREATE VIEW revenue0 ...; SELECT ...;
  DROP VIEW revenue0;`, so skip it while the current QueryScheduler coverage
  push focuses on plain SELECT statements. Revisit after SELECT-only coverage
  is closed.
- [x] Batch D: support root SortMerge-shaped QueryScheduler execution for
  plain SELECTs. Root SortMerge is allowed so final ORDER BY runs in the leader
  over merged QueryScheduler partials. Q3/Q5/Q10/Q16 all reach
  `path=query_scheduler`; exact pg_volvec-serial diffs pass. Q11 remains a
  separate nested hash-build bridge task.
- [x] Batch E: make no-scheduler shapes lower into scheduler DAGs, in this
  order: Q22, Q13, Q20. This is the current priority before any performance
  work or remaining fallback cleanup.
- [x] Q22 Batch E result, 2026-04-19:
  `contrib/pg_volvec/benchmarks/tpch_supported_twice_20260419_152235.tsv`.
  Q22 now reaches `path=query_scheduler` with 7 rows and exact pg_volvec-serial
  diff passes. Fixes included parallel Hash Anti Join lowering, build-key
  deduplication for anti membership hash tables, metadata-only non-executed
  hash-join sides, anti join parallel side binding, and PARAM_EXEC InitPlan
  value propagation to workers.
- [x] Q13 Batch E result, 2026-04-20:
  `contrib/pg_volvec/benchmarks/tpch_supported_twice_20260420_092255.tsv`.
  Q13 now reaches `path=query_scheduler` with 46 rows and exact pg_volvec-serial
  diff passes. Parallel lowering now admits Hash Right Join.
- [x] Q20 Batch E result, 2026-04-20:
  `contrib/pg_volvec/benchmarks/tpch_supported_twice_20260420_095511.tsv`.
  Q20 now reaches `path=query_scheduler` with 1804 rows and exact
  pg_volvec-serial diff passes. The initial row/sort-run bridge is deliberately
  minimal and slow: workers export ordinary row chunks from `HashProbeSource`
  into sort-run files, and the leader feeds them into root `VecSortState` for
  final sorting/output.
- [ ] Q20 follow-up: replace the minimal row/sort bridge with the intended
  vectorized sort framework. Do not mix `SortRun` into the `HashProbeSource`
  pipeline. Sort remains a separate blocking operator in the QueryScheduler
  DAG: `HashProbeSource -> row/run bridge -> SortRun/SortMerge`. The next
  version should sort row-id/permutation runs locally and use an efficient
  merge instead of leader-side full re-sort over all exported chunks.
- [ ] Batch F: reintroduce Q2/Q21 after the supported/narrow-validation query
  set has reliable QueryScheduler behavior.
- [ ] Batch G: after correctness coverage, improve real parallelism with the
  query-level worker pool, dependency-ready DAG scheduling, DSM/DSA bridge
  storage where practical, and immediate successor release after build/finalize
  dependencies complete.
- [ ] Follow-up from Batch A: implement `Aggregate -> HashBuildSource` bridge
  execution as a parallelizable QueryScheduler stage. A single BridgeFinalize
  task can materialize too many build keys in one worker; Q4 hit a 1GB
  allocation while trying to hash 13.7M aggregate keys this way.
- [x] Batch D result, 2026-04-19:
  `contrib/pg_volvec/benchmarks/tpch_supported_twice_20260419_143956.tsv`.
  Q3/Q5/Q10/Q16 completed through QueryScheduler with 4 workers. Exact
  pg_volvec-serial diffs passed for all four. The fix also preserves BPCHAR
  padding in parallel aggregate partial group-key output.

### 1. Parallel hash-build correctness and model closure

- [x] Wire the DSM query-scheduler skeleton into the active experimental
  runtime path far enough that workers claim source tasks, execute
  pipeline-local morsels, export aggregate partials, and let the leader merge
  those partials into the output `AggState`.
- [x] Complete the first file-backed HashJoin bridge handoff inside that
  scheduler: build-source
  tasks must publish worker-local build fragments, hash-build finalize tasks
  must combine them into a read-only shared bridge, and dependent probe tasks
  must attach that bridge instead of rebuilding locally.
- [ ] Move scheduler HashJoin partial fragments / bridge packs from
  `SharedFileSet` to DSM/DSA-backed storage for medium-sized artifacts, keeping
  file fallback only for payloads that are too large for practical shared
  memory.
- [x] Prefer DSM inline aggregate partial slots for small grouped aggregates;
  `bpchar` group keys are trimmed before the inline 8-byte string-key cutoff so
  Q7/Q12 no longer force aggregate partial files just because their SQL type is
  fixed-width `char(n)`.
- [ ] Turn build-dependency waves into a real shared worker-pool scheduler so
  independent build pipelines in the same wave execute concurrently instead of
  sequentially launching per-pipeline worker groups.
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
- [ ] Q4 reached the experimental QueryScheduler path in the pre-Batch-A
  2026-04-18 execution-path trace sweep, but is now intentionally gated off
  that path until aggregate-bridge HashBuildSource is parallelized correctly.
- [ ] Q11 is correct, but nested hash-build still falls back to a leader-built
  shared hash bridge.
- [ ] 2026-04-18 pre-Batch-A `bench_supported_twice.sh` trace artifact:
  `contrib/pg_volvec/benchmarks/tpch_supported_twice_20260418_191226.tsv`.
  The sweep used `pg_volvec.trace_execution_path=on`,
  `pg_volvec.parallel_experimental_hash_pipeline=on`, 4 workers, and two runs
  per query. It skipped Q2/Q17/Q21 by default.
- [x] Batch A safety result, 2026-04-18:
  `contrib/pg_volvec/benchmarks/tpch_supported_twice_20260418_194010.tsv`.
  Q4 and Q18 now report
  `path=volvec_serial_after_parallel_skip detail=query scheduler does not yet
  support aggregate-bridge hash build source safely`, with Q4 returning 5 rows
  and Q18 returning 624 rows.
- [x] Batch A correctness checks: Q4 native-vs-pg_volvec diff passed; Q18
  native-vs-pg_volvec diff passed with 624 rows on both sides.
- [ ] Queries that reached QueryScheduler in the pre-Batch-A sweep: Q4, Q7,
  Q8, Q9, Q12, Q14, Q18, Q19. After Batch A, Q4/Q18 are intentionally removed
  from the accepted QueryScheduler set until aggregate-bridge HashBuildSource
  can be implemented without single-worker materialization.
- [ ] Queries that did not need QueryScheduler and used the process-parallel
  aggregate path: Q1, Q6.
- [ ] Queries that built parallel metadata but skipped QueryScheduler because
  `SortMerge`-shaped plans are not supported there yet, then ran through
  pg_volvec serial fallback: Q3, Q5, Q10, Q11, Q16.
- [ ] Queries that stayed in pg_volvec serial because no parallel scheduler was
  built: Q13, Q20, Q22.
- [ ] Q15 still falls through to native PostgreSQL execution in this script
  shape (`path=native_pg reason=no_registered_pg_volvec_state`), likely because
  the `revenue0` view create/use/drop wrapper does not pass the current
  admission/lowering path.
- [x] Q17 Batch B result, 2026-04-18:
  `contrib/pg_volvec/benchmarks/tpch_supported_twice_20260418_202603.tsv`.
  Forced QueryScheduler/experimental-hash run completed with
  `path=query_scheduler detail=experimental_hash_pipeline rows=1`; result
  matched pg_volvec serial (`3295493.51285714`).
- [ ] Q7/Q12 experimental scheduler smoke is correct with 4 workers and uses
  scheduler-finalized shared HashJoin bridges. The bridge artifacts are still
  file-backed, so performance work remains.
- [ ] Q7/Q12 still spend too much time in scheduler HashJoin artifact
  serialization/file transfer in some paths, but the 2026-04-18 timing/xctrace
  pass showed the dominant cost is currently source execution: Q12 spends most
  time in HashBuildSource scan/filter/project/hash build, while Q7 spends most
  time in the final HashProbeSource scan/probe/aggregate pipeline. Artifact
  movement is measurable but not the primary 10s+ cost in those runs.
- [ ] Q7/Q12 optimization should next target scan/deform/filter/string expr and
  HashJoin probe/build kernels before spending more time on grouped aggregate
  partial export.
- [x] Raise the default parallel morsel size from 128 to 512 blocks after a
  Q7/Q12 sweep showed 512 reduces scheduler/block-range overhead without the
  Q7 load-balance regression seen at 2048 blocks.
- [x] Use PostgreSQL `read_stream` for scheduler-assigned morsel block ranges.
  The previous `VecSeqScanState` block-range path called `ReadBufferExtended`
  per page and bypassed PG's async/batched read stream.  The scan now keeps
  pg_volvec's DAG/morsel block ownership but reads each range with
  `read_stream_begin_relation(... block_range_read_stream_cb ...)`, which cut
  Q12 to about 3.1s and Q7 to about 3.9s in the 2026-04-18 local smoke.
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

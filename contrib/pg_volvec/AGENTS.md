# pg_volvec KNOWLEDGE BASE

**Refreshed:** 2026-04-30
**HEAD:** `6c344eb036d` ("pg_volvec: fix BGWH_STARTED-vs-ProcArray race in pipeline leader") + ~36 modified TUs / +4537/-1523 + 5 new source pairs (`aggregate_hash_table.{cpp,hpp}`, `tuple_data_collection.{cpp,hpp}`, `tuple_data_layout.{cpp,hpp}`, `tuple_data_ops.{cpp,hpp}`, `physical_projection.{cpp,hpp}`).
**Phase:** `M-FRAME-MIN` step 3g.2-final — runtime end-to-end is **plumbed**: SeqScan → HashAgg → Order → OutputSink runs in leader+workers, descriptor publish/load works cross-process, leader drains the global TDC after FINALIZE. **Two open bugs** stop Q1 from emitting rows to the client:
- **Bug G** — HashAgg dedupe wrong (row_count=3 vs expected 2, no group merge).
- **Bug I** — OutputSink emits 3 rows internally but `psql` shows 0 (`EmitGlobalTdcToDest` → `DestReceiver` path drops them).

Q1 still falls back to standard PG when run end-to-end (correct results).

## OVERVIEW

PostgreSQL extension that hooks `ExecutorStart/Run/Finish/End` and offloads supported OLAP plan subtrees into a C++ columnar `DataChunk` executor with LLVM JIT on tuple deform and expression evaluation. Active scope is **Q1 only** (target `M-Q1-PERF`); Q6 parked at `M-Q6-RESTORE`.

The legacy broad TPC-H prototype (Q1–Q22, HashJoin, MergeJoin, SubqueryScan, the legacy serial vector executor under `src/engine/exec/`, and the legacy morsel parallel runtime) was **intentionally deleted** in commits `53ac06adcb7` (step 1) and `fd9a8aaf326` (step 2). The new runtime is a DuckDB-style `PhysicalOperator` + `MetaPipeline` design over PostgreSQL DSM/DSA + parallel bgworkers.

## RUNTIME STATE (HEAD + uncommitted)

End-to-end Q1 reproduce (`/tmp/q1_diag.sql` against `~/data/pg_tpch`) walks the full pipeline:

```
SeqScan FP11 count=3                                          OK
HashAgg.GetData ENTER tdc.finalized=1 row_count=3             OK (should be 2 → Bug G)
RUN.GetData pipeline_id=1 sres=0 src_chunk.count=3            OK
Order LEADER ALLOC payload_dp=1359872                         OK (worker0 alloc)
Order LOAD     payload_dp=1359872 (leader RUN)                OK (cross-process publish)
Order.Finalize ENTER row_count=3 / sort_indices_dp=667672     OK
Output.Finalize ENTER global_tdc=… row_count=3                OK
psql output rows                                              0  (Bug I)
```

Cross-process plumbing is solid; the two open bugs are local to HashAgg dedupe and OutputSink → DestReceiver emission.

## BUGS LANDED THIS CYCLE

| Bug | Where | Fix |
|-----|-------|-----|
| Race | `pipeline_leader.cpp` | `BGWH_STARTED`-vs-`ProcArray` race (committed at `6c344eb036d`). |
| A | `pipeline_leader.cpp` | PostmasterDeath shutdown path: poll on `BGWH_STOPPED` + `WaitLatch` with no hard timeout; `worker_ready` array sized by GUC. |
| B | `physical_seq_scan.cpp` | Leader self-allocates SeqScan local state; worker fallback via descriptor Load. |
| B' | `translator.cpp` | `dsa_allocate0` for `SchemaDescriptor` + `QualDescriptor` BEFORE `PhysicalSeqScan` ctor (Store/Load symmetry). |
| C-pre | `physical_seq_scan.cpp` | Removed stray `heap_prepare_pagescan` (caused leader/worker double-init). |
| C | `pipeline_leader.cpp:73-101,125,616,634,651` | Split `SignalShutdownAndWait` from `ShutdownAndDestroy`; only success path keeps Wait. |
| E | `physical_hash_aggregate.cpp:285-304` | `GetData` gating uses DSA-authoritative `global_tdc->finalized` flag (the `HashAggGlobalSourceState::finalized` field is a stale snapshot from `GetGlobalSourceState` time and must NOT be trusted across runtimes). |
| F | `physical_hash_aggregate.cpp:206-244` | Removed leader-only guard from `Combine`. Each worker MUST run its own `Combine` because `local_tdc` lives in backend-private memory; a leader-only Combine would silently drop every other worker's partials. (Matches DuckDB.) |
| H | `physical_order.cpp:78-113` | `GetGlobalSinkState` does `LoadSharedPayloadFromDescriptor` BEFORE considering `dsa_allocate0`. The `shared_payload_dp_` field on `PhysicalOperator` is per-process (each backend reconstructs its own instance), so it can never be cached as the source of truth — Load-before-alloc is now an invariant for every sink. |
| G | `physical_seq_scan.cpp` (bpchar varlena decode in `INT32_CHAR` branch) | HashAgg dedupe was correct all along; SeqScan was emitting garbage `l_returnflag`/`l_linestatus` because the bpchar payload was being read past the varlena header — distinct rows looked distinct. Fixing the header skip collapses Q1 from 3 rows to the expected 2 (A/F, B/O). |
| I | `output_sink.cpp` (`EmitGlobalTdcToDest` AVG path + scale resolution) + `translator.cpp` (output schema layout publish) | psql received 0 rows because `EncodeColumn` resolved NUMERIC scale from a per-process layout that wasn't published cross-process; AVG_NUMERIC also tried to divide a count from a freshly-zeroed dest row. `EmitGlobalTdcToDest` now reads the finalized scaled-int64 sum directly from the row at `row_ptr+offset` (Gather finalizes AVG sum/count at producer where 16B state is intact), and the layout publish now travels with the descriptor. |
| J | `tuple_data_ops.{cpp,hpp}` (Scatter split) + `physical_hash_aggregate.cpp:184` (call-site swap) + `output_sink.cpp` (AVG read path) | Q1 emitted wrong aggregate values (avg_qty=10.35 not 15.05; avg_price=315100 not 150). Root cause: the unified `Scatter` was unconditionally copying agg-state slots from `chunk.int64_columns[column_count + a]` into freshly-zeroed row buffers — at HashAgg's `SinkChunk` call site the input chunk has only source columns (no agg state), so this corrupted the accumulators with discount/extprice values BEFORE `UpdateAggregates` ran. Fix: split into `ScatterGroupOnly` (HashAgg uses; copies group cols only) vs full `Scatter` (Order/Output use; copies group cols + finalized agg state from a Gather-produced chunk, with `count=1` idempotency stamp on AVG_NUMERIC tail so chained Gather→Scatter→Gather across HashAgg→Order→Output TDCs stays stable). Header doc on the two functions documents the mutually exclusive `USE FOR` / `DO NOT USE FOR` call-site contract — the wrong choice silently corrupts agg state with no compile error. |
| K | `physical_seq_scan.{cpp,hpp}` (GetData drain loop + `morsel_active` field) + `pg_volvec.c` (new GUC `pg_volvec.parallel_morsel_nblocks`, default 512) | Q1 1M-row tail-truncation: morsel_nblocks was hardcoded to 8 (~1088 rows/morsel > `PIPELINE_DEFAULT_CHUNK_SIZE=1024`), so the source's chunk-fill exit dropped the morsel tail. DuckDB-faithful Z-model fix: source `GetData` drains across morsel boundaries within a single call; `morsel_active` cross-call only when chunk fills mid-morsel (`HAVE_MORE_OUTPUT` preserves resume); morsel size is now GUC-driven and decoupled from chunk size. |
| K' | `physical_seq_scan.cpp:333-338` | Q1 1M-row over-counting by exactly the morsel count (15× at nblocks=512, 29× at 256, 8× at 1024) after Bug K landed. Root cause: `heap_setscanlimits` was called BEFORE `heap_rescan`, but `heap_rescan(set_params=true)` calls `initscan()` which unconditionally resets `rs_numblocks = InvalidBlockNumber` (heapam.c:459), wiping the per-morsel limit. Each morsel then scanned `[start, EOF)` instead of `[start, end)`, producing `morsel_count × actual_rows`. Fix: swap the order — `heap_rescan` first, `heap_setscanlimits` second. With workers ∈ {1,2,4} all groups now match native PG byte-for-byte. |
| L | `pipeline_leader.cpp:107-149,278,632-639` (+`pipeline_worker_main.cpp` printf strip) | Second parallel query in a session SEGV'd at `aset.c:1060` inside `AllocSetAlloc`. Root cause: stack-local `PgVector<...>` containers in `PgvolvecPipelineRun` palloc'd into `leader_mcxt`; the success and PG_CATCH paths called `MemoryContextDelete(leader_mcxt)` *before* those C++ destructors ran (or PG_RE_THROW longjmp'd over them), so destructor `pfree()` calls read freed chunk headers and corrupted the parent context's freelist — the next query's first `palloc` died. Fix: introduce a stack-scope `LeaderMemoryContextGuard` RAII whose dtor runs the existing `DestroyLeaderMemoryContext` AFTER all C++ containers in the same scope have been destroyed; remove the explicit `DestroyLeaderMemoryContext` from success path, `FailEarly`, and PG_CATCH (PG_CATCH leaves teardown to the caller's transaction abort because longjmp skips destructors). Verified: 7-query in-session stress passes; correctness byte-exact at workers ∈ {1,2,4}. |
| M | `bridge/pg_volvec.c:77-103` (new helper) + `runtime_dsm.cpp:21,85` (call-site swap) | Each parallel query called `LWLockNewTrancheId("pg_volvec_runtime_dsa")` from `CreateRuntimeDsm`, leaking one tranche ID per invocation against PG's hard 256-tranche-per-backend cap. Backends died around query #50 with `ERROR: maximum number of tranches already registered`. Fix: allocate the tranche ID once cluster-wide via `GetNamedDSMSegment("pg_volvec_runtime_dsa_tranche_id", sizeof(int), init_cb, &found, NULL)` (canonical pattern, see `src/test/modules/test_dsa`); `CreateRuntimeDsm` calls a thin `pg_volvec_runtime_dsa_tranche_id()` helper. No `shared_preload_libraries` requirement — DSM registry is lazy. Verified: 200-query in-session stress passes; perf neutral (pgvv-par-w4 174.71 ms vs pre-fix 174.58 ms median-of-5). |
| N | `physical_seq_scan.cpp:362-378` (`MaxThreads` body) | Parallel SeqScan RUN fan-out collapsed to 1: every worker config w∈{1,2,4,8} ran the entire scan on worker 0, with workers 1..N-1 spinning idle in `TryPopForWorker` (12-bucket diagnostic showed `loop_wait == active_w_task` exactly). Root cause: `PhysicalSeqScan::MaxThreads(ctx)` read the per-process operator member `shared_payload_dp_`, which is `InvalidDsaPointer` from the ctor on every backend — only `GetGlobalSourceState` ever stamps it on the leader, and even then the worker's instance is a fresh reconstruction. `ResolveSeqScanPayload(nullptr)` → `ComputeMaxThreadsFromPayload(nullptr)` returns 1 (defensive default), so `DeriveRunTaskCount` clamped fan-out to `min(worker_count, 1, …) = 1`. Fix: mirror the Bug H invariant — `dp = DsaPointerIsValid(shared_payload_dp_) ? shared_payload_dp_ : LoadSharedPayloadFromDescriptor(this)` so workers (and the leader before it self-allocates in this same call) read the cross-process channel. Ordering safe because `PipelineRunEvent::Schedule` pre-invokes `GetGlobalSourceState` on the leader (publishes the descriptor slot) before `EnqueueTasks` calls `MaxThreads`. Verified: Q1-1M correctness `250000×4` byte-exact at w=4; perf best-case w=8 = **34 ms** vs w=1 = **158 ms** = **4.6× speedup** — confirms morsel sharding (atomic `next_block` + per-worker `heap_setscanlimits`+`heap_rescan` already correct from Bug K') is now driven by the right number of workers. |
| O | `dsm_task_queue.{hpp,cpp}` (PID registry + wake-on-pop) + `pipeline_leader.cpp` (PID registration before EnqueueTasks) | Bimodal latency tail: w=4 1008–3010 ms / w=8 best 34 / worst 5014 ms. Root cause: workers blocked in `WaitLatch(MyLatch, 1000ms)` after `TryPopForWorker` returned empty had no peer wakeup mechanism — when a peer dequeued and produced more work (or the leader enqueued the next phase), only the leader latch was set, leaving idle workers stranded until their 1000 ms timeout (powers of 1000 ms tail signature). Fix: leader publishes worker PIDs into the queue's DSM control struct before `EnqueueTasks`; every successful `TryPopForWorker` calls `WakeRegisteredWorkers` which iterates `worker_pids_` and `SetLatch(&BackendPidGetProc(pid)->procLatch)` on each. Vyukov queue invariants (CAS on `dequeue_pos_` + sequence gate) untouched, so wake-on-pop is purely additive — workers either find work or re-park, never see torn state. Verified: 60/60 correctness trials at w∈{4,8}; perf median tightened to {97,53,32,22} ms at w∈{1,2,4,8} median-of-5 with no >200 ms tail, 4.35× speedup at w=8 vs w=1. |
| P | `aggregate_hash_table.{hpp,cpp}` (new `AggregateHashTableCombineRow` helper) + `physical_hash_aggregate.cpp` (`Combine` refactor) | Phantom zero-agg rows (`A\|F\|0\|0.00`, `A\|O\|0\|0.00`) appeared 1/5 trials at w=4 after Bug O closed; pre-existing race that wake-on-pop unmasked by tightening worker concurrency. Root cause: original `Combine` did **speculative** append into the global TDC BEFORE probing the hash table — `TupleDataCollectionAppendRow(global_tdc)` reserves a row at the tail, then `AggregateHashTableFindOrInsert` probes; on duplicate the code called `RollbackLastAppend(global_tdc, candidate_idx)` which only succeeds when `row_count == candidate_idx + 1`. Under concurrent COMBINE workers, a peer worker would append between this worker's reservation and rollback, making rollback no-op and stranding a row with group cols copied but agg state still zero. `HashAgg.GetData` scans the global TDC by `row_count` and emits the stranded row. Fix: new helper `AggregateHashTableCombineRow(aht, tdc, layout, src_row, hash)` holds `aht->mutex` across the full sequence {linear-probe → on-miss `AppendRow` + memcpy group cols + claim slot → on-hit resolve canonical row → `CombineAggregates(layout, canonical_row, src_row)` → release}, so any row visible at the global TDC tail is fully populated. Helper allocates **after** confirmed miss — no speculative tail row exists, no rollback path, race eliminated by construction. Mutex coverage acceptable for v1 single global AHT (also serializes per-group agg merges, preventing lost-update); per-thread local AHT lanes deferred. Verified: 60/60 trials at w∈{4,8} byte-exact, 50-query in-session stress zero log errors. |

All MetaPipeline runtime bugs through Bug P are landed. No open executor bugs.

Q1 emits correct results end-to-end through the MetaPipeline runtime (parallel-on AND parallel-off paths) at both small fixture and 1M-row scale across workers ∈ {1,2,4,8} (1M-row best-case correctness verified at w=4 post-Bug-N). In-session stability: 200 sequential parallel Q1 queries in one backend, zero errors (Bug L+M closed). All `PGVOLVEC_DIAG` fprintf instrumentation has been stripped from the runtime sources; AGENTS.md doc references the tag for historical context only.

## Q1-1M PERF SNAPSHOT (2026-04-30, post Bug O+P, median-of-5)

| config | median ms | vs native-serial | vs pgvv w=1 |
|---|---:|---:|---:|
| native-serial | 137.22 | 1.00× | — |
| native-parallel-default | 52.01 | 2.64× | — |
| pgvv-par-w1 | 97.46 | 1.41× | 1.00× |
| pgvv-par-w2 | 52.76 | 2.60× | 1.85× |
| pgvv-par-w4 | 31.88 | 4.30× | 3.06× |
| pgvv-par-w8 | **22.38** | **6.13×** | **4.35×** |

Tight std dev across 5 trials at every w (no >200 ms outliers). Bug O+P together unlocked the parallel speedup the morsel sharding (Bug K') and fan-out fix (Bug N) had built — and pgvv-par-w8 now beats native-parallel-default by **2.32×** on 1M Q1.

Stress: 50 sequential parallel Q1 queries in one backend session at w=4 — 200/200 rows correct, zero log errors (Bug L+M still solid).

## SESSION SIMPLIFICATIONS (acknowledged, deferred)

- `tts_isnull` hardcoded `false` in OutputSink (Q1 columns are non-null).
- AVG precision: `sum_scaled / count` at scale=2 (loses precision vs `numeric_div`).
- Sort: `MaxThreads=1`, in-memory single-run.
- HashAgg `Combine` serialized via `AggregateHashTable::mutex` (pg_duckdb uses partitioned lanes — see `docs/ARCHITECTURE_DEVIATIONS.md`).
- bgworker entry uses **DSM/DSA + descriptor publish**, not `shm_mq` / `TupleQueueReader`.

## STRUCTURE

```
contrib/pg_volvec/
  src/bridge/                      # PG hook integration, GUCs, query-state HTAB     → bridge/AGENTS.md
    pg_volvec.c                    # _PG_init, GUC table, hook chaining
    state.{c,h}                    # PgVolVecQueryState HTAB + admission filter
    execute.{cpp,h}                # Translator dispatch + pipeline run
  src/engine/core/                 # DataChunk, types, memory, DSA bridge, robin-hood adapter → core/AGENTS.md
  src/engine/expr/                 # Expression IR header (expr.hpp)
  src/engine/                      # expr.cpp (lowering+interpreter), JIT pair, volvec_engine.hpp
  src/engine/parallel/             # Container only; no source files                 → parallel/AGENTS.md
  src/engine/parallel/pipeline/    # PhysicalOperator IR + MetaPipeline runtime      → parallel/pipeline/AGENTS.md
                                   # (the only runtime; ~52 files, 22 active TUs)
  sql/, expected/                  # Regress: q1, q6, smoke
  docs/                            # ARCHITECTURE_DEVIATIONS (NEW), GLOBAL_LOCAL_STATE_DESIGN, PIPELINE_PORT_PLAN; pre-greenfield files banner-marked STALE
  pg_duckdb_architecture.md        # Reference architecture (untracked, 109 lines) — see docs/ARCHITECTURE_DEVIATIONS.md
  third_party/                     # Vendored robin-hood-hashing
  perf/                            # Q1 perf progression notes
```

Active translation units (per `meson.build`): C bridge (`pg_volvec.c`, `state.c`), C++ bridge (`execute.cpp`), expression (`expr.cpp`), pipeline runtime (`pipeline_worker_main.cpp`, `pipeline_leader.cpp`, `physical_operator.cpp`, `meta_pipeline.cpp`, `pipeline_descriptor.cpp`, `event.cpp`, `pipeline_run_event.cpp`, `pipeline_combine_event.cpp`, `pipeline_finalize_event.cpp`, `task.cpp`, `dsm_task_queue.cpp`, `task_scheduler.cpp`, `physical_seq_scan.cpp`, `physical_hash_aggregate.cpp`, `physical_order.cpp`, `physical_projection.cpp`, `output_sink.cpp`, `translator.cpp`, `aggregate_hash_table.cpp`, `tuple_data_collection.cpp`, `tuple_data_layout.cpp`, `tuple_data_ops.cpp`), core DSA (`parallel_dsa_bridge.cpp`), LLVM JIT pair (when `llvm.found()`).

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| Hook + GUC + admission | `src/bridge/` | See `bridge/AGENTS.md` |
| Pipeline runtime / parallel | `src/engine/parallel/pipeline/` | See `parallel/pipeline/AGENTS.md` |
| DataChunk / types / DSA bridge | `src/engine/core/` | See `core/AGENTS.md` |
| Expression lowering | `src/engine/expr.cpp`, `expr/expr.hpp` | Linear IR + interpreter |
| Expression JIT | `src/engine/llvmjit_expr.cpp` | LLVM, gated by `USE_LLVM` |
| Deform JIT | `src/engine/llvmjit_deform_datachunk.cpp` | LLVM, gated by `USE_LLVM` |
| Plan→PhysicalOperator translation | `parallel/pipeline/translator.cpp` | Q1 shape-matcher live |
| TupleData / hash table | `parallel/pipeline/{tuple_data_*,aggregate_hash_table}.{cpp,hpp}` | NEW this cycle |
| Architecture deviations | `docs/ARCHITECTURE_DEVIATIONS.md` | NEW — diff vs `pg_duckdb_architecture.md` |
| Add a regress test | `sql/` + `expected/` + `meson.build` regress list | Harness intentionally minimal until M-Q1-PERF |

## CONVENTIONS

- **C/C++ boundary**: C files use `pg_volvec_` prefix and `extern "C"` headers when included from C++. C++ uses `namespace pg_volvec` (and `pg_volvec::pipeline` for runtime). Engine `.hpp` files wrap PG includes in `extern "C" {}`.
- **Include guards**: C headers `#ifndef PG_VOLVEC_*`. C++ headers `#pragma once`.
- **Indent**: Tabs, 4-space width (`.editorconfig`).
- **Build**: Meson only. Module-level `-O3 -march=native -ftree-vectorize -funroll-loops -ffast-math` applied to all C/C++.
- **Memory**: PostgreSQL `MemoryContext` (palloc/pfree/AllocSetContextCreate) for per-query objects; `PgMemoryContextAllocator` for STL containers. Never raw malloc in hot paths.
- **Cross-process state**: leader-built objects that need to reach workers are serialized into DSA via `pipeline_descriptor.cpp` (`StoreSharedPayloadOnDescriptor` / `LoadSharedPayloadFromDescriptor`). Atomics live in `PipelineSharedControl` directly (DSM-resident). **Invariant: every sink MUST `Load`-before-`alloc` for `shared_payload_dp` because the operator field is per-process** (Bug H lesson).
- **JIT symbol resolution**: prefer `dlopen(NULL)` + `dlsym` from running process before loading the provider library.
- **Restart discipline**: always `pg_ctl restart` after `meson install`. Backend will not pick up a freshly installed `pg_volvec.so` on its own.
- **No `.inc` template files.** The pipeline runtime has none; the legacy `runtime_*.inc` family is deleted.
- **Comment policy** (per CLAUDE.md priority guidelines): mode-1/3 comments must justify *why* (not what); compress mode-3 comment line counts where possible.

## ANTI-PATTERNS

- **Do NOT assume backend picks up new `.so` without restart.** Always `pg_ctl restart` after `meson install`.
- **Do NOT reintroduce the legacy serial executor or morsel runtime.** `src/engine/exec/`, `Vec{SeqScan,Filter,Agg,Project,Sort,Limit}State`, `parallel_runtime.cpp`, `runtime_*.{cpp,hpp,inc}`, `ParallelPipelineRole/Desc/Driver/Sink`, `LoweredPipeline`, `WorkerPipelineExecutor`, `pipeline_lowering.{hpp,cpp}`, `q1_translator.*`, `partial_agg_op.*`, `agg_sink.*`, `seq_scan_source.*`, `worker_context.hpp` — all intentionally deleted.
- **Do NOT widen translator to non-Q1 shapes.** Q2–Q22 out of scope for `M-Q1-PERF`. Q6 belongs to `M-Q6-RESTORE`.
- **Do NOT extend Sort beyond MaxThreads=1, in-memory single-run.**
- **Do NOT change `PhysicalOperator` base virtual signatures** (locked in `eb7901b022a`). New per-operator state goes through descriptor-resident payloads, not new virtuals.
- **Do NOT publish palloc'd pointers to DSA.** Use DSA offsets via `dsa_get_address`/`dsa_allocate`. Atomics go in DSM-resident `PipelineSharedControl`, not DSA.
- **Do NOT trust per-process operator fields as cross-process state.** `shared_payload_dp_` on the operator instance is per-backend; always Load from descriptor first (Bug H invariant).
- **Do NOT make `Combine` leader-only.** `local_tdc` is backend-private; every worker must run its own `Combine` against the `GlobalSinkState` (Bug F lesson).
- **Do NOT trust `*GlobalSourceState::finalized` as authoritative.** It is a snapshot at `GetGlobalSourceState` time. Read `global_tdc->finalized` from DSA (Bug E lesson).
- **Do NOT call `elog(ERROR)` from a worker without first writing `PipelineSharedControl.worker_error{,_msg}`.** Buffer is `PIPELINE_WORKER_ERROR_MSG_LEN = 256` bytes (`dsm_control.hpp:20`).
- **Do NOT skip `MemoryContextSwitchTo` before C++ object construction.**
- **Do NOT touch `parallel_plan` / `parallel_scheduler` void* fields outside `parallel/pipeline/`.**
- **Do NOT enable JIT for Wide128 numeric expressions.** Interpreter-only for correctness.

## COMMANDS

```bash
# Build + install + restart
CCACHE_DISABLE=1 PATH=/opt/homebrew/bin:$PATH meson compile -C build pg_volvec
CCACHE_DISABLE=1 PATH=/opt/homebrew/bin:$PATH meson install -C build --only-changed
./installed/bin/pg_ctl -D ~/data/pg_tpch restart -m fast -l ~/data/pg_tpch/logfile

# Q1 reproduce (cross-process plumbing OK; emit blocked on Bug G/I)
: > ~/data/pg_tpch/logfile
./installed/bin/psql -h /tmp -p 5432 -d postgres -c "DROP TABLE IF EXISTS lineitem_q1;"
./installed/bin/psql -h /tmp -p 5432 -d postgres -f contrib/pg_volvec/sql/q1.sql

# Disable PG parallel for clean single-thread benchmarks
SET max_parallel_workers_per_gather = 0;
SET parallel_setup_cost = 1000000000;
```

## NOTES

- **Active scope:** Q1 only (M-Q1-PERF). Q2–Q22 out of scope.
- **bgworker model:** DSM keys `0xD8…0001/0008/0009/000A`, magic `0x56505043`. No `shm_mq` / `TupleQueueReader`. Workers DSA-attach, reconstruct descriptor, drain `DsmTaskQueue`, write `DataChunk` into DSA TDC; leader P0 does FINALIZE then `EmitGlobalTdcToDest` → `DestReceiver`.
- **`EventId` convention:** `pid*3 + {0,1,2}`. Workers atomic-decrement `EventShmState.tasks_remaining` and `SetLatch` on the leader; only the leader calls `FinishEvent`.
- **`LEADER_WORKER_INDEX = -1`.** GUC `pg_volvec.parallel_max_workers=4`; `morsel_nblocks=8`.
- **Sort scope:** `MaxThreads=1`, in-memory single-run.
- **Authoritative reference architecture:** `contrib/pg_volvec/pg_duckdb_architecture.md` (untracked, 109 lines). Deviations recorded in `docs/ARCHITECTURE_DEVIATIONS.md`.
- **Pre-greenfield docs** under `docs/` (DESIGN.md, ROADMAP.md, LOCAL_RUNBOOK.md, jit_deform_datachunk.md, llvmjit_expr.md, page-wise-scan.md) are banner-marked `STALE`.
- **Test data:** 10G TPC-H lives at `~/data/pg_tpch` (socket `/tmp`, port 5432, db `tpch`/`postgres`).
- **Profiling:** `/usr/bin/sample` on macOS; FlameGraph at `~/proj/postgres/FlameGraph/`.

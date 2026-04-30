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

Open: none in M-FRAME-MIN step 3g.2-final. Q1 emits correct results end-to-end through the MetaPipeline runtime (parallel-on AND parallel-off paths). All `PGVOLVEC_DIAG` fprintf instrumentation has been stripped from the runtime sources; AGENTS.md doc references the tag for historical context only.

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

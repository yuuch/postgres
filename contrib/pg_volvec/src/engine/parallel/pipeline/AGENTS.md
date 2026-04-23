# pipeline/ — DuckDB-Style Push-Based Pipeline (Active Core)

25 files. The **only** parallel runtime in the codebase. Replaces deleted `parallel_runtime.cpp` + `runtime_*.cpp/.inc` family. Greenfield Q1+Q6 shape only: `SeqScan -> [Filter] -> PartialAgg -> AggSink`.

## OVERVIEW

DuckDB-style push pipeline: each pipeline is `(Source, [Operator...], Sink)`. Source pulls morsels from a heap relation, operators transform `DataChunk<1024>` in place, sink accumulates partial state per worker, leader combines + finalizes. Runs over PostgreSQL's parallel infrastructure (DSM/DSA/parallel bgworkers) but does **not** use PG's executor for the offloaded subtree.

## WHERE TO LOOK

| Task | File | Notes |
|------|------|-------|
| Public entry from bridge | `pipeline_leader.hpp/.cpp` | `PgvolvecPipelineRun(QueryDesc*, PgVolVecQueryState*, const char **failure_reason)` |
| Plan → pipeline lowering | `pipeline_lowering.hpp/.cpp` | Builds `LoweredPipeline { Source, PartialAggOp, AggSink }` from `VecPlanState` |
| Per-worker pipeline driver | `executor.hpp/.cpp` | `WorkerPipelineExecutor::Execute` — pulls source, runs ops, pushes to sink, until FINISHED |
| Worker entry point | `pipeline_worker_main.cpp` | bgworker `main`: attach DSM/DSA, deserialize PlannedStmt, run pipeline, exit |
| Worker per-query state | `pipeline_worker_state.hpp/.cpp` | `PipelineWorkerState`, `InitializePipelineWorkerState`, JIT proc-exit cleanup |
| Worker exec context | `pipeline_worker_context.hpp` | `PipelineWorkerContext` — per-worker handles to estate/plan/agg/scan |
| Legacy compat shim | `worker_context.hpp` | `ParallelWorkerContext` re-exported for older exec/ callers (P2 transition) |
| Source interface | `source.hpp` | `Source`, `GlobalSourceState`, `LocalSourceState`, `SourceResultType` |
| Operator interface | `operator.hpp` | `Operator`, `OperatorState`, `OperatorResultType` |
| Sink interface | `sink.hpp` | `Sink`, `Global/LocalSinkState`, `Sink/Combine/Finalize` types |
| Pipeline struct | `pipeline.hpp` | `Pipeline { id, Source*, vector<Operator*>, Sink* }`, `INVALID_PIPELINE_ID` |
| Common types | `types.hpp` | `ExecCtx`, `PipelineChunk = DataChunk<1024>`, `LEADER_WORKER_INDEX = -1` |
| Source impl (only one) | `seq_scan_source.hpp/.cpp` | Morsel-driven heap scan; pulls block ranges via `pg_atomic_uint64 next_block` |
| Operator impl: filter | `filter_op.hpp/.cpp` | Vectorized filter against `VecPlanState` qual |
| Operator impl: partial agg | `partial_agg_op.hpp/.cpp` | Per-worker partial aggregate; writes into shared `ParallelAggPartialState` slot |
| Sink impl (only one) | `agg_sink.hpp/.cpp` | Combine partials → finalize → materialize for leader return |
| DSM layout | `dsm_control.hpp` | `PipelineSharedControl` + `PIPELINE_DSM_KEY_*` (magic `0x56505043`) |

## CONVENTIONS

- **One pipeline shape only**: `SeqScan -> [Filter] -> PartialAgg -> AggSink`. Anything else fails admission in `pipeline_lowering.cpp` and the bridge falls back to serial.
- **Single source / single sink**: no DAG, no `depends_on` chain yet (`Pipeline::depends_on` reserved for future). Each query = one pipeline.
- **Worker indexing**: leader is `LEADER_WORKER_INDEX = -1`. Bgworkers are `0..N-1`. Leader participation gated by `pg_volvec.parallel_leader_participation` GUC.
- **Chunk size**: all chunks are `PipelineChunk = DataChunk<PIPELINE_DEFAULT_CHUNK_SIZE>` (1024 rows).
- **Morsel granularity**: `PipelineSharedControl.morsel_nblocks` blocks per atomic claim from `next_block`. Default from GUC `pg_volvec.parallel_morsel_nblocks` (512). Source returns FINISHED when claim ≥ `total_blocks`.
- **DSM keys**: `PIPELINE_DSM_KEY_*` in `0xD8...` range — distinct from any legacy `VOLVEC_PARALLEL_KEY_*`. Always go through `dsm_control.hpp` constants.
- **Partial slots**: `ParallelAggPartialState[partial_slot_count]` allocated in DSA via leader. `partial_slot_count == launched_worker_count` (+1 if leader participates). Workers write their own slot only.
- **Error propagation**: workers set `PipelineSharedControl.worker_error` (atomic uint32) before erroring. Leader checks after `Combine`.
- **MemoryContext discipline**: each worker creates `PipelineWorkerState::memory_context` (private). `WorkerPipelineExecutor::Execute` runs under `ExecCtx::mcxt`. Sink combine runs in leader's context.
- **Result enums** (do not reorder, JIT consumers depend on values):
  - `SourceResultType { HAVE_MORE_OUTPUT, FINISHED, BLOCKED }`
  - `OperatorResultType { NEED_MORE_INPUT, HAVE_MORE_OUTPUT, FINISHED, BLOCKED }`
  - `SinkResultType { NEED_MORE_INPUT, FINISHED, BLOCKED }`
  - `SinkCombineResultType { FINISHED, BLOCKED }`
  - `SinkFinalizeType { READY, NO_OUTPUT_POSSIBLE, BLOCKED }`

## ANTI-PATTERNS

- **Do NOT extend the pipeline shape inline.** New shapes go through `LowerToPipeline` admission, not by patching `WorkerPipelineExecutor` to special-case node types.
- **Do NOT add a second concrete `Source`/`Sink` without lifting ownership.** `LoweredPipeline` currently holds `unique_ptr<SeqScanSource>`/`unique_ptr<AggSink>` directly. Generalize the struct first.
- **Do NOT return `BLOCKED` from any path in P1.** Executor asserts + `ereport(ERROR)` on BLOCKED. Async pipeline support is post-P3.
- **Do NOT publish palloc'd pointers via `PipelineSharedControl` or DSA.** Use DSA offsets / `dsa_allocate`. Atomics live inside `PipelineSharedControl` directly (allocated in DSM, not DSA).
- **Do NOT skip `MemoryContextSwitchTo(state->memory_context)` before constructing `VecPlanState`/`VecAggState` in the worker.** JIT and operator construction require a long-lived per-query context.
- **Do NOT touch `worker_context.hpp` (`ParallelWorkerContext`).** It exists only as a transitional re-export for older `exec/` callers; will be deleted once those callers move to `PipelineWorkerContext`.
- **Do NOT call `ereport(ERROR)` from a worker without first setting `PipelineSharedControl.worker_error`.** Otherwise the leader cannot distinguish worker death from clean FINISHED.
- **Do NOT forget `RegisterPipelineProcExitJitCleanup` on worker init.** LLVM JIT contexts leak across bgworker exit otherwise.

## EXECUTION FLOW

1. **Bridge** (`bridge/execute.cpp`) calls `PgvolvecPipelineRun(qd, qstate, &reason)`.
2. **Leader** (`pipeline_leader.cpp`):
   - Allocates DSM segment with `PIPELINE_DSM_MAGIC`, registers shm_toc keys (control, plannedstmt, query_text, partials, source pscan, fileset, param_exec, dsa).
   - Initializes `PipelineSharedControl` (relid, plan node ids, atomics, slot count).
   - Allocates `ParallelAggPartialState` array in DSA, optional `SharedFileSet` for spill.
   - Calls `LowerToPipeline(root, shared_control, &next_block, partials, n, fileset)` → `LoweredPipeline`.
   - Launches up to `pg_volvec.parallel_max_workers` bgworkers via `RegisterDynamicBackgroundWorker`.
   - If `parallel_leader_participation`: leader runs `WorkerPipelineExecutor::Execute` itself with `worker_index = LEADER_WORKER_INDEX`.
   - Waits for workers (`WaitForParallelWorkersToFinish`), checks `worker_error`.
   - Calls `AggSink::Combine` over all partial slots, then `Finalize` → result chunk(s).
   - Hands result chunks back to bridge for slot materialization.
3. **Worker** (`pipeline_worker_main.cpp`):
   - `dsm_attach` + `shm_toc_lookup` for control/plannedstmt/dsa/etc.
   - `dsa_attach` to leader's area.
   - `InitializePipelineWorkerState(...)` — builds `EState`, `VecPlanState`, `VecAggState`, sets up scan desc.
   - Re-runs `LowerToPipeline` against the worker-local `VecPlanState`.
   - Runs `WorkerPipelineExecutor::Execute` until source FINISHED.
   - Writes its `ParallelAggPartialState` slot.
   - `CleanupPipelineWorkerState` + `dsa_detach` + exit.

## NOTES

- `SeqScanSource` is the only source today. Adding a new source means: (a) implement `Source`/`GlobalSourceState`/`LocalSourceState`, (b) extend `LoweredPipeline` to hold it (currently hard-typed to `SeqScanSource`), (c) extend `LowerToPipeline` admission.
- `PartialAggOp` writes into a worker-owned `ParallelAggPartialState` slot — this struct (NOT pipeline-local) is defined in `parallel/` parent and is preserved across the demolition.
- `AggSink::Combine` is leader-only and serial today; `Pipeline::Sink` interface allows for multi-stage combine in the future.
- `dsm_control.hpp` keys are intentionally outside any historical `VOLVEC_PARALLEL_KEY_*` range to make accidental cross-attach impossible.
- `executor.cpp` here is the **pipeline driver**, not to be confused with the dead `src/engine/executor.cpp` (in tree but unbuilt).
- Trace via `pg_volvec.trace_execution_path=on` to log pipeline build, worker dispatch, morsel claims, partial slot writes.

# pipeline/ — DuckDB-Style PhysicalOperator + MetaPipeline Runtime

**41 files, 17 active translation units (per `contrib/pg_volvec/meson.build` lines 10–26).** The **only** parallel runtime in the codebase. Replaces the deleted legacy `parallel_runtime.cpp` + `runtime_*.cpp/.inc` family. Active scope: **Q1 only** (target `M-Q1-PERF`); Q6 parked at `M-Q6-RESTORE`.

## OVERVIEW

DuckDB-faithful `PhysicalOperator` tree (unified `Source/Operator/Sink` base, `physical_operator.hpp`) is built by `Translator::TranslatePlan` from a PG `PlannedStmt`. The tree is sliced at blocking operators (`HashAggregate`, `Order`) into `MetaPipeline` chains via `PhysicalOperator::BuildPipelines`. Each pipeline becomes one or more `Task`s (`PipelineRunEvent`, `PipelineCombineEvent`, `PipelineFinalizeEvent`) and is dispatched onto a DSM-resident MPMC `DsmTaskQueue` (Vyukov bounded queue) by `TaskScheduler::EnqueueTasks`. PostgreSQL parallel bgworkers (and the leader) pop tasks and execute them. Cross-process state lives in DSA, published via `PipelineSharedControl.event_states_root` and `pipelines_root`, addressed by id through `pipeline_descriptor.cpp` Store/LoadSharedPayload + `pipeline_dsm_lookup.hpp`.

**Status (HEAD `71cd856975d`, step 3g.2-prep):** infrastructure is in place; the runtime is not yet end-to-end. Bridge admits Q1, `Translator::TranslatePlan` returns `nullptr` for every nodeTag, the bridge falls back to `standard_ExecutorRun`. Step 3g.2-final wires it up.

## MODULE STATUS

### Shipped (3g.2-prep)

| Module | Notes |
|--------|-------|
| `types.hpp` | `PipelineChunk = DataChunk<PIPELINE_DEFAULT_CHUNK_SIZE>` (1024 rows), `LEADER_WORKER_INDEX = -1`, `ExecCtx`. |
| `dsm_control.hpp` | `PipelineSharedControl` (DSM-resident), `PIPELINE_DSM_KEY_{CONTROL=…0001,DSA=…0008,TASK_QUEUE=…0009}` (`0xD800000000000000` range), magic `0x56505043`, `worker_error{,_msg[256]}` per Oracle C7. |
| `pipeline.hpp` | 37-LoC pipeline metadata struct (id, source, sink, depends_on stub). |
| `operator.hpp`, `source.hpp`, `sink.hpp` | Result enums (`SourceResultType { HAVE_MORE_OUTPUT, FINISHED, BLOCKED }`, `OperatorResultType { NEED_MORE_INPUT, HAVE_MORE_OUTPUT, FINISHED, BLOCKED }`, `SinkResultType { NEED_MORE_INPUT, FINISHED, BLOCKED }`, `SinkCombineResultType { FINISHED, BLOCKED }`, `SinkFinalizeType { READY, NO_OUTPUT_POSSIBLE, BLOCKED }`). Do not reorder. |
| `dsm_task_queue.{hpp,cpp}` | Vyukov MPMC bounded queue at `PIPELINE_DSM_KEY_TASK_QUEUE`. `TaskKind { RUN=1, COMBINE=2, FINALIZE=3 }` (`hpp:39`) **is current design** (not anti-pattern). `RegisterWorkerLatches` for wakeup. |
| `event.{hpp,cpp}` + `pipeline_run_event.{hpp,cpp}`, `pipeline_combine_event.{hpp,cpp}`, `pipeline_finalize_event.{hpp,cpp}` | Atomic dependency machinery. Workers atomic-decrement `EventShmState.tasks_remaining` and `SetLatch` on the leader; only the leader calls `FinishEvent`. Each event subclass has `Schedule` body in `task_scheduler.cpp`. `EventId = pid*3 + {0,1,2}`. |
| `physical_operator.{hpp,cpp}` | Base virtual signatures **locked** in `eb7901b022a` (do not change). `BuildPipelines` slices the tree at sinks. |
| `meta_pipeline.{hpp,cpp}` | `MetaPipeline` chain. **Known bug**: `meta_pipeline.cpp:16` — `meta->bundle_` not initialized before use; fix tracked in 3g.2-final. |
| `pipeline_descriptor.{hpp,cpp}` | Leader serialize + Worker reconstruct, Store/LoadSharedPayload (one `dsa_allocate0` per payload — never share allocations across payloads). |
| `pipeline_dsm_lookup.hpp` | Template id → object resolver against the descriptor. |
| `query_state.hpp` | Opaque `void* parallel_plan` / `parallel_scheduler` layout shared with `bridge/state.c`. |
| `task_scheduler.{hpp,cpp}` | `SchedulerState`, `BindRuntime`, `AllocateEventShmStates` (DSA array via `control->event_states_root`), `EnqueueTasks` dispatching on `Event::kind()`. C1 implemented in 3g.2-prep (353 LoC). |
| `physical_seq_scan.hpp`, `physical_hash_aggregate.hpp`, `physical_order.hpp` | Headers/types in place. `.cpp` bodies are partial — see Stub list. |

### Stub / Pending (3g.2-final)

| Module | Stub state | What's missing |
|--------|-----------|----------------|
| `translator.cpp` (35 LoC) | `TranslatePlan` returns `nullptr` for every nodeTag | Q1 shape-matcher (SeqScan → HashAggregate → optional Sort → output). |
| `pipeline_leader.cpp` (33 LoC) | `PgvolvecPipelineRun` returns `false` with `*failure_reason = "pipeline runtime not implemented yet"` | Real leader: build descriptor, allocate DSM/DSA, launch bgworkers, leader-participate, drive scheduler event loop, materialize results. |
| `pipeline_worker_main.cpp` (28 LoC) | `elog(ERROR)` on entry | bgworker `main`: attach DSM/DSA, reconstruct descriptor, pop tasks from `DsmTaskQueue`, execute, on `ereport(ERROR)` populate `worker_error{,_msg}`. |
| `task.cpp` (49 LoC) | All three `Execute()` paths return `TASK_FINISHED` | Real bodies: `PipelineRunEvent::Execute` drives source→operators→sink loop; `PipelineCombineEvent::Execute` runs sink combine; `PipelineFinalizeEvent::Execute` materializes. |
| `output_sink.cpp` (65 LoC) | `SinkChunk` only counts rows | Real materialization to result chunks for the leader. |
| `physical_seq_scan.cpp` | `AppendProjectedTupleToChunk` is a stub | Real per-tuple deform + project into `PipelineChunk`. |
| `physical_hash_aggregate.cpp` | `SinkChunk` writes `rf` / `ls` characters as hard-coded `'\0'` (real bug, not stub); `GetData` column writes missing | Fix the `'\0'` bug; implement `GetData` column writes; `Combine()` is real. |
| `physical_order.cpp` | `SinkChunk` / `GetData` column writes missing | Implement column writes (Sort is `MaxThreads=1`, in-memory single-run for 3g.2). |
| Bridge wire-up | `src/bridge/execute.cpp:20`-area still has the 1-line wire-up to `PgvolvecPipelineRun` pending | Replace fall-through with `PgvolvecPipelineRun` call once leader is real. |

### Five Handoff Myths (Corrected)

The previous narrative had five errors — the refreshed module status above is the ground truth:

1. **Myth:** `physical_seq_scan` `GetData` is a stub. **Reality:** ~80% real; only `AppendProjectedTupleToChunk` is the stub.
2. **Myth:** `physical_hash_aggregate` `SinkChunk` is fully a stub. **Reality:** `Combine()` is real; the `rf`/`ls` `'\0'` is a **bug**, not absence of code.
3. **Myth:** `AttachGlobal*State` virtuals are needed. **Reality:** unnecessary — descriptor-resident payloads cover cross-process state.
4. **Myth:** An `ExecutionAffinity` enum is needed for COMBINE dispatch. **Reality:** unnecessary — `TryPopForWorker` semantics on `DsmTaskQueue` cover affinity.
5. **Myth:** AGENTS.md ×3 are accurate. **Reality:** they were pre-3g.2 stale and pointed at long-deleted modules; this refresh corrects them.

## EXECUTION FLOW (Target — what 3g.2-final lights up)

1. **Bridge** (`bridge/execute.cpp`): `Translator::Translate(plannedstmt)` → `PhysicalOperator*` root (stored as opaque `void*` in query state). On `ExecutorRun`, dispatch to `pipeline::PgvolvecPipelineRun(qd, qstate, &reason)`.
2. **Leader** (`pipeline_leader.cpp`):
   - Allocate DSM with `PIPELINE_DSM_MAGIC`; register keys `_CONTROL`, `_DSA`, `_TASK_QUEUE`.
   - Initialize `PipelineSharedControl`; `dsa_create_in_place` at `_DSA`.
   - `BuildPipelines` to slice operator tree into `MetaPipeline`.
   - `pipeline_descriptor.cpp` serializes per-pipeline payloads into DSA, publishes `pipelines_root`.
   - `TaskScheduler::AllocateEventShmStates` for the per-event `EventShmState { pg_atomic_uint32 tasks_remaining; pg_atomic_uint32 saw_error; }` array, publishes `event_states_root`.
   - `TaskScheduler::EnqueueTasks` dispatches on `Event::kind()` into `DsmTaskQueue`.
   - Launches up to `pg_volvec.parallel_max_workers` bgworkers via `RegisterDynamicBackgroundWorker`.
   - Leader-participates (worker_index `LEADER_WORKER_INDEX = -1` if GUC enables) by popping tasks itself.
   - Drives the event loop: wait on latch, on completion atomic-dec triggers, leader calls `FinishEvent` to fan in next event.
   - Finalize fan-out=1 inline; materialize via `OutputSink`; check `worker_error{,_msg}`; hand back to bridge.
3. **Worker** (`pipeline_worker_main.cpp`):
   - `dsm_attach` + `shm_toc_lookup` for `_CONTROL`, `_DSA`, `_TASK_QUEUE`; `dsa_attach` to leader area.
   - Reconstruct descriptor IR via `pipeline_descriptor.cpp` Worker side.
   - Loop: `DsmTaskQueue::TryPopForWorker` (or COMBINE-affinity pop) → `Task::Execute`.
   - On `ereport(ERROR)`, populate `worker_error{,_msg}` before re-raising.
   - On task completion, atomic-decrement `EventShmState.tasks_remaining`; `SetLatch(leader_latch)` if zero. Workers **never** call `FinishEvent`.
   - Exit cleanly when queue is exhausted and event signals done.

## CONVENTIONS

- **Single shape (Q1 only):** SeqScan → HashAggregate → optional Sort → Output. Anything else fails admission in `Translator::TranslatePlan` (returns `nullptr`); bridge falls back to PG.
- **Worker indexing**: leader is `LEADER_WORKER_INDEX = -1`. Bgworkers are `0..N-1`. Leader participation gated by `pg_volvec.parallel_leader_participation` GUC.
- **Chunk size**: `PipelineChunk = DataChunk<PIPELINE_DEFAULT_CHUNK_SIZE>` (1024 rows).
- **DSM keys**: `PIPELINE_DSM_KEY_*` in `0xD8…` range — distinct from any historical `VOLVEC_PARALLEL_KEY_*` to make accidental cross-attach impossible. Always go through `dsm_control.hpp` constants.
- **Cross-process state**: descriptor-resident payloads in DSA (`pipeline_descriptor.cpp`), addressed by id via `pipeline_dsm_lookup.hpp`. **Never** use new `AttachGlobal*State` virtuals on `PhysicalOperator` — base signatures are locked.
- **DSA allocation**: one `dsa_allocate0` per payload. Never share an allocation across payloads.
- **Atomics**: `EventShmState.tasks_remaining` + `SetLatch(leader_latch)`. Workers **never** call `FinishEvent` — only the leader does.
- **EventId convention**: `pid*3 + {0,1,2}` (one slot per event kind per pid).
- **Task ctor**: `Task` carries `event_id` + `kind` (per Oracle Q1+Q2+Q6 finding).
- **MemoryContext discipline**: each worker creates its own per-query `MemoryContext`; pipeline operators run under `ExecCtx::mcxt`. Sink combine runs in the leader's context.
- **Error propagation**: workers populate `PipelineSharedControl.worker_error` (atomic uint32) **and** `worker_error_msg[PIPELINE_WORKER_ERROR_MSG_LEN]` before erroring. Leader checks both after the event drains.
- **Sort scope (3g.2 only)**: `MaxThreads=1`, in-memory single-run. No external sort, no parallel sort.

## ANTI-PATTERNS

- **Do NOT change `PhysicalOperator` base virtual signatures.** Locked in `eb7901b022a`. New per-operator state goes through descriptor-resident payloads.
- **Do NOT add `AttachGlobal*State` virtuals** or an `ExecutionAffinity` enum. Both are explicitly unnecessary (see "Five Handoff Myths" above).
- **Do NOT widen `Translator::TranslatePlan` to non-Q1 shapes.** Q2–Q22 are out of scope for `M-Q1-PERF`. Q6 belongs to `M-Q6-RESTORE`, later.
- **Do NOT extend Sort beyond MaxThreads=1, in-memory single-run** in 3g.2.
- **Do NOT introduce ExprBytecode lowering for non-null quals** in 3g.2. Direct interpreter is the in-scope baseline.
- **Do NOT have workers call `FinishEvent`.** Atomic-dec + `SetLatch` only. Leader is the sole `FinishEvent` caller.
- **Do NOT publish palloc'd pointers via `PipelineSharedControl` or DSA.** DSA offsets / `dsa_allocate` only. Atomics live in DSM-resident `PipelineSharedControl`.
- **Do NOT skip populating `worker_error{,_msg}` before `ereport(ERROR)`** in a worker. Otherwise the leader cannot surface the failure cause.
- **Do NOT reorder result enums** (`SourceResultType`, `OperatorResultType`, `SinkResultType`, `SinkCombineResultType`, `SinkFinalizeType`). Future JIT consumers depend on values.
- **Do NOT introduce a `BLOCKED` task state** in 3g.2. Async pipeline support is post-`M-Q1-PERF`.
- **Do NOT return `BLOCKED` from any path** in 3g.2; assert + `ereport(ERROR)` if observed.
- **Do NOT implement design doc `§8.7` as written** (per design doc line 1428).
- **Do NOT touch `docs/` design files for current-shape work.** They are pre-greenfield; see `.sisyphus/plans/{pipeline-port-plan,3g2-final-delta-map}.md` and `docs/GLOBAL_LOCAL_STATE_DESIGN.md` (`§6.3`, `§8.5.2-§8.5.4`, `§8.6`).
- **Do NOT reintroduce deleted modules:** `pipeline_lowering.{hpp,cpp}`, `executor.{hpp,cpp}` (the pipeline-driver one), `seq_scan_source.*`, `filter_op.*`, `partial_agg_op.*`, `agg_sink.*`, `pipeline_worker_state.*`, `pipeline_worker_context.*`, `worker_context.hpp`, `LoweredPipeline`, `WorkerPipelineExecutor`, `q1_translator.*`, `ParallelAggPartialState`, `ParallelPipelineRole/Desc/Driver/Sink`. Replaced by the `PhysicalOperator`/`MetaPipeline`/`Event`/`Task`/`TaskScheduler` cluster above.

## NOTES

- **TaskKind is current design** (`dsm_task_queue.hpp:39`). Earlier AGENTS.md flagged it as anti-pattern; that was wrong (the anti-pattern referred to the legacy enum from the deleted morsel runtime, which used the same name). The new `TaskKind { RUN=1, COMBINE=2, FINALIZE=3 }` is the canonical event-kind tag and is read by `TaskScheduler::EnqueueTasks` and `Task` constructors.
- **`PIPELINE_DSM_KEY_*` is current design.** Earlier AGENTS.md called it forbidden; the forbidden version was the legacy `0x…0002..0007` range, which has been retired.
- **`EventShmState` and DSA-publish-via-`event_states_root`/`pipelines_root` are current design.** Earlier AGENTS.md framed DSA-publish as forbidden; that referred to publishing palloc'd pointers, not DSA allocations.
- `task_scheduler.cpp` is 353 LoC — C1 (`SchedulerState`, `BindRuntime`, `AllocateEventShmStates`, `EnqueueTasks`) is implemented; only the leader event-loop driver and the per-event `Schedule` plumbing for the runtime cut-over remain.
- LSP may report `'utils/errcodes.h' file not found` on `dsm_control.hpp` — workspace include-path quirk, not a real error.
- Trace via `pg_volvec.trace_execution_path=on` once 3g.2-final lands.

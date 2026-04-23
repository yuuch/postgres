# Pipeline Refactor Design

Status: **Draft for review** · Owner: pg_volvec executor · Target dir: `contrib/pg_volvec/src/engine/parallel/`

## 1. Goals & Non-Goals

### Goals
1. Replace the role-driven worker dispatcher with a DuckDB-style **push-based pipeline** model. Every pipeline is `(Source, [Operators], Sink)`, executed by one generic loop.
2. Delete `ParallelPipelineRole` enum and all per-role branches in `runtime_worker_main.cpp` / `runtime_lowering.cpp` / `runtime_execution.cpp`.
3. Express **all** hash join shapes (incl. current Q3-style hybrid that has the agg_state bug) as Source/Op/Sink triples — bug disappears because the agg requirement attaches to an `AggSink`, not to a role name.
4. Preserve every leader contract documented in §5 (DSM keys, atomics, fallbacks, leader-as-worker, SharedFileSet lifecycle).
5. Each migration phase ends with a green build + `meson test pg_volvec` + a clean TPC-H sweep on the 10G dataset (subject to known pre-existing failures Q4/Q11/Q18/Q20/Q22).

### P2 locked decisions (2026-04-22 user clarification)
- **Scope**: Full greenfield rebuild (option B). Treat the project as new.
- **Legacy disposition**: Delete `runtime_lowering.cpp`, `runtime_worker_main.{cpp,inc}`, `runtime_worker_state.{cpp,inc}`, `runtime_execution.{cpp,inc}` immediately in P2.
- **Reuse boundary**: Keep `ParallelContext` setup + DSM key constants + `ParallelAggregateSharedControl::next_block` morsel atomic + scheduler launch/wait skeleton. Strip everything else from `parallel_runtime_internal.hpp`.
- **Bridge API**: New symbol names (`PgVolvecPipelineLower`, `PgVolvecPipelineRun`, `pg_volvec_pipeline_worker_main`). Rewire `bridge/execute.cpp` to call them.
- **Execution model**: Workers-only. Leader does orchestration + DSM allocation + worker spawn + wait + merge (`AggSink::Combine`/`Finalize`) + result materialization (`MaterializeSink`). Leader never dequeues pipeline tasks.
- **No GUC, no fallback**: `pg_volvec.use_pipeline_executor` is NOT introduced. New executor is the only path. Per user: "deprecated the old design".
- **Acceptable regressions**: Q1 + Q6 must pass. All other TPC-H queries may break temporarily until P3 (hash join) and P4 (partition-pair) land.

### Non-Goals (this refactor)
- No new operator coverage. Same plan shapes as today.
- No async I/O. `BLOCKED` is reserved in the enum but not exercised in phase 1 (see §11).
- No fix for pre-existing failures (Q4/Q11/Q18/Q20/Q22 robin-hood / DSA-1.3GB / Q22 crash). Tracked separately.
- No change to JIT pipeline (`llvmjit_expr.cpp`, `llvmjit_deform_datachunk.cpp`).
- No change to optimizer or plan-shape detection in `runtime_lowering.cpp` *beyond* deleting role assignments.

---

## 2. Background — Why role-driven failed

The current model assigns each worker a `ParallelPipelineRole` (HashBuildSource, HashBuildFinalize, HashProbeSource, HashOuterSource, GenericSource, BuildPartitionSource, ProbePartitionSource, HashJoinPartitionSource, AggFinalize). The dispatcher in `runtime_worker_main.cpp` then runs a long if-else over the role enum, with Source/Operator/Sink semantics **welded together** inside each branch.

Consequence: a "hybrid" plan (e.g. Q3-shape: hash-join-over-partition-pair feeding an aggregate) cannot be expressed because there is no role that says "PartitionPairSource + JoinOp + **AggSink**". The closest role is `HashJoinPartitionSource`, which hardcodes a Materialize/downstream sink and has no agg_state. Adding a new role is not the fix — every new combinatorial shape would need yet another role.

The user's diagnosis: *"为什么耦合这么紧呢？算子间应该不管的才对？"* This refactor decouples by making **every** combination expressible as `(Source, [Op…], Sink)` with three independent vtables.

---

## 3. Core Abstractions

Modeled after DuckDB commit `f9d17f0eb7a6` (`physical_operator.hpp`, `physical_operator_states.hpp`, `pipeline_executor.hpp`). Adapted for PostgreSQL: lifetimes via `MemoryContext`, shared state via `dsa_area` / DSM segments, no exceptions across the C boundary.

### 3.1 Result enums

```cpp
enum class OperatorResultType : uint8_t {
    NEED_MORE_INPUT,    // op consumed input chunk; pull next from source
    HAVE_MORE_OUTPUT,   // op has more output for the same input; call again
    FINISHED,           // op is done forever; tear down pipeline
    BLOCKED,            // RESERVED, asserts in phase 1 (see §11)
};

enum class SourceResultType  : uint8_t { HAVE_MORE_OUTPUT, FINISHED, BLOCKED };
enum class SinkResultType    : uint8_t { NEED_MORE_INPUT, FINISHED, BLOCKED };
enum class SinkCombineResultType : uint8_t { FINISHED, BLOCKED };
enum class SinkFinalizeType  : uint8_t { READY, NO_OUTPUT_POSSIBLE, BLOCKED };
```

In phase 1, any `BLOCKED` returned anywhere triggers `Assert(false)` + ereport(ERROR). The enum value exists so phase 2 doesn't break the ABI.

### 3.2 State split

| Role                | Owner          | Synchronization   | Lifetime                        |
| ------------------- | -------------- | ----------------- | ------------------------------- |
| `LocalSourceState`  | one per worker | none (private)    | per-pipeline-run, MemoryContext |
| `GlobalSourceState` | one per query  | atomics / LWLocks | DSM segment, query lifetime     |
| `LocalSinkState`    | one per worker | none              | per-pipeline-run, MemoryContext |
| `GlobalSinkState`   | one per query  | atomics / LWLocks | DSM + DSA, query lifetime       |
| `OperatorState`     | one per worker | none              | per-pipeline-run                |

Leader-as-worker uses **the same** `LocalSinkState` path; it Combines into the same `GlobalSinkState`.

### 3.3 Interfaces

```cpp
class Source {
public:
    virtual ~Source() = default;
    virtual std::unique_ptr<LocalSourceState>  GetLocalSourceState (ExecCtx&, GlobalSourceState&) = 0;
    virtual std::unique_ptr<GlobalSourceState> GetGlobalSourceState(ExecCtx&) = 0;
    virtual SourceResultType GetData(ExecCtx&, DataChunk& out, OperatorSourceInput&) = 0;
    virtual bool ParallelSource() const { return false; }   // mirrors DuckDB
};

class Operator {
public:
    virtual ~Operator() = default;
    virtual std::unique_ptr<OperatorState> GetOperatorState(ExecCtx&) = 0;
    virtual OperatorResultType Execute(ExecCtx&, DataChunk& in, DataChunk& out, OperatorState&) = 0;
    virtual bool ParallelOperator() const { return true; }
};

class Sink {
public:
    virtual ~Sink() = default;
    virtual std::unique_ptr<LocalSinkState>  GetLocalSinkState (ExecCtx&, GlobalSinkState&) = 0;
    virtual std::unique_ptr<GlobalSinkState> GetGlobalSinkState(ExecCtx&) = 0;
    virtual SinkResultType         Sink    (ExecCtx&, DataChunk& in, OperatorSinkInput&) = 0;
    virtual SinkCombineResultType  Combine (ExecCtx&, OperatorSinkCombineInput&) = 0;
    virtual SinkFinalizeType       Finalize(ExecCtx&, GlobalSinkState&) = 0;   // leader-only
    virtual bool ParallelSink() const { return true; }
};
```

`ExecCtx` is a thin struct that bundles `MemoryContext`, optional `dsa_area*`, the `VecPlanState*`, and the worker index (or `LEADER_WORKER_INDEX`).

---

## 4. Generic Pipeline Executor

One executor for every pipeline. Adapted from `duckdb/src/parallel/pipeline_executor.cpp:189-272`. Kept synchronous (no BLOCKED fast-path).

```cpp
struct WorkerPipelineExecutor {
    Source*  src;
    std::vector<Operator*> ops;     // may be empty
    Sink*    sink;                  // may be null for the terminal pipeline (leader collects)
    std::vector<DataChunk> intermediates;   // one per op boundary
    std::stack<size_t> in_process;          // index of op currently producing HAVE_MORE_OUTPUT

    // Run until source exhausted OR max_chunks chunks pushed to sink.
    // Returns true iff source signaled FINISHED.
    bool Execute(ExecCtx& ctx, LocalSourceState& lsrc, LocalSinkState* lsink, size_t max_chunks);
};
```

Loop sketch (synchronous variant):

```
loop:
    if in_process not empty:
        op_idx = in_process.top()
        run op[op_idx] on its cached input -> intermediates[op_idx+1]
        switch result:
            HAVE_MORE_OUTPUT: keep op_idx on stack, fall through to push down
            NEED_MORE_INPUT:  in_process.pop(), fall through to push down
            FINISHED:         return src_finished=false (tear down)
            BLOCKED:          assert(false)
    else:
        src->GetData(intermediates[0])
        if SourceResultType::FINISHED: break
        push intermediates[0] through ops[0..n-1] left-to-right,
            recording any HAVE_MORE_OUTPUT into in_process

    if no ops, sink directly receives intermediates[0]
    else sink receives intermediates[ops.size()]
    sink->Sink(...) ; if SinkResultType::FINISHED: break
    if ++pushed == max_chunks: return false
```

Edge cases:
- **No operators**: `intermediates.size() == 1`, source feeds sink directly. Used by SeqScan→AggSink and partition-drain pipelines.
- **No sink**: leader-collect terminal pipeline; loop returns chunks to caller via `out_chunk` parameter (used during `ExecutorRun` non-parallel hand-back).
- **Empty source**: returns immediately, sink->Combine still called by caller after all workers finish.

---

## 5. Leader Contract Mapping

The 14 leader contracts identified in `runtime_execution.cpp` (1970 lines) must continue to function. New mapping:

| #   | Legacy contract                                                                                                                       | New home                                                                                                                                                                                          |
| --- | ------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | `VOLVEC_PARALLEL_KEY_CONTROL` (atomic next_block, mode flags)                                                                         | `GlobalSourceState` for `SeqScanSource`                                                                                                                                                           |
| 2   | Serialized plannedstmt / query_text / param_exec in DSM                                                                               | Unchanged. Worker boot still calls `ExecParallelGetReceiver`-equivalent, then constructs `Source/Op/Sink` from the per-pipeline descriptor.                                                       |
| 3   | SharedFileSet for partials + sort runs                                                                                                | `GlobalSinkState` for file-backed sinks (`HashTableBuildSink` file path, `SortRunSink`)                                                                                                           |
| 4   | DSA area + dsa_pointer offsets (hash bridge, partition tables)                                                                        | `GlobalSinkState::Finalize` writes the offsets; downstream pipeline's `GlobalSourceState` reads them.                                                                                             |
| 5   | Per-worker partial slots (`QUERY_AGG_PARTIALS`, `QUERY_HASH_PARTIALS`, `QUERY_SORT_RUNS`)                                             | `LocalSinkState` allocates into its slot; `Combine` merges into `GlobalSinkState`. Slot indexed by worker id.                                                                                     |
| 6   | Per-pipeline atomics (`remaining_dependencies`, `completed`, `hash_bridge_ready`, `build_finalized`, `probe_started`, `bridge_ready`) | A `PipelineDescriptor` struct in DSM, fields owned by each pipeline's `GlobalSinkState`. `Finalize()` flips the atomics.                                                                          |
| 7   | `VOLVEC_PARALLEL_KEY_QUERY_TASKS` (SourceMorsel / BridgeFinalize / HashBuildPartition / HashPartitionFinalize)                        | Replaced by **pipeline tasks**: each pipeline has N "slice" tasks + 1 "finalize" task. Task only carries `(pipeline_id, slice_id_or_FINALIZE)`.                                                   |
| 8   | Dependencies / successors / `WaitForParallelWorkersToFinish`                                                                          | Each `Pipeline` carries `vector<PipelineId> depends_on`. Scheduler decrements `remaining_dependencies` on `Finalize() == READY`.                                                                  |
| 9   | Merge worker partials (file vs in-memory)                                                                                             | Two `Sink` subclasses: `HashTableBuildSinkInMemory` and `HashTableBuildSinkFileBacked`. Both implement `Combine` + `Finalize`; choice happens in `runtime_lowering` based on existing heuristics. |
| 10  | `VOLVEC_PARALLEL_KEY_HASH_BRIDGE` published for probe pipeline                                                                        | `SharedHashBridgeSink::Finalize` writes; `HashJoinProbeOp::GetOperatorState` reads.                                                                                                               |
| 11  | Leader-as-worker (`pg_volvec_parallel_leader_participation`)                                                                          | Same code path; leader runs `WorkerPipelineExecutor::Execute` against its own `LocalSinkState`.                                                                                                   |
| 12  | Heuristics (Generic vs HashProbe, file-backed preference, build-dominance skip)                                                       | Stays in `runtime_lowering.cpp` as **pipeline construction choices** (which Sink subclass, which Source subclass), not runtime branches.                                                          |
| 13  | Fallbacks (DSM full, no workers, build dominance, oversized bridge)                                                                   | `runtime_lowering.cpp` returns `nullptr` pipelines or a "leader-only" pipeline list; `runtime_execution.cpp` honors.                                                                              |
| 14  | Lifecycle (SharedFileSetDeleteAll, dsa_free, DestroyParallelContext)                                                                  | `Pipeline::~Pipeline` releases its sink/source globals; query teardown drives reverse-order destruction.                                                                                          |

---

## 6. Role → (Source, Operators, Sink) Mapping

Authoritative table. After phase 4, no code path may add a new entry to `ParallelPipelineRole` because the enum is gone.

| Legacy role                            | Source                                                                          | Operators                                                                          | Sink                                                                 |
| -------------------------------------- | ------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------- | -------------------------------------------------------------------- |
| `HashBuildSource`                      | `SeqScanSource` (block-range from atomic)                                       | optional `PartialAggOp` (rare; only when build side aggregates)                    | `HashTableBuildSink` (in-memory or file-backed variant)              |
| `HashBuildFinalize`                    | `HashBuildPartialReadSource` (enumerate per-worker slots)                       | `HashTableMergeOp` (`reserve_capacity` + `merge_*` + `finish_parallel_hash_build`) | `SharedHashBridgeSink` (write DSA pack, set `hash_bridge_ready=1`)   |
| `HashProbeSource`                      | `SeqScanSource`                                                                 | `HashJoinProbeOp`, optional `FilterOp`, optional `PartialAggOp`                    | `AggSink` or `MaterializeSink`                                       |
| `HashOuterSource`                      | `SeqScanSource`                                                                 | `HashJoinProbeOp(outer=true)`                                                      | same as above                                                        |
| `GenericSource`                        | `SeqScanSource`                                                                 | `FilterOp`, optional `PartialAggOp`                                                | `AggSink` or `MaterializeSink`                                       |
| `BuildPartitionSource`                 | `SeqScanSource`                                                                 | —                                                                                  | `RadixPartitionSink(side=BUILD)`                                     |
| `ProbePartitionSource`                 | `SeqScanSource`                                                                 | —                                                                                  | `RadixPartitionSink(side=PROBE)`                                     |
| `HashJoinPartitionSource`              | `PartitionPairSource` (one DSA build-partition + one DSA probe-partition by id) | `PerPartitionHashJoinOp`                                                           | `AggSink` or `MaterializeSink`                                       |
| `AggFinalize`                          | `AggPartialReadSource` (per-worker agg slots)                                   | `AggMergeOp`                                                                       | `MaterializeSink`                                                    |
| **Q3-style hybrid** (currently broken) | `PartitionPairSource`                                                           | `PerPartitionHashJoinOp`                                                           | **`AggSink`** ← the hybrid is just a new combination, not a new role |

Notes:
- `HashJoinProbeOp` with `HAVE_MORE_OUTPUT` semantics naturally handles multi-match (replaces `in_process_operators` stack usage). DuckDB's `physical_hash_join.cpp:716-781` is the reference implementation pattern.
- `PartialAggOp` is a streaming aggregate that emits into a per-worker partial; the **terminal** `AggSink` is what merges (`Combine`) into the global table during `Finalize`.

---

## 7. Pipeline Construction in `runtime_lowering.cpp`

Today the file has 6 legacy role-assignment sites at lines 1792, 1794, 1811, 2003, 2006, 2023. After refactor:

```
For each VecPlanState plan tree:
  1. Walk operators bottom-up, segment at "pipeline breakers" (Sink-shaped ops:
     hash-join-build, aggregate, sort, materialize).
  2. Each segment becomes one Pipeline { Source, [Op...], Sink }.
  3. Set Pipeline::depends_on edges (probe depends_on build-finalize, etc).
  4. For partitioned hash-join shapes, emit:
       PipelineA = SeqScan(build) -> RadixPartitionSink(BUILD)
       PipelineB = SeqScan(probe) -> RadixPartitionSink(PROBE)
       PipelineC = PartitionPairSource -> PerPartitionHashJoinOp -> {AggSink|MaterializeSink}
       deps: C.depends_on = {A, B}
```

Heuristics (Generic vs HashProbe, file-backed preference, build-dominance skip) become *which Sink subclass to instantiate*. They no longer affect the dispatcher.

---

## 8. Worker Boot & State Init

Today's two entrypoints survive with renamed responsibilities:

- `TryInitializeLocalParallelAggregateProcessState` → renamed `InitializeWorkerExecutionContext`. Still creates per-worker MemoryContext + EState + `ExecInitVecPlan` + binds VecAggState/VecHashJoinState pointers. Now additionally constructs the `LocalSourceState` / `OperatorState` / `LocalSinkState` for the assigned pipeline.
- `TryInitializeParallelMergeContext` → renamed `InitializeLeaderMergeContext`. Unchanged in spirit: leader rebinds against `query_state->vec_plan` without a fresh MemoryContext. Used when leader runs `Sink::Finalize`.
- `CleanupLocalParallelAggregateProcessStateOnExit` → kept as-is. Still calls `release_jit_resources_for_proc_exit()` on proc-exit to dodge recursive teardown.

These keep all existing helpers alive: `TryAttachSharedHashBridgePack`, `PublishQueryHashBridgePack`, `BuildQueryDependencyBridgePack`, `ReadQueryBridgePack`, `VecHashJoinState::*`, `VecAggState::*`. They're called *from inside the new Source/Op/Sink classes* rather than from the dispatcher.

---

## 9. Dispatcher After Refactor

`runtime_worker_main.cpp` shrinks from ~2046 lines (with one giant role switch) to roughly:

```cpp
void WorkerMain(dsm_segment* seg, shm_toc* toc) {
    ExecCtx ctx = BootWorker(seg, toc);                  // §8
    PipelineId pid = ClaimNextPipelineSlice(toc, ctx);   // task-queue pop
    while (pid != INVALID_PIPELINE_ID) {
        Pipeline& p = ctx.pipelines[pid];
        auto lsrc  = p.src->GetLocalSourceState (ctx, *p.global_src);
        auto lsink = p.sink ? p.sink->GetLocalSinkState(ctx, *p.global_sink) : nullptr;
        WorkerPipelineExecutor exec{p.src, p.ops, p.sink};
        exec.Execute(ctx, *lsrc, lsink.get(), /*max_chunks=*/SIZE_MAX);
        if (p.sink) p.sink->Combine(ctx, OperatorSinkCombineInput{*lsink, *p.global_sink});
        SignalSliceComplete(pid, ctx);
        pid = ClaimNextPipelineSlice(toc, ctx);
    }
}
```

Leader runs the same loop, plus calls `Sink::Finalize` on each sink whose pipeline reached "all slices complete + all dependencies satisfied". `Finalize == READY` decrements `remaining_dependencies` on dependent pipelines.

---

## 10. Phased Migration

Each phase ends green (`meson compile`, `meson test pg_volvec`, TPC-H sweep). No phase removes legacy code until the new code can fully replace it.

| Phase  | Scope                                                                                                                                                                                                       | Removed                                                                                                                                                                                                                                                                                | Risk   |
| ------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------ |
| **P0** | This document, reviewed & approved by user                                                                                                                                                                  | —                                                                                                                                                                                                                                                                                      | none   |
| **P1** | Add interfaces (`Source`, `Operator`, `Sink`, result enums, `WorkerPipelineExecutor`, `Pipeline`, `ExecCtx`). No behavior change yet. New types unused.                                                     | —                                                                                                                                                                                                                                                                                      | low    |
| **P2** | **Greenfield rebuild (B).** Implement `SeqScanSource`, `FilterOp`, `PartialAggOp`, `MaterializeSink`, `AggSink`, generic worker loop. Delete `runtime_lowering.cpp`, `runtime_worker_main.{cpp,inc}`, `runtime_worker_state.{cpp,inc}`, `runtime_execution.{cpp,inc}`. Reuse only DSM keys + ParallelContext + `ParallelAggregateSharedControl` morsel atomic. New bridge API names; rewire `bridge/execute.cpp`. **Workers-only execution** (leader orchestrates + merges + materializes; never executes pipeline tasks). Q1 + Q6 must pass; Q3-Q22 acceptably broken (re-landed in P3-P4). No GUC — no toggle, no fallback. | `runtime_lowering.cpp`, `runtime_worker_main.{cpp,inc}`, `runtime_worker_state.{cpp,inc}`, `runtime_execution.{cpp,inc}`, `ParallelPipelineRole` enum, role fields in `ParallelPipelineDesc`, all 7 legacy bridge entry points (`BuildParallelPipelinePlan`, `TryExecuteQuerySchedulerSkeleton`, `TryExecuteProcessParallelAggregate`, `TryExecuteLeaderOnlyParallelPlan`, `TryInitializeLeaderOnlyAggregateWorkerContext`, `BuildParallelSchedulerState`, `pg_volvec_parallel_worker_main`) replaced by new symbols (`PgVolvecPipelineLower`, `PgVolvecPipelineRun`, `pg_volvec_pipeline_worker_main`). | high   |
| **P3** | Implement `HashTableBuildSink` (both variants), `SharedHashBridgeSink`, `HashJoinProbeOp`, `HashBuildPartialReadSource`. Reroute hash-join plans (build + finalize + probe) through new path.               | —                                                                                                                                                                                                                                                                                      | high   |
| **P4** | Implement `RadixPartitionSink`, `PartitionPairSource`, `PerPartitionHashJoinOp`. Reroute partition-pair plans. **Q3-style hybrid is fixed here** because `AggSink` is now a free choice of terminal sink.   | —                                                                                                                                                                                                                                                                                      | high   |
| **P5** | Flip GUC default to `on`. Run full sweep. Address any regressions.                                                                                                                                          | —                                                                                                                                                                                                                                                                                      | medium |
| **P6** | Delete legacy dispatcher, role enum, role-name function, all 6 role-assignment sites in `runtime_lowering.cpp`.                                                                                             | `ParallelPipelineRole`, role-switch in `runtime_worker_main.cpp`, `ParallelPipelineRoleName`, `HashBuildSource`/`HashBuildFinalize`/`HashProbeSource`/`HashOuterSource`/`GenericSource`/`BuildPartitionSource`/`ProbePartitionSource`/`HashJoinPartitionSource`/`AggFinalize` branches | medium |
| **P7** | Delete the GUC. Done.                                                                                                                                                                                       | GUC                                                                                                                                                                                                                                                                                    | low    |

Estimated landed-LoC delta: **−2k to −3k** net (dispatcher shrinks more than new abstractions add).

---

## 11. Open Questions (please answer before P1 starts)

1. **`BLOCKED` in phase 1**: keep enum value, executor asserts on it. Phase 2+ optional. **(User pre-approved during design Q&A.)** ✅
2. **Leader-borrow DSA pack optimization**: today the leader can directly read another worker's DSA-published partial without a copy. Keep this as an `unsafe_borrow_partial()` method on `LocalSinkState`, or drop for simplicity? Keep as an unsafe_borrow_partial.
3. **Pipeline scheduling unit**: today task queue items are heterogeneous (SourceMorsel, BridgeFinalize, HashBuildPartition, HashPartitionFinalize). Proposed: homogeneous `(pipeline_id, slice_id | FINALIZE)`. Acceptable, or is there a workload where heterogeneous tasks were exploited for priority?Accept the homogeneous.
4. **`PartialAggOp`** placement: model it as a regular `Operator` with `HAVE_MORE_OUTPUT` for spill, or always inline into the upstream sink? DuckDB uses the former; current pg_volvec uses the latter. Choose the duckdb style.
5. **`HashJoinProbeOp` multi-match** representation: use `OperatorState`-internal cursor + `HAVE_MORE_OUTPUT` (DuckDB style), or expand all matches per-call as today? The former is cleaner for outer/anti joins. Choose the duckdb style.
6. **Failure of `Sink::Finalize`**: if `READY` cannot be reached (e.g. DSA exhaustion on bridge publish), do we propagate `NO_OUTPUT_POSSIBLE` to dependents (they cancel) or fall back to leader-only execution of the dependent pipeline? Choose the former.
7. **GUC name**: `pg_volvec_use_pipeline_executor` — acceptable, or prefer `pg_volvec_pipeline_v2` / something shorter? Accept the former.
8. **Pipeline ID width**: `uint16` (max 65k pipelines per query, fits one DSM slot) or `uint32`? Today's plans never exceed ~32 pipelines; uint16 saves DSM bytes per task. Accept the uint16.

---

## 12. Out-of-Scope Reminders

- Pre-existing failures stay broken until separately addressed:
  - Q4/Q18/Q20 robin-hood 1GB allocator
  - Q11 worker init
  - Q22 DSA 1.3GB / crash
- `parallel_experimental_hash_pipeline` and `disable_jit_for_parallel_worker` GUCs are **not** rewired by this refactor; they continue to gate experimental code paths in unrelated files.
- `tpch` database tables are not to be modified (user constraint, multiple sessions).

---

## 13. Verification Plan

Per phase:
1. `CCACHE_DISABLE=1 PATH=/opt/homebrew/bin:$PATH meson compile -C build pg_volvec`
2. `CCACHE_DISABLE=1 PATH=/opt/homebrew/bin:$PATH meson install -C build --only-changed`
3. `./installed/bin/pg_ctl -D ~/data/pg_tpch restart -m fast -l ~/data/pg_tpch/logfile`
4. `meson test -C build pg_volvec`
5. `contrib/pg_volvec/scripts/bench_supported_twice.sh` against 10G TPC-H
6. Diff TSV against `tpch_supported_twice_20260422_183421.tsv`. Acceptance: no new errors, no Q* numerical drift > 1e-6 vs baseline (Q14 must remain `16.6475949416151`).

A phase is **not done** until all six steps pass.

---

## 14. References

- DuckDB commit `f9d17f0eb7a6f90586dbf08910910f766eb1b29c`:
  - `src/include/duckdb/execution/physical_operator.hpp`
  - `src/include/duckdb/execution/physical_operator_states.hpp`
  - `src/include/duckdb/parallel/pipeline.hpp`
  - `src/include/duckdb/parallel/pipeline_executor.hpp`
  - `src/parallel/pipeline_executor.cpp:189-272` (Execute main loop)
  - `src/parallel/pipeline_executor.cpp:405-478` (operator chain)
  - `src/execution/operator/join/physical_hash_join.cpp:487-534`, `:716-781`
  - `src/execution/operator/join/physical_join.cpp:31-83` (BuildJoinPipelines)
- Existing pg_volvec files (research notes embedded in 2026-04-22 handoff):
  - `runtime_worker_main.cpp` (dispatcher per-role behavior)
  - `runtime_execution.cpp` (14 leader contracts)
  - `runtime_worker_state.cpp` (worker init paths)
  - `parallel_runtime_internal.hpp` (DSM layout, atomics)
  - `runtime_lowering.cpp:1770-2069` (6 role-assignment sites)
  - `plan_state.hpp:92-110` (role enum to be deleted)
  - `bridge/execute.cpp:63-89, 386-431` (role name fn, routing predicate)
- Sibling design docs:
  - `GLOBAL_PARTITION_DESIGN.md`
  - `AGENTS.md` (parallel/)
  - `../../AGENTS.md` (pg_volvec/)

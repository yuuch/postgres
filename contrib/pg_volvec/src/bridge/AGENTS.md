# bridge/ — PostgreSQL Hook Integration & Plan Dispatch

5 files. The C/C++ boundary: PostgreSQL ExecutorStart/Run/Finish/End hooks → `pg_volvec` admission → either pipeline path or PostgreSQL fallback.

## OVERVIEW

`pg_volvec` does NOT replace PostgreSQL's planner. It hooks the executor entry points, inspects the planned tree, and for supported plan shapes builds a `VecPlanState` tree, then dispatches execution to either the pipeline runtime (parallel) or the serial vector executor. Per-query state lives in a backend-local HTAB keyed by `QueryDesc*`.

## WHERE TO LOOK

| Task | File | Notes |
|------|------|-------|
| `_PG_init`, GUCs, hook registration | `pg_volvec.c` | All 10 GUCs defined here; hooks installed at module load |
| Per-query state struct + HTAB | `state.h`, `state.c` | `PgVolVecQueryState` keyed by `QueryDesc*`; admission filter `plan_uses_supported_relations` |
| Plan init / dispatch / materialize | `execute.h`, `execute.cpp` | `pg_volvec_initialize_plan`, `pg_volvec_execute_query`, slot materialization from `DataChunk` via `VecOutputColMeta` |

## CONVENTIONS

- **C/C++ split**: `pg_volvec.c` and `state.c` are pure C (PG headers, hooks). `execute.cpp` is C++ and bridges into the engine. Headers wrap declarations in `extern "C"` when included from C++.
- **Naming**: all C-visible symbols use `pg_volvec_` prefix. C++ entry uses `namespace pg_volvec`.
- **Hook discipline**: `ExecutorStart_hook`, `ExecutorRun_hook`, `ExecutorFinish_hook`, `ExecutorEnd_hook` chained — always call previous hook (or standard) first/last per PG convention.
- **Worker bypass**: hooks early-out via `IsParallelWorker()`. Workers participate via the pipeline runtime's own bgworker entry, not via PG's executor hooks.
- **Per-query state**: created in `ExecutorStart_hook`, looked up by `QueryDesc*` pointer, destroyed in `ExecutorEnd_hook`. HTAB lives in TopMemoryContext; per-query data in a child `MemoryContext` that's reset on End.
- **Admission**: `plan_uses_supported_relations` walks the plan tree before init. If it rejects, `state.parallel_plan = NULL` and execution falls through to standard PostgreSQL.
- **Parallel sentinel**: when `pg_volvec.parallel=on`, `parallel_plan` and `parallel_scheduler` are set to non-null void* sentinels; `execute.cpp` reads them to decide pipeline vs serial dispatch. Real pointers are owned by the pipeline runtime.

## GUCs (defined in `pg_volvec.c::_PG_init`)

| GUC | Default | Range | Purpose |
|-----|---------|-------|---------|
| `pg_volvec.enabled` | `true` | bool | Master switch; off → all hooks pass through |
| `pg_volvec.trace_hooks` | `false` | bool | Log every hook entry/exit |
| `pg_volvec.trace_execution_path` | `false` | bool | Log dispatch decisions (serial vs pipeline, fallback reasons) |
| `pg_volvec.jit_deform` | `true` | bool | Enable LLVM tuple-deform JIT |
| `pg_volvec.parallel` | `false` | bool | Enable pipeline runtime (otherwise serial) |
| `pg_volvec.parallel_max_workers` | `4` | 0–1024 | Max bgworkers per query |
| `pg_volvec.parallel_morsel_nblocks` | `512` | 1–1048576 | Blocks per morsel claim |
| `pg_volvec.parallel_min_relation_blocks` | `1024` | int | Min relation size to consider parallel |
| `pg_volvec.parallel_leader_participation` | `true` | bool | Leader runs pipeline alongside workers |
| `pg_volvec.parallel_experimental_hash_pipeline` | `false` | bool | Reserved; no effect in greenfield (HashJoin removed) |

## ANTI-PATTERNS

- **Do NOT call standard executor + then `pg_volvec` for the same QueryDesc.** Dispatch is exclusive: either we own execution for this `QueryDesc` or we passthrough.
- **Do NOT allocate per-query memory in `TopMemoryContext`.** Use the per-query child context owned by `PgVolVecQueryState`; it is reset on `ExecutorEnd`.
- **Do NOT skip `IsParallelWorker()` early-out.** Bgworkers reach hooks too; double-dispatch into the pipeline runtime corrupts state.
- **Do NOT `elog(ERROR)` from inside a hook without a `PG_TRY/PG_CATCH` around the engine call.** C++ destructors must run; otherwise DSA/JIT contexts leak.
- **Do NOT touch `parallel_plan` / `parallel_scheduler` void* fields outside the pipeline runtime.** They are owned by `pipeline/` and the bridge only treats them as opaque sentinels.
- **Do NOT add new GUCs without registering via `DefineCustom*Variable` in `_PG_init`** and documenting in this file.
- **Do NOT chain hooks in the wrong order.** Standard pattern: save previous, install ours, call previous (or standard) at the right point. See existing wrappers as the reference.

## EXECUTION FLOW

1. **`_PG_init`** (`pg_volvec.c`): defines all GUCs, saves previous hook pointers, installs `pg_volvec_ExecutorStart/Run/Finish/End`.
2. **`ExecutorStart_hook`**:
   - If `IsParallelWorker()` or `!enabled` → call previous hook, return.
   - `state_create(qd)` → HTAB entry + child MemoryContext.
   - `pg_volvec_initialize_plan(qd, state)`:
     - `plan_uses_supported_relations` admission check.
     - On accept: `ExecInitVecPlan(qd->plannedstmt->planTree)` → `VecPlanState` tree.
     - If `pg_volvec.parallel=on`: set `parallel_plan` + `parallel_scheduler` sentinels.
   - Call previous `ExecutorStart`.
3. **`ExecutorRun_hook`**:
   - If state present and `vec_plan` non-null:
     - `pg_volvec_execute_query(qd, state, dest)`.
     - If `parallel_plan` sentinel set → `pipeline::PgvolvecPipelineRun(qd, state, &reason)`. On `failure_reason` non-null, log and fall through to serial.
     - Else → serial vector executor.
     - For each output `DataChunk`: walk `VecOutputColMeta`, materialize per-column (`Double`, `NumericScaledInt64`, `NumericAvgPair`) into `TupleTableSlot`, push to `DestReceiver`.
   - Else: call previous hook (passthrough).
4. **`ExecutorFinish_hook`**: call previous (no-op for us today).
5. **`ExecutorEnd_hook`**: `state_destroy(qd)` (resets per-query MemoryContext, removes HTAB entry), then previous hook.

## NOTES

- `PgVolVecQueryState` carries: `MemoryContext mcxt`, `VecPlanState *vec_plan`, `void *parallel_plan` (sentinel), `void *parallel_scheduler` (sentinel). Real parallel artifacts are owned by `pipeline/`.
- HTAB uses `QueryDesc*` pointer identity as key — relies on PG not reusing the pointer across overlapping queries in the same backend (true for nested executors because they get distinct `QueryDesc`).
- Slot materialization is the hot path on the result side; output column metadata (`VecOutputColMeta`) is built once in `pg_volvec_initialize_plan` and reused per chunk.
- All admission failures should produce a clean fallback to PostgreSQL execution — never `ereport(ERROR)` for "unsupported plan".
- The `parallel_experimental_hash_pipeline` GUC is a leftover from the pre-greenfield era; HashJoin is removed and this GUC currently has no effect. Kept registered to avoid breaking existing config files.

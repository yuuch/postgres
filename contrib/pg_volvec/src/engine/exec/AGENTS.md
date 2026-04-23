# exec/ — Vector Operator Implementations

19 files (10 `.cpp` + 9 `.hpp`), ~7.2k lines. Per-operator translation units plus shared init/common helpers and per-query state types.

## OVERVIEW

Serial vector executor: `VecPlanState` tree built from PostgreSQL's planner output, walked recursively via per-operator `Next(DataChunk &)`. The pipeline runtime under `parallel/pipeline/` consumes the same `VecPlanState` tree but drives execution differently (push pipeline). Shapes built today are limited to what Q1/Q6 need: SeqScan, Filter, grouped/ungrouped Agg, Project, Limit, Sort.

HashJoin and its parallel/lookup partners (`hash_join.cpp`, `hash_join_parallel.cpp`, `hash_join_lookup.cpp`) have been **removed** in the greenfield rewrite. Do not look for them.

## WHERE TO LOOK

| Task | File | Notes |
|------|------|-------|
| Plan admission + dispatch tree build | `executor_init.cpp` | `init_vec_plan_state`, walks PG plan, instantiates `VecXxxState` |
| Shared exec helpers / materialization | `executor_common.cpp` | Slot ↔ DataChunk, deform dispatch, common scan utilities |
| Per-query state struct | `query_state.hpp` | `VecQueryState`, parallel sentinels (`parallel_plan`, `parallel_scheduler` void*) |
| Plan-state base + per-op state types | `plan_state.hpp` | `VecPlanState`, `VecXxxState` declarations |
| Internal shared types | `internal.hpp` | Cross-operator helpers (HashJoin-only structs/protos already stripped) |
| Aggregation (grouped + ungrouped) | `agg.cpp`, `agg.hpp` | Numeric widening, partial-state interface for parallel pipeline |
| Aggregation plan helpers | `agg_plan.cpp` | `BuildAggWithOptionalProject` + 6 helpers (relocated from deleted `hash_join.cpp` in P2.13d) |
| Sequential scan | `seq_scan.cpp`, `seq_scan.hpp` | Page-wise deform; bypasses synchronized scan |
| Filter | `filter.cpp`, `filter.hpp` | SelectionVector-based |
| Project | `project.cpp`, `project.hpp` | Expression-driven column projection |
| Limit | `limit.cpp`, `limit.hpp` | Constant-count only |
| Sort | `sort.cpp`, `sort.hpp` | Single-run in-memory only |
| Hash probe / lookup helpers | `lookup.hpp` | Header still present; consumers may be unreachable post-HashJoin removal |

## CONVENTIONS

- **State class pattern**: each operator defines `VecXxxState : public VecPlanState`. Declared in `plan_state.hpp`, defined in the operator's `.cpp/.hpp` pair.
- **`Next(DataChunk &chunk)`**: every operator implements this and returns `bool` (true = produced rows; false = exhausted).
- **Registration**: new operators wire into `executor_init.cpp`'s plan-walk switch. No registry; admission is a switch on `nodeTag`.
- **Headers**: each operator owns a `.cpp` + `.hpp`. Cross-operator types in `internal.hpp`; per-query types in `query_state.hpp`.
- **Parallel sentinels**: `query_state.hpp` exposes `void *parallel_plan` and `void *parallel_scheduler`. The `bridge/` sets them; only `parallel/pipeline/` derefs them. `exec/` treats them as opaque.
- **Numeric semantics**: `NUMERIC(15,2)` runs as scaled `int64` with widened accumulators in `agg`. `Wide128` is interpreter-only (no JIT) — see `expr.cpp`.

## ANTI-PATTERNS

- **Do NOT reintroduce HashJoin**, `VecHashJoinState`, or any of the deleted helpers (`BuildJoinKeyEqualityProgram`, `BuildHashKeyExtractor`, `BuildLookupFilterState`, `BuildLookupProjectStateMultiKey`, etc.). Greenfield is Plan B; HashJoin is gone.
- **Do NOT add new operators without admission in `executor_init.cpp`.** Silent acceptance corrupts plan walking.
- **Do NOT pass non-equality keys into `lookup.hpp`.** It only models simple equality; remaining callers (if any) assume that.
- **Do NOT enable JIT for Wide128 numeric expressions or numeric division.** Interpreter-only for correctness.
- **Do NOT expect Sort to handle numeric average keys.** Will `ereport(ERROR)`, not fallback.
- **Do NOT allocate operator state with raw `new`/`malloc`.** Use the per-query MemoryContext (via `PgMemoryContextAllocator` for STL containers, palloc for POD).
- **Do NOT touch `parallel_plan` / `parallel_scheduler` void* fields.** Read them only as opacity sentinels; `parallel/pipeline/` owns the real types.

## NOTES

- `agg_plan.cpp` was created in P2.13d to host helpers relocated from the deleted `hash_join.cpp`. Logically belongs with `agg.cpp` but kept separate to keep diffs reviewable; safe to merge later.
- `lookup.hpp` may now be dead-code reachable only through unreachable paths — pending cleanup.
- `executor.cpp` at `src/engine/executor.cpp` is **not** part of `exec/` and is not built; ignore it.
- Only Q1 and Q6 are exercised by the Meson regress (`smoke`, `q1`, `q6`). Other queries that previously worked (Q2–Q22) are not goals of the greenfield phase.

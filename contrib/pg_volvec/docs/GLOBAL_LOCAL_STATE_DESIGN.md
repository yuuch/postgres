# Unified GlobalState / LocalState Design for pg_volvec Pipeline

**Status:** Draft, awaiting review
**Author:** orchestrator
**Date:** 2026-04-24
**Scope:** Formalize the per-operator GlobalState/LocalState abstraction for cross-process pipeline state. In-scope: AggPartial (existing, to be cleaned up) + Sort (new). HashJoin: out of scope (Greenfield Plan B; HashJoin removed).
**DuckDB reference SHA:** `5af191ce52b9869e77a9c0faa1b15e9c273d889e`
**pg_volvec reference SHA:** `e00e99dd38f` + P3-0 follow-up edits
**Companion plan:** `.sisyphus/plans/pipeline-port-plan.md` §15 (integrates this design into P3a–P3e phasing)

---

## 0. TL;DR

The pipeline runtime **already has** the DuckDB-shape `Source`/`Sink`/`Operator` interface and **already has** working concrete subclasses (`GlobalSeqScanState`, `LocalSeqScanState`, `AggGlobalSinkState`, `AggLocalSinkState`). What it lacks is:

1. **Per-pipeline ownership of cross-process state.** Today everything pipelines need lives in one global `PipelineSharedControl` + one `ParallelAggPartialState[N]` array, both keyed off fixed DSM keys. There is no notion of "this state belongs to *that* pipeline's sink."
2. **A Sort sink/source.** Q1 currently runs Sort serially after Agg in PostgreSQL's executor. There is **no** pipeline-side Sort.
3. **A canonical lifecycle contract.** When is `Get*State` called? Who owns the returned `unique_ptr`? When does Combine fire? Where do DSA buffers get freed? None of this is documented.
4. **Async/blocking machinery (`StateWithBlockableTasks`, `MaxThreads()`, `SinkFinalizeType`).** Stubbed in enums (`BLOCKED`, `READY`, `NO_OUTPUT_POSSIBLE`) but no enforcement; `BLOCKED` currently `ereport(ERROR)`s.

This design closes (1)–(3) for Q1+Q6. (4) is intentionally deferred to P3e.

---

## 1. Goals & Non-Goals

### 1.1 Goals

- **G1**: Define one canonical state-base hierarchy (`GlobalSinkState` / `LocalSinkState` / `GlobalSourceState` / `LocalSourceState` / `GlobalOperatorState` / `OperatorState`) that mirrors DuckDB's `physical_operator_states.hpp` shape closely enough that future ports (Sort, eventually HashJoin) drop in mechanically.
- **G2**: Make every cross-process state item **owned by exactly one** state object — no more orphaned fields in `PipelineSharedControl`.
- **G3**: Add `OrderGlobalSinkState` / `OrderLocalSinkState` for Q1's Sort phase, parallel-merge-sort using DSM partial slots + optional `SharedFileSet` spill (mirrors AggPartial spill).
- **G4**: Define ownership and lifecycle precisely enough that `MemoryContext` cleanup, DSA `dsa_free`, `before_shmem_exit` callbacks, and `PG_CATCH` re-entrancy are **provably leak-free**.
- **G5**: Migration is **incremental** — every step compiles, every step keeps Q6 + Q1 passing, no big-bang rewrite.

### 1.2 Non-Goals

- **NG1**: HashJoin. Greenfield Plan B removed it; do not design for it. (When/if it returns, the abstraction defined here is what it must extend.)
- **NG2**: TPC-H Q2–Q22. Active scope is Q1+Q6 only.
- **NG3**: Async/blocking task machinery (`StateWithBlockableTasks`, real `BLOCKED` propagation, `PipelineEvent` ordering, distinct-aggregate finalize tasks). Deferred to P3e.
- **NG4**: Multi-MetaPipeline DAGs (joins build multiple meta-pipelines that depend on each other). Q1+Q6 are single-MetaPipeline, single-source.
- **NG5**: Distributed `FinalAgg` (DuckDB's hash-aggregate "finalize as new pipeline" pattern). Q1+Q6 finalize fits in leader memory.

---

## 2. The Gap (Why This Design Is Needed)

### 2.1 What's already in place (don't redesign)

| Component | File:line | Status |
|---|---|---|
| `GlobalSinkState` base | `sink.hpp:16-19` | Empty shell (just virtual dtor) |
| `LocalSinkState` base | `sink.hpp:21-31` | Has `unsafe_borrow_partial(int)` virtual |
| `GlobalSourceState` base | `source.hpp:17-20` | Empty shell |
| `LocalSourceState` base | `source.hpp:22-25` | Empty shell |
| `OperatorState` base | `operator.hpp:16-19` | Empty shell |
| `Sink` interface | `sink.hpp:43-65` | `GetGlobal/LocalSinkState`, `SinkChunk`, `Combine`, `Finalize`, `ParallelSink()` |
| `Source` interface | `source.hpp:32-46` | `GetGlobal/LocalSourceState`, `GetData`, `ParallelSource()` |
| `Operator` interface | `operator.hpp:21-32` | `GetOperatorState`, `Execute`, `ParallelOperator()` |
| `Pipeline` aggregate | `pipeline.hpp:20-29` | Holds `global_src` + `global_sink` pointers (raw, not owned) |
| `ExecCtx` | `types.hpp:56-61` | Carries `mcxt`, `dsa`, `vec_plan`, `worker_index` |
| `GlobalSeqScanState`/`LocalSeqScanState` | `seq_scan_source.hpp:26-38` | Working subclasses |
| `AggGlobalSinkState`/`AggLocalSinkState` | `agg_sink.hpp:80-102` | Working subclasses |

### 2.2 What's broken / leaky (this design fixes)

| Issue | Where | Why it must change |
|---|---|---|
| **G1 — Pipeline-agnostic control fields** | `PipelineSharedControl` (`dsm_control.hpp:29-40`) holds `morsel_nblocks`, `total_blocks`, `source_scan_relid`, `source_scan_plan_node_id`, `agg_plan_node_id`, `partial_slot_count`, `next_block`. With multiple pipelines, *which* source's `next_block`? *Which* sink's `partial_slot_count`? | Each pipeline must own its source/sink state independently of others. |
| **G2 — DSM keys fixed at 8 hardcoded slots** | `dsm_control.hpp:13-21` (CONTROL, PLANNEDSTMT, QUERY_TEXT, PARTIALS, SOURCE_PSCAN, PARTIAL_FILESET, PARAM_EXEC, DSA) | Two pipelines need two `PARTIALS`, two `SOURCE_PSCAN`, two `PARTIAL_FILESET`. Cannot collide on fixed keys. |
| **G3 — Raw pointers in `Pipeline`** | `pipeline.hpp:27-28`: `GlobalSourceState *global_src; GlobalSinkState *global_sink;` (raw, no ownership) | If pipeline lifetime != state lifetime, this leaks or UAFs. Spec ownership now. |
| **G4 — `SetSharedSlots` injection** | `agg_sink.hpp:48-55`: `AggSink::SetSharedSlots(slots, num_slots, fileset)` is a pre-allocated DSM pointer set by the lowering layer | DuckDB allocates inside `GetGlobalSinkState(ctx)`. We currently can't because `ctx.dsa` came too late. P3-0 fixed `ExecCtx::dsa`; we can now move allocation. |
| **G5 — `worker_agg` wired manually** | `agg_sink.cpp` Combine assumes `lstate.worker_agg` is the worker's `VecAggState`. This handle is set by P3 lowering, not by `GetLocalSinkState`. | `GetLocalSinkState` should be the single point that constructs/binds local state. |
| **G6 — Dead key** | `PIPELINE_DSM_KEY_PARAM_EXEC` defined at `dsm_control.hpp:20`; never `shm_toc_insert`'d, never `shm_toc_lookup`'d | Delete in P3a. |
| **G7 — No Sort sink/source** | grep "Sort" under `pipeline/` returns 0. Q1 Sort runs serially in PG executor. | Add `OrderGlobalSinkState`/`OrderLocalSinkState` + `OrderSink`. |

---

## 3. Canonical State Hierarchy

Mirrors DuckDB `src/include/duckdb/execution/physical_operator_states.hpp` at `5af191ce…`. Field set is **strict subset** (HashJoin/distinct/blocking-task fields omitted per non-goals).

### 3.1 Base classes (header: `parallel/pipeline/state_base.hpp` — NEW)

```cpp
namespace pg_volvec::pipeline {

/* === Source side === */

class GlobalSourceState {
public:
    virtual ~GlobalSourceState() = default;

    /* DuckDB GlobalSourceState::MaxThreads. Caps how many workers can usefully
     * pull from this source. Default 1 (serial source). Parallel sources override. */
    virtual idx_t MaxThreads() const { return 1; }
};

class LocalSourceState {
public:
    virtual ~LocalSourceState() = default;
};

/* === Operator side === */

class GlobalOperatorState {
public:
    virtual ~GlobalOperatorState() = default;
    virtual idx_t MaxThreads(idx_t source_max_threads) const { return source_max_threads; }
};

class OperatorState {
public:
    virtual ~OperatorState() = default;

    /* DuckDB OperatorState::Finalize hook for per-operator post-pipeline cleanup.
     * Default no-op. Used today only by `PartialAggOp` (currently flushes its
     * VecAggState via the Combine() path on the sink side; if/when an operator
     * needs its own teardown, override here). */
    virtual void Finalize(ExecCtx &ctx) { (void) ctx; }
};

/* === Sink side === */

class GlobalSinkState {
public:
    GlobalSinkState() : finalize_state(SinkFinalizeType::READY) {}
    virtual ~GlobalSinkState() = default;

    /* Sink finalize result; mirrors DuckDB. Today set by Finalize() return value;
     * P3e will allow async transitions BLOCKED → READY. */
    SinkFinalizeType finalize_state;

    virtual idx_t MaxThreads(idx_t source_max_threads) const { return source_max_threads; }
};

class LocalSinkState {
public:
    virtual ~LocalSinkState() = default;

    /* Optional zero-copy access to a peer worker's published partial. Default
     * nullptr (= caller must use the sink-specific Combine path). Override only
     * when partial layout is safe for cross-worker borrow (rare; Agg today goes
     * through DSM array directly via gstate, not via this hook). */
    virtual void *unsafe_borrow_partial(int /*worker_index*/) { return nullptr; }
};

}  // namespace pg_volvec::pipeline
```

### 3.2 Differences from current code

| Current | New | Why |
|---|---|---|
| `GlobalSinkState` empty (`sink.hpp:16`) | Add `finalize_state` field + `MaxThreads()` virtual | Match DuckDB; needed for P3e BLOCKED handling and parallel-cap negotiation. |
| `GlobalSourceState` empty (`source.hpp:17`) | Add `MaxThreads()` virtual | Future-proof for partial admission of more parallelism than blocks support. |
| `OperatorState` (`operator.hpp:16`) | Add `Finalize(ExecCtx&)` virtual no-op | DuckDB shape; cheap to add now. |
| No `GlobalOperatorState` | Add it | DuckDB has it; some future operators (e.g. table-function-as-operator) need shared op state. |
| Bases live in `source.hpp`/`sink.hpp`/`operator.hpp` | Move to new `state_base.hpp`; `source.hpp`/`sink.hpp`/`operator.hpp` `#include` it | Avoid include-order cycles when concrete states want to reference each other. |

### 3.3 Naming

| DuckDB | pg_volvec |
|---|---|
| `OperatorState` | `OperatorState` (same) |
| `GlobalOperatorState` | `GlobalOperatorState` (same) |
| `LocalSourceState` | `LocalSourceState` (same) |
| `GlobalSourceState` | `GlobalSourceState` (same) |
| `LocalSinkState` | `LocalSinkState` (same) |
| `GlobalSinkState` | `GlobalSinkState` (same) |
| Operator-specific subclass: `HashAggregateGlobalSinkState` | Subclass: `AggGlobalSinkState` (existing) |
| `HashAggregateLocalSinkState` | `AggLocalSinkState` (existing) |
| `OrderGlobalSinkState` | `SortGlobalSinkState` (NEW; "Sort" matches our `VecSortState`) |
| `OrderLocalSinkState` | `SortLocalSinkState` (NEW) |
| `TableScanGlobalSourceState` | `GlobalSeqScanState` (existing; rename to `SeqScanGlobalSourceState` for consistency — see §8 migration step M2) |
| `TableScanLocalSourceState` | `LocalSeqScanState` → `SeqScanLocalSourceState` (M2) |

---

## 4. Lifecycle Contract

This is the *missing piece* — without explicit ownership rules, every refactor is a roulette.

### 4.1 Phases (per-pipeline, per-query)

```
LEADER                                       WORKER
──────                                       ──────
1. Plan  → MetaPipeline build → Pipeline list
2. For each Pipeline P (post-order):
     gsrc = P.src->GetGlobalSourceState(leader_ctx)
     gsnk = P.sink->GetGlobalSinkState(leader_ctx)
     // gsrc/gsnk now own their DSM/DSA allocations
     // Pipeline takes std::unique_ptr ownership
3. Launch N workers
                                             4. Worker attaches DSM/DSA
                                             5. Worker resolves Pipeline P (by id)
                                                lsrc = P.src->GetLocalSourceState(w_ctx, *gsrc)
                                                gop_st = P.ops[i]->GetOperatorState(w_ctx)        (×ops)
                                                lsnk = P.sink->GetLocalSinkState(w_ctx, *gsnk)
                                             6. Driver loop:
                                                  P.src->GetData(w_ctx, chunk, {*gsrc, *lsrc})
                                                  for op in P.ops: op->Execute(w_ctx, in, out, *op_st)
                                                  P.sink->SinkChunk(w_ctx, chunk, {*gsnk, *lsnk})
                                                until source FINISHED
                                             7. P.sink->Combine(w_ctx, {*lsnk, *gsnk})
                                                // Worker exports its partial into gsnk's slot
                                             8. for op in P.ops: op_st->Finalize(w_ctx)
                                                lsnk, lsrc, op_st destroyed (worker mcxt)
                                             9. Worker exits
10. WaitForParallelWorkersToFinish
11. Check control->worker_error
12. P.sink->Finalize(leader_ctx, *gsnk)
    // Leader merges all partials (reads gsnk->slots[0..N-1])
    // Returns SinkFinalizeType::{READY, NO_OUTPUT_POSSIBLE}
13. If READY: leader pulls result chunks back to client
14. gsnk, gsrc destroyed (leader mcxt) → DSA frees, SharedFileSet teardown
```

### 4.2 Ownership rules (NORMATIVE)

1. **`GlobalSinkState` / `GlobalSourceState`** are owned by `Pipeline` via `std::unique_ptr`. `Pipeline` adds:
   ```cpp
   std::unique_ptr<GlobalSourceState> global_src_owned;
   std::unique_ptr<GlobalSinkState>   global_sink_owned;
   GlobalSourceState *global_src  = nullptr;  // raw observer (= global_src_owned.get())
   GlobalSinkState   *global_sink = nullptr;
   ```
   The raw pointer pair stays for hot-path access; ownership is the `unique_ptr` pair.
2. **`LocalSinkState` / `LocalSourceState` / `OperatorState`** are owned by `WorkerPipelineExecutor` (one per pipeline being driven). Constructed at the start of `Execute()`, destroyed when `Execute()` returns.
3. **DSM allocations** (control, partials, source_pscan, fileset, dsa region) are owned by `pcxt->seg` and freed when the leader detaches (PostgreSQL standard). `GlobalSinkState` subclasses **do not call** `dsm_detach`/`dsa_detach` — that's the leader's job.
4. **DSA allocations** (per-pipeline scratch — e.g. SortGlobalSinkState's run-buffer pointers) are explicitly `dsa_free`d in the `GlobalSinkState` subclass dtor. Subclass dtor runs in **leader mcxt** at step 14, after workers have detached.
5. **`MemoryContext`**: `GlobalSinkState`'s palloc'd internals live in the per-query worker mcxt (leader: leader's `PipelineWorkerState::memory_context`). When that mcxt is reset/deleted, all internals vanish; subclass dtors must NOT pfree (let mcxt do it).
6. **`SharedFileSet`** (spill files): owned by leader's DSM. Workers open files via `BufFileOpenFileSet`, close via `BufFileClose`. Leader Finalize() reads them. `SharedFileSet` is auto-destroyed when DSM segment detaches.

### 4.3 Construction order rule

`Get*State` calls fire **in this strict order** so that later states can reference earlier states:

```
Leader: GetGlobalSourceState → GetGlobalSinkState
Worker: GetLocalSourceState(gsrc) → GetOperatorState[]  → GetLocalSinkState(gsnk)
```

Rationale: a `LocalSinkState` may want to know `gsrc.MaxThreads()` to size its own per-worker buffer. Forbidding the reverse keeps the dependency graph acyclic.

---

## 5. Concrete Subclasses

### 5.1 `SeqScanGlobalSourceState` / `SeqScanLocalSourceState` (refactor existing)

**Today** (`seq_scan_source.hpp`):
```cpp
class GlobalSeqScanState : public GlobalSourceState {
    SeqScanSourceShared shared_;  // {pg_atomic_uint64 *next_block, BlockNumber total_blocks, uint32 morsel_nblocks}
};
class LocalSeqScanState : public LocalSourceState {
    bool morsel_active = false;
};
```

**Target**:
```cpp
class SeqScanGlobalSourceState : public GlobalSourceState {
public:
    /* Allocated in DSA at GetGlobalSourceState() time, NOT pre-allocated in
     * PipelineSharedControl. One per pipeline that has a SeqScan source. */
    pg_atomic_uint64 *next_block;     // DSA-resident; freed in dtor
    BlockNumber       total_blocks;
    uint32            morsel_nblocks;
    Oid               relid;          // moved out of PipelineSharedControl
    int               plan_node_id;   // moved out of PipelineSharedControl
    ParallelTableScanDesc pscan;      // moved out of PIPELINE_DSM_KEY_SOURCE_PSCAN; now per-source

    idx_t MaxThreads() const override {
        return std::max<idx_t>(1, total_blocks / morsel_nblocks);
    }

    ~SeqScanGlobalSourceState() override {
        if (next_block) dsa_free(dsa_, next_block_handle_);
    }
private:
    dsa_area    *dsa_;
    dsa_pointer  next_block_handle_;
};

class SeqScanLocalSourceState : public LocalSourceState {
public:
    bool          morsel_active = false;
    BlockNumber   current_block;
    BlockNumber   morsel_end;
    /* No DSM/DSA — purely worker-local scan progress. */
};
```

**Removed from `PipelineSharedControl`**: `morsel_nblocks`, `total_blocks`, `source_scan_relid`, `source_scan_plan_node_id`, `next_block`. All migrate into `SeqScanGlobalSourceState`. Removed DSM key: `PIPELINE_DSM_KEY_SOURCE_PSCAN` (now lives inside the source state's DSA-allocated payload).

### 5.2 `AggGlobalSinkState` / `AggLocalSinkState` (refactor existing)

**Today** (`agg_sink.hpp:80-102`):
```cpp
class AggGlobalSinkState : public GlobalSinkState {
    VecAggState              *merger;
    ParallelAggPartialState  *slots;          // raw DSM pointer set externally via SetSharedSlots
    int                       num_slots;
    SharedFileSet            *spill_fileset;  // raw DSM pointer set externally
};
class AggLocalSinkState : public LocalSinkState {
    VecAggState *worker_agg;   // raw, set externally
    int          worker_index;
};
```

**Target**:
```cpp
class AggGlobalSinkState : public GlobalSinkState {
public:
    /* Allocated in GetGlobalSinkState(): leader's mcxt for `merger`,
     * DSA for `slots` array, DSM extension key for `spill_fileset`. */
    VecAggState              *merger;          // leader-only merger; mcxt-owned
    ParallelAggPartialState  *slots;           // DSA-resident; size = MaxThreads()+1
    uint32                    num_slots;
    SharedFileSet            *spill_fileset;   // DSM-resident; nullable
    int                       agg_plan_node_id;// moved out of PipelineSharedControl

    idx_t MaxThreads(idx_t src_max) const override { return src_max; /* 1:1 with workers */ }

    ~AggGlobalSinkState() override {
        /* SharedFileSet & DSA region freed by DSM segment detach;
         * merger lives in leader mcxt → dies with mcxt reset. */
    }
};

class AggLocalSinkState : public LocalSinkState {
public:
    VecAggState *worker_agg;     // worker-local; constructed in GetLocalSinkState
    int          worker_index;
    /* No DSM/DSA owned here. */
};
```

**Key change**: `AggSink::SetSharedSlots` is **deleted**. `GetGlobalSinkState(ctx)` allocates `slots` via `dsa_allocate(ctx.dsa, sizeof(ParallelAggPartialState) * num_slots)` and `spill_fileset` via the leader's `pcxt->toc` (extended dynamic key registration — see §6). `GetLocalSinkState(ctx, gstate)` constructs the worker's `VecAggState` (today done in `InitializePipelineWorkerState`).

**Removed from `PipelineSharedControl`**: `partial_slot_count`, `agg_plan_node_id`. Removed DSM keys: `PIPELINE_DSM_KEY_PARTIALS`, `PIPELINE_DSM_KEY_PARTIAL_FILESET` (now per-sink, registered dynamically — §6).

### 5.3 `SortGlobalSinkState` / `SortLocalSinkState` (NEW)

Mirror DuckDB `OrderGlobalSinkState`/`OrderLocalSinkState` (delegating-style — `OrderGlobalSinkState` wraps a `Sort` object that owns the actual radix-sort state).

```cpp
class SortGlobalSinkState : public GlobalSinkState {
public:
    /* Per-worker sorted-run handles; DSA-resident array. Each entry is a
     * dsa_pointer to a SortedRunHeader { row_count, key_layout_descriptor,
     * payload_dsa_pointer | spill_file_handle }. */
    struct SortedRunHandle {
        uint64       row_count;
        uint8        is_spilled;       // 0 = inline payload in DSA, 1 = BufFile in spill_fileset
        dsa_pointer  payload;          // valid if !is_spilled
        char         spill_filename[VOLVEC_PARALLEL_MAX_PARTIAL_FILE_NAME];
    };
    SortedRunHandle *runs;             // DSA-resident; size = MaxThreads()
    uint32           num_runs;

    /* Leader-side merger. Constructed in Finalize(); owns the k-way merge tree
     * over the SortedRunHandle array. */
    std::unique_ptr<VecSortState> merger;

    /* Optional spill backing for runs that exceed work_mem. */
    SharedFileSet   *spill_fileset;    // DSM-resident; nullable

    /* Sort-key layout shared with all workers (sort_key_count, ASC/DESC bits,
     * NULLS FIRST/LAST, key types). Lives in DSA so workers see a consistent view. */
    dsa_pointer      key_layout_handle;

    idx_t MaxThreads(idx_t src_max) const override { return src_max; }
};

class SortLocalSinkState : public LocalSinkState {
public:
    /* Worker-local sorter. Accumulates rows from SinkChunk(), produces one
     * SortedRunHandle at Combine() time. */
    std::unique_ptr<VecSortState> worker_sorter;
    int                           worker_index;
};
```

**`Sort` interface** (NEW operator alongside `AggSink`):

| Method | Body summary |
|---|---|
| `GetGlobalSinkState(ctx)` | `dsa_allocate` runs array (size = `gsrc.MaxThreads()`); register spill `SharedFileSet` at extended DSM key; allocate `key_layout` in DSA. |
| `GetLocalSinkState(ctx, gstate)` | Construct fresh `VecSortState` in worker mcxt with key layout from `gstate.key_layout_handle`. |
| `SinkChunk(ctx, in, {gstate, lstate})` | `lstate.worker_sorter->add_chunk(in)`. Returns `NEED_MORE_INPUT`; if work_mem exceeded → spill current run to `gstate.spill_fileset` and start fresh sorter. |
| `Combine(ctx, {lstate, gstate})` | `lstate.worker_sorter->finish_sort()` → publish `SortedRunHandle` into `gstate.runs[lstate.worker_index]`; if multiple runs spilled, link them via the run header. |
| `Finalize(ctx, gstate)` | Leader-only. Construct `gstate.merger`, feed it all `gstate.runs[]`, k-way merge to produce final sorted output. Returns `READY`. |

**Why parallel sort matters for Q1**: Q1's serial post-agg sort is currently a single-threaded tail. After Agg, we have ~100 grouped rows, and serial sort is fine for *Q1*. But the abstraction must support parallel sort because (a) it's the canonical DuckDB shape, (b) future Q1-variants with no aggregation (e.g. SELECT … ORDER BY) can't avoid parallel sort.

**Q1 implementation note**: For Q1 specifically, since post-agg cardinality is tiny (~100 rows), the *initial* P3 implementation MAY use a degenerate `SortGlobalSinkState` where `MaxThreads() == 1` and Combine is a no-op (single worker collects, leader Finalize is identity-merge). The full parallel-merge-sort body is gated behind a follow-up GUC `pg_volvec.parallel_sort` (default off until validated).

---

## 6. DSM Layout Restructure

> **⚠️ STALE (2026-04-25, M-FRAME-MIN ownership-split decision).** §6's
> `PipelineKey(pid, kind)` dynamic-DSM-key design (L432-466) and the per-pipeline
> SOURCE_STATE/SINK_STATE/SPILL_FILESET key kinds are **superseded**. Final
> M-FRAME-MIN model (see §8.5 below): leader translates `PlannedStmt` →
> `PhysicalOperator` tree, `MetaPipeline` slicer produces `Pipeline[]`, and the
> serialized `Pipeline[]` lives **inside the per-query DSA** (reachable via a
> single `dsa_pointer pipelines_root` on `PipelineSharedControl`). DSM toc keys
> shrink to **3 fixed**: `PIPELINE_DSM_KEY_CONTROL`, `PIPELINE_DSM_KEY_DSA`,
> `PIPELINE_DSM_KEY_TASK_QUEUE`. No per-pipeline toc keys, no `PipelineKey`
> helper, no `PipelineKeyKind` enum. Workers do **not** translate, do **not**
> deserialize `PlannedStmt`; they attach DSM, read `pipelines_root`, deserialize
> `Pipeline[]` from DSA, and lazily build per-process Local*State on first task.
> Read §8.5/§8.5.1/§8.5.2 instead. The mapping table in §7 below is also stale
> for the same reason; per-pipeline state lives inside the serialized Pipeline IR
> in DSA, not behind dynamic toc keys.

### 6.1 Today (broken for multi-pipeline)

```
shm_toc keys (8 fixed):
  CONTROL          → PipelineSharedControl       (one global)
  PLANNEDSTMT      → serialized PlannedStmt      (one global)
  QUERY_TEXT       → query text                  (one global)
  PARTIALS         → ParallelAggPartialState[N]  (one global, sized for THE agg sink)
  SOURCE_PSCAN     → ParallelTableScanDesc       (one global, sized for THE seq scan)
  PARTIAL_FILESET  → SharedFileSet               (one global)
  PARAM_EXEC       → DEAD CODE
  DSA              → dsa_space                   (one global)
```

### 6.2 Target (multi-pipeline ready)

```
shm_toc keys (4 fixed + N dynamic):

  Fixed (process-global, query-global):
    CONTROL          → PipelineSharedControl  (slimmed: just magic + num_pipelines + worker_error)
    PLANNEDSTMT      → serialized PlannedStmt
    QUERY_TEXT       → query text
    DSA              → dsa_space

  Dynamic, allocated per Pipeline P with PipelineId pid:
    PIPELINE_KEY_BASE = 0xD80100000000PPPP  (lower 16 bits = pid)
    For each pipeline P:
      PIPELINE_KEY(pid, SOURCE_STATE)   → opaque source-state DSM payload (sized by source)
      PIPELINE_KEY(pid, SINK_STATE)     → opaque sink-state DSM payload   (sized by sink)
      PIPELINE_KEY(pid, SPILL_FILESET)  → SharedFileSet (optional; only sinks that spill)
```

**Key generation helper** (in `dsm_control.hpp`):
```cpp
constexpr uint64 PIPELINE_DYNAMIC_KEY_BASE = UINT64CONST(0xD80100000000);
enum class PipelineKeyKind : uint16 {
    SOURCE_STATE   = 0x0001,
    SINK_STATE     = 0x0002,
    SPILL_FILESET  = 0x0003,
};
constexpr uint64 PipelineKey(PipelineId pid, PipelineKeyKind kind) {
    return PIPELINE_DYNAMIC_KEY_BASE
         | (static_cast<uint64>(pid) << 16)
         | static_cast<uint64>(kind);
}
```

**`PipelineSharedControl` slimmed to**:
```cpp
struct PipelineSharedControl {
    uint32           magic;
    uint32           num_pipelines;
    pg_atomic_uint32 worker_error;
};
```

Everything else (`morsel_nblocks`, `total_blocks`, `next_block`, `partial_slot_count`, `*_plan_node_id`, `*_relid`) moves into per-pipeline source/sink state.

**Removed keys**: `PIPELINE_DSM_KEY_PARTIALS`, `PIPELINE_DSM_KEY_SOURCE_PSCAN`, `PIPELINE_DSM_KEY_PARTIAL_FILESET`, `PIPELINE_DSM_KEY_PARAM_EXEC` (dead).

### 6.3 Sink "DSM payload" ABI

Each `GlobalSinkState` subclass defines a `SerializeToDSM(toc, key)` that the leader calls during `pcxt` setup, and a static `DeserializeFromDSM(toc, key)` that workers call to reconstruct a *worker-side view* of the same state. Bodies for AggGlobalSinkState:
```cpp
// leader, in Sink::GetGlobalSinkState
auto state = std::make_unique<AggGlobalSinkState>(...);
state->slots = (ParallelAggPartialState *) shm_toc_allocate(toc,
    sizeof(ParallelAggPartialState) * num_slots);
shm_toc_insert(toc, PipelineKey(pid, SINK_STATE), state->slots);
// state itself stays in leader mcxt; only the DSM-resident slots array is published

// worker, in Sink::GetLocalSinkState
ParallelAggPartialState *slots =
    (ParallelAggPartialState *) shm_toc_lookup(toc, PipelineKey(pid, SINK_STATE), false);
// worker's lstate references slots[worker_index] for Combine()
```

**Note**: `GlobalSinkState` itself is **not** DSM-resident (leader-only object). What goes in DSM is the *raw payload* (slots array, fileset, etc.) that workers must observe. This matches DuckDB's model where `HashAggregateGlobalSinkState` is leader-side-only and partition tables are the DSM-equivalent.

---

## 7. Mapping Table — Current State Items → Future Owners

This is the migration spec. Each row tells the engineer **exactly** where each existing field lives today and where it goes.

| # | Item (current) | Today's location | Today's site (file:line) | Future owner | Notes |
|---:|---|---|---|---|---|
| 1 | `magic` | `PipelineSharedControl` | `dsm_control.hpp:31` | Stays in `PipelineSharedControl` | Sanity check kept. |
| 2 | `partial_slot_count` | `PipelineSharedControl` | `dsm_control.hpp:32` | `AggGlobalSinkState::num_slots` | Per-sink, not global. |
| 3 | `morsel_nblocks` | `PipelineSharedControl` | `dsm_control.hpp:33` | `SeqScanGlobalSourceState::morsel_nblocks` | Per-source. |
| 4 | `total_blocks` | `PipelineSharedControl` | `dsm_control.hpp:34` | `SeqScanGlobalSourceState::total_blocks` | Per-source. |
| 5 | `source_scan_relid` | `PipelineSharedControl` | `dsm_control.hpp:35` | `SeqScanGlobalSourceState::relid` | Per-source. |
| 6 | `source_scan_plan_node_id` | `PipelineSharedControl` | `dsm_control.hpp:36` | `SeqScanGlobalSourceState::plan_node_id` | Per-source. |
| 7 | `agg_plan_node_id` | `PipelineSharedControl` | `dsm_control.hpp:37` | `AggGlobalSinkState::agg_plan_node_id` | Per-sink. |
| 8 | `next_block` (atomic) | `PipelineSharedControl` | `dsm_control.hpp:38` | `SeqScanGlobalSourceState::next_block` (DSA-allocated) | Per-source. |
| 9 | `worker_error` (atomic) | `PipelineSharedControl` | `dsm_control.hpp:39` | Stays in `PipelineSharedControl` | Process-global signal. |
| 10 | `partials` array | DSM key `PARTIALS` | `pipeline_leader.cpp:216-219` | `AggGlobalSinkState::slots` (DSA, dyn DSM key `SINK_STATE(pid)`) | Per-sink. |
| 11 | `ParallelAggPartialState` struct | `plan_state.hpp:112-127` | (definition) | Unchanged on disk; consumed by `AggGlobalSinkState::slots[]` | Layout stays. |
| 12 | `source_pscan` | DSM key `SOURCE_PSCAN` | `pipeline_leader.cpp:221-227` | `SeqScanGlobalSourceState::pscan` (DSA-allocated) | Per-source. |
| 13 | `partial_fileset` | DSM key `PARTIAL_FILESET` | `pipeline_leader.cpp:229-233` | `AggGlobalSinkState::spill_fileset` (DSM dyn key `SPILL_FILESET(pid)`) | Per-sink. |
| 14 | `dsa_space` | DSM key `DSA` | `pipeline_leader.cpp:235-241` | Stays at `PIPELINE_DSM_KEY_DSA` | Process-global. |
| 15 | `plannedstmt` | DSM key `PLANNEDSTMT` | `pipeline_leader.cpp:205-208` | Stays | Process-global. |
| 16 | `query_text` | DSM key `QUERY_TEXT` | `pipeline_leader.cpp:210-214` | Stays | Process-global. |
| 17 | `param_exec` | DSM key `PARAM_EXEC` | `dsm_control.hpp:20` (dead) | **DELETE** | Never used. |
| 18 | `SeqScanSourceShared` | `seq_scan_source.hpp:20-24` | (struct) | **DELETE** — fields absorbed into `SeqScanGlobalSourceState` | Removes one indirection. |
| 19 | `GlobalSeqScanState` (rename) | `seq_scan_source.hpp:26-33` | (class) | Rename to `SeqScanGlobalSourceState`; absorb `SeqScanSourceShared` | Naming consistency. |
| 20 | `LocalSeqScanState` (rename) | `seq_scan_source.hpp:35-38` | (class) | Rename to `SeqScanLocalSourceState` | Naming consistency. |
| 21 | `AggSink::SetSharedSlots` | `agg_sink.hpp:48-55` | (method) | **DELETE** — allocation moves into `GetGlobalSinkState` | External wiring eliminated. |
| 22 | `AggSink::shared_slots_, num_slots_, spill_fileset_` | `agg_sink.hpp:75-77` | (fields) | **DELETE** — these now live on `AggGlobalSinkState` | Sink-stateless after refactor. |
| 23 | `AggLocalSinkState::worker_agg` external set | `pipeline_lowering.cpp` (sets via P3 lowering) | TBD | Set inside `AggSink::GetLocalSinkState(ctx, gstate)` | Single point of construction. |
| 24 | `Pipeline::global_src/global_sink` (raw) | `pipeline.hpp:27-28` | (fields) | Augment with `unique_ptr` siblings (`global_src_owned`, `global_sink_owned`) | Explicit ownership. |
| 25 | `PipelineWorkerState::root_plan, agg_state, estate, plannedstmt, query_text, memory_context, init_time_us, proc_exit_callback_registered` | `pipeline_worker_state.hpp:31-39` | (fields) | Stay — these are **per-process per-query** worker bootstrap, distinct from per-pipeline state | Boundary preserved. |
| 26 | `PipelineWorkerContext` | `pipeline_worker_context.hpp:22-33` | (fields) | Stay (legacy POD, internal to worker init) | Not part of the new hierarchy. |
| 27 | (no Sort sink today) | — | — | Add `SortGlobalSinkState` + `SortLocalSinkState` + `SortSink` | NEW per §5.3. |

**Net result**: `PipelineSharedControl` shrinks from 9 fields to 3. DSM keys shrink from 8 to 4 fixed + N dynamic. `AggSink` becomes stateless (all state on `AggGlobalSinkState`). `SeqScanSource` becomes stateless except for the `VecSeqScanState *` it lowers from. New `SortSink` lands.

---

## 8. P3X Architecture — PhysicalOperator IR + MetaPipeline + DSM TaskScheduler

> **Revision (2026-04-24).** This §8 SUPERSEDES the prior M0–M7 migration table and the plan's P3a/P3b/P3b'/P3d phasing. Per user decision (handoff §6), P3X is a single phase covering: (1) PhysicalOperator polymorphic IR with 4 concrete ops, (2) `MetaPipeline::Build` blocking-op slicer faithful to DuckDB, (3) 5-event dependency-DAG scheduler, (4) DSM lock-free task queue + query-scoped bgworker pool, **(5) query-scoped BufferManager wrapping DSA/SharedFileSet with swizzle for zero-copy cross-pipeline data-plane (§8.7)**. P3-0 (`AggSink::SetSharedSlots` deletion + DSA destruction order from prior M2/M4) is already shipped and live-verified (serial Q6 = `1230113636.0101`, parallel Q6 workers=2 no SEGV). Remaining work below subsumes M3 (dynamic DSM keys) + M5 (Sort sink) + the entire P3a/P3b/P3b'/P3d framework.

### 8.0 Mapping from old M-steps

| Old step | Status | Where in P3X |
|---|---|---|
| M0 (`state_base.hpp`) | Pending | §8.1 — folded into IR header set |
| M1 (`unique_ptr` siblings on `Pipeline`) | Pending | §8.2 — superseded by `MetaPipelineBundle` ownership |
| M2 (SeqScan rename + DSA-allocated `next_block`) | **DONE in P3-0** | (no-op) |
| M3 (`PipelineKey(pid, kind)` dynamic keys) | **Superseded** | §8.5 — replaced by DSA-resident Pipeline IR + 3 fixed toc keys (no dynamic keys, no `PipelineKey` helper). |
| M4 (`AggSink::SetSharedSlots` deletion + sink-owned alloc) | **DONE in P3-0** | (no-op) |
| M5 (Sort sink + 2-pipeline Q1) | Pending | §8.1 (PhysicalOrder) + §8.3 (MetaPipeline) |
| M6 (parallel sort behind GUC) | Deferred | non-goal, GUC `pg_volvec.parallel_sort` reserved |
| M7 (`PipelineWorkerContext` cleanup) | Pending | §8.6 — happens implicitly when scheduler replaces leader/worker glue |

### 8.1 PhysicalOperator IR (4 concrete ops)

New header `pipeline/physical_operator.hpp` defining a polymorphic IR. Mirrors DuckDB `src/include/duckdb/execution/physical_operator.hpp` (lines 40–247 @ SHA `5af191c…`), trimmed to the 4 ops Q1+Q6 need.

```cpp
namespace pg_volvec::pipeline {

class MetaPipeline;          // §8.3
class Pipeline;              // existing
class ExecutionContext;      // existing
class DataChunk;             // existing
struct GlobalSinkState;      // §3.1 base
struct LocalSinkState;       // §3.1 base
struct GlobalSourceState;    // §3.1 base
struct LocalSourceState;     // §3.1 base
struct OperatorState;        // §3.1 base
struct GlobalOperatorState;  // §3.1 base

enum class PhysicalOpType : uint8 {
    SEQ_SCAN,                // source-only
    FILTER,                  // streaming op
    HASH_AGGREGATE,          // dual-role: Sink (build) + Source (drain)
    ORDER,                   // dual-role: Sink (sort) + Source (drain)
};

class PhysicalOperator {
public:
    PhysicalOperator(PhysicalOpType t) : type_(t) {}
    virtual ~PhysicalOperator() = default;

    PhysicalOpType                type() const { return type_; }
    const std::vector<std::unique_ptr<PhysicalOperator>> &children() const { return children_; }

    // -- Role predicates (dual-role allowed for HashAggregate, Order)
    virtual bool IsSource() const { return false; }
    virtual bool IsSink()   const { return false; }
    bool IsPipelineBreaker() const { return IsSink(); }   // sinks materialize → boundary

    // -- Source side (called when IsSource()==true)
    virtual std::unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &) const { return nullptr; }
    virtual std::unique_ptr<LocalSourceState>  GetLocalSourceState (ExecutionContext &, GlobalSourceState &) const { return nullptr; }
    virtual SourceResultType GetData(ExecutionContext &, DataChunk &out, OperatorSourceInput &) const = 0;

    // -- Operator side (streaming, called for non-source non-sink ops)
    virtual std::unique_ptr<OperatorState>       GetOperatorState      (ExecutionContext &) const { return nullptr; }
    virtual std::unique_ptr<GlobalOperatorState> GetGlobalOperatorState(ClientContext &)    const { return nullptr; }
    virtual OperatorResultType Execute(ExecutionContext &, DataChunk &in, DataChunk &out, GlobalOperatorState &, OperatorState &) const { return OperatorResultType::FINISHED; }

    // -- Sink side (called when IsSink()==true)
    virtual std::unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &) const { return nullptr; }
    virtual std::unique_ptr<LocalSinkState>  GetLocalSinkState (ExecutionContext &)  const { return nullptr; }
    virtual SinkResultType   Sink    (ExecutionContext &, DataChunk &in, OperatorSinkInput &)            const { return SinkResultType::FINISHED; }
    virtual SinkCombineResultType Combine(ExecutionContext &, OperatorSinkCombineInput &)               const { return SinkCombineResultType::FINISHED; }
    virtual SinkFinalizeType Finalize(Pipeline &, Event &, ClientContext &, OperatorSinkFinalizeInput &) const { return SinkFinalizeType::READY; }

    // -- Parallelism
    virtual idx_t MaxThreads(ClientContext &) const { return 1; }

    // -- Pipeline construction (default: linear pass-through; Sink overrides — §8.3)
    virtual void BuildPipelines(Pipeline &current, MetaPipeline &meta);

protected:
    PhysicalOpType type_;
    std::vector<std::unique_ptr<PhysicalOperator>> children_;
};

}  // namespace
```

**Concrete subclasses (one `.{hpp,cpp}` pair each, ~60–120 LOC):**

| File | Class | Roles | Notes |
|---|---|---|---|
| `physical_seq_scan.{hpp,cpp}` | `PhysicalSeqScan` | Source | `IsSource()=true`. POD config: `Oid relid; AttrNumber *projection; int nattrs; List *qual` (qual lowered to embedded predicate; no separate Filter node when scan-attached). `MaxThreads()` = `Min(parallel_max_workers, ceil(nblocks/morsel_nblocks))`. `GetData` drives morsel-based page scan (existing `seq_scan_source.cpp` logic, refactored under new state classes). |
| `physical_filter.{hpp,cpp}` | `PhysicalFilter` | Operator | POD config: `Expr *predicate; Oid *param_oids`. Streaming `Execute`. Used only for stand-alone Filter nodes that aren't fused into SeqScan. |
| `physical_hash_aggregate.{hpp,cpp}` | `PhysicalHashAggregate` | **Sink + Source (dual-role)** | `IsSink()=true` AND `IsSource()=true`. Sink phase populates DSM-backed slot table (`AggGlobalSinkState`, owns spill `SharedFileSet`); Source phase drains finalized slots to feed downstream pipeline. Pipeline breaker. |
| `physical_order.{hpp,cpp}` | `PhysicalOrder` | **Sink + Source (dual-role)** | `IsSink()=true` AND `IsSource()=true`. First cut: `MaxThreads()=1` (degenerate single-thread sort), identity-merge `Combine`. Pipeline breaker. Parallel-sort path gated behind GUC `pg_volvec.parallel_sort` (out of P3X scope). |

**Determinism Contract (Oracle B3 — folded from old §14.1):**
- No `unordered_*` iteration in IR construction.
- Children visited in declaration order (`children_[0]`, `children_[1]`, …).
- No address-based ordering anywhere in `BuildPipelines`.
- POD-only fields in PhysicalOperator subclasses; no `Vec*State` references (the old `VecPlanState` AST is deleted; PhysicalOperator IR is the single IR — Oracle B1).
- Enforced by grep gate `QA-P3X-3` (§15.6).

### 8.2 Translator (PG → IR)

New file `pipeline/pg_translator.{hpp,cpp}`. Walks `PlannedStmt->planTree` post-order, emitting `PhysicalOperator` subclasses. Returns `nullptr` to signal "unsupported shape; fall back to native PG" (replaces today's pattern-match in `pipeline_lowering.cpp`). Pure plan→IR — no `PipelineSharedControl*` or other runtime args (Oracle B4 from old §14.1).

Replaces today's `LowerToPipeline(VecPlanState *root, …)` at `pipeline_leader.cpp:261-266`. The bridge entry at `bridge/execute.cpp:151-170` invokes:

```cpp
auto root = PgTranslator::Translate(stmt);   // null → fall back
if (!root) return PgVolvecFallbackToPgExecutor(stmt);
auto bundle = MetaPipeline::Build(std::move(root));   // §8.3
auto sched  = TaskScheduler::Create(*bundle);          // §8.4
sched->Run();                                          // §8.5
```

**Files deleted:** `pipeline_lowering.{hpp,cpp}`, `vec_plan_state.{hpp,cpp}`, `vec_seq_scan_state.{hpp,cpp}`, `vec_agg_state.{hpp,cpp}`. Grep gate `QA-P3X-4` enforces zero `Vec*State` symbols anywhere under `contrib/pg_volvec/src/engine/parallel/pipeline/`.

### 8.3 MetaPipeline (blocking-op slicing — DuckDB-faithful)

New files `pipeline/meta_pipeline.{hpp,cpp}`. Mirrors DuckDB `src/include/duckdb/parallel/meta_pipeline.hpp` and `src/parallel/meta_pipeline.cpp`.

```cpp
namespace pg_volvec::pipeline {

class MetaPipeline {
public:
    // Public entry: build a bundle from a PhysicalOperator root.
    // Dense PipelineId 0..N-1 assigned in POST-ORDER (children before parents)
    // → identical IDs across leader and worker processes (Determinism Contract).
    static std::unique_ptr<MetaPipelineBundle>
    Build(std::unique_ptr<PhysicalOperator> root);

    // Mutator API used by PhysicalOperator::BuildPipelines (§8.1):
    Pipeline &CreateChildPipeline(Pipeline &parent, PhysicalOperator *sink);
    Pipeline &CreatePipeline();
    void      AddOperator(Pipeline &p, PhysicalOperator *op);
    void      SetSource  (Pipeline &p, PhysicalOperator *src);
    void      SetSink    (Pipeline &p, PhysicalOperator *sink);

    const std::vector<std::unique_ptr<Pipeline>> &Pipelines() const { return bundle_->pipelines; }
    const std::vector<std::vector<PipelineId>>   &DependsOn() const { return bundle_->depends_on; }

private:
    std::unique_ptr<MetaPipelineBundle> bundle_;
    PipelineId                          next_id_{0};
};

struct MetaPipelineBundle {
    std::vector<std::unique_ptr<Pipeline>> pipelines;     // index = PipelineId
    std::vector<std::vector<PipelineId>>   depends_on;    // depends_on[i] = ids that must finish before i runs
};

}  // namespace
```

**Default `PhysicalOperator::BuildPipelines` (mirrors DuckDB lines 285–325 of `physical_operator.cpp`):**

```cpp
void PhysicalOperator::BuildPipelines(Pipeline &current, MetaPipeline &meta) {
    if (IsSink()) {
        // I am a pipeline breaker. End the parent pipeline using me as its sink,
        // and start a NEW child pipeline whose source is also me (dual-role).
        meta.SetSink(current, this);
        Pipeline &child = meta.CreateChildPipeline(current, this);
        meta.SetSource(child, this);
        // Recurse into my single child as the producer of the new pipeline's sink.
        D_ASSERT(children_.size() == 1);
        children_[0]->BuildPipelines(child, meta);
        return;
    }
    if (IsSource()) {
        meta.SetSource(current, this);
        D_ASSERT(children_.empty());
        return;
    }
    // Streaming operator: add to current pipeline and recurse.
    meta.AddOperator(current, this);
    D_ASSERT(children_.size() == 1);
    children_[0]->BuildPipelines(current, meta);
}
```

**Q1 emission** (Sort → Agg → SeqScan(filter)):
- Pipeline 0 (id=0): source = SeqScan(filter), no operators, sink = HashAggregate (Sink role).
- Pipeline 1 (id=1): source = HashAggregate (Source role, drains finalized slots), no operators, sink = Order (Sink role).
- Pipeline 2 (id=2): source = Order (Source role, drains sorted runs), no operators, sink = `OutputSink` (delivers to PG `DestReceiver`).
- `depends_on = [[], [0], [1]]`.

**Q6 emission** (Agg → SeqScan(filter)):
- Pipeline 0 (id=0): source = SeqScan(filter), no operators, sink = HashAggregate (Sink role).
- Pipeline 1 (id=1): source = HashAggregate (Source role), no operators, sink = `OutputSink`.
- `depends_on = [[], [0]]`.

`OutputSink` is the 5th implicit "operator" added by `MetaPipeline::Build` to terminate the topmost pipeline. Not a user-facing PhysicalOperator subclass; lives in `pipeline/output_sink.{hpp,cpp}`.

### 8.4 5-Event lifecycle + dependency DAG (Oracle B1, B2 — lifecycle ERROR-safety + Combine-only-on-success)

Mirrors DuckDB `src/include/duckdb/parallel/event.hpp` and `src/parallel/executor.cpp` (5-event chain construction). Per pipeline, exactly 5 events are constructed at scheduler initialization:

```
PipelineInitializeEvent  — leader: build GlobalSinkState, GlobalSourceState, GlobalOperatorState in DSA from the already-serialized `Pipeline[]` (leader serialized them at scheduler `Create()`, see §8.5.2). No per-pipeline DSM toc keys are allocated here.
        ▼
PipelineEvent            — workers: attach DSM, construct LocalSinkState/LocalSourceState/OperatorState, run driver loop (Source.GetData → Operator.Execute* → Sink.Sink), exit with COMBINE_PENDING (success) or ABORT (error).
        ▼
PipelinePrepareFinishEvent — leader: assert all PipelineEvent worker tasks reported COMBINE_PENDING. If ANY reported ABORT, propagate via control->worker_error AND skip all subsequent events for this pipeline AND fan-out CompleteDependency on dependents (so they don't deadlock).
        ▼
PipelineFinishEvent      — workers: each worker calls Sink.Combine(local→global) IFF its PipelineEvent ended in COMBINE_PENDING. ABORT-ended workers SKIP Combine entirely (Oracle B2: "Combine only on success" — partial state must NOT be merged into the global, or the bug pattern from §8.5 reappears).
        ▼
PipelineCompleteEvent    — leader: Sink.Finalize(); record SinkFinalizeType (READY | NO_OUTPUT_POSSIBLE | BLOCKED); fire CompleteDependency on each parent pipeline's PipelineEvent (cascade through depends_on).
```

**Cross-MetaPipeline dependency edge (Oracle B2 + DuckDB faithful):** if pipeline B has `depends_on[B] ∋ A`, then `B.PipelineEvent.AddDependency(A.PipelineCompleteEvent)`. The Event class holds parents as `std::vector<std::weak_ptr<Event>>`; `CompleteDependency` decrements an atomic counter on each parent and triggers `Schedule()` when the counter hits zero. This is identical to DuckDB's `Event::CompleteDependency` + `Event::FinishEvent` cascade.

**ERROR-safety contract (Oracle B1 — folded from old §14.3 B6 + B7):**

PG's `ereport(ERROR)` longjmps past C++ destructors. To prevent leaks across pipelines in the long-lived worker pool model:

1. **All Local* state lives in MemoryContext.** Stack RAII at `executor.cpp:45-49` is FORBIDDEN inside `PG_TRY`. Locals are allocated via `MemoryContextAlloc(per_pipeline_ctx, …)` and registered with placement-new + explicit dtor invocation in the success path; failure path skips the dtor (the MemoryContext is destroyed wholesale on `PG_CATCH`).
2. **`PG_TRY` boundary is at the `ExecuteTask` pump (§8.5) — not inside `Sink`/`Operator`/`Source` callbacks.** Each task body is wrapped in `PG_TRY/PG_CATCH/PG_END_TRY`. On `PG_CATCH`: set `control->worker_error`, free the per-task MemoryContext, signal task done with `TaskExecutionResult::ERROR`, do NOT `PG_RE_THROW`.
3. **JIT cleanup via `before_shmem_exit` callback** (already shipped in P3-0 follow-up via `proc_exit_callback_registered` flag). Re-asserted at scheduler init.
4. **DSA destruction order (Oracle B3 — folded from old §14.3 + verified in P3-0 §8.2):** leader detaches DSA last. Worker `before_shmem_exit` callback NEVER calls `dsa_detach` directly — it only nulls `g_worker_ctx.{state,registry}` and lets the DSA segment die when the leader's `DestroyParallelContext` runs. Enforced by grep gate `QA-P3X-7`.

### 8.5 DSM TaskScheduler + query-scoped worker pool

New files `pipeline/task_scheduler.{hpp,cpp}`. Mirrors DuckDB `src/include/duckdb/parallel/task_scheduler.hpp` (`Task`, `ProducerToken`, `ScheduleTasks`, `ExecuteForever`/`ExecuteTask`), adapted for PG bgworkers.

**Worker pool lifecycle:**
- Launched ONCE per query at scheduler `Create()`.
- `pg_volvec.parallel_max_workers == 0` → ERROR (per user lock).
- `parallel_max_workers >= 1` → leader is pure coordinator (`LEADER_WORKER_INDEX = -1`), launches N workers via single `LaunchParallelWorkers` call (Oracle B7 from old §14.3: single-`ParallelContext` invariant).
- Workers + leader pump `ExecuteTask` loop until scheduler signals shutdown (all events at COMPLETE).

**Task types** (each a `Task` subclass, dispatched to workers via DSM lock-free queue):
- `PipelineInitializeTask` — leader-only, runs once per pipeline.
- `PipelineTask` — N parallel slots per pipeline (N = `op->MaxThreads()`); workers race for slots via atomic `pipeline_task_slot[pid].fetch_add(1)`.
- `PipelinePrepareFinishTask` — leader-only.
- `PipelineFinishTask` — one per worker that completed `PipelineTask` with COMBINE_PENDING.
- `PipelineCompleteTask` — leader-only.

**DSM key restructure (M-FRAME-MIN final, 2026-04-25):**

DSM toc keys collapse to **3 fixed**, all per-query:

```cpp
static constexpr uint64 PIPELINE_DSM_KEY_CONTROL    = UINT64CONST(0xD800000000000001);
static constexpr uint64 PIPELINE_DSM_KEY_DSA        = UINT64CONST(0xD800000000000008);
static constexpr uint64 PIPELINE_DSM_KEY_TASK_QUEUE = UINT64CONST(0xD800000000000009);
```

All other historical keys (`PLANNEDSTMT`, `QUERY_TEXT`, `PARTIALS`, `SOURCE_PSCAN`,
`PARTIAL_FILESET`, `PARAM_EXEC`) are **deleted**. No dynamic per-pipeline keys; no
`PipelineKey(pid, kind)` helper; no `PipelineKeyKind` enum.

`PipelineSharedControl` carries 4 fields — `magic`, `num_pipelines`,
`pg_atomic_uint32 worker_error`, and a single `dsa_pointer pipelines_root`
pointing into the DSA segment at the head of the serialized `Pipeline[]`:

```cpp
struct PipelineSharedControl {
    uint32           magic;            // 0x56505043
    int32            num_pipelines;
    pg_atomic_uint32 worker_error;     // 0 = no error
    dsa_pointer      pipelines_root;   // → PipelineDescriptor[num_pipelines] in DSA
};
```

Per-pipeline state (Global{Source,Sink,Operator}State, partial-agg slots,
sort-run handles, spill filesets) lives **inside the DSA segment**, reachable by
following `pipelines_root` and the offsets embedded in each
`PipelineDescriptor`. Workers attach DSA via `PIPELINE_DSM_KEY_DSA` and resolve
all per-pipeline addresses through `dsa_get_address` — never through additional
toc lookups.

#### 8.5.1 Ownership split (leader-translate / worker-execute) — LOCKED

| Phase | Leader | Worker |
|---|---|---|
| Translate `PlannedStmt → PhysicalOperator` tree | ✅ | ❌ |
| Slice tree → `Pipeline[]` via `MetaPipeline::Build` | ✅ | ❌ |
| **Serialize `Pipeline[]` into DSA** (one-shot at `scheduler.Create()`) | ✅ | — |
| Publish `pipelines_root` on `PipelineSharedControl` | ✅ | — |
| **Attach DSM (3 toc keys) + DSA + deserialize `Pipeline[]` from DSA** | — | ✅ at attach |
| Build per-process Global*/Local*State (lazy, on first relevant task) | ✅ (in-process) | ✅ (in-process, from deserialized Pipeline IR) |
| Generate Events + Tasks | ✅ | ❌ |
| `Execute(Task)` | ✅ (leader is also a task consumer) | ✅ |
| Schedule next event-DAG batch | ✅ | ❌ |
| Combine partials → globals (Combine event) | ✅ (leader-only `PipelineFinishTask`) | ✅ (each worker's local→shared, Combine-only-on-success) |
| Finalize sink, fan-out CompleteDependency | ✅ | ❌ |

What lives in DSA (worker-readable): serialized `PipelineDescriptor[]`,
per-pipeline shared state (partial-agg buckets, atomic morsel counters), MPMC
TaskQueue payload, sink/source global state structs.
What does **not** live in DSA: PG `PlannedStmt`, PG `Plan` nodes, raw query
text, palloc'd pointers (DSA offsets only — pre-existing anti-pattern, restated).

#### 8.5.2 Pipeline IR serialization to DSA

At `TaskScheduler::Create()`, after `MetaPipeline::Build` produces `Pipeline[]`
(an in-process `std::vector<std::unique_ptr<Pipeline>>` of `PhysicalOperator`
chains), the leader walks the vector and copies a serializable form
(`PipelineDescriptor`) into DSA via `dsa_allocate`. Each `PipelineDescriptor`
carries:

- `pipeline_id` (dense `int32` index into `pipelines_root[]`)
- `dependency_mask` (`uint64` bitmap over `pipeline_id`s; replaces `depends_on`
  vector for fixed-size DSA layout; 64-pipeline ceiling matches Q1+Q6 needs and
  keeps the descriptor POD)
- `op_count` and `dsa_pointer ops` (→ `OpDescriptor[op_count]` in DSA)
- `dsa_pointer global_source_state`, `global_sink_state`, `global_op_state[]`
  (each null until `PipelineInitializeEvent` runs)
- `pg_atomic_uint32 task_slot_next` (worker fetch-add for `MaxThreads()` slot
  assignment)

`OpDescriptor` is a tagged-union POD over the 4 concrete ops (`SeqScan`,
`Filter`, `HashAggregate`, `Order`) plus a 5th `OutputSink` tag. Each op's
metadata (schema, agg specs, filter expression IR bytecode, sort keys) is
inlined or DSA-pointer-referenced. Expression IR uses the existing linear
bytecode form from `expr.cpp`; bytecode buffers live in DSA via `dsa_allocate`
and are referenced by `dsa_pointer` from `OpDescriptor`.

Worker side: at `pg_volvec_pipeline_worker_main` attach time, the worker calls
`PipelineDsmLookup<PipelineSharedControl>(toc, PIPELINE_DSM_KEY_CONTROL,
"pipeline_control")`, then `dsa_attach_in_place(toc lookup of DSA key)`, then
walks `control->pipelines_root` to materialize a per-process `std::vector`
(allocated under per-query MemoryContext via `PgMemoryContextAllocator`) of
**process-local** `PhysicalOperator*` pointers reconstructed from the
DSA-resident `OpDescriptor[]`. The reconstructed `PhysicalOperator*` instances
are **not** copies of the leader's heap objects — they are freshly placement-new'd
into per-query MemoryContext from the DSA descriptor. Per-process Local*State
is then built lazily inside `Task::Execute` on the appropriate operator.

This serializer/deserializer pair is the only "plan-shaped" cross-process
contract; PG's `PlannedStmt` never crosses the process boundary inside
`pg_volvec`.

**`shm_toc` lookup wrapper (Oracle B4 — folded):** all `shm_toc_lookup` sites use `PipelineDsmLookup<T>(toc, key, "human_readable_name")` which `ereport(ERROR)`s on missing key with the symbolic name (today's raw `shm_toc_lookup(toc, key, false)` returns NULL on miss with no diagnostic). Grep gate `QA-P3X-5` forbids raw `shm_toc_lookup` outside the wrapper.

**Lock-free task queue:** MPMC ring buffer in DSM segment `PIPELINE_DSM_KEY_TASK_QUEUE`. Workers `dequeue()` and execute; leader `enqueue()` from event `Schedule()` callbacks. Wake via `SetLatch` on each worker's `MyLatch` (PG's standard cross-process wake primitive). No `ConditionVariable` (avoids the deadlock pattern Oracle flagged in old §14.3 B4).

**Worker entrypoint** (replaces today's `pg_volvec_pipeline_worker_main` at `pipeline_worker_main.cpp:129`):

```cpp
void pg_volvec_pipeline_worker_main(Datum) {
    // 1. PG one-time init (snapshot, parallel mode, GUCs) — already done by ParallelWorkerMain
    //    upstream; we do NOT re-do it (Oracle old §14.3 B1).
    // 2. Attach DSM (3 toc keys: CONTROL, DSA, TASK_QUEUE), look up control,
    //    dsa_attach_in_place, deserialize Pipeline[] from control->pipelines_root
    //    into a per-process PhysicalOperator vector under per-query MemoryContext
    //    (§8.5.2). NO PlannedStmt deserialization. NO Translator in worker.
    //    Per-process Local*State is built lazily inside Task::Execute (below).
    // 3. Register before_shmem_exit JIT cleanup callback (P3-0 follow-up shipped).
    // 4. ExecuteTask pump:
    while (!scheduler.ShutdownRequested()) {
        Task *task = scheduler.GetTaskFromQueue();   // blocks on MyLatch
        if (!task) continue;
        PG_TRY();
        {
            task->Execute();
        }
        PG_CATCH();
        {
            pg_atomic_write_u32(&control->worker_error, 1);
            FreeErrorData(CopyErrorData());
            FlushErrorState();
            // do NOT PG_RE_THROW — pump continues so we drain remaining tasks
            // OR exit cleanly when scheduler sees worker_error and broadcasts shutdown.
        }
        PG_END_TRY();
        scheduler.ReportTaskDone(task);
    }
    // proc_exit cleanup runs registered before_shmem_exit callbacks (JIT, Local* dtors).
}
```

### 8.6 Current → Target file delta

| Path | Action | Notes |
|---|---|---|
| `pipeline/physical_operator.{hpp,cpp}` | NEW | §8.1 base + dispatch |
| `pipeline/physical_seq_scan.{hpp,cpp}` | NEW | source-only |
| `pipeline/physical_filter.{hpp,cpp}` | NEW | streaming op |
| `pipeline/physical_hash_aggregate.{hpp,cpp}` | NEW | dual-role |
| `pipeline/physical_order.{hpp,cpp}` | NEW | dual-role, MaxThreads=1 first cut |
| `pipeline/output_sink.{hpp,cpp}` | NEW | terminates topmost pipeline |
| `pipeline/pg_translator.{hpp,cpp}` | NEW | §8.2; replaces `pipeline_lowering.*` |
| `pipeline/meta_pipeline.{hpp,cpp}` | NEW | §8.3 |
| `pipeline/event.{hpp,cpp}` | NEW | base Event class + 5 subclasses |
| `pipeline/pipeline_initialize_event.{hpp,cpp}` | NEW | §8.4 |
| `pipeline/pipeline_event.{hpp,cpp}` | NEW | §8.4 |
| `pipeline/pipeline_prepare_finish_event.{hpp,cpp}` | NEW | §8.4 |
| `pipeline/pipeline_finish_event.{hpp,cpp}` | NEW | §8.4 |
| `pipeline/pipeline_complete_event.{hpp,cpp}` | NEW | §8.4 |
| `pipeline/task_scheduler.{hpp,cpp}` | NEW | §8.5 + worker pool |
| `pipeline/task.{hpp,cpp}` | NEW | base Task + 5 subclasses |
| `pipeline/dsm_task_queue.{hpp,cpp}` | NEW | MPMC ring in DSM |
| `pipeline/state_base.hpp` | NEW (was M0) | augmented base classes |
| `pipeline/pipeline_lowering.{hpp,cpp}` | DELETE | replaced by §8.2/§8.3 |
| `pipeline/pipeline_leader.cpp` | REWRITE | thin shim → scheduler.Create().Run() |
| `pipeline/pipeline_worker_main.cpp` | REWRITE | thin shim → ExecuteTask pump (§8.5) |
| `pipeline/pipeline_worker_state.{hpp,cpp}` | KEEP | still owns per-worker JIT + before_shmem_exit registration |
| `pipeline/dsm_control.hpp` | SHRINK | 9 toc keys → 3 (`CONTROL`, `DSA`, `TASK_QUEUE`); `PipelineSharedControl` 9 fields → 4 (`magic`, `num_pipelines`, `worker_error`, `pipelines_root`). Delete `PLANNEDSTMT`, `QUERY_TEXT`, `PARTIALS`, `SOURCE_PSCAN`, `PARTIAL_FILESET`, `PARAM_EXEC` keys. No `PipelineKey`/`PipelineKeyKind`/`PIPELINE_DYNAMIC_KEY_BASE`. |
| `pipeline/pipeline_descriptor.{hpp,cpp}` | NEW | DSA-resident POD form of a Pipeline; leader serializes `Pipeline[]` here at `scheduler.Create()` (§8.5.2); workers deserialize at attach time. Includes `OpDescriptor` tagged-union for the 5 op kinds (SeqScan/Filter/HashAggregate/Order/OutputSink). |
| `pipeline/seq_scan_source.{hpp,cpp}` | RENAME + REFACTOR | now backs `PhysicalSeqScan::ToSource` impl |
| `pipeline/agg_sink.{hpp,cpp}` | REFACTOR | becomes `PhysicalHashAggregate` impl backing |
| `pipeline/sort_sink.{hpp,cpp}` | NEW | backs `PhysicalOrder` impl |
| `bridge/execute.cpp:151-170` | REWRITE | invokes new translator+meta+scheduler chain |
| `vec_plan_state.{hpp,cpp}` etc. | DELETE | per Oracle B1 (folded) |
| `pipeline/buffer_manager.{hpp,cpp}` | NEW | §8.7 — query-scoped BufferManager (DuckDB-faithful: Allocate + Pin/Unpin + memory accounting + spill) |
| `pipeline/buffer_handle.{hpp,cpp}` | NEW | §8.7 — RAII Pin handle |
| `pipeline/block_handle.{hpp,cpp}` | NEW | §8.7 — refcounted block descriptor with swizzle metadata |
| `pipeline/pipeline_row_collection.{hpp,cpp}` | NEW | §8.7 — TupleDataCollection-equivalent L3 row store |
| `core/parallel_dsa_bridge.cpp` | REFACTOR | §8.7 — replace direct `dsa_allocate`/`dsa_get_address` with `BufferManager::Allocate`/`Pin` (first migration target) |

---

## 8.7 Query-scoped BufferManager (DuckDB-faithful, four-layer data plane)

> **⚠️ STALE (2026-04-25, M-FRAME-MIN ownership-split decision).** §8.7 below
> predates the §8.5 DSM-key collapse. Internal references to
> `PipelineKey(/*pid=*/-1, BUFFER_BLOCK)`, `PipelineKeyKind::BUFFER_BLOCK`,
> dynamic per-pipeline DSM keys, and the `PipelineKeyKind` enum are all
> superseded — see §6 STALE banner. The whole BufferManager L2/L3/L4 design is
> **deferred** out of M-FRAME-MIN; it is a M-Q1-PERF / post-Q1 milestone
> (`P3X-M-BM`). When BufferManager lands, its block table will be reachable via
> a `dsa_pointer` on `PipelineSharedControl` (mirroring `pipelines_root`), not
> via a per-block toc key. Do not implement §8.7 as written.

> **Added 2026-04-24.** Per user lock: "for the global state, I think there need a BufferManager as duckdb. 可以用这个包装一层dsm/dsa来做pipeline之间的数据传递，以及做swizzle/unswizzle来避免更多序列化的代价。还有就是这个也考虑做成QueryLevel的". User selected **full DuckDB-faithful scope**: Allocate + Pin/Unpin + per-query memory accounting + spill to existing `SharedFileSet`. ~1.5k LOC. Enables correctness under memory pressure for TPC-H scale factors >10G and unblocks zero-copy cross-pipeline handoff (§8.4 dependency-DAG already establishes the *when*; this section establishes the *how*).

### 8.7.0 Layered data-plane model

§8.1–§8.6 above defines the **control plane** (IR, MetaPipeline, scheduler, events, task queue). §8.7 defines the **data plane** — how a sink-pipeline's materialized output physically lives in memory such that the next pipeline's source can scan it zero-copy from any worker process.

| Layer | Purpose | DuckDB analog | pg_volvec name |
|---|---|---|---|
| **L4 Handoff** | `sink_state` exposed via `op.sink_state`; next pipeline's `Source.GetGlobalSourceState` reads it directly; `Vector::Reference()` (no memcpy) | `physical_hash_aggregate.cpp:193-247` `HashAggregateGlobalSinkState`; `radix_partitioned_hashtable.cpp:889-940` zero-copy reference | unchanged — lives on `PhysicalOperator` per §8.1 |
| **L3 Row store** | row+heap block collection; swizzle metadata; pin/unpin semantics for variable-length payloads | `tuple_data_collection.{hpp,cpp}`, `tuple_data_allocator.hpp:92-96` `SwizzleMetaData` | `PipelineRowCollection` |
| **L2 BufferManager** | allocate/pin/unpin blocks; DSA-pointer ⇄ local-VA swizzle; per-query memory budget; spill to `SharedFileSet` when budget exceeded | `buffer_manager.hpp:25-141`, `standard_buffer_manager.{hpp,cpp}`, `buffer_handle.hpp`, `block_handle.hpp` | **`PipelineBufferManager`** |
| **L1 Primitives** | DSA arena; spill file storage | (DuckDB owns its own block I/O) | existing `dsa_*` (per-query, `pipeline_leader.cpp:237`) + `SharedFileSet*` (`pipeline_leader.cpp:231`) |

Today (verified): `DsaDataChunkBridge` collapses L2+L3+L4 into one — it serializes each `DataChunk` into flat DSA-allocated bytes and re-deserializes on read. **The serialization cost the user wants to eliminate is the L2+L3 collapse**. §8.7 splits them.

### 8.7.1 BufferManager API (DuckDB-faithful surface)

New header `pipeline/buffer_manager.hpp`:

```cpp
namespace pg_volvec::pipeline {

class BufferHandle;       // RAII Pin handle (8.7.2)
class BlockHandle;        // refcounted block descriptor (8.7.2)

// One instance per query, owned by the leader's scheduler. Workers attach via DSM
// and obtain a thin per-process BufferManagerProxy that forwards to the shared
// in-DSM control structure. Lifetime = exactly one query (matches existing DSA
// per-query lifetime; no cross-query reuse — user lock).
class PipelineBufferManager {
public:
    // -- Construction (leader-only) ----------------------------------------
    // backing_dsa: existing per-query dsa_area* from pipeline_leader.cpp:237
    // spill:        existing SharedFileSet* from pipeline_leader.cpp:231
    // max_memory:   GUC pg_volvec.query_memory_limit (bytes); 0 = unlimited
    static std::unique_ptr<PipelineBufferManager>
    Create(dsa_area *backing_dsa, SharedFileSet *spill, uint64 max_memory);

    // -- Allocation --------------------------------------------------------
    // Allocate a fixed-size block; returns BufferHandle pinned (refcount=1).
    // May trigger eviction if (used_memory_ + size > max_memory_).
    // size MUST be a multiple of BLOCK_ALIGN (=64); max BLOCK_SIZE (=256KB).
    BufferHandle Allocate(uint64 size, MemoryTag tag = MemoryTag::PIPELINE_INTERMEDIATE);

    // Allocate a small (sub-block) buffer that lives in a shared block's free space.
    // Used by row-store heap pages that are <BLOCK_SIZE.
    BufferHandle RegisterSmallMemory(uint64 size, MemoryTag tag);

    // Re-pin an unpinned block by handle id. If evicted, reads from SharedFileSet.
    // Performs swizzle pass: rewrites all in-block pointers from spill-relative
    // offsets back to local VAs (§8.7.3).
    BufferHandle Pin(const BlockHandle &block);

    // Unpin: decrements refcount. When refcount hits 0, block becomes
    // eviction-eligible (LRU queue). Unpinned blocks remain readable until
    // evicted; eviction performs swizzle: rewrites local VAs to spill-relative
    // offsets, then writes to SharedFileSet, then frees DSA backing.
    void Unpin(BlockHandle &block);

    // Reallocate (DuckDB parity): grow/shrink an existing buffer. May copy.
    BufferHandle Reallocate(BufferHandle handle, uint64 new_size);

    // -- Accounting --------------------------------------------------------
    uint64 GetUsedMemory()  const { return pg_atomic_read_u64(&used_memory_); }
    uint64 GetMaxMemory()   const { return max_memory_; }
    uint64 GetSpilledBytes() const { return pg_atomic_read_u64(&spilled_bytes_); }

    // -- Worker attach (DSM-backed) ----------------------------------------
    // Workers call this after dsa_attach_in_place; returns a process-local proxy
    // that shares the leader's BlockHandle table via DSM key
    // PipelineKey(/*pid=*/-1, BUFFER_BLOCK).
    static std::unique_ptr<PipelineBufferManager>
    Attach(dsa_area *backing_dsa, SharedFileSet *spill, shm_toc *toc);

private:
    dsa_area               *backing_dsa_;
    SharedFileSet          *spill_;
    uint64                  max_memory_;
    pg_atomic_uint64        used_memory_;
    pg_atomic_uint64        spilled_bytes_;
    // Block table: dense array of BlockHandle indexed by BlockId, allocated in DSA.
    // Lookup is O(1); evictable blocks tracked in a separate LRU MPMC queue.
    dsa_pointer             block_table_;     // BlockHandle[block_capacity_]
    uint32                  block_capacity_;
    pg_atomic_uint32        next_block_id_;
};

}  // namespace
```

**API parity with DuckDB** (verified `src/include/duckdb/storage/buffer_manager.hpp:25-141`): `Allocate`, `Pin`, `Unpin`, `Reallocate`, `RegisterSmallMemory`, `GetUsedMemory`, `GetMaxMemory` — present. `RegisterMemory`, `GetTemporaryDirectory/SetTemporaryDirectory`, `GetBufferAllocator` — omitted (PG owns memory directories via `SharedFileSet`; allocator is `dsa_allocate`).

### 8.7.2 BufferHandle + BlockHandle (RAII semantics)

```cpp
// In DSM (shared across leader + all workers); one entry per allocated block.
struct BlockHandle {
    uint32              block_id;          // dense, 0..next_block_id_-1
    pg_atomic_uint32    pin_count;         // 0 → eviction-eligible
    uint64              size;              // bytes
    MemoryTag           tag;
    enum class State : uint8 { LOADED, UNLOADED, EVICTED } state;

    // Backing storage (one of the three is valid based on `state`):
    dsa_pointer         dsa_ptr;           // LOADED: live DSA allocation
    SharedFileSetHandle spill_handle;      // EVICTED: file-set entry
    // (UNLOADED == LOADED but pin_count==0; identical layout)

    // Swizzle metadata: see §8.7.3
    SwizzleVector       swizzles;          // dsa_pointer to in-block fixup table
};

// Per-process RAII; non-copyable, movable.
class BufferHandle {
public:
    BufferHandle() : block_(nullptr), local_ptr_(nullptr) {}
    BufferHandle(BufferHandle &&other) noexcept;
    BufferHandle &operator=(BufferHandle &&other) noexcept;
    ~BufferHandle() {
        if (block_) bm_->Unpin(*block_);   // RAII unpin — Oracle B1 ERROR-safety
    }
    void *Ptr() const { return local_ptr_; }   // process-local VA, unswizzled
    template<typename T> T *Ptr() const { return static_cast<T*>(local_ptr_); }
    BlockHandle &block() const { return *block_; }
    bool IsValid() const { return block_ != nullptr; }

private:
    PipelineBufferManager *bm_;
    BlockHandle           *block_;
    void                  *local_ptr_;     // dsa_get_address(backing_dsa_, block_->dsa_ptr)
};
```

**ERROR-safety integration with Oracle B1 (§8.4):** `BufferHandle`'s destructor runs `Unpin` automatically on scope exit. When `ereport(ERROR)` longjmps past C++ destructors, the handle is *not* unpinned — but because all `BufferHandle`s used inside a task are held in the per-task MemoryContext (per §8.4 contract), the MemoryContext destruction in `PG_CATCH` invokes our placement-new'd dtors via a memory-context callback registered at `Allocate` time. Net: refcount integrity survives ERROR.

### 8.7.3 Swizzle protocol (two distinct problems)

**Problem A — String/heap pointers inside a row (DuckDB-style):** when a block containing variable-length values (`varchar`, `numeric` overflow, `Wide128` interpreter slow path) is evicted+reloaded, in-row pointers must be recomputed.

```cpp
// Per-block fixup table; lives at the head of each row block.
struct SwizzleEntry {
    uint32  vector_offset;   // byte offset within the block of the vector header
    uint32  child_block_id;  // BlockId of the heap block (0xFFFFFFFF = self/inline)
    uint32  count;           // number of pointers in this vector
};
struct SwizzleVector {
    uint32        n;
    SwizzleEntry  entries[];   // flexible array
};
```

On `Unpin → Evict → Spill`: walk `swizzles`, for each pointer write `(child_block_id, offset_within_child)` instead of the raw VA. On `Pin → Reload`: walk `swizzles`, for each pointer compute `dsa_get_address(child_block_id) + offset_within_child`. Mirrors DuckDB `column_data_allocator.cpp:219-256` `RecomputeHeapPointers`.

**Problem B — DSA-pointer ⇄ local-VA (PG-specific):** a `dsa_pointer` is a stable handle, but `dsa_get_address(dsa, ptr)` returns a *different local VA in every process*. This is the exact PG analog of DuckDB's "different pinned address after eviction." Solution: **store `dsa_pointer` (or `(BlockId, offset)`) in any in-row reference; never store a raw `T*`**. The `Pin` path resolves to local VA on the consuming process. The `Unpin` path is a no-op for Problem B (DSA pointers are already process-independent).

**Why this lets `Vector::Reference()` work zero-copy across worker processes (the user's core ask):** the `DataChunk` produced by Pipeline P0's sink stores `dsa_pointer` (not raw `T*`) for every variable-length payload. P1's source, running on a *different* worker process, calls `Pin` on the relevant `BlockHandle` → BufferManager calls `dsa_get_address` in P1's process → returns P1's local VA → `Vector::Reference()` points the output `DataChunk`'s data buffer to that VA. **No serialization, no memcpy** — exactly DuckDB's `radix_partitioned_hashtable.cpp:923,931-932` semantics.

### 8.7.4 Memory accounting + eviction policy

- `used_memory_` is an atomic counter incremented in `Allocate`, decremented in `Evict`. Compared against `max_memory_` (GUC `pg_volvec.query_memory_limit`, default = `work_mem * parallel_max_workers`).
- Eviction queue: MPMC ring of `BlockId`s with `pin_count == 0`, populated on `Unpin`.
- `Allocate(size)` when `used_memory_ + size > max_memory_`: pop from eviction queue, evict (write to `SharedFileSet`, free DSA backing, set state=EVICTED), repeat until space free or queue empty.
- Queue empty + still over budget → `ereport(ERROR, "pg_volvec: query exceeded memory limit; cannot evict pinned blocks")`. This is the only memory-limit ERROR path; sets `control->worker_error` per Oracle B1.

**TemporaryMemoryState parity:** DuckDB's `TemporaryMemoryState` (verified `sort.cpp:159,162-202`) provides per-operator dynamic memory budgeting on top of BufferManager. P3X scope explicitly excludes this; revisit when adding parallel sort (post-P3X, behind GUC `pg_volvec.parallel_sort`).

### 8.7.5 Spill via existing SharedFileSet

- The `SharedFileSet` already wired at `pipeline_leader.cpp:231` (leader) + `pipeline_worker_main.cpp:50` (worker) is the spill backing. **No new file infrastructure.**
- `Evict(BlockHandle &b)`:
  1. Walk `b.swizzles`, rewrite all pointers to `(BlockId, offset)` form (Problem A above).
  2. `BufFile *bf = SharedFileSetCreate(spill_, FormatBlockFilename(b.block_id))`.
  3. `BufFileWrite(bf, dsa_get_address(backing_dsa_, b.dsa_ptr), b.size)`.
  4. `dsa_free(backing_dsa_, b.dsa_ptr)`. Set `b.state = EVICTED`. Decrement `used_memory_`. Increment `spilled_bytes_`.
- `Reload(BlockHandle &b)`:
  1. `b.dsa_ptr = dsa_allocate(backing_dsa_, b.size)`.
  2. `BufFileRead(SharedFileSetOpen(spill_, …), dsa_get_address(backing_dsa_, b.dsa_ptr), b.size)`.
  3. Walk `b.swizzles`, rewrite `(BlockId, offset)` → local VA.
  4. Set `b.state = LOADED`. Increment `used_memory_`.

### 8.7.6 Migration: `DsaDataChunkBridge` → BufferManager

The single existing DSA consumer (`core/parallel_dsa_bridge.cpp`) is THE first migration target. Today's flow:

```
sink: serialize DataChunk → dsa_allocate(bytes) → memcpy
src:  dsa_get_address → memcpy → deserialize into DataChunk
```

becomes:

```
sink: BufferHandle h = bm.Allocate(size);
      memcpy(h.Ptr(), chunk.row_data, size);   // single copy, no serialization
      record (h.block().block_id, swizzles) on the GlobalSinkState
src:  BufferHandle h = bm.Pin(block_handle);
      output_chunk.Reference(h.Ptr(), …);     // ZERO copy
```

Net wins: (1) one memcpy at sink instead of two (serialize+memcpy); (2) zero copies at source instead of (memcpy+deserialize); (3) memory pressure handled via spill instead of OOM-ERROR; (4) cross-pipeline handoff becomes the L4 mechanism from §8.7.0 — sink stores `BlockHandle` IDs on its `GlobalSinkState`, source's `GetGlobalSourceState` reads them, `Pin` resolves them in the source's process.

### 8.7.7 New DSM key

Add to `PipelineKeyKind` in §8.5:

```cpp
enum class PipelineKeyKind : uint16 {
    SOURCE_STATE   = 1,
    SINK_STATE     = 2,
    SPILL_FILESET  = 3,
    BUFFER_BLOCK   = 4,   // NEW: BlockHandle table for PipelineBufferManager
};
```

The `BUFFER_BLOCK` key is allocated with `pid = -1` (query-global, not pipeline-scoped) since BufferManager is one-per-query. Lookup via `PipelineDsmLookup<BlockTableHeader>(toc, PipelineKey(-1, BUFFER_BLOCK), "buffer_block_table")` — same wrapper from Oracle B4 (§8.5).

### 8.7.8 Acceptance gates (folded into §15 P3X)

A new milestone **P3X-M-BM** lands BufferManager and is acceptance-gated by:

- [ ] `PipelineBufferManager::{Allocate,Pin,Unpin,Reallocate,RegisterSmallMemory,GetUsedMemory,GetMaxMemory}` all implemented and exercised.
- [ ] `DsaDataChunkBridge` rewritten on top of BufferManager; raw `dsa_allocate`/`dsa_get_address` only inside BufferManager (grep gate `QA-P3X-8`).
- [ ] Q1 + Q6 pass with `pg_volvec.query_memory_limit = 0` (unlimited).
- [ ] Q1 + Q6 pass with `pg_volvec.query_memory_limit = 64MB` (forces spill on TPC-H 10G); `GetSpilledBytes()` > 0 verified via `NOTICE`.
- [ ] No `Vector::Reference`-incompatible memcpy in the Sink→Source path (visual inspection + grep on `memcpy` inside `physical_hash_aggregate.cpp` source-side).
- [ ] ERROR injected mid-Allocate leaves `used_memory_` consistent (refcount integrity test).

---

## 9. Open Questions

1. **DSA region sizing**. `dsa_minimum_size()` is currently used (`pipeline_leader.cpp:165`). Multi-pipeline + per-source `next_block` + per-sink `slots` array + Sort run buffers may exceed it. Should we estimate `Σ pipeline.dsa_estimate()` at lowering time and pass that as `dsa_create_in_place` size? **Proposed answer**: Yes, in M3. Each `GlobalSinkState`/`GlobalSourceState` subclass declares a static `EstimateDsaBytes(idx_t max_threads)` that the leader sums.
2. **Pipeline ownership of `unique_ptr<GlobalSinkState>` across worker boundary**. The worker reconstructs a *view* of the global sink state from DSM, not the same `unique_ptr`. Is this confusing? **Proposed answer**: Document explicitly — the leader's `GlobalSinkState` instance is the canonical one; workers see a derived view. Same model as DuckDB.
3. **Should `SortGlobalSinkState` use `optional_idx`-style batch ordering** (DuckDB `SourcePartitionInfo`)? **Proposed answer**: No for Q1 (no order-preserving join below). Yes for future order-by-with-ties or window functions; revisit when needed.
4. **`StateWithBlockableTasks` parent**. DuckDB inherits from this for async-blocking. **Proposed answer**: Skip in P3 (non-goal NG3). Leave room: when added in P3e, `GlobalSinkState` and `GlobalSourceState` gain a parent class, no field name collisions expected.
5. **Migration step ordering vs P3-0 follow-up bugs** (`inline partial merge failed`, `LLVMJitContext in use count not 0`). The first is suspected to be M4's `SetSharedSlots` race; the second is leader-side JIT refcount leak when `ereport(ERROR)` longjmps past C++ destructors. **Proposed answer**: Fix the first as part of M4 (root cause + abstraction cleanup in one step). The JIT refcount leak is orthogonal — fix in P3a as a precursor, separately.

---

## 10. Acceptance Criteria

This design is accepted when ALL hold:

- [ ] Every cross-process state item from §7 has a named future owner.
- [ ] Lifecycle (§4) is unambiguous: every Get/Combine/Finalize call has a single defined caller and ownership transfer.
- [ ] DSM key scheme (§6) admits N pipelines without collision and without growing `PipelineSharedControl`.
- [ ] Sort sink/source (§5.3) interface signature is concrete enough that an engineer can implement M5 without further design.
- [ ] Migration steps (§8) are individually shippable (each step compiles + Q1+Q6 pass).
- [ ] Oracle R2 + Momus R6 review on combined plan (`pipeline-port-plan.md` v6 + this doc) returns APPROVED or APPROVE-WITH-CHANGES.

---

## 11. Out of Scope (Explicit Reminders)

- HashJoin (Greenfield Plan B; do not design).
- TPC-H Q2–Q22.
- Async/blocking task machinery (`BLOCKED` propagation, `StateWithBlockableTasks`).
- Distributed `FinalAgg` (DuckDB's "finalize as new pipeline" hash-agg pattern).
- Multi-MetaPipeline DAGs.
- Window functions, set ops, lateral joins.

---

## 12. References

- DuckDB `src/include/duckdb/execution/physical_operator_states.hpp` @ `5af191ce…` — base class definitions.
- DuckDB `src/execution/operator/aggregate/physical_hash_aggregate.cpp:193-247` — `HashAggregateGlobalSinkState`/`LocalSinkState` reference.
- DuckDB `src/execution/operator/order/physical_order.cpp:16-70` — `OrderGlobalSinkState`/`LocalSinkState` reference.
- DuckDB `src/execution/operator/scan/physical_table_scan.cpp:31-94` — `TableScanGlobalSourceState`/`LocalSourceState` reference.
- pg_volvec inventory of cross-process state — see source citations in §7 column "Today's site".
- `.sisyphus/plans/pipeline-port-plan.md` §6, §7, §8, §15 — phasing context.
- `contrib/pg_volvec/src/engine/parallel/pipeline/AGENTS.md` — pipeline runtime knowledge base (ANTI-PATTERNS section enforces several rules this design relies on).

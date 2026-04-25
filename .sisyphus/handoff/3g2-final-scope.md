# 3g.2-final Handoff (Step 5–14)

**Created:** 2026-04-25
**Predecessor commit:** TBD (this session's `pg_volvec: 3g.2-prep MetaPipeline runtime infra` checkpoint — fill in after `git commit`)
**Predecessor design lock:** `eb7901b022a` (`docs/GLOBAL_LOCAL_STATE_DESIGN.md` + mirror)
**Goal of next session:** complete M-FRAME-MIN Step 3 → MetaPipeline runtime fully wired, Q1 plan still falls back to standard PG (Q1 shape-matcher is Step 4 / M-Q1-PERF, NOT this session). After 3g.2-final, `LeaderSerializePipelines` walks a real bundle, workers reconstruct, scheduler dispatches, but Translator still returns `nullptr` for every plan → bridge still WARNING-fallbacks. The only observable change at end of 3g.2-final = a unit-style smoke that hand-builds a `MetaPipelineBundle` and round-trips it through DSA must pass.

---

## 0. Read-Before-Anything-Else

| File | Why |
|------|-----|
| This file (`.sisyphus/handoff/3g2-final-scope.md`) | full scope; supersedes any older handoff |
| `.sisyphus/plans/3g2-delta-map.md` | per-step delta map (Phase C output, Step 1–7 detail). Step 1+2+3 portions are now DONE; Step 5–14 portions still authoritative |
| `docs/GLOBAL_LOCAL_STATE_DESIGN.md` §6.3, §8.5.1–§8.5.4, §8.5.4.7, §8.7 | ABI & layout source of truth (lock `eb7901b022a`) |
| `docs/PIPELINE_PORT_PLAN.md` §15.3.2 (L1940–1961) | 3g.2 acceptance gate; §15.3.2 L1957 = "no stack-RAII Local* inside PG_TRY; use `core::PgMcxtCallbackGuard`" |
| `contrib/pg_volvec/src/engine/parallel/pipeline/AGENTS.md` | anti-patterns. **STALE on counts/status** (claims 22 files / `fd9a8aaf326` HEAD; reality: 24 files post-3g.2-prep, HEAD will be the new checkpoint). DO NOT MODIFY — anti-patterns remain authoritative |
| `contrib/pg_volvec/AGENTS.md` | top-level KB. **STALE on TU count** (claims 13; reality 23 post-3g.2-prep). DO NOT MODIFY |
| `contrib/pg_volvec/src/engine/core/AGENTS.md` | `PgMcxtCallbackGuard`, `PgVector`, `VolVecVector`, `PgMemoryContextObject`, `PgMemoryContextAllocator` reference |
| `contrib/pg_volvec/src/bridge/AGENTS.md` | bridge-side ABI (`query_state.hpp` 3 void* slots) |

---

## 1. What 3g.2-prep Already Landed (DO NOT redo)

### 1.1 Files modified
- `contrib/pg_volvec/meson.build` — added `src/engine/parallel/pipeline/pipeline_descriptor.cpp` (22 → 23 active TU).
- `contrib/pg_volvec/src/engine/parallel/pipeline/dsm_control.hpp` — collapsed legacy 9 keys → 3 keys (`CONTROL=0xD800000000000001`, `DSA=0xD800000000000008`, `TASK_QUEUE=0xD800000000000009`); `PipelineSharedControl` now `{ uint32 magic, int32 num_pipelines, pg_atomic_uint32 worker_error, dsa_pointer pipelines_root }`; `PIPELINE_DSM_MAGIC = 0x56505043`. Old `PIPELINE_DSM_KEY_{PLANNEDSTMT,QUERY_TEXT,PARTIALS,SOURCE_PSCAN,PARTIAL_FILESET,PARAM_EXEC}` and the old `0x0002..0x0007` IDs are **retired and MUST NOT be re-used**.
- `contrib/pg_volvec/src/engine/parallel/pipeline/pipeline_descriptor.hpp` — NEW, ≈260 LoC POD layout (see §2 below).
- `contrib/pg_volvec/src/engine/parallel/pipeline/pipeline_descriptor.cpp` — NEW, skeleton bodies (all `ereport(ERROR, ERRCODE_FEATURE_NOT_SUPPORTED)` except `LeaderSerializePipelines` empty-bundle smoke path which returns `InvalidDsaPointer`).

### 1.2 Behavioral state
- `Translator::Translate` still returns `nullptr` for every plan (unchanged).
- Bridge still WARNING-fallbacks every query to `standard_ExecutorRun` (unchanged).
- Q1 smoke (`./installed/bin/psql -h /tmp -p 5432 -d tpch -f contrib/pg_volvec/sql/q1.sql`) still produces 2 rows (A/F + B/O).
- Workers still `elog(ERROR)` from `pipeline_worker_main.cpp` stub (unchanged).
- Compile: 23 TU, clean (0 warnings, 0 errors). Link: `pg_volvec.dylib` builds.

### 1.3 Phase A discovery (correction to delta map)
**The 4 op `.hpp` files are already P1 fully-built**, contrary to the original delta map's claim that Step 5/6/7 needed substantial header edits. Reality:
- `physical_seq_scan.hpp` — ctor + fields + factory decls + `IsSource()`/`IsSink()`/`IsPipelineBreaker()`/`MaxThreads()` all present.
- `physical_hash_aggregate.hpp` — same.
- `physical_order.hpp` — same.
- `output_sink.hpp` — same.

Therefore Step 5/6/7 in 3g.2-final are **smaller than the delta map predicts**. The remaining header edits in 3g.2-final are narrow (see §3.4 below).

---

## 2. POD Layout in pipeline_descriptor.hpp (3g.2-prep, frozen)

This is the cross-process IR. Workers reconstruct `Pipeline` / `PhysicalOperator` instances by reading these PODs out of DSA. **Do NOT change layout in 3g.2-final** without revising §8.5.4 in the design doc and re-locking.

```
namespace pg_volvec::pipeline {

enum class OpKind : uint8_t {
    SEQ_SCAN = 0, HASH_AGGREGATE = 1, ORDER = 2, OUTPUT = 3
};

struct ColumnSchema { Oid type_oid; int16_t typlen; bool typbyval; };

struct SchemaDescriptor {
    uint32_t      n_columns;
    uint32_t      _pad0;
    ColumnSchema  columns[FLEXIBLE_ARRAY_MEMBER];
};

struct ExprBytecode {
    uint32_t n_insns;
    uint32_t n_consts;
    uint32_t const_pool_bytes;
    uint32_t _pad;
    /* trailing layout: insns[n_insns]; consts[n_consts]; const_pool[const_pool_bytes] */
};

/* DSA-resident raw payloads (§6.3) */
struct SeqScanSharedPayload {
    pg_atomic_uint64 next_block;
    BlockNumber      total_blocks;
    uint32_t         morsel_nblocks;
};
struct AggSharedPayload;   /* fwd-decl only in 3g.2-prep; full layout in 3g.2-final */
struct SortSharedPayload;  /* fwd-decl only in 3g.2-prep; full layout in 3g.2-final */

struct AggFuncDesc {
    Oid agg_oid; Oid transtype; Oid finaltype;
    uint16_t input_col_idx; uint16_t _pad;
};
struct SortKeyDesc {
    Oid collation_oid; uint16_t col_idx;
    bool asc; bool nulls_first; uint32_t _pad;
};

/* Per-op bodies, all dsa_pointer fields point to DSA-resident POD */
struct SeqScanOpBody {
    Oid         relid;
    dsa_pointer output_schema;       /* SchemaDescriptor */
    dsa_pointer projection_indices;  /* AttrNumber[] */
    dsa_pointer qual_program;        /* ExprBytecode (or InvalidDsaPointer for null qual) */
    dsa_pointer scan_shared_payload; /* SeqScanSharedPayload */
};
struct HashAggOpBody {
    dsa_pointer input_schema;  /* SchemaDescriptor */
    dsa_pointer output_schema; /* SchemaDescriptor */
    dsa_pointer group_keys;    /* uint16_t[n_group_keys] */
    dsa_pointer agg_funcs;     /* AggFuncDesc[n_agg_funcs] */
    uint16_t    n_group_keys;
    uint16_t    n_agg_funcs;
    uint32_t    _pad;
};
struct OrderOpBody {
    dsa_pointer input_schema;  /* SchemaDescriptor */
    dsa_pointer output_schema; /* SchemaDescriptor */
    dsa_pointer sort_keys;     /* SortKeyDesc[n_sort_keys] */
    uint16_t    n_sort_keys;
    uint16_t    _pad0;
    uint32_t    _pad1;
};
struct OutputOpBody {
    dsa_pointer input_schema;  /* SchemaDescriptor */
};

/* 4-children inline; deeper trees out-of-scope for M-FRAME-MIN */
struct OpDescriptor {
    OpKind   kind;
    uint8_t  n_children;
    uint16_t child_indices[4];          /* indices into the pipeline's ops array */
    /* Lazy-published §6.3 payloads (Sink/Source GetGlobalState writes here) */
    dsa_pointer global_sink_state;      /* dsa_pointer to AggSharedPayload / SortSharedPayload */
    dsa_pointer global_source_state;    /* dsa_pointer to SeqScanSharedPayload */
    union {
        SeqScanOpBody  seq_scan;
        HashAggOpBody  hash_agg;
        OrderOpBody    order;
        OutputOpBody   output;
    } body;
};

struct PipelineDescriptor {
    uint16_t          pipeline_id;
    uint16_t          op_count;
    uint32_t          dependency_mask;  /* bit i set => depends on pipeline_id i */
    dsa_pointer       ops;              /* OpDescriptor[op_count] */
    pg_atomic_uint32  task_slot_next;   /* worker slot ticket */
};

/* Helper API (3g.2-prep decl-only; bodies stub-ereport in pipeline_descriptor.cpp) */
void        StoreSharedPayloadOnDescriptor(const PhysicalOperator *op, dsa_pointer dp);
dsa_pointer LoadSharedPayloadFromDescriptor(const PhysicalOperator *op);

/* Entry points (3g.2-prep decl + stub-ereport bodies; real bodies in 3g.2-final) */
dsa_pointer LeaderSerializePipelines(MetaPipelineBundle &bundle, dsa_area *dsa);
void        WorkerReconstructPipelines(PipelineSharedControl                 *ctl,
                                       ExecCtx                               &worker_ctx,
                                       PgVector<std::unique_ptr<Pipeline>>   &out);

}  /* namespace pg_volvec::pipeline */
```

**4 ExprBytecode constraints (§8.5.4.7):**
1. integer-index operands (no pointers in bytecode)
2. by-value (or by-bytes-copied) constants only (no palloc'd objects)
3. function dispatch via Oid → `fmgr_info_cxt` (workers re-resolve)
4. host byte order (single-host parallel only; intentionally not portable)

---

## 3. Step 5–14 Scope (3g.2-final, this is the next commit)

### 3.1 Order of operations (suggested, dependency-driven)

| # | File(s) | Action | Notes |
|---|---------|--------|-------|
| 5 | `physical_seq_scan.{hpp,cpp}` | EDIT hpp: migrate `PhysicalSeqScanShared.next_block` from raw `pg_atomic_uint64*` to in-`SeqScanSharedPayload` inline atomic; ADD `dsa_pointer shared_payload_dp_` field if not already present. WRITE cpp body: `Init()` JIT wiring deferred to M-Q1-PERF; for now `Init()` no-ops, `GetData()` morsel pull from DSA payload | Source side |
| 6 | `physical_hash_aggregate.{hpp,cpp}` | EDIT hpp: ADD `dsa_pointer shared_payload_dp_` field. WRITE cpp body: `GetGlobalSinkState()` calls `StoreSharedPayloadOnDescriptor` on first call (leader, then workers attach via `LoadSharedPayloadFromDescriptor`); `Sink()`/`Combine()`/`Finalize()` minimal correct paths | Sink + Source |
| 7 | `physical_order.{hpp,cpp}` | hpp zero diff. WRITE cpp body: `MaxThreads()=1`; `Sink()`/`Finalize()`/`GetData()` minimal | Sink + Source |
| 8 | `output_sink.{hpp,cpp}` | EDIT hpp: ADD `DestReceiver *dest_` field. WRITE cpp body: `Sink()` materializes PipelineChunk → PG slot → `dest_->receiveSlot`. Bridge MUST NOT do this | Sink only |
| 9 | `pipeline_descriptor.cpp` (REAL bodies) | REPLACE all 4 stub bodies with real implementations: `LeaderSerializePipelines` walks bundle, lowers each Pipeline + ops to DSA-resident PODs; `WorkerReconstructPipelines` reverses; Store/Load helpers wire `OpDescriptor.global_sink_state` / `global_source_state` slots. ExprProgram lowering (§8.5.4.7) for null-qual returns `InvalidDsaPointer`; non-null qual is **deferred to M-Q1-PERF** (still ereport in 3g.2-final, intentional scope cut) | Cross-process IR |
| 10 | `task_scheduler.{hpp,cpp}` | EDIT-and-wire: connect to DSM `TaskQueue` (Vyukov MPMC in `dsm_task_queue.{hpp,cpp}` partial impl); 3-event protocol (Run/Combine/Finalize) | Single `LaunchParallelWorkers` per Oracle B7 |
| 11 | `pipeline_run_event.cpp` / `pipeline_combine_event.cpp` / `pipeline_finalize_event.cpp` | REWRITE all 3 stubs with real event bodies | Each ~18 LoC stub today |
| 12 | `task.cpp` | REWRITE the 49-LoC stub: 5 task types `PipelineInitializeTask` / `PipelineTask` / `PipelinePrepareFinishTask` / `PipelineFinishTask` / `PipelineCompleteTask` | TaskExecutionResult: `TASK_FINISHED` / `TASK_NOT_FINISHED` / `TASK_ERROR` (NO `BLOCKED`) |
| 13 | `pipeline_leader.cpp` | REWRITE the 33-LoC stub: enter via `PgvolvecPipelineRun(QueryDesc*, PgVolVecQueryState*, const char**)`; PG_TRY-safe; calls `LeaderSerializePipelines`, sets `PipelineSharedControl.pipelines_root`, single `LaunchParallelWorkers`, drives event DAG | Use `core::PgMcxtCallbackGuard` for stack-safe cleanup (NO stack-RAII Local* per §15.3.2 L1957) |
| 14 | `pipeline_worker_main.cpp` | REWRITE the 28-LoC stub: signature `pg_volvec_pipeline_worker_main(dsm_segment*, shm_toc*)`; `dsm_attach` + `shm_toc_lookup_or_error` for the 3 keys; `dsa_attach`; `MemoryContextSwitchTo` long-lived per-worker context BEFORE constructing op state; call `WorkerReconstructPipelines`; run scheduler; on any error path set `PipelineSharedControl.worker_error` BEFORE `ereport(ERROR)` | Anti-pattern: NEVER bare `elog(ERROR)` without setting worker_error first |
| 15 | `translator.cpp` | REWRITE the 35-LoC stub: generic recursive walker; non-Q1 plan → `ereport(ERROR)` caught by bridge PG_TRY/CATCH → bridge logs WARNING and falls back to `standard_ExecutorRun`. Q1 shape-matching itself is M-FRAME-MIN Step 4, NOT 3g.2-final — but the **walker scaffolding** lands here so Step 4 only adds shape-detection logic | Translator stays pure (Plan → IR), zero JIT coupling |

### 3.2 Files NOT to touch in 3g.2-final
- All `AGENTS.md` (per user constraint)
- `physical_operator.hpp`, `source.hpp`, `sink.hpp`, `operator.hpp`, `types.hpp`, `pipeline.hpp`, `meta_pipeline.hpp`, `task.hpp`, `event.hpp`, `task_scheduler.hpp` — base headers, signatures frozen
- `query_state.hpp` — bridge ABI; only edit jointly with `bridge/state.c`
- `dsm_control.hpp` — frozen at 3g.2-prep, 3 keys only
- `pipeline_descriptor.hpp` — POD layout frozen at 3g.2-prep

### 3.3 New TU count after 3g.2-final
23 (current) + 0 new files (all 3g.2-final work edits existing TUs). meson.build untouched.

### 3.4 ABI/Field deltas in 3g.2-final (vs 3g.2-prep header state)
1. `PhysicalSeqScanShared.next_block`: raw `pg_atomic_uint64*` → inline-in-`SeqScanSharedPayload` (DSA-resident).
2. `physical_hash_aggregate.hpp`: ADD `dsa_pointer shared_payload_dp_`.
3. `output_sink.hpp`: ADD `DestReceiver *dest_`.
4. `physical_order.hpp`: zero diff.

---

## 4. Acceptance Gates (3g.2-final QA)

From `docs/PIPELINE_PORT_PLAN.md` §15.3.2 + Phase C delta map:

- **P3X-3**: 3-event Run/Combine/Finalize protocol exercised (unit smoke that hand-builds a bundle and round-trips it).
- **P3X-5**: worker error propagation — `worker_error` slot set before any ereport from worker; leader observes and surfaces.
- **P3X-7**: `core::PgMcxtCallbackGuard` used for all stack-safe cleanup in PG_TRY paths; zero Local* RAII inside PG_TRY.

End-of-3g.2-final smoke that MUST pass:
```bash
CCACHE_DISABLE=1 PATH=/opt/homebrew/bin:$PATH meson compile -C build pg_volvec
CCACHE_DISABLE=1 PATH=/opt/homebrew/bin:$PATH meson install -C build --only-changed
./installed/bin/pg_ctl -D ~/data/pg_tpch restart -m fast -l ~/data/pg_tpch/logfile
./installed/bin/psql -h /tmp -p 5432 -d tpch -f contrib/pg_volvec/sql/q1.sql
# expected: WARNING fallback + 2 rows (A/F, B/O) — same as 3g.2-prep, since
# Translator still returns nullptr for every plan. Q1 actually entering the
# vectorized path is M-FRAME-MIN Step 4 / M-Q1-PERF.
```

---

## 5. Anti-Patterns (re-stated, MUST uphold)

From `pipeline/AGENTS.md` + `core/AGENTS.md` + `bridge/AGENTS.md`:
- NO worker `elog(ERROR)` without first setting `PipelineSharedControl.worker_error`.
- NO publish palloc'd pointers via DSA; use `dsa_allocate` + `dsa_get_address`.
- NO operator returns `BLOCKED` in M-FRAME-MIN; runtime asserts.
- Workers MUST `MemoryContextSwitchTo` long-lived per-query context BEFORE constructing `PhysicalOperator` state.
- Worker entry signature is **exactly** `pg_volvec_pipeline_worker_main(dsm_segment*, shm_toc*)`.
- All op I/O uses `PipelineChunk = DataChunk<1024>`.
- §15.3.2 L1957: zero stack-RAII Local* inside PG_TRY; use `core::PgMcxtCallbackGuard`.
- Translator stays pure (Plan → IR); JIT lives on `PhysicalSeqScan::Init()` only, wired in M-Q1-PERF.
- Single `LaunchParallelWorkers` (Oracle B7); 3-event Run/Combine/Finalize.
- DSM = 3 keys only (`CONTROL` / `DSA` / `TASK_QUEUE`); never reintroduce the retired `PLANNEDSTMT` / `QUERY_TEXT` / `PARTIALS` / `SOURCE_PSCAN` / `PARTIAL_FILESET` / `PARAM_EXEC` keys.
- Greenfield shape: `SeqScan -> [Filter] -> PartialAgg -> AggSink` only; Filter fuses into SeqScan qual.
- Q1-only narrowed scope; Q6 deferred to M-Q6-RESTORE; Q2–Q22 / HashJoin / MergeJoin / SubqueryScan FORBIDDEN.

---

## 6. Stale-AGENTS Caveats (do NOT modify, but mentally correct)

- `pipeline/AGENTS.md`:
  - "Refreshed 2026-04-24 (HEAD `fd9a8aaf326`)" → outdated; HEAD is the new 3g.2-prep checkpoint.
  - "22 files / 988 LoC" → 24 files post-3g.2-prep.
  - Active-files snapshot table marks ops as 🟡 stub — actually `.hpp` are P1 fully-built; `.cpp` are still 🟡 (as table claims).
  - DSM keys list still mentions retired keys → reality is 3 keys only.
- `contrib/pg_volvec/AGENTS.md`:
  - "13 active TUs" → 23 post-3g.2-prep.
  - "Step 3 in progress" → Step 3 = 3g.2-prep DONE; 3g.2-final is next.
  - Anti-patterns list mentions retired `dsm_control.hpp` / `PIPELINE_DSM_KEY_*` of the previous era — that warning still applies (don't reintroduce them); the **new** 3 keys live in the rewritten `dsm_control.hpp`.

If at end of M-FRAME-MIN the user lifts the AGENTS.md freeze, refresh both atomically together with the milestone-completion commit.

---

## 7. Commit Protocol Reminders (do NOT violate)

- NEVER `git add -A` — stage explicit files only.
- NEVER `--amend` an already-pushed commit; in this branch, NEVER `--amend` at all unless user requests.
- NEVER `--no-verify`.
- NEVER `push --force` to master.
- NEVER modify `git config`.
- Sisyphus trailer (double-space before URL):
  ```
  🤖 Made with Sisyphus  https://github.com/wgwz/OhMyOpenCode
  ```
- 3g.2-final = ONE atomic commit (per design lock; 2-commit split was a one-time exception for 3g.2-prep + 3g.2-final).
- Commit subject suggestion: `pg_volvec: 3g.2-final MetaPipeline runtime wiring (Step 5–15)`.

---

## 8. Reference Sessions (for resume)

Failed deep agents — DO NOT resume any of these (they all failed for unrelated infra/scope reasons):
- `bg_6f8ec28e` / ses `ses_23d55a711ffeMFtDFw5SQp7AYx` (ABANDONED, base sig mismatch + revert)
- `bg_93719749` / ses `ses_23d07ba5dffe720QFmb2l0C8im` (FAILED infra @ 2m17s)
- `bg_079a725e` / ses `ses_23d027a4bfferj4Kr6JGpTgUN1` (STOP, REWRITE misclassified)
- `bg_53b291a3` / ses `ses_23cbc39c9ffeXZojldM9ndZvhG` (FAILED infra @ 2m17s, last tool todowrite)

Useful Oracle context (still valid reference):
- `bg_6ec1d733` / ses `ses_23f99cf06ffe4at3S1FonZT2RK` — 8-section design for 3g.2 worker bootstrap + DSM publication. Output saved at `/Users/chenyunwen/.local/share/opencode/tool-output/tool_dc097d7d4001j3ngpWzy9diiXn` L670–1515.

---

## 9. Quick Resume Checklist (for the next session's first 5 minutes)

1. `git log -1` — confirm HEAD = the 3g.2-prep checkpoint commit (this session's).
2. `git status` — confirm clean workdir on the pipeline area (worktree may have unrelated pre-existing D/M; do not touch them).
3. Read this file end-to-end.
4. Re-read `docs/GLOBAL_LOCAL_STATE_DESIGN.md` §6.3 + §8.5.4 (anchors lock `eb7901b022a`).
5. Open `pipeline_descriptor.hpp` and `pipeline_descriptor.cpp` — confirm POD layout matches §2 above.
6. Start at Step 5 (physical_seq_scan); proceed in dependency order through Step 15.
7. Compile + smoke after each Step group (5/6/7/8 sources; 9 IR; 10/11/12 scheduler+events+task; 13/14 leader+worker; 15 translator).
8. Final atomic commit.

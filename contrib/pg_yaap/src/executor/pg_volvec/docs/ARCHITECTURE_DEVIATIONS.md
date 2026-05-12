# Architecture Deviations from `pg_duckdb_architecture.md`

**Refreshed:** 2026-04-30
**HEAD:** `6c344eb036d` + uncommitted Bug A/B/B'/C-pre/C/E/F/H landed
**Reference:** `contrib/pg_volvec/pg_duckdb_architecture.md` (109 lines, untracked at worktree root)

This document tracks every place where the current `pg_volvec` runtime diverges from the DuckDB-style PG-port reference architecture. It exists so future sessions don't burn cycles re-discovering each deviation, and so we can deliberately decide which deviations to keep, fix, or revisit.

Each entry: **Reference** (what the doc says) → **Current** (what we do) → **Why** (rationale) → **Status** (keep / temporary / fix-later).

---

## D1 — `Combine` previously gated leader-only (now fixed; deviation closed)

- **Reference (§2.3):** Every Worker calls `Sink.Combine()` to merge its `LocalSinkState` into the shared `GlobalSinkState`. Multiple workers may be in `Combine` simultaneously, protected by `LWLock` / atomics.
- **Earlier deviation (now removed):** v1 made HashAgg `Combine` leader-only as a serialization shortcut. This silently dropped every non-leader worker's `local_tdc` because `local_tdc` lives in backend-private memory and is invisible to the leader.
- **Current:** Bug F fix in `physical_hash_aggregate.cpp:206-244` removed the leader-only guard. Every worker now runs its own `Combine`, serialized via `AggregateHashTable::mutex`.
- **Status:** ✅ closed; aligned with reference.

---

## D2 — `Combine` serialization uses a single mutex, not partition lanes

- **Reference (§2.3):** Implies fine-grained protection (LWLock or atomics) on the global state, with potential for partitioned write paths (DuckDB uses radix-partitioned hash tables to avoid contention during Combine).
- **Current:** `aggregate_hash_table.{cpp,hpp}` uses a single `std::mutex` to serialize all workers' `Combine` calls. No partition lanes, no LWLock.
- **Why:** Q1 has only 4 groups (2 distinct after dedupe); contention is irrelevant. Mutex is simpler and avoids LWLock plumbing while we stabilize the cross-process state machine.
- **Status:** 🟡 temporary; revisit at `M-Q1-PERF` once correctness lands. Switch to LWLock + radix partitioning when group count grows.

---

## D3 — `*GlobalSourceState::finalized` field is a stale snapshot, not authoritative

- **Reference:** GlobalSinkState (and by symmetry GlobalSourceState) lives in DSA and is the single source of truth.
- **Current:** `HashAggGlobalSourceState::finalized` (and similar fields) are *not* read across runtimes. Bug E (`physical_hash_aggregate.cpp:285-304`) made `GetData` gating read `global_tdc->finalized` directly from DSA. The struct field is a snapshot at `GetGlobalSourceState` time and goes stale across pipeline boundaries.
- **Why:** Per-process `GlobalSourceState` instances are reconstructed by each backend; the field is computed locally and never refreshed. Reading the DSA-resident `finalized` flag is the only way to get cross-process truth.
- **Status:** 🟢 deviation by necessity; promoted to **invariant**: any "finalized"-style flag on a per-process state struct is a snapshot — read DSA.

---

## D4 — `PhysicalOperator::shared_payload_dp_` is per-process, not cross-process

- **Reference (§2.2):** Pipelines / operators are flattened into shared memory using array indices or DSA offsets; one canonical place.
- **Current:** `PhysicalOperator` instances are reconstructed in *each backend* (leader + every worker) from the descriptor. The `shared_payload_dp_` field is therefore per-backend. Treating it as cross-process state caused Bug H (Order sink alloc'd a fresh DSA payload in the leader RUN context, throwing away the worker0-allocated one with all the data).
- **Current fix:** `physical_order.cpp:78-113` `GetGlobalSinkState` does `LoadSharedPayloadFromDescriptor` *before* considering `dsa_allocate0`. This is generalized to an **invariant for every sink**: Load-before-alloc.
- **Why:** PG's bgworker model gives each backend its own C++ object graph; we cannot share C++ instance state via instance fields. The descriptor is the only cross-process publish channel.
- **Status:** 🟢 deviation by necessity; invariant documented in `pipeline/AGENTS.md`.

---

## D5 — Sort runs `MaxThreads=1`, in-memory, single-run

- **Reference (§2.3):** Generic Sink/Combine/Finalize three-step works for any pipeline breaker, including parallel sort.
- **Current:** `physical_order.cpp` enforces single-threaded in-memory sort. No spill, no parallel merge.
- **Why:** Q1 has 4 input rows / 2 groups; parallel sort is overkill. Stabilize correctness first.
- **Status:** 🟡 temporary; revisit when a query needs it. External sort + parallel merge is post-`M-Q1-PERF`.

---

## D6 — Leader pre-initializes `Source + Sink` inside `PipelineRunEvent::Schedule`

- **Reference (§1, workflow step 2):** Leader builds the DAG and triggers; Workers do all the work. Leader is "merely a waiter".
- **Current:** `PipelineRunEvent::Schedule` (in the leader) calls `GetGlobalSinkState` + `GetGlobalSourceState` once for every pipeline before enqueuing tasks. This is the only safe time to do the leader-side `dsa_allocate0` (Bug B/H pattern). Workers then `Load` from the descriptor.
- **Why:** PG bgworkers can't reliably perform the "first" alloc — there's a ProcArray race window (`6c344eb036d`) plus the descriptor-publish handshake. Leader pre-init makes Load-before-alloc deterministic.
- **Status:** 🟢 deviation by necessity for the PG bgworker model. Keep.

---

## D7 — bgworker model uses DSM/DSA + descriptor publish; **no `shm_mq` / `TupleQueueReader`**

- **Reference (§3 component map):** `std::thread` → BGWorker, `ConcurrentQueue` → DSM ring queue, `shared_ptr` → DSA pointer / index. (Doesn't mandate `shm_mq` for results, but PG idiom often does.)
- **Current:** No `shm_mq`, no `TupleQueueReader`. Workers write `DataChunk` rows into DSA-resident `TupleDataCollection` (TDC); leader (P0 FINALIZE) drains via `EmitGlobalTdcToDest` → `DestReceiver`.
- **Why:** Pipeline breakers (HashAgg, Order, Output) already need a DSA-resident global state. Re-using that as the result-transport channel is zero-copy and avoids per-row PG TupleQueue serialization. `shm_mq` would force a row-by-row format mismatch.
- **Status:** 🟢 deliberate; aligned with the spirit of the reference doc (`Pipeline Breaker -> GlobalSinkState`).

---

## D8 — DSA payload publish via `Store/LoadSharedPayloadOnDescriptor` (operator-id keyed)

- **Reference (§2.2):** "通过数组索引 (Index) 或基于共享内存基址的相对偏移量 (Offset / `dsa_pointer`) 来表达."
- **Current:** Each `PhysicalOperator` has a stable id during descriptor build. Sinks publish their `dsa_pointer` to a descriptor-resident slot keyed by that id (`pipeline_descriptor.cpp::StoreSharedPayloadOnDescriptor`). Workers `Load` by the same id.
- **Why:** Operator instances are per-process (D4), so the operator id (not the pointer) is the cross-process handle. The descriptor is the only object that survives the leader→worker boundary.
- **Status:** 🟢 PG-specific lowering of the reference doc's "DSA offset / Index" idea. Keep.

---

## D9 — DSM keys `0xD8…0001/0008/0009/000A`, magic `0x56505043`

- **Reference:** Doesn't prescribe specific keys.
- **Current:** Keys live in `0xD8…` range to be visibly disjoint from any historical `VOLVEC_PARALLEL_KEY_*` (legacy morsel runtime, deleted). Magic `0x56505043` ("VPPC").
- **Why:** Defensive — accidental cross-attach to legacy DSM segments would corrupt state.
- **Status:** 🟢 keep.

---

## D10 — Leader **participates** in RUN tasks (worker_index `-1`)

- **Reference (§1, step 5):** "Leader 进程在这场狂欢中仅仅充当等待者".
- **Current:** GUC `pg_volvec.parallel_leader_participation = on` (default). Leader pops from `DsmTaskQueue` and runs RUN tasks alongside workers.
- **Why:** Mirrors PG's standard parallel-query convention; reduces idle leader cost. Set GUC `off` to match the reference exactly.
- **Status:** 🟡 deliberate but configurable. Keep default `on` for now; revisit if leader-only finalization paths get complicated.

---

## D11 — `EventId = pid * 3 + {0,1,2}` (RUN/COMBINE/FINALIZE)

- **Reference:** Doesn't prescribe an EventId scheme.
- **Current:** One slot per event kind per pid. Encodes both producer and event kind in 32 bits.
- **Why:** Cheap, stable, debuggable. Doesn't conflict across pids because PG pid is bounded.
- **Status:** 🟢 keep.

---

## D12 — `OutputSink` runs in workers + leader (NOT pure leader)

- **Reference:** Implied that result emission is a leader concern (since clients connect to the leader).
- **Current:** `OutputSink::Sink` runs in every worker and writes into the DSA TDC. Only the FINAL drain (`EmitGlobalTdcToDest`) is leader-only, in the FINALIZE event.
- **Why:** Decouples row production (parallel, in workers) from row delivery (sequential, in leader). Same architecture as HashAgg/Order.
- **Status:** 🟢 keep.

---

## D13 — `worker_error{,_msg}` channel for cross-process error propagation

- **Reference (§3 component map):** "C++ Exceptions … 写入 DSM 通知 Leader, 随后 Worker 自行转为 PG `ereport`".
- **Current:** `PipelineSharedControl.worker_error` (atomic uint32) + `worker_error_msg[PIPELINE_WORKER_ERROR_MSG_LEN=256]` (`dsm_control.hpp:20`). Workers populate before `ereport(ERROR)`. Leader checks after each event drains.
- **Why:** Exact match to reference.
- **Status:** 🟢 aligned.

---

## D14 — Single PG_CATCH-owned cleanup path in leader

- **Reference:** Doesn't prescribe.
- **Current:** Bug C split `SignalShutdownAndWait` (success only) from `ShutdownAndDestroy` (everywhere else). PG_CATCH owns destroy. Bug A added `BGWH_STOPPED` poll + `WaitLatch` with no hard timeout for PostmasterDeath.
- **Why:** PG's elog/PG_CATCH machinery requires C++ destructors to run; previous double-cleanup paths corrupted state on early exit (Bug C reproducer hung indefinitely).
- **Status:** 🟢 keep.

---

## D15 — No `ExecutionAffinity` enum / no `AttachGlobal*State` virtuals

- **Reference:** Doesn't require either, but DuckDB has `ExecutionAffinity` for COMBINE.
- **Current:** COMBINE affinity handled by `DsmTaskQueue::TryPopForWorker` semantics; per-operator state goes through descriptor-resident payloads, not new virtuals on `PhysicalOperator` (base signatures locked in `eb7901b022a`).
- **Why:** Avoid touching the locked virtual table and keep cross-process state in one place (descriptor).
- **Status:** 🟢 keep.

---

## D16 — NUMERIC(15,2) hot path = scaled `int64`; AVG = `sum_scaled / count` at scale=2

- **Reference:** Doesn't prescribe.
- **Current:** Scaled `int64` on the hot path with widened `int128` accumulators. AVG computed as `sum / count` at scale=2 in `EmitGlobalTdcToDest` (NOT `numeric_div`).
- **Why:** Q1 only needs scale=2 precision; `numeric_div` at runtime is too expensive.
- **Status:** 🟡 known precision drop; revisit at `M-Q1-PERF` correctness pass.

---

## OPEN BUGS (NOT deviations — bugs to fix)

| Bug | Where | Symptom |
|-----|-------|---------|
| G | `physical_hash_aggregate.cpp` | Q1 produces row_count=3 instead of 2 — group dedupe missing. |
| I | `output_sink.cpp::EmitGlobalTdcToDest` | Internal row_count=3 but `psql` shows 0 rows — `DestReceiver` path drops them. |

Both are correctness bugs in the **current** codebase, not architectural deviations from the reference.

---

## REVIEW CADENCE

- Refresh this doc at every milestone boundary (M-Q1-PERF, M-Q6-RESTORE).
- Add an entry whenever a deviation is introduced; don't silently diverge.
- 🟡 entries are review candidates — promote to 🟢 (keep) or fix.

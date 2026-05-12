# Pipeline Port Plan: DuckDB → pg_volvec (P3a–P3e)

**Status:** Draft, awaiting review
**Author:** orchestrator
**Date:** 2026-04-23
**Scope:** Phased port of DuckDB's pipeline orchestration & scheduling layer onto PostgreSQL's bgworker process model.
**Reference commit (DuckDB):** `f9d17f0eb7a6f90586dbf08910910f766eb1b29c`
**Reference commit (pg_volvec):** `e00e99dd38f`

---

## 0. QA Conventions (apply to ALL `QA-*` scenarios below)

These conventions exist so QA scenarios are portable on macOS and reliably observable.

1. **Trace observability**: All test-only trace output (event lifecycle, timing, dsa pointer, pool messages) MUST be emitted via `elog(NOTICE, ...)` (NOT `INFO`, NOT `LOG`). Rationale: `client_min_messages` defaults to `NOTICE`, so `NOTICE` lines reach the client (and thus `psql` stdout/stderr) without any session setup. `INFO` is filtered out by default; `LOG` is server-log-only. QA scenarios `grep` `psql` output, so the underlying implementation MUST use `NOTICE` for any trace a QA scenario inspects. (Production logging may continue to use `LOG`.)

2. **psql timing capture**: To capture per-statement timing from a `.sql` file, `\timing on` MUST precede the file. Use either `-c '\timing on' -f file.sql` or a heredoc with `\timing on` before `\i file.sql`. Never put `\timing on` after `-f`.

3. **Timeouts**: Do NOT use the GNU `timeout` command (not installed on stock macOS). Enforce per-query timeouts via `PGOPTIONS='-c statement_timeout=30s'` or `SET statement_timeout='30s';` inside the session.

4. **Server-log-only checks** (rare): If a check truly needs `LOG`-level output, grep `~/data/pg_tpch/logfile` directly, not psql output.

---

## 1. Background & Goal

### 1.1 Where we are

`pg_volvec` (greenfield phase) supports exactly one pipeline shape:

```
SeqScan -> [Filter] -> PartialAgg -> AggSink
```

The pipeline runtime has the **data-structure scaffolding** of DuckDB's push-pipeline model:

- `Source` / `Operator` / `Sink` virtual interfaces (`source.hpp`, `operator.hpp`, `sink.hpp`)
- `Pipeline` aggregate struct with `id`, `Source*`, `vector<Operator*>`, `Sink*`, `depends_on` (unused)
- `LoweredPipeline` holding `vector<unique_ptr<OwnedPipeline>>` (always size 1 today)
- `WorkerPipelineExecutor` per-worker driver
- Per-query `ParallelContext` with DSM/DSA + bgworkers launched fresh each query

It has **none** of DuckDB's orchestration layer:

- ❌ `Event` base class, `PipelineEvent`, `PipelineFinishEvent`, `PipelineCompleteEvent`, `PipelineInitializeEvent`
- ❌ `MetaPipeline`
- ❌ `Pipeline::depends_on` consumers (field exists, never used)
- ❌ `Task` abstraction
- ❌ `TaskScheduler` / long-lived worker pool
- ❌ Lock-free / shared task queue
- ❌ `InterruptState` / blocked-task resume
- ❌ `PROCESS_PARTIAL` 50-chunk yield budget

Workers are launched per-query via `LaunchParallelWorkers(pcxt)` and exit after one query. Work distribution is a single `pg_atomic_fetch_add_u64(&control->next_block)` morsel claim in `seq_scan_source.cpp`.

### 1.2 Where we want to be

DuckDB's parallel runtime model:

```
Executor::Initialize(plan)
  ├─ MetaPipeline::Build(plan)        ← slice plan tree into pipelines at sink boundaries
  ├─ Ready() (reverse op order)
  └─ ScheduleEvents(meta_pipelines)
       For each pipeline: Init → Run → PrepareFinish → Finish → Complete
       (cross-pipeline deps via Event::AddDependency)
       ↓
TaskScheduler (long-lived pool):
  N background threads, each ExecuteForever():
    queue.semaphore.wait()
    task = queue.Dequeue()
    task->Execute(PROCESS_PARTIAL)   ← 50-chunk budget
      ↓
    PipelineTask::ExecuteTask:
      pipeline_executor->Execute(50)
        if BLOCKED → return INTERRUPTED → task TASK_BLOCKED → Deschedule
        if FINISHED → event->FinishTask() → maybe Schedule next event
```

Our two stated goals:

1. **"把 pipeline 那套东西给弄过来"** — DuckDB's Event/MetaPipeline/Executor coordinator layer.
2. **"线程池每个 query 启动好多 worker，worker 去拿任务"** — long-lived worker pool + task queue.

### 1.3 Why phased

Each layer depends on the one below it. Doing them in the wrong order produces throwaway work. The proposed order minimises rewrite churn:

```
P3a  (Task abstraction)
  └─ P3b  (Event + PipelineCoordinator, single pipeline)
       └─ P3b' (PhysicalOperator IR + MetaPipeline + pluggable translator + dense pipeline IDs)
            └─ P3c (multi-pipeline lowering activation; DEFERRED by default)
                 └─ P3d (PER-QUERY worker pool with per-process pipeline_id → state map)
                      └─ P3e (async/blocked task support)
```

P3a–P3b' are **internal restructuring** that keeps the per-query `LaunchParallelWorkers` model. P3d switches the worker model from "1 worker = 1 pipeline = 1 process exit" to "1 worker = N pipelines = process exits at query end". P3e adds async I/O support.

**P3b' is a NEW phase** added after Round 5 review. It introduces a `PhysicalOperator` IR layer and a `MetaPipeline` builder that walks it, mirroring DuckDB's architecture. This unblocks (a) dense pipeline ID assignment that P3d's per-process state-map needs, (b) future support for translating a DuckDB logical plan via a second translator without rewriting MetaPipeline, and (c) makes the planner front-end pluggable. **P3d depends on P3b'** because per-query worker pool requires stable dense `pipeline_id` for `WorkerPipelineRegistry[id]` indexing.

### 1.4 Hard constraint: regress trio MUST stay green

Every phase ends with `meson test -C build pg_volvec` returning success on `smoke`, `q1`, `q6`. **No phase merges if the trio breaks.**

---

## 2. Process-Model Adaptation Notes (apply to ALL phases)

Porting DuckDB (threads, shared address space) → pg_volvec (processes, shared memory) imposes constraints DuckDB doesn't have. Spell these out once so they don't keep re-surfacing:

| DuckDB primitive | pg_volvec adaptation | Reason |
|---|---|---|
| `std::shared_ptr<Task>` shared between scheduler & threads | `TaskHandle` = (uint32 task_id, uint32 generation) → table lookup in DSM | Cannot share `shared_ptr` across processes |
| `std::atomic<...>` on heap | `pg_atomic_*` in DSM-allocated struct | Cross-process atomics need DSM placement |
| `moodycamel::ConcurrentQueue<shared_ptr<Task>>` | One of: (a) `shm_mq` per worker + leader push, (b) DSA-allocated bounded ring + LWLock + condition var, (c) DSA-backed MPMC ring with `pg_atomic` heads/tails | No cross-process lock-free libraries available |
| `std::condition_variable` | PG `ConditionVariable` (`storage/condition_variable.h`) | PG has its own |
| `std::weak_ptr<Task>` for `InterruptState` | Plain integer `task_id` + generation; consumer validates via lookup | Can't weak-ref across processes |
| `Pipeline::operator new` from C++ heap, accessed by all threads | Pipeline struct lives in **leader** process memory; workers reconstruct via `LowerToPipeline` against worker-local `VecPlanState` | Heap pointers don't cross processes (already true today) |
| Event graph `vector<weak_ptr<Event>>` parents/deps | DSM array of `EventSlot`s indexed by `event_id`; deps are `event_id` lists | Same reason |
| Per-thread `PipelineExecutor` heap object | Per-worker stack/`MemoryContext` object — already true today as `WorkerPipelineExecutor` | OK as-is |
| TaskScheduler started at DB init | Worker pool started at first `pg_volvec` query, or on-demand per query (TBD in P3d) | PostgreSQL bgworker registration is per-postmaster, not per-DB |

**Universal rule:** anything passed between leader and workers lives in DSM/DSA, indexed by stable IDs. Object identity = ID, not pointer.

---

## 3. Phase P3a — Task Abstraction (Foundation)

> **⚠️ SUPERSEDED by §15 (P3X) — see design §8.** This section is RETAINED for historical context (Oracle R1 / Momus R5 review trail). Implementation MUST follow §15 milestones (M-IR → M-META → M-SCHED → M-BM → M-Q6 → M-Q1), NOT the P3a sub-phasing described below.

### 3.1 Goal

Introduce a `Task` struct + `TaskClaim()` API that wraps the existing morsel atomic. **No behaviour change, no scheduler.** Same atomic underneath.

### 3.2 Why first

Every later phase wants to schedule something. Without a `Task`, the scheduler has nothing to schedule. Doing this first means P3b can build event → task dispatch on top of a stable type.

### 3.3 Design

New file `pipeline/task.hpp`:

```cpp
namespace pg_volvec::pipeline {

enum class TaskKind : uint8 {
    PIPELINE_RUN,       // execute pipeline body until FINISHED or yield
    SINK_COMBINE,       // run Sink::Combine for one local sink state
    SINK_FINALIZE,      // run Sink::Finalize (leader-only)
};

struct TaskHandle {
    uint32 task_id;
    uint32 generation;
};

struct Task {
    TaskKind kind;
    PipelineId pipeline_id;        // which pipeline this task belongs to
    int worker_assignment;         // -1 = any, >=0 = pinned worker
    // Payload union per kind (for PIPELINE_RUN, no payload — workers claim morsels via Source)
};

// Per-pipeline shared state in DSM. For P3a, this is just a wrapper around the
// existing `pg_atomic_uint64 next_block` so call sites use TaskClaim() instead
// of raw atomic ops.
class PipelineTaskQueue {
public:
    // Returns the morsel range claimed, or {0,0} if exhausted.
    struct MorselClaim { uint64 start_block; uint32 nblocks; };

    MorselClaim ClaimMorsel(PipelineSharedControl &ctl);
};

}  // namespace
```

### 3.4 Files touched

- **NEW** `pipeline/task.hpp` (declarations)
- **NEW** `pipeline/task.cpp` (`PipelineTaskQueue::ClaimMorsel` — wraps `pg_atomic_fetch_add_u64`)
- **MODIFY** `pipeline/seq_scan_source.cpp` — replace direct `pg_atomic_fetch_add_u64` with `PipelineTaskQueue::ClaimMorsel`
- **MODIFY** `pipeline/seq_scan_source.hpp` — hold `PipelineTaskQueue*` instead of `pg_atomic_uint64*`
- **MODIFY** `pipeline/pipeline_lowering.cpp` — construct `PipelineTaskQueue` (still pointing at `&control->next_block`), pass to source
- **MODIFY** `meson.build` — add `task.cpp`

### 3.5 Acceptance criteria

- [ ] `meson compile -C build pg_volvec` succeeds.
- [ ] `meson test -C build pg_volvec` passes (smoke + q1 + q6).
- [ ] No new GUCs.
- [ ] `git diff --stat` shows ≤ 6 files changed, ≤ 150 LOC delta.
- [ ] No mention of `pg_atomic_fetch_add_u64(&control->next_block)` outside `task.cpp`.

### 3.5.1 QA scenarios (executable)

**QA-P3a-1: Build verification**
```bash
cd /Users/chenyunwen/proj/postgres
CCACHE_DISABLE=1 PATH=/opt/homebrew/bin:$PATH meson compile -C build pg_volvec 2>&1 | tee /tmp/p3a_build.log
echo "EXIT=$?"
```
- Expected: `EXIT=0` and no `error:` lines in `/tmp/p3a_build.log`.

**QA-P3a-2: Install + restart**
```bash
CCACHE_DISABLE=1 PATH=/opt/homebrew/bin:$PATH meson install -C build --only-changed
./installed/bin/pg_ctl -D ~/data/pg_tpch restart -m fast -l ~/data/pg_tpch/logfile
sleep 2
./installed/bin/pg_isready -h /tmp -p 5432
```
- Expected: `pg_isready` returns `accepting connections` (exit 0).

**QA-P3a-3: Regress trio**
```bash
meson test -C build pg_volvec 2>&1 | tee /tmp/p3a_regress.log
echo "EXIT=$?"
grep -E "Ok:|Fail:|Skipped:" /tmp/p3a_regress.log
```
- Expected: `EXIT=0`, line shows `Fail: 0`, `Ok:` includes smoke/q1/q6.

**QA-P3a-4: Source enclosure check**
```bash
grep -rn "pg_atomic_fetch_add_u64(&control->next_block" contrib/pg_volvec/src/ | grep -v task.cpp
```
- Expected: empty output (zero matches outside `task.cpp`).

**QA-P3a-5: LOC budget**
```bash
git diff --stat HEAD~1..HEAD -- contrib/pg_volvec/ | tail -1
```
- Expected: matches `≤ 6 files changed, ≤ 150 LOC delta` (manually verify).

### 3.6 Risk

**Low.** Pure refactor of one atomic op. Reverting = `git revert`.

---

## 4. Phase P3b — Event + Executor Coordinator (Single Pipeline)

> **⚠️ SUPERSEDED by §15 (P3X) — see design §8.** RETAINED for historical context. Implementation follows §15 (M-META + M-SCHED).

### 4.1 Goal

Introduce `Event`, `PipelineEvent`, `PipelineFinishEvent`, `PipelineCompleteEvent`, and an `Executor` coordinator class. Refactor `PgvolvecPipelineRun` from inline imperative code into event-driven dispatch. **Still single pipeline per query.**

### 4.2 Why before MetaPipeline

MetaPipeline (P3c) needs cross-pipeline dependency wiring via events. Without an Event layer, P3c has nothing to wire. Doing P3b first with one pipeline keeps the change small and verifiable.

### 4.3 Design

#### 4.3.1 Event hierarchy

New file `pipeline/event.hpp`:

```cpp
namespace pg_volvec::pipeline {

class Executor;  // forward

class Event {
public:
    explicit Event(Executor &exec);
    virtual ~Event() = default;

    // Lifecycle (called by Executor):
    virtual void Schedule() = 0;          // enqueue child tasks (or run inline)
    virtual void FinishEvent() {}         // called when total_tasks finished
    virtual void FinalizeFinish() {}      // post-finish hook, before Complete

    // Task callbacks:
    void FinishTask();                    // task → event: one task done
    void AddDependency(Event &parent);    // this event waits on `parent`
    void CompleteDependency();            // parent → this: parent done

    bool IsFinished() const { return finished_; }

protected:
    Executor &executor_;
    pg_atomic_uint32 finished_tasks_{0};
    uint32 total_tasks_ = 0;
    pg_atomic_uint32 finished_dependencies_{0};
    uint32 total_dependencies_ = 0;
    bool finished_ = false;

    std::vector<Event *> parents_;        // we wait on these
    std::vector<Event *> children_;       // these wait on us
};

}  // namespace
```

#### 4.3.2 Per-pipeline event chain

For each pipeline, the Executor builds 5 events linked by dependencies:

```
PipelineInitializeEvent  (leader, runs Source/Sink global state init)
  └── PipelineEvent       (parallel, dispatches PIPELINE_RUN tasks)
        └── PipelineFinishEvent  (leader, runs Sink::Combine for each local sink)
              └── PipelineCompleteEvent  (leader, runs Sink::Finalize, hands result back)
```

For P3b with single pipeline: 4 events in a linear chain (we collapse PrepareFinish into FinishEvent for now; can split in P3c).

#### 4.3.3 Executor class

New file `pipeline/coordinator.hpp` (`Executor` is too generic a name — call it `PipelineCoordinator` to avoid clash with `WorkerPipelineExecutor`):

```cpp
class PipelineCoordinator {
public:
    PipelineCoordinator(QueryDesc *qd, PgVolVecQueryState *qstate);

    // Top-level entry, replaces inline body of PgvolvecPipelineRun.
    bool Run(const char **failure_reason);

private:
    bool BuildPipelines();            // calls LowerToPipeline
    void BuildEventGraph();           // creates Init/Run/Finish/Complete events
    void ScheduleEvents();            // walks roots, calls Event::Schedule
    void DispatchTask(Task &task);    // P3b: synchronous; P3d: enqueue to worker pool

    QueryDesc *qd_;
    PgVolVecQueryState *qstate_;
    std::unique_ptr<LoweredPipeline> bundle_;
    std::vector<std::unique_ptr<Event>> events_;
    ParallelContext *pcxt_ = nullptr;
    // ... other per-query handles previously local in PgvolvecPipelineRun
};
```

#### 4.3.4 Synchronous dispatch (P3b shortcut)

To keep P3b small, `DispatchTask` **does not yet introduce a worker pool**. It either:

- Runs the task inline on leader (for INIT/COMBINE/FINALIZE events), OR
- Calls existing `LaunchParallelWorkers(pcxt)` for `PipelineEvent` (which represents the parallel run phase).

This means P3b has a **degenerate event loop**: `Schedule → execute synchronously → FinishTask → next event`. That is intentional — the Event API is the contract we want; the async dispatch comes in P3d.

### 4.4 Files touched

- **NEW** `pipeline/event.hpp`, `pipeline/event.cpp`
- **NEW** `pipeline/coordinator.hpp`, `pipeline/coordinator.cpp` (~300 LOC, replaces body of `pipeline_leader.cpp`)
- **MODIFY** `pipeline/pipeline_leader.cpp` — `PgvolvecPipelineRun` becomes a thin shim that constructs `PipelineCoordinator` and calls `Run`
- **MODIFY** `meson.build`
- **OPTIONAL** `pipeline/AGENTS.md` — document Event lifecycle

### 4.5 Acceptance criteria

- [ ] Regress trio passes.
- [ ] `PipelineCoordinator::Run` is the only function in `pipeline_leader.cpp` body that touches `ParallelContext` and DSM.
- [ ] Event lifecycle is exercised: log `pg_volvec.trace_execution_path=on` shows 4 events per Q1/Q6 run in order.
- [ ] No regression in benchmark wall-time (within 5%) on Q1 + Q6 at 14 workers.
- [ ] `git diff --stat` ≤ 8 files, ≤ 600 LOC delta.

### 4.5.1 QA scenarios (executable)

**QA-P3b-1: Build + regress trio** — same commands as QA-P3a-1/2/3. Expected: `EXIT=0`, `Fail: 0`.

**QA-P3b-2: Event lifecycle trace**

Implementation requirement: `PipelineCoordinator` MUST emit one `elog(NOTICE, "pg_volvec[event] kind=%s phase=%s pipeline_id=%u", ...)` line per event lifecycle transition (Schedule, FinishTask, FinishEvent, Complete). Use `NOTICE` so it reaches `psql` stdout under default `client_min_messages` (see §0 convention 1).

```bash
./installed/bin/psql -h /tmp -p 5432 -d tpch -f contrib/pg_volvec/sql/q1.sql 2>&1 | tee /tmp/p3b_q1.log
grep "pg_volvec\[event\]" /tmp/p3b_q1.log | wc -l
grep "pg_volvec\[event\]" /tmp/p3b_q1.log
```
- Expected:
  - At least 4 distinct event kinds appear in order: `INIT → RUN → FINISH → COMPLETE`.
  - Sequence is monotonic per pipeline_id (no FINISH before RUN).
  - Total event log lines ≥ 8 (each event: Schedule + Finish at minimum).

**QA-P3b-3: Wall-time regression check (Q1 + Q6, 14 workers)**

Baseline capture (run on `HEAD~1` — pre-P3b):
```bash
git stash; git checkout HEAD~1
CCACHE_DISABLE=1 PATH=/opt/homebrew/bin:$PATH meson compile -C build pg_volvec && meson install -C build --only-changed
./installed/bin/pg_ctl -D ~/data/pg_tpch restart -m fast
for q in q1 q6; do
  : > /tmp/baseline_${q}.txt
  for i in 1 2 3; do
    ./installed/bin/psql -h /tmp -p 5432 -d tpch \
      -c "SET pg_volvec.parallel_max_workers=14;" \
      -c '\timing on' \
      -f contrib/pg_volvec/sql/${q}.sql 2>&1 | grep "^Time:" | tee -a /tmp/baseline_${q}.txt
  done
done
git checkout - ; git stash pop
```
Post-P3b capture: same loop, write to `/tmp/p3b_${q}.txt`.

Compute median per query:
```bash
for q in q1 q6; do
  base=$(awk '/Time:/ {print $2}' /tmp/baseline_${q}.txt | sort -n | sed -n '2p')
  new=$(awk '/Time:/ {print $2}' /tmp/p3b_${q}.txt | sort -n | sed -n '2p')
  echo "$q baseline=${base}ms p3b=${new}ms ratio=$(echo "scale=3; $new/$base" | bc)"
done
```
- Expected: each `ratio` between `0.95` and `1.05` (within ±5%).

**QA-P3b-4: Coordinator isolation**
```bash
grep -nE "CreateParallelContext|InitializeParallelDSM|LaunchParallelWorkers|shm_toc_allocate" contrib/pg_volvec/src/engine/parallel/pipeline/pipeline_leader.cpp
```
- Expected: empty (all such calls relocated to `coordinator.cpp`).

### 4.6 Risk

**Medium.** Touches the leader code path. Mitigated by keeping the per-query `LaunchParallelWorkers` model unchanged — only the **calling code** is restructured.

### 4.7 Pre-implementation gate

Send this section's design (Event + PipelineCoordinator API) to **Momus** for review before writing code.

---

## 5. Phase P3c — MetaPipeline + Multi-Pipeline Lowering

### 5.1 Goal

Activate `LoweredPipeline.pipelines.size() > 1`. Implement `MetaPipeline` analog. Wire `Pipeline::depends_on`. Lower at least one shape that needs multiple pipelines (HashJoin Build + Probe is the canonical case, but **HashJoin is forbidden by current constraints**, so we need a different test case).

### 5.2 Decision (DEFAULT: defer; user may override)

**Greenfield Q1+Q6 do not exercise multi-pipeline lowering.** Both are single-pipeline shapes. To test P3c, we need either:

1. **Extend supported shapes** beyond Q1+Q6 to include something like `Sort -> SeqScan -> Agg` (2 pipelines: scan→sort sink, then sort source→agg). This conflicts with AGENTS.md ("active scope: Q1 and Q6 only").
2. **Synthetic test pipeline** — add a degenerate "passthrough sink + new source" pipeline pair that both run on Q1's data, just to exercise the multi-pipeline machinery. Throwaway test code.
3. **Defer P3c** until the team decides to widen Q1+Q6 scope.

**Default decision (proceed unless user overrides):** **Defer P3c.** Implement P3a→P3b→P3d→P3e on the Q1+Q6 surface, and revisit P3c when HashJoin (or any 2-pipeline shape) comes back into scope. A scheduler that handles 1 pipeline well is more valuable than one that handles 2 pipelines speculatively.

**P3a/P3b/P3d/P3e do NOT depend on P3c.** Skipping P3c blocks nothing in this plan.

**Hard stop point — P3c implementation MUST NOT begin until:** user explicitly overrides the default deferral AND picks one of the three options in §5.2 (extend shape, synthetic test, or scope a real query).

### 5.3 If approved: design sketch

- New `pipeline/meta_pipeline.hpp` with `MetaPipeline { vector<Pipeline*> pipelines; Sink *shared_sink; }`.
- `MetaPipeline::Build(VecPlanState *root)` slices the plan at sink boundaries.
- `Pipeline::depends_on` populated; `PipelineCoordinator::BuildEventGraph` reads it to call `Event::AddDependency`.
- Worker side rebuilds the same MetaPipeline structure.

### 5.4 Acceptance (if implemented)

- [ ] At least one test query produces `LoweredPipeline.pipelines.size() == 2`.
- [ ] Build pipeline events complete before Probe pipeline events Schedule.
- [ ] Regress trio still passes.

---

## 6. Phase P3b' — PhysicalOperator IR + MetaPipeline + Translator/RuntimeBinder Split

> **⚠️ SUPERSEDED by §15 (P3X) — see design §8.** RETAINED for historical context (much of §6's content WAS the basis for design §8.1–§8.3; that material now lives authoritatively in the design doc). Implementation follows §15 M-IR + M-META.
>
> **Revision history:** R2 (post-Oracle Round 1). Resolves §14.1 blockers B1–B4. Eliminates `VecPlanState` AST and `pipeline_lowering.{hpp,cpp}` per user decision.

### 6.1 Goal

Introduce DuckDB's three-layer architecture between PG planner and the pipeline runtime, **eliminating the existing `VecPlanState` AST** as an intermediate IR:

```
PG PlannedStmt ──[PgPlanTranslator]──▶ PhysicalOperator tree ──[MetaPipeline::Build]──▶ MetaPipelineBundle
                                                                            │             (vector<Pipeline> +
(Future) DuckDB LogicalPlan ──[DuckPlanTranslator]──┘                       │              depends_on graph,
                                                                            ▼              dense IDs 0..N-1)
                                                                  [RuntimeBinder]
                                                                            │  (injects PipelineSharedControl,
                                                                            ▼   next_block, shared_slots,
                                                                  PipelineCoordinator (P3b)   spill_fileset)
```

This phase introduces four new things and **deletes one existing thing**:

1. **NEW** `PhysicalOperator` polymorphic node IR (base + 5 concrete ops; see §6.5.1).
2. **NEW** `MetaPipeline` that walks a `PhysicalOperator` tree, splits at pipeline breakers, and emits `vector<unique_ptr<Pipeline>>` plus a `depends_on` graph with **dense `PipelineId` 0..N-1** assigned in post-order.
3. **NEW** `Translator` interface + `PgPlanTranslator` concrete impl that walks PG `PlannedStmt->planTree` directly to a `PhysicalOperator` tree (no `VecPlanState` intermediate). Pure plan→IR; no runtime wiring args.
4. **NEW** `RuntimeBinder` free-standing class that injects PG-runtime references (`PipelineSharedControl*`, atomics, shared slots, spill fileset) into the materialized Source/Sink objects after `MetaPipeline::Build`. Future `DuckPlanTranslator` does not need this.
5. **DELETE** `pipeline/pipeline_lowering.{hpp,cpp}` entirely. `LowerToPipeline()` signature is **not preserved**. Callers (leader + worker) directly invoke `PgPlanTranslator::Translate()` → `MetaPipeline::Build()` → `RuntimeBinder::Bind()`.
6. **DELETE** `VecPlanState`, `VecSeqScanState`, `VecAggState`, and all related Vec* AST symbols. The PhysicalOperator tree is the single IR.

### 6.2 Why before P3d

P3d's per-process `WorkerPipelineRegistry` is `std::vector<std::unique_ptr<WorkerPipelineState>>` indexed by `PipelineId`. This requires IDs to be:
- **Dense** (`0..N-1`, no gaps) — vector indexing
- **Stable across leader and worker** — same translator + same `PlannedStmt` produces same `(PipelineId, depends_on)` deterministically in both processes
- **Assigned in one place** (the MetaPipeline builder) — not scattered

Today `pipeline_lowering.cpp:53` hardcodes `owned->pipeline.id = 0`. There is no MetaPipeline. P3d cannot land cleanly on top of the existing structure without doing this refactor; band-aid `next_id++` would be ripped out the moment we want a second pipeline.

### 6.3 Why before P3c

P3c is **DEFERRED by default** (§5.2) and may never happen. Greenfield Q1 and Q6 already produce multi-pipeline shapes (see §6.5.6) because of distributed FinalAgg and Sort breakers, so P3b' exercises the multi-pipeline machinery on day one. P3c would only matter for HashJoin re-introduction.

### 6.4 Why pluggable translator

The user explicitly stated: *"In the future, maybe use duckdb's optimizer. just need to modify the translator, or just add a translator. pg_volvec can accept pg or duck's plans."*

Decoupling `PG plan → PhysicalOp tree` from `PhysicalOp tree → Pipeline` lets a future `DuckPlanTranslator` reuse the entire MetaPipeline + Coordinator + worker pool stack unchanged. Per Oracle B4, this decoupling requires that the translator NOT carry PG-specific runtime args — those move to `RuntimeBinder` (§6.5.3).

### 6.5 Design

#### 6.5.1 PhysicalOperator IR

New file `pipeline/physical_operator.hpp`:

```cpp
namespace pg_volvec::pipeline {

enum class PhysicalOpType : uint8 {
    SEQ_SCAN,
    PARTIAL_AGG,
    FINAL_AGG,
    SORT,
    OUTPUT,
};

// Tag: which side of a pipeline boundary this operator sits on.
// SOURCE = leaf of pipeline (reads). SINK = root of pipeline (terminates it).
// REGULAR = inner operator (transforms streaming chunks).
enum class PhysicalOpKind : uint8 {
    SOURCE,
    REGULAR,
    SINK,
};

class PhysicalOperator {
public:
    PhysicalOperator(PhysicalOpType type, PhysicalOpKind kind);
    virtual ~PhysicalOperator() = default;

    PhysicalOpType type() const { return type_; }
    PhysicalOpKind kind() const { return kind_; }
    const std::vector<std::unique_ptr<PhysicalOperator>> &children() const { return children_; }

    // Pipeline-breaker contract (Oracle B2). Operators that materialize their
    // input (PartialAgg, Sort) return true; downstream ops start a new
    // pipeline whose source reads the materialized state.
    virtual bool IsPipelineBreaker() const { return false; }

    // Materialize the runtime Source/Operator/Sink for this physical node.
    // Called by MetaPipeline during Pipeline construction. RuntimeBinder
    // (§6.5.3) injects PG-runtime references after construction.
    virtual std::unique_ptr<Source>   ToSource()   { return nullptr; }
    virtual std::unique_ptr<Operator> ToOperator() { return nullptr; }
    virtual std::unique_ptr<Sink>     ToSink()     { return nullptr; }

protected:
    PhysicalOpType type_;
    PhysicalOpKind kind_;
    std::vector<std::unique_ptr<PhysicalOperator>> children_;
};

}  // namespace
```

Concrete subclasses (one file each, ~80–120 LOC). Each holds **POD-style config** extracted from `PlannedStmt` — no `Vec*` types anywhere in headers OR `.cpp` files (they're deleted from the codebase per §6.6):

- `physical_seq_scan.{hpp,cpp}` — fields: `Oid relid; List *qual; List *targetlist; AttrNumber *projection; int nattrs;`. `ToSource()` returns `std::make_unique<SeqScanSource>(...)` constructed from these POD fields. Filter is embedded (no separate PhysicalFilter; per §6.5.7 inventory).
- `physical_partial_agg.{hpp,cpp}` — fields: `List *group_keys; List *agg_specs; int num_slots;`. `IsPipelineBreaker() → true`. `ToSink()` returns `std::make_unique<PartialAggSink>(...)`.
- `physical_final_agg.{hpp,cpp}` — fields: `List *agg_specs; int partition_count;`. `IsPipelineBreaker() → false` (consumes already-materialized partials; the breaker is the upstream PartialAgg). `ToOperator()` returns `std::make_unique<FinalAggOp>(...)`.
- `physical_sort.{hpp,cpp}` — fields: `List *sort_keys; List *sort_directions; bool partial;`. `IsPipelineBreaker() → true` (Sort materializes). `ToSink()` returns `std::make_unique<SortSink>(...)`.
- `physical_output.{hpp,cpp}` — fields: `TupleDesc result_desc;`. `ToSink()` returns `std::make_unique<OutputSink>(...)` which delivers chunks to the PG `DestReceiver`.

#### 6.5.2 MetaPipeline (with breaker detection + depends_on graph)

New files `pipeline/meta_pipeline.{hpp,cpp}`:

```cpp
namespace pg_volvec::pipeline {

struct MetaPipelineBundle {
    std::vector<std::unique_ptr<Pipeline>> pipelines;     // indexed by PipelineId
    // depends_on[i] = ids of pipelines that must finish before pipeline i runs.
    std::vector<std::vector<PipelineId>>   depends_on;
};

class MetaPipeline {
public:
    // Walks the PhysicalOperator tree, splits at IsPipelineBreaker() nodes,
    // emits a Pipeline per chain. Dense IDs assigned in POST-ORDER traversal
    // so child pipelines (data producers) get lower IDs than parents
    // (consumers). depends_on[parent] includes all child pipeline ids.
    //
    // For Q6: 2 pipelines; Q1: 3 pipelines. See §6.5.6.
    static std::unique_ptr<MetaPipelineBundle>
    Build(std::unique_ptr<PhysicalOperator> root);

private:
    struct Builder {
        std::unique_ptr<MetaPipelineBundle> bundle;
        PipelineId                          next_id = 0;

        // Walk a chain rooted at `op` (which must be SINK or pipeline-end).
        // Recurses into children; if child IsPipelineBreaker(), child becomes
        // a new pipeline AND a producer dependency of the current pipeline.
        // Returns the produced pipeline's id.
        PipelineId BuildPipeline(PhysicalOperator *root_op);
    };
};

}  // namespace
```

Key invariants (Determinism Contract — see §6.5.5):
- Pipeline IDs assigned by **post-order visitation**: children before parents. Same translator + same `PlannedStmt` → identical `(PipelineId, depends_on)` deterministically across processes.
- A Pipeline's `id` field replaces the current hardcoded `0` at the (now-deleted) `pipeline_lowering.cpp:53`.
- `INVALID_PIPELINE_ID` (already in `pipeline.hpp`) remains the sentinel for "not yet assigned".
- `MetaPipeline::Build` does NOT take `PipelineSharedControl*` or any other PG-runtime args (Oracle B4). Pure tree → bundle.

#### 6.5.3 Translator + RuntimeBinder split (Oracle B4)

New files `pipeline/translator.hpp`, `pipeline/pg_plan_translator.{hpp,cpp}`, `pipeline/runtime_binder.{hpp,cpp}`:

```cpp
namespace pg_volvec::pipeline {

// Pluggable front-end. Pure plan→PhysicalOp tree. NO PG-runtime args.
class Translator {
public:
    virtual ~Translator() = default;
    // Returns nullptr to signal "this translator cannot accept this plan
    // shape" (caller falls back to native PG execution). See §6.5.7.
    virtual std::unique_ptr<PhysicalOperator> Translate(PlannedStmt *stmt) = 0;
};

// Concrete translator #1: walks PG PlannedStmt->planTree directly.
class PgPlanTranslator : public Translator {
public:
    PgPlanTranslator() = default;   // no args (Oracle B4)
    std::unique_ptr<PhysicalOperator> Translate(PlannedStmt *stmt) override;
};

// Future:
// class DuckPlanTranslator : public Translator { ... };

// Free-standing binder. Walks the post-Build pipeline bundle and injects
// PG-runtime references into the materialized Source/Sink objects.
// Called once per query, after MetaPipeline::Build, before first pipeline
// Execute. DuckPlanTranslator users would skip this or use a different
// binder.
class RuntimeBinder {
public:
    struct PgRuntimeRefs {
        PipelineSharedControl   *shared_control;
        pg_atomic_uint64        *next_block;
        ParallelAggPartialState *shared_slots;
        int                      num_slots;
        SharedFileSet           *spill_fileset;
    };

    static void Bind(MetaPipelineBundle &bundle, const PgRuntimeRefs &refs);
};

}  // namespace
```

#### 6.5.4 Replacing `LowerToPipeline` (file deleted; callers updated)

`pipeline/pipeline_lowering.{hpp,cpp}` are **deleted entirely** (per Oracle B1 + user decision). Both leader and worker code paths are updated to invoke the three-step pipeline directly:

```cpp
// In pipeline_leader.cpp and pipeline_worker_main.cpp:

PgPlanTranslator translator;
auto physical_root = translator.Translate(stmt);
if (!physical_root) {
    // Unsupported shape → fall back to native PG execution path.
    return PgVolvecFallbackToPgExecutor(stmt);
}

auto bundle = MetaPipeline::Build(std::move(physical_root));

RuntimeBinder::PgRuntimeRefs refs{
    shared_control, next_block, shared_slots, num_slots, spill_fileset
};
RuntimeBinder::Bind(*bundle, refs);

// bundle is the MetaPipelineBundle; coordinator iterates bundle->pipelines
// in dependency order (depends_on graph).
```

The bridge code in `bridge/pg_volvec.c` is updated to invoke this from both leader and worker entrypoints. Both call `PgPlanTranslator::Translate(stmt)` against the deserialized `PlannedStmt` — Determinism Contract guarantees identical IR + IDs.

#### 6.5.5 Determinism Contract (Oracle B3) — HARD RULES

The correctness of P3d's `WorkerPipelineRegistry[id]` lookups depends on every process producing the SAME `(PipelineId, depends_on, PhysicalOp tree shape)` from the same `PlannedStmt`. Violations silently corrupt state. The following rules are enforced in code review and grep-based QA:

1. **No `std::unordered_map`/`std::unordered_set` iteration** anywhere in `translator.{hpp,cpp}`, `pg_plan_translator.{hpp,cpp}`, `meta_pipeline.{hpp,cpp}`, or any `physical_*.cpp`. Use `std::map`/`std::set` if a sorted associative container is needed; otherwise use `std::vector` of pairs.
2. **No hash-map keyed by node address** where iteration order influences emission. Pointer values vary across processes.
3. **No address-based ordering.** All `std::sort` calls in the translator/builder must use deterministic key functions (e.g. relid, attno, list position).
4. **Children visited in declaration order.** PG `Plan->lefttree`, `Plan->righttree` always in that order. PG `List*` always front-to-back via `foreach`.
5. **Sibling pipelines visited in post-order.** Bottom-up (children first). Pipeline IDs increment monotonically by visitation order: 0, 1, 2, ... N-1.
6. **Verification:** an entry is added to `contrib/pg_volvec/src/engine/parallel/pipeline/AGENTS.md` listing all 5 rules under a "Determinism Contract" heading. Acceptance criterion §6.7 includes a grep check that `unordered_` does not appear in any pipeline IR/translator/builder source file.

#### 6.5.6 Pipeline shapes for greenfield queries

User picked DuckDB-style distributed FinalAgg (§6 Q3 of design). This produces multi-pipeline shapes for both Q1 and Q6, exercising MetaPipeline `depends_on` from day one.

**Q6** (`Aggregate plain → SeqScan(filter)`):

```
Pipeline 0 (worker, depends_on=[]):
    SeqScan+filter (Source) ──▶ PartialAgg (Sink, breaker)
                                    │
                                    ▼  materializes partition-keyed partial hash table in DSM
Pipeline 1 (worker, depends_on=[0]):
    RepartitionedPartials (Source) ──▶ FinalAgg ──▶ Output (Sink)
       └─ each worker reads only its assigned hash partition of the partial table
```

**Q1** (`Sort → Aggregate group-by → SeqScan(filter)`):

```
Pipeline 0 (worker, depends_on=[]):
    SeqScan+filter (Source) ──▶ PartialAgg (Sink, breaker)

Pipeline 1 (worker, depends_on=[0]):
    RepartitionedPartials (Source) ──▶ FinalAgg ──▶ Sort (Sink, breaker)
       └─ Sort here is per-worker partial sort (each worker sorts its partition of final-agg results)

Pipeline 2 (coordinator-only, depends_on=[1]):
    MergePartialSorts (Source) ──▶ Output (Sink → PG DestReceiver)
       └─ K-way merge of N sorted runs, one per worker. THIS is the only pipeline run by the leader.
          Consistent with §7.3.d ("leader is pure coordinator"): merge+output is administrative
          dispatching of already-computed and already-sorted data, not bulk data work.
```

**Cross-worker repartition exchange:** PartialAgg writes its hash table into a DSM-backed partitioned structure (hash-partitioned by group-by key hash mod N where N = `launched_worker_count`). Pipeline 1's `RepartitionedPartials` source on worker `k` reads only partition `k`. This requires no inter-worker `shm_mq` data movement — partition assignment is implicit by worker index.

#### 6.5.7 Supported PG node types (translator coverage)

Per user decision: `PgPlanTranslator::Translate(stmt)` translates the **whole** `PlannedStmt->planTree`. If ANY node is unsupported, returns nullptr → bridge falls back to PG executor for the entire query (no hybrid execution).

**Supported (greenfield Q1+Q6 coverage):**
- `T_SeqScan` → `PhysicalSeqScan` (with embedded qual filter pushed down).
- `T_Agg` with `aggsplit = AGGSPLIT_INITIAL_SERIAL` → `PhysicalPartialAgg`.
- `T_Agg` with `aggsplit = AGGSPLIT_FINAL_DESERIAL` → `PhysicalFinalAgg`.
- `T_Agg` with `aggsplit = AGGSPLIT_SIMPLE` (non-parallel-ready) → split into `PhysicalPartialAgg + PhysicalFinalAgg` by translator if pg_volvec wants to parallelize; else nullptr.
- `T_Sort` → `PhysicalSort`.
- `T_Result` → if it's a trivial pass-through (no qual, no targetlist computation), absorb into child; otherwise nullptr.

**Explicitly REJECTED (translator returns nullptr):**
- `T_NestLoop`, `T_HashJoin`, `T_MergeJoin` (HashJoin removed in greenfield; will return in P3c).
- `T_Limit` (greenfield Q1+Q6 don't use LIMIT).
- `T_Gather`, `T_GatherMerge` (pg_volvec replaces PG parallelism with its own pool).
- `T_WindowAgg`, `T_CteScan`, `T_RecursiveUnion`, `T_SubqueryScan`.
- `T_IndexScan`, `T_IndexOnlyScan`, `T_BitmapHeapScan` (only SeqScan in greenfield).
- Any node not in the supported list above.

The whole-PlannedStmt acceptance check happens once in `Translate()`. A NOTICE is emitted on rejection: `elog(NOTICE, "pg_volvec[translator] rejected: unsupported node type %s", nodeToString(unsupported_node))`.

### 6.6 Files touched

**NEW (12 files):**
- `pipeline/physical_operator.hpp` — base class, enums, `IsPipelineBreaker()` (~70 LOC)
- `pipeline/physical_seq_scan.{hpp,cpp}` — POD config + ToSource (~100 LOC)
- `pipeline/physical_partial_agg.{hpp,cpp}` — POD config + ToSink + breaker=true (~80 LOC)
- `pipeline/physical_final_agg.{hpp,cpp}` — POD config + ToOperator (~70 LOC)
- `pipeline/physical_sort.{hpp,cpp}` — POD config + ToSink + breaker=true (~80 LOC)
- `pipeline/physical_output.{hpp,cpp}` — POD config + ToSink → DestReceiver (~60 LOC)
- `pipeline/meta_pipeline.{hpp,cpp}` — Builder with breaker detection + depends_on graph (~180 LOC)
- `pipeline/translator.hpp` — `Translator` interface (~30 LOC)
- `pipeline/pg_plan_translator.{hpp,cpp}` — concrete translator, no-args ctor, whole-PlannedStmt walker (~180 LOC)
- `pipeline/runtime_binder.{hpp,cpp}` — RuntimeBinder + PgRuntimeRefs (~80 LOC)

**MODIFY:**
- `pipeline/pipeline_leader.cpp` — replace `LowerToPipeline()` call with three-step `Translate → Build → Bind` (~30 LOC delta).
- `pipeline/pipeline_worker_main.cpp` — same three-step replacement (~30 LOC delta).
- `pipeline/pipeline.hpp` — clarify that `Pipeline::id` is densely assigned by `MetaPipeline::Builder`; add `vector<PipelineId> depends_on;` field if not already present.
- `pipeline/AGENTS.md` — document IR layer, translator/binder split, dense ID invariant, **Determinism Contract (5 hard rules)**.
- `bridge/pg_volvec.c` — update entrypoint plumbing for the new translator path; remove `VecPlanState` references.
- `meson.build` — add new sources, remove deleted ones.

**DELETE:**
- `pipeline/pipeline_lowering.hpp` — entirely.
- `pipeline/pipeline_lowering.cpp` — entirely.
- All `Vec*State` AST sources and headers (`vec_plan_state.{hpp,cpp}`, `vec_seq_scan_state.{hpp,cpp}`, `vec_agg_state.{hpp,cpp}`, etc.). PgPlanTranslator extracts equivalent info directly from `PlannedStmt->planTree`.

Total: ~12 new files + ~6 modified + ~6+ deleted. **New LOC ~700; deleted LOC ~400; net ~+300.**

### 6.7 Acceptance criteria

- [ ] Regress trio passes (smoke + q1 + q6).
- [ ] `Pipeline::id` is no longer hardcoded `0` anywhere; assigned exclusively in `MetaPipeline::Builder::BuildPipeline` via post-order traversal.
- [ ] Leader and worker independently produce **identical** `(PipelineId, depends_on)` graphs from the same `PlannedStmt` (verified via NOTICE trace, see QA-P3b'-3 and QA-P3b'-6).
- [ ] `pipeline/pipeline_lowering.hpp` and `pipeline/pipeline_lowering.cpp` no longer exist (file deletion verified).
- [ ] No `Vec(Plan|SeqScan|Agg)State` symbols remain anywhere under `contrib/pg_volvec/src/engine/parallel/pipeline/` (grep returns empty per QA-P3b'-5).
- [ ] No `std::unordered_map` / `std::unordered_set` references in any `pipeline/translator.*`, `pipeline/pg_plan_translator.*`, `pipeline/meta_pipeline.*`, or `pipeline/physical_*.*` source file (Determinism Contract grep).
- [ ] `RuntimeBinder` is the ONLY place that takes `PipelineSharedControl*` / atomic / shared-slots / fileset args; `Translator::Translate(PlannedStmt*)` takes only the plan (Oracle B4).
- [ ] `MetaPipeline::Build()` signature takes only `unique_ptr<PhysicalOperator>` (no PG-runtime args).
- [ ] Q6 produces 2 pipelines with `depends_on=[[],[0]]`; Q1 produces 3 pipelines with `depends_on=[[],[0],[1]]` (verified via QA-P3b'-6).
- [ ] `git diff --stat` ≤ 25 files changed (more files due to deletions), ≤ 1100 LOC delta.

### 6.7.1 QA scenarios (executable)

**QA-P3b'-1: Build + regress trio** — same as QA-P3a-1/2/3. Expected: `EXIT=0`, `Fail: 0`.

**QA-P3b'-2: ID assignment is dense and starts at 0**

Implementation requirement: `MetaPipeline::Build` MUST emit `elog(NOTICE, "pg_volvec[meta] pipeline_assigned id=%u sink_op=%s", id, sink_op_name)` for each pipeline produced.

```bash
./installed/bin/psql -h /tmp -p 5432 -d tpch -f contrib/pg_volvec/sql/q1.sql 2>&1 | tee /tmp/p3bprime_q1.log
grep "pg_volvec\[meta\] pipeline_assigned" /tmp/p3bprime_q1.log
```
- Expected: exactly 3 lines for Q1 with `id=0`, `id=1`, `id=2` (one per call site: leader). IDs MUST be `0..N-1` contiguous (no gaps, no duplicates).

**QA-P3b'-3: Leader/worker IR agreement**

Implementation requirement: both leader and worker emit `elog(NOTICE, "pg_volvec[meta] %s id=%u sink_op=%s deps=[%s]", role, id, sink_op_name, deps_csv)` where `role` is `"leader"` or `"worker[N]"`.

```bash
./installed/bin/psql -h /tmp -p 5432 -d tpch -c "SET pg_volvec.parallel_max_workers=4;" -f contrib/pg_volvec/sql/q1.sql 2>&1 | tee /tmp/p3bprime_agreement.log
grep "pg_volvec\[meta\]" /tmp/p3bprime_agreement.log | awk '{print $3,$4,$5,$6}' | sort -u | wc -l
```
- Expected: count = 3 (one unique tuple per pipeline; leader and all 4 workers report identical `(id, sink_op, deps)` for each pipeline → only 3 unique combinations across 5 processes × 3 pipelines = 15 lines collapsing to 3 unique).

**QA-P3b'-4: Translator rejection falls back to PG**

Implementation requirement: `Translate()` returns nullptr on any unsupported node and emits `elog(NOTICE, "pg_volvec[translator] rejected: unsupported node type T_X")`. Bridge then invokes native PG executor.

Test query: a query containing `T_NestLoop` (e.g. `SELECT * FROM lineitem_q6, (VALUES (1),(2)) v WHERE l_quantity > v.column1`).

```bash
./installed/bin/psql -h /tmp -p 5432 -d tpch -f contrib/pg_volvec/test_unsupported.sql 2>&1 | tee /tmp/p3bprime_fallback.log
grep "pg_volvec\[translator\] rejected" /tmp/p3bprime_fallback.log
```
- Expected: NOTICE present; query returns correct result via PG executor; no ERROR.

**QA-P3b'-5: VecPlanState symbols deleted**
```bash
grep -rnE "Vec(Plan|SeqScan|Agg)State" contrib/pg_volvec/src/engine/parallel/pipeline/
```
- Expected: empty (zero matches anywhere under `pipeline/`). Pre-P3b' this would match many lines; post-P3b' must be clean.

**QA-P3b'-6: depends_on graph correctness**

Uses the NOTICE format from QA-P3b'-3.

```bash
./installed/bin/psql -h /tmp -p 5432 -d tpch -f contrib/pg_volvec/sql/q6.sql 2>&1 | grep "pg_volvec\[meta\] leader" | sort -u
./installed/bin/psql -h /tmp -p 5432 -d tpch -f contrib/pg_volvec/sql/q1.sql 2>&1 | grep "pg_volvec\[meta\] leader" | sort -u
```
- Expected Q6: 2 lines with `id=0 ... deps=[]` and `id=1 ... deps=[0]`.
- Expected Q1: 3 lines with `id=0 ... deps=[]`, `id=1 ... deps=[0]`, `id=2 ... deps=[1]`.

**QA-P3b'-7: Determinism Contract grep**
```bash
grep -nE "unordered_map|unordered_set" \
  contrib/pg_volvec/src/engine/parallel/pipeline/translator.* \
  contrib/pg_volvec/src/engine/parallel/pipeline/pg_plan_translator.* \
  contrib/pg_volvec/src/engine/parallel/pipeline/meta_pipeline.* \
  contrib/pg_volvec/src/engine/parallel/pipeline/physical_*.* 2>/dev/null
```
- Expected: empty.

**QA-P3b'-8: Translator/RuntimeBinder argument separation (Oracle B4)**
```bash
grep -E "PipelineSharedControl|pg_atomic_uint64|ParallelAggPartialState|SharedFileSet" \
  contrib/pg_volvec/src/engine/parallel/pipeline/translator.hpp \
  contrib/pg_volvec/src/engine/parallel/pipeline/pg_plan_translator.hpp 2>/dev/null
```
- Expected: empty (translator headers must not reference PG-runtime types). `runtime_binder.hpp` is the only place those types appear.

### 6.8 Risk

**Medium-high.** Larger scope than R1 (~700 vs ~580 LOC; 12 new files + 6+ deleted) due to:
1. `VecPlanState` AST deletion ripples through bridge code and any operator currently dereferencing it.
2. Multi-pipeline shapes for Q1 (3 pipelines) and Q6 (2 pipelines) exercise breaker detection + depends_on machinery on day one — no single-pipeline grace period.
3. Distributed FinalAgg requires DSM-backed partitioned partial hash table (new infrastructure beyond what current PartialAggSink → leader-combine does).

Mitigated by:
1. Deterministic ID + depends_on → catchable via QA-P3b'-3/6 trace mismatch.
2. RuntimeBinder isolation → DuckPlanTranslator can be added later without churning translator interface.
3. Translator is whole-PlannedStmt accept-or-reject → no hybrid-execution edge cases.

### 6.9 Pre-implementation gate

Submit P3b' R2 design to **Momus** (round 6) AND **Oracle** (round 2) for re-review. Oracle round 2 must explicitly close §14.1 B1–B4 before implementation begins.

---

## 7. Phase P3d — Per-Query Worker Pool with Persistent Pipeline State

> **⚠️ SUPERSEDED by §15 (P3X) — see design §8.** RETAINED for historical context (§7's worker-pool design is the basis for design §8.5; that material now lives authoritatively in the design doc). Implementation follows §15 M-SCHED + M-BM.

### 7.1 Goal

Replace the current "1 bgworker = 1 pipeline = 1 process exit" model with a **per-query worker pool** in which workers spawn at query start, wait in a forever-loop for pipeline assignments, **persist process-local `pipeline_id → state` maps across pipelines within the same query**, and exit at query end.

This is your stated goal: *"线程池每个 query 启动好多 worker，worker 去拿任务"* + *"per-query workers, because I can save some status of the query in every process, such map a pipeline id to something."*

### 7.2 Why per-query (not postmaster-wide)

User decision (recorded §11). Rationale:

1. **State persistence requirement.** The user explicitly wants to map `pipeline_id → SomeState` per process and re-use that state across pipelines. Postmaster-wide pools would require per-query state lifecycle to be tracked separately from worker lifecycle, complicating cleanup. Per-query pool collapses these into one lifecycle: worker dies → all its query state dies with it.
2. **No `shared_preload_libraries` requirement.** Per-query pool uses the existing `BackgroundWorkerHandle` + dynamic registration path (`RegisterDynamicBackgroundWorker`), no postmaster preload needed.
3. **Simpler error model.** Worker crash kills only the current query; postmaster keeps running.
4. **Closer to PG idiom.** Mirrors how `parallel_workers` queries work today, just with persistent state across pipelines within one query instead of per-Gather-node.
5. **Citus precedent.** Citus's `DistributedExecution → WorkerPool → WorkerSession` is exactly this shape: pool tied to one execution, sessions reused across tasks within the execution, torn down at execution end.

### 7.3 Architectural decisions (USER-CONFIRMED)

| # | Decision | Rationale |
|---|---|---|
| 7.3.a | **Pool model**: per-query (not postmaster, not per-backend). | User-stated requirement (state persistence). |
| 7.3.b | **Pool size**: equals number of bgworkers actually launched (`launched_worker_count` from `LaunchParallelWorkers`). | Pool is bounded by what PG was willing to give us. |
| 7.3.c | **`launched_worker_count == 0` → ERROR.** | User decision. No "fallback to leader-only" for `pg_volvec` — we explicitly want worker process model for state isolation. If no workers, fail fast. |
| 7.3.d | **`launched_worker_count >= 1` → proceed with that pool size.** Leader does NO data work. | User decision. Leader is pure coordinator: launch workers, distribute pipeline assignments, drain results, send to client, handle errors. |
| 7.3.e | **`pg_volvec.parallel_leader_participation` GUC**: REMOVED. Leader is always coordinator-only in the pipeline path. | User decision. Simplification; matches the per-query pool's pure-coordinator role. |
| 7.3.f | **`pg_volvec.use_worker_pool` GUC**: REMOVED. Per-query pool is the only path. | User decision. The per-query pool is so close to current `LaunchParallelWorkers` shape that there's no meaningful "legacy path" worth A/B testing. |
| 7.3.g | **`pg_volvec.pool_size` GUC**: REMOVED. Pool size = `launched_worker_count`. | Pool size is determined by PG bgworker availability + `pg_volvec.parallel_max_workers`, not a separate GUC. |
| 7.3.h | **State map data structure**: `std::vector<std::unique_ptr<WorkerPipelineState>>` indexed by dense `PipelineId`. | Dense IDs (assigned by P3b' MetaPipeline) make vector indexing safe. Faster than hashmap; no allocations per lookup. |
| 7.3.i | **Snapshot/transaction propagation**: reuse PG's `InitializeParallelDSM` machinery. Workers call `ParallelWorkerMain`-equivalent setup ONCE at spawn (`WorkerJoinQuery()`); per-pipeline cycle handles only `Push/PopActiveSnapshot`, `EnterParallelMode`/`ExitParallelMode`, and per-pipeline error scope. | Research finding `bg_4804dd72`: 36 of 42 setup steps in `ParallelWorkerMain` are once-per-worker; only 6 must be per-pipeline. |
| 7.3.j | **State types supported in registry**: opaque `void*` slot per pipeline id, plus typed accumulators owned by individual operators (e.g. JIT'd function pointers in `PipelineWorkerState::jit_context`, hash table partitions for future HashJoin, operator-local accumulators). The registry is the *index*; specific operators own their typed state. | Matches DuckDB pattern (`PipelineExecutor` holds typed `local_source_state`/`local_sink_state`; cross-pipeline state lives in operator-owned globals). |
| 7.3.k | **Single `ParallelContext` per query.** Created once at query start (after pipeline build), reused across all pipelines, destroyed once at query end. **No DSM reinit between pipelines, no second `LaunchParallelWorkers`.** | Oracle §14.4: DSA lifetime stays valid only under this invariant; eliminates an entire class of "DSM was for pipeline N, now I'm in N+1" bugs. |
| 7.3.l | **No PG-runtime re-init in pg_volvec entrypoint.** `ParallelWorkerMain` (PG infra) already does library/GUC/connection/transaction/snapshot restore + `EnterParallelMode` + `PushActiveSnapshot` ONCE before our entrypoint runs. pg_volvec entrypoint does ONLY pg_volvec-local one-time init then enters the assignment loop. | Oracle §14.3 B1: `WorkerJoinQuery()` re-doing this hits assertion-protected paths. Verified against `src/backend/access/transam/parallel.c::ParallelWorkerMain`. |
| 7.3.m | **No per-pipeline `Push/PopActiveSnapshot`, no per-pipeline `EnterParallelMode/ExitParallelMode`.** Snapshot + parallel mode stay active for the entire query inside the worker loop. | Oracle §14.3 B2/B3: per-pipeline `GetTransactionSnapshot()` can return a *different* snapshot under READ COMMITTED, diverging from leader; per-pipeline pushes also create unbalanced unwind paths on `ereport(ERROR)`. |
| 7.3.n | **Memory strategy: `MemoryContext` + explicit cleanup, POD-style operator/source/sink local state.** No `std::unique_ptr`/`std::vector` of non-trivially-destructible objects on stack inside `PG_TRY` scope. | Oracle §14.3 B6: PG `ereport(ERROR)` does `longjmp` → C++ destructors skipped → leak per pipeline in forever-loop model. POD locals + `MemoryContextDelete` is the only `longjmp`-safe pattern. |
| 7.3.o | **Completion signaling: DSM atomic counters + PG `ConditionVariable`. No completion `shm_mq`.** Each worker increments `completion_counter[pipeline_id]` atomically when it finishes a pipeline; coordinator waits on a `ConditionVariable` until counter reaches `nworkers_launched`. | Oracle §14.3 B4: completion `shm_mq` deadlocks when worker `PG_RE_THROW`s without sending. Atomic counter + CV + `WaitForParallelWorkersToFinish` give crash-safe wait. |
| 7.3.p | **Coordinator failure handling: fan-out abort + let PG's propagated `ErrorResponse` raise.** On first detected error (`worker_error != 0` OR `CHECK_FOR_INTERRUPTS()` raises OR worker handle reports `BGWH_STOPPED` while pipeline incomplete), coordinator calls `TerminateBackgroundWorker` on all worker handles, then `WaitForParallelWorkersToFinish`, then `DestroyParallelContext`. The original error reaches the client via PG's standard error-queue propagation. | Oracle §14.3 B7: returning `false` with a generic message discards PG's `ErrorResponse` with file/line/SQLSTATE. |

### 7.4 Design (R2 — rewritten per Oracle §14.7 binding direction)

> **Reading guide.** §7.4.1 establishes what `ParallelWorkerMain` already did (so we know what NOT to redo) and the worker entrypoint shape. §7.4.2 is the registry, now POD-only. §7.4.3 fixes the `before_shmem_exit` use-after-free. §7.4.4 is the coordinator wait/abort path. §7.4.5 is the DSM-atomic completion mechanism replacing `shm_mq`. §7.4.6 is the memory/RAII discipline. §7.4.7 is the single-`ParallelContext` invariant. §7.4.8 is the assignment channel.

#### 7.4.1 Worker entrypoint (forever-loop, NO PG-runtime re-init)

**What `ParallelWorkerMain` already did before our entrypoint runs** (verified against `src/backend/access/transam/parallel.c`):

- `RestoreLibraryState`, `RestoreGUCState`, `RestoreClientConnectionInfo`, `SetCurrentRoleId`
- `StartParallelWorkerTransaction`, snapshot restore + `PushActiveSnapshot(asnapshot)` ONCE
- `EnterParallelMode()` ONCE
- DSM segment attached; `shm_toc` available via `GetCurrentParallelStateToc`-equivalent

**Therefore the pg_volvec entrypoint must NOT call any of**: `Restore*State`, `Start*Transaction`, `End*Transaction`, `PushActiveSnapshot`, `PopActiveSnapshot`, `EnterParallelMode`, `ExitParallelMode`, `RegisterDynamicBackgroundWorker`, library/GUC re-init. (Several of these are assertion-protected against re-entry.)

**The worker entrypoint shape** (replaces old `WorkerJoinQuery`/`WorkerLeaveQuery` split — both deleted):

```
pg_volvec_pipeline_worker_main(Datum arg):                [PG infra calls this]
  // ENTRYPOINT_INIT: pg_volvec-local one-time init only
  shm_toc *toc            = shm_toc_attach(PG_VOLVEC_MAGIC, seg->mapped_address);
  control                 = (PipelineSharedControl *) shm_toc_lookup(toc, KEY_CONTROL, false);
  PlannedStmt *pstmt      = deserialize_plannedstmt(toc);
  dsa_area *dsa           = dsa_attach_in_place(...);                  // FIXES P3-0
  worker_query_ctx        = AllocSetContextCreate(TopMemoryContext,
                              "pg_volvec worker query", ALLOCSET_DEFAULT_SIZES);
  MemoryContext oldctx    = MemoryContextSwitchTo(worker_query_ctx);

  // Re-run translator + MetaPipeline locally so worker has identical PhysicalOp tree
  // and identical dense PipelineIds as leader (Determinism Contract from §6).
  PhysicalOperator *root  = PgPlanTranslator{}.Translate(pstmt);    // pure plan→IR
  RuntimeBinder binder{control, &control->next_block, dsa, /*partial_slots=*/...};
  MetaPipeline meta       = MetaPipeline::Builder{}.Build(*root);   // dense IDs 0..N-1
  binder.BindAll(meta);                                             // injects PG-runtime refs

  WorkerPipelineRegistry *registry =
      MemoryContextAlloc(worker_query_ctx, sizeof(WorkerPipelineRegistry));
  WorkerPipelineRegistry_Init(registry, meta.PipelineCount(), worker_query_ctx);

  // Process-stable context (§7.4.3); registered EXACTLY ONCE per process.
  g_worker_ctx.state    = NULL;     // populated below
  g_worker_ctx.registry = registry;
  g_worker_ctx.query_ctx = worker_query_ctx;
  before_shmem_exit(WorkerCleanupCallback, PointerGetDatum(&g_worker_ctx));

  MemoryContextSwitchTo(oldctx);

  // ASSIGNMENT LOOP — runs until QUERY_TEAR_DOWN or fatal error.
  // NO Push/Pop/Enter/Exit anywhere in this loop.
  for (;;) {
    CHECK_FOR_INTERRUPTS();
    PipelineAssignment msg;
    if (!ReceivePipelineAssignment(&msg, /*timeout_ms=*/1000))
      continue;

    if (msg.kind == QUERY_TEAR_DOWN)
      break;

    Assert(msg.kind == PIPELINE_RUN);
    Assert(msg.pipeline_id < meta.PipelineCount());

    PG_TRY();
    {
      // PIPELINE_BEGIN
      WorkerPipelineState *state = WorkerPipelineRegistry_LookupOrCreate(
          registry, msg.pipeline_id, meta.PipelineAt(msg.pipeline_id));

      // Per-pipeline transient context, child of worker_query_ctx; reset between runs.
      MemoryContext old = MemoryContextSwitchTo(state->per_pipeline_ctx);
      WorkerPipelineExecutor_Run(state, &meta.PipelineAt(msg.pipeline_id));
      MemoryContextSwitchTo(old);

      // PIPELINE_END
      pg_atomic_fetch_add_u32(&control->completion_counters[msg.pipeline_id], 1);
      ConditionVariableBroadcast(&control->completion_cv);

      MemoryContextReset(state->per_pipeline_ctx);   // drop transients only
    }
    PG_CATCH();
    {
      // worker_error tells coordinator "abort the query"; PG's standard error
      // queue propagation carries the original ereport details to the leader.
      pg_atomic_write_u32(&control->worker_error, 1);
      ConditionVariableBroadcast(&control->completion_cv);   // unblock coordinator
      PG_RE_THROW();    // bgworker dies; PG infra runs before_shmem_exit → cleanup
    }
    PG_END_TRY();
  }

  // ENTRYPOINT_EXIT: nothing to do.
  // worker_query_ctx + registry are freed by WorkerCleanupCallback at proc_exit.
  // Snapshot pop, parallel-mode exit, transaction end, DSM detach are done by
  // ParallelWorkerMain AFTER our entrypoint returns. We do NOT touch them.
```

**Key invariants** (each enforced by code review + QA):

1. Zero calls to `PushActiveSnapshot`, `PopActiveSnapshot`, `EnterParallelMode`, `ExitParallelMode`, `Start*Transaction`, `End*Transaction`, `Restore*State` inside `pg_volvec/src/`.
2. The `PG_TRY` block contains *only* POD-style locals (no C++ classes with non-trivial destructors). All operator/source/sink local state lives in `worker_query_ctx` or its child `per_pipeline_ctx`.
3. `worker_error` is written before `PG_RE_THROW()` so coordinator sees the abort even if PG's error queue is slow to propagate.

#### 7.4.2 `WorkerPipelineRegistry` (POD-only, MemoryContext-allocated)

`pipeline/worker_pipeline_registry.{hpp,cpp}`:

```cpp
namespace pg_volvec::pipeline {

// POD struct — no constructor, no destructor, no std::unique_ptr/std::vector.
// All "owned" sub-state lives in worker_query_ctx/per_pipeline_ctx and is freed
// by MemoryContextDelete, NOT by destructors (longjmp-safe).
struct WorkerPipelineState {
    PipelineId      id;                   // INVALID_PIPELINE_ID = unused slot
    MemoryContext   per_pipeline_ctx;     // child of worker_query_ctx
    void           *source_local_state;   // operator-typed; allocated in per_pipeline_ctx
    void          **operator_local_states;// array, length = operator_count
    int             operator_count;
    void           *sink_local_state;     // operator-typed; allocated in per_pipeline_ctx
    void           *jit_deform_fn;        // JIT'd; lives in JIT context, not MC
    void           *jit_expr_fn;
    void           *opaque;               // future use (hash partitions, etc.)
};

struct WorkerPipelineRegistry {
    int                  capacity;        // = meta.PipelineCount()
    WorkerPipelineState *slots;           // length=capacity, allocated in worker_query_ctx
    MemoryContext        owning_ctx;      // = worker_query_ctx (parent)
};

void  WorkerPipelineRegistry_Init(WorkerPipelineRegistry *r, int n_pipelines,
                                  MemoryContext owning_ctx);
WorkerPipelineState *WorkerPipelineRegistry_LookupOrCreate(
                       WorkerPipelineRegistry *r,
                       PipelineId id,
                       const Pipeline &template_pipeline);
void  WorkerPipelineRegistry_CleanupPipeline(WorkerPipelineRegistry *r, PipelineId id);
void  WorkerPipelineRegistry_CleanupAll(WorkerPipelineRegistry *r);

}  // namespace
```

`Init` zero-initializes `slots[*].id = INVALID_PIPELINE_ID`. `LookupOrCreate` returns `&slots[id]` if `id` matches; otherwise creates `slots[id].per_pipeline_ctx` as a child of `owning_ctx`, allocates `operator_local_states` array there, and lets the operator implementations populate `source_local_state`/`operator_local_states[i]`/`sink_local_state` via per-operator factory functions. `CleanupPipeline` calls `MemoryContextDelete(slots[id].per_pipeline_ctx)` — **single point of free**, longjmp-safe. `CleanupAll` iterates and deletes each child context (the parent `worker_query_ctx` cleanup also covers this transitively).

JIT'd function pointers live in the JIT module/context (managed separately by `llvmjit_*`), not in MemoryContext; their lifetime is tied to the worker process (cleaned up by JIT's own `before_shmem_exit` hook).

#### 7.4.3 Process-stable cleanup callback (FIXES bg_547616b5; null-safe + idempotent)

```cpp
// pipeline_worker_main.cpp (worker_local_context folded in here per Oracle B1 fix)
struct WorkerLocalContext {
    WorkerPipelineRegistry *registry;     // may be NULL after explicit cleanup
    MemoryContext           query_ctx;    // may be NULL after explicit cleanup
    PipelineSharedControl  *control;      // never NULL once init'd; do NOT free
};

static WorkerLocalContext g_worker_ctx = {NULL, NULL, NULL};

static void WorkerCleanupCallback(int code, Datum arg)
{
    WorkerLocalContext *ctx = (WorkerLocalContext *) DatumGetPointer(arg);
    if (ctx == NULL)
        return;

    // Null-safe: callback may run after explicit cleanup already happened
    // (e.g. on graceful QUERY_TEAR_DOWN path). Each branch checks + nulls.
    if (ctx->registry != NULL)
    {
        WorkerPipelineRegistry_CleanupAll(ctx->registry);
        ctx->registry = NULL;
    }
    if (ctx->query_ctx != NULL)
    {
        MemoryContextDelete(ctx->query_ctx);
        ctx->query_ctx = NULL;
    }
    // ctx->control points into DSM; PG infra detaches DSM; do NOT touch.
}
```

**Two ordering guarantees (REQUIRED for safety):**

1. `before_shmem_exit(WorkerCleanupCallback, ...)` is registered EXACTLY ONCE per worker process, immediately after `g_worker_ctx` is populated in `ENTRYPOINT_INIT`.
2. If we ever add an explicit cleanup path (currently we don't — we let `before_shmem_exit` do everything), it MUST null-out the `g_worker_ctx` fields BEFORE calling `MemoryContextDelete`/`Cleanup*`, so a re-entrant callback is a no-op.

**No re-registration on subsequent queries** — the per-query pool is per-postmaster-restart in this phase; if/when we extend to "worker survives across queries" (out of scope for P3d), the callback stays registered and `g_worker_ctx` is reset between queries.

#### 7.4.4 Coordinator wait + fan-out abort (FIXES Oracle B7; uses CHECK_FOR_INTERRUPTS + WaitForParallelWorkersToFinish)

The coordinator's per-pipeline wait integrates three signals: (a) atomic completion counter reaching `nworkers_launched`, (b) `worker_error` flag set by any worker's `PG_CATCH`, (c) PG's interrupt path delivering a worker's propagated `ErrorResponse`.

```cpp
bool PipelineCoordinator::WaitForPipelineCompletion(PipelineId pid)
{
    const uint32 expected = (uint32) pcxt_->nworkers_launched;
    ConditionVariablePrepareToSleep(&control_->completion_cv);
    for (;;)
    {
        // (c) Lets PG raise any propagated worker ErrorResponse via ereport.
        // If a worker called ereport(ERROR), this will longjmp out of Run().
        CHECK_FOR_INTERRUPTS();

        // (b) Worker explicitly flagged error in its PG_CATCH.
        if (pg_atomic_read_u32(&control_->worker_error) != 0)
        {
            ConditionVariableCancelSleep();
            FanOutAbort();
            return false;
        }

        // (a) All workers done with this pipeline.
        const uint32 done = pg_atomic_read_u32(&control_->completion_counters[pid]);
        if (done >= expected)
        {
            ConditionVariableCancelSleep();
            return true;
        }

        // Bounded wait; ConditionVariableTimedSleep returns true on timeout
        // so we loop back and re-check all three conditions.
        (void) ConditionVariableTimedSleep(&control_->completion_cv,
                                           /*timeout_ms=*/100,
                                           WAIT_EVENT_PARALLEL_FINISH);
    }
}

void PipelineCoordinator::FanOutAbort()
{
    // Stop any further pipeline dispatch.
    aborted_ = true;

    // Tell every worker to die. PG handles the SIGTERM via die() → CHECK_FOR_INTERRUPTS.
    for (int i = 0; i < pcxt_->nworkers_launched; i++)
        if (pcxt_->worker[i].bgwhandle != NULL)
            TerminateBackgroundWorker(pcxt_->worker[i].bgwhandle);

    // Block until all workers exit; this also drains their error queues so any
    // pending ErrorResponse is delivered into THIS backend's error queue.
    WaitForParallelWorkersToFinish(pcxt_);

    // DestroyParallelContext is called in Run()'s cleanup path (single owner);
    // we do NOT call it here so the cleanup is in one place.

    // Don't ereport our own generic error — let CHECK_FOR_INTERRUPTS in the
    // caller (next iteration of pipeline loop, or Run()'s post-loop check)
    // re-raise PG's propagated ErrorResponse with file/line/SQLSTATE intact.
    // If by some path no PG error is queued (worker died non-ERROR), the
    // worker_error flag triggers a final fallback ereport in Run().
}
```

**Error propagation semantics inside `FanOutAbort` (self-review clarification).** `WaitForParallelWorkersToFinish(pcxt_)` on line above internally services `HandleParallelMessages` via `CHECK_FOR_INTERRUPTS`. If any worker had queued an `ErrorResponse` on its error queue (the common path — worker called `ereport(ERROR)` and `PG_RE_THROW()`'d), `HandleParallelMessages` will `ereport` in THIS backend, which longjmps out of `FanOutAbort` into the caller's `PG_CATCH`. **This is the intended path.** The outer `PG_CATCH` in `Run()` (below) is re-entrancy-safe: `aborted_` is latched to `true` before `FanOutAbort` is called, so the `if (!aborted_) FanOutAbort()` inside the catch cannot recurse. If `WaitForParallelWorkersToFinish` returns normally (no queued ErrorResponse — e.g. worker SIGKILL'd before it could enqueue), control returns to `FanOutAbort`'s caller, the outer loop's next `CHECK_FOR_INTERRUPTS` finds `worker_error != 0`, and `Run()`'s post-loop fallback `ereport` (below) raises using the captured `failure_reason`. Either way, the query fails with a specific error; no generic "worker error" is emitted.

**Condition-variable prepare/sleep race.** `ConditionVariablePrepareToSleep` is called ONCE before the `for(;;)` loop, then `ConditionVariableTimedSleep` is called each iteration. A broadcast landing between `CHECK_FOR_INTERRUPTS` and `ConditionVariableTimedSleep` within the same iteration is NOT lost: PG's `ConditionVariable` implementation uses a `proclist` (see `src/backend/storage/lmgr/condition_variable.c`) that registers the waiter at prepare time, so any broadcast arriving before the next sleep is already visible — the subsequent `ConditionVariableTimedSleep` returns immediately. We rely on this invariant; no additional locking is required.

```cpp
bool PipelineCoordinator::Run(const char **failure_reason)
{
    bundle_ = BuildPipelines();                                  // §6 P3b'
    if (!bundle_) { *failure_reason = "shape unsupported"; return false; }

    // §7.3.k: SINGLE ParallelContext per query.
    pcxt_ = CreateParallelContext("pg_volvec", "pg_volvec_pipeline_worker_main",
                                  pg_volvec_parallel_max_workers);
    InitializePipelineSharedControl(pcxt_, bundle_->PipelineCount());
    SerializePlannedStmt(pcxt_->toc);
    AllocateDSAInDSM(pcxt_->toc);

    LaunchParallelWorkers(pcxt_);
    if (pcxt_->nworkers_launched == 0)
    {
        DestroyParallelContext(pcxt_);
        *failure_reason = "no workers launched";
        ereport(ERROR, (errmsg("pg_volvec: no parallel workers were launched"),
                        errhint("check max_worker_processes and max_parallel_workers")));
    }

    bool ok = true;
    PG_TRY();
    {
        for (auto &owned : bundle_->pipelines)   // pipelines are vectors of POD ptrs
        {
            if (aborted_) break;
            DispatchPipelineToAllWorkers(owned->pipeline.id);
            if (!WaitForPipelineCompletion(owned->pipeline.id)) { ok = false; break; }
            owned->sink->LeaderCombineAndFinalize();    // leader-only K-way merge etc.
        }

        if (!aborted_)
            DispatchTearDownToAllWorkers();
        WaitForParallelWorkersToFinish(pcxt_);
    }
    PG_CATCH();
    {
        // Either CHECK_FOR_INTERRUPTS raised, or sink->LeaderCombineAndFinalize ereport'd.
        // Ensure workers don't outlive us.
        if (!aborted_) FanOutAbort();
        DestroyParallelContext(pcxt_);
        PG_RE_THROW();
    }
    PG_END_TRY();

    DestroyParallelContext(pcxt_);
    if (!ok && pg_atomic_read_u32(&control_->worker_error) != 0)
        *failure_reason = "worker reported error (see preceding log)";
    return ok;
}
```

#### 7.4.5 DSM atomic completion counters + ConditionVariable (REPLACES per-worker completion shm_mq)

`pipeline/dsm_control.hpp` revisions:

```cpp
// REMOVED:  PIPELINE_DSM_KEY_COMPLETION_QUEUES
// ADDED:
#define PIPELINE_DSM_KEY_COMPLETION_COUNTERS  0x56505047   // length = sizeof(pg_atomic_uint32) * n_pipelines
#define PIPELINE_DSM_KEY_COMPLETION_CV        0x56505048   // length = sizeof(ConditionVariable)
// KEPT:
#define PIPELINE_DSM_KEY_ASSIGNMENT_QUEUES    0x56505049   // §7.4.8

struct PipelineSharedControl {
    pg_atomic_uint32  worker_error;        // 0=ok, 1=any worker errored
    pg_atomic_uint64  next_block;          // morsel allocator (existing)
    int               partial_slot_count;  // (existing)
    int               n_pipelines;         // = bundle_.PipelineCount(); set by leader
    // completion_counters[N] and completion_cv live in their own DSM keys (above)
};
```

Coordinator init (`InitializePipelineSharedControl`):

```cpp
shm_toc_estimate_chunk(&pcxt_->estimator,
                       sizeof(pg_atomic_uint32) * n_pipelines);
shm_toc_estimate_chunk(&pcxt_->estimator, sizeof(ConditionVariable));
// ... after InitializeParallelDSM:
pg_atomic_uint32 *counters = shm_toc_allocate(pcxt_->toc,
                                              sizeof(pg_atomic_uint32) * n_pipelines);
for (int i = 0; i < n_pipelines; i++) pg_atomic_init_u32(&counters[i], 0);
shm_toc_insert(pcxt_->toc, PIPELINE_DSM_KEY_COMPLETION_COUNTERS, counters);

ConditionVariable *cv = shm_toc_allocate(pcxt_->toc, sizeof(ConditionVariable));
ConditionVariableInit(cv);
shm_toc_insert(pcxt_->toc, PIPELINE_DSM_KEY_COMPLETION_CV, cv);
```

Worker reads `counters` + `cv` once at `ENTRYPOINT_INIT` and stashes pointers into its `WorkerLocalContext`. Increment + `ConditionVariableBroadcast(cv)` happen on `PIPELINE_END` (success) AND in `PG_CATCH` (so coordinator unblocks immediately on error).

**Why this beats `shm_mq`:** atomic counter is wait-free; `ConditionVariable` integrates with PG's `CHECK_FOR_INTERRUPTS` natively; no flow-control deadlock; no buffer-full corner cases; ~`8 bytes * n_pipelines + sizeof(ConditionVariable)` of DSM vs. `2 * launched * 64KB`.

#### 7.4.6 Memory & RAII discipline (FIXES Oracle B6)

**Hard rules** (enforced by code review + grep gate in QA-P3d-9):

1. Inside the `PG_TRY { ... }` block of the worker assignment loop, NO C++ class with a non-trivial destructor may be a stack local. Permitted: scalars, `Datum`, `MemoryContext`, raw pointers, POD structs.
2. All operator/source/sink local state is allocated in `state->per_pipeline_ctx` (a child of `worker_query_ctx`). Cleanup is a single `MemoryContextReset(per_pipeline_ctx)` (transients) or `MemoryContextDelete(per_pipeline_ctx)` (full release).
3. The `WorkerPipelineExecutor_Run` function takes `WorkerPipelineState *` by pointer; it does NOT construct or own the state. Internally it MAY create POD locals, but must NOT hold `std::unique_ptr`/`std::vector<unique_ptr>` across operator calls (since operator calls may `ereport(ERROR)`).
4. `worker_query_ctx` is a child of `TopMemoryContext`. PG's `before_shmem_exit` runs while `TopMemoryContext` still exists, so `MemoryContextDelete(worker_query_ctx)` from the cleanup callback is safe.
5. JIT'd function pointers are NOT in MemoryContext — they are in the LLVM JIT module, owned by `llvmjit_*`. A separate JIT cleanup hook (already exists pre-P3d) handles them.

**What this rules OUT:** the current `WorkerPipelineExecutor::Execute` pattern of `std::vector<std::unique_ptr<OperatorState>>` on the C++ stack is FORBIDDEN inside `PG_TRY`. P3d rewrites it to take a pointer to `WorkerPipelineState->operator_local_states` (allocated in `per_pipeline_ctx`).

#### 7.4.7 Single-`ParallelContext` invariant (Oracle §14.4)

- ONE `CreateParallelContext` per query, in `Run()` before the pipeline loop.
- ONE `LaunchParallelWorkers` per query.
- ONE `DestroyParallelContext` per query, in `Run()`'s post-loop cleanup OR `PG_CATCH`.
- DSM segment + DSA + control struct + completion counters + completion CV are allocated ONCE inside `InitializeParallelDSM` and live for the entire query.
- **No** per-pipeline `InitializeParallelDSM`, `LaunchParallelWorkers`, `DestroyParallelContext`, or DSM re-allocation.

This makes the "DSA pointer captured for pipeline N is still valid in pipeline N+1" property a structural invariant rather than a discipline.

#### 7.4.8 Assignment channel (per-worker `shm_mq` — kept)

Assignment direction (leader→worker) keeps the per-worker `shm_mq` from the prior design, because:

- It's low-traffic (one message per pipeline per worker) and bounded.
- It's the natural PG idiom for leader→worker control (matches parallel apply workers).
- Worker can `shm_mq_receive` with `nowait=false` and a short timeout, integrating with `CHECK_FOR_INTERRUPTS` naturally.

```cpp
struct PipelineAssignment {
    enum class Kind : uint8 { PIPELINE_RUN, QUERY_TEAR_DOWN } kind;
    PipelineId pipeline_id;     // INVALID_PIPELINE_ID for QUERY_TEAR_DOWN
};
```

Per-worker queue size: 1 KB. Total DSM cost: `launched * 1 KB`, well below 1 MB even for `parallel_max_workers=64`.

### 7.5 Files touched (R2)

- **NEW** `pipeline/worker_pipeline_registry.{hpp,cpp}` — POD `WorkerPipelineState` + registry + `LookupOrCreate`/`CleanupPipeline`/`CleanupAll` (~220 LOC)
- **NEW** `pipeline/pipeline_assignment.hpp` — `PipelineAssignment` POD + key macros (~40 LOC)
- **MODIFY** `pipeline/pipeline_worker_main.cpp` — folds in the former `worker_local_context` + new entrypoint structure: one-time init, forever-loop on assignments, no Push/Pop/Enter/Exit, null-safe `before_shmem_exit` callback (~280 LOC delta net larger)
- **MODIFY** `pipeline/pipeline_worker_state.cpp` — strip the old `WorkerJoinQuery`/`WorkerLeaveQuery` separation; what remains is just shared helpers used by both leader and worker (~−80 LOC delta — net smaller)
- **MODIFY** `pipeline/coordinator.cpp` (introduced in P3b) — multi-pipeline dispatch loop, `WaitForPipelineCompletion`, `FanOutAbort`, single-`ParallelContext` lifecycle (~180 LOC delta)
- **MODIFY** `pipeline/dsm_control.hpp` — DROP `PIPELINE_DSM_KEY_COMPLETION_QUEUES`; ADD `PIPELINE_DSM_KEY_COMPLETION_COUNTERS`, `PIPELINE_DSM_KEY_COMPLETION_CV`; extend `PipelineSharedControl` with `n_pipelines`
- **MODIFY** `pipeline/executor.cpp` — `WorkerPipelineExecutor_Run` accepts `WorkerPipelineState *`; uses `per_pipeline_ctx` for all transients; no C++ destructors inside `PG_TRY` (~100 LOC delta)
- **MODIFY** `bridge/pg_volvec.c` — DROP GUCs `parallel_leader_participation`, `use_worker_pool`, `pool_size`; ADD debug GUCs `test_emit_two_pipelines`, `test_block_all_workers`
- **MODIFY** `pipeline/AGENTS.md` — document forever-loop entrypoint, registry POD discipline, MemoryContext memory rule, single-`ParallelContext` invariant, DSM atomic completion mechanism
- **MODIFY** `meson.build` — add 2 new sources (registry .cpp + assignment header is header-only)

Total: ~10 files touched (8 modify + 2 new files added; 1 file/pair from the prior R1 design — `worker_local_context.{hpp,cpp}` — folded into `pipeline_worker_main.cpp` per Oracle B1 fix). ~750 LOC delta.

### 7.6 Acceptance criteria (R2)

- [ ] Regress trio passes (`smoke`, `q1`, `q6`).
- [ ] Workers spawn ONCE per query; `pg_stat_activity` snapshot during a Q1 run shows `nworkers_launched` workers active for the entire query, all exit at query end.
- [ ] Per-pipeline dispatch latency (second pipeline within same query) ≤ 1 ms (no bgworker re-spawn cost).
- [ ] `WorkerPipelineRegistry` correctly holds state across pipeline boundaries (NOTICE trace: same `state_addr` for same `pipeline_id` across two sequential assignments in the same worker).
- [ ] `worker_error` propagation works: injected ERROR in worker → leader receives PG's propagated `ErrorResponse` with original SQLSTATE/file/line, NOT a generic "worker error during pipeline".
- [ ] `launched_worker_count == 0` produces clean ERROR with `errhint`, not a hang or silent fallback.
- [ ] Three GUCs deleted (`parallel_leader_participation`, `use_worker_pool`, `pool_size`); zero references in `contrib/pg_volvec/src/`.
- [ ] No `before_shmem_exit` callback registered against stack memory (callback registered against `&g_worker_ctx`).
- [ ] Zero calls in `contrib/pg_volvec/src/` to `PushActiveSnapshot`, `PopActiveSnapshot`, `EnterParallelMode`, `ExitParallelMode`, `Restore*State`, `Start*Transaction`, `End*Transaction` (verified by grep gate).
- [ ] No `std::unique_ptr` or `std::vector` of non-trivially-destructible types as stack locals inside any `PG_TRY` block in worker code (verified by grep gate).
- [ ] Single `ParallelContext` per query: exactly ONE call to `CreateParallelContext`, ONE to `LaunchParallelWorkers`, ONE to `DestroyParallelContext` per `Run()` invocation (verified by NOTICE trace).
- [ ] Fan-out abort works: kill -9 of one worker mid-pipeline → coordinator detects via `WaitForParallelWorkersToFinish` returning early or `worker_error` flag → calls `TerminateBackgroundWorker` on remaining workers → query errors out cleanly within ≤ 5 s.
- [ ] `git diff --stat` ≤ 11 files, ≤ 800 LOC delta.

### 7.6.1 QA scenarios (executable, R2)

**QA-P3d-1: Build + regress trio** — Expected `Fail: 0`.

**QA-P3d-2: Worker process lifecycle (one query = one worker spawn)**

Implementation requirement: emit `elog(NOTICE, "pg_volvec[worker] pid=%d phase=%s pipeline_id=%d", MyProcPid, phase, id)` for phases `ENTRYPOINT_INIT`, `PIPELINE_BEGIN`, `PIPELINE_END`, `ENTRYPOINT_EXIT`.

```bash
\timing on
PGOPTIONS='-c statement_timeout=60s' ./installed/bin/psql -h /tmp -p 5432 -d tpch \
  -c "SET pg_volvec.parallel_max_workers=4;" \
  -f contrib/pg_volvec/sql/q1.sql 2>&1 | tee /tmp/p3d_lifecycle.log
grep "pg_volvec\[worker\]" /tmp/p3d_lifecycle.log
```
- Expected: exactly 4 distinct worker pids. Each pid shows: `ENTRYPOINT_INIT` (once) → `PIPELINE_BEGIN/END` for every pipeline in the query → `ENTRYPOINT_EXIT` (once). No pid shows two `ENTRYPOINT_INIT` lines.

**QA-P3d-3: State persistence across pipelines**

Synthetic test: enable `pg_volvec.test_emit_two_pipelines=on` so the same Q1-shape pipeline runs twice in sequence within one `Run()`. Implementation requirement: registry emits `elog(NOTICE, "pg_volvec[registry] pid=%d pipeline_id=%u state_addr=%p source=%s", MyProcPid, id, state, source)` where `source ∈ {lookup, create}`.

```bash
\timing on
PGOPTIONS='-c statement_timeout=60s' ./installed/bin/psql -h /tmp -p 5432 -d tpch \
  -c "SET pg_volvec.test_emit_two_pipelines=on;" \
  -f contrib/pg_volvec/sql/q1.sql 2>&1 | tee /tmp/p3d_state.log
grep "pg_volvec\[registry\]" /tmp/p3d_state.log
```
- Expected per worker pid: first occurrence of `pipeline_id=0` is `source=create`; second occurrence is `source=lookup` with **identical `state_addr`**.

**QA-P3d-4: Per-pipeline dispatch latency**

Implementation requirement: leader emits `elog(NOTICE, "pg_volvec[timing] pipeline_dispatch_us=%lld pipeline_idx=%d", us, idx)`.

```bash
\timing on
PGOPTIONS='-c statement_timeout=60s' ./installed/bin/psql -h /tmp -p 5432 -d tpch \
  -c "SET pg_volvec.test_emit_two_pipelines=on;" \
  -f contrib/pg_volvec/sql/q1.sql 2>&1 | grep pipeline_dispatch_us
```
- Expected: `pipeline_idx=0` may be 50–100 ms (cold worker spawn + DSM setup); `pipeline_idx=1` ≤ 1000 µs (no spawn cost).

**QA-P3d-5: `launched == 0` produces ERROR**

Force PG to refuse workers (e.g. `max_worker_processes=0` in `postgresql.conf`, restart), then:
```bash
\timing on
PGOPTIONS='-c statement_timeout=10s' ./installed/bin/psql -h /tmp -p 5432 -d tpch \
  -f contrib/pg_volvec/sql/q1.sql 2>&1 | tee /tmp/p3d_zero.log
grep -E "ERROR|FATAL|HINT" /tmp/p3d_zero.log
echo "Last exit: $?"
```
- Expected: psql exits non-zero; output contains `ERROR: pg_volvec: no parallel workers were launched` plus the `errhint` referencing `max_worker_processes`. NO silent leader fallback.

**QA-P3d-6: Worker error propagation preserves original ereport details**

Rebuild with `-DPG_VOLVEC_TEST_WORKER_ERROR` (which makes one operator `ereport(ERROR, errcode(ERRCODE_INTERNAL_ERROR), errmsg("p3d injection at %s:%d", __FILE__, __LINE__))`):
```bash
CCACHE_DISABLE=1 PATH=/opt/homebrew/bin:$PATH \
  CFLAGS="-DPG_VOLVEC_TEST_WORKER_ERROR" CXXFLAGS="-DPG_VOLVEC_TEST_WORKER_ERROR" \
  meson compile -C build pg_volvec
meson install -C build --only-changed
./installed/bin/pg_ctl -D ~/data/pg_tpch restart -m fast
\timing on
PGOPTIONS='-c statement_timeout=30s' ./installed/bin/psql -h /tmp -p 5432 -d tpch \
  -f contrib/pg_volvec/sql/q1.sql 2>&1 | tee /tmp/p3d_err.log
grep -E "(p3d injection|SQLSTATE)" /tmp/p3d_err.log
```
- Expected: leader-side ERROR contains the literal injection message including file:line. Generic message "worker error during pipeline" must NOT appear (we let PG's propagated ErrorResponse raise).

**QA-P3d-7: GUC removal**
```bash
grep -nE "parallel_leader_participation|use_worker_pool|pool_size" contrib/pg_volvec/src/
```
- Expected: zero matches in `src/`. Matches in `docs/` or comments explicitly tagged as historical are OK.

**QA-P3d-8: Workers exit cleanly (no leak)**
```bash
PGOPTIONS='-c statement_timeout=60s' ./installed/bin/psql -h /tmp -p 5432 -d tpch \
  -f contrib/pg_volvec/sql/q1.sql > /dev/null
sleep 5
./installed/bin/psql -h /tmp -p 5432 -d postgres -c \
  "SELECT count(*) FROM pg_stat_activity WHERE backend_type='background worker' AND application_name LIKE '%pg_volvec%';"
```
- Expected: count = 0.

**QA-P3d-9: Forbidden-API grep gate (Oracle B1/B2 enforcement)**
```bash
grep -RnE "PushActiveSnapshot|PopActiveSnapshot|EnterParallelMode|ExitParallelMode|RestoreLibraryState|RestoreGUCState|RestoreClientConnectionInfo|SetCurrentRoleId|StartParallelWorkerTransaction|EndParallelWorkerTransaction" contrib/pg_volvec/src/
```
- Expected: zero matches.

**QA-P3d-10: RAII grep gate (Oracle B6 enforcement)**

Inside `pipeline_worker_main.cpp`'s assignment loop body (between `PG_TRY` and `PG_END_TRY`), search for forbidden patterns:
```bash
awk '/PG_TRY/,/PG_END_TRY/' contrib/pg_volvec/src/engine/parallel/pipeline/pipeline_worker_main.cpp | \
  grep -nE "std::unique_ptr|std::vector|std::shared_ptr|std::string\b"
```
- Expected: zero matches. (Pointers are fine; only direct stack ownership is forbidden.)

**Note:** this grep is a **smoke gate**, not a complete RAII enforcement. It misses `std::map`, `std::set`, `std::optional<T>` with non-trivial `T`, `std::function`, user-defined RAII helpers (e.g. `ScopedMemoryContext`), and lambdas capturing non-trivial types. Full enforcement is via code review at the implementation PR, where every stack local inside `PG_TRY` is checked against §7.4.6's 5 hard rules. The grep catches the ~95% case (standard-library containers) and fails loudly in CI.

**QA-P3d-11: Single-`ParallelContext` invariant**

Implementation requirement: leader emits `elog(NOTICE, "pg_volvec[pcxt] phase=%s nworkers=%d", phase, n)` for `phase ∈ {CREATE, LAUNCH, DESTROY}`.
```bash
\timing on
PGOPTIONS='-c statement_timeout=60s' ./installed/bin/psql -h /tmp -p 5432 -d tpch \
  -c "SET pg_volvec.test_emit_two_pipelines=on;" \
  -f contrib/pg_volvec/sql/q1.sql 2>&1 | grep "pg_volvec\[pcxt\]"
```
- Expected: exactly one `CREATE`, one `LAUNCH`, one `DESTROY` per query. Across the two pipelines emitted, no second CREATE/LAUNCH.

**QA-P3d-12: Fan-out abort on worker crash**

Run Q1 in background; from another shell `kill -9` one worker pid mid-pipeline:
```bash
( PGOPTIONS='-c statement_timeout=60s' ./installed/bin/psql -h /tmp -p 5432 -d tpch \
    -f contrib/pg_volvec/sql/q1.sql 2>&1 | tee /tmp/p3d_crash.log ) &
psql_pid=$!
sleep 1
worker_pid=$(./installed/bin/psql -h /tmp -p 5432 -d postgres -tAc \
  "SELECT pid FROM pg_stat_activity WHERE backend_type='background worker' AND application_name LIKE '%pg_volvec%' LIMIT 1;")
kill -9 "$worker_pid"
wait "$psql_pid"
grep -E "ERROR|TerminateBackgroundWorker|server closed" /tmp/p3d_crash.log
```
- Expected: query errors out within ≤ 5 s; log shows leader-side ERROR; no remaining `pg_volvec` workers in `pg_stat_activity` after 5 s; no orphaned DSM segment (verify with `ipcs -m` count before/after).

### 7.7 Risk

**High.** Touches PG bgworker lifecycle, snapshot management (by NOT touching it — must verify nothing accidentally does), error scoping across longjmp, control channels, MemoryContext discipline, and process-stable callback registration. Mitigated by:

1. P3-0 (`worker_error` + `dsa_attach_in_place`) lands first — prerequisites.
2. P3a/P3b extract the coordinator into its own class — reduces blast radius.
3. P3b' assigns dense pipeline IDs deterministically — registry vector indexing is safe.
4. QA-P3d-2/3/9/10/11 are grep+NOTICE gates that fail loudly on the highest-impact regressions (lifecycle, state persistence, forbidden APIs, RAII rule, single-pcxt invariant).
5. Process-stable `g_worker_ctx` design (§7.4.3) explicitly addresses the latent stack-pointer hazard found in research (`bg_547616b5`).
6. DSM atomic + ConditionVariable for completion (§7.4.5) eliminates the `shm_mq` deadlock class entirely.
7. Fan-out abort + `WaitForParallelWorkersToFinish` (§7.4.4) ensures no orphaned workers on error paths.

### 7.8 Pre-implementation gate

Submit P3d R2 design (especially §7.4.1 worker entrypoint + §7.4.4 wait/abort + §7.4.5 DSM completion + §7.4.6 RAII rule + §7.4.7 single-pcxt invariant) to **Oracle (Round 2 — REQUIRED)** and **Momus (Round 6)** before coding. Oracle re-review must clear all of B1–B7 from §14.3.

---

## 7E. Phase P3e — Async / Blocked Task Support

### 7E.1 Goal

When a task can't make progress (e.g. waiting on I/O, or downstream sink is full), the worker must yield instead of blocking the slot. Equivalent to DuckDB's `INTERRUPTED` task return + `InterruptState` re-add.

### 7E.2 Why last

P3a–P3d give us a working pool with synchronous tasks. P3e is an **optimisation** for I/O-bound workloads. Q1/Q6 are CPU-bound on hot data, so this is genuinely lowest-priority.

### 7E.3 Design sketch

- `WorkerPipelineExecutor::Execute` returns `INTERRUPTED` when source returns `BLOCKED` (today: `ereport(ERROR)`).
- Pool worker, on `INTERRUPTED`, re-enqueues task to global queue with a "ready check" callback (e.g. "I/O completion latch").
- New shared "blocked task table" in DSM keyed by task_id.
- When ready-check fires (e.g. PG's `WaitEventSet`), task moves back to runnable queue.

### 7E.4 Acceptance

- [ ] Can construct a synthetic test where source artificially returns BLOCKED 50% of the time and pipeline still completes correctly.
- [ ] No deadlock when all workers are blocked simultaneously.
- [ ] Regress trio passes.

### 7E.4.1 QA scenarios (executable)

**Pre-flight:** add behind a build-time flag `PG_VOLVEC_TEST_BLOCKED=1` an injection point in `seq_scan_source.cpp::GetData` that returns `SourceResultType::BLOCKED` on 50% of calls (deterministic via PRNG seeded from `worker_index`).

**QA-P3e-1: Correctness with injected BLOCKED returns**
```bash
CCACHE_DISABLE=1 PATH=/opt/homebrew/bin:$PATH PG_VOLVEC_TEST_BLOCKED=1 meson compile -C build pg_volvec
meson install -C build --only-changed
./installed/bin/pg_ctl -D ~/data/pg_tpch restart -m fast
./installed/bin/psql -h /tmp -p 5432 -d tpch -f contrib/pg_volvec/sql/q1.sql > /tmp/p3e_q1.out
diff /tmp/p3e_q1.out contrib/pg_volvec/expected/q1.out
echo "DIFF_EXIT=$?"
```
- Expected: `DIFF_EXIT=0` (output matches non-blocked baseline byte-for-byte).

**QA-P3e-2: All-workers-blocked deadlock check**

Implementation requirement: a debug GUC `pg_volvec.test_block_all_workers=on` makes every worker return BLOCKED on first 100 calls.
```bash
PGOPTIONS='-c statement_timeout=30s' ./installed/bin/psql -h /tmp -p 5432 -d tpch -c "SET pg_volvec.test_block_all_workers=on;" -f contrib/pg_volvec/sql/q1.sql
echo "EXIT=$?"
```
- Expected: `EXIT=0` (query completes within 30s; no `statement_timeout` ERROR, no deadlock). A timeout would produce `ERROR: canceling statement due to statement timeout` and non-zero exit.

**QA-P3e-3: Regress trio with normal path** — same commands as QA-P3a-3.

### 7E.5 Risk

**Medium-high.** Async correctness is hard. Defer until a real I/O-bound workload demands it.

---

## 8. Pre-Existing Bugs to Fix Before P3a

Both are mentioned in `pipeline/AGENTS.md` as design but not implemented. Trivial fixes; do them as Phase **P3-0** before P3a.

### 8.1 `worker_error` is never written

**Location:** `pipeline_leader.cpp:201` initialises `pg_atomic_init_u32(&control->worker_error, 0)`. Workers can `ereport(ERROR)` without ever setting it. Leader can't distinguish worker death from clean FINISHED.

**Fix:** Add helper in `pipeline_worker_main.cpp`:
```cpp
PG_TRY();
{
    RunPipelineWorkerBody(seg, toc);
}
PG_CATCH();
{
    auto *control = (PipelineSharedControl *)
        shm_toc_lookup(toc, PIPELINE_DSM_KEY_CONTROL, true);
    if (control != nullptr)
        pg_atomic_write_u32(&control->worker_error, 1);
    PG_RE_THROW();
}
PG_END_TRY();
```

And in leader after `WaitForParallelWorkersToFinish`:
```cpp
if (pg_atomic_read_u32(&control->worker_error) != 0)
    ereport(ERROR, ...);
```

### 8.2 `exec_ctx.dsa = nullptr` in workers

**Location:** `pipeline_worker_main.cpp:86` and `pipeline_leader.cpp:278`. Leader allocates DSA in DSM (`PIPELINE_DSM_KEY_DSA`), but workers never `dsa_attach_in_place` it. Any operator that needs DSA in a worker silently NULL-derefs.

**Fix:** In `RunPipelineWorkerBody`:
```cpp
char *dsa_space = (char *) shm_toc_lookup(toc, PIPELINE_DSM_KEY_DSA, true);
dsa_area *area = nullptr;
if (dsa_space != nullptr) {
    area = dsa_attach_in_place(dsa_space, seg);
    dsa_pin_mapping(area);
}
exec_ctx.dsa = area;
```

And mirror in leader (currently leader also has `dsa = nullptr` — same bug).

### 8.3 Acceptance

### 8.4 Status (2026-04-24)

**P3-0 §8.1 + §8.2: COMPLETE & VERIFIED.**

- §8.1 `worker_error`: applied in `pipeline_worker_main.cpp` (`pg_volvec_pipeline_worker_main` PG_TRY/PG_CATCH wrapper writing `pg_atomic_write_u32(&control->worker_error, 1)` then `PG_RE_THROW`) + `pipeline_leader.cpp` (post-`WaitForParallelWorkersToFinish` check → `ereport(ERROR, ERRCODE_INTERNAL_ERROR, "pg_volvec pipeline worker reported failure")`). **Live-verified**: leader correctly raises ERROR when worker fails.
- §8.2 `exec_ctx.dsa`: applied via `dsa_attach_in_place(dsa_space, seg) + dsa_pin_mapping(dsa)` in worker; leader captures `dsa_area*` from `dsa_create_in_place` + pins. Both `exec_ctx.dsa = dsa`. **Live-verified**: workers reach "partial agg supported" without DSA NULL-deref.
- **Bonus fix — JIT proc-exit UAF SEGV** (uncovered during §8.2 verification): `before_shmem_exit(pipeline_worker_proc_exit_cleanup, PointerGetDatum(&state))` was registered with a stack-local `&state` pointer that became dangling after `RunPipelineWorkerBody` returned. Fix: registration moved into `InitializePipelineWorkerState` (paired with successful state lifecycle); `CleanupPipelineWorkerState` cancels via `cancel_before_shmem_exit` guarded by new `proc_exit_callback_registered` flag on `PipelineWorkerState`; redundant caller-side `RegisterPipelineProcExitJitCleanup` removed from `pipeline_worker_main.cpp`. Files: `pipeline_worker_state.{hpp,cpp}`, `pipeline_worker_main.cpp`. **Live-verified**: parallel Q6 with 2 workers no longer SEGVs at proc_exit.
- Build: clean (1 pre-existing `-Wmissing-prototypes`). Regress trio: 1 pre-existing harness-format failure (verified via stash round-trip — not a P3-0 regression).

### 8.5 Newly-Surfaced Pre-Existing Bugs (P3a Precursors)

These were **uncovered** by the cleaner error propagation that §8.1 enabled. Both are pre-existing latent bugs in subsystems that P3a/P3b will rework. Document here; do NOT fix in P3-0.

#### 8.5.1 `inline partial merge failed` in leader combine

**Symptom:** After workers report `parallel partial agg supported`, leader's `AggSink::Combine` (or its inline-merge fast path) returns failure → `ereport(ERROR, "pg_volvec pipeline: inline partial merge failed")`.

**Likely location:** `agg_sink.cpp` combine path or `partial_agg_op.cpp` slot-write path. May be a slot-indexing mismatch (`partial_slot_count` vs. `ParallelWorkerNumber`), uninitialized slot header, or an int128/Wide128 value not actually written by the worker before the leader reads it.

**Disposition:** P3a precursor. The whole sink/combine layer is rewritten in P3b (`PartialAgg` / `FinalAgg` ops, DuckDB-style distributed FinalAgg). Fixing it on the legacy code is wasted work — verify under the new P3b ops instead.

#### 8.5.2 Leader `LLVMJitContext in use count not 0 at exit (is 1)` PANIC

**Symptom:** When leader `ereport(ERROR)`s out of `PgvolvecPipelineRun` (e.g. due to §8.5.1), longjmp unwinds past whatever was supposed to release a leader-side `LLVMJitContext`. PG's own JIT infrastructure detects the leak at `proc_exit` → `PANIC` → cluster restart.

**Likely location:** Leader does not own a `PipelineWorkerState`, so the proc-exit JIT cleanup we added in §8.4 only protects workers. Leader's `VecPlanState` / sink JIT context is released by C++ destructors during normal flow but skipped on PG `ereport`-driven longjmp.

**Disposition:** P3a precursor. P3a's task abstraction introduces a coordinator object owning the leader's per-query state — that object's `before_shmem_exit` registration (mirroring §8.4's worker pattern) is the right place to release leader JIT contexts. Trying to retrofit this onto the current leader is out of scope.

- [ ] Inject `elog(ERROR, "test")` in worker → leader sees and reports it.
- [ ] Regress trio passes.
- [ ] ≤ 60 LOC changed across ≤ 3 files (relaxed from 30/2 to allow header includes for `dsa_attach_in_place`).

### 8.3.1 QA scenarios (executable)

**QA-P3-0-1: Build + install + restart** — same as QA-P3a-1/2.

**QA-P3-0-2: Worker error propagation**

Test requires a one-line debug injection: behind `#ifdef PG_VOLVEC_TEST_WORKER_ERROR` add `elog(ERROR, "p3-0 worker error injection test")` at top of `RunPipelineWorkerBody`. Compile with `-DPG_VOLVEC_TEST_WORKER_ERROR`.
```bash
CCACHE_DISABLE=1 PATH=/opt/homebrew/bin:$PATH CFLAGS="-DPG_VOLVEC_TEST_WORKER_ERROR" CXXFLAGS="-DPG_VOLVEC_TEST_WORKER_ERROR" meson compile -C build pg_volvec
meson install -C build --only-changed
./installed/bin/pg_ctl -D ~/data/pg_tpch restart -m fast
./installed/bin/psql -h /tmp -p 5432 -d tpch -f contrib/pg_volvec/sql/q1.sql 2>&1 | tee /tmp/p3-0_err.log
grep -E "(worker error|p3-0 worker error injection)" /tmp/p3-0_err.log
```
- Expected: leader-side ERROR message references either `worker_error` or echoes the injected text; query does not silently return wrong result.

**QA-P3-0-3: DSA attach in worker**

Add temporary instrumentation to `RunPipelineWorkerBody` (revert before merge):
```cpp
elog(NOTICE, "pg_volvec[p3-0] worker dsa=%p", (void*) exec_ctx.dsa);
```
Then:
```bash
./installed/bin/psql -h /tmp -p 5432 -d tpch -f contrib/pg_volvec/sql/q1.sql 2>&1 | grep "p3-0\] worker dsa"
```
- Expected: every worker line shows non-NULL dsa pointer (not `0x0`).

**QA-P3-0-4: Regress trio** — same as QA-P3a-3.

---

## 9. Roadmap Summary

| Phase | Scope | Risk | LOC est. | Files | Gate |
|---|---|---|---|---|---|
| **P3-0** | Fix `worker_error` + `dsa=nullptr` bugs | Low | ~30–60 | 2 | None |
| **P3a** | Task abstraction | Low | ~150 | ≤6 | None |
| **P3b** | Event + PipelineCoordinator (single pipeline) | Medium | ~600 | 8 | Momus |
| **P3b'** | PhysicalOperator IR + MetaPipeline + Translator/RuntimeBinder split + dense IDs (R2 per Oracle B1–B4) | Medium | ~700 | 12 new | Momus + Oracle (R2) |
| **P3c** | Multi-pipeline lowering activation (uses MetaPipeline from P3b') | Medium | ~200 | ~3 | **DEFERRED by default** |
| **P3d** | PER-QUERY worker pool, forever-loop workers, DSM-atomic completion, single-pcxt invariant (R2 per Oracle §14.7) | High | ~750 | ~10 (2 new + 8 modify) | Momus + **Oracle R2 (mandatory)** |
| **P3e** | Async/blocked task support (`INTERRUPTED` return path) | Medium-high | ~500 | ~6 | Real workload demand |

**Recommended sequencing:**

```
Week 1:    P3-0 → P3a              (low risk, builds confidence in tooling)
Week 2-3:  P3b                     (Event layer, design review first)
Week 3-4:  P3b'                    (PhysicalOperator IR + MetaPipeline + Translator) — REQUIRED before P3d
Week 4:    DECISION POINT on P3c   (defer recommended; P3b' already provides MetaPipeline scaffolding)
Week 5-7:  P3d                     (per-query pool, biggest payoff for "线程池" goal; needs dense IDs from P3b')
Future:    P3e                     (when needed)
```

---

## 10. Out of Scope

- Restoring HashJoin, Sort, Limit, Project, or any pre-greenfield TPC-H query.
- Replacing PostgreSQL's planner.
- Cross-database worker pool sharing (each pool stays per-postmaster).
- Replacing `MemoryContext` discipline with anything else.
- Reintroducing any `runtime_*.{cpp,hpp,inc}`, `ParallelPipelineRole/Desc/Driver/Sink`, or `TaskKind` enum from the legacy runtime.

---

## 11. Open Questions — RESOLVED (v5)

All questions are now resolved with explicit user direction. Recorded for audit; no further override expected before implementation.

| # | Question | Resolution (binding) | Decided by |
|---|---|---|---|
| 1 | P3c scope | **DEFERRED.** P3b' already introduces MetaPipeline scaffolding; P3c becomes a thin "activate multi-pipeline lowering" phase, revisited only when a multi-pipeline query (e.g. HashJoin reintroduction) is in scope. | User + plan author |
| 2 | P3d pool model | **PER-QUERY pool.** Workers spawn at query start, each runs N pipelines sequentially via `WorkerPipelineRegistry`, exits at query end. Postmaster-wide pool (old Option A) **rejected** — per-query pool is required so per-process state (`pipeline_id → WorkerPipelineState`) is naturally scoped. | User (verbatim: "worker pool per query, because I can save some status of the query in every process") |
| 3 | Pool size | **`workers == 0` → ERROR.** **`workers ≥ 1` → pool size = launched worker count.** Leader is **pure coordinator** (launch, dispatch, drain results, send to client) — performs **no data work**. | User (verbatim: "if launch 0 workers report error … leader do nothing else, just for coordinate and send result back to client") |
| 4 | Pool exhaustion / leader fallback | **N/A — eliminated.** No leader-only fallback. No queueing. `workers==0` is a hard ERROR. | Follows from #3 |
| 5 | Legacy `LaunchParallelWorkers` retention GUC | **GUC `pg_volvec.use_worker_pool` DROPPED.** P3d replaces the per-pipeline `LaunchParallelWorkers` model directly; no toggle. | User (Q4) |
| 6 | Translator architecture | **Pluggable `Translator` interface** with `PgPlanTranslator` as the only concrete impl in P3b'. `DuckPlanTranslator` reserved for future without rewriting `MetaPipeline`. | User (verbatim: "just need to modify the translator, or just add a translator. pg_volvec can accept pg or duck's plans") |
| 7 | Pipeline ID assignment | **Dense `PipelineId` 0..N-1**, assigned exclusively by `MetaPipeline::Builder::BuildPipeline`. Same translator on leader+worker yields identical IDs (deterministic walk over `PhysicalOperator` tree). | Plan author (required by P3d `WorkerPipelineRegistry` indexing) |

### GUCs — Final State

**Kept:**
- `pg_volvec.parallel_max_workers` — clamp passed to `LaunchParallelWorkers`.
- `pg_volvec.parallel_morsel_nblocks` — morsel granularity.

**Dropped (do NOT add to `_PG_init`):**
- ~~`pg_volvec.pool_size`~~ — pool size now derived from launched worker count.
- ~~`pg_volvec.use_worker_pool`~~ — no toggle; per-query pool is the only model.
- ~~`pg_volvec.parallel_leader_participation`~~ — leader is always coordinator-only.

**Planned debug-only GUCs (P3b'/P3d testing):**
- `pg_volvec.test_emit_two_pipelines` (default `off`) — forces translator to emit 2 pipelines for QA-P3b'-multi.
- `pg_volvec.test_block_all_workers` (default `off`) — forces P3e `INTERRUPTED` path for QA.

## 12. Review Status

| Reviewer | Status | Round | Date | Notes |
|---|---|---|---|---|
| Momus | **Round 4 APPROVED** (stale after v5 rewrite) | 4 | 2026-04-23 | Superseded by R5. |
| Momus | **Round 5 APPROVED** | 5 | 2026-04-23 | Verdict OKAY. Verified: `pipeline_lowering.cpp` hardcodes `pipeline.id = 0`; `pipeline_leader.cpp` initializes `worker_error`; both leader/worker leave `exec_ctx.dsa = nullptr` (P3-0 target). Each phase has concrete file touch points, acceptance criteria, and executable QA. |
| Oracle (P3b' design) | **APPROVE WITH CHANGES** | 1 | 2026-04-23 | 4 blocking issues — see §14. Implementation OK after blockers resolved + re-review. |
| Oracle (P3b' design) | **PENDING R2** | 2 | 2026-04-23 | §6 R2 rewrite addresses B1 (PhysicalOp IR isolation via POD config; deletion of `pipeline_lowering` + `Vec*State`), B2 (MetaPipeline boundary detection + `depends_on` graph), B3 (Determinism Contract: no `unordered_*`, declaration-order child visitation, post-order dense IDs), B4 (Translator/RuntimeBinder split). Awaiting Oracle R2 verdict. |
| Oracle (P3d design) | **REJECT** | 1 | 2026-04-23 | 7 blocking issues — see §14. Per-pipeline snapshot/parallel-mode toggling, ParallelWorkerMain double-application, deadlock-prone completion channel, RAII-vs-longjmp leaks. |
| Oracle (P3d design) | **PENDING R2** | 2 | 2026-04-23 | §7 R2 rewrite per §14.7 binding direction: deleted `WorkerJoinQuery()` + per-pipeline `Push/Pop`/`Enter/Exit` (B1+B2+B3); replaced completion `shm_mq` with DSM atomic counters + `ConditionVariable` (B4); null-safe + idempotent `before_shmem_exit` callback against process-stable `g_worker_ctx` (B5); MemoryContext + POD-only locals inside `PG_TRY` (B6); fan-out `TerminateBackgroundWorker` + `WaitForParallelWorkersToFinish` + let PG's propagated `ErrorResponse` raise (B7); single-`ParallelContext` invariant (§14.4). 12 QA scenarios including grep gates for forbidden APIs and RAII rule. Awaiting Oracle R2 verdict. |
| Momus | **PENDING R6** | 6 | 2026-04-23 | §6 R2 (P3b') + §7 R2 (P3d) require fresh Momus pass after Oracle R2 closure. |
| User | **APPROVED v5 direction** | — | 2026-04-23 | Verbatim answers to Q1–Q4 captured in §11. Sign-off on edited document still pending. |

---

## 13. Review Checklist (Momus)

When reviewing this doc, verify:

- [ ] Each phase has clear, verifiable acceptance criteria.
- [ ] Cross-process adaptation table (§2) is complete.
- [ ] No phase silently depends on a deferred phase.
- [ ] Regress trio guard is enforced at every phase boundary.
- [ ] Risk levels match estimated LOC and file counts.
- [ ] Pre-existing bugs (§8) are not conflated with new work.
- [ ] Open questions (§11) are answerable; none are "we'll figure it out later".

---

## 14. Oracle Review Findings (2026-04-23, Round 1)

Both phases reviewed by Oracle. **§6 P3b' = APPROVE WITH CHANGES; §7 P3d = REJECT.** Implementation gated as follows: P3b' may proceed after blockers below are resolved AND a follow-up Oracle re-review approves. P3d is fully blocked pending §7 redesign + Oracle re-review.

### 14.1 §6 P3b' — Blocking Issues (4)

**B1. IR/header isolation contradiction.** Plan §6.5.1 says `PhysicalSeqScan` "wraps `VecSeqScanState*`" but QA-P3b'-5 requires zero `Vec*` references in `physical_*.hpp`. Pick one of: (a) `void *opaque` + `static_cast` in `.cpp`, (b) PIMPL with `.cpp`-only impl, (c) drop the QA-5 ban and accept Vec types in headers. Decide before implementation; otherwise either the IR is PG-bound (kills "pluggable translator") or the gate is unsatisfiable.

**B2. MetaPipeline builder underspecified for HashJoin extensibility.** Current `Builder::BuildPipeline(sink_op)` describes only a single linear SOURCE→…→SINK chain. DuckDB's MetaPipeline does multi-pipeline slicing + dependency wiring. Lock down NOW: (a) how pipeline boundaries are represented in the IR (blocking-operator marker on `PhysicalOperator`?); (b) how `depends_on` between pipelines is computed deterministically. Otherwise P3c becomes a rewrite, not an activation.

**B3. Determinism contract missing.** "Same translator + same input → same dense IDs" requires explicit prohibition of: `unordered_*` iteration anywhere in translator/builder; hash-map keyed by node address where iteration order influences emission; address-based ordering. Add as a hard "Determinism Contract" in `pipeline/AGENTS.md` and call it out in §6.5.2. Without this, mismatched IDs silently corrupt P3d's `WorkerPipelineRegistry[id]` lookups.

**B4. Translator API leaks PG-runtime specifics.** `PgPlanTranslator` constructor currently takes `PipelineSharedControl*`, `pg_atomic_uint64 *next_block`, `ParallelAggPartialState *shared_slots`, `SharedFileSet *spill_fileset` — these are runtime wiring, not plan translation. Split into `Translator` (pure plan→`PhysicalOperator` tree) + `RuntimeBinder` (injects shared-control/slots/fileset when constructing `Source/Sink` from physical nodes). Otherwise `DuckPlanTranslator` either fabricates PG artifacts or becomes a special case.

### 14.2 §6 P3b' — Verified Correct (do not revisit)
- Dense IDs assigned exclusively in MetaPipeline.
- `LowerToPipeline()` signature preserved (containment).
- `nullptr` return for unsupported shapes (fallback).
- LOC estimate (~580 / 11 files) plausible **if** builder kept minimal.

### 14.3 §7 P3d — Blocking Issues (7)

**B1. ParallelWorkerMain double-application.** `LaunchParallelWorkers` enters PG's `ParallelWorkerMain`, which already does `RestoreLibraryState`/`RestoreGUCState`/`RestoreClientConnectionInfo`/`StartParallelWorkerTransaction`/snapshot restore + `PushActiveSnapshot(asnapshot)`/`EnterParallelMode()` ONCE before our entrypoint. §7.4.1 `WorkerJoinQuery()` re-does many of these — wrong (some are assertion-protected against repetition). **Fix:** delete `WorkerJoinQuery()` entirely; do only pg_volvec-local once init in our entrypoint (deserialize `PlannedStmt`, build `VecPlanState`, lower pipelines, init `WorkerPipelineRegistry`).

**B2. Per-pipeline `PushActiveSnapshot(GetTransactionSnapshot())` is a semantic bug.** Worker already has leader-restored active snapshot from `ParallelWorkerMain`. Pushing fresh `GetTransactionSnapshot()` per pipeline can yield different snapshots (esp. READ COMMITTED), diverging from "one query = one snapshot" and from leader's view. **Fix:** keep snapshot + parallel mode active for the entire query in the worker loop (no per-pipeline Push/Pop, no Enter/Exit).

**B3. `PG_CATCH` does not unwind per-pipeline pushes.** Proposed catch sets `worker_error` and rethrows but doesn't `PopActiveSnapshot()` / `ExitParallelMode()` matching the per-pipeline pushes. **Fix:** removed by B2 fix (no per-pipeline push/enter to unwind).

**B4. Completion-channel deadlock likely.** Worker `PG_RE_THROW()` on ERROR doesn't send completion → coordinator's "wait for all" hangs. Non-ERROR worker crash also leaves `control->worker_error` unset. **Fix:** coordinator wait must integrate (a) `CHECK_FOR_INTERRUPTS()` (PG error queue propagation), (b) `WaitForParallelWorkersToFinish(pcxt_)` / worker-handle status, (c) bound the wait via PG primitives. **Consider dropping completion `shm_mq` entirely** in favor of per-worker DSM atomic `pipeline_done[worker]` + PG `ConditionVariable` + `WaitForParallelWorkersToFinish` for crash detection.

**B5. `before_shmem_exit` callback not actually safe via `static g_worker_ctx`.** Stable pointer ≠ stable contents. If `WorkerLeaveQuery()` frees the registry/state before `proc_exit`, callback dereferences freed memory. **Fix:** either never manually free (let `proc_exit` clean up), or null out `g_worker_ctx.state`/`registry` BEFORE freeing AND make callback fully null-safe + idempotent.

**B6. C++ destructor/RAII safety across `ereport(ERROR)` not addressed.** PG ERROR uses `longjmp` → C++ stack destructors skipped. In a forever-loop worker (vs current "process exits anyway"), this becomes accumulating leaks across pipelines. Current `WorkerPipelineExecutor::Execute` has stack `std::vector<std::unique_ptr<OperatorState>>` — ERROR inside operator skips destructors. **Fix:** allocate execution state in `MemoryContext` + explicit cleanup, OR keep `PG_TRY` outside any C++ RAII, OR use PG resource owners.

**B7. Coordinator failure handling loses original error + doesn't fan-out abort.** Returning `false` with generic `"worker error during pipeline"` discards PG's propagated `ErrorResponse`. **Fix:** on first failure, coordinator must (a) stop waiting for completions, (b) terminate workers / destroy parallel context, (c) surface the real ERROR to client (let PG's propagated ErrorResponse raise via `CHECK_FOR_INTERRUPTS()`, OR explicitly `ereport` with captured details from error queue).

### 14.4 §7 P3d — Non-Blocking Concerns
- `workers==0 → ERROR` policy: implementable, but `InitializeParallelDSM` can reduce `nworkers` to 0 when interrupts can't be processed (`parallel.c:245-246`). Error path must not re-enter a wait that requires interrupts.
- DSA lifetime across pipelines: fine **if** single `ParallelContext` per query, no DSM reinit between pipelines. State this explicitly in §7.

### 14.5 §7 P3d — Verified Correct (do not revisit)
- Per-query (not postmaster-wide) pool matches user's stated state-persistence requirement.
- Vector-indexed `WorkerPipelineRegistry` keyed by dense `PipelineId` is correct shape **if** ID determinism enforced (gated on §6 B3).
- Identification of current `before_shmem_exit` stack-pointer hazard (`bg_547616b5`) is a real use-after-free in forever-loop model.

### 14.6 §7 P3d — LOC Reality Check
- Original ~900 LOC estimate **not realistic** given correctness work needed.
- Path to plausible estimate: simplify per Oracle suggestions (no completion `shm_mq`, no per-pipeline snapshot toggling, no `WorkerJoinQuery()` reimplementing `ParallelWorkerMain`).

### 14.7 Oracle's Recommended Simplifications (binding direction for §7 redesign)
1. Implement per-query pool **inside the existing worker entrypoint**; do NOT replicate `ParallelWorkerMain`. Worker entrypoint does only pg_volvec-local one-time init then loops on assignments.
2. Drop completion `shm_mq`. Use DSM atomic `pipeline_done[worker]` + PG `ConditionVariable` + `WaitForParallelWorkersToFinish` for crash detection.
3. Keep snapshot + parallel mode active for the entire query in the worker loop. No per-pipeline Push/Pop, no Enter/Exit.

### 14.8 Required Plan Edits (next cycle)
- §6.5.1: resolve B1 (header isolation strategy).
- §6.5.2: resolve B2 (MetaPipeline boundary representation, deterministic `depends_on`).
- §6.5.2 + `pipeline/AGENTS.md`: add Determinism Contract (B3).
- §6.5.3: split `Translator` vs `RuntimeBinder` (B4).
- §7.4.1: rewrite worker-loop section per §14.7 (delete `WorkerJoinQuery()`, no per-pipeline snapshot/parallel-mode toggling).
- §7.4.3: callback null-safety + ordering (B5).
- §7.4.x: RAII/ERROR strategy (B6).
- §7.4.4: coordinator wait integration with `CHECK_FOR_INTERRUPTS` + `WaitForParallelWorkersToFinish`; fan-out abort + original-error preservation (B7).
- §7.4.5: replace per-worker completion `shm_mq` with DSM atomic + ConditionVariable.
- §9: revise P3d LOC estimate upward OR keep ~900 only after simplifications applied.

---

## 15. P3X-Q1 — Q1-Only Minimal Pipeline Framework (2026-04-24, AUTHORITATIVE)

> **SUPERSEDES** §3 (P3a), §4 (P3b), §6 (P3b'), §7 (P3d), AND the prior P3X 6-milestone plan from earlier this session. Per user lock (this turn): "暂时我就是想用pipeline这套并行框架跑通Q1，并且把Q1的性能跑到极致。先把pipeline之间的数据传递给做了就是buffermanager那块东西，然后是Q1需要的几个算子给做了，其他都延后。" Goal narrows to Q1 end-to-end + extreme Q1 perf. Q6 verification, generic translator, spill, Problem-A swizzle, dedicated BufferManager test SQLs, and Momus B1 unsupported-fallback test are ALL deferred.
>
> P3-0 (`AggSink::SetSharedSlots` deletion + DSA destruction order, ex-M2/M4) remains shipped and live-verified (serial Q6 = `1230113636.0101`, parallel Q6 workers=2 no SEGV). It stays in.
>
> **Authoritative companion:** `.sisyphus/plans/global-local-state-design.md` §8 (control plane) + §8.7 (BufferManager data plane). This §15 is the milestone overlay; design doc is the spec.

### 15.0 Scope Lock (this turn, verbatim)

User: "暂时我就是想用pipeline这套并行框架跑通Q1，并且把Q1的性能跑到极致。先把pipeline之间的数据传递给做了就是buffermanager那块东西，然后是Q1需要的几个算子给做了，其他都延后。"

Decoded:
1. **Target query**: TPC-H Q1 ONLY. No Q6 verification milestone, no generic translator.
2. **Performance bar**: extreme — explicitly record baseline → final speedup ratio.
3. **Order of work**: BufferManager (data passing) FIRST among the data-plane work, then Q1 operators.
4. **Defer everything else**: Q2–Q22, HashJoin, parallel sort, spill, async/BLOCKED, Problem-A swizzle, dedicated BufferManager test SQLs, fallback-test gating.

Locked design choices (this turn):
5. **BufferManager block strategy**: Fixed 256KB blocks (DuckDB default).
6. **HashAggregate model**: Cross-worker radix partitioning (DuckDB `RadixPartitionedHashTable` mode). Per worker: local hash table partitioned by hash high-bits. Combine: each worker takes 1+ partitions and finalizes; downstream Source drains finalized partitions.

Q1 plan shape:
```
SeqScan(lineitem) + Filter(l_shipdate <= date) → HashAggregate → Sort(returnflag, linestatus) → Output
```
Three pipelines:
- P0: `PhysicalSeqScan(qual=l_shipdate<=…)` → `PhysicalHashAggregate`(Sink, radix-partitioned)
- P1: `PhysicalHashAggregate`(Source, drains finalized partitions) → `PhysicalOrder`(Sink, single-thread)
- P2: `PhysicalOrder`(Source, drains sorted run) → `OutputSink`
- `depends_on = [[], [0], [1]]`

### 15.1 Milestone Sequence (4 milestones, locked order)

| # | Milestone | Lands | Dep | Acceptance Section |
|---|---|---|---|---|
| 1 | **M-IR-MIN** — `PhysicalOperator` + 3 subclasses (NO PhysicalFilter; qual fused into PhysicalSeqScan) + `OutputSink` + hardcoded `Q1Translator::TryTranslate` | design §8.1 + §8.2 | P3-0 done | §15.3.1 |
| 2 | **M-FRAME-MIN** — `MetaPipeline::BuildForQ1` (hardcoded 3-pipeline topology, NOT generic), simplified 3-event lifecycle (Run / Combine / Finalize), DSM lock-free task queue, query-scoped bgworker pool, `PipelineDsmLookup<T>`, dynamic DSM keys via `PipelineKey(pid, kind)` | design §8.3 (simplified) + §8.4 + §8.5 | M-IR-MIN | §15.3.2 |
| 3 | **M-BM-MIN** — `PipelineBufferManager` (Allocate / Pin / Unpin / Reallocate ONLY; NO spill, NO Problem-A swizzle, NO eviction), Fixed 256KB blocks, Problem-B swizzle (DSA-pointer ⇄ local-VA), `BufferHandle` RAII, `BlockHandle` in-DSM with refcount, `DsaDataChunkBridge` rewritten on top | design §8.7 (subset) | M-FRAME-MIN | §15.3.3 |
| 4 | **M-Q1-PERF** — Performance tuning: morsel size, worker count GUC, partial-agg radix bit count, JIT deform path coverage, flame-graph driven hotspot elimination | (no new design) | M-BM-MIN | §15.3.4 |

**Explicitly deferred:**
- Q6 verification milestone (Q6 already passes; not used as framework verification)
- 5-event lifecycle (Initialize/PrepareFinish events folded into leader setup + Finish)
- Generic `PgTranslator` (only Q1 shape recognized; everything else returns nullptr → PG fallback)
- Spill via SharedFileSet (`Evict` is `ereport(ERROR)` stub)
- Problem-A swizzle (heap-pointer fixup) — Q1 has no varchar/heap payloads in intermediate state
- BufferManager memory accounting + LRU eviction (`pg_volvec.query_memory_limit` defaults to 0 = unlimited)
- Standalone BufferManager tests (`test_buffer_manager_*.sql`)
- Momus B1 `test_unsupported.sql`
- Parallel sort (`PhysicalOrder::MaxThreads()` hardcoded to 1)

### 15.2 Files Created / Refactored / Deleted (Q1-minimal delta)

**NEW (15 files):**
- IR: `physical_operator.{hpp,cpp}`, `physical_seq_scan.{hpp,cpp}` (with embedded filter), `physical_hash_aggregate.{hpp,cpp}` (radix-partitioned, dual-role), `physical_order.{hpp,cpp}` (MaxThreads=1, dual-role), `output_sink.{hpp,cpp}`, `state_base.hpp`
- Translator: `q1_translator.{hpp,cpp}` (NOT generic; `TryTranslate` matches exactly Q1 shape)
- MetaPipeline: `meta_pipeline.{hpp,cpp}` (single public entry `BuildForQ1`)
- Events (3 only): `event.{hpp,cpp}`, `pipeline_run_event.{hpp,cpp}`, `pipeline_combine_event.{hpp,cpp}`, `pipeline_finalize_event.{hpp,cpp}`
- Scheduler: `task_scheduler.{hpp,cpp}`, `task.{hpp,cpp}`, `dsm_task_queue.{hpp,cpp}`
- BufferManager: `buffer_manager.{hpp,cpp}`, `buffer_handle.{hpp,cpp}`, `block_handle.{hpp,cpp}`
- Sort: `sort_sink.{hpp,cpp}` (backs `PhysicalOrder`)
- Radix-agg: `radix_partitioned_hash_table.{hpp,cpp}` (backs `PhysicalHashAggregate`)

**REWRITE (7 files, in place):**
- `src/engine/exec/query_state.hpp` — `PgVolVecQueryState` swaps `VecPlanState* vec_plan` → opaque `void *physical_root` handle (or per-query translator object); preserves C-visible struct layout for bridge.
- `src/bridge/execute.cpp` — strip lines 34-69 (ExecInitVecPlan call) + 151-170 (legacy pipeline dispatch) + 173-236 (VecPlanState materialization loop). New flow: hooks early-no-op for non-Q1 admission; dispatch invokes `Q1Translator::TryTranslate` → on hit, run new `MetaPipeline + TaskScheduler`; on miss, `WARNING("pg_volvec: unsupported plan shape, falling back to PG executor")` + `standard_ExecutorRun`.
- `src/bridge/execute.h` — update declarations (no more `VecPlanState*` in signatures).
- `src/bridge/state.{c,h}` — `PgVolVecQueryState` alloc/free no longer touches `VecPlanState`.
- `src/bridge/pg_volvec.c` — hooks gate admission early: skip parallel-sentinel install for plans Q1Translator can't accept (non-Q1 plans never enter pg_volvec).
- `pipeline/pipeline_leader.{hpp,cpp}` — full rewrite to `Q1Translator + MetaPipeline + TaskScheduler::Run` (3-event lifecycle + topological order + DSM task queue).
- `pipeline/pipeline_worker_main.cpp` — full rewrite as `ExecuteTask` pump pulling from DSM task queue; preserves `worker_error` protocol.

**REFACTOR (1 site, M-BM-MIN):**
- `core/parallel_dsa_bridge.cpp` (`DsaDataChunkBridge`) — replace `dsa_allocate`/`dsa_get_address` with `BufferManager::Allocate`/`Pin`

**DELETE — 33 files via `git rm`, atomic commit (M-FRAME-MIN start):**

`src/engine/exec/` (18 files, ~3500 LOC) — entire legacy `Vec*State` AST + builder + helpers:
- Headers: `plan_state.hpp`, `agg.hpp`, `seq_scan.hpp`, `filter.hpp`, `project.hpp`, `limit.hpp`, `sort.hpp`, `lookup.hpp`, `internal.hpp`
- Impls: `agg.cpp`, `seq_scan.cpp`, `filter.cpp`, `project.cpp`, `limit.cpp`, `sort.cpp`, `agg_plan.cpp`, `executor_init.cpp`, `executor_common.cpp`

`src/engine/parallel/pipeline/` (15 files) — legacy LoweredPipeline runtime + worker primitives slated for clean rewrite:
- Legacy admission/lowering: `pipeline_lowering.{hpp,cpp}`, `pipeline_worker_context.hpp`, `pipeline_worker_state.{hpp,cpp}`, `filter_op.{hpp,cpp}`, `partial_agg_op.{hpp,cpp}`
- Runtime primitives rewritten clean against new `PhysicalOperator` IR (per user direction): `executor.{hpp,cpp}`, `agg_sink.{hpp,cpp}`, `seq_scan_source.{hpp,cpp}` — these will be re-introduced in M-FRAME-MIN with no `Vec*State` / no `LoweredPipeline` baggage. (Original plan reused them; revised plan deletes-and-rewrites for clean architecture.)

**KEEP (4 files, exempt from QA-P3X-4 per documented exception):**
- `pipeline/physical_seq_scan.{hpp,cpp}` and `pipeline/physical_hash_aggregate.{hpp,cpp}` — may hold opaque `Vec*State*` bridge handles into legacy JIT (`llvmjit_*`) machinery. Untouched in M-FRAME-MIN.

**meson.build edits** — strip lines 10-18 (pipeline legacy `.cpp` entries) and lines 25-33 (exec `.cpp` entries) in same atomic commit; new sources added incrementally as M-FRAME-MIN files land.

**SHRINK:**
- `dsm_control.hpp` — `PipelineSharedControl` 9 fields → 3 (`magic`, `num_pipelines`, `worker_error`)

**Removed from prior P3X plan:** `physical_filter.{hpp,cpp}` (fused into SeqScan), `pg_translator.{hpp,cpp}` (generic) → replaced by `q1_translator.{hpp,cpp}`, 2 of the 5 event files (Initialize / PrepareFinish folded), `pipeline_row_collection.{hpp,cpp}` (deferred — Q1 doesn't need it; partial agg slots stored directly in BufferManager blocks).

### 15.3 Acceptance Gates Per Milestone

#### 15.3.1 P3X-Q1 M-IR-MIN
- [ ] `pipeline/physical_operator.hpp` compiles with `PhysicalOperator`, 3 concrete subclasses, `BuildPipelines`, `MaxThreads`, full virtual surface per design §8.1.
- [ ] `PhysicalSeqScan` carries embedded `Expr *qual` and evaluates it inline during `GetData` (no separate operator pass).
- [ ] `PhysicalHashAggregate` is dual-role (`IsSink()=true && IsSource()=true`).
- [ ] `PhysicalOrder` is dual-role; `MaxThreads()` hardcoded to 1.
- [ ] `Q1Translator::TryTranslate(PlannedStmt*)` returns `unique_ptr<PhysicalOperator>` for Q1 shape, `nullptr` otherwise.
- [ ] **Deletion gate**: `git grep -nE 'class (Vec(Plan|SeqScan|Agg))State\b' contrib/pg_volvec/` returns zero hits.
- [ ] `sql/q1.sql` + `sql/q6.sql` still PASS via legacy path (M-IR-MIN compiles only; runtime not switched).

#### 15.3.2 P3X-Q1 M-FRAME-MIN

**Ordering decision (user-locked, 2026-04-24)**: delete-first. Atomic deletion commit removes 33 legacy files + meson.build entries before any new MetaPipeline/scheduler code lands. Tree intentionally fails to compile after deletion until stub bridge + stub leader/worker_main land in the same milestone. Q6 vectorized path dies during M-FRAME-MIN; Q6 routes to standard PG executor for the duration of this milestone (restored in a later M-Q6-RESTORE milestone if needed).

**Bridge admission policy (user-locked, 2026-04-24)**: PG executor hooks (`ExecutorStart_hook`, `ExecutorRun_hook`, `ExecutorEnd_hook`) install pg_volvec sentinels ONLY when `Q1Translator::TryTranslate` returns non-null. Non-Q1 plans bypass pg_volvec entirely (no admission). When admission succeeds but later execution-time translation fails (should not happen post-admission, but defense-in-depth): emit `WARNING("pg_volvec: unsupported plan shape, falling back to PG executor")` and call `standard_ExecutorRun`.

- [ ] **Atomic deletion commit landed**: `git rm` of all 33 files (18 in `src/engine/exec/`, 15 in `src/engine/parallel/pipeline/`) per §15.2 DELETE list; `meson.build` lines 10-18 + 25-33 stripped; commit message references this section.
- [ ] **Stub-then-build sequence**: bridge + leader + worker_main reduced to compilable stubs immediately after deletion (Q1Translator returns nullptr → all queries take WARNING+fallback path); tree compiles; Q1+Q6 produce correct results via standard PG executor (not vectorized). This is the "broken vectorization but correct results" intermediate state.
- [ ] `MetaPipeline::BuildForQ1(root)` produces `pipelines.size()==3`, `depends_on==[[],[0],[1]]`, dense `PipelineId` 0..2 in post-order.
- [ ] 3-event lifecycle wired: `PipelineRunEvent` (workers run Source→Op→Sink driver loop) → `PipelineCombineEvent` (workers Combine local→global iff Run ended success) → `PipelineFinalizeEvent` (leader Finalize, fires CompleteDependency on dependents).
- [ ] `TaskScheduler::Create(*bundle).Run()` launches N bgworkers via single `LaunchParallelWorkers`. `pg_volvec.parallel_max_workers == 0` → `ERROR`.
- [ ] Lock-free MPMC ring at `PIPELINE_DSM_KEY_TASK_QUEUE`; workers wake via `SetLatch`.
- [ ] **QA-P3X-3** (no `unordered_*`): `git grep -nE 'unordered_(map|set)' contrib/pg_volvec/src/engine/parallel/pipeline/` → zero.
- [ ] **QA-P3X-4** (no `Vec*State` anywhere except 2 exempt physical_* files): grep gate per §15.7 (now stricter: exec/ tree is gone, so the only legitimate `Vec*State*` references in the entire `contrib/pg_volvec/` subtree are inside `physical_seq_scan.{hpp,cpp}` and `physical_hash_aggregate.{hpp,cpp}` as opaque JIT-bridge handles).
- [ ] **QA-P3X-5** (`shm_toc_lookup` only inside `PipelineDsmLookup<T>`): grep gate.
- [ ] **QA-P3X-7** (no worker-side `dsa_detach`): grep gate on `pipeline_worker_main.cpp`.
- [ ] **Combine-only-on-success**: ERROR injection in `PipelineRunEvent` skips Combine for that worker.
- [ ] **ERROR-safety**: locals via `MemoryContextAlloc` + placement-new + memory-context callback dtor; zero stack-RAII Local* inside `PG_TRY`.
- [ ] **Q1 PASS via new runtime** (still using legacy `DsaDataChunkBridge` serialization for cross-pipeline data; BufferManager not yet in play); result bit-equal to native PG output; workers ≥ 2 actually used; admission via `Q1Translator::TryTranslate` returning a valid `PhysicalOperator` tree.
- [ ] **Q6 acceptance during M-FRAME-MIN**: Q6 hits non-admission path (Q1Translator returns nullptr for Q6 shape); standard PG executor produces revenue = `1230113636.0101` (10G) / `32.0000` (small). NOT vectorized. This is expected during M-FRAME-MIN and is restored in a future milestone if needed.
- [ ] No `inline partial merge failed`. No `LLVMJitContext in use count not 0`.

#### 15.3.3 P3X-Q1 M-BM-MIN (NEW per BufferManager)
- [ ] `PipelineBufferManager::{Create,Attach,Allocate,Pin,Unpin,Reallocate,GetUsedMemory,GetMaxMemory}` implemented. `Evict` and `Reload` throw `ereport(ERROR, "spill not implemented in P3X-Q1")` — placeholder.
- [ ] Fixed 256KB block size (`PIPELINE_BLOCK_SIZE = 256 * 1024`); `Allocate(size)` rounds up; `RegisterSmallMemory` and Problem-A swizzle structs declared but bodies are stubs.
- [ ] `BufferHandle` RAII; destructor calls `Unpin`; non-copyable, movable.
- [ ] `BlockHandle` lives in DSM under key `PipelineKey(-1, BUFFER_BLOCK)`; `pin_count` is `pg_atomic_uint32`.
- [ ] **Problem-B swizzle is real and exercised**: any pointer stored in a row payload uses `dsa_pointer` (or `(BlockId, offset)`); `Pin` resolves to local VA via `dsa_get_address` in the consuming process. Verified by a worker-process attach test in M-Q1 acceptance.
- [ ] `DsaDataChunkBridge` rewritten on top of BufferManager.
- [ ] **QA-P3X-8** (DSA encapsulation): `git grep -nE 'dsa_(allocate|get_address|free)\b' contrib/pg_volvec/src/engine/parallel/pipeline/` → hits ONLY in `buffer_manager.cpp` and per-query lifecycle calls in `pipeline_{leader,worker_main}.cpp`.
- [ ] **QA-P3X-9** (zero-copy on Source-side handoff): `git grep -nE 'memcpy' contrib/pg_volvec/src/engine/parallel/pipeline/physical_hash_aggregate.cpp contrib/pg_volvec/src/engine/parallel/pipeline/physical_order.cpp` → zero hits on Source `GetData` paths. Sink-side `Sink` may legitimately memcpy into BufferManager block.
- [ ] **Q1 PASS via new runtime AND BufferManager** (no `DsaDataChunkBridge` serialization on cross-pipeline path); result bit-equal.
- [ ] **Perf gate**: M-BM-MIN Q1 wall time ≤ 0.77× M-FRAME-MIN Q1 wall time (≥ 1.3× speedup). Recorded in `contrib/pg_volvec/perf/q1_p3x_progression.md`.

#### 15.3.4 P3X-Q1 M-Q1-PERF
- [ ] Tunable GUCs added: `pg_volvec.morsel_size_blocks` (default 32), `pg_volvec.parallel_max_workers` (existing), `pg_volvec.radix_bits` (default 4 → 16 partitions).
- [ ] JIT deform path covers all Q1 column accesses (verified via NOTICE log of JIT compilation success per column).
- [ ] Flame graph (`/usr/bin/sample` + `FlameGraph/`) collected; top-3 hotspots documented in `perf/q1_p3x_progression.md`.
- [ ] **Final perf gate**: Q1 wall time on TPC-H 10G with `pg_volvec.parallel_max_workers = 4` is the historical best for pg_volvec on this machine (replaces any prior recorded best in `perf/`). Document baseline (PG native, single-worker pg_volvec, M-FRAME-MIN, M-BM-MIN, M-Q1-PERF) and speedup ratios at each step.
- [ ] No regression on Q6 (still passes, revenue = `1230113636.0101`).

### 15.4 BufferManager Q1-Minimal API Surface

Restated narrowly for M-BM-MIN scope (full surface in design §8.7.1):

```cpp
class PipelineBufferManager {
public:
    static constexpr uint64 BLOCK_SIZE = 256 * 1024;   // Fixed (user lock)

    static std::unique_ptr<PipelineBufferManager>
    Create(dsa_area *backing_dsa, SharedFileSet *spill /*unused in P3X-Q1*/, uint64 max_memory /*0=unlimited*/);

    static std::unique_ptr<PipelineBufferManager>
    Attach(dsa_area *backing_dsa, SharedFileSet *spill, shm_toc *toc);

    BufferHandle Allocate(uint64 size, MemoryTag tag = MemoryTag::PIPELINE_INTERMEDIATE);
    BufferHandle RegisterSmallMemory(uint64 size, MemoryTag tag);   // Q1: stub returns Allocate(BLOCK_SIZE)
    BufferHandle Pin(const BlockHandle &block);
    void         Unpin(BlockHandle &block);
    BufferHandle Reallocate(BufferHandle handle, uint64 new_size);

    uint64 GetUsedMemory() const;
    uint64 GetMaxMemory()  const;

private:
    // Q1-minimal: no eviction queue, no spill, no Problem-A swizzle traversal.
    // Evict() and Reload() bodies are ereport(ERROR, "deferred") stubs.
};
```

### 15.5 Radix-Partitioned HashAggregate (Q1 + future-proof)

Backing `PhysicalHashAggregate` with a `RadixPartitionedHashTable` modeled on DuckDB `radix_partitioned_hashtable.{hpp,cpp}`:

- `radix_bits` GUC (default 4) → 16 partitions.
- Per worker: local `RadixPartitionedHashTable` with one sub-hash-table per partition. `Sink` step inserts each row into `partition[hash >> (64 - radix_bits)]`.
- `Combine` step: each worker's sub-tables are appended (not merged) to the global per-partition lists held on `HashAggregateGlobalSinkState`. Append-only because radix partitioning guarantees disjoint key sets across partitions.
- `Finalize` step: per-partition merges happen lazily during `GetGlobalSourceState` setup. Each partition becomes a Source-side morsel claimed by P1 workers.
- Q1 has 6 groups → most partitions empty; this is fine. Path is exercised end-to-end on Q1 and ready for Q3/Q5 group-count scaling.

Files: `pipeline/radix_partitioned_hash_table.{hpp,cpp}`. ~400 LOC.

### 15.6 Bug Resolution Coupling

1. **`pg_volvec pipeline: inline partial merge failed`** — FIXED in P3-0. Re-verify in M-FRAME-MIN acceptance.
2. **Leader PANIC `LLVMJitContext in use count not 0 at exit (is 1)`** — FIXED in P3-0 follow-up via `proc_exit_callback_registered` + cancel-on-cleanup. Re-verify in M-FRAME-MIN acceptance.
3. **Q6 KNOWN-BROKEN-by-design during M-FRAME-MIN** (NEW): Q6 plan shape (`SeqScan(quals: l_shipdate, l_discount, l_quantity) → Aggregate(SUM(l_extendedprice * l_discount))`) does NOT match `Q1Translator::TryTranslate`'s Q1-shape pattern. Per the user-locked admission policy (§15.3.2), `pg_volvec_initialize_plan` no-ops the hooks for Q6 → query routes to `standard_ExecutorRun` → returns correct result via PostgreSQL's native executor (`1230113636.0101` on TPC-H 10G, `32.0000` on small fixture), but **NOT vectorized**. Acceptance: result correctness + zero `WARNING` emitted (admission was clean, not a fallback). Restoration of Q6 vectorization is deferred to a future M-Q6-RESTORE milestone (out of P3X-Q1 scope).

### 15.7 QA Grep Gates (consolidated, Q1-minimal subset)

```bash
# QA-P3X-3 (M-FRAME-MIN)
git grep -nE 'unordered_(map|set)' contrib/pg_volvec/src/engine/parallel/pipeline/

# QA-P3X-4 (M-FRAME-MIN, post-deletion) — TIGHTENED 2026-04-24:
# After the atomic deletion commit, the entire src/engine/exec/ tree is GONE. The only
# legitimate Vec*State references in the entire contrib/pg_volvec/ subtree are opaque
# bridge handles inside physical_seq_scan.{hpp,cpp} and physical_hash_aggregate.{hpp,cpp}
# pointing into the surviving llvmjit_* machinery. Any other hit = leftover legacy = FAIL.
# Scope widened from src/engine/parallel/pipeline/ to contrib/pg_volvec/ to catch strays
# in bridge/, core/, ir/, translate/, etc.
git grep -nE '\bVec(Plan|SeqScan|Agg|Filter|Project|Limit|Sort)State\b' contrib/pg_volvec/ \
    | grep -vE '/(physical_seq_scan|physical_hash_aggregate)\.(hpp|cpp):' \
    | grep -vE '^contrib/pg_volvec/(docs|perf|sql|expected)/'
# Expected output: empty.

# QA-P3X-4b (NEW, M-FRAME-MIN, post-deletion) — exec/ tree must be physically gone
test ! -d contrib/pg_volvec/src/engine/exec || { echo "FAIL: src/engine/exec/ still exists"; exit 1; }

# QA-P3X-4c (NEW, M-FRAME-MIN) — admission policy: hooks must early-no-op for non-Q1.
# Inspect pg_volvec.c hook bodies for the Q1Translator::TryTranslate gate.
git grep -nE 'TryTranslate\(' contrib/pg_volvec/src/bridge/pg_volvec.c
# Expected: ≥1 hit inside ExecutorStart_hook (gating sentinel install).

# QA-P3X-4d (NEW, M-FRAME-MIN) — translator-miss WARNING policy
git grep -nE 'pg_volvec: unsupported plan shape, falling back to PG executor' contrib/pg_volvec/src/bridge/
# Expected: ≥1 hit (defense-in-depth WARNING site in execute.cpp).

# QA-P3X-5 (M-FRAME-MIN)
git grep -nE 'shm_toc_lookup\(' contrib/pg_volvec/src/engine/parallel/pipeline/

# QA-P3X-7 (M-FRAME-MIN)
git grep -n 'dsa_detach' contrib/pg_volvec/src/engine/parallel/pipeline/pipeline_worker_main.cpp

# QA-P3X-8 (M-BM-MIN)
git grep -nE 'dsa_(allocate|get_address|free)\b' contrib/pg_volvec/src/engine/parallel/pipeline/

# QA-P3X-9 (M-BM-MIN)
git grep -nE 'memcpy' contrib/pg_volvec/src/engine/parallel/pipeline/physical_hash_aggregate.cpp contrib/pg_volvec/src/engine/parallel/pipeline/physical_order.cpp
```

QA-P3X-6 (combine-only-on-success unit test) deferred — proven via M-FRAME-MIN ERROR injection inline.

### 15.8 Performance Tracking Artifact

New file `contrib/pg_volvec/perf/q1_p3x_progression.md` (created in M-FRAME-MIN, updated in M-BM-MIN and M-Q1-PERF):

| Stage | Q1 wall time (TPC-H 10G, workers=4) | Speedup vs PG native |
|---|---|---|
| PG native (no pg_volvec) | (baseline, recorded once) | 1.00× |
| pg_volvec serial (workers=0 fallback path) | TBD | TBD |
| M-FRAME-MIN (new runtime, legacy DSA bridge) | TBD | TBD |
| M-BM-MIN (BufferManager zero-copy handoff) | TBD | TBD |
| M-Q1-PERF (tuned) | TBD | TBD |

Hardware fingerprint, OS, PG build flags, GUC settings recorded at top of file.

### 15.9 Re-Review Trigger

Plan now narrows to Q1. Reviewer scope tightens accordingly:

- **Oracle R3** — focused on (a) Problem-B swizzle correctness across worker process boundary, (b) refcount integrity of `BufferHandle` under `ereport(ERROR)` + memory-context callback dtor, (c) radix-partition disjointness claim for Combine append-without-merge.
- **Momus R7** — focused on §15 acceptance gates verifiability + perf-gate measurability (the ≥1.3× speedup claim and the "historical best" final claim are objective and recordable). Drop generic-translator concerns since Q1Translator is intentionally hardcoded.

Dispatched in parallel against this plan + design doc post-mirror-sync.

### 15.10 Section Status Table

| Old Plan Section | Status | Q1-Minimal Replacement |
|---|---|---|
| §3 (P3a) | **SUPERSEDED by §15 (P3X-Q1)** | M-IR-MIN + M-FRAME-MIN |
| §4 (P3b) | **SUPERSEDED by §15 (P3X-Q1)** | M-FRAME-MIN |
| §6 (P3b') | **SUPERSEDED by §15 (P3X-Q1)** | M-IR-MIN + M-FRAME-MIN |
| §7 (P3d) | **SUPERSEDED by §15 (P3X-Q1)** | M-FRAME-MIN + M-BM-MIN |
| §8.5 (bug catalog) | RETAINED — both bugs FIXED in P3-0 | (no change) |
| §14 (Oracle R1) | RETAINED — folded into design §8 + §15 | (no change) |

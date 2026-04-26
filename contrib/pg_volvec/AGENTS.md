# pg_volvec KNOWLEDGE BASE

**Refreshed:** 2026-04-26
**HEAD:** `71cd856975d` ("pg_volvec: 3g.2-prep MetaPipeline runtime infra (DSM + descriptor IR scaffolding)")
**Phase:** `M-FRAME-MIN` — step 3g.2-prep landed; step 3g.2-final (runtime end-to-end wiring for Q1) in progress.

## OVERVIEW

PostgreSQL extension that hooks `ExecutorStart/Run/Finish/End` and offloads supported OLAP plan subtrees into a C++ columnar `DataChunk` executor with LLVM JIT on tuple deform and expression evaluation. Today the active scope is **Q1 only** (target milestone `M-Q1-PERF`); Q6 is parked at `M-Q6-RESTORE` (later).

The legacy broad TPC-H prototype (Q1–Q22, HashJoin, MergeJoin, SubqueryScan, the legacy serial vector executor under `src/engine/exec/`, and the legacy morsel parallel runtime) was **intentionally deleted** in commits `53ac06adcb7` (step 1) and `fd9a8aaf326` (step 2). The new runtime is a DuckDB-style `PhysicalOperator` + `MetaPipeline` design over PostgreSQL DSM/DSA + parallel bgworkers.

**What runs through `pg_volvec` today:** **Nothing.** `Translator::TranslatePlan` returns `nullptr` for every nodeTag, the bridge logs `WARNING: pg_volvec: unsupported plan shape, falling back to standard PostgreSQL executor`, and `standard_ExecutorRun` produces the result. The `pg_volvec` runtime is being assembled in pieces; step 3g.2-final lights up the first end-to-end Q1 path.

## STRUCTURE

```
contrib/pg_volvec/
  src/bridge/                      # PG hook integration, GUCs, query-state HTAB     → bridge/AGENTS.md
    pg_volvec.c                    # _PG_init, GUC table, hook chaining
    state.{c,h}                    # PgVolVecQueryState HTAB + admission filter
    execute.{cpp,h}                # Translator dispatch + (future) pipeline run
  src/engine/core/                 # DataChunk, types, memory, DSA bridge, robin-hood adapter → core/AGENTS.md
  src/engine/expr/                 # Expression IR header (expr.hpp)
  src/engine/                      # expr.cpp (lowering+interpreter), JIT pair, volvec_engine.hpp
  src/engine/parallel/             # Container only; no source files                 → parallel/AGENTS.md
  src/engine/parallel/pipeline/    # PhysicalOperator IR + MetaPipeline runtime      → parallel/pipeline/AGENTS.md
                                   # (the only runtime; 41 files, 17 active TUs)
  sql/, expected/                  # Regress (most pre-greenfield .sql/.out deleted; harness skeletal)
  docs/                            # Mostly pre-greenfield; banner-marked STALE
  third_party/                     # Vendored robin-hood-hashing
  perf/                            # Q1 perf progression notes
```

Built sources are listed in `meson.build` (lines 6–28 + LLVM JIT pair). 19 active translation units when LLVM is enabled:

- C bridge: `src/bridge/pg_volvec.c`, `src/bridge/state.c`
- C++ bridge: `src/bridge/execute.cpp`
- Expression: `src/engine/expr.cpp`
- Pipeline (17): `pipeline_worker_main.cpp`, `pipeline_leader.cpp`, `physical_operator.cpp`, `meta_pipeline.cpp`, `pipeline_descriptor.cpp`, `event.cpp`, `pipeline_run_event.cpp`, `pipeline_combine_event.cpp`, `pipeline_finalize_event.cpp`, `task.cpp`, `dsm_task_queue.cpp`, `task_scheduler.cpp`, `physical_seq_scan.cpp`, `physical_hash_aggregate.cpp`, `physical_order.cpp`, `output_sink.cpp`, `translator.cpp`
- Core DSA: `src/engine/core/parallel_dsa_bridge.cpp`
- LLVM JIT pair (when `llvm.found()`): `src/engine/llvmjit_deform_datachunk.cpp`, `src/engine/llvmjit_expr.cpp`

`src/engine/llvmjit_deform_datachunk.h` (note `.h`, not `.hpp`) is a stale duplicate of `pg_volvec_try_compile_jit_expr` from `expr/expr.hpp`; cleanup deferred to JIT-wiring time. `src/engine/data_chunk_deform.hpp` duplicates `src/engine/core/data_chunk_deform.hpp` — ongoing refactor.

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| Hook + GUC + admission | `src/bridge/` | See `bridge/AGENTS.md` |
| Pipeline runtime / parallel | `src/engine/parallel/pipeline/` | See `parallel/pipeline/AGENTS.md`; bridge entry will be `pipeline::PgvolvecPipelineRun` (currently a stub returning `false`) |
| DataChunk / types / DSA bridge | `src/engine/core/` | See `core/AGENTS.md` |
| Expression lowering | `src/engine/expr.cpp`, `expr/expr.hpp` | Linear IR + interpreter |
| Expression JIT | `src/engine/llvmjit_expr.cpp` | LLVM, gated by `USE_LLVM` |
| Deform JIT | `src/engine/llvmjit_deform_datachunk.cpp` | LLVM, gated by `USE_LLVM` |
| Plan→PhysicalOperator translation | `parallel/pipeline/translator.cpp` | Returns `nullptr` for every nodeTag today |
| Add a regress test | `sql/` + `expected/` + `meson.build` regress list | Most pre-greenfield .sql/.out files have been deleted; harness is intentionally skeletal until M-Q1-PERF |

## CONVENTIONS

- **C/C++ boundary**: C files use `pg_volvec_` prefix and `extern "C"` headers when included from C++. C++ uses `namespace pg_volvec` (and `pg_volvec::pipeline` for runtime). Engine `.hpp` files wrap PG includes in `extern "C" {}`.
- **Include guards**: C headers `#ifndef PG_VOLVEC_*`. C++ headers `#pragma once`.
- **Classes**: pipeline interfaces use plain names in `pg_volvec::pipeline` (`PhysicalOperator`, `MetaPipeline`, `Task`, `Event`, `Source`, `Sink`). The legacy `Vec*State` family from the deleted `src/engine/exec/` is gone — do not reintroduce it.
- **Indent**: Tabs, 4-space width (`.editorconfig`).
- **Build**: Meson only. Module-level `-O3 -march=native -ftree-vectorize -funroll-loops -ffast-math` applied to all C/C++.
- **Memory**: PostgreSQL `MemoryContext` (palloc/pfree/AllocSetContextCreate) for per-query objects; `PgMemoryContextAllocator` for STL containers. Never raw malloc in hot paths.
- **Cross-process state**: Leader-built objects that need to reach workers are serialized into DSA via `pipeline_descriptor.cpp` (Store/LoadSharedPayload). Atomics live in `PipelineSharedControl` directly (DSM-resident).
- **JIT symbol resolution**: Prefer `dlopen(NULL)` + `dlsym` from running process before loading the provider library.
- **Restart discipline**: Always `pg_ctl restart` after `meson install`. The backend will not pick up a freshly installed `pg_volvec.so` on its own.
- **No `.inc` template files.** The pipeline runtime has none; the legacy `runtime_*.inc` family is deleted.

## ANTI-PATTERNS (THIS PROJECT)

- **Do NOT assume backend picks up new `.so` without restart.** Always `pg_ctl restart` after `meson install`.
- **Do NOT reintroduce the legacy serial executor or morsel runtime.** `src/engine/exec/`, `Vec{SeqScan,Filter,Agg,Project,Sort,Limit}State`, `parallel_runtime.cpp`, `runtime_*.{cpp,hpp,inc}`, `ParallelPipelineRole/Desc/Driver/Sink`, `LoweredPipeline`, `WorkerPipelineExecutor`, `pipeline_lowering.{hpp,cpp}`, `q1_translator.*`, `partial_agg_op.*`, `agg_sink.*`, `seq_scan_source.*`, `worker_context.hpp` — all intentionally deleted.
- **Do NOT widen Q1 translator to other queries.** Q2–Q22 are intentionally out of scope for `M-Q1-PERF`. Q6 is parked at `M-Q6-RESTORE`.
- **Do NOT extend Sort beyond MaxThreads=1, in-memory single-run.** External sort is post-3g.2.
- **Do NOT introduce ExprBytecode lowering for non-null quals** in 3g.2. Direct interpreter path is the in-scope baseline.
- **Do NOT change `PhysicalOperator` base virtual signatures** (locked in `eb7901b022a`). New per-operator state goes through descriptor-resident payloads, not new virtuals.
- **Do NOT publish palloc'd pointers to DSA.** Use DSA offsets via `dsa_get_address`/`dsa_allocate`. Atomics go in DSM-resident `PipelineSharedControl`, not DSA.
- **Do NOT call `elog(ERROR)` from a worker without first writing `PipelineSharedControl.worker_error{,_msg}`.** Otherwise the leader cannot distinguish worker death from clean FINISHED. The `worker_error_msg` buffer is `PIPELINE_WORKER_ERROR_MSG_LEN = 256` bytes (`dsm_control.hpp:20`).
- **Do NOT skip `MemoryContextSwitchTo` before C++ object construction.** JIT and operator construction require the right context.
- **Do NOT touch `parallel_plan` / `parallel_scheduler` void* fields outside `parallel/pipeline/`.** They are opaque sentinels everywhere else (the bridge stores the descriptor IR root and the scheduler this way; layout is shared via `query_state.hpp`).
- **Do NOT enable JIT for Wide128 numeric expressions.** Interpreter-only for correctness.
- **Experimental GUCs** (`parallel_experimental_hash_pipeline` etc.) registered in `pg_volvec.c` may be inert — delete callers, not the GUC.

## COMMANDS

```bash
# Build
CCACHE_DISABLE=1 PATH=/opt/homebrew/bin:$PATH meson compile -C build pg_volvec

# Install + restart (always restart after install)
CCACHE_DISABLE=1 PATH=/opt/homebrew/bin:$PATH meson install -C build --only-changed
./installed/bin/pg_ctl -D ~/data/pg_tpch restart -m fast -l ~/data/pg_tpch/logfile

# Regress (skeletal today; harness predates M-FRAME-MIN refresh)
meson test -C build --suite pg_volvec

# Manual on the 10G TPC-H instance (executes through standard PG today)
./installed/bin/psql -h /tmp -p 5432 -d tpch -f contrib/pg_volvec/sql/q1.sql

# Disable PG parallel for fair single-thread benchmarks
SET max_parallel_workers_per_gather = 0;
SET parallel_setup_cost = 1000000000;
```

Expected results today (produced by **standard PG**, not `pg_volvec`):
- Q1 on small fixture: 4 rows.
- Q6 on 10G TPC-H: `1230113636.0101`.

## NOTES

- **Active scope:** Q1 only (M-Q1-PERF). Q2–Q22 intentionally out of scope.
- **Step 3g.2-prep delivered (HEAD):** descriptor IR scaffolding (`pipeline_descriptor.{hpp,cpp}`, `pipeline_dsm_lookup.hpp`), DSM keys retired and replaced (`PIPELINE_DSM_KEY_{CONTROL,DSA,TASK_QUEUE}` in `0xD8…` range, magic `0x56505043`), `EventShmState` array allocated through `PipelineSharedControl.event_states_root`, MPMC `DsmTaskQueue` (Vyukov), `Event` with atomic dependency machinery, `Task` triple (`PipelineRunEvent`/`PipelineCombineEvent`/`PipelineFinalizeEvent`) with header+ctor, `TaskScheduler` (`SchedulerState` + `BindRuntime` + `AllocateEventShmStates` + `EnqueueTasks` dispatching on `Event::kind()`), `worker_error_msg[256]` shipped per Oracle C7 design.
- **Step 3g.2-final pending:** `PgvolvecPipelineRun` real implementation (replaces `pipeline_leader.cpp` 33-line stub returning `false`); worker `main` (replaces `pipeline_worker_main.cpp` 28-line `elog(ERROR)` stub); Task `Execute()` bodies (`task.cpp` 49-line stub returning `TASK_FINISHED`); `Translator::TranslatePlan` Q1 shape-matcher (`translator.cpp` 35-line stub returning `nullptr`); `physical_seq_scan` `AppendProjectedTupleToChunk` real body; `physical_hash_aggregate` `SinkChunk` `rf`/`ls` `'\0'` bug fix + `GetData` column writes; `physical_order` `SinkChunk`/`GetData` column writes; bridge wire-up at `src/bridge/execute.cpp` to call `PgvolvecPipelineRun` instead of falling through.
- **Authoritative plans:** `.sisyphus/plans/3g2-final-delta-map.md` (3619 lines, v2; `§C6` lines 2900-3100 covers the runtime cut-over); `.sisyphus/plans/pipeline-port-plan.md` (`§6.5.6` Q1 shape).
- **Authoritative design:** `contrib/pg_volvec/docs/GLOBAL_LOCAL_STATE_DESIGN.md` (`§6.3`, `§8.5.2-§8.5.4`, `§8.6`); `§8.7` is **not** to be implemented as written (per design doc line 1428).
- **Test data:** 10G TPC-H lives at `~/data/pg_tpch` (socket `/tmp`, port 5432, db `tpch`).
- **Profiling:** `/usr/bin/sample` on macOS; FlameGraph tools at `~/proj/postgres/FlameGraph/`.
- **Pre-greenfield docs** under `docs/` (DESIGN.md, ROADMAP.md, LOCAL_RUNBOOK.md, jit_deform_datachunk.md, llvmjit_expr.md, page-wise-scan.md) reflect the old codebase; they are banner-marked `STALE` and should not be trusted for current-shape work.

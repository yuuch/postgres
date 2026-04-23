# pg_volvec KNOWLEDGE BASE

**Refreshed:** 2026-04-23
**Commit:** e00e99dd38f
**Phase:** Greenfield (Plan B) — pipeline-only runtime, HashJoin removed.

## OVERVIEW

PostgreSQL extension that hooks `ExecutorStart/Run/Finish/End` to offload supported OLAP plan subtrees into a C++ columnar `DataChunk` executor with LLVM JIT on tuple deform and expression evaluation. Active scope is **Q1 and Q6** on TPC-H. Parallel execution uses a DuckDB-style push pipeline `(Source → Operators → Sink)` over PostgreSQL DSM/DSA + bgworkers. C bridge + C++ engine, Meson build, PostgreSQL 19devel.

The previous release supported 18+ TPC-H queries via a HashJoin + morsel-driven runtime. Both have been **deleted** as part of the greenfield rewrite. Do not attempt to restore them; pre-greenfield TPC-H coverage is intentionally not a goal.

## STRUCTURE

```
contrib/pg_volvec/
  src/bridge/                   # PG hook integration, GUCs, query-state HTAB (5 files)         → bridge/AGENTS.md
  src/engine/core/              # DataChunk, types, memory, DSA bridge, robin-hood adapter      → core/AGENTS.md
  src/engine/exec/              # Serial vector operators: SeqScan, Filter, Agg, Project, …    → exec/AGENTS.md
  src/engine/parallel/          # Container only; design docs                                   → parallel/AGENTS.md
  src/engine/parallel/pipeline/ # DuckDB-style push pipeline (the active runtime)              → parallel/pipeline/AGENTS.md
  src/engine/expr/              # Expression IR header (expr.hpp)
  src/engine/                   # JIT (llvmjit_expr, llvmjit_deform), expr.cpp, dead executor.cpp/parallel_runtime.cpp
  sql/ + expected/              # Regress tests; only smoke + q1 + q6 wired in CI
  tests/                        # Benchmark + profiling scripts
  scripts/                      # Three-way comparison benchmarks (PG vs pg_duckdb vs pg_volvec)
  docs/                         # Design docs, runbook, JIT notes
  third_party/                  # Vendored robin-hood-hashing
  benchmarks/                   # Checked-in benchmark snapshots
  profiles/                     # Profiling artifacts
```

Built sources are listed in `meson.build` (lines 5–29 + 45). 23 translation units total: 2 C bridge, 1 C++ bridge, 1 expr, 9 pipeline, 1 core (`parallel_dsa_bridge.cpp`), 9 exec, plus 2 LLVM JIT files when `llvm.found()`. Files on disk that are NOT built: `src/engine/executor.cpp`, `src/engine/parallel_runtime.cpp` (legacy reference; delete-on-sight candidates).

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| Hook + GUC + admission | `src/bridge/` | See `bridge/AGENTS.md` |
| Serial operator | `src/engine/exec/` | See `exec/AGENTS.md`; register in `executor_init.cpp` |
| Pipeline runtime / parallel | `src/engine/parallel/pipeline/` | See `parallel/pipeline/AGENTS.md`; entry `pipeline::PgvolvecPipelineRun` |
| Shared types / DataChunk / DSA | `src/engine/core/` | See `core/AGENTS.md` |
| Expression lowering | `src/engine/expr.cpp`, `expr/expr.hpp` | Linear IR + interpreter |
| Expression JIT | `src/engine/llvmjit_expr.cpp` | LLVM, gated by `USE_LLVM` |
| Deform JIT | `src/engine/llvmjit_deform_datachunk.cpp` | LLVM, gated by `USE_LLVM` |
| Add regress test | `sql/` + `expected/` + `meson.build` regress list | Default CI runs only `smoke`, `q1`, `q6` |
| Benchmark | `tests/bench_tpch_*.sh` | Median of 3 |
| Profile | `tests/profile_query.sh` | macOS `/usr/bin/sample` + FlameGraph |

## CONVENTIONS

- **C/C++ boundary**: C files use `pg_volvec_` prefix and `extern "C"` headers when included from C++. C++ uses `namespace pg_volvec`. Engine `.hpp` files wrap PG includes in `extern "C" {}`.
- **Include guards**: C headers `#ifndef PG_VOLVEC_*`. C++ headers `#pragma once`.
- **Classes**: PascalCase with `Vec` prefix in serial executor (`VecPlanState`, `VecSeqScanState`). Pipeline interfaces use plain names in `pg_volvec::pipeline` (`Source`, `Operator`, `Sink`, `Pipeline`).
- **Indent**: Tabs, 4-space width (`.editorconfig`).
- **Build**: Meson only. Module-level `-O3 -march=native -ftree-vectorize -funroll-loops -ffast-math` applied to all C/C++.
- **Memory**: PostgreSQL `MemoryContext` (palloc/pfree/AllocSetContextCreate) for per-query objects; `PgMemoryContextAllocator` for STL containers. Never raw malloc in hot paths.
- **JIT symbol resolution**: Prefer `dlopen(NULL)` + `dlsym` from running process before loading the provider library.
- **Restart discipline**: Always `pg_ctl restart` after `meson install`. The backend will not pick up a freshly installed `pg_volvec.so` on its own.
- **No `.inc` template files.** The pipeline runtime has none; the legacy `runtime_*.inc` family is deleted.

## ANTI-PATTERNS (THIS PROJECT)

- **Do NOT assume backend picks up new `.so` without restart.** Always `pg_ctl restart` after `meson install`.
- **Do NOT reintroduce HashJoin or the legacy parallel runtime.** `hash_join*.cpp`, `parallel_runtime.cpp`, `runtime_*.{cpp,hpp,inc}`, `ParallelPipelineRole/Desc/Driver/Sink`, `TaskKind` — all intentionally removed.
- **Do NOT widen the supported plan shape beyond `SeqScan -> [Filter] -> PartialAgg -> AggSink`** without going through `pipeline::LowerToPipeline` admission.
- **Do NOT enable JIT for Wide128 numeric expressions, int-like comparisons, or numeric division.** Interpreter-only for correctness.
- **Do NOT expect Sort to handle numeric average keys.** Will `ereport(ERROR)`, not fallback.
- **Do NOT use raw malloc/free for per-query executor objects.** Allocate via PostgreSQL `MemoryContext` to prevent leaks on error.
- **Do NOT call `elog(ERROR)` without `PG_TRY/PG_CATCH` protection** around C++ resource ownership (JIT contexts, DSA memory, pipeline state).
- **Do NOT publish palloc'd pointers to DSA.** Use DSA offsets via `dsa_get_address`/`dsa_allocate`.
- **Do NOT skip `MemoryContextSwitchTo` before C++ object construction.** JIT and operator construction require the right context.
- **Do NOT add new regress tests without updating the `meson.build` regress list** (~line 84) if you want them in CI.
- **Do NOT touch `parallel_plan` / `parallel_scheduler` void* fields outside `parallel/pipeline/`.** They are opaque sentinels everywhere else.
- **Experimental GUC `parallel_experimental_hash_pipeline` has no effect.** Kept registered to avoid breaking config files; delete callers, not the GUC.

## COMMANDS

```bash
# Build
CCACHE_DISABLE=1 PATH=/opt/homebrew/bin:$PATH meson compile -C build pg_volvec

# Install + restart
CCACHE_DISABLE=1 PATH=/opt/homebrew/bin:$PATH meson install -C build --only-changed
./installed/bin/pg_ctl -D ~/data/pg_tpch restart -m fast -l ~/data/pg_tpch/logfile

# Regress (smoke + q1 + q6 only)
meson test -C build pg_volvec

# Manual query (10G TPC-H instance)
./installed/bin/psql -h /tmp -p 5432 -d tpch -f contrib/pg_volvec/sql/q1.sql
./installed/bin/psql -h /tmp -p 5432 -d tpch -f contrib/pg_volvec/test_q6_10g.sql

# Disable PG parallel for fair single-thread benchmarks
SET max_parallel_workers_per_gather = 0;
SET parallel_setup_cost = 1000000000;
```

Q6 expected result on 10G TPC-H: `1230113636.0101`. Q1: 4 rows.

## NOTES

- Active scope: **Q1 and Q6 only**. Other TPC-H queries (Q2–Q22) are NOT goals of the greenfield phase. README's status section reflects pre-greenfield reality and will be refreshed separately.
- `src/engine/data_chunk_deform.hpp` duplicates `src/engine/core/data_chunk_deform.hpp` — ongoing refactor.
- Two on-disk-but-not-built files (`src/engine/executor.cpp`, `src/engine/parallel_runtime.cpp`) survive the demolition only as historical reference. Safe to delete.
- llvmjit library linked via absolute path in `meson.build` (macOS-specific).
- Test data: 10G TPC-H lives at `~/data/pg_tpch` (socket `/tmp`, port 5432, db `tpch`). Regress tests under `sql/` are self-contained.
- Profiling uses `/usr/bin/sample` on macOS; FlameGraph tools at `~/proj/postgres/FlameGraph/`.
- Recent: P2.13 demolition removed HashJoin + the legacy morsel runtime; pipeline runtime is the only path. P2.13d cleanup landed `e00e99dd38f`.

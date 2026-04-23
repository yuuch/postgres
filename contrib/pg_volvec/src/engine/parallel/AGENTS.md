# parallel/ — Pipeline Container

This directory holds the **pipeline-based** parallel runtime (greenfield, post-demolition).

The legacy morsel-driven runtime (`runtime_lowering.cpp`, `runtime_execution.cpp`, `runtime_worker_main.cpp`, `runtime_worker_state.cpp`, `parallel_runtime.hpp`, `parallel_runtime_internal.hpp`, `runtime_*.inc`) has been **deleted**. Do not look for it.

## STRUCTURE

- `pipeline/` — DuckDB-style push pipeline (Source → Operators → Sink). The only runtime in use today. **See `pipeline/AGENTS.md`.**
- `PIPELINE_REFACTOR_DESIGN.md` — Design doc for the greenfield refactor (Plan B / full rewrite).
- `GLOBAL_PARTITION_DESIGN.md` — Earlier design for global partition coordination (historical reference).

No source files live at this level. All built sources are under `pipeline/`.

## WHERE TO LOOK

| Task | Location |
|------|----------|
| Add a new pipeline shape / source / operator / sink | `pipeline/` (see `pipeline/AGENTS.md`) |
| Bridge entry into the runtime | `bridge/execute.cpp` calls `pipeline::PgvolvecPipelineRun(...)` |
| Per-worker shared aggregate state struct | `ParallelAggPartialState` (preserved through demolition) |
| Worker bgworker lifecycle | `pipeline/pipeline_worker_main.cpp`, `pipeline_worker_state.cpp` |
| DSM/DSA layout | `pipeline/dsm_control.hpp` (`PIPELINE_DSM_KEY_*`, `PIPELINE_DSM_MAGIC`) |

## CONVENTIONS

- The directory exists only as a container. **Add no `.cpp/.hpp` here.** New code goes under `pipeline/`.
- The `ParallelAggPartialState` family + `VOLVEC_PARALLEL_MAX_*` constants are intentionally kept across the refactor; the pipeline runtime depends on them.
- Design docs at this level describe direction; `pipeline/AGENTS.md` describes what is actually implemented.

## ANTI-PATTERNS

- **Do NOT recreate `parallel_runtime.cpp` or any `runtime_*.{cpp,hpp,inc}`.** They were intentionally deleted in the greenfield rewrite.
- **Do NOT add a parallel runtime parallel to `pipeline/`.** One runtime only.
- **Do NOT reintroduce `ParallelPipelineRole`, `ParallelPipelineDesc`, `ParallelPipelineDriver`, `ParallelPipelineSink`, or any `TaskKind` enum.** All deleted; pipeline shape is implicit in `LoweredPipeline`.
- **Do NOT reference `runtime_*.inc` template includes.** The pipeline runtime has no `.inc` template files.

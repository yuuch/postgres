# pg_volvec Design

Status refreshed: `2026-04-17`

## Goal

`pg_volvec` is a PostgreSQL extension that offloads a supported subset of
analytical query plans to a vectorized executor. PostgreSQL still parses,
rewrites, plans, owns snapshots, and delivers rows to the client.

The extension's job is narrower:

- decide whether the current plan shape is acceptable
- lower that plan into vectorized execution state
- run it either serially or through `pg_volvec`'s process-parallel runtime
- materialize final results back into PostgreSQL slots

## Current Execution Model

There are two internal execution layers.

### 1. `VecPlanState`

This is the existing vector executor tree used for:

- serial execution
- result metadata
- slot materialization
- expression metadata lookup
- hash-join / aggregate state lookup needed by the parallel runtime

The tree is built from PostgreSQL `Plan` nodes by the code now split under
`src/engine/exec/`.

### 2. `ParallelPipelinePlan`

When `pg_volvec.parallel=on` and the accepted shape is supported, the same
PostgreSQL plan is also lowered into a pipeline DAG used by the process-based
morsel scheduler in `src/engine/parallel_runtime.cpp`.

That DAG describes:

- source pipelines
- breaker/finalize pipelines
- bridge dependencies
- `pipeline x morsel` tasks

`pg_volvec` does not own planning. PostgreSQL's physical plan remains the
source of truth.

## Layers

### 1. Hook / bridge layer

Files under `src/bridge/` install executor hooks, decide whether the current
query is admitted, and manage query-state registration.

Responsibilities:

- `ExecutorStart` / `ExecutorRun` / `ExecutorEnd` hooks
- GUCs
- query-state registration / lookup
- tuple-slot materialization back to PostgreSQL

### 2. Core execution primitives

Files under `src/engine/core/` provide:

- `DataChunk`
- `SelectionVector`
- `VecStringRef`
- `MemoryContext`-backed allocators
- deform program helpers
- serialized hash metadata

These are the shared low-level building blocks used by both serial and
parallel execution.

### 3. Expression layer

Files under `src/engine/expr/`, `src/engine/expr.cpp`, and
`src/engine/llvmjit_expr.cpp` define and execute `VecExprProgram`.

The expression layer supports two runtimes:

- interpreter
- LLVM JIT

Both share the same linear step IR.

### 4. Operator layer

Files under `src/engine/exec/` now hold the real operator implementation:

- `SeqScan`
- `Filter`
- `Agg`
- `Sort`
- `Limit`
- `HashJoin`
- lookup/project helpers
- plan-lowering helpers

This code no longer builds through the old monolithic `executor.cpp`. The
separate compilation split is now real.

### 5. Parallel runtime layer

`src/engine/parallel/parallel_runtime.hpp` plus
`src/engine/parallel_runtime.cpp` implement:

- pipeline descriptors
- bridge descriptors
- scheduler runtime state
- worker context serialization
- process-worker launch / merge / cleanup

## Data Model

### `DataChunk`

`DataChunk` is the vector batch container shared by the engine.

It stores:

- row count
- typed column arrays
- per-column null arrays
- optional selection vector
- owned string arena when needed

The engine keeps data columnar from scan through filter, join, aggregate, and
sort.

### `DeformProgram`

`DeformProgram` describes which tuple attributes must be decoded and where they
land inside a `DataChunk`.

This is query-driven now. The scan path no longer blindly deforms a fixed
attribute prefix.

### `VecExprProgram`

Expressions are lowered into a linear step IR. That IR is the semantic middle
layer shared by the interpreter and JIT.

The hot path goal is not "evaluate a step machine forever". The hot path goal
is "use the step IR as a lowering surface for better code generation".

## Current Hot Paths

### Scan

`VecSeqScanState` drives heap scan traversal and fills output chunks.

Important current properties:

- query-driven column pruning
- heap `read_stream` integration for prefetch
- PostgreSQL parallel heap scan integration on supported process-worker paths
- direct fill into typed column arrays

### Deform

Tuple deform has two modes:

- fallback C++ path
- LLVM deform JIT path

The JIT path writes directly into `DataChunk` arrays and supports the current
TPC-H-relevant scalar mix, including owned string storage.

### Expression evaluation

`VecExprProgram` can run in the interpreter or as LLVM-generated fused loops.

Current JIT boundaries:

- supported arithmetic and predicate programs use JIT
- `Wide128` exact numeric programs do not use JIT
- int-like comparisons and numeric division currently stay on the interpreter
  path for correctness

### Aggregation

`VecAggState` supports:

- grouped and ungrouped aggregation
- exact fixed-point accumulation
- file-backed partial state for current process-worker paths
- grouped `COUNT(DISTINCT int-like)` on the currently validated subset

### Sort

`VecSortState` is a blocking in-memory vector sort.

Current properties:

- dense sort-owned payload chunks
- extracted key lanes
- indirect row-ref ordering
- final gather back into output chunks

This is enough for current final-sort TPCH shapes, but it is not yet a spill
or multi-run merge implementation.

### Hash join

`VecHashJoinState` covers the currently validated hash-join family:

- inner joins
- current right/left outer subset
- current hash-backed fallback for some `MergeJoin`-planned and right-anti
  shapes

Current process-parallel behavior is mixed:

- simple build/probe source pipelines can run through the pipeline scheduler
- nested build chains may still fall back to a leader-built shared hash bridge
- Q11 is the key example of that safe fallback behavior

## Numeric Model

TPC-H-style `NUMERIC(15,2)` runs as scaled `int64` in the hot path.

Exact numeric widening rules:

- precision `<= 18`: scaled `int64`
- precision `> 18`: `Wide128`

Aggregation uses widened accumulation. This removed expensive generic numeric
conversion from the hot scan/deform pipeline.

## Memory And Lifetime

The engine is intentionally aligned with PostgreSQL lifetime rules:

- containers allocate from PostgreSQL `MemoryContext`
- vector plan state lives in query-lifetime contexts
- worker-local plan state uses process-local contexts
- LLVM JIT resources are explicitly released during normal teardown and
  process-exit cleanup

This matters because `pg_volvec` is not an isolated standalone engine. It must
coexist cleanly with PostgreSQL's executor, snapshots, and shared-memory
teardown.

## Known Boundaries

- `Wide128` expression JIT is not implemented yet
- aggregate argument fusion (`sum(expr)` / `avg(expr)`) is not implemented yet
- sort is still first-cut in-memory only
- nested hash-build dependency chains in process-parallel lowering do not yet
  have a fully shared worker-build model
- generic semi/anti join execution is not implemented
- broad `Materialize` / rescan-heavy plan coverage is still missing

## Design Principles

1. Keep PostgreSQL planning unchanged.
2. Make fallback safe and boring.
3. Specialize hot paths only where they pay on real OLAP queries.
4. Prefer query-driven metadata and pruning over generic tuple materialization.
5. Keep memory lifetime and JIT lifetime aligned with PostgreSQL cleanup.

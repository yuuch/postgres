# pg_volvec Design

## Goal

`pg_volvec` is a PostgreSQL extension that offloads a small but performance-critical subset of analytical query plans to a vectorized executor. PostgreSQL still parses, rewrites, and plans the query. `pg_volvec` decides at executor startup whether a supported subtree can run in the extension.

## Current Supported Shape

Today the supported shape is intentionally narrow:

```text
SeqScan
  -> optional qual
  -> Agg
```

That shape is enough to cover the currently verified Q1 no-order and Q6 paths. `Sort`, `Join`, `Gather`, `Materialize`, and other plan nodes are still out of scope.

## Layers

### 1. Hook / bridge layer

`src/bridge/pg_volvec.c` installs executor hooks, decides whether the current plan is eligible, and routes execution to `pg_volvec`.

### 2. Vectorized plan state tree

`ExecInitVecPlan()` in [src/engine/executor.cpp](src/engine/executor.cpp) recursively builds a C++ `VecPlanState` tree from the PostgreSQL `Plan` tree. There is no separate IR between the PostgreSQL plan and the vectorized operators.

### 3. Execution engine

Operators exchange `DataChunk` batches:

- `VecSeqScanState`
- `VecFilterState`
- `VecAggState`

Final results are materialized back into PostgreSQL slots and sent to the normal `DestReceiver`.

## Data Model

### DataChunk

`DataChunk` is a fixed-capacity batch container with:

- `count`
- typed column arrays for `double`, `int64`, `int32`, and `VecStringRef`
- per-column null arrays
- an optional `SelectionVector`

The engine keeps data columnar from scan through filter and aggregation.

### DeformProgram

`DeformProgram` describes which attributes must be decoded from a heap tuple and where they land in the `DataChunk`. It is now built from actual plan needs instead of blindly materializing a fixed prefix of the relation.

### VecExprProgram

Expressions are lowered into a linear step IR (`VecExprStep`). This is the shared middle layer used by:

- the interpreter in `expr.cpp`
- the LLVM expression JIT in `llvmjit_expr.cpp`

The IR is intentionally simple, but the hot path does not have to execute it interpretively.

## Current Hot Path

### Scan

`VecSeqScanState` drives page/block iteration in C++, handles visibility, and calls `DataChunkDeformer` to append visible rows into the output chunk.

### Deform

Deforming has two modes:

- fallback C++ path using `heap_getattr()`
- LLVM JIT path that writes directly into `DataChunk` arrays

The JIT path now supports the TPC-H-relevant type mix used by Q1 / Q6, including string prefix materialization and fixed-point numeric decoding.

### Filter / expression

`VecFilterState` evaluates a `VecExprProgram` over the chunk. For supported expressions, LLVM emits a fused loop instead of the old step-by-step interpreter. This eliminates materialized temporary vectors for expressions like:

```text
(a - b) * c
```

The generated row loop computes the final result directly.

### Aggregation

`VecAggState` uses a hash table keyed by grouping prefixes and supports `SUM`, `COUNT`, and `AVG` for the current verified paths. Exact fixed-point aggregation uses widened integer accumulation instead of floating-point fallback.

## Memory and Lifetime

The engine has been moved closer to PostgreSQL lifetime rules:

- STL containers use allocators backed by PostgreSQL `MemoryContext`
- main vectorized objects allocate out of PostgreSQL contexts instead of raw `new`
- LLVM JIT contexts are released explicitly through PostgreSQL's provider-side release path

This keeps query-lifetime state aligned with PostgreSQL cleanup behavior.

## Design Choices That Matter Right Now

### Column pruning

Only the columns referenced by scan quals and upper targetlists are materialized. This was a major step in making Q6 competitive.

### Fixed-point numerics

The engine treats TPC-H-style `NUMERIC(15,2)` values as scaled `int64` in the hot path and uses widened arithmetic for aggregation. This removed expensive generic numeric conversion from the main scan/deform pipeline.

### Fused JIT loops instead of nested helpers

The expression JIT does not build a tree of runtime helper calls. It uses the linear step IR to emit a fused LLVM loop where intermediate values stay in SSA temporaries. That is a better fit for LLVM optimization and eventual SIMD-friendly kernels.

## Current Limitations

- Full Q1 with `ORDER BY` is not offloaded.
- Expression JIT still has only a generic nullable path, not a dedicated `no-null` specialization.
- Aggregation is not yet fused with expression evaluation.
- The engine still covers only a small subset of PostgreSQL plan space.

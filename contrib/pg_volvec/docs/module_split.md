# pg_volvec Module Split Record

## Status

As of 2026-04-17, `pg_volvec` has moved past a purely logical file split and now builds the `exec/` subtree as real separate translation units.

What is true now:

- `meson.build` no longer compiles `src/engine/executor.cpp`
- `src/engine/exec/*.cpp` are compiled directly
- `src/engine/executor.cpp` is still kept in the tree as a reference file, but it is no longer part of the build
- `src/engine/exec/internal.hpp` now acts as the private internal aggregation header for cross-module helpers
- `src/engine/volvec_engine.hpp` has been shrunk toward a narrower facade

This is a meaningful architectural step. The split is no longer just directory decoration; it now affects the actual build graph.

At the same time, this is still not the final architecture. A number of files are still transitional in how cleanly their names match their contents, and `exec/internal.hpp` is still broader than an ideal internal API.

## Current Layout

### `src/bridge/`

Purpose: PostgreSQL integration layer.

Current files:

- `pg_volvec.c`
- `state.c`
- `state.h`
- `execute.cpp`
- `execute.h`

Responsibilities:

- install executor hooks
- manage query-state registration / lookup
- translate between PostgreSQL `QueryDesc` / slots and the vectorized engine
- decide whether the current query should run in `pg_volvec`

Assessment:

- This split is good.
- The bridge layer is small and clearly PostgreSQL-facing.
- The main remaining issue is that the bridge still reaches a broader engine facade than strictly necessary.

### `src/engine/core/`

Purpose: low-level shared primitives.

Current files:

- `types.hpp`
- `memory.hpp`
- `data_chunk.hpp`
- `data_chunk_deform.hpp`
- `hash_table_defs.hpp`

Responsibilities:

- common type aliases and PostgreSQL-facing C++ base types
- `MemoryContext` allocators and object lifetime helpers
- `DataChunk`, string refs, selection vectors
- deform program / deform bindings primitives
- serialized/shared hash table metadata structs

Assessment:

- This split is good.
- `core/` is the right home for low-level execution primitives.
- `types.hpp` is still heavy and PostgreSQL-header-rich, so `core/` is not yet a lightweight compile foundation.
- Legacy siblings such as `src/engine/data_chunk_deform.hpp` and `src/engine/hash_table.hpp` still exist, so the source-of-truth story is not fully cleaned up.

### `src/engine/expr/`

Purpose: expression IR surface.

Current files:

- `expr.hpp`

Related implementation files still at engine root:

- `expr.cpp`
- `llvmjit_expr.cpp`

Responsibilities:

- define `VecExprStep`, `VecExprProgram`, and expression JIT-related interfaces
- support lowering PostgreSQL expressions into vector IR
- interpret or JIT expression programs

Assessment:

- The split is directionally right.
- The public expression surface is now isolated from the operator files.
- The implementation is still partly at engine root, so the physical split is not fully closed.

### `src/engine/exec/`

Purpose: operator and plan-state layer.

Current files:

- `plan_state.hpp`
- `query_state.hpp`
- `internal.hpp`
- `seq_scan.hpp`, `seq_scan.cpp`
- `agg.hpp`, `agg.cpp`, `agg_plan.cpp`
- `filter.hpp`, `filter.cpp`
- `lookup.hpp`
- `project.hpp`, `project.cpp`
- `limit.hpp`, `limit.cpp`
- `hash_join.hpp`, `hash_join.cpp`, `hash_join_lookup.cpp`, `hash_join_parallel.cpp`
- `sort.hpp`, `sort.cpp`
- `executor_common.cpp`
- `executor_init.cpp`

Responsibilities:

- define `VecPlanState` and concrete operator state classes
- hold operator implementations
- hold plan-lowering helpers from PostgreSQL plan nodes to vector plan states
- hold query-state and plan-building helpers used by bridge / parallel code

Assessment:

- This is the most important part of the split and the main place where the refactor is now real.
- The operator code is no longer compiled through `executor.cpp`; it is built directly from `exec/*.cpp`.
- `internal.hpp` is the current private integration point for cross-module helpers.

What is good:

- separate compilation is real now
- operator code is easier to find
- the boundary between "public engine surface" and "private operator glue" is clearer than before
- aggregate plan canonicalization lives in `agg_plan.cpp`
- parallel hash build/bridge serialization lives in `hash_join_parallel.cpp`

What is still transitional:

- `internal.hpp` is still quite broad
- several helper functions that used to be hidden by unity-build ordering are now explicitly shared, which is progress, but also evidence that the private API still needs pruning
- some files are still named by destination intent rather than perfectly matching every implementation they contain

### `src/engine/parallel/`

Purpose: parallel pipeline and scheduler interfaces.

Current files:

- `parallel_runtime.hpp`
- `runtime_lowering.inc`
- `runtime_worker_state.inc`
- `runtime_execution.inc`
- `runtime_worker_main.inc`

Related root facade:

- `parallel_runtime.cpp`

Responsibilities:

- pipeline descriptors
- scheduler runtime state
- bridge / morsel task metadata
- process-parallel worker context structures

Assessment:

- This split is transitional but useful.
- `parallel_runtime.cpp` is now only a thin translation-unit facade that keeps
  the include order and namespace structure stable.
- The actual runtime implementation has moved under `src/engine/parallel/`
  as smaller implementation shards.
- The shards are intentionally still compiled through one translation unit for
  now. That avoids a large symbol-boundary churn while the scheduler and
  HashJoin pipeline model are still moving.
- The next cleanup step is to turn stable shards into real separate
  translation units with a smaller `parallel/internal.hpp`.

### Top-level engine files

Current files:

- `volvec_engine.hpp`
- `executor.cpp`
- `parallel_runtime.cpp`
- `expr.cpp`
- `llvmjit_expr.cpp`
- `llvmjit_deform_datachunk.cpp`

Assessment:

- `volvec_engine.hpp` is no longer the only way to pull in the full engine surface.
- `executor.cpp` still exists, but it is now a reference artifact, not a build artifact.
- `parallel_runtime.cpp` still exists, but it is now a small facade rather than
  the full implementation body.
- The compile-time dependency graph is substantially better than before, but the engine still carries a broad private helper surface through `exec/internal.hpp`.

## Is The Split Reasonable?

Short answer: yes.

The split is reasonable if the goal is:

- make the code navigable
- stop extending one monolithic executor file
- move toward real subsystem boundaries
- allow separate compilation of operator code

That goal is now meaningfully met.

The split is not yet complete if the goal is:

- minimal internal dependency surfaces
- one-to-one alignment between filenames and implementation responsibility
- a small, stable bridge-facing facade

So the right characterization is:

- `Reasonable and already useful`
- `No longer just cosmetic`
- `Still not the final architecture`

## What Is Good About The Current Split

- The top-level responsibilities are clearer: `bridge`, `core`, `expr`, `exec`, `parallel`.
- `exec/*.cpp` now build as separate translation units.
- `executor.cpp` is no longer part of the hot path of day-to-day builds.
- The split matches the system mental model:
  PostgreSQL hook layer -> vector engine primitives -> expression engine -> operators -> parallel scheduler.

## What Still Feels Transitional

### 1. `exec/internal.hpp` is still broad

This header is now the main private glue point between operator modules.

Implications:

- the build graph is better, but the private helper surface is still larger than ideal
- some helpers that should eventually become file-local or move into smaller shared headers are still globally visible inside the `exec/` layer

### 2. Some files still carry mixed responsibilities

Examples:

- `hash_join.cpp` contains true hash-join execution code plus a number of plan-lowering helpers
- `executor_init.cpp` contains both top-level plan construction and some plan-shape-specific helper logic
- `project.cpp` owns direct projection plus lookup-project operators

These are acceptable transitional states, but they are not yet the final clean ownership boundaries.

### 3. Old and new layouts still coexist

Examples:

- `src/engine/core/data_chunk_deform.hpp`
- `src/engine/data_chunk_deform.hpp`
- `src/engine/core/hash_table_defs.hpp`
- `src/engine/hash_table.hpp`

Implications:

- the codebase is more understandable than before, but not yet fully normalized

### 4. `volvec_engine.hpp` is smaller, but not yet minimal

It has been reduced, but it still exposes more engine surface than an ideal external facade should.

Implications:

- bridge-facing code still sees more engine detail than strictly necessary
- there is still more room to separate public API from private implementation

## Suggested Dependency Rules

### Rule 1

`bridge/` may depend on a small engine facade, but should not depend on operator-private headers.

### Rule 2

`core/` should not depend on `exec/`, `parallel/`, or `bridge/`.

### Rule 3

`expr/` may depend on `core/`, but should avoid depending on concrete operator implementations.

### Rule 4

`exec/` may depend on `core/`, `expr/`, and stable parallel interfaces.

### Rule 5

`parallel/` should depend on operator abstractions and explicit internal interfaces, not on random transitive includes.

## Recommended Next Steps

### Step 1

Reduce the size of `exec/internal.hpp`.

That is now the main internal cleanup target.

### Step 2

Keep shrinking `volvec_engine.hpp` into a genuinely small bridge-facing facade.

### Step 3

Re-cut mixed-responsibility files so filenames better match the implementations they own.

Good candidates:

- `hash_join.cpp`
- `executor_init.cpp`
- `project.cpp`

### Step 4

Once the helper surface is smaller, tighten headers further:

- keep only declarations in `*.hpp`
- move private-only helpers into the smallest possible internal headers
- prefer file-local `static` again where cross-module sharing is no longer needed

## Final Judgement

The module split is reasonable and already materially improves the codebase.

The important difference from the earlier state is this:

- before, the split was mostly organizational
- now, the split is also real in the build graph

So the current state is:

- The new directory structure is good.
- The chosen buckets are mostly correct.
- The `exec/` subtree now builds as real separate translation units.
- The main remaining cleanup is not "make it real", because that part is done.
- The main remaining cleanup is "make the private interfaces smaller and the file ownership cleaner".

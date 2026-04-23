# core/ — Low-Level Primitives & Shared Infrastructure

8 active headers + 1 source file (+ 2 `.bak` snapshots, ignored). Foundation layer used by both the serial executor (`exec/`) and the pipeline runtime (`parallel/pipeline/`).

## OVERVIEW

Shared data structures, type system, memory management, and parallel coordination primitives. `DataChunk<N>` (columnar batch), hash table metadata, DSA bridge for worker↔leader communication, robin-hood hash adapter binding to PostgreSQL allocators. Hash-table-related defs are still present even though HashJoin has been removed from `exec/` — they are reused by partial aggregation and may be pruned in a future cleanup.

## WHERE TO LOOK

| Task | File | Notes |
|------|------|-------|
| DataChunk definition | `data_chunk.hpp` | Columnar batch: vectors + selection, DEFAULT_CHUNK_SIZE=1024 |
| Tuple deform API | `data_chunk_deform.hpp` | JIT and interpreter deform; duplicated in parent (refactor pending) |
| Type system | `types.hpp` | VecType, NUMERIC(15,2) as scaled int64, Wide128 handling |
| Memory allocators | `memory.hpp` | PgMemoryContextAllocator, PgMemoryContextObject, VolVecVector alias |
| Hash table metadata | `hash_table_defs.hpp` | HashTableInfo, serialization formats, partition state |
| DSA shared bridge | `parallel_dsa_bridge.cpp/.hpp` | DSA-backed shared hash bridge, QueryHashBridgePack lifecycle |
| Robin-hood adapter | `robin_hood_pg_adapter.hpp` | Wraps robin-hood-hashing with PostgreSQL palloc/pfree |

## CONVENTIONS

- **DataChunk**: Fixed-size columnar batch (1024 rows default). Each column is a typed vector (int64*, double*, etc.) with optional selection mask.
- **Memory**: All allocations use PostgreSQL MemoryContext. STL containers must use PgMemoryContextAllocator. Never raw malloc.
- **Type semantics**: NUMERIC(15,2) → scaled int64 with widened int128 accumulators for agg. Wide128 for exact numerics (interpreter-only, no JIT).
- **DSA lifecycle**: Bridge packs allocated in DSA, published via pointer offset. Leader owns pack creation; workers attach read-only. Known issue: published packs lack explicit release point (leak).

## ANTI-PATTERNS

- **Do NOT allocate DataChunk with raw malloc.** Use MemoryContext or PgMemoryContextAllocator for STL.
- **Do NOT assume DSA pointers are stable across detach/reattach.** Convert to offsets before storing in shared memory.
- **Do NOT free DSA memory without coordination.** Multiple workers may reference the same DSA pack.

## NOTES

- `data_chunk_deform.hpp` is also duplicated at `src/engine/data_chunk_deform.hpp` — ongoing refactor; prefer this `core/` copy for new includes.
- `types.hpp.bak` and `data_chunk.hpp.bak` are pre-refactor snapshots; do not include or edit.
- Robin-hood hash adapter overrides allocate/deallocate to use palloc/pfree; required for MemoryContext reclamation.
- DSA bridge packs were originally designed for the (now deleted) parallel HashJoin path. They remain in case the pipeline runtime grows a shared hash sink; today's pipeline (Q1+Q6 shape) does not use them.
- `PipelineSharedControl` (in `parallel/pipeline/dsm_control.hpp`) is the DSM-resident control block used by the active pipeline runtime; it is NOT a DSA bridge pack.

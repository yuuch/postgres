# Step4 Design: Row-Layout Radix-Partitioned Parallel Hash Join

Status: design only, not implemented yet

## Goal

Redesign `pg_volvec`'s parallel hash-join build/probe path so the partitioned
intermediate is **row-layout based**, not `DataChunk` based.

The immediate motivation is the current 256-way radix-partition experiment:

- it introduced `DataChunk` ownership and `chunk_idx` remapping complexity
- it still relies on serialize / append / rebuild behavior in the hot path
- it regressed real queries such as Q3

The new target is closer to DuckDB's radix-partitioned hash join design:

- workers partition build rows into fixed row-layout partitions
- exported worker fragments are already partition-addressable
- task completion merges local partition fragments into global partition state
- finalize builds read-only per-partition hash metadata over row storage
- workers probe the shared row-layout bridge directly

This document is intentionally about the **build-side representation and shared
bridge contract**. It should be read together with:

- `docs/step4_partition_owned_hash_join_design.md`
- `docs/query_scheduler_qbridge_no_serialize.md`

## Why a new design document is needed

The existing Step4 document already describes the correct runtime goal:

- partition ownership
- partition-scoped scheduling
- per-partition publication and backpressure

What it does **not** yet freeze is the concrete representation of the
partitioned build-side intermediate.

That representation matters because the current `DataChunk`-centric attempt has
the wrong properties for a shared radix-partitioned bridge:

1. `DataChunk` is process-local and pointer-heavy.
2. Strings live in chunk-local arenas, so chunks cannot be shared directly.
3. Per-partition merge currently has to rebuild chunks and remap indices.
4. Probe-side addressing is forced to reason about `chunk_idx` and row-in-chunk
   instead of a stable row identifier.

DuckDB avoids this class of problems by using a row-layout intermediate for hash
join / hash aggregation internals. That is the model this document adopts.

## DuckDB reference model

DuckDB stores partitioned hash-build data in row-oriented collections rather
than columnar `DataChunk` ownership.

Relevant reference points:

- `src/execution/join_hashtable.cpp`
- `src/include/duckdb/execution/join_hashtable.hpp`
- `src/common/radix_partitioning.cpp`
- `src/include/duckdb/common/radix_partitioning.hpp`
- `src/include/duckdb/common/types/row/tuple_data_collection.hpp`

Important properties of that design:

1. **Rows are the stable unit of ownership.**
2. **Hash is stored inline with the row.**
3. **Partitions own row blocks, not vectors/chunks.**
4. **Merge/finalize concatenates or repartitions row storage, not columnar
   chunks.**
5. **Probe uses stable row locations / offsets, not chunk indices.**

We do not need to clone DuckDB exactly, but we should adopt the same core
principle: **partitioned intermediate state must be row-layout based and shared
through offset-addressable storage**.

## Current pg_volvec pain points

The current code paths that motivate this redesign are:

- `src/engine/exec/hash_join_parallel.cpp`
- `src/engine/exec/hash_join_lookup.cpp`
- `src/engine/core/hash_table_defs.hpp`
- `src/engine/core/parallel_dsa_bridge.cpp`

### 1. Worker export is still blob-oriented

`HashJoinParallelAccess::SerializeState()` and `AppendState()` serialize entries,
chunks, and payload bytes into a blob, then rebuild state on the merge side.

This means:

- merge has to parse every fragment again
- merge has to allocate fresh `DataChunk` objects
- merge has to remap worker-local `chunk_idx` into new partition-local indices

That is exactly the wrong shape for radix partitioning.

### 2. Partitioned storage still carries `DataChunk` semantics

The current partitioned state is effectively:

- `partition_entries_[p]`
- `partition_chunks_[p]`

with entries pointing back into chunk arrays.

This creates avoidable complexity:

- the same chunk may need to appear in multiple partition containers
- ownership and destruction become tricky
- double-free / dedup cleanup hazards appear
- shared bridge publication must preserve `chunk_idx` semantics

### 3. Shared bridge format still reflects the wrong abstraction

`ParallelHashBuildPartition` and `ParallelHashBuildChunk` currently publish:

- entry array offsets
- chunk metadata offsets
- payload-column metadata

This is still a chunk-reconstruction contract, not a stable shared row-storage
contract.

### 4. Probe-side addressing is harder than it should be

When the shared representation is chunk-based, probe must reason about:

- which partition owns the entry
- which chunk array the entry points into
- which row inside that chunk contains the payload

For a finalized read-only bridge, the simpler contract is:

- entry resolves to one row offset inside one partition-owned row store

### 5. Q3-style multi-join shapes amplify every one of these costs

Nested hash-build / finalize chains magnify:

- serialization cost
- merge-time allocations
- bridge publication cost
- ownership bugs
- global barriers around finalize

The current regression is a concrete signal that the representation itself needs
to change.

## Non-goals

This document does **not** require the first patch to:

- implement external spill fully
- support every join family up front
- replace every old bridge format immediately
- redesign PostgreSQL planning
- eliminate the legacy serialized path on day one

The initial target remains the current validated inner-join-heavy parallel TPC-H
family.

## Design principles

1. **Partition owns rows, not chunks.**
2. **Shared bridge contains offsets and fixed-layout metadata only.**
3. **All row references are stable within a partition.**
4. **Hash is stored inline with each build row.**
5. **Probe reads a shared row-layout view directly.**
6. **Worker export must already be partition-sliced.**
7. **The representation must naturally extend to spill later.**

## Proposed build-side representation

### Logical row contents

Each build row stored in a partition contains:

1. join key fields needed for equality verification
2. build payload fields needed for output / residual filter evaluation
3. optional match flag for outer/right-semi/right-anti extensions
4. full 64-bit hash value

The hash is stored inline so we do not recompute it during merge/finalize and so
partition routing remains stable.

### Physical layout

Each partition owns one row store made of one or more fixed-size row blocks.

Conceptually:

```text
Partition p
  RowBlock[0]
    row0: [key cols][payload cols][flags][hash]
    row1: [key cols][payload cols][flags][hash]
    ...
  RowBlock[1]
  StringArena / VarlenArea blocks
```

The shared representation must contain no backend-local pointers. All addressing
is by:

- partition id
- block-relative offset or partition-relative row ordinal
- varlen offset into partition-owned varlen storage

### Fixed-width and variable-width fields

For the first MVP:

- fixed-width scalar build fields are inlined in each row
- string / varlena-like values are stored as inline prefix + `(offset, length)`
  into a partition-owned varlen arena

This keeps the row record fixed-width while still supporting shared ownership.

### Concrete row encoding contract

For the first implementation, each row should be encoded as one fixed-width
record with a compile-time-known field layout derived from build-side metadata.

The recommended field order is:

1. null bitmap or null bytes for all stored fields
2. equality join key columns
3. payload columns needed by output / residual filter / outer-join bookkeeping
4. optional per-row flags
5. 64-bit hash value

Conceptually:

```text
| nulls | key fields | payload fields | flags | hash |
```

This order is recommended because:

- key fields are read first during probe verification
- payload fields are read only after a candidate survives hash and key checks
- flags and hash are naturally terminal metadata fields

### Fixed-width scalar encoding

The row format should store current hot-path scalar types in their native
executor representation:

- `int32` / `Oid` / date-like values as 32-bit fixed width
- `int64` / scaled numeric hot-path values as 64-bit fixed width
- `double` as 64-bit fixed width
- booleans / row flags as byte-sized fields, optionally packed later

For correctness and predictability, the MVP should prefer explicit fixed-size
slots over aggressive bit-packing.

### Null representation

The first implementation should use one byte per stored logical field rather
than a packed bitset.

Why:

- simpler encoding/decoding
- simpler direct probe-side access
- easier debugging and instrumentation

If row width later becomes a measurable problem, the null section can be packed
without changing the rest of the logical contract.

### Row flags

Reserve one fixed field for row flags even if the first inner-join MVP does not
use every bit.

Suggested initial bits:

- `MATCHED` for future right/full outer or semi/anti semantics
- `RESERVED` bits for future spill / relocation markers

This avoids another row-format version bump the moment outer-join state needs to
be recorded.

### Alignment rules

The row format should align each fixed-width field to its natural alignment
within the row-layout builder, but the row as a whole should end up with one
stable `row_width` value recorded in metadata.

The doc-level contract is:

- row width is fixed per finalized bridge
- all workers agree on the same field offsets for that bridge
- probe code never recomputes field placement heuristically

This requires a single row-layout builder routine shared by:

- local build encoding
- task-local fragment export
- global partition attach/probe logic

## Varlen / string arena contract

The current `DataChunk` model uses chunk-local string arenas. The new design
must replace that with partition-owned varlen storage.

### Varlen reference format

For the MVP, each varlen field should store:

- inline prefix (for fast comparisons / branch reduction)
- 32-bit or 64-bit offset into partition varlen storage
- 32-bit length

Conceptually:

```cpp
struct RowVarlenRef {
    uint64_t prefix;
    uint32_t offset;
    uint32_t length;
};
```

If a 32-bit offset is insufficient for future oversized partitions, the format
can move to 64-bit offsets in a new row-layout version.

### Varlen arena ownership

Each global partition owns its own varlen arena region. Task-local fragment
varlen regions are temporary and are copied/appended into the global partition's
varlen region during merge.

That means:

- task-local row varlen refs are local to the fragment until merge
- merge fixes varlen offsets as rows are appended to global partition storage
- finalized global row refs always point into the global partition arena

This is intentionally asymmetric: task-local refs are cheap to produce, and the
offset fixup cost is paid once during merge.

### Why not share task-local varlen blocks directly

Directly stitching task-local varlen blocks into finalized global storage would
complicate:

- reclamation
- partition compaction
- spill layout
- stable shared publication

Appending task-local varlen payload into one global partition-owned arena keeps
the published view simpler and more durable.

### Varlen append protocol

For each merged row with varlen fields:

1. copy task-local varlen bytes into global partition varlen arena
2. compute new offset relative to global partition varlen base
3. rewrite the row's varlen ref to use the new offset

This is the only row rewrite merge should perform in the common case.

## Row-layout as unified memory / shared / spill format

One of the main reasons to move away from `DataChunk`-owned partition state is
that a good row-layout can serve as a **single structural format** across:

- task-local in-memory build state
- shared finalized bridge state
- future spill / external partition files

### What this means in practice

If the row-layout contains only:

- fixed-layout headers
- counts
- offsets
- row blocks
- varlen arenas
- hash metadata blocks

and does **not** contain backend-local pointers, then writing it to disk is not
"serialize into another format". It is simply persisting the same structural
representation.

### What should disappear versus today

The current chunk-based path pays for:

1. building process-local objects
2. serializing them into a packed blob
3. reading the blob back
4. deserializing into new process-local objects

The row-layout target should remove steps 2 and 4 as heavyweight object
translation steps.

### What still remains

Even with a unified row-layout, consumers still need an **attach/view** step:

- read header
- interpret partition descriptors
- interpret row block descriptors
- establish arena base pointers for offset arithmetic

That is acceptable. This is cheap metadata interpretation, not `DataChunk`
reconstruction.

### Design rule

The row-layout should therefore be treated as:

- the in-memory mutable task-local format
- the shared read-only published format after finalize
- the spill file format later

with versioned metadata but without introducing a second logically different
bridge representation.

## Row ordinal and block addressing

The representation needs one canonical answer to: “how does probe locate build
row `r` inside partition `p`?”

### Canonical row identifier

The canonical identifier should be:

- `(partition_id, row_ordinal)`

not `(chunk_idx, row_idx)` and not a backend-local pointer.

### Why row ordinal is the right contract

- it is stable across processes
- it is compact enough to live in bucket / chain structures
- it composes naturally with row-block append
- it matches future spill addressing better than raw pointers

### Resolving row ordinal to bytes

Each partition should maintain row-block descriptors with cumulative row ranges.

Conceptually:

```cpp
struct ParallelHashRowBlockDesc {
    uint64_t block_offset;
    uint32_t start_row_ordinal;
    uint32_t row_count;
    uint32_t row_width;
    uint32_t reserved;
};
```

Probe-side resolution then becomes:

1. binary search or cursor search row block descriptor by `row_ordinal`
2. compute `in_block = row_ordinal - start_row_ordinal`
3. compute `row_ptr = block_offset + in_block * row_width`

### Fast-path option

If binary search on block descriptors becomes measurable, partitions may publish:

- one optional sparse ordinal index, or
- fixed-size blocks with arithmetic block lookup

But the logical contract should still remain `(partition_id, row_ordinal)`.

## Hash metadata layout choice

The design should explicitly separate **row storage** from **hash lookup
metadata**.

### Bucket + chain MVP

The safest first representation is:

- `bucket_heads[bucket] -> row_ordinal or INVALID`
- `chain_next[row_ordinal] -> next_row_ordinal or INVALID`

Why this is a good first step:

- easy to build after merge
- easy to debug
- naturally uses row ordinals
- avoids requiring fully open-addressed slot ownership during merge

### Linear-probe follow-up

If later performance work prefers linear probing, the row-layout contract still
holds. Only hash metadata changes.

That means row-layout storage and task-driven merge should not be blocked on a
final decision between chaining and linear probing.

## Scheduler state vs bridge state boundary

One of the design questions is what belongs in scheduler-owned shared state and
what belongs in the finalized bridge header.

### Scheduler-owned state

Scheduler-owned state should track mutable execution lifecycle:

- number of expected task fragments per partition
- number of merged fragments per partition
- merge owner / finalize owner
- partition state machine (`LOCAL_ONLY`, `MERGING`, `FINALIZING`, etc.)
- outstanding probe task count
- reclamation eligibility

This state is mutable and runtime-facing.

### Bridge-owned state

Bridge-owned state should track immutable or quasi-immutable published layout:

- row layout version
- row width
- partition descriptors for finalized storage
- row block descriptors
- varlen arena offsets and sizes
- bucket metadata offsets and sizes

This state is data-facing and should be safe for attach-by-view.

### Boundary rule

If a field answers “what data is published and how do I read it?”, it belongs in
the bridge.

If a field answers “who is allowed to mutate or reclaim it right now?”, it
belongs in scheduler-owned state.

This boundary is important to avoid repeating the current packed-blob design
where transport, publication, and lifecycle state are mixed together.

## Failure and recovery semantics

The first implementation should define conservative failure rules up front.

### Task failure during local build

If a build task fails before fragment publication:

- its task-local storage is discarded
- the partition state remains unchanged globally
- the scheduler may retry the task from source morsels

### Task failure during merge

If a merge task fails while appending a task-local fragment into a global
partition, the system must not publish a partially finalized partition as probe
ready.

The MVP should therefore require merge append to be either:

- append-only with a commit flag after success, or
- redo-safe by rebuilding the affected partition from surviving task-local
  fragments

The first option is preferable for simplicity.

### Finalize failure

If finalize of partition `p` fails:

- `probe_ready` must remain false
- partition `p` may be retried for finalize
- already merged row storage for `p` remains valid

This is another reason row storage and hash metadata should remain separate.

## Compatibility with current VecHashJoinState

The current `VecHashJoinState` can remain the plan-state facade, but the new path
should stop using its current chunk-centric fields as the published truth.

That implies:

- `partition_entries_[]` and `partition_chunks_[]` should not remain the core
  shared-bridge contract
- chunk-based shared bridge fields should become legacy/fallback-only for the
  new path
- probe helpers should learn to decode rows from shared row-layout metadata
  rather than first reconstructing `DataChunk`s

The important architectural change is that `VecHashJoinState` becomes a consumer
of shared row-layout metadata, not the owner of a rebuilt local chunk graph.

## Incremental implementation advice

To reduce risk, implementation should freeze the following order:

1. row layout builder and offset calculator
2. task-local row-store append path
3. task-local fragment metadata format
4. local-to-global merge with row ordinals and varlen fixup
5. bucket+chain finalize over row ordinals
6. probe-side row decoder

This order is recommended because each step exposes a stable contract that the
next step can consume without backtracking on the representation.

## Questions intentionally left open

These should be decided during implementation review, not left implicit:

1. whether row blocks are fixed byte size or growable append regions
2. whether bucket metadata MVP uses chaining or linear probing
3. whether sparse ordinal index is needed in the first patch
4. whether varlen offsets are 32-bit or 64-bit in v1
5. whether finalize is executed by a dedicated finalize task kind or by a merge
   continuation task

These are implementation choices. They should not change the higher-level
contracts frozen in this document.

## Proposed shared bridge metadata

The current shared types in `src/engine/core/hash_table_defs.hpp` should evolve
from chunk-centric to row-store-centric metadata.

### New shared header shape

The published bridge should describe:

- row layout version / magic
- partition count
- payload column metadata
- row width
- optional varlen layout metadata
- per-partition descriptors

### Per-partition descriptor

Each partition descriptor should contain:

- row count
- row block descriptor offset
- row block descriptor count
- varlen arena offset/size
- bucket head array offset/size
- optional next-pointer / chain array offset
- optional bloom filter offset/size
- build-complete / finalize-complete flags

Conceptually:

```cpp
struct ParallelHashBuildPartitionRows {
    uint64_t row_blocks_offset;
    uint64_t row_blocks_size;
    uint64_t varlen_offset;
    uint64_t varlen_size;
    uint64_t bucket_heads_offset;
    uint64_t bucket_heads_size;
    uint64_t chain_next_offset;
    uint64_t chain_next_size;
    uint32_t row_count;
    uint32_t bucket_count;
    uint32_t bucket_mask;
    uint32_t row_width;
};
```

The important change is not the exact field names. The important change is that
**published metadata names row blocks and row identifiers**, not `DataChunk`
objects.

### Row block descriptor

Each row block descriptor should contain:

- block payload offset
- number of rows in block
- row stride / row width

This lets a worker resolve row ordinal -> block -> byte address without local
reconstruction.

## Proposed runtime flow

## 1. Worker-local build

Each worker:

1. computes the build-row hash
2. chooses `partition_id = volvec_radix_partition_idx(hash)`
3. materializes one row-layout record
4. appends it to that worker's partition-local row store

No `DataChunk` object becomes part of the partition-owned representation.

Workers may still use `DataChunk` as an input batch format during scan/filter,
but the **exported build artifact** is row-layout storage.

## 2. Worker export

Worker export becomes explicitly partition-sliced.

Instead of one undifferentiated fragment blob, a worker publishes:

- one fragment header
- `VOLVEC_RADIX_FANOUT` partition descriptors
- row blocks already grouped by partition
- varlen blocks already grouped by partition

This removes the need for task-completion merge to inspect each row only to rediscover
its partition.

## 3. Task-completion merge into global partitions

When a build task completes, its local partition fragments are merged into the
global partition state by partition.

For each completed task fragment and each partition `p`:

- attach task-local partition descriptors
- append task-local row blocks into global partition `p`
- append task-local varlen storage into global partition `p`
- fix up partition-local row ordinals if needed

No `DataChunk` reconstruction.
No `chunk_idx` remap table.
No duplicate chunk ownership.

This does **not** require a dedicated leader-only merge phase. The important
contract is that completed task-local partitions are merged into a shared global
partition representation as tasks finish, subject to the scheduler's ownership
rules.

If a future adaptive radix step changes the partitioning depth, repartitioning
should operate on rows read from row blocks, not on chunk payloads.

## Global partition ownership and synchronization

The global partition state must be shared, but ownership of mutation must remain
simple and explicit.

### Ownership model

At any instant, each partition is in one of these states:

- `LOCAL_ONLY`: rows exist only in task-local fragments
- `MERGING`: one merge task is appending local fragments into the global
  partition state
- `MERGED`: all currently completed task-local fragments are reflected in the
  global partition row store
- `FINALIZING`: hash metadata is being built for that partition
- `PROBE_READY`: partition row store and bucket metadata are both immutable and
  visible to probe tasks
- `PROBING`: one or more probe tasks are consuming that partition
- `DONE`: probe work for that partition is complete

The key rule is: **row-store mutation and hash-metadata mutation are partition
scoped**. No task should need to mutate another partition while operating on
partition `p`.

### Merge ownership rule

For the MVP, exactly one task may merge into a given global partition at a time.

This can be implemented either as:

- one partition-scoped mutex/spinlock, or
- scheduler-level exclusive ownership of merge work for partition `p`

The second model is preferable because it fits the Step4 task-ownership design
better and avoids making row-block append a lock-heavy hot path.

### Publication rule

Probe tasks may attach partition `p` only after:

1. all required build tasks that feed `p` are complete
2. all local fragments for `p` have been merged into the global row store
3. finalize has published immutable bucket metadata for `p`

This publication should be tracked per partition, not through one pipeline-wide
ready flag.

## Task-local and global metadata

The current metadata in `ParallelHashBuildFragmentState` and
`ParallelHashBuildPartition` is too chunk-oriented for this design.

The new model should split metadata into two levels.

### 1. Task-local fragment metadata

Each completed build task exports a fragment header with:

- task id / producing worker id
- partition count
- one descriptor per partition
- offsets to row blocks and varlen blocks for that task fragment

Conceptually:

```cpp
struct ParallelHashBuildTaskFragment {
    uint32_t task_id;
    uint32_t num_partitions;
    uint64_t partitions_offset;
    uint64_t partitions_size;
};

struct ParallelHashBuildTaskPartition {
    uint64_t row_blocks_offset;
    uint64_t row_blocks_size;
    uint64_t varlen_offset;
    uint64_t varlen_size;
    uint32_t row_count;
    uint32_t reserved;
};
```

The important property is that a merge task can skip partitions with
`row_count == 0` and can attach the data for partition `p` directly, without
scanning unrelated task payload.

### 2. Global partition metadata

Each global partition should track:

- merged row count
- merged row blocks
- merged varlen storage
- number of expected producer tasks
- number of merged producer tasks
- finalize-ready flag
- probe-ready flag

Conceptually:

```cpp
struct ParallelHashGlobalPartitionState {
    uint64_t row_blocks_offset;
    uint64_t row_blocks_size;
    uint64_t varlen_offset;
    uint64_t varlen_size;
    uint64_t bucket_heads_offset;
    uint64_t bucket_heads_size;
    uint64_t chain_next_offset;
    uint64_t chain_next_size;
    uint32_t merged_row_count;
    uint32_t expected_fragments;
    uint32_t merged_fragments;
    uint8_t finalize_ready;
    uint8_t probe_ready;
    uint8_t reserved[2];
};
```

This metadata belongs in shared scheduler-owned state, not in transient local
`VecHashJoinState` objects only.

## Detailed merge protocol

The merge protocol should be explicit so ownership bugs do not reappear in a new
form.

### Merge steps for one `(task_fragment, partition p)` pair

1. Scheduler grants merge ownership for partition `p`.
2. Merge task reads the task-local partition descriptor.
3. If `row_count == 0`, mark the fragment as merged for `p` and return.
4. Append row blocks into global partition `p`.
5. Append varlen arena bytes into global partition `p`.
6. Fix any partition-local row-ordinal base for the appended rows.
7. Update `merged_row_count` and `merged_fragments`.
8. If `merged_fragments == expected_fragments`, mark partition `p` as eligible
   for finalize.
9. Release merge ownership for partition `p`.

The critical observation is that this protocol never reconstructs build rows as
`DataChunk`s and never needs a `chunk_idx` translation map.

### Merge granularity

The default unit should be **one task-fragment partition at a time**, not “merge
all partitions of the task under one global barrier”.

That enables:

- earlier partition readiness
- less skew amplification
- future repartition / spill handling at partition granularity

## Finalize ownership

Finalize should also be partition-scoped.

Once partition `p` is finalize-eligible, one finalize task for `p`:

1. reads global row blocks for `p`
2. builds bucket metadata over partition-local row ordinals
3. publishes immutable bucket metadata into shared state
4. sets `probe_ready` for partition `p`

This makes finalize a natural continuation of task-driven partition ownership,
not a pipeline-global phase transition.

## Reclamation model

The reclamation model should match the no-serialize qbridge design:

- task-local fragments can be reclaimed after all referenced partitions have been
  merged into global state
- global partition row storage can be reclaimed only after all probe tasks that
  depend on that partition are complete
- reclamation remains scheduler-owned, not worker-owned

This avoids the same ownership ambiguity that the chunk-based design ran into.

## Observability and debugging requirements

The new path should expose partition-level counters so we can verify the design
in real runs.

Minimum counters/logging:

- rows produced per task-local partition
- rows merged into each global partition
- number of task fragments merged per partition
- time spent in merge per partition
- time spent in finalize per partition
- first timestamp when each partition becomes `probe_ready`
- skew summary: min/max/avg rows across partitions

These counters are important for validating that task-completion merge actually
improves readiness behavior versus the current global-bridge model.

## 4. Finalize

After merge, finalize builds per-partition hash metadata over the row store.

Two acceptable MVP shapes:

1. **bucket heads + next chain array**
   - each build row gets a partition-local `next` index
   - bucket head points to the first row ordinal

2. **linear-probe metadata over row ordinals**
   - slots point to partition-local row ordinals

Either is acceptable, but both should reference rows by **partition-local row
ordinal**, never by chunk index.

## 5. Probe

Probe worker flow:

1. compute probe key hash
2. route to partition `p`
3. consult partition `p` bucket metadata
4. iterate candidate build row ordinals
5. load build row bytes directly from shared row blocks
6. evaluate equality keys / residual filter
7. materialize output columns into a normal output `DataChunk`

The output can remain `DataChunk`-based. Only the shared build intermediate
changes.

## Why this is better than the current DataChunk-based attempt

| Problem | Current DataChunk path | Proposed row-layout path |
|---|---|---|
| Partition ownership | chunk arrays may be shared/duplicated | each partition owns row blocks |
| Merge cost | rebuild chunks + remap `chunk_idx` | append row blocks |
| Shared bridge contract | chunk metadata + payload reconstruction | stable row/block offsets |
| Probe addressing | `(partition, chunk_idx, row_idx)` | `(partition, row_ordinal)` |
| Cleanup | chunk dedup / pointer ownership complexity | partition-owned block reclamation |
| Spill path | requires extra conversion | row blocks are already spill-friendly |

## Scheduler implications

This document does not replace the scheduler work described in
`step4_partition_owned_hash_join_design.md`; it gives that scheduler a better
intermediate representation.

The runtime should still move toward:

```text
HashBuildPartition(partition p)
  -> HashPartitionFinalize(partition p)
  -> HashProbePartition(partition p)
```

The row-layout bridge makes this practical because partition `p` is now a
first-class independently attachable object.

Required scheduler-facing consequences:

1. `partition_id` becomes explicit in task descriptors.
2. Worker export metadata becomes per-partition.
3. Readiness is tracked per partition, not just per pipeline.
4. Bridge publication can expose partition-local views without waiting for a
   chunk-rebuild pass.

## Three logical stages vs partition-scoped execution

Radix-partitioned hash join naturally decomposes into three **logical** stages:

1. build + partition
2. merge + finalize
3. probe

At the logical level, this really is a three-stage join pipeline.

### Stage 1: build + partition

- scan build side
- compute hash
- assign `partition_id`
- append row-layout records to task-local partition fragments

### Stage 2: merge + finalize

- as tasks complete, merge task-local fragments into global partition state
- once a partition has received all required fragments, build its immutable hash
  metadata

### Stage 3: probe

- scan probe side
- compute hash
- route each probe row to its partition
- probe finalized partition state

### But the runtime should not behave like three global barriers

The important distinction is between:

- **logical stages**, and
- **scheduler execution shape**

The target runtime should **not** require:

- all build work for all partitions to finish globally
- then all finalize work for all partitions to finish globally
- then all probe work to start globally

Instead, the scheduler should operate at partition granularity.

Conceptually:

```text
BuildPartition(p)
  -> MergePartition(p)
  -> FinalizePartition(p)
  -> ProbePartition(p)
```

This means partition `p=17` may become `probe_ready` while partition `p=203` is
still receiving build fragments.

### Why this matters

If the implementation falls back to three global barriers, it gives up much of
the benefit of radix partitioning:

- less overlap between build/finalize/probe
- worse skew behavior
- larger memory high-water marks
- less opportunity for partition-local reclaim or future spill

So the correct summary is:

- **yes**, radix hash join has three logical pipeline stages
- **no**, Step4 should not execute them as three coarse global pipeline fences

The row-layout representation is specifically chosen to make partition-scoped
execution practical.

## Migration plan

### Stage 0: freeze representation contract

Before more code churn, freeze:

- row-layout header format
- per-partition descriptor format
- row identifier semantics
- varlen storage contract

### Stage 1: worker-local row-store build path

Add a new worker-local build representation alongside the current chunk-based
path.

Scope:

- keep probe and output unchanged
- only replace partitioned build-side intermediate

### Stage 2: partition-sliced worker export and task-driven local-to-global merge

Replace blob-style append/rebuild for the new path with:

- partition descriptors
- row block append/concatenate
- varlen append

### Stage 3: finalize against row ordinals

Build partition-local bucket metadata over row storage.

At this stage the published shared bridge should no longer require
`ParallelHashBuildChunk` semantics for the new path.

### Stage 4: direct shared probe

Teach probe to read build payload from shared row blocks directly.

At this point the QueryScheduler path should no longer need to deserialize a
chunk-oriented bridge for the new representation.

### Stage 5: delete or demote the old path

Only after correctness and performance are stable should the current
chunk-rebuild path become fallback-only or be removed.

## Compatibility notes

For bring-up, both paths may coexist:

- legacy chunk-based serialized bridge
- new row-layout partitioned bridge

Bring-up gating should be conservative and explicit.

Suggested initial scope:

- inner joins first
- one fixed row-layout version
- fixed `VOLVEC_RADIX_FANOUT = 256`
- no spill in first patch

## Validation requirements

The design is not done until all of the following are true:

1. Q3 parallel path runs correctly with the new representation.
2. Task-completion local-to-global merge no longer rebuilds `DataChunk` objects for partitioned build
   fragments.
3. Shared bridge publication no longer depends on `chunk_idx` remapping.
4. Probe can resolve build matches through partition-local row ordinals.
5. Per-partition cleanup is ownership-simple and does not require chunk dedup.
6. The bridge format is a natural base for future partition spill.

## Summary

The current regression is not just a bug in merge logic. It is a signal that the
current intermediate representation is wrong for a radix-partitioned shared hash
bridge.

The Step4 runtime direction remains correct: partition ownership and
partition-scoped scheduling.

The missing piece is to make the build-side intermediate itself partition-owned
and row-layout based.

That means:

- stop treating `DataChunk` as the partition-owned unit
- store build rows in partition-owned row blocks with inline hash
- publish shared metadata over row blocks and row ordinals
- finalize and probe against row-layout storage directly

That is the direction most consistent with DuckDB's design and with the actual
pressure points currently visible in `pg_volvec`.

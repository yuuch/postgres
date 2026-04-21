# Step4 Design: Partition-Owned Parallel Hash Join And Partition-Task Scheduler

Status: design only, not implemented yet

## Goal

Complete the missing Step4 for `pg_volvec`'s parallel hash join path.

The concrete target is:

- partition-owned hash build/probe
- partition-task scheduling instead of pipeline-global bridge handoff
- partition-aware shared bridge metadata
- a layout that can later support partition-wise spill / external hash join

This document is intentionally narrower than the full parallel runtime design.
It focuses on the gap between the current QueryScheduler implementation and the
actual Step4 behavior we still do not have.

## Why this document exists

We already moved the QueryScheduler hash bridge away from the old
serialize-to-file / deserialize-per-worker path and toward a shared finalized
read view. That helped, but it did **not** complete Step4.

The remaining slowdown is not explained by deserialize cost alone.
The current runtime still pays for:

- pipeline-global build/finalize/probe synchronization
- a single global finalized bridge published after all build work finishes
- probe execution that is still logically global, not partition-owned
- bridge pack assembly / dependency stitching at pipeline granularity
- no partition-local backpressure, no partition-local completion, and no
  partition-local spill contract

DuckDB-style performance on this class of plan comes from partition ownership,
not only from storing the finalized hash table in shared memory.

## Current state in the code

The current code already contains useful scaffolding, but it is still a bridge-
centric design.

### 1. Shared finalized hash bridge exists, but it is still effectively single-partition

In `src/engine/exec/hash_join_parallel.cpp`:

- `VecHashJoinState::write_shared_hash_bridge()` sets
  `state->num_partitions = 1`
- `VecHashJoinState::materialize_shared_hash_build_view()` sets
  `use_parallel_ht_ = false`

That means the published shared layout is a shared finalized view of one global
build result, not a true partitioned parallel hash table.

### 2. Runtime state is pipeline-global, not partition-global

In `src/engine/parallel/parallel_runtime_internal.hpp`, these fields are stored
once per pipeline today:

- `ParallelQueryPipelineShared::remaining_dependencies`
- `completed_tasks`
- `published_workers`
- `completed`
- `hash_bridge_ready`
- `hash_bridge_size`
- `hash_bridge_entries`
- `hash_bridge_chunks`
- `hash_bridge_dsa_pack`
- `hash_bridge_file_name`

That model can represent "pipeline finished" and "pipeline published one bridge"
but cannot represent "partition 37 is ready for probe while partition 92 is
still building".

### 3. QueryScheduler dependencies are still assembled as bridge packs

In `src/engine/parallel/runtime_worker_main.cpp`, the current worker flow still
depends on:

- `ReadQueryBridgePack(...)`
- `BuildQueryDependencyBridgePack(...)`
- `PublishQueryHashBridgePack(...)`
- `FinalizeQueryHashBuildPipeline(...)`
- `CompleteQueryPipelineAndReleaseSuccessors(...)`

This is still a pipeline-global dependency model:

1. workers build local fragments
2. a finalize step merges them
3. one packed bridge is published
4. successor pipelines attach that pack

That is not the same thing as scheduling `HashBuildPartition[p]` and
`HashProbePartition[p]` as independent runtime tasks.

### 4. `VecHashJoinState` already has partition-related fields, but they are not yet driving scheduler ownership

In `src/engine/exec/hash_join.hpp`, the following fields already exist:

- `assigned_partition_start_`
- `assigned_partition_end_`
- `partition_bucket_heads_[]`
- `partition_bucket_masks_[]`
- `shared_hash_partitions_`
- `shared_hash_partition_count_`
- `probe_partition_ids_`
- `probe_partition_offsets_`
- `probe_partition_order_`
- `probe_partition_cursor_`

These are useful building blocks, but the runtime does not yet hand workers a
partition-owned task contract. The scheduler still hands out block morsels or a
single finalize task.

## The real Step4 target

Step4 is complete only when all of the following are true:

1. The scheduler can create tasks scoped to a specific hash partition.
2. Build ownership is explicit per partition.
3. Probe ownership is explicit per partition.
4. Partition readiness is tracked independently.
5. A probe task can start for partition `p` without waiting for every other
   partition to be globally finalized.
6. The shared bridge format can describe multiple partitions as first-class
   independently consumable objects.
7. The same ownership model can later route oversized partitions to spill /
   external processing.

If any of these is missing, we still have a shared global bridge design, not a
partition-owned Step4 design.

## Non-goals

This document does **not** attempt to do the following in the first Step4 MVP:

- redesign PostgreSQL planning
- add true generic external hash join in the first patch
- add skew handling in the first patch
- support every join family up front
- replace all current bridge kinds with partition-aware variants at once
- parallelize final output materialization

The first implementation should focus on inner hash join shapes that already
work correctly today.

## Design principles

1. **Partition is the unit of ownership.**
   The runtime must stop treating the whole build side as one publishable unit.

2. **Scheduler metadata must become partition-addressable.**
   "Pipeline ready" is too coarse for the missing Step4 behavior.

3. **Shared bridge metadata must be stable before spill is added.**
   If we do not freeze the partition contract first, the external path will be
   reworked later.

4. **Do not require a global repack for each consumer.**
   Consumers should attach partition metadata, not rebuild dependency blobs.

5. **Keep MVP correctness-first.**
   Partition ownership must be clear even if the first partition scheduler is
   conservative.

## Required contracts to freeze first

Before code changes, we should explicitly freeze these contracts.

### Contract 1: partition identity

Every build/probe task that participates in Step4 must carry a stable
`partition_id` in `[0, partition_count)`.

This id must mean the same thing in:

- scheduler task metadata
- worker-local hash join state
- published shared bridge metadata
- future spill files / external partition artifacts

### Contract 2: partition ownership

At any moment, a partition may be in one of these states:

- not started
- building
- build-complete / probe-not-ready
- probe-ready
- probing
- fully consumed
- spilled / external

Ownership must be explicit. No task should infer partition readiness from
global pipeline completion counters.

### Contract 3: per-partition bridge publication

The runtime must be able to publish partition metadata independently from other
partitions.

That does **not** necessarily mean we allocate one separate DSA object per
partition in the MVP, but it does mean the published representation must let a
consumer identify and attach partition `p` without treating the whole bridge as
one indivisible artifact.

### Contract 4: per-partition completion / backpressure

The scheduler must be able to know:

- how many build tasks still feed partition `p`
- whether partition `p` is finalized
- whether probe task(s) for partition `p` may run
- whether partition `p` is already fully consumed

Without this, we cannot later add partition-wise spill or selective retries.

## Target runtime model

The runtime should move from this:

```text
HashBuildSource(many block morsels)
  -> HashBuildFinalize(one pipeline-wide task)
  -> HashProbeSource(many block morsels with one global bridge)
```

to this:

```text
HashBuildPartition(partition p, build morsel subset)
  -> HashPartitionFinalize(partition p)
  -> HashProbePartition(partition p, probe morsel subset)
```

The exact scheduler can still be process-based and conservative, but the task
model has to become partition-aware.

## Proposed scheduler changes

### 1. Extend task descriptors with `partition_id`

Files:

- `src/engine/parallel/parallel_runtime.hpp`
- `src/engine/parallel/parallel_runtime_internal.hpp`

Add `partition_id` to:

- `ParallelTaskDesc`
- `ParallelQueryTaskShared`

Use `UINT32_MAX` to mean "not partition-scoped" for non-hash tasks.

Why:

- this is the minimum scheduler-level contract change
- it lets workers know which partition view they are expected to build or probe
- it avoids overloading `pipeline_id` with mixed responsibilities

### 2. Add partition-scoped task kinds

Current `ParallelTaskKind` only has:

- `SourceMorsel`
- `BridgeFinalize`

That is too coarse. The scheduler should introduce explicit kinds for hash
partition work, for example:

- `HashBuildPartition`
- `HashPartitionFinalize`
- `HashProbePartition`

The exact enum names can differ, but the semantic split is required.

Why:

- a partition task has different completion semantics than a normal source
  morsel
- probe tasks should not pretend to be generic source tasks once they depend on
  partition readiness

### 3. Move key hash-pipeline progress out of pipeline-global counters

Files:

- `src/engine/parallel/parallel_runtime_internal.hpp`
- `src/engine/parallel/runtime_execution.cpp`
- `src/engine/parallel/runtime_worker_main.cpp`

Add a new per-partition shared state object, conceptually:

```cpp
struct ParallelHashPartitionShared {
  uint32 partition_id;
  pg_atomic_uint32 remaining_build_tasks;
  pg_atomic_uint32 build_finalized;
  pg_atomic_uint32 probe_started;
  pg_atomic_uint32 probe_completed;
  uint64 bridge_entries;
  uint64 bridge_chunks;
  uint64 bridge_dsa_offset;
  uint64 bridge_bytes;
  uint8 spilled;
};
```

This does not need to replace `ParallelQueryPipelineShared`; it supplements it.

Why:

- pipeline-level `remaining_dependencies` says nothing about individual
  partition readiness
- this is the minimal structure needed for partition-local scheduling and later
  partition-local spill

### 4. Keep `ParallelQueryPipelineShared`, but stop storing partition-varying bridge state there

The following fields should no longer be the only source of truth for hash
build completion:

- `hash_bridge_ready`
- `hash_bridge_size`
- `hash_bridge_entries`
- `hash_bridge_chunks`
- `hash_bridge_dsa_pack`
- `hash_bridge_file_name`

Instead:

- keep pipeline-global fields only for coarse metadata
- move partition-varying state to a per-partition array

Why:

- a pipeline-global `hash_bridge_ready=1` forces "all partitions or nothing"
- Step4 needs partition-local readiness

## Proposed shared bridge format changes

### 1. Make `ParallelHashBuildState` truly multi-partition

File:

- `src/engine/core/hash_table_defs.hpp`

The current structure already has:

- `num_partitions`
- `partitions_offset`
- `partitions_size`

but the writer currently emits only one partition.

The new contract should be:

- `num_partitions = actual partition count`
- each `ParallelHashBuildPartition` describes a single independently readable
  partition
- the top-level state remains the container header
- consumers can attach either the full state or a partition slice

### 2. Enrich `ParallelHashBuildPartition`

Current fields are enough for a minimal internal view, but not enough for
future spill routing or ownership checks.

Add fields such as:

- `partition_id`
- `flags` (`in_memory`, `external`, `ready`, `empty`)
- `chunk_start_idx` / `chunk_count`
- `entry_start_idx` / `entry_count`
- optional spill metadata handle / filename offset

Why:

- today we can index entries and buckets
- tomorrow we need to answer "is this partition in memory or external?" without
  inventing a second incompatible metadata path

### 3. Keep one top-level bridge allocation in MVP, but partition it logically

For the MVP, it is acceptable to publish one large DSA allocation that contains
all partitions, as long as:

- `num_partitions > 1`
- each partition is explicitly described
- scheduler logic reasons per partition

Why this is acceptable for MVP:

- it avoids multiplying allocation-lifetime complexity immediately
- it still lets us build partition-task scheduling first
- it is sufficient to remove the false "single global finalized object" model

Why this is not enough long-term:

- very large partitions will still want independent spill or reclamation
- skewed partition lifetimes will still suffer from one giant allocation

## Proposed `VecHashJoinState` changes

Files:

- `src/engine/exec/hash_join.hpp`
- `src/engine/exec/hash_join.cpp`
- `src/engine/exec/hash_join_lookup.cpp`
- `src/engine/exec/hash_join_parallel.cpp`

### 1. Turn `assigned_partition_start_` / `assigned_partition_end_` into the active execution contract

These fields already exist. They should become the primary way a worker limits
hash build and probe work for a task.

For build tasks:

- worker consumes input
- rows are partitioned by radix bits
- only owned partition(s) are inserted/finalized for the current task

For probe tasks:

- worker orders probe candidates by `probe_partition_order_`
- worker only probes partitions assigned to the current task

### 2. Make `use_parallel_ht_` meaningful for shared partitioned probe

Today `materialize_shared_hash_build_view()` forces:

- `use_parallel_ht_ = false`

That means the partition/linear-probe path is not actually activated for the
shared finalized view.

For Step4, shared finalized partition metadata must be enough to allow:

- per-partition bucket head selection
- per-partition entry range selection
- optional partition-local linear-probe table build or attach

There are two reasonable MVP choices:

1. **Attach shared chaining metadata and probe partition-locally first**
2. **Build partition-local linear-probe side structures from shared partition
   metadata on demand**

Recommendation for MVP:

- keep the published shared bridge as the source of truth
- allow workers to derive local fast probe state per assigned partition if that
  is cheaper than publishing fully shared linear-probe arrays immediately

Why:

- lower implementation risk
- preserves a stable shared bridge contract
- keeps the optimization boundary local to workers

### 3. Build-side export should become partition-aware

Current worker export paths:

- `export_parallel_build_partial_file(...)`
- `export_parallel_build_partial_dsa(...)`

currently describe one worker-local fragment, not partition ownership.

For Step4, export metadata should either:

- export per-partition fragment ranges, or
- export a worker fragment that already contains partition tables and counts

Recommendation:

- keep one worker partial object, but add partition counts / offsets inside it

Why:

- easier migration from current worker partial export path
- enough information for partition-finalize tasks to merge only one partition
  at a time

### 4. Probe scheduling should consume partition order, not just row order

`probe_partition_ids_`, `probe_partition_offsets_`, `probe_partition_order_`,
and `probe_partition_cursor_` already suggest the correct direction.

Step4 should formalize this:

- partition probe tasks iterate partitions first
- within a partition, process all candidate rows assigned to that partition
- output remains chunk-based, but ownership is partition-scoped

Why:

- improves locality
- aligns with partition-owned spill later
- removes the current mismatch between partition-aware probe helpers and
  pipeline-global scheduler semantics

## Proposed QueryScheduler flow after Step4 MVP

### Build phase

1. Scheduler creates source morsel tasks for build input.
2. Each worker partitions rows into local per-partition fragments.
3. Worker publishes fragment metadata with partition counts/offsets.
4. Scheduler decrements `remaining_build_tasks[partition_id]` as contributing
   build morsels finish.
5. When a partition's contributing build work is complete, scheduler releases a
   `HashPartitionFinalize(partition_id)` task.

### Partition finalize phase

1. Finalize task for partition `p` merges only partition `p` from all worker
   partials.
2. It publishes shared metadata for partition `p`.
3. It marks partition `p` as probe-ready.
4. Scheduler may now release probe tasks for partition `p`.

### Probe phase

1. Probe tasks are tagged with `partition_id`.
2. Worker scans or consumes probe morsels and routes rows to partition `p`.
3. Worker probes only partition `p`'s shared view.
4. Aggregate / downstream output remains unchanged from a semantic standpoint.

## Concrete file-by-file changes

### `src/engine/parallel/parallel_runtime.hpp`

Change:

- extend `ParallelTaskDesc` with `partition_id`
- extend `ParallelTaskKind` usage to include partition-specific hash tasks

Why:

- public runtime plan/task layer must describe partition work explicitly

### `src/engine/parallel/parallel_runtime_internal.hpp`

Change:

- add new shared structs for hash partition runtime state
- add `partition_id` to `ParallelQueryTaskShared`
- likely add `partition_count` to `ParallelAggregateSharedControl`

Why:

- worker main and runtime execution both need the same DSM-visible contract

### `src/engine/parallel/runtime_execution.cpp`

Change:

- stop generating only pipeline-global hash finalize tasks
- allocate and initialize per-partition shared state
- enqueue partition-finalize and partition-probe tasks

Why:

- this file currently materializes the scheduler's task table and is the right
  place to change the task topology

### `src/engine/parallel/runtime_worker_main.cpp`

Change:

- stop depending on `BuildQueryDependencyBridgePack(...)` as the normal hash
  dependency path
- execute partition-finalize tasks by merging a single partition
- execute partition-probe tasks by attaching one partition's bridge metadata
- keep pipeline-global bridge pack path only as a compatibility fallback while
  bringing Step4 up

Why:

- this is where workers currently convert scheduler metadata into actual local
  execution work

### `src/engine/parallel/runtime_worker_state.cpp`

Change:

- extend local initialization so a worker can attach a partition-scoped bridge
  or partition metadata view

Why:

- worker initialization currently assumes a pipeline-global dependency bridge

### `src/engine/exec/hash_join_parallel.cpp`

Change:

- emit `num_partitions > 1`
- publish real `ParallelHashBuildPartition[]`
- stop writing only a single global partition
- wire partition metadata into shared view attach

Why:

- this file owns the shared hash bridge representation today

### `src/engine/exec/hash_join.cpp` and `hash_join_lookup.cpp`

Change:

- use assigned partition bounds as execution constraints
- make probe iteration partition-driven
- optionally build per-partition local fast-probe tables

Why:

- shared metadata only matters if the probe path actually consumes it with
  partition-locality

### `src/engine/core/hash_table_defs.hpp`

Change:

- extend shared bridge metadata structures with partition-first semantics

Why:

- this file defines the stable shared layout contract

### `src/engine/hash_table.hpp`

Change:

- likely no major structural rewrite in MVP
- may need helpers for per-partition local linear-probe derivation

Why:

- keep low-level HT helpers reusable; avoid coupling them directly to scheduler
  state

### `src/engine/exec/plan_state.hpp`

Change:

- extend task / pipeline enums if needed

Why:

- partition task kinds are part of the shared execution vocabulary

## Recommended MVP scope

### MVP includes

- explicit `partition_id` in tasks
- per-partition shared scheduler state
- real multi-partition shared hash bridge metadata
- partition-finalize task execution
- partition-probe task execution
- probe-ready state per partition
- correctness validation on current supported inner-join shapes

### MVP excludes

- true external / spill execution
- skew splitting / repartitioning
- fine-grained early reclamation
- shared published linear-probe arrays for every partition

## Spill / external design hook

Step4 must leave a clean extension point for external partitions.

The minimum future-proof contract is:

- each `ParallelHashBuildPartition` can be marked `in_memory` or `external`
- scheduler can tell whether a probe task should attach memory metadata or an
  external artifact
- probe code can branch on partition storage kind without changing partition id
  semantics

That is why partition metadata must be first-class now, even if the first patch
keeps all partitions in memory.

## Failure modes to design for

### 1. Partition skew

One partition may dominate rows.

Initial behavior:

- allow large partitions to be slow
- expose counters so skew is visible
- do not hide the problem with ad hoc global fallback logic

### 2. Empty partitions

Many partitions will be empty.

Required behavior:

- scheduler must be able to mark them ready-and-complete cheaply
- no unnecessary finalize/probe work for empty partitions

### 3. Partition metadata mismatch

Worker partials may disagree on payload metadata.

Required behavior:

- keep current strict validation behavior
- fail early during partition finalize, not later during probe

### 4. Oversized in-memory partition

Even before true spill exists, the runtime must detect this cleanly.

Required behavior:

- fail with a clear reason or route to a temporary conservative fallback
- do not silently repack into a global structure that breaks the partition
  contract

## Observability required for bring-up

Add trace logging and counters for:

- partition count
- per-partition build rows
- per-partition entry count
- per-partition finalize time
- per-partition probe time
- number of empty partitions
- largest partition size
- number of probe tasks released before all partitions globally finish

These counters matter because Step4 correctness alone is not enough; we need to
prove the scheduler is actually running partition-owned work rather than still
serializing behind a hidden global barrier.

## Recommended implementation order

### Phase 1: scheduler metadata

1. add `partition_id` to task descriptors
2. add per-partition shared runtime state
3. keep existing global path compiling behind temporary compatibility logic

Success criterion:

- scheduler can represent partition-scoped hash tasks even if execution is not
  switched over yet

### Phase 2: shared bridge format

1. make `ParallelHashBuildState` publish real multiple partitions
2. enrich `ParallelHashBuildPartition`
3. attach partition metadata in `VecHashJoinState`

Success criterion:

- shared finalized bridge reports real partition count and valid partition
  slices

### Phase 3: partition finalize

1. export worker partials with partition information
2. implement `HashPartitionFinalize(partition_id)`
3. publish per-partition readiness

Success criterion:

- a single partition can be finalized independently

### Phase 4: partition probe

1. implement partition-scoped probe tasks
2. drive probe using `assigned_partition_start_/end_`
3. use partition-aware candidate ordering

Success criterion:

- probe task runs only against its assigned partition(s)

### Phase 5: validation and cleanup

1. validate Q9 and other current hash-heavy supported queries
2. compare traces against current global-bridge path
3. delete compatibility-only bridge-pack assembly where no longer needed

Success criterion:

- no normal QueryScheduler hash path depends on global dependency bridge pack
  assembly anymore

## What not to do

1. **Do not declare Step4 done when `num_partitions` is still 1.**
2. **Do not keep `hash_bridge_ready` only at pipeline scope and call it
   partition scheduling.**
3. **Do not add spill first before freezing partition ownership.**
4. **Do not make probe "partition-aware" only inside local helper code while the
   scheduler remains global.**
5. **Do not replace one global bridge blob with many opaque blobs without a
   stable partition metadata contract.**

## Definition of done for Step4

Step4 is done only when all of the following are true in code and runtime
behavior:

- shared hash bridge publishes real multiple partitions
- scheduler tasks carry explicit partition ownership
- partition readiness is tracked independently
- partition finalize is independent from global pipeline completion
- probe tasks consume partition-scoped bridge metadata
- QueryScheduler no longer needs to rebuild dependency bridge packs as the
  normal hash path
- the design naturally extends to partition-wise spill

Anything less is still a useful intermediate step, but it is not the actual
partition-owned Step4.

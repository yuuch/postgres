# QueryScheduler qbridge: remove serialization/deserialization

## Problem

The current QueryScheduler shared-hash-bridge path no longer writes qbridge
artifacts to files for the common case, but it still uses a serialized-blob
representation:

- finalize publishes a packed byte buffer via `publish_hash_bridge()`
- downstream workers attach the shared byte buffer
- `load_hash_bridge()` deserializes that buffer back into worker-local state

This means the dominant remaining cost is no longer file I/O or dependency-pack
rebuild. It is the repeated per-worker deserialize/rebuild cost for large
420-437MB qbridge payloads.

## What changed already

- qbridge transport moved from `SharedFileSet` to DSA-backed storage for the
  common case
- single-predecessor DSA paths now borrow the shared pack directly instead of
  rebuilding a dependency pack

That removed disk I/O and extra pack-copy overhead, but not bridge
serialization itself.

## Key observation

The current `VecHashJoinState` shared-bridge API is still byte-oriented:

- `publish_hash_bridge()` allocates `shared_hash_bridge_buffer_` and calls
  `serialize_hash_bridge(...)`
- `load_hash_bridge()` calls `deserialize_hash_bridge(...)`

So the current DSA path is only a different storage medium for serialized
bridge bytes. It is not a true shared in-memory hash bridge view.

## Why this cannot be solved by transport changes alone

The current in-memory hash-build representation is process-local and pointer-
heavy:

- `inner_chunks_` is a vector of heap-allocated `DataChunk*`
- `bucket_heads_` and `entries_` live in backend-local `palloc` memory
- `VecLinearProbeHT` and partition tables also allocate process-local arrays

Because those structures are full of raw local pointers, they cannot simply be
placed in DSA and shared across workers as-is.

## New target model

Replace the serialized qbridge blob with a shared, pointer-stable bridge layout
allocated directly in DSA.

The bridge should be publish-once / consume-many:

1. hash-build finalize allocates a read-only shared bridge in DSA
2. `ParallelQueryPipelineShared` records the shared handle + metadata
3. downstream workers attach the shared bridge as a view
4. workers probe it directly without rebuilding worker-local hash state
5. reclamation stays centralized and conservative

## Minimal design constraints

### 1. No worker-owned free

This bridge is shared across downstream consumers. Reclamation must remain
centralized in scheduler/finalize logic, not arbitrary workers.

### 2. Shared representation must avoid raw local pointers

The shared bridge must only contain:

- offsets
- counts
- fixed-layout metadata
- DSA-resident arrays / chunks

No backend-local `palloc` pointers may be embedded in the published bridge.

### 3. First implementation targets QueryScheduler qbridge only

Do not refactor all parallel hash-build paths at once. Keep scope limited to
the QueryScheduler finalized bridge that is currently published as a packed
blob.

## Planned representation change

Introduce a DSA-native shared bridge state for the finalized bridge, separate
from the existing serialized blob path.

High level shape:

- shared bridge header in DSA
  - payload column metadata
  - entry count
  - chunk count
  - partition/bucket metadata
  - DSA pointers (or offsets) to entry arrays / chunk storage / bucket heads
- chunk payload storage in DSA
- entry array in DSA
- bucket-head arrays in DSA

Workers then attach this shared header and set their runtime view directly,
without `serialize_hash_bridge()` / `deserialize_hash_bridge()`.

## Staged implementation plan

### Stage 1

Add a separate QueryScheduler shared-bridge publish/attach path that bypasses
serialization entirely for finalized qbridges.

- keep existing serialized path available as fallback during bring-up
- only switch QueryScheduler finalized bridge consumers first

### Stage 2

Teach `VecHashJoinState` to probe against a shared external bridge view.

This means the probe path must tolerate external/shared arrays instead of
assuming all structures were rebuilt into local `entries_` / `inner_chunks_`.

### Stage 3

After correctness and performance validation, decide whether the older packed
serialized bridge path should remain as fallback or be deleted.

## Reclamation follow-up

Do not solve aggressive reclamation in the first no-serialize patch.

Follow-up policy:

- once all direct DAG dependents of a pipeline have completed, reclaim its
  shared qbridge blob
- keep reclamation centralized in scheduler/finalize code

## Non-goal

This work is not just “replace file with memory.” That part is already done.
The goal now is to remove the remaining bridge serialize/deserialize cycle by
making the finalized qbridge itself a DSA-native shared structure.

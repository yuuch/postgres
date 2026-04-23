# Global Partition Data Design for 3-Pipeline Radix Hash Join

## Overview

Worker-driven append model: workers flush local partition buffers directly to global DSA storage, no leader involvement in data merge. Per-partition lightweight concurrency control.

## Architecture

```
Worker processes morsel:
  ↓
256 local partition buffers (8KB each)
  ↓ flush when full
Global Partition Storage (DSA)
  ├── Partition[0] → Segment array
  ├── Partition[1] → Segment array
  ...
  └── Partition[255] → Segment array
  ↓
HashJoinPartitionSource reads segments
```

## Data Structures

### Global (in DSA)

```cpp
// Each segment stores rows from one worker's flush
struct PartitionSegment {
    uint32 row_count;
    uint32 row_width;
    uint32 data_size;    // row data bytes
    uint32 varlen_size;  // varlen bytes
    // Followed by:
    // - row data (row_count * row_width bytes)
    // - varlen data (varlen_size bytes)
};

#define VOLVEC_MAX_SEGMENTS_PER_PARTITION 4096

// Per-partition global storage
struct GlobalPartition {
    pg_atomic_uint32 segment_count;  // Current segment count
    slock_t lock;                     // Spinlock for append
    dsa_pointer segments[VOLVEC_MAX_SEGMENTS_PER_PARTITION];
    pg_atomic_uint32 total_rows;     // Total rows (stats)
};

// Global header (256 partitions)
struct GlobalPartitionedDataHeader {
    uint32 magic;                    // 0x56564750 'VVGP'
    uint32 version;                  // 1
    uint16 partition_count;          // 256
    uint16 reserved;
    GlobalPartition partitions[VOLVEC_RADIX_FANOUT];
};
```

### Worker Local

```cpp
// Worker-local buffer (not in DSA)
struct LocalPartitionBuffer {
    VolVecVector<uint8_t> row_bytes;
    VolVecVector<char> varlen_bytes;
    uint32 row_count;
    uint32 row_width;
    
    LocalPartitionBuffer(MemoryContext ctx)
        : row_bytes(PgMemoryContextAllocator<uint8_t>(ctx)),
          varlen_bytes(PgMemoryContextAllocator<char>(ctx)),
          row_count(0), row_width(0)
    {}
};

LocalPartitionBuffer local_partitions[256];
```

## Workflow

### 1. Leader Initialization (runtime_execution.cpp)

```cpp
// Allocate global partition data in DSA
dsa_pointer global_ptr = dsa_allocate(dsa, sizeof(GlobalPartitionedDataHeader));
GlobalPartitionedDataHeader* global = 
    (GlobalPartitionedDataHeader*)dsa_get_address(dsa, global_ptr);

memset(global, 0, sizeof(*global));
global->magic = 0x56564750;
global->version = 1;
global->partition_count = VOLVEC_RADIX_FANOUT;

for (int i = 0; i < VOLVEC_RADIX_FANOUT; i++) {
    pg_atomic_init_u32(&global->partitions[i].segment_count, 0);
    pg_atomic_init_u32(&global->partitions[i].total_rows, 0);
    SpinLockInit(&global->partitions[i].lock);
    memset(global->partitions[i].segments, 0, 
           sizeof(global->partitions[i].segments));
}

// Publish to pipeline shared state
pipeline->global_partition_data_offset = global_ptr;
```

### 2. Worker Morsel Processing (runtime_worker_main.cpp)

```cpp
// Attach to global partition data
GlobalPartitionedDataHeader* global = 
    (GlobalPartitionedDataHeader*)dsa_get_address(dsa, 
        pipeline->global_partition_data_offset);

LocalPartitionBuffer local_buffers[256];

// Process morsel
while (source->get_next_batch(chunk)) {
    for (int row = 0; row < chunk.count; row++) {
        uint32 hash = compute_hash(chunk, row);
        uint16_t partition_id = hash & 0xFF;
        
        // Append to local buffer
        serialize_row_to_buffer(local_buffers[partition_id], chunk, row);
        
        // Flush if buffer full (64KB threshold)
        if (local_buffers[partition_id].row_bytes.size() >= 65536) {
            flush_partition_to_global(partition_id, global, dsa, local_buffers);
        }
    }
}

// Morsel done - flush all remaining
for (uint16_t i = 0; i < 256; i++) {
    flush_partition_to_global(i, global, dsa, local_buffers);
}
```

### 3. Flush Logic (append to global)

```cpp
void flush_partition_to_global(uint16_t partition_id, 
                               GlobalPartitionedDataHeader* global,
                               dsa_area* dsa,
                               LocalPartitionBuffer* local_buffers)
{
    LocalPartitionBuffer& local = local_buffers[partition_id];
    if (local.row_count == 0)
        return;
    
    GlobalPartition& gpart = global->partitions[partition_id];
    
    // 1. Allocate segment in DSA
    size_t segment_size = sizeof(PartitionSegment) 
                        + local.row_bytes.size() 
                        + local.varlen_bytes.size();
    dsa_pointer seg_ptr = dsa_allocate(dsa, segment_size);
    PartitionSegment* seg = (PartitionSegment*)dsa_get_address(dsa, seg_ptr);
    
    // 2. Fill segment data
    seg->row_count = local.row_count;
    seg->row_width = local.row_width;
    seg->data_size = local.row_bytes.size();
    seg->varlen_size = local.varlen_bytes.size();
    
    uint8_t* data_ptr = (uint8_t*)(seg + 1);
    memcpy(data_ptr, local.row_bytes.data(), local.row_bytes.size());
    memcpy(data_ptr + local.row_bytes.size(), 
           local.varlen_bytes.data(), 
           local.varlen_bytes.size());
    
    // 3. Append to global partition (with lock)
    SpinLockAcquire(&gpart.lock);
    
    uint32 idx = pg_atomic_read_u32(&gpart.segment_count);
    if (idx >= VOLVEC_MAX_SEGMENTS_PER_PARTITION)
        elog(ERROR, "pg_volvec: partition segment overflow (partition=%u)", partition_id);
    
    gpart.segments[idx] = seg_ptr;
    pg_atomic_write_u32(&gpart.segment_count, idx + 1);
    pg_atomic_add_fetch_u32(&gpart.total_rows, local.row_count);
    
    SpinLockRelease(&gpart.lock);
    
    // 4. Clear local buffer
    local.row_bytes.clear();
    local.varlen_bytes.clear();
    local.row_count = 0;
}
```

### 4. HashJoinPartitionSource Read (hash_join_partition_source.cpp)

```cpp
// Read all segments for assigned partition
void read_partition_segments(uint16_t partition_id, 
                            GlobalPartitionedDataHeader* global,
                            dsa_area* dsa)
{
    GlobalPartition& gpart = global->partitions[partition_id];
    uint32 seg_count = pg_atomic_read_u32(&gpart.segment_count);
    
    for (uint32 i = 0; i < seg_count; i++) {
        dsa_pointer seg_ptr = gpart.segments[i];
        PartitionSegment* seg = (PartitionSegment*)dsa_get_address(dsa, seg_ptr);
        
        uint8_t* row_data = (uint8_t*)(seg + 1);
        char* varlen_data = (char*)(row_data + seg->data_size);
        
        // Process rows
        for (uint32 row = 0; row < seg->row_count; row++) {
            uint8_t* row_ptr = row_data + row * seg->row_width;
            // Build hash table or probe
        }
    }
}
```

## Concurrency Model

### Per-Partition Spinlock

- 256 independent locks (one per partition)
- Lock granularity: single segment append operation
- Low contention: multiple workers append to different partitions in parallel
- Skew handling: even if all data goes to partition 0, lock is only held during append (~microseconds)

### Lock-Free Reads

- HashJoinPartitionSource reads `segment_count` atomically
- Segments are immutable after creation
- No locks needed for reading

### Memory Ordering

- `segment_count` increment after `segments[idx]` assignment ensures readers see complete segment
- `total_rows` atomic add for stats (no ordering requirement)

## Performance Analysis

### Concurrency

**Assumption**: 4 workers, 100 morsels each, flush every 64KB

- Total flushes: 4 × 100 × 256 = 102,400
- Per-lock operations: 102,400 / 256 = 400
- **Low contention** - partitions distribute load

### DSA Allocation

**Example**: Q5 build side 6M rows, 1K rows per segment

- Total segments: 6,000
- Distributed across workers and morsels
- DSA allocation is coarse-grained (~64KB per call)
- **Acceptable overhead**

### Read Performance

**Example**: Partition 0 has 24 segments (4 workers × 6 segments each)

- 24 × `dsa_get_address()` calls
- Sequential memory access within each segment
- Slightly slower than single large block, but **controlled overhead**

Important: segment iteration is the unit of locality. A partition may contain
many segments, but every segment is a contiguous `[rows][varlen]` block. The
join source should process one segment at a time instead of copying all segment
data into a second large partition buffer.

## Correctness Invariants

### 1. Segment Immutability

Once a worker appends a segment pointer to `GlobalPartition.segments[]`, that
segment must never be modified. Readers can safely access segment data without
locks after observing the segment pointer.

The append order must be:

```cpp
// Writer:
fill segment header and payload
SpinLockAcquire(partition.lock)
segments[idx] = seg_ptr
pg_write_barrier()
segment_count = idx + 1
SpinLockRelease(partition.lock)
```

Readers that may run concurrently with writers must read `segment_count` with
an acquire barrier before reading `segments[]`. If the scheduler guarantees
Pipeline 3 starts only after Pipeline 1/2 completion, the lock-free read path is
simpler, but the publish ordering should still be explicit.

### 2. No Metadata Overwrite

Workers must not write `partition.total_rows`, `partition.segments[]`, or
`partition.segment_count` from local finalization state except through
`FlushPartitionToGlobal()`.

This is the main difference from the old `MaterializingPartitionSink` design.
The old shape had one global partition metadata slot and each worker finalized
its local buffer into that same slot. That causes last-writer-wins data loss.
The global segment design avoids that by appending immutable segments.

### 3. Row Layout Compatibility

All segments for one global partition must have the same `row_width` and
row-layout version. A reader should validate:

```text
seg->row_width == expected_row_width
seg->data_size == seg->row_count * seg->row_width
```

If this fails, it is a pipeline construction bug: different workers serialized
different row layouts into the same partition sink.

### 4. Varlen Offsets Are Segment-Local

`ParallelHashRowVarlenRef.offset` inside row data points into that segment's
own varlen payload, not into a global partition varlen arena.

Therefore `HashJoinPartitionSource` must keep the current segment's varlen base
while decoding rows. It must not concatenate segment varlen arenas unless it
also rewrites every varlen offset.

This is especially important for text/varchar payload columns. Fixed-width
queries may pass while this bug is latent.

### 5. Empty Partitions Are Valid

A partition with `segment_count == 0` or `total_rows == 0` is a valid empty
input. Pipeline 3 should produce no output for that partition without treating
it as an error.

## Pipeline Semantics

### Pipeline 1: BuildPartitionSource

```text
Source scan morsel
  -> optional filter/project operators
  -> serialize build-side rows
  -> radix pid = hash(join_key) & mask
  -> append to worker-local LocalPartitionBuffer[pid]
  -> flush segments to GlobalBuildPartitions
```

This pipeline is morsel-driven. It has one task per source block range. It does
not build a hash table and it does not publish a hash bridge.

### Pipeline 2: ProbePartitionSource

```text
Source scan morsel
  -> optional filter/project operators
  -> serialize probe-side rows
  -> radix pid = hash(join_key) & mask
  -> append to worker-local LocalPartitionBuffer[pid]
  -> flush segments to GlobalProbePartitions
```

This pipeline is also morsel-driven. It scans each probe morsel exactly once.
It must not generate one task per partition for the same morsel.

### Pipeline 3: HashJoinPartitionSource

```text
Partition task(pid)
  -> read GlobalBuildPartitions[pid]
  -> build local hash table for pid
  -> read GlobalProbePartitions[pid]
  -> probe local hash table
  -> push joined rows to downstream operators
  -> downstream sink: Aggregate / SortRun / Result
```

This pipeline is partition-driven. It has one task per radix partition. It
should not scan base tables.

Pipeline 3 may use pull internally from `HashJoinPartitionSource::get_next_batch`
because it is the source for that pipeline. Downstream operators should still be
push-driven from that source task.

## Scheduler Contract

### Dependencies

The conservative first version should use full pipeline dependencies:

```text
HashJoinPartitionSource depends on BuildPartitionSource
HashJoinPartitionSource depends on ProbePartitionSource
```

That means Pipeline 3 starts only after both global partition inputs are fully
materialized. This avoids streaming coordination complexity and makes
`segment_count` final when read.

Later, this can be relaxed to start Pipeline 3 partition-by-partition when:

```text
build_partition[pid].sealed == true
probe_partition[pid].sealed == true or probe stream can signal EOF
```

Do not implement streaming probe until the materialized version is correct.

### Task Counts

```text
BuildPartitionSource: morsel_count(build source)
ProbePartitionSource: morsel_count(probe source)
HashJoinPartitionSource: VOLVEC_RADIX_FANOUT
```

Do not use:

```text
probe_morsel_count * VOLVEC_RADIX_FANOUT
```

That was the old repeated-scan shape and defeats the purpose of partitioning.

## HashJoinPartitionSource Details

### Build Phase

For partition `pid`:

1. Read `GlobalBuildPartitions[pid].segment_count`.
2. For each segment:
   - validate header
   - read row data
   - decode key/hash from row layout
   - append row payload into `VecHashJoinState` partition store, or build a
     compact partition-local hash table directly
3. Build bucket heads for that partition.

The most direct reuse path is to load segments into
`partition_row_stores_[pid]` and `partition_entries_[pid]`, then call
`build_linear_probe_table_for_partition(pid)`.

### Probe Phase

For partition `pid`:

1. Iterate `GlobalProbePartitions[pid]` segments.
2. Decode rows into a small `DataChunk` or a lightweight row cursor.
3. Probe only partition `pid`'s hash table.
4. Emit joined rows to downstream.

The first version can decode segment rows into `DataChunk<DEFAULT_CHUNK_SIZE>`
because it reuses existing `VecHashJoinState` output/copy logic. The optimized
version should avoid reconstructing full chunks and probe row-layout rows
directly.

### Output Contract

`HashJoinPartitionSource` is a source operator. It should not own final query
materialization. It should output joined `DataChunk`s and let downstream
operators handle:

- `PartialAgg`
- `SortRun`
- `Limit`
- final result materialization

## Varlen Design

### Segment-Local Varlen Arena

Each `PartitionSegment` stores:

```text
[PartitionSegment][row bytes][varlen bytes]
```

Rows contain `ParallelHashRowVarlenRef` offsets relative to `varlen bytes`.
When reading:

```cpp
const uint8_t *row_base = (const uint8_t *)(seg + 1);
const char *varlen_base = (const char *)(row_base + seg->data_size);
```

If a varlen ref is not inline:

```cpp
ptr = varlen_base + ref.offset;
```

Never use another segment's varlen base for a row.

### Inline Small Strings

Keep the current inline-prefix behavior for small string payloads. It reduces
varlen arena pressure and keeps fixed-width queries simple.

## Memory Sizing

### Segment Limit

`VOLVEC_MAX_SEGMENTS_PER_PARTITION = 4096` is acceptable only as a development
constant. For SF10+ with small flush thresholds, skew can overflow one
partition.

Better sizing:

```text
max_segments_per_partition =
  ceil(total_input_bytes / flush_threshold) + worker_count * 2
```

Because this value is query-dependent, a second version should allocate segment
pointer arrays dynamically in DSA instead of embedding a fixed 4096-pointer
array in every partition.

### Local Buffer Threshold

Use a total-buffer cap as well as per-partition cap. With 256 partitions:

```text
256 * 64KB = 16MB per worker per sink
```

Two sinks (build + probe) can double that. A practical worker-local policy:

```text
flush partition when partition_bytes >= 64KB
or total_local_bytes >= 8MB
```

This keeps memory bounded under uniform distribution and skew.

## Failure Handling

The following conditions should be hard errors:

- invalid DSA pointer
- segment count overflow
- inconsistent row width inside a partition
- segment data size not equal to `row_count * row_width`
- unsupported row-layout version

The following conditions are normal:

- empty build partition
- empty probe partition
- partition with segments but zero matching rows

## Current Implementation Status

Implemented pieces:

- `global_partition_data.hpp/.cpp` defines the global DSA segment model.
- `FlushPartitionToGlobal()` appends immutable worker segments under a
  per-partition spinlock.
- `HashJoinPartitionSource` has the intended source shape and reads global
  partition segment metadata.

Still required before enabling as the default HashJoin path:

- Replace `MaterializingPartitionSink` usages with global partition data
  everywhere in worker execution.
- Ensure BuildPartitionSource and ProbePartitionSource configure the correct
  source side, not always the hash build input side.
- Complete `HashJoinPartitionSource` probe output for all required fixed-width
  payloads first.
- Add segment-local varlen decoding tests before enabling string-heavy queries.
- Add a guard so incomplete 3-pipeline lowering cannot shadow the proven
  2-pipeline QueryScheduler path.

## Test Plan

### Unit-Level Checks

1. Single worker, one segment, one partition.
2. Multiple workers, same partition, verify all segments visible.
3. Multiple workers, all partitions, verify total row counts.
4. Varlen payload with two segments, verify offsets are segment-local.
5. Segment overflow path produces clear error.

### Query-Level Checks

Start with fixed-width joins:

1. Synthetic small inner join with integer keys.
2. TPC-H Q3 with exact native diff.
3. Q5/Q10 after Q3 is stable.

Then string-heavy joins:

1. Q7/Q12 style joins with string/group payloads.
2. Validate BPCHAR trimming remains unchanged.

## Advantages

1. ✅ **Leader doesn't participate** - no serial merge bottleneck
2. ✅ **Worker-driven append** - reduces data copying
3. ✅ **Per-partition locks** - high parallelism (256-way)
4. ✅ **Incremental flush** - memory-controlled (64KB threshold)
5. ✅ **DSA-friendly** - coarse-grained allocations

## Trade-offs

1. ⚠️ **MAX_SEGMENTS limit** - must be sized for worst-case (suggest `morsel_count * worker_count * 2`)
2. ⚠️ **Spinlock on skew** - if data heavily skewed to one partition, serializes those appends (mitigated by large local buffer)
3. ⚠️ **DSA fragmentation** - many small allocations, but PostgreSQL DSA has internal slab allocator
4. ⚠️ **Pipeline breaker cost** - both build and probe inputs are materialized
   before join in the conservative version. This buys correctness and
   parallel partition ownership, but it costs one write/read pass over both
   inputs.
5. ⚠️ **Probe latency** - Pipeline 3 waits for full ProbePartitionSource in
   the conservative version. Streaming probe can reduce latency later, but
   should not be the first correctness target.

## Integration Points

### New Files

- `src/engine/parallel/global_partition_data.hpp` - Structure definitions
- `src/engine/parallel/global_partition_data.cpp` - Flush/read implementations
- `src/engine/exec/hash_join_partition_source.hpp` - Partition source operator
- `src/engine/exec/hash_join_partition_source.cpp` - Partition-local build/probe source

### Modified Files

- `src/engine/parallel/parallel_runtime_internal.hpp` - Add `global_partition_data_offset` to pipeline shared
- `src/engine/parallel/runtime_execution.cpp` - Initialize global partition data
- `src/engine/parallel/runtime_worker_main.cpp` - Use flush logic in BuildPartitionSource/ProbePartitionSource
- `src/engine/exec/hash_join_partition_source.cpp` - Read from global segments
- `src/engine/exec/hash_join_parallel.cpp` - Serialize rows into global partition segments

### Deleted Files

- `src/engine/parallel/partition_sink.hpp` - Replaced by global partition data
- `src/engine/parallel/partition_sink.cpp` - Replaced by flush logic

## Migration Path

1. Implement `global_partition_data.hpp/.cpp`
2. Update pipeline shared structure
3. Modify worker morsel processing to use flush
4. Update HashJoinPartitionSource to read segments
5. Delete old `MaterializingPartitionSink`
6. Test on Q5/Q3/Q10

## Open Questions

1. **MAX_SEGMENTS_PER_PARTITION sizing**: Static (4096) vs dynamic based on table size?
2. **Local buffer size**: 64KB vs 256KB vs adaptive?
3. **Flush threshold**: Per-partition vs total across all 256 partitions?
4. **Varlen handling**: Current design inlines small strings - keep this?
5. **Streaming probe**: after materialized correctness, should ProbePartitionSource
   expose per-partition EOF and let Pipeline 3 overlap probe production?
6. **Skew fallback**: when one partition dominates, should that partition be
   split with additional radix bits or processed by multiple tasks?
7. **Spill**: should oversized global partition segments spill via SharedFileSet
   while preserving the same segment iterator abstraction?

## References

- DuckDB radix partition implementation (inspiration)
- PostgreSQL DSA allocator (`src/backend/utils/mmgr/dsa.c`)
- Existing hash build fragment export (`hash_join_parallel.cpp`)

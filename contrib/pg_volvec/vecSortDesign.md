# VecSortState Design

This document describes the first vectorized `Sort` design for `pg_volvec`.
It is intentionally scoped to unlock the standard TPC-H Q1 plan shape on the
current engine, while leaving a clean path toward multi-run merge sort later.

## Goal

Support the planner shape:

`Sort -> Agg -> SeqScan`

without converting the aggregated result back into row tuples before sorting.

For the first implementation, the main success criteria are:

- full TPC-H Q1 can be offloaded end-to-end
- sort remains columnar inside `pg_volvec`
- result materialization stays correct for exact numeric aggregate outputs
- the implementation is easy to extend into memory-bounded runs later

## Non-Goals For The First Cut

- external spill to disk
- incremental sort
- generic text collation support
- resjunk sort expressions
- planner changes

The first cut should be strong enough for Q1, not a replacement for PostgreSQL
`tuplesort.c`.

## Why A Blocking Columnar Sort

The current engine already has a blocking `VecAggState`. Q1 sorts the final
aggregated result, not the base table. That means:

- the sort input is small relative to the scan
- we do not need tuple-at-a-time interaction with the child
- keeping the result columnar avoids paying to tuple-ify and immediately unpack

So the right first step is a blocking in-memory vector sort, not a hybrid that
hands the aggregated rows back to PostgreSQL before sorting.

## High-Level Plan

1. Drain the child plan completely.
2. Copy its visible rows into dense sort-owned payload chunks.
3. Extract sort keys into continuous key lanes.
4. Sort a row-id / ordinal array indirectly by those key lanes.
5. Emit output batches by gathering payload rows in sorted order.

The first version uses a single in-memory run. The interfaces are designed so
that later work can split the active run into multiple sorted runs and merge
them.

## Operator Placement

`VecSortState` lives in the vector engine and implements the same interface as
the current operators:

```cpp
class VecPlanState {
public:
  virtual bool get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk) = 0;
};
```

Initialization happens in `ExecInitVecPlanInternal()` when the planner root is
a `Sort` whose child subtree is otherwise supported.

## Output Metadata Contract

The bridge currently peeks through the root plan to recognize exact numeric
aggregate output. That does not compose with `Sort`.

So `VecPlanState` needs a lightweight per-column metadata interface:

```cpp
enum class VecOutputStorageKind {
  Int32,
  Int64,
  Double,
  StringRef,
  NumericScaledInt64,
  NumericAvgPair
};

struct VecOutputColMeta {
  Oid sql_type;
  VecOutputStorageKind storage_kind;
  int scale;
};
```

This metadata is used for two things:

- deciding how to compare sort keys
- deciding how `execute.cpp` converts a vector output column back into a
  PostgreSQL slot

`VecAggState` is the main producer of non-trivial output metadata. Wrapper
operators like `VecFilterState` and `VecSortState` forward metadata lookup to
their child.

## Data Structures

### Payload Storage

The sort stores dense payload rows in `DataChunk`s it owns:

```cpp
VolVecVector<DataChunk<DEFAULT_CHUNK_SIZE> *> payload_chunks_;
```

Any incoming selection vector is applied during materialization. Stored sort
payload is always dense:

- `has_selection = false`
- `count` is the number of physically stored rows in that chunk

This keeps downstream sort logic simple and gather-friendly.

### Row References

Rows are sorted indirectly through compact references:

```cpp
struct VecRowRef {
  uint32_t ordinal;
  uint32_t chunk_idx;
  uint16_t row_idx;
};
```

`ordinal` is the global row number in materialized order. It is used as the
final stable tie-breaker.

### Sort Key Descriptors

Each planner sort key becomes a descriptor:

```cpp
struct VecSortKeyDesc {
  uint16_t col_idx;
  Oid sql_type;
  VecOutputStorageKind storage_kind;
  bool descending;
  bool nulls_first;
  Oid collation;
  int scale;
};
```

These are derived from PostgreSQL `Sort` fields:

- `sortColIdx`
- `sortOperators`
- `collations`
- `nullsFirst`

Direction comes from `get_ordering_op_properties()`.

### Key Lanes

For each sort key, the sort stores a continuous lane of values and null flags:

- integer/date/numeric-scaled: `VolVecVector<int64_t>` or `VolVecVector<int32_t>`
- double: `VolVecVector<uint64_t>` using order-preserving bit normalization
- string prefix: `VolVecVector<VecStringRef>`
- null flags: `VolVecVector<uint8_t>`

Sorting compares these lanes by `ordinal`, instead of repeatedly traversing
payload chunks during comparator execution.

## Materialization Phase

`VecSortState` first drains the child.

For each incoming batch:

1. Determine the active rows.
   - If no selection is present, active rows are `[0, count)`.
   - If selection is present, only the selected rows are copied.
2. Append rows into the current payload chunk.
3. Record one `VecRowRef` per appended row.
4. Append the sort-key values for that row into each key lane.

This phase intentionally separates:

- payload columns, used for final output
- key lanes, used for comparator hot loops

That makes the comparator simpler and avoids repeated chunk chasing.

## Comparator Semantics

The comparator works on two ordinals, `a` and `b`.

For each sort key in order:

1. Compare null flags.
   - if one side is null and the other is not, use `nulls_first`
2. If both null, continue to the next key.
3. Compare the normalized key values.
4. If unequal, flip the result if `descending` is true.
5. If equal, continue to the next key.

If all sort keys compare equal, fall back to:

- `ordinal(a) < ordinal(b)`

This gives deterministic stable behavior even if `std::stable_sort()` is later
replaced or if future run merge needs a strict total ordering.

## Type Handling For The First Cut

### Supported key storage kinds

- `Int32`
- `Int64`
- `Double`
- `StringRef`
- `NumericScaledInt64`

### Explicitly deferred

- `NumericAvgPair`
- generic varlena string ordering
- locale-sensitive collation

This is acceptable for Q1 because its `ORDER BY` keys are the grouped
`bpchar(1)` columns `l_returnflag` and `l_linestatus`.

The current `VecStringRef` payload only preserves `len + prefix` well enough
for short fixed keys. That is enough for Q1, but not for general text sort.

## Double Normalization

For `double` keys, sort compares a normalized integer encoding, not raw IEEE
bits. The normalized form must preserve total order for ordinary ascending
comparison:

- negative values are bitwise inverted
- non-negative values flip the sign bit

Descending order is handled at the comparator level, not by storing a different
encoding.

## Exact Numeric Outputs

Aggregate output currently uses exact fixed-point storage:

- `sum(numeric)` is a scaled `int64`
- `avg(numeric)` is represented as `(scaled_sum, count)`

For sorting:

- `NumericScaledInt64` can be compared directly as signed integers because all
  values in a lane share the same scale
- `NumericAvgPair` is deferred in the first cut; it needs cross-multiplication
  with `NumericWideInt`

For result materialization:

- metadata must flow through `VecSortState`
- `execute.cpp` must stop assuming the root plan node itself is `VecAggState`

## Single-Run First, Multi-Run Later

The first implementation builds one in-memory run:

1. materialize all rows
2. sort one ordinal array
3. emit gathered output

Later, if memory-bounded sort becomes necessary, the same structure extends to:

1. build an active run until a memory threshold is reached
2. sort that run
3. keep it in memory or spill it
4. continue with the next run
5. perform a k-way merge over sorted runs

The key design point is:

- runs are not the same thing as input chunks

The sort unit is a run containing many rows across many chunks, not a
per-chunk local sort.

## Execution Flow

### Initialization

`ExecInitVecPlanInternal()`:

1. initialize the child vector subtree
2. build `VecSortKeyDesc[]` from PostgreSQL `Sort`
3. create `VecSortState(child, sort_node, key_descs)`

### First `get_next_batch()`

On first call:

1. drain child into sort-owned payload
2. build row refs and key lanes
3. sort row refs indirectly
4. set internal emit cursor to zero

### Subsequent `get_next_batch()`

Emit rows in sorted order by gathering from payload chunks until the output
chunk is full or the sorted stream is exhausted.

## Gather Back Into Output Chunks

After sorting, output is produced by iterating the sorted `VecRowRef`s:

1. locate source payload chunk and row
2. copy all visible payload columns into the destination output chunk row
3. set destination null bits
4. advance the emit cursor

This keeps the upstream payload compact and avoids physically reordering every
stored column array during sort.

## Why Not Reuse PostgreSQL tuplesort First

That would work, but it would also:

- abandon the vector engine right after aggregation
- force tuple materialization before sort
- make later vectorized merge-sort work harder, not easier

The current engine is already columnar. Sorting row references over columnar
payload is a much better fit.

## Why Not Sort Inside Each Input Chunk

Sorting each chunk independently creates too many tiny runs and makes the
overall sort semantics depend on batch boundaries. The right unit is a run, not
an input chunk.

For the first implementation, the whole input is one run.

## Q1-First Scope

To unlock the current full Q1 plan, the first implementation only needs:

- top-level `Sort`
- in-memory single-run sort
- sort keys that directly reference child output columns
- `bpchar(1)` key comparison through `VecStringRef`
- metadata passthrough so numeric aggregate outputs still materialize correctly

That keeps the first patch focused and realistic.

## Follow-Up Work

After full Q1 is stable, the next sort-related steps are:

1. `NumericAvgPair` comparison
2. `dense + no-null` sort key fast path
3. string arena support for generic text ordering
4. memory thresholding and multi-run merge
5. disk spill if needed

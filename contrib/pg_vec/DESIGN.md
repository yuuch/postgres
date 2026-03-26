# pg_vec: TPC-H Oriented Vectorized Executor

## Goal

`pg_vec` is a PostgreSQL extension that takes over execution for a narrow but
valuable workload: the TPC-H benchmark queries and close variants.

The design intentionally makes the following tradeoff:

- Leave the PostgreSQL planner and optimizer unchanged.
- Keep PostgreSQL-facing integration in C.
- Build the actual execution engine in C++.
- Optimize for TPC-H query shapes and TPC-H data types rather than generic
  PostgreSQL compatibility.

This is not meant to be a drop-in replacement for the PostgreSQL executor.
It is a specialized analytical engine living behind the executor hook
interface.

## Non-goals

The first serious version of `pg_vec` does not try to solve these problems:

- Full PostgreSQL type coverage.
- Full executor node parity with core PostgreSQL.
- Backward scan, scrollable cursors, or arbitrary `count`-limited portal
  execution.
- DML, triggers, writable CTEs, window functions, parallel query, or custom
  collations.
- General varlena and TOAST-heavy semantics outside what TPC-H needs.

If a query falls outside the supported envelope, `pg_vec` should decline the
query and fall back to the standard executor.

## Workload Envelope

### TPC-H schema assumptions

The current local TPC-H setup uses these types:

- `INTEGER` for all keys and most dimensions.
- `DATE` for order and shipping dates.
- `CHAR(n)` / `VARCHAR(n)` for names, comments, statuses, phone prefixes,
  shipping modes, and similar text predicates.
- `DECIMAL(15,2)` for prices, discounts, taxes, balances, quantities, and
  derived monetary expressions.

This narrow type envelope is enough for the current TPC-H setup under
`contrib/pg_carbon/tests/tpch/prepare.sql`.

### TPC-H SQL features that matter

The query set requires support for:

- Sequential scans over large fact tables.
- Filters with `AND`, `OR`, `BETWEEN`, `IN`, `NOT IN`, `LIKE`, date ranges, and
  arithmetic comparisons.
- Equi-joins, one `LEFT OUTER JOIN`, `EXISTS`, `NOT EXISTS`, and `IN`
  subqueries.
- Group aggregation with `SUM`, `AVG`, `COUNT`, `MIN`, `MAX`, `HAVING`, and
  `COUNT(DISTINCT ...)`.
- `CASE` inside aggregates.
- `ORDER BY` and `LIMIT`, usually on already aggregated result sets.
- `EXTRACT(YEAR FROM date)` and `substring(c_phone from 1 for 2)`.
- Scalar subqueries and correlated aggregate subqueries.

This matters because it means the engine can be small and still run all 22
queries.

## High-Level Architecture

The design splits into four layers:

1. `PG bridge` in C:
   - hook installation and lifecycle management
   - `QueryDesc`, `EState`, `TupleDesc`, `TupleTableSlot`, `DestReceiver`
   - `Relation`, `Snapshot`, memory contexts, error boundaries
2. `Lowerer`:
   - converts PostgreSQL plans and subplans into a compact vector IR
   - performs TPC-H-specific rewrites for correlated and scalar subqueries
3. `Vector engine` in C++:
   - `DataChunk` objects, selection vectors, vector expressions
   - scans, filters, projections, joins, aggregates, sort/top-N
4. `Result sink`:
   - converts final columnar results back into row-oriented slots for the
     PostgreSQL destination receiver

The dataflow is:

```text
PG QueryDesc / PlannedStmt
        |
        v
  support check + lowering
        |
        v
   C++ vector plan / IR
        |
        v
  heap scan -> row-to-column DataChunk deform
        |
        v
  filter -> project -> join -> agg -> sort/topN
        |
        v
   column-to-row materialization
        |
        v
   TupleTableSlot -> DestReceiver
```

The crucial design choice is that PostgreSQL's physical plan nodes are not
executed directly. They are translated into a much smaller internal operator
set.

## Why C + C++ Is The Right Split

The C layer should stay small and unambitious. It exists to interact with the
PostgreSQL backend ABI safely.

The C++ layer should own:

- Template-heavy type kernels.
- Fixed-point decimal arithmetic.
- Hash tables and aggregation state.
- String predicate kernels.
- Specialized vector expression evaluation.
- Reusable operator implementations.

This split keeps PostgreSQL-facing code stable while letting the analytical
engine use templates aggressively.

## Hook Lifecycle

`pg_vec` should install all four executor hooks:

- `ExecutorStart_hook`
- `ExecutorRun_hook`
- `ExecutorFinish_hook`
- `ExecutorEnd_hook`

### `_PG_init`

`_PG_init` should:

- register GUCs such as `pg_vec.enabled`, `pg_vec.force`, and
  `pg_vec.debug_fallback`
- install the executor hooks
- optionally enforce that the extension is loaded through `LOAD 'pg_vec'` or
  `shared_preload_libraries`

### `ExecutorStart`

`ExecutorStart` is responsible for analysis and setup, not execution.

Recommended responsibilities:

- Reject unsupported statements early.
- Reject unsupported plan nodes, functions, or data types early.
- Collect referenced columns per relation.
- Lower the core PostgreSQL plan tree into a `VecPlan`.
- Rewrite supported subqueries into vectorizable forms.
- Open relations and prepare scan descriptors.
- Create a `VecQueryState` in a PostgreSQL-owned memory context.
- Prepare output descriptors and result sink state.

Recommended pragmatic rule for v1:

- Still call `standard_ExecutorStart()` to preserve query context, snapshots,
  and receiver wiring.
- Do not call `standard_ExecutorRun()` for supported queries.

This keeps lifecycle semantics safe while avoiding row-by-row execution.

### `ExecutorRun`

`ExecutorRun` is the hot path and should own the whole analytical execution.

Responsibilities:

- If the query is unsupported, call the previous or standard executor.
- If supported, run the vector plan from source to sink.
- Produce output rows by materializing final `DataChunk`s into
  `TupleTableSlot`s and
  forwarding them with `dest->receiveSlot`.

For the initial TPC-H-focused version, it is acceptable to support only:

- forward scan
- full query execution
- no portal rewinding

Anything else may fall back to core PostgreSQL.

### `ExecutorFinish`

`ExecutorFinish` should:

- finalize aggregate state
- flush pending top-N / sort state
- flush pending result `DataChunk`s if needed
- mark the query state as completed

### `ExecutorEnd`

`ExecutorEnd` should:

- destroy vector engine state
- close scan state
- free hash tables, arenas, and temporary buffers
- detach the `VecQueryState` from bridge state

## Row-To-Columnar And Column-To-Row Boundaries

This design explicitly accepts two format changes:

- At the scan boundary: rows become columns.
- At the output boundary: columns become rows again.

That is the right tradeoff for TPC-H.

### Scan boundary

The scan layer reads PostgreSQL heap tuples and batch-deforms only the columns
actually needed by the query. After that point, execution is columnar.

### Output boundary

The final result is converted back into rows because PostgreSQL's client path is
slot-oriented. This conversion should happen only once, at the top of the
pipeline.

Because TPC-H result sets are typically small after aggregation and sorting, the
output conversion cost is acceptable.

## Scan Layer Design

The scan layer should expose one abstract interface:

```text
DataChunkScanner::next_chunk() -> DataChunk
```

Under it, two implementations are useful:

- `SlotDataChunkScanner`: a correctness-first path built on PostgreSQL tuple/slot
  APIs; useful for bring-up and debugging
- `HeapPageDataChunkScanner`: a fast path that reads heap pages directly,
  checks tuple visibility, and deforms tuples into column vectors

The production fast path should target heap tables, because the TPC-H setup is
heap-based and append-heavy. A specialized path is acceptable here.

For the production path, deforming should target `DataChunk` directly. It
should not depend on PostgreSQL's `ExprState` / `TupleTableSlot` JIT deform
pipeline, because that machinery is optimized for the core row executor rather
than a columnar destination format.

The recommended progression is:

- first, lower required columns into a small `DeformProgram` and execute it with
  a `DataChunk`-oriented interpreter
- then, use that same `DeformProgram` as the input to a small code generator
  for supported query fragments
- keep query-specific hand tuning only for the hottest kernels after the common
  `DeformProgram` path exists

## Internal Data Model

### DataChunk layout

Each operator consumes and produces `DataChunk` objects.

A `DataChunk` should contain:

- `count`
- `capacity`
- optional `SelectionVector`
- one `ColumnVector` per referenced or produced column
- null bitmap per column where needed
- optional temporary arena for variable-length payloads owned by the chunk

Recommended initial chunk size:

- 1024 or 2048 rows

In code, the top-level shape should be close to:

```text
DataChunk
  count
  capacity
  sel
  columns[N]
```

The important point is that a `DataChunk` is not "one vector". It is a bundle
of multiple same-length column vectors plus an optional row selection.

### ColumnVector layout

For the first implementation, each `ColumnVector` should use a flat columnar
layout.

For fixed-width types:

- values live in one contiguous typed array
- optional validity bits live in a separate bitmap
- row `i` in the chunk maps to `values[i]` when no selection is active
- row `sel[j]` maps to the active logical row when a selection vector is active

That means the physical memory shape is:

```text
FlatVector<T>
  T values[capacity]
  uint64 validity[(capacity + 63) / 64]   // only if nullable
```

For the TPC-H fast path, many hot columns are `NOT NULL`, so the validity
bitmap can often be omitted entirely.

For strings, use a simple flat representation first:

```text
FlatVector<StringRef>
  StringRef values[capacity]

StringRef
  const char *ptr
  uint32 len
```

Dictionary encoding can be added later for low-cardinality group keys, but it
does not need to block Q6 or Q1.

### SelectionVector

`SelectionVector` should be chunk-local:

```text
SelectionVector
  uint16 or uint32 row_ids[count]
```

The execution convention should be:

- scanners usually produce dense chunks with no selection
- filters produce or refine a selection vector
- arithmetic kernels can either honor the selection vector directly or compact
  into a new output vector
- aggregates iterate only over selected row ids

This keeps filtering cheap and avoids copying rows after every predicate.

### Core physical types

The engine only needs a few physical types:

- `Int32`
- `Int64`
- `Date32` as days from epoch
- `Decimal<scale>` backed by `int64`
- `StringRef`
- small dictionary ids for low-cardinality string group keys where useful

### Decimal strategy

Do not use PostgreSQL `Numeric` in the hot path.

Instead:

- store `DECIMAL(15,2)` inputs as scaled `int64`
- use `__int128` for multiplication and aggregate accumulation
- keep scale as a template parameter

This directly matches the arithmetic used in Q1, Q5, Q6, Q8, Q9, Q10, Q14,
Q15, Q17, and Q19.

### String strategy

TPC-H string handling can be specialized:

- equality
- lexicographic order
- prefix `LIKE`
- suffix `LIKE`
- contains `LIKE`
- fixed-prefix extraction for phone country code

`CHAR(n)` columns may be normalized to trimmed views during DataChunk deform if that
matches the chosen comparison semantics for the benchmark workload.

## Expression Engine

The expression layer should be vectorized and template-driven, not `Datum`-
driven.

Support is needed for:

- arithmetic on decimals and integers
- comparisons
- boolean conjunction / disjunction
- `CASE`
- `IN` against constant lists
- `LIKE` pattern classes
- `EXTRACT(YEAR FROM date)`
- `substring(col from 1 for 2)`

Recommended structure:

- `ExprNode` as a small typed IR
- template kernels for each physical operator
- a `SelectionVector`-aware evaluation convention

The engine should prefer a small number of reusable kernels over a generic,
interpreted expression system.

## Operator Set

PostgreSQL plan nodes should lower into the following vector operators:

- `VecScan`
- `VecFilter`
- `VecProject`
- `VecHashJoin`
- `VecLeftHashJoin`
- `VecSemiJoin`
- `VecAntiJoin`
- `VecAgg`
- `VecSort`
- `VecTopN`
- `VecResultSink`

This set is enough to cover the full TPC-H query family.

### Join strategy

The core vector join algorithm should be hash join.

Even if PostgreSQL produces `HashJoin`, `MergeJoin`, or `NestLoop`, the lowerer
should normalize supported equi-join cases into the same vector hash join
operator where possible.

This keeps the engine small and stable.

## Q6 As The First Fully Vectorized Path

Q6 should be the first query that runs without row-at-a-time execution in the
hot path.

The query shape is:

- one heap table scan over `lineitem`
- one conjunctive filter
- one arithmetic projection `l_extendedprice * l_discount`
- one global `SUM`

That makes it ideal for the first end-to-end `DataChunk` pipeline.

### Q6 required columns

The Q6 scanner only needs four input columns:

- `l_shipdate`
- `l_discount`
- `l_quantity`
- `l_extendedprice`

The first specialized `lineitem` chunk layout can therefore be:

```text
DataChunk(count = N)
  shipdate      : FlatVector<int32>
  discount      : FlatVector<int64>   // DECIMAL(15,2) scaled by 100
  quantity      : FlatVector<int64>   // DECIMAL(15,2) scaled by 100
  extendedprice : FlatVector<int64>   // DECIMAL(15,2) scaled by 100
  sel           : optional SelectionVector
```

No other columns need to be touched, and no generic `Datum` values should
survive inside the C++ hot loop.

### Q6 scan path

The fully vectorized Q6 scan path should be:

1. `HeapPageDataChunkScanner` reads visible tuples from heap pages.
2. For each visible tuple, it deforms only the four required attributes.
3. It converts PostgreSQL values immediately into engine-native physical types.
4. It appends them into the current `DataChunk`.
5. Once `count == capacity`, it returns the chunk upstream.

For Q6, "fully vectorized" means:

- no `TupleTableSlot` in the steady-state hot loop
- no `Numeric` arithmetic in the hot loop
- no row materialization between scan, filter, projection, and aggregation

### Q6 filter kernel

Q6's predicate is:

- `shipdate >= lower`
- `shipdate < upper`
- `discount >= lower`
- `discount <= upper`
- `quantity < upper`

The filter operator should evaluate these predicates column-at-a-time over the
current `DataChunk` and produce one selection vector.

Conceptually:

```text
sel = filter(
  shipdate >= lower &&
  shipdate < upper &&
  discount >= lower &&
  discount <= upper &&
  quantity < upper)
```

A single fused filter kernel is better than five separate filter operators for
Q6, because it avoids intermediate selections and matches the final benchmark
shape exactly.

### Q6 projection kernel

For rows that survive the selection vector, compute:

```text
revenue_i = extendedprice_i * discount_i
```

Since both inputs are scaled by `100`, the output scale is `4`. The projection
can therefore write into:

```text
FlatVector<__int128> revenue   // scale = 4
```

or skip materializing a full output vector and feed the product directly into
the aggregate kernel.

For Q6, I prefer the second option:

- filter builds the selection vector
- aggregate directly multiplies and accumulates selected rows

That keeps the operator chain small while still being vectorized.

### Q6 aggregate kernel

The global aggregate state is just:

```text
GlobalSumState
  __int128 sum
```

For each selected row id:

```text
sum += (__int128) extendedprice[row] * (__int128) discount[row]
```

At the end of all chunks:

- finalize `sum` as a decimal with scale `4`
- convert it to PostgreSQL `Numeric` once
- materialize one output tuple

So the only `columnar -> row` conversion for Q6 happens at the very end, for a
single scalar result.

### Q6 operator graph

The intended Q6 execution graph is:

```text
HeapPageDataChunkScanner
        |
        v
   Q6FilterKernel
        |
        v
   Q6GlobalSumKernel
        |
        v
    ResultSink
```

This is still consistent with the general engine design, but it lets the first
fully vectorized path be deliberately narrow and fast.

### Why this is the right first target

If Q6 is implemented this way, we prove five important things early:

- direct heap-page to column-vector loading works
- decimal values can leave PostgreSQL `Numeric` early
- selection-vector execution works
- vectorized aggregate state works
- only the sink needs to rebuild PostgreSQL row form

Once these are stable, Q1 becomes mostly "Q6 plus grouped aggregation and more
projection expressions", which is a much better next step than starting with a
generic grouped engine first.

### Aggregation strategy

`VecAgg` should support:

- global aggregation
- hash aggregation
- grouped aggregation with composite keys
- `SUM`, `COUNT`, `AVG`, `MIN`, `MAX`
- `COUNT(DISTINCT ...)` via per-group distinct state

`AVG` should lower into `SUM + COUNT` and finalize late.

### Sorting strategy

Use:

- full materializing sort for ordinary `ORDER BY`
- heap-based `TopN` for `ORDER BY ... LIMIT`

TPC-H result sets after grouping are usually small enough that a pragmatic sort
implementation is acceptable.

## Subquery Lowering Rules

The engine's success depends more on lowering than on adding more physical
operators.

### Scalar uncorrelated subqueries

Examples:

- `ps_supplycost = (select min(...))`
- `sum(...) > (select sum(...) * 0.0001 ...)`
- `total_revenue = (select max(total_revenue) ...)`
- `c_acctbal > (select avg(c_acctbal) ...)`

Lowering rule:

- execute as `InitOnceAgg`
- materialize one scalar
- inject the scalar as a constant into the outer plan

### Correlated aggregate subqueries

Examples:

- Q17: aggregate over `lineitem` grouped by `l_partkey`
- Q20: aggregate over `lineitem` grouped by `(l_partkey, l_suppkey)`

Lowering rule:

- identify correlation keys
- pre-aggregate inner relation by those keys
- join the aggregate result back into the outer pipeline

This avoids per-row subquery execution entirely.

### `IN`, `EXISTS`, `NOT EXISTS`, `NOT IN`

Lowering rule:

- `IN` / `EXISTS` -> `VecSemiJoin`
- `NOT EXISTS` -> `VecAntiJoin`
- `NOT IN` -> `VecAntiJoin` where TPC-H schema guarantees the relevant keys are
  non-null

The TPC-H schema's `NOT NULL` keys make this simplification practical.

### Derived tables and views

Derived tables and benchmark views should lower as:

- an inner `VecPlan`
- plus a parent `VecPlan` consuming its output

Query 15's view should be treated as ordinary query structure after parsing and
planning, not as a special executor concern.

## TPC-H Coverage Matrix

The following support matrix is the target lowering behavior:

| Query | Core support needed |
| --- | --- |
| Q1 | scan, date filter, arithmetic projection, hash agg, order |
| Q2 | multi-join, suffix `LIKE`, scalar `MIN` subquery, top-N |
| Q3 | joins, date predicates, agg, top-N |
| Q4 | `EXISTS` -> semi join, group agg, order |
| Q5 | multi-join, date range, agg, order |
| Q6 | scan, filter, global agg |
| Q7 | joins, date range, `EXTRACT(YEAR)`, agg, order |
| Q8 | derived table, `CASE`, `EXTRACT(YEAR)`, agg, order |
| Q9 | multi-join, contains `LIKE`, arithmetic projection, agg, order |
| Q10 | joins, date range, agg, top-N |
| Q11 | group agg, `HAVING`, scalar aggregate subquery |
| Q12 | join, `CASE` in aggregate, date predicates, group/order |
| Q13 | left join, nested grouping, contains `LIKE` |
| Q14 | join, conditional aggregate, global aggregate |
| Q15 | view/derived table, aggregate, scalar `MAX` subquery |
| Q16 | join, `NOT IN`, `COUNT(DISTINCT ...)`, order |
| Q17 | correlated aggregate -> pre-agg + join |
| Q18 | grouped subquery in `IN`, agg, top-N |
| Q19 | join plus large DNF predicate, global aggregate |
| Q20 | nested `IN`, prefix `LIKE`, correlated aggregate -> pre-agg + join |
| Q21 | `EXISTS` + `NOT EXISTS`, joins, group agg, top-N |
| Q22 | substring prefix, scalar `AVG` subquery, anti join, group/order |

## Suggested File Layout

The final implementation should likely grow into something like this:

```text
contrib/pg_vec/
  pg_vec.control
  pg_vec--1.0.sql
  src/
    bridge/
      pg_vec.c
      state.h
      state.c
      lower.h
      lower.c
      scan.h
      scan.c
      result_sink.h
      result_sink.c
    engine/
      vec_types.hpp
      data_chunk.hpp
      vec_expr.hpp
      vec_hash_join.hpp
      vec_agg.hpp
      vec_sort.hpp
      vec_plan.hpp
      vec_exec.cpp
      vec_lower.cpp
```

Exact filenames are flexible, but the layering should stay stable.

## Implementation Priorities

The shortest path to a useful executor is:

1. Bring up the bridge and lifecycle state.
2. Implement `DataChunk` scan/filter/global agg for Q6.
3. Add grouped aggregation and row sink for Q1.
4. Add inner hash join and date/string predicates for Q3/Q5/Q10/Q14/Q19.
5. Add semi/anti join lowering for Q4/Q16/Q18/Q21/Q22.
6. Add scalar and correlated aggregate subquery rewriting for Q2/Q11/Q15/Q17/Q20/Q22.
7. Add left join and `COUNT(DISTINCT ...)` for Q13/Q16.

This sequence gets to "TPC-H runs end-to-end" much faster than chasing generic
executor parity.

## Final Design Principle

The right architecture for `pg_vec` is:

- not a general PostgreSQL executor clone
- not 22 fully hardcoded benchmark query implementations
- but a small vector execution engine whose type system, expression kernels,
  and lowering rules are deliberately shaped around TPC-H

In short:

`planner unchanged` + `C bridge` + `C++ vector IR` + `row->column at scan` +
`column->row at sink` is the intended architecture.

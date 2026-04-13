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

## Current Implementation Snapshot

The codebase already follows the intended four-way split:

- `src/bridge/`
  - thin PostgreSQL-facing C glue
  - executor hooks, query state, and row sink materialization
- `src/ir/`
  - compact `pg_vec` execution IR shared between translator and engine
- `src/translate/`
  - PostgreSQL `Plan` / `Expr` / `Aggref` to IR translation
- `src/engine/`
  - `DataChunk`, deform, scan, filter, aggregate, and join execution

The current implementation is intentionally narrower than the final design:

- the top-level IR is still a composite `scan + optional join + filter + agg`
  descriptor rather than a full independent operator tree
- the engine is chunk-at-a-time and pipeline-style, but not yet a full
  morsel-driven scheduler
- input filters now have a typed fast path for conjunctive
  `column-vs-constant` comparisons, with interpreter fallback for more complex
  boolean shapes
- as of March 31, 2026, all in-tree self-contained fixtures `q1..q22` plus
  `plain_expr` run to completion on the local 19devel instance
- as of March 31, 2026, the same self-contained `q1..q22` sweep completes
  without any `pg_vec: fallback to standard executor` warnings
- `pg_vec` now auto-raises too-small session `max_stack_depth` settings to
  `7MB` before translation on this machine, which removes the remaining
  default-stack fallback cases such as `Q8` and `Q14`

As of the current implementation:

- `Q1` runs on the generic single-table `SeqScan -> Filter -> Agg` path
- `Q6` runs on the same generic path and is the first stable fully columnar
  hot path
- `Q12`, `Q14`, and `Q19` now all run on the generic two-input inner-join plus
  aggregate path
- `Q4` now runs after widening the same join pipeline to accept planner
  semi-join shapes as well as pre-unique inner joins
- `Q3`, `Q5`, `Q7`, `Q8`, `Q9`, and `Q10` now run on the generic multi-join
  grouped top-N / grouped aggregate path
- `Q11` now runs after lowering aggregate `HAVING` clauses into a post-agg
  filter IR and resolving uncorrelated scalar `InitPlan` values into constants
- `Q15` now runs after normalizing join/project boundary `Var` slots to the
  real underlying input attnos and keeping the scalar `MAX(total_revenue)`
  subquery on the derived revenue relation in the post-agg filter path
- `Q13` now runs after preserving derived-aggregate `Agg` boundaries during
  translation, teaching the executor to treat a single derived grouped input
  as a materialized stream instead of a base-relation scan, and widening
  string `LIKE` lowering/execution to cover generic TPC-H `LIKE/NOT LIKE`
  patterns
- `Q18` now runs after accepting direct derived grouped-aggregate join inputs,
  lowering grouped-subquery `HAVING` into the derived post-agg filter path,
  and preserving grouped-input sort keys for `Limit -> GroupAggregate -> Sort`
  planner shapes
- `Q22` now runs after adding a dedicated substring-prefix expression,
  preserving high-scale scalar `AVG(...)` constants for numeric compares, and
  accepting single-join anti-join planner shapes
- `Q2` now runs after flattening bushy nested-loop inner-join trees into the
  existing left-deep join IR using per-qual owner-plan context, which also
  makes the correlated scalar `MIN(...)` rewrite lower against the generic
  project path
- `Q2`'s correlated `MIN(...)` rewrite also initializes the derived grouped
  subplan expression roots correctly, so the nested grouped input executes on
  `pg_vec` instead of tripping a runtime grouped-key decode fallback
- the `Q19` fix required lowering residual join predicates from `Join.joinqual`
  instead of only looking at `Plan.qual`
- the live planner path for `Q3` required accepting top
  `Finalize GroupAggregate` / `FINAL_DESERIAL` shapes and seeing through
  elidable `Partial HashAggregate` wrappers inside a join subtree
- the `Q10` fix required join-tree lowering to tolerate commuted bushy
  inner-join shapes while still producing a left-deep join chain for the
  executor
- the current derived-expression coverage now includes flattened derived-table
  shapes such as `Q8` and contains-`LIKE` arithmetic shapes such as `Q9`
- the remaining gap is no longer SQL coverage but performance and execution
  quality on the already-supported TPC-H shapes

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

The current production scanner already follows this direction:

- it uses `HeapScanDesc` with `SO_ALLOW_PAGEMODE`
- it reuses PostgreSQL's `rs_read_stream` sequential read path for buffer
  prefetch behavior close to core `SeqScan`
- it collects visible tuples page-by-page and deforms them directly into
  `DataChunk` column arrays

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

For strings, the current implementation is intentionally simple but should be
viewed as a transitional design rather than the desired end state.

Today, `pg_vec` stores strings as a fixed inline payload:

```text
FlatVector<String128>
  String128 values[capacity]

String128
  uint16 len
  char bytes[128]
```

This is already columnar, but it is not yet a strong vectorized string layout:

- every value is eagerly decoded from PostgreSQL varlena form
- every value is copied into a fixed-width inline object
- equality and ordering still fall back to length trimming plus `memcmp`
- there is no offset buffer, prefix cache, dictionary encoding, or late
  materialization

That is good enough for correctness and for bringing up string predicates, but
it is one reason string-heavy TPC-H queries still lag core PostgreSQL.

The recommended target is a two-level string layout:

```text
FlatVector<StringRef>
  StringRef refs[capacity]
  uint8 data_arena[...]

StringRef
  uint32 offset
  uint32 len
  uint64 prefix
```

In that design:

- `offset` points into a chunk-local or materialized-input-local byte arena
- `len` is the logical string length after any type-specific normalization
- `prefix` stores the first 8 bytes, zero-padded when shorter

This representation lets the engine:

- compare `len` and `prefix` before touching full payload bytes
- hash strings from cheap metadata first, then fall back to full-byte checks on
  collisions
- avoid copying large fixed-width string objects around the pipeline
- reuse the same representation in both scan chunks and materialized join build
  inputs

For low-cardinality TPC-H string columns, a further optimization is worth
planning from the start:

```text
DictionaryVector<StringId>
  uint16 ids[capacity]
  StringRef dictionary[ndistinct]
```

This is especially relevant for values such as shipping modes, priorities,
status flags, brands, and containers, where joins, filters, grouping, and
sorting can often run entirely on integer ids.

The intended progression is:

1. keep the current `String128` path as the correctness baseline
2. add `StringRef + arena + prefix` for general `text` / `varchar` / `bpchar`
   execution
3. add dictionary fast paths for low-cardinality columns
4. keep final row materialization as the only place that must reconstruct a
   client-visible string result

The current codebase is now in the middle of step 2:

- scan `DataChunk` storage uses `StringRef + arena + prefix`
- materialized join-build inputs use the same representation
- `bpchar` values are normalized during deform by trimming trailing spaces once
- string comparisons in the engine hot path now compare `len + prefix + tail`
  directly instead of reconstructing full inline string objects first
- grouped result rows and bridge output still use `PgVecStringConst` as the
  stable boundary format

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

Q6 is the first query that runs without row-at-a-time execution in the hot
path.

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

The implemented Q6 scan path is:

1. `HeapDataChunkScanner` reads visible tuples from heap pages.
2. For each visible tuple, it deforms only the four required attributes.
3. It converts PostgreSQL values immediately into engine-native physical types.
4. It appends them into the current `DataChunk`.
5. Once `count == capacity`, it returns the chunk upstream.

For Q6, "fully vectorized" means:

- no `TupleTableSlot` in the steady-state hot loop
- no `Numeric` arithmetic in the hot loop
- no row materialization between scan, filter, projection, and aggregation

In the current code, the scanner also reuses PostgreSQL's sequential
`read_stream` path instead of manually walking relation blocks. That closed a
major gap with core `SeqScan` on 10GB TPC-H runs.

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

The current implementation does not hardcode a Q6-specific filter function.
Instead, the engine now:

- binds a supported input filter into a `BoundFilterProgram`
- recognizes conjunctive `column-vs-constant` comparisons
- executes them as typed selection-vector kernels over the current
  `DataChunk`
- falls back to the generic recursive `eval_qual()` interpreter for unsupported
  shapes such as `OR` or richer expression trees

This is intentionally TPCH-oriented rather than PostgreSQL-generic. It gives
Q6 and Q1 a fast path without introducing query-specific execution entry
points.

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

One important implementation detail is that `DECIMAL(15,2)` values now leave
PostgreSQL packed `Numeric` form through a dedicated scale-2 fast path during
deform. That fast path handles the common TPC-H finite numeric layout directly
and falls back to the generic decoder when needed.

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

### Current performance notes

On the current local 10GB TPC-H environment:

- `Q6` is now effectively at parity with core PostgreSQL
  - median `pg_vec=off`: `4739.627 ms`
  - median `pg_vec=on`: `4739.942 ms`
- `Q1` shows a material win on the same engine path
  - median `pg_vec=off`: `21622.518 ms`
  - median `pg_vec=on`: `10542.608 ms`

On March 26, 2026, after retuning the live `/Users/chenyunwen/data/pg_tpch`
instance for analytic TPC-H work
(`shared_buffers=6GB`, `effective_cache_size=18GB`, `work_mem=128MB`,
`maintenance_work_mem=2GB`, `wal_buffers=64MB`, `max_wal_size=8GB`,
`default_statistics_target=500`, `jit=off`), the same 10GB benchmark with
session-local parallelism disabled produced:

- `Q1` still wins materially on `pg_vec`
  - median `pg_vec=off`: `21838.034 ms`
  - median `pg_vec=on`: `12630.044 ms`
  - improvement: about `42.2%`
- `Q6` regresses slightly versus core PostgreSQL
  - median `pg_vec=off`: `4152.239 ms`
  - median `pg_vec=on`: `4282.139 ms`
  - regression: about `3.1%`
- `Q14` regresses more noticeably on the generic join path
  - median `pg_vec=off`: `4522.797 ms`
  - median `pg_vec=on`: `4896.474 ms`
  - regression: about `8.3%`

The latest live `Q14` sample confirms that the hottest leaf is still
`DataChunkDeformer::append_tuple()`, with the next visible buckets in
`evaluate_filter_clause()`, join hash lookup, and the generic aggregate
expression interpreter. The flamegraph generated from that run lives at
`contrib/pg_vec/tests/q14_pgvec_on.flame.svg`.

After the string-column refactor, execution-time scan chunks and materialized
join inputs now use `StringRef + arena + prefix` instead of the original
`String128` copies on the hot path. Grouped result rows and the bridge output
still keep `String128` as the stable boundary format.

A first conservative late-materialization prototype now exists for the
single-join path:

- columns used only by aggregate/group expressions can be marked `late`
- scanner/materialize only keep early columns plus row identity
- materialized build-side rows lazily cache late columns on first access
- streaming probe-side rows can decode late columns from a chunk-local tuple
  copy fast path for sparse survivor sets

This is still a bring-up implementation rather than a finished design. On the
latest live 10GB retest after wiring that prototype, the medians were:

- `Q12`
  - median `pg_vec=off`: `9499.880 ms`
  - median `pg_vec=on`: `10163.336 ms`
  - regression: about `7.0%`
- `Q14`
  - median `pg_vec=off`: `6255.407 ms`
  - median `pg_vec=on`: `7363.543 ms`
  - regression: about `17.7%`
- `Q19`
  - median `pg_vec=off`: `7245.564 ms`
  - median `pg_vec=on`: `9689.324 ms`
  - regression: about `33.7%`

On the latest live 10GB retest after widening the generic multi-join grouped
aggregate path far enough for `Q5`, the medians were:

- `Q5`
  - median `pg_vec=off`: `6786.118 ms`
  - median `pg_vec=on`: `8790.786 ms`
  - regression: about `29.5%`

On the latest live 10GB retest after adding grouped
`EXTRACT(YEAR FROM date)` support and deferring multi-join residual filters
until all referenced inputs are bound, the medians were:

- `Q7`
  - median `pg_vec=off`: `6277.694 ms`
  - median `pg_vec=on`: `11635.590 ms`
  - regression: about `85.3%`

The main takeaway is that late materialization does help recover some of the
string/numeric eager-decode cost on join queries, but it is not yet enough to
close the gap with core PostgreSQL. The remaining hot path is still dominated
by numeric decode, residual filter work, and the row-wise nature of the
current join probe pipeline.

The most recent improvements that moved Q6 from a regression to parity were:

- switching the scanner to PostgreSQL's `read_stream` sequential scan path
- adding the `DECIMAL(15,2)` deform fast path
- adding typed selection-vector filter kernels for simple conjunctive filters

The next likely hotspot for Q6-style queries is aggregate input expression
evaluation, which still uses the generic expression interpreter.

For multi-join grouped queries, the current executor no longer materializes a
full intermediate joined-row set. It now streams the leftmost input, probes a
left-deep chain of right-side hash tables, and aggregates immediately on
successful probe chains. This is what made live `Q3` practical on the 10GB
dataset after translator support for finalize/partial aggregate planner shapes
was added. `Q7` extends the same path with one additional expression feature:
group keys can now be lowered as compact expression programs rather than only
as base columns, and the current engine specifically supports
`EXTRACT(YEAR FROM date)` on that path.

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

The implementation is currently organized like this:

```text
contrib/pg_vec/
  pg_vec.control
  pg_vec--1.0.sql
  src/
    bridge/
      pg_vec.c
      state.h
      state.c
      execute.h
      execute.c
    ir/
      vec_ir.h
    translate/
      pg_translate.h
      pg_translate.c
    engine/
      data_chunk.hpp
      data_chunk_deform.hpp
      scan_filter_agg_exec.cpp
      vec_exec_api.h
```

The likely next refinement is to split `scan_filter_agg_exec.cpp` into more
focused engine modules once the scan/filter/agg/join kernels stabilize.

The final implementation should likely grow further into something like this:

```text
contrib/pg_vec/
  src/
    bridge/
      pg_vec.c
      state.c
      execute.c
    ir/
      vec_ir.h
      vec_plan_ir.h
      vec_expr_ir.h
    translate/
      pg_plan_translate.c
      pg_expr_translate.c
      pg_agg_translate.c
    bridge/
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

The original shortest path was:

1. Bring up the bridge and lifecycle state.
2. Implement `DataChunk` scan/filter/global agg for Q6.
3. Add grouped aggregation and row sink for Q1.
4. Add inner hash join and date/string predicates for Q3/Q5/Q10/Q14/Q19.
5. Add semi/anti join lowering for Q4/Q16/Q18/Q21/Q22.
6. Add scalar and correlated aggregate subquery rewriting for Q2/Q11/Q15/Q17/Q20/Q22.
7. Add left join and `COUNT(DISTINCT ...)` for Q13/Q16.

The current status against that plan is:

- steps 1 through 3 are done
- step 4 is in progress
  - a minimal two-input inner join plus aggregate path exists
  - the current regression shapes `q12`, `q14`, and `q19` are covered
- step 5 is partially done
  - semi/anti join lowering covers `Q4`, `Q16`, `Q18`, `Q21`, and `Q22`
- step 7 is partially done
  - left join is covered for `Q13`
  - grouped `COUNT(DISTINCT int32)` is covered for `Q16`
  - broader live TPCH join/lowering coverage still needs work
- steps 5 through 7 have not started yet as full features

The current near-term priority is:

1. widen the generic join/lowering path so live TPCH `Q12`, `Q14`, and `Q19`
   stay on the same engine path as planner shapes drift
2. keep reducing interpreter work in hot loops, especially aggregate input
   expression evaluation
3. extend the same live-shape multi-join grouped top-N path from `Q3` to wider
   join graphs such as `Q10` and `Q5`

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

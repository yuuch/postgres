# pg_vec TODO

## Current State

- `Q1` is supported on the generic single-table `SeqScan -> Filter -> Agg`
  path.
- `Q6` uses the same generic single-table path and is now stable on the
  columnar hot path.
- The generic two-input inner-join plus aggregate path now supports both `Q12`
  and `Q14`.
- `Q19` now runs on the same generic two-input join path after lowering
  `Join.joinqual` into the residual join filter IR.
- `Q3` now runs on the generic multi-join grouped top-N path.
- `Q4` now runs after widening join lowering and execution to accept
  planner-produced semi-join shapes in addition to pre-unique inner joins.
- `Q10` now runs on the same generic multi-join grouped top-N path after
  widening join-tree lowering to tolerate commuted bushy inner-join shapes.
- `Q5` now runs on the same generic multi-join grouped aggregate path on live
  TPC-H planner shapes.
- `Q7` now runs on the same generic multi-join grouped aggregate path after
  adding `EXTRACT(YEAR FROM date)` support and deferring multi-join residual
  filters until all referenced inputs are bound.
- `Q8` now runs on the same generic multi-join grouped aggregate path after
  accepting flattened derived-table planner shapes with a conditional ratio
  aggregate in the output layer.
- `Q9` now runs on the same generic multi-join grouped aggregate path with
  contains-`LIKE` filtering and richer arithmetic in the aggregate input.
- `Q11` now runs after lowering aggregate `HAVING` quals into a post-aggregate
  filter IR and resolving uncorrelated `PARAM_EXEC` scalar subqueries into
  constants during translation.
- The translator now accepts live planner shapes with top `FINAL_DESERIAL`
  aggregates and elidable `INITIAL_SERIAL` pre-aggregation wrappers inside the
  join tree.
- The multi-join executor now uses a streaming left-deep hash-probe pipeline
  instead of materializing a full intermediate joined-row set.
- `q1`, `q3`, `q4`, `q5`, `q6`, `q7`, `q8`, `q9`, `q10`, `q11`, `q12`, `q14`,
  `q19`, and `plain_expr`
  regressions now have coverage in-tree.
- The scan path now uses PostgreSQL's `read_stream` sequential path rather than
  manual block walking.
- `DECIMAL(15,2)` deform now has a common-case scale-2 fast path with generic
  fallback.
- Input filters now have a typed `AND + column-vs-constant` fast path over the
  selection vector, with interpreter fallback for richer boolean shapes.
- On the retuned live 10GB instance as of March 26, 2026:
  - `Q1` median improved from `21838.034 ms` to `12630.044 ms` with `pg_vec`
  - `Q6` median regressed from `4152.239 ms` to `4282.139 ms`
  - `Q14` median regressed from `4522.797 ms` to `4896.474 ms`
- The latest live `Q14` flamegraph points primarily at
  `DataChunkDeformer::append_tuple()`, then `evaluate_filter_clause()`, join
  hash lookup, and aggregate input expression evaluation.
- The current string path is still a bring-up design:
  - execution-time scan chunks and materialized join inputs now use
    `StringRef + arena + prefix`
  - grouped result rows and bridge output still keep `String128` as the stable
    boundary format
  - dictionary encoding is not implemented yet
- A conservative single-join late-materialization prototype now exists:
  - aggregate/group-only columns can be marked `late`
  - build-side late columns lazily cache on first access
  - probe-side late columns can fall back to chunk-local tuple copies for
    sparse survivor sets
- On the latest live 10GB retest after the string-column refactor:
  - `Q12` median changed from `7742.314 ms` to `8515.599 ms`
  - `Q14` median changed from `4326.260 ms` to `4629.633 ms`
  - `Q19` median changed from `5746.584 ms` to `8046.090 ms`
- On the latest live 10GB retest after wiring single-join late materialization:
  - `Q12` median changed from `9499.880 ms` to `10163.336 ms`
  - `Q14` median changed from `6255.407 ms` to `7363.543 ms`
  - `Q19` median changed from `7245.564 ms` to `9689.324 ms`
- On the latest live 10GB retest after widening the multi-join path far enough
  for `Q5`:
  - `Q5` median changed from `6786.118 ms` to `8790.786 ms`
- On the latest live 10GB retest after adding grouped `EXTRACT(YEAR FROM
  date)` support and leaf-stage multi-join residual filter rechecks:
  - `Q7` median changed from `6277.694 ms` to `11635.590 ms`
- The next hot interpreter work is aggregate input expression evaluation and
  broader join/lowering coverage for live TPC-H shapes.
  - performance tuning is paused for now in favor of supporting more TPC-H SQL

## Rollout Order

### Stage 1: Join + Derived Expressions

- `Q8` and `Q9` are now supported on the generic multi-join grouped aggregate
  path.

### Stage 3: Semi/Anti Join and Scalar Subqueries

1. `Q15`
    - scalar `MAX` subquery over derived aggregate relation
2. `Q18`
    - `IN (grouped subquery)` plus top-N

### Stage 4: Correlated Aggregate Rewrite

5. `Q17`
    - correlated aggregate subquery rewritten as pre-agg plus join
6. `Q22`
    - `substring(... from 1 for 2)`, scalar `AVG` subquery, anti join

### Stage 5: Highest-Semantics Queries

7. `Q13`
    - `LEFT JOIN` and nested grouping
8. `Q16`
    - `NOT IN`, `COUNT(DISTINCT ...)`, and sort
9. `Q20`
    - nested `IN`, prefix `LIKE`, correlated aggregate rewrite
10. `Q21`
    - `EXISTS` + `NOT EXISTS` with joins and top-N
11. `Q2`
    - multi-join, suffix `LIKE`, scalar `MIN` subquery, top-N

## Immediate Implementation Work

### Performance Follow-up

- Add a fast path for aggregate input expressions on selected rows.
- Extend filter fast paths beyond simple conjunctive comparisons:
  - `OR`
  - `IN (...)`
  - aggregate-level filter predicates
- Redesign string columns for real vectorized execution:
  - finish propagating `StringRef + arena + prefix` beyond scan/join hot paths
  - use `len/prefix` as the first-stage filter, join, and group-by key path
  - add dictionary fast paths for low-cardinality TPC-H columns
- Keep `Q6` at or above parity while preserving the `Q1` win on the same
  engine path.

### Join Coverage Follow-up

- Carry the same live-shape lowering path from `Q8`/`Q9` into the remaining
  semi-join and scalar-subquery queries now that `Q3`, `Q5`, `Q7`, `Q10`, and
  `Q11` accept top finalize aggregates, inner pre-aggregation wrappers, and
  post-aggregate `HAVING`.
- Keep the engine generic:
  - scan + left-deep hash join chain
  - grouped/plain aggregate on joined rows
  - final output expression evaluation in the bridge sink

## Guardrails

- Keep `q1`, `q3`, `q4`, `q5`, `q6`, `q7`, `q8`, `q9`, `q10`, `q11`, `q12`,
  `q14`, `q19`, and `plain_expr` green after every stage.
- Add one regression per newly supported TPC-H query shape before moving on.

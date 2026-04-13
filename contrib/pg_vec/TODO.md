# pg_vec TODO

## Current State

- As of March 31, 2026, all self-contained in-tree SQL fixtures
  `q1..q22` plus `plain_expr` complete successfully on the local 19devel test
  instance.
- As of March 31, 2026, the same self-contained `q1..q22` sweep completes
  with no `pg_vec: fallback to standard executor` warnings.
- `meson test -C build_pg_19dev_install -v pg_vec/regress` is green with
  `23/23` subtests passing.
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
- `Q15` now runs after normalizing join/project boundary `Var` slots down to
  real input attnos, fixing derived grouped-input output mapping, and keeping
  scalar `MAX` subqueries over the derived revenue relation in the post-agg
  filter path.
- `Q13` now runs after preserving derived-aggregate `Agg` boundaries during
  translation, allowing single-input outer aggregation over a derived grouped
  input at execution time, and widening string `LIKE` lowering/execution to
  cover generic `LIKE/NOT LIKE` patterns used by TPC-H.
- `Q18` now runs after accepting direct derived grouped-aggregate join inputs,
  lowering grouped-subquery `HAVING` into the same derived post-agg filter IR,
  and preserving grouped-input sort keys when a `LIMIT` relies on
  `GroupAggregate` output order instead of a top-level `Sort`.
- `Q17` now runs after rewriting the correlated
  `0.2 * avg(...)` scalar subquery into a derived grouped-agg input plus a
  residual compare, and after widening post-agg output typing so
  `sum(decimal) / 7.0` lowers cleanly on the join path.
- `Q22` now runs after lowering `substring(... from 1 for 2)` into a dedicated
  prefix expression opcode, preserving higher-scale numeric constants from the
  scalar `AVG(...)` initplan compare, and accepting single-join anti-join
  planner shapes.
- `Q16` now runs after lowering hashed `NOT IN` membership subplans on base
  scans into semi/anti join inputs inside the join tree, and after teaching
  grouped aggregation to handle `COUNT(DISTINCT int32)` without leaving the
  generic join executor path.
- `Q20` now runs after widening base-scan leaf rewriting so a `SeqScan`
  subquery can combine hashed `IN` membership with a correlated scalar
  aggregate compare that is rewritten into an extra derived grouped-aggregate
  join.
- `Q21` now runs after nested-loop semi/anti join lowering starts extracting
  only the equi-join subset of join quals as hash/probe keys, leaving
  correlated `<>` residual predicates in the join filter path.
- `Q2` now runs after bushy nested-loop inner-join trees are flattened into
  the existing left-deep join IR using join-qual owner-plan context, which
  also lets the correlated scalar `MIN(...)` rewrite lower against the same
  generic project path.
- `Q2`'s correlated `MIN(...)` rewrite now also initializes the derived
  grouped-subplan expression roots correctly, so the nested grouped input no
  longer hits runtime fallback while decoding grouped keys.
- `pg_vec` now raises too-small session `max_stack_depth` settings to `7MB`
  before translation, which removes the remaining default-stack fallback cases
  on this machine such as `Q8` and `Q14`.
- The translator now accepts live planner shapes with top `FINAL_DESERIAL`
  aggregates and elidable `INITIAL_SERIAL` pre-aggregation wrappers inside the
  join tree.
- The multi-join executor now uses a streaming left-deep hash-probe pipeline
  instead of materializing a full intermediate joined-row set.
- `q1`, `q2`, `q3`, `q4`, `q5`, `q6`, `q7`, `q8`, `q9`, `q10`, `q11`, `q12`, `q13`,
  `q14`, `q15`, `q16`, `q17`, `q18`, `q19`, `q20`, `q21`, `q22`, and
  `plain_expr`
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

### Stage 4: Highest-Semantics Queries

1. `Q21`
    - `EXISTS` + `NOT EXISTS` with joins and top-N
    - done
2. `Q2`
    - multi-join, suffix `LIKE`, scalar `MIN` subquery, top-N
    - done

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

- Keep the engine generic:
  - scan + left-deep hash join chain
  - grouped/plain aggregate on joined rows
  - final output expression evaluation in the bridge sink

## Guardrails

- Keep `q1`, `q2`, `q3`, `q4`, `q5`, `q6`, `q7`, `q8`, `q9`, `q10`, `q11`, `q12`,
  `q13`, `q14`, `q15`, `q16`, `q17`, `q18`, `q19`, `q20`, `q21`, `q22`, and
  `plain_expr` green after every stage.
- Add one regression per newly supported TPC-H query shape before moving on.

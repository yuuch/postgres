# pg_vec TODO

## Current State

- `Q1` is supported on the generic single-table `SeqScan -> Filter -> Agg`
  path.
- `Q6` uses the same generic single-table path and is now stable on the
  columnar hot path.
- The minimal inner-join plus aggregate path exists and the `q14` regression is
  green.
- `q1`, `q6`, `q14`, and `plain_expr` regressions are green.
- The scan path now uses PostgreSQL's `read_stream` sequential path rather than
  manual block walking.
- `DECIMAL(15,2)` deform now has a common-case scale-2 fast path with generic
  fallback.
- Input filters now have a typed `AND + column-vs-constant` fast path over the
  selection vector, with interpreter fallback for richer boolean shapes.
- The next hot interpreter work is aggregate input expression evaluation and
  broader join/lowering coverage for live TPC-H shapes.

## Rollout Order

### Stage 1: First Generic Join Path

1. `Q12`
   - reuses join
   - adds grouped aggregate with conditional sums
2. `Q19`
   - reuses join
   - stresses large DNF predicates on top of the same global aggregate shape
3. widen `Q14`
   - keep `Q14` on the same generic join path in live 10GB runs
   - close remaining lowering gaps between the regression shape and the live
     planner shape

### Stage 2: Join + Group + TopN

4. `Q3`
   - multi-join, grouped aggregate, order by aggregate, limit
5. `Q10`
   - same general shape as `Q3`, but wider grouped output
6. `Q5`
   - larger inner-join graph with grouped aggregate

### Stage 3: Join + Derived Expressions

7. `Q7`
   - `EXTRACT(YEAR)` after joins
8. `Q9`
   - contains `LIKE` and richer arithmetic
9. `Q8`
   - derived table plus conditional ratio aggregate

### Stage 4: Semi/Anti Join and Scalar Subqueries

10. `Q4`
    - `EXISTS` to semi join
11. `Q11`
    - grouped aggregate plus scalar aggregate subquery in `HAVING`
12. `Q15`
    - scalar `MAX` subquery over derived aggregate relation
13. `Q18`
    - `IN (grouped subquery)` plus top-N

### Stage 5: Correlated Aggregate Rewrite

14. `Q17`
    - correlated aggregate subquery rewritten as pre-agg plus join
15. `Q22`
    - `substring(... from 1 for 2)`, scalar `AVG` subquery, anti join

### Stage 6: Highest-Semantics Queries

16. `Q13`
    - `LEFT JOIN` and nested grouping
17. `Q16`
    - `NOT IN`, `COUNT(DISTINCT ...)`, and sort
18. `Q20`
    - nested `IN`, prefix `LIKE`, correlated aggregate rewrite
19. `Q21`
    - `EXISTS` + `NOT EXISTS` with joins and top-N
20. `Q2`
    - multi-join, suffix `LIKE`, scalar `MIN` subquery, top-N

## Immediate Implementation Work

### Performance Follow-up

- Add a fast path for aggregate input expressions on selected rows.
- Extend filter fast paths beyond simple conjunctive comparisons:
  - `OR`
  - `IN (...)`
  - aggregate-level filter predicates
- Keep `Q6` at or above parity while preserving the `Q1` win on the same
  engine path.

### Join Coverage Follow-up

- Broaden `Agg <- Join <- SeqScan/SeqScan` lowering to cover live TPCH `Q14`
  planner shapes, not just the current regression shape.
- Reuse that path for `Q12` grouped conditional aggregation.
- Reuse the same path again for `Q19` once large DNF filter support is widened.
- Keep the engine generic:
  - two-input scan + hash join
  - grouped/plain aggregate on joined rows
  - final output expression evaluation in the bridge sink

## Guardrails

- Keep `q1`, `q6`, `q14`, and `plain_expr` green after every stage.
- Add one regression per newly supported TPC-H query shape before moving on.

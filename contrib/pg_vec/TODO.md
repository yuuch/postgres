# pg_vec TODO

## Current State

- `Q1` is supported on the generic single-table `SeqScan -> Filter -> Agg`
  path.
- `Q6` and `plain_expr` regressions are green.
- The current engine is still missing the first generic join stage.

## Rollout Order

### Stage 1: First Generic Join Path

1. `Q14`
   - first practical inner join target
   - requires `INTEGER` join keys, `VARCHAR` prefix `LIKE`, conditional
     aggregate, and final scalar arithmetic over aggregate results
2. `Q12`
   - reuses join
   - adds grouped aggregate with conditional sums
3. `Q19`
   - reuses join
   - stresses large DNF predicates on top of the same global aggregate shape

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

### Q14

- Extend the IR with:
  - multiple scan inputs
  - one generic inner equi-join stage
  - aggregate-level filter predicates
  - output expressions over aggregate results
- Extend physical types with:
  - `INTEGER -> int32`
  - bounded inline strings for TPC-H `VARCHAR/CHAR(n)`
- Extend the translator with:
  - `Agg <- Join <- SeqScan/SeqScan` lowering
  - join key extraction from PG join nodes
  - conditional aggregate rewrite from `SUM(CASE WHEN ... THEN x ELSE 0 END)`
  - prefix `LIKE` lowering
- Extend the engine with:
  - two-input scan + hash join
  - aggregate filter evaluation on joined rows
  - final output expression evaluation in the bridge sink

## Guardrails

- Keep `q1`, `q6`, and `plain_expr` green after every stage.
- Add one regression per newly supported TPC-H query shape before moving on.

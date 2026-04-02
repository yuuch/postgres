# pg_volvec TODO

## Current State

- Direct mapping from PostgreSQL `Plan` nodes to vectorized operators is working for the current supported single-table shapes.
- Vectorized `SeqScan`, `Filter`, and `HashAgg` operators are functional.
- A first in-memory vectorized `Sort` operator is functional for the current full-Q1 shape.
- Column pruning is implemented for scan materialization.
- LLVM JIT deform is live on the Q1 / Q6 hot paths.
- LLVM expression JIT is live and replaces the interpreter on supported programs.
- TPC-H Q1 with and without `ORDER BY`, plus Q6, are verified on `~/data/pg_tpch`.
- Fixed-point `NUMERIC(15,2)` handling is in place with widened aggregation.

## Near-Term Roadmap

### 1. Expression and Aggregation Fusion

- [ ] Add a `dense + no-null` specialized expression kernel.
- [ ] Fuse `sum(expr)` / `avg(expr)` into the aggregate update loop to avoid writing final expression result buffers.
- [ ] Add better observability for expression JIT success / fallback in logs and `EXPLAIN`.

### 2. Operator Coverage

- [ ] Implement vectorized Hash Join for multi-table TPC-H queries.
- [ ] Add `Materialize` / `Limit` handling where they are required by planner output.
- [ ] Generalize `VecSortState` beyond the current single-run in-memory final-sort path.

### 3. Scan Path Improvements

- [ ] Add stronger `no-null` / fixed-layout deform specializations.
- [ ] Explore page-level deform fusion instead of tuple-at-a-time JIT calls.
- [ ] Decide how late materialization should interact with the current deform pipeline.

### 4. Numeric and Type Coverage

- [ ] Expand exact fixed-point coverage beyond the current TPC-H-centric `NUMERIC(15,2)` path.
- [ ] Add more scalar type support in the expression engine.
- [ ] Add string predicate support beyond the current grouping-key / prefix path.
- [ ] Add full string sort support beyond the current short-key / Q1 path.

### 5. Quality

- [ ] Build a repeatable local benchmark harness for Q1 / Q6 and future supported queries.
- [ ] Add regression coverage for JIT-on and JIT-off correctness.
- [ ] Improve fallback behavior reporting when a plan or expression is rejected.

---
name: pg-yaap-dev
description: Build, install, run, test, and debug the pg_yaap optimizer and executor in /Users/chenyunwen/proj/postgres. Use when working in contrib/pg_yaap, validating optimizer-to-executor lowering, or reproducing TPC-H Q1/Q3/Q5/Q6/Q7/Q8/Q9/Q10/Q14 behavior on ~/data/pg_tpch.
---

# pg_yaap Dev

Development skill for `pg_yaap`, the optimizer + executor prototype under `contrib/pg_yaap`.

## Workspace

- Work from `/Users/chenyunwen/proj/postgres`.
- Treat `contrib/pg_yaap` as the active extension directory.
- Treat `build/` as the active Meson build directory.
- Treat `installed/` as the active install prefix.
- Use the existing TPCH instance in `~/data/pg_tpch`.

## Build And Install

Use the top-level Meson build from the repository root:

```bash
meson compile -C build pg_yaap
meson install -C build --only-changed
```

If the local instance needs a restart:

```bash
./installed/bin/pg_ctl -D ~/data/pg_tpch restart -m fast -l ~/data/pg_tpch/logfile
./installed/bin/pg_isready -h /tmp -p 5432
```

## Runtime

Typical debug session:

```sql
LOAD 'pg_yaap';
SET pg_yaap.enabled = on;
SET pg_yaap.parallel = on;
SET pg_yaap.trace_hooks = on;
SET pg_yaap.trace_execution_path = on;
```

## Critical Architecture Rule

When a query is admitted by the YAAP optimizer path, executor lowering must start from the **optimizer physical plan**, not from PostgreSQL's `PlannedStmt` tree.

- `yaap_opt_translator` must directly lower the optimizer `PhysicalOperator` tree into pipeline executor operators.
- The PostgreSQL-plan translator is no longer a supported development target for pg_yaap bring-up work. New fixes should assume the optimizer-owned physical plan is the only authoritative execution input.
- Keep lowering as close as possible to the optimizer-produced plan shape. Do **not** reshape the plan in translator just to force it into an executable form unless there is no viable alternative.
- If DuckDB has a corresponding physical/operator implementation for the needed plan shape, follow DuckDB's approach and add the matching YAAP operator/runtime support instead of encoding the behavior as translator-side rewrites.
- Do **not** treat `pipeline::Translator::Translate(queryDesc, state)` as the implementation for optimizer execution; that path lowers PostgreSQL plans and violates the intended architecture.
- Do **not** debug optimizer-executed queries by assuming PostgreSQL planner/executor shapes are authoritative. The primary object to inspect is the optimizer plan bundle (`OptimizerPlanBundle::physical_plan`) and its lowering into pipeline operators.
- Failures on the optimizer path should be fixed in optimizer support analysis or optimizer-to-executor lowering, not papered over by falling back to PG translator behavior.
- On the optimizer path, do not use `varno/attno` as the authoritative column identity. Prefer `ColumnBinding` and operator output dictionaries propagated from the optimizer plan.

## Repository Constraints

- Keep a single source file under **1000 lines**. If a file grows beyond that, split helpers or lowering logic into smaller modules instead of continuing to append code.
- Apply that rule to `contrib/pg_yaap/src/executor/engine/parallel/pipeline/yaap_opt_translator.cpp` first; it is already far beyond the limit and should be broken into focused files as the lowering path stabilizes.

## Validation Focus

For the current TPCH bring-up work, validate these queries through the YAAP optimizer + executor path:

- `Q1`
- `Q3`
- `Q5`
- `Q6`
- `Q7`
- `Q8`
- `Q9`
- `Q10`
- `Q14`

Separately, run `EXPLAIN` for TPC-H `Q1` through `Q22` on the YAAP optimizer path and review whether the produced optimizer physical plans are broadly reasonable, especially for:

- join order and whether obvious Cartesian products remain after pushdown/rewrite
- filter pushdown into scans or lower join levels
- aggregation placement and whether pre-aggregation is happening where expected
- sort and limit placement

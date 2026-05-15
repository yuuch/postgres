# pg_yaap optimizer-only executor refactor

## Direction

`pg_yaap` no longer treats PostgreSQL's planned tree as an execution input. The only authoritative plan is the YAAP optimizer bundle:

```text
Query -> YAAP LogicalPlanner -> YAAP LogicalOptimizer -> YAAP PhysicalPlanner
      -> optimizer PhysicalOperator tree -> pipeline PhysicalOperator tree
      -> pipeline runtime
```

PostgreSQL still provides the parser, permissions, snapshots, tuple descriptor plumbing, and executor hook lifecycle. Its `PlannedStmt` is only the registry key used to carry the YAAP optimizer bundle from planner hook to executor hook.

## Translator module status

The old PG-plan `pipeline::Translator` path is deprecated and removed from the active Meson build. New work must not revive this path or add PG-plan fallback behavior.

The remaining optimizer-plan lowering code is an implementation bridge from YAAP physical operators to runtime pipeline operators. It should be treated as a pipeline builder, not as a place to reshape plans. If it cannot lower a node because metadata is missing, first inspect whether the optimizer failed to publish the required `ColumnBinding`, output dictionary, names, or operator-specific descriptor.

## Rules during the refactor

1. Do not use `varno/attno` as authoritative identity on the optimizer path. Use `ColumnBinding` and operator output dictionaries.
2. Do not repair unsupported shapes by translating them into another shape in executor lowering.
3. If DuckDB has a matching physical operator, add the corresponding YAAP optimizer/runtime support instead of encoding a translator-side workaround.
4. Lowering failures should report which optimizer-owned metadata is missing.
5. Keep the bridge thin: lookup optimizer bundle, build pipeline, execute pipeline, clean up.

## Current milestone

Temporary target: keep only Q1 and Q6 green while the architecture is simplified.

Validation command shape:

```bash
meson compile -C build pg_yaap
meson install -C build --only-changed
./installed/bin/psql -h /tmp -p 5432 -d tpch -v ON_ERROR_STOP=1 -f contrib/pg_carbon/tests/tpch/q1.sql
./installed/bin/psql -h /tmp -p 5432 -d tpch -v ON_ERROR_STOP=1 -f contrib/pg_carbon/tests/tpch/q6.sql
```

Both queries must run through the optimizer-owned pipeline path and emit `pg_yaap_path: path=pipeline detail=yaap_pipeline` when tracing is enabled.

# Parallel Plan Normalization For `pg_volvec`

Status: partially implemented, refreshed on `2026-04-17`

Current scope:

- transparent `Gather`-style wrapper handling is now part of the real lowering
  path
- validated partial/final aggregate canonicalization is live for the currently
  accepted aggregate family
- grouped finalize paths that depend on preserved ordering can be rebuilt into
  `SimpleAgg (+ Sort)` on the accepted subset
- general `Gather Merge` normalization is still intentionally narrow

This note describes how `pg_volvec` should consume PostgreSQL parallel-aware
plans without requiring PostgreSQL to generate a serial plan first.

The immediate motivation is simple:

- PostgreSQL often chooses a materially better physical shape when parallel
  planning is enabled.
- Those differences are not limited to wrapping a serial subtree in `Gather`.
- If `pg_volvec` only accepts serial plans, we lose both PostgreSQL's better
  search result and our own executor/runtime advantages.

The goal is therefore:

- let PostgreSQL own planning
- let `pg_volvec` normalize accepted parallel plan nodes into its own
  serial/parallel execution IR
- reject unsupported shapes with precise reasons instead of silently falling
  back because of an opaque `nullptr`

## Why This Matters

With parallel planning enabled, PostgreSQL can change the physical shape in
ways that affect performance even if `pg_volvec` later wants to run the query
with its own runtime.

Typical examples:

- `Gather -> Parallel Seq Scan`
- `Finalize Aggregate -> Gather -> Partial Aggregate -> Parallel Seq Scan`
- `Finalize GroupAggregate -> Gather Merge -> Sort -> Partial HashAggregate -> Parallel Seq Scan`

The first case is often just a wrapper around a plan we already know how to
run. The second case is not a mere wrapper and must not be normalized away
casually. The third preserves ordering and needs an explicit merge-aware
implementation.

## Principle

`pg_volvec` should treat PostgreSQL's plan as the source of truth and lower it
into two internal layers:

1. `VecPlanState`
   The existing vector executor tree used for result production, expression
   metadata, slot materialization, and serial fallback.
2. `ParallelPipelinePlan`
   The morsel-driven runtime DAG used when `pg_volvec.parallel=on`.

Normalization therefore has two jobs:

- strip wrappers that are semantically transparent to `pg_volvec`
- stop early at plan shapes whose semantics differ from the serial form

## Node Classes

### 1. Transparent wrappers

These nodes keep the same tuple semantics and mostly forward the child output:

- `Gather`
- `Material`
- `Limit`
- `Sort` as a serial wrapper in the existing vec executor
- `SubqueryScan` when its targetlist is direct-var projection

For this class, normalization is:

- recurse into the child
- keep targetlist / output metadata aligned
- preserve any ordering or projection flags that matter to downstream lowering

`Gather` belongs here for the first implementation slice.

### 2. Semantic split/final nodes

These nodes are not just wrappers:

- `Partial Aggregate`
- `Finalize Aggregate`
- any `Agg` with `aggsplit != AGGSPLIT_SIMPLE`

The important trap is that PostgreSQL's finalize aggregate does not necessarily
consume the original SQL expression tree again. It can consume serialized or
deserialized transition state instead.

In practice, a `Finalize Aggregate` targetlist can contain `Aggref` nodes whose
arguments are `Var`s of transition-state type, such as `bytea`, rather than the
original base-column expression.

That means this transformation is **not** safe by default:

`FinalizeAgg <- Gather <- PartialAgg <- X`

to:

`SimpleAgg <- X`

Doing that correctly requires:

- rebuilding `Aggref` expressions into the simple aggregate form
- proving that the partial/final pair is equivalent to the serial aggregate
- handling serialize/deserialize/combine details per aggregate

Until that canonicalization exists, these shapes should be rejected
deliberately and with a specific reason.

### 3. Ordering-preserving wrappers

`Gather Merge` is similar to `Gather`, but it also preserves a global order
property across worker outputs.

That makes it unsafe to treat as a plain pass-through wrapper. If we normalize
it away without care, downstream operators might observe a different ordering
contract.

For now:

- detect it explicitly
- keep general `Gather Merge` lowering unsupported
- only accept it inside a recognized finalize/partial aggregate chain where we
  can rebuild the aggregate as a simple aggregate and then re-attach an
  explicit post-aggregate sort

## First Implementation Slice

The first safe slice is intentionally narrow:

1. accept `Gather` as a pass-through wrapper in both lowerers
2. extend output-metadata/source-column resolution helpers so `Gather` does not
   break direct-var tracing
3. explicitly recognize `FinalizeAgg <- Gather/GatherMerge <- [Sort/Material] <- PartialAgg`
4. canonicalize safe partial/final aggregate pairs back into the simple
   aggregate form expected by `VecAggState`
5. when the original finalize path was order-preserving, re-attach a synthetic
   post-aggregate `Sort` so result ordering stays aligned with PostgreSQL
6. keep general `Gather Merge` unsupported for now

This already improves behavior because:

- plans that only differ by a `Gather` wrapper no longer fall out of
  `pg_volvec` immediately
- safe partial/final aggregate shapes no longer fall back immediately
- grouped finalize paths keep their ordering semantics instead of silently
  returning hash-table iteration order
- unsupported partial/final shapes still fail in an understandable way

An additional runtime fence now applies during fallback:

- if leader-side `pg_volvec` plan initialization fails and execution falls back
  to PostgreSQL, PostgreSQL-managed parallel workers should not independently
  offload their worker-local subplans into `pg_volvec`

That keeps ownership clear:

- either the leader normalizes and owns the query
- or PostgreSQL owns the query end to end

without a mixed mode where worker-local fragments are opportunistically
vectorized after leader fallback.

## Lowering Rules

### `VecPlanState` lowering

For `Gather`:

- lower `plan->lefttree`
- apply a direct-var projection using `Gather`'s targetlist
- preserve output metadata through the wrapper

For `Agg`:

- if `aggsplit == AGGSPLIT_SIMPLE`, keep existing lowering
- otherwise, try canonicalization first
- if it matches `FinalizeAgg <- Gather/GatherMerge <- [Sort/Material] <- PartialAgg`
  and the partial/final pair is safe, rewrite it to a simple aggregate
- if the original finalize path depended on ordered input/output, wrap the
  canonicalized aggregate in a synthetic `Sort`
- reject the remaining partial/final shapes explicitly

### `ParallelPipelinePlan` lowering

For `Gather`:

- lower the child pipeline
- treat the node as a pass-through wrapper
- keep projection flags if the targetlist is non-empty

For `Gather Merge`:

- reject for now unless it is consumed by a recognized
  `FinalizeAgg <- GatherMerge <- [Sort/Material] <- PartialAgg` chain that can
  be rewritten into `SimpleAgg (+ Sort)`

For `Agg`:

- keep current simple aggregate lowering
- canonicalize supported partial/final aggregate chains into
  `SimpleAgg (+ Sort)` and then lower that plan
- reject the remaining partial/final aggregate modes explicitly

## Next Step After This Slice

The next real milestone is not "support every parallel aggregate plan". The
right next step is:

- broaden the canonicalization safety matrix beyond the currently tested
  grouped `count(*)` / `sum(...)` style paths
- make the accepted aggregate families explicit, especially around transition
  state shape and exact numeric handling
- keep general `Gather Merge` support as a separate problem from this narrow
  finalize/partial rewrite

That keeps the boundary honest and avoids papering over a semantic mismatch by
pretending finalize aggregates are just regular aggregates with a wrapper.

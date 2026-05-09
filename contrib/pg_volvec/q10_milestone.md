# Q10 Milestone Plan

## Goal

Implement TPC-H Q10 through generic pg_volvec capabilities rather than query-specific hard-coding.

Q10 is the forcing function for the next reusable capability tier:

- inner equi-join support
- string/varlen-capable pipeline payloads
- generic expression-backed filter/projection
- mixed-key grouped hash aggregation
- aggregate-output sort / top-N

The target is not “recognize Q10 specially”, but “support the operator and type families that make Q10 and later join-heavy TPC-H queries naturally lowerable”.

## Non-goals

- No table-name-specific or query-shape-specific hacks
- No resurrection of deleted legacy `Vec*State` / old runtime code
- No one-off shortcuts that only work for `customer/orders/lineitem/nation`
- No planner assumptions based on Q10-only functional dependencies in the executor

## Canonical Q10 Shape

```sql
select
	c_custkey,
	c_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	c_acctbal,
	n_name,
	c_address,
	c_phone,
	c_comment
from
	customer,
	orders,
	lineitem,
	nation
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate >= date '1993-10-01'
	and o_orderdate < date '1993-10-01' + interval '3' month
	and l_returnflag = 'R'
	and c_nationkey = n_nationkey
group by
	c_custkey,
	c_name,
	c_acctbal,
	c_phone,
	n_name,
	c_address,
	c_comment
order by
	revenue desc;
```

## Capability Gaps vs Current Greenfield Runtime

Current active runtime only supports:

- `SeqScan -> Agg` with optional top `Sort`
- fixed-width tuple-data layout
- narrow numeric/date/int scan quals
- fixed-width group keys / payloads / output encoding

Q10 additionally needs:

1. string-capable payload/layout/output path
2. generic expression-backed scan filtering (`l_returnflag = 'R'` and later richer string predicates)
3. inner equi-hash join
4. mixed fixed-width + string group keys
5. sorting on aggregate outputs with `DESC`

## Phased Plan

### Milestone A — String/Varlen-Capable Pipeline Substrate

Goal: let the pipeline descriptor/layout/codec/output path represent and move string-ref-backed columns generically.

Scope:

- extend descriptor decode kinds for string-backed columns
- extend tuple-data layout with a string-ref column kind
- teach row/chunk scatter/gather/hash/match paths about string refs
- extend output encoding so pipeline results can emit string-like SQL columns generically

Out of scope:

- joins
- generic expr lowering
- string grouping optimizations beyond correctness

Expected unlocks:

- Q10 string outputs
- future string group keys
- future Q14/Q19/Q3-style workloads

### Milestone B — Generic Expr-Backed Filter/Projection

Goal: stop growing compact qual special cases and route scan/runtime filtering through a reusable expression path.

Scope:

- keep current compact qual path only as a simple fast path if desired
- add expression-backed filter execution for char/text/date/numeric comparisons
- widen projection beyond the current narrow numeric step tape where needed

Expected unlocks:

- `l_returnflag = 'R'`
- future `LIKE` / prefix / richer predicate families

### Milestone C — Generic Inner Equi-Hash Join v1

Goal: add reusable binary join operators rather than a Q10-specific join chain.

Scope:

- inner equi-join only
- fixed-width join keys first
- join-local/global state follows current greenfield descriptor/DSA conventions
- projection pushdown so joins only carry needed columns

Expected unlocks:

- Q10
- Q14
- Q19
- first step toward Q3/Q12

### Milestone D — Mixed-Key Grouped Hash Aggregate

Goal: support grouping on fixed-width plus string-ref columns.

Scope:

- hash/match/scatter/gather over mixed keys
- grouped SUM path stays generic
- no Q10-specific customer-revenue map

Expected unlocks:

- Q10 grouped output
- later Q13-class grouped workloads

### Milestone E — Aggregate-Output Sort / TopN

Goal: sort on aggregate outputs with ordering semantics that are reusable.

Scope:

- aggregate-output sort keys
- `DESC`
- optionally bounded TopN as a reusable operator family

Expected unlocks:

- Q10
- Q3
- future ranking queries

### Milestone F — Translator Admission for Supported Join-Agg-Sort Shapes

Goal: lower Q10 because the generic machinery exists, not because translator recognizes one query.

Scope:

- widen admissible plan families
- map PG join/agg/sort tree into generic pipeline operators
- validate incrementally on subplans before full Q10 enablement

## Execution Order

1. Milestone A
2. Milestone B
3. Milestone C
4. Milestone D
5. Milestone E
6. Milestone F

## Implementation Principles

- Prefer reusable operator/type families over query-specific paths
- Extend current greenfield `PhysicalOperator + MetaPipeline + descriptor-published DSA payload` model
- Use old `pg_vec` and stale docs only as design mines, not as code to resurrect
- Keep each milestone buildable and independently reviewable
- Record progress at the end of this file as work lands

## Progress Log

- 2026-05-08: Created Q10 milestone plan. Execution started with Milestone A, focusing first on descriptor/layout/output support for generic string-ref-backed columns before touching joins.
- 2026-05-08: Milestone A step 1 completed. Added generic `STRING_REF` descriptor support, extended `ColumnSchema` with `typmod`, widened `SeqScan` to decode `text`/`varchar` into `VecStringRef`, and taught `OutputSink` to materialize `text`/`varchar`/`bpchar` from string refs. Deliberately kept row-store string ownership out of scope for this step: `TupleDataLayout` now knows `STRING_REF`, but `tuple_data_ops`/`physical_order` still error if string refs reach row-store HashAgg/Order paths. Verified by rebuilding `pg_volvec` successfully with `meson compile -C build pg_volvec`.
- 2026-05-08: Milestone A step 2 completed. Wired row-store `STRING_REF` ownership through `TupleDataCollection`, `tuple_data_ops`, `HashAggregate`, `PerfectHashAggregate`, `Order`, and `OutputSink`: scatter/gather/hash/match now take the owning TDC when string refs are present, row-store comparisons and ordering resolve string bytes from the TDC heap, and TDC grow/copy paths now re-home string payloads instead of blindly memcpy'ing stale offsets. Also updated all TDC allocation sites to reserve trailing heap space for layouts containing string refs. Verified by rebuilding `pg_volvec` successfully with `meson compile -C build pg_volvec`.
- 2026-05-08: Milestone B filter step completed. Replaced the remaining `QualDescriptor`-driven `PhysicalSeqScan` runtime path with the new descriptor-published filter tape (`FilterInputDesc` / `FilterExprDesc` / `FilterStep` + string-const pool): `translator.cpp` now only lowers/publishes the generic filter vectors, `physical_seq_scan.cpp` now resolves them, builds a filter-specific deform program, evaluates bool-register expressions for integer/numeric/string predicates plus boolean composition, and only then projects survivors. Removed the dead legacy qual helpers/serializers left behind by the swap so the active code path is no longer split across two filter representations. Verified by rebuilding `pg_volvec` successfully with `meson compile -C build pg_volvec`.
- 2026-05-08: Milestone B projection step completed. Widened the live projection tape beyond the earlier narrow revenue-only arithmetic: `translator.cpp` now emits generic numeric scale-alignment and `var ± var` / `var ± const` steps, so nested numeric add/sub trees can lower without depending on operand order hacks, and `physical_projection.cpp` now executes the widened opcode set while propagating nulls instead of forcibly clearing projected outputs to non-null. This keeps the active projection substrate reusable for later join-heavy plans rather than baking in a Q1/Q6-only arithmetic shape. Verified by rebuilding `pg_volvec` successfully with `meson compile -C build pg_volvec`.
- 2026-05-08: Milestone C prep step completed. Before introducing a join operator, refactored the translator's internal column lineage from bare `AttrNumber` tracking to relation-qualified column refs (`varno` + `varattno`) anywhere group keys, aggregate args, projected numeric expressions, output schema derivation, and perfect-hash eligibility were still keyed only by heap attno. This provenance stays translator-local only: it is not serialized into DSA descriptors and does not enter runtime operator contracts, which still consume only lowered `ColumnSchema` / filter / projection / layout artifacts. This preserves current single-scan behavior while removing the ambiguity that would make future generic join lowering unsafe once multiple inputs can each contribute `attno=1`, `attno=2`, etc. Verified by rebuilding `pg_volvec` successfully with `meson compile -C build pg_volvec`.
- 2026-05-08: Milestone C runtime scaffold step completed. Added a new `HASH_JOIN` operator kind to the active pipeline IR, introduced `physical_hash_join.{hpp,cpp}`, extended the descriptor union/enum with a `HashJoinOpBody`, wired leader serialization and worker reconstruction through `pipeline_descriptor.cpp`, and added the new source file to `contrib/pg_volvec/meson.build`. The operator is intentionally scaffold-only at this point: if executed it raises a hard ERROR instead of pretending to support join semantics. Verified by rebuilding `pg_volvec` successfully with `meson compile -C build pg_volvec`.
- 2026-05-08: Milestone C binary-pipeline integration step completed. Reworked `PhysicalHashJoin` from an inert mid-pipeline stub into the first binary-aware pipeline-breaker in the active runtime: it now overrides `BuildPipelines` to keep the left child as the streaming probe side while spawning a child build pipeline for the right side, participates in sink lifecycle (`GetGlobalSinkState` / `GetLocalSinkState` / `SinkChunk` / `Combine` / `Finalize`), and publishes a real `HashJoinSharedPayload` through descriptor fan-out so both build- and probe-side pipeline slots can observe the same DSA state. Probe execution is still intentionally unimplemented and errors loudly, but the runtime shape is now aligned with the current `MetaPipeline`/event/task contracts instead of pretending a binary join can fit in the unary `Execute()` path.
- 2026-05-08: Milestone C build-row materialization step completed. Extended the new join shared state so the build side now persists real rows instead of only counters: each worker allocates a local build-side `TupleDataCollection` using the serialized right-side payload layout, `SinkChunk` scatters incoming build rows into that local TDC, the leader-published registry exposes those local build buffers through DSA, and `Combine` now copies all local build rows into one global build-side TDC stored in `HashJoinSharedPayload`. `Finalize` marks that global build store finalized and resets its scan cursor so later probe-side execution can consume a real, descriptor-published build relation. Probe/hash-table logic is still not implemented yet, but the build side now has durable row-store state instead of placeholder bookkeeping.
- 2026-05-08: Milestone C build-hash-directory step completed. Tightened the join build-side shared state from “finalized row store only” into “finalized row store plus probeable directory”: the global build TDC capacity is now sized for all participating workers instead of just one worker's local cap, and `PhysicalHashJoin::Finalize()` now allocates a bucket-head array plus per-row next-link array in DSA, hashes every finalized build row with `HashGroupRow(...)`, and chains row indices into a power-of-two bucket table published through `HashJoinSharedPayload`. Probe-side matching/output is still intentionally unimplemented, but the build side now exposes the same core artifact a real hash probe will need rather than only a flat list of rows.
- 2026-05-08: Milestone C join-output contract step completed. Before wiring probe execution, extended `HashJoinOpBody` and `PhysicalHashJoin` with an explicit per-output-column mapping contract (`HashJoinOutputColumnDesc[]`), and threaded those new fields through descriptor serialization/reconstruction. This keeps join output ordering in the same translator-owned, descriptor-published lane used elsewhere (`SchemaDescriptor`, sort key descriptors, aggregate layouts) instead of forcing probe execution to guess left-vs-right column placement from `output_schema` alone. Translator does not populate that mapping yet, so probe-side row emission is still deferred to the next step; this change is the contract work that makes that later implementation generic instead of hardcoded.
- 2026-05-08: Milestone C probe execution step completed. `PhysicalHashJoin::Execute()` now probes the finalized build-side bucket/chain directory instead of hard-erroring immediately: build-side rows are split into key and payload TDCs, `Finalize()` hashes the serialized build-key rows into a bucket-head plus next-link directory, and `Execute()` now hashes each probe row, walks the matching build-side chain, verifies join-key equality via `MatchGroup(...)`, and materializes joined output rows into the output `PipelineChunk` through the explicit `HashJoinOutputColumnDesc[]` contract. To keep the runtime generic while translator-side join lowering is still absent, probe also has a temporary schema-derived fallback mapping for the simple concat-output case (`left_schema` columns followed by `right_schema` columns); once translator emits explicit join output mappings, that fallback becomes unnecessary. Verified by rebuilding `pg_volvec` successfully with `meson compile -C build pg_volvec`.
- 2026-05-08: Translator split stabilization step completed. Broke the oversized translator into `translator.cpp` plus `translator_{expr,layout,filter,shape}.cpp` with shared declarations in `translator_internal.hpp`, then fixed the refactor fallout by re-centralizing shared translator declarations (`get_opname`, `rt_fetch`, `ExtractNumericTypmodScale`) at the internal-header boundary instead of duplicating helpers across TUs. Verified by rebuilding `pg_volvec` successfully with `meson compile -C build pg_volvec` after the split.
- 2026-05-08: Milestone C translator lowering step completed for the first generic inner-hash-join slice. The split translator now admits `HashJoin` trees with scan-backed children (including PostgreSQL's build-side `Hash` wrapper), derives side-local `ColumnRef` provenance into ordered left/right scan schemas, lowers explicit left/right key layouts plus payload layouts, and publishes translator-owned `HashJoinOutputColumnDesc[]` mappings instead of relying on the runtime concat fallback. `translator.cpp` now constructs `PhysicalHashJoin` with real side schemas/layouts/output mappings and two child `PhysicalSeqScan`s, while keeping the older scan→agg→sort path intact. This is intentionally still Milestone C scope: it lowers the join subtree itself, not yet the later agg-over-join / full Q10 translator family. Verified by rebuilding `pg_volvec` successfully with `meson compile -C build pg_volvec`.
- 2026-05-08: Clarified the current join-scope boundary after auditing nested-join behavior. The active hash-join slice is explicitly scan-backed only: `HashJoin` children may be `SeqScan` or PostgreSQL's `Hash -> SeqScan`, but nested join/projection/agg children are intentionally rejected in translator admission so the bridge falls back cleanly instead of mis-lowering broader shapes. External reference checks (DuckDB-style engines) confirm that true nested join support wants recursive child translation first and schema-querying second; our current translator is not at that stage yet, so the right near-term behavior is an explicit constraint rather than accidental over-acceptance.
- 2026-05-09: Restored `src/engine/parallel/pipeline/translator.cpp` after it had been deleted, wiring it back to the split translator helpers and current operator constructors so `meson compile -C build pg_volvec` is green again. This is still the pre-recursive path structurally, but it removes the immediate broken-state blocker and gives us a concrete baseline to iterate from instead of a missing TU.
- 2026-05-09: Generalized the row-layout/chunk-slot contract underneath translator lowering. `TupleDataLayout` columns now carry an explicit `src_col_idx`, `tuple_data_ops` and the perfect-hash helpers consume that mapping instead of assuming layout ordinal == chunk slot, and `translator_layout.cpp` now stamps those source slots into group/payload/sort layouts. This change is prerequisite groundwork for recursive nested-join lowering because join-fed Agg/Order pipelines can no longer rely on scan-style column ordering.
- 2026-05-09: Validation status after the restore pass: `meson compile -C build pg_volvec` succeeds. `meson test -C build --suite pg_volvec` still fails for the already-known regress-harness drift (expected output mismatch / warning text drift in `smoke`, `q1`, `q6`), not for a new translator/runtime crash.
- 2026-05-09: Translator admission now accepts the first join-fed aggregate slice without changing the runtime pipeline design. `translator_shape.cpp` no longer hard-rejects `Agg(HashJoin(...))`, group-key extraction now reads the Agg child targetlist generically instead of assuming a direct `SeqScan`, and join output schema/column refs are now reused as the aggregate input contract so `ClassifyAggref(...)`, `BuildHashGroupLayout(...)`, `BuildSortLayouts(...)`, and final output schema derivation can operate on join-produced chunks. `translator_filter.cpp` was updated in the same step so top `Sort` keys resolve by grouped `ColumnRef` rather than raw Agg targetlist position, which keeps sort-layout construction aligned once the Agg input is no longer scan-backed. Verified by rebuilding `pg_volvec` successfully with `meson compile -C build pg_volvec`.
- 2026-05-09: Follow-up translator-only lineage step completed. The earlier admission widening exposed PostgreSQL's upper-plan rewrite semantics: above a join, Agg-visible Vars arrive as `OUTER_VAR`/child-resno references rather than base-relation `(varno, attno)` pairs. Kept the fix entirely in the translator by teaching `translator_expr.cpp` to resolve special Vars through the relevant child plan targetlist (`OUTER_VAR -> lefttree`, `INNER_VAR -> righttree`) before converting them into translator-local `ColumnRef`s. `translator_shape.cpp` now uses that resolver for group-column extraction and Agg targetlist validation, and `translator_filter.cpp` uses the same resolver for top Sort keys. This keeps runtime/operator contracts unchanged while making join-fed Agg/Sort analysis respect PostgreSQL's actual plan-shape semantics. Verified by rebuilding `pg_volvec` successfully with `meson compile -C build pg_volvec`.
- 2026-05-09: Extended the same translator-only lineage model down into the join subtree itself so join lowering and agg-over-join lowering no longer use different Var assumptions. `translator_shape.cpp` now resolves `HashJoin` hash-clause keys and join output targetlist entries through the join plan's child targetlists before turning them into `ColumnRef`s, which matches PostgreSQL's `fix_join_expr(...)` rewrite to `OUTER_VAR`/`INNER_VAR`. This keeps the scan-backed join scope unchanged, but it removes another source of direct-Var assumptions from translator admission and makes later nested-join work build on one consistent lineage path. Verified by rebuilding `pg_volvec` successfully with `meson compile -C build pg_volvec`.
- 2026-05-09: Started the first structural recursive-translation step in `translator.cpp` / `translator_shape.cpp`. The translator no longer requires HashJoin children to be represented as scan-specific shape fields before build time: `translator_shape.cpp` now derives child output refs/schema through a generic `AnalyzePlanOutput(...)` path that can recurse over `Hash` wrappers and nested `HashJoin` children, and `translator.cpp` now builds join inputs through a new subtree builder (`SeqScan` / `HashJoin` today) instead of hardcoding two fresh `PhysicalSeqScan`s inside the parent join branch. This is still an incremental step — top-level shape admission is not yet a fully general recursive translator for every node kind — but it moves the design from scan-backed join assembly toward child-first operator translation while keeping runtime contracts unchanged. Verified by rebuilding `pg_volvec` successfully with `meson compile -C build pg_volvec`.
- 2026-05-09: Continued the same translator refactor upward into the aggregate/output layer. `translator.cpp` now carries the recursively built child subtree's analyzed `ColumnRef`/`ColumnSchema` output forward as the active input contract for later stages, and uses that contract for group-key chunk-slot lookup plus final aggregate output-schema construction when the child is a join subtree. This still leaves `SupportedPlanShape` in place for agg/sort admission and precomputed layouts, but it removes another legacy assumption that join-fed aggregation must rebuild its output contract only from the old flat shape snapshot. Verified by rebuilding `pg_volvec` successfully with `meson compile -C build pg_volvec`.
- 2026-05-09: Moved the core join-fed aggregate metadata derivation into the live builder path. For `Agg(HashJoin(...))`, `translator.cpp` now recomputes `agg_funcs`, numeric scales, projection tapes, perfect-hash eligibility, hash layout, sort layouts, and final output layout from the recursively built child subtree's `current_cols/current_schema` instead of trusting the join-path copies precomputed in `SupportedPlanShape`. This keeps the old shape data available for admission during the transition, but the active build path is now substantially more child-output-driven than shape-driven. Verified by rebuilding `pg_volvec` successfully with `meson compile -C build pg_volvec`; `meson test -C build --suite pg_volvec` still shows only the pre-existing regress-harness drift (`smoke/q1/q6` expected-output mismatch / serial warning text), not a new translator failure.
- 2026-05-09: Removed the redundant join-fed aggregate precompute from `translator_shape.cpp`. For the `HashJoin -> Agg` path, shape extraction now stops after semantic validation of group columns / aggregate refs / sort keys plus exposure of the child output contract; it no longer precomputes perfect-hash choice, aggregate function descriptors, projection tapes, or hash/sort layouts for that path. Those artifacts now come from the recursive builder in `translator.cpp`, leaving only one active source of truth for join-fed agg metadata. Verified by rebuilding `pg_volvec` successfully with `meson compile -C build pg_volvec`; `meson test -C build --suite pg_volvec` still shows only the pre-existing regress-harness drift (`smoke/q1/q6` expected-output mismatch / serial warning text).
- 2026-05-09: Continued shrinking the join-fed role of `SupportedPlanShape`. `translator_shape.cpp` no longer copies the join child output contract into the legacy `input_cols` / `input_columns` slots just to validate `HashJoin -> Agg`; join-fed validation now checks group columns and aggregate argument refs directly against `hash_join_output_cols` / `hash_join_output_schema_columns`, while the scan-only path keeps using the old `input_*` plumbing. This is a smaller cleanup than the earlier builder refactor, but it reduces another transitional join-path dependency on the legacy shape fields without changing runtime behavior. Verified by rebuilding `pg_volvec` successfully with `meson compile -C build pg_volvec`; `meson test -C build --suite pg_volvec` still shows only the pre-existing regress-harness drift (`smoke/q1/q6` expected-output mismatch / serial warning text).
- 2026-05-09: Closed the next generic Q10 translator gap around post-agg ordering/output shape without hardcoding the query. `translator_filter.cpp` now allows top `Sort` keys to target either grouped columns or aggregate outputs from the Agg targetlist, `translator_layout.cpp` now builds sort-key layouts against generic post-agg payload slots (including aggregate slots) and derives a final output schema/layout in Agg targetlist order for join-fed paths, and `translator.cpp` now uses that final output contract for the join-fed sink instead of forcing `[groups..., aggs...]`. In the same step, `pipeline_descriptor.cpp` + `physical_order.{hpp,cpp}` were tightened so `SortKeyDesc[]` is descriptor-published/reconstructed and `PhysicalOrder::Finalize()` actually respects `SortKeyDesc.asc`, which fixes the previously silent “always ascending” runtime behavior for `ORDER BY ... DESC`. Also corrected `ColumnNumericScale(...)` so string/double group/output columns carry a valid zero scale through layout building instead of being rejected as non-numeric. Verified by rebuilding `pg_volvec` successfully with `meson compile -C build pg_volvec`; `meson test -C build --suite pg_volvec` still shows only the pre-existing regress-harness drift in `smoke/q1/q6` expected outputs / warning text, with no new translator/runtime crash signal.
- 2026-05-09: Re-ran a focused Q10 gap analysis against the live plan and current code instead of relying on older assumptions. Live `EXPLAIN` still shows `WARNING: pg_volvec: unsupported plan shape, falling back to standard PostgreSQL executor` on the current repository Q10, with the planner producing `Sort -> HashAggregate -> HashJoin -> HashJoin -> HashJoin` over `orders`, `lineitem`, `customer`, and `nation`. The code-level/runtime synthesis from that analysis narrowed the remaining blockers to: (1) grouped `STRING_REF` hashing in `HashAgg`, because canonical Q10 groups by multiple string columns; (2) very small runtime row-cap defaults in `HashJoin` (`1024`) and `Order` (`256`); and only after those, (3) remaining translator admission narrowness for planner-added wrappers/variants.
- 2026-05-09: Landed the first blocker from that analysis. `tuple_data_ops.cpp` `HashGroup(...)` now hashes chunk-side `STRING_REF` group keys using the same `prefix + byte stream` scheme already used by `HashGroupRow(...)`, instead of hard-erroring on any string key. This is a runtime correctness unblocker for generic Q10-style grouped text keys (`c_name`, `n_name`, `c_address`, `c_phone`, `c_comment`) and keeps chunk-side probe hashing aligned with row-store hashing/matching semantics. Verified by rebuilding `pg_volvec` successfully with `meson compile -C build pg_volvec`; `meson test -C build --suite pg_volvec` still shows only the pre-existing regress-harness drift in `smoke/q1/q6` expected outputs / serial-warning text, with no new crash signal. The next recommended work item remains removing the fixed row caps in `HashJoin` and `Order`, then re-checking whether admission widening is still needed for the live Q10 planner shape.
- 2026-05-09: Cleared the next live Q10 runtime blocker in the storage/runtime layer rather than the translator. The failure `pg_volvec: STRING_REF row-store scatter failed` turned out to be a grouped insert/grow-policy bug: `AggregateHashTableFindOrInsertBatch(...)` could append a new row as long as row slots remained, even when the backing `TupleDataCollection` string heap was already exhausted. Fixed this generically by (1) making TDC heap reservation synchronized (`heap_used` is now atomic and `TupleDataCollectionStoreStringBytes(...)` reserves under the TDC mutex), (2) adding reusable helpers to compute required heap bytes for a candidate chunk row / row-store row plus a `TupleDataCollectionHasSpaceForAppend(...)` contract, and (3) teaching `PhysicalHashAggregate` and batch AHT insert to grow on heap pressure before new-group insertion, not only on `row_count >= row_capacity`. Verified with `meson compile -C build pg_volvec`, `meson install -C build --only-changed`, `./installed/bin/pg_ctl -D ~/data/pg_tpch restart -m fast -l ~/data/pg_tpch/logfile`, and a live `psql -f contrib/pg_volvec/tpch_queries/q10.sql`: Q10 now runs end-to-end through the admitted `Sort -> HashAggregate -> Hash Join -> Hash Join -> Hash Join` path instead of failing during grouped string scatter. `meson test -C build --suite pg_volvec` still shows only the known `smoke/q1/q6` expected-output / serial-warning drift.
- 2026-05-09: Hardened the hash-join runtime against the next generic Q10-scale row-store limit. `PhysicalHashJoin` had still been relying on fixed-capacity local/global build-side `TupleDataCollection`s (defaulting from `max_rows=1024`) with no grow path, so larger string-heavy join inputs could still hit the same `STRING_REF row-store scatter failed` class of failure even after the aggregate fix. Added reusable join-side TDC growth in `physical_hash_join.cpp`: local sink and global combine now check both row-slot and string-heap pressure before append, copy existing build rows into larger TDCs when needed, and serialize global build-store growth under a new `HashJoinSharedPayload::mutex`. Verified by rebuilding/installing/restarting and then running the admitted join-agg-sort Q10 path with hook tracing (`SET pg_volvec.trace_hooks = on` plus the normalized `< DATE '1994-01-01'` upper bound): the query is now admitted and runs through pg_volvec without the previous runtime crash (`ExecutorRun hook completed plan in pg_volvec`). The remaining canonical-Q10 blocker is now narrower and translator-facing: `contrib/pg_volvec/tpch_queries/q10.sql` still falls back because the planner-folded `date '1993-10-01' + interval '3' month` filter is not yet normalized into the accepted date/timestamp compare form for this join-fed path. `meson test -C build --suite pg_volvec` remains at the same known `smoke/q1/q6` expected-output / serial-warning drift.
- 2026-05-09: Cleared the canonical-Q10 `invalid DSA memory alloc request size 1442840616` blocker without hardcoding Q10. Split HashAgg's planner group estimate from its startup allocation (`EstimateHashAggInitialGroups(...)`, capped at 8192 groups) so grouped Q10 can grow instead of preallocating from a huge `plan_rows` estimate. Applied the same startup-vs-growth policy to post-agg `Order` and `OutputSink` TDC allocation, adding reusable grow/copy paths for string-bearing TDC rows and making descriptor-published payload pointers authoritative after growth. The identical DSA request persisted until the join translator stopped blindly building on PostgreSQL's right child: for inner joins, translator now assigns the smaller estimated child to the runtime build side while preserving the generic explicit output-mapping contract. This moved Q10 past the previous DSA failure.
- 2026-05-09: Fixed the follow-up swapped-join mapping regression. The first smaller-build run failed with `pg_volvec: hash join output column mapping missing right payload column` because the side labels were flipped twice after passing probe/build columns into `BuildHashJoinOutputMappings(...)`. Removed the redundant remap so mapping sides consistently mean runtime `LEFT=probe` / `RIGHT=build`. Verified with `meson compile -C build pg_volvec`, install, restart, and canonical Q10 with hook tracing: the query is admitted and no longer hits the DSA allocation or mapping errors. Current remaining blocker is execution completion/worker lifecycle: `EXPLAIN ANALYZE` progressed through the Q10 scan/join phases but exceeded the 240s tool timeout, leaving the leader waiting on `BgworkerShutdown`; the instance was restarted cleanly after cancelling the stuck backend. Next step is to profile/diagnose the post-scan pipeline stall or slow path, not admission or oversized allocation.
- 2026-05-09: Tightened generic pipeline lifecycle/cancellation behavior after the Q10 timeout investigation. Root `RunEvent`s now enter the same `PENDING -> SCHEDULED` transition path as dependency-unblocked events (`Event::TrySchedule()`), so event observability/lifecycle no longer has a root-only `PENDING -> FINISHED` shortcut. Added a reusable pipeline cancellation helper and inserted cooperative shutdown/interrupt polling at safe boundaries in `PhysicalSeqScan`, `PhysicalHashJoin`, `PhysicalHashAggregate`, and `PhysicalOrder`, including bounded combine batches outside spinlock-held polling sites. Verified `meson compile -C build pg_volvec`, install, restart, and canonical Q10 with `statement_timeout='5s'`: the client now receives the normal `ERROR: canceling statement due to statement timeout`. However, follow-up inspection found one worker could remain alive with an 84GB physical footprint before being terminated, and longer Q10 still triggers OS-level worker kill / postmaster recovery. So the lifecycle bug is narrowed, but canonical Q10 is not correct/complete yet; the current blocker is runaway memory/materialization in the join-agg-sort pipeline, not translator admission or root event state.
- 2026-05-09: Implemented the first generic last-consumer DSA lifetime slice for HashJoin build payloads. `HashJoinSharedPayload` now has an idempotent `release_state`, its local build registry records both per-worker key and payload TDCs, and the leader runs a non-virtual post-consumer-RUN cleanup dispatcher before `FinishEvent()` dependency fan-out. The only enabled cleanup branch is `PhysicalHashJoin` as a probe-side `OPERATOR`, so build-pipeline `SINK` state is not freed early and HashAgg/Order/Output sink payloads remain untouched until their own last-consumer mapping is designed. `PhysicalHashJoin::ReleaseBuildPayloadAfterConsumerRun()` now frees global/local build TDCs plus hash bucket/link arrays, clears descriptor fan-out, and guards future probe attempts with `pg_volvec: hash join payload used after release`. Verified `meson compile -C build pg_volvec`, install, restart, Q1/Q6 smoke scripts, and canonical Q10 with `statement_timeout='5s'` plus `pg_volvec.trace_execution_path=on`: Q10 is admitted, logs a `pg_volvec hashjoin release ...` line, then exits with normal statement timeout and no payload-use-after-release or backend crash. Added `sql/nested_hashjoin_lifetime.sql` for a small native correctness harness; it currently still falls back through native PG for the pg_volvec run because the tiny plan shape is rejected above HashJoin, so Q10 remains the effective cleanup-path validation. `meson test -C build --suite pg_volvec` still shows only the known `smoke/q1/q6` expected-output / serial-warning drift, not a new runtime crash.
- 2026-05-09: Closed two generic memory/lifecycle leaks exposed by canonical Q10. First, all current copy-on-grow row stores now reclaim superseded DSA generations after a successful pointer swap: HashJoin build TDCs, HashAgg local/global partition TDCs plus resized AHTs, Order TDCs, and Output TDCs. HashJoin local build registry entries are also updated after local grow so post-consumer cleanup can still release the final generation. Second, timeout/error cleanup no longer relies only on interruptible cooperative worker shutdown: the leader's `PG_CATCH` path now terminates bgworkers and waits with a non-interruptible shutdown loop, while workers also exit if their leader PID disappears. Verified `meson compile -C build pg_volvec`, install, restart, Q1 and Q6 parallel smokes, and canonical Q10 with `statement_timeout='60s'`: Q10 no longer leaves an orphan `pg_volvec worker` or forces postmaster recovery, but it now cleanly reports the next blocker, `pg_volvec worker reported failure: invalid DSA memory alloc request size 1879048232`. Server health remains good after the error and the process list shows no lingering `pg_volvec worker` processes.
- 2026-05-09: Converted the large canonical-Q10 DSA failures into structured pg_volvec diagnostics and fixed the next cleanup hang. `TupleDataCollectionCheckedAllocSize(...)` now guards flat TDC allocations, exposing the original huge request as `TDC flat allocation exceeds limit` rather than an untyped DSA error. `TupleDataLayoutAddColumn(...)` now advances by actual aligned physical width, fixing `STRING_REF` row overlap, and HashJoin/HashAgg/Order/Output allocation sites use the checked helper. HashJoin now receives the translator's selected build-side row estimate, treats it as total build rows rather than multiplying it by worker count, and caps only the initial flat allocation to 1M rows so estimates guide startup without forcing >1GB preallocation. This moved Q10 from the old `rows=117440512 row_width=16` failure to a later real grow-pressure failure, `rows=45860864 row_width=24`, which maps to a three-fixed-column lineitem/intermediate payload. Also replaced the `terminate_workers=true` final join in `SignalShutdownAndWait()` with PostgreSQL's interruptible `WaitForBackgroundWorkerShutdown(...)`, so statement-timeout cleanup no longer strands the leader in `IPC/BgworkerShutdown`. Verified rebuild/install/restart, Q1 fixture, Q6 TPCH (`1230113636.0101`), and a short-timeout Q10 returning promptly with the structured TDC guard and no stuck backend. Current blocker is now join cardinality/materialization blow-up: the second join is actually growing a 24-byte build payload to ~45.8M rows, far above the native Q10 estimate, so the next investigation is key/filter/output cardinality rather than admission or cleanup.
- 2026-05-09: Finished the canonical Q10 memory/correctness chain through the active pg_volvec pipeline. Native PostgreSQL `EXPLAIN ANALYZE` established the ground-truth intermediate rows: filtered `orders` 573,157; filtered `lineitem` 14,808,183; each Q10 hash join output 1,147,084 rows; final HashAggregate 381,105 groups. Trace-gated HashJoin counters showed the apparent `rows=45860864 row_width=24` failure was requested TDC capacity, not actual cardinality; the bottom join emitted ~1.0M rows before the parent build grew, matching native scale. The real bug was stale cross-worker global HashJoin TDC generations: one worker could grow/free the shared build TDC while another still checked capacity through an old `HashJoinGlobalSinkState` pointer, causing bogus growth decisions. `EnsureJoinGlobalCapacity()` and `Finalize()` now re-resolve global build TDCs from `HashJoinSharedPayload` before use. Follow-up blockers were fixed generically: HashAgg local partitions now pre-grow for the whole per-partition input batch before `AggregateHashTableFindOrInsertBatch()` can append many new groups, and `Gather()` now re-homes row-store `STRING_REF` values into the destination `DataChunk` string arena instead of copying DSA heap offsets into chunk refs. Verified `meson compile -C build pg_volvec`, install, restart, Q1 fixture, Q6 TPCH (`1230113636.0101`), and exact canonical Q10 `EXPLAIN (ANALYZE, TIMING OFF, BUFFERS OFF, SUMMARY ON)` with `pg_volvec.parallel=on`, `parallel_max_workers=14`, PostgreSQL parallelism disabled: Q10 completes through pg_volvec in ~2.58s with no `pg_volvec worker reported failure`, no TDC flat allocation error, no string scatter failure, and no lingering worker/backend afterward.
- 2026-05-09: Closed the remaining HashJoin partial-output contract debt generically. `PipelineRunTask` now drains an operator suffix recursively for each source chunk, so an operator returning `HAVE_MORE_OUTPUT` is re-entered on the same logical input until it returns `NEED_MORE_INPUT` before the scheduler fetches the next source chunk. `PhysicalHashJoin` now persists probe-row and build-chain cursors in its `OperatorState`, allowing it to resume mid-bucket when one probe chunk expands to multiple output chunks; pass-through operators (`Projection`, `HashAggregate`, `PerfectHashAggregate`) now explicitly acknowledge the same re-entry contract by returning `NEED_MORE_INPUT` on the immediate second call for a fully consumed input. Verified `meson compile -C build pg_volvec`, install, restart, Q1 fixture correctness, Q6 TPCH (`1230113636.0101`), canonical Q10 `EXPLAIN ANALYZE` through pg_volvec in ~2.68s, limited Q10 output rows, and wrapped Q10 grouped row count `381105`, matching the native ground truth. No lingering `pg_volvec worker` backends remained after validation.
- 2026-05-09: Ran the requested q1/q6/q10 benchmark matrix and recorded the results under `contrib/pg_volvec/benchmarks/q1_q6_q10_pg_vs_volvec_20260509_173347.tsv` and `.log`. Median times: q1 `pg_parallel` 4073 ms vs `volvec_parallel` 2059 ms; q6 `pg_parallel` 2057 ms vs `volvec_parallel` 2059 ms; q10 `pg_parallel` 3048 ms vs `volvec_parallel` 3072 ms. The q10 run is now stable after the HashJoin drain fix, and the benchmark output is the current source of truth for this comparison.

- 2026-05-09: Re-ran the q1/q6/q10 benchmark matrix and recorded a second result set under:
  - `contrib/pg_volvec/benchmarks/q1_q6_q10_pg_vs_volvec_20260509_175234.tsv`
  - `contrib/pg_volvec/benchmarks/q1_q6_q10_pg_vs_volvec_20260509_175234.log`

  Median times: q1 `pg_parallel` 4065 ms vs `volvec_parallel` 2045 ms; q6 `pg_parallel` 1031 ms vs `volvec_parallel` 1036 ms; q10 `pg_parallel` 3048 ms vs `volvec_parallel` 2043 ms.

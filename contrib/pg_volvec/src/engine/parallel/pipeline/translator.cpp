#include "parallel/pipeline/translator.hpp"

extern "C" {
#include "postgres.h"
#include "executor/execdesc.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/plannodes.h"
}

#include "parallel/pipeline/output_sink.hpp"
#include "parallel/pipeline/physical_hash_aggregate.hpp"
#include "parallel/pipeline/physical_hash_join.hpp"
#include "parallel/pipeline/physical_order.hpp"
#include "parallel/pipeline/physical_perfect_hash_aggregate.hpp"
#include "parallel/pipeline/physical_projection.hpp"
#include "parallel/pipeline/physical_seq_scan.hpp"
#include "parallel/pipeline/translator_internal.hpp"

namespace pg_volvec {
namespace pipeline {

namespace translator_detail {

namespace {

static dsa_pointer
BuildFilterArray(dsa_area *dsa, const void *data, size_t elem_size, size_t count)
{
	if (count == 0)
		return InvalidDsaPointer;
	dsa_pointer dp = dsa_allocate0(dsa, elem_size * count);
	if (!DsaPointerIsValid(dp))
		return InvalidDsaPointer;
	std::memcpy(dsa_get_address(dsa, dp), data, elem_size * count);
	return dp;
}

static dsa_pointer
BuildCharArray(dsa_area *dsa, const std::vector<char> &bytes)
{
	if (bytes.empty())
		return InvalidDsaPointer;
	dsa_pointer dp = dsa_allocate0(dsa, bytes.size());
	if (!DsaPointerIsValid(dp))
		return InvalidDsaPointer;
	std::memcpy(dsa_get_address(dsa, dp), bytes.data(), bytes.size());
	return dp;
}

static dsa_pointer
BuildOutputTdc(dsa_area *dsa,
		       dsa_pointer layout_dp,
		       const TupleDataLayout &layout,
		       uint32_t row_capacity)
{
	const uint32_t heap_capacity = TupleDataCollectionDefaultHeapCapacity(&layout, row_capacity);
	dsa_pointer payload_dp = dsa_allocate0(dsa,
		TupleDataCollectionCheckedAllocSize(row_capacity, layout.row_width, heap_capacity));
	if (!DsaPointerIsValid(payload_dp))
		return InvalidDsaPointer;
	auto *tdc = static_cast<TupleDataCollection *>(dsa_get_address(dsa, payload_dp));
	TupleDataCollectionInit(tdc, row_capacity, layout.row_width, layout_dp, heap_capacity);
	return payload_dp;
}

static uint32_t
EstimateOutputRows(QueryDesc *qd)
{
	double plan_rows = 1024.0;
	if (qd != nullptr && qd->plannedstmt != nullptr && qd->plannedstmt->planTree != nullptr)
		plan_rows = qd->plannedstmt->planTree->plan_rows;
	const double cap_d = std::max(1024.0,
		std::min(static_cast<double>(1u << 20), plan_rows * 1.5));
	return static_cast<uint32_t>(cap_d);
}

static uint32_t
EstimateResultRows(QueryDesc *qd, uint32_t estimated_groups)
{
	if (estimated_groups > 0)
		return std::max<uint32_t>(1024u, std::min<uint32_t>(1u << 20, estimated_groups));
	return EstimateOutputRows(qd);
}

static uint32_t
EstimateInitialResultRows(QueryDesc *qd, uint32_t estimated_groups)
{
	constexpr uint32_t kMaxInitialRows = 8192u;
	return std::min<uint32_t>(kMaxInitialRows, EstimateResultRows(qd, estimated_groups));
}

static double
PlanEstimatedRows(Plan *plan)
{
	while (plan != nullptr && nodeTag(plan) == T_Hash)
		plan = plan->lefttree;
	if (plan == nullptr || plan->plan_rows <= 0.0)
		return 1024.0;
	return plan->plan_rows;
}

static uint32_t
EstimateHashJoinBuildRows(Plan *plan)
{
	constexpr double kMinRows = 1024.0;
	constexpr double kMaxRows = static_cast<double>(1u << 26);
	const double rows = PlanEstimatedRows(plan);
	const double with_margin = std::max(kMinRows, rows * 1.25);
	return static_cast<uint32_t>(std::min(kMaxRows, with_margin));
}

struct DerivedAggBuildState {
	std::vector<AggFuncDesc> agg_funcs;
	std::vector<TdcAggKind> agg_kinds;
	std::vector<int16_t> agg_numeric_scales;
	std::vector<ProjectStep> project_steps;
	std::vector<ProjectExprDesc> project_exprs;
	TupleDataLayout hash_layout;
	TupleDataLayout sort_key_layout;
	TupleDataLayout sort_payload_layout;
	TupleDataLayout final_output_layout;
	std::vector<ColumnSchema> final_output_schema;
	TupleDataLayout final_project_layout;
	std::vector<ColumnSchema> final_project_schema;
	uint32_t perfect_hash_capacity = 0;
};

struct NodeTranslation {
	std::unique_ptr<PhysicalOperator> op;
	std::vector<ColumnRef> raw_cols;
	std::vector<ColumnSchema> raw_schema;
	Agg *agg = nullptr;
	Sort *sort = nullptr;
	uint32_t estimated_groups = 0;
	std::vector<ColumnRef> group_cols;
	std::vector<Aggref *> aggrefs;
	std::vector<ColumnRef> agg_input_cols;
	std::vector<ColumnSchema> agg_input_schema;
	DerivedAggBuildState agg_state{};
	std::vector<ColumnSchema> final_output_schema;
	TupleDataLayout final_output_layout{};
	bool has_output_contract = false;
};

static void
InitializeChunkSlotWatermarks(const std::vector<ColumnSchema> &columns,
			      uint8_t &next_int32_slot,
			      uint8_t &next_int64_slot,
			      uint8_t &next_double_slot)
{
	next_int32_slot = 0;
	next_int64_slot = 0;
	next_double_slot = 0;
	for (const ColumnSchema &col : columns)
	{
		switch (col.decode_kind)
		{
			case ColumnDecodeKind::INT32_CHAR:
			case ColumnDecodeKind::INT32_DATE:
			case ColumnDecodeKind::INT32_INT4:
			case ColumnDecodeKind::STRING_REF:
				next_int32_slot = std::max<uint8_t>(next_int32_slot,
					static_cast<uint8_t>(col.chunk_slot + 1));
				break;
			case ColumnDecodeKind::INT64_INT8:
			case ColumnDecodeKind::INT64_NUMERIC_SCALED:
				next_int64_slot = std::max<uint8_t>(next_int64_slot,
					static_cast<uint8_t>(col.chunk_slot + 1));
				break;
			case ColumnDecodeKind::DOUBLE_FLOAT8:
				next_double_slot = std::max<uint8_t>(next_double_slot,
					static_cast<uint8_t>(col.chunk_slot + 1));
				break;
			case ColumnDecodeKind::NONE:
				break;
		}
	}
}

static bool
DeriveAggBuildState(Agg *agg,
		    Sort *sort,
		    const std::vector<ColumnRef> &group_cols,
		    const std::vector<Aggref *> &aggrefs,
		    const std::vector<SortKeyDesc> &sort_keys,
		    const std::vector<ColumnRef> &current_cols,
		    const std::vector<ColumnSchema> &current_schema,
		    DerivedAggBuildState &out)
{
	if (agg == nullptr)
		return false;
	uint8_t next_int32_slot = 0;
	uint8_t next_int64_slot = 0;
	uint8_t next_double_slot = 0;
	InitializeChunkSlotWatermarks(current_schema, next_int32_slot, next_int64_slot, next_double_slot);
	std::vector<MaterializedProjectExpr> materialized_exprs;
	out = DerivedAggBuildState{};
	if (!group_cols.empty())
		(void) TryBuildPerfectHashSpec(group_cols, current_cols, current_schema, out.perfect_hash_capacity);
	for (Aggref *aggref : aggrefs)
	{
		AggFuncDesc desc{};
		TdcAggKind kind = TdcAggKind::COUNT_STAR;
		int16_t numeric_scale = 0;
		if (!ClassifyAggref(aggref,
				current_cols,
				current_schema,
				&agg->plan,
				out.project_steps,
				out.project_exprs,
				materialized_exprs,
				next_int64_slot,
				desc,
				kind,
				numeric_scale))
			return false;
		out.agg_funcs.push_back(desc);
		out.agg_kinds.push_back(kind);
		out.agg_numeric_scales.push_back(numeric_scale);
	}
	if (!BuildHashGroupLayout(group_cols,
			current_cols,
			current_schema,
			out.agg_funcs,
			out.agg_kinds,
			out.agg_numeric_scales,
			out.hash_layout))
		return false;
	if (sort != nullptr && !BuildSortLayouts(group_cols,
			current_cols,
			current_schema,
			out.agg_funcs,
			out.agg_kinds,
			out.agg_numeric_scales,
			sort_keys,
			out.sort_key_layout,
			out.sort_payload_layout))
		return false;
	if (!BuildAggFinalOutput(agg,
			group_cols,
			current_cols,
			current_schema,
			aggrefs,
			out.agg_kinds,
			out.agg_numeric_scales,
			out.final_output_schema,
			out.final_output_layout))
		return false;
	return true;
}

static std::unique_ptr<PhysicalOperator>
BuildAggOperator(Agg *agg,
			 const std::vector<ColumnRef> &current_cols,
			 const std::vector<ColumnSchema> &current_schema,
			 const std::vector<ColumnRef> &group_cols,
			 const DerivedAggBuildState &agg_state,
			 uint32_t estimated_groups,
			 uint32_t initial_groups,
			 std::unique_ptr<PhysicalOperator> child,
			 PgVolVecQueryState *state)
{
	if (agg == nullptr || state == nullptr || state->runtime_dsa == nullptr || child == nullptr)
		return nullptr;
	dsa_pointer hash_layout_dp = SerializeTupleDataLayout(agg_state.hash_layout, state->runtime_dsa);
	if (!DsaPointerIsValid(hash_layout_dp))
		return nullptr;

	PgVector<uint16_t> group_keys;
	for (const ColumnRef &ref : group_cols)
	{
		const ColumnSchema *col = nullptr;
		if (!LookupRawColumn(ref, current_cols, current_schema, col))
			return nullptr;
		group_keys.push_back(col->chunk_slot);
	}

	PgVector<AggFuncDesc> agg_funcs;
	agg_funcs.assign(agg_state.agg_funcs.begin(), agg_state.agg_funcs.end());

	std::unique_ptr<PhysicalHashAggregate> hash_op;
	if (agg_state.perfect_hash_capacity > 0)
	{
		hash_op = std::make_unique<PhysicalPerfectHashAggregate>(
			hash_layout_dp,
			std::move(group_keys),
			std::move(agg_funcs),
			InvalidDsaPointer,
			estimated_groups,
			agg_state.perfect_hash_capacity);
	}
	else
	{
		hash_op = std::make_unique<PhysicalHashAggregate>(
			hash_layout_dp,
			std::move(group_keys),
			std::move(agg_funcs),
			InvalidDsaPointer,
			initial_groups,
			0);
	}

	if (!agg_state.project_exprs.empty())
	{
		PgVector<ProjectExprDesc> expr_descs;
		expr_descs.assign(agg_state.project_exprs.begin(), agg_state.project_exprs.end());
		PgVector<ProjectStep> steps;
		steps.assign(agg_state.project_steps.begin(), agg_state.project_steps.end());
		auto project_op = std::make_unique<PhysicalProjection>(
			InvalidDsaPointer,
			InvalidDsaPointer,
			std::move(expr_descs),
			std::move(steps));
		project_op->AddChild(std::move(child));
		child = std::move(project_op);
	}

	hash_op->AddChild(std::move(child));
	return hash_op;
}

static bool
BuildPlainAggOutputColumns(const DerivedAggBuildState &agg_state,
				   std::vector<ColumnRef> &out_refs,
				   std::vector<ColumnSchema> &out_schema)
{
	out_refs.clear();
	out_schema.clear();
	if (agg_state.agg_kinds.size() != agg_state.agg_numeric_scales.size() ||
	    agg_state.agg_kinds.size() > 16)
		return false;
	for (uint16_t a = 0; a < agg_state.agg_kinds.size(); ++a)
	{
		ColumnSchema cs{};
		cs.chunk_slot = static_cast<uint8_t>(a);
		cs.src_attno = 0;
		cs._pad0 = 0;
		switch (agg_state.agg_kinds[a])
		{
			case TdcAggKind::COUNT_STAR:
			case TdcAggKind::SUM_INT64:
				cs.type_oid = INT8OID;
				cs.typmod = -1;
				cs.typlen = 8;
				cs.typbyval = true;
				cs.decode_kind = ColumnDecodeKind::INT64_INT8;
				break;
			case TdcAggKind::SUM_NUMERIC:
			case TdcAggKind::AVG_NUMERIC:
				cs.type_oid = NUMERICOID;
				cs.typmod = -1;
				cs.typlen = -1;
				cs.typbyval = false;
				cs.decode_kind = ColumnDecodeKind::INT64_NUMERIC_SCALED;
				break;
			default:
				return false;
		}
		out_refs.push_back(ColumnRef{1, static_cast<AttrNumber>(a + 1)});
		out_schema.push_back(cs);
	}
	return !out_schema.empty();
}

static Expr *
ReplaceAggrefsWithVarsMutator(Node *node, const std::vector<Aggref *> *aggrefs)
{
	if (node == nullptr)
		return nullptr;
	if (nodeTag(node) == T_Aggref && aggrefs != nullptr)
	{
		for (uint16_t a = 0; a < aggrefs->size(); ++a)
		{
			if ((*aggrefs)[a] == (Aggref *) node)
				return (Expr *) makeVar(1,
					static_cast<AttrNumber>(a + 1),
					((Aggref *) node)->aggtype,
					-1,
					InvalidOid,
					0);
		}
	}
	return (Expr *) expression_tree_mutator(node,
		(Node *(*)(Node *, void *)) ReplaceAggrefsWithVarsMutator,
		(void *) aggrefs);
}

static bool
TryBuildPostAggProjection(Agg *agg,
				 const DerivedAggBuildState &agg_state,
				 const std::vector<Aggref *> &aggrefs,
				 std::unique_ptr<PhysicalOperator> &op,
				 PgVolVecQueryState *state,
				 std::vector<ColumnSchema> &out_schema,
				 TupleDataLayout &out_layout)
{
	if (agg == nullptr || agg->numCols != 0 || agg->plan.targetlist == NIL || op == nullptr ||
	    state == nullptr || state->runtime_dsa == nullptr)
		return false;
	std::vector<ColumnRef> agg_cols;
	std::vector<ColumnSchema> agg_schema;
	if (!BuildPlainAggOutputColumns(agg_state, agg_cols, agg_schema))
		return false;
	Plan agg_output_context{};
	agg_output_context.targetlist = NIL;
	for (uint16_t a = 0; a < agg_state.agg_kinds.size(); ++a)
	{
		Var *var = makeVar(1,
			static_cast<AttrNumber>(a + 1),
			agg_schema[a].type_oid,
			agg_schema[a].typmod,
			InvalidOid,
			0);
		agg_output_context.targetlist = lappend(agg_output_context.targetlist,
			makeTargetEntry((Expr *) var,
				static_cast<AttrNumber>(a + 1),
				nullptr,
				false));
	}
	uint8_t next_int64_slot = static_cast<uint8_t>(agg_schema.size());
	std::vector<ProjectStep> steps;
	std::vector<ProjectExprDesc> exprs;
	std::vector<ColumnSchema> project_schema;
	TupleDataLayout project_layout;
	TupleDataLayoutInit(&project_layout);
	ListCell *lc;
	foreach(lc, agg->plan.targetlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		if (tle == nullptr || tle->expr == nullptr)
			return false;
		const uint16_t first_step_idx = static_cast<uint16_t>(steps.size());
		Expr *rewritten = ReplaceAggrefsWithVarsMutator((Node *) tle->expr, &aggrefs);
		int8_t scale = 0;
		uint8_t slot = 0;
		if (!LowerProjectionExpr(rewritten,
				steps,
				next_int64_slot,
				agg_cols,
				agg_schema,
				&agg_output_context,
				nullptr,
				scale,
				slot) ||
		    steps.size() == first_step_idx)
			return false;
		exprs.push_back(ProjectExprDesc{first_step_idx,
			static_cast<uint16_t>(steps.size() - first_step_idx),
			slot,
			scale,
			0});
		ColumnSchema cs{};
		cs.type_oid = NUMERICOID;
		cs.typmod = -1;
		cs.typlen = -1;
		cs.typbyval = false;
		cs.chunk_slot = slot;
		cs.src_attno = 0;
		cs.decode_kind = ColumnDecodeKind::INT64_NUMERIC_SCALED;
		cs._pad0 = 0;
		project_schema.push_back(cs);
		(void) TupleDataLayoutAppendColumn(&project_layout,
			TdcColumnKind::INT64,
			slot,
			NUMERICOID,
			scale);
	}
	TupleDataLayoutSeal(&project_layout);
	if (project_schema.empty() || project_schema.size() > 16)
		return false;
	dsa_pointer input_schema_dp = BuildSchemaDescriptorFromColumns(agg_schema, state->runtime_dsa);
	dsa_pointer output_schema_dp = BuildSchemaDescriptorFromColumns(project_schema, state->runtime_dsa);
	if (!DsaPointerIsValid(input_schema_dp) || !DsaPointerIsValid(output_schema_dp))
		return false;
	PgVector<ProjectExprDesc> pg_exprs;
	pg_exprs.assign(exprs.begin(), exprs.end());
	PgVector<ProjectStep> pg_steps;
	pg_steps.assign(steps.begin(), steps.end());
	auto project_op = std::make_unique<PhysicalProjection>(
		input_schema_dp,
		output_schema_dp,
		std::move(pg_exprs),
		std::move(pg_steps));
	project_op->AddChild(std::move(op));
	op = std::move(project_op);
	out_schema = std::move(project_schema);
	out_layout = project_layout;
	return true;
}

static std::unique_ptr<PhysicalOperator>
BuildJoinSubtree(Plan *plan,
		 QueryDesc *qd,
		 PgVolVecQueryState *state,
		 std::vector<ColumnRef> &out_cols,
		 std::vector<ColumnSchema> &out_schema);

static std::unique_ptr<PhysicalOperator>
BuildLeafSeqScan(SeqScan *scan,
		 QueryDesc *qd,
		 PgVolVecQueryState *state,
		 std::vector<ColumnRef> &out_cols,
		 std::vector<ColumnSchema> &out_schema)
{
	if (scan == nullptr || state == nullptr || state->runtime_dsa == nullptr)
		return nullptr;
	Oid relid = InvalidOid;
	if (!ExtractRelid(scan, qd, relid))
		return nullptr;
	if (!AnalyzePlanOutput(&scan->scan.plan, qd, out_cols, out_schema))
		return nullptr;
	std::vector<FilterInputDesc> filter_inputs;
	std::vector<FilterExprDesc> filter_exprs;
	std::vector<FilterStep> filter_steps;
	std::vector<char> filter_string_consts;
	uint16_t next_filter_bool_reg = 0;
	if (!ExtractFilterQual(scan->scan.plan.qual,
			relid,
			filter_inputs,
			filter_exprs,
			filter_steps,
			filter_string_consts,
			next_filter_bool_reg))
		return nullptr;
	dsa_pointer output_schema_dp = BuildSchemaDescriptorFromColumns(out_schema, state->runtime_dsa);
	dsa_pointer filter_inputs_dp = BuildFilterArray(state->runtime_dsa,
		filter_inputs.data(), sizeof(FilterInputDesc), filter_inputs.size());
	dsa_pointer filter_exprs_dp = BuildFilterArray(state->runtime_dsa,
		filter_exprs.data(), sizeof(FilterExprDesc), filter_exprs.size());
	dsa_pointer filter_steps_dp = BuildFilterArray(state->runtime_dsa,
		filter_steps.data(), sizeof(FilterStep), filter_steps.size());
	dsa_pointer filter_string_consts_dp = BuildCharArray(state->runtime_dsa, filter_string_consts);
	if (!DsaPointerIsValid(output_schema_dp))
		return nullptr;
	return std::make_unique<PhysicalSeqScan>(
		relid,
		InvalidDsaPointer,
		output_schema_dp,
		filter_inputs_dp,
		filter_exprs_dp,
		filter_steps_dp,
		filter_string_consts_dp,
		static_cast<uint16_t>(filter_inputs.size()),
		static_cast<uint16_t>(filter_exprs.size()),
		static_cast<uint16_t>(filter_steps.size()),
		next_filter_bool_reg,
		static_cast<uint32_t>(filter_string_consts.size()),
		InvalidDsaPointer);
}

static std::unique_ptr<PhysicalOperator>
BuildHashJoinSubtree(HashJoin *hash_join,
		     QueryDesc *qd,
		     PgVolVecQueryState *state,
		     std::vector<ColumnRef> &out_cols,
		     std::vector<ColumnSchema> &out_schema)
{
	if (hash_join == nullptr || state == nullptr || state->runtime_dsa == nullptr)
		return nullptr;
	std::vector<ColumnRef> left_cols;
	std::vector<ColumnSchema> left_schema;
	std::vector<ColumnRef> right_cols;
	std::vector<ColumnSchema> right_schema;
	auto left_child = BuildJoinSubtree(hash_join->join.plan.lefttree, qd, state, left_cols, left_schema);
	auto right_child = BuildJoinSubtree(hash_join->join.plan.righttree, qd, state, right_cols, right_schema);
	if (left_child == nullptr || right_child == nullptr)
		return nullptr;
	std::vector<ColumnRef> left_keys;
	std::vector<ColumnRef> right_keys;
	std::vector<HashJoinOutputColumnDesc> output_mappings;
	std::vector<HashJoinFilterInputDesc> filter_inputs;
	std::vector<FilterExprDesc> filter_exprs;
	std::vector<FilterStep> filter_steps;
	std::vector<char> filter_string_consts;
	TupleDataLayout left_key_layout;
	TupleDataLayout right_key_layout;
	TupleDataLayout left_payload_layout;
	TupleDataLayout right_payload_layout;
	uint16_t next_filter_bool_reg = 0;
	const bool swap_sides = PlanEstimatedRows(hash_join->join.plan.lefttree) <
		PlanEstimatedRows(hash_join->join.plan.righttree);
	const std::vector<ColumnRef> &probe_cols = swap_sides ? right_cols : left_cols;
	const std::vector<ColumnSchema> &probe_schema = swap_sides ? right_schema : left_schema;
	const std::vector<ColumnRef> &build_cols = swap_sides ? left_cols : right_cols;
	const std::vector<ColumnSchema> &build_schema = swap_sides ? left_schema : right_schema;
	const std::vector<ColumnRef> &probe_keys = swap_sides ? right_keys : left_keys;
	const std::vector<ColumnRef> &build_keys = swap_sides ? left_keys : right_keys;
	Plan *build_plan = swap_sides ? hash_join->join.plan.lefttree : hash_join->join.plan.righttree;
	const uint32_t build_max_rows = EstimateHashJoinBuildRows(build_plan);
	if (!ExtractHashJoinClauseKeys(hash_join, left_keys, right_keys) ||
	    !ExtractHashJoinOutputCols(hash_join, out_cols) ||
	    !ExtractHashJoinFilterQual(hash_join->join.joinqual,
			&hash_join->join.plan,
			probe_cols,
			probe_schema,
			build_cols,
			build_schema,
			filter_inputs,
			filter_exprs,
			filter_steps,
			filter_string_consts,
			next_filter_bool_reg) ||
	    !BuildColumnOnlyLayoutForRefs(probe_keys, probe_cols, probe_schema, left_key_layout) ||
	    !BuildColumnOnlyLayoutForRefs(build_keys, build_cols, build_schema, right_key_layout) ||
	    !BuildColumnOnlyLayout(probe_schema, left_payload_layout) ||
	    !BuildColumnOnlyLayout(build_schema, right_payload_layout) ||
	    !BuildHashJoinOutputMappings(out_cols,
			probe_cols,
			probe_schema,
			build_cols,
			build_schema,
			output_mappings,
			out_schema))
		return nullptr;
	dsa_pointer left_schema_dp = BuildSchemaDescriptorFromColumns(probe_schema, state->runtime_dsa);
	dsa_pointer right_schema_dp = BuildSchemaDescriptorFromColumns(build_schema, state->runtime_dsa);
	dsa_pointer output_schema_dp = BuildSchemaDescriptorFromColumns(out_schema, state->runtime_dsa);
	dsa_pointer left_key_layout_dp = SerializeTupleDataLayout(left_key_layout, state->runtime_dsa);
	dsa_pointer right_key_layout_dp = SerializeTupleDataLayout(right_key_layout, state->runtime_dsa);
	dsa_pointer left_payload_layout_dp = SerializeTupleDataLayout(left_payload_layout, state->runtime_dsa);
	dsa_pointer right_payload_layout_dp = SerializeTupleDataLayout(right_payload_layout, state->runtime_dsa);
	dsa_pointer output_columns_dp = BuildFilterArray(state->runtime_dsa,
		output_mappings.data(),
		sizeof(HashJoinOutputColumnDesc),
		output_mappings.size());
	dsa_pointer filter_inputs_dp = BuildFilterArray(state->runtime_dsa,
		filter_inputs.data(),
		sizeof(HashJoinFilterInputDesc),
		filter_inputs.size());
	dsa_pointer filter_exprs_dp = BuildFilterArray(state->runtime_dsa,
		filter_exprs.data(),
		sizeof(FilterExprDesc),
		filter_exprs.size());
	dsa_pointer filter_steps_dp = BuildFilterArray(state->runtime_dsa,
		filter_steps.data(),
		sizeof(FilterStep),
		filter_steps.size());
	dsa_pointer filter_string_consts_dp = BuildCharArray(state->runtime_dsa, filter_string_consts);
	if (!DsaPointerIsValid(left_schema_dp) || !DsaPointerIsValid(right_schema_dp) ||
	    !DsaPointerIsValid(output_schema_dp) || !DsaPointerIsValid(left_key_layout_dp) ||
	    !DsaPointerIsValid(right_key_layout_dp) || !DsaPointerIsValid(left_payload_layout_dp) ||
	    !DsaPointerIsValid(right_payload_layout_dp) || !DsaPointerIsValid(output_columns_dp))
		return nullptr;
	auto join_op = std::make_unique<PhysicalHashJoin>(
		left_schema_dp,
		right_schema_dp,
		output_schema_dp,
		left_key_layout_dp,
		right_key_layout_dp,
		left_payload_layout_dp,
		right_payload_layout_dp,
		output_columns_dp,
		static_cast<uint16_t>(output_mappings.size()),
		filter_inputs_dp,
		filter_exprs_dp,
		filter_steps_dp,
		filter_string_consts_dp,
		static_cast<uint16_t>(filter_inputs.size()),
		static_cast<uint16_t>(filter_exprs.size()),
		static_cast<uint16_t>(filter_steps.size()),
		next_filter_bool_reg,
		static_cast<uint32_t>(filter_string_consts.size()),
		InvalidDsaPointer,
		static_cast<uint16_t>(left_keys.size()),
		static_cast<uint16_t>(right_keys.size()),
		build_max_rows);
	if (swap_sides)
	{
		join_op->AddChild(std::move(right_child));
		join_op->AddChild(std::move(left_child));
	}
	else
	{
		join_op->AddChild(std::move(left_child));
		join_op->AddChild(std::move(right_child));
	}
	return join_op;
}

static std::unique_ptr<PhysicalOperator>
BuildJoinSubtree(Plan *plan,
		 QueryDesc *qd,
		 PgVolVecQueryState *state,
		 std::vector<ColumnRef> &out_cols,
		 std::vector<ColumnSchema> &out_schema)
{
	if (plan == nullptr)
		return nullptr;
	if (nodeTag(plan) == T_Hash)
		return BuildJoinSubtree(plan->lefttree, qd, state, out_cols, out_schema);
	if (nodeTag(plan) == T_SeqScan)
		return BuildLeafSeqScan((SeqScan *) plan, qd, state, out_cols, out_schema);
	if (nodeTag(plan) == T_HashJoin)
		return BuildHashJoinSubtree((HashJoin *) plan, qd, state, out_cols, out_schema);
	return nullptr;
}

static bool
BuildOutputContract(NodeTranslation &node,
			  QueryDesc *qd,
			  PgVolVecQueryState *state,
			  std::unique_ptr<PhysicalOperator> &out)
{
	if (state == nullptr || state->runtime_dsa == nullptr || qd == nullptr ||
	    node.op == nullptr || !node.has_output_contract)
		return false;
	dsa_pointer output_schema_dp = BuildSchemaDescriptorFromColumns(node.final_output_schema, state->runtime_dsa);
	if (!DsaPointerIsValid(output_schema_dp))
		return false;
	const dsa_pointer output_layout_dp = SerializeTupleDataLayout(node.final_output_layout, state->runtime_dsa);
	if (!DsaPointerIsValid(output_layout_dp))
		return false;
	const uint32_t row_capacity = EstimateInitialResultRows(qd, node.estimated_groups);
	const dsa_pointer output_payload_dp = BuildOutputTdc(state->runtime_dsa,
		output_layout_dp,
		node.final_output_layout,
		row_capacity);
	if (!DsaPointerIsValid(output_payload_dp))
		return false;

	auto output_op = std::make_unique<OutputSink>(
		qd->dest,
		qd->tupDesc,
		static_cast<int>(qd->operation),
		output_schema_dp,
		output_layout_dp,
		output_payload_dp,
		row_capacity,
		nullptr);
	output_op->AddChild(std::move(node.op));
	out = std::move(output_op);
	return true;
}

static bool
TranslateNode(Plan *plan,
	      QueryDesc *qd,
	      PgVolVecQueryState *state,
	      NodeTranslation &out)
{
	if (plan == nullptr || qd == nullptr || state == nullptr || state->runtime_dsa == nullptr)
		return false;
	switch (nodeTag(plan))
	{
		case T_Hash:
			return TranslateNode(plan->lefttree, qd, state, out);

		case T_SeqScan:
		{
			out = NodeTranslation{};
			out.op = BuildLeafSeqScan((SeqScan *) plan, qd, state, out.raw_cols, out.raw_schema);
			return out.op != nullptr;
		}

		case T_HashJoin:
		{
			out = NodeTranslation{};
			out.op = BuildHashJoinSubtree((HashJoin *) plan, qd, state, out.raw_cols, out.raw_schema);
			return out.op != nullptr;
		}

		case T_Agg:
		{
			Agg *agg = (Agg *) plan;
			if (agg->aggstrategy != AGG_HASHED && agg->aggstrategy != AGG_PLAIN)
				return false;
			if (agg->aggsplit != AGGSPLIT_SIMPLE || agg->groupingSets != NIL || agg->chain != NIL)
				return false;
			if (agg->numCols > 0 && agg->aggstrategy != AGG_HASHED)
				return false;
			if (agg->plan.lefttree == nullptr || agg->plan.righttree != nullptr)
				return false;

			NodeTranslation child;
			if (!TranslateNode(agg->plan.lefttree, qd, state, child) || child.op == nullptr)
				return false;
			if (child.raw_cols.empty() || child.raw_schema.empty())
				return false;

			std::vector<ColumnRef> group_cols;
			std::vector<Aggref *> aggrefs;
			if (!ExtractGroupCols(agg, agg->plan.lefttree, group_cols) ||
			    !ExtractAggrefs(agg, &agg->plan, group_cols, aggrefs))
				return false;

			DerivedAggBuildState agg_state{};
			const std::vector<SortKeyDesc> empty_sort_keys;
			if (!DeriveAggBuildState(agg,
					nullptr,
					group_cols,
					aggrefs,
					empty_sort_keys,
					child.raw_cols,
					child.raw_schema,
					agg_state))
				return false;

			std::unique_ptr<PhysicalOperator> agg_op = BuildAggOperator(agg,
				child.raw_cols,
				child.raw_schema,
				group_cols,
				agg_state,
				(agg->numCols == 0) ? 1u : EstimateHashAggGroups(agg),
				(agg->numCols == 0) ? 1u : EstimateHashAggInitialGroups(agg),
				std::move(child.op),
				state);
			if (agg_op == nullptr)
				return false;
			bool has_post_agg_expr = false;
			ListCell *tle_lc;
			foreach(tle_lc, agg->plan.targetlist)
			{
				TargetEntry *tle = (TargetEntry *) lfirst(tle_lc);
				if (tle == nullptr || tle->expr == nullptr)
					return false;
				Expr *expr = StripRelabels((Expr *) tle->expr);
				if (expr == nullptr)
					return false;
				if (nodeTag(expr) != T_Aggref && nodeTag(expr) != T_Var)
				{
					has_post_agg_expr = true;
					break;
				}
			}
			if (has_post_agg_expr && !TryBuildPostAggProjection(agg,
					agg_state,
					aggrefs,
					agg_op,
					state,
					agg_state.final_project_schema,
					agg_state.final_project_layout))
				return false;

			out = NodeTranslation{};
			out.op = std::move(agg_op);
			out.agg = agg;
			out.estimated_groups = (agg->numCols == 0) ? 1u : EstimateHashAggGroups(agg);
			out.group_cols = std::move(group_cols);
			out.aggrefs = std::move(aggrefs);
			out.agg_input_cols = std::move(child.raw_cols);
			out.agg_input_schema = std::move(child.raw_schema);
			out.agg_state = std::move(agg_state);
			if (has_post_agg_expr)
			{
				out.final_output_schema = out.agg_state.final_project_schema;
				out.final_output_layout = out.agg_state.final_project_layout;
			}
			else
			{
				out.final_output_schema = out.agg_state.final_output_schema;
				out.final_output_layout = out.agg_state.final_output_layout;
			}
			out.has_output_contract = true;
			return true;
		}

		case T_Sort:
		{
			Sort *sort = (Sort *) plan;
			if (sort->numCols < 1 || sort->plan.lefttree == nullptr || sort->plan.righttree != nullptr)
				return false;

			NodeTranslation child;
			if (!TranslateNode(sort->plan.lefttree, qd, state, child) || child.op == nullptr)
				return false;
			if (child.agg == nullptr)
				return false;

			std::vector<SortKeyDesc> sort_keys;
			if (!ExtractSortKeys(sort,
					child.agg,
					&child.agg->plan,
					child.group_cols,
					child.aggrefs,
					sort_keys))
				return false;

			TupleDataLayout sort_key_layout;
			TupleDataLayout sort_payload_layout;
			if (!BuildSortLayouts(child.group_cols,
					child.agg_input_cols,
					child.agg_input_schema,
					child.agg_state.agg_funcs,
					child.agg_state.agg_kinds,
					child.agg_state.agg_numeric_scales,
					sort_keys,
					sort_key_layout,
					sort_payload_layout))
				return false;

			dsa_pointer sort_keys_dp = BuildFilterArray(state->runtime_dsa,
				sort_keys.data(),
				sizeof(SortKeyDesc),
				sort_keys.size());
			dsa_pointer sort_key_layout_dp = SerializeTupleDataLayout(sort_key_layout, state->runtime_dsa);
			dsa_pointer sort_payload_layout_dp = SerializeTupleDataLayout(sort_payload_layout, state->runtime_dsa);
			if ((!sort_keys.empty() && !DsaPointerIsValid(sort_keys_dp)) ||
			    !DsaPointerIsValid(sort_key_layout_dp) ||
			    !DsaPointerIsValid(sort_payload_layout_dp))
				return false;

			auto order_op = std::make_unique<PhysicalOrder>(sort_keys_dp,
				static_cast<uint16_t>(sort_keys.size()),
				sort_key_layout_dp,
				sort_payload_layout_dp,
				InvalidDsaPointer,
				EstimateInitialResultRows(qd, child.estimated_groups));
			order_op->AddChild(std::move(child.op));

			out = std::move(child);
			out.op = std::move(order_op);
			out.sort = sort;
			return true;
		}

		default:
			return false;
	}
}

} // namespace

} // namespace translator_detail

std::unique_ptr<PhysicalOperator>
Translator::TranslatePlan(Plan *plan, QueryDesc *qd, PgVolVecQueryState *state)
{
	if (plan == nullptr || qd == nullptr || state == nullptr || state->runtime_dsa == nullptr)
		return nullptr;

	translator_detail::NodeTranslation node;
	if (!translator_detail::TranslateNode(plan, qd, state, node))
		return nullptr;

	std::unique_ptr<PhysicalOperator> root;
	if (!translator_detail::BuildOutputContract(node, qd, state, root))
		return nullptr;
	return root;
}

std::unique_ptr<PhysicalOperator>
Translator::Translate(QueryDesc *qd, PgVolVecQueryState *state)
{
	if (qd == nullptr || qd->plannedstmt == nullptr || qd->plannedstmt->planTree == nullptr)
		return nullptr;
	return TranslatePlan(qd->plannedstmt->planTree, qd, state);
}

}  /* namespace pipeline */
}  /* namespace pg_volvec */

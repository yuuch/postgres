#include "parallel/pipeline/translator_internal.hpp"

extern "C" {
#include "nodes/pg_list.h"
#include "nodes/nodeFuncs.h"
}

namespace pg_volvec {
namespace pipeline {
namespace translator_detail {

namespace {

struct AggrefCollectorContext {
	std::vector<Aggref *> *out;
};

static bool
CollectAggrefWalker(Node *node, void *ctx)
{
	auto *collector = static_cast<AggrefCollectorContext *>(ctx);
	if (node == nullptr)
		return false;
	if (nodeTag(node) == T_Aggref)
	{
		collector->out->push_back((Aggref *) node);
		return false;
	}
	return expression_tree_walker(node, CollectAggrefWalker, ctx);
}

} // namespace

uint32_t
EstimateHashAggGroups(Agg *agg)
{
	constexpr double kMinGroups = 16.0;
	constexpr double kMaxGroups = static_cast<double>(1u << 20);
	double estimate = 256.0;
	if (agg != nullptr)
	{
		if (agg->numGroups > 0.0)
			estimate = agg->numGroups;
		else if (agg->plan.plan_rows > 0.0)
			estimate = agg->plan.plan_rows;
	}
	estimate *= 1.5;
	estimate = std::max(kMinGroups, std::min(kMaxGroups, estimate));
	return static_cast<uint32_t>(estimate);
}

uint32_t
EstimateHashAggInitialGroups(Agg *agg)
{
	constexpr uint32_t kMinInitialGroups = 16u;
	constexpr uint32_t kMaxInitialGroups = 8192u;
	const uint32_t estimated_groups = EstimateHashAggGroups(agg);
	return std::max(kMinInitialGroups, std::min(kMaxInitialGroups, estimated_groups));
}

static bool AnalyzeSupportedPlanTree(Plan *plan, SupportedPlanShape &out);

static bool
AnalyzeHashJoinNode(HashJoin *hash_join, SupportedPlanShape &out)
{
	if (hash_join == nullptr || out.hash_join != nullptr)
		return false;
	if (hash_join->join.jointype != JOIN_INNER)
		return false;
	if (hash_join->join.plan.lefttree == nullptr || hash_join->join.plan.righttree == nullptr)
		return false;
	if (hash_join->hashclauses == NIL)
		return false;
	out.hash_join = hash_join;
	return true;
}

static bool
AnalyzeSeqScanNode(SeqScan *scan, SupportedPlanShape &out)
{
	if (scan == nullptr || out.scan != nullptr)
		return false;
	if (scan->scan.plan.lefttree != nullptr || scan->scan.plan.righttree != nullptr)
		return false;
	out.scan = scan;
	return true;
}

static bool
AnalyzeAggNode(Agg *agg, SupportedPlanShape &out)
{
	if (agg == nullptr || out.agg != nullptr)
		return false;
	if (agg->aggstrategy != AGG_HASHED && agg->aggstrategy != AGG_PLAIN)
		return false;
	if (agg->aggsplit != AGGSPLIT_SIMPLE)
		return false;
	if (agg->groupingSets != NIL || agg->chain != NIL)
		return false;
	if (agg->numCols > 0 && agg->aggstrategy != AGG_HASHED)
		return false;
	if (agg->plan.lefttree == nullptr || agg->plan.righttree != nullptr)
		return false;
	out.agg = agg;
	if (!AnalyzeSupportedPlanTree(agg->plan.lefttree, out))
		return false;
	return nodeTag(agg->plan.lefttree) == T_SeqScan ||
		nodeTag(agg->plan.lefttree) == T_HashJoin;
}

static bool
AnalyzeSortNode(Sort *sort, SupportedPlanShape &out)
{
	if (sort == nullptr || out.sort != nullptr)
		return false;
	if (sort->numCols < 1 || sort->plan.lefttree == nullptr || sort->plan.righttree != nullptr)
		return false;
	out.sort = sort;
	if (!AnalyzeSupportedPlanTree(sort->plan.lefttree, out))
		return false;
	return nodeTag(sort->plan.lefttree) == T_Agg;
}

static bool
AnalyzeSupportedPlanTree(Plan *plan, SupportedPlanShape &out)
{
	if (plan == nullptr)
		return false;
	switch (nodeTag(plan))
	{
		case T_HashJoin:
			return AnalyzeHashJoinNode((HashJoin *) plan, out);
		case T_SeqScan:
			return AnalyzeSeqScanNode((SeqScan *) plan, out);
		case T_Agg:
			return AnalyzeAggNode((Agg *) plan, out);
		case T_Sort:
			return AnalyzeSortNode((Sort *) plan, out);
		default:
			return false;
	}
}

bool
ExtractGroupCols(Agg *agg, Plan *agg_input_plan, std::vector<ColumnRef> &out)
{
	if (agg_input_plan == nullptr || agg_input_plan->targetlist == NIL)
		return false;
	if (agg->numCols == 0)
	{
		out.clear();
		return true;
	}
	out.clear();
	out.reserve(agg->numCols);
	for (int i = 0; i < agg->numCols; ++i)
	{
		AttrNumber resno = agg->grpColIdx[i];
		if (resno < 1 || resno > list_length(agg_input_plan->targetlist))
			return false;
		TargetEntry *tle = (TargetEntry *) list_nth(agg_input_plan->targetlist, resno - 1);
		if (tle == nullptr || tle->expr == nullptr)
			return false;
		ColumnRef ref{};
		if (!ResolvePlanExprToColumnRef((Expr *) tle->expr, agg_input_plan, ref))
			return false;
		out.push_back(ref);
	}
	return true;
}

bool
ExtractAggrefs(Agg *agg,
		   Plan *agg_plan,
		   const std::vector<ColumnRef> &group_cols,
		   std::vector<Aggref *> &out)
{
	List *agg_tlist = agg->plan.targetlist;
	if (agg_tlist == NIL)
		return false;
	out.clear();
	AggrefCollectorContext ctx{&out};
	ListCell *lc;
	foreach(lc, agg_tlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		if (tle == nullptr || tle->expr == nullptr)
			return false;
		NodeTag tag = nodeTag(tle->expr);
		if (tag == T_Var)
		{
			ColumnRef ref{};
			if (!ResolvePlanExprToColumnRef((Expr *) tle->expr, agg_plan, ref))
				return false;
			bool is_group = false;
			for (const ColumnRef &col : group_cols)
			{
				if (ref == col)
				{
					is_group = true;
					break;
				}
			}
			if (!is_group)
				return false;
		}
		else if (tag == T_Aggref)
			out.push_back((Aggref *) tle->expr);
		else
		{
			const size_t before = out.size();
			(void) expression_tree_walker((Node *) tle->expr, CollectAggrefWalker, &ctx);
			if (out.size() == before)
				return false;
		}
	}
	return true;
}

bool
ExtractHashJoinClauseKeys(HashJoin *hash_join,
			  std::vector<ColumnRef> &left_keys,
			  std::vector<ColumnRef> &right_keys)
{
	if (hash_join == nullptr || hash_join->hashclauses == NIL)
		return false;
	left_keys.clear();
	right_keys.clear();
	ListCell *lc;
	foreach(lc, hash_join->hashclauses)
	{
		OpExpr *op = (OpExpr *) lfirst(lc);
		if (op == nullptr || nodeTag(op) != T_OpExpr || list_length(op->args) != 2)
			return false;
		ColumnRef left_ref{};
		ColumnRef right_ref{};
		if (!ResolvePlanExprToColumnRef((Expr *) linitial(op->args), &hash_join->join.plan, left_ref) ||
		    !ResolvePlanExprToColumnRef((Expr *) lsecond(op->args), &hash_join->join.plan, right_ref))
			return false;
		left_keys.push_back(left_ref);
		right_keys.push_back(right_ref);
	}
	return !left_keys.empty() && left_keys.size() == right_keys.size();
}

bool
ExtractHashJoinOutputCols(HashJoin *hash_join,
			 std::vector<ColumnRef> &out)
{
	if (hash_join == nullptr || hash_join->join.plan.targetlist == NIL)
		return false;
	out.clear();
	ListCell *lc;
	foreach(lc, hash_join->join.plan.targetlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		if (tle == nullptr || tle->expr == nullptr)
			return false;
		ColumnRef ref{};
		if (!ResolvePlanExprToColumnRef((Expr *) tle->expr, &hash_join->join.plan, ref))
			return false;
		out.push_back(ref);
	}
	return !out.empty();
}


bool
AnalyzePlanOutput(Plan *plan,
		  QueryDesc *qd,
		  std::vector<ColumnRef> &out_cols,
		  std::vector<ColumnSchema> &out_schema)
{
	if (plan == nullptr)
		return false;
	switch (nodeTag(plan))
	{
		case T_SeqScan:
		{
			SeqScan *scan = (SeqScan *) plan;
			Oid relid = InvalidOid;
			Index varno = scan->scan.scanrelid;
			if (!ExtractRelid(scan, qd, relid))
				return false;
			out_cols.clear();
			if (scan->scan.plan.targetlist == NIL)
				return false;
			ListCell *lc;
			foreach(lc, scan->scan.plan.targetlist)
			{
				TargetEntry *tle = (TargetEntry *) lfirst(lc);
				if (tle == nullptr || tle->expr == nullptr)
					return false;
				ColumnRef ref{};
				if (!ResolvePlanExprToColumnRef((Expr *) tle->expr, &scan->scan.plan, ref))
					return false;
				out_cols.push_back(ref);
			}
			return BuildOrderedSeqScanColumns(relid, out_cols, varno, out_schema);
		}
		case T_Hash:
			return AnalyzePlanOutput(plan->lefttree, qd, out_cols, out_schema);
		case T_HashJoin:
		{
			HashJoin *hash_join = (HashJoin *) plan;
			std::vector<ColumnRef> left_cols;
			std::vector<ColumnRef> right_cols;
			std::vector<ColumnSchema> left_schema;
			std::vector<ColumnSchema> right_schema;
			if (!AnalyzePlanOutput(hash_join->join.plan.lefttree, qd, left_cols, left_schema) ||
			    !AnalyzePlanOutput(hash_join->join.plan.righttree, qd, right_cols, right_schema) ||
			    !ExtractHashJoinOutputCols(hash_join, out_cols))
				return false;
			std::vector<HashJoinOutputColumnDesc> output_mappings;
			return BuildHashJoinOutputMappings(out_cols,
				left_cols,
				left_schema,
				right_cols,
				right_schema,
				output_mappings,
				out_schema);
		}
		default:
			return false;
	}
}

bool
ExtractSupportedPlanShape(Plan *plan, QueryDesc *qd, SupportedPlanShape &out)
{
	if (!AnalyzeSupportedPlanTree(plan, out))
		return false;
	const bool has_hash_join = out.hash_join != nullptr;
	const bool has_scan = out.scan != nullptr;
	if (out.hash_join != nullptr)
	{
		if (!ExtractHashJoinClauseKeys(out.hash_join, out.hash_join_left_keys, out.hash_join_right_keys) ||
		    !ExtractHashJoinOutputCols(out.hash_join, out.hash_join_output_cols))
			return false;
		if (!AnalyzePlanOutput(out.hash_join->join.plan.lefttree,
				qd,
				out.hash_join_left_output_cols,
				out.hash_join_left_columns) ||
		    !AnalyzePlanOutput(out.hash_join->join.plan.righttree,
				qd,
				out.hash_join_right_output_cols,
				out.hash_join_right_columns))
			return false;
		if (!BuildColumnOnlyLayoutForRefs(out.hash_join_left_keys,
				out.hash_join_left_output_cols,
				out.hash_join_left_columns,
				out.hash_join_left_key_layout) ||
			!BuildColumnOnlyLayoutForRefs(out.hash_join_right_keys,
				out.hash_join_right_output_cols,
				out.hash_join_right_columns,
				out.hash_join_right_key_layout) ||
			!BuildColumnOnlyLayout(out.hash_join_left_columns, out.hash_join_left_payload_layout) ||
			!BuildColumnOnlyLayout(out.hash_join_right_columns, out.hash_join_right_payload_layout) ||
			!BuildHashJoinOutputMappings(out.hash_join_output_cols,
				out.hash_join_left_output_cols,
				out.hash_join_left_columns,
				out.hash_join_right_output_cols,
				out.hash_join_right_columns,
				out.hash_join_output_mappings,
				out.hash_join_output_schema_columns) ||
			!BuildColumnOnlyLayout(out.hash_join_output_schema_columns, out.hash_join_output_layout))
			return false;
	}
	if (out.agg == nullptr || (!has_scan && !has_hash_join))
		return false;
	if (out.sort != nullptr)
	{
		if (plan != &out.sort->plan)
			return false;
	}
	else if (plan != &out.agg->plan)
		return false;
	out.estimated_groups = EstimateHashAggGroups(out.agg);
	if (out.agg->numCols == 0)
		out.estimated_groups = 1;
	Plan *agg_input_plan = has_hash_join ? &out.hash_join->join.plan : &out.scan->scan.plan;
	if (!ExtractGroupCols(out.agg, agg_input_plan, out.group_cols) ||
	    !ExtractAggrefs(out.agg, &out.agg->plan, out.group_cols, out.aggrefs) ||
	    (out.sort != nullptr && !ExtractSortKeys(out.sort, out.agg, &out.agg->plan, out.group_cols, out.aggrefs, out.sort_keys)))
		return false;
	std::vector<ColumnRef> agg_arg_cols;
	if (!CollectAggrefArgCols(out.aggrefs, &out.agg->plan, agg_arg_cols))
		return false;
	const std::vector<ColumnRef> &available_input_cols = has_hash_join ? out.hash_join_output_cols : out.input_cols;
	const std::vector<ColumnSchema> &available_input_schema = has_hash_join ? out.hash_join_output_schema_columns : out.input_columns;
	if (has_hash_join)
	{
		for (const ColumnRef &group_col : out.group_cols)
		{
			const ColumnSchema *col = nullptr;
			if (!LookupRawColumn(group_col, available_input_cols, available_input_schema, col))
				return false;
		}
		for (const ColumnRef &col_ref : agg_arg_cols)
		{
			const ColumnSchema *col = nullptr;
			if (!LookupRawColumn(col_ref, available_input_cols, available_input_schema, col))
				return false;
		}
	}
	else
	{
		if (!ExtractRelid(out.scan, qd, out.relid))
			return false;
		out.scan_varno = out.scan->scan.scanrelid;
		if (!ExtractFilterQual(out.scan->scan.plan.qual, out.relid, out.filter_inputs, out.filter_exprs,
				out.filter_steps, out.filter_string_consts, out.next_filter_bool_reg))
			return false;
		out.input_cols = out.group_cols;
		for (const ColumnRef &col : agg_arg_cols)
		{
			bool seen = false;
			for (const ColumnRef &existing : out.input_cols)
			{
				if (existing == col)
				{
					seen = true;
					break;
				}
			}
			if (!seen)
				out.input_cols.push_back(col);
		}
		if (!BuildSeqScanColumns(out.relid, out.input_cols, out.scan_varno,
			out.input_columns, out.next_int32_slot, out.next_int64_slot, out.next_double_slot))
			return false;
		for (const ColumnRef &group_col : out.group_cols)
		{
			const ColumnSchema *col = nullptr;
			if (!LookupRawColumn(group_col, out.input_cols, out.input_columns, col))
				return false;
		}
		for (const ColumnRef &col_ref : agg_arg_cols)
		{
			const ColumnSchema *col = nullptr;
			if (!LookupRawColumn(col_ref, out.input_cols, out.input_columns, col))
				return false;
		}
	}
	if (!has_hash_join)
	{
		if (!out.group_cols.empty())
			(void) TryBuildPerfectHashSpec(out.group_cols, out.input_cols, out.input_columns, out.perfect_hash_capacity);
		std::vector<MaterializedProjectExpr> materialized_exprs;
		for (Aggref *aggref : out.aggrefs)
		{
			AggFuncDesc desc{};
			TdcAggKind kind = TdcAggKind::COUNT_STAR;
			int16_t numeric_scale = 0;
			if (!ClassifyAggref(aggref,
					out.input_cols,
					out.input_columns,
					&out.agg->plan,
					out.project_steps,
					out.project_exprs,
					materialized_exprs,
					out.next_int64_slot,
					desc,
					kind,
					numeric_scale))
				return false;
			out.agg_funcs.push_back(desc);
			out.agg_kinds.push_back(kind);
			out.agg_numeric_scales.push_back(numeric_scale);
		}
		if (!BuildHashGroupLayout(out.group_cols, out.input_cols, out.input_columns,
			out.agg_funcs, out.agg_kinds, out.agg_numeric_scales, out.hash_layout))
			return false;
		if (out.sort != nullptr && !BuildSortLayouts(out.group_cols, out.input_cols, out.input_columns,
			out.agg_funcs, out.agg_kinds, out.agg_numeric_scales, out.sort_keys,
			out.sort_key_layout, out.sort_payload_layout))
			return false;
	}
	return true;
}

}  /* namespace translator_detail */
}  /* namespace pipeline */
}  /* namespace pg_volvec */

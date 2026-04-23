#include "exec/internal.hpp"

namespace pg_volvec {

static bool
MatchFinalizePartialAggregateChainLocal(Plan *plan,
										Plan **gather_out,
										Agg **partial_agg_out)
{
	Agg *finalize_agg;
	Plan *gather_plan;
	Plan *partial_plan;

	if (gather_out != nullptr)
		*gather_out = nullptr;
	if (partial_agg_out != nullptr)
		*partial_agg_out = nullptr;
	if (plan == nullptr || !IsA(plan, Agg))
		return false;

	finalize_agg = (Agg *) plan;
	if (!DO_AGGSPLIT_COMBINE(finalize_agg->aggsplit) ||
		finalize_agg->plan.lefttree == nullptr)
		return false;

	gather_plan = finalize_agg->plan.lefttree;
	if (!(IsA(gather_plan, Gather) || IsA(gather_plan, GatherMerge)) ||
		gather_plan->lefttree == nullptr)
		return false;

	partial_plan = gather_plan->lefttree;
	while (partial_plan != nullptr &&
		   (IsA(partial_plan, Sort) || IsA(partial_plan, IncrementalSort) ||
			IsA(partial_plan, Material)) &&
		   partial_plan->lefttree != nullptr)
		partial_plan = partial_plan->lefttree;
	if (!IsA(partial_plan, Agg))
		return false;
	if (!DO_AGGSPLIT_SKIPFINAL(((Agg *) partial_plan)->aggsplit))
		return false;

	if (gather_out != nullptr)
		*gather_out = gather_plan;
	if (partial_agg_out != nullptr)
		*partial_agg_out = (Agg *) partial_plan;
	return true;
}

static bool
BuildSortKeyDescs(Sort *sort_node, VecPlanState *child,
				  VolVecVector<VecSortKeyDesc> *out_keys)
{
	for (int i = 0; i < sort_node->numCols; i++)
	{
		VecOutputColMeta meta;
		VecSortKeyDesc key_desc;
		Oid opfamily = InvalidOid;
		Oid opcintype = InvalidOid;
		CompareType cmptype = COMPARE_INVALID;
		int target_resno = sort_node->sortColIdx[i];

		if (target_resno <= 0 || target_resno > 16)
		{
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: sort key target resno %d is out of supported range", target_resno);
			return false;
		}
		if (child == nullptr || !child->lookup_output_col_meta(target_resno, &meta))
		{
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: sort key metadata lookup failed for target resno %d", target_resno);
			return false;
		}
		if (!get_ordering_op_properties(sort_node->sortOperators[i], &opfamily, &opcintype, &cmptype))
			return false;
		(void) opfamily;
		(void) opcintype;
		if (meta.storage_kind == VecOutputStorageKind::NumericAvgPair)
		{
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: sort does not support NumericAvgPair outputs");
			return false;
		}
		if (cmptype != COMPARE_LT && cmptype != COMPARE_GT)
		{
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: sort operator compare type %d is unsupported", (int) cmptype);
			return false;
		}

		key_desc.col_idx = (uint16_t) (target_resno - 1);
		key_desc.sql_type = meta.sql_type;
		key_desc.storage_kind = meta.storage_kind;
		key_desc.descending = (cmptype == COMPARE_GT);
		key_desc.nulls_first = sort_node->nullsFirst[i];
		key_desc.collation = sort_node->collations[i];
		key_desc.scale = meta.scale;
		out_keys->push_back(key_desc);
	}

	return true;
}

static bool
ExtractLimitCount(Limit *limit_node, uint64_t *limit_count)
{
	Expr *count_expr;
	Const *count_const;

	if (limit_node == nullptr || limit_count == nullptr)
		return false;
	if (limit_node->limitOption != LIMIT_OPTION_COUNT)
		return false;
	if (limit_node->limitOffset != nullptr)
	{
		Expr *offset_expr = StripImplicitNodesLocal((Expr *) limit_node->limitOffset);

		if (offset_expr == nullptr)
			return false;
		if (!IsA(offset_expr, Const))
			return false;
		if (((Const *) offset_expr)->constisnull)
			return false;
		if ((((Const *) offset_expr)->consttype == INT8OID &&
			 DatumGetInt64(((Const *) offset_expr)->constvalue) != 0) ||
			(((Const *) offset_expr)->consttype == INT4OID &&
			 DatumGetInt32(((Const *) offset_expr)->constvalue) != 0))
			return false;
	}

	count_expr = StripImplicitNodesLocal((Expr *) limit_node->limitCount);
	if (count_expr == nullptr || !IsA(count_expr, Const))
		return false;
	count_const = (Const *) count_expr;
	if (count_const->constisnull)
		return false;
	if (count_const->consttype == INT8OID)
	{
		int64_t count = DatumGetInt64(count_const->constvalue);

		if (count < 0)
			return false;
		*limit_count = (uint64_t) count;
		return true;
	}
	if (count_const->consttype == INT4OID)
	{
		int32_t count = DatumGetInt32(count_const->constvalue);

		if (count < 0)
			return false;
		*limit_count = (uint64_t) count;
		return true;
	}
	return false;
}

std::unique_ptr<VecPlanState>
ExecInitVecPlanInternal(Plan *plan, EState *estate, Bitmapset *required_attrs,
						bool force_full_deform,
						const ParallelWorkerContext *parallel_worker_context)
{
	if (plan == NULL) return nullptr;
	if (estate != nullptr && plan->extParam != NULL &&
		!(IsProcessParallelLocalContext(parallel_worker_context) &&
		  estate->es_param_exec_vals == nullptr))
		ExecSetParamPlanMulti(plan->extParam, GetPerTupleExprContext(estate));
	if (required_attrs == nullptr && !force_full_deform)
		CollectRequiredAttrsForPlan(plan, &required_attrs);
	std::unique_ptr<VecPlanState> current_state = nullptr;
	bool plan_qual_already_applied = false;
	if (IsA(plan, Limit)) {
		uint64_t limit_count = 0;
		auto left = ExecInitVecPlanInternal(plan->lefttree, estate, required_attrs, force_full_deform, parallel_worker_context);
		if (!left)
		{
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: limit initialization could not build child state");
			return nullptr;
		}
		if (HasProcessParallelTargetAggSubtree(parallel_worker_context, left.get()))
			return left;
		if (!ExtractLimitCount((Limit *) plan, &limit_count))
		{
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: limit count/offset shape is not supported");
			return nullptr;
		}
		current_state = std::make_unique<VecLimitState>(std::move(left), limit_count);
	} else if (IsA(plan, Sort)) {
		VolVecVector<VecSortKeyDesc> key_descs{PgMemoryContextAllocator<VecSortKeyDesc>(CurrentMemoryContext)};
		auto left = ExecInitVecPlanInternal(plan->lefttree, estate, required_attrs, force_full_deform, parallel_worker_context);
		if (!left)
		{
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: sort initialization could not build child state");
			return nullptr;
		}
		if (HasProcessParallelTargetAggSubtree(parallel_worker_context, left.get()))
			return left;
		if (CanBuildDirectVarProjectTargetList(plan->targetlist))
		{
			left = BuildDirectVarProject(std::move(left), plan->targetlist);
			if (!left)
			{
				if (pg_volvec_trace_hooks)
					elog(LOG, "pg_volvec: sort direct-var projection initialization failed");
				return nullptr;
			}
		}
		if (!BuildSortKeyDescs((Sort *) plan, left.get(), &key_descs))
			return nullptr;
		current_state = std::make_unique<VecSortState>(std::move(left), (Sort *) plan, std::move(key_descs));
	} else if (IsA(plan, Gather) || IsA(plan, GatherMerge)) {
		auto left = ExecInitVecPlanInternal(plan->lefttree,
											estate,
											required_attrs,
											force_full_deform,
											parallel_worker_context);

		if (!left)
		{
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: %s initialization could not build child state",
					 IsA(plan, GatherMerge) ? "gather merge" : "gather");
			return nullptr;
		}
		if (HasProcessParallelTargetAggSubtree(parallel_worker_context, left.get()))
			return left;
		if (plan->targetlist != NIL)
		{
			left = BuildDirectVarProject(std::move(left), plan->targetlist);
			if (!left)
			{
				if (pg_volvec_trace_hooks)
					elog(LOG, "pg_volvec: %s targetlist projection is not supported",
						 IsA(plan, GatherMerge) ? "gather merge" : "gather");
				return nullptr;
			}
		}
		current_state = std::move(left);
	} else if (IsA(plan, Agg)) {
		Agg *agg_node = (Agg *) plan;
		Plan *gather_plan = nullptr;
		Agg *partial_agg = nullptr;

		if (agg_node->aggsplit != AGGSPLIT_SIMPLE)
		{
			const char *canonical_failure_reason = nullptr;
			Plan *canonical_plan = nullptr;

			if (MatchFinalizePartialAggregateChainLocal(plan, &gather_plan, &partial_agg) &&
				(canonical_plan =
					 TryCanonicalizeFinalizePartialAggregate(agg_node,
															 gather_plan,
															 partial_agg,
															 &canonical_failure_reason)) != nullptr)
			{
				return ExecInitVecPlanInternal(canonical_plan,
											  estate,
											  required_attrs,
											  force_full_deform,
											  parallel_worker_context);
			}
			if (pg_volvec_trace_hooks)
				elog(LOG,
					 "pg_volvec: aggregate canonicalization failed for aggsplit=%d (%s)",
					 (int) agg_node->aggsplit,
					 canonical_failure_reason != nullptr ? canonical_failure_reason : "unsupported aggregate shape");
			return nullptr;
		}

		auto left = ExecInitVecPlanInternal(plan->lefttree, estate, required_attrs, force_full_deform, parallel_worker_context);
		if (!left) {
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: aggregate initialization could not build child state");
			return nullptr;
		}
		bool suppress_agg_qual = ShouldSuppressPartialAggQual(parallel_worker_context,
															  agg_node);

		current_state = BuildAggWithOptionalProject(std::move(left),
													agg_node,
													estate,
													suppress_agg_qual);
		if (!current_state)
		{
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: aggregate state/project initialization failed");
			return nullptr;
		}
		if (agg_node->plan.qual != NIL)
			plan_qual_already_applied = true;
		if (ShouldSuppressPartialAggQual(parallel_worker_context, agg_node))
			return current_state;
		if (agg_node->numCols > 0 &&
			agg_node->aggstrategy != AGG_HASHED &&
			IsA(plan->lefttree, Sort))
		{
			VolVecVector<VecSortKeyDesc> key_descs{PgMemoryContextAllocator<VecSortKeyDesc>(CurrentMemoryContext)};

			if (!BuildSortKeyDescs((Sort *) plan->lefttree, current_state.get(), &key_descs))
				return nullptr;
			current_state = std::make_unique<VecSortState>(std::move(current_state),
														   (Sort *) plan->lefttree,
														   std::move(key_descs),
														   list_length(agg_node->plan.targetlist));
		}
	} else if (IsA(plan, SubqueryScan)) {
		SubqueryScan *subquery_scan = (SubqueryScan *) plan;
		auto left = ExecInitVecPlanInternal(subquery_scan->subplan, estate, nullptr, force_full_deform, parallel_worker_context);

		if (!left)
		{
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: subquery scan initialization could not build subplan state");
			return nullptr;
		}
		if (HasProcessParallelTargetAggSubtree(parallel_worker_context, left.get()))
			return left;
		current_state = BuildDirectVarProject(std::move(left), plan->targetlist);
		if (!current_state)
		{
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: subquery scan targetlist projection is not supported");
			return nullptr;
		}
	} else if (IsA(plan, Material)) {
		auto left = ExecInitVecPlanInternal(plan->lefttree, estate, required_attrs, force_full_deform, parallel_worker_context);

		if (!left)
		{
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: materialize initialization could not build child state");
			return nullptr;
		}
		if (HasProcessParallelTargetAggSubtree(parallel_worker_context, left.get()))
			return left;
		current_state = BuildDirectVarProject(std::move(left), plan->targetlist);
		if (!current_state)
		{
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: materialize targetlist projection is not supported");
			return nullptr;
		}
	} else if (IsA(plan, SeqScan)) {
		SeqScan *sscan = (SeqScan *) plan;
		Oid relid = exec_rt_fetch(sscan->scan.scanrelid, estate)->relid;
		Relation rel = table_open(relid, NoLock);
		ParallelTableScanDesc parallel_scan_desc = nullptr;
		DeformProgram prog;
		TupleDesc desc = RelationGetDescr(rel);
		BuildPrunedDeformProgram(required_attrs, desc, &prog);
		if (parallel_worker_context != nullptr &&
			parallel_worker_context->parallel_scan_desc != nullptr &&
			parallel_worker_context->parallel_scan_relid == relid &&
			(parallel_worker_context->parallel_scan_plan_node_id < 0 ||
			 parallel_worker_context->parallel_scan_plan_node_id == plan->plan_node_id))
			parallel_scan_desc = parallel_worker_context->parallel_scan_desc;
		current_state = std::make_unique<VecSeqScanState>(rel,
														 estate->es_snapshot,
														 &prog,
														 parallel_scan_desc);
	}
	if (current_state == nullptr)
	{
		if (pg_volvec_trace_hooks)
			elog(LOG, "pg_volvec: no vector init branch matched nodeType=%d", (int) nodeTag(plan));
		return nullptr;
	}
	if (current_state && plan->qual != NIL && !plan_qual_already_applied) {
		auto program = std::make_unique<VecExprProgram>();
		Expr *combined_qual = (Expr *) make_ands_explicit(plan->qual);

		if (IsA(plan, Agg))
		{
			Expr *rewritten_qual = RewriteExprAgainstTargetList(combined_qual, plan->targetlist);

			if (rewritten_qual != nullptr)
				combined_qual = rewritten_qual;
		}
		if (combined_qual != nullptr)
		{
			CompileExpr(combined_qual, *program, true, estate);
			AdjustProgramVarScales(program.get(), current_state.get());
			if (program->get_final_res_idx() < 0)
			{
				if (pg_volvec_trace_hooks)
				{
					elog(LOG, "pg_volvec: plan qual compilation failed for node type %d",
						 (int) nodeTag(plan));
					elog(LOG, "pg_volvec: failed qual expr tree: %s",
						 nodeToString(combined_qual));
				}
				return nullptr;
			}
			current_state = std::make_unique<VecFilterState>(std::move(current_state), std::move(program));
		}
	}
	if (current_state && IsA(plan, SeqScan)) {
		current_state = BuildDirectVarProject(std::move(current_state), plan->targetlist);
		if (!current_state) {
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: seq scan targetlist projection is not supported");
			return nullptr;
		}
	}
	return current_state;
}

std::unique_ptr<VecPlanState>
ExecInitVecPlan(Plan *plan,
				 EState *estate,
				 const ParallelWorkerContext *parallel_worker_context)
{
	return ExecInitVecPlanInternal(plan, estate, nullptr, false, parallel_worker_context);
}

} /* namespace pg_volvec */

#include "exec/internal.hpp"

namespace pg_volvec {

static bool
ExtractJoinKeysFromClauses(List *clauses,
						   const char *clause_kind,
						   Plan *outer_plan,
						   Plan *inner_plan,
						   VecPlanState *outer,
						   VecPlanState *inner,
						   VolVecVector<VecHashJoinKeyCol> *key_cols)
{
	ListCell *lc;

	if (key_cols == nullptr)
// Auto-split from executor.cpp
		return false;
	if (clauses == NIL || list_length(clauses) > kMaxJoinKeys)
	{
		if (pg_volvec_trace_hooks)
			elog(LOG, "pg_volvec: %s requires between 1 and %d key clauses",
				 clause_kind != nullptr ? clause_kind : "join",
				 kMaxJoinKeys);
		return false;
	}

	foreach(lc, clauses)
	{
		OpExpr *hash_clause = (OpExpr *) lfirst(lc);
		Expr *left_expr;
		Expr *right_expr;
		Var *outer_var = nullptr;
		Var *inner_var = nullptr;
		VecOutputColMeta outer_meta;
		VecOutputColMeta inner_meta;
		uint16_t outer_source_col;
		uint16_t inner_source_col;

		if (!IsA(hash_clause, OpExpr) || list_length(hash_clause->args) != 2)
		{
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: hash clause is not a binary OpExpr");
			return false;
		}

		left_expr = StripImplicitNodesLocal((Expr *) linitial(hash_clause->args));
		right_expr = StripImplicitNodesLocal((Expr *) lsecond(hash_clause->args));
		if (!IsA(left_expr, Var) || !IsA(right_expr, Var))
		{
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: hash join keys must be simple Vars after stripping relabels");
			return false;
		}
		if (((Var *) left_expr)->varno == OUTER_VAR && ((Var *) right_expr)->varno == INNER_VAR)
		{
			outer_var = (Var *) left_expr;
			inner_var = (Var *) right_expr;
		}
		else if (((Var *) left_expr)->varno == INNER_VAR && ((Var *) right_expr)->varno == OUTER_VAR)
		{
			outer_var = (Var *) right_expr;
			inner_var = (Var *) left_expr;
		}
		else
		{
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: hash join key Vars are not OUTER_VAR/INNER_VAR");
			return false;
		}

		if (!LookupPlanOutputMeta(outer_plan, outer, outer_var->varattno, &outer_source_col, &outer_meta) ||
			!LookupPlanOutputMeta(inner_plan, inner, inner_var->varattno, &inner_source_col, &inner_meta))
		{
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: hash join could not resolve key metadata (outer attno=%d inner attno=%d)",
					 outer_var != nullptr ? outer_var->varattno : -1,
					 inner_var != nullptr ? inner_var->varattno : -1);
			return false;
		}
		if (outer_meta.storage_kind != inner_meta.storage_kind || outer_meta.scale != inner_meta.scale)
		{
			if (pg_volvec_trace_hooks)
				elog(LOG,
					 "pg_volvec: hash join key metadata do not match (outer attno=%d kind=%d scale=%d, inner attno=%d kind=%d scale=%d)",
					 outer_var->varattno, (int) outer_meta.storage_kind, outer_meta.scale,
					 inner_var->varattno, (int) inner_meta.storage_kind, inner_meta.scale);
			return false;
		}
		if (outer_meta.storage_kind != VecOutputStorageKind::Int32 &&
			outer_meta.storage_kind != VecOutputStorageKind::Int64 &&
			outer_meta.storage_kind != VecOutputStorageKind::NumericScaledInt64)
		{
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: hash join key kind %d is not supported",
					 (int) outer_meta.storage_kind);
			return false;
		}

		key_cols->push_back(VecHashJoinKeyCol{
			outer_source_col,
			inner_source_col,
			outer_meta.storage_kind,
			outer_meta.scale
		});
	}

	return !key_cols->empty();
}

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
	} else if (IsA(plan, NestLoop)) {
		NestLoop *nest_loop = (NestLoop *) plan;
		List *key_clauses = NIL;
		List *residual_clauses = NIL;
		Expr *join_filter_expr = nullptr;
		VolVecVector<VecJoinOutputCol> output_cols{PgMemoryContextAllocator<VecJoinOutputCol>(CurrentMemoryContext)};
		VolVecVector<VecHashJoinKeyCol> key_cols{PgMemoryContextAllocator<VecHashJoinKeyCol>(CurrentMemoryContext)};
		bool needs_project = false;
		Bitmapset *outer_required_attrs = nullptr;
		Bitmapset *inner_required_attrs = nullptr;
		std::unique_ptr<VecPlanState> outer;
		std::unique_ptr<VecPlanState> inner;
		VecPlanState *outer_state = nullptr;
		VecPlanState *inner_state = nullptr;
		bool build_outer_side = false;
		int visible_output_count = CountVisibleTargetEntries(nest_loop->join.plan.targetlist);

		if (nest_loop->join.jointype != JOIN_INNER &&
			nest_loop->join.jointype != JOIN_SEMI)
		{
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: nestloop fallback only supports inner/semi joins");
			return nullptr;
		}

		PartitionJoinClauses(nest_loop->join.joinqual, &key_clauses, &residual_clauses);
		if (key_clauses == NIL)
		{
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: nestloop fallback requires at least one simple equality join key");
			return nullptr;
		}

		BuildBinaryJoinChildRequiredAttrs(&nest_loop->join.plan,
										  (Node *) key_clauses,
										  plan->lefttree,
										  plan->righttree,
										  &outer_required_attrs,
										  &inner_required_attrs);
		outer = ExecInitVecPlanInternal(plan->lefttree, estate, outer_required_attrs, false, parallel_worker_context);
		inner = ExecInitVecPlanInternal(plan->righttree, estate, inner_required_attrs, false, parallel_worker_context);
		if (!outer || !inner)
		{
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: nestloop child initialization failed (outer=%s inner=%s)",
					 outer ? "ok" : "null", inner ? "ok" : "null");
			return nullptr;
		}
		outer_state = outer.get();
		inner_state = inner.get();
		build_outer_side = ShouldSwapInnerJoinBuildSides(nest_loop->join.jointype,
														 plan->lefttree,
														 plan->righttree);
		if (!ExtractJoinKeysFromClauses(key_clauses,
									   "nestloop join",
									   plan->lefttree,
									   plan->righttree,
									   outer_state,
									   inner_state,
									   &key_cols))
			return nullptr;
		if (!BuildJoinOutputCols(nest_loop->join.plan.targetlist,
								 plan->lefttree,
								 plan->righttree,
								 outer_state,
								 inner_state,
								 &output_cols,
								 &needs_project))
			return nullptr;
		if (nest_loop->join.jointype == JOIN_SEMI &&
			ShouldBuildSmallerSide(plan->lefttree, plan->righttree) &&
			RewriteSemiJoinVisibleInnerOutputsToOuterKeys(&output_cols,
														 key_cols,
														 visible_output_count))
			build_outer_side = true;

		join_filter_expr = BuildCombinedQualExpr(residual_clauses, nest_loop->join.plan.qual);
		if (join_filter_expr != nullptr)
		{
			join_filter_expr = RewriteHashJoinFilterExpr(join_filter_expr,
														 plan->lefttree,
														 plan->righttree,
														 outer_state,
														 inner_state,
														 &output_cols);
			if (join_filter_expr == nullptr)
			{
				if (pg_volvec_trace_hooks)
					elog(LOG, "pg_volvec: nestloop filter rewrite failed");
				return nullptr;
			}
		}

		std::unique_ptr<VecHashJoinState> join_state = std::make_unique<VecHashJoinState>(
			std::move(outer),
			std::move(inner),
			nest_loop->join.plan.plan_node_id,
			nest_loop->join.jointype,
			build_outer_side,
			visible_output_count,
			std::move(output_cols),
			std::move(key_cols));
		VecHashJoinState *join_state_ptr = join_state.get();
		current_state = std::move(join_state);

		if (join_filter_expr != nullptr)
		{
			auto program = std::make_unique<VecExprProgram>();

			CompileExpr(join_filter_expr, *program, true, estate);
			AdjustProgramVarScales(program.get(), current_state.get());
			if (program->get_final_res_idx() < 0)
			{
				if (pg_volvec_trace_hooks)
					elog(LOG, "pg_volvec: nestloop filter expression compilation failed");
				return nullptr;
			}
			if (nest_loop->join.jointype == JOIN_SEMI)
				join_state_ptr->set_join_filter_program(std::move(program));
			else
			{
				current_state = std::make_unique<VecFilterState>(std::move(current_state), std::move(program));
				plan_qual_already_applied = true;
			}
		}
		if (needs_project)
		{
			current_state = BuildJoinProject(std::move(current_state),
											 nest_loop->join.plan.targetlist,
											 plan->lefttree,
											 plan->righttree,
											 outer_state,
											 inner_state,
											 &output_cols,
											 estate);
			if (!current_state)
				return nullptr;
		}
	} else if (IsA(plan, HashJoin)) {
		HashJoin *hash_join = (HashJoin *) plan;
		Hash *hash_node;
		Plan *outer_plan;
		Plan *inner_plan;
		List *hash_key_clauses = NIL;
		List *hash_residual_clauses = NIL;
		Expr *join_filter_expr = nullptr;
		CorrelatedLookupFilterSpec lookup_filter_spec;
		bool use_lookup_filter = false;
		VolVecVector<VecJoinOutputCol> output_cols{PgMemoryContextAllocator<VecJoinOutputCol>(CurrentMemoryContext)};
		VolVecVector<VecHashJoinKeyCol> key_cols{PgMemoryContextAllocator<VecHashJoinKeyCol>(CurrentMemoryContext)};
		bool needs_project = false;
		Bitmapset *outer_required_attrs = nullptr;
		Bitmapset *inner_required_attrs = nullptr;
		std::unique_ptr<VecPlanState> outer;
		std::unique_ptr<VecPlanState> inner;
		VecPlanState *outer_state = nullptr;
		VecPlanState *inner_state = nullptr;
		bool build_outer_side = false;
		Oid outer_relid = InvalidOid;
		Oid inner_relid = InvalidOid;
		int visible_output_count = CountVisibleTargetEntries(hash_join->join.plan.targetlist);

			if (hash_join->join.jointype != JOIN_INNER &&
				hash_join->join.jointype != JOIN_LEFT &&
				hash_join->join.jointype != JOIN_RIGHT &&
				hash_join->join.jointype != JOIN_ANTI &&
				hash_join->join.jointype != JOIN_RIGHT_ANTI)
			{
				if (pg_volvec_trace_hooks)
					elog(LOG, "pg_volvec: hash join only supports inner/left/right/anti/right-anti joins");
				return nullptr;
			}
			if (!IsA(plan->righttree, Hash))
			{
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: hash join right tree is not a Hash node");
				return nullptr;
			}
			hash_node = (Hash *) plan->righttree;
			outer_plan = plan->lefttree;
			inner_plan = hash_node->plan.lefttree;
			build_outer_side = ShouldSwapInnerJoinBuildSides(hash_join->join.jointype,
															 outer_plan,
															 inner_plan);
			outer_relid = FindPlanBaseRelid(outer_plan, estate);
			inner_relid = FindPlanBaseRelid(inner_plan, estate);
			if (parallel_worker_context != nullptr &&
				parallel_worker_context->parallel_scan_desc != nullptr &&
				parallel_worker_context->parallel_scan_plan_node_id >= 0 &&
				hash_join->join.jointype == JOIN_INNER)
			{
				bool scan_in_outer =
					PlanContainsNodeId(outer_plan,
									   parallel_worker_context->parallel_scan_plan_node_id);
				bool scan_in_inner =
					PlanContainsNodeId(inner_plan,
									   parallel_worker_context->parallel_scan_plan_node_id);

				if (pg_volvec_trace_hooks)
					elog(LOG,
						 "pg_volvec: hash join parallel side binding hash_node=%d scan_plan_node_id=%d scan_relid=%u outer_contains=%s inner_contains=%s outer_relid=%u inner_relid=%u initial_build_outer=%s",
						 plan->plan_node_id,
						 parallel_worker_context->parallel_scan_plan_node_id,
						 parallel_worker_context->parallel_scan_relid,
						 scan_in_outer ? "true" : "false",
						 scan_in_inner ? "true" : "false",
						 outer_relid,
						 inner_relid,
						 build_outer_side ? "true" : "false");

				if (scan_in_outer)
					build_outer_side = false;
				else if (scan_in_inner)
					build_outer_side = true;
				else if (OidIsValid(parallel_worker_context->parallel_scan_relid))
				{
					if (parallel_worker_context->parallel_scan_relid == outer_relid)
						build_outer_side = false;
					else if (parallel_worker_context->parallel_scan_relid == inner_relid)
						build_outer_side = true;
				}
			}
			if (build_outer_side && pg_volvec_trace_hooks)
				elog(LOG,
					 "pg_volvec: building hash table from outer side (outer_rows=%.0f inner_rows=%.0f)",
					 outer_plan->plan_rows,
					 inner_plan->plan_rows);
			PartitionJoinClauses(hash_join->hashclauses, &hash_key_clauses, &hash_residual_clauses);
			BuildBinaryJoinChildRequiredAttrs(&hash_join->join.plan,
											  (Node *) hash_join->hashclauses,
											  outer_plan,
											  inner_plan,
											  &outer_required_attrs,
											  &inner_required_attrs);
			outer = ExecInitVecPlanInternal(outer_plan, estate, outer_required_attrs, false, parallel_worker_context);
			inner = ExecInitVecPlanInternal(inner_plan, estate, inner_required_attrs, false, parallel_worker_context);
			if (!outer || !inner)
			{
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: hash join child initialization failed (outer=%s inner=%s)",
						 outer ? "ok" : "null", inner ? "ok" : "null");
				return nullptr;
		}
		outer_state = outer.get();
		inner_state = inner.get();
		if (!ExtractJoinKeysFromClauses(hash_key_clauses,
									   "hash join",
									   outer_plan,
									   inner_plan,
									   outer_state,
									   inner_state,
									   &key_cols))
			return nullptr;
		if (!BuildJoinOutputCols(hash_join->join.plan.targetlist,
								 outer_plan,
								 inner_plan,
								 outer_state,
								 inner_state,
								 &output_cols,
								 &needs_project))
			return nullptr;
		join_filter_expr = BuildCombinedQualExpr(hash_residual_clauses,
												 hash_join->join.joinqual);
		if (hash_join->join.plan.qual != NIL)
		{
			List *quals = join_filter_expr != nullptr ?
				list_make1(join_filter_expr) : NIL;

			quals = list_concat(quals, list_copy(hash_join->join.plan.qual));
			join_filter_expr = quals == NIL ? nullptr :
				(Expr *) make_ands_explicit(quals);
		}
			if (join_filter_expr != nullptr)
			{
				use_lookup_filter =
					TryBuildCorrelatedLookupFilterSpec(join_filter_expr,
												   outer_plan,
												   inner_plan,
												   outer_state,
													   inner_state,
													   &output_cols,
													   estate,
													   &lookup_filter_spec,
													   parallel_worker_context);
			if (use_lookup_filter)
			{
				join_filter_expr = lookup_filter_spec.rewritten_expr;
			}
			else
			{
				join_filter_expr = RewriteHashJoinFilterExpr(join_filter_expr,
															 outer_plan,
															 inner_plan,
															 outer_state,
															 inner_state,
															 &output_cols);
			}
			if (join_filter_expr == nullptr)
			{
				if (pg_volvec_trace_hooks)
					elog(LOG, "pg_volvec: hash join filter rewrite failed");
				return nullptr;
			}
			}
			if (hash_join->join.jointype == JOIN_ANTI)
			{
				for (const auto &output_col : output_cols)
				{
					if (output_col.output_resno > visible_output_count)
						continue;
					if (output_col.side != VecJoinSide::Outer)
					{
						if (pg_volvec_trace_hooks)
							elog(LOG, "pg_volvec: anti hash join cannot expose inner columns");
						return nullptr;
					}
				}
			}
			if (hash_join->join.jointype == JOIN_RIGHT_ANTI)
			{
				for (const auto &output_col : output_cols)
				{
					if (output_col.output_resno > visible_output_count)
						continue;
					if (output_col.side != VecJoinSide::Inner)
					{
						if (pg_volvec_trace_hooks)
							elog(LOG, "pg_volvec: right anti hash join cannot expose outer columns");
						return nullptr;
					}
				}
			}
			if (hash_join->join.jointype == JOIN_ANTI)
			{
				build_outer_side = true;
				if (pg_volvec_trace_hooks)
					elog(LOG,
						 "pg_volvec: anti hash join building hash table from outer side (outer_rows=%.0f inner_rows=%.0f)",
						 outer_plan->plan_rows,
						 inner_plan->plan_rows);
			}
			std::unique_ptr<VecHashJoinState> join_state = std::make_unique<VecHashJoinState>(
				std::move(outer),
				std::move(inner),
				hash_join->join.plan.plan_node_id,
				hash_join->join.jointype,
				build_outer_side,
				visible_output_count,
				std::move(output_cols),
				std::move(key_cols));
			VecHashJoinState *join_state_ptr = join_state.get();
			current_state = std::move(join_state);
		if (use_lookup_filter)
		{
			current_state = std::make_unique<VecLookupProjectState>(
				std::move(current_state),
				std::move(lookup_filter_spec.lookup_state),
				lookup_filter_spec.input_key_col,
				lookup_filter_spec.input_key_meta,
				lookup_filter_spec.lookup_key_col,
				lookup_filter_spec.lookup_key_meta,
				lookup_filter_spec.lookup_value_col,
				lookup_filter_spec.output_resno,
				lookup_filter_spec.output_meta);
		}
		if (join_filter_expr != nullptr)
		{
			auto program = std::make_unique<VecExprProgram>();

			CompileExpr(join_filter_expr, *program, true, estate);
			AdjustProgramVarScales(program.get(), current_state.get());
			if (program->get_final_res_idx() < 0)
			{
				if (pg_volvec_trace_hooks)
					elog(LOG, "pg_volvec: hash join filter expression compilation failed");
				return nullptr;
			}
			if (hash_join->join.jointype == JOIN_ANTI ||
				hash_join->join.jointype == JOIN_RIGHT_ANTI)
				join_state_ptr->set_join_filter_program(std::move(program));
			else
			{
				current_state = std::make_unique<VecFilterState>(std::move(current_state), std::move(program));
				plan_qual_already_applied = true;
			}
		}
		if (needs_project)
		{
			current_state = BuildJoinProject(std::move(current_state),
											 hash_join->join.plan.targetlist,
											 outer_plan,
											 inner_plan,
											 outer_state,
											 inner_state,
											 &output_cols,
											 estate);
			if (!current_state)
				return nullptr;
		}
	} else if (IsA(plan, MergeJoin)) {
		MergeJoin *merge_join = (MergeJoin *) plan;
		Expr *join_filter_expr = nullptr;
		VolVecVector<VecJoinOutputCol> output_cols{PgMemoryContextAllocator<VecJoinOutputCol>(CurrentMemoryContext)};
		VolVecVector<VecHashJoinKeyCol> key_cols{PgMemoryContextAllocator<VecHashJoinKeyCol>(CurrentMemoryContext)};
		bool needs_project = false;
		Bitmapset *outer_required_attrs = nullptr;
		Bitmapset *inner_required_attrs = nullptr;
		std::unique_ptr<VecPlanState> outer;
		std::unique_ptr<VecPlanState> inner;
		VecPlanState *outer_state = nullptr;
		VecPlanState *inner_state = nullptr;

		if (merge_join->join.jointype != JOIN_INNER)
		{
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: merge join fallback only supports inner joins");
			return nullptr;
		}
		BuildBinaryJoinChildRequiredAttrs(&merge_join->join.plan,
										  (Node *) merge_join->mergeclauses,
										  plan->lefttree,
										  plan->righttree,
										  &outer_required_attrs,
										  &inner_required_attrs);
		outer = ExecInitVecPlanInternal(plan->lefttree, estate, outer_required_attrs, false, parallel_worker_context);
		inner = ExecInitVecPlanInternal(plan->righttree, estate, inner_required_attrs, false, parallel_worker_context);
		if (!outer || !inner)
		{
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: merge join child initialization failed (outer=%s inner=%s)",
					 outer ? "ok" : "null", inner ? "ok" : "null");
			return nullptr;
		}
		outer_state = outer.get();
		inner_state = inner.get();
		if (!ExtractJoinKeysFromClauses(merge_join->mergeclauses,
									   "merge join",
									   plan->lefttree,
									   plan->righttree,
									   outer_state,
									   inner_state,
									   &key_cols))
			return nullptr;
		if (!BuildJoinOutputCols(merge_join->join.plan.targetlist,
								 plan->lefttree,
								 plan->righttree,
								 outer_state,
								 inner_state,
								 &output_cols,
								 &needs_project))
			return nullptr;
		join_filter_expr = BuildCombinedQualExpr(merge_join->join.joinqual,
												 merge_join->join.plan.qual);
		if (join_filter_expr != nullptr)
		{
			join_filter_expr = RewriteHashJoinFilterExpr(join_filter_expr,
														 plan->lefttree,
														 plan->righttree,
														 outer_state,
														 inner_state,
														 &output_cols);
			if (join_filter_expr == nullptr)
			{
				if (pg_volvec_trace_hooks)
					elog(LOG, "pg_volvec: merge join filter rewrite failed");
				return nullptr;
			}
		}
		current_state = std::make_unique<VecHashJoinState>(std::move(outer),
														   std::move(inner),
														   merge_join->join.plan.plan_node_id,
														   JOIN_INNER,
														   false,
														   CountVisibleTargetEntries(merge_join->join.plan.targetlist),
														   std::move(output_cols),
														   std::move(key_cols));
		if (join_filter_expr != nullptr)
		{
			auto program = std::make_unique<VecExprProgram>();

			CompileExpr(join_filter_expr, *program, true, estate);
			AdjustProgramVarScales(program.get(), current_state.get());
			if (program->get_final_res_idx() < 0)
			{
				if (pg_volvec_trace_hooks)
					elog(LOG, "pg_volvec: merge join filter expression compilation failed");
				return nullptr;
			}
			current_state = std::make_unique<VecFilterState>(std::move(current_state), std::move(program));
			plan_qual_already_applied = true;
		}
		if (needs_project)
		{
			current_state = BuildJoinProject(std::move(current_state),
											 merge_join->join.plan.targetlist,
											 plan->lefttree,
											 plan->righttree,
											 outer_state,
											 inner_state,
											 &output_cols,
											 estate);
			if (!current_state)
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
		LookupMembershipFilterSpec lookup_filter_spec;
		bool use_lookup_filter = false;

		if (IsA(plan, Agg))
		{
			Expr *rewritten_qual = RewriteExprAgainstTargetList(combined_qual, plan->targetlist);

			if (rewritten_qual != nullptr)
				combined_qual = rewritten_qual;
		}
		if (IsA(plan, SeqScan))
		{
			CorrelatedLookupProjectSpec lookup_project_spec;

			use_lookup_filter =
				TryBuildLookupMembershipFilterSpec(combined_qual,
												 current_state.get(),
												 estate,
												 &lookup_filter_spec);
			if (use_lookup_filter)
			{
				current_state = std::make_unique<VecLookupFilterState>(
					std::move(current_state),
					std::move(lookup_filter_spec.lookup_state),
					lookup_filter_spec.input_key_col,
					lookup_filter_spec.input_key_meta,
					lookup_filter_spec.lookup_key_col,
					lookup_filter_spec.lookup_key_meta,
					lookup_filter_spec.negate);
				combined_qual = lookup_filter_spec.residual_expr;
			}
			if (combined_qual != nullptr &&
				TryBuildPlanCorrelatedLookupProjectSpec(combined_qual,
													 current_state.get(),
													 estate,
													 &lookup_project_spec))
			{
				current_state = std::make_unique<VecLookupProjectStateMultiKey>(
					std::move(current_state),
					std::move(lookup_project_spec.lookup_state),
					lookup_project_spec.num_keys,
					lookup_project_spec.input_key_cols,
					lookup_project_spec.input_key_metas,
					lookup_project_spec.lookup_key_cols,
					lookup_project_spec.lookup_key_metas,
					lookup_project_spec.lookup_value_col,
					lookup_project_spec.output_resno,
					lookup_project_spec.output_meta);
				combined_qual = lookup_project_spec.rewritten_expr;
			}
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

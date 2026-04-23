#include "exec/internal.hpp"

namespace pg_volvec {

namespace {

static void
SetCanonicalFailure(const char **failure_reason, const char *message)
{
	if (failure_reason != nullptr)
		*failure_reason = message;
}

static bool
ResolveFinalizeInputVarToPartialExpr(Plan *gather_plan,
									 Agg *partial_agg,
									 const Var *input_var,
									 Expr **partial_expr_out,
									 const char **failure_reason)
{
	TargetEntry *gather_tle;
	Expr *gather_expr;
	TargetEntry *partial_tle;

	if (partial_expr_out != nullptr)
		*partial_expr_out = nullptr;
	if (gather_plan == nullptr || partial_agg == nullptr || input_var == nullptr)
	{
		SetCanonicalFailure(failure_reason,
							"finalize aggregate canonicalization received invalid inputs");
		return false;
	}

	gather_tle = get_tle_by_resno(gather_plan->targetlist, input_var->varattno);
	if (gather_tle == nullptr)
	{
		SetCanonicalFailure(
			failure_reason,
			psprintf("finalize aggregate canonicalization could not find gather target entry for resno %d",
					 input_var->varattno));
		return false;
	}

	gather_expr = StripImplicitNodesLocal((Expr *) gather_tle->expr);
	if (gather_expr == nullptr || !IsA(gather_expr, Var))
	{
		SetCanonicalFailure(
			failure_reason,
			psprintf("finalize aggregate canonicalization requires gather resno %d to be a Var",
					 input_var->varattno));
		return false;
	}

	partial_tle = get_tle_by_resno(partial_agg->plan.targetlist,
								   ((Var *) gather_expr)->varattno);
	if (partial_tle == nullptr)
	{
		SetCanonicalFailure(
			failure_reason,
			psprintf("finalize aggregate canonicalization could not find partial target entry for gather resno %d mapped to partial resno %d",
					 input_var->varattno,
					 ((Var *) gather_expr)->varattno));
		return false;
	}
	if (partial_tle->expr == nullptr)
	{
		SetCanonicalFailure(
			failure_reason,
			psprintf("finalize aggregate canonicalization found null partial expression for resno %d",
					 partial_tle->resno));
		return false;
	}

	if (partial_expr_out != nullptr)
		*partial_expr_out = (Expr *) partial_tle->expr;
	return true;
}

struct FinalizeAggRewriteContext
{
	Plan *gather_plan = nullptr;
	Agg *partial_agg = nullptr;
	const char **failure_reason = nullptr;
};

static Node *
RewriteFinalizeAggMutator(Node *node, FinalizeAggRewriteContext *context)
{
	Expr *partial_expr = nullptr;

	if (node == nullptr)
		return nullptr;

	if (IsA(node, Aggref))
	{
		Aggref *finalize_aggref = (Aggref *) node;
		TargetEntry *arg_tle;
		Expr *arg_expr;
		Expr *mapped_partial_expr = nullptr;
		Aggref *partial_aggref;
		Aggref *rewritten;

		if (list_length(finalize_aggref->args) != 1)
		{
			SetCanonicalFailure(context->failure_reason,
								"finalize aggregate canonicalization currently requires a single finalize input");
			return nullptr;
		}

		arg_tle = (TargetEntry *) linitial(finalize_aggref->args);
		arg_expr = StripImplicitNodesLocal((Expr *) arg_tle->expr);
		if (arg_expr == nullptr || !IsA(arg_expr, Var))
		{
			SetCanonicalFailure(context->failure_reason,
								"finalize aggregate canonicalization requires finalize input to be a Var");
			return nullptr;
		}
		if (!ResolveFinalizeInputVarToPartialExpr(context->gather_plan,
												  context->partial_agg,
												  (Var *) arg_expr,
												  &mapped_partial_expr,
												  context->failure_reason))
			return nullptr;
		mapped_partial_expr = StripImplicitNodesLocal(mapped_partial_expr);
		if (mapped_partial_expr == nullptr || !IsA(mapped_partial_expr, Aggref))
		{
			SetCanonicalFailure(context->failure_reason,
								"finalize aggregate canonicalization expected mapped partial expression to be an Aggref");
			return nullptr;
		}

		partial_aggref = (Aggref *) mapped_partial_expr;
		rewritten = (Aggref *) copyObjectImpl(partial_aggref);
		rewritten->aggsplit = AGGSPLIT_SIMPLE;
		rewritten->aggtype = finalize_aggref->aggtype;
		rewritten->aggcollid = finalize_aggref->aggcollid;
		rewritten->inputcollid = finalize_aggref->inputcollid;
		return (Node *) rewritten;
	}

	if (IsA(node, Var))
	{
		if (!ResolveFinalizeInputVarToPartialExpr(context->gather_plan,
												  context->partial_agg,
												  (Var *) node,
												  &partial_expr,
												  context->failure_reason))
			return nullptr;
		return (Node *) copyObjectImpl(partial_expr);
	}

	return expression_tree_mutator(node,
								   RewriteFinalizeAggMutator,
								   context);
}

static Plan *
FindPresortedAggInput(Plan *plan, Sort **presort_sort_out)
{
	Plan *current = plan;

	if (presort_sort_out != nullptr)
		*presort_sort_out = nullptr;
	while (current != nullptr &&
		   (IsA(current, Material) || IsA(current, Gather) || IsA(current, GatherMerge)) &&
		   current->lefttree != nullptr)
		current = current->lefttree;
	if (current != nullptr &&
		(IsA(current, Sort) || IsA(current, IncrementalSort)) &&
		current->lefttree != nullptr)
	{
		if (presort_sort_out != nullptr)
			*presort_sort_out = (Sort *) current;
		return current->lefttree;
	}
	return nullptr;
}

} /* namespace */

Plan *
TryCanonicalizeFinalizePartialAggregate(Agg *finalize_agg,
										Plan *gather_plan,
										Agg *partial_agg,
										const char **failure_reason)
{
	FinalizeAggRewriteContext context;
	Agg *canonical_agg;
	List *rewritten_targetlist = NIL;
	ListCell *lc;

	if (failure_reason != nullptr)
		*failure_reason = nullptr;
	if (finalize_agg == nullptr || gather_plan == nullptr || partial_agg == nullptr)
	{
		SetCanonicalFailure(failure_reason,
							"finalize aggregate canonicalization requires finalize/gather/partial nodes");
		return nullptr;
	}

	context.gather_plan = gather_plan;
	context.partial_agg = partial_agg;
	context.failure_reason = failure_reason;

	canonical_agg = (Agg *) copyObjectImpl(partial_agg);
	canonical_agg->aggsplit = AGGSPLIT_SIMPLE;

	foreach(lc, finalize_agg->plan.targetlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		TargetEntry *rewritten_tle = (TargetEntry *) copyObjectImpl(tle);
		Node *rewritten_expr = RewriteFinalizeAggMutator((Node *) tle->expr, &context);

		if (rewritten_expr == nullptr)
			return nullptr;
		rewritten_tle->expr = (Expr *) rewritten_expr;
		rewritten_targetlist = lappend(rewritten_targetlist, rewritten_tle);
	}

	canonical_agg->plan.targetlist = rewritten_targetlist;
	if (finalize_agg->plan.qual != NIL)
	{
		Node *rewritten_qual = RewriteFinalizeAggMutator(
			(Node *) make_ands_explicit(list_copy(finalize_agg->plan.qual)),
			&context);

		if (rewritten_qual == nullptr)
			return nullptr;
		canonical_agg->plan.qual = list_make1((Expr *) rewritten_qual);
	}
	else
		canonical_agg->plan.qual = NIL;

	if (pg_volvec_trace_hooks)
		elog(LOG,
			 "pg_volvec: canonicalized FinalizeAgg <- %s <- PartialAgg to simple aggregate (final aggsplit=%d partial aggsplit=%d)",
			 IsA(gather_plan, GatherMerge) ? "GatherMerge" : "Gather",
			 (int) finalize_agg->aggsplit,
			 (int) partial_agg->aggsplit);
	return (Plan *) canonical_agg;
}

bool
MatchPresortedGroupAggInputChain(Agg *agg,
								 Plan **agg_input_out,
								 Sort **presort_sort_out)
{
	Plan *agg_input;

	if (agg_input_out != nullptr)
		*agg_input_out = agg != nullptr ? agg->plan.lefttree : nullptr;
	if (presort_sort_out != nullptr)
		*presort_sort_out = nullptr;
	if (agg == nullptr || agg->plan.lefttree == nullptr ||
		agg->numCols <= 0 || agg->aggstrategy == AGG_HASHED)
	{
		if (pg_volvec_trace_hooks && agg != nullptr)
			elog(LOG,
				 "pg_volvec: presorted groupagg match skipped numCols=%d aggstrategy=%d child=%d",
				 agg->numCols,
				 (int) agg->aggstrategy,
				 agg->plan.lefttree != nullptr ? (int) nodeTag(agg->plan.lefttree) : -1);
		return false;
	}

	agg_input = FindPresortedAggInput(agg->plan.lefttree, presort_sort_out);
	if (agg_input == nullptr)
	{
		if (pg_volvec_trace_hooks)
			elog(LOG,
				 "pg_volvec: presorted groupagg match found no presort child=%d",
				 (int) nodeTag(agg->plan.lefttree));
		return false;
	}
	if (agg_input_out != nullptr)
		*agg_input_out = agg_input;
	if (pg_volvec_trace_hooks)
		elog(LOG,
			 "pg_volvec: presorted groupagg match child=%d input=%d",
			 (int) nodeTag(agg->plan.lefttree),
			 (int) nodeTag(agg_input));
	return true;
}

/* Relocated from deleted hash_join.cpp during P2.13d. */
namespace {

struct AggrefRewriteContext
{
	const VolVecVector<const Aggref *> *aggrefs;
	const VolVecVector<int> *aggresnos;
	const VolVecVector<const Expr *> *group_exprs;
	const VolVecVector<int> *group_resnos;
};

static bool
ExprContainsNumericDivisionWalker(Node *node, void *context)
{
	bool *found = (bool *) context;

	if (node == nullptr || *found)
		return false;
	if (IsA(node, OpExpr))
	{
		OpExpr *op = (OpExpr *) node;
		char *opname = get_opname(op->opno);

		if (opname != nullptr && strcmp(opname, "/") == 0)
		{
			*found = true;
			return false;
		}
	}
	return expression_tree_walker(node, ExprContainsNumericDivisionWalker, context);
}

static bool
ExprContainsNumericDivision(Node *node)
{
	bool found = false;

	if (node != nullptr)
		(void) ExprContainsNumericDivisionWalker(node, &found);
	return found;
}

static bool
CollectAggrefsWalker(Node *node, void *context)
{
	VolVecVector<const Aggref *> *aggrefs = (VolVecVector<const Aggref *> *) context;

	if (node == nullptr)
		return false;
	if (IsA(node, Aggref))
	{
		aggrefs->push_back((const Aggref *) node);
		return false;
	}
	return expression_tree_walker(node, CollectAggrefsWalker, context);
}

static Node *
ReplaceAggrefsWithVarsMutator(Node *node, AggrefRewriteContext *context)
{
	if (node == nullptr)
		return nullptr;
	if (IsA(node, Aggref))
	{
		const Aggref *aggref = (const Aggref *) node;

		for (size_t i = 0; i < context->aggrefs->size(); i++)
		{
			if ((*context->aggrefs)[i] == aggref)
			{
				int resno = (*context->aggresnos)[i];
				Var *replacement = makeVar(OUTER_VAR,
										   resno,
										   exprType((Node *) aggref),
										   exprTypmod((Node *) aggref),
										   exprCollation((Node *) aggref),
										   0);

				return (Node *) replacement;
			}
		}
		elog(ERROR, "pg_volvec could not rewrite aggregate reference");
	}
	if (IsRewriteExprNode(node) &&
		context->group_exprs != nullptr && context->group_resnos != nullptr)
	{
		Expr *expr = StripImplicitNodesLocal((Expr *) node);

		for (size_t i = 0; i < context->group_exprs->size(); i++)
		{
			if (!equal(expr, (*context->group_exprs)[i]))
				continue;
			return (Node *) makeVar(OUTER_VAR,
									(*context->group_resnos)[i],
									exprType(node),
									exprTypmod(node),
									exprCollation(node),
									0);
		}
	}
	return expression_tree_mutator(node, ReplaceAggrefsWithVarsMutator, context);
}

static bool
CollectAggGroupExprs(Agg *node,
					 VolVecVector<const Expr *> *group_exprs,
					 VolVecVector<int> *group_resnos,
					 List **synthetic_tlist,
					 int *next_resno)
{
	Plan *child_plan;

	if (node == nullptr || group_exprs == nullptr || group_resnos == nullptr ||
		synthetic_tlist == nullptr || next_resno == nullptr)
		return false;

	child_plan = node->plan.lefttree;
	if (node->numCols == 0)
		return true;
	if (child_plan == nullptr)
		return false;

	for (int g = 0; g < node->numCols; g++)
	{
		int child_resno = node->grpColIdx[g];
		TargetEntry *child_tle = get_tle_by_resno(child_plan->targetlist, child_resno);
		Expr *group_expr;

		if (child_tle == nullptr)
			return false;
		group_expr = StripImplicitNodesLocal((Expr *) child_tle->expr);
		if (group_expr == nullptr)
			return false;

		group_exprs->push_back(group_expr);
		group_resnos->push_back(*next_resno);
		*synthetic_tlist = lappend(*synthetic_tlist,
								   makeTargetEntry((Expr *) copyObjectImpl(child_tle->expr),
												   *next_resno,
												   NULL,
												   false));
		(*next_resno)++;
	}

	return true;
}

static bool
IsSimpleAggTargetExpr(Agg *node, Expr *expr)
{
	expr = StripImplicitNodesLocal(expr);
	return expr != nullptr &&
		(IsA(expr, Aggref) || IsA(expr, Var) ||
		 ResolveAggPassThroughExpr(node, expr, nullptr, nullptr));
}

static VecOutputStorageKind
InferProjectStorageKind(Expr *expr, VecExprProgram *program)
{
	Oid typid = exprType((Node *) expr);

	if (typid == BPCHAROID || typid == TEXTOID || typid == VARCHAROID)
		return VecOutputStorageKind::StringRef;
	if (typid == FLOAT8OID)
		return VecOutputStorageKind::Double;
	if (typid == NUMERICOID)
	{
		if (ExprContainsNumericDivision((Node *) expr))
			return VecOutputStorageKind::Double;
		return VecOutputStorageKind::NumericScaledInt64;
	}
	if (typid == INT8OID)
		return VecOutputStorageKind::Int64;
	return VecOutputStorageKind::Int32;
}

} /* namespace */

std::unique_ptr<VecPlanState>
BuildAggWithOptionalProject(std::unique_ptr<VecPlanState> left, Agg *node,
							EState *estate, bool suppress_qual_filter)
{
	bool simple_targets = true;
	bool needs_synthetic_path;
	ListCell *lc;

	foreach(lc, node->plan.targetlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);

		if (!IsSimpleAggTargetExpr(node, (Expr *) tle->expr))
		{
			simple_targets = false;
			break;
		}
	}

	needs_synthetic_path = !simple_targets || node->plan.qual != NIL;

	if (!needs_synthetic_path)
		return std::make_unique<VecAggState>(std::move(left), node);

	VolVecVector<const Aggref *> aggrefs{PgMemoryContextAllocator<const Aggref *>(CurrentMemoryContext)};
	VolVecVector<int> aggresnos{PgMemoryContextAllocator<int>(CurrentMemoryContext)};
	VolVecVector<const Expr *> group_exprs{PgMemoryContextAllocator<const Expr *>(CurrentMemoryContext)};
	VolVecVector<int> group_resnos{PgMemoryContextAllocator<int>(CurrentMemoryContext)};
	List *synthetic_tlist = NIL;
	int next_resno = 1;
	std::unique_ptr<VecPlanState> current_state;

	if (!CollectAggGroupExprs(node, &group_exprs, &group_resnos, &synthetic_tlist, &next_resno))
	{
		if (pg_volvec_trace_hooks)
			elog(LOG, "pg_volvec: aggregate project rewrite could not collect grouped expressions");
		return nullptr;
	}

	foreach(lc, node->plan.targetlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		(void) CollectAggrefsWalker((Node *) tle->expr, &aggrefs);
	}
	if (node->plan.qual != NIL)
		(void) CollectAggrefsWalker((Node *) node->plan.qual, &aggrefs);
	for (const Aggref *aggref : aggrefs)
	{
		TargetEntry *agg_tle = makeTargetEntry((Expr *) copyObjectImpl(aggref),
											   next_resno,
											   NULL,
											   false);
		aggresnos.push_back(next_resno);
		synthetic_tlist = lappend(synthetic_tlist, agg_tle);
		next_resno++;
	}
	if (aggrefs.empty())
	{
		if (group_exprs.empty())
		{
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: aggregate project rewrite found no Aggref or grouped expressions");
			return nullptr;
		}
	}

	Agg *synthetic = (Agg *) palloc0(sizeof(Agg));
	*synthetic = *node;
	synthetic->plan.targetlist = synthetic_tlist;
	synthetic->plan.qual = NIL;

	auto agg_state = std::make_unique<VecAggState>(std::move(left), synthetic);
	VolVecVector<VecProjectColDesc> project_cols{PgMemoryContextAllocator<VecProjectColDesc>(CurrentMemoryContext)};
	AggrefRewriteContext rewrite_context{&aggrefs, &aggresnos, &group_exprs, &group_resnos};
	current_state = std::move(agg_state);

	if (node->plan.qual != NIL && !suppress_qual_filter)
	{
		auto qual_program = std::make_unique<VecExprProgram>();
		Expr *combined_qual = (Expr *) make_ands_explicit(list_copy(node->plan.qual));
		Expr *rewritten_qual =
			(Expr *) ReplaceAggrefsWithVarsMutator((Node *) combined_qual, &rewrite_context);

		CompileExpr(rewritten_qual, *qual_program, true, estate);
		AdjustProgramVarScales(qual_program.get(), current_state.get());
		if (qual_program->get_final_res_idx() < 0)
		{
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: aggregate qual rewrite/compilation failed");
			return nullptr;
		}
		current_state = std::make_unique<VecFilterState>(std::move(current_state),
														 std::move(qual_program));
	}

	foreach(lc, node->plan.targetlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		VecProjectColDesc project_col;
		Expr *rewritten_expr =
			(Expr *) ReplaceAggrefsWithVarsMutator((Node *) tle->expr, &rewrite_context);
		Expr *stripped_expr = StripImplicitNodesLocal(rewritten_expr);

		project_col.target_resno = tle->resno;
		project_col.sql_type = exprType((Node *) tle->expr);
		if (stripped_expr != nullptr && IsA(stripped_expr, Var))
		{
			Var *var = (Var *) stripped_expr;
			VecOutputColMeta meta;

			if (var->varattno <= 0 || var->varattno > 16 ||
				!current_state->lookup_output_col_meta(var->varattno, &meta))
			{
				if (pg_volvec_trace_hooks)
					elog(LOG, "pg_volvec: aggregate project direct-var metadata lookup failed for target resno %d",
						 tle->resno);
				return nullptr;
			}
			project_col.expr = nullptr;
			project_col.storage_kind = meta.storage_kind;
			project_col.scale = meta.scale;
			project_col.direct_var = true;
			project_col.input_col = (uint16_t) (var->varattno - 1);
		}
		else if (MatchStringPrefixExpr(stripped_expr,
									   &project_col.input_col,
									   &project_col.string_prefix_len))
		{
			VecOutputColMeta meta;

			if (!left->lookup_output_col_meta(project_col.input_col + 1, &meta) ||
				meta.storage_kind != VecOutputStorageKind::StringRef)
			{
				if (pg_volvec_trace_hooks)
					elog(LOG, "pg_volvec: aggregate string-prefix project metadata lookup failed for target resno %d",
						 tle->resno);
				return nullptr;
			}
			project_col.expr = nullptr;
			project_col.storage_kind = VecOutputStorageKind::StringRef;
			project_col.scale = 0;
			project_col.string_prefix_var = true;
		}
		else
		{
			project_col.expr = std::make_unique<VecExprProgram>();
			CompileExpr(rewritten_expr, *project_col.expr, false, estate);
			AdjustProgramVarScales(project_col.expr.get(), current_state.get());
			if (project_col.expr->get_final_res_idx() < 0)
			{
				if (pg_volvec_trace_hooks)
					elog(LOG, "pg_volvec: aggregate project expression compilation failed for target resno %d",
						 tle->resno);
				return nullptr;
			}
			project_col.storage_kind = InferProjectStorageKind((Expr *) tle->expr, project_col.expr.get());
			project_col.scale = project_col.expr->get_register_scale(project_col.expr->get_final_res_idx());
		}
		project_cols.push_back(std::move(project_col));
	}

	return std::make_unique<VecProjectState>(std::move(current_state), std::move(project_cols));
}

} /* namespace pg_volvec */

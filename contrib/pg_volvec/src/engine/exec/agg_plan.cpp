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

} /* namespace pg_volvec */

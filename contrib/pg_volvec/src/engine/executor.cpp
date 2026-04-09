#include "volvec_engine.hpp"
#include "llvmjit_deform_datachunk.h"

#include <algorithm>
#include <cstring>

extern "C" {
#include "utils/lsyscache.h"
#include "access/tableam.h"
#include "access/visibilitymap.h"
#include "access/stratnum.h"
#include "executor/nodeSubplan.h"
#include "nodes/nodeFuncs.h"
#include "parser/parsetree.h"
#include "storage/bufmgr.h"

extern bool pg_volvec_jit_deform;
extern bool pg_volvec_trace_hooks;
}

namespace pg_volvec
{

static std::unique_ptr<VecPlanState>
ExecInitVecPlanInternal(Plan *plan, EState *estate, Bitmapset *required_attrs,
						  bool force_full_deform);

static bool
IsRewriteExprNode(Node *node)
{
	return node != nullptr &&
		(IsA(node, Var) ||
		 IsA(node, Const) ||
		 IsA(node, OpExpr) ||
		 IsA(node, FuncExpr) ||
		 IsA(node, BoolExpr) ||
		 IsA(node, CaseExpr) ||
		 IsA(node, Aggref) ||
		 IsA(node, RelabelType) ||
		 IsA(node, CoerceToDomain));
}

static Expr *
StripImplicitNodesLocal(Expr *expr)
{
	while (expr != nullptr)
	{
		if (IsA(expr, RelabelType))
			expr = ((RelabelType *) expr)->arg;
		else if (IsA(expr, CoerceToDomain))
			expr = ((CoerceToDomain *) expr)->arg;
		else if (IsA(expr, CoerceViaIO))
			expr = ((CoerceViaIO *) expr)->arg;
		else
			break;
	}

	return expr;
}

static bool
IsInt64LikeTypeLocal(Oid type)
{
	return type == NUMERICOID || type == INT8OID || type == INT4OID || type == INT2OID;
}

struct TargetListRewriteContext
{
	List *targetlist;
	bool failed;
};

static Node *
RewriteExprAgainstTargetListMutator(Node *node, TargetListRewriteContext *context)
{
	ListCell *lc;

	if (node == nullptr)
		return nullptr;
	if (context == nullptr || context->targetlist == NIL)
		return (Node *) copyObjectImpl(node);

	foreach(lc, context->targetlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		Expr *tle_expr;

		if (tle->resjunk)
			continue;
		tle_expr = StripImplicitNodesLocal((Expr *) tle->expr);
		if (tle_expr != nullptr && equal(node, tle_expr))
		{
			if (tle->resno <= 0 || tle->resno > 16)
			{
				context->failed = true;
				return nullptr;
			}
			return (Node *) makeVar(1,
									tle->resno,
									exprType(node),
									exprTypmod(node),
									exprCollation(node),
									0);
		}
	}

	return expression_tree_mutator(node,
								   RewriteExprAgainstTargetListMutator,
								   context);
}

static Expr *
RewriteExprAgainstTargetList(Expr *expr, List *targetlist)
{
	TargetListRewriteContext context;

	if (expr == nullptr)
		return nullptr;
	context.targetlist = targetlist;
	context.failed = false;

	Expr *rewritten = (Expr *) RewriteExprAgainstTargetListMutator((Node *) expr, &context);
	if (context.failed)
		return nullptr;
	return rewritten;
}

static bool
ShouldUseExactNumericAgg(Oid arg_type)
{
	return arg_type == NUMERICOID;
}

static VecOutputStorageKind
DefaultOutputStorageKindForType(Oid typid)
{
	if (typid == FLOAT8OID)
		return VecOutputStorageKind::Double;
	if (typid == NUMERICOID)
		return VecOutputStorageKind::NumericScaledInt64;
	if (typid == BPCHAROID || typid == TEXTOID || typid == VARCHAROID)
		return VecOutputStorageKind::StringRef;
	if (typid == INT8OID)
		return VecOutputStorageKind::Int64;
	return VecOutputStorageKind::Int32;
}

static uint64_t
EncodeFloat8SortKey(double value)
{
	uint64_t bits;

	memcpy(&bits, &value, sizeof(bits));
	if ((bits & (UINT64CONST(1) << 63)) != 0)
		return ~bits;
	return bits ^ (UINT64CONST(1) << 63);
}

static uint32_t
TrimBpcharLengthLocal(const char *data, uint32_t len)
{
	while (len > 0 && data[len - 1] == ' ')
		len--;
	return len;
}

static uint64_t
HashBytes64(const char *data, uint32_t len)
{
	uint64_t hash = UINT64CONST(1469598103934665603);

	for (uint32_t i = 0; i < len; i++)
	{
		hash ^= (unsigned char) data[i];
		hash *= UINT64CONST(1099511628211);
	}
	return hash;
}

static uint64_t
HashStringRefForGroupKey(const DataChunk<DEFAULT_CHUNK_SIZE> &chunk,
						 const VecStringRef &ref,
						 Oid sql_type,
						 uint32_t *len_out)
{
	const char *ptr = chunk.get_string_ptr(ref);
	uint32_t len = ref.len;

	if (sql_type == BPCHAROID)
		len = TrimBpcharLengthLocal(ptr, len);
	if (len_out != nullptr)
		*len_out = len;
	return HashBytes64(ptr, len);
}

static inline VecStringRef
CopyStringRefToChunk(DataChunk<DEFAULT_CHUNK_SIZE> &dst,
					 const DataChunk<DEFAULT_CHUNK_SIZE> &src,
					 const VecStringRef &ref)
{
	return dst.store_string_bytes(src.get_string_ptr(ref), ref.len);
}

static bool
TryExtractConstInt32Local(Const *c, int32_t *out)
{
	if (c == nullptr || c->constisnull || out == nullptr)
		return false;
	if (c->consttype == INT4OID)
	{
		*out = DatumGetInt32(c->constvalue);
		return true;
	}
	if (c->consttype == INT8OID)
	{
		int64_t value = DatumGetInt64(c->constvalue);

		if (value < PG_INT32_MIN || value > PG_INT32_MAX)
			return false;
		*out = (int32_t) value;
		return true;
	}
	return false;
}

static bool
ExtractStringSourceVarLocal(Expr *expr, Var **var_out)
{
	expr = StripImplicitNodesLocal(expr);
	if (var_out != nullptr)
		*var_out = nullptr;
	if (expr == nullptr)
		return false;
	if (IsA(expr, Var))
	{
		Oid type = exprType((Node *) expr);

		if (type != BPCHAROID && type != TEXTOID && type != VARCHAROID)
			return false;
		if (var_out != nullptr)
			*var_out = (Var *) expr;
		return true;
	}
	if (IsA(expr, FuncExpr))
	{
		FuncExpr *func = (FuncExpr *) expr;
		Oid rettype = exprType((Node *) expr);

		if (list_length(func->args) == 1 &&
			(rettype == BPCHAROID || rettype == TEXTOID || rettype == VARCHAROID))
			return ExtractStringSourceVarLocal((Expr *) linitial(func->args), var_out);
	}
	return false;
}

static bool
MatchStringPrefixExpr(Expr *expr, uint16_t *input_col, uint32_t *prefix_len)
{
	FuncExpr *func;
	char *funcname;
	Expr *arg_expr;
	Expr *start_expr;
	Expr *len_expr;
	Var *var = nullptr;
	int32_t start_val;
	int32_t len_val;

	expr = StripImplicitNodesLocal(expr);
	if (input_col != nullptr)
		*input_col = 0;
	if (prefix_len != nullptr)
		*prefix_len = 0;
	if (expr == nullptr || !IsA(expr, FuncExpr))
		return false;

	func = (FuncExpr *) expr;
	funcname = get_func_name(func->funcid);
	if (list_length(func->args) == 1 &&
		(exprType((Node *) expr) == BPCHAROID ||
		 exprType((Node *) expr) == TEXTOID ||
		 exprType((Node *) expr) == VARCHAROID))
		return MatchStringPrefixExpr((Expr *) linitial(func->args), input_col, prefix_len);
	if (funcname == nullptr ||
		(strcmp(funcname, "substring") != 0 && strcmp(funcname, "substr") != 0) ||
		(list_length(func->args) != 2 && list_length(func->args) != 3))
		return false;

	arg_expr = StripImplicitNodesLocal((Expr *) linitial(func->args));
	start_expr = StripImplicitNodesLocal((Expr *) lsecond(func->args));
	len_expr = list_length(func->args) == 3 ?
		StripImplicitNodesLocal((Expr *) lthird(func->args)) : nullptr;
	if (arg_expr == nullptr || start_expr == nullptr || !IsA(start_expr, Const))
		return false;
	if (!ExtractStringSourceVarLocal(arg_expr, &var) ||
		var == nullptr ||
		var->varattno <= 0 || var->varattno > 16 ||
		!TryExtractConstInt32Local((Const *) start_expr, &start_val) ||
		start_val != 1)
		return false;
	if (len_expr == nullptr || !IsA(len_expr, Const) ||
		!TryExtractConstInt32Local((Const *) len_expr, &len_val) ||
		len_val < 0)
		return false;

	if (input_col != nullptr)
		*input_col = (uint16_t) (var->varattno - 1);
	if (prefix_len != nullptr)
		*prefix_len = (uint32_t) len_val;
	return true;
}

static void
RecomputeProgramResultScales(VecExprProgram *program)
{
	auto clamp_scale = [](int scale) {
		if (scale < 0)
			return 0;
		if (scale > 18)
			return 18;
		return scale;
	};

	if (program == nullptr)
		return;

	for (const auto &step : program->steps)
	{
		int scale;

		switch (step.opcode)
		{
			case VecOpCode::EEOP_INT64_ADD:
			case VecOpCode::EEOP_INT64_SUB:
			case VecOpCode::EEOP_INT64_LT:
			case VecOpCode::EEOP_INT64_LE:
			case VecOpCode::EEOP_INT64_GT:
			case VecOpCode::EEOP_INT64_GE:
			case VecOpCode::EEOP_INT64_EQ:
			case VecOpCode::EEOP_INT64_NE:
				scale = Max(program->get_register_scale(step.d.op.left),
							program->get_register_scale(step.d.op.right));
				program->set_register_scale(step.res_idx, scale);
				break;
			case VecOpCode::EEOP_INT64_MUL:
				scale = clamp_scale(program->get_register_scale(step.d.op.left) +
								   program->get_register_scale(step.d.op.right));
				program->set_register_scale(step.res_idx, scale);
				break;
			case VecOpCode::EEOP_INT64_CASE:
				scale = Max(program->get_register_scale(step.d.ternary.if_true),
							program->get_register_scale(step.d.ternary.if_false));
				program->set_register_scale(step.res_idx, scale);
				break;
			case VecOpCode::EEOP_FLOAT8_ADD:
			case VecOpCode::EEOP_FLOAT8_SUB:
			case VecOpCode::EEOP_FLOAT8_MUL:
			case VecOpCode::EEOP_INT64_DIV_FLOAT8:
			case VecOpCode::EEOP_FLOAT8_LT:
			case VecOpCode::EEOP_FLOAT8_GT:
			case VecOpCode::EEOP_FLOAT8_LE:
			case VecOpCode::EEOP_FLOAT8_GE:
			case VecOpCode::EEOP_FLOAT8_CASE:
			case VecOpCode::EEOP_DATE_LT:
			case VecOpCode::EEOP_DATE_LE:
			case VecOpCode::EEOP_DATE_GT:
			case VecOpCode::EEOP_DATE_GE:
			case VecOpCode::EEOP_AND:
			case VecOpCode::EEOP_OR:
			case VecOpCode::EEOP_STR_EQ:
			case VecOpCode::EEOP_STR_NE:
			case VecOpCode::EEOP_STR_PREFIX_LIKE:
			case VecOpCode::EEOP_DATE_PART_YEAR:
			case VecOpCode::EEOP_QUAL:
				program->set_register_scale(step.res_idx, 0);
				break;
			default:
				break;
		}
	}
}

static void
AdjustProgramVarScales(VecExprProgram *program, VecPlanState *input_state)
{
	bool changed = false;

	if (program == nullptr || input_state == nullptr)
		return;

	for (const auto &step : program->steps)
	{
		VecOutputColMeta meta;

		if (step.opcode != VecOpCode::EEOP_VAR)
			continue;
		if (step.d.var.att_idx < 0 || step.d.var.att_idx >= 16)
			continue;
		if (!input_state->lookup_output_col_meta(step.d.var.att_idx + 1, &meta))
			continue;
		if (meta.storage_kind == VecOutputStorageKind::NumericScaledInt64 ||
			meta.storage_kind == VecOutputStorageKind::NumericAvgPair)
		{
			if (program->get_register_scale(step.res_idx) != meta.scale)
			{
				program->set_register_scale(step.res_idx, meta.scale);
				changed = true;
			}
		}
		else if (meta.storage_kind == VecOutputStorageKind::Int64)
		{
			if (program->get_register_scale(step.res_idx) != 0)
			{
				program->set_register_scale(step.res_idx, 0);
				changed = true;
			}
		}
	}

	if (!changed)
		return;

	RecomputeProgramResultScales(program);

#ifdef USE_LLVM
	if (program->jit_context != nullptr)
	{
		pg_volvec_release_llvm_jit_context((JitContext *) program->jit_context);
		program->jit_context = nullptr;
		program->jit_func = nullptr;
	}
#endif
	program->try_compile_jit();
}

static DeformDecodeKind
DecodeKindForType(Oid typid)
{
	if (typid == FLOAT8OID)
		return DeformDecodeKind::kFloat8;
	if (typid == NUMERICOID)
		return DeformDecodeKind::kNumeric;
	if (typid == BPCHAROID || typid == TEXTOID || typid == VARCHAROID)
		return DeformDecodeKind::kStringRef;
	if (typid == DATEOID)
		return DeformDecodeKind::kDate32;
	if (typid == INT8OID)
		return DeformDecodeKind::kInt64;
	return DeformDecodeKind::kInt32;
}

static bool
CollectAttrNosFromExprWalker(Node *node, Bitmapset **attrs)
{
	if (node == nullptr)
		return false;

	if (IsA(node, Var))
	{
		Var *var = (Var *) node;

		if (var->varlevelsup == 0 &&
			var->varattno > 0 &&
			var->varattno <= kMaxDeformTargets)
			*attrs = bms_add_member(*attrs, var->varattno - 1);
		return false;
	}

	return expression_tree_walker(node, CollectAttrNosFromExprWalker, attrs);
}

static void
CollectAttrNosFromExpr(Node *node, Bitmapset **attrs)
{
	if (node != nullptr)
		(void) CollectAttrNosFromExprWalker(node, attrs);
}

static void
CollectRequiredAttrsForPlan(Plan *plan, Bitmapset **attrs)
{
	if (plan == nullptr)
		return;

	if (plan->qual != NIL)
		CollectAttrNosFromExpr((Node *) plan->qual, attrs);

	/*
	 * Base scan targetlists are often the full physical tuple; collecting from
	 * them defeats pruning.  Instead, collect required Vars from upper plan
	 * nodes and scan quals only.
	 */
	if (!IsA(plan, SeqScan) && plan->targetlist != NIL)
		CollectAttrNosFromExpr((Node *) plan->targetlist, attrs);

	CollectRequiredAttrsForPlan(plan->lefttree, attrs);
	CollectRequiredAttrsForPlan(plan->righttree, attrs);
	if (IsA(plan, SubqueryScan))
		CollectRequiredAttrsForPlan(((SubqueryScan *) plan)->subplan, attrs);
}

static bool
ResolvePlanSourceAttno(Plan *plan, int target_resno, int *source_attno)
{
	ListCell   *lc;

	if (plan == nullptr || source_attno == nullptr ||
		target_resno <= 0 || target_resno > kMaxDeformTargets)
		return false;

	if (plan->targetlist != NIL)
	{
		foreach(lc, plan->targetlist)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(lc);
			Expr	   *expr;
			Var		   *var;

			if (tle->resno != target_resno)
				continue;

			expr = StripImplicitNodesLocal((Expr *) tle->expr);
			if (expr == nullptr || !IsA(expr, Var))
				return false;
			var = (Var *) expr;
			if (var->varlevelsup != 0 ||
				var->varattno <= 0 ||
				var->varattno > kMaxDeformTargets)
				return false;

				if (plan->lefttree != nullptr &&
					(IsA(plan, Hash) || IsA(plan, Sort) || IsA(plan, Limit) || IsA(plan, Material)))
					return ResolvePlanSourceAttno(plan->lefttree, var->varattno, source_attno);
				if (IsA(plan, SubqueryScan) &&
					((SubqueryScan *) plan)->subplan != nullptr)
					return ResolvePlanSourceAttno(((SubqueryScan *) plan)->subplan,
												 var->varattno,
												 source_attno);

			*source_attno = var->varattno;
			return true;
		}
	}

	*source_attno = target_resno;
	return true;
}

struct VecAttrCollectContext
{
	Index		wanted_varno;
	Plan	   *plan;
	Bitmapset **attrs;
};

static bool
CollectResolvedAttrsWalker(Node *node, VecAttrCollectContext *context)
{
	if (node == nullptr)
		return false;

	if (IsA(node, Var))
	{
		Var *var = (Var *) node;
		int source_attno;

		if (var->varlevelsup == 0 &&
			var->varattno > 0 &&
			(context->wanted_varno == 0 || var->varno == context->wanted_varno) &&
			ResolvePlanSourceAttno(context->plan, var->varattno, &source_attno) &&
			source_attno > 0 &&
			source_attno <= kMaxDeformTargets)
			*context->attrs = bms_add_member(*context->attrs, source_attno - 1);
		return false;
	}

	return expression_tree_walker(node, CollectResolvedAttrsWalker, context);
}

static void
CollectResolvedAttrs(Node *node, Index wanted_varno, Plan *plan, Bitmapset **attrs)
{
	VecAttrCollectContext context;

	if (node == nullptr || attrs == nullptr)
		return;

	context.wanted_varno = wanted_varno;
	context.plan = plan;
	context.attrs = attrs;
	(void) CollectResolvedAttrsWalker(node, &context);
}

static void
CollectLocalPlanQualAttrs(Plan *plan, Bitmapset **attrs)
{
	if (plan == nullptr)
		return;

	if (plan->qual != NIL)
		CollectResolvedAttrs((Node *) plan->qual, 0, plan, attrs);

	if (plan->lefttree != nullptr)
		CollectLocalPlanQualAttrs(plan->lefttree, attrs);
	if (plan->righttree != nullptr)
		CollectLocalPlanQualAttrs(plan->righttree, attrs);
	if (IsA(plan, SubqueryScan))
		CollectLocalPlanQualAttrs(((SubqueryScan *) plan)->subplan, attrs);
}

static void
BuildBinaryJoinChildRequiredAttrs(Plan *join_plan,
								  Node *key_clauses,
								  Plan *outer_plan,
								  Plan *inner_plan,
								  Bitmapset **outer_attrs,
								  Bitmapset **inner_attrs)
{
	Join *join = (Join *) join_plan;

	CollectResolvedAttrs((Node *) join_plan->targetlist, OUTER_VAR, outer_plan, outer_attrs);
	CollectResolvedAttrs((Node *) join_plan->targetlist, INNER_VAR, inner_plan, inner_attrs);
	CollectResolvedAttrs(key_clauses, OUTER_VAR, outer_plan, outer_attrs);
	CollectResolvedAttrs(key_clauses, INNER_VAR, inner_plan, inner_attrs);
	CollectResolvedAttrs((Node *) join->joinqual, OUTER_VAR, outer_plan, outer_attrs);
	CollectResolvedAttrs((Node *) join->joinqual, INNER_VAR, inner_plan, inner_attrs);
	CollectResolvedAttrs((Node *) join_plan->qual, OUTER_VAR, outer_plan, outer_attrs);
	CollectResolvedAttrs((Node *) join_plan->qual, INNER_VAR, inner_plan, inner_attrs);
	CollectLocalPlanQualAttrs(outer_plan, outer_attrs);
	CollectLocalPlanQualAttrs(inner_plan, inner_attrs);
}

static Expr *
BuildCombinedQualExpr(List *joinqual, List *planqual)
{
	List *quals = NIL;

	if (joinqual != NIL)
		quals = list_concat(quals, list_copy(joinqual));
	if (planqual != NIL)
		quals = list_concat(quals, list_copy(planqual));
	if (quals == NIL)
		return nullptr;
	return (Expr *) make_ands_explicit(quals);
}

static int
CountVisibleTargetEntries(List *targetlist)
{
	ListCell *lc;
	int count = 0;

	foreach(lc, targetlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);

		if (!tle->resjunk)
			count++;
	}
	return count;
}

static bool
ShouldSwapInnerJoinBuildSides(JoinType jointype, Plan *outer_plan, Plan *inner_plan)
{
	double outer_rows;
	double inner_rows;

	if (jointype != JOIN_INNER || outer_plan == nullptr || inner_plan == nullptr)
		return false;
	outer_rows = outer_plan->plan_rows;
	inner_rows = inner_plan->plan_rows;
	if (outer_rows <= 0 || inner_rows <= 0)
		return false;
	return outer_rows < inner_rows;
}

static bool
ShouldBuildSmallerSide(Plan *outer_plan, Plan *inner_plan)
{
	double outer_rows;
	double inner_rows;

	if (outer_plan == nullptr || inner_plan == nullptr)
		return false;
	outer_rows = outer_plan->plan_rows;
	inner_rows = inner_plan->plan_rows;
	if (outer_rows <= 0 || inner_rows <= 0)
		return false;
	return outer_rows < inner_rows;
}

static bool
RewriteSemiJoinVisibleInnerOutputsToOuterKeys(VolVecVector<VecJoinOutputCol> *output_cols,
											  const VolVecVector<VecHashJoinKeyCol> &key_cols,
											  int visible_output_count)
{
	if (output_cols == nullptr)
		return false;

	for (auto &output_col : *output_cols)
	{
		bool matched = false;

		if (output_col.output_resno > visible_output_count ||
			output_col.side != VecJoinSide::Inner)
			continue;
		for (const auto &key_col : key_cols)
		{
			if (key_col.inner_col != output_col.input_col)
				continue;
			output_col.side = VecJoinSide::Outer;
			output_col.input_col = key_col.outer_col;
			output_col.meta = VecOutputColMeta{output_col.meta.sql_type, key_col.kind, output_col.meta.scale};
			matched = true;
			break;
		}
		if (!matched)
			return false;
	}

	return true;
}

static void
BuildPrunedDeformProgram(Bitmapset *attrs, TupleDesc desc, DeformProgram *program)
{
	int att_index = -1;

	program->reset();
	if (attrs == nullptr)
	{
		for (int i = 0; i < desc->natts && i < kMaxDeformTargets; i++)
			program->add_target(i, i, DecodeKindForType(TupleDescAttr(desc, i)->atttypid));
		program->finalize();
		return;
	}

	while ((att_index = bms_next_member(attrs, att_index)) >= 0)
	{
		if (att_index >= desc->natts || att_index >= kMaxDeformTargets)
			continue;
		program->add_target(att_index, att_index,
							DecodeKindForType(TupleDescAttr(desc, att_index)->atttypid));
	}

	program->finalize();
}

static bool
ResolveAggPassThroughExpr(Agg *node,
						  Expr *expr,
						  int *input_col,
						  int *group_key_pos)
{
	Plan *child_plan;
	Expr *stripped_expr;
	ListCell *lc;

	if (node == nullptr || node->plan.lefttree == nullptr || expr == nullptr)
		return false;

	child_plan = node->plan.lefttree;
	stripped_expr = StripImplicitNodesLocal(expr);
	foreach(lc, child_plan->targetlist)
	{
		TargetEntry *child_tle = (TargetEntry *) lfirst(lc);
		Expr *child_expr = StripImplicitNodesLocal((Expr *) child_tle->expr);

		if (child_tle->resno <= 0 || child_tle->resno > 16 || child_expr == nullptr)
			continue;
		if (!equal(stripped_expr, child_expr))
			continue;

		for (int g = 0; g < node->numCols; g++)
		{
			if (node->grpColIdx[g] != child_tle->resno)
				continue;
			if (input_col != nullptr)
				*input_col = child_tle->resno - 1;
			if (group_key_pos != nullptr)
				*group_key_pos = g;
			return true;
		}
	}

	return false;
}

/* --- Optimized DataChunkDeformer --- */
void DataChunkDeformer::deform_tuple_header(HeapTupleHeader tuphdr, uint32 row_idx, const DeformBindings &bindings) {
	if (jit_func_) {
		if (pg_volvec_trace_hooks && !jit_path_logged_) {
			elog(LOG, "pg_volvec: using deform JIT path for row deconstruction");
			jit_path_logged_ = true;
		}
		jit_func_(tuphdr,
				  (void**)bindings.columns_data,
				  (uint8_t**)bindings.columns_nulls,
				  row_idx,
				  bindings.owner_chunk);
		return;
	}
	/* 
	 * SPECIALIZED DEFORMER FOR TPCH:
	 * We skip the generic heap_getattr and use a more direct approach.
	 */
	HeapTupleData tuple; tuple.t_len = HeapTupleHeaderGetDatumLength(tuphdr); tuple.t_data = tuphdr;
	
	for (int i = 0; i < program_.ntargets; i++) {
		const auto &target = program_.targets[i]; bool isnull;
		Datum val = heap_getattr(&tuple, target.att_index + 1, desc_, &isnull);
		bindings.columns_nulls[target.dst_col][row_idx] = (uint8_t)isnull;
		if (!isnull) {
			if (target.decode_kind == DeformDecodeKind::kInt32 || target.decode_kind == DeformDecodeKind::kDate32)
				((int32_t*)bindings.columns_data[target.dst_col])[row_idx] = DatumGetInt32(val);
			else if (target.decode_kind == DeformDecodeKind::kNumeric) {
				int scale = GetNumericScaleFromTypmod(TupleDescAttr(desc_, target.att_index)->atttypmod);
				int64_t scaled = 0;

				if (!TryFastNumericToScaledInt64(val, scale, &scaled))
					elog(ERROR, "pg_volvec fast numeric decode failed for attribute %d", target.att_index + 1);
				((int64_t*)bindings.columns_data[target.dst_col])[row_idx] = scaled;
			}
			else if (target.decode_kind == DeformDecodeKind::kFloat8)
				((double*)bindings.columns_data[target.dst_col])[row_idx] = DatumGetFloat8(val);
			else if (target.decode_kind == DeformDecodeKind::kInt64)
				((int64_t*)bindings.columns_data[target.dst_col])[row_idx] = DatumGetInt64(val);
				else if (target.decode_kind == DeformDecodeKind::kStringRef) {
					struct varlena *v = (struct varlena *) DatumGetPointer(val);
					char *vptr = VARDATA_ANY(v);
					int len = VARSIZE_ANY_EXHDR(v);
					((VecStringRef*)bindings.columns_data[target.dst_col])[row_idx] =
						bindings.owner_chunk->store_string_bytes(vptr, (uint32_t) len);
				}
		}
	}
}

/* --- VecAggState --- */
VecAggState::VecAggState(std::unique_ptr<VecPlanState> left, Agg *node)
		: left_(std::move(left)),
		  node_(node),
		  memory_context_(CurrentMemoryContext),
		  grp_col_indices_(PgMemoryContextAllocator<int>(memory_context_)),
		  grp_col_meta_(PgMemoryContextAllocator<VecOutputColMeta>(memory_context_)),
		  aggs_(PgMemoryContextAllocator<VecAggDesc>(memory_context_)),
		  hash_table_(0, VecGroupKeyHash{}, std::equal_to<VecGroupKey>{},
					 PgMemoryContextAllocator<std::pair<const VecGroupKey, VecAggGroupState>>(memory_context_)),
		  simple_hash_table_(0, VecSimpleGroupKeyHash{}, std::equal_to<VecSimpleGroupKey>{},
							PgMemoryContextAllocator<std::pair<const VecSimpleGroupKey, VecAggGroupState>>(memory_context_)),
		  rep_chunks_(PgMemoryContextAllocator<DataChunk<DEFAULT_CHUNK_SIZE> *>(memory_context_)),
		  fully_scanned_(false)
{
		for (int i = 0; i < node->numCols; i++) {
			VecOutputColMeta meta;
			int target_resno = node->grpColIdx[i];

			grp_col_indices_.push_back(target_resno - 1);
			if (left_ == nullptr || !left_->lookup_output_col_meta(target_resno, &meta))
			{
				meta.sql_type = InvalidOid;
				meta.storage_kind = VecOutputStorageKind::Int32;
				meta.scale = 0;
			}
			grp_col_meta_.push_back(meta);
		}
		if (grp_col_indices_.size() == 1 &&
			(grp_col_meta_[0].storage_kind == VecOutputStorageKind::Int32 ||
			 grp_col_meta_[0].storage_kind == VecOutputStorageKind::Int64 ||
			 grp_col_meta_[0].storage_kind == VecOutputStorageKind::NumericScaledInt64))
		{
			use_simple_group_key_ = true;
			simple_group_storage_ = grp_col_meta_[0].storage_kind;
		}
		if (node != nullptr && node->plan.plan_rows > 0)
		{
			double estimated_groups = node->plan.plan_rows;
			size_t reserve_count;

			if (estimated_groups > (double) (SIZE_MAX / 2))
				reserve_count = SIZE_MAX / 2;
			else
				reserve_count = (size_t) estimated_groups + 1;
			if (use_simple_group_key_)
				simple_hash_table_.reserve(reserve_count);
			else
				hash_table_.reserve(reserve_count);
		}
		ListCell *lc;
		foreach(lc, node->plan.targetlist) {
			TargetEntry *tle = (TargetEntry *) lfirst(lc);
			VecAggDesc desc; desc.target_resno = tle->resno;
			desc.output_type = exprType((Node *) tle->expr);
			desc.output_storage = DefaultOutputStorageKindForType(desc.output_type);
		if (IsA(tle->expr, Aggref)) {
			Aggref *aggref = (Aggref *) tle->expr;
			char *aggname = get_func_name(aggref->aggfnoid);
			if (aggname && strcmp(aggname, "sum") == 0) desc.type = VecAggType::SUM;
			else if (aggname && strcmp(aggname, "count") == 0) desc.type = VecAggType::COUNT;
			else if (aggname && strcmp(aggname, "avg") == 0) desc.type = VecAggType::AVG;
			else if (aggname && strcmp(aggname, "max") == 0) desc.type = VecAggType::MAX;
			else desc.type = VecAggType::SUM;
			desc.is_distinct = (aggref->aggdistinct != NIL);
			if (aggref->args != NIL) {
				TargetEntry *arg_tle = (TargetEntry *) linitial(aggref->args);
				desc.arg_type = exprType((Node *) arg_tle->expr);
				if (pg_volvec_trace_hooks && desc.is_distinct)
				{
					Expr *arg_expr = StripImplicitNodesLocal((Expr *) arg_tle->expr);

					if (arg_expr != nullptr && IsA(arg_expr, Var))
						elog(LOG,
							 "pg_volvec: distinct agg arg varattno=%d vartype=%u",
							 ((Var *) arg_expr)->varattno,
							 ((Var *) arg_expr)->vartype);
					else
						elog(LOG,
							 "pg_volvec: distinct agg arg expr node=%d type=%u",
							 arg_expr != nullptr ? (int) nodeTag(arg_expr) : -1,
							 desc.arg_type);
				}
				desc.arg_expr = std::make_unique<VecExprProgram>();
				CompileExpr((Expr *) arg_tle->expr, *desc.arg_expr, false);
				AdjustProgramVarScales(desc.arg_expr.get(), left_.get());
				if (desc.arg_expr->get_final_res_idx() >= 0)
				{
					Expr *arg_expr = StripImplicitNodesLocal((Expr *) arg_tle->expr);

					if (arg_expr != nullptr && IsA(arg_expr, Var) && left_ != nullptr)
					{
						VecOutputColMeta input_meta;
						Var *var = (Var *) arg_expr;

						if (left_->lookup_output_col_meta(var->varattno, &input_meta) &&
							input_meta.storage_kind == VecOutputStorageKind::NumericScaledInt64)
							desc.arg_expr->set_register_scale(desc.arg_expr->get_final_res_idx(),
															  input_meta.scale);
					}
				}
				if (desc.arg_expr->get_final_res_idx() >= 0)
					desc.numeric_scale = desc.arg_expr->get_register_scale(desc.arg_expr->get_final_res_idx());
				desc.use_exact_numeric = ShouldUseExactNumericAgg(desc.arg_type) &&
					desc.arg_expr->get_final_res_idx() >= 0;
				if (desc.use_exact_numeric)
				{
					if (desc.type == VecAggType::AVG)
						desc.output_storage = VecOutputStorageKind::NumericAvgPair;
					else
						desc.output_storage = VecOutputStorageKind::NumericScaledInt64;
				}
				else if (desc.type == VecAggType::COUNT)
					desc.output_storage = VecOutputStorageKind::Int64;
				else if (desc.output_type == NUMERICOID)
					desc.output_storage = VecOutputStorageKind::Double;
			} else desc.arg_expr = nullptr;
			} else {
				Expr *expr = StripImplicitNodesLocal((Expr *) tle->expr);
				desc.type = VecAggType::MAX;
				desc.arg_expr = nullptr;
				if (expr != nullptr && IsA(expr, Var))
				{
					Var *var = (Var *) expr;
					VecOutputColMeta input_meta;
					desc.input_col = var->varattno - 1;
					if (left_ != nullptr && left_->lookup_output_col_meta(var->varattno, &input_meta))
					{
						desc.output_storage = input_meta.storage_kind;
						desc.numeric_scale = input_meta.scale;
					}

					for (int g = 0; g < node->numCols; g++)
					{
						if (node->grpColIdx[g] == var->varattno)
						{
							desc.group_key_pos = g;
							break;
						}
					}
				}
				else if (ResolveAggPassThroughExpr(node, (Expr *) tle->expr,
												  &desc.input_col, &desc.group_key_pos))
				{
					VecOutputColMeta input_meta;

					if (left_ != nullptr && left_->lookup_output_col_meta(desc.input_col + 1, &input_meta))
					{
						desc.output_storage = input_meta.storage_kind;
						desc.numeric_scale = input_meta.scale;
					}
				}
			}
			aggs_.push_back(std::move(desc));
		}
}

VecAggState::~VecAggState()
{
	for (auto *chunk : rep_chunks_)
		delete chunk;
}

DataChunk<DEFAULT_CHUNK_SIZE> *
VecAggState::allocate_rep_chunk()
{
	MemoryContext old_context = MemoryContextSwitchTo(memory_context_);
	DataChunk<DEFAULT_CHUNK_SIZE> *chunk = new DataChunk<DEFAULT_CHUNK_SIZE>();
	MemoryContextSwitchTo(old_context);
	rep_chunks_.push_back(chunk);
	return chunk;
}

void
VecAggState::copy_rep_row(DataChunk<DEFAULT_CHUNK_SIZE> &dst, int dst_row,
						  const DataChunk<DEFAULT_CHUNK_SIZE> &src, int src_row) const
{
	for (const auto &agg : aggs_)
	{
		int out_col = agg.target_resno - 1;
		int src_col = agg.input_col;

		if (agg.arg_expr != nullptr || out_col < 0 || out_col >= 16 || src_col < 0 || src_col >= 16)
			continue;
		dst.nulls[out_col][dst_row] = src.nulls[src_col][src_row];
		if (dst.nulls[out_col][dst_row])
			continue;
		switch (agg.output_storage)
		{
			case VecOutputStorageKind::Double:
				dst.double_columns[out_col][dst_row] = src.double_columns[src_col][src_row];
				break;
			case VecOutputStorageKind::Int64:
			case VecOutputStorageKind::NumericScaledInt64:
			case VecOutputStorageKind::NumericAvgPair:
				dst.int64_columns[out_col][dst_row] = src.int64_columns[src_col][src_row];
				dst.double_columns[out_col][dst_row] = src.double_columns[src_col][src_row];
				break;
			case VecOutputStorageKind::StringRef:
				dst.string_columns[out_col][dst_row] =
					CopyStringRefToChunk(dst, src, src.string_columns[src_col][src_row]);
				break;
			case VecOutputStorageKind::Int32:
			default:
				dst.int32_columns[out_col][dst_row] = src.int32_columns[src_col][src_row];
				break;
		}
	}
}

void VecAggState::do_sink() {
	auto batch = std::make_unique<DataChunk<DEFAULT_CHUNK_SIZE>>();
	auto ensure_new_group = [this, &batch](VecAggGroupState *group, int row_idx)
	{
		DataChunk<DEFAULT_CHUNK_SIZE> *rep_chunk =
			rep_chunks_.empty() ? allocate_rep_chunk() : rep_chunks_.back();

		group->accs = VecAggAccumulatorList(PgMemoryContextAllocator<VecAggAccumulator>(memory_context_));
		if (rep_chunk->count >= DEFAULT_CHUNK_SIZE)
			rep_chunk = allocate_rep_chunk();
		group->rep_chunk_idx = (uint32_t) (rep_chunks_.size() - 1);
		group->rep_row_idx = (uint16_t) rep_chunk->count;
		group->has_rep_row = true;
		copy_rep_row(*rep_chunk, rep_chunk->count, *batch, row_idx);
		rep_chunk->count++;
	};
	auto update_group_accs = [this](VecAggGroupState *group, int row_idx)
	{
		auto &accs = group->accs;

		if (accs.empty())
			accs.resize(aggs_.size());
		for (size_t a = 0; a < aggs_.size(); a++) {
			if (aggs_[a].type == VecAggType::COUNT) {
				if (!aggs_[a].is_distinct)
				{
					if (!aggs_[a].arg_expr)
					{
						accs[a].count++;
						continue;
					}

					int r = aggs_[a].arg_expr->final_res_idx;

					if (r >= 0 && !aggs_[a].arg_expr->get_nulls_reg(r)[row_idx])
						accs[a].count++;
					continue;
				}
				if (!aggs_[a].arg_expr)
					continue;
				int r = aggs_[a].arg_expr->final_res_idx;
				if (r < 0 || aggs_[a].arg_expr->get_nulls_reg(r)[row_idx])
					continue;

				int64_t distinct_value;

				if (aggs_[a].use_exact_numeric || aggs_[a].arg_type == INT8OID)
					distinct_value = aggs_[a].arg_expr->get_int64_reg(r)[row_idx];
				else if (aggs_[a].arg_type == INT4OID ||
						 aggs_[a].arg_type == INT2OID ||
						 aggs_[a].arg_type == DATEOID)
					distinct_value = (int64_t) aggs_[a].arg_expr->get_int32_reg(r)[row_idx];
				else
					continue;

				if (accs[a].distinct_values == nullptr)
				{
					MemoryContext old_context = MemoryContextSwitchTo(memory_context_);
					accs[a].distinct_values =
						new VecAggAccumulator::DistinctValueSet(
							0,
							std::hash<int64_t>{},
							std::equal_to<int64_t>{},
							PgMemoryContextAllocator<std::pair<const int64_t, char>>(memory_context_));
					MemoryContextSwitchTo(old_context);
				}
				if (accs[a].distinct_values->emplace(distinct_value, 1).second)
				{
					accs[a].count++;
					if (pg_volvec_trace_hooks)
					{
						static int distinct_trace_count = 0;

						if (distinct_trace_count < 20)
						{
							elog(LOG,
								 "pg_volvec: distinct agg accepted value=%lld count=%lld",
								 (long long) distinct_value,
								 (long long) accs[a].count);
							distinct_trace_count++;
						}
					}
				}
			}
			else if (aggs_[a].arg_expr) {
				int r = aggs_[a].arg_expr->final_res_idx;
				if (r >= 0 && !aggs_[a].arg_expr->get_nulls_reg(r)[row_idx]) {
					if (aggs_[a].type == VecAggType::MAX) {
						if (aggs_[a].use_exact_numeric ||
							aggs_[a].output_storage == VecOutputStorageKind::Int64 ||
							aggs_[a].output_storage == VecOutputStorageKind::NumericScaledInt64 ||
							aggs_[a].output_storage == VecOutputStorageKind::NumericAvgPair) {
							const int64_t *r64 = aggs_[a].arg_expr->get_int64_reg(r);
							accs[a].update_max_int64(r64[row_idx]);
						} else if (aggs_[a].output_storage == VecOutputStorageKind::Double) {
							const double *rf8 = aggs_[a].arg_expr->get_float8_reg(r);
							accs[a].update_max_float(rf8[row_idx]);
						} else {
							const int32_t *r32 = aggs_[a].arg_expr->get_int32_reg(r);
							accs[a].update_max_int32(r32[row_idx]);
						}
					} else if (aggs_[a].use_exact_numeric) {
						const int64_t* r64 = aggs_[a].arg_expr->get_int64_reg(r);
						accs[a].update_numeric(r64[row_idx]);
					} else {
						double v;
						const int64_t* r64 = aggs_[a].arg_expr->get_int64_reg(r);
						const double* rf8 = aggs_[a].arg_expr->get_float8_reg(r);
						if (r64[row_idx] != 0 || (rf8[row_idx] == 0.0))
							v = (double)r64[row_idx];
						else
							v = rf8[row_idx];
						accs[a].update_float(v);
					}
				}
			}
		}
	};
	while (left_->get_next_batch(*batch)) {
			for (auto &agg : aggs_) if (agg.arg_expr) agg.arg_expr->evaluate(*batch);
		int n = batch->has_selection ? batch->sel.count : batch->count;
			for (int s = 0; s < n; s++) {
				int i = batch->has_selection ? batch->sel.row_ids[s] : s;
				if (use_simple_group_key_)
				{
					VecSimpleGroupKey key;
					int idx = grp_col_indices_[0];
					bool is_null = idx < 0 || idx >= 16 || batch->nulls[idx][i] != 0;

					key.is_null = is_null ? 1 : 0;
					if (!is_null)
					{
						if (simple_group_storage_ == VecOutputStorageKind::Int32)
							key.value = (int64_t) batch->int32_columns[idx][i];
						else
							key.value = batch->int64_columns[idx][i];
					}
					auto insert_result = simple_hash_table_.try_emplace(key);
					auto it = insert_result.first;

					if (insert_result.second)
						ensure_new_group(&it->second, i);
					update_group_accs(&it->second, i);
				}
				else
				{
					VecGroupKey key;

					key.num_cols = (int) grp_col_indices_.size();
					if (key.num_cols > kMaxDeformTargets)
						key.num_cols = kMaxDeformTargets;
					for (int k = 0; k < key.num_cols; k++) {
						int idx = grp_col_indices_[k];
						const VecOutputColMeta &meta = grp_col_meta_[k];
						bool is_null;

						if (idx < 0 || idx >= 16)
						{
							key.is_null[k] = 1;
							key.values[k] = 0;
							key.aux[k] = 0;
							continue;
						}
						is_null = batch->nulls[idx][i] != 0;
						key.is_null[k] = is_null ? 1 : 0;
						if (is_null)
						{
							key.values[k] = 0;
							key.aux[k] = 0;
							continue;
						}
						switch (meta.storage_kind)
						{
							case VecOutputStorageKind::StringRef:
							{
								uint32_t key_len = 0;
								VecStringRef ref = batch->string_columns[idx][i];

								key.values[k] = HashStringRefForGroupKey(*batch, ref, meta.sql_type, &key_len);
								key.aux[k] = key_len;
								break;
							}
							case VecOutputStorageKind::Int64:
							case VecOutputStorageKind::NumericScaledInt64:
							case VecOutputStorageKind::NumericAvgPair:
								key.values[k] = (uint64_t) batch->int64_columns[idx][i];
								key.aux[k] = 0;
								break;
							case VecOutputStorageKind::Double:
								memcpy(&key.values[k], &batch->double_columns[idx][i], sizeof(uint64_t));
								key.aux[k] = 0;
								break;
							case VecOutputStorageKind::Int32:
							default:
								key.values[k] = (uint64_t) (uint32_t) batch->int32_columns[idx][i];
								key.aux[k] = 0;
								break;
						}
					}
					auto insert_result = hash_table_.try_emplace(key);
					auto it = insert_result.first;

					if (insert_result.second)
						ensure_new_group(&it->second, i);
					update_group_accs(&it->second, i);
				}
			}
		}
		fully_scanned_ = true;
		if (use_simple_group_key_)
			simple_it_ = simple_hash_table_.begin();
		else
			it_ = hash_table_.begin();
}

bool VecAggState::get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk) {
	if (!fully_scanned_) do_sink();
	chunk.reset();
	if (use_simple_group_key_) {
		while (simple_it_ != simple_hash_table_.end() && chunk.count < DEFAULT_CHUNK_SIZE) {
			const auto& accs = simple_it_->second.accs;
			const DataChunk<DEFAULT_CHUNK_SIZE> *rep_chunk =
				simple_it_->second.has_rep_row ? rep_chunks_[simple_it_->second.rep_chunk_idx] : nullptr;
			int rep_row = simple_it_->second.rep_row_idx;
			for (size_t a = 0; a < aggs_.size(); a++) {
				int tidx = aggs_[a].target_resno - 1; if (tidx < 0 || tidx >= 16) continue;
				chunk.nulls[tidx][chunk.count] = 0;
				if (aggs_[a].arg_expr == nullptr && aggs_[a].type == VecAggType::MAX) {
					if (rep_chunk != nullptr)
					{
						chunk.nulls[tidx][chunk.count] = rep_chunk->nulls[tidx][rep_row];
						if (!chunk.nulls[tidx][chunk.count])
						{
							switch (aggs_[a].output_storage)
							{
								case VecOutputStorageKind::StringRef:
									chunk.string_columns[tidx][chunk.count] =
										CopyStringRefToChunk(chunk, *rep_chunk, rep_chunk->string_columns[tidx][rep_row]);
									break;
								case VecOutputStorageKind::Int64:
								case VecOutputStorageKind::NumericScaledInt64:
								case VecOutputStorageKind::NumericAvgPair:
									chunk.int64_columns[tidx][chunk.count] = rep_chunk->int64_columns[tidx][rep_row];
									chunk.double_columns[tidx][chunk.count] = rep_chunk->double_columns[tidx][rep_row];
									break;
								case VecOutputStorageKind::Double:
									chunk.double_columns[tidx][chunk.count] = rep_chunk->double_columns[tidx][rep_row];
									break;
								case VecOutputStorageKind::Int32:
								default:
									chunk.int32_columns[tidx][chunk.count] = rep_chunk->int32_columns[tidx][rep_row];
									break;
							}
						}
					}
				} else {
					if (aggs_[a].type == VecAggType::AVG) {
						if (aggs_[a].use_exact_numeric) {
							chunk.int64_columns[tidx][chunk.count] =
								WideIntToInt64Checked(accs[a].numeric_sum, "aggregate numeric average sum");
							chunk.double_columns[tidx][chunk.count] = (double) accs[a].count;
						} else {
							chunk.double_columns[tidx][chunk.count] = accs[a].count > 0 ? (accs[a].float_sum / accs[a].count) : 0.0;
						}
					}
					else if (aggs_[a].type == VecAggType::COUNT) chunk.int64_columns[tidx][chunk.count] = accs[a].count;
					else if (aggs_[a].type == VecAggType::MAX) {
						if (!accs[a].has_value)
							chunk.nulls[tidx][chunk.count] = 1;
						else if (aggs_[a].use_exact_numeric ||
								 aggs_[a].output_storage == VecOutputStorageKind::Int64 ||
								 aggs_[a].output_storage == VecOutputStorageKind::NumericScaledInt64 ||
								 aggs_[a].output_storage == VecOutputStorageKind::NumericAvgPair)
							chunk.int64_columns[tidx][chunk.count] = accs[a].int64_max;
						else if (aggs_[a].output_storage == VecOutputStorageKind::Double)
							chunk.double_columns[tidx][chunk.count] = accs[a].float_max;
						else
							chunk.int32_columns[tidx][chunk.count] = accs[a].int32_max;
					}
					else {
						if (aggs_[a].use_exact_numeric) {
							chunk.int64_columns[tidx][chunk.count] =
								WideIntToInt64Checked(accs[a].numeric_sum, "aggregate numeric sum");
						} else {
							chunk.double_columns[tidx][chunk.count] = accs[a].float_sum;
							chunk.int64_columns[tidx][chunk.count] = (int64_t)(accs[a].float_sum + (accs[a].float_sum >= 0 ? 0.5 : -0.5));
						}
					}
				}
			}
			chunk.count++; ++simple_it_;
		}
		return chunk.count > 0;
	}
	while (it_ != hash_table_.end() && chunk.count < DEFAULT_CHUNK_SIZE) {
			const auto& accs = it_->second.accs;
			const DataChunk<DEFAULT_CHUNK_SIZE> *rep_chunk =
				it_->second.has_rep_row ? rep_chunks_[it_->second.rep_chunk_idx] : nullptr;
			int rep_row = it_->second.rep_row_idx;
			for (size_t a = 0; a < aggs_.size(); a++) {
				int tidx = aggs_[a].target_resno - 1; if (tidx < 0 || tidx >= 16) continue;
				chunk.nulls[tidx][chunk.count] = 0;
				if (aggs_[a].arg_expr == nullptr && aggs_[a].type == VecAggType::MAX) {
					if (rep_chunk != nullptr)
					{
						chunk.nulls[tidx][chunk.count] = rep_chunk->nulls[tidx][rep_row];
						if (!chunk.nulls[tidx][chunk.count])
						{
							switch (aggs_[a].output_storage)
							{
								case VecOutputStorageKind::StringRef:
									chunk.string_columns[tidx][chunk.count] =
										CopyStringRefToChunk(chunk, *rep_chunk, rep_chunk->string_columns[tidx][rep_row]);
									break;
								case VecOutputStorageKind::Int64:
								case VecOutputStorageKind::NumericScaledInt64:
								case VecOutputStorageKind::NumericAvgPair:
									chunk.int64_columns[tidx][chunk.count] = rep_chunk->int64_columns[tidx][rep_row];
									chunk.double_columns[tidx][chunk.count] = rep_chunk->double_columns[tidx][rep_row];
									break;
								case VecOutputStorageKind::Double:
									chunk.double_columns[tidx][chunk.count] = rep_chunk->double_columns[tidx][rep_row];
									break;
								case VecOutputStorageKind::Int32:
								default:
									chunk.int32_columns[tidx][chunk.count] = rep_chunk->int32_columns[tidx][rep_row];
									break;
							}
						}
					}
				} else {
					if (aggs_[a].type == VecAggType::AVG) {
						if (aggs_[a].use_exact_numeric) {
							chunk.int64_columns[tidx][chunk.count] =
								WideIntToInt64Checked(accs[a].numeric_sum, "aggregate numeric average sum");
							chunk.double_columns[tidx][chunk.count] = (double) accs[a].count;
						} else {
							chunk.double_columns[tidx][chunk.count] = accs[a].count > 0 ? (accs[a].float_sum / accs[a].count) : 0.0;
						}
					}
					else if (aggs_[a].type == VecAggType::COUNT) chunk.int64_columns[tidx][chunk.count] = accs[a].count;
					else if (aggs_[a].type == VecAggType::MAX) {
						if (!accs[a].has_value)
							chunk.nulls[tidx][chunk.count] = 1;
						else if (aggs_[a].use_exact_numeric ||
								 aggs_[a].output_storage == VecOutputStorageKind::Int64 ||
								 aggs_[a].output_storage == VecOutputStorageKind::NumericScaledInt64 ||
								 aggs_[a].output_storage == VecOutputStorageKind::NumericAvgPair)
							chunk.int64_columns[tidx][chunk.count] = accs[a].int64_max;
						else if (aggs_[a].output_storage == VecOutputStorageKind::Double)
							chunk.double_columns[tidx][chunk.count] = accs[a].float_max;
						else
							chunk.int32_columns[tidx][chunk.count] = accs[a].int32_max;
					}
					else { 
						if (aggs_[a].use_exact_numeric) {
							chunk.int64_columns[tidx][chunk.count] =
								WideIntToInt64Checked(accs[a].numeric_sum, "aggregate numeric sum");
						} else {
							chunk.double_columns[tidx][chunk.count] = accs[a].float_sum;
							chunk.int64_columns[tidx][chunk.count] = (int64_t)(accs[a].float_sum + (accs[a].float_sum >= 0 ? 0.5 : -0.5));
						}
					}
				}
			}
			chunk.count++; ++it_;
		}
		return chunk.count > 0;
}

bool VecAggState::lookup_numeric_output_meta(int target_resno, NumericOutputKind *kind, int *scale) const {
	for (const auto &agg : aggs_) {
		if (agg.target_resno != target_resno || !agg.use_exact_numeric)
			continue;
		if (kind != nullptr) {
			if (agg.type == VecAggType::AVG)
				*kind = NumericOutputKind::Avg;
			else if (agg.type == VecAggType::SUM)
				*kind = NumericOutputKind::Sum;
			else
				*kind = NumericOutputKind::None;
		}
		if (scale != nullptr)
			*scale = agg.numeric_scale;
		return true;
	}

	if (kind != nullptr)
		*kind = NumericOutputKind::None;
	if (scale != nullptr)
		*scale = 0;
	return false;
}

bool
VecAggState::lookup_output_col_meta(int target_resno, VecOutputColMeta *out) const
{
	for (const auto &agg : aggs_) {
		if (agg.target_resno != target_resno)
			continue;
		if (out != nullptr) {
			out->sql_type = agg.output_type;
			out->storage_kind = agg.output_storage;
			out->scale = agg.numeric_scale;
		}
		return true;
	}

	if (out != nullptr) {
		out->sql_type = InvalidOid;
		out->storage_kind = VecOutputStorageKind::Int32;
		out->scale = 0;
	}
	return false;
}

/* --- VecSeqScanState --- */
VecSeqScanState::VecSeqScanState(Relation rel, Snapshot snapshot, const DeformProgram *program)
	: rel_(rel), snapshot_(snapshot), deformer_(RelationGetDescr(rel), program) {
	/*
	 * We drive block iteration ourselves below. Letting heap_beginscan choose a
	 * synchronized-scan start block would skip the prefix blocks because this
	 * custom loop never wraps back around to block 0.
	 */
	scan_ = (HeapScanDesc) heap_beginscan(rel_, snapshot_, 0, NULL, NULL, SO_TYPE_SEQSCAN | SO_ALLOW_STRAT | SO_ALLOW_PAGEMODE);
	stream_ = scan_->rs_read_stream;
	current_buf_ = InvalidBuffer;
	vmbuf_ = InvalidBuffer;
	current_offnum_ = FirstOffsetNumber;
	all_visible_ = false;
#ifdef USE_LLVM
	JitDeformFunc jf;
	const char *err;
	if (pg_volvec_jit_deform) {
		if (pg_volvec_try_compile_jit_deform_to_datachunk(RelationGetDescr(rel), program, &jf, &jit_context_, &err)) {
			deformer_.set_jit_func(jf);
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: deform JIT compiled successfully (targets=%d, func=%p)", program->ntargets, (void *) jf);
		} else if (pg_volvec_trace_hooks) {
			elog(LOG, "pg_volvec: deform JIT compile skipped or failed (targets=%d, reason=%s)", program->ntargets, err != nullptr ? err : "unknown");
		}
	} else if (pg_volvec_trace_hooks) {
		elog(LOG, "pg_volvec: deform JIT disabled by GUC");
	}
#endif
	if (pg_volvec_trace_hooks && stream_ != nullptr)
		elog(LOG, "pg_volvec: VecSeqScanState using heap read_stream for scan prefetch");
}

VecSeqScanState::~VecSeqScanState() { 
		if (pg_volvec_trace_hooks && jit_context_)
			elog(LOG, "pg_volvec: VecSeqScanState dtor releasing deform JIT context %p", (void *) jit_context_);
		if (BufferIsValid(current_buf_)) UnlockReleaseBuffer(current_buf_);
		if (BufferIsValid(vmbuf_)) ReleaseBuffer(vmbuf_);
		heap_endscan((TableScanDesc)scan_); 
		table_close(rel_, NoLock); 
		if (jit_context_) {
			pg_volvec_release_llvm_jit_context(jit_context_);
			jit_context_ = nullptr;
		}
}

bool
VecSeqScanState::lookup_output_col_meta(int target_resno, VecOutputColMeta *out) const
{
	TupleDesc desc = RelationGetDescr(rel_);
	int att_index = target_resno - 1;
	Oid typid;

	if (att_index < 0 || att_index >= desc->natts || att_index >= 16)
		return false;

	typid = TupleDescAttr(desc, att_index)->atttypid;
	if (out != nullptr) {
		out->sql_type = typid;
		out->storage_kind = DefaultOutputStorageKindForType(typid);
		out->scale = (typid == NUMERICOID) ?
			GetNumericScaleFromTypmod(TupleDescAttr(desc, att_index)->atttypmod) : 0;
	}
	return true;
}

bool VecSeqScanState::get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk) {
	chunk.reset();
	DeformBindings bindings;
	for (int i = 0; i < 16; i++) { bindings.columns_data[i] = chunk.int32_columns[i]; bindings.columns_nulls[i] = chunk.nulls[i]; }
	bindings.owner_chunk = &chunk;
	TupleDesc desc = RelationGetDescr(rel_);
	for (int i = 0; i < desc->natts && i < 16; i++) {
		Oid typid = TupleDescAttr(desc, i)->atttypid;
		if (typid == FLOAT8OID) bindings.columns_data[i] = chunk.double_columns[i];
		else if (typid == NUMERICOID || typid == INT8OID) bindings.columns_data[i] = chunk.int64_columns[i];
		else if (typid == BPCHAROID || typid == TEXTOID || typid == VARCHAROID) bindings.columns_data[i] = chunk.string_columns[i];
		else if (typid == DATEOID) bindings.columns_data[i] = chunk.int32_columns[i];
	}

	while (chunk.count < DEFAULT_CHUNK_SIZE) {
		if (current_buf_ == InvalidBuffer) {
			if (stream_ != nullptr) {
				current_buf_ = read_stream_next_buffer(stream_, NULL);
				if (!BufferIsValid(current_buf_))
					break;
				scan_->rs_cblock = BufferGetBlockNumber(current_buf_);
			} else {
				if (scan_->rs_cblock == InvalidBlockNumber) {
					scan_->rs_cblock = scan_->rs_startblock;
				} else {
					scan_->rs_cblock++;
				}

				if (scan_->rs_cblock >= scan_->rs_nblocks)
					break;

				current_buf_ = ReadBufferExtended(rel_, MAIN_FORKNUM, scan_->rs_cblock,
												 RBM_NORMAL, scan_->rs_strategy);
			}

			LockBuffer(current_buf_, BUFFER_LOCK_SHARE);
			current_offnum_ = FirstOffsetNumber;

			uint8 vmstatus = visibilitymap_get_status(rel_, scan_->rs_cblock, &vmbuf_);
			all_visible_ = (vmstatus & VISIBILITYMAP_ALL_VISIBLE) != 0;
		}

		Page page = BufferGetPage(current_buf_);
		int maxoff = PageGetMaxOffsetNumber(page);

		if (all_visible_) {
			/* Fast path: batch deform all normal items */
			while (current_offnum_ <= maxoff && chunk.count < DEFAULT_CHUNK_SIZE) {
				ItemId itemid = PageGetItemId(page, current_offnum_);
				if (ItemIdIsNormal(itemid)) {
					HeapTupleHeader tuphdr = (HeapTupleHeader) PageGetItem(page, itemid);
					deformer_.deform_tuple_header(tuphdr, chunk.count, bindings);
					chunk.count++;
				}
				current_offnum_++;
			}
		} else {
			/* Slow path: per-tuple visibility check */
			while (current_offnum_ <= maxoff && chunk.count < DEFAULT_CHUNK_SIZE) {
				ItemId itemid = PageGetItemId(page, current_offnum_);
				if (!ItemIdIsNormal(itemid)) { current_offnum_++; continue; }

				HeapTupleHeader tuphdr = (HeapTupleHeader) PageGetItem(page, itemid);
				HeapTupleData temp_tuple;
				temp_tuple.t_len = ItemIdGetLength(itemid);
				temp_tuple.t_data = tuphdr;
				BlockIdSet(&temp_tuple.t_self.ip_blkid, scan_->rs_cblock);
				temp_tuple.t_self.ip_posid = current_offnum_;
				temp_tuple.t_tableOid = RelationGetRelid(rel_);
				if (HeapTupleSatisfiesVisibility(&temp_tuple, snapshot_, current_buf_)) {
					deformer_.deform_tuple_header(tuphdr, chunk.count, bindings);
					chunk.count++;
				}
				current_offnum_++;
			}
		}

		if (current_offnum_ > maxoff) {
			UnlockReleaseBuffer(current_buf_);
			current_buf_ = InvalidBuffer;
		}
	}
	return chunk.count > 0;
}


bool VecFilterState::get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk) {
	while (left_->get_next_batch(chunk)) { 
		program_->evaluate(chunk); 
		if (chunk.sel.count > 0) return true; 
	}
	return false;
}

VecLookupProjectState::VecLookupProjectState(std::unique_ptr<VecPlanState> left,
											 std::unique_ptr<VecPlanState> lookup_source,
											 uint16_t input_key_col,
											 VecOutputColMeta input_key_meta,
											 uint16_t lookup_key_col,
											 VecOutputColMeta lookup_key_meta,
											 uint16_t lookup_value_col,
											 int output_resno,
											 VecOutputColMeta output_meta)
	: left_(std::move(left)),
	  lookup_source_(std::move(lookup_source)),
	  memory_context_(CurrentMemoryContext),
	  lookup_table_(0, VecLookupScalarKeyHash{}, std::equal_to<VecLookupScalarKey>{},
					PgMemoryContextAllocator<std::pair<const VecLookupScalarKey, VecLookupScalarValue>>(memory_context_)),
	  input_key_col_(input_key_col),
	  input_key_meta_(input_key_meta),
	  lookup_key_col_(lookup_key_col),
	  lookup_key_meta_(lookup_key_meta),
	  lookup_value_col_(lookup_value_col),
	  output_resno_(output_resno),
	  output_meta_(output_meta),
	  lookup_built_(false)
{
}

bool
VecLookupProjectState::lookup_output_col_meta(int target_resno, VecOutputColMeta *out) const
{
	if (target_resno == output_resno_)
	{
		if (out != nullptr)
			*out = output_meta_;
		return true;
	}
	return left_ != nullptr && left_->lookup_output_col_meta(target_resno, out);
}

bool
VecLookupProjectState::extract_lookup_key(const DataChunk<DEFAULT_CHUNK_SIZE> &chunk,
										  int row,
										  uint16_t col,
										  const VecOutputColMeta &meta,
										  VecLookupScalarKey *key) const
{
	if (key == nullptr || col >= 16)
		return false;

	key->is_null = chunk.nulls[col][row] ? 1 : 0;
	key->value = 0;
	if (key->is_null)
		return true;

	switch (meta.storage_kind)
	{
		case VecOutputStorageKind::Int32:
			key->value = (int64_t) chunk.int32_columns[col][row];
			return true;
		case VecOutputStorageKind::Int64:
		case VecOutputStorageKind::NumericScaledInt64:
			key->value = chunk.int64_columns[col][row];
			return true;
		default:
			return false;
	}
}

bool
VecLookupProjectState::build_lookup()
{
	if (lookup_built_)
		return true;
	if (lookup_source_ == nullptr)
		return false;

	while (lookup_source_->get_next_batch(lookup_chunk_))
	{
		int active_count = lookup_chunk_.has_selection ? lookup_chunk_.sel.count : lookup_chunk_.count;

		for (int s = 0; s < active_count; s++)
		{
			int row = lookup_chunk_.has_selection ? lookup_chunk_.sel.row_ids[s] : s;
			VecLookupScalarKey key;
			VecLookupScalarValue value;

			if (!extract_lookup_key(lookup_chunk_, row, lookup_key_col_, lookup_key_meta_, &key))
				return false;
			value.is_null = lookup_chunk_.nulls[lookup_value_col_][row] ? 1 : 0;
			if (!value.is_null)
			{
				switch (output_meta_.storage_kind)
				{
					case VecOutputStorageKind::Int32:
						value.i32 = lookup_chunk_.int32_columns[lookup_value_col_][row];
						break;
					case VecOutputStorageKind::Int64:
					case VecOutputStorageKind::NumericScaledInt64:
						value.i64 = lookup_chunk_.int64_columns[lookup_value_col_][row];
						break;
					case VecOutputStorageKind::Double:
						value.f8 = lookup_chunk_.double_columns[lookup_value_col_][row];
						break;
					default:
						return false;
				}
			}
			lookup_table_[key] = value;
		}
	}

	lookup_built_ = true;
	return true;
}

bool
VecLookupProjectState::get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk)
{
	int out_col = output_resno_ - 1;

	if (out_col < 0 || out_col >= 16)
		return false;
	if (!build_lookup())
		return false;

	while (left_->get_next_batch(chunk))
	{
		for (int row = 0; row < chunk.count; row++)
		{
			VecLookupScalarKey key;
			auto it = lookup_table_.end();

			if (!extract_lookup_key(chunk, row, input_key_col_, input_key_meta_, &key))
				return false;
			if (!key.is_null)
				it = lookup_table_.find(key);
			if (key.is_null || it == lookup_table_.end())
			{
				chunk.nulls[out_col][row] = 1;
				continue;
			}

			chunk.nulls[out_col][row] = it->second.is_null;
			if (chunk.nulls[out_col][row])
				continue;
			switch (output_meta_.storage_kind)
			{
				case VecOutputStorageKind::Int32:
					chunk.int32_columns[out_col][row] = it->second.i32;
					break;
				case VecOutputStorageKind::Int64:
				case VecOutputStorageKind::NumericScaledInt64:
					chunk.int64_columns[out_col][row] = it->second.i64;
					break;
				case VecOutputStorageKind::Double:
					chunk.double_columns[out_col][row] = it->second.f8;
					break;
				default:
					return false;
			}
		}

		if ((chunk.has_selection ? chunk.sel.count : chunk.count) > 0)
			return true;
	}

	return false;
}

VecLookupProjectStateMultiKey::VecLookupProjectStateMultiKey(
	std::unique_ptr<VecPlanState> left,
	std::unique_ptr<VecPlanState> lookup_source,
	int num_keys,
	const uint16_t *input_key_cols,
	const VecOutputColMeta *input_key_metas,
	const uint16_t *lookup_key_cols,
	const VecOutputColMeta *lookup_key_metas,
	uint16_t lookup_value_col,
	int output_resno,
	VecOutputColMeta output_meta)
	: left_(std::move(left)),
	  lookup_source_(std::move(lookup_source)),
	  memory_context_(CurrentMemoryContext),
	  lookup_table_(0, VecLookupCompositeKeyHash{}, std::equal_to<VecLookupCompositeKey>{},
					PgMemoryContextAllocator<std::pair<const VecLookupCompositeKey, VecLookupScalarValue>>(memory_context_)),
	  num_keys_(num_keys),
	  lookup_value_col_(lookup_value_col),
	  output_resno_(output_resno),
	  output_meta_(output_meta),
	  lookup_built_(false)
{
	Assert(num_keys_ > 0 && num_keys_ <= kMaxLookupKeys);
	for (int i = 0; i < kMaxLookupKeys; i++)
	{
		input_key_cols_[i] = 0;
		input_key_metas_[i] = VecOutputColMeta{InvalidOid, VecOutputStorageKind::Int32, 0};
		lookup_key_cols_[i] = 0;
		lookup_key_metas_[i] = VecOutputColMeta{InvalidOid, VecOutputStorageKind::Int32, 0};
	}
	for (int i = 0; i < num_keys_; i++)
	{
		input_key_cols_[i] = input_key_cols[i];
		input_key_metas_[i] = input_key_metas[i];
		lookup_key_cols_[i] = lookup_key_cols[i];
		lookup_key_metas_[i] = lookup_key_metas[i];
	}
}

bool
VecLookupProjectStateMultiKey::lookup_output_col_meta(int target_resno, VecOutputColMeta *out) const
{
	if (target_resno == output_resno_)
	{
		if (out != nullptr)
			*out = output_meta_;
		return true;
	}
	return left_ != nullptr && left_->lookup_output_col_meta(target_resno, out);
}

bool
VecLookupProjectStateMultiKey::extract_lookup_key(const DataChunk<DEFAULT_CHUNK_SIZE> &chunk,
												  int row,
												  const uint16_t *cols,
												  const VecOutputColMeta *metas,
												  VecLookupCompositeKey *key) const
{
	if (key == nullptr)
		return false;

	key->num_keys = (uint8_t) num_keys_;
	key->is_null = 0;
	for (int i = 0; i < num_keys_; i++)
	{
		uint16_t col = cols[i];
		const VecOutputColMeta &meta = metas[i];

		key->values[i] = 0;
		if (col >= 16)
			return false;
		if (chunk.nulls[col][row])
		{
			key->is_null = 1;
			continue;
		}

		switch (meta.storage_kind)
		{
			case VecOutputStorageKind::Int32:
				key->values[i] = (int64_t) chunk.int32_columns[col][row];
				break;
			case VecOutputStorageKind::Int64:
			case VecOutputStorageKind::NumericScaledInt64:
				key->values[i] = chunk.int64_columns[col][row];
				break;
			default:
				return false;
		}
	}

	return true;
}

bool
VecLookupProjectStateMultiKey::build_lookup()
{
	if (lookup_built_)
		return true;
	if (lookup_source_ == nullptr)
		return false;

	while (lookup_source_->get_next_batch(lookup_chunk_))
	{
		int active_count = lookup_chunk_.has_selection ? lookup_chunk_.sel.count : lookup_chunk_.count;

		for (int s = 0; s < active_count; s++)
		{
			int row = lookup_chunk_.has_selection ? lookup_chunk_.sel.row_ids[s] : s;
			VecLookupCompositeKey key;
			VecLookupScalarValue value;

			if (!extract_lookup_key(lookup_chunk_, row, lookup_key_cols_, lookup_key_metas_, &key))
				return false;
			value.is_null = lookup_chunk_.nulls[lookup_value_col_][row] ? 1 : 0;
			if (!value.is_null)
			{
				switch (output_meta_.storage_kind)
				{
					case VecOutputStorageKind::Int32:
						value.i32 = lookup_chunk_.int32_columns[lookup_value_col_][row];
						break;
					case VecOutputStorageKind::Int64:
					case VecOutputStorageKind::NumericScaledInt64:
						value.i64 = lookup_chunk_.int64_columns[lookup_value_col_][row];
						break;
					case VecOutputStorageKind::Double:
						value.f8 = lookup_chunk_.double_columns[lookup_value_col_][row];
						break;
					default:
						return false;
				}
			}
			lookup_table_[key] = value;
		}
	}

	lookup_built_ = true;
	return true;
}

bool
VecLookupProjectStateMultiKey::get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk)
{
	int out_col = output_resno_ - 1;

	if (out_col < 0 || out_col >= 16)
		return false;
	if (!build_lookup())
		return false;

	while (left_->get_next_batch(chunk))
	{
		for (int row = 0; row < chunk.count; row++)
		{
			VecLookupCompositeKey key;
			auto it = lookup_table_.end();

			if (!extract_lookup_key(chunk, row, input_key_cols_, input_key_metas_, &key))
				return false;
			if (!key.is_null)
				it = lookup_table_.find(key);
			if (key.is_null || it == lookup_table_.end())
			{
				chunk.nulls[out_col][row] = 1;
				continue;
			}

			chunk.nulls[out_col][row] = it->second.is_null;
			if (chunk.nulls[out_col][row])
				continue;
			switch (output_meta_.storage_kind)
			{
				case VecOutputStorageKind::Int32:
					chunk.int32_columns[out_col][row] = it->second.i32;
					break;
				case VecOutputStorageKind::Int64:
				case VecOutputStorageKind::NumericScaledInt64:
					chunk.int64_columns[out_col][row] = it->second.i64;
					break;
				case VecOutputStorageKind::Double:
					chunk.double_columns[out_col][row] = it->second.f8;
					break;
				default:
					return false;
			}
		}

		if ((chunk.has_selection ? chunk.sel.count : chunk.count) > 0)
			return true;
	}

	return false;
}

VecLookupFilterState::VecLookupFilterState(std::unique_ptr<VecPlanState> left,
											 std::unique_ptr<VecPlanState> lookup_source,
											 uint16_t input_key_col,
											 VecOutputColMeta input_key_meta,
											 uint16_t lookup_key_col,
											 VecOutputColMeta lookup_key_meta,
											 bool negate)
	: left_(std::move(left)),
	  lookup_source_(std::move(lookup_source)),
	  memory_context_(CurrentMemoryContext),
	  lookup_table_(0, VecLookupScalarKeyHash{}, std::equal_to<VecLookupScalarKey>{},
					PgMemoryContextAllocator<std::pair<const VecLookupScalarKey, VecLookupScalarValue>>(memory_context_)),
	  input_key_col_(input_key_col),
	  input_key_meta_(input_key_meta),
	  lookup_key_col_(lookup_key_col),
	  lookup_key_meta_(lookup_key_meta),
	  negate_(negate),
	  lookup_built_(false),
	  lookup_has_null_(false)
{
}

bool
VecLookupFilterState::extract_lookup_key(const DataChunk<DEFAULT_CHUNK_SIZE> &chunk,
										 int row,
										 uint16_t col,
										 const VecOutputColMeta &meta,
										 VecLookupScalarKey *key) const
{
	if (key == nullptr || col >= 16)
		return false;

	key->is_null = chunk.nulls[col][row] ? 1 : 0;
	key->value = 0;
	if (key->is_null)
		return true;

	switch (meta.storage_kind)
	{
		case VecOutputStorageKind::Int32:
			key->value = (int64_t) chunk.int32_columns[col][row];
			return true;
		case VecOutputStorageKind::Int64:
		case VecOutputStorageKind::NumericScaledInt64:
			key->value = chunk.int64_columns[col][row];
			return true;
		default:
			return false;
	}
}

bool
VecLookupFilterState::build_lookup()
{
	if (lookup_built_)
		return true;
	if (lookup_source_ == nullptr)
		return false;

	while (lookup_source_->get_next_batch(lookup_chunk_))
	{
		int active_count = lookup_chunk_.has_selection ? lookup_chunk_.sel.count : lookup_chunk_.count;

		for (int s = 0; s < active_count; s++)
		{
			int row = lookup_chunk_.has_selection ? lookup_chunk_.sel.row_ids[s] : s;
			VecLookupScalarKey key;
			VecLookupScalarValue value;

			if (!extract_lookup_key(lookup_chunk_, row, lookup_key_col_, lookup_key_meta_, &key))
				return false;
			if (key.is_null)
			{
				lookup_has_null_ = true;
				continue;
			}
			value.is_null = 0;
			lookup_table_[key] = value;
		}
	}

	lookup_built_ = true;
	if (pg_volvec_trace_hooks)
		elog(LOG,
			 "pg_volvec: built lookup filter table (rows=%zu, has_null=%d, negate=%d)",
			 lookup_table_.size(),
			 lookup_has_null_ ? 1 : 0,
			 negate_ ? 1 : 0);
	return true;
}

bool
VecLookupFilterState::get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk)
{
	if (!build_lookup())
		return false;

	while (left_->get_next_batch(chunk))
	{
		bool source_has_selection = chunk.has_selection;
		int active_count = source_has_selection ? chunk.sel.count : chunk.count;

		chunk.sel.count = 0;
		chunk.has_selection = true;
		for (int s = 0; s < active_count; s++)
		{
			int row = source_has_selection ? chunk.sel.row_ids[s] : s;
			VecLookupScalarKey key;
			bool matched = false;
			bool pass;

			if (!extract_lookup_key(chunk, row, input_key_col_, input_key_meta_, &key))
				return false;
			if (!key.is_null)
				matched = (lookup_table_.find(key) != lookup_table_.end());
			if (negate_)
				pass = !key.is_null && !matched && !lookup_has_null_;
			else
				pass = !key.is_null && matched;
			if (pass)
				chunk.sel.row_ids[chunk.sel.count++] = (uint16_t) row;
		}

		if (chunk.sel.count == chunk.count && !source_has_selection)
			chunk.has_selection = false;
		if (chunk.sel.count > 0 || (!chunk.has_selection && chunk.count > 0))
			return true;
	}

	return false;
}

static bool
IsIdentityVarTargetList(List *targetlist)
{
	ListCell *lc;
	int expected_resno = 1;
	bool saw_visible = false;

	foreach(lc, targetlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		Expr *expr;
		Var *var;

		if (tle->resjunk)
			continue;
		expr = StripImplicitNodesLocal((Expr *) tle->expr);
		if (expr == nullptr || !IsA(expr, Var))
			return false;
		var = (Var *) expr;
		if (tle->resno != expected_resno || var->varattno != expected_resno)
			return false;
		expected_resno++;
		saw_visible = true;
	}

	return saw_visible;
}

static std::unique_ptr<VecPlanState>
BuildDirectVarProject(std::unique_ptr<VecPlanState> left, List *targetlist)
{
	VolVecVector<VecProjectColDesc> project_cols{PgMemoryContextAllocator<VecProjectColDesc>(CurrentMemoryContext)};
	ListCell *lc;

	if (!left || targetlist == NIL || IsIdentityVarTargetList(targetlist))
		return left;

	foreach(lc, targetlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		Expr *expr = StripImplicitNodesLocal((Expr *) tle->expr);
		VecProjectColDesc project_col;
		Var *var;
		VecOutputColMeta meta;

		if (tle->resjunk)
			continue;
		if (expr == nullptr || !IsA(expr, Var))
			return nullptr;
		var = (Var *) expr;
		if (var->varattno <= 0 || var->varattno > 16)
			return nullptr;
		if (!left->lookup_output_col_meta(var->varattno, &meta))
			return nullptr;

		project_col.expr = nullptr;
		project_col.target_resno = tle->resno;
		project_col.sql_type = exprType((Node *) tle->expr);
		project_col.storage_kind = meta.storage_kind;
		project_col.scale = meta.scale;
		project_col.direct_var = true;
		project_col.input_col = (uint16_t) (var->varattno - 1);
		project_cols.push_back(std::move(project_col));
	}

	if (project_cols.empty())
		return left;

	return std::make_unique<VecProjectState>(std::move(left), std::move(project_cols));
}

VecProjectState::VecProjectState(std::unique_ptr<VecPlanState> left,
								 VolVecVector<VecProjectColDesc> columns)
	: left_(std::move(left)),
	  columns_(PgMemoryContextAllocator<VecProjectColDesc>(CurrentMemoryContext))
{
	for (auto &column : columns)
		columns_.push_back(std::move(column));
}

bool
VecProjectState::lookup_output_col_meta(int target_resno, VecOutputColMeta *out) const
{
	for (const auto &column : columns_)
	{
		if (column.target_resno != target_resno)
			continue;
		if (out != nullptr)
		{
			out->sql_type = column.sql_type;
			out->storage_kind = column.storage_kind;
			out->scale = column.scale;
		}
		return true;
	}
	return false;
}

bool
VecProjectState::get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk)
{
	chunk.reset();
	while (left_->get_next_batch(input_chunk_))
	{
		int active_count = input_chunk_.has_selection ? input_chunk_.sel.count : input_chunk_.count;

		if (active_count <= 0)
			continue;

		for (auto &column : columns_)
		{
			if (column.expr)
				column.expr->evaluate(input_chunk_);
		}

		for (int s = 0; s < active_count; s++)
		{
			int src_row = input_chunk_.has_selection ? input_chunk_.sel.row_ids[s] : s;
			int dst_row = chunk.count++;

			for (const auto &column : columns_)
			{
				int out_col = column.target_resno - 1;
				int reg = column.expr ? column.expr->get_final_res_idx() : -1;

				if (out_col < 0 || out_col >= 16)
					continue;
				if (column.direct_var || column.string_prefix_var)
					chunk.nulls[out_col][dst_row] = input_chunk_.nulls[column.input_col][src_row];
				else
					chunk.nulls[out_col][dst_row] = column.expr->get_nulls_reg(reg)[src_row];
				if (chunk.nulls[out_col][dst_row])
					continue;

				switch (column.storage_kind)
				{
					case VecOutputStorageKind::Double:
						chunk.double_columns[out_col][dst_row] = column.direct_var ?
							input_chunk_.double_columns[column.input_col][src_row] :
							column.expr->get_float8_reg(reg)[src_row];
						break;
					case VecOutputStorageKind::Int64:
					case VecOutputStorageKind::NumericScaledInt64:
						chunk.int64_columns[out_col][dst_row] = column.direct_var ?
							input_chunk_.int64_columns[column.input_col][src_row] :
							column.expr->get_int64_reg(reg)[src_row];
						break;
					case VecOutputStorageKind::Int32:
						chunk.int32_columns[out_col][dst_row] = column.direct_var ?
							input_chunk_.int32_columns[column.input_col][src_row] :
							column.expr->get_int32_reg(reg)[src_row];
						break;
					case VecOutputStorageKind::StringRef:
						if (column.string_prefix_var)
						{
							VecStringRef src_ref = input_chunk_.string_columns[column.input_col][src_row];
							uint32_t copy_len = Min(src_ref.len, column.string_prefix_len);

							chunk.string_columns[out_col][dst_row] =
								chunk.store_string_bytes(input_chunk_.get_string_ptr(src_ref), copy_len);
							break;
						}
						if (!column.direct_var)
							elog(ERROR, "pg_volvec computed string projection is not supported");
						chunk.string_columns[out_col][dst_row] =
							CopyStringRefToChunk(chunk, input_chunk_,
												 input_chunk_.string_columns[column.input_col][src_row]);
						break;
					default:
						elog(ERROR, "pg_volvec project output kind is not supported");
						break;
				}
			}
		}

		if (chunk.count > 0)
			return true;
	}

	return false;
}

bool
VecLimitState::get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk)
{
	if (done_ || left_ == nullptr)
		return false;

	while (left_->get_next_batch(chunk))
	{
		uint64_t remaining;
		int active_count;

		if (emitted_ >= limit_count_)
		{
			done_ = true;
			chunk.reset();
			return false;
		}

		active_count = chunk.has_selection ? chunk.sel.count : chunk.count;
		if (active_count <= 0)
			continue;

		remaining = limit_count_ - emitted_;
		if ((uint64_t) active_count > remaining)
		{
			if (chunk.has_selection)
				chunk.sel.count = (uint16_t) remaining;
			else
				chunk.count = (int) remaining;
			active_count = (int) remaining;
			done_ = true;
		}
		emitted_ += (uint64_t) active_count;
		return active_count > 0;
	}

	done_ = true;
	return false;
}

VecHashJoinState::VecHashJoinState(std::unique_ptr<VecPlanState> outer,
								   std::unique_ptr<VecPlanState> inner,
								   JoinType jointype,
								   bool build_outer_side,
								   int visible_output_count,
								   VolVecVector<VecJoinOutputCol> output_cols,
								   VolVecVector<VecHashJoinKeyCol> key_cols)
	: outer_(std::move(outer)),
	  inner_(std::move(inner)),
	  jointype_(jointype),
	  visible_output_count_(visible_output_count),
	  memory_context_(CurrentMemoryContext),
	  output_cols_(PgMemoryContextAllocator<VecJoinOutputCol>(memory_context_)),
	  key_cols_(PgMemoryContextAllocator<VecHashJoinKeyCol>(memory_context_)),
	  inner_payload_cols_(PgMemoryContextAllocator<VecHashPayloadCol>(memory_context_)),
	  inner_chunks_(PgMemoryContextAllocator<DataChunk<DEFAULT_CHUNK_SIZE> *>(memory_context_)),
	  bucket_heads_(PgMemoryContextAllocator<int32_t>(memory_context_)),
	  entries_(PgMemoryContextAllocator<VecHashEntry>(memory_context_)),
	  inner_entry_matched_(PgMemoryContextAllocator<uint8_t>(memory_context_)),
	  probe_rows_(PgMemoryContextAllocator<uint16_t>(memory_context_)),
	  probe_keys_(PgMemoryContextAllocator<VecHashJoinKey>(memory_context_)),
	  probe_hashes_(PgMemoryContextAllocator<uint32_t>(memory_context_)),
	  probe_next_entries_(PgMemoryContextAllocator<int32_t>(memory_context_)),
		  active_probe_sel_(PgMemoryContextAllocator<uint16_t>(memory_context_)),
		  next_probe_sel_(PgMemoryContextAllocator<uint16_t>(memory_context_)),
		  inner_built_(false),
		  probe_batch_ready_(false),
		  probe_input_exhausted_(false),
	  build_outer_side_(build_outer_side),
	  join_filter_program_(nullptr),
	  semi_build_marked_(false),
	  semi_build_emit_pos_(0),
	  anti_build_marked_(false),
	  anti_build_emit_pos_(0),
	  right_anti_marked_(false),
	  anti_outer_pos_(0),
	  right_anti_emit_pos_(0),
	  bucket_mask_(0)
{
	Assert(!build_outer_side_ ||
		   jointype_ == JOIN_INNER ||
		   jointype_ == JOIN_SEMI ||
		   jointype_ == JOIN_ANTI);
	for (const auto &key_col : key_cols)
		key_cols_.push_back(key_col);
	for (const auto &output_col : output_cols)
	{
		VecJoinOutputCol remapped = output_col;

		if ((build_outer_side_ && remapped.side == VecJoinSide::Outer) ||
			(!build_outer_side_ && remapped.side == VecJoinSide::Inner))
			remapped.input_col = ensure_inner_payload_col(remapped.input_col, remapped.meta);
		output_cols_.push_back(remapped);
	}
}

void
VecHashJoinState::set_join_filter_program(std::unique_ptr<VecExprProgram> program)
{
	join_filter_program_ = std::move(program);
}

VecHashJoinState::~VecHashJoinState()
{
	for (auto *chunk : inner_chunks_)
		delete chunk;
}

void
VecHashJoinState::init_hash_table(size_t expected_rows)
{
	size_t bucket_count = 1024;

	while (bucket_count < std::max<size_t>(expected_rows * 2, 1024))
		bucket_count <<= 1;

	bucket_heads_.assign(bucket_count, -1);
	bucket_mask_ = bucket_count - 1;
	entries_.clear();
	inner_entry_matched_.clear();
	entries_.reserve(expected_rows > 0 ? expected_rows : DEFAULT_CHUNK_SIZE);
}

void
VecHashJoinState::rehash_hash_table(size_t min_bucket_count)
{
	size_t bucket_count = 1024;

	while (bucket_count < std::max<size_t>(min_bucket_count, 1024))
		bucket_count <<= 1;

	bucket_heads_.assign(bucket_count, -1);
	bucket_mask_ = bucket_count - 1;
	for (size_t i = 0; i < entries_.size(); i++)
	{
		size_t bucket = entries_[i].hash & bucket_mask_;

		entries_[i].next = bucket_heads_[bucket];
		bucket_heads_[bucket] = (int32_t) i;
	}
}

void
VecHashJoinState::append_inner_entry(const VecHashJoinKey &key, uint32_t hash,
									 uint32_t chunk_idx, uint16_t row_idx)
{
	size_t next_size = entries_.size() + 1;
	size_t max_load = bucket_heads_.empty() ? 0 : (bucket_heads_.size() * 3) / 4;
	VecHashEntry entry;
	size_t bucket;

	if (bucket_heads_.empty())
		init_hash_table(next_size);
	else if (next_size > max_load)
		rehash_hash_table(bucket_heads_.size() << 1);

	bucket = hash & bucket_mask_;
	entry.hash = hash;
	entry.key = key;
	entry.next = bucket_heads_[bucket];
	entry.chunk_idx = chunk_idx;
	entry.row_idx = row_idx;
	entries_.push_back(entry);
	bucket_heads_[bucket] = (int32_t) (entries_.size() - 1);
}

uint16_t
VecHashJoinState::ensure_inner_payload_col(uint16_t source_col, const VecOutputColMeta &meta)
{
	for (uint16_t i = 0; i < inner_payload_cols_.size(); i++)
	{
		const VecHashPayloadCol &payload_col = inner_payload_cols_[i];

		if (payload_col.source_col == source_col)
			return i;
	}

	inner_payload_cols_.push_back(VecHashPayloadCol{source_col, meta});
	return (uint16_t) (inner_payload_cols_.size() - 1);
}

bool
VecHashJoinState::lookup_output_col_meta(int target_resno, VecOutputColMeta *out) const
{
	for (const auto &output_col : output_cols_)
	{
		if (output_col.output_resno != target_resno)
			continue;
		if (out != nullptr)
			*out = output_col.meta;
		return true;
	}
	return false;
}

DataChunk<DEFAULT_CHUNK_SIZE> *
VecHashJoinState::allocate_inner_chunk()
{
	MemoryContext old_context = MemoryContextSwitchTo(memory_context_);
	DataChunk<DEFAULT_CHUNK_SIZE> *chunk = new DataChunk<DEFAULT_CHUNK_SIZE>();
	MemoryContextSwitchTo(old_context);
	inner_chunks_.push_back(chunk);
	return chunk;
}

void
VecHashJoinState::copy_inner_payload_row(DataChunk<DEFAULT_CHUNK_SIZE> &dst, int dst_row,
										 const DataChunk<DEFAULT_CHUNK_SIZE> &src, int src_row) const
{
	for (uint16_t dst_col = 0; dst_col < inner_payload_cols_.size(); dst_col++)
	{
		const VecHashPayloadCol &payload_col = inner_payload_cols_[dst_col];
		uint16_t src_col = payload_col.source_col;

		dst.nulls[dst_col][dst_row] = src.nulls[src_col][src_row];
		if (dst.nulls[dst_col][dst_row])
			continue;

		switch (payload_col.meta.storage_kind)
		{
			case VecOutputStorageKind::Double:
				dst.double_columns[dst_col][dst_row] = src.double_columns[src_col][src_row];
				break;
			case VecOutputStorageKind::Int64:
			case VecOutputStorageKind::NumericScaledInt64:
			case VecOutputStorageKind::NumericAvgPair:
				dst.int64_columns[dst_col][dst_row] = src.int64_columns[src_col][src_row];
				dst.double_columns[dst_col][dst_row] = src.double_columns[src_col][src_row];
				break;
				case VecOutputStorageKind::StringRef:
					dst.string_columns[dst_col][dst_row] =
						CopyStringRefToChunk(dst, src, src.string_columns[src_col][src_row]);
					break;
			case VecOutputStorageKind::Int32:
				dst.int32_columns[dst_col][dst_row] = src.int32_columns[src_col][src_row];
				break;
		}
	}
}

bool
VecHashJoinState::read_key(const DataChunk<DEFAULT_CHUNK_SIZE> &chunk, bool inner_side, int row,
						   VecHashJoinKey *key) const
{
	if (key == nullptr || key_cols_.empty() || key_cols_.size() > kMaxJoinKeys)
		return false;

	memset(key->values, 0, sizeof(key->values));
	key->num_keys = (uint8_t) key_cols_.size();
	for (size_t i = 0; i < key_cols_.size(); i++)
	{
		const VecHashJoinKeyCol &key_col = key_cols_[i];
		int col = inner_side ? key_col.inner_col : key_col.outer_col;

		if (col < 0 || col >= 16 || chunk.nulls[col][row])
			return false;

		switch (key_col.kind)
		{
			case VecOutputStorageKind::Int32:
				key->values[i] = (uint64_t) (uint32_t) chunk.int32_columns[col][row];
				break;
			case VecOutputStorageKind::Int64:
			case VecOutputStorageKind::NumericScaledInt64:
				key->values[i] = (uint64_t) chunk.int64_columns[col][row];
				break;
			default:
				elog(ERROR, "pg_volvec hash join key kind is not supported");
				return false;
		}
	}

	return true;
}

uint32_t
VecHashJoinState::hash_key(const VecHashJoinKey &key) const
{
	uint64_t hash = UINT64CONST(0x9e3779b97f4a7c15);

	for (int i = 0; i < key.num_keys; i++)
	{
		uint64_t value = key.values[i];

		value ^= value >> 33;
		value *= UINT64CONST(0xff51afd7ed558ccd);
		value ^= value >> 33;
		value *= UINT64CONST(0xc4ceb9fe1a85ec53);
		value ^= value >> 33;
		hash ^= value + UINT64CONST(0x9e3779b97f4a7c15) + (hash << 6) + (hash >> 2);
	}

	return (uint32_t) (hash ^ (hash >> 32));
}

bool
VecHashJoinState::keys_equal(const VecHashJoinKey &left, const VecHashJoinKey &right) const
{
	if (left.num_keys != right.num_keys)
		return false;
	for (int i = 0; i < left.num_keys; i++)
	{
		if (left.values[i] != right.values[i])
			return false;
	}
	return true;
}

void
VecHashJoinState::build_inner_hash()
{
	DataChunk<DEFAULT_CHUNK_SIZE> input;
	VecPlanState *build_state = build_outer_side_ ? outer_.get() : inner_.get();
	bool build_is_inner = !build_outer_side_;

	if (inner_built_)
		return;

	init_hash_table(DEFAULT_CHUNK_SIZE);

	while (build_state->get_next_batch(input))
	{
		int active_count = input.has_selection ? input.sel.count : input.count;
		DataChunk<DEFAULT_CHUNK_SIZE> *dst =
			inner_chunks_.empty() ? allocate_inner_chunk() : inner_chunks_.back();

		for (int s = 0; s < active_count; s++)
		{
			int src_row = input.has_selection ? input.sel.row_ids[s] : s;
			int dst_row;
			VecHashJoinKey key;

			if (!read_key(input, build_is_inner, src_row, &key))
				continue;
			if (dst->count >= DEFAULT_CHUNK_SIZE)
				dst = allocate_inner_chunk();
			dst_row = dst->count;
			copy_inner_payload_row(*dst, dst_row, input, src_row);
			append_inner_entry(key, hash_key(key),
							  (uint32_t) (inner_chunks_.size() - 1),
							  (uint16_t) dst_row);
			dst->count++;
		}
	}

	inner_built_ = true;
	inner_entry_matched_.assign(entries_.size(), 0);
}

bool
VecHashJoinState::advance_outer_batch()
{
	VecPlanState *probe_state = build_outer_side_ ? inner_.get() : outer_.get();

	while (probe_state->get_next_batch(outer_chunk_))
	{
		int active_count = outer_chunk_.has_selection ? outer_chunk_.sel.count : outer_chunk_.count;

		if (active_count <= 0)
			continue;
		anti_outer_pos_ = 0;
		probe_batch_ready_ = false;
		probe_input_exhausted_ = false;
		return true;
	}
	probe_input_exhausted_ = true;
	return false;
}

void
VecHashJoinState::prepare_probe_batch()
{
	int active_count = outer_chunk_.has_selection ? outer_chunk_.sel.count : outer_chunk_.count;

	probe_rows_.clear();
	probe_keys_.clear();
	probe_hashes_.clear();
	probe_next_entries_.clear();
	active_probe_sel_.clear();
	next_probe_sel_.clear();
	probe_rows_.reserve(active_count);
	probe_keys_.reserve(active_count);
	probe_hashes_.reserve(active_count);
	probe_next_entries_.reserve(active_count);
	active_probe_sel_.reserve(active_count);
	next_probe_sel_.reserve(active_count);

	for (int s = 0; s < active_count; s++)
	{
		int row = outer_chunk_.has_selection ? outer_chunk_.sel.row_ids[s] : s;
		VecHashJoinKey key;
		uint32_t hash;
		int32_t head;
		uint16_t probe_idx;

		if (!read_key(outer_chunk_, build_outer_side_, row, &key))
			continue;

		hash = hash_key(key);
		head = bucket_heads_.empty() ? -1 : bucket_heads_[hash & bucket_mask_];
		if (head < 0)
			continue;

		probe_idx = (uint16_t) probe_rows_.size();
		probe_rows_.push_back((uint16_t) row);
		probe_keys_.push_back(key);
		probe_hashes_.push_back(hash);
		probe_next_entries_.push_back(head);
		active_probe_sel_.push_back(probe_idx);
	}

	probe_batch_ready_ = true;
}

bool
VecHashJoinState::advance_probe_match(uint16_t probe_idx, int32_t *match_entry_idx)
{
	int32_t entry_idx = probe_next_entries_[probe_idx];
	const VecHashJoinKey &key = probe_keys_[probe_idx];
	uint32_t hash = probe_hashes_[probe_idx];

	while (entry_idx >= 0)
	{
		const VecHashEntry &entry = entries_[entry_idx];

		probe_next_entries_[probe_idx] = entry.next;
		if (entry.hash == hash && keys_equal(entry.key, key))
		{
			if (match_entry_idx != nullptr)
				*match_entry_idx = entry_idx;
			return true;
		}
		entry_idx = entry.next;
	}

	probe_next_entries_[probe_idx] = -1;
	return false;
}

bool
VecHashJoinState::candidate_passes_join_filter(const DataChunk<DEFAULT_CHUNK_SIZE> &outer_src,
											   int outer_row,
											   const DataChunk<DEFAULT_CHUNK_SIZE> &inner_src,
											   int inner_row)
{
	if (!join_filter_program_)
		return true;

	join_filter_chunk_.reset();
	join_filter_chunk_.count = 1;
	for (const auto &output_col : output_cols_)
	{
		int out_col = output_col.output_resno - 1;
		const DataChunk<DEFAULT_CHUNK_SIZE> *src =
			output_col.side == VecJoinSide::Outer ? &outer_src : &inner_src;
		int src_row = output_col.side == VecJoinSide::Outer ? outer_row : inner_row;
		int src_col = output_col.input_col;

		join_filter_chunk_.nulls[out_col][0] = src->nulls[src_col][src_row];
		if (join_filter_chunk_.nulls[out_col][0])
			continue;
		switch (output_col.meta.storage_kind)
		{
			case VecOutputStorageKind::Double:
				join_filter_chunk_.double_columns[out_col][0] = src->double_columns[src_col][src_row];
				break;
			case VecOutputStorageKind::Int64:
			case VecOutputStorageKind::NumericScaledInt64:
			case VecOutputStorageKind::NumericAvgPair:
				join_filter_chunk_.int64_columns[out_col][0] = src->int64_columns[src_col][src_row];
				join_filter_chunk_.double_columns[out_col][0] = src->double_columns[src_col][src_row];
				break;
			case VecOutputStorageKind::StringRef:
				join_filter_chunk_.string_columns[out_col][0] =
					CopyStringRefToChunk(join_filter_chunk_, *src, src->string_columns[src_col][src_row]);
				break;
			case VecOutputStorageKind::Int32:
				join_filter_chunk_.int32_columns[out_col][0] = src->int32_columns[src_col][src_row];
				break;
		}
	}

	join_filter_program_->evaluate(join_filter_chunk_);
	return (join_filter_chunk_.has_selection ? join_filter_chunk_.sel.count : join_filter_chunk_.count) > 0;
}

bool
VecHashJoinState::get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk)
{
	if (!inner_built_)
		build_inner_hash();

	chunk.reset();
	auto copy_output_value = [&chunk](int dst_row,
									 int out_col,
									 const VecOutputColMeta &meta,
									 const DataChunk<DEFAULT_CHUNK_SIZE> &src,
									 int src_col,
									 int src_row) {
		chunk.nulls[out_col][dst_row] = src.nulls[src_col][src_row];
		if (chunk.nulls[out_col][dst_row])
			return;
		switch (meta.storage_kind)
		{
			case VecOutputStorageKind::Double:
				chunk.double_columns[out_col][dst_row] = src.double_columns[src_col][src_row];
				break;
			case VecOutputStorageKind::Int64:
			case VecOutputStorageKind::NumericScaledInt64:
			case VecOutputStorageKind::NumericAvgPair:
				chunk.int64_columns[out_col][dst_row] = src.int64_columns[src_col][src_row];
				chunk.double_columns[out_col][dst_row] = src.double_columns[src_col][src_row];
				break;
			case VecOutputStorageKind::StringRef:
				chunk.string_columns[out_col][dst_row] =
					CopyStringRefToChunk(chunk, src, src.string_columns[src_col][src_row]);
				break;
			case VecOutputStorageKind::Int32:
				chunk.int32_columns[out_col][dst_row] = src.int32_columns[src_col][src_row];
				break;
		}
	};
	if (jointype_ == JOIN_ANTI)
	{
		if (build_outer_side_)
		{
			if (!anti_build_marked_)
			{
				while (advance_outer_batch())
				{
					int active_count = outer_chunk_.has_selection ? outer_chunk_.sel.count : outer_chunk_.count;

					for (int s = 0; s < active_count; s++)
					{
						int probe_row = outer_chunk_.has_selection ? outer_chunk_.sel.row_ids[s] : s;
						VecHashJoinKey key;
						uint32_t hash;
						int32_t entry_idx;

						if (!read_key(outer_chunk_, true, probe_row, &key))
							continue;
						hash = hash_key(key);
						entry_idx = bucket_heads_.empty() ? -1 : bucket_heads_[hash & bucket_mask_];
						while (entry_idx >= 0)
						{
							const VecHashEntry &entry = entries_[entry_idx];
							const DataChunk<DEFAULT_CHUNK_SIZE> *build_chunk =
								inner_chunks_[entry.chunk_idx];

							if (entry.hash == hash &&
								keys_equal(entry.key, key) &&
								candidate_passes_join_filter(*build_chunk, entry.row_idx,
															 outer_chunk_, probe_row))
								inner_entry_matched_[entry_idx] = 1;
							entry_idx = entry.next;
						}
					}
				}
				anti_build_marked_ = true;
				anti_build_emit_pos_ = 0;
			}

			while (anti_build_emit_pos_ < entries_.size() &&
				   chunk.count < DEFAULT_CHUNK_SIZE)
			{
				const VecHashEntry &entry = entries_[anti_build_emit_pos_];
				const DataChunk<DEFAULT_CHUNK_SIZE> *build_chunk;

				if (inner_entry_matched_[anti_build_emit_pos_++])
					continue;
				build_chunk = inner_chunks_[entry.chunk_idx];
				for (const auto &output_col : output_cols_)
				{
					int out_col = output_col.output_resno - 1;

					if (output_col.output_resno > visible_output_count_)
						continue;
					if (output_col.side != VecJoinSide::Outer)
						elog(ERROR, "pg_volvec anti join build-outer path cannot expose inner columns");
					copy_output_value(chunk.count, out_col, output_col.meta,
									  *build_chunk, output_col.input_col, entry.row_idx);
				}
				chunk.count++;
			}

			return chunk.count > 0;
		}

		while (chunk.count < DEFAULT_CHUNK_SIZE)
		{
			int active_count;

			if ((outer_chunk_.has_selection ? outer_chunk_.sel.count : outer_chunk_.count) == 0 &&
				!advance_outer_batch())
				break;

			active_count = outer_chunk_.has_selection ? outer_chunk_.sel.count : outer_chunk_.count;
			while (anti_outer_pos_ < active_count && chunk.count < DEFAULT_CHUNK_SIZE)
			{
				int outer_row = outer_chunk_.has_selection ?
					outer_chunk_.sel.row_ids[anti_outer_pos_] : anti_outer_pos_;
				VecHashJoinKey key;
				bool has_match = false;
				int32_t entry_idx = -1;

				anti_outer_pos_++;
				if (read_key(outer_chunk_, false, outer_row, &key))
				{
					uint32_t hash = hash_key(key);

					entry_idx = bucket_heads_.empty() ? -1 : bucket_heads_[hash & bucket_mask_];
					while (entry_idx >= 0)
					{
						const VecHashEntry &entry = entries_[entry_idx];
						const DataChunk<DEFAULT_CHUNK_SIZE> *inner_chunk = inner_chunks_[entry.chunk_idx];

						if (entry.hash == hash &&
							keys_equal(entry.key, key) &&
							candidate_passes_join_filter(outer_chunk_, outer_row, *inner_chunk, entry.row_idx))
						{
							has_match = true;
							break;
						}
						entry_idx = entry.next;
					}
				}
				if (has_match)
					continue;

				for (const auto &output_col : output_cols_)
				{
					int out_col = output_col.output_resno - 1;
					int src_col = output_col.input_col;

					if (output_col.output_resno > visible_output_count_)
						continue;
					if (output_col.side != VecJoinSide::Outer)
						elog(ERROR, "pg_volvec anti join does not support inner output columns");
					chunk.nulls[out_col][chunk.count] = outer_chunk_.nulls[src_col][outer_row];
					if (chunk.nulls[out_col][chunk.count])
						continue;
					switch (output_col.meta.storage_kind)
					{
						case VecOutputStorageKind::Double:
							chunk.double_columns[out_col][chunk.count] =
								outer_chunk_.double_columns[src_col][outer_row];
							break;
						case VecOutputStorageKind::Int64:
						case VecOutputStorageKind::NumericScaledInt64:
						case VecOutputStorageKind::NumericAvgPair:
							chunk.int64_columns[out_col][chunk.count] =
								outer_chunk_.int64_columns[src_col][outer_row];
							chunk.double_columns[out_col][chunk.count] =
								outer_chunk_.double_columns[src_col][outer_row];
							break;
						case VecOutputStorageKind::StringRef:
							chunk.string_columns[out_col][chunk.count] =
								CopyStringRefToChunk(chunk, outer_chunk_,
													 outer_chunk_.string_columns[src_col][outer_row]);
							break;
						case VecOutputStorageKind::Int32:
							chunk.int32_columns[out_col][chunk.count] =
								outer_chunk_.int32_columns[src_col][outer_row];
							break;
					}
				}
				chunk.count++;
			}

			if (anti_outer_pos_ >= active_count)
				outer_chunk_.reset();
			if (chunk.count > 0)
				return true;
		}

		return chunk.count > 0;
	}
	if (jointype_ == JOIN_SEMI)
	{
		if (build_outer_side_)
		{
			if (!semi_build_marked_)
			{
				while (advance_outer_batch())
				{
					int active_count = outer_chunk_.has_selection ? outer_chunk_.sel.count : outer_chunk_.count;

					for (int s = 0; s < active_count; s++)
					{
						int probe_row = outer_chunk_.has_selection ? outer_chunk_.sel.row_ids[s] : s;
						VecHashJoinKey key;
						uint32_t hash;
						int32_t entry_idx;

						if (!read_key(outer_chunk_, true, probe_row, &key))
							continue;
						hash = hash_key(key);
						entry_idx = bucket_heads_.empty() ? -1 : bucket_heads_[hash & bucket_mask_];
						while (entry_idx >= 0)
						{
							const VecHashEntry &entry = entries_[entry_idx];
							const DataChunk<DEFAULT_CHUNK_SIZE> *build_chunk = inner_chunks_[entry.chunk_idx];

							if (entry.hash == hash &&
								keys_equal(entry.key, key) &&
								candidate_passes_join_filter(*build_chunk, entry.row_idx, outer_chunk_, probe_row))
								inner_entry_matched_[entry_idx] = 1;
							entry_idx = entry.next;
						}
					}
				}
				semi_build_marked_ = true;
				semi_build_emit_pos_ = 0;
			}

			while (semi_build_emit_pos_ < entries_.size() && chunk.count < DEFAULT_CHUNK_SIZE)
			{
				const VecHashEntry &entry = entries_[semi_build_emit_pos_];
				const DataChunk<DEFAULT_CHUNK_SIZE> *build_chunk;

				if (!inner_entry_matched_[semi_build_emit_pos_++])
					continue;
				build_chunk = inner_chunks_[entry.chunk_idx];
				for (const auto &output_col : output_cols_)
				{
					int out_col = output_col.output_resno - 1;
					int src_col = output_col.input_col;

					if (output_col.output_resno > visible_output_count_)
						continue;
					if (output_col.side != VecJoinSide::Outer)
						elog(ERROR, "pg_volvec semi join build-outer path cannot expose unmatched inner columns");
					chunk.nulls[out_col][chunk.count] = build_chunk->nulls[src_col][entry.row_idx];
					if (chunk.nulls[out_col][chunk.count])
						continue;
					switch (output_col.meta.storage_kind)
					{
						case VecOutputStorageKind::Double:
							chunk.double_columns[out_col][chunk.count] =
								build_chunk->double_columns[src_col][entry.row_idx];
							break;
						case VecOutputStorageKind::Int64:
						case VecOutputStorageKind::NumericScaledInt64:
						case VecOutputStorageKind::NumericAvgPair:
							chunk.int64_columns[out_col][chunk.count] =
								build_chunk->int64_columns[src_col][entry.row_idx];
							chunk.double_columns[out_col][chunk.count] =
								build_chunk->double_columns[src_col][entry.row_idx];
							break;
						case VecOutputStorageKind::StringRef:
							chunk.string_columns[out_col][chunk.count] =
								CopyStringRefToChunk(chunk, *build_chunk,
													 build_chunk->string_columns[src_col][entry.row_idx]);
							break;
						case VecOutputStorageKind::Int32:
							chunk.int32_columns[out_col][chunk.count] =
								build_chunk->int32_columns[src_col][entry.row_idx];
							break;
					}
				}
				chunk.count++;
			}

			return chunk.count > 0;
		}

		while (chunk.count < DEFAULT_CHUNK_SIZE)
		{
			int active_count;

			if ((outer_chunk_.has_selection ? outer_chunk_.sel.count : outer_chunk_.count) == 0 &&
				!advance_outer_batch())
				break;

			active_count = outer_chunk_.has_selection ? outer_chunk_.sel.count : outer_chunk_.count;
			while (anti_outer_pos_ < active_count && chunk.count < DEFAULT_CHUNK_SIZE)
			{
				int outer_row = outer_chunk_.has_selection ?
					outer_chunk_.sel.row_ids[anti_outer_pos_] : anti_outer_pos_;
				VecHashJoinKey key;
				bool has_match = false;
				int32_t entry_idx = -1;
				int32_t matched_entry_idx = -1;

				anti_outer_pos_++;
				if (read_key(outer_chunk_, false, outer_row, &key))
				{
					uint32_t hash = hash_key(key);

					entry_idx = bucket_heads_.empty() ? -1 : bucket_heads_[hash & bucket_mask_];
					while (entry_idx >= 0)
					{
						const VecHashEntry &entry = entries_[entry_idx];
						const DataChunk<DEFAULT_CHUNK_SIZE> *inner_chunk = inner_chunks_[entry.chunk_idx];

						if (entry.hash == hash &&
							keys_equal(entry.key, key) &&
							candidate_passes_join_filter(outer_chunk_, outer_row, *inner_chunk, entry.row_idx))
						{
							has_match = true;
							matched_entry_idx = entry_idx;
							break;
						}
						entry_idx = entry.next;
					}
				}
				if (!has_match)
					continue;

				for (const auto &output_col : output_cols_)
				{
					int out_col = output_col.output_resno - 1;
					int src_col = output_col.input_col;
					const VecHashEntry &matched_entry = entries_[matched_entry_idx];
					const DataChunk<DEFAULT_CHUNK_SIZE> *inner_chunk =
						inner_chunks_[matched_entry.chunk_idx];

					if (output_col.output_resno > visible_output_count_)
						continue;
					if (output_col.side == VecJoinSide::Outer)
						chunk.nulls[out_col][chunk.count] = outer_chunk_.nulls[src_col][outer_row];
					else
						chunk.nulls[out_col][chunk.count] = inner_chunk->nulls[src_col][matched_entry.row_idx];
					if (chunk.nulls[out_col][chunk.count])
						continue;
					switch (output_col.meta.storage_kind)
					{
						case VecOutputStorageKind::Double:
							chunk.double_columns[out_col][chunk.count] =
								(output_col.side == VecJoinSide::Outer) ?
								outer_chunk_.double_columns[src_col][outer_row] :
								inner_chunk->double_columns[src_col][matched_entry.row_idx];
							break;
						case VecOutputStorageKind::Int64:
						case VecOutputStorageKind::NumericScaledInt64:
						case VecOutputStorageKind::NumericAvgPair:
							chunk.int64_columns[out_col][chunk.count] =
								(output_col.side == VecJoinSide::Outer) ?
								outer_chunk_.int64_columns[src_col][outer_row] :
								inner_chunk->int64_columns[src_col][matched_entry.row_idx];
							chunk.double_columns[out_col][chunk.count] =
								(output_col.side == VecJoinSide::Outer) ?
								outer_chunk_.double_columns[src_col][outer_row] :
								inner_chunk->double_columns[src_col][matched_entry.row_idx];
							break;
						case VecOutputStorageKind::StringRef:
							chunk.string_columns[out_col][chunk.count] =
								(output_col.side == VecJoinSide::Outer) ?
								CopyStringRefToChunk(chunk, outer_chunk_,
													 outer_chunk_.string_columns[src_col][outer_row]) :
								CopyStringRefToChunk(chunk, *inner_chunk,
													 inner_chunk->string_columns[src_col][matched_entry.row_idx]);
							break;
						case VecOutputStorageKind::Int32:
							chunk.int32_columns[out_col][chunk.count] =
								(output_col.side == VecJoinSide::Outer) ?
								outer_chunk_.int32_columns[src_col][outer_row] :
								inner_chunk->int32_columns[src_col][matched_entry.row_idx];
							break;
					}
				}
				chunk.count++;
			}

			if (anti_outer_pos_ >= active_count)
				outer_chunk_.reset();
			if (chunk.count > 0)
				return true;
		}

		return chunk.count > 0;
	}
	if (jointype_ == JOIN_RIGHT_ANTI)
	{
		if (!right_anti_marked_)
		{
			while (advance_outer_batch())
			{
				int active_count = outer_chunk_.has_selection ? outer_chunk_.sel.count : outer_chunk_.count;

				for (int s = 0; s < active_count; s++)
				{
					int outer_row = outer_chunk_.has_selection ? outer_chunk_.sel.row_ids[s] : s;
					VecHashJoinKey key;
					uint32_t hash;
					int32_t entry_idx;

					if (!read_key(outer_chunk_, false, outer_row, &key))
						continue;
					hash = hash_key(key);
					entry_idx = bucket_heads_.empty() ? -1 : bucket_heads_[hash & bucket_mask_];
					while (entry_idx >= 0)
					{
						const VecHashEntry &entry = entries_[entry_idx];
						const DataChunk<DEFAULT_CHUNK_SIZE> *inner_chunk = inner_chunks_[entry.chunk_idx];

						if (entry.hash == hash &&
							keys_equal(entry.key, key) &&
							candidate_passes_join_filter(outer_chunk_, outer_row, *inner_chunk, entry.row_idx))
							inner_entry_matched_[entry_idx] = 1;
						entry_idx = entry.next;
					}
				}
			}
			right_anti_marked_ = true;
			right_anti_emit_pos_ = 0;
		}

		while (right_anti_emit_pos_ < entries_.size() && chunk.count < DEFAULT_CHUNK_SIZE)
		{
			const VecHashEntry &entry = entries_[right_anti_emit_pos_];
			const DataChunk<DEFAULT_CHUNK_SIZE> *inner_chunk;

			if (inner_entry_matched_[right_anti_emit_pos_++])
				continue;
			inner_chunk = inner_chunks_[entry.chunk_idx];
			for (const auto &output_col : output_cols_)
			{
				int out_col = output_col.output_resno - 1;
				int src_col = output_col.input_col;

				if (output_col.output_resno > visible_output_count_)
					continue;
				if (output_col.side != VecJoinSide::Inner)
					elog(ERROR, "pg_volvec right anti join does not support outer output columns");
				chunk.nulls[out_col][chunk.count] = inner_chunk->nulls[src_col][entry.row_idx];
				if (chunk.nulls[out_col][chunk.count])
					continue;
				switch (output_col.meta.storage_kind)
				{
					case VecOutputStorageKind::Double:
						chunk.double_columns[out_col][chunk.count] =
							inner_chunk->double_columns[src_col][entry.row_idx];
						break;
					case VecOutputStorageKind::Int64:
					case VecOutputStorageKind::NumericScaledInt64:
					case VecOutputStorageKind::NumericAvgPair:
						chunk.int64_columns[out_col][chunk.count] =
							inner_chunk->int64_columns[src_col][entry.row_idx];
						chunk.double_columns[out_col][chunk.count] =
							inner_chunk->double_columns[src_col][entry.row_idx];
						break;
					case VecOutputStorageKind::StringRef:
						chunk.string_columns[out_col][chunk.count] =
							CopyStringRefToChunk(chunk, *inner_chunk,
												 inner_chunk->string_columns[src_col][entry.row_idx]);
						break;
					case VecOutputStorageKind::Int32:
						chunk.int32_columns[out_col][chunk.count] =
							inner_chunk->int32_columns[src_col][entry.row_idx];
						break;
				}
			}
			chunk.count++;
		}

		return chunk.count > 0;
	}

	while (chunk.count < DEFAULT_CHUNK_SIZE)
	{
		if (!probe_batch_ready_)
		{
			if ((outer_chunk_.has_selection ? outer_chunk_.sel.count : outer_chunk_.count) == 0 &&
				!advance_outer_batch())
				break;
			prepare_probe_batch();
		}

		if (active_probe_sel_.empty())
		{
			outer_chunk_.reset();
			probe_batch_ready_ = false;
			continue;
		}

		next_probe_sel_.clear();
		for (uint16_t probe_idx : active_probe_sel_)
		{
			int32_t match_entry_idx;

			if (chunk.count >= DEFAULT_CHUNK_SIZE)
			{
				next_probe_sel_.push_back(probe_idx);
				continue;
			}
			if (advance_probe_match(probe_idx, &match_entry_idx))
			{
				const VecHashEntry &entry = entries_[match_entry_idx];
				const DataChunk<DEFAULT_CHUNK_SIZE> *inner_chunk = inner_chunks_[entry.chunk_idx];
				int outer_row = probe_rows_[probe_idx];
				int dst_row = chunk.count++;
				bool probe_is_outer = !build_outer_side_;

				if (jointype_ == JOIN_RIGHT)
					inner_entry_matched_[match_entry_idx] = 1;

				for (const auto &output_col : output_cols_)
				{
					int out_col = output_col.output_resno - 1;
					const DataChunk<DEFAULT_CHUNK_SIZE> *src;
					int src_row;
					int src_col = output_col.input_col;

					if ((output_col.side == VecJoinSide::Outer && probe_is_outer) ||
						(output_col.side == VecJoinSide::Inner && !probe_is_outer))
					{
						src = &outer_chunk_;
						src_row = outer_row;
					}
					else
					{
						src = inner_chunk;
						src_row = entry.row_idx;
					}

					copy_output_value(dst_row, out_col, output_col.meta, *src, src_col, src_row);
				}

				if (probe_next_entries_[probe_idx] >= 0)
					next_probe_sel_.push_back(probe_idx);
			}
			else if (jointype_ == JOIN_LEFT && !build_outer_side_)
			{
				int outer_row = probe_rows_[probe_idx];
				int dst_row = chunk.count++;

				for (const auto &output_col : output_cols_)
				{
					int out_col = output_col.output_resno - 1;

					if (output_col.side == VecJoinSide::Outer)
						copy_output_value(dst_row, out_col, output_col.meta,
										  outer_chunk_, output_col.input_col, outer_row);
					else
					{
						chunk.nulls[out_col][dst_row] = 1;
						chunk.string_columns[out_col][dst_row] = VecStringRef{0, 0, 0};
					}
				}
			}
		}

		active_probe_sel_.swap(next_probe_sel_);
		if (active_probe_sel_.empty())
		{
			outer_chunk_.reset();
			probe_batch_ready_ = false;
		}
	}

	if (jointype_ == JOIN_RIGHT)
	{
		if (!right_anti_marked_)
		{
			if (!probe_input_exhausted_)
				return chunk.count > 0;
			right_anti_marked_ = true;
			right_anti_emit_pos_ = 0;
		}

		while (right_anti_emit_pos_ < entries_.size() && chunk.count < DEFAULT_CHUNK_SIZE)
		{
			const VecHashEntry &entry = entries_[right_anti_emit_pos_];
			const DataChunk<DEFAULT_CHUNK_SIZE> *inner_chunk;

			if (inner_entry_matched_[right_anti_emit_pos_++])
				continue;
			inner_chunk = inner_chunks_[entry.chunk_idx];
			for (const auto &output_col : output_cols_)
			{
				int out_col = output_col.output_resno - 1;

				if (output_col.side == VecJoinSide::Outer)
				{
					chunk.nulls[out_col][chunk.count] = 1;
					chunk.string_columns[out_col][chunk.count] = VecStringRef{0, 0, 0};
					continue;
				}
				copy_output_value(chunk.count, out_col, output_col.meta,
								  *inner_chunk, output_col.input_col, entry.row_idx);
			}
			chunk.count++;
		}
	}

	return chunk.count > 0;
}

VecSortState::VecSortState(std::unique_ptr<VecPlanState> left, Sort *node,
						   VolVecVector<VecSortKeyDesc> key_descs,
						   int output_ncols)
	: left_(std::move(left)),
	  node_(node),
	  memory_context_(CurrentMemoryContext),
	  payload_chunks_(PgMemoryContextAllocator<DataChunk<DEFAULT_CHUNK_SIZE> *>(memory_context_)),
	  rows_(PgMemoryContextAllocator<VecRowRef>(memory_context_)),
	  key_descs_(PgMemoryContextAllocator<VecSortKeyDesc>(memory_context_)),
	  key_lanes_(PgMemoryContextAllocator<VecSortKeyLane>(memory_context_)),
	  emit_pos_(0),
	  output_ncols_(0),
	  materialized_(false)
{
	int max_output_col = output_ncols > 0 ? output_ncols : list_length(node->plan.targetlist);

	for (const auto &key_desc : key_descs)
	{
		key_descs_.push_back(key_desc);
		key_lanes_.emplace_back(key_desc, memory_context_);
		max_output_col = Max(max_output_col, (int) key_desc.col_idx + 1);
	}
	output_ncols_ = Min(max_output_col, 16);
}

VecSortState::~VecSortState()
{
	for (auto *chunk : payload_chunks_)
		delete chunk;
}

DataChunk<DEFAULT_CHUNK_SIZE> *
VecSortState::allocate_payload_chunk()
{
	MemoryContext old_context = MemoryContextSwitchTo(memory_context_);
	DataChunk<DEFAULT_CHUNK_SIZE> *chunk = new DataChunk<DEFAULT_CHUNK_SIZE>();
	MemoryContextSwitchTo(old_context);
	payload_chunks_.push_back(chunk);
	return chunk;
}

void
VecSortState::copy_row(const DataChunk<DEFAULT_CHUNK_SIZE> &src, int src_row,
					   DataChunk<DEFAULT_CHUNK_SIZE> &dst, int dst_row) const
{
		for (int col = 0; col < output_ncols_; col++)
		{
			dst.double_columns[col][dst_row] = src.double_columns[col][src_row];
			dst.int64_columns[col][dst_row] = src.int64_columns[col][src_row];
			dst.int32_columns[col][dst_row] = src.int32_columns[col][src_row];
			dst.nulls[col][dst_row] = src.nulls[col][src_row];
			if (!dst.nulls[col][dst_row])
				dst.string_columns[col][dst_row] =
					CopyStringRefToChunk(dst, src, src.string_columns[col][src_row]);
			else
				dst.string_columns[col][dst_row] = VecStringRef{0, 0, 0};
		}
	}

void
VecSortState::append_sort_key(uint32_t ordinal,
							  const DataChunk<DEFAULT_CHUNK_SIZE> &input,
							  int src_row)
{
	for (auto &lane : key_lanes_)
	{
		const VecSortKeyDesc &key = lane.desc;
		bool is_null = input.nulls[key.col_idx][src_row] != 0;

		Assert(lane.nulls.size() == ordinal);
		lane.nulls.push_back((uint8_t) is_null);
		if (is_null)
		{
			lane.i32_values.push_back(0);
			lane.i64_values.push_back(0);
			lane.u64_values.push_back(0);
			lane.string_values.push_back(VecStringRef{0, 0, 0});
			continue;
		}

		switch (key.storage_kind)
		{
			case VecOutputStorageKind::Int32:
				lane.i32_values.push_back(input.int32_columns[key.col_idx][src_row]);
				lane.i64_values.push_back(0);
				lane.u64_values.push_back(0);
				lane.string_values.push_back(VecStringRef{0, 0, 0});
				break;
			case VecOutputStorageKind::Int64:
			case VecOutputStorageKind::NumericScaledInt64:
				lane.i32_values.push_back(0);
				lane.i64_values.push_back(input.int64_columns[key.col_idx][src_row]);
				lane.u64_values.push_back(0);
				lane.string_values.push_back(VecStringRef{0, 0, 0});
				break;
			case VecOutputStorageKind::Double:
				lane.i32_values.push_back(0);
				lane.i64_values.push_back(0);
				lane.u64_values.push_back(EncodeFloat8SortKey(input.double_columns[key.col_idx][src_row]));
				lane.string_values.push_back(VecStringRef{0, 0, 0});
				break;
			case VecOutputStorageKind::StringRef:
			{
				VecStringRef ref = input.string_columns[key.col_idx][src_row];

				lane.i32_values.push_back(0);
				lane.i64_values.push_back(0);
				lane.u64_values.push_back(0);
				lane.string_values.push_back(
					lane.store_string_bytes(input.get_string_ptr(ref), ref.len));
				break;
			}
			case VecOutputStorageKind::NumericAvgPair:
				elog(ERROR, "pg_volvec vector sort does not yet support numeric average sort keys");
				break;
		}
	}
}

void
VecSortState::append_batch(const DataChunk<DEFAULT_CHUNK_SIZE> &input)
{
	int active_count = input.has_selection ? input.sel.count : input.count;
	DataChunk<DEFAULT_CHUNK_SIZE> *dst =
		payload_chunks_.empty() ? allocate_payload_chunk() : payload_chunks_.back();

	for (int s = 0; s < active_count; s++)
	{
		int src_row = input.has_selection ? input.sel.row_ids[s] : s;
		int dst_row;
		uint32_t ordinal;

		if (dst->count >= DEFAULT_CHUNK_SIZE)
			dst = allocate_payload_chunk();

		dst_row = dst->count;
		ordinal = (uint32_t) rows_.size();
		copy_row(input, src_row, *dst, dst_row);
		rows_.push_back(VecRowRef{ordinal, (uint32_t) (payload_chunks_.size() - 1), (uint16_t) dst_row});
		append_sort_key(ordinal, input, src_row);
		dst->count++;
	}
}

int
VecSortState::compare_string_ref(const VecSortKeyLane &lane,
								 const VecStringRef &left,
								 const VecStringRef &right) const
{
	const char *left_ptr = lane.get_string_ptr(left);
	const char *right_ptr = lane.get_string_ptr(right);
	uint32_t left_len = left.len;
	uint32_t right_len = right.len;
	int cmp_len;
	int cmp;

	if (lane.desc.sql_type == BPCHAROID)
	{
		while (left_len > 0 && left_ptr[left_len - 1] == ' ')
			left_len--;
		while (right_len > 0 && right_ptr[right_len - 1] == ' ')
			right_len--;
	}

	cmp_len = Min((int) left_len, (int) right_len);
	cmp = memcmp(left_ptr, right_ptr, cmp_len);
	if (cmp < 0)
		return -1;
	if (cmp > 0)
		return 1;
	if (left_len < right_len)
		return -1;
	if (left_len > right_len)
		return 1;
	return 0;
}

bool
VecSortState::row_less(const VecRowRef &left, const VecRowRef &right) const
{
	for (const auto &lane : key_lanes_)
	{
		bool left_null = lane.nulls[left.ordinal] != 0;
		bool right_null = lane.nulls[right.ordinal] != 0;
		int cmp = 0;

		if (left_null != right_null)
			return lane.desc.nulls_first ? left_null : !left_null;
		if (left_null)
			continue;

		switch (lane.desc.storage_kind)
		{
			case VecOutputStorageKind::Int32:
				if (lane.i32_values[left.ordinal] < lane.i32_values[right.ordinal])
					cmp = -1;
				else if (lane.i32_values[left.ordinal] > lane.i32_values[right.ordinal])
					cmp = 1;
				break;
			case VecOutputStorageKind::Int64:
			case VecOutputStorageKind::NumericScaledInt64:
				if (lane.i64_values[left.ordinal] < lane.i64_values[right.ordinal])
					cmp = -1;
				else if (lane.i64_values[left.ordinal] > lane.i64_values[right.ordinal])
					cmp = 1;
				break;
			case VecOutputStorageKind::Double:
				if (lane.u64_values[left.ordinal] < lane.u64_values[right.ordinal])
					cmp = -1;
				else if (lane.u64_values[left.ordinal] > lane.u64_values[right.ordinal])
					cmp = 1;
				break;
			case VecOutputStorageKind::StringRef:
				cmp = compare_string_ref(lane,
										 lane.string_values[left.ordinal],
										 lane.string_values[right.ordinal]);
				break;
			case VecOutputStorageKind::NumericAvgPair:
				elog(ERROR, "pg_volvec vector sort does not yet support numeric average sort keys");
				break;
		}

		if (cmp != 0)
			return lane.desc.descending ? (cmp > 0) : (cmp < 0);
	}

	return left.ordinal < right.ordinal;
}

void
VecSortState::materialize_and_sort()
{
	DataChunk<DEFAULT_CHUNK_SIZE> input;

	if (materialized_)
		return;

	while (left_->get_next_batch(input))
		append_batch(input);

	std::stable_sort(rows_.begin(), rows_.end(),
					 [this](const VecRowRef &left, const VecRowRef &right)
					 {
						 return row_less(left, right);
					 });
	emit_pos_ = 0;
	materialized_ = true;
}

bool
VecSortState::get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk)
{
	if (!materialized_)
		materialize_and_sort();

	chunk.reset();
	while (emit_pos_ < rows_.size() && chunk.count < DEFAULT_CHUNK_SIZE)
	{
		const VecRowRef &row = rows_[emit_pos_];
		const DataChunk<DEFAULT_CHUNK_SIZE> *src = payload_chunks_[row.chunk_idx];

		copy_row(*src, row.row_idx, chunk, chunk.count);
		chunk.count++;
		emit_pos_++;
	}

	return chunk.count > 0;
}

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

static std::unique_ptr<VecPlanState>
BuildAggWithOptionalProject(std::unique_ptr<VecPlanState> left, Agg *node, EState *estate)
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

	if (node->plan.qual != NIL)
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
					elog(LOG, "pg_volvec: hash join string-prefix project metadata lookup failed for target resno %d",
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
			if (pg_volvec_trace_hooks &&
				(project_col.sql_type == BPCHAROID ||
				 project_col.sql_type == TEXTOID ||
				 project_col.sql_type == VARCHAROID))
			{
				if (stripped_expr != nullptr && IsA(stripped_expr, FuncExpr))
				{
					FuncExpr *func = (FuncExpr *) stripped_expr;
					Expr *arg0 = list_length(func->args) > 0 ?
						StripImplicitNodesLocal((Expr *) linitial(func->args)) : nullptr;
					Expr *arg1 = list_length(func->args) > 1 ?
						StripImplicitNodesLocal((Expr *) lsecond(func->args)) : nullptr;
					Expr *arg2 = list_length(func->args) > 2 ?
						StripImplicitNodesLocal((Expr *) lthird(func->args)) : nullptr;

					elog(LOG,
						 "pg_volvec: hash join string project fallback expr func=%s nargs=%d rettype=%u arg0_tag=%d arg0_type=%u arg1_tag=%d arg2_tag=%d",
						 get_func_name(func->funcid),
						 list_length(func->args),
						 exprType((Node *) stripped_expr),
						 arg0 != nullptr ? (int) nodeTag(arg0) : -1,
						 arg0 != nullptr ? exprType((Node *) arg0) : InvalidOid,
						 arg1 != nullptr ? (int) nodeTag(arg1) : -1,
						 arg2 != nullptr ? (int) nodeTag(arg2) : -1);
				}
				else
				{
					elog(LOG,
						 "pg_volvec: hash join string project fallback expr node_tag=%d rettype=%u",
						 stripped_expr != nullptr ? (int) nodeTag(stripped_expr) : -1,
						 exprType((Node *) tle->expr));
				}
			}
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

static bool
LookupPlanOutputMeta(Plan *plan,
					 VecPlanState *state,
					 int target_resno,
					 uint16_t *source_col,
					 VecOutputColMeta *meta)
{
	ListCell *lc;

	if (plan == nullptr || state == nullptr || target_resno <= 0 || target_resno > 16)
		return false;

	if (IsA(plan, Hash))
	{
		foreach(lc, plan->targetlist)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(lc);
			Expr *expr;
			Var *var;

			if (tle->resno != target_resno)
				continue;
			expr = StripImplicitNodesLocal((Expr *) tle->expr);
			if (expr == nullptr || !IsA(expr, Var))
				return false;
			var = (Var *) expr;
			if (var->varattno <= 0 || var->varattno > 16)
				return false;
			if (source_col != nullptr)
				*source_col = (uint16_t) (var->varattno - 1);
			return state->lookup_output_col_meta(var->varattno, meta);
		}
		if (pg_volvec_trace_hooks)
			elog(LOG, "pg_volvec: hash output metadata lookup found no targetlist entry for resno %d",
				 target_resno);
		return false;
	}

	if (source_col != nullptr)
		*source_col = (uint16_t) (target_resno - 1);
	return state->lookup_output_col_meta(target_resno, meta);
}

static bool
ResolveHashJoinVarBinding(Var *var,
						  Plan *outer_plan,
						  Plan *inner_plan,
						  VecPlanState *outer,
						  VecPlanState *inner,
						  VecJoinSide *side,
						  uint16_t *source_col,
						  VecOutputColMeta *meta)
{
	if (var == nullptr || source_col == nullptr || meta == nullptr)
		return false;
	if (var->varattno <= 0 || var->varattno > 16)
		return false;

	if (var->varno == OUTER_VAR)
	{
		if (!LookupPlanOutputMeta(outer_plan, outer, var->varattno, source_col, meta))
			return false;
		if (side != nullptr)
			*side = VecJoinSide::Outer;
		return true;
	}
	if (var->varno == INNER_VAR)
	{
		if (!LookupPlanOutputMeta(inner_plan, inner, var->varattno, source_col, meta))
			return false;
		if (side != nullptr)
			*side = VecJoinSide::Inner;
		return true;
	}
	return false;
}

static bool
EnsureHashJoinOutputCol(VecJoinSide side,
						  uint16_t source_col,
						  const VecOutputColMeta &meta,
						  VolVecVector<VecJoinOutputCol> *output_cols,
						  int *join_resno)
{
	if (output_cols == nullptr)
		return false;

	for (const auto &output_col : *output_cols)
	{
		if (output_col.side == side && output_col.input_col == source_col)
		{
			if (join_resno != nullptr)
				*join_resno = output_col.output_resno;
			return true;
		}
	}

	if (output_cols->size() >= 16)
		return false;

	output_cols->push_back(VecJoinOutputCol{side,
											 source_col,
											 (int) output_cols->size() + 1,
											 meta});
	if (join_resno != nullptr)
		*join_resno = output_cols->back().output_resno;
	return true;
}

static bool
IsLookupCompatibleStorage(VecOutputStorageKind kind)
{
	return kind == VecOutputStorageKind::Int32 ||
		   kind == VecOutputStorageKind::Int64 ||
		   kind == VecOutputStorageKind::NumericScaledInt64;
}

static Plan *
LookupReferencedSubPlan(EState *estate, SubPlan *subplan)
{
	if (estate == nullptr || estate->es_plannedstmt == nullptr ||
		subplan == nullptr || subplan->plan_id <= 0)
		return nullptr;
	if (list_length(estate->es_plannedstmt->subplans) < subplan->plan_id)
		return nullptr;
	return (Plan *) list_nth(estate->es_plannedstmt->subplans, subplan->plan_id - 1);
}

static bool
ExtractParamEqualityVar(Expr *expr, int paramid, Var **scan_var)
{
	OpExpr *op;
	Expr *left;
	Expr *right;
	Param *param = nullptr;
	Var *var = nullptr;
	char *opname;

	expr = StripImplicitNodesLocal(expr);
	if (scan_var != nullptr)
		*scan_var = nullptr;
	if (expr == nullptr || !IsA(expr, OpExpr))
		return false;
	op = (OpExpr *) expr;
	if (list_length(op->args) != 2)
		return false;
	opname = get_opname(op->opno);
	if (opname == nullptr || strcmp(opname, "=") != 0)
		return false;

	left = StripImplicitNodesLocal((Expr *) linitial(op->args));
	right = StripImplicitNodesLocal((Expr *) lsecond(op->args));
	if (left != nullptr && right != nullptr && IsA(left, Var) && IsA(right, Param))
	{
		var = (Var *) left;
		param = (Param *) right;
	}
	else if (left != nullptr && right != nullptr && IsA(left, Param) && IsA(right, Var))
	{
		var = (Var *) right;
		param = (Param *) left;
	}
	else
		return false;

	if (param->paramkind != PARAM_EXEC || param->paramid != paramid ||
		var->varlevelsup != 0 || var->varattno <= 0 || var->varattno > kMaxDeformTargets)
		return false;
	if (scan_var != nullptr)
		*scan_var = var;
	return true;
}

static void
StripParamEqualityFromPlanQuals(Plan *plan, int paramid, Var **scan_var, bool *removed)
{
	List *new_quals = NIL;
	ListCell *lc;

	if (plan == nullptr)
		return;

	foreach(lc, plan->qual)
	{
		Expr *qual = (Expr *) lfirst(lc);
		Var *matched_var = nullptr;

		if (ExtractParamEqualityVar(qual, paramid, &matched_var))
		{
			if (scan_var != nullptr && matched_var != nullptr)
			{
				if (*scan_var == nullptr)
					*scan_var = (Var *) copyObjectImpl(matched_var);
				else if (!equal(*scan_var, matched_var))
					*removed = false;
			}
			if (removed != nullptr)
				*removed = true;
			continue;
		}
		new_quals = lappend(new_quals, qual);
	}
	plan->qual = new_quals;

	StripParamEqualityFromPlanQuals(plan->lefttree, paramid, scan_var, removed);
	StripParamEqualityFromPlanQuals(plan->righttree, paramid, scan_var, removed);
	if (IsA(plan, SubqueryScan))
		StripParamEqualityFromPlanQuals(((SubqueryScan *) plan)->subplan,
										paramid,
										scan_var,
										removed);
}

static bool
PlanContainsScanRelid(Plan *plan, Index scanrelid)
{
	if (plan == nullptr || scanrelid <= 0)
		return false;
	if (IsA(plan, SeqScan))
		return ((SeqScan *) plan)->scan.scanrelid == scanrelid;
	if (IsA(plan, SubqueryScan) &&
		PlanContainsScanRelid(((SubqueryScan *) plan)->subplan, scanrelid))
		return true;
	if (PlanContainsScanRelid(plan->lefttree, scanrelid))
		return true;
	return PlanContainsScanRelid(plan->righttree, scanrelid);
}

static Expr *
BuildJoinLookupKeyExpr(Plan *join_plan, Var *scan_key_var)
{
	bool in_outer;
	bool in_inner;

	if (join_plan == nullptr || scan_key_var == nullptr)
		return nullptr;
	in_outer = PlanContainsScanRelid(join_plan->lefttree, scan_key_var->varno);
	in_inner = PlanContainsScanRelid(join_plan->righttree, scan_key_var->varno);
	if (in_outer == in_inner)
		return nullptr;

	return (Expr *) makeVar(in_outer ? OUTER_VAR : INNER_VAR,
							scan_key_var->varattno,
							scan_key_var->vartype,
							scan_key_var->vartypmod,
							scan_key_var->varcollid,
							0);
}

static std::unique_ptr<VecPlanState>
BuildCorrelatedLookupAggState(EState *estate,
							  Agg *subplan_agg,
							  int paramid,
							  VecOutputColMeta *key_meta,
							  VecOutputColMeta *value_meta)
{
	Agg *grouped_agg;
	Plan *grouped_child;
	TargetEntry *value_tle;
	std::unique_ptr<VecPlanState> lookup_state;
	Aggref *lookup_aggref;
	TargetEntry *lookup_arg_tle;
	TargetEntry *child_value_tle;
	Expr *child_key_expr;
	bool removed = false;
	Var *extracted_key = nullptr;
	Var *lookup_key_var;

	if (estate == nullptr || subplan_agg == nullptr ||
		subplan_agg->plan.lefttree == nullptr ||
		subplan_agg->plan.targetlist == NIL)
		return nullptr;

	grouped_agg = (Agg *) copyObjectImpl(subplan_agg);
	grouped_child = grouped_agg->plan.lefttree;
	value_tle = (TargetEntry *) linitial(subplan_agg->plan.targetlist);
	StripParamEqualityFromPlanQuals(grouped_child, paramid, &extracted_key, &removed);
	lookup_key_var = extracted_key;
	if (!removed || lookup_key_var == nullptr)
		return nullptr;

	if (IsA(subplan_agg->plan.lefttree, SeqScan))
	{
		grouped_agg->aggstrategy = AGG_HASHED;
		grouped_agg->numCols = 1;
		grouped_agg->grpColIdx = (AttrNumber *) palloc(sizeof(AttrNumber));
		grouped_agg->grpColIdx[0] = (AttrNumber) lookup_key_var->varattno;
		grouped_agg->plan.qual = NIL;
		grouped_agg->plan.targetlist =
			list_make2(makeTargetEntry((Expr *) copyObjectImpl(lookup_key_var),
									   1,
									   NULL,
									   false),
					   makeTargetEntry((Expr *) copyObjectImpl(value_tle->expr),
									   2,
									   NULL,
									   false));
	}
	else if (IsA(subplan_agg->plan.lefttree, HashJoin) ||
			 IsA(subplan_agg->plan.lefttree, MergeJoin))
	{
		child_key_expr = BuildJoinLookupKeyExpr(grouped_child, lookup_key_var);
		if (child_key_expr == nullptr || grouped_child->targetlist == NIL ||
			list_length(grouped_child->targetlist) != 1)
			return nullptr;

		child_value_tle = (TargetEntry *) linitial(grouped_child->targetlist);
		grouped_child->targetlist =
			list_make2(makeTargetEntry(child_key_expr,
									   1,
									   NULL,
									   false),
					   makeTargetEntry((Expr *) copyObjectImpl(child_value_tle->expr),
									   2,
									   NULL,
									   false));

		if (value_tle == nullptr || !IsA(StripImplicitNodesLocal((Expr *) value_tle->expr), Aggref))
			return nullptr;
		lookup_aggref = (Aggref *) copyObjectImpl(StripImplicitNodesLocal((Expr *) value_tle->expr));
		if (lookup_aggref->args == NIL || list_length(lookup_aggref->args) != 1)
			return nullptr;
		lookup_arg_tle = (TargetEntry *) linitial(lookup_aggref->args);
		lookup_arg_tle->expr =
			(Expr *) makeVar(1,
							 2,
							 exprType((Node *) child_value_tle->expr),
							 exprTypmod((Node *) child_value_tle->expr),
							 exprCollation((Node *) child_value_tle->expr),
							 0);

		grouped_agg->aggstrategy = AGG_HASHED;
		grouped_agg->numCols = 1;
		grouped_agg->grpColIdx = (AttrNumber *) palloc(sizeof(AttrNumber));
		grouped_agg->grpColIdx[0] = 1;
		grouped_agg->plan.qual = NIL;
		grouped_agg->plan.targetlist =
			list_make2(makeTargetEntry((Expr *) makeVar(1,
													  1,
													  lookup_key_var->vartype,
													  lookup_key_var->vartypmod,
													  lookup_key_var->varcollid,
													  0),
									   1,
									   NULL,
									   false),
					   makeTargetEntry((Expr *) lookup_aggref,
									   2,
									   NULL,
									   false));
	}
	else
	{
		return nullptr;
	}

	lookup_state = ExecInitVecPlanInternal((Plan *) grouped_agg, estate, nullptr, false);
	if (!lookup_state)
		return nullptr;
	if ((key_meta != nullptr &&
		 !lookup_state->lookup_output_col_meta(1, key_meta)) ||
		(value_meta != nullptr &&
		 !lookup_state->lookup_output_col_meta(2, value_meta)))
		return nullptr;
	return lookup_state;
}

static bool
ExtractComparableLookupVar(Expr *expr, Var **var_out)
{
	Expr *stripped = StripImplicitNodesLocal(expr);

	if (var_out != nullptr)
		*var_out = nullptr;
	if (stripped == nullptr)
		return false;
	if (IsA(stripped, Var))
	{
		if (var_out != nullptr)
			*var_out = (Var *) stripped;
		return true;
	}
	if (IsA(stripped, FuncExpr))
	{
		FuncExpr *func = (FuncExpr *) stripped;
		Expr *arg;

		if (list_length(func->args) != 1 || exprType((Node *) stripped) != NUMERICOID)
			return false;
		arg = StripImplicitNodesLocal((Expr *) linitial(func->args));
		if (arg == nullptr || !IsA(arg, Var))
			return false;
		if (!IsInt64LikeTypeLocal(exprType((Node *) arg)))
			return false;
		if (var_out != nullptr)
			*var_out = (Var *) arg;
		return true;
	}
	return false;
}

static int
FindNextFreeOutputResno(VecPlanState *state)
{
	VecOutputColMeta meta;

	if (state == nullptr)
		return -1;
	for (int resno = 1; resno <= 16; resno++)
	{
		if (!state->lookup_output_col_meta(resno, &meta))
			return resno;
	}
	return -1;
}

static std::unique_ptr<VecPlanState>
BuildCorrelatedLookupAggStateMulti(EState *estate,
								   Agg *subplan_agg,
								   List *paramids,
								   List *args,
								   int *num_keys_out,
								   VecOutputColMeta *key_metas_out,
								   VecOutputColMeta *value_meta)
{
	Agg *grouped_agg;
	Plan *grouped_child;
	TargetEntry *value_tle;
	std::unique_ptr<VecPlanState> lookup_state;
	List *synthetic_tlist = NIL;
	ListCell *lc_param;
	ListCell *lc_arg;
	int num_keys = 0;

	if (estate == nullptr || subplan_agg == nullptr ||
		subplan_agg->plan.lefttree == nullptr ||
		subplan_agg->plan.targetlist == NIL ||
		paramids == NIL || args == NIL ||
		list_length(paramids) != list_length(args) ||
		list_length(paramids) <= 0 ||
		list_length(paramids) > kMaxLookupKeys)
		return nullptr;

	grouped_agg = (Agg *) copyObjectImpl(subplan_agg);
	grouped_child = grouped_agg->plan.lefttree;
	value_tle = (TargetEntry *) linitial(subplan_agg->plan.targetlist);

	if (!IsA(grouped_child, SeqScan))
		return nullptr;

	grouped_agg->aggstrategy = AGG_HASHED;
	grouped_agg->numCols = list_length(paramids);
	grouped_agg->grpColIdx = (AttrNumber *) palloc(sizeof(AttrNumber) * grouped_agg->numCols);
	grouped_agg->plan.qual = NIL;

	forboth(lc_param, paramids, lc_arg, args)
	{
		int paramid = lfirst_int(lc_param);
		Expr *arg_expr = StripImplicitNodesLocal((Expr *) lfirst(lc_arg));
		Var *lookup_key_var = nullptr;
		bool removed = false;

		if (arg_expr == nullptr || !IsA(arg_expr, Var))
			return nullptr;
		StripParamEqualityFromPlanQuals(grouped_child, paramid, &lookup_key_var, &removed);
		if (!removed || lookup_key_var == nullptr)
			return nullptr;
		grouped_agg->grpColIdx[num_keys] = (AttrNumber) lookup_key_var->varattno;
		synthetic_tlist =
			lappend(synthetic_tlist,
					makeTargetEntry((Expr *) copyObjectImpl(lookup_key_var),
									num_keys + 1,
									NULL,
									false));
		num_keys++;
	}

	synthetic_tlist =
		lappend(synthetic_tlist,
				makeTargetEntry((Expr *) copyObjectImpl(value_tle->expr),
								num_keys + 1,
								NULL,
								false));
	grouped_agg->plan.targetlist = synthetic_tlist;

	lookup_state = ExecInitVecPlanInternal((Plan *) grouped_agg, estate, nullptr, false);
	if (!lookup_state)
		return nullptr;
	for (int i = 0; i < num_keys; i++)
	{
		if (key_metas_out != nullptr &&
			!lookup_state->lookup_output_col_meta(i + 1, &key_metas_out[i]))
			return nullptr;
	}
	if (value_meta != nullptr &&
		!lookup_state->lookup_output_col_meta(num_keys + 1, value_meta))
		return nullptr;
	if (num_keys_out != nullptr)
		*num_keys_out = num_keys;
	return lookup_state;
}

struct CorrelatedLookupFilterSpec
{
	std::unique_ptr<VecPlanState> lookup_state;
	Expr *rewritten_expr = nullptr;
	uint16_t input_key_col = 0;
	VecOutputColMeta input_key_meta;
	uint16_t lookup_key_col = 0;
	VecOutputColMeta lookup_key_meta;
	uint16_t lookup_value_col = 0;
	int output_resno = 0;
	VecOutputColMeta output_meta;
};

struct CorrelatedLookupProjectSpec
{
	std::unique_ptr<VecPlanState> lookup_state;
	Expr *rewritten_expr = nullptr;
	int num_keys = 0;
	uint16_t input_key_cols[kMaxLookupKeys] = {0, 0, 0, 0};
	VecOutputColMeta input_key_metas[kMaxLookupKeys];
	uint16_t lookup_key_cols[kMaxLookupKeys] = {0, 0, 0, 0};
	VecOutputColMeta lookup_key_metas[kMaxLookupKeys];
	uint16_t lookup_value_col = 0;
	int output_resno = 0;
	VecOutputColMeta output_meta;
};

struct LookupMembershipFilterSpec
{
	std::unique_ptr<VecPlanState> lookup_state;
	Expr *residual_expr = nullptr;
	uint16_t input_key_col = 0;
	VecOutputColMeta input_key_meta;
	uint16_t lookup_key_col = 0;
	VecOutputColMeta lookup_key_meta;
	bool negate = false;
};

static bool
ExtractSubPlanLookupVar(SubPlan *subplan, Var **var_out)
{
	Expr *testexpr;
	OpExpr *op;
	Expr *left;
	Expr *right;
	char *opname;

	if (var_out != nullptr)
		*var_out = nullptr;
	if (subplan == nullptr || subplan->testexpr == nullptr)
		return false;

	testexpr = StripImplicitNodesLocal((Expr *) subplan->testexpr);
	if (testexpr == nullptr || !IsA(testexpr, OpExpr))
		return false;
	op = (OpExpr *) testexpr;
	if (list_length(op->args) != 2)
		return false;
	opname = get_opname(op->opno);
	if (opname == nullptr || strcmp(opname, "=") != 0)
		return false;

	left = StripImplicitNodesLocal((Expr *) linitial(op->args));
	right = StripImplicitNodesLocal((Expr *) lsecond(op->args));
	if (left != nullptr && IsA(left, Var) && right != nullptr && IsA(right, Param))
	{
		if (var_out != nullptr)
			*var_out = (Var *) left;
		return true;
	}
	if (left != nullptr && IsA(left, Param) && right != nullptr && IsA(right, Var))
	{
		if (var_out != nullptr)
			*var_out = (Var *) right;
		return true;
	}
	return false;
}

static bool
MatchLookupMembershipQual(Expr *expr,
						  VecPlanState *input_state,
						  EState *estate,
						  LookupMembershipFilterSpec *spec)
{
	bool negate = false;
	SubPlan *subplan;
	Plan *subplan_plan;
	Var *lookup_var = nullptr;
	std::unique_ptr<VecPlanState> lookup_state;
	Bitmapset *lookup_required_attrs = nullptr;

	if (expr == nullptr || input_state == nullptr || estate == nullptr || spec == nullptr)
		return false;

	expr = StripImplicitNodesLocal(expr);
	if (expr != nullptr && IsA(expr, BoolExpr))
	{
		BoolExpr *bool_expr = (BoolExpr *) expr;

		if (bool_expr->boolop == NOT_EXPR && list_length(bool_expr->args) == 1)
		{
			negate = true;
			expr = StripImplicitNodesLocal((Expr *) linitial(bool_expr->args));
		}
	}
	if (expr == nullptr || !IsA(expr, SubPlan))
		return false;

	subplan = (SubPlan *) expr;
	if (subplan->isInitPlan ||
		!subplan->useHashTable ||
		subplan->subLinkType != ANY_SUBLINK ||
		subplan->parParam != NIL ||
		subplan->args != NIL ||
		!ExtractSubPlanLookupVar(subplan, &lookup_var) ||
		lookup_var == nullptr ||
		lookup_var->varlevelsup != 0 ||
		lookup_var->varattno <= 0 ||
		lookup_var->varattno > kMaxDeformTargets ||
		!input_state->lookup_output_col_meta(lookup_var->varattno, &spec->input_key_meta) ||
		!IsLookupCompatibleStorage(spec->input_key_meta.storage_kind))
		return false;

	subplan_plan = LookupReferencedSubPlan(estate, subplan);
	if (subplan_plan == nullptr)
		return false;
	if (subplan_plan->targetlist != NIL)
		CollectAttrNosFromExpr((Node *) subplan_plan->targetlist, &lookup_required_attrs);
	if (subplan_plan->qual != NIL)
		CollectAttrNosFromExpr((Node *) subplan_plan->qual, &lookup_required_attrs);
	lookup_state = ExecInitVecPlanInternal(subplan_plan,
										   estate,
										   lookup_required_attrs,
										   false);
	if (!lookup_state ||
		!lookup_state->lookup_output_col_meta(1, &spec->lookup_key_meta) ||
		!IsLookupCompatibleStorage(spec->lookup_key_meta.storage_kind))
		return false;

	spec->lookup_state = std::move(lookup_state);
	spec->input_key_col = (uint16_t) (lookup_var->varattno - 1);
	spec->lookup_key_col = 0;
	spec->negate = negate;
	if (pg_volvec_trace_hooks)
		elog(LOG,
			 "pg_volvec: matched lookup membership qual (input_col=%u, negate=%d)",
			 spec->input_key_col,
			 spec->negate ? 1 : 0);
	return true;
}

static bool
TryBuildLookupMembershipFilterSpec(Expr *expr,
								   VecPlanState *input_state,
								   EState *estate,
								   LookupMembershipFilterSpec *spec)
{
	List *qual_list = NIL;
	List *residual_list = NIL;
	ListCell *lc;
	bool matched = false;

	if (expr == nullptr || input_state == nullptr || estate == nullptr || spec == nullptr)
		return false;

	*spec = LookupMembershipFilterSpec{};
	expr = StripImplicitNodesLocal(expr);
	if (expr != nullptr && IsA(expr, BoolExpr) &&
		((BoolExpr *) expr)->boolop == AND_EXPR)
		qual_list = list_copy(((BoolExpr *) expr)->args);
	else
		qual_list = list_make1(expr);

	foreach(lc, qual_list)
	{
		Expr *qual = (Expr *) lfirst(lc);
		LookupMembershipFilterSpec candidate;

		if (!matched && MatchLookupMembershipQual(qual, input_state, estate, &candidate))
		{
			*spec = std::move(candidate);
			matched = true;
			continue;
		}
		residual_list = lappend(residual_list, qual);
	}

	if (!matched)
		return false;

	if (residual_list == NIL)
		spec->residual_expr = nullptr;
	else if (list_length(residual_list) == 1)
		spec->residual_expr = (Expr *) linitial(residual_list);
	else
		spec->residual_expr = (Expr *) make_ands_explicit(residual_list);
	return true;
}

static bool
MatchPlanCorrelatedLookupQual(Expr *expr,
							  VecPlanState *input_state,
							  EState *estate,
							  CorrelatedLookupProjectSpec *spec)
{
	Expr *stripped = StripImplicitNodesLocal(expr);
	OpExpr *compare_op;
	Expr *left_arg;
	Expr *right_arg;
	Expr *compare_expr = nullptr;
	Var *compare_var = nullptr;
	SubPlan *subplan = nullptr;
	Plan *subplan_plan;
	Agg *subplan_agg;
	VecOutputColMeta lookup_key_metas[kMaxLookupKeys];
	VecOutputColMeta value_meta;
	std::unique_ptr<VecPlanState> lookup_state;
	OpExpr *rewritten_op;
	List *rewritten_args = NIL;
	int num_keys = 0;
	ListCell *lc_arg;
	int arg_index = 0;

	if (expr == nullptr || input_state == nullptr || estate == nullptr || spec == nullptr)
		return false;
	if (stripped == nullptr || !IsA(stripped, OpExpr))
		return false;
	compare_op = (OpExpr *) stripped;
	if (list_length(compare_op->args) != 2)
		return false;

	left_arg = StripImplicitNodesLocal((Expr *) linitial(compare_op->args));
	right_arg = StripImplicitNodesLocal((Expr *) lsecond(compare_op->args));
	if (left_arg != nullptr && right_arg != nullptr &&
		!IsA(left_arg, SubPlan) && IsA(right_arg, SubPlan) &&
		ExtractComparableLookupVar(left_arg, &compare_var))
	{
		compare_expr = left_arg;
		subplan = (SubPlan *) right_arg;
	}
	else if (left_arg != nullptr && right_arg != nullptr &&
			 IsA(left_arg, SubPlan) && !IsA(right_arg, SubPlan) &&
			 ExtractComparableLookupVar(right_arg, &compare_var))
	{
		compare_expr = right_arg;
		subplan = (SubPlan *) left_arg;
	}
	else
		return false;

	if (subplan == nullptr ||
		subplan->subLinkType != EXPR_SUBLINK ||
		subplan->useHashTable ||
		subplan->parParam == NIL ||
		subplan->args == NIL ||
		list_length(subplan->parParam) != list_length(subplan->args) ||
		list_length(subplan->parParam) <= 0 ||
		list_length(subplan->parParam) > kMaxLookupKeys)
		return false;

	subplan_plan = LookupReferencedSubPlan(estate, subplan);
	if (subplan_plan == nullptr || !IsA(subplan_plan, Agg))
		return false;
	subplan_agg = (Agg *) subplan_plan;
	lookup_state = BuildCorrelatedLookupAggStateMulti(estate,
													  subplan_agg,
													  subplan->parParam,
													  subplan->args,
													  &num_keys,
													  lookup_key_metas,
													  &value_meta);
	if (!lookup_state)
		return false;

	spec->num_keys = num_keys;
	foreach(lc_arg, subplan->args)
	{
		Expr *arg_expr = StripImplicitNodesLocal((Expr *) lfirst(lc_arg));
		Var *arg_var = nullptr;
		VecOutputColMeta input_key_meta;

		if (arg_expr == nullptr || !IsA(arg_expr, Var))
			return false;
		arg_var = (Var *) arg_expr;
		if (arg_var->varattno <= 0 || arg_var->varattno > 16 ||
			!input_state->lookup_output_col_meta(arg_var->varattno, &input_key_meta) ||
			!IsLookupCompatibleStorage(input_key_meta.storage_kind) ||
			!IsLookupCompatibleStorage(lookup_key_metas[arg_index].storage_kind))
			return false;

		spec->input_key_cols[arg_index] = (uint16_t) (arg_var->varattno - 1);
		spec->input_key_metas[arg_index] = input_key_meta;
		spec->lookup_key_cols[arg_index] = (uint16_t) arg_index;
		spec->lookup_key_metas[arg_index] = lookup_key_metas[arg_index];
		arg_index++;
	}

	if (value_meta.storage_kind != VecOutputStorageKind::Int32 &&
		value_meta.storage_kind != VecOutputStorageKind::Int64 &&
		value_meta.storage_kind != VecOutputStorageKind::NumericScaledInt64 &&
		value_meta.storage_kind != VecOutputStorageKind::Double)
		return false;

	spec->lookup_state = std::move(lookup_state);
	spec->lookup_value_col = (uint16_t) num_keys;
	spec->output_resno = FindNextFreeOutputResno(input_state);
	spec->output_meta = value_meta;
	if (spec->output_resno <= 0)
		return false;

	rewritten_op = (OpExpr *) copyObjectImpl(compare_op);
	if (compare_expr == left_arg)
	{
		rewritten_args = list_make2((Node *) copyObjectImpl(compare_var),
									makeVar(1,
											spec->output_resno,
											value_meta.sql_type,
											-1,
											InvalidOid,
											0));
	}
	else
	{
		rewritten_args = list_make2(makeVar(1,
											spec->output_resno,
											value_meta.sql_type,
											-1,
											InvalidOid,
											0),
									(Node *) copyObjectImpl(compare_var));
	}
	rewritten_op->args = rewritten_args;
	spec->rewritten_expr = (Expr *) rewritten_op;
	return true;
}

static bool
TryBuildPlanCorrelatedLookupProjectSpec(Expr *expr,
										VecPlanState *input_state,
										EState *estate,
										CorrelatedLookupProjectSpec *spec)
{
	List *qual_list = NIL;
	List *rewritten_quals = NIL;
	ListCell *lc;
	bool matched = false;

	if (expr == nullptr || input_state == nullptr || estate == nullptr || spec == nullptr)
		return false;

	*spec = CorrelatedLookupProjectSpec{};
	expr = StripImplicitNodesLocal(expr);
	if (expr != nullptr && IsA(expr, BoolExpr) &&
		((BoolExpr *) expr)->boolop == AND_EXPR)
		qual_list = list_copy(((BoolExpr *) expr)->args);
	else
		qual_list = list_make1(expr);

	foreach(lc, qual_list)
	{
		Expr *qual = (Expr *) lfirst(lc);
		CorrelatedLookupProjectSpec candidate;

		if (!matched && MatchPlanCorrelatedLookupQual(qual, input_state, estate, &candidate))
		{
			*spec = std::move(candidate);
			rewritten_quals = lappend(rewritten_quals, spec->rewritten_expr);
			matched = true;
			continue;
		}
		rewritten_quals = lappend(rewritten_quals, qual);
	}

	if (!matched)
		return false;

	if (rewritten_quals == NIL)
		spec->rewritten_expr = nullptr;
	else if (list_length(rewritten_quals) == 1)
		spec->rewritten_expr = (Expr *) linitial(rewritten_quals);
	else
		spec->rewritten_expr = (Expr *) make_ands_explicit(rewritten_quals);
	return true;
}

static bool
TryBuildCorrelatedLookupFilterSpec(Expr *expr,
								   Plan *outer_plan,
								   Plan *inner_plan,
								   VecPlanState *outer,
								   VecPlanState *inner,
								   VolVecVector<VecJoinOutputCol> *output_cols,
								   EState *estate,
								   CorrelatedLookupFilterSpec *spec)
{
	Expr *stripped = StripImplicitNodesLocal(expr);
	OpExpr *compare_op;
	Expr *left_arg;
	Expr *right_arg;
	Var *compare_var = nullptr;
	SubPlan *subplan = nullptr;
	Var *corr_arg_var;
	int compare_resno;
	int key_resno;
	VecJoinSide key_side;
	VecOutputColMeta compare_meta;
	VecOutputColMeta key_meta;
	Plan *subplan_plan;
	Agg *subplan_agg;
	VecOutputColMeta lookup_key_meta;
	VecOutputColMeta lookup_value_meta;
	std::unique_ptr<VecPlanState> lookup_state;
	OpExpr *rewritten_op;
	List *rewritten_args;
	Var *compare_input_var;
	Var *lookup_var;

	if (spec == nullptr || output_cols == nullptr || estate == nullptr)
		return false;
	if (stripped == nullptr || !IsA(stripped, OpExpr))
		return false;
	compare_op = (OpExpr *) stripped;
	if (list_length(compare_op->args) != 2)
		return false;

	left_arg = StripImplicitNodesLocal((Expr *) linitial(compare_op->args));
	right_arg = StripImplicitNodesLocal((Expr *) lsecond(compare_op->args));
	if (left_arg != nullptr && right_arg != nullptr &&
		IsA(left_arg, Var) && IsA(right_arg, SubPlan))
	{
		compare_var = (Var *) left_arg;
		subplan = (SubPlan *) right_arg;
	}
	else if (left_arg != nullptr && right_arg != nullptr &&
			 IsA(left_arg, SubPlan) && IsA(right_arg, Var))
	{
		subplan = (SubPlan *) left_arg;
		compare_var = (Var *) right_arg;
	}
	else
		return false;

	if (subplan->subLinkType != EXPR_SUBLINK ||
		subplan->useHashTable ||
		subplan->parParam == NIL ||
		list_length(subplan->parParam) != 1 ||
		subplan->args == NIL ||
		list_length(subplan->args) != 1)
		return false;

	corr_arg_var = (Var *) StripImplicitNodesLocal((Expr *) linitial(subplan->args));
	if (corr_arg_var == nullptr || !IsA(corr_arg_var, Var))
		return false;

	/* The compare input itself must be materialized by the base join. */
	{
		VecJoinSide compare_side;
		uint16_t compare_source_col = 0;

		if (!ResolveHashJoinVarBinding(compare_var,
									   outer_plan,
									   inner_plan,
									   outer,
									   inner,
									   &compare_side,
									   &compare_source_col,
									   &compare_meta) ||
			!EnsureHashJoinOutputCol(compare_side,
									 compare_source_col,
									 compare_meta,
									 output_cols,
									 &compare_resno))
			return false;
	}

	if (!ResolveHashJoinVarBinding(corr_arg_var,
								   outer_plan,
								   inner_plan,
								   outer,
								   inner,
								   &key_side,
								   &spec->input_key_col,
								   &key_meta) ||
		!EnsureHashJoinOutputCol(key_side,
								 spec->input_key_col,
								 key_meta,
								 output_cols,
								 &key_resno))
		return false;

	subplan_plan = LookupReferencedSubPlan(estate, subplan);
	if (subplan_plan == nullptr || !IsA(subplan_plan, Agg))
		return false;
	subplan_agg = (Agg *) subplan_plan;
	if (subplan_agg->numCols != 0 || subplan_agg->plan.lefttree == nullptr ||
		subplan_agg->plan.targetlist == NIL ||
		list_length(subplan_agg->plan.targetlist) != 1)
		return false;

	lookup_state = BuildCorrelatedLookupAggState(estate,
												 subplan_agg,
												 linitial_int(subplan->parParam),
												 &lookup_key_meta,
												 &lookup_value_meta);
	if (!lookup_state)
		return false;
	if (!IsLookupCompatibleStorage(key_meta.storage_kind) ||
		!IsLookupCompatibleStorage(lookup_key_meta.storage_kind))
		return false;
	if (lookup_value_meta.storage_kind != VecOutputStorageKind::Int32 &&
		lookup_value_meta.storage_kind != VecOutputStorageKind::Int64 &&
		lookup_value_meta.storage_kind != VecOutputStorageKind::NumericScaledInt64 &&
		lookup_value_meta.storage_kind != VecOutputStorageKind::Double)
		return false;
	if ((int) output_cols->size() >= 16)
		return false;

	spec->lookup_state = std::move(lookup_state);
	spec->input_key_col = (uint16_t) (key_resno - 1);
	spec->input_key_meta = key_meta;
	spec->lookup_key_col = 0;
	spec->lookup_key_meta = lookup_key_meta;
	spec->lookup_value_col = 1;
	spec->output_resno = (int) output_cols->size() + 1;
	spec->output_meta = lookup_value_meta;

	compare_input_var = makeVar(1,
								compare_resno,
								compare_var->vartype,
								compare_var->vartypmod,
								compare_var->varcollid,
								0);
	lookup_var = makeVar(1,
						 spec->output_resno,
						 subplan->firstColType,
						 subplan->firstColTypmod,
						 subplan->firstColCollation,
						 0);
	rewritten_args = NIL;
	if ((Node *) left_arg == (Node *) compare_var)
		rewritten_args = list_make2(compare_input_var, lookup_var);
	else
		rewritten_args = list_make2(lookup_var, compare_input_var);

	rewritten_op = (OpExpr *) copyObjectImpl(compare_op);
	rewritten_op->args = rewritten_args;
	spec->rewritten_expr = (Expr *) rewritten_op;
	return true;
}

struct HashJoinFilterRewriteContext
{
	Plan *outer_plan;
	Plan *inner_plan;
	VecPlanState *outer;
	VecPlanState *inner;
	VolVecVector<VecJoinOutputCol> *output_cols;
	bool failed;
};

static Node *
RewriteHashJoinFilterVarsMutator(Node *node, HashJoinFilterRewriteContext *context)
{
	if (node == nullptr)
		return nullptr;

	if (IsA(node, Var))
	{
		Var *var = (Var *) node;
		VecJoinSide side;
		VecOutputColMeta meta;
		uint16_t source_col = 0;
		int join_resno = 0;
		Var *rewritten;

		if (!ResolveHashJoinVarBinding(var,
										 context->outer_plan,
										 context->inner_plan,
										 context->outer,
										 context->inner,
										 &side,
										 &source_col,
										 &meta) ||
			!EnsureHashJoinOutputCol(side, source_col, meta,
									 context->output_cols, &join_resno))
		{
			context->failed = true;
			return nullptr;
		}

		rewritten = makeVar(1,
							join_resno,
							var->vartype,
							var->vartypmod,
							var->varcollid,
							var->varlevelsup);
		rewritten->location = var->location;
		return (Node *) rewritten;
	}

	return expression_tree_mutator(node,
								   (Node *(*)(Node *, void *)) RewriteHashJoinFilterVarsMutator,
								   context);
}

static Expr *
RewriteHashJoinFilterExpr(Expr *expr,
						   Plan *outer_plan,
						   Plan *inner_plan,
						   VecPlanState *outer,
						   VecPlanState *inner,
						   VolVecVector<VecJoinOutputCol> *output_cols)
{
	HashJoinFilterRewriteContext context;

	if (expr == nullptr)
		return nullptr;

	context.outer_plan = outer_plan;
	context.inner_plan = inner_plan;
	context.outer = outer;
	context.inner = inner;
	context.output_cols = output_cols;
	context.failed = false;

	Expr *rewritten = (Expr *) RewriteHashJoinFilterVarsMutator((Node *) expr, &context);
	if (context.failed)
		return nullptr;
	return rewritten;
}

static bool
BuildJoinOutputCols(List *targetlist,
					Plan *outer_plan,
					Plan *inner_plan,
					VecPlanState *outer,
					VecPlanState *inner,
					VolVecVector<VecJoinOutputCol> *output_cols,
					bool *needs_project)
{
	ListCell *lc;

	if (needs_project != nullptr)
		*needs_project = false;
	foreach(lc, targetlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		Expr *expr = StripImplicitNodesLocal((Expr *) tle->expr);
		if (expr != nullptr && IsA(expr, Var))
		{
			Var *var = (Var *) expr;
			VecJoinSide side;
			VecOutputColMeta meta;
			uint16_t source_col = 0;
			int join_resno = 0;

			if (!ResolveHashJoinVarBinding(var,
										   outer_plan,
										   inner_plan,
										   outer,
										   inner,
										   &side,
										   &source_col,
										   &meta) ||
				!EnsureHashJoinOutputCol(side, source_col, meta, output_cols, &join_resno))
			{
				if (pg_volvec_trace_hooks)
					elog(LOG, "pg_volvec: hash join output var binding failed for target resno %d",
						 tle->resno);
				return false;
			}
			if (needs_project != nullptr && join_resno != tle->resno)
				*needs_project = true;
			continue;
		}

		if (RewriteHashJoinFilterExpr((Expr *) copyObjectImpl(tle->expr),
									  outer_plan,
									  inner_plan,
									  outer,
									  inner,
									  output_cols) == nullptr)
		{
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: hash join output rewrite failed for target resno %d",
					 tle->resno);
			return false;
		}
		if (needs_project != nullptr)
			*needs_project = true;
	}

	return true;
}

static std::unique_ptr<VecPlanState>
BuildJoinProject(std::unique_ptr<VecPlanState> left,
				 List *targetlist,
				 Plan *outer_plan,
				 Plan *inner_plan,
				 VecPlanState *outer,
				 VecPlanState *inner,
				 VolVecVector<VecJoinOutputCol> *output_cols,
				 EState *estate)
{
	VolVecVector<VecProjectColDesc> project_cols{PgMemoryContextAllocator<VecProjectColDesc>(CurrentMemoryContext)};
	ListCell *lc;

	if (!left || output_cols == nullptr)
		return nullptr;

	foreach(lc, targetlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		VecProjectColDesc project_col;
		Expr *rewritten_expr =
			RewriteHashJoinFilterExpr((Expr *) copyObjectImpl(tle->expr),
									  outer_plan,
									  inner_plan,
									  outer,
									  inner,
									  output_cols);
		Expr *stripped_expr;

		if (rewritten_expr == nullptr)
		{
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: hash join project rewrite failed for target resno %d",
					 tle->resno);
			return nullptr;
		}

		project_col.target_resno = tle->resno;
		project_col.sql_type = exprType((Node *) tle->expr);
		stripped_expr = StripImplicitNodesLocal(rewritten_expr);
		if (stripped_expr != nullptr && IsA(stripped_expr, Var))
		{
			Var *var = (Var *) stripped_expr;
			VecOutputColMeta meta;

			if (var->varattno <= 0 || var->varattno > 16 ||
				!left->lookup_output_col_meta(var->varattno, &meta))
			{
				if (pg_volvec_trace_hooks)
					elog(LOG, "pg_volvec: hash join project direct-var metadata lookup failed for target resno %d",
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
					elog(LOG, "pg_volvec: hash join string-prefix project metadata lookup failed for target resno %d",
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
			AdjustProgramVarScales(project_col.expr.get(), left.get());
			if (project_col.expr->get_final_res_idx() < 0)
			{
				if (pg_volvec_trace_hooks)
					elog(LOG, "pg_volvec: hash join project expression compilation failed for target resno %d",
						 tle->resno);
				return nullptr;
			}
			project_col.storage_kind = InferProjectStorageKind((Expr *) tle->expr, project_col.expr.get());
			project_col.scale = project_col.expr->get_register_scale(project_col.expr->get_final_res_idx());
		}
		project_cols.push_back(std::move(project_col));
	}

	return std::make_unique<VecProjectState>(std::move(left), std::move(project_cols));
}

static bool
IsSimpleJoinKeyClause(Node *node)
{
	OpExpr *op;
	Expr *left_expr;
	Expr *right_expr;

	if (node == nullptr || !IsA(node, OpExpr))
		return false;
	op = (OpExpr *) node;
	if (list_length(op->args) != 2)
		return false;

	left_expr = StripImplicitNodesLocal((Expr *) linitial(op->args));
	right_expr = StripImplicitNodesLocal((Expr *) lsecond(op->args));
	if (left_expr == nullptr || right_expr == nullptr ||
		!IsA(left_expr, Var) || !IsA(right_expr, Var))
		return false;
	if ((((Var *) left_expr)->varno == OUTER_VAR && ((Var *) right_expr)->varno == INNER_VAR) ||
		(((Var *) left_expr)->varno == INNER_VAR && ((Var *) right_expr)->varno == OUTER_VAR))
		return true;
	return false;
}

static void
PartitionJoinClauses(List *clauses, List **key_clauses, List **residual_clauses)
{
	ListCell *lc;

	if (key_clauses != nullptr)
		*key_clauses = NIL;
	if (residual_clauses != nullptr)
		*residual_clauses = NIL;

	foreach(lc, clauses)
	{
		Node *clause = (Node *) lfirst(lc);

		if (IsSimpleJoinKeyClause(clause))
		{
			if (key_clauses != nullptr)
				*key_clauses = lappend(*key_clauses, clause);
		}
		else
		{
			if (residual_clauses != nullptr)
				*residual_clauses = lappend(*residual_clauses, clause);
		}
	}
}

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

static std::unique_ptr<VecPlanState>
ExecInitVecPlanInternal(Plan *plan, EState *estate, Bitmapset *required_attrs,
						bool force_full_deform)
{
	if (plan == NULL) return nullptr;
	if (estate != nullptr && plan->extParam != NULL)
		ExecSetParamPlanMulti(plan->extParam, GetPerTupleExprContext(estate));
	if (required_attrs == nullptr && !force_full_deform)
		CollectRequiredAttrsForPlan(plan, &required_attrs);
	std::unique_ptr<VecPlanState> current_state = nullptr;
	bool plan_qual_already_applied = false;
	if (IsA(plan, Limit)) {
		uint64_t limit_count = 0;
		auto left = ExecInitVecPlanInternal(plan->lefttree, estate, required_attrs, force_full_deform);
		if (!left)
		{
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: limit initialization could not build child state");
			return nullptr;
		}
		if (!ExtractLimitCount((Limit *) plan, &limit_count))
		{
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: limit count/offset shape is not supported");
			return nullptr;
		}
		current_state = std::make_unique<VecLimitState>(std::move(left), limit_count);
	} else if (IsA(plan, Sort)) {
		VolVecVector<VecSortKeyDesc> key_descs{PgMemoryContextAllocator<VecSortKeyDesc>(CurrentMemoryContext)};
		auto left = ExecInitVecPlanInternal(plan->lefttree, estate, required_attrs, force_full_deform);
		if (!left)
		{
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: sort initialization could not build child state");
			return nullptr;
		}
		if (!BuildSortKeyDescs((Sort *) plan, left.get(), &key_descs))
			return nullptr;
		current_state = std::make_unique<VecSortState>(std::move(left), (Sort *) plan, std::move(key_descs));
	} else if (IsA(plan, Agg)) {
		Agg *agg_node = (Agg *) plan;
		auto left = ExecInitVecPlanInternal(plan->lefttree, estate, required_attrs, force_full_deform);
		if (!left) {
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: aggregate initialization could not build child state");
			return nullptr;
		}
		current_state = BuildAggWithOptionalProject(std::move(left), agg_node, estate);
		if (!current_state)
		{
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: aggregate state/project initialization failed");
			return nullptr;
		}
		if (agg_node->plan.qual != NIL)
			plan_qual_already_applied = true;
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
		auto left = ExecInitVecPlanInternal(subquery_scan->subplan, estate, nullptr, force_full_deform);

		if (!left)
		{
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: subquery scan initialization could not build subplan state");
			return nullptr;
		}
		current_state = BuildDirectVarProject(std::move(left), plan->targetlist);
		if (!current_state)
		{
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: subquery scan targetlist projection is not supported");
			return nullptr;
		}
	} else if (IsA(plan, Material)) {
		auto left = ExecInitVecPlanInternal(plan->lefttree, estate, required_attrs, force_full_deform);

		if (!left)
		{
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: materialize initialization could not build child state");
			return nullptr;
		}
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
		outer = ExecInitVecPlanInternal(plan->lefttree, estate, outer_required_attrs, false);
		inner = ExecInitVecPlanInternal(plan->righttree, estate, inner_required_attrs, false);
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
			outer = ExecInitVecPlanInternal(outer_plan, estate, outer_required_attrs, false);
			inner = ExecInitVecPlanInternal(inner_plan, estate, inner_required_attrs, false);
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
												   &lookup_filter_spec);
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
		outer = ExecInitVecPlanInternal(plan->lefttree, estate, outer_required_attrs, false);
		inner = ExecInitVecPlanInternal(plan->righttree, estate, inner_required_attrs, false);
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
		DeformProgram prog;
		TupleDesc desc = RelationGetDescr(rel);
		BuildPrunedDeformProgram(required_attrs, desc, &prog);
			current_state = std::make_unique<VecSeqScanState>(rel, estate->es_snapshot, &prog);
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
ExecInitVecPlan(Plan *plan, EState *estate)
{
	return ExecInitVecPlanInternal(plan, estate, nullptr, false);
}

} /* namespace pg_volvec */

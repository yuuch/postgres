#include "exec/internal.hpp"

namespace pg_volvec {

bool
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

Expr *
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

bool
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

Expr *
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

bool
ShouldUseExactNumericAgg(Oid arg_type)
{
	return arg_type == NUMERICOID;
}

VecOutputStorageKind
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

uint64_t
EncodeFloat8SortKey(double value)
{
	uint64_t bits;

	memcpy(&bits, &value, sizeof(bits));
	if ((bits & (UINT64CONST(1) << 63)) != 0)
		return ~bits;
	return bits ^ (UINT64CONST(1) << 63);
}

uint32_t
TrimBpcharLengthLocal(const char *data, uint32_t len)
{
	while (len > 0 && data[len - 1] == ' ')
		len--;
	return len;
}

uint64_t
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

uint64_t
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

uint64_t
HashStringBytesForGroupKey(const char *ptr, uint32_t len, Oid sql_type, uint32_t *len_out)
{
	if (sql_type == BPCHAROID)
		len = TrimBpcharLengthLocal(ptr, len);
	if (len_out != nullptr)
		*len_out = len;
	return HashBytes64(ptr, len);
}

bool
BufFileWriteAllLocal(BufFile *file, const void *ptr, size_t size)
{
	if (file == nullptr)
		return false;
	BufFileWrite(file, ptr, size);
	return true;
}

bool
BufFileReadAllLocal(BufFile *file, void *ptr, size_t size, bool eof_ok, bool *eof_reached)
{
	size_t nread;

	if (eof_reached != nullptr)
		*eof_reached = false;
	if (file == nullptr)
		return false;
	nread = BufFileReadMaybeEOF(file, ptr, size, eof_ok);
	if (eof_ok && nread == 0)
	{
		if (eof_reached != nullptr)
			*eof_reached = true;
		return true;
	}
	return nread == size;
}

VecStringRef
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

bool
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

void
AdjustProgramVarScales(VecExprProgram *program, VecPlanState *input_state)
{
	bool changed = false;
	const int avg_pair_extra_scale = 6;

	if (program == nullptr || input_state == nullptr)
		return;

	for (auto &step : program->steps)
	{
		VecOutputColMeta meta;
		VecOutputStorageKind old_storage;
		int old_storage_scale;

		if (step.opcode != VecOpCode::EEOP_VAR)
			continue;
		if (step.d.var.att_idx < 0 || step.d.var.att_idx >= 16)
			continue;
		if (!input_state->lookup_output_col_meta(step.d.var.att_idx + 1, &meta))
			continue;
		old_storage = step.d.var.storage_kind;
		old_storage_scale = step.d.var.storage_scale;
		step.d.var.storage_kind = meta.storage_kind;
		step.d.var.storage_scale = meta.scale;
		if (old_storage != step.d.var.storage_kind ||
			old_storage_scale != step.d.var.storage_scale)
			changed = true;
		if (meta.storage_kind == VecOutputStorageKind::NumericAvgPair)
		{
			int avg_scale = Min(meta.scale + avg_pair_extra_scale, 18);

			if (program->get_register_scale(step.res_idx) != avg_scale)
			{
				program->set_register_scale(step.res_idx, avg_scale);
				changed = true;
			}
		}
		else if (meta.storage_kind == VecOutputStorageKind::NumericScaledInt64)
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

void
CollectAttrNosFromExpr(Node *node, Bitmapset **attrs)
{
	if (node != nullptr)
		(void) CollectAttrNosFromExprWalker(node, attrs);
}

void
CollectRequiredAttrsForPlan(Plan *plan, Bitmapset **attrs)
{
	if (plan == nullptr)
		return;

	if (plan->qual != NIL)
		CollectAttrNosFromExpr((Node *) plan->qual, attrs);

	/*
	 * Base scan targetlists are often the full physical tuple; collecting from
	 * them defeats pruning.  Instead, collect required Vars from upper plan
	 * nodes and scan quals only, unless the SeqScan targetlist is already a
	 * projected direct-var subset that downstream operators rely on.
	 */
	if (plan->targetlist != NIL)
	{
		bool collect_targetlist = !IsA(plan, SeqScan);

		if (!collect_targetlist)
		{
			ListCell *lc;
			bool saw_visible = false;
			bool seqscan_identity = true;

			foreach(lc, plan->targetlist)
			{
				TargetEntry *tle = (TargetEntry *) lfirst(lc);
				Expr *expr;
				Var *var;

				if (tle->resjunk)
					continue;
				saw_visible = true;
				expr = StripImplicitNodesLocal((Expr *) tle->expr);
				if (expr == nullptr || !IsA(expr, Var))
				{
					seqscan_identity = false;
					break;
				}
				var = (Var *) expr;
				if (tle->resno != var->varattno)
				{
					seqscan_identity = false;
					break;
				}
			}
			collect_targetlist = saw_visible && !seqscan_identity;
		}

		if (collect_targetlist)
			CollectAttrNosFromExpr((Node *) plan->targetlist, attrs);
	}

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

void
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

Expr *
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

int
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

bool
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

bool
IsProcessParallelLocalContext(const ParallelWorkerContext *parallel_worker_context)
{
	return parallel_worker_context != nullptr &&
		parallel_worker_context->agg_plan_node_id >= 0;
}

bool
ShouldSuppressPartialAggQual(const ParallelWorkerContext *parallel_worker_context,
							 Agg *agg)
{
	return IsProcessParallelLocalContext(parallel_worker_context) &&
		agg != nullptr &&
		agg->plan.plan_node_id == parallel_worker_context->agg_plan_node_id;
}

bool
HasProcessParallelTargetAggSubtree(const ParallelWorkerContext *parallel_worker_context,
								   VecPlanState *state)
{
	return IsProcessParallelLocalContext(parallel_worker_context) &&
		state != nullptr &&
		state->find_parallel_aggregate_state_by_plan_node_id(
			parallel_worker_context->agg_plan_node_id) != nullptr;
}

bool
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

Oid
FindPlanBaseRelid(Plan *plan, EState *estate)
{
	if (plan == nullptr || estate == nullptr)
		return InvalidOid;
	if (IsA(plan, SeqScan))
	{
		SeqScan *sscan = (SeqScan *) plan;

		return exec_rt_fetch(sscan->scan.scanrelid, estate)->relid;
	}
	if (IsA(plan, Hash))
		return FindPlanBaseRelid(((Hash *) plan)->plan.lefttree, estate);
	if (IsA(plan, Material))
		return FindPlanBaseRelid(plan->lefttree, estate);
	if (IsA(plan, Sort))
		return FindPlanBaseRelid(plan->lefttree, estate);
	if (IsA(plan, Limit))
		return FindPlanBaseRelid(plan->lefttree, estate);
	if (IsA(plan, Agg))
		return FindPlanBaseRelid(plan->lefttree, estate);
	if (IsA(plan, SubqueryScan))
		return FindPlanBaseRelid(((SubqueryScan *) plan)->subplan, estate);
	return InvalidOid;
}

bool
PlanContainsNodeId(Plan *plan, int target_plan_node_id)
{
	if (plan == nullptr || target_plan_node_id < 0)
		return false;
	if (plan->plan_node_id == target_plan_node_id)
		return true;
	if (IsA(plan, Hash) &&
		PlanContainsNodeId(((Hash *) plan)->plan.lefttree, target_plan_node_id))
		return true;
	if (IsA(plan, SubqueryScan) &&
		PlanContainsNodeId(((SubqueryScan *) plan)->subplan, target_plan_node_id))
		return true;
	return PlanContainsNodeId(plan->lefttree, target_plan_node_id) ||
		PlanContainsNodeId(plan->righttree, target_plan_node_id);
}

bool
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

void
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

bool
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

} /* namespace pg_volvec */

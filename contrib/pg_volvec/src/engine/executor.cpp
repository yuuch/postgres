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
		else
			break;
	}

	return expr;
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

static inline VecStringRef
CopyStringRefToChunk(DataChunk<DEFAULT_CHUNK_SIZE> &dst,
					 const DataChunk<DEFAULT_CHUNK_SIZE> &src,
					 const VecStringRef &ref)
{
	return dst.store_string_bytes(src.get_string_ptr(ref), ref.len);
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
					(IsA(plan, Hash) || IsA(plan, Sort) || IsA(plan, Limit)))
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
			if (aggref->args != NIL) {
				TargetEntry *arg_tle = (TargetEntry *) linitial(aggref->args);
				desc.arg_type = exprType((Node *) arg_tle->expr);
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
	while (left_->get_next_batch(*batch)) {
		for (auto &agg : aggs_) if (agg.arg_expr) agg.arg_expr->evaluate(*batch);
		int n = batch->has_selection ? batch->sel.count : batch->count;
			for (int s = 0; s < n; s++) {
				int i = batch->has_selection ? batch->sel.row_ids[s] : s;
				VecGroupKey key;

				key.num_cols = (int) grp_col_indices_.size();
				if (key.num_cols > 4)
					key.num_cols = 4;
				for (int k = 0; k < 4; k++)
				{
					key.values[k] = 0;
					key.aux[k] = 0;
					key.is_null[k] = 0;
				}
				for (int k = 0; k < key.num_cols; k++) {
					int idx = grp_col_indices_[k];
					const VecOutputColMeta &meta = grp_col_meta_[k];

					if (idx < 0 || idx >= 16)
						continue;
					key.is_null[k] = batch->nulls[idx][i];
					if (key.is_null[k])
						continue;
					switch (meta.storage_kind)
					{
						case VecOutputStorageKind::StringRef:
							key.values[k] = batch->string_columns[idx][i].prefix;
							key.aux[k] = batch->string_columns[idx][i].len;
							break;
						case VecOutputStorageKind::Int64:
						case VecOutputStorageKind::NumericScaledInt64:
						case VecOutputStorageKind::NumericAvgPair:
							key.values[k] = (uint64_t) batch->int64_columns[idx][i];
							break;
						case VecOutputStorageKind::Double:
							memcpy(&key.values[k], &batch->double_columns[idx][i], sizeof(uint64_t));
							break;
						case VecOutputStorageKind::Int32:
						default:
							key.values[k] = (uint64_t) (uint32_t) batch->int32_columns[idx][i];
							break;
					}
				}
				auto it = hash_table_.find(key);
				if (it == hash_table_.end())
				{
					VecAggGroupState new_group;
					DataChunk<DEFAULT_CHUNK_SIZE> *rep_chunk =
						rep_chunks_.empty() ? allocate_rep_chunk() : rep_chunks_.back();

					new_group.accs = VecAggAccumulatorList(PgMemoryContextAllocator<VecAggAccumulator>(memory_context_));
					if (rep_chunk->count >= DEFAULT_CHUNK_SIZE)
						rep_chunk = allocate_rep_chunk();
					new_group.rep_chunk_idx = (uint32_t) (rep_chunks_.size() - 1);
					new_group.rep_row_idx = (uint16_t) rep_chunk->count;
					new_group.has_rep_row = true;
					copy_rep_row(*rep_chunk, rep_chunk->count, *batch, i);
					rep_chunk->count++;
					it = hash_table_.emplace(key, std::move(new_group)).first;
				}
				auto& accs = it->second.accs; if (accs.empty()) accs.resize(aggs_.size());
				for (size_t a = 0; a < aggs_.size(); a++) {
					if (aggs_[a].type == VecAggType::COUNT) accs[a].count++;
					else if (aggs_[a].arg_expr) {
						int r = aggs_[a].arg_expr->final_res_idx;
						if (r >= 0 && !aggs_[a].arg_expr->get_nulls_reg(r)[i]) {
							if (aggs_[a].type == VecAggType::MAX) {
								if (aggs_[a].use_exact_numeric ||
									aggs_[a].output_storage == VecOutputStorageKind::Int64 ||
									aggs_[a].output_storage == VecOutputStorageKind::NumericScaledInt64 ||
									aggs_[a].output_storage == VecOutputStorageKind::NumericAvgPair) {
									const int64_t *r64 = aggs_[a].arg_expr->get_int64_reg(r);
									accs[a].update_max_int64(r64[i]);
								} else if (aggs_[a].output_storage == VecOutputStorageKind::Double) {
									const double *rf8 = aggs_[a].arg_expr->get_float8_reg(r);
									accs[a].update_max_float(rf8[i]);
								} else {
									const int32_t *r32 = aggs_[a].arg_expr->get_int32_reg(r);
									accs[a].update_max_int32(r32[i]);
								}
							} else if (aggs_[a].use_exact_numeric) {
								const int64_t* r64 = aggs_[a].arg_expr->get_int64_reg(r);
								accs[a].update_numeric(r64[i]);
							} else {
								double v;
								const int64_t* r64 = aggs_[a].arg_expr->get_int64_reg(r);
								const double* rf8 = aggs_[a].arg_expr->get_float8_reg(r);
								if (r64[i] != 0 || (rf8[i] == 0.0))
									v = (double)r64[i];
								else
									v = rf8[i];
								accs[a].update_float(v);
							}
						}
					}
				}
			}
		}
		fully_scanned_ = true; it_ = hash_table_.begin();
}

bool VecAggState::get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk) {
	if (!fully_scanned_) do_sink();
	chunk.reset();
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
			if (scan_->rs_cblock == InvalidBlockNumber) {
				scan_->rs_cblock = scan_->rs_startblock;
			} else {
				scan_->rs_cblock++;
			}

			if (scan_->rs_cblock >= scan_->rs_nblocks) break;

			current_buf_ = ReadBufferExtended(rel_, MAIN_FORKNUM, scan_->rs_cblock, RBM_NORMAL, scan_->rs_strategy);
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
				if (column.direct_var)
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
								   VolVecVector<VecJoinOutputCol> output_cols,
								   VolVecVector<VecHashJoinKeyCol> key_cols)
	: outer_(std::move(outer)),
	  inner_(std::move(inner)),
	  memory_context_(CurrentMemoryContext),
	  output_cols_(PgMemoryContextAllocator<VecJoinOutputCol>(memory_context_)),
	  key_cols_(PgMemoryContextAllocator<VecHashJoinKeyCol>(memory_context_)),
	  inner_payload_cols_(PgMemoryContextAllocator<VecHashPayloadCol>(memory_context_)),
	  inner_chunks_(PgMemoryContextAllocator<DataChunk<DEFAULT_CHUNK_SIZE> *>(memory_context_)),
	  bucket_heads_(PgMemoryContextAllocator<int32_t>(memory_context_)),
	  entries_(PgMemoryContextAllocator<VecHashEntry>(memory_context_)),
	  probe_rows_(PgMemoryContextAllocator<uint16_t>(memory_context_)),
	  probe_keys_(PgMemoryContextAllocator<VecHashJoinKey>(memory_context_)),
	  probe_hashes_(PgMemoryContextAllocator<uint32_t>(memory_context_)),
	  probe_next_entries_(PgMemoryContextAllocator<int32_t>(memory_context_)),
		  active_probe_sel_(PgMemoryContextAllocator<uint16_t>(memory_context_)),
		  next_probe_sel_(PgMemoryContextAllocator<uint16_t>(memory_context_)),
		  inner_built_(false),
		  probe_batch_ready_(false),
		  bucket_mask_(0)
{
	for (const auto &key_col : key_cols)
		key_cols_.push_back(key_col);
	for (const auto &output_col : output_cols)
	{
		VecJoinOutputCol remapped = output_col;

		if (remapped.side == VecJoinSide::Inner)
			remapped.input_col = ensure_inner_payload_col(remapped.input_col, remapped.meta);
		output_cols_.push_back(remapped);
	}
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

	if (inner_built_)
		return;

	init_hash_table(DEFAULT_CHUNK_SIZE);

	while (inner_->get_next_batch(input))
	{
		int active_count = input.has_selection ? input.sel.count : input.count;
		DataChunk<DEFAULT_CHUNK_SIZE> *dst =
			inner_chunks_.empty() ? allocate_inner_chunk() : inner_chunks_.back();

		for (int s = 0; s < active_count; s++)
		{
			int src_row = input.has_selection ? input.sel.row_ids[s] : s;
			int dst_row;
			VecHashJoinKey key;

			if (!read_key(input, true, src_row, &key))
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
}

bool
VecHashJoinState::advance_outer_batch()
{
	while (outer_->get_next_batch(outer_chunk_))
	{
		int active_count = outer_chunk_.has_selection ? outer_chunk_.sel.count : outer_chunk_.count;

		if (active_count <= 0)
			continue;
		probe_batch_ready_ = false;
		return true;
	}
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

		if (!read_key(outer_chunk_, false, row, &key))
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
VecHashJoinState::get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk)
{
	if (!inner_built_)
		build_inner_hash();

	chunk.reset();
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

				for (const auto &output_col : output_cols_)
				{
					int out_col = output_col.output_resno - 1;
					const DataChunk<DEFAULT_CHUNK_SIZE> *src =
						(output_col.side == VecJoinSide::Outer) ? &outer_chunk_ : inner_chunk;
					int src_row = (output_col.side == VecJoinSide::Outer) ? outer_row : entry.row_idx;
					int src_col = output_col.input_col;

					chunk.nulls[out_col][dst_row] = src->nulls[src_col][src_row];
					if (chunk.nulls[out_col][dst_row])
						continue;
					switch (output_col.meta.storage_kind)
					{
						case VecOutputStorageKind::Double:
							chunk.double_columns[out_col][dst_row] = src->double_columns[src_col][src_row];
							break;
						case VecOutputStorageKind::Int64:
						case VecOutputStorageKind::NumericScaledInt64:
						case VecOutputStorageKind::NumericAvgPair:
							chunk.int64_columns[out_col][dst_row] = src->int64_columns[src_col][src_row];
							chunk.double_columns[out_col][dst_row] = src->double_columns[src_col][src_row];
							break;
							case VecOutputStorageKind::StringRef:
								chunk.string_columns[out_col][dst_row] =
									CopyStringRefToChunk(chunk, *src, src->string_columns[src_col][src_row]);
								break;
						case VecOutputStorageKind::Int32:
							chunk.int32_columns[out_col][dst_row] = src->int32_columns[src_col][src_row];
							break;
					}
				}

				if (probe_next_entries_[probe_idx] >= 0)
					next_probe_sel_.push_back(probe_idx);
			}
		}

		active_probe_sel_.swap(next_probe_sel_);
		if (active_probe_sel_.empty())
		{
			outer_chunk_.reset();
			probe_batch_ready_ = false;
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
	  output_ncols_(output_ncols > 0 ? Min(output_ncols, 16) : Min(list_length(node->plan.targetlist), 16)),
	  materialized_(false)
{
	for (const auto &key_desc : key_descs)
	{
		key_descs_.push_back(key_desc);
		key_lanes_.emplace_back(key_desc, memory_context_);
	}
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
	bool simple = true;
	ListCell *lc;

	foreach(lc, node->plan.targetlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);

		if (!IsSimpleAggTargetExpr(node, (Expr *) tle->expr))
		{
			simple = false;
			break;
		}
	}

	if (simple)
		return std::make_unique<VecAggState>(std::move(left), node);

	VolVecVector<const Aggref *> aggrefs{PgMemoryContextAllocator<const Aggref *>(CurrentMemoryContext)};
	VolVecVector<int> aggresnos{PgMemoryContextAllocator<int>(CurrentMemoryContext)};
	VolVecVector<const Expr *> group_exprs{PgMemoryContextAllocator<const Expr *>(CurrentMemoryContext)};
	VolVecVector<int> group_resnos{PgMemoryContextAllocator<int>(CurrentMemoryContext)};
	List *synthetic_tlist = NIL;
	int next_resno = 1;

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

	auto agg_state = std::make_unique<VecAggState>(std::move(left), synthetic);
	VolVecVector<VecProjectColDesc> project_cols{PgMemoryContextAllocator<VecProjectColDesc>(CurrentMemoryContext)};
	AggrefRewriteContext rewrite_context{&aggrefs, &aggresnos, &group_exprs, &group_resnos};

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
				!agg_state->lookup_output_col_meta(var->varattno, &meta))
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
		else
		{
			project_col.expr = std::make_unique<VecExprProgram>();
			CompileExpr(rewritten_expr, *project_col.expr, false, estate);
			AdjustProgramVarScales(project_col.expr.get(), agg_state.get());
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

	return std::make_unique<VecProjectState>(std::move(agg_state), std::move(project_cols));
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
		if (meta.storage_kind == VecOutputStorageKind::StringRef &&
			meta.sql_type != BPCHAROID)
		{
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: sort currently only supports BPCHAR string refs");
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
	} else if (IsA(plan, HashJoin)) {
		HashJoin *hash_join = (HashJoin *) plan;
		Hash *hash_node;
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

			if (!IsA(plan->righttree, Hash))
			{
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: hash join right tree is not a Hash node");
				return nullptr;
			}
			hash_node = (Hash *) plan->righttree;
			BuildBinaryJoinChildRequiredAttrs(&hash_join->join.plan,
											  (Node *) hash_join->hashclauses,
											  plan->lefttree,
											  (Plan *) hash_node,
											  &outer_required_attrs,
											  &inner_required_attrs);
			outer = ExecInitVecPlanInternal(plan->lefttree, estate, outer_required_attrs, false);
			inner = ExecInitVecPlanInternal(hash_node->plan.lefttree, estate, inner_required_attrs, false);
			if (!outer || !inner)
			{
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: hash join child initialization failed (outer=%s inner=%s)",
						 outer ? "ok" : "null", inner ? "ok" : "null");
				return nullptr;
		}
		outer_state = outer.get();
		inner_state = inner.get();
		if (!ExtractJoinKeysFromClauses(hash_join->hashclauses,
									   "hash join",
									   plan->lefttree,
									   (Plan *) hash_node,
									   outer_state,
									   inner_state,
									   &key_cols))
			return nullptr;
		if (!BuildJoinOutputCols(hash_join->join.plan.targetlist,
								 plan->lefttree,
								 (Plan *) hash_node,
								 outer_state,
								 inner_state,
								 &output_cols,
								 &needs_project))
			return nullptr;
		join_filter_expr = BuildCombinedQualExpr(hash_join->join.joinqual,
												 hash_join->join.plan.qual);
		if (join_filter_expr != nullptr)
		{
			join_filter_expr = RewriteHashJoinFilterExpr(join_filter_expr,
														 plan->lefttree,
														 (Plan *) hash_node,
														 outer_state,
														 inner_state,
														 &output_cols);
			if (join_filter_expr == nullptr)
			{
				if (pg_volvec_trace_hooks)
					elog(LOG, "pg_volvec: hash join filter rewrite failed");
				return nullptr;
			}
		}
		current_state = std::make_unique<VecHashJoinState>(std::move(outer), std::move(inner),
														   std::move(output_cols),
														   std::move(key_cols));
		if (join_filter_expr != nullptr)
		{
			auto program = std::make_unique<VecExprProgram>();

			CompileExpr(join_filter_expr, *program, true, estate);
			if (program->get_final_res_idx() < 0)
			{
				if (pg_volvec_trace_hooks)
					elog(LOG, "pg_volvec: hash join filter expression compilation failed");
				return nullptr;
			}
			current_state = std::make_unique<VecFilterState>(std::move(current_state), std::move(program));
			plan_qual_already_applied = true;
		}
		if (needs_project)
		{
			current_state = BuildJoinProject(std::move(current_state),
											 hash_join->join.plan.targetlist,
											 plan->lefttree,
											 (Plan *) hash_node,
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
														   std::move(output_cols),
														   std::move(key_cols));
		if (join_filter_expr != nullptr)
		{
			auto program = std::make_unique<VecExprProgram>();

			CompileExpr(join_filter_expr, *program, true, estate);
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

		if (IsA(plan, Agg))
		{
			Expr *rewritten_qual = RewriteExprAgainstTargetList(combined_qual, plan->targetlist);

			if (rewritten_qual != nullptr)
				combined_qual = rewritten_qual;
		}
		CompileExpr(combined_qual, *program, true, estate);
		AdjustProgramVarScales(program.get(), current_state.get());
		if (program->get_final_res_idx() < 0)
		{
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: plan qual compilation failed for node type %d",
					 (int) nodeTag(plan));
			return nullptr;
		}
		current_state = std::make_unique<VecFilterState>(std::move(current_state), std::move(program));
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

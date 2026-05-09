#include "parallel/pipeline/translator_internal.hpp"

extern "C" {
#include "catalog/pg_type_d.h"
#include "fmgr.h"
#include "nodes/nodes.h"
#include "utils/fmgroids.h"
#include "utils/numeric.h"

extern Datum numeric_mul(PG_FUNCTION_ARGS);
extern Datum numeric_int8(PG_FUNCTION_ARGS);
}

#include <algorithm>
#include <cstring>

namespace pg_volvec {
namespace pipeline {
namespace translator_detail {

Expr *
StripRelabels(Expr *expr)
{
	while (expr != nullptr && nodeTag(expr) == T_RelabelType)
		expr = ((RelabelType *) expr)->arg;
	return expr;
}

bool
Pow10Int64(int exp, int64_t &out)
{
	if (exp < 0 || exp > 18)
		return false;
	out = 1;
	for (int i = 0; i < exp; ++i)
		out *= 10;
	return true;
}

bool
RescaleInt64Constant(int64_t value, int8_t from_scale, int8_t to_scale, int64_t &out)
{
	if (to_scale < 0 || from_scale < 0)
		return false;
	if (to_scale < from_scale)
	{
		int64_t factor = 1;
		if (!Pow10Int64(static_cast<int>(from_scale - to_scale), factor) || factor == 0)
			return false;
		out = value / factor;
		return true;
	}
	int64_t factor = 1;
	if (!Pow10Int64(static_cast<int>(to_scale - from_scale), factor))
		return false;
	out = value * factor;
	return true;
}

bool
LookupRawColumn(const ColumnRef &ref,
		        const std::vector<ColumnRef> &raw_cols_ref,
		        const std::vector<ColumnSchema> &raw_cols,
		        const ColumnSchema *&out_col)
{
	for (size_t i = 0; i < raw_cols_ref.size(); ++i)
	{
		if (raw_cols_ref[i] == ref)
		{
			out_col = &raw_cols[i];
			return true;
		}
	}
	return false;
}

bool
ColumnNumericScale(const ColumnSchema &col, int8_t &out)
{
		switch (col.decode_kind)
		{
			case ColumnDecodeKind::INT64_NUMERIC_SCALED:
				out = 2;
				return true;
			case ColumnDecodeKind::INT64_INT8:
			case ColumnDecodeKind::INT32_INT4:
			case ColumnDecodeKind::INT32_CHAR:
			case ColumnDecodeKind::INT32_DATE:
			case ColumnDecodeKind::DOUBLE_FLOAT8:
			case ColumnDecodeKind::STRING_REF:
				out = 0;
				return true;
			case ColumnDecodeKind::NONE:
				return false;
		}
	return false;
}

int16_t
ExtractNumericTypmodScale(int32 typmod)
{
	if (typmod < VARHDRSZ)
		return 0;
	return static_cast<int16_t>((((typmod - VARHDRSZ) & 0x7ff) ^ 1024) - 1024);
}

bool
ScaleNumericConstDatumToInt64(Const *c, int8_t &out_scale, int64_t &out_value)
{
	if (c == nullptr || c->constisnull || c->consttype != NUMERICOID)
		return false;
	out_scale = static_cast<int8_t>(ExtractNumericTypmodScale(c->consttypmod));
	int64_t factor = 1;
	if (!Pow10Int64(out_scale, factor))
		return false;
	Datum factor_numeric = NumericGetDatum(int64_to_numeric(factor));
	Datum scaled = DirectFunctionCall2(numeric_mul, c->constvalue, factor_numeric);
	out_value = DatumGetInt64(DirectFunctionCall1(numeric_int8, scaled));
	return true;
}

bool
ScaleNumericConstDatumToTargetScale(Const *c, int8_t target_scale, int64_t &out_value)
{
	if (c == nullptr || c->constisnull || c->consttype != NUMERICOID || target_scale < 0)
		return false;
	if (TryFastNumericToScaledInt64(c->constvalue, target_scale, &out_value))
		return true;
	int64_t factor = 1;
	if (!Pow10Int64(target_scale, factor))
		return false;
	Datum factor_numeric = NumericGetDatum(int64_to_numeric(factor));
	Datum scaled = DirectFunctionCall2(numeric_mul, c->constvalue, factor_numeric);
	out_value = DatumGetInt64(DirectFunctionCall1(numeric_int8, scaled));
	return true;
}

bool
ResolvePlanVarToColumnRef(Var *var, Plan *context_plan, ColumnRef &out_ref)
{
	if (var == nullptr)
		return false;
	if (var->varattno <= 0)
		return false;
	if (!IS_SPECIAL_VARNO(var->varno))
	{
		out_ref = ColumnRef{static_cast<Index>(var->varno), var->varattno};
		return out_ref.varno > 0 && out_ref.attno > 0;
	}
	if (context_plan == nullptr)
		return false;
	Plan *child_plan = nullptr;
	if (var->varno == OUTER_VAR)
		child_plan = context_plan->lefttree;
	else if (var->varno == INNER_VAR)
		child_plan = context_plan->righttree;
	else
		return false;
	if (child_plan == nullptr || child_plan->targetlist == NIL)
		return false;
	if (var->varattno < 1 || var->varattno > list_length(child_plan->targetlist))
		return false;
	TargetEntry *tle = (TargetEntry *) list_nth(child_plan->targetlist, var->varattno - 1);
	if (tle == nullptr || tle->expr == nullptr)
		return false;
	return ResolvePlanExprToColumnRef((Expr *) tle->expr, child_plan, out_ref);
}

bool
ResolvePlanExprToColumnRef(Expr *expr, Plan *context_plan, ColumnRef &out_ref)
{
	expr = StripRelabels(expr);
	if (expr == nullptr || nodeTag(expr) != T_Var)
		return false;
	return ResolvePlanVarToColumnRef((Var *) expr, context_plan, out_ref);
}

bool
IsBareVarArg(Expr *arg, Plan *context_plan, ColumnRef &out_ref)
{
	arg = StripRelabels(arg);
	if (arg == nullptr || nodeTag(arg) != T_Var)
		return false;
	return ResolvePlanVarToColumnRef((Var *) arg, context_plan, out_ref);
}

static bool
LookupCachedExpr(Expr *expr,
		     const std::vector<MaterializedProjectExpr> *cache,
		     int8_t &out_scale,
		     uint8_t &out_slot)
{
	if (cache == nullptr)
		return false;
	for (const MaterializedProjectExpr &entry : *cache)
	{
		if (equal(entry.expr, expr))
		{
			out_scale = entry.scale;
			out_slot = entry.slot;
			return true;
		}
	}
	return false;
}

static bool LowerExprToStepsInternal(Expr *e,
				     std::vector<ProjectStep> &steps,
				     uint8_t &next_int64_slot,
				     const std::vector<ColumnRef> &raw_cols_ref,
				     const std::vector<ColumnSchema> &raw_cols,
				     Plan *context_plan,
				     const std::vector<MaterializedProjectExpr> *cache,
				     int8_t &out_result_scale,
				     uint8_t &out_result_slot);

static bool
AppendScaleProjectStep(uint8_t input_slot,
			   int8_t input_scale,
			   int8_t target_scale,
			   std::vector<ProjectStep> &steps,
			   uint8_t &next_int64_slot,
			   uint8_t &out_slot)
{
	if (input_scale == target_scale)
	{
		out_slot = input_slot;
		return true;
	}
	int64_t factor = 0;
	const int scale_delta = static_cast<int>(target_scale) - static_cast<int>(input_scale);
	if (!Pow10Int64(std::abs(scale_delta), factor) || next_int64_slot >= 16)
		return false;
	out_slot = next_int64_slot++;
	steps.push_back(ProjectStep{ProjectOp::NUMERIC_SCALE_VAR_CONST,
		input_slot, 0, out_slot, scale_delta >= 0 ? factor : -factor});
	return true;
}

static bool
LowerNumericBinaryExpr(OpExpr *op,
			      Expr *lhs,
			      Expr *rhs,
			      const char *opname,
			      std::vector<ProjectStep> &steps,
			      uint8_t &next_int64_slot,
			      const std::vector<ColumnRef> &raw_cols_ref,
			      const std::vector<ColumnSchema> &raw_cols,
			      Plan *context_plan,
			      const std::vector<MaterializedProjectExpr> *cache,
			      int8_t &out_result_scale,
			      uint8_t &out_result_slot)
{
	(void) op;
	const bool is_mul = std::strcmp(opname, "*") == 0;
	const bool is_sub = std::strcmp(opname, "-") == 0;
	const bool is_add = std::strcmp(opname, "+") == 0;
	if (!is_mul && !is_sub && !is_add)
		return false;
	lhs = StripRelabels(lhs);
	rhs = StripRelabels(rhs);
	const bool lhs_const = lhs != nullptr && nodeTag(lhs) == T_Const;
	const bool rhs_const = rhs != nullptr && nodeTag(rhs) == T_Const;

	if (is_mul)
	{
		if (lhs_const && rhs_const)
			return false;
		if (lhs_const || rhs_const)
		{
			Expr *var_expr = lhs_const ? rhs : lhs;
			Const *const_expr = (Const *) (lhs_const ? lhs : rhs);
			int8_t var_scale = 0;
			uint8_t var_slot = 0;
			if (!LowerExprToStepsInternal(var_expr, steps, next_int64_slot,
					raw_cols_ref, raw_cols, context_plan, cache, var_scale, var_slot))
				return false;
			int8_t const_scale = 0;
			int64_t const_value = 0;
			if (!ScaleNumericConstDatumToInt64(const_expr, const_scale, const_value) || next_int64_slot >= 16)
				return false;
			out_result_slot = next_int64_slot++;
			out_result_scale = static_cast<int8_t>(var_scale + const_scale);
			steps.push_back(ProjectStep{ProjectOp::NUMERIC_MUL_VAR_CONST,
				var_slot, 0, out_result_slot, const_value});
			return true;
		}
		int8_t lhs_scale = 0;
		int8_t rhs_scale = 0;
		uint8_t lhs_slot = 0;
		uint8_t rhs_slot = 0;
		if (!LowerExprToStepsInternal(lhs, steps, next_int64_slot, raw_cols_ref, raw_cols, context_plan, cache, lhs_scale, lhs_slot) ||
		    !LowerExprToStepsInternal(rhs, steps, next_int64_slot, raw_cols_ref, raw_cols, context_plan, cache, rhs_scale, rhs_slot) ||
		    next_int64_slot >= 16)
			return false;
		out_result_slot = next_int64_slot++;
		out_result_scale = static_cast<int8_t>(lhs_scale + rhs_scale);
		steps.push_back(ProjectStep{ProjectOp::NUMERIC_MUL_VAR_VAR,
			lhs_slot, rhs_slot, out_result_slot, 0});
		return true;
	}

	if (!lhs_const || rhs_const)
	{
		if (lhs_const && !rhs_const)
		{
			int8_t rhs_scale = 0;
			uint8_t rhs_slot = 0;
			if (!LowerExprToStepsInternal(rhs, steps, next_int64_slot, raw_cols_ref, raw_cols, context_plan, cache, rhs_scale, rhs_slot))
				return false;
			int8_t lhs_scale = 0;
			int64_t lhs_value = 0;
			if (!ScaleNumericConstDatumToInt64((Const *) lhs, lhs_scale, lhs_value))
				return false;
			out_result_scale = static_cast<int8_t>(Max(lhs_scale, rhs_scale));
			if (!RescaleInt64Constant(lhs_value, lhs_scale, out_result_scale, lhs_value) || next_int64_slot >= 16)
				return false;
			out_result_slot = next_int64_slot++;
			steps.push_back(ProjectStep{is_sub ? ProjectOp::NUMERIC_SUB_CONST_VAR : ProjectOp::NUMERIC_ADD_CONST_VAR,
				0, rhs_slot, out_result_slot, lhs_value});
			return true;
		}
		if (!lhs_const && rhs_const)
		{
			int8_t lhs_scale = 0;
			uint8_t lhs_slot = 0;
			if (!LowerExprToStepsInternal(lhs, steps, next_int64_slot, raw_cols_ref, raw_cols, context_plan, cache, lhs_scale, lhs_slot))
				return false;
			int8_t rhs_scale = 0;
			int64_t rhs_value = 0;
			if (!ScaleNumericConstDatumToInt64((Const *) rhs, rhs_scale, rhs_value))
				return false;
			out_result_scale = static_cast<int8_t>(Max(lhs_scale, rhs_scale));
			uint8_t lhs_aligned_slot = lhs_slot;
			if (!AppendScaleProjectStep(lhs_slot, lhs_scale, out_result_scale, steps, next_int64_slot, lhs_aligned_slot) ||
			    !RescaleInt64Constant(rhs_value, rhs_scale, out_result_scale, rhs_value) ||
			    next_int64_slot >= 16)
				return false;
			out_result_slot = next_int64_slot++;
			steps.push_back(ProjectStep{is_sub ? ProjectOp::NUMERIC_SUB_VAR_CONST : ProjectOp::NUMERIC_ADD_VAR_CONST,
				lhs_aligned_slot, 0, out_result_slot, rhs_value});
			return true;
		}
		if (lhs_const && rhs_const)
			return false;
		int8_t lhs_scale = 0;
		int8_t rhs_scale = 0;
		uint8_t lhs_slot = 0;
		uint8_t rhs_slot = 0;
		if (!LowerExprToStepsInternal(lhs, steps, next_int64_slot, raw_cols_ref, raw_cols, context_plan, cache, lhs_scale, lhs_slot) ||
		    !LowerExprToStepsInternal(rhs, steps, next_int64_slot, raw_cols_ref, raw_cols, context_plan, cache, rhs_scale, rhs_slot))
			return false;
		out_result_scale = static_cast<int8_t>(Max(lhs_scale, rhs_scale));
		uint8_t lhs_aligned_slot = lhs_slot;
		uint8_t rhs_aligned_slot = rhs_slot;
		if (!AppendScaleProjectStep(lhs_slot, lhs_scale, out_result_scale, steps, next_int64_slot, lhs_aligned_slot) ||
		    !AppendScaleProjectStep(rhs_slot, rhs_scale, out_result_scale, steps, next_int64_slot, rhs_aligned_slot) ||
		    next_int64_slot >= 16)
			return false;
		out_result_slot = next_int64_slot++;
		steps.push_back(ProjectStep{is_sub ? ProjectOp::NUMERIC_SUB_VAR_VAR : ProjectOp::NUMERIC_ADD_VAR_VAR,
			lhs_aligned_slot, rhs_aligned_slot, out_result_slot, 0});
		return true;
	}

	int8_t rhs_scale = 0;
	uint8_t rhs_slot = 0;
	if (!LowerExprToStepsInternal(rhs, steps, next_int64_slot, raw_cols_ref, raw_cols, context_plan, cache, rhs_scale, rhs_slot))
		return false;
	int8_t lhs_scale = 0;
	int64_t lhs_value = 0;
	if (!ScaleNumericConstDatumToInt64((Const *) lhs, lhs_scale, lhs_value))
		return false;
	out_result_scale = static_cast<int8_t>(Max(lhs_scale, rhs_scale));
	if (!RescaleInt64Constant(lhs_value, lhs_scale, out_result_scale, lhs_value) || next_int64_slot >= 16)
		return false;
	out_result_slot = next_int64_slot++;
	steps.push_back(ProjectStep{is_sub ? ProjectOp::NUMERIC_SUB_CONST_VAR : ProjectOp::NUMERIC_ADD_CONST_VAR,
		0, rhs_slot, out_result_slot, lhs_value});
	return true;
}

static bool
LowerExprToStepsInternal(Expr *e,
				     std::vector<ProjectStep> &steps,
				     uint8_t &next_int64_slot,
				     const std::vector<ColumnRef> &raw_cols_ref,
				     const std::vector<ColumnSchema> &raw_cols,
				     Plan *context_plan,
				     const std::vector<MaterializedProjectExpr> *cache,
				     int8_t &out_result_scale,
				     uint8_t &out_result_slot)
{
	e = StripRelabels(e);
	if (e == nullptr)
		return false;
	if (LookupCachedExpr(e, cache, out_result_scale, out_result_slot))
		return true;
	ColumnRef ref{};
	if (IsBareVarArg(e, context_plan, ref))
	{
		const ColumnSchema *col = nullptr;
		if (!LookupRawColumn(ref, raw_cols_ref, raw_cols, col) || !ColumnNumericScale(*col, out_result_scale))
			return false;
		out_result_slot = col->chunk_slot;
		return true;
	}
	if (nodeTag(e) != T_OpExpr)
		return false;
	OpExpr *op = (OpExpr *) e;
	if (list_length(op->args) != 2)
		return false;
	Expr *lhs = (Expr *) linitial(op->args);
	Expr *rhs = (Expr *) lsecond(op->args);
	char *opname = get_opname(op->opno);
	if (opname == nullptr)
		return false;
	const bool ok = LowerNumericBinaryExpr(op, lhs, rhs, opname,
		steps, next_int64_slot, raw_cols_ref, raw_cols, context_plan, cache,
		out_result_scale, out_result_slot);
	pfree(opname);
	return ok;
}

static bool
CollectVarLeavesFromExpr(Expr *expr, Plan *context_plan, std::vector<ColumnRef> &out)
{
	if (expr == nullptr)
		return false;
	expr = StripRelabels(expr);
	if (expr == nullptr)
		return false;
	switch (nodeTag(expr))
	{
		case T_Var:
		{
			ColumnRef ref{};
			if (!ResolvePlanVarToColumnRef((Var *) expr, context_plan, ref))
				return false;
			out.push_back(ref);
			return true;
		}
		case T_Const:
			return true;
		case T_OpExpr:
		{
			OpExpr *op = (OpExpr *) expr;
			ListCell *lc;
			foreach(lc, op->args)
			{
				if (!CollectVarLeavesFromExpr((Expr *) lfirst(lc), context_plan, out))
					return false;
			}
			return true;
		}
		case T_FuncExpr:
		{
			FuncExpr *fe = (FuncExpr *) expr;
			ListCell *lc;
			foreach(lc, fe->args)
			{
				if (!CollectVarLeavesFromExpr((Expr *) lfirst(lc), context_plan, out))
					return false;
			}
			return true;
		}
		default:
			return false;
	}
}

bool
CollectAggrefArgCols(const std::vector<Aggref *> &aggrefs,
		     Plan *context_plan,
			     std::vector<ColumnRef> &out)
{
	for (Aggref *ag : aggrefs)
	{
		if (ag->aggorder != NIL || ag->aggdistinct != NIL || ag->aggfilter != nullptr)
			return false;
		if (ag->aggstar)
			continue;
		ListCell *lc;
		foreach(lc, ag->args)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(lc);
			if (tle == nullptr || nodeTag(tle) != T_TargetEntry)
				return false;
			if (!CollectVarLeavesFromExpr(tle->expr, context_plan, out))
				return false;
		}
	}
	return true;
}

bool
ClassifyAggref(Aggref *ag,
		       const std::vector<ColumnRef> &raw_cols_ref,
		       const std::vector<ColumnSchema> &raw_cols,
		       Plan *context_plan,
		       std::vector<ProjectStep> &project_steps,
		       std::vector<ProjectExprDesc> &project_exprs,
		       std::vector<MaterializedProjectExpr> &materialized_exprs,
		       uint8_t &next_int64_slot,
		       AggFuncDesc &out_desc,
		       TdcAggKind &out_kind,
		       int16_t &out_numeric_scale)
{
	if (ag->aggorder != NIL || ag->aggdistinct != NIL || ag->aggfilter != nullptr)
		return false;
	out_desc = AggFuncDesc{};
	out_desc.agg_oid = ag->aggfnoid;
	out_desc.transtype = ag->aggtranstype;
	out_desc.finaltype = ag->aggtype;
	out_desc.input_col_idx = 0;
	out_desc._pad = 0;
	out_numeric_scale = 0;
	if (ag->aggfnoid == F_COUNT_)
	{
		if (!ag->aggstar)
			return false;
		out_kind = TdcAggKind::COUNT_STAR;
		return true;
	}
	if (ag->aggfnoid == F_COUNT_ANY)
		return false;
	if (list_length(ag->args) != 1)
		return false;
	TargetEntry *tle = (TargetEntry *) linitial(ag->args);
	if (tle == nullptr || tle->expr == nullptr)
		return false;
	Expr *arg = (Expr *) tle->expr;
	if (ag->aggfnoid == F_SUM_INT4)
	{
		ColumnRef ref{};
		if (!IsBareVarArg(arg, context_plan, ref))
			return false;
		const ColumnSchema *col = nullptr;
		if (!LookupRawColumn(ref, raw_cols_ref, raw_cols, col))
			return false;
		out_kind = TdcAggKind::SUM_INT64;
		out_desc.input_col_idx = col->chunk_slot;
		return true;
	}
	if (ag->aggfnoid != F_SUM_NUMERIC && ag->aggfnoid != F_AVG_NUMERIC)
		return false;
	out_kind = (ag->aggfnoid == F_SUM_NUMERIC) ? TdcAggKind::SUM_NUMERIC : TdcAggKind::AVG_NUMERIC;
	ColumnRef bare_ref{};
	if (IsBareVarArg(arg, context_plan, bare_ref))
	{
		const ColumnSchema *col = nullptr;
		if (!LookupRawColumn(bare_ref, raw_cols_ref, raw_cols, col))
			return false;
		int8_t scale = 0;
		if (!ColumnNumericScale(*col, scale))
			return false;
		out_desc.input_col_idx = col->chunk_slot;
		out_numeric_scale = scale;
		return true;
	}
	int8_t lowered_scale = 0;
	uint8_t lowered_slot = 0;
	if (LookupCachedExpr(arg, &materialized_exprs, lowered_scale, lowered_slot))
	{
		out_desc.input_col_idx = lowered_slot;
		out_numeric_scale = lowered_scale;
		return true;
	}
	const uint16_t first_step_idx = static_cast<uint16_t>(project_steps.size());
	if (!LowerExprToStepsInternal(arg, project_steps, next_int64_slot,
			raw_cols_ref, raw_cols, context_plan, &materialized_exprs, lowered_scale, lowered_slot) ||
	    project_steps.size() == first_step_idx)
		return false;
	project_exprs.push_back(ProjectExprDesc{first_step_idx,
		static_cast<uint16_t>(project_steps.size() - first_step_idx),
		lowered_slot,
		lowered_scale,
		0});
	materialized_exprs.push_back(MaterializedProjectExpr{arg, lowered_scale, lowered_slot});
	out_desc.input_col_idx = lowered_slot;
	out_numeric_scale = lowered_scale;
	return true;
}

}  /* namespace translator_detail */
}  /* namespace pipeline */
}  /* namespace pg_volvec */

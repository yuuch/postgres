#include "parallel/pipeline/translator_internal.hpp"

extern "C" {
#include "catalog/pg_type_d.h"
#include "fmgr.h"
#include "nodes/nodes.h"
#include "utils/lsyscache.h"
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
			out = col.typmod >= VARHDRSZ ? static_cast<int8_t>(ExtractNumericTypmodScale(col.typmod)) : 2;
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

static bool
ExpandSpecialPlanVar(Var *var,
		     Plan *context_plan,
		     Expr *&out_expr,
		     Plan *&out_plan)
{
	out_expr = nullptr;
	out_plan = nullptr;
	if (var == nullptr || context_plan == nullptr || !IS_SPECIAL_VARNO(var->varno))
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
	out_expr = StripRelabels((Expr *) tle->expr);
	out_plan = child_plan;
	return out_expr != nullptr;
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
ExtractTextConstCString(Const *c, char *&out);

static bool
StoreShortStringConst(Const *c, uint8_t &out_len, int64_t &out_value)
{
	char *str = nullptr;
	if (!ExtractTextConstCString(c, str))
		return false;
	const size_t len = std::strlen(str);
	if (len > 8)
	{
		pfree(str);
		return false;
	}
	out_len = static_cast<uint8_t>(len);
	out_value = 0;
	if (len > 0)
		std::memcpy(&out_value, str, len);
	pfree(str);
	return true;
}

static bool
LowerBoolExprToStepsInternal(Expr *expr,
				     std::vector<ProjectStep> &steps,
				     uint8_t &next_int64_slot,
				     const std::vector<ColumnRef> &raw_cols_ref,
				     const std::vector<ColumnSchema> &raw_cols,
				     Plan *context_plan,
				     uint8_t &out_slot);

static bool
ExtractTextConstCString(Const *c, char *&out)
{
	if (c == nullptr || c->constisnull)
		return false;
	if (c->consttype != TEXTOID && c->consttype != VARCHAROID && c->consttype != BPCHAROID)
		return false;
	out = TextDatumGetCString(c->constvalue);
	return out != nullptr;
}

static bool
ScaleIntegralConstToTargetScale(Const *c, int8_t target_scale, int64_t &out_value)
{
	if (c == nullptr || c->constisnull || target_scale < 0)
		return false;
	int64_t base_value = 0;
	switch (c->consttype)
	{
		case INT4OID:
			base_value = DatumGetInt32(c->constvalue);
			break;
		case INT8OID:
			base_value = DatumGetInt64(c->constvalue);
			break;
		case BOOLOID:
			base_value = DatumGetBool(c->constvalue) ? 1 : 0;
			break;
		default:
			return false;
	}
	return RescaleInt64Constant(base_value, 0, target_scale, out_value);
}

static bool
ScaleConstDatumToTargetScale(Const *c, int8_t target_scale, int64_t &out_value)
{
	if (c == nullptr || c->constisnull)
		return false;
	if (c->consttype == NUMERICOID)
		return ScaleNumericConstDatumToTargetScale(c, target_scale, out_value);
	return ScaleIntegralConstToTargetScale(c, target_scale, out_value);
}

static bool
LowerInt64ConstExpr(Const *c,
			    std::vector<ProjectStep> &steps,
			    uint8_t &next_int64_slot,
			    int8_t &out_result_scale,
			    uint8_t &out_result_slot)
{
	if (c == nullptr || c->constisnull || next_int64_slot >= 16)
		return false;
	int64_t value = 0;
	int8_t scale = 0;
	switch (c->consttype)
	{
		case NUMERICOID:
			if (!ScaleNumericConstDatumToInt64(c, scale, value))
				return false;
			break;
		case INT4OID:
			value = DatumGetInt32(c->constvalue);
			break;
		case INT8OID:
			value = DatumGetInt64(c->constvalue);
			break;
		case BOOLOID:
			value = DatumGetBool(c->constvalue) ? 1 : 0;
			break;
		default:
			return false;
	}
	out_result_slot = next_int64_slot++;
	out_result_scale = scale;
	steps.push_back(ProjectStep{ProjectOp::CONST_INT64, 0, 0, out_result_slot, value});
	return true;
}

static bool
LowerExtractYearExpr(FuncExpr *func,
			     std::vector<ProjectStep> &steps,
			     uint8_t &next_int64_slot,
			     const std::vector<ColumnRef> &raw_cols_ref,
			     const std::vector<ColumnSchema> &raw_cols,
			     Plan *context_plan,
			     int8_t &out_result_scale,
			     uint8_t &out_result_slot)
{
	if (func == nullptr || list_length(func->args) != 2)
		return false;
	char *funcname = get_func_name(func->funcid);
	if (funcname == nullptr)
		return false;
	const bool is_part = std::strcmp(funcname, "date_part") == 0 || std::strcmp(funcname, "extract") == 0;
	pfree(funcname);
	if (!is_part)
		return false;
	Expr *field_expr = StripRelabels((Expr *) linitial(func->args));
	Expr *value_expr = StripRelabels((Expr *) lsecond(func->args));
	if (field_expr == nullptr || value_expr == nullptr || nodeTag(field_expr) != T_Const)
		return false;
	char *field = nullptr;
	if (!ExtractTextConstCString((Const *) field_expr, field))
		return false;
	const bool is_year = pg_strcasecmp(field, "year") == 0;
	pfree(field);
	if (!is_year)
		return false;
	ColumnRef ref{};
	if (!IsBareVarArg(value_expr, context_plan, ref))
		return false;
	const ColumnSchema *col = nullptr;
	if (!LookupRawColumn(ref, raw_cols_ref, raw_cols, col) || col == nullptr)
		return false;
	if (col->decode_kind != ColumnDecodeKind::INT32_DATE)
		return false;
	if (next_int64_slot >= 16)
		return false;
	out_result_slot = next_int64_slot++;
	out_result_scale = 0;
	steps.push_back(ProjectStep{ProjectOp::EXTRACT_YEAR_FROM_DATE,
		col->chunk_slot,
		0,
		out_result_slot,
		0});
	return true;
}

static bool
LowerStringCompareExpr(OpExpr *op,
			      Expr *lhs,
			      Expr *rhs,
			      const char *opname,
			      std::vector<ProjectStep> &steps,
			      uint8_t &next_int64_slot,
			      const std::vector<ColumnRef> &raw_cols_ref,
			      const std::vector<ColumnSchema> &raw_cols,
			      Plan *context_plan,
			      uint8_t &out_result_slot)
{
	(void) op;
	ColumnRef ref{};
	if (!IsBareVarArg(lhs, context_plan, ref) || nodeTag(StripRelabels(rhs)) != T_Const)
		return false;
	const ColumnSchema *col = nullptr;
	if (!LookupRawColumn(ref, raw_cols_ref, raw_cols, col) || col == nullptr)
		return false;
	if (col->decode_kind != ColumnDecodeKind::STRING_REF)
		return false;
	uint8_t const_len = 0;
	int64_t const_value = 0;
	if (!StoreShortStringConst((Const *) StripRelabels(rhs), const_len, const_value))
		return false;
	if (next_int64_slot >= 16)
		return false;
	out_result_slot = next_int64_slot++;
	steps.push_back(ProjectStep{std::strcmp(opname, "=") == 0 ? ProjectOp::STRING_EQ_VAR_CONST : ProjectOp::STRING_NE_VAR_CONST,
		col->chunk_slot,
		const_len,
		out_result_slot,
		const_value});
	return true;
}

static bool
LowerBoolExprToStepsInternal(Expr *expr,
				     std::vector<ProjectStep> &steps,
				     uint8_t &next_int64_slot,
				     const std::vector<ColumnRef> &raw_cols_ref,
				     const std::vector<ColumnSchema> &raw_cols,
				     Plan *context_plan,
				     uint8_t &out_slot)
{
	expr = StripRelabels(expr);
	if (expr == nullptr)
		return false;
	if (nodeTag(expr) == T_OpExpr)
	{
		OpExpr *op = (OpExpr *) expr;
		if (list_length(op->args) != 2)
			return false;
		char *opname = get_opname(op->opno);
		if (opname == nullptr)
			return false;
		const bool ok = LowerStringCompareExpr(op,
			(Expr *) linitial(op->args),
			(Expr *) lsecond(op->args),
			opname,
			steps,
			next_int64_slot,
			raw_cols_ref,
			raw_cols,
			context_plan,
			out_slot);
		pfree(opname);
		return ok;
	}
	if (nodeTag(expr) != T_BoolExpr)
		return false;
	BoolExpr *bool_expr = (BoolExpr *) expr;
	if (bool_expr->boolop == NOT_EXPR)
	{
		if (list_length(bool_expr->args) != 1)
			return false;
		uint8_t child_slot = 0;
		if (!LowerBoolExprToStepsInternal((Expr *) linitial(bool_expr->args), steps, next_int64_slot,
				raw_cols_ref, raw_cols, context_plan, child_slot) || next_int64_slot >= 16)
			return false;
		out_slot = next_int64_slot++;
		steps.push_back(ProjectStep{ProjectOp::BOOL_NOT_VAR, child_slot, 0, out_slot, 0});
		return true;
	}
	ListCell *lc = list_head(bool_expr->args);
	if (lc == nullptr)
		return false;
	uint8_t left_slot = 0;
	if (!LowerBoolExprToStepsInternal((Expr *) lfirst(lc), steps, next_int64_slot,
			raw_cols_ref, raw_cols, context_plan, left_slot))
		return false;
	for_each_from(lc, bool_expr->args, 1)
	{
		uint8_t right_slot = 0;
		if (!LowerBoolExprToStepsInternal((Expr *) lfirst(lc), steps, next_int64_slot,
				raw_cols_ref, raw_cols, context_plan, right_slot) || next_int64_slot >= 16)
			return false;
		out_slot = next_int64_slot++;
		steps.push_back(ProjectStep{bool_expr->boolop == AND_EXPR ? ProjectOp::BOOL_AND_VAR_VAR : ProjectOp::BOOL_OR_VAR_VAR,
			left_slot,
			right_slot,
			out_slot,
			0});
		left_slot = out_slot;
	}
	out_slot = left_slot;
	return true;
}

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
	const bool is_div = std::strcmp(opname, "/") == 0;
	if (!is_mul && !is_sub && !is_add && !is_div)
		return false;
	lhs = StripRelabels(lhs);
	rhs = StripRelabels(rhs);
	const bool lhs_const = lhs != nullptr && nodeTag(lhs) == T_Const;
	const bool rhs_const = rhs != nullptr && nodeTag(rhs) == T_Const;

	if (is_div)
	{
		if (lhs_const || rhs_const)
			return false;
		int8_t lhs_scale = 0;
		int8_t rhs_scale = 0;
		uint8_t lhs_slot = 0;
		uint8_t rhs_slot = 0;
		if (!LowerExprToStepsInternal(lhs, steps, next_int64_slot, raw_cols_ref, raw_cols, context_plan, cache, lhs_scale, lhs_slot) ||
		    !LowerExprToStepsInternal(rhs, steps, next_int64_slot, raw_cols_ref, raw_cols, context_plan, cache, rhs_scale, rhs_slot) ||
		    next_int64_slot >= 16)
			return false;
		int64_t factor = 0;
		out_result_scale = 2;
		if (!Pow10Int64(static_cast<int>(out_result_scale) + static_cast<int>(rhs_scale) - static_cast<int>(lhs_scale), factor))
			return false;
		out_result_slot = next_int64_slot++;
		steps.push_back(ProjectStep{ProjectOp::NUMERIC_DIV_VAR_VAR,
			lhs_slot, rhs_slot, out_result_slot, factor});
		return true;
	}

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
LowerStringPrefixLike(OpExpr *op,
			     std::vector<ProjectStep> &steps,
			     uint8_t &next_int64_slot,
			     const std::vector<ColumnRef> &raw_cols_ref,
			     const std::vector<ColumnSchema> &raw_cols,
			     Plan *context_plan,
			     uint8_t &out_slot)
{
	if (op == nullptr || list_length(op->args) != 2)
		return false;
	char *opname = get_opname(op->opno);
	if (opname == nullptr)
		return false;
	const bool is_like = std::strcmp(opname, "~~") == 0;
	pfree(opname);
	if (!is_like)
		return false;
	Expr *lhs = StripRelabels((Expr *) linitial(op->args));
	Expr *rhs = StripRelabels((Expr *) lsecond(op->args));
	if (lhs == nullptr || rhs == nullptr || nodeTag(rhs) != T_Const)
		return false;
	ColumnRef ref{};
	if (!IsBareVarArg(lhs, context_plan, ref))
		return false;
	const ColumnSchema *col = nullptr;
	if (!LookupRawColumn(ref, raw_cols_ref, raw_cols, col) || col->decode_kind != ColumnDecodeKind::STRING_REF)
		return false;
	std::vector<char> pool;
	uint32_t offset = 0;
	uint32_t len = 0;
	uint64_t value = 0;
	if (!ExtractStringLikePrefix((Const *) rhs, pool, offset, len, value) || offset != UINT32_MAX || len > 8 || next_int64_slot >= 16)
		return false;
	out_slot = next_int64_slot++;
	steps.push_back(ProjectStep{ProjectOp::STRING_PREFIX_LIKE,
		col->chunk_slot,
		static_cast<uint8_t>(len),
		out_slot,
		static_cast<int64_t>(value)});
	return true;
}

static bool
LowerCaseExpr(CaseExpr *case_expr,
		      std::vector<ProjectStep> &steps,
		      uint8_t &next_int64_slot,
		      const std::vector<ColumnRef> &raw_cols_ref,
		      const std::vector<ColumnSchema> &raw_cols,
		      Plan *context_plan,
		      const std::vector<MaterializedProjectExpr> *cache,
		      int8_t &out_result_scale,
		      uint8_t &out_result_slot)
{
	if (case_expr == nullptr || case_expr->arg != nullptr || list_length(case_expr->args) != 1 ||
	    case_expr->defresult == nullptr)
		return false;
	CaseWhen *when = (CaseWhen *) linitial(case_expr->args);
	if (when == nullptr || nodeTag(when) != T_CaseWhen || when->expr == nullptr || when->result == nullptr)
		return false;
	Expr *cond = StripRelabels((Expr *) when->expr);
	if (cond == nullptr)
		return false;
	uint8_t cond_slot = 0;
	if (!LowerBoolExprToStepsInternal(cond,
			steps,
			next_int64_slot,
			raw_cols_ref,
			raw_cols,
			context_plan,
			cond_slot))
		return false;
	int8_t then_scale = 0;
	uint8_t then_slot = 0;
	if (!LowerExprToStepsInternal((Expr *) when->result, steps, next_int64_slot,
			raw_cols_ref, raw_cols, context_plan, cache, then_scale, then_slot))
		return false;
	Const *else_const = (Const *) StripRelabels((Expr *) case_expr->defresult);
	if (else_const != nullptr && nodeTag(else_const) == T_Const)
	{
		int64_t else_value = 0;
		if (!ScaleConstDatumToTargetScale(else_const, then_scale, else_value) || next_int64_slot >= 16)
			return false;
		out_result_slot = next_int64_slot++;
		out_result_scale = then_scale;
		steps.push_back(ProjectStep{ProjectOp::NUMERIC_CASE_VAR_CONST,
			cond_slot,
			then_slot,
			out_result_slot,
			else_value});
		return true;
	}
	int8_t else_scale = 0;
	uint8_t else_slot = 0;
	if (!LowerExprToStepsInternal((Expr *) case_expr->defresult, steps, next_int64_slot,
			raw_cols_ref, raw_cols, context_plan, cache, else_scale, else_slot))
		return false;
	out_result_scale = static_cast<int8_t>(Max(then_scale, else_scale));
	uint8_t aligned_then_slot = then_slot;
	uint8_t aligned_else_slot = else_slot;
	if (!AppendScaleProjectStep(then_slot, then_scale, out_result_scale, steps, next_int64_slot, aligned_then_slot) ||
		!AppendScaleProjectStep(else_slot, else_scale, out_result_scale, steps, next_int64_slot, aligned_else_slot) ||
		next_int64_slot >= 16)
		return false;
	out_result_slot = next_int64_slot++;
	steps.push_back(ProjectStep{ProjectOp::NUMERIC_CASE_ELSE_VAR,
		cond_slot,
		aligned_then_slot,
		out_result_slot,
		aligned_else_slot});
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
	if (nodeTag(e) == T_Const)
		return LowerInt64ConstExpr((Const *) e, steps, next_int64_slot,
			out_result_scale, out_result_slot);
	if (nodeTag(e) == T_FuncExpr)
		return LowerExtractYearExpr((FuncExpr *) e, steps, next_int64_slot,
			raw_cols_ref, raw_cols, context_plan, out_result_scale, out_result_slot);
	if (nodeTag(e) == T_CaseExpr)
		return LowerCaseExpr((CaseExpr *) e, steps, next_int64_slot,
			raw_cols_ref, raw_cols, context_plan, cache,
			out_result_scale, out_result_slot);
	ColumnRef ref{};
	if (IsBareVarArg(e, context_plan, ref))
	{
		const ColumnSchema *col = nullptr;
		if (!LookupRawColumn(ref, raw_cols_ref, raw_cols, col) || !ColumnNumericScale(*col, out_result_scale))
			return false;
		out_result_slot = col->chunk_slot;
		return true;
	}
	if (nodeTag(e) == T_Var)
	{
		Expr *expanded = nullptr;
		Plan *expanded_plan = nullptr;
		if (ExpandSpecialPlanVar((Var *) e, context_plan, expanded, expanded_plan))
		{
			return LowerExprToStepsInternal(expanded,
				steps,
				next_int64_slot,
				raw_cols_ref,
				raw_cols,
				expanded_plan,
				cache,
				out_result_scale,
				out_result_slot);
		}
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

bool
LowerProjectionExpr(Expr *expr,
			     std::vector<ProjectStep> &steps,
			     uint8_t &next_int64_slot,
			     const std::vector<ColumnRef> &raw_cols_ref,
			     const std::vector<ColumnSchema> &raw_cols,
			     Plan *context_plan,
			     const std::vector<MaterializedProjectExpr> *cache,
			     int8_t &out_result_scale,
			     uint8_t &out_result_slot)
{
	return LowerExprToStepsInternal(expr,
		steps,
		next_int64_slot,
		raw_cols_ref,
		raw_cols,
		context_plan,
		cache,
		out_result_scale,
		out_result_slot);
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
			{
				Expr *expanded = nullptr;
				Plan *expanded_plan = nullptr;
				if (!ExpandSpecialPlanVar((Var *) expr, context_plan, expanded, expanded_plan))
					return false;
				return CollectVarLeavesFromExpr(expanded, expanded_plan, out);
			}
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
		case T_CaseExpr:
		{
			CaseExpr *case_expr = (CaseExpr *) expr;
			if (case_expr->arg != nullptr && !CollectVarLeavesFromExpr((Expr *) case_expr->arg, context_plan, out))
				return false;
			ListCell *lc;
			foreach(lc, case_expr->args)
			{
				CaseWhen *when = (CaseWhen *) lfirst(lc);
				if (when == nullptr || nodeTag(when) != T_CaseWhen ||
				    !CollectVarLeavesFromExpr((Expr *) when->expr, context_plan, out) ||
				    !CollectVarLeavesFromExpr((Expr *) when->result, context_plan, out))
					return false;
			}
			return case_expr->defresult == nullptr ||
				CollectVarLeavesFromExpr((Expr *) case_expr->defresult, context_plan, out);
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
CollectExprVarCols(Expr *expr,
		   Plan *context_plan,
		   std::vector<ColumnRef> &out)
{
	return CollectVarLeavesFromExpr(expr, context_plan, out);
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
		{
			int8_t lowered_scale = 0;
			uint8_t lowered_slot = 0;
			if (!LowerExprToStepsInternal(arg, project_steps, next_int64_slot,
					raw_cols_ref, raw_cols, context_plan, &materialized_exprs,
					lowered_scale, lowered_slot) ||
				lowered_scale != 0)
				return false;
			out_kind = TdcAggKind::SUM_INT64;
			out_desc.input_col_idx = lowered_slot;
			return true;
		}
		const ColumnSchema *col = nullptr;
		if (!LookupRawColumn(ref, raw_cols_ref, raw_cols, col) || col == nullptr)
			return false;
		if (col->decode_kind == ColumnDecodeKind::INT32_INT4)
		{
			if (next_int64_slot >= 16)
				return false;
			const uint16_t first_step_idx = static_cast<uint16_t>(project_steps.size());
			const uint8_t cast_slot = next_int64_slot++;
			project_steps.push_back(ProjectStep{ProjectOp::INT32_TO_INT64_VAR,
				col->chunk_slot,
				0,
				cast_slot,
				0});
			project_exprs.push_back(ProjectExprDesc{first_step_idx, 1, cast_slot, 0, 0});
			materialized_exprs.push_back(MaterializedProjectExpr{arg, 0, cast_slot});
			out_kind = TdcAggKind::SUM_INT64;
			out_desc.input_col_idx = cast_slot;
			return true;
		}
		if (col->decode_kind != ColumnDecodeKind::INT64_INT8)
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

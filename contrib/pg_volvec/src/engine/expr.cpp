#include "volvec_engine.hpp"

#include <cmath>

extern "C" {
#include "nodes/nodeFuncs.h"
#include "nodes/primnodes.h"
#include "utils/array.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/lsyscache.h"
#include "utils/timestamp.h"

extern bool pg_volvec_trace_hooks;
}

namespace pg_volvec
{

static const char *
VecOpCodeName(VecOpCode opcode)
{
	switch (opcode)
	{
		case VecOpCode::EEOP_VAR: return "VAR";
		case VecOpCode::EEOP_CONST: return "CONST";
		case VecOpCode::EEOP_FLOAT8_ADD: return "FLOAT8_ADD";
		case VecOpCode::EEOP_FLOAT8_SUB: return "FLOAT8_SUB";
		case VecOpCode::EEOP_FLOAT8_MUL: return "FLOAT8_MUL";
		case VecOpCode::EEOP_INT64_ADD: return "INT64_ADD";
		case VecOpCode::EEOP_INT64_SUB: return "INT64_SUB";
		case VecOpCode::EEOP_INT64_MUL: return "INT64_MUL";
		case VecOpCode::EEOP_INT64_DIV_FLOAT8: return "INT64_DIV_FLOAT8";
		case VecOpCode::EEOP_FLOAT8_LT: return "FLOAT8_LT";
		case VecOpCode::EEOP_FLOAT8_GT: return "FLOAT8_GT";
		case VecOpCode::EEOP_FLOAT8_LE: return "FLOAT8_LE";
		case VecOpCode::EEOP_FLOAT8_GE: return "FLOAT8_GE";
		case VecOpCode::EEOP_INT64_LT: return "INT64_LT";
		case VecOpCode::EEOP_INT64_GT: return "INT64_GT";
		case VecOpCode::EEOP_INT64_LE: return "INT64_LE";
		case VecOpCode::EEOP_INT64_GE: return "INT64_GE";
		case VecOpCode::EEOP_INT64_EQ: return "INT64_EQ";
		case VecOpCode::EEOP_INT64_NE: return "INT64_NE";
		case VecOpCode::EEOP_DATE_LT: return "DATE_LT";
		case VecOpCode::EEOP_DATE_LE: return "DATE_LE";
		case VecOpCode::EEOP_DATE_GT: return "DATE_GT";
		case VecOpCode::EEOP_DATE_GE: return "DATE_GE";
		case VecOpCode::EEOP_DATE_PART_YEAR: return "DATE_PART_YEAR";
		case VecOpCode::EEOP_AND: return "AND";
		case VecOpCode::EEOP_OR: return "OR";
		case VecOpCode::EEOP_INT64_CASE: return "INT64_CASE";
		case VecOpCode::EEOP_FLOAT8_CASE: return "FLOAT8_CASE";
		case VecOpCode::EEOP_STR_EQ: return "STR_EQ";
		case VecOpCode::EEOP_STR_NE: return "STR_NE";
		case VecOpCode::EEOP_STR_PREFIX_LIKE: return "STR_PREFIX_LIKE";
		case VecOpCode::EEOP_QUAL: return "QUAL";
		default: return "UNKNOWN";
	}
}

static Expr *
StripImplicitNodes(Expr *expr)
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
TryConvertConstToDate32(Const *c, int32_t *out)
{
	if (c == nullptr || c->constisnull || out == nullptr)
		return false;

	if (c->consttype == DATEOID)
	{
		*out = DatumGetDateADT(c->constvalue);
		return true;
	}

	if (c->consttype == TIMESTAMPOID)
	{
		Timestamp ts = DatumGetTimestamp(c->constvalue);
		if ((ts % USECS_PER_DAY) != 0)
			return false;
		*out = (int32_t) (ts / USECS_PER_DAY);
		return true;
	}

	if (c->consttype == TIMESTAMPTZOID)
	{
		TimestampTz ts = DatumGetTimestampTz(c->constvalue);
		if ((ts % USECS_PER_DAY) != 0)
			return false;
		*out = (int32_t) (ts / USECS_PER_DAY);
		return true;
	}

	return false;
}

static bool
TryExtractYearFieldConst(Const *c)
{
	char *field;
	bool matches;

	if (c == nullptr || c->constisnull)
		return false;
	if (c->consttype != TEXTOID &&
		c->consttype != VARCHAROID &&
		c->consttype != BPCHAROID)
		return false;

	field = TextDatumGetCString(c->constvalue);
	matches = (pg_strcasecmp(field, "year") == 0);
	pfree(field);
	return matches;
}

static bool
TryExtractConstInt32(Const *c, int32_t *out)
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
ExtractStringSourceVar(Expr *expr, Var **var_out, Oid *type_out)
{
	expr = StripImplicitNodes(expr);
	if (var_out != nullptr)
		*var_out = nullptr;
	if (type_out != nullptr)
		*type_out = InvalidOid;
	if (expr == nullptr)
		return false;
	if (IsA(expr, Var))
	{
		Var *var = (Var *) expr;
		Oid type = exprType((Node *) expr);

		if (type != BPCHAROID && type != TEXTOID && type != VARCHAROID)
			return false;
		if (var_out != nullptr)
			*var_out = var;
		if (type_out != nullptr)
			*type_out = type;
		return true;
	}
	if (IsA(expr, FuncExpr))
	{
		FuncExpr *func = (FuncExpr *) expr;
		char *funcname = get_func_name(func->funcid);
		Oid rettype = exprType((Node *) expr);

		if (list_length(func->args) == 1 &&
			(rettype == BPCHAROID || rettype == TEXTOID || rettype == VARCHAROID))
			return ExtractStringSourceVar(StripImplicitNodes((Expr *) linitial(func->args)),
										var_out,
										type_out);
	}
	return false;
}

static bool
MatchStringPrefixFuncExpr(Expr *expr, int *att_idx, Oid *source_type, uint32_t *prefix_len)
{
	FuncExpr *func;
	char *funcname;
	Expr *arg_expr;
	Expr *start_expr;
	Expr *len_expr;
	Var *var = nullptr;
	int32_t start_val;
	int32_t len_val;
	Oid arg_type;

	expr = StripImplicitNodes(expr);
	if (att_idx != nullptr)
		*att_idx = -1;
	if (source_type != nullptr)
		*source_type = InvalidOid;
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
		return MatchStringPrefixFuncExpr((Expr *) linitial(func->args),
										 att_idx,
										 source_type,
										 prefix_len);
	if (funcname == nullptr ||
		(strcmp(funcname, "substring") != 0 && strcmp(funcname, "substr") != 0) ||
		(list_length(func->args) != 2 && list_length(func->args) != 3))
		return false;

	arg_expr = StripImplicitNodes((Expr *) linitial(func->args));
	start_expr = StripImplicitNodes((Expr *) lsecond(func->args));
	len_expr = list_length(func->args) == 3 ?
		StripImplicitNodes((Expr *) lthird(func->args)) : nullptr;
	if (arg_expr == nullptr || start_expr == nullptr || !IsA(start_expr, Const))
		return false;
	if (!ExtractStringSourceVar(arg_expr, &var, &arg_type) ||
		var == nullptr ||
		var->varattno <= 0 || var->varattno > 16 ||
		!TryExtractConstInt32((Const *) start_expr, &start_val) ||
		start_val != 1)
		return false;
	if (len_expr != nullptr)
	{
		if (!IsA(len_expr, Const) || !TryExtractConstInt32((Const *) len_expr, &len_val))
			return false;
	}
	else
	{
		return false;
	}
	if (len_val < 0)
		return false;

	if (att_idx != nullptr)
		*att_idx = var->varattno - 1;
	if (source_type != nullptr)
		*source_type = arg_type;
	if (prefix_len != nullptr)
		*prefix_len = (uint32_t) len_val;
	return true;
}

static int64_t
ExtractYearFromDate32(int32_t date_val)
{
	int year;
	int month;
	int day;

	j2date(date_val + POSTGRES_EPOCH_JDATE, &year, &month, &day);
	return (int64_t) year;
}

static bool
IsInt64LikeType(Oid type)
{
	return type == NUMERICOID || type == INT8OID || type == INT4OID || type == INT2OID;
}

static bool
IsIntegerType(Oid type)
{
	return type == INT2OID || type == INT4OID || type == INT8OID;
}

static bool
IsDateLikeType(Oid type)
{
	return type == DATEOID || type == TIMESTAMPOID || type == TIMESTAMPTZOID;
}

static bool
ExprProducesFloat8Result(Expr *expr)
{
	OpExpr *op;
	Expr *left_expr;
	Expr *right_expr;
	Oid left_type;
	Oid right_type;
	char *opname;

	expr = StripImplicitNodes(expr);
	if (expr == nullptr)
		return false;
	if (exprType((Node *) expr) == FLOAT8OID)
		return true;
	if (!IsA(expr, OpExpr))
		return false;

	op = (OpExpr *) expr;
	if (list_length(op->args) != 2)
		return false;

	left_expr = StripImplicitNodes((Expr *) linitial(op->args));
	right_expr = StripImplicitNodes((Expr *) lsecond(op->args));
	if (left_expr == nullptr || right_expr == nullptr)
		return false;

	left_type = exprType((Node *) left_expr);
	right_type = exprType((Node *) right_expr);
	opname = get_opname(op->opno);
	if (opname == nullptr)
		return false;

	if (strcmp(opname, "/") == 0 &&
		IsInt64LikeType(left_type) && IsInt64LikeType(right_type))
		return true;

	if ((strcmp(opname, "+") == 0 ||
		 strcmp(opname, "-") == 0 ||
		 strcmp(opname, "*") == 0) &&
		(ExprProducesFloat8Result(left_expr) || ExprProducesFloat8Result(right_expr)))
		return true;

	return false;
}

static bool
IsValidNumericTypmod(int32 typmod)
{
	return typmod >= (int32) VARHDRSZ;
}

static int
ClampTrackedScale(int scale)
{
	if (scale < 0)
		return 0;
	if (scale > 18)
		return 18;
	return scale;
}

static int64_t
Pow10Int64(int scale)
{
	static const int64_t kPowers[] = {
		INT64CONST(1),
		INT64CONST(10),
		INT64CONST(100),
		INT64CONST(1000),
		INT64CONST(10000),
		INT64CONST(100000),
		INT64CONST(1000000),
		INT64CONST(10000000),
		INT64CONST(100000000),
		INT64CONST(1000000000),
		INT64CONST(10000000000),
		INT64CONST(100000000000),
		INT64CONST(1000000000000),
		INT64CONST(10000000000000),
		INT64CONST(100000000000000),
		INT64CONST(1000000000000000),
		INT64CONST(10000000000000000),
		INT64CONST(100000000000000000),
		INT64CONST(1000000000000000000)
	};

	scale = ClampTrackedScale(scale);
	return kPowers[scale];
}

static int64_t
RescaleInt64Value(int64_t value, int from_scale, int to_scale)
{
	if (from_scale == to_scale)
		return value;

	if (from_scale < to_scale)
	{
		NumericWideInt widened = WideIntFromInt64(value) * Pow10Int64(to_scale - from_scale);
		return WideIntToInt64Checked(widened, "rescaled numeric register");
	}

	int delta = from_scale - to_scale;
	int64_t divisor = Pow10Int64(delta);
	int64_t quotient = value / divisor;
	int64_t remainder = value % divisor;
	int64_t halfway = divisor / 2;

	if (remainder >= halfway)
		quotient++;
	else if (remainder <= -halfway)
		quotient--;

	return quotient;
}

static NumericWideInt
RescaleInt64ValueWide(int64_t value, int from_scale, int to_scale)
{
	if (from_scale == to_scale)
		return WideIntFromInt64(value);

	if (from_scale < to_scale)
	{
		NumericWideInt widened = WideIntFromInt64(value);
		NumericWideInt factor = WideIntFromInt64(Pow10Int64(to_scale - from_scale));

		return WideIntMul(widened, factor);
	}

	return WideIntFromInt64(RescaleInt64Value(value, from_scale, to_scale));
}

static bool
StepHasWideConst(const VecExprStep *step)
{
	return step != nullptr &&
		step->opcode == VecOpCode::EEOP_CONST &&
		step->d.constant.has_wide_i128;
}

static NumericWideInt
StepWideConstValue(const VecExprStep *step)
{
	if (!StepHasWideConst(step))
		return WideIntFromInt64(step != nullptr ? step->d.constant.i64val : 0);
	return MakeWideIntBits(step->d.constant.wide_lo,
						   (uint64_t) step->d.constant.wide_hi);
}

static NumericWideInt
RescaleOperandForCompare(const VecExprStep *step,
						 int64_t reg_value,
						 int from_scale,
						 int to_scale)
{
	NumericWideInt value;

	if (StepHasWideConst(step))
		value = StepWideConstValue(step);
	else
		value = WideIntFromInt64(reg_value);
	if (from_scale >= to_scale)
		return value;
	return RescaleWideIntUp(value, to_scale - from_scale);
}

static int64_t
ScaleFloatToInt64(double value, int scale)
{
	return (int64_t) std::llround(value * (double) Pow10Int64(scale));
}

static int
GetNumericScaleForVar(const Var *var)
{
	if (var == nullptr)
		return 0;
	if (var->vartype == INT8OID)
		return 0;
	if (var->vartype != NUMERICOID)
		return 0;
	if (IsValidNumericTypmod(var->vartypmod))
		return ClampTrackedScale(GetNumericScaleFromTypmod(var->vartypmod));
	return DEFAULT_NUMERIC_SCALE;
}

static int
GetNumericScaleForConst(const Const *c)
{
	if (c == nullptr)
		return 0;
	if (c->consttype == INT8OID)
		return 0;
	if (c->consttype != NUMERICOID)
		return 0;
	if (IsValidNumericTypmod(c->consttypmod))
		return ClampTrackedScale(GetNumericScaleFromTypmod(c->consttypmod));
	return DEFAULT_NUMERIC_SCALE;
}

static int
ResolveResultScale(VecOpCode opcode, int left_scale, int right_scale)
{
	switch (opcode)
	{
		case VecOpCode::EEOP_INT64_ADD:
		case VecOpCode::EEOP_INT64_SUB:
		case VecOpCode::EEOP_INT64_LT:
		case VecOpCode::EEOP_INT64_LE:
		case VecOpCode::EEOP_INT64_GT:
		case VecOpCode::EEOP_INT64_GE:
		case VecOpCode::EEOP_INT64_EQ:
			return Max(left_scale, right_scale);
		case VecOpCode::EEOP_INT64_MUL:
			return ClampTrackedScale(left_scale + right_scale);
		default:
			return 0;
	}
}

static int
AppendDateConstStep(VecExprProgram &program, int32_t date_val)
{
	int res_idx = program.max_reg_idx++;

	if (res_idx >= MAX_REGISTERS)
		return -1;

	VecExprStep step;
	step.opcode = VecOpCode::EEOP_CONST;
	step.res_idx = res_idx;
	step.d.constant.isnull = false;
	step.d.constant.has_wide_i128 = false;
	step.d.constant.wide_lo = 0;
	step.d.constant.wide_hi = 0;
	step.d.constant.ival = date_val;
	step.d.constant.i64val = (int64_t) date_val;
	step.d.constant.fval = (double) date_val;
	program.set_register_scale(res_idx, 0);
	program.steps.push_back(step);
	return res_idx;
}

static int
AllocateResultRegister(VecExprProgram &program)
{
	int res_idx = program.max_reg_idx++;

	if (res_idx >= MAX_REGISTERS)
		return -1;
	program.set_register_scale(res_idx, 0);
	return res_idx;
}

static bool TryExtractStringConstPrefix(Const *c, uint64_t *prefix_out, uint32_t *len_out);
static bool TryExtractLikePrefix(Const *c, uint64_t *prefix_out, uint32_t *len_out);
static bool TryExtractLikeContains(Const *c, char **str_out, uint32_t *len_out, uint64_t *prefix_out);
static bool TryExtractStringConst(Const *c, char **str_out, uint32_t *len_out, uint64_t *prefix_out);
static int CompileExprRecursive(Expr *expr, VecExprProgram &program, EState *estate);

static bool
AppendStringCompareStep(VecExprProgram &program,
						  int res_idx,
						  Expr *left_expr,
						  Expr *right_expr,
						  const char *opname)
{
	VecExprStep special_step;
	uint64_t prefix = 0;
	uint32_t len = 0;
	bool ok;
	Oid left_type;
	int left_att_idx = -1;

	if (opname == nullptr ||
		left_expr == nullptr ||
		right_expr == nullptr ||
		!IsA(right_expr, Const))
		return false;

	if (IsA(left_expr, Var))
	{
		left_att_idx = ((Var *) left_expr)->varattno - 1;
		left_type = exprType((Node *) left_expr);
	}
	else if (!MatchStringPrefixFuncExpr(left_expr, &left_att_idx, &left_type, &len))
		return false;

	if (left_type != BPCHAROID &&
		left_type != TEXTOID &&
		left_type != VARCHAROID)
		return false;

	special_step.res_idx = res_idx;
	special_step.d.str_prefix.att_idx = left_att_idx;
	special_step.d.str_prefix.prefix = 0;
	special_step.d.str_prefix.len = len;
	special_step.d.str_prefix.offset = UINT32_MAX;
	special_step.d.str_prefix.type = left_type;

	if (strcmp(opname, "~~") == 0)
	{
		char *match = nullptr;

		ok = TryExtractLikePrefix((Const *) right_expr, &prefix, &len);
		special_step.opcode = VecOpCode::EEOP_STR_PREFIX_LIKE;
		if (!ok)
		{
			ok = TryExtractLikeContains((Const *) right_expr, &match, &len, &prefix);
			special_step.opcode = VecOpCode::EEOP_STR_CONTAINS_LIKE;
			if (ok && len > 8)
				special_step.d.str_prefix.offset = program.store_string_const(match, len);
		}
		if (match != nullptr)
		{
			pfree(match);
			match = nullptr;
		}
		if (!ok)
		{
			ok = TryExtractStringConst((Const *) right_expr, &match, &len, &prefix);
			special_step.opcode = VecOpCode::EEOP_STR_LIKE_PATTERN;
			if (ok && len > 8)
				special_step.d.str_prefix.offset = program.store_string_const(match, len);
		}
		if (match != nullptr)
			pfree(match);
	}
	else if (strcmp(opname, "=") == 0)
	{
		char *match = nullptr;

		ok = TryExtractStringConst((Const *) right_expr, &match, &len, &prefix);
		if (ok && len > 8)
			special_step.d.str_prefix.offset = program.store_string_const(match, len);
		if (match != nullptr)
			pfree(match);
		special_step.opcode = IsA(StripImplicitNodes(left_expr), FuncExpr) ?
			VecOpCode::EEOP_STR_PREFIX_LIKE : VecOpCode::EEOP_STR_EQ;
	}
	else if (strcmp(opname, "<>") == 0)
	{
		char *match = nullptr;

		ok = TryExtractStringConst((Const *) right_expr, &match, &len, &prefix);
		if (ok && len > 8)
			special_step.d.str_prefix.offset = program.store_string_const(match, len);
		if (match != nullptr)
			pfree(match);
		special_step.opcode = VecOpCode::EEOP_STR_NE;
	}
	else
		return false;

	if (!ok)
		return false;

	special_step.d.str_prefix.prefix = prefix;
	special_step.d.str_prefix.len = len;
	program.steps.push_back(special_step);
	return true;
}

static void
EvalBoolAnd(uint8_t left_null, int32_t left_val,
			uint8_t right_null, int32_t right_val,
			uint8_t *out_null, int32_t *out_val)
{
	bool left_true = !left_null && left_val != 0;
	bool right_true = !right_null && right_val != 0;
	bool left_false = !left_null && left_val == 0;
	bool right_false = !right_null && right_val == 0;

	if (left_false || right_false)
	{
		*out_null = 0;
		*out_val = 0;
		return;
	}
	if (left_true && right_true)
	{
		*out_null = 0;
		*out_val = 1;
		return;
	}
	*out_null = 1;
	*out_val = 0;
}

static void
EvalBoolOr(uint8_t left_null, int32_t left_val,
		   uint8_t right_null, int32_t right_val,
		   uint8_t *out_null, int32_t *out_val)
{
	bool left_true = !left_null && left_val != 0;
	bool right_true = !right_null && right_val != 0;
	bool left_false = !left_null && left_val == 0;
	bool right_false = !right_null && right_val == 0;

	if (left_true || right_true)
	{
		*out_null = 0;
		*out_val = 1;
		return;
	}
	if (left_false && right_false)
	{
		*out_null = 0;
		*out_val = 0;
		return;
	}
	*out_null = 1;
	*out_val = 0;
}

static bool
AppendBoolCombineStep(VecExprProgram &program,
					  VecOpCode opcode,
					  int left,
					  int right,
					  int res_idx)
{
	VecExprStep step;

	if (left < 0 || right < 0 || res_idx < 0)
		return false;

	step.opcode = opcode;
	step.res_idx = res_idx;
	step.d.op.left = left;
	step.d.op.right = right;
	program.steps.push_back(step);
	return true;
}

static bool
AppendBoolNotStep(VecExprProgram &program, int arg, int res_idx)
{
	VecExprStep step;

	if (arg < 0 || res_idx < 0)
		return false;
	step.opcode = VecOpCode::EEOP_NOT;
	step.res_idx = res_idx;
	step.d.op.left = arg;
	step.d.op.right = 0;
	program.steps.push_back(step);
	return true;
}

static int
CompileScalarArrayExpr(ScalarArrayOpExpr *array_expr,
					   VecExprProgram &program,
					   int res_idx,
					   EState *estate)
{
	Expr *left_expr;
	Expr *right_expr;
	Const *array_const;
	ArrayType *array_value;
	Oid elem_type;
	char *opname;
	int16 typlen;
	bool typbyval;
	char typalign;
	Datum *elem_values;
	bool *elem_nulls;
	int nelems;
	int left = -1;

	if (array_expr == nullptr || list_length(array_expr->args) != 2)
		return -1;

	left_expr = StripImplicitNodes((Expr *) linitial(array_expr->args));
	right_expr = StripImplicitNodes((Expr *) lsecond(array_expr->args));
	if (left_expr == nullptr || right_expr == nullptr || !IsA(right_expr, Const))
		return -1;

	array_const = (Const *) right_expr;
	if (array_const->constisnull)
		return -1;

	opname = get_opname(array_expr->opno);
	if (opname == nullptr)
		return -1;

	array_value = DatumGetArrayTypeP(array_const->constvalue);
	elem_type = ARR_ELEMTYPE(array_value);
	get_typlenbyvalalign(elem_type, &typlen, &typbyval, &typalign);
	deconstruct_array(array_value,
					  elem_type,
					  typlen,
					  typbyval,
					  typalign,
					  &elem_values,
					  &elem_nulls,
					  &nelems);

	if (nelems == 0)
	{
		VecExprStep step;

		step.opcode = VecOpCode::EEOP_CONST;
		step.res_idx = res_idx;
		step.d.constant.isnull = false;
		step.d.constant.fval = 0.0;
		step.d.constant.i64val = array_expr->useOr ? 0 : 1;
		step.d.constant.ival = array_expr->useOr ? 0 : 1;
		step.d.constant.has_wide_i128 = true;
		step.d.constant.wide_lo = WideIntLow64(WideIntFromInt64(step.d.constant.i64val));
		step.d.constant.wide_hi = WideIntHigh64(WideIntFromInt64(step.d.constant.i64val));
		program.steps.push_back(step);
		return res_idx;
	}

	for (int i = 0; i < nelems; i++)
	{
		Const elem_const;
		int cmp_reg;
		OpExpr cmp_expr;

		if (elem_nulls[i])
			return -1;

		memset(&elem_const, 0, sizeof(elem_const));
		elem_const.xpr.type = T_Const;
		elem_const.consttype = elem_type;
		elem_const.consttypmod = -1;
		elem_const.constcollid = array_expr->inputcollid;
		elem_const.constlen = typlen;
		elem_const.constbyval = typbyval;
		elem_const.constisnull = false;
		elem_const.location = -1;
		elem_const.constvalue = elem_values[i];

		memset(&cmp_expr, 0, sizeof(cmp_expr));
		cmp_expr.xpr.type = T_OpExpr;
		cmp_expr.opno = array_expr->opno;
		cmp_expr.opfuncid = array_expr->opfuncid;
		cmp_expr.opresulttype = BOOLOID;
		cmp_expr.opretset = false;
		cmp_expr.opcollid = InvalidOid;
		cmp_expr.inputcollid = array_expr->inputcollid;
		cmp_expr.args = list_make2(left_expr, &elem_const);
		cmp_expr.location = -1;

		cmp_reg = CompileExprRecursive((Expr *) &cmp_expr, program, estate);
		if (cmp_reg < 0)
			return -1;

		if (left < 0)
			left = cmp_reg;
		else if (!AppendBoolCombineStep(program,
										array_expr->useOr ? VecOpCode::EEOP_OR : VecOpCode::EEOP_AND,
										left,
										cmp_reg,
										res_idx))
			return -1;
		else
			left = res_idx;
	}

	return left;
}

static bool
ResolveBinaryOpcode(const char *opname, Oid left_type, Oid right_type, VecOpCode *opcode)
{
	if (opcode == nullptr || opname == nullptr)
		return false;

	if (left_type == FLOAT8OID && right_type == FLOAT8OID)
	{
		if (strcmp(opname, "+") == 0) *opcode = VecOpCode::EEOP_FLOAT8_ADD;
		else if (strcmp(opname, "-") == 0) *opcode = VecOpCode::EEOP_FLOAT8_SUB;
		else if (strcmp(opname, "*") == 0) *opcode = VecOpCode::EEOP_FLOAT8_MUL;
		else if (strcmp(opname, "<") == 0) *opcode = VecOpCode::EEOP_FLOAT8_LT;
		else if (strcmp(opname, "<=") == 0) *opcode = VecOpCode::EEOP_FLOAT8_LE;
		else if (strcmp(opname, ">") == 0) *opcode = VecOpCode::EEOP_FLOAT8_GT;
		else if (strcmp(opname, ">=") == 0) *opcode = VecOpCode::EEOP_FLOAT8_GE;
		else return false;
		return true;
	}

	if (IsInt64LikeType(left_type) && IsInt64LikeType(right_type))
	{
		if (strcmp(opname, "+") == 0) *opcode = VecOpCode::EEOP_INT64_ADD;
		else if (strcmp(opname, "-") == 0) *opcode = VecOpCode::EEOP_INT64_SUB;
		else if (strcmp(opname, "*") == 0) *opcode = VecOpCode::EEOP_INT64_MUL;
		else if (strcmp(opname, "/") == 0) *opcode = VecOpCode::EEOP_INT64_DIV_FLOAT8;
		else if (strcmp(opname, "<") == 0) *opcode = VecOpCode::EEOP_INT64_LT;
		else if (strcmp(opname, "<=") == 0) *opcode = VecOpCode::EEOP_INT64_LE;
		else if (strcmp(opname, ">") == 0) *opcode = VecOpCode::EEOP_INT64_GT;
		else if (strcmp(opname, ">=") == 0) *opcode = VecOpCode::EEOP_INT64_GE;
		else if (strcmp(opname, "=") == 0) *opcode = VecOpCode::EEOP_INT64_EQ;
		else if (strcmp(opname, "<>") == 0) *opcode = VecOpCode::EEOP_INT64_NE;
		else return false;
		return true;
	}

	if (left_type == DATEOID && right_type == DATEOID)
	{
		if (strcmp(opname, "<") == 0) *opcode = VecOpCode::EEOP_DATE_LT;
		else if (strcmp(opname, "<=") == 0) *opcode = VecOpCode::EEOP_DATE_LE;
		else if (strcmp(opname, ">") == 0) *opcode = VecOpCode::EEOP_DATE_GT;
		else if (strcmp(opname, ">=") == 0) *opcode = VecOpCode::EEOP_DATE_GE;
		else return false;
		return true;
	}

	return false;
}

static bool
TryExtractStringConstPrefix(Const *c, uint64_t *prefix_out, uint32_t *len_out)
{
	char *str;
	uint32_t len;
	uint64_t prefix = 0;

	if (c == nullptr || c->constisnull)
		return false;
	if (c->consttype != TEXTOID &&
		c->consttype != VARCHAROID &&
		c->consttype != BPCHAROID)
		return false;

	str = TextDatumGetCString(c->constvalue);
	len = (uint32_t) strlen(str);
	if (len > 8)
	{
		pfree(str);
		return false;
	}
	memcpy(&prefix, str, len);
	pfree(str);
	if (prefix_out != nullptr)
		*prefix_out = prefix;
	if (len_out != nullptr)
		*len_out = len;
	return true;
}

static bool
TryExtractStringConst(Const *c, char **str_out, uint32_t *len_out, uint64_t *prefix_out)
{
	char *str;
	uint32_t len;
	uint64_t prefix = 0;

	if (str_out != nullptr)
		*str_out = nullptr;
	if (c == nullptr || c->constisnull)
		return false;
	if (c->consttype != TEXTOID &&
		c->consttype != VARCHAROID &&
		c->consttype != BPCHAROID)
		return false;

	str = TextDatumGetCString(c->constvalue);
	len = (uint32_t) strlen(str);
	memcpy(&prefix, str, len > 8 ? 8 : len);
	if (str_out != nullptr)
		*str_out = str;
	else
		pfree(str);
	if (len_out != nullptr)
		*len_out = len;
	if (prefix_out != nullptr)
		*prefix_out = prefix;
	return true;
}

static bool
TryExtractLikePrefix(Const *c, uint64_t *prefix_out, uint32_t *len_out)
{
	char *pattern;
	size_t len;
	uint64_t prefix = 0;

	if (c == nullptr || c->constisnull)
		return false;
	if (c->consttype != TEXTOID &&
		c->consttype != VARCHAROID &&
		c->consttype != BPCHAROID)
		return false;

	pattern = TextDatumGetCString(c->constvalue);
	len = strlen(pattern);
	if (len == 0 || pattern[len - 1] != '%')
	{
		pfree(pattern);
		return false;
	}
	for (size_t i = 0; i + 1 < len; i++)
	{
		if (pattern[i] == '%' || pattern[i] == '_')
		{
			pfree(pattern);
			return false;
		}
	}
	if (len - 1 > 8)
	{
		pfree(pattern);
		return false;
	}
	memcpy(&prefix, pattern, len - 1);
	if (prefix_out != nullptr)
		*prefix_out = prefix;
	if (len_out != nullptr)
		*len_out = (uint32_t) (len - 1);
	pfree(pattern);
	return true;
}

static bool
TryExtractLikeContains(Const *c, char **str_out, uint32_t *len_out, uint64_t *prefix_out)
{
	char *pattern;
	size_t len;
	size_t inner_len;
	char *match;
	uint64_t prefix = 0;

	if (c == nullptr || c->constisnull)
		return false;
	if (c->consttype != TEXTOID &&
		c->consttype != VARCHAROID &&
		c->consttype != BPCHAROID)
		return false;

	pattern = TextDatumGetCString(c->constvalue);
	len = strlen(pattern);
	if (len < 2 || pattern[0] != '%' || pattern[len - 1] != '%')
	{
		pfree(pattern);
		return false;
	}
	for (size_t i = 1; i + 1 < len; i++)
	{
		if (pattern[i] == '%' || pattern[i] == '_')
		{
			pfree(pattern);
			return false;
		}
	}

	inner_len = len - 2;
	match = (char *) palloc(inner_len + 1);
	memcpy(match, pattern + 1, inner_len);
	match[inner_len] = '\0';
	if (inner_len > 0)
		memcpy(&prefix, match, inner_len > 8 ? 8 : inner_len);

	if (str_out != nullptr)
		*str_out = match;
	else
		pfree(match);
	if (len_out != nullptr)
		*len_out = (uint32_t) inner_len;
	if (prefix_out != nullptr)
		*prefix_out = prefix;
	pfree(pattern);
	return true;
}

VecExprProgram::VecExprProgram()
	: steps(PgMemoryContextAllocator<VecExprStep>(CurrentMemoryContext)),
	  max_reg_idx(0), final_res_idx(-1), jit_func(nullptr), jit_context(nullptr),
	  string_constants(PgMemoryContextAllocator<char>(CurrentMemoryContext))
{
	registers_i32 = (int32_t *) palloc(sizeof(int32_t) * MAX_REGISTERS * DEFAULT_CHUNK_SIZE);
	registers_i64 = (int64_t *) palloc(sizeof(int64_t) * MAX_REGISTERS * DEFAULT_CHUNK_SIZE);
	registers_f8 = (double *) palloc(sizeof(double) * MAX_REGISTERS * DEFAULT_CHUNK_SIZE);
	registers_nulls = (uint8_t *) palloc(sizeof(uint8_t) * MAX_REGISTERS * DEFAULT_CHUNK_SIZE);
	reset_register_scales();
}

VecExprProgram::~VecExprProgram()
{
	pfree(registers_i32);
	pfree(registers_i64);
	pfree(registers_f8);
	pfree(registers_nulls);
#ifdef USE_LLVM
	if (jit_context)
		pg_volvec_release_llvm_jit_context((JitContext *) jit_context);
#endif
}

void
VecExprProgram::try_compile_jit()
{
#ifdef USE_LLVM
	if (jit_func != nullptr || jit_context != nullptr)
		return;
	if (final_res_idx < 0)
		return;
	const char *fr = nullptr;
	if (!pg_volvec_try_compile_jit_expr(this, &jit_func, (JitContext **) &jit_context, &fr))
	{
		if (pg_volvec_trace_hooks)
		{
			elog(LOG, "pg_volvec: expr JIT compile skipped or failed (steps=%zu, reason=%s)",
				 steps.size(), fr != nullptr ? fr : "unknown");
			for (size_t i = 0; i < steps.size(); i++)
			{
				const auto &step = steps[i];

				elog(LOG,
					 "pg_volvec: expr JIT step[%zu] opcode=%s res=%d left=%d right=%d scale=%d",
					 i,
					 VecOpCodeName(step.opcode),
					 step.res_idx,
					 step.d.op.left,
					 step.d.op.right,
					 get_register_scale(step.res_idx));
			}
		}
		jit_func = nullptr;
	}
	else if (pg_volvec_trace_hooks)
		elog(LOG, "pg_volvec: expr JIT compiled successfully (steps=%zu, func=%p)",
			 steps.size(), (void *) jit_func);
#endif
}

static void
ApplyQualSelection(DataChunk<DEFAULT_CHUNK_SIZE> &chunk,
				   const uint8_t *nulls,
				   const int32_t *values)
{
	int count = 0;
	int n = chunk.has_selection ? chunk.sel.count : chunk.count;

	for (int s = 0; s < n; s++)
	{
		int row_idx = chunk.has_selection ? chunk.sel.row_ids[s] : s;

		if (!nulls[row_idx] && values[row_idx])
			chunk.sel.row_ids[count++] = row_idx;
	}

	chunk.sel.count = count;
	chunk.has_selection = (count < chunk.count);
}

static uint32_t
TrimBpcharLength(const char *data, uint32_t len)
{
	while (len > 0 && data[len - 1] == ' ')
		len--;
	return len;
}

static bool
StringConstMatches(const DataChunk<DEFAULT_CHUNK_SIZE> &chunk,
					const VecExprProgram &program,
					const VecStringRef &ref,
					const VecExprStep &step)
{
	uint32_t match_len = step.d.str_prefix.len;
	Oid string_type = step.d.str_prefix.type;
	uint64_t mask = 0;

	if (match_len > 0)
		mask = (match_len >= 8) ? UINT64_MAX : ((UINT64CONST(1) << (match_len * 8)) - 1);
	if (match_len > 0 && ((ref.prefix & mask) != (step.d.str_prefix.prefix & mask)))
		return false;

	if (string_type == BPCHAROID)
	{
		const char *lhs = chunk.get_string_ptr(ref);
		const char *rhs = (step.d.str_prefix.offset != UINT32_MAX) ?
			program.get_string_const_ptr(step.d.str_prefix.offset) : nullptr;
		uint32_t lhs_len = TrimBpcharLength(lhs, ref.len);
		uint32_t rhs_len;

		if (rhs == nullptr)
		{
			rhs_len = match_len;
			if (lhs_len != rhs_len)
				return false;
			return lhs_len == 0 ||
				memcmp(lhs, &step.d.str_prefix.prefix, lhs_len) == 0;
		}

		rhs_len = TrimBpcharLength(rhs, match_len);
		if (lhs_len != rhs_len)
			return false;
		return lhs_len == 0 || memcmp(lhs, rhs, lhs_len) == 0;
	}

	if (ref.len != match_len)
		return false;
	if (match_len <= 8 && step.d.str_prefix.offset == UINT32_MAX)
		return match_len == 0 || memcmp(chunk.get_string_ptr(ref), &step.d.str_prefix.prefix, match_len) == 0;
	return match_len == 0 ||
		memcmp(chunk.get_string_ptr(ref), program.get_string_const_ptr(step.d.str_prefix.offset), match_len) == 0;
}

static bool
StringConstContains(const DataChunk<DEFAULT_CHUNK_SIZE> &chunk,
					  const VecExprProgram &program,
					  const VecStringRef &ref,
					  const VecExprStep &step)
{
	uint32_t match_len = step.d.str_prefix.len;
	const char *haystack;
	const char *needle;

	if (ref.len < match_len)
		return false;
	if (match_len == 0)
		return true;

	haystack = chunk.get_string_ptr(ref);
	needle = (match_len <= 8 && step.d.str_prefix.offset == UINT32_MAX) ?
		(const char *) &step.d.str_prefix.prefix :
		program.get_string_const_ptr(step.d.str_prefix.offset);

	for (uint32_t pos = 0; pos + match_len <= ref.len; pos++)
	{
		if (memcmp(haystack + pos, needle, match_len) == 0)
			return true;
	}

	return false;
}

static bool
StringLikePatternMatches(const char *text, uint32_t text_len,
						 const char *pattern, uint32_t pattern_len)
{
	uint32_t text_pos = 0;
	uint32_t pattern_pos = 0;
	uint32_t star_pattern = UINT32_MAX;
	uint32_t star_text = 0;

	while (text_pos < text_len)
	{
		if (pattern_pos < pattern_len &&
			(pattern[pattern_pos] == '_' || pattern[pattern_pos] == text[text_pos]))
		{
			pattern_pos++;
			text_pos++;
			continue;
		}
		if (pattern_pos < pattern_len && pattern[pattern_pos] == '%')
		{
			star_pattern = pattern_pos++;
			star_text = text_pos;
			continue;
		}
		if (star_pattern != UINT32_MAX)
		{
			pattern_pos = star_pattern + 1;
			text_pos = ++star_text;
			continue;
		}
		return false;
	}

	while (pattern_pos < pattern_len && pattern[pattern_pos] == '%')
		pattern_pos++;
	return pattern_pos == pattern_len;
}

static bool
StringConstLikePattern(const DataChunk<DEFAULT_CHUNK_SIZE> &chunk,
					   const VecExprProgram &program,
					   const VecStringRef &ref,
					   const VecExprStep &step)
{
	const char *text = chunk.get_string_ptr(ref);
	uint32_t text_len = ref.len;
	const char *pattern;

	if (step.d.str_prefix.len == 0)
		return true;
	if (step.d.str_prefix.type == BPCHAROID)
		text_len = TrimBpcharLength(text, text_len);
	pattern = (step.d.str_prefix.len <= 8 && step.d.str_prefix.offset == UINT32_MAX) ?
		(const char *) &step.d.str_prefix.prefix :
		program.get_string_const_ptr(step.d.str_prefix.offset);
	return StringLikePatternMatches(text, text_len, pattern, step.d.str_prefix.len);
}

static int
PopulateConstStep(VecExprProgram &program,
				 VecExprStep *step,
				 Oid consttype,
				 int32 consttypmod,
				 Datum constvalue,
				 bool constisnull)
{
	if (step == nullptr)
		return -1;

	step->opcode = VecOpCode::EEOP_CONST;
	step->d.constant.isnull = constisnull;
	step->d.constant.fval = 0.0;
	step->d.constant.i64val = 0;
	step->d.constant.ival = 0;
	step->d.constant.has_wide_i128 = false;
	step->d.constant.wide_lo = 0;
	step->d.constant.wide_hi = 0;

	if (!constisnull)
	{
		if (consttype == FLOAT8OID)
		{
			step->d.constant.fval = DatumGetFloat8(constvalue);
		}
		else if (consttype == NUMERICOID)
		{
			int scale = DEFAULT_NUMERIC_SCALE;
			int exact_scale;
			double fval = 0.0;
			bool decoded = false;
			bool wide_decoded = false;
			NumericWideInt wide_value = 0;

			if (IsValidNumericTypmod(consttypmod))
				scale = ClampTrackedScale(GetNumericScaleFromTypmod(consttypmod));
			else if (DatumGetPointer(constvalue) != nullptr)
				scale = ClampTrackedScale(VolVecNumericDscale(DatumGetPointer(constvalue)));
			exact_scale = scale;
			if (TryFastNumericToScaledWideInt(constvalue, exact_scale, &wide_value))
			{
				wide_decoded = true;
				step->d.constant.has_wide_i128 = true;
				step->d.constant.wide_lo = WideIntLow64(wide_value);
				step->d.constant.wide_hi = WideIntHigh64(wide_value);
				if (WideIntFitsInt64(wide_value))
				{
					step->d.constant.i64val = (int64_t) wide_value;
					decoded = true;
				}
			}
			for (int candidate_scale = Min(exact_scale, 18); !wide_decoded && !decoded && candidate_scale >= 0; candidate_scale--)
			{
				if (!TryFastNumericToScaledInt64(constvalue, candidate_scale, &step->d.constant.i64val))
					continue;
				scale = candidate_scale;
				decoded = true;
				step->d.constant.has_wide_i128 = true;
				step->d.constant.wide_lo = WideIntLow64(WideIntFromInt64(step->d.constant.i64val));
				step->d.constant.wide_hi = WideIntHigh64(WideIntFromInt64(step->d.constant.i64val));
				break;
			}
			fval = DatumGetFloat8(DirectFunctionCall1(numeric_float8_no_overflow, constvalue));
			if (!decoded && !wide_decoded)
			{
				scale = 0;
				step->d.constant.i64val = ScaleFloatToInt64(fval, scale);
				if (!step->d.constant.has_wide_i128)
				{
					step->d.constant.has_wide_i128 = true;
					step->d.constant.wide_lo = WideIntLow64(WideIntFromInt64(step->d.constant.i64val));
					step->d.constant.wide_hi = WideIntHigh64(WideIntFromInt64(step->d.constant.i64val));
				}
			}
			if (wide_decoded)
				scale = exact_scale;
			step->d.constant.fval = fval;
			program.set_register_scale(step->res_idx, scale);
		}
		else if (consttype == INT8OID)
		{
			step->d.constant.i64val = DatumGetInt64(constvalue);
			step->d.constant.fval = (double) step->d.constant.i64val;
			step->d.constant.has_wide_i128 = true;
			step->d.constant.wide_lo = WideIntLow64(WideIntFromInt64(step->d.constant.i64val));
			step->d.constant.wide_hi = WideIntHigh64(WideIntFromInt64(step->d.constant.i64val));
		}
		else if (consttype == DATEOID)
		{
			step->d.constant.ival = DatumGetDateADT(constvalue);
		}
		else if (consttype == TIMESTAMPOID || consttype == TIMESTAMPTZOID)
		{
			step->d.constant.i64val = DatumGetInt64(constvalue);
		}
		else
		{
			step->d.constant.ival = DatumGetInt32(constvalue);
			step->d.constant.i64val = step->d.constant.ival;
			step->d.constant.fval = (double) step->d.constant.ival;
		}
	}

	return step->res_idx;
}

static int
CompileExprRecursive(Expr *expr, VecExprProgram &program, EState *estate)
{
	expr = StripImplicitNodes(expr);
	if (expr == nullptr)
		return -1;

	int res_idx = AllocateResultRegister(program);

	if (res_idx < 0)
		return -1;

	if (IsA(expr, Var))
	{
		Var *var = (Var *) expr;
		VecExprStep step;

		step.opcode = VecOpCode::EEOP_VAR;
		step.res_idx = res_idx;
		step.d.var.att_idx = var->varattno - 1;
		step.d.var.type = var->vartype;
		if (IsInt64LikeType(var->vartype))
			program.set_register_scale(res_idx, GetNumericScaleForVar(var));
		program.steps.push_back(step);
		return res_idx;
	}

	if (IsA(expr, Param))
	{
		Param *param = (Param *) expr;
		VecExprStep step;
		ParamExecData *prm;

		if (param->paramkind != PARAM_EXEC || estate == nullptr ||
			estate->es_param_exec_vals == nullptr || param->paramid < 0)
			return -1;
		prm = &estate->es_param_exec_vals[param->paramid];
		if (prm->execPlan != nullptr)
			return -1;

		step.res_idx = res_idx;
		if (PopulateConstStep(program, &step,
							  param->paramtype,
							  param->paramtypmod,
							  prm->value,
							  prm->isnull) < 0)
			return -1;
		program.steps.push_back(step);
		return res_idx;
	}

	if (IsA(expr, Const))
	{
		Const *c = (Const *) expr;
		VecExprStep step;

		step.res_idx = res_idx;
		if (PopulateConstStep(program, &step,
							  c->consttype,
							  c->consttypmod,
							  c->constvalue,
							  c->constisnull) < 0)
			return -1;
		program.steps.push_back(step);
		return res_idx;
	}

	if (IsA(expr, BoolExpr))
	{
		BoolExpr *bool_expr = (BoolExpr *) expr;
		ListCell *lc;
		int left;
		VecOpCode combine_opcode;

		if (bool_expr->args == NIL)
			return -1;
		if (bool_expr->boolop == NOT_EXPR)
		{
			int arg_idx;

			if (list_length(bool_expr->args) != 1)
				return -1;
			arg_idx = CompileExprRecursive((Expr *) linitial(bool_expr->args), program, estate);
			if (arg_idx < 0 || !AppendBoolNotStep(program, arg_idx, res_idx))
				return -1;
			return res_idx;
		}
		if (bool_expr->boolop == AND_EXPR)
			combine_opcode = VecOpCode::EEOP_AND;
		else if (bool_expr->boolop == OR_EXPR)
			combine_opcode = VecOpCode::EEOP_OR;
		else
			return -1;

		lc = list_head(bool_expr->args);
		left = CompileExprRecursive((Expr *) lfirst(lc), program, estate);
		if (left < 0)
			return -1;
		if (lnext(bool_expr->args, lc) == nullptr)
			return left;

		for_each_from(lc, bool_expr->args, 1)
		{
			int right = CompileExprRecursive((Expr *) lfirst(lc), program, estate);

			if (right < 0)
				return -1;
			if (!AppendBoolCombineStep(program, combine_opcode, left, right, res_idx))
				return -1;
			left = res_idx;
		}

		return res_idx;
	}

	if (IsA(expr, CaseExpr))
	{
		CaseExpr *case_expr = (CaseExpr *) expr;
		CaseWhen *when_clause;
		int cond_idx;
		int true_idx;
		int false_idx;
		VecExprStep step;

		if (case_expr->arg != nullptr || list_length(case_expr->args) != 1 || case_expr->defresult == nullptr)
			return -1;

		when_clause = (CaseWhen *) linitial(case_expr->args);
		cond_idx = CompileExprRecursive((Expr *) when_clause->expr, program, estate);
		true_idx = CompileExprRecursive((Expr *) when_clause->result, program, estate);
		false_idx = CompileExprRecursive((Expr *) case_expr->defresult, program, estate);
		if (cond_idx < 0 || true_idx < 0 || false_idx < 0)
			return -1;

		step.res_idx = res_idx;
		step.d.ternary.cond = cond_idx;
		step.d.ternary.if_true = true_idx;
		step.d.ternary.if_false = false_idx;
		if (IsInt64LikeType(exprType((Node *) when_clause->result)) &&
			IsInt64LikeType(exprType((Node *) case_expr->defresult)))
		{
			step.opcode = VecOpCode::EEOP_INT64_CASE;
			program.set_register_scale(res_idx,
				Max(program.get_register_scale(true_idx),
					program.get_register_scale(false_idx)));
		}
		else if (IsIntegerType(exprType((Node *) when_clause->result)) &&
				 IsIntegerType(exprType((Node *) case_expr->defresult)))
		{
			step.opcode = VecOpCode::EEOP_INT64_CASE;
			program.set_register_scale(res_idx, 0);
		}
		else if (exprType((Node *) when_clause->result) == FLOAT8OID &&
				 exprType((Node *) case_expr->defresult) == FLOAT8OID)
		{
			step.opcode = VecOpCode::EEOP_FLOAT8_CASE;
		}
		else
			return -1;
		program.steps.push_back(step);
		return res_idx;
	}

	if (IsA(expr, ScalarArrayOpExpr))
		return CompileScalarArrayExpr((ScalarArrayOpExpr *) expr, program, res_idx, estate);

	if (IsA(expr, FuncExpr))
	{
		FuncExpr *func = (FuncExpr *) expr;
		char *funcname = get_func_name(func->funcid);
		Expr *field_expr;
		Expr *value_expr;
		int arg;
		VecExprStep step;

		if (funcname == nullptr || list_length(func->args) != 2)
			return -1;
		field_expr = StripImplicitNodes((Expr *) linitial(func->args));
		value_expr = StripImplicitNodes((Expr *) lsecond(func->args));
		if ((strcmp(funcname, "date_part") != 0 && strcmp(funcname, "extract") != 0) ||
			field_expr == nullptr || value_expr == nullptr ||
			!IsA(field_expr, Const) ||
			!TryExtractYearFieldConst((Const *) field_expr) ||
			exprType((Node *) value_expr) != DATEOID)
		{
			if (pg_volvec_trace_hooks)
				elog(LOG,
					 "pg_volvec: unsupported FuncExpr lowering (func=%s nargs=%d field_node=%d value_type=%u result_type=%u)",
					 funcname,
					 list_length(func->args),
					 field_expr != nullptr ? (int) nodeTag(field_expr) : -1,
					 value_expr != nullptr ? exprType((Node *) value_expr) : InvalidOid,
					 exprType((Node *) expr));
			return -1;
		}

		arg = CompileExprRecursive(value_expr, program, estate);
		if (arg < 0)
			return -1;
		step.opcode = VecOpCode::EEOP_DATE_PART_YEAR;
		step.res_idx = res_idx;
		step.d.op.left = arg;
		step.d.op.right = 0;
		program.set_register_scale(res_idx, 0);
		program.steps.push_back(step);
		return res_idx;
	}

	if (IsA(expr, OpExpr))
	{
		OpExpr *op = (OpExpr *) expr;
		Expr *left_expr;
		Expr *right_expr;
		Oid left_type;
		Oid right_type;
		char *opname;
		int left;
		int right;
		VecExprStep step;

		if (list_length(op->args) != 2)
			return -1;

		left_expr = StripImplicitNodes((Expr *) linitial(op->args));
		right_expr = StripImplicitNodes((Expr *) lsecond(op->args));
		left_type = exprType((Node *) left_expr);
		right_type = exprType((Node *) right_expr);
		opname = get_opname(op->opno);

		if (AppendStringCompareStep(program, res_idx, left_expr, right_expr, opname))
			return res_idx;
		if (opname != nullptr &&
			strcmp(opname, "!~~") == 0)
		{
			int like_reg = AllocateResultRegister(program);

			if (like_reg < 0)
				return -1;
			if (!AppendStringCompareStep(program, like_reg, left_expr, right_expr, "~~") ||
				!AppendBoolNotStep(program, like_reg, res_idx))
				return -1;
			return res_idx;
		}

		if (left_type == DATEOID &&
			IsDateLikeType(right_type) &&
			IsA(right_expr, Const))
		{
			int32_t right_date = 0;

			left = CompileExprRecursive(left_expr, program, estate);
			if (left < 0 || !TryConvertConstToDate32((Const *) right_expr, &right_date))
				return -1;

			right = AppendDateConstStep(program, right_date);
			if (right < 0)
				return -1;

			step.res_idx = res_idx;
			step.d.op.left = left;
			step.d.op.right = right;
			if (strcmp(opname, "<") == 0)
				step.opcode = VecOpCode::EEOP_DATE_LT;
			else if (strcmp(opname, "<=") == 0)
				step.opcode = VecOpCode::EEOP_DATE_LE;
			else if (strcmp(opname, ">") == 0)
				step.opcode = VecOpCode::EEOP_DATE_GT;
			else if (strcmp(opname, ">=") == 0)
				step.opcode = VecOpCode::EEOP_DATE_GE;
			else
				return -1;

			program.steps.push_back(step);
			return res_idx;
		}

		left = CompileExprRecursive(left_expr, program, estate);
		right = CompileExprRecursive(right_expr, program, estate);
		if (left < 0 || right < 0)
			return -1;

		step.res_idx = res_idx;
		step.d.op.left = left;
		step.d.op.right = right;
		if ((strcmp(opname, "+") == 0 ||
			 strcmp(opname, "-") == 0 ||
			 strcmp(opname, "*") == 0) &&
			(ExprProducesFloat8Result(left_expr) || ExprProducesFloat8Result(right_expr)) &&
			((left_type == FLOAT8OID || IsInt64LikeType(left_type)) &&
			 (right_type == FLOAT8OID || IsInt64LikeType(right_type))))
		{
			if (strcmp(opname, "+") == 0)
				step.opcode = VecOpCode::EEOP_FLOAT8_ADD;
			else if (strcmp(opname, "-") == 0)
				step.opcode = VecOpCode::EEOP_FLOAT8_SUB;
			else
				step.opcode = VecOpCode::EEOP_FLOAT8_MUL;
		}
		else if (!ResolveBinaryOpcode(opname, left_type, right_type, &step.opcode))
			return -1;

		program.set_register_scale(
			res_idx,
			ResolveResultScale(step.opcode,
							   program.get_register_scale(left),
							   program.get_register_scale(right)));
		program.steps.push_back(step);
		return res_idx;
	}

	return -1;
}

void
CompileExpr(Expr *expr, VecExprProgram &program, bool is_filter, EState *estate)
{
	program.steps.clear();
	program.max_reg_idx = 0;
	program.reset_register_scales();
	program.clear_string_consts();

	int final_res = CompileExprRecursive(expr, program, estate);
	if (final_res < 0)
	{
		program.steps.clear();
		program.max_reg_idx = 0;
		program.final_res_idx = -1;
		program.clear_string_consts();
		return;
	}

	program.final_res_idx = final_res;
	if (is_filter)
	{
		VecExprStep step;

		step.opcode = VecOpCode::EEOP_QUAL;
		step.res_idx = final_res;
		program.steps.push_back(step);
	}

	program.try_compile_jit();
}

void
VecExprProgram::evaluate(DataChunk<DEFAULT_CHUNK_SIZE> &chunk)
{
	const VecExprStep *reg_defs[MAX_REGISTERS] = {0};

	for (const auto &step : steps)
	{
		if (step.res_idx >= 0 && step.res_idx < MAX_REGISTERS)
			reg_defs[step.res_idx] = &step;
	}

	if (jit_func)
	{
		uint32_t active_count = chunk.has_selection ? chunk.sel.count : chunk.count;
		double *col_f8[16];
		int64_t *col_i64[16];
		int32_t *col_i32[16];
		VecStringRef *col_str[16];
		uint8_t *col_nulls[16];

		chunk.get_double_ptrs(col_f8);
		chunk.get_int64_ptrs(col_i64);
		chunk.get_int32_ptrs(col_i32);
		chunk.get_string_ptrs(col_str);
		chunk.get_null_ptrs(col_nulls);
		jit_func(active_count, col_f8, col_i64, col_i32, col_str, col_nulls,
				 chunk.string_arena.data(),
				 &registers_f8[final_res_idx * DEFAULT_CHUNK_SIZE],
				 &registers_i64[final_res_idx * DEFAULT_CHUNK_SIZE],
				 &registers_i32[final_res_idx * DEFAULT_CHUNK_SIZE],
				 &registers_nulls[final_res_idx * DEFAULT_CHUNK_SIZE],
				 chunk.sel.row_ids, chunk.has_selection);

		if (!steps.empty() && steps.back().opcode == VecOpCode::EEOP_QUAL)
		{
			uint8_t *rn = &registers_nulls[final_res_idx * DEFAULT_CHUNK_SIZE];
			int32_t *r32 = &registers_i32[final_res_idx * DEFAULT_CHUNK_SIZE];

			ApplyQualSelection(chunk, rn, r32);
		}
		return;
	}

	for (const auto &step : steps)
	{
		int res = step.res_idx * DEFAULT_CHUNK_SIZE;
		int l = step.d.op.left * DEFAULT_CHUNK_SIZE;
		int r = step.d.op.right * DEFAULT_CHUNK_SIZE;
		int left_scale = get_register_scale(step.d.op.left);
		int right_scale = get_register_scale(step.d.op.right);
		int res_scale = get_register_scale(step.res_idx);

		switch (step.opcode)
		{
			case VecOpCode::EEOP_VAR:
			{
				int att = step.d.var.att_idx;
				Oid typ = step.d.var.type;

				for (int i = 0; i < chunk.count; i++)
				{
					registers_nulls[res + i] = chunk.nulls[att][i];
					if (typ == FLOAT8OID)
						registers_f8[res + i] = chunk.double_columns[att][i];
					else if (typ == NUMERICOID || typ == INT8OID)
						registers_i64[res + i] = chunk.int64_columns[att][i];
					else
					{
						registers_i32[res + i] = chunk.int32_columns[att][i];
						if (IsIntegerType(typ))
							registers_i64[res + i] = (int64_t) chunk.int32_columns[att][i];
					}
				}
				break;
			}
			case VecOpCode::EEOP_CONST:
				for (int i = 0; i < chunk.count; i++)
				{
					registers_nulls[res + i] = (uint8_t) step.d.constant.isnull;
					registers_f8[res + i] = step.d.constant.fval;
					registers_i64[res + i] = step.d.constant.i64val;
					registers_i32[res + i] = step.d.constant.ival;
				}
				break;
			case VecOpCode::EEOP_FLOAT8_ADD:
				for (int i = 0; i < chunk.count; i++)
				{
					registers_nulls[res + i] = registers_nulls[l + i] || registers_nulls[r + i];
					registers_f8[res + i] = registers_f8[l + i] + registers_f8[r + i];
				}
				break;
			case VecOpCode::EEOP_FLOAT8_SUB:
				for (int i = 0; i < chunk.count; i++)
				{
					registers_nulls[res + i] = registers_nulls[l + i] || registers_nulls[r + i];
					registers_f8[res + i] = registers_f8[l + i] - registers_f8[r + i];
				}
				break;
			case VecOpCode::EEOP_FLOAT8_MUL:
				for (int i = 0; i < chunk.count; i++)
				{
					registers_nulls[res + i] = registers_nulls[l + i] || registers_nulls[r + i];
					registers_f8[res + i] = registers_f8[l + i] * registers_f8[r + i];
				}
				break;
			case VecOpCode::EEOP_INT64_ADD:
				for (int i = 0; i < chunk.count; i++)
				{
					NumericWideInt left_val;
					NumericWideInt right_val;

					registers_nulls[res + i] = registers_nulls[l + i] || registers_nulls[r + i];
					left_val = WideIntFromInt64(RescaleInt64Value(registers_i64[l + i], left_scale, res_scale));
					right_val = WideIntFromInt64(RescaleInt64Value(registers_i64[r + i], right_scale, res_scale));
					registers_i64[res + i] = WideIntToInt64Checked(left_val + right_val,
						"numeric add result");
				}
				break;
			case VecOpCode::EEOP_INT64_SUB:
				for (int i = 0; i < chunk.count; i++)
				{
					NumericWideInt left_val;
					NumericWideInt right_val;

					registers_nulls[res + i] = registers_nulls[l + i] || registers_nulls[r + i];
					left_val = WideIntFromInt64(RescaleInt64Value(registers_i64[l + i], left_scale, res_scale));
					right_val = WideIntFromInt64(RescaleInt64Value(registers_i64[r + i], right_scale, res_scale));
					registers_i64[res + i] = WideIntToInt64Checked(left_val - right_val,
						"numeric subtract result");
				}
				break;
			case VecOpCode::EEOP_INT64_MUL:
				for (int i = 0; i < chunk.count; i++)
				{
					NumericWideInt product;

					registers_nulls[res + i] = registers_nulls[l + i] || registers_nulls[r + i];
					product = WideIntMul(WideIntFromInt64(registers_i64[l + i]),
										 WideIntFromInt64(registers_i64[r + i]));
					registers_i64[res + i] = WideIntToInt64Checked(product,
						"numeric multiply result");
				}
				break;
			case VecOpCode::EEOP_INT64_DIV_FLOAT8:
				for (int i = 0; i < chunk.count; i++)
				{
					double left_val;
					double right_val;

					registers_nulls[res + i] = registers_nulls[l + i] || registers_nulls[r + i];
					if (registers_nulls[res + i])
						continue;
					left_val = (double) registers_i64[l + i] / (double) Pow10Int64(left_scale);
					right_val = (double) registers_i64[r + i] / (double) Pow10Int64(right_scale);
					if (right_val == 0.0)
						elog(ERROR, "pg_volvec numeric division by zero");
					registers_f8[res + i] = left_val / right_val;
				}
				break;
			case VecOpCode::EEOP_INT64_LT:
				for (int i = 0; i < chunk.count; i++)
				{
					int scale = Max(left_scale, right_scale);
					NumericWideInt left_val;
					NumericWideInt right_val;

					registers_nulls[res + i] = registers_nulls[l + i] || registers_nulls[r + i];
					left_val = RescaleOperandForCompare(reg_defs[step.d.op.left], registers_i64[l + i], left_scale, scale);
					right_val = RescaleOperandForCompare(reg_defs[step.d.op.right], registers_i64[r + i], right_scale, scale);
					registers_i32[res + i] = left_val < right_val;
				}
				break;
			case VecOpCode::EEOP_INT64_LE:
				for (int i = 0; i < chunk.count; i++)
				{
					int scale = Max(left_scale, right_scale);
					NumericWideInt left_val;
					NumericWideInt right_val;

					registers_nulls[res + i] = registers_nulls[l + i] || registers_nulls[r + i];
					left_val = RescaleOperandForCompare(reg_defs[step.d.op.left], registers_i64[l + i], left_scale, scale);
					right_val = RescaleOperandForCompare(reg_defs[step.d.op.right], registers_i64[r + i], right_scale, scale);
					registers_i32[res + i] = left_val <= right_val;
				}
				break;
			case VecOpCode::EEOP_FLOAT8_LT:
				for (int i = 0; i < chunk.count; i++)
				{
					registers_nulls[res + i] = registers_nulls[l + i] || registers_nulls[r + i];
					registers_i32[res + i] = (registers_f8[l + i] < registers_f8[r + i]);
				}
				break;
			case VecOpCode::EEOP_FLOAT8_LE:
				for (int i = 0; i < chunk.count; i++)
				{
					registers_nulls[res + i] = registers_nulls[l + i] || registers_nulls[r + i];
					registers_i32[res + i] = (registers_f8[l + i] <= registers_f8[r + i]);
				}
				break;
			case VecOpCode::EEOP_FLOAT8_GT:
				for (int i = 0; i < chunk.count; i++)
				{
					registers_nulls[res + i] = registers_nulls[l + i] || registers_nulls[r + i];
					registers_i32[res + i] = (registers_f8[l + i] > registers_f8[r + i]);
				}
				break;
			case VecOpCode::EEOP_FLOAT8_GE:
				for (int i = 0; i < chunk.count; i++)
				{
					registers_nulls[res + i] = registers_nulls[l + i] || registers_nulls[r + i];
					registers_i32[res + i] = (registers_f8[l + i] >= registers_f8[r + i]);
				}
				break;
			case VecOpCode::EEOP_DATE_LE:
				for (int i = 0; i < chunk.count; i++)
				{
					registers_nulls[res + i] = registers_nulls[l + i] || registers_nulls[r + i];
					registers_i32[res + i] = (registers_i32[l + i] <= registers_i32[r + i]);
				}
				break;
			case VecOpCode::EEOP_DATE_LT:
				for (int i = 0; i < chunk.count; i++)
				{
					registers_nulls[res + i] = registers_nulls[l + i] || registers_nulls[r + i];
					registers_i32[res + i] = (registers_i32[l + i] < registers_i32[r + i]);
				}
				break;
			case VecOpCode::EEOP_DATE_GT:
				for (int i = 0; i < chunk.count; i++)
				{
					registers_nulls[res + i] = registers_nulls[l + i] || registers_nulls[r + i];
					registers_i32[res + i] = (registers_i32[l + i] > registers_i32[r + i]);
				}
				break;
			case VecOpCode::EEOP_DATE_GE:
				for (int i = 0; i < chunk.count; i++)
				{
					registers_nulls[res + i] = registers_nulls[l + i] || registers_nulls[r + i];
					registers_i32[res + i] = (registers_i32[l + i] >= registers_i32[r + i]);
				}
				break;
			case VecOpCode::EEOP_DATE_PART_YEAR:
				for (int i = 0; i < chunk.count; i++)
				{
					registers_nulls[res + i] = registers_nulls[l + i];
					if (!registers_nulls[res + i])
						registers_i64[res + i] = ExtractYearFromDate32(registers_i32[l + i]);
				}
				break;
			case VecOpCode::EEOP_INT64_GT:
				for (int i = 0; i < chunk.count; i++)
				{
					int scale = Max(left_scale, right_scale);
					NumericWideInt left_val;
					NumericWideInt right_val;

					registers_nulls[res + i] = registers_nulls[l + i] || registers_nulls[r + i];
					left_val = RescaleOperandForCompare(reg_defs[step.d.op.left], registers_i64[l + i], left_scale, scale);
					right_val = RescaleOperandForCompare(reg_defs[step.d.op.right], registers_i64[r + i], right_scale, scale);
					registers_i32[res + i] = left_val > right_val;
				}
				break;
			case VecOpCode::EEOP_INT64_GE:
				for (int i = 0; i < chunk.count; i++)
				{
					int scale = Max(left_scale, right_scale);
					NumericWideInt left_val;
					NumericWideInt right_val;

					registers_nulls[res + i] = registers_nulls[l + i] || registers_nulls[r + i];
					left_val = RescaleOperandForCompare(reg_defs[step.d.op.left], registers_i64[l + i], left_scale, scale);
					right_val = RescaleOperandForCompare(reg_defs[step.d.op.right], registers_i64[r + i], right_scale, scale);
					registers_i32[res + i] = left_val >= right_val;
				}
				break;
			case VecOpCode::EEOP_INT64_EQ:
			case VecOpCode::EEOP_INT64_NE:
				for (int i = 0; i < chunk.count; i++)
				{
					int scale = Max(left_scale, right_scale);
					NumericWideInt left_val;
					NumericWideInt right_val;

					registers_nulls[res + i] = registers_nulls[l + i] || registers_nulls[r + i];
					left_val = RescaleOperandForCompare(reg_defs[step.d.op.left], registers_i64[l + i], left_scale, scale);
					right_val = RescaleOperandForCompare(reg_defs[step.d.op.right], registers_i64[r + i], right_scale, scale);
					registers_i32[res + i] =
						(step.opcode == VecOpCode::EEOP_INT64_EQ) ?
						(left_val == right_val) :
						(left_val != right_val);
				}
				break;
			case VecOpCode::EEOP_AND:
				for (int i = 0; i < chunk.count; i++)
				{
					EvalBoolAnd(registers_nulls[l + i], registers_i32[l + i],
								registers_nulls[r + i], registers_i32[r + i],
								&registers_nulls[res + i], &registers_i32[res + i]);
				}
				break;
			case VecOpCode::EEOP_OR:
				for (int i = 0; i < chunk.count; i++)
				{
					EvalBoolOr(registers_nulls[l + i], registers_i32[l + i],
							   registers_nulls[r + i], registers_i32[r + i],
							   &registers_nulls[res + i], &registers_i32[res + i]);
				}
				break;
			case VecOpCode::EEOP_NOT:
				for (int i = 0; i < chunk.count; i++)
				{
					registers_nulls[res + i] = registers_nulls[l + i];
					registers_i32[res + i] =
						registers_nulls[l + i] ? 0 : (registers_i32[l + i] == 0);
				}
				break;
			case VecOpCode::EEOP_INT64_CASE:
			{
				int c = step.d.ternary.cond * DEFAULT_CHUNK_SIZE;
				int t = step.d.ternary.if_true * DEFAULT_CHUNK_SIZE;
				int f = step.d.ternary.if_false * DEFAULT_CHUNK_SIZE;
				int true_scale = get_register_scale(step.d.ternary.if_true);
				int false_scale = get_register_scale(step.d.ternary.if_false);

				for (int i = 0; i < chunk.count; i++)
				{
					bool cond_null = registers_nulls[c + i] != 0;
					bool take_true = (!cond_null && registers_i32[c + i] != 0);
					int src = take_true ? t : f;
					int src_scale = take_true ? true_scale : false_scale;

					registers_nulls[res + i] = registers_nulls[src + i];
					registers_i64[res + i] =
						RescaleInt64Value(registers_i64[src + i], src_scale, res_scale);
				}
				break;
			}
			case VecOpCode::EEOP_FLOAT8_CASE:
			{
				int c = step.d.ternary.cond * DEFAULT_CHUNK_SIZE;
				int t = step.d.ternary.if_true * DEFAULT_CHUNK_SIZE;
				int f = step.d.ternary.if_false * DEFAULT_CHUNK_SIZE;

				for (int i = 0; i < chunk.count; i++)
				{
					bool cond_null = registers_nulls[c + i] != 0;
					bool take_true = (!cond_null && registers_i32[c + i] != 0);
					int src = take_true ? t : f;

					registers_nulls[res + i] = registers_nulls[src + i];
					registers_f8[res + i] = registers_f8[src + i];
				}
				break;
			}
				case VecOpCode::EEOP_STR_PREFIX_LIKE:
				{
					int att = step.d.str_prefix.att_idx;
					uint32_t prefix_len = step.d.str_prefix.len;
					uint64_t mask = 0;

				if (prefix_len > 0)
					mask = (prefix_len >= 8) ? UINT64_MAX : ((UINT64CONST(1) << (prefix_len * 8)) - 1);
				for (int i = 0; i < chunk.count; i++)
				{
					VecStringRef ref = chunk.string_columns[att][i];

					registers_nulls[res + i] = chunk.nulls[att][i];
					registers_i32[res + i] =
						(!registers_nulls[res + i] &&
							 ref.len >= prefix_len &&
							 (prefix_len == 0 || ((ref.prefix & mask) == (step.d.str_prefix.prefix & mask))));
					}
					break;
				}
			case VecOpCode::EEOP_STR_CONTAINS_LIKE:
				{
					int att = step.d.str_prefix.att_idx;

					for (int i = 0; i < chunk.count; i++)
					{
						VecStringRef ref = chunk.string_columns[att][i];

						registers_nulls[res + i] = chunk.nulls[att][i];
						registers_i32[res + i] = !registers_nulls[res + i] &&
							StringConstContains(chunk, *this, ref, step);
					}
					break;
				}
			case VecOpCode::EEOP_STR_LIKE_PATTERN:
				{
					int att = step.d.str_prefix.att_idx;

					for (int i = 0; i < chunk.count; i++)
					{
						VecStringRef ref = chunk.string_columns[att][i];

						registers_nulls[res + i] = chunk.nulls[att][i];
						registers_i32[res + i] = !registers_nulls[res + i] &&
							StringConstLikePattern(chunk, *this, ref, step);
					}
					break;
				}
			case VecOpCode::EEOP_STR_EQ:
				{
					int att = step.d.str_prefix.att_idx;

					for (int i = 0; i < chunk.count; i++)
					{
						VecStringRef ref = chunk.string_columns[att][i];

						registers_nulls[res + i] = chunk.nulls[att][i];
						registers_i32[res + i] = !registers_nulls[res + i] &&
							StringConstMatches(chunk, *this, ref, step);
					}
					break;
				}
				case VecOpCode::EEOP_STR_NE:
				{
					int att = step.d.str_prefix.att_idx;

					for (int i = 0; i < chunk.count; i++)
					{
						VecStringRef ref = chunk.string_columns[att][i];

						registers_nulls[res + i] = chunk.nulls[att][i];
						registers_i32[res + i] = registers_nulls[res + i] ? 0 :
							!StringConstMatches(chunk, *this, ref, step);
					}
					break;
				}
				case VecOpCode::EEOP_QUAL:
					ApplyQualSelection(chunk, &registers_nulls[res], &registers_i32[res]);
					break;
			default:
				break;
		}
	}
}

} /* namespace pg_volvec */

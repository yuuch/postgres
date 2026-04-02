#include "volvec_engine.hpp"

#include <cmath>

extern "C" {
#include "nodes/nodeFuncs.h"
#include "nodes/primnodes.h"
#include "utils/date.h"
#include "utils/lsyscache.h"
#include "utils/timestamp.h"
}

namespace pg_volvec
{

static Expr *
StripImplicitNodes(Expr *expr)
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
IsInt64LikeType(Oid type)
{
	return type == NUMERICOID || type == INT8OID;
}

static bool
IsDateLikeType(Oid type)
{
	return type == DATEOID || type == TIMESTAMPOID || type == TIMESTAMPTZOID;
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
	step.d.constant.ival = date_val;
	step.d.constant.i64val = (int64_t) date_val;
	step.d.constant.fval = (double) date_val;
	program.set_register_scale(res_idx, 0);
	program.steps.push_back(step);
	return res_idx;
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
		else return false;
		return true;
	}

	if (left_type == DATEOID && right_type == DATEOID)
	{
		if (strcmp(opname, "<") == 0) *opcode = VecOpCode::EEOP_DATE_LT;
		else if (strcmp(opname, "<=") == 0) *opcode = VecOpCode::EEOP_DATE_LE;
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

VecExprProgram::VecExprProgram()
	: steps(PgMemoryContextAllocator<VecExprStep>(CurrentMemoryContext)),
	  max_reg_idx(0), final_res_idx(-1), jit_func(nullptr), jit_context(nullptr)
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
		jit_func = nullptr;
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

static int
CompileExprRecursive(Expr *expr, VecExprProgram &program)
{
	expr = StripImplicitNodes(expr);
	if (expr == nullptr)
		return -1;

	int res_idx = program.max_reg_idx++;
	if (res_idx >= MAX_REGISTERS)
		return -1;
	program.set_register_scale(res_idx, 0);

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

	if (IsA(expr, Const))
	{
		Const *c = (Const *) expr;
		VecExprStep step;

		step.opcode = VecOpCode::EEOP_CONST;
		step.res_idx = res_idx;
		step.d.constant.isnull = c->constisnull;
		step.d.constant.fval = 0.0;
		step.d.constant.i64val = 0;
		step.d.constant.ival = 0;

		if (!c->constisnull)
		{
			if (c->consttype == FLOAT8OID)
			{
				step.d.constant.fval = DatumGetFloat8(c->constvalue);
			}
			else if (c->consttype == NUMERICOID)
			{
				int scale = GetNumericScaleForConst(c);
				if (!TryFastNumericToScaledInt64(c->constvalue, scale, &step.d.constant.i64val))
				{
					double fval = DatumGetFloat8(DirectFunctionCall1(numeric_float8_no_overflow, c->constvalue));
					step.d.constant.i64val = ScaleFloatToInt64(fval, scale);
					step.d.constant.fval = fval;
				}
				else
					step.d.constant.fval = (double) step.d.constant.i64val / (double) Pow10Int64(scale);
				program.set_register_scale(res_idx, scale);
			}
			else if (c->consttype == INT8OID)
			{
				step.d.constant.i64val = DatumGetInt64(c->constvalue);
				step.d.constant.fval = (double) step.d.constant.i64val;
			}
			else if (c->consttype == DATEOID)
			{
				step.d.constant.ival = DatumGetDateADT(c->constvalue);
			}
			else if (c->consttype == TIMESTAMPOID || c->consttype == TIMESTAMPTZOID)
			{
				step.d.constant.i64val = DatumGetInt64(c->constvalue);
			}
			else
			{
				step.d.constant.ival = DatumGetInt32(c->constvalue);
				step.d.constant.i64val = step.d.constant.ival;
				step.d.constant.fval = (double) step.d.constant.ival;
			}
		}

		program.steps.push_back(step);
		return res_idx;
	}

	if (IsA(expr, BoolExpr))
	{
		BoolExpr *bool_expr = (BoolExpr *) expr;
		ListCell *lc;
		int left;

		if (bool_expr->boolop != AND_EXPR || bool_expr->args == NIL)
			return -1;

		lc = list_head(bool_expr->args);
		left = CompileExprRecursive((Expr *) lfirst(lc), program);
		if (left < 0)
			return -1;

		for_each_from(lc, bool_expr->args, 1)
		{
			int right = CompileExprRecursive((Expr *) lfirst(lc), program);
			VecExprStep step;

			if (right < 0)
				return -1;

			step.opcode = VecOpCode::EEOP_AND;
			step.res_idx = res_idx;
			step.d.op.left = left;
			step.d.op.right = right;
			program.steps.push_back(step);
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
		cond_idx = CompileExprRecursive((Expr *) when_clause->expr, program);
		true_idx = CompileExprRecursive((Expr *) when_clause->result, program);
		false_idx = CompileExprRecursive((Expr *) case_expr->defresult, program);
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

		if ((strcmp(opname, "~~") == 0 || strcmp(opname, "=") == 0) &&
			IsA(left_expr, Var) &&
			IsA(right_expr, Const) &&
			(left_type == BPCHAROID || left_type == TEXTOID || left_type == VARCHAROID))
		{
			VecExprStep special_step;
			uint64_t prefix = 0;
			uint32_t len = 0;
			bool ok;

			special_step.res_idx = res_idx;
			special_step.d.str_prefix.att_idx = ((Var *) left_expr)->varattno - 1;
			special_step.d.str_prefix.prefix = 0;
			special_step.d.str_prefix.len = 0;
			if (strcmp(opname, "~~") == 0)
			{
				ok = TryExtractLikePrefix((Const *) right_expr, &prefix, &len);
				special_step.opcode = VecOpCode::EEOP_STR_PREFIX_LIKE;
			}
			else
				ok = false;
			if (!ok)
				return -1;
			special_step.d.str_prefix.prefix = prefix;
			special_step.d.str_prefix.len = len;
			program.steps.push_back(special_step);
			return res_idx;
		}

		if (left_type == DATEOID &&
			IsDateLikeType(right_type) &&
			IsA(right_expr, Const))
		{
			int32_t right_date = 0;

			left = CompileExprRecursive(left_expr, program);
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
			else if (strcmp(opname, ">=") == 0)
				step.opcode = VecOpCode::EEOP_DATE_GE;
			else if (strcmp(opname, ">") == 0)
			{
				step.opcode = VecOpCode::EEOP_DATE_LT;
				step.d.op.left = right;
				step.d.op.right = left;
			}
			else
				return -1;

			program.steps.push_back(step);
			return res_idx;
		}

		left = CompileExprRecursive(left_expr, program);
		right = CompileExprRecursive(right_expr, program);
		if (left < 0 || right < 0)
			return -1;

		step.res_idx = res_idx;
		step.d.op.left = left;
		step.d.op.right = right;
		if (!ResolveBinaryOpcode(opname, left_type, right_type, &step.opcode))
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
CompileExpr(Expr *expr, VecExprProgram &program, bool is_filter)
{
	program.steps.clear();
	program.max_reg_idx = 0;
	program.reset_register_scales();

	int final_res = CompileExprRecursive(expr, program);
	if (final_res < 0)
	{
		program.steps.clear();
		program.max_reg_idx = 0;
		program.final_res_idx = -1;
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
	if (jit_func)
	{
		uint32_t active_count = chunk.has_selection ? chunk.sel.count : chunk.count;
		double *col_f8[16];
		int64_t *col_i64[16];
		int32_t *col_i32[16];
		uint8_t *col_nulls[16];

		chunk.get_double_ptrs(col_f8);
		chunk.get_int64_ptrs(col_i64);
		chunk.get_int32_ptrs(col_i32);
		chunk.get_null_ptrs(col_nulls);
		jit_func(active_count, col_f8, col_i64, col_i32, col_nulls,
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
						registers_i32[res + i] = chunk.int32_columns[att][i];
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

					registers_nulls[res + i] = registers_nulls[l + i] || registers_nulls[r + i];
					registers_i32[res + i] =
						RescaleInt64Value(registers_i64[l + i], left_scale, scale) <
						RescaleInt64Value(registers_i64[r + i], right_scale, scale);
				}
				break;
			case VecOpCode::EEOP_INT64_LE:
				for (int i = 0; i < chunk.count; i++)
				{
					int scale = Max(left_scale, right_scale);

					registers_nulls[res + i] = registers_nulls[l + i] || registers_nulls[r + i];
					registers_i32[res + i] =
						RescaleInt64Value(registers_i64[l + i], left_scale, scale) <=
						RescaleInt64Value(registers_i64[r + i], right_scale, scale);
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
			case VecOpCode::EEOP_DATE_GE:
				for (int i = 0; i < chunk.count; i++)
				{
					registers_nulls[res + i] = registers_nulls[l + i] || registers_nulls[r + i];
					registers_i32[res + i] = (registers_i32[l + i] >= registers_i32[r + i]);
				}
				break;
			case VecOpCode::EEOP_INT64_GT:
				for (int i = 0; i < chunk.count; i++)
				{
					int scale = Max(left_scale, right_scale);

					registers_nulls[res + i] = registers_nulls[l + i] || registers_nulls[r + i];
					registers_i32[res + i] =
						RescaleInt64Value(registers_i64[l + i], left_scale, scale) >
						RescaleInt64Value(registers_i64[r + i], right_scale, scale);
				}
				break;
			case VecOpCode::EEOP_INT64_GE:
				for (int i = 0; i < chunk.count; i++)
				{
					int scale = Max(left_scale, right_scale);

					registers_nulls[res + i] = registers_nulls[l + i] || registers_nulls[r + i];
					registers_i32[res + i] =
						RescaleInt64Value(registers_i64[l + i], left_scale, scale) >=
						RescaleInt64Value(registers_i64[r + i], right_scale, scale);
				}
				break;
			case VecOpCode::EEOP_AND:
				for (int i = 0; i < chunk.count; i++)
				{
					registers_nulls[res + i] = registers_nulls[l + i] || registers_nulls[r + i];
					registers_i32[res + i] = registers_i32[l + i] && registers_i32[r + i];
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
			case VecOpCode::EEOP_QUAL:
				ApplyQualSelection(chunk, &registers_nulls[res], &registers_i32[res]);
				break;
			default:
				break;
		}
	}
}

} /* namespace pg_volvec */

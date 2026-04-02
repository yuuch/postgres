extern "C" {
#include "postgres.h"

#ifdef USE_LLVM

#include <llvm-c/Core.h>

#include "fmgr.h"
#include "jit/jit.h"
#include "jit/llvmjit.h"
#include "jit/llvmjit_emit.h"
#include "miscadmin.h"
#include "storage/fd.h"
#endif
}

#include "volvec_engine.hpp"
#include "llvmjit_deform_datachunk.h"

#ifdef USE_LLVM

#include <array>
#include <cstdio>
#include <dlfcn.h>
#include <vector>

namespace pg_volvec {

typedef void *(*llvm_create_context_type)(int);
typedef char *(*llvm_expand_funcname_type)(void *, const char *);
typedef void *(*llvm_get_function_type)(void *, const char *);
typedef LLVMModuleRef (*llvm_mutable_module_type)(LLVMJitContext *);
typedef LLVMTypeRef (*llvm_pg_var_type_type)(const char *);
typedef void (*llvm_release_context_direct_type)(LLVMJitContext *);

static llvm_create_context_type pg_llvm_create_context = nullptr;
static llvm_expand_funcname_type pg_llvm_expand_funcname = nullptr;
static llvm_get_function_type pg_llvm_get_function = nullptr;
static llvm_mutable_module_type pg_llvm_mutable_module = nullptr;
static llvm_pg_var_type_type pg_llvm_pg_var_type = nullptr;
static llvm_release_context_direct_type pg_llvm_release_context_direct = nullptr;

static bool
resolve_jit_symbols_from_process()
{
	void *h = dlopen(NULL, RTLD_NOW | RTLD_GLOBAL);
	if (!h)
		return false;
	pg_llvm_create_context = (llvm_create_context_type) dlsym(h, "llvm_create_context");
	pg_llvm_expand_funcname = (llvm_expand_funcname_type) dlsym(h, "llvm_expand_funcname");
	pg_llvm_get_function = (llvm_get_function_type) dlsym(h, "llvm_get_function");
	pg_llvm_mutable_module = (llvm_mutable_module_type) dlsym(h, "llvm_mutable_module");
	pg_llvm_pg_var_type = (llvm_pg_var_type_type) dlsym(h, "llvm_pg_var_type");
	pg_llvm_release_context_direct = (llvm_release_context_direct_type) dlsym(h, "llvm_release_context_direct");
	return pg_llvm_create_context != nullptr &&
		pg_llvm_expand_funcname != nullptr &&
		pg_llvm_get_function != nullptr &&
		pg_llvm_mutable_module != nullptr &&
		pg_llvm_pg_var_type != nullptr &&
		pg_llvm_release_context_direct != nullptr;
}

static bool
load_jit_symbols(const char **failure_reason)
{
	char provider_path[MAXPGPATH];
	const char *provider_name;

	if (pg_llvm_create_context != nullptr)
		return true;
	if (resolve_jit_symbols_from_process())
		return true;

	provider_name = (jit_provider != nullptr && jit_provider[0] != '\0') ?
		jit_provider : "llvmjit";
	snprintf(provider_path, sizeof(provider_path), "%s/%s%s",
			 pkglib_path, provider_name, DLSUFFIX);

	if (!pg_file_exists(provider_path))
	{
		if (failure_reason != nullptr)
			*failure_reason = "configured JIT provider library is not present";
		return false;
	}

	load_file(provider_path, false);
	if (resolve_jit_symbols_from_process())
		return true;

	if (failure_reason != nullptr)
		*failure_reason = "loaded JIT provider but LLVM entry points are unavailable";
	return false;
}

void
pg_volvec_release_llvm_jit_context(JitContext *context)
{
	if (context == nullptr)
		return;
	if (!load_jit_symbols(nullptr))
		return;
	pg_llvm_release_context_direct((LLVMJitContext *) context);
}

static inline int64_t
pg_volvec_jit_pow10_int64(int scale)
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

	if (scale < 0)
		scale = 0;
	else if (scale >= (int) lengthof(kPowers))
		scale = lengthof(kPowers) - 1;

	return kPowers[scale];
}

static inline LLVMValueRef
pg_volvec_i32_const(LLVMTypeRef type_i32, int32_t value)
{
	return LLVMConstInt(type_i32, (uint64_t) (uint32_t) value, true);
}

static inline LLVMValueRef
pg_volvec_i64_const(LLVMTypeRef type_i64, int64_t value)
{
	return LLVMConstInt(type_i64, (uint64_t) value, true);
}

static inline LLVMValueRef
pg_volvec_i128_const(LLVMTypeRef type_i128, int64_t value)
{
	return LLVMConstInt(type_i128, (uint64_t) value, true);
}

struct ExprJitColumnBases
{
	std::array<LLVMValueRef, kMaxDeformTargets> f8;
	std::array<LLVMValueRef, kMaxDeformTargets> i64;
	std::array<LLVMValueRef, kMaxDeformTargets> i32;
	std::array<LLVMValueRef, kMaxDeformTargets> nulls;

	ExprJitColumnBases()
	{
		f8.fill(nullptr);
		i64.fill(nullptr);
		i32.fill(nullptr);
		nulls.fill(nullptr);
	}
};

static bool
pg_volvec_expr_opcode_supported(VecOpCode opcode)
{
	switch (opcode)
	{
		case VecOpCode::EEOP_VAR:
		case VecOpCode::EEOP_CONST:
		case VecOpCode::EEOP_FLOAT8_ADD:
		case VecOpCode::EEOP_FLOAT8_SUB:
		case VecOpCode::EEOP_FLOAT8_MUL:
		case VecOpCode::EEOP_INT64_ADD:
		case VecOpCode::EEOP_INT64_SUB:
		case VecOpCode::EEOP_INT64_MUL:
		case VecOpCode::EEOP_FLOAT8_LT:
		case VecOpCode::EEOP_FLOAT8_GT:
		case VecOpCode::EEOP_FLOAT8_LE:
		case VecOpCode::EEOP_FLOAT8_GE:
		case VecOpCode::EEOP_INT64_LT:
		case VecOpCode::EEOP_INT64_GT:
		case VecOpCode::EEOP_INT64_LE:
		case VecOpCode::EEOP_INT64_GE:
		case VecOpCode::EEOP_DATE_LT:
		case VecOpCode::EEOP_DATE_LE:
		case VecOpCode::EEOP_DATE_GE:
		case VecOpCode::EEOP_AND:
		case VecOpCode::EEOP_QUAL:
			return true;
		default:
			return false;
	}
}

static ExprJitColumnBases
pg_volvec_preload_expr_column_bases(LLVMBuilderRef b,
								   LLVMTypeRef type_i8,
								   LLVMTypeRef type_i64,
								   LLVMTypeRef type_i32,
								   LLVMTypeRef type_double,
								   LLVMValueRef v_col_f8,
								   LLVMValueRef v_col_i64,
								   LLVMValueRef v_col_i32,
								   LLVMValueRef v_col_nulls,
								   const VecExprProgram *program)
{
	ExprJitColumnBases bases;

	for (const auto &step : program->steps)
	{
		if (step.opcode != VecOpCode::EEOP_VAR)
			continue;

		int att = step.d.var.att_idx;
		LLVMValueRef v_att;

		if (att < 0 || att >= kMaxDeformTargets)
			continue;

		v_att = LLVMConstInt(type_i64, att, false);
		if (bases.nulls[att] == nullptr)
			bases.nulls[att] = LLVMBuildLoad2(
				b,
				l_ptr(type_i8),
				LLVMBuildGEP2(b, l_ptr(type_i8), v_col_nulls, &v_att, 1, ""),
				"");

		if (step.d.var.type == FLOAT8OID)
		{
			if (bases.f8[att] == nullptr)
				bases.f8[att] = LLVMBuildLoad2(
					b,
					l_ptr(type_double),
					LLVMBuildGEP2(b, l_ptr(type_double), v_col_f8, &v_att, 1, ""),
					"");
		}
		else if (step.d.var.type == NUMERICOID || step.d.var.type == INT8OID)
		{
			if (bases.i64[att] == nullptr)
				bases.i64[att] = LLVMBuildLoad2(
					b,
					l_ptr(type_i64),
					LLVMBuildGEP2(b, l_ptr(type_i64), v_col_i64, &v_att, 1, ""),
					"");
		}
		else
		{
			if (bases.i32[att] == nullptr)
				bases.i32[att] = LLVMBuildLoad2(
					b,
					l_ptr(type_i32),
					LLVMBuildGEP2(b, l_ptr(type_i32), v_col_i32, &v_att, 1, ""),
					"");
		}
	}

	return bases;
}

static LLVMValueRef
pg_volvec_build_rescale_int64(LLVMBuilderRef b,
							  LLVMTypeRef type_i64,
							  LLVMTypeRef type_i128,
							  LLVMValueRef value,
							  int from_scale,
							  int to_scale)
{
	if (value == nullptr || from_scale == to_scale)
		return value;

	if (from_scale < to_scale)
	{
		int64_t factor = pg_volvec_jit_pow10_int64(to_scale - from_scale);
		LLVMValueRef widened = LLVMBuildSExt(b, value, type_i128, "");
		LLVMValueRef scaled = LLVMBuildMul(b, widened,
										   pg_volvec_i128_const(type_i128, factor), "");
		return LLVMBuildTrunc(b, scaled, type_i64, "");
	}

	int64_t divisor = pg_volvec_jit_pow10_int64(from_scale - to_scale);
	int64_t halfway = divisor / 2;
	LLVMValueRef v_divisor = pg_volvec_i64_const(type_i64, divisor);
	LLVMValueRef v_halfway = pg_volvec_i64_const(type_i64, halfway);
	LLVMValueRef v_neg_halfway = pg_volvec_i64_const(type_i64, -halfway);
	LLVMValueRef quotient = LLVMBuildSDiv(b, value, v_divisor, "");
	LLVMValueRef remainder = LLVMBuildSRem(b, value, v_divisor, "");
	LLVMValueRef round_up = LLVMBuildICmp(b, LLVMIntSGE, remainder, v_halfway, "");
	LLVMValueRef round_down = LLVMBuildICmp(b, LLVMIntSLE, remainder, v_neg_halfway, "");
	LLVMValueRef plus_one = LLVMBuildAdd(b, quotient, pg_volvec_i64_const(type_i64, 1), "");
	LLVMValueRef minus_one = LLVMBuildSub(b, quotient, pg_volvec_i64_const(type_i64, 1), "");
	LLVMValueRef rounded = LLVMBuildSelect(b, round_up, plus_one, quotient, "");

	return LLVMBuildSelect(b, round_down, minus_one, rounded, "");
}

static bool
pg_volvec_emit_expr_row(LLVMBuilderRef b,
						LLVMTypeRef type_i1,
						LLVMTypeRef type_i8,
						LLVMTypeRef type_i32,
						LLVMTypeRef type_i64,
						LLVMTypeRef type_i128,
						LLVMTypeRef type_double,
						const VecExprProgram *program,
						const ExprJitColumnBases &bases,
						LLVMValueRef v_row_idx_ext,
						LLVMValueRef v_store_idx_ext,
						LLVMValueRef v_res_f8,
						LLVMValueRef v_res_i64,
						LLVMValueRef v_res_i32,
						LLVMValueRef v_res_nulls)
{
	std::vector<LLVMValueRef> reg_f8(program->max_reg_idx, nullptr);
	std::vector<LLVMValueRef> reg_i64(program->max_reg_idx, nullptr);
	std::vector<LLVMValueRef> reg_i32(program->max_reg_idx, nullptr);
	std::vector<LLVMValueRef> reg_null(program->max_reg_idx, nullptr);

	for (const auto &step : program->steps)
	{
		int res = step.res_idx;
		int l = step.d.op.left;
		int r = step.d.op.right;
		int left_scale = program->get_register_scale(l);
		int right_scale = program->get_register_scale(r);
		int res_scale = program->get_register_scale(res);

		switch (step.opcode)
		{
			case VecOpCode::EEOP_VAR:
			{
				int att = step.d.var.att_idx;

				if (att < 0 || att >= kMaxDeformTargets || bases.nulls[att] == nullptr)
					return false;

				reg_null[res] = LLVMBuildTrunc(
					b,
					LLVMBuildLoad2(b, type_i8,
								   LLVMBuildGEP2(b, type_i8, bases.nulls[att], &v_row_idx_ext, 1, ""),
								   ""),
					type_i1,
					"");

				if (step.d.var.type == FLOAT8OID)
				{
					if (bases.f8[att] == nullptr)
						return false;
					reg_f8[res] = LLVMBuildLoad2(
						b, type_double,
						LLVMBuildGEP2(b, type_double, bases.f8[att], &v_row_idx_ext, 1, ""),
						"");
				}
				else if (step.d.var.type == NUMERICOID || step.d.var.type == INT8OID)
				{
					if (bases.i64[att] == nullptr)
						return false;
					reg_i64[res] = LLVMBuildLoad2(
						b, type_i64,
						LLVMBuildGEP2(b, type_i64, bases.i64[att], &v_row_idx_ext, 1, ""),
						"");
				}
				else
				{
					if (bases.i32[att] == nullptr)
						return false;
					reg_i32[res] = LLVMBuildLoad2(
						b, type_i32,
						LLVMBuildGEP2(b, type_i32, bases.i32[att], &v_row_idx_ext, 1, ""),
						"");
				}
				break;
			}
			case VecOpCode::EEOP_CONST:
				reg_null[res] = LLVMConstInt(type_i1, step.d.constant.isnull, false);
				reg_f8[res] = LLVMConstReal(type_double, step.d.constant.fval);
				reg_i64[res] = pg_volvec_i64_const(type_i64, step.d.constant.i64val);
				reg_i32[res] = pg_volvec_i32_const(type_i32, step.d.constant.ival);
				break;
			case VecOpCode::EEOP_FLOAT8_ADD:
				if (!reg_null[l] || !reg_null[r] || !reg_f8[l] || !reg_f8[r])
					return false;
				reg_null[res] = LLVMBuildOr(b, reg_null[l], reg_null[r], "");
				reg_f8[res] = LLVMBuildFAdd(b, reg_f8[l], reg_f8[r], "");
				break;
			case VecOpCode::EEOP_FLOAT8_SUB:
				if (!reg_null[l] || !reg_null[r] || !reg_f8[l] || !reg_f8[r])
					return false;
				reg_null[res] = LLVMBuildOr(b, reg_null[l], reg_null[r], "");
				reg_f8[res] = LLVMBuildFSub(b, reg_f8[l], reg_f8[r], "");
				break;
			case VecOpCode::EEOP_FLOAT8_MUL:
				if (!reg_null[l] || !reg_null[r] || !reg_f8[l] || !reg_f8[r])
					return false;
				reg_null[res] = LLVMBuildOr(b, reg_null[l], reg_null[r], "");
				reg_f8[res] = LLVMBuildFMul(b, reg_f8[l], reg_f8[r], "");
				break;
			case VecOpCode::EEOP_FLOAT8_LT:
				if (!reg_null[l] || !reg_null[r] || !reg_f8[l] || !reg_f8[r])
					return false;
				reg_null[res] = LLVMBuildOr(b, reg_null[l], reg_null[r], "");
				reg_i32[res] = LLVMBuildZExt(
					b, LLVMBuildFCmp(b, LLVMRealULT, reg_f8[l], reg_f8[r], ""), type_i32, "");
				break;
			case VecOpCode::EEOP_FLOAT8_LE:
				if (!reg_null[l] || !reg_null[r] || !reg_f8[l] || !reg_f8[r])
					return false;
				reg_null[res] = LLVMBuildOr(b, reg_null[l], reg_null[r], "");
				reg_i32[res] = LLVMBuildZExt(
					b, LLVMBuildFCmp(b, LLVMRealULE, reg_f8[l], reg_f8[r], ""), type_i32, "");
				break;
			case VecOpCode::EEOP_FLOAT8_GT:
				if (!reg_null[l] || !reg_null[r] || !reg_f8[l] || !reg_f8[r])
					return false;
				reg_null[res] = LLVMBuildOr(b, reg_null[l], reg_null[r], "");
				reg_i32[res] = LLVMBuildZExt(
					b, LLVMBuildFCmp(b, LLVMRealUGT, reg_f8[l], reg_f8[r], ""), type_i32, "");
				break;
			case VecOpCode::EEOP_FLOAT8_GE:
				if (!reg_null[l] || !reg_null[r] || !reg_f8[l] || !reg_f8[r])
					return false;
				reg_null[res] = LLVMBuildOr(b, reg_null[l], reg_null[r], "");
				reg_i32[res] = LLVMBuildZExt(
					b, LLVMBuildFCmp(b, LLVMRealUGE, reg_f8[l], reg_f8[r], ""), type_i32, "");
				break;
			case VecOpCode::EEOP_INT64_ADD:
			{
				LLVMValueRef left_val;
				LLVMValueRef right_val;

				if (!reg_null[l] || !reg_null[r] || !reg_i64[l] || !reg_i64[r])
					return false;
				reg_null[res] = LLVMBuildOr(b, reg_null[l], reg_null[r], "");
				left_val = pg_volvec_build_rescale_int64(b, type_i64, type_i128, reg_i64[l],
														 left_scale, res_scale);
				right_val = pg_volvec_build_rescale_int64(b, type_i64, type_i128, reg_i64[r],
														  right_scale, res_scale);
				reg_i64[res] = LLVMBuildAdd(b, left_val, right_val, "");
				break;
			}
			case VecOpCode::EEOP_INT64_SUB:
			{
				LLVMValueRef left_val;
				LLVMValueRef right_val;

				if (!reg_null[l] || !reg_null[r] || !reg_i64[l] || !reg_i64[r])
					return false;
				reg_null[res] = LLVMBuildOr(b, reg_null[l], reg_null[r], "");
				left_val = pg_volvec_build_rescale_int64(b, type_i64, type_i128, reg_i64[l],
														 left_scale, res_scale);
				right_val = pg_volvec_build_rescale_int64(b, type_i64, type_i128, reg_i64[r],
														  right_scale, res_scale);
				reg_i64[res] = LLVMBuildSub(b, left_val, right_val, "");
				break;
			}
			case VecOpCode::EEOP_INT64_MUL:
			{
				LLVMValueRef wide_left;
				LLVMValueRef wide_right;

				if (!reg_null[l] || !reg_null[r] || !reg_i64[l] || !reg_i64[r])
					return false;
				reg_null[res] = LLVMBuildOr(b, reg_null[l], reg_null[r], "");
				wide_left = LLVMBuildSExt(b, reg_i64[l], type_i128, "");
				wide_right = LLVMBuildSExt(b, reg_i64[r], type_i128, "");
				reg_i64[res] = LLVMBuildTrunc(
					b, LLVMBuildMul(b, wide_left, wide_right, ""), type_i64, "");
				break;
			}
			case VecOpCode::EEOP_INT64_LT:
			case VecOpCode::EEOP_INT64_LE:
			case VecOpCode::EEOP_INT64_GT:
			case VecOpCode::EEOP_INT64_GE:
			{
				LLVMIntPredicate pred;
				LLVMValueRef left_val;
				LLVMValueRef right_val;
				int cmp_scale;

				if (!reg_null[l] || !reg_null[r] || !reg_i64[l] || !reg_i64[r])
					return false;

				cmp_scale = Max(left_scale, right_scale);
				left_val = pg_volvec_build_rescale_int64(b, type_i64, type_i128, reg_i64[l],
														 left_scale, cmp_scale);
				right_val = pg_volvec_build_rescale_int64(b, type_i64, type_i128, reg_i64[r],
														  right_scale, cmp_scale);
				reg_null[res] = LLVMBuildOr(b, reg_null[l], reg_null[r], "");
				pred = LLVMIntSLT;
				if (step.opcode == VecOpCode::EEOP_INT64_LE)
					pred = LLVMIntSLE;
				else if (step.opcode == VecOpCode::EEOP_INT64_GT)
					pred = LLVMIntSGT;
				else if (step.opcode == VecOpCode::EEOP_INT64_GE)
					pred = LLVMIntSGE;
				reg_i32[res] = LLVMBuildZExt(
					b, LLVMBuildICmp(b, pred, left_val, right_val, ""), type_i32, "");
				break;
			}
			case VecOpCode::EEOP_DATE_LT:
			case VecOpCode::EEOP_DATE_LE:
			case VecOpCode::EEOP_DATE_GE:
			{
				LLVMIntPredicate pred = LLVMIntSLT;

				if (!reg_null[l] || !reg_null[r] || !reg_i32[l] || !reg_i32[r])
					return false;
				reg_null[res] = LLVMBuildOr(b, reg_null[l], reg_null[r], "");
				if (step.opcode == VecOpCode::EEOP_DATE_LE)
					pred = LLVMIntSLE;
				else if (step.opcode == VecOpCode::EEOP_DATE_GE)
					pred = LLVMIntSGE;
				reg_i32[res] = LLVMBuildZExt(
					b, LLVMBuildICmp(b, pred, reg_i32[l], reg_i32[r], ""), type_i32, "");
				break;
			}
			case VecOpCode::EEOP_AND:
				if (!reg_null[l] || !reg_null[r] || !reg_i32[l] || !reg_i32[r])
					return false;
				reg_null[res] = LLVMBuildOr(b, reg_null[l], reg_null[r], "");
				reg_i32[res] = LLVMBuildAnd(b, reg_i32[l], reg_i32[r], "");
				break;
			case VecOpCode::EEOP_QUAL:
				break;
			default:
				return false;
		}
	}

	if (program->final_res_idx < 0 || program->final_res_idx >= program->max_reg_idx)
		return false;

	int fres = program->final_res_idx;
	if (reg_null[fres] == nullptr)
		return false;

	LLVMBuildStore(
		b,
		LLVMBuildZExt(b, reg_null[fres], type_i8, ""),
		LLVMBuildGEP2(b, type_i8, v_res_nulls, &v_store_idx_ext, 1, ""));
	if (reg_f8[fres] != nullptr)
		LLVMBuildStore(
			b, reg_f8[fres],
			LLVMBuildGEP2(b, type_double, v_res_f8, &v_store_idx_ext, 1, ""));
	if (reg_i64[fres] != nullptr)
		LLVMBuildStore(
			b, reg_i64[fres],
			LLVMBuildGEP2(b, type_i64, v_res_i64, &v_store_idx_ext, 1, ""));
	if (reg_i32[fres] != nullptr)
		LLVMBuildStore(
			b, reg_i32[fres],
			LLVMBuildGEP2(b, type_i32, v_res_i32, &v_store_idx_ext, 1, ""));

	return true;
}

static LLVMValueRef
compile_expr_to_jit(LLVMJitContext *context,
				   const VecExprProgram *program,
				   const char *funcname)
{
	LLVMModuleRef mod = pg_llvm_mutable_module(context);
	LLVMContextRef lc = LLVMGetModuleContext(mod);
	LLVMBuilderRef b = LLVMCreateBuilderInContext(lc);

	LLVMTypeRef type_i1 = LLVMInt1TypeInContext(lc);
	LLVMTypeRef type_i8 = LLVMInt8TypeInContext(lc);
	LLVMTypeRef type_i16 = LLVMInt16TypeInContext(lc);
	LLVMTypeRef type_i32 = LLVMInt32TypeInContext(lc);
	LLVMTypeRef type_i64 = LLVMInt64TypeInContext(lc);
	LLVMTypeRef type_i128 = LLVMIntTypeInContext(lc, 128);
	LLVMTypeRef type_double = LLVMDoubleTypeInContext(lc);
	LLVMTypeRef param_types[11];
	param_types[0] = type_i32;
	param_types[1] = l_ptr(l_ptr(type_double));
	param_types[2] = l_ptr(l_ptr(type_i64));
	param_types[3] = l_ptr(l_ptr(type_i32));
	param_types[4] = l_ptr(l_ptr(type_i8));
	param_types[5] = l_ptr(type_double);
	param_types[6] = l_ptr(type_i64);
	param_types[7] = l_ptr(type_i32);
	param_types[8] = l_ptr(type_i8);
	param_types[9] = l_ptr(type_i16);
	param_types[10] = type_i1;

	LLVMTypeRef func_sig = LLVMFunctionType(LLVMVoidTypeInContext(lc), param_types, 11, 0);
	LLVMValueRef v_func = LLVMAddFunction(mod, funcname, func_sig);
	LLVMValueRef v_count = LLVMGetParam(v_func, 0);
	LLVMValueRef v_col_f8 = LLVMGetParam(v_func, 1);
	LLVMValueRef v_col_i64 = LLVMGetParam(v_func, 2);
	LLVMValueRef v_col_i32 = LLVMGetParam(v_func, 3);
	LLVMValueRef v_col_nulls = LLVMGetParam(v_func, 4);
	LLVMValueRef v_res_f8 = LLVMGetParam(v_func, 5);
	LLVMValueRef v_res_i64 = LLVMGetParam(v_func, 6);
	LLVMValueRef v_res_i32 = LLVMGetParam(v_func, 7);
	LLVMValueRef v_res_nulls = LLVMGetParam(v_func, 8);
	LLVMValueRef v_sel = LLVMGetParam(v_func, 9);
	LLVMValueRef v_has_sel = LLVMGetParam(v_func, 10);
	ExprJitColumnBases bases;
	LLVMBasicBlockRef b_entry;
	LLVMBasicBlockRef b_dense_cond;
	LLVMBasicBlockRef b_dense_body;
	LLVMBasicBlockRef b_selected_cond;
	LLVMBasicBlockRef b_selected_body;
	LLVMBasicBlockRef b_exit;
	LLVMValueRef v_dense_idx;
	LLVMValueRef v_selected_idx;
	LLVMValueRef v_zero = pg_volvec_i32_const(type_i32, 0);

	for (const auto &step : program->steps)
	{
		if (!pg_volvec_expr_opcode_supported(step.opcode))
		{
			LLVMDisposeBuilder(b);
			return nullptr;
		}
	}

	b_entry = LLVMAppendBasicBlockInContext(lc, v_func, "entry");
	b_dense_cond = LLVMAppendBasicBlockInContext(lc, v_func, "dense_cond");
	b_dense_body = LLVMAppendBasicBlockInContext(lc, v_func, "dense_body");
	b_selected_cond = LLVMAppendBasicBlockInContext(lc, v_func, "selected_cond");
	b_selected_body = LLVMAppendBasicBlockInContext(lc, v_func, "selected_body");
	b_exit = LLVMAppendBasicBlockInContext(lc, v_func, "exit");
	LLVMPositionBuilderAtEnd(b, b_entry);
	bases = pg_volvec_preload_expr_column_bases(b, type_i8, type_i64, type_i32,
												type_double, v_col_f8, v_col_i64,
												v_col_i32, v_col_nulls, program);
	LLVMBuildCondBr(b, v_has_sel, b_selected_cond, b_dense_cond);

	LLVMPositionBuilderAtEnd(b, b_dense_cond);
	v_dense_idx = LLVMBuildPhi(b, type_i32, "dense_idx");
	LLVMAddIncoming(v_dense_idx, &v_zero, &b_entry, 1);
	LLVMBuildCondBr(
		b,
		LLVMBuildICmp(b, LLVMIntULT, v_dense_idx, v_count, ""),
		b_dense_body,
		b_exit);

	LLVMPositionBuilderAtEnd(b, b_dense_body);
	{
		LLVMValueRef v_dense_idx_ext = LLVMBuildZExt(b, v_dense_idx, type_i64, "");
		LLVMValueRef v_dense_next;

		if (!pg_volvec_emit_expr_row(b, type_i1, type_i8, type_i32, type_i64, type_i128,
									 type_double, program, bases, v_dense_idx_ext,
									 v_dense_idx_ext, v_res_f8, v_res_i64, v_res_i32,
									 v_res_nulls))
		{
			LLVMDisposeBuilder(b);
			return nullptr;
		}

		v_dense_next = LLVMBuildAdd(b, v_dense_idx, pg_volvec_i32_const(type_i32, 1), "");
		LLVMBuildBr(b, b_dense_cond);
		LLVMAddIncoming(v_dense_idx, &v_dense_next, &b_dense_body, 1);
	}

	LLVMPositionBuilderAtEnd(b, b_selected_cond);
	v_selected_idx = LLVMBuildPhi(b, type_i32, "selected_idx");
	LLVMAddIncoming(v_selected_idx, &v_zero, &b_entry, 1);
	LLVMBuildCondBr(
		b,
		LLVMBuildICmp(b, LLVMIntULT, v_selected_idx, v_count, ""),
		b_selected_body,
		b_exit);

	LLVMPositionBuilderAtEnd(b, b_selected_body);
	{
		LLVMValueRef v_selected_idx_ext = LLVMBuildZExt(b, v_selected_idx, type_i64, "");
		LLVMValueRef v_sel_val = LLVMBuildLoad2(
			b, type_i16, LLVMBuildGEP2(b, type_i16, v_sel, &v_selected_idx_ext, 1, ""), "");
		LLVMValueRef v_row_idx = LLVMBuildZExt(b, v_sel_val, type_i32, "");
		LLVMValueRef v_row_idx_ext = LLVMBuildZExt(b, v_row_idx, type_i64, "");
		LLVMValueRef v_selected_next;

		if (!pg_volvec_emit_expr_row(b, type_i1, type_i8, type_i32, type_i64, type_i128,
									 type_double, program, bases, v_row_idx_ext,
									 v_row_idx_ext, v_res_f8, v_res_i64, v_res_i32,
									 v_res_nulls))
		{
			LLVMDisposeBuilder(b);
			return nullptr;
		}

		v_selected_next = LLVMBuildAdd(
			b, v_selected_idx, pg_volvec_i32_const(type_i32, 1), "");
		LLVMBuildBr(b, b_selected_cond);
		LLVMAddIncoming(v_selected_idx, &v_selected_next, &b_selected_body, 1);
	}

	LLVMPositionBuilderAtEnd(b, b_exit);
	LLVMBuildRetVoid(b);
	LLVMDisposeBuilder(b);
	return v_func;
}

bool pg_volvec_try_compile_jit_expr(const VecExprProgram *program, VecExprJitFunc *out_func, JitContext **out_context, const char **failure_reason)
{
	if (!load_jit_symbols(failure_reason))
		return false;
	LLVMJitContext *ctx = (LLVMJitContext *) pg_llvm_create_context(PGJIT_PERFORM | PGJIT_OPT3);
	char base_name[96];
	snprintf(base_name, sizeof(base_name), "pg_volvec_jit_expr_%p", (const void *) program);
	char *funcname = pg_llvm_expand_funcname(ctx, base_name);
	LLVMValueRef fn = compile_expr_to_jit(ctx, program, funcname);
	if (!fn) {
		if (failure_reason != nullptr && *failure_reason == nullptr)
			*failure_reason = "expression JIT lowering rejected the program";
		pg_volvec_release_llvm_jit_context(&ctx->base);
		return false;
	}
	*out_func = (VecExprJitFunc) pg_llvm_get_function(ctx, funcname);
	*out_context = &ctx->base;
	return true;
}

} /* namespace pg_volvec */
#endif

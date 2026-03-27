extern "C" {
#include "postgres.h"

#ifdef USE_LLVM

#include <llvm-c/Core.h>

#include "access/htup_details.h"
#include "access/tupmacs.h"
#include "access/tupdesc_details.h"
#include "jit/jit.h"
#include "jit/llvmjit.h"
#include "jit/llvmjit_emit.h"
#endif
}

#include "llvmjit_deform_datachunk.h"

#ifdef USE_LLVM

namespace pg_vec {

static bool
pg_vec_jit_deform_supported(TupleDesc desc,
							  const DeformProgram *program,
							  const char **failure_reason)
{
	if (desc == nullptr || program == nullptr)
	{
		if (failure_reason != nullptr)
			*failure_reason = "missing tuple descriptor or deform program";
		return false;
	}

	if (program->ntargets <= 0)
	{
		if (failure_reason != nullptr)
			*failure_reason = "empty deform program";
		return false;
	}

	for (int att_index = 0; att_index <= program->last_att_index; att_index++)
	{
		CompactAttribute *att;

		if (att_index < 0 || att_index >= desc->natts)
		{
			if (failure_reason != nullptr)
				*failure_reason = "deform program references an attribute outside the tuple descriptor";
			return false;
		}

		att = TupleDescCompactAttr(desc, att_index);
		if (att->attisdropped)
		{
			if (failure_reason != nullptr)
				*failure_reason = "JIT deform does not support dropped attributes before the last target";
			return false;
		}
		if (att->attnullability != ATTNULLABLE_VALID)
		{
			if (failure_reason != nullptr)
				*failure_reason = "JIT deform currently requires NOT NULL attributes up to the last target";
			return false;
		}
	}

	for (int target_idx = 0; target_idx < program->ntargets; target_idx++)
	{
		const DeformTarget &target = program->targets[target_idx];
		CompactAttribute *att;

		if (target.att_index < 0 || target.att_index >= desc->natts)
		{
			if (failure_reason != nullptr)
				*failure_reason = "deform target index is out of bounds";
			return false;
		}

		att = TupleDescCompactAttr(desc, target.att_index);
		if (att->attnullability != ATTNULLABLE_VALID)
		{
			if (failure_reason != nullptr)
				*failure_reason = "JIT deform currently requires NOT NULL attributes";
			return false;
		}
		if (target_idx > 0 &&
			program->targets[target_idx - 1].att_index >= target.att_index)
		{
			if (failure_reason != nullptr)
				*failure_reason = "deform targets must be strictly increasing by attribute number";
			return false;
		}

		switch (target.decode_kind)
		{
			case DeformDecodeKind::kInt32:
			case DeformDecodeKind::kDate32:
				if (!att->attbyval || att->attlen != sizeof(int32))
				{
					if (failure_reason != nullptr)
						*failure_reason = "JIT deform int32/date targets require fixed-width 4-byte byval attributes";
					return false;
				}
				break;
			case DeformDecodeKind::kDecimal64Scale2:
			case DeformDecodeKind::kBpChar1:
				if (att->attlen != -1)
				{
					if (failure_reason != nullptr)
						*failure_reason = "JIT deform decimal64/bpchar1 targets currently require varlena storage";
					return false;
				}
				break;
			case DeformDecodeKind::kStringRef:
			default:
				if (failure_reason != nullptr)
					*failure_reason = "JIT deform currently supports only int32/date32/decimal64_s2/bpchar1 targets";
				return false;
		}
	}

	return true;
}

static Size
pg_vec_jit_skip_varlena(const char *base, Size off, uint8 alignby)
{
	const char *ptr = base + off;

	off = att_pointer_alignby(off, alignby, -1, ptr);
	ptr = base + off;
	return att_addlength_pointer(off, -1, ptr);
}

static Size
pg_vec_jit_store_decimal64_s2(const char *base, Size off, uint8 alignby, int64 *dst)
{
	const char *ptr = base + off;
	Datum		value;

	off = att_pointer_alignby(off, alignby, -1, ptr);
	ptr = base + off;
	value = fetch_att(ptr, false, -1);
	if (!numeric_varlena_to_scaled_int64(DatumGetPointer(value),
										 kDecimalScale2,
										 dst))
		elog(ERROR, "pg_vec: LLVM JIT failed to decode numeric target");
	return att_addlength_pointer(off, -1, ptr);
}

static Size
pg_vec_jit_store_bpchar1(const char *base, Size off, uint8 alignby, char *dst)
{
	const char *ptr = base + off;
	Datum		value;

	off = att_pointer_alignby(off, alignby, -1, ptr);
	ptr = base + off;
	value = fetch_att(ptr, false, -1);
	if (!bpchar_varlena_to_char1(DatumGetPointer(value), dst))
		elog(ERROR, "pg_vec: LLVM JIT failed to decode bpchar(1) target");
	return att_addlength_pointer(off, -1, ptr);
}

static LLVMValueRef
compile_deform_to_datachunk(LLVMJitContext *context,
							   TupleDesc desc,
							   const DeformProgram *program,
							   const char *funcname)
{
	LLVMModuleRef mod = llvm_mutable_module(context);
	LLVMContextRef lc = LLVMGetModuleContext(mod);
	LLVMBuilderRef b = LLVMCreateBuilderInContext(lc);
	LLVMTypeRef param_types[4];
	LLVMTypeRef func_sig;
	LLVMValueRef v_func;
	LLVMValueRef v_tuple;
	LLVMValueRef v_col_data;
	LLVMValueRef v_col_nulls;
	LLVMValueRef v_row_idx;
	LLVMBasicBlockRef b_entry;
	LLVMValueRef v_offp;
	LLVMValueRef v_tdata_gep;
	LLVMValueRef v_tdata;
	LLVMValueRef v_hoff_gep;
	LLVMValueRef v_hoff;
	LLVMValueRef v_hoff_sizet;
	LLVMValueRef v_tupdata_base;
	LLVMTypeRef type_i8;
	LLVMTypeRef type_i32;
	LLVMTypeRef type_i64;
	LLVMTypeRef skip_fn_args[3];
	LLVMTypeRef decimal_fn_args[4];
	LLVMTypeRef bpchar1_fn_args[4];
	LLVMTypeRef skip_fn_ty;
	LLVMTypeRef decimal_fn_ty;
	LLVMTypeRef bpchar1_fn_ty;
	LLVMValueRef v_skip_varlena_fn;
	LLVMValueRef v_decimal64_fn;
	LLVMValueRef v_bpchar1_fn;
	int			target_idx = 0;

	type_i8 = LLVMInt8TypeInContext(lc);
	type_i32 = LLVMInt32TypeInContext(lc);
	type_i64 = LLVMInt64TypeInContext(lc);

	param_types[0] = l_ptr(StructHeapTupleData);
	param_types[1] = l_ptr(l_ptr(type_i8));
	param_types[2] = l_ptr(l_ptr(type_i8));
	param_types[3] = type_i32;

	func_sig = LLVMFunctionType(LLVMVoidTypeInContext(lc), param_types, 4, 0);
	v_func = LLVMAddFunction(mod, funcname, func_sig);
	v_tuple = LLVMGetParam(v_func, 0);
	v_col_data = LLVMGetParam(v_func, 1);
	v_col_nulls = LLVMGetParam(v_func, 2);
	v_row_idx = LLVMGetParam(v_func, 3);

	skip_fn_args[0] = l_ptr(type_i8);
	skip_fn_args[1] = TypeSizeT;
	skip_fn_args[2] = type_i8;
	decimal_fn_args[0] = l_ptr(type_i8);
	decimal_fn_args[1] = TypeSizeT;
	decimal_fn_args[2] = type_i8;
	decimal_fn_args[3] = l_ptr(type_i64);
	bpchar1_fn_args[0] = l_ptr(type_i8);
	bpchar1_fn_args[1] = TypeSizeT;
	bpchar1_fn_args[2] = type_i8;
	bpchar1_fn_args[3] = l_ptr(type_i8);

	skip_fn_ty = LLVMFunctionType(TypeSizeT, skip_fn_args, 3, 0);
	decimal_fn_ty = LLVMFunctionType(TypeSizeT, decimal_fn_args, 4, 0);
	bpchar1_fn_ty = LLVMFunctionType(TypeSizeT, bpchar1_fn_args, 4, 0);
	v_skip_varlena_fn = l_ptr_const(reinterpret_cast<void *>(&pg_vec_jit_skip_varlena),
									l_ptr(skip_fn_ty));
	v_decimal64_fn = l_ptr_const(reinterpret_cast<void *>(&pg_vec_jit_store_decimal64_s2),
								 l_ptr(decimal_fn_ty));
	v_bpchar1_fn = l_ptr_const(reinterpret_cast<void *>(&pg_vec_jit_store_bpchar1),
							   l_ptr(bpchar1_fn_ty));

	b_entry = LLVMAppendBasicBlockInContext(lc, v_func, "entry");
	LLVMPositionBuilderAtEnd(b, b_entry);
	(void) v_col_nulls;

	v_offp = LLVMBuildAlloca(b, TypeSizeT, "offp");
	LLVMBuildStore(b, l_sizet_const(0), v_offp);

	v_tdata_gep = LLVMBuildStructGEP2(b,
									 StructHeapTupleData,
									 v_tuple,
									 FIELDNO_HEAPTUPLEDATA_DATA,
									 "t_data_gep");
	v_tdata = LLVMBuildLoad2(b, l_ptr(StructHeapTupleHeaderData), v_tdata_gep, "t_data");

	v_hoff_gep = LLVMBuildStructGEP2(b,
									 StructHeapTupleHeaderData,
									 v_tdata,
									 FIELDNO_HEAPTUPLEHEADERDATA_HOFF,
									 "t_hoff_gep");
	v_hoff = LLVMBuildLoad2(b, type_i8, v_hoff_gep, "t_hoff");
	v_hoff_sizet = LLVMBuildZExt(b, v_hoff, TypeSizeT, "t_hoff_sizet");
	v_tupdata_base = LLVMBuildGEP2(b,
									  type_i8,
									  LLVMBuildBitCast(b,
													   v_tdata,
													   l_ptr(type_i8),
													   ""),
									  &v_hoff_sizet,
									  1,
									  "tupdata_base");

	for (int att_index = 0; att_index <= program->last_att_index; att_index++)
	{
		CompactAttribute *att = TupleDescCompactAttr(desc, att_index);
		LLVMValueRef v_off = LLVMBuildLoad2(b, TypeSizeT, v_offp, "off");
		bool		is_target = target_idx < program->ntargets &&
			program->targets[target_idx].att_index == att_index;

		if (att->attlen == -1)
		{
			LLVMValueRef v_new_off;
			LLVMValueRef v_alignby = LLVMConstInt(type_i8, att->attalignby, false);

			if (is_target)
			{
				const DeformTarget &target = program->targets[target_idx];
				LLVMValueRef v_data_ptr_ptr;
				LLVMValueRef v_data_array;
				LLVMValueRef v_col_idx = LLVMConstInt(type_i32, target.dst_col, false);

				v_data_ptr_ptr = LLVMBuildGEP2(b,
											  l_ptr(type_i8),
											  v_col_data,
											  &v_col_idx,
											  1,
											  "col_data_ptr");
				v_data_array = LLVMBuildLoad2(b,
											 l_ptr(type_i8),
											 v_data_ptr_ptr,
											 "col_data");

				switch (target.decode_kind)
				{
					case DeformDecodeKind::kDecimal64Scale2:
					{
						LLVMValueRef v_dest = LLVMBuildGEP2(b,
													type_i64,
													LLVMBuildBitCast(b,
																	 v_data_array,
																	 l_ptr(type_i64),
																	 ""),
													&v_row_idx,
													1,
													"decimal64_dest");
						LLVMValueRef args[] = {v_tupdata_base, v_off, v_alignby, v_dest};

						v_new_off = l_call(b, decimal_fn_ty, v_decimal64_fn, args, 4, "decimal64_next_off");
						break;
					}
					case DeformDecodeKind::kBpChar1:
					{
						LLVMValueRef v_dest = LLVMBuildGEP2(b,
													type_i8,
													LLVMBuildBitCast(b,
																	 v_data_array,
																	 l_ptr(type_i8),
																	 ""),
													&v_row_idx,
													1,
													"bpchar1_dest");
						LLVMValueRef args[] = {v_tupdata_base, v_off, v_alignby, v_dest};

						v_new_off = l_call(b, bpchar1_fn_ty, v_bpchar1_fn, args, 4, "bpchar1_next_off");
						break;
					}
					default:
						LLVMDisposeBuilder(b);
						return nullptr;
				}
				target_idx++;
			}
			else
			{
				LLVMValueRef args[] = {v_tupdata_base, v_off, v_alignby};

				v_new_off = l_call(b, skip_fn_ty, v_skip_varlena_fn, args, 3, "skip_varlena_next_off");
			}

			LLVMBuildStore(b, v_new_off, v_offp);
			continue;
		}

		if (att->attalignby > 1)
		{
			LLVMValueRef v_aligned_off =
				LLVMBuildAnd(b,
							 LLVMBuildAdd(b, v_off,
										  l_sizet_const(att->attalignby - 1), ""),
							 l_sizet_const(~((Size) att->attalignby - 1)),
							 "aligned_off");

			LLVMBuildStore(b, v_aligned_off, v_offp);
			v_off = v_aligned_off;
		}

		if (is_target)
		{
			const DeformTarget &target = program->targets[target_idx];
			LLVMValueRef v_val_ptr;
			LLVMValueRef v_data_ptr_ptr;
			LLVMValueRef v_data_array;
			LLVMValueRef v_col_idx = LLVMConstInt(type_i32, target.dst_col, false);

			v_val_ptr = LLVMBuildGEP2(b,
									 type_i8,
									 v_tupdata_base,
									 &v_off,
									 1,
									 "val_ptr");
			v_data_ptr_ptr = LLVMBuildGEP2(b,
											  l_ptr(type_i8),
											  v_col_data,
											  &v_col_idx,
											  1,
											  "col_data_ptr");
			v_data_array = LLVMBuildLoad2(b,
										 l_ptr(type_i8),
										 v_data_ptr_ptr,
										 "col_data");

			switch (target.decode_kind)
			{
				case DeformDecodeKind::kInt32:
				case DeformDecodeKind::kDate32:
				{
					LLVMValueRef v_val = LLVMBuildLoad2(b,
													type_i32,
													LLVMBuildBitCast(b,
																	 v_val_ptr,
																	 l_ptr(type_i32),
																	 ""),
													"val");
					LLVMValueRef v_dest = LLVMBuildGEP2(b,
													type_i32,
													LLVMBuildBitCast(b,
																	 v_data_array,
																	 l_ptr(type_i32),
																	 ""),
													&v_row_idx,
													1,
													"dest");

					LLVMBuildStore(b, v_val, v_dest);
					break;
				}
				default:
					LLVMDisposeBuilder(b);
					return nullptr;
			}

			target_idx++;
		}

		LLVMBuildStore(b,
					 LLVMBuildAdd(b, v_off, l_sizet_const(att->attlen), ""),
					 v_offp);
	}

	LLVMBuildRetVoid(b);
	LLVMDisposeBuilder(b);
	return v_func;
}

bool
pg_vec_try_compile_jit_deform_to_datachunk(TupleDesc desc,
											 const DeformProgram *program,
											 JitDeformFunc *out_func,
											 JitContext **out_context,
											 const char **failure_reason)
{
	LLVMJitContext *context;
	LLVMValueRef fn;
	char	   *funcname;

	if (out_func != nullptr)
		*out_func = nullptr;
	if (out_context != nullptr)
		*out_context = nullptr;

	if (!pg_vec_jit_deform_supported(desc, program, failure_reason))
		return false;

	context = llvm_create_context(PGJIT_PERFORM | PGJIT_DEFORM | PGJIT_OPT3);
	funcname = llvm_expand_funcname(context, "pg_vec_deform_to_chunk");
	fn = compile_deform_to_datachunk(context, desc, program, funcname);
	if (fn == nullptr)
	{
		if (failure_reason != nullptr)
			*failure_reason = "failed to build LLVM deform function";
		jit_release_context(&context->base);
		pfree(funcname);
		return false;
	}

	if (out_func != nullptr)
		*out_func = reinterpret_cast<JitDeformFunc>(llvm_get_function(context, funcname));
	if (out_context != nullptr)
		*out_context = &context->base;
	pfree(funcname);
	return (out_func == nullptr || *out_func != nullptr);
}

} /* namespace pg_vec */

#endif

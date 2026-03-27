#ifndef PG_VEC_LLVMJIT_DEFORM_DATACHUNK_H
#define PG_VEC_LLVMJIT_DEFORM_DATACHUNK_H

#include "postgres.h"

#include "data_chunk_deform.hpp"

extern "C" {
#include "jit/jit.h"
}

namespace pg_vec {

#ifdef USE_LLVM
bool pg_vec_try_compile_jit_deform_to_datachunk(TupleDesc desc,
												const DeformProgram *program,
												JitDeformFunc *out_func,
												JitContext **out_context,
												const char **failure_reason);
#else
static inline bool
pg_vec_try_compile_jit_deform_to_datachunk(TupleDesc desc,
											 const DeformProgram *program,
											 JitDeformFunc *out_func,
											 JitContext **out_context,
											 const char **failure_reason)
{
	if (out_func != nullptr)
		*out_func = nullptr;
	if (out_context != nullptr)
		*out_context = nullptr;
	if (failure_reason != nullptr)
		*failure_reason = "LLVM support is not built";
	(void) desc;
	(void) program;
	return false;
}
#endif

} /* namespace pg_vec */

#endif

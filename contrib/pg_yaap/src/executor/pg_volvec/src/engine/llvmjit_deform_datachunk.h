#ifndef PG_VOLVEC_LLVMJIT_DEFORM_DATACHUNK_H
#define PG_VOLVEC_LLVMJIT_DEFORM_DATACHUNK_H

#include "volvec_engine.hpp"

namespace pg_volvec {

#ifdef USE_LLVM
bool pg_volvec_try_compile_jit_deform_to_datachunk(TupleDesc desc,
											 const DeformProgram *program,
											 JitDeformFunc *out_func,
											 JitContext **out_context,
											 const char **failure_reason);

bool pg_volvec_try_compile_jit_expr(const VecExprProgram *program,
								 VecExprJitFunc *out_func,
								 JitContext **out_context,
								 const char **failure_reason);
#endif

} /* namespace pg_volvec */

#endif

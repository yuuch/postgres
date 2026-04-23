#pragma once

#include "parallel/parallel_runtime.hpp"

struct PgVolVecQueryState { MemoryContext context; VecPlanState* vec_plan; void* parallel_plan; void* parallel_scheduler; };

void CompileExpr(Expr *expr, VecExprProgram &program, bool is_filter = false, EState *estate = nullptr);
Plan *TryCanonicalizeFinalizePartialAggregate(Agg *finalize_agg,
											  Plan *gather_plan,
											  Agg *partial_agg,
											  const char **failure_reason);
bool MatchPresortedGroupAggInputChain(Agg *agg,
									  Plan **agg_input_out,
									  Sort **presort_sort_out);
std::unique_ptr<VecPlanState> ExecInitVecPlan(Plan *plan,
												 EState *estate,
												 const ParallelWorkerContext *parallel_worker_context = nullptr);
std::unique_ptr<ParallelPipelinePlan> BuildParallelPipelinePlan(Plan *plan,
																PlannedStmt *plannedstmt,
																EState *estate,
																const char **failure_reason);
std::unique_ptr<ParallelSchedulerState> BuildParallelSchedulerState(const ParallelPipelinePlan *plan,
																   MemoryContext context,
																   uint32_t source_morsel_nblocks,
																   const char **failure_reason);
bool TryInitializeLeaderOnlyAggregateWorkerContext(PgVolVecQueryState *query_state,
												   ParallelWorkerContext *worker_context,
												   const ParallelPipelineDesc **source_pipeline_out,
												   const char **failure_reason);
bool TryExecuteQuerySchedulerSkeleton(PgVolVecQueryState *query_state,
									  QueryDesc *queryDesc,
									  const char **failure_reason);
bool TryExecuteProcessParallelAggregate(PgVolVecQueryState *query_state,
										QueryDesc *queryDesc,
										const char **failure_reason);
bool ExecuteParallelTask(const ParallelTaskDesc &task,
						 const ParallelPipelinePlan *parallel_plan,
						 ParallelWorkerContext &worker_context,
						 const char **failure_reason);

#ifdef USE_LLVM
bool pg_volvec_try_compile_jit_deform_to_datachunk(TupleDesc desc, const DeformProgram *program, JitDeformFunc *out_func, JitContext **out_context, const char **failure_reason);
bool pg_volvec_try_compile_jit_expr(const VecExprProgram *program, VecExprJitFunc *out_func, JitContext **out_context, const char **failure_reason);
void pg_volvec_register_llvm_jit_context(JitContext *context);
void pg_volvec_release_llvm_jit_context(JitContext *context);
size_t pg_volvec_release_all_registered_llvm_jit_contexts_for_proc_exit();
#endif

} /* namespace pg_volvec */

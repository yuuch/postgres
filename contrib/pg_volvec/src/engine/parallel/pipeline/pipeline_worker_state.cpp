#include "parallel/pipeline/pipeline_worker_state.hpp"

extern "C" {
#include "postgres.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/readfuncs.h"
#include "storage/ipc.h"
#include "utils/snapmgr.h"
}

#include "core/data_chunk_deform.hpp"
#include "exec/plan_state.hpp"
#include "exec/agg.hpp"
#include "exec/query_state.hpp"

namespace pg_volvec {
namespace pipeline {

PipelineWorkerState::PipelineWorkerState() = default;
PipelineWorkerState::~PipelineWorkerState() = default;
PipelineWorkerState::PipelineWorkerState(PipelineWorkerState &&) noexcept = default;
PipelineWorkerState &PipelineWorkerState::operator=(PipelineWorkerState &&) noexcept = default;

namespace {

void
pipeline_worker_proc_exit_cleanup(int code, Datum arg)
{
	(void) code;
	auto *state = (PipelineWorkerState *) DatumGetPointer(arg);
	if (state == nullptr)
		return;

	if (state->root_plan)
	{
		if (pg_volvec_trace_hooks)
			elog(LOG,
				 "pg_volvec: pipeline proc-exit cleanup releasing JIT pid=%d root_plan=%p",
				 MyProcPid,
				 (void *) state->root_plan.get());
		state->root_plan->release_jit_resources_for_proc_exit();
		state->root_plan.reset();
		state->worker_context.root_plan = nullptr;
		state->worker_context.agg_state = nullptr;
	}

#ifdef USE_LLVM
	size_t orphaned =
		pg_volvec_release_all_registered_llvm_jit_contexts_for_proc_exit();
	if (pg_volvec_trace_hooks && orphaned > 0)
		elog(LOG,
			 "pg_volvec: pipeline proc-exit released %zu orphan JIT context(s) pid=%d",
			 orphaned,
			 MyProcPid);
#endif
}

}

void
RegisterPipelineProcExitJitCleanup(PipelineWorkerState *state)
{
	if (state == nullptr)
		return;
	before_shmem_exit(pipeline_worker_proc_exit_cleanup,
					  PointerGetDatum(state));
}

void
CleanupPipelineWorkerState(PipelineWorkerState *state)
{
	if (state == nullptr)
		return;

	state->root_plan.reset();

	if (state->estate != nullptr)
	{
		if (state->estate->es_snapshot != InvalidSnapshot)
			UnregisterSnapshot(state->estate->es_snapshot);
		FreeExecutorState(state->estate);
		state->estate = nullptr;
	}
	if (state->memory_context != nullptr)
	{
		MemoryContextDelete(state->memory_context);
		state->memory_context = nullptr;
	}
	state->plannedstmt = nullptr;
	state->query_text = nullptr;
	state->agg_state = nullptr;
	state->worker_context = PipelineWorkerContext{};
}

bool
InitializePipelineWorkerState(const char           *plannedstmt_serialized,
                              const char           *query_text,
                              int                   agg_plan_node_id,
                              Oid                   source_scan_relid,
                              int                   source_scan_plan_node_id,
                              ParallelTableScanDesc parallel_scan_desc,
                              bool                  leader,
                              PipelineWorkerState  *out,
                              const char          **failure_reason)
{
	if (failure_reason != nullptr)
		*failure_reason = nullptr;
	if (plannedstmt_serialized == nullptr || out == nullptr)
	{
		if (failure_reason != nullptr)
			*failure_reason = "pipeline worker missing serialized planned statement";
		return false;
	}

	*out = PipelineWorkerState{};

	instr_time init_start;
	INSTR_TIME_SET_CURRENT(init_start);

	MemoryContext worker_context =
		AllocSetContextCreate(CurrentMemoryContext,
							  "pg_volvec pipeline worker",
							  ALLOCSET_DEFAULT_SIZES);
	MemoryContext old_context = MemoryContextSwitchTo(worker_context);

	out->memory_context = worker_context;
	out->plannedstmt = (PlannedStmt *) stringToNode(plannedstmt_serialized);
	out->query_text = pstrdup(query_text != nullptr ? query_text : "");
	out->estate = CreateExecutorState();
	out->estate->es_snapshot = RegisterSnapshot(GetActiveSnapshot());
	out->estate->es_sourceText = out->query_text;
	out->estate->es_plannedstmt = out->plannedstmt;
	ExecInitRangeTable(out->estate,
					   out->plannedstmt->rtable,
					   out->plannedstmt->permInfos,
					   out->plannedstmt->unprunableRelids);

	out->worker_context.memory_context = worker_context;
	out->worker_context.plannedstmt = out->plannedstmt;
	out->worker_context.estate = out->estate;
	out->worker_context.agg_plan_node_id = agg_plan_node_id;
	out->worker_context.parallel_scan_relid = source_scan_relid;
	out->worker_context.parallel_scan_plan_node_id = source_scan_plan_node_id;
	out->worker_context.parallel_scan_desc = parallel_scan_desc;
	out->worker_context.leader = leader;

	if (pg_volvec_trace_hooks)
		elog(LOG,
			 "pg_volvec: pipeline %s init binding rel=%u plan_node_id=%d agg_plan_node_id=%d",
			 leader ? "leader" : "worker",
			 source_scan_relid,
			 source_scan_plan_node_id,
			 agg_plan_node_id);

	pg_volvec::ParallelWorkerContext legacy_ctx{};
	legacy_ctx.memory_context = out->worker_context.memory_context;
	legacy_ctx.plannedstmt = out->worker_context.plannedstmt;
	legacy_ctx.estate = out->worker_context.estate;
	legacy_ctx.agg_plan_node_id = out->worker_context.agg_plan_node_id;
	legacy_ctx.parallel_scan_relid = out->worker_context.parallel_scan_relid;
	legacy_ctx.parallel_scan_plan_node_id =
		out->worker_context.parallel_scan_plan_node_id;
	legacy_ctx.parallel_scan_desc = out->worker_context.parallel_scan_desc;
	legacy_ctx.leader = out->worker_context.leader;

	out->root_plan = ExecInitVecPlan(out->plannedstmt->planTree,
									 out->estate,
									 &legacy_ctx);
	if (!out->root_plan)
	{
		MemoryContextSwitchTo(old_context);
		CleanupPipelineWorkerState(out);
		if (failure_reason != nullptr)
			*failure_reason = "pipeline worker could not initialize VecPlanState";
		return false;
	}

	if (agg_plan_node_id >= 0)
		out->agg_state =
			out->root_plan->find_parallel_aggregate_state_by_plan_node_id(agg_plan_node_id);
	else
		out->agg_state = out->root_plan->find_parallel_aggregate_state();

	if (out->agg_state == nullptr ||
		!out->agg_state->supports_parallel_partial_state())
	{
		MemoryContextSwitchTo(old_context);
		CleanupPipelineWorkerState(out);
		if (failure_reason != nullptr)
			*failure_reason = "pipeline worker requires partial aggregate support";
		return false;
	}

	out->worker_context.root_plan =
		reinterpret_cast<::VecPlanState *>(out->root_plan.get());
	out->worker_context.agg_state =
		reinterpret_cast<::VecAggState *>(out->agg_state);

	instr_time init_end;
	INSTR_TIME_SET_CURRENT(init_end);
	INSTR_TIME_SUBTRACT(init_end, init_start);
	out->init_time_us = (uint64) INSTR_TIME_GET_MICROSEC(init_end);

	MemoryContextSwitchTo(old_context);
	return true;
}

}
}

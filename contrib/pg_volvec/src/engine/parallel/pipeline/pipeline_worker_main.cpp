#include "parallel/pipeline/pipeline_worker_state.hpp"
#include "parallel/pipeline/dsm_control.hpp"
#include "parallel/pipeline/executor.hpp"
#include "parallel/pipeline/pipeline.hpp"
#include "parallel/pipeline/pipeline_lowering.hpp"
#include "parallel/pipeline/sink.hpp"
#include "parallel/pipeline/source.hpp"

extern "C" {
#include "postgres.h"
#include "access/parallel.h"
#include "access/relscan.h"
#include "miscadmin.h"
#include "storage/buffile.h"
#include "storage/sharedfileset.h"
#include "storage/shm_toc.h"
#include "utils/dsa.h"
#include "utils/elog.h"
#include "utils/memutils.h"
}

namespace pg_volvec {
namespace pipeline {

namespace {

void
RunPipelineWorkerBody(dsm_segment *seg, shm_toc *toc)
{
	auto *control = (PipelineSharedControl *)
		shm_toc_lookup(toc, PIPELINE_DSM_KEY_CONTROL, false);
	const char *plannedstmt_serialized = (const char *)
		shm_toc_lookup(toc, PIPELINE_DSM_KEY_PLANNEDSTMT, false);
	const char *query_text = (const char *)
		shm_toc_lookup(toc, PIPELINE_DSM_KEY_QUERY_TEXT, true);
	auto *partials = (ParallelAggPartialState *)
		shm_toc_lookup(toc, PIPELINE_DSM_KEY_PARTIALS, false);
	auto *source_pscan = (ParallelTableScanDesc)
		shm_toc_lookup(toc, PIPELINE_DSM_KEY_SOURCE_PSCAN, false);
	auto *partial_fileset = (SharedFileSet *)
		shm_toc_lookup(toc, PIPELINE_DSM_KEY_PARTIAL_FILESET, true);

	if (control == nullptr || control->magic != PIPELINE_DSM_MAGIC ||
		plannedstmt_serialized == nullptr ||
		partials == nullptr ||
		source_pscan == nullptr)
		elog(ERROR, "pg_volvec pipeline worker missing shared control");

	if (partial_fileset != nullptr)
		SharedFileSetAttach(partial_fileset, seg);

	PipelineWorkerState state;
	const char *failure_reason = nullptr;
	if (!InitializePipelineWorkerState(plannedstmt_serialized,
									   query_text,
									   control->agg_plan_node_id,
									   control->source_scan_relid,
									   control->source_scan_plan_node_id,
									   source_pscan,
									   false,
									   &state,
									   &failure_reason))
		elog(ERROR,
			 "pg_volvec pipeline worker init failed: %s",
			 failure_reason != nullptr ? failure_reason : "unknown");

	RegisterPipelineProcExitJitCleanup(&state);

	MemoryContext old_context = MemoryContextSwitchTo(state.memory_context);

	auto bundle = LowerToPipeline(state.root_plan.get(),
								  control,
								  &control->next_block,
								  partials,
								  (int) control->partial_slot_count,
								  partial_fileset);
	if (!bundle)
	{
		MemoryContextSwitchTo(old_context);
		CleanupPipelineWorkerState(&state);
		elog(ERROR, "pg_volvec pipeline worker could not lower plan");
	}

	ExecCtx exec_ctx{};
	exec_ctx.mcxt = state.memory_context;
	exec_ctx.dsa = nullptr;
	exec_ctx.vec_plan = state.root_plan.get();
	exec_ctx.worker_index = (int) ParallelWorkerNumber;

	Source *src   = bundle->primary()->pipeline.src;
	Sink   *sink  = bundle->primary()->pipeline.sink;
	auto    gsrc  = src->GetGlobalSourceState(exec_ctx);
	auto    lsrc  = src->GetLocalSourceState(exec_ctx, *gsrc);
	auto    gsink = sink->GetGlobalSinkState(exec_ctx);
	auto    lsink = sink->GetLocalSinkState(exec_ctx, *gsink);

	WorkerPipelineExecutor exec(src, bundle->primary()->pipeline.ops, sink);
	exec.Execute(exec_ctx, *gsrc, *lsrc, gsink.get(), lsink.get(), 0);

	OperatorSinkCombineInput combine_input{*lsink, *gsink};
	sink->Combine(exec_ctx, combine_input);

	bundle.reset();
	MemoryContextSwitchTo(old_context);
	CleanupPipelineWorkerState(&state);
}

}

extern "C" PGDLLEXPORT void
pg_volvec_pipeline_worker_main(dsm_segment *seg, shm_toc *toc)
{
	RunPipelineWorkerBody(seg, toc);
}

}
}

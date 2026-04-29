#include "parallel/pipeline/task.hpp"

#include <cstdint>

#include "core/data_chunk.hpp"
#include "parallel/pipeline/physical_operator.hpp"
#include "parallel/pipeline/pipeline.hpp"

namespace pg_volvec {
namespace pipeline {

namespace {

[[noreturn]] void
RaiseBlockedForbidden()
{
	Assert(false);
	ereport(ERROR,
	        (errmsg("pg_volvec pipeline returned BLOCKED in 3g.2 (forbidden)")));
}

void
EnsureGlobalStates(ProcessPipelineExecState &ps, Pipeline &pipeline, ExecCtx &ctx)
{
	if (ps.global_source == nullptr)
		ps.global_source = pipeline.source->GetGlobalSourceState(ctx);
	if (ps.global_sink == nullptr)
		ps.global_sink = pipeline.sink->GetGlobalSinkState(ctx);
	if (ps.global_ops.empty())
	{
		ps.global_ops.reserve(pipeline.ops.size());
		for (PhysicalOperator *op : pipeline.ops)
			ps.global_ops.push_back(op->GetGlobalOperatorState(ctx));
	}
}

void
EnsureRunLocalStates(ProcessPipelineExecState &ps, Pipeline &pipeline, ExecCtx &ctx)
{
	if (ps.run_initialized)
		return;
	Assert(ps.global_source != nullptr);
	Assert(ps.global_sink != nullptr);
	ps.local_source = pipeline.source->GetLocalSourceState(ctx, *ps.global_source);
	ps.local_sink = pipeline.sink->GetLocalSinkState(ctx, *ps.global_sink);
	ps.local_ops.clear();
	ps.local_ops.reserve(pipeline.ops.size());
	for (PhysicalOperator *op : pipeline.ops)
		ps.local_ops.push_back(op->GetOperatorState(ctx));
	ps.run_initialized = true;
}

}  /* namespace */

Task::Task(EventId event_id, TaskKind kind, Pipeline *pipeline,
           WorkerTaskRuntime *runtime, int32_t worker_index)
    : event_id_(event_id)
    , kind_(kind)
    , pipeline_(pipeline)
    , runtime_(runtime)
    , worker_index_(worker_index) {}

PipelineRunTask::PipelineRunTask(EventId event_id, Pipeline *pipeline,
                                 WorkerTaskRuntime *runtime,
                                 int32_t worker_index)
    : Task(event_id, TaskKind::RUN, pipeline, runtime, worker_index) {}

TaskExecutionResult
PipelineRunTask::Execute()
{
	auto &rt = *runtime_;
	auto &ctx = rt.exec_ctx;
	auto &ps = rt.GetOrCreatePipelineState(pipeline_->id);

	fprintf(stderr,
		"PGVOLVEC_DIAG[worker_idx=%d pid=%d]: RUN.Execute ENTER pipeline_id=%u\n",
		worker_index_, (int) getpid(), (unsigned) pipeline_->id);
	EnsureGlobalStates(ps, *pipeline_, ctx);
	EnsureRunLocalStates(ps, *pipeline_, ctx);

	const bool leader_slice = (worker_index_ == LEADER_WORKER_INDEX);
	const uint32_t chunk_budget = leader_slice ? 32 : UINT32_MAX;
	uint32_t chunks_done = 0;

	PipelineChunk src_chunk;
	PipelineChunk scratch_a, scratch_b;

	for (;;)
	{
		if (chunks_done >= chunk_budget)
			return TaskExecutionResult::TASK_NOT_FINISHED;

		OperatorSourceInput src_in{*ps.global_source, *ps.local_source};
		SourceResultType sres = pipeline_->source->GetData(ctx, src_chunk, src_in);
		fprintf(stderr,
			"PGVOLVEC_DIAG[worker_idx=%d pid=%d]: RUN.GetData pipeline_id=%u sres=%d src_chunk.count=%u\n",
			worker_index_, (int) getpid(), (unsigned) pipeline_->id,
			(int) sres, (unsigned) src_chunk.count);

		if (sres == SourceResultType::BLOCKED)
			RaiseBlockedForbidden();
		if (sres == SourceResultType::FINISHED)
			return TaskExecutionResult::TASK_FINISHED;

		PipelineChunk *current_in = &src_chunk;
		PipelineChunk *current_out = &scratch_a;

		for (size_t i = 0; i < pipeline_->ops.size(); ++i)
		{
			PhysicalOperator *op = pipeline_->ops[i];
			OperatorState &op_state = *ps.local_ops[i];
			for (;;)
			{
				current_out->reset();
				OperatorResultType ores = op->Execute(ctx, *current_in, *current_out,
				                                     op_state);
				if (ores == OperatorResultType::BLOCKED)
					RaiseBlockedForbidden();
				if (ores == OperatorResultType::FINISHED)
					return TaskExecutionResult::TASK_FINISHED;
				if (ores == OperatorResultType::NEED_MORE_INPUT)
					goto next_source_chunk;
				current_in = current_out;
				current_out = (current_out == &scratch_a) ? &scratch_b : &scratch_a;
				break;
			}
		}
		{
			OperatorSinkInput sink_in{*ps.global_sink, *ps.local_sink};
			SinkResultType kres = pipeline_->sink->SinkChunk(ctx, *current_in,
			                                                sink_in);
			if (kres == SinkResultType::BLOCKED)
				RaiseBlockedForbidden();
			if (kres == SinkResultType::FINISHED)
				return TaskExecutionResult::TASK_FINISHED;
		}
next_source_chunk:
		++chunks_done;
	}
}

PipelineCombineTask::PipelineCombineTask(EventId event_id,
                                         Pipeline *pipeline,
                                         WorkerTaskRuntime *runtime,
                                         int32_t worker_index)
    : Task(event_id, TaskKind::COMBINE, pipeline, runtime, worker_index) {}

TaskExecutionResult
PipelineCombineTask::Execute()
{
	auto &rt = *runtime_;
	auto &ctx = rt.exec_ctx;
	auto &ps = rt.GetPipelineState(pipeline_->id);
	fprintf(stderr,
		"PGVOLVEC_DIAG[worker_idx=%d pid=%d]: COMBINE.Execute ENTER pipeline_id=%u\n",
		worker_index_, (int) getpid(), (unsigned) pipeline_->id);
	Assert(ps.local_sink != nullptr);
	Assert(ps.global_sink != nullptr);
	Assert(!ps.combine_done);
	OperatorSinkCombineInput in{*ps.local_sink, *ps.global_sink};
	SinkCombineResultType cres = pipeline_->sink->Combine(ctx, in);
	if (cres == SinkCombineResultType::BLOCKED)
		RaiseBlockedForbidden();
	ps.combine_done = true;
	ps.local_source.reset();
	ps.local_sink.reset();
	ps.local_ops.clear();
	return TaskExecutionResult::TASK_FINISHED;
}

PipelineFinalizeTask::PipelineFinalizeTask(EventId event_id,
                                           Pipeline *pipeline,
                                           WorkerTaskRuntime *runtime,
                                           int32_t worker_index)
    : Task(event_id, TaskKind::FINALIZE, pipeline, runtime, worker_index) {}

TaskExecutionResult
PipelineFinalizeTask::Execute()
{
	auto &rt = *runtime_;
	auto &ctx = rt.exec_ctx;
	/*
	 * GetOrCreatePipelineState (not strict GetPipelineState):
	 * FINALIZE is allowed to be the first touch on a pipeline for a given
	 * runtime. This is the OUTPUT pipeline topology: workers execute RUN
	 * (writing to the global TDC); the leader skips RUN (leader_participate
	 * is false for OUTPUT) and only sees the pipeline at FINALIZE to drain
	 * the global TDC into the DestReceiver. EnsureGlobalStates() below
	 * initializes global_sink/global_source from the descriptor on a freshly
	 * created empty ProcessPipelineExecState, which is exactly the seed
	 * state we need. The strict GetPipelineState variant traps the empty
	 * vector (libc++ Hardening / __builtin_trap) and is therefore wrong for
	 * leader-only-finalize pipelines.
	 *
	 * COMBINE keeps the strict variant intentionally: if COMBINE arrives
	 * for a runtime that never RAN, that is a real scheduling bug
	 * (local_sink/local_ops would not exist). OUTPUT does not dispatch
	 * COMBINE.
	 */
	auto &ps = rt.GetOrCreatePipelineState(pipeline_->id);
	fprintf(stderr,
		"PGVOLVEC_DIAG[worker_idx=%d pid=%d]: FINALIZE.Execute ENTER pipeline_id=%u leader_partial_pending=%d\n",
		worker_index_, (int) getpid(), (unsigned) pipeline_->id,
		(int) ps.leader_partial_pending);
	EnsureGlobalStates(ps, *pipeline_, ctx);

	if (ps.leader_partial_pending && ps.local_sink)
	{
		OperatorSinkCombineInput cin{*ps.local_sink, *ps.global_sink};
		auto cres = pipeline_->sink->Combine(ctx, cin);
		if (cres == SinkCombineResultType::BLOCKED)
			RaiseBlockedForbidden();
		ps.leader_partial_pending = false;
		ps.local_source.reset();
		ps.local_sink.reset();
		ps.local_ops.clear();
	}
	SinkFinalizeType fres = pipeline_->sink->Finalize(ctx, *ps.global_sink);
	fprintf(stderr,
		"PGVOLVEC_DIAG[worker_idx=%d pid=%d]: FINALIZE.Execute Sink->Finalize returned pipeline_id=%u fres=%d\n",
		worker_index_, (int) getpid(), (unsigned) pipeline_->id, (int) fres);
	if (fres == SinkFinalizeType::BLOCKED)
		RaiseBlockedForbidden();
	return TaskExecutionResult::TASK_FINISHED;
}

}  /* namespace pipeline */
}  /* namespace pg_volvec */

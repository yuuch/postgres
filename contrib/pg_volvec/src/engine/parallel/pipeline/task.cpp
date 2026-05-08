#include "parallel/pipeline/task.hpp"

#include <cstdint>

#include "core/data_chunk.hpp"
#include "parallel/pipeline/physical_operator.hpp"
#include "parallel/pipeline/pipeline.hpp"
#include "parallel/pipeline/pipeline_profile.hpp"

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

class TaskProfileEventGuard
{
public:
	TaskProfileEventGuard(ExecCtx &ctx, EventId event_id)
		: ctx_(ctx), previous_(ctx.profile_event_id)
	{
		ctx_.profile_event_id = event_id;
	}

	~TaskProfileEventGuard()
	{
		ctx_.profile_event_id = previous_;
	}

	TaskProfileEventGuard(const TaskProfileEventGuard &) = delete;
	TaskProfileEventGuard &operator=(const TaskProfileEventGuard &) = delete;

private:
	ExecCtx &ctx_;
	EventId previous_;
};

}  /* namespace */

Task::Task(EventId event_id, TaskKind kind, Pipeline *pipeline,
           WorkerTaskRuntime *runtime, int32_t worker_index,
           uint32_t partition_id)
    : event_id_(event_id)
    , kind_(kind)
    , pipeline_(pipeline)
    , runtime_(runtime)
    , worker_index_(worker_index)
    , partition_id_(partition_id) {}

PipelineRunTask::PipelineRunTask(EventId event_id, Pipeline *pipeline,
                                 WorkerTaskRuntime *runtime,
                                 int32_t worker_index)
    : Task(event_id, TaskKind::RUN, pipeline, runtime, worker_index) {}

TaskExecutionResult
PipelineRunTask::Execute()
{
	auto &rt = *runtime_;
	auto &ctx = rt.exec_ctx;
	TaskProfileEventGuard event_guard(ctx, event_id_);
	PipelineProfileScope task_scope(ctx, PipelineProfileStage::TASK_RUN_TOTAL);
	auto &ps = rt.GetOrCreatePipelineState(pipeline_->id);

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
		SourceResultType sres;
		{
			PipelineProfileScope source_scope(ctx,
				PipelineProfileSourceStage(pipeline_->source->type()));
			sres = pipeline_->source->GetData(ctx, src_chunk, src_in);
			if (sres == SourceResultType::HAVE_MORE_OUTPUT)
				source_scope.AddRows(src_chunk.count);
		}

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
				OperatorResultType ores;
				{
					PipelineProfileScope op_scope(ctx,
						PipelineProfileOperatorStage(op->type()));
					ores = op->Execute(ctx, *current_in, *current_out,
											op_state);
					if (ores == OperatorResultType::HAVE_MORE_OUTPUT)
						op_scope.AddRows(current_out->count);
				}
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
			SinkResultType kres;
			{
				PipelineProfileScope sink_scope(ctx,
					PipelineProfileSinkStage(pipeline_->sink->type()));
				kres = pipeline_->sink->SinkChunk(ctx, *current_in,
											 sink_in);
				sink_scope.AddRows(current_in->count);
			}
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
                                          int32_t worker_index,
                                          uint32_t partition_id)
    : Task(event_id, TaskKind::COMBINE, pipeline, runtime, worker_index, partition_id) {}

TaskExecutionResult
PipelineCombineTask::Execute()
{
	auto &rt = *runtime_;
	auto &ctx = rt.exec_ctx;
	TaskProfileEventGuard event_guard(ctx, event_id_);
	PipelineProfileScope task_scope(ctx, PipelineProfileStage::TASK_COMBINE_TOTAL);
	auto &ps = rt.GetPipelineState(pipeline_->id);
	Assert(ps.global_sink != nullptr);
	if (partition_id_ == UINT32_MAX)
		Assert(ps.local_sink != nullptr);
	Assert(partition_id_ != UINT32_MAX || !ps.combine_done);
	OperatorSinkCombineInput in{ps.local_sink.get(), *ps.global_sink, partition_id_};
	SinkCombineResultType cres;
	{
		PipelineProfileScope combine_scope(ctx,
			PipelineProfileCombineStage(pipeline_->sink->type()));
		cres = pipeline_->sink->Combine(ctx, in);
	}
	if (cres == SinkCombineResultType::BLOCKED)
		RaiseBlockedForbidden();
	if (partition_id_ == UINT32_MAX)
	{
		ps.combine_done = true;
		ps.local_source.reset();
		ps.local_sink.reset();
		ps.local_ops.clear();
	}
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
	TaskProfileEventGuard event_guard(ctx, event_id_);
	PipelineProfileScope task_scope(ctx, PipelineProfileStage::TASK_FINALIZE_TOTAL);
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
	EnsureGlobalStates(ps, *pipeline_, ctx);

	if (ps.leader_partial_pending && ps.local_sink)
	{
		OperatorSinkCombineInput cin{ps.local_sink.get(), *ps.global_sink, UINT32_MAX};
		auto cres = pipeline_->sink->Combine(ctx, cin);
		if (cres == SinkCombineResultType::BLOCKED)
			RaiseBlockedForbidden();
		ps.leader_partial_pending = false;
		ps.local_source.reset();
		ps.local_sink.reset();
		ps.local_ops.clear();
	}
	SinkFinalizeType fres;
	{
		PipelineProfileScope finalize_scope(ctx,
			PipelineProfileFinalizeStage(pipeline_->sink->type()));
		fres = pipeline_->sink->Finalize(ctx, *ps.global_sink);
	}
	if (fres == SinkFinalizeType::BLOCKED)
		RaiseBlockedForbidden();
	return TaskExecutionResult::TASK_FINISHED;
}

}  /* namespace pipeline */
}  /* namespace pg_volvec */

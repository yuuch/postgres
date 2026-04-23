#include "parallel/pipeline/agg_sink.hpp"

extern "C" {
#include "storage/buffile.h"
#include "storage/sharedfileset.h"
}

#include <cstdio>
#include <cstring>

namespace pg_volvec {
namespace pipeline {

namespace {

void
FormatPartialFileName(char *buf, size_t buflen, int worker_index)
{
	snprintf(buf, buflen, "pipeline_agg_worker_%d", worker_index);
}

}

std::unique_ptr<GlobalSinkState>
AggSink::GetGlobalSinkState(ExecCtx & /*ctx*/)
{
	return std::unique_ptr<GlobalSinkState>(
	    new AggGlobalSinkState(merger_, shared_slots_, num_slots_, spill_fileset_));
}

std::unique_ptr<LocalSinkState>
AggSink::GetLocalSinkState(ExecCtx &ctx, GlobalSinkState & /*gstate*/)
{
	VecAggState *worker_agg = nullptr;
	if (ctx.vec_plan != nullptr)
		worker_agg = ctx.vec_plan->find_parallel_aggregate_state();

	return std::unique_ptr<LocalSinkState>(
	    new AggLocalSinkState(worker_agg, ctx.worker_index));
}

SinkResultType
AggSink::SinkChunk(ExecCtx & /*ctx*/, PipelineChunk & /*in*/, OperatorSinkInput & /*input*/)
{
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType
AggSink::Combine(ExecCtx & /*ctx*/, OperatorSinkCombineInput &input)
{
	auto &lstate = static_cast<AggLocalSinkState &>(input.local_state);
	auto &gstate = static_cast<AggGlobalSinkState &>(input.global_state);

	if (lstate.worker_agg == nullptr || gstate.slots == nullptr)
		return SinkCombineResultType::FINISHED;

	if (lstate.worker_index < 0 || lstate.worker_index >= gstate.num_slots)
		return SinkCombineResultType::FINISHED;

	lstate.worker_agg->finish_sink();

	ParallelAggPartialState *partial = &gstate.slots[lstate.worker_index];

	bool exported_inline = lstate.worker_agg->export_parallel_partial_state(partial);
	if (!exported_inline &&
	    lstate.worker_agg->uses_file_backed_parallel_partial_state() &&
	    gstate.spill_fileset != nullptr)
	{
		char file_name[VOLVEC_PARALLEL_MAX_PARTIAL_FILE_NAME];
		FormatPartialFileName(file_name, sizeof(file_name), lstate.worker_index);

		BufFile *file = BufFileCreateFileSet(&gstate.spill_fileset->fs, file_name);
		if (file == nullptr ||
		    !lstate.worker_agg->export_parallel_grouped_partial_file(file, partial))
		{
			if (file != nullptr)
				BufFileClose(file);
			elog(ERROR, "pg_volvec pipeline: grouped partial export failed");
		}
		strlcpy(partial->grouped_file_name, file_name, sizeof(partial->grouped_file_name));
		BufFileExportFileSet(file);
		BufFileClose(file);
	}
	else if (!exported_inline)
	{
		elog(ERROR, "pg_volvec pipeline: inline partial export failed");
	}

	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType
AggSink::Finalize(ExecCtx &ctx, GlobalSinkState &gstate_base)
{
	if (ctx.worker_index != LEADER_WORKER_INDEX)
		return SinkFinalizeType::READY;

	auto &gstate = static_cast<AggGlobalSinkState &>(gstate_base);
	if (gstate.merger == nullptr || gstate.slots == nullptr)
		return SinkFinalizeType::NO_OUTPUT_POSSIBLE;

	for (int i = 0; i < gstate.num_slots; i++)
	{
		ParallelAggPartialState &partial = gstate.slots[i];

		if (partial.file_backed)
		{
			if (gstate.spill_fileset == nullptr)
				elog(ERROR, "pg_volvec pipeline: file-backed partial without spill fileset");

			BufFile *file = BufFileOpenFileSet(&gstate.spill_fileset->fs,
			                                    partial.grouped_file_name,
			                                    O_RDONLY,
			                                    false);
			if (file == nullptr ||
			    !gstate.merger->merge_parallel_grouped_partial_file(file, partial))
			{
				if (file != nullptr)
					BufFileClose(file);
				elog(ERROR, "pg_volvec pipeline: grouped partial merge failed");
			}
			BufFileClose(file);
			BufFileDeleteFileSet(&gstate.spill_fileset->fs,
			                     partial.grouped_file_name,
			                     true);
		}
		else if (!gstate.merger->merge_parallel_partial_state(partial))
		{
			elog(ERROR, "pg_volvec pipeline: inline partial merge failed");
		}
	}

	gstate.merger->finish_sink();
	return SinkFinalizeType::READY;
}

}  /* namespace pipeline */
}  /* namespace pg_volvec */

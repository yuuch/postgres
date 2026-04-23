#include "parallel/pipeline/executor.hpp"

#include "parallel/pipeline/operator.hpp"
#include "parallel/pipeline/sink.hpp"
#include "parallel/pipeline/source.hpp"

extern "C" {
#include "postgres.h"
#include "utils/elog.h"
}

#include <memory>
#include <vector>

#include "core/data_chunk.hpp"

}  /* close leaked pg_volvec namespace from core/types.hpp */

namespace pg_volvec {
namespace pipeline {

WorkerPipelineExecutor::WorkerPipelineExecutor(Source *src,
                                               const std::vector<Operator *> &ops,
                                               Sink *sink)
	: src_(src), ops_(ops), sink_(sink)
{
	Assert(src_ != nullptr);
}

bool
WorkerPipelineExecutor::Execute(ExecCtx           &ctx,
                                GlobalSourceState &gsrc,
                                LocalSourceState  &lsrc,
                                GlobalSinkState   *gsink,
                                LocalSinkState    *lsink,
                                std::size_t        max_chunks)
{
	if (src_ == nullptr)
		ereport(ERROR,
		        (errcode(ERRCODE_INTERNAL_ERROR),
		         errmsg("pg_volvec pipeline executor: missing source")));

	OperatorSourceInput src_input{gsrc, lsrc};

	std::vector<std::unique_ptr<OperatorState>> op_states;
	op_states.reserve(ops_.size());
	for (Operator *op : ops_)
		op_states.push_back(op->GetOperatorState(ctx));

	PipelineChunk chunk;
	std::size_t chunks_pushed = 0;
	bool source_finished = false;

	while (!source_finished && (max_chunks == 0 || chunks_pushed < max_chunks))
	{
		chunk.reset();

		SourceResultType src_rc = src_->GetData(ctx, chunk, src_input);
		if (src_rc == SourceResultType::BLOCKED)
			ereport(ERROR,
			        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			         errmsg("pg_volvec pipeline executor: BLOCKED source not supported")));
		if (src_rc == SourceResultType::FINISHED)
		{
			source_finished = true;
			break;
		}

		int active = chunk.has_selection ? chunk.sel.count : chunk.count;
		if (active <= 0)
			continue;

		bool drop_chunk = false;
		for (std::size_t i = 0; i < ops_.size(); i++)
		{
			PipelineChunk op_out;
			OperatorResultType op_rc = ops_[i]->Execute(ctx, chunk, op_out, *op_states[i]);

			if (op_rc == OperatorResultType::BLOCKED)
				ereport(ERROR,
				        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				         errmsg("pg_volvec pipeline executor: BLOCKED operator not supported")));
			if (op_rc == OperatorResultType::FINISHED)
			{
				source_finished = true;
				drop_chunk = true;
				break;
			}

			active = chunk.has_selection ? chunk.sel.count : chunk.count;
			if (active <= 0)
			{
				drop_chunk = true;
				break;
			}
		}

		if (drop_chunk)
			continue;

		if (sink_ != nullptr && lsink != nullptr && gsink != nullptr)
		{
			OperatorSinkInput sink_input{*gsink, *lsink};
			SinkResultType sink_rc = sink_->SinkChunk(ctx, chunk, sink_input);
			if (sink_rc == SinkResultType::BLOCKED)
				ereport(ERROR,
				        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				         errmsg("pg_volvec pipeline executor: BLOCKED sink not supported")));
			if (sink_rc == SinkResultType::FINISHED)
			{
				source_finished = true;
				break;
			}
		}

		chunks_pushed++;
	}

	return source_finished;
}

}  /* namespace pipeline */
}  /* namespace pg_volvec */

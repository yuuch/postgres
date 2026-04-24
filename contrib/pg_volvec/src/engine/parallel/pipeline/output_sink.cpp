#include "parallel/pipeline/output_sink.hpp"

namespace pg_volvec {
namespace pipeline {

std::unique_ptr<GlobalSinkState>
OutputSink::GetGlobalSinkState(ExecCtx &ctx)
{
	(void) ctx;
	return nullptr;
}

std::unique_ptr<LocalSinkState>
OutputSink::GetLocalSinkState(ExecCtx &ctx, GlobalSinkState &gstate)
{
	(void) ctx; (void) gstate;
	return nullptr;
}

SinkResultType
OutputSink::SinkChunk(ExecCtx &ctx, PipelineChunk &in, OperatorSinkInput &input)
{
	(void) ctx; (void) in; (void) input;
	return SinkResultType::FINISHED;
}

SinkCombineResultType
OutputSink::Combine(ExecCtx &ctx, OperatorSinkCombineInput &input)
{
	(void) ctx; (void) input;
	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType
OutputSink::Finalize(ExecCtx &ctx, GlobalSinkState &gstate)
{
	(void) ctx; (void) gstate;
	return SinkFinalizeType::READY;
}

}
}

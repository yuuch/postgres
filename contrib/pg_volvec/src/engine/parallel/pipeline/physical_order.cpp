#include "parallel/pipeline/physical_order.hpp"

namespace pg_volvec {
namespace pipeline {

std::unique_ptr<GlobalSinkState>
PhysicalOrder::GetGlobalSinkState(ExecCtx &ctx)
{
	(void) ctx;
	return nullptr;
}

std::unique_ptr<LocalSinkState>
PhysicalOrder::GetLocalSinkState(ExecCtx &ctx, GlobalSinkState &gstate)
{
	(void) ctx; (void) gstate;
	return nullptr;
}

SinkResultType
PhysicalOrder::SinkChunk(ExecCtx &ctx, PipelineChunk &in, OperatorSinkInput &input)
{
	(void) ctx; (void) in; (void) input;
	return SinkResultType::FINISHED;
}

SinkCombineResultType
PhysicalOrder::Combine(ExecCtx &ctx, OperatorSinkCombineInput &input)
{
	(void) ctx; (void) input;
	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType
PhysicalOrder::Finalize(ExecCtx &ctx, GlobalSinkState &gstate)
{
	(void) ctx; (void) gstate;
	return SinkFinalizeType::READY;
}

std::unique_ptr<GlobalSourceState>
PhysicalOrder::GetGlobalSourceState(ExecCtx &ctx)
{
	(void) ctx;
	return nullptr;
}

std::unique_ptr<LocalSourceState>
PhysicalOrder::GetLocalSourceState(ExecCtx &ctx, GlobalSourceState &gstate)
{
	(void) ctx; (void) gstate;
	return nullptr;
}

SourceResultType
PhysicalOrder::GetData(ExecCtx &ctx, PipelineChunk &out, OperatorSourceInput &input)
{
	(void) ctx; (void) out; (void) input;
	return SourceResultType::FINISHED;
}

}
}

#include "parallel/pipeline/physical_seq_scan.hpp"

namespace pg_volvec {
namespace pipeline {

std::unique_ptr<GlobalSourceState>
PhysicalSeqScan::GetGlobalSourceState(ExecCtx &ctx)
{
	(void) ctx;
	return nullptr;
}

std::unique_ptr<LocalSourceState>
PhysicalSeqScan::GetLocalSourceState(ExecCtx &ctx, GlobalSourceState &gstate)
{
	(void) ctx; (void) gstate;
	return nullptr;
}

SourceResultType
PhysicalSeqScan::GetData(ExecCtx &ctx, PipelineChunk &out, OperatorSourceInput &input)
{
	(void) ctx; (void) out; (void) input;
	return SourceResultType::FINISHED;
}

int
PhysicalSeqScan::MaxThreads(ExecCtx &ctx) const
{
	(void) ctx;
	return 0;
}

}
}

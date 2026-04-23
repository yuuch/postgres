#include "parallel/pipeline/partial_agg_op.hpp"

namespace pg_volvec {
namespace pipeline {

namespace {

class PartialAggOpState : public OperatorState {};

}

std::unique_ptr<OperatorState>
PartialAggOp::GetOperatorState(ExecCtx & /*ctx*/)
{
	return std::unique_ptr<OperatorState>(new PartialAggOpState());
}

OperatorResultType
PartialAggOp::Execute(ExecCtx & /*ctx*/, PipelineChunk &in, PipelineChunk & /*out*/,
                      OperatorState & /*state*/)
{
	if (agg_ == nullptr)
		return OperatorResultType::FINISHED;

	int active = in.has_selection ? in.sel.count : in.count;
	if (active <= 0)
		return OperatorResultType::NEED_MORE_INPUT;

	agg_->consume_batch(in);
	return OperatorResultType::NEED_MORE_INPUT;
}

}  /* namespace pipeline */
}  /* namespace pg_volvec */

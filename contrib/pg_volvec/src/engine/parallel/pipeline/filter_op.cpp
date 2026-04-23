#include "parallel/pipeline/filter_op.hpp"

namespace pg_volvec {
namespace pipeline {

namespace {

class FilterOpState : public OperatorState {};

}

std::unique_ptr<OperatorState>
FilterOp::GetOperatorState(ExecCtx & /*ctx*/)
{
	return std::unique_ptr<OperatorState>(new FilterOpState());
}

OperatorResultType
FilterOp::Execute(ExecCtx & /*ctx*/, PipelineChunk &in, PipelineChunk & /*out*/,
                  OperatorState & /*state*/)
{
	if (program_ == nullptr)
		return OperatorResultType::NEED_MORE_INPUT;

	program_->evaluate(in);

	int active = in.has_selection ? in.sel.count : in.count;
	if (active <= 0)
		return OperatorResultType::NEED_MORE_INPUT;

	return OperatorResultType::HAVE_MORE_OUTPUT;
}

}  /* namespace pipeline */
}  /* namespace pg_volvec */

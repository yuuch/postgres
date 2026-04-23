#pragma once

#include "core/types.hpp"
#include "core/memory.hpp"
#include "core/data_chunk.hpp"
#include "core/data_chunk_deform.hpp"
#include "expr/expr.hpp"
}

#include "parallel/pipeline/operator.hpp"

namespace pg_volvec {
namespace pipeline {

static_assert(PIPELINE_DEFAULT_CHUNK_SIZE == DEFAULT_CHUNK_SIZE,
              "pipeline chunk size must match core DEFAULT_CHUNK_SIZE");

/* In-place selection-vector filter. Does NOT write `out`; executor aliases `in`. */
class FilterOp : public Operator {
public:
	explicit FilterOp(VecExprProgram *program) : program_(program) {}

	std::unique_ptr<OperatorState> GetOperatorState(ExecCtx &ctx) override;

	OperatorResultType
	Execute(ExecCtx &ctx, PipelineChunk &in, PipelineChunk &out, OperatorState &state) override;

	bool ParallelOperator() const override { return true; }

private:
	VecExprProgram *program_;
};

}
}

#pragma once

#include "core/types.hpp"
#include "core/memory.hpp"
#include "core/data_chunk.hpp"
#include "core/data_chunk_deform.hpp"
#include "core/hash_table_defs.hpp"
#include "expr/expr.hpp"
#include "exec/plan_state.hpp"
#include "exec/agg.hpp"
}

#include "parallel/pipeline/operator.hpp"

namespace pg_volvec {
namespace pipeline {

static_assert(PIPELINE_DEFAULT_CHUNK_SIZE == DEFAULT_CHUNK_SIZE,
              "pipeline chunk size must match core DEFAULT_CHUNK_SIZE");

/* Per-worker partial agg ingest. Always NEED_MORE_INPUT; partials harvested by AggSink. */
class PartialAggOp : public Operator {
public:
	explicit PartialAggOp(VecAggState *agg) : agg_(agg) {}

	std::unique_ptr<OperatorState> GetOperatorState(ExecCtx &ctx) override;

	OperatorResultType
	Execute(ExecCtx &ctx, PipelineChunk &in, PipelineChunk &out, OperatorState &state) override;

	bool ParallelOperator() const override { return true; }

	VecAggState *agg_state() const { return agg_; }

private:
	VecAggState *agg_;
};

}
}

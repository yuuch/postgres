#include "parallel/pipeline/physical_projection.hpp"

extern "C" {
#include "postgres.h"
#include "utils/elog.h"
}

namespace pg_volvec {
namespace pipeline {

std::unique_ptr<OperatorState>
PhysicalProjection::GetOperatorState(ExecCtx &ctx)
{
	(void) ctx;
	return std::make_unique<ProjectionOperatorState>();
}

OperatorResultType
PhysicalProjection::Execute(ExecCtx &ctx, PipelineChunk &in, PipelineChunk &out, OperatorState &state)
{
	(void) ctx;
	(void) state;
	out = in;

	for (const ProjectExprDesc &expr : expr_descs_)
	{
		if (static_cast<size_t>(expr.first_step_idx) + static_cast<size_t>(expr.n_steps) > steps_.size())
			elog(ERROR, "pg_volvec: projection step range exceeds step tape");

		for (uint16_t step_idx = expr.first_step_idx;
		     step_idx < static_cast<uint16_t>(expr.first_step_idx + expr.n_steps);
		     ++step_idx)
		{
			const ProjectStep &step = steps_[step_idx];
			for (uint16_t row = 0; row < out.count; ++row)
			{
				switch (step.op)
				{
					case ProjectOp::NUMERIC_MUL_VAR_VAR:
						out.int64_columns[step.out_chunk_slot][row] =
							out.int64_columns[step.in_a_chunk_slot][row] *
							out.int64_columns[step.in_b_chunk_slot][row];
						break;
					case ProjectOp::NUMERIC_MUL_VAR_CONST:
						out.int64_columns[step.out_chunk_slot][row] =
							out.int64_columns[step.in_a_chunk_slot][row] *
							step.const_value;
						break;
					case ProjectOp::NUMERIC_SUB_CONST_VAR:
						out.int64_columns[step.out_chunk_slot][row] =
							step.const_value - out.int64_columns[step.in_b_chunk_slot][row];
						break;
					case ProjectOp::NUMERIC_ADD_CONST_VAR:
						out.int64_columns[step.out_chunk_slot][row] =
							step.const_value + out.int64_columns[step.in_b_chunk_slot][row];
						break;
					case ProjectOp::COPY_VAR:
						out.int64_columns[step.out_chunk_slot][row] =
							out.int64_columns[step.in_a_chunk_slot][row];
						break;
				}
				out.nulls[step.out_chunk_slot][row] = 0;
			}
		}

		if (expr.n_steps == 0 || expr.output_chunk_slot != steps_[expr.first_step_idx + expr.n_steps - 1].out_chunk_slot)
			elog(ERROR, "pg_volvec: projection output slot descriptor mismatch");
	}

	return out.count > 0 ? OperatorResultType::HAVE_MORE_OUTPUT
	                     : OperatorResultType::NEED_MORE_INPUT;
}

}  /* namespace pipeline */
}  /* namespace pg_volvec */

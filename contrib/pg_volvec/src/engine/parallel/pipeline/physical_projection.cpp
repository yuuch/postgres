#include "parallel/pipeline/physical_projection.hpp"

extern "C" {
#include "postgres.h"
#include "utils/elog.h"
}

namespace pg_volvec {
namespace pipeline {

namespace {

static inline bool
StepInputIsNull(const ProjectStep &step, const PipelineChunk &out, uint16_t row)
{
	switch (step.op)
	{
		case ProjectOp::NUMERIC_SCALE_VAR_CONST:
		case ProjectOp::NUMERIC_MUL_VAR_CONST:
		case ProjectOp::NUMERIC_ADD_VAR_CONST:
		case ProjectOp::NUMERIC_SUB_VAR_CONST:
		case ProjectOp::COPY_VAR:
			return out.nulls[step.in_a_chunk_slot][row] != 0;
		case ProjectOp::NUMERIC_SUB_CONST_VAR:
		case ProjectOp::NUMERIC_ADD_CONST_VAR:
			return out.nulls[step.in_b_chunk_slot][row] != 0;
		case ProjectOp::NUMERIC_MUL_VAR_VAR:
		case ProjectOp::NUMERIC_ADD_VAR_VAR:
		case ProjectOp::NUMERIC_SUB_VAR_VAR:
		case ProjectOp::NUMERIC_DIV_VAR_VAR:
			return out.nulls[step.in_a_chunk_slot][row] != 0 ||
			       out.nulls[step.in_b_chunk_slot][row] != 0;
		case ProjectOp::STRING_PREFIX_LIKE:
			return out.nulls[step.in_a_chunk_slot][row] != 0;
		case ProjectOp::NUMERIC_CASE_VAR_CONST:
			return false;
	}

	return true;
}

} // namespace

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
	auto &op_state = static_cast<ProjectionOperatorState &>(state);
	if (op_state.current_input_drained)
	{
		op_state.current_input_drained = false;
		out.reset();
		return OperatorResultType::NEED_MORE_INPUT;
	}
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
				if (StepInputIsNull(step, out, row))
				{
					out.nulls[step.out_chunk_slot][row] = 1;
					continue;
				}

				switch (step.op)
				{
					case ProjectOp::NUMERIC_SCALE_VAR_CONST:
						if (step.const_value >= 0)
							out.int64_columns[step.out_chunk_slot][row] =
								out.int64_columns[step.in_a_chunk_slot][row] * step.const_value;
						else
							out.int64_columns[step.out_chunk_slot][row] =
								out.int64_columns[step.in_a_chunk_slot][row] / (-step.const_value);
						break;
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
					case ProjectOp::NUMERIC_ADD_VAR_VAR:
						out.int64_columns[step.out_chunk_slot][row] =
							out.int64_columns[step.in_a_chunk_slot][row] +
							out.int64_columns[step.in_b_chunk_slot][row];
						break;
					case ProjectOp::NUMERIC_SUB_VAR_VAR:
						out.int64_columns[step.out_chunk_slot][row] =
							out.int64_columns[step.in_a_chunk_slot][row] -
							out.int64_columns[step.in_b_chunk_slot][row];
						break;
					case ProjectOp::NUMERIC_ADD_VAR_CONST:
						out.int64_columns[step.out_chunk_slot][row] =
							out.int64_columns[step.in_a_chunk_slot][row] + step.const_value;
						break;
					case ProjectOp::NUMERIC_SUB_VAR_CONST:
						out.int64_columns[step.out_chunk_slot][row] =
							out.int64_columns[step.in_a_chunk_slot][row] - step.const_value;
						break;
					case ProjectOp::COPY_VAR:
						out.int64_columns[step.out_chunk_slot][row] =
							out.int64_columns[step.in_a_chunk_slot][row];
						break;
					case ProjectOp::NUMERIC_DIV_VAR_VAR:
					{
						const int64_t denom = out.int64_columns[step.in_b_chunk_slot][row];
						if (denom == 0)
						{
							out.nulls[step.out_chunk_slot][row] = 1;
							continue;
						}
						const NumericWideInt numerator =
							WideIntFromInt64(out.int64_columns[step.in_a_chunk_slot][row]) *
							WideIntFromInt64(step.const_value);
						out.int64_columns[step.out_chunk_slot][row] =
							WideIntToInt64Checked(numerator / WideIntFromInt64(denom),
								"projection numeric division");
						break;
					}
					case ProjectOp::STRING_PREFIX_LIKE:
					{
						const VecStringRef &ref = out.string_columns[step.in_a_chunk_slot][row];
						const char *lhs = out.get_string_ptr(ref);
						const uint32_t prefix_len = step.in_b_chunk_slot;
						const char *rhs = reinterpret_cast<const char *>(&step.const_value);
						const bool match = (lhs != nullptr || ref.len == 0) &&
							ref.len >= prefix_len &&
							(prefix_len == 0 || std::memcmp(lhs, rhs, prefix_len) == 0);
						out.int64_columns[step.out_chunk_slot][row] = match ? 1 : 0;
						break;
					}
					case ProjectOp::NUMERIC_CASE_VAR_CONST:
						if (out.nulls[step.in_a_chunk_slot][row] == 0 &&
							out.int64_columns[step.in_a_chunk_slot][row] != 0)
						{
							if (out.nulls[step.in_b_chunk_slot][row] != 0)
							{
								out.nulls[step.out_chunk_slot][row] = 1;
								continue;
							}
							out.int64_columns[step.out_chunk_slot][row] =
								out.int64_columns[step.in_b_chunk_slot][row];
						}
						else
							out.int64_columns[step.out_chunk_slot][row] = step.const_value;
						break;
				}
				out.nulls[step.out_chunk_slot][row] = 0;
			}
		}

		if (expr.n_steps == 0 || expr.output_chunk_slot != steps_[expr.first_step_idx + expr.n_steps - 1].out_chunk_slot)
			elog(ERROR, "pg_volvec: projection output slot descriptor mismatch");
	}

	op_state.current_input_drained = out.count > 0;
	return out.count > 0 ? OperatorResultType::HAVE_MORE_OUTPUT
	                     : OperatorResultType::NEED_MORE_INPUT;
}

}  /* namespace pipeline */
}  /* namespace pg_volvec */

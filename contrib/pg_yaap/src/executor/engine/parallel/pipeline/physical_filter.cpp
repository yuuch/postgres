#include "parallel/pipeline/physical_filter.hpp"

extern "C" {
#include "postgres.h"
#include "utils/elog.h"
}

#include <algorithm>
#include <cstring>

namespace pg_yaap {
namespace pipeline {

namespace {

static inline const char *
ResolveFilterConstPtr(const FilterStep &step, const char *string_consts)
{
	if (step.const_len == 0)
		return "";
	if (step.const_offset == UINT32_MAX)
		return reinterpret_cast<const char *>(&step.const_value);
	if (string_consts == nullptr)
		return nullptr;
	return string_consts + step.const_offset;
}

static inline uint16_t
RequiredFilterBoolRegs(const PgVector<FilterExprDesc> &exprs)
{
	uint16_t max_reg = 0;
	for (const FilterExprDesc &expr : exprs)
		max_reg = std::max<uint16_t>(max_reg, static_cast<uint16_t>(expr.output_bool_reg + 1));
	return max_reg;
}

static bool
MatchPercentLikePattern(const char *lhs, uint32_t lhs_len,
						  const char *pattern, uint32_t pattern_len)
{
	if (lhs == nullptr || pattern == nullptr)
		return false;
	if (pattern_len == 0)
		return lhs_len == 0;

	uint32_t token_start = 0;
	uint32_t match_pos = 0;
	bool anchored_start = pattern[0] != '%';
	bool first_token = true;
	for (uint32_t i = 0; i <= pattern_len; ++i)
	{
		if (i < pattern_len && pattern[i] != '%')
			continue;
		const uint32_t token_len = i - token_start;
		if (token_len > 0)
		{
			if (first_token && anchored_start)
			{
				if (lhs_len < token_len || std::memcmp(lhs, pattern + token_start, token_len) != 0)
					return false;
				match_pos = token_len;
			}
			else
			{
				bool found = false;
				for (; match_pos + token_len <= lhs_len; ++match_pos)
				{
					if (std::memcmp(lhs + match_pos, pattern + token_start, token_len) == 0)
					{
						match_pos += token_len;
						found = true;
						break;
					}
				}
				if (!found)
					return false;
			}
			first_token = false;
		}
		token_start = i + 1;
	}

	if (pattern_len > 0 && pattern[pattern_len - 1] != '%')
	{
		const uint32_t trailing_len = pattern_len - token_start;
		if (trailing_len > lhs_len)
			return false;
		return std::memcmp(lhs + lhs_len - trailing_len, pattern + token_start, trailing_len) == 0;
	}
	return true;
}

static inline bool
EvalFilterStepAtRow(const FilterStep &step,
					const PipelineChunk &filter_chunk,
					const char *string_consts,
					uint8_t *bool_values,
					uint16_t row_idx)
{
	bool result = false;
	switch (step.op)
	{
		case FilterStepOp::INT32_CMP_CONST:
		{
			if (!filter_chunk.nulls[step.left_idx][row_idx])
			{
				const int32_t l = filter_chunk.get_int32(step.left_idx, row_idx);
				const int32_t r = static_cast<int32_t>(step.const_value);
				switch (step.cmp_op)
				{
					case QualOp::LE: result = l <= r; break;
					case QualOp::LT: result = l < r; break;
					case QualOp::EQ: result = l == r; break;
					case QualOp::GE: result = l >= r; break;
					case QualOp::GT: result = l > r; break;
					case QualOp::NE: result = l != r; break;
				}
			}
			break;
		}
		case FilterStepOp::INT64_CMP_CONST:
		{
			if (!filter_chunk.nulls[step.left_idx][row_idx])
			{
				const int64_t l = filter_chunk.get_int64(step.left_idx, row_idx);
				const int64_t r = static_cast<int64_t>(step.const_value);
				switch (step.cmp_op)
				{
					case QualOp::LE: result = l <= r; break;
					case QualOp::LT: result = l < r; break;
					case QualOp::EQ: result = l == r; break;
					case QualOp::GE: result = l >= r; break;
					case QualOp::GT: result = l > r; break;
					case QualOp::NE: result = l != r; break;
				}
			}
			break;
		}
		case FilterStepOp::INT32_CMP_VAR:
		{
			if (!filter_chunk.nulls[step.left_idx][row_idx] && !filter_chunk.nulls[step.right_idx][row_idx])
			{
				const int32_t l = filter_chunk.get_int32(step.left_idx, row_idx);
				const int32_t r = filter_chunk.get_int32(step.right_idx, row_idx);
				switch (step.cmp_op)
				{
					case QualOp::LE: result = l <= r; break;
					case QualOp::LT: result = l < r; break;
					case QualOp::EQ: result = l == r; break;
					case QualOp::GE: result = l >= r; break;
					case QualOp::GT: result = l > r; break;
					case QualOp::NE: result = l != r; break;
				}
			}
			break;
		}
		case FilterStepOp::INT64_CMP_VAR:
		{
			if (!filter_chunk.nulls[step.left_idx][row_idx] && !filter_chunk.nulls[step.right_idx][row_idx])
			{
				const int64_t l = filter_chunk.get_int64(step.left_idx, row_idx);
				const int64_t r = filter_chunk.get_int64(step.right_idx, row_idx);
				switch (step.cmp_op)
				{
					case QualOp::LE: result = l <= r; break;
					case QualOp::LT: result = l < r; break;
					case QualOp::EQ: result = l == r; break;
					case QualOp::GE: result = l >= r; break;
					case QualOp::GT: result = l > r; break;
					case QualOp::NE: result = l != r; break;
				}
			}
			break;
		}
		case FilterStepOp::STRING_EQ_CONST:
		case FilterStepOp::STRING_NE_CONST:
		{
			if (!filter_chunk.nulls[step.left_idx][row_idx])
			{
				const VecStringRef ref = filter_chunk.get_string_ref(step.left_idx, row_idx);
				const char *lhs = filter_chunk.get_string_ptr(step.left_idx, row_idx);
				const char *rhs = ResolveFilterConstPtr(step, string_consts);
				const bool eq = (lhs != nullptr || ref.len == 0) && rhs != nullptr &&
					ref.len == step.const_len &&
					(step.const_len == 0 || std::memcmp(lhs, rhs, step.const_len) == 0);
				result = step.op == FilterStepOp::STRING_EQ_CONST ? eq : !eq;
			}
			break;
		}
		case FilterStepOp::STRING_PREFIX_LIKE:
		{
			if (!filter_chunk.nulls[step.left_idx][row_idx])
			{
				const VecStringRef ref = filter_chunk.get_string_ref(step.left_idx, row_idx);
				const char *rhs = ResolveFilterConstPtr(step, string_consts);
				result = rhs != nullptr && ref.len >= step.const_len;
				if (result && step.const_len != 0)
				{
					if (step.const_len <= sizeof(ref.prefix))
						result = std::memcmp(&ref.prefix, rhs, step.const_len) == 0;
					else
					{
						const char *lhs = filter_chunk.get_string_ptr(step.left_idx, row_idx);
						result = lhs != nullptr && std::memcmp(lhs, rhs, step.const_len) == 0;
					}
				}
			}
			break;
		}
		case FilterStepOp::STRING_CONTAINS_LIKE:
		{
			if (!filter_chunk.nulls[step.left_idx][row_idx])
			{
				const VecStringRef ref = filter_chunk.get_string_ref(step.left_idx, row_idx);
				const char *rhs = ResolveFilterConstPtr(step, string_consts);
				const char *lhs = filter_chunk.get_string_ptr(step.left_idx, row_idx);
				result = rhs != nullptr && lhs != nullptr && step.const_len <= ref.len;
				if (result)
				{
					for (uint32_t start = 0; start + step.const_len <= ref.len; ++start)
					{
						if (std::memcmp(lhs + start, rhs, step.const_len) == 0)
						{
							result = true;
							break;
						}
						result = false;
					}
				}
			}
			break;
		}
		case FilterStepOp::STRING_SQL_LIKE:
		{
			if (!filter_chunk.nulls[step.left_idx][row_idx])
			{
				const VecStringRef ref = filter_chunk.get_string_ref(step.left_idx, row_idx);
				const char *rhs = ResolveFilterConstPtr(step, string_consts);
				const char *lhs = filter_chunk.get_string_ptr(step.left_idx, row_idx);
				result = MatchPercentLikePattern(lhs, ref.len, rhs, step.const_len);
			}
			break;
		}
		case FilterStepOp::BOOL_AND:
			result = bool_values[step.left_idx] && bool_values[step.right_idx];
			break;
		case FilterStepOp::BOOL_OR:
			result = bool_values[step.left_idx] || bool_values[step.right_idx];
			break;
		case FilterStepOp::BOOL_NOT:
			result = !bool_values[step.left_idx];
			break;
		default:
			elog(ERROR, "pg_yaap: unsupported filter step op %u", static_cast<unsigned>(step.op));
	}
	bool_values[step.out_bool_reg] = result ? 1 : 0;
	return result;
}

static void
PopulateFilterChunk(const PgVector<FilterInputDesc> &inputs,
					  const PipelineChunk &in,
					  PipelineChunk &filter_chunk)
{
	filter_chunk.reset();
	filter_chunk.count = in.count;
	for (const FilterInputDesc &input : inputs)
	{
		const uint8_t src_slot = input.attno > 0 ? static_cast<uint8_t>(input.attno - 1) : input.dst_col;
		const uint8_t dst_slot = input.dst_col;
		for (uint16_t row = 0; row < in.count; ++row)
		{
			filter_chunk.nulls[dst_slot][row] = in.nulls[src_slot][row];
			if (in.nulls[src_slot][row] != 0)
				continue;
			switch (input.decode_kind)
			{
				case ColumnDecodeKind::INT32_CHAR:
				case ColumnDecodeKind::INT32_DATE:
				case ColumnDecodeKind::INT32_INT4:
					filter_chunk.int32_columns[dst_slot][row] = in.get_int32(src_slot, row);
					break;
				case ColumnDecodeKind::INT64_INT8:
					if (input.source_decode_kind == ColumnDecodeKind::INT32_INT4 ||
						input.source_decode_kind == ColumnDecodeKind::INT32_CHAR ||
						input.source_decode_kind == ColumnDecodeKind::INT32_DATE)
						filter_chunk.int64_columns[dst_slot][row] = static_cast<int64_t>(in.get_int32(src_slot, row));
					else
						filter_chunk.int64_columns[dst_slot][row] = in.get_int64(src_slot, row);
					break;
				case ColumnDecodeKind::INT64_NUMERIC_SCALED:
					if (input.source_decode_kind == ColumnDecodeKind::INT32_INT4 ||
						input.source_decode_kind == ColumnDecodeKind::INT32_CHAR ||
						input.source_decode_kind == ColumnDecodeKind::INT32_DATE)
					{
						int64_t factor = 1;
						for (uint8_t i = 0; i < input.numeric_scale; ++i)
							factor *= 10;
						filter_chunk.int64_columns[dst_slot][row] =
							static_cast<int64_t>(in.get_int32(src_slot, row)) * factor;
					}
					else if (input.source_decode_kind == ColumnDecodeKind::INT64_INT8)
					{
						int64_t factor = 1;
						for (uint8_t i = 0; i < input.numeric_scale; ++i)
							factor *= 10;
						filter_chunk.int64_columns[dst_slot][row] =
							in.get_int64(src_slot, row) * factor;
					}
					else
						filter_chunk.int64_columns[dst_slot][row] = in.get_int64(src_slot, row);
					break;
				case ColumnDecodeKind::STRING_REF:
				{
					const VecStringRef ref = in.get_string_ref(src_slot, row);
					const char *ptr = in.get_string_ptr(src_slot, row);
					filter_chunk.string_columns[dst_slot][row] = filter_chunk.store_string_bytes(ptr, ref.len);
					break;
				}
				default:
					elog(ERROR, "pg_yaap: unsupported filter decode kind %u", static_cast<unsigned>(input.decode_kind));
			}
		}
	}
}

static void
CompactSelectedRows(PipelineChunk &out, const uint16_t *selected_rows, uint16_t selected_count)
{
	if (selected_count == out.count)
		return;
	for (uint16_t write_idx = 0; write_idx < selected_count; ++write_idx)
	{
		const uint16_t row = selected_rows[write_idx];
		if (write_idx == row)
			continue;
		for (uint8_t slot = 0; slot < 16; ++slot)
		{
			out.double_columns[slot][write_idx] = out.double_columns[slot][row];
			out.int64_columns[slot][write_idx] = out.int64_columns[slot][row];
			out.int32_columns[slot][write_idx] = out.int32_columns[slot][row];
			out.string_columns[slot][write_idx] = out.string_columns[slot][row];
			out.nulls[slot][write_idx] = out.nulls[slot][row];
		}
	}
	out.count = selected_count;
	out.has_selection = false;
}

} // namespace

std::unique_ptr<OperatorState>
PhysicalFilter::GetOperatorState(ExecCtx &ctx)
{
	(void) ctx;
	return std::make_unique<FilterOperatorState>();
}

OperatorResultType
PhysicalFilter::Execute(ExecCtx &ctx, PipelineChunk &in, PipelineChunk &out, OperatorState &state)
{
	(void) ctx;
	auto &op_state = static_cast<FilterOperatorState &>(state);
	if (op_state.current_input_drained)
	{
		op_state.current_input_drained = false;
		out.reset();
		return OperatorResultType::NEED_MORE_INPUT;
	}

	out = in;
	if (out.has_any_dictionary())
		out.flatten();
	if (filter_exprs_.empty())
	{
		op_state.current_input_drained = out.count > 0;
		return out.count > 0 ? OperatorResultType::HAVE_MORE_OUTPUT : OperatorResultType::NEED_MORE_INPUT;
	}

	if (!op_state.filter_chunk)
		op_state.filter_chunk = std::make_unique<PipelineChunk>();
	PopulateFilterChunk(filter_inputs_, out, *op_state.filter_chunk);

	const uint16_t required_bool_regs = RequiredFilterBoolRegs(filter_exprs_);
	const char *filter_string_consts = filter_string_consts_.empty() ? nullptr : filter_string_consts_.data();
	uint16_t selected_rows[PIPELINE_DEFAULT_CHUNK_SIZE];
	uint16_t selected_count = 0;
	for (uint16_t row = 0; row < out.count; ++row)
	{
		if (required_bool_regs > 0)
			std::memset(op_state.bool_values, 0, required_bool_regs * sizeof(op_state.bool_values[0]));
		bool pass = true;
		for (const FilterExprDesc &expr : filter_exprs_)
		{
			const uint16_t expr_end = expr.first_step_idx + expr.n_steps;
			if (expr_end > filter_steps_.size())
				elog(ERROR, "pg_yaap: filter expression step range overflow");
			for (uint16_t step_idx = expr.first_step_idx; step_idx < expr_end; ++step_idx)
				EvalFilterStepAtRow(filter_steps_[step_idx], *op_state.filter_chunk, filter_string_consts, op_state.bool_values, row);
			if (expr.output_bool_reg >= FILTER_MAX_BOOL_REGS || !op_state.bool_values[expr.output_bool_reg])
			{
				pass = false;
				break;
			}
		}
		if (pass)
			selected_rows[selected_count++] = row;
	}

	if (selected_count == 0)
	{
		out.reset();
		return OperatorResultType::NEED_MORE_INPUT;
	}
	CompactSelectedRows(out, selected_rows, selected_count);
	op_state.current_input_drained = out.count > 0;
	return OperatorResultType::HAVE_MORE_OUTPUT;
}

}  /* namespace pipeline */
}  /* namespace pg_yaap */

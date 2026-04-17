#include "exec/internal.hpp"

namespace pg_volvec {

VecSortState::VecSortState(std::unique_ptr<VecPlanState> left, Sort *node,
						   VolVecVector<VecSortKeyDesc> key_descs,
						   int output_ncols)
	: left_(std::move(left)),
	  memory_context_(CurrentMemoryContext),
	  payload_chunks_(PgMemoryContextAllocator<DataChunk<DEFAULT_CHUNK_SIZE> *>(memory_context_)),
	  rows_(PgMemoryContextAllocator<VecRowRef>(memory_context_)),
	  key_descs_(PgMemoryContextAllocator<VecSortKeyDesc>(memory_context_)),
	  key_lanes_(PgMemoryContextAllocator<VecSortKeyLane>(memory_context_)),
	  emit_pos_(0),
	  output_ncols_(0),
	  materialized_(false)
{
	int max_output_col = output_ncols > 0 ? output_ncols : list_length(node->plan.targetlist);

	for (const auto &key_desc : key_descs)
	{
		key_descs_.push_back(key_desc);
		key_lanes_.emplace_back(key_desc, memory_context_);
		max_output_col = Max(max_output_col, (int) key_desc.col_idx + 1);
	}
	output_ncols_ = Min(max_output_col, 16);
}

VecSortState::~VecSortState()
{
	for (auto *chunk : payload_chunks_)
		delete chunk;
}

DataChunk<DEFAULT_CHUNK_SIZE> *
VecSortState::allocate_payload_chunk()
{
	MemoryContext old_context = MemoryContextSwitchTo(memory_context_);
	DataChunk<DEFAULT_CHUNK_SIZE> *chunk = new DataChunk<DEFAULT_CHUNK_SIZE>();
	MemoryContextSwitchTo(old_context);
	payload_chunks_.push_back(chunk);
	return chunk;
}

void
VecSortState::copy_row(const DataChunk<DEFAULT_CHUNK_SIZE> &src, int src_row,
					   DataChunk<DEFAULT_CHUNK_SIZE> &dst, int dst_row) const
{
		for (int col = 0; col < output_ncols_; col++)
		{
			dst.double_columns[col][dst_row] = src.double_columns[col][src_row];
			dst.int64_columns[col][dst_row] = src.int64_columns[col][src_row];
			dst.int32_columns[col][dst_row] = src.int32_columns[col][src_row];
			dst.nulls[col][dst_row] = src.nulls[col][src_row];
			if (!dst.nulls[col][dst_row])
				dst.string_columns[col][dst_row] =
					CopyStringRefToChunk(dst, src, src.string_columns[col][src_row]);
			else
				dst.string_columns[col][dst_row] = VecStringRef{0, 0, 0};
		}
	}

void
VecSortState::append_sort_key(uint32_t ordinal,
							  const DataChunk<DEFAULT_CHUNK_SIZE> &input,
							  int src_row)
{
	for (auto &lane : key_lanes_)
	{
		const VecSortKeyDesc &key = lane.desc;
		bool is_null = input.nulls[key.col_idx][src_row] != 0;

		Assert(lane.nulls.size() == ordinal);
		lane.nulls.push_back((uint8_t) is_null);
		if (is_null)
		{
			lane.i32_values.push_back(0);
			lane.i64_values.push_back(0);
			lane.u64_values.push_back(0);
			lane.string_values.push_back(VecStringRef{0, 0, 0});
			continue;
		}

		switch (key.storage_kind)
		{
			case VecOutputStorageKind::Int32:
				lane.i32_values.push_back(input.int32_columns[key.col_idx][src_row]);
				lane.i64_values.push_back(0);
				lane.u64_values.push_back(0);
				lane.string_values.push_back(VecStringRef{0, 0, 0});
				break;
			case VecOutputStorageKind::Int64:
			case VecOutputStorageKind::NumericScaledInt64:
				lane.i32_values.push_back(0);
				lane.i64_values.push_back(input.int64_columns[key.col_idx][src_row]);
				lane.u64_values.push_back(0);
				lane.string_values.push_back(VecStringRef{0, 0, 0});
				break;
			case VecOutputStorageKind::Double:
				lane.i32_values.push_back(0);
				lane.i64_values.push_back(0);
				lane.u64_values.push_back(EncodeFloat8SortKey(input.double_columns[key.col_idx][src_row]));
				lane.string_values.push_back(VecStringRef{0, 0, 0});
				break;
			case VecOutputStorageKind::StringRef:
			{
				VecStringRef ref = input.string_columns[key.col_idx][src_row];

				lane.i32_values.push_back(0);
				lane.i64_values.push_back(0);
				lane.u64_values.push_back(0);
				lane.string_values.push_back(
					lane.store_string_bytes(input.get_string_ptr(ref), ref.len));
				break;
			}
			case VecOutputStorageKind::NumericAvgPair:
				elog(ERROR, "pg_volvec vector sort does not yet support numeric average sort keys");
				break;
		}
	}
}

void
VecSortState::append_batch(const DataChunk<DEFAULT_CHUNK_SIZE> &input)
{
	int active_count = input.has_selection ? input.sel.count : input.count;
	DataChunk<DEFAULT_CHUNK_SIZE> *dst =
		payload_chunks_.empty() ? allocate_payload_chunk() : payload_chunks_.back();

	for (int s = 0; s < active_count; s++)
	{
		int src_row = input.has_selection ? input.sel.row_ids[s] : s;
		int dst_row;
		uint32_t ordinal;

		if (dst->count >= DEFAULT_CHUNK_SIZE)
			dst = allocate_payload_chunk();

		dst_row = dst->count;
		ordinal = (uint32_t) rows_.size();
		copy_row(input, src_row, *dst, dst_row);
		rows_.push_back(VecRowRef{ordinal, (uint32_t) (payload_chunks_.size() - 1), (uint16_t) dst_row});
		append_sort_key(ordinal, input, src_row);
		dst->count++;
	}
}

int
VecSortState::compare_string_ref(const VecSortKeyLane &lane,
								 const VecStringRef &left,
								 const VecStringRef &right) const
{
	const char *left_ptr = lane.get_string_ptr(left);
	const char *right_ptr = lane.get_string_ptr(right);
	uint32_t left_len = left.len;
	uint32_t right_len = right.len;
	int cmp_len;
	int cmp;

	if (lane.desc.sql_type == BPCHAROID)
	{
		while (left_len > 0 && left_ptr[left_len - 1] == ' ')
			left_len--;
		while (right_len > 0 && right_ptr[right_len - 1] == ' ')
			right_len--;
	}

	cmp_len = Min((int) left_len, (int) right_len);
	cmp = memcmp(left_ptr, right_ptr, cmp_len);
	if (cmp < 0)
		return -1;
	if (cmp > 0)
		return 1;
	if (left_len < right_len)
		return -1;
	if (left_len > right_len)
		return 1;
	return 0;
}

bool
VecSortState::row_less(const VecRowRef &left, const VecRowRef &right) const
{
	for (const auto &lane : key_lanes_)
	{
		bool left_null = lane.nulls[left.ordinal] != 0;
		bool right_null = lane.nulls[right.ordinal] != 0;
		int cmp = 0;

		if (left_null != right_null)
			return lane.desc.nulls_first ? left_null : !left_null;
		if (left_null)
			continue;

		switch (lane.desc.storage_kind)
		{
			case VecOutputStorageKind::Int32:
				if (lane.i32_values[left.ordinal] < lane.i32_values[right.ordinal])
					cmp = -1;
				else if (lane.i32_values[left.ordinal] > lane.i32_values[right.ordinal])
					cmp = 1;
				break;
			case VecOutputStorageKind::Int64:
			case VecOutputStorageKind::NumericScaledInt64:
				if (lane.i64_values[left.ordinal] < lane.i64_values[right.ordinal])
					cmp = -1;
				else if (lane.i64_values[left.ordinal] > lane.i64_values[right.ordinal])
					cmp = 1;
				break;
			case VecOutputStorageKind::Double:
				if (lane.u64_values[left.ordinal] < lane.u64_values[right.ordinal])
					cmp = -1;
				else if (lane.u64_values[left.ordinal] > lane.u64_values[right.ordinal])
					cmp = 1;
				break;
			case VecOutputStorageKind::StringRef:
				cmp = compare_string_ref(lane,
										 lane.string_values[left.ordinal],
										 lane.string_values[right.ordinal]);
				break;
			case VecOutputStorageKind::NumericAvgPair:
				elog(ERROR, "pg_volvec vector sort does not yet support numeric average sort keys");
				break;
		}

		if (cmp != 0)
			return lane.desc.descending ? (cmp > 0) : (cmp < 0);
	}

	return left.ordinal < right.ordinal;
}

void
VecSortState::materialize_and_sort()
{
	DataChunk<DEFAULT_CHUNK_SIZE> input;

	if (materialized_)
		return;

	while (left_->get_next_batch(input))
		append_batch(input);

	std::stable_sort(rows_.begin(), rows_.end(),
					 [this](const VecRowRef &left, const VecRowRef &right)
					 {
						 return row_less(left, right);
					 });
	emit_pos_ = 0;
	materialized_ = true;
}

bool
VecSortState::get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk)
{
	if (!materialized_)
		materialize_and_sort();

	chunk.reset();
	while (emit_pos_ < rows_.size() && chunk.count < DEFAULT_CHUNK_SIZE)
	{
		const VecRowRef &row = rows_[emit_pos_];
		const DataChunk<DEFAULT_CHUNK_SIZE> *src = payload_chunks_[row.chunk_idx];

		copy_row(*src, row.row_idx, chunk, chunk.count);
		chunk.count++;
		emit_pos_++;
	}

	return chunk.count > 0;
}

} /* namespace pg_volvec */

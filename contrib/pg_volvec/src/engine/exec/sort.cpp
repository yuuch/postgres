#include "exec/internal.hpp"

namespace pg_volvec {

VecSortState::VecSortState(std::unique_ptr<VecPlanState> left, Sort *node,
						   VolVecVector<VecSortKeyDesc> key_descs,
						   int output_ncols)
	: left_(std::move(left)),
	  memory_context_(CurrentMemoryContext),
	  buffer_chunks_(PgMemoryContextAllocator<DataChunk<DEFAULT_CHUNK_SIZE> *>(memory_context_)),
	  buffer_rows_(0),
	  buffer_limit_(100000),
	  runs_(PgMemoryContextAllocator<SortedRun>(memory_context_)),
	  key_descs_(PgMemoryContextAllocator<VecSortKeyDesc>(memory_context_)),
	  key_lanes_(PgMemoryContextAllocator<VecSortKeyLane>(memory_context_)),
	  output_ncols_(0),
	  finalized_(false),
	  merge_heap_(nullptr)
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
	for (auto *chunk : buffer_chunks_)
		delete chunk;
	for (auto &run : runs_)
		for (auto *chunk : run.chunks)
			delete chunk;
}

void
VecSortState::reset_materialized_state()
{
	for (auto *chunk : buffer_chunks_)
		delete chunk;
	buffer_chunks_.clear();
	buffer_rows_ = 0;
	
	for (auto &run : runs_)
		for (auto *chunk : run.chunks)
			delete chunk;
	runs_.clear();
	
	for (auto &lane : key_lanes_)
	{
		lane.nulls.clear();
		lane.i32_values.clear();
		lane.i64_values.clear();
		lane.u64_values.clear();
		lane.string_values.clear();
		lane.string_arena.clear();
	}
	
	finalized_ = false;
	merge_heap_.reset();
}

void
VecSortState::reset_external_input()
{
	reset_materialized_state();
}

void
VecSortState::copy_chunk(const DataChunk<DEFAULT_CHUNK_SIZE> &src, DataChunk<DEFAULT_CHUNK_SIZE> &dst)
{
	dst.count = src.count;
	dst.has_selection = false;
	
	for (int col = 0; col < output_ncols_; col++)
	{
		VecOutputColMeta meta;
		bool copy_string =
			left_ != nullptr &&
			left_->lookup_output_col_meta(col + 1, &meta) &&
			meta.storage_kind == VecOutputStorageKind::StringRef;

		if (src.has_selection)
		{
			for (uint32_t i = 0; i < src.count; i++)
			{
				uint32_t src_row = src.sel.row_ids[i];
				dst.nulls[col][i] = src.nulls[col][src_row];
				dst.int32_columns[col][i] = src.int32_columns[col][src_row];
				dst.int64_columns[col][i] = src.int64_columns[col][src_row];
				dst.double_columns[col][i] = src.double_columns[col][src_row];
				if (copy_string && !dst.nulls[col][i])
					dst.string_columns[col][i] =
						CopyStringRefToChunk(dst, src, src.string_columns[col][src_row]);
				else
					dst.string_columns[col][i] = VecStringRef{0, 0, 0};
			}
		}
		else
		{
			memcpy(dst.nulls[col], src.nulls[col], src.count * sizeof(uint8_t));
			memcpy(dst.int32_columns[col], src.int32_columns[col], src.count * sizeof(int32_t));
			memcpy(dst.int64_columns[col], src.int64_columns[col], src.count * sizeof(int64_t));
			memcpy(dst.double_columns[col], src.double_columns[col], src.count * sizeof(double));
			for (uint32_t i = 0; i < src.count; i++)
			{
				if (copy_string && !dst.nulls[col][i])
					dst.string_columns[col][i] =
						CopyStringRefToChunk(dst, src, src.string_columns[col][i]);
				else
					dst.string_columns[col][i] = VecStringRef{0, 0, 0};
			}
		}
	}
}

void
VecSortState::append_external_batch(const DataChunk<DEFAULT_CHUNK_SIZE> &input)
{
	MemoryContext old_context = MemoryContextSwitchTo(memory_context_);
	
	DataChunk<DEFAULT_CHUNK_SIZE> *stored = new DataChunk<DEFAULT_CHUNK_SIZE>();
	copy_chunk(input, *stored);
	
	buffer_chunks_.push_back(stored);
	buffer_rows_ += stored->count;
	
	MemoryContextSwitchTo(old_context);
	
	if (buffer_rows_ >= buffer_limit_)
		flush_buffer_to_run();
}

void
VecSortState::append_sort_key_from_chunk(uint32_t ordinal,
										 const DataChunk<DEFAULT_CHUNK_SIZE> &chunk,
										 uint32_t chunk_id,
										 int row_offset)
{
	for (auto &lane : key_lanes_)
	{
		const VecSortKeyDesc &key = lane.desc;
		bool is_null = chunk.nulls[key.col_idx][row_offset] != 0;

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
				lane.i32_values.push_back(chunk.int32_columns[key.col_idx][row_offset]);
				lane.i64_values.push_back(0);
				lane.u64_values.push_back(0);
				lane.string_values.push_back(VecStringRef{0, 0, 0});
				break;
			case VecOutputStorageKind::Int64:
			case VecOutputStorageKind::NumericScaledInt64:
				lane.i32_values.push_back(0);
				lane.i64_values.push_back(chunk.int64_columns[key.col_idx][row_offset]);
				lane.u64_values.push_back(0);
				lane.string_values.push_back(VecStringRef{0, 0, 0});
				break;
			case VecOutputStorageKind::Double:
				lane.i32_values.push_back(0);
				lane.i64_values.push_back(0);
				lane.u64_values.push_back(EncodeFloat8SortKey(chunk.double_columns[key.col_idx][row_offset]));
				lane.string_values.push_back(VecStringRef{0, 0, 0});
				break;
			case VecOutputStorageKind::StringRef:
			{
				VecStringRef ref = chunk.string_columns[key.col_idx][row_offset];

				lane.i32_values.push_back(0);
				lane.i64_values.push_back(0);
				lane.u64_values.push_back(0);
				lane.string_values.push_back(
					lane.store_string_bytes(chunk.get_string_ptr(ref), ref.len));
				break;
			}
			case VecOutputStorageKind::NumericAvgPair:
				elog(ERROR, "pg_volvec vector sort does not yet support numeric average sort keys");
				break;
		}
	}
}

void
VecSortState::build_sort_keys_for_run(SortedRun &run)
{
	for (size_t i = 0; i < run.chunks.size(); i++)
	{
		DataChunk<DEFAULT_CHUNK_SIZE> *chunk = run.chunks[i];
		for (uint32_t row = 0; row < chunk->count; row++)
		{
			uint32_t ordinal = (uint32_t)(key_lanes_[0].nulls.size());
			append_sort_key_from_chunk(ordinal, *chunk, (uint32_t)i, row);
		}
	}
}

void
VecSortState::flush_buffer_to_run()
{
	if (buffer_chunks_.empty())
		return;
	
	MemoryContext old_context = MemoryContextSwitchTo(memory_context_);
	
	runs_.emplace_back(memory_context_);
	SortedRun &run = runs_.back();
	
	run.chunks = std::move(buffer_chunks_);
	run.total_rows = buffer_rows_;
	run.cursor = 0;
	
	for (uint32_t chunk_id = 0; chunk_id < run.chunks.size(); chunk_id++)
	{
		uint32_t chunk_rows = run.chunks[chunk_id]->count;
		for (uint32_t row_offset = 0; row_offset < chunk_rows; row_offset++)
		{
			uint64_t global_id = GlobalRowId::encode(chunk_id, row_offset);
			run.global_sel.push_back(global_id);
		}
	}
	
	build_sort_keys_for_run(run);
	sort_run(run);
	
	buffer_chunks_.clear();
	buffer_chunks_ = VolVecVector<DataChunk<DEFAULT_CHUNK_SIZE> *>(
		PgMemoryContextAllocator<DataChunk<DEFAULT_CHUNK_SIZE> *>(memory_context_));
	buffer_rows_ = 0;
	
	MemoryContextSwitchTo(old_context);
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

int
VecSortState::compare_global_rows(const SortedRun &run, uint64_t global_a, uint64_t global_b) const
{
	uint32_t chunk_a, offset_a, chunk_b, offset_b;
	GlobalRowId::decode(global_a, chunk_a, offset_a);
	GlobalRowId::decode(global_b, chunk_b, offset_b);
	
	uint32_t ordinal_a = chunk_a * DEFAULT_CHUNK_SIZE + offset_a;
	uint32_t ordinal_b = chunk_b * DEFAULT_CHUNK_SIZE + offset_b;
	
	for (const auto &lane : key_lanes_)
	{
		bool left_null = lane.nulls[ordinal_a] != 0;
		bool right_null = lane.nulls[ordinal_b] != 0;
		int cmp = 0;

		if (left_null != right_null)
			return lane.desc.nulls_first ? (left_null ? -1 : 1) : (left_null ? 1 : -1);
		if (left_null)
			continue;

		switch (lane.desc.storage_kind)
		{
			case VecOutputStorageKind::Int32:
				if (lane.i32_values[ordinal_a] < lane.i32_values[ordinal_b])
					cmp = -1;
				else if (lane.i32_values[ordinal_a] > lane.i32_values[ordinal_b])
					cmp = 1;
				break;
			case VecOutputStorageKind::Int64:
			case VecOutputStorageKind::NumericScaledInt64:
				if (lane.i64_values[ordinal_a] < lane.i64_values[ordinal_b])
					cmp = -1;
				else if (lane.i64_values[ordinal_a] > lane.i64_values[ordinal_b])
					cmp = 1;
				break;
			case VecOutputStorageKind::Double:
				if (lane.u64_values[ordinal_a] < lane.u64_values[ordinal_b])
					cmp = -1;
				else if (lane.u64_values[ordinal_a] > lane.u64_values[ordinal_b])
					cmp = 1;
				break;
			case VecOutputStorageKind::StringRef:
				cmp = compare_string_ref(lane,
										 lane.string_values[ordinal_a],
										 lane.string_values[ordinal_b]);
				break;
			case VecOutputStorageKind::NumericAvgPair:
				elog(ERROR, "pg_volvec vector sort does not yet support numeric average sort keys");
				break;
		}

		if (cmp != 0)
			return lane.desc.descending ? -cmp : cmp;
	}

	return 0;
}

void
VecSortState::sort_run(SortedRun &run)
{
	std::stable_sort(run.global_sel.begin(), run.global_sel.end(),
					 [this, &run](uint64_t a, uint64_t b) {
						 return compare_global_rows(run, a, b) < 0;
					 });
}

void
VecSortState::finish_external_input()
{
	if (!buffer_chunks_.empty())
		flush_buffer_to_run();
	
	for (auto &run : runs_)
		run.cursor = 0;
	
	finalized_ = true;
}

bool
VecSortState::configure_source_block_range(BlockNumber start_block, uint32_t nblocks)
{
	reset_materialized_state();
	bool ok = left_ != nullptr &&
		left_->configure_source_block_range(start_block, nblocks);

	if (pg_volvec_trace_hooks && !ok)
		elog(LOG,
			 "pg_volvec: sort block range configure failed start=%u nblocks=%u left=%s",
			 start_block,
			 nblocks,
			 left_ != nullptr ? "ok" : "null");
	return ok;
}

void
VecSortState::clear_source_block_range()
{
	if (left_ != nullptr)
		left_->clear_source_block_range();
	reset_materialized_state();
}

void
VecSortState::copy_row(const DataChunk<DEFAULT_CHUNK_SIZE> &src, int src_row,
					   DataChunk<DEFAULT_CHUNK_SIZE> &dst, int dst_row) const
{
	for (int col = 0; col < output_ncols_; col++)
	{
		VecOutputColMeta meta;
		bool copy_string =
			left_ != nullptr &&
			left_->lookup_output_col_meta(col + 1, &meta) &&
			meta.storage_kind == VecOutputStorageKind::StringRef;

		dst.double_columns[col][dst_row] = src.double_columns[col][src_row];
		dst.int64_columns[col][dst_row] = src.int64_columns[col][src_row];
		dst.int32_columns[col][dst_row] = src.int32_columns[col][src_row];
		dst.nulls[col][dst_row] = src.nulls[col][src_row];
		if (copy_string && !dst.nulls[col][dst_row])
			dst.string_columns[col][dst_row] =
				CopyStringRefToChunk(dst, src, src.string_columns[col][src_row]);
		else
			dst.string_columns[col][dst_row] = VecStringRef{0, 0, 0};
	}
}

void
VecSortState::materialize_and_sort()
{
	DataChunk<DEFAULT_CHUNK_SIZE> input;

	if (finalized_)
		return;

	while (left_->get_next_batch(input))
		append_external_batch(input);

	finish_external_input();
}

bool
VecSortState::emit_from_single_run(DataChunk<DEFAULT_CHUNK_SIZE> &output)
{
	SortedRun &run = runs_[0];
	
	if (run.cursor >= run.total_rows)
		return false;
	
	output.reset();
	output.has_selection = false;
	
	uint32_t batch_size = std::min((uint32_t)DEFAULT_CHUNK_SIZE, run.total_rows - run.cursor);
	
	for (uint32_t i = 0; i < batch_size; i++)
	{
		uint64_t global_id = run.global_sel[run.cursor + i];
		uint32_t chunk_id, row_offset;
		GlobalRowId::decode(global_id, chunk_id, row_offset);
		
		DataChunk<DEFAULT_CHUNK_SIZE> *src_chunk = run.chunks[chunk_id];
		copy_row(*src_chunk, row_offset, output, output.count);
		output.count++;
	}
	
	run.cursor += batch_size;
	return true;
}

bool
VecSortState::MergeEntryComparator::operator()(const MergeEntry &a, const MergeEntry &b) const
{
	const SortedRun &run_a = sort_state->runs_[a.run_id];
	const SortedRun &run_b = sort_state->runs_[b.run_id];
	
	int cmp = sort_state->compare_global_rows(run_a, a.global_id, b.global_id);
	return cmp > 0;
}

bool
VecSortState::k_way_merge(DataChunk<DEFAULT_CHUNK_SIZE> &output)
{
	if (merge_heap_ == nullptr)
	{
		merge_heap_ = std::make_unique<std::priority_queue<MergeEntry, std::vector<MergeEntry>, MergeEntryComparator>>(
			MergeEntryComparator(this));
		
		for (uint32_t i = 0; i < runs_.size(); i++)
		{
			SortedRun &run = runs_[i];
			if (run.cursor < run.total_rows)
			{
				merge_heap_->push(MergeEntry(i, run.global_sel[run.cursor]));
				run.cursor++;
			}
		}
	}
	
	if (merge_heap_->empty())
		return false;
	
	output.reset();
	output.has_selection = false;
	
	while (output.count < DEFAULT_CHUNK_SIZE && !merge_heap_->empty())
	{
		MergeEntry top = merge_heap_->top();
		merge_heap_->pop();
		
		uint32_t chunk_id, row_offset;
		GlobalRowId::decode(top.global_id, chunk_id, row_offset);
		DataChunk<DEFAULT_CHUNK_SIZE> *src_chunk = runs_[top.run_id].chunks[chunk_id];
		copy_row(*src_chunk, row_offset, output, output.count);
		output.count++;
		
		SortedRun &run = runs_[top.run_id];
		if (run.cursor < run.total_rows)
		{
			merge_heap_->push(MergeEntry(top.run_id, run.global_sel[run.cursor]));
			run.cursor++;
		}
	}
	
	return output.count > 0;
}

bool
VecSortState::get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk)
{
	if (!finalized_)
		materialize_and_sort();

	if (runs_.empty())
		return false;
	
	if (runs_.size() == 1)
		return emit_from_single_run(chunk);
	
	return k_way_merge(chunk);
}

} /* namespace pg_volvec */

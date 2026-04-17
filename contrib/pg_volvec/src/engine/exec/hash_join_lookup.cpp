#include "exec/internal.hpp"

namespace pg_volvec {

VecHashJoinState::VecHashJoinState(std::unique_ptr<VecPlanState> outer,
								   std::unique_ptr<VecPlanState> inner,
								   int plan_node_id,
								   JoinType jointype,
								   bool build_outer_side,
								   int visible_output_count,
								   VolVecVector<VecJoinOutputCol> output_cols,
								   VolVecVector<VecHashJoinKeyCol> key_cols)
	: plan_node_id_(plan_node_id),
	  outer_(std::move(outer)),
	  inner_(std::move(inner)),
	  jointype_(jointype),
	  visible_output_count_(visible_output_count),
	  memory_context_(CurrentMemoryContext),
	  output_cols_(PgMemoryContextAllocator<VecJoinOutputCol>(memory_context_)),
	  key_cols_(PgMemoryContextAllocator<VecHashJoinKeyCol>(memory_context_)),
	  inner_payload_cols_(PgMemoryContextAllocator<VecHashPayloadCol>(memory_context_)),
	  inner_chunks_(PgMemoryContextAllocator<DataChunk<DEFAULT_CHUNK_SIZE> *>(memory_context_)),
	  bucket_heads_(PgMemoryContextAllocator<int32_t>(memory_context_)),
	  entries_(PgMemoryContextAllocator<VecHashEntry>(memory_context_)),
	  inner_entry_matched_(PgMemoryContextAllocator<uint8_t>(memory_context_)),
	  probe_rows_(PgMemoryContextAllocator<uint16_t>(memory_context_)),
	  probe_keys_(PgMemoryContextAllocator<VecHashJoinKey>(memory_context_)),
	  probe_hashes_(PgMemoryContextAllocator<uint32_t>(memory_context_)),
	  probe_next_entries_(PgMemoryContextAllocator<int32_t>(memory_context_)),
		  active_probe_sel_(PgMemoryContextAllocator<uint16_t>(memory_context_)),
		  next_probe_sel_(PgMemoryContextAllocator<uint16_t>(memory_context_)),
		  inner_built_(false),
		  probe_batch_ready_(false),
		  probe_input_exhausted_(false),
	  build_outer_side_(build_outer_side),
	  join_filter_program_(nullptr),
	  semi_build_marked_(false),
	  semi_build_emit_pos_(0),
	  anti_build_marked_(false),
	  anti_build_emit_pos_(0),
	  right_anti_marked_(false),
	  anti_outer_pos_(0),
	  right_anti_emit_pos_(0),
	  bucket_mask_(0)
{
	Assert(!build_outer_side_ ||
		   jointype_ == JOIN_INNER ||
		   jointype_ == JOIN_SEMI ||
		   jointype_ == JOIN_ANTI);
	for (const auto &key_col : key_cols)
		key_cols_.push_back(key_col);
	for (const auto &output_col : output_cols)
	{
		VecJoinOutputCol remapped = output_col;

		if ((build_outer_side_ && remapped.side == VecJoinSide::Outer) ||
			(!build_outer_side_ && remapped.side == VecJoinSide::Inner))
			remapped.input_col = ensure_inner_payload_col(remapped.input_col, remapped.meta);
		output_cols_.push_back(remapped);
	}
}

void
VecHashJoinState::set_join_filter_program(std::unique_ptr<VecExprProgram> program)
{
	join_filter_program_ = std::move(program);
}

VecHashJoinState::~VecHashJoinState()
{
	for (auto *chunk : inner_chunks_)
		delete chunk;
}

bool
VecHashJoinState::configure_source_block_range(BlockNumber start_block, uint32_t nblocks)
{
	VecPlanState *probe_state = build_outer_side_ ? inner_.get() : outer_.get();

	reset_probe_task_state();
	return probe_state != nullptr &&
		probe_state->configure_source_block_range(start_block, nblocks);
}

void
VecHashJoinState::clear_source_block_range()
{
	VecPlanState *probe_state = build_outer_side_ ? inner_.get() : outer_.get();

	reset_probe_task_state();
	if (probe_state != nullptr)
		probe_state->clear_source_block_range();
}

bool
VecHashJoinState::configure_build_input_block_range(BlockNumber start_block, uint32_t nblocks)
{
	VecPlanState *build_state = build_outer_side_ ? outer_.get() : inner_.get();

	return build_state != nullptr &&
		build_state->configure_source_block_range(start_block, nblocks);
}

void
VecHashJoinState::clear_build_input_block_range()
{
	VecPlanState *build_state = build_outer_side_ ? outer_.get() : inner_.get();

	if (build_state != nullptr)
		build_state->clear_source_block_range();
}

VecSeqScanState *
VecHashJoinState::find_parallel_source_scan_state()
{
	VecPlanState *probe_state = build_outer_side_ ? inner_.get() : outer_.get();

	return probe_state != nullptr ? probe_state->find_parallel_source_scan_state() : nullptr;
}

VecSeqScanState *
VecHashJoinState::find_parallel_build_scan_state()
{
	VecPlanState *build_state = build_outer_side_ ? outer_.get() : inner_.get();

	return build_state != nullptr ? build_state->find_parallel_source_scan_state() : nullptr;
}

void
VecHashJoinState::reset_probe_task_state()
{
	outer_chunk_.reset();
	probe_rows_.clear();
	probe_keys_.clear();
	probe_hashes_.clear();
	probe_next_entries_.clear();
	active_probe_sel_.clear();
	next_probe_sel_.clear();
	probe_batch_ready_ = false;
	probe_input_exhausted_ = false;
	anti_outer_pos_ = 0;
}

void
VecHashJoinState::init_hash_table(size_t expected_rows)
{
	size_t bucket_count = 1024;

	while (bucket_count < std::max<size_t>(expected_rows * 2, 1024))
		bucket_count <<= 1;

	bucket_heads_.assign(bucket_count, -1);
	bucket_mask_ = bucket_count - 1;
	entries_.clear();
	inner_entry_matched_.clear();
	entries_.reserve(expected_rows > 0 ? expected_rows : DEFAULT_CHUNK_SIZE);
}

void
VecHashJoinState::rehash_hash_table(size_t min_bucket_count)
{
	size_t bucket_count = 1024;

	while (bucket_count < std::max<size_t>(min_bucket_count, 1024))
		bucket_count <<= 1;

	bucket_heads_.assign(bucket_count, -1);
	bucket_mask_ = bucket_count - 1;
	for (size_t i = 0; i < entries_.size(); i++)
	{
		size_t bucket = entries_[i].hash & bucket_mask_;

		entries_[i].next = bucket_heads_[bucket];
		bucket_heads_[bucket] = (int32_t) i;
	}
}

void
VecHashJoinState::append_inner_entry(const VecHashJoinKey &key, uint32_t hash,
									 uint32_t chunk_idx, uint16_t row_idx)
{
	size_t next_size = entries_.size() + 1;
	size_t max_load = bucket_heads_.empty() ? 0 : (bucket_heads_.size() * 3) / 4;
	VecHashEntry entry;
	size_t bucket;

	if (bucket_heads_.empty())
		init_hash_table(next_size);
	else if (next_size > max_load)
		rehash_hash_table(bucket_heads_.size() << 1);

	bucket = hash & bucket_mask_;
	entry.hash = hash;
	entry.key = key;
	entry.next = bucket_heads_[bucket];
	entry.chunk_idx = chunk_idx;
	entry.row_idx = row_idx;
	entries_.push_back(entry);
	bucket_heads_[bucket] = (int32_t) (entries_.size() - 1);
}

uint16_t
VecHashJoinState::ensure_inner_payload_col(uint16_t source_col, const VecOutputColMeta &meta)
{
	for (uint16_t i = 0; i < inner_payload_cols_.size(); i++)
	{
		const VecHashPayloadCol &payload_col = inner_payload_cols_[i];

		if (payload_col.source_col == source_col)
			return i;
	}

	inner_payload_cols_.push_back(VecHashPayloadCol{source_col, meta});
	return (uint16_t) (inner_payload_cols_.size() - 1);
}

bool
VecHashJoinState::lookup_output_col_meta(int target_resno, VecOutputColMeta *out) const
{
	for (const auto &output_col : output_cols_)
	{
		if (output_col.output_resno != target_resno)
			continue;
		if (out != nullptr)
			*out = output_col.meta;
		return true;
	}
	return false;
}

DataChunk<DEFAULT_CHUNK_SIZE> *
VecHashJoinState::allocate_inner_chunk()
{
	MemoryContext old_context = MemoryContextSwitchTo(memory_context_);
	DataChunk<DEFAULT_CHUNK_SIZE> *chunk = new DataChunk<DEFAULT_CHUNK_SIZE>();
	MemoryContextSwitchTo(old_context);
	inner_chunks_.push_back(chunk);
	return chunk;
}

void
VecHashJoinState::copy_inner_payload_row(DataChunk<DEFAULT_CHUNK_SIZE> &dst, int dst_row,
										 const DataChunk<DEFAULT_CHUNK_SIZE> &src, int src_row) const
{
	for (uint16_t dst_col = 0; dst_col < inner_payload_cols_.size(); dst_col++)
	{
		const VecHashPayloadCol &payload_col = inner_payload_cols_[dst_col];
		uint16_t src_col = payload_col.source_col;

		dst.nulls[dst_col][dst_row] = src.nulls[src_col][src_row];
		if (dst.nulls[dst_col][dst_row])
			continue;

		switch (payload_col.meta.storage_kind)
		{
			case VecOutputStorageKind::Double:
				dst.double_columns[dst_col][dst_row] = src.double_columns[src_col][src_row];
				break;
			case VecOutputStorageKind::Int64:
			case VecOutputStorageKind::NumericScaledInt64:
			case VecOutputStorageKind::NumericAvgPair:
				dst.int64_columns[dst_col][dst_row] = src.int64_columns[src_col][src_row];
				dst.double_columns[dst_col][dst_row] = src.double_columns[src_col][src_row];
				break;
				case VecOutputStorageKind::StringRef:
					dst.string_columns[dst_col][dst_row] =
						CopyStringRefToChunk(dst, src, src.string_columns[src_col][src_row]);
					break;
			case VecOutputStorageKind::Int32:
				dst.int32_columns[dst_col][dst_row] = src.int32_columns[src_col][src_row];
				break;
		}
	}
}

bool
VecHashJoinState::read_key(const DataChunk<DEFAULT_CHUNK_SIZE> &chunk, bool inner_side, int row,
						   VecHashJoinKey *key) const
{
	if (key == nullptr || key_cols_.empty() || key_cols_.size() > kMaxJoinKeys)
		return false;

	memset(key->values, 0, sizeof(key->values));
	key->num_keys = (uint8_t) key_cols_.size();
	for (size_t i = 0; i < key_cols_.size(); i++)
	{
		const VecHashJoinKeyCol &key_col = key_cols_[i];
		int col = inner_side ? key_col.inner_col : key_col.outer_col;

		if (col < 0 || col >= 16 || chunk.nulls[col][row])
			return false;

		switch (key_col.kind)
		{
			case VecOutputStorageKind::Int32:
				key->values[i] = (uint64_t) (uint32_t) chunk.int32_columns[col][row];
				break;
			case VecOutputStorageKind::Int64:
			case VecOutputStorageKind::NumericScaledInt64:
				key->values[i] = (uint64_t) chunk.int64_columns[col][row];
				break;
			default:
				elog(ERROR, "pg_volvec hash join key kind is not supported");
				return false;
		}
	}

	return true;
}

uint32_t
VecHashJoinState::hash_key(const VecHashJoinKey &key) const
{
	uint64_t hash = UINT64CONST(0x9e3779b97f4a7c15);

	for (int i = 0; i < key.num_keys; i++)
	{
		uint64_t value = key.values[i];

		value ^= value >> 33;
		value *= UINT64CONST(0xff51afd7ed558ccd);
		value ^= value >> 33;
		value *= UINT64CONST(0xc4ceb9fe1a85ec53);
		value ^= value >> 33;
		hash ^= value + UINT64CONST(0x9e3779b97f4a7c15) + (hash << 6) + (hash >> 2);
	}

	return (uint32_t) (hash ^ (hash >> 32));
}

bool
VecHashJoinState::keys_equal(const VecHashJoinKey &left, const VecHashJoinKey &right) const
{
	if (left.num_keys != right.num_keys)
		return false;
	for (int i = 0; i < left.num_keys; i++)
	{
		if (left.values[i] != right.values[i])
			return false;
	}
	return true;
}

void
VecHashJoinState::consume_build_batch(const DataChunk<DEFAULT_CHUNK_SIZE> &input)
{
	bool build_is_inner = !build_outer_side_;
	int active_count = input.has_selection ? input.sel.count : input.count;
	DataChunk<DEFAULT_CHUNK_SIZE> *dst =
		inner_chunks_.empty() ? allocate_inner_chunk() : inner_chunks_.back();
// Auto-split from executor.cpp

	for (int s = 0; s < active_count; s++)
	{
		int src_row = input.has_selection ? input.sel.row_ids[s] : s;
		int dst_row;
		VecHashJoinKey key;

		if (!read_key(input, build_is_inner, src_row, &key))
			continue;
		if (dst->count >= DEFAULT_CHUNK_SIZE)
			dst = allocate_inner_chunk();
		dst_row = dst->count;
		copy_inner_payload_row(*dst, dst_row, input, src_row);
		append_inner_entry(key, hash_key(key),
						  (uint32_t) (inner_chunks_.size() - 1),
						  (uint16_t) dst_row);
		dst->count++;
	}
}

void
VecHashJoinState::consume_build_input()
{
	DataChunk<DEFAULT_CHUNK_SIZE> input;
	VecPlanState *build_state = build_outer_side_ ? outer_.get() : inner_.get();

	if (inner_built_)
		return;
	if (bucket_heads_.empty())
		init_hash_table(DEFAULT_CHUNK_SIZE);
	while (build_state->get_next_batch(input))
	{
		int active_count = input.has_selection ? input.sel.count : input.count;

		build_input_batches_consumed_++;
		build_input_rows_consumed_ += (uint64_t) active_count;
		consume_build_batch(input);
	}
}

void
VecHashJoinState::finish_parallel_hash_build()
{
	if (inner_built_)
		return;
	inner_built_ = true;
	inner_entry_matched_.assign(entries_.size(), 0);
}

void
VecHashJoinState::build_inner_hash()
{
	consume_build_input();
	finish_parallel_hash_build();
}

bool
VecHashJoinState::advance_outer_batch()
{
	VecPlanState *probe_state = build_outer_side_ ? inner_.get() : outer_.get();

	while (probe_state->get_next_batch(outer_chunk_))
	{
		int active_count = outer_chunk_.has_selection ? outer_chunk_.sel.count : outer_chunk_.count;

		if (active_count <= 0)
			continue;
		anti_outer_pos_ = 0;
		probe_batch_ready_ = false;
		probe_input_exhausted_ = false;
		return true;
	}
	probe_input_exhausted_ = true;
	return false;
}

void
VecHashJoinState::prepare_probe_batch()
{
	int active_count = outer_chunk_.has_selection ? outer_chunk_.sel.count : outer_chunk_.count;

	probe_rows_.clear();
	probe_keys_.clear();
	probe_hashes_.clear();
	probe_next_entries_.clear();
	active_probe_sel_.clear();
	next_probe_sel_.clear();
	probe_rows_.reserve(active_count);
	probe_keys_.reserve(active_count);
	probe_hashes_.reserve(active_count);
	probe_next_entries_.reserve(active_count);
	active_probe_sel_.reserve(active_count);
	next_probe_sel_.reserve(active_count);

	for (int s = 0; s < active_count; s++)
	{
		int row = outer_chunk_.has_selection ? outer_chunk_.sel.row_ids[s] : s;
		VecHashJoinKey key;
		uint32_t hash;
		int32_t head;
		uint16_t probe_idx;

		if (!read_key(outer_chunk_, build_outer_side_, row, &key))
			continue;

		hash = hash_key(key);
		head = bucket_heads_.empty() ? -1 : bucket_heads_[hash & bucket_mask_];
		if (head < 0)
			continue;

		probe_idx = (uint16_t) probe_rows_.size();
		probe_rows_.push_back((uint16_t) row);
		probe_keys_.push_back(key);
		probe_hashes_.push_back(hash);
		probe_next_entries_.push_back(head);
		active_probe_sel_.push_back(probe_idx);
	}

	probe_batch_ready_ = true;
}

bool
VecHashJoinState::advance_probe_match(uint16_t probe_idx, int32_t *match_entry_idx)
{
	int32_t entry_idx = probe_next_entries_[probe_idx];
	const VecHashJoinKey &key = probe_keys_[probe_idx];
	uint32_t hash = probe_hashes_[probe_idx];

	while (entry_idx >= 0)
	{
		const VecHashEntry &entry = entries_[entry_idx];

		probe_next_entries_[probe_idx] = entry.next;
		if (entry.hash == hash && keys_equal(entry.key, key))
		{
			if (match_entry_idx != nullptr)
				*match_entry_idx = entry_idx;
			return true;
		}
		entry_idx = entry.next;
	}

	probe_next_entries_[probe_idx] = -1;
	return false;
}

bool
VecHashJoinState::candidate_passes_join_filter(const DataChunk<DEFAULT_CHUNK_SIZE> &outer_src,
											   int outer_row,
											   const DataChunk<DEFAULT_CHUNK_SIZE> &inner_src,
											   int inner_row)
{
	if (!join_filter_program_)
		return true;

	join_filter_chunk_.reset();
	join_filter_chunk_.count = 1;
	for (const auto &output_col : output_cols_)
	{
		int out_col = output_col.output_resno - 1;
		const DataChunk<DEFAULT_CHUNK_SIZE> *src =
			output_col.side == VecJoinSide::Outer ? &outer_src : &inner_src;
		int src_row = output_col.side == VecJoinSide::Outer ? outer_row : inner_row;
		int src_col = output_col.input_col;

		join_filter_chunk_.nulls[out_col][0] = src->nulls[src_col][src_row];
		if (join_filter_chunk_.nulls[out_col][0])
			continue;
		switch (output_col.meta.storage_kind)
		{
			case VecOutputStorageKind::Double:
				join_filter_chunk_.double_columns[out_col][0] = src->double_columns[src_col][src_row];
				break;
			case VecOutputStorageKind::Int64:
			case VecOutputStorageKind::NumericScaledInt64:
			case VecOutputStorageKind::NumericAvgPair:
				join_filter_chunk_.int64_columns[out_col][0] = src->int64_columns[src_col][src_row];
				join_filter_chunk_.double_columns[out_col][0] = src->double_columns[src_col][src_row];
				break;
			case VecOutputStorageKind::StringRef:
				join_filter_chunk_.string_columns[out_col][0] =
					CopyStringRefToChunk(join_filter_chunk_, *src, src->string_columns[src_col][src_row]);
				break;
			case VecOutputStorageKind::Int32:
				join_filter_chunk_.int32_columns[out_col][0] = src->int32_columns[src_col][src_row];
				break;
		}
	}

	join_filter_program_->evaluate(join_filter_chunk_);
	return (join_filter_chunk_.has_selection ? join_filter_chunk_.sel.count : join_filter_chunk_.count) > 0;
}

} /* namespace pg_volvec */

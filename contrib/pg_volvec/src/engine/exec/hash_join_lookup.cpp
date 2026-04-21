#include "exec/internal.hpp"
#include "hash_table.hpp"

#include <unordered_set>

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
	  probe_partition_ids_(PgMemoryContextAllocator<uint16_t>(memory_context_)),
	  active_probe_sel_(PgMemoryContextAllocator<uint16_t>(memory_context_)),
	  next_probe_sel_(PgMemoryContextAllocator<uint16_t>(memory_context_)),
	  probe_candidates_(PgMemoryContextAllocator<ProbeCandidate>(memory_context_)),
	  probe_partition_offsets_(PgMemoryContextAllocator<uint32_t>(memory_context_)),
	  probe_partition_order_(PgMemoryContextAllocator<uint32_t>(memory_context_)),
	  probe_partition_cursor_(PgMemoryContextAllocator<uint32_t>(memory_context_)),
	  join_filter_build_col_mask_(PgMemoryContextAllocator<uint8_t>(memory_context_)),
	  output_build_col_mask_(PgMemoryContextAllocator<uint8_t>(memory_context_)),
	  required_build_col_mask_(PgMemoryContextAllocator<uint8_t>(memory_context_)),
	  ht_match_payloads_(PgMemoryContextAllocator<uint64_t>(memory_context_)),
	  ht_match_starts_(PgMemoryContextAllocator<uint32_t>(memory_context_)),
	  ht_match_counts_(PgMemoryContextAllocator<uint32_t>(memory_context_)),
	  ht_match_pos_(PgMemoryContextAllocator<uint32_t>(memory_context_)),
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
	  bucket_mask_(0),
	  build_input_rows_consumed_(0),
	  build_input_batches_consumed_(0),
	  local_hash_tables_(PgMemoryContextAllocator<VecLinearProbeHT>(memory_context_)),
	  partition_bloom_filters_(PgMemoryContextAllocator<VecBloomFilter>(memory_context_)),
	  shared_hash_bridge_(nullptr),
	  shared_hash_bridge_buffer_(nullptr),
	  shared_hash_bridge_buffer_size_(0),
	  shared_hash_bridge_buffer_owned_(false),
	  shared_hash_partitions_(nullptr),
	  shared_hash_partition_count_(0),
	  shared_payload_cols_view_(nullptr),
	  shared_payload_col_count_(0),
	  shared_bucket_heads_view_(nullptr),
	  shared_bucket_count_(0),
	  shared_entries_view_(nullptr),
	  shared_entry_count_(0),
	  shared_hash_chunks_(nullptr),
	  shared_hash_chunk_count_(0),
	  assigned_partition_start_(0),
	  assigned_partition_end_(VOLVEC_RADIX_FANOUT),
	  total_build_rows_(0),
	  use_parallel_ht_(false)
{
	Assert(!build_outer_side_ ||
		   jointype_ == JOIN_INNER ||
		   jointype_ == JOIN_SEMI ||
		   jointype_ == JOIN_ANTI);
	for (int i = 0; i < VOLVEC_RADIX_FANOUT; i++)
	{
		partition_bucket_heads_[i] = nullptr;
		partition_bucket_masks_[i] = 0;
		partition_bucket_heads_external_[i] = false;
		build_histogram_[i] = 0;
	}
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
	join_filter_build_col_mask_.assign(16, 0);
	output_build_col_mask_.assign(16, 0);
	required_build_col_mask_.assign(16, 0);
	for (const auto &output_col : output_cols_)
	{
		if ((build_outer_side_ && output_col.side == VecJoinSide::Outer) ||
			(!build_outer_side_ && output_col.side == VecJoinSide::Inner))
		{
			if (output_col.input_col < output_build_col_mask_.size())
				output_build_col_mask_[output_col.input_col] = 1;
			if (output_col.input_col < required_build_col_mask_.size())
				required_build_col_mask_[output_col.input_col] = 1;
		}
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
	
	std::unordered_set<DataChunk<DEFAULT_CHUNK_SIZE> *> deleted_chunks;
	for (int i = 0; i < VOLVEC_RADIX_FANOUT; i++)
	{
		for (auto *chunk : partition_chunks_[i])
		{
			if (deleted_chunks.find(chunk) == deleted_chunks.end())
			{
				delete chunk;
				deleted_chunks.insert(chunk);
			}
		}
		
		if (partition_bucket_heads_[i] != nullptr &&
			!partition_bucket_heads_external_[i])
		{
			pfree(partition_bucket_heads_[i]);
		}
	}
}

bool
VecHashJoinState::configure_source_block_range(BlockNumber start_block, uint32_t nblocks)
{
	VecPlanState *probe_state = build_outer_side_ ? inner_.get() : outer_.get();
	bool ok;

	reset_probe_task_state();
	ok = probe_state != nullptr &&
		probe_state->configure_source_block_range(start_block, nblocks);
	if (pg_volvec_trace_hooks && !ok)
		elog(LOG,
			 "pg_volvec: hash join probe block range configure failed plan_node_id=%d build_outer=%s start=%u nblocks=%u probe=%s",
			 plan_node_id_,
			 build_outer_side_ ? "true" : "false",
			 start_block,
			 nblocks,
			 probe_state != nullptr ? "ok" : "null");
	return ok;
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

void
VecHashJoinState::set_assigned_partition_range(uint32_t start_partition,
							   uint32_t end_partition)
{
	assigned_partition_start_ = Min(start_partition, (uint32_t) VOLVEC_RADIX_FANOUT);
	assigned_partition_end_ = Min(Max(end_partition, assigned_partition_start_),
						   (uint32_t) VOLVEC_RADIX_FANOUT);
}

void
VecHashJoinState::reset_assigned_partition_range()
{
	assigned_partition_start_ = 0;
	assigned_partition_end_ = VOLVEC_RADIX_FANOUT;
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
	probe_partition_ids_.clear();
	active_probe_sel_.clear();
	next_probe_sel_.clear();
	probe_candidates_.clear();
	probe_partition_offsets_.clear();
	probe_partition_order_.clear();
	probe_partition_cursor_.clear();
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
	shared_bucket_heads_view_ = nullptr;
	shared_bucket_count_ = 0;
	shared_entries_view_ = nullptr;
	shared_entry_count_ = 0;
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
	shared_bucket_heads_view_ = nullptr;
	shared_bucket_count_ = 0;
	shared_entries_view_ = nullptr;
	shared_entry_count_ = 0;
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

bool
VecHashJoinState::build_key_exists(const VecHashJoinKey &key, uint32_t hash) const
{
	int32_t entry_idx;

	if (bucket_heads_.empty())
		return false;
	entry_idx = bucket_heads_[hash & bucket_mask_];
	while (entry_idx >= 0)
	{
		const VecHashEntry &entry = entries_[entry_idx];

		if (entry.hash == hash && keys_equal(entry.key, key))
			return true;
		entry_idx = entry.next;
	}
	return false;
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
		uint32_t hash;

		if (!read_key(input, build_is_inner, src_row, &key))
			continue;
		hash = hash_key(key);
		if (jointype_ == JOIN_ANTI && !build_outer_side_ &&
			inner_payload_cols_.empty() &&
			build_key_exists(key, hash))
			continue;
		if (dst->count >= DEFAULT_CHUNK_SIZE)
			dst = allocate_inner_chunk();
		dst_row = dst->count;
		copy_inner_payload_row(*dst, dst_row, input, src_row);
		append_inner_entry(key, hash,
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
VecHashJoinState::consume_build_input_radix()
{
	DataChunk<DEFAULT_CHUNK_SIZE> input;
	VecPlanState *build_state = build_outer_side_ ? outer_.get() : inner_.get();
	bool build_is_inner = !build_outer_side_;

	if (inner_built_)
		return;

	struct PartitionRow {
		VecHashJoinKey key;
		uint32_t hash;
		uint32_t partition_id;
		int src_row;
	};

	VolVecVector<PartitionRow> pending_rows;
	pending_rows.reserve(DEFAULT_CHUNK_SIZE);

	while (build_state->get_next_batch(input))
	{
		int active_count = input.has_selection ? input.sel.count : input.count;

		build_input_batches_consumed_++;
		build_input_rows_consumed_ += (uint64_t) active_count;

		pending_rows.clear();

		for (int s = 0; s < active_count; s++)
		{
			int src_row = input.has_selection ? input.sel.row_ids[s] : s;
			VecHashJoinKey key;
			uint32_t hash;

			if (!read_key(input, build_is_inner, src_row, &key))
				continue;

			hash = hash_key(key);
			uint32_t partition_id = volvec_radix_partition_idx(hash);

			if (jointype_ == JOIN_ANTI && !build_outer_side_ &&
				inner_payload_cols_.empty() &&
				build_key_exists(key, hash))
				continue;

			pending_rows.push_back({key, hash, partition_id, src_row});
			build_histogram_[partition_id]++;
		}

		for (const auto &prow : pending_rows)
		{
			uint32_t pid = prow.partition_id;
			DataChunk<DEFAULT_CHUNK_SIZE> *dst;

			if (partition_chunks_[pid].empty() ||
				partition_chunks_[pid].back()->count >= DEFAULT_CHUNK_SIZE)
			{
				MemoryContext old = MemoryContextSwitchTo(memory_context_);
				dst = new DataChunk<DEFAULT_CHUNK_SIZE>();
				MemoryContextSwitchTo(old);
				partition_chunks_[pid].push_back(dst);
			}
			else
			{
				dst = partition_chunks_[pid].back();
			}

			int dst_row = dst->count;
			copy_inner_payload_row(*dst, dst_row, input, prow.src_row);

			VecHashEntry entry;
			entry.hash = prow.hash;
			entry.key = prow.key;
			entry.chunk_idx = partition_row_stores_[pid].row_count;
			entry.row_idx = (uint16_t)dst_row;
			entry.next = -1;

			append_partition_row_from_input(pid, input, prow.src_row, prow.key, prow.hash);
			partition_entries_[pid].push_back(entry);
			dst->count++;
		}
	}

	total_build_rows_ = 0;
	for (int i = 0; i < VOLVEC_RADIX_FANOUT; i++)
		total_build_rows_ += build_histogram_[i];
}

void
VecHashJoinState::finish_parallel_hash_build()
{
	if (inner_built_)
		return;
	inner_built_ = true;
	
	if (use_parallel_ht_)
	{
		for (int part_idx = 0; part_idx < VOLVEC_RADIX_FANOUT; part_idx++)
		{
			if (build_histogram_[part_idx] > 0)
				build_linear_probe_table_for_partition(part_idx);
		}
	}
	else
	{
		inner_entry_matched_.assign(entries_.size(), 0);
	}
}

void
VecHashJoinState::build_inner_hash()
{
	consume_build_input();
	finish_parallel_hash_build();
}

void
VecHashJoinState::build_linear_probe_table_for_partition(int part_idx)
{
	if (part_idx < 0 || part_idx >= VOLVEC_RADIX_FANOUT)
		return;

	const auto &entries = partition_entries_[part_idx];
	if (entries.empty())
		return;

	size_t entry_count = entries.size();
	size_t bucket_count = 1024;

	while (bucket_count < entry_count * 2)
		bucket_count <<= 1;

	MemoryContext old = MemoryContextSwitchTo(memory_context_);
	int32_t *bucket_heads = (int32_t *) palloc(bucket_count * sizeof(int32_t));
	MemoryContextSwitchTo(old);

	for (size_t i = 0; i < bucket_count; i++)
		bucket_heads[i] = -1;

	size_t mask = bucket_count - 1;

	VecHashEntry *entries_mutable = const_cast<VecHashEntry *>(entries.data());

	for (size_t i = 0; i < entry_count; i++)
	{
		size_t bucket = entries[i].hash & mask;
		entries_mutable[i].next = bucket_heads[bucket];
		bucket_heads[bucket] = (int32_t) i;
	}

	partition_bucket_heads_[part_idx] = bucket_heads;
	partition_bucket_masks_[part_idx] = mask;
	partition_bucket_heads_external_[part_idx] = false;
}

void
VecHashJoinState::partition_build_batch(const DataChunk<DEFAULT_CHUNK_SIZE> &input,
										uint32_t *hashes, uint64_t *keys, uint64_t *payloads)
{
	for (int part_idx = 0; part_idx < VOLVEC_RADIX_FANOUT; part_idx++)
	{
		if (build_histogram_[part_idx] == 0)
			continue;
		build_linear_probe_table_for_partition(part_idx);
	}
}

void
VecHashJoinState::build_linear_probe_tables()
{
	for (int part_idx = 0; part_idx < VOLVEC_RADIX_FANOUT; part_idx++)
	{
		build_linear_probe_table_for_partition(part_idx);
	}
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
	probe_partition_ids_.clear();
	active_probe_sel_.clear();
	next_probe_sel_.clear();
	probe_candidates_.clear();
	probe_partition_offsets_.clear();
	probe_partition_order_.clear();
	probe_partition_cursor_.clear();
	probe_rows_.reserve(active_count);
	probe_keys_.reserve(active_count);
	probe_hashes_.reserve(active_count);
	probe_next_entries_.reserve(active_count);
	probe_partition_ids_.reserve(active_count);
	active_probe_sel_.reserve(active_count);
	next_probe_sel_.reserve(active_count);
	probe_candidates_.reserve(active_count);

	probe_partition_offsets_.assign(VOLVEC_RADIX_FANOUT + 1, 0);
	for (int s = 0; s < active_count; s++)
	{
		int row = outer_chunk_.has_selection ? outer_chunk_.sel.row_ids[s] : s;
		VecHashJoinKey key;
		uint32_t hash;
		int32_t head;
		uint16_t partition_id;
		uint16_t probe_idx;

		if (!read_key(outer_chunk_, build_outer_side_, row, &key))
			continue;

		hash = hash_key(key);
		partition_id = (shared_hash_partition_count_ == 1) ? 0 : (uint16_t) volvec_radix_partition_idx(hash);
		const int32_t *bucket_heads = use_parallel_ht_ ? 
			bucket_heads_for_partition(partition_id) : active_bucket_heads();
		size_t mask = use_parallel_ht_ ? 
			active_bucket_mask_for_partition(partition_id) : bucket_mask_;
		head = bucket_heads == nullptr ? -1 : bucket_heads[hash & mask];
		if (head < 0)
			continue;

		probe_idx = (uint16_t) probe_rows_.size();
		probe_rows_.push_back((uint16_t) row);
		probe_keys_.push_back(key);
		probe_hashes_.push_back(hash);
		probe_next_entries_.push_back(head);
		probe_partition_ids_.push_back(partition_id);
		active_probe_sel_.push_back(probe_idx);
		probe_partition_offsets_[partition_id + 1]++;
	}
	for (uint32_t i = 1; i < probe_partition_offsets_.size(); i++)
		probe_partition_offsets_[i] += probe_partition_offsets_[i - 1];
	initialize_probe_partition_order();
	assign_probe_candidates_from_partition(0);

	probe_batch_ready_ = true;
}

const VecHashJoinState::VecHashEntry &
VecHashJoinState::get_entry_at(size_t entry_idx) const
{
	if (shared_entries_view_ != nullptr)
		return shared_entries_view_[entry_idx];
	return entries_[entry_idx];
}

const int32_t *
VecHashJoinState::active_bucket_heads() const
{
	if (shared_bucket_heads_view_ != nullptr)
		return shared_bucket_heads_view_;
	return bucket_heads_.empty() ? nullptr : bucket_heads_.data();
}

size_t
VecHashJoinState::active_entry_count() const
{
	if (shared_entries_view_ != nullptr)
		return shared_entry_count_;
	return entries_.size();
}

void
VecHashJoinState::initialize_probe_partition_order()
{
	probe_partition_order_.clear();
	probe_partition_cursor_.assign(VOLVEC_RADIX_FANOUT, 0);
	for (uint32_t part_idx = assigned_partition_start_;
		 part_idx < assigned_partition_end_ && part_idx < VOLVEC_RADIX_FANOUT;
		 part_idx++)
	{
		if (probe_partition_offsets_[part_idx + 1] > probe_partition_offsets_[part_idx])
			probe_partition_order_.push_back(part_idx);
	}
}

void
VecHashJoinState::assign_probe_candidates_from_partition(uint32_t start_partition_order_idx)
{
	probe_candidates_.clear();
	for (uint32_t order_idx = start_partition_order_idx;
		 order_idx < probe_partition_order_.size();
		 order_idx++)
	{
		uint32_t part_idx = probe_partition_order_[order_idx];
		for (uint16_t probe_idx : active_probe_sel_)
		{
			if (probe_partition_ids_[probe_idx] != part_idx)
				continue;
			probe_candidates_.push_back(ProbeCandidate{probe_idx, probe_next_entries_[probe_idx]});
		}
		if (!probe_candidates_.empty())
		{
			probe_partition_cursor_[part_idx] = order_idx;
			return;
		}
	}
}

bool
VecHashJoinState::advance_probe_match(uint16_t probe_idx, int32_t *match_entry_idx)
{
	int32_t entry_idx = probe_next_entries_[probe_idx];
	const VecHashJoinKey &key = probe_keys_[probe_idx];
	uint32_t hash = probe_hashes_[probe_idx];

	while (entry_idx >= 0)
	{
		const VecHashEntry &entry = get_entry_at(entry_idx);

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
VecHashJoinState::next_probe_candidate(ProbeCandidate *candidate)
{
	if (candidate == nullptr)
		return false;
	while (!probe_candidates_.empty())
	{
		*candidate = probe_candidates_.back();
		probe_candidates_.pop_back();
		if (candidate->entry_idx >= 0)
			return true;
	}
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

const ParallelHashBuildPartition *
VecHashJoinState::active_partition_view() const
{
	if (!use_parallel_ht_ || shared_hash_partition_count_ == 0)
		return nullptr;
	return shared_hash_partitions_;
}

size_t
VecHashJoinState::active_entry_base() const
{
	if (!use_parallel_ht_ || shared_hash_partition_count_ <= 1)
		return 0;
	/* Multi-partition: entries are partitioned. Base = 0 for now. */
	return 0;
}

size_t
VecHashJoinState::active_bucket_mask_for_partition(uint32_t partition_id) const
{
	if (!use_parallel_ht_ || shared_hash_partition_count_ <= 1)
		return bucket_mask_;
	Assert(partition_id < VOLVEC_RADIX_FANOUT);
	return partition_bucket_masks_[partition_id];
}

const int32_t *
VecHashJoinState::bucket_heads_for_partition(uint32_t partition_id) const
{
	if (!use_parallel_ht_ || shared_hash_partition_count_ <= 1)
		return active_bucket_heads();
	Assert(partition_id < VOLVEC_RADIX_FANOUT);
	return partition_bucket_heads_[partition_id];
}

} /* namespace pg_volvec */

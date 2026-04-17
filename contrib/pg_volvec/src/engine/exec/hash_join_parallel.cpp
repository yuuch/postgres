#include "exec/internal.hpp"
#include "hash_table.hpp"

namespace pg_volvec {

constexpr uint32 VOLVEC_HASH_BUILD_FILE_MAGIC = 0x56564846;
constexpr uint32 VOLVEC_HASH_BUILD_FILE_VERSION = 1;

struct SerializedHashBuildFileHeader
{
	uint32 magic = VOLVEC_HASH_BUILD_FILE_MAGIC;
	uint32 version = VOLVEC_HASH_BUILD_FILE_VERSION;
	uint32 num_payload_cols = 0;
	uint32 entry_count = 0;
	uint32 chunk_count = 0;
	uint32 reserved = 0;
};

struct SerializedHashBuildChunkHeader
{
	uint32 row_count = 0;
	uint32 string_arena_size = 0;
};

struct SerializedHashBuildEntry
{
	uint32 hash = 0;
	VecHashJoinKey key{};
	uint32 chunk_idx = 0;
	uint16 row_idx = 0;
	uint16 reserved = 0;
};

static void
AppendBytes(VolVecVector<uint8_t> *buffer, const void *ptr, size_t size)
{
	const uint8_t *src = (const uint8_t *) ptr;

	if (buffer == nullptr || ptr == nullptr || size == 0)
		return;
	buffer->insert(buffer->end(), src, src + size);
}

static bool
ReadBytes(const uint8_t *buffer,
		  size_t buffer_size,
		  size_t *offset,
		  void *dst,
		  size_t size)
{
	if (buffer == nullptr || offset == nullptr || dst == nullptr)
		return false;
	if (*offset > buffer_size || size > (buffer_size - *offset))
		return false;
	memcpy(dst, buffer + *offset, size);
	*offset += size;
	return true;
}

struct HashJoinParallelAccess
{
	static size_t ComputeChunkPayloadSize(const VecHashJoinState &state,
										 const DataChunk<DEFAULT_CHUNK_SIZE> &chunk)
	{
		size_t size = sizeof(SerializedHashBuildChunkHeader);

		for (const auto &payload_col : state.inner_payload_cols_)
		{
			size += (size_t) chunk.count * sizeof(uint8_t);
			switch (payload_col.meta.storage_kind)
			{
				case VecOutputStorageKind::Int32:
					size += (size_t) chunk.count * sizeof(int32_t);
					break;
				case VecOutputStorageKind::Int64:
				case VecOutputStorageKind::NumericScaledInt64:
				case VecOutputStorageKind::NumericAvgPair:
					size += (size_t) chunk.count * (sizeof(int64_t) + sizeof(double));
					break;
				case VecOutputStorageKind::Double:
					size += (size_t) chunk.count * sizeof(double);
					break;
				case VecOutputStorageKind::StringRef:
					size += (size_t) chunk.count * sizeof(VecStringRef);
					break;
			}
		}
		size += chunk.string_arena.size();
		return size;
	}

	static size_t ComputeSerializedStateSize(const VecHashJoinState &state)
	{
		size_t size = sizeof(SerializedHashBuildFileHeader);

		size += state.inner_payload_cols_.size() * sizeof(VecHashPayloadCol);
		size += state.entries_.size() * sizeof(SerializedHashBuildEntry);
		for (const auto *chunk : state.inner_chunks_)
			size += ComputeChunkPayloadSize(state, *chunk);
		return size;
	}

	static void SerializeChunk(const VecHashJoinState &state,
							   const DataChunk<DEFAULT_CHUNK_SIZE> &chunk,
							   VolVecVector<uint8_t> *buffer)
	{
		SerializedHashBuildChunkHeader header;

		header.row_count = chunk.count;
		header.string_arena_size = (uint32) chunk.string_arena.size();
		AppendBytes(buffer, &header, sizeof(header));

		for (uint16_t payload_idx = 0; payload_idx < state.inner_payload_cols_.size(); payload_idx++)
		{
			const VecHashPayloadCol &payload_col = state.inner_payload_cols_[payload_idx];

			AppendBytes(buffer, chunk.nulls[payload_idx], (size_t) chunk.count * sizeof(uint8_t));
			switch (payload_col.meta.storage_kind)
			{
				case VecOutputStorageKind::Int32:
					AppendBytes(buffer,
								chunk.int32_columns[payload_idx],
								(size_t) chunk.count * sizeof(int32_t));
					break;
				case VecOutputStorageKind::Int64:
				case VecOutputStorageKind::NumericScaledInt64:
				case VecOutputStorageKind::NumericAvgPair:
					AppendBytes(buffer,
								chunk.int64_columns[payload_idx],
								(size_t) chunk.count * sizeof(int64_t));
					AppendBytes(buffer,
								chunk.double_columns[payload_idx],
								(size_t) chunk.count * sizeof(double));
					break;
				case VecOutputStorageKind::Double:
					AppendBytes(buffer,
								chunk.double_columns[payload_idx],
								(size_t) chunk.count * sizeof(double));
					break;
				case VecOutputStorageKind::StringRef:
					AppendBytes(buffer,
								chunk.string_columns[payload_idx],
								(size_t) chunk.count * sizeof(VecStringRef));
					break;
			}
		}

		if (!chunk.string_arena.empty())
			AppendBytes(buffer, chunk.string_arena.data(), chunk.string_arena.size());
	}

	static bool DeserializeChunk(const VecHashJoinState &state,
								 const uint8_t *buffer,
								 size_t buffer_size,
								 size_t *offset,
								 DataChunk<DEFAULT_CHUNK_SIZE> *chunk)
	{
		SerializedHashBuildChunkHeader header;

		if (!ReadBytes(buffer, buffer_size, offset, &header, sizeof(header)))
			return false;
		if (header.row_count > DEFAULT_CHUNK_SIZE)
			return false;

		chunk->reset();
		chunk->count = (uint16) header.row_count;
		for (uint16_t payload_idx = 0; payload_idx < state.inner_payload_cols_.size(); payload_idx++)
		{
			const VecHashPayloadCol &payload_col = state.inner_payload_cols_[payload_idx];

			if (!ReadBytes(buffer,
						   buffer_size,
						   offset,
						   chunk->nulls[payload_idx],
						   (size_t) header.row_count * sizeof(uint8_t)))
				return false;
			switch (payload_col.meta.storage_kind)
			{
				case VecOutputStorageKind::Int32:
					if (!ReadBytes(buffer,
								   buffer_size,
								   offset,
								   chunk->int32_columns[payload_idx],
								   (size_t) header.row_count * sizeof(int32_t)))
						return false;
					break;
				case VecOutputStorageKind::Int64:
				case VecOutputStorageKind::NumericScaledInt64:
				case VecOutputStorageKind::NumericAvgPair:
					if (!ReadBytes(buffer,
								   buffer_size,
								   offset,
								   chunk->int64_columns[payload_idx],
								   (size_t) header.row_count * sizeof(int64_t)) ||
						!ReadBytes(buffer,
								   buffer_size,
								   offset,
								   chunk->double_columns[payload_idx],
								   (size_t) header.row_count * sizeof(double)))
						return false;
					break;
				case VecOutputStorageKind::Double:
					if (!ReadBytes(buffer,
								   buffer_size,
								   offset,
								   chunk->double_columns[payload_idx],
								   (size_t) header.row_count * sizeof(double)))
						return false;
					break;
				case VecOutputStorageKind::StringRef:
					if (!ReadBytes(buffer,
								   buffer_size,
								   offset,
								   chunk->string_columns[payload_idx],
								   (size_t) header.row_count * sizeof(VecStringRef)))
						return false;
					break;
			}
		}

		chunk->string_arena.clear();
		chunk->string_arena.resize(header.string_arena_size);
		if (header.string_arena_size > 0 &&
			!ReadBytes(buffer,
					   buffer_size,
					   offset,
					   chunk->string_arena.data(),
					   header.string_arena_size))
			return false;
		return true;
	}

	static void SerializeState(const VecHashJoinState &state,
							   uint8_t *buffer,
							   size_t buffer_size)
	{
		VolVecVector<uint8_t> tmp{PgMemoryContextAllocator<uint8_t>(CurrentMemoryContext)};
		SerializedHashBuildFileHeader header;

		header.num_payload_cols = (uint32) state.inner_payload_cols_.size();
		header.entry_count = (uint32) state.entries_.size();
		header.chunk_count = (uint32) state.inner_chunks_.size();
		AppendBytes(&tmp, &header, sizeof(header));
		if (!state.inner_payload_cols_.empty())
			AppendBytes(&tmp,
						state.inner_payload_cols_.data(),
						state.inner_payload_cols_.size() * sizeof(VecHashPayloadCol));
		for (const auto &entry : state.entries_)
		{
			SerializedHashBuildEntry serialized{};

			serialized.hash = entry.hash;
			serialized.key = entry.key;
			serialized.chunk_idx = entry.chunk_idx;
			serialized.row_idx = entry.row_idx;
			AppendBytes(&tmp, &serialized, sizeof(serialized));
		}
		for (const auto *chunk : state.inner_chunks_)
			SerializeChunk(state, *chunk, &tmp);

		if (tmp.size() != buffer_size)
			elog(ERROR, "pg_volvec hash build serialized size mismatch: expected %zu got %zu",
				 buffer_size, tmp.size());
		memcpy(buffer, tmp.data(), buffer_size);
	}

	static void AppendState(VecHashJoinState &state,
							const uint8_t *buffer,
							size_t buffer_size)
	{
		SerializedHashBuildFileHeader header;
		size_t offset = 0;
		size_t base_chunk_idx;

		if (!ReadBytes(buffer, buffer_size, &offset, &header, sizeof(header)))
			elog(ERROR, "pg_volvec could not read hash build state header");
		if (header.magic != VOLVEC_HASH_BUILD_FILE_MAGIC ||
			header.version != VOLVEC_HASH_BUILD_FILE_VERSION)
			elog(ERROR, "pg_volvec hash build state header mismatch");

		base_chunk_idx = state.inner_chunks_.size();

		if (state.inner_payload_cols_.empty())
		{
			state.inner_payload_cols_.resize(header.num_payload_cols);
			if (header.num_payload_cols > 0 &&
				!ReadBytes(buffer,
						   buffer_size,
						   &offset,
						   state.inner_payload_cols_.data(),
						   header.num_payload_cols * sizeof(VecHashPayloadCol)))
				elog(ERROR, "pg_volvec could not read hash build payload metadata");
		}
		else
		{
			VolVecVector<VecHashPayloadCol> payload_cols{PgMemoryContextAllocator<VecHashPayloadCol>(CurrentMemoryContext)};

			payload_cols.resize(header.num_payload_cols);
			if (header.num_payload_cols > 0 &&
				!ReadBytes(buffer,
						   buffer_size,
						   &offset,
						   payload_cols.data(),
						   header.num_payload_cols * sizeof(VecHashPayloadCol)))
				elog(ERROR, "pg_volvec could not read hash build payload metadata");
			if (payload_cols.size() != state.inner_payload_cols_.size() ||
				memcmp(payload_cols.data(),
					   state.inner_payload_cols_.data(),
					   payload_cols.size() * sizeof(VecHashPayloadCol)) != 0)
				elog(ERROR, "pg_volvec hash build payload metadata mismatch across fragments");
		}

		for (uint32 i = 0; i < header.entry_count; i++)
		{
			SerializedHashBuildEntry serialized{};
			VecHashJoinState::VecHashEntry entry{};

			if (!ReadBytes(buffer, buffer_size, &offset, &serialized, sizeof(serialized)))
				elog(ERROR, "pg_volvec could not read serialized hash build entry");
			entry.hash = serialized.hash;
			entry.key = serialized.key;
			entry.next = -1;
			entry.chunk_idx = (uint32) base_chunk_idx + serialized.chunk_idx;
			entry.row_idx = serialized.row_idx;
			state.entries_.push_back(entry);
		}

		for (uint32 i = 0; i < header.chunk_count; i++)
		{
			MemoryContext old_context = MemoryContextSwitchTo(state.memory_context_);
			DataChunk<DEFAULT_CHUNK_SIZE> *chunk = new DataChunk<DEFAULT_CHUNK_SIZE>();
			MemoryContextSwitchTo(old_context);

			if (!DeserializeChunk(state, buffer, buffer_size, &offset, chunk))
				elog(ERROR, "pg_volvec could not deserialize hash build chunk");
			state.inner_chunks_.push_back(chunk);
		}

		if (offset != buffer_size)
			elog(ERROR, "pg_volvec serialized hash build buffer has trailing bytes");

		if (!state.entries_.empty())
			state.rehash_hash_table(state.entries_.size() * 2);
	}
};

void
VecHashJoinState::clear_partition_hash_tables()
{
	for (auto &ht : local_hash_tables_)
		volvec_ht_destroy(&ht);
	for (auto &bloom : partition_bloom_filters_)
		volvec_bloom_destroy(&bloom);
	local_hash_tables_.clear();
	partition_bloom_filters_.clear();
	for (int i = 0; i < VOLVEC_RADIX_FANOUT; i++)
	{
		partition_bucket_heads_[i] = nullptr;
		partition_bucket_masks_[i] = 0;
		partition_bucket_heads_external_[i] = false;
		build_histogram_[i] = 0;
	}
	memset(&build_partition_table_, 0, sizeof(build_partition_table_));
	memset(&probe_partition_table_, 0, sizeof(probe_partition_table_));
}

void
VecHashJoinState::clear_inner_build_chunks()
{
	for (auto *chunk : inner_chunks_)
		delete chunk;
	inner_chunks_.clear();
}

void
VecHashJoinState::clear_shared_hash_payload_view()
{
	for (int i = 0; i < VOLVEC_RADIX_FANOUT; i++)
	{
		partition_bucket_heads_[i] = nullptr;
		partition_bucket_masks_[i] = 0;
		partition_bucket_heads_external_[i] = false;
	}
	shared_hash_bridge_ = nullptr;
	shared_hash_chunks_ = nullptr;
	shared_hash_chunk_count_ = 0;
}

void
VecHashJoinState::copy_inner_payload_value_to_chunk(DataChunk<DEFAULT_CHUNK_SIZE> &dst,
													int dst_row,
													int dst_col,
													const VecOutputColMeta &meta,
													int src_col,
													uint32_t chunk_idx,
													uint16_t row_idx) const
{
	const DataChunk<DEFAULT_CHUNK_SIZE> *src;

	if (chunk_idx >= inner_chunks_.size() || src_col < 0 || src_col >= 16 ||
		dst_col < 0 || dst_col >= 16)
		return;
	src = inner_chunks_[chunk_idx];
	dst.nulls[dst_col][dst_row] = src->nulls[src_col][row_idx];
	if (dst.nulls[dst_col][dst_row])
		return;

	switch (meta.storage_kind)
	{
		case VecOutputStorageKind::Double:
			dst.double_columns[dst_col][dst_row] = src->double_columns[src_col][row_idx];
			break;
		case VecOutputStorageKind::Int64:
		case VecOutputStorageKind::NumericScaledInt64:
		case VecOutputStorageKind::NumericAvgPair:
			dst.int64_columns[dst_col][dst_row] = src->int64_columns[src_col][row_idx];
			dst.double_columns[dst_col][dst_row] = src->double_columns[src_col][row_idx];
			break;
		case VecOutputStorageKind::StringRef:
			dst.string_columns[dst_col][dst_row] =
				CopyStringRefToChunk(dst, *src, src->string_columns[src_col][row_idx]);
			break;
		case VecOutputStorageKind::Int32:
			dst.int32_columns[dst_col][dst_row] = src->int32_columns[src_col][row_idx];
			break;
	}
}

size_t
VecHashJoinState::compute_hash_build_fragment_size() const
{
	return HashJoinParallelAccess::ComputeSerializedStateSize(*this);
}

void
VecHashJoinState::serialize_hash_build_fragment(uint8_t *buffer, size_t buffer_size) const
{
	HashJoinParallelAccess::SerializeState(*this, buffer, buffer_size);
}

void
VecHashJoinState::append_hash_build_fragment(const uint8_t *buffer, size_t buffer_size)
{
	HashJoinParallelAccess::AppendState(*this, buffer, buffer_size);
}

size_t
VecHashJoinState::compute_hash_bridge_size() const
{
	return compute_hash_build_fragment_size();
}

void
VecHashJoinState::serialize_hash_bridge(uint8_t *buffer, size_t buffer_size) const
{
	serialize_hash_build_fragment(buffer, buffer_size);
}

void
VecHashJoinState::deserialize_hash_bridge(const uint8_t *buffer, size_t buffer_size)
{
	clear_partition_hash_tables();
	clear_inner_build_chunks();
	bucket_heads_.clear();
	entries_.clear();
	inner_entry_matched_.clear();
	append_hash_build_fragment(buffer, buffer_size);
	inner_entry_matched_.assign(entries_.size(), 0);
	inner_built_ = true;
}

bool
VecHashJoinState::export_parallel_build_partial_file(BufFile *file,
													 ParallelHashBuildPartialState *out) const
{
	size_t size;
	VolVecVector<uint8_t> buffer{PgMemoryContextAllocator<uint8_t>(CurrentMemoryContext)};

	if (file == nullptr || out == nullptr)
		return false;
	size = compute_hash_build_fragment_size();
	buffer.resize(size);
	serialize_hash_build_fragment(buffer.data(), size);
	if (!BufFileWriteAllLocal(file, buffer.data(), size))
		return false;
	memset(out, 0, sizeof(*out));
	out->entry_count = entries_.size();
	out->chunk_count = inner_chunks_.size();
	out->file_bytes = size;
	return true;
}

bool
VecHashJoinState::merge_parallel_build_partial_file(BufFile *file,
													const ParallelHashBuildPartialState &partial)
{
	VolVecVector<uint8_t> buffer{PgMemoryContextAllocator<uint8_t>(CurrentMemoryContext)};

	if (file == nullptr || partial.file_bytes == 0)
		return false;
	buffer.resize((size_t) partial.file_bytes);
	if (!BufFileReadAllLocal(file, buffer.data(), (size_t) partial.file_bytes, false))
		return false;
	append_hash_build_fragment(buffer.data(), buffer.size());
	return true;
}

void
VecHashJoinState::reserve_parallel_hash_build_capacity(size_t total_entries,
													   size_t total_chunks)
{
	entries_.reserve(total_entries);
	inner_entry_matched_.reserve(total_entries);
	inner_chunks_.reserve(total_chunks);
}

size_t
VecHashJoinState::estimate_parallel_hash_bridge_size() const
{
	return compute_hash_bridge_size();
}

void
VecHashJoinState::reset_parallel_hash_build_state()
{
	clear_partition_hash_tables();
	clear_inner_build_chunks();
	clear_shared_hash_payload_view();
	bucket_heads_.clear();
	entries_.clear();
	inner_entry_matched_.clear();
	inner_built_ = false;
	bucket_mask_ = 0;
	build_input_rows_consumed_ = 0;
	build_input_batches_consumed_ = 0;
	total_build_rows_ = 0;
	use_parallel_ht_ = false;
	if (shared_hash_bridge_buffer_owned_ && shared_hash_bridge_buffer_ != nullptr)
		pfree(shared_hash_bridge_buffer_);
	shared_hash_bridge_ = nullptr;
	shared_hash_bridge_buffer_ = nullptr;
	shared_hash_bridge_buffer_size_ = 0;
	shared_hash_bridge_buffer_owned_ = false;
	shared_hash_chunks_ = nullptr;
	shared_hash_chunk_count_ = 0;
}

void
VecHashJoinState::publish_hash_bridge()
{
	size_t size;

	if (shared_hash_bridge_buffer_owned_ && shared_hash_bridge_buffer_ != nullptr)
		pfree(shared_hash_bridge_buffer_);
	size = compute_hash_bridge_size();
	shared_hash_bridge_buffer_ = (uint8_t *) MemoryContextAlloc(memory_context_, size);
	serialize_hash_bridge(shared_hash_bridge_buffer_, size);
	shared_hash_bridge_buffer_size_ = size;
	shared_hash_bridge_buffer_owned_ = true;
	shared_hash_chunk_count_ = (uint32) inner_chunks_.size();
}

void
VecHashJoinState::load_hash_bridge()
{
	if (shared_hash_bridge_buffer_ == nullptr || shared_hash_bridge_buffer_size_ == 0)
		return;
	deserialize_hash_bridge(shared_hash_bridge_buffer_, shared_hash_bridge_buffer_size_);
	shared_hash_chunk_count_ = (uint32) inner_chunks_.size();
}

void
VecHashJoinState::attach_shared_hash_bridge(const uint8_t *buffer, size_t buffer_size)
{
	shared_hash_bridge_buffer_ = const_cast<uint8_t *>(buffer);
	shared_hash_bridge_buffer_size_ = buffer_size;
	shared_hash_bridge_buffer_owned_ = false;
	shared_hash_bridge_ = nullptr;
	shared_hash_chunks_ = nullptr;
	shared_hash_chunk_count_ = 0;
}

} /* namespace pg_volvec */

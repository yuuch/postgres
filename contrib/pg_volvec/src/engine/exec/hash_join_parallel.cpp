#include "exec/internal.hpp"
#include "hash_table.hpp"
#include <unordered_map>
#include <unordered_set>
#include <array>

namespace pg_volvec {

static inline uint32_t
VolvecAlignU32(uint32_t value, uint32_t alignment)
{
	if (alignment == 0)
		return value;
	return (value + alignment - 1) & ~(alignment - 1);
}

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

struct SerializedRowLayoutFragmentHeader
{
	uint32 magic = VOLVEC_HASH_BUILD_FILE_MAGIC;
	uint32 version = 2;
	uint32 num_partitions = VOLVEC_RADIX_FANOUT;
	uint32 row_width = 0;
	uint32 total_rows = 0;
	uint32 reserved = 0;
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

size_t
VecHashJoinState::compute_row_layout_fragment_size() const
{
	size_t size = sizeof(SerializedRowLayoutFragmentHeader);
	size += VOLVEC_RADIX_FANOUT * sizeof(ParallelHashBuildTaskPartitionRows);
	for (int part_idx = 0; part_idx < VOLVEC_RADIX_FANOUT; part_idx++)
	{
		const VecHashPartitionRowStore &store = partition_row_stores_[part_idx];
		size += store.blocks.size() * sizeof(ParallelHashRowBlockDesc);
		for (const auto *block : store.blocks)
		{
			if (block != nullptr)
				size += block->bytes.size();
		}
		size += store.varlen_arena.size();
	}
	return size;
}

void
VecHashJoinState::serialize_row_layout_fragment(uint8_t *buffer, size_t buffer_size) const
{
	SerializedRowLayoutFragmentHeader header{};
	header.row_width = row_layout_.row_width;
	header.total_rows = (uint32) total_build_rows_;
	size_t offset = sizeof(header);
	memcpy(buffer, &header, sizeof(header));

	ParallelHashBuildTaskPartitionRows *partitions =
		reinterpret_cast<ParallelHashBuildTaskPartitionRows *>(buffer + offset);
	memset(partitions, 0, VOLVEC_RADIX_FANOUT * sizeof(ParallelHashBuildTaskPartitionRows));
	offset += VOLVEC_RADIX_FANOUT * sizeof(ParallelHashBuildTaskPartitionRows);

	for (int part_idx = 0; part_idx < VOLVEC_RADIX_FANOUT; part_idx++)
	{
		const VecHashPartitionRowStore &store = partition_row_stores_[part_idx];
		ParallelHashBuildTaskPartitionRows &partition = partitions[part_idx];
		partition.row_count = store.row_count;
		partition.row_width = store.row_width;
		if (store.blocks.empty() && store.varlen_arena.empty())
			continue;

		partition.row_blocks_offset = offset;
		partition.row_blocks_size = store.blocks.size() * sizeof(ParallelHashRowBlockDesc);
		ParallelHashRowBlockDesc *block_descs =
			reinterpret_cast<ParallelHashRowBlockDesc *>(buffer + offset);
		memset(block_descs, 0, partition.row_blocks_size);
		offset += partition.row_blocks_size;
		for (size_t block_idx = 0; block_idx < store.blocks.size(); block_idx++)
		{
			const VecHashRowBlock *block = store.blocks[block_idx];
			if (block == nullptr)
				continue;
			block_descs[block_idx].block_offset = offset;
			block_descs[block_idx].start_row_ordinal = block->start_row_ordinal;
			block_descs[block_idx].row_count = block->row_count;
			block_descs[block_idx].row_width = block->row_width;
			if (!block->bytes.empty())
			{
				memcpy(buffer + offset, block->bytes.data(), block->bytes.size());
				offset += block->bytes.size();
			}
		}
		partition.varlen_offset = offset;
		partition.varlen_size = store.varlen_arena.size();
		if (!store.varlen_arena.empty())
		{
			memcpy(buffer + offset, store.varlen_arena.data(), store.varlen_arena.size());
			offset += store.varlen_arena.size();
		}
	}

	if (offset != buffer_size)
		elog(ERROR, "pg_volvec row-layout fragment size mismatch: expected %zu got %zu",
			 buffer_size, offset);
}

void
VecHashJoinState::append_row_layout_fragment(const uint8_t *buffer, size_t buffer_size)
{
	SerializedRowLayoutFragmentHeader header{};
	size_t offset = 0;
	if (!ReadBytes(buffer, buffer_size, &offset, &header, sizeof(header)))
		elog(ERROR, "pg_volvec could not read row-layout fragment header");
	if (header.version != 2 || header.num_partitions != VOLVEC_RADIX_FANOUT)
		elog(ERROR, "pg_volvec row-layout fragment header mismatch");
	init_row_layout_if_needed();
	if (header.row_width != 0 && row_layout_.row_width != 0 &&
		header.row_width != row_layout_.row_width)
		elog(ERROR, "pg_volvec row-layout fragment row-width mismatch");

	VolVecVector<ParallelHashBuildTaskPartitionRows> partitions;
	partitions.resize(VOLVEC_RADIX_FANOUT);
	if (!ReadBytes(buffer, buffer_size, &offset, partitions.data(),
				  VOLVEC_RADIX_FANOUT * sizeof(ParallelHashBuildTaskPartitionRows)))
		elog(ERROR, "pg_volvec could not read row-layout partition descriptors");

	for (int part_idx = 0; part_idx < VOLVEC_RADIX_FANOUT; part_idx++)
	{
		const ParallelHashBuildTaskPartitionRows &partition = partitions[part_idx];
		if (partition.row_count == 0)
			continue;
		if (partition.row_blocks_offset + partition.row_blocks_size > buffer_size ||
			partition.varlen_offset + partition.varlen_size > buffer_size)
			elog(ERROR, "pg_volvec row-layout partition offsets invalid");

		size_t block_count = partition.row_blocks_size / sizeof(ParallelHashRowBlockDesc);
		const ParallelHashRowBlockDesc *block_descs =
			reinterpret_cast<const ParallelHashRowBlockDesc *>(buffer + partition.row_blocks_offset);
		VecHashPartitionRowStore &store = partition_row_stores_[part_idx];
		for (size_t block_idx = 0; block_idx < block_count; block_idx++)
		{
			const ParallelHashRowBlockDesc &desc = block_descs[block_idx];
			if (desc.block_offset + ((size_t) desc.row_count * desc.row_width) > buffer_size)
				elog(ERROR, "pg_volvec row block descriptor invalid");
			MemoryContext old_context = MemoryContextSwitchTo(memory_context_);
			VecHashRowBlock *block = new VecHashRowBlock();
			MemoryContextSwitchTo(old_context);
			block->row_width = desc.row_width;
			block->start_row_ordinal = store.row_count;
			block->row_count = desc.row_count;
			size_t bytes = (size_t) desc.row_count * desc.row_width;
			block->bytes.resize(bytes);
			memcpy(block->bytes.data(), buffer + desc.block_offset, bytes);
			store.blocks.push_back(block);
			store.row_count += desc.row_count;
		}
		if (partition.varlen_size > 0)
		{
			store.varlen_arena.insert(store.varlen_arena.end(),
				(const char *) (buffer + partition.varlen_offset),
				(const char *) (buffer + partition.varlen_offset + partition.varlen_size));
		}
		store.row_width = partition.row_width;
		if (store.chain_next.size() < store.row_count)
			store.chain_next.resize(store.row_count, -1);
	}
}

static const ParallelHashRowVarlenRef *
GetRowVarlenRefAt(const uint8_t *row_ptr, uint32_t *payload_offset)
{
	*payload_offset = VolvecAlignU32(*payload_offset, 8);
	const ParallelHashRowVarlenRef *ref =
		reinterpret_cast<const ParallelHashRowVarlenRef *>(row_ptr + *payload_offset);
	*payload_offset += sizeof(ParallelHashRowVarlenRef);
	return ref;
}

struct HashJoinParallelAccess
{
	static bool PayloadColEquals(const VecHashPayloadCol &left,
								 const VecHashPayloadCol &right)
	{
		return left.meta.sql_type == right.meta.sql_type &&
			left.meta.storage_kind == right.meta.storage_kind &&
			left.meta.scale == right.meta.scale;
	}

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
		
		size_t total_entries = 0;
		size_t total_chunks = 0;
		for (int i = 0; i < VOLVEC_RADIX_FANOUT; i++)
		{
			total_entries += state.partition_entries_[i].size();
			total_chunks += state.partition_chunks_[i].size();
		}
		
		size += total_entries * sizeof(SerializedHashBuildEntry);
		for (int i = 0; i < VOLVEC_RADIX_FANOUT; i++)
		{
			for (const auto *chunk : state.partition_chunks_[i])
				size += ComputeChunkPayloadSize(state, *chunk);
		}
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
		SerializedHashBuildChunkHeader header{};

		if (!ReadBytes(buffer, buffer_size, offset, &header, sizeof(header)))
			return false;
		if (header.row_count > DEFAULT_CHUNK_SIZE ||
			header.string_arena_size > buffer_size)
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
		if (header.string_arena_size > buffer_size - *offset)
			return false;
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
		SerializedHashBuildFileHeader header{};

		size_t total_entries = 0;
		size_t total_chunks = 0;
		for (int i = 0; i < VOLVEC_RADIX_FANOUT; i++)
		{
			total_entries += state.partition_entries_[i].size();
			total_chunks += state.partition_chunks_[i].size();
		}

		header.num_payload_cols = (uint32) state.inner_payload_cols_.size();
		header.entry_count = (uint32) total_entries;
		header.chunk_count = (uint32) total_chunks;
		AppendBytes(&tmp, &header, sizeof(header));
		if (!state.inner_payload_cols_.empty())
			AppendBytes(&tmp,
						state.inner_payload_cols_.data(),
						state.inner_payload_cols_.size() * sizeof(VecHashPayloadCol));
		
		for (int part_idx = 0; part_idx < VOLVEC_RADIX_FANOUT; part_idx++)
		{
			for (const auto &entry : state.partition_entries_[part_idx])
			{
				SerializedHashBuildEntry serialized{};
				uint32_t chunk_idx = entry.row_idx >= DEFAULT_CHUNK_SIZE ? UINT32_MAX :
					(entry.chunk_idx / DEFAULT_CHUNK_SIZE);

				serialized.hash = entry.hash;
				serialized.key = entry.key;
				if (chunk_idx >= state.partition_chunks_[part_idx].size())
					elog(ERROR,
						 "pg_volvec could not map row ordinal %u to partition chunk index for part %d",
						 entry.chunk_idx,
						 part_idx);
				serialized.chunk_idx = chunk_idx;
				serialized.row_idx = entry.row_idx;
				AppendBytes(&tmp, &serialized, sizeof(serialized));
			}
		}
		
		for (int part_idx = 0; part_idx < VOLVEC_RADIX_FANOUT; part_idx++)
		{
			for (const auto *chunk : state.partition_chunks_[part_idx])
				SerializeChunk(state, *chunk, &tmp);
		}

		if (tmp.size() != buffer_size)
			elog(ERROR, "pg_volvec hash build serialized size mismatch: expected %zu got %zu",
				 buffer_size, tmp.size());
		memcpy(buffer, tmp.data(), buffer_size);
	}

	static void AppendState(VecHashJoinState &state,
							const uint8_t *buffer,
							size_t buffer_size)
	{
		SerializedHashBuildFileHeader header{};
		size_t offset = 0;
		uint32_t base_chunk_indices[VOLVEC_RADIX_FANOUT];

		if (!ReadBytes(buffer, buffer_size, &offset, &header, sizeof(header)))
			elog(ERROR, "pg_volvec could not read hash build state header");
		if (header.num_payload_cols > 16 ||
			header.entry_count > buffer_size / sizeof(SerializedHashBuildEntry) ||
			header.chunk_count > buffer_size / sizeof(SerializedHashBuildChunkHeader))
			elog(ERROR,
				 "pg_volvec hash build state header has invalid counts (payload_cols=%u entries=%u chunks=%u bytes=%zu)",
				 header.num_payload_cols,
				 header.entry_count,
				 header.chunk_count,
				 buffer_size);

		for (int i = 0; i < VOLVEC_RADIX_FANOUT; i++)
			base_chunk_indices[i] = (uint32) state.partition_chunks_[i].size();

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
			if (payload_cols.size() != state.inner_payload_cols_.size())
				elog(ERROR,
					 "pg_volvec hash build payload metadata mismatch across fragments (incoming_cols=%zu existing_cols=%zu)",
					 payload_cols.size(),
					 state.inner_payload_cols_.size());
			for (size_t i = 0; i < payload_cols.size(); i++)
			{
				if (!PayloadColEquals(payload_cols[i], state.inner_payload_cols_[i]))
					elog(ERROR,
						 "pg_volvec hash build payload metadata mismatch across fragments (idx=%zu incoming_source=%u existing_source=%u incoming_type=%u existing_type=%u incoming_kind=%u existing_kind=%u incoming_scale=%d existing_scale=%d)",
						 i,
						 payload_cols[i].source_col,
						 state.inner_payload_cols_[i].source_col,
						 payload_cols[i].meta.sql_type,
						 state.inner_payload_cols_[i].meta.sql_type,
						 (unsigned) payload_cols[i].meta.storage_kind,
						 (unsigned) state.inner_payload_cols_[i].meta.storage_kind,
						 payload_cols[i].meta.scale,
						 state.inner_payload_cols_[i].meta.scale);
			}
		}

		VolVecVector<SerializedHashBuildEntry> entries_buffer;
		entries_buffer.resize(header.entry_count);
		for (uint32 i = 0; i < header.entry_count; i++)
		{
			if (!ReadBytes(buffer, buffer_size, &offset, &entries_buffer[i], sizeof(SerializedHashBuildEntry)))
				elog(ERROR, "pg_volvec could not read serialized hash build entry");
		}

		VolVecVector<DataChunk<DEFAULT_CHUNK_SIZE> *> chunks_buffer;
		chunks_buffer.resize(header.chunk_count);
		for (uint32 i = 0; i < header.chunk_count; i++)
		{
			MemoryContext old_context = MemoryContextSwitchTo(state.memory_context_);
			DataChunk<DEFAULT_CHUNK_SIZE> *chunk = new DataChunk<DEFAULT_CHUNK_SIZE>();
			MemoryContextSwitchTo(old_context);

			if (!DeserializeChunk(state, buffer, buffer_size, &offset, chunk))
				elog(ERROR, "pg_volvec could not deserialize hash build chunk");
			chunks_buffer[i] = chunk;
		}

	if (offset != buffer_size)
		elog(ERROR, "pg_volvec serialized hash build buffer has trailing bytes");

	std::unordered_map<uint32_t, std::array<uint32_t, VOLVEC_RADIX_FANOUT>> chunk_to_partition_local_idx;
	
	for (uint32 i = 0; i < header.entry_count; i++)
	{
		const SerializedHashBuildEntry &serialized = entries_buffer[i];
		int partition_id = volvec_radix_partition_idx(serialized.hash);
		
		if (serialized.chunk_idx >= header.chunk_count)
			elog(ERROR, "pg_volvec entry references invalid chunk %u", serialized.chunk_idx);
		
		auto it = chunk_to_partition_local_idx.find(serialized.chunk_idx);
		if (it == chunk_to_partition_local_idx.end())
		{
			std::array<uint32_t, VOLVEC_RADIX_FANOUT> local_indices;
			for (int p = 0; p < VOLVEC_RADIX_FANOUT; p++)
				local_indices[p] = 0xFFFFFFFF;
			
			local_indices[partition_id] = (uint32) state.partition_chunks_[partition_id].size();
			state.partition_chunks_[partition_id].push_back(chunks_buffer[serialized.chunk_idx]);
			chunk_to_partition_local_idx[serialized.chunk_idx] = local_indices;
		}
		else if (it->second[partition_id] == 0xFFFFFFFF)
		{
			it->second[partition_id] = (uint32) state.partition_chunks_[partition_id].size();
			state.partition_chunks_[partition_id].push_back(chunks_buffer[serialized.chunk_idx]);
		}
	}

	for (uint32 i = 0; i < header.entry_count; i++)
	{
		const SerializedHashBuildEntry &serialized = entries_buffer[i];
		VecHashJoinState::VecHashEntry entry{};
		int partition_id = volvec_radix_partition_idx(serialized.hash);

		if (serialized.row_idx >= DEFAULT_CHUNK_SIZE)
			elog(ERROR, "pg_volvec entry references invalid row %u", serialized.row_idx);
		
		entry.hash = serialized.hash;
		entry.key = serialized.key;
		entry.next = -1;
		entry.chunk_idx = (uint32_t) state.partition_row_stores_[partition_id].row_count;
		entry.row_idx = serialized.row_idx;
		if (serialized.chunk_idx < header.chunk_count)
		{
			DataChunk<DEFAULT_CHUNK_SIZE> *chunk = chunks_buffer[serialized.chunk_idx];
			if (chunk != nullptr && serialized.row_idx < chunk->count)
				state.append_partition_row_from_input((uint32_t) partition_id,
										 *chunk,
										 serialized.row_idx,
										 serialized.key,
										 serialized.hash);
		}
		state.partition_entries_[partition_id].push_back(entry);
		state.build_histogram_[partition_id]++;
	}
	
	state.total_build_rows_ += header.entry_count;
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
		partition_chunks_[i].clear();
		partition_entries_[i].clear();

		if (partition_bucket_heads_[i] != nullptr &&
			!partition_bucket_heads_external_[i])
		{
			pfree(partition_bucket_heads_[i]);
		}
		partition_bucket_heads_[i] = nullptr;
		partition_bucket_masks_[i] = 0;
		partition_bucket_heads_external_[i] = false;
		build_histogram_[i] = 0;
	}
	memset(&build_partition_table_, 0, sizeof(build_partition_table_));
	memset(&probe_partition_table_, 0, sizeof(probe_partition_table_));
	clear_partition_row_stores();
}

void
VecHashJoinState::clear_partition_row_stores()
{
	for (int i = 0; i < VOLVEC_RADIX_FANOUT; i++)
	{
		for (auto *block : partition_row_stores_[i].blocks)
			delete block;
		partition_row_stores_[i].blocks.clear();
		partition_row_stores_[i].chain_next.clear();
		partition_row_stores_[i].varlen_arena.clear();
		partition_row_stores_[i].row_count = 0;
		partition_row_stores_[i].row_width = 0;
	}
	row_layout_ = VecHashRowLayout{};
}

void
VecHashJoinState::init_row_layout_if_needed()
{
	if (row_layout_.row_width != 0)
		return;

	uint32_t offset = 0;
	row_layout_.nulls_offset = (uint16_t) offset;
	row_layout_.fixed_field_count = (uint16_t) (key_cols_.size() + inner_payload_cols_.size());
	offset += row_layout_.fixed_field_count;
	offset = VolvecAlignU32(offset, 8);
	row_layout_.key_offset = (uint16_t) offset;
	offset += (uint32_t) key_cols_.size() * sizeof(uint64_t);
	row_layout_.payload_offset = (uint16_t) offset;
	for (const auto &payload_col : inner_payload_cols_)
	{
		switch (payload_col.meta.storage_kind)
		{
			case VecOutputStorageKind::Int32:
				offset = VolvecAlignU32(offset, 4);
				offset += sizeof(int32_t);
				break;
			case VecOutputStorageKind::Int64:
			case VecOutputStorageKind::NumericScaledInt64:
			case VecOutputStorageKind::NumericAvgPair:
			case VecOutputStorageKind::Double:
				offset = VolvecAlignU32(offset, 8);
				offset += sizeof(uint64_t);
				break;
			case VecOutputStorageKind::StringRef:
				offset = VolvecAlignU32(offset, 8);
				offset += sizeof(ParallelHashRowVarlenRef);
				break;
		}
	}
	row_layout_.flags_offset = (uint16_t) offset;
	offset += sizeof(uint8_t);
	offset = VolvecAlignU32(offset, 8);
	row_layout_.hash_offset = (uint16_t) offset;
	offset += sizeof(uint64_t);
	row_layout_.row_width = VolvecAlignU32(offset, 8);

	for (int i = 0; i < VOLVEC_RADIX_FANOUT; i++)
		partition_row_stores_[i].row_width = row_layout_.row_width;
}

VecHashRowBlock *
VecHashJoinState::allocate_partition_row_block(uint32_t partition_id, uint32_t start_row_ordinal)
{
	MemoryContext old_context = MemoryContextSwitchTo(memory_context_);
	VecHashRowBlock *block = new VecHashRowBlock();
	MemoryContextSwitchTo(old_context);
	block->row_width = row_layout_.row_width;
	block->start_row_ordinal = start_row_ordinal;
	block->bytes.reserve((size_t) row_layout_.row_width * DEFAULT_CHUNK_SIZE);
	partition_row_stores_[partition_id].blocks.push_back(block);
	return block;
}

void
VecHashJoinState::append_partition_row_from_input(uint32_t partition_id,
										 const DataChunk<DEFAULT_CHUNK_SIZE> &input,
										 int src_row,
										 const VecHashJoinKey &key,
										 uint32_t hash)
{
	init_row_layout_if_needed();
	VecHashPartitionRowStore &store = partition_row_stores_[partition_id];
	VecHashRowBlock *block =
		(store.blocks.empty() || store.blocks.back()->row_count >= DEFAULT_CHUNK_SIZE) ?
		allocate_partition_row_block(partition_id, store.row_count) : store.blocks.back();
	uint8_t row_buf[1024];
	if (row_layout_.row_width > sizeof(row_buf))
		elog(ERROR, "pg_volvec row layout width %u exceeds temporary buffer", row_layout_.row_width);
	memset(row_buf, 0, row_layout_.row_width);

	for (size_t i = 0; i < key.num_keys; i++)
		memcpy(row_buf + row_layout_.key_offset + i * sizeof(uint64_t), &key.values[i], sizeof(uint64_t));

	uint32_t payload_offset = row_layout_.payload_offset;
	for (uint16_t payload_idx = 0; payload_idx < inner_payload_cols_.size(); payload_idx++)
	{
		const VecHashPayloadCol &payload_col = inner_payload_cols_[payload_idx];
		uint16_t src_col = payload_col.source_col;
		row_buf[row_layout_.nulls_offset + key_cols_.size() + payload_idx] = input.nulls[src_col][src_row];
		if (input.nulls[src_col][src_row])
		{
			switch (payload_col.meta.storage_kind)
			{
				case VecOutputStorageKind::Int32:
					payload_offset = VolvecAlignU32(payload_offset, 4) + sizeof(int32_t);
					break;
				case VecOutputStorageKind::Int64:
				case VecOutputStorageKind::NumericScaledInt64:
				case VecOutputStorageKind::NumericAvgPair:
				case VecOutputStorageKind::Double:
					payload_offset = VolvecAlignU32(payload_offset, 8) + sizeof(uint64_t);
					break;
				case VecOutputStorageKind::StringRef:
					payload_offset = VolvecAlignU32(payload_offset, 8) + sizeof(ParallelHashRowVarlenRef);
					break;
			}
			continue;
		}

		switch (payload_col.meta.storage_kind)
		{
			case VecOutputStorageKind::Int32:
			{
				payload_offset = VolvecAlignU32(payload_offset, 4);
				memcpy(row_buf + payload_offset, &input.int32_columns[src_col][src_row], sizeof(int32_t));
				payload_offset += sizeof(int32_t);
				break;
			}
			case VecOutputStorageKind::Int64:
			case VecOutputStorageKind::NumericScaledInt64:
			case VecOutputStorageKind::NumericAvgPair:
			{
				payload_offset = VolvecAlignU32(payload_offset, 8);
				memcpy(row_buf + payload_offset, &input.int64_columns[src_col][src_row], sizeof(int64_t));
				payload_offset += sizeof(int64_t);
				break;
			}
			case VecOutputStorageKind::Double:
			{
				payload_offset = VolvecAlignU32(payload_offset, 8);
				memcpy(row_buf + payload_offset, &input.double_columns[src_col][src_row], sizeof(double));
				payload_offset += sizeof(double);
				break;
			}
			case VecOutputStorageKind::StringRef:
			{
				payload_offset = VolvecAlignU32(payload_offset, 8);
				ParallelHashRowVarlenRef ref;
				memset(&ref, 0, sizeof(ref));
				const VecStringRef &src_ref = input.string_columns[src_col][src_row];
				ref.prefix = src_ref.prefix;
				ref.length = src_ref.len;
				if (src_ref.len == 0)
				{
					ref.offset = 0;
				}
				else if (VecStringRefIsInline(src_ref))
				{
					ref.offset = VOLVEC_ROW_VARLEN_INLINE_OFFSET;
				}
				else
				{
					const char *ptr = input.get_string_ptr(src_ref);
					ref.offset = (uint32_t) store.varlen_arena.size();
					store.varlen_arena.insert(store.varlen_arena.end(), ptr, ptr + src_ref.len);
				}
				memcpy(row_buf + payload_offset, &ref, sizeof(ref));
				payload_offset += sizeof(ref);
				break;
			}
		}
	}

	row_buf[row_layout_.flags_offset] = 0;
	uint64_t hash64 = hash;
	memcpy(row_buf + row_layout_.hash_offset, &hash64, sizeof(uint64_t));
	block->bytes.insert(block->bytes.end(), row_buf, row_buf + row_layout_.row_width);
	block->row_count++;
	store.row_count++;
	store.chain_next.push_back(-1);
}

const uint8_t *
VecHashJoinState::get_partition_row_ptr(uint32_t partition_id, uint32_t row_ordinal) const
{
	if (partition_id >= VOLVEC_RADIX_FANOUT)
		return nullptr;
	const VecHashPartitionRowStore &store = partition_row_stores_[partition_id];
	for (const auto *block : store.blocks)
	{
		if (block == nullptr)
			continue;
		if (row_ordinal < block->start_row_ordinal ||
			row_ordinal >= block->start_row_ordinal + block->row_count)
			continue;
		uint32_t in_block = row_ordinal - block->start_row_ordinal;
		return block->bytes.data() + ((size_t) in_block * block->row_width);
	}
	return nullptr;
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
	shared_hash_partitions_ = nullptr;
	shared_hash_partition_count_ = 0;
	shared_payload_cols_view_ = nullptr;
	shared_payload_col_count_ = 0;
	shared_bucket_heads_view_ = nullptr;
	shared_bucket_count_ = 0;
	shared_entries_view_ = nullptr;
	shared_entry_count_ = 0;
	shared_hash_chunks_ = nullptr;
	shared_hash_chunk_count_ = 0;
}

size_t
VecHashJoinState::compute_shared_hash_bridge_size() const
{
	size_t size = sizeof(ParallelHashBuildState);
	
	if (use_parallel_ht_ && total_build_rows_ > 0)
	{
		size += VOLVEC_RADIX_FANOUT * sizeof(ParallelHashBuildPartition);
		size += VOLVEC_RADIX_FANOUT * sizeof(ParallelHashBuildPartitionRows);

		size += inner_payload_cols_.size() * sizeof(VecHashPayloadCol);

		for (int part_idx = 0; part_idx < VOLVEC_RADIX_FANOUT; part_idx++)
		{
		if (build_histogram_[part_idx] == 0)
			continue;

		size_t bucket_count = 1024;
		while (bucket_count < partition_entries_[part_idx].size() * 2)
			bucket_count <<= 1;
		size += bucket_count * sizeof(int32_t);

			size += partition_entries_[part_idx].size() * sizeof(VecHashEntry);
			size += partition_chunks_[part_idx].size() * sizeof(ParallelHashBuildChunk);
			size += partition_row_stores_[part_idx].blocks.size() * sizeof(ParallelHashRowBlockDesc);

			for (const auto *chunk : partition_chunks_[part_idx])
			{
			for (uint16_t payload_idx = 0; payload_idx < inner_payload_cols_.size(); payload_idx++)
			{
				const VecHashPayloadCol &payload_col = inner_payload_cols_[payload_idx];

				size += (size_t) chunk->count * sizeof(uint8_t);
				switch (payload_col.meta.storage_kind)
				{
					case VecOutputStorageKind::Int32:
						size += (size_t) chunk->count * sizeof(int32_t);
						break;
					case VecOutputStorageKind::Int64:
					case VecOutputStorageKind::NumericScaledInt64:
					case VecOutputStorageKind::NumericAvgPair:
						size += (size_t) chunk->count * sizeof(int64_t);
						size += (size_t) chunk->count * sizeof(double);
						break;
					case VecOutputStorageKind::Double:
						size += (size_t) chunk->count * sizeof(double);
						break;
					case VecOutputStorageKind::StringRef:
						size += (size_t) chunk->count * sizeof(VecStringRef);
						break;
				}
			}
			size += chunk->string_arena.size();
		}
		}
	}
	else
	{
		size += 1 * sizeof(ParallelHashBuildPartition);

		size += inner_payload_cols_.size() * sizeof(VecHashPayloadCol);
		size += bucket_heads_.size() * sizeof(int32_t);
		size += entries_.size() * sizeof(VecHashEntry);
		size += inner_chunks_.size() * sizeof(ParallelHashBuildChunk);
		for (const auto *chunk : inner_chunks_)
		{
			for (uint16_t payload_idx = 0; payload_idx < inner_payload_cols_.size(); payload_idx++)
			{
				const VecHashPayloadCol &payload_col = inner_payload_cols_[payload_idx];

				size += (size_t) chunk->count * sizeof(uint8_t);
				switch (payload_col.meta.storage_kind)
				{
					case VecOutputStorageKind::Int32:
						size += (size_t) chunk->count * sizeof(int32_t);
						break;
					case VecOutputStorageKind::Int64:
					case VecOutputStorageKind::NumericScaledInt64:
					case VecOutputStorageKind::NumericAvgPair:
						size += (size_t) chunk->count * sizeof(int64_t);
						size += (size_t) chunk->count * sizeof(double);
						break;
					case VecOutputStorageKind::Double:
						size += (size_t) chunk->count * sizeof(double);
						break;
					case VecOutputStorageKind::StringRef:
						size += (size_t) chunk->count * sizeof(VecStringRef);
						break;
				}
			}
			size += chunk->string_arena.size();
		}
	}

	return size;
}

void
VecHashJoinState::write_shared_hash_bridge(uint8_t *buffer, size_t buffer_size) const
{
	ParallelHashBuildState *state;
	size_t offset = sizeof(ParallelHashBuildState);

	if (buffer == nullptr || buffer_size != compute_shared_hash_bridge_size())
		elog(ERROR, "pg_volvec shared hash bridge size mismatch");

	state = reinterpret_cast<ParallelHashBuildState *>(buffer);
	memset(state, 0, sizeof(*state));
	state->magic = VOLVEC_SHARED_HASH_BUILD_MAGIC;
	state->version = VOLVEC_SHARED_HASH_BUILD_VERSION;
	state->build_complete = inner_built_ ? 1 : 0;

	if (use_parallel_ht_ && total_build_rows_ > 0)
	{
		state->num_partitions = VOLVEC_RADIX_FANOUT;
		state->num_payload_cols = (uint32) inner_payload_cols_.size();
		state->total_entries = (uint32) total_build_rows_;
		state->row_layout_version = 1;
		state->row_width = row_layout_.row_width;

		state->partitions_offset = offset;
		state->partitions_size = VOLVEC_RADIX_FANOUT * sizeof(ParallelHashBuildPartition);

		ParallelHashBuildPartition *partitions =
			reinterpret_cast<ParallelHashBuildPartition *>(buffer + offset);
		memset(partitions, 0, (size_t) state->partitions_size);
		offset += (size_t) state->partitions_size;

		state->row_partitions_offset = offset;
		state->row_partitions_size = VOLVEC_RADIX_FANOUT * sizeof(ParallelHashBuildPartitionRows);
		ParallelHashBuildPartitionRows *row_partitions =
			reinterpret_cast<ParallelHashBuildPartitionRows *>(buffer + offset);
		memset(row_partitions, 0, (size_t) state->row_partitions_size);
		offset += (size_t) state->row_partitions_size;

		if (!inner_payload_cols_.empty())
		{
			state->payload_cols_offset = offset;
			state->payload_cols_size = inner_payload_cols_.size() * sizeof(VecHashPayloadCol);
			memcpy(buffer + offset, inner_payload_cols_.data(), (size_t) state->payload_cols_size);
			offset += (size_t) state->payload_cols_size;
	}

	uint32_t total_chunks = 0;
	for (int part_idx = 0; part_idx < VOLVEC_RADIX_FANOUT; part_idx++)
	{
		if (build_histogram_[part_idx] == 0)
			continue;

		ParallelHashBuildPartition *partition = &partitions[part_idx];
		ParallelHashBuildPartitionRows *row_partition = &row_partitions[part_idx];
		const auto &entries = partition_entries_[part_idx];
		const auto &chunks = partition_chunks_[part_idx];
		const auto &row_store = partition_row_stores_[part_idx];

		partition->entry_count = (uint32) entries.size();
		row_partition->row_count = row_store.row_count;
		row_partition->row_width = row_store.row_width;

		size_t bucket_count = 1024;
		while (bucket_count < entries.size() * 2)
			bucket_count <<= 1;

		partition->bucket_count = (uint32) bucket_count;
		partition->bucket_mask = (uint32) (bucket_count - 1);
		partition->bucket_heads_offset = offset;
		partition->bucket_heads_size = bucket_count * sizeof(int32_t);

		if (partition_bucket_heads_[part_idx] != nullptr)
		{
			memcpy(buffer + offset, partition_bucket_heads_[part_idx],
				   bucket_count * sizeof(int32_t));
		}
		offset += bucket_count * sizeof(int32_t);

		partition->entries_offset = offset;
		partition->entries_size = entries.size() * sizeof(VecHashEntry);
		memcpy(buffer + offset, entries.data(), (size_t) partition->entries_size);
		offset += (size_t) partition->entries_size;

		partition->chunk_count = (uint32) chunks.size();
		partition->chunks_offset = offset;
		size_t chunks_header_size = chunks.size() * sizeof(ParallelHashBuildChunk);
		ParallelHashBuildChunk *chunk_headers =
			reinterpret_cast<ParallelHashBuildChunk *>(buffer + offset);
		memset(chunk_headers, 0, chunks_header_size);
		offset += chunks_header_size;

		for (size_t chunk_idx = 0; chunk_idx < chunks.size(); chunk_idx++)
		{
			const DataChunk<DEFAULT_CHUNK_SIZE> &chunk = *chunks[chunk_idx];
				ParallelHashBuildChunk &chunk_header = chunk_headers[chunk_idx];

				chunk_header.row_count = chunk.count;
				chunk_header.string_arena_size = (uint32) chunk.string_arena.size();
				if (!chunk.string_arena.empty())
				{
					chunk_header.string_arena_offset = offset;
					memcpy(buffer + offset, chunk.string_arena.data(), chunk.string_arena.size());
					offset += chunk.string_arena.size();
				}

				for (uint16_t payload_idx = 0; payload_idx < inner_payload_cols_.size(); payload_idx++)
				{
					const VecHashPayloadCol &payload_col = inner_payload_cols_[payload_idx];
					size_t nulls_size = (size_t) chunk.count * sizeof(uint8_t);

					chunk_header.nulls_offsets[payload_idx] = offset;
					memcpy(buffer + offset, chunk.nulls[payload_idx], nulls_size);
					offset += nulls_size;

					switch (payload_col.meta.storage_kind)
					{
						case VecOutputStorageKind::Int32:
							chunk_header.value_offsets[payload_idx] = offset;
							memcpy(buffer + offset, chunk.int32_columns[payload_idx],
								   (size_t) chunk.count * sizeof(int32_t));
							offset += (size_t) chunk.count * sizeof(int32_t);
							break;
						case VecOutputStorageKind::Int64:
						case VecOutputStorageKind::NumericScaledInt64:
						case VecOutputStorageKind::NumericAvgPair:
							chunk_header.value_offsets[payload_idx] = offset;
							memcpy(buffer + offset, chunk.int64_columns[payload_idx],
								   (size_t) chunk.count * sizeof(int64_t));
							offset += (size_t) chunk.count * sizeof(int64_t);
							chunk_header.aux_value_offsets[payload_idx] = offset;
							memcpy(buffer + offset, chunk.double_columns[payload_idx],
								   (size_t) chunk.count * sizeof(double));
							offset += (size_t) chunk.count * sizeof(double);
							break;
						case VecOutputStorageKind::Double:
							chunk_header.value_offsets[payload_idx] = offset;
							memcpy(buffer + offset, chunk.double_columns[payload_idx],
								   (size_t) chunk.count * sizeof(double));
							offset += (size_t) chunk.count * sizeof(double);
							break;
						case VecOutputStorageKind::StringRef:
							chunk_header.value_offsets[payload_idx] = offset;
							memcpy(buffer + offset, chunk.string_columns[payload_idx],
								   (size_t) chunk.count * sizeof(VecStringRef));
							offset += (size_t) chunk.count * sizeof(VecStringRef);
							break;
					}
				}
		}

		partition->chunks_size = offset - partition->chunks_offset;

		if (!row_store.blocks.empty())
		{
			row_partition->row_blocks_offset = offset;
			row_partition->row_blocks_size = row_store.blocks.size() * sizeof(ParallelHashRowBlockDesc);
			row_partition->row_block_descs_offset = offset;
			row_partition->row_block_descs_size = row_partition->row_blocks_size;
			ParallelHashRowBlockDesc *row_descs =
				reinterpret_cast<ParallelHashRowBlockDesc *>(buffer + offset);
			memset(row_descs, 0, (size_t) row_partition->row_blocks_size);
			offset += (size_t) row_partition->row_blocks_size;
			for (size_t block_idx = 0; block_idx < row_store.blocks.size(); block_idx++)
			{
				const VecHashRowBlock *block = row_store.blocks[block_idx];
				if (block == nullptr)
					continue;
				row_descs[block_idx].block_offset = offset;
				row_descs[block_idx].start_row_ordinal = block->start_row_ordinal;
				row_descs[block_idx].row_count = block->row_count;
				row_descs[block_idx].row_width = block->row_width;
				if (!block->bytes.empty())
				{
					memcpy(buffer + offset, block->bytes.data(), block->bytes.size());
					offset += block->bytes.size();
				}
			}
		}
		if (!row_store.varlen_arena.empty())
		{
			row_partition->varlen_offset = offset;
			row_partition->varlen_size = row_store.varlen_arena.size();
			memcpy(buffer + offset, row_store.varlen_arena.data(), row_store.varlen_arena.size());
			offset += row_store.varlen_arena.size();
		}
		total_chunks += (uint32) chunks.size();
	}
	state->num_chunks = total_chunks;
	}
	else
	{
		state->num_partitions = 1;
		state->total_entries = (uint32) entries_.size();
		state->num_chunks = (uint32) inner_chunks_.size();
		state->num_payload_cols = (uint32) inner_payload_cols_.size();
		state->partitions_offset = offset;
		state->partitions_size = 1 * sizeof(ParallelHashBuildPartition);
		state->bucket_count = (uint32) bucket_heads_.size();
		state->bucket_mask = (uint32) bucket_mask_;

		ParallelHashBuildPartition *partition =
			reinterpret_cast<ParallelHashBuildPartition *>(buffer + offset);
		memset(partition, 0, sizeof(*partition));
		partition->entry_count = (uint32) entries_.size();
		partition->bucket_count = (uint32) bucket_heads_.size();
		partition->bucket_mask = (uint32) bucket_mask_;
		offset += sizeof(ParallelHashBuildPartition);

		if (!inner_payload_cols_.empty())
		{
			state->payload_cols_offset = offset;
			state->payload_cols_size = inner_payload_cols_.size() * sizeof(VecHashPayloadCol);
			memcpy(buffer + offset, inner_payload_cols_.data(), (size_t) state->payload_cols_size);
			offset += (size_t) state->payload_cols_size;
		}
		if (!bucket_heads_.empty())
		{
			state->bucket_heads_offset = offset;
			state->bucket_heads_size = bucket_heads_.size() * sizeof(int32_t);
			partition->bucket_heads_offset = offset;
			partition->bucket_heads_size = state->bucket_heads_size;
			memcpy(buffer + offset, bucket_heads_.data(), (size_t) state->bucket_heads_size);
			offset += (size_t) state->bucket_heads_size;
		}
		if (!entries_.empty())
		{
			state->entries_offset = offset;
			state->entries_size = entries_.size() * sizeof(VecHashEntry);
			partition->entries_offset = offset;
			partition->entries_size = state->entries_size;
			memcpy(buffer + offset, entries_.data(), (size_t) state->entries_size);
			offset += (size_t) state->entries_size;
		}
		if (!inner_chunks_.empty())
		{
			partition->chunk_count = (uint32) inner_chunks_.size();
			partition->chunks_offset = offset;
			
			ParallelHashBuildChunk *chunk_headers;
			state->chunks_offset = offset;
			state->chunks_size = inner_chunks_.size() * sizeof(ParallelHashBuildChunk);
			chunk_headers = reinterpret_cast<ParallelHashBuildChunk *>(buffer + offset);
			memset(chunk_headers, 0, (size_t) state->chunks_size);
			offset += (size_t) state->chunks_size;

			for (size_t chunk_idx = 0; chunk_idx < inner_chunks_.size(); chunk_idx++)
			{
				const DataChunk<DEFAULT_CHUNK_SIZE> &chunk = *inner_chunks_[chunk_idx];
				ParallelHashBuildChunk &chunk_header = chunk_headers[chunk_idx];

				chunk_header.row_count = chunk.count;
				chunk_header.string_arena_size = (uint32) chunk.string_arena.size();
				if (!chunk.string_arena.empty())
				{
					chunk_header.string_arena_offset = offset;
					memcpy(buffer + offset, chunk.string_arena.data(), chunk.string_arena.size());
					offset += chunk.string_arena.size();
				}

				for (uint16_t payload_idx = 0; payload_idx < inner_payload_cols_.size(); payload_idx++)
				{
					const VecHashPayloadCol &payload_col = inner_payload_cols_[payload_idx];
					size_t nulls_size = (size_t) chunk.count * sizeof(uint8_t);

					chunk_header.nulls_offsets[payload_idx] = offset;
					memcpy(buffer + offset, chunk.nulls[payload_idx], nulls_size);
					offset += nulls_size;

					switch (payload_col.meta.storage_kind)
					{
						case VecOutputStorageKind::Int32:
							chunk_header.value_offsets[payload_idx] = offset;
							memcpy(buffer + offset, chunk.int32_columns[payload_idx],
								   (size_t) chunk.count * sizeof(int32_t));
							offset += (size_t) chunk.count * sizeof(int32_t);
							break;
						case VecOutputStorageKind::Int64:
						case VecOutputStorageKind::NumericScaledInt64:
						case VecOutputStorageKind::NumericAvgPair:
							chunk_header.value_offsets[payload_idx] = offset;
							memcpy(buffer + offset, chunk.int64_columns[payload_idx],
								   (size_t) chunk.count * sizeof(int64_t));
							offset += (size_t) chunk.count * sizeof(int64_t);
							chunk_header.aux_value_offsets[payload_idx] = offset;
							memcpy(buffer + offset, chunk.double_columns[payload_idx],
								   (size_t) chunk.count * sizeof(double));
							offset += (size_t) chunk.count * sizeof(double);
							break;
						case VecOutputStorageKind::Double:
							chunk_header.value_offsets[payload_idx] = offset;
							memcpy(buffer + offset, chunk.double_columns[payload_idx],
								   (size_t) chunk.count * sizeof(double));
							offset += (size_t) chunk.count * sizeof(double);
							break;
						case VecOutputStorageKind::StringRef:
							chunk_header.value_offsets[payload_idx] = offset;
							memcpy(buffer + offset, chunk.string_columns[payload_idx],
								   (size_t) chunk.count * sizeof(VecStringRef));
							offset += (size_t) chunk.count * sizeof(VecStringRef);
							break;
					}
				}
			}
			
			partition->chunks_size = offset - partition->chunks_offset;
		}
	}

	if (offset != buffer_size)
		elog(ERROR, "pg_volvec shared hash bridge serialized size mismatch: expected %zu got %zu",
			 buffer_size, offset);
}

void
VecHashJoinState::materialize_shared_hash_build_view()
{
	ParallelHashBuildState *state;

	clear_partition_hash_tables();
	clear_inner_build_chunks();
	clear_shared_hash_payload_view();
	inner_payload_cols_.clear();
	bucket_heads_.clear();
	entries_.clear();
	inner_entry_matched_.clear();

	if (shared_hash_bridge_buffer_ == nullptr ||
		shared_hash_bridge_buffer_size_ < sizeof(ParallelHashBuildState))
		elog(ERROR, "pg_volvec shared hash bridge buffer is invalid");

	state = reinterpret_cast<ParallelHashBuildState *>(shared_hash_bridge_buffer_);
	if (state->magic != VOLVEC_SHARED_HASH_BUILD_MAGIC ||
		state->version != VOLVEC_SHARED_HASH_BUILD_VERSION)
		elog(ERROR, "pg_volvec shared hash bridge header mismatch");
	if (state->payload_cols_offset + state->payload_cols_size > shared_hash_bridge_buffer_size_ ||
		state->partitions_offset + state->partitions_size > shared_hash_bridge_buffer_size_ ||
		state->row_partitions_offset + state->row_partitions_size > shared_hash_bridge_buffer_size_ ||
		state->bucket_heads_offset + state->bucket_heads_size > shared_hash_bridge_buffer_size_ ||
		state->entries_offset + state->entries_size > shared_hash_bridge_buffer_size_ ||
		state->chunks_offset + state->chunks_size > shared_hash_bridge_buffer_size_)
		elog(ERROR, "pg_volvec shared hash bridge has invalid offsets");

	shared_hash_bridge_ = state;
	shared_hash_partitions_ = state->num_partitions == 0 ? nullptr :
		reinterpret_cast<const ParallelHashBuildPartition *>(shared_hash_bridge_buffer_ + state->partitions_offset);
	shared_hash_partition_count_ = state->num_partitions;
	shared_payload_cols_view_ = state->num_payload_cols == 0 ? nullptr :
		reinterpret_cast<const VecHashPayloadCol *>(shared_hash_bridge_buffer_ + state->payload_cols_offset);
	shared_payload_col_count_ = state->num_payload_cols;
	shared_bucket_heads_view_ = state->bucket_count == 0 ? nullptr :
		reinterpret_cast<const int32_t *>(shared_hash_bridge_buffer_ + state->bucket_heads_offset);
	shared_bucket_count_ = state->bucket_count;
	shared_entries_view_ = state->total_entries == 0 ? nullptr :
		reinterpret_cast<const VecHashEntry *>(shared_hash_bridge_buffer_ + state->entries_offset);
	shared_entry_count_ = state->total_entries;
	shared_hash_chunks_ = state->num_chunks == 0 ? nullptr :
		reinterpret_cast<const ParallelHashBuildChunk *>(shared_hash_bridge_buffer_ + state->chunks_offset);
	shared_hash_chunk_count_ = state->num_chunks;
	bucket_mask_ = state->bucket_mask;
	inner_built_ = state->build_complete != 0;
	use_parallel_ht_ = (state->num_partitions == VOLVEC_RADIX_FANOUT);
	row_layout_.row_width = state->row_width;
	
	for (uint32_t i = 0; i < shared_hash_partition_count_ && i < VOLVEC_RADIX_FANOUT; i++)
	{
		const ParallelHashBuildPartition &partition = shared_hash_partitions_[i];
		partition_bucket_heads_[i] = partition.bucket_count == 0 ? nullptr :
			reinterpret_cast<int32_t *>(shared_hash_bridge_buffer_ + partition.bucket_heads_offset);
		partition_bucket_masks_[i] = partition.bucket_mask;
		partition_bucket_heads_external_[i] = true;
	}

	if (use_parallel_ht_ && state->row_partitions_size >= VOLVEC_RADIX_FANOUT * sizeof(ParallelHashBuildPartitionRows))
	{
		const ParallelHashBuildPartitionRows *row_partitions =
			reinterpret_cast<const ParallelHashBuildPartitionRows *>(shared_hash_bridge_buffer_ + state->row_partitions_offset);
		for (uint32_t part_idx = 0; part_idx < VOLVEC_RADIX_FANOUT; part_idx++)
		{
			const ParallelHashBuildPartitionRows &row_partition = row_partitions[part_idx];
			VecHashPartitionRowStore &store = partition_row_stores_[part_idx];
			store.row_width = row_partition.row_width;
			store.row_count = row_partition.row_count;
			store.chain_next.assign(row_partition.row_count, -1);
			if (row_partition.varlen_size > 0)
			{
				store.varlen_arena.insert(store.varlen_arena.end(),
					(const char *) (shared_hash_bridge_buffer_ + row_partition.varlen_offset),
					(const char *) (shared_hash_bridge_buffer_ + row_partition.varlen_offset + row_partition.varlen_size));
			}
			if (row_partition.row_block_descs_size > 0)
			{
				size_t block_count = row_partition.row_block_descs_size / sizeof(ParallelHashRowBlockDesc);
				const ParallelHashRowBlockDesc *row_descs =
					reinterpret_cast<const ParallelHashRowBlockDesc *>(shared_hash_bridge_buffer_ + row_partition.row_block_descs_offset);
				for (size_t block_idx = 0; block_idx < block_count; block_idx++)
				{
					const ParallelHashRowBlockDesc &desc = row_descs[block_idx];
					MemoryContext old = MemoryContextSwitchTo(memory_context_);
					VecHashRowBlock *block = new VecHashRowBlock();
					MemoryContextSwitchTo(old);
					block->row_width = desc.row_width;
					block->row_count = desc.row_count;
					block->start_row_ordinal = desc.start_row_ordinal;
					size_t block_bytes = (size_t) desc.row_count * desc.row_width;
					block->bytes.resize(block_bytes);
					memcpy(block->bytes.data(), shared_hash_bridge_buffer_ + desc.block_offset, block_bytes);
					store.blocks.push_back(block);
				}
			}
		}
	}

	if (use_parallel_ht_ && shared_hash_partition_count_ == VOLVEC_RADIX_FANOUT)
	{
		if (shared_payload_col_count_ > 0)
		{
			inner_payload_cols_.assign(shared_payload_cols_view_,
									   shared_payload_cols_view_ + shared_payload_col_count_);
		}
	}

	inner_entry_matched_.assign(state->total_entries, 0);
}

bool
VecHashJoinState::candidate_passes_join_filter_for_build_entry(const DataChunk<DEFAULT_CHUNK_SIZE> &probe_src,
								   int probe_row,
								   uint32_t partition_id,
								   uint32_t chunk_idx,
								   uint16_t row_idx)
{
	if (!join_filter_program_)
		return true;

	join_filter_chunk_.reset();
	join_filter_chunk_.count = 1;
	for (const auto &output_col : output_cols_)
	{
		int out_col = output_col.output_resno - 1;

		if (output_col.side == VecJoinSide::Outer)
		{
			join_filter_chunk_.nulls[out_col][0] = probe_src.nulls[output_col.input_col][probe_row];
			if (join_filter_chunk_.nulls[out_col][0])
				continue;
			switch (output_col.meta.storage_kind)
			{
				case VecOutputStorageKind::Double:
					join_filter_chunk_.double_columns[out_col][0] =
						probe_src.double_columns[output_col.input_col][probe_row];
					break;
				case VecOutputStorageKind::Int64:
				case VecOutputStorageKind::NumericScaledInt64:
				case VecOutputStorageKind::NumericAvgPair:
					join_filter_chunk_.int64_columns[out_col][0] =
						probe_src.int64_columns[output_col.input_col][probe_row];
					join_filter_chunk_.double_columns[out_col][0] =
						probe_src.double_columns[output_col.input_col][probe_row];
					break;
				case VecOutputStorageKind::StringRef:
					join_filter_chunk_.string_columns[out_col][0] =
						CopyStringRefToChunk(join_filter_chunk_,
										 probe_src,
										 probe_src.string_columns[output_col.input_col][probe_row]);
					break;
				case VecOutputStorageKind::Int32:
					join_filter_chunk_.int32_columns[out_col][0] =
						probe_src.int32_columns[output_col.input_col][probe_row];
					break;
			}
		}
		else
		{
			if (output_col.input_col >= 0 &&
				output_col.input_col < (int) required_build_col_mask_.size())
				required_build_col_mask_[output_col.input_col] = 1;
			copy_inner_payload_value_to_chunk(join_filter_chunk_,
								 0,
								 out_col,
								 output_col.meta,
								 output_col.input_col,
								 partition_id,
								 chunk_idx,
								 row_idx);
		}
	}

	join_filter_program_->evaluate(join_filter_chunk_);
	return (join_filter_chunk_.has_selection ? join_filter_chunk_.sel.count : join_filter_chunk_.count) > 0;
}

void
VecHashJoinState::copy_inner_payload_value_to_chunk(DataChunk<DEFAULT_CHUNK_SIZE> &dst,
							int dst_row,
									int dst_col,
									const VecOutputColMeta &meta,
									int src_col,
									uint32_t partition_id,
									uint32_t chunk_idx,
									uint16_t row_idx) const
{
	const DataChunk<DEFAULT_CHUNK_SIZE> *src;
	const uint8_t *base;
	const ParallelHashBuildChunk *chunk_header;
	uint32_t row_ordinal = 0;

	if (src_col < 0 || src_col >= 16 ||
		dst_col < 0 || dst_col >= 16)
		return;
	if (shared_payload_cols_view_ != nullptr && src_col >= (int) shared_payload_col_count_)
		return;

	if (use_parallel_ht_)
	{
		row_ordinal = chunk_idx;
		if (partition_id < VOLVEC_RADIX_FANOUT)
		{
			const uint8_t *row_ptr = get_partition_row_ptr(partition_id, row_ordinal);
			if (row_ptr != nullptr)
			{
				uint32_t payload_offset = row_layout_.payload_offset;
				dst.nulls[dst_col][dst_row] = row_ptr[row_layout_.nulls_offset + key_cols_.size() + src_col];
				for (int payload_idx = 0; payload_idx < src_col; payload_idx++)
				{
					const VecHashPayloadCol &payload_col = inner_payload_cols_[payload_idx];
					switch (payload_col.meta.storage_kind)
					{
						case VecOutputStorageKind::Int32:
							payload_offset = VolvecAlignU32(payload_offset, 4) + sizeof(int32_t);
							break;
						case VecOutputStorageKind::Int64:
						case VecOutputStorageKind::NumericScaledInt64:
						case VecOutputStorageKind::NumericAvgPair:
						case VecOutputStorageKind::Double:
							payload_offset = VolvecAlignU32(payload_offset, 8) + sizeof(uint64_t);
							break;
						case VecOutputStorageKind::StringRef:
							payload_offset = VolvecAlignU32(payload_offset, 8) + sizeof(ParallelHashRowVarlenRef);
							break;
					}
				}
				if (dst.nulls[dst_col][dst_row])
					return;
				switch (meta.storage_kind)
				{
					case VecOutputStorageKind::Int32:
						payload_offset = VolvecAlignU32(payload_offset, 4);
						dst.int32_columns[dst_col][dst_row] =
							*reinterpret_cast<const int32_t *>(row_ptr + payload_offset);
						return;
					case VecOutputStorageKind::Int64:
					case VecOutputStorageKind::NumericScaledInt64:
					case VecOutputStorageKind::NumericAvgPair:
						payload_offset = VolvecAlignU32(payload_offset, 8);
						dst.int64_columns[dst_col][dst_row] =
							*reinterpret_cast<const int64_t *>(row_ptr + payload_offset);
						return;
					case VecOutputStorageKind::Double:
						payload_offset = VolvecAlignU32(payload_offset, 8);
						dst.double_columns[dst_col][dst_row] =
							*reinterpret_cast<const double *>(row_ptr + payload_offset);
						return;
					case VecOutputStorageKind::StringRef:
					{
						const ParallelHashRowVarlenRef *ref = GetRowVarlenRefAt(row_ptr, &payload_offset);
						if (ref->length == 0)
						{
							dst.string_columns[dst_col][dst_row] = VecStringRef{};
							return;
						}
						if (ref->offset == VOLVEC_ROW_VARLEN_INLINE_OFFSET)
						{
							const char *inline_ptr = reinterpret_cast<const char *>(&ref->prefix);
							dst.string_columns[dst_col][dst_row] = dst.store_string_bytes(inline_ptr, ref->length);
							return;
						}
						const char *ptr = partition_row_stores_[partition_id].varlen_arena.data() + ref->offset;
						dst.string_columns[dst_col][dst_row] = dst.store_string_bytes(ptr, ref->length);
						return;
					}
				}
			}
		}
	}
	if (chunk_idx < inner_chunks_.size())
	{
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
		return;
	}

	if (shared_hash_chunks_ == nullptr ||
		shared_hash_bridge_buffer_ == nullptr ||
		chunk_idx >= shared_hash_chunk_count_)
		return;

	chunk_header = &shared_hash_chunks_[chunk_idx];
	if (row_idx >= chunk_header->row_count)
		return;
	base = shared_hash_bridge_buffer_;
	dst.nulls[dst_col][dst_row] = *(base + chunk_header->nulls_offsets[src_col] + row_idx);
	if (dst.nulls[dst_col][dst_row])
		return;

	switch (meta.storage_kind)
	{
		case VecOutputStorageKind::Double:
			dst.double_columns[dst_col][dst_row] =
				*(reinterpret_cast<const double *>(base + chunk_header->value_offsets[src_col]) + row_idx);
			break;
		case VecOutputStorageKind::Int64:
		case VecOutputStorageKind::NumericScaledInt64:
		case VecOutputStorageKind::NumericAvgPair:
			dst.int64_columns[dst_col][dst_row] =
				*(reinterpret_cast<const int64_t *>(base + chunk_header->value_offsets[src_col]) + row_idx);
			dst.double_columns[dst_col][dst_row] =
				*(reinterpret_cast<const double *>(base + chunk_header->aux_value_offsets[src_col]) + row_idx);
			break;
		case VecOutputStorageKind::StringRef:
		{
			const VecStringRef &ref =
				*(reinterpret_cast<const VecStringRef *>(base + chunk_header->value_offsets[src_col]) + row_idx);
			const char *arena_base = chunk_header->string_arena_size == 0 ? nullptr :
				reinterpret_cast<const char *>(base + chunk_header->string_arena_offset);
			const char *ptr = VecStringRefDataPtr(ref, arena_base);

			dst.string_columns[dst_col][dst_row] = dst.store_string_bytes(ptr, ref.len);
			break;
		}
		case VecOutputStorageKind::Int32:
			dst.int32_columns[dst_col][dst_row] =
				*(reinterpret_cast<const int32_t *>(base + chunk_header->value_offsets[src_col]) + row_idx);
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
	inner_payload_cols_.clear();
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
	size_t row_size = compute_row_layout_fragment_size();
	buffer.resize(size + row_size);
	serialize_hash_build_fragment(buffer.data(), size);
	serialize_row_layout_fragment(buffer.data() + size, row_size);
	if (!BufFileWriteAllLocal(file, buffer.data(), buffer.size()))
		return false;
	memset(out, 0, sizeof(*out));
	out->entry_count = entries_.size();
	out->chunk_count = inner_chunks_.size();
	out->row_count = total_build_rows_;
	out->file_bytes = size + row_size;
	out->row_file_bytes = row_size;
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
	size_t legacy_size = partial.row_file_bytes > 0 && partial.row_file_bytes <= partial.file_bytes ?
		(size_t) (partial.file_bytes - partial.row_file_bytes) : buffer.size();
	append_hash_build_fragment(buffer.data(), legacy_size);
	if (partial.row_file_bytes > 0)
		append_row_layout_fragment(buffer.data() + legacy_size, (size_t) partial.row_file_bytes);
	return true;
}

bool
VecHashJoinState::export_parallel_build_partial_dsa(struct dsa_area *dsa,
													ParallelHashBuildPartialState *out) const
{
	if (dsa == nullptr || out == nullptr)
		return false;

	size_t size = compute_hash_build_fragment_size();
	size_t row_size = compute_row_layout_fragment_size();
	dsa_pointer ptr = dsa_allocate_extended(dsa, size + row_size, DSA_ALLOC_NO_OOM);
	if (!DsaPointerIsValid(ptr))
		return false;

	uint8_t *buffer = (uint8_t *) dsa_get_address(dsa, ptr);
	serialize_hash_build_fragment(buffer, size);
	serialize_row_layout_fragment(buffer + size, row_size);

	memset(out, 0, sizeof(*out));
	out->entry_count = entries_.size();
	out->chunk_count = inner_chunks_.size();
	out->row_count = total_build_rows_;
	out->file_bytes = size + row_size;
	out->row_file_bytes = row_size;
	out->dsa_pack = (uint64_t) ptr;
	return true;
}

bool
VecHashJoinState::merge_parallel_build_partial_dsa(struct dsa_area *dsa,
												   const ParallelHashBuildPartialState &partial)
{
	if (dsa == nullptr || partial.dsa_pack == 0 || partial.file_bytes == 0)
		return false;

	const uint8_t *buffer = (const uint8_t *) dsa_get_address(dsa, (dsa_pointer) partial.dsa_pack);
	size_t legacy_size = partial.row_file_bytes > 0 && partial.row_file_bytes <= partial.file_bytes ?
		(size_t) (partial.file_bytes - partial.row_file_bytes) : (size_t) partial.file_bytes;
	append_hash_build_fragment(buffer, legacy_size);
	if (partial.row_file_bytes > 0)
		append_row_layout_fragment(buffer + legacy_size, (size_t) partial.row_file_bytes);
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
	inner_payload_cols_.clear();
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
	size = compute_shared_hash_bridge_size();
	shared_hash_bridge_buffer_ = (uint8_t *) MemoryContextAlloc(memory_context_, size);
	write_shared_hash_bridge(shared_hash_bridge_buffer_, size);
	shared_hash_bridge_buffer_size_ = size;
	shared_hash_bridge_buffer_owned_ = true;
	shared_hash_bridge_ = reinterpret_cast<ParallelHashBuildState *>(shared_hash_bridge_buffer_);
	shared_hash_chunks_ = shared_hash_bridge_->num_chunks == 0 ? nullptr :
		reinterpret_cast<const ParallelHashBuildChunk *>(shared_hash_bridge_buffer_ +
													 shared_hash_bridge_->chunks_offset);
	shared_hash_chunk_count_ = (uint32) inner_chunks_.size();
}

void
VecHashJoinState::load_hash_bridge()
{
	if (shared_hash_bridge_buffer_ == nullptr || shared_hash_bridge_buffer_size_ == 0)
		return;
	materialize_shared_hash_build_view();
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

void
VecHashJoinState::attach_shared_finalized_hash_bridge(const uint8_t *buffer, size_t buffer_size)
{
	attach_shared_hash_bridge(buffer, buffer_size);
	materialize_shared_hash_build_view();
}

} /* namespace pg_volvec */

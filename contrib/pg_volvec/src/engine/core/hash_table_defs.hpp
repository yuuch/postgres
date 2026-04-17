#pragma once

#include "core/types.hpp"
#include "core/memory.hpp"

struct VecHashPartition {
	uint64_t *keys;       /* join key values */
	uint64_t *payloads;   /* payload columns compact storage */
	uint32_t  count;
	uint32_t  capacity;
	bool      is_external; /* data in DSM (not local memory) */
};

struct VecHashPartitionTable {
	VecHashPartition partitions[VOLVEC_RADIX_FANOUT];
	uint32_t partition_counts[VOLVEC_RADIX_FANOUT];  /* histogram / running offsets */

	VecHashPartitionTable() {
		memset(this, 0, sizeof(*this));
	}
};

struct VecLinearProbeHT {
	uint64_t *keys;      /* key array, size = capacity */
	uint64_t *payloads;  /* payload data, compact */
	uint32_t *values;    /* index into payloads */
	uint8_t  *states;    /* 0=empty, 1=occupied */
	uint32_t capacity;
	uint32_t count;

	VecLinearProbeHT() : keys(nullptr), payloads(nullptr), values(nullptr), states(nullptr), capacity(0), count(0) {}
};

struct VecBloomFilter {
	uint64_t *bits;
	uint32_t bit_count;
	uint32_t hash_funcs;
	uint64_t seeds[4];

	VecBloomFilter() : bits(nullptr), bit_count(0), hash_funcs(0) { memset(seeds, 0, sizeof(seeds)); }
};

/* Serialized partition metadata for the shared hash bridge. */
struct ParallelHashBuildPartition {
	uint64_t bucket_heads_offset;
	uint64_t bucket_heads_size;
	uint32_t bucket_count;
	uint32_t bucket_mask;
	uint32_t entry_count;
	uint32_t reserved;
};

/* Serialized build payload chunk metadata for the shared hash bridge. */
struct ParallelHashBuildChunk {
	uint32_t row_count;
	uint32_t string_arena_size;
	uint64_t string_arena_offset;
	uint64_t value_offsets[16];
	uint64_t aux_value_offsets[16];
	uint64_t nulls_offsets[16];
};

/* Serialized header for the shared hash bridge. */
struct ParallelHashBuildState {
	uint32_t num_partitions;
	uint32_t total_entries;
	uint32_t num_chunks;
	uint32_t num_payload_cols;
	uint64_t entries_offset;
	uint64_t entries_size;
	uint64_t chunks_offset;
	uint64_t chunks_size;
	uint8_t build_complete;
	uint8_t reserved[7];
};

/* Serialized header for a worker-local hash build fragment. */
struct ParallelHashBuildFragmentState {
	uint32_t total_entries;
	uint32_t num_chunks;
	uint32_t num_payload_cols;
	uint32_t reserved;
	uint64_t entries_offset;
	uint64_t entries_size;
	uint64_t chunks_offset;
	uint64_t chunks_size;
};


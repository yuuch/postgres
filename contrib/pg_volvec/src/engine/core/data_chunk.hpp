#pragma once

#include "core/types.hpp"
#include "core/memory.hpp"

struct SelectionVector { uint16_t row_ids[DEFAULT_CHUNK_SIZE]; uint16_t count; void clear() { count = 0; } };
struct VecStringRef { uint32_t len; uint32_t offset; uint64_t prefix; };
static constexpr uint32_t kVecStringInlineOffset = UINT32_MAX;

static inline bool
VecStringRefIsInline(const VecStringRef &ref)
{
	return ref.len > 0 && ref.len <= 8 && ref.offset == kVecStringInlineOffset;
}

static inline const char *
VecStringRefDataPtr(const VecStringRef &ref, const char *arena_base)
{
	if (ref.len == 0)
		return "";
	if (VecStringRefIsInline(ref))
		return reinterpret_cast<const char *>(&ref.prefix);
	if (arena_base == nullptr)
		return nullptr;
	return arena_base + ref.offset;
}

enum class VecOutputStorageKind : uint8_t {
	Int32,
	Int64,
	Double,
	StringRef,
	NumericScaledInt64,
	NumericAvgPair
};

struct VecOutputColMeta {
	Oid sql_type = InvalidOid;
	VecOutputStorageKind storage_kind = VecOutputStorageKind::Int32;
	int scale = 0;
};

template <uint16_t Capacity>
struct alignas(16) DataChunk {
	static void *operator new(std::size_t size)
	{
		return MemoryContextAllocAligned(CurrentMemoryContext, size, alignof(DataChunk), 0);
	}

	static void operator delete(void *ptr) noexcept
	{
		if (ptr != nullptr)
			pfree(ptr);
	}

	static void operator delete(void *ptr, std::size_t) noexcept
	{
		if (ptr != nullptr)
			pfree(ptr);
	}

	uint16_t count;
	alignas(16) double double_columns[16][Capacity];
	alignas(16) int64_t int64_columns[16][Capacity];
	alignas(16) int32_t int32_columns[16][Capacity];
	alignas(16) VecStringRef string_columns[16][Capacity];
	alignas(16) uint8_t nulls[16][Capacity]; /* Use uint8_t for reliability */
	SelectionVector sel;
	bool has_selection;
	VolVecVector<char> string_arena;

	DataChunk()
		: count(0),
		  has_selection(false),
		  string_arena(PgMemoryContextAllocator<char>(CurrentMemoryContext))
	{
		memset(nulls, 0, sizeof(nulls));
	}

	void reset() { count = 0; sel.clear(); has_selection = false; memset(nulls, 0, sizeof(nulls)); string_arena.clear(); }
	VecStringRef store_string_bytes(const char *data, uint32_t len)
	{
		VecStringRef ref{len, 0, 0};

		if (len == 0 || data == nullptr)
			return ref;
		if (len > 65536)
			elog(ERROR, "pg_volvec suspicious string length %u while materializing DataChunk", len);
		memcpy(&ref.prefix, data, len > 8 ? 8 : len);
		if (len <= 8)
		{
			ref.offset = kVecStringInlineOffset;
			return ref;
		}
		ref.offset = (uint32_t) string_arena.size();
		string_arena.insert(string_arena.end(), data, data + len);
		return ref;
	}
	const char *get_string_ptr(const VecStringRef &ref) const
	{
		return VecStringRefDataPtr(ref, string_arena.data());
	}
	void get_double_ptrs(double** out) { for(int i=0; i<16; i++) out[i] = double_columns[i]; }
	void get_int64_ptrs(int64_t** out) { for(int i=0; i<16; i++) out[i] = int64_columns[i]; }
	void get_int32_ptrs(int32_t** out) { for(int i=0; i<16; i++) out[i] = int32_columns[i]; }
	void get_string_ptrs(VecStringRef** out) { for(int i=0; i<16; i++) out[i] = string_columns[i]; }
	void get_null_ptrs(uint8_t** out) { for(int i=0; i<16; i++) out[i] = nulls[i]; }
};

static constexpr int kMaxDeformTargets = 16;
enum class DeformDecodeKind : uint8_t { kInt32, kInt64, kDate32, kFloat8, kNumeric, kStringRef };
struct DeformTarget { int att_index; uint16_t dst_col; DeformDecodeKind decode_kind; };
struct DeformProgram {
	int ntargets; int last_att_index; DeformTarget targets[kMaxDeformTargets];
	void reset() { ntargets = 0; last_att_index = -1; }
	void add_target(int att, int dst, DeformDecodeKind k) { if(ntargets<kMaxDeformTargets) targets[ntargets++] = {att, (uint16_t)dst, k}; }
	void finalize() {
		for (int i = 1; i < ntargets; i++) {
			DeformTarget key = targets[i];
			int j = i - 1;
			while (j >= 0 && targets[j].att_index > key.att_index) {
				targets[j + 1] = targets[j];
				j--;
			}
			targets[j + 1] = key;
		}
		last_att_index = (ntargets > 0) ? targets[ntargets - 1].att_index : -1;
	}
};
struct DeformBindings { void *columns_data[kMaxDeformTargets]; uint8_t *columns_nulls[kMaxDeformTargets]; int ncolumns; DataChunk<DEFAULT_CHUNK_SIZE> *owner_chunk; };
typedef void (*JitDeformFunc)(HeapTupleHeader tuphdr, void **col_data_ptrs, uint8_t **col_null_ptrs, uint32 row_idx, DataChunk<DEFAULT_CHUNK_SIZE> *owner_chunk);


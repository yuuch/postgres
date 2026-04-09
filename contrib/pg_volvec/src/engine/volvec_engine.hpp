#ifndef VOLVEC_ENGINE_HPP
#define VOLVEC_ENGINE_HPP

#include <memory>
#include <limits>
#include <new>
#include <utility>
#include <vector>
#include <unordered_map>
#include <cstdint>
#include <cstring>

extern "C" {
#include "postgres.h"
#include "utils/rel.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/tupdesc_details.h"
#include "nodes/plannodes.h"
#include "executor/executor.h"
#include "jit/jit.h"
#include "storage/bufmgr.h"
#include "storage/read_stream.h"
#include "catalog/pg_type_d.h"
#include "nodes/makefuncs.h"
#include "utils/builtins.h"
#include "utils/numeric.h"
#include "access/visibilitymap.h"
#include "optimizer/optimizer.h"
}

namespace pg_volvec
{

static constexpr uint16_t DEFAULT_CHUNK_SIZE = 1024;
static constexpr int MAX_REGISTERS = 64;
static constexpr int DEFAULT_NUMERIC_SCALE = 2;
static constexpr int VOLVEC_DEC_DIGITS = 4;
static constexpr int VOLVEC_NBASE = 10000;

#if defined(__SIZEOF_INT128__)
using NumericWideInt = __int128_t;

static inline NumericWideInt
MakeWideIntBits(uint64_t lo, uint64_t hi)
{
	return (((NumericWideInt) ((__int128_t) ((int64_t) hi))) << 64) |
		(NumericWideInt) lo;
}

static inline uint64_t
WideIntLow64(NumericWideInt value)
{
	return (uint64_t) value;
}

static inline int64_t
WideIntHigh64(NumericWideInt value)
{
	return (int64_t) (((__int128_t) value) >> 64);
}
#else
struct NumericWideInt
{
	uint64_t	lo;
	int64_t		hi;

	constexpr NumericWideInt() : lo(0), hi(0) {}
	constexpr NumericWideInt(int64_t value)
		: lo((uint64_t) value), hi(value < 0 ? -1 : 0) {}
	constexpr NumericWideInt(uint64_t lo_bits, int64_t hi_bits)
		: lo(lo_bits), hi(hi_bits) {}
};

static inline NumericWideInt
MakeWideIntBits(uint64_t lo, uint64_t hi)
{
	return NumericWideInt(lo, (int64_t) hi);
}

static inline uint64_t
WideIntLow64(NumericWideInt value)
{
	return value.lo;
}

static inline int64_t
WideIntHigh64(NumericWideInt value)
{
	return value.hi;
}

static inline void
Mul64Wide(uint64_t left, uint64_t right, uint64_t *hi, uint64_t *lo)
{
	uint64_t left_lo = (uint32_t) left;
	uint64_t left_hi = left >> 32;
	uint64_t right_lo = (uint32_t) right;
	uint64_t right_hi = right >> 32;
	uint64_t prod_ll = left_lo * right_lo;
	uint64_t prod_lh = left_lo * right_hi;
	uint64_t prod_hl = left_hi * right_lo;
	uint64_t prod_hh = left_hi * right_hi;
	uint64_t middle = (prod_ll >> 32) + (uint32_t) prod_lh + (uint32_t) prod_hl;

	*lo = (middle << 32) | (uint32_t) prod_ll;
	*hi = prod_hh + (prod_lh >> 32) + (prod_hl >> 32) + (middle >> 32);
}

static inline bool
operator==(const NumericWideInt &left, const NumericWideInt &right)
{
	return left.hi == right.hi && left.lo == right.lo;
}

static inline bool
operator!=(const NumericWideInt &left, const NumericWideInt &right)
{
	return !(left == right);
}

static inline bool
operator<(const NumericWideInt &left, const NumericWideInt &right)
{
	if (left.hi != right.hi)
		return left.hi < right.hi;
	return left.lo < right.lo;
}

static inline bool
operator<=(const NumericWideInt &left, const NumericWideInt &right)
{
	return !(right < left);
}

static inline bool
operator>(const NumericWideInt &left, const NumericWideInt &right)
{
	return right < left;
}

static inline bool
operator>=(const NumericWideInt &left, const NumericWideInt &right)
{
	return !(left < right);
}

static inline NumericWideInt
operator-(const NumericWideInt &value)
{
	uint64_t lo = ~value.lo + 1;
	uint64_t hi = ~((uint64_t) value.hi) + (lo == 0 ? 1 : 0);

	return MakeWideIntBits(lo, hi);
}

static inline NumericWideInt
operator+(const NumericWideInt &left, const NumericWideInt &right)
{
	uint64_t lo = left.lo + right.lo;
	uint64_t carry = (lo < left.lo) ? 1 : 0;
	uint64_t hi = (uint64_t) left.hi + (uint64_t) right.hi + carry;

	return MakeWideIntBits(lo, hi);
}

static inline NumericWideInt
operator-(const NumericWideInt &left, const NumericWideInt &right)
{
	uint64_t lo = left.lo - right.lo;
	uint64_t borrow = (left.lo < right.lo) ? 1 : 0;
	uint64_t hi = (uint64_t) left.hi - (uint64_t) right.hi - borrow;

	return MakeWideIntBits(lo, hi);
}

static inline NumericWideInt
operator*(const NumericWideInt &left, const NumericWideInt &right)
{
	uint64_t prod_hi = 0;
	uint64_t prod_lo = 0;
	uint64_t cross1_hi = 0;
	uint64_t cross1_lo = 0;
	uint64_t cross2_hi = 0;
	uint64_t cross2_lo = 0;
	uint64_t hi;

	Mul64Wide(left.lo, right.lo, &prod_hi, &prod_lo);
	Mul64Wide(left.lo, (uint64_t) right.hi, &cross1_hi, &cross1_lo);
	Mul64Wide((uint64_t) left.hi, right.lo, &cross2_hi, &cross2_lo);
	hi = prod_hi + cross1_lo + cross2_lo;
	return MakeWideIntBits(prod_lo, hi);
}

static inline NumericWideInt &
operator+=(NumericWideInt &left, const NumericWideInt &right)
{
	left = left + right;
	return left;
}

static inline NumericWideInt &
operator-=(NumericWideInt &left, const NumericWideInt &right)
{
	left = left - right;
	return left;
}

static inline NumericWideInt &
operator*=(NumericWideInt &left, const NumericWideInt &right)
{
	left = left * right;
	return left;
}
#endif

static inline NumericWideInt
WideIntFromInt64(int64_t value)
{
	return NumericWideInt(value);
}

static inline bool
WideIntFitsInt64(NumericWideInt value)
{
	return value >= WideIntFromInt64(PG_INT64_MIN) &&
		   value <= WideIntFromInt64(PG_INT64_MAX);
}

static inline int64_t
WideIntToInt64Checked(NumericWideInt value, const char *what)
{
	if (!WideIntFitsInt64(value))
		elog(ERROR, "pg_volvec %s exceeds int64 range", what);
#if defined(__SIZEOF_INT128__)
	return (int64_t) value;
#else
	return (int64_t) value.lo;
#endif
}

static inline NumericWideInt
WideIntMul(NumericWideInt left, NumericWideInt right)
{
	return left * right;
}

static inline NumericWideInt
RescaleWideIntUp(NumericWideInt value, int delta_scale)
{
	static const int64_t kPowers[] = {
		INT64CONST(1),
		INT64CONST(10),
		INT64CONST(100),
		INT64CONST(1000),
		INT64CONST(10000),
		INT64CONST(100000),
		INT64CONST(1000000),
		INT64CONST(10000000),
		INT64CONST(100000000),
		INT64CONST(1000000000),
		INT64CONST(10000000000),
		INT64CONST(100000000000),
		INT64CONST(1000000000000),
		INT64CONST(10000000000000),
		INT64CONST(100000000000000),
		INT64CONST(1000000000000000),
		INT64CONST(10000000000000000),
		INT64CONST(100000000000000000),
		INT64CONST(1000000000000000000)
	};

	if (delta_scale <= 0)
		return value;
	if (delta_scale >= (int) lengthof(kPowers))
		elog(ERROR, "pg_volvec rescale delta %d exceeds supported range", delta_scale);
	return WideIntMul(value, WideIntFromInt64(kPowers[delta_scale]));
}

static inline int
GetNumericScaleFromTypmod(int32 typmod)
{
	if (typmod < (int32) VARHDRSZ)
		return DEFAULT_NUMERIC_SCALE;
	return ((((typmod - VARHDRSZ) & 0x7ff) ^ 1024) - 1024);
}

struct VolVecNumericShort
{
	uint16		n_header;
	int16		n_data[FLEXIBLE_ARRAY_MEMBER];
};

struct VolVecNumericLong
{
	uint16		n_sign_dscale;
	int16		n_weight;
	int16		n_data[FLEXIBLE_ARRAY_MEMBER];
};

static inline bool
VolVecNumericHeaderIsShort(const void *datum_ptr)
{
	const VolVecNumericShort *num =
		reinterpret_cast<const VolVecNumericShort *>(VARDATA_ANY((const struct varlena *) datum_ptr));
	return (num->n_header & 0x8000) != 0;
}

static inline bool
VolVecNumericIsSpecial(const void *datum_ptr)
{
	const VolVecNumericShort *num =
		reinterpret_cast<const VolVecNumericShort *>(VARDATA_ANY((const struct varlena *) datum_ptr));
	return (num->n_header & 0xC000) == 0xC000;
}

static inline int
VolVecNumericDscale(const void *datum_ptr)
{
	if (VolVecNumericHeaderIsShort(datum_ptr))
	{
		const VolVecNumericShort *num =
			reinterpret_cast<const VolVecNumericShort *>(VARDATA_ANY((const struct varlena *) datum_ptr));
		return (num->n_header & 0x1F80) >> 7;
	}

	const VolVecNumericLong *num =
		reinterpret_cast<const VolVecNumericLong *>(VARDATA_ANY((const struct varlena *) datum_ptr));
	return (num->n_sign_dscale & 0x3FFF);
}

static inline int
VolVecNumericWeight(const void *datum_ptr)
{
	if (VolVecNumericHeaderIsShort(datum_ptr))
	{
		const VolVecNumericShort *num =
			reinterpret_cast<const VolVecNumericShort *>(VARDATA_ANY((const struct varlena *) datum_ptr));
		int weight = num->n_header & 0x003F;
		if ((num->n_header & 0x0040) != 0)
			weight |= ~0x003F;
		return weight;
	}

	const VolVecNumericLong *num =
		reinterpret_cast<const VolVecNumericLong *>(VARDATA_ANY((const struct varlena *) datum_ptr));
	return num->n_weight;
}

static inline bool
VolVecNumericNegative(const void *datum_ptr)
{
	if (VolVecNumericHeaderIsShort(datum_ptr))
	{
		const VolVecNumericShort *num =
			reinterpret_cast<const VolVecNumericShort *>(VARDATA_ANY((const struct varlena *) datum_ptr));
		return (num->n_header & 0x2000) != 0;
	}

	const VolVecNumericLong *num =
		reinterpret_cast<const VolVecNumericLong *>(VARDATA_ANY((const struct varlena *) datum_ptr));
	return (num->n_sign_dscale & 0xC000) == 0x4000;
}

static inline int
VolVecNumericHeaderSize(const void *datum_ptr)
{
	return sizeof(uint16) + (VolVecNumericHeaderIsShort(datum_ptr) ? 0 : sizeof(int16));
}

static inline const int16 *
VolVecNumericDigits(const void *datum_ptr)
{
	if (VolVecNumericHeaderIsShort(datum_ptr))
	{
		const VolVecNumericShort *num =
			reinterpret_cast<const VolVecNumericShort *>(VARDATA_ANY((const struct varlena *) datum_ptr));
		return num->n_data;
	}

	const VolVecNumericLong *num =
		reinterpret_cast<const VolVecNumericLong *>(VARDATA_ANY((const struct varlena *) datum_ptr));
	return num->n_data;
}

static inline int
VolVecNumericNDigits(const void *datum_ptr)
{
	return (VARSIZE_ANY_EXHDR((const struct varlena *) datum_ptr) - VolVecNumericHeaderSize(datum_ptr)) / (int) sizeof(int16);
}

static inline bool
TryFastNumericToScaledInt64(Datum value, int target_scale, int64_t *out)
{
	const void *datum_ptr = DatumGetPointer(value);
	const int16 *digits;
	NumericWideInt accum = 0;
	int ndigits;
	int weight;
	int dscale;
	int frac_index;
	int i;

	if (out == nullptr || datum_ptr == nullptr)
		return false;
	if (VolVecNumericIsSpecial(datum_ptr))
		return false;
	if (target_scale < 0 || target_scale > VOLVEC_DEC_DIGITS)
		return false;

	dscale = VolVecNumericDscale(datum_ptr);
	if (dscale < 0 || dscale > VOLVEC_DEC_DIGITS || dscale > target_scale)
		return false;

	digits = VolVecNumericDigits(datum_ptr);
	ndigits = VolVecNumericNDigits(datum_ptr);
	weight = VolVecNumericWeight(datum_ptr);

	for (i = 0; i <= weight; i++)
	{
		accum *= VOLVEC_NBASE;
		if (i >= 0 && i < ndigits)
			accum += digits[i];
	}

	for (i = 0; i < target_scale; i++)
		accum *= 10;

	frac_index = weight + 1;
	if (target_scale > 0)
	{
		int16 frac_digit = (frac_index >= 0 && frac_index < ndigits) ? digits[frac_index] : 0;
		int divisor = 1;

		for (i = 0; i < VOLVEC_DEC_DIGITS - target_scale; i++)
			divisor *= 10;
		if (frac_digit % divisor != 0)
			return false;
		accum += frac_digit / divisor;
	}

	for (i = Max(frac_index + 1, 0); i < ndigits; i++)
	{
		if (digits[i] != 0)
			return false;
	}

	if (VolVecNumericNegative(datum_ptr))
		accum = -accum;

	if (!WideIntFitsInt64(accum))
		return false;

	*out = (int64_t) accum;
	return true;
}

static inline bool
TryFastNumericToScaledWideInt(Datum value, int target_scale, NumericWideInt *out)
{
	const void *datum_ptr = DatumGetPointer(value);
	const int16 *digits;
	NumericWideInt accum = 0;
	int ndigits;
	int weight;
	int remaining_scale;
	int processed_groups;
	int i;
	NumericWideInt scale_factor = 1;

	if (out == nullptr || datum_ptr == nullptr)
		return false;
	if (VolVecNumericIsSpecial(datum_ptr))
		return false;
	if (target_scale < 0 || target_scale > 18)
		return false;

	digits = VolVecNumericDigits(datum_ptr);
	ndigits = VolVecNumericNDigits(datum_ptr);
	weight = VolVecNumericWeight(datum_ptr);

	for (i = 0; i < target_scale; i++)
		scale_factor *= WideIntFromInt64(10);

	for (i = 0; i <= weight; i++)
	{
		int16 digit = 0;

		if (i >= 0 && i < ndigits)
			digit = digits[i];
		accum *= WideIntFromInt64(VOLVEC_NBASE);
		accum += WideIntFromInt64(digit);
	}

	accum *= scale_factor;
	remaining_scale = target_scale;
	processed_groups = (target_scale + VOLVEC_DEC_DIGITS - 1) / VOLVEC_DEC_DIGITS;

	for (i = 0; i < processed_groups; i++)
	{
		int group_exp = -1 - i;
		int digit_index = weight - group_exp;
		int16 digit = (digit_index >= 0 && digit_index < ndigits) ? digits[digit_index] : 0;
		int take = Min(remaining_scale, VOLVEC_DEC_DIGITS);
		int drop = VOLVEC_DEC_DIGITS - take;
		int64_t divisor = 1;
		int64_t contrib;

		for (int j = 0; j < drop; j++)
			divisor *= 10;
		if (digit % divisor != 0)
			return false;
		contrib = digit / divisor;
		for (int j = 0; j < remaining_scale - take; j++)
			contrib *= 10;
		accum += WideIntFromInt64(contrib);
		remaining_scale -= take;
	}

	for (i = Max(weight + 1 + processed_groups, 0); i < ndigits; i++)
	{
		if (digits[i] != 0)
			return false;
	}

	if (VolVecNumericNegative(datum_ptr))
		accum = -accum;

	*out = accum;
	return true;
}

class PgMemoryContextObject
{
public:
	static void *operator new(std::size_t size)
	{
		return MemoryContextAlloc(CurrentMemoryContext, size);
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
};

template <typename T>
class PgMemoryContextAllocator
{
public:
	using value_type = T;

	PgMemoryContextAllocator() noexcept : context_(CurrentMemoryContext) {}
	explicit PgMemoryContextAllocator(MemoryContext context) noexcept
		: context_(context != nullptr ? context : CurrentMemoryContext) {}

	template <typename U>
	PgMemoryContextAllocator(const PgMemoryContextAllocator<U> &other) noexcept
		: context_(other.context()) {}

	T *allocate(std::size_t n)
	{
		if (n > (std::numeric_limits<std::size_t>::max() / sizeof(T)))
			elog(ERROR, "pg_volvec allocator size overflow");
		return static_cast<T *>(MemoryContextAlloc(context_, n * sizeof(T)));
	}

	void deallocate(T *ptr, std::size_t) noexcept
	{
		if (ptr != nullptr)
			pfree(ptr);
	}

	MemoryContext context() const noexcept { return context_; }

	template <typename U>
	bool operator==(const PgMemoryContextAllocator<U> &other) const noexcept
	{
		return context_ == other.context();
	}

	template <typename U>
	bool operator!=(const PgMemoryContextAllocator<U> &other) const noexcept
	{
		return !(*this == other);
	}

private:
	template <typename>
	friend class PgMemoryContextAllocator;

	MemoryContext context_;
};

template <typename T>
using VolVecVector = std::vector<T, PgMemoryContextAllocator<T>>;

template <typename Key, typename Value, typename Hash = std::hash<Key>, typename KeyEqual = std::equal_to<Key>>
using VolVecHashMap = std::unordered_map<Key, Value, Hash, KeyEqual,
	PgMemoryContextAllocator<std::pair<const Key, Value>>>;

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

class DataChunkDeformer {
public:
	DataChunkDeformer(TupleDesc desc, const DeformProgram *program) : desc_(desc), program_(*program) {}
	void set_jit_func(JitDeformFunc f) { jit_func_ = f; }
	void deform_tuple_header(HeapTupleHeader tuphdr, uint32 row_idx, const DeformBindings &bindings);
private:
	TupleDesc desc_; DeformProgram program_; JitDeformFunc jit_func_ = nullptr; bool jit_path_logged_ = false;
};

enum class VecOpCode {
	EEOP_VAR,
	EEOP_CONST,
	EEOP_FLOAT8_ADD,
	EEOP_FLOAT8_SUB,
	EEOP_FLOAT8_MUL,
	EEOP_INT64_ADD,
	EEOP_INT64_SUB,
	EEOP_INT64_MUL,
	EEOP_INT64_DIV_FLOAT8,
	EEOP_FLOAT8_LT,
	EEOP_FLOAT8_GT,
	EEOP_FLOAT8_LE,
	EEOP_FLOAT8_GE,
	EEOP_INT64_LT,
	EEOP_INT64_GT,
	EEOP_INT64_LE,
	EEOP_INT64_GE,
	EEOP_INT64_EQ,
	EEOP_INT64_NE,
	EEOP_DATE_LT,
	EEOP_DATE_LE,
	EEOP_DATE_GT,
	EEOP_DATE_GE,
	EEOP_DATE_PART_YEAR,
	EEOP_INT32_EQ,
	EEOP_AND,
	EEOP_OR,
	EEOP_NOT,
	EEOP_INT64_CASE,
	EEOP_FLOAT8_CASE,
	EEOP_STR_EQ,
	EEOP_STR_NE,
	EEOP_STR_PREFIX_LIKE,
	EEOP_STR_CONTAINS_LIKE,
	EEOP_STR_LIKE_PATTERN,
	EEOP_QUAL
};
struct VecExprStep {
	VecOpCode opcode;
	int res_idx;
	union {
		struct { int att_idx; Oid type; } var;
		struct {
			double fval;
			int64_t i64val;
			int32_t ival;
			bool isnull;
			bool has_wide_i128;
			uint64_t wide_lo;
			int64_t wide_hi;
		} constant;
		struct { int left; int right; } op;
		struct { int cond; int if_true; int if_false; } ternary;
		struct { int att_idx; uint32_t len; uint32_t offset; uint64_t prefix; Oid type; } str_prefix;
	} d;
	};
typedef void (*VecExprJitFunc)(uint32_t count, double** col_f8, int64_t** col_i64, int32_t** col_i32, VecStringRef** col_str, uint8_t** col_nulls, const char *string_arena_base, double* res_f8, int64_t* res_i64, int32_t* res_i32, uint8_t* res_nulls, uint16_t* sel, bool has_sel);

class VecExprProgram : public PgMemoryContextObject {
public:
	VecExprProgram(); ~VecExprProgram();
	void evaluate(DataChunk<DEFAULT_CHUNK_SIZE> &chunk);
	void try_compile_jit();
	void clear_string_consts() { string_constants.clear(); }
	uint32_t store_string_const(const char *data, uint32_t len)
	{
		uint32_t offset;

		if (data == nullptr || len == 0)
			return UINT32_MAX;
		offset = (uint32_t) string_constants.size();
		string_constants.insert(string_constants.end(), data, data + len);
		return offset;
	}
	const char *get_string_const_ptr(uint32_t offset) const
	{
		if (offset == UINT32_MAX)
			return nullptr;
		return string_constants.data() + offset;
	}
	const double* get_float8_reg(int i) const { return &registers_f8[i * DEFAULT_CHUNK_SIZE]; }
	const int64_t* get_int64_reg(int i) const { return &registers_i64[i * DEFAULT_CHUNK_SIZE]; }
	const int32_t* get_int32_reg(int i) const { return &registers_i32[i * DEFAULT_CHUNK_SIZE]; }
	const uint8_t* get_nulls_reg(int i) const { return &registers_nulls[i * DEFAULT_CHUNK_SIZE]; }
	int get_register_scale(int i) const { return (i >= 0 && i < MAX_REGISTERS) ? register_scales[i] : 0; }
	void set_register_scale(int i, int scale) { if (i >= 0 && i < MAX_REGISTERS) register_scales[i] = scale; }
	void reset_register_scales() { memset(register_scales, 0, sizeof(register_scales)); }
	int get_final_res_idx() const { return final_res_idx; }
	VolVecVector<VecExprStep> steps; int max_reg_idx; int final_res_idx;
	VecExprJitFunc jit_func = nullptr; void* jit_context = nullptr;
private:
	int32_t* registers_i32; int64_t* registers_i64; double* registers_f8; uint8_t* registers_nulls;
	VolVecVector<char> string_constants;
	int register_scales[MAX_REGISTERS];
};

class VecPlanState : public PgMemoryContextObject {
public:
	virtual ~VecPlanState() = default;
	virtual bool get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk) = 0;
	virtual bool lookup_output_col_meta(int target_resno, VecOutputColMeta *out) const
	{
		return false;
	}
};

class VecSeqScanState : public VecPlanState {
public:
	VecSeqScanState(Relation rel, Snapshot snapshot, const DeformProgram *program); ~VecSeqScanState() override;
	bool get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk) override;
	bool lookup_output_col_meta(int target_resno, VecOutputColMeta *out) const override;
private:
	Relation rel_; Snapshot snapshot_; HeapScanDesc scan_; ReadStream *stream_ = nullptr; Buffer current_buf_ = InvalidBuffer; Buffer vmbuf_ = InvalidBuffer; OffsetNumber current_offnum_ = FirstOffsetNumber; bool all_visible_ = false; DataChunkDeformer deformer_; JitContext *jit_context_ = nullptr;
};

class VecAggState : public VecPlanState {
public:
	enum class NumericOutputKind { None, Sum, Avg };

	VecAggState(std::unique_ptr<VecPlanState> left, Agg *node);
	~VecAggState() override;
	bool get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk) override;
	bool lookup_numeric_output_meta(int target_resno, NumericOutputKind *kind, int *scale) const;
	bool lookup_output_col_meta(int target_resno, VecOutputColMeta *out) const override;
private:
		std::unique_ptr<VecPlanState> left_; Agg *node_; MemoryContext memory_context_; VolVecVector<int> grp_col_indices_;
		VolVecVector<VecOutputColMeta> grp_col_meta_;
	struct VecAggAccumulator {
		double float_sum = 0.0;
		NumericWideInt numeric_sum = 0;
		double float_max = 0.0;
		int64_t int64_max = 0;
		int32_t int32_max = 0;
		int64_t count = 0;
		bool has_value = false;

		void update_float(double v) { float_sum += v; count++; }
		void update_numeric(int64_t v) { numeric_sum += WideIntFromInt64(v); count++; }
		void update_max_float(double v)
		{
			if (!has_value || v > float_max)
				float_max = v;
			has_value = true;
		}
		void update_max_int64(int64_t v)
		{
			if (!has_value || v > int64_max)
				int64_max = v;
			has_value = true;
		}
		void update_max_int32(int32_t v)
		{
			if (!has_value || v > int32_max)
				int32_max = v;
			has_value = true;
		}

		using DistinctValueSet = VolVecHashMap<int64_t, char>;
		DistinctValueSet *distinct_values = nullptr;
	};
		struct VecGroupKey {
			uint64_t values[kMaxDeformTargets];
			uint32_t aux[kMaxDeformTargets];
			uint8_t is_null[kMaxDeformTargets];
			int num_cols;
			bool operator==(const VecGroupKey& o) const {
				if (num_cols != o.num_cols)
					return false;
				for (int i = 0; i < num_cols; i++)
				{
					if (is_null[i] != o.is_null[i])
						return false;
					if (!is_null[i] && (values[i] != o.values[i] || aux[i] != o.aux[i]))
						return false;
				}
				return true;
			}
		};
		struct VecGroupKeyHash {
			std::size_t operator()(const VecGroupKey& k) const {
				std::size_t h = 0;
				for (int i = 0; i < k.num_cols; i++)
				{
					h ^= std::hash<uint8_t>{}(k.is_null[i]) + 0x9e3779b9 + (h << 6) + (h >> 2);
					if (k.is_null[i])
						continue;
					h ^= std::hash<uint64_t>{}(k.values[i]) + 0x9e3779b9 + (h << 6) + (h >> 2);
					h ^= std::hash<uint32_t>{}(k.aux[i]) + 0x9e3779b9 + (h << 6) + (h >> 2);
				}
				return h;
			}
		};
		struct VecSimpleGroupKey {
			int64_t value = 0;
			uint8_t is_null = 0;
			bool operator==(const VecSimpleGroupKey& o) const {
				return value == o.value && is_null == o.is_null;
			}
		};
		struct VecSimpleGroupKeyHash {
			std::size_t operator()(const VecSimpleGroupKey& k) const {
				std::size_t h = std::hash<uint8_t>{}(k.is_null);
				if (!k.is_null)
					h ^= std::hash<int64_t>{}(k.value) + 0x9e3779b9 + (h << 6) + (h >> 2);
				return h;
			}
		};
	enum class VecAggType { SUM, COUNT, AVG, MAX };
		struct VecAggDesc {
			VecAggType type;
			std::unique_ptr<VecExprProgram> arg_expr;
			int target_resno;
			int group_key_pos = -1;
			int input_col = -1;
			Oid output_type = InvalidOid;
			VecOutputStorageKind output_storage = VecOutputStorageKind::Int32;
			Oid arg_type = InvalidOid;
			int numeric_scale = 0;
			bool use_exact_numeric = false;
			bool is_distinct = false;
		};
		using VecAggAccumulatorList = VolVecVector<VecAggAccumulator>;
		struct VecAggGroupState {
			VecAggAccumulatorList accs;
			uint32_t rep_chunk_idx = 0;
			uint16_t rep_row_idx = 0;
			bool has_rep_row = false;
		};
		using VecAggHashTable = VolVecHashMap<VecGroupKey, VecAggGroupState, VecGroupKeyHash>;
		using VecAggSimpleHashTable = VolVecHashMap<VecSimpleGroupKey, VecAggGroupState, VecSimpleGroupKeyHash>;
		DataChunk<DEFAULT_CHUNK_SIZE> *allocate_rep_chunk();
		void copy_rep_row(DataChunk<DEFAULT_CHUNK_SIZE> &dst, int dst_row,
						  const DataChunk<DEFAULT_CHUNK_SIZE> &src, int src_row) const;
		VolVecVector<VecAggDesc> aggs_; VecAggHashTable hash_table_;
		VecAggSimpleHashTable simple_hash_table_;
		VolVecVector<DataChunk<DEFAULT_CHUNK_SIZE> *> rep_chunks_;
		VecAggHashTable::iterator it_;
		VecAggSimpleHashTable::iterator simple_it_;
		bool use_simple_group_key_ = false;
		VecOutputStorageKind simple_group_storage_ = VecOutputStorageKind::Int32;
		bool fully_scanned_ = false; void do_sink();
	};

class VecFilterState : public VecPlanState {
public:
	VecFilterState(std::unique_ptr<VecPlanState> left, std::unique_ptr<VecExprProgram> prog) : left_(std::move(left)), program_(std::move(prog)) {}
	bool get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk) override;
	bool lookup_output_col_meta(int target_resno, VecOutputColMeta *out) const override
	{
		return left_ != nullptr && left_->lookup_output_col_meta(target_resno, out);
	}
private:
	std::unique_ptr<VecPlanState> left_; std::unique_ptr<VecExprProgram> program_;
};

struct VecLookupScalarKey {
	int64_t value = 0;
	uint8_t is_null = 0;

	bool operator==(const VecLookupScalarKey &other) const
	{
		return value == other.value && is_null == other.is_null;
	}
};

struct VecLookupScalarKeyHash {
	std::size_t operator()(const VecLookupScalarKey &key) const
	{
		std::size_t h = std::hash<uint8_t>{}(key.is_null);

		if (!key.is_null)
			h ^= std::hash<int64_t>{}(key.value) + 0x9e3779b9 + (h << 6) + (h >> 2);
		return h;
	}
};

struct VecLookupScalarValue {
	int32_t i32 = 0;
	int64_t i64 = 0;
	double f8 = 0.0;
	uint8_t is_null = 1;
};

using VecLookupScalarHashTable =
	VolVecHashMap<VecLookupScalarKey, VecLookupScalarValue, VecLookupScalarKeyHash>;

static constexpr int kMaxLookupKeys = 4;

struct VecLookupCompositeKey {
	int64_t values[kMaxLookupKeys] = {0, 0, 0, 0};
	uint8_t num_keys = 0;
	uint8_t is_null = 0;

	bool operator==(const VecLookupCompositeKey &other) const
	{
		if (num_keys != other.num_keys || is_null != other.is_null)
			return false;
		if (is_null)
			return true;
		for (int i = 0; i < num_keys; i++)
		{
			if (values[i] != other.values[i])
				return false;
		}
		return true;
	}
};

struct VecLookupCompositeKeyHash {
	std::size_t operator()(const VecLookupCompositeKey &key) const
	{
		std::size_t h = std::hash<uint8_t>{}(key.num_keys);

		h ^= std::hash<uint8_t>{}(key.is_null) + 0x9e3779b9 + (h << 6) + (h >> 2);
		if (key.is_null)
			return h;
		for (int i = 0; i < key.num_keys; i++)
			h ^= std::hash<int64_t>{}(key.values[i]) + 0x9e3779b9 + (h << 6) + (h >> 2);
		return h;
	}
};

using VecLookupCompositeHashTable =
	VolVecHashMap<VecLookupCompositeKey, VecLookupScalarValue, VecLookupCompositeKeyHash>;

class VecLookupProjectState : public VecPlanState {
public:
	VecLookupProjectState(std::unique_ptr<VecPlanState> left,
						  std::unique_ptr<VecPlanState> lookup_source,
						  uint16_t input_key_col,
						  VecOutputColMeta input_key_meta,
						  uint16_t lookup_key_col,
						  VecOutputColMeta lookup_key_meta,
						  uint16_t lookup_value_col,
						  int output_resno,
						  VecOutputColMeta output_meta);
	bool get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk) override;
	bool lookup_output_col_meta(int target_resno, VecOutputColMeta *out) const override;
private:
	bool build_lookup();
	bool extract_lookup_key(const DataChunk<DEFAULT_CHUNK_SIZE> &chunk,
							int row,
							uint16_t col,
							const VecOutputColMeta &meta,
							VecLookupScalarKey *key) const;

	std::unique_ptr<VecPlanState> left_;
	std::unique_ptr<VecPlanState> lookup_source_;
	MemoryContext memory_context_;
	VecLookupScalarHashTable lookup_table_;
	DataChunk<DEFAULT_CHUNK_SIZE> lookup_chunk_;
	uint16_t input_key_col_;
	VecOutputColMeta input_key_meta_;
	uint16_t lookup_key_col_;
	VecOutputColMeta lookup_key_meta_;
	uint16_t lookup_value_col_;
	int output_resno_;
	VecOutputColMeta output_meta_;
	bool lookup_built_;
};

class VecLookupProjectStateMultiKey : public VecPlanState {
public:
	VecLookupProjectStateMultiKey(std::unique_ptr<VecPlanState> left,
								  std::unique_ptr<VecPlanState> lookup_source,
								  int num_keys,
								  const uint16_t *input_key_cols,
								  const VecOutputColMeta *input_key_metas,
								  const uint16_t *lookup_key_cols,
								  const VecOutputColMeta *lookup_key_metas,
								  uint16_t lookup_value_col,
								  int output_resno,
								  VecOutputColMeta output_meta);
	bool get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk) override;
	bool lookup_output_col_meta(int target_resno, VecOutputColMeta *out) const override;
private:
	bool build_lookup();
	bool extract_lookup_key(const DataChunk<DEFAULT_CHUNK_SIZE> &chunk,
							int row,
							const uint16_t *cols,
							const VecOutputColMeta *metas,
							VecLookupCompositeKey *key) const;

	std::unique_ptr<VecPlanState> left_;
	std::unique_ptr<VecPlanState> lookup_source_;
	MemoryContext memory_context_;
	VecLookupCompositeHashTable lookup_table_;
	DataChunk<DEFAULT_CHUNK_SIZE> lookup_chunk_;
	int num_keys_;
	uint16_t input_key_cols_[kMaxLookupKeys];
	VecOutputColMeta input_key_metas_[kMaxLookupKeys];
	uint16_t lookup_key_cols_[kMaxLookupKeys];
	VecOutputColMeta lookup_key_metas_[kMaxLookupKeys];
	uint16_t lookup_value_col_;
	int output_resno_;
	VecOutputColMeta output_meta_;
	bool lookup_built_;
};

class VecLookupFilterState : public VecPlanState {
public:
	VecLookupFilterState(std::unique_ptr<VecPlanState> left,
						 std::unique_ptr<VecPlanState> lookup_source,
						 uint16_t input_key_col,
						 VecOutputColMeta input_key_meta,
						 uint16_t lookup_key_col,
						 VecOutputColMeta lookup_key_meta,
						 bool negate);
	bool get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk) override;
	bool lookup_output_col_meta(int target_resno, VecOutputColMeta *out) const override
	{
		return left_ != nullptr && left_->lookup_output_col_meta(target_resno, out);
	}
private:
	bool build_lookup();
	bool extract_lookup_key(const DataChunk<DEFAULT_CHUNK_SIZE> &chunk,
							int row,
							uint16_t col,
							const VecOutputColMeta &meta,
							VecLookupScalarKey *key) const;

	std::unique_ptr<VecPlanState> left_;
	std::unique_ptr<VecPlanState> lookup_source_;
	MemoryContext memory_context_;
	VecLookupScalarHashTable lookup_table_;
	DataChunk<DEFAULT_CHUNK_SIZE> lookup_chunk_;
	uint16_t input_key_col_;
	VecOutputColMeta input_key_meta_;
	uint16_t lookup_key_col_;
	VecOutputColMeta lookup_key_meta_;
	bool negate_;
	bool lookup_built_;
	bool lookup_has_null_;
};

struct VecProjectColDesc {
	std::unique_ptr<VecExprProgram> expr;
	int target_resno;
	Oid sql_type;
	VecOutputStorageKind storage_kind;
	int scale;
	bool direct_var = false;
	bool string_prefix_var = false;
	uint16_t input_col = 0;
	uint32_t string_prefix_len = 0;
};

class VecProjectState : public VecPlanState {
public:
	VecProjectState(std::unique_ptr<VecPlanState> left,
					VolVecVector<VecProjectColDesc> columns);
	bool get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk) override;
	bool lookup_output_col_meta(int target_resno, VecOutputColMeta *out) const override;
private:
	std::unique_ptr<VecPlanState> left_;
	VolVecVector<VecProjectColDesc> columns_;
	DataChunk<DEFAULT_CHUNK_SIZE> input_chunk_;
};

class VecLimitState : public VecPlanState {
public:
	VecLimitState(std::unique_ptr<VecPlanState> left, uint64_t limit_count)
		: left_(std::move(left)), limit_count_(limit_count), emitted_(0), done_(false) {}
	bool get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk) override;
	bool lookup_output_col_meta(int target_resno, VecOutputColMeta *out) const override
	{
		return left_ != nullptr && left_->lookup_output_col_meta(target_resno, out);
	}
private:
	std::unique_ptr<VecPlanState> left_;
	uint64_t limit_count_;
	uint64_t emitted_;
	bool done_;
};

enum class VecJoinSide : uint8_t {
	Outer,
	Inner
};

struct VecJoinOutputCol {
	VecJoinSide side;
	uint16_t input_col;
	int output_resno;
	VecOutputColMeta meta;
};

struct VecHashPayloadCol {
	uint16_t source_col;
	VecOutputColMeta meta;
};

static constexpr int kMaxJoinKeys = 4;

struct VecHashJoinKeyCol {
	uint16_t outer_col;
	uint16_t inner_col;
	VecOutputStorageKind kind;
	int scale;
};

struct VecHashJoinKey {
	uint64_t values[kMaxJoinKeys];
	uint8_t num_keys;
};

struct VecRowRef {
	uint32_t ordinal;
	uint32_t chunk_idx;
	uint16_t row_idx;
};

class VecHashJoinState : public VecPlanState {
public:
	VecHashJoinState(std::unique_ptr<VecPlanState> outer,
					 std::unique_ptr<VecPlanState> inner,
					 JoinType jointype,
					 bool build_outer_side,
					 int visible_output_count,
					 VolVecVector<VecJoinOutputCol> output_cols,
					 VolVecVector<VecHashJoinKeyCol> key_cols);
	~VecHashJoinState() override;
	bool get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk) override;
	bool lookup_output_col_meta(int target_resno, VecOutputColMeta *out) const override;
	void set_join_filter_program(std::unique_ptr<VecExprProgram> program);
	private:
		struct VecHashEntry {
			uint32_t hash;
			VecHashJoinKey key;
			int32_t next;
			uint32_t chunk_idx;
			uint16_t row_idx;
		};

		void build_inner_hash();
		void init_hash_table(size_t expected_rows);
		void rehash_hash_table(size_t min_bucket_count);
		void append_inner_entry(const VecHashJoinKey &key, uint32_t hash, uint32_t chunk_idx, uint16_t row_idx);
		uint16_t ensure_inner_payload_col(uint16_t source_col, const VecOutputColMeta &meta);
		DataChunk<DEFAULT_CHUNK_SIZE> *allocate_inner_chunk();
		void copy_inner_payload_row(DataChunk<DEFAULT_CHUNK_SIZE> &dst, int dst_row,
									const DataChunk<DEFAULT_CHUNK_SIZE> &src, int src_row) const;
		bool advance_outer_batch();
		void prepare_probe_batch();
		bool advance_probe_match(uint16_t probe_idx, int32_t *match_entry_idx);
		uint32_t hash_key(const VecHashJoinKey &key) const;
		bool read_key(const DataChunk<DEFAULT_CHUNK_SIZE> &chunk, bool inner_side, int row, VecHashJoinKey *key) const;
		bool keys_equal(const VecHashJoinKey &left, const VecHashJoinKey &right) const;
		bool candidate_passes_join_filter(const DataChunk<DEFAULT_CHUNK_SIZE> &outer_src, int outer_row,
										  const DataChunk<DEFAULT_CHUNK_SIZE> &inner_src, int inner_row);

		std::unique_ptr<VecPlanState> outer_;
		std::unique_ptr<VecPlanState> inner_;
		JoinType jointype_;
		int visible_output_count_;
		MemoryContext memory_context_;
		VolVecVector<VecJoinOutputCol> output_cols_;
		VolVecVector<VecHashJoinKeyCol> key_cols_;
		VolVecVector<VecHashPayloadCol> inner_payload_cols_;
		VolVecVector<DataChunk<DEFAULT_CHUNK_SIZE> *> inner_chunks_;
		VolVecVector<int32_t> bucket_heads_;
		VolVecVector<VecHashEntry> entries_;
		VolVecVector<uint8_t> inner_entry_matched_;
	DataChunk<DEFAULT_CHUNK_SIZE> outer_chunk_;
	VolVecVector<uint16_t> probe_rows_;
	VolVecVector<VecHashJoinKey> probe_keys_;
	VolVecVector<uint32_t> probe_hashes_;
	VolVecVector<int32_t> probe_next_entries_;
	VolVecVector<uint16_t> active_probe_sel_;
	VolVecVector<uint16_t> next_probe_sel_;
	bool inner_built_;
	bool probe_batch_ready_;
	bool probe_input_exhausted_;
	bool build_outer_side_;
	std::unique_ptr<VecExprProgram> join_filter_program_;
	DataChunk<DEFAULT_CHUNK_SIZE> join_filter_chunk_;
	bool semi_build_marked_;
	uint32_t semi_build_emit_pos_;
	bool anti_build_marked_;
	uint32_t anti_build_emit_pos_;
	bool right_anti_marked_;
	uint16_t anti_outer_pos_;
	uint32_t right_anti_emit_pos_;
	size_t bucket_mask_;
};

struct VecSortKeyDesc {
	uint16_t col_idx;
	Oid sql_type;
	VecOutputStorageKind storage_kind;
	bool descending;
	bool nulls_first;
	Oid collation;
	int scale;
};

struct VecSortKeyLane {
	VecSortKeyDesc desc;
	VolVecVector<uint8_t> nulls;
	VolVecVector<int32_t> i32_values;
	VolVecVector<int64_t> i64_values;
	VolVecVector<uint64_t> u64_values;
	VolVecVector<VecStringRef> string_values;
	VolVecVector<char> string_arena;

	VecSortKeyLane(const VecSortKeyDesc &key_desc, MemoryContext context)
		: desc(key_desc),
		  nulls(PgMemoryContextAllocator<uint8_t>(context)),
		  i32_values(PgMemoryContextAllocator<int32_t>(context)),
		  i64_values(PgMemoryContextAllocator<int64_t>(context)),
		  u64_values(PgMemoryContextAllocator<uint64_t>(context)),
		  string_values(PgMemoryContextAllocator<VecStringRef>(context)),
		  string_arena(PgMemoryContextAllocator<char>(context))
	{
	}

	VecStringRef store_string_bytes(const char *data, uint32_t len)
	{
		VecStringRef ref{len, 0, 0};

		if (len == 0 || data == nullptr)
			return ref;
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
};

class VecSortState : public VecPlanState {
public:
	VecSortState(std::unique_ptr<VecPlanState> left, Sort *node,
				 VolVecVector<VecSortKeyDesc> key_descs,
				 int output_ncols = -1);
	~VecSortState() override;
	bool get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk) override;
	bool lookup_output_col_meta(int target_resno, VecOutputColMeta *out) const override
	{
		return left_ != nullptr && left_->lookup_output_col_meta(target_resno, out);
	}
private:
	void materialize_and_sort();
	DataChunk<DEFAULT_CHUNK_SIZE> *allocate_payload_chunk();
	void append_batch(const DataChunk<DEFAULT_CHUNK_SIZE> &input);
	void append_sort_key(uint32_t ordinal, const DataChunk<DEFAULT_CHUNK_SIZE> &input, int src_row);
	void copy_row(const DataChunk<DEFAULT_CHUNK_SIZE> &src, int src_row,
				  DataChunk<DEFAULT_CHUNK_SIZE> &dst, int dst_row) const;
	bool row_less(const VecRowRef &left, const VecRowRef &right) const;
	int compare_string_ref(const VecSortKeyLane &lane,
						  const VecStringRef &left,
						  const VecStringRef &right) const;

	std::unique_ptr<VecPlanState> left_;
	Sort *node_;
	MemoryContext memory_context_;
	VolVecVector<DataChunk<DEFAULT_CHUNK_SIZE> *> payload_chunks_;
	VolVecVector<VecRowRef> rows_;
	VolVecVector<VecSortKeyDesc> key_descs_;
	VolVecVector<VecSortKeyLane> key_lanes_;
	size_t emit_pos_;
	int output_ncols_;
	bool materialized_;
};

struct PgVolVecQueryState { MemoryContext context; VecPlanState* vec_plan; };

void CompileExpr(Expr *expr, VecExprProgram &program, bool is_filter = false, EState *estate = nullptr);
std::unique_ptr<VecPlanState> ExecInitVecPlan(Plan *plan, EState *estate);

#ifdef USE_LLVM
bool pg_volvec_try_compile_jit_deform_to_datachunk(TupleDesc desc, const DeformProgram *program, JitDeformFunc *out_func, JitContext **out_context, const char **failure_reason);
bool pg_volvec_try_compile_jit_expr(const VecExprProgram *program, VecExprJitFunc *out_func, JitContext **out_context, const char **failure_reason);
void pg_volvec_release_llvm_jit_context(JitContext *context);
#endif

} /* namespace pg_volvec */

#endif

#ifndef PG_VEC_DATA_CHUNK_DEFORM_HPP
#define PG_VEC_DATA_CHUNK_DEFORM_HPP

extern "C" {
#include "postgres.h"

#include "access/htup_details.h"
#include "fmgr.h"
}

#include "../ir/vec_ir.h"

#include <cstdint>
#include <cstring>

namespace pg_vec
{

static constexpr int kMaxDeformTargets = 16;
static constexpr int kNumericDecDigits = 4;
static constexpr int kNumericBase = 10000;
static constexpr int kDecimalScale2 = 2;
static constexpr std::uint32_t kStringNoTailOffset = UINT32_MAX;
static constexpr std::size_t kStringPrefixBytes = PG_VEC_STRING_PREFIX_BYTES;

static constexpr uint16 kNumericSignMask = 0xC000;
static constexpr uint16 kNumericNeg = 0x4000;
static constexpr uint16 kNumericShort = 0x8000;
static constexpr uint16 kNumericSpecial = 0xC000;
static constexpr uint16 kNumericShortSignMask = 0x2000;
static constexpr uint16 kNumericShortDscaleMask = 0x1F80;
static constexpr uint16 kNumericShortDscaleShift = 7;
static constexpr uint16 kNumericShortWeightSignMask = 0x0040;
static constexpr uint16 kNumericShortWeightMask = 0x003F;
static constexpr uint16 kNumericDscaleMask = 0x3FFF;

enum class DeformDecodeKind : uint8_t
{
	kInt32,
	kDate32,
	kDecimal64Scale2,
	kBpChar1,
	kStringRef
};

struct DeformTarget
{
	int			att_index;
	std::uint16_t dst_col;
	DeformDecodeKind decode_kind;
};

struct DeformProgram
{
	int			ntargets;
	int			last_att_index;
	DeformTarget targets[kMaxDeformTargets];

	void
	reset()
	{
		ntargets = 0;
		last_att_index = -1;
	}

	bool
	add_target(int att_index, std::uint16_t dst_col, DeformDecodeKind decode_kind)
	{
		if (ntargets >= kMaxDeformTargets)
			return false;

		targets[ntargets++] = {att_index, dst_col, decode_kind};
		return true;
	}

	void
	finalize()
	{
		for (int i = 1; i < ntargets; i++)
		{
			DeformTarget key = targets[i];
			int			j = i - 1;

			while (j >= 0 && targets[j].att_index > key.att_index)
			{
				targets[j + 1] = targets[j];
				j--;
			}

			targets[j + 1] = key;
		}

		last_att_index = (ntargets == 0) ? -1 : targets[ntargets - 1].att_index;
	}
};

struct DeformColumnBinding
{
	void	   *data;
	void	   *aux;
};

struct DeformBindings
{
	const DeformColumnBinding *columns;
	void	   *columns_data[kMaxDeformTargets];
	bool	   *columns_nulls[kMaxDeformTargets];
	int			ncolumns;
};

struct PgVecStringArena
{
	char	   *data;
	std::uint32_t size;
	std::uint32_t capacity;
};

struct DeformFilterClause
{
	int			att_index;
	std::uint16_t dst_col;
	PgVecScalarKind scalar_kind;
	PgVecFilterOp op;
	PgVecConstValue constant;
};

enum class DeformFilterNodeKind : uint8_t
{
	kInvalid = 0,
	kClause,
	kAnd,
	kOr
};

struct DeformFilterNode
{
	DeformFilterNodeKind kind;
	int			left;
	int			right;
	int			clause_idx;
};

struct DeformFilterProgram
{
	bool		valid;
	bool		deform_safe;
	bool		dnf_valid;
	int			root;
	int			nnodes;
	int			nclauses;
	int			last_att_index;
	int			max_clause_att_index;
	int			ndnf_branches;
	int			att_clause_heads[PG_VEC_MAX_FILTER_NODES];
	int			clause_next[PG_VEC_MAX_FILTER_NODES];
	uint64_t	all_branch_mask;
	uint64_t	dnf_branch_masks[PG_VEC_MAX_FILTER_NODES];
	uint64_t	clause_branch_masks[PG_VEC_MAX_FILTER_NODES];
	DeformFilterClause clauses[PG_VEC_MAX_FILTER_NODES];
	DeformFilterNode nodes[PG_VEC_MAX_FILTER_NODES];
};

struct NumericDecodeInfo
{
	const char *digits_ptr;
	int			sign;
	int			weight;
	int			dscale;
	int			ndigits;
};

static inline void
string_arena_reset(PgVecStringArena *arena)
{
	arena->size = 0;
}

static inline bool
string_arena_ensure_capacity(PgVecStringArena *arena, std::uint32_t additional)
{
	std::uint32_t needed;
	std::uint32_t new_capacity;

	if (additional == 0)
		return true;
	if (arena->size > UINT32_MAX - additional)
		return false;

	needed = arena->size + additional;
	if (needed <= arena->capacity)
		return true;

	new_capacity = (arena->capacity == 0) ? 256U : arena->capacity;
	while (new_capacity < needed)
	{
		if (new_capacity > UINT32_MAX / 2)
			new_capacity = needed;
		else
			new_capacity *= 2;
	}

	if (arena->data == nullptr)
		arena->data = static_cast<char *>(palloc(new_capacity));
	else
		arena->data = static_cast<char *>(repalloc(arena->data, new_capacity));
	arena->capacity = new_capacity;
	return true;
}

static inline bool
string_arena_append(PgVecStringArena *arena,
					  const char *src,
					  std::uint32_t len,
					  std::uint32_t *offset)
{
	if (!string_arena_ensure_capacity(arena, len))
		return false;

	*offset = arena->size;
	if (len > 0)
		std::memcpy(arena->data + arena->size, src, len);
	arena->size += len;
	return true;
}

static inline std::uint64_t
pack_string_prefix(const char *src, std::size_t len)
{
	std::uint64_t prefix = 0;
	std::size_t	copy_len = (len < kStringPrefixBytes) ? len : kStringPrefixBytes;

	if (copy_len > 0)
		std::memcpy(&prefix, src, copy_len);
	return prefix;
}

static inline void
unpack_string_prefix(std::uint64_t prefix, char *dst)
{
	std::memcpy(dst, &prefix, kStringPrefixBytes);
}

static inline uint16
load_u16_unaligned(const void *ptr)
{
	uint16		value;

	std::memcpy(&value, ptr, sizeof(value));
	return value;
}

static inline int16
load_i16_unaligned(const void *ptr)
{
	int16		value;

	std::memcpy(&value, ptr, sizeof(value));
	return value;
}

static inline bool
scale_numeric_accumulator(__int128 *value, int exponent)
{
	if (exponent >= 0)
	{
		for (int i = 0; i < exponent; i++)
			*value *= 10;
		return true;
	}

	for (int i = 0; i < -exponent; i++)
	{
		if ((*value % 10) != 0)
			return false;
		*value /= 10;
	}

	return true;
}

static inline bool
parse_numeric_varlena_inline(const void *ptr, NumericDecodeInfo *out)
{
	const char *payload;
	Size		payload_size;
	Size		header_size;
	uint16		header;

	payload = VARDATA_ANY(ptr);
	payload_size = VARSIZE_ANY_EXHDR(ptr);
	if (payload_size < sizeof(uint16))
		return false;

	header = load_u16_unaligned(payload);
	if ((header & kNumericSignMask) == kNumericSpecial)
		return false;

	if ((header & kNumericSignMask) == kNumericShort)
	{
		out->sign = (header & kNumericShortSignMask) ? kNumericNeg : 0;
		out->dscale = (header & kNumericShortDscaleMask) >> kNumericShortDscaleShift;
		out->weight = ((header & kNumericShortWeightSignMask) ?
				  ~kNumericShortWeightMask : 0) |
			(header & kNumericShortWeightMask);
		header_size = sizeof(uint16);
	}
	else
	{
		if (payload_size < sizeof(uint16) + sizeof(int16))
			return false;

		out->sign = header & kNumericSignMask;
		out->dscale = header & kNumericDscaleMask;
		out->weight = load_i16_unaligned(payload + sizeof(uint16));
		header_size = sizeof(uint16) + sizeof(int16);
	}

	if ((payload_size - header_size) % sizeof(int16) != 0)
		return false;

	out->digits_ptr = payload + header_size;
	out->ndigits = (payload_size - header_size) / sizeof(int16);
	return true;
}

static inline bool
load_numeric_digit(const NumericDecodeInfo &info, int idx, int16 *digit)
{
	if (idx < 0 || idx >= info.ndigits)
		return false;

	*digit = load_i16_unaligned(info.digits_ptr + idx * sizeof(int16));
	return *digit >= 0 && *digit < kNumericBase;
}

static inline bool
accumulate_base10000_int64(int64 *value, int16 digit)
{
	__int128	next = static_cast<__int128>(*value) * kNumericBase + digit;

	if (next > PG_INT64_MAX)
		return false;

	*value = static_cast<int64>(next);
	return true;
}

static inline bool
multiply_int64_small(int64 *value, int factor)
{
	__int128	next = static_cast<__int128>(*value) * factor;

	if (next > PG_INT64_MAX)
		return false;

	*value = static_cast<int64>(next);
	return true;
}

static inline bool
numeric_varlena_to_decimal64_scale2_fast_inline(const void *ptr, int64 *out)
{
	NumericDecodeInfo info;
	int			integer_groups;
	int			groups_from_digits;
	int			fractional_idx;
	int64		scaled = 0;
	int16		digit;

	if (!parse_numeric_varlena_inline(ptr, &info))
		return false;

	if (info.dscale > kDecimalScale2)
		return false;

	if (info.ndigits == 0)
	{
		*out = 0;
		return true;
	}

	if (info.weight < -1)
		return false;

	if (info.ndigits > info.weight + 2)
		return false;

	integer_groups = info.weight + 1;
	if (integer_groups > 0)
	{
		groups_from_digits = (info.ndigits < integer_groups) ? info.ndigits : integer_groups;

		for (int i = 0; i < groups_from_digits; i++)
		{
			if (!load_numeric_digit(info, i, &digit) ||
				!accumulate_base10000_int64(&scaled, digit))
				return false;
		}

		for (int i = groups_from_digits; i < integer_groups; i++)
		{
			if (!multiply_int64_small(&scaled, kNumericBase))
				return false;
		}
	}

	if (!multiply_int64_small(&scaled, 100))
		return false;

	fractional_idx = info.weight + 1;
	if (fractional_idx >= 0 && fractional_idx < info.ndigits)
	{
		if (!load_numeric_digit(info, fractional_idx, &digit) || (digit % 100) != 0)
			return false;
		scaled += digit / 100;
	}

	if (info.sign == kNumericNeg)
		scaled = -scaled;

	*out = scaled;
	return true;
}

static inline bool
numeric_varlena_to_scaled_int64_inline(const void *ptr, int scale, int64 *out)
{
	NumericDecodeInfo info;
	__int128	accum = 0;
	int16		digit;

	if (scale == kDecimalScale2 &&
		numeric_varlena_to_decimal64_scale2_fast_inline(ptr, out))
		return true;

	if (!parse_numeric_varlena_inline(ptr, &info))
		return false;

	for (int i = 0; i < info.ndigits; i++)
	{
		if (!load_numeric_digit(info, i, &digit))
			return false;

		accum = accum * kNumericBase + digit;
	}

	if (!scale_numeric_accumulator(&accum,
								   kNumericDecDigits * (info.weight - info.ndigits + 1) + scale))
		return false;

	if (info.sign == kNumericNeg)
		accum = -accum;

	if (accum < PG_INT64_MIN || accum > PG_INT64_MAX)
		return false;

	*out = (int64) accum;
	return true;
}

static inline bool
numeric_varlena_to_scaled_int64(const void *ptr, int scale, int64 *out)
{
	if (VARATT_IS_EXTERNAL(ptr) || VARATT_IS_COMPRESSED(ptr))
	{
		struct varlena *detoasted = PG_DETOAST_DATUM_PACKED(PointerGetDatum(ptr));
		bool		ok = numeric_varlena_to_scaled_int64_inline(detoasted, scale, out);

		if ((const void *) detoasted != ptr)
			pfree(detoasted);

		return ok;
	}

	return numeric_varlena_to_scaled_int64_inline(ptr, scale, out);
}

static inline bool
bpchar_varlena_to_char1_inline(const void *ptr, char *out)
{
	Size		payload_size = VARSIZE_ANY_EXHDR(ptr);
	const char *payload = VARDATA_ANY(ptr);

	if (payload_size < 1)
		return false;

	*out = payload[0];
	return true;
}

static inline bool
bpchar_varlena_to_char1(const void *ptr, char *out)
{
	if (VARATT_IS_EXTERNAL(ptr) || VARATT_IS_COMPRESSED(ptr))
	{
		struct varlena *detoasted = PG_DETOAST_DATUM_PACKED(PointerGetDatum(ptr));
		bool		ok = bpchar_varlena_to_char1_inline(detoasted, out);

		if ((const void *) detoasted != ptr)
			pfree(detoasted);

		return ok;
	}

	return bpchar_varlena_to_char1_inline(ptr, out);
}

template <typename T>
static inline bool
deform_compare_value(T lhs, T rhs, PgVecFilterOp op)
{
	switch (op)
	{
		case PG_VEC_OP_EQ:
			return lhs == rhs;
		case PG_VEC_OP_NE:
			return lhs != rhs;
		case PG_VEC_OP_LT:
			return lhs < rhs;
		case PG_VEC_OP_LE:
			return lhs <= rhs;
		case PG_VEC_OP_GT:
			return lhs > rhs;
		case PG_VEC_OP_GE:
			return lhs >= rhs;
		case PG_VEC_OP_PREFIX_LIKE:
		case PG_VEC_OP_CONTAINS_LIKE:
		case PG_VEC_OP_INVALID:
		default:
			return false;
	}
}

static inline int
trimmed_payload_length(const char *payload, std::size_t len)
{
	while (len > 0 && payload[len - 1] == ' ')
		len--;
	return len;
}

static inline int
string_const_compare(const PgVecStringConst &lhs, const PgVecStringConst &rhs)
{
	int			cmp;
	uint16		min_len = (lhs.len < rhs.len) ? lhs.len : rhs.len;

	if (min_len > 0)
	{
		cmp = std::memcmp(lhs.bytes, rhs.bytes, min_len);
		if (cmp != 0)
			return cmp;
	}

	if (lhs.len < rhs.len)
		return -1;
	if (lhs.len > rhs.len)
		return 1;
	return 0;
}

static inline bool
string_const_compare_value(const PgVecStringConst &lhs,
							  const PgVecStringConst &rhs,
							  PgVecFilterOp op)
{
	int			cmp = string_const_compare(lhs, rhs);

	switch (op)
	{
		case PG_VEC_OP_EQ:
			return cmp == 0;
		case PG_VEC_OP_NE:
			return cmp != 0;
		case PG_VEC_OP_LT:
			return cmp < 0;
		case PG_VEC_OP_LE:
			return cmp <= 0;
		case PG_VEC_OP_GT:
			return cmp > 0;
		case PG_VEC_OP_GE:
			return cmp >= 0;
		case PG_VEC_OP_PREFIX_LIKE:
		case PG_VEC_OP_CONTAINS_LIKE:
		case PG_VEC_OP_INVALID:
		default:
			return false;
	}
}

static inline bool
string_const_starts_with(const PgVecStringConst &value,
						   const PgVecStringConst &prefix)
{
	if (value.len < prefix.len)
		return false;
	if (prefix.len == 0)
		return true;
	return std::memcmp(value.bytes, prefix.bytes, prefix.len) == 0;
}

static inline bool
payload_contains_bytes(const char *payload,
						 std::size_t payload_len,
						 const char *needle,
						 std::size_t needle_len)
{
	if (needle_len == 0)
		return true;
	if (payload_len < needle_len)
		return false;

	for (std::size_t start = 0; start + needle_len <= payload_len; start++)
	{
		if (std::memcmp(payload + start, needle, needle_len) == 0)
			return true;
	}

	return false;
}

static inline bool
string_const_contains(const PgVecStringConst &value,
						 const PgVecStringConst &needle)
{
	return payload_contains_bytes(value.bytes, value.len, needle.bytes, needle.len);
}

static inline const char *
string_ref_tail_ptr(const PgVecStringRef &ref, const PgVecStringArena *arena)
{
	if (ref.tail_offset == kStringNoTailOffset)
		return nullptr;
	return arena->data + ref.tail_offset;
}

static inline char
string_ref_byte_at(const PgVecStringRef &ref,
					 const PgVecStringArena *arena,
					 std::uint16_t idx)
{
	const char *prefix_bytes = reinterpret_cast<const char *>(&ref.prefix);

	if (idx < kStringPrefixBytes)
		return prefix_bytes[idx];

	return string_ref_tail_ptr(ref, arena)[idx - kStringPrefixBytes];
}

static inline int
compare_string_segments(const char *lhs_prefix,
						  std::uint16_t lhs_len,
						  const char *lhs_tail,
						  const char *rhs_prefix,
						  std::uint16_t rhs_len,
						  const char *rhs_tail)
{
	int			cmp;
	std::uint16_t common_len = (lhs_len < rhs_len) ? lhs_len : rhs_len;
	std::uint16_t prefix_len = (common_len < kStringPrefixBytes) ?
		common_len : static_cast<std::uint16_t>(kStringPrefixBytes);

	if (prefix_len > 0)
	{
		cmp = std::memcmp(lhs_prefix, rhs_prefix, prefix_len);
		if (cmp != 0)
			return cmp;
	}

	if (common_len > kStringPrefixBytes)
	{
		cmp = std::memcmp(lhs_tail, rhs_tail, common_len - kStringPrefixBytes);
		if (cmp != 0)
			return cmp;
	}

	if (lhs_len < rhs_len)
		return -1;
	if (lhs_len > rhs_len)
		return 1;
	return 0;
}

static inline bool
string_ref_copy_to_const(const PgVecStringRef &ref,
						 const PgVecStringArena *arena,
						 PgVecStringConst *out)
{
	char		prefix_buf[kStringPrefixBytes];
	std::size_t prefix_len;
	std::size_t tail_len;

	if (ref.len >= PG_VEC_INLINE_STRING_MAX)
		return false;

	out->len = ref.len;
	if (out->len == 0)
	{
		std::memset(out->bytes, 0, PG_VEC_INLINE_STRING_MAX);
		return true;
	}

	unpack_string_prefix(ref.prefix, prefix_buf);
	prefix_len = (ref.len < kStringPrefixBytes) ? ref.len : kStringPrefixBytes;
	std::memcpy(out->bytes, prefix_buf, prefix_len);

	if (ref.len > kStringPrefixBytes)
	{
		tail_len = ref.len - kStringPrefixBytes;
		std::memcpy(out->bytes + kStringPrefixBytes,
					string_ref_tail_ptr(ref, arena),
					tail_len);
	}

	if (out->len < PG_VEC_INLINE_STRING_MAX)
		std::memset(out->bytes + out->len, 0, PG_VEC_INLINE_STRING_MAX - out->len);
	return true;
}

static inline int
string_ref_compare_const(const PgVecStringRef &lhs,
						 const PgVecStringArena *lhs_arena,
						 const PgVecStringConst &rhs)
{
	const char *lhs_prefix = reinterpret_cast<const char *>(&lhs.prefix);
	const char *lhs_tail = string_ref_tail_ptr(lhs, lhs_arena);
	const char *rhs_prefix = rhs.bytes;
	const char *rhs_tail = (rhs.len > kStringPrefixBytes) ?
		rhs.bytes + kStringPrefixBytes : nullptr;

	return compare_string_segments(lhs_prefix,
								   lhs.len,
								   lhs_tail,
								   rhs_prefix,
								   rhs.len,
								   rhs_tail);
}

static inline int
string_ref_compare_ref(const PgVecStringRef &lhs,
					   const PgVecStringArena *lhs_arena,
					   const PgVecStringRef &rhs,
					   const PgVecStringArena *rhs_arena)
{
	const char *lhs_prefix = reinterpret_cast<const char *>(&lhs.prefix);
	const char *rhs_prefix = reinterpret_cast<const char *>(&rhs.prefix);
	const char *lhs_tail = string_ref_tail_ptr(lhs, lhs_arena);
	const char *rhs_tail = string_ref_tail_ptr(rhs, rhs_arena);

	return compare_string_segments(lhs_prefix,
								   lhs.len,
								   lhs_tail,
								   rhs_prefix,
								   rhs.len,
								   rhs_tail);
}

static inline bool
string_ref_compare_value(const PgVecStringRef &lhs,
						 const PgVecStringArena *lhs_arena,
						 const PgVecStringConst &rhs,
						 PgVecFilterOp op)
{
	int			cmp = string_ref_compare_const(lhs, lhs_arena, rhs);

	switch (op)
	{
		case PG_VEC_OP_EQ:
			return cmp == 0;
		case PG_VEC_OP_NE:
			return cmp != 0;
		case PG_VEC_OP_LT:
			return cmp < 0;
		case PG_VEC_OP_LE:
			return cmp <= 0;
		case PG_VEC_OP_GT:
			return cmp > 0;
		case PG_VEC_OP_GE:
			return cmp >= 0;
		case PG_VEC_OP_PREFIX_LIKE:
		case PG_VEC_OP_CONTAINS_LIKE:
		case PG_VEC_OP_INVALID:
		default:
			return false;
	}
}

static inline bool
string_ref_starts_with_const(const PgVecStringRef &value,
							  const PgVecStringArena *value_arena,
							  const PgVecStringConst &prefix)
{
	std::uint16_t prefix_prefix_len;

	if (value.len < prefix.len)
		return false;
	if (prefix.len == 0)
		return true;

	prefix_prefix_len = (prefix.len < kStringPrefixBytes) ?
		prefix.len : static_cast<std::uint16_t>(kStringPrefixBytes);
	if (prefix_prefix_len > 0 &&
		std::memcmp(reinterpret_cast<const char *>(&value.prefix),
					prefix.bytes,
					prefix_prefix_len) != 0)
		return false;

	if (prefix.len > kStringPrefixBytes)
	{
		const char *value_tail = string_ref_tail_ptr(value, value_arena);

		if (value_tail == nullptr)
			return false;
		return std::memcmp(value_tail,
						   prefix.bytes + kStringPrefixBytes,
						   prefix.len - kStringPrefixBytes) == 0;
	}

	return true;
}

static inline bool
string_ref_contains_const(const PgVecStringRef &value,
							 const PgVecStringArena *value_arena,
							 const PgVecStringConst &needle)
{
	if (needle.len == 0)
		return true;
	if (value.len < needle.len)
		return false;

	for (std::uint16_t start = 0; start + needle.len <= value.len; start++)
	{
		std::uint16_t matched = 0;

		while (matched < needle.len &&
			   string_ref_byte_at(value, value_arena, start + matched) ==
				   needle.bytes[matched])
			matched++;
		if (matched == needle.len)
			return true;
	}

	return false;
}

static inline bool
bytes_to_string_const(const char *payload,
					  std::size_t payload_size,
					  PgVecStringConst *out)
{
	if (payload_size >= PG_VEC_INLINE_STRING_MAX)
		return false;

	out->len = static_cast<uint16>(payload_size);
	if (payload_size > 0)
		std::memcpy(out->bytes, payload, payload_size);
	if (payload_size < PG_VEC_INLINE_STRING_MAX)
		std::memset(out->bytes + payload_size,
					0,
					PG_VEC_INLINE_STRING_MAX - payload_size);
	return true;
}

static inline bool
bytes_to_string_ref(const char *payload,
					std::size_t payload_size,
					PgVecStringArena *arena,
					PgVecStringRef *out)
{
	std::size_t tail_len;

	if (payload_size > UINT16_MAX)
		return false;

	out->len = static_cast<uint16>(payload_size);
	out->flags = 0;
	out->prefix = pack_string_prefix(payload, payload_size);

	if (payload_size <= kStringPrefixBytes)
	{
		out->tail_offset = kStringNoTailOffset;
		return true;
	}

	tail_len = payload_size - kStringPrefixBytes;
	return string_arena_append(arena,
							   payload + kStringPrefixBytes,
							   static_cast<std::uint32_t>(tail_len),
							   &out->tail_offset);
}

static inline bool
varlena_payload_to_string_ref_inline(const void *ptr,
									 bool trim_trailing_spaces,
									 PgVecStringArena *arena,
									 PgVecStringRef *out)
{
	Size		payload_size = VARSIZE_ANY_EXHDR(ptr);
	const char *payload = VARDATA_ANY(ptr);

	if (trim_trailing_spaces)
		payload_size = trimmed_payload_length(payload, payload_size);

	return bytes_to_string_ref(payload, payload_size, arena, out);
}

static inline bool
varlena_payload_to_string_ref(const void *ptr,
							  bool trim_trailing_spaces,
							  PgVecStringArena *arena,
							  PgVecStringRef *out)
{
	if (VARATT_IS_EXTERNAL(ptr) || VARATT_IS_COMPRESSED(ptr))
	{
		struct varlena *detoasted = PG_DETOAST_DATUM_PACKED(PointerGetDatum(ptr));
		bool		ok = varlena_payload_to_string_ref_inline(detoasted,
															 trim_trailing_spaces,
															 arena,
															 out);

		if ((const void *) detoasted != ptr)
			pfree(detoasted);

		return ok;
	}

	return varlena_payload_to_string_ref_inline(ptr,
												 trim_trailing_spaces,
												 arena,
												 out);
}

static inline bool
varlena_matches_string_const_inline(const void *ptr,
									  bool trim_trailing_spaces,
									  const PgVecStringConst &constant,
									  PgVecFilterOp op)
{
	Size		payload_size = VARSIZE_ANY_EXHDR(ptr);
	const char *payload = VARDATA_ANY(ptr);
	PgVecStringConst value;

	if (trim_trailing_spaces)
		payload_size = trimmed_payload_length(payload, payload_size);
	if (op == PG_VEC_OP_CONTAINS_LIKE)
		return payload_contains_bytes(payload,
									  payload_size,
									  constant.bytes,
									  constant.len);
	if (!bytes_to_string_const(payload, payload_size, &value))
		return false;

	if (op == PG_VEC_OP_PREFIX_LIKE)
		return string_const_starts_with(value, constant);
	if (op == PG_VEC_OP_CONTAINS_LIKE)
		return string_const_contains(value, constant);
	return string_const_compare_value(value, constant, op);
}

static inline bool
varlena_matches_string_const(const void *ptr,
							   bool trim_trailing_spaces,
							   const PgVecStringConst &constant,
							   PgVecFilterOp op)
{
	if (VARATT_IS_EXTERNAL(ptr) || VARATT_IS_COMPRESSED(ptr))
	{
		struct varlena *detoasted = PG_DETOAST_DATUM_PACKED(PointerGetDatum(ptr));
		bool		ok = varlena_matches_string_const_inline(detoasted,
															 trim_trailing_spaces,
															 constant,
															 op);

		if ((const void *) detoasted != ptr)
			pfree(detoasted);

		return ok;
	}

	return varlena_matches_string_const_inline(ptr,
												 trim_trailing_spaces,
												 constant,
												 op);
}

enum class AppendTupleResult : uint8_t
{
	kError,
	kFilteredOut,
	kStored
};

typedef void (*JitDeformFunc)(HeapTuple tuple, void **col_data_ptrs, bool **col_null_ptrs, uint32 row_idx);

class DataChunkDeformer
{
public:
	DataChunkDeformer(TupleDesc desc, const DeformProgram *program) :
		desc_(desc),
		program_(*program),
		jit_func_(nullptr)
	{
	}

	void set_jit_func(JitDeformFunc func) { jit_func_ = func; }
	bool has_jit_func() const { return jit_func_ != nullptr; }

	AppendTupleResult
	append_tuple(HeapTuple tuple,
				 std::uint16_t row,
				 const DeformBindings &bindings,
				 const DeformFilterProgram *filter = nullptr) const
	{
		if (jit_func_ != nullptr)
		{
			jit_func_(tuple,
					 const_cast<void **>(bindings.columns_data),
					 const_cast<bool **>(bindings.columns_nulls),
					 row);
			return AppendTupleResult::kStored;
		}

		HeapTupleHeader td = tuple->t_data;
		char	   *tp = (char *) td + td->t_hoff;
		bits8	   *bp = td->t_bits;
		bool		usecache = true;
		bool		has_nulls = HeapTupleHasNulls(tuple);
		int			off = 0;
		int			target_index = 0;
		bool		filter_active = (filter != nullptr && filter->valid &&
									filter->root >= 0 &&
									filter->nclauses > 0);
		bool		filter_resolved = !filter_active;
		bool		filter_passed = !filter_active;
		uint64_t	clause_bits = 0;
		uint64_t	dead_branch_mask = 0;
		int			clauses_done = 0;
		PendingTarget pending_targets[kMaxDeformTargets];
		int			npending = 0;
		int			last_att_index = program_.last_att_index;

		if (program_.ntargets <= 0)
			return AppendTupleResult::kError;

		if (filter_active && filter->last_att_index > last_att_index)
			last_att_index = filter->last_att_index;

		if ((int) HeapTupleHeaderGetNatts(td) <= last_att_index)
			return AppendTupleResult::kError;

		for (int att_index = 0; att_index <= last_att_index; att_index++)
		{
			CompactAttribute *att = TupleDescCompactAttr(desc_, att_index);

			if (att->attisdropped)
				return AppendTupleResult::kError;

			if (has_nulls && att_isnull(att_index, bp))
			{
				usecache = false;

				if (target_index < program_.ntargets &&
					program_.targets[target_index].att_index == att_index)
					return AppendTupleResult::kError;

				continue;
			}

			if (usecache && att->attcacheoff >= 0)
				off = att->attcacheoff;
			else if (att->attlen == -1)
			{
				if (usecache && off == att_nominal_alignby(off, att->attalignby))
					att->attcacheoff = off;
				else
				{
					off = att_pointer_alignby(off, att->attalignby, -1, tp + off);
					usecache = false;
				}
			}
			else
			{
				off = att_nominal_alignby(off, att->attalignby);

				if (usecache)
					att->attcacheoff = off;
			}

			if (filter_active && !filter_resolved)
			{
				for (int clause_idx = filter->att_clause_heads[att_index];
					 clause_idx >= 0;
					 clause_idx = filter->clause_next[clause_idx])
				{
					if (evaluate_filter_clause(filter->clauses[clause_idx],
													 desc_,
													 att,
													 tp + off))
						clause_bits |= (UINT64CONST(1) << clause_idx);
					else if (filter->dnf_valid)
						dead_branch_mask |= filter->clause_branch_masks[clause_idx];

					clauses_done++;
				}

				if (filter->dnf_valid)
				{
					uint64_t live_branch_mask =
						filter->all_branch_mask & ~dead_branch_mask;

					for (int branch_idx = 0; branch_idx < filter->ndnf_branches; branch_idx++)
					{
						uint64_t branch_mask = filter->dnf_branch_masks[branch_idx];

						if ((live_branch_mask & (UINT64CONST(1) << branch_idx)) == 0)
							continue;
						if ((clause_bits & branch_mask) == branch_mask)
						{
							filter_resolved = true;
							filter_passed = true;
							break;
						}
					}

					if (!filter_resolved && live_branch_mask == 0)
						return AppendTupleResult::kFilteredOut;
				}
				else
				{
					filter_resolved = (clauses_done == filter->nclauses);
					if (filter_resolved)
					{
						filter_passed = evaluate_filter_node(*filter,
															filter->root,
															clause_bits);
						if (!filter_passed)
							return AppendTupleResult::kFilteredOut;
					}
				}
			}

			if (target_index < program_.ntargets &&
				program_.targets[target_index].att_index == att_index)
			{
				if (!filter_resolved)
				{
					if (npending >= kMaxDeformTargets)
						return AppendTupleResult::kError;
					pending_targets[npending++] = {&program_.targets[target_index],
												   att,
												   tp + off};
				}
					else if (!store_target(program_.targets[target_index],
									   att,
									   tp + off,
									   row,
									   desc_,
									   bindings))
				{
					return AppendTupleResult::kError;
				}

				target_index++;
				if (filter_resolved && npending > 0)
				{
						if (!materialize_pending_targets(pending_targets,
														 npending,
														 row,
														 desc_,
														 bindings))
						return AppendTupleResult::kError;
					npending = 0;
				}
				if (target_index == program_.ntargets && (!filter_active || filter_resolved))
					return AppendTupleResult::kStored;
			}

			off = att_addlength_pointer(off, att->attlen, tp + off);

			if (usecache && att->attlen <= 0)
				usecache = false;
		}

		if (filter_active)
		{
			if (!filter_resolved)
				return AppendTupleResult::kError;
			if (!filter_passed)
				return AppendTupleResult::kFilteredOut;
		}

		if (npending > 0)
		{
				if (!materialize_pending_targets(pending_targets,
												 npending,
												 row,
												 desc_,
												 bindings))
				return AppendTupleResult::kError;
		}

		return (target_index == program_.ntargets) ?
			AppendTupleResult::kStored :
			AppendTupleResult::kError;
	}

private:
	static bool
	evaluate_filter_node(const DeformFilterProgram &program,
						 int node_idx,
						 uint64_t clause_bits)
	{
		const DeformFilterNode &node = program.nodes[node_idx];

		switch (node.kind)
		{
			case DeformFilterNodeKind::kClause:
				return (clause_bits & (UINT64CONST(1) << node.clause_idx)) != 0;
			case DeformFilterNodeKind::kAnd:
				return evaluate_filter_node(program, node.left, clause_bits) &&
					evaluate_filter_node(program, node.right, clause_bits);
			case DeformFilterNodeKind::kOr:
				return evaluate_filter_node(program, node.left, clause_bits) ||
					evaluate_filter_node(program, node.right, clause_bits);
			case DeformFilterNodeKind::kInvalid:
			default:
				return false;
		}
	}

	struct PendingTarget
	{
		const DeformTarget *target;
		const CompactAttribute *att;
		const char *ptr;
	};

	static bool
	evaluate_filter_clause(const DeformFilterClause &clause,
						   TupleDesc desc,
						   const CompactAttribute *att,
						   const char *ptr)
	{
		Datum		value = fetch_att(ptr, att->attbyval, att->attlen);

		switch (clause.scalar_kind)
		{
			case PG_VEC_SCALAR_INT32:
				return deform_compare_value(DatumGetInt32(value),
											 clause.constant.int32_value,
											 clause.op);
			case PG_VEC_SCALAR_DATE32:
				return deform_compare_value(DatumGetDateADT(value),
											 clause.constant.date32,
											 clause.op);
			case PG_VEC_SCALAR_DECIMAL64_S2:
			{
				int64		decoded;

				if (!numeric_varlena_to_scaled_int64(DatumGetPointer(value),
													 kDecimalScale2,
													 &decoded))
					return false;
				return deform_compare_value(decoded,
											 clause.constant.decimal64_s2,
											 clause.op);
			}
			case PG_VEC_SCALAR_CHAR1:
			{
				char		decoded;

				if (!bpchar_varlena_to_char1(DatumGetPointer(value), &decoded))
					return false;
				return deform_compare_value(decoded, clause.constant.char1, clause.op);
			}
			case PG_VEC_SCALAR_STRING128:
			{
				Form_pg_attribute attr = TupleDescAttr(desc, clause.att_index);
				bool trim_trailing_spaces = (attr->atttypid == BPCHAROID);

				return varlena_matches_string_const(DatumGetPointer(value),
												 trim_trailing_spaces,
												 clause.constant.string128,
												 clause.op);
			}
			case PG_VEC_SCALAR_DECIMAL128_S4:
			case PG_VEC_SCALAR_DECIMAL128_S6:
			case PG_VEC_SCALAR_INVALID:
			default:
				return false;
		}
	}

	static bool
	materialize_pending_targets(const PendingTarget *pending_targets,
								 int npending,
								 std::uint16_t row,
								 TupleDesc desc,
								 const DeformBindings &bindings)
	{
		for (int i = 0; i < npending; i++)
		{
			if (!store_target(*pending_targets[i].target,
							  pending_targets[i].att,
							  pending_targets[i].ptr,
							  row,
							  desc,
							  bindings))
				return false;
		}

		return true;
	}

	static bool
	store_target(const DeformTarget &target,
				 const CompactAttribute *att,
				 const char *ptr,
				 std::uint16_t row,
				 TupleDesc desc,
				 const DeformBindings &bindings)
	{
		Datum		value;
		void	   *dst;

		if (target.dst_col >= bindings.ncolumns)
			return false;

		value = fetch_att(ptr, att->attbyval, att->attlen);
		dst = bindings.columns[target.dst_col].data;

		switch (target.decode_kind)
		{
			case DeformDecodeKind::kInt32:
				static_cast<std::int32_t *>(dst)[row] = DatumGetInt32(value);
				return true;
			case DeformDecodeKind::kDate32:
				static_cast<std::int32_t *>(dst)[row] = DatumGetDateADT(value);
				return true;
			case DeformDecodeKind::kDecimal64Scale2:
				return numeric_varlena_to_scaled_int64(DatumGetPointer(value),
													 kDecimalScale2,
													 &static_cast<std::int64_t *>(dst)[row]);
			case DeformDecodeKind::kBpChar1:
				return bpchar_varlena_to_char1(DatumGetPointer(value),
											  &static_cast<char *>(dst)[row]);
			case DeformDecodeKind::kStringRef:
			{
				Form_pg_attribute attr = TupleDescAttr(desc, target.att_index);
				PgVecStringArena *arena = static_cast<PgVecStringArena *>(bindings.columns[target.dst_col].aux);
				bool trim_trailing_spaces = (attr->atttypid == BPCHAROID);

				if (arena == nullptr)
					return false;
				return varlena_payload_to_string_ref(DatumGetPointer(value),
													 trim_trailing_spaces,
													 arena,
													 &static_cast<PgVecStringRef *>(dst)[row]);
			}
		}

		return false;
	}

	TupleDesc	desc_;
	DeformProgram program_;
	JitDeformFunc jit_func_;
};

} /* namespace pg_vec */

#endif

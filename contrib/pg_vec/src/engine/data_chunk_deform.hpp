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
	kString128
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
};

struct DeformBindings
{
	const DeformColumnBinding *columns;
	int			ncolumns;
};

struct NumericDecodeInfo
{
	const char *digits_ptr;
	int			sign;
	int			weight;
	int			dscale;
	int			ndigits;
};

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

static inline bool
varlena_to_string128_inline(const void *ptr, PgVecStringConst *out)
{
	Size		payload_size = VARSIZE_ANY_EXHDR(ptr);
	const char *payload = VARDATA_ANY(ptr);

	if (payload_size >= PG_VEC_INLINE_STRING_MAX)
		return false;

	out->len = (uint16) payload_size;
	if (payload_size > 0)
		std::memcpy(out->bytes, payload, payload_size);
	if (payload_size < PG_VEC_INLINE_STRING_MAX)
		std::memset(out->bytes + payload_size,
					0,
					PG_VEC_INLINE_STRING_MAX - payload_size);
	return true;
}

static inline bool
varlena_to_string128(const void *ptr, PgVecStringConst *out)
{
	if (VARATT_IS_EXTERNAL(ptr) || VARATT_IS_COMPRESSED(ptr))
	{
		struct varlena *detoasted = PG_DETOAST_DATUM_PACKED(PointerGetDatum(ptr));
		bool		ok = varlena_to_string128_inline(detoasted, out);

		if ((const void *) detoasted != ptr)
			pfree(detoasted);

		return ok;
	}

	return varlena_to_string128_inline(ptr, out);
}

class DataChunkDeformer
{
public:
	DataChunkDeformer(TupleDesc desc, const DeformProgram *program) :
		desc_(desc),
		program_(*program)
	{
	}

	bool
	append_tuple(HeapTuple tuple,
				 std::uint16_t row,
				 const DeformBindings &bindings) const
	{
		HeapTupleHeader td = tuple->t_data;
		char	   *tp = (char *) td + td->t_hoff;
		bits8	   *bp = td->t_bits;
		bool		usecache = true;
		bool		has_nulls = HeapTupleHasNulls(tuple);
		int			off = 0;
		int			target_index = 0;

		if (program_.ntargets <= 0)
			return false;

		if ((int) HeapTupleHeaderGetNatts(td) <= program_.last_att_index)
			return false;

		for (int att_index = 0; att_index <= program_.last_att_index; att_index++)
		{
			CompactAttribute *att = TupleDescCompactAttr(desc_, att_index);

			if (att->attisdropped)
				return false;

			if (has_nulls && att_isnull(att_index, bp))
			{
				usecache = false;

				if (program_.targets[target_index].att_index == att_index)
					return false;

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

			if (program_.targets[target_index].att_index == att_index)
			{
				if (!store_target(program_.targets[target_index],
								  att,
								  tp + off,
								  row,
								  bindings))
					return false;

				target_index++;
				if (target_index == program_.ntargets)
					return true;
			}

			off = att_addlength_pointer(off, att->attlen, tp + off);

			if (usecache && att->attlen <= 0)
				usecache = false;
		}

		return false;
	}

private:
	static bool
	store_target(const DeformTarget &target,
				 const CompactAttribute *att,
				 const char *ptr,
				 std::uint16_t row,
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
			case DeformDecodeKind::kString128:
				return varlena_to_string128(DatumGetPointer(value),
											&static_cast<PgVecStringConst *>(dst)[row]);
		}

		return false;
	}

	TupleDesc	desc_;
	DeformProgram program_;
};

} /* namespace pg_vec */

#endif

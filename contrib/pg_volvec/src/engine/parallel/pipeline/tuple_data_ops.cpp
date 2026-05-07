/*
 * pipeline/tuple_data_ops.cpp
 *
 * Stateless row<->chunk codec + hash/match/agg-update primitives. Spec:
 * .sisyphus/plans/3g2-tuple-data-collection-design.md §3.2, §4, §10 step 2.
 *
 * Layout invariant note: TupleDataLayout advances row offsets by 8 bytes
 * per slot regardless of TdcColumnDesc::width (file-header policy in
 * tuple_data_layout.cpp). Scatter writes only `col.width` bytes at
 * `col.offset` (the trailing pad bytes stay zero from dsa_allocate0).
 * HashGroup / MatchGroup also operate on `col.width` bytes — never on the
 * 8-byte slot — so int32 group cols hash 4 bytes, not 4 bytes + 4 zeros.
 * This is critical for Q1 group-by (returnflag,linestatus) being two int32
 * cols: hashing the slot would still be deterministic, but matching
 * against a freshly-zeroed dst row would falsely accept any 4-byte int32
 * value — so we keep both consistently width-bound.
 */

#include "tuple_data_ops.hpp"
#include "tuple_data_layout.hpp"
#include "parallel/pipeline/types.hpp"
#include "core/data_chunk.hpp"

extern "C" {
#include "postgres.h"
#include "utils/elog.h"
}

#include <cstring>
#include <cstdint>

namespace pg_volvec {
namespace pipeline {

namespace {

static inline uint64_t
Mix64(uint64_t v)
{
	v ^= v >> 33;
	v *= UINT64CONST(0xff51afd7ed558ccd);
	v ^= v >> 33;
	v *= UINT64CONST(0xc4ceb9fe1a85ec53);
	v ^= v >> 33;
	return v;
}

static inline uint64_t
HashStart()
{
	return UINT64CONST(0x9ae16a3b2f90404f);
}

static inline uint64_t
HashCombine(uint64_t hash, uint64_t value, uint16_t column_index)
{
	const uint64_t lane = value +
		(UINT64CONST(0x9e3779b97f4a7c15) * static_cast<uint64_t>(column_index + 1));
	return Mix64(hash ^ Mix64(lane));
}

static inline bool
IsQ1Agg(const TdcAggregateDesc &agg,
        TdcAggKind kind,
        uint16_t src_col_idx,
        uint16_t offset,
        uint16_t width,
        int16_t numeric_scale)
{
	return agg.kind == kind &&
		agg.src_col_idx == src_col_idx &&
		agg.offset == offset &&
		agg.width == width &&
		agg.numeric_scale == numeric_scale;
}

static inline void
AddInt64At(uint8_t *row_ptr, uint16_t offset, int64_t add)
{
	int64_t acc;
	std::memcpy(&acc, row_ptr + offset, sizeof(acc));
	acc += add;
	std::memcpy(row_ptr + offset, &acc, sizeof(acc));
}

static inline void
UpdateAvgNumericAt(uint8_t *row_ptr, uint16_t offset, int64_t add)
{
	int64_t acc_sum;
	int64_t acc_cnt;
	std::memcpy(&acc_sum, row_ptr + offset, sizeof(acc_sum));
	std::memcpy(&acc_cnt, row_ptr + offset + 8, sizeof(acc_cnt));
	acc_sum += add;
	acc_cnt += 1;
	std::memcpy(row_ptr + offset, &acc_sum, sizeof(acc_sum));
	std::memcpy(row_ptr + offset + 8, &acc_cnt, sizeof(acc_cnt));
}

static inline void
UpdateCanonicalQ1Aggregates(uint8_t *row_ptr,
                            const PipelineChunk &chunk,
                            uint16_t row_idx)
{
	const int64_t qty = chunk.int64_columns[0][row_idx];
	const int64_t base_price = chunk.int64_columns[1][row_idx];
	const int64_t discount = chunk.int64_columns[2][row_idx];
	const int64_t disc_price = chunk.int64_columns[5][row_idx];
	const int64_t charge = chunk.int64_columns[7][row_idx];

	AddInt64At(row_ptr, 16, qty);
	AddInt64At(row_ptr, 24, base_price);
	AddInt64At(row_ptr, 32, disc_price);
	AddInt64At(row_ptr, 40, charge);
	UpdateAvgNumericAt(row_ptr, 48, qty);
	UpdateAvgNumericAt(row_ptr, 64, base_price);
	UpdateAvgNumericAt(row_ptr, 80, discount);
	AddInt64At(row_ptr, 96, 1);
}

struct Q1AggDelta
{
	uint8_t *row_ptr = nullptr;
	int64_t sum_qty = 0;
	int64_t sum_base_price = 0;
	int64_t sum_disc_price = 0;
	int64_t sum_charge = 0;
	int64_t sum_discount = 0;
	int64_t count = 0;
};

static inline void
AccumulateQ1Delta(Q1AggDelta &delta,
                  const PipelineChunk &chunk,
                  uint16_t row_idx)
{
	const int64_t qty = chunk.int64_columns[0][row_idx];
	const int64_t base_price = chunk.int64_columns[1][row_idx];
	delta.sum_qty += qty;
	delta.sum_base_price += base_price;
	delta.sum_disc_price += chunk.int64_columns[5][row_idx];
	delta.sum_charge += chunk.int64_columns[7][row_idx];
	delta.sum_discount += chunk.int64_columns[2][row_idx];
	delta.count += 1;
}

static inline void
ApplyQ1Delta(const Q1AggDelta &delta)
{
	AddInt64At(delta.row_ptr, 16, delta.sum_qty);
	AddInt64At(delta.row_ptr, 24, delta.sum_base_price);
	AddInt64At(delta.row_ptr, 32, delta.sum_disc_price);
	AddInt64At(delta.row_ptr, 40, delta.sum_charge);
	AddInt64At(delta.row_ptr, 48, delta.sum_qty);
	AddInt64At(delta.row_ptr, 56, delta.count);
	AddInt64At(delta.row_ptr, 64, delta.sum_base_price);
	AddInt64At(delta.row_ptr, 72, delta.count);
	AddInt64At(delta.row_ptr, 80, delta.sum_discount);
	AddInt64At(delta.row_ptr, 88, delta.count);
	AddInt64At(delta.row_ptr, 96, delta.count);
}

static void
UpdateCanonicalQ1AggregatesBatch(uint8_t **row_ptrs,
                                 const PipelineChunk &chunk,
                                 const uint16_t *row_indices,
                                 uint16_t count)
{
	Q1AggDelta deltas[PIPELINE_DEFAULT_CHUNK_SIZE];
	uint16_t delta_count = 0;

	for (uint16_t i = 0; i < count; ++i)
	{
		Assert(row_ptrs[i] != nullptr);
		Assert(row_indices[i] < chunk.count);
		uint16_t delta_idx = 0;
		for (; delta_idx < delta_count; ++delta_idx)
		{
			if (deltas[delta_idx].row_ptr == row_ptrs[i])
				break;
		}
		if (delta_idx == delta_count)
		{
			deltas[delta_idx].row_ptr = row_ptrs[i];
			++delta_count;
		}
		AccumulateQ1Delta(deltas[delta_idx], chunk, row_indices[i]);
	}

	for (uint16_t i = 0; i < delta_count; ++i)
		ApplyQ1Delta(deltas[i]);
}

static inline bool
MatchCanonicalQ1GroupRow(const uint8_t *row_a, const uint8_t *row_b)
{
	uint32_t a0, a1, b0, b1;
	std::memcpy(&a0, row_a, sizeof(a0));
	std::memcpy(&a1, row_a + 8, sizeof(a1));
	std::memcpy(&b0, row_b, sizeof(b0));
	std::memcpy(&b1, row_b + 8, sizeof(b1));
	return a0 == b0 && a1 == b1;
}

static inline bool
MatchCanonicalQ1Group(const uint8_t *row_ptr,
                      const PipelineChunk &chunk,
                      uint16_t row_idx)
{
	uint32_t row0, row1;
	std::memcpy(&row0, row_ptr, sizeof(row0));
	std::memcpy(&row1, row_ptr + 8, sizeof(row1));
	return row0 == static_cast<uint32_t>(chunk.int32_columns[0][row_idx]) &&
		row1 == static_cast<uint32_t>(chunk.int32_columns[1][row_idx]);
}

}  /* namespace */

bool
IsCanonicalQ1HashAggLayout(const TupleDataLayout *layout)
{
	if (layout == nullptr || layout->validity_width != 0 ||
		layout->column_count != 2 || layout->aggregate_count != 8 ||
		layout->row_width != 104)
		return false;

	const TdcColumnDesc &col0 = layout->columns[0];
	const TdcColumnDesc &col1 = layout->columns[1];
	if (col0.kind != TdcColumnKind::INT32 || col0.offset != 0 ||
		col0.width != 4 || col0.numeric_scale != 0)
		return false;
	if (col1.kind != TdcColumnKind::INT32 || col1.offset != 8 ||
		col1.width != 4 || col1.numeric_scale != 0)
		return false;

	const TdcAggregateDesc *aggs = layout->aggregates;
	return IsQ1Agg(aggs[0], TdcAggKind::SUM_NUMERIC, 0, 16, 8, 2) &&
		IsQ1Agg(aggs[1], TdcAggKind::SUM_NUMERIC, 1, 24, 8, 2) &&
		IsQ1Agg(aggs[2], TdcAggKind::SUM_NUMERIC, 5, 32, 8, 4) &&
		IsQ1Agg(aggs[3], TdcAggKind::SUM_NUMERIC, 7, 40, 8, 6) &&
		IsQ1Agg(aggs[4], TdcAggKind::AVG_NUMERIC, 0, 48, 16, 2) &&
		IsQ1Agg(aggs[5], TdcAggKind::AVG_NUMERIC, 1, 64, 16, 2) &&
		IsQ1Agg(aggs[6], TdcAggKind::AVG_NUMERIC, 2, 80, 16, 2) &&
		IsQ1Agg(aggs[7], TdcAggKind::COUNT_STAR, 0, 96, 8, 0);
}

static inline void
ScatterGroupColumns(const TupleDataLayout *layout,
                    uint8_t *row_ptr,
                    const PipelineChunk &chunk,
                    uint16_t row_idx)
{
	for (uint16_t i = 0; i < layout->column_count; ++i)
	{
		const TdcColumnDesc &col = layout->columns[i];
		Assert(i < 16);
		Assert(col.width == 4 || col.width == 8);

		switch (col.kind)
		{
			case TdcColumnKind::INT32:
			{
				int32_t v = chunk.int32_columns[i][row_idx];
				std::memcpy(row_ptr + col.offset, &v, sizeof(v));
				break;
			}
			case TdcColumnKind::INT64:
			{
				int64_t v = chunk.int64_columns[i][row_idx];
				std::memcpy(row_ptr + col.offset, &v, sizeof(v));
				break;
			}
			case TdcColumnKind::DOUBLE:
			{
				double v = chunk.double_columns[i][row_idx];
				std::memcpy(row_ptr + col.offset, &v, sizeof(v));
				break;
			}
		}
	}
}

void
ScatterGroupOnly(const TupleDataLayout *layout,
                 uint8_t *row_ptr,
                 const PipelineChunk &chunk,
                 uint16_t row_idx)
{
	Assert(layout != nullptr && row_ptr != nullptr);
	Assert(row_idx < chunk.count);
	ScatterGroupColumns(layout, row_ptr, chunk, row_idx);
}

void
Scatter(const TupleDataLayout *layout,
        uint8_t *row_ptr,
        const PipelineChunk &chunk,
        uint16_t row_idx)
{
	Assert(layout != nullptr && row_ptr != nullptr);
	Assert(row_idx < chunk.count);

	ScatterGroupColumns(layout, row_ptr, chunk, row_idx);

	const uint16_t agg_chunk_base = layout->column_count;
	for (uint16_t a = 0; a < layout->aggregate_count; ++a)
	{
		const TdcAggregateDesc &agg = layout->aggregates[a];
		int64_t v = chunk.int64_columns[agg_chunk_base + a][row_idx];
		std::memcpy(row_ptr + agg.offset, &v, sizeof(v));
		if (agg.kind == TdcAggKind::AVG_NUMERIC)
		{
			const int64_t one = 1;
			std::memcpy(row_ptr + agg.offset + 8, &one, sizeof(one));
		}
	}
}

void
Gather(const TupleDataLayout *layout,
       const uint8_t *row_ptr,
       PipelineChunk &chunk,
       uint16_t row_idx)
{
	Assert(layout != nullptr && row_ptr != nullptr);
	Assert(layout->column_count + layout->aggregate_count <= 16);

	/* Group/payload columns: chunk col i = row col i. */
	for (uint16_t i = 0; i < layout->column_count; ++i)
	{
		const TdcColumnDesc &col = layout->columns[i];

		switch (col.kind)
		{
			case TdcColumnKind::INT32:
			{
				int32_t v;
				std::memcpy(&v, row_ptr + col.offset, sizeof(v));
				chunk.int32_columns[i][row_idx] = v;
				break;
			}
			case TdcColumnKind::INT64:
			{
				int64_t v;
				std::memcpy(&v, row_ptr + col.offset, sizeof(v));
				chunk.int64_columns[i][row_idx] = v;
				break;
			}
			case TdcColumnKind::DOUBLE:
			{
				double v;
				std::memcpy(&v, row_ptr + col.offset, sizeof(v));
				chunk.double_columns[i][row_idx] = v;
				break;
			}
		}
	}

	/* Aggregate state columns: chunk col N+a = aggregate a (always int64). */
	const uint16_t agg_chunk_base = layout->column_count;
	for (uint16_t a = 0; a < layout->aggregate_count; ++a)
	{
		const TdcAggregateDesc &agg = layout->aggregates[a];
		int64_t v;
		std::memcpy(&v, row_ptr + agg.offset, sizeof(v));

		/* AVG_NUMERIC stores {sum:int64, count:int64} as a 16B pair; the
		 * downstream chunk only has one slot per aggregate, and Scatter only
		 * round-trips the first 8B. Finalize the average here so the count
		 * half is consumed at the producer (HashAgg) where it is still
		 * authoritative, instead of trying to thread a second chunk slot
		 * through Order/Output. Result is a scale=2 scaled-int64; div-by-zero
		 * becomes 0 to match SQL's "no rows produces NULL" only at the
		 * EncodeColumn boundary - here we still emit 0, and OutputSink turns
		 * the NULL signal off via tts_isnull (currently hardcoded false; if we
		 * later add NULL tracking the count==0 case is the place to flip it). */
		if (agg.kind == TdcAggKind::AVG_NUMERIC)
		{
			int64_t count;
			std::memcpy(&count, row_ptr + agg.offset + 8, sizeof(count));
			v = (count != 0) ? (v / count) : 0;
		}

		chunk.int64_columns[agg_chunk_base + a][row_idx] = v;
	}
}

uint64_t
HashGroup(const TupleDataLayout *layout,
          const PipelineChunk &chunk,
          uint16_t row_idx)
{
	Assert(layout != nullptr);
	Assert(row_idx < chunk.count);

	uint64_t h = HashStart();

	for (uint16_t i = 0; i < layout->column_count; ++i)
	{
		const TdcColumnDesc &col = layout->columns[i];
		Assert(i < 16);

		uint64_t value = 0;
		switch (col.kind)
		{
			case TdcColumnKind::INT32:
			{
				value = static_cast<uint32_t>(chunk.int32_columns[i][row_idx]);
				break;
			}
			case TdcColumnKind::INT64:
			{
				value = static_cast<uint64_t>(chunk.int64_columns[i][row_idx]);
				break;
			}
			case TdcColumnKind::DOUBLE:
			{
				double v = chunk.double_columns[i][row_idx];
				std::memcpy(&value, &v, sizeof(value));
				break;
			}
		}
		h = HashCombine(h, value, i);
	}
	return Mix64(h ^ static_cast<uint64_t>(layout->column_count));
}

uint64_t
HashGroupRow(const TupleDataLayout *layout,
             const uint8_t *row_ptr)
{
	Assert(layout != nullptr && row_ptr != nullptr);

	uint64_t h = HashStart();
	for (uint16_t i = 0; i < layout->column_count; ++i)
	{
		const TdcColumnDesc &col = layout->columns[i];
		Assert(i < 16);
		Assert(col.width == 4 || col.width == 8);

		uint64_t value = 0;
		switch (col.kind)
		{
			case TdcColumnKind::INT32:
			{
				int32_t v;
				std::memcpy(&v, row_ptr + col.offset, sizeof(v));
				value = static_cast<uint32_t>(v);
				break;
			}
			case TdcColumnKind::INT64:
			{
				int64_t v;
				std::memcpy(&v, row_ptr + col.offset, sizeof(v));
				value = static_cast<uint64_t>(v);
				break;
			}
			case TdcColumnKind::DOUBLE:
				std::memcpy(&value, row_ptr + col.offset, sizeof(value));
				break;
		}
		h = HashCombine(h, value, i);
	}
	return Mix64(h ^ static_cast<uint64_t>(layout->column_count));
}

bool
MatchGroup(const TupleDataLayout *layout,
           const uint8_t *row_ptr,
           const PipelineChunk &chunk,
           uint16_t row_idx)
{
	Assert(layout != nullptr && row_ptr != nullptr);
	Assert(row_idx < chunk.count);
	if (IsCanonicalQ1HashAggLayout(layout))
		return MatchCanonicalQ1Group(row_ptr, chunk, row_idx);

	for (uint16_t i = 0; i < layout->column_count; ++i)
	{
		const TdcColumnDesc &col = layout->columns[i];
		Assert(i < 16);

		switch (col.kind)
		{
			case TdcColumnKind::INT32:
			{
				int32_t row_v;
				std::memcpy(&row_v, row_ptr + col.offset, sizeof(row_v));
				if (row_v != chunk.int32_columns[i][row_idx])
					return false;
				break;
			}
			case TdcColumnKind::INT64:
			{
				int64_t row_v;
				std::memcpy(&row_v, row_ptr + col.offset, sizeof(row_v));
				if (row_v != chunk.int64_columns[i][row_idx])
					return false;
				break;
			}
			case TdcColumnKind::DOUBLE:
			{
				double row_v;
				std::memcpy(&row_v, row_ptr + col.offset, sizeof(row_v));
				/* Bitwise equality (memcmp) on doubles — caller must ensure
				 * both sides came through Scatter with no canonicalization
				 * (Q1 group keys are int, so this branch is unused there). */
				double chunk_v = chunk.double_columns[i][row_idx];
				if (std::memcmp(&row_v, &chunk_v, sizeof(double)) != 0)
					return false;
				break;
			}
		}
	}
	return true;
}

bool
MatchGroupRow(const TupleDataLayout *layout,
              const uint8_t *row_a,
              const uint8_t *row_b)
{
	Assert(layout != nullptr && row_a != nullptr && row_b != nullptr);
	if (IsCanonicalQ1HashAggLayout(layout))
		return MatchCanonicalQ1GroupRow(row_a, row_b);

	for (uint16_t i = 0; i < layout->column_count; ++i)
	{
		const TdcColumnDesc &col = layout->columns[i];
		Assert(i < 16);
		Assert(col.width == 4 || col.width == 8);

		if (std::memcmp(row_a + col.offset,
		                row_b + col.offset,
		                col.width) != 0)
			return false;
	}
	return true;
}

void
UpdateAggregates(const TupleDataLayout *layout,
                 uint8_t *row_ptr,
                 const PipelineChunk &chunk,
                 uint16_t row_idx)
{
	Assert(layout != nullptr && row_ptr != nullptr);
	Assert(row_idx < chunk.count);
	if (IsCanonicalQ1HashAggLayout(layout))
	{
		UpdateCanonicalQ1Aggregates(row_ptr, chunk, row_idx);
		return;
	}

	for (uint16_t a = 0; a < layout->aggregate_count; ++a)
	{
		const TdcAggregateDesc &agg = layout->aggregates[a];

		switch (agg.kind)
		{
			case TdcAggKind::SUM_INT64:
			{
				Assert(agg.src_col_idx < 16);
				int64_t add = chunk.int64_columns[agg.src_col_idx][row_idx];
				int64_t acc;
				std::memcpy(&acc, row_ptr + agg.offset, sizeof(acc));
				acc += add;
				std::memcpy(row_ptr + agg.offset, &acc, sizeof(acc));
				break;
			}
			case TdcAggKind::COUNT_STAR:
			{
				int64_t acc;
				std::memcpy(&acc, row_ptr + agg.offset, sizeof(acc));
				acc += 1;
				std::memcpy(row_ptr + agg.offset, &acc, sizeof(acc));
				break;
			}
			case TdcAggKind::SUM_NUMERIC:
			{
				Assert(agg.src_col_idx < 16);
				int64_t add = chunk.int64_columns[agg.src_col_idx][row_idx];
				int64_t acc;
				std::memcpy(&acc, row_ptr + agg.offset, sizeof(acc));
				acc += add;
				std::memcpy(row_ptr + agg.offset, &acc, sizeof(acc));
				break;
			}
			case TdcAggKind::AVG_NUMERIC:
			{
				Assert(agg.src_col_idx < 16);
				int64_t add = chunk.int64_columns[agg.src_col_idx][row_idx];
				int64_t acc_sum;
				int64_t acc_cnt;
				std::memcpy(&acc_sum, row_ptr + agg.offset, sizeof(acc_sum));
				std::memcpy(&acc_cnt, row_ptr + agg.offset + 8, sizeof(acc_cnt));
				acc_sum += add;
				acc_cnt += 1;
				std::memcpy(row_ptr + agg.offset, &acc_sum, sizeof(acc_sum));
				std::memcpy(row_ptr + agg.offset + 8, &acc_cnt, sizeof(acc_cnt));
				break;
			}
		}
	}
}

void
UpdateAggregatesBatch(const TupleDataLayout *layout,
                      uint8_t **row_ptrs,
                      const PipelineChunk &chunk,
                      const uint16_t *row_indices,
                      uint16_t count)
{
	Assert(layout != nullptr && row_ptrs != nullptr);
	Assert(row_indices != nullptr);
	Assert(count <= chunk.count);

	if (IsCanonicalQ1HashAggLayout(layout))
	{
		UpdateCanonicalQ1AggregatesBatch(row_ptrs, chunk, row_indices, count);
		return;
	}

	for (uint16_t i = 0; i < count; ++i)
	{
		Assert(row_ptrs[i] != nullptr);
		Assert(row_indices[i] < chunk.count);
		UpdateAggregates(layout, row_ptrs[i], chunk, row_indices[i]);
	}
}

void
CombineAggregates(const TupleDataLayout *layout,
                  uint8_t *dst_row,
                  const uint8_t *src_row)
{
	Assert(layout != nullptr && dst_row != nullptr && src_row != nullptr);

	for (uint16_t a = 0; a < layout->aggregate_count; ++a)
	{
		const TdcAggregateDesc &agg = layout->aggregates[a];
		switch (agg.kind)
		{
			case TdcAggKind::SUM_INT64:
			case TdcAggKind::COUNT_STAR:
			{
				int64_t dst_v;
				int64_t src_v;
				std::memcpy(&dst_v, dst_row + agg.offset, sizeof(dst_v));
				std::memcpy(&src_v, src_row + agg.offset, sizeof(src_v));
				dst_v += src_v;
				std::memcpy(dst_row + agg.offset, &dst_v, sizeof(dst_v));
				break;
			}
			case TdcAggKind::SUM_NUMERIC:
			{
				int64_t dst_v;
				int64_t src_v;
				std::memcpy(&dst_v, dst_row + agg.offset, sizeof(dst_v));
				std::memcpy(&src_v, src_row + agg.offset, sizeof(src_v));
				dst_v += src_v;
				std::memcpy(dst_row + agg.offset, &dst_v, sizeof(dst_v));
				break;
			}
			case TdcAggKind::AVG_NUMERIC:
			{
				int64_t dst_sum, dst_cnt, src_sum, src_cnt;
				std::memcpy(&dst_sum, dst_row + agg.offset, sizeof(dst_sum));
				std::memcpy(&dst_cnt, dst_row + agg.offset + 8, sizeof(dst_cnt));
				std::memcpy(&src_sum, src_row + agg.offset, sizeof(src_sum));
				std::memcpy(&src_cnt, src_row + agg.offset + 8, sizeof(src_cnt));
				dst_sum += src_sum;
				dst_cnt += src_cnt;
				std::memcpy(dst_row + agg.offset, &dst_sum, sizeof(dst_sum));
				std::memcpy(dst_row + agg.offset + 8, &dst_cnt, sizeof(dst_cnt));
				break;
			}
		}
	}

	/* Suppress -Wunused-but-set-variable if asserts compiled out. */
	(void) layout;
}

}  /* namespace pipeline */
}  /* namespace pg_volvec */

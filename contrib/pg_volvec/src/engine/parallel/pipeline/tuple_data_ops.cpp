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

	/* FNV-1a 64-bit. */
	constexpr uint64_t FNV_OFFSET = 0xcbf29ce484222325ULL;
	constexpr uint64_t FNV_PRIME  = 0x100000001b3ULL;
	uint64_t h = FNV_OFFSET;

	for (uint16_t i = 0; i < layout->column_count; ++i)
	{
		const TdcColumnDesc &col = layout->columns[i];
		Assert(i < 16);

		uint8_t bytes[8];
		switch (col.kind)
		{
			case TdcColumnKind::INT32:
			{
				int32_t v = chunk.int32_columns[i][row_idx];
				std::memcpy(bytes, &v, 4);
				break;
			}
			case TdcColumnKind::INT64:
			{
				int64_t v = chunk.int64_columns[i][row_idx];
				std::memcpy(bytes, &v, 8);
				break;
			}
			case TdcColumnKind::DOUBLE:
			{
				double v = chunk.double_columns[i][row_idx];
				std::memcpy(bytes, &v, 8);
				break;
			}
		}

		for (uint16_t b = 0; b < col.width; ++b)
		{
			h ^= bytes[b];
			h *= FNV_PRIME;
		}
	}
	return h;
}

bool
MatchGroup(const TupleDataLayout *layout,
           const uint8_t *row_ptr,
           const PipelineChunk &chunk,
           uint16_t row_idx)
{
	Assert(layout != nullptr && row_ptr != nullptr);
	Assert(row_idx < chunk.count);

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

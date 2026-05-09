/*
 * pipeline/tuple_data_ops.cpp
 *
 * Stateless row<->chunk codec + hash/match/agg-update primitives. Spec:
 * .sisyphus/plans/3g2-tuple-data-collection-design.md §3.2, §4, §10 step 2.
 *
 * Layout invariant note: TupleDataLayout advances row offsets by each
 * column's aligned physical width. Scatter writes the exact physical value at
 * `col.offset` (including 16-byte VecStringRef values). HashGroup / MatchGroup
 * use typed comparison for STRING_REF and width-bound byte comparison for
 * fixed-width values.
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
	v ^= v >> 30;
	v *= UINT64CONST(0xbf58476d1ce4e5b9);
	v ^= v >> 27;
	v *= UINT64CONST(0x94d049bb133111eb);
	v ^= v >> 31;
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

static inline void
AddInt64At(uint8_t *row_ptr, uint16_t offset, int64_t add)
{
	int64_t acc;
	std::memcpy(&acc, row_ptr + offset, sizeof(acc));
	acc += add;
	std::memcpy(row_ptr + offset, &acc, sizeof(acc));
}

struct AggDelta
{
	uint8_t *row_ptr = nullptr;
	int64_t values[TUPLE_DATA_MAX_COLUMNS] = {0};
	int64_t counts[TUPLE_DATA_MAX_COLUMNS] = {0};
};

static inline void
AccumulateAggDelta(const TupleDataLayout *layout,
                   AggDelta &delta,
                   const PipelineChunk &chunk,
                   uint16_t row_idx)
{
	for (uint16_t a = 0; a < layout->aggregate_count; ++a)
	{
		const TdcAggregateDesc &agg = layout->aggregates[a];
		switch (agg.kind)
		{
			case TdcAggKind::SUM_INT64:
			case TdcAggKind::SUM_NUMERIC:
				Assert(agg.src_col_idx < 16);
				delta.values[a] += chunk.int64_columns[agg.src_col_idx][row_idx];
				break;
			case TdcAggKind::COUNT_STAR:
				delta.values[a] += 1;
				break;
			case TdcAggKind::AVG_NUMERIC:
				Assert(agg.src_col_idx < 16);
				delta.values[a] += chunk.int64_columns[agg.src_col_idx][row_idx];
				delta.counts[a] += 1;
				break;
		}
	}
}

static inline void
ApplyAggDelta(const TupleDataLayout *layout, const AggDelta &delta)
{
	for (uint16_t a = 0; a < layout->aggregate_count; ++a)
	{
		const TdcAggregateDesc &agg = layout->aggregates[a];
		AddInt64At(delta.row_ptr, agg.offset, delta.values[a]);
		if (agg.kind == TdcAggKind::AVG_NUMERIC)
			AddInt64At(delta.row_ptr, agg.offset + 8, delta.counts[a]);
	}
}

static void
UpdateAggregatesBatchGroupedGeneric(const TupleDataLayout *layout,
                                    uint8_t **row_ptrs,
                                    const PipelineChunk &chunk,
                                    const uint16_t *row_indices,
                                    uint16_t count)
{
	AggDelta deltas[PIPELINE_DEFAULT_CHUNK_SIZE];
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
		AccumulateAggDelta(layout, deltas[delta_idx], chunk, row_indices[i]);
	}

	for (uint16_t i = 0; i < delta_count; ++i)
		ApplyAggDelta(layout, deltas[i]);
}

static void
UpdateAggregatesGatherGroupedGeneric(const TupleDataLayout *layout,
                                     uint8_t *tdc_base,
                                     uint32_t row_width,
                                     const uint32_t *canonical_row_indices,
                                     const PipelineChunk &chunk,
                                     const uint16_t *row_indices,
                                     uint16_t count)
{
	AggDelta deltas[PIPELINE_DEFAULT_CHUNK_SIZE];
	uint32_t delta_row_indices[PIPELINE_DEFAULT_CHUNK_SIZE];
	uint16_t delta_count = 0;

	for (uint16_t i = 0; i < count; ++i)
	{
		Assert(row_indices[i] < chunk.count);
		const uint32_t canonical_idx = canonical_row_indices[i];
		uint16_t delta_idx = 0;
		for (; delta_idx < delta_count; ++delta_idx)
		{
			if (delta_row_indices[delta_idx] == canonical_idx)
				break;
		}
		if (delta_idx == delta_count)
		{
			delta_row_indices[delta_idx] = canonical_idx;
			deltas[delta_idx].row_ptr = tdc_base + static_cast<size_t>(canonical_idx) * row_width;
			++delta_count;
		}
		AccumulateAggDelta(layout, deltas[delta_idx], chunk, row_indices[i]);
	}

	for (uint16_t i = 0; i < delta_count; ++i)
		ApplyAggDelta(layout, deltas[i]);
}

}  /* namespace */

static inline void
ScatterGroupColumns(const TupleDataLayout *layout,
                    TupleDataCollection *tdc,
                    uint8_t *row_ptr,
                    const PipelineChunk &chunk,
                    uint16_t row_idx)
{
	for (uint16_t i = 0; i < layout->column_count; ++i)
	{
		const TdcColumnDesc &col = layout->columns[i];
		Assert(col.src_col_idx < 16);

			switch (col.kind)
			{
				case TdcColumnKind::INT32:
			{
				int32_t v = chunk.int32_columns[col.src_col_idx][row_idx];
				std::memcpy(row_ptr + col.offset, &v, sizeof(v));
				break;
			}
			case TdcColumnKind::INT64:
			{
				int64_t v = chunk.int64_columns[col.src_col_idx][row_idx];
				std::memcpy(row_ptr + col.offset, &v, sizeof(v));
				break;
			}
				case TdcColumnKind::DOUBLE:
				{
					double v = chunk.double_columns[col.src_col_idx][row_idx];
					std::memcpy(row_ptr + col.offset, &v, sizeof(v));
					break;
				}
				case TdcColumnKind::STRING_REF:
				{
					const VecStringRef &src = chunk.string_columns[col.src_col_idx][row_idx];
					const char *ptr = VecStringRefDataPtr(src,
						reinterpret_cast<const char *>(chunk.string_arena.data()));
					VecStringRef dst;
					if ((ptr == nullptr && src.len != 0) || tdc == nullptr ||
						!TupleDataCollectionStoreStringBytes(tdc, ptr, src.len, &dst))
						elog(ERROR,
							"pg_volvec: STRING_REF row-store scatter failed (slot=%u row=%u len=%u offset=%u inline=%d arena=%zu heap_used=%u heap_capacity=%u)",
							static_cast<unsigned>(col.src_col_idx),
							static_cast<unsigned>(row_idx),
							src.len,
							src.offset,
							VecStringRefIsInline(src) ? 1 : 0,
							chunk.string_arena.size(),
							tdc != nullptr ? pg_atomic_read_u32(&tdc->heap_used) : 0,
							tdc != nullptr ? tdc->heap_capacity : 0);
					std::memcpy(row_ptr + col.offset, &dst, sizeof(dst));
					break;
				}
			}
	}
}

void
ScatterGroupOnly(const TupleDataLayout *layout,
                 TupleDataCollection *tdc,
                 uint8_t *row_ptr,
                 const PipelineChunk &chunk,
                 uint16_t row_idx)
{
	Assert(layout != nullptr && row_ptr != nullptr);
	Assert(row_idx < chunk.count);
	ScatterGroupColumns(layout, tdc, row_ptr, chunk, row_idx);
}

void
Scatter(const TupleDataLayout *layout,
        TupleDataCollection *tdc,
        uint8_t *row_ptr,
        const PipelineChunk &chunk,
        uint16_t row_idx)
{
	Assert(layout != nullptr && row_ptr != nullptr);
	Assert(row_idx < chunk.count);

	ScatterGroupColumns(layout, tdc, row_ptr, chunk, row_idx);

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
       const TupleDataCollection *tdc,
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
		Assert(col.src_col_idx < 16);

			switch (col.kind)
			{
				case TdcColumnKind::INT32:
			{
				int32_t v;
				std::memcpy(&v, row_ptr + col.offset, sizeof(v));
				chunk.int32_columns[col.src_col_idx][row_idx] = v;
				break;
			}
			case TdcColumnKind::INT64:
			{
				int64_t v;
				std::memcpy(&v, row_ptr + col.offset, sizeof(v));
				chunk.int64_columns[col.src_col_idx][row_idx] = v;
				break;
			}
				case TdcColumnKind::DOUBLE:
				{
					double v;
					std::memcpy(&v, row_ptr + col.offset, sizeof(v));
					chunk.double_columns[col.src_col_idx][row_idx] = v;
					break;
				}
				case TdcColumnKind::STRING_REF:
				{
					VecStringRef v;
					std::memcpy(&v, row_ptr + col.offset, sizeof(v));
					const char *ptr = VecStringRefDataPtr(v,
						tdc != nullptr ? reinterpret_cast<const char *>(TupleDataCollectionHeapConst(tdc)) : nullptr);
					if (ptr == nullptr && v.len != 0)
						elog(ERROR, "pg_volvec: STRING_REF gather missing backing storage");
					chunk.string_columns[col.src_col_idx][row_idx] =
						chunk.store_string_bytes(ptr, v.len);
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
		Assert(col.src_col_idx < 16);

		uint64_t value = 0;
			switch (col.kind)
			{
				case TdcColumnKind::INT32:
			{
				value = static_cast<uint32_t>(chunk.int32_columns[col.src_col_idx][row_idx]);
				break;
			}
			case TdcColumnKind::INT64:
			{
				value = static_cast<uint64_t>(chunk.int64_columns[col.src_col_idx][row_idx]);
				break;
			}
			case TdcColumnKind::DOUBLE:
			{
				double v = chunk.double_columns[col.src_col_idx][row_idx];
				std::memcpy(&value, &v, sizeof(value));
				break;
			}
				case TdcColumnKind::STRING_REF:
				{
					const VecStringRef &ref = chunk.string_columns[col.src_col_idx][row_idx];
					const char *ptr = chunk.get_string_ptr(ref);
					if (ptr == nullptr && ref.len != 0)
						elog(ERROR, "pg_volvec: STRING_REF chunk hashing missing arena backing");
					value = Mix64(ref.prefix ^ static_cast<uint64_t>(ref.len));
					for (uint32_t byte_idx = 0; byte_idx < ref.len; ++byte_idx)
						value = HashCombine(value,
							static_cast<unsigned char>(ptr[byte_idx]),
							static_cast<uint16_t>(i + byte_idx));
					break;
				}
		}
		h = HashCombine(h, value, i);
	}
	return Mix64(h ^ static_cast<uint64_t>(layout->column_count));
}

uint64_t
HashGroupRow(const TupleDataLayout *layout,
             const TupleDataCollection *tdc,
             const uint8_t *row_ptr)
{
	Assert(layout != nullptr && row_ptr != nullptr);

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
				case TdcColumnKind::STRING_REF:
				{
					VecStringRef ref;
					std::memcpy(&ref, row_ptr + col.offset, sizeof(ref));
					const char *ptr = VecStringRefDataPtr(ref,
						tdc != nullptr ? reinterpret_cast<const char *>(TupleDataCollectionHeapConst(tdc)) : nullptr);
					if (ptr == nullptr && ref.len != 0)
						elog(ERROR, "pg_volvec: STRING_REF row-store hashing missing heap backing");
					value = Mix64(ref.prefix ^ static_cast<uint64_t>(ref.len));
					for (uint32_t byte_idx = 0; byte_idx < ref.len; ++byte_idx)
						value = HashCombine(value, static_cast<unsigned char>(ptr[byte_idx]), static_cast<uint16_t>(i + byte_idx));
					break;
				}
			}
		h = HashCombine(h, value, i);
	}
	return Mix64(h ^ static_cast<uint64_t>(layout->column_count));
}

bool
MatchGroup(const TupleDataLayout *layout,
           const TupleDataCollection *tdc,
           const uint8_t *row_ptr,
           const PipelineChunk &chunk,
           uint16_t row_idx)
{
	Assert(layout != nullptr && row_ptr != nullptr);
	Assert(row_idx < chunk.count);

	for (uint16_t i = 0; i < layout->column_count; ++i)
	{
		const TdcColumnDesc &col = layout->columns[i];
		Assert(col.src_col_idx < 16);

			switch (col.kind)
			{
				case TdcColumnKind::INT32:
			{
				int32_t row_v;
				std::memcpy(&row_v, row_ptr + col.offset, sizeof(row_v));
				if (row_v != chunk.int32_columns[col.src_col_idx][row_idx])
					return false;
				break;
			}
			case TdcColumnKind::INT64:
			{
				int64_t row_v;
				std::memcpy(&row_v, row_ptr + col.offset, sizeof(row_v));
				if (row_v != chunk.int64_columns[col.src_col_idx][row_idx])
					return false;
				break;
			}
				case TdcColumnKind::DOUBLE:
				{
					uint64_t row_v;
					uint64_t chunk_v;
					std::memcpy(&row_v, row_ptr + col.offset, sizeof(row_v));
					std::memcpy(&chunk_v, &chunk.double_columns[col.src_col_idx][row_idx], sizeof(chunk_v));
					if (row_v != chunk_v)
						return false;
				break;
			}
				case TdcColumnKind::STRING_REF:
				{
					VecStringRef row_ref = *reinterpret_cast<const VecStringRef *>(row_ptr + col.offset);
					const VecStringRef &chunk_ref = chunk.string_columns[col.src_col_idx][row_idx];
					if (row_ref.len != chunk_ref.len || row_ref.prefix != chunk_ref.prefix)
						return false;
					const char *row_ptr_data = VecStringRefDataPtr(row_ref,
						tdc != nullptr ? reinterpret_cast<const char *>(TupleDataCollectionHeapConst(tdc)) : nullptr);
					const char *chunk_ptr_data = chunk.get_string_ptr(chunk_ref);
					if (row_ref.len != 0 && (row_ptr_data == nullptr || chunk_ptr_data == nullptr))
						elog(ERROR, "pg_volvec: STRING_REF row-store match missing backing storage");
					if (row_ref.len > 8 && std::memcmp(row_ptr_data, chunk_ptr_data, row_ref.len) != 0)
						return false;
					break;
				}
			}
	}
	return true;
}

bool
MatchGroupLayouts(const TupleDataLayout *row_layout,
                  const TupleDataCollection *tdc,
                  const uint8_t *row_ptr,
                  const TupleDataLayout *chunk_layout,
                  const PipelineChunk &chunk,
                  uint16_t row_idx)
{
	Assert(row_layout != nullptr && chunk_layout != nullptr && row_ptr != nullptr);
	Assert(row_idx < chunk.count);
	if (row_layout->column_count != chunk_layout->column_count)
		return false;

	for (uint16_t i = 0; i < row_layout->column_count; ++i)
	{
		const TdcColumnDesc &row_col = row_layout->columns[i];
		const TdcColumnDesc &chunk_col = chunk_layout->columns[i];
		if (row_col.kind != chunk_col.kind || row_col.width != chunk_col.width)
			return false;
		Assert(chunk_col.src_col_idx < 16);

		switch (row_col.kind)
		{
			case TdcColumnKind::INT32:
			{
				int32_t row_v;
				std::memcpy(&row_v, row_ptr + row_col.offset, sizeof(row_v));
				if (row_v != chunk.int32_columns[chunk_col.src_col_idx][row_idx])
					return false;
				break;
			}
			case TdcColumnKind::INT64:
			{
				int64_t row_v;
				std::memcpy(&row_v, row_ptr + row_col.offset, sizeof(row_v));
				if (row_v != chunk.int64_columns[chunk_col.src_col_idx][row_idx])
					return false;
				break;
			}
			case TdcColumnKind::DOUBLE:
			{
				uint64_t row_v;
				uint64_t chunk_v;
				std::memcpy(&row_v, row_ptr + row_col.offset, sizeof(row_v));
				std::memcpy(&chunk_v, &chunk.double_columns[chunk_col.src_col_idx][row_idx], sizeof(chunk_v));
				if (row_v != chunk_v)
					return false;
				break;
			}
			case TdcColumnKind::STRING_REF:
			{
				VecStringRef row_ref;
				std::memcpy(&row_ref, row_ptr + row_col.offset, sizeof(row_ref));
				const VecStringRef &chunk_ref = chunk.string_columns[chunk_col.src_col_idx][row_idx];
				if (row_ref.len != chunk_ref.len || row_ref.prefix != chunk_ref.prefix)
					return false;
				const char *row_ptr_data = VecStringRefDataPtr(row_ref,
					tdc != nullptr ? reinterpret_cast<const char *>(TupleDataCollectionHeapConst(tdc)) : nullptr);
				const char *chunk_ptr_data = chunk.get_string_ptr(chunk_ref);
				if (row_ref.len != 0 && (row_ptr_data == nullptr || chunk_ptr_data == nullptr))
					elog(ERROR, "pg_volvec: STRING_REF cross-layout match missing backing storage");
				if (row_ref.len > 8 && std::memcmp(row_ptr_data, chunk_ptr_data, row_ref.len) != 0)
					return false;
				break;
			}
		}
	}
	return true;
}

void
MatchGroupBatch(const TupleDataLayout *layout,
                const TupleDataCollection *tdc,
                const uint8_t *const *row_ptrs,
                const PipelineChunk &chunk,
                const uint16_t *row_indices,
                uint16_t count,
                bool *matches)
{
	Assert(layout != nullptr && row_ptrs != nullptr && row_indices != nullptr && matches != nullptr);
	Assert(count <= PIPELINE_DEFAULT_CHUNK_SIZE);

	for (uint16_t i = 0; i < count; ++i)
		matches[i] = row_ptrs[i] != nullptr && MatchGroup(layout, tdc, row_ptrs[i], chunk, row_indices[i]);
}

bool
MatchGroupRow(const TupleDataLayout *layout,
              const TupleDataCollection *tdc_a,
              const uint8_t *row_a,
              const TupleDataCollection *tdc_b,
              const uint8_t *row_b)
{
	Assert(layout != nullptr && row_a != nullptr && row_b != nullptr);

	for (uint16_t i = 0; i < layout->column_count; ++i)
	{
		const TdcColumnDesc &col = layout->columns[i];
		Assert(i < 16);

		switch (col.kind)
		{
			case TdcColumnKind::STRING_REF:
			{
				VecStringRef ref_a;
				VecStringRef ref_b;
				std::memcpy(&ref_a, row_a + col.offset, sizeof(ref_a));
				std::memcpy(&ref_b, row_b + col.offset, sizeof(ref_b));
				if (ref_a.len != ref_b.len || ref_a.prefix != ref_b.prefix)
					return false;
				const char *ptr_a = VecStringRefDataPtr(ref_a,
					tdc_a != nullptr ? reinterpret_cast<const char *>(TupleDataCollectionHeapConst(tdc_a)) : nullptr);
				const char *ptr_b = VecStringRefDataPtr(ref_b,
					tdc_b != nullptr ? reinterpret_cast<const char *>(TupleDataCollectionHeapConst(tdc_b)) : nullptr);
				if (ref_a.len != 0 && (ptr_a == nullptr || ptr_b == nullptr))
					elog(ERROR, "pg_volvec: STRING_REF row-store match missing heap backing");
				if (ref_a.len > 8 && std::memcmp(ptr_a, ptr_b, ref_a.len) != 0)
					return false;
				break;
			}
			default:
				if (std::memcmp(row_a + col.offset,
				                row_b + col.offset,
				                col.width) != 0)
					return false;
				break;
		}
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
UpdateAggregatesBatch(const TupleDataLayout *layout,
                      uint8_t **row_ptrs,
                      const PipelineChunk &chunk,
                      const uint16_t *row_indices,
                      uint16_t count)
{
	Assert(layout != nullptr && row_ptrs != nullptr);
	Assert(row_indices != nullptr);
	Assert(count <= chunk.count);

	UpdateAggregatesBatchGroupedGeneric(layout, row_ptrs, chunk, row_indices, count);
}

void
UpdateAggregatesGather(const TupleDataLayout *layout,
                       uint8_t *tdc_base,
                       uint32_t row_width,
                       const uint32_t *canonical_row_indices,
                       const PipelineChunk &chunk,
                       const uint16_t *row_indices,
                       uint16_t count)
{
	Assert(layout != nullptr && tdc_base != nullptr);
	Assert(canonical_row_indices != nullptr && row_indices != nullptr);
	Assert(row_width == layout->row_width);
	Assert(count <= PIPELINE_DEFAULT_CHUNK_SIZE);
	UpdateAggregatesGatherGroupedGeneric(layout,
		tdc_base,
		row_width,
		canonical_row_indices,
		chunk,
		row_indices,
		count);
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

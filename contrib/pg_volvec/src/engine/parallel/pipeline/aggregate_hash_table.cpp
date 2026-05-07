/*
 * pipeline/aggregate_hash_table.cpp
 *
 * Open-addressing hash table over TupleDataCollection. See header for the
 * design contract. Spec: §3.4, §10 step 4 of
 * .sisyphus/plans/3g2-tuple-data-collection-design.md.
 *
 * Probe algorithm:
 *   slot = hash & mask
 *   step = ((hash >> 59) | 1)  # odd, covers power-of-two table
 *   for tries = 0 .. capacity:
 *     entry = entries[slot]
 *     if entry is empty:
 *         claim: write [salt:16 | row_idx:48], return INSERTED
 *     elif entry.salt == our_salt and MatchGroup(our_row, entry.row):
 *         existing match: return EXISTING with entry.row_index
 *     else:
 *         slot = (slot + step) & mask
 *   elog(ERROR, "AHT full")
 *
 * The whole probe is under the mutex in v1 — coarse but trivially correct.
 * Q3+ moves to per-thread local AHTs + merge phase, eliminating the lock.
 */

#include "aggregate_hash_table.hpp"

extern "C" {
#include "postgres.h"
#include "utils/elog.h"
}

#include <cstring>

#include "tuple_data_ops.hpp"

namespace pg_volvec {
namespace pipeline {

namespace {

static inline uint16_t
HashSalt(uint64_t hash)
{
	return static_cast<uint16_t>(hash >> 48);
}

static inline uint32_t
ProbeStep(uint64_t hash)
{
	return static_cast<uint32_t>((hash >> 59) | UINT64CONST(1));
}

static inline uint64_t
PackEntry(uint16_t salt, uint32_t row_index)
{
	Assert(row_index != AHT_INVALID_ROW_INDEX);
	return (static_cast<uint64_t>(salt) << 48) |
		(static_cast<uint64_t>(row_index) & AHT_ROW_INDEX_MASK);
}

static inline bool
EntryIsEmpty(const AggHtEntry &entry)
{
	return entry.value == AHT_EMPTY_ENTRY_VALUE;
}

static inline uint16_t
EntrySalt(const AggHtEntry &entry)
{
	return static_cast<uint16_t>(entry.value >> 48);
}

static inline uint32_t
EntryRowIndex(const AggHtEntry &entry)
{
	return static_cast<uint32_t>(entry.value & AHT_ROW_INDEX_MASK);
}

struct ProbeMeta
{
	uint16_t salt;
	uint32_t slot;
	uint32_t step;
};

static inline ProbeMeta
BuildProbeMeta(const AggregateHashTable *aht, uint64_t hash)
{
	ProbeMeta meta;
	meta.salt = HashSalt(hash);
	meta.slot = static_cast<uint32_t>(hash) & aht->capacity_mask;
	meta.step = ProbeStep(hash);
	return meta;
}

static inline void
PrefetchEntry(const AggregateHashTable *aht, uint32_t slot)
{
#if defined(__GNUC__) || defined(__clang__)
	__builtin_prefetch(&aht->entries[slot], 0, 1);
#else
	(void) aht;
	(void) slot;
#endif
}

static bool FindOrInsertLocked(AggregateHashTable *aht,
                               TupleDataCollection *tdc,
                               const TupleDataLayout *layout,
                               uint32_t group_row_idx,
                               const uint8_t *group_row_ptr,
                               const ProbeMeta &meta,
                               uint32_t *out_existing_row_idx);

static bool FindOrInsertFromSlotLocked(AggregateHashTable *aht,
                                       TupleDataCollection *tdc,
                                       const TupleDataLayout *layout,
                                       uint32_t group_row_idx,
                                       const uint8_t *group_row_ptr,
                                       const ProbeMeta &meta,
                                       uint32_t slot,
                                       uint32_t already_tried,
                                       uint32_t *out_existing_row_idx);

static bool
FindOrInsertLocked(AggregateHashTable *aht,
                   TupleDataCollection *tdc,
                   const TupleDataLayout *layout,
                   uint32_t group_row_idx,
                   const uint8_t *group_row_ptr,
                   uint64_t hash,
                   uint32_t *out_existing_row_idx)
{
	const ProbeMeta meta = BuildProbeMeta(aht, hash);
	PrefetchEntry(aht, meta.slot);
	return FindOrInsertLocked(aht,
		tdc,
		layout,
		group_row_idx,
		group_row_ptr,
		meta,
		out_existing_row_idx);
}

static bool
FindOrInsertLocked(AggregateHashTable *aht,
                   TupleDataCollection *tdc,
                   const TupleDataLayout *layout,
                   uint32_t group_row_idx,
                   const uint8_t *group_row_ptr,
                   const ProbeMeta &meta,
                   uint32_t *out_existing_row_idx)
{
	return FindOrInsertFromSlotLocked(aht,
		tdc,
		layout,
		group_row_idx,
		group_row_ptr,
		meta,
		meta.slot,
		0,
		out_existing_row_idx);
}

static bool
FindOrInsertFromSlotLocked(AggregateHashTable *aht,
                           TupleDataCollection *tdc,
                           const TupleDataLayout *layout,
                           uint32_t group_row_idx,
                           const uint8_t *group_row_ptr,
                           const ProbeMeta &meta,
                           uint32_t slot,
                           uint32_t already_tried,
                           uint32_t *out_existing_row_idx)
{
	const uint16_t our_salt = meta.salt;
	const uint32_t mask = aht->capacity_mask;
	const uint32_t step = meta.step;

	for (uint32_t tries = already_tried; tries < aht->capacity; ++tries)
	{
		AggHtEntry &e = aht->entries[slot];
		if (EntryIsEmpty(e))
		{
			e.value = PackEntry(our_salt, group_row_idx);
			*out_existing_row_idx = group_row_idx;
			return true;
		}
		if (EntrySalt(e) == our_salt)
		{
			const uint32_t existing_idx = EntryRowIndex(e);
			const uint8_t *existing_ptr = TupleDataCollectionGetRowConst(tdc, existing_idx);
			if (MatchGroupRow(layout, group_row_ptr, existing_ptr))
			{
				*out_existing_row_idx = existing_idx;
				return false;
			}
		}
		slot = (slot + step) & mask;
	}

	ereport(ERROR,
	        (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
	         errmsg("pg_volvec: AggregateHashTable full (capacity=%u)",
	                aht->capacity)));
	pg_unreachable();
}

}  /* namespace */

uint32_t
AggregateHashTableChooseCapacity(uint32_t max_groups)
{
	constexpr uint32_t kMinCapacity = 32;
	constexpr uint32_t kMaxCapacity = 1u << 22;
	uint64_t needed = static_cast<uint64_t>(max_groups < 1 ? 1u : max_groups);
	needed = needed + (needed >> 1) + 1u;
	if (needed < kMinCapacity)
		needed = kMinCapacity;
	if (needed > kMaxCapacity)
		needed = kMaxCapacity;

	uint32_t pow2 = 1;
	while (pow2 < needed)
		pow2 <<= 1;
	return pow2;
}

uint32_t
HashAggChoosePartitionCount(uint32_t worker_count, uint32_t row_width)
{
	uint32_t width_target;
	if (row_width < 32u)
		width_target = 1u << 8;
	else if (row_width < 64u)
		width_target = 1u << 7;
	else
		width_target = 1u << 6;

	uint32_t target = worker_count < 1 ? 1u : worker_count * 4u;
	if (target > width_target)
		target = width_target;
	if (target < 8u)
		target = 8u;
	if (target > 32u)
		target = 32u;

	uint32_t pow2 = 1;
	while (pow2 < target)
		pow2 <<= 1;
	return pow2;
}

uint32_t
HashAggPartitionIndex(uint64_t hash, uint32_t partition_mask)
{
	return static_cast<uint32_t>(hash >> HashAggPartitionShift(partition_mask)) & partition_mask;
}

uint32_t
HashAggPartitionShift(uint32_t partition_mask)
{
	uint32_t bits = 0;
	uint32_t tmp = partition_mask;
	while (tmp != 0)
	{
		bits++;
		tmp >>= 1;
	}
	return 64u - bits;
}

void
AggregateHashTableInit(AggregateHashTable *aht, uint32_t capacity, dsa_pointer tdc_dp)
{
	Assert(aht != nullptr);
	Assert(capacity > 0 && (capacity & (capacity - 1)) == 0);

	aht->capacity      = capacity;
	aht->capacity_mask = capacity - 1u;
	aht->tdc_dp        = tdc_dp;
	SpinLockInit(&aht->mutex);
	aht->_pad = 0;

	/* dsa_allocate0 gives value=0, a valid packed [salt=0,row=0] entry. */
	for (uint32_t i = 0; i < capacity; ++i)
		aht->entries[i].value = AHT_EMPTY_ENTRY_VALUE;
}

void
AggregateHashTableRehash(AggregateHashTable *aht,
                         TupleDataCollection *tdc,
                         const TupleDataLayout *layout)
{
	Assert(aht != nullptr && tdc != nullptr && layout != nullptr);

	for (uint32_t i = 0; i < aht->capacity; ++i)
		aht->entries[i].value = AHT_EMPTY_ENTRY_VALUE;

	const uint32_t row_count = pg_atomic_read_u32(&tdc->row_count);
	for (uint32_t row_idx = 0; row_idx < row_count; ++row_idx)
	{
		const uint8_t *row_ptr = TupleDataCollectionGetRowConst(tdc, row_idx);
		const uint64_t hash = HashGroupRow(layout, row_ptr);
		const uint16_t salt = HashSalt(hash);
		const uint32_t mask = aht->capacity_mask;
		const uint32_t step = ProbeStep(hash);
		uint32_t slot = static_cast<uint32_t>(hash) & mask;

		for (uint32_t tries = 0; tries < aht->capacity; ++tries)
		{
			AggHtEntry &e = aht->entries[slot];
			if (EntryIsEmpty(e))
			{
				e.value = PackEntry(salt, row_idx);
				break;
			}
			slot = (slot + step) & mask;
			if (tries + 1 == aht->capacity)
				ereport(ERROR,
				        (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				         errmsg("pg_volvec: AggregateHashTable rehash failed (capacity=%u)",
				                aht->capacity)));
		}
	}
}

bool
AggregateHashTableShouldResize(const AggregateHashTable *aht,
                               TupleDataCollection *tdc)
{
	Assert(aht != nullptr && tdc != nullptr);
	const uint32_t row_count = pg_atomic_read_u32(&tdc->row_count);
	return static_cast<uint64_t>(row_count) * 3u >=
		static_cast<uint64_t>(aht->capacity) * 2u;
}

bool
AggregateHashTableFindOrInsert(AggregateHashTable *aht,
                               TupleDataCollection *tdc,
                               const TupleDataLayout *layout,
                               uint32_t group_row_idx,
                               const uint8_t *group_row_ptr,
                               uint64_t hash,
                               uint32_t *out_existing_row_idx)
{
	Assert(aht != nullptr && tdc != nullptr && layout != nullptr);
	Assert(group_row_ptr != nullptr && out_existing_row_idx != nullptr);
	Assert(group_row_idx != AHT_INVALID_ROW_INDEX);

	uint32_t found_idx = AHT_INVALID_ROW_INDEX;
	SpinLockAcquire(&aht->mutex);
	const bool inserted = FindOrInsertLocked(aht,
		tdc,
		layout,
		group_row_idx,
		group_row_ptr,
		hash,
		&found_idx);
	SpinLockRelease(&aht->mutex);

	*out_existing_row_idx = found_idx;
	return inserted;
}

void
AggregateHashTableFindOrInsertBatch(AggregateHashTable *aht,
                                    TupleDataCollection *tdc,
                                    const TupleDataLayout *layout,
                                    PipelineChunk &chunk,
                                    const AggregateHashTableBatchProbeInput *inputs,
                                    uint16_t count,
                                    AggregateHashTableBatchProbeResult *results)
{
	Assert(aht != nullptr && tdc != nullptr && layout != nullptr);
	Assert(inputs != nullptr && results != nullptr);
	ProbeMeta probe_meta[PIPELINE_DEFAULT_CHUNK_SIZE];
	Assert(count <= PIPELINE_DEFAULT_CHUNK_SIZE);

	for (uint16_t i = 0; i < count; ++i)
	{
		probe_meta[i] = BuildProbeMeta(aht, inputs[i].hash);
		PrefetchEntry(aht, probe_meta[i].slot);
	}

	SpinLockAcquire(&aht->mutex);
	{
		for (uint16_t i = 0; i < count; ++i)
		{
			uint32_t canonical_idx = AHT_INVALID_ROW_INDEX;
			bool inserted = false;
			uint32_t slot = probe_meta[i].slot;

			for (uint32_t tries = 0; tries < aht->capacity; ++tries)
			{
				AggHtEntry &entry = aht->entries[slot];
				if (EntryIsEmpty(entry))
				{
					uint8_t *candidate_row = nullptr;
					const uint32_t candidate_idx = TupleDataCollectionAppendRow(tdc, &candidate_row);
					if (candidate_idx == TDC_INVALID_ROW_INDEX)
						elog(ERROR, "pg_volvec: local hash aggregate row capacity exceeded");

					ScatterGroupOnly(layout, candidate_row, chunk, inputs[i].row_idx);
					entry.value = PackEntry(probe_meta[i].salt, candidate_idx);
					canonical_idx = candidate_idx;
					inserted = true;
					break;
				}

				if (EntrySalt(entry) == probe_meta[i].salt)
				{
					const uint32_t existing_idx = EntryRowIndex(entry);
					const uint8_t *existing_ptr = TupleDataCollectionGetRowConst(tdc, existing_idx);
					if (MatchGroup(layout, existing_ptr, chunk, inputs[i].row_idx))
					{
						canonical_idx = existing_idx;
						inserted = false;
						break;
					}
				}

				slot = (slot + probe_meta[i].step) & aht->capacity_mask;
			}

			if (canonical_idx == AHT_INVALID_ROW_INDEX)
				ereport(ERROR,
				        (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				         errmsg("pg_volvec: AggregateHashTable full (capacity=%u)",
				                aht->capacity)));

			results[i].row_idx = inputs[i].row_idx;
			results[i].canonical_row_idx = canonical_idx;
			results[i].inserted = inserted;
		}
	}
	SpinLockRelease(&aht->mutex);
}

void
AggregateHashTableCombineRow(AggregateHashTable *aht,
                             TupleDataCollection *tdc,
                             const TupleDataLayout *layout,
                             const uint8_t *src_row,
                             uint64_t hash)
{
	Assert(aht != nullptr && tdc != nullptr && layout != nullptr && src_row != nullptr);

	const uint16_t our_salt = HashSalt(hash);
	const uint32_t mask     = aht->capacity_mask;
	uint32_t       slot     = static_cast<uint32_t>(hash) & mask;
	const uint32_t step     = ProbeStep(hash);

	uint8_t *canonical_row = nullptr;
	bool     overflow      = false;

	SpinLockAcquire(&aht->mutex);
	{
		for (uint32_t tries = 0; tries < aht->capacity; ++tries)
		{
			AggHtEntry &e = aht->entries[slot];
			if (EntryIsEmpty(e))
			{
				/* Miss: allocate a fresh canonical row, copy group cols, claim
				 * the slot. The new row's aggregate slots are zero from
				 * dsa_allocate0; CombineAggregates() below merges src into
				 * that zero state, leaving a fully-initialized canonical row
				 * before we drop the mutex. No window for a peer worker to
				 * see a half-built row. */
				uint8_t *new_row = nullptr;
				const uint32_t new_idx = TupleDataCollectionAppendRow(tdc, &new_row);
				if (new_idx == TDC_INVALID_ROW_INDEX)
				{
					overflow = true;
					break;
				}
				for (uint16_t col_idx = 0; col_idx < layout->column_count; ++col_idx)
				{
					const TdcColumnDesc &col = layout->columns[col_idx];
					std::memcpy(new_row + col.offset, src_row + col.offset, col.width);
				}
				e.value       = PackEntry(our_salt, new_idx);
				canonical_row = new_row;
				break;
			}
			if (EntrySalt(e) == our_salt)
			{
				const uint32_t existing_idx = EntryRowIndex(e);
				const uint8_t *existing_ptr =
					TupleDataCollectionGetRowConst(tdc, existing_idx);
				if (MatchGroupRow(layout, src_row, existing_ptr))
				{
					canonical_row = TupleDataCollectionGetRow(tdc, existing_idx);
					break;
				}
			}
			slot = (slot + step) & mask;
		}

		if (canonical_row != nullptr)
			CombineAggregates(layout, canonical_row, src_row);
	}
	SpinLockRelease(&aht->mutex);

	if (overflow)
		ereport(ERROR,
		        (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
		         errmsg("pg_volvec: AggregateHashTableCombineRow TDC full")));

	if (canonical_row == nullptr)
		ereport(ERROR,
		        (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
		         errmsg("pg_volvec: AggregateHashTableCombineRow AHT full (capacity=%u)",
		                aht->capacity)));
}

}  /* namespace pipeline */
}  /* namespace pg_volvec */

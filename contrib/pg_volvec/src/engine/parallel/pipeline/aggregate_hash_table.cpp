/*
 * pipeline/aggregate_hash_table.cpp
 *
 * Open-addressing hash table over TupleDataCollection. See header for the
 * design contract. Spec: §3.4, §10 step 4 of
 * .sisyphus/plans/3g2-tuple-data-collection-design.md.
 *
 * Probe algorithm:
 *   slot = hash & mask
 *   for tries = 0 .. capacity:
 *     entry = entries[slot]
 *     if entry.row_index == INVALID:
 *         claim: write {our_row_idx, salt}, return INSERTED
 *     elif entry.salt == our_salt and MatchGroup(our_row, entry.row):
 *         existing match: return EXISTING with entry.row_index
 *     else:
 *         slot = (slot + 1) & mask  # linear probe
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

uint32_t
AggregateHashTableChooseCapacity(uint32_t max_groups)
{
	/* Target ≥ 2x load headroom; round up to next power of two; clamp to
	 * v1 hard cap (2048 entries = 16 KB directory). */
	constexpr uint32_t kHardCap = 2048;
	uint32_t target = max_groups < 1 ? 1u : max_groups;
	if (target > kHardCap / 2)
		target = kHardCap / 2;
	uint32_t needed = target * 2u;

	uint32_t pow2 = 1;
	while (pow2 < needed)
		pow2 <<= 1;
	if (pow2 > kHardCap)
		pow2 = kHardCap;
	return pow2;
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

	/*
	 * dsa_allocate0 gave us row_index=0 in every entry, which is a VALID
	 * row index — would cause the very first probe to falsely match row 0.
	 * Overwrite to the sentinel.
	 */
	for (uint32_t i = 0; i < capacity; ++i)
	{
		aht->entries[i].row_index = AHT_INVALID_ROW_INDEX;
		aht->entries[i].salt      = 0;
		aht->entries[i]._pad      = 0;
	}
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

	const uint16_t our_salt = static_cast<uint16_t>(hash & 0xFFFFu);
	const uint32_t mask     = aht->capacity_mask;
	uint32_t       slot     = static_cast<uint32_t>(hash) & mask;

	bool inserted = false;
	uint32_t found_idx = AHT_INVALID_ROW_INDEX;

	SpinLockAcquire(&aht->mutex);
	{
		for (uint32_t tries = 0; tries < aht->capacity; ++tries)
		{
			AggHtEntry &e = aht->entries[slot];
			if (e.row_index == AHT_INVALID_ROW_INDEX)
			{
				/* Empty slot — claim it. */
				e.row_index = group_row_idx;
				e.salt      = our_salt;
				e._pad      = 0;
				found_idx   = group_row_idx;
				inserted    = true;
				break;
			}
			if (e.salt == our_salt)
			{
				/* Salt matches — do the full row compare. */
				const uint8_t *existing_ptr =
					TupleDataCollectionGetRowConst(tdc, e.row_index);
				if (MatchGroupRow(layout, group_row_ptr, existing_ptr))
				{
					found_idx = e.row_index;
					inserted  = false;
					break;
				}
			}
			slot = (slot + 1u) & mask;
		}
	}
	SpinLockRelease(&aht->mutex);

	if (found_idx == AHT_INVALID_ROW_INDEX)
	{
		/* Walked the whole table without finding a slot or match. v1 hard
		 * cap should make this impossible for Q1 (4 distinct groups, 2048
		 * slots). If it triggers, the caller exceeded v1 assumptions. */
		ereport(ERROR,
		        (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
		         errmsg("pg_volvec: AggregateHashTable full (capacity=%u)",
		                aht->capacity)));
	}

	*out_existing_row_idx = found_idx;
	return inserted;
}

void
AggregateHashTableCombineRow(AggregateHashTable *aht,
                             TupleDataCollection *tdc,
                             const TupleDataLayout *layout,
                             const uint8_t *src_row,
                             uint64_t hash)
{
	Assert(aht != nullptr && tdc != nullptr && layout != nullptr && src_row != nullptr);

	const uint16_t our_salt = static_cast<uint16_t>(hash & 0xFFFFu);
	const uint32_t mask     = aht->capacity_mask;
	uint32_t       slot     = static_cast<uint32_t>(hash) & mask;

	uint8_t *canonical_row = nullptr;
	bool     overflow      = false;

	SpinLockAcquire(&aht->mutex);
	{
		for (uint32_t tries = 0; tries < aht->capacity; ++tries)
		{
			AggHtEntry &e = aht->entries[slot];
			if (e.row_index == AHT_INVALID_ROW_INDEX)
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
				e.row_index   = new_idx;
				e.salt        = our_salt;
				e._pad        = 0;
				canonical_row = new_row;
				break;
			}
			if (e.salt == our_salt)
			{
				const uint8_t *existing_ptr =
					TupleDataCollectionGetRowConst(tdc, e.row_index);
				if (MatchGroupRow(layout, src_row, existing_ptr))
				{
					canonical_row = TupleDataCollectionGetRow(tdc, e.row_index);
					break;
				}
			}
			slot = (slot + 1u) & mask;
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

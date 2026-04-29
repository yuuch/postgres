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

}  /* namespace pipeline */
}  /* namespace pg_volvec */

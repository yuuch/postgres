#pragma once

/*
 * pipeline/aggregate_hash_table.hpp
 *
 * 3g.2-final — DSA-resident open-addressing hash table that maps a tuple of
 * group-by column values to a row index in a TupleDataCollection. The TDC
 * stores the materialized group + aggregate state row; the AHT only stores
 * directory entries (8 bytes each) for fast probe.
 *
 * v1 scope (per design §3.4, §10 step 4):
 *   - Single global AHT under a coarse spinlock — workers contend directly.
 *     Per-thread local AHTs land at Q3+.
 *   - Capacity is fixed at Init time (power of two, ≥ 2 * max_rows). Default
 *     max_rows=256, hard cap 1024 → capacity ≤ 2048 entries (16 KB).
 *     Overflow on insert = elog(ERROR) in v1.
 *   - Linear probe on collision (Robin-Hood / quadratic deferred to Q3+).
 *   - Hash function = FNV-1a over col.width bytes per group col (consistent
 *     with TupleDataMatchGroup's typed compare width).
 *   - 16-bit salt stored alongside row_index for early reject during probe.
 *
 * Memory layout (single dsa_allocate0):
 *
 *   | AggregateHashTable header | entries[capacity] |
 *
 * Each entry is 8 bytes; row_index = INVALID_ROW_INDEX (UINT32_MAX) marks
 * empty. Caller MUST re-init entries[] to INVALID_ROW_INDEX after
 * dsa_allocate0 (which gives row_index=0, a VALID index — bug landmine).
 *
 * Spec: .sisyphus/plans/3g2-tuple-data-collection-design.md §3.4, §10 step 4.
 */

extern "C" {
#include "postgres.h"
#include "storage/spin.h"
#include "utils/dsa.h"
}

#include <cstdint>

#include "tuple_data_collection.hpp"
#include "tuple_data_layout.hpp"

namespace pg_volvec {
namespace pipeline {

/*
 * Sentinel marking an empty AHT slot. Note dsa_allocate0 gives 0 which is a
 * VALID row index; AggregateHashTableInit MUST overwrite all entries to
 * this value before the table is usable.
 */
static constexpr uint32_t AHT_INVALID_ROW_INDEX = UINT32_MAX;

/*
 * Directory entry. 8 bytes — fits a cache line worth (8 entries / 64B).
 *   - row_index: index into the backing TupleDataCollection.
 *   - salt:      low 16 bits of the hash, used to early-reject during
 *                linear probe before the full row compare.
 *   - _pad:      explicit padding for ABI stability across compilers.
 */
struct AggHtEntry
{
	uint32_t row_index;
	uint16_t salt;
	uint16_t _pad;
};
static_assert(sizeof(AggHtEntry) == 8, "AggHtEntry must be 8 bytes");

struct AggregateHashTable
{
	/* immutable after Init */
	uint32_t   capacity;       /* power of two; mask = capacity - 1 */
	uint32_t   capacity_mask;  /* capacity - 1, cached */
	dsa_pointer tdc_dp;        /* -> TupleDataCollection backing canonical rows */

	/* mutator-side guard. Insert and lookup-then-insert both take this. */
	slock_t    mutex;

	uint32_t   _pad;

	/* Trailing entries[capacity]. See AggregateHashTableAllocSize(). */
	AggHtEntry entries[1];
};

/*
 * Allocation size. Use with dsa_allocate0; then call AggregateHashTableInit
 * to overwrite the zero-init row_index fields with AHT_INVALID_ROW_INDEX.
 */
inline size_t
AggregateHashTableAllocSize(uint32_t capacity)
{
	Assert(capacity > 0 && (capacity & (capacity - 1)) == 0);
	return offsetof(AggregateHashTable, entries) +
	       static_cast<size_t>(capacity) * sizeof(AggHtEntry);
}

/*
 * Choose AHT capacity for a given expected max distinct group count.
 * Returns the next power of two ≥ 2 * max_groups, clamped to a v1 hard cap.
 * Hard cap = 2048 entries (covers max_rows=1024 at 50% load). Q3+ relaxes.
 */
uint32_t AggregateHashTableChooseCapacity(uint32_t max_groups);

/*
 * Initialize an already-zeroed AHT in place. Sets capacity / mask /
 * spinlock, publishes the backing TupleDataCollection pointer, and overwrites
 * entries[].row_index = AHT_INVALID_ROW_INDEX.
 * Salt bits stay zero (don't care; only inspected when row_index != INVALID).
 */
void AggregateHashTableInit(AggregateHashTable *aht,
						 uint32_t capacity,
						 dsa_pointer tdc_dp = InvalidDsaPointer);

/*
 * Lookup-or-insert: given group_row_ptr (a fully-Scattered row in tdc that
 * the caller pre-allocated via TupleDataCollectionAppendRow), find an
 * existing matching group or claim this entry as the new group.
 *
 * Inputs:
 *   - aht, tdc, layout: hash table, backing collection, row schema.
 *   - group_row_idx: index returned by AppendRow for the candidate row.
 *   - group_row_ptr: pointer returned by AppendRow (already Scattered with
 *                    group col values; agg slots zero from dsa_allocate0).
 *   - hash:         FNV-1a over the group cols (caller computes via
 *                   HashGroup or per-row equivalent). Full 64-bit value;
 *                   AHT narrows internally for slot index and salt.
 *
 * Outputs:
 *   - *out_existing_row_idx: the canonical row index for this group. If
 *     equal to group_row_idx, the caller's row was inserted (claim); else
 *     a pre-existing row matched and the caller's row is now garbage at the
 *     tail of the TDC (acceptable v1 waste — fixed at Q3+ via probe-first).
 *
 * Returns true if a NEW group was inserted (caller's row is canonical),
 * false if an EXISTING group matched (caller must Combine into existing).
 *
 * Thread-safe under the spinlock. On AHT-full, elog(ERROR).
 */
bool AggregateHashTableFindOrInsert(AggregateHashTable *aht,
                                    TupleDataCollection *tdc,
                                    const TupleDataLayout *layout,
                                    uint32_t group_row_idx,
                                    const uint8_t *group_row_ptr,
                                    uint64_t hash,
                                    uint32_t *out_existing_row_idx);

/*
 * Atomic combine: probe AHT for a row matching src_row's group cols; on
 * miss, append a new canonical row to tdc, copy group cols into it, and
 * mark it claimed; on hit, locate the existing canonical row. Either way,
 * merge src_row's aggregate state into the canonical row via
 * CombineAggregates(layout, ...).
 *
 * Bug P: serializes probe + (optional) append + group-col copy + agg merge
 * under aht->mutex so concurrent COMBINE workers cannot strand a
 * partially-populated row at the tail of the global TDC. Replaces the
 * pre-Bug-P pattern in PhysicalHashAggregate::Combine that did
 * speculative AppendRow → FindOrInsert → RollbackLastAppend (rollback
 * was racy under wake-on-pop).
 *
 * Caller MUST NOT hold aht->mutex. CombineAggregates() is invoked while
 * the mutex is held; for v1 (single global AHT) this also serializes
 * canonical-row aggregate updates — necessary to prevent lost-update
 * races on the same group across workers.
 */
void AggregateHashTableCombineRow(AggregateHashTable *aht,
                                  TupleDataCollection *tdc,
                                  const TupleDataLayout *layout,
                                  const uint8_t *src_row,
                                  uint64_t hash);

}  /* namespace pipeline */
}  /* namespace pg_volvec */

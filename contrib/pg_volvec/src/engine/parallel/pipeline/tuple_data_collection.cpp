/*
 * pipeline/tuple_data_collection.cpp
 *
 * Implementation of the DSA-resident append-only row buffer. See header
 * for the design contract. Spec: §3.3, §10 step 3 of
 * .sisyphus/plans/3g2-tuple-data-collection-design.md.
 *
 * Concurrency model:
 *   - AppendRow grabs the slock_t, bumps an internal index, releases.
 *     Atomic store on row_count is published AFTER the row is materialized
 *     so concurrent scanners observing row_count see fully-written rows.
 *     (The mutex serializes appenders; the atomic publish is for readers.)
 *   - ClaimScanRow uses fetch_add on scan_cursor and bounds against an
 *     atomic-load snapshot of row_count. No lock on the read path.
 *   - GetRow has no synchronization; caller bounds by row_count snapshot.
 *
 * The slock_t covers the bump (we need atomicity between "decide my index"
 * and "publish row_count >= my_index+1" only insofar as readers need a
 * monotonic row_count; pg_atomic_fetch_add would suffice for the count
 * itself, but the spinlock is cheap at v1 contention levels and gives a
 * single obvious append-side serialization point. Q3+ moves to per-thread
 * local TDCs where this lock disappears entirely.).
 */

#include "tuple_data_collection.hpp"

extern "C" {
#include "postgres.h"
#include "utils/elog.h"
}

#include <cstring>

namespace pg_volvec {
namespace pipeline {

void
TupleDataCollectionInit(TupleDataCollection *tdc,
                        uint32_t row_capacity,
						uint32_t row_width,
						dsa_pointer layout_dp)
{
	Assert(tdc != nullptr);
	Assert(row_capacity > 0);
	Assert(row_width > 0);
	Assert(row_width % 8 == 0);  /* TupleDataLayoutSeal post-condition */

	tdc->row_width    = row_width;
	tdc->row_capacity = row_capacity;
	tdc->layout_dp    = layout_dp;
	SpinLockInit(&tdc->mutex);
	pg_atomic_init_u32(&tdc->row_count, 0);
	pg_atomic_init_u32(&tdc->scan_cursor, 0);
	tdc->finalized = false;
	tdc->_pad[0] = 0;
	tdc->_pad[1] = 0;
	tdc->_pad[2] = 0;
	/* tdc->rows is already zero from dsa_allocate0; do not memset
	 * row_capacity*row_width bytes here (could be many MB). */
}

uint32_t
TupleDataCollectionAppendRow(TupleDataCollection *tdc, uint8_t **out_row)
{
	Assert(tdc != nullptr && out_row != nullptr);

	uint32_t my_idx;
	SpinLockAcquire(&tdc->mutex);
	{
		const uint32_t cur = pg_atomic_read_u32(&tdc->row_count);
		if (cur >= tdc->row_capacity)
		{
			SpinLockRelease(&tdc->mutex);
			*out_row = nullptr;
			return TDC_INVALID_ROW_INDEX;
		}
		my_idx = cur;
		/*
		 * Publish row_count BEFORE releasing the lock. Readers that observe
		 * row_count >= my_idx+1 are guaranteed to see a row buffer slot
		 * that the caller is about to write into. The slot itself is
		 * already zeroed by dsa_allocate0, so a reader racing the caller's
		 * Scatter sees zeros (which would be a logic bug — readers MUST
		 * bound by their own snapshot of row_count taken AFTER the
		 * appender finished, not during).
		 *
		 * In practice TDC has a strict sink-then-source phase boundary
		 * (HashAggregate sinks all rows before any source-side scan
		 * starts — enforced by the MetaPipeline DAG), so the race window
		 * is closed at the architectural level. The atomic publish is
		 * defense in depth.
		 */
		pg_atomic_write_u32(&tdc->row_count, my_idx + 1);
	}
	SpinLockRelease(&tdc->mutex);

	*out_row = TupleDataCollectionGetRow(tdc, my_idx);
	return my_idx;
}

uint32_t
TupleDataCollectionClaimScanRow(TupleDataCollection *tdc)
{
	Assert(tdc != nullptr);

	const uint32_t total = pg_atomic_read_u32(&tdc->row_count);
	uint32_t claimed = pg_atomic_fetch_add_u32(&tdc->scan_cursor, 1);
	if (claimed >= total)
	{
		/* Over-claim: pull the cursor back so subsequent claims still see
		 * the same total bound. Without this, scan_cursor can race past
		 * UINT32_MAX in pathological cases. CAS is best-effort: if another
		 * thread also over-claimed and rewound, we leave it. Note CAS
		 * writes back to `claimed` on failure — must be non-const. */
		uint32_t expected = claimed + 1;
		pg_atomic_compare_exchange_u32(&tdc->scan_cursor, &expected, total);
		return TDC_INVALID_ROW_INDEX;
	}
	return claimed;
}

void
TupleDataCollectionResetScan(TupleDataCollection *tdc)
{
	Assert(tdc != nullptr);
	pg_atomic_write_u32(&tdc->scan_cursor, 0);
}

}  /* namespace pipeline */
}  /* namespace pg_volvec */

#include "parallel/pipeline/physical_hash_join_combine.hpp"

extern "C" {
#include "postgres.h"
}

#include <cstring>

#include "core/data_chunk.hpp"
#include "parallel/pipeline/tuple_data_collection.hpp"
#include "parallel/pipeline/tuple_data_layout.hpp"

namespace pg_yaap {
namespace pipeline {

namespace {

static HashJoinLocalBuildRegistryEntry *
ResolveLocalBuildRegistry(dsa_area *dsa, HashJoinSharedPayload *payload)
{
	if (payload == nullptr || !DsaPointerIsValid(payload->local_build_registry_dp))
		return nullptr;
	return static_cast<HashJoinLocalBuildRegistryEntry *>(
		dsa_get_address(dsa, payload->local_build_registry_dp));
}

static TupleDataCollection *
ResolveTdc(dsa_area *dsa, dsa_pointer tdc_dp)
{
	if (!DsaPointerIsValid(tdc_dp))
		return nullptr;
	return static_cast<TupleDataCollection *>(dsa_get_address(dsa, tdc_dp));
}

static uint32_t
ReadTdcRowCount(const TupleDataCollection *tdc)
{
	return tdc == nullptr ? 0u :
		pg_atomic_read_u32(const_cast<pg_atomic_uint32 *>(&tdc->row_count));
}

static uint32_t
ReadTdcHeapUsed(const TupleDataCollection *tdc)
{
	return tdc == nullptr ? 0u :
		pg_atomic_read_u32(const_cast<pg_atomic_uint32 *>(&tdc->heap_used));
}

static void
EnsureEmptyJoinGlobalCapacity(ExecCtx &ctx,
                              const TupleDataLayout *layout,
                              dsa_pointer layout_dp,
                              dsa_pointer *tdc_dp,
                              uint32_t target_rows,
                              uint32_t target_heap_used)
{
	TupleDataCollection *tdc = ResolveTdc(ctx.dsa, *tdc_dp);
	if (tdc == nullptr)
		elog(ERROR, "pg_yaap: hash join global TDC missing during combine prepare");
	if (tdc->row_capacity >= target_rows && tdc->heap_capacity >= target_heap_used)
		return;
	if (ReadTdcRowCount(tdc) != 0 || ReadTdcHeapUsed(tdc) != 0)
		elog(ERROR, "pg_yaap: hash join combine prepare expected empty global TDC");

	uint32_t new_row_capacity = std::max(tdc->row_capacity, std::max(1u, target_rows));
	new_row_capacity = TupleDataCollectionClampRowCapacity(new_row_capacity,
		layout->row_width,
		target_heap_used,
		std::max(1u, target_rows));

	uint32_t new_heap_capacity = tdc->heap_capacity;
	if (new_heap_capacity < target_heap_used)
	{
		new_heap_capacity = TupleDataCollectionGrowHeapCapacity(layout,
			tdc,
			new_row_capacity,
			target_heap_used);
	}
	if (new_heap_capacity < target_heap_used)
		elog(ERROR, "pg_yaap: hash join combine prepare failed to size string heap");

	dsa_pointer new_tdc_dp = TupleDataCollectionAllocate(ctx.dsa,
		new_row_capacity,
		layout->row_width,
		new_heap_capacity);
	TupleDataCollection *new_tdc = ResolveTdc(ctx.dsa, new_tdc_dp);
	TupleDataCollectionInit(new_tdc,
		new_row_capacity,
		layout->row_width,
		layout_dp,
		new_heap_capacity);
	dsa_free(ctx.dsa, *tdc_dp);
	*tdc_dp = new_tdc_dp;
}

static void
PrepareHashJoinCombine(ExecCtx &ctx, HashJoinGlobalSinkState &global)
{
	HashJoinSharedPayload *payload = global.payload;
	if (payload == nullptr)
		elog(ERROR, "pg_yaap: hash join combine missing shared payload");

	uint32 expected = 0;
	if (pg_atomic_compare_exchange_u32(&payload->combine_prepare_state, &expected, 1))
	{
		HashJoinLocalBuildRegistryEntry *registry = ResolveLocalBuildRegistry(ctx.dsa, payload);
		if (registry == nullptr)
			elog(ERROR, "pg_yaap: hash join combine registry missing");

		uint64_t total_rows = 0;
		uint64_t total_key_heap = 0;
		uint64_t total_row_heap = 0;
		for (uint32_t slot = 0; slot < payload->local_state_slot_count; ++slot)
		{
			TupleDataCollection *key_tdc = ResolveTdc(ctx.dsa, registry[slot].build_keys_dp);
			TupleDataCollection *row_tdc = ResolveTdc(ctx.dsa, registry[slot].build_rows_dp);
			registry[slot].row_count = ReadTdcRowCount(key_tdc);
			registry[slot].key_heap_used = ReadTdcHeapUsed(key_tdc);
			registry[slot].row_heap_used = ReadTdcHeapUsed(row_tdc);
			registry[slot].global_row_offset = static_cast<uint32_t>(total_rows);
			registry[slot].global_key_heap_offset = static_cast<uint32_t>(total_key_heap);
			registry[slot].global_row_heap_offset = static_cast<uint32_t>(total_row_heap);
			total_rows += registry[slot].row_count;
			total_key_heap += registry[slot].key_heap_used;
			total_row_heap += registry[slot].row_heap_used;
		}
		if (total_rows > UINT32_MAX || total_key_heap > UINT32_MAX || total_row_heap > UINT32_MAX)
			elog(ERROR, "pg_yaap: hash join combine totals exceed uint32 range");

		SpinLockAcquire(&payload->mutex);
		EnsureEmptyJoinGlobalCapacity(ctx,
			global.build_key_layout,
			global.build_key_layout_dp,
			&payload->build_keys_dp,
			static_cast<uint32_t>(total_rows),
			static_cast<uint32_t>(total_key_heap));
		EnsureEmptyJoinGlobalCapacity(ctx,
			global.build_layout,
			global.build_layout_dp,
			&payload->build_rows_dp,
			static_cast<uint32_t>(total_rows),
			static_cast<uint32_t>(total_row_heap));
		SpinLockRelease(&payload->mutex);

		payload->combined_row_count = static_cast<uint32_t>(total_rows);
		payload->combined_key_heap_used = static_cast<uint32_t>(total_key_heap);
		payload->combined_row_heap_used = static_cast<uint32_t>(total_row_heap);
		pg_atomic_write_u32(&payload->combine_prepare_state, 2u);
	}
	else
	{
		while (pg_atomic_read_u32(&payload->combine_prepare_state) != 2u)
		{
			CHECK_FOR_INTERRUPTS();
			pg_usleep(1000L);
		}
	}

	global.build_keys = ResolveTdc(ctx.dsa, payload->build_keys_dp);
	global.build_rows = ResolveTdc(ctx.dsa, payload->build_rows_dp);
	if (global.build_keys == nullptr || global.build_rows == nullptr)
		elog(ERROR, "pg_yaap: hash join combine global TDC missing after prepare");
}

static void
CopyStringAwareRowsIntoGlobal(const TupleDataLayout *layout,
                              const TupleDataCollection *src,
                              TupleDataCollection *dst,
                              uint32_t row_offset,
                              uint32_t row_count,
                              uint32_t heap_offset)
{
	if (layout == nullptr || row_count == 0)
		return;

	for (uint32_t row_idx = 0; row_idx < row_count; ++row_idx)
	{
		const uint8_t *src_row = TupleDataCollectionGetRowConst(src, row_idx);
		uint8_t *dst_row = TupleDataCollectionGetRow(dst, row_offset + row_idx);
		std::memcpy(dst_row, src_row, layout->row_width);
		for (uint16_t col_idx = 0; col_idx < layout->column_count; ++col_idx)
		{
			const TdcColumnDesc &col = layout->columns[col_idx];
			if (col.kind != TdcColumnKind::STRING_REF)
				continue;

			VecStringRef ref;
			std::memcpy(&ref, src_row + col.offset, sizeof(ref));
			if (ref.len == 0 || VecStringRefIsInline(ref))
				continue;

			ref.offset += heap_offset;
			std::memcpy(dst_row + col.offset, &ref, sizeof(ref));
		}
	}
}

static void
CopyLocalTdcIntoGlobal(const TupleDataLayout *layout,
                       const TupleDataCollection *src,
                       TupleDataCollection *dst,
                       uint32_t row_offset,
                       uint32_t heap_offset)
{
	const uint32_t row_count = ReadTdcRowCount(src);
	const uint32_t heap_used = ReadTdcHeapUsed(src);
	if (heap_used > 0)
	{
		std::memcpy(TupleDataCollectionHeap(dst) + heap_offset,
			TupleDataCollectionHeapConst(src),
			heap_used);
	}

	bool has_string_ref = false;
	for (uint16_t col_idx = 0; col_idx < layout->column_count; ++col_idx)
	{
		if (layout->columns[col_idx].kind == TdcColumnKind::STRING_REF)
		{
			has_string_ref = true;
			break;
		}
	}

	if (row_count == 0)
		return;
	if (!has_string_ref)
	{
		std::memcpy(TupleDataCollectionGetRow(dst, row_offset),
			TupleDataCollectionGetRowConst(src, 0),
			static_cast<size_t>(row_count) * layout->row_width);
		return;
	}
	CopyStringAwareRowsIntoGlobal(layout, src, dst, row_offset, row_count, heap_offset);
}

}  // namespace

SinkCombineResultType
ExecuteHashJoinCombine(ExecCtx &ctx,
                       HashJoinGlobalSinkState &global,
                       HashJoinLocalSinkState &local)
{
	if (local.build_keys == nullptr || local.build_rows == nullptr)
		elog(ERROR, "pg_yaap: hash join combine local build rows missing");
	if (ctx.worker_index < 0)
		elog(ERROR, "pg_yaap: hash join combine requires worker-local slot");

	PrepareHashJoinCombine(ctx, global);

	HashJoinLocalBuildRegistryEntry *registry = ResolveLocalBuildRegistry(ctx.dsa, global.payload);
	if (registry == nullptr || static_cast<uint32_t>(ctx.worker_index) >= global.payload->local_state_slot_count)
		elog(ERROR, "pg_yaap: hash join combine registry slot missing");
	HashJoinLocalBuildRegistryEntry &entry = registry[ctx.worker_index];

	CopyLocalTdcIntoGlobal(global.build_key_layout,
		local.build_keys,
		global.build_keys,
		entry.global_row_offset,
		entry.global_key_heap_offset);
	CopyLocalTdcIntoGlobal(global.build_layout,
		local.build_rows,
		global.build_rows,
		entry.global_row_offset,
		entry.global_row_heap_offset);
	return SinkCombineResultType::FINISHED;
}

void
PublishHashJoinCombinedRows(HashJoinGlobalSinkState &global)
{
	if (global.payload == nullptr ||
		pg_atomic_read_u32(&global.payload->combine_prepare_state) != 2u)
		return;

	global.build_keys = ResolveTdc(global.dsa, global.payload->build_keys_dp);
	global.build_rows = ResolveTdc(global.dsa, global.payload->build_rows_dp);
	if (global.build_keys == nullptr || global.build_rows == nullptr)
		elog(ERROR, "pg_yaap: hash join finalize missing combined global TDC");

	pg_atomic_write_u32(&global.build_keys->row_count, global.payload->combined_row_count);
	pg_atomic_write_u32(&global.build_rows->row_count, global.payload->combined_row_count);
	pg_atomic_write_u32(&global.build_keys->heap_used, global.payload->combined_key_heap_used);
	pg_atomic_write_u32(&global.build_rows->heap_used, global.payload->combined_row_heap_used);
	global.payload->combined = true;
}

}  /* namespace pipeline */
}  /* namespace pg_yaap */

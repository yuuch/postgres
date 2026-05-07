#include "parallel/pipeline/physical_hash_aggregate.hpp"

extern "C" {
#include "postgres.h"
#include "utils/elog.h"

extern int pg_volvec_parallel_max_workers;
}

#include <algorithm>
#include <array>
#include <cstring>

#include "core/data_chunk.hpp"
#include "parallel/pipeline/tuple_data_ops.hpp"
#include "parallel/pipeline/types.hpp"

namespace pg_volvec {
namespace pipeline {

namespace {

static const TupleDataLayout *
ResolveLayout(dsa_area *dsa, dsa_pointer layout_dp)
{
	if (!DsaPointerIsValid(layout_dp))
		return nullptr;
	return static_cast<const TupleDataLayout *>(dsa_get_address(dsa, layout_dp));
}

static AggregateHashTable *
ResolveAht(dsa_area *dsa, dsa_pointer aht_dp)
{
	if (!DsaPointerIsValid(aht_dp))
		return nullptr;
	return static_cast<AggregateHashTable *>(dsa_get_address(dsa, aht_dp));
}

static HashAggSharedPayload *
ResolvePayload(dsa_area *dsa, dsa_pointer payload_dp)
{
	if (!DsaPointerIsValid(payload_dp))
		return nullptr;
	return static_cast<HashAggSharedPayload *>(dsa_get_address(dsa, payload_dp));
}

static HashAggPartition *
ResolvePartitions(dsa_area *dsa, HashAggSharedPayload *payload)
{
	if (payload == nullptr || !DsaPointerIsValid(payload->partitions_dp))
		return nullptr;
	return static_cast<HashAggPartition *>(dsa_get_address(dsa, payload->partitions_dp));
}

static dsa_pointer *
ResolveLocalRegistry(dsa_area *dsa, HashAggSharedPayload *payload)
{
	if (payload == nullptr || !DsaPointerIsValid(payload->local_partitions_registry_dp))
		return nullptr;
	return static_cast<dsa_pointer *>(dsa_get_address(dsa, payload->local_partitions_registry_dp));
}

static TupleDataCollection *
ResolveTdc(dsa_area *dsa, dsa_pointer tdc_dp)
{
	if (!DsaPointerIsValid(tdc_dp))
		return nullptr;
	return static_cast<TupleDataCollection *>(dsa_get_address(dsa, tdc_dp));
}

static uint32_t
PartitionRowCapacity(uint32_t max_groups, uint32_t partition_count)
{
	const uint32_t n = partition_count < 1 ? 1u : partition_count;
	uint32_t per = (max_groups + n - 1u) / n;
	if (per < 16u)
		per = 16u;
	return per;
}

static void
AllocAhtForTdc(ExecCtx &ctx,
               dsa_pointer tdc_dp,
               uint32_t max_groups,
               dsa_pointer *out_aht_dp,
               AggregateHashTable **out_aht)
{
	const uint32_t capacity = AggregateHashTableChooseCapacity(max_groups);
	*out_aht_dp = dsa_allocate0(ctx.dsa, AggregateHashTableAllocSize(capacity));
	*out_aht = static_cast<AggregateHashTable *>(dsa_get_address(ctx.dsa, *out_aht_dp));
	AggregateHashTableInit(*out_aht, capacity, tdc_dp);
}

static void
ResizeAhtForTdc(ExecCtx &ctx,
                dsa_pointer tdc_dp,
                TupleDataCollection *tdc,
                const TupleDataLayout *layout,
                dsa_pointer *aht_dp)
{
	AggregateHashTable *old_aht = ResolveAht(ctx.dsa, *aht_dp);
	if (old_aht == nullptr || !AggregateHashTableShouldResize(old_aht, tdc))
		return;

	const uint32_t new_capacity = old_aht->capacity << 1;
	dsa_pointer new_aht_dp = dsa_allocate0(ctx.dsa, AggregateHashTableAllocSize(new_capacity));
	AggregateHashTable *new_aht = static_cast<AggregateHashTable *>(dsa_get_address(ctx.dsa, new_aht_dp));
	AggregateHashTableInit(new_aht, new_capacity, tdc_dp);
	AggregateHashTableRehash(new_aht, tdc, layout);
	*aht_dp = new_aht_dp;
}

static void
GrowTdcForPartition(ExecCtx &ctx,
                    dsa_pointer layout_dp,
                    const TupleDataLayout *layout,
                    HashAggPartition &part)
{
	TupleDataCollection *old_tdc = ResolveTdc(ctx.dsa, part.tdc_dp);
	if (old_tdc == nullptr)
		elog(ERROR, "pg_volvec: hash aggregate partition TDC missing");

	const uint32_t old_count = pg_atomic_read_u32(&old_tdc->row_count);
	const uint32_t new_capacity = std::max(old_tdc->row_capacity * 2u, old_count + 1u);
	dsa_pointer new_tdc_dp = dsa_allocate0(ctx.dsa,
		TupleDataCollectionAllocSize(new_capacity, layout->row_width));
	auto *new_tdc = ResolveTdc(ctx.dsa, new_tdc_dp);
	TupleDataCollectionInit(new_tdc, new_capacity, layout->row_width, layout_dp);

	for (uint32_t row_idx = 0; row_idx < old_count; ++row_idx)
	{
		uint8_t *dst = nullptr;
		const uint32_t copied_idx = TupleDataCollectionAppendRow(new_tdc, &dst);
		if (copied_idx == TDC_INVALID_ROW_INDEX)
			elog(ERROR, "pg_volvec: hash aggregate TDC grow copy overflow");
		const uint8_t *src = TupleDataCollectionGetRowConst(old_tdc, row_idx);
		std::memcpy(dst, src, layout->row_width);
	}

	const uint32_t capacity = AggregateHashTableChooseCapacity(new_capacity);
	dsa_pointer new_aht_dp = dsa_allocate0(ctx.dsa, AggregateHashTableAllocSize(capacity));
	AggregateHashTable *new_aht = static_cast<AggregateHashTable *>(dsa_get_address(ctx.dsa, new_aht_dp));
	AggregateHashTableInit(new_aht, capacity, new_tdc_dp);
	AggregateHashTableRehash(new_aht, new_tdc, layout);
	part.tdc_dp = new_tdc_dp;
	part.aht_dp = new_aht_dp;
}

static void
GrowLocalTdc(ExecCtx &ctx,
             HashAggLocalSinkState &local,
             HashAggPartition &part)
{
	TupleDataCollection *old_tdc = ResolveTdc(ctx.dsa, part.tdc_dp);
	if (old_tdc == nullptr)
		elog(ERROR, "pg_volvec: local hash aggregate partition TDC missing");
	const uint32_t old_count = pg_atomic_read_u32(&old_tdc->row_count);
	const uint32_t new_capacity = std::max(old_tdc->row_capacity * 2u, old_count + 1u);
	dsa_pointer new_tdc_dp = dsa_allocate0(ctx.dsa,
		TupleDataCollectionAllocSize(new_capacity, local.layout->row_width));
	auto *new_tdc = ResolveTdc(ctx.dsa, new_tdc_dp);
	TupleDataCollectionInit(new_tdc,
		new_capacity,
		local.layout->row_width,
		local.layout_dp);

	for (uint32_t row_idx = 0; row_idx < old_count; ++row_idx)
	{
		uint8_t *dst = nullptr;
		const uint32_t copied_idx = TupleDataCollectionAppendRow(new_tdc, &dst);
		if (copied_idx == TDC_INVALID_ROW_INDEX)
			elog(ERROR, "pg_volvec: local hash aggregate TDC grow copy overflow");
		const uint8_t *src = TupleDataCollectionGetRowConst(old_tdc, row_idx);
		std::memcpy(dst, src, local.layout->row_width);
	}

	const uint32_t capacity = AggregateHashTableChooseCapacity(new_capacity);
	dsa_pointer new_aht_dp = dsa_allocate0(ctx.dsa, AggregateHashTableAllocSize(capacity));
	AggregateHashTable *new_aht = static_cast<AggregateHashTable *>(dsa_get_address(ctx.dsa, new_aht_dp));
	AggregateHashTableInit(new_aht, capacity, new_tdc_dp);
	AggregateHashTableRehash(new_aht, new_tdc, local.layout);
	part.tdc_dp = new_tdc_dp;
	part.aht_dp = new_aht_dp;
}

}  /* namespace */

int
PhysicalHashAggregate::MaxThreads(ExecCtx &ctx) const
{
	(void) ctx;
	return std::max(1, pg_volvec_parallel_max_workers);
}

std::unique_ptr<GlobalSinkState>
PhysicalHashAggregate::GetGlobalSinkState(ExecCtx &ctx)
{
	auto state = std::make_unique<HashAggGlobalSinkState>();
	state->dsa = ctx.dsa;
	state->desc = desc_;
	state->layout_dp = DsaPointerIsValid(layout_dp_) ? layout_dp_ :
		(desc_ != nullptr ? desc_->body.hash_agg.layout : InvalidDsaPointer);
	state->layout = ResolveLayout(ctx.dsa, state->layout_dp);
	state->shared_payload_dp = DsaPointerIsValid(shared_payload_dp_) ? shared_payload_dp_ :
		(desc_ != nullptr ? desc_->body.hash_agg.shared_payload : InvalidDsaPointer);
	state->max_groups = desc_ != nullptr && desc_->body.hash_agg.max_groups > 0 ?
		desc_->body.hash_agg.max_groups : 256;

	if (state->layout == nullptr)
		elog(ERROR, "pg_volvec: hash aggregate missing TupleDataLayout");

	if (ctx.worker_index == LEADER_WORKER_INDEX && !DsaPointerIsValid(state->shared_payload_dp))
	{
		const uint32_t workers = static_cast<uint32_t>(std::max(1, pg_volvec_parallel_max_workers));
		state->partition_count = HashAggChoosePartitionCount(workers, state->layout->row_width);
		const uint32_t per_partition_groups = PartitionRowCapacity(state->max_groups, state->partition_count);

		state->shared_payload_dp = dsa_allocate0(ctx.dsa, sizeof(HashAggSharedPayload));
		state->payload = ResolvePayload(ctx.dsa, state->shared_payload_dp);
		state->payload->partition_count = state->partition_count;
		state->payload->partition_mask = state->partition_count - 1u;
		state->payload->max_groups = state->max_groups;
		state->payload->local_state_slot_count = static_cast<uint32_t>(std::max(1, pg_volvec_parallel_max_workers));
		state->payload->finalized = false;
		state->payload->partitions_dp = dsa_allocate0(ctx.dsa,
			static_cast<size_t>(state->partition_count) * sizeof(HashAggPartition));
		state->payload->local_partitions_registry_dp = dsa_allocate0(ctx.dsa,
			static_cast<size_t>(state->payload->local_state_slot_count) * sizeof(dsa_pointer));
		state->partitions = ResolvePartitions(ctx.dsa, state->payload);
		auto *registry = ResolveLocalRegistry(ctx.dsa, state->payload);
		for (uint32_t slot = 0; slot < state->payload->local_state_slot_count; ++slot)
			registry[slot] = InvalidDsaPointer;

		for (uint32_t part_idx = 0; part_idx < state->partition_count; ++part_idx)
		{
			HashAggPartition &part = state->partitions[part_idx];
			SpinLockInit(&part.mutex);
			part.tdc_dp = dsa_allocate0(ctx.dsa,
				TupleDataCollectionAllocSize(per_partition_groups, state->layout->row_width));
			auto *tdc = ResolveTdc(ctx.dsa, part.tdc_dp);
			TupleDataCollectionInit(tdc,
				per_partition_groups,
				state->layout->row_width,
				state->layout_dp);
			AggregateHashTable *aht = nullptr;
			AllocAhtForTdc(ctx, part.tdc_dp, per_partition_groups, &part.aht_dp, &aht);
		}
		StoreSharedPayloadOnDescriptor(this, state->shared_payload_dp);
	}
	else
	{
		if (!DsaPointerIsValid(state->shared_payload_dp))
			state->shared_payload_dp = LoadSharedPayloadFromDescriptor(this);
		state->payload = ResolvePayload(ctx.dsa, state->shared_payload_dp);
		state->partitions = ResolvePartitions(ctx.dsa, state->payload);
		state->partition_count = state->payload != nullptr ? state->payload->partition_count : 0;
	}

	if (state->payload == nullptr || state->partitions == nullptr || state->partition_count == 0)
		elog(ERROR, "pg_volvec: hash aggregate global payload not initialized");

	return state;
}

std::unique_ptr<LocalSinkState>
PhysicalHashAggregate::GetLocalSinkState(ExecCtx &ctx, GlobalSinkState &gstate)
{
	auto &global = static_cast<HashAggGlobalSinkState &>(gstate);
	auto state = std::make_unique<HashAggLocalSinkState>();
	state->layout = global.layout;
	state->layout_dp = global.layout_dp;
	state->max_groups = global.max_groups;
	state->partition_count = global.partition_count;
	state->partition_mask = global.payload->partition_mask;
	state->partition_shift = HashAggPartitionShift(state->partition_mask);
	const uint32_t per_partition_groups = PartitionRowCapacity(state->max_groups, state->partition_count);

	state->local_partitions_dp = dsa_allocate0(ctx.dsa,
		static_cast<size_t>(state->partition_count) * sizeof(HashAggPartition));
	state->local_partitions = static_cast<HashAggPartition *>(
		dsa_get_address(ctx.dsa, state->local_partitions_dp));

	for (uint32_t part_idx = 0; part_idx < state->partition_count; ++part_idx)
	{
		HashAggPartition &part = state->local_partitions[part_idx];
		SpinLockInit(&part.mutex);
		part.tdc_dp = dsa_allocate0(ctx.dsa,
			TupleDataCollectionAllocSize(per_partition_groups, state->layout->row_width));
		auto *tdc = ResolveTdc(ctx.dsa, part.tdc_dp);
		TupleDataCollectionInit(tdc,
			per_partition_groups,
			state->layout->row_width,
			global.layout_dp);
		AggregateHashTable *aht = nullptr;
		AllocAhtForTdc(ctx, part.tdc_dp, per_partition_groups, &part.aht_dp, &aht);
	}

	if (ctx.worker_index >= 0)
	{
		auto *registry = ResolveLocalRegistry(ctx.dsa, global.payload);
		if (registry == nullptr || static_cast<uint32_t>(ctx.worker_index) >= global.payload->local_state_slot_count)
			elog(ERROR, "pg_volvec: hash aggregate local registry missing");
		registry[ctx.worker_index] = state->local_partitions_dp;
	}

	return state;
}

SinkResultType
PhysicalHashAggregate::SinkChunk(ExecCtx &ctx, PipelineChunk &in, OperatorSinkInput &input)
{
	auto &local = static_cast<HashAggLocalSinkState &>(input.local_state);
	(void) ctx;
	std::array<uint64_t, PIPELINE_DEFAULT_CHUNK_SIZE> hashes;
	std::array<uint32_t, PIPELINE_DEFAULT_CHUNK_SIZE> partitions;
	std::array<uint16_t, PIPELINE_DEFAULT_CHUNK_SIZE> probe_rows;
	std::array<AggregateHashTableBatchProbeInput, PIPELINE_DEFAULT_CHUNK_SIZE> probe_inputs;
	std::array<AggregateHashTableBatchProbeResult, PIPELINE_DEFAULT_CHUNK_SIZE> probe_results;
	std::array<uint8_t *, PIPELINE_DEFAULT_CHUNK_SIZE> update_rows;
	std::array<uint16_t, PIPELINE_DEFAULT_CHUNK_SIZE> update_row_indices;
	uint16_t update_count = 0;

	for (uint16_t row_idx = 0; row_idx < in.count; ++row_idx)
	{
		hashes[row_idx] = HashGroup(local.layout, in, row_idx);
		partitions[row_idx] = static_cast<uint32_t>(hashes[row_idx] >> local.partition_shift) &
			local.partition_mask;
	}

	for (uint32_t part_idx = 0; part_idx < local.partition_count; ++part_idx)
	{
		uint16_t probe_count = 0;
		for (uint16_t row_idx = 0; row_idx < in.count; ++row_idx)
		{
			if (partitions[row_idx] != part_idx)
				continue;
			probe_rows[probe_count++] = row_idx;
		}
		if (probe_count == 0)
			continue;

		HashAggPartition &part = local.local_partitions[part_idx];
		TupleDataCollection *tdc = ResolveTdc(ctx.dsa, part.tdc_dp);
		if (tdc == nullptr)
			elog(ERROR, "pg_volvec: local hash aggregate partition TDC missing");
		while (pg_atomic_read_u32(&tdc->row_count) >= tdc->row_capacity)
		{
			GrowLocalTdc(ctx, local, part);
			tdc = ResolveTdc(ctx.dsa, part.tdc_dp);
		}
		AggregateHashTable *aht = ResolveAht(ctx.dsa, part.aht_dp);
		for (uint16_t i = 0; i < probe_count; ++i)
		{
			const uint16_t row_idx = probe_rows[i];
			probe_inputs[i].row_idx = row_idx;
			probe_inputs[i].hash = hashes[row_idx];
		}

		AggregateHashTableFindOrInsertBatch(aht,
			tdc,
			local.layout,
			in,
			probe_inputs.data(),
			probe_count,
			probe_results.data());

		if (AggregateHashTableShouldResize(aht, tdc))
		{
			ResizeAhtForTdc(ctx,
				part.tdc_dp,
				tdc,
				local.layout,
				&part.aht_dp);
			tdc = ResolveTdc(ctx.dsa, part.tdc_dp);
			if (tdc == nullptr)
				elog(ERROR, "pg_volvec: local hash aggregate partition TDC missing");
		}

		for (uint16_t i = 0; i < probe_count; ++i)
		{
			update_row_indices[update_count] = probe_results[i].row_idx;
			update_rows[update_count] = TupleDataCollectionGetRow(tdc,
				probe_results[i].canonical_row_idx);
			++update_count;
		}
	}
	UpdateAggregatesBatch(local.layout,
		update_rows.data(),
		in,
		update_row_indices.data(),
		update_count);

	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType
PhysicalHashAggregate::Combine(ExecCtx &ctx, OperatorSinkCombineInput &input)
{
	auto &global = static_cast<HashAggGlobalSinkState &>(input.global_state);
	(void) ctx;

	if (input.partition_id == UINT32_MAX)
	{
		if (input.local_state == nullptr)
			elog(ERROR, "pg_volvec: hash aggregate combine local state missing");
		auto &local = static_cast<HashAggLocalSinkState &>(*input.local_state);
		for (uint32_t local_part_idx = 0; local_part_idx < local.partition_count; ++local_part_idx)
		{
			HashAggPartition &local_part = local.local_partitions[local_part_idx];
			TupleDataCollection *local_tdc = ResolveTdc(ctx.dsa, local_part.tdc_dp);
			if (local_tdc == nullptr)
				elog(ERROR, "pg_volvec: local hash aggregate partition TDC missing");
			const uint32_t local_row_count = pg_atomic_read_u32(&local_tdc->row_count);
			if (local_row_count == 0)
				continue;

			HashAggPartition &global_part = global.partitions[local_part_idx & global.payload->partition_mask];
			SpinLockAcquire(&global_part.mutex);
			{
				for (uint32_t local_row_idx = 0; local_row_idx < local_row_count; ++local_row_idx)
				{
					const uint8_t *src_row = TupleDataCollectionGetRowConst(local_tdc, local_row_idx);
					const uint64_t hash = HashGroupRow(global.layout, src_row);
					TupleDataCollection *global_tdc = ResolveTdc(ctx.dsa, global_part.tdc_dp);
					if (global_tdc == nullptr)
						elog(ERROR, "pg_volvec: hash aggregate partition TDC missing");
					if (pg_atomic_read_u32(&global_tdc->row_count) >= global_tdc->row_capacity)
						GrowTdcForPartition(ctx, global.layout_dp, global.layout, global_part);

					AggregateHashTable *global_aht = ResolveAht(ctx.dsa, global_part.aht_dp);
					global_tdc = ResolveTdc(ctx.dsa, global_part.tdc_dp);
					AggregateHashTableCombineRow(global_aht,
						global_tdc,
						global.layout,
						src_row,
						hash);
					ResizeAhtForTdc(ctx,
						global_part.tdc_dp,
						global_tdc,
						global.layout,
						&global_part.aht_dp);
				}
			}
			SpinLockRelease(&global_part.mutex);
		}
		return SinkCombineResultType::FINISHED;
	}

	if (input.partition_id >= global.partition_count)
		elog(ERROR, "pg_volvec: hash aggregate combine partition %u out of range",
			 input.partition_id);

	dsa_pointer *registry = ResolveLocalRegistry(ctx.dsa, global.payload);
	if (registry == nullptr)
		elog(ERROR, "pg_volvec: hash aggregate local registry missing");

	HashAggPartition &global_part = global.partitions[input.partition_id];
	for (uint32_t slot = 0; slot < global.payload->local_state_slot_count; ++slot)
	{
		if (!DsaPointerIsValid(registry[slot]))
			continue;
		auto *local_parts = static_cast<HashAggPartition *>(dsa_get_address(ctx.dsa, registry[slot]));
		HashAggPartition &local_part = local_parts[input.partition_id];
		TupleDataCollection *local_tdc = ResolveTdc(ctx.dsa, local_part.tdc_dp);
		if (local_tdc == nullptr)
			continue;
		const uint32_t local_row_count = pg_atomic_read_u32(&local_tdc->row_count);
		for (uint32_t local_row_idx = 0; local_row_idx < local_row_count; ++local_row_idx)
		{
			const uint8_t *src_row = TupleDataCollectionGetRowConst(local_tdc, local_row_idx);
			const uint64_t hash = HashGroupRow(global.layout, src_row);
			TupleDataCollection *global_tdc = ResolveTdc(ctx.dsa, global_part.tdc_dp);
			if (global_tdc == nullptr)
				elog(ERROR, "pg_volvec: hash aggregate partition TDC missing");
			if (pg_atomic_read_u32(&global_tdc->row_count) >= global_tdc->row_capacity)
				GrowTdcForPartition(ctx, global.layout_dp, global.layout, global_part);

			AggregateHashTable *global_aht = ResolveAht(ctx.dsa, global_part.aht_dp);
			global_tdc = ResolveTdc(ctx.dsa, global_part.tdc_dp);
			AggregateHashTableCombineRow(global_aht,
				global_tdc,
				global.layout,
				src_row,
				hash);
			ResizeAhtForTdc(ctx,
				global_part.tdc_dp,
				global_tdc,
				global.layout,
				&global_part.aht_dp);
		}
	}

	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType
PhysicalHashAggregate::Finalize(ExecCtx &ctx, GlobalSinkState &gstate)
{
	auto &global = static_cast<HashAggGlobalSinkState &>(gstate);
	(void) ctx;
	for (uint32_t part_idx = 0; part_idx < global.partition_count; ++part_idx)
	{
		TupleDataCollection *tdc = ResolveTdc(global.dsa, global.partitions[part_idx].tdc_dp);
		if (tdc != nullptr)
			tdc->finalized = true;
	}
	global.payload->finalized = true;
	global.finalized = true;
	return SinkFinalizeType::READY;
}

std::unique_ptr<GlobalSourceState>
PhysicalHashAggregate::GetGlobalSourceState(ExecCtx &ctx)
{
	auto state = std::make_unique<HashAggGlobalSourceState>();
	dsa_pointer payload_dp = DsaPointerIsValid(shared_payload_dp_) ? shared_payload_dp_ :
		LoadSharedPayloadFromDescriptor(this);
	state->payload = ResolvePayload(ctx.dsa, payload_dp);
	state->partitions = ResolvePartitions(ctx.dsa, state->payload);
	state->partition_count = state->payload != nullptr ? state->payload->partition_count : 0;
	if (state->payload == nullptr || state->partitions == nullptr || state->partition_count == 0)
		elog(ERROR, "pg_volvec: hash aggregate source payload not initialized");
	TupleDataCollection *first_tdc = ResolveTdc(ctx.dsa, state->partitions[0].tdc_dp);
	state->layout = first_tdc != nullptr ? ResolveLayout(ctx.dsa, first_tdc->layout_dp) : nullptr;
	if (state->layout == nullptr)
		elog(ERROR, "pg_volvec: hash aggregate source layout missing");
	state->finalized = state->payload->finalized;
	return state;
}

std::unique_ptr<LocalSourceState>
PhysicalHashAggregate::GetLocalSourceState(ExecCtx &ctx, GlobalSourceState &gstate)
{
	(void) ctx;
	(void) gstate;
	return std::make_unique<HashAggLocalSourceState>();
}

SourceResultType
PhysicalHashAggregate::GetData(ExecCtx &ctx, PipelineChunk &out, OperatorSourceInput &input)
{
	auto &global = static_cast<HashAggGlobalSourceState &>(input.global_state);
	(void) input.local_state;
	(void) ctx;
	out.reset();

	/* global.finalized is a stale snapshot from GetGlobalSourceState() time;
	 * only the DSA-resident wrapper flag is authoritative across runtimes. */
	if (!global.payload->finalized)
		return SourceResultType::FINISHED;

	while (global.source_partition < global.partition_count &&
	       out.count < PIPELINE_DEFAULT_CHUNK_SIZE)
	{
		TupleDataCollection *tdc = ResolveTdc(ctx.dsa,
			global.partitions[global.source_partition].tdc_dp);
		if (tdc == nullptr || !tdc->finalized)
			return SourceResultType::FINISHED;

		const uint32_t row_count = pg_atomic_read_u32(&tdc->row_count);
		if (global.source_cursor >= row_count)
		{
			global.source_partition++;
			global.source_cursor = 0;
			continue;
		}

		const uint8_t *row = TupleDataCollectionGetRowConst(tdc, global.source_cursor++);
		Gather(global.layout, row, out, out.count);
		++out.count;
	}

	return out.count > 0 ? SourceResultType::HAVE_MORE_OUTPUT : SourceResultType::FINISHED;
}

std::unique_ptr<OperatorState>
PhysicalHashAggregate::GetOperatorState(ExecCtx &ctx)
{
	(void) ctx;
	return std::make_unique<HashAggOperatorState>();
}

OperatorResultType
PhysicalHashAggregate::Execute(ExecCtx &ctx, PipelineChunk &in, PipelineChunk &out, OperatorState &state)
{
	(void) ctx;
	(void) state;
	out = in;
	return out.count > 0 ? OperatorResultType::HAVE_MORE_OUTPUT
	                      : OperatorResultType::NEED_MORE_INPUT;
}

}  /* namespace pipeline */
}  /* namespace pg_volvec */

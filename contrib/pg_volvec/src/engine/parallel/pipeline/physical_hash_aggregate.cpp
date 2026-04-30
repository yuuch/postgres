#include "parallel/pipeline/physical_hash_aggregate.hpp"

extern "C" {
#include "postgres.h"
#include "utils/elog.h"

extern int pg_volvec_parallel_max_workers;
}

#include <algorithm>
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

static TupleDataCollection *
ResolveTdc(dsa_area *dsa, AggregateHashTable *aht)
{
	if (aht == nullptr || !DsaPointerIsValid(aht->tdc_dp))
		return nullptr;
	return static_cast<TupleDataCollection *>(dsa_get_address(dsa, aht->tdc_dp));
}

static uint64_t
HashGroupRow(const TupleDataLayout *layout, const uint8_t *row_ptr)
{
	constexpr uint64_t FNV_OFFSET = UINT64CONST(0xcbf29ce484222325);
	constexpr uint64_t FNV_PRIME = UINT64CONST(0x100000001b3);
	uint64_t hash = FNV_OFFSET;

	for (uint16_t col_idx = 0; col_idx < layout->column_count; ++col_idx)
	{
		const TdcColumnDesc &col = layout->columns[col_idx];
		for (uint16_t byte_idx = 0; byte_idx < col.width; ++byte_idx)
		{
			hash ^= static_cast<uint64_t>(row_ptr[col.offset + byte_idx]);
			hash *= FNV_PRIME;
		}
	}

	return hash;
}

static void
RollbackLastAppend(TupleDataCollection *tdc, uint32_t appended_row_idx)
{
	const uint32_t current = pg_atomic_read_u32(&tdc->row_count);
	if (current == appended_row_idx + 1)
		pg_atomic_write_u32(&tdc->row_count, appended_row_idx);
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
		dsa_pointer tdc_dp = dsa_allocate0(ctx.dsa,
			TupleDataCollectionAllocSize(state->max_groups, state->layout->row_width));
		state->global_tdc = static_cast<TupleDataCollection *>(dsa_get_address(ctx.dsa, tdc_dp));
		TupleDataCollectionInit(state->global_tdc,
			state->max_groups,
			state->layout->row_width,
			state->layout_dp);

		const uint32_t capacity = AggregateHashTableChooseCapacity(state->max_groups);
		state->shared_payload_dp = dsa_allocate0(ctx.dsa, AggregateHashTableAllocSize(capacity));
		state->global_aht = static_cast<AggregateHashTable *>(dsa_get_address(ctx.dsa, state->shared_payload_dp));
		AggregateHashTableInit(state->global_aht, capacity, tdc_dp);
		StoreSharedPayloadOnDescriptor(this, state->shared_payload_dp);
	}
	else
	{
		if (!DsaPointerIsValid(state->shared_payload_dp))
			state->shared_payload_dp = LoadSharedPayloadFromDescriptor(this);
		state->global_aht = ResolveAht(ctx.dsa, state->shared_payload_dp);
		state->global_tdc = ResolveTdc(ctx.dsa, state->global_aht);
	}

	if (state->global_aht == nullptr || state->global_tdc == nullptr)
		elog(ERROR, "pg_volvec: hash aggregate global payload not initialized");

	return state;
}

std::unique_ptr<LocalSinkState>
PhysicalHashAggregate::GetLocalSinkState(ExecCtx &ctx, GlobalSinkState &gstate)
{
	auto &global = static_cast<HashAggGlobalSinkState &>(gstate);
	auto state = std::make_unique<HashAggLocalSinkState>();
	state->layout = global.layout;
	state->max_groups = global.max_groups;

	state->local_tdc_dp = dsa_allocate0(ctx.dsa,
		TupleDataCollectionAllocSize(state->max_groups, state->layout->row_width));
	state->local_tdc = static_cast<TupleDataCollection *>(dsa_get_address(ctx.dsa, state->local_tdc_dp));
	TupleDataCollectionInit(state->local_tdc,
		state->max_groups,
		state->layout->row_width,
		global.layout_dp);

	const uint32_t capacity = AggregateHashTableChooseCapacity(256);
	state->local_aht_dp = dsa_allocate0(ctx.dsa, AggregateHashTableAllocSize(capacity));
	state->local_aht = static_cast<AggregateHashTable *>(dsa_get_address(ctx.dsa, state->local_aht_dp));
	AggregateHashTableInit(state->local_aht, capacity, state->local_tdc_dp);

	return state;
}

SinkResultType
PhysicalHashAggregate::SinkChunk(ExecCtx &ctx, PipelineChunk &in, OperatorSinkInput &input)
{
	auto &local = static_cast<HashAggLocalSinkState &>(input.local_state);
	(void) ctx;

	for (uint16_t row_idx = 0; row_idx < in.count; ++row_idx)
	{
		uint8_t *candidate_row = nullptr;
		const uint32_t candidate_idx = TupleDataCollectionAppendRow(local.local_tdc, &candidate_row);
		if (candidate_idx == TDC_INVALID_ROW_INDEX)
			elog(ERROR, "pg_volvec: local hash aggregate row capacity exceeded");

		ScatterGroupOnly(local.layout, candidate_row, in, row_idx);
		const uint64_t hash = HashGroup(local.layout, in, row_idx);

		uint32_t canonical_idx = TDC_INVALID_ROW_INDEX;
		const bool inserted = AggregateHashTableFindOrInsert(local.local_aht,
			local.local_tdc,
			local.layout,
			candidate_idx,
			candidate_row,
			hash,
			&canonical_idx);
		if (!inserted)
			RollbackLastAppend(local.local_tdc, candidate_idx);

		uint8_t *canonical_row = TupleDataCollectionGetRow(local.local_tdc, canonical_idx);
		UpdateAggregates(local.layout, canonical_row, in, row_idx);
	}

	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType
PhysicalHashAggregate::Combine(ExecCtx &ctx, OperatorSinkCombineInput &input)
{
	auto &global = static_cast<HashAggGlobalSinkState &>(input.global_state);
	auto &local = static_cast<HashAggLocalSinkState &>(input.local_state);
	(void) ctx;

	/* Per-worker (incl. leader if it RAN) merge of process-private local_tdc
	 * into the DSA-resident global_tdc. Bug F: leader-only Combine drops
	 * worker partials. Bug P: AggregateHashTableCombineRow holds the AHT
	 * mutex across probe + (optional) append + group-col copy + agg merge,
	 * preventing the speculative-append rollback race that wake-on-pop
	 * (Bug O) exposed — concurrent COMBINE workers must not strand a
	 * partially-populated row at the global TDC tail. */
	const uint32_t local_row_count = pg_atomic_read_u32(&local.local_tdc->row_count);
	for (uint32_t local_row_idx = 0; local_row_idx < local_row_count; ++local_row_idx)
	{
		const uint8_t *src_row = TupleDataCollectionGetRowConst(local.local_tdc, local_row_idx);
		const uint64_t hash = HashGroupRow(global.layout, src_row);
		AggregateHashTableCombineRow(global.global_aht,
			global.global_tdc,
			global.layout,
			src_row,
			hash);
	}

	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType
PhysicalHashAggregate::Finalize(ExecCtx &ctx, GlobalSinkState &gstate)
{
	auto &global = static_cast<HashAggGlobalSinkState &>(gstate);
	(void) ctx;
	global.global_tdc->finalized = true;
	global.finalized = true;
	return SinkFinalizeType::READY;
}

std::unique_ptr<GlobalSourceState>
PhysicalHashAggregate::GetGlobalSourceState(ExecCtx &ctx)
{
	auto state = std::make_unique<HashAggGlobalSourceState>();
	dsa_pointer payload_dp = DsaPointerIsValid(shared_payload_dp_) ? shared_payload_dp_ :
		LoadSharedPayloadFromDescriptor(this);
	state->global_aht = ResolveAht(ctx.dsa, payload_dp);
	state->global_tdc = ResolveTdc(ctx.dsa, state->global_aht);
	if (state->global_tdc == nullptr)
		elog(ERROR, "pg_volvec: hash aggregate source payload not initialized");
	state->layout = ResolveLayout(ctx.dsa, state->global_tdc->layout_dp);
	if (state->layout == nullptr)
		elog(ERROR, "pg_volvec: hash aggregate source layout missing");
	state->finalized = state->global_tdc->finalized;
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

	/* global.finalized is a stale snapshot from GetGlobalSourceState() time
	 * (leader pre-init runs before sink Finalize); only the DSA-resident
	 * global_tdc->finalized is authoritative across runtimes. */
	if (!global.global_tdc->finalized)
		return SourceResultType::FINISHED;

	while (global.source_cursor < pg_atomic_read_u32(&global.global_tdc->row_count) &&
	       out.count < PIPELINE_DEFAULT_CHUNK_SIZE)
	{
		const uint8_t *row = TupleDataCollectionGetRowConst(global.global_tdc, global.source_cursor++);
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

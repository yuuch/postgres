#include "parallel/pipeline/physical_order.hpp"

extern "C" {
#include "postgres.h"
#include "utils/elog.h"
}

#include <algorithm>
#include <cstring>
#include <vector>

#include "core/data_chunk.hpp"
#include "parallel/pipeline/pipeline_descriptor.hpp"
#include "parallel/pipeline/tuple_data_ops.hpp"
#include "parallel/pipeline/types.hpp"

namespace pg_volvec {
namespace pipeline {

namespace {

static int
CompareKeyColumn(TdcColumnKind kind, const uint8_t *a, const uint8_t *b)
{
	switch (kind)
	{
		case TdcColumnKind::INT32:
		{
			int32_t va, vb;
			std::memcpy(&va, a, sizeof(int32_t));
			std::memcpy(&vb, b, sizeof(int32_t));
			return (va < vb) ? -1 : (va > vb) ? 1 : 0;
		}
		case TdcColumnKind::INT64:
		{
			int64_t va, vb;
			std::memcpy(&va, a, sizeof(int64_t));
			std::memcpy(&vb, b, sizeof(int64_t));
			return (va < vb) ? -1 : (va > vb) ? 1 : 0;
		}
		case TdcColumnKind::DOUBLE:
		{
			double va, vb;
			std::memcpy(&va, a, sizeof(double));
			std::memcpy(&vb, b, sizeof(double));
			return (va < vb) ? -1 : (va > vb) ? 1 : 0;
		}
	}
	return 0;
}

}  /* namespace */

std::unique_ptr<GlobalSinkState>
PhysicalOrder::GetGlobalSinkState(ExecCtx &ctx)
{
	auto state = std::make_unique<OrderGlobalState>();
	state->dsa = ctx.dsa;
	state->desc = desc_;
	state->key_layout_dp = key_layout_dp_;
	state->payload_layout_dp = payload_layout_dp_;
	state->shared_payload_dp = shared_payload_dp_;
	state->max_rows = desc_ != nullptr ? desc_->body.order.max_rows : 256;

	if (!DsaPointerIsValid(state->key_layout_dp) && desc_ != nullptr)
		state->key_layout_dp = desc_->body.order.key_layout;
	if (!DsaPointerIsValid(state->payload_layout_dp) && desc_ != nullptr)
		state->payload_layout_dp = desc_->body.order.payload_layout;
	if (DsaPointerIsValid(state->key_layout_dp))
		state->key_layout = static_cast<const TupleDataLayout *>(dsa_get_address(ctx.dsa, state->key_layout_dp));
	if (DsaPointerIsValid(state->payload_layout_dp))
		state->payload_layout = static_cast<const TupleDataLayout *>(dsa_get_address(ctx.dsa, state->payload_layout_dp));

	if (!DsaPointerIsValid(state->shared_payload_dp))
		state->shared_payload_dp = LoadSharedPayloadFromDescriptor(this);

	if (ctx.worker_index == LEADER_WORKER_INDEX && !DsaPointerIsValid(state->shared_payload_dp))
	{
		const uint32_t row_width = state->payload_layout != nullptr ? state->payload_layout->row_width : 8;
		state->shared_payload_dp = dsa_allocate0(ctx.dsa,
			TupleDataCollectionAllocSize(state->max_rows, row_width));
		state->payload = static_cast<TupleDataCollection *>(dsa_get_address(ctx.dsa, state->shared_payload_dp));
		TupleDataCollectionInit(state->payload,
			state->max_rows,
			row_width,
			state->payload_layout_dp);
		StoreSharedPayloadOnDescriptor(this, state->shared_payload_dp);
	}
	else if (DsaPointerIsValid(state->shared_payload_dp) ||
			 (state->shared_payload_dp = LoadSharedPayloadFromDescriptor(this), DsaPointerIsValid(state->shared_payload_dp)))
	{
		state->payload = static_cast<TupleDataCollection *>(dsa_get_address(ctx.dsa, state->shared_payload_dp));
	}
	else
	{
	}

	return state;
}

std::unique_ptr<LocalSinkState>
PhysicalOrder::GetLocalSinkState(ExecCtx &ctx, GlobalSinkState &gstate)
{
	(void) ctx;
	(void) gstate;
	return std::make_unique<OrderLocalState>();
}

SinkResultType
PhysicalOrder::SinkChunk(ExecCtx &ctx, PipelineChunk &in, OperatorSinkInput &input)
{
	auto &global = static_cast<OrderGlobalState &>(input.global_state);
	(void) ctx;
	(void) input.local_state;

	if (global.payload == nullptr || global.payload_layout == nullptr)
		elog(ERROR, "pg_volvec: order sink payload not initialized");

	for (uint16_t row_idx = 0; row_idx < in.count; ++row_idx)
	{
		uint8_t *row_ptr = nullptr;
		const uint32_t appended = TupleDataCollectionAppendRow(global.payload, &row_ptr);
		if (appended == TDC_INVALID_ROW_INDEX)
			elog(ERROR, "pg_volvec: order sink row capacity exceeded");
		Scatter(global.payload_layout, row_ptr, in, row_idx);
	}

	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType
PhysicalOrder::Combine(ExecCtx &ctx, OperatorSinkCombineInput &input)
{
	(void) ctx;
	(void) input;
	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType
PhysicalOrder::Finalize(ExecCtx &ctx, GlobalSinkState &gstate)
{
	auto &global = static_cast<OrderGlobalState &>(gstate);

	if (global.payload == nullptr)
		return SinkFinalizeType::READY;
	if (global.key_layout == nullptr || global.payload_layout == nullptr)
		elog(ERROR, "pg_volvec: order finalize missing key/payload layouts");

	const uint32_t row_count = pg_atomic_read_u32(&global.payload->row_count);

	const Size indices_bytes = sizeof(OrderSortIndices) +
		static_cast<Size>(row_count) * sizeof(uint32_t);
	global.sort_indices_dp = dsa_allocate0(ctx.dsa, indices_bytes);
	auto *indices_obj = static_cast<OrderSortIndices *>(
		dsa_get_address(ctx.dsa, global.sort_indices_dp));
	indices_obj->count = row_count;
	for (uint32_t i = 0; i < row_count; ++i)
		indices_obj->indices[i] = i;
	if (global.desc != nullptr)
		global.desc->body.order.sort_indices = global.sort_indices_dp;
	/* Fan-out to all descriptor slots so the consumer-pipeline source-side
	 * resolves the same indices (Order is both P1.sink and P0.source; each
	 * slot is a separate descriptor entry per Fix A2). */
	for (OpDescriptor *d : desc_list_)
		if (d != nullptr)
			d->body.order.sort_indices = global.sort_indices_dp;

	const TupleDataLayout *key_layout = global.key_layout;
	const TupleDataCollection *payload = global.payload;

	std::sort(indices_obj->indices,
	          indices_obj->indices + row_count,
	          [key_layout, payload](uint32_t lhs, uint32_t rhs)
	{
		const uint8_t *row_a = TupleDataCollectionGetRowConst(payload, lhs);
		const uint8_t *row_b = TupleDataCollectionGetRowConst(payload, rhs);
		for (uint16_t k = 0; k < key_layout->column_count; ++k)
		{
			const TdcColumnDesc &col = key_layout->columns[k];
			const int cmp = CompareKeyColumn(col.kind,
			                                 row_a + col.offset,
			                                 row_b + col.offset);
			if (cmp != 0)
				return cmp < 0;
		}
		return false;
	});

	global.payload->finalized = true;
	return SinkFinalizeType::READY;
}

std::unique_ptr<GlobalSourceState>
PhysicalOrder::GetGlobalSourceState(ExecCtx &ctx)
{
	auto state = std::make_unique<OrderGlobalState>();
	state->dsa = ctx.dsa;
	state->desc = desc_;
	state->key_layout_dp = key_layout_dp_;
	state->payload_layout_dp = payload_layout_dp_;
	state->shared_payload_dp = shared_payload_dp_;

	if (!DsaPointerIsValid(state->key_layout_dp) && desc_ != nullptr)
		state->key_layout_dp = desc_->body.order.key_layout;
	if (!DsaPointerIsValid(state->payload_layout_dp) && desc_ != nullptr)
		state->payload_layout_dp = desc_->body.order.payload_layout;
	if (DsaPointerIsValid(state->key_layout_dp))
		state->key_layout = static_cast<const TupleDataLayout *>(dsa_get_address(ctx.dsa, state->key_layout_dp));
	if (DsaPointerIsValid(state->payload_layout_dp))
		state->payload_layout = static_cast<const TupleDataLayout *>(dsa_get_address(ctx.dsa, state->payload_layout_dp));
	if (!DsaPointerIsValid(state->shared_payload_dp))
		state->shared_payload_dp = LoadSharedPayloadFromDescriptor(this);
	if (DsaPointerIsValid(state->shared_payload_dp))
		state->payload = static_cast<TupleDataCollection *>(dsa_get_address(ctx.dsa, state->shared_payload_dp));
	if (desc_ != nullptr)
		state->sort_indices_dp = desc_->body.order.sort_indices;
	return state;
}

std::unique_ptr<LocalSourceState>
PhysicalOrder::GetLocalSourceState(ExecCtx &ctx, GlobalSourceState &gstate)
{
	(void) ctx;
	(void) gstate;
	return std::make_unique<OrderLocalState>();
}

SourceResultType
PhysicalOrder::GetData(ExecCtx &ctx, PipelineChunk &out, OperatorSourceInput &input)
{
	auto &global = static_cast<OrderGlobalState &>(input.global_state);
	auto &local = static_cast<OrderLocalState &>(input.local_state);
	out.reset();

	if (global.payload == nullptr || !global.payload->finalized)
		return SourceResultType::FINISHED;
	if (global.payload_layout == nullptr)
		elog(ERROR, "pg_volvec: order source missing payload layout");
	if (!DsaPointerIsValid(global.sort_indices_dp))
		elog(ERROR, "pg_volvec: order source missing sort indices");

	auto *indices_obj = static_cast<const OrderSortIndices *>(
		dsa_get_address(ctx.dsa, global.sort_indices_dp));

	while (local.source_cursor < indices_obj->count &&
	       out.count < PIPELINE_DEFAULT_CHUNK_SIZE)
	{
		const uint32_t row_idx = indices_obj->indices[local.source_cursor++];
		const uint8_t *row_ptr = TupleDataCollectionGetRowConst(global.payload, row_idx);
		Gather(global.payload_layout, row_ptr, out, out.count);
		++out.count;
	}

	return out.count > 0 ? SourceResultType::HAVE_MORE_OUTPUT : SourceResultType::FINISHED;
}

}  /* namespace pipeline */
}  /* namespace pg_volvec */

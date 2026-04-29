#include "parallel/pipeline/output_sink.hpp"

#include <cstring>

extern "C" {
#include "postgres.h"
#include "executor/executor.h"
#include "executor/tuptable.h"
#include "port/atomics.h"
#include "utils/date.h"
#include "utils/dsa.h"
#include "utils/elog.h"
#include "utils/numeric.h"
}

#include "core/data_chunk.hpp"
#include "parallel/pipeline/pipeline_descriptor.hpp"
#include "parallel/pipeline/tuple_data_collection.hpp"
#include "parallel/pipeline/tuple_data_layout.hpp"
#include "parallel/pipeline/tuple_data_ops.hpp"

namespace pg_volvec {
namespace pipeline {

namespace {

static Datum
EncodeColumn(const ColumnSchema &col,
             const TupleDataLayout *layout,
             const PipelineChunk &in,
             uint16_t row)
{
	switch (col.decode_kind)
	{
		case ColumnDecodeKind::INT32_CHAR:
			return CharGetDatum(static_cast<char>(in.int32_columns[col.chunk_slot][row]));
		case ColumnDecodeKind::INT32_INT4:
			return Int32GetDatum(in.int32_columns[col.chunk_slot][row]);
		case ColumnDecodeKind::INT32_DATE:
			return DateADTGetDatum(static_cast<DateADT>(in.int32_columns[col.chunk_slot][row]));
		case ColumnDecodeKind::INT64_INT8:
			return Int64GetDatum(in.int64_columns[col.chunk_slot][row]);
		case ColumnDecodeKind::DOUBLE_FLOAT8:
			return Float8GetDatum(in.double_columns[col.chunk_slot][row]);
		case ColumnDecodeKind::INT64_NUMERIC_SCALED:
		{
			/* AVG is handled by the caller (it needs row_ptr to read count
			 * from the aggregate state's second 8 bytes). For SUM_NUMERIC the
			 * scale lives on the matching aggregate descriptor; layout column
			 * count is the boundary between group cols and agg state cols. */
			const uint16_t slot = col.chunk_slot;
			int16_t scale = 0;
			if (slot < layout->column_count)
				scale = layout->columns[slot].numeric_scale;
			else
				scale = layout->aggregates[slot - layout->column_count].numeric_scale;
			return NumericGetDatum(
				int64_div_fast_to_numeric(in.int64_columns[slot][row], scale));
		}
		case ColumnDecodeKind::NONE:
			elog(ERROR, "pg_volvec: output column decode_kind=NONE invalid for sink");
			return (Datum) 0;
	}
	elog(ERROR, "pg_volvec: unknown decode_kind %u", static_cast<unsigned>(col.decode_kind));
	return (Datum) 0;
}

static const TupleDataLayout *
ResolveLayout(dsa_area *dsa, dsa_pointer layout_dp)
{
	if (!DsaPointerIsValid(layout_dp))
		return nullptr;
	return static_cast<const TupleDataLayout *>(dsa_get_address(dsa, layout_dp));
}

static TupleDataCollection *
ResolveTdc(dsa_area *dsa, dsa_pointer tdc_dp)
{
	if (!DsaPointerIsValid(tdc_dp))
		return nullptr;
	return static_cast<TupleDataCollection *>(dsa_get_address(dsa, tdc_dp));
}

}

std::unique_ptr<GlobalSinkState>
OutputSink::GetGlobalSinkState(ExecCtx &ctx)
{
	auto state = std::make_unique<OutputGlobalState>();

	/* Resolve descriptor-resident DSA references shared across workers + leader.
	 * Prefer translator-set member dps; fall back to the descriptor (workers
	 * reconstruct OutputSink with the same dps but the descriptor is the
	 * canonical publish point — see pipeline_descriptor.cpp:240-244).
	 *
	 * desc_ may legitimately be nullptr on the leader path (translator passes
	 * desc=nullptr in the 6-arg leader ctor; the descriptor is built later and
	 * never re-attached to the leader's OutputSink). When member dps are all
	 * valid the descriptor fallback is never consulted, so a null desc_ is
	 * fine. We only fault if a member dp is missing AND desc_ is also null. */
	const dsa_pointer schema_dp = DsaPointerIsValid(input_schema_dp_) ?
		input_schema_dp_ : (desc_ ? desc_->body.output.input_schema : InvalidDsaPointer);
	const dsa_pointer layout_dp = DsaPointerIsValid(layout_dp_) ?
		layout_dp_ : (desc_ ? desc_->body.output.layout : InvalidDsaPointer);
	dsa_pointer payload_dp = DsaPointerIsValid(shared_payload_dp_) ?
		shared_payload_dp_ : (desc_ ? desc_->body.output.shared_payload : InvalidDsaPointer);

	if (!DsaPointerIsValid(schema_dp))
		elog(ERROR, "pg_volvec: output sink missing input_schema");
	if (!DsaPointerIsValid(layout_dp))
		elog(ERROR, "pg_volvec: output sink missing TupleDataLayout");

	state->input_schema = static_cast<const SchemaDescriptor *>(
		dsa_get_address(ctx.dsa, schema_dp));
	state->layout = ResolveLayout(ctx.dsa, layout_dp);
	if (state->layout == nullptr)
		elog(ERROR, "pg_volvec: output sink layout resolve failed");

	/* Global TDC is pre-allocated + Init'd by Translator at descriptor-build
	 * time and either threaded directly through shared_payload_dp_ (leader
	 * ctor path) or republished via desc_->body.output.shared_payload (worker
	 * ctor path); both leader and workers only attach here. See translator.cpp
	 * around the OutputSink ctor for the alloc site and rationale (OUTPUT
	 * pipeline RUN tasks fan out across all workers, so no leader-first gate
	 * is available at attach time). */
	if (!DsaPointerIsValid(payload_dp))
		elog(ERROR, "pg_volvec: output sink shared_payload not published by translator");
	state->global_tdc = ResolveTdc(ctx.dsa, payload_dp);

	if (state->global_tdc == nullptr)
		elog(ERROR, "pg_volvec: output sink global TDC resolve failed");

	state->shared_payload_dp = payload_dp;

	/* Leader-only artifacts: dest/tupdesc/slot are owned in the leader process
	 * and consumed by EmitGlobalTdcToDest after FINALIZE; workers leave these
	 * null and only stage rows into the shared TDC. */
	if (ctx.worker_index == LEADER_WORKER_INDEX && dest_ != nullptr)
	{
		state->dest = dest_;
		state->tupdesc = tupdesc_;
		state->slot = MakeSingleTupleTableSlot(tupdesc_, &TTSOpsVirtual);
	}

	return state;
}

std::unique_ptr<LocalSinkState>
OutputSink::GetLocalSinkState(ExecCtx &ctx, GlobalSinkState &gstate)
{
	(void) ctx;
	(void) gstate;
	return std::make_unique<OutputLocalState>();
}

SinkResultType
OutputSink::SinkChunk(ExecCtx &ctx, PipelineChunk &in, OperatorSinkInput &input)
{
	(void) ctx;
	auto &global = static_cast<OutputGlobalState &>(input.global_state);
	auto &local = static_cast<OutputLocalState &>(input.local_state);

	fprintf(stderr,
		"PGVOLVEC_DIAG[worker_idx=%d pid=%d]: Output.SinkChunk ENTER count=%u global_tdc=%p\n",
		ctx.worker_index, (int) getpid(), (unsigned) in.count,
		(void *) global.global_tdc);

	if (global.global_tdc == nullptr || global.layout == nullptr)
		elog(ERROR, "pg_volvec: output sink not initialized");

	for (uint16_t row = 0; row < in.count; ++row)
	{
		uint8_t *row_ptr = nullptr;
		const uint32_t row_idx = TupleDataCollectionAppendRow(global.global_tdc, &row_ptr);
		if (row_idx == TDC_INVALID_ROW_INDEX)
			elog(ERROR, "pg_volvec: output sink row capacity %u exceeded",
			     global.global_tdc->row_capacity);

		/* OutputSink layout has columns only (no aggregates); Scatter writes
		 * exactly layout->columns[0..N-1] from the input chunk slots, matching
		 * the column-decode metadata in the parallel SchemaDescriptor. */
		Scatter(global.layout, row_ptr, in, row);
		++local.emitted_rows;
	}

	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType
OutputSink::Combine(ExecCtx &ctx, OperatorSinkCombineInput &input)
{
	/* Single shared TDC: workers append directly under the TDC spinlock, so
	 * there is no per-thread local payload to merge. Combine is a no-op. */
	(void) ctx;
	(void) input;
	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType
OutputSink::Finalize(ExecCtx &ctx, GlobalSinkState &gstate)
{
	(void) ctx;
	auto &global = static_cast<OutputGlobalState &>(gstate);

	fprintf(stderr,
		"PGVOLVEC_DIAG[worker_idx=%d pid=%d]: Output.Finalize ENTER global_tdc=%p row_count=%u\n",
		ctx.worker_index, (int) getpid(), (void *) global.global_tdc,
		global.global_tdc != nullptr ? pg_atomic_read_u32(&global.global_tdc->row_count) : 0u);

	/* Publish sink->source handoff: Output has no downstream operator, but
	 * EmitGlobalTdcToDest gates on this flag so the leader cannot drain a
	 * partially-populated TDC if Finalize is somehow re-entered. */
	if (global.global_tdc != nullptr)
		global.global_tdc->finalized = true;
	global.finalized = true;

	return SinkFinalizeType::READY;
}

void
OutputSink::EmitGlobalTdcToDest(ExecCtx &ctx)
{
	if (dest_ == nullptr)
		return;  /* Worker-side reconstruct never has a dest; leader-only path. */

	const dsa_pointer schema_dp = DsaPointerIsValid(input_schema_dp_) ?
		input_schema_dp_ : (desc_ ? desc_->body.output.input_schema : InvalidDsaPointer);
	const dsa_pointer layout_dp = DsaPointerIsValid(layout_dp_) ?
		layout_dp_ : (desc_ ? desc_->body.output.layout : InvalidDsaPointer);
	const dsa_pointer payload_dp = DsaPointerIsValid(shared_payload_dp_) ?
		shared_payload_dp_ : (desc_ ? desc_->body.output.shared_payload : InvalidDsaPointer);

	if (!DsaPointerIsValid(schema_dp) || !DsaPointerIsValid(layout_dp) ||
	    !DsaPointerIsValid(payload_dp))
		elog(ERROR, "pg_volvec: EmitGlobalTdcToDest missing DSA references");

	const auto *schema = static_cast<const SchemaDescriptor *>(
		dsa_get_address(ctx.dsa, schema_dp));
	const auto *layout = ResolveLayout(ctx.dsa, layout_dp);
	auto *tdc = ResolveTdc(ctx.dsa, payload_dp);
	if (schema == nullptr || layout == nullptr || tdc == nullptr)
		elog(ERROR, "pg_volvec: EmitGlobalTdcToDest resolve failed");
	if (!tdc->finalized)
		elog(ERROR, "pg_volvec: EmitGlobalTdcToDest invoked before TDC finalized");

	const uint16_t natts = schema->n_columns;
	if (tupdesc_ == nullptr || natts > tupdesc_->natts)
		elog(ERROR, "pg_volvec: output schema has %u columns but tupdesc has %d",
		     natts, tupdesc_ != nullptr ? tupdesc_->natts : 0);

	TupleTableSlot *slot = MakeSingleTupleTableSlot(tupdesc_, &TTSOpsVirtual);

	/* Stage one TDC row at a time into a single-row PipelineChunk so we can
	 * reuse Gather + EncodeColumn (both consume PipelineChunk-shaped input).
	 * One-row staging is intentional for v1: avoids allocating a 1024-row
	 * chunk just to encode-and-discard, and matches the row-at-a-time
	 * DestReceiver contract. */
	auto staging = std::make_unique<PipelineChunk>();
	staging->reset();

	const uint32_t row_count = pg_atomic_read_u32(&tdc->row_count);
	for (uint32_t i = 0; i < row_count; ++i)
	{
		const uint8_t *row_ptr = TupleDataCollectionGetRowConst(tdc, i);

		staging->reset();
		Gather(layout, row_ptr, *staging, 0);
		staging->count = 1;

		ExecClearTuple(slot);
		for (uint16_t c = 0; c < natts; ++c)
		{
			const ColumnSchema &col = schema->columns[c];

			/* AVG_NUMERIC needs the count half (row_ptr+offset+8); Gather
			 * only places the sum in the chunk slot, so we bypass Encode
			 * and compute sum/count at scale=2 directly. */
			if (col.decode_kind == ColumnDecodeKind::INT64_NUMERIC_SCALED &&
			    col.chunk_slot >= layout->column_count)
			{
				const uint16_t a = col.chunk_slot - layout->column_count;
				if (a < layout->aggregate_count &&
				    layout->aggregates[a].kind == TdcAggKind::AVG_NUMERIC)
				{
					const uint8_t *agg_ptr = row_ptr + layout->aggregates[a].offset;
					int64 sum, count;
					std::memcpy(&sum,   agg_ptr,     sizeof(int64));
					std::memcpy(&count, agg_ptr + 8, sizeof(int64));
					const int64 avg_scaled = (count != 0) ? (sum / count) : 0;
					slot->tts_values[c] = NumericGetDatum(
						int64_div_fast_to_numeric(avg_scaled, 2));
					slot->tts_isnull[c] = (count == 0);
					continue;
				}
			}

			slot->tts_values[c] = EncodeColumn(col, layout, *staging, 0);
			slot->tts_isnull[c] = false;
		}
		ExecStoreVirtualTuple(slot);
		dest_->receiveSlot(slot, dest_);
	}

	ExecDropSingleTupleTableSlot(slot);
}

}  /* namespace pipeline */
}  /* namespace pg_volvec */

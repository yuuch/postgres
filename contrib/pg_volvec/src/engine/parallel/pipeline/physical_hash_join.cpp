#include "parallel/pipeline/physical_hash_join.hpp"

extern "C" {
#include "postgres.h"
#include "utils/elog.h"

extern int pg_volvec_parallel_max_workers;
extern bool pg_volvec_trace_execution_path;
}

#include <algorithm>
#include <cstring>

#include "core/data_chunk.hpp"
#include "parallel/pipeline/cancel.hpp"
#include "parallel/pipeline/meta_pipeline.hpp"
#include "parallel/pipeline/pipeline.hpp"
#include "parallel/pipeline/tuple_data_collection.hpp"
#include "parallel/pipeline/tuple_data_ops.hpp"

namespace pg_volvec {
namespace pipeline {

namespace {

static constexpr uint32_t HASH_JOIN_INVALID_ROW = UINT32_MAX;
static constexpr uint32_t HASH_JOIN_MAX_INITIAL_ROWS = 1u << 20;

static uint32_t
HashJoinInitialRows(uint32_t estimated_rows)
{
	return std::max<uint32_t>(1024u, std::min<uint32_t>(estimated_rows, HASH_JOIN_MAX_INITIAL_ROWS));
}

static HashJoinSharedPayload *
ResolvePayload(dsa_area *dsa, dsa_pointer payload_dp)
{
	if (!DsaPointerIsValid(payload_dp))
		return nullptr;
	return static_cast<HashJoinSharedPayload *>(dsa_get_address(dsa, payload_dp));
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

static HashJoinLocalBuildRegistryEntry *
ResolveLocalBuildRegistry(dsa_area *dsa, HashJoinSharedPayload *payload)
{
	if (payload == nullptr || !DsaPointerIsValid(payload->local_build_registry_dp))
		return nullptr;
	return static_cast<HashJoinLocalBuildRegistryEntry *>(
		dsa_get_address(dsa, payload->local_build_registry_dp));
}

static void
FreeDsaPointerIfValid(dsa_area *dsa, dsa_pointer *dp)
{
	if (dp != nullptr && DsaPointerIsValid(*dp))
	{
		dsa_free(dsa, *dp);
		*dp = InvalidDsaPointer;
	}
}

static uint64_t
EstimateTdcAllocBytes(dsa_area *dsa, dsa_pointer tdc_dp)
{
	TupleDataCollection *tdc = ResolveTdc(dsa, tdc_dp);
	if (tdc == nullptr)
		return 0;
	return static_cast<uint64_t>(TupleDataCollectionAllocSize(tdc->row_capacity,
		tdc->row_width,
		tdc->heap_capacity));
}

static void
CopyBuildRow(const TupleDataLayout *layout,
	         const TupleDataCollection *src_tdc,
	         const uint8_t *src_row,
	         TupleDataCollection *dst_tdc,
	         uint8_t *dst_row)
{
	for (uint16_t col_idx = 0; col_idx < layout->column_count; ++col_idx)
	{
		const TdcColumnDesc &col = layout->columns[col_idx];
		if (col.kind != TdcColumnKind::STRING_REF)
		{
			std::memcpy(dst_row + col.offset, src_row + col.offset, col.width);
			continue;
		}

		VecStringRef src_ref;
		std::memcpy(&src_ref, src_row + col.offset, sizeof(src_ref));
		const char *src_ptr = VecStringRefDataPtr(src_ref,
			src_tdc != nullptr ? reinterpret_cast<const char *>(TupleDataCollectionHeapConst(src_tdc)) : nullptr);
		VecStringRef dst_ref;
		if (!TupleDataCollectionStoreStringBytes(dst_tdc, src_ptr, src_ref.len, &dst_ref))
			elog(ERROR, "pg_volvec: hash join build-row copy ran out of heap");
		std::memcpy(dst_row + col.offset, &dst_ref, sizeof(dst_ref));
	}
}

static inline bool
TdcNeedsGrowForChunkRow(const TupleDataLayout *layout,
	                  const TupleDataCollection *tdc,
	                  const PipelineChunk &chunk,
	                  uint16_t row_idx)
{
	return !TupleDataCollectionHasSpaceForAppend(
		tdc,
		TupleDataCollectionRequiredHeapBytesForChunkRow(layout, chunk, row_idx));
}

static inline bool
TdcNeedsGrowForStoredRow(const TupleDataLayout *layout,
	                    const TupleDataCollection *tdc,
	                    const TupleDataCollection *src_tdc,
	                    const uint8_t *src_row)
{
	return !TupleDataCollectionHasSpaceForAppend(
		tdc,
		TupleDataCollectionRequiredHeapBytesForRow(layout, src_tdc, src_row));
}

static void
GrowJoinTdc(ExecCtx &ctx,
	       const TupleDataLayout *layout,
	       dsa_pointer layout_dp,
	       dsa_pointer *tdc_dp,
	       uint32_t required_heap_bytes)
{
	dsa_pointer old_tdc_dp = *tdc_dp;
	TupleDataCollection *old_tdc = ResolveTdc(ctx.dsa, *tdc_dp);
	if (old_tdc == nullptr)
		elog(ERROR, "pg_volvec: hash join TDC missing during grow");

	const uint32_t old_count = pg_atomic_read_u32(&old_tdc->row_count);
	const uint32_t new_capacity = std::max(old_tdc->row_capacity * 2u, old_count + 1u);
	const uint32_t heap_capacity = TupleDataCollectionGrowHeapCapacity(layout,
		old_tdc,
		new_capacity,
		required_heap_bytes);
	if (pg_volvec_trace_execution_path)
		ereport(LOG,
			(errmsg("pg_volvec hashjoin tdc grow old_count=%u old_capacity=%u new_capacity=%u row_width=%u heap_capacity=%u required_heap=%u",
				old_count,
				old_tdc->row_capacity,
				new_capacity,
				layout->row_width,
				heap_capacity,
				required_heap_bytes)));
	dsa_pointer new_tdc_dp = dsa_allocate0(ctx.dsa,
		TupleDataCollectionCheckedAllocSize(new_capacity, layout->row_width, heap_capacity));
	auto *new_tdc = ResolveTdc(ctx.dsa, new_tdc_dp);
	TupleDataCollectionInit(new_tdc,
		new_capacity,
		layout->row_width,
		layout_dp,
		heap_capacity);

	for (uint32_t row_idx = 0; row_idx < old_count; ++row_idx)
	{
		uint8_t *dst = nullptr;
		const uint32_t copied_idx = TupleDataCollectionAppendRow(new_tdc, &dst);
		if (copied_idx == TDC_INVALID_ROW_INDEX)
			elog(ERROR, "pg_volvec: hash join TDC grow overflow");
		const uint8_t *src = TupleDataCollectionGetRowConst(old_tdc, row_idx);
		CopyBuildRow(layout, old_tdc, src, new_tdc, dst);
	}

	*tdc_dp = new_tdc_dp;
	dsa_free(ctx.dsa, old_tdc_dp);
}

static void
UpdateHashJoinLocalBuildRegistry(ExecCtx &ctx,
	                            HashJoinSharedPayload *payload,
	                            HashJoinLocalSinkState &local)
{
	if (ctx.worker_index < 0)
		return;
	auto *registry = ResolveLocalBuildRegistry(ctx.dsa, payload);
	if (registry == nullptr || static_cast<uint32_t>(ctx.worker_index) >= payload->local_state_slot_count)
		elog(ERROR, "pg_volvec: hash join local build registry missing during grow");
	registry[ctx.worker_index].build_keys_dp = local.build_keys_dp;
	registry[ctx.worker_index].build_rows_dp = local.build_rows_dp;
}

static void
EnsureJoinLocalCapacity(ExecCtx &ctx,
	                  HashJoinSharedPayload *payload,
	                  dsa_pointer key_layout_dp,
	                  const TupleDataLayout *key_layout,
	                  dsa_pointer row_layout_dp,
	                  const TupleDataLayout *row_layout,
	                  HashJoinLocalSinkState &local,
	                  const PipelineChunk &chunk,
	                  uint16_t row_idx)
{
	while (TdcNeedsGrowForChunkRow(key_layout, local.build_keys, chunk, row_idx) ||
	       TdcNeedsGrowForChunkRow(row_layout, local.build_rows, chunk, row_idx))
	{
		if (TdcNeedsGrowForChunkRow(key_layout, local.build_keys, chunk, row_idx))
			GrowJoinTdc(ctx,
				key_layout,
				key_layout_dp,
				&local.build_keys_dp,
				TupleDataCollectionRequiredHeapBytesForChunkRow(key_layout, chunk, row_idx));
		if (TdcNeedsGrowForChunkRow(row_layout, local.build_rows, chunk, row_idx))
			GrowJoinTdc(ctx,
				row_layout,
				row_layout_dp,
				&local.build_rows_dp,
				TupleDataCollectionRequiredHeapBytesForChunkRow(row_layout, chunk, row_idx));
		local.build_keys = ResolveTdc(ctx.dsa, local.build_keys_dp);
		local.build_rows = ResolveTdc(ctx.dsa, local.build_rows_dp);
		if (local.build_keys == nullptr || local.build_rows == nullptr)
			elog(ERROR, "pg_volvec: hash join local build rows missing after grow");
		UpdateHashJoinLocalBuildRegistry(ctx, payload, local);
	}
}

static void
EnsureJoinGlobalCapacity(ExecCtx &ctx,
	                   HashJoinGlobalSinkState &global,
	                   const uint8_t *src_key_row,
	                   const TupleDataCollection *src_key_tdc,
	                   const uint8_t *src_row,
	                   const TupleDataCollection *src_row_tdc)
{
	global.build_keys = ResolveTdc(ctx.dsa, global.payload->build_keys_dp);
	global.build_rows = ResolveTdc(ctx.dsa, global.payload->build_rows_dp);
	if (global.build_keys == nullptr || global.build_rows == nullptr)
		elog(ERROR, "pg_volvec: hash join global build rows missing before grow");
	while (TdcNeedsGrowForStoredRow(global.build_key_layout,
			global.build_keys,
			src_key_tdc,
			src_key_row) ||
	       TdcNeedsGrowForStoredRow(global.build_layout,
			global.build_rows,
			src_row_tdc,
			src_row))
	{
		if (TdcNeedsGrowForStoredRow(global.build_key_layout,
				global.build_keys,
				src_key_tdc,
				src_key_row))
			GrowJoinTdc(ctx,
				global.build_key_layout,
				global.build_key_layout_dp,
				&global.payload->build_keys_dp,
				TupleDataCollectionRequiredHeapBytesForRow(global.build_key_layout,
					src_key_tdc,
					src_key_row));
		if (TdcNeedsGrowForStoredRow(global.build_layout,
				global.build_rows,
				src_row_tdc,
				src_row))
			GrowJoinTdc(ctx,
				global.build_layout,
				global.build_layout_dp,
				&global.payload->build_rows_dp,
				TupleDataCollectionRequiredHeapBytesForRow(global.build_layout,
					src_row_tdc,
					src_row));
		global.build_keys = ResolveTdc(ctx.dsa, global.payload->build_keys_dp);
		global.build_rows = ResolveTdc(ctx.dsa, global.payload->build_rows_dp);
		if (global.build_keys == nullptr || global.build_rows == nullptr)
			elog(ERROR, "pg_volvec: hash join global build rows missing after grow");
	}
}

static void
BuildRightColumnsBySlot(const TupleDataLayout *right_layout,
	                const TdcColumnDesc **right_columns_by_slot)
{
	std::fill_n(right_columns_by_slot, 16, nullptr);
	for (uint16_t col_idx = 0; col_idx < right_layout->column_count; ++col_idx)
	{
		const TdcColumnDesc &col = right_layout->columns[col_idx];
		if (col.src_col_idx >= 16)
			elog(ERROR, "pg_volvec: hash join right payload slot %u out of range",
			     static_cast<unsigned>(col.src_col_idx));
		right_columns_by_slot[col.src_col_idx] = &col;
	}
}

static void
CopyRowByMapping(const PipelineChunk &left_chunk,
	           uint16_t left_row_idx,
	           const TupleDataLayout *right_layout,
	           const TupleDataCollection *right_tdc,
	           const uint8_t *right_row,
	           const TdcColumnDesc *const *right_columns_by_slot,
	           const HashJoinOutputColumnDesc *output_columns,
	           uint16_t output_column_count,
	           PipelineChunk &out,
	           uint16_t out_row_idx)
{
	Assert(right_columns_by_slot != nullptr);

	for (uint16_t i = 0; i < output_column_count; ++i)
	{
		const HashJoinOutputColumnDesc &desc = output_columns[i];
		const TdcColumnDesc *right_col = desc.side == HashJoinOutputSide::RIGHT
			? right_columns_by_slot[desc.input_chunk_slot]
			: nullptr;
		if (desc.side == HashJoinOutputSide::RIGHT && right_col == nullptr)
			elog(ERROR, "pg_volvec: hash join output column mapping missing right payload column");
		switch (desc.decode_kind)
		{
			case ColumnDecodeKind::INT32_CHAR:
			case ColumnDecodeKind::INT32_DATE:
			case ColumnDecodeKind::INT32_INT4:
				if (desc.side == HashJoinOutputSide::LEFT)
					out.int32_columns[desc.output_chunk_slot][out_row_idx] =
						left_chunk.int32_columns[desc.input_chunk_slot][left_row_idx];
				else
				{
					int32_t value;
					std::memcpy(&value, right_row + right_col->offset, sizeof(value));
					out.int32_columns[desc.output_chunk_slot][out_row_idx] = value;
				}
				break;
			case ColumnDecodeKind::INT64_INT8:
			case ColumnDecodeKind::INT64_NUMERIC_SCALED:
				if (desc.side == HashJoinOutputSide::LEFT)
					out.int64_columns[desc.output_chunk_slot][out_row_idx] =
						left_chunk.int64_columns[desc.input_chunk_slot][left_row_idx];
				else
				{
					int64_t value;
					std::memcpy(&value, right_row + right_col->offset, sizeof(value));
					out.int64_columns[desc.output_chunk_slot][out_row_idx] = value;
				}
				break;
			case ColumnDecodeKind::DOUBLE_FLOAT8:
				if (desc.side == HashJoinOutputSide::LEFT)
					out.double_columns[desc.output_chunk_slot][out_row_idx] =
						left_chunk.double_columns[desc.input_chunk_slot][left_row_idx];
				else
				{
					double value;
					std::memcpy(&value, right_row + right_col->offset, sizeof(value));
					out.double_columns[desc.output_chunk_slot][out_row_idx] = value;
				}
				break;
			case ColumnDecodeKind::STRING_REF:
			{
				VecStringRef ref;
				const char *ptr = nullptr;
				if (desc.side == HashJoinOutputSide::LEFT)
				{
					const VecStringRef &src = left_chunk.string_columns[desc.input_chunk_slot][left_row_idx];
					ptr = left_chunk.get_string_ptr(src);
					ref = src;
				}
				else
				{
					std::memcpy(&ref, right_row + right_col->offset, sizeof(ref));
					ptr = VecStringRefDataPtr(ref,
						right_tdc != nullptr ? reinterpret_cast<const char *>(TupleDataCollectionHeapConst(right_tdc)) : nullptr);
				}
				if (ptr == nullptr && ref.len != 0)
					elog(ERROR, "pg_volvec: hash join output string missing backing storage");
				out.string_columns[desc.output_chunk_slot][out_row_idx] =
					out.store_string_bytes(ptr, ref.len);
				break;
			}
			case ColumnDecodeKind::NONE:
				elog(ERROR, "pg_volvec: hash join output decode_kind NONE is invalid");
		}
	}
}

static void
BuildFallbackOutputColumns(const SchemaDescriptor *left_schema,
	                      const SchemaDescriptor *right_schema,
	                      const SchemaDescriptor *output_schema,
	                      PgVector<HashJoinOutputColumnDesc> &out)
{
	if (left_schema == nullptr || right_schema == nullptr || output_schema == nullptr)
		elog(ERROR, "pg_volvec: hash join fallback output mapping missing schema");
	if (output_schema->n_columns != left_schema->n_columns + right_schema->n_columns)
		elog(ERROR, "pg_volvec: hash join fallback output mapping requires concat output schema");

	for (uint16_t i = 0; i < left_schema->n_columns; ++i)
	{
		HashJoinOutputColumnDesc desc{};
		desc.side = HashJoinOutputSide::LEFT;
		desc.input_chunk_slot = left_schema->columns[i].chunk_slot;
		desc.decode_kind = left_schema->columns[i].decode_kind;
		desc.output_chunk_slot = output_schema->columns[i].chunk_slot;
		out.push_back(desc);
	}
	for (uint16_t i = 0; i < right_schema->n_columns; ++i)
	{
		HashJoinOutputColumnDesc desc{};
		desc.side = HashJoinOutputSide::RIGHT;
		desc.input_chunk_slot = right_schema->columns[i].chunk_slot;
		desc.decode_kind = right_schema->columns[i].decode_kind;
		desc.output_chunk_slot = output_schema->columns[left_schema->n_columns + i].chunk_slot;
		out.push_back(desc);
	}
}

class HashJoinOperatorState final : public OperatorState {
public:
	bool initialized = false;
	bool active_probe = false;
	bool current_input_drained = false;
	uint16_t probe_row_idx = 0;
	uint32_t build_row_idx = HASH_JOIN_INVALID_ROW;
	bool have_build_cursor = false;
};

} // namespace

int
PhysicalHashJoin::MaxThreads(ExecCtx &ctx) const
{
	(void) ctx;
	return std::max(1, pg_volvec_parallel_max_workers);
}

void
PhysicalHashJoin::BuildPipelines(Pipeline &current, MetaPipeline &meta)
{
	Assert(children().size() == 2);

	/*
	 * HashJoin is the first binary operator in this runtime. We keep the probe
	 * side streaming in the current pipeline and create a child producer
	 * pipeline for the build side whose sink is this same operator instance.
	 * Current convention is left=probe, right=build.
	 */
	Pipeline &build_pipeline = meta.CreateChildPipeline(current, *this);
	meta.SetSink(build_pipeline, *this);
	build_pipeline.source = nullptr;
	children()[1]->BuildPipelines(build_pipeline, meta);
	children()[0]->BuildPipelines(current, meta);
	meta.AddOperator(current, *this);
}

std::unique_ptr<GlobalSinkState>
PhysicalHashJoin::GetGlobalSinkState(ExecCtx &ctx)
{
	auto state = std::make_unique<HashJoinGlobalSinkState>();
	state->dsa = ctx.dsa;
	state->desc = desc_;
	state->probe_key_layout_dp = left_key_layout_dp_;
	state->build_key_layout_dp = right_key_layout_dp_;
	state->output_columns_dp = output_columns_dp_;
	state->build_layout_dp = right_payload_layout_dp_;
	state->shared_payload_dp = DsaPointerIsValid(shared_payload_dp_)
		? shared_payload_dp_
		: LoadSharedPayloadFromDescriptor(this);
	if (!DsaPointerIsValid(state->probe_key_layout_dp) && desc_ != nullptr)
		state->probe_key_layout_dp = desc_->body.hash_join.left_key_layout;
	if (!DsaPointerIsValid(state->build_key_layout_dp) && desc_ != nullptr)
		state->build_key_layout_dp = desc_->body.hash_join.right_key_layout;
	if (!DsaPointerIsValid(state->build_layout_dp) && desc_ != nullptr)
		state->build_layout_dp = desc_->body.hash_join.right_payload_layout;
	if (!DsaPointerIsValid(state->output_columns_dp) && desc_ != nullptr)
		state->output_columns_dp = desc_->body.hash_join.output_columns;
	state->probe_key_layout = ResolveLayout(ctx.dsa, state->probe_key_layout_dp);
	state->build_key_layout = ResolveLayout(ctx.dsa, state->build_key_layout_dp);
	state->build_layout = ResolveLayout(ctx.dsa, state->build_layout_dp);
	state->output_column_count = output_column_count_ > 0 ? output_column_count_ :
		(desc_ != nullptr ? desc_->body.hash_join.output_column_count : 0);
	if (DsaPointerIsValid(state->output_columns_dp) && state->output_column_count > 0)
		state->output_columns = static_cast<const HashJoinOutputColumnDesc *>(dsa_get_address(ctx.dsa, state->output_columns_dp));
	if (state->probe_key_layout == nullptr)
		elog(ERROR, "pg_volvec: hash join missing probe-side key layout");
	if (state->build_key_layout == nullptr)
		elog(ERROR, "pg_volvec: hash join missing build-side key layout");
	if (state->build_layout == nullptr)
		elog(ERROR, "pg_volvec: hash join missing build-side payload layout");

	if (ctx.worker_index == LEADER_WORKER_INDEX && !DsaPointerIsValid(state->shared_payload_dp))
	{
		state->shared_payload_dp = dsa_allocate0(ctx.dsa, sizeof(HashJoinSharedPayload));
		state->payload = ResolvePayload(ctx.dsa, state->shared_payload_dp);
		state->payload->local_state_slot_count = static_cast<uint32_t>(std::max(1, pg_volvec_parallel_max_workers));
		state->payload->build_partition_count = 1;
		state->payload->hash_table_capacity = 0;
		state->payload->radix_bits = 0;
		state->payload->combined = false;
		state->payload->finalized = false;
		pg_atomic_init_u32(&state->payload->release_state, 0);
		SpinLockInit(&state->payload->mutex);
		state->payload->local_build_registry_dp = dsa_allocate0(ctx.dsa,
			static_cast<size_t>(state->payload->local_state_slot_count) * sizeof(HashJoinLocalBuildRegistryEntry));
		const uint32_t initial_rows = HashJoinInitialRows(max_rows_);
		const uint64_t per_worker_rows =
			(initial_rows + state->payload->local_state_slot_count - 1) / state->payload->local_state_slot_count;
		const uint64_t initial_global_rows = per_worker_rows * state->payload->local_state_slot_count;
		const uint32_t global_row_capacity = static_cast<uint32_t>(std::max<uint64_t>(1024u, initial_global_rows));
		const uint32_t key_heap_capacity = TupleDataCollectionDefaultHeapCapacity(state->build_key_layout,
			global_row_capacity);
		const uint32_t heap_capacity = TupleDataCollectionDefaultHeapCapacity(state->build_layout,
			global_row_capacity);
	state->payload->build_keys_dp = dsa_allocate0(ctx.dsa,
		TupleDataCollectionCheckedAllocSize(global_row_capacity, state->build_key_layout->row_width, key_heap_capacity));
	state->payload->build_rows_dp = dsa_allocate0(ctx.dsa,
		TupleDataCollectionCheckedAllocSize(global_row_capacity, state->build_layout->row_width, heap_capacity));
		state->build_keys = ResolveTdc(ctx.dsa, state->payload->build_keys_dp);
		state->build_rows = ResolveTdc(ctx.dsa, state->payload->build_rows_dp);
		TupleDataCollectionInit(state->build_keys,
			global_row_capacity,
			state->build_key_layout->row_width,
			state->build_key_layout_dp,
			key_heap_capacity);
		TupleDataCollectionInit(state->build_rows,
			global_row_capacity,
			state->build_layout->row_width,
			state->build_layout_dp,
			heap_capacity);
		StoreSharedPayloadOnDescriptor(this, state->shared_payload_dp);
	}
	else
	{
		if (!DsaPointerIsValid(state->shared_payload_dp))
			state->shared_payload_dp = LoadSharedPayloadFromDescriptor(this);
		state->payload = ResolvePayload(ctx.dsa, state->shared_payload_dp);
	}

	if (state->payload == nullptr)
		elog(ERROR, "pg_volvec: hash join shared payload not initialized");

	state->build_keys = ResolveTdc(ctx.dsa, state->payload->build_keys_dp);
	state->build_rows = ResolveTdc(ctx.dsa, state->payload->build_rows_dp);
	if (state->build_keys == nullptr)
		elog(ERROR, "pg_volvec: hash join global build keys not initialized");
	if (state->build_rows == nullptr)
		elog(ERROR, "pg_volvec: hash join global build rows not initialized");

	state->local_state_slot_count = state->payload->local_state_slot_count;
	state->finalized = state->payload->finalized;
	return state;
}

std::unique_ptr<LocalSinkState>
PhysicalHashJoin::GetLocalSinkState(ExecCtx &ctx, GlobalSinkState &gstate)
{
	auto &global = static_cast<HashJoinGlobalSinkState &>(gstate);
	auto state = std::make_unique<HashJoinLocalSinkState>();
	state->build_key_layout = global.build_key_layout;
	state->build_layout = global.build_layout;
	const uint32_t initial_rows = HashJoinInitialRows(max_rows_);
	uint32_t local_capacity = initial_rows;
	if (global.local_state_slot_count > 0)
		local_capacity = (initial_rows + global.local_state_slot_count - 1) / global.local_state_slot_count;
	local_capacity = std::max<uint32_t>(1024u, local_capacity);
	const uint32_t key_heap_capacity = TupleDataCollectionDefaultHeapCapacity(state->build_key_layout,
		local_capacity);
	const uint32_t heap_capacity = TupleDataCollectionDefaultHeapCapacity(state->build_layout,
		local_capacity);
	state->build_keys_dp = dsa_allocate0(ctx.dsa,
		TupleDataCollectionCheckedAllocSize(local_capacity, state->build_key_layout->row_width, key_heap_capacity));
	state->build_rows_dp = dsa_allocate0(ctx.dsa,
		TupleDataCollectionCheckedAllocSize(local_capacity, state->build_layout->row_width, heap_capacity));
	state->build_keys = ResolveTdc(ctx.dsa, state->build_keys_dp);
	state->build_rows = ResolveTdc(ctx.dsa, state->build_rows_dp);
	TupleDataCollectionInit(state->build_keys,
		local_capacity,
		state->build_key_layout->row_width,
		global.build_key_layout_dp,
		key_heap_capacity);
	TupleDataCollectionInit(state->build_rows,
		local_capacity,
		state->build_layout->row_width,
		global.build_layout_dp,
		heap_capacity);

	if (ctx.worker_index >= 0)
	{
		auto *registry = ResolveLocalBuildRegistry(ctx.dsa, global.payload);
		if (registry == nullptr || static_cast<uint32_t>(ctx.worker_index) >= global.payload->local_state_slot_count)
			elog(ERROR, "pg_volvec: hash join local build registry missing");
		registry[ctx.worker_index].build_keys_dp = state->build_keys_dp;
		registry[ctx.worker_index].build_rows_dp = state->build_rows_dp;
		if (pg_volvec_trace_execution_path)
			ereport(LOG,
				(errmsg("pg_volvec hashjoin registry slot=%d keys=" UINT64_FORMAT " rows=" UINT64_FORMAT,
					ctx.worker_index,
					(uint64) state->build_keys_dp,
					(uint64) state->build_rows_dp)));
	}

	return state;
}

SinkResultType
PhysicalHashJoin::SinkChunk(ExecCtx &ctx, PipelineChunk &in, OperatorSinkInput &input)
{
	auto &global = static_cast<HashJoinGlobalSinkState &>(input.global_state);
	auto &local = static_cast<HashJoinLocalSinkState &>(input.local_state);
	if (local.build_keys == nullptr || local.build_rows == nullptr ||
		local.build_key_layout == nullptr || local.build_layout == nullptr)
		elog(ERROR, "pg_volvec: hash join local build rows not initialized");

	for (uint16_t row_idx = 0; row_idx < in.count; ++row_idx)
	{
		if (PipelineCancelRequestedEvery(ctx, row_idx))
			return SinkResultType::FINISHED;
		EnsureJoinLocalCapacity(ctx,
			global.payload,
			global.build_key_layout_dp,
			local.build_key_layout,
			global.build_layout_dp,
			local.build_layout,
			local,
			in,
			row_idx);
		uint8_t *key_row_ptr = nullptr;
		uint8_t *row_ptr = nullptr;
		const uint32_t key_appended = TupleDataCollectionAppendRow(local.build_keys, &key_row_ptr);
		const uint32_t appended = TupleDataCollectionAppendRow(local.build_rows, &row_ptr);
		if (key_appended == TDC_INVALID_ROW_INDEX)
			elog(ERROR, "pg_volvec: hash join local build key row capacity exceeded");
		if (appended == TDC_INVALID_ROW_INDEX)
			elog(ERROR, "pg_volvec: hash join local build row capacity exceeded");
		ScatterGroupOnly(local.build_key_layout, local.build_keys, key_row_ptr, in, row_idx);
		ScatterGroupOnly(local.build_layout, local.build_rows, row_ptr, in, row_idx);
	}

	(void) ctx;
	local.build_input_rows += in.count;
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType
PhysicalHashJoin::Combine(ExecCtx &ctx, OperatorSinkCombineInput &input)
{
	auto &global = static_cast<HashJoinGlobalSinkState &>(input.global_state);
	if (input.local_state == nullptr)
		elog(ERROR, "pg_volvec: hash join combine local state missing");
	auto &local = static_cast<HashJoinLocalSinkState &>(*input.local_state);
	if (local.build_keys == nullptr || local.build_rows == nullptr)
		elog(ERROR, "pg_volvec: hash join combine local build rows missing");

	const uint32_t local_row_count = pg_atomic_read_u32(&local.build_keys->row_count);
	if (local_row_count != pg_atomic_read_u32(&local.build_rows->row_count))
		elog(ERROR, "pg_volvec: hash join local build key/payload row counts diverged");
	uint32_t row_idx = 0;
	while (row_idx < local_row_count)
	{
		if (PipelineCancelRequested(ctx))
			return SinkCombineResultType::FINISHED;
		const uint32_t batch_end = std::min(row_idx + 64u, local_row_count);
		SpinLockAcquire(&global.payload->mutex);
		for (; row_idx < batch_end; ++row_idx)
		{
			const uint8_t *src_key_row = TupleDataCollectionGetRowConst(local.build_keys, row_idx);
			const uint8_t *src_row = TupleDataCollectionGetRowConst(local.build_rows, row_idx);
			EnsureJoinGlobalCapacity(ctx,
				global,
				src_key_row,
				local.build_keys,
				src_row,
				local.build_rows);
			uint8_t *dst_key_row = nullptr;
			uint8_t *dst_row = nullptr;
			const uint32_t key_appended = TupleDataCollectionAppendRow(global.build_keys, &dst_key_row);
			const uint32_t appended = TupleDataCollectionAppendRow(global.build_rows, &dst_row);
			if (key_appended == TDC_INVALID_ROW_INDEX)
				elog(ERROR, "pg_volvec: hash join global build key row capacity exceeded during combine");
			if (appended == TDC_INVALID_ROW_INDEX)
				elog(ERROR, "pg_volvec: hash join global build row capacity exceeded during combine");
			CopyBuildRow(global.build_key_layout, local.build_keys, src_key_row, global.build_keys, dst_key_row);
			CopyBuildRow(global.build_layout, local.build_rows, src_row, global.build_rows, dst_row);
		}
		SpinLockRelease(&global.payload->mutex);
	}

	global.payload->combined = true;
	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType
PhysicalHashJoin::Finalize(ExecCtx &ctx, GlobalSinkState &gstate)
{
	auto &global = static_cast<HashJoinGlobalSinkState &>(gstate);
	if (global.payload != nullptr)
	{
		global.build_keys = ResolveTdc(ctx.dsa, global.payload->build_keys_dp);
		global.build_rows = ResolveTdc(ctx.dsa, global.payload->build_rows_dp);
	}
	if (global.build_keys == nullptr || global.build_rows == nullptr)
		elog(ERROR, "pg_volvec: hash join finalize missing global build rows");
	const uint32_t row_count = pg_atomic_read_u32(&global.build_keys->row_count);
	if (row_count != pg_atomic_read_u32(&global.build_rows->row_count))
		elog(ERROR, "pg_volvec: hash join finalize key/payload row counts diverged");
	if (pg_volvec_trace_execution_path)
		ereport(LOG,
			(errmsg("pg_volvec hashjoin finalize build_rows=%u key_width=%u payload_width=%u hash_capacity_pre=%u",
				row_count,
				global.build_key_layout != nullptr ? global.build_key_layout->row_width : 0,
				global.build_layout != nullptr ? global.build_layout->row_width : 0,
				row_count > 0 ? 1u : 0u)));
	if (row_count > 0)
	{
		uint32_t hash_capacity = 1;
		while (hash_capacity < row_count * 2u)
		{
			if (PipelineCancelRequested(ctx))
				return SinkFinalizeType::READY;
			hash_capacity <<= 1;
		}
		global.payload->hash_table_capacity = hash_capacity;
		global.payload->hash_table_dp = dsa_allocate0(ctx.dsa,
			static_cast<size_t>(hash_capacity) * sizeof(uint32_t));
		global.payload->hash_links_dp = dsa_allocate0(ctx.dsa,
			static_cast<size_t>(row_count) * sizeof(uint32_t));

		auto *bucket_heads = static_cast<uint32_t *>(dsa_get_address(ctx.dsa, global.payload->hash_table_dp));
		auto *links = static_cast<uint32_t *>(dsa_get_address(ctx.dsa, global.payload->hash_links_dp));
		for (uint32_t i = 0; i < hash_capacity; ++i)
		{
			if (PipelineCancelRequestedEvery(ctx, i, 1023u))
				return SinkFinalizeType::READY;
			bucket_heads[i] = HASH_JOIN_INVALID_ROW;
		}
		for (uint32_t row_idx = 0; row_idx < row_count; ++row_idx)
		{
			if (PipelineCancelRequestedEvery(ctx, row_idx, 1023u))
				return SinkFinalizeType::READY;
			links[row_idx] = HASH_JOIN_INVALID_ROW;
		}

		for (uint32_t row_idx = 0; row_idx < row_count; ++row_idx)
		{
			if (PipelineCancelRequestedEvery(ctx, row_idx, 1023u))
				return SinkFinalizeType::READY;
			const uint8_t *row_ptr = TupleDataCollectionGetRowConst(global.build_keys, row_idx);
			const uint64_t hash = HashGroupRow(global.build_key_layout, global.build_keys, row_ptr);
			const uint32_t bucket = static_cast<uint32_t>(hash) & (hash_capacity - 1u);
			links[row_idx] = bucket_heads[bucket];
			bucket_heads[bucket] = row_idx;
		}
	}
	else
	{
		global.payload->hash_table_capacity = 0;
		global.payload->hash_table_dp = InvalidDsaPointer;
		global.payload->hash_links_dp = InvalidDsaPointer;
	}
	TupleDataCollectionResetScan(global.build_keys);
	TupleDataCollectionResetScan(global.build_rows);
	global.build_keys->finalized = true;
	global.build_rows->finalized = true;
	global.payload->finalized = true;
	global.finalized = true;
	return SinkFinalizeType::READY;
}

std::unique_ptr<OperatorState>
PhysicalHashJoin::GetOperatorState(ExecCtx &ctx)
{
	(void) ctx;
	return std::make_unique<HashJoinOperatorState>();
}

void
PhysicalHashJoin::ReleaseBuildPayloadAfterConsumerRun(ExecCtx &ctx)
{
	dsa_pointer payload_dp = DsaPointerIsValid(shared_payload_dp_)
		? shared_payload_dp_
		: LoadSharedPayloadFromDescriptor(this);
	HashJoinSharedPayload *payload = ResolvePayload(ctx.dsa, payload_dp);
	if (payload == nullptr)
		return;

	uint32 expected = 0;
	if (!pg_atomic_compare_exchange_u32(&payload->release_state, &expected, 1))
		return;

	const uint64_t global_keys_bytes = EstimateTdcAllocBytes(ctx.dsa, payload->build_keys_dp);
	const uint64_t global_rows_bytes = EstimateTdcAllocBytes(ctx.dsa, payload->build_rows_dp);
	uint64_t local_keys_bytes = 0;
	uint64_t local_rows_bytes = 0;
	auto *registry = ResolveLocalBuildRegistry(ctx.dsa, payload);
	if (registry != nullptr)
	{
		for (uint32_t i = 0; i < payload->local_state_slot_count; ++i)
		{
			local_keys_bytes += EstimateTdcAllocBytes(ctx.dsa, registry[i].build_keys_dp);
			local_rows_bytes += EstimateTdcAllocBytes(ctx.dsa, registry[i].build_rows_dp);
		}
	}
	const uint64_t hash_bytes = DsaPointerIsValid(payload->hash_table_dp)
		? static_cast<uint64_t>(payload->hash_table_capacity) * sizeof(uint32_t)
		: 0;
	TupleDataCollection *build_rows = ResolveTdc(ctx.dsa, payload->build_rows_dp);
	const uint64_t link_bytes = DsaPointerIsValid(payload->hash_links_dp) && build_rows != nullptr
		? static_cast<uint64_t>(pg_atomic_read_u32(&build_rows->row_count)) * sizeof(uint32_t)
		: 0;

	FreeDsaPointerIfValid(ctx.dsa, &payload->hash_table_dp);
	FreeDsaPointerIfValid(ctx.dsa, &payload->hash_links_dp);
	FreeDsaPointerIfValid(ctx.dsa, &payload->build_keys_dp);
	FreeDsaPointerIfValid(ctx.dsa, &payload->build_rows_dp);
	if (registry != nullptr)
	{
		for (uint32_t i = 0; i < payload->local_state_slot_count; ++i)
		{
			FreeDsaPointerIfValid(ctx.dsa, &registry[i].build_keys_dp);
			FreeDsaPointerIfValid(ctx.dsa, &registry[i].build_rows_dp);
		}
	}
	FreeDsaPointerIfValid(ctx.dsa, &payload->local_build_registry_dp);

	if (pg_volvec_trace_execution_path)
		ereport(LOG,
			(errmsg("pg_volvec hashjoin release op=%p global_keys=" UINT64_FORMAT " global_rows=" UINT64_FORMAT " local_keys=" UINT64_FORMAT " local_rows=" UINT64_FORMAT " hash=" UINT64_FORMAT " links=" UINT64_FORMAT,
				static_cast<void *>(this),
				(uint64) global_keys_bytes,
				(uint64) global_rows_bytes,
				(uint64) local_keys_bytes,
				(uint64) local_rows_bytes,
				(uint64) hash_bytes,
				(uint64) link_bytes)));

	ClearSharedPayloadOnDescriptor(this);
	pg_atomic_write_u32(&payload->release_state, 2);
	dsa_free(ctx.dsa, payload_dp);
}

OperatorResultType
PhysicalHashJoin::Execute(ExecCtx &ctx, PipelineChunk &in, PipelineChunk &out, OperatorState &state)
{
	auto &op_state = static_cast<HashJoinOperatorState &>(state);
	if (op_state.current_input_drained)
	{
		op_state.current_input_drained = false;
		out.reset();
		return OperatorResultType::NEED_MORE_INPUT;
	}
	dsa_pointer payload_dp = DsaPointerIsValid(shared_payload_dp_)
		? shared_payload_dp_
		: LoadSharedPayloadFromDescriptor(this);
	HashJoinSharedPayload *payload = ResolvePayload(ctx.dsa, payload_dp);
	if (payload == nullptr || !payload->finalized)
		elog(ERROR, "pg_volvec: hash join probe ran before build/finalize completed");
	if (pg_atomic_read_u32(&payload->release_state) != 0)
		elog(ERROR, "pg_volvec: hash join payload used after release");
	const SchemaDescriptor *left_schema = static_cast<const SchemaDescriptor *>(dsa_get_address(ctx.dsa,
		DsaPointerIsValid(left_input_schema_dp_) ? left_input_schema_dp_ :
		(desc_ != nullptr ? desc_->body.hash_join.left_input_schema : InvalidDsaPointer)));
	const SchemaDescriptor *right_schema = static_cast<const SchemaDescriptor *>(dsa_get_address(ctx.dsa,
		DsaPointerIsValid(right_input_schema_dp_) ? right_input_schema_dp_ :
		(desc_ != nullptr ? desc_->body.hash_join.right_input_schema : InvalidDsaPointer)));
	const SchemaDescriptor *output_schema = static_cast<const SchemaDescriptor *>(dsa_get_address(ctx.dsa,
		DsaPointerIsValid(output_schema_dp_) ? output_schema_dp_ :
		(desc_ != nullptr ? desc_->body.hash_join.output_schema : InvalidDsaPointer)));
	TupleDataCollection *build_keys = ResolveTdc(ctx.dsa, payload->build_keys_dp);
	TupleDataCollection *build_rows = ResolveTdc(ctx.dsa, payload->build_rows_dp);
	if (build_keys == nullptr || build_rows == nullptr)
		elog(ERROR, "pg_volvec: hash join probe missing finalized build-side rows");
	const TupleDataLayout *probe_layout = ResolveLayout(ctx.dsa,
		DsaPointerIsValid(left_key_layout_dp_) ? left_key_layout_dp_ :
		(desc_ != nullptr ? desc_->body.hash_join.left_key_layout : InvalidDsaPointer));
	const TupleDataLayout *build_key_layout = ResolveLayout(ctx.dsa,
		DsaPointerIsValid(right_key_layout_dp_) ? right_key_layout_dp_ :
		(desc_ != nullptr ? desc_->body.hash_join.right_key_layout : InvalidDsaPointer));
	const TupleDataLayout *build_row_layout = ResolveLayout(ctx.dsa,
		DsaPointerIsValid(right_payload_layout_dp_) ? right_payload_layout_dp_ :
		(desc_ != nullptr ? desc_->body.hash_join.right_payload_layout : InvalidDsaPointer));
	if (probe_layout == nullptr || build_key_layout == nullptr || build_row_layout == nullptr)
		elog(ERROR, "pg_volvec: hash join probe missing payload layouts");
	const auto *output_columns = DsaPointerIsValid(output_columns_dp_)
		? static_cast<const HashJoinOutputColumnDesc *>(dsa_get_address(ctx.dsa, output_columns_dp_))
		: (desc_ != nullptr && DsaPointerIsValid(desc_->body.hash_join.output_columns)
			? static_cast<const HashJoinOutputColumnDesc *>(dsa_get_address(ctx.dsa, desc_->body.hash_join.output_columns))
			: nullptr);
	const TdcColumnDesc *right_columns_by_slot[16];
	uint16_t output_column_count = output_column_count_ > 0 ? output_column_count_ :
		(desc_ != nullptr ? desc_->body.hash_join.output_column_count : 0);
	PgVector<HashJoinOutputColumnDesc> fallback_output_columns;
	if ((output_columns == nullptr || output_column_count == 0) &&
		left_schema != nullptr && right_schema != nullptr && output_schema != nullptr)
	{
		BuildFallbackOutputColumns(left_schema, right_schema, output_schema, fallback_output_columns);
		output_columns = fallback_output_columns.data();
		output_column_count = static_cast<uint16_t>(fallback_output_columns.size());
	}
	if (output_columns == nullptr || output_column_count == 0)
		elog(ERROR, "pg_volvec: hash join probe missing output column mapping");
	BuildRightColumnsBySlot(build_row_layout, right_columns_by_slot);
	if (!DsaPointerIsValid(payload->hash_table_dp) || !DsaPointerIsValid(payload->hash_links_dp))
		return OperatorResultType::NEED_MORE_INPUT;

	auto *bucket_heads = static_cast<const uint32_t *>(dsa_get_address(ctx.dsa, payload->hash_table_dp));
	auto *links = static_cast<const uint32_t *>(dsa_get_address(ctx.dsa, payload->hash_links_dp));
	out.reset();
	uint32_t matched_rows = 0;

	if (!op_state.active_probe)
	{
		op_state.active_probe = true;
		op_state.probe_row_idx = 0;
		op_state.build_row_idx = HASH_JOIN_INVALID_ROW;
		op_state.have_build_cursor = false;
	}

	while (op_state.probe_row_idx < in.count)
	{
		if (PipelineCancelRequestedEvery(ctx, op_state.probe_row_idx))
			break;
		if (out.count >= PIPELINE_DEFAULT_CHUNK_SIZE)
			break;
		if (!op_state.have_build_cursor)
		{
			const uint64_t hash = HashGroup(probe_layout, in, op_state.probe_row_idx);
			const uint32_t bucket = static_cast<uint32_t>(hash) & (payload->hash_table_capacity - 1u);
			op_state.build_row_idx = bucket_heads[bucket];
			op_state.have_build_cursor = true;
		}
		while (op_state.build_row_idx != HASH_JOIN_INVALID_ROW)
		{
			const uint32_t build_row_idx = op_state.build_row_idx;
			op_state.build_row_idx = links[build_row_idx];
			const uint8_t *build_key_row = TupleDataCollectionGetRowConst(build_keys, build_row_idx);
			if (MatchGroupLayouts(build_key_layout, build_keys, build_key_row, probe_layout, in, op_state.probe_row_idx))
			{
				++matched_rows;
				const uint8_t *build_payload_row = TupleDataCollectionGetRowConst(build_rows, build_row_idx);
				CopyRowByMapping(in,
					op_state.probe_row_idx,
					build_row_layout,
					build_rows,
					build_payload_row,
					right_columns_by_slot,
					output_columns,
					output_column_count,
					out,
					out.count);
				++out.count;
				if (out.count >= PIPELINE_DEFAULT_CHUNK_SIZE)
					break;
			}
		}
		if (out.count >= PIPELINE_DEFAULT_CHUNK_SIZE)
			break;
		op_state.have_build_cursor = false;
		++op_state.probe_row_idx;
	}
	if (op_state.probe_row_idx >= in.count)
	{
		op_state.active_probe = false;
		op_state.current_input_drained = out.count > 0;
		op_state.build_row_idx = HASH_JOIN_INVALID_ROW;
		op_state.have_build_cursor = false;
	}
	if (out.count > 0 && pg_volvec_trace_execution_path)
		ereport(LOG,
			(errmsg("pg_volvec hashjoin execute worker=%d probe_rows=%u out_rows=%u matched_rows=%u build_rows=%u key_width=%u payload_width=%u",
				ctx.worker_index,
				in.count,
				out.count,
				matched_rows,
				pg_atomic_read_u32(&build_rows->row_count),
				build_key_layout->row_width,
				build_row_layout->row_width)));
	op_state.initialized = true;

	return out.count > 0 ? OperatorResultType::HAVE_MORE_OUTPUT : OperatorResultType::NEED_MORE_INPUT;
}

}  /* namespace pipeline */
}  /* namespace pg_volvec */

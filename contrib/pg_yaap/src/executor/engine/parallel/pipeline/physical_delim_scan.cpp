#include "parallel/pipeline/physical_delim_scan.hpp"

#include "parallel/pipeline/meta_pipeline.hpp"
#include "parallel/pipeline/tuple_data_collection.hpp"
#include "parallel/pipeline/tuple_data_ops.hpp"

namespace pg_yaap {
namespace pipeline {

namespace {

static HashAggSharedPayload *
ResolvePayload(dsa_area *dsa, dsa_pointer payload_dp)
{
	return DsaPointerIsValid(payload_dp)
		? static_cast<HashAggSharedPayload *>(dsa_get_address(dsa, payload_dp))
		: nullptr;
}

static HashAggPartition *
ResolvePartitions(dsa_area *dsa, HashAggSharedPayload *payload)
{
	if (payload == nullptr || !DsaPointerIsValid(payload->partitions_dp))
		return nullptr;
	return static_cast<HashAggPartition *>(dsa_get_address(dsa, payload->partitions_dp));
}

static TupleDataCollection *
ResolveTdc(dsa_area *dsa, dsa_pointer tdc_dp)
{
	return DsaPointerIsValid(tdc_dp)
		? static_cast<TupleDataCollection *>(dsa_get_address(dsa, tdc_dp))
		: nullptr;
}

static const TupleDataLayout *
ResolveLayout(dsa_area *dsa, dsa_pointer layout_dp)
{
	return DsaPointerIsValid(layout_dp)
		? static_cast<const TupleDataLayout *>(dsa_get_address(dsa, layout_dp))
		: nullptr;
}

}  // namespace

int
PhysicalDelimScan::MaxThreads(ExecCtx &ctx) const
{
	(void) ctx;
	return 1;
}

void
PhysicalDelimScan::BuildPipelines(Pipeline &current, MetaPipeline &meta)
{
	meta.SetSource(current, *this);
	if (!producer_sink_)
		return;

	Pipeline &producer = meta.CreateChildPipeline(current, *producer_sink_);
	meta.SetSink(producer, *producer_sink_);
	producer.source = nullptr;
	Assert(producer_sink_->children().size() == 1);
	producer_sink_->children()[0]->BuildPipelines(producer, meta);
}

std::unique_ptr<GlobalSourceState>
PhysicalDelimScan::GetGlobalSourceState(ExecCtx &ctx)
{
	auto state = std::make_unique<DelimScanGlobalSourceState>();
	const dsa_pointer payload_dp = DsaPointerIsValid(shared_payload_dp_)
		? shared_payload_dp_
		: LoadSharedPayloadFromDescriptor(this);
	state->payload = ResolvePayload(ctx.dsa, payload_dp);
	state->partitions = ResolvePartitions(ctx.dsa, state->payload);
	state->partition_count = state->payload != nullptr ? state->payload->partition_count : 0;
	if (state->payload == nullptr || state->partitions == nullptr || state->partition_count == 0)
		elog(ERROR, "pg_yaap: delim scan source payload not initialized");

	TupleDataCollection *first_tdc = ResolveTdc(ctx.dsa, state->partitions[0].tdc_dp);
	state->layout = first_tdc != nullptr ? ResolveLayout(ctx.dsa, first_tdc->layout_dp) : nullptr;
	if (state->layout == nullptr)
		elog(ERROR, "pg_yaap: delim scan source layout missing");
	return state;
}

std::unique_ptr<LocalSourceState>
PhysicalDelimScan::GetLocalSourceState(ExecCtx &ctx, GlobalSourceState &gstate)
{
	(void) ctx;
	(void) gstate;
	return std::make_unique<DelimScanLocalSourceState>();
}

SourceResultType
PhysicalDelimScan::GetData(ExecCtx &ctx, PipelineChunk &out, OperatorSourceInput &input)
{
	auto &global = static_cast<DelimScanGlobalSourceState &>(input.global_state);
	(void) input.local_state;
	out.reset();

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
		Gather(global.layout, tdc, row, out, out.count);
		++out.count;
	}

	return out.count > 0 ? SourceResultType::HAVE_MORE_OUTPUT : SourceResultType::FINISHED;
}

}  /* namespace pipeline */
}  /* namespace pg_yaap */

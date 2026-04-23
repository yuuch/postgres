#include "parallel/pipeline/pipeline_lowering.hpp"

#include "parallel/pipeline/agg_sink.hpp"

namespace pg_volvec {
#include "parallel/pipeline/partial_agg_op.hpp"

namespace pg_volvec {
#include "parallel/pipeline/seq_scan_source.hpp"

namespace pg_volvec {
namespace pipeline {

LoweredPipeline::LoweredPipeline() = default;
LoweredPipeline::~LoweredPipeline() = default;

std::unique_ptr<LoweredPipeline>
LowerToPipeline(VecPlanState                *root,
                const PipelineSharedControl *shared_control,
                pg_atomic_uint64            *next_block,
                ParallelAggPartialState     *shared_slots,
                int                          num_slots,
                SharedFileSet               *spill_fileset)
{
	if (root == nullptr || shared_control == nullptr || next_block == nullptr)
		return nullptr;

	VecSeqScanState *scan_state = root->find_parallel_source_scan_state();
	VecAggState     *agg_state  = root->find_parallel_aggregate_state();

	if (scan_state == nullptr || agg_state == nullptr)
		return nullptr;

	auto bundle = std::make_unique<LoweredPipeline>();
	bundle->shared_control = shared_control;

	SeqScanSourceShared shared{};
	shared.next_block     = next_block;
	shared.total_blocks   = shared_control->total_blocks;
	shared.morsel_nblocks = shared_control->morsel_nblocks;

	bundle->source         = std::make_unique<SeqScanSource>(scan_state, shared);
	bundle->partial_agg_op = std::make_unique<PartialAggOp>(agg_state);
	bundle->agg_sink       = std::make_unique<AggSink>(agg_state);
	bundle->agg_sink->SetSharedSlots(shared_slots, num_slots, spill_fileset);

	bundle->pipeline.id   = 0;
	bundle->pipeline.src  = bundle->source.get();
	bundle->pipeline.ops.push_back(bundle->partial_agg_op.get());
	bundle->pipeline.sink = bundle->agg_sink.get();

	return bundle;
}

}
}

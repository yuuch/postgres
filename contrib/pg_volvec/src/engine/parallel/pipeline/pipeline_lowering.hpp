#pragma once

#include <memory>

#include "parallel/pipeline/dsm_control.hpp"
#include "parallel/pipeline/pipeline.hpp"

extern "C" {
#include "port/atomics.h"
}

struct SharedFileSet;

class VecPlanState;

namespace pg_volvec {

struct ParallelAggPartialState;

namespace pipeline {

class SeqScanSource;
class PartialAggOp;
class AggSink;

struct LoweredPipeline {
	LoweredPipeline();
	~LoweredPipeline();

	Pipeline                                pipeline;
	std::unique_ptr<SeqScanSource>          source;
	std::unique_ptr<PartialAggOp>           partial_agg_op;
	std::unique_ptr<AggSink>                agg_sink;
	const PipelineSharedControl            *shared_control = nullptr;
};

std::unique_ptr<LoweredPipeline>
LowerToPipeline(VecPlanState                *root,
                const PipelineSharedControl *shared_control,
                pg_atomic_uint64            *next_block,
                ParallelAggPartialState     *shared_slots,
                int                          num_slots,
                SharedFileSet               *spill_fileset);

}
}

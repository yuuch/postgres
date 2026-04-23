#pragma once

#include <memory>
#include <vector>

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

class Source;
class Operator;
class Sink;

/*
 * OwnedPipeline: a Pipeline plus the unique_ptr ownership of its
 * Source / Operator / Sink instances.
 *
 * The base-class unique_ptrs let LoweredPipeline hold heterogenous pipeline
 * shapes (P3+: HashJoin Build pipeline, Probe pipeline, etc.) without the
 * struct having to enumerate each concrete type. The Pipeline value inside
 * holds raw observer pointers into the unique_ptrs; lifetime is governed by
 * OwnedPipeline.
 *
 * Construction order matters: source/operators/sink unique_ptrs must be set
 * BEFORE Pipeline.src/.ops/.sink raw pointers are wired up, otherwise the
 * raw pointers would observe a not-yet-owned object.
 */
struct OwnedPipeline {
	OwnedPipeline();
	~OwnedPipeline();

	Pipeline                                      pipeline;
	std::unique_ptr<Source>                       source;
	std::vector<std::unique_ptr<Operator>>        operators;
	std::unique_ptr<Sink>                         sink;
};

/*
 * LoweredPipeline: container for one or more OwnedPipelines plus their
 * shared control reference.
 *
 * P1/P2 (Q1+Q6): pipelines.size() == 1.
 * P3+ (HashJoin): pipelines.size() == 2 (Build pipeline + Probe pipeline)
 *                 wired via Pipeline::depends_on.
 */
struct LoweredPipeline {
	LoweredPipeline();
	~LoweredPipeline();

	std::vector<std::unique_ptr<OwnedPipeline>>  pipelines;
	const PipelineSharedControl                 *shared_control = nullptr;

	OwnedPipeline *primary() const
	{
		return pipelines.empty() ? nullptr : pipelines.front().get();
	}
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

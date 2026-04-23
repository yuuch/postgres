#pragma once

#include "core/types.hpp"
#include "core/memory.hpp"
#include "core/data_chunk.hpp"
#include "core/data_chunk_deform.hpp"
#include "core/hash_table_defs.hpp"
#include "expr/expr.hpp"
#include "exec/plan_state.hpp"
#include "exec/agg.hpp"
}

extern "C" {
#include "storage/sharedfileset.h"
}

#include "parallel/pipeline/sink.hpp"

namespace pg_volvec {

struct ParallelAggPartialState;

namespace pipeline {

static_assert(PIPELINE_DEFAULT_CHUNK_SIZE == DEFAULT_CHUNK_SIZE,
              "pipeline chunk size must match core DEFAULT_CHUNK_SIZE");

/*
 * Process-parallel aggregate sink (DuckDB-shape adapted to PG bgworkers).
 *
 * SinkChunk is a no-op: PartialAggOp upstream consumes every batch into the
 * worker's VecAggState. Combine runs per worker (parallel across workers) and
 * exports that worker's partial into a DSM slot (or BufFile for grouped
 * spill). Finalize runs once on the leader, iterates DSM slots, merges into
 * merger_, then calls finish_sink so the orchestrator can drain via
 * get_next_batch.
 */
class AggSink : public Sink {
public:
	explicit AggSink(VecAggState *merger) : merger_(merger) {}

	/*
	 * Lowering hands in DSM-allocated cross-process scratch:
	 *   - slots: one ParallelAggPartialState per worker (DSM-resident array).
	 *   - spill_fileset: SharedFileSet for grouped overflow (may be nullptr
	 *     if the merger does not use file-backed partial state).
	 */
	void SetSharedSlots(ParallelAggPartialState *slots,
	                    int num_slots,
	                    SharedFileSet *spill_fileset)
	{
		shared_slots_ = slots;
		num_slots_ = num_slots;
		spill_fileset_ = spill_fileset;
	}

	std::unique_ptr<GlobalSinkState> GetGlobalSinkState(ExecCtx &ctx) override;
	std::unique_ptr<LocalSinkState>  GetLocalSinkState(ExecCtx &ctx, GlobalSinkState &gstate) override;

	SinkResultType
	SinkChunk(ExecCtx &ctx, PipelineChunk &in, OperatorSinkInput &input) override;

	SinkCombineResultType
	Combine(ExecCtx &ctx, OperatorSinkCombineInput &input) override;

	SinkFinalizeType
	Finalize(ExecCtx &ctx, GlobalSinkState &gstate) override;

	bool ParallelSink() const override { return true; }

	VecAggState *merger() const { return merger_; }

private:
	VecAggState              *merger_;
	ParallelAggPartialState  *shared_slots_ = nullptr;
	int                       num_slots_ = 0;
	SharedFileSet            *spill_fileset_ = nullptr;
};

class AggGlobalSinkState : public GlobalSinkState {
public:
	AggGlobalSinkState(VecAggState *merger,
	                   ParallelAggPartialState *slots,
	                   int num_slots,
	                   SharedFileSet *spill_fileset)
	    : merger(merger), slots(slots), num_slots(num_slots),
	      spill_fileset(spill_fileset) {}

	VecAggState              *merger;
	ParallelAggPartialState  *slots;
	int                       num_slots;
	SharedFileSet            *spill_fileset;
};

class AggLocalSinkState : public LocalSinkState {
public:
	AggLocalSinkState(VecAggState *worker_agg, int worker_index)
	    : worker_agg(worker_agg), worker_index(worker_index) {}

	VecAggState *worker_agg;
	int          worker_index;
};

}  /* namespace pipeline */
}  /* namespace pg_volvec */

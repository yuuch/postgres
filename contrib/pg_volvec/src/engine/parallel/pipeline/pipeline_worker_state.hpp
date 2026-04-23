#pragma once

#include <memory>

#include "parallel/pipeline/pipeline_worker_context.hpp"

extern "C" {
#include "postgres.h"
#include "access/relscan.h"
#include "executor/execdesc.h"
#include "nodes/plannodes.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
}

class VecPlanState;
class VecAggState;

class VecPlanState;
class VecAggState;

namespace pg_volvec {

class VecPlanState;
class VecAggState;

namespace pipeline {

struct PipelineWorkerState
{
	MemoryContext               memory_context = nullptr;
	PlannedStmt                *plannedstmt = nullptr;
	char                       *query_text = nullptr;
	EState                     *estate = nullptr;
	std::unique_ptr<pg_volvec::VecPlanState> root_plan;
	pg_volvec::VecAggState     *agg_state = nullptr;
	PipelineWorkerContext       worker_context{};
	uint64                      init_time_us = 0;

	PipelineWorkerState();
	~PipelineWorkerState();
	PipelineWorkerState(PipelineWorkerState &&) noexcept;
	PipelineWorkerState &operator=(PipelineWorkerState &&) noexcept;
	PipelineWorkerState(const PipelineWorkerState &) = delete;
	PipelineWorkerState &operator=(const PipelineWorkerState &) = delete;
};

bool
InitializePipelineWorkerState(const char         *plannedstmt_serialized,
                              const char         *query_text,
                              int                 agg_plan_node_id,
                              Oid                 source_scan_relid,
                              int                 source_scan_plan_node_id,
                              ParallelTableScanDesc parallel_scan_desc,
                              bool                leader,
                              PipelineWorkerState *out,
                              const char        **failure_reason);

void CleanupPipelineWorkerState(PipelineWorkerState *state);

void RegisterPipelineProcExitJitCleanup(PipelineWorkerState *state);

}
}

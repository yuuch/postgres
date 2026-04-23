#pragma once

extern "C" {
#include "postgres.h"
#include "access/relscan.h"
#include "executor/execdesc.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "utils/memutils.h"
}

class VecPlanState;
class VecAggState;

namespace pg_volvec {

class VecPlanState;
class VecAggState;

namespace pipeline {

struct PipelineWorkerContext {
	MemoryContext memory_context = nullptr;
	PlannedStmt *plannedstmt = nullptr;
	EState *estate = nullptr;
	::VecPlanState *root_plan = nullptr;
	::VecAggState *agg_state = nullptr;
	int agg_plan_node_id = -1;
	Oid parallel_scan_relid = InvalidOid;
	int parallel_scan_plan_node_id = -1;
	ParallelTableScanDesc parallel_scan_desc = nullptr;
	bool leader = false;
};

}
}

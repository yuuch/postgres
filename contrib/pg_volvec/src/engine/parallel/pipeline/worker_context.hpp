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
class VecHashJoinState;

/*
 * Process-local execution context for one leader/worker running one task.
 * Holds local executor objects only; never DSM-visible.
 *
 * Owned by the new pipeline runtime; legacy parallel_runtime.hpp re-includes
 * this header so existing exec/ callers compile unchanged during P2.
 */
struct ParallelWorkerContext {
	MemoryContext memory_context = nullptr;
	PlannedStmt *plannedstmt = nullptr;
	EState *estate = nullptr;
	VecPlanState *root_plan = nullptr;
	VecAggState *agg_state = nullptr;
	VecHashJoinState *hash_join_state = nullptr;
	int agg_plan_node_id = -1;
	int hash_join_plan_node_id = -1;
	int input_hash_join_plan_node_id = -1;
	Oid parallel_scan_relid = InvalidOid;
	int parallel_scan_plan_node_id = -1;
	ParallelTableScanDesc parallel_scan_desc = nullptr;
	bool hash_build_execution = false;
	bool leader = false;
};

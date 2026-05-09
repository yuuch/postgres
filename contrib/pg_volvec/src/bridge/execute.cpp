/*
 * bridge/execute.cpp — thin C++ shim between PostgreSQL executor hooks and
 * the pg_volvec pipeline runtime.
 *
 * M-FRAME-MIN step 2 (greenfield): no slot materialization, no serial
 * fallback loop. The bridge has exactly three jobs:
 *
 *   1. initialize_plan: run pipeline::Translator on the PG plan tree. If the
 *      Translator returns a PhysicalOperator root, store it as an opaque
 *      void* in state->parallel_plan and (when GUC pg_volvec.parallel=on) set
 *      a non-null parallel_scheduler sentinel. Returning false here causes
 *      ExecutorStart_hook to discard the query state, so ExecutorRun_hook
 *      finds nothing registered and falls through to standard_ExecutorRun.
 *
 *   2. delete_plan: tear down the PhysicalOperator tree owned by
 *      state->parallel_plan.
 *
 *   3. execute_query: when called by ExecutorRun_hook (which only happens if
 *      a state was registered), dispatch to pipeline::PgvolvecPipelineRun.
 *      Returning false triggers the hook to fall through to
 *      standard_ExecutorRun.
 *
 * Result materialization (DataChunk -> TupleTableSlot -> DestReceiver) lives
 * inside the pipeline runtime's OutputSink; the bridge no longer owns it.
 *
 * Spec: PIPELINE_PORT_PLAN.md §15.3.2, §15.4 (P3X-Q1 step 2).
 */

extern "C" {
#include "postgres.h"
#include "executor/executor.h"
#include "nodes/plannodes.h"
#include "utils/elog.h"
#include "utils/memutils.h"
}

#include <string>

#include "execute.h"

#include "parallel/pipeline/physical_operator.hpp"
#include "parallel/pipeline/pipeline_leader.hpp"
#include "parallel/pipeline/query_state.hpp"
#include "parallel/pipeline/runtime_dsm.hpp"
#include "parallel/pipeline/translator.hpp"

extern "C" {
extern bool pg_volvec_trace_hooks;
extern bool pg_volvec_trace_execution_path;
extern bool pg_volvec_parallel;
}

/*
 * Opaque non-null sentinel that satisfies the parallel-enabled gate in
 * pg_volvec_execute_query(). The real TaskScheduler ownership moves into
 * pipeline_leader once M-FRAME-MIN lands its scheduler implementation.
 */
static char pgvolvec_parallel_scheduler_sentinel;

namespace {

static const char *
PgvolvecPlanNodeName(Plan *plan)
{
	if (plan == nullptr)
		return "NULL";
	if (IsA(plan, Gather))
		return "Gather";
	if (IsA(plan, GatherMerge))
		return "GatherMerge";
	if (IsA(plan, Agg))
		return "Agg";
	if (IsA(plan, Sort))
		return "Sort";
	if (IsA(plan, Limit))
		return "Limit";
	if (IsA(plan, SeqScan))
		return "SeqScan";
	if (IsA(plan, HashJoin))
		return "HashJoin";
	if (IsA(plan, MergeJoin))
		return "MergeJoin";
	if (IsA(plan, NestLoop))
		return "NestLoop";
	if (IsA(plan, SubqueryScan))
		return "SubqueryScan";
	if (IsA(plan, Material))
		return "Material";
	if (IsA(plan, Hash))
		return "Hash";
	if (IsA(plan, Append))
		return "Append";
	if (IsA(plan, MergeAppend))
		return "MergeAppend";
	return "Other";
}

static void
AppendPlanTreeSummary(Plan *plan, std::string &out)
{
	if (plan == nullptr)
		return;
	if (!out.empty())
		out += " -> ";
	out += PgvolvecPlanNodeName(plan);
	out += "(";
	out += std::to_string((int) nodeTag(plan));
	out += ")";

	if (IsA(plan, Append))
	{
		ListCell *lc;
		foreach(lc, ((Append *) plan)->appendplans)
			AppendPlanTreeSummary((Plan *) lfirst(lc), out);
		return;
	}
	if (IsA(plan, MergeAppend))
	{
		ListCell *lc;
		foreach(lc, ((MergeAppend *) plan)->mergeplans)
			AppendPlanTreeSummary((Plan *) lfirst(lc), out);
		return;
	}
	if (IsA(plan, SubqueryScan))
	{
		AppendPlanTreeSummary(((SubqueryScan *) plan)->subplan, out);
		return;
	}

	AppendPlanTreeSummary(plan->lefttree, out);
	AppendPlanTreeSummary(plan->righttree, out);
}

static const char *
PlanTreeSummaryCString(Plan *plan)
{
	std::string summary;
	AppendPlanTreeSummary(plan, summary);
	if (summary.empty())
		summary = "<empty-plan-tree>";
	return pstrdup(summary.c_str());
}

} // namespace

extern "C" {

bool
pg_volvec_initialize_plan(QueryDesc *queryDesc, pg_volvec::PgVolVecQueryState *state_ptr)
{
	if (state_ptr == nullptr || queryDesc == nullptr || queryDesc->plannedstmt == nullptr)
		return false;

	MemoryContext old_context = MemoryContextSwitchTo(state_ptr->context);

	if (pg_volvec_trace_hooks)
		elog(LOG,
			 "pg_volvec: initialize_plan root_nodeTag=%d parallelModeNeeded=%s operation=%d",
			 queryDesc->plannedstmt->planTree != nullptr
				 ? (int) nodeTag(queryDesc->plannedstmt->planTree) : -1,
			 queryDesc->plannedstmt->parallelModeNeeded ? "on" : "off",
			 (int) queryDesc->operation);

	state_ptr->parallel_plan = nullptr;
	state_ptr->parallel_scheduler = nullptr;

	{
		const char *dsm_err = nullptr;
		if (!pg_volvec::pipeline::CreateRuntimeDsm(state_ptr, &dsm_err))
		{
			MemoryContextSwitchTo(old_context);
			elog(WARNING,
				 "pg_volvec: %s; falling back to standard PostgreSQL executor",
				 dsm_err != nullptr ? dsm_err : "runtime DSM/DSA allocation failed");
			return false;
		}
	}

	std::unique_ptr<pg_volvec::pipeline::PhysicalOperator> root =
		pg_volvec::pipeline::Translator::Translate(queryDesc, state_ptr);

	if (root != nullptr)
	{
		state_ptr->parallel_plan = static_cast<void *>(root.release());
		if (pg_volvec_parallel)
			state_ptr->parallel_scheduler = &pgvolvec_parallel_scheduler_sentinel;
	}

	MemoryContextSwitchTo(old_context);

	if (state_ptr->parallel_plan == nullptr)
	{
		pg_volvec::pipeline::DestroyRuntimeDsm(state_ptr);
		const char *plan_summary = PlanTreeSummaryCString(queryDesc->plannedstmt->planTree);
		elog(WARNING,
			 "pg_volvec: unsupported plan shape, falling back to standard PostgreSQL executor; root=%s plan_nodes=%s",
			 PgvolvecPlanNodeName(queryDesc->plannedstmt->planTree),
			 plan_summary);
		return false;
	}

	if (!pg_volvec_parallel)
		elog(WARNING,
			 "pg_volvec: pg_volvec.parallel=off, falling back to standard PostgreSQL executor");

	return state_ptr->parallel_scheduler != nullptr;
}

void
pg_volvec_delete_plan(pg_volvec::PgVolVecQueryState *state_ptr)
{
	if (state_ptr == nullptr)
		return;

	state_ptr->parallel_scheduler = nullptr;
	if (state_ptr->parallel_plan != nullptr)
	{
		auto *root = static_cast<pg_volvec::pipeline::PhysicalOperator *>(state_ptr->parallel_plan);
		state_ptr->parallel_plan = nullptr;

		PG_TRY();
		{
			delete root;
		}
		PG_CATCH();
		{
			FlushErrorState();
			elog(WARNING, "pg_volvec: error during PhysicalOperator teardown, suppressed");
		}
		PG_END_TRY();
	}

	pg_volvec::pipeline::DestroyRuntimeDsm(state_ptr);
}

bool
pg_volvec_execute_query(QueryDesc *queryDesc, pg_volvec::PgVolVecQueryState *state_ptr,
						ScanDirection direction, uint64 count)
{
	(void) direction;
	(void) count;

	if (state_ptr == nullptr ||
		state_ptr->parallel_plan == nullptr ||
		state_ptr->parallel_scheduler == nullptr)
		return false;

	const char *failure_reason = nullptr;
	bool ok = pg_volvec::pipeline::PgvolvecPipelineRun(queryDesc, state_ptr, &failure_reason);

	if (!ok)
	{
		const char *plan_summary = PlanTreeSummaryCString(
			queryDesc != nullptr && queryDesc->plannedstmt != nullptr ? queryDesc->plannedstmt->planTree : nullptr);
		elog(WARNING,
			 "pg_volvec: pipeline run skipped (%s), falling back to standard PostgreSQL executor; root=%s plan_nodes=%s",
			 failure_reason != nullptr ? failure_reason : "no reason recorded",
			 queryDesc != nullptr && queryDesc->plannedstmt != nullptr ?
			 	PgvolvecPlanNodeName(queryDesc->plannedstmt->planTree) : "NULL",
			 plan_summary);
		return false;
	}

	if (pg_volvec_trace_execution_path)
		elog(LOG, "pg_volvec_path: path=pipeline detail=duckdb_style_pipeline");

	return true;
}

}  /* extern "C" */

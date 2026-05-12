/*
 * bridge/execute.cpp — thin C++ shim between PostgreSQL executor hooks and
 * the pg_yaap pipeline runtime.
 *
 * M-FRAME-MIN step 2 (greenfield): no slot materialization, no silent
 * fallback loop. The bridge has exactly three jobs:
 *
 *   1. initialize_plan: run pipeline::Translator on the PG plan tree. If the
 *      Translator returns a PhysicalOperator root, store it as an opaque
 *      void* in state->parallel_plan and (when GUC pg_yaap.parallel=on) set
 *      a non-null parallel_scheduler sentinel. Initialization failures are
 *      raised as ERRORs; a false return is reserved for the "pg_yaap did not
 *      admit this query at all" case before state registration.
 *
 *   2. delete_plan: tear down the PhysicalOperator tree owned by
 *      state->parallel_plan.
 *
 *   3. execute_query: when called by ExecutorRun_hook (which only happens if
 *      a state was registered), dispatch to pipeline::PgYaapPipelineRun.
 *      Execution failures are raised as ERRORs; a false return is reserved
 *      for the no-state/no-admission case.
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

#include "optimizer_registry.hpp"
#include "parallel/pipeline/physical_operator.hpp"
#include "parallel/pipeline/pipeline_leader.hpp"
#include "parallel/pipeline/query_state.hpp"
#include "parallel/pipeline/runtime_dsm.hpp"
#include "parallel/pipeline/translator.hpp"
#include "parallel/pipeline/yaap_opt_translator.hpp"

extern "C" {
extern bool pg_yaap_trace_hooks;
extern bool pg_yaap_trace_execution_path;
extern bool pg_yaap_parallel;
}

/*
 * Opaque non-null sentinel that satisfies the parallel-enabled gate in
 * pg_yaap_execute_query(). The real TaskScheduler ownership moves into
 * pipeline_leader once M-FRAME-MIN lands its scheduler implementation.
 */
static char pgyaap_parallel_scheduler_sentinel;
static char pgyaap_serial_plan_sentinel;

namespace {

static const char *
PgYaapPlanNodeName(Plan *plan)
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
	out += PgYaapPlanNodeName(plan);
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
pg_yaap_initialize_plan(QueryDesc *queryDesc, PgYaapQueryState *state_ptr)
{
	auto *state = reinterpret_cast<pg_yaap::PgYaapQueryState *>(state_ptr);

	if (state == nullptr || queryDesc == nullptr || queryDesc->plannedstmt == nullptr)
		return false;

	MemoryContext old_context = MemoryContextSwitchTo(state->context);

	if (pg_yaap_trace_hooks)
		elog(LOG,
			 "pg_yaap: initialize_plan root_nodeTag=%d parallelModeNeeded=%s operation=%d",
			 queryDesc->plannedstmt->planTree != nullptr
				 ? (int) nodeTag(queryDesc->plannedstmt->planTree) : -1,
			 queryDesc->plannedstmt->parallelModeNeeded ? "on" : "off",
			 (int) queryDesc->operation);

	state->parallel_plan = nullptr;
	state->parallel_scheduler = nullptr;

	if (queryDesc->plannedstmt != nullptr)
	{
		if (pg_yaap::OptimizerPlanBundle *bundle =
				pg_yaap::LookupOptimizerPlanBundle(queryDesc->plannedstmt))
		{
			{
				pg_yaap::OptimizerPlanSupportStatus support =
					pg_yaap::AnalyzeOptimizerPlanSupport(*bundle);
				if (!support.supported)
				{
					const char *errmsg_cstr = pstrdup(psprintf(
						"pg_yaap: unsupported optimizer node at %s: %s",
						support.path.c_str(),
						support.detail.c_str()));
					MemoryContextSwitchTo(old_context);
					ereport(ERROR, (errmsg("%s", errmsg_cstr)));
				}
			}
			if (pg_yaap::CanExecuteOptimizerPlanSerial(*bundle))
			{
				if (pg_yaap_trace_hooks)
					elog(LOG,
						 "pg_yaap: using serial optimizer-executor glue path optimizer_plan=%s pg_plan_root=%s",
						 pg_yaap::DescribeOptimizerPlan(*bundle).c_str(),
						 PgYaapPlanNodeName(queryDesc->plannedstmt->planTree));
				state->parallel_plan = &pgyaap_serial_plan_sentinel;
				state->parallel_scheduler = &pgyaap_parallel_scheduler_sentinel;
				MemoryContextSwitchTo(old_context);
				return true;
			}
		}
	}

	{
		const char *dsm_err = nullptr;
		if (!pg_yaap::pipeline::CreateRuntimeDsm(state, &dsm_err))
		{
			MemoryContextSwitchTo(old_context);
			ereport(ERROR,
					(errmsg("pg_yaap: %s",
							dsm_err != nullptr ? dsm_err : "runtime DSM/DSA allocation failed")));
		}
	}

	std::unique_ptr<pg_yaap::pipeline::PhysicalOperator> root;
	bool saw_optimizer_bundle = false;
	if (queryDesc->plannedstmt != nullptr)
	{
		if (pg_yaap::OptimizerPlanBundle *bundle =
				pg_yaap::LookupOptimizerPlanBundle(queryDesc->plannedstmt))
		{
			saw_optimizer_bundle = true;
			if (pg_yaap_trace_hooks)
				elog(LOG,
					 "pg_yaap: lowering optimizer physical plan into executor pipeline optimizer_plan=%s pg_plan_root=%s pg_plan_nodes=%s",
					 pg_yaap::DescribeOptimizerPlan(*bundle).c_str(),
					 PgYaapPlanNodeName(queryDesc->plannedstmt->planTree),
					 PlanTreeSummaryCString(queryDesc->plannedstmt->planTree));
			root = pg_yaap::TranslateOptimizerPlan(queryDesc, state, *bundle);
			if (root == nullptr)
			{
				const char *errmsg_cstr = "pg_yaap: optimizer support analysis passed, but lowering returned null";
				MemoryContextSwitchTo(old_context);
				pg_yaap::pipeline::DestroyRuntimeDsm(state);
				ereport(ERROR, (errmsg("%s", errmsg_cstr)));
			}
		}
	}
	if (root == nullptr && !saw_optimizer_bundle)
		root = pg_yaap::pipeline::Translator::Translate(queryDesc, state);

	MemoryContextSwitchTo(old_context);

	if (root == nullptr)
	{
		pg_yaap::pipeline::DestroyRuntimeDsm(state);
		const char *plan_summary = PlanTreeSummaryCString(queryDesc->plannedstmt->planTree);
		ereport(ERROR,
				(errmsg("pg_yaap: executor lowering produced no plan; root=%s plan_nodes=%s",
						PgYaapPlanNodeName(queryDesc->plannedstmt->planTree),
						plan_summary)));
	}

	if (!pg_yaap_parallel)
	{
		pg_yaap::pipeline::DestroyRuntimeDsm(state);
		ereport(ERROR,
				(errmsg("pg_yaap: pipeline executor requires pg_yaap.parallel=on")));
	}

	state->parallel_scheduler = &pgyaap_parallel_scheduler_sentinel;
	state->parallel_plan = static_cast<void *>(root.release());
	return true;
}

void
pg_yaap_delete_plan(PgYaapQueryState *state_ptr)
{
	auto *state = reinterpret_cast<pg_yaap::PgYaapQueryState *>(state_ptr);

	if (state == nullptr)
		return;

	state->parallel_scheduler = nullptr;
	if (state->parallel_plan != nullptr)
	{
		if (state->parallel_plan == &pgyaap_serial_plan_sentinel)
		{
			state->parallel_plan = nullptr;
			pg_yaap::pipeline::DestroyRuntimeDsm(state);
			return;
		}

		auto *root = static_cast<pg_yaap::pipeline::PhysicalOperator *>(state->parallel_plan);
		state->parallel_plan = nullptr;

		PG_TRY();
		{
			delete root;
		}
		PG_CATCH();
		{
			FlushErrorState();
			elog(WARNING, "pg_yaap: error during PhysicalOperator teardown, suppressed");
		}
		PG_END_TRY();
	}

	pg_yaap::pipeline::DestroyRuntimeDsm(state);
}

bool
pg_yaap_execute_query(QueryDesc *queryDesc, PgYaapQueryState *state_ptr,
						ScanDirection direction, uint64 count)
{
	const char *failure_reason = nullptr;
	auto *state = reinterpret_cast<pg_yaap::PgYaapQueryState *>(state_ptr);

	(void) direction;
	(void) count;

	if (state == nullptr)
		return false;
	if (state->parallel_scheduler == nullptr)
		ereport(ERROR,
				(errmsg("pg_yaap: executor state is registered without a scheduler")));

	if (queryDesc != nullptr && queryDesc->plannedstmt != nullptr &&
		state->parallel_plan == &pgyaap_serial_plan_sentinel)
	{
		if (pg_yaap::OptimizerPlanBundle *bundle =
				pg_yaap::LookupOptimizerPlanBundle(queryDesc->plannedstmt))
		{
			if (pg_yaap::ExecuteOptimizerPlanSerial(queryDesc, *bundle, &failure_reason))
				return true;
			ereport(ERROR,
					(errmsg("pg_yaap: serial optimizer executor failed: %s",
							failure_reason != nullptr ? failure_reason : "no reason recorded")));
		}
		ereport(ERROR,
				(errmsg("pg_yaap: serial optimizer executor is missing its optimizer bundle")));
	}

	if (state->parallel_plan == nullptr)
		ereport(ERROR,
				(errmsg("pg_yaap: executor state is registered without a physical plan")));

	bool ok = pg_yaap::pipeline::PgYaapPipelineRun(queryDesc, state, &failure_reason);

	if (!ok)
	{
		const char *plan_summary = PlanTreeSummaryCString(
			queryDesc != nullptr && queryDesc->plannedstmt != nullptr ? queryDesc->plannedstmt->planTree : nullptr);
		ereport(ERROR,
				(errmsg("pg_yaap: pipeline execution failed: %s; root=%s plan_nodes=%s",
						failure_reason != nullptr ? failure_reason : "no reason recorded",
						queryDesc != nullptr && queryDesc->plannedstmt != nullptr ?
							PgYaapPlanNodeName(queryDesc->plannedstmt->planTree) : "NULL",
						plan_summary)));
	}

	if (pg_yaap_trace_execution_path)
		elog(LOG, "pg_yaap_path: path=pipeline detail=yaap_pipeline");

	return true;
}

}  /* extern "C" */

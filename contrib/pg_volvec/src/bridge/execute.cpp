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
#include "utils/elog.h"
#include "utils/memutils.h"
}

#include "execute.h"

#include "parallel/pipeline/physical_operator.hpp"
#include "parallel/pipeline/pipeline_leader.hpp"
#include "parallel/pipeline/query_state.hpp"
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

	/*
	 * B2.3 Layer A boundary: the C++ Translator may transitively call PG C
	 * code that ereport(ERROR)s. PG_TRY/PG_CATCH translates that longjmp
	 * into a clean WARNING + fallback so destructors of any half-built
	 * PhysicalOperator subtree run via state->context teardown later.
	 */
	std::unique_ptr<pg_volvec::pipeline::PhysicalOperator> root;
	volatile bool caught_error = false;

	PG_TRY();
	{
		root = pg_volvec::pipeline::Translator::Translate(queryDesc->plannedstmt);
	}
	PG_CATCH();
	{
		caught_error = true;
		FlushErrorState();
		root.reset();
	}
	PG_END_TRY();

	if (caught_error)
	{
		MemoryContextSwitchTo(old_context);
		elog(WARNING,
			 "pg_volvec: translator raised error, falling back to standard PostgreSQL executor");
		return false;
	}

	if (root != nullptr)
	{
		state_ptr->parallel_plan = static_cast<void *>(root.release());
		if (pg_volvec_parallel)
			state_ptr->parallel_scheduler = &pgvolvec_parallel_scheduler_sentinel;
	}

	MemoryContextSwitchTo(old_context);

	if (state_ptr->parallel_plan == nullptr)
	{
		elog(WARNING,
			 "pg_volvec: unsupported plan shape, falling back to standard PostgreSQL executor");
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
	volatile bool ok = false;
	volatile bool caught_error = false;

	PG_TRY();
	{
		ok = pg_volvec::pipeline::PgvolvecPipelineRun(queryDesc, state_ptr, &failure_reason);
	}
	PG_CATCH();
	{
		caught_error = true;
		FlushErrorState();
		ok = false;
	}
	PG_END_TRY();

	if (caught_error)
	{
		elog(WARNING,
			 "pg_volvec: pipeline run raised error, falling back to standard PostgreSQL executor");
		return false;
	}

	if (!ok)
	{
		if (pg_volvec_trace_hooks || pg_volvec_trace_execution_path)
			elog(LOG,
				 "pg_volvec: pipeline run skipped (%s), falling back to standard PostgreSQL executor",
				 failure_reason != nullptr ? failure_reason : "no reason recorded");
		return false;
	}

	if (pg_volvec_trace_execution_path)
		elog(LOG, "pg_volvec_path: path=pipeline detail=duckdb_style_pipeline");

	return true;
}

}  /* extern "C" */

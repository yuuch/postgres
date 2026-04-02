#include "postgres.h"

#include "executor/executor.h"
#include "utils/guc.h"
#include "nodes/print.h"
#include "utils/snapmgr.h"

#include "execute.h"
#include "state.h"

PG_MODULE_MAGIC;

static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;

static bool pg_volvec_enabled = true;
bool pg_volvec_trace_hooks = false;
bool pg_volvec_jit_deform = true;

static void pg_volvec_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void pg_volvec_ExecutorRun(QueryDesc *queryDesc,
							   ScanDirection direction,
							   uint64 count);
static void pg_volvec_ExecutorFinish(QueryDesc *queryDesc);
static void pg_volvec_ExecutorEnd(QueryDesc *queryDesc);

void            _PG_init(void);
void            _PG_fini(void);

void
_PG_init(void)
{
	DefineCustomBoolVariable("pg_volvec.enabled",
							 "Enable the pg_volvec executor hook.",
							 NULL,
							 &pg_volvec_enabled,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_volvec.trace_hooks",
							 "Emit WARNING messages whenever pg_volvec executor hooks run.",
							 NULL,
							 &pg_volvec_trace_hooks,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_volvec.jit_deform",
							 "Enable LLVM JIT deform for pg_volvec when a supported deform program is available.",
							 NULL,
							 &pg_volvec_jit_deform,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	pg_volvec_init_state_table();

	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = pg_volvec_ExecutorStart;

	prev_ExecutorRun = ExecutorRun_hook;
	ExecutorRun_hook = pg_volvec_ExecutorRun;

	prev_ExecutorFinish = ExecutorFinish_hook;
	ExecutorFinish_hook = pg_volvec_ExecutorFinish;

	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = pg_volvec_ExecutorEnd;
}

void
_PG_fini(void)
{
	ExecutorStart_hook = prev_ExecutorStart;
	ExecutorRun_hook = prev_ExecutorRun;
	ExecutorFinish_hook = prev_ExecutorFinish;
	ExecutorEnd_hook = prev_ExecutorEnd;
}

static void
pg_volvec_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	PgVolVecQueryState *state;

	if (prev_ExecutorStart)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);

	if (!pg_volvec_enabled)
		return;

	state = pg_volvec_try_build_query_state(queryDesc, eflags);
	if (state != NULL)
	{
		if (pg_volvec_initialize_plan(queryDesc, state)) {
			pg_volvec_register_state(queryDesc, state);
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: [VERSION 6 - JIT_FIX] registered volvec plan for query");
		} else {
			pg_volvec_close_query_state(state);
		}
	}
}

static void
pg_volvec_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count)
{
	PgVolVecQueryState *state = pg_volvec_lookup_state(queryDesc);

	if (state != NULL)
	{
		/* Use active snapshot if estate's is missing */
		if (queryDesc->estate->es_snapshot == NULL)
			queryDesc->estate->es_snapshot = GetActiveSnapshot();

		if (pg_volvec_execute_query(queryDesc, state, direction, count))
		{
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: ExecutorRun hook completed plan in pg_volvec");
			return;
		}
	}

	if (prev_ExecutorRun)
		prev_ExecutorRun(queryDesc, direction, count);
	else
		standard_ExecutorRun(queryDesc, direction, count);
}

static void
pg_volvec_ExecutorFinish(QueryDesc *queryDesc)
{
	if (prev_ExecutorFinish)
		prev_ExecutorFinish(queryDesc);
	else
		standard_ExecutorFinish(queryDesc);
}

static void
pg_volvec_ExecutorEnd(QueryDesc *queryDesc)
{
	PgVolVecQueryState *state = pg_volvec_lookup_state(queryDesc);

	if (state != NULL) {
		if (pg_volvec_trace_hooks)
			elog(LOG, "pg_volvec: ExecutorEnd closing query state");
		pg_volvec_close_query_state(state);
	}

	pg_volvec_unregister_state(queryDesc);

	if (prev_ExecutorEnd)
		prev_ExecutorEnd(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
}

#include "postgres.h"

#include "executor/executor.h"
#include "utils/guc.h"

#include "execute.h"
#include "state.h"
#include "../translate/pg_translate.h"

PG_MODULE_MAGIC;

static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;

static bool pg_vec_enabled = true;
static bool pg_vec_trace_hooks = false;

static void pg_vec_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void pg_vec_ExecutorRun(QueryDesc *queryDesc,
							   ScanDirection direction,
							   uint64 count);
static void pg_vec_ExecutorFinish(QueryDesc *queryDesc);
static void pg_vec_ExecutorEnd(QueryDesc *queryDesc);
static void pg_vec_trace_hook_warning(const char *hook_name,
										  const char *detail);

void		_PG_init(void);
void		_PG_fini(void);

void
_PG_init(void)
{
	DefineCustomBoolVariable("pg_vec.enabled",
							 "Enable the pg_vec executor hook.",
							 NULL,
							 &pg_vec_enabled,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_vec.trace_hooks",
							 "Emit WARNING messages whenever pg_vec executor hooks run.",
							 NULL,
							 &pg_vec_trace_hooks,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	pg_vec_init_state_table();

	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = pg_vec_ExecutorStart;

	prev_ExecutorRun = ExecutorRun_hook;
	ExecutorRun_hook = pg_vec_ExecutorRun;

	prev_ExecutorFinish = ExecutorFinish_hook;
	ExecutorFinish_hook = pg_vec_ExecutorFinish;

	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = pg_vec_ExecutorEnd;
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
pg_vec_trace_hook_warning(const char *hook_name, const char *detail)
{
	if (!pg_vec_trace_hooks)
		return;

	if (detail != NULL)
		elog(WARNING, "pg_vec: %s hook active: %s", hook_name, detail);
	else
		elog(WARNING, "pg_vec: %s hook active", hook_name);
}

static void
pg_vec_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	PgVecQueryState *state;

	pg_vec_trace_hook_warning("ExecutorStart", NULL);

	if (prev_ExecutorStart)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);

	if (!pg_vec_enabled)
	{
		pg_vec_trace_hook_warning("ExecutorStart", "pg_vec.enabled=off");
		return;
	}

	state = pg_vec_try_build_query_state(queryDesc, eflags);
	if (state != NULL)
	{
		pg_vec_register_state(queryDesc, state);
		pg_vec_trace_hook_warning("ExecutorStart",
								  psprintf("registered %s plan",
										   pg_vec_plan_kind_name(state->plan.kind)));
		elog(LOG, "pg_vec: registered %s execution path",
			 pg_vec_plan_kind_name(state->plan.kind));
	}
}

static void
pg_vec_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count)
{
	PgVecQueryState *state = pg_vec_lookup_state(queryDesc);

	pg_vec_trace_hook_warning("ExecutorRun", NULL);

	if (state != NULL)
	{
		pg_vec_trace_hook_warning("ExecutorRun",
								  psprintf("taking over %s plan",
										   pg_vec_plan_kind_name(state->plan.kind)));
		if (pg_vec_execute_query(queryDesc, state, direction, count))
		{
			pg_vec_trace_hook_warning("ExecutorRun",
									  psprintf("completed %s plan in pg_vec",
											   pg_vec_plan_kind_name(state->plan.kind)));
			return;
		}

		elog(WARNING, "pg_vec: fallback to standard executor during ExecutorRun for %s plan",
			 pg_vec_plan_kind_name(state->plan.kind));
	}
	else
		pg_vec_trace_hook_warning("ExecutorRun", "no pg_vec state registered");

	if (prev_ExecutorRun)
		prev_ExecutorRun(queryDesc, direction, count);
	else
		standard_ExecutorRun(queryDesc, direction, count);
}

static void
pg_vec_ExecutorFinish(QueryDesc *queryDesc)
{
	pg_vec_trace_hook_warning("ExecutorFinish",
							  pg_vec_lookup_state(queryDesc) != NULL ?
							  "query has pg_vec state" :
							  "query has no pg_vec state");

	if (prev_ExecutorFinish)
		prev_ExecutorFinish(queryDesc);
	else
		standard_ExecutorFinish(queryDesc);
}

static void
pg_vec_ExecutorEnd(QueryDesc *queryDesc)
{
	PgVecQueryState *state = pg_vec_lookup_state(queryDesc);

	pg_vec_trace_hook_warning("ExecutorEnd",
							  state != NULL ?
							  "query has pg_vec state" :
							  "query has no pg_vec state");

	if (state != NULL)
		pg_vec_close_query_state(state);

	pg_vec_unregister_state(queryDesc);

	if (prev_ExecutorEnd)
		prev_ExecutorEnd(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
}

#include "postgres.h"

#include "access/table.h"
#include "executor/tuptable.h"
#include "miscadmin.h"
#include "utils/hsearch.h"
#include "utils/guc.h"
#include "utils/memutils.h"

#include "state.h"
#include "../translate/pg_translate.h"

typedef struct PgVecStateEntry
{
	QueryDesc  *queryDesc;
	PgVecQueryState *state;
} PgVecStateEntry;

static HTAB *pg_vec_state_htab = NULL;
static int pg_vec_translation_guard_depth = 0;
static const int pg_vec_min_stack_depth_kb = 7168;

static void
pg_vec_ensure_translation_stack_depth(void)
{
	if (max_stack_depth >= pg_vec_min_stack_depth_kb)
		return;

	SetConfigOption("max_stack_depth", "7MB", PGC_SUSET, PGC_S_SESSION);
}

void
pg_vec_init_state_table(void)
{
	HASHCTL		ctl;

	if (pg_vec_state_htab != NULL)
		return;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(QueryDesc *);
	ctl.entrysize = sizeof(PgVecStateEntry);
	ctl.hcxt = TopMemoryContext;

	pg_vec_state_htab = hash_create("pg_vec query state table",
									32,
									&ctl,
									HASH_BLOBS | HASH_ELEM | HASH_CONTEXT);
}

PgVecQueryState *
pg_vec_lookup_state(QueryDesc *queryDesc)
{
	PgVecStateEntry *entry;

	if (pg_vec_state_htab == NULL)
		return NULL;

	entry = hash_search(pg_vec_state_htab, &queryDesc, HASH_FIND, NULL);
	return entry != NULL ? entry->state : NULL;
}

void
pg_vec_register_state(QueryDesc *queryDesc, PgVecQueryState *state)
{
	PgVecStateEntry *entry;
	bool		found;

	entry = hash_search(pg_vec_state_htab, &queryDesc, HASH_ENTER, &found);
	entry->queryDesc = queryDesc;
	entry->state = state;
}

void
pg_vec_unregister_state(QueryDesc *queryDesc)
{
	if (pg_vec_state_htab != NULL)
		hash_search(pg_vec_state_htab, &queryDesc, HASH_REMOVE, NULL);
}

PgVecQueryState *
pg_vec_try_build_query_state(QueryDesc *queryDesc, int eflags)
{
	MemoryContext oldcxt;
	PgVecQueryState *state;
	PgVecPlan	plan;
	const char *failure_reason = NULL;
	ErrorData  *edata = NULL;
	MemoryContext errcxt;
	char	   *errmsg = NULL;
	char	   *detail = NULL;

	if (queryDesc == NULL || queryDesc->operation != CMD_SELECT ||
		queryDesc->estate == NULL)
		return NULL;
	if (pg_vec_translation_guard_active())
		return NULL;

	pg_vec_ensure_translation_stack_depth();

	PG_TRY();
	{
		if (!pg_vec_try_translate_plan(queryDesc, eflags, &plan, &failure_reason))
		{
			elog(WARNING, "pg_vec: fallback to standard executor: %s",
				 failure_reason != NULL ? failure_reason :
				 "query could not be translated into pg_vec IR");
			return NULL;
		}
	}
	PG_CATCH();
	{
		errcxt = MemoryContextSwitchTo(TopMemoryContext);
		edata = CopyErrorData();
		if (edata != NULL && edata->message != NULL)
			errmsg = pstrdup(edata->message);
		if (edata != NULL && edata->funcname != NULL)
			detail = psprintf("%s [func=%s file=%s line=%d]",
							  errmsg != NULL ? errmsg : "translation raised an internal error",
							  edata->funcname,
							  edata->filename != NULL ? edata->filename : "?",
							  edata->lineno);
		MemoryContextSwitchTo(errcxt);
		FlushErrorState();
		elog(WARNING, "pg_vec: fallback to standard executor: %s",
			 detail != NULL ? detail :
			 errmsg != NULL ? errmsg :
			 "translation raised an internal error");
		if (edata != NULL)
		{
			MemoryContextSwitchTo(TopMemoryContext);
			FreeErrorData(edata);
			MemoryContextSwitchTo(errcxt);
		}
		if (errmsg != NULL)
		{
			MemoryContextSwitchTo(TopMemoryContext);
			pfree(errmsg);
			MemoryContextSwitchTo(errcxt);
		}
		if (detail != NULL)
		{
			MemoryContextSwitchTo(TopMemoryContext);
			pfree(detail);
			MemoryContextSwitchTo(errcxt);
		}
		return NULL;
	}
	PG_END_TRY();

	oldcxt = MemoryContextSwitchTo(queryDesc->estate->es_query_cxt);
	state = palloc0(sizeof(PgVecQueryState));
	state->plan = plan;
	state->result_slot = ExecInitExtraTupleSlot(queryDesc->estate,
												queryDesc->tupDesc,
												&TTSOpsVirtual);
	MemoryContextSwitchTo(oldcxt);

	switch (state->plan.kind)
	{
		case PG_VEC_PLAN_SCAN_FILTER_AGG:
			for (int input_id = 0; input_id < state->plan.ninputs; input_id++)
			{
				if (state->plan.inputs[input_id].kind != PG_VEC_INPUT_RELATION)
					continue;
				state->rels[input_id] = table_open(state->plan.inputs[input_id].relid,
												   NoLock);
			}
			return state;

		case PG_VEC_PLAN_UNSUPPORTED:
		default:
			elog(WARNING, "pg_vec: fallback to standard executor: unsupported translated plan kind %d",
				 (int) state->plan.kind);
			pfree(state);
			return NULL;
	}
}

bool
pg_vec_translation_guard_active(void)
{
	return pg_vec_translation_guard_depth > 0;
}

void
pg_vec_push_translation_guard(void)
{
	pg_vec_translation_guard_depth++;
}

void
pg_vec_pop_translation_guard(void)
{
	if (pg_vec_translation_guard_depth > 0)
		pg_vec_translation_guard_depth--;
}

void
pg_vec_close_query_state(PgVecQueryState *state)
{
	if (state == NULL)
		return;

	for (int input_id = 0; input_id < PG_VEC_MAX_INPUTS; input_id++)
	{
		if (state->rels[input_id] != NULL)
			table_close(state->rels[input_id], NoLock);
	}
}

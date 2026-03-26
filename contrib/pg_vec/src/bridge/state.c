#include "postgres.h"

#include "access/table.h"
#include "executor/tuptable.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"

#include "state.h"
#include "../translate/pg_translate.h"

typedef struct PgVecStateEntry
{
	QueryDesc  *queryDesc;
	PgVecQueryState *state;
} PgVecStateEntry;

static HTAB *pg_vec_state_htab = NULL;

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

	if (!pg_vec_try_translate_plan(queryDesc, eflags, &plan, &failure_reason))
	{
		elog(WARNING, "pg_vec: fallback to standard executor: %s",
			 failure_reason != NULL ? failure_reason :
			 "query could not be translated into pg_vec IR");
		return NULL;
	}

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
				state->rels[input_id] = table_open(state->plan.inputs[input_id].relid,
												   NoLock);
			return state;

		case PG_VEC_PLAN_UNSUPPORTED:
		default:
			elog(WARNING, "pg_vec: fallback to standard executor: unsupported translated plan kind %d",
				 (int) state->plan.kind);
			pfree(state);
			return NULL;
	}
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

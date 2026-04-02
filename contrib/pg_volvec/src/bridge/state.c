#include "postgres.h"
#include "utils/hsearch.h"
#include "state.h"
#include "execute.h"
#include "nodes/plannodes.h"
#include "utils/memutils.h"

/* Definition of PgVolVecQueryState for C code */
struct PgVolVecQueryState
{
	MemoryContext context;
	void *vec_plan; /* placeholder for C++ unique_ptr */
};

typedef struct StateEntry
{
	QueryDesc  *queryDesc;
	PgVolVecQueryState *state;
} StateEntry;

static HTAB *state_table = NULL;

void
pg_volvec_init_state_table(void)
{
	HASHCTL         ctl;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(QueryDesc *);
	ctl.entrysize = sizeof(StateEntry);
	state_table = hash_create("pg_volvec query state table",
							  128,
							  &ctl,
							  HASH_ELEM | HASH_BLOBS);
}

void
pg_volvec_register_state(QueryDesc *queryDesc, PgVolVecQueryState *state)
{
	StateEntry *entry;
	bool            found;

	entry = (StateEntry *) hash_search(state_table, &queryDesc, HASH_ENTER, &found);
	entry->state = state;
}

PgVolVecQueryState *
pg_volvec_lookup_state(QueryDesc *queryDesc)
{
	StateEntry *entry;

	if (state_table == NULL)
		return NULL;

	entry = (StateEntry *) hash_search(state_table, &queryDesc, HASH_FIND, NULL);
	return entry ? entry->state : NULL;
}

void
pg_volvec_unregister_state(QueryDesc *queryDesc)
{
	if (state_table == NULL)
		return;

	hash_search(state_table, &queryDesc, HASH_REMOVE, NULL);
}

void
pg_volvec_close_query_state(PgVolVecQueryState *state)
{
	pg_volvec_delete_plan(state);
	if (state->context)
		MemoryContextDelete(state->context);
	pfree(state);
}

/* 
 * RELAXED VERSION: Support Agg and SeqScan.
 */
static bool
is_supported_plan(Plan *plan)
{
	if (plan == NULL)
		return false;

	if (IsA(plan, SeqScan))
		return plan->lefttree == NULL && plan->righttree == NULL;

	if (IsA(plan, Hash))
		return plan->lefttree != NULL &&
			   plan->righttree == NULL &&
			   is_supported_plan(plan->lefttree);

	if (IsA(plan, HashJoin))
		return plan->lefttree != NULL &&
			   plan->righttree != NULL &&
			   IsA(plan->righttree, Hash) &&
			   is_supported_plan(plan->lefttree) &&
			   is_supported_plan(plan->righttree);

	if (IsA(plan, Agg) || IsA(plan, Sort))
		return plan->lefttree != NULL &&
			   plan->righttree == NULL &&
			   is_supported_plan(plan->lefttree);

	return false;
}

PgVolVecQueryState *
pg_volvec_try_build_query_state(QueryDesc *queryDesc, int eflags)
{
	PgVolVecQueryState *state;
	Plan *plan = queryDesc->plannedstmt->planTree;

	if (is_supported_plan(plan))
	{
		state = (PgVolVecQueryState *) palloc0(sizeof(PgVolVecQueryState));
		state->context = AllocSetContextCreate(CurrentMemoryContext,
											   "pg_volvec query context",
											   ALLOCSET_DEFAULT_SIZES);
		return state;
	}

	return NULL;
}

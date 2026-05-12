#include "postgres.h"
#include "catalog/catalog.h"
#include "utils/hsearch.h"
#include "state.h"
#include "execute.h"
#include "optimizer_registry.h"
#include "nodes/nodeFuncs.h"
#include "nodes/plannodes.h"
#include "parser/parsetree.h"
#include "utils/dsa.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"

struct dsm_segment;

extern bool pg_yaap_trace_hooks;

/* Layout MUST match query_state.hpp PgYaapQueryState (C++ mirror). */
struct PgYaapQueryState
{
	MemoryContext context;
	void *parallel_plan;
	void *parallel_scheduler;
	struct dsm_segment *runtime_dsm;
	dsa_area *runtime_dsa;
};

typedef struct StateEntry
{
	QueryDesc  *queryDesc;
	PgYaapQueryState *state;
} StateEntry;

static HTAB *state_table = NULL;

void
pg_yaap_init_state_table(void)
{
	HASHCTL         ctl;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(QueryDesc *);
	ctl.entrysize = sizeof(StateEntry);
	state_table = hash_create("pg_yaap query state table",
							  128,
							  &ctl,
							  HASH_ELEM | HASH_BLOBS);
}

void
pg_yaap_register_state(QueryDesc *queryDesc, PgYaapQueryState *state)
{
	StateEntry *entry;
	bool            found;

	entry = (StateEntry *) hash_search(state_table, &queryDesc, HASH_ENTER, &found);
	entry->state = state;
}

PgYaapQueryState *
pg_yaap_lookup_state(QueryDesc *queryDesc)
{
	StateEntry *entry;

	if (state_table == NULL)
		return NULL;

	entry = (StateEntry *) hash_search(state_table, &queryDesc, HASH_FIND, NULL);
	return entry ? entry->state : NULL;
}

void
pg_yaap_unregister_state(QueryDesc *queryDesc)
{
	if (state_table == NULL)
		return;

	hash_search(state_table, &queryDesc, HASH_REMOVE, NULL);
}

void
pg_yaap_close_query_state(PgYaapQueryState *state)
{
	pg_yaap_delete_plan(state);
	if (state->context)
		MemoryContextDelete(state->context);
	pfree(state);
}

/* 
 * RELAXED VERSION: Support Agg and SeqScan.
 */
static bool
plan_uses_supported_relations(Plan *plan, PlannedStmt *plannedstmt)
{
	if (plan == NULL)
		return false;

	if (IsA(plan, SeqScan))
	{
		SeqScan *scan = (SeqScan *) plan;
		RangeTblEntry *rte;

		if (plannedstmt == NULL ||
			scan->scan.scanrelid <= 0 ||
			scan->scan.scanrelid > list_length(plannedstmt->rtable))
			return false;
		rte = rt_fetch(scan->scan.scanrelid, plannedstmt->rtable);
		if (rte == NULL || rte->rtekind != RTE_RELATION)
			return false;
		if (rte->relid == InvalidOid ||
			IsCatalogRelationOid(rte->relid) ||
			IsCatalogNamespace(get_rel_namespace(rte->relid)))
			return false;
		return true;
	}

	if (IsA(plan, SubqueryScan))
		return ((SubqueryScan *) plan)->subplan != NULL &&
			   plan_uses_supported_relations(((SubqueryScan *) plan)->subplan, plannedstmt);

	return plan_uses_supported_relations(plan->lefttree, plannedstmt) &&
		   (plan->righttree == NULL || plan_uses_supported_relations(plan->righttree, plannedstmt));
}

PgYaapQueryState *
pg_yaap_try_build_query_state(QueryDesc *queryDesc, int eflags)
{
	PgYaapQueryState *state;
	Plan *plan = queryDesc->plannedstmt->planTree;
	void *optimizer_bundle = pg_yaap_lookup_optimizer_plan(queryDesc->plannedstmt);
	(void) eflags;

	/*
	 * Admission here is intentionally broad: PostgreSQL owns planning, and
	 * pg_yaap decides exact shape support later during VecPlan / parallel
	 * lowering. At this stage we only fence out plans that never touch
	 * supported user relations.
	 */
	if (optimizer_bundle != NULL)
	{
		state = (PgYaapQueryState *) palloc0(sizeof(PgYaapQueryState));
		state->context = AllocSetContextCreate(CurrentMemoryContext,
											   "pg_yaap query context",
											   ALLOCSET_DEFAULT_SIZES);
		if (pg_yaap_trace_hooks)
			elog(LOG,
				 "pg_yaap: query state admitted via optimizer bundle (context=%p)",
				 (void *) state->context);
		return state;
	}

	if (plan == NULL)
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: query state admission rejected because root plan is NULL");
		return NULL;
	}
	if (!plan_uses_supported_relations(plan, queryDesc->plannedstmt))
	{
		if (pg_yaap_trace_hooks)
			elog(LOG,
				 "pg_yaap: query state admission rejected because plan does not touch supported user relations (nodeTag=%d)",
				 (int) nodeTag(plan));
		return NULL;
	}

	state = (PgYaapQueryState *) palloc0(sizeof(PgYaapQueryState));
	state->context = AllocSetContextCreate(CurrentMemoryContext,
										   "pg_yaap query context",
										   ALLOCSET_DEFAULT_SIZES);
	if (pg_yaap_trace_hooks)
		elog(LOG,
			 "pg_yaap: query state admitted (nodeTag=%d context=%p)",
			 (int) nodeTag(plan),
			 (void *) state->context);
	return state;
}

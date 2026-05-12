#include "postgres.h"

#include "access/parallel.h"
#include "executor/executor.h"
#include "storage/dsm_registry.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
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
bool pg_volvec_trace_execution_path = false;
bool pg_volvec_jit_deform = true;
bool pg_volvec_parallel = false;
int pg_volvec_parallel_max_workers = 4;
int pg_volvec_parallel_min_relation_blocks = 1024;
bool pg_volvec_parallel_leader_participation = true;
bool pg_volvec_parallel_experimental_hash_pipeline = false;
bool pg_volvec_disable_jit_for_parallel_worker = false;
bool pg_volvec_profile = false;

static void pg_volvec_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void pg_volvec_ExecutorRun(QueryDesc *queryDesc,
							   ScanDirection direction,
							   uint64 count);
static void pg_volvec_ExecutorFinish(QueryDesc *queryDesc);
static void pg_volvec_ExecutorEnd(QueryDesc *queryDesc);

extern int pg_volvec_runtime_dsa_tranche_id(void);
extern void pg_volvec_proc_exit_release_jit_contexts(int code, Datum arg);

void            _PG_init(void);
void            _PG_fini(void);

static const char *
pg_volvec_plan_node_name(Plan *plan)
{
	if (plan == NULL)
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
	return "Other";
}

/*
 * Bug M (2026-04-30): runtime DSA tranche id must be allocated ONCE per
 * cluster, not per query. Per-query LWLockNewTrancheId leaks into the
 * 256-tranche-per-backend hard cap and kills sessions after ~50 parallel
 * queries. Using GetNamedDSMSegment guarantees a single allocation across
 * all backends without requiring shared_preload_libraries semantics —
 * matches the canonical pattern in src/test/modules/test_dsa.
 */
static void
pg_volvec_init_runtime_dsa_tranche(void *ptr, void *arg)
{
	int *tranche_id = (int *) ptr;
	*tranche_id = LWLockNewTrancheId("pg_volvec_runtime_dsa");
}

int
pg_volvec_runtime_dsa_tranche_id(void)
{
	bool		found;
	int		   *tranche_id;

	tranche_id = (int *) GetNamedDSMSegment("pg_volvec_runtime_dsa_tranche_id",
											sizeof(int),
											pg_volvec_init_runtime_dsa_tranche,
											&found,
											NULL);
	return *tranche_id;
}

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

	DefineCustomBoolVariable("pg_volvec.trace_execution_path",
							 "Emit one low-noise execution path log line per query.",
							 NULL,
							 &pg_volvec_trace_execution_path,
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

	DefineCustomBoolVariable("pg_volvec.parallel",
							 "Enable experimental block-pool parallel lowering inside pg_volvec.",
							 NULL,
							 &pg_volvec_parallel,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable("pg_volvec.parallel_max_workers",
							"Maximum number of experimental pg_volvec parallel workers.",
							NULL,
							&pg_volvec_parallel_max_workers,
							4,
							0,
							1024,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("pg_volvec.parallel_min_relation_blocks",
							"Minimum relation size in blocks before experimental pg_volvec parallel lowering is considered.",
							NULL,
							&pg_volvec_parallel_min_relation_blocks,
							1024,
							0,
							INT_MAX,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable("pg_volvec.parallel_leader_participation",
							 "Allow the leader to participate in experimental pg_volvec block-pool execution.",
							 NULL,
							 &pg_volvec_parallel_leader_participation,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_volvec.parallel_experimental_hash_pipeline",
							 "Enable the experimental leader-only HashBuild->Finalize->Probe pipeline DAG path.",
							 NULL,
							 &pg_volvec_parallel_experimental_hash_pipeline,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_volvec.profile",
							 "Emit pg_volvec per-stage pipeline timing after each offloaded query.",
							 NULL,
							 &pg_volvec_profile,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	pg_volvec_init_state_table();

	before_shmem_exit(pg_volvec_proc_exit_release_jit_contexts, (Datum) 0);

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
	Plan *plan = queryDesc != NULL && queryDesc->plannedstmt != NULL ?
		queryDesc->plannedstmt->planTree : NULL;

	if (prev_ExecutorStart)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);

	if (!pg_volvec_enabled)
		return;

	if (pg_volvec_trace_hooks)
		elog(LOG,
			 "pg_volvec: ExecutorStart pid=%d parallel_worker=%s parallelModeNeeded=%s root=%s nodeTag=%d",
			 MyProcPid,
			 IsParallelWorker() ? "on" : "off",
			 (queryDesc != NULL && queryDesc->plannedstmt != NULL &&
			  queryDesc->plannedstmt->parallelModeNeeded) ? "on" : "off",
			 pg_volvec_plan_node_name(plan),
			 plan != NULL ? (int) nodeTag(plan) : -1);

	if (IsParallelWorker())
	{
		if (pg_volvec_trace_hooks)
			elog(LOG,
				 "pg_volvec: skipping hook state build inside PostgreSQL parallel worker pid=%d",
				 MyProcPid);
		return;
	}

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
	else if (pg_volvec_trace_hooks)
		elog(LOG, "pg_volvec: query state admission rejected in leader pid=%d", MyProcPid);
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
		/* pg_volvec_execute_query returned false - diagnose why */
		if (pg_volvec_trace_execution_path)
			elog(LOG,
				 "pg_volvec_path: path=native_pg reason=pg_volvec_execute_query_returned_false");
		if (pg_volvec_trace_hooks)
			elog(LOG, "pg_volvec: ExecutorRun fallback to native PG executor");
	}
	else if (pg_volvec_trace_execution_path)
		elog(LOG,
			 "pg_volvec_path: path=native_pg reason=no_registered_pg_volvec_state");

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

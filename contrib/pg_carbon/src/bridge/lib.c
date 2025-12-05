#include "postgres.h"
struct _dummy;
#include "../optimizer/optimizer.h"
#include "fmgr.h"
#include "optimizer/planner.h"

// We need to declare the C++ entry point as extern "C" in the C++ file,
// or use a wrapper. Since we can't include C++ headers here directly if they
// use C++ features, we'll declare the function signature.

// However, Optimizer::Optimize is a static C++ method.
// We need a C-compatible wrapper function.
// Let's assume we add a C wrapper in optimizer.cpp or a separate file.
// For simplicity, let's declare a C function that we will implement in
// optimizer.cpp inside extern "C".

#ifdef __cplusplus
extern "C" {
#endif

PG_MODULE_MAGIC;

void _PG_init(void);

static planner_hook_type prev_planner_hook = NULL;

// Forward declaration of the C++ wrapper function
Plan *pg_carbon_optimize_query(Query *parse, int cursorOptions,
                               ParamListInfo boundParams);

static PlannedStmt *pg_carbon_planner(Query *parse, const char *query_string,
                                      int cursorOptions,
                                      ParamListInfo boundParams) {
  elog(WARNING, "Hello World from pg_carbon planner!");

  // Call our C++ optimizer
  Plan *plan = pg_carbon_optimize_query(parse, cursorOptions, boundParams);
  elog(WARNING, "pg carbon optimized done!");

  if (plan) {
    // Wrap Plan in PlannedStmt
    PlannedStmt *result = makeNode(PlannedStmt);
    result->commandType = parse->commandType;
    result->queryId = parse->queryId;
    result->hasReturning = parse->returningList != NIL;
    result->hasModifyingCTE = parse->hasModifyingCTE;
    result->canSetTag = true;
    result->transientPlan = false;
    result->dependsOnRole = false;
    result->parallelModeNeeded = false;
    result->planTree = plan;
    result->rtable = parse->rtable;
    result->resultRelations = NIL; // Simplified
    result->subplans = NIL;

    // Populate relationOids and unprunableRelids
    ListCell *lc;
    List *relationOids = NIL;
    Bitmapset *unprunableRelids = NULL;
    int rti = 1;
    foreach (lc, parse->rtable) {
      RangeTblEntry *rte = (RangeTblEntry *)lfirst(lc);
      if (rte->rtekind == RTE_RELATION) {
        relationOids = lappend_oid(relationOids, rte->relid);
        unprunableRelids = bms_add_member(unprunableRelids, rti);
      }
      rti++;
    }
    result->relationOids = relationOids;
    result->unprunableRelids = unprunableRelids;

    return result;
  }
  elog(WARNING,
       "pg carbon generate plan failed, fallback to standard planner!");

  // Fallback to standard planner if Carbon fails or returns null
  if (prev_planner_hook)
    return prev_planner_hook(parse, query_string, cursorOptions, boundParams);
  else
    return standard_planner(parse, query_string, cursorOptions, boundParams);
}

void _PG_init(void) {
  elog(WARNING, "pg_carbon loaded!");
  prev_planner_hook = planner_hook;
  planner_hook = pg_carbon_planner;
}

#ifdef __cplusplus
}
#endif

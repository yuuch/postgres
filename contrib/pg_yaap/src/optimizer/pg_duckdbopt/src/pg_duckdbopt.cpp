extern "C" {
#include "postgres.h"
#include "fmgr.h"
#include "optimizer/planner.h"
#include "utils/elog.h"
}

#include <exception>
#include <stdexcept>

#include "adapter/duckdb_adapter.hpp"
#include "planner/logical_planner.hpp"
#include "optimizer/optimizer_core.hpp"
#include "physical/physical_planner.hpp"
#include "executor/duckdb_scan.hpp"

extern "C" {
PG_MODULE_MAGIC;

void _PG_init(void);
void _PG_fini(void);
}

static planner_hook_type prev_planner_hook = nullptr;

/*
 * duckdbopt_planner_hook
 * Intercepts query planning. Uses our custom C++ Optimizer Core 
 * (inspired by DuckDB) and falls back on exception.
 */
static PlannedStmt* duckdbopt_planner_hook(Query *parse, const char *query_string,
                                           int cursorOptions, ParamListInfo boundParams)
{
    PlannedStmt* result = nullptr;

    /*
     * Boundary: Prevent C++ exceptions from unwinding into C code,
     * which would lead to crashes or leaks.
     */
    try {
        elog(DEBUG1, "pg_duckdbopt: Intercepted query via planner hook.");
        
        // 1. Planner stage: build and normalize the logical plan
        duckdbopt::LogicalPlanner logical_planner;
        auto planned_tree = logical_planner.Plan(parse);
        
        if (!planned_tree) {
            throw std::runtime_error("Translation rejected: Query contains unsupported features.");
        }

        // 2. Optimizer stage: run logical rewrites and costing passes
        duckdbopt::LogicalOptimizer logical_optimizer;
        auto optimized_tree = logical_optimizer.Optimize(std::move(planned_tree));

        if (!optimized_tree) {
            throw std::runtime_error("Optimization failed.");
        }

        // 3. Physical planner stage: choose physical operators for the logical tree
        duckdbopt::PhysicalPlanner physical_planner;
        auto physical_plan = physical_planner.CreatePlan(*optimized_tree);

        if (!physical_plan) {
            throw std::runtime_error("Physical planning failed.");
        }

        // 4. Package result into CustomScan (PlannedStmt)
        duckdbopt::DuckDBExecutorScan executor_scan;
        result = executor_scan.CreateCustomScanPlan(parse, optimized_tree.get(), physical_plan.get());

        if (!result) {
            throw std::runtime_error("Reification failed to create PlannedStmt.");
        }

        return result;

    } catch (const std::exception& e) {
        elog(WARNING, "pg_duckdbopt planning fallback: %s. Reverting to PG native planner.", e.what());
        
        // Safe fallback to native PostgreSQL planner
        if (prev_planner_hook) {
            result = prev_planner_hook(parse, query_string, cursorOptions, boundParams);
        } else {
            result = standard_planner(parse, query_string, cursorOptions, boundParams);
        }
    } catch (...) {
        elog(WARNING, "pg_duckdbopt planning fallback due to unknown C++ exception. Reverting to PG native planner.");
        if (prev_planner_hook) {
            result = prev_planner_hook(parse, query_string, cursorOptions, boundParams);
        } else {
            result = standard_planner(parse, query_string, cursorOptions, boundParams);
        }
    }

    return result;
}

void _PG_init(void)
{
    elog(LOG, "pg_duckdbopt extension loaded.");
    duckdbopt::DuckDBExecutorScan::RegisterScanMethods();
    prev_planner_hook = planner_hook;
    planner_hook = duckdbopt_planner_hook;
}

void _PG_fini(void)
{
    planner_hook = prev_planner_hook;
    elog(LOG, "pg_duckdbopt extension unloaded.");
}

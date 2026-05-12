#include "logical_planner.hpp"

#include "../adapter/duckdb_adapter.hpp"
#include "planner_normalizer.hpp"

namespace duckdbopt {

std::unique_ptr<LogicalOperator> LogicalPlanner::CreateInitialPlan(::Query* pg_query) {
    DuckDBAdapter adapter;
    return adapter.TranslatePGQuery(pg_query);
}

std::unique_ptr<LogicalOperator> LogicalPlanner::Normalize(std::unique_ptr<LogicalOperator> plan) {
    if (!plan) {
        return nullptr;
    }

    PlannerNormalizer normalizer;
    return normalizer.Normalize(std::move(plan));
}

std::unique_ptr<LogicalOperator> LogicalPlanner::Plan(::Query* pg_query) {
    return Normalize(CreateInitialPlan(pg_query));
}

} // namespace duckdbopt

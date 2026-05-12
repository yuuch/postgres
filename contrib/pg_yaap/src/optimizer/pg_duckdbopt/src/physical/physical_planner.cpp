#include "physical_planner.hpp"

#include "physical_plan_generator.hpp"

namespace duckdbopt {

std::unique_ptr<PhysicalOperator> PhysicalPlanner::CreatePlan(LogicalOperator& op) {
    PhysicalPlanGenerator generator;
    return generator.Plan(op);
}

} // namespace duckdbopt

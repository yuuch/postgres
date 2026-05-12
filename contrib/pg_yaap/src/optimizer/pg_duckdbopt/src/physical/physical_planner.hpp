#pragma once

#include "physical_plan.hpp"

namespace duckdbopt {

class LogicalOperator;

class PhysicalPlanner {
public:
    std::unique_ptr<PhysicalOperator> CreatePlan(LogicalOperator& op);
};

} // namespace duckdbopt

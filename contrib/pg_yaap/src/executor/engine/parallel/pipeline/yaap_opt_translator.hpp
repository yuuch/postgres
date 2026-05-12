#pragma once

#include <memory>
#include <string>

extern "C" {
#include "executor/execdesc.h"
}

#include "optimizer_registry.hpp"
#include "parallel/pipeline/physical_operator.hpp"
#include "parallel/pipeline/query_state.hpp"

namespace pg_yaap {

struct OptimizerPlanSupportStatus {
	bool supported = false;
	std::string path;
	std::string detail;
};

std::unique_ptr<pg_yaap::pipeline::PhysicalOperator>
TranslateOptimizerPlan(QueryDesc *queryDesc,
					   pg_yaap::PgYaapQueryState *state,
					   const OptimizerPlanBundle &bundle);

OptimizerPlanSupportStatus
AnalyzeOptimizerPlanSupport(const OptimizerPlanBundle &bundle);

std::string
DescribeOptimizerPlan(const OptimizerPlanBundle &bundle);

bool CanExecuteOptimizerPlanSerial(const OptimizerPlanBundle &bundle);
bool ExecuteOptimizerPlanSerial(QueryDesc *queryDesc,
								const OptimizerPlanBundle &bundle,
								const char **failure_reason);

TupleDesc
BuildOptimizerOutputTupleDesc(const OptimizerPlanBundle &bundle);

}  // namespace pg_yaap

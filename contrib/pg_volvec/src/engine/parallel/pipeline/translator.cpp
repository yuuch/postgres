#include "parallel/pipeline/translator.hpp"

extern "C" {
#include "postgres.h"
#include "nodes/nodes.h"
#include "nodes/plannodes.h"
}

namespace pg_volvec {
namespace pipeline {

std::unique_ptr<PhysicalOperator>
Translator::TranslatePlan(Plan *plan, PlannedStmt *stmt)
{
	if (plan == nullptr)
		return nullptr;

	switch (nodeTag(plan))
	{
		default:
			return nullptr;
	}
}

std::unique_ptr<PhysicalOperator>
Translator::Translate(PlannedStmt *stmt)
{
	if (stmt == nullptr || stmt->planTree == nullptr)
		return nullptr;

	return TranslatePlan(stmt->planTree, stmt);
}

}  /* namespace pipeline */
}  /* namespace pg_volvec */

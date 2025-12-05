#include "rules.h"
#include "../operators/operators.h"

namespace pg_carbon {

bool RuleGetToScan::Matches(GroupExpression *expr) const {
  return expr->GetOperator()->IsLogical() &&
         dynamic_cast<LogicalGet *>(expr->GetOperator()) != nullptr;
}

PgVector<GroupExpression *>
RuleGetToScan::Transform(GroupExpression *expr) const {
  auto logical_get = dynamic_cast<LogicalGet *>(expr->GetOperator());
  if (!logical_get) {
    return {};
  }

  auto physical_scan = new PhysicalTableScan(logical_get->GetTableOid(),
                                             logical_get->GetRtIndex());
  auto new_expr = new GroupExpression(physical_scan, PgVector<Group *>{});

  return {new_expr};
}

} // namespace pg_carbon

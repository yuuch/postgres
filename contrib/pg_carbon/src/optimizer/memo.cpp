#include "memo.h"

namespace pg_carbon {

void Group::AddExpression(GroupExpression *expr) {
  expr->SetGroup(this);
  if (expr->GetOperator()->IsLogical()) {
    logical_exprs_.push_back(expr);
  } else {
    physical_exprs_.push_back(expr);
  }
}

Group *Memo::InsertExpression(GroupExpression *expr) {
  // In a real implementation, check if expr already exists in the memo.
  // For this skeleton, we assume we are building the initial tree or adding new
  // expressions.

  // If the expression doesn't have a group, create one.
  // Note: This is a simplification. Usually, we look up if the expression
  // exists. If we are inserting a new expression into an existing group, the
  // caller handles that. Here we assume we are creating a new group for a new
  // logical expression.

  Group *group = NewGroup();
  group->AddExpression(expr);
  return group;
}

Group *Memo::InitMemo(Operator *root_op) {
  if (!root_op)
    return nullptr;

  PgVector<Group *> child_groups;
  for (auto *input : root_op->GetInputs()) {
    child_groups.push_back(InitMemo(input));
  }

  auto expr = new GroupExpression(root_op, child_groups);
  return InsertExpression(expr);
}

Group *Memo::NewGroup() {
  Group *group = new Group();
  groups_.push_back(group);
  return group;
}

} // namespace pg_carbon

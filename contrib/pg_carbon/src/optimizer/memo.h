#ifndef PG_CARBON_MEMO_H
#define PG_CARBON_MEMO_H

#include "../common/memory.h"
#include "../operators/operators.h"

namespace pg_carbon {

class Group;
class Memo;

class GroupExpression : public PgObject {
public:
  GroupExpression(Operator *op, PgVector<Group *> children)
      : op_(op), children_(children), group_(nullptr) {}

  void SetGroup(Group *group) { group_ = group; }
  Group *GetGroup() const { return group_; }
  Operator *GetOperator() const { return op_; }
  const PgVector<Group *> &GetChildren() const { return children_; }

private:
  Operator *op_;
  PgVector<Group *> children_;
  Group *group_; // Back pointer to the group this expression belongs to
};

class Group : public PgObject {
public:
  void AddExpression(GroupExpression *expr);
  const PgVector<GroupExpression *> &GetLogicalExpressions() const {
    return logical_exprs_;
  }
  const PgVector<GroupExpression *> &GetPhysicalExpressions() const {
    return physical_exprs_;
  }

  void SetExplored(bool explored) { explored_ = explored; }
  bool IsExplored() const { return explored_; }

  void SetImplemented(bool implemented) { implemented_ = implemented; }
  bool IsImplemented() const { return implemented_; }

  // For simplicity in this skeleton, we just store the best plan directly.
  // In a real optimizer, this would be a map of RequiredProperties -> Best
  // Plan.
  void SetBestExpression(GroupExpression *expr) { best_expression_ = expr; }
  GroupExpression *GetBestExpression() const { return best_expression_; }

private:
  PgVector<GroupExpression *> logical_exprs_;
  PgVector<GroupExpression *> physical_exprs_;
  bool explored_ = false;
  bool implemented_ = false;
  GroupExpression *best_expression_ = nullptr;
};

class Memo : public PgObject {
public:
  Group *InsertExpression(GroupExpression *expr);
  Group *InitMemo(Operator *root_op);
  Group *NewGroup();
  const PgVector<Group *> &GetGroups() const { return groups_; }

private:
  PgVector<Group *> groups_;
  // In a real implementation, we would have a hash map to detect duplicate
  // expressions.
};

} // namespace pg_carbon

#endif // PG_CARBON_MEMO_H

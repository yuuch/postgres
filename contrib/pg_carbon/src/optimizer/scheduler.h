#ifndef PG_CARBON_SCHEDULER_H
#define PG_CARBON_SCHEDULER_H

#include "../common/memory.h"
#include "../rules/rules.h"
#include "memo.h"
#include <stack>

namespace pg_carbon {

class TaskScheduler;

// Context for optimization (e.g., cost limits, required properties)
class Context : public PgObject {
public:
  // For now, empty or basic. In real Columbia, holds cost definitions, etc.
};

// ID Types
using GroupID = Group *;
using MExprID = GroupExpression *;
using RuleID = Rule *;

class Task : public PgObject {
public:
  virtual ~Task() = default;
  // perform returns void in this design, logic is inside.
  virtual void perform(TaskScheduler *scheduler) = 0;
};

class TaskScheduler : public PgObject {
public:
  void ScheduleTask(Task *task);
  void Run();

  // Helper to get available rules, effectively part of the scheduler/optimizer
  // context
  const PgVector<Rule *> &GetRules() const;

private:
  PgStack<Task *> task_stack_;
  PgVector<Rule *> rules_; // Simplification: Rules stored here
};

// 1. O_Group (Optimize Group)
class O_Group : public Task {
public:
  O_Group(GroupID group, Context *context) : group_(group), context_(context) {}
  void perform(TaskScheduler *scheduler) override;

private:
  GroupID group_;
  Context *context_;
};

// 2. E_Group (Explore Group)
class E_Group : public Task {
public:
  E_Group(GroupID group, Context *context) : group_(group), context_(context) {}
  void perform(TaskScheduler *scheduler) override;

private:
  GroupID group_;
  Context *context_;
};

// 3. O_Expr (Optimize Expression)
class O_Expr : public Task {
public:
  O_Expr(MExprID expr, Context *context, bool exploring)
      : expr_(expr), context_(context), exploring_(exploring) {}
  void perform(TaskScheduler *scheduler) override;

private:
  MExprID expr_;
  Context *context_;
  bool exploring_;
};

// 4. Apply_Rule
class Apply_Rule : public Task {
public:
  Apply_Rule(RuleID rule, MExprID expr, Context *context, bool exploring)
      : rule_(rule), expr_(expr), context_(context), exploring_(exploring) {}
  void perform(TaskScheduler *scheduler) override;

private:
  RuleID rule_;
  MExprID expr_;
  Context *context_;
  bool exploring_;
};

// 5. O_Inputs
class O_Inputs : public Task {
public:
  O_Inputs(MExprID expr, Context *context) : expr_(expr), context_(context) {}
  void perform(TaskScheduler *scheduler) override;

private:
  MExprID expr_;
  Context *context_;
  int current_input_index_ = 0;
};

} // namespace pg_carbon

#endif // PG_CARBON_SCHEDULER_H

#include "scheduler.h"
#include <iostream>

namespace pg_carbon {

class RuleGetToScan; // Forward decl

void TaskScheduler::ScheduleTask(Task *task) { task_stack_.push(task); }

void TaskScheduler::Run() {
  // Initialize rules (Simplification: just add one rule)
  if (rules_.empty()) {
    rules_.push_back(new RuleGetToScan());
  }

  while (!task_stack_.empty()) {
    Task *task = task_stack_.top();
    task_stack_.pop();
    task->perform(this);
    delete task;
  }
}

const PgVector<Rule *> &TaskScheduler::GetRules() const { return rules_; }

// 1. O_Group (Optimize Group)
// Logic: Traverse Group Exprs, schedule O_Expr or O_Inputs.
void O_Group::perform(TaskScheduler *scheduler) {
  // In a real implementation:
  // 1. Check if group is already optimized for the context. If so, return.
  // 2. Mark group as being optimized.

  // For this simplified version, we just iterate over all logical expressions
  // and optimize them. Lower bounds / cost pruning would happen here.

  const auto &logical_exprs = group_->GetLogicalExpressions();
  // Reverse iteration to push tasks in correct order (stack)
  for (int i = logical_exprs.size() - 1; i >= 0; --i) {
    auto *expr = logical_exprs[i];
    // Schedule O_Expr for each logical expression
    scheduler->ScheduleTask(new O_Expr(expr, context_, false));
  }
}

// 2. E_Group (Explore Group)
// Logic: Schedule O_Expr (exploring=true).
void E_Group::perform(TaskScheduler *scheduler) {
  // Check if group is already explored.
  if (group_->IsExplored()) {
    return;
  }
  group_->SetExplored(true);

  const auto &logical_exprs = group_->GetLogicalExpressions();
  for (int i = logical_exprs.size() - 1; i >= 0; --i) {
    auto *expr = logical_exprs[i];
    // Schedule O_Expr with exploring=true
    scheduler->ScheduleTask(new O_Expr(expr, context_, true));
  }
}

// 3. O_Expr (Optimize Expression)
// Logic: Match rules, schedule Apply_Rule and E_Group.
void O_Expr::perform(TaskScheduler *scheduler) {
  // 1. Apply Transformation Rules
  // Iterate over all available rules.
  // In a real system, we'd look up rules relevant to the operator.
  const auto &rules = scheduler->GetRules();
  for (int i = rules.size() - 1; i >= 0; --i) {
    auto *rule = rules[i];
    if (rule->Matches(expr_)) {
      // Schedule Apply_Rule
      scheduler->ScheduleTask(
          new Apply_Rule(rule, expr_, context_, exploring_));
    }
  }

  // 2. If we are NOT merely exploring, we also need to optimize the inputs of
  // this expression. This usually happens after exploration (rules
  // application). However, in Columbia, O_Expr typically triggers exploration
  // of children groups first if needed. And if it's a physical operator
  // (implementation), we optimize inputs.

  if (!exploring_) {
    // If this is a physical operator, we need to optimize its inputs to cost
    // it.
    if (expr_->GetOperator()->IsPhysical()) {
      scheduler->ScheduleTask(new O_Inputs(expr_, context_));
    }
  }

  // 3. Ensure children groups are explored (if this is a logical operator or we
  // need to match patterns) For simplicity, we assume we explore all input
  const auto &children = expr_->GetChildren();
  for (int i = children.size() - 1; i >= 0; --i) {
    scheduler->ScheduleTask(new E_Group(children[i], context_));
  }
}

// 4. Apply_Rule
// Logic: Generate new Expr, CopyIn to Memo, according to result type schedule
// O_Expr or O_Inputs.
void Apply_Rule::perform(TaskScheduler *scheduler) {
  // 1. Transform: Generate new expressions (binding generation)
  auto new_exprs = rule_->Transform(expr_);

  // 2. Insert into Memo
  auto *group =
      expr_->GetGroup(); // Currently we stay in the same group (Transformation)
  // Note: logical rules allow staying in the same group. Implementation rules
  // might target the same group too.

  // Reverse order for stack
  for (int i = new_exprs.size() - 1; i >= 0; --i) {
    auto *new_expr = new_exprs[i];
    // CopyIn logic is essentially asking the Memo to insert it.
    // In our simplified memo, InsertExpression takes care of adding to group.
    new_expr->SetGroup(group);
    group->AddExpression(new_expr);

    // 3. Schedule further work
    if (new_expr->GetOperator()->IsLogical()) {
      // If result is logical, we might need to explore/optimize it further
      // Usually, we schedule O_Expr on the new expression if we are exploring,
      // or if we are optimizing and want to consider this new logical path.
      scheduler->ScheduleTask(new O_Expr(new_expr, context_, exploring_));
    } else {
      // If result is physical (Implementation Rule), we need to optimize its
      // inputs
      if (!exploring_) {
        scheduler->ScheduleTask(new O_Inputs(new_expr, context_));
      }

      // Also update best plan if applicable (omitted cost model for now)
      // In this skeleton, we just pick the last physical one as best.
      group->SetBestExpression(new_expr);
    }
  }
}

// 5. O_Inputs
// Logic: State machine, loop inputs. Push self(i+1), push input(i) O_Group.
void O_Inputs::perform(TaskScheduler *scheduler) {
  const auto &children = expr_->GetChildren();

  // If we have processed all inputs
  if (current_input_index_ >= children.size()) {
    // All inputs optimized. We can now cost this expression (Physical).
    // (Costing logic omitted)
    return;
  }

  // We have more inputs to process.

  // 1. Push self back onto stack with incremented index
  // We need to clone `this` task or modify it. Since tasks are deleted after
  // perform, we must create a new task or rely on the scheduler design.
  // Standard Columbia "O_Inputs" is a single task that re-pushes itself.
  auto *next_step = new O_Inputs(expr_, context_);
  next_step->current_input_index_ = current_input_index_ + 1;
  scheduler->ScheduleTask(next_step);

  // 2. Push optimization task for the current input group
  auto *child_group = children[current_input_index_];
  scheduler->ScheduleTask(new O_Group(child_group, context_));
}

} // namespace pg_carbon

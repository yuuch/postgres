#include "optimizer.h"
#include "memo.h"
#include "scheduler.h"
#include "translator.h"

namespace pg_carbon {

Plan *Optimizer::Optimize(Query *pg_query) {
  // 1. Translate PG Query -> Carbon Operator Tree
  Translator translator;
  Operator *root_op = translator.TranslateQueryToCarbon(pg_query);

  if (!root_op) {
    // Fallback or error
    return nullptr;
  }

  // 2. Initialize Memo with the operator tree
  Group *root_group = memo_.InitMemo(root_op);

  // 3. Initialize Scheduler
  TaskScheduler scheduler;

  // 4. Schedule optimization of the root group
  // In a real system, we would pass required properties (e.g., sort order).
  scheduler.ScheduleTask(new O_Group(root_group, nullptr));

  // 5. (Removed step from previous impl)

  // 5. Run Scheduler
  scheduler.Run();

  // 6. Extract best plan
  // In a real system, we extract based on required properties.
  // Here we just take the best expression stored in the group.
  auto best_expr = root_group->GetBestExpression();

  // 7. Translate Carbon Plan -> PG Plan
  return translator.TranslatePlanToPG(best_expr, pg_query);
}

} // namespace pg_carbon

extern "C" {
Plan *pg_carbon_optimize_query(Query *parse, int cursorOptions,
                               ParamListInfo boundParams) {
  // We ignore cursorOptions and boundParams for this skeleton
  pg_carbon::Optimizer optimizer;
  return optimizer.Optimize(parse);
}
}

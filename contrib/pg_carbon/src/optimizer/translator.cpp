#include "translator.h"
#include "../operators/operators.h"
#include <iostream>

extern "C" {
#include "nodes/makefuncs.h"
}

namespace pg_carbon {

Operator *Translator::TranslateQueryToCarbon(Query *pg_query) {
  // Mock implementation.
  // In a real system, we would traverse the Query tree recursively.
  // For now, we assume a simple SELECT * FROM t1;

  // Check if it's a simple relation scan
  if (pg_query->rtable && pg_query->jointree && pg_query->jointree->fromlist) {
    List *fromlist = pg_query->jointree->fromlist;
    if (list_length(fromlist) == 1) {
      Node *node = (Node *)linitial(fromlist);
      if (IsA(node, RangeTblRef)) {
        RangeTblRef *rtr = (RangeTblRef *)node;
        elog(WARNING, "Translator: Found RangeTblRef with rtindex %d",
             rtr->rtindex);
        elog(WARNING, "Translator: RTable length is %d",
             list_length(pg_query->rtable));
        RangeTblEntry *rte =
            (RangeTblEntry *)list_nth(pg_query->rtable, rtr->rtindex - 1);

        if (rte->rtekind == RTE_RELATION) {
          auto op = new LogicalGet(rte->relid, rtr->rtindex);
          return op;
        }
      }
    }
  }

  return nullptr;
}

Plan *Translator::TranslatePlanToPG(GroupExpression *best_physical_plan,
                                    Query *pg_query) {
  if (!best_physical_plan)
    return nullptr;

  auto op = best_physical_plan->GetOperator();
  if (op->IsPhysical()) {
    if (auto scan = dynamic_cast<PhysicalTableScan *>(op)) {
      // Create a SeqScan node
      // Note: This is a very simplified construction.
      // Real PG nodes need target lists, qual lists, etc.
      SeqScan *node = makeNode(SeqScan);
      node->scan.scanrelid = scan->GetRtIndex();
      node->scan.plan.targetlist = (List *)copyObjectImpl(pg_query->targetList);
      elog(WARNING, "Translator: Created SeqScan with scanrelid %d",
           node->scan.scanrelid);

      return (Plan *)node;
    }
  }

  return nullptr;
}

} // namespace pg_carbon

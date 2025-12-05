#ifndef PG_CARBON_TRANSLATOR_H
#define PG_CARBON_TRANSLATOR_H

#include "../common/memory.h"
#include "memo.h"

extern "C" {
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "postgres.h"
struct _dummy;
}

namespace pg_carbon {

class Translator : public PgObject {
public:
  Translator() = default;

  // Ingest: PG Query -> Carbon Operator Tree (Root)
  Operator *TranslateQueryToCarbon(Query *pg_query);

  // Egest: Carbon Best Physical Plan -> PG Plan
  Plan *TranslatePlanToPG(GroupExpression *best_physical_plan, Query *pg_query);
};

} // namespace pg_carbon

#endif // PG_CARBON_TRANSLATOR_H

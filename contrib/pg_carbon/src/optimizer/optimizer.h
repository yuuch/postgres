#ifndef PG_CARBON_OPTIMIZER_H
#define PG_CARBON_OPTIMIZER_H

#ifdef __cplusplus
extern "C" {
#endif
#include "postgres.h"
struct _dummy;
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#ifdef __cplusplus
}
#endif

#ifdef __cplusplus
#include "../common/memory.h"
#include "memo.h"

namespace pg_carbon {

class Optimizer : public PgObject {
public:
  Plan *Optimize(Query *pg_query);

private:
  Memo memo_;
};

} // namespace pg_carbon
#endif

#ifdef __cplusplus
extern "C" {
#endif
Plan *pg_carbon_optimize_query(Query *parse, int cursorOptions,
                               ParamListInfo boundParams);
#ifdef __cplusplus
}
#endif

#endif // PG_CARBON_OPTIMIZER_H

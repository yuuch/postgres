#ifndef PG_VEC_PG_TRANSLATE_H
#define PG_VEC_PG_TRANSLATE_H

#include "postgres.h"

#include "executor/executor.h"

#include "../ir/vec_ir.h"

bool pg_vec_try_translate_plan(QueryDesc *queryDesc,
							   int eflags,
							   PgVecPlan *plan,
							   const char **failure_reason);
const char *pg_vec_plan_kind_name(PgVecPlanKind kind);

#endif

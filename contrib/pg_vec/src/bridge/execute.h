#ifndef PG_VEC_EXECUTE_H
#define PG_VEC_EXECUTE_H

#include "postgres.h"

#include "executor/executor.h"

#include "state.h"

extern bool pg_vec_jit_deform;

bool pg_vec_execute_query(QueryDesc *queryDesc,
						  PgVecQueryState *state,
						  ScanDirection direction,
						  uint64 count);

#endif

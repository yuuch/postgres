#ifndef PG_VOLVEC_EXECUTE_H
#define PG_VOLVEC_EXECUTE_H

#include "postgres.h"
#include "executor/executor.h"
#include "state.h"

bool pg_volvec_initialize_plan(QueryDesc *queryDesc, PgVolVecQueryState *state);
void pg_volvec_delete_plan(PgVolVecQueryState *state);
bool pg_volvec_execute_query(QueryDesc *queryDesc, PgVolVecQueryState *state,
							ScanDirection direction, uint64 count);

#endif

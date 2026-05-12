#ifndef PG_VOLVEC_STATE_H
#define PG_VOLVEC_STATE_H

#include "postgres.h"
#include "executor/executor.h"

/* Opaque pointer for both C and C++ */
typedef struct PgVolVecQueryState PgVolVecQueryState;

#ifdef __cplusplus
extern "C" {
#endif

void pg_volvec_init_state_table(void);
void pg_volvec_register_state(QueryDesc *queryDesc, PgVolVecQueryState *state);
PgVolVecQueryState *pg_volvec_lookup_state(QueryDesc *queryDesc);
void pg_volvec_unregister_state(QueryDesc *queryDesc);

PgVolVecQueryState *pg_volvec_try_build_query_state(QueryDesc *queryDesc, int eflags);
void pg_volvec_close_query_state(PgVolVecQueryState *state);

#ifdef __cplusplus
}
#endif

#endif

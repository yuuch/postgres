#ifndef PG_VEC_STATE_H
#define PG_VEC_STATE_H

#include "postgres.h"

#include "executor/executor.h"
#include "utils/rel.h"

#include "../ir/vec_ir.h"

typedef struct PgVecQueryState
{
	PgVecPlan	plan;
	Relation	rels[PG_VEC_MAX_INPUTS];
	TupleTableSlot *result_slot;
	bool		completed;
} PgVecQueryState;

void pg_vec_init_state_table(void);
PgVecQueryState *pg_vec_lookup_state(QueryDesc *queryDesc);
void pg_vec_register_state(QueryDesc *queryDesc, PgVecQueryState *state);
void pg_vec_unregister_state(QueryDesc *queryDesc);
PgVecQueryState *pg_vec_try_build_query_state(QueryDesc *queryDesc, int eflags);
void pg_vec_close_query_state(PgVecQueryState *state);
bool pg_vec_translation_guard_active(void);
void pg_vec_push_translation_guard(void);
void pg_vec_pop_translation_guard(void);

#endif

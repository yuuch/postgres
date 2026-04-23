#pragma once

extern "C" {
#include "postgres.h"
#include "executor/execdesc.h"
}

namespace pg_volvec {

struct PgVolVecQueryState;

namespace pipeline {

bool PgvolvecPipelineRun(QueryDesc *queryDesc,
                         PgVolVecQueryState *state,
                         const char **failure_reason);

}
}

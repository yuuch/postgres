#pragma once

extern "C" {
#include "postgres.h"
}

#include "parallel/pipeline/query_state.hpp"

namespace pg_volvec {
namespace pipeline {

bool CreateRuntimeDsm(PgVolVecQueryState *state, const char **error_out);
void DestroyRuntimeDsm(PgVolVecQueryState *state);

}
}

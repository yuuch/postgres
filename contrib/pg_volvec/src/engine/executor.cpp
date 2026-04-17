#include "volvec_engine.hpp"
#include "llvmjit_deform_datachunk.h"

#include <algorithm>
#include <cstring>

extern "C" {
#include "utils/lsyscache.h"
#include "access/tableam.h"
#include "access/visibilitymap.h"
#include "access/stratnum.h"
#include "executor/nodeSubplan.h"
#include "nodes/nodeFuncs.h"
#include "parser/parsetree.h"
#include "storage/bufmgr.h"

extern bool pg_volvec_jit_deform;
extern bool pg_volvec_trace_hooks;
extern bool pg_volvec_disable_jit_for_parallel_worker;
}

namespace pg_volvec
{
#include "exec/executor_common.cpp"
#include "exec/agg.cpp"
#include "exec/seq_scan.cpp"
#include "exec/filter.cpp"
#include "exec/hash_join_lookup.cpp"
#include "exec/project.cpp"
#include "exec/limit.cpp"
#include "exec/hash_join.cpp"
#include "exec/sort.cpp"
#include "exec/executor_init.cpp"
} /* namespace pg_volvec */

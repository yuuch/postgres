#include "exec/internal.hpp"

extern "C" {
#include "access/parallel.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "executor/executor.h"
#include "nodes/plannodes.h"
#include "nodes/nodes.h"
#include "parser/parsetree.h"
#include "portability/instr_time.h"
#include "port/atomics.h"
#include "storage/ipc.h"
#include "tcop/pquery.h"
#include "utils/datum.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "storage/sharedfileset.h"
}

extern "C" {
extern bool pg_volvec_trace_hooks;
extern int pg_volvec_parallel_max_workers;
extern int pg_volvec_parallel_morsel_nblocks;
extern int pg_volvec_parallel_min_relation_blocks;
extern bool pg_volvec_parallel_leader_participation;
}

extern "C" PGDLLEXPORT void pg_volvec_parallel_worker_main(dsm_segment *seg, shm_toc *toc);

namespace pg_volvec {

#include "parallel/runtime_lowering.inc"
#include "parallel/runtime_worker_state.inc"
#include "parallel/runtime_execution.inc"

} /* namespace pg_volvec */

#include "parallel/runtime_worker_main.inc"

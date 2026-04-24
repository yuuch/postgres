/*
 * pipeline_leader.cpp — Step 2 stub.
 *
 * The legacy DuckDB-style leader (DSM allocation, ParallelContext launch,
 * LowerToPipeline + WorkerPipelineExecutor + AggSink combine/finalize) was
 * deleted in the M-IR-MIN demolition. This stub keeps the bridge entry-point
 * symbol alive so contrib/pg_volvec links during the M-FRAME-MIN rewrite.
 *
 * Returning false here causes bridge/execute.cpp to fall through to the
 * standard PostgreSQL ExecutorRun path (with a WARNING). Both Q1 and Q6 will
 * therefore execute on the native PG executor until the real pipeline runtime
 * lands in the next milestone.
 */

#include "parallel/pipeline/pipeline_leader.hpp"

namespace pg_volvec {
namespace pipeline {

bool
PgvolvecPipelineRun(QueryDesc *queryDesc,
					PgVolVecQueryState *state,
					const char **failure_reason)
{
	(void) queryDesc;
	(void) state;
	if (failure_reason != nullptr)
		*failure_reason = "pipeline runtime not implemented yet";
	return false;
}

}
}

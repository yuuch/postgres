/*
 * pipeline_worker_main.cpp — Step 2 stub.
 *
 * The legacy bgworker entry-point (DSM/DSA attach, PipelineWorkerState,
 * LowerToPipeline, WorkerPipelineExecutor, AggSink combine) was deleted in
 * the M-IR-MIN demolition. The leader stub never launches workers, so this
 * symbol is currently unreachable; it exists only to satisfy any lingering
 * dynamic background-worker registration string and to give the M-FRAME-MIN
 * rewrite a stable insertion point.
 *
 * If somehow invoked, fail loudly so the leader's worker_error path triggers
 * and the user does not get silent partial results.
 */

extern "C" {
#include "postgres.h"
#include "storage/dsm.h"
#include "storage/shm_toc.h"
#include "utils/elog.h"
}

extern "C" PGDLLEXPORT void
pg_volvec_pipeline_worker_main(dsm_segment *seg, shm_toc *toc)
{
	(void) seg;
	(void) toc;
	elog(ERROR, "pg_volvec pipeline worker invoked but runtime not implemented");
}

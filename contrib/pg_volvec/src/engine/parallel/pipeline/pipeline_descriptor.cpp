/*-------------------------------------------------------------------------
 *
 * pipeline_descriptor.cpp
 *	  3g.2-prep skeleton bodies for the cross-process IR helpers declared in
 *	  pipeline_descriptor.hpp.
 *
 * STATUS: 3g.2-prep (infrastructure scaffolding only). Function bodies in
 * this file are deliberate stubs that ereport(ERROR) as soon as they are
 * asked to do real work. Real implementations land in 3g.2-final together
 * with the rewritten pipeline_leader.cpp / pipeline_worker_main.cpp /
 * pipeline_*_event.cpp / translator.cpp / task.cpp set.
 *
 * DESIGN ANCHORS:
 *   docs/GLOBAL_LOCAL_STATE_DESIGN.md
 *	   §6.3   Sink global-state ABI (DSA-resident raw payload)
 *	   §8.5.4 PipelineDescriptor / OpDescriptor / *OpBody POD layout
 *	   §8.5.4.7 ExprBytecode constraints
 *
 * INVARIANTS (3g.2-prep, MUST be upheld in 3g.2-final too):
 *   - LeaderSerializePipelines() is leader-only. Workers MUST NOT call it.
 *   - WorkerReconstructPipelines() is worker-only. Leader MUST NOT call it.
 *   - StoreSharedPayloadOnDescriptor / LoadSharedPayloadFromDescriptor are
 *	   leader-only helpers used by PhysicalSink::GetGlobalSinkState() to
 *	   thread the leader-allocated raw payload back into the descriptor for
 *	   workers to attach to (lazy, single-init).
 *
 * 3g.2-prep BEHAVIOR (NOT a bug, intentional safety net):
 *   - LeaderSerializePipelines() with an empty bundle returns
 *	   InvalidDsaPointer (zero pipelines = nothing to publish; lets the
 *	   leader wire the call site without crashing during smoke tests).
 *   - All non-trivial inputs ereport(ERROR, ERRCODE_FEATURE_NOT_SUPPORTED).
 *   - WorkerReconstructPipelines() always validates the magic (defensive
 *	   even in prep) before ereport(ERROR).
 *
 * 3g.2-final TODO (next session, NOT this commit):
 *   - LeaderSerializePipelines: walk bundle.pipelines, dsa_allocate
 *	   PipelineDescriptor + OpDescriptor[] arrays, lower PhysicalOperator
 *	   tree to OpBody bodies, lower qual ExprProgram (§8.5.4.7), publish
 *	   pipelines_root dsa_pointer to PipelineSharedControl.
 *   - WorkerReconstructPipelines: dsa_get_address(pipelines_root), reverse
 *	   the lowering into Pipeline / PhysicalOperator instances under
 *	   worker_ctx.mcxt (MemoryContextSwitchTo'd by caller), rebuild the
 *	   PgVector<unique_ptr<Pipeline>> for the worker scheduler.
 *   - Store/Load helpers: write/read OpDescriptor.global_sink_state /
 *	   global_source_state dsa_pointer slots.
 *
 *-------------------------------------------------------------------------
 */

extern "C" {
#include "postgres.h"
#include "utils/elog.h"
}

#include "core/memory.hpp"
#include "parallel/pipeline/dsm_control.hpp"
#include "parallel/pipeline/meta_pipeline.hpp"
#include "parallel/pipeline/physical_operator.hpp"
#include "parallel/pipeline/pipeline.hpp"
#include "parallel/pipeline/pipeline_descriptor.hpp"
#include "parallel/pipeline/types.hpp"

namespace pg_volvec {
namespace pipeline {

/*
 * Leader-only: serialize a fully-built MetaPipelineBundle into DSA, returning
 * the dsa_pointer to the root PipelineDescriptor[] array. The returned
 * pointer is what the leader writes into PipelineSharedControl.pipelines_root
 * before launching workers.
 *
 * 3g.2-prep: only the empty-bundle smoke path is allowed (returns
 * InvalidDsaPointer). Anything else raises ERRCODE_FEATURE_NOT_SUPPORTED.
 */
dsa_pointer
LeaderSerializePipelines(MetaPipelineBundle &bundle, dsa_area *dsa)
{
	if (dsa == nullptr)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_volvec: LeaderSerializePipelines called with null dsa_area")));

	if (bundle.pipelines.empty())
		return InvalidDsaPointer;

	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("pg_volvec: LeaderSerializePipelines is not implemented in 3g.2-prep"),
			 errdetail("Pipeline serialization (PipelineDescriptor + OpDescriptor lowering) lands in 3g.2-final.")));

	return InvalidDsaPointer;
}

/*
 * Worker-only: reverse the leader's serialization. The caller (the worker
 * bgworker entry) MUST have already MemoryContextSwitchTo'd into a long-lived
 * per-query context before invoking this helper, so that PhysicalOperator
 * state and JIT contexts attach to the right context.
 *
 * 3g.2-prep: stub. Real workers do not run yet (pipeline_worker_main.cpp is
 * still a stub elog(ERROR)), so this function is unreachable in practice.
 * The defensive magic check is kept in prep to guarantee that any premature
 * caller fails loudly rather than silently corrupting unrelated DSM segments.
 */
void
WorkerReconstructPipelines(PipelineSharedControl                 *ctl,
						   ExecCtx                               &worker_ctx,
						   PgVector<std::unique_ptr<Pipeline>>   &out)
{
	(void) worker_ctx;
	(void) out;

	if (ctl == nullptr)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_volvec: WorkerReconstructPipelines called with null PipelineSharedControl")));

	if (ctl->magic != PIPELINE_DSM_MAGIC)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_volvec: PipelineSharedControl magic mismatch (got 0x%08x, expected 0x%08x)",
						ctl->magic, PIPELINE_DSM_MAGIC)));

	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("pg_volvec: WorkerReconstructPipelines is not implemented in 3g.2-prep"),
			 errdetail("Worker-side PipelineDescriptor reconstruction lands in 3g.2-final.")));
}

/*
 * Leader-only: lazy-publish the leader-allocated raw payload (e.g. the
 * AggSharedPayload behind a HashAggregate, the SortSharedPayload behind an
 * Order) into the OpDescriptor.global_sink_state / global_source_state slot
 * so that workers can dsa_get_address it and attach.
 *
 * 3g.2-prep: stub. Real call sites in PhysicalHashAggregate::GetGlobalSinkState
 * and PhysicalOrder::GetGlobalSinkState arrive in 3g.2-final.
 */
void
StoreSharedPayloadOnDescriptor(const PhysicalOperator *op, dsa_pointer payload_dp)
{
	(void) op;
	(void) payload_dp;

	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("pg_volvec: StoreSharedPayloadOnDescriptor is not implemented in 3g.2-prep"),
			 errdetail("Lazy global-sink-state publication lands in 3g.2-final together with the Sink rewrite.")));
}

/*
 * Worker-and-leader: load the previously-published raw payload dsa_pointer.
 *
 * 3g.2-prep: stub. Real call sites in PhysicalHashAggregate::GetGlobalSinkState
 * (worker attach branch) and PhysicalSeqScan::GetGlobalSourceState arrive in
 * 3g.2-final.
 */
dsa_pointer
LoadSharedPayloadFromDescriptor(const PhysicalOperator *op)
{
	(void) op;

	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("pg_volvec: LoadSharedPayloadFromDescriptor is not implemented in 3g.2-prep"),
			 errdetail("Lazy global-sink-state attach lands in 3g.2-final together with the Sink rewrite.")));

	return InvalidDsaPointer;
}

}  /* namespace pipeline */
}  /* namespace pg_volvec */

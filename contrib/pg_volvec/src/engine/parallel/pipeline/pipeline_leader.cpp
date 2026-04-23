#include "parallel/pipeline/pipeline_leader.hpp"

extern "C" {
#include "postgres.h"
#include "access/parallel.h"
#include "access/relation.h"
#include "access/table.h"
#include "access/tableam.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "parser/parsetree.h"
#include "storage/sharedfileset.h"
#include "storage/shm_toc.h"
#include "utils/dsa.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
}

#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "core/data_chunk_deform.hpp"
#include "exec/plan_state.hpp"
#include "exec/agg.hpp"
#include "exec/query_state.hpp"

namespace pg_volvec {
#include "parallel/pipeline/agg_sink.hpp"

#include "parallel/pipeline/dsm_control.hpp"
#include "parallel/pipeline/executor.hpp"
#include "parallel/pipeline/pipeline_lowering.hpp"
#include "parallel/pipeline/pipeline_worker_state.hpp"
#include "parallel/pipeline/sink.hpp"
#include "parallel/pipeline/source.hpp"

namespace pg_volvec {
namespace pipeline {

extern "C" int  pg_volvec_parallel_max_workers;
extern "C" int  pg_volvec_parallel_morsel_nblocks;
extern "C" bool pg_volvec_trace_hooks;

namespace {

struct PipelineSourceInfo
{
	Oid         relid = InvalidOid;
	int         plan_node_id = -1;
	BlockNumber total_blocks = 0;
};

bool
DiscoverSeqScanSourceWithRtable(Plan *plan, List *rtable, PipelineSourceInfo *out)
{
	if (plan == nullptr)
		return false;
	if (IsA(plan, SeqScan))
	{
		auto *scan = (SeqScan *) plan;
		RangeTblEntry *rte = rt_fetch(scan->scan.scanrelid, rtable);
		if (rte == nullptr || rte->rtekind != RTE_RELATION)
			return false;
		out->plan_node_id = plan->plan_node_id;
		out->relid = rte->relid;
		return true;
	}
	if (DiscoverSeqScanSourceWithRtable(plan->lefttree, rtable, out))
		return true;
	return DiscoverSeqScanSourceWithRtable(plan->righttree, rtable, out);
}

int
DiscoverAggPlanNodeId(Plan *plan)
{
	if (plan == nullptr)
		return -1;
	if (IsA(plan, Agg))
		return plan->plan_node_id;
	int left = DiscoverAggPlanNodeId(plan->lefttree);
	if (left >= 0)
		return left;
	return DiscoverAggPlanNodeId(plan->righttree);
}

}

bool
PgvolvecPipelineRun(QueryDesc *queryDesc,
					PgVolVecQueryState *state,
					const char **failure_reason)
{
	if (failure_reason != nullptr)
		*failure_reason = nullptr;
	if (queryDesc == nullptr || state == nullptr || state->vec_plan == nullptr)
	{
		if (failure_reason != nullptr)
			*failure_reason = "pipeline leader missing query state";
		return false;
	}

	PipelineSourceInfo source_info;
	if (!DiscoverSeqScanSourceWithRtable(queryDesc->plannedstmt->planTree,
										 queryDesc->plannedstmt->rtable,
										 &source_info))
	{
		if (failure_reason != nullptr)
			*failure_reason = "pipeline leader could not locate SeqScan source";
		return false;
	}

	int agg_plan_node_id =
		DiscoverAggPlanNodeId(queryDesc->plannedstmt->planTree);
	if (agg_plan_node_id < 0)
	{
		if (failure_reason != nullptr)
			*failure_reason = "pipeline leader could not locate Agg node";
		return false;
	}

	{
		Relation rel = table_open(source_info.relid, AccessShareLock);
		source_info.total_blocks =
			RelationGetNumberOfBlocks(rel);
		table_close(rel, AccessShareLock);
	}

	const char *plannedstmt_serialized =
		nodeToString(queryDesc->plannedstmt);
	const char *query_text =
		queryDesc->sourceText != nullptr ? queryDesc->sourceText : "";
	size_t      plannedstmt_len = strlen(plannedstmt_serialized) + 1;
	size_t      query_text_len  = strlen(query_text) + 1;
	int         requested_workers = Max(1, pg_volvec_parallel_max_workers);
	int         morsel_nblocks    = Max(1, pg_volvec_parallel_morsel_nblocks);

	ParallelContext           *pcxt = nullptr;
	bool                       entered_parallel = false;
	bool                       ok = false;
	const char                *local_reason = nullptr;
	PipelineWorkerState        leader_state;
	std::unique_ptr<LoweredPipeline> bundle;

	PG_TRY();
	{
		EnterParallelMode();
		entered_parallel = true;

		pcxt = CreateParallelContext("pg_volvec",
									 "pg_volvec_pipeline_worker_main",
									 requested_workers);

		const int leader_slot_count = 1;
		size_t partials_len =
			sizeof(ParallelAggPartialState) *
			(size_t) (pcxt->nworkers + leader_slot_count);
		size_t source_pscan_len = 0;
		Relation source_rel = table_open(source_info.relid, NoLock);
		source_pscan_len =
			table_parallelscan_estimate(source_rel,
										queryDesc->estate->es_snapshot);
		size_t dsa_minsize = dsa_minimum_size();

		shm_toc_estimate_chunk(&pcxt->estimator, sizeof(PipelineSharedControl));
		shm_toc_estimate_keys(&pcxt->estimator, 1);
		shm_toc_estimate_chunk(&pcxt->estimator, plannedstmt_len);
		shm_toc_estimate_keys(&pcxt->estimator, 1);
		shm_toc_estimate_chunk(&pcxt->estimator, query_text_len);
		shm_toc_estimate_keys(&pcxt->estimator, 1);
		shm_toc_estimate_chunk(&pcxt->estimator, partials_len);
		shm_toc_estimate_keys(&pcxt->estimator, 1);
		shm_toc_estimate_chunk(&pcxt->estimator, source_pscan_len);
		shm_toc_estimate_keys(&pcxt->estimator, 1);
		shm_toc_estimate_chunk(&pcxt->estimator, sizeof(SharedFileSet));
		shm_toc_estimate_keys(&pcxt->estimator, 1);
		shm_toc_estimate_chunk(&pcxt->estimator, dsa_minsize);
		shm_toc_estimate_keys(&pcxt->estimator, 1);

		InitializeParallelDSM(pcxt);
		if (pcxt->seg == nullptr)
		{
			table_close(source_rel, NoLock);
			local_reason = "pipeline leader DSM initialization failed";
		}
		else
		{
			auto *control = (PipelineSharedControl *)
				shm_toc_allocate(pcxt->toc, sizeof(PipelineSharedControl));
			memset(control, 0, sizeof(*control));
			control->magic                    = PIPELINE_DSM_MAGIC;
			control->partial_slot_count       = (uint32) (pcxt->nworkers + 1);
			control->morsel_nblocks           = (uint32) morsel_nblocks;
			control->total_blocks             = (uint32) source_info.total_blocks;
			control->source_scan_relid        = source_info.relid;
			control->source_scan_plan_node_id = source_info.plan_node_id;
			control->agg_plan_node_id         = agg_plan_node_id;
			pg_atomic_init_u64(&control->next_block, 0);
			pg_atomic_init_u32(&control->worker_error, 0);
			shm_toc_insert(pcxt->toc, PIPELINE_DSM_KEY_CONTROL, control);

			char *shared_plannedstmt = (char *)
				shm_toc_allocate(pcxt->toc, plannedstmt_len);
			memcpy(shared_plannedstmt, plannedstmt_serialized, plannedstmt_len);
			shm_toc_insert(pcxt->toc, PIPELINE_DSM_KEY_PLANNEDSTMT,
						   shared_plannedstmt);

			char *shared_query_text = (char *)
				shm_toc_allocate(pcxt->toc, query_text_len);
			memcpy(shared_query_text, query_text, query_text_len);
			shm_toc_insert(pcxt->toc, PIPELINE_DSM_KEY_QUERY_TEXT,
						   shared_query_text);

			auto *partials = (ParallelAggPartialState *)
				shm_toc_allocate(pcxt->toc, partials_len);
			memset(partials, 0, partials_len);
			shm_toc_insert(pcxt->toc, PIPELINE_DSM_KEY_PARTIALS, partials);

			auto *source_pscan = (ParallelTableScanDesc)
				shm_toc_allocate(pcxt->toc, source_pscan_len);
			table_parallelscan_initialize(source_rel,
										  source_pscan,
										  queryDesc->estate->es_snapshot);
			shm_toc_insert(pcxt->toc, PIPELINE_DSM_KEY_SOURCE_PSCAN,
						   source_pscan);

			auto *partial_fileset = (SharedFileSet *)
				shm_toc_allocate(pcxt->toc, sizeof(SharedFileSet));
			SharedFileSetInit(partial_fileset, pcxt->seg);
			shm_toc_insert(pcxt->toc, PIPELINE_DSM_KEY_PARTIAL_FILESET,
						   partial_fileset);

			char *dsa_space = (char *)
				shm_toc_allocate(pcxt->toc, dsa_minsize);
			(void) dsa_create_in_place(dsa_space, dsa_minsize,
									   LWTRANCHE_PARALLEL_QUERY_DSA,
									   pcxt->seg);
			shm_toc_insert(pcxt->toc, PIPELINE_DSM_KEY_DSA, dsa_space);

			table_close(source_rel, NoLock);

			if (!InitializePipelineWorkerState(plannedstmt_serialized,
											   query_text,
											   agg_plan_node_id,
											   source_info.relid,
											   source_info.plan_node_id,
											   source_pscan,
											   true,
											   &leader_state,
											   &local_reason))
			{
			}
			else
			{
				MemoryContext old_context =
					MemoryContextSwitchTo(leader_state.memory_context);

				bundle = LowerToPipeline(leader_state.root_plan.get(),
										 control,
										 &control->next_block,
										 partials,
										 (int) control->partial_slot_count,
										 partial_fileset);

				if (!bundle)
				{
					MemoryContextSwitchTo(old_context);
					local_reason = "pipeline leader could not lower plan";
				}
				else
				{
					LaunchParallelWorkers(pcxt);

					ExecCtx exec_ctx{};
					exec_ctx.mcxt         = leader_state.memory_context;
					exec_ctx.dsa          = nullptr;
					exec_ctx.vec_plan     = leader_state.root_plan.get();
					exec_ctx.worker_index = LEADER_WORKER_INDEX;

					OwnedPipeline *owned = bundle->primary();
					Source *src   = owned->pipeline.src;
					Sink   *sink  = owned->pipeline.sink;
					auto    gsrc  = src->GetGlobalSourceState(exec_ctx);
					auto    lsrc  = src->GetLocalSourceState(exec_ctx, *gsrc);
					auto    gsink = sink->GetGlobalSinkState(exec_ctx);
					auto    lsink = sink->GetLocalSinkState(exec_ctx, *gsink);

					WorkerPipelineExecutor exec(src,
												owned->pipeline.ops,
												sink);
					exec.Execute(exec_ctx, *gsrc, *lsrc,
								 gsink.get(), lsink.get(), 0);

					OperatorSinkCombineInput combine_input{*lsink, *gsink};
					sink->Combine(exec_ctx, combine_input);

					WaitForParallelWorkersToFinish(pcxt);

					sink->Finalize(exec_ctx, *gsink);

					delete state->vec_plan;
					state->vec_plan = leader_state.root_plan.release();

					MemoryContextSwitchTo(old_context);
					ok = true;
				}
			}
		}
	}
	PG_CATCH();
	{
		bundle.reset();
		CleanupPipelineWorkerState(&leader_state);
		if (pcxt != nullptr)
			DestroyParallelContext(pcxt);
		if (entered_parallel)
			ExitParallelMode();
		PG_RE_THROW();
	}
	PG_END_TRY();

	bundle.reset();
	if (!ok)
		CleanupPipelineWorkerState(&leader_state);
	if (pcxt != nullptr)
		DestroyParallelContext(pcxt);
	if (entered_parallel)
		ExitParallelMode();

	if (!ok && failure_reason != nullptr)
		*failure_reason = local_reason != nullptr
			? local_reason
			: "pipeline leader run failed";

	if (ok && pg_volvec_trace_hooks)
		elog(LOG,
			 "pg_volvec: pipeline leader run ok rel=%u blocks=%u workers=%d",
			 source_info.relid,
			 source_info.total_blocks,
			 requested_workers);

	return ok;
}

}
}

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

namespace {

enum class ParallelWorkerExecutionMode : uint32_t {
	AggregateProbe = 1,
	HashBuild = 2
};

static const char *
ParallelDriverKindDebugName(ParallelPipelineDriverKind kind)
{
	switch (kind)
	{
		case ParallelPipelineDriverKind::SourceScan:
			return "SourceScan";
		case ParallelPipelineDriverKind::BridgeFinalize:
			return "BridgeFinalize";
	}
	return "Unknown";
}

static const char *
ParallelPipelineRoleDebugName(ParallelPipelineRole role)
{
	switch (role)
	{
		case ParallelPipelineRole::GenericSource:
			return "GenericSource";
		case ParallelPipelineRole::AggFinalize:
			return "AggFinalize";
		case ParallelPipelineRole::SortMerge:
			return "SortMerge";
		case ParallelPipelineRole::HashBuildSource:
			return "HashBuildSource";
		case ParallelPipelineRole::HashBuildFinalize:
			return "HashBuildFinalize";
		case ParallelPipelineRole::HashProbeSource:
			return "HashProbeSource";
		case ParallelPipelineRole::HashOuterSource:
			return "HashOuterSource";
	}
	return "Unknown";
}

static const char *
ParallelWorkerExecutionModeDebugName(ParallelWorkerExecutionMode mode)
{
	switch (mode)
	{
		case ParallelWorkerExecutionMode::AggregateProbe:
			return "AggregateProbe";
		case ParallelWorkerExecutionMode::HashBuild:
			return "HashBuild";
	}
	return "Unknown";
}

static constexpr uint64 VOLVEC_PARALLEL_KEY_CONTROL =
	UINT64CONST(0xD700000000000001);
static constexpr uint64 VOLVEC_PARALLEL_KEY_PLANNEDSTMT =
	UINT64CONST(0xD700000000000002);
static constexpr uint64 VOLVEC_PARALLEL_KEY_QUERY_TEXT =
	UINT64CONST(0xD700000000000003);
static constexpr uint64 VOLVEC_PARALLEL_KEY_PARTIALS =
	UINT64CONST(0xD700000000000004);
static constexpr uint64 VOLVEC_PARALLEL_KEY_SOURCE_PSCAN =
	UINT64CONST(0xD700000000000005);
static constexpr uint64 VOLVEC_PARALLEL_KEY_PARTIAL_FILESET =
	UINT64CONST(0xD700000000000006);
static constexpr uint64 VOLVEC_PARALLEL_KEY_HASH_BRIDGE =
	UINT64CONST(0xD700000000000007);
static constexpr uint64 VOLVEC_PARALLEL_KEY_HASH_BUILD_PARTIALS =
	UINT64CONST(0xD700000000000008);
static constexpr uint32 VOLVEC_PARALLEL_MAGIC = 0x56565050;
static constexpr uint32_t VOLVEC_PARALLEL_HASH_BUILD_DOMINANCE_RATIO = 4;
static constexpr double VOLVEC_PARALLEL_HASH_BUILD_SMALL_ROWS_RATIO = 0.25;

struct ParallelAggregateSharedControl
{
	uint32 magic;
	uint32 source_pipeline_id;
	uint32 partial_slot_count;
	uint32 morsel_nblocks;
	uint32 total_blocks;
	Oid source_scan_relid;
	int source_scan_plan_node_id;
	int agg_plan_node_id;
	int hash_join_plan_node_id;
	int input_hash_join_plan_node_id;
	uint32 execution_mode;
	bool need_hash_join_state;
	uint8 hash_bridge_ready;
	uint8 reserved[2];
	uint64 hash_bridge_size;
	pg_atomic_uint64 next_block;
};

struct LocalParallelAggregateProcessState
{
	MemoryContext memory_context = nullptr;
	char *query_text = nullptr;
	PlannedStmt *plannedstmt = nullptr;
	EState *estate = nullptr;
	VecPlanState *root_plan = nullptr;
	ParallelWorkerContext worker_context;
	uint64 init_time_us = 0;
	uint64 exec_time_us = 0;
};

struct PipelineLoweringResult {
	uint32_t pipeline_id = UINT32_MAX;
	bool valid = false;
};

struct PipelineLoweringContext {
	PlannedStmt *plannedstmt = nullptr;
	ParallelPipelinePlan *parallel_plan = nullptr;
	const char **failure_reason = nullptr;
};

static bool
SetFailure(PipelineLoweringContext *context, const char *reason)
{
	if (context != nullptr && context->failure_reason != nullptr)
		*context->failure_reason = reason;
	return false;
}

static void
FormatParallelPartialFileName(char *dst, size_t dstlen, const char *prefix, int slot)
{
	snprintf(dst, dstlen, "pg_volvec_%s_%d", prefix, slot);
}

static bool
ShouldSwapInnerJoinBuildSidesForParallelLowering(JoinType jointype,
												 Plan *outer_plan,
												 Plan *inner_plan)
{
	double outer_rows;
	double inner_rows;

	if (jointype != JOIN_INNER || outer_plan == nullptr || inner_plan == nullptr)
		return false;
	outer_rows = outer_plan->plan_rows;
	inner_rows = inner_plan->plan_rows;
	if (outer_rows <= 0 || inner_rows <= 0)
		return false;
	return outer_rows < inner_rows;
}

static bool
CanUsePipelineAsParallelHashProbe(const ParallelPipelineDesc *pipeline)
{
	return pipeline != nullptr &&
		pipeline->driver_kind == ParallelPipelineDriverKind::SourceScan &&
		pipeline->source_morsel_driven;
}

static bool
ShouldBuildOuterSideForParallelHashJoin(JoinType jointype,
										Plan *outer_plan,
										Plan *inner_plan,
										const ParallelPipelineDesc *outer_pipeline,
										const ParallelPipelineDesc *inner_pipeline)
{
	bool outer_can_probe = CanUsePipelineAsParallelHashProbe(outer_pipeline);
	bool inner_can_probe = CanUsePipelineAsParallelHashProbe(inner_pipeline);

	/*
	 * The current process-worker path parallelizes exactly one source scan and
	 * then lets the local vector executor drive the rest of the subtree. If a
	 * HashJoin input is already behind a breaker/bridge, keep that side as the
	 * hash-build input so the probe side remains morsel-driven.
	 */
	if (outer_can_probe && !inner_can_probe)
		return false;
	if (!outer_can_probe && inner_can_probe)
		return true;
	return ShouldSwapInnerJoinBuildSidesForParallelLowering(jointype,
															outer_plan,
															inner_plan);
}

static Plan *
FindPlanNodeById(Plan *plan, int target_plan_node_id)
{
	Plan *found = nullptr;

	if (plan == nullptr || target_plan_node_id < 0)
		return nullptr;
	if (plan->plan_node_id == target_plan_node_id)
		return plan;
	if (IsA(plan, SubqueryScan))
	{
		found = FindPlanNodeById(((SubqueryScan *) plan)->subplan,
								 target_plan_node_id);
		if (found != nullptr)
			return found;
	}
	found = FindPlanNodeById(plan->lefttree, target_plan_node_id);
	if (found != nullptr)
		return found;
	return FindPlanNodeById(plan->righttree, target_plan_node_id);
}

static double
LookupPlannedStmtNodeRows(const PlannedStmt *plannedstmt, int target_plan_node_id)
{
	Plan *plan;

	if (plannedstmt == nullptr || target_plan_node_id < 0)
		return -1.0;
	plan = FindPlanNodeById(plannedstmt->planTree, target_plan_node_id);
	if (plan == nullptr)
		return -1.0;
	return plan->plan_rows;
}

static BlockNumber
LookupRelationBlocks(Oid relid)
{
	Relation rel;
	BlockNumber nblocks;

	if (!OidIsValid(relid))
		return InvalidBlockNumber;
	rel = relation_open(relid, AccessShareLock);
	nblocks = RelationGetNumberOfBlocks(rel);
	relation_close(rel, AccessShareLock);
	return nblocks;
}

static RangeTblEntry *
LookupScanRte(PlannedStmt *plannedstmt, Index scanrelid)
{
	if (plannedstmt == nullptr ||
		scanrelid <= 0 ||
		scanrelid > list_length(plannedstmt->rtable))
		return nullptr;
	return rt_fetch(scanrelid, plannedstmt->rtable);
}

static Oid
LookupSeqScanRelid(SeqScan *scan, PlannedStmt *plannedstmt)
{
	RangeTblEntry *rte = LookupScanRte(plannedstmt, scan->scan.scanrelid);

	if (rte == nullptr || rte->rtekind != RTE_RELATION)
		return InvalidOid;
	return rte->relid;
}

static bool
IsAggregateSourcePipeline(const ParallelPipelineDesc &pipeline)
{
	return pipeline.source_morsel_driven &&
		pipeline.output_bridge == ParallelBridgeKind::Aggregate &&
		(pipeline.stage_mask & (uint32_t) ParallelPipelineStage::PartialAgg) != 0 &&
		(pipeline.role == ParallelPipelineRole::GenericSource ||
		 pipeline.role == ParallelPipelineRole::HashProbeSource);
}

static bool
FindLargestHashBuildDependencyBlocks(const ParallelPipelinePlan *parallel_plan,
									 const ParallelSchedulerState *scheduler,
									 uint32_t pipeline_id,
									 uint32_t depth,
									 BlockNumber *max_blocks_out,
									 uint32_t *max_pipeline_id_out)
{
	const ParallelPipelineDesc *pipeline;
	bool found = false;

	if (parallel_plan == nullptr || scheduler == nullptr ||
		max_blocks_out == nullptr || max_pipeline_id_out == nullptr ||
		depth > parallel_plan->pipeline_count())
		return false;

	pipeline = parallel_plan->get_pipeline(pipeline_id);
	if (pipeline == nullptr)
		return false;

	for (uint32_t dependency_id : pipeline->dependencies)
	{
		const ParallelPipelineDesc *dependency = parallel_plan->get_pipeline(dependency_id);

		if (dependency == nullptr)
			continue;
		if (dependency->role == ParallelPipelineRole::HashBuildSource)
		{
			const ParallelPipelineRuntimeState *runtime =
				scheduler->get_pipeline_runtime(dependency_id);

			if (runtime != nullptr &&
				runtime->total_blocks != InvalidBlockNumber &&
				runtime->total_blocks > 0 &&
				(*max_pipeline_id_out == UINT32_MAX ||
				 runtime->total_blocks > *max_blocks_out))
			{
				*max_blocks_out = runtime->total_blocks;
				*max_pipeline_id_out = dependency_id;
				found = true;
			}
		}
		if (FindLargestHashBuildDependencyBlocks(parallel_plan,
												 scheduler,
												 dependency_id,
												 depth + 1,
												 max_blocks_out,
												 max_pipeline_id_out))
			found = true;
	}
	return found;
}

static bool
ShouldSkipHashProbeParallelForBuildDominatedDependency(
	const ParallelPipelinePlan *parallel_plan,
	const ParallelSchedulerState *scheduler,
	const ParallelPipelineDesc *source_pipeline,
	const ParallelPipelineRuntimeState *source_runtime,
	BlockNumber *build_blocks_out,
	uint32_t *build_pipeline_id_out,
	double *build_rows_out,
	double *source_rows_out)
{
	BlockNumber build_blocks = 0;
	uint32_t build_pipeline_id = UINT32_MAX;
	bool found_build_dependency;
	double build_rows = -1.0;
	double source_rows = -1.0;

	if (source_pipeline == nullptr ||
		source_pipeline->role != ParallelPipelineRole::HashProbeSource ||
		source_runtime == nullptr ||
		source_runtime->total_blocks == InvalidBlockNumber ||
		source_runtime->total_blocks == 0)
		return false;

	found_build_dependency =
		FindLargestHashBuildDependencyBlocks(parallel_plan,
											 scheduler,
											 source_pipeline->pipeline_id,
											 0,
											 &build_blocks,
											 &build_pipeline_id);
	if (build_blocks_out != nullptr)
		*build_blocks_out = build_blocks;
	if (build_pipeline_id_out != nullptr)
		*build_pipeline_id_out = build_pipeline_id;
	if (build_rows_out != nullptr)
		*build_rows_out = build_rows;
	if (source_rows_out != nullptr)
		*source_rows_out = source_rows;
	if (!found_build_dependency)
		return false;

	if (parallel_plan != nullptr)
	{
		const ParallelPipelineDesc *build_pipeline = parallel_plan->get_pipeline(build_pipeline_id);

		if (build_pipeline != nullptr)
			build_rows = build_pipeline->estimated_rows;
		source_rows = source_pipeline->estimated_rows;
		if (build_rows_out != nullptr)
			*build_rows_out = build_rows;
		if (source_rows_out != nullptr)
			*source_rows_out = source_rows;
		if (build_rows > 0.0 &&
			source_rows > 0.0 &&
			build_rows <= source_rows * VOLVEC_PARALLEL_HASH_BUILD_SMALL_ROWS_RATIO)
			return false;
	}

	return (uint64) build_blocks >
		(uint64) source_runtime->total_blocks *
		VOLVEC_PARALLEL_HASH_BUILD_DOMINANCE_RATIO;
}

static const ParallelPipelineDesc *
FindLargestHashBuildDependency(const ParallelPipelinePlan *parallel_plan,
								 const ParallelSchedulerState *scheduler,
								 const ParallelPipelineDesc *source_pipeline,
								 const ParallelPipelineRuntimeState **runtime_out)
{
	BlockNumber build_blocks = 0;
	uint32_t build_pipeline_id = UINT32_MAX;
	bool found;
	const ParallelPipelineDesc *build_pipeline;

	if (runtime_out != nullptr)
		*runtime_out = nullptr;
	if (parallel_plan == nullptr || scheduler == nullptr || source_pipeline == nullptr)
		return nullptr;

	found = FindLargestHashBuildDependencyBlocks(parallel_plan,
												 scheduler,
												 source_pipeline->pipeline_id,
												 0,
												 &build_blocks,
												 &build_pipeline_id);
	if (!found || build_pipeline_id == UINT32_MAX)
		return nullptr;

	build_pipeline = parallel_plan->get_pipeline(build_pipeline_id);
	if (build_pipeline == nullptr)
		return nullptr;
	if (runtime_out != nullptr)
		*runtime_out = scheduler->get_pipeline_runtime(build_pipeline_id);
	return build_pipeline;
}

static bool
MarkPipelineStage(ParallelPipelinePlan *parallel_plan,
				  uint32_t pipeline_id,
				  ParallelPipelineStage stage,
				  ParallelBridgeKind output_bridge,
				  bool grouped_agg)
{
	ParallelPipelineDesc *pipeline = parallel_plan->get_pipeline(pipeline_id);
	uint32_t stage_bit = (uint32_t) stage;

	if (pipeline == nullptr)
		return false;
	switch (stage)
	{
		case ParallelPipelineStage::PartialAgg:
			if ((pipeline->stage_mask & (uint32_t) ParallelPipelineStage::HashBuild) != 0 ||
				(pipeline->stage_mask & (uint32_t) ParallelPipelineStage::SortRun) != 0)
				return false;
			break;
		case ParallelPipelineStage::HashProbe:
			if ((pipeline->stage_mask & (uint32_t) ParallelPipelineStage::HashBuild) != 0 ||
				(pipeline->stage_mask & (uint32_t) ParallelPipelineStage::SortRun) != 0)
				return false;
			break;
		case ParallelPipelineStage::HashBuild:
			/*
			 * Nested HashJoin trees can produce a pipeline that first probes an
			 * already-built hash table and then becomes the build side of an
			 * upper HashJoin. Allow that specific HashProbe -> HashBuild shape,
			 * but continue treating PartialAgg/SortRun/HashBuild as breakers.
			 */
			if ((pipeline->stage_mask & (uint32_t) ParallelPipelineStage::PartialAgg) != 0 ||
				(pipeline->stage_mask & (uint32_t) ParallelPipelineStage::SortRun) != 0 ||
				(pipeline->stage_mask & (uint32_t) ParallelPipelineStage::HashBuild) != 0)
				return false;
			break;
		case ParallelPipelineStage::SortRun:
			if (pipeline->stage_mask != 0)
				return false;
			break;
	}
	pipeline->stage_mask |= stage_bit;
	pipeline->output_bridge = output_bridge;
	pipeline->grouped_agg = grouped_agg;
	return true;
}

static bool
MatchFinalizePartialAggregateChain(Plan *plan,
								   Agg **finalize_agg_out,
								   Plan **gather_out,
								   Agg **partial_agg_out)
{
	Agg *finalize_agg;
	Plan *gather_plan;
	Plan *partial_plan;

	if (finalize_agg_out != nullptr)
		*finalize_agg_out = nullptr;
	if (gather_out != nullptr)
		*gather_out = nullptr;
	if (partial_agg_out != nullptr)
		*partial_agg_out = nullptr;

	if (plan == nullptr || !IsA(plan, Agg))
		return false;

	finalize_agg = (Agg *) plan;
	if (!DO_AGGSPLIT_COMBINE(finalize_agg->aggsplit) ||
		finalize_agg->plan.lefttree == nullptr)
		return false;

	gather_plan = finalize_agg->plan.lefttree;
	if (!(IsA(gather_plan, Gather) || IsA(gather_plan, GatherMerge)) ||
		gather_plan->lefttree == nullptr)
		return false;

	partial_plan = gather_plan->lefttree;
	while (partial_plan != nullptr &&
		   (IsA(partial_plan, Sort) || IsA(partial_plan, IncrementalSort) ||
			IsA(partial_plan, Material)) &&
		   partial_plan->lefttree != nullptr)
		partial_plan = partial_plan->lefttree;
	if (!IsA(partial_plan, Agg))
		return false;

	if (!DO_AGGSPLIT_SKIPFINAL(((Agg *) partial_plan)->aggsplit))
		return false;

	if (finalize_agg_out != nullptr)
		*finalize_agg_out = finalize_agg;
	if (gather_out != nullptr)
		*gather_out = gather_plan;
	if (partial_agg_out != nullptr)
		*partial_agg_out = (Agg *) partial_plan;
	return true;
}

static PipelineLoweringResult
LowerParallelPipelinePlan(Plan *plan, PipelineLoweringContext *context)
{
	PipelineLoweringResult result;

	if (plan == nullptr)
		return result;

	if (IsA(plan, SeqScan))
	{
		SeqScan *scan = (SeqScan *) plan;
		ParallelPipelineDesc &pipeline =
			context->parallel_plan->add_pipeline(ParallelPipelineDriverKind::SourceScan);

		pipeline.role = ParallelPipelineRole::GenericSource;
		pipeline.scan_relid = LookupSeqScanRelid(scan, context->plannedstmt);
		pipeline.scan_plan_node_id = plan->plan_node_id;
		pipeline.estimated_rows = plan->plan_rows;
		pipeline.source_morsel_driven = true;
		pipeline.has_filter = plan->qual != NIL;
		pipeline.has_projection = plan->targetlist != NIL;
		result.pipeline_id = pipeline.pipeline_id;
		result.valid = (pipeline.scan_relid != InvalidOid);
		if (!result.valid)
			SetFailure(context, "parallel lowering could not resolve SeqScan relation");
		return result;
	}

	if (IsA(plan, Gather))
	{
		PipelineLoweringResult child =
			LowerParallelPipelinePlan(plan->lefttree, context);
		ParallelPipelineDesc *pipeline;

		if (!child.valid)
			return child;
		pipeline = context->parallel_plan->get_pipeline(child.pipeline_id);
		if (pipeline != nullptr && plan->targetlist != NIL)
			pipeline->has_projection = true;
		return child;
	}

	if (IsA(plan, GatherMerge))
	{
		PipelineLoweringResult child =
			LowerParallelPipelinePlan(plan->lefttree, context);
		ParallelPipelineDesc *pipeline;

		if (!child.valid)
			return child;
		pipeline = context->parallel_plan->get_pipeline(child.pipeline_id);
		if (pipeline != nullptr && plan->targetlist != NIL)
			pipeline->has_projection = true;
		return child;
	}

	if (IsA(plan, SubqueryScan))
	{
		PipelineLoweringResult child =
			LowerParallelPipelinePlan(((SubqueryScan *) plan)->subplan, context);
		ParallelPipelineDesc *pipeline;

		if (!child.valid)
			return child;
		pipeline = context->parallel_plan->get_pipeline(child.pipeline_id);
		if (pipeline != nullptr)
			pipeline->has_projection = true;
		return child;
	}

	if (IsA(plan, Material))
	{
		PipelineLoweringResult child =
			LowerParallelPipelinePlan(plan->lefttree, context);
		ParallelPipelineDesc *pipeline;

		if (!child.valid)
			return child;
		pipeline = context->parallel_plan->get_pipeline(child.pipeline_id);
		if (pipeline != nullptr)
			pipeline->has_projection = true;
		return child;
	}

	if (IsA(plan, Limit))
	{
		PipelineLoweringResult child =
			LowerParallelPipelinePlan(plan->lefttree, context);
		ParallelPipelineDesc *pipeline;

		if (!child.valid)
			return child;
		pipeline = context->parallel_plan->get_pipeline(child.pipeline_id);
		if (pipeline != nullptr)
			pipeline->has_limit = true;
		return child;
	}

	if (IsA(plan, Agg))
	{
		Agg *agg = (Agg *) plan;
		Plan *agg_input_plan = plan->lefttree;
		bool skip_presorted_groupagg_sort = false;
		PipelineLoweringResult child = PipelineLoweringResult{};
		uint32_t finalize_pipeline_id;
		ParallelPipelineDesc *finalize_pipeline = nullptr;

		if (agg->aggsplit != AGGSPLIT_SIMPLE)
		{
			Agg *partial_agg = nullptr;
			Plan *gather_plan = nullptr;
			Plan *canonical_plan = nullptr;
			const char *canonical_failure_reason = nullptr;

			if (MatchFinalizePartialAggregateChain(plan, nullptr, &gather_plan, &partial_agg) &&
				(canonical_plan =
					 TryCanonicalizeFinalizePartialAggregate(agg,
															 gather_plan,
															 partial_agg,
															 &canonical_failure_reason)) != nullptr)
				return LowerParallelPipelinePlan(canonical_plan, context);
			if (MatchFinalizePartialAggregateChain(plan, nullptr, &gather_plan, &partial_agg))
				SetFailure(context,
						   psprintf("parallel lowering could not canonicalize FinalizeAgg <- %s <- PartialAgg (%s)",
								   IsA(gather_plan, GatherMerge) ? "GatherMerge" : "Gather",
								   canonical_failure_reason != nullptr ? canonical_failure_reason : "no reason recorded"));
			else
				SetFailure(context,
						   psprintf("parallel lowering only supports AGGSPLIT_SIMPLE aggregates (aggsplit=%d)",
								   (int) agg->aggsplit));
			return result;
		}

		/*
		 * pg_volvec executes sorted GroupAggregate shapes via hash/grouped
		 * aggregation plus a post-agg VecSortState. For parallel lowering,
		 * do not treat the planner's pre-group Sort as a breaker; lower the
		 * underlying child directly so the aggregate source pipeline remains
		 * morsel-driven and eligible for worker execution.
		 */
		if (MatchPresortedGroupAggInputChain(agg,
											&agg_input_plan,
											nullptr))
		{
			skip_presorted_groupagg_sort = true;
		}
		child = LowerParallelPipelinePlan(agg_input_plan, context);

		if (!child.valid)
			return child;
		finalize_pipeline_id =
			context->parallel_plan->add_pipeline(ParallelPipelineDriverKind::BridgeFinalize).pipeline_id;
		finalize_pipeline = context->parallel_plan->get_pipeline(finalize_pipeline_id);
		if (finalize_pipeline == nullptr)
		{
			SetFailure(context, "parallel lowering could not materialize aggregate finalize pipeline");
			return result;
		}
		if (!MarkPipelineStage(context->parallel_plan,
							   child.pipeline_id,
							   ParallelPipelineStage::PartialAgg,
							   ParallelBridgeKind::Aggregate,
							   agg->numCols > 0))
		{
			SetFailure(context, "parallel lowering found incompatible pipeline stages before Agg");
			return result;
		}
		if (auto *child_pipeline = context->parallel_plan->get_pipeline(child.pipeline_id))
			child_pipeline->agg_plan_node_id = plan->plan_node_id;
		finalize_pipeline->role = ParallelPipelineRole::AggFinalize;
		finalize_pipeline->input_bridge = ParallelBridgeKind::Aggregate;
		finalize_pipeline->agg_plan_node_id = plan->plan_node_id;
		finalize_pipeline->has_projection = plan->targetlist != NIL;
		finalize_pipeline->grouped_agg = agg->numCols > 0;
		if (skip_presorted_groupagg_sort)
			finalize_pipeline->has_projection = true;
		context->parallel_plan->add_dependency(finalize_pipeline_id,
											   child.pipeline_id);
		result.pipeline_id = finalize_pipeline_id;
		result.valid = true;
		return result;
	}

	if (IsA(plan, Sort) || IsA(plan, IncrementalSort))
	{
		PipelineLoweringResult child =
			LowerParallelPipelinePlan(plan->lefttree, context);
		uint32_t merge_pipeline_id;
		ParallelPipelineDesc *merge_pipeline = nullptr;

		if (!child.valid)
			return child;
		merge_pipeline_id =
			context->parallel_plan->add_pipeline(ParallelPipelineDriverKind::BridgeFinalize).pipeline_id;
		merge_pipeline = context->parallel_plan->get_pipeline(merge_pipeline_id);
		if (merge_pipeline == nullptr)
		{
			SetFailure(context, "parallel lowering could not materialize sort merge pipeline");
			return result;
		}
		if (!MarkPipelineStage(context->parallel_plan,
							   child.pipeline_id,
							   ParallelPipelineStage::SortRun,
							   ParallelBridgeKind::SortRuns,
							   false))
		{
			if (pg_volvec_trace_hooks)
			{
				ParallelPipelineDesc *child_pipeline =
					context->parallel_plan->get_pipeline(child.pipeline_id);

				elog(LOG,
					 "pg_volvec: sort lowering incompatible plan_node_id=%d child_pipeline=%u stage_mask=%u role=%s driver=%s",
					 plan->plan_node_id,
					 child.pipeline_id,
					 child_pipeline != nullptr ? child_pipeline->stage_mask : 0,
					 child_pipeline != nullptr ? ParallelPipelineRoleDebugName(child_pipeline->role) : "<null>",
					 child_pipeline != nullptr ? ParallelDriverKindDebugName(child_pipeline->driver_kind) : "<null>");
			}
			SetFailure(context, "parallel lowering found incompatible pipeline stages before Sort");
			return result;
		}
		merge_pipeline->role = ParallelPipelineRole::SortMerge;
		merge_pipeline->input_bridge = ParallelBridgeKind::SortRuns;
		merge_pipeline->has_projection = plan->targetlist != NIL;
		context->parallel_plan->add_dependency(merge_pipeline_id,
											   child.pipeline_id);
		result.pipeline_id = merge_pipeline_id;
		result.valid = true;
		return result;
	}

	if (IsA(plan, NestLoop))
	{
		NestLoop *nest_loop = (NestLoop *) plan;
		Plan *outer_plan = plan->lefttree;
		Plan *inner_plan = plan->righttree;
		PipelineLoweringResult outer_result;
		PipelineLoweringResult inner_result;
		PipelineLoweringResult build_result;
		PipelineLoweringResult probe_result;
		uint32_t build_finalize_pipeline_id;
		ParallelPipelineDesc *outer_pipeline;
		ParallelPipelineDesc *inner_pipeline;
		ParallelPipelineDesc *build_pipeline;
		ParallelPipelineDesc *probe_pipeline;
		ParallelPipelineDesc *build_finalize_pipeline;
		bool build_outer_side = false;

		if (nest_loop->join.jointype != JOIN_INNER &&
			nest_loop->join.jointype != JOIN_SEMI)
		{
			SetFailure(context, "parallel lowering currently only supports inner/semi NestLoop");
			return result;
		}
		outer_result = LowerParallelPipelinePlan(outer_plan, context);
		if (!outer_result.valid)
			return outer_result;
		inner_result = LowerParallelPipelinePlan(inner_plan, context);
		if (!inner_result.valid)
			return inner_result;
		outer_pipeline = context->parallel_plan->get_pipeline(outer_result.pipeline_id);
		inner_pipeline = context->parallel_plan->get_pipeline(inner_result.pipeline_id);
		if (outer_pipeline == nullptr || inner_pipeline == nullptr)
		{
			SetFailure(context, "parallel lowering could not resolve NestLoop input pipelines");
			return result;
		}
		build_outer_side =
			ShouldBuildOuterSideForParallelHashJoin(nest_loop->join.jointype,
													outer_plan,
													inner_plan,
													outer_pipeline,
													inner_pipeline);
		build_result = build_outer_side ? outer_result : inner_result;
		probe_result = build_outer_side ? inner_result : outer_result;
		build_finalize_pipeline_id =
			context->parallel_plan->add_pipeline(ParallelPipelineDriverKind::BridgeFinalize).pipeline_id;
		build_finalize_pipeline =
			context->parallel_plan->get_pipeline(build_finalize_pipeline_id);
		if (build_finalize_pipeline == nullptr)
		{
			SetFailure(context, "parallel lowering could not materialize NestLoop build finalize pipeline");
			return result;
		}
		if (!MarkPipelineStage(context->parallel_plan,
							   build_result.pipeline_id,
							   ParallelPipelineStage::HashBuild,
							   ParallelBridgeKind::HashBuild,
							   false))
		{
			SetFailure(context, "parallel lowering found incompatible build stages before NestLoop");
			return result;
		}
		if (!MarkPipelineStage(context->parallel_plan,
							   probe_result.pipeline_id,
							   ParallelPipelineStage::HashProbe,
							   ParallelBridgeKind::None,
							   false))
		{
			SetFailure(context, "parallel lowering found incompatible probe stages before NestLoop");
			return result;
		}
		build_pipeline = context->parallel_plan->get_pipeline(build_result.pipeline_id);
		if (build_pipeline == nullptr)
		{
			SetFailure(context, "parallel lowering could not resolve NestLoop build pipeline");
			return result;
		}
		if (build_pipeline->driver_kind != ParallelPipelineDriverKind::SourceScan &&
			build_pipeline->driver_kind != ParallelPipelineDriverKind::BridgeFinalize)
		{
			SetFailure(context,
					   "parallel lowering does not yet support this NestLoop build pipeline driver");
			return result;
		}
		build_pipeline->role = ParallelPipelineRole::HashBuildSource;
		build_pipeline->hash_join_plan_node_id = plan->plan_node_id;
		build_finalize_pipeline->role = ParallelPipelineRole::HashBuildFinalize;
		build_finalize_pipeline->hash_join_plan_node_id = plan->plan_node_id;
		build_finalize_pipeline->input_bridge = ParallelBridgeKind::HashBuild;
		build_finalize_pipeline->output_bridge = ParallelBridgeKind::HashTable;
		probe_pipeline = context->parallel_plan->get_pipeline(probe_result.pipeline_id);
		if (probe_pipeline == nullptr)
		{
			SetFailure(context, "parallel lowering could not resolve NestLoop probe pipeline");
			return result;
		}
		if (probe_pipeline->driver_kind != ParallelPipelineDriverKind::SourceScan ||
			!probe_pipeline->source_morsel_driven)
		{
			SetFailure(context,
					   "parallel lowering does not yet support NestLoop probe input produced by a bridge");
			return result;
		}
		probe_pipeline->role = ParallelPipelineRole::HashProbeSource;
		probe_pipeline->hash_join_plan_node_id = plan->plan_node_id;
		probe_pipeline->input_bridge = ParallelBridgeKind::HashTable;
		probe_pipeline->input_hash_join_plan_node_id = plan->plan_node_id;
		probe_pipeline->has_filter =
			probe_pipeline->has_filter ||
			nest_loop->join.joinqual != NIL ||
			nest_loop->join.plan.qual != NIL;
		context->parallel_plan->add_dependency(build_finalize_pipeline_id,
											   build_result.pipeline_id);
		context->parallel_plan->add_dependency(probe_result.pipeline_id,
											   build_finalize_pipeline_id);
		return probe_result;
	}

	if (IsA(plan, HashJoin))
	{
		HashJoin *hash_join = (HashJoin *) plan;
		Hash *hash_node;
		Plan *outer_plan;
		Plan *inner_plan;
		PipelineLoweringResult outer_result;
		PipelineLoweringResult inner_result;
		PipelineLoweringResult build_result;
		PipelineLoweringResult probe_result;
		uint32_t build_finalize_pipeline_id;
		ParallelPipelineDesc *outer_pipeline;
		ParallelPipelineDesc *inner_pipeline;
		ParallelPipelineDesc *build_pipeline;
		ParallelPipelineDesc *probe_pipeline;
		ParallelPipelineDesc *build_finalize_pipeline;
		bool build_outer_side = false;

		if (!IsA(plan->righttree, Hash))
		{
			SetFailure(context, "parallel lowering requires HashJoin right tree to be Hash");
			return result;
		}
		if (hash_join->join.jointype != JOIN_INNER)
		{
			SetFailure(context, "parallel lowering currently only supports inner HashJoin");
			return result;
		}
		hash_node = (Hash *) plan->righttree;
		outer_plan = plan->lefttree;
		inner_plan = hash_node->plan.lefttree;
		outer_result = LowerParallelPipelinePlan(outer_plan, context);
		if (!outer_result.valid)
			return outer_result;
		inner_result = LowerParallelPipelinePlan(inner_plan, context);
		if (!inner_result.valid)
			return inner_result;
		outer_pipeline = context->parallel_plan->get_pipeline(outer_result.pipeline_id);
		inner_pipeline = context->parallel_plan->get_pipeline(inner_result.pipeline_id);
		if (outer_pipeline == nullptr || inner_pipeline == nullptr)
		{
			SetFailure(context, "parallel lowering could not resolve HashJoin input pipelines");
			return result;
		}
		build_outer_side =
			ShouldBuildOuterSideForParallelHashJoin(hash_join->join.jointype,
													outer_plan,
													inner_plan,
													outer_pipeline,
													inner_pipeline);
		build_result = build_outer_side ? outer_result : inner_result;
		probe_result = build_outer_side ? inner_result : outer_result;
		build_finalize_pipeline_id =
			context->parallel_plan->add_pipeline(ParallelPipelineDriverKind::BridgeFinalize).pipeline_id;
		build_finalize_pipeline =
			context->parallel_plan->get_pipeline(build_finalize_pipeline_id);
		if (build_finalize_pipeline == nullptr)
		{
			SetFailure(context, "parallel lowering could not materialize HashBuildFinalize pipeline");
			return result;
		}

		if (!MarkPipelineStage(context->parallel_plan,
							   build_result.pipeline_id,
							   ParallelPipelineStage::HashBuild,
							   ParallelBridgeKind::HashBuild,
							   false))
		{
			SetFailure(context, "parallel lowering found incompatible build stages before HashJoin");
			return result;
		}
		if (!MarkPipelineStage(context->parallel_plan,
							   probe_result.pipeline_id,
							   ParallelPipelineStage::HashProbe,
							   ParallelBridgeKind::None,
							   false))
		{
			SetFailure(context, "parallel lowering found incompatible probe stages before HashJoin");
			return result;
		}
		build_pipeline = context->parallel_plan->get_pipeline(build_result.pipeline_id);
		if (build_pipeline == nullptr)
		{
			SetFailure(context, "parallel lowering could not resolve HashJoin build pipeline");
			return result;
		}
		if (build_pipeline->driver_kind != ParallelPipelineDriverKind::SourceScan &&
			build_pipeline->driver_kind != ParallelPipelineDriverKind::BridgeFinalize)
		{
			SetFailure(context,
					   "parallel lowering does not yet support this HashJoin build pipeline driver");
			return result;
		}
		if (build_pipeline != nullptr)
			build_pipeline->role = ParallelPipelineRole::HashBuildSource;
		if (build_pipeline != nullptr)
			build_pipeline->hash_join_plan_node_id = plan->plan_node_id;
		build_finalize_pipeline->role = ParallelPipelineRole::HashBuildFinalize;
		build_finalize_pipeline->hash_join_plan_node_id = plan->plan_node_id;
		build_finalize_pipeline->input_bridge = ParallelBridgeKind::HashBuild;
		build_finalize_pipeline->output_bridge = ParallelBridgeKind::HashTable;
		probe_pipeline = context->parallel_plan->get_pipeline(probe_result.pipeline_id);
		if (probe_pipeline == nullptr)
		{
			SetFailure(context, "parallel lowering could not resolve HashJoin probe pipeline");
			return result;
		}
		if (probe_pipeline->driver_kind != ParallelPipelineDriverKind::SourceScan ||
			!probe_pipeline->source_morsel_driven)
		{
			SetFailure(context,
					   "parallel lowering does not yet support HashJoin probe input produced by a bridge");
			return result;
		}
		probe_pipeline->role = ParallelPipelineRole::HashProbeSource;
		probe_pipeline->hash_join_plan_node_id = plan->plan_node_id;
		probe_pipeline->input_bridge = ParallelBridgeKind::HashTable;
		probe_pipeline->input_hash_join_plan_node_id = plan->plan_node_id;
		probe_pipeline->has_filter =
			probe_pipeline->has_filter ||
			hash_join->join.joinqual != NIL ||
			hash_join->join.plan.qual != NIL;
		context->parallel_plan->add_dependency(build_finalize_pipeline_id,
											   build_result.pipeline_id);
		context->parallel_plan->add_dependency(probe_result.pipeline_id,
											   build_finalize_pipeline_id);
		return probe_result;
	}

	SetFailure(context, "parallel lowering does not yet support this plan node");
	return result;
}

} /* namespace */

std::unique_ptr<ParallelPipelinePlan>
BuildParallelPipelinePlan(Plan *plan,
						  PlannedStmt *plannedstmt,
						  EState *estate,
						  const char **failure_reason)
{
	MemoryContext context = CurrentMemoryContext;
	std::unique_ptr<ParallelPipelinePlan> parallel_plan;
	PipelineLoweringContext lowering_context;
	PipelineLoweringResult root_result;

	if (failure_reason != nullptr)
		*failure_reason = nullptr;
	(void) estate;

	parallel_plan = std::make_unique<ParallelPipelinePlan>(context);
	lowering_context.plannedstmt = plannedstmt;
	lowering_context.parallel_plan = parallel_plan.get();
	lowering_context.failure_reason = failure_reason;
	root_result = LowerParallelPipelinePlan(plan, &lowering_context);
	if (!root_result.valid)
		return nullptr;

	parallel_plan->set_root_pipeline(root_result.pipeline_id);
	return parallel_plan;
}

std::unique_ptr<ParallelSchedulerState>
BuildParallelSchedulerState(const ParallelPipelinePlan *plan,
							MemoryContext context,
							uint32_t source_morsel_nblocks,
							const char **failure_reason)
{
	auto scheduler = std::make_unique<ParallelSchedulerState>(context, plan, source_morsel_nblocks);
	bool has_eligible_parallel_source = false;
	size_t source_pipeline_count = 0;

	if (failure_reason != nullptr)
		*failure_reason = nullptr;
	if (plan == nullptr)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel scheduler requires a pipeline plan";
		return nullptr;
	}
	if (plan->pipeline_count() == 0)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel scheduler requires at least one pipeline";
		return nullptr;
	}

	for (const auto &pipeline : plan->pipelines())
	{
		ParallelPipelineRuntimeState runtime;

		runtime.pipeline_id = pipeline.pipeline_id;
		runtime.remaining_dependencies = (uint32_t) pipeline.dependencies.size();
		runtime.next_task_kind =
			(pipeline.driver_kind == ParallelPipelineDriverKind::SourceScan)
				? ParallelTaskKind::SourceMorsel
				: ParallelTaskKind::BridgeFinalize;
		runtime.scan_relid = pipeline.scan_relid;
		runtime.scan_plan_node_id = pipeline.scan_plan_node_id;
		runtime.estimated_rows = pipeline.estimated_rows;
		runtime.next_morsel_block =
			pipeline.source_morsel_driven ? 0 : InvalidBlockNumber;
		if (pipeline.source_morsel_driven)
		{
			source_pipeline_count++;
			runtime.total_blocks = LookupRelationBlocks(pipeline.scan_relid);
			if (runtime.total_blocks == InvalidBlockNumber)
			{
				if (failure_reason != nullptr)
					*failure_reason = "parallel scheduler could not open source relation";
				return nullptr;
			}
			runtime.estimated_morsels =
				(uint32_t) ((runtime.total_blocks +
							 pg_volvec_parallel_morsel_nblocks - 1) /
							pg_volvec_parallel_morsel_nblocks);
			if (runtime.estimated_morsels == 0)
				runtime.estimated_morsels = 1;
			if (runtime.total_blocks >= (BlockNumber) pg_volvec_parallel_min_relation_blocks)
				has_eligible_parallel_source = true;
		}
		scheduler->append_pipeline_runtime(runtime);

		if (pipeline.output_bridge != ParallelBridgeKind::None)
		{
			ParallelBridgeState bridge;

			bridge.bridge_kind = pipeline.output_bridge;
			bridge.producer_pipeline_id = pipeline.pipeline_id;
			scheduler->append_bridge(bridge);
		}
	}

	for (const auto &pipeline : plan->pipelines())
	{
		if (pipeline.dependencies.empty())
			scheduler->enqueue_ready_pipeline(pipeline.pipeline_id);
	}

	if (source_pipeline_count > 0 && !has_eligible_parallel_source)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel scheduler found no source relation above min_relation_blocks";
		return nullptr;
	}

	return scheduler;
}

static void
CleanupLocalParallelAggregateProcessState(LocalParallelAggregateProcessState *local)
{
	if (local == nullptr)
		return;
	if (local->root_plan != nullptr)
	{
		delete local->root_plan;
		local->root_plan = nullptr;
	}
	if (local->estate != nullptr)
	{
		if (local->estate->es_snapshot != InvalidSnapshot)
			UnregisterSnapshot(local->estate->es_snapshot);
		FreeExecutorState(local->estate);
		local->estate = nullptr;
	}
	if (local->memory_context != nullptr)
	{
		MemoryContextDelete(local->memory_context);
		local->memory_context = nullptr;
	}
	*local = LocalParallelAggregateProcessState{};
}

static void
CleanupLocalParallelAggregateProcessStateOnExit(int code, Datum arg)
{
	LocalParallelAggregateProcessState *local;
#ifdef USE_LLVM
	size_t orphaned_jit_contexts = 0;
#endif

	(void) code;
	local = (LocalParallelAggregateProcessState *) DatumGetPointer(arg);
	if (local == nullptr)
		return;

	/*
	 * Avoid full executor/scan teardown during before_shmem_exit. At that
	 * point PostgreSQL may already be unwinding AIO/read_stream state, and a
	 * full root_plan delete can recurse into heap_endscan()/read_stream
	 * teardown and raise a second FATAL. We only need JIT context accounting
	 * to reach zero before llvm_shutdown().
	 */
	if (local->root_plan != nullptr)
	{
		if (pg_volvec_trace_hooks)
			elog(LOG,
				 "pg_volvec: proc-exit cleanup releasing worker JIT resources pid=%d root_plan=%p",
				 MyProcPid,
				 (void *) local->root_plan);
		local->root_plan->release_jit_resources_for_proc_exit();
		local->root_plan = nullptr;
		local->worker_context.root_plan = nullptr;
		local->worker_context.agg_state = nullptr;
		local->worker_context.hash_join_state = nullptr;
		if (pg_volvec_trace_hooks)
			elog(LOG,
				 "pg_volvec: proc-exit cleanup finished releasing worker JIT resources pid=%d",
				 MyProcPid);
	}
	else if (pg_volvec_trace_hooks)
		elog(LOG,
			 "pg_volvec: proc-exit cleanup saw no root plan pid=%d",
			 MyProcPid);

#ifdef USE_LLVM
	orphaned_jit_contexts =
		pg_volvec_release_all_registered_llvm_jit_contexts_for_proc_exit();
	if (pg_volvec_trace_hooks && orphaned_jit_contexts > 0)
		elog(LOG,
			 "pg_volvec: proc-exit cleanup released %zu orphaned JIT context(s) pid=%d",
			 orphaned_jit_contexts,
			 MyProcPid);
#endif
}

static bool
TryInitializeLocalParallelAggregateProcessState(const char *plannedstmt_serialized,
												 const char *query_text,
												 int agg_plan_node_id,
												 int hash_join_plan_node_id,
												 int input_hash_join_plan_node_id,
												 bool require_agg_state,
												 bool need_hash_join_state,
												 const uint8_t *shared_hash_bridge,
												 size_t shared_hash_bridge_size,
												 Oid source_scan_relid,
												 int source_scan_plan_node_id,
												 ParallelTableScanDesc parallel_scan_desc,
												 bool leader,
	LocalParallelAggregateProcessState *local,
												 const char **failure_reason)
{
	MemoryContext worker_context;
	MemoryContext old_context;
	std::unique_ptr<VecPlanState> root_plan;
	Plan *local_init_plan;
	VecAggState *agg_state;
	instr_time init_start;
	instr_time init_end;

	if (failure_reason != nullptr)
		*failure_reason = nullptr;
	if (local != nullptr)
		*local = LocalParallelAggregateProcessState{};
	if (plannedstmt_serialized == nullptr || local == nullptr)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel worker missing serialized planned statement";
		return false;
	}

	INSTR_TIME_SET_CURRENT(init_start);
	worker_context = AllocSetContextCreate(CurrentMemoryContext,
										   "pg_volvec parallel worker",
										   ALLOCSET_DEFAULT_SIZES);
	old_context = MemoryContextSwitchTo(worker_context);
	local->memory_context = worker_context;
	local->plannedstmt = (PlannedStmt *) stringToNode(plannedstmt_serialized);
	local->query_text = pstrdup(query_text != nullptr ? query_text : "");
	local->estate = CreateExecutorState();
	if (local->plannedstmt->paramExecTypes != NIL)
	{
		int n_param_exec = list_length(local->plannedstmt->paramExecTypes);

		local->estate->es_param_exec_vals =
			(ParamExecData *) palloc0_array(ParamExecData, n_param_exec);
	}
	local->estate->es_snapshot = RegisterSnapshot(GetActiveSnapshot());
	local->estate->es_sourceText = local->query_text;
	local->estate->es_plannedstmt = local->plannedstmt;
	ExecInitRangeTable(local->estate,
					   local->plannedstmt->rtable,
					   local->plannedstmt->permInfos,
					   local->plannedstmt->unprunableRelids);

	local->worker_context.memory_context = worker_context;
	local->worker_context.plannedstmt = local->plannedstmt;
	local->worker_context.estate = local->estate;
	local->worker_context.agg_plan_node_id = agg_plan_node_id;
	local->worker_context.hash_join_plan_node_id = hash_join_plan_node_id;
	local->worker_context.input_hash_join_plan_node_id = input_hash_join_plan_node_id;
	local->worker_context.parallel_scan_relid = source_scan_relid;
	local->worker_context.parallel_scan_plan_node_id = source_scan_plan_node_id;
	local->worker_context.parallel_scan_desc = parallel_scan_desc;
	local->worker_context.leader = leader;
	if (pg_volvec_trace_hooks)
		elog(LOG,
			 "pg_volvec: parallel %s local init binding rel=%u plan_node_id=%d agg_plan_node_id=%d hash_join_plan_node_id=%d input_hash_join_plan_node_id=%d require_agg=%s need_hash=%s",
			 leader ? "leader" : "worker",
			 source_scan_relid,
			 source_scan_plan_node_id,
			 agg_plan_node_id,
			 hash_join_plan_node_id,
			 input_hash_join_plan_node_id,
			 require_agg_state ? "true" : "false",
			 need_hash_join_state ? "true" : "false");

	local_init_plan = local->plannedstmt->planTree;

	root_plan = ExecInitVecPlan(local_init_plan,
								local->estate,
								&local->worker_context);
	if (!root_plan &&
		local_init_plan != local->plannedstmt->planTree)
	{
		if (pg_volvec_trace_hooks)
			elog(LOG,
				 "pg_volvec: parallel %s local init hash-build subtree failed, retrying full root",
				 leader ? "leader" : "worker");
		root_plan = ExecInitVecPlan(local->plannedstmt->planTree,
									local->estate,
									&local->worker_context);
	}
	if (!root_plan)
	{
		MemoryContextSwitchTo(old_context);
		CleanupLocalParallelAggregateProcessState(local);
		if (failure_reason != nullptr)
			*failure_reason = "parallel worker could not initialize VecPlanState";
		return false;
	}

	agg_state = nullptr;
	if (require_agg_state)
	{
		if (agg_plan_node_id >= 0)
			agg_state = root_plan->find_parallel_aggregate_state_by_plan_node_id(agg_plan_node_id);
		else
			agg_state = root_plan->find_parallel_aggregate_state();
		if (agg_state == nullptr || !agg_state->supports_parallel_partial_state())
		{
			MemoryContextSwitchTo(old_context);
			CleanupLocalParallelAggregateProcessState(local);
			if (failure_reason != nullptr)
				*failure_reason = "parallel worker requires partial aggregate support";
			return false;
		}
	}

	local->worker_context.root_plan = root_plan.get();
	local->worker_context.agg_state = agg_state;
	if (need_hash_join_state)
	{
		if (hash_join_plan_node_id >= 0)
			local->worker_context.hash_join_state =
				root_plan->find_parallel_hash_join_state_by_plan_node_id(hash_join_plan_node_id);
		else
			local->worker_context.hash_join_state = root_plan->find_parallel_hash_join_state();
		if (local->worker_context.hash_join_state == nullptr)
		{
			MemoryContextSwitchTo(old_context);
			CleanupLocalParallelAggregateProcessState(local);
			if (failure_reason != nullptr)
				*failure_reason = "parallel worker requires hash join state";
			return false;
		}
	}
	if (shared_hash_bridge != nullptr && shared_hash_bridge_size > 0)
	{
		VecHashJoinState *input_hash_join_state = nullptr;

		if (input_hash_join_plan_node_id >= 0)
			input_hash_join_state =
				root_plan->find_parallel_hash_join_state_by_plan_node_id(
					input_hash_join_plan_node_id);
		if (input_hash_join_state == nullptr)
			input_hash_join_state = local->worker_context.hash_join_state;
		if (input_hash_join_state == nullptr)
		{
			MemoryContextSwitchTo(old_context);
			CleanupLocalParallelAggregateProcessState(local);
			if (failure_reason != nullptr)
				*failure_reason = "parallel worker requires input hash join state for shared bridge";
			return false;
		}
		input_hash_join_state->attach_shared_hash_bridge(shared_hash_bridge,
														 shared_hash_bridge_size);
		input_hash_join_state->load_hash_bridge();
		if (pg_volvec_trace_hooks)
			elog(LOG,
				 "pg_volvec: parallel %s attached shared hash bridge bytes=%zu entries=%zu chunks=%zu input_hash_join_plan_node_id=%d",
				 leader ? "leader" : "worker",
				 shared_hash_bridge_size,
				 input_hash_join_state->parallel_hash_entry_count(),
				 input_hash_join_state->parallel_hash_chunk_count(),
				 input_hash_join_plan_node_id);
	}
	local->root_plan = root_plan.release();
	INSTR_TIME_SET_CURRENT(init_end);
	INSTR_TIME_SUBTRACT(init_end, init_start);
	local->init_time_us = (uint64) INSTR_TIME_GET_MICROSEC(init_end);
	MemoryContextSwitchTo(old_context);
	return true;
}

static bool
ExecuteParallelWorkerSourceLoop(ParallelWorkerExecutionMode mode,
								ParallelWorkerContext &worker_context,
								const char **failure_reason)
{
	if (failure_reason != nullptr)
		*failure_reason = nullptr;

	switch (mode)
	{
		case ParallelWorkerExecutionMode::AggregateProbe:
			if (worker_context.agg_state == nullptr)
			{
				if (failure_reason != nullptr)
					*failure_reason = "parallel source pipeline loop requires aggregate state";
				return false;
			}
			worker_context.agg_state->consume_left_input();
			worker_context.agg_state->finish_sink();
			return true;

		case ParallelWorkerExecutionMode::HashBuild:
			if (worker_context.hash_join_state == nullptr)
			{
				if (failure_reason != nullptr)
					*failure_reason = "parallel source pipeline loop requires hash join state";
				return false;
			}
			worker_context.hash_join_state->consume_build_input();
			return true;
	}
	if (failure_reason != nullptr)
		*failure_reason = "parallel source pipeline loop saw unknown execution mode";
	return false;
}

static void
PopulatePartialDiagnostics(const LocalParallelAggregateProcessState &local,
						   ParallelAggPartialState *partial)
{
	VecSeqScanState *scan_state = nullptr;

	if (partial == nullptr)
		return;
	partial->init_time_us = local.init_time_us;
	partial->exec_time_us = local.exec_time_us;
	if (local.worker_context.agg_state != nullptr)
	{
		partial->input_batches = local.worker_context.agg_state->input_batches_consumed();
		partial->input_rows = local.worker_context.agg_state->input_rows_consumed();
	}
	if (local.worker_context.root_plan != nullptr)
		scan_state = local.worker_context.root_plan->find_parallel_source_scan_state();
	if (scan_state != nullptr)
		partial->blocks_opened = scan_state->blocks_opened();
}

static void
PopulateHashBuildPartialDiagnostics(const LocalParallelAggregateProcessState &local,
									ParallelHashBuildPartialState *partial)
{
	VecSeqScanState *scan_state = nullptr;

	if (partial == nullptr)
		return;
	partial->init_time_us = local.init_time_us;
	partial->exec_time_us = local.exec_time_us;
	if (local.worker_context.hash_join_state != nullptr)
	{
		partial->input_batches =
			local.worker_context.hash_join_state->build_input_batches_consumed();
		partial->input_rows =
			local.worker_context.hash_join_state->build_input_rows_consumed();
		partial->entry_count =
			local.worker_context.hash_join_state->parallel_hash_entry_count();
		partial->chunk_count =
			local.worker_context.hash_join_state->parallel_hash_chunk_count();
	}
	if (local.worker_context.root_plan != nullptr)
		scan_state = local.worker_context.root_plan->find_parallel_source_scan_state();
	if (scan_state != nullptr)
		partial->blocks_opened = scan_state->blocks_opened();
}

static bool
TryInitializeParallelMergeContext(PgVolVecQueryState *query_state,
								  int agg_plan_node_id,
								  int hash_join_plan_node_id,
								  bool need_hash_join_state,
								  ParallelWorkerContext *worker_context,
								  const char **failure_reason)
{
	VecAggState *agg_state = nullptr;

	if (failure_reason != nullptr)
		*failure_reason = nullptr;
	if (worker_context != nullptr)
		*worker_context = ParallelWorkerContext{};

	if (query_state == nullptr ||
		query_state->vec_plan == nullptr ||
		query_state->parallel_plan == nullptr ||
		query_state->parallel_scheduler == nullptr)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel query state is incomplete";
		return false;
	}

	if (agg_plan_node_id >= 0)
		agg_state = query_state->vec_plan->find_parallel_aggregate_state_by_plan_node_id(agg_plan_node_id);
	else
		agg_state = query_state->vec_plan->find_parallel_aggregate_state();
	if (agg_state == nullptr)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel leader path requires aggregate state";
		return false;
	}

	if (worker_context != nullptr)
	{
		worker_context->memory_context = query_state->context;
		worker_context->root_plan = query_state->vec_plan;
		worker_context->agg_state = agg_state;
		worker_context->agg_plan_node_id = agg_plan_node_id;
		worker_context->hash_join_plan_node_id = hash_join_plan_node_id;
		worker_context->input_hash_join_plan_node_id = hash_join_plan_node_id;
		if (need_hash_join_state)
		{
			if (hash_join_plan_node_id >= 0)
				worker_context->hash_join_state =
					query_state->vec_plan->find_parallel_hash_join_state_by_plan_node_id(
						hash_join_plan_node_id);
			else
				worker_context->hash_join_state =
					query_state->vec_plan->find_parallel_hash_join_state();
			if (worker_context->hash_join_state == nullptr)
			{
				if (failure_reason != nullptr)
					*failure_reason = "parallel leader path requires hash join state";
				return false;
			}
		}
		worker_context->leader = true;
	}
	return true;
}

bool
TryInitializeLeaderOnlyAggregateWorkerContext(PgVolVecQueryState *query_state,
												   ParallelWorkerContext *worker_context,
												   const ParallelPipelineDesc **source_pipeline_out,
												   const char **failure_reason)
{
	const ParallelPipelineDesc *source_pipeline = nullptr;

	if (source_pipeline_out != nullptr)
		*source_pipeline_out = nullptr;
	if (!TryInitializeParallelMergeContext(query_state, -1, -1, false, worker_context, failure_reason))
		return false;

	for (const auto &pipeline : query_state->parallel_plan->pipelines())
	{
		if (!IsAggregateSourcePipeline(pipeline))
			continue;
		if (source_pipeline != nullptr)
		{
			if (failure_reason != nullptr)
				*failure_reason =
					"parallel leader path currently requires exactly one aggregate source pipeline";
			return false;
		}
		source_pipeline = &pipeline;
	}

	if (source_pipeline == nullptr)
	{
		if (failure_reason != nullptr)
			*failure_reason =
				"parallel leader path found no aggregate source pipeline";
		return false;
	}
	if (source_pipeline->driver_kind != ParallelPipelineDriverKind::SourceScan)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel leader path requires a source-scan driver";
		return false;
	}
	if (source_pipeline_out != nullptr)
		*source_pipeline_out = source_pipeline;
	return true;
}

bool
ExecuteParallelTask(const ParallelTaskDesc &task,
					 const ParallelPipelinePlan *parallel_plan,
					 ParallelWorkerContext &worker_context,
					 const char **failure_reason)
{
	const ParallelPipelineDesc *pipeline = nullptr;

	if (failure_reason != nullptr)
		*failure_reason = nullptr;
	if (parallel_plan == nullptr)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel task execution requires a pipeline plan";
		return false;
	}

	pipeline = parallel_plan->get_pipeline(task.pipeline_id);
	if (pipeline == nullptr)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel task referenced an unknown pipeline";
		return false;
	}

	switch (task.task_kind)
	{
		case ParallelTaskKind::SourceMorsel:
			if (pipeline->driver_kind != ParallelPipelineDriverKind::SourceScan)
			{
				if (failure_reason != nullptr)
					*failure_reason = "source morsel task does not target a source pipeline";
				return false;
			}
			if (task.morsel_start_block == InvalidBlockNumber || task.morsel_nblocks == 0)
				return true;
			if (pipeline->role == ParallelPipelineRole::HashBuildSource &&
				worker_context.hash_join_state != nullptr)
			{
				if (!worker_context.hash_join_state->configure_build_input_block_range(task.morsel_start_block,
																				 task.morsel_nblocks))
				{
					if (failure_reason != nullptr)
						*failure_reason = "hash build source task could not configure block range";
					return false;
				}
				worker_context.hash_join_state->consume_build_input();
				worker_context.hash_join_state->clear_build_input_block_range();
				return true;
			}
			if (pipeline->output_bridge == ParallelBridgeKind::Aggregate &&
				(pipeline->stage_mask & (uint32_t) ParallelPipelineStage::PartialAgg) != 0 &&
				worker_context.agg_state != nullptr)
			{
				if (!worker_context.agg_state->configure_input_block_range(task.morsel_start_block,
																		  task.morsel_nblocks))
				{
					if (failure_reason != nullptr)
						*failure_reason = "aggregate source task could not configure block range";
					return false;
				}
				worker_context.agg_state->consume_left_input();
				worker_context.agg_state->clear_input_block_range();
				return true;
			}
			if (failure_reason != nullptr)
				*failure_reason = psprintf("source morsel pipeline kernel not implemented (pipeline=%u role=%s driver=%s output_bridge=%u stage_mask=%u)",
										   pipeline->pipeline_id,
										   ParallelPipelineRoleDebugName(pipeline->role),
										   ParallelDriverKindDebugName(pipeline->driver_kind),
										   (unsigned) pipeline->output_bridge,
										   pipeline->stage_mask);
			return false;

		case ParallelTaskKind::BridgeFinalize:
			if (pipeline->role == ParallelPipelineRole::HashBuildFinalize &&
				worker_context.hash_join_state != nullptr)
			{
			worker_context.hash_join_state->publish_hash_bridge();
				worker_context.hash_join_state->finish_parallel_hash_build();
				return true;
			}
			if (pipeline->role == ParallelPipelineRole::AggFinalize &&
				worker_context.agg_state != nullptr)
			{
				worker_context.agg_state->finish_sink();
				return true;
			}
			if (failure_reason != nullptr)
				*failure_reason = psprintf("bridge finalize task kernel not implemented (pipeline=%u role=%s driver=%s input_bridge=%u output_bridge=%u)",
										   pipeline->pipeline_id,
										   ParallelPipelineRoleDebugName(pipeline->role),
										   ParallelDriverKindDebugName(pipeline->driver_kind),
										   (unsigned) pipeline->input_bridge,
										   (unsigned) pipeline->output_bridge);
			return false;
	}

	if (failure_reason != nullptr)
		*failure_reason = "unknown parallel task kind";
	return false;
}

static bool
TryExecuteParallelHashBuildPhase(PgVolVecQueryState *query_state,
								 QueryDesc *queryDesc,
								 const ParallelPipelineDesc *probe_pipeline,
								 ParallelWorkerContext &merge_context,
								 const char **failure_reason)
{
	const ParallelPipelineRuntimeState *build_runtime = nullptr;
	const ParallelPipelineDesc *build_pipeline = nullptr;
	const char *plannedstmt_serialized = nullptr;
	const char *query_text = nullptr;
	char *plannedstmt_serialized_owned = nullptr;
	Relation source_rel = nullptr;
	size_t source_pscan_len = 0;
	size_t plannedstmt_len = 0;
	size_t query_text_len = 0;
	size_t build_partials_len = 0;
	ParallelContext *pcxt = nullptr;
	ParallelAggregateSharedControl *control = nullptr;
	ParallelTableScanDesc source_pscan = nullptr;
	ParallelHashBuildPartialState *build_partials = nullptr;
	SharedFileSet *partial_fileset = nullptr;
	LocalParallelAggregateProcessState leader_local_state;
	ParallelHashBuildPartialState leader_partial{};
	const char *leader_failure_reason = nullptr;
	bool entered_parallel = false;
	bool leader_local_initialized = false;
	bool success = false;
	int requested_workers;
	size_t total_partial_entries = 0;
	size_t total_partial_chunks = 0;

	if (failure_reason != nullptr)
		*failure_reason = nullptr;
	if (query_state == nullptr ||
		query_state->parallel_plan == nullptr ||
		query_state->parallel_scheduler == nullptr ||
		queryDesc == nullptr ||
		probe_pipeline == nullptr ||
		merge_context.hash_join_state == nullptr)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel hash build phase has incomplete state";
		return false;
	}
	merge_context.hash_join_state->reset_parallel_hash_build_state();

	build_pipeline = FindLargestHashBuildDependency(query_state->parallel_plan,
													 query_state->parallel_scheduler,
													 probe_pipeline,
													 &build_runtime);
	if (build_pipeline == nullptr || build_runtime == nullptr)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel hash build phase found no build dependency pipeline";
		return false;
	}
	if (build_pipeline->driver_kind != ParallelPipelineDriverKind::SourceScan ||
		build_pipeline->scan_relid == InvalidOid ||
		build_pipeline->scan_plan_node_id < 0)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel hash build phase requires a source-scan build pipeline";
		return false;
	}
	if (build_runtime->total_blocks == InvalidBlockNumber || build_runtime->total_blocks == 0)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel hash build phase found no eligible build-side blocks";
		return false;
	}
	if (build_pipeline->hash_join_plan_node_id >= 0)
	{
		merge_context.hash_join_state =
			query_state->vec_plan->find_parallel_hash_join_state_by_plan_node_id(
				build_pipeline->hash_join_plan_node_id);
		merge_context.hash_join_plan_node_id = build_pipeline->hash_join_plan_node_id;
		if (merge_context.hash_join_state == nullptr)
		{
			if (failure_reason != nullptr)
				*failure_reason = "parallel hash build phase could not resolve build-side hash join state";
			return false;
		}
	}

	requested_workers = Min(pg_volvec_parallel_max_workers,
							(int) Max(build_runtime->estimated_morsels, 1u));
	if (requested_workers <= 0)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel hash build phase requested zero workers";
		return false;
	}

	query_text = queryDesc->sourceText != nullptr ? queryDesc->sourceText : "";
	plannedstmt_serialized_owned = nodeToString(queryDesc->plannedstmt);
	plannedstmt_serialized = plannedstmt_serialized_owned;
	plannedstmt_len = strlen(plannedstmt_serialized) + 1;
	query_text_len = strlen(query_text) + 1;

	source_rel = table_open(build_pipeline->scan_relid, NoLock);
	source_pscan_len = table_parallelscan_estimate(source_rel, queryDesc->estate->es_snapshot);

	EnterParallelMode();
	entered_parallel = true;
	pcxt = CreateParallelContext("pg_volvec",
								 "pg_volvec_parallel_worker_main",
								 requested_workers);

	shm_toc_estimate_chunk(&pcxt->estimator, sizeof(ParallelAggregateSharedControl));
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	shm_toc_estimate_chunk(&pcxt->estimator, plannedstmt_len);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	shm_toc_estimate_chunk(&pcxt->estimator, query_text_len);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	shm_toc_estimate_chunk(&pcxt->estimator, source_pscan_len);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	build_partials_len = mul_size(sizeof(ParallelHashBuildPartialState), pcxt->nworkers);
	shm_toc_estimate_chunk(&pcxt->estimator, build_partials_len);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	shm_toc_estimate_chunk(&pcxt->estimator, sizeof(SharedFileSet));
	shm_toc_estimate_keys(&pcxt->estimator, 1);

	InitializeParallelDSM(pcxt);
	if (pcxt->seg == nullptr)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel hash build DSM initialization failed";
		goto done;
	}

	control = (ParallelAggregateSharedControl *)
		shm_toc_allocate(pcxt->toc, sizeof(ParallelAggregateSharedControl));
	control->magic = VOLVEC_PARALLEL_MAGIC;
	control->source_pipeline_id = build_pipeline->pipeline_id;
	control->partial_slot_count = pcxt->nworkers;
	control->morsel_nblocks = pg_volvec_parallel_morsel_nblocks;
	control->total_blocks = build_runtime->total_blocks;
	control->source_scan_relid = build_pipeline->scan_relid;
	control->source_scan_plan_node_id = build_pipeline->scan_plan_node_id;
	control->agg_plan_node_id = -1;
	control->hash_join_plan_node_id = build_pipeline->hash_join_plan_node_id;
	control->input_hash_join_plan_node_id = build_pipeline->input_hash_join_plan_node_id;
	control->execution_mode = (uint32) ParallelWorkerExecutionMode::HashBuild;
	control->need_hash_join_state = true;
	control->hash_bridge_ready = 0;
	control->reserved[0] = 0;
	control->reserved[1] = 0;
	control->hash_bridge_size = 0;
	pg_atomic_init_u64(&control->next_block, 0);
	shm_toc_insert(pcxt->toc, VOLVEC_PARALLEL_KEY_CONTROL, control);

	{
		char *shared_plannedstmt = (char *) shm_toc_allocate(pcxt->toc, plannedstmt_len);
		char *shared_query_text = (char *) shm_toc_allocate(pcxt->toc, query_text_len);

		memcpy(shared_plannedstmt, plannedstmt_serialized, plannedstmt_len);
		memcpy(shared_query_text, query_text, query_text_len);
		shm_toc_insert(pcxt->toc, VOLVEC_PARALLEL_KEY_PLANNEDSTMT, shared_plannedstmt);
		shm_toc_insert(pcxt->toc, VOLVEC_PARALLEL_KEY_QUERY_TEXT, shared_query_text);
	}

	source_pscan = (ParallelTableScanDesc) shm_toc_allocate(pcxt->toc, source_pscan_len);
	table_parallelscan_initialize(source_rel, source_pscan, queryDesc->estate->es_snapshot);
	shm_toc_insert(pcxt->toc, VOLVEC_PARALLEL_KEY_SOURCE_PSCAN, source_pscan);

	build_partials = (ParallelHashBuildPartialState *)
		shm_toc_allocate(pcxt->toc, build_partials_len);
	memset(build_partials, 0, build_partials_len);
	shm_toc_insert(pcxt->toc, VOLVEC_PARALLEL_KEY_HASH_BUILD_PARTIALS, build_partials);

	partial_fileset = (SharedFileSet *) shm_toc_allocate(pcxt->toc, sizeof(SharedFileSet));
	SharedFileSetInit(partial_fileset, pcxt->seg);
	shm_toc_insert(pcxt->toc, VOLVEC_PARALLEL_KEY_PARTIAL_FILESET, partial_fileset);
	table_close(source_rel, NoLock);
	source_rel = nullptr;

	LaunchParallelWorkers(pcxt);
	if (pg_volvec_trace_hooks)
		elog(LOG,
			 "pg_volvec: launched %d/%d parallel workers for hash build pipeline %u",
			 pcxt->nworkers_launched,
			 pcxt->nworkers,
			 build_pipeline->pipeline_id);
	if (pcxt->nworkers_launched == 0 && !pg_volvec_parallel_leader_participation)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel hash build launched no workers and leader participation is disabled";
		goto done;
	}

	if (pg_volvec_parallel_leader_participation)
	{
		instr_time leader_exec_start;
		instr_time leader_exec_end;
		BufFile *file;
		char file_name[VOLVEC_PARALLEL_MAX_PARTIAL_FILE_NAME];

		if (!TryInitializeLocalParallelAggregateProcessState(plannedstmt_serialized,
															 query_text,
															 -1,
															 build_pipeline->hash_join_plan_node_id,
															 build_pipeline->input_hash_join_plan_node_id,
															 false,
															 true,
															 nullptr,
															 0,
															 build_pipeline->scan_relid,
															 build_pipeline->scan_plan_node_id,
															 source_pscan,
															 true,
															 &leader_local_state,
															 &leader_failure_reason))
		{
			if (failure_reason != nullptr)
				*failure_reason = leader_failure_reason != nullptr ?
					leader_failure_reason :
					"parallel hash build leader local init failed";
			goto done;
		}
		leader_local_initialized = true;
		INSTR_TIME_SET_CURRENT(leader_exec_start);
		if (!ExecuteParallelWorkerSourceLoop(ParallelWorkerExecutionMode::HashBuild,
											 leader_local_state.worker_context,
											 failure_reason))
			goto done;
		INSTR_TIME_SET_CURRENT(leader_exec_end);
		INSTR_TIME_SUBTRACT(leader_exec_end, leader_exec_start);
		leader_local_state.exec_time_us = (uint64) INSTR_TIME_GET_MICROSEC(leader_exec_end);

		FormatParallelPartialFileName(file_name, sizeof(file_name), "build_leader", 0);
		file = BufFileCreateFileSet(&partial_fileset->fs, file_name);
		if (file == nullptr ||
			!leader_local_state.worker_context.hash_join_state->export_parallel_build_partial_file(
				file,
				&leader_partial))
		{
			if (file != nullptr)
				BufFileClose(file);
			if (failure_reason != nullptr)
				*failure_reason = "parallel hash build leader partial export failed";
			goto done;
		}
		strlcpy(leader_partial.file_name, file_name, sizeof(leader_partial.file_name));
		BufFileExportFileSet(file);
		BufFileClose(file);
		PopulateHashBuildPartialDiagnostics(leader_local_state, &leader_partial);
	}

	WaitForParallelWorkersToFinish(pcxt);
	if (leader_local_initialized)
	{
		total_partial_entries += (size_t) leader_partial.entry_count;
		total_partial_chunks += (size_t) leader_partial.chunk_count;
	}
	for (int i = 0; i < pcxt->nworkers_launched; i++)
	{
		total_partial_entries += (size_t) build_partials[i].entry_count;
		total_partial_chunks += (size_t) build_partials[i].chunk_count;
	}
	if (pg_volvec_trace_hooks)
		elog(LOG,
			 "pg_volvec: parallel hash build merge reserve entries=%zu chunks=%zu current_entries=%zu current_chunks=%zu",
			 total_partial_entries,
			 total_partial_chunks,
			 merge_context.hash_join_state->parallel_hash_entry_count(),
			 merge_context.hash_join_state->parallel_hash_chunk_count());
	merge_context.hash_join_state->reserve_parallel_hash_build_capacity(
		total_partial_entries,
		total_partial_chunks);

	if (leader_local_initialized)
	{
		BufFile *file = BufFileOpenFileSet(&partial_fileset->fs,
											 leader_partial.file_name,
											 O_RDONLY,
											 false);

		if (file == nullptr ||
			!merge_context.hash_join_state->merge_parallel_build_partial_file(file, leader_partial))
		{
			if (file != nullptr)
				BufFileClose(file);
			if (failure_reason != nullptr)
				*failure_reason = "parallel hash build leader partial merge failed";
			goto done;
		}
		BufFileClose(file);
		BufFileDeleteFileSet(&partial_fileset->fs, leader_partial.file_name, true);
	}

	for (int i = 0; i < pcxt->nworkers_launched; i++)
	{
		BufFile *file;

		if (pg_volvec_trace_hooks)
			elog(LOG,
				 "pg_volvec: parallel hash build worker[%d] partial init_us=%llu exec_us=%llu blocks=%llu batches=%llu rows=%llu entries=%llu chunks=%llu",
				 i,
				 (unsigned long long) build_partials[i].init_time_us,
				 (unsigned long long) build_partials[i].exec_time_us,
				 (unsigned long long) build_partials[i].blocks_opened,
				 (unsigned long long) build_partials[i].input_batches,
				 (unsigned long long) build_partials[i].input_rows,
				 (unsigned long long) build_partials[i].entry_count,
				 (unsigned long long) build_partials[i].chunk_count);
		file = BufFileOpenFileSet(&partial_fileset->fs,
								 build_partials[i].file_name,
								 O_RDONLY,
								 false);
		if (file == nullptr ||
			!merge_context.hash_join_state->merge_parallel_build_partial_file(file, build_partials[i]))
		{
			if (file != nullptr)
				BufFileClose(file);
			if (failure_reason != nullptr)
				*failure_reason = "parallel hash build partial merge failed";
			goto done;
		}
		BufFileClose(file);
		BufFileDeleteFileSet(&partial_fileset->fs, build_partials[i].file_name, true);
	}

	merge_context.hash_join_state->finish_parallel_hash_build();
	if (merge_context.hash_join_state->estimate_parallel_hash_bridge_size() > MaxAllocSize)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel hash build shared bridge exceeds MaxAllocSize";
		goto done;
	}
	merge_context.hash_join_state->publish_hash_bridge();
	if (merge_context.hash_join_state->shared_hash_bridge_buffer() == nullptr ||
		merge_context.hash_join_state->shared_hash_bridge_size() == 0)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel hash build did not publish a shared hash bridge";
		goto done;
	}
	success = true;

done:
	CleanupLocalParallelAggregateProcessState(&leader_local_state);
	if (source_rel != nullptr)
		table_close(source_rel, NoLock);
	if (partial_fileset != nullptr)
		SharedFileSetDeleteAll(partial_fileset);
	if (pcxt != nullptr)
		DestroyParallelContext(pcxt);
	if (entered_parallel)
		ExitParallelMode();
	if (plannedstmt_serialized_owned != nullptr)
		pfree(plannedstmt_serialized_owned);
	return success;
}

bool
TryExecuteProcessParallelAggregate(PgVolVecQueryState *query_state,
								   QueryDesc *queryDesc,
									 const char **failure_reason)
{
	const ParallelPipelineDesc *source_pipeline = nullptr;
	const ParallelPipelineDesc *execution_source_pipeline = nullptr;
	const ParallelPipelineDesc *ready_aggregate_source_pipeline = nullptr;
	const ParallelPipelineDesc *hash_probe_aggregate_source_pipeline = nullptr;
	ParallelWorkerContext merge_context;
	ParallelContext *pcxt = nullptr;
	ParallelAggregateSharedControl *control = nullptr;
	ParallelAggPartialState *partials = nullptr;
	ParallelAggPartialState leader_partial;
	SharedFileSet *partial_fileset = nullptr;
	ParallelTableScanDesc source_pscan = nullptr;
	Relation source_rel = nullptr;
	char *shared_plannedstmt = nullptr;
	char *shared_query_text = nullptr;
	uint8_t *shared_hash_bridge_dsm = nullptr;
	const ParallelPipelineRuntimeState *source_runtime = nullptr;
	char *plannedstmt_serialized = nullptr;
	const char *query_text = "";
	const char *leader_failure_reason = nullptr;
	const uint8_t *shared_hash_bridge = nullptr;
	Size plannedstmt_len;
	Size query_text_len;
	Size partials_len;
	Size source_pscan_len;
	Size shared_hash_bridge_len = 0;
	int requested_workers;
	int aggregate_source_count = 0;
	int selected_agg_plan_node_id = -1;
	bool need_hash_join_state = false;
	bool file_backed_grouped_partials = false;
	bool entered_parallel = false;
	bool leader_local_initialized = false;
	bool success = false;
	LocalParallelAggregateProcessState leader_local_state;

	memset(&leader_partial, 0, sizeof(leader_partial));

	if (failure_reason != nullptr)
		*failure_reason = nullptr;
	if (query_state == nullptr || queryDesc == nullptr ||
		query_state->parallel_plan == nullptr || query_state->parallel_scheduler == nullptr)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel aggregate execution requires initialized query state";
		return false;
	}

	const ParallelPipelineDesc *generic_aggregate_source_pipeline = nullptr;

	for (const auto &pipeline : query_state->parallel_plan->pipelines())
	{
		const ParallelPipelineRuntimeState *runtime;

		if (!IsAggregateSourcePipeline(pipeline))
			continue;
		aggregate_source_count++;
		if (pipeline.role == ParallelPipelineRole::GenericSource &&
			generic_aggregate_source_pipeline == nullptr)
			generic_aggregate_source_pipeline = &pipeline;
		if (pipeline.role == ParallelPipelineRole::HashProbeSource &&
			hash_probe_aggregate_source_pipeline == nullptr)
			hash_probe_aggregate_source_pipeline = &pipeline;
		runtime = query_state->parallel_scheduler->get_pipeline_runtime(pipeline.pipeline_id);
		if (runtime == nullptr)
			continue;
		if (runtime->remaining_dependencies == 0 &&
			runtime->ready &&
			pipeline.role == ParallelPipelineRole::GenericSource)
			ready_aggregate_source_pipeline = &pipeline;
		if (execution_source_pipeline == nullptr)
		{
			execution_source_pipeline = &pipeline;
			continue;
		}
		if (merge_context.hash_join_state != nullptr)
		{
			bool prefer_pipeline =
				(execution_source_pipeline->role != ParallelPipelineRole::HashProbeSource &&
				 pipeline.role == ParallelPipelineRole::HashProbeSource);
			if (!prefer_pipeline)
			{
				const ParallelPipelineRuntimeState *current_runtime =
					query_state->parallel_scheduler->get_pipeline_runtime(
						execution_source_pipeline->pipeline_id);

				prefer_pipeline =
					current_runtime != nullptr &&
					runtime->total_blocks != InvalidBlockNumber &&
					current_runtime->total_blocks != InvalidBlockNumber &&
					runtime->total_blocks > current_runtime->total_blocks;
			}
			if (prefer_pipeline)
				execution_source_pipeline = &pipeline;
		}
		else if (source_pipeline != nullptr)
		{
			/* handled below after we've seen the full set of candidates */
		}
		source_pipeline = &pipeline;
	}
	if (ready_aggregate_source_pipeline != nullptr)
		execution_source_pipeline = ready_aggregate_source_pipeline;
	else if (generic_aggregate_source_pipeline != nullptr)
		execution_source_pipeline = generic_aggregate_source_pipeline;
	if (execution_source_pipeline == nullptr)
	{
		execution_source_pipeline = source_pipeline;
	}
	if (execution_source_pipeline != nullptr &&
		execution_source_pipeline->role == ParallelPipelineRole::GenericSource &&
		hash_probe_aggregate_source_pipeline != nullptr &&
		query_state->vec_plan != nullptr)
	{
		VecAggState *generic_agg_state =
			query_state->vec_plan->find_parallel_aggregate_state_by_plan_node_id(
				execution_source_pipeline->agg_plan_node_id);

		/*
		 * File-backed grouped partials are a strong signal that the generic
		 * source path will spill and then force the leader to merge a large
		 * number of partial groups. Q4's DISTINCT-build-side shape is the
		 * canonical example. Prefer the hash-probe candidate instead and let
		 * the existing build-dominance guard decide whether process-parallel
		 * should proceed or fall back.
		 */
		if (generic_agg_state != nullptr &&
			generic_agg_state->uses_file_backed_parallel_partial_state())
		{
			if (pg_volvec_trace_hooks)
				elog(LOG,
					 "pg_volvec: process parallel preferring hash probe source pipeline=%u over generic source pipeline=%u because generic grouped partials are file-backed",
					 hash_probe_aggregate_source_pipeline->pipeline_id,
					 execution_source_pipeline->pipeline_id);
			execution_source_pipeline = hash_probe_aggregate_source_pipeline;
		}
	}
	if (execution_source_pipeline == nullptr)
	{
		elog(WARNING, "pg_volvec: TryExecuteProcessParallelAggregate exit: no aggregate source pipeline");
		if (failure_reason != nullptr)
			*failure_reason = "parallel aggregate execution found no aggregate source pipeline";
		return false;
	}
	selected_agg_plan_node_id = execution_source_pipeline->agg_plan_node_id;
	/*
	 * HashProbeSource workers execute the full probe-side subtree through
	 * agg_state->consume_left_input(). They still need an explicit top-level
	 * hash-join handle now that the build side can be pre-built once and handed
	 * to workers through a shared bridge instead of being rebuilt per worker.
	 */
	need_hash_join_state =
		execution_source_pipeline->role == ParallelPipelineRole::HashBuildSource ||
		execution_source_pipeline->role == ParallelPipelineRole::HashProbeSource;
	if (!TryInitializeParallelMergeContext(query_state,
										   selected_agg_plan_node_id,
										   execution_source_pipeline->hash_join_plan_node_id,
										   need_hash_join_state,
										   &merge_context,
										   failure_reason) ||
		merge_context.agg_state == nullptr ||
		!merge_context.agg_state->supports_parallel_partial_state())
	{
		elog(WARNING, "pg_volvec: TryExecuteProcessParallelAggregate exit: merge context init failed (reason: %s)",
			 (failure_reason != nullptr && *failure_reason != nullptr) ? *failure_reason : "agg doesn't support parallel partial");
		if (failure_reason != nullptr && *failure_reason == nullptr)
			*failure_reason = "selected aggregate state does not support parallel partial state";
		return false;
	}
	file_backed_grouped_partials =
		merge_context.agg_state->uses_file_backed_parallel_partial_state();
	if (pg_volvec_trace_hooks)
		elog(LOG,
			 "pg_volvec: process parallel selecting source pipeline=%u role=%s rel=%u plan_node_id=%d agg_plan_node_id=%d output_bridge=%u stage_mask=%u candidates=%d",
			 execution_source_pipeline->pipeline_id,
			 ParallelPipelineRoleDebugName(execution_source_pipeline->role),
			 execution_source_pipeline->scan_relid,
			 execution_source_pipeline->scan_plan_node_id,
			 execution_source_pipeline->agg_plan_node_id,
			 (unsigned) execution_source_pipeline->output_bridge,
			 execution_source_pipeline->stage_mask,
			 aggregate_source_count);

	source_runtime = query_state->parallel_scheduler->get_pipeline_runtime(execution_source_pipeline->pipeline_id);
	if (source_runtime == nullptr || source_runtime->total_blocks == InvalidBlockNumber ||
		source_runtime->total_blocks == 0)
	{
		elog(WARNING, "pg_volvec: TryExecuteProcessParallelAggregate exit: no eligible source blocks");
		if (failure_reason != nullptr)
			*failure_reason = "parallel aggregate execution found no eligible source blocks";
		return false;
	}
	if (execution_source_pipeline->role == ParallelPipelineRole::HashProbeSource)
	{
		BlockNumber build_blocks = 0;
		uint32_t build_pipeline_id = UINT32_MAX;
		double build_rows = -1.0;
		double source_rows = -1.0;
		const ParallelPipelineDesc *build_pipeline = nullptr;
		bool skip_for_build_dominance;

		skip_for_build_dominance =
			ShouldSkipHashProbeParallelForBuildDominatedDependency(query_state->parallel_plan,
																   query_state->parallel_scheduler,
																   execution_source_pipeline,
																   source_runtime,
																   &build_blocks,
																   &build_pipeline_id,
																   &build_rows,
																   &source_rows);
		build_pipeline =
			query_state->parallel_plan != nullptr
				? query_state->parallel_plan->get_pipeline(build_pipeline_id)
				: nullptr;
		source_rows =
			LookupPlannedStmtNodeRows(queryDesc->plannedstmt,
									  execution_source_pipeline->scan_plan_node_id);
		build_rows =
			LookupPlannedStmtNodeRows(queryDesc->plannedstmt,
									  build_pipeline != nullptr ? build_pipeline->scan_plan_node_id : -1);
		if (skip_for_build_dominance &&
			build_rows > 0.0 &&
			source_rows > 0.0 &&
			build_rows <= source_rows * VOLVEC_PARALLEL_HASH_BUILD_SMALL_ROWS_RATIO)
		{
			skip_for_build_dominance = false;
			if (pg_volvec_trace_hooks)
				elog(LOG,
					 "pg_volvec: process parallel kept hash probe source pipeline=%u despite build block dominance because build rows=%lld are small relative to probe rows=%lld",
					 execution_source_pipeline->pipeline_id,
					 (long long) llround(build_rows),
					 (long long) llround(source_rows));
		}

		if (skip_for_build_dominance)
		{
			if (pg_volvec_trace_hooks)
				elog(LOG,
					 "pg_volvec: process parallel skipped for hash probe source pipeline=%u blocks=%u build_rows=%lld because build dependency pipeline=%u blocks=%u probe_rows=%lld dominates by ratio>%u",
					 execution_source_pipeline->pipeline_id,
					 (unsigned) source_runtime->total_blocks,
					 (long long) llround(build_rows),
					 build_pipeline_id,
					 (unsigned) build_blocks,
					 (long long) llround(source_rows),
					 VOLVEC_PARALLEL_HASH_BUILD_DOMINANCE_RATIO);
			if (failure_reason != nullptr)
				*failure_reason =
					"parallel aggregate skipped because hash build dependency dominates probe source";
			return false;
		}
	}
	if (execution_source_pipeline->role == ParallelPipelineRole::HashProbeSource &&
		merge_context.hash_join_state != nullptr)
	{
		if (!TryExecuteParallelHashBuildPhase(query_state,
												 queryDesc,
												 execution_source_pipeline,
												 merge_context,
												 failure_reason))
		{
			if (pg_volvec_trace_hooks)
				elog(LOG,
					 "pg_volvec: falling back to leader-built shared hash bridge for probe pipeline=%u (reason: %s)",
					 execution_source_pipeline->pipeline_id,
					 (failure_reason != nullptr && *failure_reason != nullptr) ?
						 *failure_reason :
						 "unknown");
			merge_context.hash_join_state->reset_parallel_hash_build_state();
			merge_context.hash_join_state->consume_build_input();
			merge_context.hash_join_state->finish_parallel_hash_build();
			if (merge_context.hash_join_state->estimate_parallel_hash_bridge_size() > MaxAllocSize)
			{
				if (failure_reason != nullptr)
					*failure_reason =
						"leader-built hash bridge exceeds MaxAllocSize";
				return false;
			}
			merge_context.hash_join_state->publish_hash_bridge();
		}
		shared_hash_bridge = merge_context.hash_join_state->shared_hash_bridge_buffer();
		shared_hash_bridge_len = merge_context.hash_join_state->shared_hash_bridge_size();
		if (shared_hash_bridge == nullptr || shared_hash_bridge_len == 0)
		{
			if (failure_reason != nullptr)
				*failure_reason = "parallel aggregate failed to publish shared hash bridge";
			return false;
		}
		if (pg_volvec_trace_hooks)
			elog(LOG,
				 "pg_volvec: published shared hash bridge for probe pipeline=%u bytes=%zu entries=%zu chunks=%zu",
				 execution_source_pipeline->pipeline_id,
				 (size_t) shared_hash_bridge_len,
				 merge_context.hash_join_state->parallel_hash_entry_count(),
				 merge_context.hash_join_state->parallel_hash_chunk_count());
	}

	requested_workers = Min(pg_volvec_parallel_max_workers,
							(int) Max(source_runtime->estimated_morsels, 1u));
	if (requested_workers <= 0)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel aggregate execution requested zero workers";
		return false;
	}

	query_text = queryDesc->sourceText != nullptr ? queryDesc->sourceText : "";
	plannedstmt_serialized = nodeToString(queryDesc->plannedstmt);
	plannedstmt_len = strlen(plannedstmt_serialized) + 1;
	query_text_len = strlen(query_text) + 1;
	source_rel = table_open(execution_source_pipeline->scan_relid, NoLock);
	source_pscan_len = table_parallelscan_estimate(source_rel, queryDesc->estate->es_snapshot);

	EnterParallelMode();
	entered_parallel = true;
	pcxt = CreateParallelContext("pg_volvec",
								 "pg_volvec_parallel_worker_main",
								 requested_workers);

	shm_toc_estimate_chunk(&pcxt->estimator, sizeof(ParallelAggregateSharedControl));
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	shm_toc_estimate_chunk(&pcxt->estimator, plannedstmt_len);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	shm_toc_estimate_chunk(&pcxt->estimator, query_text_len);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	shm_toc_estimate_chunk(&pcxt->estimator, source_pscan_len);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	partials_len = mul_size(sizeof(ParallelAggPartialState), pcxt->nworkers);
	shm_toc_estimate_chunk(&pcxt->estimator, partials_len);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	shm_toc_estimate_chunk(&pcxt->estimator, sizeof(SharedFileSet));
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	if (shared_hash_bridge_len > 0)
	{
		shm_toc_estimate_chunk(&pcxt->estimator, shared_hash_bridge_len);
		shm_toc_estimate_keys(&pcxt->estimator, 1);
	}

	InitializeParallelDSM(pcxt);
	if (pcxt->seg == nullptr)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel aggregate DSM initialization failed";
		goto done;
	}

	control = (ParallelAggregateSharedControl *)
		shm_toc_allocate(pcxt->toc, sizeof(ParallelAggregateSharedControl));
	control->magic = VOLVEC_PARALLEL_MAGIC;
	control->source_pipeline_id = execution_source_pipeline->pipeline_id;
	control->partial_slot_count = pcxt->nworkers;
	control->morsel_nblocks = pg_volvec_parallel_morsel_nblocks;
	control->total_blocks = source_runtime->total_blocks;
	control->source_scan_relid = execution_source_pipeline->scan_relid;
	control->source_scan_plan_node_id = execution_source_pipeline->scan_plan_node_id;
	control->agg_plan_node_id = execution_source_pipeline->agg_plan_node_id;
	control->hash_join_plan_node_id = merge_context.hash_join_plan_node_id;
	control->input_hash_join_plan_node_id = merge_context.hash_join_plan_node_id;
	control->execution_mode = (uint32) ParallelWorkerExecutionMode::AggregateProbe;
	control->need_hash_join_state = need_hash_join_state;
	control->hash_bridge_ready = shared_hash_bridge_len > 0 ? 1 : 0;
	control->reserved[0] = 0;
	control->reserved[1] = 0;
	control->hash_bridge_size = shared_hash_bridge_len;
	pg_atomic_init_u64(&control->next_block, 0);
	shm_toc_insert(pcxt->toc, VOLVEC_PARALLEL_KEY_CONTROL, control);

	shared_plannedstmt = (char *) shm_toc_allocate(pcxt->toc, plannedstmt_len);
	memcpy(shared_plannedstmt, plannedstmt_serialized, plannedstmt_len);
	shm_toc_insert(pcxt->toc, VOLVEC_PARALLEL_KEY_PLANNEDSTMT, shared_plannedstmt);

	shared_query_text = (char *) shm_toc_allocate(pcxt->toc, query_text_len);
	memcpy(shared_query_text, query_text, query_text_len);
	shm_toc_insert(pcxt->toc, VOLVEC_PARALLEL_KEY_QUERY_TEXT, shared_query_text);

	source_pscan = (ParallelTableScanDesc) shm_toc_allocate(pcxt->toc, source_pscan_len);
	table_parallelscan_initialize(source_rel, source_pscan, queryDesc->estate->es_snapshot);
	shm_toc_insert(pcxt->toc, VOLVEC_PARALLEL_KEY_SOURCE_PSCAN, source_pscan);

	partials = (ParallelAggPartialState *) shm_toc_allocate(pcxt->toc, partials_len);
	memset(partials, 0, partials_len);
	shm_toc_insert(pcxt->toc, VOLVEC_PARALLEL_KEY_PARTIALS, partials);
	partial_fileset = (SharedFileSet *) shm_toc_allocate(pcxt->toc, sizeof(SharedFileSet));
	SharedFileSetInit(partial_fileset, pcxt->seg);
	shm_toc_insert(pcxt->toc, VOLVEC_PARALLEL_KEY_PARTIAL_FILESET, partial_fileset);
	if (shared_hash_bridge_len > 0)
	{
		shared_hash_bridge_dsm = (uint8_t *) shm_toc_allocate(pcxt->toc, shared_hash_bridge_len);
		memcpy(shared_hash_bridge_dsm, shared_hash_bridge, shared_hash_bridge_len);
		shm_toc_insert(pcxt->toc, VOLVEC_PARALLEL_KEY_HASH_BRIDGE, shared_hash_bridge_dsm);
	}
	table_close(source_rel, NoLock);
	source_rel = nullptr;

	LaunchParallelWorkers(pcxt);
	if (pg_volvec_trace_hooks)
		elog(LOG,
			 "pg_volvec: launched %d/%d parallel workers for source pipeline %u",
			 pcxt->nworkers_launched,
			 pcxt->nworkers,
			 execution_source_pipeline->pipeline_id);
	if (pcxt->nworkers_launched == 0 && !pg_volvec_parallel_leader_participation)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel aggregate launched no workers and leader participation is disabled";
		goto done;
	}

	if (pg_volvec_parallel_leader_participation)
	{
		instr_time leader_exec_start;
		instr_time leader_exec_end;

		if (!TryInitializeLocalParallelAggregateProcessState(plannedstmt_serialized,
																 query_text,
																 execution_source_pipeline->agg_plan_node_id,
																 merge_context.hash_join_plan_node_id,
																 merge_context.hash_join_plan_node_id,
																 true,
																 need_hash_join_state,
																 shared_hash_bridge_dsm,
																 shared_hash_bridge_len,
																 execution_source_pipeline->scan_relid,
																 execution_source_pipeline->scan_plan_node_id,
																 source_pscan,
															 true,
															 &leader_local_state,
															 &leader_failure_reason))
		{
			if (failure_reason != nullptr)
				*failure_reason = leader_failure_reason != nullptr ?
					leader_failure_reason :
					"parallel aggregate leader local init failed";
			goto done;
		}
		leader_local_initialized = true;
		INSTR_TIME_SET_CURRENT(leader_exec_start);
		if (!ExecuteParallelWorkerSourceLoop(ParallelWorkerExecutionMode::AggregateProbe,
											 leader_local_state.worker_context,
											 failure_reason))
			goto done;
		INSTR_TIME_SET_CURRENT(leader_exec_end);
		INSTR_TIME_SUBTRACT(leader_exec_end, leader_exec_start);
		leader_local_state.exec_time_us = (uint64) INSTR_TIME_GET_MICROSEC(leader_exec_end);
		if (file_backed_grouped_partials)
		{
			BufFile *file;
			char file_name[VOLVEC_PARALLEL_MAX_PARTIAL_FILE_NAME];

			FormatParallelPartialFileName(file_name, sizeof(file_name), "leader", 0);
			file = BufFileCreateFileSet(&partial_fileset->fs, file_name);
			if (file == nullptr ||
				!leader_local_state.worker_context.agg_state->export_parallel_grouped_partial_file(
					file,
					&leader_partial))
			{
				if (file != nullptr)
					BufFileClose(file);
				if (failure_reason != nullptr)
					*failure_reason = "parallel aggregate leader grouped partial export failed";
				goto done;
			}
			strlcpy(leader_partial.grouped_file_name,
					file_name,
					sizeof(leader_partial.grouped_file_name));
			BufFileExportFileSet(file);
			BufFileClose(file);
		}
		else if (!leader_local_state.worker_context.agg_state->export_parallel_partial_state(&leader_partial))
		{
			if (failure_reason != nullptr)
				*failure_reason = "parallel aggregate leader partial export failed";
			goto done;
		}
		PopulatePartialDiagnostics(leader_local_state, &leader_partial);
	}

	WaitForParallelWorkersToFinish(pcxt);
	if (leader_local_initialized)
	{
		if (pg_volvec_trace_hooks)
			elog(LOG,
				 "pg_volvec: parallel leader local partial init_us=%llu exec_us=%llu blocks=%llu batches=%llu rows=%llu",
				 (unsigned long long) leader_partial.init_time_us,
				 (unsigned long long) leader_partial.exec_time_us,
				 (unsigned long long) leader_partial.blocks_opened,
				 (unsigned long long) leader_partial.input_batches,
				 (unsigned long long) leader_partial.input_rows);
		if (leader_partial.file_backed)
		{
			BufFile *file = BufFileOpenFileSet(&partial_fileset->fs,
												 leader_partial.grouped_file_name,
												 O_RDONLY,
												 false);

			if (file == nullptr ||
				!merge_context.agg_state->merge_parallel_grouped_partial_file(file, leader_partial))
			{
				if (file != nullptr)
					BufFileClose(file);
				if (failure_reason != nullptr)
					*failure_reason = "parallel aggregate leader grouped partial merge failed";
				goto done;
			}
			BufFileClose(file);
			BufFileDeleteFileSet(&partial_fileset->fs,
								 leader_partial.grouped_file_name,
								 true);
		}
		else if (!merge_context.agg_state->merge_parallel_partial_state(leader_partial))
		{
			if (failure_reason != nullptr)
				*failure_reason = "parallel aggregate leader partial merge failed";
			goto done;
		}
	}
	for (int i = 0; i < pcxt->nworkers_launched; i++)
	{
		if (pg_volvec_trace_hooks)
			elog(LOG,
				 "pg_volvec: parallel worker[%d] partial init_us=%llu exec_us=%llu blocks=%llu batches=%llu rows=%llu",
				 i,
				 (unsigned long long) partials[i].init_time_us,
				 (unsigned long long) partials[i].exec_time_us,
				 (unsigned long long) partials[i].blocks_opened,
				 (unsigned long long) partials[i].input_batches,
				 (unsigned long long) partials[i].input_rows);
		if (partials[i].file_backed)
		{
			BufFile *file = BufFileOpenFileSet(&partial_fileset->fs,
												 partials[i].grouped_file_name,
												 O_RDONLY,
												 false);

			if (file == nullptr ||
				!merge_context.agg_state->merge_parallel_grouped_partial_file(file, partials[i]))
			{
				if (file != nullptr)
					BufFileClose(file);
				if (failure_reason != nullptr)
					*failure_reason = "parallel aggregate grouped partial merge failed";
				goto done;
			}
			BufFileClose(file);
			BufFileDeleteFileSet(&partial_fileset->fs,
								 partials[i].grouped_file_name,
								 true);
		}
		else if (!merge_context.agg_state->merge_parallel_partial_state(partials[i]))
		{
			if (failure_reason != nullptr)
				*failure_reason = "parallel aggregate merge failed";
			goto done;
		}
	}
	/* Load the shared hash bridge for probe-side execution */
	if (merge_context.hash_join_state != nullptr &&
		execution_source_pipeline->role == ParallelPipelineRole::HashProbeSource)
	{
		merge_context.hash_join_state->load_hash_bridge();
	}
	merge_context.agg_state->finish_sink();
	success = true;

done:
	if (!success && failure_reason != nullptr && *failure_reason == nullptr)
		*failure_reason = "unknown process parallel aggregate failure";
	CleanupLocalParallelAggregateProcessState(&leader_local_state);
	if (source_rel != nullptr)
		table_close(source_rel, NoLock);
	if (plannedstmt_serialized != nullptr)
		pfree(plannedstmt_serialized);
	if (partial_fileset != nullptr)
		SharedFileSetDeleteAll(partial_fileset);
	if (pcxt != nullptr)
		DestroyParallelContext(pcxt);
	if (entered_parallel)
		ExitParallelMode();
	return success;
}

} /* namespace pg_volvec */

extern "C" PGDLLEXPORT void
pg_volvec_parallel_worker_main(dsm_segment *seg, shm_toc *toc)
{
	using namespace pg_volvec;
	ParallelAggregateSharedControl *control;
	ParallelAggPartialState *partials = nullptr;
	ParallelHashBuildPartialState *build_partials = nullptr;
	ParallelTableScanDesc source_pscan;
	SharedFileSet *partial_fileset;
	const uint8_t *shared_hash_bridge = nullptr;
	const char *plannedstmt_serialized;
	const char *query_text;
	LocalParallelAggregateProcessState *local_state;
	const char *failure_reason = nullptr;
	instr_time exec_start;
	instr_time exec_end;
	ParallelWorkerExecutionMode execution_mode;

	control = (ParallelAggregateSharedControl *) shm_toc_lookup(toc,
																VOLVEC_PARALLEL_KEY_CONTROL,
																false);
	source_pscan = (ParallelTableScanDesc) shm_toc_lookup(toc,
															 VOLVEC_PARALLEL_KEY_SOURCE_PSCAN,
															 false);
	partial_fileset = (SharedFileSet *) shm_toc_lookup(toc,
															VOLVEC_PARALLEL_KEY_PARTIAL_FILESET,
															false);
	plannedstmt_serialized = (const char *) shm_toc_lookup(toc,
														   VOLVEC_PARALLEL_KEY_PLANNEDSTMT,
														   false);
	query_text = (const char *) shm_toc_lookup(toc,
												VOLVEC_PARALLEL_KEY_QUERY_TEXT,
												true);
	if (control != nullptr && control->hash_bridge_ready && control->hash_bridge_size > 0)
		shared_hash_bridge = (const uint8_t *) shm_toc_lookup(toc,
															  VOLVEC_PARALLEL_KEY_HASH_BRIDGE,
															  false);
	execution_mode = control != nullptr ?
		(ParallelWorkerExecutionMode) control->execution_mode :
		ParallelWorkerExecutionMode::AggregateProbe;
	if (execution_mode == ParallelWorkerExecutionMode::AggregateProbe)
	{
		partials = (ParallelAggPartialState *) shm_toc_lookup(toc,
																VOLVEC_PARALLEL_KEY_PARTIALS,
																false);
	}
	else if (execution_mode == ParallelWorkerExecutionMode::HashBuild)
	{
		build_partials = (ParallelHashBuildPartialState *) shm_toc_lookup(
			toc,
			VOLVEC_PARALLEL_KEY_HASH_BUILD_PARTIALS,
			false);
	}

	if (control == nullptr || control->magic != VOLVEC_PARALLEL_MAGIC ||
		source_pscan == nullptr || partial_fileset == nullptr ||
		plannedstmt_serialized == nullptr ||
		(execution_mode == ParallelWorkerExecutionMode::AggregateProbe && partials == nullptr) ||
		(execution_mode == ParallelWorkerExecutionMode::HashBuild && build_partials == nullptr) ||
		(control->hash_bridge_ready && control->hash_bridge_size > 0 && shared_hash_bridge == nullptr))
		elog(ERROR, "pg_volvec parallel worker missing shared control");
	SharedFileSetAttach(partial_fileset, seg);
	local_state = (LocalParallelAggregateProcessState *)
		MemoryContextAllocZero(TopMemoryContext,
							   sizeof(LocalParallelAggregateProcessState));
	PG_ENSURE_ERROR_CLEANUP(CleanupLocalParallelAggregateProcessStateOnExit,
						   PointerGetDatum(local_state));
	{
		if (!TryInitializeLocalParallelAggregateProcessState(plannedstmt_serialized,
															 query_text,
															 control->agg_plan_node_id,
															 control->hash_join_plan_node_id,
															 control->input_hash_join_plan_node_id,
															 execution_mode == ParallelWorkerExecutionMode::AggregateProbe,
															 control->need_hash_join_state,
															 shared_hash_bridge,
															 (size_t) control->hash_bridge_size,
															 control->source_scan_relid,
															 control->source_scan_plan_node_id,
															 source_pscan,
															 false,
															 local_state,
															 &failure_reason))
			elog(ERROR, "pg_volvec parallel worker init failed: %s",
				 failure_reason != nullptr ? failure_reason : "unknown reason");
		if (pg_volvec_trace_hooks)
			elog(LOG,
				 "pg_volvec: parallel worker pid=%d entering execute mode=%s hash_join_plan_node_id=%d input_hash_join_plan_node_id=%d",
				 MyProcPid,
				 ParallelWorkerExecutionModeDebugName(execution_mode),
				 control->hash_join_plan_node_id,
				 control->input_hash_join_plan_node_id);
		INSTR_TIME_SET_CURRENT(exec_start);
		if (!ExecuteParallelWorkerSourceLoop(execution_mode,
											 local_state->worker_context,
											 &failure_reason))
			elog(ERROR, "pg_volvec parallel worker execution failed: %s",
				 failure_reason != nullptr ? failure_reason : "unknown reason");
		INSTR_TIME_SET_CURRENT(exec_end);
		INSTR_TIME_SUBTRACT(exec_end, exec_start);
		local_state->exec_time_us = (uint64) INSTR_TIME_GET_MICROSEC(exec_end);
		if (pg_volvec_trace_hooks)
			elog(LOG,
				 "pg_volvec: parallel worker pid=%d finished execute mode=%s",
				 MyProcPid,
				 ParallelWorkerExecutionModeDebugName(execution_mode));
		if (ParallelWorkerNumber < 0 ||
			(uint32) ParallelWorkerNumber >= control->partial_slot_count)
			elog(ERROR, "pg_volvec parallel worker number %d out of range",
				 ParallelWorkerNumber);
		if (execution_mode == ParallelWorkerExecutionMode::HashBuild)
		{
			BufFile *file;
			char file_name[VOLVEC_PARALLEL_MAX_PARTIAL_FILE_NAME];

			if (pg_volvec_trace_hooks)
				elog(LOG,
					 "pg_volvec: parallel worker pid=%d starting hash build partial export",
					 MyProcPid);
			if (pg_volvec_trace_hooks)
				elog(LOG,
					 "pg_volvec: parallel worker hash build finished execution rows=%llu batches=%llu entries=%zu chunks=%zu",
					 (unsigned long long)
					 local_state->worker_context.hash_join_state->build_input_rows_consumed(),
					 (unsigned long long)
					 local_state->worker_context.hash_join_state->build_input_batches_consumed(),
					 local_state->worker_context.hash_join_state->parallel_hash_entry_count(),
					 local_state->worker_context.hash_join_state->parallel_hash_chunk_count());

			FormatParallelPartialFileName(file_name,
										 sizeof(file_name),
										 "build_worker",
										 ParallelWorkerNumber);
			file = BufFileCreateFileSet(&partial_fileset->fs, file_name);
			if (pg_volvec_trace_hooks)
				elog(LOG,
					 "pg_volvec: parallel worker exporting hash build partial file=%s",
					 file_name);
			if (file == nullptr ||
				!local_state->worker_context.hash_join_state->export_parallel_build_partial_file(
					file,
					&build_partials[ParallelWorkerNumber]))
			{
				if (file != nullptr)
					BufFileClose(file);
				elog(ERROR, "pg_volvec parallel worker hash build partial export failed");
			}
			strlcpy(build_partials[ParallelWorkerNumber].file_name,
					file_name,
					sizeof(build_partials[ParallelWorkerNumber].file_name));
			BufFileExportFileSet(file);
			BufFileClose(file);
			PopulateHashBuildPartialDiagnostics(*local_state,
											   &build_partials[ParallelWorkerNumber]);
		}
		else if (local_state->worker_context.agg_state->uses_file_backed_parallel_partial_state())
		{
			BufFile *file;
			char file_name[VOLVEC_PARALLEL_MAX_PARTIAL_FILE_NAME];

			if (pg_volvec_trace_hooks)
				elog(LOG,
					 "pg_volvec: parallel worker pid=%d starting grouped partial export",
					 MyProcPid);
			FormatParallelPartialFileName(file_name,
										 sizeof(file_name),
										 "worker",
										 ParallelWorkerNumber);
			file = BufFileCreateFileSet(&partial_fileset->fs, file_name);
			if (file == nullptr ||
				!local_state->worker_context.agg_state->export_parallel_grouped_partial_file(
					file,
					&partials[ParallelWorkerNumber]))
			{
				if (file != nullptr)
					BufFileClose(file);
				elog(ERROR, "pg_volvec parallel worker grouped partial export failed");
			}
			strlcpy(partials[ParallelWorkerNumber].grouped_file_name,
					file_name,
					sizeof(partials[ParallelWorkerNumber].grouped_file_name));
			BufFileExportFileSet(file);
			BufFileClose(file);
			PopulatePartialDiagnostics(*local_state, &partials[ParallelWorkerNumber]);
		}
		else
		{
			if (pg_volvec_trace_hooks)
				elog(LOG,
					 "pg_volvec: parallel worker pid=%d starting inline partial export",
					 MyProcPid);
			if (!local_state->worker_context.agg_state->export_parallel_partial_state(
					 &partials[ParallelWorkerNumber]))
				elog(ERROR, "pg_volvec parallel worker partial export failed");
			PopulatePartialDiagnostics(*local_state, &partials[ParallelWorkerNumber]);
		}
		CleanupLocalParallelAggregateProcessState(local_state);
	}
	PG_END_ENSURE_ERROR_CLEANUP(CleanupLocalParallelAggregateProcessStateOnExit,
								PointerGetDatum(local_state));
}

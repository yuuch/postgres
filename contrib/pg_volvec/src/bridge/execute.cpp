extern "C" {
#include "postgres.h"
#include "executor/executor.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "catalog/pg_type.h"
#include "nodes/nodeFuncs.h"
}

#include "execute.h"
#include "volvec_engine.hpp"

#include <string>

extern "C" {
extern int pg_volvec_parallel_morsel_nblocks;
extern bool pg_volvec_trace_hooks;
extern bool pg_volvec_parallel_leader_participation;
extern bool pg_volvec_parallel_experimental_hash_pipeline;
}

namespace {

const char *
ParallelDriverKindName(pg_volvec::ParallelPipelineDriverKind kind)
{
	switch (kind)
	{
		case pg_volvec::ParallelPipelineDriverKind::SourceScan:
			return "SourceScan";
		case pg_volvec::ParallelPipelineDriverKind::BridgeFinalize:
			return "BridgeFinalize";
	}
	return "Unknown";
}

const char *
ParallelBridgeKindName(pg_volvec::ParallelBridgeKind kind)
{
	switch (kind)
	{
		case pg_volvec::ParallelBridgeKind::None:
			return "None";
		case pg_volvec::ParallelBridgeKind::Aggregate:
			return "Aggregate";
		case pg_volvec::ParallelBridgeKind::HashBuild:
			return "HashBuild";
		case pg_volvec::ParallelBridgeKind::HashTable:
			return "HashTable";
		case pg_volvec::ParallelBridgeKind::SortRuns:
			return "SortRuns";
	}
	return "Unknown";
}

const char *
ParallelPipelineRoleName(pg_volvec::ParallelPipelineRole role)
{
	switch (role)
	{
		case pg_volvec::ParallelPipelineRole::GenericSource:
			return "GenericSource";
		case pg_volvec::ParallelPipelineRole::AggFinalize:
			return "AggFinalize";
		case pg_volvec::ParallelPipelineRole::SortMerge:
			return "SortMerge";
		case pg_volvec::ParallelPipelineRole::HashBuildSource:
			return "HashBuildSource";
		case pg_volvec::ParallelPipelineRole::HashBuildFinalize:
			return "HashBuildFinalize";
		case pg_volvec::ParallelPipelineRole::HashProbeSource:
			return "HashProbeSource";
		case pg_volvec::ParallelPipelineRole::HashOuterSource:
			return "HashOuterSource";
	}
	return "Unknown";
}

const char *
ParallelTaskKindName(pg_volvec::ParallelTaskKind kind)
{
	switch (kind)
	{
		case pg_volvec::ParallelTaskKind::SourceMorsel:
			return "SourceMorsel";
		case pg_volvec::ParallelTaskKind::BridgeFinalize:
			return "BridgeFinalize";
	}
	return "Unknown";
}

std::string
ParallelStageMaskName(uint32_t stage_mask)
{
	std::string stages;
	auto append_stage = [&stages](const char *name) {
		if (!stages.empty())
			stages += ",";
		stages += name;
	};

	if ((stage_mask & (uint32_t) pg_volvec::ParallelPipelineStage::PartialAgg) != 0)
		append_stage("PartialAgg");
	if ((stage_mask & (uint32_t) pg_volvec::ParallelPipelineStage::HashBuild) != 0)
		append_stage("HashBuild");
	if ((stage_mask & (uint32_t) pg_volvec::ParallelPipelineStage::HashProbe) != 0)
		append_stage("HashProbe");
	if ((stage_mask & (uint32_t) pg_volvec::ParallelPipelineStage::SortRun) != 0)
		append_stage("SortRun");
	if (stages.empty())
		stages = "None";
	return stages;
}

std::string
ParallelReadyTasksName(const pg_volvec::ParallelSchedulerState *scheduler)
{
		std::string ready_list;

		if (scheduler == nullptr || scheduler->ready_task_count() == 0)
			return "<none>";
		for (const auto &task : scheduler->ready_tasks())
		{
			if (!ready_list.empty())
				ready_list += ",";
			ready_list += "{pipeline=" + std::to_string(task.pipeline_id) +
					  ",task=" + ParallelTaskKindName(task.task_kind);
			if (task.task_kind == pg_volvec::ParallelTaskKind::SourceMorsel)
				ready_list += ",start=" + std::to_string(task.morsel_start_block) +
							  ",nblocks=" + std::to_string(task.morsel_nblocks);
			ready_list += "}";
		}
		return ready_list;
}

void
LogParallelPipelinePlan(const pg_volvec::ParallelPipelinePlan *parallel_plan)
{
	if (parallel_plan == nullptr)
		return;

	for (const auto &pipeline : parallel_plan->pipelines())
	{
		std::string stages = ParallelStageMaskName(pipeline.stage_mask);

		elog(LOG,
			 "pg_volvec: parallel pipeline %u driver=%s role=%s input_bridge=%s output_bridge=%s stages=%s deps=%zu succs=%zu rel=%u morsel=%s filter=%s project=%s limit=%s grouped=%s",
			 pipeline.pipeline_id,
			 ParallelDriverKindName(pipeline.driver_kind),
			 ParallelPipelineRoleName(pipeline.role),
			 ParallelBridgeKindName(pipeline.input_bridge),
			 ParallelBridgeKindName(pipeline.output_bridge),
			 stages.c_str(),
			 pipeline.dependencies.size(),
			 pipeline.successors.size(),
			 pipeline.scan_relid,
			 pipeline.source_morsel_driven ? "on" : "off",
			 pipeline.has_filter ? "on" : "off",
			 pipeline.has_projection ? "on" : "off",
			 pipeline.has_limit ? "on" : "off",
			 pipeline.grouped_agg ? "on" : "off");
	}
}

void
LogParallelSchedulerState(const pg_volvec::ParallelSchedulerState *scheduler)
{
	if (scheduler == nullptr)
		return;

	for (const auto &runtime : scheduler->pipeline_runtime())
	{
		elog(LOG,
			 "pg_volvec: parallel runtime pipeline %u deps_remaining=%u deps_done=%u task=%s rel=%u total_blocks=%u morsels=%u next_block=%u ready=%s queued=%s running=%s completed=%s",
			 runtime.pipeline_id,
			 runtime.remaining_dependencies,
			 runtime.completed_predecessors,
			 ParallelTaskKindName(runtime.next_task_kind),
			 runtime.scan_relid,
			 runtime.total_blocks,
			 runtime.estimated_morsels,
			 runtime.next_morsel_block,
			 runtime.ready ? "on" : "off",
			 runtime.queued ? "on" : "off",
			 runtime.running ? "on" : "off",
			 runtime.completed ? "on" : "off");
	}

	for (const auto &bridge : scheduler->bridges())
	{
		elog(LOG,
			 "pg_volvec: parallel bridge producer=%u kind=%s ready=%s finalized=%s",
			 bridge.producer_pipeline_id,
			 ParallelBridgeKindName(bridge.bridge_kind),
			 bridge.ready ? "on" : "off",
			 bridge.finalized ? "on" : "off");
	}

	if (scheduler->ready_task_count() > 0)
	{
		std::string ready_list = ParallelReadyTasksName(scheduler);
		elog(LOG, "pg_volvec: parallel scheduler ready tasks=%s", ready_list.c_str());
	}
	else
		elog(LOG, "pg_volvec: parallel scheduler ready tasks=<none>");
}

void
TraceParallelSchedulerDryRun(const pg_volvec::ParallelPipelinePlan *parallel_plan,
							 MemoryContext context)
{
	const char *failure_reason = nullptr;
	MemoryContext old_context = MemoryContextSwitchTo(context);
	std::unique_ptr<pg_volvec::ParallelSchedulerState> dry_run =
		pg_volvec::BuildParallelSchedulerState(parallel_plan,
											   context,
											   pg_volvec_parallel_morsel_nblocks,
											   &failure_reason);
	MemoryContextSwitchTo(old_context);

	if (dry_run == nullptr)
	{
		elog(LOG,
			 "pg_volvec: parallel scheduler dry-run skipped (%s)",
			 failure_reason != nullptr ? failure_reason : "unknown reason");
		return;
	}

	elog(LOG,
		 "pg_volvec: parallel scheduler dry-run starting ready=%s",
		 ParallelReadyTasksName(dry_run.get()).c_str());

	const int kMaxDryRunDispatches = 16;
	int dispatches = 0;

	for (;;)
	{
		pg_volvec::ParallelTaskDesc task;
		bool dequeued = dry_run->dequeue_ready_task(&task);
		const pg_volvec::ParallelPipelineRuntimeState *runtime_after;

		if (!dequeued)
			break;
		dispatches++;
		elog(LOG,
			 "pg_volvec: parallel scheduler dry-run dispatch pipeline=%u task=%s",
			 task.pipeline_id,
			 ParallelTaskKindName(task.task_kind));
		if (task.task_kind == pg_volvec::ParallelTaskKind::SourceMorsel)
			elog(LOG,
				 "pg_volvec: parallel scheduler dry-run source morsel pipeline=%u start=%u nblocks=%u",
				 task.pipeline_id,
				 task.morsel_start_block,
				 task.morsel_nblocks);
		dry_run->finish_task(task);
		runtime_after = dry_run->get_pipeline_runtime(task.pipeline_id);
		if (runtime_after != nullptr)
			elog(LOG,
				 "pg_volvec: parallel scheduler dry-run state pipeline=%u next_block=%u queued=%s running=%s completed=%s",
				 task.pipeline_id,
				 runtime_after->next_morsel_block,
				 runtime_after->queued ? "on" : "off",
				 runtime_after->running ? "on" : "off",
				 runtime_after->completed ? "on" : "off");
		elog(LOG,
			 "pg_volvec: parallel scheduler dry-run after pipeline=%u ready=%s",
			 task.pipeline_id,
			 ParallelReadyTasksName(dry_run.get()).c_str());
		if (dispatches >= kMaxDryRunDispatches)
		{
			elog(LOG,
				 "pg_volvec: parallel scheduler dry-run truncated after %d dispatches",
				 dispatches);
			break;
		}
	}
}

bool
TryExecuteLeaderOnlyParallelAggregate(pg_volvec::PgVolVecQueryState *state_ptr)
{
	auto *scheduler = state_ptr->parallel_scheduler;
	const auto *parallel_plan = state_ptr->parallel_plan;
	const auto *source_pipeline = static_cast<const pg_volvec::ParallelPipelineDesc *>(nullptr);
	pg_volvec::ParallelWorkerContext worker_context;
	const int kMaxLoggedDispatches = 32;
	int dispatch_count = 0;
	bool started = false;
	const char *failure_reason = nullptr;

	if (!pg_volvec_parallel_leader_participation ||
		scheduler == nullptr ||
		parallel_plan == nullptr)
		return false;
	if (!pg_volvec::TryInitializeLeaderOnlyAggregateWorkerContext(state_ptr,
																  &worker_context,
																  &source_pipeline,
																  &failure_reason))
		return false;

	if (pg_volvec_trace_hooks)
		elog(LOG,
			 "pg_volvec: leader-only morsel execution enabled for aggregate source pipeline=%u root_pipeline=%u",
			 source_pipeline->pipeline_id,
			 parallel_plan->root_pipeline_id());

	while (scheduler->ready_task_count() > 0)
	{
		pg_volvec::ParallelTaskDesc task;
		const pg_volvec::ParallelTaskDesc &next_task = scheduler->ready_tasks().front();

		if (next_task.task_kind != pg_volvec::ParallelTaskKind::SourceMorsel ||
			next_task.pipeline_id != source_pipeline->pipeline_id)
			break;
		if (!scheduler->dequeue_ready_task(&task))
			break;
		started = true;
		dispatch_count++;
		if (pg_volvec_trace_hooks && dispatch_count <= kMaxLoggedDispatches)
			elog(LOG,
				 "pg_volvec: leader-only morsel dispatch pipeline=%u task=%s start=%u nblocks=%u",
				 task.pipeline_id,
				 ParallelTaskKindName(task.task_kind),
				 task.morsel_start_block,
				 task.morsel_nblocks);
		switch (task.task_kind)
		{
			case pg_volvec::ParallelTaskKind::SourceMorsel:
				if (!pg_volvec::ExecuteParallelTask(task,
													  parallel_plan,
													  worker_context,
													  &failure_reason))
					elog(ERROR,
						 "pg_volvec leader-only morsel task failed (%s)",
						 failure_reason != nullptr ? failure_reason : "unknown reason");
				scheduler->finish_task(task);
				break;
			case pg_volvec::ParallelTaskKind::BridgeFinalize:
				elog(ERROR, "pg_volvec leader-only aggregate path received unexpected finalize task");
				break;
		}
	}
	if (!started)
		return false;
	worker_context.agg_state->finish_sink();
	worker_context.agg_state->clear_input_block_range();
	if (pg_volvec_trace_hooks && dispatch_count > kMaxLoggedDispatches)
		elog(LOG,
			 "pg_volvec: leader-only morsel dispatch truncated after %d tasks (total=%d)",
			 kMaxLoggedDispatches,
			 dispatch_count);
	if (pg_volvec_trace_hooks && scheduler->ready_task_count() > 0)
		elog(LOG,
			 "pg_volvec: leader-only aggregate handoff to serial downstream ready=%s",
			 ParallelReadyTasksName(scheduler).c_str());

	return true;
}

bool
ParallelPlanContainsHashBuild(const pg_volvec::ParallelPipelinePlan *parallel_plan)
{
	if (parallel_plan == nullptr)
		return false;
	for (const auto &pipeline : parallel_plan->pipelines())
	{
		if (pipeline.role == pg_volvec::ParallelPipelineRole::HashBuildSource ||
			pipeline.role == pg_volvec::ParallelPipelineRole::HashBuildFinalize ||
			pipeline.role == pg_volvec::ParallelPipelineRole::HashProbeSource ||
			(pipeline.stage_mask & (uint32_t) pg_volvec::ParallelPipelineStage::HashBuild) != 0 ||
			(pipeline.stage_mask & (uint32_t) pg_volvec::ParallelPipelineStage::HashProbe) != 0)
			return true;
	}
	return false;
}

bool
SupportsLeaderOnlyParallelPlan(const pg_volvec::ParallelPipelinePlan *parallel_plan)
{
	if (parallel_plan == nullptr)
		return false;

	for (const auto &pipeline : parallel_plan->pipelines())
	{
		switch (pipeline.role)
		{
			case pg_volvec::ParallelPipelineRole::GenericSource:
			case pg_volvec::ParallelPipelineRole::AggFinalize:
			case pg_volvec::ParallelPipelineRole::HashBuildSource:
			case pg_volvec::ParallelPipelineRole::HashBuildFinalize:
			case pg_volvec::ParallelPipelineRole::HashProbeSource:
				break;
			case pg_volvec::ParallelPipelineRole::SortMerge:
			case pg_volvec::ParallelPipelineRole::HashOuterSource:
				return false;
		}
	}
	return true;
}

bool
TryExecuteLeaderOnlyParallelPlan(pg_volvec::PgVolVecQueryState *state_ptr)
{
	auto *scheduler = state_ptr->parallel_scheduler;
	const auto *parallel_plan = state_ptr->parallel_plan;
	pg_volvec::ParallelWorkerContext worker_context;
	const pg_volvec::ParallelPipelineDesc *source_pipeline = nullptr;
	const int kMaxLoggedDispatches = 64;
	int dispatch_count = 0;
	bool started = false;
	const char *failure_reason = nullptr;
	int trace_elevel = pg_volvec_parallel_experimental_hash_pipeline ? WARNING : LOG;

	if (!pg_volvec_parallel_leader_participation ||
		scheduler == nullptr ||
		parallel_plan == nullptr ||
		!SupportsLeaderOnlyParallelPlan(parallel_plan))
		return false;
	if (!pg_volvec::TryInitializeLeaderOnlyAggregateWorkerContext(state_ptr,
																  &worker_context,
																  &source_pipeline,
																  &failure_reason))
		return false;

	if (pg_volvec_trace_hooks)
		elog(trace_elevel,
			 "pg_volvec: leader-only parallel plan execution enabled root_pipeline=%u ready=%s",
			 parallel_plan->root_pipeline_id(),
			 ParallelReadyTasksName(scheduler).c_str());

	while (scheduler->ready_task_count() > 0)
	{
		pg_volvec::ParallelTaskDesc task;

		if (!scheduler->dequeue_ready_task(&task))
			break;
		started = true;
		dispatch_count++;
		if (pg_volvec_trace_hooks && dispatch_count <= kMaxLoggedDispatches)
		{
			if (task.task_kind == pg_volvec::ParallelTaskKind::SourceMorsel)
				elog(trace_elevel,
					 "pg_volvec: leader-only parallel dispatch pipeline=%u task=%s start=%u nblocks=%u",
					 task.pipeline_id,
					 ParallelTaskKindName(task.task_kind),
					 task.morsel_start_block,
					 task.morsel_nblocks);
			else
				elog(trace_elevel,
					 "pg_volvec: leader-only parallel dispatch pipeline=%u task=%s",
					 task.pipeline_id,
					 ParallelTaskKindName(task.task_kind));
		}
		uint64 before_agg_rows =
			worker_context.agg_state != nullptr ? worker_context.agg_state->input_rows_consumed() : 0;
		uint64 before_agg_batches =
			worker_context.agg_state != nullptr ? worker_context.agg_state->input_batches_consumed() : 0;
		uint64 before_build_rows =
			worker_context.hash_join_state != nullptr ? worker_context.hash_join_state->build_input_rows_consumed() : 0;
		uint64 before_build_batches =
			worker_context.hash_join_state != nullptr ? worker_context.hash_join_state->build_input_batches_consumed() : 0;
		uint64 before_probe_blocks = 0;
		uint64 before_build_blocks = 0;
		size_t before_hash_entries =
			worker_context.hash_join_state != nullptr ? worker_context.hash_join_state->parallel_hash_entry_count() : 0;
		size_t before_hash_chunks =
			worker_context.hash_join_state != nullptr ? worker_context.hash_join_state->parallel_hash_chunk_count() : 0;
		if (worker_context.root_plan != nullptr)
		{
			auto *probe_scan = worker_context.root_plan->find_parallel_source_scan_state();
			if (probe_scan != nullptr)
				before_probe_blocks = probe_scan->blocks_opened();
		}
		if (worker_context.hash_join_state != nullptr)
		{
			auto *build_scan = worker_context.hash_join_state->find_parallel_build_scan_state();
			if (build_scan != nullptr)
				before_build_blocks = build_scan->blocks_opened();
		}
		if (!pg_volvec::ExecuteParallelTask(task,
											 parallel_plan,
											 worker_context,
											 &failure_reason))
			elog(ERROR,
				 "pg_volvec leader-only parallel task failed (%s)",
				 failure_reason != nullptr ? failure_reason : "unknown reason");
		if (pg_volvec_trace_hooks)
		{
			uint64 after_agg_rows =
				worker_context.agg_state != nullptr ? worker_context.agg_state->input_rows_consumed() : 0;
			uint64 after_agg_batches =
				worker_context.agg_state != nullptr ? worker_context.agg_state->input_batches_consumed() : 0;
			uint64 after_build_rows =
				worker_context.hash_join_state != nullptr ? worker_context.hash_join_state->build_input_rows_consumed() : 0;
			uint64 after_build_batches =
				worker_context.hash_join_state != nullptr ? worker_context.hash_join_state->build_input_batches_consumed() : 0;
			uint64 after_probe_blocks = 0;
			uint64 after_build_blocks = 0;
			size_t after_hash_entries =
				worker_context.hash_join_state != nullptr ? worker_context.hash_join_state->parallel_hash_entry_count() : 0;
			size_t after_hash_chunks =
				worker_context.hash_join_state != nullptr ? worker_context.hash_join_state->parallel_hash_chunk_count() : 0;
			if (worker_context.root_plan != nullptr)
			{
				auto *probe_scan = worker_context.root_plan->find_parallel_source_scan_state();
				if (probe_scan != nullptr)
					after_probe_blocks = probe_scan->blocks_opened();
			}
			if (worker_context.hash_join_state != nullptr)
			{
				auto *build_scan = worker_context.hash_join_state->find_parallel_build_scan_state();
				if (build_scan != nullptr)
					after_build_blocks = build_scan->blocks_opened();
			}
			elog(trace_elevel,
				 "pg_volvec: leader-only parallel stats pipeline=%u task=%s delta_build_rows=%llu delta_build_batches=%llu delta_build_blocks=%llu delta_hash_entries=%zu delta_hash_chunks=%zu delta_agg_rows=%llu delta_agg_batches=%llu delta_probe_blocks=%llu",
				 task.pipeline_id,
				 ParallelTaskKindName(task.task_kind),
				 (unsigned long long) (after_build_rows - before_build_rows),
				 (unsigned long long) (after_build_batches - before_build_batches),
				 (unsigned long long) (after_build_blocks - before_build_blocks),
				 after_hash_entries - before_hash_entries,
				 after_hash_chunks - before_hash_chunks,
				 (unsigned long long) (after_agg_rows - before_agg_rows),
				 (unsigned long long) (after_agg_batches - before_agg_batches),
				 (unsigned long long) (after_probe_blocks - before_probe_blocks));
		}
		scheduler->finish_task(task);
	}
	if (!started)
		return false;
	if (pg_volvec_trace_hooks && dispatch_count > kMaxLoggedDispatches)
		elog(trace_elevel,
			 "pg_volvec: leader-only parallel dispatch truncated after %d tasks (total=%d)",
			 kMaxLoggedDispatches,
			 dispatch_count);
	if (pg_volvec_trace_hooks)
		elog(trace_elevel,
			 "pg_volvec: leader-only parallel plan completed ready=%s",
			 ParallelReadyTasksName(scheduler).c_str());

	return true;
}

} /* namespace */

extern "C" {

extern bool pg_volvec_trace_hooks;
extern bool pg_volvec_parallel;
extern int pg_volvec_parallel_max_workers;
extern int pg_volvec_parallel_morsel_nblocks;
extern int pg_volvec_parallel_min_relation_blocks;
extern bool pg_volvec_parallel_leader_participation;
extern bool pg_volvec_parallel_experimental_hash_pipeline;

bool pg_volvec_initialize_plan(QueryDesc *queryDesc, pg_volvec::PgVolVecQueryState *state_ptr)
{
	MemoryContext old_context = MemoryContextSwitchTo(state_ptr->context);
	const char *parallel_failure_reason = nullptr;
	const char *scheduler_failure_reason = nullptr;
	Plan *root_plan = queryDesc != nullptr && queryDesc->plannedstmt != nullptr ?
		queryDesc->plannedstmt->planTree : nullptr;

	if (pg_volvec_trace_hooks)
		elog(LOG,
			 "pg_volvec: initialize_plan root_nodeTag=%d parallelModeNeeded=%s operation=%d",
			 root_plan != nullptr ? (int) nodeTag(root_plan) : -1,
			 (queryDesc != nullptr && queryDesc->plannedstmt != nullptr &&
			  queryDesc->plannedstmt->parallelModeNeeded) ? "on" : "off",
			 queryDesc != nullptr ? (int) queryDesc->operation : -1);

	state_ptr->vec_plan = pg_volvec::ExecInitVecPlan(queryDesc->plannedstmt->planTree, queryDesc->estate).release();
	state_ptr->parallel_plan = nullptr;
	state_ptr->parallel_scheduler = nullptr;
	if (pg_volvec_parallel)
	{
		std::unique_ptr<pg_volvec::ParallelPipelinePlan> parallel_plan =
			pg_volvec::BuildParallelPipelinePlan(queryDesc->plannedstmt->planTree,
												 queryDesc->plannedstmt,
												 queryDesc->estate,
												 &parallel_failure_reason);
		if (state_ptr->vec_plan != nullptr)
		{
			state_ptr->parallel_plan = parallel_plan.release();
			if (state_ptr->parallel_plan != nullptr)
			{
				std::unique_ptr<pg_volvec::ParallelSchedulerState> parallel_scheduler =
					pg_volvec::BuildParallelSchedulerState(state_ptr->parallel_plan,
														   state_ptr->context,
														   pg_volvec_parallel_morsel_nblocks,
														   &scheduler_failure_reason);
				state_ptr->parallel_scheduler = parallel_scheduler.release();
			}
		}
		else if (parallel_plan != nullptr && parallel_failure_reason == nullptr)
		{
			parallel_failure_reason =
				"parallel lowering succeeded, but current executor bridge still requires vec_plan";
		}
	}
	MemoryContextSwitchTo(old_context);
	if (state_ptr->vec_plan == nullptr)
		elog(WARNING,
			 "pg_volvec: plan initialization returned null, falling back to PostgreSQL executor%s%s%s",
			 parallel_failure_reason != nullptr ? " (parallel: " : "",
			 parallel_failure_reason != nullptr ? parallel_failure_reason : "",
			 parallel_failure_reason != nullptr ? ")" : "");
	else if (pg_volvec_parallel && state_ptr->parallel_plan == nullptr)
		elog(WARNING, "pg_volvec: vec_plan built but parallel lowering skipped, running volvec serially (reason: %s)",
			 parallel_failure_reason != nullptr ? parallel_failure_reason : "scheduler init");
	else if (!pg_volvec_parallel)
		elog(WARNING, "pg_volvec: pg_volvec.parallel=%s, running volvec serially",
			 pg_volvec_parallel ? "on" : "off");
	else
		elog(WARNING, "pg_volvec: parallel plan built, pipelines=%zu, sched=%s, running parallel",
			 state_ptr->parallel_plan != nullptr ? state_ptr->parallel_plan->pipeline_count() : 0,
			 state_ptr->parallel_scheduler != nullptr ? "ok" : "null");
	if (pg_volvec_trace_hooks && state_ptr->parallel_plan != nullptr)
	{
		elog(LOG,
			 "pg_volvec: parallel lowering initialized (pipelines=%zu, source_pipelines=%zu, root=%u, max_workers=%d, morsel_nblocks=%d, min_relation_blocks=%d, leader=%s)",
			 state_ptr->parallel_plan->pipeline_count(),
			 state_ptr->parallel_plan->source_pipeline_count(),
			 state_ptr->parallel_plan->root_pipeline_id(),
			 pg_volvec_parallel_max_workers,
			 pg_volvec_parallel_morsel_nblocks,
			 pg_volvec_parallel_min_relation_blocks,
			 pg_volvec_parallel_leader_participation ? "on" : "off");
		LogParallelPipelinePlan(state_ptr->parallel_plan);
		if (state_ptr->parallel_scheduler != nullptr)
		{
			elog(LOG,
				 "pg_volvec: parallel scheduler initialized (ready_pipelines=%zu, ready_tasks=%zu, bridges=%zu)",
				 state_ptr->parallel_scheduler->ready_pipeline_count(),
				 state_ptr->parallel_scheduler->ready_task_count(),
				 state_ptr->parallel_scheduler->bridge_count());
			LogParallelSchedulerState(state_ptr->parallel_scheduler);
			TraceParallelSchedulerDryRun(state_ptr->parallel_plan, state_ptr->context);
		}
		else
			elog(LOG,
				 "pg_volvec: parallel scheduler initialization skipped (%s)",
				 scheduler_failure_reason != nullptr ? scheduler_failure_reason : "unknown reason");
	}
	else if (pg_volvec_trace_hooks && pg_volvec_parallel)
		elog(LOG,
			 "pg_volvec: parallel lowering skipped (%s)",
			 parallel_failure_reason != nullptr ? parallel_failure_reason : "unknown reason");
	return state_ptr->vec_plan != nullptr;
}

void pg_volvec_delete_plan(pg_volvec::PgVolVecQueryState *state_ptr)
{
	/*
	 * The parallel lowering metadata is allocated inside state_ptr->context and
	 * only lives until pg_volvec_close_query_state() immediately deletes that
	 * MemoryContext. Let the context reclaim it wholesale instead of walking the
	 * container graph here, which is fragile while ParallelPipelineDesc stores
	 * MemoryContext-backed std::vector members.
	 */
	state_ptr->parallel_scheduler = nullptr;
	state_ptr->parallel_plan = nullptr;
	if (state_ptr->vec_plan) {
		delete state_ptr->vec_plan;
		state_ptr->vec_plan = nullptr;
	}
}

}

static Datum
int64_scaled_to_numeric(int64_t val, int scale)
{
	if (scale <= 0)
		return NumericGetDatum(int64_to_numeric(val));
	return NumericGetDatum(int64_div_fast_to_numeric(val, scale));
}

static Datum
scaled_avg_to_numeric(int64_t scaled_sum, int scale, int64_t count)
{
	Numeric sum_numeric;
	Numeric count_numeric;

	if (count <= 0)
		return NumericGetDatum(int64_to_numeric(0));

	sum_numeric = (scale <= 0) ? int64_to_numeric(scaled_sum)
							   : int64_div_fast_to_numeric(scaled_sum, scale);
	count_numeric = int64_to_numeric(count);
	return NumericGetDatum(numeric_div_safe(sum_numeric, count_numeric, nullptr));
}

static Datum
vec_stringref_to_text_datum(const pg_volvec::DataChunk<pg_volvec::DEFAULT_CHUNK_SIZE> *batch,
							const pg_volvec::VecStringRef &ref)
{
	if (ref.len == 0)
		return PointerGetDatum(cstring_to_text_with_len("", 0));

	if (pg_volvec::VecStringRefIsInline(ref))
	{
		char inline_buf[8];
		memcpy(inline_buf, &ref.prefix, ref.len);
		return PointerGetDatum(cstring_to_text_with_len(inline_buf, ref.len));
	}

	if (ref.offset == pg_volvec::kVecStringInlineOffset)
		elog(ERROR, "pg_volvec invalid inline string reference with length %u", ref.len);

	if (batch == nullptr || ref.offset > batch->string_arena.size() ||
		ref.len > batch->string_arena.size() - ref.offset)
		elog(ERROR, "pg_volvec invalid string arena reference (offset=%u len=%u arena=%zu)",
			 ref.offset, ref.len, batch ? batch->string_arena.size() : 0);

	return PointerGetDatum(cstring_to_text_with_len(batch->string_arena.data() + ref.offset, ref.len));
}

extern "C" {

bool pg_volvec_execute_query(QueryDesc *queryDesc, pg_volvec::PgVolVecQueryState *state_ptr,
								ScanDirection direction, uint64 count)
{
	if (!state_ptr || !state_ptr->vec_plan)
		return false;

	uint64 processed = 0;
	bool send_tuples = (queryDesc->operation == CMD_SELECT || queryDesc->plannedstmt->hasReturning);

	if (state_ptr->parallel_plan != nullptr && state_ptr->parallel_scheduler != nullptr)
	{
		const char *parallel_failure_reason = nullptr;
		bool has_hash_build = ParallelPlanContainsHashBuild(state_ptr->parallel_plan);

		if (has_hash_build && pg_volvec_parallel_experimental_hash_pipeline)
		{
			if (!TryExecuteLeaderOnlyParallelPlan(state_ptr) && pg_volvec_trace_hooks)
				elog(LOG,
					 "pg_volvec: experimental hash pipeline path skipped, falling back to regular vec execution");
		}
		else if (!pg_volvec::TryExecuteProcessParallelAggregate(state_ptr,
																 queryDesc,
																 &parallel_failure_reason))
		{
			if (pg_volvec_trace_hooks)
				elog(LOG,
					 "pg_volvec: process parallel aggregate path skipped (%s), falling back to leader-only",
					 parallel_failure_reason != nullptr ? parallel_failure_reason : "no reason recorded");
			if (has_hash_build && !pg_volvec_parallel_experimental_hash_pipeline)
				return false;
			if (!TryExecuteLeaderOnlyParallelPlan(state_ptr))
				(void) TryExecuteLeaderOnlyParallelAggregate(state_ptr);
		}
	}

	pg_volvec::DataChunk<pg_volvec::DEFAULT_CHUNK_SIZE> *batch = new pg_volvec::DataChunk<pg_volvec::DEFAULT_CHUNK_SIZE>();
	TupleTableSlot *slot = ExecAllocTableSlot(&queryDesc->estate->es_tupleTable, queryDesc->tupDesc, &TTSOpsVirtual);
	if (!slot) { delete batch; return false; }

	queryDesc->estate->es_processed = 0;
	if (send_tuples && queryDesc->dest && queryDesc->dest->rStartup)
		queryDesc->dest->rStartup(queryDesc->dest, queryDesc->operation, queryDesc->tupDesc);

	while (state_ptr->vec_plan->get_next_batch(*batch)) {
		int n = batch->has_selection ? batch->sel.count : batch->count;
		for (int s = 0; s < n; s++) {
			int i = batch->has_selection ? batch->sel.row_ids[s] : s;
			ExecClearTuple(slot);
				for (int j = 0; j < slot->tts_tupleDescriptor->natts && j < 16; j++) {
					Oid typid = TupleDescAttr(slot->tts_tupleDescriptor, j)->atttypid;
					pg_volvec::VecOutputColMeta col_meta;
					bool has_meta = state_ptr->vec_plan->lookup_output_col_meta(j + 1, &col_meta);
					if (batch->nulls[j][i]) {
						slot->tts_isnull[j] = true;
						slot->tts_values[j] = (Datum) 0;
					} else {
						slot->tts_isnull[j] = false;
							if (typid == FLOAT8OID) {
								slot->tts_values[j] = Float8GetDatum(batch->double_columns[j][i]);
							} else if (typid == NUMERICOID) {
								double fval = batch->double_columns[j][i];
								int64_t ival = batch->int64_columns[j][i];

								if (has_meta && col_meta.storage_kind == pg_volvec::VecOutputStorageKind::Double)
									slot->tts_values[j] = DirectFunctionCall1(float8_numeric, Float8GetDatum(fval));
							else if (has_meta && col_meta.storage_kind == pg_volvec::VecOutputStorageKind::NumericScaledInt64)
								slot->tts_values[j] = int64_scaled_to_numeric(ival, col_meta.scale);
							else if (has_meta && col_meta.storage_kind == pg_volvec::VecOutputStorageKind::NumericAvgPair)
								slot->tts_values[j] = scaled_avg_to_numeric(ival, col_meta.scale, (int64_t) batch->double_columns[j][i]);
							else if (ival == 0 && fval != 0.0)
								slot->tts_values[j] = DirectFunctionCall1(float8_numeric, Float8GetDatum(fval));
							else
								slot->tts_values[j] = int64_scaled_to_numeric(ival, pg_volvec::DEFAULT_NUMERIC_SCALE);
							} else if (typid == INT8OID) {
								slot->tts_values[j] = Int64GetDatum(batch->int64_columns[j][i]);
							} else if (typid == BPCHAROID || typid == TEXTOID || typid == VARCHAROID) {
								const pg_volvec::VecStringRef &ref = batch->string_columns[j][i];
								slot->tts_values[j] = vec_stringref_to_text_datum(batch, ref);
							} else {
							slot->tts_values[j] = Int32GetDatum(batch->int32_columns[j][i]);
							}
				}
			}
			ExecStoreVirtualTuple(slot);
			if (queryDesc->dest && queryDesc->dest->receiveSlot)
				queryDesc->dest->receiveSlot(slot, queryDesc->dest);
			processed++;
			if (count != 0 && processed >= count)
				break;
		}
		if (count != 0 && processed >= count)
			break;
	}

	queryDesc->estate->es_processed = processed;
	queryDesc->estate->es_total_processed += processed;
	if (send_tuples && queryDesc->dest && queryDesc->dest->rShutdown)
		queryDesc->dest->rShutdown(queryDesc->dest);
	delete batch;
	return true;
}

}

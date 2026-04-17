#pragma once

#include "exec/plan_state.hpp"

class ParallelPipelinePlan : public PgMemoryContextObject {
public:
	explicit ParallelPipelinePlan(MemoryContext context)
		: memory_context_(context),
		  pipelines_(PgMemoryContextAllocator<ParallelPipelineDesc>(context))
	{
	}

	ParallelPipelineDesc &add_pipeline(ParallelPipelineDriverKind driver_kind)
	{
		pipelines_.emplace_back(memory_context_);
		ParallelPipelineDesc &pipeline = pipelines_.back();

		pipeline.pipeline_id = (uint32_t) (pipelines_.size() - 1);
		pipeline.driver_kind = driver_kind;
		return pipeline;
	}

	void add_dependency(uint32_t pipeline_id, uint32_t dependency_id)
	{
		ParallelPipelineDesc &pipeline = pipelines_[pipeline_id];
		ParallelPipelineDesc &dependency = pipelines_[dependency_id];

		pipeline.dependencies.push_back(dependency_id);
		dependency.successors.push_back(pipeline_id);
	}

	ParallelPipelineDesc *get_pipeline(uint32_t pipeline_id)
	{
		if (pipeline_id >= pipelines_.size())
			return nullptr;
		return &pipelines_[pipeline_id];
	}

	const ParallelPipelineDesc *get_pipeline(uint32_t pipeline_id) const
	{
		if (pipeline_id >= pipelines_.size())
			return nullptr;
		return &pipelines_[pipeline_id];
	}

	size_t pipeline_count() const
	{
		return pipelines_.size();
	}

	size_t source_pipeline_count() const
	{
		size_t count = 0;

		for (const auto &pipeline : pipelines_)
		{
			if (pipeline.source_morsel_driven)
				count++;
		}
		return count;
	}

	void set_root_pipeline(uint32_t pipeline_id)
	{
		root_pipeline_id_ = pipeline_id;
	}

	uint32_t root_pipeline_id() const
	{
		return root_pipeline_id_;
	}

	const VolVecVector<ParallelPipelineDesc> &pipelines() const
	{
		return pipelines_;
	}

private:
	MemoryContext memory_context_;
	VolVecVector<ParallelPipelineDesc> pipelines_;
	uint32_t root_pipeline_id_ = UINT32_MAX;
};

struct ParallelTaskDesc {
	/*
	 * One runnable execution instance owned by a specific pipeline. A worker
	 * never receives a free-floating morsel; it always receives a task that
	 * names the pipeline whose kernel must run.
	 */
	ParallelTaskKind task_kind = ParallelTaskKind::SourceMorsel;
	uint32_t pipeline_id = UINT32_MAX;
	BlockNumber morsel_start_block = InvalidBlockNumber;
	uint32_t morsel_nblocks = 0;
};

struct ParallelBridgeState {
	/* Shared handoff state between a producer pipeline and its consumers. */
	ParallelBridgeKind bridge_kind = ParallelBridgeKind::None;
	uint32_t producer_pipeline_id = UINT32_MAX;
	bool ready = false;
	bool finalized = false;
};

struct ParallelPipelineRuntimeState {
	/*
	 * Mutable scheduler-owned runtime state for one pipeline. This is kept
	 * separate from ParallelPipelineDesc so the static lowering remains stable
	 * while execution mutates queueing, dependency, and progress state.
	 */
	uint32_t pipeline_id = UINT32_MAX;
	uint32_t remaining_dependencies = 0;
	uint32_t completed_predecessors = 0;
	ParallelTaskKind next_task_kind = ParallelTaskKind::SourceMorsel;
	Oid scan_relid = InvalidOid;
	int scan_plan_node_id = -1;
	double estimated_rows = -1.0;
	BlockNumber next_morsel_block = InvalidBlockNumber;
	BlockNumber total_blocks = InvalidBlockNumber;
	uint32_t estimated_morsels = 0;
	bool ready = false;
	bool queued = false;
	bool running = false;
	bool completed = false;
};

class ParallelSchedulerState : public PgMemoryContextObject {
public:
	ParallelSchedulerState(MemoryContext context,
						  const ParallelPipelinePlan *plan,
						  uint32_t source_morsel_nblocks)
		: plan_(plan),
		  source_morsel_nblocks_(source_morsel_nblocks),
		  pipeline_runtime_(PgMemoryContextAllocator<ParallelPipelineRuntimeState>(context)),
		  bridges_(PgMemoryContextAllocator<ParallelBridgeState>(context)),
		  ready_pipeline_ids_(PgMemoryContextAllocator<uint32_t>(context)),
		  ready_tasks_(PgMemoryContextAllocator<ParallelTaskDesc>(context))
	{
	}

	const ParallelPipelinePlan *plan() const
	{
		return plan_;
	}

	ParallelPipelineRuntimeState *get_pipeline_runtime(uint32_t pipeline_id)
	{
		if (pipeline_id >= pipeline_runtime_.size())
			return nullptr;
		return &pipeline_runtime_[pipeline_id];
	}

	const ParallelPipelineRuntimeState *get_pipeline_runtime(uint32_t pipeline_id) const
	{
		if (pipeline_id >= pipeline_runtime_.size())
			return nullptr;
		return &pipeline_runtime_[pipeline_id];
	}

	ParallelBridgeState *get_bridge_state(uint32_t producer_pipeline_id)
	{
		for (auto &bridge : bridges_)
		{
			if (bridge.producer_pipeline_id == producer_pipeline_id)
				return &bridge;
		}
		return nullptr;
	}

	const ParallelBridgeState *get_bridge_state(uint32_t producer_pipeline_id) const
	{
		for (const auto &bridge : bridges_)
		{
			if (bridge.producer_pipeline_id == producer_pipeline_id)
				return &bridge;
		}
		return nullptr;
	}

	size_t ready_pipeline_count() const
	{
		return ready_pipeline_ids_.size();
	}

	size_t bridge_count() const
	{
		return bridges_.size();
	}

	size_t ready_task_count() const
	{
		return ready_tasks_.size();
	}

	const VolVecVector<uint32_t> &ready_pipeline_ids() const
	{
		return ready_pipeline_ids_;
	}

	const VolVecVector<ParallelTaskDesc> &ready_tasks() const
	{
		return ready_tasks_;
	}

	const VolVecVector<ParallelPipelineRuntimeState> &pipeline_runtime() const
	{
		return pipeline_runtime_;
	}

	const VolVecVector<ParallelBridgeState> &bridges() const
	{
		return bridges_;
	}

	void append_pipeline_runtime(const ParallelPipelineRuntimeState &runtime)
	{
		pipeline_runtime_.push_back(runtime);
	}

	void append_bridge(const ParallelBridgeState &bridge)
	{
		bridges_.push_back(bridge);
	}

	void enqueue_ready_pipeline(uint32_t pipeline_id)
	{
		ParallelPipelineRuntimeState *runtime = get_pipeline_runtime(pipeline_id);

		if (runtime == nullptr || runtime->completed || runtime->queued)
			return;
		runtime->ready = true;
		runtime->queued = true;
		ready_pipeline_ids_.push_back(pipeline_id);
		/*
		 * Source pipelines produce block-range morsel tasks. Finalize-style
		 * pipelines enqueue one bridge task until they are rescheduled.
		 */
		if (runtime->next_task_kind == ParallelTaskKind::SourceMorsel)
		{
			BlockNumber start_block = runtime->next_morsel_block;
			uint32_t nblocks = 0;

			if (runtime->total_blocks != InvalidBlockNumber &&
				start_block < runtime->total_blocks)
			{
				BlockNumber remaining = runtime->total_blocks - start_block;
				nblocks = (remaining > source_morsel_nblocks_) ?
					source_morsel_nblocks_ : (uint32_t) remaining;
			}
			ready_tasks_.push_back(ParallelTaskDesc{
				runtime->next_task_kind,
				pipeline_id,
				start_block,
				nblocks
			});
		}
		else
		{
			ready_tasks_.push_back(ParallelTaskDesc{
				runtime->next_task_kind,
				pipeline_id,
				runtime->next_morsel_block,
				0
			});
		}
	}

	bool dequeue_ready_task(ParallelTaskDesc *task_out)
	{
		ParallelPipelineRuntimeState *runtime;
		ParallelTaskDesc task;

		if (ready_tasks_.empty() || ready_pipeline_ids_.empty())
			return false;
		task = ready_tasks_.front();
		ready_tasks_.erase(ready_tasks_.begin());
		ready_pipeline_ids_.erase(ready_pipeline_ids_.begin());
		runtime = get_pipeline_runtime(task.pipeline_id);
		if (runtime == nullptr || runtime->completed)
			return false;
		runtime->queued = false;
		runtime->ready = false;
		runtime->running = true;
		if (task_out != nullptr)
			*task_out = task;
		return true;
	}

	bool mark_pipeline_completed(uint32_t pipeline_id)
	{
		const ParallelPipelineDesc *pipeline_desc;
		ParallelPipelineRuntimeState *runtime = get_pipeline_runtime(pipeline_id);

		if (runtime == nullptr || runtime->completed)
			return false;
		pipeline_desc = plan_ != nullptr ? plan_->get_pipeline(pipeline_id) : nullptr;
		runtime->running = false;
		runtime->ready = false;
		runtime->completed = true;
		if (pipeline_desc != nullptr && pipeline_desc->output_bridge != ParallelBridgeKind::None)
		{
			ParallelBridgeState *bridge = get_bridge_state(pipeline_id);

			if (bridge != nullptr)
			{
				bridge->ready = true;
				bridge->finalized = true;
			}
		}
		if (pipeline_desc == nullptr)
			return true;
		for (uint32_t successor_id : pipeline_desc->successors)
		{
			ParallelPipelineRuntimeState *successor = get_pipeline_runtime(successor_id);

			if (successor == nullptr || successor->completed)
				continue;
			successor->completed_predecessors++;
			if (successor->remaining_dependencies > 0)
				successor->remaining_dependencies--;
			if (successor->remaining_dependencies == 0)
				enqueue_ready_pipeline(successor_id);
		}
		return true;
	}

	bool finish_task(const ParallelTaskDesc &task)
	{
		ParallelPipelineRuntimeState *runtime = get_pipeline_runtime(task.pipeline_id);

		if (runtime == nullptr || runtime->completed)
			return false;
		runtime->running = false;
		runtime->ready = false;
		runtime->queued = false;

		if (task.task_kind == ParallelTaskKind::SourceMorsel &&
			runtime->total_blocks != InvalidBlockNumber)
		{
			BlockNumber next_block = task.morsel_start_block + task.morsel_nblocks;

			runtime->next_morsel_block = next_block;
			if (next_block < runtime->total_blocks)
			{
				enqueue_ready_pipeline(task.pipeline_id);
				return true;
			}
		}

		return mark_pipeline_completed(task.pipeline_id);
	}

private:
	const ParallelPipelinePlan *plan_;
	uint32_t source_morsel_nblocks_;
	VolVecVector<ParallelPipelineRuntimeState> pipeline_runtime_;
	VolVecVector<ParallelBridgeState> bridges_;
	VolVecVector<uint32_t> ready_pipeline_ids_;
	VolVecVector<ParallelTaskDesc> ready_tasks_;
};

struct ParallelWorkerContext {
	/*
	 * Process-local execution context used by a leader or worker while running
	 * one scheduled task. This intentionally holds local executor objects,
	 * not DSM-visible state.
	 */
	MemoryContext memory_context = nullptr;
	PlannedStmt *plannedstmt = nullptr;
	EState *estate = nullptr;
	VecPlanState *root_plan = nullptr;
	VecAggState *agg_state = nullptr;
	VecHashJoinState *hash_join_state = nullptr;
	int agg_plan_node_id = -1;
	int hash_join_plan_node_id = -1;
	int input_hash_join_plan_node_id = -1;
	Oid parallel_scan_relid = InvalidOid;
	int parallel_scan_plan_node_id = -1;
	ParallelTableScanDesc parallel_scan_desc = nullptr;
	bool leader = false;
};

namespace {

enum class ParallelWorkerExecutionMode : uint32_t {
	AggregateProbe = 1,
	HashBuild = 2,
	HashBuildWave = 3,
	QueryScheduler = 4
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
		case ParallelWorkerExecutionMode::HashBuildWave:
			return "HashBuildWave";
		case ParallelWorkerExecutionMode::QueryScheduler:
			return "QueryScheduler";
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
static constexpr uint64 VOLVEC_PARALLEL_KEY_HASH_BUILD_WAVE_PIPELINES =
	UINT64CONST(0xD700000000000009);
static constexpr uint64 VOLVEC_PARALLEL_KEY_QUERY_SCHEDULER =
	UINT64CONST(0xD70000000000000A);
static constexpr uint64 VOLVEC_PARALLEL_KEY_QUERY_PIPELINES =
	UINT64CONST(0xD70000000000000B);
static constexpr uint64 VOLVEC_PARALLEL_KEY_QUERY_TASKS =
	UINT64CONST(0xD70000000000000C);
static constexpr uint64 VOLVEC_PARALLEL_KEY_QUERY_SUCCESSORS =
	UINT64CONST(0xD70000000000000D);
static constexpr uint64 VOLVEC_PARALLEL_KEY_QUERY_AGG_PARTIALS =
	UINT64CONST(0xD70000000000000E);
static constexpr uint64 VOLVEC_PARALLEL_KEY_QUERY_HASH_PARTIALS =
	UINT64CONST(0xD70000000000000F);
static constexpr uint64 VOLVEC_PARALLEL_KEY_PARAM_EXEC =
	UINT64CONST(0xD700000000000010);
static constexpr uint64 VOLVEC_PARALLEL_KEY_QUERY_SORT_RUNS =
	UINT64CONST(0xD700000000000011);
static constexpr uint64 VOLVEC_PARALLEL_KEY_WAVE_SOURCE_PSCAN_BASE =
	UINT64CONST(0xD700000000001000);
static constexpr uint64 VOLVEC_PARALLEL_KEY_QUERY_SOURCE_PSCAN_BASE =
	UINT64CONST(0xD700000000002000);
static constexpr uint32 VOLVEC_PARALLEL_MAGIC = 0x56565050;
static constexpr uint32 VOLVEC_QUERY_SCHEDULER_MAGIC = 0x56565153;
static constexpr uint32 VOLVEC_SHARED_HASH_BRIDGE_PACK_MAGIC = 0x56564850;
static constexpr uint32 VOLVEC_SHARED_HASH_BRIDGE_PACK_VERSION = 1;
static constexpr uint32_t VOLVEC_PARALLEL_HASH_BUILD_DOMINANCE_RATIO = 4;
static constexpr double VOLVEC_PARALLEL_HASH_BUILD_SMALL_ROWS_RATIO = 0.25;
static constexpr size_t VOLVEC_PARALLEL_LARGE_SHARED_HASH_BRIDGE_BYTES =
	(size_t) 64 * 1024 * 1024;

static uint64
RuntimeElapsedUsSince(instr_time start)
{
	instr_time end;

	INSTR_TIME_SET_CURRENT(end);
	INSTR_TIME_SUBTRACT(end, start);
	return (uint64) INSTR_TIME_GET_MICROSEC(end);
}

struct SerializedSharedHashBridgePackHeader
{
	uint32 magic = VOLVEC_SHARED_HASH_BRIDGE_PACK_MAGIC;
	uint32 version = VOLVEC_SHARED_HASH_BRIDGE_PACK_VERSION;
	uint32 bridge_count = 0;
	uint32 reserved = 0;
};

struct SerializedSharedHashBridgePackEntryHeader
{
	int32 hash_join_plan_node_id = -1;
	uint32 bridge_size = 0;
	uint32 reserved0 = 0;
	uint32 reserved1 = 0;
};

struct ParallelAggregateSharedControl
{
	uint32 magic;
	uint32 source_pipeline_id;
	uint32 partial_slot_count;
	uint32 wave_pipeline_count;
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

struct ParallelHashBuildWavePipelineShared
{
	uint32 pipeline_id;
	Oid source_scan_relid;
	int source_scan_plan_node_id;
	int hash_join_plan_node_id;
	int input_hash_join_plan_node_id;
	uint32 total_blocks;
	pg_atomic_uint64 next_block;
};

enum class ParallelQueryTaskState : uint32_t {
	Pending = 0,
	Running = 1,
	Done = 2
};

struct ParallelQuerySchedulerShared
{
	uint32 magic = VOLVEC_QUERY_SCHEDULER_MAGIC;
	uint32 pipeline_count = 0;
	uint32 task_count = 0;
	uint32 worker_count = 0;
	uint32 morsel_nblocks = 0;
	pg_atomic_uint32 next_task_scan;
	pg_atomic_uint32 completed_tasks;
	pg_atomic_uint32 shutdown;
	pg_atomic_uint32 error;
};

struct ParallelQueryPipelineShared
{
	uint32 pipeline_id = UINT32_MAX;
	uint32 role = 0;
	uint32 driver_kind = 0;
	uint32 task_kind = 0;
	uint32 input_bridge = 0;
	uint32 output_bridge = 0;
	uint32 total_dependencies = 0;
	uint32 total_tasks = 0;
	pg_atomic_uint32 remaining_dependencies;
	pg_atomic_uint32 completed_tasks;
	pg_atomic_uint32 published_workers;
	pg_atomic_uint32 completed;
	Oid source_scan_relid = InvalidOid;
	int source_scan_plan_node_id = -1;
	int agg_plan_node_id = -1;
	int hash_join_plan_node_id = -1;
	int input_hash_join_plan_node_id = -1;
	uint32 successor_start = 0;
	uint32 successor_count = 0;
	pg_atomic_uint32 hash_bridge_ready;
	uint64 hash_bridge_size = 0;
	uint64 hash_bridge_entries = 0;
	uint64 hash_bridge_chunks = 0;
	char hash_bridge_file_name[VOLVEC_PARALLEL_MAX_PARTIAL_FILE_NAME] = {0};
};

struct ParallelQueryTaskShared
{
	uint32 task_id = UINT32_MAX;
	uint32 pipeline_id = UINT32_MAX;
	uint32 task_kind = 0;
	BlockNumber morsel_start_block = InvalidBlockNumber;
	uint32 morsel_nblocks = 0;
	pg_atomic_uint32 state;
};

struct ParallelSortRunState
{
	uint32 task_id = UINT32_MAX;
	uint32 pipeline_id = UINT32_MAX;
	uint32 chunk_count = 0;
	uint64 row_count = 0;
	uint64 file_bytes = 0;
	char file_name[VOLVEC_PARALLEL_MAX_PARTIAL_FILE_NAME] = {0};
};

struct ParallelRowChunkHeader
{
	uint32 row_count = 0;
	uint32 string_arena_size = 0;
};

static bool
WriteDataChunkRows(BufFile *file, const DataChunk<DEFAULT_CHUNK_SIZE> &chunk)
{
	ParallelRowChunkHeader header;

	if (file == nullptr)
		return false;
	header.row_count = chunk.has_selection ? chunk.sel.count : chunk.count;
	header.string_arena_size = (uint32) chunk.string_arena.size();
	if (!BufFileWriteAllLocal(file, &header, sizeof(header)))
		return false;
	for (int col = 0; col < 16; col++)
	{
		for (uint32 r = 0; r < header.row_count; r++)
		{
			int src_row = chunk.has_selection ? chunk.sel.row_ids[r] : (int) r;

			if (!BufFileWriteAllLocal(file, &chunk.nulls[col][src_row], sizeof(uint8_t)) ||
				!BufFileWriteAllLocal(file, &chunk.int32_columns[col][src_row], sizeof(int32_t)) ||
				!BufFileWriteAllLocal(file, &chunk.int64_columns[col][src_row], sizeof(int64_t)) ||
				!BufFileWriteAllLocal(file, &chunk.double_columns[col][src_row], sizeof(double)) ||
				!BufFileWriteAllLocal(file, &chunk.string_columns[col][src_row], sizeof(VecStringRef)))
				return false;
		}
	}
	if (header.string_arena_size > 0 &&
		!BufFileWriteAllLocal(file,
							  chunk.string_arena.data(),
							  header.string_arena_size))
		return false;
	return true;
}

static bool
ReadDataChunkRows(BufFile *file, DataChunk<DEFAULT_CHUNK_SIZE> *chunk)
{
	ParallelRowChunkHeader header;

	if (file == nullptr || chunk == nullptr)
		return false;
	chunk->reset();
	if (!BufFileReadAllLocal(file, &header, sizeof(header), false))
		return false;
	if (header.row_count > DEFAULT_CHUNK_SIZE)
		return false;
	chunk->count = (uint16) header.row_count;
	for (int col = 0; col < 16; col++)
	{
		for (uint32 r = 0; r < header.row_count; r++)
		{
			if (!BufFileReadAllLocal(file, &chunk->nulls[col][r], sizeof(uint8_t), false) ||
				!BufFileReadAllLocal(file, &chunk->int32_columns[col][r], sizeof(int32_t), false) ||
				!BufFileReadAllLocal(file, &chunk->int64_columns[col][r], sizeof(int64_t), false) ||
				!BufFileReadAllLocal(file, &chunk->double_columns[col][r], sizeof(double), false) ||
				!BufFileReadAllLocal(file, &chunk->string_columns[col][r], sizeof(VecStringRef), false))
				return false;
		}
	}
	chunk->string_arena.resize(header.string_arena_size);
	if (header.string_arena_size > 0 &&
		!BufFileReadAllLocal(file,
							 chunk->string_arena.data(),
							 header.string_arena_size,
							 false))
		return false;
	return true;
}

struct ParallelParamExecHeader
{
	uint32 magic = 0x56565045;
	uint32 count = 0;
	uint32 bytes = 0;
	uint32 reserved = 0;
};

struct ParallelParamExecEntry
{
	Oid type = InvalidOid;
	int16 typ_len = 0;
	bool typ_byval = false;
	bool valid = false;
	bool isnull = true;
	uint8 reserved = 0;
	uint32 offset = 0;
	uint32 size = 0;
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

static void
CleanupLocalParallelAggregateProcessState(LocalParallelAggregateProcessState *local);

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
												 const char **failure_reason,
												 const uint8_t *serialized_param_exec = nullptr,
												 size_t serialized_param_exec_size = 0);

static bool
ExecuteParallelWorkerSourceLoop(ParallelWorkerExecutionMode mode,
									ParallelWorkerContext &worker_context,
									const char **failure_reason);

static void
PopulateHashBuildPartialDiagnostics(const LocalParallelAggregateProcessState &local,
									 ParallelHashBuildPartialState *partial);

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
CollectHashBuildDependenciesPostOrder(const ParallelPipelinePlan *parallel_plan,
										 uint32_t pipeline_id,
										 uint32_t depth,
										 VolVecVector<uint32_t> *out_pipeline_ids,
										 VolVecVector<uint8_t> *seen_pipeline_ids)
{
	const ParallelPipelineDesc *pipeline;
	bool found = false;

	if (parallel_plan == nullptr ||
		out_pipeline_ids == nullptr ||
		seen_pipeline_ids == nullptr ||
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
		if (CollectHashBuildDependenciesPostOrder(parallel_plan,
													 dependency_id,
													 depth + 1,
													 out_pipeline_ids,
													 seen_pipeline_ids))
			found = true;
		if (dependency->role == ParallelPipelineRole::HashBuildSource &&
			dependency_id < seen_pipeline_ids->size() &&
			(*seen_pipeline_ids)[dependency_id] == 0)
		{
			(*seen_pipeline_ids)[dependency_id] = 1;
			out_pipeline_ids->push_back(dependency_id);
			found = true;
		}
	}

	return found;
}

static bool
HashBuildDependenciesSatisfied(const ParallelPipelinePlan *parallel_plan,
								 uint32_t pipeline_id,
								 const VolVecVector<uint8_t> &completed_build_pipelines)
{
	VolVecVector<uint32_t> dependency_build_pipelines{
		PgMemoryContextAllocator<uint32_t>(CurrentMemoryContext)};
	VolVecVector<uint8_t> seen_pipeline_ids{
		PgMemoryContextAllocator<uint8_t>(CurrentMemoryContext)};

	if (parallel_plan == nullptr || pipeline_id >= parallel_plan->pipeline_count())
		return false;

	seen_pipeline_ids.resize(parallel_plan->pipeline_count(), 0);
	(void) CollectHashBuildDependenciesPostOrder(parallel_plan,
												 pipeline_id,
												 0,
												 &dependency_build_pipelines,
												 &seen_pipeline_ids);
	for (uint32_t dependency_id : dependency_build_pipelines)
	{
		if (dependency_id >= completed_build_pipelines.size() ||
			completed_build_pipelines[dependency_id] == 0)
			return false;
	}
	return true;
}

static bool
ReadPackedBridgeBytes(const uint8_t *buffer,
						 size_t buffer_size,
						 size_t *offset,
						 void *dst,
						 size_t size)
{
	if (buffer == nullptr || offset == nullptr || dst == nullptr)
		return false;
	if (*offset > buffer_size || size > (buffer_size - *offset))
		return false;
	memcpy(dst, buffer + *offset, size);
	*offset += size;
	return true;
}

static bool
TryAttachSharedHashBridgePack(VecPlanState *root_plan,
								 const uint8_t *buffer,
								 size_t buffer_size,
								 bool leader,
								 const char **failure_reason)
{
	SerializedSharedHashBridgePackHeader header{};
	size_t offset = 0;

	if (failure_reason != nullptr)
		*failure_reason = nullptr;
	if (root_plan == nullptr || buffer == nullptr || buffer_size < sizeof(header))
		return false;
	if (!ReadPackedBridgeBytes(buffer, buffer_size, &offset, &header, sizeof(header)))
		return false;
	if (header.magic != VOLVEC_SHARED_HASH_BRIDGE_PACK_MAGIC ||
		header.version != VOLVEC_SHARED_HASH_BRIDGE_PACK_VERSION)
		return false;

	for (uint32 i = 0; i < header.bridge_count; i++)
	{
		SerializedSharedHashBridgePackEntryHeader entry_header{};
		VecHashJoinState *hash_join_state;

		if (!ReadPackedBridgeBytes(buffer,
								   buffer_size,
								   &offset,
								   &entry_header,
								   sizeof(entry_header)))
		{
			if (failure_reason != nullptr)
				*failure_reason = "parallel shared hash bridge pack header is truncated";
			return false;
		}
		if (entry_header.bridge_size == 0 ||
			offset > buffer_size ||
			entry_header.bridge_size > (buffer_size - offset))
		{
			if (failure_reason != nullptr)
				*failure_reason = "parallel shared hash bridge pack entry is invalid";
			return false;
		}
		hash_join_state =
			root_plan->find_parallel_hash_join_state_by_plan_node_id(
				entry_header.hash_join_plan_node_id);
		if (hash_join_state == nullptr)
		{
			if (failure_reason != nullptr)
				*failure_reason =
					"parallel shared hash bridge pack could not resolve hash join state";
			return false;
		}
		hash_join_state->attach_shared_hash_bridge(buffer + offset,
													 entry_header.bridge_size);
		hash_join_state->load_hash_bridge();
		if (pg_volvec_trace_hooks)
			elog(LOG,
				 "pg_volvec: parallel %s attached packed shared hash bridge plan_node_id=%d bytes=%u entries=%zu chunks=%zu",
				 leader ? "leader" : "worker",
				 entry_header.hash_join_plan_node_id,
				 entry_header.bridge_size,
				 hash_join_state->parallel_hash_entry_count(),
				 hash_join_state->parallel_hash_chunk_count());
		offset += entry_header.bridge_size;
	}

	if (offset != buffer_size)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel shared hash bridge pack has trailing bytes";
		return false;
	}

	return true;
}

static bool
BuildPublishedSharedHashBridgePack(PgVolVecQueryState *query_state,
									  const ParallelPipelineDesc *probe_pipeline,
									  uint8_t **buffer_out,
									  size_t *buffer_size_out,
									  uint32 *bridge_count_out,
									  const char **failure_reason)
{
	VolVecVector<uint32_t> build_pipeline_ids{PgMemoryContextAllocator<uint32_t>(CurrentMemoryContext)};
	VolVecVector<uint8_t> seen_pipeline_ids{PgMemoryContextAllocator<uint8_t>(CurrentMemoryContext)};
	VolVecVector<SerializedSharedHashBridgePackEntryHeader> entry_headers{
		PgMemoryContextAllocator<SerializedSharedHashBridgePackEntryHeader>(CurrentMemoryContext)};
	VolVecVector<VecHashJoinState *> hash_join_states{
		PgMemoryContextAllocator<VecHashJoinState *>(CurrentMemoryContext)};
	size_t total_size = sizeof(SerializedSharedHashBridgePackHeader);
	size_t offset = 0;
	uint8_t *pack_buffer;
	SerializedSharedHashBridgePackHeader pack_header{};

	if (buffer_out != nullptr)
		*buffer_out = nullptr;
	if (buffer_size_out != nullptr)
		*buffer_size_out = 0;
	if (bridge_count_out != nullptr)
		*bridge_count_out = 0;
	if (failure_reason != nullptr)
		*failure_reason = nullptr;
	if (query_state == nullptr ||
		query_state->parallel_plan == nullptr ||
		query_state->vec_plan == nullptr ||
		probe_pipeline == nullptr)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel shared hash bridge pack has incomplete state";
		return false;
	}

	seen_pipeline_ids.resize(query_state->parallel_plan->pipeline_count(), 0);
		if (!CollectHashBuildDependenciesPostOrder(query_state->parallel_plan,
														 probe_pipeline->pipeline_id,
														 0,
														 &build_pipeline_ids,
														 &seen_pipeline_ids))
			return true;

	for (uint32_t build_pipeline_id : build_pipeline_ids)
	{
		const ParallelPipelineDesc *build_pipeline =
			query_state->parallel_plan->get_pipeline(build_pipeline_id);
		VecHashJoinState *hash_join_state;
		SerializedSharedHashBridgePackEntryHeader entry_header{};
		size_t bridge_size;

		if (build_pipeline == nullptr || build_pipeline->hash_join_plan_node_id < 0)
		{
			if (failure_reason != nullptr)
				*failure_reason =
					"parallel shared hash bridge pack found invalid build pipeline";
			return false;
		}
		hash_join_state =
			query_state->vec_plan->find_parallel_hash_join_state_by_plan_node_id(
				build_pipeline->hash_join_plan_node_id);
		if (hash_join_state == nullptr)
		{
			if (failure_reason != nullptr)
				*failure_reason =
					"parallel shared hash bridge pack could not resolve leader hash join state";
			return false;
		}
			if (hash_join_state->shared_hash_bridge_buffer() == nullptr ||
				hash_join_state->shared_hash_bridge_size() == 0)
			{
				if (failure_reason != nullptr)
					*failure_reason =
						"parallel shared hash bridge pack found unpublished build dependency";
				return false;
			}
			bridge_size = hash_join_state->shared_hash_bridge_size();
		if (bridge_size > MaxAllocSize ||
			total_size > MaxAllocSize ||
			sizeof(entry_header) > (MaxAllocSize - total_size) ||
			bridge_size > (MaxAllocSize - total_size - sizeof(entry_header)))
		{
			if (failure_reason != nullptr)
				*failure_reason = "leader-built shared hash bridge pack exceeds MaxAllocSize";
			return false;
		}
			entry_header.hash_join_plan_node_id = build_pipeline->hash_join_plan_node_id;
			entry_header.bridge_size = (uint32) bridge_size;
			entry_headers.push_back(entry_header);
			hash_join_states.push_back(hash_join_state);
		total_size += sizeof(entry_header) + bridge_size;
	}

	pack_header.bridge_count = (uint32) entry_headers.size();
	pack_buffer = (uint8_t *) MemoryContextAlloc(query_state->context, total_size);
	memcpy(pack_buffer + offset, &pack_header, sizeof(pack_header));
	offset += sizeof(pack_header);
	for (size_t i = 0; i < entry_headers.size(); i++)
	{
			memcpy(pack_buffer + offset, &entry_headers[i], sizeof(entry_headers[i]));
			offset += sizeof(entry_headers[i]);
			memcpy(pack_buffer + offset,
				   hash_join_states[i]->shared_hash_bridge_buffer(),
				   entry_headers[i].bridge_size);
			offset += entry_headers[i].bridge_size;
		}
		if (offset != total_size)
		{
			if (failure_reason != nullptr)
				*failure_reason = "parallel shared hash bridge pack size mismatch";
			return false;
		}

	if (buffer_out != nullptr)
		*buffer_out = pack_buffer;
	if (buffer_size_out != nullptr)
		*buffer_size_out = total_size;
	if (bridge_count_out != nullptr)
		*bridge_count_out = pack_header.bridge_count;
	return true;
}

static bool
BuildPublishedSharedHashBridgePackFromCompletedIds(
	PgVolVecQueryState *query_state,
	const VolVecVector<uint32_t> &build_pipeline_ids,
	const VolVecVector<uint8_t> &completed_build_pipelines,
	uint8_t **buffer_out,
	size_t *buffer_size_out,
	uint32 *bridge_count_out,
	const char **failure_reason)
{
	VolVecVector<SerializedSharedHashBridgePackEntryHeader> entry_headers{
		PgMemoryContextAllocator<SerializedSharedHashBridgePackEntryHeader>(CurrentMemoryContext)};
	VolVecVector<VecHashJoinState *> hash_join_states{
		PgMemoryContextAllocator<VecHashJoinState *>(CurrentMemoryContext)};
	size_t total_size = sizeof(SerializedSharedHashBridgePackHeader);
	size_t offset = 0;
	uint8_t *pack_buffer;
	SerializedSharedHashBridgePackHeader pack_header{};

	if (buffer_out != nullptr)
		*buffer_out = nullptr;
	if (buffer_size_out != nullptr)
		*buffer_size_out = 0;
	if (bridge_count_out != nullptr)
		*bridge_count_out = 0;
	if (failure_reason != nullptr)
		*failure_reason = nullptr;
	if (query_state == nullptr || query_state->parallel_plan == nullptr || query_state->vec_plan == nullptr)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel shared hash bridge pack has incomplete state";
		return false;
	}

	for (uint32_t build_pipeline_id : build_pipeline_ids)
	{
		const ParallelPipelineDesc *build_pipeline;
		VecHashJoinState *hash_join_state;
		SerializedSharedHashBridgePackEntryHeader entry_header{};
		size_t bridge_size;

		if (build_pipeline_id >= completed_build_pipelines.size() ||
			completed_build_pipelines[build_pipeline_id] == 0)
			continue;
		build_pipeline = query_state->parallel_plan->get_pipeline(build_pipeline_id);
		if (build_pipeline == nullptr || build_pipeline->hash_join_plan_node_id < 0)
		{
			if (failure_reason != nullptr)
				*failure_reason =
					"parallel shared hash bridge pack found invalid completed pipeline";
			return false;
		}
		hash_join_state =
			query_state->vec_plan->find_parallel_hash_join_state_by_plan_node_id(
				build_pipeline->hash_join_plan_node_id);
		if (hash_join_state == nullptr ||
			hash_join_state->shared_hash_bridge_buffer() == nullptr ||
			hash_join_state->shared_hash_bridge_size() == 0)
		{
			if (failure_reason != nullptr)
				*failure_reason =
					"parallel shared hash bridge pack found unpublished completed dependency";
			return false;
		}
		bridge_size = hash_join_state->shared_hash_bridge_size();
		if (bridge_size > MaxAllocSize ||
			total_size > MaxAllocSize ||
			sizeof(entry_header) > (MaxAllocSize - total_size) ||
			bridge_size > (MaxAllocSize - total_size - sizeof(entry_header)))
		{
			if (failure_reason != nullptr)
				*failure_reason = "parallel shared hash bridge pack exceeds MaxAllocSize";
			return false;
		}
		entry_header.hash_join_plan_node_id = build_pipeline->hash_join_plan_node_id;
		entry_header.bridge_size = (uint32) bridge_size;
		entry_headers.push_back(entry_header);
		hash_join_states.push_back(hash_join_state);
		total_size += sizeof(entry_header) + bridge_size;
	}

	if (entry_headers.empty())
		return true;

	pack_header.bridge_count = (uint32) entry_headers.size();
	pack_buffer = (uint8_t *) MemoryContextAlloc(query_state->context, total_size);
	memcpy(pack_buffer + offset, &pack_header, sizeof(pack_header));
	offset += sizeof(pack_header);
	for (size_t i = 0; i < entry_headers.size(); i++)
	{
		memcpy(pack_buffer + offset, &entry_headers[i], sizeof(entry_headers[i]));
		offset += sizeof(entry_headers[i]);
		memcpy(pack_buffer + offset,
			   hash_join_states[i]->shared_hash_bridge_buffer(),
			   entry_headers[i].bridge_size);
		offset += entry_headers[i].bridge_size;
	}
	if (offset != total_size)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel shared hash bridge pack size mismatch";
		return false;
	}

	if (buffer_out != nullptr)
		*buffer_out = pack_buffer;
	if (buffer_size_out != nullptr)
		*buffer_size_out = total_size;
	if (bridge_count_out != nullptr)
		*bridge_count_out = pack_header.bridge_count;
	return true;
}

[[maybe_unused]] static bool
TryExecuteParallelHashBuildPipeline(PgVolVecQueryState *query_state,
										QueryDesc *queryDesc,
										const ParallelPipelineDesc *build_pipeline,
										const uint8_t *shared_hash_bridge,
										size_t shared_hash_bridge_len,
										const char **failure_reason)
{
	const ParallelPipelineRuntimeState *build_runtime = nullptr;
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
	bool run_leader_build = false;
	int requested_workers;
	size_t total_partial_entries = 0;
	size_t total_partial_chunks = 0;
	VecHashJoinState *target_hash_join_state = nullptr;

	if (failure_reason != nullptr)
		*failure_reason = nullptr;
	if (query_state == nullptr ||
		query_state->parallel_plan == nullptr ||
		query_state->parallel_scheduler == nullptr ||
		queryDesc == nullptr ||
		build_pipeline == nullptr)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel hash build pipeline has incomplete state";
		return false;
	}
	build_runtime =
		query_state->parallel_scheduler->get_pipeline_runtime(build_pipeline->pipeline_id);
	if (build_runtime == nullptr)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel hash build pipeline has no runtime state";
		return false;
	}
	if (build_pipeline->driver_kind != ParallelPipelineDriverKind::SourceScan ||
		build_pipeline->scan_relid == InvalidOid ||
		build_pipeline->scan_plan_node_id < 0)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel hash build pipeline requires a source-scan build pipeline";
		return false;
	}
	if (build_runtime->total_blocks == InvalidBlockNumber || build_runtime->total_blocks == 0)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel hash build pipeline found no eligible build-side blocks";
		return false;
	}
	if (build_pipeline->hash_join_plan_node_id < 0)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel hash build pipeline missing hash join plan node id";
		return false;
	}

	target_hash_join_state =
		query_state->vec_plan->find_parallel_hash_join_state_by_plan_node_id(
			build_pipeline->hash_join_plan_node_id);
	if (target_hash_join_state == nullptr)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel hash build pipeline could not resolve build-side hash join state";
		return false;
	}
	target_hash_join_state->reset_parallel_hash_build_state();

	requested_workers = Min(pg_volvec_parallel_max_workers,
							(int) Max(build_runtime->estimated_morsels, 1u));
	if (requested_workers <= 0)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel hash build pipeline requested zero workers";
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
	if (shared_hash_bridge != nullptr && shared_hash_bridge_len > 0)
	{
		shm_toc_estimate_chunk(&pcxt->estimator, shared_hash_bridge_len);
		shm_toc_estimate_keys(&pcxt->estimator, 1);
	}

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
	control->wave_pipeline_count = 0;
	control->morsel_nblocks = pg_volvec_parallel_morsel_nblocks;
	control->total_blocks = build_runtime->total_blocks;
	control->source_scan_relid = build_pipeline->scan_relid;
	control->source_scan_plan_node_id = build_pipeline->scan_plan_node_id;
	control->agg_plan_node_id = -1;
	control->hash_join_plan_node_id = build_pipeline->hash_join_plan_node_id;
	control->input_hash_join_plan_node_id = build_pipeline->input_hash_join_plan_node_id;
	control->execution_mode = (uint32) ParallelWorkerExecutionMode::HashBuild;
	control->need_hash_join_state = true;
	control->hash_bridge_ready =
		(shared_hash_bridge != nullptr && shared_hash_bridge_len > 0) ? 1 : 0;
	control->reserved[0] = 0;
	control->reserved[1] = 0;
	control->hash_bridge_size = shared_hash_bridge_len;
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
	if (shared_hash_bridge != nullptr && shared_hash_bridge_len > 0)
	{
		uint8_t *shared_hash_bridge_dsm =
			(uint8_t *) shm_toc_allocate(pcxt->toc, shared_hash_bridge_len);
		memcpy(shared_hash_bridge_dsm, shared_hash_bridge, shared_hash_bridge_len);
		shm_toc_insert(pcxt->toc, VOLVEC_PARALLEL_KEY_HASH_BRIDGE, shared_hash_bridge_dsm);
	}
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
	run_leader_build =
		pg_volvec_parallel_leader_participation && pcxt->nworkers_launched == 0;

	if (run_leader_build)
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
																 shared_hash_bridge,
																 shared_hash_bridge_len,
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
	else if (pg_volvec_trace_hooks && pcxt->nworkers_launched > 0)
		elog(LOG,
			 "pg_volvec: hash build pipeline %u using worker-only build participation (leader local build disabled)",
			 build_pipeline->pipeline_id);

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
	target_hash_join_state->reserve_parallel_hash_build_capacity(
		total_partial_entries,
		total_partial_chunks);

	if (leader_local_initialized)
	{
		BufFile *file = BufFileOpenFileSet(&partial_fileset->fs,
											 leader_partial.file_name,
											 O_RDONLY,
											 false);

		if (file == nullptr ||
			!target_hash_join_state->merge_parallel_build_partial_file(file, leader_partial))
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

		file = BufFileOpenFileSet(&partial_fileset->fs,
								 build_partials[i].file_name,
								 O_RDONLY,
								 false);
		if (file == nullptr ||
			!target_hash_join_state->merge_parallel_build_partial_file(file, build_partials[i]))
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

	target_hash_join_state->finish_parallel_hash_build();
	if (target_hash_join_state->estimate_parallel_hash_bridge_size() > MaxAllocSize)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel hash build shared bridge exceeds MaxAllocSize";
		goto done;
	}
	target_hash_join_state->publish_hash_bridge();
	if (target_hash_join_state->shared_hash_bridge_buffer() == nullptr ||
		target_hash_join_state->shared_hash_bridge_size() == 0)
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

static bool
TryExecuteParallelHashBuildWave(PgVolVecQueryState *query_state,
								  QueryDesc *queryDesc,
								  const VolVecVector<uint32_t> &ready_wave,
								  const VolVecVector<uint32_t> &all_build_pipeline_ids,
								  const VolVecVector<uint8_t> &completed_build_pipelines,
								  const char **failure_reason)
{
	const char *plannedstmt_serialized = nullptr;
	const char *query_text = nullptr;
	char *plannedstmt_serialized_owned = nullptr;
	size_t plannedstmt_len = 0;
	size_t query_text_len = 0;
	size_t build_partials_len = 0;
	size_t shared_hash_bridge_len = 0;
	uint8_t *shared_hash_bridge = nullptr;
	uint32 shared_hash_bridge_count = 0;
	ParallelContext *pcxt = nullptr;
	ParallelAggregateSharedControl *control = nullptr;
	ParallelHashBuildWavePipelineShared *wave_pipelines = nullptr;
	ParallelHashBuildPartialState *build_partials = nullptr;
	SharedFileSet *partial_fileset = nullptr;
	VolVecVector<Relation> source_rels{PgMemoryContextAllocator<Relation>(CurrentMemoryContext)};
	VolVecVector<size_t> source_pscan_lens{PgMemoryContextAllocator<size_t>(CurrentMemoryContext)};
	bool entered_parallel = false;
	bool success = false;
	int requested_workers = 0;

	if (failure_reason != nullptr)
		*failure_reason = nullptr;
	if (query_state == nullptr ||
		query_state->parallel_plan == nullptr ||
		query_state->parallel_scheduler == nullptr ||
		query_state->vec_plan == nullptr ||
		queryDesc == nullptr ||
		ready_wave.empty())
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel hash build wave has incomplete state";
		return false;
	}

	if (!BuildPublishedSharedHashBridgePackFromCompletedIds(query_state,
															 all_build_pipeline_ids,
															 completed_build_pipelines,
															 &shared_hash_bridge,
															 &shared_hash_bridge_len,
															 &shared_hash_bridge_count,
															 failure_reason))
		return false;

	for (uint32_t build_pipeline_id : ready_wave)
	{
		const ParallelPipelineDesc *build_pipeline =
			query_state->parallel_plan->get_pipeline(build_pipeline_id);
		const ParallelPipelineRuntimeState *runtime =
			query_state->parallel_scheduler->get_pipeline_runtime(build_pipeline_id);
		VecHashJoinState *target_hash_join_state;
		Relation rel;
		size_t pscan_len;

		if (build_pipeline == nullptr || runtime == nullptr ||
			build_pipeline->driver_kind != ParallelPipelineDriverKind::SourceScan ||
			build_pipeline->scan_relid == InvalidOid ||
			build_pipeline->scan_plan_node_id < 0 ||
			build_pipeline->hash_join_plan_node_id < 0 ||
			runtime->total_blocks == InvalidBlockNumber ||
			runtime->total_blocks == 0)
		{
			if (failure_reason != nullptr)
				*failure_reason = "parallel hash build wave found invalid build pipeline";
			goto done;
		}
		target_hash_join_state =
			query_state->vec_plan->find_parallel_hash_join_state_by_plan_node_id(
				build_pipeline->hash_join_plan_node_id);
		if (target_hash_join_state == nullptr)
		{
			if (failure_reason != nullptr)
				*failure_reason = "parallel hash build wave could not resolve hash join state";
			goto done;
		}
		target_hash_join_state->reset_parallel_hash_build_state();
		requested_workers += (int) Max(runtime->estimated_morsels, 1u);
		rel = table_open(build_pipeline->scan_relid, NoLock);
		pscan_len = table_parallelscan_estimate(rel, queryDesc->estate->es_snapshot);
		source_rels.push_back(rel);
		source_pscan_lens.push_back(pscan_len);
	}

	requested_workers = Min(pg_volvec_parallel_max_workers, Max(requested_workers, 1));
	query_text = queryDesc->sourceText != nullptr ? queryDesc->sourceText : "";
	plannedstmt_serialized_owned = nodeToString(queryDesc->plannedstmt);
	plannedstmt_serialized = plannedstmt_serialized_owned;
	plannedstmt_len = strlen(plannedstmt_serialized) + 1;
	query_text_len = strlen(query_text) + 1;

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
	shm_toc_estimate_chunk(&pcxt->estimator,
						   sizeof(ParallelHashBuildWavePipelineShared) * ready_wave.size());
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	for (size_t i = 0; i < ready_wave.size(); i++)
	{
		shm_toc_estimate_chunk(&pcxt->estimator, source_pscan_lens[i]);
		shm_toc_estimate_keys(&pcxt->estimator, 1);
	}
	build_partials_len =
		mul_size(sizeof(ParallelHashBuildPartialState),
				 mul_size(pcxt->nworkers, ready_wave.size()));
	shm_toc_estimate_chunk(&pcxt->estimator, build_partials_len);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	shm_toc_estimate_chunk(&pcxt->estimator, sizeof(SharedFileSet));
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	if (shared_hash_bridge != nullptr && shared_hash_bridge_len > 0)
	{
		shm_toc_estimate_chunk(&pcxt->estimator, shared_hash_bridge_len);
		shm_toc_estimate_keys(&pcxt->estimator, 1);
	}

	InitializeParallelDSM(pcxt);
	if (pcxt->seg == nullptr)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel hash build wave DSM initialization failed";
		goto done;
	}

	control = (ParallelAggregateSharedControl *)
		shm_toc_allocate(pcxt->toc, sizeof(ParallelAggregateSharedControl));
	control->magic = VOLVEC_PARALLEL_MAGIC;
	control->source_pipeline_id = UINT32_MAX;
	control->partial_slot_count = pcxt->nworkers;
	control->wave_pipeline_count = (uint32) ready_wave.size();
	control->morsel_nblocks = pg_volvec_parallel_morsel_nblocks;
	control->total_blocks = 0;
	control->source_scan_relid = InvalidOid;
	control->source_scan_plan_node_id = -1;
	control->agg_plan_node_id = -1;
	control->hash_join_plan_node_id = -1;
	control->input_hash_join_plan_node_id = -1;
	control->execution_mode = (uint32) ParallelWorkerExecutionMode::HashBuildWave;
	control->need_hash_join_state = true;
	control->hash_bridge_ready =
		(shared_hash_bridge != nullptr && shared_hash_bridge_len > 0) ? 1 : 0;
	control->reserved[0] = 0;
	control->reserved[1] = 0;
	control->hash_bridge_size = shared_hash_bridge_len;
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

	wave_pipelines = (ParallelHashBuildWavePipelineShared *)
		shm_toc_allocate(pcxt->toc,
						 sizeof(ParallelHashBuildWavePipelineShared) * ready_wave.size());
	for (size_t i = 0; i < ready_wave.size(); i++)
	{
		const ParallelPipelineDesc *build_pipeline =
			query_state->parallel_plan->get_pipeline(ready_wave[i]);
		const ParallelPipelineRuntimeState *runtime =
			query_state->parallel_scheduler->get_pipeline_runtime(ready_wave[i]);
		ParallelTableScanDesc source_pscan =
			(ParallelTableScanDesc) shm_toc_allocate(pcxt->toc, source_pscan_lens[i]);

		table_parallelscan_initialize(source_rels[i], source_pscan, queryDesc->estate->es_snapshot);
		shm_toc_insert(pcxt->toc, VOLVEC_PARALLEL_KEY_WAVE_SOURCE_PSCAN_BASE + i, source_pscan);
		wave_pipelines[i].pipeline_id = build_pipeline->pipeline_id;
		wave_pipelines[i].source_scan_relid = build_pipeline->scan_relid;
		wave_pipelines[i].source_scan_plan_node_id = build_pipeline->scan_plan_node_id;
		wave_pipelines[i].hash_join_plan_node_id = build_pipeline->hash_join_plan_node_id;
		wave_pipelines[i].input_hash_join_plan_node_id = build_pipeline->input_hash_join_plan_node_id;
		wave_pipelines[i].total_blocks = runtime->total_blocks;
		pg_atomic_init_u64(&wave_pipelines[i].next_block, 0);
	}
	shm_toc_insert(pcxt->toc, VOLVEC_PARALLEL_KEY_HASH_BUILD_WAVE_PIPELINES, wave_pipelines);

	build_partials = (ParallelHashBuildPartialState *)
		shm_toc_allocate(pcxt->toc, build_partials_len);
	memset(build_partials, 0, build_partials_len);
	shm_toc_insert(pcxt->toc, VOLVEC_PARALLEL_KEY_HASH_BUILD_PARTIALS, build_partials);

	partial_fileset = (SharedFileSet *) shm_toc_allocate(pcxt->toc, sizeof(SharedFileSet));
	SharedFileSetInit(partial_fileset, pcxt->seg);
	shm_toc_insert(pcxt->toc, VOLVEC_PARALLEL_KEY_PARTIAL_FILESET, partial_fileset);
	if (shared_hash_bridge != nullptr && shared_hash_bridge_len > 0)
	{
		uint8_t *shared_hash_bridge_dsm =
			(uint8_t *) shm_toc_allocate(pcxt->toc, shared_hash_bridge_len);
		memcpy(shared_hash_bridge_dsm, shared_hash_bridge, shared_hash_bridge_len);
		shm_toc_insert(pcxt->toc, VOLVEC_PARALLEL_KEY_HASH_BRIDGE, shared_hash_bridge_dsm);
	}

	for (Relation rel : source_rels)
		table_close(rel, NoLock);
	source_rels.clear();

	LaunchParallelWorkers(pcxt);
	if (pg_volvec_trace_hooks)
		elog(LOG,
			 "pg_volvec: launched %d/%d parallel workers for hash build wave with %zu pipeline(s)",
			 pcxt->nworkers_launched,
			 pcxt->nworkers,
			 ready_wave.size());
	if (pcxt->nworkers_launched == 0)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel hash build wave launched no workers";
		goto done;
	}

	WaitForParallelWorkersToFinish(pcxt);

	for (size_t pipeline_index = 0; pipeline_index < ready_wave.size(); pipeline_index++)
	{
		const ParallelPipelineDesc *build_pipeline =
			query_state->parallel_plan->get_pipeline(ready_wave[pipeline_index]);
		VecHashJoinState *target_hash_join_state =
			query_state->vec_plan->find_parallel_hash_join_state_by_plan_node_id(
				build_pipeline->hash_join_plan_node_id);
		size_t total_partial_entries = 0;
		size_t total_partial_chunks = 0;

		for (int worker_index = 0; worker_index < pcxt->nworkers_launched; worker_index++)
		{
			ParallelHashBuildPartialState &partial =
				build_partials[pipeline_index * pcxt->nworkers + worker_index];

			total_partial_entries += (size_t) partial.entry_count;
			total_partial_chunks += (size_t) partial.chunk_count;
		}
		target_hash_join_state->reserve_parallel_hash_build_capacity(total_partial_entries,
																	 total_partial_chunks);
		for (int worker_index = 0; worker_index < pcxt->nworkers_launched; worker_index++)
		{
			ParallelHashBuildPartialState &partial =
				build_partials[pipeline_index * pcxt->nworkers + worker_index];
			BufFile *file;

			if (partial.file_bytes == 0)
				continue;
			file = BufFileOpenFileSet(&partial_fileset->fs,
									 partial.file_name,
									 O_RDONLY,
									 false);
			if (file == nullptr ||
				!target_hash_join_state->merge_parallel_build_partial_file(file, partial))
			{
				if (file != nullptr)
					BufFileClose(file);
				if (failure_reason != nullptr)
					*failure_reason = "parallel hash build wave partial merge failed";
				goto done;
			}
			BufFileClose(file);
			BufFileDeleteFileSet(&partial_fileset->fs, partial.file_name, true);
		}
		target_hash_join_state->finish_parallel_hash_build();
		if (target_hash_join_state->estimate_parallel_hash_bridge_size() > MaxAllocSize)
		{
			if (failure_reason != nullptr)
				*failure_reason = "parallel hash build wave shared bridge exceeds MaxAllocSize";
			goto done;
		}
		target_hash_join_state->publish_hash_bridge();
		if (target_hash_join_state->shared_hash_bridge_buffer() == nullptr ||
			target_hash_join_state->shared_hash_bridge_size() == 0)
		{
			if (failure_reason != nullptr)
				*failure_reason = "parallel hash build wave did not publish a shared bridge";
			goto done;
		}
	}
	success = true;

done:
	for (Relation rel : source_rels)
		table_close(rel, NoLock);
	if (partial_fileset != nullptr)
		SharedFileSetDeleteAll(partial_fileset);
	if (pcxt != nullptr)
		DestroyParallelContext(pcxt);
	if (entered_parallel)
		ExitParallelMode();
	if (plannedstmt_serialized_owned != nullptr)
		pfree(plannedstmt_serialized_owned);
	if (shared_hash_bridge != nullptr)
		pfree(shared_hash_bridge);
	return success;
}

static bool
TryExecuteParallelHashBuildDependencyChain(PgVolVecQueryState *query_state,
											 QueryDesc *queryDesc,
											 const ParallelPipelineDesc *probe_pipeline,
											 const char **failure_reason)
{
	VolVecVector<uint32_t> build_pipeline_ids{PgMemoryContextAllocator<uint32_t>(CurrentMemoryContext)};
	VolVecVector<uint8_t> seen_pipeline_ids{PgMemoryContextAllocator<uint8_t>(CurrentMemoryContext)};
	VolVecVector<uint8_t> completed_build_pipelines{PgMemoryContextAllocator<uint8_t>(CurrentMemoryContext)};
	VolVecVector<uint8_t> scheduled_build_pipelines{PgMemoryContextAllocator<uint8_t>(CurrentMemoryContext)};
	size_t completed_count = 0;
	uint32_t wave_no = 0;

	if (failure_reason != nullptr)
		*failure_reason = nullptr;
	if (query_state == nullptr || query_state->parallel_plan == nullptr || probe_pipeline == nullptr)
	{
		if (failure_reason != nullptr)
			*failure_reason = "parallel hash build dependency chain has incomplete state";
		return false;
	}

	seen_pipeline_ids.resize(query_state->parallel_plan->pipeline_count(), 0);
	if (!CollectHashBuildDependenciesPostOrder(query_state->parallel_plan,
													 probe_pipeline->pipeline_id,
													 0,
													 &build_pipeline_ids,
													 &seen_pipeline_ids))
		return true;
	completed_build_pipelines.resize(query_state->parallel_plan->pipeline_count(), 0);
	scheduled_build_pipelines.resize(query_state->parallel_plan->pipeline_count(), 0);

	while (completed_count < build_pipeline_ids.size())
	{
		VolVecVector<uint32_t> ready_wave{PgMemoryContextAllocator<uint32_t>(CurrentMemoryContext)};

		for (uint32_t build_pipeline_id : build_pipeline_ids)
		{
			if (build_pipeline_id >= scheduled_build_pipelines.size() ||
				scheduled_build_pipelines[build_pipeline_id] != 0)
				continue;
			if (!HashBuildDependenciesSatisfied(query_state->parallel_plan,
												build_pipeline_id,
												completed_build_pipelines))
				continue;
			ready_wave.push_back(build_pipeline_id);
		}

		if (ready_wave.empty())
		{
			if (failure_reason != nullptr)
				*failure_reason =
					"parallel hash build dependency scheduler found no ready pipeline";
			return false;
		}

		wave_no++;
		if (pg_volvec_trace_hooks)
			elog(LOG,
				 "pg_volvec: executing hash build dependency wave %u with %zu ready pipeline(s)",
				 wave_no,
				 ready_wave.size());

		if (!TryExecuteParallelHashBuildWave(query_state,
											 queryDesc,
											 ready_wave,
											 build_pipeline_ids,
											 completed_build_pipelines,
											 failure_reason))
			return false;
		for (uint32_t build_pipeline_id : ready_wave)
		{
			scheduled_build_pipelines[build_pipeline_id] = 1;
			completed_build_pipelines[build_pipeline_id] = 1;
			completed_count++;
		}
	}

	return true;
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
			ParallelPipelineDesc *child_pipeline =
				context->parallel_plan->get_pipeline(child.pipeline_id);

			if (child_pipeline != nullptr &&
				child_pipeline->role == ParallelPipelineRole::HashProbeSource &&
				child_pipeline->driver_kind == ParallelPipelineDriverKind::SourceScan)
			{
				child_pipeline->output_bridge = ParallelBridgeKind::SortRuns;
			}
			else
			{
			if (pg_volvec_trace_hooks)
			{
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
		if (hash_join->join.jointype != JOIN_INNER &&
			hash_join->join.jointype != JOIN_ANTI &&
			hash_join->join.jointype != JOIN_RIGHT)
		{
			SetFailure(context, "parallel lowering currently only supports inner/anti/right HashJoin");
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
		if (hash_join->join.jointype == JOIN_ANTI)
			build_outer_side = false;
		else
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

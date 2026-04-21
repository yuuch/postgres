#pragma once

/*
 * parallel_runtime_internal.hpp
 *
 * Shared types, constants, and function declarations for the parallel runtime
 * translation units (runtime_lowering.cpp, runtime_worker_state.cpp,
 * runtime_execution.cpp, runtime_worker_main.cpp).
 *
 * This header is internal to the parallel runtime and must NOT be included
 * from outside src/engine/parallel/.
 */

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
#include "utils/dsa.h"
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

/* ----------------------------------------------------------------
 * Enums
 * ---------------------------------------------------------------- */

enum class ParallelWorkerExecutionMode : uint32_t {
	AggregateProbe = 1,
	HashBuild = 2,
	QueryScheduler = 3
};

enum class ParallelQueryTaskState : uint32_t {
	Pending = 0,
	Running = 1,
	Done = 2
};

/* ----------------------------------------------------------------
 * Constants
 * ---------------------------------------------------------------- */

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
static constexpr uint64 VOLVEC_PARALLEL_KEY_DSA =
	UINT64CONST(0xD700000000000012);
static constexpr uint64 VOLVEC_PARALLEL_KEY_QUERY_PARTITIONS =
	UINT64CONST(0xD700000000000013);
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

/* ----------------------------------------------------------------
 * Structs
 * ---------------------------------------------------------------- */

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
	uint32 partition_start = 0;
	uint32 partition_count = 0;
	uint32 successor_start = 0;
	uint32 successor_count = 0;
	pg_atomic_uint32 partition_finalize_started;
	pg_atomic_uint32 hash_bridge_ready;
	uint64 hash_bridge_size = 0;
	uint64 hash_bridge_entries = 0;
	uint64 hash_bridge_chunks = 0;
	uint64 hash_bridge_dsa_pack = 0;
	char hash_bridge_file_name[VOLVEC_PARALLEL_MAX_PARTIAL_FILE_NAME] = {0};
};

struct ParallelQueryTaskShared
{
	uint32 task_id = UINT32_MAX;
	uint32 pipeline_id = UINT32_MAX;
	uint32 task_kind = 0;
	uint32 partition_id = UINT32_MAX;
	BlockNumber morsel_start_block = InvalidBlockNumber;
	uint32 morsel_nblocks = 0;
	pg_atomic_uint32 state;
};

struct ParallelQueryPartitionShared
{
	uint32 pipeline_id = UINT32_MAX;
	uint32 partition_id = UINT32_MAX;
	pg_atomic_uint32 remaining_build_tasks;
	pg_atomic_uint32 build_finalized;
	pg_atomic_uint32 probe_started;
	pg_atomic_uint32 probe_completed;
	pg_atomic_uint32 bridge_ready;
	uint32 reserved = 0;
	uint64 bridge_size = 0;
	uint64 bridge_dsa_pack = 0;
	char bridge_file_name[VOLVEC_PARALLEL_MAX_PARTIAL_FILE_NAME] = {0};
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

struct PipelineLoweringResult {
	uint32_t pipeline_id = UINT32_MAX;
	bool valid = false;
};

struct PipelineLoweringContext {
	PlannedStmt *plannedstmt = nullptr;
	ParallelPipelinePlan *parallel_plan = nullptr;
	const char **failure_reason = nullptr;
};

/* ----------------------------------------------------------------
 * Functions defined in runtime_lowering.cpp, used cross-file
 * ---------------------------------------------------------------- */

const char *ParallelDriverKindDebugName(ParallelPipelineDriverKind kind);
const char *ParallelPipelineRoleDebugName(ParallelPipelineRole role);
const char *ParallelWorkerExecutionModeDebugName(ParallelWorkerExecutionMode mode);

uint64 RuntimeElapsedUsSince(instr_time start);

bool WriteDataChunkRows(BufFile *file, const DataChunk<DEFAULT_CHUNK_SIZE> &chunk);
bool ReadDataChunkRows(BufFile *file, DataChunk<DEFAULT_CHUNK_SIZE> *chunk);

void FormatParallelPartialFileName(char *dst, size_t dstlen, const char *prefix, int slot);

bool IsAggregateSourcePipeline(const ParallelPipelineDesc &pipeline);

const ParallelPipelineDesc *
FindLargestHashBuildDependency(const ParallelPipelinePlan *parallel_plan,
							   const ParallelSchedulerState *scheduler,
							   const ParallelPipelineDesc *source_pipeline,
							   const ParallelPipelineRuntimeState **runtime_out);

bool ShouldSkipHashProbeParallelForBuildDominatedDependency(
	const ParallelPipelinePlan *parallel_plan,
	const ParallelSchedulerState *scheduler,
	const ParallelPipelineDesc *source_pipeline,
	const ParallelPipelineRuntimeState *source_runtime,
	BlockNumber *build_blocks_out,
	uint32_t *build_pipeline_id_out,
	double *build_rows_out,
	double *source_rows_out);

bool TryExecuteParallelHashBuildDependencyChain(PgVolVecQueryState *query_state,
												QueryDesc *queryDesc,
												const ParallelPipelineDesc *probe_pipeline,
												const char **failure_reason);

bool BuildPublishedSharedHashBridgePack(PgVolVecQueryState *query_state,
										const ParallelPipelineDesc *probe_pipeline,
										uint8_t **buffer_out,
										size_t *buffer_size_out,
										uint32 *bridge_count_out,
										const char **failure_reason);

double LookupPlannedStmtNodeRows(const PlannedStmt *plannedstmt, int target_plan_node_id);

bool TryAttachSharedHashBridgePack(VecPlanState *root_plan,
								   const uint8_t *buffer,
								   size_t buffer_size,
								   bool leader,
								   const char **failure_reason);

/* ----------------------------------------------------------------
 * Functions defined in runtime_worker_state.cpp, used cross-file
 * ---------------------------------------------------------------- */

void CleanupLocalParallelAggregateProcessState(LocalParallelAggregateProcessState *local);

void CleanupLocalParallelAggregateProcessStateOnExit(int code, Datum arg);

bool TryInitializeLocalParallelAggregateProcessState(
	const char *plannedstmt_serialized,
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

bool ExecuteParallelWorkerSourceLoop(ParallelWorkerExecutionMode mode,
									 ParallelWorkerContext &worker_context,
									 const char **failure_reason);

void PopulatePartialDiagnostics(const LocalParallelAggregateProcessState &local,
								ParallelAggPartialState *partial);

void PopulateHashBuildPartialDiagnostics(const LocalParallelAggregateProcessState &local,
										 ParallelHashBuildPartialState *partial);

bool TryInitializeParallelMergeContext(PgVolVecQueryState *query_state,
									   int agg_plan_node_id,
									   int hash_join_plan_node_id,
									   bool need_hash_join_state,
									   ParallelWorkerContext *worker_context,
									   const char **failure_reason);

/* ----------------------------------------------------------------
 * Functions defined in runtime_execution.cpp (public API)
 * ---------------------------------------------------------------- */

bool TryInitializeLeaderOnlyAggregateWorkerContext(PgVolVecQueryState *query_state,
												   ParallelWorkerContext *worker_context,
												   const ParallelPipelineDesc **source_pipeline_out,
												   const char **failure_reason);

bool TryExecuteQuerySchedulerSkeleton(PgVolVecQueryState *query_state,
									  QueryDesc *queryDesc,
									  const char **failure_reason);

bool ExecuteParallelTask(const ParallelTaskDesc &task,
						 const ParallelPipelinePlan *parallel_plan,
						 ParallelWorkerContext &worker_context,
						 const char **failure_reason);

bool TryExecuteProcessParallelAggregate(PgVolVecQueryState *query_state,
										QueryDesc *queryDesc,
										const char **failure_reason);

} /* namespace pg_volvec */

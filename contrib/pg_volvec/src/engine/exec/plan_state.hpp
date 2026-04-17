#pragma once

#include "core/data_chunk.hpp"
#include "expr/expr.hpp"

class VecAggState;
class VecHashJoinState;

class VecPlanState : public PgMemoryContextObject {
public:
	virtual ~VecPlanState() = default;
	virtual bool get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk) = 0;
	virtual void release_jit_resources_for_proc_exit()
	{
	}
		virtual bool lookup_output_col_meta(int target_resno, VecOutputColMeta *out) const
		{
			return false;
		}
		virtual bool lookup_remapped_output_col_meta(int child_input_resno,
													 uint16_t *output_col,
													 VecOutputColMeta *out) const
		{
			(void) child_input_resno;
			(void) output_col;
			(void) out;
			return false;
		}
	virtual bool configure_source_block_range(BlockNumber start_block, uint32_t nblocks)
	{
		return false;
	}
	virtual void clear_source_block_range()
	{
	}
	virtual VecAggState *find_parallel_aggregate_state()
	{
		return nullptr;
	}
	virtual VecAggState *find_parallel_aggregate_state_by_plan_node_id(int target_plan_node_id)
	{
		(void) target_plan_node_id;
		return nullptr;
	}
	virtual class VecSeqScanState *find_parallel_source_scan_state()
	{
		return nullptr;
	}
	virtual VecHashJoinState *find_parallel_hash_join_state()
	{
		return nullptr;
	}
	virtual VecHashJoinState *find_parallel_hash_join_state_by_plan_node_id(int target_plan_node_id)
	{
		(void) target_plan_node_id;
		return nullptr;
	}
};

enum class ParallelPipelineDriverKind : uint8_t {
	SourceScan,
	BridgeFinalize
};

enum class ParallelPipelineRole : uint8_t {
	GenericSource,
	AggFinalize,
	SortMerge,
	HashBuildSource,
	HashBuildFinalize,
	HashProbeSource,
	HashOuterSource
};

enum class ParallelPipelineStage : uint32_t {
	PartialAgg = 1u << 0,
	HashBuild = 1u << 1,
	HashProbe = 1u << 2,
	SortRun = 1u << 3
};

enum class ParallelBridgeKind : uint8_t {
	None,
	Aggregate,
	HashBuild,
	HashTable,
	SortRuns
};

enum class ParallelTaskKind : uint8_t {
	SourceMorsel,
	BridgeFinalize
};

struct ParallelAggPartialAccumulator {
	double float_sum = 0.0;
	uint64_t numeric_sum_lo = 0;
	int64_t numeric_sum_hi = 0;
	uint64_t numeric_max_lo = 0;
	int64_t numeric_max_hi = 0;
	double float_max = 0.0;
	int64_t int64_max = 0;
	int32_t int32_max = 0;
	int64_t count = 0;
	uint8_t has_value = 0;
};

static constexpr uint32 VOLVEC_PARALLEL_MAX_GROUPS = 256;
static constexpr uint32 VOLVEC_PARALLEL_MAX_GROUP_COLS = 16;
static constexpr uint32 VOLVEC_PARALLEL_MAX_PARTIAL_FILE_NAME = 64;

struct ParallelAggPartialGroupKeyCol {
	uint8_t storage_kind = 0;
	uint8_t is_null = 0;
	uint16_t reserved = 0;
	uint32_t string_len = 0;
	uint64_t value_bits = 0;
};

struct ParallelAggFileGroupKeyCol {
	ParallelAggPartialGroupKeyCol header;
	std::string string_bytes;
};

struct ParallelAggPartialGroupEntry {
	uint32_t num_group_cols = 0;
	ParallelAggPartialGroupKeyCol group_cols[VOLVEC_PARALLEL_MAX_GROUP_COLS];
	ParallelAggPartialAccumulator accs[16];
};

struct ParallelAggPartialState {
	uint32_t naggs = 0;
	uint32_t group_count = 0;
	uint8_t grouped = 0;
	uint8_t file_backed = 0;
	uint8_t reserved[2] = {0, 0};
	uint64_t init_time_us = 0;
	uint64_t exec_time_us = 0;
	uint64_t blocks_opened = 0;
	uint64_t input_batches = 0;
	uint64_t input_rows = 0;
	uint64_t file_bytes = 0;
	char grouped_file_name[VOLVEC_PARALLEL_MAX_PARTIAL_FILE_NAME] = {0};
	ParallelAggPartialAccumulator accs[16];
	ParallelAggPartialGroupEntry groups[VOLVEC_PARALLEL_MAX_GROUPS];
};

struct ParallelHashBuildPartialState {
	uint64_t init_time_us = 0;
	uint64_t exec_time_us = 0;
	uint64_t blocks_opened = 0;
	uint64_t input_batches = 0;
	uint64_t input_rows = 0;
	uint64_t entry_count = 0;
	uint64_t chunk_count = 0;
	uint64_t file_bytes = 0;
	char file_name[VOLVEC_PARALLEL_MAX_PARTIAL_FILE_NAME] = {0};
};

struct ParallelPipelineDesc {
	/* Static plan-time descriptor for one lowered pipeline. */
	uint32_t pipeline_id = 0;
	ParallelPipelineDriverKind driver_kind = ParallelPipelineDriverKind::SourceScan;
	ParallelPipelineRole role = ParallelPipelineRole::GenericSource;
	ParallelBridgeKind input_bridge = ParallelBridgeKind::None;
	ParallelBridgeKind output_bridge = ParallelBridgeKind::None;
	uint32_t stage_mask = 0;
	Oid scan_relid = InvalidOid;
	int scan_plan_node_id = -1;
	int agg_plan_node_id = -1;
	int hash_join_plan_node_id = -1;
	int input_hash_join_plan_node_id = -1;
	double estimated_rows = -1.0;
	bool source_morsel_driven = false;
	bool has_filter = false;
	bool has_projection = false;
	bool has_limit = false;
	bool grouped_agg = false;
	VolVecVector<uint32_t> dependencies;
	VolVecVector<uint32_t> successors;

	explicit ParallelPipelineDesc(MemoryContext context)
		: dependencies(PgMemoryContextAllocator<uint32_t>(context)),
		  successors(PgMemoryContextAllocator<uint32_t>(context))
	{
	}
};


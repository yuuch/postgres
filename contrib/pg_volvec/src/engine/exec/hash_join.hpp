#pragma once

#include "exec/plan_state.hpp"
#include "core/hash_table_defs.hpp"

enum class VecJoinSide : uint8_t {
	Outer,
	Inner
};

struct VecJoinOutputCol {
	VecJoinSide side;
	uint16_t input_col;
	int output_resno;
	VecOutputColMeta meta;
};

struct VecHashPayloadCol {
	uint16_t source_col;
	VecOutputColMeta meta;
};

static constexpr int kMaxJoinKeys = 4;

struct VecHashJoinKeyCol {
	uint16_t outer_col;
	uint16_t inner_col;
	VecOutputStorageKind kind;
	int scale;
};

struct VecHashJoinKey {
	uint64_t values[kMaxJoinKeys];
	uint8_t num_keys;
};

struct VecRowRef {
	uint32_t ordinal;
	uint32_t chunk_idx;
	uint16_t row_idx;
};

class VecHashJoinState : public VecPlanState {
public:
	VecHashJoinState(std::unique_ptr<VecPlanState> outer,
					 std::unique_ptr<VecPlanState> inner,
					 int plan_node_id,
					 JoinType jointype,
					 bool build_outer_side,
					 int visible_output_count,
					 VolVecVector<VecJoinOutputCol> output_cols,
					 VolVecVector<VecHashJoinKeyCol> key_cols);
	~VecHashJoinState() override;
	bool get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk) override;
	bool lookup_output_col_meta(int target_resno, VecOutputColMeta *out) const override;
	bool configure_source_block_range(BlockNumber start_block, uint32_t nblocks) override;
	void clear_source_block_range() override;
	VecSeqScanState *find_parallel_source_scan_state() override;
	VecHashJoinState *find_parallel_hash_join_state() override
	{
		return this;
	}
	VecHashJoinState *find_parallel_hash_join_state_by_plan_node_id(int target_plan_node_id) override
	{
		if (target_plan_node_id < 0 || plan_node_id_ == target_plan_node_id)
			return this;
		VecHashJoinState *state = outer_ != nullptr ?
			outer_->find_parallel_hash_join_state_by_plan_node_id(target_plan_node_id) :
			nullptr;
		if (state != nullptr)
			return state;
		return inner_ != nullptr ?
			inner_->find_parallel_hash_join_state_by_plan_node_id(target_plan_node_id) :
			nullptr;
	}
	VecAggState *find_parallel_aggregate_state_by_plan_node_id(int target_plan_node_id) override
	{
		VecAggState *agg = outer_ != nullptr ?
			outer_->find_parallel_aggregate_state_by_plan_node_id(target_plan_node_id) : nullptr;
		if (agg != nullptr)
			return agg;
		return inner_ != nullptr ? inner_->find_parallel_aggregate_state_by_plan_node_id(target_plan_node_id) : nullptr;
	}
	void release_jit_resources_for_proc_exit() override
	{
		if (outer_ != nullptr)
			outer_->release_jit_resources_for_proc_exit();
		if (inner_ != nullptr)
			inner_->release_jit_resources_for_proc_exit();
		if (join_filter_program_ != nullptr)
			join_filter_program_->release_jit_resources_for_proc_exit();
	}
	VecSeqScanState *find_parallel_build_scan_state();
	void set_join_filter_program(std::unique_ptr<VecExprProgram> program);
	bool configure_build_input_block_range(BlockNumber start_block, uint32_t nblocks);
	void clear_build_input_block_range();
	void consume_build_input();
	void finish_parallel_hash_build();
	uint64_t build_input_rows_consumed() const
	{
		return build_input_rows_consumed_;
	}
	uint64_t build_input_batches_consumed() const
	{
		return build_input_batches_consumed_;
	}
	size_t parallel_hash_entry_count() const
	{
		return entries_.size();
	}
	size_t parallel_hash_chunk_count() const
	{
		return shared_hash_chunk_count_ > 0 ? shared_hash_chunk_count_ : inner_chunks_.size();
	}
	bool export_parallel_build_partial_file(BufFile *file,
											 ParallelHashBuildPartialState *out) const;
	bool merge_parallel_build_partial_file(BufFile *file,
											const ParallelHashBuildPartialState &partial);
	void reserve_parallel_hash_build_capacity(size_t total_entries,
											  size_t total_chunks);
	size_t estimate_parallel_hash_bridge_size() const;
	void reset_parallel_hash_build_state();
	void publish_hash_bridge();
	void load_hash_bridge();
	void attach_shared_hash_bridge(const uint8_t *buffer, size_t buffer_size);
	const uint8_t *shared_hash_bridge_buffer() const
	{
		return shared_hash_bridge_buffer_;
	}
	size_t shared_hash_bridge_size() const
	{
		return shared_hash_bridge_buffer_size_;
	}
	private:
		struct VecHashEntry {
			uint32_t hash;
			VecHashJoinKey key;
			int32_t next;
			uint32_t chunk_idx;
			uint16_t row_idx;
		};

		void build_inner_hash();
		void consume_build_batch(const DataChunk<DEFAULT_CHUNK_SIZE> &input);
		void consume_build_input_radix();
		void clear_partition_hash_tables();
		void clear_inner_build_chunks();
		void clear_shared_hash_payload_view();
		size_t compute_hash_build_fragment_size() const;
		void serialize_hash_build_fragment(uint8_t *buffer, size_t buffer_size) const;
		void append_hash_build_fragment(const uint8_t *buffer, size_t buffer_size);
		size_t compute_hash_bridge_size() const;
		void serialize_hash_bridge(uint8_t *buffer, size_t buffer_size) const;
		void deserialize_hash_bridge(const uint8_t *buffer, size_t buffer_size);
		void copy_inner_payload_value_to_chunk(DataChunk<DEFAULT_CHUNK_SIZE> &dst,
											  int dst_row,
											  int dst_col,
											  const VecOutputColMeta &meta,
											  int src_col,
											  uint32_t chunk_idx,
											  uint16_t row_idx) const;
		void partition_build_batch(const DataChunk<DEFAULT_CHUNK_SIZE> &input,
								   uint32_t *hashes, uint64_t *keys, uint64_t *payloads);
		void build_linear_probe_tables();
		void build_linear_probe_table_for_partition(int part_idx);
		void reset_probe_task_state();
		void init_hash_table(size_t expected_rows);
		void rehash_hash_table(size_t min_bucket_count);
		void append_inner_entry(const VecHashJoinKey &key, uint32_t hash, uint32_t chunk_idx, uint16_t row_idx);
		uint16_t ensure_inner_payload_col(uint16_t source_col, const VecOutputColMeta &meta);
		DataChunk<DEFAULT_CHUNK_SIZE> *allocate_inner_chunk();
		void copy_inner_payload_row(DataChunk<DEFAULT_CHUNK_SIZE> &dst, int dst_row,
									const DataChunk<DEFAULT_CHUNK_SIZE> &src, int src_row) const;
		bool advance_outer_batch();
		void prepare_probe_batch();
		void prepare_probe_batch_ht();
		bool advance_probe_match(uint16_t probe_idx, int32_t *match_entry_idx);
		bool advance_probe_match_ht(uint16_t probe_idx, uint64_t *match_payload);
		uint32_t hash_key(const VecHashJoinKey &key) const;
		bool read_key(const DataChunk<DEFAULT_CHUNK_SIZE> &chunk, bool inner_side, int row, VecHashJoinKey *key) const;
		bool keys_equal(const VecHashJoinKey &left, const VecHashJoinKey &right) const;
		bool candidate_passes_join_filter(const DataChunk<DEFAULT_CHUNK_SIZE> &outer_src, int outer_row,
										  const DataChunk<DEFAULT_CHUNK_SIZE> &inner_src, int inner_row);
		int plan_node_id_;

		std::unique_ptr<VecPlanState> outer_;
		std::unique_ptr<VecPlanState> inner_;
		JoinType jointype_;
		int visible_output_count_;
		MemoryContext memory_context_;
		VolVecVector<VecJoinOutputCol> output_cols_;
		VolVecVector<VecHashJoinKeyCol> key_cols_;
		VolVecVector<VecHashPayloadCol> inner_payload_cols_;
		VolVecVector<DataChunk<DEFAULT_CHUNK_SIZE> *> inner_chunks_;
		VolVecVector<int32_t> bucket_heads_;
		VolVecVector<VecHashEntry> entries_;
		VolVecVector<uint8_t> inner_entry_matched_;
	DataChunk<DEFAULT_CHUNK_SIZE> outer_chunk_;
	VolVecVector<uint16_t> probe_rows_;
	VolVecVector<VecHashJoinKey> probe_keys_;
	VolVecVector<uint32_t> probe_hashes_;
	VolVecVector<int32_t> probe_next_entries_;
	VolVecVector<uint16_t> active_probe_sel_;
	VolVecVector<uint16_t> next_probe_sel_;
	/* Linear probe HT probe state */
	VolVecVector<uint64_t> ht_match_payloads_;       /* flat list of all matching payloads */
	VolVecVector<uint32_t> ht_match_starts_;         /* start index in payload list per probe */
	VolVecVector<uint32_t> ht_match_counts_;         /* match count per probe */
	VolVecVector<uint32_t> ht_match_pos_;            /* how many matches emitted per probe */
	bool inner_built_;
	bool probe_batch_ready_;
	bool probe_input_exhausted_;
	bool build_outer_side_;
	std::unique_ptr<VecExprProgram> join_filter_program_;
	DataChunk<DEFAULT_CHUNK_SIZE> join_filter_chunk_;
	bool semi_build_marked_;
	uint32_t semi_build_emit_pos_;
	bool anti_build_marked_;
	uint32_t anti_build_emit_pos_;
	bool right_anti_marked_;
	uint16_t anti_outer_pos_;
	uint32_t right_anti_emit_pos_;
	size_t bucket_mask_;
	uint64_t build_input_rows_consumed_ = 0;
	uint64_t build_input_batches_consumed_ = 0;

	/* === Radix Partition + Linear Probe (parallel hash join) === */
	VecHashPartitionTable build_partition_table_;
	VecHashPartitionTable probe_partition_table_;
	VolVecVector<VecLinearProbeHT> local_hash_tables_;
	VolVecVector<VecBloomFilter> partition_bloom_filters_;
	ParallelHashBuildState *shared_hash_bridge_;
	uint8_t *shared_hash_bridge_buffer_;  /* owned buffer for DSM-loaded bridge */
	size_t shared_hash_bridge_buffer_size_;
	bool shared_hash_bridge_buffer_owned_;
	const ParallelHashBuildChunk *shared_hash_chunks_;
	uint32_t shared_hash_chunk_count_;
	int32_t *partition_bucket_heads_[VOLVEC_RADIX_FANOUT];
	size_t partition_bucket_masks_[VOLVEC_RADIX_FANOUT];
	bool partition_bucket_heads_external_[VOLVEC_RADIX_FANOUT];
	uint32_t assigned_partition_start_;
	uint32_t assigned_partition_end_;
	uint32_t build_histogram_[VOLVEC_RADIX_FANOUT];
	uint64_t total_build_rows_;
	bool use_parallel_ht_;  /* use radix+linear-probe path */
	friend struct HashJoinParallelAccess;
};

#pragma once

#include "exec/plan_state.hpp"
#include "core/hash_table_defs.hpp"

class VecAggState : public VecPlanState {
public:
	enum class NumericOutputKind { None, Sum, Avg };

	VecAggState(std::unique_ptr<VecPlanState> left, Agg *node);
	~VecAggState() override;
		bool get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk) override;
		bool lookup_numeric_output_meta(int target_resno, NumericOutputKind *kind, int *scale) const;
		bool lookup_output_col_meta(int target_resno, VecOutputColMeta *out) const override;
		bool lookup_remapped_output_col_meta(int child_input_resno,
											 uint16_t *output_col,
											 VecOutputColMeta *out) const override;
	VecAggState *find_parallel_aggregate_state() override
	{
		return this;
	}
	bool is_valid() const
	{
		return valid_;
	}
	VecAggState *find_parallel_aggregate_state_by_plan_node_id(int target_plan_node_id) override
	{
		if (target_plan_node_id >= 0 &&
			node_ != nullptr &&
			node_->plan.plan_node_id == target_plan_node_id)
			return this;
		return left_ != nullptr ? left_->find_parallel_aggregate_state_by_plan_node_id(target_plan_node_id) : nullptr;
	}
	VecSeqScanState *find_parallel_source_scan_state() override
	{
		return left_ != nullptr ? left_->find_parallel_source_scan_state() : nullptr;
	}
	VecHashJoinState *find_parallel_hash_join_state() override
	{
		return left_ != nullptr ? left_->find_parallel_hash_join_state() : nullptr;
	}
	VecHashJoinState *find_parallel_hash_join_state_by_plan_node_id(int target_plan_node_id) override
	{
		return left_ != nullptr ?
			left_->find_parallel_hash_join_state_by_plan_node_id(target_plan_node_id) :
			nullptr;
	}
	void release_jit_resources_for_proc_exit() override
	{
		if (left_ != nullptr)
			left_->release_jit_resources_for_proc_exit();
		for (auto &agg : aggs_)
		{
			if (agg.arg_expr != nullptr)
				agg.arg_expr->release_jit_resources_for_proc_exit();
		}
	}
	bool configure_input_block_range(BlockNumber start_block, uint32_t nblocks);
	void clear_input_block_range();
		VecPlanState *input_plan()
		{
			return left_.get();
		}
		bool push_batch(DataChunk<DEFAULT_CHUNK_SIZE> &batch) override
		{
			consume_batch(batch);
			return true;
		}
		void consume_left_input();
	void consume_batch(DataChunk<DEFAULT_CHUNK_SIZE> &batch);
	void finish_sink();
	bool supports_parallel_partial_state() const;
	bool uses_file_backed_parallel_partial_state() const
	{
		return !grp_col_indices_.empty();
	}
	bool export_parallel_partial_state(ParallelAggPartialState *out) const;
	bool export_parallel_grouped_partial_file(BufFile *file,
											  ParallelAggPartialState *out) const;
	bool merge_parallel_partial_state(const ParallelAggPartialState &partial);
	bool merge_parallel_grouped_partial_file(BufFile *file,
											 const ParallelAggPartialState &partial);
	uint64_t input_rows_consumed() const
	{
		return input_rows_consumed_;
	}
	uint64_t input_batches_consumed() const
	{
		return input_batches_consumed_;
	}
private:
		std::unique_ptr<VecPlanState> left_; Agg *node_; MemoryContext memory_context_; VolVecVector<int> grp_col_indices_;
		VolVecVector<VecOutputColMeta> grp_col_meta_;
	struct VecAggAccumulator {
		double float_sum = 0.0;
		NumericWideInt numeric_sum = 0;
		NumericWideInt numeric_max = 0;
		double float_max = 0.0;
		int64_t int64_max = 0;
		int32_t int32_max = 0;
		int64_t count = 0;
		bool has_value = false;

		void update_float(double v) { float_sum += v; count++; }
		void update_numeric(NumericWideInt v) { numeric_sum += v; count++; }
		void update_max_float(double v)
		{
			if (!has_value || v > float_max)
				float_max = v;
			has_value = true;
		}
		void update_max_int64(int64_t v)
		{
			if (!has_value || v > int64_max)
				int64_max = v;
			has_value = true;
		}
		void update_max_int32(int32_t v)
		{
			if (!has_value || v > int32_max)
				int32_max = v;
			has_value = true;
		}
		void update_max_numeric(NumericWideInt v)
		{
			if (!has_value || v > numeric_max)
				numeric_max = v;
			has_value = true;
		}

		using DistinctValueSet = VolVecHashMap<int64_t, char>;
		DistinctValueSet *distinct_values = nullptr;
	};
		struct VecGroupKey {
			uint64_t values[kMaxDeformTargets];
			uint32_t aux[kMaxDeformTargets];
			uint8_t is_null[kMaxDeformTargets];
			int num_cols;
			bool operator==(const VecGroupKey& o) const {
				if (num_cols != o.num_cols)
					return false;
				for (int i = 0; i < num_cols; i++)
				{
					if (is_null[i] != o.is_null[i])
						return false;
					if (!is_null[i] && (values[i] != o.values[i] || aux[i] != o.aux[i]))
						return false;
				}
				return true;
			}
		};
		struct VecGroupKeyHash {
			std::size_t operator()(const VecGroupKey& k) const {
				std::size_t h = 0;
				for (int i = 0; i < k.num_cols; i++)
				{
					h ^= std::hash<uint8_t>{}(k.is_null[i]) + 0x9e3779b9 + (h << 6) + (h >> 2);
					if (k.is_null[i])
						continue;
					h ^= std::hash<uint64_t>{}(k.values[i]) + 0x9e3779b9 + (h << 6) + (h >> 2);
					h ^= std::hash<uint32_t>{}(k.aux[i]) + 0x9e3779b9 + (h << 6) + (h >> 2);
				}
				return h;
			}
		};
		struct VecSimpleGroupKey {
			int64_t value = 0;
			uint8_t is_null = 0;
			bool operator==(const VecSimpleGroupKey& o) const {
				return value == o.value && is_null == o.is_null;
			}
		};
		struct VecSimpleGroupKeyHash {
			std::size_t operator()(const VecSimpleGroupKey& k) const {
				std::size_t h = std::hash<uint8_t>{}(k.is_null);
				if (!k.is_null)
					h ^= std::hash<int64_t>{}(k.value) + 0x9e3779b9 + (h << 6) + (h >> 2);
				return h;
			}
		};
	enum class VecAggType { SUM, COUNT, AVG, MAX };
		struct VecAggDesc {
			VecAggType type;
			std::unique_ptr<VecExprProgram> arg_expr;
			int target_resno;
			int group_key_pos = -1;
			int input_col = -1;
			Oid output_type = InvalidOid;
			VecOutputStorageKind output_storage = VecOutputStorageKind::Int32;
			Oid arg_type = InvalidOid;
			int numeric_scale = 0;
			int numeric_precision = 0;
			VecNumericWidth numeric_width = VecNumericWidth::None;
			bool use_exact_numeric = false;
			bool is_distinct = false;
		};
		using VecAggAccumulatorList = VolVecVector<VecAggAccumulator>;
		struct VecAggGroupState {
			VecAggAccumulatorList accs;
			uint32_t rep_chunk_idx = 0;
			uint16_t rep_row_idx = 0;
			bool has_rep_row = false;
		};
		using VecAggHashTable = VolVecHashMap<VecGroupKey, VecAggGroupState, VecGroupKeyHash>;
		using VecAggSimpleHashTable = VolVecHashMap<VecSimpleGroupKey, VecAggGroupState, VecSimpleGroupKeyHash>;
		DataChunk<DEFAULT_CHUNK_SIZE> *allocate_rep_chunk();
		void copy_rep_row(DataChunk<DEFAULT_CHUNK_SIZE> &dst, int dst_row,
						  const DataChunk<DEFAULT_CHUNK_SIZE> &src, int src_row) const;
		void export_partial_accumulator(const VecAggAccumulator *src,
										 ParallelAggPartialAccumulator *dst) const;
		void merge_partial_accumulator(const ParallelAggPartialAccumulator &src,
										size_t agg_index,
										VecAggAccumulator *dst) const;
		bool is_supported_parallel_distinct_agg(const VecAggDesc &agg) const;
		VecAggAccumulator::DistinctValueSet *ensure_distinct_value_set(VecAggAccumulator *acc) const;
		bool write_distinct_values_to_partial_file(BufFile *file,
												   const VecAggAccumulator *acc) const;
		bool read_distinct_values_from_partial_file(BufFile *file,
													VecAggAccumulator *acc) const;
		bool append_group_record_to_partial_file(BufFile *file,
												 const VecGroupKey *key,
												 const VecSimpleGroupKey *simple_key,
												 const VecAggGroupState &group) const;
		void store_group_rep_row_from_partial(VecAggGroupState *group,
											   const ParallelAggPartialGroupEntry &entry);
		bool store_group_rep_row_from_file(VecAggGroupState *group,
											const std::vector<ParallelAggFileGroupKeyCol> &group_cols);
		void ensure_group_rep_row(VecAggGroupState *group,
								  const DataChunk<DEFAULT_CHUNK_SIZE> &batch,
								  int row_idx);
		void update_group_accumulators(VecAggGroupState *group,
									   const DataChunk<DEFAULT_CHUNK_SIZE> &batch,
									   int row_idx);

		// P0 optimization: batch hash computation
		void batch_compute_group_hashes(const DataChunk<DEFAULT_CHUNK_SIZE> &batch,
										uint64_t *hashes_out);
		void batch_update_simple_aggregates(VecAggGroupState **group_ptrs,
											const DataChunk<DEFAULT_CHUNK_SIZE> &batch,
											int n_rows);

		struct VecAggPartition {
			VecAggHashTable groups;
			VecAggSimpleHashTable simple_groups;
			VolVecVector<DataChunk<DEFAULT_CHUNK_SIZE> *> rep_chunks;

		VecAggPartition(MemoryContext ctx)
			: groups(ctx),
			  simple_groups(ctx),
			  rep_chunks(VolVecVector<DataChunk<DEFAULT_CHUNK_SIZE> *>::allocator_type(ctx)) {}
		};

		static constexpr int NUM_PARTITIONS = 256;

		void consume_batch_partitioned(DataChunk<DEFAULT_CHUNK_SIZE> &batch);
		void finalize_partitions();

	VolVecVector<VecAggDesc> aggs_; VecAggHashTable hash_table_;
	VecAggSimpleHashTable simple_hash_table_;
	VolVecVector<DataChunk<DEFAULT_CHUNK_SIZE> *> rep_chunks_;
	VecAggHashTable::Iterator it_;
	VecAggSimpleHashTable::Iterator simple_it_;
		bool use_simple_group_key_ = false;
		VecOutputStorageKind simple_group_storage_ = VecOutputStorageKind::Int32;
		uint64_t input_rows_consumed_ = 0;
		uint64_t input_batches_consumed_ = 0;
	bool valid_ = true;
	bool fully_scanned_ = false;
	bool use_partitioned_ = false;
	VolVecVector<VecAggPartition *> partitions_;

	void do_sink();
};

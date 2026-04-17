#pragma once

#include "exec/plan_state.hpp"

struct VecLookupScalarKey {
	int64_t value = 0;
	uint8_t is_null = 0;

	bool operator==(const VecLookupScalarKey &other) const
	{
		return value == other.value && is_null == other.is_null;
	}
};

struct VecLookupScalarKeyHash {
	std::size_t operator()(const VecLookupScalarKey &key) const
	{
		std::size_t h = std::hash<uint8_t>{}(key.is_null);

		if (!key.is_null)
			h ^= std::hash<int64_t>{}(key.value) + 0x9e3779b9 + (h << 6) + (h >> 2);
		return h;
	}
};

struct VecLookupScalarValue {
	int32_t i32 = 0;
	int64_t i64 = 0;
	double f8 = 0.0;
	uint8_t is_null = 1;
};

using VecLookupScalarHashTable =
	VolVecHashMap<VecLookupScalarKey, VecLookupScalarValue, VecLookupScalarKeyHash>;

static constexpr int kMaxLookupKeys = 4;

struct VecLookupCompositeKey {
	int64_t values[kMaxLookupKeys] = {0, 0, 0, 0};
	uint8_t num_keys = 0;
	uint8_t is_null = 0;

	bool operator==(const VecLookupCompositeKey &other) const
	{
		if (num_keys != other.num_keys || is_null != other.is_null)
			return false;
		if (is_null)
			return true;
		for (int i = 0; i < num_keys; i++)
		{
			if (values[i] != other.values[i])
				return false;
		}
		return true;
	}
};

struct VecLookupCompositeKeyHash {
	std::size_t operator()(const VecLookupCompositeKey &key) const
	{
		std::size_t h = std::hash<uint8_t>{}(key.num_keys);

		h ^= std::hash<uint8_t>{}(key.is_null) + 0x9e3779b9 + (h << 6) + (h >> 2);
		if (key.is_null)
			return h;
		for (int i = 0; i < key.num_keys; i++)
			h ^= std::hash<int64_t>{}(key.values[i]) + 0x9e3779b9 + (h << 6) + (h >> 2);
		return h;
	}
};

using VecLookupCompositeHashTable =
	VolVecHashMap<VecLookupCompositeKey, VecLookupScalarValue, VecLookupCompositeKeyHash>;

class VecLookupProjectState : public VecPlanState {
public:
	VecLookupProjectState(std::unique_ptr<VecPlanState> left,
						  std::unique_ptr<VecPlanState> lookup_source,
						  uint16_t input_key_col,
						  VecOutputColMeta input_key_meta,
						  uint16_t lookup_key_col,
						  VecOutputColMeta lookup_key_meta,
						  uint16_t lookup_value_col,
						  int output_resno,
						  VecOutputColMeta output_meta);
	bool get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk) override;
	bool lookup_output_col_meta(int target_resno, VecOutputColMeta *out) const override;
	bool lookup_remapped_output_col_meta(int child_input_resno,
										 uint16_t *output_col,
										 VecOutputColMeta *out) const override;
	void release_jit_resources_for_proc_exit() override
	{
		if (left_ != nullptr)
			left_->release_jit_resources_for_proc_exit();
		if (lookup_source_ != nullptr)
			lookup_source_->release_jit_resources_for_proc_exit();
	}
	VecHashJoinState *find_parallel_hash_join_state_by_plan_node_id(int target_plan_node_id) override
	{
		VecHashJoinState *state = left_ != nullptr ?
			left_->find_parallel_hash_join_state_by_plan_node_id(target_plan_node_id) :
			nullptr;
		if (state != nullptr)
			return state;
		return lookup_source_ != nullptr ?
			lookup_source_->find_parallel_hash_join_state_by_plan_node_id(target_plan_node_id) :
			nullptr;
	}
private:
	bool build_lookup();
	bool extract_lookup_key(const DataChunk<DEFAULT_CHUNK_SIZE> &chunk,
							int row,
							uint16_t col,
							const VecOutputColMeta &meta,
							VecLookupScalarKey *key) const;

	std::unique_ptr<VecPlanState> left_;
	std::unique_ptr<VecPlanState> lookup_source_;
	MemoryContext memory_context_;
	VecLookupScalarHashTable lookup_table_;
	DataChunk<DEFAULT_CHUNK_SIZE> lookup_chunk_;
	uint16_t input_key_col_;
	VecOutputColMeta input_key_meta_;
	uint16_t lookup_key_col_;
	VecOutputColMeta lookup_key_meta_;
	uint16_t lookup_value_col_;
	int output_resno_;
	VecOutputColMeta output_meta_;
	bool lookup_built_;
};

class VecLookupProjectStateMultiKey : public VecPlanState {
public:
	VecLookupProjectStateMultiKey(std::unique_ptr<VecPlanState> left,
								  std::unique_ptr<VecPlanState> lookup_source,
								  int num_keys,
								  const uint16_t *input_key_cols,
								  const VecOutputColMeta *input_key_metas,
								  const uint16_t *lookup_key_cols,
								  const VecOutputColMeta *lookup_key_metas,
								  uint16_t lookup_value_col,
								  int output_resno,
								  VecOutputColMeta output_meta);
	bool get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk) override;
	bool lookup_output_col_meta(int target_resno, VecOutputColMeta *out) const override;
	bool lookup_remapped_output_col_meta(int child_input_resno,
										 uint16_t *output_col,
										 VecOutputColMeta *out) const override;
	void release_jit_resources_for_proc_exit() override
	{
		if (left_ != nullptr)
			left_->release_jit_resources_for_proc_exit();
		if (lookup_source_ != nullptr)
			lookup_source_->release_jit_resources_for_proc_exit();
	}
	VecHashJoinState *find_parallel_hash_join_state_by_plan_node_id(int target_plan_node_id) override
	{
		VecHashJoinState *state = left_ != nullptr ?
			left_->find_parallel_hash_join_state_by_plan_node_id(target_plan_node_id) :
			nullptr;
		if (state != nullptr)
			return state;
		return lookup_source_ != nullptr ?
			lookup_source_->find_parallel_hash_join_state_by_plan_node_id(target_plan_node_id) :
			nullptr;
	}
private:
	bool build_lookup();
	bool extract_lookup_key(const DataChunk<DEFAULT_CHUNK_SIZE> &chunk,
							int row,
							const uint16_t *cols,
							const VecOutputColMeta *metas,
							VecLookupCompositeKey *key) const;

	std::unique_ptr<VecPlanState> left_;
	std::unique_ptr<VecPlanState> lookup_source_;
	MemoryContext memory_context_;
	VecLookupCompositeHashTable lookup_table_;
	DataChunk<DEFAULT_CHUNK_SIZE> lookup_chunk_;
	int num_keys_;
	uint16_t input_key_cols_[kMaxLookupKeys];
	VecOutputColMeta input_key_metas_[kMaxLookupKeys];
	uint16_t lookup_key_cols_[kMaxLookupKeys];
	VecOutputColMeta lookup_key_metas_[kMaxLookupKeys];
	uint16_t lookup_value_col_;
	int output_resno_;
	VecOutputColMeta output_meta_;
	bool lookup_built_;
};

class VecLookupFilterState : public VecPlanState {
public:
	VecLookupFilterState(std::unique_ptr<VecPlanState> left,
						 std::unique_ptr<VecPlanState> lookup_source,
						 uint16_t input_key_col,
						 VecOutputColMeta input_key_meta,
						 uint16_t lookup_key_col,
						 VecOutputColMeta lookup_key_meta,
						 bool negate);
	bool get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk) override;
	bool lookup_output_col_meta(int target_resno, VecOutputColMeta *out) const override
	{
		return left_ != nullptr && left_->lookup_output_col_meta(target_resno, out);
	}
	bool lookup_remapped_output_col_meta(int child_input_resno,
										 uint16_t *output_col,
										 VecOutputColMeta *out) const override
	{
		return left_ != nullptr &&
			left_->lookup_remapped_output_col_meta(child_input_resno, output_col, out);
	}
	void release_jit_resources_for_proc_exit() override
	{
		if (left_ != nullptr)
			left_->release_jit_resources_for_proc_exit();
		if (lookup_source_ != nullptr)
			lookup_source_->release_jit_resources_for_proc_exit();
	}
	VecHashJoinState *find_parallel_hash_join_state_by_plan_node_id(int target_plan_node_id) override
	{
		VecHashJoinState *state = left_ != nullptr ?
			left_->find_parallel_hash_join_state_by_plan_node_id(target_plan_node_id) :
			nullptr;
		if (state != nullptr)
			return state;
		return lookup_source_ != nullptr ?
			lookup_source_->find_parallel_hash_join_state_by_plan_node_id(target_plan_node_id) :
			nullptr;
	}
private:
	bool build_lookup();
	bool extract_lookup_key(const DataChunk<DEFAULT_CHUNK_SIZE> &chunk,
							int row,
							uint16_t col,
							const VecOutputColMeta &meta,
							VecLookupScalarKey *key) const;

	std::unique_ptr<VecPlanState> left_;
	std::unique_ptr<VecPlanState> lookup_source_;
	MemoryContext memory_context_;
	VecLookupScalarHashTable lookup_table_;
	DataChunk<DEFAULT_CHUNK_SIZE> lookup_chunk_;
	uint16_t input_key_col_;
	VecOutputColMeta input_key_meta_;
	uint16_t lookup_key_col_;
	VecOutputColMeta lookup_key_meta_;
	bool negate_;
	bool lookup_built_;
	bool lookup_has_null_;
};

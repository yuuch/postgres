#pragma once

#include "exec/plan_state.hpp"

struct VecSortKeyDesc {
	uint16_t col_idx;
	Oid sql_type;
	VecOutputStorageKind storage_kind;
	bool descending;
	bool nulls_first;
	Oid collation;
	int scale;
};

struct VecSortKeyLane {
	VecSortKeyDesc desc;
	VolVecVector<uint8_t> nulls;
	VolVecVector<int32_t> i32_values;
	VolVecVector<int64_t> i64_values;
	VolVecVector<uint64_t> u64_values;
	VolVecVector<VecStringRef> string_values;
	VolVecVector<char> string_arena;

	VecSortKeyLane(const VecSortKeyDesc &key_desc, MemoryContext context)
		: desc(key_desc),
		  nulls(PgMemoryContextAllocator<uint8_t>(context)),
		  i32_values(PgMemoryContextAllocator<int32_t>(context)),
		  i64_values(PgMemoryContextAllocator<int64_t>(context)),
		  u64_values(PgMemoryContextAllocator<uint64_t>(context)),
		  string_values(PgMemoryContextAllocator<VecStringRef>(context)),
		  string_arena(PgMemoryContextAllocator<char>(context))
	{
	}

	VecStringRef store_string_bytes(const char *data, uint32_t len)
	{
		VecStringRef ref{len, 0, 0};

		if (len == 0 || data == nullptr)
			return ref;
		memcpy(&ref.prefix, data, len > 8 ? 8 : len);
		if (len <= 8)
		{
			ref.offset = kVecStringInlineOffset;
			return ref;
		}
		ref.offset = (uint32_t) string_arena.size();
		string_arena.insert(string_arena.end(), data, data + len);
		return ref;
	}

	const char *get_string_ptr(const VecStringRef &ref) const
	{
		return VecStringRefDataPtr(ref, string_arena.data());
	}
};

class VecSortState : public VecPlanState {
public:
	VecSortState(std::unique_ptr<VecPlanState> left, Sort *node,
				 VolVecVector<VecSortKeyDesc> key_descs,
				 int output_ncols = -1);
	~VecSortState() override;
	bool get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk) override;
	bool lookup_output_col_meta(int target_resno, VecOutputColMeta *out) const override
	{
		return left_ != nullptr && left_->lookup_output_col_meta(target_resno, out);
	}
	VecAggState *find_parallel_aggregate_state() override
	{
		return left_ != nullptr ? left_->find_parallel_aggregate_state() : nullptr;
	}
	VecAggState *find_parallel_aggregate_state_by_plan_node_id(int target_plan_node_id) override
	{
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
	}
private:
	void materialize_and_sort();
	DataChunk<DEFAULT_CHUNK_SIZE> *allocate_payload_chunk();
	void append_batch(const DataChunk<DEFAULT_CHUNK_SIZE> &input);
	void append_sort_key(uint32_t ordinal, const DataChunk<DEFAULT_CHUNK_SIZE> &input, int src_row);
	void copy_row(const DataChunk<DEFAULT_CHUNK_SIZE> &src, int src_row,
				  DataChunk<DEFAULT_CHUNK_SIZE> &dst, int dst_row) const;
	bool row_less(const VecRowRef &left, const VecRowRef &right) const;
	int compare_string_ref(const VecSortKeyLane &lane,
						  const VecStringRef &left,
						  const VecStringRef &right) const;

	std::unique_ptr<VecPlanState> left_;
	MemoryContext memory_context_;
	VolVecVector<DataChunk<DEFAULT_CHUNK_SIZE> *> payload_chunks_;
	VolVecVector<VecRowRef> rows_;
	VolVecVector<VecSortKeyDesc> key_descs_;
	VolVecVector<VecSortKeyLane> key_lanes_;
	size_t emit_pos_;
	int output_ncols_;
	bool materialized_;
};

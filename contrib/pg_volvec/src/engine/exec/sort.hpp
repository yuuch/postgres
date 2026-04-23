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

/* Global row ID encoding: (chunk_id << 32) | row_offset */
struct GlobalRowId {
	static inline uint64_t encode(uint32_t chunk_id, uint32_t row_offset) {
		return ((uint64_t)chunk_id << 32) | row_offset;
	}
	
	static inline void decode(uint64_t global_id, uint32_t &chunk_id, uint32_t &row_offset) {
		chunk_id = (uint32_t)(global_id >> 32);
		row_offset = (uint32_t)(global_id & 0xFFFFFFFFUL);
	}
};

/* A sorted run of data chunks */
struct SortedRun {
	VolVecVector<DataChunk<DEFAULT_CHUNK_SIZE> *> chunks;  /* Owned chunk pointers */
	VolVecVector<uint64_t> global_sel;                     /* Sorted global row IDs */
	uint32_t total_rows;
	uint32_t cursor;  /* Current read position for merge */
	uint32_t key_base;
	
	SortedRun(MemoryContext context)
		: chunks(PgMemoryContextAllocator<DataChunk<DEFAULT_CHUNK_SIZE> *>(context)),
		  global_sel(PgMemoryContextAllocator<uint64_t>(context)),
		  total_rows(0),
		  cursor(0),
		  key_base(0)
	{
	}
	
	/* Access sorted row at index i in global_sel */
	inline std::pair<DataChunk<DEFAULT_CHUNK_SIZE> *, uint32_t> get_sorted_row(uint32_t i) const {
		uint32_t chunk_id, row_offset;
		GlobalRowId::decode(global_sel[i], chunk_id, row_offset);
		return {chunks[chunk_id], row_offset};
	}
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
	bool push_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk) override
	{
		append_external_batch(chunk);
		return true;
	}
	void reset_external_input();
	void append_external_batch(const DataChunk<DEFAULT_CHUNK_SIZE> &chunk);
	void finish_external_input();
	VecPlanState *source_plan()
	{
		return left_.get();
	}
	bool configure_source_block_range(BlockNumber start_block, uint32_t nblocks) override;
	void clear_source_block_range() override;
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
	void reset_materialized_state();
	void materialize_and_sort();
	void flush_buffer_to_run();
	void sort_run(SortedRun &run);
	int compare_global_rows(const SortedRun &run, uint64_t global_a, uint64_t global_b) const;
	int compare_global_rows(const SortedRun &run_a,
							uint64_t global_a,
							const SortedRun &run_b,
							uint64_t global_b) const;
	int compare_string_ref(const VecSortKeyLane &lane,
						  const VecStringRef &left,
						  const VecStringRef &right) const;
	void copy_chunk(const DataChunk<DEFAULT_CHUNK_SIZE> &src, DataChunk<DEFAULT_CHUNK_SIZE> &dst);
	void copy_row(const DataChunk<DEFAULT_CHUNK_SIZE> &src, int src_row,
				  DataChunk<DEFAULT_CHUNK_SIZE> &dst, int dst_row) const;
	bool emit_from_single_run(DataChunk<DEFAULT_CHUNK_SIZE> &output);
	bool k_way_merge(DataChunk<DEFAULT_CHUNK_SIZE> &output);
	void build_sort_keys_for_run(SortedRun &run);
	void append_sort_key_from_chunk(uint32_t ordinal, const DataChunk<DEFAULT_CHUNK_SIZE> &chunk,
									uint32_t chunk_id, int row_offset);

	std::unique_ptr<VecPlanState> left_;
	MemoryContext memory_context_;
	
	VolVecVector<DataChunk<DEFAULT_CHUNK_SIZE> *> buffer_chunks_;
	uint32_t buffer_rows_;
	uint32_t buffer_limit_;
	
	VolVecVector<SortedRun> runs_;
	VolVecVector<VecSortKeyDesc> key_descs_;
	VolVecVector<VecSortKeyLane> key_lanes_;
	
	int output_ncols_;
	bool finalized_;
	
	struct MergeEntry {
		uint32_t run_id;
		uint64_t global_id;
		
		MergeEntry(uint32_t rid, uint64_t gid) : run_id(rid), global_id(gid) {}
	};
	
	struct MergeEntryComparator {
		const VecSortState *sort_state;
		
		MergeEntryComparator(const VecSortState *state) : sort_state(state) {}
		
		bool operator()(const MergeEntry &a, const MergeEntry &b) const;
	};
	
	std::unique_ptr<std::priority_queue<MergeEntry, std::vector<MergeEntry>, MergeEntryComparator>> merge_heap_;
};

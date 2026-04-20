#pragma once

#include "exec/plan_state.hpp"

struct VecProjectColDesc {
	std::unique_ptr<VecExprProgram> expr;
	int target_resno;
	Oid sql_type;
	VecOutputStorageKind storage_kind;
	int scale;
	bool direct_var = false;
	bool string_prefix_var = false;
	uint16_t input_col = 0;
	uint32_t string_prefix_len = 0;
};

class VecProjectState : public VecPlanState {
public:
	VecProjectState(std::unique_ptr<VecPlanState> left,
					VolVecVector<VecProjectColDesc> columns);
	bool get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk) override;
	bool lookup_output_col_meta(int target_resno, VecOutputColMeta *out) const override;
	bool lookup_remapped_output_col_meta(int child_input_resno,
										 uint16_t *output_col,
										 VecOutputColMeta *out) const override;
	bool configure_source_block_range(BlockNumber start_block, uint32_t nblocks) override
	{
		bool ok = left_ != nullptr && left_->configure_source_block_range(start_block, nblocks);

		if (pg_volvec_trace_hooks && !ok)
			elog(LOG,
				 "pg_volvec: project block range configure failed start=%u nblocks=%u left=%s",
				 start_block,
				 nblocks,
				 left_ != nullptr ? "ok" : "null");
		return ok;
	}
	void clear_source_block_range() override
	{
		if (left_ != nullptr)
			left_->clear_source_block_range();
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
		for (auto &column : columns_)
		{
			if (column.expr != nullptr)
				column.expr->release_jit_resources_for_proc_exit();
		}
	}
private:
	std::unique_ptr<VecPlanState> left_;
	VolVecVector<VecProjectColDesc> columns_;
	DataChunk<DEFAULT_CHUNK_SIZE> input_chunk_;
};

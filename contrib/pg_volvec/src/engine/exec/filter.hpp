#pragma once

#include "exec/plan_state.hpp"

class VecFilterState : public VecPlanState {
public:
		VecFilterState(std::unique_ptr<VecPlanState> left, std::unique_ptr<VecExprProgram> prog) : left_(std::move(left)), program_(std::move(prog)) {}
		bool get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk) override;
		bool drain_to(VecPlanState *downstream) override;
		bool push_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk) override;
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
	bool configure_source_block_range(BlockNumber start_block, uint32_t nblocks) override
	{
		bool ok = left_ != nullptr && left_->configure_source_block_range(start_block, nblocks);

		if (pg_volvec_trace_hooks && !ok)
			elog(LOG,
				 "pg_volvec: filter block range configure failed start=%u nblocks=%u left=%s",
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
		if (program_ != nullptr)
			program_->release_jit_resources_for_proc_exit();
	}
	private:
		std::unique_ptr<VecPlanState> left_; std::unique_ptr<VecExprProgram> program_;
		VecPlanState *push_downstream_ = nullptr;
	};

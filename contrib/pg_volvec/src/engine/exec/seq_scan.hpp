#pragma once

#include "exec/plan_state.hpp"

class VecSeqScanState : public VecPlanState {
public:
	VecSeqScanState(Relation rel,
					Snapshot snapshot,
					const DeformProgram *program,
					ParallelTableScanDesc parallel_scan_desc = nullptr);
	~VecSeqScanState() override;
	bool get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk) override;
	bool lookup_output_col_meta(int target_resno, VecOutputColMeta *out) const override;
	bool configure_source_block_range(BlockNumber start_block, uint32_t nblocks) override;
	void clear_source_block_range() override;
	void configure_block_range(BlockNumber start_block, uint32_t nblocks);
	void clear_block_range();
	VecSeqScanState *find_parallel_source_scan_state() override
	{
		return this;
	}
	void release_jit_resources_for_proc_exit() override
	{
#ifdef USE_LLVM
		if (jit_context_ != nullptr)
		{
			pg_volvec_release_llvm_jit_context(jit_context_);
			jit_context_ = nullptr;
		}
#endif
	}
	uint64_t blocks_opened() const
	{
		return blocks_opened_;
	}
private:
	void prepare_bindings(DataChunk<DEFAULT_CHUNK_SIZE> &chunk, DeformBindings *bindings) const;
	bool open_next_buffer();

	Relation rel_;
	Snapshot snapshot_;
	HeapScanDesc scan_;
	ReadStream *stream_ = nullptr;
	Buffer current_buf_ = InvalidBuffer;
	Buffer vmbuf_ = InvalidBuffer;
	OffsetNumber current_offnum_ = FirstOffsetNumber;
	bool all_visible_ = false;
	bool block_range_active_ = false;
	ReadStream *block_range_stream_ = nullptr;
	BlockRangeReadStreamPrivate block_range_stream_private_{};
	BlockNumber block_range_start_ = InvalidBlockNumber;
	BlockNumber block_range_end_ = InvalidBlockNumber;
	uint64_t blocks_opened_ = 0;
	DataChunkDeformer deformer_;
	JitContext *jit_context_ = nullptr;
};

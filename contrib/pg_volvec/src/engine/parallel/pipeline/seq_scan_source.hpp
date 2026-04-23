#pragma once

#include "core/types.hpp"
#include "core/memory.hpp"
#include "core/data_chunk.hpp"
#include "core/data_chunk_deform.hpp"
#include "expr/expr.hpp"
#include "exec/plan_state.hpp"
#include "exec/seq_scan.hpp"
}

#include "parallel/pipeline/source.hpp"

namespace pg_volvec {
namespace pipeline {

static_assert(PIPELINE_DEFAULT_CHUNK_SIZE == DEFAULT_CHUNK_SIZE,
              "pipeline chunk size must match core DEFAULT_CHUNK_SIZE");

struct SeqScanSourceShared {
	pg_atomic_uint64 *next_block;
	BlockNumber       total_blocks;
	uint32            morsel_nblocks;
};

class GlobalSeqScanState : public GlobalSourceState {
public:
	explicit GlobalSeqScanState(const SeqScanSourceShared &shared) : shared_(shared) {}
	const SeqScanSourceShared &shared() const { return shared_; }

private:
	SeqScanSourceShared shared_;
};

class LocalSeqScanState : public LocalSourceState {
public:
	bool morsel_active = false;
};

class SeqScanSource : public Source {
public:
	SeqScanSource(VecSeqScanState *scan, const SeqScanSourceShared &shared)
		: scan_(scan), shared_(shared) {}

	std::unique_ptr<GlobalSourceState> GetGlobalSourceState(ExecCtx &ctx) override;
	std::unique_ptr<LocalSourceState>  GetLocalSourceState(ExecCtx &ctx, GlobalSourceState &gstate) override;
	SourceResultType GetData(ExecCtx &ctx, PipelineChunk &out, OperatorSourceInput &input) override;
	bool ParallelSource() const override { return true; }

private:
	VecSeqScanState   *scan_;
	SeqScanSourceShared shared_;
};

}
}

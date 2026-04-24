#pragma once

/*
 * pipeline/physical_seq_scan.hpp
 *
 * PhysicalSeqScan (M-IR-MIN). Source-only operator that owns its qual
 * (Filter is fused, never lowered as a separate node). Wraps a VecSeqScanState.
 *
 * NOT WIRED INTO RUNTIME YET. Sibling of SeqScanSource (the legacy runtime's
 * Source impl). M-FRAME-MIN's MetaPipeline walker will instantiate this; until
 * then it is dead code by design and exists only to lock down the API shape.
 *
 * Spec: PIPELINE_PORT_PLAN.md §15.4.
 */

extern "C" {
#include "postgres.h"
#include "storage/block.h"
#include "port/atomics.h"
}

#include "parallel/pipeline/physical_operator.hpp"

namespace pg_volvec {

class VecSeqScanState;
struct ExprProgram;

namespace pipeline {

struct PhysicalSeqScanShared {
	pg_atomic_uint64 *next_block;
	BlockNumber       total_blocks;
	uint32            morsel_nblocks;
};

class PhysicalSeqScan final : public PhysicalOperator {
public:
	PhysicalSeqScan(VecSeqScanState *scan, ExprProgram *qual, const PhysicalSeqScanShared &shared)
		: PhysicalOperator(PhysicalOperatorType::SEQ_SCAN), scan_(scan), qual_(qual), shared_(shared) {}

	bool IsSource() const override { return true; }

	std::unique_ptr<GlobalSourceState> GetGlobalSourceState(ExecCtx &ctx) override;
	std::unique_ptr<LocalSourceState>  GetLocalSourceState(ExecCtx &ctx, GlobalSourceState &gstate) override;
	SourceResultType GetData(ExecCtx &ctx, PipelineChunk &out, OperatorSourceInput &input) override;

	int MaxThreads(ExecCtx &ctx) const override;

private:
	VecSeqScanState         *scan_;
	ExprProgram             *qual_;
	PhysicalSeqScanShared    shared_;
};

}  /* namespace pipeline */
}  /* namespace pg_volvec */

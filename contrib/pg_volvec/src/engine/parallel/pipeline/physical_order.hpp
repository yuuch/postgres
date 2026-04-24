#pragma once

/*
 * pipeline/physical_order.hpp
 *
 * PhysicalOrder (M-IR-MIN). Dual Sink+Source pipeline-breaker. MaxThreads=1
 * for Q1 (≤6 group rows post-aggregation; parallel sort deferred).
 *
 * NOT WIRED INTO RUNTIME YET. Real sort logic lands in M-BM-MIN (sort_sink).
 *
 * Spec: PIPELINE_PORT_PLAN.md §15.4.
 */

#include "parallel/pipeline/physical_operator.hpp"

namespace pg_volvec {
namespace pipeline {

class PhysicalOrder final : public PhysicalOperator {
public:
	PhysicalOrder() : PhysicalOperator(PhysicalOperatorType::ORDER) {}

	bool IsSource() const override { return true; }
	bool IsSink() const override { return true; }
	bool IsPipelineBreaker() const override { return true; }

	int MaxThreads(ExecCtx &ctx) const override { (void) ctx; return 1; }

	std::unique_ptr<GlobalSinkState>   GetGlobalSinkState(ExecCtx &ctx) override;
	std::unique_ptr<LocalSinkState>    GetLocalSinkState(ExecCtx &ctx, GlobalSinkState &gstate) override;
	SinkResultType                     SinkChunk(ExecCtx &ctx, PipelineChunk &in, OperatorSinkInput &input) override;
	SinkCombineResultType              Combine(ExecCtx &ctx, OperatorSinkCombineInput &input) override;
	SinkFinalizeType                   Finalize(ExecCtx &ctx, GlobalSinkState &gstate) override;

	std::unique_ptr<GlobalSourceState> GetGlobalSourceState(ExecCtx &ctx) override;
	std::unique_ptr<LocalSourceState>  GetLocalSourceState(ExecCtx &ctx, GlobalSourceState &gstate) override;
	SourceResultType                   GetData(ExecCtx &ctx, PipelineChunk &out, OperatorSourceInput &input) override;
};

}  /* namespace pipeline */
}  /* namespace pg_volvec */

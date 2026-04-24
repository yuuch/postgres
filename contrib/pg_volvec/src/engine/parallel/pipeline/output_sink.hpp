#pragma once

/*
 * pipeline/output_sink.hpp
 *
 * OutputSink (M-IR-MIN). Pipeline terminator that hands materialized chunks
 * back to the bridge for slot delivery. Sink-only; not a pipeline-breaker
 * because nothing follows it.
 *
 * NOT WIRED INTO RUNTIME YET. M-FRAME-MIN's leader event loop drains it.
 *
 * Spec: PIPELINE_PORT_PLAN.md §15.4.
 */

#include "parallel/pipeline/physical_operator.hpp"

namespace pg_volvec {
namespace pipeline {

class OutputSink final : public PhysicalOperator {
public:
	OutputSink() : PhysicalOperator(PhysicalOperatorType::OUTPUT) {}

	bool IsSink() const override { return true; }
	bool IsPipelineBreaker() const override { return false; }

	int MaxThreads(ExecCtx &ctx) const override { (void) ctx; return 1; }

	std::unique_ptr<GlobalSinkState> GetGlobalSinkState(ExecCtx &ctx) override;
	std::unique_ptr<LocalSinkState>  GetLocalSinkState(ExecCtx &ctx, GlobalSinkState &gstate) override;
	SinkResultType                   SinkChunk(ExecCtx &ctx, PipelineChunk &in, OperatorSinkInput &input) override;
	SinkCombineResultType            Combine(ExecCtx &ctx, OperatorSinkCombineInput &input) override;
	SinkFinalizeType                 Finalize(ExecCtx &ctx, GlobalSinkState &gstate) override;
};

}  /* namespace pipeline */
}  /* namespace pg_volvec */

#pragma once

/*
 * pipeline/physical_hash_aggregate.hpp
 *
 * PhysicalHashAggregate (M-IR-MIN). Dual Sink+Source pipeline-breaker. Uses
 * cross-worker radix-partitioned hash table (per user decision §15 + handoff).
 *
 * NOT WIRED INTO RUNTIME YET. Real radix table + spill semantics land in
 * M-BM-MIN. M-IR-MIN only locks the API shape so M-FRAME-MIN can wire it.
 *
 * Spec: PIPELINE_PORT_PLAN.md §15.4.
 */

#include "parallel/pipeline/physical_operator.hpp"

namespace pg_volvec {

class VecAggState;

namespace pipeline {

class PhysicalHashAggregate final : public PhysicalOperator {
public:
	explicit PhysicalHashAggregate(VecAggState *agg)
		: PhysicalOperator(PhysicalOperatorType::HASH_AGGREGATE), agg_(agg) {}

	bool IsSource() const override { return true; }
	bool IsSink() const override { return true; }
	bool IsPipelineBreaker() const override { return true; }

	std::unique_ptr<GlobalSinkState>   GetGlobalSinkState(ExecCtx &ctx) override;
	std::unique_ptr<LocalSinkState>    GetLocalSinkState(ExecCtx &ctx, GlobalSinkState &gstate) override;
	SinkResultType                     SinkChunk(ExecCtx &ctx, PipelineChunk &in, OperatorSinkInput &input) override;
	SinkCombineResultType              Combine(ExecCtx &ctx, OperatorSinkCombineInput &input) override;
	SinkFinalizeType                   Finalize(ExecCtx &ctx, GlobalSinkState &gstate) override;

	std::unique_ptr<GlobalSourceState> GetGlobalSourceState(ExecCtx &ctx) override;
	std::unique_ptr<LocalSourceState>  GetLocalSourceState(ExecCtx &ctx, GlobalSourceState &gstate) override;
	SourceResultType                   GetData(ExecCtx &ctx, PipelineChunk &out, OperatorSourceInput &input) override;

private:
	VecAggState *agg_;
};

}  /* namespace pipeline */
}  /* namespace pg_volvec */

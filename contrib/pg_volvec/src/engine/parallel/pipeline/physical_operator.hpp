#pragma once

/*
 * pipeline/physical_operator.hpp
 *
 * PhysicalOperator IR (M-IR-MIN). DuckDB-faithful unified operator base that
 * fuses Source / Operator / Sink roles via IsSource()/IsSink() flags.
 *
 * NOT WIRED INTO RUNTIME YET. Coexists with the legacy LoweredPipeline path
 * (see pipeline_lowering.hpp). The LoweredPipeline runtime continues to use
 * the plain Source/Operator/Sink interfaces. PhysicalOperator becomes live
 * only at M-FRAME-MIN, when MetaPipeline::BuildForQ1 walks this tree.
 *
 * Spec: PIPELINE_PORT_PLAN.md §15.4 (P3X-Q1).
 */

#include <memory>
#include <vector>

#include "parallel/pipeline/operator.hpp"
#include "parallel/pipeline/sink.hpp"
#include "parallel/pipeline/source.hpp"
#include "parallel/pipeline/types.hpp"

namespace pg_volvec {
namespace pipeline {

enum class PhysicalOperatorType : uint8_t {
	SEQ_SCAN,
	HASH_AGGREGATE,
	ORDER,
	OUTPUT,
};

class MetaPipeline;

class PhysicalOperator {
public:
	explicit PhysicalOperator(PhysicalOperatorType type) : type_(type) {}
	virtual ~PhysicalOperator() = default;

	PhysicalOperatorType type() const { return type_; }
	const std::vector<std::unique_ptr<PhysicalOperator>> &children() const { return children_; }

	void AddChild(std::unique_ptr<PhysicalOperator> child) { children_.push_back(std::move(child)); }

	virtual bool IsSource() const { return false; }
	virtual bool IsSink() const { return false; }

	/* Pipeline-breakers split MetaPipeline construction. HashAggregate / Order = true. */
	virtual bool IsPipelineBreaker() const { return IsSink(); }

	virtual std::unique_ptr<GlobalSourceState> GetGlobalSourceState(ExecCtx &ctx) {
		(void) ctx;
		return nullptr;
	}
	virtual std::unique_ptr<LocalSourceState> GetLocalSourceState(ExecCtx &ctx, GlobalSourceState &gstate) {
		(void) ctx; (void) gstate;
		return nullptr;
	}
	virtual SourceResultType GetData(ExecCtx &ctx, PipelineChunk &out, OperatorSourceInput &input) {
		(void) ctx; (void) out; (void) input;
		return SourceResultType::FINISHED;
	}

	virtual std::unique_ptr<GlobalOperatorState> GetGlobalOperatorState(ExecCtx &ctx) {
		(void) ctx;
		return nullptr;
	}
	virtual std::unique_ptr<OperatorState> GetOperatorState(ExecCtx &ctx) {
		(void) ctx;
		return nullptr;
	}
	virtual OperatorResultType Execute(ExecCtx &ctx, PipelineChunk &in, PipelineChunk &out, OperatorState &state) {
		(void) ctx; (void) in; (void) out; (void) state;
		return OperatorResultType::FINISHED;
	}

	virtual std::unique_ptr<GlobalSinkState> GetGlobalSinkState(ExecCtx &ctx) {
		(void) ctx;
		return nullptr;
	}
	virtual std::unique_ptr<LocalSinkState> GetLocalSinkState(ExecCtx &ctx, GlobalSinkState &gstate) {
		(void) ctx; (void) gstate;
		return nullptr;
	}
	virtual SinkResultType SinkChunk(ExecCtx &ctx, PipelineChunk &in, OperatorSinkInput &input) {
		(void) ctx; (void) in; (void) input;
		return SinkResultType::FINISHED;
	}
	virtual SinkCombineResultType Combine(ExecCtx &ctx, OperatorSinkCombineInput &input) {
		(void) ctx; (void) input;
		return SinkCombineResultType::FINISHED;
	}
	virtual SinkFinalizeType Finalize(ExecCtx &ctx, GlobalSinkState &gstate) {
		(void) ctx; (void) gstate;
		return SinkFinalizeType::READY;
	}

	/* Worker count hint for the scheduler. Single-threaded ops return 1. */
	virtual int MaxThreads(ExecCtx &ctx) const {
		(void) ctx;
		return 0;
	}

	/*
	 * BuildPipelines hook reserved for M-FRAME-MIN MetaPipeline construction.
	 * Default no-op; concrete operators override only when they need custom
	 * dependency wiring beyond the standard child-walk.
	 */
	virtual void BuildPipelines(MetaPipeline &meta) { (void) meta; }

private:
	PhysicalOperatorType                                  type_;
	std::vector<std::unique_ptr<PhysicalOperator>>        children_;
};

}  /* namespace pipeline */
}  /* namespace pg_volvec */

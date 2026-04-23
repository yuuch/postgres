#pragma once

/*
 * pipeline/operator.hpp
 *
 * Operator interface (P1: declarations only). See PIPELINE_REFACTOR_DESIGN.md §3.3.
 */

#include <memory>

#include "parallel/pipeline/types.hpp"

namespace pg_volvec {
namespace pipeline {

class OperatorState {
public:
	virtual ~OperatorState() = default;
};

class Operator {
public:
	virtual ~Operator() = default;

	virtual std::unique_ptr<OperatorState>
	GetOperatorState(ExecCtx &ctx) = 0;

	virtual OperatorResultType
	Execute(ExecCtx &ctx, PipelineChunk &in, PipelineChunk &out, OperatorState &state) = 0;

	virtual bool ParallelOperator() const { return true; }
};

}  /* namespace pipeline */
}  /* namespace pg_volvec */

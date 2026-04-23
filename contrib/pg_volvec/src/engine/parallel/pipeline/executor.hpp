#pragma once

/*
 * pipeline/executor.hpp
 *
 * WorkerPipelineExecutor (P1: declaration; impl in executor.cpp).
 * See PIPELINE_REFACTOR_DESIGN.md §4.
 */

#include <cstddef>
#include <vector>

#include "parallel/pipeline/pipeline.hpp"
#include "parallel/pipeline/types.hpp"

namespace pg_volvec {
namespace pipeline {

class WorkerPipelineExecutor {
public:
	WorkerPipelineExecutor(Source *src,
	                       const std::vector<Operator *> &ops,
	                       Sink *sink);

	/*
	 * Run the pipeline until source returns FINISHED, sink returns FINISHED,
	 * or max_chunks chunks have been pushed to the sink.
	 *
	 * Returns true iff the source signaled FINISHED. Caller is responsible
	 * for invoking Sink::Combine after the executor returns.
	 *
	 * P1 contract: BLOCKED on any path triggers an Assert + ereport(ERROR).
	 */
	bool Execute(ExecCtx          &ctx,
	             GlobalSourceState &gsrc,
	             LocalSourceState  &lsrc,
	             GlobalSinkState   *gsink,
	             LocalSinkState    *lsink,
	             std::size_t        max_chunks);

private:
	[[maybe_unused]] Source        *src_;
	const std::vector<Operator *>   ops_;
	[[maybe_unused]] Sink          *sink_;
};

}  /* namespace pipeline */
}  /* namespace pg_volvec */

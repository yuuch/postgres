#pragma once

/*
 * pipeline/pipeline.hpp
 *
 * Pipeline aggregate (P1: declarations only).
 * See PIPELINE_REFACTOR_DESIGN.md §4, §7.
 */

#include <vector>

#include "parallel/pipeline/operator.hpp"
#include "parallel/pipeline/sink.hpp"
#include "parallel/pipeline/source.hpp"
#include "parallel/pipeline/types.hpp"

namespace pg_volvec {
namespace pipeline {

struct Pipeline {
	PipelineId               id = INVALID_PIPELINE_ID;
	Source                  *src   = nullptr;
	std::vector<Operator *>  ops;
	Sink                    *sink  = nullptr;             /* nullable: terminal pipeline */
	std::vector<PipelineId>  depends_on;

	GlobalSourceState       *global_src  = nullptr;
	GlobalSinkState         *global_sink = nullptr;
};

}  /* namespace pipeline */
}  /* namespace pg_volvec */

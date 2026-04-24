#include "parallel/pipeline/pipeline_combine_event.hpp"

namespace pg_volvec {
namespace pipeline {

PipelineCombineEvent::PipelineCombineEvent(PipelineId pid, Pipeline *pipeline,
                                           TaskScheduler *scheduler)
    : Event(pid, scheduler), pipeline_(pipeline) {}

void
PipelineCombineEvent::Schedule()
{
	/* Sink::Combine fan-out lands in step 3g; stub keeps lifecycle wired. */
	FinishEvent();
}

}  /* namespace pipeline */
}  /* namespace pg_volvec */

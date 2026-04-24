#include "parallel/pipeline/pipeline_finalize_event.hpp"

namespace pg_volvec {
namespace pipeline {

PipelineFinalizeEvent::PipelineFinalizeEvent(PipelineId pid, Pipeline *pipeline,
                                             TaskScheduler *scheduler)
    : Event(pid, scheduler), pipeline_(pipeline) {}

void
PipelineFinalizeEvent::Schedule()
{
	/* Leader Sink::Finalize call lands in step 3g; stub keeps lifecycle wired. */
	FinishEvent();
}

}  /* namespace pipeline */
}  /* namespace pg_volvec */

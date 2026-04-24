#include "parallel/pipeline/pipeline_run_event.hpp"

namespace pg_volvec {
namespace pipeline {

PipelineRunEvent::PipelineRunEvent(PipelineId pid, Pipeline *pipeline,
                                   TaskScheduler *scheduler)
    : Event(pid, scheduler), pipeline_(pipeline) {}

void
PipelineRunEvent::Schedule()
{
	/* Task scheduling lands in step 3g; this stub keeps the lifecycle wired. */
	FinishEvent();
}

}  /* namespace pipeline */
}  /* namespace pg_volvec */

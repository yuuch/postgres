/*
 * pipeline/task.cpp  (M-FRAME-MIN step 3d)
 *
 * See task.hpp for the Task / Event completion contract. Bodies are stubs
 * until step 3g wires real Source -> Operator -> Sink driving and Combine /
 * Finalize fan-out through TaskScheduler.
 */

#include "parallel/pipeline/task.hpp"

namespace pg_volvec {
namespace pipeline {

Task::Task(std::shared_ptr<Event> event, Pipeline *pipeline)
    : event_(std::move(event)), pipeline_(pipeline) {}

PipelineRunTask::PipelineRunTask(std::shared_ptr<Event> event,
                                 Pipeline *pipeline, int32_t worker_index)
    : Task(std::move(event), pipeline), worker_index_(worker_index) {}

TaskExecutionResult
PipelineRunTask::Execute()
{
	return TaskExecutionResult::TASK_FINISHED;
}

PipelineCombineTask::PipelineCombineTask(std::shared_ptr<Event> event,
                                         Pipeline *pipeline,
                                         int32_t worker_index)
    : Task(std::move(event), pipeline), worker_index_(worker_index) {}

TaskExecutionResult
PipelineCombineTask::Execute()
{
	return TaskExecutionResult::TASK_FINISHED;
}

PipelineFinalizeTask::PipelineFinalizeTask(std::shared_ptr<Event> event,
                                           Pipeline *pipeline)
    : Task(std::move(event), pipeline) {}

TaskExecutionResult
PipelineFinalizeTask::Execute()
{
	return TaskExecutionResult::TASK_FINISHED;
}

}  /* namespace pipeline */
}  /* namespace pg_volvec */

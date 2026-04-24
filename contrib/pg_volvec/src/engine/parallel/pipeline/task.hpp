#pragma once

/*
 * pipeline/task.hpp  (M-FRAME-MIN step 3d)
 *
 * Task base + 3 subclasses (Run / Combine / Finalize) for the M-FRAME-MIN
 * pipeline runtime locked by PIPELINE_PORT_PLAN.md §15.1 and
 * GLOBAL_LOCAL_STATE_DESIGN.md §8.5.
 *
 * Contract:
 *   - One Event schedules N Tasks (one per worker for Run/Combine; exactly
 *     one for Finalize, leader-only).
 *   - Task::Execute() returns TASK_FINISHED on success or TASK_ERROR on
 *     failure; the calling worker is responsible for invoking
 *     event->FinishEvent() (success) or event->Abort() (failure) exactly
 *     once after the LAST task of that event has finished. Aggregation
 *     across workers is the TaskScheduler's job (3g).
 *   - TASK_NOT_FINISHED is reserved for cooperative re-yield (used by
 *     PipelineRunTask if morsel queue is empty but more workers may push).
 *   - BLOCKED is intentionally absent (forbidden in M-FRAME-MIN per
 *     pipeline/AGENTS.md ANTI-PATTERNS).
 *
 * Tasks own no DSM/DSA resources directly; they borrow Pipeline + Event
 * pointers whose lifetimes are guaranteed by the TaskScheduler (3g).
 */

#include <cstdint>
#include <memory>

extern "C" {
#include "postgres.h"
}

#include "core/memory.hpp"

namespace pg_volvec {
namespace pipeline {

class Event;
struct Pipeline;

enum class TaskExecutionResult : uint8_t {
	TASK_FINISHED,
	TASK_NOT_FINISHED,
	TASK_ERROR,
};

class Task : public PgMemoryContextObject {
public:
	Task(std::shared_ptr<Event> event, Pipeline *pipeline);
	virtual ~Task() = default;

	Task(const Task &)            = delete;
	Task &operator=(const Task &) = delete;

	virtual TaskExecutionResult Execute() = 0;

	const std::shared_ptr<Event> &event()    const { return event_; }
	Pipeline                     *pipeline() const { return pipeline_; }

protected:
	std::shared_ptr<Event>  event_;
	Pipeline               *pipeline_;
};

class PipelineRunTask final : public Task {
public:
	PipelineRunTask(std::shared_ptr<Event> event, Pipeline *pipeline,
	                int32_t worker_index);

	TaskExecutionResult Execute() override;

	int32_t worker_index() const { return worker_index_; }

private:
	int32_t worker_index_;
};

class PipelineCombineTask final : public Task {
public:
	PipelineCombineTask(std::shared_ptr<Event> event, Pipeline *pipeline,
	                    int32_t worker_index);

	TaskExecutionResult Execute() override;

	int32_t worker_index() const { return worker_index_; }

private:
	int32_t worker_index_;
};

class PipelineFinalizeTask final : public Task {
public:
	PipelineFinalizeTask(std::shared_ptr<Event> event, Pipeline *pipeline);

	TaskExecutionResult Execute() override;
};

}  /* namespace pipeline */
}  /* namespace pg_volvec */

#pragma once

/*
 * pipeline/task_scheduler.hpp  (M-FRAME-MIN step 3g.1)
 *
 * Per-query orchestrator for the 3-event pipeline runtime. Locked by:
 *   docs/PIPELINE_PORT_PLAN.md §15.5
 *   docs/GLOBAL_LOCAL_STATE_DESIGN.md §8.5
 *
 * 3g.1 (this commit): skeleton only. Owns MetaPipelineBundle, allocates
 * EventIds and PipelineIds, owns the two PipelineDsmLookups, computes the
 * total DSM size needed by the query. EnqueueTasks() is a stub that the
 * concrete *Event::Schedule() overrides will call (3g.2 wires real
 * dispatch; today they still go to FinishEvent() directly).
 *
 * 3g.2 will add: leader bgworker launch, worker latch table, real
 * EnqueueTasks fan-out, leader wait loop, worker pump.
 *
 * 3g.3 will fill PipelineRunTask::Execute / Combine / Finalize bodies.
 *
 * Lifetime: owned by the leader process for the duration of one query.
 * Shared structures (PipelineSharedControl, DsmTaskQueue, DSA segment) are
 * referenced via raw pointers; this class does NOT own DSM memory and does
 * NOT call dsm_attach / dsm_detach itself (bridge owns the segment).
 *
 * Q1-narrowed scope today; M-Q1-PERF will widen Pipeline -> N RUN tasks.
 */

#include <cstddef>
#include <cstdint>
#include <memory>

extern "C" {
#include "postgres.h"
#include "utils/dsa.h"
}

#include "core/memory.hpp"
#include "parallel/pipeline/event.hpp"
#include "parallel/pipeline/meta_pipeline.hpp"
#include "parallel/pipeline/physical_operator.hpp"
#include "parallel/pipeline/pipeline.hpp"
#include "parallel/pipeline/pipeline_dsm_lookup.hpp"
#include "parallel/pipeline/types.hpp"

namespace pg_volvec {
namespace pipeline {

class DsmTaskQueue;
struct PipelineSharedControl;
struct TaskDescriptor;

/* DSM sizing inputs decided by the leader before dsm_create. */
struct TaskSchedulerSizing {
	uint32_t worker_count;       /* including leader */
	uint32_t task_queue_capacity;/* power of two */
};

/* DSM section sizes returned by EstimateDsmSize, also used by 3g.2's
 * leader allocator to compute shm_toc keys. */
struct TaskSchedulerDsmLayout {
	size_t control_bytes;        /* PipelineSharedControl */
	size_t task_queue_bytes;     /* DsmTaskQueue + cells */
	size_t event_counters_bytes; /* per-event pg_atomic_uint32 outstanding_tasks */
	size_t total_bytes;          /* sum (caller adds shm_toc overhead) */
};

class TaskScheduler {
public:
	TaskScheduler(MemoryContext              mcxt,
	              std::unique_ptr<MetaPipelineBundle> bundle,
	              const TaskSchedulerSizing &sizing);

	~TaskScheduler() = default;

	TaskScheduler(const TaskScheduler &)            = delete;
	TaskScheduler &operator=(const TaskScheduler &) = delete;

	/* Compute the DSM byte budget for the given sizing. Used by leader to
	 * pre-size the segment before dsm_create. event_count is determined by
	 * BuildEvents() and so MUST be called after construction. */
	TaskSchedulerDsmLayout EstimateDsmSize() const;

	/* Build the 3 Events per Pipeline (Run -> Combine -> Finalize) and the
	 * inter-pipeline dependency edges. Assigns EventIds and registers them
	 * in event_lookup_. Idempotent: callable exactly once per scheduler. */
	void BuildEvents();

	/* Concrete *Event::Schedule() implementations call this. 3g.1 stub:
	 * does nothing. 3g.2: fans out N TaskDescriptors into the DSM queue and
	 * wakes worker latches. */
	void EnqueueTasks(Event &event);

	/* Read-side accessors (used by leader + worker pumps in 3g.2). */
	MetaPipelineBundle           &bundle()            { return *bundle_; }
	MemoryContext                 mcxt()        const { return mcxt_; }
	PipelineDsmLookup<Pipeline>  &pipeline_lookup()   { return pipelines_; }
	PipelineDsmLookup<Event>     &event_lookup()      { return events_; }
	uint32_t                      event_count() const { return next_event_id_; }
	uint32_t                      worker_count() const { return sizing_.worker_count; }
	const TaskSchedulerSizing    &sizing()      const { return sizing_; }

private:
	MemoryContext                                  mcxt_;
	std::unique_ptr<MetaPipelineBundle>            bundle_;
	TaskSchedulerSizing                            sizing_;

	PipelineDsmLookup<Pipeline>                    pipelines_;
	PipelineDsmLookup<Event>                       events_;

	PgVector<std::shared_ptr<Event>>               events_owned_;
	uint32_t                                       next_event_id_ = 0;
};

}  /* namespace pipeline */
}  /* namespace pg_volvec */

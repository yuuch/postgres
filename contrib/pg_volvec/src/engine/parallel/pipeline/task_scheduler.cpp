/*
 * pipeline/task_scheduler.cpp  (M-FRAME-MIN step 3g.1)
 *
 * Skeleton orchestrator. See task_scheduler.hpp for the contract.
 *
 * 3g.1 ships: id assignment, event construction, DSM size estimation. The
 * EnqueueTasks body is intentionally a no-op until 3g.2 wires real dispatch.
 */

#include "parallel/pipeline/task_scheduler.hpp"

extern "C" {
#include "port/atomics.h"
}

#include "parallel/pipeline/dsm_control.hpp"
#include "parallel/pipeline/dsm_task_queue.hpp"
#include "parallel/pipeline/pipeline_combine_event.hpp"
#include "parallel/pipeline/pipeline_finalize_event.hpp"
#include "parallel/pipeline/pipeline_run_event.hpp"

namespace pg_volvec {
namespace pipeline {

TaskScheduler::TaskScheduler(MemoryContext              mcxt,
                             std::unique_ptr<MetaPipelineBundle> bundle,
                             const TaskSchedulerSizing &sizing)
    : mcxt_(mcxt)
    , bundle_(std::move(bundle))
    , sizing_(sizing)
    , pipelines_(mcxt)
    , events_(mcxt)
{
	Assert(mcxt_ != nullptr);
	Assert(bundle_ != nullptr);
	Assert(sizing_.worker_count >= 1);

	for (auto &p : bundle_->pipelines)
	{
		Assert(p->id != INVALID_PIPELINE_ID);
		pipelines_.Register(static_cast<uint32_t>(p->id), p.get());
	}
}

void
TaskScheduler::BuildEvents()
{
	Assert(events_owned_.empty());

	const auto &pipelines = bundle_->pipelines;

	PgMemoryContextAllocator<std::shared_ptr<PipelineRunEvent>>      run_alloc(mcxt_);
	PgMemoryContextAllocator<std::shared_ptr<PipelineCombineEvent>>  cmb_alloc(mcxt_);
	PgMemoryContextAllocator<std::shared_ptr<PipelineFinalizeEvent>> fin_alloc(mcxt_);

	std::vector<std::shared_ptr<PipelineRunEvent>,
	            PgMemoryContextAllocator<std::shared_ptr<PipelineRunEvent>>>
	    run_events(run_alloc);
	std::vector<std::shared_ptr<PipelineCombineEvent>,
	            PgMemoryContextAllocator<std::shared_ptr<PipelineCombineEvent>>>
	    combine_events(cmb_alloc);
	std::vector<std::shared_ptr<PipelineFinalizeEvent>,
	            PgMemoryContextAllocator<std::shared_ptr<PipelineFinalizeEvent>>>
	    finalize_events(fin_alloc);
	run_events.reserve(pipelines.size());
	combine_events.reserve(pipelines.size());
	finalize_events.reserve(pipelines.size());

	for (auto &p : pipelines)
	{
		auto run = AllocatePgShared<PipelineRunEvent>(p->id, p.get(), this);
		auto cmb = AllocatePgShared<PipelineCombineEvent>(p->id, p.get(), this);
		auto fin = AllocatePgShared<PipelineFinalizeEvent>(p->id, p.get(), this);

		cmb->AddDependency(run);
		fin->AddDependency(cmb);

		run_events.push_back(run);
		combine_events.push_back(cmb);
		finalize_events.push_back(fin);
	}

	for (size_t i = 0; i < pipelines.size(); i++)
	{
		for (PipelineId dep_pid : pipelines[i]->depends_on)
		{
			Assert(dep_pid < pipelines.size());
			run_events[i]->AddDependency(finalize_events[dep_pid]);
		}
	}

	auto publish = [&](const std::shared_ptr<Event> &ev) {
		EventId id = next_event_id_++;
		events_.Register(id, ev.get());
		events_owned_.push_back(ev);
	};

	for (size_t i = 0; i < pipelines.size(); i++)
	{
		publish(run_events[i]);
		publish(combine_events[i]);
		publish(finalize_events[i]);
	}
}

TaskSchedulerDsmLayout
TaskScheduler::EstimateDsmSize() const
{
	TaskSchedulerDsmLayout layout{};
	layout.control_bytes        = MAXALIGN(sizeof(PipelineSharedControl));
	layout.task_queue_bytes     = MAXALIGN(
		DsmTaskQueue::EstimateSize(sizing_.task_queue_capacity));
	layout.event_counters_bytes = MAXALIGN(
		(size_t) next_event_id_ * sizeof(pg_atomic_uint32));
	layout.total_bytes          = layout.control_bytes
	                            + layout.task_queue_bytes
	                            + layout.event_counters_bytes;
	return layout;
}

void
TaskScheduler::EnqueueTasks(Event &event)
{
	(void) event;
	/* 3g.2 will: assign per-event outstanding_tasks count, push N
	 * TaskDescriptors, SetLatch on each worker. Today: no-op. The 3c stub
	 * Schedule() impls still go straight to FinishEvent(), so DAG drains. */
}

}  /* namespace pipeline */
}  /* namespace pg_volvec */

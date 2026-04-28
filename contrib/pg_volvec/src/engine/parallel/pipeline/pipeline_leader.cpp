extern "C" {
#include "postgres.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/procarray.h"
#include "storage/shm_toc.h"
#include "utils/dsa.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "utils/wait_event.h"
}

#include <cstdio>
#include <cstring>
#include <memory>

#include "parallel/pipeline/dsm_control.hpp"
#include "parallel/pipeline/dsm_task_queue.hpp"
#include "parallel/pipeline/meta_pipeline.hpp"
#include "parallel/pipeline/output_sink.hpp"
#include "parallel/pipeline/physical_operator.hpp"
#include "parallel/pipeline/pipeline.hpp"
#include "parallel/pipeline/pipeline_descriptor.hpp"
#include "parallel/pipeline/pipeline_dsm_lookup.hpp"
#include "parallel/pipeline/pipeline_leader.hpp"
#include "parallel/pipeline/query_state.hpp"
#include "parallel/pipeline/runtime_dsm.hpp"
#include "parallel/pipeline/task.hpp"
#include "parallel/pipeline/task_scheduler.hpp"
#include "parallel/pipeline/types.hpp"

extern "C" {
extern int pg_volvec_parallel_max_workers;
extern bool pg_volvec_parallel_leader_participation;
}

namespace pg_volvec {
namespace pipeline {

namespace {

static constexpr uint32_t kTaskQueueCapacity = 64;

struct LeaderCleanupState {
	PipelineSharedControl *control = nullptr;
	PgVector<BackgroundWorkerHandle *> *handles = nullptr;
	PgVolVecQueryState *state = nullptr;
};

static void
WakeStartedWorkers(const PgVector<BackgroundWorkerHandle *> &handles)
{
	for (BackgroundWorkerHandle *handle : handles)
	{
		if (handle == nullptr)
			continue;

		pid_t pid = 0;
		if (GetBackgroundWorkerPid(handle, &pid) != BGWH_STARTED)
			continue;

		PGPROC *proc = BackendPidGetProc(pid);
		if (proc != nullptr)
			SetLatch(&proc->procLatch);
	}
}

static void
SignalShutdownAndWait(const LeaderCleanupState &cleanup)
{
	if (cleanup.control != nullptr)
		pg_atomic_write_u32(&cleanup.control->shutdown_requested, 1u);

	if (cleanup.handles != nullptr)
	{
		WakeStartedWorkers(*cleanup.handles);
		for (BackgroundWorkerHandle *handle : *cleanup.handles)
		{
			if (handle != nullptr)
				WaitForBackgroundWorkerShutdown(handle);
		}
	}

	if (cleanup.state != nullptr)
		DestroyRuntimeDsm(cleanup.state);
}

static void
DestroyLeaderMemoryContext(MemoryContext old_mcxt, MemoryContext leader_mcxt)
{
	MemoryContextSwitchTo(old_mcxt);
	if (leader_mcxt != nullptr)
		MemoryContextDelete(leader_mcxt);
}

static bool
FailEarly(const char **failure_reason,
		  const char *reason,
		  const LeaderCleanupState &cleanup,
		  MemoryContext old_mcxt,
		  MemoryContext leader_mcxt,
		  PgVolVecQueryState *state)
{
	if (failure_reason != nullptr)
		*failure_reason = reason;
	if (state != nullptr)
	{
		state->parallel_plan = nullptr;
		state->parallel_scheduler = nullptr;
	}
	SignalShutdownAndWait(cleanup);
	DestroyLeaderMemoryContext(old_mcxt, leader_mcxt);
	return false;
}

static bool
AllEventsFinished(TaskScheduler &scheduler)
{
	for (uint32_t id = 0; id < scheduler.event_count(); ++id)
	{
		Event *event = scheduler.event_lookup().Resolve(id);
		Assert(event != nullptr);
		EventState state = event->state();
		if (state != EventState::FINISHED && state != EventState::ABORTED)
			return false;
	}
	return true;
}

static void
RaiseWorkerFailure(PipelineSharedControl *control,
			   const PgVector<BackgroundWorkerHandle *> &handles,
			   PgVolVecQueryState *state)
{
	char errmsg_buf[PIPELINE_WORKER_ERROR_MSG_LEN];

	std::memcpy(errmsg_buf, control->worker_error_msg, sizeof(errmsg_buf));
	errmsg_buf[sizeof(errmsg_buf) - 1] = '\0';

	LeaderCleanupState cleanup{};
	cleanup.control = control;
	cleanup.handles = const_cast<PgVector<BackgroundWorkerHandle *> *>(&handles);
	cleanup.state = state;
	SignalShutdownAndWait(cleanup);

	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("pg_volvec worker reported failure: %s",
					errmsg_buf[0] != '\0' ? errmsg_buf : "(no message)")));
}

static TaskExecutionResult
ExecuteLeaderTask(TaskDescriptor desc,
			 Pipeline *pipeline,
			 WorkerTaskRuntime *leader_rt)
{
	std::unique_ptr<Task> task;

	switch (static_cast<TaskKind>(desc.kind))
	{
		case TaskKind::RUN:
			task = std::make_unique<PipelineRunTask>(desc.event_id,
								pipeline,
								leader_rt,
								desc.worker_index);
			break;
		case TaskKind::COMBINE:
			task = std::make_unique<PipelineCombineTask>(desc.event_id,
								pipeline,
								leader_rt,
								desc.worker_index);
			break;
		case TaskKind::FINALIZE:
			task = std::make_unique<PipelineFinalizeTask>(desc.event_id,
								pipeline,
								leader_rt,
								desc.worker_index);
			break;
	}

	Assert(task != nullptr);
	return task->Execute();
}

}  /* namespace */

bool
PgvolvecPipelineRun(QueryDesc *queryDesc,
					PgVolVecQueryState *state,
					const char **failure_reason)
{
	static uint32 wait_event_id = 0;

	if (failure_reason != nullptr)
		*failure_reason = nullptr;

	if (queryDesc == nullptr)
	{
		if (failure_reason != nullptr)
			*failure_reason = "pg_volvec: QueryDesc is null";
		return false;
	}
	if (state == nullptr)
	{
		if (failure_reason != nullptr)
			*failure_reason = "pg_volvec: query state is null";
		return false;
	}
	if (state->parallel_plan == nullptr)
	{
		if (failure_reason != nullptr)
			*failure_reason = "pg_volvec: parallel_plan is null";
		return false;
	}

	MemoryContext old_mcxt = CurrentMemoryContext;
	MemoryContext leader_mcxt = AllocSetContextCreate(CurrentMemoryContext,
									 "pg_volvec leader",
									 ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(leader_mcxt);

	LeaderCleanupState cleanup{};
	cleanup.state = state;

	auto *root = static_cast<PhysicalOperator *>(state->parallel_plan);
	state->parallel_plan = nullptr;

	std::unique_ptr<MetaPipelineBundle> bundle;
	PipelineSharedControl *control = nullptr;
	DsmTaskQueue *queue = nullptr;
	dsa_area *dsa = nullptr;
	shm_toc *toc = nullptr;
	WorkerTaskRuntime leader_rt{};
	PipelineDsmLookup<Pipeline> leader_lookup(leader_mcxt);
	PgVector<BackgroundWorkerHandle *> handles{
		PgMemoryContextAllocator<BackgroundWorkerHandle *>(leader_mcxt)};
	PgVector<Latch *> registered_latches{
		PgMemoryContextAllocator<Latch *>(leader_mcxt)};
	PgVector<char> event_scheduled{PgMemoryContextAllocator<char>(leader_mcxt)};
	PgVector<char> event_finished{PgMemoryContextAllocator<char>(leader_mcxt)};

	cleanup.handles = &handles;

	PG_TRY();
	{
		if (wait_event_id == 0)
			wait_event_id = WaitEventExtensionNew("pg_volvec leader");

		bundle = MetaPipeline::Build(std::unique_ptr<PhysicalOperator>(root));
		root = nullptr;
		if (bundle == nullptr)
		{
			return FailEarly(failure_reason,
				"pg_volvec: MetaPipeline::Build returned null",
				cleanup,
				old_mcxt,
				leader_mcxt,
				state);
		}
		if (bundle->pipelines.empty())
		{
			return FailEarly(failure_reason,
				"pg_volvec: BuildPipelines produced no pipelines",
				cleanup,
				old_mcxt,
				leader_mcxt,
				state);
		}

		for (size_t i = 0; i < bundle->pipelines.size(); ++i)
			Assert(bundle->pipelines[i]->id == static_cast<PipelineId>(i));

		if (state->runtime_dsm == nullptr || state->runtime_dsa == nullptr)
		{
			const char *err = nullptr;
			if (!CreateRuntimeDsm(state, &err))
			{
				return FailEarly(failure_reason,
					err != nullptr ? err : "pg_volvec: runtime DSM creation failed",
					cleanup,
					old_mcxt,
					leader_mcxt,
					state);
			}
		}

		toc = shm_toc_attach(PIPELINE_DSM_MAGIC,
						  dsm_segment_address(state->runtime_dsm));
		if (toc == nullptr)
		{
			return FailEarly(failure_reason,
				"pg_volvec: failed to attach runtime shm_toc",
				cleanup,
				old_mcxt,
				leader_mcxt,
				state);
		}

		control = static_cast<PipelineSharedControl *>(
			shm_toc_lookup(toc, PIPELINE_DSM_KEY_CONTROL, false));
		void *queue_buf = shm_toc_lookup(toc, PIPELINE_DSM_KEY_TASK_QUEUE, false);
		dsa = state->runtime_dsa;
		queue = DsmTaskQueue::AttachInPlace(queue_buf);

		cleanup.control = control;

		Assert(control != nullptr);
		Assert(dsa != nullptr);
		Assert(queue != nullptr);
		Assert(control->magic == PIPELINE_DSM_MAGIC);
		Assert(control->leader_pid == MyProcPid);

		control->pipelines_root = LeaderSerializePipelines(*bundle, dsa);
		control->num_pipelines = static_cast<int32>(bundle->pipelines.size());

		const bool leader_participate_requested =
			pg_volvec_parallel_leader_participation;
		const bool leader_participate = false;
		(void) leader_participate_requested;
		/* Per-current scheduler/task-descriptor contract, leader participation
		 * remains disabled until a dedicated LEADER_WORKER_INDEX task slot is
		 * published. */

		const int bgworker_count = pg_volvec_parallel_max_workers;
		if (bgworker_count <= 0)
		{
			return FailEarly(failure_reason,
				"pg_volvec: pg_volvec.parallel_max_workers must be >= 1",
				cleanup,
				old_mcxt,
				leader_mcxt,
				state);
		}

		TaskScheduler scheduler(leader_mcxt,
						std::move(bundle),
						TaskSchedulerSizing{static_cast<uint32_t>(bgworker_count),
										kTaskQueueCapacity});
		scheduler.BuildEvents();
		scheduler.BindRuntime(control, queue, dsa);
		scheduler.AllocateEventShmStates();

		/* Shared payload publication happens during descriptor serialization;
		 * there is no separate leader pre-bind virtual in the current operator
		 * base. */

		handles.reserve(static_cast<size_t>(bgworker_count));
		for (int worker_index = 0; worker_index < bgworker_count; ++worker_index)
		{
			BackgroundWorker bgw{};
			BackgroundWorkerHandle *handle = nullptr;

			bgw.bgw_flags = BGWORKER_SHMEM_ACCESS |
						BGWORKER_BACKEND_DATABASE_CONNECTION;
			bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
			bgw.bgw_restart_time = BGW_NEVER_RESTART;
			std::snprintf(bgw.bgw_library_name, BGW_MAXLEN, "pg_volvec");
			std::snprintf(bgw.bgw_function_name, BGW_MAXLEN,
					  "pg_volvec_pipeline_worker_main");
			std::snprintf(bgw.bgw_name, BGW_MAXLEN,
					  "pg_volvec worker %d", worker_index);
			std::snprintf(bgw.bgw_type, BGW_MAXLEN, "pg_volvec worker");
			bgw.bgw_main_arg = UInt32GetDatum(dsm_segment_handle(state->runtime_dsm));
			bgw.bgw_notify_pid = MyProcPid;
			std::memcpy(bgw.bgw_extra, &worker_index, sizeof(int32_t));

			if (!RegisterDynamicBackgroundWorker(&bgw, &handle))
			{
				return FailEarly(failure_reason,
					"pg_volvec: RegisterDynamicBackgroundWorker failed (worker slots exhausted?)",
					cleanup,
					old_mcxt,
					leader_mcxt,
					state);
			}

			handles.push_back(handle);
		}

		registered_latches.reserve(handles.size() + (leader_participate ? 1 : 0));

		auto *ready_array = static_cast<pg_atomic_uint32 *>(
			shm_toc_lookup(toc, PIPELINE_DSM_KEY_WORKER_READY, false));
		Assert(ready_array != nullptr);

		for (size_t worker_idx = 0; worker_idx < handles.size(); ++worker_idx)
		{
			BackgroundWorkerHandle *handle = handles[worker_idx];
			pid_t worker_pid = 0;
			BgwHandleStatus status = WaitForBackgroundWorkerStartup(handle, &worker_pid);

			if (status != BGWH_STARTED)
			{
				const char *reason = (status == BGWH_POSTMASTER_DIED)
					? "pg_volvec: postmaster died while starting pipeline worker"
					: "pg_volvec: pipeline worker exited before startup completed";
				return FailEarly(failure_reason,
					reason,
					cleanup,
					old_mcxt,
					leader_mcxt,
					state);
			}

			/*
			 * BGWH_STARTED means the postmaster has assigned slot->pid only;
			 * the worker may not yet have completed InitProcessPhase2, so its
			 * PGPROC may not yet be visible to BackendPidGetProc. Wait for the
			 * worker to publish its ready bit (set after
			 * BackgroundWorkerInitializeConnectionByOid returns). Detect early
			 * worker death via BGWH_STOPPED.
			 */
			while (pg_atomic_read_u32(&ready_array[worker_idx]) == 0)
			{
				CHECK_FOR_INTERRUPTS();

				pid_t poll_pid = 0;
				BgwHandleStatus poll_status = GetBackgroundWorkerPid(handle, &poll_pid);
				if (poll_status == BGWH_STOPPED)
				{
					return FailEarly(failure_reason,
						"pg_volvec: pipeline worker exited before reporting ready",
						cleanup,
						old_mcxt,
						leader_mcxt,
						state);
				}
				if (poll_status == BGWH_POSTMASTER_DIED)
				{
					return FailEarly(failure_reason,
						"pg_volvec: postmaster died while waiting for worker ready",
						cleanup,
						old_mcxt,
						leader_mcxt,
						state);
				}

				if (pg_atomic_read_u32(&ready_array[worker_idx]) != 0)
					break;

				int rc = WaitLatch(MyLatch,
								   WL_LATCH_SET | WL_EXIT_ON_PM_DEATH | WL_TIMEOUT,
								   1000,
								   wait_event_id);
				ResetLatch(MyLatch);
				if (rc & WL_POSTMASTER_DEATH)
				{
					return FailEarly(failure_reason,
						"pg_volvec: postmaster died while waiting for worker ready",
						cleanup,
						old_mcxt,
						leader_mcxt,
						state);
				}
			}

			PGPROC *proc = BackendPidGetProc(worker_pid);
			if (proc == nullptr)
			{
				return FailEarly(failure_reason,
					"pg_volvec: BackendPidGetProc returned null after worker ready",
					cleanup,
					old_mcxt,
					leader_mcxt,
					state);
			}

			registered_latches.push_back(&proc->procLatch);
		}

		if (leader_participate)
			registered_latches.push_back(MyLatch);

		queue->RegisterWorkerLatches(registered_latches.data(),
						 static_cast<uint32>(registered_latches.size()));

		leader_rt.exec_ctx = ExecCtx{leader_mcxt, dsa, LEADER_WORKER_INDEX};
		leader_rt.control = control;
		leader_rt.event_shm = static_cast<EventShmState *>(
			dsa_get_address(dsa, control->event_states_root));
		leader_rt.pipelines = &leader_lookup;
		leader_rt.leader_qd = queryDesc;
		leader_rt.final_output = nullptr;

		for (auto &pipeline_uptr : scheduler.bundle().pipelines)
		{
			leader_lookup.Register(static_cast<uint32_t>(pipeline_uptr->id),
						      pipeline_uptr.get());
			if (pipeline_uptr->sink != nullptr &&
				pipeline_uptr->sink->type() == PhysicalOperatorType::OUTPUT)
			{
				leader_rt.final_output = static_cast<OutputSink *>(pipeline_uptr->sink);
			}
		}

		event_scheduled.assign(scheduler.event_count(), 0);
		event_finished.assign(scheduler.event_count(), 0);

		for (auto &pipeline_uptr : scheduler.bundle().pipelines)
		{
			if (!pipeline_uptr->depends_on.empty())
				continue;

			EventId event_id = static_cast<EventId>(pipeline_uptr->id) * 3u;
			Event *event = scheduler.event_lookup().Resolve(event_id);
			Assert(event != nullptr);
			event->Schedule();
			event_scheduled[event_id] = 1;
		}

		for (;;)
		{
			CHECK_FOR_INTERRUPTS();

			if (pg_atomic_read_u32(&control->worker_error) != 0)
				RaiseWorkerFailure(control, handles, state);

			bool progress = false;

			for (uint32_t event_id = 0; event_id < scheduler.event_count(); ++event_id)
			{
				Event *event = scheduler.event_lookup().Resolve(event_id);
				Assert(event != nullptr);

				if (!event_scheduled[event_id] && event->state() == EventState::SCHEDULED)
					event_scheduled[event_id] = 1;

				if (!event_finished[event_id] && event->state() == EventState::ABORTED)
				{
					event_finished[event_id] = 1;
					progress = true;
					continue;
				}

				if ((event_id % 3u) == 2u &&
					event_scheduled[event_id] &&
					!event_finished[event_id] &&
					event->state() == EventState::SCHEDULED &&
					pg_atomic_read_u32(&leader_rt.event_shm[event_id].tasks_remaining) > 0)
				{
					Pipeline *pipeline = leader_lookup.Resolve(event->pipeline_id());
					Assert(pipeline != nullptr);

					TaskDescriptor desc{};
					desc.pipeline_id = static_cast<uint32_t>(pipeline->id);
					desc.event_id = event_id;
					desc.worker_index = LEADER_WORKER_INDEX;
					desc.kind = static_cast<uint8_t>(TaskKind::FINALIZE);

					TaskExecutionResult exec_result = ExecuteLeaderTask(desc,
										   pipeline,
										   &leader_rt);
					if (exec_result != TaskExecutionResult::TASK_FINISHED)
					{
						ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("pg_volvec: leader finalize task did not finish")));
					}

					pg_atomic_write_u32(&leader_rt.event_shm[event_id].tasks_remaining, 0);
					event->FinishEvent();
					event_finished[event_id] = 1;
					progress = true;
					continue;
				}

				if (event_scheduled[event_id] &&
					!event_finished[event_id] &&
					pg_atomic_read_u32(&leader_rt.event_shm[event_id].tasks_remaining) == 0)
				{
					event->FinishEvent();
					event_finished[event_id] = 1;
					progress = true;
				}
			}

			if (progress)
				continue;

			if (AllEventsFinished(scheduler))
				break;

			int rc = WaitLatch(MyLatch,
						   WL_LATCH_SET | WL_EXIT_ON_PM_DEATH | WL_TIMEOUT,
						   1000,
						   wait_event_id);
			ResetLatch(MyLatch);
			if (rc & WL_POSTMASTER_DEATH)
			{
				cleanup.control = control;
				SignalShutdownAndWait(cleanup);
				DestroyLeaderMemoryContext(old_mcxt, leader_mcxt);
				proc_exit(1);
			}
		}

		for (BackgroundWorkerHandle *handle : handles)
			WaitForBackgroundWorkerShutdown(handle);

		if (leader_rt.final_output != nullptr)
			leader_rt.final_output->EmitGlobalTdcToDest(leader_rt.exec_ctx);

		if (pg_atomic_read_u32(&control->worker_error) != 0)
			RaiseWorkerFailure(control, handles, state);

		DestroyRuntimeDsm(state);
		state->parallel_scheduler = nullptr;
		DestroyLeaderMemoryContext(old_mcxt, leader_mcxt);
		return true;
	}
	PG_CATCH();
	{
		state->parallel_plan = nullptr;
		state->parallel_scheduler = nullptr;
		SignalShutdownAndWait(cleanup);
		DestroyLeaderMemoryContext(old_mcxt, leader_mcxt);
		PG_RE_THROW();
	}
	PG_END_TRY();
}

}  /* namespace pipeline */
}  /* namespace pg_volvec */

extern "C" {
#include "postgres.h"

#include "miscadmin.h"
#include "storage/dsm.h"
#include "storage/lwlock.h"
#include "storage/shm_toc.h"
#include "utils/dsa.h"
}

#include <cstring>
#include <new>

#include "parallel/pipeline/runtime_dsm.hpp"
#include "parallel/pipeline/dsm_control.hpp"
#include "parallel/pipeline/dsm_task_queue.hpp"

namespace pg_volvec {
namespace pipeline {

static constexpr uint32_t kRuntimeTaskQueueCapacity = 64;
static constexpr size_t   kRuntimeDsaSizeBytes      = 256u * 1024u * 1024u;
static constexpr int      kRuntimeTocNumKeys        = 3;
static constexpr const char *kRuntimeDsaTrancheName = "pg_volvec_runtime_dsa";

bool
CreateRuntimeDsm(PgVolVecQueryState *state, const char **error_out)
{
	Assert(state != nullptr);
	Assert(state->runtime_dsm == nullptr);
	Assert(state->runtime_dsa == nullptr);

	shm_toc_estimator estimator;
	shm_toc_initialize_estimator(&estimator);
	shm_toc_estimate_chunk(&estimator, sizeof(PipelineSharedControl));
	shm_toc_estimate_chunk(&estimator, kRuntimeDsaSizeBytes);
	shm_toc_estimate_chunk(&estimator,
						   DsmTaskQueue::EstimateSize(kRuntimeTaskQueueCapacity));
	shm_toc_estimate_keys(&estimator, kRuntimeTocNumKeys);
	const Size segsize = shm_toc_estimate(&estimator);

	dsm_segment *seg = dsm_create(segsize, DSM_CREATE_NULL_IF_MAXSEGMENTS);
	if (seg == nullptr)
	{
		if (error_out != nullptr)
			*error_out = "pg_volvec: dsm_create returned NULL (max DSM segments exhausted)";
		return false;
	}

	shm_toc *toc = shm_toc_create(PIPELINE_DSM_MAGIC,
								  dsm_segment_address(seg),
								  segsize);

	void *control_mem = shm_toc_allocate(toc, sizeof(PipelineSharedControl));
	auto *control = new (control_mem) PipelineSharedControl();
	control->magic = PIPELINE_DSM_MAGIC;
	control->num_pipelines = 0;
	pg_atomic_init_u32(&control->worker_error, 0);
	control->pipelines_root = InvalidDsaPointer;
	control->leader_pid = MyProcPid;
	pg_atomic_init_u32(&control->shutdown_requested, 0);
	std::memset(control->worker_error_msg, 0, sizeof(control->worker_error_msg));
	control->event_states_root = InvalidDsaPointer;
	control->event_count = 0;
	control->db_oid = MyDatabaseId;
	shm_toc_insert(toc, PIPELINE_DSM_KEY_CONTROL, control);

	void *dsa_place = shm_toc_allocate(toc, kRuntimeDsaSizeBytes);
	int tranche_id = LWLockNewTrancheId(kRuntimeDsaTrancheName);
	dsa_area *area = dsa_create_in_place(dsa_place,
										 kRuntimeDsaSizeBytes,
										 tranche_id,
										 seg);
	dsa_pin_mapping(area);
	shm_toc_insert(toc, PIPELINE_DSM_KEY_DSA, dsa_place);

	void *queue_mem = shm_toc_allocate(toc,
									   DsmTaskQueue::EstimateSize(kRuntimeTaskQueueCapacity));
	(void) DsmTaskQueue::InitInPlace(queue_mem, kRuntimeTaskQueueCapacity);
	shm_toc_insert(toc, PIPELINE_DSM_KEY_TASK_QUEUE, queue_mem);

	state->runtime_dsm = seg;
	state->runtime_dsa = area;
	return true;
}

void
DestroyRuntimeDsm(PgVolVecQueryState *state)
{
	if (state == nullptr)
		return;
	if (state->runtime_dsa != nullptr)
	{
		dsa_detach(state->runtime_dsa);
		state->runtime_dsa = nullptr;
	}
	if (state->runtime_dsm != nullptr)
	{
		dsm_detach(state->runtime_dsm);
		state->runtime_dsm = nullptr;
	}
}

}
}

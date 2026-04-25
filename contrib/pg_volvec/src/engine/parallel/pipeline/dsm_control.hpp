#pragma once

extern "C" {
#include "postgres.h"
#include "port/atomics.h"
#include "utils/dsa.h"
}

namespace pg_volvec {
namespace pipeline {

/*
 * shm_toc keys for the MetaPipeline runtime DSM segment.
 *
 * Per docs/GLOBAL_LOCAL_STATE_DESIGN.md §8.5.2 / §8.6 (HEAD eb7901b022a),
 * the segment publishes EXACTLY THREE keys -- no PlannedStmt, no query text,
 * no partial fileset, no param-exec, no per-source ParallelTableScanDesc.
 * Workers reconstruct PhysicalOperator instances from the DSA-resident
 * OpDescriptor[] reachable via PipelineSharedControl::pipelines_root.
 *
 * Keys remain in the 0xD800000000000000 high-bit range to make accidental
 * cross-attach to a stale (pre-greenfield) DSM segment impossible. Old key
 * IDs 0x...0002..0007 are intentionally retired and MUST NOT be re-used.
 */
static constexpr uint64 PIPELINE_DSM_KEY_CONTROL    = UINT64CONST(0xD800000000000001);
static constexpr uint64 PIPELINE_DSM_KEY_DSA        = UINT64CONST(0xD800000000000008);
static constexpr uint64 PIPELINE_DSM_KEY_TASK_QUEUE = UINT64CONST(0xD800000000000009);

static constexpr uint32 PIPELINE_DSM_MAGIC = 0x56505043;

/*
 * Per-query control block published at PIPELINE_DSM_KEY_CONTROL.
 *
 * Plan-shape-agnostic. Workers attach, validate magic, then walk the
 * DSA-resident PipelineDescriptor[] rooted at pipelines_root to reconstruct
 * the operator graph. See §8.5.4.2 (POD layout) and §8.5.4.4 (worker
 * reconstruction) for the IR contract.
 */
struct PipelineSharedControl
{
	uint32           magic;             /* == PIPELINE_DSM_MAGIC */
	int32            num_pipelines;     /* length of PipelineDescriptor[] at pipelines_root */
	pg_atomic_uint32 worker_error;      /* set by any worker on ERROR (§8.5.2 worker contract) */
	dsa_pointer      pipelines_root;    /* DSA pointer to PipelineDescriptor[num_pipelines] */
};

}  /* namespace pipeline */
}  /* namespace pg_volvec */

#pragma once

#include <cstdint>

extern "C" {
#include "postgres.h"
#include "port/atomics.h"
}

namespace pg_volvec {
namespace pipeline {

/* shm_toc keys -- distinct from legacy VOLVEC_PARALLEL_KEY_* range. */
static constexpr uint64 PIPELINE_DSM_KEY_CONTROL        = UINT64CONST(0xD800000000000001);
static constexpr uint64 PIPELINE_DSM_KEY_PLANNEDSTMT    = UINT64CONST(0xD800000000000002);
static constexpr uint64 PIPELINE_DSM_KEY_QUERY_TEXT     = UINT64CONST(0xD800000000000003);
static constexpr uint64 PIPELINE_DSM_KEY_PARTIALS       = UINT64CONST(0xD800000000000004);
static constexpr uint64 PIPELINE_DSM_KEY_SOURCE_PSCAN   = UINT64CONST(0xD800000000000005);
static constexpr uint64 PIPELINE_DSM_KEY_PARTIAL_FILESET = UINT64CONST(0xD800000000000006);
static constexpr uint64 PIPELINE_DSM_KEY_PARAM_EXEC     = UINT64CONST(0xD800000000000007);
static constexpr uint64 PIPELINE_DSM_KEY_DSA            = UINT64CONST(0xD800000000000008);

static constexpr uint32 PIPELINE_DSM_MAGIC = 0x56505043;

/*
 * Greenfield Q1+Q6 shape: SeqScan -> [Filter] -> PartialAgg -> AggSink.
 * Single source node, single agg node, no DAG. P3 replaces with full scheduler.
 */
struct PipelineSharedControl
{
	uint32           magic;
	uint32           partial_slot_count;        /* == launched worker count */
	uint32           morsel_nblocks;
	uint32           total_blocks;
	Oid              source_scan_relid;
	int              source_scan_plan_node_id;
	int              agg_plan_node_id;
	pg_atomic_uint64 next_block;
	pg_atomic_uint32 worker_error;              /* set by any worker on ERROR */
};

}  /* namespace pipeline */
}  /* namespace pg_volvec */

#include "parallel/pipeline/pipeline_profile.hpp"

extern "C" {
#include "utils/dsa.h"
#include "utils/elog.h"
}

#include <algorithm>
#include <cstring>

#include "parallel/pipeline/dsm_control.hpp"
#include "parallel/pipeline/physical_operator.hpp"
#include "parallel/pipeline/types.hpp"

extern "C" {
extern bool pg_volvec_profile;
}

namespace pg_volvec {
namespace pipeline {

namespace {

static constexpr uint32 kStageCount = static_cast<uint32>(PipelineProfileStage::COUNT);

uint32
ProfileWorkerSlot(int worker_index)
{
	return worker_index == LEADER_WORKER_INDEX ? 0u : static_cast<uint32>(worker_index + 1);
}

const char *
StageName(PipelineProfileStage stage)
{
	switch (stage)
	{
		case PipelineProfileStage::TOTAL: return "total";
		case PipelineProfileStage::TASK_RUN_TOTAL: return "task_run_total";
		case PipelineProfileStage::TASK_COMBINE_TOTAL: return "task_combine_total";
		case PipelineProfileStage::TASK_FINALIZE_TOTAL: return "task_finalize_total";
		case PipelineProfileStage::SOURCE_SEQ_SCAN: return "source_seq_scan";
		case PipelineProfileStage::SOURCE_HASH_AGG: return "source_hashagg_readback";
		case PipelineProfileStage::SOURCE_ORDER: return "source_order_readback";
		case PipelineProfileStage::OP_PROJECTION: return "project_expr";
		case PipelineProfileStage::SINK_HASH_AGG_UPDATE: return "agg_update_local";
		case PipelineProfileStage::SINK_ORDER_APPEND: return "order_sink_append";
		case PipelineProfileStage::SINK_OUTPUT_APPEND: return "output_sink_append";
		case PipelineProfileStage::COMBINE_HASH_AGG: return "agg_combine_global";
		case PipelineProfileStage::FINALIZE_HASH_AGG: return "agg_finalize_global";
		case PipelineProfileStage::FINALIZE_ORDER: return "order_finalize_sort";
		case PipelineProfileStage::FINALIZE_OUTPUT: return "output_finalize";
		case PipelineProfileStage::OUTPUT_EMIT: return "output_emit_to_dest";
		case PipelineProfileStage::SCAN_BLOCK_FETCH: return "scan_block_fetch";
		case PipelineProfileStage::SCAN_QUAL_DEFORM: return "scan_qual_deform";
		case PipelineProfileStage::SCAN_FILTER: return "scan_filter";
		case PipelineProfileStage::SCAN_PROJ_DEFORM: return "scan_proj_deform";
		case PipelineProfileStage::SCAN_LOAD_PAGE: return "scan_load_page";
		case PipelineProfileStage::SCAN_PREPARE_PAGE: return "scan_prepare_pagescan";
		case PipelineProfileStage::SCAN_VISIBLE_TUPLE: return "scan_visible_tuple_iter";
		case PipelineProfileStage::LEADER_WAIT_READY: return "leader_wait_worker_ready";
		case PipelineProfileStage::LEADER_WAIT_EVENT: return "leader_wait_event";
		case PipelineProfileStage::WORKER_WAIT_TASK: return "worker_wait_task";
		case PipelineProfileStage::COUNT: break;
	}
	return "unknown";
}

PipelineProfileRecord *
ProfileRecords(PipelineSharedControl *control, dsa_area *dsa)
{
	if (control == nullptr || dsa == nullptr ||
		!DsaPointerIsValid(control->profile_records_root))
		return nullptr;
	return static_cast<PipelineProfileRecord *>(
		dsa_get_address(dsa, control->profile_records_root));
}

PipelineProfileRecord *
ProfileRecord(PipelineSharedControl *control,
			  dsa_area *dsa,
			  int worker_index,
			  EventId event_id,
			  PipelineProfileStage stage)
{
	PipelineProfileRecord *records = ProfileRecords(control, dsa);
	if (records == nullptr || event_id >= control->profile_event_count)
		return nullptr;

	const uint32 slot = ProfileWorkerSlot(worker_index);
	if (slot >= control->profile_worker_slots)
		return nullptr;

	const uint32 stage_idx = static_cast<uint32>(stage);
	if (stage_idx >= kStageCount)
		return nullptr;

	const uint64 idx =
		((uint64) slot * control->profile_event_count + event_id) * kStageCount + stage_idx;
	return &records[idx];
}

}  /* namespace */

bool
PipelineProfileAllocate(PipelineSharedControl *control,
						 dsa_area *dsa,
						 uint32 event_count,
						 uint32 num_workers)
{
	if (control == nullptr || dsa == nullptr)
		return false;

	control->profile_records_root = InvalidDsaPointer;
	control->profile_event_count = event_count;
	control->profile_worker_slots = num_workers + 1u;
	pg_atomic_write_u32(&control->profile_enabled, 0u);

	if (!pg_volvec_profile || event_count == 0)
		return true;

	const Size nrecords = static_cast<Size>(control->profile_worker_slots) *
		static_cast<Size>(event_count) * static_cast<Size>(kStageCount);
	const Size bytes = nrecords * sizeof(PipelineProfileRecord);
	control->profile_records_root = dsa_allocate0(dsa, bytes);
	pg_atomic_write_u32(&control->profile_enabled, 1u);
	return true;
}

bool
PipelineProfileEnabled(const ExecCtx &ctx)
{
	return ctx.control != nullptr &&
		pg_atomic_read_u32(&ctx.control->profile_enabled) != 0;
}

void
PipelineProfileAddElapsed(PipelineSharedControl *control,
						   dsa_area *dsa,
						   int worker_index,
						   EventId event_id,
						   PipelineProfileStage stage,
						   instr_time elapsed,
						   uint64 rows)
{
	if (control == nullptr || pg_atomic_read_u32(&control->profile_enabled) == 0)
		return;

	PipelineProfileRecord *record = ProfileRecord(control, dsa, worker_index, event_id, stage);
	if (record == nullptr)
		return;

	record->elapsed_ns += (uint64) INSTR_TIME_GET_NANOSEC(elapsed);
	record->calls += 1;
	record->rows += rows;
}

void
PipelineProfileAddDiff(const ExecCtx &ctx,
						PipelineProfileStage stage,
						instr_time end,
						instr_time start,
						uint64 rows)
{
	instr_time elapsed = end;
	INSTR_TIME_SUBTRACT(elapsed, start);
	PipelineProfileAddElapsed(ctx.control,
						  ctx.dsa,
						  ctx.worker_index,
						  ctx.profile_event_id,
						  stage,
						  elapsed,
						  rows);
}

void
PipelineProfileReport(PipelineSharedControl *control, dsa_area *dsa)
{
	if (control == nullptr || dsa == nullptr ||
		pg_atomic_read_u32(&control->profile_enabled) == 0)
		return;

	PipelineProfileRecord *records = ProfileRecords(control, dsa);
	if (records == nullptr)
		return;

	elog(NOTICE,
		 "pg_volvec[timing] workers=%u worker_slots=%u events=%u stages=%u",
		 control->profile_worker_slots > 0 ? control->profile_worker_slots - 1 : 0,
		 control->profile_worker_slots,
		 control->profile_event_count,
		 kStageCount);

	for (uint32 stage_idx = 0; stage_idx < kStageCount; ++stage_idx)
	{
		uint64 sum_ns = 0;
		uint64 max_slot_ns = 0;
		uint64 calls = 0;
		uint64 rows = 0;

		for (uint32 slot = 0; slot < control->profile_worker_slots; ++slot)
		{
			uint64 slot_ns = 0;
			for (uint32 event_id = 0; event_id < control->profile_event_count; ++event_id)
			{
				const uint64 idx =
					((uint64) slot * control->profile_event_count + event_id) * kStageCount + stage_idx;
				const PipelineProfileRecord &record = records[idx];
				slot_ns += record.elapsed_ns;
				sum_ns += record.elapsed_ns;
				calls += record.calls;
				rows += record.rows;
			}
			max_slot_ns = std::max(max_slot_ns, slot_ns);
		}

		if (calls == 0 && sum_ns == 0)
			continue;

		elog(NOTICE,
			 "pg_volvec[timing] stage=%s sum_ms=%.3f max_slot_ms=%.3f calls=" UINT64_FORMAT " rows=" UINT64_FORMAT,
			 StageName(static_cast<PipelineProfileStage>(stage_idx)),
			 (double) sum_ns / 1000000.0,
			 (double) max_slot_ns / 1000000.0,
			 calls,
			 rows);
	}
}

PipelineProfileStage
PipelineProfileSourceStage(PhysicalOperatorType type)
{
	switch (type)
	{
		case PhysicalOperatorType::SEQ_SCAN: return PipelineProfileStage::SOURCE_SEQ_SCAN;
		case PhysicalOperatorType::HASH_AGGREGATE: return PipelineProfileStage::SOURCE_HASH_AGG;
		case PhysicalOperatorType::ORDER: return PipelineProfileStage::SOURCE_ORDER;
		default: return PipelineProfileStage::TASK_RUN_TOTAL;
	}
}

PipelineProfileStage
PipelineProfileOperatorStage(PhysicalOperatorType type)
{
	return type == PhysicalOperatorType::PROJECTION
		? PipelineProfileStage::OP_PROJECTION
		: PipelineProfileStage::TASK_RUN_TOTAL;
}

PipelineProfileStage
PipelineProfileSinkStage(PhysicalOperatorType type)
{
	switch (type)
	{
		case PhysicalOperatorType::HASH_AGGREGATE: return PipelineProfileStage::SINK_HASH_AGG_UPDATE;
		case PhysicalOperatorType::ORDER: return PipelineProfileStage::SINK_ORDER_APPEND;
		case PhysicalOperatorType::OUTPUT: return PipelineProfileStage::SINK_OUTPUT_APPEND;
		default: return PipelineProfileStage::TASK_RUN_TOTAL;
	}
}

PipelineProfileStage
PipelineProfileCombineStage(PhysicalOperatorType type)
{
	return type == PhysicalOperatorType::HASH_AGGREGATE
		? PipelineProfileStage::COMBINE_HASH_AGG
		: PipelineProfileStage::TASK_COMBINE_TOTAL;
}

PipelineProfileStage
PipelineProfileFinalizeStage(PhysicalOperatorType type)
{
	switch (type)
	{
		case PhysicalOperatorType::HASH_AGGREGATE: return PipelineProfileStage::FINALIZE_HASH_AGG;
		case PhysicalOperatorType::ORDER: return PipelineProfileStage::FINALIZE_ORDER;
		case PhysicalOperatorType::OUTPUT: return PipelineProfileStage::FINALIZE_OUTPUT;
		default: return PipelineProfileStage::TASK_FINALIZE_TOTAL;
	}
}

}  /* namespace pipeline */
}  /* namespace pg_volvec */

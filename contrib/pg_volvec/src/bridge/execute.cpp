extern "C" {
#include "postgres.h"
#include "executor/executor.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "catalog/pg_type.h"
#include "nodes/nodeFuncs.h"
}

#include "execute.h"
#include "volvec_engine.hpp"
#include "parallel/pipeline/pipeline_leader.hpp"

#include <string>

extern "C" {
extern bool pg_volvec_trace_hooks;
extern bool pg_volvec_trace_execution_path;
extern bool pg_volvec_parallel;
}

extern "C" {

/*
 * Opaque non-null sentinels that satisfy the admission gate in
 * pg_volvec_execute_query(). The DuckDB-style pipeline runtime in
 * pg_volvec::pipeline::PgvolvecPipelineRun() lowers from VecPlanState on
 * demand, so the bridge only needs a truthy marker that parallel is enabled.
 */
static char pgvolvec_parallel_plan_sentinel;
static char pgvolvec_parallel_scheduler_sentinel;

bool pg_volvec_initialize_plan(QueryDesc *queryDesc, pg_volvec::PgVolVecQueryState *state_ptr)
{
	MemoryContext old_context = MemoryContextSwitchTo(state_ptr->context);
	Plan *root_plan = queryDesc != nullptr && queryDesc->plannedstmt != nullptr ?
		queryDesc->plannedstmt->planTree : nullptr;

	if (pg_volvec_trace_hooks)
		elog(LOG,
			 "pg_volvec: initialize_plan root_nodeTag=%d parallelModeNeeded=%s operation=%d",
			 root_plan != nullptr ? (int) nodeTag(root_plan) : -1,
			 (queryDesc != nullptr && queryDesc->plannedstmt != nullptr &&
			  queryDesc->plannedstmt->parallelModeNeeded) ? "on" : "off",
			 queryDesc != nullptr ? (int) queryDesc->operation : -1);

	state_ptr->vec_plan = pg_volvec::ExecInitVecPlan(queryDesc->plannedstmt->planTree, queryDesc->estate).release();
	state_ptr->parallel_plan = nullptr;
	state_ptr->parallel_scheduler = nullptr;

	if (pg_volvec_parallel && state_ptr->vec_plan != nullptr)
	{
		state_ptr->parallel_plan = &pgvolvec_parallel_plan_sentinel;
		state_ptr->parallel_scheduler = &pgvolvec_parallel_scheduler_sentinel;
	}

	MemoryContextSwitchTo(old_context);

	if (state_ptr->vec_plan == nullptr)
		elog(WARNING,
			 "pg_volvec: plan initialization returned null, falling back to PostgreSQL executor");
	else if (!pg_volvec_parallel)
		elog(WARNING, "pg_volvec: pg_volvec.parallel=off, running volvec serially");
	else
		elog(WARNING, "pg_volvec: pipeline admission enabled, running parallel");

	return state_ptr->vec_plan != nullptr;
}

void pg_volvec_delete_plan(pg_volvec::PgVolVecQueryState *state_ptr)
{
	state_ptr->parallel_scheduler = nullptr;
	state_ptr->parallel_plan = nullptr;
	if (state_ptr->vec_plan) {
		delete state_ptr->vec_plan;
		state_ptr->vec_plan = nullptr;
	}
}

}

static Datum
int64_scaled_to_numeric(int64_t val, int scale)
{
	if (scale <= 0)
		return NumericGetDatum(int64_to_numeric(val));
	return NumericGetDatum(int64_div_fast_to_numeric(val, scale));
}

static Datum
scaled_avg_to_numeric(int64_t scaled_sum, int scale, int64_t count)
{
	Numeric sum_numeric;
	Numeric count_numeric;

	if (count <= 0)
		return NumericGetDatum(int64_to_numeric(0));

	sum_numeric = (scale <= 0) ? int64_to_numeric(scaled_sum)
							   : int64_div_fast_to_numeric(scaled_sum, scale);
	count_numeric = int64_to_numeric(count);
	return NumericGetDatum(numeric_div_safe(sum_numeric, count_numeric, nullptr));
}

static Datum
vec_stringref_to_text_datum(const pg_volvec::DataChunk<pg_volvec::DEFAULT_CHUNK_SIZE> *batch,
							const pg_volvec::VecStringRef &ref)
{
	if (ref.len == 0)
		return PointerGetDatum(cstring_to_text_with_len("", 0));
	if (ref.len > 65536)
		elog(ERROR,
			 "pg_volvec suspicious output string reference length (len=%u offset=%u prefix=%llu arena=%zu)",
			 ref.len,
			 ref.offset,
			 (unsigned long long) ref.prefix,
			 batch != nullptr ? batch->string_arena.size() : 0);

	if (pg_volvec::VecStringRefIsInline(ref))
	{
		char inline_buf[8];
		memcpy(inline_buf, &ref.prefix, ref.len);
		return PointerGetDatum(cstring_to_text_with_len(inline_buf, ref.len));
	}

	if (ref.offset == pg_volvec::kVecStringInlineOffset)
		elog(ERROR, "pg_volvec invalid inline string reference with length %u", ref.len);

	if (batch == nullptr || ref.offset > batch->string_arena.size() ||
		ref.len > batch->string_arena.size() - ref.offset)
		elog(ERROR, "pg_volvec invalid string arena reference (offset=%u len=%u arena=%zu)",
			 ref.offset, ref.len, batch ? batch->string_arena.size() : 0);

	return PointerGetDatum(cstring_to_text_with_len(batch->string_arena.data() + ref.offset, ref.len));
}

extern "C" {

bool pg_volvec_execute_query(QueryDesc *queryDesc, pg_volvec::PgVolVecQueryState *state_ptr,
								ScanDirection direction, uint64 count)
{
	if (!state_ptr || !state_ptr->vec_plan)
		return false;

	uint64 processed = 0;
	bool send_tuples = (queryDesc->operation == CMD_SELECT || queryDesc->plannedstmt->hasReturning);
	const char *execution_path = "volvec_serial";
	const char *execution_detail = "no_parallel_scheduler";

	if (state_ptr->parallel_plan != nullptr && state_ptr->parallel_scheduler != nullptr)
	{
		const char *parallel_failure_reason = nullptr;
		if (pg_volvec::pipeline::PgvolvecPipelineRun(queryDesc, state_ptr, &parallel_failure_reason))
		{
			execution_path = "pipeline";
			execution_detail = "duckdb_style_pipeline";
		}
		else
		{
			if (pg_volvec_trace_hooks)
				elog(LOG,
					 "pg_volvec: pipeline run skipped (%s), falling back to native pg",
					 parallel_failure_reason != nullptr ? parallel_failure_reason : "no reason recorded");
			if (pg_volvec_trace_execution_path)
				elog(LOG,
					 "pg_volvec_path: path=native_pg reason=pipeline_skipped detail=%s",
					 parallel_failure_reason != nullptr ? parallel_failure_reason : "no_reason");
			return false;
		}
	}

	pg_volvec::DataChunk<pg_volvec::DEFAULT_CHUNK_SIZE> *batch = new pg_volvec::DataChunk<pg_volvec::DEFAULT_CHUNK_SIZE>();
	TupleTableSlot *slot = ExecAllocTableSlot(&queryDesc->estate->es_tupleTable, queryDesc->tupDesc, &TTSOpsVirtual);
	if (!slot) { delete batch; return false; }

	queryDesc->estate->es_processed = 0;
	if (send_tuples && queryDesc->dest && queryDesc->dest->rStartup)
		queryDesc->dest->rStartup(queryDesc->dest, queryDesc->operation, queryDesc->tupDesc);

	while (state_ptr->vec_plan->get_next_batch(*batch)) {
		int n = batch->has_selection ? batch->sel.count : batch->count;
		for (int s = 0; s < n; s++) {
			int i = batch->has_selection ? batch->sel.row_ids[s] : s;
			ExecClearTuple(slot);
				for (int j = 0; j < slot->tts_tupleDescriptor->natts && j < 16; j++) {
					Oid typid = TupleDescAttr(slot->tts_tupleDescriptor, j)->atttypid;
					pg_volvec::VecOutputColMeta col_meta;
					bool has_meta = state_ptr->vec_plan->lookup_output_col_meta(j + 1, &col_meta);
					if (batch->nulls[j][i]) {
						slot->tts_isnull[j] = true;
						slot->tts_values[j] = (Datum) 0;
					} else {
						slot->tts_isnull[j] = false;
							if (typid == FLOAT8OID) {
								slot->tts_values[j] = Float8GetDatum(batch->double_columns[j][i]);
							} else if (typid == NUMERICOID) {
								double fval = batch->double_columns[j][i];
								int64_t ival = batch->int64_columns[j][i];

								if (has_meta && col_meta.storage_kind == pg_volvec::VecOutputStorageKind::Double)
									slot->tts_values[j] = DirectFunctionCall1(float8_numeric, Float8GetDatum(fval));
							else if (has_meta && col_meta.storage_kind == pg_volvec::VecOutputStorageKind::NumericScaledInt64)
								slot->tts_values[j] = int64_scaled_to_numeric(ival, col_meta.scale);
							else if (has_meta && col_meta.storage_kind == pg_volvec::VecOutputStorageKind::NumericAvgPair)
								slot->tts_values[j] = scaled_avg_to_numeric(ival, col_meta.scale, (int64_t) batch->double_columns[j][i]);
							else if (ival == 0 && fval != 0.0)
								slot->tts_values[j] = DirectFunctionCall1(float8_numeric, Float8GetDatum(fval));
							else
								slot->tts_values[j] = int64_scaled_to_numeric(ival, pg_volvec::DEFAULT_NUMERIC_SCALE);
							} else if (typid == INT8OID) {
								slot->tts_values[j] = Int64GetDatum(batch->int64_columns[j][i]);
							} else if (typid == BPCHAROID || typid == TEXTOID || typid == VARCHAROID) {
								const pg_volvec::VecStringRef &ref = batch->string_columns[j][i];
								slot->tts_values[j] = vec_stringref_to_text_datum(batch, ref);
							} else {
							slot->tts_values[j] = Int32GetDatum(batch->int32_columns[j][i]);
							}
				}
			}
			ExecStoreVirtualTuple(slot);
			if (queryDesc->dest && queryDesc->dest->receiveSlot)
				queryDesc->dest->receiveSlot(slot, queryDesc->dest);
			processed++;
			if (count != 0 && processed >= count)
				break;
		}
		if (count != 0 && processed >= count)
			break;
	}

	queryDesc->estate->es_processed = processed;
	queryDesc->estate->es_total_processed += processed;
	if (send_tuples && queryDesc->dest && queryDesc->dest->rShutdown)
		queryDesc->dest->rShutdown(queryDesc->dest);
	delete batch;
	if (pg_volvec_trace_execution_path)
		elog(LOG,
			 "pg_volvec_path: path=%s detail=%s rows=%llu parallel_plan=%s scheduler=%s",
			 execution_path,
			 execution_detail != nullptr ? execution_detail : "none",
			 (unsigned long long) processed,
			 state_ptr->parallel_plan != nullptr ? "yes" : "no",
			 state_ptr->parallel_scheduler != nullptr ? "yes" : "no");
	return true;
}

}

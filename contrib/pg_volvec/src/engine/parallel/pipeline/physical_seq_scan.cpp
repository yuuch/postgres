#include "parallel/pipeline/physical_seq_scan.hpp"

extern "C" {
#include "access/heapam.h"
#include "access/tableam.h"
#include "catalog/pg_type_d.h"
#include "executor/tuptable.h"
#include "fmgr.h"
#include "utils/date.h"
#include "utils/elog.h"
#include "utils/numeric.h"
#include "utils/snapmgr.h"
#include "varatt.h"

extern int pg_volvec_parallel_max_workers;

extern Datum int8_numeric(PG_FUNCTION_ARGS);
extern Datum numeric_mul(PG_FUNCTION_ARGS);
extern Datum numeric_int8(PG_FUNCTION_ARGS);
}

#include <algorithm>
#include <cmath>

#include "parallel/pipeline/pipeline_descriptor.hpp"

namespace pg_volvec {
namespace pipeline {

namespace {

static SeqScanSharedPayload *
ResolveSeqScanPayload(ExecCtx &ctx, dsa_pointer dp)
{
	if (!DsaPointerIsValid(dp))
		return nullptr;
	return static_cast<SeqScanSharedPayload *>(dsa_get_address(ctx.dsa, dp));
}

static SchemaDescriptor *
ResolveSchemaDescriptor(dsa_area *dsa, dsa_pointer dp)
{
	if (!DsaPointerIsValid(dp))
		return nullptr;
	return static_cast<SchemaDescriptor *>(dsa_get_address(dsa, dp));
}

static QualDescriptor *
ResolveQualDescriptor(dsa_area *dsa, dsa_pointer dp)
{
	if (!DsaPointerIsValid(dp))
		return nullptr;
	return static_cast<QualDescriptor *>(dsa_get_address(dsa, dp));
}

static uint32
ComputeMaxThreadsFromPayload(const SeqScanSharedPayload *shared)
{
	if (shared == nullptr || shared->morsel_nblocks == 0)
		return 1;

	double chunks = std::ceil((double) shared->total_blocks / (double) shared->morsel_nblocks);
	uint32 want = static_cast<uint32>(std::max(1.0, chunks));
	return (uint32) std::max(1, std::min(pg_volvec_parallel_max_workers, (int) want));
}

static inline int64_t
DatumNumericToScaledInt64(Datum d)
{
	Datum hundred = DirectFunctionCall1(int8_numeric, Int64GetDatum(100));
	Datum scaled  = DirectFunctionCall2(numeric_mul, d, hundred);
	return DatumGetInt64(DirectFunctionCall1(numeric_int8, scaled));
}

static inline bool
EvalTypedCompare(Oid typoid, uint64_t lhs_datum, QualOp op, uint64_t rhs_datum)
{
	switch (typoid)
	{
		case DATEOID:
		{
			DateADT l = DatumGetDateADT((Datum) lhs_datum);
			DateADT r = DatumGetDateADT((Datum) rhs_datum);
			switch (op)
			{
				case QualOp::LE: return l <= r;
				case QualOp::LT: return l <  r;
				case QualOp::EQ: return l == r;
				case QualOp::GE: return l >= r;
				case QualOp::GT: return l >  r;
				case QualOp::NE: return l != r;
			}
			break;
		}
		case INT4OID:
		{
			int32 l = DatumGetInt32((Datum) lhs_datum);
			int32 r = DatumGetInt32((Datum) rhs_datum);
			switch (op)
			{
				case QualOp::LE: return l <= r;
				case QualOp::LT: return l <  r;
				case QualOp::EQ: return l == r;
				case QualOp::GE: return l >= r;
				case QualOp::GT: return l >  r;
				case QualOp::NE: return l != r;
			}
			break;
		}
		case INT8OID:
		{
			int64 l = DatumGetInt64((Datum) lhs_datum);
			int64 r = DatumGetInt64((Datum) rhs_datum);
			switch (op)
			{
				case QualOp::LE: return l <= r;
				case QualOp::LT: return l <  r;
				case QualOp::EQ: return l == r;
				case QualOp::GE: return l >= r;
				case QualOp::GT: return l >  r;
				case QualOp::NE: return l != r;
			}
			break;
		}
		default:
			elog(ERROR, "pg_volvec: QualDescriptor const_typoid=%u not supported in v1 (by-value only)",
			     typoid);
	}
	return false;
}

static bool
EvalQualOnTuple(const QualDescriptor *qual, HeapTuple tuple, TupleDesc tupdesc)
{
	if (qual == nullptr || qual->kind == QualKind::NONE)
		return true;

	bool isnull = false;
	Datum d = heap_getattr(tuple, qual->col_attno, tupdesc, &isnull);
	if (isnull)
		return false;

	return EvalTypedCompare(qual->const_typoid,
	                        (uint64_t) d,
	                        qual->op,
	                        qual->const_value);
}

static inline void
AppendProjectedTupleToChunk(PipelineChunk &out,
                            TupleTableSlot *slot,
                            TupleDesc tupdesc,
                            const SchemaDescriptor *out_schema)
{
	HeapTuple tuple = ExecFetchSlotHeapTuple(slot, false, nullptr);
	uint16_t  row   = out.count++;

	const uint16_t n = out_schema->n_columns;
	for (uint16_t i = 0; i < n; ++i)
	{
		const ColumnSchema &col = out_schema->columns[i];
		const uint8_t       slot_idx = col.chunk_slot;

		bool  isnull = false;
		Datum d = heap_getattr(tuple, col.src_attno, tupdesc, &isnull);

		switch (col.decode_kind)
		{
			case ColumnDecodeKind::INT32_CHAR:
				if (isnull)
					out.int32_columns[slot_idx][row] = 0;
				else if (col.type_oid == BPCHAROID)
				{
					/* bpchar(1) Datum is varlena*; first payload byte is the char.
					 * DatumGetChar would read the low byte of the pointer (Bug G). */
					out.int32_columns[slot_idx][row] =
						(int32_t)(uint8_t) *(VARDATA_ANY(DatumGetPointer(d)));
				}
				else
				{
					/* "char" type — true 1-byte by-value. */
					out.int32_columns[slot_idx][row] = (int32_t) DatumGetChar(d);
				}
				out.nulls[slot_idx][row] = isnull ? 1 : 0;
				break;

			case ColumnDecodeKind::INT32_DATE:
				out.int32_columns[slot_idx][row] = isnull ? 0 : (int32_t) DatumGetDateADT(d);
				out.nulls[slot_idx][row]         = isnull ? 1 : 0;
				break;

			case ColumnDecodeKind::INT32_INT4:
				out.int32_columns[slot_idx][row] = isnull ? 0 : DatumGetInt32(d);
				out.nulls[slot_idx][row]         = isnull ? 1 : 0;
				break;

			case ColumnDecodeKind::INT64_INT8:
				out.int64_columns[slot_idx][row] = isnull ? 0 : DatumGetInt64(d);
				out.nulls[slot_idx][row]         = isnull ? 1 : 0;
				break;

			case ColumnDecodeKind::INT64_NUMERIC_SCALED:
				out.int64_columns[slot_idx][row] = isnull ? 0 : DatumNumericToScaledInt64(d);
				out.nulls[slot_idx][row]         = isnull ? 1 : 0;
				break;

			case ColumnDecodeKind::DOUBLE_FLOAT8:
				out.double_columns[slot_idx][row] = isnull ? 0.0 : DatumGetFloat8(d);
				out.nulls[slot_idx][row]          = isnull ? 1 : 0;
				break;

			case ColumnDecodeKind::NONE:
			default:
				elog(ERROR, "pg_volvec: ColumnSchema.decode_kind=%u invalid for SeqScan output column %u",
				     (unsigned) col.decode_kind, (unsigned) i);
		}
	}
}

} // namespace

std::unique_ptr<GlobalSourceState>
PhysicalSeqScan::GetGlobalSourceState(ExecCtx &ctx)
{
	auto state = std::make_unique<SeqScanGlobalState>();
	state->dsa = ctx.dsa;
	state->desc = desc_;

	/*
	 * Bug B fix — mirror HashAgg pattern (physical_hash_aggregate.cpp:94-129).
	 *
	 * SeqScan ctor receives shared_payload_dp = InvalidDsaPointer from the
	 * translator. The leader must self-allocate the SeqScanSharedPayload in
	 * DSA (one per relation, contains the morsel cursor) and publish it back
	 * through StoreSharedPayloadOnDescriptor so worker pipelines can resolve
	 * it via LoadSharedPayloadFromDescriptor. PipelineRunEvent::Schedule
	 * pre-invokes this on the leader before EnqueueTasks (DuckDB-faithful
	 * Pipeline::ResetSource), so when workers later call this method the
	 * descriptor is already populated.
	 */
	state->shared_payload_dp = DsaPointerIsValid(shared_payload_dp_) ? shared_payload_dp_ :
		LoadSharedPayloadFromDescriptor(this);

	if (ctx.worker_index == LEADER_WORKER_INDEX && !DsaPointerIsValid(state->shared_payload_dp))
	{
		Relation rel = relation_open(relid_, AccessShareLock);
		BlockNumber total = RelationGetNumberOfBlocks(rel);
		relation_close(rel, AccessShareLock);

		state->shared_payload_dp = dsa_allocate0(ctx.dsa, sizeof(SeqScanSharedPayload));
		auto *payload = static_cast<SeqScanSharedPayload *>(
			dsa_get_address(ctx.dsa, state->shared_payload_dp));
		pg_atomic_init_u64(&payload->next_block, 0);
		payload->total_blocks = total;
		/*
		 * Morsel size: small enough for parallel fan-out, large enough to
		 * amortise atomic-fetch-add cost. PostgreSQL parallel SeqScan uses
		 * a similar small constant (8 blocks). Tuned for Q1 single-table
		 * scan; revisit at M-Q1-PERF.
		 */
		payload->morsel_nblocks = 8;
		StoreSharedPayloadOnDescriptor(this, state->shared_payload_dp);
	}

	state->shared = ResolveSeqScanPayload(ctx, state->shared_payload_dp);
	state->max_threads = ComputeMaxThreadsFromPayload(state->shared);
	return state;
}

std::unique_ptr<LocalSourceState>
PhysicalSeqScan::GetLocalSourceState(ExecCtx &ctx, GlobalSourceState &gstate)
{
	auto &global = static_cast<SeqScanGlobalState &>(gstate);
	auto local = std::make_unique<SeqScanLocalState>();

	(void) ctx;

	local->rel = relation_open(relid_, AccessShareLock);
	local->scan_tupdesc = RelationGetDescr(local->rel);
	local->slot = MakeSingleTupleTableSlot(local->scan_tupdesc, &TTSOpsBufferHeapTuple);
	local->scan_desc = table_beginscan(local->rel, GetActiveSnapshot(), 0, nullptr);
	local->exhausted = (global.shared == nullptr || global.shared->total_blocks == 0);
	local->qual_program = nullptr;

	return local;
}

SourceResultType
PhysicalSeqScan::GetData(ExecCtx &ctx, PipelineChunk &out, OperatorSourceInput &input)
{
	auto &global = static_cast<SeqScanGlobalState &>(input.global_state);
	auto &local = static_cast<SeqScanLocalState &>(input.local_state);

	if (!local.diag_first_call_logged)
	{
		local.diag_first_call_logged = true;
	}

	out.reset();

	if (local.exhausted || global.shared == nullptr)
		return SourceResultType::FINISHED;

	SchemaDescriptor *out_schema = ResolveSchemaDescriptor(global.dsa, output_schema_dp_);
	if (out_schema == nullptr)
		elog(ERROR, "pg_volvec: PhysicalSeqScan output_schema_dp not published");

	QualDescriptor *qual = ResolveQualDescriptor(global.dsa, qual_desc_dp_);

	(void) ctx;

	for (;;)
	{
		uint64 start = pg_atomic_fetch_add_u64(&global.shared->next_block,
		                                      global.shared->morsel_nblocks);
		if (start >= global.shared->total_blocks)
		{
			local.exhausted = true;
			return out.count > 0 ? SourceResultType::HAVE_MORE_OUTPUT
			                      : SourceResultType::FINISHED;
		}

		local.current_block = (BlockNumber) start;
		local.end_block = Min((BlockNumber) (start + global.shared->morsel_nblocks),
		                     global.shared->total_blocks);

		BlockNumber numBlks = local.end_block - local.current_block;
		if (numBlks == 0)
		{
			local.exhausted = true;
			return out.count > 0 ? SourceResultType::HAVE_MORE_OUTPUT
			                      : SourceResultType::FINISHED;
		}
		heap_setscanlimits(local.scan_desc, local.current_block, numBlks);
		heap_rescan(local.scan_desc, NULL, true, false, false, true);

		while (out.count < PIPELINE_DEFAULT_CHUNK_SIZE &&
		       heap_getnextslot(local.scan_desc, ForwardScanDirection, local.slot))
		{
			HeapTuple tuple = ExecFetchSlotHeapTuple(local.slot, false, nullptr);
			if (tuple == nullptr)
				continue;
			if (!EvalQualOnTuple(qual, tuple, local.scan_tupdesc))
				continue;
			AppendProjectedTupleToChunk(out, local.slot, local.scan_tupdesc, out_schema);
		}

		if (out.count >= PIPELINE_DEFAULT_CHUNK_SIZE)
			return SourceResultType::HAVE_MORE_OUTPUT;

		if (out.count > 0)
			return SourceResultType::HAVE_MORE_OUTPUT;

		local.exhausted = true;
		return SourceResultType::FINISHED;
	}
}

int
PhysicalSeqScan::MaxThreads(ExecCtx &ctx) const
{
	SeqScanSharedPayload *shared = ResolveSeqScanPayload(ctx, shared_payload_dp_);
	return (int) ComputeMaxThreadsFromPayload(shared);
}

}  /* namespace pipeline */
}  /* namespace pg_volvec */

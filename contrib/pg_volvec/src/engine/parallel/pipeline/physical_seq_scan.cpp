#include "parallel/pipeline/physical_seq_scan.hpp"

extern "C" {
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/tableam.h"
#include "catalog/pg_type_d.h"
#include "executor/tuptable.h"
#include "fmgr.h"
#include "utils/date.h"
#include "utils/elog.h"
#include "utils/numeric.h"
#include "utils/snapmgr.h"
#include "varatt.h"

extern int  pg_volvec_parallel_max_workers;
extern int  pg_volvec_parallel_morsel_nblocks;
extern bool pg_volvec_jit_deform;
extern bool pg_volvec_disable_jit_for_parallel_worker;

extern Datum int8_numeric(PG_FUNCTION_ARGS);
extern Datum numeric_mul(PG_FUNCTION_ARGS);
extern Datum numeric_int8(PG_FUNCTION_ARGS);
}

#include <algorithm>
#include <cmath>

#include "parallel/pipeline/pipeline_descriptor.hpp"
#ifdef USE_LLVM
#include "llvmjit_deform_datachunk.h"
#endif

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

/*
 * Inline single-row predicate evaluator. Reads from qual_chunk at row 0
 * (the qual deformer always writes there) using the dst_col resolved at
 * build time. NULL → false; QualKind::NONE → true short-circuits before
 * any deform happens (caller skips the qual deform entirely in that
 * case). Type dispatch is the same set as EvalTypedCompare's by-value
 * Datum path (DATEOID/INT4/INT8 × 6 ops); we read pre-decoded typed
 * values from the chunk so no Datum unpacking is needed here.
 */
static inline bool
EvalSinglePredicate(const QualDescriptor *qual,
                    const DataChunk<PIPELINE_DEFAULT_CHUNK_SIZE> &qchunk,
                    uint16_t dst_col)
{
	if (qchunk.nulls[dst_col][0])
		return false;

	switch (qual->const_typoid)
	{
		case DATEOID:
		{
			DateADT l = (DateADT) qchunk.int32_columns[dst_col][0];
			DateADT r = DatumGetDateADT((Datum) qual->const_value);
			switch (qual->op)
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
			int32 l = qchunk.int32_columns[dst_col][0];
			int32 r = DatumGetInt32((Datum) qual->const_value);
			switch (qual->op)
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
			int64 l = qchunk.int64_columns[dst_col][0];
			int64 r = DatumGetInt64((Datum) qual->const_value);
			switch (qual->op)
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
			     qual->const_typoid);
	}
	return false;
}

/*
 * Map planner-published per-column decode kind onto the deformer's
 * physical decode kind. The two enums are intentionally distinct:
 * ColumnDecodeKind is the projection-layer contract (lives in the
 * descriptor IR, must stay stable across processes); DeformDecodeKind
 * is the physical decoder's per-target tag and may add new kinds
 * (e.g. kBpchar1) without touching the descriptor format. Returns
 * false for ColumnDecodeKind::NONE so caller can hard-error.
 */
static inline bool
MapColumnToDeformKind(const ColumnSchema &col, DeformDecodeKind &out_kind)
{
	switch (col.decode_kind)
	{
		case ColumnDecodeKind::INT32_CHAR:
			out_kind = (col.type_oid == BPCHAROID)
				? DeformDecodeKind::kBpchar1
				: DeformDecodeKind::kInt32;
			return true;
		case ColumnDecodeKind::INT32_DATE:
			out_kind = DeformDecodeKind::kDate32;
			return true;
		case ColumnDecodeKind::INT32_INT4:
			out_kind = DeformDecodeKind::kInt32;
			return true;
		case ColumnDecodeKind::INT64_INT8:
			out_kind = DeformDecodeKind::kInt64;
			return true;
		case ColumnDecodeKind::INT64_NUMERIC_SCALED:
			out_kind = DeformDecodeKind::kNumeric;
			return true;
		case ColumnDecodeKind::DOUBLE_FLOAT8:
			out_kind = DeformDecodeKind::kFloat8;
			return true;
		case ColumnDecodeKind::NONE:
		default:
			return false;
	}
}

/*
 * Build per-schema-column DeformBindings for the given output chunk.
 * Invariant: BuildDeformProgramFromSchema stamps target.dst_col with the
 * source schema column index (NOT chunk_slot), and the deformer writes
 * bindings.columns_data[dst_col]. We therefore index bindings by schema
 * column index s in [0, n_columns), and route each schema column to its
 * per-storage chunk array via columns[s].chunk_slot + decode_kind.
 *
 * Heads point at row 0; the deformer offsets by row_idx.
 */
static inline void
BuildDeformBindings(const SchemaDescriptor *out_schema,
                    PipelineChunk &out,
                    DeformBindings &bindings)
{
	const uint16_t n = out_schema->n_columns;
	bindings.ncolumns = n;
	bindings.owner_chunk = &out;
	for (uint16_t s = 0; s < n; ++s)
	{
		const ColumnSchema &col = out_schema->columns[s];
		const uint8_t       slot_idx = col.chunk_slot;
		void *data_head;
		switch (col.decode_kind)
		{
			case ColumnDecodeKind::INT32_CHAR:
			case ColumnDecodeKind::INT32_DATE:
			case ColumnDecodeKind::INT32_INT4:
				data_head = static_cast<void *>(out.int32_columns[slot_idx]);
				break;
			case ColumnDecodeKind::INT64_INT8:
			case ColumnDecodeKind::INT64_NUMERIC_SCALED:
				data_head = static_cast<void *>(out.int64_columns[slot_idx]);
				break;
			case ColumnDecodeKind::DOUBLE_FLOAT8:
				data_head = static_cast<void *>(out.double_columns[slot_idx]);
				break;
			default:
				elog(ERROR, "pg_volvec: unsupported ColumnDecodeKind=%u in SeqScan deform binding",
				     (unsigned) col.decode_kind);
		}
		bindings.columns_data[s]  = data_head;
		bindings.columns_nulls[s] = out.nulls[slot_idx];
	}
}

/*
 * Native + JIT deform path. Replaces the per-column heap_getattr loop
 * (which called nocachegetattr — Q1's dominant hot leaf at 16.7K samples
 * pre-B.1) with a single offset-walking deform that emits all targets
 * in one pass. The deformer dispatches to the JIT'd function if
 * proj_jit_func is set, else interprets. Caller is responsible for
 * having qual already evaluated and survived; out.count is incremented
 * here on append.
 */
static inline void
AppendProjectedTupleViaDeformer(PipelineChunk &out,
                                 HeapTuple tuple,
                                 const SchemaDescriptor *out_schema,
                                 SeqScanLocalState &local)
{
	uint16_t row = out.count++;

	DeformBindings bindings;
	BuildDeformBindings(out_schema, out, bindings);
	local.proj_deformer->deform_tuple_header(tuple->t_data, row, bindings);
}

/*
 * Build DeformProgram from SchemaDescriptor (projection side). Stamps
 * target.dst_col with the SCHEMA column index s, NOT chunk_slot —
 * matches the contract used by BuildDeformBindings above and consumed
 * by data_chunk_deform.cpp's `bindings.columns_data[t.dst_col]` writes.
 * att_index uses 0-based convention (PG's attnum-1) to match
 * TupleDescCompactAttr indexing inside DataChunkDeformer::deform_tuple_header.
 */
static bool
BuildProjDeformProgramFromSchema(const SchemaDescriptor *out_schema,
                                  DeformProgram &program)
{
	program.reset();
	const uint16_t n = out_schema->n_columns;
	if (n > kMaxDeformTargets)
		return false;
	for (uint16_t s = 0; s < n; ++s)
	{
		const ColumnSchema &col = out_schema->columns[s];
		DeformDecodeKind    kind;
		if (col.src_attno <= 0)
			return false;
		if (!MapColumnToDeformKind(col, kind))
			return false;
		program.add_target((int) col.src_attno - 1, (int) s, kind);
	}
	program.finalize();
	return true;
}

/*
 * Build DeformProgram for the qual column (qual side). v1 supports a
 * single col_op_const predicate so the program has exactly 1 target.
 * out_dst_col=0 is hard-coded (the qual chunk has only this one column
 * we care about) and stored in local.qual_dst_col so EvalSinglePredicate
 * skips a search per tuple. Decode kind chosen from const_typoid (the
 * column's type matches by construction — translator gates the extract).
 *
 * Routes the qual_chunk's int32_columns[0] / int64_columns[0] storage
 * via dst_col=0 so the binding builder's chunk_slot=0 default works
 * without a SchemaDescriptor.
 */
static bool
BuildQualDeformProgramFromQual(const QualDescriptor *qual,
                                DeformProgram &program,
                                uint16_t &out_dst_col)
{
	program.reset();
	if (qual == nullptr || qual->kind == QualKind::NONE)
		return false;
	if (qual->col_attno <= 0)
		return false;

	DeformDecodeKind kind;
	switch (qual->const_typoid)
	{
		case DATEOID: kind = DeformDecodeKind::kDate32; break;
		case INT4OID: kind = DeformDecodeKind::kInt32;  break;
		case INT8OID: kind = DeformDecodeKind::kInt64;  break;
		default:
			return false;
	}
	program.add_target((int) qual->col_attno - 1, 0, kind);
	program.finalize();
	out_dst_col = 0;
	return true;
}

/*
 * Build qual-side DeformBindings against qual_chunk slot 0. The qual
 * deformer writes one column per tuple at row 0; the chunk type used
 * is the same PIPELINE_DEFAULT_CHUNK_SIZE template so the existing
 * deformer/JIT instantiation works without widening.
 */
static inline void
BuildQualDeformBindings(DataChunk<PIPELINE_DEFAULT_CHUNK_SIZE> &qchunk,
                         DeformDecodeKind kind,
                         DeformBindings &bindings)
{
	bindings.ncolumns = 1;
	bindings.owner_chunk = &qchunk;
	void *data_head;
	switch (kind)
	{
		case DeformDecodeKind::kInt32:
		case DeformDecodeKind::kDate32:
			data_head = static_cast<void *>(qchunk.int32_columns[0]);
			break;
		case DeformDecodeKind::kInt64:
			data_head = static_cast<void *>(qchunk.int64_columns[0]);
			break;
		default:
			elog(ERROR, "pg_volvec: qual deform kind=%u unsupported (v1 by-value only)",
			     (unsigned) kind);
	}
	bindings.columns_data[0]  = data_head;
	bindings.columns_nulls[0] = qchunk.nulls[0];
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
		/* Bug K: morsel size from GUC; source drains across morsels inside
		 * one GetData call (DuckDB-faithful), so chunk-size is decoupled. */
		payload->morsel_nblocks = (uint32) Max(1, pg_volvec_parallel_morsel_nblocks);
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

	/*
	 * First-call deformer build. Why here (not in GetLocalSourceState):
	 * out_schema is resolved from a DSA pointer that is only valid after
	 * descriptor publish; the local-state ctor runs before that for
	 * leader-self-allocate paths. Builds BOTH proj and qual programs
	 * (qual only when present), allocates qual_chunk lazily, and JITs
	 * each independently. JIT compile is opportunistic and silently
	 * falls back to native interpreter on failure. JIT for the qual
	 * deformer is identical to proj — same factory, same dispatch.
	 */
	if (!local.deform_programs_built)
	{
		if (BuildProjDeformProgramFromSchema(out_schema, local.proj_deform_program))
		{
			local.proj_deformer = std::make_unique<DataChunkDeformer>(local.scan_tupdesc,
			                                                           &local.proj_deform_program);
#ifdef USE_LLVM
			if (pg_volvec_jit_deform &&
			    !(ctx.worker_index != LEADER_WORKER_INDEX &&
			      pg_volvec_disable_jit_for_parallel_worker))
			{
				JitDeformFunc fn = nullptr;
				JitContext   *jc = nullptr;
				const char   *err = nullptr;
				if (pg_volvec_try_compile_jit_deform_to_datachunk(local.scan_tupdesc,
				                                                    &local.proj_deform_program,
				                                                    &fn, &jc, &err) &&
				    fn != nullptr)
				{
					local.proj_jit_func    = fn;
					local.proj_jit_context = jc;
					local.proj_deformer->set_jit_func(fn);
					if (jc != nullptr)
						pg_volvec_register_llvm_jit_context(jc);
				}
			}
#endif
		}

		/* Qual side: only built if a real predicate exists. NONE qual
		 * skips this entire block; inner loop calls projection directly. */
		if (qual != nullptr && qual->kind != QualKind::NONE)
		{
			if (BuildQualDeformProgramFromQual(qual, local.qual_deform_program,
			                                    local.qual_dst_col))
			{
				local.qual_chunk = std::unique_ptr<DataChunk<PIPELINE_DEFAULT_CHUNK_SIZE>>(
					new DataChunk<PIPELINE_DEFAULT_CHUNK_SIZE>());
				local.qual_deformer = std::make_unique<DataChunkDeformer>(local.scan_tupdesc,
				                                                            &local.qual_deform_program);
#ifdef USE_LLVM
				if (pg_volvec_jit_deform &&
				    !(ctx.worker_index != LEADER_WORKER_INDEX &&
				      pg_volvec_disable_jit_for_parallel_worker))
				{
					JitDeformFunc fn = nullptr;
					JitContext   *jc = nullptr;
					const char   *err = nullptr;
					if (pg_volvec_try_compile_jit_deform_to_datachunk(local.scan_tupdesc,
					                                                    &local.qual_deform_program,
					                                                    &fn, &jc, &err) &&
					    fn != nullptr)
					{
						local.qual_jit_func    = fn;
						local.qual_jit_context = jc;
						local.qual_deformer->set_jit_func(fn);
						if (jc != nullptr)
							pg_volvec_register_llvm_jit_context(jc);
					}
				}
#endif
			}
		}
		local.deform_programs_built = true;
	}

	/* Pre-build qual bindings once per GetData call (heads point at row 0
	 * of the persistent qual_chunk; deformer always writes there). */
	DeformBindings qual_bindings;
	const bool has_qual = (qual != nullptr && qual->kind != QualKind::NONE
	                       && local.qual_deformer != nullptr);
	if (has_qual)
	{
		DeformDecodeKind kind;
		switch (qual->const_typoid)
		{
			case DATEOID: kind = DeformDecodeKind::kDate32; break;
			case INT4OID: kind = DeformDecodeKind::kInt32;  break;
			case INT8OID: kind = DeformDecodeKind::kInt64;  break;
			default:      kind = DeformDecodeKind::kInt64;  break;  /* unreachable: BuildQual… would have refused */
		}
		BuildQualDeformBindings(*local.qual_chunk, kind, qual_bindings);
	}

	for (;;)
	{
		if (!local.morsel_active)
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
			/* Bug K': heap_setscanlimits MUST run AFTER heap_rescan
			 * (initscan() resets rs_numblocks=Invalid). */
			heap_rescan(local.scan_desc, NULL, true, false, false, true);
			heap_setscanlimits(local.scan_desc, local.current_block, numBlks);
			local.morsel_active = true;
		}

		while (out.count < PIPELINE_DEFAULT_CHUNK_SIZE &&
		       heap_getnextslot(local.scan_desc, ForwardScanDirection, local.slot))
		{
			HeapTuple tuple = ExecFetchSlotHeapTuple(local.slot, false, nullptr);
			if (tuple == nullptr)
				continue;

			/* B.1 Option C: deform qual cols first (1-row scratch at row 0),
			 * evaluate predicate inline; only on survival do we deform the
			 * full projection at out.count and increment. Q1 rejects ~96.6%
			 * of rows so the projection deform is short-circuited for the
			 * vast majority of tuples. */
			if (has_qual)
			{
				local.qual_deformer->deform_tuple_header(tuple->t_data, 0, qual_bindings);
				if (!EvalSinglePredicate(qual, *local.qual_chunk, local.qual_dst_col))
					continue;
			}
			AppendProjectedTupleViaDeformer(out, tuple, out_schema, local);
		}

		if (out.count >= PIPELINE_DEFAULT_CHUNK_SIZE)
			return SourceResultType::HAVE_MORE_OUTPUT;

		/* Inner loop exited because heap_getnextslot returned false:
		 * current morsel fully drained. Loop back to fetch the next morsel. */
		local.morsel_active = false;
	}
}

int
PhysicalSeqScan::MaxThreads(ExecCtx &ctx) const
{
	/* Bug N: shared_payload_dp_ ctor field is InvalidDsaPointer until the
	 * leader self-allocates inside GetGlobalSourceState; reading it here
	 * collapsed RUN fan-out to 1 and serialized the entire scan onto
	 * worker 0 (loop_wait == active_w_task across w in {1,2,4,8}).
	 * Load-from-descriptor is the canonical cross-process channel
	 * (Bug H invariant). PipelineRunEvent::Schedule pre-invokes
	 * GetGlobalSourceState on the leader before EnqueueTasks calls
	 * MaxThreads, so the descriptor slot is populated by this point. */
	dsa_pointer dp = DsaPointerIsValid(shared_payload_dp_)
		? shared_payload_dp_
		: LoadSharedPayloadFromDescriptor(this);
	SeqScanSharedPayload *shared = ResolveSeqScanPayload(ctx, dp);
	return (int) ComputeMaxThreadsFromPayload(shared);
}

}  /* namespace pipeline */
}  /* namespace pg_volvec */

#include "exec/internal.hpp"

namespace pg_volvec {

VecSeqScanState::VecSeqScanState(Relation rel,
								 Snapshot snapshot,
								 const DeformProgram *program,
								 ParallelTableScanDesc parallel_scan_desc)
	: rel_(rel), snapshot_(snapshot), deformer_(RelationGetDescr(rel), program) {
	/*
	 * We drive block iteration ourselves below. Letting heap_beginscan choose a
	 * synchronized-scan start block would skip the prefix blocks because this
	 * custom loop never wraps back around to block 0.
	 */
	if (parallel_scan_desc != nullptr)
		scan_ = (HeapScanDesc) table_beginscan_parallel(rel_, parallel_scan_desc);
	else
		scan_ = (HeapScanDesc) heap_beginscan(rel_, snapshot_, 0, NULL, NULL, SO_TYPE_SEQSCAN | SO_ALLOW_STRAT | SO_ALLOW_PAGEMODE);
	stream_ = scan_->rs_read_stream;
	current_buf_ = InvalidBuffer;
	vmbuf_ = InvalidBuffer;
	current_offnum_ = FirstOffsetNumber;
	all_visible_ = false;
#ifdef USE_LLVM
	JitDeformFunc jf;
	const char *err;
	if (pg_volvec_jit_deform) {
		if (pg_volvec_disable_jit_for_parallel_worker) {
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: deform JIT disabled in process parallel worker");
		} else if (pg_volvec_try_compile_jit_deform_to_datachunk(RelationGetDescr(rel), program, &jf, &jit_context_, &err)) {
			deformer_.set_jit_func(jf);
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: deform JIT compiled successfully (targets=%d, func=%p)", program->ntargets, (void *) jf);
		} else if (pg_volvec_trace_hooks) {
			elog(LOG, "pg_volvec: deform JIT compile skipped or failed (targets=%d, reason=%s)", program->ntargets, err != nullptr ? err : "unknown");
		}
	} else if (pg_volvec_trace_hooks) {
		elog(LOG, "pg_volvec: deform JIT disabled by GUC");
	}
#endif
	if (pg_volvec_trace_hooks && stream_ != nullptr)
	{
		if (scan_->rs_base.rs_parallel != nullptr)
			elog(LOG, "pg_volvec: VecSeqScanState using PostgreSQL parallel heap scan/read_stream");
		else
			elog(LOG, "pg_volvec: VecSeqScanState using heap read_stream for scan prefetch");
	}
}

VecSeqScanState::~VecSeqScanState() { 
		if (pg_volvec_trace_hooks && jit_context_)
			elog(LOG, "pg_volvec: VecSeqScanState dtor releasing deform JIT context %p", (void *) jit_context_);
		if (BufferIsValid(current_buf_)) UnlockReleaseBuffer(current_buf_);
		if (BufferIsValid(vmbuf_)) ReleaseBuffer(vmbuf_);
		if (block_range_stream_ != nullptr)
		{
			read_stream_end(block_range_stream_);
			block_range_stream_ = nullptr;
		}
		heap_endscan((TableScanDesc)scan_); 
		table_close(rel_, NoLock); 
		if (jit_context_) {
			pg_volvec_release_llvm_jit_context(jit_context_);
			jit_context_ = nullptr;
		}
}

bool
VecSeqScanState::lookup_output_col_meta(int target_resno, VecOutputColMeta *out) const
{
	TupleDesc desc = RelationGetDescr(rel_);
	int att_index = target_resno - 1;
	Oid typid;

	if (att_index < 0 || att_index >= desc->natts || att_index >= 16)
		return false;

	typid = TupleDescAttr(desc, att_index)->atttypid;
	if (out != nullptr) {
		out->sql_type = typid;
		out->storage_kind = DefaultOutputStorageKindForType(typid);
		out->scale = (typid == NUMERICOID) ?
			GetNumericScaleFromTypmod(TupleDescAttr(desc, att_index)->atttypmod) : 0;
	}
	return true;
}

bool
VecSeqScanState::configure_source_block_range(BlockNumber start_block, uint32_t nblocks)
{
	configure_block_range(start_block, nblocks);
	return true;
}

void
VecSeqScanState::clear_source_block_range()
{
	clear_block_range();
}

void
VecSeqScanState::configure_block_range(BlockNumber start_block, uint32_t nblocks)
{
	uint64 end_block = (uint64) start_block + (uint64) nblocks;

	block_range_active_ = true;
	block_range_start_ = start_block;
	block_range_end_ = (end_block > scan_->rs_nblocks) ? scan_->rs_nblocks : (BlockNumber) end_block;
	if (block_range_end_ < block_range_start_)
		block_range_end_ = block_range_start_;
	if (block_range_stream_ != nullptr)
	{
		read_stream_end(block_range_stream_);
		block_range_stream_ = nullptr;
	}
	if (BufferIsValid(current_buf_))
	{
		UnlockReleaseBuffer(current_buf_);
		current_buf_ = InvalidBuffer;
	}
	if (BufferIsValid(vmbuf_))
	{
		ReleaseBuffer(vmbuf_);
		vmbuf_ = InvalidBuffer;
	}
	current_offnum_ = FirstOffsetNumber;
	scan_->rs_cblock = InvalidBlockNumber;
	block_range_stream_private_.current_blocknum = block_range_start_;
	block_range_stream_private_.last_exclusive = block_range_end_;
	if (block_range_start_ < block_range_end_)
	{
		block_range_stream_ =
			read_stream_begin_relation(READ_STREAM_SEQUENTIAL |
									   READ_STREAM_USE_BATCHING,
									   scan_->rs_strategy,
									   rel_,
									   MAIN_FORKNUM,
									   block_range_read_stream_cb,
									   &block_range_stream_private_,
									   0);
		if (pg_volvec_trace_hooks && block_range_stream_ != nullptr)
			elog(LOG,
				 "pg_volvec: VecSeqScanState using block-range read_stream start=%u end=%u",
				 block_range_start_,
				 block_range_end_);
	}
}

void
VecSeqScanState::clear_block_range()
{
	block_range_active_ = false;
	block_range_start_ = InvalidBlockNumber;
	block_range_end_ = InvalidBlockNumber;
	if (block_range_stream_ != nullptr)
	{
		read_stream_end(block_range_stream_);
		block_range_stream_ = nullptr;
	}
	if (BufferIsValid(current_buf_))
	{
		UnlockReleaseBuffer(current_buf_);
		current_buf_ = InvalidBuffer;
	}
	if (BufferIsValid(vmbuf_))
	{
		ReleaseBuffer(vmbuf_);
		vmbuf_ = InvalidBuffer;
	}
	current_offnum_ = FirstOffsetNumber;
	scan_->rs_cblock = InvalidBlockNumber;
}

void
VecSeqScanState::prepare_bindings(DataChunk<DEFAULT_CHUNK_SIZE> &chunk,
								   DeformBindings *bindings) const
{
	TupleDesc desc = RelationGetDescr(rel_);

	memset(bindings, 0, sizeof(*bindings));
	for (int i = 0; i < 16; i++)
	{
		bindings->columns_data[i] = chunk.int32_columns[i];
		bindings->columns_nulls[i] = chunk.nulls[i];
	}
	bindings->owner_chunk = &chunk;
	for (int i = 0; i < desc->natts && i < 16; i++)
	{
		Oid typid = TupleDescAttr(desc, i)->atttypid;

		if (typid == FLOAT8OID)
			bindings->columns_data[i] = chunk.double_columns[i];
		else if (typid == NUMERICOID || typid == INT8OID)
			bindings->columns_data[i] = chunk.int64_columns[i];
		else if (typid == BPCHAROID || typid == TEXTOID || typid == VARCHAROID)
			bindings->columns_data[i] = chunk.string_columns[i];
		else if (typid == DATEOID)
			bindings->columns_data[i] = chunk.int32_columns[i];
	}
}

bool
VecSeqScanState::open_next_buffer()
{
	if (block_range_active_)
	{
		if (block_range_stream_ == nullptr)
			return false;
		current_buf_ = read_stream_next_buffer(block_range_stream_, NULL);
		if (!BufferIsValid(current_buf_))
			return false;
		scan_->rs_cblock = BufferGetBlockNumber(current_buf_);
		if (BufferIsValid(current_buf_))
			blocks_opened_++;
		return BufferIsValid(current_buf_);
	}

	if (stream_ != nullptr) {
		current_buf_ = read_stream_next_buffer(stream_, NULL);
		if (!BufferIsValid(current_buf_))
			return false;
		scan_->rs_cblock = BufferGetBlockNumber(current_buf_);
		blocks_opened_++;
		return true;
	}

	if (scan_->rs_cblock == InvalidBlockNumber) {
		scan_->rs_cblock = scan_->rs_startblock;
	} else {
		scan_->rs_cblock++;
	}

	if (scan_->rs_cblock >= scan_->rs_nblocks)
		return false;

	current_buf_ = ReadBufferExtended(rel_, MAIN_FORKNUM, scan_->rs_cblock,
									 RBM_NORMAL, scan_->rs_strategy);
	if (BufferIsValid(current_buf_))
		blocks_opened_++;
	return BufferIsValid(current_buf_);
}

bool VecSeqScanState::get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk) {
	chunk.reset();
	DeformBindings bindings;

	prepare_bindings(chunk, &bindings);

	while (chunk.count < DEFAULT_CHUNK_SIZE) {
		if (current_buf_ == InvalidBuffer) {
			if (!open_next_buffer())
				break;

			LockBuffer(current_buf_, BUFFER_LOCK_SHARE);
			current_offnum_ = FirstOffsetNumber;

			uint8 vmstatus = visibilitymap_get_status(rel_, scan_->rs_cblock, &vmbuf_);
			all_visible_ = (vmstatus & VISIBILITYMAP_ALL_VISIBLE) != 0;
		}

		Page page = BufferGetPage(current_buf_);
		int maxoff = PageGetMaxOffsetNumber(page);

		if (all_visible_) {
			/* Fast path: batch deform all normal items */
			while (current_offnum_ <= maxoff && chunk.count < DEFAULT_CHUNK_SIZE) {
				ItemId itemid = PageGetItemId(page, current_offnum_);
				if (ItemIdIsNormal(itemid)) {
					HeapTupleHeader tuphdr = (HeapTupleHeader) PageGetItem(page, itemid);
					deformer_.deform_tuple_header(tuphdr, chunk.count, bindings);
					chunk.count++;
				}
				current_offnum_++;
			}
		} else {
			/* Slow path: per-tuple visibility check */
			while (current_offnum_ <= maxoff && chunk.count < DEFAULT_CHUNK_SIZE) {
				ItemId itemid = PageGetItemId(page, current_offnum_);
				if (!ItemIdIsNormal(itemid)) { current_offnum_++; continue; }

				HeapTupleHeader tuphdr = (HeapTupleHeader) PageGetItem(page, itemid);
				HeapTupleData temp_tuple;
				temp_tuple.t_len = ItemIdGetLength(itemid);
				temp_tuple.t_data = tuphdr;
				BlockIdSet(&temp_tuple.t_self.ip_blkid, scan_->rs_cblock);
				temp_tuple.t_self.ip_posid = current_offnum_;
				temp_tuple.t_tableOid = RelationGetRelid(rel_);
				if (HeapTupleSatisfiesVisibility(&temp_tuple, snapshot_, current_buf_)) {
					deformer_.deform_tuple_header(tuphdr, chunk.count, bindings);
					chunk.count++;
				}
				current_offnum_++;
			}
		}

		if (current_offnum_ > maxoff) {
			UnlockReleaseBuffer(current_buf_);
			current_buf_ = InvalidBuffer;
		}
	}
	return chunk.count > 0;
}

} /* namespace pg_volvec */

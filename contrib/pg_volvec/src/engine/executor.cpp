#include "volvec_engine.hpp"
#include "llvmjit_deform_datachunk.h"

#include <algorithm>

extern "C" {
#include "utils/lsyscache.h"
#include "access/tableam.h"
#include "access/visibilitymap.h"
#include "access/stratnum.h"
#include "nodes/nodeFuncs.h"
#include "storage/bufmgr.h"

extern bool pg_volvec_jit_deform;
extern bool pg_volvec_trace_hooks;
}

namespace pg_volvec
{

static bool
ShouldUseExactNumericAgg(Oid arg_type)
{
	return arg_type == NUMERICOID;
}

static VecOutputStorageKind
DefaultOutputStorageKindForType(Oid typid)
{
	if (typid == FLOAT8OID)
		return VecOutputStorageKind::Double;
	if (typid == NUMERICOID)
		return VecOutputStorageKind::NumericScaledInt64;
	if (typid == BPCHAROID || typid == TEXTOID || typid == VARCHAROID)
		return VecOutputStorageKind::StringRef;
	if (typid == INT8OID)
		return VecOutputStorageKind::Int64;
	return VecOutputStorageKind::Int32;
}

static uint64_t
EncodeFloat8SortKey(double value)
{
	uint64_t bits;

	memcpy(&bits, &value, sizeof(bits));
	if ((bits & (UINT64CONST(1) << 63)) != 0)
		return ~bits;
	return bits ^ (UINT64CONST(1) << 63);
}

static DeformDecodeKind
DecodeKindForType(Oid typid)
{
	if (typid == FLOAT8OID)
		return DeformDecodeKind::kFloat8;
	if (typid == NUMERICOID)
		return DeformDecodeKind::kNumeric;
	if (typid == BPCHAROID || typid == TEXTOID || typid == VARCHAROID)
		return DeformDecodeKind::kStringRef;
	if (typid == DATEOID)
		return DeformDecodeKind::kDate32;
	if (typid == INT8OID)
		return DeformDecodeKind::kInt64;
	return DeformDecodeKind::kInt32;
}

static bool
CollectAttrNosFromExprWalker(Node *node, Bitmapset **attrs)
{
	if (node == nullptr)
		return false;

	if (IsA(node, Var))
	{
		Var *var = (Var *) node;

		if (var->varlevelsup == 0 &&
			var->varattno > 0 &&
			var->varattno <= kMaxDeformTargets)
			*attrs = bms_add_member(*attrs, var->varattno - 1);
		return false;
	}

	return expression_tree_walker(node, CollectAttrNosFromExprWalker, attrs);
}

static void
CollectAttrNosFromExpr(Node *node, Bitmapset **attrs)
{
	if (node != nullptr)
		(void) CollectAttrNosFromExprWalker(node, attrs);
}

static void
CollectRequiredAttrsForPlan(Plan *plan, Bitmapset **attrs)
{
	if (plan == nullptr)
		return;

	if (plan->qual != NIL)
		CollectAttrNosFromExpr((Node *) plan->qual, attrs);

	/*
	 * Base scan targetlists are often the full physical tuple; collecting from
	 * them defeats pruning.  Instead, collect required Vars from upper plan
	 * nodes and scan quals only.
	 */
	if (!IsA(plan, SeqScan) && plan->targetlist != NIL)
		CollectAttrNosFromExpr((Node *) plan->targetlist, attrs);

	CollectRequiredAttrsForPlan(plan->lefttree, attrs);
	CollectRequiredAttrsForPlan(plan->righttree, attrs);
}

static void
BuildPrunedDeformProgram(Bitmapset *attrs, TupleDesc desc, DeformProgram *program)
{
	int att_index = -1;

	program->reset();
	if (attrs == nullptr)
	{
		program->finalize();
		return;
	}

	while ((att_index = bms_next_member(attrs, att_index)) >= 0)
	{
		if (att_index >= desc->natts || att_index >= kMaxDeformTargets)
			continue;
		program->add_target(att_index, att_index,
							DecodeKindForType(TupleDescAttr(desc, att_index)->atttypid));
	}

	program->finalize();
}

/* --- Optimized DataChunkDeformer --- */
void DataChunkDeformer::deform_tuple_header(HeapTupleHeader tuphdr, uint32 row_idx, const DeformBindings &bindings) {
	if (jit_func_) {
		if (pg_volvec_trace_hooks && !jit_path_logged_) {
			elog(LOG, "pg_volvec: using deform JIT path for row deconstruction");
			jit_path_logged_ = true;
		}
		jit_func_(tuphdr, (void**)bindings.columns_data, (uint8_t**)bindings.columns_nulls, row_idx);
		return;
	}
	/* 
	 * SPECIALIZED DEFORMER FOR TPCH:
	 * We skip the generic heap_getattr and use a more direct approach.
	 */
	HeapTupleData tuple; tuple.t_len = HeapTupleHeaderGetDatumLength(tuphdr); tuple.t_data = tuphdr;
	
	for (int i = 0; i < program_.ntargets; i++) {
		const auto &target = program_.targets[i]; bool isnull;
		Datum val = heap_getattr(&tuple, target.att_index + 1, desc_, &isnull);
		bindings.columns_nulls[target.dst_col][row_idx] = (uint8_t)isnull;
		if (!isnull) {
			if (target.decode_kind == DeformDecodeKind::kInt32 || target.decode_kind == DeformDecodeKind::kDate32)
				((int32_t*)bindings.columns_data[target.dst_col])[row_idx] = DatumGetInt32(val);
			else if (target.decode_kind == DeformDecodeKind::kNumeric) {
				int scale = GetNumericScaleFromTypmod(TupleDescAttr(desc_, target.att_index)->atttypmod);
				int64_t scaled = 0;

				if (!TryFastNumericToScaledInt64(val, scale, &scaled))
					elog(ERROR, "pg_volvec fast numeric decode failed for attribute %d", target.att_index + 1);
				((int64_t*)bindings.columns_data[target.dst_col])[row_idx] = scaled;
			}
			else if (target.decode_kind == DeformDecodeKind::kFloat8)
				((double*)bindings.columns_data[target.dst_col])[row_idx] = DatumGetFloat8(val);
			else if (target.decode_kind == DeformDecodeKind::kInt64)
				((int64_t*)bindings.columns_data[target.dst_col])[row_idx] = DatumGetInt64(val);
			else if (target.decode_kind == DeformDecodeKind::kStringRef) {
				struct varlena *v = (struct varlena *) DatumGetPointer(val);
				char *vptr = VARDATA_ANY(v);
				int len = VARSIZE_ANY_EXHDR(v);
				uint64_t pref = 0;
				memcpy(&pref, vptr, len > 8 ? 8 : len);
				((VecStringRef*)bindings.columns_data[target.dst_col])[row_idx] = { (uint32_t)len, 0, pref };
			}
		}
	}
}

/* --- VecAggState --- */
VecAggState::VecAggState(std::unique_ptr<VecPlanState> left, Agg *node)
	: left_(std::move(left)),
	  node_(node),
	  memory_context_(CurrentMemoryContext),
	  grp_col_indices_(PgMemoryContextAllocator<int>(memory_context_)),
	  aggs_(PgMemoryContextAllocator<VecAggDesc>(memory_context_)),
	  hash_table_(0, VecGroupKeyHash{}, std::equal_to<VecGroupKey>{},
				 PgMemoryContextAllocator<std::pair<const VecGroupKey, VecAggAccumulatorList>>(memory_context_)),
	  fully_scanned_(false)
{
	for (int i = 0; i < node->numCols; i++) grp_col_indices_.push_back(node->grpColIdx[i] - 1);
	ListCell *lc;
	foreach(lc, node->plan.targetlist) {
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		VecAggDesc desc; desc.target_resno = tle->resno;
		desc.output_type = exprType((Node *) tle->expr);
		desc.output_storage = DefaultOutputStorageKindForType(desc.output_type);
		if (IsA(tle->expr, Aggref)) {
			Aggref *aggref = (Aggref *) tle->expr;
			char *aggname = get_func_name(aggref->aggfnoid);
			if (aggname && strcmp(aggname, "sum") == 0) desc.type = VecAggType::SUM;
			else if (aggname && strcmp(aggname, "count") == 0) desc.type = VecAggType::COUNT;
			else if (aggname && strcmp(aggname, "avg") == 0) desc.type = VecAggType::AVG;
			else desc.type = VecAggType::SUM;
			if (aggref->args != NIL) {
				TargetEntry *arg_tle = (TargetEntry *) linitial(aggref->args);
				desc.arg_type = exprType((Node *) arg_tle->expr);
				desc.arg_expr = std::make_unique<VecExprProgram>();
				CompileExpr((Expr *) arg_tle->expr, *desc.arg_expr, false);
				if (desc.arg_expr->get_final_res_idx() >= 0)
					desc.numeric_scale = desc.arg_expr->get_register_scale(desc.arg_expr->get_final_res_idx());
				desc.use_exact_numeric = ShouldUseExactNumericAgg(desc.arg_type) &&
					desc.arg_expr->get_final_res_idx() >= 0;
				if (desc.use_exact_numeric)
				{
					if (desc.type == VecAggType::AVG)
						desc.output_storage = VecOutputStorageKind::NumericAvgPair;
					else
						desc.output_storage = VecOutputStorageKind::NumericScaledInt64;
				}
				else if (desc.type == VecAggType::COUNT)
					desc.output_storage = VecOutputStorageKind::Int64;
				else if (desc.output_type == NUMERICOID)
					desc.output_storage = VecOutputStorageKind::Double;
			} else desc.arg_expr = nullptr;
		} else { desc.type = VecAggType::MAX; desc.arg_expr = nullptr; }
		aggs_.push_back(std::move(desc));
	}
}

void VecAggState::do_sink() {
	auto batch = std::make_unique<DataChunk<DEFAULT_CHUNK_SIZE>>();
	while (left_->get_next_batch(*batch)) {
		for (auto &agg : aggs_) if (agg.arg_expr) agg.arg_expr->evaluate(*batch);
		int n = batch->has_selection ? batch->sel.count : batch->count;
		for (int s = 0; s < n; s++) {
			int i = batch->has_selection ? batch->sel.row_ids[s] : s;
			VecGroupKey key; key.num_cols = (int)grp_col_indices_.size(); if (key.num_cols > 4) key.num_cols = 4;
			for (int k = 0; k < 4; k++) key.prefixes[k] = 0;
				for (int k = 0; k < key.num_cols; k++) {
					int idx = grp_col_indices_[k];
					if (idx >= 0 && idx < 16) key.prefixes[k] = batch->string_columns[idx][i].prefix;
				}
					auto it = hash_table_.find(key);
					if (it == hash_table_.end())
						it = hash_table_.emplace(key, VecAggAccumulatorList(PgMemoryContextAllocator<VecAggAccumulator>(memory_context_))).first;
					auto& accs = it->second; if (accs.empty()) accs.resize(aggs_.size());
					for (size_t a = 0; a < aggs_.size(); a++) {
						if (aggs_[a].type == VecAggType::COUNT) accs[a].count++;
						else if (aggs_[a].arg_expr) {
						int r = aggs_[a].arg_expr->final_res_idx;
						if (r >= 0 && !aggs_[a].arg_expr->get_nulls_reg(r)[i]) {
							if (aggs_[a].use_exact_numeric) {
								const int64_t* r64 = aggs_[a].arg_expr->get_int64_reg(r);
								accs[a].update_numeric(r64[i]);
							} else {
								double v;
								const int64_t* r64 = aggs_[a].arg_expr->get_int64_reg(r);
								const double* rf8 = aggs_[a].arg_expr->get_float8_reg(r);
								if (r64[i] != 0 || (rf8[i] == 0.0))
									v = (double)r64[i];
								else
									v = rf8[i];
								accs[a].update_float(v);
							}
						}
					}
				}
			}
	}
	fully_scanned_ = true; it_ = hash_table_.begin();
}

bool VecAggState::get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk) {
	if (!fully_scanned_) do_sink();
	chunk.reset();
	while (it_ != hash_table_.end() && chunk.count < DEFAULT_CHUNK_SIZE) {
		const auto& accs = it_->second; const auto& key = it_->first;
		for (size_t a = 0; a < aggs_.size(); a++) {
			int tidx = aggs_[a].target_resno - 1; if (tidx < 0 || tidx >= 16) continue;
			chunk.nulls[tidx][chunk.count] = 0;
				if (aggs_[a].arg_expr == nullptr && aggs_[a].type == VecAggType::MAX) {
					if (a < (size_t)key.num_cols) {
						chunk.string_columns[tidx][chunk.count].prefix = key.prefixes[a];
						chunk.string_columns[tidx][chunk.count].len = 1; 
					}
				} else {
					if (aggs_[a].type == VecAggType::AVG) {
						if (aggs_[a].use_exact_numeric) {
							chunk.int64_columns[tidx][chunk.count] =
								WideIntToInt64Checked(accs[a].numeric_sum, "aggregate numeric average sum");
							chunk.double_columns[tidx][chunk.count] = (double) accs[a].count;
						} else {
							chunk.double_columns[tidx][chunk.count] = accs[a].count > 0 ? (accs[a].float_sum / accs[a].count) : 0.0;
						}
					}
					else if (aggs_[a].type == VecAggType::COUNT) chunk.int64_columns[tidx][chunk.count] = accs[a].count;
					else { 
						if (aggs_[a].use_exact_numeric) {
							chunk.int64_columns[tidx][chunk.count] =
								WideIntToInt64Checked(accs[a].numeric_sum, "aggregate numeric sum");
						} else {
							chunk.double_columns[tidx][chunk.count] = accs[a].float_sum;
							chunk.int64_columns[tidx][chunk.count] = (int64_t)(accs[a].float_sum + (accs[a].float_sum >= 0 ? 0.5 : -0.5));
						}
					}
				}
			}
			chunk.count++; ++it_;
	}
	return chunk.count > 0;
}

bool VecAggState::lookup_numeric_output_meta(int target_resno, NumericOutputKind *kind, int *scale) const {
	for (const auto &agg : aggs_) {
		if (agg.target_resno != target_resno || !agg.use_exact_numeric)
			continue;
		if (kind != nullptr) {
			if (agg.type == VecAggType::AVG)
				*kind = NumericOutputKind::Avg;
			else if (agg.type == VecAggType::SUM)
				*kind = NumericOutputKind::Sum;
			else
				*kind = NumericOutputKind::None;
		}
		if (scale != nullptr)
			*scale = agg.numeric_scale;
		return true;
	}

	if (kind != nullptr)
		*kind = NumericOutputKind::None;
	if (scale != nullptr)
		*scale = 0;
	return false;
}

bool
VecAggState::lookup_output_col_meta(int target_resno, VecOutputColMeta *out) const
{
	for (const auto &agg : aggs_) {
		if (agg.target_resno != target_resno)
			continue;
		if (out != nullptr) {
			out->sql_type = agg.output_type;
			out->storage_kind = agg.output_storage;
			out->scale = agg.numeric_scale;
		}
		return true;
	}

	if (out != nullptr) {
		out->sql_type = InvalidOid;
		out->storage_kind = VecOutputStorageKind::Int32;
		out->scale = 0;
	}
	return false;
}

/* --- VecSeqScanState --- */
VecSeqScanState::VecSeqScanState(Relation rel, Snapshot snapshot, const DeformProgram *program)
	: rel_(rel), snapshot_(snapshot), deformer_(RelationGetDescr(rel), program) {
	/*
	 * We drive block iteration ourselves below. Letting heap_beginscan choose a
	 * synchronized-scan start block would skip the prefix blocks because this
	 * custom loop never wraps back around to block 0.
	 */
	scan_ = (HeapScanDesc) heap_beginscan(rel_, snapshot_, 0, NULL, NULL, SO_TYPE_SEQSCAN | SO_ALLOW_STRAT | SO_ALLOW_PAGEMODE);
	current_buf_ = InvalidBuffer;
	vmbuf_ = InvalidBuffer;
	current_offnum_ = FirstOffsetNumber;
	all_visible_ = false;
#ifdef USE_LLVM
	JitDeformFunc jf;
	const char *err;
	if (pg_volvec_jit_deform) {
		if (pg_volvec_try_compile_jit_deform_to_datachunk(RelationGetDescr(rel), program, &jf, &jit_context_, &err)) {
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
}

VecSeqScanState::~VecSeqScanState() { 
		if (pg_volvec_trace_hooks && jit_context_)
			elog(LOG, "pg_volvec: VecSeqScanState dtor releasing deform JIT context %p", (void *) jit_context_);
		if (BufferIsValid(current_buf_)) UnlockReleaseBuffer(current_buf_);
		if (BufferIsValid(vmbuf_)) ReleaseBuffer(vmbuf_);
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

bool VecSeqScanState::get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk) {
	chunk.reset();
	DeformBindings bindings;
	for (int i = 0; i < 16; i++) { bindings.columns_data[i] = chunk.int32_columns[i]; bindings.columns_nulls[i] = chunk.nulls[i]; }
	TupleDesc desc = RelationGetDescr(rel_);
	for (int i = 0; i < desc->natts && i < 16; i++) {
		Oid typid = TupleDescAttr(desc, i)->atttypid;
		if (typid == FLOAT8OID) bindings.columns_data[i] = chunk.double_columns[i];
		else if (typid == NUMERICOID || typid == INT8OID) bindings.columns_data[i] = chunk.int64_columns[i];
		else if (typid == BPCHAROID || typid == TEXTOID || typid == VARCHAROID) bindings.columns_data[i] = chunk.string_columns[i];
		else if (typid == DATEOID) bindings.columns_data[i] = chunk.int32_columns[i];
	}

	while (chunk.count < DEFAULT_CHUNK_SIZE) {
		if (current_buf_ == InvalidBuffer) {
			if (scan_->rs_cblock == InvalidBlockNumber) {
				scan_->rs_cblock = scan_->rs_startblock;
			} else {
				scan_->rs_cblock++;
			}

			if (scan_->rs_cblock >= scan_->rs_nblocks) break;

			current_buf_ = ReadBufferExtended(rel_, MAIN_FORKNUM, scan_->rs_cblock, RBM_NORMAL, scan_->rs_strategy);
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


bool VecFilterState::get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk) {
	while (left_->get_next_batch(chunk)) { 
		program_->evaluate(chunk); 
		if (chunk.sel.count > 0) return true; 
	}
	return false;
}

VecSortState::VecSortState(std::unique_ptr<VecPlanState> left, Sort *node,
						   VolVecVector<VecSortKeyDesc> key_descs)
	: left_(std::move(left)),
	  node_(node),
	  memory_context_(CurrentMemoryContext),
	  payload_chunks_(PgMemoryContextAllocator<DataChunk<DEFAULT_CHUNK_SIZE> *>(memory_context_)),
	  rows_(PgMemoryContextAllocator<VecRowRef>(memory_context_)),
	  key_descs_(PgMemoryContextAllocator<VecSortKeyDesc>(memory_context_)),
	  key_lanes_(PgMemoryContextAllocator<VecSortKeyLane>(memory_context_)),
	  emit_pos_(0),
	  output_ncols_(Min(list_length(node->plan.targetlist), 16)),
	  materialized_(false)
{
	for (const auto &key_desc : key_descs)
	{
		key_descs_.push_back(key_desc);
		key_lanes_.emplace_back(key_desc, memory_context_);
	}
}

VecSortState::~VecSortState()
{
	for (auto *chunk : payload_chunks_)
		delete chunk;
}

DataChunk<DEFAULT_CHUNK_SIZE> *
VecSortState::allocate_payload_chunk()
{
	MemoryContext old_context = MemoryContextSwitchTo(memory_context_);
	DataChunk<DEFAULT_CHUNK_SIZE> *chunk = new DataChunk<DEFAULT_CHUNK_SIZE>();
	MemoryContextSwitchTo(old_context);
	payload_chunks_.push_back(chunk);
	return chunk;
}

void
VecSortState::copy_row(const DataChunk<DEFAULT_CHUNK_SIZE> &src, int src_row,
					   DataChunk<DEFAULT_CHUNK_SIZE> &dst, int dst_row) const
{
	for (int col = 0; col < output_ncols_; col++)
	{
		dst.double_columns[col][dst_row] = src.double_columns[col][src_row];
		dst.int64_columns[col][dst_row] = src.int64_columns[col][src_row];
		dst.int32_columns[col][dst_row] = src.int32_columns[col][src_row];
		dst.string_columns[col][dst_row] = src.string_columns[col][src_row];
		dst.nulls[col][dst_row] = src.nulls[col][src_row];
	}
}

void
VecSortState::append_sort_key(uint32_t ordinal,
							  const DataChunk<DEFAULT_CHUNK_SIZE> &input,
							  int src_row)
{
	for (auto &lane : key_lanes_)
	{
		const VecSortKeyDesc &key = lane.desc;
		bool is_null = input.nulls[key.col_idx][src_row] != 0;

		Assert(lane.nulls.size() == ordinal);
		lane.nulls.push_back((uint8_t) is_null);
		if (is_null)
		{
			lane.i32_values.push_back(0);
			lane.i64_values.push_back(0);
			lane.u64_values.push_back(0);
			lane.string_values.push_back(VecStringRef{0, 0, 0});
			continue;
		}

		switch (key.storage_kind)
		{
			case VecOutputStorageKind::Int32:
				lane.i32_values.push_back(input.int32_columns[key.col_idx][src_row]);
				lane.i64_values.push_back(0);
				lane.u64_values.push_back(0);
				lane.string_values.push_back(VecStringRef{0, 0, 0});
				break;
			case VecOutputStorageKind::Int64:
			case VecOutputStorageKind::NumericScaledInt64:
				lane.i32_values.push_back(0);
				lane.i64_values.push_back(input.int64_columns[key.col_idx][src_row]);
				lane.u64_values.push_back(0);
				lane.string_values.push_back(VecStringRef{0, 0, 0});
				break;
			case VecOutputStorageKind::Double:
				lane.i32_values.push_back(0);
				lane.i64_values.push_back(0);
				lane.u64_values.push_back(EncodeFloat8SortKey(input.double_columns[key.col_idx][src_row]));
				lane.string_values.push_back(VecStringRef{0, 0, 0});
				break;
			case VecOutputStorageKind::StringRef:
			{
				VecStringRef ref = input.string_columns[key.col_idx][src_row];

				if (ref.len > 8)
					elog(ERROR, "pg_volvec vector sort currently supports string sort keys up to 8 bytes");
				lane.i32_values.push_back(0);
				lane.i64_values.push_back(0);
				lane.u64_values.push_back(0);
				lane.string_values.push_back(ref);
				break;
			}
			case VecOutputStorageKind::NumericAvgPair:
				elog(ERROR, "pg_volvec vector sort does not yet support numeric average sort keys");
				break;
		}
	}
}

void
VecSortState::append_batch(const DataChunk<DEFAULT_CHUNK_SIZE> &input)
{
	int active_count = input.has_selection ? input.sel.count : input.count;
	DataChunk<DEFAULT_CHUNK_SIZE> *dst =
		payload_chunks_.empty() ? allocate_payload_chunk() : payload_chunks_.back();

	for (int s = 0; s < active_count; s++)
	{
		int src_row = input.has_selection ? input.sel.row_ids[s] : s;
		int dst_row;
		uint32_t ordinal;

		if (dst->count >= DEFAULT_CHUNK_SIZE)
			dst = allocate_payload_chunk();

		dst_row = dst->count;
		ordinal = (uint32_t) rows_.size();
		copy_row(input, src_row, *dst, dst_row);
		rows_.push_back(VecRowRef{ordinal, (uint32_t) (payload_chunks_.size() - 1), (uint16_t) dst_row});
		append_sort_key(ordinal, input, src_row);
		dst->count++;
	}
}

int
VecSortState::compare_string_ref(const VecStringRef &left, const VecStringRef &right) const
{
	int cmp_len = Min((int) left.len, (int) right.len);
	int cmp;

	if (left.len > 8 || right.len > 8)
		elog(ERROR, "pg_volvec vector sort currently supports string sort keys up to 8 bytes");

	cmp = memcmp(&left.prefix, &right.prefix, cmp_len);
	if (cmp < 0)
		return -1;
	if (cmp > 0)
		return 1;
	if (left.len < right.len)
		return -1;
	if (left.len > right.len)
		return 1;
	return 0;
}

bool
VecSortState::row_less(const VecRowRef &left, const VecRowRef &right) const
{
	for (const auto &lane : key_lanes_)
	{
		bool left_null = lane.nulls[left.ordinal] != 0;
		bool right_null = lane.nulls[right.ordinal] != 0;
		int cmp = 0;

		if (left_null != right_null)
			return lane.desc.nulls_first ? left_null : !left_null;
		if (left_null)
			continue;

		switch (lane.desc.storage_kind)
		{
			case VecOutputStorageKind::Int32:
				if (lane.i32_values[left.ordinal] < lane.i32_values[right.ordinal])
					cmp = -1;
				else if (lane.i32_values[left.ordinal] > lane.i32_values[right.ordinal])
					cmp = 1;
				break;
			case VecOutputStorageKind::Int64:
			case VecOutputStorageKind::NumericScaledInt64:
				if (lane.i64_values[left.ordinal] < lane.i64_values[right.ordinal])
					cmp = -1;
				else if (lane.i64_values[left.ordinal] > lane.i64_values[right.ordinal])
					cmp = 1;
				break;
			case VecOutputStorageKind::Double:
				if (lane.u64_values[left.ordinal] < lane.u64_values[right.ordinal])
					cmp = -1;
				else if (lane.u64_values[left.ordinal] > lane.u64_values[right.ordinal])
					cmp = 1;
				break;
			case VecOutputStorageKind::StringRef:
				cmp = compare_string_ref(lane.string_values[left.ordinal],
										 lane.string_values[right.ordinal]);
				break;
			case VecOutputStorageKind::NumericAvgPair:
				elog(ERROR, "pg_volvec vector sort does not yet support numeric average sort keys");
				break;
		}

		if (cmp != 0)
			return lane.desc.descending ? (cmp > 0) : (cmp < 0);
	}

	return left.ordinal < right.ordinal;
}

void
VecSortState::materialize_and_sort()
{
	DataChunk<DEFAULT_CHUNK_SIZE> input;

	if (materialized_)
		return;

	while (left_->get_next_batch(input))
		append_batch(input);

	std::stable_sort(rows_.begin(), rows_.end(),
					 [this](const VecRowRef &left, const VecRowRef &right)
					 {
						 return row_less(left, right);
					 });
	emit_pos_ = 0;
	materialized_ = true;
}

bool
VecSortState::get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk)
{
	if (!materialized_)
		materialize_and_sort();

	chunk.reset();
	while (emit_pos_ < rows_.size() && chunk.count < DEFAULT_CHUNK_SIZE)
	{
		const VecRowRef &row = rows_[emit_pos_];
		const DataChunk<DEFAULT_CHUNK_SIZE> *src = payload_chunks_[row.chunk_idx];

		copy_row(*src, row.row_idx, chunk, chunk.count);
		chunk.count++;
		emit_pos_++;
	}

	return chunk.count > 0;
}

static bool
BuildSortKeyDescs(Sort *sort_node, VecPlanState *child,
				  VolVecVector<VecSortKeyDesc> *out_keys)
{
	for (int i = 0; i < sort_node->numCols; i++)
	{
		VecOutputColMeta meta;
		VecSortKeyDesc key_desc;
		Oid opfamily = InvalidOid;
		Oid opcintype = InvalidOid;
		CompareType cmptype = COMPARE_INVALID;
		int target_resno = sort_node->sortColIdx[i];

		if (target_resno <= 0 || target_resno > 16)
			return false;
		if (child == nullptr || !child->lookup_output_col_meta(target_resno, &meta))
			return false;
		if (!get_ordering_op_properties(sort_node->sortOperators[i], &opfamily, &opcintype, &cmptype))
			return false;
		(void) opfamily;
		(void) opcintype;
		if (meta.storage_kind == VecOutputStorageKind::NumericAvgPair)
			return false;
		if (meta.storage_kind == VecOutputStorageKind::StringRef &&
			meta.sql_type != BPCHAROID)
			return false;
		if (cmptype != COMPARE_LT && cmptype != COMPARE_GT)
			return false;

		key_desc.col_idx = (uint16_t) (target_resno - 1);
		key_desc.sql_type = meta.sql_type;
		key_desc.storage_kind = meta.storage_kind;
		key_desc.descending = (cmptype == COMPARE_GT);
		key_desc.nulls_first = sort_node->nullsFirst[i];
		key_desc.collation = sort_node->collations[i];
		key_desc.scale = meta.scale;
		out_keys->push_back(key_desc);
	}

	return true;
}

static std::unique_ptr<VecPlanState>
ExecInitVecPlanInternal(Plan *plan, EState *estate, Bitmapset *required_attrs)
{
	if (plan == NULL) return nullptr;
	if (required_attrs == nullptr)
		CollectRequiredAttrsForPlan(plan, &required_attrs);
	std::unique_ptr<VecPlanState> current_state = nullptr;
	if (IsA(plan, Sort)) {
		VolVecVector<VecSortKeyDesc> key_descs{PgMemoryContextAllocator<VecSortKeyDesc>(CurrentMemoryContext)};
		auto left = ExecInitVecPlanInternal(plan->lefttree, estate, required_attrs);
		if (!left)
			return nullptr;
		if (!BuildSortKeyDescs((Sort *) plan, left.get(), &key_descs))
			return nullptr;
		current_state = std::make_unique<VecSortState>(std::move(left), (Sort *) plan, std::move(key_descs));
	} else if (IsA(plan, Agg)) {
		auto left = ExecInitVecPlanInternal(plan->lefttree, estate, required_attrs);
		if (!left) return nullptr;
		current_state = std::make_unique<VecAggState>(std::move(left), (Agg *) plan);
	} else if (IsA(plan, SeqScan)) {
		SeqScan *sscan = (SeqScan *) plan;
		Oid relid = exec_rt_fetch(sscan->scan.scanrelid, estate)->relid;
		Relation rel = table_open(relid, NoLock);
		DeformProgram prog;
		TupleDesc desc = RelationGetDescr(rel);
		BuildPrunedDeformProgram(required_attrs, desc, &prog);
			current_state = std::make_unique<VecSeqScanState>(rel, estate->es_snapshot, &prog);
	}
	if (current_state && plan->qual != NIL) {
		auto program = std::make_unique<VecExprProgram>();
		Expr *combined_qual = (Expr *) make_ands_explicit(plan->qual);
		CompileExpr(combined_qual, *program, true);
		if (program->get_final_res_idx() < 0)
			return nullptr;
		current_state = std::make_unique<VecFilterState>(std::move(current_state), std::move(program));
	}
	return current_state;
}

std::unique_ptr<VecPlanState>
ExecInitVecPlan(Plan *plan, EState *estate)
{
	return ExecInitVecPlanInternal(plan, estate, nullptr);
}

} /* namespace pg_volvec */

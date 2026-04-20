#include "exec/internal.hpp"

namespace pg_volvec {

/* --- VecAggState --- */
VecAggState::VecAggState(std::unique_ptr<VecPlanState> left, Agg *node)
		: left_(std::move(left)),
		  node_(node),
		  memory_context_(CurrentMemoryContext),
		  grp_col_indices_(PgMemoryContextAllocator<int>(memory_context_)),
		  grp_col_meta_(PgMemoryContextAllocator<VecOutputColMeta>(memory_context_)),
		  aggs_(PgMemoryContextAllocator<VecAggDesc>(memory_context_)),
		  hash_table_(0, VecGroupKeyHash{}, std::equal_to<VecGroupKey>{},
					 PgMemoryContextAllocator<std::pair<const VecGroupKey, VecAggGroupState>>(memory_context_)),
		  simple_hash_table_(0, VecSimpleGroupKeyHash{}, std::equal_to<VecSimpleGroupKey>{},
							PgMemoryContextAllocator<std::pair<const VecSimpleGroupKey, VecAggGroupState>>(memory_context_)),
		  rep_chunks_(PgMemoryContextAllocator<DataChunk<DEFAULT_CHUNK_SIZE> *>(memory_context_)),
		  fully_scanned_(false)
{
		for (int i = 0; i < node->numCols; i++) {
			VecOutputColMeta meta;
			int target_resno = node->grpColIdx[i];

			grp_col_indices_.push_back(target_resno - 1);
			if (left_ == nullptr || !left_->lookup_output_col_meta(target_resno, &meta))
			{
				meta.sql_type = InvalidOid;
				meta.storage_kind = VecOutputStorageKind::Int32;
				meta.scale = 0;
			}
			grp_col_meta_.push_back(meta);
		}
		if (grp_col_indices_.size() == 1 &&
			(grp_col_meta_[0].storage_kind == VecOutputStorageKind::Int32 ||
			 grp_col_meta_[0].storage_kind == VecOutputStorageKind::Int64 ||
			 grp_col_meta_[0].storage_kind == VecOutputStorageKind::NumericScaledInt64))
		{
			use_simple_group_key_ = true;
			simple_group_storage_ = grp_col_meta_[0].storage_kind;
		}
		if (node != nullptr && node->plan.plan_rows > 0)
		{
			double estimated_groups = node->plan.plan_rows;
			size_t reserve_count;

			if (estimated_groups > (double) (SIZE_MAX / 2))
				reserve_count = SIZE_MAX / 2;
			else
				reserve_count = (size_t) estimated_groups + 1;
			if (use_simple_group_key_)
				simple_hash_table_.reserve(reserve_count);
			else
				hash_table_.reserve(reserve_count);
		}
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
			else if (aggname && strcmp(aggname, "max") == 0) desc.type = VecAggType::MAX;
			else desc.type = VecAggType::SUM;
			desc.is_distinct = (aggref->aggdistinct != NIL);
			if (aggref->args != NIL) {
				TargetEntry *arg_tle = (TargetEntry *) linitial(aggref->args);
				Expr *compiled_arg_expr = (Expr *) arg_tle->expr;

				if (node->plan.lefttree != nullptr &&
					node->plan.lefttree->targetlist != NIL)
				{
					Expr *rewritten_arg_expr =
						RewriteExprAgainstTargetList((Expr *) arg_tle->expr,
													 node->plan.lefttree->targetlist);

					if (rewritten_arg_expr != nullptr)
						compiled_arg_expr = rewritten_arg_expr;
				}
				desc.arg_type = exprType((Node *) arg_tle->expr);
				if (pg_volvec_trace_hooks && desc.is_distinct)
				{
					Expr *arg_expr = StripImplicitNodesLocal((Expr *) arg_tle->expr);

					if (arg_expr != nullptr && IsA(arg_expr, Var))
						elog(LOG,
							 "pg_volvec: distinct agg arg varattno=%d vartype=%u",
							 ((Var *) arg_expr)->varattno,
							 ((Var *) arg_expr)->vartype);
					else
						elog(LOG,
							 "pg_volvec: distinct agg arg expr node=%d type=%u",
							 arg_expr != nullptr ? (int) nodeTag(arg_expr) : -1,
							 desc.arg_type);
				}
				desc.arg_expr = std::make_unique<VecExprProgram>();
				CompileExpr(compiled_arg_expr, *desc.arg_expr, false);
				AdjustProgramVarScales(desc.arg_expr.get(), left_.get());
				if (desc.arg_expr->get_final_res_idx() >= 0)
				{
					Expr *arg_expr = StripImplicitNodesLocal((Expr *) arg_tle->expr);

					if (arg_expr != nullptr && IsA(arg_expr, Var) && left_ != nullptr)
					{
						VecOutputColMeta input_meta;
						Var *var = (Var *) arg_expr;

						if (left_->lookup_output_col_meta(var->varattno, &input_meta) &&
							input_meta.storage_kind == VecOutputStorageKind::NumericScaledInt64)
							desc.arg_expr->set_register_scale(desc.arg_expr->get_final_res_idx(),
															  input_meta.scale);
					}
				}
				if (desc.arg_expr->get_final_res_idx() >= 0)
				{
					desc.numeric_scale = desc.arg_expr->get_register_scale(desc.arg_expr->get_final_res_idx());
					desc.numeric_precision = desc.arg_expr->get_register_precision(desc.arg_expr->get_final_res_idx());
					desc.numeric_width = desc.arg_expr->get_register_numeric_width(desc.arg_expr->get_final_res_idx());
				}
				desc.use_exact_numeric = ShouldUseExactNumericAgg(desc.arg_type) &&
					desc.arg_expr->get_final_res_idx() >= 0;
				/*
				 * Exact numeric aggregate arguments currently rely on the i64 register
				 * path carrying scaled integer values. Keep them on the interpreter path
				 * for now until the expr JIT exact-numeric result path is fully
				 * validated, otherwise grouped SUM/AVG can observe zeroed i64 outputs.
				 */
				if (desc.use_exact_numeric &&
					desc.arg_expr->jit_context != nullptr)
				{
					pg_volvec_release_llvm_jit_context((JitContext *) desc.arg_expr->jit_context);
					desc.arg_expr->jit_context = nullptr;
					desc.arg_expr->jit_func = nullptr;
				}
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
			} else {
				Expr *expr = StripImplicitNodesLocal((Expr *) tle->expr);
				desc.type = VecAggType::MAX;
				desc.arg_expr = nullptr;
				if (expr != nullptr && IsA(expr, Var))
				{
					Var *var = (Var *) expr;
					VecOutputColMeta input_meta;
					desc.input_col = var->varattno - 1;
					if (left_ != nullptr && left_->lookup_output_col_meta(var->varattno, &input_meta))
					{
						desc.output_storage = input_meta.storage_kind;
						desc.numeric_scale = input_meta.scale;
					}

					for (int g = 0; g < node->numCols; g++)
					{
						if (node->grpColIdx[g] == var->varattno)
						{
							desc.group_key_pos = g;
							break;
						}
					}
				}
				else if (ResolveAggPassThroughExpr(node, (Expr *) tle->expr,
												  &desc.input_col, &desc.group_key_pos))
				{
					VecOutputColMeta input_meta;

					if (left_ != nullptr && left_->lookup_output_col_meta(desc.input_col + 1, &input_meta))
					{
						desc.output_storage = input_meta.storage_kind;
						desc.numeric_scale = input_meta.scale;
					}
				}
			}
			aggs_.push_back(std::move(desc));
			if (pg_volvec_trace_hooks)
			{
				const auto &trace_desc = aggs_.back();

				elog(LOG,
					 "pg_volvec: agg desc target_resno=%d type=%u output_type=%u output_storage=%u use_exact_numeric=%s numeric_scale=%d numeric_precision=%d numeric_width=%u arg_expr=%s input_col=%d group_key_pos=%d distinct=%s",
					 trace_desc.target_resno,
					 (unsigned) trace_desc.type,
					 trace_desc.output_type,
					 (unsigned) trace_desc.output_storage,
					 trace_desc.use_exact_numeric ? "on" : "off",
					 trace_desc.numeric_scale,
					 trace_desc.numeric_precision,
					 (unsigned) trace_desc.numeric_width,
					 trace_desc.arg_expr != nullptr ? "on" : "off",
					 trace_desc.input_col,
					 trace_desc.group_key_pos,
					 trace_desc.is_distinct ? "on" : "off");
				if (trace_desc.arg_expr != nullptr)
				{
					for (size_t step_idx = 0; step_idx < trace_desc.arg_expr->steps.size(); step_idx++)
					{
						const auto &step = trace_desc.arg_expr->steps[step_idx];

						elog(LOG,
							 "pg_volvec: agg expr step[%zu] target_resno=%d opcode=%u res=%d left=%d right=%d scale=%d",
							 step_idx,
							 trace_desc.target_resno,
							 (unsigned) step.opcode,
							 step.res_idx,
							 step.d.op.left,
							 step.d.op.right,
							 trace_desc.arg_expr->get_register_scale(step.res_idx));
					}
				}
				}
			}
	}

VecAggState::~VecAggState()
{
	for (auto *chunk : rep_chunks_)
		delete chunk;
}

DataChunk<DEFAULT_CHUNK_SIZE> *
VecAggState::allocate_rep_chunk()
{
	MemoryContext old_context = MemoryContextSwitchTo(memory_context_);
	DataChunk<DEFAULT_CHUNK_SIZE> *chunk = new DataChunk<DEFAULT_CHUNK_SIZE>();
	MemoryContextSwitchTo(old_context);
	rep_chunks_.push_back(chunk);
	return chunk;
}

void
VecAggState::copy_rep_row(DataChunk<DEFAULT_CHUNK_SIZE> &dst, int dst_row,
						  const DataChunk<DEFAULT_CHUNK_SIZE> &src, int src_row) const
{
	for (const auto &agg : aggs_)
	{
		int out_col = agg.target_resno - 1;
		int src_col = agg.input_col;

		if (agg.arg_expr != nullptr || out_col < 0 || out_col >= 16 || src_col < 0 || src_col >= 16)
			continue;
		dst.nulls[out_col][dst_row] = src.nulls[src_col][src_row];
		if (dst.nulls[out_col][dst_row])
			continue;
		switch (agg.output_storage)
		{
			case VecOutputStorageKind::Double:
				dst.double_columns[out_col][dst_row] = src.double_columns[src_col][src_row];
				break;
			case VecOutputStorageKind::Int64:
			case VecOutputStorageKind::NumericScaledInt64:
			case VecOutputStorageKind::NumericAvgPair:
				dst.int64_columns[out_col][dst_row] = src.int64_columns[src_col][src_row];
				dst.double_columns[out_col][dst_row] = src.double_columns[src_col][src_row];
				break;
			case VecOutputStorageKind::StringRef:
				dst.string_columns[out_col][dst_row] =
					CopyStringRefToChunk(dst, src, src.string_columns[src_col][src_row]);
				break;
			case VecOutputStorageKind::Int32:
			default:
				dst.int32_columns[out_col][dst_row] = src.int32_columns[src_col][src_row];
				break;
		}
	}
}

void
VecAggState::export_partial_accumulator(const VecAggAccumulator *src,
										 ParallelAggPartialAccumulator *dst) const
{
	if (dst == nullptr)
		return;
	memset(dst, 0, sizeof(*dst));
	if (src == nullptr)
		return;
	dst->float_sum = src->float_sum;
	dst->numeric_sum_lo = WideIntLow64(src->numeric_sum);
	dst->numeric_sum_hi = WideIntHigh64(src->numeric_sum);
	dst->numeric_max_lo = WideIntLow64(src->numeric_max);
	dst->numeric_max_hi = WideIntHigh64(src->numeric_max);
	dst->float_max = src->float_max;
	dst->int64_max = src->int64_max;
	dst->int32_max = src->int32_max;
	dst->count = src->count;
	dst->has_value = src->has_value ? 1 : 0;
}

bool
VecAggState::is_supported_parallel_distinct_agg(const VecAggDesc &agg) const
{
	if (!agg.is_distinct)
		return true;
	if (agg.type != VecAggType::COUNT || agg.arg_expr == nullptr)
		return false;
	return agg.arg_type == INT2OID ||
		agg.arg_type == INT4OID ||
		agg.arg_type == INT8OID ||
		agg.arg_type == DATEOID;
}

VecAggState::VecAggAccumulator::DistinctValueSet *
VecAggState::ensure_distinct_value_set(VecAggAccumulator *acc) const
{
	MemoryContext old_context;

	if (acc == nullptr)
		return nullptr;
	if (acc->distinct_values != nullptr)
		return acc->distinct_values;
	old_context = MemoryContextSwitchTo(memory_context_);
	acc->distinct_values =
		new VecAggAccumulator::DistinctValueSet(
			0,
			std::hash<int64_t>{},
			std::equal_to<int64_t>{},
			PgMemoryContextAllocator<std::pair<const int64_t, char>>(memory_context_));
	MemoryContextSwitchTo(old_context);
	return acc->distinct_values;
}

bool
VecAggState::write_distinct_values_to_partial_file(BufFile *file,
												   const VecAggAccumulator *acc) const
{
	uint32_t distinct_count = 0;

	if (acc != nullptr && acc->distinct_values != nullptr)
	{
		if (acc->distinct_values->size() > UINT32_MAX)
			return false;
		distinct_count = (uint32_t) acc->distinct_values->size();
	}
	if (!BufFileWriteAllLocal(file, &distinct_count, sizeof(distinct_count)))
		return false;
	if (acc == nullptr || acc->distinct_values == nullptr)
		return true;
	for (const auto &entry : *acc->distinct_values)
	{
		int64_t value = entry.first;

		if (!BufFileWriteAllLocal(file, &value, sizeof(value)))
			return false;
	}
	return true;
}

bool
VecAggState::read_distinct_values_from_partial_file(BufFile *file,
													VecAggAccumulator *acc) const
{
	VecAggAccumulator::DistinctValueSet *values;
	uint32_t distinct_count = 0;

	if (!BufFileReadAllLocal(file, &distinct_count, sizeof(distinct_count), false))
		return false;
	if (distinct_count == 0)
		return true;
	values = ensure_distinct_value_set(acc);
	if (values == nullptr)
		return false;
	for (uint32_t i = 0; i < distinct_count; i++)
	{
		int64_t value;

		if (!BufFileReadAllLocal(file, &value, sizeof(value), false))
			return false;
		if (values->emplace(value, 1).second)
			acc->count++;
	}
	return true;
}

void
VecAggState::merge_partial_accumulator(const ParallelAggPartialAccumulator &src,
										size_t agg_index,
										VecAggAccumulator *dst) const
{
	const VecAggDesc &agg = aggs_[agg_index];
	NumericWideInt src_numeric = MakeWideIntBits(src.numeric_sum_lo,
												 (uint64_t) src.numeric_sum_hi);
	NumericWideInt src_numeric_max = MakeWideIntBits(src.numeric_max_lo,
													 (uint64_t) src.numeric_max_hi);

	if (dst == nullptr)
		return;
	switch (agg.type)
	{
		case VecAggType::COUNT:
			dst->count += src.count;
			break;
		case VecAggType::SUM:
			if (agg.use_exact_numeric)
				dst->numeric_sum += src_numeric;
			else
				dst->float_sum += src.float_sum;
			dst->count += src.count;
			break;
		case VecAggType::AVG:
			if (agg.use_exact_numeric)
				dst->numeric_sum += src_numeric;
			else
				dst->float_sum += src.float_sum;
			dst->count += src.count;
			break;
		case VecAggType::MAX:
			if (!src.has_value)
				break;
			if (!dst->has_value)
			{
				dst->numeric_max = src_numeric_max;
				dst->float_max = src.float_max;
				dst->int64_max = src.int64_max;
				dst->int32_max = src.int32_max;
				dst->has_value = true;
				break;
			}
			if (agg.use_exact_numeric)
				dst->numeric_max = Max(dst->numeric_max, src_numeric_max);
			else if (agg.output_storage == VecOutputStorageKind::Int64 ||
				agg.output_storage == VecOutputStorageKind::NumericScaledInt64 ||
				agg.output_storage == VecOutputStorageKind::NumericAvgPair)
				dst->int64_max = Max(dst->int64_max, src.int64_max);
			else if (agg.output_storage == VecOutputStorageKind::Double)
				dst->float_max = Max(dst->float_max, src.float_max);
			else
				dst->int32_max = Max(dst->int32_max, src.int32_max);
			dst->has_value = true;
			break;
	}
}

void
VecAggState::ensure_group_rep_row(VecAggGroupState *group,
								  const DataChunk<DEFAULT_CHUNK_SIZE> &batch,
								  int row_idx)
{
	DataChunk<DEFAULT_CHUNK_SIZE> *rep_chunk =
		rep_chunks_.empty() ? allocate_rep_chunk() : rep_chunks_.back();

	group->accs = VecAggAccumulatorList(PgMemoryContextAllocator<VecAggAccumulator>(memory_context_));
	if (rep_chunk->count >= DEFAULT_CHUNK_SIZE)
		rep_chunk = allocate_rep_chunk();
	group->rep_chunk_idx = (uint32_t) (rep_chunks_.size() - 1);
	group->rep_row_idx = (uint16_t) rep_chunk->count;
	group->has_rep_row = true;
	copy_rep_row(*rep_chunk, rep_chunk->count, batch, row_idx);
	rep_chunk->count++;
}

void
VecAggState::store_group_rep_row_from_partial(VecAggGroupState *group,
											   const ParallelAggPartialGroupEntry &entry)
{
	DataChunk<DEFAULT_CHUNK_SIZE> *rep_chunk =
		rep_chunks_.empty() ? allocate_rep_chunk() : rep_chunks_.back();
	int rep_row;

	if (group == nullptr || group->has_rep_row)
		return;
	if (rep_chunk->count >= DEFAULT_CHUNK_SIZE)
		rep_chunk = allocate_rep_chunk();
	rep_row = rep_chunk->count;
	group->rep_chunk_idx = (uint32_t) (rep_chunks_.size() - 1);
	group->rep_row_idx = (uint16_t) rep_row;
	group->has_rep_row = true;
	for (const auto &agg : aggs_)
	{
		int out_col = agg.target_resno - 1;
		const ParallelAggPartialGroupKeyCol *col;

		if (agg.arg_expr != nullptr || agg.group_key_pos < 0 ||
			agg.group_key_pos >= (int) entry.num_group_cols ||
			out_col < 0 || out_col >= 16)
			continue;
		col = &entry.group_cols[agg.group_key_pos];
		rep_chunk->nulls[out_col][rep_row] = col->is_null;
		if (col->is_null)
			continue;
		switch (agg.output_storage)
		{
			case VecOutputStorageKind::StringRef:
			{
				char buf[8] = {0};

				if (col->string_len > sizeof(buf))
					elog(ERROR, "pg_volvec grouped partial string key too long: %u",
						 col->string_len);
				memcpy(buf, &col->value_bits, col->string_len);
				rep_chunk->string_columns[out_col][rep_row] =
					rep_chunk->store_string_bytes(buf, col->string_len);
				break;
			}
			case VecOutputStorageKind::Int64:
			case VecOutputStorageKind::NumericScaledInt64:
			case VecOutputStorageKind::NumericAvgPair:
				rep_chunk->int64_columns[out_col][rep_row] = (int64_t) col->value_bits;
				rep_chunk->double_columns[out_col][rep_row] = (double) ((int64_t) col->value_bits);
				break;
			case VecOutputStorageKind::Double:
			{
				double value;

				memcpy(&value, &col->value_bits, sizeof(value));
				rep_chunk->double_columns[out_col][rep_row] = value;
				break;
			}
			case VecOutputStorageKind::Int32:
			default:
				rep_chunk->int32_columns[out_col][rep_row] = (int32_t) col->value_bits;
				break;
		}
	}
	rep_chunk->count++;
}

void
VecAggState::update_group_accumulators(VecAggGroupState *group,
									   const DataChunk<DEFAULT_CHUNK_SIZE> &batch,
									   int row_idx)
{
	auto &accs = group->accs;

	if (accs.empty())
		accs.resize(aggs_.size());
	for (size_t a = 0; a < aggs_.size(); a++) {
		if (aggs_[a].type == VecAggType::COUNT) {
			if (!aggs_[a].is_distinct)
			{
				if (!aggs_[a].arg_expr)
				{
					accs[a].count++;
					continue;
				}

				int r = aggs_[a].arg_expr->final_res_idx;

				if (r >= 0 && !aggs_[a].arg_expr->get_nulls_reg(r)[row_idx])
					accs[a].count++;
				continue;
			}
			if (!aggs_[a].arg_expr)
				continue;
			int r = aggs_[a].arg_expr->final_res_idx;
			if (r < 0 || aggs_[a].arg_expr->get_nulls_reg(r)[row_idx])
				continue;

			int64_t distinct_value;

			if (aggs_[a].use_exact_numeric || aggs_[a].arg_type == INT8OID)
				distinct_value = aggs_[a].arg_expr->get_int64_reg(r)[row_idx];
			else if (aggs_[a].arg_type == INT4OID ||
					 aggs_[a].arg_type == INT2OID ||
					 aggs_[a].arg_type == DATEOID)
				distinct_value = (int64_t) aggs_[a].arg_expr->get_int32_reg(r)[row_idx];
			else
				continue;

			if (accs[a].distinct_values == nullptr)
				ensure_distinct_value_set(&accs[a]);
			if (accs[a].distinct_values->emplace(distinct_value, 1).second)
			{
				accs[a].count++;
				if (pg_volvec_trace_hooks)
				{
					static int distinct_trace_count = 0;

					if (distinct_trace_count < 20)
					{
						elog(LOG,
							 "pg_volvec: distinct agg accepted value=%lld count=%lld",
							 (long long) distinct_value,
							 (long long) accs[a].count);
						distinct_trace_count++;
					}
				}
			}
		}
		else if (aggs_[a].arg_expr) {
			int r = aggs_[a].arg_expr->final_res_idx;
			if (r >= 0 && !aggs_[a].arg_expr->get_nulls_reg(r)[row_idx]) {
				if (aggs_[a].type == VecAggType::MAX) {
					if (aggs_[a].use_exact_numeric) {
						accs[a].update_max_numeric(
							aggs_[a].arg_expr->get_wide_int_reg_value(r, row_idx));
					} else if (aggs_[a].output_storage == VecOutputStorageKind::Int64 ||
						aggs_[a].output_storage == VecOutputStorageKind::NumericScaledInt64 ||
						aggs_[a].output_storage == VecOutputStorageKind::NumericAvgPair) {
						const int64_t *r64 = aggs_[a].arg_expr->get_int64_reg(r);
						accs[a].update_max_int64(r64[row_idx]);
					} else if (aggs_[a].output_storage == VecOutputStorageKind::Double) {
						const double *rf8 = aggs_[a].arg_expr->get_float8_reg(r);
						accs[a].update_max_float(rf8[row_idx]);
					} else {
						const int32_t *r32 = aggs_[a].arg_expr->get_int32_reg(r);
						accs[a].update_max_int32(r32[row_idx]);
					}
				} else if (aggs_[a].use_exact_numeric) {
					NumericWideInt wide_value =
						aggs_[a].arg_expr->get_wide_int_reg_value(r, row_idx);
					if (pg_volvec_trace_hooks)
					{
						static int agg_numeric_input_trace_count = 0;

						if (agg_numeric_input_trace_count < 20)
						{
							const int64_t* r64 = aggs_[a].arg_expr->get_int64_reg(r);
							const int64_t* r64_hi = aggs_[a].arg_expr->get_int64_hi_reg(r);
							const double *rf8 = aggs_[a].arg_expr->get_float8_reg(r);

							elog(LOG,
								 "pg_volvec: agg numeric input target_resno=%d row=%d reg=%d scale=%d i64_lo=%lld i64_hi=%lld f8=%.10f",
								 aggs_[a].target_resno,
								 row_idx,
								 r,
								 aggs_[a].numeric_scale,
								 (long long) r64[row_idx],
								 (long long) r64_hi[row_idx],
								 rf8[row_idx]);
							agg_numeric_input_trace_count++;
						}
					}
					accs[a].update_numeric(wide_value);
				} else {
					double v;
					const int64_t* r64 = aggs_[a].arg_expr->get_int64_reg(r);
					const double* rf8 = aggs_[a].arg_expr->get_float8_reg(r);
					if (aggs_[a].output_storage == VecOutputStorageKind::Double &&
						aggs_[a].arg_type != INT2OID &&
						aggs_[a].arg_type != INT4OID &&
						aggs_[a].arg_type != INT8OID &&
						aggs_[a].arg_type != DATEOID)
						v = rf8[row_idx];
					else
						v = (double)r64[row_idx];
					accs[a].update_float(v);
				}
			}
		}
	}
}

bool
VecAggState::configure_input_block_range(BlockNumber start_block, uint32_t nblocks)
{
	bool ok = left_ != nullptr && left_->configure_source_block_range(start_block, nblocks);

	if (pg_volvec_trace_hooks && !ok)
		elog(LOG,
			 "pg_volvec: agg block range configure failed plan_node_id=%d start=%u nblocks=%u left=%s",
			 node_ != nullptr ? node_->plan.plan_node_id : -1,
			 start_block,
			 nblocks,
			 left_ != nullptr ? "ok" : "null");
	return ok;
}

void
VecAggState::clear_input_block_range()
{
	if (left_ != nullptr)
		left_->clear_source_block_range();
}

void
VecAggState::consume_batch(DataChunk<DEFAULT_CHUNK_SIZE> &batch)
{
	int n;

	for (auto &agg : aggs_)
	{
		if (agg.arg_expr)
		{
			agg.arg_expr->evaluate(batch);
			if (pg_volvec_trace_hooks && agg.use_exact_numeric)
			{
				static int exact_numeric_batch_trace_count = 0;

				if (exact_numeric_batch_trace_count < 4)
				{
					int trace_rows = Min(batch.count, 4);

					for (int row = 0; row < trace_rows; row++)
					{
						elog(LOG,
							 "pg_volvec: agg batch row=%d col1_i64=%lld col2_i64=%lld col1_i32=%d col2_i32=%d null1=%d null2=%d reg0=%lld reg1=%lld reg2=%lld reg3=%lld reg4=%lld",
							 row,
							 (long long) batch.int64_columns[1][row],
							 (long long) batch.int64_columns[2][row],
							 batch.int32_columns[1][row],
							 batch.int32_columns[2][row],
							 (int) batch.nulls[1][row],
							 (int) batch.nulls[2][row],
							 (long long) agg.arg_expr->get_int64_reg(0)[row],
							 (long long) agg.arg_expr->get_int64_reg(1)[row],
							 (long long) agg.arg_expr->get_int64_reg(2)[row],
							 (long long) agg.arg_expr->get_int64_reg(3)[row],
							 (long long) agg.arg_expr->get_int64_reg(4)[row]);
					}
					exact_numeric_batch_trace_count++;
				}
			}
		}
	}
	n = batch.has_selection ? batch.sel.count : batch.count;
	input_batches_consumed_++;
	input_rows_consumed_ += (uint64_t) n;
	for (int s = 0; s < n; s++) {
		int i = batch.has_selection ? batch.sel.row_ids[s] : s;
		if (use_simple_group_key_)
		{
			VecSimpleGroupKey key;
			int idx = grp_col_indices_[0];
			bool is_null = idx < 0 || idx >= 16 || batch.nulls[idx][i] != 0;

			key.is_null = is_null ? 1 : 0;
			if (!is_null)
			{
				if (simple_group_storage_ == VecOutputStorageKind::Int32)
					key.value = (int64_t) batch.int32_columns[idx][i];
				else
					key.value = batch.int64_columns[idx][i];
			}
			auto insert_result = simple_hash_table_.try_emplace(key);
			auto it = insert_result.first;

			if (insert_result.second)
				ensure_group_rep_row(&it->second, batch, i);
			update_group_accumulators(&it->second, batch, i);
		}
		else
		{
			VecGroupKey key;

			key.num_cols = (int) grp_col_indices_.size();
			if (key.num_cols > kMaxDeformTargets)
				key.num_cols = kMaxDeformTargets;
			for (int k = 0; k < key.num_cols; k++) {
				int idx = grp_col_indices_[k];
				const VecOutputColMeta &meta = grp_col_meta_[k];
				bool is_null;

				if (idx < 0 || idx >= 16)
				{
					key.is_null[k] = 1;
					key.values[k] = 0;
					key.aux[k] = 0;
					continue;
				}
				is_null = batch.nulls[idx][i] != 0;
				key.is_null[k] = is_null ? 1 : 0;
				if (is_null)
				{
					key.values[k] = 0;
					key.aux[k] = 0;
					continue;
				}
				switch (meta.storage_kind)
				{
					case VecOutputStorageKind::StringRef:
					{
						uint32_t key_len = 0;
						VecStringRef ref = batch.string_columns[idx][i];

						key.values[k] = HashStringRefForGroupKey(batch, ref, meta.sql_type, &key_len);
						key.aux[k] = key_len;
						break;
					}
					case VecOutputStorageKind::Int64:
					case VecOutputStorageKind::NumericScaledInt64:
					case VecOutputStorageKind::NumericAvgPair:
						key.values[k] = (uint64_t) batch.int64_columns[idx][i];
						key.aux[k] = 0;
						break;
					case VecOutputStorageKind::Double:
						memcpy(&key.values[k], &batch.double_columns[idx][i], sizeof(uint64_t));
						key.aux[k] = 0;
						break;
					case VecOutputStorageKind::Int32:
					default:
						key.values[k] = (uint64_t) (uint32_t) batch.int32_columns[idx][i];
						key.aux[k] = 0;
						break;
				}
			}
			auto insert_result = hash_table_.try_emplace(key);
			auto it = insert_result.first;

			if (insert_result.second)
				ensure_group_rep_row(&it->second, batch, i);
			update_group_accumulators(&it->second, batch, i);
		}
	}
}

void
VecAggState::consume_left_input()
{
	auto batch = std::make_unique<DataChunk<DEFAULT_CHUNK_SIZE>>();

	while (left_ != nullptr && left_->get_next_batch(*batch))
		consume_batch(*batch);
}

void
VecAggState::finish_sink()
{
	if (fully_scanned_)
		return;
	fully_scanned_ = true;
	if (grp_col_indices_.empty() && hash_table_.empty())
	{
		VecGroupKey key = {};
		auto insert_result = hash_table_.try_emplace(key);
		auto &accs = insert_result.first->second.accs;

		if (accs.empty())
			accs = VecAggAccumulatorList(PgMemoryContextAllocator<VecAggAccumulator>(memory_context_));
		accs.resize(aggs_.size());
	}
	if (use_simple_group_key_)
		simple_it_ = simple_hash_table_.begin();
	else
		it_ = hash_table_.begin();
}

bool
VecAggState::supports_parallel_partial_state() const
{
	if (!grp_col_indices_.empty())
	{
		if (grp_col_indices_.size() > VOLVEC_PARALLEL_MAX_GROUP_COLS)
		{
			if (pg_volvec_trace_hooks)
				elog(LOG,
					 "pg_volvec: parallel partial agg unsupported: group cols %zu exceed max %u",
					 grp_col_indices_.size(),
					 (unsigned) VOLVEC_PARALLEL_MAX_GROUP_COLS);
			return false;
		}
		for (const auto &meta : grp_col_meta_)
		{
			switch (meta.storage_kind)
			{
				case VecOutputStorageKind::Int32:
				case VecOutputStorageKind::Int64:
				case VecOutputStorageKind::NumericScaledInt64:
				case VecOutputStorageKind::StringRef:
					break;
				default:
					if (pg_volvec_trace_hooks)
						elog(LOG,
							 "pg_volvec: parallel partial agg unsupported: group col storage kind %u",
							 (unsigned) meta.storage_kind);
					return false;
			}
		}
	}
	if (!grp_col_indices_.empty())
	{
		for (const auto &agg : aggs_)
		{
			if (agg.arg_expr == nullptr &&
				agg.type != VecAggType::COUNT &&
				agg.group_key_pos < 0)
			{
				if (pg_volvec_trace_hooks)
					elog(LOG,
						 "pg_volvec: parallel partial agg unsupported: grouped agg target_resno=%d has no arg expr and no group passthrough",
						 agg.target_resno);
				return false;
			}
		}
	}
	for (const auto &agg : aggs_)
	{
		if (agg.is_distinct)
		{
			if (grp_col_indices_.empty() ||
				!is_supported_parallel_distinct_agg(agg))
			{
				if (pg_volvec_trace_hooks)
					elog(LOG,
						 "pg_volvec: parallel partial agg unsupported: DISTINCT target_resno=%d arg_type=%u grouped=%s",
						 agg.target_resno,
						 agg.arg_type,
						 grp_col_indices_.empty() ? "off" : "on");
				return false;
			}
		}
		if (agg.type != VecAggType::SUM &&
			agg.type != VecAggType::COUNT &&
			agg.type != VecAggType::AVG &&
			agg.type != VecAggType::MAX)
		{
			if (pg_volvec_trace_hooks)
				elog(LOG,
					 "pg_volvec: parallel partial agg unsupported: agg type %u target_resno=%d",
					 (unsigned) agg.type,
					 agg.target_resno);
			return false;
		}
		if (agg.arg_expr == nullptr &&
			agg.type != VecAggType::COUNT &&
			agg.group_key_pos < 0)
		{
			if (pg_volvec_trace_hooks)
				elog(LOG,
					 "pg_volvec: parallel partial agg unsupported: target_resno=%d has no arg expr and no group passthrough",
					 agg.target_resno);
			return false;
		}
	}
	if (pg_volvec_trace_hooks)
		elog(LOG,
			 "pg_volvec: parallel partial agg supported (groups=%zu aggs=%zu file_backed=%s plan_node_id=%d)",
			 grp_col_indices_.size(),
			 aggs_.size(),
			 uses_file_backed_parallel_partial_state() ? "on" : "off",
			 node_ != nullptr ? node_->plan.plan_node_id : -1);
	return true;
}

bool
VecAggState::export_parallel_partial_state(ParallelAggPartialState *out) const
{
	const VecAggAccumulatorList *accs = nullptr;

	if (out == nullptr || !supports_parallel_partial_state())
		return false;
	memset(out, 0, sizeof(*out));
	out->naggs = (uint32_t) aggs_.size();
	out->grouped = grp_col_indices_.empty() ? 0 : 1;
	out->input_batches = input_batches_consumed_;
	out->input_rows = input_rows_consumed_;
	if (!grp_col_indices_.empty())
	{
		if (use_simple_group_key_)
		{
			for (const auto &entry : simple_hash_table_)
			{
				ParallelAggPartialGroupEntry *dst_group;
				const VecAggAccumulatorList *src_accs = &entry.second.accs;

				if (out->group_count >= VOLVEC_PARALLEL_MAX_GROUPS)
					return false;
				dst_group = &out->groups[out->group_count++];
				memset(dst_group, 0, sizeof(*dst_group));
				dst_group->num_group_cols = 1;
				dst_group->group_cols[0].storage_kind = (uint8_t) simple_group_storage_;
				dst_group->group_cols[0].is_null = entry.first.is_null;
				dst_group->group_cols[0].value_bits = (uint64_t) entry.first.value;
				for (size_t a = 0; a < aggs_.size() && a < lengthof(dst_group->accs); a++)
				{
					const VecAggAccumulator *src =
						(src_accs != nullptr && a < src_accs->size()) ? &(*src_accs)[a] : nullptr;
					export_partial_accumulator(src, &dst_group->accs[a]);
				}
			}
			return true;
		}

		for (const auto &entry : hash_table_)
		{
			ParallelAggPartialGroupEntry *dst_group;
			const VecAggAccumulatorList *src_accs = &entry.second.accs;
			const DataChunk<DEFAULT_CHUNK_SIZE> *rep_chunk =
				entry.second.has_rep_row ? rep_chunks_[entry.second.rep_chunk_idx] : nullptr;
			int rep_row = entry.second.rep_row_idx;

			if (out->group_count >= VOLVEC_PARALLEL_MAX_GROUPS)
				return false;
			dst_group = &out->groups[out->group_count++];
			memset(dst_group, 0, sizeof(*dst_group));
			dst_group->num_group_cols = (uint32_t) Min(entry.first.num_cols, (int) VOLVEC_PARALLEL_MAX_GROUP_COLS);
			for (uint32_t k = 0; k < dst_group->num_group_cols; k++)
			{
				const VecOutputColMeta &meta = grp_col_meta_[k];
				ParallelAggPartialGroupKeyCol *dst_col = &dst_group->group_cols[k];

				dst_col->storage_kind = (uint8_t) meta.storage_kind;
				dst_col->is_null = entry.first.is_null[k];
				if (dst_col->is_null)
					continue;
				switch (meta.storage_kind)
				{
					case VecOutputStorageKind::StringRef:
					{
						const VecStringRef *ref = nullptr;
						const char *ptr = nullptr;
						uint32_t len = 0;

						if (rep_chunk == nullptr)
							return false;
						for (const auto &agg : aggs_)
						{
							int out_col = agg.target_resno - 1;

							if (agg.arg_expr == nullptr && agg.group_key_pos == (int) k &&
								out_col >= 0 && out_col < 16)
							{
								ref = &rep_chunk->string_columns[out_col][rep_row];
								break;
							}
						}
						if (ref == nullptr)
							return false;
						ptr = rep_chunk->get_string_ptr(*ref);
						len = ref->len;
						if (len > 8)
							return false;
						dst_col->string_len = len;
						memcpy(&dst_col->value_bits, ptr, len);
						break;
					}
					case VecOutputStorageKind::Int64:
					case VecOutputStorageKind::NumericScaledInt64:
					case VecOutputStorageKind::NumericAvgPair:
						dst_col->value_bits = entry.first.values[k];
						break;
					case VecOutputStorageKind::Int32:
					default:
						dst_col->value_bits = entry.first.values[k];
						break;
				}
			}
			for (size_t a = 0; a < aggs_.size() && a < lengthof(dst_group->accs); a++)
			{
				const VecAggAccumulator *src =
					(src_accs != nullptr && a < src_accs->size()) ? &(*src_accs)[a] : nullptr;
				export_partial_accumulator(src, &dst_group->accs[a]);
			}
		}
		return true;
	}
	if (!hash_table_.empty())
		accs = &hash_table_.begin()->second.accs;
	for (size_t a = 0; a < aggs_.size() && a < lengthof(out->accs); a++)
	{
		const VecAggAccumulator *src = (accs != nullptr && a < accs->size()) ? &(*accs)[a] : nullptr;
		export_partial_accumulator(src, &out->accs[a]);
	}
	return true;
}

bool
VecAggState::append_group_record_to_partial_file(BufFile *file,
												 const VecGroupKey *key,
												 const VecSimpleGroupKey *simple_key,
												 const VecAggGroupState &group) const
{
	uint32_t num_group_cols = simple_key != nullptr ? 1u : (uint32_t) grp_col_indices_.size();
	const DataChunk<DEFAULT_CHUNK_SIZE> *rep_chunk =
		group.has_rep_row ? rep_chunks_[group.rep_chunk_idx] : nullptr;
	int rep_row = group.rep_row_idx;

	if (!BufFileWriteAllLocal(file, &num_group_cols, sizeof(num_group_cols)))
		return false;
	for (uint32_t k = 0; k < num_group_cols; k++)
	{
		ParallelAggPartialGroupKeyCol col{};
		const VecOutputColMeta *meta = nullptr;

		if (simple_key != nullptr)
		{
			col.storage_kind = (uint8_t) simple_group_storage_;
			col.is_null = simple_key->is_null;
			col.value_bits = (uint64_t) simple_key->value;
		}
		else
		{
			const char *ptr = nullptr;
			uint32_t len = 0;

			meta = &grp_col_meta_[k];
			col.storage_kind = (uint8_t) meta->storage_kind;
			col.is_null = key->is_null[k];
			if (!col.is_null)
			{
				switch (meta->storage_kind)
				{
					case VecOutputStorageKind::StringRef:
					{
						const VecStringRef *ref = nullptr;

						if (rep_chunk == nullptr)
							return false;
						for (const auto &agg : aggs_)
						{
							int out_col = agg.target_resno - 1;

							if (agg.arg_expr == nullptr &&
								agg.group_key_pos == (int) k &&
								out_col >= 0 && out_col < 16)
							{
								ref = &rep_chunk->string_columns[out_col][rep_row];
								break;
							}
						}
						if (ref == nullptr)
							return false;
						ptr = rep_chunk->get_string_ptr(*ref);
						len = ref->len;
						col.string_len = len;
						break;
					}
					case VecOutputStorageKind::Int32:
						col.value_bits = (uint64_t) (uint32_t) key->values[k];
						break;
					case VecOutputStorageKind::Int64:
					case VecOutputStorageKind::NumericScaledInt64:
					case VecOutputStorageKind::NumericAvgPair:
					case VecOutputStorageKind::Double:
						col.value_bits = key->values[k];
						break;
				}
			}
			if (!BufFileWriteAllLocal(file, &col, sizeof(col)))
				return false;
			if (!col.is_null &&
				meta->storage_kind == VecOutputStorageKind::StringRef &&
				col.string_len > 0)
			{
				if (!BufFileWriteAllLocal(file, ptr, col.string_len))
					return false;
			}
			continue;
		}

		if (!BufFileWriteAllLocal(file, &col, sizeof(col)))
			return false;
	}

	for (size_t a = 0; a < aggs_.size(); a++)
	{
		ParallelAggPartialAccumulator acc{};
		const VecAggAccumulator *src =
			a < group.accs.size() ? &group.accs[a] : nullptr;

		export_partial_accumulator(src, &acc);
		if (!BufFileWriteAllLocal(file, &acc, sizeof(acc)))
			return false;
		if (aggs_[a].is_distinct &&
			!write_distinct_values_to_partial_file(file, src))
			return false;
	}
	return true;
}

bool
VecAggState::export_parallel_grouped_partial_file(BufFile *file,
												  ParallelAggPartialState *out) const
{
	GroupedPartialFileHeader header;

	if (file == nullptr || out == nullptr || !uses_file_backed_parallel_partial_state() ||
		!supports_parallel_partial_state())
		return false;

	memset(out, 0, sizeof(*out));
	out->naggs = (uint32_t) aggs_.size();
	out->grouped = 1;
	out->file_backed = 1;
	out->input_batches = input_batches_consumed_;
	out->input_rows = input_rows_consumed_;
	header.naggs = out->naggs;
	if (!BufFileWriteAllLocal(file, &header, sizeof(header)))
		return false;

	if (use_simple_group_key_)
	{
		for (const auto &entry : simple_hash_table_)
		{
			if (!append_group_record_to_partial_file(file, nullptr, &entry.first, entry.second))
				return false;
			out->group_count++;
		}
	}
	else
	{
		for (const auto &entry : hash_table_)
		{
			if (!append_group_record_to_partial_file(file, &entry.first, nullptr, entry.second))
				return false;
			out->group_count++;
		}
	}

	out->file_bytes = (uint64_t) BufFileSize(file);
	return true;
}

bool
VecAggState::store_group_rep_row_from_file(VecAggGroupState *group,
										   const std::vector<ParallelAggFileGroupKeyCol> &group_cols)
{
	DataChunk<DEFAULT_CHUNK_SIZE> *rep_chunk =
		rep_chunks_.empty() ? allocate_rep_chunk() : rep_chunks_.back();
	int rep_row;

	if (group == nullptr || group->has_rep_row)
		return false;
	if (rep_chunk->count >= DEFAULT_CHUNK_SIZE)
		rep_chunk = allocate_rep_chunk();
	rep_row = rep_chunk->count;
	group->rep_chunk_idx = (uint32_t) (rep_chunks_.size() - 1);
	group->rep_row_idx = (uint16_t) rep_row;
	group->has_rep_row = true;

	for (const auto &agg : aggs_)
	{
		int out_col = agg.target_resno - 1;
		const ParallelAggFileGroupKeyCol *col;

		if (agg.arg_expr != nullptr || agg.group_key_pos < 0 ||
			agg.group_key_pos >= (int) group_cols.size() ||
			out_col < 0 || out_col >= 16)
			continue;
		col = &group_cols[agg.group_key_pos];
		rep_chunk->nulls[out_col][rep_row] = col->header.is_null;
		if (col->header.is_null)
			continue;
		switch (agg.output_storage)
		{
			case VecOutputStorageKind::StringRef:
				rep_chunk->string_columns[out_col][rep_row] =
					rep_chunk->store_string_bytes(col->string_bytes.data(),
												  col->header.string_len);
				break;
			case VecOutputStorageKind::Int64:
			case VecOutputStorageKind::NumericScaledInt64:
			case VecOutputStorageKind::NumericAvgPair:
				rep_chunk->int64_columns[out_col][rep_row] = (int64_t) col->header.value_bits;
				rep_chunk->double_columns[out_col][rep_row] = (double) ((int64_t) col->header.value_bits);
				break;
			case VecOutputStorageKind::Double:
			{
				double value;

				memcpy(&value, &col->header.value_bits, sizeof(value));
				rep_chunk->double_columns[out_col][rep_row] = value;
				break;
			}
			case VecOutputStorageKind::Int32:
			default:
				rep_chunk->int32_columns[out_col][rep_row] = (int32_t) col->header.value_bits;
				break;
		}
	}
	rep_chunk->count++;
	return true;
}

bool
VecAggState::merge_parallel_grouped_partial_file(BufFile *file,
												 const ParallelAggPartialState &partial)
{
	GroupedPartialFileHeader header{};
	std::vector<ParallelAggFileGroupKeyCol> group_cols;

	if (file == nullptr || !supports_parallel_partial_state() ||
		!partial.grouped || !partial.file_backed ||
		partial.naggs != aggs_.size())
		return false;
	if (!BufFileReadAllLocal(file, &header, sizeof(header), false))
		return false;
	if (header.magic != VOLVEC_GROUPED_PARTIAL_FILE_MAGIC ||
		header.version != VOLVEC_GROUPED_PARTIAL_FILE_VERSION ||
		header.naggs != partial.naggs)
		return false;

	if (partial.group_count > 0)
	{
		if (use_simple_group_key_)
			simple_hash_table_.reserve(simple_hash_table_.size() + partial.group_count);
		else
			hash_table_.reserve(hash_table_.size() + partial.group_count);
	}
	group_cols.reserve(grp_col_indices_.size());
	for (uint32_t g = 0; g < partial.group_count; g++)
	{
		uint32_t num_group_cols = 0;
		VecAggAccumulatorList *group_accs = nullptr;

		if (!BufFileReadAllLocal(file, &num_group_cols, sizeof(num_group_cols), false))
			return false;
		group_cols.clear();
		group_cols.resize(num_group_cols);
		for (uint32_t k = 0; k < num_group_cols; k++)
		{
			if (!BufFileReadAllLocal(file, &group_cols[k].header, sizeof(group_cols[k].header), false))
				return false;
			if (!group_cols[k].header.is_null &&
				group_cols[k].header.storage_kind == (uint8_t) VecOutputStorageKind::StringRef &&
				group_cols[k].header.string_len > 0)
			{
				group_cols[k].string_bytes.resize(group_cols[k].header.string_len);
				if (!BufFileReadAllLocal(file,
										 group_cols[k].string_bytes.data(),
										 group_cols[k].header.string_len,
										 false))
					return false;
			}
		}

		if (use_simple_group_key_)
		{
			VecSimpleGroupKey simple_key;
			decltype(simple_hash_table_.try_emplace(simple_key)) insert_result;
			VecAggGroupState *group;

			if (num_group_cols != 1)
				return false;
			simple_key.is_null = group_cols[0].header.is_null;
			simple_key.value = (int64_t) group_cols[0].header.value_bits;
			insert_result = simple_hash_table_.try_emplace(simple_key);
			group = &insert_result.first->second;
			if (insert_result.second)
			{
				group->accs = VecAggAccumulatorList(PgMemoryContextAllocator<VecAggAccumulator>(memory_context_));
				if (!store_group_rep_row_from_file(group, group_cols))
					return false;
			}
			group_accs = &group->accs;
		}
		else
		{
			VecGroupKey key;
			decltype(hash_table_.try_emplace(key)) insert_result;
			VecAggGroupState *group;

			memset(&key, 0, sizeof(key));
			key.num_cols = (int) Min(num_group_cols, (uint32_t) grp_col_indices_.size());
			for (int k = 0; k < key.num_cols; k++)
			{
				const ParallelAggFileGroupKeyCol &src_col = group_cols[k];
				const VecOutputColMeta &meta = grp_col_meta_[k];

				key.is_null[k] = src_col.header.is_null;
				if (src_col.header.is_null)
					continue;
				switch (meta.storage_kind)
				{
					case VecOutputStorageKind::StringRef:
						key.values[k] =
							HashStringBytesForGroupKey(src_col.string_bytes.data(),
													   src_col.header.string_len,
													   meta.sql_type,
													   &key.aux[k]);
						break;
					case VecOutputStorageKind::Int32:
						key.values[k] = (uint64_t) (uint32_t) src_col.header.value_bits;
						key.aux[k] = 0;
						break;
					case VecOutputStorageKind::Int64:
					case VecOutputStorageKind::NumericScaledInt64:
					case VecOutputStorageKind::NumericAvgPair:
					case VecOutputStorageKind::Double:
						key.values[k] = src_col.header.value_bits;
						key.aux[k] = 0;
						break;
				}
			}

			insert_result = hash_table_.try_emplace(key);
			group = &insert_result.first->second;
			if (insert_result.second)
			{
				group->accs = VecAggAccumulatorList(PgMemoryContextAllocator<VecAggAccumulator>(memory_context_));
				if (!store_group_rep_row_from_file(group, group_cols))
					return false;
			}
			group_accs = &group->accs;
		}

		if (group_accs->empty())
			group_accs->resize(aggs_.size());
		for (size_t a = 0; a < aggs_.size(); a++)
		{
			ParallelAggPartialAccumulator acc{};

			if (!BufFileReadAllLocal(file, &acc, sizeof(acc), false))
				return false;
			if (aggs_[a].is_distinct)
			{
				if (!read_distinct_values_from_partial_file(file, &(*group_accs)[a]))
					return false;
			}
			else
				merge_partial_accumulator(acc, a, &(*group_accs)[a]);
		}
	}

	return true;
}

bool
VecAggState::merge_parallel_partial_state(const ParallelAggPartialState &partial)
{
	VecGroupKey key;
	VecAggGroupState *group;
	VecAggAccumulatorList *accs;

	if (!supports_parallel_partial_state() ||
		partial.naggs != aggs_.size())
		return false;
	if (partial.grouped)
	{
		for (uint32_t g = 0; g < partial.group_count; g++)
		{
			const ParallelAggPartialGroupEntry &src_group = partial.groups[g];
			VecAggAccumulatorList *group_accs = nullptr;

			if (use_simple_group_key_)
			{
				VecSimpleGroupKey simple_key;
				decltype(simple_hash_table_.try_emplace(simple_key)) insert_result;

				if (src_group.num_group_cols != 1)
					return false;
				simple_key.is_null = src_group.group_cols[0].is_null;
				simple_key.value = (int64_t) src_group.group_cols[0].value_bits;
				insert_result = simple_hash_table_.try_emplace(simple_key);
				group = &insert_result.first->second;
				if (insert_result.second)
				{
					group->accs = VecAggAccumulatorList(PgMemoryContextAllocator<VecAggAccumulator>(memory_context_));
					store_group_rep_row_from_partial(group, src_group);
				}
				group_accs = &group->accs;
			}
			else
			{
				memset(&key, 0, sizeof(key));
				key.num_cols = (int) Min(src_group.num_group_cols, (uint32_t) grp_col_indices_.size());
				for (int k = 0; k < key.num_cols; k++)
				{
					const ParallelAggPartialGroupKeyCol &src_col = src_group.group_cols[k];
					const VecOutputColMeta &meta = grp_col_meta_[k];

					key.is_null[k] = src_col.is_null;
					if (src_col.is_null)
						continue;
					switch (meta.storage_kind)
					{
						case VecOutputStorageKind::StringRef:
						{
							char buf[8] = {0};
							uint32_t len = src_col.string_len;

							if (len > sizeof(buf))
								return false;
							memcpy(buf, &src_col.value_bits, len);
							if (meta.sql_type == BPCHAROID)
								len = TrimBpcharLengthLocal(buf, len);
							key.values[k] = HashBytes64(buf, len);
							key.aux[k] = len;
							break;
						}
						case VecOutputStorageKind::Int32:
							key.values[k] = (uint64_t) (uint32_t) src_col.value_bits;
							key.aux[k] = 0;
							break;
						case VecOutputStorageKind::Int64:
						case VecOutputStorageKind::NumericScaledInt64:
						case VecOutputStorageKind::NumericAvgPair:
						default:
							key.values[k] = src_col.value_bits;
							key.aux[k] = 0;
							break;
					}
				}
				auto insert_result = hash_table_.try_emplace(key);
				group = &insert_result.first->second;
				if (insert_result.second)
				{
					group->accs = VecAggAccumulatorList(PgMemoryContextAllocator<VecAggAccumulator>(memory_context_));
					store_group_rep_row_from_partial(group, src_group);
				}
				group_accs = &group->accs;
			}

			if (group_accs->size() < aggs_.size())
				group_accs->resize(aggs_.size());
			for (size_t a = 0; a < aggs_.size() && a < lengthof(src_group.accs); a++)
				merge_partial_accumulator(src_group.accs[a], a, &(*group_accs)[a]);
		}
		return true;
	}
	key.num_cols = 0;
	auto insert_result = hash_table_.try_emplace(key);
	group = &insert_result.first->second;
	if (insert_result.second)
		group->accs = VecAggAccumulatorList(PgMemoryContextAllocator<VecAggAccumulator>(memory_context_));
	accs = &group->accs;
	if (accs->size() < aggs_.size())
		accs->resize(aggs_.size());

	for (size_t a = 0; a < aggs_.size() && a < lengthof(partial.accs); a++)
	{
		merge_partial_accumulator(partial.accs[a], a, &(*accs)[a]);
	}

	return true;
}

void
VecAggState::do_sink()
{
	consume_left_input();
	finish_sink();
}

bool VecAggState::get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk) {
	if (!fully_scanned_) do_sink();
	chunk.reset();
	if (use_simple_group_key_) {
		while (simple_it_ != simple_hash_table_.end() && chunk.count < DEFAULT_CHUNK_SIZE) {
			const auto& accs = simple_it_->second.accs;
			const DataChunk<DEFAULT_CHUNK_SIZE> *rep_chunk =
				simple_it_->second.has_rep_row ? rep_chunks_[simple_it_->second.rep_chunk_idx] : nullptr;
			int rep_row = simple_it_->second.rep_row_idx;
			for (size_t a = 0; a < aggs_.size(); a++) {
				int tidx = aggs_[a].target_resno - 1; if (tidx < 0 || tidx >= 16) continue;
				chunk.nulls[tidx][chunk.count] = 0;
				if (aggs_[a].arg_expr == nullptr && aggs_[a].type == VecAggType::MAX) {
					if (rep_chunk != nullptr)
					{
						chunk.nulls[tidx][chunk.count] = rep_chunk->nulls[tidx][rep_row];
						if (!chunk.nulls[tidx][chunk.count])
						{
							switch (aggs_[a].output_storage)
							{
								case VecOutputStorageKind::StringRef:
									chunk.string_columns[tidx][chunk.count] =
										CopyStringRefToChunk(chunk, *rep_chunk, rep_chunk->string_columns[tidx][rep_row]);
									break;
								case VecOutputStorageKind::Int64:
								case VecOutputStorageKind::NumericScaledInt64:
								case VecOutputStorageKind::NumericAvgPair:
									chunk.int64_columns[tidx][chunk.count] = rep_chunk->int64_columns[tidx][rep_row];
									chunk.double_columns[tidx][chunk.count] = rep_chunk->double_columns[tidx][rep_row];
									break;
								case VecOutputStorageKind::Double:
									chunk.double_columns[tidx][chunk.count] = rep_chunk->double_columns[tidx][rep_row];
									break;
								case VecOutputStorageKind::Int32:
								default:
									chunk.int32_columns[tidx][chunk.count] = rep_chunk->int32_columns[tidx][rep_row];
									break;
							}
						}
					}
					} else {
						if (aggs_[a].type == VecAggType::AVG) {
							if (accs[a].count == 0)
								chunk.nulls[tidx][chunk.count] = 1;
							else if (aggs_[a].use_exact_numeric) {
								chunk.int64_columns[tidx][chunk.count] =
									WideIntToInt64Checked(accs[a].numeric_sum, "aggregate numeric average sum");
								chunk.double_columns[tidx][chunk.count] = (double) accs[a].count;
							} else {
								chunk.double_columns[tidx][chunk.count] = accs[a].float_sum / accs[a].count;
							}
						}
					else if (aggs_[a].type == VecAggType::COUNT) chunk.int64_columns[tidx][chunk.count] = accs[a].count;
					else if (aggs_[a].type == VecAggType::MAX) {
						if (!accs[a].has_value)
							chunk.nulls[tidx][chunk.count] = 1;
						else if (aggs_[a].use_exact_numeric)
							chunk.int64_columns[tidx][chunk.count] =
								WideIntToInt64Checked(accs[a].numeric_max, "aggregate numeric max");
						else if (aggs_[a].output_storage == VecOutputStorageKind::Int64 ||
								 aggs_[a].output_storage == VecOutputStorageKind::NumericScaledInt64 ||
								 aggs_[a].output_storage == VecOutputStorageKind::NumericAvgPair)
							chunk.int64_columns[tidx][chunk.count] = accs[a].int64_max;
						else if (aggs_[a].output_storage == VecOutputStorageKind::Double)
							chunk.double_columns[tidx][chunk.count] = accs[a].float_max;
						else
							chunk.int32_columns[tidx][chunk.count] = accs[a].int32_max;
					}
							else {
								if (accs[a].count == 0) {
									chunk.nulls[tidx][chunk.count] = 1;
								} else if (aggs_[a].use_exact_numeric) {
									if (pg_volvec_trace_hooks)
									{
										static int simple_group_exact_numeric_trace_count = 0;

									if (simple_group_exact_numeric_trace_count < 12)
									{
										long long lo = (long long) WideIntLow64(accs[a].numeric_sum);
										long long hi = (long long) WideIntHigh64(accs[a].numeric_sum);

										elog(LOG,
											 "pg_volvec: simple grouped exact numeric materialize target_resno=%d scale=%d count=%lld wide_hi=%lld wide_lo=%lld",
											 aggs_[a].target_resno,
											 aggs_[a].numeric_scale,
											 (long long) accs[a].count,
											 hi,
											 lo);
										simple_group_exact_numeric_trace_count++;
									}
								}
								chunk.int64_columns[tidx][chunk.count] =
									WideIntToInt64Checked(accs[a].numeric_sum, "aggregate numeric sum");
							} else {
							chunk.double_columns[tidx][chunk.count] = accs[a].float_sum;
							chunk.int64_columns[tidx][chunk.count] = (int64_t)(accs[a].float_sum + (accs[a].float_sum >= 0 ? 0.5 : -0.5));
						}
					}
				}
			}
			chunk.count++; ++simple_it_;
		}
		return chunk.count > 0;
	}
		while (it_ != hash_table_.end() && chunk.count < DEFAULT_CHUNK_SIZE) {
				const auto& accs = it_->second.accs;
				const DataChunk<DEFAULT_CHUNK_SIZE> *rep_chunk =
					it_->second.has_rep_row ? rep_chunks_[it_->second.rep_chunk_idx] : nullptr;
				int rep_row = it_->second.rep_row_idx;
			for (size_t a = 0; a < aggs_.size(); a++) {
				int tidx = aggs_[a].target_resno - 1; if (tidx < 0 || tidx >= 16) continue;
				chunk.nulls[tidx][chunk.count] = 0;
				if (aggs_[a].arg_expr == nullptr && aggs_[a].type == VecAggType::MAX) {
					if (rep_chunk != nullptr)
					{
						chunk.nulls[tidx][chunk.count] = rep_chunk->nulls[tidx][rep_row];
						if (!chunk.nulls[tidx][chunk.count])
						{
							switch (aggs_[a].output_storage)
							{
								case VecOutputStorageKind::StringRef:
									chunk.string_columns[tidx][chunk.count] =
										CopyStringRefToChunk(chunk, *rep_chunk, rep_chunk->string_columns[tidx][rep_row]);
									break;
								case VecOutputStorageKind::Int64:
								case VecOutputStorageKind::NumericScaledInt64:
								case VecOutputStorageKind::NumericAvgPair:
									chunk.int64_columns[tidx][chunk.count] = rep_chunk->int64_columns[tidx][rep_row];
									chunk.double_columns[tidx][chunk.count] = rep_chunk->double_columns[tidx][rep_row];
									break;
								case VecOutputStorageKind::Double:
									chunk.double_columns[tidx][chunk.count] = rep_chunk->double_columns[tidx][rep_row];
									break;
								case VecOutputStorageKind::Int32:
								default:
									chunk.int32_columns[tidx][chunk.count] = rep_chunk->int32_columns[tidx][rep_row];
									break;
							}
						}
					}
					} else {
						if (aggs_[a].type == VecAggType::AVG) {
							if (accs[a].count == 0)
								chunk.nulls[tidx][chunk.count] = 1;
							else if (aggs_[a].use_exact_numeric) {
								chunk.int64_columns[tidx][chunk.count] =
									WideIntToInt64Checked(accs[a].numeric_sum, "aggregate numeric average sum");
								chunk.double_columns[tidx][chunk.count] = (double) accs[a].count;
							} else {
								chunk.double_columns[tidx][chunk.count] = accs[a].float_sum / accs[a].count;
							}
						}
					else if (aggs_[a].type == VecAggType::COUNT) chunk.int64_columns[tidx][chunk.count] = accs[a].count;
					else if (aggs_[a].type == VecAggType::MAX) {
						if (!accs[a].has_value)
							chunk.nulls[tidx][chunk.count] = 1;
						else if (aggs_[a].use_exact_numeric)
							chunk.int64_columns[tidx][chunk.count] =
								WideIntToInt64Checked(accs[a].numeric_max, "aggregate numeric max");
						else if (aggs_[a].output_storage == VecOutputStorageKind::Int64 ||
								 aggs_[a].output_storage == VecOutputStorageKind::NumericScaledInt64 ||
								 aggs_[a].output_storage == VecOutputStorageKind::NumericAvgPair)
							chunk.int64_columns[tidx][chunk.count] = accs[a].int64_max;
						else if (aggs_[a].output_storage == VecOutputStorageKind::Double)
							chunk.double_columns[tidx][chunk.count] = accs[a].float_max;
						else
							chunk.int32_columns[tidx][chunk.count] = accs[a].int32_max;
					}
							else {
								if (accs[a].count == 0) {
									chunk.nulls[tidx][chunk.count] = 1;
								} else if (aggs_[a].use_exact_numeric) {
									if (pg_volvec_trace_hooks)
									{
										static int grouped_exact_numeric_trace_count = 0;

									if (grouped_exact_numeric_trace_count < 12)
									{
										long long lo = (long long) WideIntLow64(accs[a].numeric_sum);
										long long hi = (long long) WideIntHigh64(accs[a].numeric_sum);

										elog(LOG,
											 "pg_volvec: grouped exact numeric materialize target_resno=%d scale=%d count=%lld wide_hi=%lld wide_lo=%lld",
											 aggs_[a].target_resno,
											 aggs_[a].numeric_scale,
											 (long long) accs[a].count,
											 hi,
											 lo);
										grouped_exact_numeric_trace_count++;
									}
								}
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
VecAggState::lookup_remapped_output_col_meta(int child_input_resno,
											 uint16_t *output_col,
											 VecOutputColMeta *out) const
{
	for (const auto &agg : aggs_)
	{
		if (agg.input_col < 0 || agg.arg_expr != nullptr)
			continue;
		if (agg.input_col + 1 != child_input_resno)
			continue;
		if (output_col != nullptr)
			*output_col = (uint16_t) (agg.target_resno - 1);
		if (out != nullptr)
		{
			out->sql_type = agg.output_type;
			out->storage_kind = agg.output_storage;
			out->scale = agg.numeric_scale;
		}
		return true;
	}

	for (size_t i = 0; i < grp_col_indices_.size(); i++)
	{
		if (grp_col_indices_[i] + 1 != child_input_resno)
			continue;
		if (output_col != nullptr)
			*output_col = (uint16_t) i;
		if (out != nullptr)
			*out = grp_col_meta_[i];
		return true;
	}

	return left_ != nullptr &&
		left_->lookup_remapped_output_col_meta(child_input_resno, output_col, out);
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

} /* namespace pg_volvec */

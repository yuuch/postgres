#include "exec/internal.hpp"

namespace pg_volvec {

static bool
IsIdentityVarTargetList(List *targetlist)
{
	ListCell *lc;
	int expected_resno = 1;
	bool saw_visible = false;

	foreach(lc, targetlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		Expr *expr;
		Var *var;

		if (tle->resjunk)
			continue;
		expr = StripImplicitNodesLocal((Expr *) tle->expr);
		if (expr == nullptr || !IsA(expr, Var))
			return false;
		var = (Var *) expr;
		if (tle->resno != expected_resno || var->varattno != expected_resno)
			return false;
		expected_resno++;
		saw_visible = true;
	}

	return saw_visible;
}

bool
CanBuildDirectVarProjectTargetList(List *targetlist)
{
	ListCell *lc;
	bool saw_visible = false;

	if (targetlist == NIL)
		return false;

	foreach(lc, targetlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		Expr *expr;
		Var *var;

		if (tle->resjunk)
			continue;
		expr = StripImplicitNodesLocal((Expr *) tle->expr);
		if (expr == nullptr || !IsA(expr, Var))
			return false;
		var = (Var *) expr;
		if (var->varattno <= 0 || var->varattno > 16)
			return false;
		saw_visible = true;
	}

	return saw_visible;
}

std::unique_ptr<VecPlanState>
BuildDirectVarProject(std::unique_ptr<VecPlanState> left, List *targetlist)
{
	VolVecVector<VecProjectColDesc> project_cols{PgMemoryContextAllocator<VecProjectColDesc>(CurrentMemoryContext)};
	ListCell *lc;

	if (!left || targetlist == NIL || IsIdentityVarTargetList(targetlist))
		return left;

	foreach(lc, targetlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		Expr *expr = StripImplicitNodesLocal((Expr *) tle->expr);
		VecProjectColDesc project_col;
		Var *var;
		VecOutputColMeta meta;

		if (tle->resjunk)
			continue;
		if (expr == nullptr || !IsA(expr, Var))
			return nullptr;
		var = (Var *) expr;
		if (var->varattno <= 0 || var->varattno > 16)
			return nullptr;
		if (!left->lookup_output_col_meta(var->varattno, &meta))
			return nullptr;

		project_col.expr = nullptr;
		project_col.target_resno = tle->resno;
		project_col.sql_type = exprType((Node *) tle->expr);
		project_col.storage_kind = meta.storage_kind;
		project_col.scale = meta.scale;
		project_col.direct_var = true;
		project_col.input_col = (uint16_t) (var->varattno - 1);
		project_cols.push_back(std::move(project_col));
	}

	if (project_cols.empty())
		return left;

	return std::make_unique<VecProjectState>(std::move(left), std::move(project_cols));
}

VecProjectState::VecProjectState(std::unique_ptr<VecPlanState> left,
								 VolVecVector<VecProjectColDesc> columns)
	: left_(std::move(left)),
	  columns_(PgMemoryContextAllocator<VecProjectColDesc>(CurrentMemoryContext))
{
	for (auto &column : columns)
		columns_.push_back(std::move(column));
}

bool
VecProjectState::lookup_output_col_meta(int target_resno, VecOutputColMeta *out) const
{
	for (const auto &column : columns_)
	{
		if (column.target_resno != target_resno)
			continue;
		if (out != nullptr)
		{
			out->sql_type = column.sql_type;
			out->storage_kind = column.storage_kind;
			out->scale = column.scale;
		}
		return true;
	}
	return false;
}

bool
VecProjectState::lookup_remapped_output_col_meta(int child_input_resno,
												 uint16_t *output_col,
												 VecOutputColMeta *out) const
{
	for (const auto &column : columns_)
	{
		if ((!column.direct_var && !column.string_prefix_var) ||
			column.input_col + 1 != child_input_resno)
			continue;
		if (output_col != nullptr)
			*output_col = (uint16_t) (column.target_resno - 1);
		if (out != nullptr)
		{
			out->sql_type = column.sql_type;
			out->storage_kind = column.storage_kind;
			out->scale = column.scale;
		}
		return true;
	}

	return left_ != nullptr &&
		left_->lookup_remapped_output_col_meta(child_input_resno, output_col, out);
}

bool
VecProjectState::get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk)
{
	chunk.reset();
	while (left_->get_next_batch(input_chunk_))
	{
		int active_count = input_chunk_.has_selection ? input_chunk_.sel.count : input_chunk_.count;

		if (active_count <= 0)
			continue;

		for (auto &column : columns_)
		{
			if (column.expr)
				column.expr->evaluate(input_chunk_);
		}

		for (int s = 0; s < active_count; s++)
		{
			int src_row = input_chunk_.has_selection ? input_chunk_.sel.row_ids[s] : s;
			int dst_row = chunk.count++;

			for (const auto &column : columns_)
			{
				int out_col = column.target_resno - 1;
				int reg = column.expr ? column.expr->get_final_res_idx() : -1;

				if (out_col < 0 || out_col >= 16)
					continue;
				if (column.direct_var || column.string_prefix_var)
					chunk.nulls[out_col][dst_row] = input_chunk_.nulls[column.input_col][src_row];
				else
					chunk.nulls[out_col][dst_row] = column.expr->get_nulls_reg(reg)[src_row];
				if (chunk.nulls[out_col][dst_row])
					continue;

				switch (column.storage_kind)
				{
					case VecOutputStorageKind::Double:
						chunk.double_columns[out_col][dst_row] = column.direct_var ?
							input_chunk_.double_columns[column.input_col][src_row] :
							column.expr->get_float8_reg(reg)[src_row];
						break;
					case VecOutputStorageKind::Int64:
					case VecOutputStorageKind::NumericScaledInt64:
						chunk.int64_columns[out_col][dst_row] = column.direct_var ?
							input_chunk_.int64_columns[column.input_col][src_row] :
							column.expr->get_int64_reg(reg)[src_row];
						break;
					case VecOutputStorageKind::Int32:
						chunk.int32_columns[out_col][dst_row] = column.direct_var ?
							input_chunk_.int32_columns[column.input_col][src_row] :
							column.expr->get_int32_reg(reg)[src_row];
						break;
					case VecOutputStorageKind::StringRef:
						if (column.string_prefix_var)
						{
							VecStringRef src_ref = input_chunk_.string_columns[column.input_col][src_row];
							uint32_t copy_len = Min(src_ref.len, column.string_prefix_len);

							chunk.string_columns[out_col][dst_row] =
								chunk.store_string_bytes(input_chunk_.get_string_ptr(src_ref), copy_len);
							break;
						}
						if (!column.direct_var)
							elog(ERROR, "pg_volvec computed string projection is not supported");
						chunk.string_columns[out_col][dst_row] =
							CopyStringRefToChunk(chunk, input_chunk_,
												 input_chunk_.string_columns[column.input_col][src_row]);
						break;
					default:
						elog(ERROR, "pg_volvec project output kind is not supported");
						break;
				}
			}
		}

		if (chunk.count > 0)
			return true;
	}

	return false;
}

bool
VecProjectState::drain_to(VecPlanState *downstream)
{
	bool ok;

	if (left_ == nullptr || downstream == nullptr)
		return false;
	push_downstream_ = downstream;
	ok = left_->drain_to(this);
	push_downstream_ = nullptr;
	return ok;
}

bool
VecProjectState::push_batch(DataChunk<DEFAULT_CHUNK_SIZE> &input)
{
	int active_count;

	if (push_downstream_ == nullptr)
		return false;
	active_count = input.has_selection ? input.sel.count : input.count;
	if (active_count <= 0)
		return true;

	push_chunk_.reset();
	for (auto &column : columns_)
	{
		if (column.expr)
			column.expr->evaluate(input);
	}
	for (int s = 0; s < active_count; s++)
	{
		int src_row = input.has_selection ? input.sel.row_ids[s] : s;
		int dst_row = push_chunk_.count++;

		for (const auto &column : columns_)
		{
			int out_col = column.target_resno - 1;
			int reg = column.expr ? column.expr->get_final_res_idx() : -1;

			if (out_col < 0 || out_col >= 16)
				continue;
			if (column.direct_var || column.string_prefix_var)
				push_chunk_.nulls[out_col][dst_row] = input.nulls[column.input_col][src_row];
			else
				push_chunk_.nulls[out_col][dst_row] = column.expr->get_nulls_reg(reg)[src_row];
			if (push_chunk_.nulls[out_col][dst_row])
				continue;
			switch (column.storage_kind)
			{
				case VecOutputStorageKind::Double:
					push_chunk_.double_columns[out_col][dst_row] = column.direct_var ?
						input.double_columns[column.input_col][src_row] :
						column.expr->get_float8_reg(reg)[src_row];
					break;
				case VecOutputStorageKind::Int64:
				case VecOutputStorageKind::NumericScaledInt64:
					push_chunk_.int64_columns[out_col][dst_row] = column.direct_var ?
						input.int64_columns[column.input_col][src_row] :
						column.expr->get_int64_reg(reg)[src_row];
					break;
				case VecOutputStorageKind::Int32:
					push_chunk_.int32_columns[out_col][dst_row] = column.direct_var ?
						input.int32_columns[column.input_col][src_row] :
						column.expr->get_int32_reg(reg)[src_row];
					break;
				case VecOutputStorageKind::StringRef:
					if (column.string_prefix_var)
					{
						VecStringRef src_ref = input.string_columns[column.input_col][src_row];
						uint32_t copy_len = Min(src_ref.len, column.string_prefix_len);

						push_chunk_.string_columns[out_col][dst_row] =
							push_chunk_.store_string_bytes(input.get_string_ptr(src_ref), copy_len);
						break;
					}
					if (!column.direct_var)
						elog(ERROR, "pg_volvec computed string projection is not supported");
					push_chunk_.string_columns[out_col][dst_row] =
						CopyStringRefToChunk(push_chunk_, input,
											 input.string_columns[column.input_col][src_row]);
					break;
				default:
					elog(ERROR, "pg_volvec project output kind is not supported");
			}
		}
	}
	return push_chunk_.count == 0 || push_downstream_->push_batch(push_chunk_);
}

VecLookupProjectState::VecLookupProjectState(std::unique_ptr<VecPlanState> left,
											 std::unique_ptr<VecPlanState> lookup_source,
											 uint16_t input_key_col,
											 VecOutputColMeta input_key_meta,
											 uint16_t lookup_key_col,
											 VecOutputColMeta lookup_key_meta,
											 uint16_t lookup_value_col,
											 int output_resno,
											 VecOutputColMeta output_meta)
	: left_(std::move(left)),
	  lookup_source_(std::move(lookup_source)),
	  memory_context_(CurrentMemoryContext),
	  lookup_table_(memory_context_),
	  input_key_col_(input_key_col),
	  input_key_meta_(input_key_meta),
	  lookup_key_col_(lookup_key_col),
	  lookup_key_meta_(lookup_key_meta),
	  lookup_value_col_(lookup_value_col),
	  output_resno_(output_resno),
	  output_meta_(output_meta),
	  lookup_built_(false)
{
}

bool
VecLookupProjectState::lookup_output_col_meta(int target_resno, VecOutputColMeta *out) const
{
	if (target_resno == output_resno_)
	{
		if (out != nullptr)
			*out = output_meta_;
		return true;
	}
	return left_ != nullptr && left_->lookup_output_col_meta(target_resno, out);
}

bool
VecLookupProjectState::lookup_remapped_output_col_meta(int child_input_resno,
													   uint16_t *output_col,
													   VecOutputColMeta *out) const
{
	return left_ != nullptr &&
		left_->lookup_remapped_output_col_meta(child_input_resno, output_col, out);
}

bool
VecLookupProjectState::extract_lookup_key(const DataChunk<DEFAULT_CHUNK_SIZE> &chunk,
										  int row,
										  uint16_t col,
										  const VecOutputColMeta &meta,
										  VecLookupScalarKey *key) const
{
	if (key == nullptr || col >= 16)
		return false;

	key->is_null = chunk.nulls[col][row] ? 1 : 0;
	key->value = 0;
	if (key->is_null)
		return true;

	switch (meta.storage_kind)
	{
		case VecOutputStorageKind::Int32:
			key->value = (int64_t) chunk.int32_columns[col][row];
			return true;
		case VecOutputStorageKind::Int64:
		case VecOutputStorageKind::NumericScaledInt64:
			key->value = chunk.int64_columns[col][row];
			return true;
		default:
			return false;
	}
}

bool
VecLookupProjectState::build_lookup()
{
	if (lookup_built_)
		return true;
	if (lookup_source_ == nullptr)
		return false;

	while (lookup_source_->get_next_batch(lookup_chunk_))
	{
		int active_count = lookup_chunk_.has_selection ? lookup_chunk_.sel.count : lookup_chunk_.count;

		for (int s = 0; s < active_count; s++)
		{
			int row = lookup_chunk_.has_selection ? lookup_chunk_.sel.row_ids[s] : s;
			VecLookupScalarKey key;
			VecLookupScalarValue value;

			if (!extract_lookup_key(lookup_chunk_, row, lookup_key_col_, lookup_key_meta_, &key))
				return false;
			value.is_null = lookup_chunk_.nulls[lookup_value_col_][row] ? 1 : 0;
			if (!value.is_null)
			{
				switch (output_meta_.storage_kind)
				{
					case VecOutputStorageKind::Int32:
						value.i32 = lookup_chunk_.int32_columns[lookup_value_col_][row];
						break;
					case VecOutputStorageKind::Int64:
					case VecOutputStorageKind::NumericScaledInt64:
						value.i64 = lookup_chunk_.int64_columns[lookup_value_col_][row];
						break;
					case VecOutputStorageKind::Double:
						value.f8 = lookup_chunk_.double_columns[lookup_value_col_][row];
						break;
					default:
						return false;
				}
			}
			auto [slot, inserted] = lookup_table_.insert(key); slot->val = value;
		}
	}

	lookup_built_ = true;
	return true;
}

bool
VecLookupProjectState::get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk)
{
	int out_col = output_resno_ - 1;

	if (out_col < 0 || out_col >= 16)
		return false;
	if (!build_lookup())
		return false;

	while (left_->get_next_batch(chunk))
	{
		for (int row = 0; row < chunk.count; row++)
	{
		VecLookupScalarKey key;
		RobinHoodPgMap<VecLookupScalarKey, VecLookupScalarValue, VecLookupScalarKeyHash>::Slot *slot = nullptr;

		if (!extract_lookup_key(chunk, row, input_key_col_, input_key_meta_, &key))
			return false;
		if (!key.is_null)
			slot = lookup_table_.find(key);
		if (key.is_null || slot == nullptr)
		{
			chunk.nulls[out_col][row] = 1;
			continue;
		}

		chunk.nulls[out_col][row] = slot->val.is_null;
		if (chunk.nulls[out_col][row])
			continue;
		switch (output_meta_.storage_kind)
		{
			case VecOutputStorageKind::Int32:
				chunk.int32_columns[out_col][row] = slot->val.i32;
				break;
			case VecOutputStorageKind::Int64:
			case VecOutputStorageKind::NumericScaledInt64:
				chunk.int64_columns[out_col][row] = slot->val.i64;
				break;
			case VecOutputStorageKind::Double:
				chunk.double_columns[out_col][row] = slot->val.f8;
				break;
			default:
				return false;
		}
	}

		if ((chunk.has_selection ? chunk.sel.count : chunk.count) > 0)
			return true;
	}

	return false;
}

VecLookupProjectStateMultiKey::VecLookupProjectStateMultiKey(
	std::unique_ptr<VecPlanState> left,
	std::unique_ptr<VecPlanState> lookup_source,
	int num_keys,
	const uint16_t *input_key_cols,
	const VecOutputColMeta *input_key_metas,
	const uint16_t *lookup_key_cols,
	const VecOutputColMeta *lookup_key_metas,
	uint16_t lookup_value_col,
	int output_resno,
	VecOutputColMeta output_meta)
	: left_(std::move(left)),
	  lookup_source_(std::move(lookup_source)),
	  memory_context_(CurrentMemoryContext),
	  lookup_table_(memory_context_),
	  num_keys_(num_keys),
	  lookup_value_col_(lookup_value_col),
	  output_resno_(output_resno),
	  output_meta_(output_meta),
	  lookup_built_(false)
{
	Assert(num_keys_ > 0 && num_keys_ <= kMaxLookupKeys);
	for (int i = 0; i < kMaxLookupKeys; i++)
	{
		input_key_cols_[i] = 0;
		input_key_metas_[i] = VecOutputColMeta{InvalidOid, VecOutputStorageKind::Int32, 0};
		lookup_key_cols_[i] = 0;
		lookup_key_metas_[i] = VecOutputColMeta{InvalidOid, VecOutputStorageKind::Int32, 0};
	}
	for (int i = 0; i < num_keys_; i++)
	{
		input_key_cols_[i] = input_key_cols[i];
		input_key_metas_[i] = input_key_metas[i];
		lookup_key_cols_[i] = lookup_key_cols[i];
		lookup_key_metas_[i] = lookup_key_metas[i];
	}
}

bool
VecLookupProjectStateMultiKey::lookup_output_col_meta(int target_resno, VecOutputColMeta *out) const
{
	if (target_resno == output_resno_)
	{
// Auto-split from executor.cpp
		if (out != nullptr)
			*out = output_meta_;
		return true;
	}
	return left_ != nullptr && left_->lookup_output_col_meta(target_resno, out);
}

bool
VecLookupProjectStateMultiKey::lookup_remapped_output_col_meta(int child_input_resno,
															   uint16_t *output_col,
															   VecOutputColMeta *out) const
{
	return left_ != nullptr &&
		left_->lookup_remapped_output_col_meta(child_input_resno, output_col, out);
}

bool
VecLookupProjectStateMultiKey::extract_lookup_key(const DataChunk<DEFAULT_CHUNK_SIZE> &chunk,
												  int row,
												  const uint16_t *cols,
												  const VecOutputColMeta *metas,
												  VecLookupCompositeKey *key) const
{
	if (key == nullptr)
		return false;

	key->num_keys = (uint8_t) num_keys_;
	key->is_null = 0;
	for (int i = 0; i < num_keys_; i++)
	{
		uint16_t col = cols[i];
		const VecOutputColMeta &meta = metas[i];

		key->values[i] = 0;
		if (col >= 16)
			return false;
		if (chunk.nulls[col][row])
		{
			key->is_null = 1;
			continue;
		}

		switch (meta.storage_kind)
		{
			case VecOutputStorageKind::Int32:
				key->values[i] = (int64_t) chunk.int32_columns[col][row];
				break;
			case VecOutputStorageKind::Int64:
			case VecOutputStorageKind::NumericScaledInt64:
				key->values[i] = chunk.int64_columns[col][row];
				break;
			default:
				return false;
		}
	}

	return true;
}

bool
VecLookupProjectStateMultiKey::build_lookup()
{
	if (lookup_built_)
		return true;
	if (lookup_source_ == nullptr)
		return false;

	while (lookup_source_->get_next_batch(lookup_chunk_))
	{
		int active_count = lookup_chunk_.has_selection ? lookup_chunk_.sel.count : lookup_chunk_.count;

		for (int s = 0; s < active_count; s++)
		{
			int row = lookup_chunk_.has_selection ? lookup_chunk_.sel.row_ids[s] : s;
			VecLookupCompositeKey key;
			VecLookupScalarValue value;

			if (!extract_lookup_key(lookup_chunk_, row, lookup_key_cols_, lookup_key_metas_, &key))
				return false;
			value.is_null = lookup_chunk_.nulls[lookup_value_col_][row] ? 1 : 0;
			if (!value.is_null)
			{
				switch (output_meta_.storage_kind)
				{
					case VecOutputStorageKind::Int32:
						value.i32 = lookup_chunk_.int32_columns[lookup_value_col_][row];
						break;
					case VecOutputStorageKind::Int64:
					case VecOutputStorageKind::NumericScaledInt64:
						value.i64 = lookup_chunk_.int64_columns[lookup_value_col_][row];
						break;
					case VecOutputStorageKind::Double:
						value.f8 = lookup_chunk_.double_columns[lookup_value_col_][row];
						break;
					default:
						return false;
				}
			}
			auto [slot, inserted] = lookup_table_.insert(key); slot->val = value;
		}
	}

	lookup_built_ = true;
	return true;
}

bool
VecLookupProjectStateMultiKey::get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk)
{
	int out_col = output_resno_ - 1;

	if (out_col < 0 || out_col >= 16)
		return false;
	if (!build_lookup())
		return false;

	while (left_->get_next_batch(chunk))
	{
	for (int row = 0; row < chunk.count; row++)
	{
		VecLookupCompositeKey key;
		RobinHoodPgMap<VecLookupCompositeKey, VecLookupScalarValue, VecLookupCompositeKeyHash>::Slot *slot = nullptr;

		if (!extract_lookup_key(chunk, row, input_key_cols_, input_key_metas_, &key))
			return false;
		if (!key.is_null)
			slot = lookup_table_.find(key);
		if (key.is_null || slot == nullptr)
		{
			chunk.nulls[out_col][row] = 1;
			continue;
		}

		chunk.nulls[out_col][row] = slot->val.is_null;
		if (chunk.nulls[out_col][row])
			continue;
		switch (output_meta_.storage_kind)
		{
			case VecOutputStorageKind::Int32:
				chunk.int32_columns[out_col][row] = slot->val.i32;
				break;
			case VecOutputStorageKind::Int64:
			case VecOutputStorageKind::NumericScaledInt64:
				chunk.int64_columns[out_col][row] = slot->val.i64;
				break;
			case VecOutputStorageKind::Double:
				chunk.double_columns[out_col][row] = slot->val.f8;
				break;
			default:
				return false;
		}
	}

		if ((chunk.has_selection ? chunk.sel.count : chunk.count) > 0)
			return true;
	}

	return false;
}

} /* namespace pg_volvec */

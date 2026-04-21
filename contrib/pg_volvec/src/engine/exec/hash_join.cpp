#include "exec/internal.hpp"
#include "hash_table.hpp"

namespace pg_volvec {

bool
VecHashJoinState::get_next_batch(DataChunk<DEFAULT_CHUNK_SIZE> &chunk)
{
	if (!inner_built_)
// Auto-split from executor.cpp
		build_inner_hash();

	chunk.reset();
	auto copy_output_value = [&chunk](int dst_row,
									 int out_col,
									 const VecOutputColMeta &meta,
									 const DataChunk<DEFAULT_CHUNK_SIZE> &src,
									 int src_col,
									 int src_row) {
		chunk.nulls[out_col][dst_row] = src.nulls[src_col][src_row];
		if (chunk.nulls[out_col][dst_row])
			return;
		switch (meta.storage_kind)
		{
			case VecOutputStorageKind::Double:
				chunk.double_columns[out_col][dst_row] = src.double_columns[src_col][src_row];
				break;
			case VecOutputStorageKind::Int64:
			case VecOutputStorageKind::NumericScaledInt64:
			case VecOutputStorageKind::NumericAvgPair:
				chunk.int64_columns[out_col][dst_row] = src.int64_columns[src_col][src_row];
				chunk.double_columns[out_col][dst_row] = src.double_columns[src_col][src_row];
				break;
			case VecOutputStorageKind::StringRef:
				chunk.string_columns[out_col][dst_row] =
					CopyStringRefToChunk(chunk, src, src.string_columns[src_col][src_row]);
				break;
			case VecOutputStorageKind::Int32:
				chunk.int32_columns[out_col][dst_row] = src.int32_columns[src_col][src_row];
				break;
		}
	};
	if (jointype_ == JOIN_ANTI)
	{
		if (build_outer_side_)
		{
			if (!anti_build_marked_)
			{
				while (advance_outer_batch())
				{
					int active_count = outer_chunk_.has_selection ? outer_chunk_.sel.count : outer_chunk_.count;

				for (int s = 0; s < active_count; s++)
				{
					int probe_row = outer_chunk_.has_selection ? outer_chunk_.sel.row_ids[s] : s;
					VecHashJoinKey key;
					uint32_t hash;
					int32_t entry_idx;

					if (!read_key(outer_chunk_, true, probe_row, &key))
						continue;
					hash = hash_key(key);
					uint16_t partition_id = (shared_hash_partition_count_ == 1) ? 0 : (uint16_t) volvec_radix_partition_idx(hash);
					const int32_t *bucket_heads = use_parallel_ht_ ? 
						bucket_heads_for_partition(partition_id) : active_bucket_heads();
					size_t mask = use_parallel_ht_ ? 
						active_bucket_mask_for_partition(partition_id) : bucket_mask_;
					entry_idx = bucket_heads == nullptr ? -1 : bucket_heads[hash & mask];
					while (entry_idx >= 0)
				{
				const VecHashEntry &entry = get_entry_at(entry_idx);

				if (entry.hash == hash &&
					keys_equal(entry.key, key) &&
					candidate_passes_join_filter_for_build_entry(outer_chunk_,
										 probe_row,
										 partition_id,
										 entry.chunk_idx,
										 entry.row_idx))
					inner_entry_matched_[entry_idx] = 1;
				entry_idx = entry.next;
				}
			}
		}
		anti_build_marked_ = true;
				anti_build_emit_pos_ = 0;
			}

			while (anti_build_emit_pos_ < active_entry_count() &&
				   chunk.count < DEFAULT_CHUNK_SIZE)
			{
				const VecHashEntry &entry = get_entry_at(anti_build_emit_pos_);

				if (inner_entry_matched_[anti_build_emit_pos_++])
					continue;
				for (const auto &output_col : output_cols_)
				{
					int out_col = output_col.output_resno - 1;

					if (output_col.output_resno > visible_output_count_)
						continue;
					if (output_col.side != VecJoinSide::Outer)
						elog(ERROR, "pg_volvec anti join build-outer path cannot expose inner columns");
					copy_inner_payload_value_to_chunk(chunk,
										chunk.count,
										out_col,
										output_col.meta,
										output_col.input_col,
										0,
										entry.chunk_idx,
										entry.row_idx);
				}
				chunk.count++;
			}

			return chunk.count > 0;
		}

		while (chunk.count < DEFAULT_CHUNK_SIZE)
		{
			int active_count;

			if ((outer_chunk_.has_selection ? outer_chunk_.sel.count : outer_chunk_.count) == 0 &&
				!advance_outer_batch())
				break;

		active_count = outer_chunk_.has_selection ? outer_chunk_.sel.count : outer_chunk_.count;
		while (anti_outer_pos_ < active_count && chunk.count < DEFAULT_CHUNK_SIZE)
		{
			int outer_row = outer_chunk_.has_selection ?
				outer_chunk_.sel.row_ids[anti_outer_pos_] : anti_outer_pos_;
			VecHashJoinKey key;
			bool has_match = false;
			int32_t entry_idx = -1;

			anti_outer_pos_++;
			if (read_key(outer_chunk_, false, outer_row, &key))
			{
				uint32_t hash = hash_key(key);
				uint16_t partition_id = (shared_hash_partition_count_ == 1) ? 0 : (uint16_t) volvec_radix_partition_idx(hash);
				const int32_t *bucket_heads = use_parallel_ht_ ? 
					bucket_heads_for_partition(partition_id) : active_bucket_heads();
				size_t mask = use_parallel_ht_ ? 
					active_bucket_mask_for_partition(partition_id) : bucket_mask_;
				entry_idx = bucket_heads == nullptr ? -1 : bucket_heads[hash & mask];
				while (entry_idx >= 0)
				{
					const VecHashEntry &entry = get_entry_at(entry_idx);

					if (entry.hash == hash &&
						keys_equal(entry.key, key) &&
						candidate_passes_join_filter_for_build_entry(outer_chunk_,
											 outer_row,
											 partition_id,
											 entry.chunk_idx,
											 entry.row_idx))
						{
							has_match = true;
							break;
						}
						entry_idx = entry.next;
					}
				}
				if (has_match)
					continue;

				for (const auto &output_col : output_cols_)
				{
					int out_col = output_col.output_resno - 1;
					int src_col = output_col.input_col;

					if (output_col.output_resno > visible_output_count_)
						continue;
					if (output_col.side != VecJoinSide::Outer)
						elog(ERROR, "pg_volvec anti join does not support inner output columns");
					chunk.nulls[out_col][chunk.count] = outer_chunk_.nulls[src_col][outer_row];
					if (chunk.nulls[out_col][chunk.count])
						continue;
					switch (output_col.meta.storage_kind)
					{
						case VecOutputStorageKind::Double:
							chunk.double_columns[out_col][chunk.count] =
								outer_chunk_.double_columns[src_col][outer_row];
							break;
						case VecOutputStorageKind::Int64:
						case VecOutputStorageKind::NumericScaledInt64:
						case VecOutputStorageKind::NumericAvgPair:
							chunk.int64_columns[out_col][chunk.count] =
								outer_chunk_.int64_columns[src_col][outer_row];
							chunk.double_columns[out_col][chunk.count] =
								outer_chunk_.double_columns[src_col][outer_row];
							break;
						case VecOutputStorageKind::StringRef:
							chunk.string_columns[out_col][chunk.count] =
								CopyStringRefToChunk(chunk, outer_chunk_,
													 outer_chunk_.string_columns[src_col][outer_row]);
							break;
						case VecOutputStorageKind::Int32:
							chunk.int32_columns[out_col][chunk.count] =
								outer_chunk_.int32_columns[src_col][outer_row];
							break;
					}
				}
				chunk.count++;
			}

			if (anti_outer_pos_ >= active_count)
				outer_chunk_.reset();
			if (chunk.count > 0)
				return true;
		}

		return chunk.count > 0;
	}
	if (jointype_ == JOIN_SEMI)
	{
		if (build_outer_side_)
		{
			if (!semi_build_marked_)
			{
				while (advance_outer_batch())
				{
					int active_count = outer_chunk_.has_selection ? outer_chunk_.sel.count : outer_chunk_.count;

				for (int s = 0; s < active_count; s++)
				{
					int probe_row = outer_chunk_.has_selection ? outer_chunk_.sel.row_ids[s] : s;
					VecHashJoinKey key;
					uint32_t hash;
					int32_t entry_idx;

					if (!read_key(outer_chunk_, true, probe_row, &key))
						continue;
					hash = hash_key(key);
					uint16_t partition_id = (shared_hash_partition_count_ == 1) ? 0 : (uint16_t) volvec_radix_partition_idx(hash);
					const int32_t *bucket_heads = use_parallel_ht_ ? 
						bucket_heads_for_partition(partition_id) : active_bucket_heads();
					size_t mask = use_parallel_ht_ ? 
						active_bucket_mask_for_partition(partition_id) : bucket_mask_;
					entry_idx = bucket_heads == nullptr ? -1 : bucket_heads[hash & mask];
					while (entry_idx >= 0)
					{
				const VecHashEntry &entry = get_entry_at(entry_idx);

				if (entry.hash == hash &&
					keys_equal(entry.key, key) &&
					candidate_passes_join_filter_for_build_entry(outer_chunk_,
										 probe_row,
										 partition_id,
										 entry.chunk_idx,
										 entry.row_idx))
					inner_entry_matched_[entry_idx] = 1;
				entry_idx = entry.next;
				}
				}
			}
			semi_build_marked_ = true;
			semi_build_emit_pos_ = 0;
			}

			while (semi_build_emit_pos_ < active_entry_count() && chunk.count < DEFAULT_CHUNK_SIZE)
			{
				const VecHashEntry &entry = get_entry_at(semi_build_emit_pos_);

				if (!inner_entry_matched_[semi_build_emit_pos_++])
					continue;
				for (const auto &output_col : output_cols_)
				{
					int out_col = output_col.output_resno - 1;
					int src_col = output_col.input_col;

					if (output_col.output_resno > visible_output_count_)
						continue;
					if (output_col.side != VecJoinSide::Outer)
						elog(ERROR, "pg_volvec semi join build-outer path cannot expose unmatched inner columns");
					copy_inner_payload_value_to_chunk(chunk,
										chunk.count,
										out_col,
										output_col.meta,
										src_col,
										0,
										entry.chunk_idx,
										entry.row_idx);
				}
				chunk.count++;
			}

			return chunk.count > 0;
		}

		while (chunk.count < DEFAULT_CHUNK_SIZE)
		{
			int active_count;

			if ((outer_chunk_.has_selection ? outer_chunk_.sel.count : outer_chunk_.count) == 0 &&
				!advance_outer_batch())
				break;

			active_count = outer_chunk_.has_selection ? outer_chunk_.sel.count : outer_chunk_.count;
			while (anti_outer_pos_ < active_count && chunk.count < DEFAULT_CHUNK_SIZE)
			{
				int outer_row = outer_chunk_.has_selection ?
					outer_chunk_.sel.row_ids[anti_outer_pos_] : anti_outer_pos_;
				VecHashJoinKey key;
				bool has_match = false;
				int32_t entry_idx = -1;
				int32_t matched_entry_idx = -1;
				uint16_t partition_id = 0;

			anti_outer_pos_++;
			if (read_key(outer_chunk_, false, outer_row, &key))
			{
				uint32_t hash = hash_key(key);
				partition_id = (shared_hash_partition_count_ == 1) ? 0 : (uint16_t) volvec_radix_partition_idx(hash);
				const int32_t *bucket_heads = use_parallel_ht_ ? 
					bucket_heads_for_partition(partition_id) : active_bucket_heads();
				size_t mask = use_parallel_ht_ ? 
					active_bucket_mask_for_partition(partition_id) : bucket_mask_;
				entry_idx = bucket_heads == nullptr ? -1 : bucket_heads[hash & mask];
				while (entry_idx >= 0)
				{
					const VecHashEntry &entry = get_entry_at(entry_idx);

					if (entry.hash == hash &&
						keys_equal(entry.key, key) &&
						candidate_passes_join_filter_for_build_entry(outer_chunk_,
											 outer_row,
											 partition_id,
											 entry.chunk_idx,
											 entry.row_idx))
					{
						has_match = true;
						matched_entry_idx = entry_idx;
						break;
					}
					entry_idx = entry.next;
			}
		}
		if (!has_match)
			continue;

		for (const auto &output_col : output_cols_)
		{
			int out_col = output_col.output_resno - 1;
			int src_col = output_col.input_col;
			const VecHashEntry &matched_entry = get_entry_at(matched_entry_idx);

			if (output_col.output_resno > visible_output_count_)
				continue;
					if (output_col.side == VecJoinSide::Outer)
						chunk.nulls[out_col][chunk.count] = outer_chunk_.nulls[src_col][outer_row];
					else
						copy_inner_payload_value_to_chunk(chunk,
										chunk.count,
										out_col,
										output_col.meta,
										src_col,
										partition_id,
										matched_entry.chunk_idx,
										matched_entry.row_idx);
					if (chunk.nulls[out_col][chunk.count])
						continue;
					switch (output_col.meta.storage_kind)
					{
						case VecOutputStorageKind::Double:
							if (output_col.side == VecJoinSide::Outer)
								chunk.double_columns[out_col][chunk.count] =
									outer_chunk_.double_columns[src_col][outer_row];
							break;
						case VecOutputStorageKind::Int64:
						case VecOutputStorageKind::NumericScaledInt64:
						case VecOutputStorageKind::NumericAvgPair:
							if (output_col.side == VecJoinSide::Outer)
							{
								chunk.int64_columns[out_col][chunk.count] =
									outer_chunk_.int64_columns[src_col][outer_row];
								chunk.double_columns[out_col][chunk.count] =
									outer_chunk_.double_columns[src_col][outer_row];
							}
							break;
						case VecOutputStorageKind::StringRef:
							if (output_col.side == VecJoinSide::Outer)
								chunk.string_columns[out_col][chunk.count] =
									CopyStringRefToChunk(chunk, outer_chunk_,
												 outer_chunk_.string_columns[src_col][outer_row]);
							break;
						case VecOutputStorageKind::Int32:
							if (output_col.side == VecJoinSide::Outer)
								chunk.int32_columns[out_col][chunk.count] =
									outer_chunk_.int32_columns[src_col][outer_row];
							break;
					}
				}
				chunk.count++;
			}

			if (anti_outer_pos_ >= active_count)
				outer_chunk_.reset();
			if (chunk.count > 0)
				return true;
		}

		return chunk.count > 0;
	}
	if (jointype_ == JOIN_RIGHT_ANTI)
	{
		if (!right_anti_marked_)
		{
			while (advance_outer_batch())
			{
				int active_count = outer_chunk_.has_selection ? outer_chunk_.sel.count : outer_chunk_.count;

			for (int s = 0; s < active_count; s++)
			{
				int outer_row = outer_chunk_.has_selection ? outer_chunk_.sel.row_ids[s] : s;
				VecHashJoinKey key;
				uint32_t hash;
				int32_t entry_idx;

				if (!read_key(outer_chunk_, false, outer_row, &key))
					continue;
					hash = hash_key(key);
					uint16_t partition_id = (shared_hash_partition_count_ == 1) ? 0 : (uint16_t) volvec_radix_partition_idx(hash);
					const int32_t *bucket_heads = use_parallel_ht_ ?
					bucket_heads_for_partition(partition_id) : active_bucket_heads();
				size_t mask = use_parallel_ht_ ? 
					active_bucket_mask_for_partition(partition_id) : bucket_mask_;
			entry_idx = bucket_heads == nullptr ? -1 : bucket_heads[hash & mask];
			while (entry_idx >= 0)
			{
			const VecHashEntry &entry = get_entry_at(entry_idx);

			if (entry.hash == hash &&
				keys_equal(entry.key, key) &&
				candidate_passes_join_filter_for_build_entry(outer_chunk_,
									 outer_row,
									 partition_id,
									 entry.chunk_idx,
									 entry.row_idx))
				inner_entry_matched_[entry_idx] = 1;
			entry_idx = entry.next;
			}
		}
			}
			right_anti_marked_ = true;
			right_anti_emit_pos_ = 0;
		}

		while (right_anti_emit_pos_ < active_entry_count() && chunk.count < DEFAULT_CHUNK_SIZE)
		{
			const VecHashEntry &entry = get_entry_at(right_anti_emit_pos_);

			if (inner_entry_matched_[right_anti_emit_pos_++])
				continue;
			for (const auto &output_col : output_cols_)
			{
				int out_col = output_col.output_resno - 1;
				int src_col = output_col.input_col;

				if (output_col.output_resno > visible_output_count_)
					continue;
				if (output_col.side != VecJoinSide::Inner)
					elog(ERROR, "pg_volvec right anti join does not support outer output columns");
				copy_inner_payload_value_to_chunk(chunk,
									chunk.count,
									out_col,
									output_col.meta,
									src_col,
									0,
									entry.chunk_idx,
									entry.row_idx);
			}
			chunk.count++;
		}

		return chunk.count > 0;
	}

	while (chunk.count < DEFAULT_CHUNK_SIZE)
	{
		if (!probe_batch_ready_)
		{
			if ((outer_chunk_.has_selection ? outer_chunk_.sel.count : outer_chunk_.count) == 0 &&
				!advance_outer_batch())
				break;
			prepare_probe_batch();
		}

		if (active_probe_sel_.empty())
		{
			outer_chunk_.reset();
			probe_batch_ready_ = false;
			continue;
		}

		next_probe_sel_.clear();
		ProbeCandidate candidate{};
		while (next_probe_candidate(&candidate))
		{
			int32_t match_entry_idx;
			uint16_t probe_idx = candidate.probe_idx;

			if (chunk.count >= DEFAULT_CHUNK_SIZE)
			{
				next_probe_sel_.push_back(probe_idx);
				continue;
			}
			if (advance_probe_match(probe_idx, &match_entry_idx))
			{
				const VecHashEntry &entry = get_entry_at(match_entry_idx);
				int outer_row = probe_rows_[probe_idx];
				int dst_row = chunk.count++;
				bool probe_is_outer = !build_outer_side_;

				if (jointype_ == JOIN_RIGHT)
					inner_entry_matched_[match_entry_idx] = 1;

				for (const auto &output_col : output_cols_)
				{
					int out_col = output_col.output_resno - 1;
					int src_col = output_col.input_col;

					if ((output_col.side == VecJoinSide::Outer && probe_is_outer) ||
						(output_col.side == VecJoinSide::Inner && !probe_is_outer))
						copy_output_value(dst_row, out_col, output_col.meta,
									  outer_chunk_, src_col, outer_row);
					else
					copy_inner_payload_value_to_chunk(chunk,
										  dst_row,
										  out_col,
										  output_col.meta,
										  src_col,
										  probe_partition_ids_[probe_idx],
										  entry.chunk_idx,
										  entry.row_idx);
				}

				if (probe_next_entries_[probe_idx] >= 0)
					next_probe_sel_.push_back(probe_idx);
			}
			else if (jointype_ == JOIN_LEFT && !build_outer_side_)
			{
				int outer_row = probe_rows_[probe_idx];
				int dst_row = chunk.count++;

				for (const auto &output_col : output_cols_)
				{
					int out_col = output_col.output_resno - 1;

					if (output_col.side == VecJoinSide::Outer)
						copy_output_value(dst_row, out_col, output_col.meta,
										  outer_chunk_, output_col.input_col, outer_row);
					else
					{
						chunk.nulls[out_col][dst_row] = 1;
						chunk.string_columns[out_col][dst_row] = VecStringRef{0, 0, 0};
					}
				}
			}
		}

		active_probe_sel_.swap(next_probe_sel_);
		if (!active_probe_sel_.empty())
			assign_probe_candidates_from_partition(0);
		if (active_probe_sel_.empty())
		{
			outer_chunk_.reset();
			probe_batch_ready_ = false;
		}
	}

	if (jointype_ == JOIN_RIGHT)
	{
		if (!right_anti_marked_)
		{
			if (!probe_input_exhausted_)
				return chunk.count > 0;
			right_anti_marked_ = true;
			right_anti_emit_pos_ = 0;
		}

		while (right_anti_emit_pos_ < active_entry_count() && chunk.count < DEFAULT_CHUNK_SIZE)
		{
			const VecHashEntry &entry = get_entry_at(right_anti_emit_pos_);

			if (inner_entry_matched_[right_anti_emit_pos_++])
				continue;
			for (const auto &output_col : output_cols_)
			{
				int out_col = output_col.output_resno - 1;

				if (output_col.side == VecJoinSide::Outer)
				{
					chunk.nulls[out_col][chunk.count] = 1;
					chunk.string_columns[out_col][chunk.count] = VecStringRef{0, 0, 0};
					continue;
				}
				copy_inner_payload_value_to_chunk(chunk,
									chunk.count,
									out_col,
									output_col.meta,
									output_col.input_col,
									use_parallel_ht_ ? volvec_radix_partition_idx(entry.hash) : 0,
									entry.chunk_idx,
									entry.row_idx);
			}
			chunk.count++;
		}
	}

	return chunk.count > 0;
}
	struct AggrefRewriteContext
{
	const VolVecVector<const Aggref *> *aggrefs;
	const VolVecVector<int> *aggresnos;
	const VolVecVector<const Expr *> *group_exprs;
	const VolVecVector<int> *group_resnos;
};

static bool
ExprContainsNumericDivisionWalker(Node *node, void *context)
{
	bool *found = (bool *) context;

	if (node == nullptr || *found)
		return false;
	if (IsA(node, OpExpr))
	{
		OpExpr *op = (OpExpr *) node;
		char *opname = get_opname(op->opno);

		if (opname != nullptr && strcmp(opname, "/") == 0)
		{
			*found = true;
			return false;
		}
	}
	return expression_tree_walker(node, ExprContainsNumericDivisionWalker, context);
}

static bool
ExprContainsNumericDivision(Node *node)
{
	bool found = false;

	if (node != nullptr)
		(void) ExprContainsNumericDivisionWalker(node, &found);
	return found;
}

static bool
CollectAggrefsWalker(Node *node, void *context)
{
	VolVecVector<const Aggref *> *aggrefs = (VolVecVector<const Aggref *> *) context;

	if (node == nullptr)
		return false;
	if (IsA(node, Aggref))
	{
		aggrefs->push_back((const Aggref *) node);
		return false;
	}
	return expression_tree_walker(node, CollectAggrefsWalker, context);
}

static Node *
ReplaceAggrefsWithVarsMutator(Node *node, AggrefRewriteContext *context)
{
	if (node == nullptr)
		return nullptr;
	if (IsA(node, Aggref))
	{
		const Aggref *aggref = (const Aggref *) node;

		for (size_t i = 0; i < context->aggrefs->size(); i++)
		{
			if ((*context->aggrefs)[i] == aggref)
			{
				int resno = (*context->aggresnos)[i];
				Var *replacement = makeVar(OUTER_VAR,
										   resno,
										   exprType((Node *) aggref),
										   exprTypmod((Node *) aggref),
										   exprCollation((Node *) aggref),
										   0);

				return (Node *) replacement;
			}
		}
		elog(ERROR, "pg_volvec could not rewrite aggregate reference");
	}
	if (IsRewriteExprNode(node) &&
		context->group_exprs != nullptr && context->group_resnos != nullptr)
	{
		Expr *expr = StripImplicitNodesLocal((Expr *) node);

		for (size_t i = 0; i < context->group_exprs->size(); i++)
		{
			if (!equal(expr, (*context->group_exprs)[i]))
				continue;
			return (Node *) makeVar(OUTER_VAR,
									(*context->group_resnos)[i],
									exprType(node),
									exprTypmod(node),
									exprCollation(node),
									0);
		}
	}
	return expression_tree_mutator(node, ReplaceAggrefsWithVarsMutator, context);
}

static bool
CollectAggGroupExprs(Agg *node,
					 VolVecVector<const Expr *> *group_exprs,
					 VolVecVector<int> *group_resnos,
					 List **synthetic_tlist,
					 int *next_resno)
{
	Plan *child_plan;

	if (node == nullptr || group_exprs == nullptr || group_resnos == nullptr ||
		synthetic_tlist == nullptr || next_resno == nullptr)
		return false;

	child_plan = node->plan.lefttree;
	if (node->numCols == 0)
		return true;
	if (child_plan == nullptr)
		return false;

	for (int g = 0; g < node->numCols; g++)
	{
		int child_resno = node->grpColIdx[g];
		TargetEntry *child_tle = get_tle_by_resno(child_plan->targetlist, child_resno);
		Expr *group_expr;

		if (child_tle == nullptr)
			return false;
		group_expr = StripImplicitNodesLocal((Expr *) child_tle->expr);
		if (group_expr == nullptr)
			return false;

		group_exprs->push_back(group_expr);
		group_resnos->push_back(*next_resno);
		*synthetic_tlist = lappend(*synthetic_tlist,
								   makeTargetEntry((Expr *) copyObjectImpl(child_tle->expr),
												   *next_resno,
												   NULL,
												   false));
		(*next_resno)++;
	}

	return true;
}

static bool
IsSimpleAggTargetExpr(Agg *node, Expr *expr)
{
	expr = StripImplicitNodesLocal(expr);
	return expr != nullptr &&
		(IsA(expr, Aggref) || IsA(expr, Var) ||
		 ResolveAggPassThroughExpr(node, expr, nullptr, nullptr));
}

static VecOutputStorageKind
InferProjectStorageKind(Expr *expr, VecExprProgram *program)
{
	Oid typid = exprType((Node *) expr);

	if (typid == BPCHAROID || typid == TEXTOID || typid == VARCHAROID)
		return VecOutputStorageKind::StringRef;
	if (typid == FLOAT8OID)
		return VecOutputStorageKind::Double;
	if (typid == NUMERICOID)
	{
		if (ExprContainsNumericDivision((Node *) expr))
			return VecOutputStorageKind::Double;
		return VecOutputStorageKind::NumericScaledInt64;
	}
	if (typid == INT8OID)
		return VecOutputStorageKind::Int64;
	return VecOutputStorageKind::Int32;
}

std::unique_ptr<VecPlanState>
BuildAggWithOptionalProject(std::unique_ptr<VecPlanState> left, Agg *node,
							EState *estate, bool suppress_qual_filter)
{
	bool simple_targets = true;
	bool needs_synthetic_path;
	ListCell *lc;

	foreach(lc, node->plan.targetlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);

		if (!IsSimpleAggTargetExpr(node, (Expr *) tle->expr))
		{
			simple_targets = false;
			break;
		}
	}

	needs_synthetic_path = !simple_targets || node->plan.qual != NIL;

	if (!needs_synthetic_path)
		return std::make_unique<VecAggState>(std::move(left), node);

	VolVecVector<const Aggref *> aggrefs{PgMemoryContextAllocator<const Aggref *>(CurrentMemoryContext)};
	VolVecVector<int> aggresnos{PgMemoryContextAllocator<int>(CurrentMemoryContext)};
	VolVecVector<const Expr *> group_exprs{PgMemoryContextAllocator<const Expr *>(CurrentMemoryContext)};
	VolVecVector<int> group_resnos{PgMemoryContextAllocator<int>(CurrentMemoryContext)};
	List *synthetic_tlist = NIL;
	int next_resno = 1;
	std::unique_ptr<VecPlanState> current_state;

	if (!CollectAggGroupExprs(node, &group_exprs, &group_resnos, &synthetic_tlist, &next_resno))
	{
		if (pg_volvec_trace_hooks)
			elog(LOG, "pg_volvec: aggregate project rewrite could not collect grouped expressions");
		return nullptr;
	}

	foreach(lc, node->plan.targetlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		(void) CollectAggrefsWalker((Node *) tle->expr, &aggrefs);
	}
	if (node->plan.qual != NIL)
		(void) CollectAggrefsWalker((Node *) node->plan.qual, &aggrefs);
	for (const Aggref *aggref : aggrefs)
	{
		TargetEntry *agg_tle = makeTargetEntry((Expr *) copyObjectImpl(aggref),
											   next_resno,
											   NULL,
											   false);
		aggresnos.push_back(next_resno);
		synthetic_tlist = lappend(synthetic_tlist, agg_tle);
		next_resno++;
	}
	if (aggrefs.empty())
	{
		if (group_exprs.empty())
		{
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: aggregate project rewrite found no Aggref or grouped expressions");
			return nullptr;
		}
	}

	Agg *synthetic = (Agg *) palloc0(sizeof(Agg));
	*synthetic = *node;
	synthetic->plan.targetlist = synthetic_tlist;
	synthetic->plan.qual = NIL;

	auto agg_state = std::make_unique<VecAggState>(std::move(left), synthetic);
	VolVecVector<VecProjectColDesc> project_cols{PgMemoryContextAllocator<VecProjectColDesc>(CurrentMemoryContext)};
	AggrefRewriteContext rewrite_context{&aggrefs, &aggresnos, &group_exprs, &group_resnos};
	current_state = std::move(agg_state);

	if (node->plan.qual != NIL && !suppress_qual_filter)
	{
		auto qual_program = std::make_unique<VecExprProgram>();
		Expr *combined_qual = (Expr *) make_ands_explicit(list_copy(node->plan.qual));
		Expr *rewritten_qual =
			(Expr *) ReplaceAggrefsWithVarsMutator((Node *) combined_qual, &rewrite_context);

		CompileExpr(rewritten_qual, *qual_program, true, estate);
		AdjustProgramVarScales(qual_program.get(), current_state.get());
		if (qual_program->get_final_res_idx() < 0)
		{
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: aggregate qual rewrite/compilation failed");
			return nullptr;
		}
		current_state = std::make_unique<VecFilterState>(std::move(current_state),
														 std::move(qual_program));
	}

	foreach(lc, node->plan.targetlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		VecProjectColDesc project_col;
		Expr *rewritten_expr =
			(Expr *) ReplaceAggrefsWithVarsMutator((Node *) tle->expr, &rewrite_context);
		Expr *stripped_expr = StripImplicitNodesLocal(rewritten_expr);

		project_col.target_resno = tle->resno;
		project_col.sql_type = exprType((Node *) tle->expr);
		if (stripped_expr != nullptr && IsA(stripped_expr, Var))
		{
			Var *var = (Var *) stripped_expr;
			VecOutputColMeta meta;

			if (var->varattno <= 0 || var->varattno > 16 ||
				!current_state->lookup_output_col_meta(var->varattno, &meta))
			{
				if (pg_volvec_trace_hooks)
					elog(LOG, "pg_volvec: aggregate project direct-var metadata lookup failed for target resno %d",
						 tle->resno);
				return nullptr;
			}
			project_col.expr = nullptr;
			project_col.storage_kind = meta.storage_kind;
			project_col.scale = meta.scale;
			project_col.direct_var = true;
			project_col.input_col = (uint16_t) (var->varattno - 1);
		}
		else if (MatchStringPrefixExpr(stripped_expr,
									   &project_col.input_col,
									   &project_col.string_prefix_len))
		{
			VecOutputColMeta meta;

			if (!left->lookup_output_col_meta(project_col.input_col + 1, &meta) ||
				meta.storage_kind != VecOutputStorageKind::StringRef)
			{
				if (pg_volvec_trace_hooks)
					elog(LOG, "pg_volvec: hash join string-prefix project metadata lookup failed for target resno %d",
						 tle->resno);
				return nullptr;
			}
			project_col.expr = nullptr;
			project_col.storage_kind = VecOutputStorageKind::StringRef;
			project_col.scale = 0;
			project_col.string_prefix_var = true;
		}
		else
		{
			if (pg_volvec_trace_hooks &&
				(project_col.sql_type == BPCHAROID ||
				 project_col.sql_type == TEXTOID ||
				 project_col.sql_type == VARCHAROID))
			{
				if (stripped_expr != nullptr && IsA(stripped_expr, FuncExpr))
				{
					FuncExpr *func = (FuncExpr *) stripped_expr;
					Expr *arg0 = list_length(func->args) > 0 ?
						StripImplicitNodesLocal((Expr *) linitial(func->args)) : nullptr;
					Expr *arg1 = list_length(func->args) > 1 ?
						StripImplicitNodesLocal((Expr *) lsecond(func->args)) : nullptr;
					Expr *arg2 = list_length(func->args) > 2 ?
						StripImplicitNodesLocal((Expr *) lthird(func->args)) : nullptr;

					elog(LOG,
						 "pg_volvec: hash join string project fallback expr func=%s nargs=%d rettype=%u arg0_tag=%d arg0_type=%u arg1_tag=%d arg2_tag=%d",
						 get_func_name(func->funcid),
						 list_length(func->args),
						 exprType((Node *) stripped_expr),
						 arg0 != nullptr ? (int) nodeTag(arg0) : -1,
						 arg0 != nullptr ? exprType((Node *) arg0) : InvalidOid,
						 arg1 != nullptr ? (int) nodeTag(arg1) : -1,
						 arg2 != nullptr ? (int) nodeTag(arg2) : -1);
				}
				else
				{
					elog(LOG,
						 "pg_volvec: hash join string project fallback expr node_tag=%d rettype=%u",
						 stripped_expr != nullptr ? (int) nodeTag(stripped_expr) : -1,
						 exprType((Node *) tle->expr));
				}
			}
			project_col.expr = std::make_unique<VecExprProgram>();
			CompileExpr(rewritten_expr, *project_col.expr, false, estate);
			AdjustProgramVarScales(project_col.expr.get(), current_state.get());
			if (project_col.expr->get_final_res_idx() < 0)
			{
				if (pg_volvec_trace_hooks)
					elog(LOG, "pg_volvec: aggregate project expression compilation failed for target resno %d",
						 tle->resno);
				return nullptr;
			}
			project_col.storage_kind = InferProjectStorageKind((Expr *) tle->expr, project_col.expr.get());
			project_col.scale = project_col.expr->get_register_scale(project_col.expr->get_final_res_idx());
		}
		project_cols.push_back(std::move(project_col));
	}

	return std::make_unique<VecProjectState>(std::move(current_state), std::move(project_cols));
}

bool
LookupPlanOutputMeta(Plan *plan,
					 VecPlanState *state,
					 int target_resno,
					 uint16_t *source_col,
					 VecOutputColMeta *meta)
{
	ListCell *lc;

	if (plan == nullptr || state == nullptr || target_resno <= 0 || target_resno > 16)
		return false;

	if (IsA(plan, Hash))
	{
		foreach(lc, plan->targetlist)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(lc);
			Expr *expr;
			Var *var;

			if (tle->resno != target_resno)
				continue;
			expr = StripImplicitNodesLocal((Expr *) tle->expr);
			if (expr == nullptr || !IsA(expr, Var))
				return false;
			var = (Var *) expr;
			if (var->varattno <= 0 || var->varattno > 16)
				return false;
			if (source_col != nullptr)
				*source_col = (uint16_t) (var->varattno - 1);
			return state->lookup_output_col_meta(var->varattno, meta);
		}
		if (pg_volvec_trace_hooks)
			elog(LOG, "pg_volvec: hash output metadata lookup found no targetlist entry for resno %d",
				 target_resno);
		return false;
	}

	foreach(lc, plan->targetlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		Expr *expr;
		Var *var;

		if (tle->resno != target_resno || tle->resjunk)
			continue;
		expr = StripImplicitNodesLocal((Expr *) tle->expr);
		if (expr == nullptr || !IsA(expr, Var))
			break;
		var = (Var *) expr;
		if (var->varattno <= 0 || var->varattno > 16)
			break;
		if (state->lookup_remapped_output_col_meta(var->varattno, source_col, meta))
			return true;
		break;
	}

	if (source_col != nullptr)
		*source_col = (uint16_t) (target_resno - 1);
	return state->lookup_output_col_meta(target_resno, meta);
}

static bool
ResolveHashJoinVarBinding(Var *var,
						  Plan *outer_plan,
						  Plan *inner_plan,
						  VecPlanState *outer,
						  VecPlanState *inner,
						  VecJoinSide *side,
						  uint16_t *source_col,
						  VecOutputColMeta *meta)
{
	if (var == nullptr || source_col == nullptr || meta == nullptr)
		return false;
	if (var->varattno <= 0 || var->varattno > 16)
		return false;

	if (var->varno == OUTER_VAR)
	{
		if (!LookupPlanOutputMeta(outer_plan, outer, var->varattno, source_col, meta))
			return false;
		if (side != nullptr)
			*side = VecJoinSide::Outer;
		return true;
	}
	if (var->varno == INNER_VAR)
	{
		if (!LookupPlanOutputMeta(inner_plan, inner, var->varattno, source_col, meta))
			return false;
		if (side != nullptr)
			*side = VecJoinSide::Inner;
		return true;
	}
	return false;
}

static bool
EnsureHashJoinOutputCol(VecJoinSide side,
						  uint16_t source_col,
						  const VecOutputColMeta &meta,
						  VolVecVector<VecJoinOutputCol> *output_cols,
						  int *join_resno)
{
	if (output_cols == nullptr)
		return false;

	for (const auto &output_col : *output_cols)
	{
		if (output_col.side == side && output_col.input_col == source_col)
		{
			if (join_resno != nullptr)
				*join_resno = output_col.output_resno;
			return true;
		}
	}

	if (output_cols->size() >= 16)
		return false;

	output_cols->push_back(VecJoinOutputCol{side,
											 source_col,
											 (int) output_cols->size() + 1,
											 meta});
	if (join_resno != nullptr)
		*join_resno = output_cols->back().output_resno;
	return true;
}

static bool
IsLookupCompatibleStorage(VecOutputStorageKind kind)
{
	return kind == VecOutputStorageKind::Int32 ||
		   kind == VecOutputStorageKind::Int64 ||
		   kind == VecOutputStorageKind::NumericScaledInt64;
}

static Plan *
LookupReferencedSubPlan(EState *estate, SubPlan *subplan)
{
	if (estate == nullptr || estate->es_plannedstmt == nullptr ||
		subplan == nullptr || subplan->plan_id <= 0)
		return nullptr;
	if (list_length(estate->es_plannedstmt->subplans) < subplan->plan_id)
		return nullptr;
	return (Plan *) list_nth(estate->es_plannedstmt->subplans, subplan->plan_id - 1);
}

static bool
ExtractParamEqualityVar(Expr *expr, int paramid, Var **scan_var)
{
	OpExpr *op;
	Expr *left;
	Expr *right;
	Param *param = nullptr;
	Var *var = nullptr;
	char *opname;

	expr = StripImplicitNodesLocal(expr);
	if (scan_var != nullptr)
		*scan_var = nullptr;
	if (expr == nullptr || !IsA(expr, OpExpr))
		return false;
	op = (OpExpr *) expr;
	if (list_length(op->args) != 2)
		return false;
	opname = get_opname(op->opno);
	if (opname == nullptr || strcmp(opname, "=") != 0)
		return false;

	left = StripImplicitNodesLocal((Expr *) linitial(op->args));
	right = StripImplicitNodesLocal((Expr *) lsecond(op->args));
	if (left != nullptr && right != nullptr && IsA(left, Var) && IsA(right, Param))
	{
		var = (Var *) left;
		param = (Param *) right;
	}
	else if (left != nullptr && right != nullptr && IsA(left, Param) && IsA(right, Var))
	{
		var = (Var *) right;
		param = (Param *) left;
	}
	else
		return false;

	if (param->paramkind != PARAM_EXEC || param->paramid != paramid ||
		var->varlevelsup != 0 || var->varattno <= 0 || var->varattno > kMaxDeformTargets)
		return false;
	if (scan_var != nullptr)
		*scan_var = var;
	return true;
}

static void
StripParamEqualityFromPlanQuals(Plan *plan, int paramid, Var **scan_var, bool *removed)
{
	List *new_quals = NIL;
	ListCell *lc;

	if (plan == nullptr)
		return;

	foreach(lc, plan->qual)
	{
		Expr *qual = (Expr *) lfirst(lc);
		Var *matched_var = nullptr;

		if (ExtractParamEqualityVar(qual, paramid, &matched_var))
		{
			if (scan_var != nullptr && matched_var != nullptr)
			{
				if (*scan_var == nullptr)
					*scan_var = (Var *) copyObjectImpl(matched_var);
				else if (!equal(*scan_var, matched_var))
					*removed = false;
			}
			if (removed != nullptr)
				*removed = true;
			continue;
		}
		new_quals = lappend(new_quals, qual);
	}
	plan->qual = new_quals;

	StripParamEqualityFromPlanQuals(plan->lefttree, paramid, scan_var, removed);
	StripParamEqualityFromPlanQuals(plan->righttree, paramid, scan_var, removed);
	if (IsA(plan, SubqueryScan))
		StripParamEqualityFromPlanQuals(((SubqueryScan *) plan)->subplan,
										paramid,
										scan_var,
										removed);
}

static bool
PlanContainsScanRelid(Plan *plan, Index scanrelid)
{
	if (plan == nullptr || scanrelid <= 0)
		return false;
	if (IsA(plan, SeqScan))
		return ((SeqScan *) plan)->scan.scanrelid == scanrelid;
	if (IsA(plan, SubqueryScan) &&
		PlanContainsScanRelid(((SubqueryScan *) plan)->subplan, scanrelid))
		return true;
	if (PlanContainsScanRelid(plan->lefttree, scanrelid))
		return true;
	return PlanContainsScanRelid(plan->righttree, scanrelid);
}

static Expr *
BuildJoinLookupKeyExpr(Plan *join_plan, Var *scan_key_var)
{
	bool in_outer;
	bool in_inner;

	if (join_plan == nullptr || scan_key_var == nullptr)
		return nullptr;
	in_outer = PlanContainsScanRelid(join_plan->lefttree, scan_key_var->varno);
	in_inner = PlanContainsScanRelid(join_plan->righttree, scan_key_var->varno);
	if (in_outer == in_inner)
		return nullptr;

	return (Expr *) makeVar(in_outer ? OUTER_VAR : INNER_VAR,
							scan_key_var->varattno,
							scan_key_var->vartype,
							scan_key_var->vartypmod,
							scan_key_var->varcollid,
							0);
}

static std::unique_ptr<VecPlanState>
BuildCorrelatedLookupAggState(EState *estate,
							  Agg *subplan_agg,
							  int paramid,
							  VecOutputColMeta *key_meta,
							  VecOutputColMeta *value_meta,
							  const ParallelWorkerContext *parallel_worker_context)
{
	Agg *grouped_agg;
	Plan *grouped_child;
	TargetEntry *value_tle;
	std::unique_ptr<VecPlanState> lookup_state;
	Aggref *lookup_aggref;
	TargetEntry *lookup_arg_tle;
	TargetEntry *child_value_tle;
	Expr *child_key_expr;
	bool removed = false;
	Var *extracted_key = nullptr;
	Var *lookup_key_var;

	if (estate == nullptr || subplan_agg == nullptr ||
		subplan_agg->plan.lefttree == nullptr ||
		subplan_agg->plan.targetlist == NIL)
		return nullptr;

	grouped_agg = (Agg *) copyObjectImpl(subplan_agg);
	grouped_child = grouped_agg->plan.lefttree;
	value_tle = (TargetEntry *) linitial(subplan_agg->plan.targetlist);
	StripParamEqualityFromPlanQuals(grouped_child, paramid, &extracted_key, &removed);
	lookup_key_var = extracted_key;
	if (!removed || lookup_key_var == nullptr)
		return nullptr;

	if (IsA(subplan_agg->plan.lefttree, SeqScan))
	{
		grouped_agg->aggstrategy = AGG_HASHED;
		grouped_agg->numCols = 1;
		grouped_agg->grpColIdx = (AttrNumber *) palloc(sizeof(AttrNumber));
		grouped_agg->grpColIdx[0] = (AttrNumber) lookup_key_var->varattno;
		grouped_agg->plan.qual = NIL;
		grouped_agg->plan.targetlist =
			list_make2(makeTargetEntry((Expr *) copyObjectImpl(lookup_key_var),
									   1,
									   NULL,
									   false),
					   makeTargetEntry((Expr *) copyObjectImpl(value_tle->expr),
									   2,
									   NULL,
									   false));
	}
	else if (IsA(subplan_agg->plan.lefttree, HashJoin) ||
			 IsA(subplan_agg->plan.lefttree, MergeJoin))
	{
		child_key_expr = BuildJoinLookupKeyExpr(grouped_child, lookup_key_var);
		if (child_key_expr == nullptr || grouped_child->targetlist == NIL ||
			list_length(grouped_child->targetlist) != 1)
			return nullptr;

		child_value_tle = (TargetEntry *) linitial(grouped_child->targetlist);
		grouped_child->targetlist =
			list_make2(makeTargetEntry(child_key_expr,
									   1,
									   NULL,
									   false),
					   makeTargetEntry((Expr *) copyObjectImpl(child_value_tle->expr),
									   2,
									   NULL,
									   false));

		if (value_tle == nullptr || !IsA(StripImplicitNodesLocal((Expr *) value_tle->expr), Aggref))
			return nullptr;
		lookup_aggref = (Aggref *) copyObjectImpl(StripImplicitNodesLocal((Expr *) value_tle->expr));
		if (lookup_aggref->args == NIL || list_length(lookup_aggref->args) != 1)
			return nullptr;
		lookup_arg_tle = (TargetEntry *) linitial(lookup_aggref->args);
		lookup_arg_tle->expr =
			(Expr *) makeVar(1,
							 2,
							 exprType((Node *) child_value_tle->expr),
							 exprTypmod((Node *) child_value_tle->expr),
							 exprCollation((Node *) child_value_tle->expr),
							 0);

		grouped_agg->aggstrategy = AGG_HASHED;
		grouped_agg->numCols = 1;
		grouped_agg->grpColIdx = (AttrNumber *) palloc(sizeof(AttrNumber));
		grouped_agg->grpColIdx[0] = 1;
		grouped_agg->plan.qual = NIL;
		grouped_agg->plan.targetlist =
			list_make2(makeTargetEntry((Expr *) makeVar(1,
													  1,
													  lookup_key_var->vartype,
													  lookup_key_var->vartypmod,
													  lookup_key_var->varcollid,
													  0),
									   1,
									   NULL,
									   false),
					   makeTargetEntry((Expr *) lookup_aggref,
									   2,
									   NULL,
									   false));
	}
	else
	{
		return nullptr;
	}

	lookup_state = ExecInitVecPlanInternal((Plan *) grouped_agg,
										   estate,
										   nullptr,
										   false,
										   parallel_worker_context);
	if (!lookup_state)
		return nullptr;
	if ((key_meta != nullptr &&
		 !lookup_state->lookup_output_col_meta(1, key_meta)) ||
		(value_meta != nullptr &&
		 !lookup_state->lookup_output_col_meta(2, value_meta)))
		return nullptr;
	return lookup_state;
}

static bool
ExtractComparableLookupVar(Expr *expr, Var **var_out)
{
	Expr *stripped = StripImplicitNodesLocal(expr);

	if (var_out != nullptr)
		*var_out = nullptr;
	if (stripped == nullptr)
		return false;
	if (IsA(stripped, Var))
	{
		if (var_out != nullptr)
			*var_out = (Var *) stripped;
		return true;
	}
	if (IsA(stripped, FuncExpr))
	{
		FuncExpr *func = (FuncExpr *) stripped;
		Expr *arg;

		if (list_length(func->args) != 1 || exprType((Node *) stripped) != NUMERICOID)
			return false;
		arg = StripImplicitNodesLocal((Expr *) linitial(func->args));
		if (arg == nullptr || !IsA(arg, Var))
			return false;
		if (!IsInt64LikeTypeLocal(exprType((Node *) arg)))
			return false;
		if (var_out != nullptr)
			*var_out = (Var *) arg;
		return true;
	}
	return false;
}

static int
FindNextFreeOutputResno(VecPlanState *state)
{
	VecOutputColMeta meta;

	if (state == nullptr)
		return -1;
	for (int resno = 1; resno <= 16; resno++)
	{
		if (!state->lookup_output_col_meta(resno, &meta))
			return resno;
	}
	return -1;
}

static std::unique_ptr<VecPlanState>
BuildCorrelatedLookupAggStateMulti(EState *estate,
								   Agg *subplan_agg,
								   List *paramids,
								   List *args,
								   int *num_keys_out,
								   VecOutputColMeta *key_metas_out,
								   VecOutputColMeta *value_meta)
{
	Agg *grouped_agg;
	Plan *grouped_child;
	TargetEntry *value_tle;
	std::unique_ptr<VecPlanState> lookup_state;
	List *synthetic_tlist = NIL;
	ListCell *lc_param;
	ListCell *lc_arg;
	int num_keys = 0;

	if (estate == nullptr || subplan_agg == nullptr ||
		subplan_agg->plan.lefttree == nullptr ||
		subplan_agg->plan.targetlist == NIL ||
		paramids == NIL || args == NIL ||
		list_length(paramids) != list_length(args) ||
		list_length(paramids) <= 0 ||
		list_length(paramids) > kMaxLookupKeys)
		return nullptr;

	grouped_agg = (Agg *) copyObjectImpl(subplan_agg);
	grouped_child = grouped_agg->plan.lefttree;
	value_tle = (TargetEntry *) linitial(subplan_agg->plan.targetlist);

	if (!IsA(grouped_child, SeqScan))
		return nullptr;

	grouped_agg->aggstrategy = AGG_HASHED;
	grouped_agg->numCols = list_length(paramids);
	grouped_agg->grpColIdx = (AttrNumber *) palloc(sizeof(AttrNumber) * grouped_agg->numCols);
	grouped_agg->plan.qual = NIL;

	forboth(lc_param, paramids, lc_arg, args)
	{
		int paramid = lfirst_int(lc_param);
		Expr *arg_expr = StripImplicitNodesLocal((Expr *) lfirst(lc_arg));
		Var *lookup_key_var = nullptr;
		bool removed = false;

		if (arg_expr == nullptr || !IsA(arg_expr, Var))
			return nullptr;
		StripParamEqualityFromPlanQuals(grouped_child, paramid, &lookup_key_var, &removed);
		if (!removed || lookup_key_var == nullptr)
			return nullptr;
		grouped_agg->grpColIdx[num_keys] = (AttrNumber) lookup_key_var->varattno;
		synthetic_tlist =
			lappend(synthetic_tlist,
					makeTargetEntry((Expr *) copyObjectImpl(lookup_key_var),
									num_keys + 1,
									NULL,
									false));
		num_keys++;
	}

	synthetic_tlist =
		lappend(synthetic_tlist,
				makeTargetEntry((Expr *) copyObjectImpl(value_tle->expr),
								num_keys + 1,
								NULL,
								false));
	grouped_agg->plan.targetlist = synthetic_tlist;

	lookup_state = ExecInitVecPlanInternal((Plan *) grouped_agg, estate, nullptr, false, nullptr);
	if (!lookup_state)
		return nullptr;
	for (int i = 0; i < num_keys; i++)
	{
		if (key_metas_out != nullptr &&
			!lookup_state->lookup_output_col_meta(i + 1, &key_metas_out[i]))
			return nullptr;
	}
	if (value_meta != nullptr &&
		!lookup_state->lookup_output_col_meta(num_keys + 1, value_meta))
		return nullptr;
	if (num_keys_out != nullptr)
		*num_keys_out = num_keys;
	return lookup_state;
}

static bool
ExtractSubPlanLookupVar(SubPlan *subplan, Var **var_out)
{
	Expr *testexpr;
	OpExpr *op;
	Expr *left;
	Expr *right;
	char *opname;

	if (var_out != nullptr)
		*var_out = nullptr;
	if (subplan == nullptr || subplan->testexpr == nullptr)
		return false;

	testexpr = StripImplicitNodesLocal((Expr *) subplan->testexpr);
	if (testexpr == nullptr || !IsA(testexpr, OpExpr))
		return false;
	op = (OpExpr *) testexpr;
	if (list_length(op->args) != 2)
		return false;
	opname = get_opname(op->opno);
	if (opname == nullptr || strcmp(opname, "=") != 0)
		return false;

	left = StripImplicitNodesLocal((Expr *) linitial(op->args));
	right = StripImplicitNodesLocal((Expr *) lsecond(op->args));
	if (left != nullptr && IsA(left, Var) && right != nullptr && IsA(right, Param))
	{
		if (var_out != nullptr)
			*var_out = (Var *) left;
		return true;
	}
	if (left != nullptr && IsA(left, Param) && right != nullptr && IsA(right, Var))
	{
		if (var_out != nullptr)
			*var_out = (Var *) right;
		return true;
	}
	return false;
}

static bool
MatchLookupMembershipQual(Expr *expr,
						  VecPlanState *input_state,
						  EState *estate,
						  LookupMembershipFilterSpec *spec)
{
	bool negate = false;
	SubPlan *subplan;
	Plan *subplan_plan;
	Var *lookup_var = nullptr;
	std::unique_ptr<VecPlanState> lookup_state;
	Bitmapset *lookup_required_attrs = nullptr;

	if (expr == nullptr || input_state == nullptr || estate == nullptr || spec == nullptr)
		return false;

	expr = StripImplicitNodesLocal(expr);
	if (expr != nullptr && IsA(expr, BoolExpr))
	{
		BoolExpr *bool_expr = (BoolExpr *) expr;

		if (bool_expr->boolop == NOT_EXPR && list_length(bool_expr->args) == 1)
		{
			negate = true;
			expr = StripImplicitNodesLocal((Expr *) linitial(bool_expr->args));
		}
	}
	if (expr == nullptr || !IsA(expr, SubPlan))
		return false;

	subplan = (SubPlan *) expr;
	if (subplan->isInitPlan ||
		!subplan->useHashTable ||
		subplan->subLinkType != ANY_SUBLINK ||
		subplan->parParam != NIL ||
		subplan->args != NIL ||
		!ExtractSubPlanLookupVar(subplan, &lookup_var) ||
		lookup_var == nullptr ||
		lookup_var->varlevelsup != 0 ||
		lookup_var->varattno <= 0 ||
		lookup_var->varattno > kMaxDeformTargets ||
		!input_state->lookup_output_col_meta(lookup_var->varattno, &spec->input_key_meta) ||
		!IsLookupCompatibleStorage(spec->input_key_meta.storage_kind))
		return false;

	subplan_plan = LookupReferencedSubPlan(estate, subplan);
	if (subplan_plan == nullptr)
		return false;
	if (subplan_plan->targetlist != NIL)
		CollectAttrNosFromExpr((Node *) subplan_plan->targetlist, &lookup_required_attrs);
	if (subplan_plan->qual != NIL)
		CollectAttrNosFromExpr((Node *) subplan_plan->qual, &lookup_required_attrs);
	lookup_state = ExecInitVecPlanInternal(subplan_plan,
										   estate,
										   lookup_required_attrs,
										   false,
										   nullptr);
	if (!lookup_state ||
		!lookup_state->lookup_output_col_meta(1, &spec->lookup_key_meta) ||
		!IsLookupCompatibleStorage(spec->lookup_key_meta.storage_kind))
		return false;

	spec->lookup_state = std::move(lookup_state);
	spec->input_key_col = (uint16_t) (lookup_var->varattno - 1);
	spec->lookup_key_col = 0;
	spec->negate = negate;
	if (pg_volvec_trace_hooks)
		elog(LOG,
			 "pg_volvec: matched lookup membership qual (input_col=%u, negate=%d)",
			 spec->input_key_col,
			 spec->negate ? 1 : 0);
	return true;
}

bool
TryBuildLookupMembershipFilterSpec(Expr *expr,
								   VecPlanState *input_state,
								   EState *estate,
								   LookupMembershipFilterSpec *spec)
{
	List *qual_list = NIL;
	List *residual_list = NIL;
	ListCell *lc;
	bool matched = false;

	if (expr == nullptr || input_state == nullptr || estate == nullptr || spec == nullptr)
		return false;

	*spec = LookupMembershipFilterSpec{};
	expr = StripImplicitNodesLocal(expr);
	if (expr != nullptr && IsA(expr, BoolExpr) &&
		((BoolExpr *) expr)->boolop == AND_EXPR)
		qual_list = list_copy(((BoolExpr *) expr)->args);
	else
		qual_list = list_make1(expr);

	foreach(lc, qual_list)
	{
		Expr *qual = (Expr *) lfirst(lc);
		LookupMembershipFilterSpec candidate;

		if (!matched && MatchLookupMembershipQual(qual, input_state, estate, &candidate))
		{
			*spec = std::move(candidate);
			matched = true;
			continue;
		}
		residual_list = lappend(residual_list, qual);
	}

	if (!matched)
		return false;

	if (residual_list == NIL)
		spec->residual_expr = nullptr;
	else if (list_length(residual_list) == 1)
		spec->residual_expr = (Expr *) linitial(residual_list);
	else
		spec->residual_expr = (Expr *) make_ands_explicit(residual_list);
	return true;
}

static bool
MatchPlanCorrelatedLookupQual(Expr *expr,
							  VecPlanState *input_state,
							  EState *estate,
							  CorrelatedLookupProjectSpec *spec)
{
	Expr *stripped = StripImplicitNodesLocal(expr);
	OpExpr *compare_op;
	Expr *left_arg;
	Expr *right_arg;
	Expr *compare_expr = nullptr;
	Var *compare_var = nullptr;
	SubPlan *subplan = nullptr;
	Plan *subplan_plan;
	Agg *subplan_agg;
	VecOutputColMeta lookup_key_metas[kMaxLookupKeys];
	VecOutputColMeta value_meta;
	std::unique_ptr<VecPlanState> lookup_state;
	OpExpr *rewritten_op;
	List *rewritten_args = NIL;
	int num_keys = 0;
	ListCell *lc_arg;
	int arg_index = 0;

	if (expr == nullptr || input_state == nullptr || estate == nullptr || spec == nullptr)
		return false;
	if (stripped == nullptr || !IsA(stripped, OpExpr))
		return false;
	compare_op = (OpExpr *) stripped;
	if (list_length(compare_op->args) != 2)
		return false;

	left_arg = StripImplicitNodesLocal((Expr *) linitial(compare_op->args));
	right_arg = StripImplicitNodesLocal((Expr *) lsecond(compare_op->args));
	if (left_arg != nullptr && right_arg != nullptr &&
		!IsA(left_arg, SubPlan) && IsA(right_arg, SubPlan) &&
		ExtractComparableLookupVar(left_arg, &compare_var))
	{
		compare_expr = left_arg;
		subplan = (SubPlan *) right_arg;
	}
	else if (left_arg != nullptr && right_arg != nullptr &&
			 IsA(left_arg, SubPlan) && !IsA(right_arg, SubPlan) &&
			 ExtractComparableLookupVar(right_arg, &compare_var))
	{
		compare_expr = right_arg;
		subplan = (SubPlan *) left_arg;
	}
	else
		return false;

	if (subplan == nullptr ||
		subplan->subLinkType != EXPR_SUBLINK ||
		subplan->useHashTable ||
		subplan->parParam == NIL ||
		subplan->args == NIL ||
		list_length(subplan->parParam) != list_length(subplan->args) ||
		list_length(subplan->parParam) <= 0 ||
		list_length(subplan->parParam) > kMaxLookupKeys)
		return false;

	subplan_plan = LookupReferencedSubPlan(estate, subplan);
	if (subplan_plan == nullptr || !IsA(subplan_plan, Agg))
		return false;
	subplan_agg = (Agg *) subplan_plan;
	lookup_state = BuildCorrelatedLookupAggStateMulti(estate,
													  subplan_agg,
													  subplan->parParam,
													  subplan->args,
													  &num_keys,
													  lookup_key_metas,
													  &value_meta);
	if (!lookup_state)
		return false;

	spec->num_keys = num_keys;
	foreach(lc_arg, subplan->args)
	{
		Expr *arg_expr = StripImplicitNodesLocal((Expr *) lfirst(lc_arg));
		Var *arg_var = nullptr;
		VecOutputColMeta input_key_meta;

		if (arg_expr == nullptr || !IsA(arg_expr, Var))
			return false;
		arg_var = (Var *) arg_expr;
		if (arg_var->varattno <= 0 || arg_var->varattno > 16 ||
			!input_state->lookup_output_col_meta(arg_var->varattno, &input_key_meta) ||
			!IsLookupCompatibleStorage(input_key_meta.storage_kind) ||
			!IsLookupCompatibleStorage(lookup_key_metas[arg_index].storage_kind))
			return false;

		spec->input_key_cols[arg_index] = (uint16_t) (arg_var->varattno - 1);
		spec->input_key_metas[arg_index] = input_key_meta;
		spec->lookup_key_cols[arg_index] = (uint16_t) arg_index;
		spec->lookup_key_metas[arg_index] = lookup_key_metas[arg_index];
		arg_index++;
	}

	if (value_meta.storage_kind != VecOutputStorageKind::Int32 &&
		value_meta.storage_kind != VecOutputStorageKind::Int64 &&
		value_meta.storage_kind != VecOutputStorageKind::NumericScaledInt64 &&
		value_meta.storage_kind != VecOutputStorageKind::Double)
		return false;

	spec->lookup_state = std::move(lookup_state);
	spec->lookup_value_col = (uint16_t) num_keys;
	spec->output_resno = FindNextFreeOutputResno(input_state);
	spec->output_meta = value_meta;
	if (spec->output_resno <= 0)
		return false;

	rewritten_op = (OpExpr *) copyObjectImpl(compare_op);
	if (compare_expr == left_arg)
	{
		rewritten_args = list_make2((Node *) copyObjectImpl(compare_var),
									makeVar(1,
											spec->output_resno,
											value_meta.sql_type,
											-1,
											InvalidOid,
											0));
	}
	else
	{
		rewritten_args = list_make2(makeVar(1,
											spec->output_resno,
											value_meta.sql_type,
											-1,
											InvalidOid,
											0),
									(Node *) copyObjectImpl(compare_var));
	}
	rewritten_op->args = rewritten_args;
	spec->rewritten_expr = (Expr *) rewritten_op;
	return true;
}

bool
TryBuildPlanCorrelatedLookupProjectSpec(Expr *expr,
										VecPlanState *input_state,
										EState *estate,
										CorrelatedLookupProjectSpec *spec)
{
	List *qual_list = NIL;
	List *rewritten_quals = NIL;
	ListCell *lc;
	bool matched = false;

	if (expr == nullptr || input_state == nullptr || estate == nullptr || spec == nullptr)
		return false;

	*spec = CorrelatedLookupProjectSpec{};
	expr = StripImplicitNodesLocal(expr);
	if (expr != nullptr && IsA(expr, BoolExpr) &&
		((BoolExpr *) expr)->boolop == AND_EXPR)
		qual_list = list_copy(((BoolExpr *) expr)->args);
	else
		qual_list = list_make1(expr);

	foreach(lc, qual_list)
	{
		Expr *qual = (Expr *) lfirst(lc);
		CorrelatedLookupProjectSpec candidate;

		if (!matched && MatchPlanCorrelatedLookupQual(qual, input_state, estate, &candidate))
		{
			*spec = std::move(candidate);
			rewritten_quals = lappend(rewritten_quals, spec->rewritten_expr);
			matched = true;
			continue;
		}
		rewritten_quals = lappend(rewritten_quals, qual);
	}

	if (!matched)
		return false;

	if (rewritten_quals == NIL)
		spec->rewritten_expr = nullptr;
	else if (list_length(rewritten_quals) == 1)
		spec->rewritten_expr = (Expr *) linitial(rewritten_quals);
	else
		spec->rewritten_expr = (Expr *) make_ands_explicit(rewritten_quals);
	return true;
}

bool
TryBuildCorrelatedLookupFilterSpec(Expr *expr,
								   Plan *outer_plan,
								   Plan *inner_plan,
								   VecPlanState *outer,
								   VecPlanState *inner,
								   VolVecVector<VecJoinOutputCol> *output_cols,
								   EState *estate,
								   CorrelatedLookupFilterSpec *spec,
								   const ParallelWorkerContext *parallel_worker_context)
{
	Expr *stripped = StripImplicitNodesLocal(expr);
	OpExpr *compare_op;
	Expr *left_arg;
	Expr *right_arg;
	Var *compare_var = nullptr;
	SubPlan *subplan = nullptr;
	Var *corr_arg_var;
	int compare_resno;
	int key_resno;
	VecJoinSide key_side;
	VecOutputColMeta compare_meta;
	VecOutputColMeta key_meta;
	Plan *subplan_plan;
	Agg *subplan_agg;
	VecOutputColMeta lookup_key_meta;
	VecOutputColMeta lookup_value_meta;
	std::unique_ptr<VecPlanState> lookup_state;
	OpExpr *rewritten_op;
	List *rewritten_args;
	Var *compare_input_var;
	Var *lookup_var;

	if (spec == nullptr || output_cols == nullptr || estate == nullptr)
		return false;
	if (stripped == nullptr || !IsA(stripped, OpExpr))
		return false;
	compare_op = (OpExpr *) stripped;
	if (list_length(compare_op->args) != 2)
		return false;

	left_arg = StripImplicitNodesLocal((Expr *) linitial(compare_op->args));
	right_arg = StripImplicitNodesLocal((Expr *) lsecond(compare_op->args));
	if (left_arg != nullptr && right_arg != nullptr &&
		IsA(left_arg, Var) && IsA(right_arg, SubPlan))
	{
		compare_var = (Var *) left_arg;
		subplan = (SubPlan *) right_arg;
	}
	else if (left_arg != nullptr && right_arg != nullptr &&
			 IsA(left_arg, SubPlan) && IsA(right_arg, Var))
	{
		subplan = (SubPlan *) left_arg;
		compare_var = (Var *) right_arg;
	}
	else
		return false;

	if (subplan->subLinkType != EXPR_SUBLINK ||
		subplan->useHashTable ||
		subplan->parParam == NIL ||
		list_length(subplan->parParam) != 1 ||
		subplan->args == NIL ||
		list_length(subplan->args) != 1)
		return false;

	corr_arg_var = (Var *) StripImplicitNodesLocal((Expr *) linitial(subplan->args));
	if (corr_arg_var == nullptr || !IsA(corr_arg_var, Var))
		return false;

	/* The compare input itself must be materialized by the base join. */
	{
		VecJoinSide compare_side;
		uint16_t compare_source_col = 0;

		if (!ResolveHashJoinVarBinding(compare_var,
									   outer_plan,
									   inner_plan,
									   outer,
									   inner,
									   &compare_side,
									   &compare_source_col,
									   &compare_meta) ||
			!EnsureHashJoinOutputCol(compare_side,
									 compare_source_col,
									 compare_meta,
									 output_cols,
									 &compare_resno))
			return false;
	}

	if (!ResolveHashJoinVarBinding(corr_arg_var,
								   outer_plan,
								   inner_plan,
								   outer,
								   inner,
								   &key_side,
								   &spec->input_key_col,
								   &key_meta) ||
		!EnsureHashJoinOutputCol(key_side,
								 spec->input_key_col,
								 key_meta,
								 output_cols,
								 &key_resno))
		return false;

	subplan_plan = LookupReferencedSubPlan(estate, subplan);
	if (subplan_plan == nullptr || !IsA(subplan_plan, Agg))
		return false;
	subplan_agg = (Agg *) subplan_plan;
	if (subplan_agg->numCols != 0 || subplan_agg->plan.lefttree == nullptr ||
		subplan_agg->plan.targetlist == NIL ||
		list_length(subplan_agg->plan.targetlist) != 1)
		return false;

	lookup_state = BuildCorrelatedLookupAggState(estate,
													 subplan_agg,
													 linitial_int(subplan->parParam),
													 &lookup_key_meta,
													 &lookup_value_meta,
													 parallel_worker_context);
	if (!lookup_state)
		return false;
	if (!IsLookupCompatibleStorage(key_meta.storage_kind) ||
		!IsLookupCompatibleStorage(lookup_key_meta.storage_kind))
		return false;
	if (lookup_value_meta.storage_kind != VecOutputStorageKind::Int32 &&
		lookup_value_meta.storage_kind != VecOutputStorageKind::Int64 &&
		lookup_value_meta.storage_kind != VecOutputStorageKind::NumericScaledInt64 &&
		lookup_value_meta.storage_kind != VecOutputStorageKind::Double)
		return false;
	if ((int) output_cols->size() >= 16)
		return false;

	spec->lookup_state = std::move(lookup_state);
	spec->input_key_col = (uint16_t) (key_resno - 1);
	spec->input_key_meta = key_meta;
	spec->lookup_key_col = 0;
	spec->lookup_key_meta = lookup_key_meta;
	spec->lookup_value_col = 1;
	spec->output_resno = (int) output_cols->size() + 1;
	spec->output_meta = lookup_value_meta;

	compare_input_var = makeVar(1,
								compare_resno,
								compare_var->vartype,
								compare_var->vartypmod,
								compare_var->varcollid,
								0);
	lookup_var = makeVar(1,
						 spec->output_resno,
						 subplan->firstColType,
						 subplan->firstColTypmod,
						 subplan->firstColCollation,
						 0);
	rewritten_args = NIL;
	if ((Node *) left_arg == (Node *) compare_var)
		rewritten_args = list_make2(compare_input_var, lookup_var);
	else
		rewritten_args = list_make2(lookup_var, compare_input_var);

	rewritten_op = (OpExpr *) copyObjectImpl(compare_op);
	rewritten_op->args = rewritten_args;
	spec->rewritten_expr = (Expr *) rewritten_op;
	return true;
}

struct HashJoinFilterRewriteContext
{
	Plan *outer_plan;
	Plan *inner_plan;
	VecPlanState *outer;
	VecPlanState *inner;
	VolVecVector<VecJoinOutputCol> *output_cols;
	bool failed;
};

static Node *
RewriteHashJoinFilterVarsMutator(Node *node, HashJoinFilterRewriteContext *context)
{
	if (node == nullptr)
		return nullptr;

	if (IsA(node, Var))
	{
		Var *var = (Var *) node;
		VecJoinSide side;
		VecOutputColMeta meta;
		uint16_t source_col = 0;
		int join_resno = 0;
		Var *rewritten;

		if (!ResolveHashJoinVarBinding(var,
										 context->outer_plan,
										 context->inner_plan,
										 context->outer,
										 context->inner,
										 &side,
										 &source_col,
										 &meta) ||
			!EnsureHashJoinOutputCol(side, source_col, meta,
									 context->output_cols, &join_resno))
		{
			context->failed = true;
			return nullptr;
		}

		rewritten = makeVar(1,
							join_resno,
							var->vartype,
							var->vartypmod,
							var->varcollid,
							var->varlevelsup);
		rewritten->location = var->location;
		return (Node *) rewritten;
	}

	return expression_tree_mutator(node,
								   (Node *(*)(Node *, void *)) RewriteHashJoinFilterVarsMutator,
								   context);
}

Expr *
RewriteHashJoinFilterExpr(Expr *expr,
						   Plan *outer_plan,
						   Plan *inner_plan,
						   VecPlanState *outer,
						   VecPlanState *inner,
						   VolVecVector<VecJoinOutputCol> *output_cols)
{
	HashJoinFilterRewriteContext context;

	if (expr == nullptr)
		return nullptr;

	context.outer_plan = outer_plan;
	context.inner_plan = inner_plan;
	context.outer = outer;
	context.inner = inner;
	context.output_cols = output_cols;
	context.failed = false;

	Expr *rewritten = (Expr *) RewriteHashJoinFilterVarsMutator((Node *) expr, &context);
	if (context.failed)
		return nullptr;
	return rewritten;
}

bool
BuildJoinOutputCols(List *targetlist,
					Plan *outer_plan,
					Plan *inner_plan,
					VecPlanState *outer,
					VecPlanState *inner,
					VolVecVector<VecJoinOutputCol> *output_cols,
					bool *needs_project)
{
	ListCell *lc;

	if (needs_project != nullptr)
		*needs_project = false;
	foreach(lc, targetlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		Expr *expr = StripImplicitNodesLocal((Expr *) tle->expr);
		if (expr != nullptr && IsA(expr, Var))
		{
			Var *var = (Var *) expr;
			VecJoinSide side;
			VecOutputColMeta meta;
			uint16_t source_col = 0;
			int join_resno = 0;

			if (!ResolveHashJoinVarBinding(var,
										   outer_plan,
										   inner_plan,
										   outer,
										   inner,
										   &side,
										   &source_col,
										   &meta) ||
				!EnsureHashJoinOutputCol(side, source_col, meta, output_cols, &join_resno))
			{
				if (pg_volvec_trace_hooks)
					elog(LOG, "pg_volvec: hash join output var binding failed for target resno %d",
						 tle->resno);
				return false;
			}
			if (needs_project != nullptr && join_resno != tle->resno)
				*needs_project = true;
			continue;
		}

		if (RewriteHashJoinFilterExpr((Expr *) copyObjectImpl(tle->expr),
									  outer_plan,
									  inner_plan,
									  outer,
									  inner,
									  output_cols) == nullptr)
		{
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: hash join output rewrite failed for target resno %d",
					 tle->resno);
			return false;
		}
		if (needs_project != nullptr)
			*needs_project = true;
	}

	return true;
}

std::unique_ptr<VecPlanState>
BuildJoinProject(std::unique_ptr<VecPlanState> left,
				 List *targetlist,
				 Plan *outer_plan,
				 Plan *inner_plan,
				 VecPlanState *outer,
				 VecPlanState *inner,
				 VolVecVector<VecJoinOutputCol> *output_cols,
				 EState *estate)
{
	VolVecVector<VecProjectColDesc> project_cols{PgMemoryContextAllocator<VecProjectColDesc>(CurrentMemoryContext)};
	ListCell *lc;

	if (!left || output_cols == nullptr)
		return nullptr;

	foreach(lc, targetlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		VecProjectColDesc project_col;
		Expr *rewritten_expr =
			RewriteHashJoinFilterExpr((Expr *) copyObjectImpl(tle->expr),
									  outer_plan,
									  inner_plan,
									  outer,
									  inner,
									  output_cols);
		Expr *stripped_expr;

		if (rewritten_expr == nullptr)
		{
			if (pg_volvec_trace_hooks)
				elog(LOG, "pg_volvec: hash join project rewrite failed for target resno %d",
					 tle->resno);
			return nullptr;
		}

		project_col.target_resno = tle->resno;
		project_col.sql_type = exprType((Node *) tle->expr);
		stripped_expr = StripImplicitNodesLocal(rewritten_expr);
		if (stripped_expr != nullptr && IsA(stripped_expr, Var))
		{
			Var *var = (Var *) stripped_expr;
			VecOutputColMeta meta;

			if (var->varattno <= 0 || var->varattno > 16 ||
				!left->lookup_output_col_meta(var->varattno, &meta))
			{
				if (pg_volvec_trace_hooks)
					elog(LOG, "pg_volvec: hash join project direct-var metadata lookup failed for target resno %d",
						 tle->resno);
				return nullptr;
			}
			project_col.expr = nullptr;
			project_col.storage_kind = meta.storage_kind;
			project_col.scale = meta.scale;
			project_col.direct_var = true;
			project_col.input_col = (uint16_t) (var->varattno - 1);
		}
		else if (MatchStringPrefixExpr(stripped_expr,
									   &project_col.input_col,
									   &project_col.string_prefix_len))
		{
			VecOutputColMeta meta;

			if (!left->lookup_output_col_meta(project_col.input_col + 1, &meta) ||
				meta.storage_kind != VecOutputStorageKind::StringRef)
			{
				if (pg_volvec_trace_hooks)
					elog(LOG, "pg_volvec: hash join string-prefix project metadata lookup failed for target resno %d",
						 tle->resno);
				return nullptr;
			}
			project_col.expr = nullptr;
			project_col.storage_kind = VecOutputStorageKind::StringRef;
			project_col.scale = 0;
			project_col.string_prefix_var = true;
		}
		else
		{
			project_col.expr = std::make_unique<VecExprProgram>();
			CompileExpr(rewritten_expr, *project_col.expr, false, estate);
			AdjustProgramVarScales(project_col.expr.get(), left.get());
			if (project_col.expr->get_final_res_idx() < 0)
			{
				if (pg_volvec_trace_hooks)
					elog(LOG, "pg_volvec: hash join project expression compilation failed for target resno %d",
						 tle->resno);
				return nullptr;
			}
			project_col.storage_kind = InferProjectStorageKind((Expr *) tle->expr, project_col.expr.get());
			project_col.scale = project_col.expr->get_register_scale(project_col.expr->get_final_res_idx());
		}
		project_cols.push_back(std::move(project_col));
	}

	return std::make_unique<VecProjectState>(std::move(left), std::move(project_cols));
}

static bool
IsSimpleJoinKeyClause(Node *node)
{
	OpExpr *op;
	Expr *left_expr;
	Expr *right_expr;

	if (node == nullptr || !IsA(node, OpExpr))
		return false;
	op = (OpExpr *) node;
	if (list_length(op->args) != 2)
		return false;

	left_expr = StripImplicitNodesLocal((Expr *) linitial(op->args));
	right_expr = StripImplicitNodesLocal((Expr *) lsecond(op->args));
	if (left_expr == nullptr || right_expr == nullptr ||
		!IsA(left_expr, Var) || !IsA(right_expr, Var))
		return false;
	if ((((Var *) left_expr)->varno == OUTER_VAR && ((Var *) right_expr)->varno == INNER_VAR) ||
		(((Var *) left_expr)->varno == INNER_VAR && ((Var *) right_expr)->varno == OUTER_VAR))
		return true;
	return false;
}

void
PartitionJoinClauses(List *clauses, List **key_clauses, List **residual_clauses)
{
	ListCell *lc;

	if (key_clauses != nullptr)
		*key_clauses = NIL;
	if (residual_clauses != nullptr)
		*residual_clauses = NIL;

	foreach(lc, clauses)
	{
		Node *clause = (Node *) lfirst(lc);

		if (IsSimpleJoinKeyClause(clause))
		{
			if (key_clauses != nullptr)
				*key_clauses = lappend(*key_clauses, clause);
		}
		else
		{
			if (residual_clauses != nullptr)
				*residual_clauses = lappend(*residual_clauses, clause);
		}
	}
}

} /* namespace pg_volvec */

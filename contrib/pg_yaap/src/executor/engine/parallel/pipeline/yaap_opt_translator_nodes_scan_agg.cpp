#include "parallel/pipeline/yaap_opt_translator_internal.hpp"

#include <sstream>

namespace pg_yaap::optimizer_translator_detail {

static std::string
ProjectionExprSemanticKey(const Expression *expression)
{
	if (expression == nullptr)
		return "<null>";

	std::stringstream ss;
	switch (expression->type)
	{
		case ExpressionType::BOUND_COLUMN_REF:
		{
			const auto *column = static_cast<const BoundColumnRefExpression *>(expression);
			ss << "col:" << column->binding.table_index.index << "." << column->binding.column_index.index;
			break;
		}
		case ExpressionType::BOUND_CONSTANT:
		{
			const auto *constant = static_cast<const BoundConstantExpression *>(expression);
			ss << "const:" << (constant->is_null ? "NULL" : constant->value);
			break;
		}
		case ExpressionType::BOUND_FUNCTION:
		{
			const auto *function = static_cast<const BoundFunctionExpression *>(expression);
			ss << "fn:" << function->function_name << "(";
			for (size_t i = 0; i < function->children.size(); ++i)
			{
				if (i > 0)
					ss << ",";
				ss << ProjectionExprSemanticKey(function->children[i].get());
			}
			ss << ")";
			break;
		}
		case ExpressionType::BOUND_AGGREGATE:
		{
			const auto *aggregate = static_cast<const BoundAggregateExpression *>(expression);
			ss << "agg:" << aggregate->function_name << "(";
			for (size_t i = 0; i < aggregate->children.size(); ++i)
			{
				if (i > 0)
					ss << ",";
				ss << ProjectionExprSemanticKey(aggregate->children[i].get());
			}
			ss << ")";
			break;
		}
		default:
			ss << "opaque";
			break;
	}
	return ss.str();
}

static std::unique_ptr<Expression>
RewriteProjectionAggregateExpr(const BoundAggregateExpression *aggregate_expr,
							   const PhysicalOperator *source_op)
{
	if (aggregate_expr == nullptr || source_op == nullptr)
		return nullptr;
	if (source_op->type == PhysicalOperatorType::HASH_GROUP_BY)
	{
		const auto *aggregate = static_cast<const PhysicalHashAggregate *>(source_op);
		const std::string fingerprint = ProjectionExprSemanticKey(aggregate_expr);
		for (size_t idx = 0; idx < aggregate->expressions.size(); ++idx)
		{
			if (ProjectionExprSemanticKey(aggregate->expressions[idx]) != fingerprint)
				continue;
			std::string column_name = idx < aggregate->aggregate_names.size()
				? aggregate->aggregate_names[idx]
				: aggregate_expr->function_name;
			return std::make_unique<BoundColumnRefExpression>(
				yaap::ColumnBinding{aggregate->aggregate_index, yaap::ProjectionIndex{idx}},
				"agg",
				std::move(column_name));
		}
	}
	for (const auto &child : source_op->children)
	{
		if (child == nullptr)
			continue;
		if (auto rewritten = RewriteProjectionAggregateExpr(aggregate_expr, child.get()))
			return rewritten;
	}
	return nullptr;
}

static std::unique_ptr<Expression>
RewriteProjectionAggregateRefs(const Expression *expr,
							   const PhysicalOperator *source_op)
{
	if (expr == nullptr)
		return nullptr;
	switch (expr->type)
	{
		case ExpressionType::BOUND_COLUMN_REF:
		{
			const auto *column = static_cast<const BoundColumnRefExpression *>(expr);
			return std::make_unique<BoundColumnRefExpression>(column->binding, column->table_name, column->column_name);
		}
		case ExpressionType::BOUND_CONSTANT:
		{
			const auto *constant = static_cast<const BoundConstantExpression *>(expr);
			return std::make_unique<BoundConstantExpression>(constant->value, constant->is_null);
		}
		case ExpressionType::BOUND_AGGREGATE:
		{
			if (auto rewritten = RewriteProjectionAggregateExpr(static_cast<const BoundAggregateExpression *>(expr), source_op))
				return rewritten;
			const auto *aggregate = static_cast<const BoundAggregateExpression *>(expr);
			auto clone = std::make_unique<BoundAggregateExpression>(aggregate->function_name, aggregate->agg_oid, aggregate->is_distinct);
			for (const auto &child : aggregate->children)
				clone->children.push_back(RewriteProjectionAggregateRefs(child.get(), source_op));
			return clone;
		}
		case ExpressionType::BOUND_FUNCTION:
		{
			const auto *function = static_cast<const BoundFunctionExpression *>(expr);
			auto clone = std::make_unique<BoundFunctionExpression>(function->function_name, function->op_oid);
			for (const auto &child : function->children)
				clone->children.push_back(RewriteProjectionAggregateRefs(child.get(), source_op));
			return clone;
		}
		case ExpressionType::BOUND_CONJUNCTION:
		{
			const auto *conjunction = static_cast<const BoundConjunctionExpression *>(expr);
			auto clone = std::make_unique<BoundConjunctionExpression>(conjunction->bool_expr_type);
			for (const auto &child : conjunction->children)
				clone->children.push_back(RewriteProjectionAggregateRefs(child.get(), source_op));
			return clone;
		}
		default:
			return nullptr;
	}
}

static bool
InferNamedColumnDecodeKind(const BoundColumnRefExpression *expr,
						   ColumnDecodeKind &out)
{
	if (expr == nullptr || expr->table_name.empty() || expr->column_name.empty())
		return false;

	const Oid relid = RelnameGetRelid(expr->table_name.c_str());
	if (!OidIsValid(relid))
		return false;
	const AttrNumber attnum = get_attnum(relid, expr->column_name.c_str());
	if (attnum == InvalidAttrNumber)
		return false;

	Oid type_oid = InvalidOid;
	int32 typmod = -1;
	Oid collation = InvalidOid;
	get_atttypetypmodcoll(relid, attnum, &type_oid, &typmod, &collation);
	switch (type_oid)
	{
		case BPCHAROID:
			out = UseInt32CharDecodeForType(type_oid, typmod) ?
				ColumnDecodeKind::INT32_CHAR :
				ColumnDecodeKind::STRING_REF;
			return true;
		case CHAROID:
			out = ColumnDecodeKind::INT32_CHAR;
			return true;
		case DATEOID:
			out = ColumnDecodeKind::INT32_DATE;
			return true;
		case INT4OID:
			out = ColumnDecodeKind::INT32_INT4;
			return true;
		case INT8OID:
			out = ColumnDecodeKind::INT64_INT8;
			return true;
		case NUMERICOID:
			out = ColumnDecodeKind::INT64_NUMERIC_SCALED;
			return true;
		case FLOAT8OID:
			out = ColumnDecodeKind::DOUBLE_FLOAT8;
			return true;
		case TEXTOID:
		case VARCHAROID:
			out = ColumnDecodeKind::STRING_REF;
			return true;
		default:
			return false;
	}
}

static bool
LookupProjectionInputColumn(const BoundColumnRefExpression *expr,
							const std::vector<yaap::PhysicalOperator::OutputColumn> *outputs,
							const std::vector<ColumnRef> &cols,
							const std::vector<ColumnSchema> &schema,
							ColumnRef &out_ref,
							const ColumnSchema *&out_col)
{
	if (expr == nullptr)
		return false;

	return LookupNamedExprInputColumn(expr, outputs, cols, schema, out_ref, out_col);
}

static bool
LookupProjectionPassthroughColumn(const ColumnRef &ref,
								  const std::vector<yaap::PhysicalOperator::OutputColumn> &outputs,
								  const std::vector<ColumnRef> &cols,
								  const std::vector<ColumnSchema> &schema,
								  const ColumnSchema *&out_col,
								  const yaap::PhysicalOperator::OutputColumn *&out_output)
{
	out_col = nullptr;
	out_output = nullptr;
	for (size_t i = 0; i < cols.size() && i < schema.size(); ++i)
	{
		if (SameColumnRef(ref, cols[i]))
		{
			out_col = &schema[i];
			if (i < outputs.size())
				out_output = &outputs[i];
			return true;
		}
	}
	for (size_t i = 0; i < outputs.size() && i < schema.size(); ++i)
	{
		const ColumnRef output_ref = BindingToColumnRef(outputs[i].binding);
		if (!SameColumnRef(ref, output_ref))
			continue;
		out_col = &schema[i];
		out_output = &outputs[i];
		return true;
	}
	return false;
}

static bool
LookupProjectedOutputOrdinal(const BoundColumnRefExpression *expr,
							 const PhysicalOperator *source_op,
							 size_t &out_idx)
{
	if (expr == nullptr || source_op == nullptr)
		return false;
	if ((source_op->type == PhysicalOperatorType::FILTER ||
		 source_op->type == PhysicalOperatorType::ORDER_BY ||
		 source_op->type == PhysicalOperatorType::LIMIT) &&
		source_op->children.size() == 1 &&
		source_op->children[0] != nullptr)
		return LookupProjectedOutputOrdinal(expr, source_op->children[0].get(), out_idx);
	if (source_op->type != PhysicalOperatorType::PROJECTION)
		return false;
	const auto &projection = static_cast<const PhysicalProjection &>(*source_op);
	for (size_t idx = 0; idx < projection.select_list.size(); ++idx)
	{
		const auto *projected_col =
			dynamic_cast<const BoundColumnRefExpression *>(projection.select_list[idx]);
		if (projected_col == nullptr)
			continue;
		const bool binding_match =
			projected_col->binding.table_index.index == expr->binding.table_index.index &&
			projected_col->binding.column_index.index == expr->binding.column_index.index;
		const bool name_match =
			!expr->table_name.empty() &&
			!expr->column_name.empty() &&
			projected_col->table_name == expr->table_name &&
			projected_col->column_name == expr->column_name;
		if (!binding_match && !name_match)
			continue;
		out_idx = idx;
		return true;
	}
	return false;
}

static bool
TryBuildPureProjection(const PhysicalProjection &projection,
					   const PhysicalOperator *source_op,
					   const std::vector<Expression *> &select_list,
					   const std::vector<ColumnRef> *required_output_cols,
					   OptimizerNodeTranslation &child,
					   OptimizerNodeTranslation &out)
{
	if (select_list.size() != child.cols.size() || child.cols.size() != child.schema.size())
		return false;

	std::vector<ColumnRef> raw_output_cols;
	std::vector<ColumnSchema> new_schema;
	raw_output_cols.reserve(select_list.size());
	new_schema.reserve(select_list.size());
	for (size_t idx = 0; idx < select_list.size(); ++idx)
	{
		Expression *expr = select_list[idx];
		const auto *col_expr = dynamic_cast<const BoundColumnRefExpression *>(expr);
		if (col_expr == nullptr)
			return false;
		ColumnRef ref{};
		const ColumnSchema *col = nullptr;
		if (!LookupProjectionInputColumn(col_expr, &child.outputs, child.cols, child.schema, ref, col) || col == nullptr)
			return false;
		if (ref.varno != child.cols[idx].varno ||
			ref.attno != child.cols[idx].attno ||
			col->chunk_slot != child.schema[idx].chunk_slot ||
			col->decode_kind != child.schema[idx].decode_kind ||
			col->type_oid != child.schema[idx].type_oid ||
			col->typmod != child.schema[idx].typmod)
			return false;
		raw_output_cols.push_back(ColumnRef{
			static_cast<Index>(projection.table_index.index + 1),
			static_cast<AttrNumber>(raw_output_cols.size() + 1)});
		new_schema.push_back(*col);
	}
	out.op = std::move(child.op);
	out.cols = BuildParentFacingOutputCols(raw_output_cols, required_output_cols);
	out.schema = std::move(new_schema);
	out.outputs = projection.outputs;
	if (pg_yaap_trace_hooks)
	{
		elog(LOG,
			 "pg_yaap: pure projection table_index=%zu required=%zu raw=%zu out=%zu",
			 projection.table_index.index,
			 required_output_cols != nullptr ? required_output_cols->size() : 0,
			 raw_output_cols.size(),
			 out.cols.size());
		for (size_t i = 0; i < out.cols.size(); ++i)
			elog(LOG,
				 "pg_yaap: pure projection out_col[%zu]=(%u,%d)",
				 i,
				 out.cols[i].varno,
				 out.cols[i].attno);
	}
	if (pg_yaap_trace_hooks && out.outputs.size() != out.cols.size())
		elog(LOG,
			 "pg_yaap: pure projection output mismatch table_index=%zu out_cols=%zu out_outputs=%zu child_cols=%zu child_outputs=%zu",
			 projection.table_index.index,
			 out.cols.size(),
			 out.outputs.size(),
			 child.cols.size(),
			 child.outputs.size());
	out.final_sort_keys = child.final_sort_keys;
	out.limit_count = child.limit_count;
	out.estimated_groups = child.estimated_groups;
	return true;
}

bool
TranslateProjectionNode(const PhysicalProjection &projection,
						QueryDesc *queryDesc,
						PgYaapQueryState *state,
						const std::vector<ColumnRef> *required_output_cols,
						const PhysicalOperator *delim_outer_child,
						const std::vector<yaap::ColumnBinding> *delim_outer_bindings,
						OptimizerNodeTranslation &out)
{
	if (projection.children.size() != 1 || projection.children[0] == nullptr)
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer projection rejected: invalid child shape");
		return false;
	}
	OptimizerNodeTranslation child;
	std::vector<ColumnRef> child_required;
	std::vector<std::unique_ptr<Expression>> rewritten_select_storage;
	std::vector<Expression *> rewritten_select_list;
	std::vector<Expression *> selected_select_list;
	std::vector<size_t> selected_select_indices;
	rewritten_select_storage.reserve(projection.select_list.size());
	rewritten_select_list.reserve(projection.select_list.size());
	for (Expression *expr : projection.select_list)
	{
		auto rewritten = RewriteProjectionAggregateRefs(expr, projection.children[0].get());
		rewritten_select_list.push_back(rewritten != nullptr ? rewritten.get() : expr);
		rewritten_select_storage.push_back(std::move(rewritten));
	}
	const Index projection_varno = static_cast<Index>(projection.table_index.index + 1);
	if (required_output_cols == nullptr || required_output_cols->empty())
	{
		for (size_t idx = 0; idx < rewritten_select_list.size(); ++idx)
		{
			selected_select_indices.push_back(idx);
			selected_select_list.push_back(rewritten_select_list[idx]);
		}
	}
	else
	{
		for (size_t idx = 0; idx < rewritten_select_list.size(); ++idx)
		{
			const ColumnRef projected_ref{
				projection_varno,
				static_cast<AttrNumber>(idx + 1)};
			for (const ColumnRef &required : *required_output_cols)
			{
				if (SameColumnRef(required, projected_ref))
				{
					selected_select_indices.push_back(idx);
					selected_select_list.push_back(rewritten_select_list[idx]);
					break;
				}
			}
		}
	}
	std::vector<ColumnRef> hidden_passthrough_candidates;
	if (required_output_cols != nullptr)
	{
		for (const ColumnRef &ref : *required_output_cols)
		{
			if (ref.varno == projection_varno &&
				ref.attno > 0 &&
				ref.attno <= static_cast<AttrNumber>(projection.select_list.size()))
				continue;
			AppendUniqueColumnRef(ref, hidden_passthrough_candidates);
		}
	}
	for (Expression *expr : selected_select_list)
		CollectReferencedSourceColumns(expr, projection.children[0].get(), child_required);
	for (const ColumnRef &ref : hidden_passthrough_candidates)
		AppendUniqueColumnRef(ref, child_required);
	const std::vector<ColumnRef> *child_required_ptr =
		child_required.empty() ? nullptr : &child_required;
	if (!TranslateOptimizerNode(*projection.children[0], queryDesc, state, {}, child_required_ptr, delim_outer_child, delim_outer_bindings, child) || child.op == nullptr)
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer projection rejected: child translation failed");
		return false;
	}
	if (pg_yaap_trace_hooks)
	{
		elog(LOG,
			 "pg_yaap: projection table_index=%zu child_type=%d select_count=%zu child_cols=%zu child_schema=%zu hidden_candidates=%zu",
			 projection.table_index.index,
			 static_cast<int>(projection.children[0]->type),
			 selected_select_list.size(),
			 child.cols.size(),
			 child.schema.size(),
			 hidden_passthrough_candidates.size());
		for (size_t i = 0; i < child.cols.size(); ++i)
			elog(LOG,
				 "pg_yaap: projection child_col[%zu]=(%u,%d) decode=%d slot=%u",
				 i,
				 child.cols[i].varno,
				 child.cols[i].attno,
				 static_cast<int>(child.schema[i].decode_kind),
				 child.schema[i].chunk_slot);
		for (size_t i = 0; i < child.outputs.size(); ++i)
			elog(LOG,
				 "pg_yaap: projection child_output[%zu] binding=(%zu,%zu) name=%s.%s",
				 i,
				 child.outputs[i].binding.table_index.index,
				 child.outputs[i].binding.column_index.index,
				 child.outputs[i].table_name.c_str(),
				 child.outputs[i].column_name.c_str());
		for (size_t i = 0; i < selected_select_list.size(); ++i)
		{
			if (selected_select_list[i] != nullptr &&
				selected_select_list[i]->type == ExpressionType::BOUND_COLUMN_REF)
			{
				const auto *col_expr =
					static_cast<const BoundColumnRefExpression *>(selected_select_list[i]);
				elog(LOG,
					 "pg_yaap: projection select[%zu] source_idx=%zu binding=(%zu,%zu)",
					 i,
					 selected_select_indices[i],
					 col_expr->binding.table_index.index,
					 col_expr->binding.column_index.index);
			}
		}
		for (size_t i = 0; i < hidden_passthrough_candidates.size(); ++i)
			elog(LOG,
				 "pg_yaap: projection hidden_candidate[%zu]=(%u,%d)",
				 i,
				 hidden_passthrough_candidates[i].varno,
				 hidden_passthrough_candidates[i].attno);
	}
	std::vector<ColumnRef> hidden_passthrough;
	if (!hidden_passthrough_candidates.empty())
	{
		for (const ColumnRef &ref : hidden_passthrough_candidates)
		{
			const ColumnSchema *col = nullptr;
			const yaap::PhysicalOperator::OutputColumn *output = nullptr;
			if (LookupProjectionPassthroughColumn(ref, child.outputs, child.cols, child.schema, col, output) &&
				col != nullptr)
				AppendUniqueColumnRef(ref, hidden_passthrough);
		}
	}

	if (hidden_passthrough.empty() &&
		selected_select_list.size() == rewritten_select_list.size() &&
		TryBuildPureProjection(projection, projection.children[0].get(), selected_select_list, required_output_cols, child, out))
		return true;

	std::vector<ProjectStep> steps;
	std::vector<ProjectExprDesc> expr_descs;
	std::vector<ColumnSchema> out_schema;
	uint8_t next_int64_slot = NextFreeInt64Slot(child.schema);
	uint8_t next_string_slot = NextFreeStringSlot(child.schema);
	for (Expression *expr : selected_select_list)
	{
		const uint16_t first_step_idx = static_cast<uint16_t>(steps.size());
		int8_t result_scale = 0;
		uint8_t result_slot = 0;
		Oid type_oid = InvalidOid;
		int32 typmod = -1;
		bool lowered = false;
		if (!InferProjectionExprSchema(expr, child.cols, child.schema, type_oid, typmod, result_scale))
		{
			if (pg_yaap_trace_hooks)
			{
				if (expr->type == ExpressionType::BOUND_COLUMN_REF)
				{
					const auto *col_expr = static_cast<const BoundColumnRefExpression *>(expr);
					elog(LOG,
						 "pg_yaap: optimizer projection missing bound col=(%zu,%zu) child_cols=%zu",
						 col_expr->binding.table_index.index,
						 col_expr->binding.column_index.index,
						 child.cols.size());
					for (size_t j = 0; j < child.cols.size(); ++j)
						elog(LOG,
							 "pg_yaap: optimizer projection child_col[%zu]=(%u,%d)",
							 j,
							 child.cols[j].varno,
							 child.cols[j].attno);
				}
				if (expr->type == ExpressionType::BOUND_FUNCTION)
					elog(LOG, "pg_yaap: optimizer projection rejected: expr lowering failed fn=%s",
						 static_cast<const BoundFunctionExpression *>(expr)->function_name.c_str());
				else if (expr->type == ExpressionType::BOUND_AGGREGATE)
					elog(LOG, "pg_yaap: optimizer projection rejected: expr lowering failed agg=%s",
						 static_cast<const BoundAggregateExpression *>(expr)->function_name.c_str());
				else
					elog(LOG, "pg_yaap: optimizer projection rejected: expr lowering failed type=%d",
						 static_cast<int>(expr->type));
			}
			return false;
		}
		if (expr->type == ExpressionType::BOUND_FUNCTION)
		{
			const auto *func = static_cast<const BoundFunctionExpression *>(expr);
			if (func->function_name == "prefix_slice")
				lowered = LowerProjectionStringPrefixSlice(func, steps, next_string_slot, child.cols, child.schema, result_slot);
		}
		if (!lowered)
			lowered = LowerOptimizerExpr(expr, steps, next_int64_slot, child.cols, child.schema, nullptr, result_scale, result_slot);
		if (!lowered)
		{
			if (pg_yaap_trace_hooks)
			{
				if (expr->type == ExpressionType::BOUND_COLUMN_REF)
				{
					const auto *col_expr = static_cast<const BoundColumnRefExpression *>(expr);
					elog(LOG,
						 "pg_yaap: optimizer projection missing bound col=(%zu,%zu) child_cols=%zu",
						 col_expr->binding.table_index.index,
						 col_expr->binding.column_index.index,
						 child.cols.size());
					for (size_t j = 0; j < child.cols.size(); ++j)
						elog(LOG,
							 "pg_yaap: optimizer projection child_col[%zu]=(%u,%d)",
							 j,
							 child.cols[j].varno,
							 child.cols[j].attno);
				}
				if (expr->type == ExpressionType::BOUND_FUNCTION)
					elog(LOG, "pg_yaap: optimizer projection rejected: expr lowering failed fn=%s",
						 static_cast<const BoundFunctionExpression *>(expr)->function_name.c_str());
				else if (expr->type == ExpressionType::BOUND_AGGREGATE)
					elog(LOG, "pg_yaap: optimizer projection rejected: expr lowering failed agg=%s",
						 static_cast<const BoundAggregateExpression *>(expr)->function_name.c_str());
				else
					elog(LOG, "pg_yaap: optimizer projection rejected: expr lowering failed type=%d",
						 static_cast<int>(expr->type));
			}
			return false;
		}
		ColumnSchema mapped{};
		if (expr->type == ExpressionType::BOUND_COLUMN_REF)
		{
			ColumnRef ref{};
			const ColumnSchema *source_col = nullptr;
			const auto *col_expr = static_cast<const BoundColumnRefExpression *>(expr);
			if (!LookupProjectionInputColumn(col_expr, &child.outputs, child.cols, child.schema, ref, source_col) ||
				source_col == nullptr)
			{
				if (pg_yaap_trace_hooks)
					elog(LOG,
						 "pg_yaap: optimizer projection rejected: missing source schema for bound col=(%zu,%zu)",
						 col_expr->binding.table_index.index,
						 col_expr->binding.column_index.index);
				return false;
			}
			mapped = *source_col;
			mapped.chunk_slot = result_slot;
		}
		else if (!MapProjectedExprSchema(type_oid, typmod, result_scale, result_slot, mapped))
		{
			if (pg_yaap_trace_hooks)
				elog(LOG, "pg_yaap: optimizer projection rejected: schema mapping failed type=%u slot=%u scale=%d",
					 type_oid, result_slot, result_scale);
			return false;
		}
		out_schema.push_back(mapped);
		expr_descs.push_back(ProjectExprDesc{
			first_step_idx,
			static_cast<uint16_t>(steps.size() - first_step_idx),
			result_slot,
			result_scale,
			0});
	}

	for (const ColumnRef &ref : hidden_passthrough)
	{
		const ColumnSchema *col = nullptr;
		const yaap::PhysicalOperator::OutputColumn *output = nullptr;
		if (!LookupProjectionPassthroughColumn(ref, child.outputs, child.cols, child.schema, col, output) ||
			col == nullptr)
			return false;
		out_schema.push_back(*col);
		expr_descs.push_back(ProjectExprDesc{
			static_cast<uint16_t>(steps.size()),
			0,
			col->chunk_slot,
			0,
			0});
	}

	dsa_pointer input_schema_dp = BuildSchemaDescriptorFromColumns(child.schema, state->runtime_dsa);
	dsa_pointer output_schema_dp = BuildSchemaDescriptorFromColumns(out_schema, state->runtime_dsa);
	dsa_pointer expr_descs_dp = BuildFilterArray(state->runtime_dsa, expr_descs.data(), sizeof(ProjectExprDesc), expr_descs.size());
	dsa_pointer steps_dp = BuildFilterArray(state->runtime_dsa, steps.data(), sizeof(ProjectStep), steps.size());
	if (!DsaPointerIsValid(input_schema_dp) || !DsaPointerIsValid(output_schema_dp))
	{
		if (pg_yaap_trace_hooks)
			elog(LOG,
				 "pg_yaap: optimizer projection rejected: schema DSA publish failed child_schema=%zu out_schema=%zu hidden_passthrough=%zu exprs=%zu input_ok=%d output_ok=%d",
				 child.schema.size(),
				 out_schema.size(),
				 hidden_passthrough.size(),
				 selected_select_list.size(),
				 DsaPointerIsValid(input_schema_dp) ? 1 : 0,
				 DsaPointerIsValid(output_schema_dp) ? 1 : 0);
		return false;
	}

	PgVector<ProjectExprDesc> expr_descs_vec;
	expr_descs_vec.assign(expr_descs.begin(), expr_descs.end());
	PgVector<ProjectStep> steps_vec;
	steps_vec.assign(steps.begin(), steps.end());

	auto project_op = std::make_unique<PipelineProjection>(
		input_schema_dp,
		output_schema_dp,
		std::move(expr_descs_vec),
		std::move(steps_vec),
		expr_descs_dp,
		steps_dp,
		nullptr);
	project_op->AddChild(std::move(child.op));

	out.op = std::move(project_op);
	out.schema = std::move(out_schema);
	std::vector<ColumnRef> raw_output_cols;
	raw_output_cols.reserve(selected_select_indices.size() + hidden_passthrough.size());
	for (size_t idx : selected_select_indices)
		raw_output_cols.push_back(ColumnRef{
			projection_varno,
			static_cast<AttrNumber>(idx + 1)});
	for (const ColumnRef &ref : hidden_passthrough)
		raw_output_cols.push_back(ref);
	out.cols = BuildParentFacingOutputCols(raw_output_cols, required_output_cols);
	out.outputs.clear();
	out.outputs.reserve(selected_select_indices.size() + hidden_passthrough.size());
	for (size_t idx : selected_select_indices)
	{
		if (idx < projection.outputs.size())
			out.outputs.push_back(projection.outputs[idx]);
	}
	for (const ColumnRef &ref : hidden_passthrough)
	{
		const ColumnSchema *col = nullptr;
		const yaap::PhysicalOperator::OutputColumn *output = nullptr;
		if (!LookupProjectionPassthroughColumn(ref, child.outputs, child.cols, child.schema, col, output) ||
			output == nullptr)
			return false;
		out.outputs.push_back(*output);
	}
	if (pg_yaap_trace_hooks && out.outputs.size() != out.cols.size())
		elog(LOG,
			 "pg_yaap: projection output mismatch table_index=%zu out_cols=%zu out_outputs=%zu selected=%zu hidden_passthrough=%zu child_cols=%zu child_outputs=%zu",
			 projection.table_index.index,
			 out.cols.size(),
			 out.outputs.size(),
			 selected_select_indices.size(),
			 hidden_passthrough.size(),
			 child.cols.size(),
			 child.outputs.size());
	if (pg_yaap_trace_hooks)
	{
		elog(LOG,
			 "pg_yaap: projection output table_index=%zu out_cols=%zu out_schema=%zu hidden_passthrough=%zu",
			 projection.table_index.index,
			 out.cols.size(),
			 out.schema.size(),
			 hidden_passthrough.size());
		for (size_t i = 0; i < out.cols.size(); ++i)
			elog(LOG,
				 "pg_yaap: projection out_col[%zu]=(%u,%d)",
				 i,
				 out.cols[i].varno,
				 out.cols[i].attno);
	}
	out.final_sort_keys = std::move(child.final_sort_keys);
	out.limit_count = child.limit_count;
	out.estimated_groups = child.estimated_groups;
	return true;
}

bool
TranslateTableScanNode(const PhysicalTableScan &scan,
					   PgYaapQueryState *state,
					   const std::vector<Expression *> &pending_filters,
					   const std::vector<ColumnRef> *required_output_cols,
					   OptimizerNodeTranslation &out)
{
	if (state == nullptr || state->runtime_dsa == nullptr)
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer scan rejected: missing state/runtime_dsa");
		return false;
	}

	std::vector<ColumnRef> all_cols;
	if (!BuildProjectedTableColumnRefs(scan.relid,
									   static_cast<Index>(scan.table_index.index + 1),
									   scan.projected_columns,
									   all_cols))
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer scan rejected: failed to enumerate projected table columns relid=%u projected=%zu",
				 scan.relid,
				 scan.projected_columns.size());
		return false;
	}

	for (Expression *filter_expr : scan.filters)
		CollectReferencedColumns(filter_expr, all_cols);
	for (Expression *filter_expr : pending_filters)
		CollectReferencedColumns(filter_expr, all_cols);
	if (required_output_cols != nullptr)
	{
		const Index scan_varno = static_cast<Index>(scan.table_index.index + 1);
		for (const ColumnRef &ref : *required_output_cols)
		{
			if (ref.varno == scan_varno)
				AppendUniqueColumnRef(ref, all_cols);
		}
	}

	std::vector<ColumnSchema> ordered_cols;
	if (!BuildOrderedSeqScanColumns(scan.relid, all_cols, static_cast<Index>(scan.table_index.index + 1), ordered_cols))
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer scan rejected: BuildOrderedSeqScanColumns failed relid=%u", scan.relid);
		return false;
	}
	std::vector<Expression *> filters = scan.filters;
	filters.insert(filters.end(), pending_filters.begin(), pending_filters.end());

	std::vector<FilterInputDesc> filter_inputs;
	std::vector<FilterExprDesc> filter_exprs;
	std::vector<FilterStep> filter_steps;
	std::vector<char> filter_string_consts;
	if (!LowerScanFilters(filters, all_cols, ordered_cols, filter_inputs, filter_exprs, filter_steps, filter_string_consts))
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer scan rejected: filter lowering failed n_filters=%zu", filters.size());
		return false;
	}

	dsa_pointer output_schema_dp = BuildSchemaDescriptorFromColumns(ordered_cols, state->runtime_dsa);
	dsa_pointer filter_inputs_dp = BuildFilterArray(state->runtime_dsa, filter_inputs.data(), sizeof(FilterInputDesc), filter_inputs.size());
	dsa_pointer filter_exprs_dp = BuildFilterArray(state->runtime_dsa, filter_exprs.data(), sizeof(FilterExprDesc), filter_exprs.size());
	dsa_pointer filter_steps_dp = BuildFilterArray(state->runtime_dsa, filter_steps.data(), sizeof(FilterStep), filter_steps.size());
	dsa_pointer filter_string_consts_dp = BuildCharArray(state->runtime_dsa, filter_string_consts);
	if (!DsaPointerIsValid(output_schema_dp))
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer scan rejected: output schema publish failed");
		return false;
	}

	out.op = std::make_unique<PipelineSeqScan>(
		scan.relid,
		InvalidDsaPointer,
		output_schema_dp,
		filter_inputs_dp,
		filter_exprs_dp,
		filter_steps_dp,
		filter_string_consts_dp,
		static_cast<uint16_t>(filter_inputs.size()),
		static_cast<uint16_t>(filter_exprs.size()),
		static_cast<uint16_t>(filter_steps.size()),
		static_cast<uint16_t>(std::min<size_t>(pg_yaap::pipeline::FILTER_MAX_BOOL_REGS, filter_exprs.size() ? filter_steps.back().out_bool_reg + 1 : 0)),
		static_cast<uint32_t>(filter_string_consts.size()),
		InvalidDsaPointer);
	out.cols = std::move(all_cols);
	out.schema = std::move(ordered_cols);
	out.outputs.clear();
	out.outputs.reserve(out.cols.size());
	const Index scan_varno = static_cast<Index>(scan.table_index.index + 1);
	for (const ColumnRef &ref : out.cols)
	{
		bool matched = false;
		for (const auto &output : scan.outputs)
		{
			if (SameColumnRef(ref, BindingToColumnRef(output.binding)))
			{
				out.outputs.push_back(output);
				matched = true;
				break;
			}
		}
		if (matched)
			continue;
		if (ref.varno != scan_varno || ref.attno <= 0)
			continue;
		const char *attname = get_attname(scan.relid, ref.attno, true);
		out.outputs.push_back(yaap::PhysicalOperator::OutputColumn{
			yaap::ColumnBinding{scan.table_index, yaap::ProjectionIndex{static_cast<size_t>(ref.attno - 1)}},
			scan.table_name,
			attname != nullptr ? std::string(attname) : ("col" + std::to_string(ref.attno))});
	}
	out.final_sort_keys.clear();
	out.limit_count = 0;
	out.estimated_groups = 0;
	return true;
}

bool
TranslateHashAggregateNode(const PhysicalHashAggregate &agg,
						   QueryDesc *queryDesc,
						   PgYaapQueryState *state,
						   const PhysicalOperator *delim_outer_child,
						   const std::vector<yaap::ColumnBinding> *delim_outer_bindings,
						   OptimizerNodeTranslation &out)
{
	if (agg.children.size() != 1 || agg.children[0] == nullptr || state == nullptr || state->runtime_dsa == nullptr)
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer agg rejected: invalid child/state");
		return false;
	}
	OptimizerNodeTranslation child;
	std::vector<ColumnRef> child_required;
	bool need_full_child_outputs = false;
	for (Expression *group_expr : agg.groups)
		CollectReferencedColumns(group_expr, child_required);
	for (Expression *agg_expr : agg.expressions)
	{
		CollectReferencedColumns(agg_expr, child_required);
		if (agg_expr != nullptr && agg_expr->type == ExpressionType::BOUND_AGGREGATE &&
			static_cast<const BoundAggregateExpression *>(agg_expr)->is_distinct)
			need_full_child_outputs = true;
	}
	const std::vector<ColumnRef> *child_required_ptr =
		need_full_child_outputs ? nullptr : &child_required;
	if (!TranslateOptimizerNode(*agg.children[0], queryDesc, state, {}, child_required_ptr, delim_outer_child, delim_outer_bindings, child) || child.op == nullptr)
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer agg rejected: child translation failed");
		return false;
	}
	AggBuildState agg_state;
	TupleDataLayoutInit(&agg_state.hash_layout);
	uint8_t next_int64_slot = NextFreeInt64Slot(child.schema);
	for (Expression *expr : agg.expressions)
	{
		const auto *bound_agg = dynamic_cast<const BoundAggregateExpression *>(expr);
		if (bound_agg == nullptr)
			return false;
		AggFuncDesc desc{};
		TdcAggKind kind{};
		int16_t numeric_scale = 0;
		if (!ClassifyOptimizerAggregate(bound_agg,
										&child.outputs,
										child.cols,
										child.schema,
										agg_state.project_steps,
										agg_state.project_exprs,
										agg_state.materialized_exprs,
										next_int64_slot,
										desc,
										kind,
										numeric_scale))
		{
			if (pg_yaap_trace_hooks)
				elog(LOG, "pg_yaap: optimizer agg rejected: aggregate classify failed fn=%s",
					 bound_agg != nullptr ? bound_agg->function_name.c_str() : "<null>");
			return false;
		}
		agg_state.agg_funcs.push_back(desc);
		agg_state.agg_kinds.push_back(kind);
		agg_state.agg_numeric_scales.push_back(numeric_scale);
	}
	for (TdcAggKind kind : agg_state.agg_kinds)
	{
		if (kind == TdcAggKind::COUNT_DISTINCT_NONNULL && agg_state.agg_kinds.size() != 1)
		{
			if (pg_yaap_trace_hooks)
				elog(LOG, "pg_yaap: optimizer agg rejected: mixed distinct aggregates not yet supported");
			return false;
		}
	}

	std::vector<ColumnRef> group_cols;
	group_cols.reserve(agg.groups.size());
	for (Expression *group_expr : agg.groups)
	{
		const auto *group_col = dynamic_cast<const BoundColumnRefExpression *>(group_expr);
		if (group_col == nullptr)
		{
			if (pg_yaap_trace_hooks)
				elog(LOG, "pg_yaap: optimizer agg rejected: non-column group expr");
			return false;
		}
		yaap::ColumnBinding resolved_binding = group_col->binding;
		(void) ResolveOutputBinding(group_col, agg.children[0].get(), resolved_binding);
		BoundColumnRefExpression resolved_group_col(
			resolved_binding,
			group_col->table_name,
			group_col->column_name);
		std::unique_ptr<BoundColumnRefExpression> named_group_lookup;
		const BoundColumnRefExpression *lookup_group_col = &resolved_group_col;
		const size_t group_idx = group_cols.size();
		if (resolved_group_col.table_name.empty() &&
			resolved_group_col.column_name.empty() &&
			group_idx < agg.outputs.size() &&
			!agg.outputs[group_idx].table_name.empty() &&
			!agg.outputs[group_idx].column_name.empty())
		{
			named_group_lookup = std::make_unique<BoundColumnRefExpression>(
				resolved_binding,
				agg.outputs[group_idx].table_name,
				agg.outputs[group_idx].column_name);
			lookup_group_col = named_group_lookup.get();
		}
		ColumnRef input_ref{};
		const ColumnSchema *input_col = nullptr;
		if (!LookupNamedExprInputColumn(lookup_group_col,
										&child.outputs,
										child.cols,
										child.schema,
										input_ref,
										input_col) ||
			input_col == nullptr)
		{
			if (!LookupExprInputColumn(resolved_binding,
									   child.cols,
									   child.schema,
									   input_ref,
									   input_col) ||
				input_col == nullptr)
			{
			size_t projected_idx = 0;
				if (LookupProjectedOutputOrdinal(group_col, agg.children[0].get(), projected_idx) &&
					projected_idx < child.cols.size() &&
					projected_idx < child.schema.size())
				{
					input_ref = child.cols[projected_idx];
					input_col = &child.schema[projected_idx];
				}
			}
		}
		if (input_col == nullptr)
		{
			if (pg_yaap_trace_hooks)
				elog(LOG,
					 "pg_yaap: optimizer agg rejected: missing translated group column binding=(%zu,%zu) resolved=(%zu,%zu) child_cols=%zu child_outputs=%zu",
					 group_col->binding.table_index.index,
					 group_col->binding.column_index.index,
					 resolved_binding.table_index.index,
					 resolved_binding.column_index.index,
					 child.cols.size(),
					 child.outputs.size());
			return false;
		}
		group_cols.push_back(input_ref);
	}

	if (!BuildHashGroupLayout(group_cols,
							  child.cols,
							  child.schema,
							  agg_state.agg_funcs,
							  agg_state.agg_kinds,
							  agg_state.agg_numeric_scales,
							  agg_state.hash_layout))
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer agg rejected: BuildHashGroupLayout failed groups=%zu aggs=%zu",
				 group_cols.size(), agg_state.agg_funcs.size());
		return false;
	}
	(void) pg_yaap::pipeline::translator_detail::TryBuildPerfectHashSpec(group_cols, child.cols, child.schema, agg_state.perfect_hash_capacity);

	std::unique_ptr<PipelineOperator> agg_child = std::move(child.op);
	if (!agg_state.project_exprs.empty())
	{
		PgVector<ProjectExprDesc> expr_descs_vec;
		expr_descs_vec.assign(agg_state.project_exprs.begin(), agg_state.project_exprs.end());
		PgVector<ProjectStep> steps_vec;
		steps_vec.assign(agg_state.project_steps.begin(), agg_state.project_steps.end());
		dsa_pointer input_schema_dp = BuildSchemaDescriptorFromColumns(child.schema, state->runtime_dsa);
		auto project_op = std::make_unique<PipelineProjection>(
			input_schema_dp,
			InvalidDsaPointer,
			std::move(expr_descs_vec),
			std::move(steps_vec));
		project_op->AddChild(std::move(agg_child));
		agg_child = std::move(project_op);
	}

	dsa_pointer hash_layout_dp = SerializeTupleDataLayout(agg_state.hash_layout, state->runtime_dsa);
	if (!DsaPointerIsValid(hash_layout_dp))
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer agg rejected: hash layout publish failed");
		return false;
	}
	PgVector<uint16_t> group_keys;
	for (const ColumnRef &ref : group_cols)
	{
		const ColumnSchema *col = nullptr;
		if ((!LookupRawColumn(ref, child.cols, child.schema, col) || col == nullptr) &&
			!(ref.attno != InvalidAttrNumber &&
			  ref.attno > 0 &&
			  static_cast<size_t>(ref.attno) <= child.schema.size() &&
			  (col = &child.schema[ref.attno - 1], true)))
		{
			if (pg_yaap_trace_hooks)
				elog(LOG, "pg_yaap: optimizer agg rejected: missing group key column");
			return false;
		}
		group_keys.push_back(col->chunk_slot);
	}
	PgVector<AggFuncDesc> agg_funcs_vec;
	agg_funcs_vec.assign(agg_state.agg_funcs.begin(), agg_state.agg_funcs.end());
	std::unique_ptr<PipelineOperator> hash_op;
	if (agg_state.perfect_hash_capacity > 0)
	{
		hash_op = std::make_unique<PipelinePerfectHashAggregate>(
			hash_layout_dp,
			std::move(group_keys),
			std::move(agg_funcs_vec),
			InvalidDsaPointer,
			std::max<uint32_t>(1024u, static_cast<uint32_t>(agg.estimated_cardinality)),
			agg_state.perfect_hash_capacity);
	}
	else
	{
		hash_op = std::make_unique<PipelineHashAggregate>(
			hash_layout_dp,
			std::move(group_keys),
			std::move(agg_funcs_vec),
			InvalidDsaPointer,
			std::max<uint32_t>(1024u, static_cast<uint32_t>(agg.estimated_cardinality)),
			0);
	}
	hash_op->AddChild(std::move(agg_child));

	out.op = std::move(hash_op);
	out.final_sort_keys = std::move(child.final_sort_keys);
	out.limit_count = child.limit_count;
	out.estimated_groups = static_cast<uint32_t>(std::max<size_t>(1, agg.estimated_cardinality));
	if (!BuildOptimizerAggOutput(agg, &child.outputs, child.cols, child.schema, agg_state, out.cols, out.schema))
		return false;
	out.outputs = agg.outputs;
	return true;
}

bool
TranslateDelimScanNode(const PhysicalDelimScan &scan,
					   QueryDesc *queryDesc,
					   PgYaapQueryState *state,
					   const std::vector<Expression *> &pending_filters,
					   const PhysicalOperator *delim_outer_child,
					   const std::vector<yaap::ColumnBinding> *delim_outer_bindings,
					   OptimizerNodeTranslation &out)
{
	if (state == nullptr || state->runtime_dsa == nullptr || delim_outer_child == nullptr)
		return false;
	if (!pending_filters.empty() || !scan.children.empty() || scan.correlated_columns.empty())
		return false;

	std::vector<ColumnRef> required_cols;
	required_cols.reserve(scan.correlated_columns.size());
	const auto *outer_bindings =
		(delim_outer_bindings != nullptr && delim_outer_bindings->size() == scan.correlated_columns.size())
			? delim_outer_bindings
			: &scan.correlated_columns;
	for (const auto &binding : *outer_bindings)
		AppendUniqueColumnRef(BindingToColumnRef(binding), required_cols);

	OptimizerNodeTranslation producer_child;
	std::vector<ColumnRef> selected_cols;
	std::vector<ColumnSchema> selected_schema;
	std::vector<yaap::PhysicalOperator::OutputColumn> selected_outputs;
	auto try_extract_correlated_outputs =
		[&](const std::vector<ColumnRef> *required_ptr) -> bool
	{
		OptimizerNodeTranslation candidate;
		if (!TranslateOptimizerNode(*delim_outer_child,
				queryDesc,
				state,
				{},
				required_ptr,
				nullptr,
				nullptr,
				candidate) ||
			candidate.op == nullptr ||
			candidate.cols.empty() ||
			candidate.cols.size() != candidate.schema.size())
			return false;
		std::vector<ColumnRef> candidate_cols;
		std::vector<ColumnSchema> candidate_schema;
		std::vector<yaap::PhysicalOperator::OutputColumn> candidate_outputs;
		candidate_cols.reserve(outer_bindings->size());
		candidate_schema.reserve(outer_bindings->size());
		candidate_outputs.reserve(outer_bindings->size());
		for (size_t i = 0; i < outer_bindings->size(); ++i)
		{
			const yaap::ColumnBinding &binding = (*outer_bindings)[i];
			BoundColumnRefExpression bound_expr(
				binding,
				i < scan.outputs.size() ? scan.outputs[i].table_name : std::string{},
				i < scan.outputs.size() ? scan.outputs[i].column_name : std::string{});
			ColumnRef selected_ref{};
			const ColumnSchema *selected_col = nullptr;
			if (!LookupNamedExprInputColumn(&bound_expr,
											&candidate.outputs,
											candidate.cols,
											candidate.schema,
											selected_ref,
											selected_col) ||
				selected_col == nullptr)
				return false;
			candidate_cols.push_back(selected_ref);
			candidate_schema.push_back(*selected_col);
			if (i < scan.outputs.size())
				candidate_outputs.push_back(scan.outputs[i]);
		}
		if (candidate_cols.empty())
			return false;
		producer_child = std::move(candidate);
		selected_cols = std::move(candidate_cols);
		selected_schema = std::move(candidate_schema);
		selected_outputs = std::move(candidate_outputs);
		if (pg_yaap_trace_hooks)
		{
			elog(LOG,
				 "pg_yaap: delim producer selected via required=%d candidate_cols=%zu candidate_outputs=%zu",
				 required_ptr != nullptr ? 1 : 0,
				 producer_child.cols.size(),
				 producer_child.outputs.size());
			for (size_t i = 0; i < producer_child.cols.size(); ++i)
				elog(LOG,
					 "pg_yaap: delim producer col[%zu]=(%u,%d) decode=%d",
					 i,
					 producer_child.cols[i].varno,
					 producer_child.cols[i].attno,
					 static_cast<int>(producer_child.schema[i].decode_kind));
			for (size_t i = 0; i < producer_child.outputs.size(); ++i)
				elog(LOG,
					 "pg_yaap: delim producer output[%zu] binding=(%zu,%zu) name=%s.%s",
					 i,
					 producer_child.outputs[i].binding.table_index.index,
					 producer_child.outputs[i].binding.column_index.index,
					 producer_child.outputs[i].table_name.c_str(),
					 producer_child.outputs[i].column_name.c_str());
		}
		return true;
	};

	if (!try_extract_correlated_outputs(nullptr) &&
		!try_extract_correlated_outputs(&required_cols))
		return false;

	TupleDataLayout group_layout{};
	if (!BuildHashGroupLayout(selected_cols,
			producer_child.cols,
			producer_child.schema,
			{},
			{},
			{},
			group_layout))
		return false;

	const dsa_pointer input_schema_dp = BuildSchemaDescriptorFromColumns(selected_schema, state->runtime_dsa);
	const dsa_pointer layout_dp = SerializeTupleDataLayout(group_layout, state->runtime_dsa);
	const dsa_pointer shared_payload_dp = dsa_allocate0(state->runtime_dsa, sizeof(pipeline::HashAggSharedPayload));
	if (!DsaPointerIsValid(input_schema_dp) ||
		!DsaPointerIsValid(layout_dp) ||
		!DsaPointerIsValid(shared_payload_dp))
		return false;

	PgVector<uint16_t> group_keys;
	group_keys.reserve(selected_cols.size());
	for (const ColumnRef &ref : selected_cols)
	{
		const ColumnSchema *selected_col = nullptr;
		if (!LookupRawColumn(ref, producer_child.cols, producer_child.schema, selected_col) || selected_col == nullptr)
			return false;
		group_keys.push_back(selected_col->chunk_slot);
	}

	auto producer_sink = std::make_unique<PipelineHashAggregate>(
		layout_dp,
		std::move(group_keys),
		PgVector<AggFuncDesc>{},
		shared_payload_dp,
		std::max<uint32_t>(static_cast<uint32_t>(producer_child.estimated_groups), 1024u));
	producer_sink->AddChild(std::move(producer_child.op));

	out.op = std::make_unique<PipelineDelimScan>(
		input_schema_dp,
		shared_payload_dp,
		std::move(producer_sink));
	out.cols.clear();
	for (size_t i = 0; i < selected_schema.size(); ++i)
	{
		out.cols.push_back(ColumnRef{
			static_cast<Index>(scan.table_index.index + 1),
			static_cast<AttrNumber>(i + 1)});
	}
	out.schema = std::move(selected_schema);
	out.outputs = std::move(selected_outputs);
	if (pg_yaap_trace_hooks)
	{
		elog(LOG,
			 "pg_yaap: delim scan table_index=%zu corr=%zu producer_cols=%zu out_cols=%zu out_outputs=%zu",
			 scan.table_index.index,
			 scan.correlated_columns.size(),
			 producer_child.cols.size(),
			 out.cols.size(),
			 out.outputs.size());
		for (size_t i = 0; i < out.cols.size(); ++i)
			elog(LOG,
				 "pg_yaap: delim scan out_col[%zu]=(%u,%d)",
				 i,
				 out.cols[i].varno,
				 out.cols[i].attno);
		for (size_t i = 0; i < out.outputs.size(); ++i)
			elog(LOG,
				 "pg_yaap: delim scan out_output[%zu] binding=(%zu,%zu) name=%s.%s",
				 i,
				 out.outputs[i].binding.table_index.index,
				 out.outputs[i].binding.column_index.index,
				 out.outputs[i].table_name.c_str(),
				 out.outputs[i].column_name.c_str());
	}
	out.final_sort_keys.clear();
	out.limit_count = 0;
	out.estimated_groups = 0;
	return true;
}

}  // namespace pg_yaap::optimizer_translator_detail

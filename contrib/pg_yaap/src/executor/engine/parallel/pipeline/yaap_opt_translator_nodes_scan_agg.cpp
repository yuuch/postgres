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
TryBuildPureProjection(const PhysicalProjection &projection,
					   const std::vector<Expression *> &select_list,
					   const std::vector<ColumnRef> *required_output_cols,
					   OptimizerNodeTranslation &child,
					   OptimizerNodeTranslation &out)
{
	std::vector<ColumnRef> raw_output_cols;
	std::vector<ColumnSchema> new_schema;
	raw_output_cols.reserve(select_list.size());
	new_schema.reserve(select_list.size());
	for (Expression *expr : select_list)
	{
		const auto *col_expr = dynamic_cast<const BoundColumnRefExpression *>(expr);
		if (col_expr == nullptr)
			return false;
		ColumnRef ref{};
		const ColumnSchema *col = nullptr;
		if (!LookupExprInputColumn(col_expr->binding, child.cols, child.schema, ref, col) || col == nullptr)
			return false;
		raw_output_cols.push_back(ColumnRef{
			static_cast<Index>(projection.table_index.index + 1),
			static_cast<AttrNumber>(raw_output_cols.size() + 1)});
		new_schema.push_back(*col);
	}
	out.op = std::move(child.op);
	out.cols = BuildParentFacingOutputCols(raw_output_cols, required_output_cols);
	out.schema = std::move(new_schema);
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
	rewritten_select_storage.reserve(projection.select_list.size());
	rewritten_select_list.reserve(projection.select_list.size());
	for (Expression *expr : projection.select_list)
	{
		auto rewritten = RewriteProjectionAggregateRefs(expr, projection.children[0].get());
		rewritten_select_list.push_back(rewritten != nullptr ? rewritten.get() : expr);
		rewritten_select_storage.push_back(std::move(rewritten));
	}
	for (Expression *expr : rewritten_select_list)
		CollectReferencedColumns(expr, child_required);
	if (!TranslateOptimizerNode(*projection.children[0], queryDesc, state, {}, &child_required, delim_outer_child, delim_outer_bindings, child) || child.op == nullptr)
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer projection rejected: child translation failed");
		return false;
	}
	if (pg_yaap_trace_hooks)
	{
		elog(LOG,
			 "pg_yaap: projection table_index=%zu select_count=%zu child_cols=%zu child_schema=%zu",
			 projection.table_index.index,
			 rewritten_select_list.size(),
			 child.cols.size(),
			 child.schema.size());
		for (size_t i = 0; i < child.cols.size(); ++i)
			elog(LOG,
				 "pg_yaap: projection child_col[%zu]=(%u,%d) decode=%d slot=%u",
				 i,
				 child.cols[i].varno,
				 child.cols[i].attno,
				 static_cast<int>(child.schema[i].decode_kind),
				 child.schema[i].chunk_slot);
		for (size_t i = 0; i < rewritten_select_list.size(); ++i)
		{
			if (rewritten_select_list[i] != nullptr &&
				rewritten_select_list[i]->type == ExpressionType::BOUND_COLUMN_REF)
			{
				const auto *col_expr =
					static_cast<const BoundColumnRefExpression *>(rewritten_select_list[i]);
				elog(LOG,
					 "pg_yaap: projection select[%zu] binding=(%zu,%zu)",
					 i,
					 col_expr->binding.table_index.index,
					 col_expr->binding.column_index.index);
			}
		}
	}
	std::vector<ColumnRef> hidden_passthrough;
	if (required_output_cols != nullptr)
	{
		const Index projection_varno = static_cast<Index>(projection.table_index.index + 1);
		for (const ColumnRef &ref : *required_output_cols)
		{
			/*
			 * Required refs that already target this projection's visible
			 * output shape are not hidden passthrough columns. Treating them
			 * as passthrough duplicates the projected outputs and can leave
			 * downstream operators reading the wrong string slot.
			 */
			if (ref.varno == projection_varno &&
				ref.attno > 0 &&
				ref.attno <= static_cast<AttrNumber>(projection.select_list.size()))
				continue;
			const ColumnSchema *col = nullptr;
			if (LookupPassthroughColumn(ref, child.cols, child.schema, col) && col != nullptr)
				AppendUniqueColumnRef(ref, hidden_passthrough);
		}
	}

	if (hidden_passthrough.empty() && TryBuildPureProjection(projection, rewritten_select_list, required_output_cols, child, out))
		return true;

	std::vector<ProjectStep> steps;
	std::vector<ProjectExprDesc> expr_descs;
	std::vector<ColumnSchema> out_schema;
	uint8_t next_int64_slot = NextFreeInt64Slot(child.schema);
	uint8_t next_string_slot = NextFreeStringSlot(child.schema);
	for (Expression *expr : rewritten_select_list)
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
			if (!LookupExprInputColumn(col_expr->binding, child.cols, child.schema, ref, source_col) ||
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
		if (!LookupPassthroughColumn(ref, child.cols, child.schema, col) || col == nullptr)
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
			elog(LOG, "pg_yaap: optimizer projection rejected: schema DSA publish failed");
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
	raw_output_cols.reserve(projection.select_list.size() + hidden_passthrough.size());
	for (size_t i = 0; i < projection.select_list.size(); ++i)
		raw_output_cols.push_back(ColumnRef{
			static_cast<Index>(projection.table_index.index + 1),
			static_cast<AttrNumber>(i + 1)});
	for (const ColumnRef &ref : hidden_passthrough)
		raw_output_cols.push_back(ref);
	out.cols = BuildParentFacingOutputCols(raw_output_cols, required_output_cols);
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
	for (Expression *group_expr : agg.groups)
		CollectReferencedColumns(group_expr, child_required);
	for (Expression *agg_expr : agg.expressions)
		CollectReferencedColumns(agg_expr, child_required);
	if (!TranslateOptimizerNode(*agg.children[0], queryDesc, state, {}, &child_required, delim_outer_child, delim_outer_bindings, child) || child.op == nullptr)
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
		group_cols.push_back(BindingToColumnRef(group_col->binding));
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
		if (!LookupRawColumn(ref, child.cols, child.schema, col) || col == nullptr)
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
	return BuildOptimizerAggOutput(agg, child.cols, child.schema, agg_state, out.cols, out.schema);
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
	if (!TranslateOptimizerNode(*delim_outer_child,
			queryDesc,
			state,
			{},
			&required_cols,
			nullptr,
			nullptr,
			producer_child) ||
		producer_child.op == nullptr)
		return false;
	if (producer_child.cols.empty() || producer_child.cols.size() != producer_child.schema.size())
		return false;

	TupleDataLayout group_layout{};
	if (!BuildHashGroupLayout(producer_child.cols,
			producer_child.cols,
			producer_child.schema,
			{},
			{},
			{},
			group_layout))
		return false;

	const dsa_pointer input_schema_dp = BuildSchemaDescriptorFromColumns(producer_child.schema, state->runtime_dsa);
	const dsa_pointer layout_dp = SerializeTupleDataLayout(group_layout, state->runtime_dsa);
	const dsa_pointer shared_payload_dp = dsa_allocate0(state->runtime_dsa, sizeof(pipeline::HashAggSharedPayload));
	if (!DsaPointerIsValid(input_schema_dp) ||
		!DsaPointerIsValid(layout_dp) ||
		!DsaPointerIsValid(shared_payload_dp))
		return false;

	PgVector<uint16_t> group_keys;
	group_keys.reserve(producer_child.cols.size());
	for (uint16_t i = 0; i < producer_child.cols.size(); ++i)
		group_keys.push_back(i);

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
	for (size_t i = 0; i < producer_child.cols.size(); ++i)
	{
		out.cols.push_back(ColumnRef{
			static_cast<Index>(scan.table_index.index + 1),
			static_cast<AttrNumber>(i + 1)});
	}
	out.schema = std::move(producer_child.schema);
	out.final_sort_keys.clear();
	out.limit_count = 0;
	out.estimated_groups = 0;
	return true;
}

}  // namespace pg_yaap::optimizer_translator_detail

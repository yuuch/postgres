#include "parallel/pipeline/yaap_opt_translator_internal.hpp"

#include <sstream>

namespace pg_yaap::optimizer_translator_detail {

static std::string
ExpressionSemanticKey(const Expression *expression)
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
				ss << ExpressionSemanticKey(function->children[i].get());
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
				ss << ExpressionSemanticKey(aggregate->children[i].get());
			}
			ss << ")";
			break;
		}
		case ExpressionType::BOUND_CONJUNCTION:
		{
			const auto *conjunction = static_cast<const BoundConjunctionExpression *>(expression);
			ss << "conj:" << conjunction->bool_expr_type << "(";
			for (size_t i = 0; i < conjunction->children.size(); ++i)
			{
				if (i > 0)
					ss << ",";
				ss << ExpressionSemanticKey(conjunction->children[i].get());
			}
			ss << ")";
			break;
		}
		case ExpressionType::BOUND_SUBQUERY:
		{
			const auto *subquery = static_cast<const yaap::BoundSubqueryExpression *>(expression);
			ss << "subquery:" << subquery->sublink_name << "(";
			for (size_t i = 0; i < subquery->children.size(); ++i)
			{
				if (i > 0)
					ss << ",";
				ss << ExpressionSemanticKey(subquery->children[i].get());
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
RewriteAggregateExprToProducedColumn(const BoundAggregateExpression *aggregate_expr,
									 const PhysicalOperator *source_op)
{
	if (aggregate_expr == nullptr || source_op == nullptr)
		return nullptr;
	if ((source_op->type == PhysicalOperatorType::FILTER ||
		 source_op->type == PhysicalOperatorType::PROJECTION ||
		 source_op->type == PhysicalOperatorType::ORDER_BY ||
		 source_op->type == PhysicalOperatorType::LIMIT) &&
		source_op->children.size() == 1 &&
		source_op->children[0] != nullptr)
		return RewriteAggregateExprToProducedColumn(aggregate_expr, source_op->children[0].get());
	if (source_op->type != PhysicalOperatorType::HASH_GROUP_BY)
		return nullptr;

	const auto *aggregate = static_cast<const PhysicalHashAggregate *>(source_op);
	const std::string fingerprint = ExpressionSemanticKey(aggregate_expr);
	for (size_t idx = 0; idx < aggregate->expressions.size(); ++idx)
	{
		if (ExpressionSemanticKey(aggregate->expressions[idx]) != fingerprint)
			continue;
		std::string column_name = idx < aggregate->aggregate_names.size()
			? aggregate->aggregate_names[idx]
			: aggregate_expr->function_name;
		return std::make_unique<BoundColumnRefExpression>(
			yaap::ColumnBinding{aggregate->aggregate_index, yaap::ProjectionIndex{idx}},
			"agg",
			std::move(column_name));
	}
	return nullptr;
}

static std::unique_ptr<Expression>
RewriteJoinResidualAggregateRefs(const Expression *expr,
								 const PhysicalOperator *left_source_op,
								 const PhysicalOperator *right_source_op)
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
			if (auto rewritten = RewriteAggregateExprToProducedColumn(static_cast<const BoundAggregateExpression *>(expr), left_source_op))
				return rewritten;
			if (auto rewritten = RewriteAggregateExprToProducedColumn(static_cast<const BoundAggregateExpression *>(expr), right_source_op))
				return rewritten;
			const auto *aggregate = static_cast<const BoundAggregateExpression *>(expr);
			auto clone = std::make_unique<BoundAggregateExpression>(aggregate->function_name, aggregate->agg_oid, aggregate->is_distinct);
			for (const auto &child : aggregate->children)
				clone->children.push_back(RewriteJoinResidualAggregateRefs(child.get(), left_source_op, right_source_op));
			return clone;
		}
		case ExpressionType::BOUND_FUNCTION:
		{
			const auto *function = static_cast<const BoundFunctionExpression *>(expr);
			auto clone = std::make_unique<BoundFunctionExpression>(function->function_name, function->op_oid);
			for (const auto &child : function->children)
				clone->children.push_back(RewriteJoinResidualAggregateRefs(child.get(), left_source_op, right_source_op));
			return clone;
		}
		case ExpressionType::BOUND_CONJUNCTION:
		{
			const auto *conjunction = static_cast<const BoundConjunctionExpression *>(expr);
			auto clone = std::make_unique<BoundConjunctionExpression>(conjunction->bool_expr_type);
			for (const auto &child : conjunction->children)
				clone->children.push_back(RewriteJoinResidualAggregateRefs(child.get(), left_source_op, right_source_op));
			return clone;
		}
		default:
			return nullptr;
	}
}

static bool
IsScalarPhysicalNode(const PhysicalOperator *op)
{
	if (op == nullptr)
		return false;
	if (op->type == PhysicalOperatorType::HASH_GROUP_BY)
	{
		const auto *agg = static_cast<const PhysicalHashAggregate *>(op);
		return agg->groups.empty();
	}
	if ((op->type == PhysicalOperatorType::FILTER ||
		 op->type == PhysicalOperatorType::PROJECTION ||
		 op->type == PhysicalOperatorType::ORDER_BY ||
		 op->type == PhysicalOperatorType::LIMIT) &&
		op->children.size() == 1 && op->children[0] != nullptr)
	{
		return IsScalarPhysicalNode(op->children[0].get());
	}
	return false;
}

bool
TranslateHashJoinNode(const PhysicalHashJoin &join,
					  QueryDesc *queryDesc,
					  PgYaapQueryState *state,
					  const std::vector<Expression *> &pending_filters,
					  const std::vector<ColumnRef> *required_output_cols,
					  const PhysicalOperator *delim_outer_child,
					  const std::vector<yaap::ColumnBinding> *delim_outer_bindings,
					  OptimizerNodeTranslation &out)
{
	if (join.children.size() != 2 || join.children[0] == nullptr || join.children[1] == nullptr || state == nullptr || state->runtime_dsa == nullptr)
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer hash join rejected: invalid children/state");
		return false;
	}
	const bool scalar_delim_join = join.delim_join && join.join_type == yaap::JOIN_SINGLE;
	const bool semi_or_anti_join = join.join_type == yaap::JOIN_SEMI || join.join_type == yaap::JOIN_ANTI;
	if (join.join_type != yaap::JOIN_INNER && !scalar_delim_join && !semi_or_anti_join)
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer hash join rejected: join_type=%d", join.join_type);
		return false;
	}
	const pg_yaap::pipeline::HashJoinMatchMode join_mode =
		join.join_type == yaap::JOIN_SEMI ? pg_yaap::pipeline::HashJoinMatchMode::SEMI :
		join.join_type == yaap::JOIN_ANTI ? pg_yaap::pipeline::HashJoinMatchMode::ANTI :
		pg_yaap::pipeline::HashJoinMatchMode::INNER;
	const bool right_output_semi_or_anti =
		semi_or_anti_join &&
		join.children_swapped;

	std::vector<ColumnRef> join_required_cols;
	for (Expression *expr : join.conditions)
		CollectReferencedColumns(expr, join_required_cols);
	for (Expression *expr : pending_filters)
		CollectReferencedColumns(expr, join_required_cols);

	std::vector<ColumnRef> left_required_cols = join_required_cols;
	std::vector<ColumnRef> right_required_cols = join_required_cols;
	if (!semi_or_anti_join && required_output_cols != nullptr)
	{
		for (const ColumnRef &ref : *required_output_cols)
		{
			AppendUniqueColumnRef(ref, left_required_cols);
			AppendUniqueColumnRef(ref, right_required_cols);
		}
	}
	if (semi_or_anti_join && required_output_cols != nullptr)
	{
		auto &output_required_cols = right_output_semi_or_anti ? right_required_cols : left_required_cols;
		for (const ColumnRef &ref : *required_output_cols)
			AppendUniqueColumnRef(ref, output_required_cols);
	}

	OptimizerNodeTranslation left_child;
	OptimizerNodeTranslation right_child;
	if (!TranslateOptimizerNode(*join.children[0],
			queryDesc,
			state,
			{},
			left_required_cols.empty() ? nullptr : &left_required_cols,
			delim_outer_child,
			delim_outer_bindings,
			left_child) ||
		!TranslateOptimizerNode(*join.children[1],
			queryDesc,
			state,
			{},
			right_required_cols.empty() ? nullptr : &right_required_cols,
			scalar_delim_join ? join.children[0].get() : delim_outer_child,
			scalar_delim_join ? &join.correlated_columns : delim_outer_bindings,
			right_child) ||
		left_child.op == nullptr || right_child.op == nullptr)
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer hash join rejected: child translation failed");
		return false;
	}

	std::vector<ColumnRef> left_keys;
	std::vector<ColumnRef> right_keys;
	std::vector<Expression *> residuals = pending_filters;
	for (Expression *expr : join.conditions)
	{
		if (!CollectJoinKeys(expr, left_child.cols, left_child.schema, right_child.cols, right_child.schema, left_keys, right_keys, residuals))
		{
			if (pg_yaap_trace_hooks)
				elog(LOG, "pg_yaap: optimizer hash join rejected: join key extraction failed");
			return false;
		}
	}
	const size_t left_rows = join.children[0] != nullptr ? join.children[0]->estimated_cardinality : 0;
	const size_t right_rows = join.children[1] != nullptr ? join.children[1]->estimated_cardinality : 0;
	const bool left_scalar = IsScalarPhysicalNode(join.children[0].get());
	const bool right_scalar = IsScalarPhysicalNode(join.children[1].get());
	if (pg_yaap_trace_hooks)
		elog(LOG,
			 "pg_yaap: optimizer hash join scalar-shape left_rows=%zu right_rows=%zu left_scalar=%d right_scalar=%d join_type=%d conditions=%zu pending_filters=%zu",
			 left_rows,
			 right_rows,
			 left_scalar ? 1 : 0,
			 right_scalar ? 1 : 0,
			 join.join_type,
			 join.conditions.size(),
			 pending_filters.size());
	const bool scalar_residual_join =
		(join.join_type == yaap::JOIN_INNER || scalar_delim_join) &&
		left_keys.empty() &&
		right_keys.empty() &&
		!residuals.empty() &&
		(left_rows == 1 || right_rows == 1 || left_scalar || right_scalar);
	if ((!scalar_residual_join && left_keys.empty()) || left_keys.size() != right_keys.size())
	{
		if (pg_yaap_trace_hooks)
		{
			for (size_t i = 0; i < left_child.cols.size(); ++i)
				elog(LOG,
					 "pg_yaap: optimizer hash join left_col[%zu]=(%u,%d)",
					 i,
					 left_child.cols[i].varno,
					 left_child.cols[i].attno);
			for (size_t i = 0; i < right_child.cols.size(); ++i)
				elog(LOG,
					 "pg_yaap: optimizer hash join right_col[%zu]=(%u,%d)",
					 i,
					 right_child.cols[i].varno,
					 right_child.cols[i].attno);
			for (size_t i = 0; i < join.conditions.size(); ++i)
			{
				const Expression *expr = join.conditions[i];
				if (expr != nullptr && expr->type == ExpressionType::BOUND_FUNCTION)
				{
					const auto *func = static_cast<const BoundFunctionExpression *>(expr);
					elog(LOG,
						 "pg_yaap: optimizer hash join cond[%zu] fn=%s children=%zu",
						 i,
						 func->function_name.c_str(),
						 func->children.size());
				}
				else
				{
					elog(LOG,
						 "pg_yaap: optimizer hash join cond[%zu] type=%d",
						 i,
						 expr != nullptr ? static_cast<int>(expr->type) : -1);
				}
			}
			elog(LOG,
				 "pg_yaap: optimizer hash join rejected: no usable equi-join keys (left=%zu right=%zu residuals=%zu)",
				 left_keys.size(),
				 right_keys.size(),
				 residuals.size());
		}
		return false;
	}
	if (pg_yaap_trace_hooks && scalar_residual_join)
		elog(LOG,
			 "pg_yaap: optimizer hash join using zero-key scalar residual fallback left_rows=%zu right_rows=%zu left_scalar=%d right_scalar=%d residuals=%zu",
			 left_rows,
			 right_rows,
			 left_scalar ? 1 : 0,
			 right_scalar ? 1 : 0,
			 residuals.size());

	const bool swap_sides = semi_or_anti_join ? right_output_semi_or_anti :
		((left_rows > 0 && right_rows > 0)
			? (left_rows < right_rows)
			: (left_child.schema.size() < right_child.schema.size()));
	const auto &probe_cols = swap_sides ? right_child.cols : left_child.cols;
	const auto &probe_schema = swap_sides ? right_child.schema : left_child.schema;
	const auto &build_cols = swap_sides ? left_child.cols : right_child.cols;
	const auto &build_schema = swap_sides ? left_child.schema : right_child.schema;
	const PhysicalOperator *probe_source_op = swap_sides ? join.children[1].get() : join.children[0].get();
	const PhysicalOperator *build_source_op = swap_sides ? join.children[0].get() : join.children[1].get();
	const auto &probe_keys = swap_sides ? right_keys : left_keys;
	const auto &build_keys = swap_sides ? left_keys : right_keys;

	std::vector<ColumnRef> raw_output_cols = probe_cols;
	if (!semi_or_anti_join)
		raw_output_cols.insert(raw_output_cols.end(), build_cols.begin(), build_cols.end());
	if (pg_yaap_trace_hooks && scalar_delim_join)
	{
		elog(LOG,
			 "pg_yaap: scalar delim join probe_cols=%zu build_cols=%zu raw_output_cols=%zu required_output_cols=%zu",
			 probe_cols.size(),
			 build_cols.size(),
			 raw_output_cols.size(),
			 required_output_cols != nullptr ? required_output_cols->size() : 0);
		for (size_t i = 0; i < raw_output_cols.size(); ++i)
			elog(LOG,
				 "pg_yaap: scalar delim join raw_output_col[%zu]=(%u,%d)",
				 i,
				 raw_output_cols[i].varno,
				 raw_output_cols[i].attno);
		if (required_output_cols != nullptr)
		{
			for (size_t i = 0; i < required_output_cols->size(); ++i)
				elog(LOG,
					 "pg_yaap: scalar delim join required_output_col[%zu]=(%u,%d)",
					 i,
					 (*required_output_cols)[i].varno,
					 (*required_output_cols)[i].attno);
		}
	}
	std::vector<ColumnRef> requested_output_cols;
	if (scalar_delim_join || semi_or_anti_join)
		requested_output_cols = raw_output_cols;
	else
	{
		FilterRequestedColumns(raw_output_cols, required_output_cols, requested_output_cols);
		if (requested_output_cols.empty())
			requested_output_cols = raw_output_cols;
	}
	if (semi_or_anti_join)
	{
		for (const ColumnRef &ref : requested_output_cols)
		{
			const ColumnSchema *probe_col = nullptr;
			if (!LookupRawColumn(ref, probe_cols, probe_schema, probe_col) || probe_col == nullptr)
			{
				if (pg_yaap_trace_hooks)
					elog(LOG, "pg_yaap: optimizer hash join rejected: semi/anti output references build side");
				return false;
			}
		}
	}
	std::vector<HashJoinOutputColumnDesc> output_mappings;
	std::vector<ColumnSchema> output_schema;
	TupleDataLayout probe_key_layout;
	TupleDataLayout build_key_layout;
	TupleDataLayout probe_payload_layout;
	TupleDataLayout build_payload_layout;
	const std::vector<ColumnSchema> empty_build_payload_schema;
	const auto &build_payload_schema =
		(semi_or_anti_join && residuals.empty()) ? empty_build_payload_schema : build_schema;
	if (!BuildColumnOnlyLayoutForRefs(probe_keys, probe_cols, probe_schema, probe_key_layout) ||
		!BuildColumnOnlyLayoutForRefs(build_keys, build_cols, build_schema, build_key_layout) ||
		!BuildColumnOnlyLayout(probe_schema, probe_payload_layout) ||
		!BuildColumnOnlyLayout(build_payload_schema, build_payload_layout) ||
		!BuildHashJoinOutputMappings(requested_output_cols, probe_cols, probe_schema, build_cols, build_schema, output_mappings, output_schema))
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer hash join rejected: join layout/output mapping build failed");
		return false;
	}

	std::vector<HashJoinFilterInputDesc> filter_inputs;
	std::vector<FilterExprDesc> filter_exprs;
	std::vector<FilterStep> filter_steps;
	std::vector<char> filter_string_consts;
	uint16_t filter_bool_regs = 0;
	std::vector<std::unique_ptr<Expression>> rewritten_residual_storage;
	std::vector<Expression *> rewritten_residuals;
	rewritten_residual_storage.reserve(residuals.size());
	rewritten_residuals.reserve(residuals.size());
	for (Expression *expr : residuals)
	{
		auto rewritten = RewriteJoinResidualAggregateRefs(expr, probe_source_op, build_source_op);
		rewritten_residuals.push_back(rewritten != nullptr ? rewritten.get() : expr);
		rewritten_residual_storage.push_back(std::move(rewritten));
	}
	if (!LowerJoinFilters(rewritten_residuals,
						  probe_source_op,
						  build_source_op,
						  probe_cols, probe_schema,
						  build_cols, build_schema,
						  filter_inputs, filter_exprs, filter_steps, filter_string_consts, filter_bool_regs))
	{
		if (pg_yaap_trace_hooks)
			elog(LOG,
				 "pg_yaap: optimizer hash join rejected: residual join filter lowering failed (%zu residuals)",
				 residuals.size());
		return false;
	}

	dsa_pointer left_schema_dp = BuildSchemaDescriptorFromColumns(probe_schema, state->runtime_dsa);
	dsa_pointer right_schema_dp = BuildSchemaDescriptorFromColumns(build_payload_schema, state->runtime_dsa);
	dsa_pointer output_schema_dp = BuildSchemaDescriptorFromColumns(output_schema, state->runtime_dsa);
	dsa_pointer left_key_layout_dp = SerializeTupleDataLayout(probe_key_layout, state->runtime_dsa);
	dsa_pointer right_key_layout_dp = SerializeTupleDataLayout(build_key_layout, state->runtime_dsa);
	dsa_pointer left_payload_layout_dp = SerializeTupleDataLayout(probe_payload_layout, state->runtime_dsa);
	dsa_pointer right_payload_layout_dp = SerializeTupleDataLayout(build_payload_layout, state->runtime_dsa);
	dsa_pointer output_columns_dp = BuildFilterArray(state->runtime_dsa, output_mappings.data(), sizeof(HashJoinOutputColumnDesc), output_mappings.size());
	dsa_pointer filter_inputs_dp = BuildFilterArray(state->runtime_dsa, filter_inputs.data(), sizeof(HashJoinFilterInputDesc), filter_inputs.size());
	dsa_pointer filter_exprs_dp = BuildFilterArray(state->runtime_dsa, filter_exprs.data(), sizeof(FilterExprDesc), filter_exprs.size());
	dsa_pointer filter_steps_dp = BuildFilterArray(state->runtime_dsa, filter_steps.data(), sizeof(FilterStep), filter_steps.size());
	dsa_pointer filter_string_consts_dp = BuildCharArray(state->runtime_dsa, filter_string_consts);
	if (!DsaPointerIsValid(left_schema_dp) || !DsaPointerIsValid(right_schema_dp) ||
		!DsaPointerIsValid(output_schema_dp) || !DsaPointerIsValid(left_key_layout_dp) ||
		!DsaPointerIsValid(right_key_layout_dp) || !DsaPointerIsValid(left_payload_layout_dp) ||
		!DsaPointerIsValid(right_payload_layout_dp) || !DsaPointerIsValid(output_columns_dp))
	{
		if (pg_yaap_trace_hooks)
			elog(LOG,
				 "pg_yaap: optimizer hash join rejected: DSA publish failed left_schema=%d right_schema=%d output_schema=%d left_key=%d right_key=%d left_payload=%d right_payload=%d output_cols=%d output_mappings=%zu output_schema_cols=%zu",
				 DsaPointerIsValid(left_schema_dp) ? 1 : 0,
				 DsaPointerIsValid(right_schema_dp) ? 1 : 0,
				 DsaPointerIsValid(output_schema_dp) ? 1 : 0,
				 DsaPointerIsValid(left_key_layout_dp) ? 1 : 0,
				 DsaPointerIsValid(right_key_layout_dp) ? 1 : 0,
				 DsaPointerIsValid(left_payload_layout_dp) ? 1 : 0,
				 DsaPointerIsValid(right_payload_layout_dp) ? 1 : 0,
				 DsaPointerIsValid(output_columns_dp) ? 1 : 0,
				 output_mappings.size(),
				 output_schema.size());
		return false;
	}

	auto join_op = std::make_unique<pg_yaap::pipeline::PhysicalHashJoin>(
		left_schema_dp,
		right_schema_dp,
		output_schema_dp,
		left_key_layout_dp,
		right_key_layout_dp,
		left_payload_layout_dp,
		right_payload_layout_dp,
		output_columns_dp,
		static_cast<uint16_t>(output_mappings.size()),
		filter_inputs_dp,
		filter_exprs_dp,
		filter_steps_dp,
		filter_string_consts_dp,
		static_cast<uint16_t>(filter_inputs.size()),
		static_cast<uint16_t>(filter_exprs.size()),
		static_cast<uint16_t>(filter_steps.size()),
		filter_bool_regs,
		join_mode,
		static_cast<uint32_t>(filter_string_consts.size()),
		InvalidDsaPointer,
		static_cast<uint16_t>(probe_keys.size()),
		static_cast<uint16_t>(build_keys.size()),
		EstimateHashJoinBuildRows(swap_sides ? left_child.schema.size() : right_child.schema.size()));
	if (swap_sides)
	{
		join_op->AddChild(std::move(right_child.op));
		join_op->AddChild(std::move(left_child.op));
	}
	else
	{
		join_op->AddChild(std::move(left_child.op));
		join_op->AddChild(std::move(right_child.op));
	}

	std::vector<ColumnRef> parent_facing_cols =
		BuildParentFacingOutputCols(requested_output_cols, required_output_cols);
	out.op = std::move(join_op);
	out.cols = std::move(parent_facing_cols);
	out.schema = std::move(output_schema);
	out.final_sort_keys.clear();
	out.limit_count = 0;
	out.estimated_groups = 0;
	return true;
}

bool
TranslateCrossProductNode(const PhysicalCrossProduct &join,
						  QueryDesc *queryDesc,
						  PgYaapQueryState *state,
						  const std::vector<Expression *> &pending_filters,
						  const std::vector<ColumnRef> *required_output_cols,
						  const PhysicalOperator *delim_outer_child,
						  const std::vector<yaap::ColumnBinding> *delim_outer_bindings,
						  OptimizerNodeTranslation &out)
{
	if (join.children.size() != 2 || join.children[0] == nullptr || join.children[1] == nullptr || state == nullptr || state->runtime_dsa == nullptr)
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer cross product rejected: invalid children/state");
		return false;
	}
	if (pending_filters.empty())
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer cross product rejected: pure cartesian product is unsupported");
		return false;
	}

	OptimizerNodeTranslation left_child;
	OptimizerNodeTranslation right_child;
	if (!TranslateOptimizerNode(*join.children[0], queryDesc, state, {}, nullptr, delim_outer_child, delim_outer_bindings, left_child) ||
		!TranslateOptimizerNode(*join.children[1], queryDesc, state, {}, nullptr, delim_outer_child, delim_outer_bindings, right_child) ||
		left_child.op == nullptr || right_child.op == nullptr)
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer cross product rejected: child translation failed");
		return false;
	}

	std::vector<ColumnRef> left_keys;
	std::vector<ColumnRef> right_keys;
	std::vector<Expression *> residuals;
	for (Expression *expr : pending_filters)
	{
		if (!CollectJoinKeys(expr, left_child.cols, left_child.schema, right_child.cols, right_child.schema, left_keys, right_keys, residuals))
		{
			if (pg_yaap_trace_hooks)
				elog(LOG, "pg_yaap: optimizer cross product rejected: join key extraction failed");
			return false;
		}
	}
	if (left_keys.empty() || left_keys.size() != right_keys.size())
	{
		if (pg_yaap_trace_hooks)
			elog(LOG,
				 "pg_yaap: optimizer cross product rejected: no usable equi-join keys (left=%zu right=%zu residuals=%zu)",
				 left_keys.size(),
				 right_keys.size(),
				 residuals.size());
		return false;
	}

	const size_t left_rows = join.children[0] != nullptr ? join.children[0]->estimated_cardinality : 0;
	const size_t right_rows = join.children[1] != nullptr ? join.children[1]->estimated_cardinality : 0;
	const bool swap_sides =
		(left_rows > 0 && right_rows > 0)
			? (left_rows < right_rows)
			: (left_child.schema.size() < right_child.schema.size());
	const auto &probe_cols = swap_sides ? right_child.cols : left_child.cols;
	const auto &probe_schema = swap_sides ? right_child.schema : left_child.schema;
	const auto &build_cols = swap_sides ? left_child.cols : right_child.cols;
	const auto &build_schema = swap_sides ? left_child.schema : right_child.schema;
	const auto &probe_keys = swap_sides ? right_keys : left_keys;
	const auto &build_keys = swap_sides ? left_keys : right_keys;

	std::vector<ColumnRef> raw_output_cols = probe_cols;
	raw_output_cols.insert(raw_output_cols.end(), build_cols.begin(), build_cols.end());
	std::vector<ColumnRef> requested_output_cols;
	FilterRequestedColumns(raw_output_cols, required_output_cols, requested_output_cols);
	if (requested_output_cols.empty())
		requested_output_cols = raw_output_cols;
	std::vector<HashJoinOutputColumnDesc> output_mappings;
	std::vector<ColumnSchema> output_schema;
	TupleDataLayout probe_key_layout;
	TupleDataLayout build_key_layout;
	TupleDataLayout probe_payload_layout;
	TupleDataLayout build_payload_layout;
	if (!BuildColumnOnlyLayoutForRefs(probe_keys, probe_cols, probe_schema, probe_key_layout) ||
		!BuildColumnOnlyLayoutForRefs(build_keys, build_cols, build_schema, build_key_layout) ||
		!BuildColumnOnlyLayout(probe_schema, probe_payload_layout) ||
		!BuildColumnOnlyLayout(build_schema, build_payload_layout) ||
		!BuildHashJoinOutputMappings(requested_output_cols, probe_cols, probe_schema, build_cols, build_schema, output_mappings, output_schema))
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer cross product rejected: join layout/output mapping build failed");
		return false;
	}

	std::vector<HashJoinFilterInputDesc> filter_inputs;
	std::vector<FilterExprDesc> filter_exprs;
	std::vector<FilterStep> filter_steps;
	std::vector<char> filter_string_consts;
	uint16_t filter_bool_regs = 0;
	std::vector<std::unique_ptr<Expression>> rewritten_residual_storage;
	std::vector<Expression *> rewritten_residuals;
	rewritten_residual_storage.reserve(residuals.size());
	rewritten_residuals.reserve(residuals.size());
	for (Expression *expr : residuals)
	{
		auto rewritten = RewriteJoinResidualAggregateRefs(expr, join.children[0].get(), join.children[1].get());
		rewritten_residuals.push_back(rewritten != nullptr ? rewritten.get() : expr);
		rewritten_residual_storage.push_back(std::move(rewritten));
	}
	if (!LowerJoinFilters(rewritten_residuals,
						  join.children[0].get(),
						  join.children[1].get(),
						  probe_cols, probe_schema,
						  build_cols, build_schema,
						  filter_inputs, filter_exprs, filter_steps, filter_string_consts, filter_bool_regs))
	{
		if (pg_yaap_trace_hooks)
			elog(LOG,
				 "pg_yaap: optimizer cross product rejected: residual join filter lowering failed (%zu residuals)",
				 residuals.size());
		return false;
	}

	dsa_pointer left_schema_dp = BuildSchemaDescriptorFromColumns(probe_schema, state->runtime_dsa);
	dsa_pointer right_schema_dp = BuildSchemaDescriptorFromColumns(build_schema, state->runtime_dsa);
	dsa_pointer output_schema_dp = BuildSchemaDescriptorFromColumns(output_schema, state->runtime_dsa);
	dsa_pointer left_key_layout_dp = SerializeTupleDataLayout(probe_key_layout, state->runtime_dsa);
	dsa_pointer right_key_layout_dp = SerializeTupleDataLayout(build_key_layout, state->runtime_dsa);
	dsa_pointer left_payload_layout_dp = SerializeTupleDataLayout(probe_payload_layout, state->runtime_dsa);
	dsa_pointer right_payload_layout_dp = SerializeTupleDataLayout(build_payload_layout, state->runtime_dsa);
	dsa_pointer output_columns_dp = BuildFilterArray(state->runtime_dsa, output_mappings.data(), sizeof(HashJoinOutputColumnDesc), output_mappings.size());
	dsa_pointer filter_inputs_dp = BuildFilterArray(state->runtime_dsa, filter_inputs.data(), sizeof(HashJoinFilterInputDesc), filter_inputs.size());
	dsa_pointer filter_exprs_dp = BuildFilterArray(state->runtime_dsa, filter_exprs.data(), sizeof(FilterExprDesc), filter_exprs.size());
	dsa_pointer filter_steps_dp = BuildFilterArray(state->runtime_dsa, filter_steps.data(), sizeof(FilterStep), filter_steps.size());
	dsa_pointer filter_string_consts_dp = BuildCharArray(state->runtime_dsa, filter_string_consts);
	if (!DsaPointerIsValid(left_schema_dp) || !DsaPointerIsValid(right_schema_dp) ||
		!DsaPointerIsValid(output_schema_dp) || !DsaPointerIsValid(left_key_layout_dp) ||
		!DsaPointerIsValid(right_key_layout_dp) || !DsaPointerIsValid(left_payload_layout_dp) ||
		!DsaPointerIsValid(right_payload_layout_dp) || !DsaPointerIsValid(output_columns_dp))
	{
		if (pg_yaap_trace_hooks)
			elog(LOG,
				 "pg_yaap: optimizer cross product rejected: DSA publish failed left_schema=%d right_schema=%d output_schema=%d left_key=%d right_key=%d left_payload=%d right_payload=%d output_cols=%d output_mappings=%zu output_schema_cols=%zu",
				 DsaPointerIsValid(left_schema_dp) ? 1 : 0,
				 DsaPointerIsValid(right_schema_dp) ? 1 : 0,
				 DsaPointerIsValid(output_schema_dp) ? 1 : 0,
				 DsaPointerIsValid(left_key_layout_dp) ? 1 : 0,
				 DsaPointerIsValid(right_key_layout_dp) ? 1 : 0,
				 DsaPointerIsValid(left_payload_layout_dp) ? 1 : 0,
				 DsaPointerIsValid(right_payload_layout_dp) ? 1 : 0,
				 DsaPointerIsValid(output_columns_dp) ? 1 : 0,
				 output_mappings.size(),
				 output_schema.size());
		return false;
	}

	auto join_op = std::make_unique<pg_yaap::pipeline::PhysicalHashJoin>(
		left_schema_dp,
		right_schema_dp,
		output_schema_dp,
		left_key_layout_dp,
		right_key_layout_dp,
		left_payload_layout_dp,
		right_payload_layout_dp,
		output_columns_dp,
		static_cast<uint16_t>(output_mappings.size()),
		filter_inputs_dp,
		filter_exprs_dp,
		filter_steps_dp,
		filter_string_consts_dp,
		static_cast<uint16_t>(filter_inputs.size()),
		static_cast<uint16_t>(filter_exprs.size()),
		static_cast<uint16_t>(filter_steps.size()),
		filter_bool_regs,
		static_cast<uint32_t>(filter_string_consts.size()),
		InvalidDsaPointer,
		static_cast<uint16_t>(probe_keys.size()),
		static_cast<uint16_t>(build_keys.size()),
		EstimateHashJoinBuildRows(swap_sides ? left_child.schema.size() : right_child.schema.size()));
	if (swap_sides)
	{
		join_op->AddChild(std::move(right_child.op));
		join_op->AddChild(std::move(left_child.op));
	}
	else
	{
		join_op->AddChild(std::move(left_child.op));
		join_op->AddChild(std::move(right_child.op));
	}

	std::vector<ColumnRef> parent_facing_cols =
		BuildParentFacingOutputCols(requested_output_cols, required_output_cols);
	out.op = std::move(join_op);
	out.cols = std::move(parent_facing_cols);
	out.schema = std::move(output_schema);
	out.final_sort_keys.clear();
	out.limit_count = 0;
	out.estimated_groups = 0;
	return true;
}

bool
BuildFinalSortKeys(const PhysicalOrderBy &order,
				   QueryDesc *queryDesc,
				   const std::vector<ColumnRef> &cols,
				   const std::vector<ColumnSchema> &schema,
				   std::vector<SortKeyDesc> &out)
{
	out.clear();
	const std::vector<bool> directions = ParseOrderDirections(queryDesc != nullptr ? queryDesc->sourceText : nullptr, order.orders.size());
	for (size_t i = 0; i < order.orders.size(); ++i)
	{
		const auto *col_expr = dynamic_cast<const BoundColumnRefExpression *>(order.orders[i]);
		uint16_t output_col_idx = UINT16_MAX;
		if (col_expr == nullptr)
		{
			if (pg_yaap_trace_hooks)
				elog(LOG, "pg_yaap: optimizer order rejected: order expr %zu is not a column ref", i);
			return false;
		}
		ColumnRef ref = BindingToColumnRef(col_expr->binding);
		const ColumnSchema *col = nullptr;
		for (size_t col_idx = 0; col_idx < cols.size() && col_idx < schema.size(); ++col_idx)
		{
			if (cols[col_idx] == ref)
			{
				col = &schema[col_idx];
				output_col_idx = static_cast<uint16_t>(col_idx);
				break;
			}
		}
		if ((col == nullptr || output_col_idx == UINT16_MAX) &&
			col_expr->binding.column_index.index < schema.size())
		{
			col = &schema[col_expr->binding.column_index.index];
			output_col_idx = static_cast<uint16_t>(col_expr->binding.column_index.index);
		}
		if (col == nullptr || output_col_idx == UINT16_MAX)
		{
			if (pg_yaap_trace_hooks)
				elog(LOG,
					 "pg_yaap: optimizer order rejected: order expr %zu binding=(%zu,%zu) not found in output cols=%zu schema=%zu",
					 i,
					 col_expr->binding.table_index.index,
					 col_expr->binding.column_index.index,
					 cols.size(),
					 schema.size());
			return false;
		}
		out.push_back(SortKeyDesc{InvalidOid, output_col_idx, directions[i], false, 0});
	}
	return true;
}

bool
TranslateOptimizerNode(const PhysicalOperator &op,
					   QueryDesc *queryDesc,
					   PgYaapQueryState *state,
					   const std::vector<Expression *> &pending_filters,
					   const std::vector<ColumnRef> *required_output_cols,
					   const PhysicalOperator *delim_outer_child,
					   const std::vector<yaap::ColumnBinding> *delim_outer_bindings,
					   OptimizerNodeTranslation &out)
{
	if (pg_yaap_trace_hooks)
		elog(LOG,
			 "pg_yaap: TranslateOptimizerNode op_type=%d pending_filters=%zu required_output_cols=%zu",
			 static_cast<int>(op.type),
			 pending_filters.size(),
			 required_output_cols != nullptr ? required_output_cols->size() : 0);
	switch (op.type)
	{
		case PhysicalOperatorType::TABLE_SCAN:
			return TranslateTableScanNode(static_cast<const PhysicalTableScan &>(op), state, pending_filters, required_output_cols, out);

		case PhysicalOperatorType::FILTER:
		{
			const auto &filter = static_cast<const PhysicalFilter &>(op);
			if (filter.children.size() != 1 || filter.children[0] == nullptr)
				return false;
			std::vector<Expression *> next_filters = pending_filters;
			next_filters.insert(next_filters.end(), filter.expressions.begin(), filter.expressions.end());
			return TranslateOptimizerNode(*filter.children[0], queryDesc, state, next_filters, required_output_cols, delim_outer_child, delim_outer_bindings, out);
		}

		case PhysicalOperatorType::PROJECTION:
			if (!pending_filters.empty())
				return false;
			return TranslateProjectionNode(static_cast<const PhysicalProjection &>(op), queryDesc, state, required_output_cols, delim_outer_child, delim_outer_bindings, out);

		case PhysicalOperatorType::HASH_GROUP_BY:
			if (!pending_filters.empty())
				return false;
			return TranslateHashAggregateNode(static_cast<const PhysicalHashAggregate &>(op), queryDesc, state, delim_outer_child, delim_outer_bindings, out);

		case PhysicalOperatorType::HASH_JOIN:
			return TranslateHashJoinNode(static_cast<const PhysicalHashJoin &>(op), queryDesc, state, pending_filters, required_output_cols, delim_outer_child, delim_outer_bindings, out);

		case PhysicalOperatorType::CROSS_PRODUCT:
			return TranslateCrossProductNode(static_cast<const PhysicalCrossProduct &>(op), queryDesc, state, pending_filters, required_output_cols, delim_outer_child, delim_outer_bindings, out);

		case PhysicalOperatorType::DELIM_SCAN:
			return TranslateDelimScanNode(static_cast<const PhysicalDelimScan &>(op), queryDesc, state, pending_filters, delim_outer_child, delim_outer_bindings, out);

		case PhysicalOperatorType::ORDER_BY:
		{
			if (!pending_filters.empty())
				return false;
			const auto &order = static_cast<const PhysicalOrderBy &>(op);
			if (order.children.size() != 1 || order.children[0] == nullptr)
				return false;
			if (!TranslateOptimizerNode(*order.children[0], queryDesc, state, {}, required_output_cols, delim_outer_child, delim_outer_bindings, out))
			{
				if (pg_yaap_trace_hooks)
					elog(LOG, "pg_yaap: optimizer order rejected: child translation failed");
				return false;
			}
			return BuildFinalSortKeys(order, queryDesc, out.cols, out.schema, out.final_sort_keys);
		}

		case PhysicalOperatorType::LIMIT:
		{
			if (!pending_filters.empty())
				return false;
			const auto &limit = static_cast<const PhysicalLimit &>(op);
			if (limit.children.size() != 1 || limit.children[0] == nullptr)
				return false;
			uint64_t limit_count = 0;
			if (!TryParseLimitExpression(limit.limit_count, limit_count))
				return false;
			OptimizerNodeTranslation child;
			if (!TranslateOptimizerNode(*limit.children[0], queryDesc, state, {}, required_output_cols, delim_outer_child, delim_outer_bindings, child))
				return false;
			out = std::move(child);
			out.limit_count = limit_count;
			return true;
		}

		default:
			return false;
	}
}

bool
BuildAllProjectionColumnRefs(const PhysicalProjection &projection,
							 const PhysicalTableScan &scan,
							 std::vector<ColumnRef> &out_cols)
{
	out_cols.clear();
	for (Expression *expr : projection.select_list)
	{
		const auto *colref = dynamic_cast<const BoundColumnRefExpression *>(expr);
		if (colref == nullptr || colref->binding.table_index.index != scan.table_index.index)
			return false;
		out_cols.push_back(BindingToColumnRef(colref->binding));
	}
	return !out_cols.empty();
}

bool
ExtractScanShape(const PhysicalOperator &op,
				 const PhysicalTableScan *&out_scan,
				 std::vector<ColumnRef> &out_cols)
{
	out_scan = nullptr;
	switch (op.type)
	{
		case PhysicalOperatorType::TABLE_SCAN:
			out_scan = static_cast<const PhysicalTableScan *>(&op);
			return BuildProjectedTableColumnRefs(out_scan->relid,
												 static_cast<Index>(out_scan->table_index.index + 1),
												 out_scan->projected_columns,
												 out_cols);
		case PhysicalOperatorType::PROJECTION:
		{
			const auto &projection = static_cast<const PhysicalProjection &>(op);
			if (projection.children.size() != 1 || projection.children[0] == nullptr ||
				projection.children[0]->type != PhysicalOperatorType::TABLE_SCAN)
				return false;
			out_scan = static_cast<const PhysicalTableScan *>(projection.children[0].get());
			return BuildAllProjectionColumnRefs(projection, *out_scan, out_cols);
		}
		default:
			return false;
	}
}

}  // namespace pg_yaap::optimizer_translator_detail

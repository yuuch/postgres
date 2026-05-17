#include "parallel/pipeline/yaap_opt_translator_internal.hpp"

#include <sstream>

namespace pg_yaap::optimizer_translator_detail {

static bool
BuildOrderedSchemaForRefs(const std::vector<ColumnRef> &refs,
						  const std::vector<ColumnRef> &available_cols,
						  const std::vector<ColumnSchema> &available_schema,
						  std::vector<ColumnSchema> &out_schema)
{
	out_schema.clear();
	out_schema.reserve(refs.size());
	for (const ColumnRef &ref : refs)
	{
		const ColumnSchema *col = nullptr;
		if (!LookupRawColumn(ref, available_cols, available_schema, col) || col == nullptr)
			return false;
		out_schema.push_back(*col);
	}
	return true;
}

static bool
OutputBindingsMatch(const std::vector<yaap::PhysicalOperator::OutputColumn> &lhs,
					  const std::vector<yaap::PhysicalOperator::OutputColumn> &rhs)
{
	if (lhs.size() != rhs.size())
		return false;
	for (size_t i = 0; i < lhs.size(); ++i)
	{
		if (lhs[i].binding.table_index.index != rhs[i].binding.table_index.index ||
			lhs[i].binding.column_index.index != rhs[i].binding.column_index.index)
			return false;
	}
	return true;
}

static bool
BuildOrderedOutputBindingsForRefs(const std::vector<ColumnRef> &requested_refs,
								  const std::vector<ColumnRef> &raw_refs,
								  const std::vector<yaap::PhysicalOperator::OutputColumn> &raw_outputs,
								  std::vector<yaap::PhysicalOperator::OutputColumn> &out_bindings)
{
	if (raw_refs.size() != raw_outputs.size())
	{
		if (pg_yaap_trace_hooks)
			elog(LOG,
				 "pg_yaap: output binding alignment size mismatch requested=%zu raw_refs=%zu raw_outputs=%zu",
				 requested_refs.size(),
				 raw_refs.size(),
				 raw_outputs.size());
		return false;
	}

	if (requested_refs.size() == raw_refs.size())
	{
		bool identical = true;
		for (size_t i = 0; i < requested_refs.size(); ++i)
		{
			if (!SameColumnRef(requested_refs[i], raw_refs[i]))
			{
				if (pg_yaap_trace_hooks)
					elog(LOG,
						 "pg_yaap: output binding alignment order mismatch idx=%zu requested=(%u,%d) raw=(%u,%d)",
						 i,
						 requested_refs[i].varno,
						 requested_refs[i].attno,
						 raw_refs[i].varno,
						 raw_refs[i].attno);
				identical = false;
				break;
			}
		}
		if (identical)
		{
			out_bindings = raw_outputs;
			return true;
		}
	}

	out_bindings.clear();
	out_bindings.reserve(requested_refs.size());
	std::vector<bool> used(raw_refs.size(), false);
	for (const ColumnRef &ref : requested_refs)
	{
		bool matched = false;
		for (size_t i = 0; i < raw_refs.size(); ++i)
		{
			if (used[i] || !SameColumnRef(ref, raw_refs[i]))
				continue;
			out_bindings.push_back(raw_outputs[i]);
			used[i] = true;
			matched = true;
			break;
		}
		if (!matched)
		{
			if (pg_yaap_trace_hooks)
				elog(LOG,
					 "pg_yaap: output binding alignment missing requested=(%u,%d) raw_refs=%zu",
					 ref.varno,
					 ref.attno,
					 raw_refs.size());
			return false;
		}
	}
	return true;
}

static size_t
CountSourceOutputMatches(const PhysicalOperator *source_op,
						 const std::vector<ColumnRef> *required_refs)
{
	if (source_op == nullptr || required_refs == nullptr || required_refs->empty())
		return 0;
	size_t matched_count = 0;
	for (const ColumnRef &required_ref : *required_refs)
	{
		for (const auto &output : source_op->outputs)
		{
			if (SameColumnRef(required_ref, BindingToColumnRef(output.binding)))
			{
				++matched_count;
				break;
			}
		}
	}
	return matched_count;
}

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
			yaap::ColumnBinding resolved_binding = column->binding;
			if (!ResolveOutputBinding(column, left_source_op, resolved_binding))
				(void) ResolveOutputBinding(column, right_source_op, resolved_binding);
			return std::make_unique<BoundColumnRefExpression>(
				resolved_binding,
				column->table_name,
				column->column_name);
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

enum class JoinExprSide : uint8_t {
	NONE = 0,
	LEFT = 1,
	RIGHT = 2,
	BOTH = 3,
};

static JoinExprSide
DetermineJoinExprSide(const Expression *expr,
					   const std::vector<ColumnRef> &left_cols,
					   const std::vector<ColumnSchema> &left_schema,
					   const std::vector<ColumnRef> &right_cols,
					   const std::vector<ColumnSchema> &right_schema)
{
	std::vector<ColumnRef> refs;
	CollectReferencedColumns(expr, refs);
	bool found_left = false;
	bool found_right = false;
	for (const ColumnRef &ref : refs)
	{
		bool ref_left = false;
		bool ref_right = false;
		const ColumnSchema *col = nullptr;
		if (LookupRawColumn(ref, left_cols, left_schema, col))
			ref_left = true;
		if (LookupRawColumn(ref, right_cols, right_schema, col))
			ref_right = true;
		if ((!ref_left && !ref_right) || (ref_left && ref_right))
			return JoinExprSide::BOTH;
		found_left = found_left || ref_left;
		found_right = found_right || ref_right;
		if (found_left && found_right)
			return JoinExprSide::BOTH;
	}
	if (found_left)
		return JoinExprSide::LEFT;
	if (found_right)
		return JoinExprSide::RIGHT;
	return JoinExprSide::NONE;
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
	const bool left_outer_join = join.join_type == yaap::JOIN_LEFT;
	const bool correlated_delim_join = join.delim_join && !join.correlated_columns.empty();
	if (join.join_type != yaap::JOIN_INNER && !left_outer_join && !scalar_delim_join && !semi_or_anti_join)
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer hash join rejected: join_type=%d", join.join_type);
		return false;
	}
	const pg_yaap::pipeline::HashJoinMatchMode join_mode =
		join.join_type == yaap::JOIN_SEMI ? pg_yaap::pipeline::HashJoinMatchMode::SEMI :
		join.join_type == yaap::JOIN_ANTI ? pg_yaap::pipeline::HashJoinMatchMode::ANTI :
		join.join_type == yaap::JOIN_LEFT ? pg_yaap::pipeline::HashJoinMatchMode::LEFT :
		pg_yaap::pipeline::HashJoinMatchMode::INNER;
	const bool left_join_output_matches =
		semi_or_anti_join &&
		join.children[0] != nullptr &&
		OutputBindingsMatch(join.outputs, join.children[0]->outputs);
	const bool right_join_output_matches =
		semi_or_anti_join &&
		join.children[1] != nullptr &&
		OutputBindingsMatch(join.outputs, join.children[1]->outputs);
	const size_t left_required_match_count =
		semi_or_anti_join &&
		CountSourceOutputMatches(join.children[0].get(), required_output_cols);
	const size_t right_required_match_count =
		semi_or_anti_join &&
		CountSourceOutputMatches(join.children[1].get(), required_output_cols);
	const bool use_required_output_side =
		semi_or_anti_join &&
		(left_required_match_count != right_required_match_count) &&
		(left_required_match_count > 0 || right_required_match_count > 0);
	const bool use_join_output_side =
		semi_or_anti_join &&
		join.join_type == yaap::JOIN_SEMI &&
		(left_join_output_matches != right_join_output_matches);
	const bool right_output_semi_or_anti =
		semi_or_anti_join &&
		(use_required_output_side
			 ? (right_required_match_count > left_required_match_count)
			 : (use_join_output_side
					? right_join_output_matches
					: join.children_swapped));
	if (pg_yaap_trace_hooks && semi_or_anti_join)
		elog(LOG,
			 "pg_yaap: semi/anti routing join_type=%d children_swapped=%d left_req=%zu right_req=%zu left_join=%d right_join=%d use_required=%d use_join=%d right_output=%d delim=%d corr=%zu",
			 join.join_type,
			 join.children_swapped ? 1 : 0,
			 left_required_match_count,
			 right_required_match_count,
			 left_join_output_matches ? 1 : 0,
			 right_join_output_matches ? 1 : 0,
			 use_required_output_side ? 1 : 0,
			 use_join_output_side ? 1 : 0,
			 right_output_semi_or_anti ? 1 : 0,
			 join.delim_join ? 1 : 0,
			 join.correlated_columns.size());

	std::vector<ColumnRef> join_condition_required_cols;
	for (Expression *expr : join.conditions)
		CollectReferencedColumns(expr, join_condition_required_cols);
	std::vector<ColumnRef> pending_filter_required_cols;
	for (Expression *expr : pending_filters)
		CollectReferencedColumns(expr, pending_filter_required_cols);

	std::vector<ColumnRef> left_required_cols = join_condition_required_cols;
	std::vector<ColumnRef> right_required_cols = join_condition_required_cols;
	if (!semi_or_anti_join)
	{
		for (const ColumnRef &ref : pending_filter_required_cols)
		{
			AppendUniqueColumnRef(ref, left_required_cols);
			AppendUniqueColumnRef(ref, right_required_cols);
		}
	}
	else
	{
		auto &output_required_cols = right_output_semi_or_anti ? right_required_cols : left_required_cols;
		for (const ColumnRef &ref : pending_filter_required_cols)
			AppendUniqueColumnRef(ref, output_required_cols);
	}
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

	auto translate_children = [&](const std::vector<Expression *> &left_filters,
								  const std::vector<Expression *> &right_filters,
								  OptimizerNodeTranslation &left_out,
								  OptimizerNodeTranslation &right_out) -> bool
	{
		if (!TranslateOptimizerNode(*join.children[0],
				queryDesc,
				state,
				left_filters,
				left_required_cols.empty() ? nullptr : &left_required_cols,
				delim_outer_child,
				delim_outer_bindings,
				left_out) ||
			!TranslateOptimizerNode(*join.children[1],
				queryDesc,
				state,
				right_filters,
				right_required_cols.empty() ? nullptr : &right_required_cols,
				correlated_delim_join ? join.children[0].get() : delim_outer_child,
				correlated_delim_join ? &join.correlated_columns : delim_outer_bindings,
				right_out) ||
			left_out.op == nullptr || right_out.op == nullptr)
		{
			if (pg_yaap_trace_hooks)
				elog(LOG, "pg_yaap: optimizer hash join rejected: child translation failed");
			return false;
		}
		return true;
	};

	std::vector<std::unique_ptr<Expression>> rewritten_join_expr_storage;
	std::vector<Expression *> rewritten_pending_filters;
	std::vector<Expression *> rewritten_join_conditions;
	rewritten_join_expr_storage.reserve(pending_filters.size() + join.conditions.size());
	rewritten_pending_filters.reserve(pending_filters.size());
	rewritten_join_conditions.reserve(join.conditions.size());
	auto rewrite_join_expr = [&](Expression *expr) -> Expression *
	{
		auto rewritten = RewriteJoinResidualAggregateRefs(
			expr,
			join.children[0].get(),
			join.children[1].get());
		Expression *result = rewritten != nullptr ? rewritten.get() : expr;
		rewritten_join_expr_storage.push_back(std::move(rewritten));
		return result;
	};
	for (Expression *expr : pending_filters)
		rewritten_pending_filters.push_back(rewrite_join_expr(expr));
	for (Expression *expr : join.conditions)
		rewritten_join_conditions.push_back(rewrite_join_expr(expr));

	auto collect_join_state =
		[&](const OptimizerNodeTranslation &left_input,
			const OptimizerNodeTranslation &right_input,
			std::vector<ColumnRef> &out_left_keys,
			std::vector<ColumnRef> &out_right_keys,
			std::vector<Expression *> &out_residuals,
			std::vector<Expression *> *out_left_pushdown,
			std::vector<Expression *> *out_right_pushdown) -> bool
	{
		out_left_keys.clear();
		out_right_keys.clear();
		out_residuals.clear();
			if (out_left_pushdown != nullptr)
				out_left_pushdown->clear();
		if (out_right_pushdown != nullptr)
			out_right_pushdown->clear();

		auto consider_pushdown = [&](Expression *expr, bool from_join_condition)
		{
			const JoinExprSide side =
				DetermineJoinExprSide(expr,
									  left_input.cols, left_input.schema,
									  right_input.cols, right_input.schema);
			if (join.join_type == yaap::JOIN_INNER)
			{
				if (side == JoinExprSide::LEFT && out_left_pushdown != nullptr)
				{
					out_left_pushdown->push_back(expr);
					return;
				}
				if (side == JoinExprSide::RIGHT && out_right_pushdown != nullptr)
				{
					out_right_pushdown->push_back(expr);
					return;
				}
			}
			else if (left_outer_join && from_join_condition &&
					 side == JoinExprSide::RIGHT && out_right_pushdown != nullptr)
			{
				out_right_pushdown->push_back(expr);
				return;
			}
			out_residuals.push_back(expr);
		};

		for (Expression *expr : rewritten_pending_filters)
			consider_pushdown(expr, false);

		std::vector<Expression *> join_condition_residuals;
		for (Expression *expr : rewritten_join_conditions)
		{
			if (!CollectJoinKeys(expr,
								 &left_input.outputs,
								 left_input.cols, left_input.schema,
								 &right_input.outputs,
								 right_input.cols, right_input.schema,
								 out_left_keys, out_right_keys,
								 join_condition_residuals))
			{
				if (pg_yaap_trace_hooks)
					elog(LOG, "pg_yaap: optimizer hash join rejected: join key extraction failed");
				return false;
			}
		}
		for (Expression *expr : join_condition_residuals)
			consider_pushdown(expr, true);
		return true;
	};

	OptimizerNodeTranslation left_child;
	OptimizerNodeTranslation right_child;
	if (!translate_children({}, {}, left_child, right_child))
		return false;

	std::vector<ColumnRef> left_keys;
	std::vector<ColumnRef> right_keys;
	std::vector<Expression *> residuals;
	std::vector<Expression *> left_pushdown_filters;
	std::vector<Expression *> right_pushdown_filters;
	if (!collect_join_state(left_child,
							right_child,
							left_keys,
							right_keys,
							residuals,
							&left_pushdown_filters,
							&right_pushdown_filters))
	{
		return false;
	}
	if (!left_pushdown_filters.empty() || !right_pushdown_filters.empty())
	{
		if (pg_yaap_trace_hooks)
			elog(LOG,
				 "pg_yaap: optimizer hash join pushing residuals into children left=%zu right=%zu",
				 left_pushdown_filters.size(),
				 right_pushdown_filters.size());
		OptimizerNodeTranslation pushed_left_child;
		OptimizerNodeTranslation pushed_right_child;
		if (!translate_children(left_pushdown_filters,
								right_pushdown_filters,
								pushed_left_child,
								pushed_right_child))
			return false;
		left_child = std::move(pushed_left_child);
		right_child = std::move(pushed_right_child);
		std::vector<Expression *> ignored_left_pushdown;
		std::vector<Expression *> ignored_right_pushdown;
		if (!collect_join_state(left_child,
								right_child,
								left_keys,
								right_keys,
								residuals,
								&ignored_left_pushdown,
								&ignored_right_pushdown))
		{
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
		(left_outer_join ? false :
		((left_rows > 0 && right_rows > 0)
			? (left_rows < right_rows)
			: (left_child.schema.size() < right_child.schema.size())));
	const auto &probe_cols = swap_sides ? right_child.cols : left_child.cols;
	const auto &probe_schema = swap_sides ? right_child.schema : left_child.schema;
	const auto &probe_outputs = swap_sides ? right_child.outputs : left_child.outputs;
	const auto &build_cols = swap_sides ? left_child.cols : right_child.cols;
	const auto &build_schema = swap_sides ? left_child.schema : right_child.schema;
	const auto &build_outputs = swap_sides ? left_child.outputs : right_child.outputs;
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
	std::vector<yaap::PhysicalOperator::OutputColumn> output_bindings;
	std::vector<yaap::PhysicalOperator::OutputColumn> raw_output_bindings = probe_outputs;
	if (!semi_or_anti_join)
		raw_output_bindings.insert(raw_output_bindings.end(), build_outputs.begin(), build_outputs.end());
	TupleDataLayout probe_key_layout;
	TupleDataLayout build_key_layout;
	TupleDataLayout probe_payload_layout;
	TupleDataLayout build_payload_layout;
	std::vector<ColumnRef> build_payload_refs;
	std::vector<ColumnSchema> build_payload_schema_storage;
	const std::vector<ColumnSchema> *build_payload_schema = &build_schema;
	{
		{
			const bool keep_all_build_payload =
				!residuals.empty();
			for (const ColumnRef &candidate : build_cols)
			{
				bool needed = keep_all_build_payload;
				for (const ColumnRef &requested_ref : requested_output_cols)
				{
					if (SameColumnRef(candidate, requested_ref))
					{
						needed = true;
						break;
					}
				}
				if (needed)
					build_payload_refs.push_back(candidate);
			}
		}
		if (build_payload_refs.empty())
		{
			if (!build_keys.empty())
				build_payload_refs.push_back(build_keys.front());
			else if (!build_cols.empty())
				build_payload_refs.push_back(build_cols.front());
		}
		if (!BuildOrderedSchemaForRefs(build_payload_refs, build_cols, build_schema, build_payload_schema_storage))
		{
			if (pg_yaap_trace_hooks)
				elog(LOG, "pg_yaap: optimizer hash join rejected: build payload schema derivation failed");
			return false;
		}
		build_payload_schema = &build_payload_schema_storage;
	}
	if (!BuildColumnOnlyLayoutForRefs(probe_keys, probe_cols, probe_schema, probe_key_layout) ||
		!BuildColumnOnlyLayoutForRefs(build_keys, build_cols, build_schema, build_key_layout) ||
		!BuildColumnOnlyLayout(probe_schema, probe_payload_layout) ||
		!BuildColumnOnlyLayout(*build_payload_schema, build_payload_layout) ||
		!BuildHashJoinOutputMappings(requested_output_cols, probe_cols, probe_schema, build_cols, build_schema, output_mappings, output_schema))
	{
		if (pg_yaap_trace_hooks)
			elog(LOG, "pg_yaap: optimizer hash join rejected: join layout/output mapping build failed");
		return false;
	}
	if (!BuildOrderedOutputBindingsForRefs(requested_output_cols, raw_output_cols, raw_output_bindings, output_bindings))
	{
		if (pg_yaap_trace_hooks)
			elog(LOG,
				 "pg_yaap: optimizer hash join rejected: output binding alignment failed probe_cols=%zu probe_outputs=%zu build_cols=%zu build_outputs=%zu raw_output_cols=%zu requested=%zu semi_or_anti=%d swapped=%d probe_child_type=%d build_child_type=%d",
				 probe_cols.size(),
				 probe_outputs.size(),
				 build_cols.size(),
				 build_outputs.size(),
				 raw_output_cols.size(),
				 requested_output_cols.size(),
				 semi_or_anti_join ? 1 : 0,
				 swap_sides ? 1 : 0,
				 static_cast<int>(swap_sides ? join.children[1]->type : join.children[0]->type),
				 static_cast<int>(swap_sides ? join.children[0]->type : join.children[1]->type));
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
						  &probe_outputs,
						  probe_cols, probe_schema,
						  &build_outputs,
						  build_cols, build_schema,
						  filter_inputs, filter_exprs, filter_steps, filter_string_consts, filter_bool_regs))
	{
		if (pg_yaap_trace_hooks)
		{
			for (size_t i = 0; i < rewritten_residuals.size(); ++i)
			{
				const Expression *expr = rewritten_residuals[i];
				if (const auto *func = dynamic_cast<const BoundFunctionExpression *>(expr))
					elog(LOG,
						 "pg_yaap: residual[%zu] fn=%s children=%zu",
						 i,
						 func->function_name.c_str(),
						 func->children.size());
				else if (const auto *conj = dynamic_cast<const BoundConjunctionExpression *>(expr))
					elog(LOG,
						 "pg_yaap: residual[%zu] conjunction type=%d children=%zu",
						 i,
						 conj->bool_expr_type,
						 conj->children.size());
				else
					elog(LOG,
						 "pg_yaap: residual[%zu] type=%d",
						 i,
						 expr != nullptr ? static_cast<int>(expr->type) : -1);
			}
			elog(LOG,
				 "pg_yaap: optimizer hash join rejected: residual join filter lowering failed (%zu residuals)",
				 residuals.size());
		}
		return false;
	}

	dsa_pointer left_schema_dp = BuildSchemaDescriptorFromColumns(probe_schema, state->runtime_dsa);
	dsa_pointer right_schema_dp = BuildSchemaDescriptorFromColumns(*build_payload_schema, state->runtime_dsa);
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
	out.outputs = std::move(output_bindings);
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
		if (!CollectJoinKeys(expr,
							 &left_child.outputs,
							 left_child.cols,
							 left_child.schema,
							 &right_child.outputs,
							 right_child.cols,
							 right_child.schema,
							 left_keys,
							 right_keys,
							 residuals))
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
	const auto &probe_outputs = swap_sides ? right_child.outputs : left_child.outputs;
	const auto &build_cols = swap_sides ? left_child.cols : right_child.cols;
	const auto &build_schema = swap_sides ? left_child.schema : right_child.schema;
	const auto &build_outputs = swap_sides ? left_child.outputs : right_child.outputs;
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
	std::vector<yaap::PhysicalOperator::OutputColumn> output_bindings;
	std::vector<yaap::PhysicalOperator::OutputColumn> raw_output_bindings = probe_outputs;
	raw_output_bindings.insert(raw_output_bindings.end(), build_outputs.begin(), build_outputs.end());
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
	if (!BuildOrderedOutputBindingsForRefs(requested_output_cols, raw_output_cols, raw_output_bindings, output_bindings))
	{
		if (pg_yaap_trace_hooks)
			elog(LOG,
				 "pg_yaap: optimizer cross product rejected: output binding alignment failed probe_cols=%zu probe_outputs=%zu build_cols=%zu build_outputs=%zu raw_output_cols=%zu requested=%zu",
				 probe_cols.size(),
				 probe_outputs.size(),
				 build_cols.size(),
				 build_outputs.size(),
				 raw_output_cols.size(),
				 requested_output_cols.size());
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
						  &probe_outputs,
						  probe_cols, probe_schema,
						  &build_outputs,
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
	out.outputs = std::move(output_bindings);
	out.final_sort_keys.clear();
	out.limit_count = 0;
	out.estimated_groups = 0;
	return true;
}

static bool
BuildFinalSortKeys(const PhysicalOrderBy &order,
				   QueryDesc *queryDesc,
				   const std::vector<yaap::PhysicalOperator::OutputColumn> *outputs,
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
		const ColumnSchema *col = nullptr;
		ColumnRef ref{};
		if (outputs != nullptr)
		{
			for (size_t col_idx = 0; col_idx < outputs->size() && col_idx < schema.size(); ++col_idx)
			{
				const auto &output = (*outputs)[col_idx];
				const bool binding_match =
					output.binding.table_index.index == col_expr->binding.table_index.index &&
					output.binding.column_index.index == col_expr->binding.column_index.index;
				const bool name_match =
					!col_expr->table_name.empty() &&
					!col_expr->column_name.empty() &&
					output.table_name == col_expr->table_name &&
					output.column_name == col_expr->column_name;
				if (!binding_match && !name_match)
					continue;
				col = &schema[col_idx];
				output_col_idx = static_cast<uint16_t>(col_idx);
				break;
			}
		}
		if ((col == nullptr || output_col_idx == UINT16_MAX) &&
			LookupNamedExprInputColumn(col_expr, outputs, cols, schema, ref, col) && col != nullptr)
		{
			for (size_t col_idx = 0; col_idx < cols.size() && col_idx < schema.size(); ++col_idx)
			{
				if (cols[col_idx] == ref)
				{
					output_col_idx = static_cast<uint16_t>(col_idx);
					break;
				}
			}
		}
		if ((col == nullptr || output_col_idx == UINT16_MAX) &&
			col_expr->binding.column_index.index < schema.size())
		{
			output_col_idx = static_cast<uint16_t>(col_expr->binding.column_index.index);
			col = &schema[output_col_idx];
		}
		if (col == nullptr || output_col_idx == UINT16_MAX)
		{
			if (pg_yaap_trace_hooks)
			{
				elog(LOG,
					 "pg_yaap: optimizer order rejected: order expr %zu binding=(%zu,%zu) not found in output cols=%zu schema=%zu",
					 i,
					 col_expr->binding.table_index.index,
					 col_expr->binding.column_index.index,
					 cols.size(),
					 schema.size());
				if (outputs != nullptr)
				{
					for (size_t out_idx = 0; out_idx < outputs->size(); ++out_idx)
						elog(LOG,
							 "pg_yaap: optimizer order output[%zu]=%s.%s binding=(%zu,%zu)",
							 out_idx,
							 (*outputs)[out_idx].table_name.c_str(),
							 (*outputs)[out_idx].column_name.c_str(),
							 (*outputs)[out_idx].binding.table_index.index,
							 (*outputs)[out_idx].binding.column_index.index);
				}
			}
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
		{
			OptimizerNodeTranslation agg_out;
			if (!TranslateHashAggregateNode(static_cast<const PhysicalHashAggregate &>(op), queryDesc, state, delim_outer_child, delim_outer_bindings, agg_out))
				return false;
			return ApplyPostAggregateFilters(std::move(agg_out), static_cast<const PhysicalHashAggregate &>(op), pending_filters, state, out);
		}

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
			return BuildFinalSortKeys(order, queryDesc, &out.outputs, out.cols, out.schema, out.final_sort_keys);
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

#include "optimizer_core.hpp"

#include "../logical/logical_utils.hpp"

namespace duckdbopt {

OptimizerPass JoinPredicateExtraction::Pass() const {
    return OptimizerPass::JOIN_PREDICATE_EXTRACTION;
}

bool JoinPredicateExtraction::IsEquiJoinPredicate(Expression* expression,
                                                  const std::set<size_t>& left_tables,
                                                  const std::set<size_t>& right_tables) {
    if (!expression || expression->type != ExpressionType::BOUND_FUNCTION) {
        return false;
    }

    auto* function = static_cast<BoundFunctionExpression*>(expression);
    if (function->function_name != "=" || function->children.size() != 2) {
        return false;
    }

    std::set<size_t> left_expression_tables;
    std::set<size_t> right_expression_tables;
    CollectReferencedTables(function->children[0].get(), left_expression_tables);
    CollectReferencedTables(function->children[1].get(), right_expression_tables);

    bool left_to_right = !left_expression_tables.empty() && !right_expression_tables.empty() &&
                         IsSubset(left_expression_tables, left_tables) &&
                         IsSubset(right_expression_tables, right_tables);
    bool right_to_left = !left_expression_tables.empty() && !right_expression_tables.empty() &&
                         IsSubset(left_expression_tables, right_tables) &&
                         IsSubset(right_expression_tables, left_tables);
    return left_to_right || right_to_left;
}

std::unique_ptr<LogicalOperator> JoinPredicateExtraction::ExtractFromFilter(std::unique_ptr<LogicalOperator> filter_plan) {
    auto* filter = static_cast<LogicalFilter*>(filter_plan.get());
    auto cross_product = std::move(filter_plan->children[0]);
    auto* cross = static_cast<LogicalCrossProduct*>(cross_product.get());

    std::set<size_t> left_tables;
    std::set<size_t> right_tables;
    CollectOutputTables(cross->children[0].get(), left_tables);
    CollectOutputTables(cross->children[1].get(), right_tables);

    std::vector<std::unique_ptr<Expression>> join_conditions;
    std::vector<std::unique_ptr<Expression>> remaining_filters;

    for (auto& expression : filter->expressions) {
        if (IsEquiJoinPredicate(expression.get(), left_tables, right_tables)) {
            join_conditions.push_back(std::move(expression));
        } else {
            remaining_filters.push_back(std::move(expression));
        }
    }

    if (join_conditions.empty()) {
        filter_plan->children[0] = std::move(cross_product);
        return filter_plan;
    }

    auto join = std::make_unique<LogicalComparisonJoin>(0);
    join->conditions = std::move(join_conditions);
    join->children.push_back(std::move(cross->children[0]));
    join->children.push_back(std::move(cross->children[1]));
    join->estimated_cardinality = cross->estimated_cardinality;

    if (!remaining_filters.empty()) {
        auto remaining_filter = std::make_unique<LogicalFilter>();
        remaining_filter->expressions = std::move(remaining_filters);
        remaining_filter->children.push_back(std::move(join));
        remaining_filter->estimated_cardinality = remaining_filter->children[0]->estimated_cardinality;
        return remaining_filter;
    }

    return join;
}

std::unique_ptr<LogicalOperator> JoinPredicateExtraction::Rewrite(std::unique_ptr<LogicalOperator> plan) {
    for (auto& child : plan->children) {
        child = Rewrite(std::move(child));
    }

    if (plan->type == LogicalOperatorType::LOGICAL_FILTER &&
        plan->children.size() == 1 &&
        plan->children[0]->type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
        return ExtractFromFilter(std::move(plan));
    }

    return plan;
}

std::unique_ptr<LogicalOperator> JoinPredicateExtraction::Optimize(std::unique_ptr<LogicalOperator> plan) {
    if (!plan) {
        return nullptr;
    }
    return Rewrite(std::move(plan));
}

} // namespace duckdbopt

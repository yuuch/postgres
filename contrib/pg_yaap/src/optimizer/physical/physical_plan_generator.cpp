#include "physical_plan_generator.hpp"

#include <stdexcept>

namespace yaap {

std::unique_ptr<PhysicalOperator> PhysicalPlanGenerator::Plan(LogicalOperator& op) {
    return CreatePlan(op);
}

std::vector<Expression*> PhysicalPlanGenerator::BorrowExpressions(
    const std::vector<std::unique_ptr<Expression>>& expressions) {
    std::vector<Expression*> result;
    result.reserve(expressions.size());
    for (const auto& expression : expressions) {
        result.push_back(expression.get());
    }
    return result;
}

size_t PhysicalPlanGenerator::EstimateCardinality(LogicalOperator& op) {
    return op.estimated_cardinality;
}

std::unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalOperator& op) {
    switch (op.type) {
        case LogicalOperatorType::LOGICAL_GET:
            return CreatePlan(static_cast<LogicalGet&>(op));
        case LogicalOperatorType::LOGICAL_FILTER:
            return CreatePlan(static_cast<LogicalFilter&>(op));
        case LogicalOperatorType::LOGICAL_LIMIT:
            return CreatePlan(static_cast<LogicalLimit&>(op));
        case LogicalOperatorType::LOGICAL_WINDOW:
            return CreatePlan(static_cast<LogicalWindow&>(op));
        case LogicalOperatorType::LOGICAL_PROJECTION:
            return CreatePlan(static_cast<LogicalProjection&>(op));
        case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
            return CreatePlan(static_cast<LogicalAggregate&>(op));
        case LogicalOperatorType::LOGICAL_DISTINCT:
            return CreatePlan(static_cast<LogicalDistinct&>(op));
        case LogicalOperatorType::LOGICAL_SET_OPERATION:
            return CreatePlan(static_cast<LogicalSetOperation&>(op));
        case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
            return CreatePlan(static_cast<LogicalComparisonJoin&>(op));
        case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
            return CreatePlan(static_cast<LogicalDependentJoin&>(op));
        case LogicalOperatorType::LOGICAL_DELIM_JOIN:
            return CreatePlan(static_cast<LogicalDependentJoin&>(op));
        case LogicalOperatorType::LOGICAL_DELIM_GET:
            return CreatePlan(static_cast<LogicalDelimGet&>(op));
        case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
            return CreatePlan(static_cast<LogicalCrossProduct&>(op));
        case LogicalOperatorType::LOGICAL_ORDER:
            return CreatePlan(static_cast<LogicalOrder&>(op));
        default:
            throw std::runtime_error("Unsupported logical operator for physical planning");
    }
}

std::unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalGet& op) {
    return std::make_unique<PhysicalTableScan>(
        op.table_index,
        op.pg_rtindex,
        op.relid,
        op.table_name,
        op.projected_columns,
        BorrowExpressions(op.filters),
        EstimateCardinality(op));
}

std::unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalProjection& op) {
    if (op.children.size() != 1) {
        throw std::runtime_error("LogicalProjection expects one child");
    }
    auto child = CreatePlan(*op.children[0]);
    auto projection = std::make_unique<PhysicalProjection>(op.table_index,
                                                            BorrowExpressions(op.expressions), op.output_names,
                                                            EstimateCardinality(op));
    projection->children.push_back(std::move(child));
    return projection;
}

std::unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalFilter& op) {
    if (op.children.size() != 1) {
        throw std::runtime_error("LogicalFilter expects one child");
    }
    auto child = CreatePlan(*op.children[0]);
    auto filter = std::make_unique<PhysicalFilter>(BorrowExpressions(op.expressions), EstimateCardinality(op));
    filter->children.push_back(std::move(child));
    return filter;
}

std::unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalDistinct& op) {
    if (op.children.size() != 1) {
        throw std::runtime_error("LogicalDistinct expects one child");
    }
    auto child = CreatePlan(*op.children[0]);
    auto distinct = std::make_unique<PhysicalDistinct>(BorrowExpressions(op.expressions), EstimateCardinality(op));
    distinct->children.push_back(std::move(child));
    return distinct;
}

std::unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalSetOperation& op) {
    if (op.children.size() != 2) {
        throw std::runtime_error("LogicalSetOperation expects two children");
    }
    auto left = CreatePlan(*op.children[0]);
    auto right = CreatePlan(*op.children[1]);
    auto setop = std::make_unique<PhysicalSetOperation>(
        op.table_index,
        op.setop_type,
        op.all,
        op.output_names,
        EstimateCardinality(op));
    setop->children.push_back(std::move(left));
    setop->children.push_back(std::move(right));
    return setop;
}

std::unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalLimit& op) {
    if (op.children.size() != 1) {
        throw std::runtime_error("LogicalLimit expects one child");
    }
    auto child = CreatePlan(*op.children[0]);
    auto limit = std::make_unique<PhysicalLimit>(op.limit_count.get(), op.limit_offset.get(), EstimateCardinality(op));
    limit->children.push_back(std::move(child));
    return limit;
}

std::unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalWindow& op) {
    if (op.children.size() != 1) {
        throw std::runtime_error("LogicalWindow expects one child");
    }
    auto child = CreatePlan(*op.children[0]);
    std::vector<Expression*> orders;
    orders.reserve(op.orders.size());
    for (const auto& order : op.orders) {
        orders.push_back(order.expression.get());
    }
    auto window = std::make_unique<PhysicalWindow>(
        op.table_index,
        op.function_names,
        op.output_names,
        BorrowExpressions(op.partitions),
        std::move(orders),
        EstimateCardinality(op));
    window->children.push_back(std::move(child));
    return window;
}

std::unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalComparisonJoin& op) {
    if (op.children.size() != 2) {
        throw std::runtime_error("LogicalComparisonJoin expects two children");
    }
    auto left = CreatePlan(*op.children[0]);
    auto right = CreatePlan(*op.children[1]);
    auto hash_join = std::make_unique<PhysicalHashJoin>(
        op.join_type,
        BorrowExpressions(op.conditions),
        EstimateCardinality(op));
    hash_join->dependent = op.dependent;
    hash_join->children_swapped = op.children_swapped;
    hash_join->mark_index = op.mark_index;
    hash_join->has_mark_index = op.has_mark_index;
    hash_join->invert_result = op.invert_result;
    hash_join->children.push_back(std::move(left));
    hash_join->children.push_back(std::move(right));
    return hash_join;
}

std::unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalDependentJoin& op) {
    auto physical = CreatePlan(static_cast<LogicalComparisonJoin&>(op));
    auto* hash_join = static_cast<PhysicalHashJoin*>(physical.get());
    hash_join->correlated_columns = op.correlated_columns;
    hash_join->mark_index = op.mark_index;
    hash_join->has_mark_index = op.has_mark_index;
    hash_join->invert_result = op.invert_result;
    return physical;
}

std::unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalDelimGet& op) {
    auto physical = std::make_unique<PhysicalDelimGet>(op.table_index, EstimateCardinality(op));
    physical->correlated_columns = op.correlated_columns;
    physical->output_names = op.output_names;
    return physical;
}

std::unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalCrossProduct& op) {
    if (op.children.size() != 2) {
        throw std::runtime_error("LogicalCrossProduct expects two children");
    }
    auto left = CreatePlan(*op.children[0]);
    auto right = CreatePlan(*op.children[1]);
    auto cross_product = std::make_unique<PhysicalCrossProduct>(EstimateCardinality(op));
    cross_product->children.push_back(std::move(left));
    cross_product->children.push_back(std::move(right));
    return cross_product;
}

std::unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalAggregate& op) {
    if (op.children.size() != 1) {
        throw std::runtime_error("LogicalAggregate expects one child");
    }
    auto child = CreatePlan(*op.children[0]);
    auto hash_aggregate = std::make_unique<PhysicalHashAggregate>(
        op.group_index,
        op.aggregate_index,
        BorrowExpressions(op.groups),
        BorrowExpressions(op.expressions),
        op.group_names,
        op.aggregate_names,
        EstimateCardinality(op));
    hash_aggregate->children.push_back(std::move(child));
    return hash_aggregate;
}

std::unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalOrder& op) {
    if (op.children.size() != 1) {
        throw std::runtime_error("LogicalOrder expects one child");
    }
    auto child = CreatePlan(*op.children[0]);
    std::vector<Expression*> orders;
    orders.reserve(op.orders.size());
    for (const auto& order : op.orders) {
        orders.push_back(order.expression.get());
    }
    auto physical_order = std::make_unique<PhysicalOrderBy>(std::move(orders), EstimateCardinality(op));
    physical_order->children.push_back(std::move(child));
    return physical_order;
}

} // namespace yaap

#include "physical_plan_generator.hpp"

#include <stdexcept>

namespace yaap {

namespace {

using OutputColumn = PhysicalOperator::OutputColumn;

std::string ExprTableName(Expression *expr, const std::string &fallback) {
    auto *col = dynamic_cast<BoundColumnRefExpression *>(expr);
    return (col != nullptr && !col->table_name.empty()) ? col->table_name : fallback;
}

std::string ExprColumnName(Expression *expr, const std::string &fallback) {
    auto *col = dynamic_cast<BoundColumnRefExpression *>(expr);
    return (col != nullptr && !col->column_name.empty()) ? col->column_name : fallback;
}

void CopyOutputs(PhysicalOperator &target, const PhysicalOperator &source) {
    target.outputs = source.outputs;
}

void AppendOutputs(PhysicalOperator &target, const PhysicalOperator &source) {
    target.outputs.insert(target.outputs.end(), source.outputs.begin(), source.outputs.end());
}

} // namespace

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
    auto scan = std::make_unique<PhysicalTableScan>(
        op.table_index,
        op.pg_rtindex,
        op.relid,
        op.table_name,
        op.projected_columns,
        std::vector<Expression*>{},
        EstimateCardinality(op));
    for (auto proj_idx : op.projected_columns) {
        std::string column_name =
            proj_idx.index < op.output_names.size() ? op.output_names[proj_idx.index]
                                                    : "col" + std::to_string(proj_idx.index + 1);
        scan->outputs.push_back(OutputColumn{
            ColumnBinding{op.table_index, proj_idx},
            op.table_name,
            std::move(column_name)});
    }
    if (op.filters.empty()) {
        return scan;
    }
    auto filter = std::make_unique<PhysicalFilter>(BorrowExpressions(op.filters), EstimateCardinality(op));
    filter->children.push_back(std::move(scan));
    CopyOutputs(*filter, *filter->children[0]);
    return filter;
}

std::unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalProjection& op) {
    if (op.children.size() != 1) {
        throw std::runtime_error("LogicalProjection expects one child");
    }
    auto child = CreatePlan(*op.children[0]);
    auto projection = std::make_unique<PhysicalProjection>(op.table_index,
                                                            BorrowExpressions(op.expressions), op.output_names,
                                                            EstimateCardinality(op));
    projection->outputs.reserve(op.expressions.size());
    for (size_t idx = 0; idx < op.expressions.size(); ++idx) {
        std::string column_name =
            idx < op.output_names.size() ? op.output_names[idx]
                                         : ExprColumnName(op.expressions[idx].get(), "col" + std::to_string(idx + 1));
        projection->outputs.push_back(OutputColumn{
            ColumnBinding{op.table_index, ProjectionIndex{idx}},
            ExprTableName(op.expressions[idx].get(), "proj"),
            std::move(column_name)});
    }
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
    CopyOutputs(*filter, *filter->children[0]);
    return filter;
}

std::unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalDistinct& op) {
    if (op.children.size() != 1) {
        throw std::runtime_error("LogicalDistinct expects one child");
    }
    auto child = CreatePlan(*op.children[0]);
    auto distinct = std::make_unique<PhysicalDistinct>(BorrowExpressions(op.expressions), EstimateCardinality(op));
    distinct->children.push_back(std::move(child));
    CopyOutputs(*distinct, *distinct->children[0]);
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
    setop->outputs.reserve(op.output_names.size());
    for (size_t idx = 0; idx < op.output_names.size(); ++idx) {
        setop->outputs.push_back(OutputColumn{
            ColumnBinding{op.table_index, ProjectionIndex{idx}},
            "setop",
            op.output_names[idx]});
    }
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
    CopyOutputs(*limit, *limit->children[0]);
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
    if (child) {
        CopyOutputs(*window, *child);
    }
    for (size_t idx = 0; idx < op.function_names.size(); ++idx) {
        std::string column_name =
            idx < op.output_names.size() ? op.output_names[idx] : "window" + std::to_string(idx + 1);
        window->outputs.push_back(OutputColumn{
            ColumnBinding{op.table_index, ProjectionIndex{idx}},
            "window",
            std::move(column_name)});
    }
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
    const bool semi_or_anti = op.join_type == JOIN_SEMI || op.join_type == JOIN_ANTI;
    AppendOutputs(*hash_join, *hash_join->children[0]);
    if (!semi_or_anti) {
        AppendOutputs(*hash_join, *hash_join->children[1]);
    }
    if (op.has_mark_index) {
        hash_join->outputs.push_back(OutputColumn{
            ColumnBinding{op.mark_index, ProjectionIndex{0}},
            "mark",
            "mark"});
    }
    return hash_join;
}

std::unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalDependentJoin& op) {
    auto physical = CreatePlan(static_cast<LogicalComparisonJoin&>(op));
    auto* hash_join = static_cast<PhysicalHashJoin*>(physical.get());
    hash_join->correlated_columns = op.correlated_columns;
    hash_join->delim_join =
        (op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN) ||
        (op.type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN &&
         (op.join_type == JOIN_SEMI || op.join_type == JOIN_ANTI || op.join_type == JOIN_SINGLE));
    hash_join->mark_index = op.mark_index;
    hash_join->has_mark_index = op.has_mark_index;
    hash_join->invert_result = op.invert_result;
    return physical;
}

std::unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalDelimGet& op) {
    auto physical = std::make_unique<PhysicalDelimScan>(op.table_index, EstimateCardinality(op));
    physical->correlated_columns = op.correlated_columns;
    physical->output_names = op.output_names;
    physical->outputs.reserve(op.correlated_columns.size());
    for (size_t idx = 0; idx < op.correlated_columns.size(); ++idx) {
        std::string column_name =
            idx < op.output_names.size() ? op.output_names[idx] : "delim" + std::to_string(idx + 1);
        physical->outputs.push_back(OutputColumn{
            ColumnBinding{op.table_index, ProjectionIndex{idx}},
            "delim",
            std::move(column_name)});
    }
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
    AppendOutputs(*cross_product, *cross_product->children[0]);
    AppendOutputs(*cross_product, *cross_product->children[1]);
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
    hash_aggregate->outputs.reserve(op.groups.size() + op.expressions.size());
    for (size_t idx = 0; idx < op.groups.size(); ++idx) {
        std::string column_name =
            idx < op.group_names.size() ? op.group_names[idx]
                                        : ExprColumnName(op.groups[idx].get(), "group" + std::to_string(idx + 1));
        hash_aggregate->outputs.push_back(OutputColumn{
            ColumnBinding{op.group_index, ProjectionIndex{idx}},
            ExprTableName(op.groups[idx].get(), "group"),
            std::move(column_name)});
    }
    for (size_t idx = 0; idx < op.expressions.size(); ++idx) {
        std::string column_name =
            idx < op.aggregate_names.size() ? op.aggregate_names[idx] : "agg" + std::to_string(idx + 1);
        hash_aggregate->outputs.push_back(OutputColumn{
            ColumnBinding{op.aggregate_index, ProjectionIndex{idx}},
            "agg",
            std::move(column_name)});
    }
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
    CopyOutputs(*physical_order, *physical_order->children[0]);
    return physical_order;
}

} // namespace yaap

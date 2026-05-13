#include "optimizer_core.hpp"

#include "../adapter/yaap_adapter.hpp"
#include "../logical/logical_utils.hpp"

#include <algorithm>
#include <set>
#include <utility>

namespace yaap {

namespace {

using BindingKey = std::pair<size_t, size_t>;

BindingKey ToBindingKey(const ColumnBinding &binding) {
    return std::make_pair(binding.table_index.index, binding.column_index.index);
}

void AddBinding(const ColumnBinding &binding, std::set<BindingKey> &out) {
    out.insert(ToBindingKey(binding));
}

void CollectExpressionBindings(Expression *expression, std::set<BindingKey> &out) {
    if (!expression) {
        return;
    }

    switch (expression->type) {
        case ExpressionType::BOUND_COLUMN_REF: {
            auto *column = static_cast<BoundColumnRefExpression *>(expression);
            AddBinding(column->binding, out);
            return;
        }
        case ExpressionType::BOUND_FUNCTION: {
            auto *function = static_cast<BoundFunctionExpression *>(expression);
            for (auto &child : function->children) {
                CollectExpressionBindings(child.get(), out);
            }
            return;
        }
        case ExpressionType::BOUND_AGGREGATE: {
            auto *aggregate = static_cast<BoundAggregateExpression *>(expression);
            for (auto &child : aggregate->children) {
                CollectExpressionBindings(child.get(), out);
            }
            return;
        }
        case ExpressionType::BOUND_CONJUNCTION: {
            auto *conjunction = static_cast<BoundConjunctionExpression *>(expression);
            for (auto &child : conjunction->children) {
                CollectExpressionBindings(child.get(), out);
            }
            return;
        }
        case ExpressionType::BOUND_SUBQUERY: {
            auto *subquery = static_cast<BoundSubqueryExpression *>(expression);
            for (auto &child : subquery->children) {
                CollectExpressionBindings(child.get(), out);
            }
            return;
        }
        default:
            return;
    }
}

template <class T>
void CollectExpressionListBindings(const std::vector<std::unique_ptr<T>> &expressions, std::set<BindingKey> &out) {
    for (const auto &expression : expressions) {
        CollectExpressionBindings(expression.get(), out);
    }
}

void CollectOrderBindings(const std::vector<OrderByNode> &orders, std::set<BindingKey> &out) {
    for (const auto &order : orders) {
        CollectExpressionBindings(order.expression.get(), out);
    }
}

bool ContainsBinding(const std::set<BindingKey> &required, const ColumnBinding &binding) {
    return required.find(ToBindingKey(binding)) != required.end();
}

std::set<size_t> OutputTablesOf(LogicalOperator *op) {
    std::set<size_t> tables;
    CollectOutputTables(op, tables);
    return tables;
}

bool GetOperatorOutputBindings(LogicalOperator *op, std::vector<ColumnBinding> &out) {
    out.clear();
    if (!op) {
        return false;
    }

    switch (op->type) {
        case LogicalOperatorType::LOGICAL_FILTER:
        case LogicalOperatorType::LOGICAL_ORDER:
        case LogicalOperatorType::LOGICAL_DISTINCT:
        case LogicalOperatorType::LOGICAL_LIMIT:
            return !op->children.empty() && GetOperatorOutputBindings(op->children[0].get(), out);
        case LogicalOperatorType::LOGICAL_PROJECTION: {
            auto &projection = static_cast<LogicalProjection &>(*op);
            out.reserve(projection.expressions.size());
            for (size_t idx = 0; idx < projection.expressions.size(); ++idx) {
                out.push_back(ColumnBinding{projection.table_index, ProjectionIndex{idx}});
            }
            return true;
        }
        case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
            auto &aggregate = static_cast<LogicalAggregate &>(*op);
            out.reserve(aggregate.groups.size() + aggregate.expressions.size());
            for (size_t idx = 0; idx < aggregate.groups.size(); ++idx) {
                out.push_back(ColumnBinding{aggregate.group_index, ProjectionIndex{idx}});
            }
            for (size_t idx = 0; idx < aggregate.expressions.size(); ++idx) {
                out.push_back(ColumnBinding{aggregate.aggregate_index, ProjectionIndex{idx}});
            }
            return true;
        }
        case LogicalOperatorType::LOGICAL_GET: {
            auto &get = static_cast<LogicalGet &>(*op);
            if (get.projected_columns.empty()) {
                return false;
            }
            out.reserve(get.projected_columns.size());
            for (const auto &column : get.projected_columns) {
                out.push_back(ColumnBinding{get.table_index, column});
            }
            return true;
        }
        case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
        case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
        case LogicalOperatorType::LOGICAL_DELIM_JOIN:
        case LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
            if (op->children.size() != 2) {
                return false;
            }
            std::vector<ColumnBinding> left;
            std::vector<ColumnBinding> right;
            if (!GetOperatorOutputBindings(op->children[0].get(), left) ||
                !GetOperatorOutputBindings(op->children[1].get(), right)) {
                return false;
            }
            out = std::move(left);
            out.insert(out.end(), right.begin(), right.end());
            return true;
        }
        case LogicalOperatorType::LOGICAL_DELIM_GET: {
            auto &delim_get = static_cast<LogicalDelimGet &>(*op);
            out = delim_get.correlated_columns;
            return true;
        }
        default:
            return false;
    }
}

bool IsIdentityProjection(LogicalProjection &projection) {
    if (projection.children.size() != 1 || !projection.children[0]) {
        return false;
    }

    std::vector<ColumnBinding> child_outputs;
    if (!GetOperatorOutputBindings(projection.children[0].get(), child_outputs) ||
        child_outputs.size() != projection.expressions.size()) {
        return false;
    }

    for (size_t idx = 0; idx < projection.expressions.size(); ++idx) {
        auto *column = dynamic_cast<BoundColumnRefExpression *>(projection.expressions[idx].get());
        if (!column) {
            return false;
        }
        if (column->binding.table_index.index != child_outputs[idx].table_index.index ||
            column->binding.column_index.index != child_outputs[idx].column_index.index) {
            return false;
        }
    }
    return true;
}

std::unique_ptr<LogicalOperator> EliminateIdentityProjections(std::unique_ptr<LogicalOperator> plan) {
    if (!plan) {
        return nullptr;
    }

    for (auto &child : plan->children) {
        child = EliminateIdentityProjections(std::move(child));
    }

    if (plan->type != LogicalOperatorType::LOGICAL_PROJECTION) {
        return plan;
    }

    auto *projection = static_cast<LogicalProjection *>(plan.get());
    if (!IsIdentityProjection(*projection)) {
        return plan;
    }

    return std::move(plan->children[0]);
}

void AddBindingsForTables(const std::set<BindingKey> &required,
                          const std::set<size_t> &tables,
                          std::set<BindingKey> &out) {
    for (const auto &entry : required) {
        if (tables.find(entry.first) != tables.end()) {
            out.insert(entry);
        }
    }
}

void PruneRequiredColumns(LogicalOperator &op, const std::set<BindingKey> &required, bool all_outputs_required) {
    switch (op.type) {
        case LogicalOperatorType::LOGICAL_GET: {
            auto &get = static_cast<LogicalGet &>(op);
            if (all_outputs_required) {
                get.projected_columns.clear();
                return;
            }

            std::set<size_t> selected;
            for (const auto &entry : required) {
                if (entry.first == get.table_index.index) {
                    selected.insert(entry.second);
                }
            }
            for (const auto &filter : get.filters) {
                std::set<BindingKey> filter_refs;
                CollectExpressionBindings(filter.get(), filter_refs);
                for (const auto &entry : filter_refs) {
                    if (entry.first == get.table_index.index) {
                        selected.insert(entry.second);
                    }
                }
            }

            if (selected.empty()) {
                selected.insert(0);
            }

            get.projected_columns.clear();
            get.projected_columns.reserve(selected.size());
            for (size_t column_idx : selected) {
                get.projected_columns.push_back(ProjectionIndex{column_idx});
            }
            return;
        }
        case LogicalOperatorType::LOGICAL_FILTER: {
            if (op.children.empty()) {
                return;
            }
            std::set<BindingKey> child_required = required;
            auto &filter = static_cast<LogicalFilter &>(op);
            CollectExpressionListBindings(filter.expressions, child_required);
            PruneRequiredColumns(*op.children[0], child_required, all_outputs_required);
            return;
        }
        case LogicalOperatorType::LOGICAL_PROJECTION: {
            if (op.children.empty()) {
                return;
            }
            auto &projection = static_cast<LogicalProjection &>(op);
            std::set<BindingKey> child_required;
            for (size_t idx = 0; idx < projection.expressions.size(); ++idx) {
                ColumnBinding output_binding{projection.table_index, ProjectionIndex{idx}};
                if (all_outputs_required || ContainsBinding(required, output_binding)) {
                    CollectExpressionBindings(projection.expressions[idx].get(), child_required);
                }
            }
            PruneRequiredColumns(*op.children[0], child_required, false);
            return;
        }
        case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
            if (op.children.empty()) {
                return;
            }
            auto &aggregate = static_cast<LogicalAggregate &>(op);
            std::set<BindingKey> child_required;
            for (size_t idx = 0; idx < aggregate.groups.size(); ++idx) {
                ColumnBinding output_binding{aggregate.group_index, ProjectionIndex{idx}};
                if (all_outputs_required || ContainsBinding(required, output_binding)) {
                    CollectExpressionBindings(aggregate.groups[idx].get(), child_required);
                }
            }
            for (size_t idx = 0; idx < aggregate.expressions.size(); ++idx) {
                ColumnBinding output_binding{aggregate.aggregate_index, ProjectionIndex{idx}};
                if (all_outputs_required || ContainsBinding(required, output_binding)) {
                    CollectExpressionBindings(aggregate.expressions[idx].get(), child_required);
                }
            }
            PruneRequiredColumns(*op.children[0], child_required, false);
            return;
        }
        case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
        case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
        case LogicalOperatorType::LOGICAL_DELIM_JOIN:
        case LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
            if (op.children.size() != 2) {
                return;
            }

            std::set<BindingKey> left_required;
            std::set<BindingKey> right_required;
            const std::set<size_t> left_tables = OutputTablesOf(op.children[0].get());
            const std::set<size_t> right_tables = OutputTablesOf(op.children[1].get());

            if (all_outputs_required) {
                PruneRequiredColumns(*op.children[0], left_required, true);
                PruneRequiredColumns(*op.children[1], right_required, true);
                return;
            }

            AddBindingsForTables(required, left_tables, left_required);
            AddBindingsForTables(required, right_tables, right_required);

            if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
                op.type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN ||
                op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
                auto &join = static_cast<LogicalComparisonJoin &>(op);
                CollectExpressionListBindings(join.conditions, left_required);
                AddBindingsForTables(left_required, right_tables, right_required);
                AddBindingsForTables(right_required, left_tables, left_required);
                if (op.type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN ||
                    op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
                    auto &dependent = static_cast<LogicalDependentJoin &>(op);
                    for (const auto &binding : dependent.correlated_columns) {
                        AddBinding(binding, left_required);
                    }
                }
            }

            PruneRequiredColumns(*op.children[0], left_required, false);
            PruneRequiredColumns(*op.children[1], right_required, false);
            return;
        }
        case LogicalOperatorType::LOGICAL_ORDER: {
            if (op.children.empty()) {
                return;
            }
            std::set<BindingKey> child_required = required;
            auto &order = static_cast<LogicalOrder &>(op);
            CollectOrderBindings(order.orders, child_required);
            PruneRequiredColumns(*op.children[0], child_required, all_outputs_required);
            return;
        }
        case LogicalOperatorType::LOGICAL_DISTINCT: {
            if (op.children.empty()) {
                return;
            }
            std::set<BindingKey> child_required = required;
            auto &distinct = static_cast<LogicalDistinct &>(op);
            CollectExpressionListBindings(distinct.expressions, child_required);
            PruneRequiredColumns(*op.children[0], child_required, all_outputs_required);
            return;
        }
        case LogicalOperatorType::LOGICAL_WINDOW: {
            if (op.children.empty()) {
                return;
            }
            std::set<BindingKey> child_required;
            const std::set<size_t> child_tables = OutputTablesOf(op.children[0].get());
            AddBindingsForTables(required, child_tables, child_required);
            auto &window = static_cast<LogicalWindow &>(op);
            CollectExpressionListBindings(window.partitions, child_required);
            CollectOrderBindings(window.orders, child_required);
            PruneRequiredColumns(*op.children[0], child_required, false);
            return;
        }
        case LogicalOperatorType::LOGICAL_LIMIT: {
            if (op.children.empty()) {
                return;
            }
            std::set<BindingKey> child_required = required;
            auto &limit = static_cast<LogicalLimit &>(op);
            CollectExpressionBindings(limit.limit_count.get(), child_required);
            CollectExpressionBindings(limit.limit_offset.get(), child_required);
            PruneRequiredColumns(*op.children[0], child_required, all_outputs_required);
            return;
        }
        case LogicalOperatorType::LOGICAL_SET_OPERATION: {
            for (auto &child : op.children) {
                if (child) {
                    PruneRequiredColumns(*child, {}, true);
                }
            }
            return;
        }
        case LogicalOperatorType::LOGICAL_DELIM_GET:
            return;
        default: {
            for (auto &child : op.children) {
                if (child) {
                    PruneRequiredColumns(*child, {}, true);
                }
            }
            return;
        }
    }
}

} // namespace

OptimizerPass RemoveUnusedColumns::Pass() const {
    return OptimizerPass::REMOVE_UNUSED_COLUMNS;
}

std::unique_ptr<LogicalOperator> RemoveUnusedColumns::Optimize(std::unique_ptr<LogicalOperator> plan) {
    if (!plan) {
        return nullptr;
    }
    PruneRequiredColumns(*plan, {}, true);
    return EliminateIdentityProjections(std::move(plan));
}

} // namespace yaap

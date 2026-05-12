#include "optimizer_core.hpp"

#include "../logical/logical_utils.hpp"

#include <sstream>

namespace yaap {

OptimizerPass PredicatePropagation::Pass() const {
    return OptimizerPass::PREDICATE_PROPAGATION;
}

void PredicatePropagation::CollectConjuncts(Expression* expression, std::vector<Expression*>& conjuncts) {
    if (!expression) {
        return;
    }
    if (expression->type == ExpressionType::BOUND_CONJUNCTION) {
        auto* conjunction = static_cast<BoundConjunctionExpression*>(expression);
        if (conjunction->bool_expr_type == 0) {
            for (auto& child : conjunction->children) {
                CollectConjuncts(child.get(), conjuncts);
            }
            return;
        }
    }
    if (expression->type == ExpressionType::BOUND_SUBQUERY) {
        conjuncts.push_back(expression);
        return;
    }
    conjuncts.push_back(expression);
}

bool PredicatePropagation::IsEqualityPredicate(Expression* expression) const {
    if (!expression || expression->type != ExpressionType::BOUND_FUNCTION) {
        return false;
    }
    auto* function = static_cast<BoundFunctionExpression*>(expression);
    return function->function_name == "=" && function->children.size() == 2;
}

bool PredicatePropagation::IsColumnConstantEquality(Expression* expression,
                                                    ColumnBinding& binding,
                                                    BoundConstantExpression*& constant) const {
    if (!IsEqualityPredicate(expression)) {
        return false;
    }
    auto* function = static_cast<BoundFunctionExpression*>(expression);
    auto* left = function->children[0].get();
    auto* right = function->children[1].get();
    if (left->type == ExpressionType::BOUND_COLUMN_REF && right->type == ExpressionType::BOUND_CONSTANT) {
        binding = static_cast<BoundColumnRefExpression*>(left)->binding;
        constant = static_cast<BoundConstantExpression*>(right);
        return true;
    }
    if (left->type == ExpressionType::BOUND_CONSTANT && right->type == ExpressionType::BOUND_COLUMN_REF) {
        binding = static_cast<BoundColumnRefExpression*>(right)->binding;
        constant = static_cast<BoundConstantExpression*>(left);
        return true;
    }
    return false;
}

bool PredicatePropagation::IsColumnColumnEquality(Expression* expression,
                                                  ColumnBinding& left,
                                                  ColumnBinding& right) const {
    if (!IsEqualityPredicate(expression)) {
        return false;
    }
    auto* function = static_cast<BoundFunctionExpression*>(expression);
    if (function->children[0]->type != ExpressionType::BOUND_COLUMN_REF ||
        function->children[1]->type != ExpressionType::BOUND_COLUMN_REF) {
        return false;
    }
    left = static_cast<BoundColumnRefExpression*>(function->children[0].get())->binding;
    right = static_cast<BoundColumnRefExpression*>(function->children[1].get())->binding;
    return true;
}

void PredicatePropagation::CollectEqualities(LogicalOperator* op, std::vector<Expression*>& equalities) {
    if (!op) {
        return;
    }

    switch (op->type) {
        case LogicalOperatorType::LOGICAL_FILTER: {
            auto* filter = static_cast<LogicalFilter*>(op);
            for (auto& expression : filter->expressions) {
                std::vector<Expression*> conjuncts;
                CollectConjuncts(expression.get(), conjuncts);
                for (auto* conjunct : conjuncts) {
                    if (IsEqualityPredicate(conjunct)) {
                        equalities.push_back(conjunct);
                    }
                }
            }
            break;
        }
        case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
        case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
        case LogicalOperatorType::LOGICAL_DELIM_JOIN: {
            auto* join = static_cast<LogicalComparisonJoin*>(op);
            for (auto& expression : join->conditions) {
                std::vector<Expression*> conjuncts;
                CollectConjuncts(expression.get(), conjuncts);
                for (auto* conjunct : conjuncts) {
                    if (IsEqualityPredicate(conjunct)) {
                        equalities.push_back(conjunct);
                    }
                }
            }
            break;
        }
        default:
            break;
    }

    for (auto& child : op->children) {
        CollectEqualities(child.get(), equalities);
    }
}

std::string PredicatePropagation::SerializeExpression(Expression* expression) const {
    if (!expression) {
        return "<null>";
    }

    std::stringstream ss;
    switch (expression->type) {
        case ExpressionType::BOUND_COLUMN_REF: {
            auto* column = static_cast<BoundColumnRefExpression*>(expression);
            ss << "col(" << column->binding.table_index.index << "."
               << column->binding.column_index.index << ")";
            break;
        }
        case ExpressionType::BOUND_CONSTANT: {
            auto* constant = static_cast<BoundConstantExpression*>(expression);
            ss << "const(" << (constant->is_null ? "NULL" : constant->value) << ")";
            break;
        }
        case ExpressionType::BOUND_FUNCTION: {
            auto* function = static_cast<BoundFunctionExpression*>(expression);
            ss << "fn(" << function->function_name << ":";
            for (size_t i = 0; i < function->children.size(); ++i) {
                if (i > 0) {
                    ss << ",";
                }
                ss << SerializeExpression(function->children[i].get());
            }
            ss << ")";
            break;
        }
        case ExpressionType::BOUND_AGGREGATE: {
            auto* aggregate = static_cast<BoundAggregateExpression*>(expression);
            ss << "agg(" << aggregate->function_name << ":";
            for (size_t i = 0; i < aggregate->children.size(); ++i) {
                if (i > 0) {
                    ss << ",";
                }
                ss << SerializeExpression(aggregate->children[i].get());
            }
            ss << ")";
            break;
        }
        case ExpressionType::BOUND_CONJUNCTION: {
            auto* conjunction = static_cast<BoundConjunctionExpression*>(expression);
            ss << "conj(" << conjunction->bool_expr_type << ":";
            for (size_t i = 0; i < conjunction->children.size(); ++i) {
                if (i > 0) {
                    ss << ",";
                }
                ss << SerializeExpression(conjunction->children[i].get());
            }
            ss << ")";
            break;
        }
        case ExpressionType::BOUND_SUBQUERY: {
            auto* subquery = static_cast<BoundSubqueryExpression*>(expression);
            ss << "subquery(" << subquery->sublink_name << ":";
            for (size_t i = 0; i < subquery->children.size(); ++i) {
                if (i > 0) {
                    ss << ",";
                }
                ss << SerializeExpression(subquery->children[i].get());
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

std::unique_ptr<Expression> PredicatePropagation::CloneExpression(Expression* expression) const {
    if (!expression) {
        return nullptr;
    }

    switch (expression->type) {
        case ExpressionType::BOUND_COLUMN_REF: {
            auto* column = static_cast<BoundColumnRefExpression*>(expression);
            return std::make_unique<BoundColumnRefExpression>(column->binding, column->table_name, column->column_name);
        }
        case ExpressionType::BOUND_CONSTANT: {
            auto* constant = static_cast<BoundConstantExpression*>(expression);
            return std::make_unique<BoundConstantExpression>(constant->value, constant->is_null);
        }
        case ExpressionType::BOUND_FUNCTION: {
            auto* function = static_cast<BoundFunctionExpression*>(expression);
            auto clone = std::make_unique<BoundFunctionExpression>(function->function_name, function->op_oid);
            for (auto& child : function->children) {
                clone->children.push_back(CloneExpression(child.get()));
            }
            return clone;
        }
        case ExpressionType::BOUND_AGGREGATE: {
            auto* aggregate = static_cast<BoundAggregateExpression*>(expression);
            auto clone = std::make_unique<BoundAggregateExpression>(aggregate->function_name, aggregate->agg_oid, aggregate->is_distinct);
            for (auto& child : aggregate->children) {
                clone->children.push_back(CloneExpression(child.get()));
            }
            return clone;
        }
        case ExpressionType::BOUND_CONJUNCTION: {
            auto* conjunction = static_cast<BoundConjunctionExpression*>(expression);
            auto clone = std::make_unique<BoundConjunctionExpression>(conjunction->bool_expr_type);
            for (auto& child : conjunction->children) {
                clone->children.push_back(CloneExpression(child.get()));
            }
            return clone;
        }
        case ExpressionType::BOUND_SUBQUERY: {
            auto* subquery = static_cast<BoundSubqueryExpression*>(expression);
            auto clone = std::make_unique<BoundSubqueryExpression>(subquery->sublink_type, subquery->sublink_name);
            for (auto& child : subquery->children) {
                clone->children.push_back(CloneExpression(child.get()));
            }
            return clone;
        }
        default:
            return nullptr;
    }
}

std::map<ColumnBindingKey, PredicatePropagation::EqualityClass>
PredicatePropagation::BuildEqualityClasses(const std::vector<Expression*>& equalities) const {
    std::map<ColumnBindingKey, ColumnBindingKey> parent;
    std::map<ColumnBindingKey, std::pair<std::string, std::string>> labels;

    auto ensure = [&](ColumnBindingKey key) {
        if (parent.find(key) == parent.end()) {
            parent[key] = key;
        }
    };

    auto find_root = [&](auto&& self, ColumnBindingKey key) -> ColumnBindingKey {
        auto it = parent.find(key);
        if (it == parent.end()) {
            return key;
        }
        if (it->second.table_index == key.table_index && it->second.column_index == key.column_index) {
            return key;
        }
        it->second = self(self, it->second);
        return it->second;
    };

    auto unite = [&](ColumnBindingKey left, ColumnBindingKey right) {
        ensure(left);
        ensure(right);
        auto left_root = find_root(find_root, left);
        auto right_root = find_root(find_root, right);
        if (left_root.table_index == right_root.table_index &&
            left_root.column_index == right_root.column_index) {
            return;
        }
        parent[right_root] = left_root;
    };

    for (auto* expression : equalities) {
        ColumnBinding left;
        ColumnBinding right;
        BoundConstantExpression* constant = nullptr;
        if (IsColumnColumnEquality(expression, left, right)) {
            auto* left_ref = static_cast<BoundColumnRefExpression*>(
                static_cast<BoundFunctionExpression*>(expression)->children[0].get());
            auto* right_ref = static_cast<BoundColumnRefExpression*>(
                static_cast<BoundFunctionExpression*>(expression)->children[1].get());
            auto left_key = MakeColumnBindingKey(left);
            auto right_key = MakeColumnBindingKey(right);
            ensure(left_key);
            ensure(right_key);
            labels[left_key] = {left_ref->table_name, left_ref->column_name};
            labels[right_key] = {right_ref->table_name, right_ref->column_name};
            unite(left_key, right_key);
            continue;
        }

        if (IsColumnConstantEquality(expression, left, constant)) {
            auto* function = static_cast<BoundFunctionExpression*>(expression);
            auto* column_ref = static_cast<BoundColumnRefExpression*>(
                function->children[0]->type == ExpressionType::BOUND_COLUMN_REF
                    ? function->children[0].get()
                    : function->children[1].get());
            auto key = MakeColumnBindingKey(left);
            ensure(key);
            labels[key] = {column_ref->table_name, column_ref->column_name};
        }
    }

    std::map<ColumnBindingKey, EqualityClass> classes;
    for (const auto& entry : parent) {
        auto root = find_root(find_root, entry.first);
        auto& cls = classes[root];
        cls.columns.insert(entry.first);
        auto label_it = labels.find(entry.first);
        if (label_it != labels.end()) {
            cls.labels[entry.first] = label_it->second;
        }
    }

    for (auto* expression : equalities) {
        ColumnBinding binding;
        BoundConstantExpression* constant = nullptr;
        if (!IsColumnConstantEquality(expression, binding, constant) || constant == nullptr || constant->is_null) {
            continue;
        }

        auto root = find_root(find_root, MakeColumnBindingKey(binding));
        auto& cls = classes[root];
        if (!cls.has_constant) {
            cls.constant = std::make_unique<BoundConstantExpression>(constant->value, constant->is_null);
            cls.has_constant = true;
        }
    }

    return classes;
}

std::map<size_t, std::vector<std::unique_ptr<Expression>>>
PredicatePropagation::BuildPropagatedFilters(const std::map<ColumnBindingKey, EqualityClass>& classes,
                                             const std::set<ColumnBindingKey>& direct_constant_bindings) const {
    std::map<size_t, std::vector<std::unique_ptr<Expression>>> filters;

    for (const auto& entry : classes) {
        const auto& cls = entry.second;
        if (!cls.has_constant || !cls.constant || cls.constant->is_null) {
            continue;
        }

        for (const auto& binding_key : cls.columns) {
            if (direct_constant_bindings.find(binding_key) != direct_constant_bindings.end()) {
                continue;
            }

            auto label_it = cls.labels.find(binding_key);
            std::string table_name = label_it != cls.labels.end()
                ? label_it->second.first
                : "t" + std::to_string(binding_key.table_index);
            std::string column_name = label_it != cls.labels.end()
                ? label_it->second.second
                : "col" + std::to_string(binding_key.column_index + 1);

            auto column = std::make_unique<BoundColumnRefExpression>(
                ColumnBinding{TableIndex{binding_key.table_index}, ProjectionIndex{binding_key.column_index}},
                table_name,
                column_name);
            auto constant = std::make_unique<BoundConstantExpression>(cls.constant->value, cls.constant->is_null);
            auto equal = std::make_unique<BoundFunctionExpression>("=", 96);
            equal->children.push_back(std::move(column));
            equal->children.push_back(std::move(constant));
            filters[binding_key.table_index].push_back(std::move(equal));
        }
    }

    return filters;
}

std::unique_ptr<LogicalOperator> PredicatePropagation::InjectFilters(
    std::unique_ptr<LogicalOperator> plan,
    std::map<size_t, std::vector<std::unique_ptr<Expression>>>& filters) {
    if (!plan) {
        return nullptr;
    }

    for (auto& child : plan->children) {
        child = InjectFilters(std::move(child), filters);
    }

    if (plan->type == LogicalOperatorType::LOGICAL_GET) {
        auto* get = static_cast<LogicalGet*>(plan.get());
        auto it = filters.find(get->table_index.index);
        if (it != filters.end() && !it->second.empty()) {
            for (auto& expression : it->second) {
                AppendUniqueFilter(get->filters, std::move(expression));
            }
            filters.erase(it);
        }
    }

    return plan;
}

std::unique_ptr<LogicalOperator> PredicatePropagation::Rewrite(std::unique_ptr<LogicalOperator> plan) {
    for (auto& child : plan->children) {
        child = Rewrite(std::move(child));
    }

    if (plan->type != LogicalOperatorType::LOGICAL_CROSS_PRODUCT &&
        plan->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
        return plan;
    }

    std::vector<Expression*> equalities;
    CollectEqualities(plan.get(), equalities);

    std::set<ColumnBindingKey> direct_constant_bindings;
    for (auto* expression : equalities) {
        ColumnBinding binding;
        BoundConstantExpression* constant = nullptr;
        if (IsColumnConstantEquality(expression, binding, constant) && constant != nullptr && !constant->is_null) {
            direct_constant_bindings.insert(MakeColumnBindingKey(binding));
        }
    }

    auto classes = BuildEqualityClasses(equalities);
    auto propagated_filters = BuildPropagatedFilters(classes, direct_constant_bindings);
    return InjectFilters(std::move(plan), propagated_filters);
}

std::unique_ptr<LogicalOperator> PredicatePropagation::Optimize(std::unique_ptr<LogicalOperator> plan) {
    if (!plan) {
        return nullptr;
    }
    return Rewrite(std::move(plan));
}

} // namespace yaap

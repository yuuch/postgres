#include "optimizer_core.hpp"
#include "join_order_plan_enumerator.hpp"

#include "../adapter/duckdb_adapter.hpp"

namespace duckdbopt {

namespace {

void NormalizeJoinCardinality(LogicalOperator* plan) {
	if (!plan) {
		return;
	}
	for (auto& child : plan->children) {
		NormalizeJoinCardinality(child.get());
	}
	if ((plan->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
		 plan->type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN ||
		 plan->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) &&
		plan->children.size() == 2 && plan->children[0]) {
		auto* join = static_cast<LogicalComparisonJoin*>(plan);
		if (!join->dependent && IsSemiOrAntiJoinType(join->join_type)) {
			plan->estimated_cardinality =
				std::min(plan->estimated_cardinality, plan->children[0]->estimated_cardinality);
		}
	}
}

} // namespace

std::unique_ptr<LogicalOperator> JoinOrderOptimizer::OptimizeJoinSubset(
	uint64_t subset_mask,
	std::vector<JoinRelation>& relations,
	const std::vector<JoinCondition>& conditions) {
	JoinOrderPlanEnumerator enumerator(*this, subset_mask, relations, conditions);
	return enumerator.Solve();
}

std::unique_ptr<LogicalOperator> JoinOrderOptimizer::Rewrite(std::unique_ptr<LogicalOperator> plan) {
    if (!plan) {
        return nullptr;
    }

    if (!IsSafeJoinOrderTree(plan.get())) {
        return plan;
    }

    if (!IsReorderableJoinTree(plan.get())) {
        for (auto& child : plan->children) {
            if (child) {
                child = Rewrite(std::move(child));
            }
        }
        for (auto& child : plan->children) {
            if (!child) {
                return nullptr;
            }
        }
        if (plan->children.size() == 1 && plan->children[0]) {
            if (plan->type == LogicalOperatorType::LOGICAL_FILTER) {
                auto* filter = static_cast<LogicalFilter*>(plan.get());
                RelationStatisticsHelper statistics_helper;
                plan->estimated_cardinality = statistics_helper.EstimateFilterCardinality(
                    statistics_helper.Extract(*plan->children[0]),
                    filter->expressions);
            } else if (plan->type == LogicalOperatorType::LOGICAL_PROJECTION ||
                       plan->type == LogicalOperatorType::LOGICAL_ORDER ||
                       plan->type == LogicalOperatorType::LOGICAL_WINDOW) {
                plan->estimated_cardinality = plan->children[0]->estimated_cardinality;
            }
        }
        return plan;
    }

    std::vector<JoinRelation> relations;
    std::vector<JoinCondition> conditions;
    dp_stats_.clear();
    ExtractJoinGraph(std::move(plan), relations, conditions);

    if (relations.size() < 2) {
        if (relations.empty()) {
            return nullptr;
        }
        auto result = std::move(relations[0].plan);
        std::vector<std::unique_ptr<Expression>> residual_filters;
        for (auto& condition : conditions) {
            if (__builtin_popcountll(condition.relation_mask) < 2) {
                residual_filters.push_back(std::move(condition.expression));
            }
        }
        if (!residual_filters.empty()) {
            auto filter = std::make_unique<LogicalFilter>();
            filter->expressions = std::move(residual_filters);
            filter->children.push_back(std::move(result));
            RelationStatisticsHelper statistics_helper;
            filter->estimated_cardinality = statistics_helper.EstimateFilterCardinality(
                statistics_helper.Extract(*filter->children[0]), filter->expressions);
            result = std::move(filter);
        }
        return result;
    }

    if (relations.size() > 20) {
        throw std::runtime_error("Join order DP currently supports up to 20 relations");
    }

    std::vector<JoinCondition> join_conditions;
    std::vector<std::unique_ptr<Expression>> residual_filters;
    for (auto& condition : conditions) {
        condition.relation_mask = ReferencedRelationMask(condition.expression.get(), relations);
        if (__builtin_popcountll(condition.relation_mask) < 2) {
            residual_filters.push_back(std::move(condition.expression));
            continue;
        }
		join_conditions.push_back(
			{std::move(condition.expression), condition.relation_mask, condition.join_type, condition.invert_result, condition.from_residual_predicate});
    }

    auto components = FindJoinComponents(relations, join_conditions);
    if (components.empty()) {
        return nullptr;
    }

    std::vector<std::unique_ptr<LogicalOperator>> component_plans;
    component_plans.reserve(components.size());
    for (auto component_mask : components) {
		auto component_plan = OptimizeJoinSubset(component_mask, relations, join_conditions);
        if (!component_plan) {
            return nullptr;
        }
        component_plans.push_back(std::move(component_plan));
    }

    if (component_plans.size() == 1) {
        return std::move(component_plans[0]);
    }

    std::sort(component_plans.begin(), component_plans.end(),
              [](const std::unique_ptr<LogicalOperator>& left, const std::unique_ptr<LogicalOperator>& right) {
                  return left->estimated_cardinality < right->estimated_cardinality;
              });

    auto result = std::move(component_plans[0]);
    for (size_t idx = 1; idx < component_plans.size(); ++idx) {
        auto cross_product = std::make_unique<LogicalCrossProduct>();
        cross_product->children.push_back(std::move(result));
        cross_product->children.push_back(std::move(component_plans[idx]));
        cross_product->estimated_cardinality =
            cross_product->children[0]->estimated_cardinality * cross_product->children[1]->estimated_cardinality;
        result = std::move(cross_product);
    }

    if (!residual_filters.empty()) {
        auto filter = std::make_unique<LogicalFilter>();
        filter->expressions = std::move(residual_filters);
        filter->children.push_back(std::move(result));
        RelationStatisticsHelper statistics_helper;
        filter->estimated_cardinality = statistics_helper.EstimateFilterCardinality(
            statistics_helper.Extract(*filter->children[0]), filter->expressions);
        result = std::move(filter);
    }
    return result;
}

std::unique_ptr<LogicalOperator> JoinOrderOptimizer::Optimize(std::unique_ptr<LogicalOperator> plan) {
	if (!plan) {
		return nullptr;
	}
	auto rewritten = Rewrite(std::move(plan));
	NormalizeJoinCardinality(rewritten.get());
	return rewritten;
}

} // namespace duckdbopt

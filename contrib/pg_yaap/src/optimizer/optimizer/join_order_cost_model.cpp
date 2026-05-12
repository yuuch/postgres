#include "join_order_cost_model.hpp"
#include "join_order_plan_node.hpp"

#include <set>

namespace yaap {

JoinOrderCostModel::JoinOrderCostModel(JoinOrderOptimizer& optimizer)
    : optimizer_(optimizer) {
}

size_t JoinOrderCostModel::EstimateCardinality(uint64_t left,
                                               uint64_t right,
                                               const std::map<uint64_t, std::unique_ptr<JoinOrderDPJoinNode>>& plans,
                                               const std::vector<JoinOrderJoinRelation>& relations,
                                               const std::vector<const JoinOrderNeighborInfo*>& connections) const {
	RelationStatisticsHelper statistics_helper;
	auto left_stats = optimizer_.GetDPStats(left, plans, relations);
	auto right_stats = optimizer_.GetDPStats(right, plans, relations);
	if (left_stats.cardinality == 0 || right_stats.cardinality == 0) {
		return 0;
	}

	std::vector<Expression*> join_conditions;
	size_t residual_filter_count = 0;
	std::set<const JoinOrderFilterInfo*> seen_filters;
	const auto& left_set = plans.find(left)->second->set;
	const auto& right_set = plans.find(right)->second->set;
	double cardinality = static_cast<double>(left_stats.cardinality) * static_cast<double>(right_stats.cardinality);
	bool used_column_stats = false;
    for (const auto* connection : connections) {
        for (const auto* filter : connection->filters) {
            if (!seen_filters.insert(filter).second) {
                continue;
            }
			bool left_matches =
				JoinOrderRelationSet::IsSubset(left_set, filter->left_set) &&
				JoinOrderRelationSet::IsSubset(right_set, filter->right_set);
			bool right_matches =
				JoinOrderRelationSet::IsSubset(left_set, filter->right_set) &&
				JoinOrderRelationSet::IsSubset(right_set, filter->left_set);
			if (!(left_matches || right_matches)) {
				continue;
			}
			if (filter->from_residual_predicate) {
				residual_filter_count++;
			} else {
				if (filter->has_left_binding && filter->has_right_binding) {
					auto left_binding = left_matches ? filter->left_binding : filter->right_binding;
					auto right_binding = left_matches ? filter->right_binding : filter->left_binding;
					auto left_column = statistics_helper.LookupColumnStats(left_stats, left_binding);
					auto right_column = statistics_helper.LookupColumnStats(right_stats, right_binding);
					if (!left_column.has_stats || !right_column.has_stats) {
						left_column = statistics_helper.LookupColumnStats(left_stats, right_binding);
						right_column = statistics_helper.LookupColumnStats(right_stats, left_binding);
					}
					if (left_column.has_stats && right_column.has_stats &&
						left_column.distinct.distinct_count > 0 && right_column.distinct.distinct_count > 0) {
						auto divisor = std::max(left_column.distinct.distinct_count, right_column.distinct.distinct_count);
						cardinality /= static_cast<double>(divisor);
						used_column_stats = true;
						continue;
					}
				}
				join_conditions.push_back(filter->condition.expression.get());
            }
		}
	}

	int primary_join_type = 0;
	for (const auto* connection : connections) {
		for (const auto* filter : connection->filters) {
			if (filter->from_residual_predicate) {
				continue;
			}
			if (filter->join_type == JOIN_INNER) {
				primary_join_type = JOIN_INNER;
				goto join_type_resolved;
			}
			if (primary_join_type == JOIN_INNER) {
				primary_join_type = filter->join_type;
			}
		}
	}
join_type_resolved:

	if (!used_column_stats && !join_conditions.empty()) {
		cardinality = static_cast<double>(
			statistics_helper.EstimateJoinCardinality(left_stats, right_stats, join_conditions));
	}

	for (size_t idx = 0; idx < residual_filter_count; ++idx) {
		cardinality *= RelationStatisticsHelper::DEFAULT_SELECTIVITY;
	}

	if (cardinality < 1.0 && (left_stats.cardinality > 0 && right_stats.cardinality > 0)) {
		cardinality = 1.0;
	}
	if (IsSemiOrAntiJoinType(primary_join_type)) {
		cardinality = std::min(cardinality, static_cast<double>(left_stats.cardinality));
	}
    return static_cast<size_t>(cardinality);
}

double JoinOrderCostModel::ComputeCost(uint64_t left,
                                       uint64_t right,
                                       size_t join_cardinality,
                                       const std::map<uint64_t, std::unique_ptr<JoinOrderDPJoinNode>>& plans) const {
    auto left_state = plans.find(left);
    auto right_state = plans.find(right);
    return left_state->second->cost + right_state->second->cost + static_cast<double>(join_cardinality);
}

RelationStats JoinOrderCostModel::CombineStats(uint64_t left,
                                               uint64_t right,
                                               size_t join_cardinality,
                                               const std::map<uint64_t, std::unique_ptr<JoinOrderDPJoinNode>>& plans,
                                               const std::vector<JoinOrderJoinRelation>& relations) const {
    auto left_stats = optimizer_.GetDPStats(left, plans, relations);
    auto right_stats = optimizer_.GetDPStats(right, plans, relations);
    RelationStatisticsHelper statistics_helper;
    return statistics_helper.CombineReorderableStats(left_stats, right_stats, join_cardinality);
}

} // namespace yaap

#include "join_order_query_graph.hpp"

namespace yaap {

JoinOrderQueryGraphManager::JoinOrderQueryGraphManager(uint64_t subset_mask)
    : subset_mask_(subset_mask) {
}

void JoinOrderQueryGraphManager::Build(const std::vector<JoinOrderJoinCondition>& conditions) {
	filter_infos_.clear();
	size_t filter_index = 0;
	for (const auto& condition : conditions) {
		auto condition_mask = condition.relation_mask & subset_mask_;
		if (__builtin_popcountll(condition_mask) < 2) {
			continue;
		}
		uint64_t left = 0;
		uint64_t right = 0;
		bool derived_sides = TryExtractJoinSides(condition.expression.get(), left, right);
		if (derived_sides) {
			left &= subset_mask_;
			right &= subset_mask_;
		}

		auto emit_filter = [&](uint64_t left_mask, uint64_t right_mask, bool residual) {
			if (left_mask == 0 || right_mask == 0 || (left_mask & right_mask) != 0) {
				return;
			}
			auto& left_set = set_manager_.GetJoinRelation(left_mask);
			auto& right_set = set_manager_.GetJoinRelation(right_mask);
			auto filter_info = std::make_unique<JoinOrderFilterInfo>(
				condition, left_set, right_set, filter_index++, condition.join_type);
			filter_info->invert_result = condition.invert_result;
			filter_info->from_residual_predicate = residual;
			CollectColumnBindingsByMask(condition.expression.get(), left_mask, right_mask, *filter_info);
			auto* filter_ptr = filter_info.get();
			filter_infos_.push_back(std::move(filter_info));
			query_graph_.CreateEdge(left_set, right_set, filter_ptr);
			query_graph_.CreateEdge(right_set, left_set, filter_ptr);
		};

		if (derived_sides && left != 0 && right != 0 &&
			__builtin_popcountll(left) == 1 && __builtin_popcountll(right) == 1) {
			emit_filter(left, right, false);
			continue;
		}

		uint64_t subset = condition_mask;
		while (subset != 0) {
			subset = (subset - 1) & condition_mask;
			if (subset == 0 || subset == condition_mask) {
				continue;
			}
			auto other = condition_mask ^ subset;
			if (subset > other) {
				continue;
			}
			emit_filter(subset, other, condition.from_residual_predicate);
		}
	}
}

const JoinOrderQueryGraphEdges& JoinOrderQueryGraphManager::GetQueryGraphEdges() const {
	return query_graph_;
}

const std::vector<std::unique_ptr<JoinOrderFilterInfo>>& JoinOrderQueryGraphManager::GetFilterInfos() const {
	return filter_infos_;
}

const JoinOrderRelationSetManager& JoinOrderQueryGraphManager::GetSetManager() const {
	return set_manager_;
}

JoinOrderRelationSetManager& JoinOrderQueryGraphManager::GetSetManager() {
	return set_manager_;
}

} // namespace yaap

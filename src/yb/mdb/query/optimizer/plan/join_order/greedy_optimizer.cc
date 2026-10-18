// Portions Copyright (c) YugabyteDB, Inc.

#include "yb/mdb/query/optimizer/plan/join_order/greedy_optimizer.h"

#include <algorithm>
#include <cmath>
#include <limits>

#include "query/optimizer/plan/join/index_nested_loop_plan.h"
#include "query/optimizer/plan/join/hash_join_plan.h"
#include "query/optimizer/rdf_model/plan/triple_plan.h"
#include "query/executor/binding_iter/docdb_scan.h"
#include "graph_models/rdf_model/rdf_object_id.h"
#include "yb/mdb/model/rdf_yb_model.h"
#include "yb/util/flags.h"
#include "yb/util/logging.h"

DEFINE_NON_RUNTIME_bool(mdb_enable_hash_join, false,
    "Allow statistics-based hash selection for supported two-pattern predicate chains");
DEFINE_NON_RUNTIME_uint64(mdb_hash_join_max_estimated_build_bytes, 16 * 1024 * 1024,
    "Planning threshold for estimated hash build bytes; not a runtime memory limit");

bool GreedyOptimizer::should_use_hash_join(const Plan& left, const Plan& right) {
    if (!FLAGS_mdb_enable_hash_join) return false;
    const auto& model = yb::mdb::RdfYBModel::Instance();
    const auto* stats = model.GetStatistics();
    if (stats == nullptr || !model.UsePartitionLoad() || model.HasTransaction()) return false;
    const auto* lhs = dynamic_cast<const SPARQL::TriplePlan*>(&left);
    const auto* rhs = dynamic_cast<const SPARQL::TriplePlan*>(&right);
    if (lhs == nullptr || rhs == nullptr) return false;
    const auto unbound_chain_scan = [](const SPARQL::TriplePlan& scan) {
        return scan.subject.is_var() && scan.object.is_var() && scan.predicate.is_OID()
            && !scan.subject_assigned && !scan.object_assigned
            && scan.subject != scan.object;
    };
    if (!unbound_chain_scan(*lhs) || !unbound_chain_scan(*rhs)
        || lhs->object != rhs->subject || lhs->subject == rhs->object
        || !rhs->tablet_id_.empty()) return false;
    // A fixed POS anchor contains the entire predicate partition. SPO fragments
    // lack conditional per-tablet key statistics, so retain their existing plan.
    if (!lhs->tablet_id_.empty() && lhs->tablet_perm_index_ != 1) return false;
    const auto storage_id = [](Id term) {
        const auto id = term.get_OID().id;
        const auto mapped = g_pack_to_gmap.find(id);
        return mapped == g_pack_to_gmap.end() ? id : mapped->second;
    };
    const auto objects = stats->predicate_object_counts.find(storage_id(lhs->predicate));
    const auto right_count = stats->term_counts[1].find(storage_id(rhs->predicate));
    const auto right_subjects = stats->predicate_subject_counts.find(storage_id(rhs->predicate));
    if (objects == stats->predicate_object_counts.end()
        || right_count == stats->term_counts[1].end()
        || right_subjects == stats->predicate_subject_counts.end()) return false;
    const double distinct = objects->second.size();
    const double limit = DocDBScan::kMaxDynamicPartitions;
    const double build_rows = right_count->second;
    if (distinct <= limit || build_rows == 0
        || build_rows > HashJoinBindingIter::kDefaultMaxBuildRows) return false;
    // Two ObjectIds per row plus conservative per-row container/hash allowance.
    // This is a planning estimate, not an allocator accounting or spill budget.
    const long double build_bytes = 4096 + build_rows
        * (2 * rhs->get_vars().size() * sizeof(ObjectId) + 160);
    if (build_bytes > FLAGS_mdb_hash_join_max_estimated_build_bytes) return false;
    double probes = 0, partition_rows = 0, repeated_matches = 0;
    for (const auto& [key, frequency] : objects->second) {
        const auto subject = stats->term_counts[0].find(key);
        // Empty subject partitions have different cache/reload behavior.
        if (subject == stats->term_counts[0].end()) return false;
        probes += frequency;
        partition_rows += subject->second;
        // Partition reads include all predicates; the SPO prefix seek visits
        // only rows under Q, so unrelated predicates must not inflate probes.
        const auto match = right_subjects->second.find(key);
        if (match != right_subjects->second.end()) {
            repeated_matches += static_cast<double>(frequency) * match->second;
        }
    }
    const double index_work = stats->rows + 1
        + limit * (1 + partition_rows / distinct) + probes + repeated_matches;
    const double hash_work = 2 * build_rows + probes;
    if (index_work > 1.5 * hash_work) return true;
    // With cold SPO/POS caches, >64 present subject keys require 64 targeted
    // reads plus full-index fallback; hash reads one fixed predicate partition.
    // Prefer that read reduction only when modeled row work is also lower.
    // Existing caches invalidate this cold-read argument, so retain the margin.
    return !model.HasQueryCaches(0) && !model.HasQueryCaches(1)
        && index_work > hash_work;
}

std::unique_ptr<Plan> GreedyOptimizer::get_plan(
    const std::vector<std::unique_ptr<Plan>>& real_base_plans, bool compare_first_join) {
  assert(!real_base_plans.empty());
  const auto base_plans_size = real_base_plans.size();
  if (base_plans_size == 2) {
    for (size_t first : {0U, 1U}) {
      const auto second = 1 - first;
      if (should_use_hash_join(*real_base_plans[first], *real_base_plans[second])) {
        LOG(INFO) << "MDB hash join selected: statistics-based predicate chain";
        return std::make_unique<HashJoinPlan>(
            real_base_plans[first]->clone(), real_base_plans[second]->clone());
      }
    }
  }

  // We have to duplicate the base plans to not affect them outside this function
  std::vector<std::unique_ptr<Plan>> base_plans;
  for (auto& plan : real_base_plans) {
    base_plans.push_back(plan->clone());
  }

  // choose the first scan
  int best_index = 0;
  double best_cost = std::numeric_limits<double>::infinity();
  for (size_t j = 0; j < base_plans_size; j++) {
    auto current_element_cost = base_plans[j]->estimate_cost();
    if (compare_first_join && base_plans_size > 1) {
      current_element_cost = std::numeric_limits<double>::infinity();
      for (size_t k = 0; k < base_plans_size; ++k) {
        if (j == k || base_plans[j]->cartesian_product_needed(*base_plans[k])) continue;
        IndexNestedLoopPlan prefix(base_plans[j]->clone(), base_plans[k]->clone());
        current_element_cost = std::min(current_element_cost, prefix.estimate_cost());
      }
    }
    if (current_element_cost < best_cost) {
      best_cost = current_element_cost;
      best_index = j;
    }
  }
  if (!std::isfinite(best_cost)) {
    for (size_t j = 0; j < base_plans_size; ++j) {
      const auto cost = base_plans[j]->estimate_cost();
      if (cost < best_cost) {
        best_cost = cost;
        best_index = j;
      }
    }
  }
  auto root_plan = std::move(base_plans[best_index]);

  // choose the next scan and make a Join (left deep plan)
  for (size_t i = 1; i < base_plans_size; i++) {
    best_index = 0;
    best_cost = std::numeric_limits<double>::infinity();
    std::unique_ptr<Plan> best_step_plan = nullptr;

    for (size_t j = 0; j < base_plans_size; j++) {
      if (base_plans[j] != nullptr && !base_plans[j]->cartesian_product_needed(*root_plan)) {
        auto nested_loop_plan =
            std::make_unique<IndexNestedLoopPlan>(root_plan->clone(), base_plans[j]->clone());
        auto nested_loop_cost = nested_loop_plan->estimate_cost();

        if (nested_loop_cost < best_cost) {
          best_cost = nested_loop_cost;
          best_index = j;
          best_step_plan = std::move(nested_loop_plan);
        }
      }
    }

    // All elements would form a cross product, iterate again, allowing cross products
    if (best_cost == std::numeric_limits<double>::infinity()) {
      best_index = 0;

      for (size_t j = 0; j < base_plans_size; j++) {
        if (base_plans[j] == nullptr) {
          continue;
        }
        auto nested_loop_plan =
            std::make_unique<IndexNestedLoopPlan>(root_plan->clone(), base_plans[j]->clone());
        auto nested_loop_cost = nested_loop_plan->estimate_cost();

        if (nested_loop_cost <= best_cost) {
          best_cost = nested_loop_cost;
          best_index = j;
          best_step_plan = std::move(nested_loop_plan);
        }
      }
    }
    base_plans[best_index] = nullptr;
    root_plan = std::move(best_step_plan);
  }

  return root_plan;
}

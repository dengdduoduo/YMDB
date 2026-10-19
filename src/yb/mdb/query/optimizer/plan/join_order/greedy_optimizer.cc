#include "greedy_optimizer.h"

#include <limits>
#include <algorithm>
#include "query/optimizer/plan/join/hash_join_plan.h"
#include "yb/mdb/storage/docdb_read_context.h"

#include "query/optimizer/plan/join/index_nested_loop_plan.h"

std::unique_ptr<Plan> GreedyOptimizer::get_plan(const std::vector<std::unique_ptr<Plan>>& real_base_plans)
{
    assert(!real_base_plans.empty());
    const auto base_plans_size = real_base_plans.size();

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
        if (current_element_cost < best_cost) {
            best_cost = current_element_cost;
            best_index = j;
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
                auto nested_loop_plan = std::make_unique<IndexNestedLoopPlan>(
                    root_plan->clone(),
                    base_plans[j]->clone()
                );
                auto nested_loop_cost = nested_loop_plan->estimate_cost();

                auto* context = yb::mdb::DocDBReadContextScope::Current();
                bool use_hash = false;
                if (context && context->batch_options.policy != yb::mdb::JoinPolicy::Index &&
                    root_plan->hash_join_safe() && base_plans[j]->hash_join_safe()) {
                    const double left_rows = root_plan->estimate_output_size();
                    const double right_rows = base_plans[j]->estimate_output_size();
                    const double build_bytes = std::min(
                        left_rows * (root_plan->get_vars().size() + 3) * 8,
                        right_rows * (base_plans[j]->get_vars().size() + 3) * 8);
                    // Transparent initial rule: missing stats and small inputs keep
                    // indexed probes. Both large inputs may scan/hash if build fits.
                    use_hash = context->batch_options.policy == yb::mdb::JoinPolicy::Hash ||
                        (context->statistics && context->statistics->predicates_complete &&
                         context->statistics->subjects.Estimate() >= 0 &&
                         context->statistics->objects.Estimate() >= 0 &&
                         left_rows >= 64 && right_rows >= 64 &&
                         build_bytes < context->batch_options.memory_bytes / 2);
                }
                std::unique_ptr<Plan> candidate = std::move(nested_loop_plan);
                if (use_hash) candidate = std::make_unique<HashJoinPlan>(
                    root_plan->clone(), base_plans[j]->clone());
                const double cost = use_hash ? candidate->estimate_cost() : nested_loop_cost;
                if (cost < best_cost) {
                    best_cost = cost;
                    best_index = j;
                    best_step_plan = std::move(candidate);
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
                auto nested_loop_plan = std::make_unique<IndexNestedLoopPlan>(
                    root_plan->clone(),
                    base_plans[j]->clone()
                );
                auto nested_loop_cost = nested_loop_plan->estimate_cost();

                if (nested_loop_cost <= best_cost) {
                    best_cost = nested_loop_cost;
                    best_index = j;
                    best_step_plan = std::move(nested_loop_plan);
                }
            }
        }
        if (auto* context = yb::mdb::DocDBReadContextScope::Current()) {
            const bool hash = dynamic_cast<HashJoinPlan*>(best_step_plan.get()) != nullptr;
            if (hash) ++context->stats.optimizer_hash_choices;
            else ++context->stats.optimizer_index_choices;
            if (!context->stats.join_decisions.empty()) context->stats.join_decisions += ';';
            context->stats.join_decisions += hash
                ? (context->batch_options.policy == yb::mdb::JoinPolicy::Hash
                    ? "forced_hash_bound_bgp" : "auto_hash_both_inputs_ge_64_build_fits")
                : (context->batch_options.policy == yb::mdb::JoinPolicy::Index
                    ? "forced_index" : (!context->statistics ? "index_missing_statistics"
                       : "index_small_input_or_memory_or_unsupported_keys"));
            if (!context->statistics) ++context->stats.optimizer_missing_stats;
        }
        base_plans[best_index] = nullptr;
        root_plan = std::move(best_step_plan);
    }

    return root_plan;
}

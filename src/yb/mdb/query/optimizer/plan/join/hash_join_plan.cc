#include "hash_join_plan.h"

#include "query/exceptions.h"
#include <algorithm>
#include <iterator>
#include "query/executor/hash_join.h"
#include "yb/mdb/storage/docdb_read_context.h"

HashJoinPlan::HashJoinPlan(
    std::unique_ptr<Plan> _lhs,
    std::unique_ptr<Plan> _rhs
) :
    lhs (std::move(_lhs)),
    rhs (std::move(_rhs))
{
    const auto left_rows = lhs->estimate_output_size();
    const auto right_rows = rhs->estimate_output_size();
    estimated_output_size = std::max(left_rows, right_rows);
    estimated_cost = lhs->estimate_cost() + rhs->estimate_cost() + left_rows + right_rows;
}


void HashJoinPlan::print(std::ostream& os, int indent) const {
    for (int i = 0; i < indent; ++i) {
        os << ' ';
    }
    os << "HashJoin(bound BGP keys; smaller estimated bytes build side, estimated rows="
       << estimated_output_size << ", cost=" << estimated_cost << ",\n";
    lhs->print(os, indent + 2);
    os << ",\n";
    rhs->print(os, indent + 2);
    os << "\n";
    for (int i = 0; i < indent; ++i) {
        os << ' ';
    }
    os << ")";
}


std::set<VarId> HashJoinPlan::get_vars() const {
    auto result = lhs->get_vars();
    for (auto var : rhs->get_vars()) {
        result.insert(var);
    }
    return result;
}


void HashJoinPlan::set_input_vars(const std::set<VarId>& input_vars) {
    lhs->set_input_vars(input_vars);
    rhs->set_input_vars(input_vars);
}


std::unique_ptr<BindingIter> HashJoinPlan::get_binding_iter() const {
    auto* context = yb::mdb::DocDBReadContextScope::Current();
    if (!context || !hash_join_safe())
        throw NotSupportedException("Hash join requires streaming reads and bound BGP keys");
    const auto left_vars = lhs->get_vars(), right_vars = rhs->get_vars();
    std::vector<VarId> keys;
    std::set_intersection(left_vars.begin(), left_vars.end(), right_vars.begin(), right_vars.end(),
                          std::back_inserter(keys));
    if (keys.empty()) throw NotSupportedException("Hash join requires shared bound keys");
    const auto left_bytes = lhs->estimate_output_size() * (left_vars.size() + 3) * 8;
    const auto right_bytes = rhs->estimate_output_size() * (right_vars.size() + 3) * 8;
    const auto* build = left_bytes <= right_bytes ? lhs.get() : rhs.get();
    const auto* probe = left_bytes <= right_bytes ? rhs.get() : lhs.get();
    context->stats.hash_build_right_operators += left_bytes > right_bytes;
    context->stats.estimated_hash_build_rows += static_cast<uint64_t>(build->estimate_output_size());
    context->stats.estimated_hash_build_bytes += static_cast<uint64_t>(std::min(left_bytes, right_bytes));
    const auto vars = get_vars(), build_vars = build->get_vars();
    return std::make_unique<yb::mdb::BatchBindingIter>(
        std::make_unique<yb::mdb::HashJoin>(build->get_binding_iter(), probe->get_binding_iter(),
            std::move(keys), std::vector<VarId>(build_vars.begin(), build_vars.end()),
            context, context->batch_options),
        std::vector<VarId>(vars.begin(), vars.end()), "HashJoin");
}

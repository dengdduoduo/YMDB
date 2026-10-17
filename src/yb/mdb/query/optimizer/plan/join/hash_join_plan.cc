#include "hash_join_plan.h"

#include "query/exceptions.h"

HashJoinPlan::HashJoinPlan(
    std::unique_ptr<Plan> _lhs,
    std::unique_ptr<Plan> _rhs
) :
    lhs (std::move(_lhs)),
    rhs (std::move(_rhs))
{
    estimated_output_size = lhs->estimate_output_size() * rhs->estimate_output_size();
    estimated_cost = lhs->estimate_cost() + rhs->estimate_cost();
}


void HashJoinPlan::print(std::ostream& os, int indent) const {
    for (int i = 0; i < indent; ++i) {
        os << ' ';
    }
    os << "HashJoin(\n";
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


void HashJoinPlan::set_input_vars(const std::set<VarId>& /*input_vars*/) {
    throw LogicException("HashJoin only works for left deep plans.");
}


std::unique_ptr<BindingIter> HashJoinPlan::get_binding_iter() const {
    throw NotSupportedException("HashJoinPlan::get_binding_iter not yet implemented");
}

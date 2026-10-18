// Portions Copyright (c) YugabyteDB, Inc.

#include "yb/mdb/query/optimizer/plan/join/index_nested_loop_plan.h"

#include "query/exceptions.h"
#include "query/executor/binding_iter/index_nested_loop_join.h"
#include "query/optimizer/rdf_model/plan/triple_plan.h"

IndexNestedLoopPlan::IndexNestedLoopPlan(std::unique_ptr<Plan> _lhs, std::unique_ptr<Plan> _rhs)
    : lhs(std::move(_lhs)), rhs(std::move(_rhs)) {
  rhs->set_input_vars(lhs->get_vars());

  const auto* input = dynamic_cast<const SPARQL::TriplePlan*>(lhs.get());
  const auto* scan = dynamic_cast<const SPARQL::TriplePlan*>(rhs.get());
  double rhs_cost;
  if (input != nullptr && scan != nullptr &&
      scan->estimate_index_join(*input, &estimated_output_size, &rhs_cost)) {
    estimated_cost = lhs->estimate_cost() + rhs_cost;
  } else {
    const auto lhs_output_size = lhs->estimate_output_size();
    estimated_output_size = lhs_output_size * rhs->estimate_output_size();
    estimated_cost = lhs->estimate_cost() + rhs->estimate_cost_for_input(lhs_output_size);
  }
}

void IndexNestedLoopPlan::print(std::ostream& os, int indent) const {
    for (int i = 0; i < indent; ++i) {
        os << ' ';
    }
    os << "IndexNestedLoopJoin(\n";
    lhs->print(os, indent + 2);
    os << ",\n";
    rhs->print(os, indent + 2);
    os << "\n";
    for (int i = 0; i < indent; ++i) {
        os << ' ';
    }
    os << ")";
}

std::set<VarId> IndexNestedLoopPlan::get_vars() const {
    auto result = lhs->get_vars();
    for (auto var : rhs->get_vars()) {
        result.insert(var);
    }
    return result;
}


void IndexNestedLoopPlan::set_input_vars(const std::set<VarId>& /*input_vars*/) {
    throw LogicException("NestedLoop only works for left deep plans.");
}


std::unique_ptr<BindingIter> IndexNestedLoopPlan::get_binding_iter() const {
    return std::make_unique<IndexNestedLoopJoin>(
        lhs->get_binding_iter(),
        rhs->get_binding_iter()
    );
}

#pragma once

#include <memory>
#include <set>
#include <vector>

#include "query/id.h"
#include "query/optimizer/plan/plan.h"
#include "query/parser/paths/regular_path_expr.h"

namespace SPARQL {

class PathPlan : public ::Plan {
public:
    PathPlan(VarId var, Id start, Id end, std::unique_ptr<RegularPathExpr> path);

    std::unique_ptr<Plan> clone() const override;
    double estimate_cost() const override;
    double estimate_output_size() const override;
    std::set<VarId> get_vars() const override;
    void set_input_vars(const std::set<VarId>& input_vars) override;
    std::unique_ptr<::BindingIter> get_binding_iter() const override;
    bool get_leapfrog_iter(std::vector<std::unique_ptr<::LeapfrogIter>>& leapfrog_iters,
                           std::vector<VarId>&                         var_order,
                           uint_fast32_t&                              enumeration_level) const override;
    void print(std::ostream& os, int indent) const override;
    int relation_size() const override;

    VarId var;
    Id start;
    Id end;
    std::unique_ptr<RegularPathExpr> path;
    bool start_assigned;
    bool end_assigned;
    std::vector<VarId> vars;
};

} // namespace SPARQL

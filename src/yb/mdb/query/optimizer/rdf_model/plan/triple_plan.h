#pragma once

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "query/id.h"
#include "query/optimizer/plan/plan.h"

namespace SPARQL {
class TriplePlan : public ::Plan {

    enum class Index {
        NORMAL,
        EQUAL_SPO,
        EQUAL_SP,
        EQUAL_SO,
        EQUAL_PO,
    };

public:
    TriplePlan(Id subject, Id predicate, Id object);

    TriplePlan(const TriplePlan& other) :
        subject            (other.subject),
        predicate          (other.predicate),
        object             (other.object),
        subject_assigned   (other.subject_assigned),
        predicate_assigned (other.predicate_assigned),
        object_assigned    (other.object_assigned),
        index              (other.index),
        cached_output_estimation          (other.cached_output_estimation),
        cached_output_estimation_is_valid (other.cached_output_estimation_is_valid) { }

    std::unique_ptr<Plan> clone() const override {
        return std::make_unique<TriplePlan>(*this);
    }

    int relation_size() const override { return 3; }

    double estimate_cost() const override;
    double estimate_output_size() const override;

    std::set<VarId> get_vars() const override;
    bool hash_join_safe() const override { return true; }
    void set_input_vars(const std::set<VarId>& input_vars) override;

    std::unique_ptr<::BindingIter> get_binding_iter() const override;
    bool get_leapfrog_iter(std::vector<std::unique_ptr<::LeapfrogIter>>& leapfrog_iters,
                           std::vector<VarId>&                         var_order,
                           uint_fast32_t&                              enumeration_level) const override;

    void print(std::ostream& os, int indent) const override;

    Id subject;
    Id predicate;
    Id object;

    bool subject_assigned;
    bool predicate_assigned;
    bool object_assigned;

    Index index;

private:
    mutable double cached_output_estimation;

    mutable bool cached_output_estimation_is_valid = false;

    double _estimate_output_size() const;

    double estimate_with_docdb() const;
};
} // namespace SPARQL

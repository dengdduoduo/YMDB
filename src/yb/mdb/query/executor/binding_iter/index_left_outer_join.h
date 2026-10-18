#pragma once

#include <memory>
#include <vector>

#include "query/executor/binding_iter.h"
#include "query/executor/binding_iter/empty_binding_iter.h"

class IndexLeftOuterJoin : public BindingIter {
public:
    IndexLeftOuterJoin(
        std::unique_ptr<BindingIter> lhs,
        std::unique_ptr<BindingIter> rhs,
        std::vector<VarId> rhs_only_vars) :
        lhs           (std::move(lhs)),
        original_rhs  (std::move(rhs)),
        rhs_only_vars (std::move(rhs_only_vars)) { }

    void print(std::ostream& os, int indent, bool stats) const override;
    void _begin(Binding& parent_binding) override;
    bool _next() override;
    void _reset() override;
    void assign_nulls() override;

    std::unique_ptr<BindingIter> lhs;
    std::unique_ptr<BindingIter> original_rhs;
    std::vector<VarId> rhs_only_vars;

private:
    BindingIter* rhs;

    Binding* parent_binding;

    bool must_return_null;

    EmptyBindingIter empty_iter;
};

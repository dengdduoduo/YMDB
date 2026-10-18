#pragma once

#include <memory>
#include <vector>

#include "query/executor/binding_iter.h"

class SubSelect : public BindingIter {
public:
    SubSelect(
        std::unique_ptr<BindingIter> child_iter,
        std::vector<VarId> projection_vars,
        std::vector<VarId> safe_assigned_vars) :
        child_iter         (std::move(child_iter)),
        projection_vars    (std::move(projection_vars)),
        safe_assigned_vars (std::move(safe_assigned_vars)) { }

    void print(std::ostream& os, int indent, bool stats) const override;
    void _begin(Binding& parent_binding) override;
    bool _next() override;
    void _reset() override;
    void assign_nulls() override;

    std::unique_ptr<BindingIter> child_iter;
    std::vector<VarId> projection_vars;
    std::vector<VarId> safe_assigned_vars;

private:
    Binding* parent_binding;

    std::unique_ptr<Binding> child_binding;
};

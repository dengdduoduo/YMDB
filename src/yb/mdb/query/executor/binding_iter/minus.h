#pragma once

#include <memory>
#include <vector>

#include "query/executor/binding_iter.h"

class Minus : public BindingIter {
public:
    Minus(std::unique_ptr<BindingIter> lhs, std::unique_ptr<BindingIter> rhs,
          std::vector<VarId> common_vars, std::vector<VarId> parent_safe_vars,
          std::vector<VarId> output_vars)
        : lhs(std::move(lhs)), rhs(std::move(rhs)), common_vars(std::move(common_vars)),
          parent_safe_vars(std::move(parent_safe_vars)), output_vars(std::move(output_vars)) {}

    void print(std::ostream& os, int indent, bool stats) const override;
    void _begin(Binding& parent_binding) override;
    bool _next() override;
    void _reset() override;
    void assign_nulls() override;

    std::unique_ptr<BindingIter> lhs;
    std::unique_ptr<BindingIter> rhs;
    std::vector<VarId> common_vars;
    std::vector<VarId> parent_safe_vars;
    std::vector<VarId> output_vars;

private:
    Binding* parent_binding;

    std::unique_ptr<Binding> lhs_binding;
    std::unique_ptr<Binding> rhs_binding;
};

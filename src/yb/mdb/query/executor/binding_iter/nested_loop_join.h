#pragma once
#include <memory>
#include <vector>
#include "query/executor/binding_iter.h"
#include "query/executor/binding_iter/empty_binding_iter.h"

class NestedLoopJoin : public BindingIter {
public:
    NestedLoopJoin(std::unique_ptr<BindingIter> lhs, std::unique_ptr<BindingIter> rhs,
        std::vector<VarId> safe_join_vars, std::vector<VarId> unsafe_join_vars,
        std::vector<VarId> parent_safe_vars, std::vector<VarId> lhs_only_vars,
        std::vector<VarId> rhs_only_vars);

    void _begin(Binding& parent_binding) override;
    bool _next() override;
    void _reset() override;
    void assign_nulls() override;
    void print(std::ostream& os, int indent, bool stats) const override;

    std::unique_ptr<BindingIter> lhs;
    std::unique_ptr<BindingIter> original_rhs;
    std::vector<VarId> safe_join_vars, unsafe_join_vars, parent_safe_vars, lhs_only_vars, rhs_only_vars;

private:
    BindingIter* rhs;
    Binding* parent_binding;
    std::unique_ptr<Binding> lhs_binding;
    std::unique_ptr<Binding> rhs_binding;
    EmptyBindingIter empty_iter;
};

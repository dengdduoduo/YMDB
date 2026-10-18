#pragma once
#include <memory>
#include "query/executor/binding_iter.h"
class SetEndBoundaryVariable : public BindingIter {
public:
    SetEndBoundaryVariable(std::unique_ptr<BindingIter> child_iter, VarId var) : child_iter(std::move(child_iter)), var(var) { }
    void _begin(Binding& parent_binding) override { child_iter->begin(parent_binding); }
    bool _next() override { return child_iter->next(); }
    void _reset() override { child_iter->reset(); }
    void assign_nulls() override { child_iter->assign_nulls(); }
    void print(std::ostream& os, int indent, bool stats) const override { os << std::string(indent, ' ') << "SetEndBoundaryVariable\\n"; }
    std::unique_ptr<BindingIter> child_iter;
    VarId var;
};

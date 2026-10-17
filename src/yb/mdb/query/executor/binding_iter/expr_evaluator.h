#pragma once

#include <memory>
#include <vector>
#include <utility>

#include "query/executor/binding_iter.h"
#include "query/executor/binding_iter/binding_expr/binding_expr.h"

class ExprEvaluator : public BindingIter {
public:
    ExprEvaluator(
        std::unique_ptr<BindingIter> child_iter,
        std::vector<std::pair<VarId, std::unique_ptr<BindingExpr>>>&& exprs) :
        child_iter(std::move(child_iter)),
        exprs     (std::move(exprs)) { }

    void print(std::ostream& os, int indent, bool stats) const override;
    void _begin(Binding& parent_binding) override;
    bool _next() override;
    void _reset() override;
    void assign_nulls() override;

    std::unique_ptr<BindingIter> child_iter;
    std::vector<std::pair<VarId, std::unique_ptr<BindingExpr>>> exprs;

private:
    Binding* parent_binding;
};

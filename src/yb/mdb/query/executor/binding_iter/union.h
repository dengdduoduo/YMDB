#pragma once
#include <memory>
#include <vector>
#include "query/executor/binding_iter.h"
class Union : public BindingIter {
public:
    Union(std::vector<std::unique_ptr<BindingIter>>&& iters);

    void _begin(Binding& parent_binding) override;
    bool _next() override;
    void _reset() override;
    void assign_nulls() override;
    void print(std::ostream& os, int indent, bool stats) const override;

    std::vector<std::unique_ptr<BindingIter>> iters;

private:
    Binding* parent_binding_ = nullptr;
    size_t current_iter_ = 0;
    bool first_call_ = true;
};

#pragma once

#include <memory>
#include <set>
#include <vector>

#include "graph_models/object_id.h"
#include "query/executor/binding_iter.h"

class OrderByWithLimit : public BindingIter {
public:
    OrderByWithLimit(
        std::unique_ptr<BindingIter> child_iter,
        const std::set<VarId>& saved_vars,
        std::vector<VarId>&& order_vars,
        std::vector<bool>&& ascending,
        int64_t (*_compare)(ObjectId, ObjectId),
        uint64_t limit
    ) : child_iter(std::move(child_iter)) { }

    void _begin(Binding& parent_binding) override { child_iter->begin(parent_binding); }

    bool _next() override { return child_iter->next(); }

    void _reset() override { child_iter->reset(); }

    void assign_nulls() override { child_iter->assign_nulls(); }

    void print(std::ostream& os, int indent, bool stats) const override {
        os << std::string(indent, ' ') << "OrderByWithLimit\n";
    }

    std::unique_ptr<BindingIter> child_iter;
};

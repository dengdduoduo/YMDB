#pragma once

#include <memory>
#include <set>
#include <vector>

#include "graph_models/object_id.h"
#include "query/executor/binding_iter.h"
#include "query/var_id.h"

class DistinctHash : public BindingIter {
public:
    DistinctHash(std::unique_ptr<BindingIter> child_iter, std::vector<VarId>&& projected_vars) :
        child_iter(std::move(child_iter)), projected_vars(std::move(projected_vars)) { }

    void _begin(Binding& parent_binding) override
    {
        parent_binding_ = &parent_binding;
        child_iter->begin(parent_binding);
        seen_.clear();
        current_tuple_.resize(projected_vars.size());
    }

    bool _next() override
    {
        while (child_iter->next()) {
            for (size_t i = 0; i < projected_vars.size(); i++) {
                current_tuple_[i] = (*parent_binding_)[projected_vars[i]];
            }
            if (seen_.insert(current_tuple_).second) {
                return true;
            }
        }
        return false;
    }

    void _reset() override
    {
        child_iter->reset();
        seen_.clear();
    }

    void assign_nulls() override
    {
        child_iter->assign_nulls();
    }

    void print(std::ostream& os, int indent, bool stats) const override
    {
        if (stats) {
            print_generic_stats(os, indent);
        }
        os << std::string(indent, ' ') << "DistinctHash(";
        for (size_t i = 0; i < projected_vars.size(); i++) {
            if (i != 0) os << ", ";
            os << "?" << get_query_ctx().get_var_name(projected_vars[i]);
        }
        os << ")\n";
        child_iter->print(os, indent + 2, stats);
    }

    std::unique_ptr<BindingIter> child_iter;
    std::vector<VarId> projected_vars;

private:
    std::set<std::vector<ObjectId>> seen_;
    std::vector<ObjectId> current_tuple_;
    Binding* parent_binding_ = nullptr;
};

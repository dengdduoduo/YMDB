#pragma once
#include <memory>
#include <vector>
#include "query/executor/binding_iter.h"
#include "query/executor/binding_iter/binding_expr/binding_expr.h"

class Filter : public BindingIter {
public:
    Filter(ObjectId (*_to_boolean)(ObjectId), std::unique_ptr<BindingIter> child, std::vector<std::unique_ptr<BindingExpr>>&& filters) :
        child_iter(std::move(child)), to_boolean(_to_boolean), filters(std::move(filters)) { }

    void _begin(Binding& parent_binding) override {
        parent_binding_ = &parent_binding;
        child_iter->begin(parent_binding);
    }

    bool _next() override {
        while (child_iter->next()) {
            bool passes = true;
            for (auto& filter : filters) {
                auto result = filter->eval(*parent_binding_);
                if (to_boolean(result) == ObjectId(ObjectId::BOOL_FALSE)) {
                    passes = false;
                    break;
                }
                if (result.is_null()) {
                    passes = false;
                    break;
                }
            }
            if (passes) return true;
        }
        return false;
    }

    void _reset() override { child_iter->reset(); }
    void assign_nulls() override { child_iter->assign_nulls(); }
    void print(std::ostream& os, int indent, bool stats) const override {
        os << std::string(indent, ' ') << "Filter\\n";
        child_iter->print(os, indent + 2, stats);
    }
    std::unique_ptr<BindingIter> child_iter;
    Binding* parent_binding_;
    ObjectId (*to_boolean)(ObjectId);
    std::vector<std::unique_ptr<BindingExpr>> filters;
};

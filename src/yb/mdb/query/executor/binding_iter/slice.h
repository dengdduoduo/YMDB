#pragma once
#include <memory>
#include <cstdint>
#include "query/executor/binding_iter.h"
class Slice : public BindingIter {
public:
    Slice(std::unique_ptr<BindingIter> child_iter, uint64_t offset, uint64_t limit) :
        child_iter(std::move(child_iter)), offset(offset), limit(limit) { }

    void _begin(Binding& _parent_binding) override {
        parent_binding = &_parent_binding;
        child_iter->begin(_parent_binding);
        count = 0;
        position = 0;
    }

    bool _next() override {
        while (position < offset) {
            if (child_iter->next()) {
                position++;
            } else {
                return false;
            }
        }

        if (count < limit && child_iter->next()) {
            count++;
            return true;
        }
        return false;
    }

    void _reset() override {
        child_iter->reset();
        position = 0;
        count = 0;
    }

    void assign_nulls() override { child_iter->assign_nulls(); }

    void print(std::ostream& os, int indent, bool stats) const override {
        os << std::string(indent, ' ') << "Slice(offset=" << offset << " limit=" << limit << ")\\n";
        child_iter->print(os, indent + 2, stats);
    }

    std::unique_ptr<BindingIter> child_iter;
    uint64_t offset, limit;

private:
    Binding* parent_binding;
    uint64_t count = 0;
    uint64_t position = 0;
};

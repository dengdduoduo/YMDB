#include "union.h"

#include "query/executor/binding.h"

Union::Union(std::vector<std::unique_ptr<BindingIter>>&& iters)
    : iters(std::move(iters)) {}

void Union::_begin(Binding& parent_binding) {
    parent_binding_ = &parent_binding;
    current_iter_ = 0;
    first_call_ = true;

    if (!iters.empty()) {
        iters[0]->begin(parent_binding);
    }
}

bool Union::_next() {
    if (iters.empty()) {
        return false;
    }

    while (current_iter_ < iters.size()) {
        if (first_call_) {
            first_call_ = false;
        }

        if (iters[current_iter_]->next()) {
            return true;
        }

        current_iter_++;
        if (current_iter_ < iters.size()) {
            for (std::size_t i = 0; i < parent_binding_->size; i++) {
                parent_binding_->add(VarId(i), ObjectId::get_null());
            }
            iters[current_iter_]->begin(*parent_binding_);
            first_call_ = true;
        }
    }

    return false;
}

void Union::_reset() {
    current_iter_ = 0;
    first_call_ = true;

    if (!iters.empty()) {
        iters[0]->reset();
    }
}

void Union::assign_nulls() {
    for (auto& iter : iters) {
        iter->assign_nulls();
    }
}

void Union::print(std::ostream& os, int indent, bool stats) const {
    if (stats) {
        print_generic_stats(os, indent);
    }
    os << std::string(indent, ' ') << "Union\n";
    for (auto& iter : iters) {
        iter->print(os, indent + 2, stats);
    }
}

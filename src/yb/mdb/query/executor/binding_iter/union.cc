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
        set_status_from(*iters[0]);
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
            set_status_from(*iters[current_iter_]);
            return true;
        }
        set_status_from(*iters[current_iter_]);

        current_iter_++;
        if (current_iter_ < iters.size()) {
            for (std::size_t i = 0; i < parent_binding_->size; i++) {
                parent_binding_->add(VarId(i), ObjectId::get_null());
            }
            iters[current_iter_]->begin(*parent_binding_);
            set_status_from(*iters[current_iter_]);
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
        set_status_from(*iters[0]);
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

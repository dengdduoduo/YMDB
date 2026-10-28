#include "minus.h"
#include <algorithm>

void Minus::_begin(Binding& binding) {
    parent_binding = &binding;
    lhs_binding = std::make_unique<Binding>(binding.size);
    rhs_binding = std::make_unique<Binding>(binding.size);
    lhs_binding->add_all(binding);
    for (auto var : parent_safe_vars) rhs_binding->add(var, binding[var]);
    lhs->begin(*lhs_binding);
    rhs->begin(*rhs_binding);
}

bool Minus::_next() {
    while (lhs->next()) {
        bool excluded = false;
        if (!common_vars.empty()) {
            for (size_t i = 0; i < rhs_binding->size; ++i)
                rhs_binding->add(VarId(i), ObjectId::get_null());
            for (auto var : parent_safe_vars)
                rhs_binding->add(var, (*parent_binding)[var]);
            rhs->reset();
            while (rhs->next()) {
                bool overlap = false, compatible = true;
                for (auto var : common_vars) {
                    const auto left = (*lhs_binding)[var], right = (*rhs_binding)[var];
                    if (left.is_null() || right.is_null()) continue;
                    overlap = true;
                    if (left != right) { compatible = false; break; }
                }
                // MINUS preserves mappings with disjoint bound domains.
                if (overlap && compatible) { excluded = true; break; }
            }
        }
        if (!excluded) { parent_binding->add_all(*lhs_binding); return true; }
    }
    return false;
}

void Minus::_reset() {
    for (size_t i = 0; i < lhs_binding->size; ++i)
        lhs_binding->add(VarId(i), ObjectId::get_null());
    for (auto var : parent_safe_vars) lhs_binding->add(var, (*parent_binding)[var]);
    lhs->reset();
}

void Minus::assign_nulls() {
    lhs->assign_nulls();
    for (const auto var : output_vars) {
        if (std::find(parent_safe_vars.begin(), parent_safe_vars.end(), var) == parent_safe_vars.end())
            parent_binding->add(var, ObjectId::get_null());
    }
}

void Minus::print(std::ostream& os, int indent, bool stats) const {
    if (stats) print_generic_stats(os, indent);
    os << std::string(indent, ' ') << "Minus()\n";
    lhs->print(os, indent + 2, stats);
    rhs->print(os, indent + 2, stats);
}

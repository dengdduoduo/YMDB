#include "minus.h"

using namespace std;

void Minus::_begin(Binding& _parent_binding)
{
    parent_binding = &_parent_binding;
    lhs_binding = std::make_unique<Binding>(_parent_binding.size);
    rhs_binding = std::make_unique<Binding>(_parent_binding.size);

    for (const auto& var : parent_safe_vars) {
        lhs_binding->add(var, (*parent_binding)[var]);
        rhs_binding->add(var, (*parent_binding)[var]);
    }

    lhs->begin(*lhs_binding);
    rhs->begin(*rhs_binding);
}

bool Minus::_next()
{
    while (lhs->next()) {
        rhs_binding->add_all(*lhs_binding);

        for (const auto& var : safe_join_vars) {
            rhs_binding->add(var, (*lhs_binding)[var]);
        }

        rhs->reset();

        bool match_found = false;
        while (rhs->next()) {
            bool match = true;
            for (auto& var : safe_join_vars) {
                auto rhs_oid = (*rhs_binding)[var];
                if (rhs_oid.is_null()) {
                    continue;
                }

                auto lhs_oid = (*lhs_binding)[var];
                if (lhs_oid.is_null()) {
                    continue;
                }

                if (lhs_oid != rhs_oid) {
                    match = false;
                    break;
                }
            }

            if (match) {
                for (auto& var : unsafe_join_vars) {
                    auto rhs_oid = (*rhs_binding)[var];
                    if (rhs_oid.is_null()) {
                        continue;
                    }

                    auto lhs_oid = (*lhs_binding)[var];
                    if (lhs_oid.is_null()) {
                        continue;
                    }

                    if (lhs_oid != rhs_oid) {
                        match = false;
                        break;
                    }
                }
            }

            if (match) {
                match_found = true;
                break;
            }
        }

        if (!match_found) {
            for (auto& var : parent_safe_vars) {
                parent_binding->add(var, (*lhs_binding)[var]);
            }

            for (auto& var : safe_join_vars) {
                parent_binding->add(var, (*lhs_binding)[var]);
            }

            for (auto& var : unsafe_join_vars) {
                parent_binding->add(var, (*lhs_binding)[var]);
            }

            return true;
        }
    }
    return false;
}

void Minus::_reset()
{
    for (const auto& var : parent_safe_vars) {
        lhs_binding->add(var, (*parent_binding)[var]);
        rhs_binding->add(var, (*parent_binding)[var]);
    }

    lhs->reset();
    rhs->reset();
}

void Minus::assign_nulls()
{
    lhs->assign_nulls();
    rhs->assign_nulls();
}

void Minus::print(std::ostream& os, int indent, bool stats) const
{
    if (stats) {
        print_generic_stats(os, indent);
    }
    os << std::string(indent, ' ') << "Minus(";

    os << "safe_join: [";
    for (auto& var : safe_join_vars) {
        os << ' ' << var;
    }

    os << " ] unsafe_join: [";
    for (auto& var : unsafe_join_vars) {
        os << ' ' << var;
    }

    os << " ] parent_safe: [";
    for (auto& var : parent_safe_vars) {
        os << ' ' << var;
    }
    os << " ])\n";

    lhs->print(os, indent + 2, stats);
    rhs->print(os, indent + 2, stats);
}

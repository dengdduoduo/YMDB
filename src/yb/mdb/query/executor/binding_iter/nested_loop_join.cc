#include "nested_loop_join.h"

#include <memory>
#include <vector>

#include "query/executor/binding.h"
#include "query/var_id.h"

NestedLoopJoin::NestedLoopJoin(
    std::unique_ptr<BindingIter> lhs,
    std::unique_ptr<BindingIter> rhs,
    std::vector<VarId> safe_join_vars,
    std::vector<VarId> unsafe_join_vars,
    std::vector<VarId> parent_safe_vars,
    std::vector<VarId> lhs_only_vars,
    std::vector<VarId> rhs_only_vars)
    : lhs(std::move(lhs)),
      original_rhs(std::move(rhs)),
      safe_join_vars(std::move(safe_join_vars)),
      unsafe_join_vars(std::move(unsafe_join_vars)),
      parent_safe_vars(std::move(parent_safe_vars)),
      lhs_only_vars(std::move(lhs_only_vars)),
      rhs_only_vars(std::move(rhs_only_vars)) {}

void NestedLoopJoin::_begin(Binding& _parent_binding)
{
    this->parent_binding = &_parent_binding;

    lhs_binding = std::make_unique<Binding>(_parent_binding.size);
    rhs_binding = std::make_unique<Binding>(_parent_binding.size);

    for (const auto& var : parent_safe_vars) {
        rhs_binding->add(var, _parent_binding[var]);
    }
    lhs_binding->add_all(_parent_binding);

    lhs->begin(*lhs_binding);
    if (lhs->next()) {
        for (const auto& var : safe_join_vars) {
            rhs_binding->add(var, (*lhs_binding)[var]);
        }
        rhs = original_rhs.get();
    } else {
        rhs = &empty_iter;
    }
    original_rhs->begin(*rhs_binding);
}

bool NestedLoopJoin::_next()
{
    while (true) {
        if (rhs->next()) {
            bool match = true;
            for (auto& var : unsafe_join_vars) {
                auto lhs_oid = (*lhs_binding)[var];
                if (lhs_oid.is_null()) {
                    continue;
                }

                auto rhs_oid = (*rhs_binding)[var];
                if (rhs_oid.is_null()) {
                    continue;
                }

                if (lhs_oid != rhs_oid) {
                    match = false;
                    break;
                }
            }

            if (match) {
                for (auto& var : lhs_only_vars) {
                    parent_binding->add(var, (*lhs_binding)[var]);
                }

                for (auto& var : rhs_only_vars) {
                    parent_binding->add(var, (*rhs_binding)[var]);
                }

                for (auto& var : safe_join_vars) {
                    parent_binding->add(var, (*rhs_binding)[var]);
                }

                for (auto& var : unsafe_join_vars) {
                    auto var_oid = (*lhs_binding)[var];
                    if (var_oid.is_null()) {
                        var_oid = (*rhs_binding)[var];
                    }
                    parent_binding->add(var, var_oid);
                }
                return true;
            }
        } else {
            if (lhs->next()) {
                for (const auto& var : safe_join_vars) {
                    rhs_binding->add(var, (*lhs_binding)[var]);
                }
                rhs->reset();
            } else
                return false;
        }
    }
}

void NestedLoopJoin::_reset()
{
    for (const auto& var : parent_safe_vars) {
        rhs_binding->add(var, (*parent_binding)[var]);
    }
    lhs_binding->add_all(*parent_binding);

    lhs->reset();
    if (lhs->next()) {
        for (const auto& var : safe_join_vars) {
            rhs_binding->add(var, (*lhs_binding)[var]);
        }
        rhs = original_rhs.get();
        rhs->reset();
    } else {
        rhs = &empty_iter;
    }
}

void NestedLoopJoin::assign_nulls()
{
    lhs->assign_nulls();
    original_rhs->assign_nulls();
}

void NestedLoopJoin::print(std::ostream& os, int indent, bool stats) const
{
    if (stats) {
        print_generic_stats(os, indent);
    }
    os << std::string(indent, ' ') << "NestedLoopJoin(";

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

    os << " ] lhs_only: [";
    for (auto& var : lhs_only_vars) {
        os << ' ' << var;
    }

    os << " ] rhs_only: [";
    for (auto& var : rhs_only_vars) {
        os << ' ' << var;
    }
    os << " ])\n";

    lhs->print(os, indent + 2, stats);
    original_rhs->print(os, indent + 2, stats);
}

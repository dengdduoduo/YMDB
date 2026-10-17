#include "no_free_variable_minus.h"

void NoFreeVariableMinus::_begin(Binding& _parent_binding)
{
    parent_binding = &_parent_binding;

    lhs->begin(_parent_binding);
    rhs->begin(_parent_binding);

    if (rhs->next()) {
        rhs_has_next = true;
    } else {
        rhs_has_next = false;
    }
}

bool NoFreeVariableMinus::_next()
{
    if (rhs_has_next) {
        return false;
    }
    return lhs->next();
}

void NoFreeVariableMinus::_reset()
{
    rhs->reset();

    if (rhs->next()) {
        rhs_has_next = true;
    } else {
        rhs_has_next = false;
    }
    lhs->reset();
}

void NoFreeVariableMinus::assign_nulls()
{
    lhs->assign_nulls();
    rhs->assign_nulls();
}

void NoFreeVariableMinus::print(std::ostream& os, int indent, bool stats) const
{
    if (stats) {
        print_generic_stats(os, indent);
    }
    os << std::string(indent, ' ') << "NoFreeVariableMinus()\n";
    lhs->print(os, indent + 2, stats);
    rhs->print(os, indent + 2, stats);
}

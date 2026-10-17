#include "cross_product.h"

void CrossProduct::_begin(Binding& _parent_binding)
{
    parent_binding = &_parent_binding;
    lhs->begin(_parent_binding);
    if (lhs->next()) {
        rhs->begin(_parent_binding);
    }
}

bool CrossProduct::_next()
{
    while (true) {
        if (rhs->next()) {
            return true;
        } else {
            if (lhs->next()) {
                rhs->reset();
            } else {
                return false;
            }
        }
    }
}

void CrossProduct::_reset()
{
    lhs->reset();
    if (lhs->next()) {
        rhs->reset();
    }
}

void CrossProduct::assign_nulls()
{
    lhs->assign_nulls();
    rhs->assign_nulls();
}

void CrossProduct::print(std::ostream& os, int indent, bool stats) const
{
    if (stats) {
        print_generic_stats(os, indent);
    }
    os << std::string(indent, ' ') << "CrossProduct()\n";
    lhs->print(os, indent + 2, stats);
    rhs->print(os, indent + 2, stats);
}

#include "index_nested_loop_join.h"

void IndexNestedLoopJoin::_begin(Binding& parent_binding)
{
    this->parent_binding = &parent_binding;

    lhs->begin(parent_binding);
    set_status_from(*lhs);
    if (lhs->next()) {
        set_status_from(*lhs);
        rhs = original_rhs.get();
    } else {
        set_status_from(*lhs);
        rhs = &empty_iter;
    }
    original_rhs->begin(parent_binding);
    set_status_from(*original_rhs);
}

bool IndexNestedLoopJoin::_next()
{
    while (true) {
        if (rhs->next()) {
            set_status_from(*rhs);
            return true;
        } else {
            set_status_from(*rhs);
            if (lhs->next()) {
                set_status_from(*lhs);
                rhs->reset();
                set_status_from(*rhs);
            } else {
                set_status_from(*lhs);
                return false;
            }
        }
    }
}

void IndexNestedLoopJoin::_reset()
{
    lhs->reset();
    set_status_from(*lhs);
    if (lhs->next()) {
        set_status_from(*lhs);
        rhs = original_rhs.get();
        rhs->reset();
        set_status_from(*rhs);
    } else {
        set_status_from(*lhs);
        rhs = &empty_iter;
    }
}

void IndexNestedLoopJoin::assign_nulls()
{
    lhs->assign_nulls();
    original_rhs->assign_nulls();
}

void IndexNestedLoopJoin::print(std::ostream& os, int indent, bool stats) const
{
    if (stats) {
        print_generic_stats(os, indent);
    }
    os << std::string(indent, ' ') << "IndexNestedLoopJoin()\n";
    lhs->print(os, indent + 2, stats);
    original_rhs->print(os, indent + 2, stats);
}

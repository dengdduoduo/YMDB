#include "index_left_outer_join.h"

using namespace std;

void IndexLeftOuterJoin::_begin(Binding& _parent_binding)
{
    parent_binding = &_parent_binding;

    lhs->begin(_parent_binding);
    if (lhs->next()) {
        must_return_null = true;
        rhs = original_rhs.get();
    } else {
        must_return_null = false;
        rhs = &empty_iter;
    }
    original_rhs->begin(_parent_binding);
}

bool IndexLeftOuterJoin::_next()
{
    while (true) {
        if (rhs->next()) {
            must_return_null = false;
            return true;
        } else {
            if (must_return_null) {
                for (auto& var : rhs_only_vars) {
                    parent_binding->add(var, ObjectId::get_null());
                }
                must_return_null = false;
                return true;
            } else {
                if (lhs->next()) {
                    must_return_null = true;
                    rhs->reset();
                } else {
                    return false;
                }
            }
        }
    }
}

void IndexLeftOuterJoin::_reset()
{
    lhs->reset();
    if (lhs->next()) {
        must_return_null = true;
        rhs = original_rhs.get();
        rhs->reset();
    } else {
        must_return_null = false;
        rhs = &empty_iter;
    }
}

void IndexLeftOuterJoin::assign_nulls()
{
    lhs->assign_nulls();
    original_rhs->assign_nulls();
}

void IndexLeftOuterJoin::print(std::ostream& os, int indent, bool stats) const
{
    if (stats) {
        print_generic_stats(os, indent);
    }
    os << std::string(indent, ' ') << "IndexLeftOuterJoin()\n";
    lhs->print(os, indent + 2, stats);
    original_rhs->print(os, indent + 2, stats);
}

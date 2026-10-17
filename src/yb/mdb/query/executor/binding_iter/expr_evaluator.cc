#include "expr_evaluator.h"

void ExprEvaluator::_begin(Binding& _parent_binding)
{
    parent_binding = &_parent_binding;
    child_iter->begin(_parent_binding);
}

bool ExprEvaluator::_next()
{
    if (child_iter->next()) {
        for (auto&& [var, expr] : exprs) {
            parent_binding->add(var, expr->eval(*parent_binding));
        }
        return true;
    }
    return false;
}

void ExprEvaluator::_reset()
{
    child_iter->reset();
}

void ExprEvaluator::assign_nulls()
{
    child_iter->assign_nulls();
}

void ExprEvaluator::print(std::ostream& os, int indent, bool stats) const
{
    if (stats) {
        print_generic_stats(os, indent);
    }
    os << std::string(indent, ' ') << "ExprEvaluator(";
    bool first = true;
    for (auto&& [var, _] : exprs) {
        if (!first) os << ", ";
        first = false;
        os << var;
    }
    os << ")\n";
    child_iter->print(os, indent + 2, stats);
}

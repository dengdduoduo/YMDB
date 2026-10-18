#include "expr_evaluator.h"

#include "graph_models/rdf_model/conversions.h"

void ExprEvaluator::_begin(Binding& _parent_binding)
{
    parent_binding = &_parent_binding;
    child_iter->begin(_parent_binding);
    set_status_from(*child_iter);
}

bool ExprEvaluator::_next()
{
    if (!status().ok()) return false;
    if (child_iter->next()) {
        set_status_from(*child_iter);
        if (!status().ok()) return false;
        for (auto&& [var, expr] : exprs) {
            parent_binding->add(
                var, SPARQL::Conversions::term_identity(expr->eval(*parent_binding)));
        }
        return true;
    }
    set_status_from(*child_iter);
    return false;
}

void ExprEvaluator::_reset()
{
    child_iter->reset();
    set_status_from(*child_iter);
}

void ExprEvaluator::assign_nulls()
{
    child_iter->assign_nulls();
    for (const auto& [var, expr] : exprs) {
        parent_binding->add(var, ObjectId::get_null());
    }
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

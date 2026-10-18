#pragma once

#include <memory>
#include <vector>

#include "query/executor/binding_iter/binding_expr/sparql/binding_expr_equals.h"
#include "graph_models/rdf_model/conversions.h"
#include "query/executor/binding_iter/binding_expr/binding_expr.h"

namespace SPARQL {
class BindingExprIn : public BindingExpr {
public:
    std::unique_ptr<BindingExpr> lhs_expr;

    std::vector<std::unique_ptr<BindingExpr>> exprs;

    BindingExprIn(std::unique_ptr<BindingExpr> lhs_expr, std::vector<std::unique_ptr<BindingExpr>> exprs) :
        lhs_expr(std::move(lhs_expr)),
        exprs(std::move(exprs))
    { }

    ObjectId eval(const Binding& binding) override
    {
        bool error = false;

        auto lhs_oid = lhs_expr->eval(binding);

        if (lhs_oid.is_null()) {
            return lhs_oid;
        }

        for (auto& expr : exprs) {
            const auto equal = BindingExprEquals::eval_values(lhs_oid, expr->eval(binding));
            if (equal.is_null()) {
                error = true;
            } else if (equal.is_true()) {
                return Conversions::pack_bool(true);
            }
        }
        if (error) {
            return ObjectId::get_null();
        } else {
            return Conversions::pack_bool(false);
        }
    }

    void accept_visitor(BindingExprVisitor& visitor) override
    {
        visitor.visit(*this);
    }

    void print(std::ostream& os, std::vector<BindingIter*>& ops) const override
    {
        os << '(';
        lhs_expr->print(os, ops);
        os << " IN (";
        auto first = true;
        for (auto& e : exprs) {
            if (first)
                first = false;
            else
                os << ", ";
            e->print(os, ops);
        }
        os << "))";
    }
};
} // namespace SPARQL

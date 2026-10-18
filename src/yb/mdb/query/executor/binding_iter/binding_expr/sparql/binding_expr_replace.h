#pragma once

#include <memory>
#include <string>

#include "graph_models/rdf_model/conversions.h"
#include "query/executor/binding_iter/binding_expr/binding_expr.h"

namespace SPARQL {
class BindingExprReplace : public BindingExpr {
public:
    std::unique_ptr<BindingExpr> expr1;
    std::unique_ptr<BindingExpr> expr2;
    std::unique_ptr<BindingExpr> expr3;
    std::unique_ptr<BindingExpr> expr4;

public:
    BindingExprReplace(
        std::unique_ptr<BindingExpr> expr1,
        std::unique_ptr<BindingExpr> expr2,
        std::unique_ptr<BindingExpr> expr3,
        std::unique_ptr<BindingExpr> expr4
    ) :
        expr1(std::move(expr1)),
        expr2(std::move(expr2)),
        expr3(std::move(expr3)),
        expr4(std::move(expr4))
    { }

    static void string_replace(std::string& str, const std::string& from, const std::string& to) {
        if (from.empty()) {
            return;
        }
        size_t start_pos = 0;
        while ((start_pos = str.find(from, start_pos)) != std::string::npos) {
            str.replace(start_pos, from.length(), to);
            start_pos += to.length();
        }
    }

    ObjectId eval(const Binding& binding) override
    {
        auto expr1_oid = expr1->eval(binding);
        auto expr2_oid = expr2->eval(binding);
        auto expr3_oid = expr3->eval(binding);

        ObjectId expr4_oid;
        if (expr4) {
            expr4_oid = expr4->eval(binding);
        }

        if (RDF_OID::get_generic_sub_type(expr2_oid) != RDF_OID::GenericSubType::STRING_SIMPLE
            || RDF_OID::get_generic_sub_type(expr3_oid) != RDF_OID::GenericSubType::STRING_SIMPLE)
        {
            return ObjectId::get_null();
        }
        if (expr4 != nullptr
            && RDF_OID::get_generic_sub_type(expr4_oid) != RDF_OID::GenericSubType::STRING_SIMPLE)
        {
            return ObjectId::get_null();
        }

        auto expr2_str = Conversions::unpack_string(expr2_oid);
        auto expr3_str = Conversions::unpack_string(expr3_oid);

        switch (RDF_OID::get_generic_sub_type(expr1_oid)) {
        case RDF_OID::GenericSubType::STRING_SIMPLE:
        case RDF_OID::GenericSubType::STRING_XSD: {
            auto result = Conversions::unpack_string(expr1_oid);
            string_replace(result, expr2_str, expr3_str);
            return Conversions::pack_string_simple(result);
        }
        case RDF_OID::GenericSubType::STRING_LANG: {
            auto&& [lang, result] = Conversions::unpack_string_lang(expr1_oid);
            string_replace(result, expr2_str, expr3_str);
            return Conversions::pack_string_lang(lang, result);
        }
        default:
            return ObjectId::get_null();
        }
    }

    void accept_visitor(BindingExprVisitor& visitor) override
    {
        visitor.visit(*this);
    }

    void print(std::ostream& os, std::vector<BindingIter*>& ops) const override
    {
        os << "REPLACE(";
        expr1->print(os, ops);
        os << ", ";
        expr2->print(os, ops);
        os << ", ";
        expr3->print(os, ops);
        if (expr4 != nullptr) {
            os << ", ";
            expr4->print(os, ops);
        }
        os << ')';
    }
};
} // namespace SPARQL

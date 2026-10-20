#pragma once

#include <string>
#include <set>
#include <vector>
#include <limits>
#include <iomanip>
#include <sstream>
#include <boost/multiprecision/cpp_dec_float.hpp>

#include "graph_models/rdf_model/comparisons.h"
#include "graph_models/rdf_model/conversions.h"
#include "query/executor/binding_iter/aggregation/agg.h"
#include "query/executor/binding_iter/binding_expr/binding_expr_printer.h"
#include "query/var_id.h"

namespace SPARQL {

// Bounded RDF numeric accumulator. Integer totals are exact while admitted
// row counts fit uint64_t (at most 38 decimal digits). Decimal results use the
// model's finite precision: 18 significant digits, exponent -100..100.
struct NumericAggregateState {
    // 256 digits cover the full admitted decimal exponent span plus all
    // uint64_t input multiplicities, including cancellation across that span.
    using Wide = boost::multiprecision::number<boost::multiprecision::cpp_dec_float<256>>;
    Conversions::OpType type = Conversions::OpType::INTEGER;
    Wide exact = 0;
    float single = 0;
    double real = 0;
    uint64_t count = 0;
    bool error = false;

    void add(ObjectId value) {
        if (!value.is_null()) ++count;
        if (error) return;
        const auto next = Conversions::calculate_optype(value);
        if (value.is_null() || next > Conversions::OpType::DOUBLE) {
            error = true;
            return;
        }
        if (next > type) {
            if (next == Conversions::OpType::FLOAT) single = exact.convert_to<float>();
            if (next == Conversions::OpType::DOUBLE)
                real = type == Conversions::OpType::FLOAT ? single : exact.convert_to<double>();
            type = next;
        }
        if (type == Conversions::OpType::DOUBLE) real += Conversions::to_double(value);
        else if (type == Conversions::OpType::FLOAT) single += Conversions::to_float(value);
        else if (next == Conversions::OpType::INTEGER) exact += Conversions::unpack_int(value);
        else {
            auto term = RdfTermRegistry::Find(value.id);
            if (!term || term->lexical.size() > 256) { error = true; return; }
            std::string digits;
            for (char c : term->lexical) if (c >= '0' && c <= '9') digits += c;
            auto first = digits.find_first_not_of('0');
            auto last = digits.find_last_not_of('0');
            if (first != std::string::npos && last - first + 1 > 18) { error = true; return; }
            Wide number(term->lexical);
            const auto magnitude = boost::multiprecision::abs(number);
            if (magnitude != 0 && (magnitude < Wide("1e-100") || magnitude > Wide("1e100"))) {
                error = true; return;
            }
            exact += number;
        }
    }

    static ObjectId decimal(const Wide& number) {
        std::ostringstream out;
        out << std::scientific << std::setprecision(17) << number;
        std::string text = out.str();
        const auto e = text.find('e');
        if (e == std::string::npos) return ObjectId::get_null();
        int exponent = std::stoi(text.substr(e + 1));
        if (exponent < -100 || exponent > 100) return ObjectId::get_null();
        bool negative = text.front() == '-';
        std::string digits = text.substr(negative ? 1 : 0, e - (negative ? 1 : 0));
        digits.erase(digits.find('.'), 1);
        const int point = exponent + 1;
        if (point <= 0) text = "0." + std::string(-point, '0') + digits;
        else if (point >= int(digits.size())) text = digits + std::string(point - digits.size(), '0') + ".0";
        else text = digits.substr(0, point) + "." + digits.substr(point);
        while (text.back() == '0' && text[text.size() - 2] != '.') text.pop_back();
        if (negative && number != 0) text = "-" + text;
        return Conversions::try_pack_string_datatype(std::string(RdfTermIdentity::kXsd) + "decimal", text);
    }

    ObjectId get(bool average) const {
        if (average && count == 0) return Conversions::pack_int(0);
        if (error) return ObjectId::get_null();
        if (type == Conversions::OpType::FLOAT)
            return Conversions::pack_float(average ? single / count : single);
        if (type == Conversions::OpType::DOUBLE)
            return Conversions::pack_double(average ? real / count : real);
        if (average) return decimal(exact / Wide(count));
        if (type == Conversions::OpType::DECIMAL) return decimal(exact);
        if (exact < Wide(std::numeric_limits<int64_t>::min()) ||
            exact > Wide(std::numeric_limits<int64_t>::max())) return ObjectId::get_null();
        return Conversions::pack_int(exact.convert_to<int64_t>());
    }
};

template<bool Average, bool Distinct>
class AggNumeric : public Agg {
public:
    using Agg::Agg;
    void begin() override { state = NumericAggregateState(); seen.clear(); }
    void process() override {
        const auto value = expr->eval(*binding);
        if (!Distinct || seen.insert(value).second) state.add(value);
    }
    ObjectId get() override { return state.get(Average); }
    std::ostream& print(std::ostream& os) const override {
        os << (Average ? "AVG(" : "SUM(") << (Distinct ? "DISTINCT " : "");
        BindingExprPrinter printer(os); printer.print(*expr);
        return os << ")";
    }
private:
    NumericAggregateState state;
    std::set<ObjectId> seen;
};
using AggAvg = AggNumeric<true, false>;
using AggSum = AggNumeric<false, false>;
using AggAvgDistinct = AggNumeric<true, true>;
using AggSumDistinct = AggNumeric<false, true>;

class AggCount : public Agg {
public:
    using Agg::Agg;
    void begin() override
    {
        count = 0;
    }

    void process() override
    {
        auto oid = expr->eval(*binding);
        if (!oid.is_null()) {
            count++;
        }
    }

    ObjectId get() override
    {
        return Conversions::pack_int(count);
    }

    std::ostream& print(std::ostream& os) const override
    {
        os << "COUNT(";
        BindingExprPrinter printer(os);
        printer.print(*expr);
        os << ")";
        return os;
    }

private:
    uint64_t count = 0;
};

class AggCountAll : public Agg {
public:
    AggCountAll(VarId var_id, std::unique_ptr<BindingExpr> expr) : Agg(var_id, std::move(expr)) { }
    void begin() override {
        count = 0;
    }

    void process() override {
        count++;
    }

    ObjectId get() override {
        return Conversions::pack_int(count);
    }

    std::ostream& print(std::ostream& os) const override {
        os << "COUNT(*)";
        return os;
    }

private:
    uint64_t count = 0;
};

class AggGroupConcat : public Agg {
public:
    AggGroupConcat(VarId var_id, std::unique_ptr<BindingExpr> expr, const std::string& sep) :
        Agg (var_id, std::move(expr)),
        sep (std::move(sep)) { }

    void begin() override {
        res = std::string();
        type = GroupConcatType::UNSET;
    }

    void process() override {
        if (type == GroupConcatType::NULL_RESULT) {
            return;
        }

        if (type != GroupConcatType::UNSET) {
            res += sep;
        }

        auto oid = expr->eval(*binding);

        switch (RDF_OID::get_generic_sub_type(oid)) {
        case RDF_OID::GenericSubType::STRING_SIMPLE: {
            auto str = Conversions::unpack_string(oid);
            res += str;
            type = GroupConcatType::SIMPLE;
            break;
        }
        case RDF_OID::GenericSubType::STRING_XSD: {
            auto str = Conversions::unpack_string(oid);
            res += str;
            if (type == GroupConcatType::UNSET) {
                type = GroupConcatType::XSD;
            } else if (type != GroupConcatType::XSD) {
                type = GroupConcatType::SIMPLE;
            }
            break;
        }
        case RDF_OID::GenericSubType::STRING_LANG: {
            auto&& [l, str] = Conversions::unpack_string_lang(oid);
            res += str;
            if (type == GroupConcatType::UNSET) {
                lang = l;
                type = GroupConcatType::LANG;
            } else if (type == GroupConcatType::LANG) {
                if (lang != l) {
                    type = GroupConcatType::SIMPLE;
                }
            } else {
                type = GroupConcatType::SIMPLE;
            }
            break;
        }
        default: {
            type = GroupConcatType::NULL_RESULT;
            break;
        }
        }
    }

    ObjectId get() override {
        switch (type) {
        case GroupConcatType::UNSET:
            return Conversions::pack_empty_string();
        case GroupConcatType::NULL_RESULT:
            return ObjectId::get_null();
        case GroupConcatType::SIMPLE:
            return Conversions::pack_string_simple(res);
        case GroupConcatType::XSD:
            return Conversions::pack_string_xsd(res);
        case GroupConcatType::LANG:
            return Conversions::pack_string_lang(lang, res);
        default:
            assert(false);
            return ObjectId::get_null();
        }
    }

    std::ostream& print(std::ostream& os) const override {
        os << "GROUPCONCAT(";
        BindingExprPrinter printer(os);
        printer.print(*expr);
        os << ")";
        return os;
    }

    std::string sep;

private:
    enum class GroupConcatType {
        UNSET,
        NULL_RESULT,
        SIMPLE,
        XSD,
        LANG,
    };

    GroupConcatType type;
    std::string res;
    std::string lang;
};

class AggMax : public Agg {
public:
    using Agg::Agg;
    void begin() override {
        max = ObjectId::get_null();
    }

    void process() override {
        auto oid = expr->eval(*binding);
        if (max.is_null()) {
            max = oid;
        } else if (!oid.is_null()) {
            auto cmp = SPARQL::Comparisons::compare_for_order(oid, max);
            if (cmp > 0) {
                max = oid;
            }
        }
    }

    ObjectId get() override {
        return max;
    }

    std::ostream& print(std::ostream& os) const override {
        os << "MAX(";
        BindingExprPrinter printer(os);
        printer.print(*expr);
        os << ")";
        return os;
    }

private:
    ObjectId max;
};

class AggMin : public Agg {
public:
    using Agg::Agg;
    void begin() override
    {
        min = ObjectId::get_null();
    }

    void process() override
    {
        auto oid = expr->eval(*binding);
        if (min.is_null()) {
            min = oid;
        } else if (!oid.is_null()) {
            auto cmp = SPARQL::Comparisons::compare_for_order(oid, min);
            if (cmp < 0) {
                min = oid;
            }
        }
    }

    ObjectId get() override
    {
        return min;
    }

    std::ostream& print(std::ostream& os) const override
    {
        os << "MIN(";
        BindingExprPrinter printer(os);
        printer.print(*expr);
        os << ")";
        return os;
    }

private:
    ObjectId min;
};

class AggSample : public Agg {
public:
    using Agg::Agg;
    void begin() override
    {
        sample = ObjectId::get_null();
    }

    void process() override
    {
        if (!sample.is_null()) {
            return;
        }
        sample = expr->eval(*binding);
    }

    ObjectId get() override
    {
        return sample;
    }

    std::ostream& print(std::ostream& os) const override
    {
        os << "SAMPLE(";
        BindingExprPrinter printer(os);
        printer.print(*expr);
        os << ")";
        return os;
    }

private:
    ObjectId sample;
};

class AggCountAllDistinct : public Agg {
public:
    AggCountAllDistinct(VarId var_id, std::unique_ptr<BindingExpr> expr, std::vector<VarId> vars)
        : Agg(var_id, std::move(expr)), basic_vars(std::move(vars)) { }
    void begin() override { seen.clear(); }
    void process() override {
        std::vector<ObjectId> tuple;
        tuple.reserve(basic_vars.size());
        for (auto var : basic_vars) tuple.push_back((*binding)[var]);
        seen.insert(std::move(tuple));
    }
    ObjectId get() override { return Conversions::pack_int(seen.size()); }
    std::ostream& print(std::ostream& os) const override { return os << "AggCountAllDistinct"; }
private:
    std::vector<VarId> basic_vars;
    std::set<std::vector<ObjectId>> seen;
};

class AggCountDistinct : public Agg {
public:
    using Agg::Agg;
    void begin() override { seen.clear(); }
    void process() override {
        const auto value = expr->eval(*binding);
        if (!value.is_null()) seen.insert(value);
    }
    ObjectId get() override { return Conversions::pack_int(seen.size()); }
    std::ostream& print(std::ostream& os) const override { return os << "AggCountDistinct"; }
  private:
    std::set<ObjectId> seen;
};

class AggGroupConcatDistinct : public Agg {
public:
    AggGroupConcatDistinct(VarId var_id, std::unique_ptr<BindingExpr> expr, const std::string& sep)
        : Agg(var_id, std::move(expr)), separator(sep) { }
    void begin() override { }
    void process() override { }
    ObjectId get() override { return ObjectId::get_null(); }
    std::ostream& print(std::ostream& os) const override { return os << "AggGroupConcatDistinct"; }
private:
    std::string separator;
};

} // namespace SPARQL

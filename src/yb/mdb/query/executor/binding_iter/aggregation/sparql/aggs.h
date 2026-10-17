#pragma once

#include <string>
#include <vector>

#include "graph_models/rdf_model/comparisons.h"
#include "graph_models/rdf_model/conversions.h"
#include "query/executor/binding_iter/aggregation/agg.h"
#include "query/executor/binding_iter/binding_expr/binding_expr_printer.h"
#include "query/var_id.h"

namespace SPARQL {

class AggAvg : public Agg {
public:
    using Agg::Agg;
    void begin() override
    {
        sum_integer = 0;
        sum_decimal = Decimal();
        sum_float = 0;
        sum_double = 0;

        type = Conversions::OpType::INTEGER;
        count = 0;
    }

    void process() override
    {
        if (type == Conversions::OpType::INVALID) {
            return;
        }

        auto oid = expr->eval(*binding);
        auto op_type = Conversions::calculate_optype(oid);

        if (op_type > Conversions::OpType::DOUBLE) {
            type = Conversions::OpType::INVALID;
            return;
        }

        if (op_type > type) {
            if (op_type == Conversions::OpType::DECIMAL) {
                sum_decimal = Decimal(sum_integer);
                type = Conversions::OpType::DECIMAL;
            } else if (op_type == Conversions::OpType::FLOAT) {
                if (type == Conversions::OpType::INTEGER) {
                    sum_float = sum_integer;
                } else {
                    sum_float = sum_decimal.to_float();
                }
                type = Conversions::OpType::FLOAT;
            } else {
                if (type == Conversions::OpType::INTEGER) {
                    sum_double = sum_integer;
                } else if (type == Conversions::OpType::DECIMAL) {
                    sum_double = sum_decimal.to_double();
                } else {
                    sum_double = sum_float;
                }
                type = Conversions::OpType::DOUBLE;
            }
        }

        if (type == Conversions::OpType::INTEGER) {
            sum_integer += Conversions::unpack_int(oid);
        } else if (type == Conversions::OpType::DECIMAL) {
            sum_decimal = sum_decimal + Conversions::to_decimal(oid);
        } else if (type == Conversions::OpType::FLOAT) {
            sum_float += Conversions::to_float(oid);
        } else {
            sum_double += Conversions::to_double(oid);
        }
        count++;
    }

    ObjectId get() override
    {
        if (type == Conversions::OpType::INVALID) {
            return ObjectId::get_null();
        }

        if (count == 0) {
            return Conversions::pack_int(0);
        }

        if (type == Conversions::OpType::INTEGER) {
            Decimal avg;
            if (sum_integer % count == 0) {
                avg = Decimal(int64_t(sum_integer / count));
            } else {
                avg = Decimal(sum_integer) / Decimal(count);
            }
            return Conversions::pack_decimal(avg);
        } else if (type == Conversions::OpType::DECIMAL) {
            auto avg = sum_decimal / Decimal(count);
            return Conversions::pack_decimal(avg);
        } else if (type == Conversions::OpType::FLOAT) {
            auto avg = sum_float / count;
            return Conversions::pack_float(avg);
        } else if (type == Conversions::OpType::DOUBLE) {
            auto avg = sum_double / count;
            return Conversions::pack_double(avg);
        } else {
            return ObjectId::get_null();
        }
    }

    std::ostream& print(std::ostream& os) const override
    {
        os << "AVG(";
        BindingExprPrinter printer(os);
        printer.print(*expr);
        os << ")";
        return os;
    }

private:
    Conversions::OpType type;

    int64_t sum_integer;
    Decimal sum_decimal;
    float sum_float;
    double sum_double;

    int64_t count = 0;
};

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
            auto cmp = SPARQL::Comparisons::compare(oid, max);
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
            auto cmp = SPARQL::Comparisons::compare(oid, min);
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

class AggSum : public Agg {
public:
    using Agg::Agg;
    void begin() override
    {
        sum_integer = 0;
        sum_decimal = Decimal();
        sum_float = 0;
        sum_double = 0;

        type = Conversions::OpType::INTEGER;
    }

    void process() override
    {
        if (type == Conversions::OpType::INVALID) {
            return;
        }

        auto oid = expr->eval(*binding);
        auto op_type = Conversions::calculate_optype(oid);

        if (op_type > Conversions::OpType::DOUBLE) {
            type = Conversions::OpType::INVALID;
            return;
        }

        if (op_type > type) {
            if (op_type == Conversions::OpType::DECIMAL) {
                sum_decimal = Decimal(sum_integer);
                type = Conversions::OpType::DECIMAL;
            } else if (op_type == Conversions::OpType::FLOAT) {
                if (type == Conversions::OpType::INTEGER) {
                    sum_float = sum_integer;
                } else {
                    sum_float = sum_decimal.to_float();
                }
                type = Conversions::OpType::FLOAT;
            } else {
                if (type == Conversions::OpType::INTEGER) {
                    sum_double = sum_integer;
                } else if (type == Conversions::OpType::DECIMAL) {
                    sum_double = sum_decimal.to_double();
                } else {
                    sum_double = sum_float;
                }
                type = Conversions::OpType::DOUBLE;
            }
        }

        if (type == Conversions::OpType::INTEGER) {
            sum_integer += Conversions::unpack_int(oid);
        } else if (type == Conversions::OpType::DECIMAL) {
            sum_decimal = sum_decimal + Conversions::to_decimal(oid);
        } else if (type == Conversions::OpType::FLOAT) {
            sum_float += Conversions::to_float(oid);
        } else {
            sum_double += Conversions::to_double(oid);
        }
    }

    ObjectId get() override
    {
        switch (type) {
        case Conversions::OpType::INTEGER:
            return Conversions::pack_int(sum_integer);
        case Conversions::OpType::DECIMAL:
            return Conversions::pack_decimal(sum_decimal);
        case Conversions::OpType::FLOAT:
            return Conversions::pack_float(sum_float);
        case Conversions::OpType::DOUBLE:
            return Conversions::pack_double(sum_double);
        case Conversions::OpType::TENSOR_FLOAT:
        case Conversions::OpType::TENSOR_DOUBLE:
        case Conversions::OpType::INVALID:
        default:
            return ObjectId::get_null();
        }
    }

    std::ostream& print(std::ostream& os) const override
    {
        os << "SUM(";
        BindingExprPrinter printer(os);
        printer.print(*expr);
        os << ")";
        return os;
    }

private:
    Conversions::OpType type;

    int64_t sum_integer;
    Decimal sum_decimal;
    float sum_float;
    double sum_double;
};

class AggCountAllDistinct : public Agg {
public:
    AggCountAllDistinct(VarId var_id, std::unique_ptr<BindingExpr> expr, std::vector<VarId> vars)
        : Agg(var_id, std::move(expr)), basic_vars(std::move(vars)) { }
    void begin() override { }
    void process() override { }
    ObjectId get() override { return ObjectId::get_null(); }
    std::ostream& print(std::ostream& os) const override { return os << "AggCountAllDistinct"; }
private:
    std::vector<VarId> basic_vars;
};

class AggCountDistinct : public Agg {
public:
    using Agg::Agg;
    void begin() override { }
    void process() override { }
    ObjectId get() override { return ObjectId::get_null(); }
    std::ostream& print(std::ostream& os) const override { return os << "AggCountDistinct"; }
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

class AggAvgDistinct : public Agg {
public:
    using Agg::Agg;
    void begin() override { }
    void process() override { }
    ObjectId get() override { return ObjectId::get_null(); }
    std::ostream& print(std::ostream& os) const override { return os << "AggAvgDistinct"; }
};

class AggSumDistinct : public Agg {
public:
    using Agg::Agg;
    void begin() override { }
    void process() override { }
    ObjectId get() override { return ObjectId::get_null(); }
    std::ostream& print(std::ostream& os) const override { return os << "AggSumDistinct"; }
};

} // namespace SPARQL

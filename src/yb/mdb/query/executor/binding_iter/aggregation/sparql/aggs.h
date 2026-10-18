#pragma once

#include <set>
#include <string>
#include <utility>
#include <vector>

#include "graph_models/rdf_model/comparisons.h"
#include "graph_models/rdf_model/conversions.h"
#include "query/executor/binding_iter/aggregation/agg.h"
#include "query/executor/binding_iter/binding_expr/binding_expr_var.h"
#include "query/executor/binding_iter/order_by.h"
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

        auto oid = Conversions::numeric_operand(expr->eval(*binding));
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
            if (__builtin_add_overflow(sum_integer, Conversions::unpack_int(oid), &sum_integer)) {
                type = Conversions::OpType::INVALID;
                return;
            }
        } else if (type == Conversions::OpType::DECIMAL) {
            try {
                sum_decimal = sum_decimal + Conversions::to_decimal(oid);
            } catch (const std::out_of_range&) {
                type = Conversions::OpType::INVALID;
            }
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
            return Conversions::decimal_result([&] {
                return Decimal(sum_integer) / Decimal(count);
            });
        } else if (type == Conversions::OpType::DECIMAL) {
            return Conversions::decimal_result([&] { return sum_decimal / Decimal(count); });
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
        case GroupConcatType::XSD:
        case GroupConcatType::LANG:
            return Conversions::pack_string_simple(res);
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
            auto cmp = OrderBy::rdf_compare(oid, max);
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
            auto cmp = OrderBy::rdf_compare(oid, min);
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

        auto oid = Conversions::numeric_operand(expr->eval(*binding));
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
            if (__builtin_add_overflow(sum_integer, Conversions::unpack_int(oid), &sum_integer)) {
                type = Conversions::OpType::INVALID;
                return;
            }
        } else if (type == Conversions::OpType::DECIMAL) {
            try {
                sum_decimal = sum_decimal + Conversions::to_decimal(oid);
            } catch (const std::out_of_range&) {
                type = Conversions::OpType::INVALID;
            }
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
    AggCountAllDistinct(VarId var, std::unique_ptr<BindingExpr> expr, std::vector<VarId> vars)
        : Agg(var, std::move(expr)), vars_(std::move(vars)) {}
    void begin() override { seen_.clear(); }
    void process() override {
        std::vector<ObjectId> row;
        for (const auto var : vars_) row.push_back(Conversions::term_identity((*binding)[var]));
        seen_.insert(std::move(row));
    }
    ObjectId get() override { return Conversions::pack_int(seen_.size()); }
    std::ostream& print(std::ostream& os) const override { return os << "COUNT(DISTINCT *)"; }
 private:
    std::vector<VarId> vars_;
    std::set<std::vector<ObjectId>> seen_;
};

// Evaluate the argument once: DISTINCT must not re-evaluate volatile expressions in Base.
template<class Base>
class DistinctAggregate : public Agg {
 public:
    template<class... Args>
    DistinctAggregate(
        VarId var, std::unique_ptr<BindingExpr> expr, const char* name, Args&&... args)
        : Agg(var, std::move(expr)),
          inner_(var, std::make_unique<BindingExprVar>(VarId(0)), std::forward<Args>(args)...),
          name_(name) {
        inner_.set_binding(value_binding_);
    }
    void begin() override {
        seen_.clear();
        inner_.begin();
    }
    void process() override {
        const auto value = expr->eval(*binding);
        if (seen_.insert(Conversions::term_identity(value)).second) {
            value_binding_.add(VarId(0), value);
            inner_.process();
        }
    }
    ObjectId get() override { return inner_.get(); }
    std::ostream& print(std::ostream& os) const override {
        os << name_ << "(DISTINCT ";
        BindingExprPrinter printer(os);
        printer.print(*expr);
        return os << ")";
    }
 private:
    Binding value_binding_{1};
    Base inner_;
    const char* name_;
    std::set<ObjectId> seen_;
};

class AggCountDistinct : public DistinctAggregate<AggCount> {
 public:
    AggCountDistinct(VarId var, std::unique_ptr<BindingExpr> expr)
        : DistinctAggregate(var, std::move(expr), "COUNT") {}
};

class AggGroupConcatDistinct : public DistinctAggregate<AggGroupConcat> {
 public:
    AggGroupConcatDistinct(VarId var, std::unique_ptr<BindingExpr> expr, const std::string& sep)
        : DistinctAggregate(var, std::move(expr), "GROUP_CONCAT", sep) {}
};

class AggAvgDistinct : public DistinctAggregate<AggAvg> {
 public:
    AggAvgDistinct(VarId var, std::unique_ptr<BindingExpr> expr)
        : DistinctAggregate(var, std::move(expr), "AVG") {}
};

class AggSumDistinct : public DistinctAggregate<AggSum> {
 public:
    AggSumDistinct(VarId var, std::unique_ptr<BindingExpr> expr)
        : DistinctAggregate(var, std::move(expr), "SUM") {}
};

} // namespace SPARQL

#pragma once
#include <algorithm>

#include "graph_models/object_id.h"
#include "graph_models/rdf_model/conversions.h"
#include "query/exceptions.h"

namespace SPARQL {

class Comparisons {
public:
    static int64_t compare_for_order(ObjectId lhs, ObjectId rhs) {
        if (lhs.id == rhs.id) return 0;
        auto rank = [](ObjectId value) {
            switch (RDF_OID::get_generic_type(value)) {
                case RDF_OID::GenericType::NULL_ID: return 0;
                case RDF_OID::GenericType::BLANK: return 1;
                case RDF_OID::GenericType::IRI: return 2;
                default: return 3;
            }
        };
        const int lhs_rank = rank(lhs);
        const int rhs_rank = rank(rhs);
        if (lhs_rank != rhs_rank) return lhs_rank < rhs_rank ? -1 : 1;
        if (lhs_rank == 2) {
            std::string lhs_text, rhs_text;
            if (!Conversions::try_unpack_iri(lhs, &lhs_text) ||
                !Conversions::try_unpack_iri(rhs, &rhs_text)) {
                throw QueryException("Cannot resolve IRI text for ORDER BY");
            }
            return lhs_text.compare(rhs_text);
        }
        if (RDF_OID::get_generic_type(lhs) == RDF_OID::GenericType::NUMERIC &&
            RDF_OID::get_generic_type(rhs) == RDF_OID::GenericType::NUMERIC) {
            return NumericCompare(lhs, rhs);
        }
        if (RDF_OID::get_generic_type(lhs) == RDF_OID::GenericType::STRING &&
            RDF_OID::get_generic_type(rhs) == RDF_OID::GenericType::STRING) {
            const auto text_order = Conversions::unpack_string(lhs).compare(Conversions::unpack_string(rhs));
            if (text_order) return text_order;
        }
        return lhs.id < rhs.id ? -1 : 1;
    }

    static int64_t compare(ObjectId lhs, ObjectId rhs) {
        return _compare<Mode::Normal>(lhs, rhs);
    }

    static int64_t strict_compare(ObjectId lhs, ObjectId rhs, bool* error) {
        *error = false;
        auto left = RDF_OID::get_generic_type(lhs), right = RDF_OID::get_generic_type(rhs);
        if (left == RDF_OID::GenericType::NUMERIC && right == left) return NumericCompare(lhs, rhs);
        if (left == RDF_OID::GenericType::STRING && right == left) {
            auto ls = RDF_OID::get_generic_subtype(lhs), rs = RDF_OID::get_generic_subtype(rhs);
            if (ls == RDF_OID::GenericSubType::STRING_LANG && rs == ls) {
                auto l = Conversions::unpack_string_lang(lhs), r = Conversions::unpack_string_lang(rhs);
                if (l.first == r.first) return l.second.compare(r.second);
            } else if ((ls == RDF_OID::GenericSubType::STRING_SIMPLE || ls == RDF_OID::GenericSubType::STRING_XSD) &&
                       (rs == RDF_OID::GenericSubType::STRING_SIMPLE || rs == RDF_OID::GenericSubType::STRING_XSD)) {
                return Conversions::unpack_string(lhs).compare(Conversions::unpack_string(rhs));
            }
        }
        *error = true;
        return 0;
    }

private:
    static int64_t NumericCompare(ObjectId lhs, ObjectId rhs) {
        if (Conversions::calculate_optype(lhs, rhs) == Conversions::OpType::INTEGER) {
            const auto l = Conversions::unpack_int(lhs), r = Conversions::unpack_int(rhs);
            return l < r ? -1 : l > r ? 1 : 0;
        }
        if (Conversions::calculate_optype(lhs) <= Conversions::OpType::DECIMAL &&
            Conversions::calculate_optype(rhs) <= Conversions::OpType::DECIMAL) {
            // Compare normalized decimal digits without losing integer precision
            // through double, including mixed integer/decimal extrema.
            struct Key { bool negative; int64_t exponent; std::string digits; };
            auto key = [](ObjectId value) {
                std::string text = Conversions::to_lexical_str(value);
                bool negative = !text.empty() && text.front() == '-';
                if (!text.empty() && (text.front() == '-' || text.front() == '+')) text.erase(0, 1);
                auto point = text.find('.');
                if (point == std::string::npos) point = text.size();
                else text.erase(point, 1);
                auto first = text.find_first_not_of('0');
                if (first == std::string::npos) return Key{false, 0, ""};
                auto exponent = int64_t(point) - int64_t(first);
                text.erase(0, first);
                while (text.back() == '0') text.pop_back();
                return Key{negative, exponent, std::move(text)};
            };
            const auto l = key(lhs), r = key(rhs);
            if (l.negative != r.negative) return l.negative ? -1 : 1;
            int64_t order = 0;
            if (l.digits.empty() || r.digits.empty())
                order = l.digits.empty() ? (r.digits.empty() ? 0 : -1) : 1;
            else if (l.exponent != r.exponent) order = l.exponent < r.exponent ? -1 : 1;
            else for (size_t i = 0; i < std::max(l.digits.size(), r.digits.size()); ++i) {
                const char a = i < l.digits.size() ? l.digits[i] : '0';
                const char b = i < r.digits.size() ? r.digits[i] : '0';
                if (a != b) { order = a < b ? -1 : 1; break; }
            }
            return l.negative ? -order : order;
        }
        const auto l = Conversions::to_double(lhs), r = Conversions::to_double(rhs);
        // A deterministic NaN position keeps sorting a strict weak order.
        if (std::isnan(l) || std::isnan(r))
            return std::isnan(l) ? (std::isnan(r) ? 0 : -1) : 1;
        return l < r ? -1 : l > r ? 1 : 0;
    }
    enum class Mode { Normal, Strict };

    template<Mode mode>
    static int64_t _compare(ObjectId lhs, ObjectId rhs, bool* error = nullptr) {
        if (lhs.id < rhs.id) return -1;
        if (lhs.id > rhs.id) return 1;
        return 0;
    }
};

} // namespace SPARQL

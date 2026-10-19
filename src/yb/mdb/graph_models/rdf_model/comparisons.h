#pragma once

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
        const auto l = Conversions::to_double(lhs), r = Conversions::to_double(rhs);
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

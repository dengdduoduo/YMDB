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
        // Keep existing within-literal ordering; this change fixes IRI ordering only.
        return lhs.id < rhs.id ? -1 : 1;
    }

    static int64_t compare(ObjectId lhs, ObjectId rhs) {
        return _compare<Mode::Normal>(lhs, rhs);
    }

    static int64_t strict_compare(ObjectId lhs, ObjectId rhs, bool* error) {
        return _compare<Mode::Strict>(lhs, rhs, error);
    }

private:
    enum class Mode { Normal, Strict };

    template<Mode mode>
    static int64_t _compare(ObjectId lhs, ObjectId rhs, bool* error = nullptr) {
        if (lhs.id < rhs.id) return -1;
        if (lhs.id > rhs.id) return 1;
        return 0;
    }
};

} // namespace SPARQL

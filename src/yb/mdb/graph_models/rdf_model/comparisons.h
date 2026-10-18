// Copyright (c) YugabyteDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.
//

#pragma once

#include <compare>

#include <boost/multiprecision/cpp_int.hpp>

#include "graph_models/object_id.h"
#include "graph_models/rdf_model/conversions.h"

namespace SPARQL {

class Comparisons {
 public:
    static bool numeric_unordered(ObjectId lhs, ObjectId rhs) {
        lhs = Conversions::numeric_operand(lhs);
        rhs = Conversions::numeric_operand(rhs);
        if (RDF_OID::get_generic_type(lhs) != RDF_OID::GenericType::NUMERIC ||
            RDF_OID::get_generic_type(rhs) != RDF_OID::GenericType::NUMERIC) return false;
        return std::isnan(Conversions::to_double(lhs)) || std::isnan(Conversions::to_double(rhs));
    }

    static int64_t compare(ObjectId lhs, ObjectId rhs) {
        lhs = Conversions::value_operand(lhs);
        rhs = Conversions::value_operand(rhs);
        if (RDF_OID::get_generic_type(lhs) == RDF_OID::GenericType::NUMERIC &&
            RDF_OID::get_generic_type(rhs) == RDF_OID::GenericType::NUMERIC) {
            const auto order = numeric_compare(lhs, rhs);
            if (order == std::partial_ordering::unordered) {
                // ORDER BY needs a strict weak order even in the presence of NaNs.
                const bool left_nan = std::isnan(Conversions::to_double(lhs));
                const bool right_nan = std::isnan(Conversions::to_double(rhs));
                return static_cast<int>(right_nan) - static_cast<int>(left_nan);
            }
            return (order > 0) - (order < 0);
        }
        return (lhs.id > rhs.id) - (lhs.id < rhs.id);
    }

    // Exact value ordering avoids non-transitive ties from pairwise float promotion.
    static int64_t order_compare(ObjectId lhs, ObjectId rhs) {
        lhs = Conversions::numeric_operand(lhs);
        rhs = Conversions::numeric_operand(rhs);
        const auto left = exact_value(lhs);
        const auto right = exact_value(rhs);
        if (left.special != right.special) return left.special < right.special ? -1 : 1;
        if (left.special != 0) return 0;
        const BigInt l = left.numerator * right.denominator;
        const BigInt r = right.numerator * left.denominator;
        return (l > r) - (l < r);
    }

    static int64_t strict_compare(ObjectId lhs, ObjectId rhs, bool* error) {
        lhs = Conversions::value_operand(lhs);
        rhs = Conversions::value_operand(rhs);
        *error = false;
        if (lhs.is_null() || rhs.is_null()) {
            *error = true;
            return 0;
        }

        const bool left_numeric = RDF_OID::get_generic_type(lhs) == RDF_OID::GenericType::NUMERIC;
        const bool right_numeric = RDF_OID::get_generic_type(rhs) == RDF_OID::GenericType::NUMERIC;
        if (left_numeric != right_numeric) {
            *error = true;
            return 0;
        }
        const bool left_boolean = RDF_OID::get_generic_type(lhs) == RDF_OID::GenericType::BOOL;
        const bool right_boolean = RDF_OID::get_generic_type(rhs) == RDF_OID::GenericType::BOOL;
        if (left_boolean != right_boolean) {
            *error = true;
            return 0;
        }
        return compare(lhs, rhs);
    }

 private:
    using BigInt = boost::multiprecision::cpp_int;
    struct ExactValue {
        BigInt numerator = 0;
        BigInt denominator = 1;
        // NaN, -infinity, finite, +infinity.
        int special = 0;
    };

    static ExactValue exact_value(ObjectId oid) {
        ExactValue result;
        const auto type = RDF_OID::get_generic_subtype(oid);
        if (type == RDF_OID::GenericSubType::INTEGER) {
            result.numerator = Conversions::unpack_int(oid);
        } else if (type == RDF_OID::GenericSubType::DECIMAL) {
            char data[Decimal::EXTERN_BUFFER_SIZE];
            Conversions::unpack_decimal(oid).serialize_extern(data);
            uint64_t significand = 0;
            for (size_t i = 0; i < 8; ++i) {
                const auto byte = static_cast<unsigned char>(data[i]);
                significand |= static_cast<uint64_t>(byte) << (8 * i);
            }
            result.numerator = significand;
            const auto exponent = static_cast<int8_t>(data[8]);
            for (int i = 0; i < std::abs(static_cast<int>(exponent)); ++i) {
                if (exponent < 0) {
                    result.denominator *= 10;
                } else {
                    result.numerator *= 10;
                }
            }
            if (data[9]) result.numerator = -result.numerator;
        } else {
            const auto bits = std::bit_cast<uint64_t>(Conversions::to_double(oid));
            const int exponent = static_cast<int>((bits >> 52) & 0x7ff);
            const auto fraction = bits & ((uint64_t{1} << 52) - 1);
            const bool negative = (bits >> 63) != 0;
            if (exponent == 0x7ff) {
                result.special = fraction ? -2 : negative ? -1 : 1;
                return result;
            }
            result.numerator = fraction | (exponent ? uint64_t{1} << 52 : 0);
            const auto shift = (exponent ? exponent - 1023 : -1022) - 52;
            if (shift < 0) {
                result.denominator <<= -shift;
            } else {
                result.numerator <<= shift;
            }
            if (negative) result.numerator = -result.numerator;
        }
        return result;
    }

    static std::partial_ordering numeric_compare(ObjectId lhs, ObjectId rhs) {
        switch (Conversions::calculate_optype(lhs, rhs)) {
        case Conversions::OpType::INTEGER:
            return Conversions::to_integer(lhs) <=> Conversions::to_integer(rhs);
        case Conversions::OpType::FLOAT:
            return Conversions::to_float(lhs) <=> Conversions::to_float(rhs);
        case Conversions::OpType::DOUBLE:
            return Conversions::to_double(lhs) <=> Conversions::to_double(rhs);
        case Conversions::OpType::DECIMAL: {
            const auto left = Conversions::to_decimal(lhs);
            const auto right = Conversions::to_decimal(rhs);
            if (left < right) return std::partial_ordering::less;
            if (left > right) return std::partial_ordering::greater;
            return std::partial_ordering::equivalent;
        }
        default:
            throw std::invalid_argument("Expected numeric operands");
        }
    }
};

} // namespace SPARQL

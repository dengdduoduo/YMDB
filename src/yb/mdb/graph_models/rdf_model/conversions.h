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
#include <bit>
#include <charconv>
#include <limits>
#include <string>
#include <stdexcept>
#include <cstdint>
#include <string_view>
#include <cmath>
#include <cstring>
#include <mutex>
#include <unordered_map>
#include "graph_models/common/datatypes/decimal.h"
#include "graph_models/common/datatypes/datetime.h"
#include "graph_models/common/datatypes/tensor/tensor.h"
#include "graph_models/object_id.h"
#include "graph_models/rdf_model/rdf_object_id.h"
#include "query/id.h"
#include "yb/mdb/import/rdf_term.h"

namespace SPARQL {

struct Conversions {
    enum class OpType : uint8_t {
        INTEGER    = 0,
        DECIMAL    = 1,
        FLOAT      = 2,
        DOUBLE     = 3,
        TENSOR_FLOAT  = 4,
        TENSOR_DOUBLE = 5,
        INVALID    = 6
    };

 private:
    static uint64_t fnv1a_64(std::string_view str) {
        constexpr uint64_t FNV_OFFSET_BASIS = 14695981039346656037ULL;
        constexpr uint64_t FNV_PRIME = 1099511628211ULL;
        uint64_t hash = FNV_OFFSET_BASIS;
        for (unsigned char c : str) {
            hash ^= c;
            hash *= FNV_PRIME;
        }
        return hash;
    }

    static uint64_t inline_string_7(const char* str) {
        uint64_t res = 0;
        int shift_size = 8 * 6;
        for (const char* i = str; *i != '\0'; i++) {
            uint8_t byte = static_cast<uint8_t>(*i);
            res |= static_cast<uint64_t>(byte) << shift_size;
            shift_size -= 8;
        }
        return res;
    }

    static uint64_t inline_string_5(const char* str) {
        uint64_t res = 0;
        int shift_size = 8 * 4;
        for (const char* i = str; *i != '\0'; i++) {
            uint8_t byte = static_cast<uint8_t>(*i);
            res |= static_cast<uint64_t>(byte) << shift_size;
            shift_size -= 8;
        }
        return res;
    }

    static std::string get_string_inlined_7(uint64_t id) {
        constexpr int N = 7;
        char buff[N];
        int suffix_shift_size = 8 * (N - 1);
        uint_fast32_t str_len = N;
        for (uint_fast32_t i = 0; i < N; i++) {
            uint8_t byte = (id >> suffix_shift_size) & 0xFF;
            char c = static_cast<char>(byte);
            if (c == '\0') {
                str_len = i;
                break;
            }
            buff[i] = c;
            suffix_shift_size -= 8;
        }
        return std::string(buff, str_len);
    }

    static std::string get_string_inlined_5(uint64_t id) {
        constexpr int N = 5;
        char buff[N];
        int suffix_shift_size = 8 * (N - 1);
        uint_fast32_t str_len = N;
        for (uint_fast32_t i = 0; i < N; i++) {
            uint8_t byte = (id >> suffix_shift_size) & 0xFF;
            char c = static_cast<char>(byte);
            if (c == '\0') {
                str_len = i;
                break;
            }
            buff[i] = c;
            suffix_shift_size -= 8;
        }
        return std::string(buff, str_len);
    }

    static std::unordered_map<uint64_t, std::string>& extern_string_storage() {
        static std::unordered_map<uint64_t, std::string> storage;
        return storage;
    }

    static std::unordered_map<uint64_t, std::string>& iri_storage() {
        static std::unordered_map<uint64_t, std::string> storage;
        return storage;
    }

    static std::mutex& string_storage_mutex() {
        static std::mutex mtx;
        return mtx;
    }

    static std::unordered_map<uint64_t, yb::mdb::RdfTerm>& metadata_storage() {
        static std::unordered_map<uint64_t, yb::mdb::RdfTerm> storage;
        return storage;
    }

    static ObjectId pack_metadata_string(const yb::mdb::RdfTerm& term, uint64_t mask) {
        const auto key = term.Key();
        if (!key.ok()) throw std::runtime_error(key.status().ToString());
        const auto canonical = yb::mdb::RdfTerm::FromKey(*key);
        if (!canonical.ok()) throw std::runtime_error(canonical.status().ToString());
        const uint64_t content = fnv1a_64(*key) & ObjectId::MASK_EXTERNAL_ID;
        const uint64_t id = mask | content;
        std::lock_guard<std::mutex> lock(string_storage_mutex());
        auto [it, inserted] = metadata_storage().emplace(id, *canonical);
        const auto previous_key = it->second.Key();
        if (!inserted && (!previous_key.ok() || *previous_key != *key)) {
            throw std::runtime_error("Conflicting RDF literal packed identities");
        }
        auto [value, new_value] = extern_string_storage().emplace(content, term.lexical);
        if (!new_value && value->second != term.lexical) {
            throw std::runtime_error("Conflicting RDF external string identities");
        }
        return ObjectId(id);
    }

    static std::unordered_map<uint64_t, std::vector<char>>& extern_tensor_storage() {
        static std::unordered_map<uint64_t, std::vector<char>> storage;
        return storage;
    }

    static std::mutex& tensor_storage_mutex() {
        static std::mutex mtx;
        return mtx;
    }

    struct NumericStorage {
        std::mutex mutex;
        std::unordered_map<std::string, uint64_t> ids;
        std::unordered_map<uint64_t, std::string> values;
    };

    static NumericStorage& numeric_storage() {
        static NumericStorage storage;
        return storage;
    }

    // These handles belong to the process, never to the persisted RDF graph ID space.
    static ObjectId pack_numeric(uint64_t mask, std::string bytes) {
        auto& storage = numeric_storage();
        std::lock_guard<std::mutex> lock(storage.mutex);
        const auto key = std::string(1, static_cast<char>(mask >> 56)) + bytes;
        const auto found = storage.ids.find(key);
        if (found != storage.ids.end()) return ObjectId(found->second);
        const uint64_t index = storage.values.size() + 1;
        if (index > ObjectId::MASK_EXTERNAL_ID) {
            throw std::overflow_error("Numeric handles exhausted");
        }
        const auto id = mask | index;
        storage.values.emplace(id, std::move(bytes));
        storage.ids.emplace(key, id);
        return ObjectId(id);
    }

    static std::string unpack_numeric(ObjectId oid, size_t size) {
        auto& storage = numeric_storage();
        std::lock_guard<std::mutex> lock(storage.mutex);
        const auto found = storage.values.find(oid.id);
        if (found == storage.values.end() || found->second.size() != size) {
            throw std::runtime_error("Missing or invalid temporary numeric payload");
        }
        return found->second;
    }

    template<class T>
    static std::string floating_lexical(T value) {
        if (std::isnan(value)) return "NaN";
        if (std::isinf(value)) return std::signbit(value) ? "-INF" : "INF";
        char buffer[64];
        const auto result = std::to_chars(
            buffer, buffer + sizeof(buffer), value, std::chars_format::general);
        if (result.ec != std::errc{}) throw std::runtime_error("Cannot format numeric value");
        return std::string(buffer, result.ptr);
    }

 public:
    static void set_extern_string(uint64_t external_id, const std::string& str) {
        std::lock_guard<std::mutex> lock(string_storage_mutex());
        auto& storage = extern_string_storage();
        const auto [it, inserted] = storage.emplace(external_id, str);
        if (!inserted && it->second != str) {
            throw std::runtime_error("Conflicting RDF external string identities");
        }
    }

    static ObjectId pack_string_simple(const std::string& str) {
        if (str.size() <= ObjectId::STR_INLINE_BYTES && str.find('\0') == std::string::npos) {
            return ObjectId(inline_string_7(str.c_str()) | ObjectId::MASK_STRING_SIMPLE_INLINED);
        }
        uint64_t hash = fnv1a_64(str);
        uint64_t content = hash & ObjectId::MASK_EXTERNAL_ID;
        {
            std::lock_guard<std::mutex> lock(string_storage_mutex());
            auto& storage = extern_string_storage();
            const auto [it, inserted] = storage.emplace(content, str);
            if (!inserted && it->second != str) {
                throw std::runtime_error("Conflicting RDF external string identities");
            }
        }
        return ObjectId(ObjectId::MASK_STRING_SIMPLE_EXTERN | content);
    }

    static ObjectId pack_string_xsd(const std::string& str) {
        if (str.size() <= ObjectId::STR_INLINE_BYTES && str.find('\0') == std::string::npos) {
            return ObjectId(inline_string_7(str.c_str()) | ObjectId::MASK_STRING_XSD_INLINED);
        }
        uint64_t hash = fnv1a_64(str);
        uint64_t content = hash & ObjectId::MASK_EXTERNAL_ID;
        {
            std::lock_guard<std::mutex> lock(string_storage_mutex());
            auto& storage = extern_string_storage();
            const auto [it, inserted] = storage.emplace(content, str);
            if (!inserted && it->second != str) {
                throw std::runtime_error("Conflicting RDF external string identities");
            }
        }
        return ObjectId(ObjectId::MASK_STRING_XSD_EXTERN | content);
    }

    static ObjectId pack_string_datatype(const std::string& datatype, const std::string& str) {
        if (datatype == "http://www.w3.org/2001/XMLSchema#string") return pack_string_simple(str);
        return pack_metadata_string(
            {yb::mdb::TripleTermType::LITERAL, str, "", datatype},
            ObjectId::MASK_STRING_DATATYPE_EXTERN);
    }

    static ObjectId pack_string_lang(const std::string& lang, const std::string& str) {
        return pack_metadata_string(
            {yb::mdb::TripleTermType::LITERAL, str, lang, ""},
            ObjectId::MASK_STRING_LANG_EXTERN);
    }

    static ObjectId try_pack_string_datatype(const std::string& datatype, const std::string& str) {
        return pack_string_datatype(datatype, str);
    }

    static ObjectId pack_empty_string() {
        return ObjectId(ObjectId::MASK_STRING_SIMPLE_INLINED);
    }

    static ObjectId pack_int(int64_t i) {
        constexpr int64_t limit = int64_t{1} << 55;
        if (i >= -limit && i < limit) {
            return ObjectId(ObjectId::MASK_POSITIVE_INT |
                            (static_cast<uint64_t>(i) & ObjectId::VALUE_MASK));
        }
        return pack_numeric(ObjectId::MASK_INT64_TMP,
                            std::string(reinterpret_cast<const char*>(&i), sizeof(i)));
    }

    static ObjectId pack_float(float f) {
        return ObjectId(ObjectId::MASK_FLOAT | std::bit_cast<uint32_t>(f));
    }

    static ObjectId pack_double(double d) {
        return pack_numeric(ObjectId::MASK_DOUBLE_TMP,
                            std::string(reinterpret_cast<const char*>(&d), sizeof(d)));
    }

    static ObjectId pack_decimal(const Decimal& dec) {
        if (dec.can_inline()) {
            return ObjectId(ObjectId::MASK_DECIMAL_INLINED | dec.serialize_inlined());
        }
        char buffer[Decimal::EXTERN_BUFFER_SIZE];
        dec.serialize_extern(buffer);
        return pack_numeric(ObjectId::MASK_DECIMAL_TMP, std::string(buffer, sizeof(buffer)));
    }

    template<class Operation>
    static ObjectId decimal_result(Operation operation) {
        try {
            return pack_decimal(operation());
        } catch (const std::out_of_range&) {
            return ObjectId::get_null();
        } catch (const std::domain_error&) {
            return ObjectId::get_null();
        }
    }

    static ObjectId pack_decimal(double d) {
        if (d == 0) return pack_decimal(Decimal());
        if (!std::isnormal(d)) throw std::out_of_range("Value cannot be represented as decimal");
        return pack_decimal(Decimal(d));
    }

    static ObjectId pack_iri(const std::string& iri) {
        uint64_t hash = fnv1a_64(iri);
        uint64_t content = hash & ObjectId::MASK_IRI_CONTENT;
        const auto id = ObjectId::MASK_IRI_INLINED | content;
        std::lock_guard<std::mutex> lock(string_storage_mutex());
        const auto [it, inserted] = iri_storage().emplace(id, iri);
        if (!inserted && it->second != iri) {
            throw std::runtime_error("Conflicting RDF IRI packed identities");
        }
        return ObjectId(id);
    }

    static ObjectId pack_bool(bool b) {
        return b ? ObjectId(ObjectId::BOOL_TRUE) : ObjectId(ObjectId::BOOL_FALSE);
    }

    static ObjectId pack_blank_tmp(uint64_t id) {
        return ObjectId(ObjectId::MASK_BLANK_TMP | id);
    }

    template<typename T>
    static ObjectId pack_tensor(const tensor::Tensor<T>& t) {
        if (t.empty()) {
            return ObjectId(tensor::Tensor<T>::get_inline_mask());
        }
        const auto* bytes = reinterpret_cast<const char*>(t.data());
        const auto num_bytes = sizeof(T) * t.size();
        uint64_t hash = fnv1a_64(std::string_view(bytes, num_bytes));
        uint64_t content = hash & ObjectId::MASK_EXTERNAL_ID;
        {
            std::lock_guard<std::mutex> lock(tensor_storage_mutex());
            auto& storage = extern_tensor_storage();
            if (storage.find(content) == storage.end()) {
                storage[content].assign(bytes, bytes + num_bytes);
            }
        }
        return ObjectId(tensor::Tensor<T>::get_external_mask() | content);
    }

    static ObjectId unpack_iri(const std::string&) { return ObjectId::get_null(); }
    static ObjectId try_unpack_xsd_string(const std::string&) { return ObjectId::get_null(); }
    static ObjectId try_unpack_xsd_datetime(const std::string&) { return ObjectId::get_null(); }
    static ObjectId try_unpack_xsd_date(const std::string&) { return ObjectId::get_null(); }
    static ObjectId try_unpack_xsd_integer(const std::string&, unsigned char) {
        return ObjectId::get_null();
    }
    static ObjectId try_unpack_xsd_boolean(const std::string&) { return ObjectId::get_null(); }
    static ObjectId try_unpack_string(const std::string&) { return ObjectId::get_null(); }
    static bool check_iri(const std::string&) { return true; }
    static ObjectId unpack_special_path(const std::string&) { return ObjectId::get_null(); }

    static std::string unpack_string(ObjectId oid) {
        switch (oid.get_type()) {
        case ObjectId::MASK_STRING_SIMPLE_INLINED:
        case ObjectId::MASK_STRING_XSD_INLINED:
            return get_string_inlined_7(oid.id);
        case ObjectId::MASK_STRING_SIMPLE_EXTERN:
        case ObjectId::MASK_STRING_XSD_EXTERN:
        case ObjectId::MASK_STRING_DATATYPE_EXTERN:
        case ObjectId::MASK_STRING_LANG_EXTERN: {
            uint64_t external_id = oid.id & ObjectId::MASK_EXTERNAL_ID;
            std::lock_guard<std::mutex> lock(string_storage_mutex());
            auto& storage = extern_string_storage();
            auto it = storage.find(external_id);
            if (it != storage.end()) return it->second;
            return {};
        }
        case ObjectId::MASK_STRING_SIMPLE_TMP:
        case ObjectId::MASK_STRING_XSD_TMP:
        case ObjectId::MASK_STRING_DATATYPE_TMP:
        case ObjectId::MASK_STRING_LANG_TMP: {
            uint64_t external_id = oid.id & ObjectId::MASK_EXTERNAL_ID;
            std::lock_guard<std::mutex> lock(string_storage_mutex());
            auto& storage = extern_string_storage();
            auto it = storage.find(external_id);
            if (it != storage.end()) return it->second;
            return {};
        }
        case ObjectId::MASK_STRING_DATATYPE_INLINED:
        case ObjectId::MASK_STRING_LANG_INLINED:
            return get_string_inlined_5(oid.id);
        default:
            return {};
        }
    }

    static float unpack_float(ObjectId oid) {
        uint32_t bits = static_cast<uint32_t>(oid.id & 0xFFFFFFFF);
        float f;
        std::memcpy(&f, &bits, sizeof(float));
        return f;
    }

    static int64_t unpack_int(ObjectId oid) {
        if (oid.get_type() == ObjectId::MASK_POSITIVE_INT) {
            const auto value = oid.get_value();
            return (value & (uint64_t{1} << 55)) ?
                static_cast<int64_t>(value) - (int64_t{1} << 56) : static_cast<int64_t>(value);
        }
        if (RDF_OID::get_generic_subtype(oid) != RDF_OID::GenericSubType::INTEGER) {
            throw std::invalid_argument("Expected integer ObjectId");
        }
        const auto bytes = unpack_numeric(oid, sizeof(int64_t));
        int64_t result;
        std::memcpy(&result, bytes.data(), sizeof(result));
        return result;
    }

    static double unpack_double(ObjectId oid) {
        if (RDF_OID::get_generic_subtype(oid) != RDF_OID::GenericSubType::DOUBLE) {
            throw std::invalid_argument("Expected double ObjectId");
        }
        const auto bytes = unpack_numeric(oid, sizeof(double));
        double result;
        std::memcpy(&result, bytes.data(), sizeof(result));
        return result;
    }

    static Decimal unpack_decimal(ObjectId oid) {
        if (oid.get_type() == ObjectId::MASK_DECIMAL_INLINED) {
            return Decimal::from_inlined(oid.get_value());
        }
        if (RDF_OID::get_generic_subtype(oid) != RDF_OID::GenericSubType::DECIMAL) {
            throw std::invalid_argument("Expected decimal ObjectId");
        }
        const auto bytes = unpack_numeric(oid, Decimal::EXTERN_BUFFER_SIZE);
        return Decimal::from_external(bytes.data());
    }

    static std::pair<std::string, std::string> unpack_string_lang(ObjectId oid) {
        std::lock_guard<std::mutex> lock(string_storage_mutex());
        const auto found = metadata_storage().find(oid.id);
        if (found == metadata_storage().end()) {
            throw std::runtime_error("Missing RDF literal metadata");
        }
        return {found->second.language, found->second.lexical};
    }

    static std::pair<std::string, std::string> unpack_string_datatype(ObjectId oid) {
        std::lock_guard<std::mutex> lock(string_storage_mutex());
        const auto found = metadata_storage().find(oid.id);
        if (found == metadata_storage().end()) {
            throw std::runtime_error("Missing RDF literal metadata");
        }
        return {found->second.datatype, found->second.lexical};
    }

    static bool try_get_iri(ObjectId oid, std::string* text) {
        {
            std::lock_guard<std::mutex> lock(string_storage_mutex());
            const auto found = iri_storage().find(oid.id);
            if (found != iri_storage().end()) {
                *text = found->second;
                return true;
            }
        }
        auto packed = g_pack_to_gmap.find(oid.id);
        const auto graph_id = packed == g_pack_to_gmap.end() ? oid.id : packed->second;
        auto found = g_gmap_to_uri.find(graph_id);
        if (found == g_gmap_to_uri.end()) return false;
        *text = found->second;
        return true;
    }

    static std::string unpack_iri(ObjectId oid) {
        std::string text;
        try_get_iri(oid, &text);
        return text;
    }


    // Resolve values only at expression boundaries; graph bindings retain their RDF identity.
    static ObjectId numeric_operand(ObjectId oid) {
        if (RDF_OID::get_generic_subtype(oid) != RDF_OID::GenericSubType::STRING_DATATYPE) {
            return oid;
        }
        const auto [datatype, lexical] = unpack_string_datatype(oid);
        constexpr std::string_view xsd = "http://www.w3.org/2001/XMLSchema#";
        if (!datatype.starts_with(xsd)) return oid;
        const auto name = std::string_view(datatype).substr(xsd.size());
        const bool integer = name == "integer" || name == "long" || name == "int" ||
            name == "short" || name == "byte" || name == "nonNegativeInteger" ||
            name == "positiveInteger" || name == "nonPositiveInteger" ||
            name == "negativeInteger" || name == "unsignedLong" || name == "unsignedInt" ||
            name == "unsignedShort" || name == "unsignedByte";
        if (!integer && name != "decimal" && name != "float" && name != "double") return oid;
        const auto invalid = ObjectId::get_null();
        std::string_view text(lexical);
        const auto first = text.find_first_not_of(" \t\r\n");
        if (first == std::string_view::npos) return invalid;
        text = text.substr(first, text.find_last_not_of(" \t\r\n") - first + 1);
        auto magnitude = text;
        if (magnitude.front() == '+' || magnitude.front() == '-') magnitude.remove_prefix(1);
        if (magnitude.empty()) return invalid;
        if (integer) {
            if (magnitude.find_first_not_of("0123456789") != std::string_view::npos) return invalid;
            // Leading zeroes are lexical identity, not additional value precision.
            const auto nonzero = magnitude.find_first_not_of('0');
            auto digits = nonzero == std::string_view::npos ?
                std::string_view("0") : magnitude.substr(nonzero);
            std::string canonical = text.front() == '-' ? "-" : "";
            canonical += digits;
            int64_t value;
            const auto parsed = std::from_chars(
                canonical.data(), canonical.data() + canonical.size(), value);
            if (parsed.ec != std::errc() || parsed.ptr != canonical.data() + canonical.size()) {
                return invalid;
            }
            if ((name == "positiveInteger" && value <= 0) ||
                (name == "negativeInteger" && value >= 0) ||
                (name == "nonNegativeInteger" && value < 0) ||
                (name == "nonPositiveInteger" && value > 0) ||
                (name.starts_with("unsigned") && value < 0)) return invalid;
            if ((name == "byte" && (value < -128 || value > 127)) ||
                (name == "short" && (value < -32768 || value > 32767)) ||
                (name == "int" && (value < INT32_MIN || value > INT32_MAX)) ||
                (name == "unsignedByte" && value > 255) ||
                (name == "unsignedShort" && value > 65535) ||
                (name == "unsignedInt" && value > UINT32_MAX)) return invalid;
            return pack_int(value);
        }
        if (name == "decimal") {
            if (magnitude.find_first_not_of(".0123456789") != std::string_view::npos ||
                magnitude.find_first_of("0123456789") == std::string_view::npos ||
                magnitude.find('.') != magnitude.rfind('.')) return invalid;
            std::string digits;
            for (char c : magnitude) if (c != '.') digits += c;
            const auto start = digits.find_first_not_of('0');
            if (start != std::string::npos &&
                digits.find_last_not_of('0') - start + 1 > Decimal::MAX_DIGIT_PRECISION) {
                return invalid;
            }
            bool error = false;
            const Decimal value(text, &error);
            return error ? invalid : pack_decimal(value);
        }
        if (text == "NaN" || text == "INF" || text == "+INF" || text == "-INF") {
            const double value = text == "NaN" ? std::numeric_limits<double>::quiet_NaN() :
                text.front() == '-' ? -std::numeric_limits<double>::infinity() :
                                      std::numeric_limits<double>::infinity();
            return name == "float" ? pack_float(static_cast<float>(value)) : pack_double(value);
        }
        // Reject C spellings (nan, infinity, hex) and require the complete XSD lexical form.
        size_t pos = 0, digits = 0;
        while (pos < magnitude.size() && magnitude[pos] >= '0' && magnitude[pos] <= '9') {
            ++pos;
            ++digits;
        }
        if (pos < magnitude.size() && magnitude[pos] == '.') {
            ++pos;
            while (pos < magnitude.size() && magnitude[pos] >= '0' && magnitude[pos] <= '9') {
                ++pos;
                ++digits;
            }
        }
        if (!digits) return invalid;
        if (pos < magnitude.size() && (magnitude[pos] == 'e' || magnitude[pos] == 'E')) {
            ++pos;
            if (pos < magnitude.size() && (magnitude[pos] == '+' || magnitude[pos] == '-')) ++pos;
            const auto exponent_start = pos;
            while (pos < magnitude.size() && magnitude[pos] >= '0' && magnitude[pos] <= '9') ++pos;
            if (pos == exponent_start) return invalid;
        }
        if (pos != magnitude.size()) return invalid;
        if (text.front() == '+') text.remove_prefix(1);
        if (name == "float") {
            float value;
            const auto parsed = std::from_chars(text.data(), text.data() + text.size(), value);
            return parsed.ec == std::errc() && parsed.ptr == text.data() + text.size() ?
                pack_float(value) : invalid;
        }
        double value;
        const auto parsed = std::from_chars(text.data(), text.data() + text.size(), value);
        return parsed.ec == std::errc() && parsed.ptr == text.data() + text.size() ?
            pack_double(value) : invalid;
    }

    static ObjectId boolean_operand(ObjectId oid) {
        if (RDF_OID::get_generic_subtype(oid) != RDF_OID::GenericSubType::STRING_DATATYPE) {
            return oid;
        }
        const auto [datatype, lexical] = unpack_string_datatype(oid);
        if (datatype != "http://www.w3.org/2001/XMLSchema#boolean") return oid;
        const auto first = lexical.find_first_not_of(" \t\r\n");
        if (first == std::string::npos) return ObjectId::get_null();
        const auto text = lexical.substr(first, lexical.find_last_not_of(" \t\r\n") - first + 1);
        if (text == "true" || text == "1") return pack_bool(true);
        if (text == "false" || text == "0") return pack_bool(false);
        return ObjectId::get_null();
    }

    static ObjectId value_operand(ObjectId oid) {
        return boolean_operand(numeric_operand(oid));
    }

    static ObjectId to_boolean(ObjectId oid) {
        if (oid.is_null()) return oid;
        oid = value_operand(oid);
        // Ill-typed boolean/numeric literals have false EBV; evaluation errors stay errors.
        if (oid.is_null()) return pack_bool(false);
        if (oid.id == ObjectId::BOOL_TRUE || oid.id == ObjectId::BOOL_FALSE) return oid;
        const auto subtype = RDF_OID::get_generic_subtype(oid);
        if (subtype == RDF_OID::GenericSubType::STRING_SIMPLE ||
            subtype == RDF_OID::GenericSubType::STRING_XSD ||
            subtype == RDF_OID::GenericSubType::STRING_LANG) {
            return pack_bool(!unpack_string(oid).empty());
        }
        if (RDF_OID::get_generic_type(oid) == RDF_OID::GenericType::NUMERIC) {
            const auto value = to_double(oid);
            return pack_bool(value != 0 && !std::isnan(value));
        }
        return ObjectId::get_null();
    }

    static std::string to_lexical_str(ObjectId oid) {
        switch (RDF_OID::get_type(oid)) {
        case RDF_OID::Type::STRING_SIMPLE_INLINE:
        case RDF_OID::Type::STRING_SIMPLE_EXTERN:
        case RDF_OID::Type::STRING_SIMPLE_TMP:
        case RDF_OID::Type::STRING_XSD_INLINE:
        case RDF_OID::Type::STRING_XSD_EXTERN:
        case RDF_OID::Type::STRING_XSD_TMP:
        case RDF_OID::Type::STRING_DATATYPE_INLINE:
        case RDF_OID::Type::STRING_DATATYPE_EXTERN:
        case RDF_OID::Type::STRING_DATATYPE_TMP:
        case RDF_OID::Type::STRING_LANG_INLINE:
        case RDF_OID::Type::STRING_LANG_EXTERN:
        case RDF_OID::Type::STRING_LANG_TMP:
            return unpack_string(oid);
        case RDF_OID::Type::INT56_INLINE:
        case RDF_OID::Type::INT64_EXTERN:
        case RDF_OID::Type::INT64_TMP:
            return std::to_string(unpack_int(oid));
        case RDF_OID::Type::FLOAT32:
            return floating_lexical(unpack_float(oid));
        case RDF_OID::Type::DOUBLE64_EXTERN:
        case RDF_OID::Type::DOUBLE64_TMP:
            return floating_lexical(unpack_double(oid));
        case RDF_OID::Type::DECIMAL_INLINE:
        case RDF_OID::Type::DECIMAL_EXTERN:
        case RDF_OID::Type::DECIMAL_TMP:
            return unpack_decimal(oid).to_string();
        case RDF_OID::Type::BOOL:
            return oid.is_true() ? "true" : "false";
        case RDF_OID::Type::IRI_INLINE:
        case RDF_OID::Type::IRI_EXTERN:
        case RDF_OID::Type::IRI_TMP:
            return unpack_iri(oid);
        default:
            return "";
        }
    }

    static ObjectId term_identity(ObjectId oid) {
        if (RDF_OID::get_generic_type(oid) == RDF_OID::GenericType::BOOL) {
            return pack_string_datatype(
                "http://www.w3.org/2001/XMLSchema#boolean", to_lexical_str(oid));
        }
        if (RDF_OID::get_generic_type(oid) != RDF_OID::GenericType::NUMERIC) return oid;
        const char* datatype = nullptr;
        switch (RDF_OID::get_generic_subtype(oid)) {
        case RDF_OID::GenericSubType::INTEGER: datatype = "integer"; break;
        case RDF_OID::GenericSubType::DECIMAL: datatype = "decimal"; break;
        case RDF_OID::GenericSubType::FLOAT: datatype = "float"; break;
        case RDF_OID::GenericSubType::DOUBLE: datatype = "double"; break;
        default: return oid;
        }
        return pack_string_datatype(
            std::string("http://www.w3.org/2001/XMLSchema#") + datatype, to_lexical_str(oid));
    }

    static ObjectId string_simple_to_xsd(ObjectId oid) {
        return ObjectId(
            (oid.id & ~ObjectId::SUB_TYPE_MASK) | ObjectId::MASK_STRING_XSD_INLINED
        );
    }

    static double to_double(ObjectId oid) {
        oid = numeric_operand(oid);
        switch (RDF_OID::get_generic_type(oid)) {
        case RDF_OID::GenericType::NUMERIC: {
            auto subtype = RDF_OID::get_generic_subtype(oid);
            if (subtype == RDF_OID::GenericSubType::INTEGER) {
                return static_cast<double>(unpack_int(oid));
            }
            if (subtype == RDF_OID::GenericSubType::FLOAT) {
                return static_cast<double>(unpack_float(oid));
            }
            if (subtype == RDF_OID::GenericSubType::DOUBLE) return unpack_double(oid);
            if (subtype == RDF_OID::GenericSubType::DECIMAL) return unpack_decimal(oid).to_double();
            return 0.0;
        }
        default:
            return 0.0;
        }
    }

    static float to_float(ObjectId oid) {
        return static_cast<float>(to_double(oid));
    }

    static Decimal to_decimal(ObjectId oid) {
        oid = numeric_operand(oid);
        auto subtype = RDF_OID::get_generic_subtype(oid);
        if (subtype == RDF_OID::GenericSubType::INTEGER) return Decimal(unpack_int(oid));
        if (subtype == RDF_OID::GenericSubType::DECIMAL) return unpack_decimal(oid);
        if (subtype == RDF_OID::GenericSubType::FLOAT) {
            const auto value = unpack_float(oid);
            if (value == 0) return Decimal();
            if (!std::isnormal(value)) throw std::out_of_range("Value is outside decimal range");
            return Decimal(value);
        }
        if (subtype == RDF_OID::GenericSubType::DOUBLE) {
            const auto value = unpack_double(oid);
            if (value == 0) return Decimal();
            if (!std::isnormal(value)) throw std::out_of_range("Value is outside decimal range");
            return Decimal(value);
        }
        return Decimal(int64_t(0));
    }

    static int64_t to_integer(ObjectId oid) {
        oid = numeric_operand(oid);
        auto subtype = RDF_OID::get_generic_subtype(oid);
        if (subtype == RDF_OID::GenericSubType::INTEGER) return unpack_int(oid);
        if (subtype == RDF_OID::GenericSubType::DECIMAL) {
            bool error = false;
            const auto value = unpack_decimal(oid).to_int(&error);
            if (error) throw std::out_of_range("Decimal does not fit int64");
            return value;
        }
        if (subtype == RDF_OID::GenericSubType::FLOAT ||
            subtype == RDF_OID::GenericSubType::DOUBLE) {
            const double value = to_double(oid);
            const double limit = std::ldexp(1.0, 63);
            if (!std::isfinite(value) || value < -limit || value >= limit) {
                throw std::out_of_range("Floating value does not fit int64");
            }
            return static_cast<int64_t>(value);
        }
        return 0;
    }

    static OpType calculate_optype(ObjectId oid) {
        oid = numeric_operand(oid);
        auto sub = RDF_OID::get_generic_subtype(oid);
        switch (sub) {
        case RDF_OID::GenericSubType::INTEGER:
            return OpType::INTEGER;
        case RDF_OID::GenericSubType::DECIMAL:
            return OpType::DECIMAL;
        case RDF_OID::GenericSubType::FLOAT:
            return OpType::FLOAT;
        case RDF_OID::GenericSubType::DOUBLE:
            return OpType::DOUBLE;
        case RDF_OID::GenericSubType::TENSOR_FLOAT:
            return OpType::TENSOR_FLOAT;
        case RDF_OID::GenericSubType::TENSOR_DOUBLE:
            return OpType::TENSOR_DOUBLE;
        default:
            return OpType::INVALID;
        }
    }

    static OpType calculate_optype(ObjectId lhs, ObjectId rhs) {
        lhs = numeric_operand(lhs);
        rhs = numeric_operand(rhs);
        if (RDF_OID::get_generic_type(lhs) != RDF_OID::GenericType::NUMERIC ||
            RDF_OID::get_generic_type(rhs) != RDF_OID::GenericType::NUMERIC) {
            return OpType::INVALID;
        }
        auto lhs_sub = RDF_OID::get_generic_subtype(lhs);
        auto rhs_sub = RDF_OID::get_generic_subtype(rhs);
        if (lhs_sub == RDF_OID::GenericSubType::INTEGER &&
            rhs_sub == RDF_OID::GenericSubType::INTEGER)
            return OpType::INTEGER;
        if ((lhs_sub == RDF_OID::GenericSubType::DECIMAL &&
            rhs_sub == RDF_OID::GenericSubType::INTEGER) ||
            (lhs_sub == RDF_OID::GenericSubType::INTEGER &&
            rhs_sub == RDF_OID::GenericSubType::DECIMAL) ||
            (lhs_sub == RDF_OID::GenericSubType::DECIMAL &&
            rhs_sub == RDF_OID::GenericSubType::DECIMAL))
            return OpType::DECIMAL;
        if (lhs_sub == RDF_OID::GenericSubType::DOUBLE ||
            rhs_sub == RDF_OID::GenericSubType::DOUBLE)
            return OpType::DOUBLE;
        if (lhs_sub == RDF_OID::GenericSubType::FLOAT || rhs_sub == RDF_OID::GenericSubType::FLOAT)
            return OpType::FLOAT;
        return OpType::INTEGER;
    }

    template<typename T>
    static tensor::Tensor<T> unpack_tensor(ObjectId oid) {
        if (oid.get_sub_type() != tensor::Tensor<T>::get_subtype()) {
            return tensor::Tensor<T>();
        }
        if (oid.get_mod() == ObjectId::MOD_INLINE) {
            return tensor::Tensor<T>();
        }
        uint64_t external_id = oid.id & ObjectId::MASK_EXTERNAL_ID;
        {
            std::lock_guard<std::mutex> lock(tensor_storage_mutex());
            auto& storage = extern_tensor_storage();
            auto it = storage.find(external_id);
            if (it != storage.end()) {
                const auto& vec = it->second;
                return tensor::Tensor<T>::from_bytes(vec.data(), vec.size());
            }
        }
        return tensor::Tensor<T>();
    }

    template<typename T>
    static tensor::Tensor<T> to_tensor(ObjectId oid) {
        return unpack_tensor<T>(oid);
    }
};

} // namespace SPARQL

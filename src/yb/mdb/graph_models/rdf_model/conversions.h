#pragma once
#include <string>
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

    static std::mutex& string_storage_mutex() {
        static std::mutex mtx;
        return mtx;
    }

    static std::unordered_map<uint64_t, std::string>& iri_storage() {
        static std::unordered_map<uint64_t, std::string> storage;
        return storage;
    }

    static std::unordered_map<uint64_t, std::vector<char>>& extern_tensor_storage() {
        static std::unordered_map<uint64_t, std::vector<char>> storage;
        return storage;
    }

    static std::mutex& tensor_storage_mutex() {
        static std::mutex mtx;
        return mtx;
    }

public:
    static void set_extern_string(uint64_t external_id, const std::string& str) {
        std::lock_guard<std::mutex> lock(string_storage_mutex());
        auto& storage = extern_string_storage();
        if (storage.find(external_id) == storage.end()) {
            storage[external_id] = str;
        }
    }

    static ObjectId pack_string_simple(const std::string& str) {
        if (str.size() <= ObjectId::STR_INLINE_BYTES) {
            return ObjectId(inline_string_7(str.c_str()) | ObjectId::MASK_STRING_SIMPLE_INLINED);
        }
        uint64_t hash = fnv1a_64(str);
        uint64_t content = hash & ObjectId::MASK_EXTERNAL_ID;
        {
            std::lock_guard<std::mutex> lock(string_storage_mutex());
            auto& storage = extern_string_storage();
            if (storage.find(content) == storage.end()) {
                storage[content] = str;
            }
        }
        return ObjectId(ObjectId::MASK_STRING_SIMPLE_EXTERN | content);
    }

    static ObjectId pack_string_xsd(const std::string& str) {
        if (str.size() <= ObjectId::STR_INLINE_BYTES) {
            return ObjectId(inline_string_7(str.c_str()) | ObjectId::MASK_STRING_XSD_INLINED);
        }
        uint64_t hash = fnv1a_64(str);
        uint64_t content = hash & ObjectId::MASK_EXTERNAL_ID;
        {
            std::lock_guard<std::mutex> lock(string_storage_mutex());
            auto& storage = extern_string_storage();
            if (storage.find(content) == storage.end()) {
                storage[content] = str;
            }
        }
        return ObjectId(ObjectId::MASK_STRING_XSD_EXTERN | content);
    }

    static ObjectId pack_string_datatype(const std::string& datatype, const std::string& str) {
        if (str.size() <= ObjectId::STR_DT_INLINE_BYTES) {
            return ObjectId(inline_string_5(str.c_str()) | ObjectId::MASK_STRING_DATATYPE_INLINED);
        }
        uint64_t hash = fnv1a_64(str);
        uint64_t content = hash & ObjectId::MASK_EXTERNAL_ID;
        {
            std::lock_guard<std::mutex> lock(string_storage_mutex());
            auto& storage = extern_string_storage();
            if (storage.find(content) == storage.end()) {
                storage[content] = str;
            }
        }
        return ObjectId(ObjectId::MASK_STRING_DATATYPE_EXTERN | content);
    }

    static ObjectId pack_string_lang(const std::string& lang, const std::string& str) {
        if (str.size() <= ObjectId::STR_LANG_INLINE_BYTES) {
            return ObjectId(inline_string_5(str.c_str()) | ObjectId::MASK_STRING_LANG_INLINED);
        }
        uint64_t hash = fnv1a_64(str);
        uint64_t content = hash & ObjectId::MASK_EXTERNAL_ID;
        {
            std::lock_guard<std::mutex> lock(string_storage_mutex());
            auto& storage = extern_string_storage();
            if (storage.find(content) == storage.end()) {
                storage[content] = str;
            }
        }
        return ObjectId(ObjectId::MASK_STRING_LANG_EXTERN | content);
    }

    static ObjectId try_pack_string_datatype(const std::string&, const std::string& str) {
        return pack_string_simple(str);
    }

    static ObjectId pack_empty_string() {
        return ObjectId(ObjectId::MASK_STRING_SIMPLE_INLINED);
    }

    static ObjectId pack_int(int64_t i) {
        return ObjectId((static_cast<uint64_t>(i) << 8) | ObjectId::MASK_POSITIVE_INT);
    }

    static ObjectId pack_float(float f) {
        uint64_t bits;
        std::memcpy(&bits, &f, sizeof(float));
        return ObjectId(ObjectId::MASK_FLOAT | (bits & 0x00'00FFFFFFFFFFFFUL));
    }

    static ObjectId pack_double(double d) {
        uint64_t bits;
        std::memcpy(&bits, &d, sizeof(double));
        uint64_t content = bits & ObjectId::MASK_EXTERNAL_ID;
        return ObjectId(ObjectId::MASK_DOUBLE_EXTERN | content);
    }

    static ObjectId pack_decimal(const Decimal& dec) {
        char buffer[Decimal::EXTERN_BUFFER_SIZE];
        dec.serialize_extern(buffer);
        uint64_t packed = 0;
        for (size_t i = 0; i < Decimal::EXTERN_BUFFER_SIZE && i < sizeof(uint64_t); i++) {
            packed |= static_cast<uint64_t>(static_cast<unsigned char>(buffer[i])) << (i * 8);
        }
        return ObjectId(ObjectId::MASK_DECIMAL_EXTERN | packed);
    }

    static ObjectId pack_decimal(double d) {
        return pack_decimal(Decimal(d));
    }

    static ObjectId pack_iri(const std::string& iri) {
        uint64_t hash = fnv1a_64(iri);
        uint64_t content = hash & ObjectId::MASK_IRI_CONTENT;
        ObjectId packed(ObjectId::MASK_IRI_INLINED | content);
        // Query constants also need their lexical form, even without a graph dictionary entry.
        std::lock_guard<std::mutex> lock(string_storage_mutex());
        iri_storage().emplace(packed.id, iri);
        return packed;
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
    static ObjectId try_unpack_xsd_integer(const std::string&, unsigned char) { return ObjectId::get_null(); }
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
        return static_cast<int64_t>((oid.id >> 8) & 0x0000FFFFFFFFFFFF);
    }

    static double unpack_double(ObjectId oid) {
        uint64_t bits = oid.id & ObjectId::MASK_EXTERNAL_ID;
        double d;
        std::memcpy(&d, &bits, sizeof(double));
        return d;
    }

    static Decimal unpack_decimal(ObjectId oid) {
        uint64_t data = oid.id & ObjectId::MASK_EXTERNAL_ID;
        char buffer[Decimal::EXTERN_BUFFER_SIZE];
        for (size_t i = 0; i < Decimal::EXTERN_BUFFER_SIZE; i++) {
            buffer[i] = static_cast<char>((data >> (i * 8)) & 0xFF);
        }
        return Decimal::from_external(buffer);
    }

    static std::pair<std::string, std::string> unpack_string_lang(ObjectId oid) {
        return {"", unpack_string(oid)};
    }

    static std::pair<std::string, std::string> unpack_string_datatype(ObjectId oid) {
        return {"", unpack_string(oid)};
    }

    static bool try_unpack_iri(ObjectId oid, std::string* text) {
        // Packed hashes and graph IDs are separate namespaces. Resolve the dictionary first.
        auto graph = g_pack_to_gmap.find(oid.id);
        if (graph != g_pack_to_gmap.end()) {
            auto term = g_gmap_to_uri.find(graph->second);
            if (term != g_gmap_to_uri.end()) {
                *text = term->second;
                return true;
            }
        }
        auto direct = g_gmap_to_uri.find(oid.id);
        if (direct != g_gmap_to_uri.end()) {
            *text = direct->second;
            return true;
        }
        if (oid.get_type() == ObjectId::MASK_IRI_EXTERN) {
            auto external = g_gmap_to_uri.find(oid.id & ObjectId::MASK_IRI_CONTENT);
            if (external != g_gmap_to_uri.end()) {
                *text = external->second;
                return true;
            }
        }
        std::lock_guard<std::mutex> lock(string_storage_mutex());
        auto stored = iri_storage().find(oid.id);
        if (stored == iri_storage().end()) return false;
        *text = stored->second;
        return true;
    }

    static std::string unpack_iri(ObjectId oid) {
        std::string text;
        try_unpack_iri(oid, &text);
        return text;
    }

    static ObjectId to_boolean(ObjectId oid) {
        uint64_t mask = oid.id & ObjectId::TYPE_MASK;
        if (oid.id == ObjectId::BOOL_TRUE || oid.id == ObjectId::BOOL_FALSE) {
            return oid;
        }
        if (mask == ObjectId::MASK_BOOL) {
            return oid;
        }
        if (mask == ObjectId::MASK_STRING_SIMPLE_INLINED ||
            mask == ObjectId::MASK_STRING_SIMPLE_EXTERN ||
            mask == ObjectId::MASK_STRING_SIMPLE_TMP) {
            return oid.id == ObjectId::MASK_STRING_SIMPLE_INLINED ?
                ObjectId(ObjectId::BOOL_FALSE) : ObjectId(ObjectId::BOOL_TRUE);
        }
        if (mask == ObjectId::MASK_POSITIVE_INT) {
            int64_t val = unpack_int(oid);
            return ObjectId(ObjectId::MASK_BOOL | (val != 0 ? 1ULL : 0ULL));
        }
        return ObjectId(ObjectId::BOOL_FALSE);
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
            return std::to_string(unpack_float(oid));
        case RDF_OID::Type::DOUBLE64_EXTERN:
        case RDF_OID::Type::DOUBLE64_TMP:
            return std::to_string(unpack_double(oid));
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

    static ObjectId string_simple_to_xsd(ObjectId oid) {
        return ObjectId(
            (oid.id & ~ObjectId::SUB_TYPE_MASK) | ObjectId::MASK_STRING_XSD_INLINED
        );
    }

    static double to_double(ObjectId oid) {
        switch (RDF_OID::get_generic_type(oid)) {
        case RDF_OID::GenericType::NUMERIC: {
            auto subtype = RDF_OID::get_generic_subtype(oid);
            if (subtype == RDF_OID::GenericSubType::INTEGER) return static_cast<double>(unpack_int(oid));
            if (subtype == RDF_OID::GenericSubType::FLOAT) return static_cast<double>(unpack_float(oid));
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
        auto subtype = RDF_OID::get_generic_subtype(oid);
        if (subtype == RDF_OID::GenericSubType::INTEGER) return Decimal(unpack_int(oid));
        if (subtype == RDF_OID::GenericSubType::DECIMAL) return unpack_decimal(oid);
        if (subtype == RDF_OID::GenericSubType::FLOAT) return Decimal(unpack_float(oid));
        if (subtype == RDF_OID::GenericSubType::DOUBLE) return Decimal(unpack_double(oid));
        return Decimal(int64_t(0));
    }

    static int64_t to_integer(ObjectId oid) {
        auto subtype = RDF_OID::get_generic_subtype(oid);
        if (subtype == RDF_OID::GenericSubType::INTEGER) return unpack_int(oid);
        if (subtype == RDF_OID::GenericSubType::DECIMAL) {
            bool error = false;
            return unpack_decimal(oid).to_int(&error);
        }
        if (subtype == RDF_OID::GenericSubType::FLOAT) return static_cast<int64_t>(unpack_float(oid));
        if (subtype == RDF_OID::GenericSubType::DOUBLE) return static_cast<int64_t>(unpack_double(oid));
        return 0;
    }

    static OpType calculate_optype(ObjectId oid) {
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
        auto lhs_sub = RDF_OID::get_generic_subtype(lhs);
        auto rhs_sub = RDF_OID::get_generic_subtype(rhs);
        if (lhs_sub == RDF_OID::GenericSubType::INTEGER && rhs_sub == RDF_OID::GenericSubType::INTEGER)
            return OpType::INTEGER;
        if ((lhs_sub == RDF_OID::GenericSubType::DECIMAL && rhs_sub == RDF_OID::GenericSubType::INTEGER) ||
            (lhs_sub == RDF_OID::GenericSubType::INTEGER && rhs_sub == RDF_OID::GenericSubType::DECIMAL) ||
            (lhs_sub == RDF_OID::GenericSubType::DECIMAL && rhs_sub == RDF_OID::GenericSubType::DECIMAL))
            return OpType::DECIMAL;
        if (lhs_sub == RDF_OID::GenericSubType::FLOAT || rhs_sub == RDF_OID::GenericSubType::FLOAT)
            return OpType::FLOAT;
        if (lhs_sub == RDF_OID::GenericSubType::DOUBLE || rhs_sub == RDF_OID::GenericSubType::DOUBLE)
            return OpType::DOUBLE;
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

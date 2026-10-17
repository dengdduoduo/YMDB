#pragma once
#include <cassert>
#include <cstdint>
#include <string>
#include <unordered_map>
#include "graph_models/object_id.h"

extern std::unordered_map<uint64_t, std::string> g_gmap_to_uri;
extern std::unordered_map<uint64_t, ObjectId> g_gmap_to_pack;
extern std::unordered_map<uint64_t, uint64_t> g_pack_to_gmap;

namespace RDF_OID {

inline bool is_gmap_iri(uint64_t id) {
    if (id == 0) return false;
    auto it = g_gmap_to_uri.find(id);
    if (it == g_gmap_to_uri.end()) return false;
    const std::string& s = it->second;
    return !s.empty() && s.find('|') == std::string::npos;
}
    enum class GenericType : uint8_t {
        NULL_ID = 0x00, BLANK, IRI, STRING, NUMERIC, DATE, BOOL, PATH, TENSOR
    };
    enum class GenericSubType : uint64_t {
        NULL_ID = 0x00, BLANK, IRI,
        STRING_SIMPLE, STRING_XSD, STRING_LANG, STRING_DATATYPE,
        INTEGER, FLOAT, DECIMAL, DOUBLE,
        DATE, DATETIME, TIME,
        BOOL,
        PATH,
        TENSOR_INT, TENSOR_FLOAT, TENSOR_DOUBLE
    };
    enum class Type {
        NULL_ID = 0x00,
        BLANK_INLINED,
        BLANK_TMP,
        IRI_INLINE,
        IRI_INLINE_INT_SUFFIX,
        IRI_EXTERN,
        IRI_TMP,
        STRING_SIMPLE_INLINE,
        STRING_SIMPLE_EXTERN,
        STRING_SIMPLE_TMP,
        STRING_XSD_INLINE,
        STRING_XSD_EXTERN,
        STRING_XSD_TMP,
        STRING_LANG_INLINE,
        STRING_LANG_EXTERN,
        STRING_LANG_TMP,
        STRING_DATATYPE_INLINE,
        STRING_DATATYPE_EXTERN,
        STRING_DATATYPE_TMP,
        INT56_INLINE,
        INT64_EXTERN,
        INT64_TMP,
        FLOAT32,
        DOUBLE64_EXTERN,
        DOUBLE64_TMP,
        DECIMAL_INLINE,
        DECIMAL_EXTERN,
        DECIMAL_TMP,
        DATE,
        TIME,
        DATETIME,
        DATETIMESTAMP,
        BOOL,
        PATH,
        TENSOR_FLOAT_INLINE,
        TENSOR_FLOAT_EXTERN,
        TENSOR_FLOAT_TMP,
        TENSOR_DOUBLE_INLINE,
        TENSOR_DOUBLE_EXTERN,
        TENSOR_DOUBLE_TMP
    };

    inline Type get_type(ObjectId oid) {
        uint8_t top_byte = oid.id >> 56;
        switch (top_byte) {
            case 0x00:
                if (oid.id == 0) return Type::NULL_ID;
                if (is_gmap_iri(oid.id)) return Type::IRI_INLINE;
                return Type::NULL_ID;
            case 0x01: return Type::BLANK_INLINED;
            case 0x02: return Type::BLANK_TMP;
            case 0x30: return Type::IRI_INLINE;
            case 0x31: return Type::IRI_EXTERN;
            case 0x32: return Type::IRI_TMP;
            case 0x40: return Type::STRING_SIMPLE_INLINE;
            case 0x41: return Type::STRING_SIMPLE_EXTERN;
            case 0x42: return Type::STRING_SIMPLE_TMP;
            case 0x44: return Type::STRING_XSD_INLINE;
            case 0x45: return Type::STRING_XSD_EXTERN;
            case 0x46: return Type::STRING_XSD_TMP;
            case 0x48: return Type::STRING_LANG_INLINE;
            case 0x49: return Type::STRING_LANG_EXTERN;
            case 0x4A: return Type::STRING_LANG_TMP;
            case 0x4C: return Type::STRING_DATATYPE_INLINE;
            case 0x4D: return Type::STRING_DATATYPE_EXTERN;
            case 0x4E: return Type::STRING_DATATYPE_TMP;
            case 0x50: return Type::INT56_INLINE;
            case 0x51: return Type::INT64_EXTERN;
            case 0x52: return Type::INT64_TMP;
            case 0x54: return Type::DECIMAL_INLINE;
            case 0x55: return Type::DECIMAL_EXTERN;
            case 0x56: return Type::DECIMAL_TMP;
            case 0x58: return Type::FLOAT32;
            case 0x5C: return Type::DOUBLE64_EXTERN;
            case 0x5D: return Type::DOUBLE64_EXTERN;
            case 0x5E: return Type::DOUBLE64_TMP;
            case 0x60: return Type::DATE;
            case 0x64: return Type::TIME;
            case 0x68: return Type::DATETIME;
            case 0x6C: return Type::DATETIMESTAMP;
            case 0x70: return Type::BOOL;
            case 0x90: return Type::PATH;
            case 0xB0: return Type::TENSOR_FLOAT_INLINE;
            case 0xB1: return Type::TENSOR_FLOAT_EXTERN;
            case 0xB2: return Type::TENSOR_FLOAT_TMP;
            case 0xB4: return Type::TENSOR_DOUBLE_INLINE;
            case 0xB5: return Type::TENSOR_DOUBLE_EXTERN;
            case 0xB6: return Type::TENSOR_DOUBLE_TMP;
            default:
                if (oid.id == ObjectId::BOOL_TRUE || oid.id == ObjectId::BOOL_FALSE) {
                    return Type::BOOL;
                }
                return Type::NULL_ID;
        }
    }

    inline GenericType get_generic_type(ObjectId oid) {
        switch (get_type(oid)) {
            case Type::NULL_ID: return GenericType::NULL_ID;
            case Type::BLANK_INLINED:
            case Type::BLANK_TMP: return GenericType::BLANK;
            case Type::IRI_INLINE:
            case Type::IRI_INLINE_INT_SUFFIX:
            case Type::IRI_EXTERN:
            case Type::IRI_TMP: return GenericType::IRI;
            case Type::STRING_SIMPLE_INLINE:
            case Type::STRING_SIMPLE_EXTERN:
            case Type::STRING_SIMPLE_TMP:
            case Type::STRING_XSD_INLINE:
            case Type::STRING_XSD_EXTERN:
            case Type::STRING_XSD_TMP:
            case Type::STRING_LANG_INLINE:
            case Type::STRING_LANG_EXTERN:
            case Type::STRING_LANG_TMP:
            case Type::STRING_DATATYPE_INLINE:
            case Type::STRING_DATATYPE_EXTERN:
            case Type::STRING_DATATYPE_TMP: return GenericType::STRING;
            case Type::INT56_INLINE:
            case Type::INT64_EXTERN:
            case Type::INT64_TMP:
            case Type::FLOAT32:
            case Type::DOUBLE64_EXTERN:
            case Type::DOUBLE64_TMP:
            case Type::DECIMAL_INLINE:
            case Type::DECIMAL_EXTERN:
            case Type::DECIMAL_TMP: return GenericType::NUMERIC;
            case Type::DATE:
            case Type::TIME:
            case Type::DATETIME:
            case Type::DATETIMESTAMP: return GenericType::DATE;
            case Type::BOOL: return GenericType::BOOL;
            case Type::PATH: return GenericType::PATH;
            case Type::TENSOR_FLOAT_INLINE:
            case Type::TENSOR_FLOAT_EXTERN:
            case Type::TENSOR_FLOAT_TMP:
            case Type::TENSOR_DOUBLE_INLINE:
            case Type::TENSOR_DOUBLE_EXTERN:
            case Type::TENSOR_DOUBLE_TMP: return GenericType::TENSOR;
            default: return GenericType::NULL_ID;
        }
    }

    inline GenericSubType get_generic_subtype(ObjectId oid) {
        switch (get_type(oid)) {
            case Type::NULL_ID: return GenericSubType::NULL_ID;
            case Type::BLANK_INLINED:
            case Type::BLANK_TMP: return GenericSubType::BLANK;
            case Type::IRI_INLINE:
            case Type::IRI_INLINE_INT_SUFFIX:
            case Type::IRI_EXTERN:
            case Type::IRI_TMP: return GenericSubType::IRI;
            case Type::STRING_SIMPLE_INLINE:
            case Type::STRING_SIMPLE_EXTERN:
            case Type::STRING_SIMPLE_TMP: return GenericSubType::STRING_SIMPLE;
            case Type::STRING_XSD_INLINE:
            case Type::STRING_XSD_EXTERN:
            case Type::STRING_XSD_TMP: return GenericSubType::STRING_XSD;
            case Type::STRING_LANG_INLINE:
            case Type::STRING_LANG_EXTERN:
            case Type::STRING_LANG_TMP: return GenericSubType::STRING_LANG;
            case Type::STRING_DATATYPE_INLINE:
            case Type::STRING_DATATYPE_EXTERN:
            case Type::STRING_DATATYPE_TMP: return GenericSubType::STRING_DATATYPE;
            case Type::INT56_INLINE:
            case Type::INT64_EXTERN:
            case Type::INT64_TMP: return GenericSubType::INTEGER;
            case Type::FLOAT32: return GenericSubType::FLOAT;
            case Type::DOUBLE64_EXTERN:
            case Type::DOUBLE64_TMP: return GenericSubType::DOUBLE;
            case Type::DECIMAL_INLINE:
            case Type::DECIMAL_EXTERN:
            case Type::DECIMAL_TMP: return GenericSubType::DECIMAL;
            case Type::DATE:
            case Type::TIME:
            case Type::DATETIME:
            case Type::DATETIMESTAMP: return GenericSubType::DATE;
            case Type::BOOL: return GenericSubType::BOOL;
            case Type::PATH: return GenericSubType::PATH;
            case Type::TENSOR_FLOAT_INLINE:
            case Type::TENSOR_FLOAT_EXTERN:
            case Type::TENSOR_FLOAT_TMP: return GenericSubType::TENSOR_FLOAT;
            case Type::TENSOR_DOUBLE_INLINE:
            case Type::TENSOR_DOUBLE_EXTERN:
            case Type::TENSOR_DOUBLE_TMP: return GenericSubType::TENSOR_DOUBLE;
            default: return GenericSubType::NULL_ID;
        }
    }

    inline GenericSubType get_generic_sub_type(ObjectId oid) { return get_generic_subtype(oid); }

    inline ObjectId get_oid(const std::string&) { return ObjectId::get_null(); }
}

#pragma once
#include <cstdint>
#include <ostream>
#include <type_traits>
#include "graph_models/rdf_model/term_identity.h"

class ObjectId {
public:
    static constexpr uint64_t BOOL_TRUE   = UINT64_MAX - 2;
    static constexpr uint64_t BOOL_FALSE  = UINT64_MAX - 3;
    static constexpr uint64_t NULL_ID = UINT64_MAX;
    static constexpr uint64_t OBJECT_ID_NOT_FOUND = UINT64_MAX - 1;
    static constexpr uint64_t MASK_NOT_FOUND = UINT64_MAX;

    static constexpr uint64_t GENERIC_TYPE_MASK             = 0xF0'00000000000000UL;
    static constexpr uint64_t MASK_TENSOR                   = 0xB0'00000000000000UL;
    static constexpr uint64_t MASK_TENSOR_FLOAT             = 0xB0'00000000000000UL;
    static constexpr uint64_t MASK_TENSOR_FLOAT_INLINED    = 0xB0'00000000000000UL;
    static constexpr uint64_t MASK_TENSOR_FLOAT_EXTERN     = 0xB1'00000000000000UL;
    static constexpr uint64_t MASK_TENSOR_FLOAT_TMP        = 0xB2'00000000000000UL;
    static constexpr uint64_t MASK_TENSOR_DOUBLE            = 0xB4'00000000000000UL;
    static constexpr uint64_t MASK_TENSOR_DOUBLE_INLINED   = 0xB4'00000000000000UL;
    static constexpr uint64_t MASK_TENSOR_DOUBLE_EXTERN    = 0xB5'00000000000000UL;
    static constexpr uint64_t MASK_TENSOR_DOUBLE_TMP       = 0xB6'00000000000000UL;

    static constexpr uint64_t VALUE_MASK        = 0x00'FFFFFFFFFFFFFFUL;
    static constexpr uint64_t TYPE_MASK         = 0xFF'00000000000000UL;
    static constexpr uint64_t SUB_TYPE_MASK     = 0xFC'00000000000000UL;
    static constexpr uint64_t MOD_MASK          = 0x03'00000000000000UL;
    static constexpr uint64_t MOD_INLINE        = 0x00'00000000000000UL;
    static constexpr uint64_t MOD_EXTERNAL      = 0x01'00000000000000UL;
    static constexpr uint64_t MOD_TMP           = 0x02'00000000000000UL;

    static constexpr uint64_t MASK_IRI                     = 0x30'00000000000000UL;
    static constexpr uint64_t MASK_IRI_INLINED             = 0x30'00000000000000UL;
    static constexpr uint64_t MASK_IRI_EXTERN              = 0x31'00000000000000UL;
    static constexpr uint64_t MASK_IRI_TMP                 = 0x32'00000000000000UL;
    static constexpr uint64_t MASK_IRI_CONTENT             = 0x00'00FFFFFFFFFFFFUL;

    static constexpr uint64_t MASK_DT                      = 0x60'00000000000000UL;
    static constexpr uint64_t MASK_DT_DATE                 = 0x60'00000000000000UL;
    static constexpr uint64_t MASK_DT_TIME                 = 0x64'00000000000000UL;
    static constexpr uint64_t MASK_DT_DATETIME             = 0x68'00000000000000UL;
    static constexpr uint64_t MASK_DT_DATETIMESTAMP        = 0x6C'00000000000000UL;

    static constexpr uint64_t MASK_STRING_SIMPLE_INLINED   = 0x40'00000000000000UL;
    static constexpr uint64_t MASK_STRING_SIMPLE_EXTERN    = 0x41'00000000000000UL;
    static constexpr uint64_t MASK_STRING_SIMPLE_TMP       = 0x42'00000000000000UL;
    static constexpr uint64_t MASK_STRING_XSD_INLINED      = 0x44'00000000000000UL;
    static constexpr uint64_t MASK_STRING_XSD_EXTERN       = 0x45'00000000000000UL;
    static constexpr uint64_t MASK_STRING_XSD_TMP          = 0x46'00000000000000UL;
    static constexpr uint64_t MASK_STRING_LANG_INLINED     = 0x48'00000000000000UL;
    static constexpr uint64_t MASK_STRING_LANG_EXTERN      = 0x49'00000000000000UL;
    static constexpr uint64_t MASK_STRING_LANG_TMP         = 0x4A'00000000000000UL;
    static constexpr uint64_t MASK_STRING_DATATYPE_INLINED = 0x4C'00000000000000UL;
    static constexpr uint64_t MASK_STRING_DATATYPE_EXTERN  = 0x4D'00000000000000UL;
    static constexpr uint64_t MASK_STRING_DATATYPE_TMP     = 0x4E'00000000000000UL;

    static constexpr uint64_t MASK_POSITIVE_INT            = 0x50'00000000000000UL;
    static constexpr uint64_t MASK_INT64_EXTERN            = 0x51'00000000000000UL;
    static constexpr uint64_t MASK_INT64_TMP               = 0x52'00000000000000UL;

    static constexpr uint64_t MASK_FLOAT                   = 0x58'00000000000000UL;

    static constexpr uint64_t MASK_DOUBLE_EXTERN           = 0x5C'00000000000000UL;
    static constexpr uint64_t MASK_DOUBLE_TMP              = 0x5E'00000000000000UL;

    static constexpr uint64_t MASK_DECIMAL_INLINED         = 0x54'00000000000000UL;
    static constexpr uint64_t MASK_DECIMAL_EXTERN          = 0x55'00000000000000UL;
    static constexpr uint64_t MASK_DECIMAL_TMP             = 0x56'00000000000000UL;

    static constexpr uint64_t MASK_BOOL                    = 0x70'00000000000000UL;

    static constexpr uint64_t MASK_BLANK_INLINED           = 0x01'00000000000000UL;
    static constexpr uint64_t MASK_BLANK_TMP               = 0x02'00000000000000UL;

    static constexpr uint64_t MASK_PATH                    = 0x90'00000000000000UL;

    static constexpr uint64_t MASK_EXTERNAL_ID             = 0x00'00FFFFFFFFFFFFUL;

    static constexpr int STR_INLINE_BYTES = 7;
    static constexpr int STR_DT_INLINE_BYTES = 5;
    static constexpr int STR_LANG_INLINE_BYTES = 5;
    static constexpr int IRI_INLINE_BYTES = 6;

    uint64_t id;

    constexpr ObjectId() noexcept : id(NULL_ID) {}
    explicit constexpr ObjectId(uint64_t id) noexcept : id(id) {}

    static ObjectId get_null() { return ObjectId(NULL_ID); }
    static ObjectId get_not_found() { return ObjectId(OBJECT_ID_NOT_FOUND); }

    bool is_null() const { return id == NULL_ID; }
    bool is_not_found() const { return id == OBJECT_ID_NOT_FOUND; }
    bool is_true() const {
        if (id == BOOL_TRUE) return true;
        if (get_type() != MASK_BOOL) return false;
        auto term = SPARQL::RdfTermRegistry::Find(id);
        return term && (term->lexical == "true" || term->lexical == "1");
    }
    bool is_false() const {
        if (id == BOOL_FALSE) return true;
        if (get_type() != MASK_BOOL) return false;
        auto term = SPARQL::RdfTermRegistry::Find(id);
        return term && (term->lexical == "false" || term->lexical == "0");
    }

    inline bool operator<(ObjectId rhs) const noexcept  { return id < rhs.id; }
    inline bool operator<=(ObjectId rhs) const noexcept { return id <= rhs.id; }
    inline bool operator>(ObjectId rhs) const noexcept  { return id > rhs.id; }
    inline bool operator>=(ObjectId rhs) const noexcept { return id >= rhs.id; }
    inline bool operator==(ObjectId rhs) const noexcept { return id == rhs.id; }
    inline bool operator!=(ObjectId rhs) const noexcept { return id != rhs.id; }

    inline ObjectId& operator=(uint64_t new_id) { id = new_id; return *this; }

    inline uint64_t get_type() const noexcept {
        return id & TYPE_MASK;
    }

    inline uint64_t get_sub_type() const noexcept {
        return id & SUB_TYPE_MASK;
    }

    inline uint64_t get_mod() const noexcept {
        return id & MOD_MASK;
    }

    inline uint64_t get_value() const noexcept {
        return id & VALUE_MASK;
    }

    inline bool is_tmp() const noexcept {
        return (id & MOD_MASK) == MOD_TMP;
    }
};

std::ostream& operator<<(std::ostream& os, ObjectId oid);

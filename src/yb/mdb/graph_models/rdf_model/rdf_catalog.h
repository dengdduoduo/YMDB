#pragma once
#include <cstdint>
#include <string>
#include "storage/index/hnsw/hnsw_index_manager.h"
#include "storage/index/text_search/text_index_manager.h"
struct RdfCatalog {
    uint64_t get_predicate_count(uint64_t) const { return 0; }
    uint64_t get_subject_count(uint64_t) const { return 0; }
    uint64_t get_object_count(uint64_t) const { return 0; }
    uint64_t get_equal_spo_count() const { return 0; }
    uint64_t get_equal_sp_count() const { return 0; }
    uint64_t get_equal_so_count() const { return 0; }
    uint64_t get_equal_po_count() const { return 0; }
    uint64_t get_triples_count() const { return 0; }
    uint64_t get_distinct_subjects() const { return 0; }
    uint64_t get_distinct_predicates() const { return 0; }
    uint64_t get_distinct_objects() const { return 0; }
    HNSW::HNSWIndexManager hnsw_index_manager;
    TextSearch::TextIndexManager text_index_manager;
};

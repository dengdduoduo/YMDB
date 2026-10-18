// Portions Copyright (c) YugabyteDB, Inc.

#pragma once

#include <cstdint>
#include <optional>

#include "storage/index/hnsw/hnsw_index_manager.h"
#include "storage/index/text_search/text_index_manager.h"

struct RdfCatalog {
  // Counts refer to the last collected DocDB statistics and take storage IDs.
  // Missing/invalidated statistics are unknown; a present zero is a known count.
  std::optional<uint64_t> get_predicate_count(uint64_t id) const;
  std::optional<uint64_t> get_subject_count(uint64_t id) const;
  std::optional<uint64_t> get_object_count(uint64_t id) const;
  std::optional<uint64_t> get_equal_spo_count() const;
  std::optional<uint64_t> get_equal_sp_count() const;
  std::optional<uint64_t> get_equal_so_count() const;
  std::optional<uint64_t> get_equal_po_count() const;
  std::optional<uint64_t> get_triples_count() const;
  std::optional<uint64_t> get_distinct_subjects() const;
  std::optional<uint64_t> get_distinct_predicates() const;
  std::optional<uint64_t> get_distinct_objects() const;

  HNSW::HNSWIndexManager hnsw_index_manager;
  TextSearch::TextIndexManager text_index_manager;
};

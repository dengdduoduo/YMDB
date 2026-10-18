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

#include "yb/mdb/graph_models/rdf_model/rdf_catalog.h"

#include "yb/mdb/model/rdf_yb_model.h"

namespace {

std::optional<uint64_t> TermCount(size_t column, uint64_t id) {
  const auto* stats = yb::mdb::RdfYBModel::Instance().GetStatistics();
  if (stats == nullptr) return std::nullopt;
  const auto& counts = stats->term_counts[column];
  const auto found = counts.find(id);
  return found == counts.end() ? 0 : found->second;
}

std::optional<uint64_t> EqualityCount(size_t equality) {
  const auto* stats = yb::mdb::RdfYBModel::Instance().GetStatistics();
  if (stats == nullptr) return std::nullopt;
  return stats->equal_rows[equality];
}

std::optional<uint64_t> DistinctCount(size_t column) {
  const auto* stats = yb::mdb::RdfYBModel::Instance().GetStatistics();
  if (stats == nullptr) return std::nullopt;
  return stats->term_counts[column].size();
}

}  // namespace

std::optional<uint64_t> RdfCatalog::get_predicate_count(uint64_t id) const {
  return TermCount(1, id);
}

std::optional<uint64_t> RdfCatalog::get_subject_count(uint64_t id) const {
  return TermCount(0, id);
}

std::optional<uint64_t> RdfCatalog::get_object_count(uint64_t id) const {
  return TermCount(2, id);
}

std::optional<uint64_t> RdfCatalog::get_equal_spo_count() const {
  return EqualityCount(3);
}

std::optional<uint64_t> RdfCatalog::get_equal_sp_count() const {
  return EqualityCount(0);
}

std::optional<uint64_t> RdfCatalog::get_equal_so_count() const {
  return EqualityCount(1);
}

std::optional<uint64_t> RdfCatalog::get_equal_po_count() const {
  return EqualityCount(2);
}

std::optional<uint64_t> RdfCatalog::get_triples_count() const {
  const auto* stats = yb::mdb::RdfYBModel::Instance().GetStatistics();
  if (stats == nullptr) return std::nullopt;
  return stats->rows;
}

std::optional<uint64_t> RdfCatalog::get_distinct_subjects() const {
  return DistinctCount(0);
}

std::optional<uint64_t> RdfCatalog::get_distinct_predicates() const {
  return DistinctCount(1);
}

std::optional<uint64_t> RdfCatalog::get_distinct_objects() const {
  return DistinctCount(2);
}

// Copyright (c) YugabyteDB, Inc.
#pragma once

#include <algorithm>
#include <utility>
#include <vector>

#include "graph_models/rdf_model/comparisons.h"
#include "yb/mdb/query/distributed/fragment_plan.h"

namespace yb::mdb {

// Keeps full typed bindings, including hidden sort variables and duplicate rows.
// Admission and dictionary validation happen before Add, even when rows == 0.
class GlobalTopK {
 public:
  explicit GlobalTopK(const GlobalTopKSpec& spec) : spec_(spec) {}
  size_t size() const { return heap_.size(); }

  void Add(std::vector<::ObjectId> row) {
    if (!spec_.rows) return;
    auto before = [&](const auto& lhs, const auto& rhs) { return Before(lhs, rhs); };
    if (heap_.size() < spec_.rows) {
      heap_.push_back(std::move(row));
    } else {
      if (!Before(row, heap_.front())) return;
      std::pop_heap(heap_.begin(), heap_.end(), before);
      heap_.back() = std::move(row);
    }
    std::push_heap(heap_.begin(), heap_.end(), before);
  }

  std::vector<::ObjectId> ReleaseRows() {
    std::vector<::ObjectId> result;
    if (!heap_.empty()) result.reserve(heap_.size() * heap_.front().size());
    for (const auto& row : heap_) result.insert(result.end(), row.begin(), row.end());
    heap_.clear();
    return result;
  }

 private:
  bool Before(const std::vector<::ObjectId>& lhs, const std::vector<::ObjectId>& rhs) const {
    for (size_t i = 0; i < spec_.key_columns.size(); ++i) {
      const auto column = spec_.key_columns[i];
      const auto order = SPARQL::Comparisons::compare_for_order(lhs[column], rhs[column]);
      if (order) return spec_.ascending[i] ? order < 0 : order > 0;
    }
    return false;
  }

  const GlobalTopKSpec spec_;
  std::vector<std::vector<::ObjectId>> heap_;
};

}  // namespace yb::mdb

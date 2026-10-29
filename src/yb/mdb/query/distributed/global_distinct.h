// Copyright (c) YugabyteDB, Inc.
#pragma once

#include <algorithm>
#include <map>
#include <utility>
#include <vector>

#include "graph_models/object_id.h"
#include "yb/mdb/query/distributed/fragment_plan.h"

namespace yb::mdb {

// Typed ObjectId tuple ordering matches the existing DistinctHash set exactly.
// Keep full representatives for the original Values/order/projection finalizer.
class GlobalDistinct {
 public:
  explicit GlobalDistinct(const GlobalDistinctSpec& spec) : spec_(spec) {}
  size_t size() const { return rows_.size(); }
  uint64_t reserved_bytes() const { return reserved_; }

  Status Add(std::vector<::ObjectId> row, uint64_t budget) {
    const uint64_t capacity = budget / spec_.row_reservation;
    if (rows_.size() >= capacity)
      return STATUS(RuntimeError, "MDB global DISTINCT input budget exceeded");
    std::vector<::ObjectId> key;
    key.reserve(spec_.key_columns.size());
    for (auto column : spec_.key_columns) key.push_back(row[column]);
    const auto found = rows_.lower_bound(key);
    const bool is_new = found == rows_.end() || found->first != key;
    // A candidate/key coexists with retained representatives. New keys also
    // need space while map insertion and the later finalizer retain that row.
    const uint64_t slots = rows_.size() + 1 + is_new;
    if (slots > capacity)
      return STATUS(RuntimeError, "MDB global DISTINCT input budget exceeded");
    reserved_ = std::max(reserved_, slots * spec_.row_reservation);
    if (is_new) rows_.emplace_hint(found, std::move(key), std::move(row));
    return Status::OK();
  }

  std::vector<::ObjectId> ReleaseRows() {
    std::vector<::ObjectId> result;
    if (!rows_.empty()) result.reserve(rows_.size() * rows_.begin()->second.size());
    for (const auto& entry : rows_)
      result.insert(result.end(), entry.second.begin(), entry.second.end());
    rows_.clear();
    return result;
  }

 private:
  const GlobalDistinctSpec spec_;
  std::map<std::vector<::ObjectId>, std::vector<::ObjectId>> rows_;
  uint64_t reserved_ = 0;
};

}  // namespace yb::mdb

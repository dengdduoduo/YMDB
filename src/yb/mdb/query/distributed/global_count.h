// Copyright (c) YugabyteDB, Inc.
#pragma once

#include <algorithm>
#include <limits>
#include <map>
#include <utility>
#include <vector>

#include "graph_models/rdf_model/conversions.h"
#include "yb/mdb/query/distributed/fragment_plan.h"

namespace yb::mdb {

// Group identity matches the original internal ObjectId sort and aggregation.
class GlobalCount {
 public:
  explicit GlobalCount(const GlobalCountSpec& spec) : spec_(spec) {}
  size_t size() const { return groups_.size(); }
  uint64_t reserved_bytes() const { return reserved_; }

  Status AdmitCandidate(uint64_t budget) const {
    if (groups_.size() >= budget / spec_.row_reservation)
      return STATUS(RuntimeError, "MDB global COUNT input budget exceeded");
    return Status::OK();
  }

  Status Initialize(uint64_t budget) {
    if (spec_.grouped) return Status::OK();
    RETURN_NOT_OK(Reserve(2, budget));
    groups_.emplace(std::vector<::ObjectId>{}, std::vector<uint64_t>(spec_.count_columns.size(), 0));
    return Status::OK();
  }

  static Status Increment(uint64_t* value) {
    if (*value >= static_cast<uint64_t>(std::numeric_limits<int64_t>::max()))
      return STATUS(RuntimeError, "MDB global COUNT exceeds signed integer range");
    ++*value;
    return Status::OK();
  }

  Status Add(const std::vector<::ObjectId>& row, uint64_t budget) {
    RETURN_NOT_OK(AdmitCandidate(budget));
    std::vector<::ObjectId> key;
    key.reserve(spec_.group_columns.size());
    for (auto column : spec_.group_columns)
      key.push_back(column < 0 ? ::ObjectId::get_null() : row[column]);
    auto found = groups_.lower_bound(key);
    const bool is_new = found == groups_.end() || found->first != key;
    RETURN_NOT_OK(Reserve(groups_.size() + 1 + is_new, budget));
    if (is_new) found = groups_.emplace_hint(found, std::move(key),
        std::vector<uint64_t>(spec_.count_columns.size(), 0));
    for (size_t i = 0; i < spec_.count_columns.size(); ++i) {
      const auto column = spec_.count_columns[i];
      if (column == -2 || (column >= 0 && !row[column].is_null()))
        RETURN_NOT_OK(Increment(&found->second[i]));
    }
    return Status::OK();
  }

  std::vector<::ObjectId> ReleaseRows() {
    std::vector<::ObjectId> result;
    result.reserve(groups_.size() * (spec_.group_columns.size() + spec_.count_columns.size()));
    for (const auto& group : groups_) {
      result.insert(result.end(), group.first.begin(), group.first.end());
      for (auto count : group.second)
        result.push_back(SPARQL::Conversions::pack_int(static_cast<int64_t>(count)));
    }
    groups_.clear();
    return result;
  }

 private:
  Status Reserve(uint64_t slots, uint64_t budget) {
    if (slots > budget / spec_.row_reservation)
      return STATUS(RuntimeError, "MDB global COUNT input budget exceeded");
    reserved_ = std::max(reserved_, slots * spec_.row_reservation);
    return Status::OK();
  }

  const GlobalCountSpec spec_;
  std::map<std::vector<::ObjectId>, std::vector<uint64_t>> groups_;
  uint64_t reserved_ = 0;
};

}  // namespace yb::mdb

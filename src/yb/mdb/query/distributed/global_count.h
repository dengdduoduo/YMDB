// Copyright (c) YugabyteDB, Inc.
#pragma once

#include <algorithm>
#include <limits>
#include <map>
#include <set>
#include <utility>
#include <vector>

#include "graph_models/rdf_model/comparisons.h"
#include "graph_models/rdf_model/conversions.h"
#include "yb/mdb/query/distributed/fragment_plan.h"

namespace yb::mdb {

// Group and DISTINCT identities match the original ObjectId-based aggregates.
class GlobalCount {
 public:
  explicit GlobalCount(const GlobalCountSpec& spec) : spec_(spec) {}
  size_t size() const { return groups_.size(); }
  uint64_t reserved_bytes() const { return reserved_; }
  uint64_t distinct_entries() const { return entries_; }

  Status AdmitCandidate(uint64_t budget) const {
    return Check(groups_.size() + 1, entries_, budget);
  }

  Status Initialize(uint64_t budget) {
    if (spec_.grouped) return Status::OK();
    RETURN_NOT_OK(Reserve(2, 0, budget));
    groups_.emplace(std::vector<::ObjectId>{}, Group(spec_));
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
    uint64_t additions = 0;
    for (size_t slot = 0; slot < spec_.distinct_columns.size(); ++slot) {
      const auto value = row[spec_.distinct_columns[slot]];
      if (!value.is_null() && (is_new || !found->second.seen[slot].count(value))) ++additions;
    }
    // Admit all new group/set state before allocating any tree node.
    RETURN_NOT_OK(Reserve(groups_.size() + 1 + is_new, entries_ + additions, budget));
    if (is_new) found = groups_.emplace_hint(found, std::move(key), Group(spec_));
    auto& group = found->second;
    for (size_t i = 0; i < spec_.count_columns.size(); ++i) {
      if (spec_.extrema_directions[i]) {
        const auto column = spec_.count_columns[i];
        if (column < 0 || row[column].is_null()) continue;
        auto& selected = group.extrema[i];
        const auto value = row[column];
        const auto order = selected.is_null() ? 0 : SPARQL::Comparisons::compare_for_order(value, selected);
        if (selected.is_null() || (spec_.extrema_directions[i] < 0 ? order < 0 : order > 0))
          selected = value;
        continue;
      }
      if (spec_.count_set_slots[i] != -1) continue;
      const auto column = spec_.count_columns[i];
      if (column == -2 || (column >= 0 && !row[column].is_null()))
        RETURN_NOT_OK(Increment(&group.counts[i]));
    }
    for (size_t slot = 0; slot < spec_.distinct_columns.size(); ++slot) {
      const auto value = row[spec_.distinct_columns[slot]];
      auto& seen = group.seen[slot];
      if (value.is_null() || seen.count(value)) continue;
      if (seen.size() >= static_cast<uint64_t>(std::numeric_limits<int64_t>::max()))
        return STATUS(RuntimeError, "MDB global DISTINCT COUNT exceeds signed integer range");
      seen.insert(value);
      ++entries_;
    }
    return Status::OK();
  }

  std::vector<::ObjectId> ReleaseRows() {
    std::vector<::ObjectId> result;
    result.reserve(groups_.size() * (spec_.group_columns.size() + spec_.count_columns.size()));
    for (const auto& group : groups_) {
      result.insert(result.end(), group.first.begin(), group.first.end());
      for (size_t i = 0; i < spec_.count_columns.size(); ++i) {
        if (spec_.extrema_directions[i]) {
          result.push_back(group.second.extrema[i]);
          continue;
        }
        const auto slot = spec_.count_set_slots[i];
        const uint64_t count = slot >= 0 ? group.second.seen[slot].size() : group.second.counts[i];
        result.push_back(SPARQL::Conversions::pack_int(static_cast<int64_t>(count)));
      }
    }
    groups_.clear();
    return result;
  }

 private:
  struct Group {
    explicit Group(const GlobalCountSpec& spec)
        : counts(spec.count_columns.size(), 0), seen(spec.distinct_columns.size()),
          extrema(spec.has_extrema ? spec.count_columns.size() : 0, ::ObjectId::get_null()) {}
    std::vector<uint64_t> counts;
    std::vector<std::set<::ObjectId>> seen;
    std::vector<::ObjectId> extrema;
  };

  Status Check(uint64_t slots, uint64_t entries, uint64_t budget) const {
    if (slots > budget / spec_.row_reservation ||
        entries > (budget - slots * spec_.row_reservation) / spec_.distinct_entry_reservation)
      return STATUS(RuntimeError, "MDB global COUNT input budget exceeded");
    return Status::OK();
  }

  Status Reserve(uint64_t slots, uint64_t entries, uint64_t budget) {
    RETURN_NOT_OK(Check(slots, entries, budget));
    reserved_ = std::max(reserved_, slots * spec_.row_reservation +
                                   entries * spec_.distinct_entry_reservation);
    return Status::OK();
  }

  const GlobalCountSpec spec_;
  std::map<std::vector<::ObjectId>, Group> groups_;
  uint64_t entries_ = 0;
  uint64_t reserved_ = 0;
};

}  // namespace yb::mdb

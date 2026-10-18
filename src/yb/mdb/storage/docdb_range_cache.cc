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

#include "yb/mdb/storage/docdb_range_cache.h"

#include <algorithm>
#include <utility>

namespace yb::mdb {

DocDBRangeCache::DocDBRangeCache(DocDBRangeCache&& other) noexcept
    : rows_(std::move(other.rows_)), row_index_(other.row_index_) {
  other.row_index_ = 0;
  ++other.mutation_version_;
}

DocDBRangeCache& DocDBRangeCache::operator=(const DocDBRangeCache& other) {
  if (this != &other) {
    rows_ = other.rows_;
    row_index_ = other.row_index_;
    ++mutation_version_;
  }
  return *this;
}

DocDBRangeCache& DocDBRangeCache::operator=(DocDBRangeCache&& other) noexcept {
  if (this != &other) {
    rows_ = std::move(other.rows_);
    row_index_ = other.row_index_;
    other.row_index_ = 0;
    ++mutation_version_;
    ++other.mutation_version_;
  }
  return *this;
}

void DocDBRangeCache::InsertRow(uint64_t c0, uint64_t c1, uint64_t c2) {
  const Row row{c0, c1, c2};
  const auto less = [](const Row& lhs, const Row& rhs) {
    return lhs.c0 < rhs.c0 ||
           (lhs.c0 == rhs.c0 && (lhs.c1 < rhs.c1 || (lhs.c1 == rhs.c1 && lhs.c2 < rhs.c2)));
  };
  // Predicate pages often arrive in target order; avoid rescanning their growing prefix.
  if (rows_.empty() || !less(row, rows_.back())) {
    rows_.push_back(row);
    ++mutation_version_;
    return;
  }
  rows_.insert(std::lower_bound(rows_.begin(), rows_.end(), row, less), row);
  ++mutation_version_;
}

bool DocDBRangeCache::SeekRange(
    uint64_t target_c0, uint64_t max_c0, uint64_t* out_c0, uint64_t* out_c1, uint64_t* out_c2) {
  while (row_index_ < rows_.size()) {
    auto& row = rows_[row_index_];
    if (row.c0 >= target_c0 && row.c0 <= max_c0) {
      *out_c0 = row.c0;
      *out_c1 = row.c1;
      *out_c2 = row.c2;
      return true;
    }
    if (row.c0 > max_c0) {
      return false;
    }
    row_index_++;
  }
  return false;
}

bool DocDBRangeCache::Next(uint64_t max_c0, uint64_t* out_c0, uint64_t* out_c1, uint64_t* out_c2) {
  if (row_index_ + 1 >= rows_.size()) {
    return false;
  }
  row_index_++;
  auto& row = rows_[row_index_];
  if (row.c0 > max_c0) {
    return false;
  }
  *out_c0 = row.c0;
  *out_c1 = row.c1;
  *out_c2 = row.c2;
  return true;
}

uint64_t DocDBRangeCache::CurrentKey() const {
  if (row_index_ < rows_.size()) {
    return rows_[row_index_].c0;
  }
  return 0;
}

bool DocDBRangeCache::SeekRangeWithIndex(
    uint64_t target_c0, uint64_t max_c0, uint64_t* out_c0, uint64_t* out_c1, uint64_t* out_c2,
    size_t* inout_index, size_t* out_rows_examined) const {
  if (out_rows_examined != nullptr) {
    *out_rows_examined = 0;
  }
  while (*inout_index < rows_.size()) {
    auto& row = rows_[*inout_index];
    if (out_rows_examined != nullptr) {
      ++(*out_rows_examined);
    }
    if (row.c0 >= target_c0 && row.c0 <= max_c0) {
      *out_c0 = row.c0;
      *out_c1 = row.c1;
      *out_c2 = row.c2;
      return true;
    }
    if (row.c0 > max_c0) {
      return false;
    }
    (*inout_index)++;
  }
  return false;
}

bool DocDBRangeCache::NextWithIndex(
    uint64_t max_c0, uint64_t* out_c0, uint64_t* out_c1, uint64_t* out_c2,
    size_t* inout_index) const {
  if (*inout_index + 1 >= rows_.size()) {
    return false;
  }
  (*inout_index)++;
  auto& row = rows_[*inout_index];
  if (row.c0 > max_c0) {
    return false;
  }
  *out_c0 = row.c0;
  *out_c1 = row.c1;
  *out_c2 = row.c2;
  return true;
}

bool DocDBRangeCache::SeekC0PrefixWithIndex(
    uint64_t target_c0, uint64_t min_c1, uint64_t max_c1, uint64_t min_c2, uint64_t max_c2,
    uint64_t* out_c0, uint64_t* out_c1, uint64_t* out_c2, size_t* inout_index,
    size_t* out_rows_examined) const {
  if (out_rows_examined != nullptr) {
    *out_rows_examined = 0;
  }
  const Row key{target_c0, min_c1, min_c2};
  *inout_index = static_cast<size_t>(
      std::lower_bound(
          rows_.begin(), rows_.end(), key,
          [](const Row& lhs, const Row& rhs) {
            return lhs.c0 < rhs.c0 ||
                   (lhs.c0 == rhs.c0 && (lhs.c1 < rhs.c1 || (lhs.c1 == rhs.c1 && lhs.c2 < rhs.c2)));
          }) -
      rows_.begin());
  while (*inout_index < rows_.size()) {
    const auto& row = rows_[*inout_index];
    if (out_rows_examined != nullptr) {
      ++(*out_rows_examined);
    }
    if (row.c0 != target_c0 || row.c1 > max_c1) {
      return false;
    }
    if (row.c2 >= min_c2 && row.c2 <= max_c2) {
      *out_c0 = row.c0;
      *out_c1 = row.c1;
      *out_c2 = row.c2;
      return true;
    }
    ++(*inout_index);
  }
  return false;
}

bool DocDBRangeCache::NextC0PrefixWithIndex(
    uint64_t target_c0, uint64_t max_c1, uint64_t min_c2, uint64_t max_c2, uint64_t* out_c0,
    uint64_t* out_c1, uint64_t* out_c2, size_t* inout_index) const {
  if (*inout_index < rows_.size()) {
    ++(*inout_index);
  }
  while (*inout_index < rows_.size()) {
    const auto& row = rows_[*inout_index];
    if (row.c0 != target_c0 || row.c1 > max_c1) {
      return false;
    }
    if (row.c2 >= min_c2 && row.c2 <= max_c2) {
      *out_c0 = row.c0;
      *out_c1 = row.c1;
      *out_c2 = row.c2;
      return true;
    }
    ++(*inout_index);
  }
  return false;
}

}  // namespace yb::mdb

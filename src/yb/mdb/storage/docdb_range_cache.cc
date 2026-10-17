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

namespace yb::mdb {

void DocDBRangeCache::InsertRow(uint64_t c0, uint64_t c1, uint64_t c2) {
  Row r{c0, c1, c2};
  auto it = rows_.begin();
  while (it != rows_.end() && (it->c0 < c0 ||
         (it->c0 == c0 && (it->c1 < c1 || (it->c1 == c1 && it->c2 < c2))))) {
    ++it;
  }
  rows_.insert(it, r);
}

bool DocDBRangeCache::SeekRange(uint64_t target_c0, uint64_t max_c0,
                                 uint64_t* out_c0, uint64_t* out_c1, uint64_t* out_c2) {
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

bool DocDBRangeCache::Next(uint64_t max_c0,
                            uint64_t* out_c0, uint64_t* out_c1, uint64_t* out_c2) {
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

bool DocDBRangeCache::SeekRangeWithIndex(uint64_t target_c0, uint64_t max_c0,
                                          uint64_t* out_c0, uint64_t* out_c1, uint64_t* out_c2,
                                          size_t* inout_index) {
  while (*inout_index < rows_.size()) {
    auto& row = rows_[*inout_index];
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

bool DocDBRangeCache::NextWithIndex(uint64_t max_c0,
                                     uint64_t* out_c0, uint64_t* out_c1, uint64_t* out_c2,
                                     size_t* inout_index) {
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

}  // namespace yb::mdb

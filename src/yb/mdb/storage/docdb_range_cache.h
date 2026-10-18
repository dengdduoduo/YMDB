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

#pragma once

#include <cstdint>
#include <vector>

namespace yb::mdb {

class DocDBRangeCache {
 public:
  DocDBRangeCache() = default;
  DocDBRangeCache(const DocDBRangeCache&) = default;
  DocDBRangeCache(DocDBRangeCache&& other) noexcept;
  DocDBRangeCache& operator=(const DocDBRangeCache& other);
  DocDBRangeCache& operator=(DocDBRangeCache&& other) noexcept;

  void InsertRow(uint64_t c0, uint64_t c1, uint64_t c2);

  void Clear() {
    rows_.clear();
    row_index_ = 0;
    ++mutation_version_;
  }
  void Reset() { row_index_ = 0; }
  size_t Size() const { return rows_.size(); }
  // Content changes invalidate estimates; moving a legacy cursor does not.
  uint64_t MutationVersion() const { return mutation_version_; }

  // Reserved tuple storage only; excludes container and allocator overhead.
  size_t AllocatedRowBytes() const { return rows_.capacity() * sizeof(Row); }

  bool SeekRange(
      uint64_t target_c0, uint64_t max_c0, uint64_t* out_c0, uint64_t* out_c1, uint64_t* out_c2);

  bool Next(uint64_t max_c0, uint64_t* out_c0, uint64_t* out_c1, uint64_t* out_c2);

  bool SeekRangeWithIndex(
      uint64_t target_c0, uint64_t max_c0, uint64_t* out_c0, uint64_t* out_c1, uint64_t* out_c2,
      size_t* inout_index, size_t* out_rows_examined = nullptr) const;

  bool NextWithIndex(
      uint64_t max_c0, uint64_t* out_c0, uint64_t* out_c1, uint64_t* out_c2,
      size_t* inout_index) const;

  // Seek and iterate within one exact c0 prefix while honoring c1/c2 bounds.
  // The index is independent from the legacy cursor used by SeekRange/Next.
  bool SeekC0PrefixWithIndex(
      uint64_t target_c0, uint64_t min_c1, uint64_t max_c1, uint64_t min_c2, uint64_t max_c2,
      uint64_t* out_c0, uint64_t* out_c1, uint64_t* out_c2, size_t* inout_index,
      size_t* out_rows_examined = nullptr) const;

  bool NextC0PrefixWithIndex(
      uint64_t target_c0, uint64_t max_c1, uint64_t min_c2, uint64_t max_c2, uint64_t* out_c0,
      uint64_t* out_c1, uint64_t* out_c2, size_t* inout_index) const;

  uint64_t CurrentKey() const;

  void GetRowAt(size_t index, uint64_t* c0, uint64_t* c1, uint64_t* c2) const {
    if (index < rows_.size()) {
      *c0 = rows_[index].c0;
      *c1 = rows_[index].c1;
      *c2 = rows_[index].c2;
    } else {
      *c0 = *c1 = *c2 = 0;
    }
  }

  void GetCurrentRow(uint64_t* c0, uint64_t* c1, uint64_t* c2) const {
    GetRowAt(row_index_, c0, c1, c2);
  }

 private:
  struct Row {
    uint64_t c0, c1, c2;
  };
  std::vector<Row> rows_;
  size_t row_index_ = 0;
  uint64_t mutation_version_ = 0;
};

}  // namespace yb::mdb

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

  void InsertRow(uint64_t c0, uint64_t c1, uint64_t c2);

  void Clear() { rows_.clear(); row_index_ = 0; }
  void Reset() { row_index_ = 0; }
  size_t Size() const { return rows_.size(); }

  bool SeekRange(uint64_t target_c0, uint64_t max_c0,
                 uint64_t* out_c0, uint64_t* out_c1, uint64_t* out_c2);

  bool Next(uint64_t max_c0,
            uint64_t* out_c0, uint64_t* out_c1, uint64_t* out_c2);

  bool SeekRangeWithIndex(uint64_t target_c0, uint64_t max_c0,
                          uint64_t* out_c0, uint64_t* out_c1, uint64_t* out_c2,
                          size_t* inout_index);

  bool NextWithIndex(uint64_t max_c0,
                     uint64_t* out_c0, uint64_t* out_c1, uint64_t* out_c2,
                     size_t* inout_index);

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
};

}  // namespace yb::mdb

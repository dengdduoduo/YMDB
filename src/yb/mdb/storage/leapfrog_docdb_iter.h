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

#include <memory>
#include <vector>

#include "storage/index/leapfrog/leapfrog_iter.h"

#include "yb/mdb/storage/docdb_range_cache.h"

namespace yb::mdb {

class LeapfrogDocDBIter : public LeapfrogIter {
 public:
  LeapfrogDocDBIter(
      bool* interruption_requested,
      std::vector<std::unique_ptr<ScanRange>> initial_ranges,
      std::vector<VarId> intersection_vars,
      std::vector<VarId> enumeration_vars);

  DocDBRangeCache& range_cache() { return cache_; }

  void down() override;
  bool next() override;
  bool seek(uint64_t key) override;
  uint64_t get_key() const override;

  bool open_terms(Binding& input_binding) override;

  void begin_enumeration() override;
  void reset_enumeration() override;
  bool next_enumeration(Binding& binding) override;

  std::string get_iter_name() const override { return "LeapfrogDocDBIter"; }

  bool try_estimate(
      std::vector<double>& initial_estimations,
      std::vector<double>& after_estimations) const override;

 private:
  static constexpr int kNumCols = 3;

  bool internal_search(
      const uint64_t* min, const uint64_t* max);

  DocDBRangeCache cache_;
  uint64_t current_tuple_[kNumCols];
  size_t enum_row_index_ = 0;
};

}  // namespace yb::mdb

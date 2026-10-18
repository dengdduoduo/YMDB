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

#include <array>
#include <memory>
#include <vector>

#include "storage/index/leapfrog/leapfrog_iter.h"
#include "yb/mdb/storage/docdb_range_cache.h"

namespace yb::mdb {

// The cache must remain unchanged between open_terms and the end of traversal.
class LeapfrogDocDBIter : public LeapfrogIter {
 public:
  LeapfrogDocDBIter(
      bool* interruption_requested, std::vector<std::unique_ptr<ScanRange>> initial_ranges,
      std::vector<VarId> intersection_vars, std::vector<VarId> enumeration_vars,
      Status initial_status = Status::OK());

  DocDBRangeCache& range_cache() { return cache_; }
  const DocDBRangeCache& read_cache() const { return shared_cache_ ? *shared_cache_ : cache_; }
  bool uses_shared_cache() const { return shared_cache_ != nullptr; }
  void up() override;
  void down() override;
  bool next() override;
  bool seek(uint64_t key) override;
  uint64_t get_key() const override;
  bool open_terms(Binding& input_binding) override;
  Result<bool> open_for_estimation(Binding& input_binding) override {
    // Bypass virtual open_terms overrides that could load remote data.
    const bool opened = LeapfrogDocDBIter::open_terms(input_binding);
    RETURN_NOT_OK(status());
    return opened;
  }
  void begin_enumeration() override;
  void reset_enumeration() override;
  bool next_enumeration(Binding& binding) override;
  std::string get_iter_name() const override { return "LeapfrogDocDBIter"; }
  // Append exact tuple counts under the current intersection's parent prefix:
  // all rows, then rows at/after the current key (inclusive, preserving duplicates).
  // No loading or cursor movement; unavailable states leave both vectors unchanged.
  ProgressCountUnit progress_count_unit() const override {
    return ProgressCountUnit::kTuples;
  }
  bool try_estimate(
      std::vector<double>& initial_estimations,
      std::vector<double>& after_estimations) const override;

 protected:
  void UseSharedCache(std::shared_ptr<const DocDBRangeCache> cache) {
    opened_cache_ = nullptr;
    shared_cache_ = std::move(cache);
  }

 private:
  static constexpr size_t kNumCols = 3;
  using Tuple = std::array<uint64_t, kNumCols>;
  bool Ready();
  bool AtIntersection() const;
  bool InternalSearch(const Tuple& min, const Tuple& max);

  DocDBRangeCache cache_;
  std::shared_ptr<const DocDBRangeCache> shared_cache_;
  Tuple current_tuple_{};
  // Each trie depth keeps its parent's valid position after a child is exhausted.
  std::array<bool, kNumCols + 1> positioned_{};
  size_t current_row_index_ = 0;
  size_t enum_row_index_ = 0;
  const DocDBRangeCache* opened_cache_ = nullptr;
  uint64_t opened_cache_version_ = 0;
  bool opened_ = false;
  bool enumerating_ = false;
};

}  // namespace yb::mdb

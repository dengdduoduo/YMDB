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

#include "yb/mdb/storage/leapfrog_docdb_iter.h"

#include <limits>

#include "graph_models/object_id.h"
#include "query/executor/binding.h"
#include "query/executor/binding_iter/scan_ranges/term.h"

namespace yb::mdb {

LeapfrogDocDBIter::LeapfrogDocDBIter(
    bool* interruption_requested,
    std::vector<std::unique_ptr<ScanRange>> initial_ranges,
    std::vector<VarId> intersection_vars,
    std::vector<VarId> enumeration_vars)
    : LeapfrogIter(
          interruption_requested,
          std::move(initial_ranges),
          std::move(intersection_vars),
          std::move(enumeration_vars)) {
  for (size_t i = 0; i < kNumCols; i++) {
    current_tuple_[i] = 0;
  }
}

void LeapfrogDocDBIter::down() {
  level++;

  uint64_t min[kNumCols];
  uint64_t max[kNumCols];

  for (int_fast32_t i = 0; i < level; i++) {
    min[i] = current_tuple_[i];
    max[i] = current_tuple_[i];
  }
  for (size_t i = level; i < kNumCols; i++) {
    min[i] = 0;
    max[i] = std::numeric_limits<uint64_t>::max();
  }

  internal_search(min, max);
}

bool LeapfrogDocDBIter::next() {
  uint64_t min[kNumCols];
  uint64_t max[kNumCols];

  for (int_fast32_t i = 0; i < level; i++) {
    min[i] = current_tuple_[i];
    max[i] = current_tuple_[i];
  }

  min[level] = current_tuple_[level] + 1;
  max[level] = std::numeric_limits<uint64_t>::max();

  for (size_t i = level + 1; i < kNumCols; i++) {
    min[i] = 0;
    max[i] = std::numeric_limits<uint64_t>::max();
  }

  return internal_search(min, max);
}

bool LeapfrogDocDBIter::seek(uint64_t key) {
  uint64_t min[kNumCols];
  uint64_t max[kNumCols];

  for (int_fast32_t i = 0; i < level; i++) {
    min[i] = current_tuple_[i];
    max[i] = current_tuple_[i];
  }

  min[level] = key;
  max[level] = std::numeric_limits<uint64_t>::max();

  for (size_t i = level + 1; i < kNumCols; i++) {
    min[i] = 0;
    max[i] = std::numeric_limits<uint64_t>::max();
  }

  return internal_search(min, max);
}

uint64_t LeapfrogDocDBIter::get_key() const {
  if (level >= 0 && level < static_cast<int_fast32_t>(kNumCols)) {
    return current_tuple_[level];
  }
  return 0;
}

bool LeapfrogDocDBIter::open_terms(Binding& input_binding) {
  uint64_t min[kNumCols];
  uint64_t max[kNumCols];

  size_t i = 0;
  for (; i < initial_ranges.size(); i++) {
    min[i] = initial_ranges[i]->get_min(input_binding);
    max[i] = initial_ranges[i]->get_max(input_binding);
  }

  for (; i < kNumCols; i++) {
    min[i] = 0;
    max[i] = std::numeric_limits<uint64_t>::max();
  }

  level = static_cast<int_fast32_t>(initial_ranges.size()) - 1;
  return internal_search(min, max);
}

void LeapfrogDocDBIter::begin_enumeration() {
  enum_row_index_ = 0;
}

void LeapfrogDocDBIter::reset_enumeration() {
  begin_enumeration();
}

bool LeapfrogDocDBIter::next_enumeration(Binding& binding) {
  if (enum_row_index_ >= 1) {
    return false;
  }
  enum_row_index_++;
  for (uint_fast32_t i = 0; i < enumeration_vars.size(); i++) {
    binding.add(
        enumeration_vars[i],
        ObjectId(current_tuple_[initial_ranges.size() + intersection_vars.size() + i]));
  }
  return true;
}

bool LeapfrogDocDBIter::try_estimate(
    std::vector<double>& /*initial_estimations*/,
    std::vector<double>& /*after_estimations*/) const {
  return false;
}

bool LeapfrogDocDBIter::internal_search(
    const uint64_t* min, const uint64_t* max) {
  uint64_t found_c0, found_c1, found_c2;
  bool found = cache_.SeekRange(min[0], max[0], &found_c0, &found_c1, &found_c2);

  while (found) {
    if (found_c1 >= min[1] && found_c1 <= max[1] &&
        found_c2 >= min[2] && found_c2 <= max[2]) {
      current_tuple_[0] = found_c0;
      current_tuple_[1] = found_c1;
      current_tuple_[2] = found_c2;
      return true;
    }
    found = cache_.Next(max[0], &found_c0, &found_c1, &found_c2);
  }

  return false;
}

}  // namespace yb::mdb

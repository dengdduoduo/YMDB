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

#include <algorithm>
#include <limits>
#include <set>

#include "graph_models/object_id.h"
#include "query/executor/binding.h"

namespace yb::mdb {

LeapfrogDocDBIter::LeapfrogDocDBIter(
    bool* interruption_requested, std::vector<std::unique_ptr<ScanRange>> initial_ranges,
    std::vector<VarId> intersection_vars, std::vector<VarId> enumeration_vars,
    Status initial_status)
    : LeapfrogIter(
          interruption_requested, std::move(initial_ranges), std::move(intersection_vars),
          std::move(enumeration_vars)) {
  set_status(std::move(initial_status));
}

bool LeapfrogDocDBIter::Ready() {
  if (status().ok() && interruption_requested && *interruption_requested) {
    set_status(STATUS(Aborted, "Leapfrog traversal interrupted"));
  }
  return status().ok();
}

bool LeapfrogDocDBIter::AtIntersection() const {
  return opened_ && level >= static_cast<int_fast32_t>(initial_ranges.size()) &&
         level < static_cast<int_fast32_t>(initial_ranges.size() + intersection_vars.size());
}

bool LeapfrogDocDBIter::open_terms(Binding& input_binding) {
  opened_ = false;
  opened_cache_ = nullptr;
  enumerating_ = false;
  positioned_.fill(false);
  current_row_index_ = 0;
  level = -1;
  if (!Ready()) return false;
  if (get_relation_size() == 0 || get_relation_size() > kNumCols) {
    set_status(STATUS(InvalidArgument, "Leapfrog relation must have one to three columns"));
    return false;
  }
  std::set<VarId> vars;
  for (const auto* group : {&intersection_vars, &enumeration_vars}) {
    for (const auto var : *group) {
      if (var.id >= input_binding.size || !vars.insert(var).second) {
        set_status(STATUS(InvalidArgument, "Invalid or repeated Leapfrog variable"));
        return false;
      }
    }
  }
  Tuple min{}, max;
  max.fill(std::numeric_limits<uint64_t>::max());
  for (size_t i = 0; i < initial_ranges.size(); ++i) {
    if (!initial_ranges[i]) {
      set_status(STATUS(InvalidArgument, "Null Leapfrog initial range"));
      return false;
    }
    min[i] = initial_ranges[i]->get_min(input_binding);
    max[i] = initial_ranges[i]->get_max(input_binding);
    if (min[i] != max[i]) {
      set_status(STATUS(InvalidArgument, "Leapfrog initial terms must be fixed prefixes"));
      return false;
    }
  }
  level = static_cast<int_fast32_t>(initial_ranges.size()) - 1;
  opened_cache_ = &read_cache();
  opened_cache_version_ = opened_cache_->MutationVersion();
  opened_ = true;
  return InternalSearch(min, max);
}

void LeapfrogDocDBIter::up() {
  enumerating_ = false;
  if (level >= static_cast<int_fast32_t>(initial_ranges.size())) --level;
}

void LeapfrogDocDBIter::down() {
  enumerating_ = false;
  if (!Ready() || !opened_ || !positioned_[level + 1]) return;
  if (level + 1 >= static_cast<int_fast32_t>(initial_ranges.size() + intersection_vars.size())) {
    set_status(STATUS(InvalidArgument, "Leapfrog down exceeds intersection columns"));
    return;
  }
  ++level;
  current_row_index_ = 0;
  Tuple min{}, max;
  max.fill(std::numeric_limits<uint64_t>::max());
  for (int_fast32_t i = 0; i < level; ++i) min[i] = max[i] = current_tuple_[i];
  InternalSearch(min, max);
}

bool LeapfrogDocDBIter::next() {
  enumerating_ = false;
  if (!Ready() || !AtIntersection() || !positioned_[level + 1]) return false;
  if (current_tuple_[level] == std::numeric_limits<uint64_t>::max()) {
    positioned_[level + 1] = false;
    return false;
  }
  return seek(current_tuple_[level] + 1);
}

bool LeapfrogDocDBIter::seek(uint64_t key) {
  enumerating_ = false;
  if (!Ready() || !AtIntersection() || !positioned_[level + 1]) return false;
  Tuple min{}, max;
  max.fill(std::numeric_limits<uint64_t>::max());
  for (int_fast32_t i = 0; i < level; ++i) min[i] = max[i] = current_tuple_[i];
  min[level] = std::max(key, current_tuple_[level]);
  return InternalSearch(min, max);
}

uint64_t LeapfrogDocDBIter::get_key() const {
  return AtIntersection() && positioned_[level + 1] ? current_tuple_[level] : 0;
}

void LeapfrogDocDBIter::begin_enumeration() {
  enumerating_ = false;
  if (!Ready() || !opened_ || !positioned_[level + 1]) return;
  if (level + 1 != static_cast<int_fast32_t>(initial_ranges.size() + intersection_vars.size())) {
    set_status(STATUS(InvalidArgument, "Leapfrog enumeration requires all intersection keys"));
    return;
  }
  enum_row_index_ = current_row_index_;
  enumerating_ = true;
}

void LeapfrogDocDBIter::reset_enumeration() { begin_enumeration(); }

bool LeapfrogDocDBIter::next_enumeration(Binding& binding) {
  if (!Ready() || !enumerating_ || enum_row_index_ >= read_cache().Size()) return false;
  Tuple row;
  read_cache().GetRowAt(enum_row_index_++, &row[0], &row[1], &row[2]);
  const size_t prefix = initial_ranges.size() + intersection_vars.size();
  for (size_t i = 0; i < prefix; ++i) {
    if (row[i] != current_tuple_[i]) {
      enumerating_ = false;
      return false;
    }
  }
  for (size_t i = 0; i < enumeration_vars.size(); ++i) {
    if (enumeration_vars[i].id >= binding.size) {
      set_status(STATUS(InvalidArgument, "Leapfrog output binding is too small"));
      return false;
    }
    binding.add(enumeration_vars[i], ObjectId(row[prefix + i]));
  }
  return true;
}

bool LeapfrogDocDBIter::try_estimate(
    std::vector<double>& initial_estimations, std::vector<double>& after_estimations) const {
  const auto& cache = read_cache();
  if (&initial_estimations == &after_estimations ||
      initial_estimations.size() != after_estimations.size() || !status().ok() ||
      (interruption_requested && *interruption_requested) || !AtIntersection() ||
      !positioned_[level + 1] || opened_cache_ != &cache ||
      opened_cache_version_ != cache.MutationVersion()) {
    return false;
  }
  // Find prefix bounds without relying on a child traversal's last row index.
  const auto bound = [&](size_t prefix_size, bool upper) {
    size_t first = 0, last = cache.Size();
    while (first < last) {
      const size_t mid = first + (last - first) / 2;
      Tuple row;
      cache.GetRowAt(mid, &row[0], &row[1], &row[2]);
      int comparison = 0;
      for (size_t i = 0; i < prefix_size && comparison == 0; ++i) {
        if (row[i] < current_tuple_[i])
          comparison = -1;
        else if (row[i] > current_tuple_[i])
          comparison = 1;
      }
      if (comparison < 0 || (upper && comparison == 0))
        first = mid + 1;
      else
        last = mid;
    }
    return first;
  };
  const auto depth = static_cast<size_t>(level);
  const size_t begin = depth == 0 ? 0 : bound(depth, false);
  const size_t end = depth == 0 ? cache.Size() : bound(depth, true);
  const size_t remaining_begin = bound(depth + 1, false);
  if (begin > remaining_begin || remaining_begin >= end ||
      end - begin > (uint64_t{1} << std::numeric_limits<double>::digits)) {
    return false;
  }
  initial_estimations.push_back(static_cast<double>(end - begin));
  after_estimations.push_back(static_cast<double>(end - remaining_begin));
  return true;
}

bool LeapfrogDocDBIter::InternalSearch(const Tuple& min, const Tuple& max) {
  positioned_[level + 1] = false;
  Tuple row;
  bool found = min[0] == max[0]
                   ? read_cache().SeekC0PrefixWithIndex(
                         min[0], min[1], max[1], min[2], max[2], &row[0], &row[1], &row[2],
                         &current_row_index_)
                   : read_cache().SeekRangeWithIndex(
                         min[0], max[0], &row[0], &row[1], &row[2], &current_row_index_);
  while (found && Ready()) {
    if (row[1] >= min[1] && row[1] <= max[1] && row[2] >= min[2] && row[2] <= max[2]) {
      current_tuple_ = row;
      positioned_[level + 1] = true;
      return true;
    }
    found = read_cache().NextWithIndex(max[0], &row[0], &row[1], &row[2], &current_row_index_);
  }
  return false;
}

}  // namespace yb::mdb

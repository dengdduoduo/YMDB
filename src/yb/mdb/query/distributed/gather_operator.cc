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

#include "yb/mdb/query/distributed/gather_operator.h"

#include <unordered_set>

#include "yb/mdb/query/executor/binding.h"
#include "yb/util/status.h"

namespace yb::mdb {

Status GatherOperator::Merge(
    const std::vector<ScatterExecutor::FragmentResultSet>& fragment_results,
    GatherStrategy strategy,
    GatherResult* result) {
  result->bindings.clear();
  result->total_input_rows = 0;
  result->total_output_rows = 0;

  for (const auto& frag_result : fragment_results) {
    if (!frag_result.status.ok()) {
      return frag_result.status;
    }
    result->total_input_rows += static_cast<int>(frag_result.bindings.size());
  }

  switch (strategy) {
    case GatherStrategy::UNION_ALL:
      MergeUnionAll(fragment_results, result);
      break;
    case GatherStrategy::UNION_ALL_SORT:
      MergeUnionAllSort(fragment_results, result);
      break;
    case GatherStrategy::UNION_DISTINCT:
      MergeUnionDistinct(fragment_results, result);
      break;
  }

  result->total_output_rows = static_cast<int>(result->bindings.size());
  return Status::OK();
}

void GatherOperator::MergeUnionAll(
    const std::vector<ScatterExecutor::FragmentResultSet>& fragment_results,
    GatherResult* result) {
  for (const auto& frag_result : fragment_results) {
    for (const auto& binding : frag_result.bindings) {
      auto copy = std::make_unique<Binding>(binding->size);
      copy->add_all(*binding);
      result->bindings.push_back(std::move(copy));
    }
  }
}

void GatherOperator::MergeUnionAllSort(
    const std::vector<ScatterExecutor::FragmentResultSet>& fragment_results,
    GatherResult* result) {
  MergeUnionAll(fragment_results, result);
}

struct BindingPtrHash {
  std::size_t operator()(const Binding* b) const {
    std::size_t h = 0;
    for (std::size_t i = 0; i < b->size; i++) {
      h ^= std::hash<uint64_t>()(b->operator[](VarId(static_cast<uint_fast32_t>(i))).id);
    }
    return h;
  }
};

struct BindingPtrEq {
  bool operator()(const Binding* a, const Binding* b) const {
    return *a == *b;
  }
};

void GatherOperator::MergeUnionDistinct(
    const std::vector<ScatterExecutor::FragmentResultSet>& fragment_results,
    GatherResult* result) {
  std::unordered_set<const Binding*, BindingPtrHash, BindingPtrEq> seen;
  for (const auto& frag_result : fragment_results) {
    for (const auto& binding : frag_result.bindings) {
      auto copy = std::make_unique<Binding>(binding->size);
      copy->add_all(*binding);
      if (seen.find(copy.get()) == seen.end()) {
        seen.insert(copy.get());
        result->bindings.push_back(std::move(copy));
      }
    }
  }
}

}  // namespace yb::mdb

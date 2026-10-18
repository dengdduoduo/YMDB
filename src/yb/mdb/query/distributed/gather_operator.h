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
#include <memory>
#include <set>
#include <vector>

#include "yb/mdb/query/distributed/distributed_plan.h"
#include "yb/mdb/query/distributed/scatter_executor.h"
#include "yb/util/status.h"

namespace yb::mdb {

struct GatherResult {
  std::vector<std::unique_ptr<Binding>> bindings;
  int total_input_rows = 0;
  int total_output_rows = 0;
};

class GatherOperator {
 public:
  GatherOperator() = default;

  Status Merge(const std::vector<ScatterExecutor::FragmentResultSet>& fragment_results,
               GatherStrategy strategy,
               GatherResult* result);

 private:
  static void MergeUnionAll(
      const std::vector<ScatterExecutor::FragmentResultSet>& fragment_results,
      GatherResult* result);

  static void MergeUnionAllSort(
      const std::vector<ScatterExecutor::FragmentResultSet>& fragment_results,
      GatherResult* result);

  static void MergeUnionDistinct(
      const std::vector<ScatterExecutor::FragmentResultSet>& fragment_results,
      GatherResult* result);
};

}  // namespace yb::mdb

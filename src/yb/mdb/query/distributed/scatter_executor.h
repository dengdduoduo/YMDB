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
#include <functional>
#include <memory>
#include <vector>

#include "query/executor/binding.h"

#include "yb/mdb/query/distributed/distributed_plan.h"
#include "yb/util/status.h"

namespace yb::mdb {

using FragmentResultCallback = std::function<void(uint32_t fragment_index,
                                                   std::unique_ptr<Binding>)>;

class ScatterExecutor {
 public:
  ScatterExecutor() = default;

  struct FragmentResultSet {
    uint32_t fragment_index;
    std::vector<std::unique_ptr<Binding>> bindings;
    Status status;
  };

  Status Execute(const DistributedPlan& plan,
                 std::vector<FragmentResultSet>* results);

 private:
  Status ExecuteLocal(const TabletFragment& fragment,
                      FragmentResultSet* result);
};

}  // namespace yb::mdb

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
#include <string>
#include <utility>
#include <vector>

#include "yb/mdb/query/distributed/distributed_plan.h"
#include "yb/mdb/query/distributed/tablet_router.h"
#include "yb/util/result.h"
#include "yb/util/status.h"

namespace yb::mdb {

enum class JoinDistributionType {
  CO_LOCATED,
  BROADCAST,
  SHUFFLE,
};

struct JoinDistribution {
  JoinDistributionType type = JoinDistributionType::CO_LOCATED;
  std::string join_var;
};

class DistributedJoinOptimizer {
 public:
  explicit DistributedJoinOptimizer(TabletRouter* router) : router_(router) {}

  Result<JoinDistribution> AnalyzeJoin(
      const std::string& join_var,
      const std::vector<QueryFragment>& left_frags,
      const std::vector<QueryFragment>& right_frags);

  Result<std::vector<std::pair<int, int>>> MapCoLocatedFragments(
      const std::vector<TabletFragment>& left_frags,
      const std::vector<TabletFragment>& right_frags);

 private:
  bool IsCoLocated(const TabletFragment& left, const TabletFragment& right) const;

  TabletRouter* router_;
};

}  // namespace yb::mdb

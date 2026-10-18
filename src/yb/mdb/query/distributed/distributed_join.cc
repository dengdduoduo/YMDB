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

#include "yb/mdb/query/distributed/distributed_join.h"

#include "yb/util/logging.h"

namespace yb::mdb {

Result<JoinDistribution> DistributedJoinOptimizer::AnalyzeJoin(
    const std::string& join_var,
    const std::vector<QueryFragment>& left_frags,
    const std::vector<QueryFragment>& right_frags) {
  JoinDistribution result;
  result.join_var = join_var;

  if (left_frags.size() <= 1 && right_frags.size() <= 1) {
    result.type = JoinDistributionType::CO_LOCATED;
    return result;
  }

  if (left_frags.size() == right_frags.size()) {
    result.type = JoinDistributionType::CO_LOCATED;
  } else if (left_frags.size() == 1 || right_frags.size() == 1) {
    result.type = JoinDistributionType::BROADCAST;
  } else {
    result.type = JoinDistributionType::SHUFFLE;
  }

  return result;
}

Result<std::vector<std::pair<int, int>>> DistributedJoinOptimizer::MapCoLocatedFragments(
    const std::vector<TabletFragment>& left_frags,
    const std::vector<TabletFragment>& right_frags) {
  std::vector<std::pair<int, int>> pairs;

  if (left_frags.size() <= 1 && right_frags.size() <= 1) {
    if (!left_frags.empty() && !right_frags.empty()) {
      pairs.emplace_back(0, 0);
    }
    return pairs;
  }

  for (int i = 0; i < static_cast<int>(left_frags.size()); i++) {
    for (int j = 0; j < static_cast<int>(right_frags.size()); j++) {
      if (IsCoLocated(left_frags[i], right_frags[j])) {
        pairs.emplace_back(i, j);
      }
    }
  }

  return pairs;
}

bool DistributedJoinOptimizer::IsCoLocated(
    const TabletFragment& left, const TabletFragment& right) const {
  return left.tablet_id == right.tablet_id;
}

}  // namespace yb::mdb

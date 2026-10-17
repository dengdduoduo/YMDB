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
#include <string>
#include <vector>

#include "query/parser/op/sparql/op.h"

#include "yb/mdb/query/distributed/tablet_router.h"
#include "yb/util/status.h"

namespace yb::mdb {

enum class GatherStrategy {
  UNION_ALL,
  UNION_ALL_SORT,
  UNION_DISTINCT,
};

struct TabletFragment {
  int perm_index;
  int tablet_index;
  std::string tablet_id;
  std::string partition_start;
  std::string partition_end;
  std::string leader_host;
  uint32_t leader_port;
  std::unique_ptr<SPARQL::Op> op_tree;
};

class DistributedPlan {
 public:
  DistributedPlan() = default;

  void AddFragment(TabletFragment fragment);

  const std::vector<TabletFragment>& fragments() const { return fragments_; }
  std::vector<TabletFragment>& mutable_fragments() { return fragments_; }

  GatherStrategy gather_strategy() const { return gather_strategy_; }
  void set_gather_strategy(GatherStrategy strategy) { gather_strategy_ = strategy; }

  int num_fragments() const { return static_cast<int>(fragments_.size()); }

  bool is_distributed() const { return fragments_.size() > 1; }

 private:
  std::vector<TabletFragment> fragments_;
  GatherStrategy gather_strategy_ = GatherStrategy::UNION_ALL;
};

class DistributedPlanGenerator {
 public:
  DistributedPlanGenerator(TabletRouter* router) : router_(router) {}

  Status Generate(const SPARQL::Op& root, DistributedPlan* plan);

 private:
  Status SplitForTablet(const SPARQL::Op& root, DistributedPlan* plan);

  bool HasFixedHashKey(const SPARQL::Op& node, int* out_perm_index, std::string* out_key);

  TabletRouter* router_;
};

}  // namespace yb::mdb

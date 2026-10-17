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

#include "yb/mdb/query/distributed/distributed_plan.h"

#include "yb/mdb/query/parser/op/sparql/op_basic_graph_pattern.h"
#include "yb/mdb/query/parser/op/sparql/op_select.h"
#include "yb/mdb/query/parser/op/sparql/ops.h"
#include "yb/util/logging.h"

namespace yb::mdb {

void DistributedPlan::AddFragment(TabletFragment fragment) {
  fragments_.push_back(std::move(fragment));
}

Status DistributedPlanGenerator::Generate(const SPARQL::Op& root, DistributedPlan* plan) {
  const auto& select = dynamic_cast<const SPARQL::OpSelect&>(root);

  if (select.distinct) {
    plan->set_gather_strategy(GatherStrategy::UNION_DISTINCT);
  }

  return SplitForTablet(root, plan);
}

Status DistributedPlanGenerator::SplitForTablet(const SPARQL::Op& root, DistributedPlan* plan) {
  if (!router_->IsInitialized()) {
    return STATUS(IllegalState, "TabletRouter not initialized");
  }

  int best_perm = -1;
  std::string fixed_key;
  if (HasFixedHashKey(root, &best_perm, &fixed_key)) {
    const TabletInfo* tablet = router_->LocateTablet(best_perm, fixed_key);
    if (tablet == nullptr) {
      return STATUS(NotFound, "No tablet found for fixed key");
    }

    TabletFragment fragment;
    fragment.perm_index = best_perm;
    fragment.tablet_index = 0;
    fragment.tablet_id = tablet->tablet_id;
    fragment.partition_start = tablet->partition_start;
    fragment.partition_end = tablet->partition_end;
    fragment.leader_host = tablet->leader_host;
    fragment.leader_port = tablet->leader_port;
    fragment.op_tree = root.clone();
    plan->AddFragment(std::move(fragment));
    return Status::OK();
  }

  int perm_index = 0;
  const auto& tablets = router_->GetTablets(perm_index);
  if (tablets.empty()) {
    return STATUS(NotFound, "No tablets found for SPO table");
  }

  for (int i = 0; i < static_cast<int>(tablets.size()); i++) {
    TabletFragment fragment;
    fragment.perm_index = perm_index;
    fragment.tablet_index = i;
    fragment.tablet_id = tablets[i].tablet_id;
    fragment.partition_start = tablets[i].partition_start;
    fragment.partition_end = tablets[i].partition_end;
    fragment.leader_host = tablets[i].leader_host;
    fragment.leader_port = tablets[i].leader_port;
    fragment.op_tree = root.clone();
    plan->AddFragment(std::move(fragment));
  }

  return Status::OK();
}

bool DistributedPlanGenerator::HasFixedHashKey(const SPARQL::Op& node,
                                                int* out_perm_index,
                                                std::string* out_key) {
  return false;
}

}  // namespace yb::mdb

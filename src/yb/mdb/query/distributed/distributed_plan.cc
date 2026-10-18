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
#include "yb/mdb/query/parser/op/sparql/op_triple.h"
#include "yb/mdb/query/parser/op/sparql/ops.h"
#include "yb/common/ql_protocol_util.h"
#include "yb/dockv/partition.h"
#include "yb/mdb/storage/docdb_key_encoder.h"
#include "query/optimizer/plan/join_order/greedy_optimizer.h"
#include "query/optimizer/rdf_model/plan/triple_plan.h"
#include "yb/util/logging.h"
#include "yb/util/flags.h"

DECLARE_bool(mdb_enable_hash_join);

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
    fragment.tablet_index = static_cast<int>(
        tablet - router_->GetTablets(best_perm).data());
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
  const SPARQL::Op* child = &node;
  if (const auto* select = dynamic_cast<const SPARQL::OpSelect*>(child)) {
    child = select->op.get();
  }

  const auto* bgp = dynamic_cast<const SPARQL::OpBasicGraphPattern*>(child);
  if (bgp == nullptr || bgp->triples.empty() || !bgp->paths.empty()) {
    return false;
  }

  if (bgp->triples.size() != 1) {
    if (!FLAGS_mdb_enable_hash_join || bgp->triples.size() != 2) return false;
    const auto& left = bgp->triples[0];
    const auto& right = bgp->triples[1];
    // Retain the same fixed predicate anchor when execution chooses this candidate.
    SPARQL::TriplePlan lhs(left.subject, left.predicate, left.object);
    SPARQL::TriplePlan rhs(right.subject, right.predicate, right.object);
    if (!GreedyOptimizer::should_use_hash_join(lhs, rhs)) return false;
  }

  const auto& triple = bgp->triples.front();
  int perm = -1;
  uint64_t fixed_value = 0;
  // Match TriplePlan's scan permutation so the fragment's tablet belongs to
  // the table that execution will read, including OSP for fixed S and O.
  if (triple.subject.is_OID()) {
    if (!triple.predicate.is_OID() && triple.object.is_OID()) {
      perm = DocKeyEncoder::kOSP;
      fixed_value = triple.object.get_OID().id;
    } else {
      perm = DocKeyEncoder::kSPO;
      fixed_value = triple.subject.get_OID().id;
    }
  } else if (triple.predicate.is_OID()) {
    perm = DocKeyEncoder::kPOS;
    fixed_value = triple.predicate.get_OID().id;
  } else if (triple.object.is_OID()) {
    perm = DocKeyEncoder::kOSP;
    fixed_value = triple.object.get_OID().id;
  } else {
    return false;
  }

  // The RDF tables use an INT64 hash column. Reuse the QL encoding path used
  // by writes so the generated partition key is byte-for-byte compatible.
  QLWriteRequestPB request;
  QLAddInt64HashValue(&request, static_cast<int64_t>(fixed_value));
  QLSetHashCode(&request);
  *out_perm_index = perm;
  *out_key = dockv::PartitionSchema::EncodeMultiColumnHashValue(request.hash_code());
  return true;
}

}  // namespace yb::mdb

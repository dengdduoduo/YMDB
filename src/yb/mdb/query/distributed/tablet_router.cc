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

#include "yb/mdb/query/distributed/tablet_router.h"

#include "yb/client/client.h"
#include "yb/client/table.h"
#include "yb/client/yb_table_name.h"
#include "yb/common/common_types.pb.h"
#include "yb/common/wire_protocol.h"
#include "yb/master/master_client.pb.h"
#include "yb/master/master_util.h"
#include "yb/mdb/model/rdf_yb_model.h"
#include "yb/util/logging.h"
#include "yb/util/net/net_util.h"
#include "yb/util/status.h"

namespace yb::mdb {

Status TabletRouter::Init(yb::client::YBClient* client, const std::string& namespace_name) {
  client_ = client;
  namespace_name_ = namespace_name;

  for (int perm = 0; perm < 6; perm++) {
    RETURN_NOT_OK(LoadTablets(perm, client, namespace_name));
  }

  initialized_ = true;
  LOG(INFO) << "TabletRouter initialized: "
            << "SPO=" << tablets_[0].size()
            << " POS=" << tablets_[1].size()
            << " OSP=" << tablets_[2].size()
            << " PSO=" << tablets_[3].size()
            << " SOP=" << tablets_[4].size()
            << " OPS=" << tablets_[5].size();
  return Status::OK();
}

Status TabletRouter::LoadTablets(int perm_index, yb::client::YBClient* client,
                                 const std::string& namespace_name) {
  client::YBTableName table_name(YQL_DATABASE_CQL, namespace_name,
                                 RdfYBModel::kTableNames[perm_index]);

  client::YBTablePtr table;
  RETURN_NOT_OK(client->OpenTable(table_name, &table));

  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablet_locations;
  RETURN_NOT_OK(client->GetTabletsFromTableId(table->id(), 0, &tablet_locations));

  std::vector<TabletInfo>& infos = tablets_[perm_index];
  infos.clear();
  infos.reserve(tablet_locations.size());

  for (const auto& loc : tablet_locations) {
    TabletInfo info;
    info.tablet_id = loc.tablet_id();
    info.partition_start = loc.partition().partition_key_start();
    info.partition_end = loc.partition().partition_key_end();

    for (const auto& replica : loc.replicas()) {
      if (replica.role() == PeerRole::LEADER) {
        HostPort hp = HostPortFromPB(
            DesiredHostPort(replica.ts_info(), CloudInfoPB()));
        info.leader_host = hp.host();
        info.leader_port = hp.port();
        break;
      }
    }

    infos.push_back(std::move(info));
  }

  return Status::OK();
}

const std::vector<TabletInfo>& TabletRouter::GetTablets(int perm_index) const {
  return tablets_[perm_index];
}

const TabletInfo* TabletRouter::LocateTablet(int perm_index,
                                              const std::string& partition_key) const {
  const auto& infos = tablets_[perm_index];
  for (const auto& info : infos) {
    if (partition_key >= info.partition_start &&
        (info.partition_end.empty() || partition_key < info.partition_end)) {
      return &info;
    }
  }
  return nullptr;
}

int TabletRouter::GetNumTablets(int perm_index) const {
  return static_cast<int>(tablets_[perm_index].size());
}

}  // namespace yb::mdb

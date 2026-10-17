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
#include <string>
#include <vector>

#include "yb/util/result.h"
#include "yb/util/status.h"

namespace yb {

namespace client {
class YBClient;
class YBTable;
}  // namespace client

namespace mdb {

struct TabletInfo {
  std::string tablet_id;
  std::string partition_start;
  std::string partition_end;
  std::string leader_host;
  uint32_t leader_port = 0;
};

struct QueryFragment {
  int perm_index;
  std::string partition_key_start;
  std::string partition_key_end;
};

class TabletRouter {
 public:
  TabletRouter() = default;

  Status Init(yb::client::YBClient* client, const std::string& namespace_name);

  const std::vector<TabletInfo>& GetTablets(int perm_index) const;

  const TabletInfo* LocateTablet(int perm_index, const std::string& partition_key) const;

  int GetNumTablets(int perm_index) const;

  bool IsInitialized() const { return initialized_; }

 private:
  Status LoadTablets(int perm_index, yb::client::YBClient* client,
                     const std::string& namespace_name);

  yb::client::YBClient* client_ = nullptr;
  std::string namespace_name_;
  bool initialized_ = false;
  std::vector<TabletInfo> tablets_[6];
};

}  // namespace mdb
}  // namespace yb

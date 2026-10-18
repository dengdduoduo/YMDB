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

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "yb/client/table_handle.h"
#include "yb/mdb/import/rdf_term.h"
#include "yb/common/read_hybrid_time.h"

namespace yb::mdb {

// Immutable term claims and a monotonic counter use single-row conditional
// writes. An interrupted allocation may waste an ID, but cannot reuse it.
class PersistentIdMap {
 public:
  Status Init(client::YBClient* client, const std::string& namespace_name);
  Result<uint64_t> GetOrCreate(const RdfTerm& term);
  Result<std::vector<std::pair<uint64_t, RdfTerm>>> Entries(
      const ReadHybridTime& read_time = ReadHybridTime());
  static constexpr const char* kTableName = "rdf_terms_v2";

 private:
  Result<std::optional<int64_t>> Read(
      const std::string& key, const ReadHybridTime& read_time = ReadHybridTime());
  Result<bool> CompareExchange(
      const std::string& key, std::optional<int64_t> expected, int64_t value);
  client::YBClient* client_ = nullptr;
  client::TableHandle table_;
  bool initialized_ = false;
};

}  // namespace yb::mdb

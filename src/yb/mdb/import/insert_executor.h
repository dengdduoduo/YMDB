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

#include <functional>
#include <string>
#include <string_view>
#include <vector>

#include "yb/mdb/import/rdf_term.h"
#include "yb/util/status.h"

namespace yb::mdb {

class InsertDataExecutor {
 public:
  // Legacy callers must persist each term through the callback before graph
  // writes. Dictionary v2 persists terms itself. An active caller transaction
  // is rejected unchanged; this executor owns its write transactions.
  static Status Execute(const std::string& insert_data_query,
                        IdMap& id_map,
                        ImportStats& stats,
                        const std::function<Status(uint64_t, const RdfTerm&)>& persist = {});

 private:
  static Result<std::string> ExtractTripleBlock(const std::string& query);
};

}  // namespace yb::mdb

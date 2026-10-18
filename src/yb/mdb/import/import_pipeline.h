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

#include <string>
#include <vector>

#include "yb/mdb/import/import.h"
#include "yb/util/result.h"
#include "yb/util/status.h"

namespace yb::mdb {

class ImportPipeline {
 public:
  ImportPipeline() = default;

  // Nonempty standalone imports require dictionary v2; legacy process-local IDs
  // cannot be recovered by another reader. Existing legacy data is not migrated.
  // Successful imports flush their writes. An active transaction remains owned
  // by the caller and must still be committed before other sessions can read it.
  Result<ImportStats> ImportFile(const std::string& file_path);

  Result<ImportStats> ImportTriples(const std::vector<Triple>& triples);

  const ImportStats& stats() const { return stats_; }

 private:
  Result<ImportStats> ImportNtFile(const std::string& file_path);
  Result<ImportStats> ImportCsvFile(const std::string& file_path);
  Result<ImportStats> ImportRdfXmlFile(const std::string& file_path);
  Status BatchWrite(const std::vector<Triple>& triples);

  std::string document_scope_;
  IdMap id_map_;
  ImportStats stats_;
};

}  // namespace yb::mdb

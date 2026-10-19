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

  Result<ImportStats> ImportFile(const std::string& file_path);

  Result<ImportStats> ImportTriples(const std::vector<Triple>& triples);

  const ImportStats& stats() const { return stats_; }

 private:
  Result<ImportStats> ImportNtFile(const std::string& file_path);
  Result<ImportStats> ImportCsvFile(const std::string& file_path);
  Result<ImportStats> ImportRdfXmlFile(const std::string& file_path);
  Status BatchWrite(const std::vector<Triple>& triples);

  IdMap id_map_;
  std::string blank_scope_;
  ImportStats stats_;
};

}  // namespace yb::mdb

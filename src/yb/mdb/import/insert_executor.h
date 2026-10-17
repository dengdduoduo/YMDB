#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "yb/mdb/import/import.h"
#include "yb/util/status.h"

namespace yb::mdb {

class InsertDataExecutor {
 public:
  static Status Execute(const std::string& insert_data_query,
                        IdMap& id_map,
                        ImportStats& stats);

 private:
  static std::string ExtractTripleBlock(const std::string& query);
};

}  // namespace yb::mdb

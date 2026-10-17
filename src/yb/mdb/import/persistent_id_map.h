#pragma once

#include <optional>
#include <string>
#include <string_view>

#include "yb/client/table_handle.h"
#include "yb/util/result.h"

namespace yb::mdb {

// The counter and term claims use single-row conditional writes. Allocations
// may leave gaps after a crash or a competing claim, but never reuse an ID.
class PersistentIdMap {
 public:
  Status Init(client::YBClient* client, const std::string& namespace_name);
  Result<uint64_t> GetOrCreate(std::string_view term);
  static constexpr const char* kTableName = "rdf_terms";

 private:
  Result<std::optional<int64_t>> Read(const std::string& key);
  Result<bool> CompareExchange(
      const std::string& key, std::optional<int64_t> expected, int64_t value);
  Status EnsureMinimumId(int64_t minimum);
  Status MigrateLegacy(const std::string& namespace_name);

  client::YBClient* client_ = nullptr;
  client::TableHandle table_;
  bool initialized_ = false;
};

}  // namespace yb::mdb

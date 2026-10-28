#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "yb/client/table_handle.h"
#include "yb/util/result.h"

namespace yb::mdb {

class IdMap;

// The counter and term claims use single-row conditional writes. Allocations
// may leave gaps after a crash or a competing claim, but never reuse an ID.
class PersistentIdMap {
 public:
  // Counts legacy term claims only; not total storage operations.
  struct InitStats {
    uint64_t legacy_terms = 0;
    uint64_t conditional_claims = 0;
    uint64_t known_terms = 0;
    uint64_t new_terms = 0;
  };
  struct ScanStats {
    uint64_t rows = 0;
    uint64_t page_read_operations = 0;
    uint64_t response_bytes = 0;
    bool page_counts_available = false;
  };
  const ScanStats& scan_stats() const { return scan_stats_; }
  const InitStats& init_stats() const { return init_stats_; }
  Status Init(client::YBClient* client, const std::string& namespace_name);
  Result<uint64_t> GetOrCreate(std::string_view term);
  // Claims are durable before their data commits; the legacy mirror may lag.
  // A failed scan or inconsistent mapping leaves destination unchanged.
  Status Restore(IdMap* destination);
  static constexpr const char* kTableName = "rdf_terms";

 private:
  Result<std::optional<int64_t>> Read(const std::string& key);
  Result<bool> CompareExchange(
      const std::string& key, std::optional<int64_t> expected, int64_t value);
  Status EnsureMinimumId(int64_t minimum);
  Status MigrateLegacy(const std::string& namespace_name);
  Status ScanClaims(IdMap* destination);

  client::YBClient* client_ = nullptr;
  client::TableHandle table_;
  bool initialized_ = false;
  InitStats init_stats_;
  ScanStats scan_stats_;
};

}  // namespace yb::mdb

// Copyright (c) YugabyteDB, Inc.

#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <vector>

#include "yb/client/client_fwd.h"
#include "yb/util/monotime.h"
#include "yb/util/result.h"

namespace yb::mdb {

struct DocDBScanSpec {
  struct Range {
    uint64_t min = 0;
    uint64_t max = std::numeric_limits<uint64_t>::max();
  };
  // Logical IDs in the selected table's column order, not physical hash keys.
  std::array<Range, 3> columns;
  std::optional<uint64_t> hash_key;
  // Encoded YCQL hash partition bounds [start, end); empty means unbounded.
  std::string partition_start;
  std::string partition_end;
  size_t page_bytes = 64 * 1024;
  size_t page_rows = 256;
  CoarseTimePoint deadline = CoarseMonoClock::Now() + MonoDelta::FromSeconds(30);
  const std::atomic<bool>* cancelled = nullptr;
  bool push_exact_filters = false;
};

struct DocDBScanBatch {
  using Row = std::array<uint64_t, 3>;
  std::vector<Row> rows;
  bool eof = false;
};

// Synchronous, query-owned cursor. The caller supplies the query's read
// session (and transaction/read point) and keeps the table and cancellation
// token alive. Never share a session concurrently between workers.
class DocDBScanCursor {
 public:
  Status Open(const client::TableHandle* table, client::YBSessionPtr session,
              DocDBScanSpec spec);
  Result<DocDBScanBatch> NextBatch();
  // Queue one page per cursor, then flush once. All cursors must use the same
  // query session; the client routes the bounded operation set concurrently.
  static Result<std::vector<DocDBScanBatch>> NextBatches(
      const std::vector<DocDBScanCursor*>& cursors);
  void Close();
  uint64_t rpc_calls() const { return rpc_calls_; }
  uint64_t payload_bytes() const { return payload_bytes_; }
  uint64_t decoded_rows() const { return decoded_rows_; }
  uint64_t retained_payload_bytes() const { return retained_payload_bytes_; }

 private:
  Result<client::YBqlReadOpPtr> PreparePage();
  Result<DocDBScanBatch> ConsumePage(client::YBqlReadOpPtr op);

  const client::TableHandle* table_ = nullptr;
  client::YBSessionPtr session_;
  client::YBqlReadOpPtr previous_;
  DocDBScanSpec spec_;
  Status failure_;
  size_t row_limit_ = 0;
  bool eof_ = false;
  uint64_t rpc_calls_ = 0;
  uint64_t payload_bytes_ = 0;
  uint64_t decoded_rows_ = 0;
  uint64_t retained_payload_bytes_ = 0;
};

}  // namespace yb::mdb

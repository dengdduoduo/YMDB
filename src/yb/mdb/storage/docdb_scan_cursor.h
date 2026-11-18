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
      const std::vector<DocDBScanCursor*>& cursors, bool trace_second_hop = false);
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

enum class GraphDirection { Out, In };

// A tablet-routed adjacency scan. Continuation state belongs to this cursor
// and remains bound to the original query session and read view.
class DocDBNeighborCursor {
 public:
  Status Open(const std::array<const client::TableHandle*, 6>& tables,
              const client::YBSessionPtr& session, GraphDirection direction,
              uint64_t start, uint64_t predicate, uint64_t bound_neighbor,
              size_t page_rows, CoarseTimePoint deadline, const std::atomic<bool>& cancelled);

  // One page per input, in input order. The client groups the underlying
  // reads into tablet Read RPCs, each carrying a ql_batch.
  static Result<std::vector<DocDBScanBatch>> ScanNeighbors(
      const std::vector<DocDBNeighborCursor*>& cursors, bool trace_second_hop = false);
  uint64_t read_operations() const { return cursor_.rpc_calls(); }
  uint64_t retained_payload_bytes() const { return cursor_.retained_payload_bytes(); }

 private:
  DocDBScanCursor cursor_;
};

}  // namespace yb::mdb

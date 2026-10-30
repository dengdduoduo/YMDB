// Copyright (c) YugabyteDB, Inc.
#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "yb/client/client.h"
#include "yb/client/session.h"
#include "yb/client/table_handle.h"
#include "yb/client/yb_op.h"
#include "yb/mdb/storage/yb_client_compat.h"
#include "yb/qlexpr/ql_rowblock.h"
#include "yb/util/monotime.h"
#include "yb/util/result.h"
#include "yb/util/status.h"

namespace yb::mdb {

struct LegacyDictionaryScanStats {
  uint64_t rows = 0;
  uint64_t page_read_operations = 0;
  uint64_t response_bytes = 0;
};

// Visitors must copy any row data they retain beyond the callback.
template <class Visitor>
Status ScanLegacyDictionaryRows(
    const client::TableHandle& table, client::YBClient* client,
    const std::vector<std::string>& columns, Visitor&& visitor,
    LegacyDictionaryScanStats* stats = nullptr) {
  constexpr uint64_t kPageRows = 1024;
  if (stats) *stats = {};
  if (!client || columns.empty()) return STATUS(InvalidArgument, "Invalid legacy scan input");
  auto session = client->NewSession(MonoDelta::FromSeconds(60));
  client::YBqlReadOpPtr previous;
  auto reset_arena = [](const auto& value) {
    if constexpr (requires { value->ResetArena(nullptr); }) value->ResetArena(nullptr);
  };
  for (;;) {
    // Keep the preceding arena alive until its continuation has been copied.
    reset_arena(session);
    auto op = NewMdbReadOp(table, session);
    op->set_yb_consistency_level(YBConsistencyLevel::STRONG);
    auto* request = op->mutable_request();
    table.AddColumns(columns, request);
    request->set_limit(kPageRows);
    request->set_return_paging_state(true);
    if (previous) *request->mutable_paging_state() = previous->response().paging_state();
    if (stats) ++stats->page_read_operations;
    RETURN_NOT_OK(session->TEST_ApplyAndFlush(op));
    if (op->response().status() != QLResponsePB::YQL_STATUS_OK) {
      return STATUS(RuntimeError,
          "Scan legacy RDF dictionary: " + std::string(op->response().error_message()));
    }
    if (stats) stats->response_bytes += op->rows_data().size();
    auto rows = VERIFY_RESULT(op->MakeRowBlock());
    if (rows.row_count() > kPageRows)
      return STATUS(Corruption, "Legacy RDF dictionary page exceeds its row limit");
    for (const auto& row : rows.rows()) {
      if (stats) ++stats->rows;
      RETURN_NOT_OK(visitor(row));
    }
    // An empty tablet can still return a continuation to the next partition.
    if (!op->response().has_paging_state()) break;
    if (previous) {
      const auto& before = previous->response().paging_state();
      const auto& after = op->response().paging_state();
      if (before.next_partition_key() == after.next_partition_key() &&
          before.next_row_key() == after.next_row_key()) {
        return STATUS(Corruption, "Legacy RDF dictionary scan continuation did not advance");
      }
    }
    previous = std::move(op);
  }
  return Status::OK();
}

}  // namespace yb::mdb

// Copyright (c) YugabyteDB, Inc.

#include "yb/mdb/storage/docdb_scan_cursor.h"

#include <algorithm>
#include <utility>

#include "yb/client/session.h"
#include "yb/client/table_handle.h"
#include "yb/client/yb_op.h"
#include "yb/common/ql_protocol_util.h"
#include "yb/common/ql_value.h"
#include "yb/dockv/partition.h"
#include "yb/mdb/storage/yb_client_compat.h"
#include "yb/mdb/storage/docdb_read_context.h"
#include "yb/mdb/query/executor/wall_time_scope.h"

namespace yb::mdb {
namespace {
template <class Session>
void StartPageArena(const Session& session) {
  // Newer clients otherwise retain every page's protobuf allocations for the
  // entire session. All reads here flush synchronously; outstanding cursor
  // operations retain their own arena until their continuation is consumed.
  if constexpr (requires { session->ResetArena(nullptr); }) {
    session->ResetArena(nullptr);
  }
}
}  // namespace

Status DocDBScanCursor::Open(
    const client::TableHandle* table, client::YBSessionPtr session, DocDBScanSpec spec) {
  Close();
  if (!table || !session) return STATUS(InvalidArgument, "Missing MDB scan table or session");
  if (spec.page_rows == 0 || spec.page_bytes < sizeof(DocDBScanBatch::Row)) {
    return STATUS(InvalidArgument, "MDB scan page budget cannot hold one row");
  }
  for (const auto* bound : {&spec.partition_start, &spec.partition_end}) {
    if (!bound->empty() && bound->size() != sizeof(uint16_t)) {
      return STATUS(NotSupported, "MDB scan requires encoded YCQL hash partition bounds");
    }
  }
  if (!spec.partition_end.empty() && spec.partition_start >= spec.partition_end) {
    return STATUS(InvalidArgument, "MDB scan partition bounds are empty or reversed");
  }
  row_limit_ = std::min(spec.page_rows, spec.page_bytes / sizeof(DocDBScanBatch::Row));
  table_ = table;
  session_ = std::move(session);
  spec_ = std::move(spec);
  for (const auto& range : spec_.columns) {
    if (range.min > range.max) eof_ = true;
  }
  return Status::OK();
}

Result<DocDBScanBatch> DocDBScanCursor::NextBatch() {
  auto batches = VERIFY_RESULT(NextBatches({this}));
  return std::move(batches.front());
}

Result<std::vector<DocDBScanBatch>> DocDBScanCursor::NextBatches(
    const std::vector<DocDBScanCursor*>& cursors) {
  if (cursors.empty()) return std::vector<DocDBScanBatch>();
  if (!cursors.front()) return STATUS(InvalidArgument, "Null MDB batch cursor");
  const auto session = cursors.front()->session_;
  if (!session) return STATUS(IllegalState, "MDB scan cursor is closed");
  for (auto* cursor : cursors) {
    if (!cursor || !cursor->table_ || cursor->session_ != session) {
      return STATUS(InvalidArgument, "MDB batch cursors need one query-owned session");
    }
    RETURN_NOT_OK(cursor->failure_);
  }
  auto execute = [&]() -> Result<std::vector<DocDBScanBatch>> {
    StartPageArena(session);
    std::vector<client::YBqlReadOpPtr> ops;
    ops.reserve(cursors.size());
    for (auto* cursor : cursors) ops.push_back(VERIFY_RESULT(cursor->PreparePage()));
    bool pending = false;
    for (size_t i = 0; i < ops.size(); ++i) {
      if (!ops[i]) continue;
      ++cursors[i]->rpc_calls_;  // Read operations, not a claim about wire RPC count.
      session->Apply(ops[i]);
      pending = true;
    }
    if (pending) {
      auto deadline = cursors.front()->spec_.deadline;
      for (auto* cursor : cursors) deadline = std::min(deadline, cursor->spec_.deadline);
      session->SetDeadline(deadline);
      {
        auto* context = DocDBReadContextScope::Current();
        WallTimeScope timer(context && context->session == session ?
            &context->stats.storage_flush_us : nullptr);
        RETURN_NOT_OK(session->TEST_Flush());
      }
      if (session->IsRestartRequired()) return STATUS(TryAgain, "MDB query read view requires restart");
    }
    auto* context = DocDBReadContextScope::Current();
    WallTimeScope timer(context && context->session == session ?
        &context->stats.storage_decode_us : nullptr);
    std::vector<DocDBScanBatch> batches;
    batches.reserve(cursors.size());
    for (size_t i = 0; i < cursors.size(); ++i) {
      batches.push_back(VERIFY_RESULT(cursors[i]->ConsumePage(std::move(ops[i]))));
    }
    return batches;
  };
  auto result = execute();
  if (!result.ok()) {
    for (auto* cursor : cursors) {
      cursor->failure_ = result.status();
      cursor->previous_.reset();
    }
  }
  return result;
}

Result<client::YBqlReadOpPtr> DocDBScanCursor::PreparePage() {
  if (spec_.cancelled && spec_.cancelled->load(std::memory_order_relaxed)) {
    return STATUS(Aborted, "MDB scan cancelled");
  }
  if (CoarseMonoClock::Now() >= spec_.deadline) {
    return STATUS(TimedOut, "MDB scan deadline exceeded");
  }
  if (eof_) return client::YBqlReadOpPtr();
  if (session_->IsRestartRequired()) {
    return STATUS(TryAgain, "MDB query read view requires restart");
  }
  auto op = NewMdbReadOp(*table_, session_);
  auto* req = op->mutable_request();
  if (spec_.hash_key) {
    QLAddInt64HashValue(req, static_cast<int64_t>(*spec_.hash_key));
    std::string key;
    RETURN_NOT_OK(op->GetPartitionKey(&key));
    if (key < spec_.partition_start ||
        (!spec_.partition_end.empty() && key >= spec_.partition_end)) {
      eof_ = true;
      return client::YBqlReadOpPtr();
    }
  } else {
    if (!spec_.partition_start.empty()) {
      req->set_hash_code(dockv::PartitionSchema::DecodeMultiColumnHashValue(spec_.partition_start));
    }
    if (!spec_.partition_end.empty()) {
      const auto end = dockv::PartitionSchema::DecodeMultiColumnHashValue(spec_.partition_end);
      if (end == 0) {
        eof_ = true;
        return client::YBqlReadOpPtr();
      }
      req->set_max_hash_code(end - 1);
    }
  }
  table_->AddColumns({"c0", "c1", "c2"}, req);
  if (spec_.push_exact_filters && (spec_.columns[1].min == spec_.columns[1].max ||
                                   spec_.columns[2].min == spec_.columns[2].max)) {
    auto* where = req->mutable_where_expr()->mutable_condition();
    where->set_op(QL_OP_AND);
    for (int col = 1; col < 3; ++col) {
      if (spec_.columns[col].min == spec_.columns[col].max) {
        table_->AddInt64Condition(where, "c" + std::to_string(col), QL_OP_EQUAL,
                                 static_cast<int64_t>(spec_.columns[col].min));
      }
    }
  }
  req->set_limit(row_limit_);
  req->set_return_paging_state(true);
  if (previous_) {
    *req->mutable_paging_state() = previous_->response().paging_state();
  }
  return op;
}

Result<DocDBScanBatch> DocDBScanCursor::ConsumePage(client::YBqlReadOpPtr op) {
  DocDBScanBatch batch;
  if (!op) { batch.eof = true; return batch; }
  if (op->response().status() != QLResponsePB::YQL_STATUS_OK) {
    return STATUS(RuntimeError, "MDB scan failed: " + std::string(op->response().error_message()));
  }
  auto rows = VERIFY_RESULT(op->MakeRowBlock());
  payload_bytes_ += op->rows_data().size();
  decoded_rows_ += rows.row_count();
  if (rows.row_count() > row_limit_) {
    return STATUS(Corruption, "MDB scan response exceeds requested row budget");
  }
  batch.rows.reserve(rows.row_count());
  for (const auto& row : rows.rows()) {
    DocDBScanBatch::Row values;
    bool matches = true;
    for (int column = 0; column < 3; ++column) {
      if (row.column(column).IsNull()) return STATUS(Corruption, "Null MDB index key");
      values[column] = static_cast<uint64_t>(row.column(column).int64_value());
      const auto& range = spec_.columns[column];
      matches = matches && values[column] >= range.min && values[column] <= range.max;
    }
    if (matches) batch.rows.push_back(values);
  }
  eof_ = !op->response().has_paging_state();
  if (!eof_) {
    const auto& next = op->response().paging_state();
    const std::string next_partition(next.next_partition_key());
    if (!spec_.partition_end.empty() && next_partition >= spec_.partition_end) {
      eof_ = true;
    } else if (!next_partition.empty() && next_partition < spec_.partition_start) {
      return STATUS(Corruption, "MDB scan continuation moved before its partition boundary");
    }
    if (!eof_ && previous_) {
      const auto& old = previous_->response().paging_state();
      if (next.next_partition_key() == old.next_partition_key() &&
          next.next_row_key() == old.next_row_key()) {
        return STATUS(Corruption, "MDB scan continuation did not advance");
      }
    }
  }
  batch.eof = eof_;
  retained_payload_bytes_ = eof_ ? 0 : op->rows_data().size();
  previous_ = eof_ ? nullptr : std::move(op);
  // An empty filtered page is not EOF when the server provided continuation.
  return batch;
}

void DocDBScanCursor::Close() {
  previous_.reset();
  session_.reset();
  table_ = nullptr;
  spec_ = DocDBScanSpec();
  failure_ = Status::OK();
  row_limit_ = 0;
  eof_ = false;
  rpc_calls_ = 0;
  payload_bytes_ = 0;
  decoded_rows_ = 0;
  retained_payload_bytes_ = 0;
}

}  // namespace yb::mdb

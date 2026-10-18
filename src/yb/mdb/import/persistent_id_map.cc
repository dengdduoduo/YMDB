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

#include "yb/mdb/import/persistent_id_map.h"

#include <algorithm>
#include <vector>

#include "yb/client/client.h"
#include "yb/client/schema.h"
#include "yb/client/session.h"
#include "yb/client/yb_op.h"
#include "yb/client/yb_table_name.h"
#include "yb/common/ql_protocol_util.h"
#include "yb/common/schema.h"
#include "yb/common/schema_pbutil.h"
#include "yb/mdb/import/import.h"
#include "yb/qlexpr/ql_rowblock.h"

namespace yb::mdb {
namespace {
constexpr const char* kCounterKey = "counter";
constexpr int kMaxAttempts = 128;
}  // namespace

Status PersistentIdMap::Init(client::YBClient* client, const std::string& namespace_name) {
  initialized_ = false;
  if (!client) return STATUS(InvalidArgument, "YBClient is null");
  client_ = client;
  client::YBTableName name(YQL_DATABASE_CQL, namespace_name, kTableName);
  client::YBSchemaBuilder builder;
  builder.AddColumn("term_key")->Type(DataType::STRING)->HashPrimaryKey()->NotNull();
  builder.AddColumn("term_id")->Type(DataType::INT64)->NotNull();
  auto status = table_.Create(name, 1, client_, &builder);
  if (!status.ok() && !status.IsAlreadyPresent()) return status;
  RETURN_NOT_OK(table_.Open(name, client_));
  if (!VERIFY_RESULT(Read(kCounterKey))) {
    Status scan_status;
    bool has_terms = false;
    client::TableIteratorOptions options;
    options.columns = {"term_key"};
    options.error_handler = [&scan_status](const Status& s) { scan_status = s; };
    for (const auto& row : client::TableRange(table_, options)) {
      if (row.column(0).string_value() != kCounterKey) has_terms = true;
    }
    RETURN_NOT_OK(scan_status);
    if (has_terms && !VERIFY_RESULT(Read(kCounterKey))) {
      return STATUS(Corruption, "RDF dictionary counter missing beside existing terms");
    }
    VERIFY_RESULT(CompareExchange(kCounterKey, std::nullopt, 0));
  }
  initialized_ = true;
  const auto entries = Entries();
  if (!entries.ok()) {
    initialized_ = false;
    return entries.status();
  }
  return Status::OK();
}

Result<std::optional<int64_t>> PersistentIdMap::Read(
    const std::string& key, const ReadHybridTime& read_time) {
  auto session = client_->NewSession(MonoDelta::FromSeconds(30));
  auto op = table_.NewReadOp(session->arena());
  if (read_time) op->SetReadTime(read_time);
  QLAddStringHashValue(op->mutable_request(), key);
  table_.AddColumns({"term_id"}, op->mutable_request());
  RETURN_NOT_OK(session->TEST_ApplyAndFlush(op));
  if (op->response().status() != QLResponsePB::YQL_STATUS_OK) {
    return STATUS(
        RuntimeError, "Read RDF dictionary: " + std::string(op->response().error_message()));
  }
  auto rows = VERIFY_RESULT(op->MakeRowBlock());
  if (rows.row_count() == 0) return std::optional<int64_t>();
  if (rows.row_count() != 1 || rows.row(0).column(0).IsNull()) {
    return STATUS(Corruption, "Invalid RDF dictionary row");
  }
  auto value = rows.row(0).column(0).int64_value();
  if (value < 0 || static_cast<uint64_t>(value) > IdMap::kMaxId) {
    return STATUS(Corruption, "RDF term ID is outside the 56-bit range");
  }
  return std::optional<int64_t>(value);
}

Result<bool> PersistentIdMap::CompareExchange(
    const std::string& key, std::optional<int64_t> expected, int64_t value) {
  auto session = client_->NewSession(MonoDelta::FromSeconds(30));
  auto op = table_.NewInsertOp(session->arena());
  auto* req = op->mutable_request();
  QLAddStringHashValue(req, key);
  table_.AddInt64ColumnValue(req, "term_id", value);
  if (expected) {
    table_.SetInt64Condition(
        req->mutable_if_expr()->mutable_condition(), "term_id", QL_OP_EQUAL, *expected);
    req->mutable_column_refs()->add_ids(table_.ColumnId("term_id"));
  } else {
    req->mutable_if_expr()->mutable_condition()->set_op(QL_OP_NOT_EXISTS);
  }
  RETURN_NOT_OK(session->TEST_ApplyAndFlush(op));
  if (op->response().status() != QLResponsePB::YQL_STATUS_OK) {
    return STATUS(
        RuntimeError, "Claim RDF term ID: " + std::string(op->response().error_message()));
  }
  std::vector<ColumnSchema> columns;
  for (const auto& column : op->response().column_schemas()) {
    columns.push_back(ColumnSchemaFromPB(column));
  }
  if (columns.empty() || columns.front().name() != "[applied]") {
    return STATUS(Corruption, "Missing conditional-write result for RDF dictionary");
  }
  qlexpr::QLRowBlock rows{Schema(columns)};
  auto data = op->rows_data().AsSlice();
  RETURN_NOT_OK(rows.Deserialize(YQL_CLIENT_CQL, &data));
  if (rows.row_count() != 1 || rows.row(0).column(0).IsNull()) {
    return STATUS(Corruption, "Invalid conditional-write result for RDF dictionary");
  }
  return rows.row(0).column(0).bool_value();
}

Result<uint64_t> PersistentIdMap::GetOrCreate(const RdfTerm& term) {
  if (!initialized_) return STATUS(IllegalState, "Persistent RDF dictionary is not initialized");
  const auto key = VERIFY_RESULT(term.Key());
  if (auto existing = VERIFY_RESULT(Read(key))) {
    if (*existing == 0) return STATUS(Corruption, "RDF term ID zero is reserved");
    return static_cast<uint64_t>(*existing);
  }
  for (int attempt = 0; attempt < kMaxAttempts; ++attempt) {
    auto current = VERIFY_RESULT(Read(kCounterKey));
    if (!current) return STATUS(Corruption, "Missing RDF ID counter");
    if (*current == static_cast<int64_t>(IdMap::kMaxId)) {
      return STATUS(IllegalState, "RDF term ID space exhausted");
    }
    const auto next = *current + 1;
    if (!VERIFY_RESULT(CompareExchange(kCounterKey, current, next))) continue;
    if (VERIFY_RESULT(CompareExchange(key, std::nullopt, next))) {
      return static_cast<uint64_t>(next);
    }
    auto winner = VERIFY_RESULT(Read(key));
    if (!winner || *winner == 0) return STATUS(Corruption, "Missing claimed RDF term");
    return static_cast<uint64_t>(*winner);
  }
  return STATUS(TryAgain, "RDF ID counter is busy");
}

Result<std::vector<std::pair<uint64_t, RdfTerm>>> PersistentIdMap::Entries(
    const ReadHybridTime& read_time) {
  if (!initialized_) return STATUS(IllegalState, "Persistent RDF dictionary is not initialized");
  std::vector<std::pair<uint64_t, RdfTerm>> result;
  Status scan_status;
  IdMap verified;
  uint64_t maximum = 0;
  client::TableIteratorOptions options;
  options.read_time = read_time;
  options.columns = {"term_key", "term_id"};
  options.error_handler = [&scan_status](const Status& s) {
    if (scan_status.ok()) scan_status = s;
  };
  for (const auto& row : client::TableRange(table_, options)) {
    if (row.column(0).IsNull() || row.column(1).IsNull()) {
      return STATUS(Corruption, "Incomplete RDF dictionary row");
    }
    const auto key = row.column(0).string_value();
    if (key == kCounterKey) continue;
    const auto id = row.column(1).int64_value();
    if (id <= 0 || static_cast<uint64_t>(id) > IdMap::kMaxId) {
      return STATUS(Corruption, "Invalid RDF term ID");
    }
    auto term = VERIFY_RESULT(RdfTerm::FromKey(key));
    if (!verified.Restore(id, term)) return STATUS(Corruption, "Conflicting RDF dictionary IDs");
    maximum = std::max(maximum, static_cast<uint64_t>(id));
    result.emplace_back(id, std::move(term));
  }
  RETURN_NOT_OK(scan_status);
  const auto counter = VERIFY_RESULT(Read(kCounterKey, read_time));
  if (!counter || static_cast<uint64_t>(*counter) < maximum) {
    return STATUS(Corruption, "RDF dictionary counter trails assigned IDs");
  }
  return result;
}

}  // namespace yb::mdb

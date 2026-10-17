#include "yb/mdb/import/persistent_id_map.h"

#include <limits>
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
#include "yb/mdb/storage/yb_client_compat.h"
#include "yb/qlexpr/ql_rowblock.h"

namespace yb::mdb {
namespace {
constexpr const char* kCounterKey = "counter";
constexpr int kMaxAttempts = 128;

std::string TermKey(std::string_view term) { return "term:" + std::string(term); }
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
  // Upgrade the old dictionary before accepting allocations in this client.
  RETURN_NOT_OK(MigrateLegacy(namespace_name));
  RETURN_NOT_OK(EnsureMinimumId(0));
  initialized_ = true;
  return Status::OK();
}

Result<std::optional<int64_t>> PersistentIdMap::Read(const std::string& key) {
  auto session = client_->NewSession(MonoDelta::FromSeconds(30));
  auto op = NewMdbReadOp(table_, session);
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
  if (value < 0) return STATUS(Corruption, "Negative RDF term ID");
  return std::optional<int64_t>(value);
}

Result<bool> PersistentIdMap::CompareExchange(
    const std::string& key, std::optional<int64_t> expected, int64_t value) {
  auto session = client_->NewSession(MonoDelta::FromSeconds(30));
  auto op = NewMdbInsertOp(table_, session);
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

Status PersistentIdMap::EnsureMinimumId(int64_t minimum) {
  for (int attempt = 0; attempt < kMaxAttempts; ++attempt) {
    auto current = VERIFY_RESULT(Read(kCounterKey));
    if (current && *current >= minimum) return Status::OK();
    if (VERIFY_RESULT(CompareExchange(kCounterKey, current, minimum))) return Status::OK();
  }
  return STATUS(TryAgain, "RDF ID counter is busy");
}

Status PersistentIdMap::MigrateLegacy(const std::string& namespace_name) {
  client::TableHandle legacy;
  auto status =
      legacy.Open(client::YBTableName(YQL_DATABASE_CQL, namespace_name, "rdf_idmap"), client_);
  if (status.IsNotFound()) return Status::OK();
  RETURN_NOT_OK(status);
  IdMap restored;
  Status scan_status;
  client::TableIteratorOptions options;
  options.columns = std::vector<std::string>{"gmap_id", "uri"};
  options.error_handler = [&scan_status](const Status& s) { scan_status = s; };
  for (const auto& row : client::TableRange(legacy, options)) {
    auto id = row.column(0).int64_value();
    if (id <= 0 || row.column(1).IsNull() ||
        !restored.Restore(static_cast<uint64_t>(id), row.column(1).string_value())) {
      return STATUS(Corruption, "Conflicting or incomplete legacy RDF dictionary");
    }
  }
  RETURN_NOT_OK(scan_status);
  RETURN_NOT_OK(EnsureMinimumId(static_cast<int64_t>(restored.MaxId())));
  for (const auto& [id, term] : restored.Entries()) {
    if (!VERIFY_RESULT(CompareExchange(TermKey(term), std::nullopt, id))) {
      auto existing = VERIFY_RESULT(Read(TermKey(term)));
      if (!existing || *existing != static_cast<int64_t>(id)) {
        return STATUS(Corruption, "Legacy RDF term conflicts with persistent dictionary");
      }
    }
  }
  return Status::OK();
}

Result<uint64_t> PersistentIdMap::GetOrCreate(std::string_view term) {
  if (!initialized_) return STATUS(IllegalState, "Persistent RDF dictionary is not initialized");
  const auto key = TermKey(term);
  if (auto existing = VERIFY_RESULT(Read(key))) {
    if (*existing == 0) return STATUS(Corruption, "RDF term ID zero is reserved");
    return static_cast<uint64_t>(*existing);
  }
  for (int attempt = 0; attempt < kMaxAttempts; ++attempt) {
    auto current = VERIFY_RESULT(Read(kCounterKey));
    if (!current) return STATUS(Corruption, "Missing RDF ID counter");
    if (*current == std::numeric_limits<int64_t>::max()) {
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

}  // namespace yb::mdb

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
#include "yb/mdb/storage/legacy_dictionary_scan.h"
#include "yb/qlexpr/ql_rowblock.h"

namespace yb::mdb {
namespace {
constexpr const char* kCounterKey = "counter";
constexpr int kMaxAttempts = 128;

std::string TermKey(std::string_view term) { return "term:" + std::string(term); }
}  // namespace

Status PersistentIdMap::Init(client::YBClient* client, const std::string& namespace_name) {
  initialized_ = false;
  init_stats_ = {};
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
  IdMap restored;
  auto restore_status = Restore(&restored);
  if (!restore_status.ok()) initialized_ = false;
  return restore_status;
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
  RETURN_NOT_OK(ScanLegacyDictionaryRows(
      legacy, client_, {"gmap_id", "uri"}, [&](const auto& row) -> Status {
        if (row.column(0).IsNull() || row.column(1).IsNull())
          return STATUS(Corruption, "Incomplete legacy RDF dictionary row");
        const auto id = row.column(0).int64_value();
        if (id <= 0 ||
            !restored.Restore(static_cast<uint64_t>(id), row.column(1).string_value())) {
          return STATUS(Corruption, "Conflicting or incomplete legacy RDF dictionary");
        }
        return Status::OK();
      }));
  init_stats_.legacy_terms = restored.Size();
  RETURN_NOT_OK(EnsureMinimumId(static_cast<int64_t>(restored.MaxId())));
  if (restored.Size() == 0) return Status::OK();
  IdMap claims;
  RETURN_NOT_OK(ScanClaims(&claims));
  // Validate the entire legacy set before publishing anything. Otherwise a
  // missing term can claim an ID already owned by a different term.
  for (const auto& [id, term] : restored.Entries()) {
    const auto by_id = claims.Entries().find(id);
    if ((claims.Contains(term) && claims.Get(term) != id) ||
        (by_id != claims.Entries().end() && by_id->second != term)) {
      return STATUS(Corruption, "Legacy RDF term conflicts with persistent dictionary");
    }
  }
  for (const auto& [id, term] : restored.Entries()) {
    // Claims are immutable. A strong scan can validate existing claims in
    // batches; missing claims still arbitrate with concurrent clients by CAS.
    if (claims.Contains(term)) {
      ++init_stats_.known_terms;
      continue;
    }
    ++init_stats_.conditional_claims;
    if (!VERIFY_RESULT(CompareExchange(TermKey(term), std::nullopt, id))) {
      auto existing = VERIFY_RESULT(Read(TermKey(term)));
      if (!existing || *existing != static_cast<int64_t>(id)) {
        return STATUS(Corruption, "Legacy RDF term conflicts with persistent dictionary");
      }
      ++init_stats_.known_terms;
    } else {
      ++init_stats_.new_terms;
    }
  }
  return Status::OK();
}

Status PersistentIdMap::ScanClaims(IdMap* destination) {
  constexpr uint64_t kPageRows = 1024;
  scan_stats_ = {};
  scan_stats_.page_counts_available = true;
  IdMap restored;
  auto session = client_->NewSession(MonoDelta::FromSeconds(60));
  client::YBqlReadOpPtr previous;
  auto reset_arena = [](const auto& value) {
    if constexpr (requires { value->ResetArena(nullptr); }) value->ResetArena(nullptr);
  };
  for (;;) {
    // Keep the preceding operation alive while copying its continuation into
    // the new arena; completed pages must not accumulate for the whole scan.
    reset_arena(session);
    auto op = NewMdbReadOp(table_, session);
    op->set_yb_consistency_level(YBConsistencyLevel::STRONG);
    auto* request = op->mutable_request();
    table_.AddColumns({"term_key", "term_id"}, request);
    request->set_limit(kPageRows);
    request->set_return_paging_state(true);
    if (previous) *request->mutable_paging_state() = previous->response().paging_state();
    ++scan_stats_.page_read_operations;
    RETURN_NOT_OK(session->TEST_ApplyAndFlush(op));
    if (op->response().status() != QLResponsePB::YQL_STATUS_OK) {
      return STATUS(RuntimeError,
          "Scan RDF dictionary: " + std::string(op->response().error_message()));
    }
    scan_stats_.response_bytes += op->rows_data().size();
    auto rows = VERIFY_RESULT(op->MakeRowBlock());
    if (rows.row_count() > kPageRows)
      return STATUS(Corruption, "RDF dictionary page exceeds its row limit");
    for (const auto& row : rows.rows()) {
      ++scan_stats_.rows;
      if (row.column(0).IsNull() || row.column(1).IsNull())
        return STATUS(Corruption, "Incomplete persistent RDF dictionary row");
      const auto key = row.column(0).string_value();
      const auto id = row.column(1).int64_value();
      if (key == kCounterKey) continue;
      if (key.compare(0, 5, "term:") != 0 || id <= 0 ||
          !restored.Restore(static_cast<uint64_t>(id), std::string_view(key).substr(5))) {
        return STATUS(Corruption, "Conflicting or invalid persistent RDF dictionary");
      }
    }
    // Empty pages can advance to the next tablet and are not necessarily EOF.
    if (!op->response().has_paging_state()) break;
    if (previous) {
      const auto& before = previous->response().paging_state();
      const auto& after = op->response().paging_state();
      if (before.next_partition_key() == after.next_partition_key() &&
          before.next_row_key() == after.next_row_key()) {
        return STATUS(Corruption, "RDF dictionary scan continuation did not advance");
      }
    }
    previous = std::move(op);
  }
  // Read after scanning: concurrent claims can advance the counter, never
  // decrease it. A missing/rewound counter must not allocate a reused ID.
  auto counter = VERIFY_RESULT(Read(kCounterKey));
  if (!counter || static_cast<uint64_t>(*counter) < restored.MaxId())
    return STATUS(Corruption, "RDF ID counter precedes persisted claims");
  *destination = std::move(restored);
  return Status::OK();
}

Status PersistentIdMap::Restore(IdMap* destination) {
  if (!initialized_) return STATUS(IllegalState, "Persistent RDF dictionary is not initialized");
  if (!destination) return STATUS(InvalidArgument, "Missing RDF dictionary destination");
  IdMap restored;
  RETURN_NOT_OK(ScanClaims(&restored));
  for (const auto& [id, term] : destination->Entries()) {
    if (!restored.Contains(term) || restored.Get(term) != id)
      return STATUS(Corruption, "Persistent RDF dictionary conflicts with local mapping");
  }
  *destination = std::move(restored);
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

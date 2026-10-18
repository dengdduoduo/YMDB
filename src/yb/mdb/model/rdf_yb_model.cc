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

#include "yb/mdb/model/rdf_yb_model.h"

#include <future>
#include <vector>

#include "yb/client/client.h"
#include "yb/client/schema.h"
#include "yb/client/session.h"
#include "yb/client/table_handle.h"
#include "yb/client/transaction.h"
#include "yb/client/transaction_manager.h"
#include "yb/client/yb_op.h"
#include "yb/client/yb_table_name.h"
#include "yb/common/common.pb.h"
#include "yb/common/ql_protocol_util.h"
#include "yb/common/transaction.h"
#include "yb/gutil/macros.h"
#include "yb/server/clock.h"
#include "yb/util/status.h"
#include "yb/util/flags.h"
#include "yb/util/status_format.h"

DEFINE_NON_RUNTIME_bool(mdb_dictionary_v2, false,
    "Use the versioned shared RDF dictionary in a fresh namespace");

using yb::client::YBTableName;

namespace yb::mdb {

RdfYBModel& RdfYBModel::Instance() {
  static RdfYBModel instance;
  return instance;
}

RdfYBModel::~RdfYBModel() = default;

Status RdfYBModel::Init(yb::client::YBClient* client, const std::string& namespace_name) {
  if (initialized_) {
    return Status::OK();
  }

  if (!client) {
    return STATUS(InvalidArgument, "YBClient is null");
  }

  client_ = client;

  for (int i = 0; i < 6; i++) {
    YBTableName table_name(YQL_DATABASE_CQL, namespace_name, kTableNames[i]);
    Status s = tables_[i].Open(table_name, client);
    if (!s.ok()) {
      initialized_ = true;
      return Status::OK();
    }
  }

  client::TableHandle dictionary_table;
  const auto dictionary_status = dictionary_table.Open(
      YBTableName(YQL_DATABASE_CQL, namespace_name, PersistentIdMap::kTableName), client);
  if (!dictionary_status.ok() && !dictionary_status.IsNotFound()) return dictionary_status;
  if (!FLAGS_mdb_dictionary_v2 && dictionary_status.ok()) {
    return STATUS(NotSupported, "Namespace requires mdb_dictionary_v2; stop legacy writers");
  }
  if (FLAGS_mdb_dictionary_v2) {
    if (dictionary_status.IsNotFound()) {
      // Never reinterpret existing graph IDs or migrate an ambiguous legacy dictionary.
      std::vector<std::string> existing_names{"rdf_idmap"};
      for (const auto* name : kTableNames) existing_names.emplace_back(name);
      for (const auto& name : existing_names) {
        client::TableHandle existing;
        const auto opened = existing.Open(
            YBTableName(YQL_DATABASE_CQL, namespace_name, name), client);
        if (opened.IsNotFound()) continue;
        RETURN_NOT_OK(opened);
        Status scan_status;
        client::TableIteratorOptions options;
        options.columns = {std::string(name) == "rdf_idmap" ? "gmap_id" : "c0"};
        options.error_handler = [&scan_status](const Status& s) { scan_status = s; };
        for (const auto& row : client::TableRange(existing, options)) {
          (void)row;
          return STATUS(NotSupported,
              "Dictionary v2 requires a fresh namespace; no auto-migration");
        }
        RETURN_NOT_OK(scan_status);
      }
    }
    dictionary_ = std::make_unique<PersistentIdMap>();
    RETURN_NOT_OK(dictionary_->Init(client, namespace_name));
  }

  write_session_ = client->NewSession(MonoDelta::FromSeconds(15));

  txn_manager_ = std::make_unique<client::TransactionManager>(
      client, scoped_refptr<server::Clock>(client->Clock()), client::LocalTabletFilter());

  connected_ = true;
  initialized_ = true;
  return Status::OK();
}

Result<uint64_t> RdfYBModel::ResolveTerm(IdMap& cache, const RdfTerm& term) {
  if (!dictionary_) {
    if (!term.language.empty() || (!term.datatype.empty() &&
        term.datatype != "http://www.w3.org/2001/XMLSchema#string")) {
      return STATUS(NotSupported, "Literal metadata requires dictionary v2 in a fresh namespace");
    }
    if (connected_ && cache.Size() == 0) {
      // An empty process-local map cannot assign IDs beside an existing graph.
      Status read_status;
      client::TableIteratorOptions options;
      options.read_time = read_time_;
      options.columns = {"c0"};
      options.error_handler = [&read_status](const Status& s) { read_status = s; };
      for (const auto& row : client::TableRange(tables_[0], options)) {
        (void)row;
        return STATUS(NotSupported,
            "Existing RDF data needs a restored dictionary or dictionary v2");
      }
      RETURN_NOT_OK(read_status);
    }
    return cache.GetOrCreate(term);
  }
  const auto id = VERIFY_RESULT(dictionary_->GetOrCreate(term));
  if (!cache.Restore(id, term)) {
    return STATUS(Corruption, "RDF term cache conflicts with dictionary");
  }
  return id;
}

const yb::client::TableHandle* RdfYBModel::GetTable(int perm_index) const {
  if (perm_index < 0 || perm_index > 5) {
    return nullptr;
  }
  return &tables_[perm_index];
}

DocDBRangeCache& RdfYBModel::GetCache(int perm_index) {
  return caches_[perm_index];
}

void RdfYBModel::InsertTriple(uint64_t s, uint64_t p, uint64_t o) {
  InvalidateStatistics();
  uint64_t c0, c1, c2;
  for (int perm = 0; perm < 6; perm++) {
    DocKeyEncoder::Permute(perm, s, p, o, &c0, &c1, &c2);
    caches_[perm].InsertRow(c0, c1, c2);
  }
}

Status RdfYBModel::WriteTripleToDocDB(uint64_t s, uint64_t p, uint64_t o) {
  if (!connected_) {
    return STATUS(IllegalState, "RdfYBModel not connected to DocDB");
  }

  InvalidateStatistics();
  uint64_t c0, c1, c2;
  for (int perm = 0; perm < 6; perm++) {
    DocKeyEncoder::Permute(perm, s, p, o, &c0, &c1, &c2);

    auto op = tables_[perm].NewInsertOp(write_session_->arena());
    auto* req = op->mutable_request();
    QLAddInt64HashValue(req, static_cast<int64_t>(c0));
    QLAddInt64RangeValue(req, static_cast<int64_t>(c1));
    QLAddInt64RangeValue(req, static_cast<int64_t>(c2));
    write_session_->Apply(op);
  }

  if (!HasTransaction()) {
    return Status::OK();
  }

  return Status::OK();
}

Status RdfYBModel::Flush() {
  InvalidateStatistics();
  if (!connected_) {
    return STATUS(IllegalState, "RdfYBModel not connected to DocDB");
  }
  return write_session_->TEST_Flush();
}

Status RdfYBModel::RefreshStatistics() {
  InvalidateStatistics();
  if (!connected_) {
    return STATUS(IllegalState, "RdfYBModel not connected to DocDB");
  }
  if (HasTransaction()) {
    return STATUS(IllegalState, "Cannot collect RDF statistics during a write transaction");
  }

  Statistics next;
  Status read_status = Status::OK();
  client::TableIteratorOptions options;
  options.read_time = read_time_;
  options.columns = {"c0", "c1", "c2"};
  options.error_handler = [&read_status](const Status& status) {
    if (read_status.ok()) read_status = status;
  };
  for (const auto& row : client::TableRange(tables_[0], options)) {
    const uint64_t terms[] = {
        static_cast<uint64_t>(row.column(0).int64_value()),
        static_cast<uint64_t>(row.column(1).int64_value()),
        static_cast<uint64_t>(row.column(2).int64_value())};
    ++next.rows;
    for (size_t i = 0; i < 3; ++i) ++next.term_counts[i][terms[i]];
    ++next.predicate_object_counts[terms[1]][terms[2]];
    ++next.predicate_subject_counts[terms[1]][terms[0]];
    next.equal_rows[0] += terms[0] == terms[1];
    next.equal_rows[1] += terms[0] == terms[2];
    next.equal_rows[2] += terms[1] == terms[2];
    next.equal_rows[3] += terms[0] == terms[1] && terms[1] == terms[2];
  }
  RETURN_NOT_OK(read_status);
  statistics_ = std::move(next);
  size_t pair_entries = 0;
  for (const auto& [predicate, objects] : statistics_->predicate_object_counts) {
    pair_entries += objects.size();
  }
  size_t subject_pair_entries = 0;
  for (const auto& [predicate, subjects] : statistics_->predicate_subject_counts) {
    subject_pair_entries += subjects.size();
  }
  LOG(INFO) << "MDB statistics collected: rows=" << statistics_->rows
            << " predicate_object_pairs=" << pair_entries
            << " predicate_subject_pairs=" << subject_pair_entries;
  return Status::OK();
}

Status RdfYBModel::LoadFromDocDB(int perm_index) {
  if (!connected_) {
    return STATUS(IllegalState, "RdfYBModel not connected to DocDB");
  }

  loaded_[perm_index] = false;
  caches_[perm_index].Clear();

  Status read_status = Status::OK();
  client::TableIteratorOptions options;
  options.read_time = read_time_;
  options.error_handler = [&read_status](const Status& status) {
    if (read_status.ok()) {
      read_status = status;
    }
  };
  options.columns = {"c0", "c1", "c2"};
  size_t rows_loaded = 0;
  for (const auto& row : client::TableRange(tables_[perm_index], options)) {
    int64_t c0 = row.column(0).int64_value();
    int64_t c1 = row.column(1).int64_value();
    int64_t c2 = row.column(2).int64_value();
    caches_[perm_index].InsertRow(
        static_cast<uint64_t>(c0),
        static_cast<uint64_t>(c1),
        static_cast<uint64_t>(c2));
    ++rows_loaded;
  }
  if (!read_status.ok()) {
    caches_[perm_index].Clear();
    return read_status;
  }
  LOG(INFO) << "MDB_PROFILE load_all perm=" << perm_index
            << " rows=" << rows_loaded;

  return Status::OK();
}

Status RdfYBModel::LoadAllFromDocDB() {
  if (!connected_) {
    return STATUS(IllegalState, "RdfYBModel not connected to DocDB");
  }

  for (int i = 0; i < 6; i++) {
    RETURN_NOT_OK(LoadFromDocDB(i));
  }
  return Status::OK();
}

Status RdfYBModel::LoadPartition(int perm_index, uint64_t c0_val) {
  if (!connected_) {
    return STATUS(IllegalState, "RdfYBModel not connected to DocDB");
  }

  auto& cache = partition_caches_[perm_index][c0_val];
  if (cache.Size() > 0) {
    return Status::OK();
  }

  auto session = read_session_ ? read_session_
                               : client_->NewSession(MonoDelta::FromSeconds(30));
  auto op = tables_[perm_index].NewReadOp(session->arena());
  if (read_time_) op->SetReadTime(read_time_);
  auto* req = op->mutable_request();
  QLAddInt64HashValue(req, static_cast<int64_t>(c0_val));
  tables_[perm_index].AddColumns({"c0", "c1", "c2"}, req);

  RETURN_NOT_OK(session->TEST_ApplyAndFlush(op));
  if (op->response().status() != QLResponsePB::YQL_STATUS_OK) {
    return STATUS_FORMAT(RuntimeError, "MDB partition read failed: $0", op->response());
  }
  auto rowblock = VERIFY_RESULT(op->MakeRowBlock());
  for (size_t i = 0; i < rowblock.row_count(); i++) {
    const auto& row = rowblock.rows()[i];
    int64_t c0 = row.column(0).int64_value();
    int64_t c1 = row.column(1).int64_value();
    int64_t c2 = row.column(2).int64_value();
    cache.InsertRow(
        static_cast<uint64_t>(c0),
        static_cast<uint64_t>(c1),
        static_cast<uint64_t>(c2));
  }

  LOG(INFO) << "Loaded partition perm=" << perm_index << " c0=" << c0_val
            << " rows=" << cache.Size();
  return Status::OK();
}

Status RdfYBModel::LoadTablet(
    int perm_index,
    const std::string& tablet_id) {
  if (!connected_) {
    return STATUS(
        IllegalState,
        "RdfYBModel not connected to DocDB");
  }

  auto& cache = tablet_caches_[perm_index][tablet_id];
  cache.Clear();

  Status read_status = Status::OK();
  client::TableIteratorOptions options;
  options.read_time = read_time_;
  options.error_handler = [&read_status](const Status& status) {
    if (read_status.ok()) {
      read_status = status;
    }
  };
  options.columns = {"c0", "c1", "c2"};
  options.tablet = tablet_id;

  for (const auto& row :
       client::TableRange(tables_[perm_index], options)) {
    int64_t c0 = row.column(0).int64_value();
    int64_t c1 = row.column(1).int64_value();
    int64_t c2 = row.column(2).int64_value();

    cache.InsertRow(
        static_cast<uint64_t>(c0),
        static_cast<uint64_t>(c1),
        static_cast<uint64_t>(c2));
  }

  if (!read_status.ok()) {
    cache.Clear();
    return read_status;
  }

  LOG(INFO) << "Loaded tablet perm=" << perm_index
            << " tablet=" << tablet_id
            << " rows=" << cache.Size();

  return Status::OK();
}

DocDBRangeCache& RdfYBModel::GetTabletCache(
    int perm_index,
    const std::string& tablet_id) {
  return tablet_caches_[perm_index][tablet_id];
}

DocDBRangeCache& RdfYBModel::GetPartitionCache(int perm_index, uint64_t c0_val) {
  return partition_caches_[perm_index][c0_val];
}

Status RdfYBModel::BeginTransaction() {
  if (read_session_) return STATUS(IllegalState, "Read snapshot is active");
  if (!txn_manager_) {
    return STATUS(IllegalState, "TransactionManager not initialized");
  }

  if (current_txn_) {
    (void)AbortTransaction();
  }

  current_txn_ = std::make_shared<client::YBTransaction>(
      txn_manager_.get(), TransactionFullLocality::Global());
  RETURN_NOT_OK(current_txn_->Init(IsolationLevel::SNAPSHOT_ISOLATION));
  write_session_->SetTransaction(current_txn_);

  LOG(INFO) << "Begin transaction id=" << current_txn_->id();
  return Status::OK();
}

Status RdfYBModel::CommitTransaction() {
  InvalidateStatistics();
  if (!current_txn_) {
    return STATUS(IllegalState, "No active transaction");
  }

  RETURN_NOT_OK(write_session_->TEST_Flush());

  auto commit_future = std::make_shared<std::promise<Status>>();
  current_txn_->Commit(CoarseMonoClock::Now() + MonoDelta::FromSeconds(30),
      [commit_future](const Status& s) {
        commit_future->set_value(s);
      });
  auto status = commit_future->get_future().get();

  write_session_->SetTransaction(nullptr);
  LOG(INFO) << "Commit transaction id=" << current_txn_->id()
            << " status=" << status.ToString();
  current_txn_.reset();
  return status;
}

Status RdfYBModel::AbortTransaction() {
  if (!current_txn_) {
    return Status::OK();
  }

  InvalidateStatistics();
  current_txn_->Abort();
  write_session_->SetTransaction(nullptr);
  LOG(INFO) << "Abort transaction id=" << current_txn_->id();
  current_txn_.reset();
  return Status::OK();
}

Status RdfYBModel::BeginReadSnapshot() {
  if (!connected_) {
    return STATUS(IllegalState, "RdfYBModel not connected to DocDB");
  }

  if (read_session_ || HasTransaction()) {
    return STATUS(IllegalState, "Cannot nest a read snapshot or read inside a write transaction");
  }
  ClearQueryCaches();
  // Explicit timestamps must also reach TableRange and sessions created by other scans.
  read_time_ = ReadHybridTime::SingleTime(client_->Clock()->Now());
  read_session_ = client_->NewSession(MonoDelta::FromSeconds(30));

  LOG(INFO) << "Begin read snapshot: " << read_time_;
  return Status::OK();
}

void RdfYBModel::EndReadSnapshot() {
  if (read_session_) {
    LOG(INFO) << "End read snapshot";
    read_session_.reset();
  }
  read_time_ = ReadHybridTime();
}

RdfYBModel::CacheMemory RdfYBModel::QueryCacheMemory() const {
  CacheMemory result;
  const auto add = [&result](const DocDBRangeCache& cache) {
    result.rows += cache.Size();
    result.capacity_bytes += cache.AllocatedRowBytes();
  };
  for (size_t i = 0; i < 6; ++i) {
    add(caches_[i]);
    for (const auto& entry : partition_caches_[i]) add(entry.second);
    for (const auto& entry : tablet_caches_[i]) add(entry.second);
  }
  return result;
}

void RdfYBModel::ClearQueryCaches() {
  for (int i = 0; i < 6; ++i) {
    caches_[i].Clear();
    partition_caches_[i].clear();
    tablet_caches_[i].clear();
    loaded_[i] = false;
  }
}

void RdfYBModel::ClearAll() {
  InvalidateStatistics();
  if (current_txn_) {
    (void)AbortTransaction();
  }
  EndReadSnapshot();
  ClearQueryCaches();
  for (int i = 0; i < 6; i++) {
    tables_[i] = client::TableHandle();
  }
  write_session_.reset();
  txn_manager_.reset();
  dictionary_.reset();
  client_ = nullptr;
  initialized_ = false;
  connected_ = false;
}

}  // namespace yb::mdb

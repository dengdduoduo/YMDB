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
#include "yb/mdb/import/import.h"

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
#include "yb/mdb/storage/yb_client_compat.h"
#include "yb/mdb/storage/docdb_read_context.h"
#include "yb/server/clock.h"
#include "yb/util/status.h"

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
      return s;
    }
  }

  RETURN_NOT_OK(term_dictionary_.Init(client, namespace_name));
  write_session_ = client->NewSession(MonoDelta::FromSeconds(15));

  txn_manager_ = std::make_unique<client::TransactionManager>(
      client, scoped_refptr<server::Clock>(client->Clock()), client::LocalTabletFilter());

  connected_ = true;
  initialized_ = true;
  return Status::OK();
}

const yb::client::TableHandle* RdfYBModel::GetTable(int perm_index) const {
  if (perm_index < 0 || perm_index > 5) {
    return nullptr;
  }
  return &tables_[perm_index];
}

Result<uint64_t> RdfYBModel::ResolveTerm(IdMap& local_map, std::string_view term) {
  if (!connected_) return STATUS(IllegalState, "RdfYBModel not connected to DocDB");
  if (local_map.Contains(term)) return local_map.Get(term);
  auto id = VERIFY_RESULT(term_dictionary_.GetOrCreate(term));
  if (!local_map.Restore(id, term)) {
    return STATUS(Corruption, "Persistent RDF dictionary conflicts with local mapping");
  }
  return id;
}

DocDBRangeCache& RdfYBModel::GetCache(int perm_index) {
  return caches_[perm_index];
}

void RdfYBModel::InsertTriple(uint64_t s, uint64_t p, uint64_t o) {
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

  uint64_t c0, c1, c2;
  for (int perm = 0; perm < 6; perm++) {
    DocKeyEncoder::Permute(perm, s, p, o, &c0, &c1, &c2);

    auto op = NewMdbInsertOp(tables_[perm], write_session_);
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
  if (!connected_) {
    return STATUS(IllegalState, "RdfYBModel not connected to DocDB");
  }
  return write_session_->TEST_Flush();
}

Status RdfYBModel::LoadFromDocDB(int perm_index) {
  if (!connected_) {
    return STATUS(IllegalState, "RdfYBModel not connected to DocDB");
  }

  caches_[perm_index].Clear();

  client::TableIteratorOptions options;
  options.columns = std::vector<std::string>{"c0", "c1", "c2"};
  for (const auto& row : client::TableRange(tables_[perm_index], options)) {
    int64_t c0 = row.column(0).int64_value();
    int64_t c1 = row.column(1).int64_value();
    int64_t c2 = row.column(2).int64_value();
    caches_[perm_index].InsertRow(
        static_cast<uint64_t>(c0),
        static_cast<uint64_t>(c1),
        static_cast<uint64_t>(c2));
  }

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
  auto op = NewMdbReadOp(tables_[perm_index], session);
  auto* req = op->mutable_request();
  QLAddInt64HashValue(req, static_cast<int64_t>(c0_val));
  tables_[perm_index].AddColumns({"c0", "c1", "c2"}, req);

  RETURN_NOT_OK(session->TEST_ApplyAndFlush(op));
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

DocDBRangeCache& RdfYBModel::GetPartitionCache(int perm_index, uint64_t c0_val) {
  return partition_caches_[perm_index][c0_val];
}

Status RdfYBModel::BeginTransaction() {
  if (!txn_manager_) {
    return STATUS(IllegalState, "TransactionManager not initialized");
  }

  if (current_txn_) {
    (void)AbortTransaction();
  }

  // Use the constructor's default locality instead of naming a version-specific type.
  current_txn_ = std::make_shared<client::YBTransaction>(txn_manager_.get());
  RETURN_NOT_OK(current_txn_->Init(IsolationLevel::SNAPSHOT_ISOLATION));
  write_session_->SetTransaction(current_txn_);

  LOG(INFO) << "Begin transaction id=" << current_txn_->id();
  return Status::OK();
}

Status RdfYBModel::CommitTransaction() {
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

  read_session_ = client_->NewSession(MonoDelta::FromSeconds(30));

  LOG(INFO) << "Begin read snapshot";
  return Status::OK();
}

Result<std::unique_ptr<DocDBReadContext>> RdfYBModel::NewQueryReadContext() {
  if (!connected_) return STATUS(IllegalState, "MDB model is not connected");
  if (!use_transactions_) return STATUS(NotSupported, "Streaming reads require transactions");
  auto context = VERIFY_RESULT(DocDBReadContext::Create(client_, txn_manager_.get()));
  context->page_rows = streaming_page_rows_;
  context->batch_options = batch_join_options_;
  return context;
}

void RdfYBModel::EndReadSnapshot() {
  if (read_session_) {
    LOG(INFO) << "End read snapshot";
    read_session_.reset();
  }
}

void RdfYBModel::ClearAll() {
  if (current_txn_) {
    (void)AbortTransaction();
  }
  read_session_.reset();
  for (int i = 0; i < 6; i++) {
    caches_[i].Clear();
    partition_caches_[i].clear();
    loaded_[i] = false;
    tables_[i] = client::TableHandle();
  }
  write_session_.reset();
  // Abort callbacks reference the manager. Drain them while its Impl still exists.
  if (txn_manager_) txn_manager_->Shutdown();
  txn_manager_.reset();
  client_ = nullptr;
  term_dictionary_ = PersistentIdMap();
  initialized_ = false;
  connected_ = false;
}

}  // namespace yb::mdb

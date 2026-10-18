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

#pragma once

#include <array>
#include <memory>
#include <string>
#include <unordered_map>

#include "yb/client/client.h"
#include "yb/client/session.h"
#include "yb/client/table_handle.h"
#include "yb/client/yb_op.h"

#include "yb/mdb/storage/docdb_key_encoder.h"
#include "yb/mdb/storage/docdb_range_cache.h"
#include "yb/mdb/import/persistent_id_map.h"
#include "yb/common/read_hybrid_time.h"

namespace yb::client {
class TransactionManager;
class YBTransaction;
}  // namespace yb::client

namespace yb::mdb {

class IdMap;
class DocDBReadContext;

class RdfYBModel {
 public:
  static RdfYBModel& Instance();

  Status Init(yb::client::YBClient* client, const std::string& namespace_name = "yugabyte");

  ~RdfYBModel();

  bool IsInitialized() const { return initialized_; }

  bool IsConnected() const { return connected_; }

  Result<uint64_t> ResolveTerm(IdMap& local_map, std::string_view term);

  const yb::client::TableHandle* GetTable(int perm_index) const;

  DocDBRangeCache& GetCache(int perm_index);

  void InsertTriple(uint64_t s, uint64_t p, uint64_t o);

  Status WriteTripleToDocDB(uint64_t s, uint64_t p, uint64_t o);

  Status Flush();

  Status LoadFromDocDB(int perm_index);

  Status LoadAllFromDocDB();

  Status LoadPartition(int perm_index, uint64_t c0_val);

  DocDBRangeCache& GetPartitionCache(int perm_index, uint64_t c0_val);

  bool IsLoaded(int perm_index) const { return loaded_[perm_index]; }

  void SetLoaded(int perm_index) { loaded_[perm_index] = true; }

  void ClearAll();

  void SetUsePartitionLoad(bool val) { use_partition_load_ = val; }
  bool UsePartitionLoad() const { return use_partition_load_; }

  void SetUseTransactions(bool val) { use_transactions_ = val; }
  bool UseTransactions() const { return use_transactions_; }

  // Opt-in until streaming correctness and supported query shapes are validated.
  void SetUseStreamingReads(bool value) { use_streaming_reads_ = value; }
  bool UseStreamingReads() const { return use_streaming_reads_; }
  void SetStreamingPageRows(size_t value) { streaming_page_rows_ = value; }
  Result<std::unique_ptr<DocDBReadContext>> NewQueryReadContext();

  Status BeginTransaction();
  Status CommitTransaction();
  Status AbortTransaction();
  bool HasTransaction() const { return current_txn_ != nullptr; }

  Status BeginReadSnapshot();
  void EndReadSnapshot();

  static constexpr const char* kTableNames[6] = {
      "rdf_spo", "rdf_pos", "rdf_osp",
      "rdf_pso", "rdf_sop", "rdf_ops"
  };

 private:
  RdfYBModel() = default;

  bool initialized_ = false;
  bool connected_ = false;
  bool use_partition_load_ = true;
  bool use_transactions_ = true;
  bool use_streaming_reads_ = false;
  size_t streaming_page_rows_ = 256;
  yb::client::YBClient* client_ = nullptr;
  yb::client::TableHandle tables_[6];
  yb::client::YBSessionPtr write_session_;
  PersistentIdMap term_dictionary_;
  std::unique_ptr<client::TransactionManager> txn_manager_;
  client::YBTransactionPtr current_txn_;
  std::shared_ptr<client::YBSession> read_session_;
  DocDBRangeCache caches_[6];
  bool loaded_[6] = {false, false, false, false, false, false};
  std::unordered_map<uint64_t, DocDBRangeCache> partition_caches_[6];
};

}  // namespace yb::mdb

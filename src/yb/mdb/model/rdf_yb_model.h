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
#include <optional>
#include <string>
#include <unordered_map>

#include "yb/client/client.h"
#include "yb/client/session.h"
#include "yb/client/table_handle.h"
#include "yb/client/yb_op.h"

#include "yb/mdb/import/persistent_id_map.h"
#include "yb/mdb/storage/docdb_key_encoder.h"
#include "yb/mdb/storage/docdb_range_cache.h"
#include "yb/common/read_hybrid_time.h"

namespace yb::client {
class TransactionManager;
class YBTransaction;
}  // namespace yb::client

namespace yb::mdb {

class RdfYBModel {
 public:
  static RdfYBModel& Instance();

  Status Init(yb::client::YBClient* client, const std::string& namespace_name = "yugabyte");

  ~RdfYBModel();

  bool IsInitialized() const { return initialized_; }

  bool IsConnected() const { return connected_; }

  const yb::client::TableHandle* GetTable(int perm_index) const;

  DocDBRangeCache& GetCache(int perm_index);

  void InsertTriple(uint64_t s, uint64_t p, uint64_t o);

  Status WriteTripleToDocDB(uint64_t s, uint64_t p, uint64_t o);

  Status Flush();

  Result<uint64_t> ResolveTerm(IdMap& cache, const RdfTerm& term);
  PersistentIdMap* Dictionary() { return dictionary_.get(); }

  struct Statistics {
    uint64_t rows = 0;
    std::array<std::unordered_map<uint64_t, uint64_t>, 3> term_counts;
    std::unordered_map<uint64_t, std::unordered_map<uint64_t, uint64_t>> predicate_object_counts;
    std::unordered_map<uint64_t, std::unordered_map<uint64_t, uint64_t>> predicate_subject_counts;
    // SP, SO, PO, and SPO equality counts.
    std::array<uint64_t, 4> equal_rows{};
  };

  Status RefreshStatistics();
  const Statistics* GetStatistics() const { return statistics_ ? &*statistics_ : nullptr; }
  uint64_t StatisticsVersion() const { return statistics_version_; }

  Status LoadFromDocDB(int perm_index);

  Status LoadAllFromDocDB();

  Status LoadPartition(int perm_index, uint64_t c0_val);

  DocDBRangeCache& GetPartitionCache(int perm_index, uint64_t c0_val);

  Status LoadTablet(int perm_index, const std::string& tablet_id);

  DocDBRangeCache& GetTabletCache(
    int perm_index, const std::string& tablet_id);

  bool IsLoaded(int perm_index) const { return loaded_[perm_index]; }

  void SetLoaded(int perm_index) { loaded_[perm_index] = true; }

  bool HasQueryCaches(int perm_index) const {
    return loaded_[perm_index] || caches_[perm_index].Size() != 0
        || !partition_caches_[perm_index].empty() || !tablet_caches_[perm_index].empty();
  }

  struct CacheMemory {
    size_t rows = 0;
    size_t capacity_bytes = 0;
  };

  // Counts reserved tuple buffers, excluding map nodes, iterators and result bindings.
  CacheMemory QueryCacheMemory() const;

  // Call only between queries, after their iterators have been destroyed.
  void ClearQueryCaches();

  void ClearAll();

  void SetUsePartitionLoad(bool val) { use_partition_load_ = val; }
  bool UsePartitionLoad() const { return use_partition_load_; }

  void SetUseTransactions(bool val) { use_transactions_ = val; }
  bool UseTransactions() const { return use_transactions_; }

  Status BeginTransaction();
  Status CommitTransaction();
  Status AbortTransaction();
  bool HasTransaction() const { return current_txn_ != nullptr; }

  Status BeginReadSnapshot();
  void EndReadSnapshot();
  const ReadHybridTime& QueryReadTime() const { return read_time_; }

  static constexpr const char* kTableNames[6] = {
      "rdf_spo", "rdf_pos", "rdf_osp",
      "rdf_pso", "rdf_sop", "rdf_ops"
  };

 private:
  RdfYBModel() = default;

  std::unique_ptr<PersistentIdMap> dictionary_;

  void InvalidateStatistics() {
    statistics_.reset();
    ++statistics_version_;
  }

  std::optional<Statistics> statistics_;
  uint64_t statistics_version_ = 0;

  bool initialized_ = false;
  bool connected_ = false;
  bool use_partition_load_ = true;
  bool use_transactions_ = true;
  yb::client::YBClient* client_ = nullptr;
  yb::client::TableHandle tables_[6];
  yb::client::YBSessionPtr write_session_;
  std::unique_ptr<client::TransactionManager> txn_manager_;
  client::YBTransactionPtr current_txn_;
  std::shared_ptr<client::YBSession> read_session_;
  ReadHybridTime read_time_;
  DocDBRangeCache caches_[6];
  bool loaded_[6] = {false, false, false, false, false, false};
  std::unordered_map<uint64_t, DocDBRangeCache> partition_caches_[6];

  std::unordered_map<std::string, DocDBRangeCache> tablet_caches_[6];
};

}  // namespace yb::mdb

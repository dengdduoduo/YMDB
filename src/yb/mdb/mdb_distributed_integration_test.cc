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

#include <gtest/gtest.h>

#include <array>
#include <future>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "query/executor/binding.h"
#include "query/optimizer/plan/join/index_nested_loop_plan.h"
#include "query/optimizer/rdf_model/plan/triple_plan.h"
#include "query/optimizer/binding_iter_constructor.h"
#include "query/parser/sparql_query_parser.h"
#include "query/query_context.h"
#include "graph_models/rdf_model/conversions.h"
#include "yb/mdb/adapter/mdb_adapter.h"

#include "yb/client/schema.h"
#include "yb/client/meta_cache.h"
#include "yb/client/transaction.h"
#include "yb/client/transaction_manager.h"
#include "yb/common/transaction.h"
#include "yb/server/clock.h"
#include "yb/util/flags.h"
#include "yb/util/mem_tracker.h"
#include "yb/util/scope_exit.h"
#include "yb/common/ql_protocol_util.h"
#include "yb/mdb/storage/rdf_table_schema.h"
#include "yb/mdb/storage/docdb_scan_cursor.h"
#include "yb/mdb/storage/docdb_read_context.h"
#include "yb/mdb/storage/yb_client_compat.h"
#include "yb/client/table_handle.h"
#include "yb/client/yb_table_name.h"
#include "yb/common/common.pb.h"
#include "yb/integration-tests/mini_cluster.h"
#include "yb/integration-tests/yb_mini_cluster_test_base.h"
#include "yb/mdb/model/rdf_yb_model.h"
#include "yb/mdb/query/distributed/distributed_join.h"
#include "yb/mdb/query/distributed/distributed_plan.h"
#include "yb/mdb/query/distributed/gather_operator.h"
#include "yb/mdb/query/distributed/scatter_executor.h"
#include "yb/mdb/query/distributed/tablet_router.h"
#include "yb/util/status.h"
#include "yb/util/subprocess.h"
#include "yb/util/metrics.h"
#include "yb/tserver/mini_tablet_server.h"
#include "yb/mdb/import/rdf_term.h"
#include "yb/util/test_macros.h"

using namespace SPARQL;

DECLARE_int64(mem_tracker_tcmalloc_gc_release_bytes);
METRIC_DECLARE_histogram(handler_latency_yb_tserver_TabletServerService_Read);

namespace yb::mdb {

class MdbDistributedIntegrationTest : public MiniClusterTestWithClient<MiniCluster> {
 protected:
  static constexpr const char* kNamespace = "mdb_dist_test";
  static constexpr int kNumTablets = 4;

  void SetUp() override {
    YBMiniClusterTestBase<MiniCluster>::SetUp();
    // Initialize after YBTest has set its memory limit, and preserve the derived
    // GC threshold across its per-test FlagSaver. Restoring the -1 auto sentinel
    // with an already initialized root tracker causes GC on every release.
    static const int64_t gc_threshold = [] {
      (void)MemTracker::GetRootTracker();
      return FLAGS_mem_tracker_tcmalloc_gc_release_bytes;
    }();
    if (FLAGS_mem_tracker_tcmalloc_gc_release_bytes < 0) {
      ANNOTATE_UNPROTECTED_WRITE(FLAGS_mem_tracker_tcmalloc_gc_release_bytes) = gc_threshold;
    }
    MiniClusterOptions opts;
    opts.num_tablet_servers = 3;
    cluster_.reset(new MiniCluster(std::move(opts)));
    ASSERT_OK(cluster_->Start());
    ASSERT_OK(CreateClient());
  }

  void DoTearDown() override {
    RdfYBModel::Instance().ClearAll();
    MiniClusterTestWithClient<MiniCluster>::DoTearDown();
  }

  void CreateRdfTables() {
    ASSERT_OK(client_->CreateNamespaceIfNotExists(kNamespace, YQL_DATABASE_CQL));

    for (int i = 0; i < 6; i++) {
      client::YBTableName table_name(YQL_DATABASE_CQL, kNamespace,
                                     RdfYBModel::kTableNames[i]);
      ASSERT_OK(CreateRdfTable(table_name));
    }
  }

  Status CreateRdfTable(const client::YBTableName& table_name) {
    client::YBSchemaBuilder builder;
    ConfigureRdfTableSchema(&builder);

    client::TableHandle tmp;
    return tmp.Create(table_name, kNumTablets, client_.get(), &builder);
  }

  void InitModel() {
    ASSERT_OK(RdfYBModel::Instance().Init(client_.get(), kNamespace));
  }

  void InitRouter() {
    ASSERT_OK(router_.Init(client_.get(), kNamespace));
  }

  struct SingleNodeResult {
    int row_count = 0;
    std::set<uint64_t> subjects;
  };

  SingleNodeResult ExecuteSingleNode(const std::string& query) {
    SingleNodeResult result;
    try {
      QueryParser qp(query);
      auto op = qp.get_query_plan({});
      if (op == nullptr) return result;

      BindingIterConstructor optimizer;
      op->accept_visitor(optimizer);
      if (optimizer.tmp == nullptr) return result;

      auto& iter = optimizer.tmp;
      auto& qctx = get_query_ctx();
      Binding binding(qctx.get_var_size());
      iter->begin(binding);

      VarId s_var = get_query_ctx().get_or_create_var("s");

      while (iter->next()) {
        if (!binding[s_var].is_null()) {
          result.subjects.insert(binding[s_var].id);
        }
        result.row_count++;
      }
    } catch (const QueryParsingException&) {
    }
    return result;
  }

  TabletRouter router_;
};

TEST_F(MdbDistributedIntegrationTest, InitRouterWithRealTables) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  ASSERT_TRUE(RdfYBModel::Instance().IsConnected());

  ASSERT_NO_FATALS(InitRouter());
  ASSERT_TRUE(router_.IsInitialized());

  int spo_tablets = router_.GetNumTablets(0);
  ASSERT_EQ(spo_tablets, kNumTablets);

  const auto& tablets = router_.GetTablets(0);
  ASSERT_EQ(tablets.size(), static_cast<size_t>(kNumTablets));
  for (const auto& t : tablets) {
    ASSERT_FALSE(t.tablet_id.empty());
  }
}

TEST_F(MdbDistributedIntegrationTest, AllPermutationsPreserveSharedPrefixes) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  // Every pair of coordinates is shared by two triples. This detects a
  // missing third key column in every permutation, not only SPO.
  const std::vector<std::array<uint64_t, 3>> triples = {
      {1, 10, 100}, {1, 10, 101}, {1, 11, 100}, {2, 10, 100}};
  auto& model = RdfYBModel::Instance();
  for (const auto& triple : triples) {
    ASSERT_OK(model.WriteTripleToDocDB(triple[0], triple[1], triple[2]));
  }
  // Re-inserting an RDF triple is idempotent; projection duplicates below
  // are different and must be preserved by SPARQL bag semantics.
  ASSERT_OK(model.WriteTripleToDocDB(1, 10, 100));
  ASSERT_OK(model.Flush());

  for (int perm = 0; perm < 6; ++perm) {
    SCOPED_TRACE(perm);
    std::multiset<std::array<uint64_t, 3>> expected, actual;
    for (const auto& triple : triples) {
      std::array<uint64_t, 3> row;
      DocKeyEncoder::Permute(perm, triple[0], triple[1], triple[2],
                            &row[0], &row[1], &row[2]);
      expected.insert(row);
    }
    client::TableIteratorOptions options;
    options.columns = std::vector<std::string>{"c0", "c1", "c2"};
    Status read_status;
    options.error_handler = [&read_status](const Status& status) { read_status = status; };
    for (const auto& row : client::TableRange(*model.GetTable(perm), options)) {
      actual.insert({static_cast<uint64_t>(row.column(0).int64_value()),
                     static_cast<uint64_t>(row.column(1).int64_value()),
                     static_cast<uint64_t>(row.column(2).int64_value())});
    }
    ASSERT_OK(read_status);
    ASSERT_EQ(actual, expected);
  }
  ASSERT_OK(model.LoadAllFromDocDB());
  auto single = ExecuteSingleNode("SELECT ?s WHERE { ?s ?p ?o }");
  ASSERT_EQ(single.row_count, 4);
  ASSERT_EQ(single.subjects.size(), 2);
}

TEST_F(MdbDistributedIntegrationTest, PointReadsReachEveryTablet) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  ASSERT_NO_FATALS(InitRouter());
  auto& model = RdfYBModel::Instance();
  auto session = client_->NewSession(MonoDelta::FromSeconds(30));
  const auto& table = *model.GetTable(0);
  std::map<std::string, uint64_t> subjects;
  // Use the client's physical hash encoding, never integer ID ranges or an
  // assumption that consecutive subjects land in different tablets.
  for (uint64_t s = 1; s <= 4096 && subjects.size() < kNumTablets; ++s) {
    auto op = NewMdbReadOp(table, session);
    QLAddInt64HashValue(op->mutable_request(), static_cast<int64_t>(s));
    std::string key;
    ASSERT_OK(op->GetPartitionKey(&key));
    const auto* tablet = router_.LocateTablet(0, key);
    ASSERT_NE(tablet, nullptr);
    subjects.emplace(tablet->tablet_id, s);
  }
  ASSERT_EQ(subjects.size(), kNumTablets);
  for (const auto& entry : subjects) {
    ASSERT_OK(model.WriteTripleToDocDB(entry.second, 10, 100));
  }
  ASSERT_OK(model.Flush());
  for (const auto& entry : subjects) {
    auto op = NewMdbReadOp(table, session);
    QLAddInt64HashValue(op->mutable_request(), static_cast<int64_t>(entry.second));
    table.AddColumns({"c0", "c1", "c2"}, op->mutable_request());
    std::string key;
    ASSERT_OK(op->GetPartitionKey(&key));
    auto tablet = ASSERT_RESULT(client_->LookupTabletByKeyFuture(
        table.table(), key, CoarseMonoClock::Now() + MonoDelta::FromSeconds(30)).get());
    ASSERT_EQ(tablet->tablet_id(), entry.first);
    // Pin the read to the resolved tablet. Some client versions clear the
    // operation's tablet pointer after Flush, so retain our own reference.
    op->SetTablet(tablet);
    ASSERT_OK(session->TEST_ApplyAndFlush(op));
    ASSERT_EQ(op->response().status(), QLResponsePB::YQL_STATUS_OK);
    auto rows = ASSERT_RESULT(op->MakeRowBlock());
    ASSERT_EQ(rows.row_count(), 1);
    ASSERT_EQ(rows.rows()[0].column(0).int64_value(), entry.second);
    ASSERT_NE(tablet->LeaderTServer(), nullptr);
    LOG(INFO) << "Verified SPO subject=" << entry.second << " tablet=" << entry.first
              << " leader_uuid=" << tablet->LeaderTServer()->permanent_uuid();
  }
  for (const auto& tablet : router_.GetTablets(0)) {
    LOG(INFO) << "SPO tablet=" << tablet.tablet_id << " leader="
              << tablet.leader_host << ":" << tablet.leader_port;
  }
}

TEST_F(MdbDistributedIntegrationTest, RejectUnboundedTabletExecution) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  ASSERT_NO_FATALS(InitRouter());
  for (const auto* query : {"SELECT * WHERE { ?s ?p ?o }",
                            "SELECT DISTINCT * WHERE { ?s ?p ?o }"}) {
    QueryParser parser(query);
    auto op = parser.get_query_plan({});
    ASSERT_NE(op, nullptr);
    DistributedPlan plan;
    DistributedPlanGenerator generator(&router_);
    ASSERT_OK(generator.Generate(*op, &plan));
    ASSERT_EQ(plan.num_fragments(), kNumTablets);
    ScatterExecutor executor;
    std::vector<ScatterExecutor::FragmentResultSet> results;
    const auto status = executor.Execute(plan, &results);
    ASSERT_TRUE(status.IsNotSupported()) << status;
    ASSERT_TRUE(results.empty());
  }
}

TEST_F(MdbDistributedIntegrationTest, TransactionReadViewAcrossPagesAndIndexes) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  auto& model = RdfYBModel::Instance();
  ASSERT_OK(model.WriteTripleToDocDB(1, 10, 100));
  ASSERT_OK(model.WriteTripleToDocDB(1, 10, 101));
  ASSERT_OK(model.Flush());

  // Leave intents in all six indexes before the reader establishes its view.
  ASSERT_OK(model.BeginTransaction());
  auto abort_writer = ScopeExit([&] { (void)model.AbortTransaction(); });
  ASSERT_OK(model.WriteTripleToDocDB(1, 10, 102));
  ASSERT_OK(model.Flush());

  client::TransactionManager manager(
      client_.get(), scoped_refptr<server::Clock>(client_->Clock()), client::LocalTabletFilter());
  auto shutdown_reader = ScopeExit([&] { manager.Shutdown(); });
  auto txn = std::make_shared<client::YBTransaction>(&manager);
  // Pin a historical snapshot before returning any rows. The default uncertainty
  // interval can otherwise require a restart after the writer commits.
  ASSERT_OK(txn->Init(IsolationLevel::SNAPSHOT_ISOLATION,
                      ReadHybridTime::SingleTime(manager.Now())));
  auto abort_reader = ScopeExit([&] { txn->Abort(); });
  auto session = client_->NewSession(MonoDelta::FromSeconds(30));
  session->SetTransaction(txn);
  const auto& spo = *model.GetTable(0);
  auto op = NewMdbReadOp(spo, session);
  auto* req = op->mutable_request();
  QLAddInt64HashValue(req, 1);
  spo.AddColumns({"c0", "c1", "c2"}, req);
  req->set_limit(1);
  req->set_return_paging_state(true);
  ASSERT_OK(session->TEST_ApplyAndFlush(op));
  ASSERT_EQ(op->response().status(), QLResponsePB::YQL_STATUS_OK);
  auto first = ASSERT_RESULT(op->MakeRowBlock());
  ASSERT_EQ(first.row_count(), 1);
  ASSERT_TRUE(op->response().has_paging_state());
  ASSERT_FALSE(txn->IsRestartRequired());

  ASSERT_OK(model.CommitTransaction());
  std::multiset<int64_t> objects{first.rows()[0].column(2).int64_value()};
  int pages = 1;
  while (op->response().has_paging_state()) {
    ASSERT_LT(pages, 10) << "Pagination did not terminate";
    auto next = NewMdbReadOp(spo, session);
    auto* next_req = next->mutable_request();
    QLAddInt64HashValue(next_req, 1);
    spo.AddColumns({"c0", "c1", "c2"}, next_req);
    next_req->set_limit(1);
    next_req->set_return_paging_state(true);
    *next_req->mutable_paging_state() = op->response().paging_state();
    ASSERT_OK(session->TEST_ApplyAndFlush(next));
    ASSERT_EQ(next->response().status(), QLResponsePB::YQL_STATUS_OK)
        << next->response().error_message();
    ASSERT_FALSE(txn->IsRestartRequired());
    auto rows = ASSERT_RESULT(next->MakeRowBlock());
    for (const auto& row : rows.rows()) objects.insert(row.column(2).int64_value());
    op = std::move(next);
    ++pages;
  }
  EXPECT_EQ(objects, (std::multiset<int64_t>{100, 101}));

  const auto& pos = *model.GetTable(1);
  auto same_view = NewMdbReadOp(pos, session);
  QLAddInt64HashValue(same_view->mutable_request(), 10);
  pos.AddColumns({"c0", "c1", "c2"}, same_view->mutable_request());
  ASSERT_OK(session->TEST_ApplyAndFlush(same_view));
  ASSERT_EQ(same_view->response().status(), QLResponsePB::YQL_STATUS_OK);
  ASSERT_FALSE(txn->IsRestartRequired());
  auto old_rows = ASSERT_RESULT(same_view->MakeRowBlock());
  std::multiset<int64_t> old_objects;
  for (const auto& row : old_rows.rows()) old_objects.insert(row.column(1).int64_value());
  EXPECT_EQ(old_objects, (std::multiset<int64_t>{100, 101}));

  auto fresh_session = client_->NewSession(MonoDelta::FromSeconds(30));
  auto fresh = NewMdbReadOp(pos, fresh_session);
  QLAddInt64HashValue(fresh->mutable_request(), 10);
  pos.AddColumns({"c0", "c1", "c2"}, fresh->mutable_request());
  ASSERT_OK(fresh_session->TEST_ApplyAndFlush(fresh));
  ASSERT_EQ(fresh->response().status(), QLResponsePB::YQL_STATUS_OK);
  auto new_rows = ASSERT_RESULT(fresh->MakeRowBlock());
  std::multiset<int64_t> new_objects;
  for (const auto& row : new_rows.rows()) new_objects.insert(row.column(1).int64_value());
  EXPECT_EQ(new_objects, (std::multiset<int64_t>{100, 101, 102}));
}

TEST_F(MdbDistributedIntegrationTest, CursorPreservesEmptyPagesAndRuntimeReset) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  auto& model = RdfYBModel::Instance();
  for (uint64_t o = 100; o < 105; ++o) ASSERT_OK(model.WriteTripleToDocDB(1, 10, o));
  ASSERT_OK(model.Flush());
  auto session = client_->NewSession(MonoDelta::FromSeconds(30));
  DocDBScanCursor cursor;
  DocDBScanSpec spec;
  spec.hash_key = 1;
  spec.page_rows = 1;
  spec.columns[2].min = 102;
  ASSERT_OK(cursor.Open(model.GetTable(0), session, spec));
  auto empty = ASSERT_RESULT(cursor.NextBatch());
  ASSERT_TRUE(empty.rows.empty());
  ASSERT_FALSE(empty.eof);
  std::multiset<uint64_t> objects;
  bool eof = false;
  for (int page = 0; page < 10 && !eof; ++page) {
    auto batch = ASSERT_RESULT(cursor.NextBatch());
    ASSERT_LE(batch.rows.size(), 1);
    for (const auto& row : batch.rows) objects.insert(row[2]);
    eof = batch.eof;
  }
  ASSERT_TRUE(eof);
  EXPECT_EQ(objects, (std::multiset<uint64_t>{102, 103, 104}));
  spec.hash_key = 999;
  ASSERT_OK(cursor.Open(model.GetTable(0), session, spec));
  auto missing = ASSERT_RESULT(cursor.NextBatch());
  EXPECT_TRUE(missing.rows.empty());
  EXPECT_TRUE(missing.eof);
  spec.hash_key = 1;
  ASSERT_OK(cursor.Open(model.GetTable(0), session, spec));
  auto restarted = ASSERT_RESULT(cursor.NextBatch());
  EXPECT_TRUE(restarted.rows.empty());
  EXPECT_FALSE(restarted.eof);
}

TEST_F(MdbDistributedIntegrationTest, CursorPartitionRangesAreDisjointAndComplete) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  ASSERT_NO_FATALS(InitRouter());
  auto& model = RdfYBModel::Instance();
  std::multiset<DocDBScanBatch::Row> expected, actual;
  for (uint64_t s = 1; s <= 32; ++s) {
    for (uint64_t o = 100; o < 103; ++o) {
      ASSERT_OK(model.WriteTripleToDocDB(s, 10, o));
      expected.insert({s, 10, o});
    }
  }
  ASSERT_OK(model.Flush());
  const auto& table = *model.GetTable(0);
  auto session = client_->NewSession(MonoDelta::FromSeconds(30));
  for (const auto& tablet : router_.GetTablets(0)) {
    SCOPED_TRACE(tablet.tablet_id);
    DocDBScanSpec spec;
    spec.partition_start = tablet.partition_start;
    spec.partition_end = tablet.partition_end;
    spec.page_bytes = 2 * sizeof(DocDBScanBatch::Row);
    DocDBScanCursor cursor;
    ASSERT_OK(cursor.Open(&table, session, spec));
    size_t count = 0;
    bool eof = false;
    for (int page = 0; page < 100 && !eof; ++page) {
      auto batch = ASSERT_RESULT(cursor.NextBatch());
      ASSERT_LE(batch.rows.size(), 2);
      for (const auto& row : batch.rows) {
        auto key_op = NewMdbReadOp(table, session);
        QLAddInt64HashValue(key_op->mutable_request(), static_cast<int64_t>(row[0]));
        std::string key;
        ASSERT_OK(key_op->GetPartitionKey(&key));
        const auto* owner = router_.LocateTablet(0, key);
        ASSERT_NE(owner, nullptr);
        ASSERT_EQ(owner->tablet_id, tablet.tablet_id);
        actual.insert(row);
        ++count;
      }
      eof = batch.eof;
    }
    ASSERT_TRUE(eof);
    EXPECT_GT(count, 0);
  }
  EXPECT_EQ(actual, expected);  // No DISTINCT: duplicates would fail this comparison.
}

TEST_F(MdbDistributedIntegrationTest, CursorFailureCancellationAndDeadlineAreNotEof) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  auto& model = RdfYBModel::Instance();
  ASSERT_OK(model.WriteTripleToDocDB(1, 10, 100));
  ASSERT_OK(model.WriteTripleToDocDB(1, 10, 101));
  ASSERT_OK(model.Flush());
  auto session = client_->NewSession(MonoDelta::FromSeconds(30));
  std::atomic<bool> cancelled{false};
  DocDBScanSpec spec;
  spec.hash_key = 1;
  spec.page_rows = 1;
  spec.cancelled = &cancelled;
  DocDBScanCursor cursor;
  ASSERT_OK(cursor.Open(model.GetTable(0), session, spec));
  ASSERT_FALSE(ASSERT_RESULT(cursor.NextBatch()).eof);
  cancelled = true;
  auto stopped = cursor.NextBatch();
  ASSERT_FALSE(stopped.ok());
  EXPECT_TRUE(stopped.status().IsAborted());
  cancelled = false;
  ASSERT_FALSE(cursor.NextBatch().ok());  // Failure is sticky until explicitly reopened.
  cursor.Close();
  ASSERT_FALSE(cursor.NextBatch().ok());
  spec.deadline = CoarseMonoClock::Now() - MonoDelta::FromSeconds(1);
  ASSERT_OK(cursor.Open(model.GetTable(0), session, spec));
  auto expired = cursor.NextBatch();
  ASSERT_FALSE(expired.ok());
  EXPECT_TRUE(expired.status().IsTimedOut());
  spec.deadline = CoarseMonoClock::Now() + MonoDelta::FromSeconds(30);
  ASSERT_OK(cursor.Open(model.GetTable(0), session, spec));
  ASSERT_FALSE(ASSERT_RESULT(cursor.NextBatch()).eof);
  ASSERT_OK(client_->DeleteTable(model.GetTable(0)->name()));
  auto failed_read = cursor.NextBatch();
  ASSERT_FALSE(failed_read.ok());
  ASSERT_FALSE(cursor.NextBatch().ok());
}

TEST_F(MdbDistributedIntegrationTest, StreamingPublicEntryUsesQueryViewWithoutCache) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  auto& model = RdfYBModel::Instance();
  auto saved_uris = std::move(g_gmap_to_uri);
  auto saved_packed = std::move(g_gmap_to_pack);
  auto saved_graph = std::move(g_pack_to_gmap);
  g_gmap_to_uri.clear();
  g_gmap_to_pack.clear();
  g_pack_to_gmap.clear();
  auto restore = ScopeExit([&] {
    model.SetUseStreamingReads(false);
    model.SetStreamingPageRows(256);
    g_gmap_to_uri = std::move(saved_uris);
    g_gmap_to_pack = std::move(saved_packed);
    g_pack_to_gmap = std::move(saved_graph);
  });
  auto iri = [](uint64_t id) { return "http://stream.example/" + std::to_string(id); };
  for (auto id : {1, 2, 7, 9, 10, 20, 100, 101, 200, 300, 301, 302, 900, 999}) {
    auto packed = Conversions::pack_iri(iri(id));
    g_gmap_to_uri[id] = iri(id);
    g_gmap_to_pack[id] = packed;
    g_pack_to_gmap[packed.id] = id;
  }
  const std::vector<DocDBScanBatch::Row> triples = {
      {1, 10, 100}, {1, 10, 101}, {2, 10, 200}, {100, 20, 300},
      {101, 20, 301}, {200, 20, 302}, {7, 10, 7}};
  for (const auto& row : triples) ASSERT_OK(model.WriteTripleToDocDB(row[0], row[1], row[2]));
  ASSERT_OK(model.Flush());
  // A stale cache-only row must never appear, nor may the query overwrite the
  // cache by preloading the table as an implicit fallback.
  model.InsertTriple(999, 999, 999);
  model.SetUseStreamingReads(true);
  model.SetStreamingPageRows(1);
  auto query = [&](const char* text) {
    std::multiset<std::string> values;
    auto callback = [](const char*, const char* value, void* arg) {
      EXPECT_NE(DocDBReadContextScope::Current(), nullptr);
      static_cast<std::multiset<std::string>*>(arg)->insert(value);
    };
    EXPECT_EQ(YBCMdbExecuteSparql(text, callback, &values), 0);
    return values;
  };
  const std::multiset<std::string> subjects = {
      iri(1), iri(1), iri(2), iri(100), iri(101), iri(200), iri(7)};
  EXPECT_EQ(query("SELECT ?s WHERE { ?s ?p ?o }"), subjects);

  // Concurrent public requests currently serialize over the shared dictionary.
  // Each request must still get fresh variable bindings and its own read scope.
  std::promise<void> release_queries;
  auto released = release_queries.get_future().share();
  auto left = std::async(std::launch::async, [&] {
    released.wait();
    return query("SELECT ?same WHERE { ?same ?p ?o }");
  });
  auto right = std::async(std::launch::async, [&] {
    released.wait();
    return query("SELECT ?same WHERE { <http://stream.example/1> ?p ?same }");
  });
  release_queries.set_value();
  EXPECT_EQ(left.get(), subjects);
  EXPECT_EQ(right.get(), (std::multiset<std::string>{iri(100), iri(101)}));

  int cancelled_rows = 0;
  auto cancel_after_first = [](const char*, const char*, void* arg) {
    ++*static_cast<int*>(arg);
    auto* context = DocDBReadContextScope::Current();
    ASSERT_NE(context, nullptr);
    context->cancelled = true;
  };
  EXPECT_EQ(YBCMdbExecuteSparql(
      "SELECT ?s WHERE { ?s ?p ?o }", cancel_after_first, &cancelled_rows), 1);
  EXPECT_EQ(cancelled_rows, 1);
  EXPECT_EQ(DocDBReadContextScope::Current(), nullptr);
  EXPECT_EQ(query("SELECT ?s WHERE { ?s ?p ?o }"), subjects);

  EXPECT_EQ(query("SELECT ?a WHERE { ?a ?p ?b . ?b ?q ?c }"),
            (std::multiset<std::string>{iri(1), iri(1), iri(2), iri(7)}));
  EXPECT_EQ(query("SELECT ?x WHERE { ?x ?p ?x }"), (std::multiset<std::string>{iri(7)}));
  EXPECT_TRUE(query("SELECT ?s WHERE { ?s <http://stream.example/missing> ?o }").empty());
  EXPECT_EQ(query("SELECT ?s WHERE { ?s ?p ?o OPTIONAL { "
                  "?s <http://stream.example/missing> ?extra } }"), subjects);
  EXPECT_EQ(query("SELECT ?s WHERE { { ?s <http://stream.example/missing> ?o } "
                  "UNION { ?s ?p ?o } }"), subjects);
  EXPECT_EQ(query("SELECT DISTINCT ?s WHERE { ?s ?p ?o }").size(), 6);
  EXPECT_EQ(query("SELECT ?s WHERE { ?s ?p ?o } ORDER BY ?s LIMIT 3"),
            (std::multiset<std::string>{iri(1), iri(1), iri(100)}));

  struct DuringRead {
    RdfYBModel* model;
    std::multiset<std::string> values;
    bool committed = false;
  } during{&model, {}, false};
  auto commit_after_first_row = [](const char*, const char* value, void* arg) {
    auto& state = *static_cast<DuringRead*>(arg);
    state.values.insert(value);
    if (!state.committed) {
      ASSERT_OK(state.model->BeginTransaction());
      ASSERT_OK(state.model->WriteTripleToDocDB(9, 10, 900));
      ASSERT_OK(state.model->CommitTransaction());
      state.committed = true;
    }
  };
  ASSERT_EQ(YBCMdbExecuteSparql(
      "SELECT ?s WHERE { ?s ?p ?o }", commit_after_first_row, &during), 0);
  ASSERT_TRUE(during.committed);
  EXPECT_EQ(during.values, subjects);
  auto after_commit = subjects;
  after_commit.insert(iri(9));
  EXPECT_EQ(query("SELECT ?s WHERE { ?s ?p ?o }"), after_commit);
  for (int perm = 0; perm < 6; ++perm) {
    EXPECT_EQ(model.GetCache(perm).Size(), 1);
    EXPECT_FALSE(model.IsLoaded(perm));
  }
  ASSERT_OK(model.WriteTripleToDocDB(777, 20, 900));
  ASSERT_OK(model.Flush());
  int invalid_id_rows = 0;
  auto count_invalid = [](const char*, const char*, void* arg) {
    ++*static_cast<int*>(arg);
  };
  EXPECT_EQ(YBCMdbExecuteSparql(
      "SELECT ?s WHERE { ?s <http://stream.example/20> <http://stream.example/900> }",
      count_invalid, &invalid_id_rows), 1);
  EXPECT_EQ(invalid_id_rows, 0);
}

TEST_F(MdbDistributedIntegrationTest, EmptyPlanHandling) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  ASSERT_NO_FATALS(InitRouter());

  DistributedPlan plan;
  ScatterExecutor executor;
  std::vector<ScatterExecutor::FragmentResultSet> fragment_results;
  ASSERT_OK(executor.Execute(plan, &fragment_results));
  ASSERT_TRUE(fragment_results.empty());

  GatherOperator gather;
  GatherResult gather_result;
  ASSERT_OK(gather.Merge(fragment_results, GatherStrategy::UNION_ALL, &gather_result));
  ASSERT_EQ(gather_result.total_input_rows, 0);
  ASSERT_EQ(gather_result.total_output_rows, 0);
}

TEST_F(MdbDistributedIntegrationTest, TypedIdentitySurvivesFreshCliProcesses) {
  ASSERT_NO_FATALS(CreateRdfTables());
  // Every CLI invocation has fresh conversion registries and reloads rdf_idmap.
  std::string executable;
  ASSERT_OK(env_->GetExecutablePath(&executable));
  auto cli = executable.substr(0, executable.find_last_of('/')) + "/../bin/mdb_cli";
  auto run = [&](std::vector<std::string> args, std::string* output) {
    args.insert(args.begin(), {cli, "--mdb_masters=" + cluster_->GetMasterAddresses(),
        "--mdb_keyspace=" + std::string(kNamespace), "--mdb_streaming_reads=true",
        "--mdb_scan_page_rows=1"});
    std::string error;
    const auto status = Subprocess::Call(args, output, &error);
    EXPECT_TRUE(status.ok()) << status << "\n" << error;
    return status.ok();
  };
  const auto path = GetTestPath("types.nt");
  {
    std::ofstream file(path);
    file << R"(<urn:types:en> <urn:types:p> "same"@EN .
<urn:types:fr> <urn:types:p> "same"@fr .
<urn:types:d1> <urn:types:p> "same"^^<urn:type:1> .
<urn:types:d2> <urn:types:p> "same"^^<urn:type:2> .
<urn:types:str> <urn:types:p> "urn:types:en" .
<urn:types:iri> <urn:types:p> <urn:types:en> .
<urn:types:num> <urn:types:p> "01"^^<http://www.w3.org/2001/XMLSchema#integer> .
<urn:types:num> <urn:types:p> "1"^^<http://www.w3.org/2001/XMLSchema#integer> .
_:b <urn:types:blank> "value" .
)";
  }
  std::string output;
  ASSERT_TRUE(run({"import", path}, &output));
  const auto second = GetTestPath("other.nt");
  { std::ofstream file(second); file << "_:b <urn:types:blank> \"value\" .\n"; }
  ASSERT_TRUE(run({"import", second}, &output));
  const std::vector<std::pair<std::string, std::string>> checks = {
      {"SELECT ?s WHERE { ?s <urn:types:p> \"same\"@en }", "urn:types:en"},
      {"SELECT ?s WHERE { ?s <urn:types:p> \"same\"^^<urn:type:2> }", "urn:types:d2"},
      {"SELECT ?s WHERE { ?s <urn:types:p> \"urn:types:en\" }", "urn:types:str"},
      {"SELECT ?s WHERE { ?s <urn:types:p> <urn:types:en> }", "urn:types:iri"},
      {"SELECT ?o WHERE { <urn:types:en> <urn:types:p> ?o }", "same"},
      {"SELECT ?o WHERE { <urn:types:d1> <urn:types:p> ?o }", "same"},
      {"SELECT ?v WHERE { <urn:types:en> <urn:types:p> ?o BIND(LANG(?o) AS ?v) }", "en"},
      {"SELECT ?v WHERE { <urn:types:d1> <urn:types:p> ?o BIND(DATATYPE(?o) AS ?v) }", "urn:type:1"},
  };
  for (const auto& [query, expected] : checks) {
    ASSERT_TRUE(run({"query", query}, &output));
    ASSERT_NE(output.find("\t" + expected + "\n"), std::string::npos) << query << "\n" << output;
    ASSERT_EQ(std::count(output.begin(), output.end(), '\t'), 1) << query << "\n" << output;
  }
  ASSERT_TRUE(run({"query", "SELECT DISTINCT ?b WHERE { ?b <urn:types:blank> ?o FILTER(isBlank(?b)) }"}, &output));
  ASSERT_EQ(std::count(output.begin(), output.end(), '\t'), 2) << output;
  ASSERT_TRUE(run({"query", "SELECT ?o WHERE { <urn:types:num> <urn:types:p> ?o }"}, &output));
  ASSERT_NE(output.find("\t01\n"), std::string::npos) << output;
  ASSERT_NE(output.find("\t1\n"), std::string::npos) << output;
  ASSERT_TRUE(run({"query", "INSERT DATA { <urn:types:new> <urn:types:p> \"v.with.dot\"@fr . }"}, &output));
  ASSERT_TRUE(run({"query", "SELECT ?s WHERE { ?s <urn:types:p> \"v.with.dot\"@fr }"}, &output));
  ASSERT_NE(output.find("\turn:types:new\n"), std::string::npos) << output;
}

namespace {

std::string P4Iri(uint64_t id) { return "urn:p4:" + std::to_string(id); }
constexpr const char* kP4Query =
    "SELECT ?s ?o WHERE { ?s <urn:p4:9000> ?k . ?k <urn:p4:9001> ?o }";

Status P4Seed(const std::vector<DocDBScanBatch::Row>& triples) {
  auto& model = RdfYBModel::Instance();
  model.SetBatchJoinOptions(BatchJoinOptions());
  model.SetUseStreamingReads(true);
  model.SetStreamingPageRows(1);
  std::set<uint64_t> terms;
  for (const auto& row : triples) for (auto id : row) terms.insert(id);
  for (auto id : terms) RegisterRdfIdentity(id, RdfIdentity(TripleTermType::URI, P4Iri(id)));
  for (const auto& row : triples) RETURN_NOT_OK(model.WriteTripleToDocDB(row[0], row[1], row[2]));
  return model.Flush();
}

struct P4QueryResult {
  int status = -1;
  std::multiset<std::vector<std::string>> rows;
  std::string profile;
};

P4QueryResult P4Run(const std::string& query, BatchJoinOptions options,
                   size_t page_rows = 1, size_t columns = 2) {
  auto& model = RdfYBModel::Instance();
  model.SetBatchJoinOptions(options);
  model.SetStreamingPageRows(page_rows);
  P4QueryResult result;
  struct Capture {
    P4QueryResult* result;
    size_t columns;
    std::vector<std::string> current;
  } capture{&result, columns, {}};
  auto callback = [](const char*, const char* value, void* arg) {
    auto& state = *static_cast<Capture*>(arg);
    state.current.emplace_back(value);
    if (state.current.size() == state.columns) {
      state.result->rows.insert(std::move(state.current));
      state.current.clear();
    }
  };
  result.status = YBCMdbExecuteSparql(query.c_str(), callback, &capture);
  result.profile = YBCMdbGetLastQueryProfile();
  EXPECT_TRUE(capture.current.empty());
  return result;
}

uint64_t P4Field(const std::string& profile, const std::string& field) {
  const auto prefix = "\"" + field + "\":";
  const auto pos = profile.find(prefix);
  EXPECT_NE(pos, std::string::npos) << profile;
  return pos == std::string::npos ? 0 : std::stoull(profile.substr(pos + prefix.size()));
}

uint64_t P4ServerReadRpcs(MiniCluster* cluster) {
  uint64_t total = 0;
  for (int i = 0; i < 3; ++i) {
    total += cluster->mini_tablet_server(i)->metric_entity().FindOrCreateMetric<Histogram>(
        &METRIC_handler_latency_yb_tserver_TabletServerService_Read)->TotalCount();
  }
  return total;
}

}  // namespace

TEST_F(MdbDistributedIntegrationTest, BatchJoinBagPagesSnapshotAndReset) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  ASSERT_OK(P4Seed({{101,9000,1}, {102,9000,1}, {103,9000,1},
                    {1,9001,1001}, {1,9001,1002}}));
  std::multiset<std::vector<std::string>> expected;
  for (auto s : {101,102,103}) for (auto o : {1001,1002}) expected.insert({P4Iri(s), P4Iri(o)});
  BatchJoinOptions scalar, batch;
  scalar.enabled = false;
  batch.output_rows = 2;
  auto reference = P4Run(kP4Query, scalar);
  ASSERT_EQ(reference.status, 0);
  ASSERT_EQ(reference.rows, expected);
  auto actual = P4Run(kP4Query, batch);
  ASSERT_EQ(actual.status, 0);
  ASSERT_EQ(actual.rows, expected);
  ASSERT_EQ(P4Field(actual.profile, "unique_probes"), 1);
  ASSERT_EQ(P4Field(actual.profile, "duplicate_rows"), 2);
  ASSERT_GT(P4Field(reference.profile, "read_operations"), P4Field(actual.profile, "read_operations"));

  auto& model = RdfYBModel::Instance();
  auto context = ASSERT_RESULT(model.NewQueryReadContext());
  DocDBReadContextScope scope(*context);
  auto left = std::make_unique<TriplePlan>(Id(VarId(0)), Id(::ObjectId(9000)), Id(VarId(1)));
  auto right = std::make_unique<TriplePlan>(Id(VarId(1)), Id(::ObjectId(9001)), Id(VarId(2)));
  IndexNestedLoopPlan plan(std::move(left), std::move(right));
  auto iter = plan.get_binding_iter();
  std::ostringstream printed;
  iter->print(printed, 0, false);
  ASSERT_NE(printed.str().find("BatchIndexJoin"), std::string::npos);
  Binding binding(3);
  iter->begin(binding);
  ASSERT_TRUE(iter->next());
  RegisterRdfIdentity(1999, RdfIdentity(TripleTermType::URI, P4Iri(1999)));
  ASSERT_OK(model.BeginTransaction());
  ASSERT_OK(model.WriteTripleToDocDB(1, 9001, 1999));
  ASSERT_OK(model.CommitTransaction());
  int count = 1;
  while (iter->next()) ++count;
  ASSERT_EQ(count, 6);
  iter->reset();
  count = 0;
  while (iter->next()) ++count;
  ASSERT_EQ(count, 6);  // Reset retains this query's historical read view.
  auto fresh = P4Run(kP4Query, batch);
  ASSERT_EQ(fresh.status, 0);
  ASSERT_EQ(fresh.rows.size(), 9);
  ASSERT_LE(P4Field(fresh.profile, "peak_buffered_bytes"), batch.memory_bytes);
}

TEST_F(MdbDistributedIntegrationTest, BatchJoinGroupsRealTabletsAndReducesWireReads) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  ASSERT_NO_FATALS(InitRouter());
  auto& model = RdfYBModel::Instance();
  auto routing_session = client_->NewSession(MonoDelta::FromSeconds(30));
  std::map<std::string, std::vector<uint64_t>> by_tablet;
  for (uint64_t id = 1; id < 2048; ++id) {
    auto op = NewMdbReadOp(*model.GetTable(0), routing_session);
    QLAddInt64HashValue(op->mutable_request(), static_cast<int64_t>(id));
    std::string encoded;
    ASSERT_OK(op->GetPartitionKey(&encoded));
    auto* tablet = router_.LocateTablet(0, encoded);
    ASSERT_NE(tablet, nullptr);
    auto& keys = by_tablet[tablet->tablet_id];
    if (keys.size() < 3) keys.push_back(id);
    bool enough = by_tablet.size() == kNumTablets;
    for (const auto& entry : by_tablet) enough = enough && entry.second.size() == 3;
    if (enough) break;
  }
  ASSERT_EQ(by_tablet.size(), kNumTablets);
  std::vector<DocDBScanBatch::Row> triples;
  std::multiset<std::vector<std::string>> expected;
  for (const auto& [tablet, keys] : by_tablet) {
    ASSERT_EQ(keys.size(), 3);
    for (auto key : keys) {
      triples.push_back({key, 9001, 20000 + key});
      for (uint64_t j = 0; j < 2; ++j) {
        triples.push_back({10000 + key*2+j, 9000, key});
        expected.insert({P4Iri(10000+key*2+j), P4Iri(20000+key)});
      }
    }
  }
  ASSERT_OK(P4Seed(triples));
  BatchJoinOptions scalar, batch;
  scalar.enabled = false;
  batch.max_in_flight = 8;
  auto before = P4ServerReadRpcs(cluster_.get());
  auto reference = P4Run(kP4Query, scalar, 256);
  auto scalar_rpcs = P4ServerReadRpcs(cluster_.get()) - before;
  before = P4ServerReadRpcs(cluster_.get());
  auto actual = P4Run(kP4Query, batch, 256);
  auto batch_rpcs = P4ServerReadRpcs(cluster_.get()) - before;
  ASSERT_EQ(reference.status, 0);
  ASSERT_EQ(actual.status, 0);
  ASSERT_EQ(reference.rows, expected);
  ASSERT_EQ(actual.rows, expected);
  ASSERT_EQ(P4Field(actual.profile, "unique_probes"), 12);
  ASSERT_EQ(P4Field(actual.profile, "duplicate_rows"), 12);
  ASSERT_EQ(P4Field(actual.profile, "tablet_groups"), 4);
  ASSERT_LE(P4Field(actual.profile, "max_in_flight"), 8);
  ASSERT_GT(batch_rpcs, 0);
  ASSERT_LT(batch_rpcs, scalar_rpcs);
  ASSERT_LT(batch_rpcs, P4Field(actual.profile, "read_operations"));
  LOG(INFO) << "P4_MEASUREMENT scalar_server_read_rpcs=" << scalar_rpcs
            << " batch_server_read_rpcs=" << batch_rpcs << " profile=" << actual.profile;
  // A high-fanout key must stream through bounded pages and output batches.
  std::vector<DocDBScanBatch::Row> fanout;
  for (uint64_t i = 0; i < 100; ++i) fanout.push_back({by_tablet.begin()->second.front(),9001,30000+i});
  ASSERT_OK(P4Seed(fanout));
  batch.input_rows = 4; batch.output_rows = 3; batch.max_in_flight = 2;
  auto reference_fanout = P4Run(kP4Query, scalar, 2);
  auto actual_fanout = P4Run(kP4Query, batch, 2);
  ASSERT_EQ(reference_fanout.status, 0);
  ASSERT_EQ(actual_fanout.status, 0);
  ASSERT_EQ(actual_fanout.rows, reference_fanout.rows);
  ASSERT_EQ(actual_fanout.rows.size(), 224);
  ASSERT_LE(P4Field(actual_fanout.profile, "peak_buffered_bytes"), batch.memory_bytes);
}

TEST_F(MdbDistributedIntegrationTest, BatchJoinMissCacheCancellationBudgetAndRecovery) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  ASSERT_OK(P4Seed({{101,9000,1}, {102,9000,1}, {103,9000,1},
                    {104,9000,2}, {2,9001,1001}, {2,9001,1002}}));
  BatchJoinOptions options;
  options.input_rows = 1;
  auto actual = P4Run(kP4Query, options);
  ASSERT_EQ(actual.status, 0);
  ASSERT_EQ(actual.rows.size(), 2);
  ASSERT_EQ(P4Field(actual.profile, "unique_probes"), 2);
  ASSERT_EQ(P4Field(actual.profile, "negative_cache_hits"), 2);
  auto tiny = options;
  tiny.memory_bytes = 4096;
  EXPECT_NE(P4Run(kP4Query, tiny).status, 0);
  ASSERT_EQ(P4Run(kP4Query, options).rows, actual.rows);
  int callbacks = 0;
  auto cancel = [](const char*, const char*, void* arg) {
    ++*static_cast<int*>(arg);
    DocDBReadContextScope::Current()->cancelled = true;
  };
  EXPECT_EQ(YBCMdbExecuteSparql(
      "SELECT ?s WHERE { ?s <urn:p4:9000> ?k . ?k <urn:p4:9001> ?o }", cancel, &callbacks), 1);
  ASSERT_EQ(callbacks, 1);
  ASSERT_EQ(DocDBReadContextScope::Current(), nullptr);
  ASSERT_EQ(P4Run(kP4Query, options).rows, actual.rows);
  int expired_callbacks = 0;
  auto expire = [](const char*, const char*, void* arg) {
    ++*static_cast<int*>(arg);
    DocDBReadContextScope::Current()->deadline = CoarseMonoClock::Now();
  };
  EXPECT_EQ(YBCMdbExecuteSparql(
      "SELECT ?s WHERE { ?s <urn:p4:9000> ?k . ?k <urn:p4:9001> ?o }", expire, &expired_callbacks), 1);
  ASSERT_EQ(expired_callbacks, 1);
  ASSERT_EQ(P4Run(kP4Query, options).rows, actual.rows);
}

TEST_F(MdbDistributedIntegrationTest, BatchCursorFailureInOneOperationFailsWholePageSet) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  ASSERT_OK(P4Seed({{1,9001,1001}, {1,9001,1002}, {2,9001,1003}}));
  auto& model = RdfYBModel::Instance();
  auto context = ASSERT_RESULT(model.NewQueryReadContext());
  DocDBScanSpec spo, pos;
  spo.hash_key = 1; spo.page_rows = 1;
  pos.hash_key = 9001; pos.page_rows = 1;
  DocDBScanCursor first, second;
  ASSERT_OK(first.Open(model.GetTable(0), context->session, spo));
  ASSERT_OK(second.Open(model.GetTable(1), context->session, pos));
  auto pages = ASSERT_RESULT(DocDBScanCursor::NextBatches({&first, &second}));
  ASSERT_EQ(pages.size(), 2);
  ASSERT_FALSE(pages[0].eof);
  ASSERT_FALSE(pages[1].eof);
  ASSERT_OK(client_->DeleteTable(model.GetTable(0)->name()));
  auto failed = DocDBScanCursor::NextBatches({&first, &second});
  ASSERT_FALSE(failed.ok());
  ASSERT_FALSE(first.NextBatch().ok());
  ASSERT_FALSE(second.NextBatch().ok());
}

}  // namespace yb::mdb

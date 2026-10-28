// Copyright (c) YugabyteDB, Inc.
#include <gtest/gtest.h>
#include <arpa/inet.h>
#include "yb/mdb/query/distributed/fragment_transport.h"
#include <limits>
#include "yb/mdb/import/persistent_id_map.h"
#include "yb/util/test_thread_holder.h"
#include "yb/mdb/query/executor/wall_time_scope.h"
#include "yb/mdb/storage/managed_transaction.h"
#include "yb/client/transaction.h"
#include "yb/client/transaction_manager.h"
#include "yb/server/clock.h"
#include <iomanip>
#include "rapidjson/document.h"
#include <algorithm>
#include <chrono>
#include <cerrno>
#include <cstdlib>
#include <csignal>
#include <fstream>
#include <filesystem>
#include <pwd.h>
#include <map>
#include <set>
#include <sstream>
#include <thread>
#include <unistd.h>

#include "yb/client/schema.h"
#include "yb/client/table.h"
#include "yb/common/ql_protocol_util.h"
#include "yb/client/table_handle.h"
#include "yb/client/yb_table_name.h"
#include "yb/integration-tests/mini_cluster.h"
#include "yb/integration-tests/yb_mini_cluster_test_base.h"
#include "yb/mdb/adapter/mdb_adapter.h"
#include "yb/mdb/import/rdf_term.h"
#include "yb/mdb/model/rdf_yb_model.h"
#include "yb/mdb/storage/docdb_read_context.h"
#include "yb/mdb/storage/rdf_table_schema.h"
#include "yb/mdb/storage/yb_client_compat.h"
#include "yb/mdb/query/distributed/tablet_router.h"
#include "yb/mdb/query/distributed/fragment_gather.h"
#include "yb/mdb/query/distributed/exchange.h"
#include "yb/util/flags.h"
#include "yb/util/mem_tracker.h"
#include "yb/util/subprocess.h"
#include "yb/util/test_macros.h"
#include "yb/util/test_util.h"

DECLARE_int64(mem_tracker_tcmalloc_gc_release_bytes);

namespace yb::mdb {
namespace {

constexpr const char* kQuery =
    "SELECT ?o WHERE { ?s <urn:p6:9000> ?k . ?k <urn:p6:9001> ?o }";
std::string Iri(uint64_t id) { return "urn:p6:" + std::to_string(id); }

class MdbFragmentIntegrationTest : public MiniClusterTestWithClient<MiniCluster> {
 protected:
  void SetUp() override {
    YBMiniClusterTestBase<MiniCluster>::SetUp();
    static const int64_t gc = [] {
      (void)MemTracker::GetRootTracker();
      return FLAGS_mem_tracker_tcmalloc_gc_release_bytes;
    }();
    if (FLAGS_mem_tracker_tcmalloc_gc_release_bytes < 0)
      ANNOTATE_UNPROTECTED_WRITE(FLAGS_mem_tracker_tcmalloc_gc_release_bytes) = gc;
    MiniClusterOptions options;
    options.num_tablet_servers = 3;
    cluster_ = std::make_unique<MiniCluster>(options);
    ASSERT_OK(cluster_->Start());
    ASSERT_OK(CreateClient());
    ASSERT_OK(client_->CreateNamespaceIfNotExists("mdb_p6_test", YQL_DATABASE_CQL));
    for (int perm = 0; perm < 6; ++perm) {
      client::YBSchemaBuilder schema;
      ConfigureRdfTableSchema(&schema);
      client::TableHandle table;
      ASSERT_OK(table.Create(client::YBTableName(YQL_DATABASE_CQL, "mdb_p6_test",
          RdfYBModel::kTableNames[perm]), 4, client_.get(), &schema));
    }
    ASSERT_OK(RdfYBModel::Instance().Init(client_.get(), "mdb_p6_test"));
    RdfYBModel::Instance().SetUseStreamingReads(true);
    std::string executable;
    ASSERT_OK(env_->GetExecutablePath(&executable));
    workers_.targets = {"local", "local", "local"};
    workers_.binary = executable.substr(0, executable.find_last_of('/')) + "/../bin/mdb_fragment_worker";
    workers_.batch_rows = 2;
    workers_.page_rows = 1;
  }

  void DoTearDown() override {
    (void)YBCMdbShutdown();
    MiniClusterTestWithClient<MiniCluster>::DoTearDown();
  }

  void Write(uint64_t s, uint64_t p, uint64_t o) {
    for (auto id : {s, p, o}) RegisterRdfIdentity(id, RdfIdentity(TripleTermType::URI, Iri(id)));
    ASSERT_OK(RdfYBModel::Instance().WriteTripleToDocDB(s, p, o));
  }

  void Seed(size_t count = 32) {
    for (size_t s = 101; s < 101 + count; ++s) Write(s, 9000, 1);
    Write(1, 9001, 1001);
    Write(1, 9001, 1002);
    ASSERT_OK(RdfYBModel::Instance().Flush());
  }

  void SeedOrderedGroups() {
    const std::vector<size_t> sizes = {32, 8, 2, 8};
    uint64_t subject = 101;
    for (size_t i = 0; i < sizes.size(); ++i) {
      for (size_t j = 0; j < sizes[i]; ++j) Write(subject++, 9000, i + 1);
      Write(i + 1, 9001, 1001 + i);
    }
    ASSERT_OK(RdfYBModel::Instance().Flush());
  }

  void SeedNumericGroups() {
    SeedOrderedGroups();
    RegisterRdfIdentity(9002, RdfIdentity(TripleTermType::URI, Iri(9002)));
    for (const auto& item : std::vector<std::pair<uint64_t, std::string>>{
        {50001, "2"}, {50002, "10"}, {50003, "-4"}})
      RegisterRdfIdentity(item.first, RdfIdentity(TripleTermType::LITERAL, item.second,
          "http://www.w3.org/2001/XMLSchema#integer"));
    const std::vector<uint64_t> values = {50001, 50001, 50002, 50003};
    for (uint64_t i = 0; i < 50; ++i)
      ASSERT_OK(RdfYBModel::Instance().WriteTripleToDocDB(101 + i, 9002, values[i % 4]));
    ASSERT_OK(RdfYBModel::Instance().Flush());
  }

  struct Output {
    int status = -1;
    std::multiset<std::string> values;
    std::vector<std::string> ordered;
    std::string profile;
  };
  Output Query(const std::string& query, WorkerOptions options) {
    RdfYBModel::Instance().SetWorkerOptions(std::move(options));
    Output result;
    auto sink = [](const char*, const char* value, void* arg) {
      auto* output = static_cast<Output*>(arg);
      output->values.insert(value);
      output->ordered.emplace_back(value);
    };
    result.status = YBCMdbExecuteSparql(query.c_str(), sink, &result);
    result.profile = YBCMdbGetLastQueryProfile();
    return result;
  }
  uint64_t Field(const std::string& profile, const std::string& field) {
    const auto start = profile.find("\"" + field + "\":");
    EXPECT_NE(start, std::string::npos) << profile;
    return start == std::string::npos ? 0 : std::stoull(profile.substr(start + field.size() + 3));
  }
  WorkerOptions workers_;
};

TEST_F(MdbFragmentIntegrationTest, CrossTabletBagIdentityAndGlobalFallback) {
  Seed();
  TabletRouter router;
  ASSERT_OK(router.Init(client_.get(), "mdb_p6_test"));
  auto session = client_->NewSession(MonoDelta::FromSeconds(10));
  auto owner = [&](uint64_t key) {
    auto read = NewMdbReadOp(*RdfYBModel::Instance().GetTable(0), session);
    QLAddInt64HashValue(read->mutable_request(), static_cast<int64_t>(key));
    std::string encoded;
    EXPECT_OK(read->GetPartitionKey(&encoded));
    const auto* tablet = router.LocateTablet(0, encoded);
    EXPECT_NE(tablet, nullptr);
    return tablet ? tablet->tablet_id : std::string();
  };
  const auto inner_owner = owner(1);
  std::set<std::string> anchor_owners;
  size_t cross_tablet = 0;
  for (uint64_t subject = 101; subject < 133; ++subject) {
    const auto anchor = owner(subject);
    anchor_owners.insert(anchor);
    cross_tablet += anchor != inner_owner;
  }
  ASSERT_EQ(anchor_owners.size(), 4);
  ASSERT_GT(cross_tablet, 0);
  LOG(INFO) << "P6_CROSS_TABLET anchors=" << anchor_owners.size()
            << " inner_probe_crosses_partition=" << cross_tablet;
  std::multiset<std::string> expected;
  for (int i = 0; i < 32; ++i) { expected.insert(Iri(1001)); expected.insert(Iri(1002)); }
  const auto local = Query(kQuery, {});
  const auto remote = Query(kQuery, workers_);
  ASSERT_EQ(local.status, 0);
  ASSERT_EQ(remote.status, 0) << remote.profile;
  ASSERT_EQ(remote.values, expected);
  ASSERT_EQ(remote.values, local.values);
  ASSERT_EQ(Field(remote.profile, "remote_fragments"), 4);
  ASSERT_EQ(Field(remote.profile, "remote_max_workers"), 3);
  ASSERT_EQ(Field(remote.profile, "remote_rows"), 64);
  ASSERT_LE(Field(remote.profile, "remote_peak_frame_bytes"), 1024 * 1024);
  LOG(INFO) << "P6_PROFILE " << remote.profile;
  const auto distinct = Query(
      "SELECT DISTINCT ?o WHERE { ?s <urn:p6:9000> ?k . ?k <urn:p6:9001> ?o }", workers_);
  ASSERT_EQ(distinct.status, 0);
  ASSERT_EQ(distinct.values, (std::multiset<std::string>{Iri(1001), Iri(1002)}));
  ASSERT_EQ(Field(distinct.profile, "remote_fragments"), 4);
  ASSERT_EQ(Field(distinct.profile, "global_finalize_operators"), 1);
  ASSERT_EQ(Field(distinct.profile, "remote_coordinator_fallbacks"), 0);
  const auto missing = Query("SELECT ?s WHERE { ?s <urn:p6:missing> ?o }", workers_);
  ASSERT_EQ(missing.status, 0);
  ASSERT_TRUE(missing.values.empty());
  Write(7, 9002, 7); Write(7, 9002, 8);
  ASSERT_OK(RdfYBModel::Instance().Flush());
  const auto repeated = Query("SELECT ?x WHERE { ?x <urn:p6:9002> ?x }", workers_);
  ASSERT_EQ(repeated.status, 0);
  ASSERT_EQ(repeated.values, (std::multiset<std::string>{Iri(7)}));
}

TEST_F(MdbFragmentIntegrationTest, GlobalDistinctOrderAndSlice) {
  Seed();
  const std::string body = " WHERE { ?s <urn:p6:9000> ?k . ?k <urn:p6:9001> ?o }";
  for (const auto& query : {
      "SELECT DISTINCT ?o" + body + " ORDER BY DESC(?o) LIMIT 1 OFFSET 1",
      "SELECT ?o" + body + " ORDER BY ?o LIMIT 4 OFFSET 31",
      "SELECT ?s" + body + " ORDER BY ?o DESC(?s) LIMIT 7 OFFSET 29",
      "SELECT ?o" + body + " ORDER BY ?o LIMIT 0",
      "SELECT ?o" + body + " ORDER BY ?o OFFSET 100",
      "SELECT DISTINCT ?o" + body + " ORDER BY ?o"}) {
    const auto local = Query(query, {});
    const auto remote = Query(query, workers_);
    ASSERT_EQ(local.status, 0) << query;
    ASSERT_EQ(remote.status, 0) << query;
    ASSERT_EQ(remote.ordered, local.ordered) << query;
    ASSERT_EQ(Field(remote.profile, "remote_fragments"), 4) << remote.profile;
    ASSERT_EQ(Field(remote.profile, "remote_coordinator_fallbacks"), 0);
    ASSERT_EQ(Field(remote.profile, "global_input_rows"), 64);
    ASSERT_EQ(Field(remote.profile, "global_finalize_operators"), 1);
  }
  const auto page = Query("SELECT DISTINCT ?o" + body +
      " ORDER BY DESC(?o) LIMIT 1 OFFSET 1", workers_);
  ASSERT_EQ(page.ordered, (std::vector<std::string>{Iri(1001)}));
  const auto zero = Query("SELECT ?o" + body + " LIMIT 0", workers_);
  ASSERT_TRUE(zero.values.empty());
}

TEST_F(MdbFragmentIntegrationTest, GlobalCountAndGroups) {
  Seed();
  const std::vector<std::string> twos(32, "2");
  const std::string body = " WHERE { ?s <urn:p6:9000> ?k . ?k <urn:p6:9001> ?o }";
  const std::vector<std::pair<std::string, std::multiset<std::string>>> cases = {
    {"SELECT (COUNT(*) AS ?n)" + body, {"64"}},
    {"SELECT (COUNT(?o) AS ?n)" + body, {"64"}},
    {"SELECT (COUNT(DISTINCT ?o) AS ?n)" + body, {"2"}},
    {"SELECT (COUNT(DISTINCT *) AS ?n)" + body, {"64"}},
    {"SELECT (COUNT(?missing) AS ?n)" + body, {"0"}},
    {"SELECT (COUNT(*) AS ?n)" + body + " GROUP BY ?o", {"32", "32"}},
    {"SELECT (COUNT(DISTINCT ?o) AS ?n)" + body + " GROUP BY ?s",
      std::multiset<std::string>(twos.begin(), twos.end())},
    {"SELECT (COUNT(*) AS ?n) WHERE { ?s <urn:p6:missing> ?o }", {"0"}},
    {"SELECT (COUNT(DISTINCT ?o) AS ?n) WHERE { ?s <urn:p6:missing> ?o }", {"0"}},
    {"SELECT (COUNT(*) AS ?n) WHERE { ?s <urn:p6:missing> ?o } GROUP BY ?s", {}}
  };
  for (const auto& item : cases) {
    const auto remote = Query(item.first, workers_);
    const auto local = Query(item.first, {});
    ASSERT_EQ(remote.status, 0) << item.first << remote.profile;
    ASSERT_EQ(local.status, 0) << item.first;
    ASSERT_EQ(remote.values, item.second) << item.first;
    ASSERT_EQ(remote.values, local.values) << item.first;
    ASSERT_EQ(Field(remote.profile, "global_finalize_operators"), 1);
    ASSERT_EQ(Field(remote.profile, "remote_coordinator_fallbacks"), 0);
  }
  Write(133, 9000, 2);
  Write(2, 9001, 1001);
  ASSERT_OK(RdfYBModel::Instance().Flush());
  const auto grouped = Query("SELECT ?o (COUNT(*) AS ?n)" + body + " GROUP BY ?o", workers_);
  ASSERT_EQ(grouped.status, 0);
  ASSERT_EQ(grouped.ordered.size(), 4);
  std::map<std::string, std::string> groups;
  for (size_t i = 0; i < grouped.ordered.size(); i += 2)
    groups.emplace(grouped.ordered[i], grouped.ordered[i + 1]);
  ASSERT_EQ(groups, (std::map<std::string, std::string>{{Iri(1001), "33"}, {Iri(1002), "32"}}));
}

TEST_F(MdbFragmentIntegrationTest, GlobalOrderedCountGroupsAndSlice) {
  SeedOrderedGroups();
  // Each input subject belongs to exactly one physical anchor partition. The
  // largest group spans multiple tablets, so sorting partial counts is wrong.
  TabletRouter router;
  ASSERT_OK(router.Init(client_.get(), "mdb_p6_test"));
  auto session = client_->NewSession(MonoDelta::FromSeconds(10));
  std::set<std::string> owners;
  for (uint64_t s = 101; s < 133; ++s) {
    auto read = NewMdbReadOp(*RdfYBModel::Instance().GetTable(0), session);
    QLAddInt64HashValue(read->mutable_request(), static_cast<int64_t>(s));
    std::string encoded;
    ASSERT_OK(read->GetPartitionKey(&encoded));
    const auto* tablet = router.LocateTablet(0, encoded);
    ASSERT_NE(tablet, nullptr);
    owners.insert(tablet->tablet_id);
  }
  ASSERT_EQ(owners.size(), 4);
  const std::string body = " WHERE { ?s <urn:p6:9000> ?k . ?k <urn:p6:9001> ?o }";
  const std::string grouped = body + " GROUP BY ?o";
  const std::string counts = "SELECT ?o (COUNT(*) AS ?n)" + grouped;
  const std::vector<std::pair<std::string, std::vector<std::string>>> cases = {
    {counts + " ORDER BY DESC(?n) ?o",
      {Iri(1001), "32", Iri(1002), "8", Iri(1004), "8", Iri(1003), "2"}},
    {counts + " ORDER BY ?n DESC(?o)",
      {Iri(1003), "2", Iri(1004), "8", Iri(1002), "8", Iri(1001), "32"}},
    {counts + " ORDER BY DESC(?n) ?o LIMIT 2 OFFSET 1",
      {Iri(1002), "8", Iri(1004), "8"}},
    {"SELECT (COUNT(?s) AS ?n)" + grouped + " ORDER BY DESC(?n) ?o",
      {"32", "8", "8", "2"}},
    {"SELECT (COUNT(DISTINCT ?s) AS ?n)" + grouped + " ORDER BY ?n DESC(?o)",
      {"2", "8", "8", "32"}},
    {"SELECT (COUNT(DISTINCT *) AS ?n)" + grouped + " ORDER BY ?n ?o",
      {"2", "8", "8", "32"}},
    {"SELECT DISTINCT (COUNT(*) AS ?n)" + grouped + " ORDER BY ?n LIMIT 1 OFFSET 1", {"8"}},
    {"SELECT ?o" + grouped + " ORDER BY DESC(?o)",
      {Iri(1004), Iri(1003), Iri(1002), Iri(1001)}},
    {"SELECT ?o (COUNT(?missing) AS ?n)" + grouped + " ORDER BY ?n DESC(?o) LIMIT 2",
      {Iri(1004), "0", Iri(1003), "0"}},
    {"SELECT (COUNT(*) AS ?n)" + body + " ORDER BY DESC(?n)", {"50"}},
    {"SELECT (COUNT(*) AS ?n)" + body + " GROUP BY ?missing ORDER BY ?n", {"50"}},
    {counts + " ORDER BY DESC(?n) DESC(?n) ?o LIMIT 1", {Iri(1001), "32"}},
    {counts + " ORDER BY ?n LIMIT 0", {}},
    {counts + " ORDER BY ?n OFFSET 9", {}}
  };
  for (const auto& item : cases) {
    SCOPED_TRACE(item.first);
    const auto local = Query(item.first, {});
    const auto remote = Query(item.first, workers_);
    ASSERT_EQ(local.status, 0);
    ASSERT_EQ(remote.status, 0) << remote.profile;
    // All projected cells are bound. Comparing this sequence checks complete
    // rows, tie-breaks and bag multiplicity, not just a multiset of cells.
    ASSERT_EQ(local.ordered, item.second);
    ASSERT_EQ(remote.ordered, item.second);
    ASSERT_EQ(Field(remote.profile, "remote_fragments"), 4);
    ASSERT_EQ(Field(remote.profile, "remote_coordinator_fallbacks"), 0);
    ASSERT_EQ(Field(remote.profile, "remote_rows"), 50);
    ASSERT_EQ(Field(remote.profile, "global_input_rows"), 50);
    ASSERT_EQ(Field(remote.profile, "global_finalize_operators"), 1);
  }
  for (bool group : {false, true}) {
    const std::string query = "SELECT (COUNT(*) AS ?n) WHERE { ?s <urn:p6:missing> ?o }" +
        std::string(group ? " GROUP BY ?o" : "") + " ORDER BY ?n LIMIT 1";
    const auto remote = Query(query, workers_);
    const auto local = Query(query, {});
    ASSERT_EQ(remote.status, 0);
    ASSERT_EQ(local.status, 0);
    ASSERT_EQ(remote.ordered, (group ? std::vector<std::string>{} : std::vector<std::string>{"0"}));
    ASSERT_EQ(remote.ordered, local.ordered);
    ASSERT_EQ(Field(remote.profile, "remote_coordinator_fallbacks"), 0);
    ASSERT_EQ(Field(remote.profile, "global_finalize_operators"), 1);
  }
}

TEST_F(MdbFragmentIntegrationTest, GlobalOrderedGroupBudgetFailureAndRecovery) {
  SeedOrderedGroups();
  const std::string query = "SELECT ?o (COUNT(*) AS ?n) WHERE { ?s <urn:p6:9000> ?k . "
      "?k <urn:p6:9001> ?o } GROUP BY ?o ORDER BY DESC(?n) ?o LIMIT 1";
  const auto success = Query(query, workers_);
  ASSERT_EQ(success.status, 0);
  ASSERT_EQ(success.ordered, (std::vector<std::string>{Iri(1001), "32"}));
  // Four variables, two sort keys. LIMIT does not reduce Gather's admission.
  ASSERT_EQ(Field(success.profile, "global_reserved_bytes"), 50 * (1024 + 128 * 4 + 16 * 2));
  auto limited = workers_;
  limited.coordinator_memory_bytes = 50 * (1024 + 128 * 4);
  const auto exhausted = Query(query, limited);
  ASSERT_NE(exhausted.status, 0);
  ASSERT_TRUE(exhausted.ordered.empty());
  ASSERT_EQ(Field(exhausted.profile, "remote_coordinator_fallbacks"), 0);
  auto broken = workers_;
  broken.binary = "/bin/false";
  const auto failed = Query(query, broken);
  ASSERT_NE(failed.status, 0);
  ASSERT_TRUE(failed.ordered.empty());
  ASSERT_EQ(Field(failed.profile, "remote_coordinator_fallbacks"), 0);
  for (bool expire : {false, true}) {
    struct Stop { bool expire; size_t cells = 0; } stop{expire};
    auto sink = [](const char*, const char*, void* arg) {
      auto& state = *static_cast<Stop*>(arg);
      ++state.cells;
      auto* context = DocDBReadContextScope::Current();
      if (state.expire) context->deadline = CoarseMonoClock::Now();
      else context->cancelled.store(true);
    };
    RdfYBModel::Instance().SetWorkerOptions(workers_);
    ASSERT_NE(YBCMdbExecuteSparql(query.c_str(), sink, &stop), 0);
    ASSERT_EQ(stop.cells, 2);
    ASSERT_EQ(Field(YBCMdbGetLastQueryProfile(), "remote_coordinator_fallbacks"), 0);
  }
  const auto recovered = Query(query, workers_);
  ASSERT_EQ(recovered.status, 0);
  ASSERT_EQ(recovered.ordered, success.ordered);
}

TEST_F(MdbFragmentIntegrationTest, GlobalHavingCountGroupsAndSlice) {
  SeedOrderedGroups();
  const std::string body = " WHERE { ?s <urn:p6:9000> ?k . ?k <urn:p6:9001> ?o }";
  const std::string grouped = body + " GROUP BY ?o";
  const std::string counts = "SELECT ?o (COUNT(*) AS ?n)" + grouped;
  const std::vector<std::pair<std::string, std::vector<std::string>>> cases = {
    {counts + " HAVING (COUNT(*) > 8) ORDER BY DESC(?n) ?o", {Iri(1001), "32"}},
    {counts + " HAVING (COUNT(*) >= 8) ORDER BY DESC(?n) ?o LIMIT 2 OFFSET 1",
      {Iri(1002), "8", Iri(1004), "8"}},
    {counts + " HAVING (COUNT(*) <= 2) ORDER BY ?o", {Iri(1003), "2"}},
    {counts + " HAVING (COUNT(*) < 8) ORDER BY ?o", {Iri(1003), "2"}},
    {counts + " HAVING (COUNT(*) != 8) ORDER BY ?o", {Iri(1001), "32", Iri(1003), "2"}},
    {counts + " HAVING (COUNT(*) >= 2) (COUNT(*) <= 8) ORDER BY DESC(?n) ?o",
      {Iri(1002), "8", Iri(1004), "8", Iri(1003), "2"}},
    {counts + " HAVING (COUNT(DISTINCT ?s) > 8) ORDER BY ?o", {Iri(1001), "32"}},
    {counts + " HAVING (COUNT(DISTINCT *) > 8) ORDER BY ?o", {Iri(1001), "32"}},
    {counts + " HAVING (COUNT(*) = COUNT(?s)) (COUNT(DISTINCT ?o) = 1) "
      "ORDER BY ?n ?o LIMIT 1", {Iri(1003), "2"}},
    {counts + " HAVING (COUNT(?missing) = 0) ORDER BY ?n ?o LIMIT 1", {Iri(1003), "2"}},
    {counts + " HAVING (COUNT(*)) ORDER BY ?n ?o LIMIT 1", {Iri(1003), "2"}},
    {"SELECT ?o" + grouped + " HAVING (COUNT(*) >= 8) ORDER BY ?o",
      {Iri(1001), Iri(1002), Iri(1004)}},
    {"SELECT DISTINCT (COUNT(*) AS ?n)" + grouped +
      " HAVING (COUNT(*) >= 8) ORDER BY ?n LIMIT 1 OFFSET 1", {"32"}},
    {counts + " HAVING (?o = ?o) ORDER BY ?n ?o LIMIT 1", {Iri(1003), "2"}},
    {counts + " HAVING (COUNT(*) > 0) ORDER BY ?n LIMIT 0", {}},
    {counts + " HAVING (COUNT(*) > 0) ORDER BY ?n OFFSET 9", {}},
    {"SELECT (COUNT(*) AS ?n)" + body + " HAVING (COUNT(*) = 50) ORDER BY ?n", {"50"}},
    {"SELECT (COUNT(*) AS ?n)" + body + " HAVING (COUNT(*) = 0) ORDER BY ?n", {}}
  };
  for (const auto& item : cases) {
    SCOPED_TRACE(item.first);
    const auto local = Query(item.first, {});
    const auto remote = Query(item.first, workers_);
    ASSERT_EQ(local.status, 0);
    ASSERT_EQ(remote.status, 0) << remote.profile;
    ASSERT_EQ(local.ordered, item.second);
    ASSERT_EQ(remote.ordered, item.second);
    ASSERT_EQ(Field(remote.profile, "remote_fragments"), 4);
    ASSERT_EQ(Field(remote.profile, "remote_coordinator_fallbacks"), 0);
    ASSERT_EQ(Field(remote.profile, "global_input_rows"), 50);
    ASSERT_EQ(Field(remote.profile, "global_finalize_operators"), 1);
  }
}

TEST_F(MdbFragmentIntegrationTest, GlobalHavingErrorsBooleanAndEmptyInput) {
  SeedOrderedGroups();
  const std::string counts = "SELECT ?o (COUNT(*) AS ?n) WHERE { ?s <urn:p6:9000> ?k . "
      "?k <urn:p6:9001> ?o } GROUP BY ?o ?missing HAVING ";
  const std::vector<std::pair<std::string, std::vector<std::string>>> cases = {
    {"(!(?missing = ?missing))", {}},
    {"((?missing = ?missing) || (COUNT(*) > 8))", {Iri(1001), "32"}},
    {"((COUNT(*) > 8) || (?missing = ?missing))", {Iri(1001), "32"}},
    {"((?missing = ?missing) && false)", {}},
    {"((?missing = ?missing) && true)", {}},
    {"(!((?missing = ?missing) && false))", {Iri(1001), "32"}},
    {"(!(COUNT(?missing)))", {Iri(1001), "32"}},
    {"((COUNT(*) > 8) && !(COUNT(*) < 8))", {Iri(1001), "32"}},
    {"((COUNT(*) = 8) || (COUNT(*) = 2))", {Iri(1002), "8"}}
  };
  for (const auto& item : cases) {
    const auto query = counts + item.first + " ORDER BY ?o LIMIT 1";
    SCOPED_TRACE(query);
    const auto local = Query(query, {});
    const auto remote = Query(query, workers_);
    ASSERT_EQ(local.status, 0);
    ASSERT_EQ(remote.status, 0);
    ASSERT_EQ(local.ordered, item.second);
    ASSERT_EQ(remote.ordered, item.second);
    ASSERT_EQ(Field(remote.profile, "remote_coordinator_fallbacks"), 0);
    ASSERT_EQ(Field(remote.profile, "global_finalize_operators"), 1);
  }
  for (const auto& item : std::vector<std::pair<std::string, std::vector<std::string>>>{
      {" HAVING (COUNT(*) = 0)", {"0"}},
      {" HAVING (COUNT(*) > 0)", {}},
      {" HAVING (COUNT(DISTINCT *) = 0)", {"0"}},
      {" GROUP BY ?o HAVING (COUNT(*) = 0)", {}}}) {
    const std::string query = "SELECT (COUNT(*) AS ?n) WHERE { ?s <urn:p6:missing> ?o }" +
        item.first + " ORDER BY ?n";
    const auto local = Query(query, {});
    const auto remote = Query(query, workers_);
    ASSERT_EQ(local.status, 0) << query;
    ASSERT_EQ(remote.status, 0) << query;
    ASSERT_EQ(remote.ordered, item.second) << query;
    ASSERT_EQ(local.ordered, item.second) << query;
    ASSERT_EQ(Field(remote.profile, "remote_coordinator_fallbacks"), 0);
  }
}

TEST_F(MdbFragmentIntegrationTest, GlobalHavingBudgetFailureAndRecovery) {
  SeedOrderedGroups();
  const std::string query = "SELECT ?o (COUNT(*) AS ?n) WHERE { ?s <urn:p6:9000> ?k . "
      "?k <urn:p6:9001> ?o } GROUP BY ?o "
      "HAVING (COUNT(DISTINCT *) >= 8) (COUNT(DISTINCT *) <= 32) ORDER BY DESC(?n) ?o LIMIT 1";
  const auto success = Query(query, workers_);
  ASSERT_EQ(success.status, 0);
  ASSERT_EQ(success.ordered, (std::vector<std::string>{Iri(1001), "32"}));
  // Four parsed variables, two private aggregate slots, two tuple sets,
  // two ordering keys and six HAVING expression nodes.
  const uint64_t per_row = 1024 + 128 * 6 + 16 * 2 * 6 + 16 * 2 + 64 * 6;
  ASSERT_EQ(Field(success.profile, "global_reserved_bytes"), per_row * 50);
  auto limited = workers_;
  limited.coordinator_memory_bytes = per_row * 50 - 1;
  const auto exhausted = Query(query, limited);
  ASSERT_NE(exhausted.status, 0);
  ASSERT_TRUE(exhausted.ordered.empty());
  ASSERT_EQ(Field(exhausted.profile, "remote_coordinator_fallbacks"), 0);
  auto broken = workers_;
  broken.binary = "/bin/false";
  const auto failed = Query(query, broken);
  ASSERT_NE(failed.status, 0);
  ASSERT_TRUE(failed.ordered.empty());
  ASSERT_EQ(Field(failed.profile, "remote_coordinator_fallbacks"), 0);
  for (bool expire : {false, true}) {
    struct Stop { bool expire; size_t cells = 0; } stop{expire};
    auto sink = [](const char*, const char*, void* arg) {
      auto& state = *static_cast<Stop*>(arg);
      ++state.cells;
      auto* context = DocDBReadContextScope::Current();
      if (state.expire) context->deadline = CoarseMonoClock::Now();
      else context->cancelled.store(true);
    };
    RdfYBModel::Instance().SetWorkerOptions(workers_);
    ASSERT_NE(YBCMdbExecuteSparql(query.c_str(), sink, &stop), 0);
    ASSERT_EQ(stop.cells, 2);
    ASSERT_EQ(Field(YBCMdbGetLastQueryProfile(), "remote_coordinator_fallbacks"), 0);
  }
  const auto recovered = Query(query, workers_);
  ASSERT_EQ(recovered.status, 0);
  ASSERT_EQ(recovered.ordered, success.ordered);
}

TEST_F(MdbFragmentIntegrationTest, GlobalProjectionCountExpressionsAndAliases) {
  SeedOrderedGroups();
  const std::string body = " WHERE { ?s <urn:p6:9000> ?k . ?k <urn:p6:9001> ?o }";
  const std::string grouped = body + " GROUP BY ?o";
  const std::string counts = "SELECT ?o ((COUNT(*) + 1) AS ?n)" + grouped;
  const std::vector<std::pair<std::string, std::vector<std::string>>> cases = {
    {counts + " ORDER BY DESC(?n) ?o",
      {Iri(1001), "33", Iri(1002), "9", Iri(1004), "9", Iri(1003), "3"}},
    {counts + " HAVING (COUNT(*) >= 8) ORDER BY DESC(?n) ?o LIMIT 2 OFFSET 1",
      {Iri(1002), "9", Iri(1004), "9"}},
    {"SELECT (COUNT(*) AS ?raw) ((?raw + 1) AS ?n) ((?n - 1) AS ?again)" + grouped +
      " ORDER BY DESC(?n) ?o", {"32", "33", "32", "8", "9", "8", "8", "9", "8", "2", "3", "2"}},
    {"SELECT ?o ((COUNT(DISTINCT ?s) + 1) AS ?n)" + grouped + " ORDER BY ?n ?o LIMIT 1",
      {Iri(1003), "3"}},
    {"SELECT ?o ((COUNT(DISTINCT *) + 1) AS ?n)" + grouped + " ORDER BY ?n ?o LIMIT 1",
      {Iri(1003), "3"}},
    {"SELECT ?o ((COUNT(?missing) + 1) AS ?n)" + grouped + " ORDER BY DESC(?o) LIMIT 1",
      {Iri(1004), "1"}},
    {"SELECT DISTINCT ((COUNT(*) + 1) AS ?n)" + grouped + " ORDER BY ?n LIMIT 1 OFFSET 1", {"9"}},
    {"SELECT (?o AS ?group) ((COUNT(*) + 1) AS ?n)" + grouped + " ORDER BY DESC(?group) LIMIT 1",
      {Iri(1004), "9"}},
    {"SELECT ?o (7 AS ?n)" + grouped + " ORDER BY ?o LIMIT 1", {Iri(1001), "7"}},
    {"SELECT ?o ((COUNT(*) + COUNT(?s) - 1) AS ?n)" + grouped + " ORDER BY ?o LIMIT 1",
      {Iri(1001), "63"}},
    {"SELECT ?o ((9223372036854775807 - COUNT(*)) AS ?n)" + grouped + " ORDER BY ?o LIMIT 1",
      {Iri(1001), "9223372036854775775"}},
    {"SELECT ((COUNT(*) + 1) AS ?n)" + body + " ORDER BY ?n", {"51"}},
    {counts + " ORDER BY ?n LIMIT 0", {}},
    {counts + " ORDER BY ?n OFFSET 9", {}}
  };
  for (const auto& item : cases) {
    SCOPED_TRACE(item.first);
    const auto local = Query(item.first, {});
    const auto remote = Query(item.first, workers_);
    ASSERT_EQ(local.status, 0);
    ASSERT_EQ(remote.status, 0) << remote.profile;
    ASSERT_EQ(local.ordered, item.second);
    ASSERT_EQ(remote.ordered, item.second);
    ASSERT_EQ(Field(remote.profile, "remote_fragments"), 4);
    ASSERT_EQ(Field(remote.profile, "remote_coordinator_fallbacks"), 0);
    ASSERT_EQ(Field(remote.profile, "global_input_rows"), 50);
  }
  for (const auto& item : std::vector<std::pair<std::string, std::vector<std::string>>>{
      {"", {"1"}}, {" HAVING (COUNT(*) = 0)", {"1"}},
      {" HAVING (COUNT(*) > 0)", {}}, {" GROUP BY ?o", {}}}) {
    const std::string query = "SELECT ((COUNT(*) + 1) AS ?n) WHERE { ?s <urn:p6:missing> ?o }" +
        item.first + " ORDER BY ?n";
    const auto local = Query(query, {});
    const auto remote = Query(query, workers_);
    ASSERT_EQ(local.status, 0) << query;
    ASSERT_EQ(remote.status, 0) << query;
    ASSERT_EQ(local.ordered, item.second);
    ASSERT_EQ(remote.ordered, item.second);
    ASSERT_EQ(Field(remote.profile, "remote_coordinator_fallbacks"), 0);
  }
}

TEST_F(MdbFragmentIntegrationTest, GlobalProjectionErrorsBudgetAndRecovery) {
  SeedOrderedGroups();
  const std::string body = " WHERE { ?s <urn:p6:9000> ?k . ?k <urn:p6:9001> ?o } GROUP BY ?o";
  // The cell callback omits unbound cells. Keep the group key bound and test
  // its complete ordered bag, so errors must retain all four solution rows.
  for (const auto& expr : {"COUNT(*) + 9223372036854775807",
                          "0 - 9223372036854775807 - COUNT(*)"}) {
    const std::string query = "SELECT ?o ((" + std::string(expr) +
        ") AS ?bad) ((?bad + 1) AS ?next)" + body + " ORDER BY ?o";
    const auto local = Query(query, {});
    const auto remote = Query(query, workers_);
    ASSERT_EQ(local.status, 0) << query;
    ASSERT_EQ(remote.status, 0) << query;
    ASSERT_EQ(remote.ordered, (std::vector<std::string>{Iri(1001), Iri(1002), Iri(1003), Iri(1004)}));
    ASSERT_EQ(remote.ordered, local.ordered);
    ASSERT_EQ(Field(remote.profile, "remote_coordinator_fallbacks"), 0);
  }
  const std::string query = "SELECT ?o ((COUNT(DISTINCT *) + 1) AS ?n)" + body +
      " HAVING (COUNT(DISTINCT *) >= 2) ORDER BY DESC(?n) ?o LIMIT 1";
  const auto success = Query(query, workers_);
  ASSERT_EQ(success.status, 0);
  ASSERT_EQ(success.ordered, (std::vector<std::string>{Iri(1001), "33"}));
  // Four parsed variables, one private COUNT in SELECT and one in HAVING;
  // two tuple sets, two sort keys, three HAVING and three SELECT nodes.
  const uint64_t per_row = 1024 + 128 * 6 + 16 * 2 * 6 + 16 * 2 + 64 * 3 + 512 * 3;
  ASSERT_EQ(Field(success.profile, "global_reserved_bytes"), per_row * 50);
  auto limited = workers_;
  limited.coordinator_memory_bytes = per_row * 50 - 1;
  const auto exhausted = Query(query, limited);
  ASSERT_NE(exhausted.status, 0);
  ASSERT_TRUE(exhausted.ordered.empty());
  ASSERT_EQ(Field(exhausted.profile, "remote_coordinator_fallbacks"), 0);
  auto broken = workers_;
  broken.binary = "/bin/false";
  const auto failed = Query(query, broken);
  ASSERT_NE(failed.status, 0);
  ASSERT_TRUE(failed.ordered.empty());
  ASSERT_EQ(Field(failed.profile, "remote_coordinator_fallbacks"), 0);
  for (bool expire : {false, true}) {
    struct Stop { bool expire; size_t cells = 0; } stop{expire};
    auto sink = [](const char*, const char*, void* arg) {
      auto& state = *static_cast<Stop*>(arg);
      ++state.cells;
      auto* context = DocDBReadContextScope::Current();
      if (state.expire) context->deadline = CoarseMonoClock::Now();
      else context->cancelled.store(true);
    };
    RdfYBModel::Instance().SetWorkerOptions(workers_);
    ASSERT_NE(YBCMdbExecuteSparql(query.c_str(), sink, &stop), 0);
    ASSERT_EQ(stop.cells, 2);
    ASSERT_EQ(Field(YBCMdbGetLastQueryProfile(), "remote_coordinator_fallbacks"), 0);
  }
  const auto recovered = Query(query, workers_);
  ASSERT_EQ(recovered.status, 0);
  ASSERT_EQ(recovered.ordered, success.ordered);
}

TEST_F(MdbFragmentIntegrationTest, GlobalProjectionScopeAndCapabilityFallback) {
  SeedOrderedGroups();
  auto unreachable = workers_;
  unreachable.binary = "/bin/false";
  const std::string body = " WHERE { ?s <urn:p6:9000> ?k . ?k <urn:p6:9001> ?o } GROUP BY ?o";
  for (const auto& query : {
      "SELECT ((?later + 1) AS ?n) (COUNT(*) AS ?later)" + body,
      "SELECT ((COUNT(*) + 1) AS ?n)" + body + " HAVING (?n > 1)"}) {
    const auto remote = Query(query, unreachable);
    ASSERT_NE(remote.status, 0);
    ASSERT_TRUE(remote.ordered.empty());
    ASSERT_EQ(Field(remote.profile, "remote_fragments"), 0);
    ASSERT_EQ(Field(remote.profile, "remote_coordinator_fallbacks"), 1);
  }
  std::string deep = "COUNT(*)";
  for (size_t i = 0; i < 18; ++i) deep = "(" + deep + " + 1)";
  std::string many = "COUNT(*)";
  for (size_t i = 0; i < 16; ++i) many += " + COUNT(*)";
  std::vector<std::string> nodes(65, "1");
  nodes[0] = "COUNT(*)";
  while (nodes.size() > 1) {
    std::vector<std::string> next;
    for (size_t i = 0; i < nodes.size(); i += 2)
      next.push_back(i + 1 < nodes.size() ? "(" + nodes[i] + " + " + nodes[i + 1] + ")" : nodes[i]);
    nodes = std::move(next);
  }
  for (const auto& expr : {std::string("COUNT(*) * 2"), std::string("GROUP_CONCAT(?s) + 1"),
                          std::string("?o + 1"), deep, many, nodes.front()}) {
    const std::string query = "SELECT ((" + expr + ") AS ?n)" + body + " ORDER BY ?o LIMIT 1";
    const auto local = Query(query, {});
    const auto fallback = Query(query, unreachable);
    ASSERT_EQ(fallback.status, local.status) << query;
    ASSERT_EQ(fallback.ordered, local.ordered) << query;
    ASSERT_EQ(Field(fallback.profile, "remote_fragments"), 0);
    ASSERT_EQ(Field(fallback.profile, "remote_coordinator_fallbacks"), 1);
  }
}

TEST_F(MdbFragmentIntegrationTest, GlobalNumericAggregatesAndDistinct) {
  SeedNumericGroups();
  const std::string body = " WHERE { ?s <urn:p6:9000> ?k . ?k <urn:p6:9001> ?g . "
      "?s <urn:p6:9002> ?v }";
  const std::string grouped = body + " GROUP BY ?g";
  const std::vector<std::pair<std::string, std::vector<std::string>>> cases = {
    {"SELECT ?g (SUM(?v) AS ?t) (AVG(?v) AS ?a) (MIN(?v) AS ?lo) (MAX(?v) AS ?hi)" +
      grouped + " ORDER BY DESC(?t) ?g",
      {Iri(1001), "80", "2.5", "-4", "10", Iri(1002), "20", "2.5", "-4", "10",
       Iri(1004), "20", "2.5", "-4", "10", Iri(1003), "4", "2.0", "2", "2"}},
    {"SELECT ?g (SUM(DISTINCT ?v) AS ?t) (AVG(DISTINCT ?v) AS ?a) "
      "(MIN(DISTINCT ?v) AS ?lo) (MAX(DISTINCT ?v) AS ?hi)" + grouped + " ORDER BY ?g LIMIT 1",
      {Iri(1001), "8", "2.66666666666666667", "-4", "10"}},
    {"SELECT ?g (SUM(?v) AS ?t)" + grouped + " HAVING (SUM(?v) > 4) (AVG(?v) >= 2) "
      "ORDER BY DESC(?t) ?g LIMIT 2 OFFSET 1", {Iri(1002), "20", Iri(1004), "20"}},
    {"SELECT ((SUM(?v) + 1) AS ?t) ((?t - 1) AS ?again) (AVG(?v) AS ?a)" + grouped +
      " ORDER BY DESC(?t) ?g LIMIT 1", {"81", "80", "2.5"}},
    {"SELECT DISTINCT (SUM(DISTINCT ?v) AS ?t)" + grouped + " ORDER BY ?t LIMIT 1 OFFSET 1", {"8"}},
    {"SELECT (SUM(?v) AS ?t) (AVG(?v) AS ?a) (MIN(?v) AS ?lo) (MAX(?v) AS ?hi)" + body,
      {"124", "2.48", "-4", "10"}},
    {"SELECT ?g (MIN(?v) AS ?lo)" + grouped + " HAVING (MIN(?v) < 0) (MAX(?v) = 10) "
      "ORDER BY ?g LIMIT 1", {Iri(1001), "-4"}},
    {"SELECT (SUM(?v) AS ?t)" + grouped + " ORDER BY ?t LIMIT 0", {}},
    {"SELECT (AVG(?v) AS ?a)" + grouped + " ORDER BY ?a OFFSET 9", {}}
  };
  for (const auto& item : cases) {
    SCOPED_TRACE(item.first);
    const auto local = Query(item.first, {});
    const auto remote = Query(item.first, workers_);
    ASSERT_EQ(local.status, 0); ASSERT_EQ(remote.status, 0) << remote.profile;
    ASSERT_EQ(local.ordered, item.second); ASSERT_EQ(remote.ordered, item.second);
    ASSERT_EQ(Field(remote.profile, "remote_fragments"), 4);
    ASSERT_EQ(Field(remote.profile, "remote_coordinator_fallbacks"), 0);
    ASSERT_EQ(Field(remote.profile, "global_input_rows"), 50);
  }
  const std::string empty = "SELECT (COUNT(*) AS ?n) (SUM(?v) AS ?t) (AVG(?v) AS ?a) "
      "(MIN(?v) AS ?lo) (MAX(?v) AS ?hi) WHERE { ?s <urn:p6:missing> ?v }";
  for (const auto& suffix : {"", " GROUP BY ?s"}) {
    const auto local = Query(empty + suffix, {});
    const auto remote = Query(empty + suffix, workers_);
    ASSERT_EQ(remote.status, 0); ASSERT_EQ(local.status, 0);
    ASSERT_EQ(remote.ordered, suffix[0] ? std::vector<std::string>{} :
        (std::vector<std::string>{"0", "0", "0"}));
    ASSERT_EQ(remote.ordered, local.ordered);
    ASSERT_EQ(Field(remote.profile, "remote_coordinator_fallbacks"), 0);
  }
}

TEST_F(MdbFragmentIntegrationTest, GlobalNumericTypesErrorsAndExtrema) {
  const std::string xsd = "http://www.w3.org/2001/XMLSchema#";
  RegisterRdfIdentity(9004, RdfIdentity(TripleTermType::URI, Iri(9004)));
  const std::vector<std::pair<std::string, std::string>> inputs = {
    {"1", "integer"}, {"2.5", "decimal"}, {"3.5", "float"}, {"1", "double"}};
  for (size_t i = 0; i < inputs.size(); ++i) {
    RegisterRdfIdentity(101 + i, RdfIdentity(TripleTermType::URI, Iri(101 + i)));
    RegisterRdfIdentity(60000 + i, RdfIdentity(TripleTermType::LITERAL, inputs[i].first, xsd + inputs[i].second));
    ASSERT_OK(RdfYBModel::Instance().WriteTripleToDocDB(101 + i, 9004, 60000 + i));
  }
  ASSERT_OK(RdfYBModel::Instance().Flush());
  for (const auto& item : std::vector<std::pair<std::string, std::vector<std::string>>>{
      {"SELECT (SUM(?v) AS ?t) (AVG(?v) AS ?a) (MIN(?v) AS ?lo) (MAX(?v) AS ?hi) "
       "WHERE { ?s <urn:p6:9004> ?v }", {"8", "2", "1", "3.5"}},
      {"SELECT (COUNT(*) AS ?n) (SUM(?s) AS ?t) (AVG(?s) AS ?a) (MIN(?s) AS ?lo) (MAX(?s) AS ?hi) "
       "WHERE { ?s <urn:p6:9004> ?v }", {"4", Iri(101), Iri(104)}},
      {"SELECT (COUNT(*) AS ?n) (SUM(?missing) AS ?t) (AVG(?missing) AS ?a) "
       "(MIN(?missing) AS ?lo) (MAX(?missing) AS ?hi) WHERE { ?s <urn:p6:9004> ?v }", {"4", "0"}},
      {"SELECT (COUNT(*) AS ?n) (SUM(?s) AS ?t) WHERE { ?s <urn:p6:9004> ?v } HAVING (SUM(?s) > 0)", {}}}) {
    SCOPED_TRACE(item.first);
    const auto local = Query(item.first, {});
    const auto remote = Query(item.first, workers_);
    ASSERT_EQ(local.status, 0); ASSERT_EQ(remote.status, 0);
    ASSERT_EQ(local.ordered, item.second); ASSERT_EQ(remote.ordered, item.second);
    ASSERT_EQ(Field(remote.profile, "remote_coordinator_fallbacks"), 0);
  }
  RegisterRdfIdentity(9005, RdfIdentity(TripleTermType::URI, Iri(9005)));
  for (const auto& item : std::vector<std::pair<uint64_t, std::string>>{{61000, "INF"}, {61001, "-INF"}}) {
    RegisterRdfIdentity(item.first, RdfIdentity(TripleTermType::LITERAL, item.second, xsd + "double"));
    ASSERT_OK(RdfYBModel::Instance().WriteTripleToDocDB(101, 9005, item.first));
  }
  ASSERT_OK(RdfYBModel::Instance().Flush());
  for (bool negate : {false, true}) {
    const std::string query = "SELECT (COUNT(*) AS ?n) (SUM(?v) AS ?t) "
        "WHERE { ?s <urn:p6:9005> ?v } HAVING (" +
        std::string(negate ? "!(SUM(?v) >= 0)" : "SUM(?v) >= 0") + ")";
    const auto local = Query(query, {}); const auto remote = Query(query, workers_);
    ASSERT_EQ(local.status, 0); ASSERT_EQ(remote.status, 0);
    ASSERT_EQ(remote.ordered, negate ? (std::vector<std::string>{"2", "NaN"}) : std::vector<std::string>{});
    ASSERT_EQ(remote.ordered, local.ordered);
    ASSERT_EQ(Field(remote.profile, "remote_coordinator_fallbacks"), 0);
  }
  auto fallback = workers_; fallback.binary = "/bin/false";
  for (const auto& expr : {"SUM(?v + 1)", "AVG(COUNT(?v))", "GROUP_CONCAT(?v)"}) {
    const std::string query = "SELECT (" + std::string(expr) + " AS ?t) WHERE { ?s <urn:p6:9004> ?v }";
    const auto local = Query(query, {});
    const auto remote = Query(query, fallback);
    ASSERT_EQ(remote.status, local.status); ASSERT_EQ(remote.ordered, local.ordered);
    ASSERT_EQ(Field(remote.profile, "remote_fragments"), 0);
    ASSERT_EQ(Field(remote.profile, "remote_coordinator_fallbacks"), 1);
  }
}

TEST_F(MdbFragmentIntegrationTest, GlobalNumericBudgetFailureAndRecovery) {
  SeedNumericGroups();
  const std::string query = "SELECT ?g (SUM(DISTINCT ?v) AS ?t) (AVG(DISTINCT ?v) AS ?a) "
      "(MIN(?v) AS ?lo) (MAX(?v) AS ?hi) WHERE { ?s <urn:p6:9000> ?k . "
      "?k <urn:p6:9001> ?g . ?s <urn:p6:9002> ?v } GROUP BY ?g "
      "HAVING (SUM(DISTINCT ?v) >= 8) ORDER BY DESC(?t) ?g LIMIT 1";
  const auto success = Query(query, workers_);
  ASSERT_EQ(success.status, 0);
  ASSERT_EQ(success.ordered, (std::vector<std::string>{Iri(1001), "8", "2.66666666666666667", "-4", "10"}));
  // Eight parsed variables, one private aggregate, two sort keys, three
  // HAVING nodes, four SELECT nodes, one extra value-aggregate reservation.
  const uint64_t per_row = 1024 + 128 * 9 + 16 * 2 + 64 * 3 + 512 * 4 + 512;
  ASSERT_EQ(Field(success.profile, "global_reserved_bytes"), per_row * 50);
  auto limited = workers_; limited.coordinator_memory_bytes = per_row * 50;
  ASSERT_EQ(Query(query, limited).ordered, success.ordered);
  --limited.coordinator_memory_bytes;
  const auto exhausted = Query(query, limited);
  ASSERT_NE(exhausted.status, 0); ASSERT_TRUE(exhausted.ordered.empty());
  ASSERT_EQ(Field(exhausted.profile, "remote_coordinator_fallbacks"), 0);
  auto broken = workers_; broken.binary = "/bin/false";
  const auto failed = Query(query, broken);
  ASSERT_NE(failed.status, 0); ASSERT_TRUE(failed.ordered.empty());
  ASSERT_EQ(Field(failed.profile, "remote_coordinator_fallbacks"), 0);
  for (bool expire : {false, true}) {
    struct Stop { bool expire; size_t cells = 0; } stop{expire};
    auto sink = [](const char*, const char*, void* arg) {
      auto& state = *static_cast<Stop*>(arg); ++state.cells;
      auto* context = DocDBReadContextScope::Current();
      if (state.expire) context->deadline = CoarseMonoClock::Now();
      else context->cancelled.store(true);
    };
    RdfYBModel::Instance().SetWorkerOptions(workers_);
    ASSERT_NE(YBCMdbExecuteSparql(query.c_str(), sink, &stop), 0);
    ASSERT_EQ(stop.cells, 5);
  }
  const auto recovered = Query(query, workers_);
  ASSERT_EQ(recovered.status, 0); ASSERT_EQ(recovered.ordered, success.ordered);
}

TEST_F(MdbFragmentIntegrationTest, P8HashBroadcastAndCompositeJoin) {
  Seed();
  const auto root = GetTestPath("p8-spill"); std::filesystem::create_directory(root);
  const auto local = Query(kQuery, {});
  ASSERT_EQ(local.status, 0);
  for (auto mode : {ExchangeMode::Hash, ExchangeMode::Broadcast}) {
    auto options = workers_; options.exchange = mode;
    options.exchange_memory_bytes = 65536; options.exchange_spill_directory = root;
    const auto remote = Query(kQuery, options);
    ASSERT_EQ(remote.status, 0) << remote.profile;
    ASSERT_EQ(remote.values, local.values); ASSERT_EQ(remote.ordered.size(), 64);
    ASSERT_EQ(Field(remote.profile, "remote_fragments"), 8);
    ASSERT_EQ(Field(remote.profile, "remote_coordinator_fallbacks"), 0);
    ASSERT_EQ(Field(remote.profile, "exchange_operators"), 1);
    ASSERT_EQ(Field(remote.profile, "exchange_left_rows"), 32);
    ASSERT_EQ(Field(remote.profile, "exchange_right_rows"), 2);
    ASSERT_EQ(Field(remote.profile, "exchange_output_rows"), 64);
    ASSERT_LE(Field(remote.profile, "exchange_peak_reserved_bytes"), options.exchange_memory_bytes);
    ASSERT_GT(Field(remote.profile, "exchange_spill_bytes"), 0);
    ASSERT_EQ(Field(remote.profile, "exchange_live_files"), 0);
    ASSERT_TRUE(std::filesystem::is_empty(root));
    const auto ordered = Query(std::string(kQuery) + " ORDER BY DESC(?o) LIMIT 3 OFFSET 1", options);
    ASSERT_EQ(ordered.status, 0);
    ASSERT_EQ(ordered.ordered, (std::vector<std::string>{Iri(1002), Iri(1002), Iri(1002)}));
    const auto empty = Query("SELECT ?o WHERE { ?s <urn:p6:missing> ?k . ?k <urn:p6:9001> ?o }", options);
    ASSERT_EQ(empty.status, 0); ASSERT_TRUE(empty.ordered.empty());
    // Two shared keys (?s, ?k), with hidden predicate columns on both sides.
    const std::string composite = "SELECT ?s ?k WHERE { ?s ?p ?k . ?s ?q ?k } ORDER BY ?s ?k";
    const auto expected = Query(composite, {}); const auto joined = Query(composite, options);
    ASSERT_EQ(expected.status, 0); ASSERT_EQ(joined.status, 0);
    ASSERT_EQ(joined.ordered, expected.ordered);
    ASSERT_EQ(Field(joined.profile, "remote_coordinator_fallbacks"), 0);
    auto unreachable = options; unreachable.binary = "/bin/false";
    const auto fallback = Query("SELECT ?s WHERE { ?s <urn:p6:9000> ?k }", unreachable);
    ASSERT_EQ(fallback.status, 0);
    ASSERT_EQ(Field(fallback.profile, "remote_fragments"), 0);
    ASSERT_EQ(Field(fallback.profile, "remote_coordinator_fallbacks"), 1);
    ASSERT_TRUE(std::filesystem::is_empty(root));
  }
}

TEST_F(MdbFragmentIntegrationTest, P8HotKeySpillsAndKeepsBag) {
  for (uint64_t s = 101; s < 109; ++s) Write(s, 9000, 1);
  for (uint64_t o = 10000; o < 11500; ++o) Write(1, 9001, o);
  ASSERT_OK(RdfYBModel::Instance().Flush());
  const auto root = GetTestPath("p8-hot"); std::filesystem::create_directory(root);
  for (auto mode : {ExchangeMode::Hash, ExchangeMode::Broadcast}) {
    auto options = workers_; options.exchange = mode; options.exchange_memory_bytes = 65536;
    options.exchange_spill_directory = root; options.batch_rows = 64;
    const auto count = Query("SELECT (COUNT(*) AS ?n) WHERE { ?s <urn:p6:9000> ?k . ?k <urn:p6:9001> ?o }", options);
    ASSERT_EQ(count.status, 0) << count.profile;
    ASSERT_EQ(count.ordered, (std::vector<std::string>{"12000"}));
    ASSERT_GT(Field(count.profile, "exchange_chunked_passes"), 1);
    if (mode == ExchangeMode::Hash) ASSERT_EQ(Field(count.profile, "exchange_skew_partitions"), 1);
    ASSERT_EQ(Field(count.profile, "exchange_output_rows"), 12000);
    ASSERT_EQ(Field(count.profile, "remote_rows"), 1508);
    ASSERT_EQ(Field(count.profile, "global_input_rows"), 12000);
    ASSERT_LE(Field(count.profile, "exchange_peak_reserved_bytes"), options.exchange_memory_bytes);
    ASSERT_EQ(Field(count.profile, "exchange_cleanup_failures"), 0);
    ASSERT_TRUE(std::filesystem::is_empty(root));
  }
}

TEST_F(MdbFragmentIntegrationTest, P8QuotaWorkerCancellationAndRecovery) {
  Seed(8);
  const auto root = GetTestPath("p8-failure"); std::filesystem::create_directory(root);
  auto options = workers_; options.exchange = ExchangeMode::Hash; options.exchange_spill_directory = root;
  auto limited = options; limited.exchange_spill_bytes = 140;
  const auto exhausted = Query(kQuery, limited);
  ASSERT_NE(exhausted.status, 0); ASSERT_TRUE(exhausted.ordered.empty());
  ASSERT_EQ(Field(exhausted.profile, "remote_coordinator_fallbacks"), 0);
  ASSERT_TRUE(std::filesystem::is_empty(root));
  auto broken = options; broken.binary = "/bin/false";
  const auto failed = Query(kQuery, broken);
  ASSERT_NE(failed.status, 0); ASSERT_TRUE(failed.ordered.empty());
  ASSERT_EQ(Field(failed.profile, "remote_coordinator_fallbacks"), 0);
  ASSERT_TRUE(std::filesystem::is_empty(root));
  for (bool expire : {false, true}) {
    struct Stop { bool expire; size_t cells = 0; } stop{expire};
    auto sink = [](const char*, const char*, void* arg) {
      auto& state = *static_cast<Stop*>(arg); ++state.cells;
      auto* context = DocDBReadContextScope::Current();
      if (state.expire) context->deadline = CoarseMonoClock::Now(); else context->cancelled.store(true);
    };
    RdfYBModel::Instance().SetWorkerOptions(options);
    ASSERT_NE(YBCMdbExecuteSparql(kQuery, sink, &stop), 0); ASSERT_EQ(stop.cells, 1);
    ASSERT_EQ(Field(YBCMdbGetLastQueryProfile(), "exchange_live_files"), 0);
    ASSERT_TRUE(std::filesystem::is_empty(root));
  }
  auto invalid = options; invalid.exchange_memory_bytes = 1;
  const auto rejected = Query(kQuery, invalid);
  ASSERT_NE(rejected.status, 0); ASSERT_EQ(Field(rejected.profile, "remote_fragments"), 0);
  const auto recovered = Query(kQuery, options);
  ASSERT_EQ(recovered.status, 0); ASSERT_EQ(recovered.ordered.size(), 16);
  ASSERT_EQ(Field(recovered.profile, "exchange_live_files"), 0);
  ASSERT_TRUE(std::filesystem::is_empty(root));
}

TEST_F(MdbFragmentIntegrationTest, P8InputsShareOneReadView) {
  Seed(8);
  RegisterRdfIdentity(1003, RdfIdentity(TripleTermType::URI, Iri(1003)));
  const auto root = GetTestPath("p8-snapshot"); std::filesystem::create_directory(root);
  auto options = workers_; options.exchange = ExchangeMode::Hash; options.exchange_spill_directory = root;
  auto& model = RdfYBModel::Instance();
  auto result = model.NewQueryReadContext(); ASSERT_OK(result);
  auto context = std::move(*result);
  FragmentPlan plan;
  plan.physical.set_master_addresses(client_->GetMasterLeaderAddress().ToString());
  plan.physical.set_keyspace("mdb_p6_test"); plan.physical.set_variable_count(3);
  plan.physical.set_batch_rows(options.batch_rows); plan.physical.set_page_rows(options.page_rows);
  plan.physical.set_memory_bytes(options.memory_bytes);
  for (int perm = 0; perm < 6; ++perm) plan.physical.add_table_ids(model.GetTable(perm)->table()->id());
  for (uint32_t side = 0; side < 2; ++side) {
    auto* triple = plan.physical.add_triples();
    triple->add_terms()->set_variable(side); triple->add_terms()->set_constant(9000 + side);
    triple->add_terms()->set_variable(side + 1);
  }
  plan.physical.add_projection(0); plan.physical.add_projection(2);
  TabletRouter router; ASSERT_OK(router.Init(client_.get(), "mdb_p6_test")); plan.tasks = router.GetTablets(0);
  std::sort(plan.tasks.begin(), plan.tasks.end(), [](const auto& a, const auto& b) { return a.partition_start < b.partition_start; });
  size_t outputs = 0;
  ExchangeControl control{&context->cancelled, context->deadline, &context->stats, &context->deadline};
  ASSERT_OK(ExchangeRows(plan.physical, options, control,
      [&](size_t side, const FragmentRowSink& sink) -> Status {
        FragmentPlan child = plan;
        child.physical.clear_triples(); *child.physical.add_triples() = plan.physical.triples(side);
        child.physical.clear_projection(); child.physical.add_projection(side); child.physical.add_projection(side + 1);
        RETURN_NOT_OK(GatherFragments(child, options, *context, sink));
        if (!side) {
          RETURN_NOT_OK(model.WriteTripleToDocDB(1, 9001, 1003));
          RETURN_NOT_OK(model.Flush());
        }
        return Status::OK();
      }, [&](const std::vector<uint64_t>& row) -> Status {
        if (row[1] == 1003) return STATUS(Corruption, "Exchange mixed read views");
        ++outputs; return Status::OK();
      }));
  ASSERT_EQ(outputs, 16); ASSERT_EQ(context->stats.exchange_right_rows, 2);
  ASSERT_TRUE(std::filesystem::is_empty(root));
  ASSERT_EQ(Query(kQuery, {}).ordered.size(), 24);
}

TEST_F(MdbFragmentIntegrationTest, GlobalTypedIdentityAndValueOrdering) {
  RegisterRdfIdentity(500, RdfIdentity(TripleTermType::URI, Iri(500)));
  RegisterRdfIdentity(9003, RdfIdentity(TripleTermType::URI, Iri(9003)));
  const std::string xsd = "http://www.w3.org/2001/XMLSchema#";
  const std::vector<SPARQL::RdfTermIdentity> terms = {
    RdfIdentity(TripleTermType::URI, "same"),
    RdfIdentity(TripleTermType::LITERAL, "same"),
    RdfIdentity(TripleTermType::LITERAL, "same", "@en"),
    RdfIdentity(TripleTermType::LITERAL, "same", "@fr"),
    RdfIdentity(TripleTermType::LITERAL, "10", xsd + "integer"),
    RdfIdentity(TripleTermType::LITERAL, "2", xsd + "integer"),
    RdfIdentity(TripleTermType::BLANK, "b", "", "scope-a"),
    RdfIdentity(TripleTermType::BLANK, "b", "", "scope-b")
  };
  for (size_t i = 0; i < terms.size(); ++i) {
    RegisterRdfIdentity(600 + i, terms[i]);
    ASSERT_OK(RdfYBModel::Instance().WriteTripleToDocDB(500, 9003, 600 + i));
  }
  ASSERT_OK(RdfYBModel::Instance().Flush());
  const auto count = Query("SELECT (COUNT(DISTINCT ?o) AS ?n) WHERE { "
      "?s <urn:p6:9003> ?o }", workers_);
  ASSERT_EQ(count.status, 0);
  ASSERT_EQ(count.values, (std::multiset<std::string>{"8"}));
  const std::string query = "SELECT DISTINCT ?o WHERE { ?s <urn:p6:9003> ?o } ORDER BY ?o";
  const auto local = Query(query, {});
  const auto remote = Query(query, workers_);
  ASSERT_EQ(local.status, 0);
  ASSERT_EQ(remote.status, 0);
  ASSERT_EQ(remote.ordered, local.ordered);
  ASSERT_EQ(remote.ordered.size(), 8);
  const auto two = std::find(remote.ordered.begin(), remote.ordered.end(), "2");
  const auto ten = std::find(remote.ordered.begin(), remote.ordered.end(), "10");
  ASSERT_NE(two, remote.ordered.end());
  ASSERT_NE(ten, remote.ordered.end());
  ASSERT_LT(two, ten);
  ASSERT_EQ(Field(remote.profile, "remote_coordinator_fallbacks"), 0);
}

TEST_F(MdbFragmentIntegrationTest, GlobalFailureBudgetAndRecovery) {
  Seed();
  const std::string query = "SELECT DISTINCT ?o WHERE { ?s <urn:p6:9000> ?k . "
      "?k <urn:p6:9001> ?o } ORDER BY ?o";
  auto limited = workers_;
  limited.coordinator_memory_bytes = 1;
  const auto exhausted = Query(query, limited);
  ASSERT_NE(exhausted.status, 0);
  ASSERT_TRUE(exhausted.values.empty());
  ASSERT_EQ(Field(exhausted.profile, "remote_coordinator_fallbacks"), 0);
  auto broken = workers_;
  broken.binary = "/bin/false";
  const auto failed = Query(query, broken);
  ASSERT_NE(failed.status, 0);
  ASSERT_TRUE(failed.values.empty());
  ASSERT_EQ(Field(failed.profile, "remote_coordinator_fallbacks"), 0);
  const auto recovered = Query(query, workers_);
  ASSERT_EQ(recovered.status, 0);
  ASSERT_EQ(recovered.ordered, (std::vector<std::string>{Iri(1001), Iri(1002)}));
  const std::string tuple_counts = "SELECT (COUNT(DISTINCT *) AS ?a) "
      "(COUNT(DISTINCT *) AS ?b) WHERE { ?s <urn:p6:9000> ?k . ?k <urn:p6:9001> ?o }";
  const auto counted = Query(tuple_counts, workers_);
  ASSERT_EQ(counted.status, 0);
  ASSERT_EQ(counted.ordered, (std::vector<std::string>{"64", "64"}));
  auto tuples_limited = workers_;
  // Five query variables, two full-tuple sets: 1824 reserved bytes per row.
  // This budget fits input without the tuple-set charge, but not with it.
  tuples_limited.coordinator_memory_bytes = 110000;
  const auto tuple_exhausted = Query(tuple_counts, tuples_limited);
  ASSERT_NE(tuple_exhausted.status, 0);
  ASSERT_TRUE(tuple_exhausted.values.empty());
  for (bool expire : {false, true}) {
    struct Stop { bool expire; size_t cells = 0; } stop{expire};
    auto sink = [](const char*, const char*, void* arg) {
      auto& state = *static_cast<Stop*>(arg);
      ++state.cells;
      auto* context = DocDBReadContextScope::Current();
      if (state.expire) context->deadline = CoarseMonoClock::Now();
      else context->cancelled.store(true);
    };
    RdfYBModel::Instance().SetWorkerOptions(workers_);
    ASSERT_NE(YBCMdbExecuteSparql(query.c_str(), sink, &stop), 0);
    ASSERT_EQ(stop.cells, 1);
  }
}

TEST_F(MdbFragmentIntegrationTest, GlobalUnsupportedEnvelopeFallsBackBeforeLaunch) {
  Seed();
  auto unreachable = workers_;
  unreachable.binary = "/bin/false";
  for (const std::string query : {
      "SELECT ?o WHERE { { ?s <urn:p6:9000> ?o } UNION { ?s <urn:p6:9001> ?o } }",
      "SELECT ?s WHERE { ?s <urn:p6:9000> ?k OPTIONAL { ?k <urn:p6:9001> ?o } }",
      "SELECT (SUM(STR(?s)) AS ?n) WHERE { ?s <urn:p6:9000> ?k }",
      "SELECT (COUNT(*) AS ?n) WHERE { ?s <urn:p6:9000> ?k } GROUP BY ?k ORDER BY ?s",
      "SELECT (COUNT(*) AS ?n) WHERE { ?s <urn:p6:9000> ?k } GROUP BY ?k HAVING (SUM(STR(?s)) > 1)",
      "SELECT (COUNT(*) AS ?n) WHERE { ?s <urn:p6:9000> ?k } GROUP BY ?k HAVING (?n > 1)",
      "SELECT (COUNT(*) AS ?n) WHERE { ?s <urn:p6:9000> ?k } GROUP BY ?k HAVING (COUNT(*) + 1 > 1)",
      "SELECT (COUNT(*) AS ?n) WHERE { ?s <urn:p6:9000> ?k } GROUP BY ?k HAVING (COUNT(STR(?s)) > 1)",
      "SELECT (COUNT(*) AS ?n) WHERE { ?s <urn:p6:9000> ?k } GROUP BY ?k HAVING (COUNT(*) > ?k)",
      "SELECT ?s WHERE { ?s <urn:p6:9000> ?k } ORDER BY STR(?s)"}) {
    const auto remote = Query(query, unreachable);
    const auto local = Query(query, {});
    ASSERT_EQ(remote.status, local.status) << query;
    ASSERT_EQ(remote.values, local.values) << query;
    ASSERT_EQ(Field(remote.profile, "remote_fragments"), 0);
    ASSERT_EQ(Field(remote.profile, "remote_coordinator_fallbacks"), 1);
  }
  std::string excessive_keys = "SELECT (COUNT(*) AS ?n) WHERE { ?s <urn:p6:9000> ?k } ORDER BY";
  const auto alias_scope = Query("SELECT (COUNT(*) AS ?n) WHERE { ?s <urn:p6:9000> ?k } "
      "GROUP BY ?k HAVING (?n > 1)", unreachable);
  ASSERT_NE(alias_scope.status, 0);
  ASSERT_TRUE(alias_scope.ordered.empty());
  ASSERT_EQ(Field(alias_scope.profile, "remote_fragments"), 0);
  ASSERT_EQ(Field(alias_scope.profile, "remote_coordinator_fallbacks"), 1);
  for (size_t i = 0; i < 129; ++i) excessive_keys += " ?n";
  const auto excessive = Query(excessive_keys, unreachable);
  ASSERT_EQ(excessive.status, 0);
  ASSERT_EQ(excessive.ordered, (std::vector<std::string>{"32"}));
  ASSERT_EQ(Field(excessive.profile, "remote_fragments"), 0);
  ASSERT_EQ(Field(excessive.profile, "remote_coordinator_fallbacks"), 1);
  const std::string prefix = "SELECT (COUNT(*) AS ?n) WHERE { ?s <urn:p6:9000> ?k } ";
  std::string many_aggregates = prefix + "HAVING ";
  for (size_t i = 0; i < 17; ++i) many_aggregates += "(COUNT(*) > 0) ";
  std::string deep = "COUNT(*) > 0";
  for (size_t i = 0; i < 18; ++i) deep = "!(" + deep + ")";
  std::string many_nodes = prefix + "HAVING ";
  for (size_t i = 0; i < 130; ++i) many_nodes += "(true) ";
  for (const auto& query : {many_aggregates, prefix + "HAVING (" + deep + ")", many_nodes}) {
    const auto fallback = Query(query, unreachable);
    ASSERT_EQ(fallback.status, 0) << query;
    ASSERT_EQ(fallback.ordered, (std::vector<std::string>{"32"})) << query;
    ASSERT_EQ(Field(fallback.profile, "remote_fragments"), 0);
    ASSERT_EQ(Field(fallback.profile, "remote_coordinator_fallbacks"), 1);
  }
}

TEST_F(MdbFragmentIntegrationTest, SharedChildSnapshotAndSlowConsumer) {
  Seed();
  RegisterRdfIdentity(1003, RdfIdentity(TripleTermType::URI, Iri(1003)));
  auto& model = RdfYBModel::Instance();
  model.SetWorkerOptions(workers_);
  struct State { size_t rows = 0; } state;
  auto callback = [](const char*, const char*, void* arg) {
    auto& state = *static_cast<State*>(arg);
    if (state.rows++ == 0) {
      auto& model = RdfYBModel::Instance();
      ASSERT_OK(model.BeginTransaction());
      ASSERT_OK(model.WriteTripleToDocDB(1, 9001, 1003));
      ASSERT_OK(model.CommitTransaction());
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  };
  ASSERT_EQ(YBCMdbExecuteSparql(kQuery, callback, &state), 0);
  ASSERT_EQ(state.rows, 64);
  const auto profile = std::string(YBCMdbGetLastQueryProfile());
  ASSERT_LE(Field(profile, "remote_peak_frame_bytes"), 1024 * 1024);
  const auto fresh = Query(kQuery, workers_);
  ASSERT_EQ(fresh.status, 0);
  ASSERT_EQ(fresh.values.size(), 96);
}

TEST_F(MdbFragmentIntegrationTest, WorkerFailureCancellationDeadlineBudgetAndRecovery) {
  Seed(128);
  auto broken = workers_; broken.binary = "/bin/false";
  EXPECT_NE(Query(kQuery, broken).status, 0);
  auto tiny = workers_; tiny.memory_bytes = 4096;
  EXPECT_NE(Query(kQuery, tiny).status, 0);
  auto& model = RdfYBModel::Instance();
  model.SetWorkerOptions(workers_);
  int callbacks = 0;
  auto cancel = [](const char*, const char*, void* arg) {
    ++*static_cast<int*>(arg);
    DocDBReadContextScope::Current()->cancelled = true;
  };
  EXPECT_NE(YBCMdbExecuteSparql(kQuery, cancel, &callbacks), 0);
  ASSERT_EQ(callbacks, 1);
  ASSERT_EQ(DocDBReadContextScope::Current(), nullptr);
  callbacks = 0;
  auto expire = [](const char*, const char*, void* arg) {
    ++*static_cast<int*>(arg);
    DocDBReadContextScope::Current()->deadline = CoarseMonoClock::Now();
  };
  EXPECT_NE(YBCMdbExecuteSparql(kQuery, expire, &callbacks), 0);
  ASSERT_EQ(callbacks, 1);
  callbacks = 0;
  auto kill_worker = [](const char*, const char*, void* arg) {
    if ((*static_cast<int*>(arg))++ == 0) {
      const auto& pids = DocDBReadContextScope::Current()->stats.remote_worker_pids;
      ASSERT_FALSE(pids.empty());
      ASSERT_EQ(kill(static_cast<pid_t>(std::stoll(pids)), SIGKILL), 0);
    }
  };
  EXPECT_NE(YBCMdbExecuteSparql(kQuery, kill_worker, &callbacks), 0);
  const auto recovered = Query(kQuery, workers_);
  ASSERT_EQ(recovered.status, 0);
  ASSERT_EQ(recovered.values.size(), 256);
}

TEST_F(MdbFragmentIntegrationTest, AuthenticatedSshTransportAndHostKeyRejection) {
  Seed(8);
  const auto host_key = GetTestPath("ssh-host-key");
  const auto user_key = GetTestPath("ssh-user-key");
  for (const auto& key : {host_key, user_key})
    ASSERT_OK(Subprocess::Call({"/usr/bin/ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", key}));
  const auto* user = getpwuid(geteuid());
  ASSERT_NE(user, nullptr);
  const auto port = AllocateFreePort();
  const auto server_config = GetTestPath("sshd.conf");
  {
    std::ofstream config(server_config);
    config << "ListenAddress 127.0.0.1\nPort " << port
           << "\nHostKey " << host_key << "\nPidFile " << GetTestPath("sshd.pid")
           << "\nAuthorizedKeysFile " << user_key << ".pub\nStrictModes no\nUsePAM no\n"
              "PubkeyAuthentication yes\nPasswordAuthentication no\nKbdInteractiveAuthentication no\n"
              "AuthenticationMethods publickey\nAllowUsers " << user->pw_name << "\nLogLevel ERROR\n";
  }
  // The temporary sshd uses test-only keys and loopback; no system SSH files change.
  const auto* override_sshd = std::getenv("MDB_TEST_SSHD");
  const std::string sshd_binary = override_sshd ? override_sshd : "/usr/sbin/sshd";
  Subprocess sshd(sshd_binary, {sshd_binary, "-D", "-e", "-f", server_config});
  ASSERT_OK(sshd.Start());
  const auto known_hosts = GetTestPath("known-hosts");
  {
    std::ifstream pubkey(host_key + ".pub");
    std::string key; std::getline(pubkey, key);
    ASSERT_FALSE(key.empty());
    std::ofstream file(known_hosts); file << "p6-test " << key << '\n';
  }
  const auto client_config = GetTestPath("ssh.conf");
  auto write_config = [&](const std::string& path, const std::string& hosts) {
    std::ofstream config(path);
    config << "Host p6-test-a p6-test-b p6-test-c\n  HostName 127.0.0.1\n  Port " << port
           << "\n  User " << user->pw_name << "\n  IdentityFile " << user_key
           << "\n  IdentitiesOnly yes\n  UserKnownHostsFile " << hosts
           << "\n  HostKeyAlias p6-test\n";
  };
  write_config(client_config, known_hosts);
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(Subprocess::Call({"/usr/bin/ssh", "-F", client_config, "-o", "BatchMode=yes",
        "-o", "StrictHostKeyChecking=yes", "-o", "ConnectTimeout=1", "p6-test-a", "/bin/true"}));
  });
  auto options = workers_;
  options.ssh_config = client_config;
  options.targets = {"p6-test-a", "p6-test-b", "p6-test-c"};
  const auto result = Query(kQuery, options);
  ASSERT_EQ(result.status, 0) << result.profile;
  ASSERT_EQ(result.values.size(), 16);
  ASSERT_EQ(Field(result.profile, "remote_max_workers"), 3);
  const auto untrusted_hosts = GetTestPath("untrusted-hosts");
  { std::ofstream empty(untrusted_hosts); }
  const auto bad_config = GetTestPath("untrusted-ssh.conf");
  write_config(bad_config, untrusted_hosts);
  options.ssh_config = bad_config;
  ASSERT_NE(Query(kQuery, options).status, 0);
}

TEST_F(MdbFragmentIntegrationTest, CliStagesCompleteRowsAndDiscardsFailedOutput) {
  const auto file_name = GetTestPath("cli.nt");
  {
    std::ofstream file(file_name);
    file << "<urn:cli:a> <urn:cli:p1> <urn:cli:k> .\n"
            "<urn:cli:b> <urn:cli:p1> <urn:cli:k> .\n"
            "<urn:cli:k> <urn:cli:p2> <urn:cli:o1> .\n"
            "<urn:cli:k> <urn:cli:p2> <urn:cli:o2> .\n";
  }
  std::string executable;
  ASSERT_OK(env_->GetExecutablePath(&executable));
  const auto cli = executable.substr(0, executable.find_last_of('/')) + "/../bin/mdb_cli";
  const std::vector<std::string> base = {cli, "--mdb_masters=" + cluster_->GetMasterAddresses(),
      "--mdb_keyspace=mdb_p6_test", "--mdb_streaming_reads=true"};
  auto import = base;
  import.insert(import.end(), {"import", file_name});
  ASSERT_OK(Subprocess::Call(import));
  const std::string query = "SELECT ?s ?o WHERE { ?s <urn:cli:p1> ?k . ?k <urn:cli:p2> ?o }";
  auto args = base;
  args.insert(args.end(), {"--mdb_worker_targets=local,local,local",
      "--mdb_worker_binary=" + workers_.binary, "--mdb_worker_batch_rows=1",
      "--mdb_worker_page_rows=1", "--mdb_query_profile=true", "query", query});
  std::string output, error;
  ASSERT_OK(Subprocess::Call(args, &output, &error));
  std::multiset<std::pair<std::string, std::string>> rows;
  std::istringstream lines(output);
  std::string line;
  std::vector<std::string> values;
  while (std::getline(lines, line)) {
    const auto tab = line.find('\t');
    if (tab == std::string::npos) continue;
    values.push_back(line.substr(tab + 1));
    if (values.size() == 2) { rows.emplace(values[0], values[1]); values.clear(); }
  }
  ASSERT_TRUE(values.empty()) << output;
  ASSERT_EQ(rows, (std::multiset<std::pair<std::string, std::string>>{
      {"urn:cli:a", "urn:cli:o1"}, {"urn:cli:a", "urn:cli:o2"},
      {"urn:cli:b", "urn:cli:o1"}, {"urn:cli:b", "urn:cli:o2"}})) << output << error;
  ASSERT_NE(error.find("\"remote_fragments\":4"), std::string::npos) << error;
  auto exchanged = args;
  exchanged.insert(exchanged.begin() + 1, {"--mdb_exchange=hash", "--mdb_exchange_memory_bytes=65536"});
  output.clear(); error.clear();
  ASSERT_OK(Subprocess::Call(exchanged, &output, &error));
  ASSERT_EQ(std::count(output.begin(), output.end(), '\t'), 8);
  ASSERT_NE(error.find("\"remote_fragments\":8"), std::string::npos) << error;
  ASSERT_NE(error.find("\"exchange_operators\":1"), std::string::npos) << error;
  exchanged.insert(exchanged.begin() + 1, "--mdb_exchange_spill_bytes=140");
  output.clear(); error.clear();
  ASSERT_NOK(Subprocess::Call(exchanged, &output, &error)); ASSERT_TRUE(output.empty());
  auto forced_hash = args;
  forced_hash.insert(forced_hash.begin() + 1, "--mdb_join_policy=hash");
  output.clear(); error.clear();
  ASSERT_OK(Subprocess::Call(forced_hash, &output, &error));
  ASSERT_EQ(std::count(output.begin(), output.end(), '\t'), 8) << output;
  ASSERT_NE(error.find("\"remote_fragments\":0"), std::string::npos) << error;
  ASSERT_NE(error.find("\"hash_join_operators\":1"), std::string::npos) << error;
  ASSERT_NE(error.find("\"remote_coordinator_fallbacks\":1"), std::string::npos) << error;
  args.insert(args.begin() + 1, "--mdb_worker_output_spool_bytes=1");
  output.clear(); error.clear();
  ASSERT_NOK(Subprocess::Call(args, &output, &error));
  ASSERT_TRUE(output.empty()) << output;
  ASSERT_NE(error.find("staging failed or exceeded"), std::string::npos) << error;
}


// Local diagnostic harness, not part of the upstream implementation.
TEST_F(MdbFragmentIntegrationTest, PeerQueryLatencyProbe) {
  ASSERT_NO_FATALS(Seed(32));
  auto& model = RdfYBModel::Instance();
  model.SetStreamingPageRows(16);
  std::multiset<std::string> expected;
  for (int i = 0; i < 32; ++i) {
    expected.insert(Iri(1001));
    expected.insert(Iri(1002));
  }
  auto remote = workers_;
  remote.batch_rows = 128;
  remote.page_rows = 16;
  auto exchange = remote;
  exchange.exchange = ExchangeMode::Hash;
  exchange.exchange_memory_bytes = 8 * 1024 * 1024;
  const auto spill_root = GetTestPath("peer-latency-spill");
  std::filesystem::create_directory(spill_root);
  exchange.exchange_spill_directory = spill_root;
  const std::vector<std::pair<std::string, WorkerOptions>> modes = {
      {"coordinator_streaming", WorkerOptions{}},
      {"local_fragment_processes", remote},
      {"local_exchange_hash", exchange}};
  // Separate each mode's first observation from repeated observations.
  // The timed region includes the complete adapter call and result collection;
  // fragment modes therefore include fresh child-process and client startup.
  // It excludes fixture/import/cluster startup and the CLI spool/replay layer.
  for (int sample = -1; sample < 5; ++sample) {
    for (size_t i = 0; i < modes.size(); ++i) {
      const size_t index = (i + static_cast<size_t>(sample + 1)) % modes.size();
      const auto& mode = modes[index];
      const auto begin = std::chrono::steady_clock::now();
      const auto output = Query(kQuery, mode.second);
      const auto elapsed = std::chrono::duration<double, std::milli>(
          std::chrono::steady_clock::now() - begin).count();
      ASSERT_EQ(output.status, 0) << output.profile;
      ASSERT_EQ(output.values, expected);
      ASSERT_EQ(output.ordered.size(), 64);
      if (!mode.second.targets.empty()) {
        ASSERT_EQ(Field(output.profile, "remote_coordinator_fallbacks"), 0);
        ASSERT_EQ(Field(output.profile, "remote_fragments"),
                  mode.second.exchange == ExchangeMode::None ? 4 : 8);
      }
      LOG(INFO) << "PEER_LATENCY mode=" << mode.first << " sample=" << sample
                << " elapsed_ms=" << elapsed << " rows=64 profile=" << output.profile;
    }
  }
  ASSERT_TRUE(std::filesystem::is_empty(spill_root));
}


// Synthetic query timing only; no product execution or default policy changes.
class PeerPerformanceScaleTest : public MdbFragmentIntegrationTest {
 protected:
  void RunScale(const std::string& shape, size_t count) {
    std::multiset<std::string> expected;
    uint64_t triples = 0;
    auto write = [&](uint64_t s, uint64_t p, uint64_t o) {
      ASSERT_NO_FATALS(Write(s, p, o));
      ++triples;
      if (triples % 256 == 0) ASSERT_OK(RdfYBModel::Instance().Flush());
    };
    const auto load_begin = std::chrono::steady_clock::now();
    for (size_t i = 0; i < count; ++i) {
      const uint64_t key = shape == "unique" ? 100000 + i :
                           shape == "shared" ? 100000 : 100000 + i % 64;
      ASSERT_NO_FATALS(write(100 + i, 9000, key));
      if (shape == "unique") {
        ASSERT_NO_FATALS(write(key, 9001, 200000 + i));
        expected.insert(Iri(200000 + i));
      } else if (shape == "shared") {
        expected.insert(Iri(200000));
        expected.insert(Iri(200001));
      } else {
        for (uint64_t j = 0; j < 16; ++j)
          expected.insert(Iri(200000 + (i % 64) * 16 + j));
      }
    }
    if (shape == "shared") {
      ASSERT_NO_FATALS(write(100000, 9001, 200000));
      ASSERT_NO_FATALS(write(100000, 9001, 200001));
    } else if (shape == "fanout") {
      for (uint64_t key = 0; key < 64; ++key)
        for (uint64_t j = 0; j < 16; ++j)
          ASSERT_NO_FATALS(write(100000 + key, 9001, 200000 + key * 16 + j));
    }
    ASSERT_OK(RdfYBModel::Instance().Flush());
    const auto load_ms = std::chrono::duration<double, std::milli>(
        std::chrono::steady_clock::now() - load_begin).count();
    auto& model = RdfYBModel::Instance();
    model.SetStreamingPageRows(16);
    auto remote = workers_;
    remote.batch_rows = 128;
    remote.page_rows = 16;
    auto serial = remote;
    serial.max_workers = 1;
    auto exchange = remote;
    exchange.exchange = ExchangeMode::Hash;
    const auto spill = GetTestPath("peer-scale-spill");
    std::filesystem::create_directory(spill);
    exchange.exchange_spill_directory = spill;
    exchange.exchange_memory_bytes = 8 * 1024 * 1024;
    const std::vector<std::pair<std::string, WorkerOptions>> modes = {
        {"coordinator", WorkerOptions{}}, {"worker1", serial},
        {"worker3", remote}, {"hash3", exchange}};
    LOG(INFO) << "PEER_SCALE_DATA shape=" << shape << " n=" << count
              << " triples=" << triples << " expected_rows=" << expected.size()
              << " load_ms=" << load_ms;
    // Timed calls include process/client startup and full materialization in the
    // common result sink. Fixture setup, loading and result checking are excluded.
    for (int sample = -1; sample < 5; ++sample) {
      for (size_t i = 0; i < modes.size(); ++i) {
        const auto& mode = modes[(i + static_cast<size_t>(sample + 1)) % modes.size()];
        const auto begin = std::chrono::steady_clock::now();
        const auto output = Query(kQuery, mode.second);
        const auto elapsed = std::chrono::duration<double, std::milli>(
            std::chrono::steady_clock::now() - begin).count();
        const bool correct = output.status == 0 && output.values == expected &&
                             output.ordered.size() == expected.size();
        LOG(INFO) << "PEER_SCALE shape=" << shape << " n=" << count
                  << " mode=" << mode.first << " sample=" << sample
                  << " elapsed_ms=" << elapsed << " rows=" << output.ordered.size()
                  << " status=" << output.status << " correct=" << correct
                  << " profile=" << output.profile;
        ASSERT_EQ(output.status, 0) << shape << " n=" << count << " mode=" << mode.first;
        ASSERT_TRUE(correct) << "Complete result multiset mismatch for " << shape
                             << " n=" << count << " mode=" << mode.first;
        if (!mode.second.targets.empty()) {
          ASSERT_EQ(Field(output.profile, "remote_coordinator_fallbacks"), 0);
          ASSERT_EQ(Field(output.profile, "remote_fragments"),
                    mode.second.exchange == ExchangeMode::None ? 4 : 8);
          ASSERT_EQ(Field(output.profile, "remote_max_workers"), mode.second.max_workers);
        }
        ASSERT_TRUE(std::filesystem::is_empty(spill));
      }
    }
  }
};

TEST_F(PeerPerformanceScaleTest, UniqueN128) { RunScale("unique", 128); }
TEST_F(PeerPerformanceScaleTest, UniqueN1024) { RunScale("unique", 1024); }
TEST_F(PeerPerformanceScaleTest, UniqueN4096) { RunScale("unique", 4096); }
TEST_F(PeerPerformanceScaleTest, SharedN128) { RunScale("shared", 128); }
TEST_F(PeerPerformanceScaleTest, SharedN1024) { RunScale("shared", 1024); }
TEST_F(PeerPerformanceScaleTest, SharedN4096) { RunScale("shared", 4096); }
TEST_F(PeerPerformanceScaleTest, FanoutN128) { RunScale("fanout", 128); }
TEST_F(PeerPerformanceScaleTest, FanoutN1024) { RunScale("fanout", 1024); }
TEST_F(PeerPerformanceScaleTest, FanoutN4096) { RunScale("fanout", 4096); }


// Acceptance against the dedicated external test cluster and independent VM.
class PeerCrossVmTest : public YBTest {
 protected:
  void SetUp() override {
    YBTest::SetUp();
    const auto* master = std::getenv("MDB_VM_MASTER");
    const auto* config = std::getenv("MDB_VM_SSH_CONFIG");
    ASSERT_NE(master, nullptr);
    ASSERT_NE(config, nullptr);
    master_ = master;
    ssh_config_ = config;
    keyspace_ = "mdb_vm_" + std::to_string(getpid()) + "_" + std::to_string(
        std::chrono::steady_clock::now().time_since_epoch().count());
    auto result = client::YBClientBuilder().add_master_server_addr(master_).Build();
    ASSERT_OK(result);
    client_ = std::move(*result);
    ASSERT_OK(client_->CreateNamespaceIfNotExists(keyspace_, YQL_DATABASE_CQL));
    for (int perm = 0; perm < 6; ++perm) {
      client::YBSchemaBuilder schema;
      ConfigureRdfTableSchema(&schema);
      client::TableHandle table;
      ASSERT_OK(table.Create(client::YBTableName(YQL_DATABASE_CQL, keyspace_,
          RdfYBModel::kTableNames[perm]), 4, client_.get(), &schema));
    }
    const auto seed = GetTestPath("cross-vm.nt");
    {
      std::ofstream file(seed);
      for (int i = 0; i < 128; ++i)
        file << "<urn:vm:s" << i << "> <urn:vm:p1> <urn:vm:k1> .\n";
      file << "<urn:vm:k1> <urn:vm:p2> <urn:vm:o1> .\n"
              "<urn:vm:k1> <urn:vm:p2> <urn:vm:o2> .\n"
              "<urn:vm:k2> <urn:vm:p2> <urn:vm:o1> .\n";
      ASSERT_TRUE(file.good());
    }
    std::string executable;
    ASSERT_OK(env_->GetExecutablePath(&executable));
    cli_ = executable.substr(0, executable.find_last_of('/')) + "/../bin/mdb_cli";
    std::string import_output, import_error;
    const auto imported = Subprocess::Call({cli_, "--mdb_masters=" + master_,
        "--mdb_keyspace=" + keyspace_, "import", seed}, &import_output, &import_error);
    ASSERT_TRUE(imported.ok()) << imported << " " << import_error;
    // Import ran in another process: this initialization restores its dictionary.
    ASSERT_EQ(YBCMdbInitCluster(master_.c_str(), keyspace_.c_str()), 0);
    RdfYBModel::Instance().SetUseStreamingReads(true);
    RdfYBModel::Instance().SetStreamingPageRows(16);
    remote_.targets = {"mdb-vm-worker"};
    remote_.binary = "/opt/mdb-peer-vm-20261026/bin/mdb_fragment_worker";
    remote_.ssh_config = ssh_config_;
    remote_.max_workers = 1;
    remote_.batch_rows = 16;
    remote_.page_rows = 16;
    for (int i = 0; i < 128; ++i) {
      expected_.insert("urn:vm:o1");
      expected_.insert("urn:vm:o2");
    }
    LOG(INFO) << "MDB_CROSS_VM_DATA keyspace=" << keyspace_
              << " triples=131 result_rows=256 tablets_per_index=4";
  }

  void TearDown() override {
    RdfYBModel::Instance().ClearAll();
    client_.reset();
    YBTest::TearDown();
  }

  struct Output {
    int status = -1;
    std::multiset<std::string> values;
    std::vector<std::string> ordered;
    std::string profile;
  };

  Output Query(const std::string& query, WorkerOptions options) {
    RdfYBModel::Instance().SetWorkerOptions(std::move(options));
    Output output;
    auto sink = [](const char*, const char* value, void* arg) {
      auto* result = static_cast<Output*>(arg);
      result->values.insert(value);
      result->ordered.emplace_back(value);
    };
    const auto begin = std::chrono::steady_clock::now();
    output.status = YBCMdbExecuteSparql(query.c_str(), sink, &output);
    output.profile = YBCMdbGetLastQueryProfile();
    LOG(INFO) << "MDB_CROSS_VM_QUERY elapsed_ms="
              << std::chrono::duration<double, std::milli>(
                   std::chrono::steady_clock::now() - begin).count()
              << " status=" << output.status << " rows=" << output.ordered.size()
              << " profile=" << output.profile;
    return output;
  }

  uint64_t Field(const std::string& profile, const std::string& name) {
    const auto prefix = "\"" + name + "\":";
    const auto start = profile.find(prefix);
    EXPECT_NE(start, std::string::npos);
    return start == std::string::npos ? 0 : std::stoull(profile.substr(start + prefix.size()));
  }

  void CheckRemote(const Output& output, uint64_t fragments = 4) {
    ASSERT_EQ(output.status, 0) << output.profile;
    ASSERT_EQ(Field(output.profile, "remote_coordinator_fallbacks"), 0);
    ASSERT_EQ(Field(output.profile, "remote_fragments"), fragments);
  }

  uint64_t GraphId(const std::string& iri) {
    for (const auto& [id, text] : g_gmap_to_uri) if (text == iri) return id;
    ADD_FAILURE() << "Missing persistent IRI " << iri;
    return 0;
  }

  std::vector<std::string> CliArgs(const std::string& query) {
    return {cli_, "--mdb_masters=" + master_, "--mdb_keyspace=" + keyspace_,
        "--mdb_streaming_reads=true", "--mdb_worker_targets=mdb-vm-worker",
        "--mdb_worker_binary=" + remote_.binary, "--mdb_worker_ssh_config=" + ssh_config_,
        "--mdb_worker_max_processes=1", "--mdb_worker_batch_rows=1",
        "--mdb_query_profile=true", "query", query};
  }

  const std::string query_ =
      "SELECT ?o WHERE { ?s <urn:vm:p1> ?k . ?k <urn:vm:p2> ?o }";
  std::unique_ptr<client::YBClient> client_;
  std::string master_, ssh_config_, keyspace_, cli_;
  WorkerOptions remote_;
  std::multiset<std::string> expected_;
};

TEST_F(PeerCrossVmTest, BagGlobalAndExchangeAcrossKernels) {
  const auto local = Query(query_, {});
  ASSERT_EQ(local.status, 0);
  ASSERT_EQ(local.values, expected_);
  const auto remote = Query(query_, remote_);
  ASSERT_NO_FATALS(CheckRemote(remote));
  ASSERT_EQ(remote.values, expected_);
  auto mixed = remote_;
  mixed.targets = {"local", "mdb-vm-worker"};
  mixed.max_workers = 2;
  const auto distributed = Query(query_, mixed);
  ASSERT_NO_FATALS(CheckRemote(distributed));
  ASSERT_EQ(distributed.values, expected_);
  ASSERT_EQ(Field(distributed.profile, "remote_max_workers"), 2);
  const auto distinct = Query("SELECT DISTINCT ?o WHERE { ?s <urn:vm:p1> ?k . "
      "?k <urn:vm:p2> ?o } ORDER BY DESC(?o) LIMIT 1 OFFSET 1", mixed);
  ASSERT_NO_FATALS(CheckRemote(distinct));
  ASSERT_EQ(distinct.ordered, (std::vector<std::string>{"urn:vm:o1"}));
  const auto count = Query("SELECT (COUNT(*) AS ?n) WHERE { ?s <urn:vm:p1> ?k . "
      "?k <urn:vm:p2> ?o }", mixed);
  ASSERT_NO_FATALS(CheckRemote(count));
  ASSERT_EQ(count.ordered, (std::vector<std::string>{"256"}));
  auto exchange = mixed;
  exchange.exchange = ExchangeMode::Hash;
  exchange.exchange_spill_directory = GetTestPath("vm-exchange");
  std::filesystem::create_directory(exchange.exchange_spill_directory);
  const auto hash = Query(query_, exchange);
  ASSERT_NO_FATALS(CheckRemote(hash, 8));
  ASSERT_EQ(hash.values, expected_);
  ASSERT_TRUE(std::filesystem::is_empty(exchange.exchange_spill_directory));
}

TEST_F(PeerCrossVmTest, SharedSnapshotDuringCommittedWrite) {
  remote_.batch_rows = 1;
  remote_.page_rows = 1;
  struct State {
    uint64_t subject, predicate, key;
    bool wrote = false;
    std::multiset<std::string> values;
  } state{GraphId("urn:vm:s127"), GraphId("urn:vm:p1"), GraphId("urn:vm:k2"), false, {}};
  ASSERT_NE(state.subject, 0);
  ASSERT_NE(state.predicate, 0);
  ASSERT_NE(state.key, 0);
  auto sink = [](const char*, const char* value, void* arg) {
    auto& state = *static_cast<State*>(arg);
    state.values.insert(value);
    if (!state.wrote) {
      state.wrote = true;
      auto& model = RdfYBModel::Instance();
      ASSERT_OK(model.BeginTransaction());
      ASSERT_OK(model.WriteTripleToDocDB(state.subject, state.predicate, state.key));
      ASSERT_OK(model.CommitTransaction());
    }
  };
  RdfYBModel::Instance().SetWorkerOptions(remote_);
  ASSERT_EQ(YBCMdbExecuteSparql(query_.c_str(), sink, &state), 0);
  ASSERT_TRUE(state.wrote);
  ASSERT_EQ(state.values, expected_);
  ASSERT_EQ(Field(YBCMdbGetLastQueryProfile(), "remote_fragments"), 4);
  auto after_expected = expected_;
  after_expected.insert("urn:vm:o1");
  const auto after = Query(query_, remote_);
  ASSERT_NO_FATALS(CheckRemote(after));
  ASSERT_EQ(after.values, after_expected);
}

TEST_F(PeerCrossVmTest, RemoteWorkerFailureAndCliRecovery) {
  remote_.batch_rows = 1;
  remote_.page_rows = 1;
  struct State { std::string config; size_t cells = 0; bool terminated = false; } state{ssh_config_};
  auto sink = [](const char*, const char*, void* arg) {
    auto& state = *static_cast<State*>(arg);
    if (state.cells++ == 0) {
      std::string output, error;
      const auto terminated = Subprocess::Call({"/usr/bin/ssh", "-F", state.config,
          "mdb-vm-worker", "/usr/local/bin/mdb-lab-kill-workers"}, &output, &error);
      ASSERT_TRUE(terminated.ok()) << terminated << " " << error;
      state.terminated = true;
      LOG(INFO) << "MDB_CROSS_VM_KILLED " << output;
    }
  };
  RdfYBModel::Instance().SetWorkerOptions(remote_);
  ASSERT_NE(YBCMdbExecuteSparql(query_.c_str(), sink, &state), 0);
  ASSERT_TRUE(state.terminated);
  ASSERT_LT(state.cells, expected_.size());
  ASSERT_EQ(DocDBReadContextScope::Current(), nullptr);
  const auto recovered = Query(query_, remote_);
  ASSERT_NO_FATALS(CheckRemote(recovered));
  ASSERT_EQ(recovered.values, expected_);
  auto args = CliArgs(query_);
  std::string output, error;
  const auto cli_success = Subprocess::Call(args, &output, &error);
  ASSERT_TRUE(cli_success.ok()) << cli_success << " " << error;
  ASSERT_EQ(std::count(output.begin(), output.end(), '\t'), expected_.size());
  ASSERT_NE(error.find("MDB_VM_WORKER host=mdb-worker-vm"), std::string::npos) << error;
  auto limited = args;
  limited.insert(limited.begin() + 1, "--mdb_worker_output_spool_bytes=1");
  output.clear(); error.clear();
  ASSERT_NOK(Subprocess::Call(limited, &output, &error));
  ASSERT_TRUE(output.empty());
  output.clear(); error.clear();
  const auto cli_recovered = Subprocess::Call(args, &output, &error);
  ASSERT_TRUE(cli_recovered.ok()) << cli_recovered << " " << error;
  ASSERT_EQ(std::count(output.begin(), output.end(), '\t'), expected_.size());
}


// Generated only from the independent synthetic graph oracle.
struct PeerGraphCase {
  std::string name, query;
  size_t width;
  std::vector<std::string> expected;
  bool ordered, exchange, empty_plan;
};
std::vector<PeerGraphCase> PeerGraphCases(const std::string& group) {
  std::vector<PeerGraphCase> cases;
  if (group == "Structures") cases.push_back({"two_hop_bag", "SELECT ?s ?o WHERE { ?s <urn:graph-test:knows> ?k . ?k <urn:graph-test:knows> ?o }", 2, {"urn:graph-test:n00\turn:graph-test:n02", "urn:graph-test:n00\turn:graph-test:n03", "urn:graph-test:n00\turn:graph-test:n00", "urn:graph-test:n00\turn:graph-test:n03", "urn:graph-test:n00\turn:graph-test:n04", "urn:graph-test:n01\turn:graph-test:n00", "urn:graph-test:n01\turn:graph-test:n03", "urn:graph-test:n01\turn:graph-test:n04", "urn:graph-test:n02\turn:graph-test:n01", "urn:graph-test:n02\turn:graph-test:n02", "urn:graph-test:n02\turn:graph-test:n03", "urn:graph-test:n02\turn:graph-test:n04", "urn:graph-test:n03\turn:graph-test:n04", "urn:graph-test:n03\turn:graph-test:n05", "urn:graph-test:n04\turn:graph-test:n04", "urn:graph-test:n04\turn:graph-test:n05", "urn:graph-test:n04\turn:graph-test:leaf", "urn:graph-test:n04\turn:graph-test:n06", "urn:graph-test:n05\turn:graph-test:n07", "urn:graph-test:n06\turn:graph-test:n08", "urn:graph-test:n07\turn:graph-test:n09", "urn:graph-test:n08\turn:graph-test:n10", "urn:graph-test:n09\turn:graph-test:n11", "urn:graph-test:n10\turn:graph-test:n12", "urn:graph-test:n11\turn:graph-test:n13", "urn:graph-test:n12\turn:graph-test:n14", "urn:graph-test:n13\turn:graph-test:n15", "urn:graph-test:n14\turn:graph-test:n16", "urn:graph-test:n15\turn:graph-test:n17", "urn:graph-test:n16\turn:graph-test:n18", "urn:graph-test:n17\turn:graph-test:n19", "urn:graph-test:n18\turn:graph-test:n20", "urn:graph-test:n19\turn:graph-test:n21", "urn:graph-test:n20\turn:graph-test:n22", "urn:graph-test:n21\turn:graph-test:n23", "urn:graph-test:n22\turn:graph-test:n24", "urn:graph-test:n23\turn:graph-test:n25", "urn:graph-test:n24\turn:graph-test:n26", "urn:graph-test:n25\turn:graph-test:n27", "urn:graph-test:n26\turn:graph-test:n28", "urn:graph-test:n27\turn:graph-test:n29", "urn:graph-test:n28\turn:graph-test:n30", "urn:graph-test:n29\turn:graph-test:n31", "urn:graph-test:n30\turn:graph-test:n00", "urn:graph-test:n31\turn:graph-test:n01", "urn:graph-test:n31\turn:graph-test:n02", "urn:graph-test:n31\turn:graph-test:n03"}, false, true, false});
  if (group == "Structures") cases.push_back({"three_hop_projection_order", "SELECT ?last ?s ?k WHERE { ?s <urn:graph-test:knows> ?k . ?k <urn:graph-test:knows> ?o . ?o <urn:graph-test:knows> ?last }", 3, {"urn:graph-test:n00\turn:graph-test:n00\turn:graph-test:n01", "urn:graph-test:n03\turn:graph-test:n00\turn:graph-test:n01", "urn:graph-test:n04\turn:graph-test:n00\turn:graph-test:n01", "urn:graph-test:n01\turn:graph-test:n00\turn:graph-test:n02", "urn:graph-test:n02\turn:graph-test:n00\turn:graph-test:n02", "urn:graph-test:n03\turn:graph-test:n00\turn:graph-test:n02", "urn:graph-test:n04\turn:graph-test:n00\turn:graph-test:n02", "urn:graph-test:n04\turn:graph-test:n00\turn:graph-test:n03", "urn:graph-test:n05\turn:graph-test:n00\turn:graph-test:n03", "urn:graph-test:n01\turn:graph-test:n01\turn:graph-test:n02", "urn:graph-test:n02\turn:graph-test:n01\turn:graph-test:n02", "urn:graph-test:n03\turn:graph-test:n01\turn:graph-test:n02", "urn:graph-test:n04\turn:graph-test:n01\turn:graph-test:n02", "urn:graph-test:n04\turn:graph-test:n01\turn:graph-test:n03", "urn:graph-test:n05\turn:graph-test:n01\turn:graph-test:n03", "urn:graph-test:n02\turn:graph-test:n02\turn:graph-test:n00", "urn:graph-test:n03\turn:graph-test:n02\turn:graph-test:n00", "urn:graph-test:n00\turn:graph-test:n02\turn:graph-test:n00", "urn:graph-test:n03\turn:graph-test:n02\turn:graph-test:n00", "urn:graph-test:n04\turn:graph-test:n02\turn:graph-test:n00", "urn:graph-test:n04\turn:graph-test:n02\turn:graph-test:n03", "urn:graph-test:n05\turn:graph-test:n02\turn:graph-test:n03", "urn:graph-test:n04\turn:graph-test:n03\turn:graph-test:n04", "urn:graph-test:n05\turn:graph-test:n03\turn:graph-test:n04", "urn:graph-test:leaf\turn:graph-test:n03\turn:graph-test:n04", "urn:graph-test:n06\turn:graph-test:n03\turn:graph-test:n04", "urn:graph-test:n04\turn:graph-test:n04\turn:graph-test:n04", "urn:graph-test:n05\turn:graph-test:n04\turn:graph-test:n04", "urn:graph-test:leaf\turn:graph-test:n04\turn:graph-test:n04", "urn:graph-test:n06\turn:graph-test:n04\turn:graph-test:n04", "urn:graph-test:n07\turn:graph-test:n04\turn:graph-test:n05", "urn:graph-test:n08\turn:graph-test:n05\turn:graph-test:n06", "urn:graph-test:n09\turn:graph-test:n06\turn:graph-test:n07", "urn:graph-test:n10\turn:graph-test:n07\turn:graph-test:n08", "urn:graph-test:n11\turn:graph-test:n08\turn:graph-test:n09", "urn:graph-test:n12\turn:graph-test:n09\turn:graph-test:n10", "urn:graph-test:n13\turn:graph-test:n10\turn:graph-test:n11", "urn:graph-test:n14\turn:graph-test:n11\turn:graph-test:n12", "urn:graph-test:n15\turn:graph-test:n12\turn:graph-test:n13", "urn:graph-test:n16\turn:graph-test:n13\turn:graph-test:n14", "urn:graph-test:n17\turn:graph-test:n14\turn:graph-test:n15", "urn:graph-test:n18\turn:graph-test:n15\turn:graph-test:n16", "urn:graph-test:n19\turn:graph-test:n16\turn:graph-test:n17", "urn:graph-test:n20\turn:graph-test:n17\turn:graph-test:n18", "urn:graph-test:n21\turn:graph-test:n18\turn:graph-test:n19", "urn:graph-test:n22\turn:graph-test:n19\turn:graph-test:n20", "urn:graph-test:n23\turn:graph-test:n20\turn:graph-test:n21", "urn:graph-test:n24\turn:graph-test:n21\turn:graph-test:n22", "urn:graph-test:n25\turn:graph-test:n22\turn:graph-test:n23", "urn:graph-test:n26\turn:graph-test:n23\turn:graph-test:n24", "urn:graph-test:n27\turn:graph-test:n24\turn:graph-test:n25", "urn:graph-test:n28\turn:graph-test:n25\turn:graph-test:n26", "urn:graph-test:n29\turn:graph-test:n26\turn:graph-test:n27", "urn:graph-test:n30\turn:graph-test:n27\turn:graph-test:n28", "urn:graph-test:n31\turn:graph-test:n28\turn:graph-test:n29", "urn:graph-test:n00\turn:graph-test:n29\turn:graph-test:n30", "urn:graph-test:n01\turn:graph-test:n30\turn:graph-test:n31", "urn:graph-test:n02\turn:graph-test:n30\turn:graph-test:n31", "urn:graph-test:n03\turn:graph-test:n30\turn:graph-test:n31", "urn:graph-test:n02\turn:graph-test:n31\turn:graph-test:n00", "urn:graph-test:n03\turn:graph-test:n31\turn:graph-test:n00", "urn:graph-test:n00\turn:graph-test:n31\turn:graph-test:n00", "urn:graph-test:n03\turn:graph-test:n31\turn:graph-test:n00", "urn:graph-test:n04\turn:graph-test:n31\turn:graph-test:n00"}, false, false, false});
  if (group == "Structures") cases.push_back({"triangle_cycle", "SELECT ?a ?b ?c WHERE { ?a <urn:graph-test:knows> ?b . ?b <urn:graph-test:knows> ?c . ?c <urn:graph-test:knows> ?a }", 3, {"urn:graph-test:n00\turn:graph-test:n01\turn:graph-test:n02", "urn:graph-test:n01\turn:graph-test:n02\turn:graph-test:n00", "urn:graph-test:n02\turn:graph-test:n00\turn:graph-test:n01", "urn:graph-test:n04\turn:graph-test:n04\turn:graph-test:n04"}, false, false, false});
  if (group == "Structures") cases.push_back({"star_two_predicates", "SELECT ?s ?team ?topic WHERE { ?s <urn:graph-test:member> ?team . ?s <urn:graph-test:likes> ?topic }", 3, {"urn:graph-test:n00\turn:graph-test:team0\turn:graph-test:topic0", "urn:graph-test:n00\turn:graph-test:team0\turn:graph-test:topic1", "urn:graph-test:n01\turn:graph-test:team1\turn:graph-test:topic1", "urn:graph-test:n02\turn:graph-test:team2\turn:graph-test:topic2", "urn:graph-test:n03\turn:graph-test:team3\turn:graph-test:topic0", "urn:graph-test:n04\turn:graph-test:team0\turn:graph-test:topic1", "urn:graph-test:n05\turn:graph-test:team1\turn:graph-test:topic2", "urn:graph-test:n06\turn:graph-test:team2\turn:graph-test:topic0", "urn:graph-test:n07\turn:graph-test:team3\turn:graph-test:topic1", "urn:graph-test:n08\turn:graph-test:team0\turn:graph-test:topic2", "urn:graph-test:n09\turn:graph-test:team1\turn:graph-test:topic0", "urn:graph-test:n10\turn:graph-test:team2\turn:graph-test:topic1", "urn:graph-test:n11\turn:graph-test:team3\turn:graph-test:topic2", "urn:graph-test:n12\turn:graph-test:team0\turn:graph-test:topic0", "urn:graph-test:n13\turn:graph-test:team1\turn:graph-test:topic1", "urn:graph-test:n14\turn:graph-test:team2\turn:graph-test:topic2", "urn:graph-test:n15\turn:graph-test:team3\turn:graph-test:topic0", "urn:graph-test:n16\turn:graph-test:team0\turn:graph-test:topic1", "urn:graph-test:n17\turn:graph-test:team1\turn:graph-test:topic2", "urn:graph-test:n18\turn:graph-test:team2\turn:graph-test:topic0", "urn:graph-test:n19\turn:graph-test:team3\turn:graph-test:topic1", "urn:graph-test:n20\turn:graph-test:team0\turn:graph-test:topic2", "urn:graph-test:n21\turn:graph-test:team1\turn:graph-test:topic0", "urn:graph-test:n22\turn:graph-test:team2\turn:graph-test:topic1", "urn:graph-test:n23\turn:graph-test:team3\turn:graph-test:topic2", "urn:graph-test:n24\turn:graph-test:team0\turn:graph-test:topic0", "urn:graph-test:n25\turn:graph-test:team1\turn:graph-test:topic1", "urn:graph-test:n26\turn:graph-test:team2\turn:graph-test:topic2", "urn:graph-test:n27\turn:graph-test:team3\turn:graph-test:topic0", "urn:graph-test:n28\turn:graph-test:team0\turn:graph-test:topic1", "urn:graph-test:n29\turn:graph-test:team1\turn:graph-test:topic2", "urn:graph-test:n30\turn:graph-test:team2\turn:graph-test:topic0", "urn:graph-test:n31\turn:graph-test:team3\turn:graph-test:topic1"}, false, true, false});
  if (group == "Structures") cases.push_back({"team_to_city", "SELECT ?s ?city WHERE { ?s <urn:graph-test:member> ?team . ?team <urn:graph-test:located> ?city }", 2, {"urn:graph-test:n00\turn:graph-test:city0", "urn:graph-test:n01\turn:graph-test:city1", "urn:graph-test:n02\turn:graph-test:city0", "urn:graph-test:n03\turn:graph-test:city1", "urn:graph-test:n04\turn:graph-test:city0", "urn:graph-test:n05\turn:graph-test:city1", "urn:graph-test:n06\turn:graph-test:city0", "urn:graph-test:n07\turn:graph-test:city1", "urn:graph-test:n08\turn:graph-test:city0", "urn:graph-test:n09\turn:graph-test:city1", "urn:graph-test:n10\turn:graph-test:city0", "urn:graph-test:n11\turn:graph-test:city1", "urn:graph-test:n12\turn:graph-test:city0", "urn:graph-test:n13\turn:graph-test:city1", "urn:graph-test:n14\turn:graph-test:city0", "urn:graph-test:n15\turn:graph-test:city1", "urn:graph-test:n16\turn:graph-test:city0", "urn:graph-test:n17\turn:graph-test:city1", "urn:graph-test:n18\turn:graph-test:city0", "urn:graph-test:n19\turn:graph-test:city1", "urn:graph-test:n20\turn:graph-test:city0", "urn:graph-test:n21\turn:graph-test:city1", "urn:graph-test:n22\turn:graph-test:city0", "urn:graph-test:n23\turn:graph-test:city1", "urn:graph-test:n24\turn:graph-test:city0", "urn:graph-test:n25\turn:graph-test:city1", "urn:graph-test:n26\turn:graph-test:city0", "urn:graph-test:n27\turn:graph-test:city1", "urn:graph-test:n28\turn:graph-test:city0", "urn:graph-test:n29\turn:graph-test:city1", "urn:graph-test:n30\turn:graph-test:city0", "urn:graph-test:n31\turn:graph-test:city1"}, false, true, false});
  if (group == "Structures") cases.push_back({"reverse_object_probe", "SELECT ?s WHERE { ?s <urn:graph-test:knows> <urn:graph-test:n00> }", 1, {"urn:graph-test:n02", "urn:graph-test:n31"}, false, false, false});
  if (group == "Structures") cases.push_back({"self_loop_repeated_variable", "SELECT ?x WHERE { ?x <urn:graph-test:knows> ?x }", 1, {"urn:graph-test:n04"}, false, false, false});
  if (group == "Structures") cases.push_back({"variable_predicate", "SELECT ?p ?o WHERE { <urn:graph-test:n00> ?p ?o }", 2, {"urn:graph-test:knows\turn:graph-test:n01", "urn:graph-test:knows\turn:graph-test:n02", "urn:graph-test:knows\turn:graph-test:n03", "urn:graph-test:likes\turn:graph-test:topic0", "urn:graph-test:likes\turn:graph-test:topic1", "urn:graph-test:member\turn:graph-test:team0"}, false, false, false});
  if (group == "Global") cases.push_back({"hidden_path_duplicates", "SELECT ?o WHERE { ?s <urn:graph-test:knows> ?k . ?k <urn:graph-test:knows> ?o }", 1, {"urn:graph-test:n02", "urn:graph-test:n03", "urn:graph-test:n00", "urn:graph-test:n03", "urn:graph-test:n04", "urn:graph-test:n00", "urn:graph-test:n03", "urn:graph-test:n04", "urn:graph-test:n01", "urn:graph-test:n02", "urn:graph-test:n03", "urn:graph-test:n04", "urn:graph-test:n04", "urn:graph-test:n05", "urn:graph-test:n04", "urn:graph-test:n05", "urn:graph-test:leaf", "urn:graph-test:n06", "urn:graph-test:n07", "urn:graph-test:n08", "urn:graph-test:n09", "urn:graph-test:n10", "urn:graph-test:n11", "urn:graph-test:n12", "urn:graph-test:n13", "urn:graph-test:n14", "urn:graph-test:n15", "urn:graph-test:n16", "urn:graph-test:n17", "urn:graph-test:n18", "urn:graph-test:n19", "urn:graph-test:n20", "urn:graph-test:n21", "urn:graph-test:n22", "urn:graph-test:n23", "urn:graph-test:n24", "urn:graph-test:n25", "urn:graph-test:n26", "urn:graph-test:n27", "urn:graph-test:n28", "urn:graph-test:n29", "urn:graph-test:n30", "urn:graph-test:n31", "urn:graph-test:n00", "urn:graph-test:n01", "urn:graph-test:n02", "urn:graph-test:n03"}, false, true, false});
  if (group == "Global") cases.push_back({"distinct_destinations", "SELECT DISTINCT ?o WHERE { ?s <urn:graph-test:knows> ?k . ?k <urn:graph-test:knows> ?o }", 1, {"urn:graph-test:n02", "urn:graph-test:n03", "urn:graph-test:n00", "urn:graph-test:n04", "urn:graph-test:n01", "urn:graph-test:n05", "urn:graph-test:leaf", "urn:graph-test:n06", "urn:graph-test:n07", "urn:graph-test:n08", "urn:graph-test:n09", "urn:graph-test:n10", "urn:graph-test:n11", "urn:graph-test:n12", "urn:graph-test:n13", "urn:graph-test:n14", "urn:graph-test:n15", "urn:graph-test:n16", "urn:graph-test:n17", "urn:graph-test:n18", "urn:graph-test:n19", "urn:graph-test:n20", "urn:graph-test:n21", "urn:graph-test:n22", "urn:graph-test:n23", "urn:graph-test:n24", "urn:graph-test:n25", "urn:graph-test:n26", "urn:graph-test:n27", "urn:graph-test:n28", "urn:graph-test:n29", "urn:graph-test:n30", "urn:graph-test:n31"}, false, true, false});
  if (group == "Global") cases.push_back({"order_hidden_variable_slice", "SELECT ?s WHERE { ?s <urn:graph-test:knows> ?k . ?k <urn:graph-test:knows> ?o } ORDER BY ?o DESC(?s) LIMIT 9 OFFSET 3", 1, {"urn:graph-test:n00", "urn:graph-test:n31", "urn:graph-test:n02", "urn:graph-test:n31", "urn:graph-test:n02", "urn:graph-test:n00", "urn:graph-test:n31", "urn:graph-test:n02", "urn:graph-test:n01"}, true, true, false});
  if (group == "Global") cases.push_back({"distinct_order_page", "SELECT DISTINCT ?o WHERE { ?s <urn:graph-test:knows> ?k . ?k <urn:graph-test:knows> ?o } ORDER BY ?o LIMIT 5 OFFSET 2", 1, {"urn:graph-test:n01", "urn:graph-test:n02", "urn:graph-test:n03", "urn:graph-test:n04", "urn:graph-test:n05"}, true, true, false});
  if (group == "Global") cases.push_back({"count_paths", "SELECT (COUNT(*) AS ?n) WHERE { ?s <urn:graph-test:knows> ?k . ?k <urn:graph-test:knows> ?o }", 1, {"47"}, false, true, false});
  if (group == "Global") cases.push_back({"city_group_having", "SELECT ?city (COUNT(*) AS ?n) WHERE { ?s <urn:graph-test:member> ?team . ?team <urn:graph-test:located> ?city } GROUP BY ?city HAVING (COUNT(*) > 10) ORDER BY DESC(?n) ?city", 2, {"urn:graph-test:city0\t16", "urn:graph-test:city1\t16"}, true, true, false});
  if (group == "Boundary") cases.push_back({"unknown_predicate_empty", "SELECT ?s ?o WHERE { ?s <urn:graph-test:absent-predicate> ?o }", 2, {}, false, false, true});
  if (group == "Boundary") cases.push_back({"known_terms_empty_join", "SELECT ?s WHERE { ?s <urn:graph-test:member> ?team . ?team <urn:graph-test:knows> ?s }", 1, {}, false, true, false});
  if (group == "Boundary") cases.push_back({"duplicate_input_set_semantics", "SELECT ?s ?o WHERE { ?s <urn:graph-test:knows> ?o }", 2, {"urn:graph-test:n00\turn:graph-test:n01", "urn:graph-test:n00\turn:graph-test:n02", "urn:graph-test:n00\turn:graph-test:n03", "urn:graph-test:n01\turn:graph-test:n02", "urn:graph-test:n01\turn:graph-test:n03", "urn:graph-test:n02\turn:graph-test:n00", "urn:graph-test:n02\turn:graph-test:n03", "urn:graph-test:n03\turn:graph-test:n04", "urn:graph-test:n04\turn:graph-test:n04", "urn:graph-test:n04\turn:graph-test:n05", "urn:graph-test:n05\turn:graph-test:leaf", "urn:graph-test:n05\turn:graph-test:n06", "urn:graph-test:n06\turn:graph-test:n07", "urn:graph-test:n07\turn:graph-test:n08", "urn:graph-test:n08\turn:graph-test:n09", "urn:graph-test:n09\turn:graph-test:n10", "urn:graph-test:n10\turn:graph-test:n11", "urn:graph-test:n11\turn:graph-test:n12", "urn:graph-test:n12\turn:graph-test:n13", "urn:graph-test:n13\turn:graph-test:n14", "urn:graph-test:n14\turn:graph-test:n15", "urn:graph-test:n15\turn:graph-test:n16", "urn:graph-test:n16\turn:graph-test:n17", "urn:graph-test:n17\turn:graph-test:n18", "urn:graph-test:n18\turn:graph-test:n19", "urn:graph-test:n19\turn:graph-test:n20", "urn:graph-test:n20\turn:graph-test:n21", "urn:graph-test:n21\turn:graph-test:n22", "urn:graph-test:n22\turn:graph-test:n23", "urn:graph-test:n23\turn:graph-test:n24", "urn:graph-test:n24\turn:graph-test:n25", "urn:graph-test:n25\turn:graph-test:n26", "urn:graph-test:n26\turn:graph-test:n27", "urn:graph-test:n27\turn:graph-test:n28", "urn:graph-test:n28\turn:graph-test:n29", "urn:graph-test:n29\turn:graph-test:n30", "urn:graph-test:n30\turn:graph-test:n31", "urn:graph-test:n31\turn:graph-test:n00"}, false, false, false});
  if (group == "Boundary") cases.push_back({"bound_subject_neighbors", "SELECT ?o WHERE { <urn:graph-test:n00> <urn:graph-test:knows> ?o }", 1, {"urn:graph-test:n01", "urn:graph-test:n02", "urn:graph-test:n03"}, false, false, false});
  if (group == "Boundary") cases.push_back({"limit_zero", "SELECT ?o WHERE { ?s <urn:graph-test:knows> ?k . ?k <urn:graph-test:knows> ?o } LIMIT 0", 1, {}, false, false, false});
  if (group == "Boundary") cases.push_back({"offset_beyond_end", "SELECT ?s ?o WHERE { ?s <urn:graph-test:knows> ?k . ?k <urn:graph-test:knows> ?o } ORDER BY ?s ?o OFFSET 10000", 2, {}, true, false, false});
  return cases;
}

class PeerGraphFunctionTest : public PeerCrossVmTest {
 protected:
  void SetUp() override {
    ASSERT_NO_FATALS(PeerCrossVmTest::SetUp());
    const auto* seed = std::getenv("MDB_GRAPH_SEED");
    ASSERT_NE(seed, nullptr);
    // Reimport checks RDF set semantics while the oracle keeps path multiplicity.
    ASSERT_EQ(YBCMdbImportFile(seed), 0);
    ASSERT_EQ(YBCMdbImportFile(seed), 0);
    LOG(INFO) << "MDB_GRAPH_SEED imported_lines=110 unique_graph_triples=107 imports=2";
  }

  void CheckOwnership() {
    TabletRouter router;
    ASSERT_OK(router.Init(client_.get(), keyspace_));
    auto session = client_->NewSession(MonoDelta::FromSeconds(10));
    auto owner = [&](uint64_t key) {
      auto read = NewMdbReadOp(*RdfYBModel::Instance().GetTable(0), session);
      QLAddInt64HashValue(read->mutable_request(), static_cast<int64_t>(key));
      std::string encoded;
      EXPECT_OK(read->GetPartitionKey(&encoded));
      const auto* tablet = router.LocateTablet(0, encoded);
      EXPECT_NE(tablet, nullptr);
      return tablet ? tablet->tablet_id : std::string();
    };
    std::set<std::string> owners;
    size_t crossing = 0;
    for (int i = 0; i < 32; ++i) {
      const auto name = [](int index) {
        return std::string("urn:graph-test:n") + (index < 10 ? "0" : "") + std::to_string(index);
      };
      const auto subject_owner = owner(GraphId(name(i)));
      owners.insert(subject_owner);
      crossing += subject_owner != owner(GraphId(name((i + 1) % 32)));
    }
    ASSERT_GE(owners.size(), 2);
    ASSERT_GT(crossing, 0);
    LOG(INFO) << "MDB_GRAPH_OWNERSHIP anchor_tablets=" << owners.size()
              << " ring_edges_crossing_tablets=" << crossing << " storage_hosts=1";
  }

  void RunGroup(const std::string& group) {
    ASSERT_NO_FATALS(CheckOwnership());
    auto mixed = remote_;
    mixed.targets = {"local", "mdb-vm-worker"};
    mixed.max_workers = 2;
    mixed.batch_rows = 128;
    mixed.page_rows = 16;
    auto pure_remote = mixed;
    pure_remote.targets = {"mdb-vm-worker"};
    auto hash = mixed;
    hash.exchange = ExchangeMode::Hash;
    hash.exchange_spill_directory = GetTestPath("graph-exchange");
    std::filesystem::create_directory(hash.exchange_spill_directory);
    struct Rows {
      size_t width;
      std::vector<std::string> partial;
      std::vector<std::string> complete;
    };
    for (const auto& test : PeerGraphCases(group)) {
      SCOPED_TRACE(test.name + " " + test.query);
      std::vector<std::pair<std::string, WorkerOptions>> modes = {
          {"coordinator", WorkerOptions{}}, {"mixed", mixed}, {"remote", pure_remote}};
      if (test.exchange) modes.emplace_back("hash", hash);
      const std::multiset<std::string> expected(test.expected.begin(), test.expected.end());
      for (const auto& mode : modes) {
        SCOPED_TRACE(mode.first);
        Rows output{test.width, {}, {}};
        auto sink = [](const char*, const char* value, void* arg) {
          auto& rows = *static_cast<Rows*>(arg);
          rows.partial.emplace_back(value);
          if (rows.partial.size() == rows.width) {
            std::string tuple;
            for (size_t i = 0; i < rows.width; ++i) {
              if (i) tuple += '\t';
              tuple += rows.partial[i];
            }
            rows.complete.push_back(std::move(tuple));
            rows.partial.clear();
          }
        };
        RdfYBModel::Instance().SetWorkerOptions(mode.second);
        const auto begin = std::chrono::steady_clock::now();
        const auto status = YBCMdbExecuteSparql(test.query.c_str(), sink, &output);
        const auto elapsed = std::chrono::duration<double, std::milli>(
            std::chrono::steady_clock::now() - begin).count();
        const std::string profile = YBCMdbGetLastQueryProfile();
        const std::multiset<std::string> actual(output.complete.begin(), output.complete.end());
        const bool correct = status == 0 && output.partial.empty() && actual == expected &&
            (!test.ordered || output.complete == test.expected);
        LOG(INFO) << "MDB_GRAPH_CASE group=" << group << " name=" << test.name
                  << " mode=" << mode.first << " status=" << status
                  << " rows=" << output.complete.size() << " expected=" << test.expected.size()
                  << " correct=" << correct << " elapsed_ms=" << elapsed << " profile=" << profile;
        ASSERT_EQ(status, 0) << profile;
        ASSERT_TRUE(output.partial.empty()) << "Incomplete tuple";
        ASSERT_EQ(actual, expected) << "Complete tuple multiset differs from the independent oracle";
        if (test.ordered) ASSERT_EQ(output.complete, test.expected);
        if (mode.first != "coordinator") {
          ASSERT_EQ(Field(profile, "remote_coordinator_fallbacks"), 0);
          ASSERT_EQ(Field(profile, "remote_fragments"), test.empty_plan ? 0 :
                    mode.first == "hash" ? 8 : 4);
        }
        ASSERT_TRUE(std::filesystem::is_empty(hash.exchange_spill_directory));
      }
    }
  }
};

TEST_F(PeerGraphFunctionTest, GraphStructures) { RunGroup("Structures"); }
TEST_F(PeerGraphFunctionTest, GlobalGraphSemantics) { RunGroup("Global"); }
TEST_F(PeerGraphFunctionTest, EmptyAndDuplicateBoundaries) { RunGroup("Boundary"); }


TEST_F(PeerCrossVmTest, ComprehensiveSeedAndExport) {
  const auto* seed = std::getenv("MDB_COMPREHENSIVE_SEED");
  const auto* export_path = std::getenv("MDB_COMPREHENSIVE_EXPORT");
  ASSERT_NE(seed, nullptr);
  ASSERT_NE(export_path, nullptr);
  ASSERT_EQ(YBCMdbImportFile(seed), 0);
  ASSERT_OK(RdfYBModel::Instance().Flush());
  std::ofstream state(export_path);
  state << keyspace_ << '\n';
  ASSERT_TRUE(state.good());
}
}  // namespace
}  // namespace yb::mdb

namespace yb::mdb {
namespace {

struct FirstBatchCase {
  std::string name, query, expected;
  bool ordered;
};

std::string FirstBatchCanonical(const rapidjson::Value& value) {
  if (value.IsObject()) {
    std::map<std::string, std::string> fields;
    for (auto it = value.MemberBegin(); it != value.MemberEnd(); ++it) {
      auto key = std::string(it->name.GetString(), it->name.GetStringLength());
      if (key == "value" && value.HasMember("type") && value.HasMember("datatype") &&
          std::string(value["type"].GetString()) == "literal") {
        const auto datatype = std::string(value["datatype"].GetString());
        if (datatype == "http://www.w3.org/2001/XMLSchema#integer" ||
            datatype == "http://www.w3.org/2001/XMLSchema#decimal") {
          std::ostringstream normalized;
          normalized << std::setprecision(18) << std::stold(it->value.GetString());
          fields[key] = normalized.str();
          continue;
        }
      }
      fields[key] = FirstBatchCanonical(it->value);
    }
    std::string out;
    for (const auto& field : fields) out += field.first + "\x1e" + field.second + "\x1f";
    return "{" + out + "}";
  }
  if (value.IsArray()) {
    std::string out;
    for (const auto& item : value.GetArray()) out += FirstBatchCanonical(item) + "\x1d";
    return "[" + out + "]";
  }
  if (value.IsString()) return std::string("s:") + value.GetString();
  if (value.IsBool()) return value.GetBool() ? "true" : "false";
  return "null";
}

void FirstBatchCompare(const FirstBatchCase& test, const std::string& output) {
  rapidjson::Document expected, actual;
  expected.Parse(test.expected.c_str());
  actual.Parse(output.c_str());
  ASSERT_FALSE(expected.HasParseError());
  ASSERT_FALSE(actual.HasParseError()) << output;
  ASSERT_TRUE(actual.IsObject());
  if (expected.HasMember("boolean")) {
    ASSERT_TRUE(actual.HasMember("boolean"));
    ASSERT_EQ(FirstBatchCanonical(actual), FirstBatchCanonical(expected));
    return;
  }
  const rapidjson::Value* want;
  const rapidjson::Value* got;
  if (expected.HasMember("triples")) {
    ASSERT_TRUE(actual.HasMember("triples"));
    want = &expected["triples"]; got = &actual["triples"];
  } else {
    ASSERT_TRUE(actual.HasMember("head"));
    ASSERT_TRUE(actual["head"].HasMember("vars"));
    ASSERT_EQ(FirstBatchCanonical(actual["head"]), FirstBatchCanonical(expected["head"]));
    ASSERT_TRUE(actual.HasMember("results"));
    ASSERT_TRUE(actual["results"].HasMember("bindings"));
    want = &expected["results"]["bindings"]; got = &actual["results"]["bindings"];
  }
  std::vector<std::string> left, right;
  for (const auto& item : want->GetArray()) left.push_back(FirstBatchCanonical(item));
  for (const auto& item : got->GetArray()) right.push_back(FirstBatchCanonical(item));
  if (!test.ordered) { std::sort(left.begin(), left.end()); std::sort(right.begin(), right.end()); }
  ASSERT_EQ(right, left) << output;
}

class MdbFirstBatchCorrectnessTest : public MdbFragmentIntegrationTest {
 protected:
  void Run(const std::vector<FirstBatchCase>& cases) {
    const auto seed = GetTestPath("first-batch.nt");
    {
      std::ofstream file(seed);
      file << R"MDBSEED(<urn:graph-test:n00> <urn:graph-test:knows> <urn:graph-test:n01> .
<urn:graph-test:n00> <urn:graph-test:member> <urn:graph-test:team0> .
<urn:graph-test:n00> <urn:graph-test:likes> <urn:graph-test:topic0> .
<urn:graph-test:n01> <urn:graph-test:knows> <urn:graph-test:n02> .
<urn:graph-test:n01> <urn:graph-test:member> <urn:graph-test:team1> .
<urn:graph-test:n01> <urn:graph-test:likes> <urn:graph-test:topic1> .
<urn:graph-test:n02> <urn:graph-test:knows> <urn:graph-test:n03> .
<urn:graph-test:n02> <urn:graph-test:member> <urn:graph-test:team2> .
<urn:graph-test:n02> <urn:graph-test:likes> <urn:graph-test:topic2> .
<urn:graph-test:n03> <urn:graph-test:knows> <urn:graph-test:n04> .
<urn:graph-test:n03> <urn:graph-test:member> <urn:graph-test:team3> .
<urn:graph-test:n03> <urn:graph-test:likes> <urn:graph-test:topic0> .
<urn:graph-test:n04> <urn:graph-test:knows> <urn:graph-test:n05> .
<urn:graph-test:n04> <urn:graph-test:member> <urn:graph-test:team0> .
<urn:graph-test:n04> <urn:graph-test:likes> <urn:graph-test:topic1> .
<urn:graph-test:n05> <urn:graph-test:knows> <urn:graph-test:n06> .
<urn:graph-test:n05> <urn:graph-test:member> <urn:graph-test:team1> .
<urn:graph-test:n05> <urn:graph-test:likes> <urn:graph-test:topic2> .
<urn:graph-test:n06> <urn:graph-test:knows> <urn:graph-test:n07> .
<urn:graph-test:n06> <urn:graph-test:member> <urn:graph-test:team2> .
<urn:graph-test:n06> <urn:graph-test:likes> <urn:graph-test:topic0> .
<urn:graph-test:n07> <urn:graph-test:knows> <urn:graph-test:n08> .
<urn:graph-test:n07> <urn:graph-test:member> <urn:graph-test:team3> .
<urn:graph-test:n07> <urn:graph-test:likes> <urn:graph-test:topic1> .
<urn:graph-test:n08> <urn:graph-test:knows> <urn:graph-test:n09> .
<urn:graph-test:n08> <urn:graph-test:member> <urn:graph-test:team0> .
<urn:graph-test:n08> <urn:graph-test:likes> <urn:graph-test:topic2> .
<urn:graph-test:n09> <urn:graph-test:knows> <urn:graph-test:n10> .
<urn:graph-test:n09> <urn:graph-test:member> <urn:graph-test:team1> .
<urn:graph-test:n09> <urn:graph-test:likes> <urn:graph-test:topic0> .
<urn:graph-test:n10> <urn:graph-test:knows> <urn:graph-test:n11> .
<urn:graph-test:n10> <urn:graph-test:member> <urn:graph-test:team2> .
<urn:graph-test:n10> <urn:graph-test:likes> <urn:graph-test:topic1> .
<urn:graph-test:n11> <urn:graph-test:knows> <urn:graph-test:n12> .
<urn:graph-test:n11> <urn:graph-test:member> <urn:graph-test:team3> .
<urn:graph-test:n11> <urn:graph-test:likes> <urn:graph-test:topic2> .
<urn:graph-test:n12> <urn:graph-test:knows> <urn:graph-test:n13> .
<urn:graph-test:n12> <urn:graph-test:member> <urn:graph-test:team0> .
<urn:graph-test:n12> <urn:graph-test:likes> <urn:graph-test:topic0> .
<urn:graph-test:n13> <urn:graph-test:knows> <urn:graph-test:n14> .
<urn:graph-test:n13> <urn:graph-test:member> <urn:graph-test:team1> .
<urn:graph-test:n13> <urn:graph-test:likes> <urn:graph-test:topic1> .
<urn:graph-test:n14> <urn:graph-test:knows> <urn:graph-test:n15> .
<urn:graph-test:n14> <urn:graph-test:member> <urn:graph-test:team2> .
<urn:graph-test:n14> <urn:graph-test:likes> <urn:graph-test:topic2> .
<urn:graph-test:n15> <urn:graph-test:knows> <urn:graph-test:n16> .
<urn:graph-test:n15> <urn:graph-test:member> <urn:graph-test:team3> .
<urn:graph-test:n15> <urn:graph-test:likes> <urn:graph-test:topic0> .
<urn:graph-test:n16> <urn:graph-test:knows> <urn:graph-test:n17> .
<urn:graph-test:n16> <urn:graph-test:member> <urn:graph-test:team0> .
<urn:graph-test:n16> <urn:graph-test:likes> <urn:graph-test:topic1> .
<urn:graph-test:n17> <urn:graph-test:knows> <urn:graph-test:n18> .
<urn:graph-test:n17> <urn:graph-test:member> <urn:graph-test:team1> .
<urn:graph-test:n17> <urn:graph-test:likes> <urn:graph-test:topic2> .
<urn:graph-test:n18> <urn:graph-test:knows> <urn:graph-test:n19> .
<urn:graph-test:n18> <urn:graph-test:member> <urn:graph-test:team2> .
<urn:graph-test:n18> <urn:graph-test:likes> <urn:graph-test:topic0> .
<urn:graph-test:n19> <urn:graph-test:knows> <urn:graph-test:n20> .
<urn:graph-test:n19> <urn:graph-test:member> <urn:graph-test:team3> .
<urn:graph-test:n19> <urn:graph-test:likes> <urn:graph-test:topic1> .
<urn:graph-test:n20> <urn:graph-test:knows> <urn:graph-test:n21> .
<urn:graph-test:n20> <urn:graph-test:member> <urn:graph-test:team0> .
<urn:graph-test:n20> <urn:graph-test:likes> <urn:graph-test:topic2> .
<urn:graph-test:n21> <urn:graph-test:knows> <urn:graph-test:n22> .
<urn:graph-test:n21> <urn:graph-test:member> <urn:graph-test:team1> .
<urn:graph-test:n21> <urn:graph-test:likes> <urn:graph-test:topic0> .
<urn:graph-test:n22> <urn:graph-test:knows> <urn:graph-test:n23> .
<urn:graph-test:n22> <urn:graph-test:member> <urn:graph-test:team2> .
<urn:graph-test:n22> <urn:graph-test:likes> <urn:graph-test:topic1> .
<urn:graph-test:n23> <urn:graph-test:knows> <urn:graph-test:n24> .
<urn:graph-test:n23> <urn:graph-test:member> <urn:graph-test:team3> .
<urn:graph-test:n23> <urn:graph-test:likes> <urn:graph-test:topic2> .
<urn:graph-test:n24> <urn:graph-test:knows> <urn:graph-test:n25> .
<urn:graph-test:n24> <urn:graph-test:member> <urn:graph-test:team0> .
<urn:graph-test:n24> <urn:graph-test:likes> <urn:graph-test:topic0> .
<urn:graph-test:n25> <urn:graph-test:knows> <urn:graph-test:n26> .
<urn:graph-test:n25> <urn:graph-test:member> <urn:graph-test:team1> .
<urn:graph-test:n25> <urn:graph-test:likes> <urn:graph-test:topic1> .
<urn:graph-test:n26> <urn:graph-test:knows> <urn:graph-test:n27> .
<urn:graph-test:n26> <urn:graph-test:member> <urn:graph-test:team2> .
<urn:graph-test:n26> <urn:graph-test:likes> <urn:graph-test:topic2> .
<urn:graph-test:n27> <urn:graph-test:knows> <urn:graph-test:n28> .
<urn:graph-test:n27> <urn:graph-test:member> <urn:graph-test:team3> .
<urn:graph-test:n27> <urn:graph-test:likes> <urn:graph-test:topic0> .
<urn:graph-test:n28> <urn:graph-test:knows> <urn:graph-test:n29> .
<urn:graph-test:n28> <urn:graph-test:member> <urn:graph-test:team0> .
<urn:graph-test:n28> <urn:graph-test:likes> <urn:graph-test:topic1> .
<urn:graph-test:n29> <urn:graph-test:knows> <urn:graph-test:n30> .
<urn:graph-test:n29> <urn:graph-test:member> <urn:graph-test:team1> .
<urn:graph-test:n29> <urn:graph-test:likes> <urn:graph-test:topic2> .
<urn:graph-test:n30> <urn:graph-test:knows> <urn:graph-test:n31> .
<urn:graph-test:n30> <urn:graph-test:member> <urn:graph-test:team2> .
<urn:graph-test:n30> <urn:graph-test:likes> <urn:graph-test:topic0> .
<urn:graph-test:n31> <urn:graph-test:knows> <urn:graph-test:n00> .
<urn:graph-test:n31> <urn:graph-test:member> <urn:graph-test:team3> .
<urn:graph-test:n31> <urn:graph-test:likes> <urn:graph-test:topic1> .
<urn:graph-test:n00> <urn:graph-test:knows> <urn:graph-test:n02> .
<urn:graph-test:n00> <urn:graph-test:knows> <urn:graph-test:n03> .
<urn:graph-test:n01> <urn:graph-test:knows> <urn:graph-test:n03> .
<urn:graph-test:n02> <urn:graph-test:knows> <urn:graph-test:n00> .
<urn:graph-test:n04> <urn:graph-test:knows> <urn:graph-test:n04> .
<urn:graph-test:n05> <urn:graph-test:knows> <urn:graph-test:leaf> .
<urn:graph-test:n00> <urn:graph-test:likes> <urn:graph-test:topic1> .
<urn:graph-test:team0> <urn:graph-test:located> <urn:graph-test:city0> .
<urn:graph-test:team1> <urn:graph-test:located> <urn:graph-test:city1> .
<urn:graph-test:team2> <urn:graph-test:located> <urn:graph-test:city0> .
<urn:graph-test:team3> <urn:graph-test:located> <urn:graph-test:city1> .
<urn:graph-test:n00> <urn:graph-test:knows> <urn:graph-test:n01> .
<urn:graph-test:n00> <urn:graph-test:knows> <urn:graph-test:n01> .
<urn:graph-test:n01> <urn:graph-test:member> <urn:graph-test:team1> .
<urn:extra:a> <urn:extra:number> "-4"^^<http://www.w3.org/2001/XMLSchema#integer> .
<urn:extra:b> <urn:extra:number> "2"^^<http://www.w3.org/2001/XMLSchema#integer> .
<urn:extra:c> <urn:extra:number> "10"^^<http://www.w3.org/2001/XMLSchema#integer> .
<urn:extra:a> <urn:extra:label> "hello"@en .
<urn:extra:b> <urn:extra:label> "bonjour"@fr .
<urn:extra:c> <urn:extra:label> "hello" .

<urn:fix:a> <urn:fix:p> <urn:fix:b> .
<urn:fix:b> <urn:fix:p> <urn:fix:c> .
<urn:fix:c> <urn:fix:p> <urn:fix:a> .
<urn:fix:a> <urn:fix:q> <urn:fix:b> .
<urn:fix:d> <urn:fix:label> "isolated" .
<urn:fix:d> <urn:fix:term> "hello"@en .
<urn:fix:d> <urn:fix:term> "hello"@fr .
<urn:fix:d> <urn:fix:term> "hello"^^<urn:fix:custom> .
<urn:fix:d> <urn:fix:term> "" .
<urn:fix:d> <urn:fix:term> "quote\" tab\t line\n" .
<urn:fix:d> <urn:fix:term> "\u4e2d\u6587" .
<urn:fix:d> <urn:fix:blank> _:input .
)MDBSEED";
      ASSERT_TRUE(file.good());
    }
    const auto cli = workers_.binary.substr(0, workers_.binary.find_last_of('/')) + "/mdb_cli";
    const std::vector<std::string> base = {cli, "--mdb_masters=" + cluster_->GetMasterAddresses(),
        "--mdb_keyspace=mdb_p6_test"};
    auto import = base;
    import.insert(import.end(), {"import", seed});
    std::string output, error;
    const auto imported = Subprocess::Call(import, &output, &error);
    ASSERT_TRUE(imported.ok()) << imported << error;
    for (const auto& mode : {"default", "stream", "workers"}) {
      for (const auto& test : cases) {
        SCOPED_TRACE(std::string(mode) + ":" + test.name + ":" + test.query);
        auto args = base;
        args.push_back("--mdb_result_format=json");
        args.push_back("--mdb_scan_page_rows=2");
        if (std::string(mode) != "default") args.push_back("--mdb_streaming_reads=true");
        if (std::string(mode) == "workers") args.insert(args.end(), {
            "--mdb_worker_targets=local,local", "--mdb_worker_binary=" + workers_.binary,
            "--mdb_worker_page_rows=2", "--mdb_worker_batch_rows=2"});
        args.insert(args.end(), {"query", test.query});
        output.clear(); error.clear();
        const auto status = Subprocess::Call(args, &output, &error);
        const auto artifact = GetTestPath(std::string(mode) + "-" + test.name);
        { std::ofstream file(artifact + ".json"); file << output; }
        { std::ofstream file(artifact + ".stderr"); file << error; }
        if (!status.ok()) { ADD_FAILURE() << status << ":" << error; continue; }
        FirstBatchCompare(test, output);
      }
    }
  }
};

TEST_F(MdbFirstBatchCorrectnessTest, Compatibility) {
  Run({
    {R"MDBCASE(filter_iri)MDBCASE", R"MDBCASE(PREFIX g: <urn:graph-test:> PREFIX e: <urn:extra:> SELECT ?o WHERE { g:n00 g:knows ?o FILTER(?o != g:n01) })MDBCASE", R"MDBCASE({"head":{"vars":["o"]},"results":{"bindings":[{"o":{"type":"uri","value":"urn:graph-test:n03"}},{"o":{"type":"uri","value":"urn:graph-test:n02"}}]}})MDBCASE", false},
    {R"MDBCASE(union_bag)MDBCASE", R"MDBCASE(PREFIX g: <urn:graph-test:> PREFIX e: <urn:extra:> SELECT ?o WHERE { {g:n00 g:knows ?o} UNION {g:n01 g:knows ?o} })MDBCASE", R"MDBCASE({"head":{"vars":["o"]},"results":{"bindings":[{"o":{"type":"uri","value":"urn:graph-test:n03"}},{"o":{"type":"uri","value":"urn:graph-test:n02"}},{"o":{"type":"uri","value":"urn:graph-test:n01"}},{"o":{"type":"uri","value":"urn:graph-test:n03"}},{"o":{"type":"uri","value":"urn:graph-test:n02"}}]}})MDBCASE", false},
    {R"MDBCASE(optional_match)MDBCASE", R"MDBCASE(PREFIX g: <urn:graph-test:> PREFIX e: <urn:extra:> SELECT ?s ?o WHERE { ?s g:knows g:leaf OPTIONAL { ?s g:member ?o } })MDBCASE", R"MDBCASE({"head":{"vars":["s","o"]},"results":{"bindings":[{"s":{"type":"uri","value":"urn:graph-test:n05"},"o":{"type":"uri","value":"urn:graph-test:team1"}}]}})MDBCASE", false},
    {R"MDBCASE(bind_iri)MDBCASE", R"MDBCASE(PREFIX g: <urn:graph-test:> PREFIX e: <urn:extra:> SELECT ?x WHERE { g:n00 g:knows ?o BIND(?o AS ?x) })MDBCASE", R"MDBCASE({"head":{"vars":["x"]},"results":{"bindings":[{"x":{"type":"uri","value":"urn:graph-test:n03"}},{"x":{"type":"uri","value":"urn:graph-test:n02"}},{"x":{"type":"uri","value":"urn:graph-test:n01"}}]}})MDBCASE", false},
    {R"MDBCASE(bind_str)MDBCASE", R"MDBCASE(PREFIX g: <urn:graph-test:> PREFIX e: <urn:extra:> SELECT ?x WHERE { g:n00 g:knows ?o BIND(STR(?o) AS ?x) })MDBCASE", R"MDBCASE({"head":{"vars":["x"]},"results":{"bindings":[{"x":{"type":"literal","value":"urn:graph-test:n03","datatype":"http://www.w3.org/2001/XMLSchema#string"}},{"x":{"type":"literal","value":"urn:graph-test:n02","datatype":"http://www.w3.org/2001/XMLSchema#string"}},{"x":{"type":"literal","value":"urn:graph-test:n01","datatype":"http://www.w3.org/2001/XMLSchema#string"}}]}})MDBCASE", false},
    {R"MDBCASE(not_exists)MDBCASE", R"MDBCASE(PREFIX g: <urn:graph-test:> PREFIX e: <urn:extra:> SELECT ?o WHERE { g:n00 g:knows ?o FILTER NOT EXISTS { g:n01 g:knows ?o } })MDBCASE", R"MDBCASE({"head":{"vars":["o"]},"results":{"bindings":[{"o":{"type":"uri","value":"urn:graph-test:n01"}}]}})MDBCASE", false},
    {R"MDBCASE(subquery)MDBCASE", R"MDBCASE(PREFIX g: <urn:graph-test:> PREFIX e: <urn:extra:> SELECT ?o WHERE { {SELECT ?o WHERE { g:n00 g:knows ?o } ORDER BY ?o LIMIT 2} })MDBCASE", R"MDBCASE({"head":{"vars":["o"]},"results":{"bindings":[{"o":{"type":"uri","value":"urn:graph-test:n01"}},{"o":{"type":"uri","value":"urn:graph-test:n02"}}]}})MDBCASE", true},
    {R"MDBCASE(path_sequence)MDBCASE", R"MDBCASE(PREFIX g: <urn:graph-test:> PREFIX e: <urn:extra:> SELECT ?o WHERE { g:n00 g:knows/g:knows ?o })MDBCASE", R"MDBCASE({"head":{"vars":["o"]},"results":{"bindings":[{"o":{"type":"uri","value":"urn:graph-test:n04"}},{"o":{"type":"uri","value":"urn:graph-test:n00"}},{"o":{"type":"uri","value":"urn:graph-test:n03"}},{"o":{"type":"uri","value":"urn:graph-test:n03"}},{"o":{"type":"uri","value":"urn:graph-test:n02"}}]}})MDBCASE", false},
    {R"MDBCASE(path_inverse)MDBCASE", R"MDBCASE(PREFIX g: <urn:graph-test:> PREFIX e: <urn:extra:> SELECT ?s WHERE { g:n00 ^g:knows ?s })MDBCASE", R"MDBCASE({"head":{"vars":["s"]},"results":{"bindings":[{"s":{"type":"uri","value":"urn:graph-test:n02"}},{"s":{"type":"uri","value":"urn:graph-test:n31"}}]}})MDBCASE", false},
    {R"MDBCASE(projection_expression)MDBCASE", R"MDBCASE(PREFIX g: <urn:graph-test:> PREFIX e: <urn:extra:> SELECT (STR(?o) AS ?x) WHERE { g:n00 g:knows ?o })MDBCASE", R"MDBCASE({"head":{"vars":["x"]},"results":{"bindings":[{"x":{"type":"literal","value":"urn:graph-test:n03","datatype":"http://www.w3.org/2001/XMLSchema#string"}},{"x":{"type":"literal","value":"urn:graph-test:n02","datatype":"http://www.w3.org/2001/XMLSchema#string"}},{"x":{"type":"literal","value":"urn:graph-test:n01","datatype":"http://www.w3.org/2001/XMLSchema#string"}}]}})MDBCASE", false},
    {R"MDBCASE(count_distinct)MDBCASE", R"MDBCASE(PREFIX g: <urn:graph-test:> PREFIX e: <urn:extra:> SELECT (COUNT(DISTINCT ?o) AS ?n) WHERE { ?s g:knows ?o })MDBCASE", R"MDBCASE({"head":{"vars":["n"]},"results":{"bindings":[{"n":{"type":"literal","value":"33","datatype":"http://www.w3.org/2001/XMLSchema#integer"}}]}})MDBCASE", false},
    {R"MDBCASE(sum_avg)MDBCASE", R"MDBCASE(PREFIX g: <urn:graph-test:> PREFIX e: <urn:extra:> SELECT (SUM(?v) AS ?total) (AVG(?v) AS ?a) WHERE {?s e:number ?v})MDBCASE", R"MDBCASE({"head":{"vars":["total","a"]},"results":{"bindings":[{"total":{"type":"literal","value":"8","datatype":"http://www.w3.org/2001/XMLSchema#integer"},"a":{"type":"literal","value":"2.666666666666666666","datatype":"http://www.w3.org/2001/XMLSchema#decimal"}}]}})MDBCASE", false},
    {R"MDBCASE(numeric_order)MDBCASE", R"MDBCASE(PREFIX g: <urn:graph-test:> PREFIX e: <urn:extra:> SELECT ?s WHERE {?s e:number ?v} ORDER BY ?v)MDBCASE", R"MDBCASE({"head":{"vars":["s"]},"results":{"bindings":[{"s":{"type":"uri","value":"urn:extra:a"}},{"s":{"type":"uri","value":"urn:extra:b"}},{"s":{"type":"uri","value":"urn:extra:c"}}]}})MDBCASE", true},
    {R"MDBCASE(numeric_filter)MDBCASE", R"MDBCASE(PREFIX g: <urn:graph-test:> PREFIX e: <urn:extra:> SELECT ?s WHERE {?s e:number ?v FILTER(?v > 1)})MDBCASE", R"MDBCASE({"head":{"vars":["s"]},"results":{"bindings":[{"s":{"type":"uri","value":"urn:extra:c"}},{"s":{"type":"uri","value":"urn:extra:b"}}]}})MDBCASE", false},
    {R"MDBCASE(language_filter)MDBCASE", R"MDBCASE(PREFIX g: <urn:graph-test:> PREFIX e: <urn:extra:> SELECT ?s WHERE {?s e:label ?v FILTER(LANG(?v) = "en")})MDBCASE", R"MDBCASE({"head":{"vars":["s"]},"results":{"bindings":[{"s":{"type":"uri","value":"urn:extra:a"}}]}})MDBCASE", false},
    {R"MDBCASE(datatype_filter)MDBCASE", R"MDBCASE(PREFIX g: <urn:graph-test:> PREFIX e: <urn:extra:> SELECT ?s WHERE {?s e:number ?v FILTER(DATATYPE(?v) = <http://www.w3.org/2001/XMLSchema#integer>)})MDBCASE", R"MDBCASE({"head":{"vars":["s"]},"results":{"bindings":[{"s":{"type":"uri","value":"urn:extra:c"}},{"s":{"type":"uri","value":"urn:extra:b"}},{"s":{"type":"uri","value":"urn:extra:a"}}]}})MDBCASE", false},
    {R"MDBCASE(str_filter)MDBCASE", R"MDBCASE(PREFIX g: <urn:graph-test:> PREFIX e: <urn:extra:> SELECT ?s WHERE {?s e:label ?v FILTER(STR(?v) = "hello")})MDBCASE", R"MDBCASE({"head":{"vars":["s"]},"results":{"bindings":[{"s":{"type":"uri","value":"urn:extra:c"}},{"s":{"type":"uri","value":"urn:extra:a"}}]}})MDBCASE", false},
    {R"MDBCASE(regex)MDBCASE", R"MDBCASE(PREFIX g: <urn:graph-test:> PREFIX e: <urn:extra:> SELECT ?s WHERE {?s e:label ?v FILTER(REGEX(STR(?v), "^hell"))})MDBCASE", R"MDBCASE({"head":{"vars":["s"]},"results":{"bindings":[{"s":{"type":"uri","value":"urn:extra:c"}},{"s":{"type":"uri","value":"urn:extra:a"}}]}})MDBCASE", false},
    {R"MDBCASE(iri_lexical_order)MDBCASE", R"MDBCASE(PREFIX g: <urn:graph-test:> PREFIX e: <urn:extra:> SELECT ?s WHERE {?s g:member g:team0} ORDER BY DESC(?s) LIMIT 3 OFFSET 1)MDBCASE", R"MDBCASE({"head":{"vars":["s"]},"results":{"bindings":[{"s":{"type":"uri","value":"urn:graph-test:n24"}},{"s":{"type":"uri","value":"urn:graph-test:n20"}},{"s":{"type":"uri","value":"urn:graph-test:n16"}}]}})MDBCASE", true},
    {R"MDBCASE(empty_count)MDBCASE", R"MDBCASE(PREFIX g: <urn:graph-test:> PREFIX e: <urn:extra:> SELECT (COUNT(*) AS ?n) WHERE {?s g:absent ?o})MDBCASE", R"MDBCASE({"head":{"vars":["n"]},"results":{"bindings":[{"n":{"type":"literal","value":"0","datatype":"http://www.w3.org/2001/XMLSchema#integer"}}]}})MDBCASE", false},
    {R"MDBCASE(cartesian)MDBCASE", R"MDBCASE(PREFIX g: <urn:graph-test:> PREFIX e: <urn:extra:> SELECT ?a ?b WHERE {g:n00 g:knows ?a . g:n01 g:knows ?b})MDBCASE", R"MDBCASE({"head":{"vars":["a","b"]},"results":{"bindings":[{"a":{"type":"uri","value":"urn:graph-test:n01"},"b":{"type":"uri","value":"urn:graph-test:n03"}},{"a":{"type":"uri","value":"urn:graph-test:n02"},"b":{"type":"uri","value":"urn:graph-test:n03"}},{"a":{"type":"uri","value":"urn:graph-test:n03"},"b":{"type":"uri","value":"urn:graph-test:n03"}},{"a":{"type":"uri","value":"urn:graph-test:n01"},"b":{"type":"uri","value":"urn:graph-test:n02"}},{"a":{"type":"uri","value":"urn:graph-test:n02"},"b":{"type":"uri","value":"urn:graph-test:n02"}},{"a":{"type":"uri","value":"urn:graph-test:n03"},"b":{"type":"uri","value":"urn:graph-test:n02"}}]}})MDBCASE", false},
    {R"MDBCASE(blank_pattern)MDBCASE", R"MDBCASE(PREFIX g: <urn:graph-test:> PREFIX e: <urn:extra:> SELECT ?o WHERE { [g:knows ?o; g:member g:team0] })MDBCASE", R"MDBCASE({"head":{"vars":["o"]},"results":{"bindings":[{"o":{"type":"uri","value":"urn:graph-test:n29"}},{"o":{"type":"uri","value":"urn:graph-test:n25"}},{"o":{"type":"uri","value":"urn:graph-test:n21"}},{"o":{"type":"uri","value":"urn:graph-test:n17"}},{"o":{"type":"uri","value":"urn:graph-test:n13"}},{"o":{"type":"uri","value":"urn:graph-test:n09"}},{"o":{"type":"uri","value":"urn:graph-test:n04"}},{"o":{"type":"uri","value":"urn:graph-test:n05"}},{"o":{"type":"uri","value":"urn:graph-test:n03"}},{"o":{"type":"uri","value":"urn:graph-test:n02"}},{"o":{"type":"uri","value":"urn:graph-test:n01"}}]}})MDBCASE", false},
    {R"MDBCASE(empty_bgp)MDBCASE", R"MDBCASE(PREFIX g: <urn:graph-test:> PREFIX e: <urn:extra:> SELECT (1 AS ?n) WHERE {})MDBCASE", R"MDBCASE({"head":{"vars":["n"]},"results":{"bindings":[{"n":{"type":"literal","value":"1","datatype":"http://www.w3.org/2001/XMLSchema#integer"}}]}})MDBCASE", false},
  });
}

TEST_F(MdbFirstBatchCorrectnessTest, Results) {
  Run({
    {R"MDBCASE(optional_unbound)MDBCASE", R"MDBCASE(PREFIX g: <urn:graph-test:> PREFIX e: <urn:extra:> SELECT ?s ?o WHERE { g:n05 g:knows ?s OPTIONAL { ?s g:member ?o } })MDBCASE", R"MDBCASE({"head":{"vars":["s","o"]},"results":{"bindings":[{"s":{"type":"uri","value":"urn:graph-test:leaf"}},{"s":{"type":"uri","value":"urn:graph-test:n06"},"o":{"type":"uri","value":"urn:graph-test:team2"}}]}})MDBCASE", false},
    {R"MDBCASE(unbound_projection)MDBCASE", R"MDBCASE(PREFIX g: <urn:graph-test:> PREFIX e: <urn:extra:> SELECT ?o ?missing WHERE {g:n00 g:knows ?o})MDBCASE", R"MDBCASE({"head":{"vars":["o","missing"]},"results":{"bindings":[{"o":{"type":"uri","value":"urn:graph-test:n03"}},{"o":{"type":"uri","value":"urn:graph-test:n02"}},{"o":{"type":"uri","value":"urn:graph-test:n01"}}]}})MDBCASE", false},
    {R"MDBCASE(numeric_div_zero)MDBCASE", R"MDBCASE(PREFIX g: <urn:graph-test:> PREFIX e: <urn:extra:> SELECT (?v / 0 AS ?x) WHERE {?s e:number ?v})MDBCASE", R"MDBCASE({"head":{"vars":["x"]},"results":{"bindings":[{},{},{}]}})MDBCASE", false},
    {R"MDBCASE(all_unbound_three)MDBCASE", R"MDBCASE(SELECT (?v/0 AS ?x) WHERE { VALUES ?v { 1 2 3 } })MDBCASE", R"MDBCASE({"head":{"vars":["x"]},"results":{"bindings":[{},{},{}]}})MDBCASE", false},
    {R"MDBCASE(variable_order)MDBCASE", R"MDBCASE(SELECT ?b ?a WHERE { VALUES (?a ?b) { (1 2) (3 UNDEF) } })MDBCASE", R"MDBCASE({"head":{"vars":["b","a"]},"results":{"bindings":[{"b":{"type":"literal","value":"2","datatype":"http://www.w3.org/2001/XMLSchema#integer"},"a":{"type":"literal","value":"1","datatype":"http://www.w3.org/2001/XMLSchema#integer"}},{"a":{"type":"literal","value":"3","datatype":"http://www.w3.org/2001/XMLSchema#integer"}}]}})MDBCASE", false},
    {R"MDBCASE(zero_columns_two)MDBCASE", R"MDBCASE(SELECT * WHERE { VALUES () { () () } })MDBCASE", R"MDBCASE({"head":{"vars":[]},"results":{"bindings":[{},{}]}})MDBCASE", false},
    {R"MDBCASE(zero_columns_empty)MDBCASE", R"MDBCASE(SELECT * WHERE { VALUES () { } })MDBCASE", R"MDBCASE({"head":{"vars":[]},"results":{"bindings":[]}})MDBCASE", false},
    {R"MDBCASE(missing_projection)MDBCASE", R"MDBCASE(SELECT ?a ?missing WHERE { VALUES ?a { 1 1 2 } })MDBCASE", R"MDBCASE({"head":{"vars":["a","missing"]},"results":{"bindings":[{"a":{"type":"literal","value":"1","datatype":"http://www.w3.org/2001/XMLSchema#integer"}},{"a":{"type":"literal","value":"1","datatype":"http://www.w3.org/2001/XMLSchema#integer"}},{"a":{"type":"literal","value":"2","datatype":"http://www.w3.org/2001/XMLSchema#integer"}}]}})MDBCASE", false},
  });
}

TEST_F(MdbFirstBatchCorrectnessTest, Values) {
  Run({
    {R"MDBCASE(values)MDBCASE", R"MDBCASE(PREFIX g: <urn:graph-test:> PREFIX e: <urn:extra:> SELECT ?o WHERE { VALUES ?s {g:n00 g:n01} ?s g:knows ?o })MDBCASE", R"MDBCASE({"head":{"vars":["o"]},"results":{"bindings":[{"o":{"type":"uri","value":"urn:graph-test:n03"}},{"o":{"type":"uri","value":"urn:graph-test:n02"}},{"o":{"type":"uri","value":"urn:graph-test:n01"}},{"o":{"type":"uri","value":"urn:graph-test:n03"}},{"o":{"type":"uri","value":"urn:graph-test:n02"}}]}})MDBCASE", false},
    {R"MDBCASE(standalone_duplicates)MDBCASE", R"MDBCASE(SELECT ?x WHERE { VALUES ?x { <urn:fix:new> <urn:fix:new> UNDEF } })MDBCASE", R"MDBCASE({"head":{"vars":["x"]},"results":{"bindings":[{"x":{"type":"uri","value":"urn:fix:new"}},{"x":{"type":"uri","value":"urn:fix:new"}},{}]}})MDBCASE", false},
    {R"MDBCASE(multicol_unbound)MDBCASE", R"MDBCASE(SELECT ?s ?o WHERE { VALUES (?s ?o) { (<urn:fix:a> UNDEF) (UNDEF <urn:fix:b>) } ?s <urn:fix:p> ?o })MDBCASE", R"MDBCASE({"head":{"vars":["s","o"]},"results":{"bindings":[{"s":{"type":"uri","value":"urn:fix:a"},"o":{"type":"uri","value":"urn:fix:b"}},{"s":{"type":"uri","value":"urn:fix:a"},"o":{"type":"uri","value":"urn:fix:b"}}]}})MDBCASE", false},
    {R"MDBCASE(after_pattern)MDBCASE", R"MDBCASE(SELECT ?s ?o WHERE { ?s <urn:fix:p> ?o VALUES ?s { <urn:fix:a> <urn:fix:a> } })MDBCASE", R"MDBCASE({"head":{"vars":["s","o"]},"results":{"bindings":[{"s":{"type":"uri","value":"urn:fix:a"},"o":{"type":"uri","value":"urn:fix:b"}},{"s":{"type":"uri","value":"urn:fix:a"},"o":{"type":"uri","value":"urn:fix:b"}}]}})MDBCASE", false},
    {R"MDBCASE(incompatible)MDBCASE", R"MDBCASE(SELECT ?x WHERE { VALUES ?x { 1 2 } VALUES ?x { 3 } })MDBCASE", R"MDBCASE({"head":{"vars":["x"]},"results":{"bindings":[]}})MDBCASE", false},
    {R"MDBCASE(compatible_undef)MDBCASE", R"MDBCASE(SELECT ?x ?y WHERE { VALUES (?x ?y) { (1 UNDEF) (UNDEF 2) } VALUES ?x { 1 3 } })MDBCASE", R"MDBCASE({"head":{"vars":["x","y"]},"results":{"bindings":[{"x":{"type":"literal","value":"1","datatype":"http://www.w3.org/2001/XMLSchema#integer"},"y":{"type":"literal","value":"2","datatype":"http://www.w3.org/2001/XMLSchema#integer"}},{"x":{"type":"literal","value":"1","datatype":"http://www.w3.org/2001/XMLSchema#integer"}},{"x":{"type":"literal","value":"3","datatype":"http://www.w3.org/2001/XMLSchema#integer"},"y":{"type":"literal","value":"2","datatype":"http://www.w3.org/2001/XMLSchema#integer"}}]}})MDBCASE", false},
    {R"MDBCASE(empty_rhs)MDBCASE", R"MDBCASE(SELECT ?x WHERE { VALUES ?x { 1 2 } VALUES ?y { } })MDBCASE", R"MDBCASE({"head":{"vars":["x"]},"results":{"bindings":[]}})MDBCASE", false},
  });
}

TEST_F(MdbFirstBatchCorrectnessTest, Minus) {
  Run({
    {R"MDBCASE(minus)MDBCASE", R"MDBCASE(PREFIX g: <urn:graph-test:> PREFIX e: <urn:extra:> SELECT ?o WHERE { g:n00 g:knows ?o MINUS { g:n01 g:knows ?o } })MDBCASE", R"MDBCASE({"head":{"vars":["o"]},"results":{"bindings":[{"o":{"type":"uri","value":"urn:graph-test:n01"}}]}})MDBCASE", false},
    {R"MDBCASE(disjoint_domain)MDBCASE", R"MDBCASE(SELECT ?x WHERE { VALUES ?x { 1 1 2 } MINUS { VALUES ?y { 9 } } })MDBCASE", R"MDBCASE({"head":{"vars":["x"]},"results":{"bindings":[{"x":{"type":"literal","value":"1","datatype":"http://www.w3.org/2001/XMLSchema#integer"}},{"x":{"type":"literal","value":"1","datatype":"http://www.w3.org/2001/XMLSchema#integer"}},{"x":{"type":"literal","value":"2","datatype":"http://www.w3.org/2001/XMLSchema#integer"}}]}})MDBCASE", false},
    {R"MDBCASE(right_undef)MDBCASE", R"MDBCASE(SELECT ?x WHERE { VALUES ?x { 1 2 } MINUS { VALUES ?x { UNDEF } } })MDBCASE", R"MDBCASE({"head":{"vars":["x"]},"results":{"bindings":[{"x":{"type":"literal","value":"1","datatype":"http://www.w3.org/2001/XMLSchema#integer"}},{"x":{"type":"literal","value":"2","datatype":"http://www.w3.org/2001/XMLSchema#integer"}}]}})MDBCASE", false},
    {R"MDBCASE(left_undef)MDBCASE", R"MDBCASE(SELECT ?x WHERE { VALUES ?x { 1 UNDEF 2 } MINUS { VALUES ?x { 1 } } })MDBCASE", R"MDBCASE({"head":{"vars":["x"]},"results":{"bindings":[{},{"x":{"type":"literal","value":"2","datatype":"http://www.w3.org/2001/XMLSchema#integer"}}]}})MDBCASE", false},
    {R"MDBCASE(empty_right)MDBCASE", R"MDBCASE(SELECT ?x WHERE { VALUES ?x { 1 1 2 } MINUS { VALUES ?x { } } })MDBCASE", R"MDBCASE({"head":{"vars":["x"]},"results":{"bindings":[{"x":{"type":"literal","value":"1","datatype":"http://www.w3.org/2001/XMLSchema#integer"}},{"x":{"type":"literal","value":"1","datatype":"http://www.w3.org/2001/XMLSchema#integer"}},{"x":{"type":"literal","value":"2","datatype":"http://www.w3.org/2001/XMLSchema#integer"}}]}})MDBCASE", false},
    {R"MDBCASE(two_common_conflict)MDBCASE", R"MDBCASE(SELECT ?x ?y WHERE { VALUES (?x ?y) { (1 2) (1 3) (UNDEF 2) } MINUS { VALUES (?x ?y) { (1 2) } } })MDBCASE", R"MDBCASE({"head":{"vars":["x","y"]},"results":{"bindings":[{"x":{"type":"literal","value":"1","datatype":"http://www.w3.org/2001/XMLSchema#integer"},"y":{"type":"literal","value":"3","datatype":"http://www.w3.org/2001/XMLSchema#integer"}}]}})MDBCASE", false},
    {R"MDBCASE(filter_disjoint)MDBCASE", R"MDBCASE(SELECT ?x WHERE { VALUES ?x { 1 2 } MINUS { FILTER(true) } })MDBCASE", R"MDBCASE({"head":{"vars":["x"]},"results":{"bindings":[{"x":{"type":"literal","value":"1","datatype":"http://www.w3.org/2001/XMLSchema#integer"}},{"x":{"type":"literal","value":"2","datatype":"http://www.w3.org/2001/XMLSchema#integer"}}]}})MDBCASE", false},
  });
}

TEST_F(MdbFirstBatchCorrectnessTest, Paths) {
  Run({
    {R"MDBCASE(path_alternative)MDBCASE", R"MDBCASE(PREFIX g: <urn:graph-test:> PREFIX e: <urn:extra:> SELECT ?o WHERE { g:n00 (g:knows|g:member) ?o })MDBCASE", R"MDBCASE({"head":{"vars":["o"]},"results":{"bindings":[{"o":{"type":"uri","value":"urn:graph-test:n03"}},{"o":{"type":"uri","value":"urn:graph-test:n02"}},{"o":{"type":"uri","value":"urn:graph-test:n01"}},{"o":{"type":"uri","value":"urn:graph-test:team0"}}]}})MDBCASE", false},
    {R"MDBCASE(path_plus)MDBCASE", R"MDBCASE(PREFIX g: <urn:graph-test:> PREFIX e: <urn:extra:> SELECT ?o WHERE { g:n00 g:knows+ ?o } ORDER BY ?o LIMIT 40)MDBCASE", R"MDBCASE({"head":{"vars":["o"]},"results":{"bindings":[{"o":{"type":"uri","value":"urn:graph-test:leaf"}},{"o":{"type":"uri","value":"urn:graph-test:n00"}},{"o":{"type":"uri","value":"urn:graph-test:n01"}},{"o":{"type":"uri","value":"urn:graph-test:n02"}},{"o":{"type":"uri","value":"urn:graph-test:n03"}},{"o":{"type":"uri","value":"urn:graph-test:n04"}},{"o":{"type":"uri","value":"urn:graph-test:n05"}},{"o":{"type":"uri","value":"urn:graph-test:n06"}},{"o":{"type":"uri","value":"urn:graph-test:n07"}},{"o":{"type":"uri","value":"urn:graph-test:n08"}},{"o":{"type":"uri","value":"urn:graph-test:n09"}},{"o":{"type":"uri","value":"urn:graph-test:n10"}},{"o":{"type":"uri","value":"urn:graph-test:n11"}},{"o":{"type":"uri","value":"urn:graph-test:n12"}},{"o":{"type":"uri","value":"urn:graph-test:n13"}},{"o":{"type":"uri","value":"urn:graph-test:n14"}},{"o":{"type":"uri","value":"urn:graph-test:n15"}},{"o":{"type":"uri","value":"urn:graph-test:n16"}},{"o":{"type":"uri","value":"urn:graph-test:n17"}},{"o":{"type":"uri","value":"urn:graph-test:n18"}},{"o":{"type":"uri","value":"urn:graph-test:n19"}},{"o":{"type":"uri","value":"urn:graph-test:n20"}},{"o":{"type":"uri","value":"urn:graph-test:n21"}},{"o":{"type":"uri","value":"urn:graph-test:n22"}},{"o":{"type":"uri","value":"urn:graph-test:n23"}},{"o":{"type":"uri","value":"urn:graph-test:n24"}},{"o":{"type":"uri","value":"urn:graph-test:n25"}},{"o":{"type":"uri","value":"urn:graph-test:n26"}},{"o":{"type":"uri","value":"urn:graph-test:n27"}},{"o":{"type":"uri","value":"urn:graph-test:n28"}},{"o":{"type":"uri","value":"urn:graph-test:n29"}},{"o":{"type":"uri","value":"urn:graph-test:n30"}},{"o":{"type":"uri","value":"urn:graph-test:n31"}}]}})MDBCASE", true},
    {R"MDBCASE(path_star)MDBCASE", R"MDBCASE(PREFIX g: <urn:graph-test:> PREFIX e: <urn:extra:> SELECT ?o WHERE { g:leaf g:knows* ?o })MDBCASE", R"MDBCASE({"head":{"vars":["o"]},"results":{"bindings":[{"o":{"type":"uri","value":"urn:graph-test:leaf"}}]}})MDBCASE", false},
    {R"MDBCASE(cycle_plus)MDBCASE", R"MDBCASE(SELECT ?o WHERE { <urn:fix:a> <urn:fix:p>+ ?o })MDBCASE", R"MDBCASE({"head":{"vars":["o"]},"results":{"bindings":[{"o":{"type":"uri","value":"urn:fix:a"}},{"o":{"type":"uri","value":"urn:fix:c"}},{"o":{"type":"uri","value":"urn:fix:b"}}]}})MDBCASE", false},
    {R"MDBCASE(cycle_star)MDBCASE", R"MDBCASE(SELECT ?o WHERE { <urn:fix:a> <urn:fix:p>* ?o })MDBCASE", R"MDBCASE({"head":{"vars":["o"]},"results":{"bindings":[{"o":{"type":"uri","value":"urn:fix:a"}},{"o":{"type":"uri","value":"urn:fix:c"}},{"o":{"type":"uri","value":"urn:fix:b"}}]}})MDBCASE", false},
    {R"MDBCASE(alternative_bag)MDBCASE", R"MDBCASE(SELECT ?o WHERE { <urn:fix:a> (<urn:fix:p>|<urn:fix:q>) ?o })MDBCASE", R"MDBCASE({"head":{"vars":["o"]},"results":{"bindings":[{"o":{"type":"uri","value":"urn:fix:b"}},{"o":{"type":"uri","value":"urn:fix:b"}}]}})MDBCASE", false},
    {R"MDBCASE(optional_set)MDBCASE", R"MDBCASE(SELECT ?o WHERE { <urn:fix:a> <urn:fix:p>? ?o })MDBCASE", R"MDBCASE({"head":{"vars":["o"]},"results":{"bindings":[{"o":{"type":"uri","value":"urn:fix:a"}},{"o":{"type":"uri","value":"urn:fix:b"}}]}})MDBCASE", false},
    {R"MDBCASE(unknown_star)MDBCASE", R"MDBCASE(SELECT ?o WHERE { <urn:fix:unknown> <urn:fix:missing>* ?o })MDBCASE", R"MDBCASE({"head":{"vars":["o"]},"results":{"bindings":[{"o":{"type":"uri","value":"urn:fix:unknown"}}]}})MDBCASE", false},
    {R"MDBCASE(bound_both_star)MDBCASE", R"MDBCASE(SELECT ?x WHERE { <urn:fix:unknown> <urn:fix:missing>* <urn:fix:unknown> BIND(1 AS ?x) })MDBCASE", R"MDBCASE({"head":{"vars":["x"]},"results":{"bindings":[{"x":{"type":"literal","value":"1","datatype":"http://www.w3.org/2001/XMLSchema#integer"}}]}})MDBCASE", false},
    {R"MDBCASE(target_only_star)MDBCASE", R"MDBCASE(SELECT ?s WHERE { ?s <urn:fix:p>* <urn:fix:d> })MDBCASE", R"MDBCASE({"head":{"vars":["s"]},"results":{"bindings":[{"s":{"type":"uri","value":"urn:fix:d"}}]}})MDBCASE", false},
    {R"MDBCASE(same_variable)MDBCASE", R"MDBCASE(SELECT ?s WHERE { ?s <urn:fix:p>+ ?s })MDBCASE", R"MDBCASE({"head":{"vars":["s"]},"results":{"bindings":[{"s":{"type":"uri","value":"urn:fix:c"}},{"s":{"type":"uri","value":"urn:fix:b"}},{"s":{"type":"uri","value":"urn:fix:a"}}]}})MDBCASE", false},
    {R"MDBCASE(reset_from_values)MDBCASE", R"MDBCASE(SELECT ?s ?o WHERE { VALUES ?s { <urn:fix:a> <urn:fix:b> <urn:fix:a> } ?s <urn:fix:p>+ ?o })MDBCASE", R"MDBCASE({"head":{"vars":["s","o"]},"results":{"bindings":[{"s":{"type":"uri","value":"urn:fix:a"},"o":{"type":"uri","value":"urn:fix:a"}},{"s":{"type":"uri","value":"urn:fix:a"},"o":{"type":"uri","value":"urn:fix:c"}},{"s":{"type":"uri","value":"urn:fix:a"},"o":{"type":"uri","value":"urn:fix:b"}},{"s":{"type":"uri","value":"urn:fix:b"},"o":{"type":"uri","value":"urn:fix:a"}},{"s":{"type":"uri","value":"urn:fix:b"},"o":{"type":"uri","value":"urn:fix:c"}},{"s":{"type":"uri","value":"urn:fix:b"},"o":{"type":"uri","value":"urn:fix:b"}},{"s":{"type":"uri","value":"urn:fix:a"},"o":{"type":"uri","value":"urn:fix:a"}},{"s":{"type":"uri","value":"urn:fix:a"},"o":{"type":"uri","value":"urn:fix:c"}},{"s":{"type":"uri","value":"urn:fix:a"},"o":{"type":"uri","value":"urn:fix:b"}}]}})MDBCASE", false},
    {R"MDBCASE(nested_closure)MDBCASE", R"MDBCASE(SELECT ?o WHERE { <urn:fix:a> (<urn:fix:p>/<urn:fix:p>)+ ?o })MDBCASE", R"MDBCASE({"head":{"vars":["o"]},"results":{"bindings":[{"o":{"type":"uri","value":"urn:fix:a"}},{"o":{"type":"uri","value":"urn:fix:c"}},{"o":{"type":"uri","value":"urn:fix:b"}}]}})MDBCASE", false},
    {R"MDBCASE(inverse_closure)MDBCASE", R"MDBCASE(SELECT ?o WHERE { <urn:fix:a> (^<urn:fix:p>)+ ?o })MDBCASE", R"MDBCASE({"head":{"vars":["o"]},"results":{"bindings":[{"o":{"type":"uri","value":"urn:fix:a"}},{"o":{"type":"uri","value":"urn:fix:c"}},{"o":{"type":"uri","value":"urn:fix:b"}}]}})MDBCASE", false},
    {R"MDBCASE(negated_edge)MDBCASE", R"MDBCASE(SELECT ?o WHERE { <urn:fix:a> !<urn:fix:q> ?o })MDBCASE", R"MDBCASE({"head":{"vars":["o"]},"results":{"bindings":[{"o":{"type":"uri","value":"urn:fix:b"}}]}})MDBCASE", false},
  });
}

TEST_F(MdbFirstBatchCorrectnessTest, Ask) {
  Run({
    {R"MDBCASE(ask)MDBCASE", R"MDBCASE(PREFIX g: <urn:graph-test:> PREFIX e: <urn:extra:> ASK WHERE { g:n00 g:knows g:n01 })MDBCASE", R"MDBCASE({"head":{},"boolean":true})MDBCASE", false},
    {R"MDBCASE(false)MDBCASE", R"MDBCASE(ASK WHERE { <urn:fix:a> <urn:fix:missing> ?o })MDBCASE", R"MDBCASE({"head":{},"boolean":false})MDBCASE", false},
    {R"MDBCASE(empty_solution)MDBCASE", R"MDBCASE(ASK WHERE { })MDBCASE", R"MDBCASE({"head":{},"boolean":true})MDBCASE", false},
    {R"MDBCASE(zero_limit)MDBCASE", R"MDBCASE(ASK WHERE { <urn:fix:a> <urn:fix:p> ?o } LIMIT 0)MDBCASE", R"MDBCASE({"head":{},"boolean":false})MDBCASE", false},
    {R"MDBCASE(values)MDBCASE", R"MDBCASE(ASK WHERE { VALUES ?x { 1 2 } })MDBCASE", R"MDBCASE({"head":{},"boolean":true})MDBCASE", false},
  });
}

TEST_F(MdbFirstBatchCorrectnessTest, Construct) {
  Run({
    {R"MDBCASE(construct)MDBCASE", R"MDBCASE(PREFIX g: <urn:graph-test:> PREFIX e: <urn:extra:> CONSTRUCT {g:n00 g:test ?o} WHERE {g:n00 g:knows ?o})MDBCASE", R"MDBCASE({"type":"graph","triples":[[{"type":"uri","value":"urn:graph-test:n00"},{"type":"uri","value":"urn:graph-test:test"},{"type":"uri","value":"urn:graph-test:n03"}],[{"type":"uri","value":"urn:graph-test:n00"},{"type":"uri","value":"urn:graph-test:test"},{"type":"uri","value":"urn:graph-test:n02"}],[{"type":"uri","value":"urn:graph-test:n00"},{"type":"uri","value":"urn:graph-test:test"},{"type":"uri","value":"urn:graph-test:n01"}]]})MDBCASE", false},
    {R"MDBCASE(fresh_template)MDBCASE", R"MDBCASE(CONSTRUCT { ?s <urn:fix:new_predicate> "fresh" } WHERE { ?s <urn:fix:p> <urn:fix:b> })MDBCASE", R"MDBCASE({"type":"graph","triples":[[{"type":"uri","value":"urn:fix:a"},{"type":"uri","value":"urn:fix:new_predicate"},{"type":"literal","value":"fresh","datatype":"http://www.w3.org/2001/XMLSchema#string"}]]})MDBCASE", false},
    {R"MDBCASE(deduplicated)MDBCASE", R"MDBCASE(CONSTRUCT { <urn:fix:a> <urn:fix:out> ?o } WHERE { VALUES ?o { <urn:fix:b> <urn:fix:b> } })MDBCASE", R"MDBCASE({"type":"graph","triples":[[{"type":"uri","value":"urn:fix:a"},{"type":"uri","value":"urn:fix:out"},{"type":"uri","value":"urn:fix:b"}]]})MDBCASE", false},
    {R"MDBCASE(unbound_template)MDBCASE", R"MDBCASE(CONSTRUCT { ?s <urn:fix:out> ?missing } WHERE { ?s <urn:fix:p> ?o })MDBCASE", R"MDBCASE({"type":"graph","triples":[]})MDBCASE", false},
    {R"MDBCASE(invalid_subject)MDBCASE", R"MDBCASE(CONSTRUCT { ?literal <urn:fix:out> ?s } WHERE { VALUES ?literal { "text" } VALUES ?s { <urn:fix:a> } })MDBCASE", R"MDBCASE({"type":"graph","triples":[]})MDBCASE", false},
    {R"MDBCASE(zero_limit)MDBCASE", R"MDBCASE(CONSTRUCT { ?s <urn:fix:out> ?o } WHERE { ?s <urn:fix:p> ?o } LIMIT 0)MDBCASE", R"MDBCASE({"type":"graph","triples":[]})MDBCASE", false},
    {R"MDBCASE(ordered_slice)MDBCASE", R"MDBCASE(CONSTRUCT { ?s <urn:fix:out> ?o } WHERE { ?s <urn:fix:p> ?o } ORDER BY DESC(?s) LIMIT 1 OFFSET 1)MDBCASE", R"MDBCASE({"type":"graph","triples":[[{"type":"uri","value":"urn:fix:b"},{"type":"uri","value":"urn:fix:out"},{"type":"uri","value":"urn:fix:c"}]]})MDBCASE", false},
    {R"MDBCASE(shorthand)MDBCASE", R"MDBCASE(CONSTRUCT WHERE { ?s <urn:fix:p> <urn:fix:b> })MDBCASE", R"MDBCASE({"type":"graph","triples":[[{"type":"uri","value":"urn:fix:a"},{"type":"uri","value":"urn:fix:p"},{"type":"uri","value":"urn:fix:b"}]]})MDBCASE", false},
  });
}

TEST_F(MdbFirstBatchCorrectnessTest, Terms) {
  Run({
    {R"MDBCASE(lang_identity)MDBCASE", R"MDBCASE(PREFIX g: <urn:graph-test:> PREFIX e: <urn:extra:> SELECT DISTINCT ?v WHERE {?s e:label ?v})MDBCASE", R"MDBCASE({"head":{"vars":["v"]},"results":{"bindings":[{"v":{"type":"literal","value":"hello","datatype":"http://www.w3.org/2001/XMLSchema#string"}},{"v":{"type":"literal","value":"bonjour","xml:lang":"fr"}},{"v":{"type":"literal","value":"hello","xml:lang":"en"}}]}})MDBCASE", false},
    {R"MDBCASE(full_term_identity)MDBCASE", R"MDBCASE(SELECT ?o WHERE { <urn:fix:d> <urn:fix:term> ?o })MDBCASE", R"MDBCASE({"head":{"vars":["o"]},"results":{"bindings":[{"o":{"type":"literal","value":"\u4e2d\u6587","datatype":"http://www.w3.org/2001/XMLSchema#string"}},{"o":{"type":"literal","value":"quote\" tab\t line\n","datatype":"http://www.w3.org/2001/XMLSchema#string"}},{"o":{"type":"literal","value":"","datatype":"http://www.w3.org/2001/XMLSchema#string"}},{"o":{"type":"literal","value":"hello","datatype":"urn:fix:custom"}},{"o":{"type":"literal","value":"hello","xml:lang":"fr"}},{"o":{"type":"literal","value":"hello","xml:lang":"en"}}]}})MDBCASE", false},
    {R"MDBCASE(query_only_language)MDBCASE", R"MDBCASE(SELECT ?x WHERE { VALUES ?x { "hi"@EN "hi"@fr ""^^<urn:fix:custom> } })MDBCASE", R"MDBCASE({"head":{"vars":["x"]},"results":{"bindings":[{"x":{"type":"literal","value":"hi","xml:lang":"en"}},{"x":{"type":"literal","value":"hi","xml:lang":"fr"}},{"x":{"type":"literal","value":"","datatype":"urn:fix:custom"}}]}})MDBCASE", false},
    {R"MDBCASE(blank_kind)MDBCASE", R"MDBCASE(SELECT (isBlank(?b) AS ?yes) WHERE { <urn:fix:d> <urn:fix:blank> ?b })MDBCASE", R"MDBCASE({"head":{"vars":["yes"]},"results":{"bindings":[{"yes":{"type":"literal","value":"true","datatype":"http://www.w3.org/2001/XMLSchema#boolean"}}]}})MDBCASE", false},
  });
}

}  // namespace
}  // namespace yb::mdb

namespace yb::mdb {
namespace {

struct FirstBatchTypedOutput {
  using Term = std::tuple<int, std::string, std::string, std::string>;
  YBCMdbResultKind kind = YBC_MDB_BINDINGS;
  std::vector<std::string> names;
  std::vector<std::vector<Term>> rows, triples;
  std::function<void()> first_row;
  bool ended = false, boolean = false;

  static Term Copy(const YBCMdbTerm& term) {
    return {term.kind, term.value, term.datatype, term.language};
  }
  static const YBCMdbResultCallbacks* Callbacks() {
    static const YBCMdbResultCallbacks callbacks = {
      [](YBCMdbResultKind kind, const YBCMdbColumn* cols, size_t n, void* ctx) {
        auto& out = *static_cast<FirstBatchTypedOutput*>(ctx);
        out.kind = kind;
        for (size_t i = 0; i < n; ++i) out.names.emplace_back(cols[i].name);
      },
      [](const YBCMdbTerm* terms, size_t n, void* ctx) {
        auto& out = *static_cast<FirstBatchTypedOutput*>(ctx);
        std::vector<Term> row;
        for (size_t i = 0; i < n; ++i) row.push_back(Copy(terms[i]));
        out.rows.push_back(std::move(row));
        if (out.rows.size() == 1 && out.first_row) out.first_row();
      },
      [](int value, void* ctx) { static_cast<FirstBatchTypedOutput*>(ctx)->boolean = value; },
      [](const YBCMdbTerm* terms, void* ctx) {
        auto& out = *static_cast<FirstBatchTypedOutput*>(ctx);
        out.triples.push_back({Copy(terms[0]), Copy(terms[1]), Copy(terms[2])});
      },
      [](void* ctx) { static_cast<FirstBatchTypedOutput*>(ctx)->ended = true; }
    };
    return &callbacks;
  }
};

TEST_F(MdbFirstBatchCorrectnessTest, TypedApiEmptyRowsAndLegacyRecovery) {
  const char* query = "SELECT (?v/0 AS ?x) WHERE { VALUES ?v { 1 2 3 } }";
  FirstBatchTypedOutput output;
  ASSERT_EQ(YBCMdbExecuteSparqlWithResults(query, output.Callbacks(), &output), 0);
  ASSERT_TRUE(output.ended);
  ASSERT_EQ(output.names, std::vector<std::string>({"x"}));
  ASSERT_EQ(output.rows.size(), 3);
  for (const auto& row : output.rows) {
    ASSERT_EQ(row.size(), 1);
    ASSERT_EQ(std::get<0>(row[0]), YBC_MDB_UNBOUND);
  }
  size_t values = 0;
  auto legacy = [](const char*, const char*, void* arg) { ++*static_cast<size_t*>(arg); };
  ASSERT_NE(YBCMdbExecuteSparql(query, legacy, &values), 0);
  ASSERT_EQ(values, 0);
  ASSERT_EQ(YBCMdbExecuteSparql("SELECT ?x WHERE { VALUES ?x { 1 2 } }", legacy, &values), 0);
  ASSERT_EQ(values, 2);
}

TEST_F(MdbFirstBatchCorrectnessTest, ConstructBlankIdentityPerSolution) {
  const char* query = "CONSTRUCT { _:b <urn:fix:left> ?x . _:b <urn:fix:right> ?x } "
                      "WHERE { VALUES ?x { 1 1 } }";
  std::set<std::string> previous;
  for (size_t run = 0; run < 2; ++run) {
    FirstBatchTypedOutput output;
    ASSERT_EQ(YBCMdbExecuteSparqlWithResults(query, output.Callbacks(), &output), 0);
    ASSERT_EQ(output.kind, YBC_MDB_GRAPH);
    ASSERT_TRUE(output.ended);
    ASSERT_EQ(output.triples.size(), 4);
    std::map<std::string, std::set<std::string>> subjects;
    for (const auto& triple : output.triples) {
      ASSERT_EQ(std::get<0>(triple[0]), YBC_MDB_BLANK);
      ASSERT_EQ(std::get<0>(triple[1]), YBC_MDB_IRI);
      ASSERT_EQ(std::get<1>(triple[2]), "1");
      const auto& subject = std::get<1>(triple[0]);
      ASSERT_EQ(previous.count(subject), 0);
      subjects[subject].insert(std::get<1>(triple[1]));
    }
    ASSERT_EQ(subjects.size(), 2);
    for (const auto& item : subjects) {
      ASSERT_EQ(item.second, std::set<std::string>({"urn:fix:left", "urn:fix:right"}));
      previous.insert(item.first);
    }
  }
}

TEST_F(MdbFirstBatchCorrectnessTest, PropertyPathReadViewSurvivesCommittedWrite) {
  for (uint64_t s : {101, 102, 103}) Write(s, 9000, s + 100);
  ASSERT_OK(RdfYBModel::Instance().Flush());
  RegisterRdfIdentity(999, RdfIdentity(TripleTermType::URI, Iri(999)));
  uint64_t last_source = 101;
  for (uint64_t s : {102, 103})
    if (g_gmap_to_pack.at(s).id > g_gmap_to_pack.at(last_source).id) last_source = s;
  Status write_status;
  FirstBatchTypedOutput output;
  output.first_row = [&] {
    write_status = RdfYBModel::Instance().WriteTripleToDocDB(last_source, 9000, 999);
    if (write_status.ok()) write_status = RdfYBModel::Instance().Flush();
  };
  const char* query = "SELECT ?s ?o WHERE { ?s <urn:p6:9000>+ ?o }";
  ASSERT_EQ(YBCMdbExecuteSparqlWithResults(query, output.Callbacks(), &output), 0);
  ASSERT_OK(write_status);
  ASSERT_EQ(output.rows.size(), 3);
  for (const auto& row : output.rows) ASSERT_NE(std::get<1>(row[1]), Iri(999));
  FirstBatchTypedOutput later;
  ASSERT_EQ(YBCMdbExecuteSparqlWithResults(query, later.Callbacks(), &later), 0);
  ASSERT_EQ(later.rows.size(), 4);
}

}  // namespace
}  // namespace yb::mdb

namespace yb::mdb {
namespace {
TEST_F(MdbFirstBatchCorrectnessTest, TypedEntryKeepsInsertDataCompatibility) {
  ASSERT_EQ(YBCMdbInitCluster(cluster_->GetMasterAddresses().c_str(), "mdb_p6_test"), 0);
  FirstBatchTypedOutput update;
  ASSERT_EQ(YBCMdbExecuteSparqlWithResults(
      "INSERT DATA { <urn:fix:update-s> <urn:fix:update-p> \"updated\"@en . }",
      update.Callbacks(), &update), 0);
  FirstBatchTypedOutput output;
  ASSERT_EQ(YBCMdbExecuteSparqlWithResults(
      "SELECT ?o WHERE { <urn:fix:update-s> <urn:fix:update-p> ?o }",
      output.Callbacks(), &output), 0);
  ASSERT_EQ(output.rows.size(), 1);
  ASSERT_EQ(output.rows[0].size(), 1);
  ASSERT_EQ(std::get<1>(output.rows[0][0]), "updated");
  ASSERT_EQ(std::get<3>(output.rows[0][0]), "en");
}
}  // namespace
}  // namespace yb::mdb

namespace yb::mdb {
namespace {

class MdbDictionaryRecoveryTest : public MdbFragmentIntegrationTest {
 protected:
  void DictionaryRow(const std::string& key, int64_t value) {
    client::TableHandle table;
    ASSERT_OK(table.Open(client::YBTableName(YQL_DATABASE_CQL, "mdb_p6_test",
                                           PersistentIdMap::kTableName), client_.get()));
    auto session = client_->NewSession(MonoDelta::FromSeconds(30));
    auto op = NewMdbInsertOp(table, session);
    QLAddStringHashValue(op->mutable_request(), key);
    table.AddInt64ColumnValue(op->mutable_request(), "term_id", value);
    ASSERT_OK(session->TEST_ApplyAndFlush(op));
    ASSERT_EQ(op->response().status(), QLResponsePB::YQL_STATUS_OK);
  }
  std::string Cli() const {
    return workers_.binary.substr(0, workers_.binary.find_last_of('/')) + "/mdb_cli";
  }
  std::vector<std::string> Base() const {
    return {Cli(), "--mdb_masters=" + cluster_->GetMasterAddresses(),
                  "--mdb_keyspace=mdb_p6_test", "--mdb_streaming_reads=true"};
  }
  size_t StoredRows(int permutation) {
    size_t count = 0;
    client::TableIteratorOptions options;
    Status error;
    options.columns = std::vector<std::string>{"c0", "c1", "c2"};
    options.error_handler = [&](const Status& status) { error = status; };
    for (const auto& row : client::TableRange(*RdfYBModel::Instance().GetTable(permutation), options)) {
      (void)row; ++count;
    }
    if (!error.ok()) ADD_FAILURE() << error;
    return count;
  }
  void QueryOne(const std::string& query, const std::string& expected) {
    auto args = Base();
    args.insert(args.end(), {"--mdb_result_format=json", "query", query});
    std::string out, error;
    const auto status = Subprocess::Call(args, &out, &error);
    ASSERT_TRUE(status.ok()) << status << error;
    rapidjson::Document result;
    result.Parse(out.c_str());
    ASSERT_FALSE(result.HasParseError()) << out;
    ASSERT_TRUE(result.HasMember("results")) << out;
    ASSERT_EQ(result["results"]["bindings"].Size(), 1) << out << error;
    ASSERT_EQ(result["results"]["bindings"][0]["o"]["value"].GetString(), expected);
  }
};

TEST_F(MdbDictionaryRecoveryTest, ConcurrentIndependentClientsShareStableIds) {
  constexpr size_t kWriters = 4, kRounds = 12;
  std::vector<std::unique_ptr<client::YBClient>> clients;
  for (size_t i = 0; i < kWriters; ++i) {
    auto result = client::YBClientBuilder().add_master_server_addr(cluster_->GetMasterAddresses()).Build();
    ASSERT_OK(result);
    clients.push_back(std::move(*result));
  }
  std::vector<std::map<std::string, uint64_t>> results(kWriters);
  std::vector<Status> statuses(kWriters);
  std::atomic<size_t> ready{0};
  std::atomic<bool> go{false};
  TestThreadHolder threads;
  for (size_t i = 0; i < kWriters; ++i) threads.AddThreadFunctor([&, i] {
    PersistentIdMap dictionary;
    statuses[i] = dictionary.Init(clients[i].get(), "mdb_p6_test");
    ++ready;
    while (!go.load()) SleepFor(MonoDelta::FromMilliseconds(1));
    if (!statuses[i].ok()) return;
    for (size_t n = 0; n < kRounds; ++n) {
      for (const auto& term : {"urn:dict:shared:" + std::to_string(n),
                              "urn:dict:writer:" + std::to_string(i) + ":" + std::to_string(n)}) {
        auto id = dictionary.GetOrCreate(term);
        if (!id.ok()) { statuses[i] = id.status(); return; }
        results[i].emplace(term, *id);
      }
    }
  });
  while (ready.load() != kWriters) SleepFor(MonoDelta::FromMilliseconds(1));
  go = true;
  threads.JoinAll();
  std::map<std::string, uint64_t> expected;
  std::map<uint64_t, std::string> identities;
  for (size_t i = 0; i < kWriters; ++i) {
    ASSERT_OK(statuses[i]);
    ASSERT_EQ(results[i].size(), kRounds * 2);
    for (const auto& entry : results[i]) {
      ASSERT_NE(entry.second, 0);
      const auto by_term = expected.emplace(entry).first;
      ASSERT_EQ(by_term->second, entry.second);
      const auto by_id = identities.emplace(entry.second, entry.first).first;
      ASSERT_EQ(by_id->second, entry.first);
    }
  }
  ASSERT_EQ(expected.size(), kRounds * (kWriters + 1));
  PersistentIdMap reopened;
  ASSERT_OK(reopened.Init(client_.get(), "mdb_p6_test"));
  for (const auto& entry : expected) {
    const auto found = reopened.GetOrCreate(entry.first);
    ASSERT_OK(found);
    ASSERT_EQ(*found, entry.second);
  }
}

TEST_F(MdbDictionaryRecoveryTest, ReservedGapsAndExhaustionNeverReuseIds) {
  PersistentIdMap dictionary;
  ASSERT_OK(dictionary.Init(client_.get(), "mdb_p6_test"));
  const auto first = dictionary.GetOrCreate("urn:dict:first");
  ASSERT_OK(first);
  ASSERT_NO_FATALS(DictionaryRow("counter", 100));
  PersistentIdMap reopened;
  ASSERT_OK(reopened.Init(client_.get(), "mdb_p6_test"));
  const auto after_gap = reopened.GetOrCreate("urn:dict:after-gap");
  ASSERT_OK(after_gap);
  ASSERT_GT(*after_gap, 100);
  ASSERT_NO_FATALS(DictionaryRow("counter", std::numeric_limits<int64_t>::max()));
  const auto existing = reopened.GetOrCreate("urn:dict:first");
  ASSERT_OK(existing);
  ASSERT_EQ(*existing, *first);
  ASSERT_NOK(reopened.GetOrCreate("urn:dict:exhausted"));
}

TEST_F(MdbDictionaryRecoveryTest, CommittedDataRecoversWithoutLegacyMirror) {
  IdMap local;
  auto& model = RdfYBModel::Instance();
  const auto s = model.ResolveRdfTerm(local, TripleTermType::URI, "urn:dict:subject");
  const auto p = model.ResolveRdfTerm(local, TripleTermType::URI, "urn:dict:predicate");
  const auto o = model.ResolveRdfTerm(local, TripleTermType::LITERAL, "recovered", "@en");
  ASSERT_OK(s); ASSERT_OK(p); ASSERT_OK(o);
  ASSERT_OK(model.BeginTransaction());
  ASSERT_OK(model.WriteTripleToDocDB(*s, *p, *o));
  ASSERT_OK(model.CommitTransaction());
  for (int perm = 0; perm < 6; ++perm) ASSERT_EQ(StoredRows(perm), 1);
  ASSERT_NO_FATALS(QueryOne("SELECT ?o WHERE { <urn:dict:subject> <urn:dict:predicate> ?o }", "recovered"));
}

TEST_F(MdbDictionaryRecoveryTest, KilledImporterRecoversCommittedTermsAndAllIndexes) {
  const auto input = GetTestPath("crash-import.nt");
  { std::ofstream file(input); file << "<urn:dict:crash> <urn:dict:predicate> \"survives\"@en .\n"; }
  auto args = Base();
  args.insert(args.end(), {"--TEST_mdb_pause_before_dictionary_mirror_ms=60000", "import", input});
  Subprocess importer(args.front(), args);
  ASSERT_OK(importer.Start());
  ASSERT_EVENTUALLY([&] {
    for (int perm = 0; perm < 6; ++perm) ASSERT_EQ(StoredRows(perm), 1);
  });
  ASSERT_OK(importer.Kill(SIGKILL));
  int result;
  ASSERT_OK(importer.Wait(&result));
  ASSERT_NE(result, 0);
  ASSERT_NO_FATALS(QueryOne("SELECT ?o WHERE { <urn:dict:crash> <urn:dict:predicate> ?o }", "survives"));
  auto again = Base(); again.insert(again.end(), {"import", input});
  ASSERT_OK(Subprocess::Call(again));
  for (int perm = 0; perm < 6; ++perm) ASSERT_EQ(StoredRows(perm), 1);
}

TEST_F(MdbDictionaryRecoveryTest, LongLivedAdapterRefreshesAnotherWritersTerms) {
  ASSERT_EQ(YBCMdbInitCluster(cluster_->GetMasterAddresses().c_str(), "mdb_p6_test"), 0);
  const auto input = GetTestPath("other-writer.nt");
  { std::ofstream file(input); file << "<urn:dict:other> <urn:dict:predicate> \"fresh\"@fr .\n"; }
  auto args = Base(); args.insert(args.end(), {"import", input});
  ASSERT_OK(Subprocess::Call(args));
  FirstBatchTypedOutput output;
  ASSERT_EQ(YBCMdbExecuteSparqlWithResults(
      "SELECT ?o WHERE { <urn:dict:other> <urn:dict:predicate> ?o }", output.Callbacks(), &output), 0);
  ASSERT_EQ(output.rows.size(), 1);
  ASSERT_EQ(std::get<1>(output.rows[0][0]), "fresh");
  ASSERT_EQ(std::get<3>(output.rows[0][0]), "fr");
}

}  // namespace
}  // namespace yb::mdb

namespace yb::mdb {
namespace {

TEST_F(MdbDictionaryRecoveryTest, CorruptionFailsClosedWithoutPartialRestore) {
  PersistentIdMap dictionary;
  ASSERT_OK(dictionary.Init(client_.get(), "mdb_p6_test"));
  const auto first = dictionary.GetOrCreate("urn:dict:valid");
  ASSERT_OK(first);
  IdMap restored;
  ASSERT_OK(dictionary.Restore(&restored));
  ASSERT_EQ(restored.Size(), 1);
  ASSERT_NO_FATALS(DictionaryRow("term:urn:dict:collision", *first));
  ASSERT_TRUE(dictionary.Restore(&restored).IsCorruption());
  ASSERT_EQ(restored.Size(), 1);
  ASSERT_FALSE(restored.Contains("urn:dict:collision"));
  PersistentIdMap reopened;
  ASSERT_TRUE(reopened.Init(client_.get(), "mdb_p6_test").IsCorruption());
  ASSERT_NOK(reopened.GetOrCreate("urn:dict:must-not-allocate"));
}

TEST_F(MdbDictionaryRecoveryTest, RewoundCounterRejectsReopenBeforeAllocation) {
  PersistentIdMap dictionary;
  ASSERT_OK(dictionary.Init(client_.get(), "mdb_p6_test"));
  ASSERT_OK(dictionary.GetOrCreate("urn:dict:first"));
  ASSERT_OK(dictionary.GetOrCreate("urn:dict:second"));
  ASSERT_NO_FATALS(DictionaryRow("counter", 0));
  PersistentIdMap reopened;
  ASSERT_TRUE(reopened.Init(client_.get(), "mdb_p6_test").IsCorruption());
  ASSERT_NOK(reopened.GetOrCreate("urn:dict:must-not-reuse"));
}

TEST_F(MdbDictionaryRecoveryTest, LegacyModeRefreshesDataWithAlreadyKnownTerms) {
  ASSERT_EQ(YBCMdbInitCluster(cluster_->GetMasterAddresses().c_str(), "mdb_p6_test"), 0);
  RdfYBModel::Instance().SetUseStreamingReads(false);
  const auto input = GetTestPath("existing-terms.nt");
  {
    std::ofstream file(input);
    file << "<urn:dict:s> <urn:dict:p> <urn:dict:o> .\n"
            "<urn:dict:o> <urn:dict:p> <urn:dict:s> .\n";
  }
  auto args = Base(); args.insert(args.end(), {"import", input});
  ASSERT_OK(Subprocess::Call(args));
  const char* query = "SELECT ?o WHERE { <urn:dict:s> <urn:dict:p> ?o }";
  FirstBatchTypedOutput before;
  ASSERT_EQ(YBCMdbExecuteSparqlWithResults(query, before.Callbacks(), &before), 0);
  ASSERT_EQ(before.rows.size(), 1);
  { std::ofstream file(input); file << "<urn:dict:s> <urn:dict:p> <urn:dict:s> .\n"; }
  ASSERT_OK(Subprocess::Call(args));
  FirstBatchTypedOutput after;
  ASSERT_EQ(YBCMdbExecuteSparqlWithResults(query, after.Callbacks(), &after), 0);
  ASSERT_EQ(after.rows.size(), 2);
  std::set<std::string> objects;
  for (const auto& row : after.rows) objects.insert(std::get<1>(row[0]));
  ASSERT_EQ(objects, std::set<std::string>({"urn:dict:s", "urn:dict:o"}));
}

}  // namespace
}  // namespace yb::mdb

namespace yb::mdb {
namespace {
TEST_F(MdbDictionaryRecoveryTest, ConcurrentImportProcessesPreserveAllTypedRows) {
  constexpr size_t kWriters = 4, kRows = 8;
  std::vector<std::unique_ptr<Subprocess>> importers;
  std::map<std::string, std::string> expected;
  for (size_t writer = 0; writer < kWriters; ++writer) {
    const auto input = GetTestPath("writer-" + std::to_string(writer) + ".nt");
    {
      std::ofstream file(input);
      for (size_t row = 0; row < kRows; ++row) {
        const auto subject = "urn:dict:process:" + std::to_string(writer) + ":" + std::to_string(row);
        const auto object = "value " + std::to_string(row);
        expected.emplace(subject, object);
        file << '<' << subject << "> <urn:dict:predicate> \"" << object << "\"@en .\n";
      }
    }
    auto args = Base(); args.insert(args.end(), {"import", input});
    auto process = std::make_unique<Subprocess>(args.front(), args);
    ASSERT_OK(process->Start());
    importers.push_back(std::move(process));
  }
  for (const auto& process : importers) {
    int result;
    ASSERT_OK(process->Wait(&result));
    ASSERT_EQ(result, 0);
  }
  for (int perm = 0; perm < 6; ++perm) ASSERT_EQ(StoredRows(perm), kWriters * kRows);
  ASSERT_EQ(YBCMdbInitCluster(cluster_->GetMasterAddresses().c_str(), "mdb_p6_test"), 0);
  FirstBatchTypedOutput output;
  ASSERT_EQ(YBCMdbExecuteSparqlWithResults(
      "SELECT ?s ?o WHERE { ?s <urn:dict:predicate> ?o }", output.Callbacks(), &output), 0);
  ASSERT_EQ(output.rows.size(), expected.size());
  std::map<std::string, std::string> actual;
  for (const auto& row : output.rows) {
    ASSERT_EQ(row.size(), 2);
    ASSERT_EQ(std::get<0>(row[0]), YBC_MDB_IRI);
    ASSERT_EQ(std::get<0>(row[1]), YBC_MDB_LITERAL);
    ASSERT_EQ(std::get<3>(row[1]), "en");
    ASSERT_TRUE(actual.emplace(std::get<1>(row[0]), std::get<1>(row[1])).second);
  }
  ASSERT_EQ(actual, expected);
}
}  // namespace
}  // namespace yb::mdb


namespace yb::mdb {
namespace {
class MdbStage9BaselineTest : public MdbDictionaryRecoveryTest {
 protected:
  using Rows = std::multiset<std::pair<std::string, std::string>>;
  void CheckProfile(const std::string& profile, bool remote) {
    rapidjson::Document value; value.Parse(profile.c_str());
    ASSERT_FALSE(value.HasParseError()) << profile;
    uint64_t stages = 0;
    for (const auto* name : {"adapter_parse_us", "adapter_read_view_us", "adapter_dictionary_us",
                             "adapter_plan_us", "adapter_execute_us"}) {
      ASSERT_TRUE(value.HasMember(name)) << profile;
      ASSERT_TRUE(value[name].IsUint64()) << profile;
      stages += value[name].GetUint64();
    }
    ASSERT_GE(value["adapter_total_us"].GetUint64(), stages);
    ASSERT_GT(value["adapter_dictionary_us"].GetUint64(), 0);
    if (remote) {
      ASSERT_EQ(value["remote_coordinator_fallbacks"].GetUint64(), 0);
      ASSERT_GT(value["remote_fragments"].GetUint64(), 0);
      ASSERT_GT(value["remote_open_wait_us"].GetUint64(), 0);
      ASSERT_GT(value["remote_read_operations"].GetUint64(), 0);
    } else {
      ASSERT_EQ(value["remote_fragments"].GetUint64(), 0);
      ASSERT_GT(value["storage_flush_us"].GetUint64(), 0);
    }
  }
  void RunBaseline(const std::string& shape, size_t count) {
    const auto* datasets = std::getenv("MDB_STAGE9_DATASETS");
    if (!datasets) GTEST_SKIP() << "Set MDB_STAGE9_DATASETS for opt-in performance cases";
    const std::string input = std::string(datasets) + "/" + shape + "-" + std::to_string(count) + ".nt";
    ASSERT_TRUE(std::filesystem::exists(input));
    auto import = Base(); import.insert(import.end(), {"import", input});
    const auto load_started = WallMicros();
    ASSERT_OK(Subprocess::Call(import));
    LOG(INFO) << "STAGE9_LOAD {\"shape\":\"" << shape << "\",\"n\":" << count
              << ",\"elapsed_us\":" << WallMicros() - load_started << "}";
    Rows expected;
    for (size_t i = 0; i < count; ++i) {
      if (shape == "unique") expected.emplace(Iri(100+i), Iri(200000+i));
      else if (shape == "shared") {
        expected.emplace(Iri(100+i), Iri(200000));
        expected.emplace(Iri(100+i), Iri(200001));
      } else for (size_t j = 0; j < 16; ++j)
        expected.emplace(Iri(100+i), Iri(200000+(i%64)*16+j));
    }
    auto& model = RdfYBModel::Instance();
    model.SetUseStreamingReads(true);
    model.SetStreamingPageRows(16);
    model.SetBatchJoinOptions(BatchJoinOptions());
    const auto init_started = WallMicros();
    ASSERT_EQ(YBCMdbInitCluster(cluster_->GetMasterAddresses().c_str(), "mdb_p6_test"), 0);
    const auto adapter_init_us = WallMicros() - init_started;
    const std::string query = "SELECT ?s ?o WHERE { ?s <urn:p6:9000> ?k . ?k <urn:p6:9001> ?o }";
    auto worker3 = workers_; worker3.batch_rows = 128; worker3.page_rows = 16;
    auto worker1 = worker3; worker1.max_workers = 1;
    auto exchange = worker3; exchange.exchange = ExchangeMode::Hash;
    const auto spill = GetTestPath("stage9-spill"); std::filesystem::create_directory(spill);
    exchange.exchange_spill_directory = spill;
    const std::vector<std::pair<std::string, WorkerOptions>> modes = {
      {"coordinator", WorkerOptions{}}, {"worker1", worker1}, {"worker3", worker3}, {"exchange3", exchange}};
    size_t repeats = 5;
    if (const auto* override_repeats = std::getenv("MDB_STAGE9_REPEATS")) repeats = std::stoul(override_repeats);
    ASSERT_GE(repeats, 1); ASSERT_LE(repeats, 10);
    LOG(INFO) << "STAGE9_CONFIG {\"shape\":\"" << shape << "\",\"n\":" << count
              << ",\"rows\":" << expected.size() << ",\"adapter_init_us\":" << adapter_init_us
              << ",\"repeats\":" << repeats << ",\"tservers\":3,\"replicas\":3,\"tablets_per_index\":4}";
    for (int sample = -1; sample < static_cast<int>(repeats); ++sample) {
      for (size_t offset = 0; offset < modes.size(); ++offset) {
        const auto& [mode, options] = modes[(offset+sample+1)%modes.size()];
        model.SetWorkerOptions(options);
        FirstBatchTypedOutput output;
        const auto api_started = WallMicros();
        const auto status = YBCMdbExecuteSparqlWithResults(query.c_str(), output.Callbacks(), &output);
        const auto api_us = WallMicros() - api_started;
        const std::string profile = YBCMdbGetLastQueryProfile();
        ASSERT_EQ(status, 0) << profile;
        ASSERT_EQ(output.kind, YBC_MDB_BINDINGS); ASSERT_TRUE(output.ended);
        ASSERT_EQ(output.names, std::vector<std::string>({"s", "o"}));
        Rows actual;
        for (const auto& row : output.rows) {
          ASSERT_EQ(row.size(), 2);
          ASSERT_EQ(std::get<0>(row[0]), YBC_MDB_IRI); ASSERT_EQ(std::get<0>(row[1]), YBC_MDB_IRI);
          actual.emplace(std::get<1>(row[0]), std::get<1>(row[1]));
        }
        ASSERT_EQ(actual, expected);
        ASSERT_NO_FATALS(CheckProfile(profile, !options.targets.empty()));
        rapidjson::Document api_profile; api_profile.Parse(profile.c_str());
        ASSERT_LE(api_profile["adapter_total_us"].GetUint64(), api_us);
        ASSERT_TRUE(std::filesystem::is_empty(spill));
        LOG(INFO) << "STAGE9_SAMPLE {\"shape\":\"" << shape << "\",\"n\":" << count
                  << ",\"boundary\":\"adapter\",\"mode\":\"" << mode << "\",\"sample\":" << sample
                  << ",\"elapsed_us\":" << api_us << ",\"rows\":" << actual.size()
                  << ",\"correct\":true,\"profile\":" << profile << "}";
        // Query-endpoint repeats isolate execution; fresh-process observations
        // separately expose initialization. Bound the expensive large-graph
        // startup probes to the coordinator, before any worker optimization.
        if (sample > 0 || (count > 128 && mode != "coordinator")) continue;
        auto args = Base();
        args.insert(args.end(), {"--mdb_scan_page_rows=16", "--mdb_result_format=json", "--mdb_query_profile=true"});
        if (!options.targets.empty()) args.insert(args.end(), {
          "--mdb_worker_targets=local,local,local", "--mdb_worker_binary=" + workers_.binary,
          "--mdb_worker_max_processes=" + std::to_string(options.max_workers),
          "--mdb_worker_batch_rows=128", "--mdb_worker_page_rows=16",
          "--mdb_exchange=" + std::string(options.exchange == ExchangeMode::None ? "none" : "hash"),
          "--mdb_exchange_spill_directory=" + spill});
        args.insert(args.end(), {"query", query});
        std::string out, error;
        const auto cli_started = WallMicros();
        const auto cli_status = Subprocess::Call(args, &out, &error);
        const auto cli_us = WallMicros() - cli_started;
        ASSERT_TRUE(cli_status.ok()) << cli_status << error;
        rapidjson::Document result; result.Parse(out.c_str());
        ASSERT_FALSE(result.HasParseError());
        ASSERT_EQ(result["head"]["vars"].Size(), 2);
        actual.clear();
        for (const auto& row : result["results"]["bindings"].GetArray()) {
          ASSERT_STREQ(row["s"]["type"].GetString(), "uri"); ASSERT_STREQ(row["o"]["type"].GetString(), "uri");
          actual.emplace(row["s"]["value"].GetString(), row["o"]["value"].GetString());
        }
        ASSERT_EQ(actual, expected);
        auto extract = [&](const std::string& prefix) {
          const auto start = error.find(prefix);
          if (start == std::string::npos) return std::string();
          const auto end = error.find('\n', start);
          return error.substr(start+prefix.size(), end-start-prefix.size());
        };
        const auto cli_profile = extract("MDB_QUERY_PROFILE ");
        const auto cli_timing = extract("MDB_CLI_TIMING ");
        ASSERT_NO_FATALS(CheckProfile(cli_profile, !options.targets.empty()));
        rapidjson::Document timing; timing.Parse(cli_timing.c_str());
        ASSERT_FALSE(timing.HasParseError()) << error;
        ASSERT_EQ(timing["status"].GetInt(), 0);
        ASSERT_GE(timing["total_us"].GetUint64(), timing["init_us"].GetUint64()+
            timing["execute_us"].GetUint64()+timing["publish_us"].GetUint64()+timing["analyze_us"].GetUint64());
        ASSERT_LE(timing["total_us"].GetUint64(), cli_us);
        LOG(INFO) << "STAGE9_SAMPLE {\"shape\":\"" << shape << "\",\"n\":" << count
                  << ",\"boundary\":\"cli\",\"mode\":\"" << mode << "\",\"sample\":" << sample
                  << ",\"elapsed_us\":" << cli_us << ",\"rows\":" << actual.size()
                  << ",\"correct\":true,\"profile\":" << cli_profile << ",\"cli\":" << cli_timing << "}";
        ASSERT_TRUE(std::filesystem::is_empty(spill));
      }
    }
  }
};
TEST_F(MdbStage9BaselineTest, Unique128) { RunBaseline("unique", 128); }
TEST_F(MdbStage9BaselineTest, Unique4096) { RunBaseline("unique", 4096); }
TEST_F(MdbStage9BaselineTest, Shared128) { RunBaseline("shared", 128); }
TEST_F(MdbStage9BaselineTest, Shared4096) { RunBaseline("shared", 4096); }
TEST_F(MdbStage9BaselineTest, Fanout128) { RunBaseline("fanout", 128); }
TEST_F(MdbStage9BaselineTest, Fanout4096) { RunBaseline("fanout", 4096); }
}  // namespace
}  // namespace yb::mdb


namespace yb::mdb {
namespace {
TEST_F(MdbDictionaryRecoveryTest, TransactionRetainsStoppedManagerUntilLastStrongReference) {
  auto manager = std::make_shared<client::TransactionManager>(
      client_.get(), scoped_refptr<server::Clock>(client_->Clock()), client::LocalTabletFilter());
  std::weak_ptr<client::TransactionManager> weak_manager = manager;
  auto transaction = MakeManagedTransaction(manager);
  ASSERT_OK(transaction->Init(IsolationLevel::SNAPSHOT_ISOLATION));
  auto callback_reference = transaction;
  std::weak_ptr<client::YBTransaction> weak_transaction = transaction;
  transaction.reset();
  manager->Shutdown();
  manager.reset();
  ASSERT_FALSE(weak_manager.expired());
  TestThreadHolder callback;
  callback.AddThreadFunctor([&callback_reference] { callback_reference.reset(); });
  callback.JoinAll();
  ASSERT_TRUE(weak_transaction.expired());
  ASSERT_TRUE(weak_manager.expired());
}

TEST_F(MdbDictionaryRecoveryTest, ExplicitAdapterShutdownReopensPersistedData) {
  ASSERT_EQ(YBCMdbInitCluster(cluster_->GetMasterAddresses().c_str(), "mdb_p6_test"), 0);
  FirstBatchTypedOutput update;
  ASSERT_EQ(YBCMdbExecuteSparqlWithResults(
      "INSERT DATA { <urn:lifetime:s> <urn:lifetime:p> \"kept\"@en . }",
      update.Callbacks(), &update), 0);
  for (int round = 0; round < 3; ++round) {
    ASSERT_EQ(YBCMdbShutdown(), 0);
    ASSERT_EQ(YBCMdbShutdown(), 0);
    ASSERT_FALSE(RdfYBModel::Instance().IsConnected());
    ASSERT_EQ(YBCMdbInitCluster(cluster_->GetMasterAddresses().c_str(), "mdb_p6_test"), 0);
    FirstBatchTypedOutput output;
    ASSERT_EQ(YBCMdbExecuteSparqlWithResults(
        "SELECT ?o WHERE { <urn:lifetime:s> <urn:lifetime:p> ?o }", output.Callbacks(), &output), 0);
    ASSERT_EQ(output.rows.size(), 1);
    ASSERT_EQ(std::get<1>(output.rows[0][0]), "kept");
    ASSERT_EQ(std::get<3>(output.rows[0][0]), "en");
  }
  ASSERT_EQ(YBCMdbShutdown(), 0);
}
}  // namespace
}  // namespace yb::mdb


namespace yb::mdb {
namespace {
TEST_F(MdbDictionaryRecoveryTest, ReinitializeOwnedAdapterPreservesDataAndChangesNamespace) {
  ASSERT_EQ(YBCMdbShutdown(), 0);
  const auto masters = cluster_->GetMasterAddresses();
  ASSERT_EQ(YBCMdbInitCluster(masters.c_str(), "mdb_p6_test"), 0);
  FirstBatchTypedOutput update;
  ASSERT_EQ(YBCMdbExecuteSparqlWithResults(
      "INSERT DATA { <urn:exit:s> <urn:exit:p> \"kept\"@en . }",
      update.Callbacks(), &update), 0);
  for (int round = 0; round < 3; ++round) {
    ASSERT_EQ(YBCMdbInitCluster(masters.c_str(), "mdb_p6_test"), 0);
    FirstBatchTypedOutput output;
    ASSERT_EQ(YBCMdbExecuteSparqlWithResults(
        "SELECT ?o WHERE { <urn:exit:s> <urn:exit:p> ?o }", output.Callbacks(), &output), 0);
    ASSERT_EQ(output.rows.size(), 1);
    ASSERT_EQ(std::get<1>(output.rows[0][0]), "kept");
    ASSERT_EQ(std::get<3>(output.rows[0][0]), "en");
  }
  auto workers = workers_; workers.batch_rows = 128; workers.page_rows = 16;
  RdfYBModel::Instance().SetWorkerOptions(workers);
  ASSERT_EQ(YBCMdbInitCluster(masters.c_str(), "mdb_exit_other"), 0);
  ASSERT_EQ(RdfYBModel::Instance().Namespace(), "mdb_exit_other");
  ASSERT_EQ(RdfYBModel::Instance().GetWorkerOptions().targets, workers.targets);
  FirstBatchTypedOutput empty;
  ASSERT_EQ(YBCMdbExecuteSparqlWithResults(
      "SELECT ?o WHERE { <urn:exit:s> <urn:exit:p> ?o }", empty.Callbacks(), &empty), 0);
  ASSERT_TRUE(empty.rows.empty());
  ASSERT_EQ(YBCMdbInitCluster(masters.c_str(), "mdb_p6_test"), 0);
  FirstBatchTypedOutput restored;
  ASSERT_EQ(YBCMdbExecuteSparqlWithResults(
      "SELECT ?o WHERE { <urn:exit:s> <urn:exit:p> ?o }", restored.Callbacks(), &restored), 0);
  ASSERT_EQ(restored.rows.size(), 1);
  ASSERT_EQ(std::get<1>(restored.rows[0][0]), "kept");
  ASSERT_EQ(YBCMdbShutdown(), 0);
}

TEST_F(MdbDictionaryRecoveryTest, FailedInitializationDropsPartialClientAndCanRecover) {
  PersistentIdMap dictionary;
  ASSERT_OK(dictionary.Init(client_.get(), "mdb_p6_test"));
  const auto id = dictionary.GetOrCreate("urn:exit:recover");
  ASSERT_OK(id);
  ASSERT_NO_FATALS(DictionaryRow("term:urn:exit:recover", 0));
  ASSERT_EQ(YBCMdbShutdown(), 0);
  const auto masters = cluster_->GetMasterAddresses();
  ASSERT_NE(YBCMdbInitCluster(masters.c_str(), "mdb_p6_test"), 0);
  ASSERT_FALSE(RdfYBModel::Instance().IsConnected());
  ASSERT_FALSE(RdfYBModel::Instance().IsInitialized());
  ASSERT_EQ(RdfYBModel::Instance().Client(), nullptr);
  ASSERT_NE(YBCMdbImportFile("/nonexistent-input.nt"), 0);
  ASSERT_NO_FATALS(DictionaryRow("term:urn:exit:recover", static_cast<int64_t>(*id)));
  ASSERT_EQ(YBCMdbInitCluster(masters.c_str(), "mdb_p6_test"), 0);
  ASSERT_TRUE(RdfYBModel::Instance().IsConnected());
  ASSERT_EQ(YBCMdbShutdown(), 0);
}

TEST_F(MdbDictionaryRecoveryTest, RepeatedRemoteQueriesAndFreshCliProcessesShutDown) {
  ASSERT_EQ(YBCMdbShutdown(), 0);
  ASSERT_EQ(YBCMdbInitCluster(cluster_->GetMasterAddresses().c_str(), "mdb_p6_test"), 0);
  const auto input = GetTestPath("exit-check.nt");
  std::set<std::pair<std::string, std::string>> expected;
  {
    std::ofstream file(input);
    for (size_t i = 0; i < 64; ++i) {
      const auto suffix = std::to_string(i);
      file << "<urn:exit:s" << suffix << "> <urn:exit:p1> <urn:exit:k" << suffix << "> .\n"
           << "<urn:exit:k" << suffix << "> <urn:exit:p2> <urn:exit:o" << suffix << "> .\n";
      expected.emplace("urn:exit:s" + suffix, "urn:exit:o" + suffix);
    }
  }
  ASSERT_EQ(YBCMdbImportFile(input.c_str()), 0);
  const char* query = "SELECT ?s ?o WHERE { ?s <urn:exit:p1> ?k . ?k <urn:exit:p2> ?o }";
  auto remote = workers_; remote.batch_rows = 128; remote.page_rows = 16;
  auto exchange = remote; exchange.exchange = ExchangeMode::Hash;
  const auto spill = GetTestPath("exit-spill"); std::filesystem::create_directory(spill);
  exchange.exchange_spill_directory = spill;
  const std::vector<WorkerOptions> modes = {WorkerOptions{}, remote, exchange};
  for (int round = 0; round < 6; ++round) {
    for (const auto& options : modes) {
      RdfYBModel::Instance().SetWorkerOptions(options);
      FirstBatchTypedOutput output;
      ASSERT_EQ(YBCMdbExecuteSparqlWithResults(query, output.Callbacks(), &output), 0);
      ASSERT_TRUE(output.ended); ASSERT_EQ(output.rows.size(), expected.size());
      const std::string profile = YBCMdbGetLastQueryProfile();
      ASSERT_EQ(Field(profile, "remote_coordinator_fallbacks"), 0);
      if (options.targets.empty()) ASSERT_EQ(Field(profile, "remote_fragments"), 0);
      else ASSERT_GT(Field(profile, "remote_fragments"), 0);
      if (options.exchange == ExchangeMode::Hash) ASSERT_EQ(Field(profile, "exchange_operators"), 1);
      std::set<std::pair<std::string, std::string>> actual;
      for (const auto& row : output.rows) {
        ASSERT_EQ(row.size(), 2);
        ASSERT_EQ(std::get<0>(row[0]), YBC_MDB_IRI); ASSERT_EQ(std::get<0>(row[1]), YBC_MDB_IRI);
        ASSERT_TRUE(actual.emplace(std::get<1>(row[0]), std::get<1>(row[1])).second);
      }
      ASSERT_EQ(actual, expected);
      if (round < 2) {
        auto args = Base(); args.insert(args.end(), {"--mdb_result_format=json", "--mdb_query_profile=true"});
        if (!options.targets.empty()) args.insert(args.end(), {
          "--mdb_worker_targets=local,local,local", "--mdb_worker_binary=" + workers_.binary,
          "--mdb_worker_batch_rows=128", "--mdb_worker_page_rows=16",
          "--mdb_exchange=" + std::string(options.exchange == ExchangeMode::None ? "none" : "hash"),
          "--mdb_exchange_spill_directory=" + spill});
        args.insert(args.end(), {"query", query});
        std::string text, error;
        const auto status = Subprocess::Call(args, &text, &error);
        ASSERT_TRUE(status.ok()) << status << error;
        const auto marker = error.find("MDB_QUERY_PROFILE ");
        ASSERT_NE(marker, std::string::npos) << error;
        const auto cli_profile = error.substr(marker + std::string("MDB_QUERY_PROFILE ").size());
        ASSERT_EQ(Field(cli_profile, "remote_coordinator_fallbacks"), 0);
        if (options.targets.empty()) ASSERT_EQ(Field(cli_profile, "remote_fragments"), 0);
        else ASSERT_GT(Field(cli_profile, "remote_fragments"), 0);
        if (options.exchange == ExchangeMode::Hash) ASSERT_EQ(Field(cli_profile, "exchange_operators"), 1);
        rapidjson::Document parsed; parsed.Parse(text.c_str());
        ASSERT_FALSE(parsed.HasParseError()); actual.clear();
        for (const auto& row : parsed["results"]["bindings"].GetArray())
          ASSERT_TRUE(actual.emplace(row["s"]["value"].GetString(), row["o"]["value"].GetString()).second);
        ASSERT_EQ(actual, expected);
      }
      ASSERT_TRUE(std::filesystem::is_empty(spill));
    }
  }
  ASSERT_EQ(YBCMdbShutdown(), 0);
  ASSERT_EQ(YBCMdbShutdown(), 0);
  ASSERT_FALSE(RdfYBModel::Instance().IsConnected());
}
}  // namespace
}  // namespace yb::mdb


namespace yb::mdb {
namespace {
class MdbDictionaryInitTest : public MdbDictionaryRecoveryTest {
 protected:
  using Entries = std::map<uint64_t, std::string>;
  static std::string Term(uint64_t value) { return "urn:startup:term:" + std::to_string(value); }
  void CreateLegacy() {
    client::YBSchemaBuilder schema;
    schema.AddColumn("pack_id")->Type(DataType::INT64)->HashPrimaryKey()->NotNull();
    schema.AddColumn("gmap_id")->Type(DataType::INT64)->NotNull();
    schema.AddColumn("uri")->Type(DataType::STRING)->NotNull();
    TableProperties properties; properties.SetTransactional(true); schema.SetTableProperties(properties);
    auto status = legacy_.Create(client::YBTableName(YQL_DATABASE_CQL, "mdb_p6_test", "rdf_idmap"),
                                 1, client_.get(), &schema);
    ASSERT_TRUE(status.ok() || status.IsAlreadyPresent()) << status;
    ASSERT_OK(legacy_.Open(client::YBTableName(YQL_DATABASE_CQL, "mdb_p6_test", "rdf_idmap"), client_.get()));
    ASSERT_OK(terms_.Open(client::YBTableName(YQL_DATABASE_CQL, "mdb_p6_test", "rdf_terms"), client_.get()));
  }
  void Seed(const Entries& entries, const std::set<uint64_t>& published) {
    ASSERT_NO_FATALS(CreateLegacy());
    auto session = client_->NewSession(MonoDelta::FromSeconds(30));
    std::vector<decltype(NewMdbInsertOp(legacy_, session))> pending;
    auto flush = [&] {
      ASSERT_OK(session->TEST_Flush());
      for (const auto& op : pending) ASSERT_EQ(op->response().status(), QLResponsePB::YQL_STATUS_OK);
      pending.clear();
      session->ResetArena(nullptr);
    };
    for (const auto& [id, key] : entries) {
      const auto identity = SPARQL::RdfTermIdentity::IsKey(key) ? SPARQL::RdfTermIdentity::Decode(key)
          : RdfIdentity(TripleTermType::URI, key);
      auto legacy = NewMdbInsertOp(legacy_, session);
      QLAddInt64HashValue(legacy->mutable_request(), static_cast<int64_t>(PackRdfIdentity(identity).id));
      legacy_.AddInt64ColumnValue(legacy->mutable_request(), "gmap_id", id);
      legacy_.AddStringColumnValue(legacy->mutable_request(), "uri", key);
      session->Apply(legacy); pending.push_back(legacy);
      if (published.count(id)) {
        auto claim = NewMdbInsertOp(terms_, session);
        QLAddStringHashValue(claim->mutable_request(), "term:" + key);
        terms_.AddInt64ColumnValue(claim->mutable_request(), "term_id", id);
        session->Apply(claim); pending.push_back(claim);
      }
      if (pending.size() >= 128) ASSERT_NO_FATALS(flush());
    }
    if (!pending.empty()) ASSERT_NO_FATALS(flush());
    ASSERT_NO_FATALS(DictionaryRow("counter", entries.empty() ? 0 : entries.rbegin()->first));
  }
  void Check(const IdMap& actual, const Entries& expected) {
    ASSERT_EQ(actual.Size(), expected.size());
    for (const auto& [id, key] : expected) {
      ASSERT_TRUE(actual.Contains(key)); ASSERT_EQ(actual.Get(key), id); ASSERT_EQ(actual.Lookup(id), key);
    }
  }
  bool HasClaim(const std::string& key) {
    client::TableIteratorOptions options; options.columns=std::vector<std::string>{"term_key", "term_id"};
    Status error; options.error_handler=[&](const Status& s) { error=s; };
    bool found=false;
    for (const auto& row : client::TableRange(terms_,options))
      if (row.column(0).string_value()=="term:"+key) found=true;
    if (!error.ok()) ADD_FAILURE() << error;
    return found;
  }
  void RunBenchmark(size_t count) {
    if (!std::getenv("MDB_DICT_INIT_BENCHMARK")) GTEST_SKIP() << "Opt-in startup benchmark";
    Entries expected; std::set<uint64_t> published;
    for (size_t id=1;id<=count;++id) {expected.emplace(id,Term(id));published.insert(id);}
    ASSERT_NO_FATALS(Seed(expected,published));
    for (int sample=-1;sample<5;++sample) {
      PersistentIdMap dictionary;
      const auto begin=WallMicros();
      const auto status=dictionary.Init(client_.get(),"mdb_p6_test");
      const auto elapsed=WallMicros()-begin;
      ASSERT_OK(status);
      const auto stats=dictionary.init_stats();
      ASSERT_EQ(stats.legacy_terms,count);ASSERT_EQ(stats.known_terms,count);ASSERT_EQ(stats.new_terms,0);
      ASSERT_LE(stats.conditional_claims,count);
      IdMap restored;ASSERT_OK(dictionary.Restore(&restored));ASSERT_NO_FATALS(Check(restored,expected));
      LOG(INFO) << "DICT_INIT_SAMPLE {\"terms\":" << count << ",\"sample\":" << sample
                << ",\"boundary\":\"dictionary_init\",\"elapsed_us\":" << elapsed
                << ",\"legacy_terms\":" << stats.legacy_terms
                << ",\"conditional_claims\":" << stats.conditional_claims
                << ",\"known_terms\":" << stats.known_terms << ",\"new_terms\":" << stats.new_terms
                << ",\"correct\":true}";
      if(sample>0)continue;
      auto args=Base();args.insert(args.end(),{"--mdb_result_format=json","--mdb_query_profile=true","query","ASK { }"});
      std::string output,error;const auto cli_begin=WallMicros();
      const auto cli_status=Subprocess::Call(args,&output,&error);const auto cli_us=WallMicros()-cli_begin;
      ASSERT_TRUE(cli_status.ok()) << cli_status << error;
      rapidjson::Document parsed;parsed.Parse(output.c_str());ASSERT_FALSE(parsed.HasParseError());
      ASSERT_TRUE(parsed.HasMember("boolean"));ASSERT_TRUE(parsed["boolean"].GetBool());
      const auto marker=error.find("MDB_CLI_TIMING ");ASSERT_NE(marker,std::string::npos) << error;
      const auto end=error.find('\n',marker);const auto timing=error.substr(marker+15,end-marker-15);
      rapidjson::Document metrics;metrics.Parse(timing.c_str());ASSERT_FALSE(metrics.HasParseError()) << timing;
      ASSERT_EQ(metrics["status"].GetInt(),0);
      LOG(INFO) << "DICT_INIT_SAMPLE {\"terms\":" << count << ",\"sample\":" << sample
                << ",\"boundary\":\"fresh_cli\",\"elapsed_us\":" << cli_us
                << ",\"cli\":" << timing << ",\"correct\":true}";
    }
  }
  client::TableHandle legacy_,terms_;
};

TEST_F(MdbDictionaryInitTest, Startup128) {RunBenchmark(128);}
TEST_F(MdbDictionaryInitTest, Startup1024) {RunBenchmark(1024);}
TEST_F(MdbDictionaryInitTest, Startup8192) {RunBenchmark(8192);}

TEST_F(MdbDictionaryInitTest, SparseTypedLegacyClaimsKeepExactIds) {
  const auto language=RdfDictionaryKey(RdfIdentity(TripleTermType::LITERAL,"lexical","@en"));
  const auto typed=RdfDictionaryKey(RdfIdentity(TripleTermType::LITERAL,"01","http://www.w3.org/2001/XMLSchema#integer"));
  const Entries expected={{3,Term(3)},{107,language},{9001,typed}};
  ASSERT_NO_FATALS(Seed(expected,{3,9001}));
  PersistentIdMap dictionary;ASSERT_OK(dictionary.Init(client_.get(),"mdb_p6_test"));
  IdMap restored;ASSERT_OK(dictionary.Restore(&restored));ASSERT_NO_FATALS(Check(restored,expected));
  ASSERT_EQ(dictionary.init_stats().known_terms,2);ASSERT_EQ(dictionary.init_stats().new_terms,1);
  const auto next=dictionary.GetOrCreate(Term(99999));ASSERT_OK(next);ASSERT_GT(*next,9001);
  PersistentIdMap reopened;ASSERT_OK(reopened.Init(client_.get(),"mdb_p6_test"));
  ASSERT_OK(reopened.Restore(&restored));
  for(const auto& [id,key]:expected)ASSERT_EQ(restored.Get(key),id);
}

TEST_F(MdbDictionaryInitTest, ConflictingIdDoesNotPublishAnotherLegacyClaim) {
  ASSERT_NO_FATALS(Seed({{50,Term(2)}},{}));
  ASSERT_NO_FATALS(DictionaryRow("term:"+Term(1),50));
  PersistentIdMap dictionary;ASSERT_TRUE(dictionary.Init(client_.get(),"mdb_p6_test").IsCorruption());
  ASSERT_FALSE(HasClaim(Term(2)));
  ASSERT_NOK(dictionary.GetOrCreate(Term(3)));
}

TEST_F(MdbDictionaryInitTest, ConcurrentMigrationAndAllocationsPreserveClaims) {
  Entries expected;for(uint64_t id=1;id<=8;++id)expected.emplace(id,Term(id));
  ASSERT_NO_FATALS(Seed(expected,{1,3,5,7}));
  constexpr size_t writers=4,rounds=8;
  std::vector<std::unique_ptr<client::YBClient>> clients;
  for(size_t i=0;i<writers;++i){auto client=client::YBClientBuilder().add_master_server_addr(cluster_->GetMasterAddresses()).Build();ASSERT_OK(client);clients.push_back(std::move(*client));}
  std::vector<Status> status(writers);std::vector<Entries> results(writers);
  std::atomic<size_t> ready{0};std::atomic<bool> go{false};TestThreadHolder threads;
  for(size_t i=0;i<writers;++i)threads.AddThreadFunctor([&,i]{
    ++ready;while(!go.load())SleepFor(MonoDelta::FromMilliseconds(1));
    PersistentIdMap dictionary;status[i]=dictionary.Init(clients[i].get(),"mdb_p6_test");if(!status[i].ok())return;
    for(size_t round=0;round<rounds;++round)for(const auto& term:{"urn:startup:shared:"+std::to_string(round),"urn:startup:writer:"+std::to_string(i)+":"+std::to_string(round)}){
      auto id=dictionary.GetOrCreate(term);if(!id.ok()){status[i]=id.status();return;}results[i][*id]=term;
    }
  });
  while(ready.load()!=writers)SleepFor(MonoDelta::FromMilliseconds(1));go=true;threads.JoinAll();
  IdMap combined;for(const auto& [id,key]:expected)ASSERT_TRUE(combined.Restore(id,key));
  for(size_t i=0;i<writers;++i){ASSERT_OK(status[i]);ASSERT_EQ(results[i].size(),rounds*2);for(const auto& [id,key]:results[i]){ASSERT_GT(id,8);ASSERT_TRUE(combined.Restore(id,key));}}
  ASSERT_EQ(combined.Size(),8+rounds*(writers+1));
  PersistentIdMap reopened;ASSERT_OK(reopened.Init(client_.get(),"mdb_p6_test"));IdMap restored;ASSERT_OK(reopened.Restore(&restored));
  ASSERT_EQ(restored.Size(),combined.Size());for(const auto& [id,key]:combined.Entries())ASSERT_EQ(restored.Get(key),id);
}
}  // namespace
}  // namespace yb::mdb


namespace yb::mdb {
namespace {
class MdbDictionaryScanTest : public MdbDictionaryInitTest {
 protected:
  void RunRefreshBenchmark(size_t count) {
    if (!std::getenv("MDB_DICT_SCAN_BENCHMARK")) GTEST_SKIP() << "Opt-in dictionary refresh benchmark";
    Entries expected;
    std::set<uint64_t> published;
    for (size_t id = 1; id <= count; ++id) {
      expected.emplace(id, Term(id));
      published.insert(id);
    }
    ASSERT_NO_FATALS(Seed(expected, published));
    PersistentIdMap dictionary;
    ASSERT_OK(dictionary.Init(client_.get(), "mdb_p6_test"));
    IdMap restored;
    ASSERT_OK(dictionary.Restore(&restored));
    for (int sample = -1; sample < 5; ++sample) {
      const auto begin = WallMicros();
      const auto status = dictionary.Restore(&restored);
      const auto elapsed = WallMicros() - begin;
      ASSERT_OK(status);
      ASSERT_NO_FATALS(Check(restored, expected));
      const auto stats = dictionary.scan_stats();
      ASSERT_EQ(stats.rows, count + 1);
      LOG(INFO) << "DICT_SCAN_SAMPLE {\"terms\":" << count << ",\"sample\":" << sample
                << ",\"elapsed_us\":" << elapsed << ",\"rows\":" << stats.rows
                << ",\"page_counts_available\":" << (stats.page_counts_available ? "true" : "false")
                << ",\"page_read_operations\":" << stats.page_read_operations
                << ",\"response_bytes\":" << stats.response_bytes << ",\"correct\":true}";
    }
  }

  void CreateClaims(const std::string& ns, int tablets, const Entries& expected) {
    ASSERT_OK(client_->CreateNamespaceIfNotExists(ns, YQL_DATABASE_CQL));
    client::YBSchemaBuilder schema;
    schema.AddColumn("term_key")->Type(DataType::STRING)->HashPrimaryKey()->NotNull();
    schema.AddColumn("term_id")->Type(DataType::INT64)->NotNull();
    ASSERT_OK(claims_.Create(client::YBTableName(YQL_DATABASE_CQL, ns, "rdf_terms"),
                             tablets, client_.get(), &schema));
    auto session = client_->NewSession(MonoDelta::FromSeconds(30));
    std::vector<decltype(NewMdbInsertOp(claims_, session))> pending;
    auto flush = [&] {
      ASSERT_OK(session->TEST_Flush());
      for (const auto& op : pending) ASSERT_EQ(op->response().status(), QLResponsePB::YQL_STATUS_OK);
      pending.clear();
      session->ResetArena(nullptr);
    };
    for (const auto& [id, key] : expected) {
      auto op = NewMdbInsertOp(claims_, session);
      QLAddStringHashValue(op->mutable_request(), "term:" + key);
      claims_.AddInt64ColumnValue(op->mutable_request(), "term_id", id);
      session->Apply(op);
      pending.push_back(op);
      if (pending.size() == 128) ASSERT_NO_FATALS(flush());
    }
    if (!pending.empty()) ASSERT_NO_FATALS(flush());
    ASSERT_NO_FATALS(ClaimRow("counter", expected.empty() ? 0 : expected.rbegin()->first));
  }
  void ClaimRow(const std::string& key, int64_t value) {
    auto session = client_->NewSession(MonoDelta::FromSeconds(30));
    auto op = NewMdbInsertOp(claims_, session);
    QLAddStringHashValue(op->mutable_request(), key);
    claims_.AddInt64ColumnValue(op->mutable_request(), "term_id", value);
    ASSERT_OK(session->TEST_ApplyAndFlush(op));
    ASSERT_EQ(op->response().status(), QLResponsePB::YQL_STATUS_OK);
  }
  Entries MakeTerms(size_t count) {
    Entries expected;
    for (size_t i = 1; i <= count; ++i) expected.emplace(i * 3, Term(i));
    return expected;
  }
  client::TableHandle claims_;
};

TEST_F(MdbDictionaryScanTest, Refresh128) { RunRefreshBenchmark(128); }
TEST_F(MdbDictionaryScanTest, Refresh8192) { RunRefreshBenchmark(8192); }

TEST_F(MdbDictionaryScanTest, EmptyAndPageEdgesPreserveEveryMapping) {
  for (size_t n : {0, 1, 127, 128, 129, 1023, 1024, 1025, 2048}) {
    const auto ns = "mdb_scan_edge_" + std::to_string(n);
    const auto expected = MakeTerms(n);
    ASSERT_NO_FATALS(CreateClaims(ns, 1, expected));
    PersistentIdMap dictionary;
    ASSERT_OK(dictionary.Init(client_.get(), ns));
    IdMap restored;
    ASSERT_OK(dictionary.Restore(&restored));
    ASSERT_NO_FATALS(Check(restored, expected));
    const auto stats = dictionary.scan_stats();
    ASSERT_TRUE(stats.page_counts_available);
    ASSERT_EQ(stats.rows, n + 1);
    ASSERT_GE(stats.page_read_operations * 1024, stats.rows);
  }
}

TEST_F(MdbDictionaryScanTest, TypedAndLargeTermsAcrossFourTablets) {
  auto expected = MakeTerms(2051);
  expected[3] = RdfDictionaryKey(RdfIdentity(TripleTermType::LITERAL,
      std::string{'c', 'a', 'f', char(0xc3), char(0xa9)}, "@EN"));
  expected[6] = RdfDictionaryKey(RdfIdentity(TripleTermType::LITERAL, "01",
      "http://www.w3.org/2001/XMLSchema#integer"));
  expected[9] = RdfDictionaryKey(RdfIdentity(TripleTermType::BLANK, "b", "", "scan-scope"));
  expected[12] = RdfDictionaryKey(RdfIdentity(TripleTermType::LITERAL, std::string(8192, 'x')));
  ASSERT_NO_FATALS(CreateClaims("mdb_scan_four", 4, expected));
  PersistentIdMap dictionary;
  ASSERT_OK(dictionary.Init(client_.get(), "mdb_scan_four"));
  IdMap restored;
  ASSERT_OK(dictionary.Restore(&restored));
  ASSERT_NO_FATALS(Check(restored, expected));
  ASSERT_EQ(dictionary.scan_stats().rows, expected.size() + 1);
  ASSERT_GT(dictionary.scan_stats().page_read_operations, 1);
  auto next = dictionary.GetOrCreate("urn:scan:after-four-tablets");
  ASSERT_OK(next);
  ASSERT_GT(*next, expected.rbegin()->first);
}

TEST_F(MdbDictionaryScanTest, LaterPageCorruptionKeepsDestinationUnchanged) {
  const auto expected = MakeTerms(2050);
  ASSERT_NO_FATALS(CreateClaims("mdb_scan_corrupt", 1, expected));
  PersistentIdMap dictionary;
  ASSERT_OK(dictionary.Init(client_.get(), "mdb_scan_corrupt"));
  IdMap restored;
  ASSERT_OK(dictionary.Restore(&restored));
  std::string last_term;
  Status scan_error;
  client::TableIteratorOptions options;
  options.columns = std::vector<std::string>{"term_key"};
  options.error_handler = [&](const Status& s) { scan_error = s; };
  for (const auto& row : client::TableRange(claims_, options)) {
    const auto key = row.column(0).string_value();
    if (key != "counter") last_term = key;
  }
  ASSERT_OK(scan_error);
  ASSERT_FALSE(last_term.empty());
  ASSERT_NO_FATALS(ClaimRow(last_term, 0));
  ASSERT_TRUE(dictionary.Restore(&restored).IsCorruption());
  ASSERT_GT(dictionary.scan_stats().rows, 1024);
  ASSERT_NO_FATALS(Check(restored, expected));
}

TEST_F(MdbDictionaryScanTest, PublishedClaimIsVisibleWithoutAnotherCounterAdvance) {
  auto expected = MakeTerms(1025);
  ASSERT_NO_FATALS(CreateClaims("mdb_scan_reserved", 1, expected));
  PersistentIdMap dictionary;
  ASSERT_OK(dictionary.Init(client_.get(), "mdb_scan_reserved"));
  IdMap restored;
  ASSERT_OK(dictionary.Restore(&restored));
  ASSERT_NO_FATALS(ClaimRow("counter", 50000));
  ASSERT_OK(dictionary.Restore(&restored));
  ASSERT_NO_FATALS(ClaimRow("term:urn:scan:delayed-claim", 50000));
  expected.emplace(50000, "urn:scan:delayed-claim");
  ASSERT_OK(dictionary.Restore(&restored));
  ASSERT_NO_FATALS(Check(restored, expected));
}
}  // namespace
}  // namespace yb::mdb


namespace yb::mdb {
namespace {
TEST_F(MdbDictionaryScanTest, EmptyTabletsBeforeLastPartitionDoNotEndScan) {
  ASSERT_NO_FATALS(CreateClaims("mdb_scan_empty_tablets", 4, {}));
  auto session = client_->NewSession(MonoDelta::FromSeconds(30));
  std::string last_partition_term;
  for (size_t i = 0; i < 1024; ++i) {
    auto op = NewMdbReadOp(claims_, session);
    const auto term = Term(i);
    QLAddStringHashValue(op->mutable_request(), "term:" + term);
    std::string encoded;
    ASSERT_OK(op->GetPartitionKey(&encoded));
    ASSERT_EQ(encoded.size(), 2);
    if (static_cast<unsigned char>(encoded[0]) >= 192) {
      last_partition_term = term;
      break;
    }
  }
  ASSERT_FALSE(last_partition_term.empty());
  ASSERT_NO_FATALS(ClaimRow("term:" + last_partition_term, 3));
  ASSERT_NO_FATALS(ClaimRow("counter", 3));
  PersistentIdMap dictionary;
  ASSERT_OK(dictionary.Init(client_.get(), "mdb_scan_empty_tablets"));
  IdMap restored;
  ASSERT_OK(dictionary.Restore(&restored));
  ASSERT_NO_FATALS(Check(restored, {{3, last_partition_term}}));
  ASSERT_EQ(dictionary.scan_stats().rows, 2);
}
}  // namespace
}  // namespace yb::mdb


namespace yb::mdb {
namespace {
class MdbWorkerProbeTest : public MdbFragmentIntegrationTest {
 protected:
  using Pattern = std::array<int64_t, 3>;
  using Rows = std::multiset<std::vector<uint64_t>>;
  Result<FragmentPlan> Plan(const std::vector<Pattern>& triples, uint32_t variables,
                            const std::vector<uint32_t>& projection, size_t page, size_t batch) {
    FragmentPlan plan;
    auto& open = plan.physical;
    open.set_master_addresses(cluster_->GetMasterAddresses());
    open.set_keyspace("mdb_p6_test");
    for (int perm = 0; perm < 6; ++perm)
      open.add_table_ids(RdfYBModel::Instance().GetTable(perm)->table()->id());
    open.set_variable_count(variables);
    for (auto variable : projection) open.add_projection(variable);
    open.set_page_rows(page); open.set_batch_rows(batch);
    open.set_memory_bytes(workers_.memory_bytes);
    for (const auto& pattern : triples) {
      auto* triple = open.add_triples();
      for (auto value : pattern) {
        auto* term = triple->add_terms();
        if (value < 0) term->set_variable(-value - 1);
        else term->set_constant(value);
      }
    }
    TabletRouter router;
    RETURN_NOT_OK(router.Init(client_.get(), "mdb_p6_test"));
    plan.tasks = router.GetTablets(0);
    return plan;
  }
  Status ReadView(const FragmentPlan& plan, DocDBReadContext& view, Rows* rows,
                  uint64_t* reads, size_t max_workers = 3) {
    auto options = workers_;
    options.batch_rows = plan.physical.batch_rows();
    options.page_rows = plan.physical.page_rows();
    options.max_workers = max_workers;
    auto status = GatherFragments(plan, options, view, [&](const std::vector<uint64_t>& row) {
      rows->insert(row);
      return Status::OK();
    });
    *reads = view.stats.remote_read_operations;
    if (status.ok() && view.stats.remote_fragments != 4)
      return STATUS(Corruption, "Probe test did not run all four remote fragments");
    return status;
  }
  Status Read(const FragmentPlan& plan, Rows* rows, uint64_t* reads, size_t workers = 3) {
    auto view = VERIFY_RESULT(RdfYBModel::Instance().NewQueryReadContext());
    return ReadView(plan, *view, rows, reads, workers);
  }
};

TEST_F(MdbWorkerProbeTest, OnePageReuseKeepsOuterBagAcrossCredits) {
  Seed(96);
  const auto plan = Plan({{-1,9000,-2},{-2,9001,-3}}, 3, {0,2}, 4, 1);
  ASSERT_OK(plan);
  Rows expected;
  for (uint64_t s = 101; s < 197; ++s)
    for (uint64_t o : {1001,1002}) expected.insert({s,o});
  for (size_t workers : {1,3}) {
    Rows actual; uint64_t reads = 0;
    ASSERT_OK(Read(*plan, &actual, &reads, workers));
    ASSERT_EQ(actual, expected);
    ASSERT_LT(reads, 64);
  }
}

TEST_F(MdbWorkerProbeTest, EmptyReuseSharesSnapshotAndDoesNotSurviveNewFragment) {
  Seed(96);
  const auto plan = Plan({{-1,9000,-2},{-2,9002,-3}}, 3, {0,2}, 4, 1);
  ASSERT_OK(plan);
  auto old_view = RdfYBModel::Instance().NewQueryReadContext();
  ASSERT_OK(old_view);
  Rows empty; uint64_t reads = 0;
  ASSERT_OK(Read(*plan, &empty, &reads));
  ASSERT_TRUE(empty.empty()); ASSERT_LT(reads, 64);
  Write(1,9002,1003);
  ASSERT_OK(RdfYBModel::Instance().Flush());
  Rows snapshot;
  ASSERT_OK(ReadView(*plan, **old_view, &snapshot, &reads));
  ASSERT_TRUE(snapshot.empty()); ASSERT_LT(reads, 64);
  Rows fresh, expected;
  for (uint64_t s = 101; s < 197; ++s) expected.insert({s,1003});
  ASSERT_OK(Read(*plan, &fresh, &reads));
  ASSERT_EQ(fresh, expected); ASSERT_LT(reads, 64);
}

TEST_F(MdbWorkerProbeTest, MultiPageProbeNeverReusesOnlyItsTail) {
  Seed(32);
  for (uint64_t o = 1003; o < 1018; ++o) Write(1,9001,o);
  ASSERT_OK(RdfYBModel::Instance().Flush());
  const auto plan = Plan({{-1,9000,-2},{-2,9001,-3}}, 3, {0,2}, 4, 2);
  ASSERT_OK(plan);
  Rows expected;
  for (uint64_t s = 101; s < 133; ++s)
    for (uint64_t o = 1001; o < 1018; ++o) expected.insert({s,o});
  Rows actual; uint64_t reads = 0;
  ASSERT_OK(Read(*plan, &actual, &reads));
  ASSERT_EQ(actual, expected);
  ASSERT_GT(reads, 100);
}

TEST_F(MdbWorkerProbeTest, ChangedProbeAndRepeatedVariablesKeepFreshBindings) {
  Seed(32);
  for (uint64_t s = 102; s < 133; s += 2) Write(s,9000,2);
  Write(2,9001,2001); Write(2,9001,2002);
  Write(1001,9002,1001); Write(1001,9002,2001);
  Write(1002,9002,1002); Write(2001,9002,2001); Write(2002,9002,2003);
  ASSERT_OK(RdfYBModel::Instance().Flush());
  const auto plan = Plan({{-1,9000,-2},{-2,9001,-3},{-4,9002,-4}}, 4, {0,2,3}, 8, 1);
  ASSERT_OK(plan);
  Rows expected;
  for (uint64_t s = 101; s < 133; ++s) {
    for (uint64_t o : {1001,1002})
      for (uint64_t x : {1001,1002,2001}) expected.insert({s,o,x});
    if (s % 2 == 0) for (uint64_t o : {2001,2002})
      for (uint64_t x : {1001,1002,2001}) expected.insert({s,o,x});
  }
  Rows actual; uint64_t reads = 0;
  ASSERT_OK(Read(*plan, &actual, &reads));
  ASSERT_EQ(actual, expected);
}

TEST_F(MdbWorkerProbeTest, ExactPageAndMinimumBudgetPreserveResultsAndRecovery) {
  Seed(32);
  auto plan = Plan({{-1,9000,-2},{-2,9001,-3}}, 3, {0,2}, 2, 1);
  ASSERT_OK(plan);
  const uint64_t reserved = 4 * 1024 * 1024 + 2 * (3 * 8 + 2 * 96 + 4096);
  plan->physical.set_memory_bytes(reserved);
  Rows expected;
  for (uint64_t s = 101; s < 133; ++s)
    for (uint64_t o : {1001,1002}) expected.insert({s,o});
  Rows actual; uint64_t reads = 0;
  ASSERT_OK(Read(*plan, &actual, &reads)); ASSERT_EQ(actual, expected);
  plan->physical.set_memory_bytes(reserved - 1);
  Rows rejected;
  ASSERT_NOK(Read(*plan, &rejected, &reads)); ASSERT_TRUE(rejected.empty());
  plan->physical.set_memory_bytes(reserved);
  Rows recovered;
  ASSERT_OK(Read(*plan, &recovered, &reads)); ASSERT_EQ(recovered, expected);
}
}  // namespace
}  // namespace yb::mdb


namespace yb::mdb {
namespace {
TEST_F(MdbWorkerProbeTest, CacheHitsStillHonorCancellationAndDeadline) {
  Seed(96);
  const auto plan = Plan({{-1,9000,-2},{-2,9001,-3}}, 3, {0,2}, 4, 1);
  ASSERT_OK(plan);
  auto options = workers_;
  options.page_rows = 4; options.batch_rows = 1;
  for (bool cancel : {true, false}) {
    auto view = RdfYBModel::Instance().NewQueryReadContext();
    ASSERT_OK(view);
    size_t callbacks = 0;
    const auto status = GatherFragments(*plan, options, **view, [&](const auto&) {
      if (++callbacks == 7) {
        if (cancel) (*view)->cancelled = true;
        else (*view)->deadline = CoarseMonoClock::Now();
      }
      return Status::OK();
    });
    ASSERT_NOK(status);
    ASSERT_EQ(callbacks, 7);
    // Three streams have consumed their first two rows; the next row reused
    // a complete inner page and did not perform a seventh storage read.
    ASSERT_LE((*view)->stats.remote_read_operations, 6);
  }
  Rows actual, expected; uint64_t reads = 0;
  for (uint64_t s = 101; s < 197; ++s)
    for (uint64_t o : {1001,1002}) expected.insert({s,o});
  ASSERT_OK(Read(*plan, &actual, &reads));
  ASSERT_EQ(actual, expected); ASSERT_LT(reads, 64);
}
}  // namespace
}  // namespace yb::mdb


namespace yb::mdb {
namespace {
class MdbWorkerBatchTest : public MdbWorkerProbeTest {
 protected:
  Rows SeedUnique(size_t count, size_t fanout = 1) {
    Rows expected;
    for (uint64_t s = 101; s < 101 + count; ++s) {
      Write(s,9000,10000+s);
      for (uint64_t j = 0; j < fanout; ++j) {
        Write(10000+s,9001,20000+j);
        expected.insert({s,20000+j});
      }
    }
    return expected;
  }
};

TEST_F(MdbWorkerBatchTest, DifferentKeysPreserveEveryRowAcrossCredits) {
  const auto expected = SeedUnique(128);
  ASSERT_OK(RdfYBModel::Instance().Flush());
  const auto plan = Plan({{-1,9000,-2},{-2,9001,-3}},3,{0,2},16,1);
  ASSERT_OK(plan);
  for (size_t workers : {1,3}) {
    Rows actual; uint64_t reads = 0;
    ASSERT_OK(Read(*plan,&actual,&reads,workers));
    ASSERT_EQ(actual,expected);
  }
}

TEST_F(MdbWorkerBatchTest, RepeatedParentVariablesAndMissingProbesKeepBag) {
  Rows expected;
  for (uint64_t s = 101; s < 197; ++s) {
    Write(s,9000,s+10000);
    if (s % 2 == 0) {
      Write(s,9000,s);
      if (s % 4 == 0) {
        Write(s,9001,20001); Write(s,9001,20002);
        expected.insert({s,20001}); expected.insert({s,20002});
      }
    }
  }
  ASSERT_OK(RdfYBModel::Instance().Flush());
  const auto plan=Plan({{-1,9000,-1},{-1,9001,-2}},2,{0,1},16,1);
  ASSERT_OK(plan);
  Rows actual; uint64_t reads=0;
  ASSERT_OK(Read(*plan,&actual,&reads)); ASSERT_EQ(actual,expected);
}

TEST_F(MdbWorkerBatchTest, DifferentMultiPageProbesRetainTheirOwnContinuations) {
  const auto expected=SeedUnique(32,17);
  ASSERT_OK(RdfYBModel::Instance().Flush());
  const auto plan=Plan({{-1,9000,-2},{-2,9001,-3}},3,{0,2},4,2);
  ASSERT_OK(plan);
  Rows actual; uint64_t reads=0;
  ASSERT_OK(Read(*plan,&actual,&reads)); ASSERT_EQ(actual,expected);
}

TEST_F(MdbWorkerBatchTest, ParentPageReuseRebindsPrefixBeforeLeafLookahead) {
  Seed(64);
  Rows expected;
  for (uint64_t s=101;s<165;++s) {
    const uint64_t middle=s%2 ? 1001 : 1002;
    Write(middle,9002,s); expected.insert({s,middle});
  }
  ASSERT_OK(RdfYBModel::Instance().Flush());
  const auto plan=Plan({{-1,9000,-2},{-2,9001,-3},{-3,9002,-1}},3,{0,2},4,1);
  ASSERT_OK(plan);
  Rows actual; uint64_t reads=0;
  ASSERT_OK(Read(*plan,&actual,&reads)); ASSERT_EQ(actual,expected);
}

TEST_F(MdbWorkerBatchTest, ExactMinimumBudgetFallsBackWithoutDroppingRows) {
  const auto expected=SeedUnique(32,3);
  ASSERT_OK(RdfYBModel::Instance().Flush());
  auto plan=Plan({{-1,9000,-2},{-2,9001,-3}},3,{0,2},4,1);
  ASSERT_OK(plan);
  const auto minimum=FragmentBufferedReservation(plan->physical);
  plan->physical.set_memory_bytes(minimum);
  Rows actual; uint64_t reads=0;
  ASSERT_OK(Read(*plan,&actual,&reads)); ASSERT_EQ(actual,expected);
  plan->physical.set_memory_bytes(minimum-1);
  Rows rejected; ASSERT_NOK(Read(*plan,&rejected,&reads)); ASSERT_TRUE(rejected.empty());
  plan->physical.set_memory_bytes(workers_.memory_bytes);
  Rows recovered; ASSERT_OK(Read(*plan,&recovered,&reads)); ASSERT_EQ(recovered,expected);
}

TEST_F(MdbWorkerBatchTest, CancelledOrExpiredPrefetchCanRecover) {
  const auto expected=SeedUnique(96);
  ASSERT_OK(RdfYBModel::Instance().Flush());
  const auto plan=Plan({{-1,9000,-2},{-2,9001,-3}},3,{0,2},16,1);
  ASSERT_OK(plan);
  auto options=workers_; options.page_rows=16; options.batch_rows=1;
  for (bool cancel : {true,false}) {
    auto view=RdfYBModel::Instance().NewQueryReadContext(); ASSERT_OK(view);
    size_t callbacks=0;
    auto status=GatherFragments(*plan,options,**view,[&](const auto&) {
      if (++callbacks==1) {
        if(cancel) (*view)->cancelled=true;
        else (*view)->deadline=CoarseMonoClock::Now();
      }
      return Status::OK();
    });
    ASSERT_NOK(status); ASSERT_EQ(callbacks,1);
  }
  Rows actual; uint64_t reads=0;
  ASSERT_OK(Read(*plan,&actual,&reads)); ASSERT_EQ(actual,expected);
}
}  // namespace
}  // namespace yb::mdb


namespace yb::mdb {
namespace {
TEST_F(MdbWorkerBatchTest, ConcurrentCommitDoesNotMoveLookaheadReadView) {
  const auto expected=SeedUnique(96);
  ASSERT_OK(RdfYBModel::Instance().Flush());
  const auto plan=Plan({{-1,9000,-2},{-2,9001,-3}},3,{0,2},16,1);
  ASSERT_OK(plan);
  auto view=RdfYBModel::Instance().NewQueryReadContext(); ASSERT_OK(view);
  auto fresh_expected=expected;
  for(uint64_t s=101;s<197;++s) {
    Write(10000+s,9001,30000); fresh_expected.insert({s,30000});
  }
  ASSERT_OK(RdfYBModel::Instance().Flush());
  Rows snapshot; uint64_t reads=0;
  ASSERT_OK(ReadView(*plan,**view,&snapshot,&reads)); ASSERT_EQ(snapshot,expected);
  Rows fresh;
  ASSERT_OK(Read(*plan,&fresh,&reads)); ASSERT_EQ(fresh,fresh_expected);
}
}  // namespace
}  // namespace yb::mdb


namespace yb::mdb {
namespace {
class MdbWorkerExitTest : public MdbWorkerProbeTest {
 protected:
  Result<FragmentPlan> WholeTablePlan() {
    auto plan = VERIFY_RESULT(Plan({{-1,9000,-2},{-2,9001,-3}}, 3, {0,2}, 4, 128));
    // One task spanning the table isolates EOF cleanup from scheduling other tasks.
    plan.tasks.resize(1);
    plan.tasks.front().partition_end.clear();
    return plan;
  }
  std::string Wrapper(const std::string& suffix) {
    const auto path = GetTestPath("worker-exit-wrapper");
    std::ofstream file(path);
    file << "#!/bin/sh\n'" << workers_.binary << "' \"$@\"\n"
            "rc=$?\n[ \"$rc\" -eq 0 ] || exit \"$rc\"\n" << suffix;
    file.close();
    std::filesystem::permissions(path, std::filesystem::perms::owner_all);
    return path;
  }
  Status Run(const FragmentPlan& plan, DocDBReadContext& view, const std::string& binary,
             Rows* rows, std::atomic<bool>* delivered = nullptr) {
    auto options = workers_;
    options.binary = binary;
    options.page_rows = 4; options.batch_rows = 128; options.max_workers = 1;
    return GatherFragments(plan, options, view, [&](const auto& row) {
      rows->insert(row);
      if (delivered && rows->size() == 6) *delivered = true;
      return Status::OK();
    });
  }
  void CheckReaped(const std::string& profile_pids) {
    std::istringstream pids(profile_pids);
    std::string pid;
    while (std::getline(pids, pid, ',')) {
      errno = 0;
      ASSERT_EQ(kill(static_cast<pid_t>(std::stoll(pid)), 0), -1);
      ASSERT_EQ(errno, ESRCH);
    }
  }
};

TEST_F(MdbWorkerExitTest, SuccessfulEofWaitsForNaturalExit) {
  Seed(3);
  auto plan = WholeTablePlan(); ASSERT_OK(plan);
  const auto marker = GetTestPath("worker-completed");
  const auto binary = Wrapper("sleep 0.1\nprintf '%s\\n' \"$$\" > '" + marker + "'\nexit 0\n");
  auto view = RdfYBModel::Instance().NewQueryReadContext(); ASSERT_OK(view);
  Rows rows, expected;
  for (uint64_t s = 101; s < 104; ++s)
    for (uint64_t o : {1001,1002}) expected.insert({s,o});
  ASSERT_OK(Run(*plan, **view, binary, &rows));
  ASSERT_EQ(rows, expected);
  ASSERT_TRUE(std::filesystem::exists(marker));
  ASSERT_NO_FATALS(CheckReaped((*view)->stats.remote_worker_pids));
  std::ifstream completed(marker); std::string pid; completed >> pid;
  ASSERT_FALSE(pid.empty()); ASSERT_NO_FATALS(CheckReaped(pid));
}

TEST_F(MdbWorkerExitTest, NonzeroOrSignalAfterEofFailsAndRecovers) {
  Seed(3);
  auto plan = WholeTablePlan(); ASSERT_OK(plan);
  for (const auto& ending : {"exit 23\n", "kill -TERM $$\n"}) {
    auto view = RdfYBModel::Instance().NewQueryReadContext(); ASSERT_OK(view);
    Rows rows;
    const auto status = Run(*plan, **view, Wrapper(ending), &rows);
    ASSERT_TRUE(status.IsRuntimeError()) << status;
    ASSERT_NE(status.ToString().find("non-zero exit status"), std::string::npos) << status;
    ASSERT_EQ(rows.size(), 6);
    ASSERT_NO_FATALS(CheckReaped((*view)->stats.remote_worker_pids));
  }
  auto fresh = Query(kQuery, workers_);
  ASSERT_EQ(fresh.status, 0); ASSERT_EQ(fresh.values.size(), 6);
}

TEST_F(MdbWorkerExitTest, EofCleanupHonorsDeadlineAndReaps) {
  Seed(3);
  auto plan = WholeTablePlan(); ASSERT_OK(plan);
  const auto marker = GetTestPath("worker-waiting");
  const auto binary = Wrapper("printf '%s\\n' \"$$\" > '" + marker + "'\nexec /bin/sleep 30\n");
  auto view = RdfYBModel::Instance().NewQueryReadContext(); ASSERT_OK(view);
  (*view)->deadline = CoarseMonoClock::Now() + MonoDelta::FromSeconds(5);
  const auto begin = std::chrono::steady_clock::now();
  Rows rows; const auto status = Run(*plan, **view, binary, &rows);
  ASSERT_TRUE(status.IsTimedOut()) << status;
  ASSERT_NE(status.ToString().find("worker exit"), std::string::npos) << status;
  ASSERT_EQ(rows.size(), 6); ASSERT_TRUE(std::filesystem::exists(marker));
  ASSERT_LT(std::chrono::steady_clock::now() - begin, std::chrono::seconds(8));
  std::ifstream waiting(marker); std::string pid; waiting >> pid;
  ASSERT_FALSE(pid.empty()); ASSERT_NO_FATALS(CheckReaped(pid));
  ASSERT_NO_FATALS(CheckReaped((*view)->stats.remote_worker_pids));
  const auto recovered = Query(kQuery, workers_);
  ASSERT_EQ(recovered.status, 0); ASSERT_EQ(recovered.values.size(), 6);
}

TEST_F(MdbWorkerExitTest, EofCleanupHonorsCancellationAndReaps) {
  Seed(3);
  auto plan = WholeTablePlan(); ASSERT_OK(plan);
  const auto marker = GetTestPath("worker-waiting");
  const auto binary = Wrapper("printf '%s\\n' \"$$\" > '" + marker + "'\nexec /bin/sleep 30\n");
  auto view = RdfYBModel::Instance().NewQueryReadContext(); ASSERT_OK(view);
  std::atomic<bool> stop{false}, seen{false}, delivered{false};
  TestThreadHolder cancellation;
  cancellation.AddThreadFunctor([&] {
    while (!stop.load()) {
      if (delivered.load() && std::filesystem::exists(marker)) {
        seen = true; (*view)->cancelled = true; break;
      }
      SleepFor(MonoDelta::FromMilliseconds(1));
    }
  });
  const auto begin = std::chrono::steady_clock::now();
  Rows rows; const auto status = Run(*plan, **view, binary, &rows, &delivered);
  stop = true; cancellation.JoinAll();
  ASSERT_TRUE(seen.load()); ASSERT_TRUE(status.IsAborted()) << status;
  ASSERT_NE(status.ToString().find("worker exit"), std::string::npos) << status;
  ASSERT_EQ(rows.size(), 6);
  ASSERT_LT(std::chrono::steady_clock::now() - begin, std::chrono::seconds(5));
  std::ifstream waiting(marker); std::string pid; waiting >> pid;
  ASSERT_FALSE(pid.empty()); ASSERT_NO_FATALS(CheckReaped(pid));
  ASSERT_NO_FATALS(CheckReaped((*view)->stats.remote_worker_pids));
  const auto recovered = Query(kQuery, workers_);
  ASSERT_EQ(recovered.status, 0); ASSERT_EQ(recovered.values.size(), 6);
}
}  // namespace
}  // namespace yb::mdb


namespace yb::mdb {
namespace {
class MdbReadyGatherTest : public MdbWorkerProbeTest {
 protected:
  std::string Relay(const std::string& release, const std::string& expired, size_t prefix, bool gate_open = false) {
    const auto path = GetTestPath("ready-relay-" + std::to_string(prefix));
    std::ofstream file(path);
    file << "#!/usr/bin/python3\nimport os,struct,subprocess,sys,time\n"
            "worker='" << workers_.binary << "'\nrelease='" << release << "'\nexpired='" << expired << "'\nprefix=" << prefix << "\ngate_open=" << (gate_open ? "True" : "False") << R"PY(
def exact(stream,n):
    out=b''
    while len(out)<n:
        part=stream.read(n-len(out))
        if not part: raise EOFError('truncated frame')
        out+=part
    return out
def frame(stream):
    h=exact(stream,4);n=struct.unpack('!I',h)[0]
    if not 0<n<=1048576: raise ValueError('frame limit')
    return h+exact(stream,n)
def varint(data,pos):
    value=0;shift=0
    while True:
        b=data[pos];pos+=1;value|=(b&127)<<shift
        if b<128:return value,pos
        shift+=7
def number(data,field):
    pos=0
    while pos<len(data):
        tag,pos=varint(data,pos);wire=tag&7
        if wire==0:
            value,pos=varint(data,pos)
            if tag>>3==field:return value
        elif wire==1:pos+=8
        elif wire==2:n,pos=varint(data,pos);pos+=n
        elif wire==5:pos+=4
        else:raise ValueError('wire type')
    return 0
p=subprocess.Popen([worker]+sys.argv[1:],stdin=subprocess.PIPE,stdout=subprocess.PIPE)
first=True
try:
    while True:
        request=frame(sys.stdin.buffer);p.stdin.write(request);p.stdin.flush()
        response=frame(p.stdout)
        eof=number(response[4:],9)
        fragment=number(request[4:],3)
        if fragment==0 and first==gate_open and not os.path.exists(release):
            sys.stdout.buffer.write(response[:prefix]);sys.stdout.buffer.flush()
            until=time.monotonic()+2
            while not os.path.exists(release) and time.monotonic()<until:time.sleep(.001)
            if not os.path.exists(release):open(expired,'w').close()
            response=response[prefix:]
        first=False
        sys.stdout.buffer.write(response);sys.stdout.buffer.flush()
        if eof:break
    p.stdin.close();sys.exit(p.wait())
finally:
    if p.poll() is None:p.kill();p.wait()
)PY";
    file.close();
    std::filesystem::permissions(path, std::filesystem::perms::owner_all);
    return path;
  }
};

TEST_F(MdbReadyGatherTest, OtherWorkersProgressBeforePartialFrameCompletes) {
  Seed(32);
  auto plan = Plan({{-1,9000,-2},{-2,9001,-3}},3,{0,2},4,1); ASSERT_OK(plan);
  Rows expected;
  for (uint64_t s=101;s<133;++s) for (uint64_t o:{1001,1002}) expected.insert({s,o});
  for (size_t prefix : {0,2,5}) {
    const auto release=GetTestPath("release-"+std::to_string(prefix));
    const auto expired=GetTestPath("expired-"+std::to_string(prefix));
    auto options=workers_;options.binary=Relay(release,expired,prefix);
    options.page_rows=4;options.batch_rows=1;
    auto view=RdfYBModel::Instance().NewQueryReadContext(); ASSERT_OK(view);
    Rows rows;
    ASSERT_OK(GatherFragments(*plan,options,**view,[&](const auto& row) {
      rows.insert(row); std::ofstream signal(release); signal << "ready\n";
      return Status::OK();
    }));
    ASSERT_EQ(rows,expected);
    ASSERT_FALSE(std::filesystem::exists(expired)) << "prefix=" << prefix;
    ASSERT_EQ((*view)->stats.remote_fragments,4);
  }
}

TEST_F(MdbReadyGatherTest, PendingFrameCancellationDeadlineAndRecovery) {
  Seed(32);
  auto plan = Plan({{-1,9000,-2},{-2,9001,-3}},3,{0,2},4,1); ASSERT_OK(plan);
  for (bool cancel : {true,false}) {
    const auto release=GetTestPath(cancel?"cancel-release":"deadline-release");
    const auto expired=GetTestPath(cancel?"cancel-expired":"deadline-expired");
    auto options=workers_;options.binary=Relay(release,expired,2);
    options.page_rows=4;options.batch_rows=1;
    auto view=RdfYBModel::Instance().NewQueryReadContext(); ASSERT_OK(view);
    size_t callbacks=0;
    const auto status=GatherFragments(*plan,options,**view,[&](const auto&) {
      ++callbacks;
      if(cancel)(*view)->cancelled=true;else (*view)->deadline=CoarseMonoClock::Now();
      return Status::OK();
    });
    ASSERT_TRUE(cancel?status.IsAborted():status.IsTimedOut()) << status;
    ASSERT_EQ(callbacks,1); ASSERT_FALSE(std::filesystem::exists(expired));
  }
  const auto fresh=Query(kQuery,workers_);
  ASSERT_EQ(fresh.status,0);ASSERT_EQ(fresh.values.size(),64);
}
}  // namespace
}  // namespace yb::mdb


namespace yb::mdb {
namespace {
class MdbFrameReaderTest : public YBTest {
 protected:
  void SetUp() override {
    YBTest::SetUp(); ASSERT_EQ(pipe(fds_),0); ASSERT_OK(SetFragmentNonblocking(fds_[0]));
  }
  void TearDown() override {
    for(auto fd:fds_) if(fd>=0)close(fd);
    YBTest::TearDown();
  }
  std::string Frame(const std::string& payload) {
    const auto length=htonl(payload.size());
    return std::string(reinterpret_cast<const char*>(&length),4)+payload;
  }
  void Send(const std::string& bytes) { ASSERT_EQ(write(fds_[1],bytes.data(),bytes.size()),bytes.size()); }
  Status Read(bool* complete, CoarseTimePoint deadline=CoarseMonoClock::Now()+MonoDelta::FromSeconds(5),
              const std::atomic<bool>* cancelled=nullptr) {
    return reader_.ReadAvailable(fds_[0],&response_,complete,deadline,cancelled);
  }
  int fds_[2]={-1,-1};
  FragmentFrameReader reader_;
  FragmentResponsePB response_;
};

TEST_F(MdbFrameReaderTest, PartialHeaderBodyAndBackToBackFrames) {
  FragmentResponsePB expected;expected.set_query_id("ready-test");expected.set_batch_seq(3);
  const auto frame=Frame(expected.SerializeAsString());
  bool done=true;ASSERT_OK(Read(&done));ASSERT_FALSE(done);
  ASSERT_NO_FATALS(Send(frame.substr(0,2)));ASSERT_OK(Read(&done));ASSERT_FALSE(done);
  ASSERT_NO_FATALS(Send(frame.substr(2,3)));ASSERT_OK(Read(&done));ASSERT_FALSE(done);
  ASSERT_NO_FATALS(Send(frame.substr(5)+frame));
  ASSERT_OK(Read(&done));ASSERT_TRUE(done);ASSERT_EQ(response_.SerializeAsString(),expected.SerializeAsString());
  ASSERT_OK(Read(&done));ASSERT_TRUE(done);ASSERT_EQ(response_.SerializeAsString(),expected.SerializeAsString());
  ASSERT_OK(Read(&done));ASSERT_FALSE(done);
}

TEST_F(MdbFrameReaderTest, RejectsOversizedZeroMalformedAndTruncatedFrames) {
  for(const auto& frame:std::vector<std::string>{std::string(4,'\0'),
      Frame(std::string(kFragmentMaxFrame+1,'x')).substr(0,4),Frame(std::string(1,'\xff'))}) {
    FragmentFrameReader reader;bool done=false;
    ASSERT_NO_FATALS(Send(frame));
    const auto status=reader.ReadAvailable(fds_[0],&response_,&done,CoarseMonoClock::Now()+MonoDelta::FromSeconds(5));
    ASSERT_NOK(status);ASSERT_FALSE(done);
  }
  ASSERT_NO_FATALS(Send(Frame("abc").substr(0,5)));close(fds_[1]);fds_[1]=-1;
  bool done=false;ASSERT_TRUE(Read(&done).IsNetworkError());ASSERT_FALSE(done);
}

TEST_F(MdbFrameReaderTest, PartialFrameChecksCancellationAndDeadline) {
  ASSERT_NO_FATALS(Send(std::string(1,'\0')));bool done=false;
  ASSERT_OK(Read(&done));ASSERT_FALSE(done);
  std::atomic<bool> cancelled{true};
  ASSERT_TRUE(Read(&done,CoarseMonoClock::Now()+MonoDelta::FromSeconds(5),&cancelled).IsAborted());
  cancelled=false;ASSERT_TRUE(Read(&done,CoarseMonoClock::Now(),&cancelled).IsTimedOut());
}
}  // namespace
}  // namespace yb::mdb


namespace yb::mdb {
namespace {
TEST_F(MdbFragmentIntegrationTest, ExchangeStageTimingsFailureAndReset) {
  Seed(8);
  const auto root=GetTestPath("exchange-profile");std::filesystem::create_directory(root);
  auto options=workers_;options.exchange=ExchangeMode::Hash;
  options.exchange_spill_directory=root;options.batch_rows=128;options.page_rows=16;
  auto check=[&](const std::string& profile,bool active) {
    rapidjson::Document value;value.Parse(profile.c_str());ASSERT_FALSE(value.HasParseError());
    for(const auto* name:{"exchange_input_us","exchange_total_us","exchange_setup_us","exchange_left_input_us",
        "exchange_right_input_us","exchange_partition_us","exchange_consume_us",
        "exchange_sink_us","exchange_cleanup_us"}) {
      ASSERT_TRUE(value.HasMember(name)) << name;ASSERT_TRUE(value[name].IsUint64());
      if(!active)ASSERT_EQ(value[name].GetUint64(),0) << name;
    }
    const auto total=value["exchange_total_us"].GetUint64();
    const auto inputs=value["exchange_input_us"].GetUint64();
    ASSERT_GE(total,inputs+value["exchange_setup_us"].GetUint64()+
        value["exchange_consume_us"].GetUint64()+value["exchange_cleanup_us"].GetUint64());
    ASSERT_GE(inputs,value["exchange_partition_us"].GetUint64());
    ASSERT_GE(value["exchange_consume_us"].GetUint64(),value["exchange_sink_us"].GetUint64());
    ASSERT_GE(value["adapter_execute_us"].GetUint64(),total);
    if(active){ASSERT_GT(total,0);ASSERT_GT(value["exchange_cleanup_us"].GetUint64(),0);}
    ASSERT_EQ(value["exchange_live_files"].GetUint64(),0);
  };
  const auto correct=Query(kQuery,options);ASSERT_EQ(correct.status,0);ASSERT_EQ(correct.values.size(),16);
  ASSERT_NO_FATALS(check(correct.profile,true));
  ASSERT_GT(Field(correct.profile,"exchange_input_us"),0);
  ASSERT_EQ(Field(correct.profile,"exchange_left_input_us"),0);
  ASSERT_EQ(Field(correct.profile,"exchange_right_input_us"),0);
  ASSERT_EQ(Field(correct.profile,"exchange_interleaved_operators"),1);
  ASSERT_GT(Field(correct.profile,"exchange_partition_us"),0);
  RdfYBModel::Instance().SetWorkerOptions(options);
  size_t rows=0;
  auto slow=[](const char*,const char*,void* arg) {
    ++*static_cast<size_t*>(arg);SleepFor(MonoDelta::FromMilliseconds(1));
  };
  ASSERT_EQ(YBCMdbExecuteSparql(kQuery,slow,&rows),0);ASSERT_EQ(rows,16);
  const std::string slow_profile=YBCMdbGetLastQueryProfile();
  ASSERT_NO_FATALS(check(slow_profile,true));ASSERT_GT(Field(slow_profile,"exchange_sink_us"),8000);
  auto limited=options;limited.exchange_spill_bytes=140;
  const auto failed=Query(kQuery,limited);ASSERT_NE(failed.status,0);ASSERT_TRUE(failed.values.empty());
  ASSERT_NO_FATALS(check(failed.profile,true));
  const auto local=Query(kQuery,{});ASSERT_EQ(local.status,0);ASSERT_EQ(local.values,correct.values);
  ASSERT_NO_FATALS(check(local.profile,false));
  const auto recovered=Query(kQuery,options);ASSERT_EQ(recovered.status,0);ASSERT_EQ(recovered.values,correct.values);
  ASSERT_NO_FATALS(check(recovered.profile,true));ASSERT_TRUE(std::filesystem::is_empty(root));
}
}  // namespace
}  // namespace yb::mdb


namespace yb::mdb {
namespace {
class MdbInterleavedExchangeTest : public MdbReadyGatherTest {
 protected:
  std::array<FragmentPlan,2> Inputs(const FragmentPlan& plan) {
    std::array<FragmentPlan,2> result={plan,plan};
    for(size_t side=0;side<2;++side) {
      auto& open=result[side].physical;open.clear_triples();
      *open.add_triples()=plan.physical.triples(side);open.clear_projection();
      std::set<uint32_t> vars;
      for(const auto& term:open.triples(0).terms())if(term.has_variable())vars.insert(term.variable());
      for(auto var:vars)open.add_projection(var);
    }
    return result;
  }
  WorkerOptions Options(size_t workers=3) {
    auto options=workers_;options.max_workers=workers;options.page_rows=16;options.batch_rows=128;
    options.exchange=ExchangeMode::Hash;options.exchange_spill_directory=GetTestPath("paired-spill");
    std::filesystem::create_directories(options.exchange_spill_directory);
    return options;
  }
  Status Run(const FragmentPlan& plan,const WorkerOptions& options,DocDBReadContext& view,
             bool interleaved,Rows* rows,const FragmentInputSink& hook={}) {
    const auto children=Inputs(plan);
    ExchangeControl control{&view.cancelled,view.deadline,&view.stats,&view.deadline};
    auto output=[&](const auto& row){rows->insert(row);return Status::OK();};
    if(interleaved) {
      return ExchangeRowsWithInputs(plan.physical,options,control,[&](const FragmentInputSink& append) {
        ++view.stats.exchange_interleaved_operators;
        return GatherFragmentPair(children,options,view,[&](size_t side,const auto& row)->Status {
          if(hook)RETURN_NOT_OK(hook(side,row));
          return append(side,row);
        });
      },output);
    }
    return ExchangeRows(plan.physical,options,control,[&](size_t side,const FragmentRowSink& append) {
      return GatherFragments(children[side],options,view,[&](const auto& row)->Status {
        if(hook)RETURN_NOT_OK(hook(side,row));
        return append(row);
      });
    },output);
  }
  void CheckClean(const WorkerOptions& options,const DocDBReadContext& view) {
    ASSERT_EQ(view.stats.exchange_live_files,0);ASSERT_TRUE(std::filesystem::is_empty(options.exchange_spill_directory));
    ASSERT_LE(view.stats.remote_max_workers,options.max_workers);
  }
  void Benchmark(const std::string& shape) {
    constexpr size_t n=512;
    const size_t groups=shape=="unique"?n:shape=="shared"?1:32;
    const size_t fanout=shape=="unique"?1:shape=="shared"?2:16;
    Rows expected;
    for(size_t i=0;i<n;++i) {
      Write(101+i,9000,10000+i%groups);
      for(size_t j=0;j<fanout;++j)expected.insert({101+i,20000+(i%groups)*fanout+j});
    }
    for(size_t g=0;g<groups;++g)for(size_t j=0;j<fanout;++j)Write(10000+g,9001,20000+g*fanout+j);
    ASSERT_OK(RdfYBModel::Instance().Flush());
    auto plan=Plan({{-1,9000,-2},{-2,9001,-3}},3,{0,2},16,128);ASSERT_OK(plan);
    for(size_t workers:{1,3})for(int sample=-1;sample<5;++sample)for(int offset=0;offset<2;++offset) {
      const bool interleaved=(offset+sample+1)%2;
      const auto options=Options(workers);auto view=RdfYBModel::Instance().NewQueryReadContext();ASSERT_OK(view);
      Rows rows;const auto begin=WallMicros();const auto status=Run(*plan,options,**view,interleaved,&rows);
      const auto elapsed=WallMicros()-begin;ASSERT_OK(status);ASSERT_EQ(rows,expected);
      ASSERT_EQ((*view)->stats.remote_fragments,8);ASSERT_NO_FATALS(CheckClean(options,**view));
      ASSERT_GT((*view)->stats.exchange_input_us,0);
      if(!interleaved)ASSERT_GT((*view)->stats.exchange_left_input_us+(*view)->stats.exchange_right_input_us,0);
      LOG(INFO)<<"INTERLEAVE_SAMPLE {\"shape\":\""<<shape<<"\",\"n\":"<<n
          <<",\"workers\":"<<workers<<",\"mode\":\""<<(interleaved?"interleaved":"serial")
          <<"\",\"sample\":"<<sample<<",\"elapsed_us\":"<<elapsed<<",\"rows\":"<<rows.size()
          <<",\"correct\":true,\"profile\":"<<(*view)->stats.ToJson()<<"}";
    }
  }
};

TEST_F(MdbInterleavedExchangeTest, PairSchemasBagsAndAggregateWorkerLimit) {
  Seed(32);auto plan=Plan({{-1,9000,-2},{-2,-4,-3}},4,{0,2},16,128);ASSERT_OK(plan);
  Rows expected;for(uint64_t s=101;s<133;++s)for(uint64_t o:{1001,1002})expected.insert({s,o});
  for(size_t workers:{1,2,3})for(auto mode:{ExchangeMode::Hash,ExchangeMode::Broadcast}) {
    auto options=Options(workers);options.exchange=mode;options.exchange_memory_bytes=65536;
    auto view=RdfYBModel::Instance().NewQueryReadContext();ASSERT_OK(view);Rows rows;
    ASSERT_OK(Run(*plan,options,**view,true,&rows));ASSERT_EQ(rows,expected);
    ASSERT_EQ((*view)->stats.remote_fragments,8);ASSERT_EQ((*view)->stats.remote_max_workers,workers);
    ASSERT_LE((*view)->stats.exchange_peak_reserved_bytes,options.exchange_memory_bytes);
    ASSERT_NO_FATALS(CheckClean(options,**view));
  }
}

TEST_F(MdbInterleavedExchangeTest, OtherInputProgressesWhileFirstHasPartialFrame) {
  Seed(32);auto plan=Plan({{-1,9000,-2},{-2,9001,-3}},3,{0,2},4,1);ASSERT_OK(plan);
  auto options=Options();options.page_rows=4;options.batch_rows=1;
  for(bool gate_open:{false,true}) {
    const auto release=GetTestPath(gate_open?"right-open-release":"right-pull-release");
    const auto expired=GetTestPath(gate_open?"right-open-expired":"right-pull-expired");
    options.binary=Relay(release,expired,gate_open?2:5,gate_open);
    auto view=RdfYBModel::Instance().NewQueryReadContext();ASSERT_OK(view);Rows rows;
    ASSERT_OK(Run(*plan,options,**view,true,&rows,[&](size_t side,const auto&)->Status {
      if(side==1){std::ofstream signal(release);signal<<"right input progressed\n";}
      return Status::OK();
    }));
    Rows expected;for(uint64_t s=101;s<133;++s)for(uint64_t o:{1001,1002})expected.insert({s,o});
    ASSERT_EQ(rows,expected);ASSERT_TRUE(std::filesystem::exists(release));
    ASSERT_FALSE(std::filesystem::exists(expired));ASSERT_NO_FATALS(CheckClean(options,**view));
  }
}

TEST_F(MdbInterleavedExchangeTest, SharedSnapshotAndFreshQueryVisibility) {
  Seed(32);RegisterRdfIdentity(1003,RdfIdentity(TripleTermType::URI,Iri(1003)));
  auto plan=Plan({{-1,9000,-2},{-2,9001,-3}},3,{0,2},16,128);ASSERT_OK(plan);
  auto options=Options();auto view=RdfYBModel::Instance().NewQueryReadContext();ASSERT_OK(view);
  bool written=false;Rows rows;
  ASSERT_OK(Run(*plan,options,**view,true,&rows,[&](size_t,const auto&)->Status {
    if(!written){written=true;RETURN_NOT_OK(RdfYBModel::Instance().WriteTripleToDocDB(1,9001,1003));
      RETURN_NOT_OK(RdfYBModel::Instance().Flush());}
    return Status::OK();
  }));
  Rows expected;for(uint64_t s=101;s<133;++s)for(uint64_t o:{1001,1002})expected.insert({s,o});
  ASSERT_TRUE(written);ASSERT_EQ(rows,expected);ASSERT_NO_FATALS(CheckClean(options,**view));
  auto fresh=RdfYBModel::Instance().NewQueryReadContext();ASSERT_OK(fresh);Rows latest;
  ASSERT_OK(Run(*plan,options,**fresh,true,&latest));
  for(uint64_t s=101;s<133;++s)expected.insert({s,1003});ASSERT_EQ(latest,expected);
}

TEST_F(MdbInterleavedExchangeTest, AdmissionEmptySideAndFailureRecovery) {
  Seed(8);auto plan=Plan({{-1,9000,-2},{-2,9001,-3}},3,{0,2},16,128);ASSERT_OK(plan);
  auto options=Options(2);const auto valid=Inputs(*plan);
  for(int bad=0;bad<3;++bad) {
    auto pair=valid;
    if(bad==0)pair[1].tasks.pop_back();
    if(bad==1)pair[1].physical.set_table_ids(0,"wrong-table");
    if(bad==2)pair[1].physical.set_memory_bytes(1);
    auto view=RdfYBModel::Instance().NewQueryReadContext();ASSERT_OK(view);
    const auto status=GatherFragmentPair(pair,options,**view,[](size_t,const auto&){return Status::OK();});
    ASSERT_NOK(status);ASSERT_EQ((*view)->stats.remote_fragments,0);
  }
  auto empty=valid;empty[0].empty=true;auto view=RdfYBModel::Instance().NewQueryReadContext();ASSERT_OK(view);
  std::array<size_t,2> counts={0,0};
  ASSERT_OK(GatherFragmentPair(empty,options,**view,[&](size_t side,const auto&){++counts[side];return Status::OK();}));
  ASSERT_EQ(counts[0],0);ASSERT_EQ(counts[1],2);ASSERT_EQ((*view)->stats.remote_fragments,4);
  for(bool cancel:{true,false}) {
    auto stopped=RdfYBModel::Instance().NewQueryReadContext();ASSERT_OK(stopped);Rows rows;
    const auto status=Run(*plan,options,**stopped,true,&rows,[&](size_t,const auto&)->Status {
      if(cancel)(*stopped)->cancelled=true;else (*stopped)->deadline=CoarseMonoClock::Now();
      return Status::OK();
    });
    ASSERT_TRUE(cancel?status.IsAborted():status.IsTimedOut())<<status;ASSERT_TRUE(rows.empty());
    ASSERT_NO_FATALS(CheckClean(options,**stopped));
  }
  const auto wrapper=GetTestPath("late-pair-error");
  {std::ofstream file(wrapper);file<<"#!/bin/sh\n'"<<workers_.binary<<"' \"$@\"\nexit 23\n";}
  std::filesystem::permissions(wrapper,std::filesystem::perms::owner_all);
  auto broken=options;broken.binary=wrapper;auto failed=RdfYBModel::Instance().NewQueryReadContext();ASSERT_OK(failed);Rows rejected;
  ASSERT_NOK(Run(*plan,broken,**failed,true,&rejected));ASSERT_TRUE(rejected.empty());ASSERT_NO_FATALS(CheckClean(broken,**failed));
  auto fresh=RdfYBModel::Instance().NewQueryReadContext();ASSERT_OK(fresh);Rows recovered;
  ASSERT_OK(Run(*plan,options,**fresh,true,&recovered));ASSERT_EQ(recovered.size(),16);ASSERT_NO_FATALS(CheckClean(options,**fresh));
}

TEST_F(MdbInterleavedExchangeTest, Unique512Comparison){Benchmark("unique");}
TEST_F(MdbInterleavedExchangeTest, Shared512Comparison){Benchmark("shared");}
TEST_F(MdbInterleavedExchangeTest, Fanout512Comparison){Benchmark("fanout");}
}  // namespace
}  // namespace yb::mdb

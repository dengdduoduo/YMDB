// Copyright (c) YugabyteDB, Inc.
#include <gtest/gtest.h>
#include <algorithm>
#include <csignal>
#include <fstream>
#include <pwd.h>
#include <map>
#include <set>
#include <sstream>
#include <thread>
#include <unistd.h>

#include "yb/client/schema.h"
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
    RdfYBModel::Instance().ClearAll();
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
      "SELECT (SUM(?s) AS ?n) WHERE { ?s <urn:p6:9000> ?k }",
      "SELECT (COUNT(*) AS ?n) WHERE { ?s <urn:p6:9000> ?k } GROUP BY ?k ORDER BY ?s",
      "SELECT (COUNT(*) AS ?n) WHERE { ?s <urn:p6:9000> ?k } GROUP BY ?k HAVING (SUM(?s) > 1)",
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
  Subprocess sshd("/usr/sbin/sshd", {"/usr/sbin/sshd", "-D", "-e", "-f", server_config});
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

}  // namespace
}  // namespace yb::mdb

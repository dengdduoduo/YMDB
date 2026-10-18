// Portions Copyright (c) YugabyteDB, Inc.

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "graph_models/rdf_model/rdf_object_id.h"
#include "query/optimizer/binding_iter_constructor.h"
#include "query/optimizer/plan/join_order/leapfrog_estimator.h"
#include "query/optimizer/rdf_model/plan/docdb_leapfrog_plan.h"
#include "query/optimizer/rdf_model/plan/triple_plan.h"
#include "query/parser/op/sparql/op_basic_graph_pattern.h"
#include "query/parser/op/sparql/op_select.h"
#include "query/parser/sparql_query_parser.h"
#include "yb/client/schema.h"
#include "yb/client/yb_table_name.h"
#include "yb/integration-tests/mini_cluster.h"
#include "yb/integration-tests/yb_mini_cluster_test_base.h"
#include "yb/mdb/model/rdf_yb_model.h"
#include "yb/mdb/query/distributed/distributed_plan.h"
#include "yb/mdb/query/distributed/gather_operator.h"
#include "yb/mdb/query/distributed/scatter_executor.h"
#include "yb/mdb/query/distributed/tablet_router.h"
#include "yb/mdb/storage/docdb_key_encoder.h"
#include "yb/mdb/storage/leapfrog_docdb_iter.h"
#include "yb/tserver/tserver.messages.h"
#include "yb/util/flags.h"
#include "yb/util/scope_exit.h"
#include "yb/util/sync_point.h"
#include "yb/util/test_macros.h"

DECLARE_bool(mdb_enable_hash_join);
DECLARE_bool(mdb_enable_leapfrog_join);
DECLARE_uint32(mdb_leapfrog_read_page_rows);
DECLARE_uint64(mdb_leapfrog_reuse_bytes);
DECLARE_bool(mdb_profile_leapfrog);

namespace yb::mdb {

class MdbLeapfrogDocDBTest : public MiniClusterTestWithClient<MiniCluster> {
 protected:
  using Row = std::vector<uint64_t>;
  using Bag = std::multiset<Row>;
  using Triple = std::array<uint64_t, 3>;

  void SetUp() override {
    old_flag_ = FLAGS_mdb_enable_leapfrog_join;
    old_to_storage_ = g_pack_to_gmap;
    old_to_packed_ = g_gmap_to_pack;
    g_pack_to_gmap.clear();
    g_gmap_to_pack.clear();
    get_query_ctx() = QueryContext{};
    YBMiniClusterTestBase<MiniCluster>::SetUp();
    MiniClusterOptions opts;
    opts.num_tablet_servers = 1;
    cluster_ = std::make_unique<MiniCluster>(opts);
    ASSERT_OK(cluster_->Start());
    ASSERT_OK(CreateClient());
    ASSERT_OK(client_->CreateNamespaceIfNotExists("mdb_leapfrog", YQL_DATABASE_CQL));
    for (const auto* name : RdfYBModel::kTableNames) {
      client::YBSchemaBuilder schema;
      schema.AddColumn("c0")->Type(DataType::INT64)->HashPrimaryKey()->NotNull();
      schema.AddColumn("c1")->Type(DataType::INT64)->PrimaryKey()->NotNull();
      schema.AddColumn("c2")->Type(DataType::INT64)->PrimaryKey()->NotNull();
      client::TableHandle table;
      ASSERT_OK(table.Create(
          client::YBTableName(YQL_DATABASE_CQL, "mdb_leapfrog", name), 4, client_.get(), &schema));
    }
    ASSERT_OK(RdfYBModel::Instance().Init(client_.get(), "mdb_leapfrog"));
    ASSERT_OK(router_.Init(client_.get(), "mdb_leapfrog"));
  }

  void DoTearDown() override {
    RdfYBModel::Instance().ClearAll();
    FLAGS_mdb_enable_leapfrog_join = old_flag_;
    g_pack_to_gmap = std::move(old_to_storage_);
    g_gmap_to_pack = std::move(old_to_packed_);
    get_query_ctx() = QueryContext{};
    MiniClusterTestWithClient<MiniCluster>::DoTearDown();
  }

  std::unique_ptr<SPARQL::Op> Parse(const std::string& query) {
    SPARQL::QueryParser parser(query);
    auto op = parser.get_query_plan({});
    auto& select = dynamic_cast<SPARQL::OpSelect&>(*op);
    auto& bgp = dynamic_cast<SPARQL::OpBasicGraphPattern&>(*select.op);
    for (auto& triple : bgp.triples) {
      if (!triple.predicate.is_OID()) continue;
      const auto packed = triple.predicate.get_OID().id;
      for (const auto& [iri, storage] :
           std::map<std::string, uint64_t>{{"p", 10}, {"q", 20}, {"r", 30}, {"missing", 99}}) {
        SPARQL::QueryParser ref(
            "SELECT ?a ?b WHERE { ?a <https://example.org/lf/" + iri + "> ?b }");
        auto ref_op = ref.get_query_plan({});
        const auto& ref_select = dynamic_cast<const SPARQL::OpSelect&>(*ref_op);
        const auto& ref_bgp = dynamic_cast<const SPARQL::OpBasicGraphPattern&>(*ref_select.op);
        if (packed == ref_bgp.triples.front().predicate.get_OID().id) {
          g_pack_to_gmap[packed] = storage;
          // Mirror the CLI's RebindOpIds before either planner sees constants.
          triple.predicate = Id(::ObjectId(storage));
        }
      }
    }
    return op;
  }

  void Write(const Triple& row) {
    ASSERT_OK(RdfYBModel::Instance().WriteTripleToDocDB(row[0], row[1], row[2]));
  }

  static uint64_t Packed(uint64_t stored) { return 1000000 - stored; }

  void MapNodes(const std::vector<Triple>& data) {
    for (const auto& row : data) {
      for (const auto index : {0, 2}) {
        g_gmap_to_pack[row[index]] = ::ObjectId(Packed(row[index]));
        g_pack_to_gmap[Packed(row[index])] = row[index];
      }
    }
  }

  static Bag Oracle(const std::vector<Triple>& data, bool projected) {
    Bag expected;
    for (const auto& a : data) {
      if (a[1] != 10) continue;
      for (const auto& b : data) {
        if (b[1] != 20 || b[0] != a[2]) continue;
        for (const auto& c : data) {
          if (c[1] == 30 && c[0] == a[0] && c[2] == b[2]) {
            expected.insert(
                projected ? Row{Packed(a[0]), Packed(b[2])}
                          : Row{Packed(a[0]), Packed(a[2]), Packed(b[2])});
          }
        }
      }
    }
    return expected;
  }

  void Drain(
      BindingIter& iter, Binding& binding, const std::vector<VarId>& vars, const Bag& expected) {
    Bag actual;
    for (size_t count = 0; count <= expected.size() && iter.next(); ++count) {
      Row row;
      for (const auto var : vars) row.push_back(binding[var].id);
      actual.insert(std::move(row));
    }
    ASSERT_OK(iter.status());
    ASSERT_EQ(actual, expected);
  }

  void CompareTriangleAlgorithms(
      const std::string& shape, uint64_t left_nodes = 512, bool scale_comparison = false) {
    const std::string label = shape + (scale_comparison ? "25k" : "");
    auto& model = RdfYBModel::Instance();
    ASSERT_EQ(model.GetStatistics(), nullptr);
    ASSERT_FALSE(model.HasTransaction());
    const auto previous_hash = FLAGS_mdb_enable_hash_join;
    const auto previous_leapfrog = FLAGS_mdb_enable_leapfrog_join;
    const auto previous_partition = model.UsePartitionLoad();
    const auto previous_profile = FLAGS_mdb_profile_leapfrog;
    const auto previous_page_rows = FLAGS_mdb_leapfrog_read_page_rows;
    const auto previous_reuse_bytes = FLAGS_mdb_leapfrog_reuse_bytes;
    auto restore = ScopeExit([&] {
      FLAGS_mdb_enable_hash_join = previous_hash;
      FLAGS_mdb_enable_leapfrog_join = previous_leapfrog;
      model.SetUsePartitionLoad(previous_partition);
      FLAGS_mdb_profile_leapfrog = previous_profile;
      FLAGS_mdb_leapfrog_read_page_rows = previous_page_rows;
      FLAGS_mdb_leapfrog_reuse_bytes = previous_reuse_bytes;
    });
    FLAGS_mdb_enable_hash_join = false;
    FLAGS_mdb_profile_leapfrog = true;
    model.SetUsePartitionLoad(true);
    std::vector<Triple> data;
    if (shape == "dense") {
      for (uint64_t a = 0; a < 24; ++a) {
        for (uint64_t b = 0; b < 24; ++b) {
          data.push_back({1000 + a, 10, 2000 + b});
          data.push_back({2000 + a, 20, 3000 + b});
          data.push_back({1000 + a, 30, 3000 + b});
        }
      }
    } else if (shape == "skew") {
      for (uint64_t x = 0; x < left_nodes; ++x) {
        data.push_back({1000 + x, 10, 2000 + x % 8});
        data.push_back({1000 + x, 30, 3000 + x % 32});
        data.push_back({1000 + x, 30, 3000 + (x + 1) % 32});
      }
      for (uint64_t y = 0; y < 8; ++y) {
        for (uint64_t z = 0; z < 32; ++z) data.push_back({2000 + y, 20, 3000 + z});
      }
    } else {
      ASSERT_TRUE(shape == "sparse" || shape == "empty");
      for (uint64_t i = 0; i < left_nodes; ++i) {
        data.push_back({1000 + i, 10, 2000 + i});
        data.push_back({2000 + i, 20, 3000 + i});
        data.push_back({1000 + i, 30, 3000 + (shape == "empty" ? (i + 1) % left_nodes : i)});
      }
    }
    if (scale_comparison) {
      ASSERT_TRUE(shape == "sparse" || shape == "skew");
      for (uint64_t i = data.size(); i < 25000; ++i) data.push_back({100000 + i, 40, 200000 + i});
      ASSERT_EQ(data.size(), 25000);
    }
    for (const auto& row : data) ASSERT_NO_FATALS(Write(row));
    ASSERT_OK(model.Flush());
    std::map<uint64_t, std::vector<uint64_t>> outgoing;
    std::set<std::pair<uint64_t, uint64_t>> closing;
    for (const auto& row : data) {
      if (row[1] == 20) outgoing[row[0]].push_back(row[2]);
      if (row[1] == 30) closing.emplace(row[0], row[2]);
    }
    std::vector<Row> expected;
    for (const auto& row : data) {
      if (row[1] != 10) continue;
      for (const auto z : outgoing[row[2]]) {
        if (closing.count({row[0], z})) expected.push_back({row[0], row[2], z});
      }
    }
    std::sort(expected.begin(), expected.end());
    const auto expected_count = shape == "dense"   ? 13824
                                : shape == "skew"  ? 2 * left_nodes
                                : shape == "empty" ? 0
                                                   : left_nodes;
    ASSERT_EQ(expected.size(), expected_count);
    auto op = Parse(
        "SELECT ?x ?y ?z WHERE { ?x <https://example.org/lf/p> ?y . "
        "?y <https://example.org/lf/q> ?z . ?x <https://example.org/lf/r> ?z }");
    const auto& select = dynamic_cast<const SPARQL::OpSelect&>(*op);
    auto* sync = SyncPoint::GetInstance();
    auto cleanup = ScopeExit([sync] {
      sync->DisableProcessing();
      sync->ClearAllCallBacks();
    });
    std::atomic<uint64_t> requests{0};
    size_t private_rows = 0, private_bytes = 0, shared_bytes = 0, live_bytes = 0;
    size_t materializations = 0, reuse_reports = 0;
    SPARQL::DocDBLeapfrogReuseStats reuse;
    sync->SetCallBack("ReadRpc::CallRemoteMethod", [&](void* arg) {
      const auto* request = static_cast<tserver::LWReadRequestPB*>(arg);
      requests += request->ql_batch().size();
    });
    sync->SetCallBack("MDBLeapfrogTriangle::Materialized", [&](void* arg) {
      const auto& memory = *static_cast<SPARQL::DocDBLeapfrogMemory*>(arg);
      ++materializations;
      private_rows = std::max(private_rows, memory.private_rows);
      private_bytes = std::max(private_bytes, memory.private_bytes);
      shared_bytes = std::max(shared_bytes, memory.shared_bytes);
      live_bytes = std::max(live_bytes, memory.private_bytes + memory.shared_bytes);
    });
    sync->SetCallBack("MDBLeapfrogQuery::Reuse", [&](void* arg) {
      reuse = *static_cast<SPARQL::DocDBLeapfrogReuseStats*>(arg);
      ++reuse_reports;
    });
    SPARQL::DocDBLeapfrogProfile phases;
    size_t phase_reports = 0;
    sync->SetCallBack("MDBLeapfrogTriangle::Phases", [&](void* arg) {
      const auto& part = *static_cast<SPARQL::DocDBLeapfrogProfile*>(arg);
      phases.load_ns += part.load_ns;
      phases.build_ns += part.build_ns;
      phases.join_ns += part.join_ns;
      phases.output_ns += part.output_ns;
      phases.loads += part.loads;
      phases.scanned_rows += part.scanned_rows;
      phases.loaded_rows += part.loaded_rows;
      ++phase_reports;
    });
    for (int trial = 0; trial < 4; ++trial) {
      for (int position = 0; position < 3; ++position) {
        const int mode = (trial + position) % 3;
        const bool leapfrog = scale_comparison || mode != 0;
        const char* algorithm = scale_comparison ? (mode == 0   ? "private"
                                                    : mode == 1 ? "reuse"
                                                                : "limited")
                                                 : (mode == 0   ? "greedy"
                                                    : mode == 1 ? "leapfrog_private"
                                                                : "leapfrog_reuse");
        FLAGS_mdb_leapfrog_read_page_rows = 1024;
        FLAGS_mdb_leapfrog_reuse_bytes = scale_comparison ? (mode == 0   ? 0
                                                             : mode == 1 ? 8 * 1024 * 1024
                                                                         : 192 * 1024)
                                                          : (mode == 2 ? 8 * 1024 * 1024 : 0);
        const uint64_t admissions = scale_comparison ? (mode == 0   ? 0
                                                        : mode == 1 ? 2
                                                                    : 1)
                                                     : (mode == 2 ? 2 : 0);
        FLAGS_mdb_enable_leapfrog_join = leapfrog;
        model.ClearQueryCaches();
        const auto baseline_memory = model.QueryCacheMemory();
        ASSERT_EQ(baseline_memory.rows, 0);
        requests = 0;
        private_rows = private_bytes = shared_bytes = live_bytes = materializations = 0;
        reuse_reports = 0;
        reuse = {};
        phases = {};
        phase_reports = 0;
        DistributedPlan plan;
        DistributedPlanGenerator generator(&router_);
        ScatterExecutor executor;
        std::vector<ScatterExecutor::FragmentResultSet> fragments;
        GatherOperator gather;
        GatherResult merged;
        std::vector<Row> actual;
        LOG(INFO) << "MDB_TRIANGLE_PERF_BEGIN shape=" << label << " trial=" << trial
                  << " algorithm=" << algorithm;
        sync->EnableProcessing();
        const auto start = std::chrono::steady_clock::now();
        ASSERT_OK(generator.Generate(*op, &plan));
        ASSERT_OK(executor.Execute(plan, &fragments));
        ASSERT_OK(gather.Merge(fragments, plan.gather_strategy(), &merged));
        for (const auto& binding : merged.bindings) {
          Row row;
          for (const auto var : select.vars) row.push_back((*binding)[var].id);
          actual.push_back(std::move(row));
        }
        const auto end = std::chrono::steady_clock::now();
        sync->DisableProcessing();
        const auto elapsed_us =
            std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        const auto model_memory = model.QueryCacheMemory();
        ASSERT_EQ(plan.num_fragments(), 4);
        ASSERT_EQ(materializations, leapfrog ? 4 : 0);
        ASSERT_EQ(phase_reports, leapfrog ? 4 : 0);
        ASSERT_EQ(reuse_reports, 1);
        ASSERT_EQ(reuse.hits, admissions * 3);
        ASSERT_EQ(reuse.admissions, admissions);
        ASSERT_EQ(phases.loads, leapfrog ? 12 - admissions * 3 : 0);
        ASSERT_EQ(reuse.rejections, leapfrog ? (2 - admissions) * 4 : 0);
        ASSERT_LE(reuse.retained_bytes, FLAGS_mdb_leapfrog_reuse_bytes);
        ASSERT_GE(phases.load_ns, phases.build_ns);
        ASSERT_GE(phases.join_ns, 0);
        ASSERT_GT(requests.load(), 0);
        ASSERT_GE(model_memory.capacity_bytes, model_memory.rows * 3 * sizeof(uint64_t));
        ASSERT_GE(private_bytes, private_rows * 3 * sizeof(uint64_t));
        // Include all query-retained vectors once, plus the simultaneously live private ones.
        const auto peak_bytes = model_memory.capacity_bytes + live_bytes;
        ASSERT_GE(peak_bytes, baseline_memory.capacity_bytes);
        LOG(INFO) << "MDB_TRIANGLE_PERF shape=" << label << " trial=" << trial
                  << " algorithm=" << algorithm
                  << " page_rows=" << (leapfrog ? FLAGS_mdb_leapfrog_read_page_rows : 0)
                  << " anchor_page_rows=" << (leapfrog ? FLAGS_mdb_leapfrog_read_page_rows : 0)
                  << " input_rows=" << data.size() << " output_rows=" << actual.size()
                  << " total_us=" << elapsed_us << " ql_requests=" << requests.load()
                  << " baseline_row_capacity_bytes=" << baseline_memory.capacity_bytes
                  << " model_cache_rows=" << model_memory.rows
                  << " model_row_capacity_bytes=" << model_memory.capacity_bytes
                  << " private_cache_peak_rows=" << private_rows
                  << " private_row_capacity_bytes=" << private_bytes
                  << " shared_row_capacity_bytes=" << shared_bytes << " reuse_hits=" << reuse.hits
                  << " reuse_admissions=" << reuse.admissions
                  << " reuse_budget_bytes=" << FLAGS_mdb_leapfrog_reuse_bytes
                  << " reuse_retained_bytes=" << reuse.retained_bytes
                  << " reuse_rejections=" << reuse.rejections
                  << " cache_peak_capacity_bytes=" << peak_bytes
                  << " cache_growth_bytes=" << peak_bytes - baseline_memory.capacity_bytes
                  << " load_other_us=" << (phases.load_ns - phases.build_ns) / 1000
                  << " build_us=" << phases.build_ns / 1000 << " join_us=" << phases.join_ns / 1000
                  << " output_us=" << phases.output_ns / 1000 << " relation_loads=" << phases.loads
                  << " scanned_rows=" << phases.scanned_rows
                  << " loaded_rows=" << phases.loaded_rows;
        std::sort(actual.begin(), actual.end());
        ASSERT_EQ(actual, expected);
      }
    }
  }

  void CheckDistributed(SPARQL::Op& op, const Bag& expected) {
    DistributedPlan plan;
    DistributedPlanGenerator generator(&router_);
    ASSERT_OK(generator.Generate(op, &plan));
    ASSERT_EQ(plan.num_fragments(), 4);
    ScatterExecutor executor;
    std::vector<ScatterExecutor::FragmentResultSet> fragments;
    ASSERT_OK(executor.Execute(plan, &fragments));
    GatherOperator gather;
    GatherResult merged;
    ASSERT_OK(gather.Merge(fragments, plan.gather_strategy(), &merged));
    const auto& select = dynamic_cast<const SPARQL::OpSelect&>(op);
    Bag actual;
    for (const auto& binding : merged.bindings) {
      Row row;
      for (const auto var : select.vars) row.push_back((*binding)[var].id);
      actual.insert(std::move(row));
    }
    ASSERT_EQ(actual, expected);
  }

  TabletRouter router_;
  bool old_flag_ = false;
  decltype(g_pack_to_gmap) old_to_storage_;
  decltype(g_gmap_to_pack) old_to_packed_;
};

TEST_F(MdbLeapfrogDocDBTest, RealTriangleBagMappingAndTabletOwnership) {
  ASSERT_FALSE(FLAGS_mdb_enable_leapfrog_join);
  std::vector<Triple> data;
  for (uint64_t x = 100; x < 124; ++x) {
    for (uint64_t y = 500; y < 503; ++y) data.push_back({x, 10, y});
    for (uint64_t z = 900; z < 904; ++z) data.push_back({x, 30, z});
  }
  for (uint64_t y = 500; y < 503; ++y) {
    for (uint64_t z = 900; z < 904; ++z) data.push_back({y, 20, z});
  }
  data.push_back({999, 40, 999});
  for (const auto& row : data) ASSERT_NO_FATALS(Write(row));
  ASSERT_OK(RdfYBModel::Instance().Flush());
  MapNodes(data);
  const std::array<std::string, 3> patterns{
      "?x <https://example.org/lf/p> ?y . ", "?y <https://example.org/lf/q> ?z . ",
      "?x <https://example.org/lf/r> ?z . "};
  std::array<int, 3> order{0, 1, 2};
  size_t comparisons = 0;
  do {
    for (const bool projected : {false, true}) {
      const auto query =
          std::string(projected ? "SELECT ?x ?z WHERE { " : "SELECT ?x ?y ?z WHERE { ") +
          patterns[order[0]] + patterns[order[1]] + patterns[order[2]] + "}";
      auto op = Parse(query);
      const auto& select = dynamic_cast<const SPARQL::OpSelect&>(*op);
      const auto expected = Oracle(data, projected);
      ASSERT_EQ(expected.size(), 288);
      if (projected) ASSERT_EQ(expected.count({Packed(100), Packed(900)}), 3);
      for (const bool enabled : {false, true}) {
        FLAGS_mdb_enable_leapfrog_join = enabled;
        RdfYBModel::Instance().ClearQueryCaches();
        SPARQL::BindingIterConstructor constructor;
        op->accept_visitor(constructor);
        ASSERT_NE(constructor.tmp, nullptr);
        std::ostringstream printed;
        constructor.tmp->print(printed, 0, false);
        ASSERT_EQ(printed.str().find("DocDBLeapfrogTriangle") != std::string::npos, enabled);
        Binding binding(get_query_ctx().get_var_size());
        constructor.tmp->begin(binding);
        ASSERT_NO_FATALS(Drain(*constructor.tmp, binding, select.vars, expected));
        constructor.tmp->reset();
        ASSERT_NO_FATALS(Drain(*constructor.tmp, binding, select.vars, expected));
        ++comparisons;

        DistributedPlan plan;
        DistributedPlanGenerator generator(&router_);
        ASSERT_OK(generator.Generate(*op, &plan));
        ASSERT_EQ(plan.num_fragments(), 4);
        ScatterExecutor executor;
        std::vector<ScatterExecutor::FragmentResultSet> fragments;
        ASSERT_OK(executor.Execute(plan, &fragments));
        GatherResult merged;
        GatherOperator gather;
        ASSERT_OK(gather.Merge(fragments, plan.gather_strategy(), &merged));
        Bag actual;
        for (const auto& item : merged.bindings) {
          Row row;
          for (const auto var : select.vars) row.push_back((*item)[var].id);
          actual.insert(std::move(row));
        }
        ASSERT_EQ(actual, expected);
        if (order[0] != 1) {
          ASSERT_GE(
              std::count_if(
                  fragments.begin(), fragments.end(),
                  [](const auto& fragment) { return !fragment.bindings.empty(); }),
              2);
        }
        ++comparisons;
      }
    }
  } while (std::next_permutation(order.begin(), order.end()));
  ASSERT_EQ(comparisons, 48);
}

TEST_F(MdbLeapfrogDocDBTest, UnsupportedShapesFallBackWithoutPlanningReads) {
  FLAGS_mdb_enable_leapfrog_join = true;
  for (const std::string query :
       {"SELECT * WHERE { ?x <https://example.org/lf/p> ?x }",
        "SELECT * WHERE { ?x <https://example.org/lf/p> ?y . ?y <https://example.org/lf/q> ?z }",
        "SELECT * WHERE { ?x <https://example.org/lf/p> ?y . ?x <https://example.org/lf/q> ?z . ?x "
        "<https://example.org/lf/r> ?w }",
        "SELECT * WHERE { ?x ?p ?y . ?y <https://example.org/lf/q> ?z . ?x "
        "<https://example.org/lf/r> ?z }",
        "SELECT * WHERE { <https://example.org/lf/fixed> <https://example.org/lf/p> ?y . ?y "
        "<https://example.org/lf/q> ?z . ?z <https://example.org/lf/r> ?w }"}) {
    auto op = Parse(query);
    SPARQL::BindingIterConstructor constructor;
    op->accept_visitor(constructor);
    std::ostringstream printed;
    constructor.tmp->print(printed, 0, false);
    ASSERT_EQ(printed.str().find("DocDBLeapfrogTriangle"), std::string::npos);
  }
  const auto make_plans = [] {
    std::vector<std::unique_ptr<Plan>> plans;
    plans.push_back(
        std::make_unique<SPARQL::TriplePlan>(Id(VarId(0)), Id(::ObjectId(10)), Id(VarId(1))));
    plans.push_back(
        std::make_unique<SPARQL::TriplePlan>(Id(VarId(1)), Id(::ObjectId(20)), Id(VarId(2))));
    plans.push_back(
        std::make_unique<SPARQL::TriplePlan>(Id(VarId(0)), Id(::ObjectId(30)), Id(VarId(2))));
    return plans;
  };
  auto plans = make_plans();
  ASSERT_EQ(SPARQL::TryBuildDocDBLeapfrogTriangle(plans, {VarId(0)}), nullptr);
  for (const bool enabled : {false, true}) {
    FLAGS_mdb_enable_leapfrog_join = enabled;
    auto* sync = SyncPoint::GetInstance();
    std::atomic<int> requests{0};
    auto cleanup = ScopeExit([sync] {
      sync->DisableProcessing();
      sync->ClearAllCallBacks();
    });
    sync->SetCallBack("ReadRpc::CallRemoteMethod", [&](void*) { ++requests; });
    sync->EnableProcessing();
    auto iter = SPARQL::TryBuildDocDBLeapfrogTriangle(plans, {});
    sync->DisableProcessing();
    ASSERT_EQ(iter != nullptr, enabled);
    ASSERT_EQ(requests.load(), 0);
  }
  FLAGS_mdb_enable_leapfrog_join = true;
  ASSERT_OK(RdfYBModel::Instance().BeginTransaction());
  ASSERT_EQ(SPARQL::TryBuildDocDBLeapfrogTriangle(plans, {}), nullptr);
  ASSERT_OK(RdfYBModel::Instance().AbortTransaction());
}

TEST_F(MdbLeapfrogDocDBTest, FilteredTabletPagesPreservePhysicalOwnership) {
  std::vector<Triple> data;
  for (uint64_t i = 0; i < 1025; ++i) {
    data.push_back({1000 + i, 10, 3000 + i});
    data.push_back({1000 + i, 40, 5000 + i});
  }
  const uint64_t high_predicate = uint64_t{1} << 63;
  for (uint64_t i = 0; i < 129; ++i) data.push_back({7000 + i, high_predicate, 9000 + i});
  for (const auto& row : data) ASSERT_NO_FATALS(Write(row));
  ASSERT_OK(RdfYBModel::Instance().Flush());
  auto* sync = SyncPoint::GetInstance();
  auto cleanup = ScopeExit([sync] {
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  });
  std::atomic<int> requests{0}, paged{0}, continuations{0};
  sync->SetCallBack("ReadRpc::CallRemoteMethod", [&](void* arg) {
    for (const auto& ql : static_cast<tserver::LWReadRequestPB*>(arg)->ql_batch()) {
      ++requests;
      if (ql.return_paging_state() && ql.limit() == 128) ++paged;
      if (ql.has_paging_state() && !ql.paging_state().next_row_key().empty()) ++continuations;
    }
  });
  // This independent column map must agree with all six persisted permutations.
  const std::array<std::string, 6> predicate_columns{"c1", "c0", "c2", "c0", "c2", "c1"};
  for (int perm = 0; perm < 6; ++perm) {
    const auto& table = *RdfYBModel::Instance().GetTable(perm);
    for (const uint64_t predicate : {uint64_t{10}, high_predicate, uint64_t{99}}) {
      Bag combined;
      requests = paged = continuations = 0;
      for (const auto& tablet : router_.GetTablets(perm)) {
        client::TableIteratorOptions options;
        options.columns = {"c0", "c1", "c2"};
        options.tablet = tablet.tablet_id;
        Status read_status;
        options.error_handler = [&](const Status& status) { read_status = status; };
        Bag expected;
        for (const auto& row : client::TableRange(table, options)) {
          uint64_t s, p, o;
          DocKeyEncoder::ReversePermute(
              perm, row.column(0).int64_value(), row.column(1).int64_value(),
              row.column(2).int64_value(), &s, &p, &o);
          if (p == predicate) expected.insert({s, p, o});
        }
        ASSERT_OK(read_status);
        options.page_size = 128;
        options.filter = [&, predicate](const client::TableHandle& handle, LWQLConditionPB* cond) {
          handle.SetInt64Condition(
              cond, predicate_columns[perm], QL_OP_EQUAL, static_cast<int64_t>(predicate));
        };
        Bag actual;
        sync->EnableProcessing();
        for (const auto& row : client::TableRange(table, options)) {
          uint64_t s, p, o;
          DocKeyEncoder::ReversePermute(
              perm, row.column(0).int64_value(), row.column(1).int64_value(),
              row.column(2).int64_value(), &s, &p, &o);
          actual.insert({s, p, o});
        }
        sync->DisableProcessing();
        ASSERT_OK(read_status);
        ASSERT_EQ(actual, expected) << "perm=" << perm << " tablet=" << tablet.tablet_id;
        combined.insert(actual.begin(), actual.end());
      }
      Bag expected_all;
      for (const auto& row : data)
        if (row[1] == predicate) expected_all.insert({row[0], row[1], row[2]});
      ASSERT_EQ(combined, expected_all);
      LOG(INFO) << "MDB_FILTERED_SCAN perm=" << perm << " predicate=" << predicate
                << " rows=" << combined.size() << " requests=" << requests.load()
                << " paged=" << paged.load() << " continuations=" << continuations.load();
      ASSERT_EQ(paged.load(), requests.load());
      if (predicate == 10) ASSERT_GT(continuations.load(), 0);
    }
  }
}

TEST_F(MdbLeapfrogDocDBTest, FilteredPagingChecksCancellationAndInvalidLimit) {
  for (uint64_t i = 0; i < 1025; ++i) ASSERT_NO_FATALS(Write({1000 + i, 10, 3000 + i}));
  ASSERT_OK(RdfYBModel::Instance().Flush());
  const auto& table = *RdfYBModel::Instance().GetTable(DocKeyEncoder::kPSO);
  auto* sync = SyncPoint::GetInstance();
  auto cleanup = ScopeExit([sync] {
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  });
  std::atomic<size_t> requests{0};
  sync->SetCallBack("ReadRpc::CallRemoteMethod", [&](void* arg) {
    requests += static_cast<tserver::LWReadRequestPB*>(arg)->ql_batch().size();
  });
  std::string owner;
  for (const auto& tablet : router_.GetTablets(DocKeyEncoder::kPSO)) {
    client::TableIteratorOptions options;
    options.tablet = tablet.tablet_id;
    if (client::TableRange(table, options).begin() != client::TableIterator())
      owner = tablet.tablet_id;
  }
  ASSERT_FALSE(owner.empty());
  for (const int stop_at : {1, 2, 0}) {
    client::TableIteratorOptions options;
    options.tablet = owner;
    options.page_size = stop_at == 0 ? 0 : 128;
    options.filter = [](const client::TableHandle& handle, LWQLConditionPB* condition) {
      handle.SetInt64Condition(condition, "c0", QL_OP_EQUAL, 10);
    };
    int checks = 0;
    options.check_status = [&] {
      return ++checks == stop_at ? STATUS(Aborted, "cancel filtered paging") : Status::OK();
    };
    Status status;
    options.error_handler = [&](const Status& error) { status = error; };
    requests = 0;
    size_t rows = 0;
    sync->EnableProcessing();
    for (const auto& row : client::TableRange(table, options)) {
      ASSERT_EQ(row.column(0).int64_value(), 10);
      ++rows;
    }
    sync->DisableProcessing();
    if (stop_at == 0)
      ASSERT_TRUE(status.IsInvalidArgument()) << status;
    else
      ASSERT_TRUE(status.IsAborted()) << status;
    ASSERT_EQ(checks, stop_at);
    ASSERT_EQ(rows, stop_at == 2 ? 128 : 0);
    ASSERT_EQ(requests.load(), stop_at == 2 ? 1 : 0);
  }
}

TEST_F(MdbLeapfrogDocDBTest, FilteredAnchorsKeepAllPermutationsAndLateErrors) {
  FLAGS_mdb_enable_leapfrog_join = true;
  const auto old_profile = FLAGS_mdb_profile_leapfrog;
  const auto old_page_rows = FLAGS_mdb_leapfrog_read_page_rows;
  auto* sync = SyncPoint::GetInstance();
  auto cleanup = ScopeExit([&] {
    FLAGS_mdb_profile_leapfrog = old_profile;
    FLAGS_mdb_leapfrog_read_page_rows = old_page_rows;
    get_query_ctx().thread_info.interruption_requested = false;
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  });
  FLAGS_mdb_profile_leapfrog = true;
  constexpr uint64_t kTriples = 4097;
  for (uint64_t i = 0; i < kTriples; ++i) {
    ASSERT_NO_FATALS(Write({1000 + i, 10, 3000 + i}));
    ASSERT_NO_FATALS(Write({3000 + i, 20, 5000 + i}));
    ASSERT_NO_FATALS(Write({1000 + i, 30, 5000 + i}));
    ASSERT_NO_FATALS(Write({1000 + i, 40, 9000 + i}));
  }
  ASSERT_OK(RdfYBModel::Instance().Flush());
  const std::vector<VarId> vars{VarId(0), VarId(1), VarId(2)};
  const auto make_iter = [](int perm, const std::string& tablet, uint64_t predicate) {
    std::vector<std::unique_ptr<Plan>> plans;
    plans.push_back(
        std::make_unique<SPARQL::TriplePlan>(
            Id(VarId(0)), Id(::ObjectId(predicate)), Id(VarId(1)), perm, tablet));
    plans.push_back(
        std::make_unique<SPARQL::TriplePlan>(Id(VarId(1)), Id(::ObjectId(20)), Id(VarId(2))));
    plans.push_back(
        std::make_unique<SPARQL::TriplePlan>(Id(VarId(0)), Id(::ObjectId(30)), Id(VarId(2))));
    return SPARQL::TryBuildDocDBLeapfrogTriangle(plans, {});
  };
  for (const uint32_t page_rows : {128U, 1024U}) {
    FLAGS_mdb_leapfrog_read_page_rows = page_rows;
    for (int perm = 0; perm < 6; ++perm) {
      std::atomic<int> wrong_limits{0};
      int continuation_tablets = 0;
      Bag combined;
      for (const auto& tablet : router_.GetTablets(perm)) {
        Bag expected;
        client::TableIteratorOptions options;
        options.columns = {"c0", "c1", "c2"};
        options.tablet = tablet.tablet_id;
        for (const auto& row :
             client::TableRange(*RdfYBModel::Instance().GetTable(perm), options)) {
          uint64_t s, p, o;
          DocKeyEncoder::ReversePermute(
              perm, row.column(0).int64_value(), row.column(1).int64_value(),
              row.column(2).int64_value(), &s, &p, &o);
          if (p == 10) expected.insert({s, o, s + 4000});
        }
        SPARQL::DocDBLeapfrogProfile profile;
        sync->SetCallBack("MDBLeapfrogTriangle::Phases", [&](void* arg) {
          profile = *static_cast<SPARQL::DocDBLeapfrogProfile*>(arg);
        });
        sync->SetCallBack("ReadRpc::CallRemoteMethod", [&](void* arg) {
          for (const auto& ql : static_cast<tserver::LWReadRequestPB*>(arg)->ql_batch()) {
            if (!ql.return_paging_state() || ql.limit() != page_rows) ++wrong_limits;
          }
        });
        auto iter = make_iter(perm, tablet.tablet_id, 10);
        ASSERT_NE(iter, nullptr);
        Binding binding(3);
        sync->EnableProcessing();
        iter->begin(binding);
        ASSERT_NO_FATALS(Drain(*iter, binding, vars, expected));
        sync->DisableProcessing();
        ASSERT_EQ(profile.scanned_rows, expected.size() + 2 * kTriples);
        ASSERT_EQ(profile.loaded_rows, profile.scanned_rows);
        combined.insert(expected.begin(), expected.end());
        // No-match filters must not turn into an unfiltered scan.
        iter = make_iter(perm, tablet.tablet_id, 99);
        profile = {};
        sync->EnableProcessing();
        iter->begin(binding);
        ASSERT_NO_FATALS(Drain(*iter, binding, vars, {}));
        sync->DisableProcessing();
        ASSERT_EQ(profile.scanned_rows, 2 * kTriples);
        if (expected.size() <= page_rows) continue;
        ++continuation_tablets;
        for (const bool cancel : {false, true}) {
          std::atomic<bool> armed{false};
          std::atomic<int> continuations{0};
          sync->SetCallBack("ReadRpc::CallRemoteMethod", [&](void* arg) {
            for (const auto& ql : static_cast<tserver::LWReadRequestPB*>(arg)->ql_batch()) {
              if (ql.has_where_expr() && ql.has_paging_state() &&
                  !ql.paging_state().next_row_key().empty()) {
                ++continuations;
                armed = true;
              }
            }
          });
          sync->SetCallBack("ReadRpc::NotifyBatcher", [&](void* arg) {
            if (!armed.exchange(false)) return;
            if (cancel) {
              get_query_ctx().thread_info.interruption_requested = true;
            } else {
              for (auto& ql : *static_cast<tserver::LWReadResponsePB*>(arg)->mutable_ql_batch()) {
                ql.set_status(QLResponsePB::YQL_STATUS_RUNTIME_ERROR);
                ql.dup_error_message("injected filtered anchor continuation failure");
              }
            }
          });
          iter = make_iter(perm, tablet.tablet_id, 10);
          sync->EnableProcessing();
          iter->begin(binding);
          sync->DisableProcessing();
          ASSERT_GT(continuations.load(), 0);
          ASSERT_FALSE(iter->status().ok());
          if (cancel)
            ASSERT_TRUE(iter->status().IsAborted()) << iter->status();
          else
            ASSERT_NE(iter->status().ToString().find("filtered anchor"), std::string::npos);
          get_query_ctx().thread_info.interruption_requested = false;
          ASSERT_FALSE(iter->next());
          iter->reset();
          iter->begin(binding);
          ASSERT_FALSE(iter->next());
          ASSERT_FALSE(iter->status().ok());
          sync->SetCallBack("ReadRpc::CallRemoteMethod", [](void*) {});
          sync->SetCallBack("ReadRpc::NotifyBatcher", [](void*) {});
        }
      }
      Bag expected_all;
      for (uint64_t i = 0; i < kTriples; ++i) expected_all.insert({1000 + i, 3000 + i, 5000 + i});
      ASSERT_EQ(combined, expected_all) << "perm=" << perm;
      LOG(INFO) << "MDB_ANCHOR_PAGE_CHECK page_rows=" << page_rows << " perm=" << perm
                << " continuation_tablets=" << continuation_tablets
                << " wrong_limits=" << wrong_limits.load();
      ASSERT_EQ(wrong_limits.load(), 0);
      ASSERT_GT(continuation_tablets, 0);
    }
  }
}

TEST_F(MdbLeapfrogDocDBTest, EstimatorSamplesOnlyAlreadyLoadedScans) {
  FLAGS_mdb_enable_leapfrog_join = true;
  for (uint64_t i = 0; i < 1025; ++i) ASSERT_NO_FATALS(Write({1000 + i, 10, 3000 + i}));
  ASSERT_OK(RdfYBModel::Instance().Flush());
  auto* sync = SyncPoint::GetInstance();
  auto cleanup = ScopeExit([&] {
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  });
  std::atomic<uint64_t> requests{0};
  sync->SetCallBack("ReadRpc::CallRemoteMethod", [&](void* arg) {
    requests += static_cast<tserver::LWReadRequestPB*>(arg)->ql_batch().size();
  });
  const auto append = [](uint64_t predicate, std::vector<std::unique_ptr<LeapfrogIter>>& scans) {
    SPARQL::TriplePlan plan(Id(VarId(0)), Id(::ObjectId(predicate)), Id(VarId(1)));
    ASSERT_TRUE(SPARQL::AppendDocDBLeapfrogScan(plan, &scans, {VarId(0), VarId(1), VarId(2)}, 3));
  };
  std::vector<std::unique_ptr<LeapfrogIter>> scans;
  ASSERT_NO_FATALS(append(10, scans));
  auto* observed = scans.front().get();
  LeapfrogEstimator estimator(VarId(0), std::move(scans));
  sync->EnableProcessing();
  ASSERT_TRUE(ResultToStatus(estimator.EstimateVarCost(3)).IsNotSupported());
  ASSERT_TRUE(ResultToStatus(estimator.SampleVarWork(3)).IsNotSupported());
  ASSERT_EQ(requests.load(), 0);
  ASSERT_OK(observed->status());
  Binding binding(3);
  ASSERT_TRUE(observed->open_terms(binding));
  const auto loaded_requests = requests.load();
  ASSERT_GT(loaded_requests, 0);
  for (int repeat = 0; repeat < 2; ++repeat) {
    const auto cost = estimator.EstimateVarCost(3);
    ASSERT_OK(cost);
    ASSERT_GT(*cost, 0);
    const auto work = estimator.SampleVarWork(3);
    ASSERT_OK(work);
    ASSERT_FALSE(work->finished);
    ASSERT_EQ(work->direct_seeks, LeapfrogEstimator::MAX_SEEKS);
    ASSERT_EQ(work->progress[0].unit, LeapfrogIter::ProgressCountUnit::kTuples);
    ASSERT_EQ(requests.load(), loaded_requests);
  }
  std::vector<std::unique_ptr<LeapfrogIter>> empty;
  ASSERT_NO_FATALS(append(99, empty));
  ASSERT_TRUE(ResultToStatus(empty.front()->open_for_estimation(binding)).IsNotSupported());
  ASSERT_FALSE(empty.front()->open_terms(binding));
  ASSERT_OK(empty.front()->status());
  const auto empty_requests = requests.load();
  LeapfrogEstimator empty_estimator(VarId(0), std::move(empty));
  const auto empty_cost = empty_estimator.EstimateVarCost(3);
  ASSERT_OK(empty_cost);
  ASSERT_DOUBLE_EQ(*empty_cost, 0);
  const auto empty_work = empty_estimator.SampleVarWork(3);
  ASSERT_OK(empty_work);
  ASSERT_TRUE(empty_work->finished);
  ASSERT_TRUE(empty_work->has_empty_input);
  ASSERT_EQ(empty_work->direct_seeks, 0);
  ASSERT_EQ(requests.load(), empty_requests);
}

TEST_F(MdbLeapfrogDocDBTest, EstimatesUseSuccessfulCachesWithoutExtraReads) {
  FLAGS_mdb_enable_leapfrog_join = true;
  const auto old_page_rows = FLAGS_mdb_leapfrog_read_page_rows;
  FLAGS_mdb_leapfrog_read_page_rows = 1024;
  for (uint64_t i = 0; i < 1025; ++i) {
    ASSERT_NO_FATALS(Write({1000 + i, 10, 3000 + i}));
    ASSERT_NO_FATALS(Write({9000 + i, 40, 11000 + i}));
  }
  ASSERT_OK(RdfYBModel::Instance().Flush());
  auto* sync = SyncPoint::GetInstance();
  auto cleanup = ScopeExit([&] {
    FLAGS_mdb_leapfrog_read_page_rows = old_page_rows;
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  });
  std::atomic<uint64_t> requests{0};
  sync->SetCallBack("ReadRpc::CallRemoteMethod", [&](void* arg) {
    requests += static_cast<tserver::LWReadRequestPB*>(arg)->ql_batch().size();
  });
  const auto append = [](uint64_t predicate, std::vector<std::unique_ptr<LeapfrogIter>>& scans) {
    SPARQL::TriplePlan plan(Id(VarId(0)), Id(::ObjectId(predicate)), Id(VarId(1)));
    ASSERT_TRUE(SPARQL::AppendDocDBLeapfrogScan(plan, &scans, {VarId(0), VarId(1), VarId(2)}, 3));
  };
  const auto unavailable = [&](const LeapfrogIter& iter) {
    const auto count = requests.load();
    const auto status = iter.status().ToString();
    std::vector<double> before{7}, after{3};
    ASSERT_FALSE(iter.try_estimate(before, after));
    ASSERT_EQ(before, (std::vector<double>{7}));
    ASSERT_EQ(after, (std::vector<double>{3}));
    ASSERT_EQ(iter.status().ToString(), status);
    ASSERT_EQ(requests.load(), count);
  };
  const auto estimate = [&](const LeapfrogIter& iter, double all, double remaining) {
    const auto count = requests.load();
    const auto key = iter.get_key();
    std::vector<double> before, after;
    ASSERT_TRUE(iter.try_estimate(before, after));
    ASSERT_EQ(before, (std::vector<double>{all}));
    ASSERT_EQ(after, (std::vector<double>{remaining}));
    ASSERT_EQ(iter.get_key(), key);
    ASSERT_EQ(requests.load(), count);
  };
  Binding binding(3);
  std::vector<std::unique_ptr<LeapfrogIter>> first, second;
  sync->EnableProcessing();
  {
    SPARQL::DocDBLeapfrogQueryScope scope;
    ASSERT_NO_FATALS(append(10, first));
    ASSERT_NO_FATALS(unavailable(*first.front()));
    ASSERT_EQ(requests.load(), 0);
    ASSERT_TRUE(first.front()->open_terms(binding));
    ASSERT_GT(requests.load(), 0);
    ASSERT_NO_FATALS(unavailable(*first.front()));
    first.front()->down();
    ASSERT_NO_FATALS(estimate(*first.front(), 1025, 1025));
    ASSERT_TRUE(first.front()->seek(1100));
    ASSERT_NO_FATALS(estimate(*first.front(), 1025, 925));
    first.front()->down();
    ASSERT_NO_FATALS(estimate(*first.front(), 1, 1));
    first.front()->up();
    ASSERT_NO_FATALS(estimate(*first.front(), 1025, 925));
    const auto after_load = requests.load();
    ASSERT_NO_FATALS(append(10, second));
    ASSERT_NO_FATALS(unavailable(*second.front()));
    ASSERT_TRUE(second.front()->open_terms(binding));
    second.front()->down();
    ASSERT_TRUE(static_cast<const LeapfrogDocDBIter&>(*second.front()).uses_shared_cache());
    ASSERT_NO_FATALS(estimate(*second.front(), 1025, 1025));
    ASSERT_EQ(requests.load(), after_load);
  }
  ASSERT_NO_FATALS(estimate(*first.front(), 1025, 925));
  ASSERT_NO_FATALS(estimate(*second.front(), 1025, 1025));
  std::atomic<bool> armed{false};
  std::atomic<int> continuations{0};
  sync->DisableProcessing();
  sync->SetCallBack("ReadRpc::CallRemoteMethod", [&](void* arg) {
    for (const auto& ql : static_cast<tserver::LWReadRequestPB*>(arg)->ql_batch()) {
      ++requests;
      if (ql.has_paging_state() && !ql.paging_state().next_row_key().empty()) {
        ++continuations;
        armed = true;
      }
    }
  });
  sync->SetCallBack("ReadRpc::NotifyBatcher", [&](void* arg) {
    if (!armed.exchange(false)) return;
    for (auto& ql : *static_cast<tserver::LWReadResponsePB*>(arg)->mutable_ql_batch()) {
      ql.set_status(QLResponsePB::YQL_STATUS_RUNTIME_ERROR);
      ql.dup_error_message("injected estimate load continuation failure");
    }
  });
  std::vector<std::unique_ptr<LeapfrogIter>> failed;
  ASSERT_NO_FATALS(append(40, failed));
  sync->EnableProcessing();
  ASSERT_NO_FATALS(unavailable(*failed.front()));
  ASSERT_FALSE(failed.front()->open_terms(binding));
  ASSERT_GT(continuations.load(), 0);
  ASSERT_FALSE(failed.front()->status().ok());
  ASSERT_NE(failed.front()->status().ToString().find("injected estimate"), std::string::npos);
  ASSERT_NO_FATALS(unavailable(*failed.front()));
}

TEST_F(MdbLeapfrogDocDBTest, PagedReadsAndLateChildErrorsRemainVisible) {
  FLAGS_mdb_enable_leapfrog_join = true;
  const auto previous_page_rows = FLAGS_mdb_leapfrog_read_page_rows;
  auto restore = ScopeExit([&] { FLAGS_mdb_leapfrog_read_page_rows = previous_page_rows; });
  for (uint64_t i = 0; i < 1025; ++i) {
    ASSERT_NO_FATALS(Write({1000 + i, 10, 3000 + i}));
    ASSERT_NO_FATALS(Write({3000 + i, 20, 5000 + i}));
    ASSERT_NO_FATALS(Write({1000 + i, 30, 5000 + i}));
  }
  ASSERT_OK(RdfYBModel::Instance().Flush());
  for (const auto page_rows : {128, 1024}) {
    FLAGS_mdb_leapfrog_read_page_rows = page_rows;
    for (const bool first_empty : {false, true}) {
      auto op = Parse(
          std::string("SELECT ?x ?y ?z WHERE { ?x <https://example.org/lf/") +
          (first_empty ? "missing" : "p") +
          "> ?y . ?y <https://example.org/lf/q> ?z . ?x <https://example.org/lf/r> ?z }");
      const auto& select = dynamic_cast<const SPARQL::OpSelect&>(*op);
      SPARQL::BindingIterConstructor constructor;
      op->accept_visitor(constructor);
      Bag expected;
      if (!first_empty) {
        for (uint64_t i = 0; i < 1025; ++i) expected.insert({1000 + i, 3000 + i, 5000 + i});
      }
      Binding binding(get_query_ctx().get_var_size());
      constructor.tmp->begin(binding);
      ASSERT_NO_FATALS(Drain(*constructor.tmp, binding, select.vars, expected));

      auto* sync = SyncPoint::GetInstance();
      auto cleanup = ScopeExit([sync] {
        sync->DisableProcessing();
        sync->ClearAllCallBacks();
      });
      std::atomic<int> continuations{0};
      std::atomic<bool> fail_response{false};
      sync->SetCallBack("ReadRpc::CallRemoteMethod", [&](void* arg) {
        const auto* request = static_cast<tserver::LWReadRequestPB*>(arg);
        for (const auto& ql : request->ql_batch()) {
          if (ql.has_paging_state() && (!ql.paging_state().next_row_key().empty() ||
                                        !ql.paging_state().next_partition_key().empty())) {
            if (++continuations == 1) fail_response = true;
          }
        }
      });
      sync->SetCallBack("ReadRpc::NotifyBatcher", [&](void* arg) {
        if (!fail_response.exchange(false)) return;
        auto* response = static_cast<tserver::LWReadResponsePB*>(arg);
        for (auto& ql : *response->mutable_ql_batch()) {
          ql.set_status(QLResponsePB::YQL_STATUS_RUNTIME_ERROR);
          ql.dup_error_message("injected Leapfrog second-page failure");
        }
      });
      SPARQL::BindingIterConstructor failing;
      op->accept_visitor(failing);
      sync->EnableProcessing();
      failing.tmp->begin(binding);
      sync->DisableProcessing();
      ASSERT_GE(continuations.load(), 1);
      ASSERT_FALSE(failing.tmp->status().ok());
      ASSERT_FALSE(failing.tmp->next());
      ASSERT_NE(failing.tmp->status().ToString().find("injected Leapfrog"), std::string::npos);
      failing.tmp->reset();
      failing.tmp->begin(binding);
      ASSERT_FALSE(failing.tmp->next());
      ASSERT_FALSE(failing.tmp->status().ok());

      continuations = 0;
      fail_response = false;
      DistributedPlan plan;
      DistributedPlanGenerator generator(&router_);
      ASSERT_OK(generator.Generate(*op, &plan));
      ScatterExecutor executor;
      std::vector<ScatterExecutor::FragmentResultSet> fragments;
      sync->EnableProcessing();
      const auto error = executor.Execute(plan, &fragments);
      sync->DisableProcessing();
      ASSERT_GE(continuations.load(), 1);
      ASSERT_FALSE(error.ok());
      ASSERT_NE(error.ToString().find("injected Leapfrog"), std::string::npos) << error;
    }
  }
}

TEST_F(MdbLeapfrogDocDBTest, InvalidReadPageSizeFailsBeforeReads) {
  FLAGS_mdb_enable_leapfrog_join = true;
  const auto previous_page_rows = FLAGS_mdb_leapfrog_read_page_rows;
  auto* sync = SyncPoint::GetInstance();
  auto cleanup = ScopeExit([&] {
    FLAGS_mdb_leapfrog_read_page_rows = previous_page_rows;
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  });
  std::atomic<int> requests{0};
  sync->SetCallBack("ReadRpc::CallRemoteMethod", [&](void*) { ++requests; });
  for (const auto page_rows : {0, 65537}) {
    FLAGS_mdb_leapfrog_read_page_rows = page_rows;
    auto op = Parse(
        "SELECT ?x ?y ?z WHERE { ?x <https://example.org/lf/p> ?y . "
        "?y <https://example.org/lf/q> ?z . ?x <https://example.org/lf/r> ?z }");
    SPARQL::BindingIterConstructor constructor;
    op->accept_visitor(constructor);
    Binding binding(get_query_ctx().get_var_size());
    sync->EnableProcessing();
    constructor.tmp->begin(binding);
    sync->DisableProcessing();
    ASSERT_TRUE(constructor.tmp->status().IsInvalidArgument()) << constructor.tmp->status();
    ASSERT_FALSE(constructor.tmp->next());
    ASSERT_EQ(requests.load(), 0);
    DistributedPlan plan;
    DistributedPlanGenerator generator(&router_);
    ASSERT_OK(generator.Generate(*op, &plan));
    ASSERT_EQ(plan.num_fragments(), 4);
    ScatterExecutor executor;
    std::vector<ScatterExecutor::FragmentResultSet> fragments;
    sync->EnableProcessing();
    const auto status = executor.Execute(plan, &fragments);
    sync->DisableProcessing();
    ASSERT_TRUE(status.IsInvalidArgument()) << status;
    ASSERT_EQ(requests.load(), 0);
  }
}

TEST_F(MdbLeapfrogDocDBTest, ReuseBudgetFallbackAndFreshQueryReads) {
  FLAGS_mdb_enable_leapfrog_join = true;
  const auto old_budget = FLAGS_mdb_leapfrog_reuse_bytes;
  const auto old_profile = FLAGS_mdb_profile_leapfrog;
  auto* sync = SyncPoint::GetInstance();
  auto cleanup = ScopeExit([&] {
    FLAGS_mdb_leapfrog_reuse_bytes = old_budget;
    FLAGS_mdb_profile_leapfrog = old_profile;
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  });
  FLAGS_mdb_profile_leapfrog = true;
  std::vector<Triple> data;
  for (uint64_t i = 0; i < 129; ++i) {
    data.push_back({1000 + i, 10, 3000 + i});
    data.push_back({3000 + i, 20, 5000 + i});
    data.push_back({1000 + i, 30, 5000 + i});
  }
  for (const auto& row : data) ASSERT_NO_FATALS(Write(row));
  ASSERT_OK(RdfYBModel::Instance().Flush());
  auto op = Parse(
      "SELECT ?x ?y ?z WHERE { ?x <https://example.org/lf/p> ?y . "
      "?y <https://example.org/lf/q> ?z . ?x <https://example.org/lf/r> ?z }");
  MapNodes(data);
  SPARQL::DocDBLeapfrogReuseStats reuse;
  uint64_t loads = 0;
  std::atomic<uint64_t> requests{0};
  sync->SetCallBack("MDBLeapfrogQuery::Reuse", [&](void* arg) {
    reuse = *static_cast<SPARQL::DocDBLeapfrogReuseStats*>(arg);
  });
  sync->SetCallBack("MDBLeapfrogTriangle::Phases", [&](void* arg) {
    loads += static_cast<SPARQL::DocDBLeapfrogProfile*>(arg)->loads;
  });
  sync->SetCallBack("ReadRpc::CallRemoteMethod", [&](void* arg) {
    requests += static_cast<tserver::LWReadRequestPB*>(arg)->ql_batch().size();
  });
  FLAGS_mdb_leapfrog_reuse_bytes = 8 * 1024 * 1024;
  sync->EnableProcessing();
  ASSERT_NO_FATALS(CheckDistributed(*op, Oracle(data, false)));
  sync->DisableProcessing();
  ASSERT_EQ(reuse.hits, 6);
  ASSERT_EQ(reuse.misses, 2);
  ASSERT_EQ(reuse.admissions, 2);
  ASSERT_EQ(loads, 6);
  ASSERT_GT(reuse.retained_bytes, 2);
  const auto retained = reuse.retained_bytes;
  const auto reused_requests = requests.load();
  for (const uint64_t budget : {uint64_t{0}, uint64_t{1}, retained / 2}) {
    FLAGS_mdb_leapfrog_reuse_bytes = budget;
    loads = 0;
    requests = 0;
    sync->EnableProcessing();
    ASSERT_NO_FATALS(CheckDistributed(*op, Oracle(data, false)));
    sync->DisableProcessing();
    ASSERT_LE(reuse.retained_bytes, budget);
    ASSERT_EQ(reuse.admissions, budget > 1 ? 1 : 0);
    ASSERT_EQ(reuse.hits, budget > 1 ? 3 : 0);
    ASSERT_EQ(reuse.rejections, budget > 1 ? 4 : 8);
    ASSERT_EQ(loads, budget > 1 ? 9 : 12);
    ASSERT_GT(requests.load(), reused_requests);
  }
  // A subsequent Execute must observe new data, even with the same parsed plan.
  for (const Triple row : {Triple{3000, 20, 7000}, Triple{1000, 30, 7000}}) {
    ASSERT_NO_FATALS(Write(row));
    data.push_back(row);
  }
  ASSERT_OK(RdfYBModel::Instance().Flush());
  MapNodes(data);
  const auto expected = Oracle(data, false);
  ASSERT_EQ(expected.size(), 130);
  FLAGS_mdb_leapfrog_reuse_bytes = 8 * 1024 * 1024;
  loads = 0;
  sync->EnableProcessing();
  ASSERT_NO_FATALS(CheckDistributed(*op, expected));
  sync->DisableProcessing();
  ASSERT_EQ(reuse.misses, 2);
  ASSERT_EQ(reuse.hits, 6);
  ASSERT_EQ(loads, 6);
}

TEST_F(MdbLeapfrogDocDBTest, ReuseScopesRestoreAndCursorsOutliveOwner) {
  FLAGS_mdb_enable_leapfrog_join = true;
  for (uint64_t i = 0; i < 33; ++i) {
    ASSERT_NO_FATALS(Write({1000 + i, 10, 3000 + i}));
    ASSERT_NO_FATALS(Write({3000 + i, 20, 5000 + i}));
    ASSERT_NO_FATALS(Write({1000 + i, 30, 5000 + i}));
  }
  ASSERT_OK(RdfYBModel::Instance().Flush());
  auto op = Parse(
      "SELECT ?x ?y ?z WHERE { ?x <https://example.org/lf/p> ?y . "
      "?y <https://example.org/lf/q> ?z . ?x <https://example.org/lf/r> ?z }");
  const auto& vars = dynamic_cast<const SPARQL::OpSelect&>(*op).vars;
  Bag expected;
  for (uint64_t i = 0; i < 33; ++i) expected.insert({1000 + i, 3000 + i, 5000 + i});
  auto* sync = SyncPoint::GetInstance();
  auto cleanup = ScopeExit([&] {
    get_query_ctx().thread_info.interruption_requested = false;
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  });
  std::vector<SPARQL::DocDBLeapfrogReuseStats> reports;
  std::atomic<int> requests{0};
  sync->SetCallBack("MDBLeapfrogQuery::Reuse", [&](void* arg) {
    reports.push_back(*static_cast<SPARQL::DocDBLeapfrogReuseStats*>(arg));
  });
  sync->SetCallBack("ReadRpc::CallRemoteMethod", [&](void*) { ++requests; });
  sync->EnableProcessing();
  SPARQL::BindingIterConstructor first, second;
  Binding a(get_query_ctx().get_var_size()), b(get_query_ctx().get_var_size());
  {
    SPARQL::DocDBLeapfrogQueryScope outer;
    op->accept_visitor(first);
    first.tmp->begin(a);
    ASSERT_OK(first.tmp->status());
    {
      SPARQL::DocDBLeapfrogQueryScope inner;
      SPARQL::BindingIterConstructor nested;
      op->accept_visitor(nested);
      Binding binding(get_query_ctx().get_var_size());
      nested.tmp->begin(binding);
      ASSERT_NO_FATALS(Drain(*nested.tmp, binding, vars, expected));
    }
    op->accept_visitor(second);
    second.tmp->begin(b);
    ASSERT_OK(second.tmp->status());
    const auto before_cancel = requests.load();
    get_query_ctx().thread_info.interruption_requested = true;
    SPARQL::BindingIterConstructor cancelled;
    op->accept_visitor(cancelled);
    Binding cancelled_binding(get_query_ctx().get_var_size());
    cancelled.tmp->begin(cancelled_binding);
    ASSERT_TRUE(cancelled.tmp->status().IsAborted());
    ASSERT_EQ(requests.load(), before_cancel);
    get_query_ctx().thread_info.interruption_requested = false;
    ASSERT_TRUE(first.tmp->next());
    ASSERT_TRUE(second.tmp->next());
    for (const auto var : vars) ASSERT_EQ(a[var], b[var]);
  }
  ASSERT_EQ(reports.size(), 2);
  ASSERT_EQ(reports[0].hits, 0);
  ASSERT_EQ(reports[0].admissions, 3);
  ASSERT_EQ(reports[1].hits, 3);
  ASSERT_EQ(reports[1].admissions, 3);
  const auto after_loads = requests.load();
  expected.erase(expected.begin());
  ASSERT_NO_FATALS(Drain(*first.tmp, a, vars, expected));
  ASSERT_NO_FATALS(Drain(*second.tmp, b, vars, expected));
  first.tmp->reset();
  expected.insert({1000, 3000, 5000});
  ASSERT_NO_FATALS(Drain(*first.tmp, a, vars, expected));
  ASSERT_EQ(requests.load(), after_loads);
}

TEST_F(MdbLeapfrogDocDBTest, FailedPartialLoadsAreNotReused) {
  FLAGS_mdb_enable_leapfrog_join = true;
  for (uint64_t i = 0; i < 1025; ++i) {
    ASSERT_NO_FATALS(Write({1000 + i, 10, 3000 + i}));
    ASSERT_NO_FATALS(Write({3000 + i, 20, 5000 + i}));
    ASSERT_NO_FATALS(Write({1000 + i, 30, 5000 + i}));
  }
  ASSERT_OK(RdfYBModel::Instance().Flush());
  auto op = Parse(
      "SELECT ?x ?y ?z WHERE { ?x <https://example.org/lf/p> ?y . "
      "?y <https://example.org/lf/q> ?z . ?x <https://example.org/lf/r> ?z }");
  const auto& vars = dynamic_cast<const SPARQL::OpSelect&>(*op).vars;
  auto* sync = SyncPoint::GetInstance();
  auto cleanup = ScopeExit([sync] {
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  });
  std::atomic<bool> fail_next{false};
  sync->SetCallBack("ReadRpc::CallRemoteMethod", [&](void* arg) {
    for (const auto& ql : static_cast<tserver::LWReadRequestPB*>(arg)->ql_batch()) {
      if (ql.has_paging_state() && !ql.paging_state().next_row_key().empty()) fail_next = true;
    }
  });
  sync->SetCallBack("ReadRpc::NotifyBatcher", [&](void* arg) {
    if (!fail_next.exchange(false)) return;
    for (auto& ql : *static_cast<tserver::LWReadResponsePB*>(arg)->mutable_ql_batch()) {
      ql.set_status(QLResponsePB::YQL_STATUS_RUNTIME_ERROR);
      ql.dup_error_message("injected partial reuse load failure");
    }
  });
  SPARQL::DocDBLeapfrogReuseStats stats;
  sync->SetCallBack("MDBLeapfrogQuery::Reuse", [&](void* arg) {
    stats = *static_cast<SPARQL::DocDBLeapfrogReuseStats*>(arg);
  });
  {
    SPARQL::DocDBLeapfrogQueryScope scope;
    SPARQL::BindingIterConstructor failing;
    op->accept_visitor(failing);
    Binding binding(get_query_ctx().get_var_size());
    sync->EnableProcessing();
    failing.tmp->begin(binding);
    ASSERT_FALSE(failing.tmp->status().ok());
    ASSERT_NE(failing.tmp->status().ToString().find("injected partial reuse"), std::string::npos);
    sync->DisableProcessing();
    sync->SetCallBack("ReadRpc::CallRemoteMethod", [](void*) {});
    sync->SetCallBack("ReadRpc::NotifyBatcher", [](void*) {});
    sync->EnableProcessing();
    Bag expected;
    for (uint64_t i = 0; i < 1025; ++i) expected.insert({1000 + i, 3000 + i, 5000 + i});
    for (int attempt = 0; attempt < 2; ++attempt) {
      SPARQL::BindingIterConstructor good;
      op->accept_visitor(good);
      good.tmp->begin(binding);
      ASSERT_NO_FATALS(Drain(*good.tmp, binding, vars, expected));
    }
  }
  ASSERT_EQ(stats.misses, 4);
  ASSERT_EQ(stats.admissions, 3);
  ASSERT_EQ(stats.hits, 3);
}

TEST_F(MdbLeapfrogDocDBTest, ReuseKeysSeparatePredicatePermutations) {
  FLAGS_mdb_enable_leapfrog_join = true;
  for (const Triple row : {Triple{1000, 10, 2000}, Triple{2000, 10, 3000}, Triple{1000, 10, 3000}})
    ASSERT_NO_FATALS(Write(row));
  ASSERT_OK(RdfYBModel::Instance().Flush());
  auto* sync = SyncPoint::GetInstance();
  auto cleanup = ScopeExit([sync] {
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  });
  SPARQL::DocDBLeapfrogReuseStats stats;
  sync->SetCallBack("MDBLeapfrogQuery::Reuse", [&](void* arg) {
    stats = *static_cast<SPARQL::DocDBLeapfrogReuseStats*>(arg);
  });
  sync->EnableProcessing();
  {
    SPARQL::DocDBLeapfrogQueryScope scope;
    for (const bool reverse : {false, true}) {
      auto op = Parse(
          reverse ? "SELECT ?x ?y ?z WHERE { ?y <https://example.org/lf/p> ?x . "
                    "?z <https://example.org/lf/p> ?y . ?z <https://example.org/lf/p> ?x }"
                  : "SELECT ?x ?y ?z WHERE { ?x <https://example.org/lf/p> ?y . "
                    "?y <https://example.org/lf/p> ?z . ?x <https://example.org/lf/p> ?z }");
      const auto& vars = dynamic_cast<const SPARQL::OpSelect&>(*op).vars;
      SPARQL::BindingIterConstructor constructor;
      op->accept_visitor(constructor);
      Binding binding(get_query_ctx().get_var_size());
      constructor.tmp->begin(binding);
      const Bag expected{reverse ? Row{3000, 2000, 1000} : Row{1000, 2000, 3000}};
      ASSERT_NO_FATALS(Drain(*constructor.tmp, binding, vars, expected));
    }
  }
  ASSERT_EQ(stats.admissions, 2);
  ASSERT_EQ(stats.misses, 2);
  ASSERT_EQ(stats.hits, 4);
}

TEST_F(MdbLeapfrogDocDBTest, YB_DEBUG_ONLY_TEST(GreedyLeapfrogSparseComparison)) {
  ASSERT_NO_FATALS(CompareTriangleAlgorithms("sparse"));
}

TEST_F(MdbLeapfrogDocDBTest, YB_DEBUG_ONLY_TEST(GreedyLeapfrogDenseComparison)) {
  ASSERT_NO_FATALS(CompareTriangleAlgorithms("dense"));
}

TEST_F(MdbLeapfrogDocDBTest, YB_DEBUG_ONLY_TEST(GreedyLeapfrogSkewComparison)) {
  ASSERT_NO_FATALS(CompareTriangleAlgorithms("skew"));
}

TEST_F(MdbLeapfrogDocDBTest, YB_DEBUG_ONLY_TEST(GreedyLeapfrogEmptyComparison)) {
  ASSERT_NO_FATALS(CompareTriangleAlgorithms("empty"));
}

TEST_F(MdbLeapfrogDocDBTest, YB_DEBUG_ONLY_TEST(Leapfrog25kSparseRetentionComparison)) {
  if (!std::getenv("MDB_LEAPFROG_SCALE_RUN")) GTEST_SKIP() << "Explicit scale run required";
  ASSERT_NO_FATALS(CompareTriangleAlgorithms("sparse", 8192, true));
}

TEST_F(MdbLeapfrogDocDBTest, YB_DEBUG_ONLY_TEST(Leapfrog25kSkewRetentionComparison)) {
  if (!std::getenv("MDB_LEAPFROG_SCALE_RUN")) GTEST_SKIP() << "Explicit scale run required";
  ASSERT_NO_FATALS(CompareTriangleAlgorithms("skew", 8192, true));
}

}  // namespace yb::mdb

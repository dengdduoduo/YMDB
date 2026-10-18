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


#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <tuple>
#include <vector>

#include <gtest/gtest.h>

#include "graph_models/rdf_model/rdf_catalog.h"
#include "query/executor/binding.h"
#include "query/executor/binding_iter/hash_join_binding_iter.h"
#include "query/executor/binding_iter/index_nested_loop_join.h"
#include "query/optimizer/binding_iter_constructor.h"
#include "query/optimizer/plan/join_order/greedy_optimizer.h"
#include "query/optimizer/plan/join/hash_join_plan.h"
#include "query/optimizer/plan/join/index_nested_loop_plan.h"
#include "query/optimizer/rdf_model/plan/triple_plan.h"
#include "query/parser/op/sparql/op_basic_graph_pattern.h"
#include "query/parser/op/sparql/op_select.h"
#include "query/parser/sparql_query_parser.h"
#include "query/query_context.h"

#include "yb/client/schema.h"
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
#include "yb/tserver/tserver.messages.h"
#include "yb/util/scope_exit.h"
#include "yb/util/flags.h"
#include "yb/util/status.h"
#include "yb/util/sync_point.h"
#include "yb/util/test_macros.h"

DECLARE_bool(mdb_enable_hash_join);
DECLARE_uint64(mdb_hash_join_max_estimated_build_bytes);

using SPARQL::BindingIterConstructor;
using SPARQL::Op;
using SPARQL::OpBasicGraphPattern;
using SPARQL::OpSelect;
using SPARQL::QueryParser;
using SPARQL::TriplePlan;

namespace yb::mdb {

class MdbDistributedIntegrationTest : public MiniClusterTestWithClient<MiniCluster> {
 protected:
  static constexpr const char* kNamespace = "mdb_dist_test";
  static constexpr int kNumTablets = 4;

  void SetUp() override {
    YBMiniClusterTestBase<MiniCluster>::SetUp();
    MiniClusterOptions opts;
    opts.num_tablet_servers = 1;
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
    builder.AddColumn("c0")->Type(DataType::INT64)->HashPrimaryKey()->NotNull();
    builder.AddColumn("c1")->Type(DataType::INT64)->PrimaryKey()->NotNull();
    builder.AddColumn("c2")->Type(DataType::INT64)->PrimaryKey()->NotNull();

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

  static uint64_t ParseIriForTest(const std::string& iri) {
    QueryParser parser("SELECT ?p ?o WHERE { <" + iri + "> ?p ?o }");
    auto op = parser.get_query_plan({});
    const auto& select = dynamic_cast<const OpSelect&>(*op);
    const auto& bgp = dynamic_cast<const OpBasicGraphPattern&>(*select.op);
    const auto& subject = bgp.triples.at(0).subject;
    if (!subject.is_OID()) {
      throw std::runtime_error("IRI fixture did not parse as an ObjectId");
    }
    return subject.get_OID().id;
  }

  void CheckFixedPattern(unsigned fixed_mask, int expected_perm, bool missing = false) {
    ASSERT_NO_FATALS(CreateRdfTables());
    ASSERT_NO_FATALS(InitModel());
    ASSERT_TRUE(RdfYBModel::Instance().IsConnected());

    using Triple = std::array<uint64_t, 3>;
    const std::array<std::string, 3> iris{
        "https://example.org/s1", "https://example.org/p1", "https://example.org/o1"};
    auto pack = ParseIriForTest;
    const Triple anchor{pack(iris[0]), pack(iris[1]), pack(iris[2])};
    const uint64_t s2 = pack("https://example.org/s2");
    const uint64_t p2 = pack("https://example.org/p2");
    const uint64_t o2 = pack("https://example.org/o2");
    std::vector<Triple> data{
        anchor, {anchor[0], anchor[1], o2}, {anchor[0], p2, anchor[2]},
        {s2, anchor[1], anchor[2]}, {s2, p2, o2}};
    // Fixed S and SP queries must continue within a tablet across multiple pages.
    for (int i = 0; i < 140; ++i) {
      data.push_back({anchor[0], anchor[1], pack("https://example.org/page/" + std::to_string(i))});
    }
    auto& model = RdfYBModel::Instance();
    for (const auto& triple : data) {
      ASSERT_OK(model.WriteTripleToDocDB(triple[0], triple[1], triple[2]));
    }
    ASSERT_OK(model.Flush());
    ASSERT_NO_FATALS(InitRouter());

    auto query_iris = iris;
    Triple pattern = anchor;
    if (missing) {
      ASSERT_EQ(fixed_mask, 7u);
      query_iris[2] = "https://example.org/missing";
      pattern[2] = pack(query_iris[2]);
    }
    std::string query = "SELECT * WHERE { ";
    const std::array<std::string, 3> variables{"s", "p", "o"};
    for (size_t column = 0; column < 3; ++column) {
      query += (fixed_mask & (1u << column))
          ? "<" + query_iris[column] + "> " : "?" + variables[column] + " ";
    }
    query += "}";
    SCOPED_TRACE(query);

    std::multiset<Triple> expected;
    for (const auto& triple : data) {
      bool matches = true;
      for (size_t column = 0; column < 3; ++column) {
        if ((fixed_mask & (1u << column)) && triple[column] != pattern[column]) {
          matches = false;
        }
      }
      if (matches) {
        expected.insert(triple);
      }
    }
    ASSERT_EQ(expected.empty(), missing);

    // Derive the owning tablet from actual persisted rows, independently of
    // the planner's hash-key calculation and LocateTablet implementation.
    uint64_t c0, c1, c2;
    DocKeyEncoder::Permute(expected_perm, pattern[0], pattern[1], pattern[2], &c0, &c1, &c2);
    const auto& tablets = router_.GetTablets(expected_perm);
    ASSERT_EQ(tablets.size(), static_cast<size_t>(kNumTablets));
    const auto* table = model.GetTable(expected_perm);
    ASSERT_NE(table, nullptr);
    int owner_index = -1;
    std::vector<size_t> tablet_rows;
    for (size_t index = 0; index < tablets.size(); ++index) {
      client::TableIteratorOptions options;
      options.columns = {"c0", "c1", "c2"};
      options.tablet = tablets[index].tablet_id;
      size_t row_count = 0;
      for (const auto& row : client::TableRange(*table, options)) {
        ++row_count;
        if (static_cast<uint64_t>(row.column(0).int64_value()) == c0) {
          if (owner_index == -1) {
            owner_index = static_cast<int>(index);
          }
          ASSERT_EQ(owner_index, static_cast<int>(index));
        }
      }
      tablet_rows.push_back(row_count);
    }
    ASSERT_GE(owner_index, 0);

    std::unique_ptr<Op> op;
    ASSERT_NO_THROW({
      QueryParser parser(query);
      op = parser.get_query_plan({});
    });
    ASSERT_NE(op, nullptr);
    const std::array<VarId, 3> vars{
        get_query_ctx().get_or_create_var("s"), get_query_ctx().get_or_create_var("p"),
        get_query_ctx().get_or_create_var("o")};
    DistributedPlan plan;
    DistributedPlanGenerator generator(&router_);
    ASSERT_OK(generator.Generate(*op, &plan));
    ASSERT_EQ(plan.num_fragments(), 1);
    ASSERT_FALSE(plan.is_distributed());
    const auto& fragment = plan.fragments().front();
    ASSERT_EQ(fragment.perm_index, expected_perm);
    ASSERT_EQ(fragment.tablet_id, tablets[owner_index].tablet_id);
    ASSERT_EQ(fragment.tablet_index, owner_index);
    ASSERT_EQ(fragment.partition_start, tablets[owner_index].partition_start);
    ASSERT_EQ(fragment.partition_end, tablets[owner_index].partition_end);

    ScatterExecutor executor;
    std::vector<ScatterExecutor::FragmentResultSet> fragment_results;
    ASSERT_OK(executor.Execute(plan, &fragment_results));
    ASSERT_EQ(fragment_results.size(), size_t{1});
    ASSERT_OK(fragment_results.front().status);
    GatherOperator gather;
    GatherResult result;
    ASSERT_OK(gather.Merge(fragment_results, plan.gather_strategy(), &result));
    ASSERT_EQ(result.total_input_rows, static_cast<int>(expected.size()));
    ASSERT_EQ(result.total_output_rows, static_cast<int>(expected.size()));
    std::multiset<Triple> actual;
    for (const auto& row : result.bindings) {
      Triple triple = pattern;
      for (size_t column = 0; column < 3; ++column) {
        if (!(fixed_mask & (1u << column))) {
          ASSERT_FALSE((*row)[vars[column]].is_null());
          triple[column] = (*row)[vars[column]].id;
        }
      }
      actual.insert(triple);
    }
    ASSERT_EQ(actual, expected);
    ASSERT_EQ(model.GetTabletCache(expected_perm, fragment.tablet_id).Size(),
              tablet_rows[owner_index]);
    for (int perm = 0; perm < 6; ++perm) {
      ASSERT_FALSE(model.IsLoaded(perm));
      ASSERT_EQ(model.GetCache(perm).Size(), size_t{0});
    }
  }

  void CheckCrossTabletJoin(const std::string& shape) {
    ASSERT_NO_FATALS(CreateRdfTables());
    ASSERT_NO_FATALS(InitModel());
    ASSERT_TRUE(RdfYBModel::Instance().IsConnected());
    auto& model = RdfYBModel::Instance();
    using Triple = std::array<uint64_t, 3>;
    const std::string p_iri = "https://example.org/join/link";
    const std::string q_iri = "https://example.org/join/value";
    const std::string r_iri = "https://example.org/join/end";
    const uint64_t p = ParseIriForTest(p_iri);
    const uint64_t q = ParseIriForTest(q_iri);
    const uint64_t r = ParseIriForTest(r_iri);
    const uint64_t probe_p = ParseIriForTest("https://example.org/join/probe");
    const uint64_t probe_o = ParseIriForTest("https://example.org/join/probe-end");
    std::vector<Triple> data;
    std::map<uint64_t, std::string> names;
    for (int i = 0; i < 64; ++i) {
      const std::string iri = "https://example.org/join/node/" + std::to_string(i);
      const uint64_t id = ParseIriForTest(iri);
      names.emplace(id, iri);
      data.push_back({id, probe_p, probe_o});
      ASSERT_OK(model.WriteTripleToDocDB(id, probe_p, probe_o));
    }
    ASSERT_OK(model.Flush());
    ASSERT_NO_FATALS(InitRouter());

    // Select endpoints from four different tablets using physical reads,
    // rather than assuming that nearby IDs hash to different partitions.
    const auto& tablets = router_.GetTablets(DocKeyEncoder::kSPO);
    ASSERT_EQ(tablets.size(), size_t{4});
    const auto* table = model.GetTable(DocKeyEncoder::kSPO);
    ASSERT_NE(table, nullptr);
    std::vector<uint64_t> nodes;
    std::map<uint64_t, size_t> owners;
    for (size_t index = 0; index < tablets.size(); ++index) {
      client::TableIteratorOptions options;
      options.columns = {"c0", "c1", "c2"};
      options.tablet = tablets[index].tablet_id;
      for (const auto& row : client::TableRange(*table, options)) {
        const uint64_t id = static_cast<uint64_t>(row.column(0).int64_value());
        ASSERT_EQ(owners.count(id), size_t{0});
        owners.emplace(id, index);
        if (nodes.size() == index) {
          nodes.push_back(id);
        }
      }
      ASSERT_EQ(nodes.size(), index + 1);
    }
    const uint64_t a = nodes[0], b = nodes[1], c = nodes[2], d = nodes[3];
    ASSERT_NE(owners.at(a), owners.at(b));
    ASSERT_NE(owners.at(b), owners.at(c));
    ASSERT_NE(owners.at(c), owners.at(d));
    const std::vector<Triple> edges{
        {a, p, b}, {a, p, c}, {d, p, b},
        {b, q, c}, {b, q, d}, {c, q, d},
        {a, q, c}, {a, q, d}, {d, q, c},
        {c, r, a}, {d, r, a}, {d, r, b}};
    for (const auto& triple : edges) {
      data.push_back(triple);
      ASSERT_OK(model.WriteTripleToDocDB(triple[0], triple[1], triple[2]));
    }
    ASSERT_OK(model.Flush());

    const std::string chain = "?a <" + p_iri + "> ?b . ?b <" + q_iri + "> ?c";
    std::string query;
    if (shape == "variables") {
      query = "SELECT ?a ?p ?b ?q ?c WHERE { ?a ?p ?b . ?b ?q ?c }";
    } else if (shape == "chain") {
      query = "SELECT ?a ?b ?c WHERE { " + chain + " }";
    } else if (shape == "three") {
      query = "SELECT ?a ?b ?c ?d WHERE { " + chain + " . ?c <" + r_iri + "> ?d }";
    } else if (shape == "star") {
      query = "SELECT ?a ?b ?c WHERE { ?a <" + p_iri + "> ?b . ?a <" + q_iri + "> ?c }";
    } else if (shape == "self") {
      query = "SELECT ?a ?b ?c WHERE { ?a <" + p_iri + "> ?b . ?a <" + p_iri + "> ?c }";
    } else if (shape == "bag") {
      query = "SELECT ?a ?c WHERE { " + chain + " }";
    } else if (shape == "distinct") {
      query = "SELECT DISTINCT ?c WHERE { " + chain + " }";
    } else if (shape == "empty") {
      query = "SELECT ?a ?c WHERE { ?a <" + p_iri +
          "> ?b . ?b <https://example.org/join/missing> ?c }";
    } else if (shape == "reversed") {
      query = "SELECT ?a ?b ?c WHERE { ?b <" + q_iri + "> ?c . ?a <" + p_iri + "> ?b }";
    } else if (shape == "bound-anchor") {
      query = "SELECT ?p ?b ?c WHERE { <" + names.at(a) + "> ?p ?b . ?b <" + q_iri + "> ?c }";
    } else {
      FAIL() << "Unknown join fixture: " << shape;
    }
    SCOPED_TRACE(query);
    std::unique_ptr<Op> op;
    ASSERT_NO_THROW({
      QueryParser parser(query);
      op = parser.get_query_plan({});
    });
    ASSERT_NE(op, nullptr);
    const auto* select = dynamic_cast<const OpSelect*>(op.get());
    ASSERT_NE(select, nullptr);
    const auto* bgp = dynamic_cast<const OpBasicGraphPattern*>(select->op.get());
    ASSERT_NE(bgp, nullptr);
    ASSERT_GE(bgp->triples.size(), size_t{2});

    // A small independent natural-join oracle preserves duplicate solutions.
    using ReferenceBinding = std::map<VarId, uint64_t>;
    std::vector<ReferenceBinding> reference(1);
    for (const auto& pattern : bgp->triples) {
      const std::array<Id, 3> terms{pattern.subject, pattern.predicate, pattern.object};
      std::vector<ReferenceBinding> next;
      for (const auto& binding : reference) {
        for (const auto& triple : data) {
          auto joined = binding;
          bool matches = true;
          for (size_t column = 0; column < 3; ++column) {
            if (terms[column].is_OID()) {
              matches = matches && terms[column].get_OID().id == triple[column];
            } else {
              auto [it, inserted] = joined.emplace(terms[column].get_var(), triple[column]);
              matches = matches && (inserted || it->second == triple[column]);
            }
          }
          if (matches) {
            next.push_back(std::move(joined));
          }
        }
      }
      reference = std::move(next);
    }
    using ResultRow = std::vector<uint64_t>;
    std::multiset<ResultRow> expected;
    for (const auto& binding : reference) {
      ResultRow row;
      for (auto var : select->vars) {
        row.push_back(binding.at(var));
      }
      expected.insert(std::move(row));
    }
    if (select->distinct) {
      const std::set<ResultRow> unique(expected.begin(), expected.end());
      expected = std::multiset<ResultRow>(unique.begin(), unique.end());
    }
    ASSERT_EQ(expected.empty(), shape == "empty");
    if (shape == "bag") {
      ASSERT_EQ(expected.count(ResultRow{a, d}), size_t{2});
    }
    if (shape == "distinct") {
      ASSERT_EQ(expected.size(), size_t{2});
    }

    DistributedPlan plan;
    DistributedPlanGenerator generator(&router_);
    ASSERT_OK(generator.Generate(*op, &plan));
    ASSERT_EQ(plan.num_fragments(), kNumTablets);
    ScatterExecutor executor;
    std::vector<ScatterExecutor::FragmentResultSet> fragments;
    ASSERT_OK(executor.Execute(plan, &fragments));
    GatherResult result;
    GatherOperator gather;
    ASSERT_OK(gather.Merge(fragments, plan.gather_strategy(), &result));
    std::multiset<ResultRow> actual;
    for (const auto& binding : result.bindings) {
      ResultRow row;
      for (auto var : select->vars) {
        ASSERT_FALSE((*binding)[var].is_null());
        row.push_back((*binding)[var].id);
      }
      actual.insert(std::move(row));
    }
    ASSERT_EQ(actual.size(), expected.size());
    ASSERT_EQ(actual, expected);
  }

  void CheckReadRpcFailure(int fail_on_page) {
    ASSERT_NO_FATALS(CreateRdfTables());
    ASSERT_NO_FATALS(InitModel());
    ASSERT_NO_FATALS(InitRouter());
    auto& model = RdfYBModel::Instance();
    constexpr int kRows = 257;
    // One hash key guarantees three pages in one physical SPO tablet.
    for (int i = 0; i < kRows; ++i) {
      ASSERT_OK(model.WriteTripleToDocDB(1, 10, 100 + i));
    }
    ASSERT_OK(model.Flush());

    std::string owner;
    for (const auto& tablet : router_.GetTablets(0)) {
      ASSERT_OK(model.LoadTablet(0, tablet.tablet_id));
      if (model.GetTabletCache(0, tablet.tablet_id).Size() == kRows) {
        owner = tablet.tablet_id;
      }
    }
    ASSERT_FALSE(owner.empty());
    QueryParser parser("SELECT * WHERE { ?s ?p ?o }");
    auto op = parser.get_query_plan({});
    ASSERT_NE(op, nullptr);
    DistributedPlan plan;
    DistributedPlanGenerator generator(&router_);
    ASSERT_OK(generator.Generate(*op, &plan));

    auto* sync = SyncPoint::GetInstance();
    std::atomic<int> pages{0};
    auto cleanup = ScopeExit([sync] {
      sync->DisableProcessing();
      sync->ClearAllCallBacks();
    });
    for (const std::string mode : {"full", "tablet", "scatter"}) {
      SCOPED_TRACE(mode);
      SCOPED_TRACE(fail_on_page);
      ASSERT_OK(model.LoadFromDocDB(0));
      model.SetLoaded(0);
      ASSERT_EQ(model.GetCache(0).Size(), kRows);
      pages = 0;
      sync->SetCallBack("ReadRpc::NotifyBatcher", [&](void* arg) {
        auto* response = static_cast<tserver::LWReadResponsePB*>(arg);
        for (auto& ql : *response->mutable_ql_batch()) {
          if (ql.has_paging_state() && !ql.paging_state().next_row_key().empty()) {
            if (++pages == fail_on_page) {
              ql.set_status(QLResponsePB::YQL_STATUS_RUNTIME_ERROR);
              ql.dup_error_message("injected MDB page failure");
            }
          }
        }
      });
      sync->EnableProcessing();

      ScatterExecutor executor;
      std::vector<ScatterExecutor::FragmentResultSet> fragments;
      const auto status = mode == "full" ? model.LoadFromDocDB(0)
          : mode == "tablet" ? model.LoadTablet(0, owner)
          : executor.Execute(plan, &fragments);
      sync->DisableProcessing();
      sync->ClearAllCallBacks();
      ASSERT_GE(pages.load(), fail_on_page);
      ASSERT_FALSE(status.ok());
      ASSERT_NE(status.ToString().find("injected MDB page failure"), std::string::npos)
          << status;
      if (mode == "full") {
        ASSERT_EQ(model.GetCache(0).Size(), 0);
        ASSERT_FALSE(model.IsLoaded(0));
        ASSERT_OK(model.LoadFromDocDB(0));
        ASSERT_EQ(model.GetCache(0).Size(), kRows);
      } else {
        ASSERT_EQ(model.GetTabletCache(0, owner).Size(), 0);
        if (mode == "tablet") {
          ASSERT_OK(model.LoadTablet(0, owner));
          ASSERT_EQ(model.GetTabletCache(0, owner).Size(), kRows);
        } else {
          GatherOperator gather;
          GatherResult result;
          ASSERT_EQ(gather.Merge(fragments, plan.gather_strategy(), &result).ToString(),
                    status.ToString());
          ASSERT_TRUE(result.bindings.empty());
          ASSERT_OK(executor.Execute(plan, &fragments));
          ASSERT_OK(gather.Merge(fragments, plan.gather_strategy(), &result));
          ASSERT_EQ(result.total_output_rows, kRows);
        }
      }
    }
  }

  void CheckHashJoinPredicateSubjectOverlap(
      size_t distinct_keys, size_t matching_keys, bool expected_hash);

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

TEST_F(MdbDistributedIntegrationTest, ScatterGatherFullScan) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  ASSERT_TRUE(RdfYBModel::Instance().IsConnected());

  ASSERT_OK(RdfYBModel::Instance().WriteTripleToDocDB(1, 10, 100));
  ASSERT_OK(RdfYBModel::Instance().WriteTripleToDocDB(2, 20, 200));
  ASSERT_OK(RdfYBModel::Instance().WriteTripleToDocDB(3, 30, 300));
  ASSERT_OK(RdfYBModel::Instance().Flush());



  ASSERT_NO_FATALS(InitRouter());
  ASSERT_TRUE(router_.IsInitialized());

  std::unique_ptr<Op> op;
  try {
    QueryParser qp("SELECT * WHERE { ?s ?p ?o }");
    op = qp.get_query_plan({});
  } catch (const QueryParsingException&) {
    FAIL() << "Failed to parse query";
  }
  ASSERT_NE(op, nullptr);

  DistributedPlan plan;
  DistributedPlanGenerator generator(&router_);
  ASSERT_OK(generator.Generate(*op, &plan));

  ASSERT_GT(plan.num_fragments(), 0);
  ASSERT_TRUE(plan.is_distributed());
  ASSERT_EQ(plan.gather_strategy(), GatherStrategy::UNION_ALL);

  ScatterExecutor executor;
  std::vector<ScatterExecutor::FragmentResultSet> fragment_results;
  ASSERT_OK(executor.Execute(plan, &fragment_results));
  ASSERT_EQ(static_cast<int>(fragment_results.size()), kNumTablets);

  size_t total_fragment_rows = 0;
  for (const auto& fr : fragment_results) {
    total_fragment_rows += fr.bindings.size();
  }

  ASSERT_EQ(total_fragment_rows, 3);

  GatherOperator gather;
  GatherResult gather_result;
  ASSERT_OK(gather.Merge(fragment_results, plan.gather_strategy(), &gather_result));

  ASSERT_EQ(gather_result.total_input_rows, 3);
  ASSERT_EQ(gather_result.total_output_rows, 3);

  auto single = ExecuteSingleNode("SELECT * WHERE { ?s ?p ?o }");
  ASSERT_EQ(single.row_count, 3);
}

TEST_F(MdbDistributedIntegrationTest, ScatterGatherWithDataLoaded) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  ASSERT_TRUE(RdfYBModel::Instance().IsConnected());

  for (int i = 0; i < 10; i++) {
    ASSERT_OK(RdfYBModel::Instance().WriteTripleToDocDB(
        static_cast<uint64_t>(i + 1),
        static_cast<uint64_t>(100 + i),
        static_cast<uint64_t>(200 + i)));
  }

  ASSERT_OK(RdfYBModel::Instance().Flush());
  ASSERT_OK(RdfYBModel::Instance().LoadAllFromDocDB());

  ASSERT_NO_FATALS(InitRouter());
  ASSERT_TRUE(router_.IsInitialized());

  std::unique_ptr<Op> op;
  try {
    QueryParser qp("SELECT DISTINCT * WHERE { ?s ?p ?o }");
    op = qp.get_query_plan({});
  } catch (const QueryParsingException&) {
    FAIL() << "Failed to parse query";
  }
  ASSERT_NE(op, nullptr);

  DistributedPlan plan;
  DistributedPlanGenerator generator(&router_);
  ASSERT_OK(generator.Generate(*op, &plan));

  ASSERT_GT(plan.num_fragments(), 0);
  ASSERT_TRUE(plan.is_distributed());
  ASSERT_EQ(plan.gather_strategy(), GatherStrategy::UNION_DISTINCT);

  ScatterExecutor executor;
  std::vector<ScatterExecutor::FragmentResultSet> fragment_results;
  ASSERT_OK(executor.Execute(plan, &fragment_results));
  ASSERT_EQ(static_cast<int>(fragment_results.size()), kNumTablets);

  // Verify the union of tablet fragments, not a full-table copy per fragment.
  const VarId s_var = get_query_ctx().get_or_create_var("s");
  const VarId p_var = get_query_ctx().get_or_create_var("p");
  const VarId o_var = get_query_ctx().get_or_create_var("o");
  using Triple = std::tuple<uint64_t, uint64_t, uint64_t>;
  std::set<Triple> expected;
  for (int i = 0; i < 10; ++i) {
    expected.emplace(i + 1, 100 + i, 200 + i);
  }
  std::set<Triple> actual;
  size_t total_fragment_rows = 0;
  for (const auto& fr : fragment_results) {
    ASSERT_OK(fr.status);
    total_fragment_rows += fr.bindings.size();
    for (const auto& row : fr.bindings) {
      ASSERT_FALSE((*row)[s_var].is_null());
      ASSERT_FALSE((*row)[p_var].is_null());
      ASSERT_FALSE((*row)[o_var].is_null());
      actual.emplace((*row)[s_var].id, (*row)[p_var].id, (*row)[o_var].id);
    }
  }
  ASSERT_EQ(total_fragment_rows, size_t{10});
  ASSERT_EQ(actual, expected);

  GatherOperator gather;
  GatherResult gather_result;
  ASSERT_OK(gather.Merge(fragment_results, plan.gather_strategy(), &gather_result));

  ASSERT_EQ(gather_result.total_input_rows, 10);
  ASSERT_EQ(gather_result.total_output_rows, 10);

  auto single = ExecuteSingleNode("SELECT * WHERE { ?s ?p ?o }");
  ASSERT_EQ(single.row_count, 10);
}

TEST_F(MdbDistributedIntegrationTest, DistinctGatherStrategy) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  ASSERT_TRUE(RdfYBModel::Instance().IsConnected());

  ASSERT_OK(RdfYBModel::Instance().WriteTripleToDocDB(1, 10, 100));
  ASSERT_OK(RdfYBModel::Instance().WriteTripleToDocDB(1, 10, 100));

  ASSERT_OK(RdfYBModel::Instance().Flush());
  ASSERT_OK(RdfYBModel::Instance().LoadAllFromDocDB());

  ASSERT_NO_FATALS(InitRouter());
  ASSERT_TRUE(router_.IsInitialized());

  std::unique_ptr<Op> op;
  try {
    QueryParser qp("SELECT DISTINCT ?s WHERE { ?s ?p ?o }");
    op = qp.get_query_plan({});
  } catch (const QueryParsingException&) {
    FAIL() << "Failed to parse query";
  }
  ASSERT_NE(op, nullptr);

  DistributedPlan plan;
  DistributedPlanGenerator generator(&router_);
  ASSERT_OK(generator.Generate(*op, &plan));

  ASSERT_EQ(plan.gather_strategy(), GatherStrategy::UNION_DISTINCT);

  ScatterExecutor executor;
  std::vector<ScatterExecutor::FragmentResultSet> fragment_results;
  ASSERT_OK(executor.Execute(plan, &fragment_results));
  ASSERT_EQ(static_cast<int>(fragment_results.size()), kNumTablets);

  // Repeated insertion of the same RDF triple leaves one stored triple.
  // Only its owning tablet should return a binding; other tablets may be empty.
  const VarId s_var = get_query_ctx().get_or_create_var("s");
  size_t total_fragment_rows = 0;
  for (const auto& fr : fragment_results) {
    ASSERT_OK(fr.status);
    total_fragment_rows += fr.bindings.size();
    for (const auto& row : fr.bindings) {
      ASSERT_FALSE((*row)[s_var].is_null());
      ASSERT_EQ((*row)[s_var].id, uint64_t{1});
    }
  }
  ASSERT_EQ(total_fragment_rows, size_t{1});

  GatherOperator gather;
  GatherResult gather_result;
  ASSERT_OK(gather.Merge(fragment_results, plan.gather_strategy(), &gather_result));

  ASSERT_EQ(gather_result.total_input_rows, 1);
  ASSERT_EQ(gather_result.total_output_rows, 1);
}

TEST_F(MdbDistributedIntegrationTest, TabletScanPaginationAndContents) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  ASSERT_TRUE(RdfYBModel::Instance().IsConnected());

  // Each subject hashes to one SPO tablet. Its 140 objects exceed the
  // TableIterator page size of 128 and also test distinct c2 key values.
  constexpr uint64_t kSubjects = 8;
  constexpr uint64_t kObjectsPerSubject = 140;
  using Triple = std::tuple<uint64_t, uint64_t, uint64_t>;
  std::set<Triple> expected;
  for (uint64_t s = 1; s <= kSubjects; ++s) {
    for (uint64_t i = 0; i < kObjectsPerSubject; ++i) {
      const uint64_t o = 1000 + i;
      ASSERT_OK(RdfYBModel::Instance().WriteTripleToDocDB(s, 10, o));
      expected.emplace(s, 10, o);
    }
  }
  ASSERT_OK(RdfYBModel::Instance().Flush());
  ASSERT_NO_FATALS(InitRouter());

  const auto* table = RdfYBModel::Instance().GetTable(0);
  ASSERT_NE(table, nullptr);
  const auto& tablets = router_.GetTablets(0);
  ASSERT_EQ(tablets.size(), static_cast<size_t>(kNumTablets));
  std::set<Triple> actual;
  size_t total_rows = 0;
  size_t max_tablet_rows = 0;
  for (const auto& tablet : tablets) {
    SCOPED_TRACE(tablet.tablet_id);
    client::TableIteratorOptions options;
    options.columns = {"c0", "c1", "c2"};
    options.tablet = tablet.tablet_id;
    size_t tablet_rows = 0;
    for (const auto& row : client::TableRange(*table, options)) {
      const Triple triple{
          static_cast<uint64_t>(row.column(0).int64_value()),
          static_cast<uint64_t>(row.column(1).int64_value()),
          static_cast<uint64_t>(row.column(2).int64_value())};
      ASSERT_EQ(expected.count(triple), size_t{1});
      ASSERT_TRUE(actual.insert(triple).second)
          << "A row was returned more than once across tablet scans";
      ++tablet_rows;
      ++total_rows;
    }
    if (tablet_rows > max_tablet_rows) {
      max_tablet_rows = tablet_rows;
    }
  }
  ASSERT_GT(max_tablet_rows, size_t{128});
  ASSERT_EQ(total_rows, expected.size());
  ASSERT_EQ(actual, expected);
}

TEST_F(MdbDistributedIntegrationTest, FixedSubjectRouting) {
  ASSERT_NO_FATALS(CheckFixedPattern(1, DocKeyEncoder::kSPO));
}

TEST_F(MdbDistributedIntegrationTest, FixedPredicateRouting) {
  ASSERT_NO_FATALS(CheckFixedPattern(2, DocKeyEncoder::kPOS));
}

TEST_F(MdbDistributedIntegrationTest, FixedObjectRouting) {
  ASSERT_NO_FATALS(CheckFixedPattern(4, DocKeyEncoder::kOSP));
}

TEST_F(MdbDistributedIntegrationTest, FixedSubjectPredicateRouting) {
  ASSERT_NO_FATALS(CheckFixedPattern(3, DocKeyEncoder::kSPO));
}

TEST_F(MdbDistributedIntegrationTest, FixedSubjectObjectRouting) {
  ASSERT_NO_FATALS(CheckFixedPattern(5, DocKeyEncoder::kOSP));
}

TEST_F(MdbDistributedIntegrationTest, FixedPredicateObjectRouting) {
  ASSERT_NO_FATALS(CheckFixedPattern(6, DocKeyEncoder::kPOS));
}

TEST_F(MdbDistributedIntegrationTest, FullyBoundTripleRouting) {
  ASSERT_NO_FATALS(CheckFixedPattern(7, DocKeyEncoder::kSPO));
}

TEST_F(MdbDistributedIntegrationTest, FixedPatternNoMatch) {
  ASSERT_NO_FATALS(CheckFixedPattern(7, DocKeyEncoder::kSPO, true));
}

TEST_F(MdbDistributedIntegrationTest, CrossTabletVariableChain) {
  ASSERT_NO_FATALS(CheckCrossTabletJoin("variables"));
}

TEST_F(MdbDistributedIntegrationTest, CrossTabletPredicateChain) {
  ASSERT_NO_FATALS(CheckCrossTabletJoin("chain"));
}

TEST_F(MdbDistributedIntegrationTest, CrossTabletThreePatternChain) {
  ASSERT_NO_FATALS(CheckCrossTabletJoin("three"));
}

TEST_F(MdbDistributedIntegrationTest, CrossTabletStarJoin) {
  ASSERT_NO_FATALS(CheckCrossTabletJoin("star"));
}

TEST_F(MdbDistributedIntegrationTest, CrossTabletSelfJoin) {
  ASSERT_NO_FATALS(CheckCrossTabletJoin("self"));
}

TEST_F(MdbDistributedIntegrationTest, CrossTabletJoinBagSemantics) {
  ASSERT_NO_FATALS(CheckCrossTabletJoin("bag"));
}

TEST_F(MdbDistributedIntegrationTest, CrossTabletJoinDistinctProjection) {
  ASSERT_NO_FATALS(CheckCrossTabletJoin("distinct"));
}

TEST_F(MdbDistributedIntegrationTest, CrossTabletJoinNoMatch) {
  ASSERT_NO_FATALS(CheckCrossTabletJoin("empty"));
}

TEST_F(MdbDistributedIntegrationTest, CrossTabletReversedPatternOrder) {
  ASSERT_NO_FATALS(CheckCrossTabletJoin("reversed"));
}

TEST_F(MdbDistributedIntegrationTest, CrossTabletBoundAnchorAfterReorder) {
  ASSERT_NO_FATALS(CheckCrossTabletJoin("bound-anchor"));
}

TEST_F(MdbDistributedIntegrationTest, ScatterPropagatesInitialReadFailures) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitRouter());

  auto& model = RdfYBModel::Instance();
  for (const std::string mode : {"tablet", "partition", "full"}) {
    SCOPED_TRACE(mode);
    ASSERT_NO_FATALS(InitModel());
    QueryParser parser(mode == "full"
        ? "SELECT * WHERE { ?s ?p ?o }"
        : "SELECT ?p ?o WHERE { <https://example.org/subject> ?p ?o }");
    auto op = parser.get_query_plan({});
    ASSERT_NE(op, nullptr);

    DistributedPlan plan;
    DistributedPlanGenerator generator(&router_);
    ASSERT_OK(generator.Generate(*op, &plan));
    ASSERT_GT(plan.num_fragments(), 0);
    if (mode != "tablet") {
      // Exercise the fixed-partition and full-cache construction paths too.
      for (auto& fragment : plan.mutable_fragments()) {
        fragment.tablet_id.clear();
      }
    }

    // Invalidate the model after planning, without changing the real plan.
    model.ClearAll();
    ScatterExecutor executor;
    std::vector<ScatterExecutor::FragmentResultSet> fragments;
    const auto status = executor.Execute(plan, &fragments);
    ASSERT_TRUE(status.IsIllegalState()) << status;
    ASSERT_NE(status.ToString().find("not connected to DocDB"), std::string::npos);
    ASSERT_EQ(fragments.size(), static_cast<size_t>(plan.num_fragments()));
    for (const auto& fragment : fragments) {
      ASSERT_EQ(fragment.status.ToString(), status.ToString());
      ASSERT_TRUE(fragment.bindings.empty());
    }
    for (int perm = 0; perm < 6; ++perm) {
      ASSERT_FALSE(model.IsLoaded(perm));
    }

    GatherOperator gather;
    GatherResult gathered;
    ASSERT_EQ(gather.Merge(fragments, plan.gather_strategy(), &gathered).ToString(),
              status.ToString());
    ASSERT_TRUE(gathered.bindings.empty());

    // A connected empty table must still succeed with exactly the same plan.
    ASSERT_NO_FATALS(InitModel());
    ASSERT_OK(executor.Execute(plan, &fragments));
    ASSERT_OK(gather.Merge(fragments, plan.gather_strategy(), &gathered));
    ASSERT_EQ(gathered.total_output_rows, 0);
    model.ClearAll();
  }
}

TEST_F(MdbDistributedIntegrationTest, YB_DEBUG_ONLY_TEST(InitialReadRpcFailureReturnsStatus)) {
  ASSERT_NO_FATALS(CheckReadRpcFailure(1));
}

TEST_F(
    MdbDistributedIntegrationTest, YB_DEBUG_ONLY_TEST(PagedReadFailureClearsCacheAndPropagates)) {
  ASSERT_NO_FATALS(CheckReadRpcFailure(2));
}

TEST_F(MdbDistributedIntegrationTest, YB_DEBUG_ONLY_TEST(PartitionReadRpcFailureReturnsStatus)) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  ASSERT_NO_FATALS(InitRouter());
  auto& model = RdfYBModel::Instance();
  const std::string present_iri = "https://example.org/partition-present";
  const std::string missing_iri = "https://example.org/partition-missing";
  const auto present = ParseIriForTest(present_iri);
  constexpr int kRows = 3;
  for (int i = 0; i < kRows; ++i) {
    ASSERT_OK(model.WriteTripleToDocDB(present, 10, 100 + i));
  }
  ASSERT_OK(model.Flush());

  auto* sync = SyncPoint::GetInstance();
  auto cleanup = ScopeExit([sync] {
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  });
  for (const std::string mode : {"direct", "scatter"}) {
    SCOPED_TRACE(mode);
    for (const auto& iri : {present_iri, missing_iri}) {
      SCOPED_TRACE(iri);
      const auto key = ParseIriForTest(iri);
      auto& cache = model.GetPartitionCache(0, key);
      cache.Clear();
      const int expected_rows = iri == present_iri ? kRows : 0;
      QueryParser parser("SELECT ?p ?o WHERE { <" + iri + "> ?p ?o }");
      auto op = parser.get_query_plan({});
      ASSERT_NE(op, nullptr);
      DistributedPlan plan;
      DistributedPlanGenerator generator(&router_);
      ASSERT_OK(generator.Generate(*op, &plan));
      ASSERT_EQ(plan.num_fragments(), 1);
      // Select the fixed-partition fallback instead of the physical-tablet path.
      plan.mutable_fragments().front().tablet_id.clear();

      std::atomic<int> injected{0};
      sync->SetCallBack("ReadRpc::NotifyBatcher", [&](void* arg) {
        auto* response = static_cast<tserver::LWReadResponsePB*>(arg);
        for (auto& ql : *response->mutable_ql_batch()) {
          ql.set_status(QLResponsePB::YQL_STATUS_RUNTIME_ERROR);
          ql.dup_error_message("injected MDB partition failure");
          ++injected;
        }
      });
      sync->EnableProcessing();
      ScatterExecutor executor;
      std::vector<ScatterExecutor::FragmentResultSet> fragments;
      const auto status = mode == "direct" ? model.LoadPartition(0, key)
                                           : executor.Execute(plan, &fragments);
      sync->DisableProcessing();
      sync->ClearAllCallBacks();
      ASSERT_GT(injected.load(), 0);
      ASSERT_TRUE(status.IsRuntimeError()) << status;
      ASSERT_NE(status.ToString().find("injected MDB partition failure"), std::string::npos);
      ASSERT_EQ(cache.Size(), 0);
      ASSERT_FALSE(model.IsLoaded(0));
      GatherOperator gather;
      GatherResult result;
      if (mode == "scatter") {
        ASSERT_EQ(fragments.size(), 1);
        ASSERT_EQ(fragments.front().status.ToString(), status.ToString());
        ASSERT_TRUE(fragments.front().bindings.empty());
        ASSERT_EQ(gather.Merge(fragments, plan.gather_strategy(), &result).ToString(),
                  status.ToString());
        ASSERT_TRUE(result.bindings.empty());
      }

      if (mode == "direct") {
        ASSERT_OK(model.LoadPartition(0, key));
        ASSERT_EQ(cache.Size(), expected_rows);
      } else {
        ASSERT_OK(executor.Execute(plan, &fragments));
        ASSERT_OK(gather.Merge(fragments, plan.gather_strategy(), &result));
        ASSERT_EQ(result.total_output_rows, expected_rows);
      }
    }
  }
}

TEST_F(MdbDistributedIntegrationTest, StatisticsCountPredicateObjectCombinations) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  auto& model = RdfYBModel::Instance();
  const auto oid = [](uint64_t value) { return Id(::ObjectId(value)); };
  TriplePlan frequent(Id(VarId(0)), oid(10), oid(100));
  TriplePlan absent_pair(Id(VarId(0)), oid(20), oid(101));
  ASSERT_OK(model.RefreshStatistics());
  ASSERT_TRUE(model.GetStatistics()->predicate_object_counts.empty());
  ASSERT_TRUE(model.GetStatistics()->predicate_subject_counts.empty());
  ASSERT_DOUBLE_EQ(frequent.estimate_output_size(), 0);
  for (const auto& triple : std::vector<std::array<uint64_t, 3>>{
           {1, 10, 100}, {1, 10, 100}, {2, 10, 100}, {3, 10, 101},
           {1, 20, 100}, {4, 20, 102}, {7, 7, 7}}) {
    ASSERT_OK(model.WriteTripleToDocDB(triple[0], triple[1], triple[2]));
  }
  ASSERT_OK(model.Flush());
  ASSERT_OK(model.RefreshStatistics());
  const auto* stats = model.GetStatistics();
  ASSERT_EQ(stats->rows, 6);
  ASSERT_EQ(stats->predicate_object_counts.size(), 3);
  ASSERT_EQ(stats->predicate_object_counts.at(10).at(100), 2);
  ASSERT_EQ(stats->predicate_object_counts.at(10).size(), 2);
  ASSERT_EQ(stats->predicate_object_counts.at(20).size(), 2);
  ASSERT_EQ(stats->predicate_subject_counts.size(), 3);
  ASSERT_EQ(stats->predicate_subject_counts.at(10).size(), 3);
  ASSERT_EQ(stats->predicate_subject_counts.at(10).at(1), 1);
  ASSERT_EQ(stats->predicate_subject_counts.at(20).at(1), 1);
  ASSERT_EQ(stats->predicate_subject_counts.at(7).at(7), 1);
  ASSERT_DOUBLE_EQ(frequent.estimate_output_size(), 2);
  ASSERT_DOUBLE_EQ(absent_pair.estimate_output_size(), 0);
  TriplePlan absent_predicate(Id(VarId(0)), oid(999), oid(100));
  TriplePlan absent_object(Id(VarId(0)), oid(10), oid(999));
  TriplePlan equal_constants(Id(VarId(0)), oid(7), oid(7));
  TriplePlan missing_subject(oid(999), oid(10), oid(100));
  TriplePlan missing_triple(oid(1), oid(20), oid(101));
  ASSERT_DOUBLE_EQ(absent_predicate.estimate_output_size(), 0);
  ASSERT_DOUBLE_EQ(absent_object.estimate_output_size(), 0);
  ASSERT_DOUBLE_EQ(equal_constants.estimate_output_size(), 1);
  ASSERT_DOUBLE_EQ(missing_subject.estimate_output_size(), 0);
  ASSERT_DOUBLE_EQ(missing_triple.estimate_output_size(), 0);
  auto bound_subject = frequent.clone();
  bound_subject->set_input_vars({VarId(0)});
  ASSERT_GT(bound_subject->estimate_output_size(), 0);
  ASSERT_LE(bound_subject->estimate_output_size(), 1);
  ASSERT_DOUBLE_EQ(frequent.estimate_output_size(), 2);
  const auto version = model.StatisticsVersion();
  ASSERT_OK(model.WriteTripleToDocDB(5, 10, 100));
  ASSERT_EQ(model.GetStatistics(), nullptr);
  ASSERT_GT(model.StatisticsVersion(), version);
  ASSERT_OK(model.Flush());
  ASSERT_OK(model.RefreshStatistics());
  ASSERT_DOUBLE_EQ(frequent.estimate_output_size(), 3);
  ASSERT_EQ(model.GetStatistics()->predicate_subject_counts.at(10).at(5), 1);
}

TEST_F(MdbDistributedIntegrationTest, StatisticsCountPredicateSubjectCombinations) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  auto& model = RdfYBModel::Instance();
  const auto oid = [](uint64_t value) { return Id(::ObjectId(value)); };
  TriplePlan pair(oid(1), oid(10), Id(VarId(2)));
  ASSERT_OK(model.RefreshStatistics());
  ASSERT_DOUBLE_EQ(pair.estimate_output_size(), 0);
  for (const auto& row : std::vector<std::array<uint64_t, 3>>{
           {1, 10, 100}, {1, 10, 101}, {1, 10, 101}, {2, 10, 100},
           {1, 20, 200}, {3, 20, 201}, {7, 7, 7}}) {
    ASSERT_OK(model.WriteTripleToDocDB(row[0], row[1], row[2]));
  }
  ASSERT_OK(model.Flush());
  ASSERT_OK(model.RefreshStatistics());
  ASSERT_EQ(model.GetStatistics()->rows, 6);
  ASSERT_DOUBLE_EQ(pair.estimate_output_size(), 2);
  TriplePlan absent_pair(oid(3), oid(10), Id(VarId(2)));
  TriplePlan absent_subject(oid(999), oid(10), Id(VarId(2)));
  TriplePlan absent_predicate(oid(1), oid(999), Id(VarId(2)));
  TriplePlan equal_constants(oid(7), oid(7), Id(VarId(2)));
  ASSERT_DOUBLE_EQ(absent_pair.estimate_output_size(), 0);
  ASSERT_DOUBLE_EQ(absent_subject.estimate_output_size(), 0);
  ASSERT_DOUBLE_EQ(absent_predicate.estimate_output_size(), 0);
  ASSERT_DOUBLE_EQ(equal_constants.estimate_output_size(), 1);
  auto clone = pair.clone();
  ASSERT_DOUBLE_EQ(clone->estimate_output_size(), 2);
  auto bound = pair.clone();
  bound->set_input_vars({VarId(2)});
  ASSERT_GT(bound->estimate_output_size(), 0);
  ASSERT_LE(bound->estimate_output_size(), 2);
  ASSERT_DOUBLE_EQ(pair.estimate_output_size(), 2);
  ASSERT_EQ(g_pack_to_gmap.count(800001), 0);
  ASSERT_EQ(g_pack_to_gmap.count(800010), 0);
  g_pack_to_gmap[800001] = 1;
  g_pack_to_gmap[800010] = 10;
  auto restore_mapping = ScopeExit([] {
    g_pack_to_gmap.erase(800001);
    g_pack_to_gmap.erase(800010);
  });
  TriplePlan mapped(oid(800001), oid(800010), Id(VarId(2)));
  ASSERT_DOUBLE_EQ(mapped.estimate_output_size(), 2);
  const auto version = model.StatisticsVersion();
  ASSERT_OK(model.WriteTripleToDocDB(1, 10, 102));
  ASSERT_EQ(model.GetStatistics(), nullptr);
  ASSERT_GT(model.StatisticsVersion(), version);
  ASSERT_OK(model.Flush());
  ASSERT_OK(model.RefreshStatistics());
  ASSERT_DOUBLE_EQ(pair.estimate_output_size(), 3);
  ASSERT_DOUBLE_EQ(clone->estimate_output_size(), 3);
  ASSERT_DOUBLE_EQ(mapped.estimate_output_size(), 3);
}

TEST_F(
    MdbDistributedIntegrationTest, YB_DEBUG_ONLY_TEST(StatisticsPredicateSubjectChangesJoinOrder)) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  auto& model = RdfYBModel::Instance();
  for (uint64_t i = 0; i < 96; ++i) {
    ASSERT_OK(model.WriteTripleToDocDB(1, 10, 100 + i));
    ASSERT_OK(model.WriteTripleToDocDB(2, i == 0 ? 20 : 30, i == 0 ? 100 : 1000 + i));
  }
  for (uint64_t i = 0; i < 1024; ++i) {
    ASSERT_OK(model.WriteTripleToDocDB(10000 + i, 20, 20000 + i));
  }
  ASSERT_OK(model.Flush());
  ASSERT_OK(model.RefreshStatistics());
  ASSERT_EQ(model.GetStatistics()->rows, 1216);
  get_query_ctx() = QueryContext{};
  const VarId variable = get_query_ctx().get_or_create_var("shared");
  std::vector<std::unique_ptr<Plan>> base;
  base.push_back(std::make_unique<TriplePlan>(
      Id(::ObjectId(uint64_t{1})), Id(::ObjectId(uint64_t{10})), Id(variable)));
  base.push_back(std::make_unique<TriplePlan>(
      Id(::ObjectId(uint64_t{2})), Id(::ObjectId(uint64_t{20})), Id(variable)));
  auto* sync = SyncPoint::GetInstance();
  auto cleanup = ScopeExit([sync] {
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  });
  std::atomic<int> requests{0};
  sync->SetCallBack("ReadRpc::CallRemoteMethod", [&](void* arg) {
    const auto* request = static_cast<tserver::LWReadRequestPB*>(arg);
    requests += static_cast<int>(request->ql_batch().size());
  });
  for (bool reversed : {false, true}) {
    if (reversed) std::swap(base[0], base[1]);
    for (bool compare_first : {false, true}) {
      for (bool optimized : {false, true}) {
        SCOPED_TRACE(Format("reversed=$0 lookahead=$1 optimized=$2",
                            reversed, compare_first, optimized));
        model.ClearQueryCaches();
        requests = 0;
        sync->EnableProcessing();
        auto plan = optimized ? GreedyOptimizer::get_plan(base, compare_first)
            : std::make_unique<IndexNestedLoopPlan>(
                  base[reversed ? 1 : 0]->clone(), base[reversed ? 0 : 1]->clone());
        sync->DisableProcessing();
        ASSERT_EQ(requests.load(), 0);
        auto iter = plan->clone()->get_binding_iter();
        auto* indexed = dynamic_cast<IndexNestedLoopJoin*>(iter.get());
        ASSERT_NE(indexed, nullptr);
        Binding binding(1);
        iter->begin(binding);
        std::vector<uint64_t> actual;
        while (iter->next()) actual.push_back(binding[variable].id);
        ASSERT_OK(iter->status());
        ASSERT_EQ(actual, (std::vector<uint64_t>{100}));
        ASSERT_EQ(indexed->lhs->results, optimized ? 1 : 96);
        ASSERT_EQ(indexed->original_rhs->stat_reset, optimized ? 0 : 95);
        LOG(INFO) << "MDB_SP_ORDER reversed=" << reversed << " lookahead=" << compare_first
                  << " optimized=" << optimized << " lhs_rows=" << indexed->lhs->results
                  << " rhs_resets=" << indexed->original_rhs->stat_reset;
      }
    }
  }
}


TEST_F(MdbDistributedIntegrationTest, YB_DEBUG_ONLY_TEST(CatalogCountsTrackStatisticsLifecycle)) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  auto& model = RdfYBModel::Instance();
  const RdfCatalog catalog;
  const auto counts = [&catalog] {
    return std::array<std::optional<uint64_t>, 11>{
        catalog.get_triples_count(), catalog.get_predicate_count(2),
        catalog.get_subject_count(1), catalog.get_object_count(1),
        catalog.get_equal_sp_count(), catalog.get_equal_so_count(),
        catalog.get_equal_po_count(), catalog.get_equal_spo_count(),
        catalog.get_distinct_subjects(), catalog.get_distinct_predicates(),
        catalog.get_distinct_objects()};
  };
  auto* sync = SyncPoint::GetInstance();
  auto cleanup = ScopeExit([sync] {
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  });
  std::atomic<int> requests{0};
  sync->SetCallBack("ReadRpc::CallRemoteMethod", [&](void* arg) {
    const auto* request = static_cast<tserver::LWReadRequestPB*>(arg);
    requests += static_cast<int>(request->ql_batch().size());
  });
  const auto check_unknown = [&] {
    requests = 0;
    sync->EnableProcessing();
    const auto observed = counts();
    sync->DisableProcessing();
    ASSERT_EQ(requests.load(), 0);
    for (const auto& value : observed) ASSERT_FALSE(value.has_value());
  };
  const auto check_known = [&](const std::array<uint64_t, 11>& expected) {
    requests = 0;
    sync->EnableProcessing();
    const auto observed = counts();
    ASSERT_EQ(catalog.get_subject_count(999), std::optional<uint64_t>(0));
    ASSERT_EQ(catalog.get_predicate_count(999), std::optional<uint64_t>(0));
    ASSERT_EQ(catalog.get_object_count(999), std::optional<uint64_t>(0));
    sync->DisableProcessing();
    ASSERT_EQ(requests.load(), 0);
    for (size_t i = 0; i < observed.size(); ++i) {
      ASSERT_TRUE(observed[i].has_value()) << i;
      ASSERT_EQ(*observed[i], expected[i]) << i;
    }
  };
  ASSERT_NO_FATALS(check_unknown());
  ASSERT_OK(model.RefreshStatistics());
  ASSERT_NO_FATALS(check_known({0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}));
  const auto var = [](int value) { return Id(VarId(value)); };
  TriplePlan all(var(0), var(1), var(2));
  ASSERT_DOUBLE_EQ(all.estimate_output_size(), 0);
  for (const auto& row : std::vector<std::array<uint64_t, 3>>{
           {1, 1, 1}, {1, 1, 1}, {1, 2, 1}, {2, 2, 3},
           {4, 5, 5}, {4, 2, 6}, {7, 8, 9}}) {
    ASSERT_OK(model.WriteTripleToDocDB(row[0], row[1], row[2]));
  }
  ASSERT_NO_FATALS(check_unknown());
  ASSERT_GT(all.estimate_output_size(), 0);
  ASSERT_OK(model.Flush());
  ASSERT_OK(model.RefreshStatistics());
  ASSERT_NO_FATALS(check_known({6, 3, 2, 2, 2, 2, 2, 1, 4, 4, 5}));
  ASSERT_DOUBLE_EQ(all.estimate_output_size(), 6);
  model.ClearQueryCaches();
  ASSERT_NO_FATALS(check_known({6, 3, 2, 2, 2, 2, 2, 1, 4, 4, 5}));
  ASSERT_OK(model.WriteTripleToDocDB(10, 10, 10));
  ASSERT_NO_FATALS(check_unknown());
  ASSERT_OK(model.Flush());
  ASSERT_OK(model.RefreshStatistics());
  ASSERT_NO_FATALS(check_known({7, 3, 2, 2, 3, 3, 3, 2, 5, 5, 6}));
  ASSERT_DOUBLE_EQ(all.estimate_output_size(), 7);
  model.ClearAll();
  ASSERT_NO_FATALS(check_unknown());
  ASSERT_GT(all.estimate_output_size(), 0);
}

TEST_F(MdbDistributedIntegrationTest, StatisticsReflectStoredRowsAndInvalidate) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  auto& model = RdfYBModel::Instance();
  ASSERT_EQ(model.GetStatistics(), nullptr);
  ASSERT_OK(model.RefreshStatistics());
  ASSERT_EQ(model.GetStatistics()->rows, 0);
  TriplePlan by_subject(Id(::ObjectId(uint64_t{1})), Id(VarId(1)), Id(VarId(2)));
  TriplePlan by_predicate(Id(VarId(0)), Id(::ObjectId(uint64_t{10})), Id(VarId(2)));
  ASSERT_DOUBLE_EQ(by_subject.estimate_output_size(), 0);
  for (const auto& triple : std::vector<std::array<uint64_t, 3>>{
           {1, 10, 100}, {1, 10, 100}, {1, 10, 101}, {2, 20, 100}, {1, 1, 1}}) {
    ASSERT_OK(model.WriteTripleToDocDB(triple[0], triple[1], triple[2]));
  }
  ASSERT_EQ(model.GetStatistics(), nullptr);
  ASSERT_OK(model.Flush());
  ASSERT_OK(model.RefreshStatistics());
  ASSERT_EQ(model.GetStatistics()->rows, 4);
  ASSERT_DOUBLE_EQ(by_subject.estimate_output_size(), 3);
  ASSERT_DOUBLE_EQ(by_predicate.estimate_output_size(), 2);
  TriplePlan by_object(Id(VarId(0)), Id(VarId(1)), Id(::ObjectId(uint64_t{100})));
  ASSERT_DOUBLE_EQ(by_object.estimate_output_size(), 2);
  TriplePlan missing(Id(::ObjectId(uint64_t{999})), Id(VarId(1)), Id(VarId(2)));
  ASSERT_DOUBLE_EQ(missing.estimate_output_size(), 0);
  TriplePlan equal(Id(VarId(0)), Id(VarId(0)), Id(VarId(0)));
  ASSERT_DOUBLE_EQ(equal.estimate_output_size(), 1);
  TriplePlan pair(Id(::ObjectId(uint64_t{1})), Id(::ObjectId(uint64_t{10})), Id(VarId(2)));
  ASSERT_DOUBLE_EQ(pair.estimate_output_size(), 2);
  const auto version = model.StatisticsVersion();
  ASSERT_OK(model.WriteTripleToDocDB(3, 10, 103));
  ASSERT_GT(model.StatisticsVersion(), version);
  ASSERT_EQ(model.GetStatistics(), nullptr);
  ASSERT_OK(model.Flush());
  ASSERT_OK(model.RefreshStatistics());
  ASSERT_DOUBLE_EQ(by_predicate.estimate_output_size(), 3);
  model.ClearAll();
  ASSERT_NO_FATALS(InitModel());
  ASSERT_EQ(model.GetStatistics(), nullptr);
  ASSERT_OK(model.RefreshStatistics());
  ASSERT_EQ(model.GetStatistics()->rows, 5);
  ASSERT_DOUBLE_EQ(by_predicate.estimate_output_size(), 3);
  ASSERT_OK(model.BeginTransaction());
  ASSERT_TRUE(model.RefreshStatistics().IsIllegalState());
  ASSERT_EQ(model.GetStatistics(), nullptr);
  ASSERT_OK(model.AbortTransaction());
  ASSERT_OK(model.RefreshStatistics());
  ASSERT_EQ(model.GetStatistics()->rows, 5);
}

TEST_F(MdbDistributedIntegrationTest, YB_DEBUG_ONLY_TEST(StatisticsSurviveQueryCacheReset)) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  auto& model = RdfYBModel::Instance();
  ASSERT_OK(model.WriteTripleToDocDB(1, 10, 100));
  ASSERT_OK(model.WriteTripleToDocDB(2, 10, 200));
  ASSERT_OK(model.Flush());
  ASSERT_OK(model.RefreshStatistics());
  const auto version = model.StatisticsVersion();
  ASSERT_NO_FATALS(InitRouter());
  QueryParser parser("SELECT ?s ?p ?o WHERE { ?s ?p ?o }");
  auto op = parser.get_query_plan({});
  DistributedPlan distributed;
  DistributedPlanGenerator generator(&router_);
  ASSERT_OK(generator.Generate(*op, &distributed));
  auto* sync = SyncPoint::GetInstance();
  std::atomic<int> reads{0};
  auto cleanup = ScopeExit([sync] {
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  });
  sync->SetCallBack("ReadRpc::CallRemoteMethod", [&](void* arg) {
    auto* request = static_cast<tserver::LWReadRequestPB*>(arg);
    reads += static_cast<int>(request->ql_batch().size());
  });
  sync->EnableProcessing();
  for (int phase = 0; phase < 3; ++phase) {
    model.ClearQueryCaches();
    if (phase == 2) {
      ASSERT_OK(model.WriteTripleToDocDB(3, 10, 300));
      ASSERT_OK(model.Flush());
      ASSERT_EQ(model.GetStatistics(), nullptr);
      ASSERT_GT(model.StatisticsVersion(), version);
      ASSERT_OK(model.RefreshStatistics());
    } else {
      ASSERT_NE(model.GetStatistics(), nullptr);
      ASSERT_EQ(model.StatisticsVersion(), version);
    }
    reads = 0;
    ScatterExecutor scatter;
    std::vector<ScatterExecutor::FragmentResultSet> fragments;
    ASSERT_OK(scatter.Execute(distributed, &fragments));
    GatherOperator gather;
    GatherResult gathered;
    ASSERT_OK(gather.Merge(fragments, distributed.gather_strategy(), &gathered));
    ASSERT_EQ(gathered.bindings.size(), phase == 2 ? 3 : 2);
    ASSERT_EQ(model.GetStatistics()->rows, phase == 2 ? 3 : 2);
    ASSERT_EQ(model.GetStatistics()->predicate_subject_counts.at(10).size(), phase == 2 ? 3 : 2);
    ASSERT_GT(reads.load(), 0) << "Query must reload its tablet cache";
  }
}

TEST_F(MdbDistributedIntegrationTest, YB_DEBUG_ONLY_TEST(StatisticsFailureDiscardsPartialCounts)) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  auto& model = RdfYBModel::Instance();
  for (int i = 0; i < 257; ++i) ASSERT_OK(model.WriteTripleToDocDB(1, 10, 100 + i));
  ASSERT_OK(model.Flush());
  ASSERT_OK(model.RefreshStatistics());
  ASSERT_EQ(model.GetStatistics()->rows, 257);
  auto* sync = SyncPoint::GetInstance();
  std::atomic<int> pages{0};
  auto cleanup = ScopeExit([sync] {
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  });
  sync->SetCallBack("ReadRpc::NotifyBatcher", [&](void* arg) {
    auto* response = static_cast<tserver::LWReadResponsePB*>(arg);
    for (auto& ql : *response->mutable_ql_batch()) {
      if (ql.has_paging_state() && !ql.paging_state().next_row_key().empty() && ++pages == 2) {
        ql.set_status(QLResponsePB::YQL_STATUS_RUNTIME_ERROR);
        ql.dup_error_message("injected statistics failure");
      }
    }
  });
  sync->EnableProcessing();
  const auto status = model.RefreshStatistics();
  sync->DisableProcessing();
  sync->ClearAllCallBacks();
  ASSERT_EQ(pages.load(), 2);
  ASSERT_FALSE(status.ok());
  ASSERT_NE(status.ToString().find("injected statistics failure"), std::string::npos);
  ASSERT_EQ(model.GetStatistics(), nullptr);
  ASSERT_OK(model.RefreshStatistics());
  ASSERT_EQ(model.GetStatistics()->rows, 257);
  ASSERT_EQ(model.GetStatistics()->predicate_subject_counts.at(10).at(1), 257);
}

TEST_F(
    MdbDistributedIntegrationTest,
    YB_DEBUG_ONLY_TEST(StatisticsChooseSelectiveJoinWithoutPlanningReads)) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  auto& model = RdfYBModel::Instance();
  const auto common = ParseIriForTest("https://example.org/common");
  const auto rare = ParseIriForTest("https://example.org/rare");
  const auto first = ParseIriForTest("https://example.org/node/0");
  constexpr int kCommonRows = 128;
  for (int i = 0; i < kCommonRows; ++i) {
    const auto node = ParseIriForTest("https://example.org/node/" + std::to_string(i));
    ASSERT_OK(model.WriteTripleToDocDB(node, common, 100 + i));
  }
  ASSERT_OK(model.WriteTripleToDocDB(first, rare, 999));
  ASSERT_OK(model.Flush());
  auto* sync = SyncPoint::GetInstance();
  auto cleanup = ScopeExit([sync] {
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  });
  std::array<int, 2> reads{};
  int collection_reads = 0;
  using Row = std::array<uint64_t, 3>;
  std::array<std::multiset<Row>, 2> results;
  for (int phase = 0; phase < 2; ++phase) {
    if (phase == 1) {
      model.ClearAll();
      ASSERT_NO_FATALS(InitModel());
      std::atomic<int> collected{0};
      auto collection_cleanup = ScopeExit([sync] {
        sync->DisableProcessing();
        sync->ClearAllCallBacks();
      });
      sync->SetCallBack("ReadRpc::CallRemoteMethod", [&](void* arg) {
        auto* request = static_cast<tserver::LWReadRequestPB*>(arg);
        collected += static_cast<int>(request->ql_batch().size());
      });
      sync->EnableProcessing();
      const auto start = std::chrono::steady_clock::now();
      ASSERT_OK(model.RefreshStatistics());
      sync->DisableProcessing();
      sync->ClearAllCallBacks();
      collection_reads = collected.load();
      LOG(INFO) << "MDB_ESTIMATE stats_read_requests=" << collection_reads
                << " histogram_entries=" << model.GetStatistics()->term_counts[0].size()
                    + model.GetStatistics()->term_counts[1].size()
                    + model.GetStatistics()->term_counts[2].size()
                << " stats_collection_us="
                << std::chrono::duration_cast<std::chrono::microseconds>(
                       std::chrono::steady_clock::now() - start).count();
    }
    QueryParser parser("SELECT ?s ?o ?r WHERE { ?s <https://example.org/common> ?o . "
                       "?s <https://example.org/rare> ?r }");
    auto op = parser.get_query_plan({});
    const auto* select = dynamic_cast<const OpSelect*>(op.get());
    ASSERT_NE(select, nullptr);
    const auto* bgp = dynamic_cast<const OpBasicGraphPattern*>(select->op.get());
    ASSERT_NE(bgp, nullptr);
    std::vector<std::unique_ptr<Plan>> base;
    for (const auto& triple : bgp->triples) {
      base.push_back(std::make_unique<TriplePlan>(triple.subject, triple.predicate, triple.object));
    }
    std::atomic<int> requests{0};
    auto phase_cleanup = ScopeExit([sync] {
      sync->DisableProcessing();
      sync->ClearAllCallBacks();
    });
    sync->SetCallBack("ReadRpc::CallRemoteMethod", [&](void* arg) {
      auto* request = static_cast<tserver::LWReadRequestPB*>(arg);
      requests += static_cast<int>(request->ql_batch().size());
    });
    sync->EnableProcessing();
    const auto start = std::chrono::steady_clock::now();
    auto plan = GreedyOptimizer::get_plan(base);
    ASSERT_EQ(requests.load(), 0);
    std::ostringstream printed;
    plan->print(printed, 0);
    const auto common_pos = printed.str().find(std::to_string(common));
    const auto rare_pos = printed.str().find(std::to_string(rare));
    ASSERT_NE(common_pos, std::string::npos);
    ASSERT_NE(rare_pos, std::string::npos);
    if (phase == 0) {
      ASSERT_LT(common_pos, rare_pos);
    } else {
      ASSERT_LT(rare_pos, common_pos);
    }
    auto iter = plan->get_binding_iter();
    Binding binding(get_query_ctx().get_var_size());
    iter->begin(binding);
    while (iter->next()) {
      results[phase].insert({binding[select->vars[0]].id,
                             binding[select->vars[1]].id,
                             binding[select->vars[2]].id});
    }
    ASSERT_OK(iter->status());
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
    reads[phase] = requests.load();
    LOG(INFO) << "MDB_ESTIMATE phase=" << phase << " read_requests=" << reads[phase]
              << " query_us=" << std::chrono::duration_cast<std::chrono::microseconds>(
                     std::chrono::steady_clock::now() - start).count()
              << " plan=" << printed.str();
  }
  ASSERT_EQ(results[0], results[1]);
  ASSERT_EQ(results[1].size(), 1);
  ASSERT_EQ(*results[1].begin(), (Row{first, 100, 999}));
  ASSERT_LT(reads[1], reads[0]);
  ASSERT_GT(collection_reads, 0);
  ASSERT_LT(reads[1] + collection_reads, reads[0]);
  ASSERT_NO_FATALS(InitRouter());
  QueryParser parser("SELECT ?s ?o ?r WHERE { ?s <https://example.org/common> ?o . "
                     "?s <https://example.org/rare> ?r }");
  auto op = parser.get_query_plan({});
  DistributedPlan distributed;
  DistributedPlanGenerator generator(&router_);
  ASSERT_OK(generator.Generate(*op, &distributed));
  ScatterExecutor scatter;
  std::vector<ScatterExecutor::FragmentResultSet> fragments;
  ASSERT_OK(scatter.Execute(distributed, &fragments));
  GatherOperator gather;
  GatherResult gathered;
  ASSERT_OK(gather.Merge(fragments, distributed.gather_strategy(), &gathered));
  ASSERT_EQ(gathered.total_output_rows, 1);
}

TEST_F(MdbDistributedIntegrationTest, YB_DEBUG_ONLY_TEST(StatisticsPreserveCorrelatedJoinAtScale)) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  auto& model = RdfYBModel::Instance();
  const auto common = ParseIriForTest("https://example.org/common");
  const auto member = ParseIriForTest("https://example.org/member");
  const auto label = ParseIriForTest("https://example.org/label");
  const auto noise = ParseIriForTest("https://example.org/noise");
  const auto rare = ParseIriForTest("https://example.org/rare");
  const auto group = ParseIriForTest("https://example.org/GroupA");
  for (int i = 0; i < 10000; ++i) {
    const uint64_t subject = 100000 + i;
    ASSERT_OK(model.WriteTripleToDocDB(subject, common, 200000 + i));
    if (i < 5000) {
      ASSERT_OK(model.WriteTripleToDocDB(subject, member, group));
    } else {
      ASSERT_OK(model.WriteTripleToDocDB(subject, label, 300000 + i));
    }
    if (i < 4990) ASSERT_OK(model.WriteTripleToDocDB(subject, noise, 999));
    if (i < 10) ASSERT_OK(model.WriteTripleToDocDB(subject, rare, 998));
  }
  ASSERT_OK(model.Flush());
  ASSERT_NO_FATALS(InitRouter());
  using Row = std::array<uint64_t, 2>;
  std::multiset<Row> expected;
  for (uint64_t i = 0; i < 4990; ++i) expected.insert({100000 + i, 999});
  for (bool negative : {false, true}) {
    for (bool reversed : {false, true}) {
      std::array<int, 2> execution_reads{};
      for (int phase = 0; phase < 2; ++phase) {
        SCOPED_TRACE(Format("negative=$0 reversed=$1 statistics=$2", negative, reversed, phase));
        model.ClearAll();
        ASSERT_NO_FATALS(InitModel());
        model.SetUsePartitionLoad(true);
        auto* sync = SyncPoint::GetInstance();
        std::atomic<int> requests{0};
        auto cleanup = ScopeExit([sync] {
          sync->DisableProcessing();
          sync->ClearAllCallBacks();
        });
        sync->SetCallBack("ReadRpc::CallRemoteMethod", [&](void* arg) {
          auto* request = static_cast<tserver::LWReadRequestPB*>(arg);
          requests += static_cast<int>(request->ql_batch().size());
        });
        sync->EnableProcessing();
        int collection_reads = 0;
        if (phase == 1) {
          ASSERT_OK(model.RefreshStatistics());
          ASSERT_EQ(model.GetStatistics()->rows, 25000);
          ASSERT_EQ(model.GetStatistics()->predicate_object_counts.at(member).at(group), 5000);
          collection_reads = requests.exchange(0);
        }
        const std::string membership =
            "?s <https://example.org/member> <https://example.org/GroupA>";
        const std::string alternative =
            negative ? "?s <https://example.org/label> ?o" : "?s <https://example.org/noise> ?o";
        QueryParser parser("SELECT ?s ?o WHERE { " +
                           (reversed ? alternative + " . " + membership
                                     : membership + " . " + alternative) + " }");
        auto op = parser.get_query_plan({});
        const auto* select = dynamic_cast<const OpSelect*>(op.get());
        ASSERT_NE(select, nullptr);
        const auto* bgp = dynamic_cast<const OpBasicGraphPattern*>(select->op.get());
        ASSERT_NE(bgp, nullptr);
        DistributedPlan distributed;
        DistributedPlanGenerator generator(&router_);
        ASSERT_OK(generator.Generate(*op, &distributed));
        ASSERT_EQ(distributed.num_fragments(), kNumTablets);
        const auto& fragment = distributed.fragments().front();
        std::vector<std::unique_ptr<Plan>> base;
        for (size_t i = 0; i < bgp->triples.size(); ++i) {
          const auto& triple = bgp->triples[i];
          base.push_back(std::make_unique<TriplePlan>(
              triple.subject, triple.predicate, triple.object,
              i == 0 ? fragment.perm_index : -1, i == 0 ? fragment.tablet_id : ""));
        }
        if (phase == 1 && negative) {
          ASSERT_DOUBLE_EQ(base[0]->estimate_output_size(), 5000);
          ASSERT_DOUBLE_EQ(base[1]->estimate_output_size(), 5000);
          ASSERT_GT(base[0]->estimate_cost(), base[1]->estimate_cost());
        }
        auto plan = GreedyOptimizer::get_plan(base, phase == 1);
        std::ostringstream printed;
        plan->print(printed, 0);
        if (phase == 1) {
          const auto other = negative ? label : noise;
          const auto first = reversed ? other : member;
          const auto second = reversed ? member : other;
          ASSERT_NE(printed.str().find(std::to_string(first)), std::string::npos);
          ASSERT_NE(printed.str().find(std::to_string(second)), std::string::npos);
          ASSERT_LT(printed.str().find(std::to_string(second)),
                    printed.str().find(std::to_string(first)));
        }
        ASSERT_EQ(requests.load(), 0);
        ScatterExecutor scatter;
        std::vector<ScatterExecutor::FragmentResultSet> fragments;
        ASSERT_OK(scatter.Execute(distributed, &fragments));
        GatherOperator gather;
        GatherResult gathered;
        ASSERT_OK(gather.Merge(fragments, distributed.gather_strategy(), &gathered));
        sync->DisableProcessing();
        std::multiset<Row> actual;
        for (const auto& binding : gathered.bindings) {
          actual.insert({(*binding)[select->vars[0]].id, (*binding)[select->vars[1]].id});
        }
        if (negative) {
          ASSERT_TRUE(actual.empty());
        } else {
          ASSERT_EQ(actual, expected);
        }
        execution_reads[phase] = requests.load();
        LOG(INFO) << "MDB_SCAN_COST kind=" << (negative ? "negative" : "positive")
                  << " reversed=" << reversed << " phase=" << phase
                  << " collection_reads=" << collection_reads
                  << " execution_reads=" << execution_reads[phase]
                  << " total_reads=" << collection_reads + execution_reads[phase]
                  << " anchor_rows=" << base[0]->estimate_output_size()
                  << " other_rows=" << base[1]->estimate_output_size()
                  << " anchor_cost=" << base[0]->estimate_cost()
                  << " other_cost=" << base[1]->estimate_cost()
                  << " member_id=" << member << " alternative_id=" << (negative ? label : noise)
                  << " plan=" << printed.str();
      }
      ASSERT_GT(execution_reads[0], 0);
      ASSERT_LE(execution_reads[1], execution_reads[0]);
    }
  }
}

TEST_F(MdbDistributedIntegrationTest, HashJoinDocDBMatchesNestedLoopAndOracle) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  auto& model = RdfYBModel::Instance();
  using Triple = std::array<uint64_t, 3>;
  std::vector<Triple> data;
  for (uint64_t i = 1; i <= 130; ++i) {
    data.push_back({i, 1000 + i % 3, i + 1});
  }
  data.push_back({1, 2000, 2});
  data.push_back({2, 2000, 3});
  data.push_back({10000, 2000, 20000});
  for (const auto& triple : data) {
    ASSERT_OK(model.WriteTripleToDocDB(triple[0], triple[1], triple[2]));
  }
  ASSERT_OK(model.Flush());

  std::multiset<std::array<uint64_t, 5>> expected;
  for (const auto& left : data) {
    for (const auto& right : data) {
      if (left[2] == right[0]) {
        expected.insert({left[0], left[1], left[2], right[1], right[2]});
      }
    }
  }
  ASSERT_GT(expected.size(), 129U);
  get_query_ctx() = QueryContext{};
  const std::array<VarId, 5> vars{
      get_query_ctx().get_or_create_var("s"), get_query_ctx().get_or_create_var("p"),
      get_query_ctx().get_or_create_var("m"), get_query_ctx().get_or_create_var("q"),
      get_query_ctx().get_or_create_var("o")};
  for (bool hash : {true, false}) {
    model.ClearQueryCaches();
    auto left = std::make_unique<SPARQL::TriplePlan>(Id(vars[0]), Id(vars[1]), Id(vars[2]));
    auto right = std::make_unique<SPARQL::TriplePlan>(Id(vars[2]), Id(vars[3]), Id(vars[4]));
    std::unique_ptr<Plan> plan;
    if (hash) {
      plan = std::make_unique<HashJoinPlan>(std::move(left), std::move(right));
    } else {
      plan = std::make_unique<IndexNestedLoopPlan>(std::move(left), std::move(right));
    }
    auto iter = plan->get_binding_iter();
    Binding binding(5);
    iter->begin(binding);
    std::multiset<std::array<uint64_t, 5>> actual;
    while (iter->next()) {
      actual.insert({binding[vars[0]].id, binding[vars[1]].id, binding[vars[2]].id,
                     binding[vars[3]].id, binding[vars[4]].id});
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(actual, expected) << "hash=" << hash;
    if (hash) {
      iter->reset();
      size_t rows = 0;
      while (iter->next()) ++rows;
      ASSERT_OK(iter->status());
      ASSERT_EQ(rows, expected.size());
    }
  }
}

// Diagnostic timings have no pass/fail threshold; all output rows are checked independently.
TEST_F(MdbDistributedIntegrationTest, YB_DEBUG_ONLY_TEST(HashJoinAccessPathComparison)) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  auto& model = RdfYBModel::Instance();
  const bool previous_partition_load = model.UsePartitionLoad();
  auto restore_options = ScopeExit([&] { model.SetUsePartitionLoad(previous_partition_load); });
  model.SetUsePartitionLoad(true);
  using Triple = std::array<uint64_t, 3>;
  using Row = std::array<uint64_t, 3>;
  struct Scenario {
    const char* name;
    uint64_t predicate;
    uint64_t right_predicate;
    std::vector<Triple> left;
    std::vector<Triple> right;
  };
  std::vector<Triple> unique_right, fanout_right;
  for (uint64_t i = 0; i < 2048; ++i) {
    unique_right.push_back({100000 + i, 2000, 300000 + i});
    fanout_right.push_back({100000 + i / 16, 2001, 400000 + i});
  }
  std::vector<Scenario> scenarios{
      {"sparse", 1000, 2000, {}, unique_right},
      {"distinct", 1001, 2000, {}, unique_right},
      {"hot", 1002, 2000, {}, unique_right},
      {"fanout", 1003, 2001, {}, fanout_right},
      {"no_match", 1004, 2000, {}, unique_right}};
  for (size_t scenario = 0; scenario < scenarios.size(); ++scenario) {
    auto& test = scenarios[scenario];
    const size_t count = scenario == 0 ? 4 : scenario == 3 ? 128 : scenario == 4 ? 64 : 512;
    for (size_t i = 0; i < count; ++i) {
      const uint64_t key = scenario == 2 ? i % 8 : i;
      test.left.push_back({500000 + scenario * 10000 + i, test.predicate,
                           (scenario == 4 ? 900000 : 100000) + key});
    }
  }
  for (const auto& triple : unique_right) {
    ASSERT_OK(model.WriteTripleToDocDB(triple[0], triple[1], triple[2]));
  }
  for (const auto& triple : fanout_right) {
    ASSERT_OK(model.WriteTripleToDocDB(triple[0], triple[1], triple[2]));
  }
  for (const auto& test : scenarios) {
    for (const auto& triple : test.left) {
      ASSERT_OK(model.WriteTripleToDocDB(triple[0], triple[1], triple[2]));
    }
  }
  ASSERT_OK(model.Flush());
  get_query_ctx() = QueryContext{};
  const VarId s_var = get_query_ctx().get_or_create_var("s");
  const VarId m_var = get_query_ctx().get_or_create_var("m");
  const VarId o_var = get_query_ctx().get_or_create_var("o");

  auto* sync = SyncPoint::GetInstance();
  auto cleanup = ScopeExit([sync] {
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  });
  std::atomic<int> requests{0};
  sync->SetCallBack("ReadRpc::CallRemoteMethod", [&](void* arg) {
    auto* request = static_cast<tserver::LWReadRequestPB*>(arg);
    requests += static_cast<int>(request->ql_batch().size());
  });
  for (const auto& test : scenarios) {
    std::vector<Row> expected;
    for (const auto& left : test.left) {
      for (const auto& right : test.right) {
        if (left[2] == right[0]) expected.push_back({left[0], left[2], right[2]});
      }
    }
    std::sort(expected.begin(), expected.end());
    for (int trial = 0; trial < 4; ++trial) {
      for (int position = 0; position < 2; ++position) {
        const bool hash = ((trial + position) % 2 == 0);
        SCOPED_TRACE(Format("scenario=$0 trial=$1 hash=$2", test.name, trial, hash));
        model.ClearQueryCaches();
        requests = 0;
        auto left = std::make_unique<SPARQL::TriplePlan>(
            Id(s_var), Id(::ObjectId(test.predicate)), Id(m_var));
        auto right = std::make_unique<SPARQL::TriplePlan>(
            Id(m_var), Id(::ObjectId(test.right_predicate)), Id(o_var));
        if (!hash) right->set_input_vars(left->get_vars());
        const auto left_vars = left->get_vars();
        const auto right_vars = right->get_vars();
        LOG(INFO) << "MDB_JOIN_RUN_BEGIN scenario=" << test.name << " trial=" << trial
                  << " algorithm=" << (hash ? "hash" : "index");
        sync->EnableProcessing();
        const auto start = std::chrono::steady_clock::now();
        auto lhs = left->get_binding_iter();
        auto rhs = right->get_binding_iter();
        const auto* lhs_stats = lhs.get();
        const auto* rhs_stats = rhs.get();
        std::unique_ptr<BindingIter> iter;
        HashJoinBindingIter* hash_iter = nullptr;
        if (hash) {
          auto join = std::make_unique<HashJoinBindingIter>(
              std::move(lhs), std::move(rhs), left_vars, right_vars);
          hash_iter = join.get();
          iter = std::move(join);
        } else {
          iter = std::make_unique<IndexNestedLoopJoin>(std::move(lhs), std::move(rhs));
        }
        Binding binding(3);
        iter->begin(binding);
        const auto build_end = std::chrono::steady_clock::now();
        ASSERT_OK(iter->status());
        const auto memory = hash_iter ? hash_iter->build_memory()
                                      : HashJoinBindingIter::BuildMemory{};
        // Exclude the diagnostic's O(build rows) memory traversal and oracle comparison.
        const auto drain_start = std::chrono::steady_clock::now();
        std::vector<Row> actual;
        while (iter->next()) {
          actual.push_back({binding[s_var].id, binding[m_var].id, binding[o_var].id});
        }
        const auto end = std::chrono::steady_clock::now();
        sync->DisableProcessing();
        ASSERT_OK(iter->status());
        const auto prepare_us = std::chrono::duration_cast<std::chrono::microseconds>(
            build_end - start).count();
        const auto drain_us = std::chrono::duration_cast<std::chrono::microseconds>(
            end - drain_start).count();
        LOG(INFO) << "MDB_JOIN_RUN_END scenario=" << test.name << " trial=" << trial
                  << " algorithm=" << (hash ? "hash" : "index")
                  << " left_rows=" << test.left.size() << " right_rows=" << test.right.size()
                  << " output_rows=" << actual.size() << " requests=" << requests.load()
                  << " prepare_us=" << prepare_us << " drain_us=" << drain_us
                  << " total_us=" << prepare_us + drain_us
                  << " lhs_results=" << lhs_stats->results << " rhs_results=" << rhs_stats->results
                  << " rhs_next=" << rhs_stats->stat_next << " rhs_reset=" << rhs_stats->stat_reset
                  << " build_rows=" << memory.rows << " key_buckets=" << memory.key_buckets
                  << " payload_bytes=" << memory.payload_bytes
                  << " accounted_bytes=" << memory.accounted_bytes;
        std::sort(actual.begin(), actual.end());
        ASSERT_EQ(actual, expected);
      }
    }
  }
}

// Keep the same physical POS anchor to separate join choice from CLI routing changes.
TEST_F(MdbDistributedIntegrationTest, YB_DEBUG_ONLY_TEST(HashJoinSamePosScaleComparison)) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  auto& model = RdfYBModel::Instance();
  const bool previous_partition = model.UsePartitionLoad();
  const bool previous_flag = FLAGS_mdb_enable_hash_join;
  auto restore = ScopeExit([&] {
    model.SetUsePartitionLoad(previous_partition);
    FLAGS_mdb_enable_hash_join = previous_flag;
  });
  model.SetUsePartitionLoad(true);
  FLAGS_mdb_enable_hash_join = true;
  for (uint64_t i = 0; i < 5000; ++i) {
    ASSERT_OK(model.WriteTripleToDocDB(100000 + i, 1000, 100000 + i));
    ASSERT_OK(model.WriteTripleToDocDB(105000 + i, 1001, 100000 + i % 8));
    ASSERT_OK(model.WriteTripleToDocDB(100000 + i, 2000, 300000 + i));
  }
  for (uint64_t i = 0; i < 10000; ++i) {
    ASSERT_OK(model.WriteTripleToDocDB(100000 + i, 3000, 400000 + i));
  }
  ASSERT_OK(model.Flush());
  ASSERT_OK(model.RefreshStatistics());
  ASSERT_EQ(model.GetStatistics()->rows, 25000U);
  ASSERT_NO_FATALS(InitRouter());

  // Discover ownership from persisted POS rows, independently of planner routing.
  std::array<std::string, 2> anchors;
  std::array<size_t, 2> left_counts{};
  const auto* table = model.GetTable(DocKeyEncoder::kPOS);
  ASSERT_NE(table, nullptr);
  for (const auto& tablet : router_.GetTablets(DocKeyEncoder::kPOS)) {
    client::TableIteratorOptions options;
    options.columns = {"c0", "c1", "c2"};
    options.tablet = tablet.tablet_id;
    for (const auto& row : client::TableRange(*table, options)) {
      const auto predicate = row.column(0).int64_value();
      if (predicate != 1000 && predicate != 1001) continue;
      const auto scenario = static_cast<size_t>(predicate - 1000);
      if (anchors[scenario].empty()) anchors[scenario] = tablet.tablet_id;
      ASSERT_EQ(anchors[scenario], tablet.tablet_id);
      ++left_counts[scenario];
    }
  }
  ASSERT_EQ(left_counts[0], 5000U);
  ASSERT_EQ(left_counts[1], 5000U);
  get_query_ctx() = QueryContext{};
  const VarId s_var = get_query_ctx().get_or_create_var("s");
  const VarId m_var = get_query_ctx().get_or_create_var("m");
  const VarId o_var = get_query_ctx().get_or_create_var("o");
  using Row = std::array<uint64_t, 3>;
  auto* sync = SyncPoint::GetInstance();
  auto cleanup = ScopeExit([sync] {
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  });
  std::atomic<int> requests{0};
  sync->SetCallBack("ReadRpc::CallRemoteMethod", [&](void* arg) {
    auto* request = static_cast<tserver::LWReadRequestPB*>(arg);
    requests += static_cast<int>(request->ql_batch().size());
  });
  for (size_t scenario = 0; scenario < 2; ++scenario) {
    const char* name = scenario == 0 ? "distinct" : "hot";
    std::vector<Row> expected;
    for (uint64_t i = 0; i < 5000; ++i) {
      const uint64_t key = scenario == 0 ? i : i % 8;
      expected.push_back({100000 + scenario * 5000 + i, 100000 + key, 300000 + key});
    }
    std::sort(expected.begin(), expected.end());
    for (int trial = 0; trial < 4; ++trial) {
      for (int position = 0; position < 2; ++position) {
        const bool hash = (trial + position) % 2 == 0;
        SCOPED_TRACE(Format("scenario=$0 trial=$1 hash=$2", name, trial, hash));
        model.ClearQueryCaches();
        auto left = std::make_unique<TriplePlan>(
            Id(s_var), Id(::ObjectId(1000 + scenario)), Id(m_var),
            DocKeyEncoder::kPOS, anchors[scenario]);
        auto right = std::make_unique<TriplePlan>(
            Id(m_var), Id(::ObjectId(2000)), Id(o_var));
        ASSERT_EQ(GreedyOptimizer::should_use_hash_join(*left, *right), scenario == 0);
        if (!hash) right->set_input_vars(left->get_vars());
        const auto left_vars = left->get_vars();
        const auto right_vars = right->get_vars();
        requests = 0;
        sync->EnableProcessing();
        const auto start = std::chrono::steady_clock::now();
        auto lhs = left->get_binding_iter();
        auto rhs = right->get_binding_iter();
        std::unique_ptr<BindingIter> iter;
        HashJoinBindingIter* hash_iter = nullptr;
        if (hash) {
          auto join = std::make_unique<HashJoinBindingIter>(
              std::move(lhs), std::move(rhs), left_vars, right_vars);
          hash_iter = join.get();
          iter = std::move(join);
        } else {
          iter = std::make_unique<IndexNestedLoopJoin>(std::move(lhs), std::move(rhs));
        }
        Binding binding(3);
        iter->begin(binding);
        const auto build_end = std::chrono::steady_clock::now();
        ASSERT_OK(iter->status());
        const auto memory = hash_iter ? hash_iter->build_memory()
                                      : HashJoinBindingIter::BuildMemory{};
        const auto drain_start = std::chrono::steady_clock::now();
        std::vector<Row> actual;
        while (iter->next()) {
          actual.push_back({binding[s_var].id, binding[m_var].id, binding[o_var].id});
        }
        const auto end = std::chrono::steady_clock::now();
        sync->DisableProcessing();
        ASSERT_OK(iter->status());
        const auto prepare_us = std::chrono::duration_cast<std::chrono::microseconds>(
            build_end - start).count();
        const auto drain_us = std::chrono::duration_cast<std::chrono::microseconds>(
            end - drain_start).count();
        LOG(INFO) << "MDB_SAME_POS scenario=" << name << " trial=" << trial
                  << " algorithm=" << (hash ? "hash" : "index")
                  << " left_rows=5000 right_rows=5000 anchor=" << anchors[scenario]
                  << " output_rows=" << actual.size() << " requests=" << requests.load()
                  << " prepare_us=" << prepare_us << " drain_us=" << drain_us
                  << " total_us=" << prepare_us + drain_us
                  << " build_rows=" << memory.rows << " accounted_bytes=" << memory.accounted_bytes;
        std::sort(actual.begin(), actual.end());
        ASSERT_EQ(actual, expected);
      }
    }
  }
}

TEST_F(MdbDistributedIntegrationTest, HashJoinSelectionUsesConditionalKeysAndGuards) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  auto& model = RdfYBModel::Instance();
  const bool previous_flag = FLAGS_mdb_enable_hash_join;
  const auto previous_budget = FLAGS_mdb_hash_join_max_estimated_build_bytes;
  const bool previous_partition = model.UsePartitionLoad();
  auto restore = ScopeExit([&] {
    FLAGS_mdb_enable_hash_join = previous_flag;
    FLAGS_mdb_hash_join_max_estimated_build_bytes = previous_budget;
    model.SetUsePartitionLoad(previous_partition);
  });
  ASSERT_FALSE(FLAGS_mdb_enable_hash_join);
  model.SetUsePartitionLoad(true);
  using Triple = std::array<uint64_t, 3>;
  using Row = std::array<uint64_t, 3>;
  std::vector<Triple> unique_right, fanout_right;
  for (uint64_t i = 0; i < 2048; ++i) {
    unique_right.push_back({100000 + i, 2000, 300000 + i});
    fanout_right.push_back({100000 + i / 16, 2001, 400000 + i});
  }
  for (const auto* data : {&unique_right, &fanout_right}) {
    for (const auto& t : *data) ASSERT_OK(model.WriteTripleToDocDB(t[0], t[1], t[2]));
  }
  std::array<std::vector<Triple>, 5> left_rows;
  for (size_t scenario = 0; scenario < left_rows.size(); ++scenario) {
    const size_t count = scenario == 0 ? 4 : scenario == 3 ? 128 : scenario == 4 ? 128 : 512;
    for (size_t i = 0; i < count; ++i) {
      const uint64_t key = scenario == 2 ? i % 8 : i;
      Triple t{500000 + scenario * 10000 + i, 1000 + scenario,
               (scenario == 4 ? 900000 : 100000) + key};
      left_rows[scenario].push_back(t);
      ASSERT_OK(model.WriteTripleToDocDB(t[0], t[1], t[2]));
    }
  }
  ASSERT_OK(model.Flush());
  ASSERT_OK(model.RefreshStatistics());
  ASSERT_EQ(model.GetStatistics()->rows, 5380U);
  get_query_ctx() = QueryContext{};
  const VarId s_var = get_query_ctx().get_or_create_var("s");
  const VarId m_var = get_query_ctx().get_or_create_var("m");
  const VarId o_var = get_query_ctx().get_or_create_var("o");
  auto make_plans = [&](size_t scenario) {
    std::vector<std::unique_ptr<Plan>> plans;
    plans.push_back(std::make_unique<TriplePlan>(
        Id(s_var), Id(::ObjectId(1000 + scenario)), Id(m_var)));
    plans.push_back(std::make_unique<TriplePlan>(
        Id(m_var), Id(::ObjectId(scenario == 3 ? 2001 : 2000)), Id(o_var)));
    return plans;
  };
  auto* sync = SyncPoint::GetInstance();
  auto cleanup = ScopeExit([sync] {
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  });
  std::atomic<int> planning_requests{0};
  sync->SetCallBack("ReadRpc::CallRemoteMethod", [&](void* arg) {
    auto* request = static_cast<tserver::LWReadRequestPB*>(arg);
    planning_requests += static_cast<int>(request->ql_batch().size());
  });
  for (size_t scenario = 0; scenario < left_rows.size(); ++scenario) {
    const auto& right = scenario == 3 ? fanout_right : unique_right;
    std::vector<Row> expected;
    for (const auto& l : left_rows[scenario]) {
      for (const auto& r : right) {
        if (l[2] == r[0]) expected.push_back({l[0], l[2], r[2]});
      }
    }
    std::sort(expected.begin(), expected.end());
    for (int mode = 0; mode < 3; ++mode) {
      for (bool reversed : {false, true}) {
        SCOPED_TRACE(Format("scenario=$0 mode=$1 reversed=$2", scenario, mode, reversed));
        FLAGS_mdb_enable_hash_join = mode != 0;
        FLAGS_mdb_hash_join_max_estimated_build_bytes = mode == 2 ? 1 : previous_budget;
        model.ClearQueryCaches();
        auto plans = make_plans(scenario);
        if (reversed) std::swap(plans[0], plans[1]);
        planning_requests = 0;
        sync->EnableProcessing();
        auto plan = GreedyOptimizer::get_plan(plans, true);
        sync->DisableProcessing();
        ASSERT_EQ(planning_requests.load(), 0);
        const bool expected_hash = mode == 1 && (scenario == 1 || scenario == 3);
        ASSERT_EQ(dynamic_cast<HashJoinPlan*>(plan.get()) != nullptr, expected_hash);
        auto clone = plan->clone();
        ASSERT_EQ(dynamic_cast<HashJoinPlan*>(clone.get()) != nullptr, expected_hash);
        auto iter = clone->get_binding_iter();
        Binding binding(3);
        iter->begin(binding);
        std::vector<Row> actual;
        while (iter->next()) {
          actual.push_back({binding[s_var].id, binding[m_var].id, binding[o_var].id});
        }
        ASSERT_OK(iter->status());
        std::sort(actual.begin(), actual.end());
        ASSERT_EQ(actual, expected);
        LOG(INFO) << "MDB_HASH_SELECTION scenario=" << scenario << " mode=" << mode
                  << " reversed=" << reversed << " hash=" << expected_hash
                  << " rows=" << actual.size() << " planning_requests=" << planning_requests.load();
      }
    }
  }
  FLAGS_mdb_enable_hash_join = true;
  FLAGS_mdb_hash_join_max_estimated_build_bytes = previous_budget;
  {
    // Exercise the planning cap with synthetic estimates, without importing 65K rows.
    auto plans = make_plans(3);
    auto* stats = const_cast<RdfYBModel::Statistics*>(model.GetStatistics());
    const auto saved_total = stats->rows;
    auto& build_rows = stats->term_counts[1].at(2001);
    const auto saved_build = build_rows;
    auto restore_stats = ScopeExit([&] {
      stats->rows = saved_total;
      build_rows = saved_build;
    });
    stats->rows = 1000000;
    build_rows = HashJoinBindingIter::kDefaultMaxBuildRows;
    ASSERT_TRUE(GreedyOptimizer::should_use_hash_join(*plans[0], *plans[1]));
    ++build_rows;
    ASSERT_FALSE(GreedyOptimizer::should_use_hash_join(*plans[0], *plans[1]));
  }
  {
    auto plans = make_plans(3);
    ASSERT_TRUE(GreedyOptimizer::should_use_hash_join(*plans[0], *plans[1]));
    plans[0]->set_input_vars({s_var});
    ASSERT_FALSE(GreedyOptimizer::should_use_hash_join(*plans[0], *plans[1]));
  }
  {
    auto plans = make_plans(3);
    auto* lhs = dynamic_cast<TriplePlan*>(plans[0].get());
    lhs->tablet_id_ = "synthetic-anchor";
    lhs->tablet_perm_index_ = 0;
    ASSERT_FALSE(GreedyOptimizer::should_use_hash_join(*plans[0], *plans[1]));
    lhs->tablet_perm_index_ = 1;
    ASSERT_TRUE(GreedyOptimizer::should_use_hash_join(*plans[0], *plans[1]));
    auto* rhs = dynamic_cast<TriplePlan*>(plans[1].get());
    rhs->tablet_id_ = "synthetic-build-anchor";
    ASSERT_FALSE(GreedyOptimizer::should_use_hash_join(*plans[0], *plans[1]));
  }
  {
    auto plans = make_plans(3);
    plans.push_back(std::make_unique<TriplePlan>(
        Id(o_var), Id(::ObjectId(2000)), Id(s_var)));
    auto plan = GreedyOptimizer::get_plan(plans, true);
    ASSERT_EQ(dynamic_cast<HashJoinPlan*>(plan.get()), nullptr);
  }
  {
    auto plans = make_plans(3);
    model.SetUsePartitionLoad(false);
    ASSERT_FALSE(GreedyOptimizer::should_use_hash_join(*plans[0], *plans[1]));
    model.SetUsePartitionLoad(true);
    auto star = std::make_unique<TriplePlan>(
        Id(s_var), Id(::ObjectId(2000)), Id(o_var));
    ASSERT_FALSE(GreedyOptimizer::should_use_hash_join(*plans[0], *star));
    ASSERT_OK(model.WriteTripleToDocDB(9999999, 8888, 7777));
    ASSERT_OK(model.Flush());
    ASSERT_EQ(model.GetStatistics(), nullptr);
    auto plan = GreedyOptimizer::get_plan(plans, true);
    ASSERT_EQ(dynamic_cast<HashJoinPlan*>(plan.get()), nullptr);
  }
}

void MdbDistributedIntegrationTest::CheckHashJoinPredicateSubjectOverlap(
    size_t distinct_keys, size_t matching_keys, bool expected_hash) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  auto& model = RdfYBModel::Instance();
  const auto previous_flag = FLAGS_mdb_enable_hash_join;
  const auto previous_partition = model.UsePartitionLoad();
  auto restore = ScopeExit([&] {
    FLAGS_mdb_enable_hash_join = previous_flag;
    model.SetUsePartitionLoad(previous_partition);
  });
  model.SetUsePartitionLoad(true);
  using Row = std::array<uint64_t, 3>;
  std::vector<Row> left, right, expected;
  for (uint64_t i = 0; i < 768; ++i) {
    left.push_back({500000 + i, 1000, 100000 + i % distinct_keys});
  }
  for (uint64_t i = 0; i < 1280; ++i) {
    right.push_back({i < 2 * matching_keys ? 100000 + i / 2 : 900000 + i,
                     2000, 300000 + i});
  }
  for (const auto* rows : {&left, &right}) {
    for (const auto& row : *rows) ASSERT_OK(model.WriteTripleToDocDB(row[0], row[1], row[2]));
  }
  // Every probe key exists as a subject, often only under a different predicate.
  for (uint64_t key = 0; key < distinct_keys; ++key) {
    for (uint64_t i = 0; i < 8; ++i) {
      ASSERT_OK(model.WriteTripleToDocDB(100000 + key, 3000, 700000 + key * 8 + i));
    }
  }
  ASSERT_OK(model.Flush());
  auto* sync = SyncPoint::GetInstance();
  auto cleanup = ScopeExit([sync] {
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  });
  std::atomic<int> requests{0};
  sync->SetCallBack("ReadRpc::CallRemoteMethod", [&](void* arg) {
    const auto* request = static_cast<tserver::LWReadRequestPB*>(arg);
    requests += static_cast<int>(request->ql_batch().size());
  });
  sync->EnableProcessing();
  const auto collection_start = std::chrono::steady_clock::now();
  ASSERT_OK(model.RefreshStatistics());
  const auto collection_end = std::chrono::steady_clock::now();
  sync->DisableProcessing();
  const auto collection_reads = requests.load();
  const auto* stats = model.GetStatistics();
  size_t ps_pairs = 0, po_pairs = 0;
  for (const auto& [predicate, subjects] : stats->predicate_subject_counts)
    ps_pairs += subjects.size();
  for (const auto& [predicate, objects] : stats->predicate_object_counts)
    po_pairs += objects.size();
  LOG(INFO) << "MDB_OVERLAP_STATS keys=" << distinct_keys << " matching=" << matching_keys
            << " rows=" << stats->rows << " requests=" << collection_reads
            << " ps_pairs=" << ps_pairs << " po_pairs=" << po_pairs << " collection_us="
            << std::chrono::duration_cast<std::chrono::microseconds>(
                   collection_end - collection_start)
                   .count();
  ASSERT_EQ(stats->rows, 2048 + 8 * distinct_keys);
  ASSERT_EQ(stats->predicate_object_counts.at(1000).size(), distinct_keys);
  const auto& subjects = stats->predicate_subject_counts.at(2000);
  for (uint64_t key = 0; key < distinct_keys; ++key) {
    ASSERT_EQ(stats->term_counts[0].at(100000 + key), key < matching_keys ? 10 : 8);
    if (key < matching_keys) {
      ASSERT_EQ(subjects.at(100000 + key), 2);
    } else {
      ASSERT_EQ(subjects.count(100000 + key), 0);
    }
  }
  for (const auto& l : left) {
    for (const auto& r : right) {
      if (l[2] == r[0]) expected.push_back({l[0], l[2], r[2]});
    }
  }
  ASSERT_EQ(expected.size(), 2 * matching_keys * (768 / distinct_keys));
  std::sort(expected.begin(), expected.end());
  get_query_ctx() = QueryContext{};
  const VarId s_var = get_query_ctx().get_or_create_var("s");
  const VarId m_var = get_query_ctx().get_or_create_var("m");
  const VarId o_var = get_query_ctx().get_or_create_var("o");
  {
    auto left = std::make_unique<TriplePlan>(
        Id(s_var), Id(::ObjectId(uint64_t{1000})), Id(m_var));
    auto right = std::make_unique<TriplePlan>(
        Id(m_var), Id(::ObjectId(uint64_t{2000})), Id(o_var));
    IndexNestedLoopPlan forward(left->clone(), right->clone());
    IndexNestedLoopPlan reverse(right->clone(), left->clone());
    ASSERT_DOUBLE_EQ(forward.estimate_output_size(), expected.size());
    ASSERT_DOUBLE_EQ(reverse.estimate_output_size(), expected.size());
    ASSERT_DOUBLE_EQ(forward.clone()->estimate_output_size(), expected.size());
  }
  for (bool enabled : {false, true}) {
    for (bool reversed : {false, true}) {
      SCOPED_TRACE(Format("keys=$0 matching=$1 enabled=$2 reversed=$3",
                          distinct_keys, matching_keys, enabled, reversed));
      FLAGS_mdb_enable_hash_join = enabled;
      model.ClearQueryCaches();
      std::vector<std::unique_ptr<Plan>> plans;
      plans.push_back(std::make_unique<TriplePlan>(
          Id(s_var), Id(::ObjectId(uint64_t{1000})), Id(m_var)));
      plans.push_back(std::make_unique<TriplePlan>(
          Id(m_var), Id(::ObjectId(uint64_t{2000})), Id(o_var)));
      if (reversed) std::swap(plans[0], plans[1]);
      requests = 0;
      sync->EnableProcessing();
      auto plan = GreedyOptimizer::get_plan(plans, true);
      sync->DisableProcessing();
      ASSERT_EQ(requests.load(), 0);
      ASSERT_EQ(dynamic_cast<HashJoinPlan*>(plan.get()) != nullptr, enabled && expected_hash);
      auto clone = plan->clone();
      ASSERT_EQ(dynamic_cast<HashJoinPlan*>(clone.get()) != nullptr, enabled && expected_hash);
      auto iter = clone->get_binding_iter();
      Binding binding(3);
      iter->begin(binding);
      std::vector<Row> actual;
      while (iter->next()) {
        actual.push_back({binding[s_var].id, binding[m_var].id, binding[o_var].id});
      }
      ASSERT_OK(iter->status());
      std::sort(actual.begin(), actual.end());
      ASSERT_EQ(actual, expected);
      LOG(INFO) << "MDB_HASH_SUBJECT_OVERLAP keys=" << distinct_keys
                << " matching=" << matching_keys << " enabled=" << enabled
                << " reversed=" << reversed << " hash=" << (enabled && expected_hash)
                << " rows=" << actual.size() << " planning_requests=" << requests.load();
    }
  }
  if (distinct_keys > 64 && matching_keys < distinct_keys) {
    model.ClearQueryCaches();
    FLAGS_mdb_enable_hash_join = true;
    TriplePlan lhs(Id(s_var), Id(::ObjectId(uint64_t{1000})), Id(m_var));
    TriplePlan rhs(Id(m_var), Id(::ObjectId(uint64_t{2000})), Id(o_var));
    ASSERT_FALSE(model.HasQueryCaches(0));
    ASSERT_FALSE(model.HasQueryCaches(1));
    ASSERT_TRUE(GreedyOptimizer::should_use_hash_join(lhs, rhs));
    model.GetPartitionCache(0, 100000).InsertRow(100000, 3000, 700000);
    ASSERT_TRUE(model.HasQueryCaches(0));
    ASSERT_FALSE(GreedyOptimizer::should_use_hash_join(lhs, rhs));
    model.ClearQueryCaches();
    model.GetTabletCache(1, "cached-tablet").InsertRow(1000, 100000, 500000);
    ASSERT_TRUE(model.HasQueryCaches(1));
    ASSERT_FALSE(GreedyOptimizer::should_use_hash_join(lhs, rhs));
    model.ClearQueryCaches();
    ASSERT_TRUE(GreedyOptimizer::should_use_hash_join(lhs, rhs));
  }
  if (std::getenv("MDB_MEASURE_BOUND_INPUT") != nullptr) {
    FLAGS_mdb_enable_hash_join = false;
    for (int trial = 0; trial < 4; ++trial) {
      std::array<int, 2> reads{};
      for (bool optimized : {trial % 2 != 0, trial % 2 == 0}) {
        model.ClearQueryCaches();
        std::vector<std::unique_ptr<Plan>> base;
        base.push_back(std::make_unique<TriplePlan>(
            Id(s_var), Id(::ObjectId(uint64_t{1000})), Id(m_var)));
        base.push_back(std::make_unique<TriplePlan>(
            Id(m_var), Id(::ObjectId(uint64_t{2000})), Id(o_var)));
        std::vector<Row> actual;
        actual.reserve(expected.size());
        requests = 0;
        sync->EnableProcessing();
        const auto start = std::chrono::steady_clock::now();
        std::unique_ptr<Plan> plan;
        if (optimized) {
          plan = GreedyOptimizer::get_plan(base, true);
        } else {
          plan = std::make_unique<IndexNestedLoopPlan>(base[0]->clone(), base[1]->clone());
        }
        const auto plan_end = std::chrono::steady_clock::now();
        ASSERT_EQ(requests.load(), 0);
        ASSERT_DOUBLE_EQ(plan->estimate_output_size(), expected.size());
        auto iter = plan->get_binding_iter();
        auto* indexed = dynamic_cast<IndexNestedLoopJoin*>(iter.get());
        ASSERT_NE(indexed, nullptr);
        Binding binding(3);
        iter->begin(binding);
        while (iter->next())
          actual.push_back({binding[s_var].id, binding[m_var].id, binding[o_var].id});
        const auto finish = std::chrono::steady_clock::now();
        sync->DisableProcessing();
        ASSERT_OK(iter->status());
        ASSERT_EQ(indexed->lhs->results, optimized && distinct_keys > 64 ? 1280 : 768);
        reads[optimized] = requests.load();
        LOG(INFO) << "MDB_BOUND_INPUT keys=" << distinct_keys << " matching=" << matching_keys
                  << " trial=" << trial << " optimized=" << optimized
                  << " output_rows=" << actual.size()
                  << " estimated_rows=" << plan->estimate_output_size()
                  << " lhs_rows=" << indexed->lhs->results << " requests=" << requests.load()
                  << " planning_us="
                  << std::chrono::duration_cast<std::chrono::microseconds>(plan_end - start).count()
                  << " total_us="
                  << std::chrono::duration_cast<std::chrono::microseconds>(finish - start).count();
        std::sort(actual.begin(), actual.end());
        ASSERT_EQ(actual, expected);
      }
      if (distinct_keys > 64) {
        ASSERT_LT(reads[1], reads[0]);
      } else {
        ASSERT_EQ(reads[1], reads[0]);
      }
    }
    FLAGS_mdb_enable_hash_join = true;
  }
  if (std::getenv("MDB_MEASURE_PREDICATE_OVERLAP") == nullptr) return;
  ASSERT_NO_FATALS(InitRouter());
  std::string anchor;
  size_t anchor_rows = 0;
  const auto* table = model.GetTable(DocKeyEncoder::kPOS);
  ASSERT_NE(table, nullptr);
  for (const auto& tablet : router_.GetTablets(DocKeyEncoder::kPOS)) {
    client::TableIteratorOptions options;
    options.columns = {"c0", "c1", "c2"};
    options.tablet = tablet.tablet_id;
    for (const auto& row : client::TableRange(*table, options)) {
      if (row.column(0).int64_value() != 1000) continue;
      if (anchor.empty()) anchor = tablet.tablet_id;
      ASSERT_EQ(anchor, tablet.tablet_id);
      ++anchor_rows;
    }
  }
  ASSERT_EQ(anchor_rows, 768);
  for (int trial = 0; trial < 4; ++trial) {
    for (bool hash : {trial % 2 != 0, trial % 2 == 0}) {
      model.ClearQueryCaches();
      auto lhs_plan = std::make_unique<TriplePlan>(
          Id(s_var), Id(::ObjectId(uint64_t{1000})), Id(m_var), 1, anchor);
      auto rhs_plan = std::make_unique<TriplePlan>(
          Id(m_var), Id(::ObjectId(uint64_t{2000})), Id(o_var));
      ASSERT_EQ(GreedyOptimizer::should_use_hash_join(*lhs_plan, *rhs_plan), expected_hash);
      if (!hash) rhs_plan->set_input_vars(lhs_plan->get_vars());
      const auto lhs_vars = lhs_plan->get_vars();
      const auto rhs_vars = rhs_plan->get_vars();
      std::vector<Row> actual;
      actual.reserve(expected.size());
      requests = 0;
      sync->EnableProcessing();
      const auto start = std::chrono::steady_clock::now();
      auto lhs = lhs_plan->get_binding_iter();
      auto rhs = rhs_plan->get_binding_iter();
      std::unique_ptr<BindingIter> iter;
      HashJoinBindingIter* hash_iter = nullptr;
      if (hash) {
        auto join = std::make_unique<HashJoinBindingIter>(
            std::move(lhs), std::move(rhs), lhs_vars, rhs_vars);
        hash_iter = join.get();
        iter = std::move(join);
      } else {
        iter = std::make_unique<IndexNestedLoopJoin>(std::move(lhs), std::move(rhs));
      }
      Binding binding(3);
      iter->begin(binding);
      const auto build_end = std::chrono::steady_clock::now();
      ASSERT_OK(iter->status());
      const auto memory =
          hash_iter ? hash_iter->build_memory() : HashJoinBindingIter::BuildMemory{};
      const auto drain_start = std::chrono::steady_clock::now();
      while (iter->next())
        actual.push_back({binding[s_var].id, binding[m_var].id, binding[o_var].id});
      const auto end = std::chrono::steady_clock::now();
      sync->DisableProcessing();
      ASSERT_OK(iter->status());
      const auto final_memory = hash_iter ? hash_iter->build_memory()
                                         : HashJoinBindingIter::BuildMemory{};
      ASSERT_EQ(final_memory.tracked_bytes, 0);
      ASSERT_LE(final_memory.peak_tracked_bytes, final_memory.byte_limit);
      const auto prepare_us = std::chrono::duration_cast<std::chrono::microseconds>(
          build_end - start).count();
      const auto drain_us = std::chrono::duration_cast<std::chrono::microseconds>(
          end - drain_start).count();
      LOG(INFO) << "MDB_OVERLAP_RUN keys=" << distinct_keys << " matching=" << matching_keys
                << " trial=" << trial << " algorithm=" << (hash ? "hash" : "index")
                << " selected_hash=" << expected_hash << " output_rows=" << actual.size()
                << " requests=" << requests.load() << " prepare_us=" << prepare_us
                << " drain_us=" << drain_us << " total_us=" << prepare_us + drain_us
                << " build_rows=" << memory.rows
                << " tracked_peak=" << final_memory.peak_tracked_bytes
                << " byte_limit=" << final_memory.byte_limit << " anchor=" << anchor;
      std::sort(actual.begin(), actual.end());
      ASSERT_EQ(actual, expected);
    }
  }
}

TEST_F(MdbDistributedIntegrationTest, HashSelectionCostsUnrelatedPredicateSubjects) {
  ASSERT_NO_FATALS(CheckHashJoinPredicateSubjectOverlap(96, 0, true));
}

TEST_F(MdbDistributedIntegrationTest, HashSelectionUsesPartialPredicateSubjectOverlap) {
  ASSERT_NO_FATALS(CheckHashJoinPredicateSubjectOverlap(96, 24, true));
}

TEST_F(MdbDistributedIntegrationTest, HashSelectionKeepsFanoutWithPredicateOverlap) {
  ASSERT_NO_FATALS(CheckHashJoinPredicateSubjectOverlap(96, 96, true));
}

TEST_F(MdbDistributedIntegrationTest, HashSelectionKeepsHotPredicateKeysIndexed) {
  ASSERT_NO_FATALS(CheckHashJoinPredicateSubjectOverlap(8, 8, false));
}

TEST_F(MdbDistributedIntegrationTest, YB_DEBUG_ONLY_TEST(KnownInputJoinGuardsAndInvalidation)) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  auto& model = RdfYBModel::Instance();
  const bool previous_partition = model.UsePartitionLoad();
  auto restore_partition = ScopeExit([&] { model.SetUsePartitionLoad(previous_partition); });
  model.SetUsePartitionLoad(true);
  for (const auto& row : std::vector<std::array<uint64_t, 3>>{
           {1, 10, 100}, {2, 10, 100}, {3, 10, 101},
           {100, 20, 500}, {100, 20, 501}, {9, 30, 999}}) {
    ASSERT_OK(model.WriteTripleToDocDB(row[0], row[1], row[2]));
  }
  ASSERT_OK(model.Flush());
  ASSERT_OK(model.RefreshStatistics());
  const auto oid = [](uint64_t value) { return Id(::ObjectId(value)); };
  TriplePlan input(Id(VarId(0)), oid(10), Id(VarId(1)));
  TriplePlan scan(Id(VarId(1)), oid(20), Id(VarId(2)));
  double output = -1, cost = -1;
  ASSERT_FALSE(scan.estimate_index_join(input, &output, &cost));
  scan.set_input_vars(input.get_vars());
  ASSERT_TRUE(scan.estimate_index_join(input, &output, &cost));
  ASSERT_DOUBLE_EQ(output, 4);
  ASSERT_GT(cost, 0);
  input.subject_assigned = true;
  ASSERT_FALSE(scan.estimate_index_join(input, &output, &cost));
  input.subject_assigned = false;
  input.tablet_id_ = "synthetic-anchor";
  input.tablet_perm_index_ = 0;
  ASSERT_FALSE(scan.estimate_index_join(input, &output, &cost));
  input.tablet_perm_index_ = 1;
  ASSERT_TRUE(scan.estimate_index_join(input, &output, &cost));
  ASSERT_DOUBLE_EQ(output, 4);
  input.tablet_id_.clear();
  scan.tablet_id_ = "synthetic-rhs-anchor";
  ASSERT_FALSE(scan.estimate_index_join(input, &output, &cost));
  scan.tablet_id_.clear();
  TriplePlan star(Id(VarId(0)), oid(20), Id(VarId(2)));
  star.set_input_vars(input.get_vars());
  ASSERT_FALSE(star.estimate_index_join(input, &output, &cost));
  TriplePlan cycle(Id(VarId(1)), oid(20), Id(VarId(0)));
  cycle.set_input_vars(input.get_vars());
  ASSERT_FALSE(cycle.estimate_index_join(input, &output, &cost));
  model.SetUsePartitionLoad(false);
  ASSERT_FALSE(scan.estimate_index_join(input, &output, &cost));
  model.SetUsePartitionLoad(true);
  ASSERT_EQ(g_pack_to_gmap.count(800010), 0);
  ASSERT_EQ(g_pack_to_gmap.count(800020), 0);
  g_pack_to_gmap[800010] = 10;
  g_pack_to_gmap[800020] = 20;
  auto restore_mapping = ScopeExit([] {
    g_pack_to_gmap.erase(800010);
    g_pack_to_gmap.erase(800020);
  });
  TriplePlan mapped_input(Id(VarId(0)), oid(800010), Id(VarId(1)));
  TriplePlan mapped_scan(Id(VarId(1)), oid(800020), Id(VarId(2)));
  mapped_scan.set_input_vars(mapped_input.get_vars());
  ASSERT_TRUE(mapped_scan.estimate_index_join(mapped_input, &output, &cost));
  ASSERT_DOUBLE_EQ(output, 4);
  {
    model.ClearQueryCaches();
    IndexNestedLoopPlan missing(input.clone(), std::make_unique<TriplePlan>(
        Id(VarId(1)), oid(404), Id(VarId(2))));
    ASSERT_DOUBLE_EQ(missing.estimate_output_size(), 0);
    auto* sync = SyncPoint::GetInstance();
    std::atomic<int> injected{0};
    auto cleanup = ScopeExit([sync] {
      sync->DisableProcessing();
      sync->ClearAllCallBacks();
    });
    sync->SetCallBack("ReadRpc::NotifyBatcher", [&](void* arg) {
      auto* response = static_cast<tserver::LWReadResponsePB*>(arg);
      for (auto& ql : *response->mutable_ql_batch()) {
        ++injected;
        ql.set_status(QLResponsePB::YQL_STATUS_RUNTIME_ERROR);
        ql.dup_error_message("injected bound-input read failure");
      }
    });
    sync->EnableProcessing();
    auto iter = missing.get_binding_iter();
    Binding binding(3);
    iter->begin(binding);
    ASSERT_FALSE(iter->next());
    sync->DisableProcessing();
    ASSERT_GT(injected.load(), 0);
    ASSERT_FALSE(iter->status().ok());
    ASSERT_NE(
        iter->status().ToString().find("injected bound-input read failure"), std::string::npos);
  }
  ASSERT_OK(model.WriteTripleToDocDB(101, 20, 502));
  ASSERT_EQ(model.GetStatistics(), nullptr);
  ASSERT_FALSE(scan.estimate_index_join(input, &output, &cost));
  ASSERT_OK(model.Flush());
  ASSERT_OK(model.RefreshStatistics());
  IndexNestedLoopPlan rebuilt(input.clone(), std::make_unique<TriplePlan>(
      Id(VarId(1)), oid(20), Id(VarId(2))));
  ASSERT_DOUBLE_EQ(rebuilt.estimate_output_size(), 5);
  ASSERT_DOUBLE_EQ(rebuilt.clone()->estimate_output_size(), 5);
  model.ClearQueryCaches();
  auto iter = rebuilt.get_binding_iter();
  Binding binding(3);
  iter->begin(binding);
  std::vector<std::array<uint64_t, 3>> actual;
  while (iter->next())
    actual.push_back({binding[VarId(0)].id, binding[VarId(1)].id, binding[VarId(2)].id});
  ASSERT_OK(iter->status());
  std::sort(actual.begin(), actual.end());
  ASSERT_EQ(actual, (std::vector<std::array<uint64_t, 3>>{
      {1, 100, 500}, {1, 100, 501}, {2, 100, 500}, {2, 100, 501}, {3, 101, 502}}));
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

}  // namespace yb::mdb

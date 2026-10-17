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

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "query/executor/binding.h"
#include "query/optimizer/binding_iter_constructor.h"
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
#include "yb/util/status.h"
#include "yb/util/test_macros.h"

using namespace SPARQL;

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
    builder.AddColumn("c2")->Type(DataType::INT64)->NotNull();

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

TEST_F(MdbDistributedIntegrationTest, ScatterGatherFullScan) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  ASSERT_TRUE(RdfYBModel::Instance().IsConnected());

  ASSERT_OK(RdfYBModel::Instance().WriteTripleToDocDB(1, 10, 100));
  ASSERT_OK(RdfYBModel::Instance().WriteTripleToDocDB(2, 20, 200));
  ASSERT_OK(RdfYBModel::Instance().WriteTripleToDocDB(3, 30, 300));

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

  for (const auto& fr : fragment_results) {
    ASSERT_EQ(static_cast<int>(fr.bindings.size()), 3);
  }

  GatherOperator gather;
  GatherResult gather_result;
  ASSERT_OK(gather.Merge(fragment_results, plan.gather_strategy(), &gather_result));

  ASSERT_EQ(gather_result.total_input_rows, kNumTablets * 3);
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

  for (const auto& fr : fragment_results) {
    ASSERT_EQ(static_cast<int>(fr.bindings.size()), 10);
  }

  GatherOperator gather;
  GatherResult gather_result;
  ASSERT_OK(gather.Merge(fragment_results, plan.gather_strategy(), &gather_result));

  ASSERT_EQ(gather_result.total_input_rows, kNumTablets * 10);
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

  for (const auto& fr : fragment_results) {
    ASSERT_EQ(static_cast<int>(fr.bindings.size()), 1);
  }

  GatherOperator gather;
  GatherResult gather_result;
  ASSERT_OK(gather.Merge(fragment_results, plan.gather_strategy(), &gather_result));

  ASSERT_EQ(gather_result.total_input_rows, kNumTablets * 1);
  ASSERT_EQ(gather_result.total_output_rows, 1);
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

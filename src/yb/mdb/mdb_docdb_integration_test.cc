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

#include <set>
#include <string>

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
#include "yb/util/status.h"
#include "yb/util/test_macros.h"

using namespace SPARQL;

namespace yb::mdb {

namespace {

struct CompiledPipeline {
  std::unique_ptr<Op> op;
  std::unique_ptr<BindingIter> iter;

  explicit CompiledPipeline(const std::string& query) {
    try {
      QueryParser qp(query);
      op = qp.get_query_plan({});
      if (op == nullptr) return;

      BindingIterConstructor optimizer;
      op->accept_visitor(optimizer);
      iter = std::move(optimizer.tmp);
    } catch (const QueryParsingException&) {
    }
  }

  bool ok() const { return op != nullptr && iter != nullptr; }
};

}  // namespace

class MdbDocDBIntegrationTest : public MiniClusterTestWithClient<MiniCluster> {
 protected:
  static constexpr const char* kNamespace = "mdb_test";
  static constexpr int kNumTablets = 1;

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
};

TEST_F(MdbDocDBIntegrationTest, BasicTripleScanFromDocDB) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  ASSERT_TRUE(RdfYBModel::Instance().IsConnected());

  ASSERT_OK(RdfYBModel::Instance().WriteTripleToDocDB(1, 2, 3));

  ASSERT_OK(RdfYBModel::Instance().LoadAllFromDocDB());

  CompiledPipeline pipeline("SELECT * WHERE { ?s ?p ?o }");
  ASSERT_TRUE(pipeline.ok());

  Binding binding(get_query_ctx().get_var_size());
  pipeline.iter->begin(binding);

  VarId s_var = get_query_ctx().get_or_create_var("s");
  VarId p_var = get_query_ctx().get_or_create_var("p");
  VarId o_var = get_query_ctx().get_or_create_var("o");

  int row_count = 0;
  while (pipeline.iter->next()) {
    EXPECT_EQ(binding[s_var].id, 1u);
    EXPECT_EQ(binding[p_var].id, 2u);
    EXPECT_EQ(binding[o_var].id, 3u);
    row_count++;
  }
  EXPECT_EQ(row_count, 1);
}

TEST_F(MdbDocDBIntegrationTest, MultipleTriplesScanFromDocDB) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  ASSERT_TRUE(RdfYBModel::Instance().IsConnected());

  ASSERT_OK(RdfYBModel::Instance().WriteTripleToDocDB(10, 100, 20));
  ASSERT_OK(RdfYBModel::Instance().WriteTripleToDocDB(20, 200, 30));
  ASSERT_OK(RdfYBModel::Instance().WriteTripleToDocDB(30, 300, 40));

  ASSERT_OK(RdfYBModel::Instance().LoadAllFromDocDB());

  CompiledPipeline pipeline("SELECT * WHERE { ?s ?p ?o }");
  ASSERT_TRUE(pipeline.ok());

  Binding binding(get_query_ctx().get_var_size());
  pipeline.iter->begin(binding);

  VarId s_var = get_query_ctx().get_or_create_var("s");
  VarId p_var = get_query_ctx().get_or_create_var("p");
  VarId o_var = get_query_ctx().get_or_create_var("o");

  std::set<uint64_t> subjects;
  while (pipeline.iter->next()) {
    subjects.insert(binding[s_var].id);
    EXPECT_FALSE(binding[s_var].is_null());
    EXPECT_FALSE(binding[p_var].is_null());
    EXPECT_FALSE(binding[o_var].is_null());
  }

  EXPECT_EQ(subjects.size(), 3u);
  EXPECT_TRUE(subjects.count(10));
  EXPECT_TRUE(subjects.count(20));
  EXPECT_TRUE(subjects.count(30));
}

TEST_F(MdbDocDBIntegrationTest, ChainJoinFromDocDB) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  ASSERT_TRUE(RdfYBModel::Instance().IsConnected());

  ASSERT_OK(RdfYBModel::Instance().WriteTripleToDocDB(10, 100, 20));
  ASSERT_OK(RdfYBModel::Instance().WriteTripleToDocDB(20, 200, 30));
  ASSERT_OK(RdfYBModel::Instance().WriteTripleToDocDB(40, 400, 50));

  ASSERT_OK(RdfYBModel::Instance().LoadAllFromDocDB());

  CompiledPipeline pipeline("SELECT * WHERE { ?a ?x ?b . ?b ?y ?c }");
  ASSERT_TRUE(pipeline.ok());

  Binding binding(get_query_ctx().get_var_size());
  pipeline.iter->begin(binding);

  VarId b_var = get_query_ctx().get_or_create_var("b");
  VarId c_var = get_query_ctx().get_or_create_var("c");

  int row_count = 0;
  while (pipeline.iter->next()) {
    uint64_t b_val = binding[b_var].id;
    uint64_t c_val = binding[c_var].id;
    if (b_val == 20) {
      EXPECT_EQ(c_val, 30u);
    }
    row_count++;
  }
  EXPECT_EQ(row_count, 1);
}

}  // namespace yb::mdb

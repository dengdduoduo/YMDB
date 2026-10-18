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

#include "graph_models/rdf_model/rdf_model.h"
#include "query/exceptions.h"
#include "query/executor/binding.h"
#include "query/optimizer/binding_iter_constructor.h"
#include "query/parser/sparql_query_parser.h"
#include "query/query_context.h"
#include "yb/mdb/adapter/mdb_adapter.h"
#include "yb/mdb/model/rdf_yb_model.h"

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
};

}  // namespace

TEST(MdbPipelineE2ETest, T71_SelectAllTriples) {
  CompiledPipeline pipeline("SELECT ?s WHERE { ?s ?p ?o }");
  ASSERT_NE(pipeline.op, nullptr);
  ASSERT_NE(pipeline.iter, nullptr);
}

TEST(MdbPipelineE2ETest, T72_SelectFixedTriple) {
  CompiledPipeline pipeline("SELECT ?s WHERE { ?s <http://ex/p> <http://ex/o> }");
  ASSERT_NE(pipeline.op, nullptr);
  ASSERT_NE(pipeline.iter, nullptr);
}

TEST(MdbPipelineE2ETest, T73_SelectTwoVars) {
  CompiledPipeline pipeline("SELECT ?s ?o WHERE { ?s <http://ex/p> ?o }");
  ASSERT_NE(pipeline.op, nullptr);
  ASSERT_NE(pipeline.iter, nullptr);
}

TEST(MdbPipelineE2ETest, T74_DualTripleJoin) {
  CompiledPipeline pipeline(
      "SELECT ?s WHERE { "
      "  ?s <http://ex/p1> ?o . "
      "  ?s <http://ex/p2> ?o2 "
      "}");
  ASSERT_NE(pipeline.op, nullptr);
  ASSERT_NE(pipeline.iter, nullptr);
}

TEST(MdbPipelineE2ETest, T75_ChainJoin) {
  CompiledPipeline pipeline(
      "SELECT ?s ?o WHERE { "
      "  ?s <http://ex/p1> ?o . "
      "  ?o <http://ex/p2> ?x "
      "}");
  ASSERT_NE(pipeline.op, nullptr);
  ASSERT_NE(pipeline.iter, nullptr);
}

TEST(MdbPipelineE2ETest, T76_Optional) {
  CompiledPipeline pipeline(
      "SELECT ?s WHERE { "
      "  ?s <http://ex/p1> ?o . "
      "  OPTIONAL { ?s <http://ex/p2> ?o2 } "
      "}");
  ASSERT_NE(pipeline.op, nullptr);
  ASSERT_NE(pipeline.iter, nullptr);
}

TEST(MdbPipelineE2ETest, T77_Filter) {
  CompiledPipeline pipeline(
      "SELECT ?s WHERE { "
      "  ?s <http://ex/p> ?o . "
      "  FILTER(?o > 3000) "
      "}");
  ASSERT_NE(pipeline.op, nullptr);
  ASSERT_NE(pipeline.iter, nullptr);
}

TEST(MdbPipelineE2ETest, SelectStarUnitTable) {
  CompiledPipeline pipeline("SELECT * WHERE { }");
  ASSERT_NE(pipeline.op, nullptr);
  ASSERT_NE(pipeline.iter, nullptr);
}

TEST(MdbPipelineE2ETest, SelectWithUnion) {
  CompiledPipeline pipeline(
      "SELECT ?s WHERE { "
      "  { ?s <http://ex/p1> <http://ex/o1> } UNION "
      "  { ?s <http://ex/p2> <http://ex/o2> } "
      "}");
  ASSERT_NE(pipeline.op, nullptr);
  ASSERT_NE(pipeline.iter, nullptr);
}

TEST(MdbPipelineE2ETest, SelectWithOrderBy) {
  CompiledPipeline pipeline(
      "SELECT ?s WHERE { ?s <http://ex/p> ?o } ORDER BY ?o");
  ASSERT_NE(pipeline.op, nullptr);
  ASSERT_NE(pipeline.iter, nullptr);
}

TEST(MdbPipelineE2ETest, SelectWithLimit) {
  CompiledPipeline pipeline(
      "SELECT ?s WHERE { ?s <http://ex/p> ?o } LIMIT 10");
  ASSERT_NE(pipeline.op, nullptr);
  ASSERT_NE(pipeline.iter, nullptr);
}

TEST(MdbPipelineE2ETest, SelectWithDistinct) {
  CompiledPipeline pipeline(
      "SELECT DISTINCT ?s WHERE { ?s <http://ex/p> ?o }");
  ASSERT_NE(pipeline.op, nullptr);
  ASSERT_NE(pipeline.iter, nullptr);
}

TEST(MdbPipelineE2ETest, SelectWithGroupBy) {
  CompiledPipeline pipeline(
      "SELECT ?s (COUNT(?o) AS ?cnt) WHERE { ?s <http://ex/p> ?o } GROUP BY ?s");
  ASSERT_NE(pipeline.op, nullptr);
  ASSERT_NE(pipeline.iter, nullptr);
}

TEST(MdbPipelineE2ETest, SelectWithPrefix) {
  CompiledPipeline pipeline(
      "PREFIX ex: <http://example.org/> "
      "SELECT ?s WHERE { ?s ex:p ex:o }");
  ASSERT_NE(pipeline.op, nullptr);
  ASSERT_NE(pipeline.iter, nullptr);
}

TEST(MdbPipelineE2ETest, SelectWithValues) {
  CompiledPipeline pipeline(
      "SELECT ?s WHERE { VALUES ?s { <http://ex/a> <http://ex/b> } }");
  ASSERT_NE(pipeline.op, nullptr);
  ASSERT_NE(pipeline.iter, nullptr);
}

TEST(MdbPipelineE2ETest, SelectWithMinus) {
  CompiledPipeline pipeline(
      "SELECT ?s WHERE { "
      "  ?s <http://ex/p1> ?o . "
      "  MINUS { ?s <http://ex/p2> ?o2 } "
      "}");
  ASSERT_NE(pipeline.op, nullptr);
  ASSERT_NE(pipeline.iter, nullptr);
}

TEST(MdbPipelineE2ETest, SelectWithNotExists) {
  CompiledPipeline pipeline(
      "SELECT ?s WHERE { "
      "  ?s <http://ex/p1> ?o . "
      "  FILTER NOT EXISTS { ?s <http://ex/p2> ?o2 } "
      "}");
  ASSERT_NE(pipeline.op, nullptr);
  ASSERT_NE(pipeline.iter, nullptr);
}

TEST(MdbPipelineE2ETest, SelectWithBind) {
  CompiledPipeline pipeline(
      "SELECT ?s ?val WHERE { "
      "  ?s <http://ex/p> ?o . "
      "  BIND(?o + 1 AS ?val) "
      "}");
  ASSERT_NE(pipeline.op, nullptr);
  ASSERT_NE(pipeline.iter, nullptr);
}

TEST(MdbPipelineE2ETest, SyntaxErrorReturnsNonZero) {
  int rc = YBCMdbExecuteSparql("SELECT ?s WHERE { ?s ?p", nullptr, nullptr);
  EXPECT_NE(rc, 0);
}

TEST(MdbPipelineE2ETest, NullInputReturnsError) {
  ASSERT_EQ(YBCMdbExecuteSparql(nullptr, nullptr, nullptr), 1);
}

TEST(MdbPipelineE2ETest, NullCallbackReturnsError) {
  ASSERT_EQ(YBCMdbExecuteSparql(
      "SELECT ?s WHERE { ?s ?p ?o }", nullptr, nullptr), 1);
}

TEST(MdbPipelineE2ETest, DescribeQuery) {
  CompiledPipeline pipeline("DESCRIBE <http://ex/s>");
  ASSERT_NE(pipeline.op, nullptr);
  ASSERT_NE(pipeline.iter, nullptr);
}

TEST(MdbPipelineE2ETest, ConstructQuery) {
  CompiledPipeline pipeline(
      "CONSTRUCT { ?s <http://ex/p> ?o } WHERE { ?s <http://ex/p> ?o }");
  ASSERT_NE(pipeline.op, nullptr);
  ASSERT_NE(pipeline.iter, nullptr);
}

TEST(MdbPipelineE2ETest, AskQuery) {
  CompiledPipeline pipeline("ASK WHERE { ?s <http://ex/p> <http://ex/o> }");
  ASSERT_NE(pipeline.op, nullptr);
  ASSERT_NE(pipeline.iter, nullptr);
}

TEST(MdbPipelineE2ETest, MultiTripleBGP) {
  CompiledPipeline pipeline(
      "SELECT ?s ?p ?o WHERE { "
      "  ?s ?p ?o . "
      "  ?s <http://ex/a> ?x . "
      "  ?o <http://ex/b> ?y "
      "}");
  ASSERT_NE(pipeline.op, nullptr);
  ASSERT_NE(pipeline.iter, nullptr);
}

TEST(MdbPipelineE2ETest, ComplexNestedOptional) {
  CompiledPipeline pipeline(
      "SELECT ?s ?o WHERE { "
      "  ?s <http://ex/p1> ?o . "
      "  OPTIONAL { "
      "    ?s <http://ex/p2> ?o2 . "
      "    OPTIONAL { ?s <http://ex/p3> ?o3 } "
      "  } "
      "}");
  ASSERT_NE(pipeline.op, nullptr);
  ASSERT_NE(pipeline.iter, nullptr);
}

TEST(MdbPipelineE2ETest, SelectAllWithOffset) {
  CompiledPipeline pipeline(
      "SELECT ?s WHERE { ?s <http://ex/p> ?o } OFFSET 5 LIMIT 10");
  ASSERT_NE(pipeline.op, nullptr);
  ASSERT_NE(pipeline.iter, nullptr);
}

TEST(MdbPipelineE2ETest, SelectWithPathExpression) {
  CompiledPipeline pipeline(
      "SELECT ?s ?o WHERE { ?s <http://ex/p>+ ?o }");
  ASSERT_NE(pipeline.op, nullptr);
  ASSERT_NE(pipeline.iter, nullptr);
}

TEST(MdbPipelineE2ETest, EmptyBGP) {
  CompiledPipeline pipeline("SELECT ?s WHERE { }");
  ASSERT_NE(pipeline.op, nullptr);
}

TEST(MdbPipelineE2ETest, IntegrationBasicTripleScan) {
  RdfYBModel::Instance().ClearAll();
  RdfYBModel::Instance().InsertTriple(1, 2, 3);

  CompiledPipeline pipeline("SELECT * WHERE { ?s ?p ?o }");
  ASSERT_NE(pipeline.op, nullptr);
  ASSERT_NE(pipeline.iter, nullptr);

  Binding binding(get_query_ctx().get_var_size());
  ASSERT_GT(binding.size, 0u);

  pipeline.iter->begin(binding);

  int row_count = 0;
  VarId s_var = get_query_ctx().get_or_create_var("s");
  VarId p_var = get_query_ctx().get_or_create_var("p");
  VarId o_var = get_query_ctx().get_or_create_var("o");

  while (pipeline.iter->next()) {
    EXPECT_EQ(binding[s_var].id, 1u);
    EXPECT_EQ(binding[p_var].id, 2u);
    EXPECT_EQ(binding[o_var].id, 3u);
    row_count++;
  }
  EXPECT_EQ(row_count, 1);
}

TEST(MdbPipelineE2ETest, IntegrationMultipleTriples) {
  RdfYBModel::Instance().ClearAll();
  RdfYBModel::Instance().InsertTriple(10, 100, 20);
  RdfYBModel::Instance().InsertTriple(20, 200, 30);
  RdfYBModel::Instance().InsertTriple(30, 300, 40);

  CompiledPipeline pipeline("SELECT * WHERE { ?s ?p ?o }");
  ASSERT_NE(pipeline.op, nullptr);
  ASSERT_NE(pipeline.iter, nullptr);

  Binding binding(get_query_ctx().get_var_size());
  pipeline.iter->begin(binding);

  std::set<uint64_t> subjects;
  VarId s_var = get_query_ctx().get_or_create_var("s");
  VarId p_var = get_query_ctx().get_or_create_var("p");
  VarId o_var = get_query_ctx().get_or_create_var("o");

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

TEST(MdbPipelineE2ETest, IntegrationChainJoin) {
  RdfYBModel::Instance().ClearAll();
  RdfYBModel::Instance().InsertTriple(10, 100, 20);
  RdfYBModel::Instance().InsertTriple(20, 200, 30);
  RdfYBModel::Instance().InsertTriple(40, 400, 50);

  CompiledPipeline pipeline("SELECT * WHERE { ?a ?x ?b . ?b ?y ?c }");
  ASSERT_NE(pipeline.op, nullptr);
  ASSERT_NE(pipeline.iter, nullptr);

  Binding binding(get_query_ctx().get_var_size());
  pipeline.iter->begin(binding);

  std::set<uint64_t> a_vals;
  VarId a_var = get_query_ctx().get_or_create_var("a");
  VarId b_var = get_query_ctx().get_or_create_var("b");
  VarId c_var = get_query_ctx().get_or_create_var("c");

  while (pipeline.iter->next()) {
    a_vals.insert(binding[a_var].id);
    uint64_t b = binding[b_var].id;
    uint64_t c = binding[c_var].id;
    if (b == 20) {
      EXPECT_EQ(c, 30u);
    }
    EXPECT_FALSE(binding[a_var].is_null());
  }

  EXPECT_EQ(a_vals.size(), 1u);
  EXPECT_TRUE(a_vals.count(10));
}

}  // namespace yb::mdb

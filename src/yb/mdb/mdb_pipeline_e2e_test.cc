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

#include <iomanip>
#include <future>
#include <memory>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "graph_models/rdf_model/rdf_model.h"
#include "graph_models/rdf_model/conversions.h"
#include "query/exceptions.h"
#include "query/executor/binding.h"
#include "query/optimizer/binding_iter_constructor.h"
#include "query/parser/sparql_query_parser.h"
#include "query/query_context.h"
#include "yb/mdb/adapter/mdb_adapter.h"
#include "yb/mdb/model/rdf_yb_model.h"
#include "yb/mdb/import/rdf_term.h"

using namespace SPARQL;

namespace yb::mdb {

TEST(MdbRdfIdentityTest, LanguageAndDatatypeArePartOfIdentity) {
  for (const std::string text : {"hi", "a long literal with | separators"}) {
    auto en = Conversions::pack_string_lang("EN-us", text);
    EXPECT_EQ(en, Conversions::pack_string_lang("en-US", text));
    EXPECT_NE(en, Conversions::pack_string_lang("fr", text));
    EXPECT_EQ(Conversions::unpack_string_lang(en), (std::make_pair(std::string("en-us"), text)));
    auto first = Conversions::try_pack_string_datatype("urn:type:first", text);
    EXPECT_NE(first, Conversions::try_pack_string_datatype("urn:type:second", text));
    EXPECT_NE(first, Conversions::pack_string_simple(text));
    EXPECT_EQ(Conversions::unpack_string_datatype(first),
              (std::make_pair(std::string("urn:type:first"), text)));
    EXPECT_EQ(Conversions::pack_string_simple(text), Conversions::pack_string_xsd(text));
  }
}

TEST(MdbRdfIdentityTest, NumericIdentityPreservesLexicalFormsAndValues) {
  const std::string xsd = RdfTermIdentity::kXsd;
  auto one = Conversions::try_pack_string_datatype(xsd + "integer", "1");
  auto zero_one = Conversions::try_pack_string_datatype(xsd + "integer", "01");
  EXPECT_NE(one, zero_one);
  EXPECT_EQ(Conversions::unpack_int(one), Conversions::unpack_int(zero_one));
  EXPECT_EQ(Conversions::to_lexical_str(zero_one), "01");
  EXPECT_EQ(one, Conversions::pack_int(1));
  EXPECT_EQ(Conversions::unpack_int(Conversions::pack_int(-123)), -123);
  EXPECT_EQ(Conversions::unpack_int(Conversions::pack_int(INT64_MAX)), INT64_MAX);
  EXPECT_DOUBLE_EQ(Conversions::unpack_double(Conversions::pack_double(-123.25)), -123.25);
  EXPECT_FLOAT_EQ(Conversions::unpack_float(Conversions::pack_float(-1.5f)), -1.5f);
  auto decimal = Conversions::try_pack_string_datatype(xsd + "decimal", "1.00");
  EXPECT_EQ(Conversions::to_lexical_str(decimal), "1.00");
  EXPECT_DOUBLE_EQ(Conversions::unpack_decimal(decimal).to_double(), 1);
  EXPECT_TRUE(Conversions::try_pack_string_datatype(xsd + "boolean", "1").is_true());
  EXPECT_NE(Conversions::pack_bool(true), Conversions::try_pack_string_datatype(xsd + "boolean", "1"));
  EXPECT_EQ(RDF_OID::get_generic_type(Conversions::try_pack_string_datatype(xsd + "integer", "bad")),
            RDF_OID::GenericType::STRING);
}

TEST(MdbRdfIdentityTest, VersionedKeysAvoidDelimiterAndKindCollisions) {
  const auto plain = RdfIdentity(TripleTermType::LITERAL, "a|b", "urn:c");
  const auto other = RdfIdentity(TripleTermType::LITERAL, "a", "urn:b|c");
  EXPECT_NE(plain.Key(), other.Key());
  EXPECT_NE(RdfDictionaryKey(plain), RdfDictionaryKey(RdfIdentity(TripleTermType::URI, "a|b")));
  auto packed = PackRdfIdentity(plain);
  auto decoded = DecodeRdfDictionaryEntry(plain.Key(), packed);
  EXPECT_EQ(decoded.Key(), plain.Key());
  EXPECT_EQ(PackRdfIdentity(decoded), packed);
  EXPECT_THROW(DecodeRdfDictionaryEntry("old literal|en", packed), std::runtime_error);
  EXPECT_THROW(RdfTermIdentity::Decode(std::string("\0MDB1L999:", 10)), std::runtime_error);
  auto first = RdfIdentity(TripleTermType::BLANK, "_:b", "", "import-1");
  auto second = RdfIdentity(TripleTermType::BLANK, "_:b", "", "import-2");
  EXPECT_NE(PackRdfIdentity(first), PackRdfIdentity(second));
  EXPECT_EQ(RDF_OID::get_generic_type(PackRdfIdentity(first)), RDF_OID::GenericType::BLANK);
}

TEST(MdbRdfIdentityTest, ExpressionsKeepIdentitySeparateFromNumericEquality) {
  RdfYBModel::Instance().ClearAll();
  std::multiset<std::string> values;
  auto collect = [](const char*, const char* value, void* arg) {
    static_cast<std::multiset<std::string>*>(arg)->insert(value);
  };
  EXPECT_EQ(YBCMdbExecuteSparql(
      "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> SELECT ?v WHERE { VALUES ?v { \"ok\" } "
      "FILTER(!sameTerm(\"x\"@en, \"x\"@fr) && sameTerm(\"x\"@EN, \"x\"@en) && "
      "sameTerm(\"x\", \"x\"^^xsd:string) && !sameTerm(\"01\"^^xsd:integer, 1) && "
      "\"01\"^^xsd:integer = 1 && \"1\"^^xsd:boolean = true && "
      "LANG(\"x\"@EN) = \"en\" && DATATYPE(\"x\"^^<urn:custom>) = <urn:custom>) }",
      collect, &values), 0);
  EXPECT_EQ(values, (std::multiset<std::string>{"ok"}));
  values.clear();
  EXPECT_EQ(YBCMdbExecuteSparql(
      "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> SELECT ?v WHERE { "
      "VALUES ?v { \"ok\" } FILTER(\"1\"^^xsd:boolean && !\"0\"^^xsd:boolean && "
      "xsd:integer(\"1\"^^xsd:boolean) = 1) }", collect, &values), 0);
  EXPECT_EQ(values, (std::multiset<std::string>{"ok"}));
  values.clear();
  EXPECT_EQ(YBCMdbExecuteSparql(
      "SELECT ?v WHERE { VALUES ?v { \"\" } }", collect, &values), 0);
  EXPECT_EQ(values, (std::multiset<std::string>{""}));
}

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

class MdbOrderByRegressionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    saved_uris_ = std::move(g_gmap_to_uri);
    saved_packed_ = std::move(g_gmap_to_pack);
    saved_graph_ = std::move(g_pack_to_gmap);
    g_gmap_to_uri.clear();
    g_gmap_to_pack.clear();
    g_pack_to_gmap.clear();
    RdfYBModel::Instance().ClearAll();
  }

  void TearDown() override {
    RdfYBModel::Instance().ClearAll();
    g_gmap_to_uri = std::move(saved_uris_);
    g_gmap_to_pack = std::move(saved_packed_);
    g_pack_to_gmap = std::move(saved_graph_);
  }

  static std::string Subject(int i) {
    std::ostringstream text;
    text << "http://order.example/v" << std::setfill('0') << std::setw(6) << i;
    return text.str();
  }

  static ::ObjectId AddIri(uint64_t graph_id, const std::string& text) {
    auto packed = Conversions::pack_iri(text);
    g_gmap_to_uri[graph_id] = text;
    g_gmap_to_pack[graph_id] = packed;
    g_pack_to_gmap[packed.id] = graph_id;
    return packed;
  }

  void SeedSubjects() {
    AddIri(10, "http://order.example/p");
    AddIri(50, "http://order.example/g0");
    AddIri(51, "http://order.example/g1");
    for (int i = 19; i >= 0; --i) {
      const uint64_t graph_id = 1000 - i;
      AddIri(graph_id, Subject(i));
      RdfYBModel::Instance().InsertTriple(graph_id, 10, 50 + i % 2);
    }
  }

  std::vector<std::vector<uint64_t>> ReadRows(
      const std::string& query, const std::vector<std::string>& columns = {"s"}) {
    CompiledPipeline pipeline(query);
    EXPECT_NE(pipeline.iter, nullptr);
    if (!pipeline.iter) return {};
    std::vector<VarId> vars;
    for (const auto& name : columns) vars.push_back(get_query_ctx().get_or_create_var(name));
    Binding binding(get_query_ctx().get_var_size());
    pipeline.iter->begin(binding);
    std::vector<std::vector<uint64_t>> rows;
    while (pipeline.iter->next()) {
      std::vector<uint64_t> row;
      for (auto var : vars) row.push_back(binding[var].id);
      rows.push_back(std::move(row));
    }
    return rows;
  }

  std::vector<std::vector<uint64_t>> ExpectedSubjects(const std::vector<int>& indices) {
    std::vector<std::vector<uint64_t>> rows;
    for (int i : indices) rows.push_back({Conversions::pack_iri(Subject(i)).id});
    return rows;
  }

 private:
  std::unordered_map<uint64_t, std::string> saved_uris_;
  std::unordered_map<uint64_t, ::ObjectId> saved_packed_;
  std::unordered_map<uint64_t, uint64_t> saved_graph_;
};

TEST_F(MdbOrderByRegressionTest, AscendingIriLimitUsesTextRatherThanIds) {
  SeedSubjects();
  EXPECT_EQ(ReadRows("SELECT ?s WHERE { ?s ?p ?o } ORDER BY ?s LIMIT 10"),
            ExpectedSubjects({0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
}

TEST_F(MdbOrderByRegressionTest, DescendingIriLimitUsesTextRatherThanIds) {
  SeedSubjects();
  EXPECT_EQ(ReadRows("SELECT ?s WHERE { ?s ?p ?o } ORDER BY DESC(?s) LIMIT 5"),
            ExpectedSubjects({19, 18, 17, 16, 15}));
}

TEST_F(MdbOrderByRegressionTest, OffsetSelectsTheLexicalPage) {
  SeedSubjects();
  EXPECT_EQ(ReadRows("SELECT ?s WHERE { ?s ?p ?o } ORDER BY ?s LIMIT 4 OFFSET 5"),
            ExpectedSubjects({5, 6, 7, 8}));
}

TEST_F(MdbOrderByRegressionTest, MultipleKeysRespectEachDirection) {
  SeedSubjects();
  EXPECT_EQ(ReadRows("SELECT ?s WHERE { ?s ?p ?o } ORDER BY ?o DESC(?s) LIMIT 10"),
            ExpectedSubjects({18, 16, 14, 12, 10, 8, 6, 4, 2, 0}));
}

TEST_F(MdbOrderByRegressionTest, ParsedIriConstantsSortLexically) {
  const std::string base = "http://order.example/";
  EXPECT_EQ(ReadRows("SELECT ?s WHERE { VALUES ?s { <http://order.example/z> "
                     "<http://order.example/a> <http://order.example/m> } } ORDER BY ?s"),
            (std::vector<std::vector<uint64_t>>{{Conversions::pack_iri(base + "a").id},
                                               {Conversions::pack_iri(base + "m").id},
                                               {Conversions::pack_iri(base + "z").id}}));
}

TEST_F(MdbOrderByRegressionTest, UnboundPrecedesIriAndLiteral) {
  EXPECT_EQ(ReadRows("SELECT ?s WHERE { VALUES ?s { \"text\" "
                     "<http://order.example/a> UNDEF } } ORDER BY ?s"),
            (std::vector<std::vector<uint64_t>>{{::ObjectId::NULL_ID},
                {Conversions::pack_iri("http://order.example/a").id},
                {Conversions::pack_string_simple("text").id}}));
}

TEST_F(MdbOrderByRegressionTest, RestoredPackedIriUsesItsGraphDictionaryEntry) {
  const ::ObjectId packed(::ObjectId::MASK_IRI_INLINED | 1);
  g_pack_to_gmap[packed.id] = 71;
  g_gmap_to_uri[71] = "http://order.example/z";
  g_gmap_to_uri[1] = "http://order.example/a";
  EXPECT_EQ(Conversions::unpack_iri(packed), "http://order.example/z");
  EXPECT_EQ(Conversions::unpack_iri(Conversions::pack_iri("http://order.example/constant")),
            "http://order.example/constant");
}

TEST_F(MdbOrderByRegressionTest, DuplicateSortKeysPreserveRows) {
  SeedSubjects();
  RdfYBModel::Instance().InsertTriple(1000, 11, 50);
  auto expected = ExpectedSubjects({0, 0, 1, 2});
  EXPECT_EQ(ReadRows("SELECT ?s WHERE { ?s ?p ?o } ORDER BY ?s LIMIT 4"), expected);
}

TEST_F(MdbOrderByRegressionTest, MissingIriTextIsReportedInsteadOfSortingIds) {
  g_gmap_to_pack[1] = ::ObjectId(::ObjectId::MASK_IRI_EXTERN | 901011);
  g_gmap_to_pack[2] = ::ObjectId(::ObjectId::MASK_IRI_EXTERN | 901012);
  RdfYBModel::Instance().InsertTriple(1, 10, 50);
  RdfYBModel::Instance().InsertTriple(2, 10, 50);
  EXPECT_THROW(ReadRows("SELECT ?s WHERE { ?s ?p ?o } ORDER BY ?s"), QueryException);
}

TEST(MdbQueryContextTest, NestedScopesRestoreOwnerAfterException) {
  auto* legacy = &get_query_ctx();
  QueryContext outer;
  QueryContext inner;
  {
    QueryContextScope outer_scope(outer);
    EXPECT_EQ(get_query_ctx().get_or_create_var("outer").id, 0);
    try {
      QueryContextScope inner_scope(inner);
      EXPECT_EQ(get_query_ctx().get_or_create_var("inner").id, 0);
      throw std::runtime_error("stop inner query");
    } catch (const std::runtime_error&) {
    }
    EXPECT_EQ(&get_query_ctx(), &outer);
    EXPECT_EQ(outer.get_var_size(), 1);
    EXPECT_EQ(inner.get_var_size(), 1);
  }
  EXPECT_EQ(&get_query_ctx(), legacy);
}

TEST(MdbQueryContextTest, ExplicitOwnerSurvivesThreadHandoff) {
  QueryContext context;
  {
    QueryContextScope scope(context);
    EXPECT_EQ(get_query_ctx().get_or_create_var("first").id, 0);
  }
  auto next = std::async(std::launch::async, [&context] {
    QueryContextScope scope(context);
    return get_query_ctx().get_or_create_var("second").id;
  });
  EXPECT_EQ(next.get(), 1);
  QueryContextScope resumed(context);
  EXPECT_EQ(get_query_ctx().get_or_create_var("first").id, 0);
  EXPECT_EQ(get_query_ctx().get_var_size(), 2);
}

TEST(MdbQueryContextTest, ConcurrentOwnersDoNotShareVariables) {
  QueryContext left;
  QueryContext right;
  std::promise<void> ready;
  std::promise<void> release;
  auto released = release.get_future();
  auto worker = std::async(std::launch::async, [&] {
    QueryContextScope scope(left);
    get_query_ctx().get_or_create_var("left");
    ready.set_value();
    released.wait();
    return get_query_ctx().get_var_size();
  });
  ready.get_future().wait();
  {
    QueryContextScope scope(right);
    EXPECT_EQ(get_query_ctx().get_or_create_var("right").id, 0);
    get_query_ctx().get_or_create_var("another");
  }
  release.set_value();
  EXPECT_EQ(worker.get(), 1);
  EXPECT_EQ(right.get_var_size(), 2);
}

TEST(MdbQueryContextTest, PublicEntryUsesFreshVariablesAndRestoresCaller) {
  QueryContext caller;
  QueryContextScope caller_scope(caller);
  caller.get_or_create_var("unrelated");
  caller.get_or_create_var("prior");
  struct Capture {
    std::vector<std::string> names;
    std::vector<size_t> variable_counts;
  };
  auto callback = [](const char* name, const char*, void* arg) {
    auto& capture = *static_cast<Capture*>(arg);
    capture.names.emplace_back(name);
    capture.variable_counts.push_back(get_query_ctx().get_var_size());
  };
  for (const auto* query : {"SELECT ?a WHERE { VALUES ?a { 1 } }",
                            "SELECT ?b WHERE { VALUES ?b { 2 } }"}) {
    Capture result;
    ASSERT_EQ(YBCMdbExecuteSparql(query, callback, &result), 0);
    EXPECT_EQ(result.names, (std::vector<std::string>{"v0"}));
    EXPECT_EQ(result.variable_counts, (std::vector<size_t>{1}));
    EXPECT_EQ(&get_query_ctx(), &caller);
    EXPECT_EQ(caller.get_var_size(), 2);
  }
}

TEST(MdbQueryContextTest, PublicEntryRejectsReentryAndRecoversFromCallbackFailure) {
  int nested_status = 0;
  auto nested = [](const char*, const char*, void* arg) {
    auto noop = [](const char*, const char*, void*) {};
    *static_cast<int*>(arg) = YBCMdbExecuteSparql(
        "SELECT ?inner WHERE { VALUES ?inner { 2 } }", noop, nullptr);
  };
  ASSERT_EQ(YBCMdbExecuteSparql(
      "SELECT ?outer WHERE { VALUES ?outer { 1 } }", nested, &nested_status), 0);
  EXPECT_EQ(nested_status, 1);
  QueryContext caller;
  QueryContextScope scope(caller);
  auto throwing = [](const char*, const char*, void*) { throw std::runtime_error("callback failed"); };
  EXPECT_EQ(YBCMdbExecuteSparql(
      "SELECT ?a WHERE { VALUES ?a { 1 } }", throwing, nullptr), 1);
  EXPECT_EQ(&get_query_ctx(), &caller);
  int count = 0;
  auto counting = [](const char*, const char*, void* arg) { ++*static_cast<int*>(arg); };
  EXPECT_EQ(YBCMdbExecuteSparql(
      "SELECT ?b WHERE { VALUES ?b { 2 } }", counting, &count), 0);
  EXPECT_EQ(count, 1);
}

TEST(MdbQueryContextTest, PublicEntryHonorsProjectionAndColumnOrder) {
  std::vector<std::string> names;
  auto callback = [](const char* name, const char*, void* arg) {
    static_cast<std::vector<std::string>*>(arg)->emplace_back(name);
  };
  ASSERT_EQ(YBCMdbExecuteSparql(
      "SELECT ?b ?a WHERE { VALUES (?a ?b ?hidden) { (1 2 3) (4 5 6) } }",
      callback, &names), 0);
  EXPECT_EQ(names, (std::vector<std::string>{"v1", "v0", "v1", "v0"}));
}

}  // namespace yb::mdb

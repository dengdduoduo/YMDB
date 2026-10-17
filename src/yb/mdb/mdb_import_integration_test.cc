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

#include <fstream>
#include <future>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <utility>

#include "query/executor/binding.h"
#include "query/optimizer/binding_iter_constructor.h"
#include "query/parser/sparql_query_parser.h"
#include "query/query_context.h"

#include "yb/client/schema.h"
#include "yb/client/session.h"
#include "yb/client/table_handle.h"
#include "yb/client/yb_op.h"
#include "yb/client/yb_table_name.h"
#include "yb/common/common.pb.h"
#include "yb/common/ql_protocol_util.h"
#include "yb/integration-tests/mini_cluster.h"
#include "yb/integration-tests/yb_mini_cluster_test_base.h"
#include "yb/mdb/import/csv_parser.h"
#include "yb/mdb/import/import_pipeline.h"
#include "yb/mdb/import/nt_parser.h"
#include "yb/mdb/import/persistent_id_map.h"
#include "yb/mdb/import/rdf_xml_parser.h"
#include "yb/mdb/model/rdf_yb_model.h"
#include "yb/mdb/storage/yb_client_compat.h"
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

class MdbImportIntegrationTest : public MiniClusterTestWithClient<MiniCluster> {
 protected:
  static constexpr const char* kNamespace = "mdb_import_test";
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
    builder.AddColumn("c2")->Type(DataType::INT64)->PrimaryKey()->NotNull();

    client::TableHandle tmp;
    return tmp.Create(table_name, kNumTablets, client_.get(), &builder);
  }

  void InitModel() {
    ASSERT_OK(RdfYBModel::Instance().Init(client_.get(), kNamespace));
  }

  void LoadFromDocDB() {
    ASSERT_OK(RdfYBModel::Instance().LoadAllFromDocDB());
  }
};

TEST_F(MdbImportIntegrationTest, PersistentTermIdsSurviveNewDictionaryInstance) {
  ASSERT_OK(client_->CreateNamespaceIfNotExists(kNamespace, YQL_DATABASE_CQL));
  uint64_t first_id;
  {
    PersistentIdMap dictionary;
    ASSERT_OK(dictionary.Init(client_.get(), kNamespace));
    first_id = ASSERT_RESULT(dictionary.GetOrCreate("http://ex/first"));
  }
  PersistentIdMap restarted;
  ASSERT_OK(restarted.Init(client_.get(), kNamespace));
  ASSERT_EQ(ASSERT_RESULT(restarted.GetOrCreate("http://ex/first")), first_id);
  ASSERT_GT(ASSERT_RESULT(restarted.GetOrCreate("http://ex/second")), first_id);
  auto empty_id = ASSERT_RESULT(restarted.GetOrCreate(""));
  ASSERT_EQ(ASSERT_RESULT(restarted.GetOrCreate("")), empty_id);
}

TEST_F(MdbImportIntegrationTest, ConcurrentDictionaryClientsClaimUniqueIds) {
  ASSERT_OK(client_->CreateNamespaceIfNotExists(kNamespace, YQL_DATABASE_CQL));
  constexpr int kWorkers = 8;
  std::vector<std::unique_ptr<PersistentIdMap>> dictionaries;
  for (int i = 0; i < kWorkers; ++i) {
    auto dictionary = std::make_unique<PersistentIdMap>();
    ASSERT_OK(dictionary->Init(client_.get(), kNamespace));
    dictionaries.push_back(std::move(dictionary));
  }
  std::promise<void> start;
  auto ready = start.get_future().share();
  std::vector<std::future<Result<uint64_t>>> shared_claims;
  for (auto& dictionary : dictionaries) {
    auto* ptr = dictionary.get();
    shared_claims.push_back(std::async(std::launch::async, [ptr, ready] {
      ready.wait();
      return ptr->GetOrCreate("http://ex/shared");
    }));
  }
  start.set_value();
  std::set<uint64_t> shared_ids;
  for (auto& claim : shared_claims) shared_ids.insert(ASSERT_RESULT(claim.get()));
  ASSERT_EQ(shared_ids.size(), 1);

  std::vector<std::future<Result<uint64_t>>> unique_claims;
  for (int i = 0; i < kWorkers; ++i) {
    auto* ptr = dictionaries[i].get();
    unique_claims.push_back(std::async(std::launch::async, [ptr, i] {
      return ptr->GetOrCreate("http://ex/unique/" + std::to_string(i));
    }));
  }
  std::set<uint64_t> unique_ids = shared_ids;
  for (auto& claim : unique_claims) {
    ASSERT_TRUE(unique_ids.insert(ASSERT_RESULT(claim.get())).second);
  }
  ASSERT_EQ(unique_ids.size(), kWorkers + 1);
}

TEST_F(MdbImportIntegrationTest, ModelUsesPersistedIdsWithFreshLocalMap) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  IdMap first;
  auto& model = RdfYBModel::Instance();
  auto first_id = ASSERT_RESULT(model.ResolveTerm(first, "http://ex/first"));
  auto second_id = ASSERT_RESULT(model.ResolveTerm(first, "http://ex/second"));
  model.ClearAll();
  ASSERT_NO_FATALS(InitModel());
  IdMap restarted;
  ASSERT_EQ(ASSERT_RESULT(model.ResolveTerm(restarted, "http://ex/second")), second_id);
  ASSERT_EQ(ASSERT_RESULT(model.ResolveTerm(restarted, "http://ex/first")), first_id);
  auto new_id = ASSERT_RESULT(model.ResolveTerm(restarted, "http://ex/new"));
  ASSERT_GT(new_id, second_id);
  ASSERT_EQ(restarted.Lookup(first_id), "http://ex/first");
}

TEST_F(MdbImportIntegrationTest, MigrateLegacyDictionaryWithoutRenumbering) {
  ASSERT_OK(client_->CreateNamespaceIfNotExists(kNamespace, YQL_DATABASE_CQL));
  client::YBSchemaBuilder builder;
  builder.AddColumn("pack_id")->Type(DataType::INT64)->HashPrimaryKey()->NotNull();
  builder.AddColumn("gmap_id")->Type(DataType::INT64)->NotNull();
  builder.AddColumn("uri")->Type(DataType::STRING)->NotNull();
  client::TableHandle legacy;
  ASSERT_OK(legacy.Create(
      client::YBTableName(YQL_DATABASE_CQL, kNamespace, "rdf_idmap"),
      1, client_.get(), &builder));
  auto session = client_->NewSession(MonoDelta::FromSeconds(30));
  auto op = NewMdbInsertOp(legacy, session);
  QLAddInt64HashValue(op->mutable_request(), 123);
  legacy.AddInt64ColumnValue(op->mutable_request(), "gmap_id", 1000);
  legacy.AddStringColumnValue(op->mutable_request(), "uri", "http://ex/legacy");
  ASSERT_OK(session->TEST_ApplyAndFlush(op));
  PersistentIdMap dictionary;
  ASSERT_OK(dictionary.Init(client_.get(), kNamespace));
  ASSERT_EQ(ASSERT_RESULT(dictionary.GetOrCreate("http://ex/legacy")), 1000);
  ASSERT_GT(ASSERT_RESULT(dictionary.GetOrCreate("http://ex/new")), 1000);
  ASSERT_OK(dictionary.Init(client_.get(), kNamespace));
  ASSERT_EQ(ASSERT_RESULT(dictionary.GetOrCreate("http://ex/legacy")), 1000);
}

TEST_F(MdbImportIntegrationTest, ImportNtFileAndQuery) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  ASSERT_TRUE(RdfYBModel::Instance().IsConnected());

  std::vector<Triple> triples;
  const char* nt_data = R"(
<http://example.org/s1> <http://example.org/name> "Alice" .
<http://example.org/s2> <http://example.org/name> "Bob" .
<http://example.org/s3> <http://example.org/name> "Charlie" .
<http://example.org/s1> <http://example.org/knows> <http://example.org/s2> .
<http://example.org/s2> <http://example.org/knows> <http://example.org/s3> .
<http://example.org/s3> <http://example.org/knows> <http://example.org/s1> .
)";

  std::istringstream ss(nt_data);
  std::string line;
  while (std::getline(ss, line)) {
    if (line.empty()) continue;
    auto r = NtParser::ParseLine(line);
    if (r.ok()) triples.push_back(std::move(*r));
  }

  ImportPipeline pipeline;
  auto istats = pipeline.ImportTriples(triples);
  ASSERT_TRUE(istats.ok());
  ASSERT_EQ(istats->triples_written, 6);

  ASSERT_NO_FATALS(LoadFromDocDB());

  CompiledPipeline cp("SELECT * WHERE { ?s ?p ?o }");
  ASSERT_TRUE(cp.ok());

  Binding binding(get_query_ctx().get_var_size());
  cp.iter->begin(binding);

  int row_count = 0;
  while (cp.iter->next()) {
    row_count++;
  }
  ASSERT_EQ(row_count, 6);
}

TEST_F(MdbImportIntegrationTest, ImportCsvFileAndQuery) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  ASSERT_TRUE(RdfYBModel::Instance().IsConnected());

  const char* csv_data = R"(
http://example.org/s1,http://example.org/name,"Alice"
http://example.org/s1,http://example.org/age,30
http://example.org/s2,http://example.org/name,"Bob"
http://example.org/s2,http://example.org/age,25
http://example.org/s3,http://example.org/name,"Charlie"
http://example.org/s1,http://example.org/knows,http://example.org/s2
http://example.org/s2,http://example.org/knows,http://example.org/s3
http://example.org/s3,http://example.org/knows,http://example.org/s1
)";

  CsvParseConfig config;
  config.has_header = false;
  std::vector<Triple> triples;
  std::istringstream ss(csv_data);
  std::string line;
  while (std::getline(ss, line)) {
    if (line.empty()) continue;
    auto r = CsvParser::ParseLine(line, config);
    if (r.ok()) triples.push_back(std::move(*r));
  }

  ImportPipeline pipeline;
  auto stats = pipeline.ImportTriples(triples);
  ASSERT_TRUE(stats.ok());
  ASSERT_EQ(stats->triples_written, 8);

  ASSERT_NO_FATALS(LoadFromDocDB());

  CompiledPipeline cp("SELECT * WHERE { ?s ?p ?o }");
  ASSERT_TRUE(cp.ok());

  Binding binding(get_query_ctx().get_var_size());
  cp.iter->begin(binding);

  int row_count = 0;
  while (cp.iter->next()) {
    row_count++;
  }
  ASSERT_EQ(row_count, 8);
}

TEST_F(MdbImportIntegrationTest, ImportRdfXmlFileAndQuery) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  ASSERT_TRUE(RdfYBModel::Instance().IsConnected());

  const char* rdf_xml = R"(
<?xml version="1.0"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
         xmlns:ex="http://example.org/">
  <rdf:Description rdf:about="http://example.org/s1">
    <ex:name>Alice</ex:name>
    <ex:knows rdf:resource="http://example.org/s2"/>
  </rdf:Description>
  <rdf:Description rdf:about="http://example.org/s2">
    <ex:name>Bob</ex:name>
    <ex:knows rdf:resource="http://example.org/s3"/>
  </rdf:Description>
  <rdf:Description rdf:about="http://example.org/s3">
    <ex:name>Charlie</ex:name>
    <ex:knows rdf:resource="http://example.org/s1"/>
  </rdf:Description>
</rdf:RDF>
)";

  auto parse_result = RdfXmlParser::ParseString(rdf_xml);
  ASSERT_TRUE(parse_result.ok());

  ImportPipeline pipeline;
  auto stats = pipeline.ImportTriples(*parse_result);
  ASSERT_TRUE(stats.ok());
  ASSERT_EQ(stats->triples_written, 6);

  ASSERT_NO_FATALS(LoadFromDocDB());

  CompiledPipeline cp("SELECT * WHERE { ?s ?p ?o }");
  ASSERT_TRUE(cp.ok());

  Binding binding(get_query_ctx().get_var_size());
  cp.iter->begin(binding);

  int row_count = 0;
  while (cp.iter->next()) {
    row_count++;
  }
  ASSERT_EQ(row_count, 6);
}

TEST_F(MdbImportIntegrationTest, ImportTriplesDirectly) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  ASSERT_TRUE(RdfYBModel::Instance().IsConnected());

  std::vector<Triple> triples;
  Triple t;
  t.sub = "http://example.org/x";
  t.pred = "http://example.org/p";
  t.obj = "http://example.org/y";
  t.sub_type = TripleTermType::URI;
  t.pred_type = TripleTermType::URI;
  t.obj_type = TripleTermType::URI;
  triples.push_back(t);

  t.sub = "http://example.org/y";
  t.pred = "http://example.org/q";
  t.obj = "value";
  t.sub_type = TripleTermType::URI;
  t.pred_type = TripleTermType::URI;
  t.obj_type = TripleTermType::LITERAL;
  triples.push_back(t);

  ImportPipeline pipeline;
  auto stats = pipeline.ImportTriples(triples);
  ASSERT_TRUE(stats.ok());
  ASSERT_EQ(stats->triples_written, 2);

  ASSERT_NO_FATALS(LoadFromDocDB());

  CompiledPipeline cp("SELECT * WHERE { ?s ?p ?o }");
  ASSERT_TRUE(cp.ok());

  Binding binding(get_query_ctx().get_var_size());
  cp.iter->begin(binding);

  int row_count = 0;
  while (cp.iter->next()) {
    row_count++;
  }
  ASSERT_EQ(row_count, 2);
}

TEST_F(MdbImportIntegrationTest, ImportFilesAreVisibleWhenImportReturns) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());

  const std::pair<const char*, const char*> files[] = {
      {"visible.nt",
       "<http://example.org/nt1> <http://example.org/p> <http://example.org/o> .\n"
       "<http://example.org/nt2> <http://example.org/p> <http://example.org/o> .\n"},
      {"visible.csv",
       "subject,predicate,object\n"
       "http://example.org/csv1,http://example.org/p,http://example.org/o\n"
       "http://example.org/csv2,http://example.org/p,http://example.org/o\n"},
      {"visible.rdf", R"(<?xml version="1.0"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
         xmlns:ex="http://example.org/">
  <rdf:Description rdf:about="http://example.org/xml1">
    <ex:p rdf:resource="http://example.org/o"/>
  </rdf:Description>
  <rdf:Description rdf:about="http://example.org/xml2">
    <ex:p rdf:resource="http://example.org/o"/>
  </rdf:Description>
</rdf:RDF>)"}};

  ImportPipeline pipeline;
  size_t expected_rows = 0;
  for (const auto& [name, data] : files) {
    SCOPED_TRACE(name);
    auto path = GetTestPath(name);
    {
      std::ofstream file(path);
      ASSERT_TRUE(file.is_open());
      file << data;
      file.close();
      ASSERT_FALSE(file.fail());
    }
    auto stats = ASSERT_RESULT(pipeline.ImportFile(path));
    ASSERT_EQ(stats.triples_parsed, 2);
    ASSERT_EQ(stats.triples_written, 2);
    expected_rows += 2;

    // Do not flush here: successful import must make its own writes visible.
    ASSERT_NO_FATALS(LoadFromDocDB());
    for (int perm = 0; perm < 6; ++perm) {
      ASSERT_EQ(RdfYBModel::Instance().GetCache(perm).Size(), expected_rows);
    }
  }
}

}  // namespace yb::mdb

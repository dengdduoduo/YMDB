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

#include <array>
#include <fstream>
#include <set>
#include <sstream>
#include <string>

#include <gtest/gtest.h>

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
#include "yb/master/master_client.pb.h"
#include "yb/mdb/import/csv_parser.h"
#include "yb/mdb/import/import_pipeline.h"
#include "yb/mdb/import/insert_executor.h"
#include "yb/mdb/import/nt_parser.h"
#include "yb/mdb/import/rdf_xml_parser.h"
#include "yb/mdb/model/rdf_yb_model.h"
#include "yb/util/status.h"
#include "yb/util/countdown_latch.h"
#include "yb/util/flags.h"
#include "yb/util/test_thread_holder.h"
#include "yb/util/scope_exit.h"
#include "yb/util/test_macros.h"

DECLARE_bool(mdb_dictionary_v2);

using SPARQL::BindingIterConstructor;
using SPARQL::Op;
using SPARQL::QueryParser;

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
    opts.num_tablet_servers = TabletServerCount();
    cluster_.reset(new MiniCluster(std::move(opts)));
    ASSERT_OK(cluster_->Start());
    ASSERT_OK(CreateClient());
  }

  virtual int TabletServerCount() const { return 1; }

  bool original_dictionary_v2_ = FLAGS_mdb_dictionary_v2;

  void DoTearDown() override {
    RdfYBModel::Instance().ClearAll();
    FLAGS_mdb_dictionary_v2 = original_dictionary_v2_;
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

  void InitPersistentModel() {
    FLAGS_mdb_dictionary_v2 = true;
    ASSERT_NO_FATALS(InitModel());
  }

  void AssertStoredObjects(int count) {
    client::TableHandle table;
    ASSERT_OK(table.Open(client::YBTableName(
        YQL_DATABASE_CQL, kNamespace, RdfYBModel::kTableNames[0]), client_.get()));
    Status read_status = Status::OK();
    client::TableIteratorOptions options;
    options.columns = {"c0", "c1", "c2"};
    options.error_handler = [&read_status](const Status& status) { read_status = status; };
    std::multiset<std::array<int64_t, 3>> actual;
    for (const auto& row : client::TableRange(table, options)) {
      actual.insert({row.column(0).int64_value(), row.column(1).int64_value(),
                     row.column(2).int64_value()});
    }
    ASSERT_OK(read_status);
    std::multiset<std::array<int64_t, 3>> expected;
    for (int i = 0; i < count; ++i) {
      expected.insert({1, 2, 3 + i});
    }
    ASSERT_EQ(actual, expected);
  }

  void AssertNoStoredTriples() {
    auto& model = RdfYBModel::Instance();
    ASSERT_OK(model.LoadAllFromDocDB());
    for (int perm = 0; perm < 6; ++perm) ASSERT_EQ(model.GetCache(perm).Size(), size_t{0});
  }

  void CheckFileVisibility(const std::string& extension, const std::string& contents) {
    ASSERT_NO_FATALS(CreateRdfTables());
    ASSERT_NO_FATALS(InitPersistentModel());
    const auto path = GetTestPath("visibility" + extension);
    {
      std::ofstream file(path);
      ASSERT_TRUE(file.is_open());
      file << contents;
      file.close();
      ASSERT_TRUE(file.good());
    }
    ImportPipeline pipeline;
    auto stats = ASSERT_RESULT(pipeline.ImportFile(path));
    ASSERT_EQ(stats.triples_written, 2);
    // Read through a separate session, without flushing or loading the model cache.
    ASSERT_NO_FATALS(AssertStoredObjects(2));
    stats = ASSERT_RESULT(pipeline.ImportFile(path));
    ASSERT_EQ(stats.triples_written, 2);
    ASSERT_NO_FATALS(AssertStoredObjects(2));
    Triple appended{"http://example.org/s", "http://example.org/p", "third",
                    TripleTermType::URI, TripleTermType::URI, TripleTermType::LITERAL, ""};
    stats = ASSERT_RESULT(pipeline.ImportTriples({appended}));
    ASSERT_EQ(stats.triples_written, 1);
    ASSERT_NO_FATALS(AssertStoredObjects(3));
  }

  void LoadFromDocDB() {
    ASSERT_OK(RdfYBModel::Instance().LoadAllFromDocDB());
  }
};

TEST_F(MdbImportIntegrationTest, ImportNtFileAndQuery) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitPersistentModel());
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
  ASSERT_NO_FATALS(InitPersistentModel());
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
  ASSERT_NO_FATALS(InitPersistentModel());
  ASSERT_TRUE(RdfYBModel::Instance().IsConnected());

  const char* rdf_xml = R"(

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
  ASSERT_NO_FATALS(InitPersistentModel());
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

TEST_F(MdbImportIntegrationTest, NtFileIsVisibleOnReturn) {
  ASSERT_NO_FATALS(CheckFileVisibility(".nt",
      "<http://example.org/s> <http://example.org/p> \"first\" .\n"
      "<http://example.org/s> <http://example.org/p> \"second\" .\n"));
}

TEST_F(MdbImportIntegrationTest, CsvFileIsVisibleOnReturn) {
  ASSERT_NO_FATALS(CheckFileVisibility(".csv",
      "subject,predicate,object\n"
      "http://example.org/s,http://example.org/p,first\n"
      "http://example.org/s,http://example.org/p,second\n"));
}

TEST_F(MdbImportIntegrationTest, XmlFileIsVisibleOnReturn) {
  ASSERT_NO_FATALS(CheckFileVisibility(".rdf", R"(
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
         xmlns:ex="http://example.org/">
  <rdf:Description rdf:about="http://example.org/s">
    <ex:p>first</ex:p>
    <ex:p>second</ex:p>
  </rdf:Description>
</rdf:RDF>
)"));
}

TEST_F(MdbImportIntegrationTest, TriplesAreVisibleOnReturn) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitPersistentModel());
  Triple first{"http://example.org/s", "http://example.org/p", "first",
               TripleTermType::URI, TripleTermType::URI, TripleTermType::LITERAL, ""};
  auto second = first;
  second.obj = "second";
  ImportPipeline pipeline;
  auto stats = ASSERT_RESULT(pipeline.ImportTriples({first, second}));
  ASSERT_EQ(stats.triples_written, 2);
  ASSERT_NO_FATALS(AssertStoredObjects(2));
  stats = ASSERT_RESULT(pipeline.ImportTriples({first, second}));
  ASSERT_EQ(stats.triples_written, 2);
  ASSERT_NO_FATALS(AssertStoredObjects(2));
}



TEST_F(MdbImportIntegrationTest, SameLexemeIriAndLiteralStayDistinct) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitPersistentModel());
  Triple iri{"http://example.org/s", "http://example.org/p", "http://example.org/value",
             TripleTermType::URI, TripleTermType::URI, TripleTermType::URI, ""};
  auto literal = iri;
  literal.obj_type = TripleTermType::LITERAL;
  ImportPipeline pipeline;
  auto stats = ASSERT_RESULT(pipeline.ImportTriples({iri, literal}));
  ASSERT_EQ(stats.triples_written, 2);
  ASSERT_NO_FATALS(AssertStoredObjects(2));
}


TEST_F(MdbImportIntegrationTest, AllocationFailureAbortsOwnedInsertTransaction) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  auto& model = RdfYBModel::Instance();
  const auto previous = model.UseTransactions();
  auto restore_option = ScopeExit([&] { model.SetUseTransactions(previous); });
  model.SetUseTransactions(true);
  IdMap ids;
  ASSERT_TRUE(ids.Restore(IdMap::kMaxId, "urn:example:reserved"));
  ImportStats stats;
  const auto status = InsertDataExecutor::Execute(
      "INSERT DATA { <urn:example:s> <urn:example:p> <urn:example:o> . }", ids, stats,
      [](uint64_t, const RdfTerm&) { return Status::OK(); });
  ASSERT_TRUE(status.IsIllegalState()) << status;
  ASSERT_FALSE(model.HasTransaction());
  ASSERT_EQ(stats.triples_written, 0);
  ASSERT_NO_FATALS(AssertStoredObjects(0));
}

TEST_F(MdbImportIntegrationTest, ImportPropagatesFlushFailure) {
  RdfYBModel::Instance().ClearAll();
  ImportPipeline pipeline;
  auto result = pipeline.ImportTriples({});
  ASSERT_FALSE(result.ok());
  ASSERT_TRUE(result.status().IsIllegalState()) << result.status();
}


TEST_F(MdbImportIntegrationTest, IndependentPipelinesPreserveDifferentTriples) {
  const auto old = FLAGS_mdb_dictionary_v2;
  auto restore = ScopeExit([&] { FLAGS_mdb_dictionary_v2 = old; });
  FLAGS_mdb_dictionary_v2 = true;
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  Triple first{"urn:example:s", "urn:example:p", "first",
               TripleTermType::URI, TripleTermType::URI, TripleTermType::LITERAL, ""};
  auto second = first;
  second.obj = "second";
  ImportPipeline first_pipeline;
  ImportPipeline second_pipeline;
  ASSERT_RESULT(first_pipeline.ImportTriples({first}));
  ASSERT_RESULT(second_pipeline.ImportTriples({second}));
  ASSERT_NO_FATALS(AssertStoredObjects(2));
}


TEST_F(MdbImportIntegrationTest, DictionaryTwoClientsAllocateWithoutCollisions) {
  ASSERT_OK(client_->CreateNamespaceIfNotExists(kNamespace, YQL_DATABASE_CQL));
  auto second_client = ASSERT_RESULT(cluster_->CreateClient());
  PersistentIdMap first;
  PersistentIdMap second;
  ASSERT_OK(first.Init(client_.get(), kNamespace));
  ASSERT_OK(second.Init(second_client.get(), kNamespace));
  std::array<std::array<uint64_t, 16>, 2> shared{};
  std::array<std::set<uint64_t>, 2> distinct;
  CountDownLatch ready(2);
  TestThreadHolder threads;
  for (size_t worker = 0; worker < 2; ++worker) {
    threads.AddThreadFunctor([&, worker] {
      auto& dictionary = worker == 0 ? first : second;
      ready.CountDown();
      ready.Wait();
      for (size_t i = 0; i < 16; ++i) {
        const RdfTerm common{TripleTermType::LITERAL, "common-" + std::to_string(i), "en", ""};
        shared[worker][i] = ASSERT_RESULT(dictionary.GetOrCreate(common));
        const RdfTerm own{TripleTermType::LITERAL,
            "own-" + std::to_string(worker) + "-" + std::to_string(i), "", ""};
        ASSERT_TRUE(distinct[worker].insert(ASSERT_RESULT(dictionary.GetOrCreate(own))).second);
      }
    });
  }
  threads.JoinAll();
  ASSERT_EQ(shared[0], shared[1]);
  std::set<uint64_t> all;
  for (auto id : shared[0]) ASSERT_TRUE(all.insert(id).second);
  for (const auto& group : distinct) {
    ASSERT_EQ(group.size(), size_t{16});
    for (auto id : group) ASSERT_TRUE(all.insert(id).second);
  }
  ASSERT_EQ(all.size(), size_t{48});
  auto entries = ASSERT_RESULT(first.Entries());
  ASSERT_EQ(entries.size(), size_t{48});
  PersistentIdMap reopened;
  ASSERT_OK(reopened.Init(second_client.get(), kNamespace));
  const RdfTerm common{TripleTermType::LITERAL, "common-0", "EN", ""};
  ASSERT_EQ(ASSERT_RESULT(reopened.GetOrCreate(common)), shared[0][0]);
}

TEST_F(MdbImportIntegrationTest, V2RejectsPopulatedLegacyNamespace) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  ASSERT_OK(RdfYBModel::Instance().WriteTripleToDocDB(1, 2, 3));
  ASSERT_OK(RdfYBModel::Instance().Flush());
  RdfYBModel::Instance().ClearAll();
  const auto old = FLAGS_mdb_dictionary_v2;
  auto restore = ScopeExit([&] { FLAGS_mdb_dictionary_v2 = old; });
  FLAGS_mdb_dictionary_v2 = true;
  const auto status = RdfYBModel::Instance().Init(client_.get(), kNamespace);
  ASSERT_TRUE(status.IsNotSupported()) << status;
  ASSERT_NO_FATALS(AssertStoredObjects(1));
  client::TableHandle spo;
  ASSERT_OK(spo.Open(client::YBTableName(
      YQL_DATABASE_CQL, kNamespace, RdfYBModel::kTableNames[0]), client_.get()));
  auto session = client_->NewSession(MonoDelta::FromSeconds(30));
  auto deletion = spo.NewDeleteOp(session->arena());
  QLAddInt64HashValue(deletion->mutable_request(), 1);
  QLAddInt64RangeValue(deletion->mutable_request(), 2);
  QLAddInt64RangeValue(deletion->mutable_request(), 3);
  ASSERT_OK(session->TEST_ApplyAndFlush(deletion));
  ASSERT_NO_FATALS(AssertStoredObjects(0));
  // A partial old write in another permutation must also block initialization.
  const auto orphan_status = RdfYBModel::Instance().Init(client_.get(), kNamespace);
  ASSERT_TRUE(orphan_status.IsNotSupported()) << orphan_status;
}

TEST_F(MdbImportIntegrationTest, V2RequiresFlagOnReopen) {
  const auto old = FLAGS_mdb_dictionary_v2;
  auto restore = ScopeExit([&] { FLAGS_mdb_dictionary_v2 = old; });
  FLAGS_mdb_dictionary_v2 = true;
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  RdfYBModel::Instance().ClearAll();
  FLAGS_mdb_dictionary_v2 = false;
  const auto status = RdfYBModel::Instance().Init(client_.get(), kNamespace);
  ASSERT_TRUE(status.IsNotSupported()) << status;
}

TEST_F(MdbImportIntegrationTest, LegacyRejectsMetadataInsteadOfCollapsing) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  const auto triple = ASSERT_RESULT(NtParser::ParseLine(
      "<urn:example:s> <urn:example:p> \"same\"@en ."));
  IdMap ids;
  const auto result = RdfYBModel::Instance().ResolveTerm(ids, ObjectTerm(triple));
  ASSERT_FALSE(result.ok());
  ASSERT_TRUE(result.status().IsNotSupported()) << result.status();
  ASSERT_NO_FATALS(AssertStoredObjects(0));
}


TEST_F(MdbImportIntegrationTest, DictionaryCounterCorruptionAndExhaustionAreChecked) {
  ASSERT_OK(client_->CreateNamespaceIfNotExists(kNamespace, YQL_DATABASE_CQL));
  PersistentIdMap dictionary;
  ASSERT_OK(dictionary.Init(client_.get(), kNamespace));
  const RdfTerm term{TripleTermType::LITERAL, "existing", "en", ""};
  const auto id = ASSERT_RESULT(dictionary.GetOrCreate(term));
  client::TableHandle table;
  ASSERT_OK(table.Open(client::YBTableName(
      YQL_DATABASE_CQL, kNamespace, PersistentIdMap::kTableName), client_.get()));
  auto session = client_->NewSession(MonoDelta::FromSeconds(30));
  auto seed = [&](const std::string& key, uint64_t value) {
    auto op = table.NewInsertOp(session->arena());
    QLAddStringHashValue(op->mutable_request(), key);
    table.AddInt64ColumnValue(op->mutable_request(), "term_id", static_cast<int64_t>(value));
    ASSERT_OK(session->TEST_ApplyAndFlush(op));
  };
  ASSERT_NO_FATALS(seed("counter", 0));
  PersistentIdMap behind;
  ASSERT_TRUE(behind.Init(client_.get(), kNamespace).IsCorruption());
  ASSERT_NO_FATALS(seed("counter", IdMap::kMaxId + 1));
  PersistentIdMap overflow;
  ASSERT_TRUE(overflow.Init(client_.get(), kNamespace).IsCorruption());
  ASSERT_NO_FATALS(seed("counter", IdMap::kMaxId));
  PersistentIdMap exhausted;
  ASSERT_OK(exhausted.Init(client_.get(), kNamespace));
  ASSERT_EQ(ASSERT_RESULT(exhausted.GetOrCreate(term)), id);
  const auto failed = exhausted.GetOrCreate({TripleTermType::URI, "urn:example:new", "", ""});
  ASSERT_FALSE(failed.ok());
  ASSERT_TRUE(failed.status().IsIllegalState()) << failed.status();
  auto deletion = table.NewDeleteOp(session->arena());
  QLAddStringHashValue(deletion->mutable_request(), "counter");
  ASSERT_OK(session->TEST_ApplyAndFlush(deletion));
  PersistentIdMap missing;
  ASSERT_TRUE(missing.Init(client_.get(), kNamespace).IsCorruption());
  ASSERT_NO_FATALS(seed("counter", id));
  const RdfTerm other{TripleTermType::LITERAL, "different", "fr", ""};
  ASSERT_NO_FATALS(seed(ASSERT_RESULT(other.Key()), id));
  PersistentIdMap duplicate;
  ASSERT_TRUE(duplicate.Init(client_.get(), kNamespace).IsCorruption());
}

TEST_F(MdbImportIntegrationTest, InterruptedAllocationLeavesGapWithoutReusingId) {
  ASSERT_OK(client_->CreateNamespaceIfNotExists(kNamespace, YQL_DATABASE_CQL));
  PersistentIdMap dictionary;
  ASSERT_OK(dictionary.Init(client_.get(), kNamespace));
  client::TableHandle table;
  ASSERT_OK(table.Open(client::YBTableName(
      YQL_DATABASE_CQL, kNamespace, PersistentIdMap::kTableName), client_.get()));
  auto session = client_->NewSession(MonoDelta::FromSeconds(30));
  // Simulate a committed counter reservation followed by a crash before the term claim.
  auto reservation = table.NewInsertOp(session->arena());
  QLAddStringHashValue(reservation->mutable_request(), "counter");
  table.AddInt64ColumnValue(reservation->mutable_request(), "term_id", 7);
  ASSERT_OK(session->TEST_ApplyAndFlush(reservation));
  PersistentIdMap reopened;
  ASSERT_OK(reopened.Init(client_.get(), kNamespace));
  const RdfTerm next{TripleTermType::URI, "urn:example:after-gap", "", ""};
  ASSERT_EQ(ASSERT_RESULT(reopened.GetOrCreate(next)), uint64_t{8});
  ASSERT_EQ(ASSERT_RESULT(dictionary.GetOrCreate(next)), uint64_t{8});
  ASSERT_EQ(ASSERT_RESULT(reopened.Entries()).size(), size_t{1});
}


TEST_F(MdbImportIntegrationTest, LegacyIndependentPipelineRefusesUnrestoredIds) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  // Synthetic orphaned legacy IDs must not be reinterpreted by a local dictionary.
  ASSERT_OK(RdfYBModel::Instance().WriteTripleToDocDB(1, 2, 3));
  ASSERT_OK(RdfYBModel::Instance().Flush());
  const Triple second{"urn:example:s", "urn:example:p", "second",
      TripleTermType::URI, TripleTermType::URI, TripleTermType::LITERAL, ""};
  ImportPipeline pipeline;
  const auto failed = pipeline.ImportTriples({second});
  ASSERT_FALSE(failed.ok());
  ASSERT_TRUE(failed.status().IsNotSupported()) << failed.status();
  ASSERT_EQ(pipeline.stats().triples_written, 0);
  ASSERT_NO_FATALS(AssertStoredObjects(1));
}


TEST_F(MdbImportIntegrationTest, MalformedNtFileDoesNotWriteValidPrefix) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  const auto path = GetTestPath("malformed.nt");
  std::ofstream file(path);
  file << "<urn:example:s> <urn:example:p> \"valid\" .\ninvalid .\n";
  file.close();
  ImportPipeline pipeline;
  const auto result = pipeline.ImportFile(path);
  ASSERT_FALSE(result.ok());
  ASSERT_EQ(pipeline.stats().errors, 1);
  ASSERT_EQ(pipeline.stats().triples_written, 0);
  ASSERT_NO_FATALS(AssertStoredObjects(0));
}

TEST_F(MdbImportIntegrationTest, BlankLabelsShareOnlyWithinOneImportCall) {
  const auto old = FLAGS_mdb_dictionary_v2;
  auto restore = ScopeExit([&] { FLAGS_mdb_dictionary_v2 = old; });
  FLAGS_mdb_dictionary_v2 = true;
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  const auto first = ASSERT_RESULT(NtParser::ParseLine("_:b <urn:example:p> _:b ."));
  const auto second = ASSERT_RESULT(NtParser::ParseLine("_:b <urn:example:q> \"x\" ."));
  ImportPipeline pipeline;
  ASSERT_RESULT(pipeline.ImportTriples({first, second}));
  ASSERT_RESULT(pipeline.ImportTriples({first, second}));
  auto entries = ASSERT_RESULT(RdfYBModel::Instance().Dictionary()->Entries());
  std::set<uint64_t> blanks;
  for (const auto& [id, term] : entries) {
    if (term.kind == TripleTermType::BLANK) blanks.insert(id);
  }
  ASSERT_EQ(blanks.size(), size_t{2});
  ASSERT_NO_FATALS(LoadFromDocDB());
  auto& cache = RdfYBModel::Instance().GetCache(0);
  for (auto id : blanks) {
    // Distinct subject partitions each retain their self-edge and value edge.
    size_t rows = 0;
    for (size_t i = 0; i < cache.Size(); ++i) {
      uint64_t s, p, o;
      cache.GetRowAt(i, &s, &p, &o);
      if (s == id) ++rows;
    }
    ASSERT_EQ(rows, size_t{2});
  }
}


TEST_F(MdbImportIntegrationTest, InsertPersistenceFailureStopsBeforeDataWrite) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  auto& model = RdfYBModel::Instance();
  const auto old = model.UseTransactions();
  auto restore = ScopeExit([&] { model.SetUseTransactions(old); });
  model.SetUseTransactions(false);
  IdMap ids;
  ImportStats stats;
  const auto status = InsertDataExecutor::Execute(
      "INSERT DATA { <urn:example:s> <urn:example:p> \"value\" . }", ids, stats,
      [](uint64_t, const RdfTerm&) {
        return STATUS(IOError, "synthetic dictionary write failure");
      });
  ASSERT_TRUE(status.IsIOError()) << status;
  ASSERT_EQ(stats.triples_written, 0);
  ASSERT_NO_FATALS(AssertStoredObjects(0));
}


TEST_F(MdbImportIntegrationTest, ReadSnapshotPinsEveryModelScan) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  auto& model = RdfYBModel::Instance();
  ASSERT_OK(model.WriteTripleToDocDB(1, 2, 3));
  ASSERT_OK(model.Flush());
  ASSERT_OK(model.BeginReadSnapshot());
  auto second_client = ASSERT_RESULT(cluster_->CreateClient());
  auto session = second_client->NewSession(MonoDelta::FromSeconds(30));
  for (int perm = 0; perm < 6; ++perm) {
    client::TableHandle table;
    ASSERT_OK(table.Open(client::YBTableName(
        YQL_DATABASE_CQL, kNamespace, RdfYBModel::kTableNames[perm]), second_client.get()));
    uint64_t c0, c1, c2;
    DocKeyEncoder::Permute(perm, 1, 2, 4, &c0, &c1, &c2);
    auto op = table.NewInsertOp(session->arena());
    QLAddInt64HashValue(op->mutable_request(), static_cast<int64_t>(c0));
    QLAddInt64RangeValue(op->mutable_request(), static_cast<int64_t>(c1));
    QLAddInt64RangeValue(op->mutable_request(), static_cast<int64_t>(c2));
    ASSERT_OK(session->TEST_ApplyAndFlush(op));
  }
  ASSERT_OK(model.LoadPartition(0, 1));
  ASSERT_EQ(model.GetPartitionCache(0, 1).Size(), size_t{1});
  ASSERT_OK(model.LoadAllFromDocDB());
  for (int perm = 0; perm < 6; ++perm) {
    const auto& cache = model.GetCache(perm);
    ASSERT_EQ(cache.Size(), size_t{1});
    uint64_t c0, c1, c2, subject, predicate, object;
    cache.GetRowAt(0, &c0, &c1, &c2);
    DocKeyEncoder::ReversePermute(perm, c0, c1, c2, &subject, &predicate, &object);
    ASSERT_EQ(subject, 1);
    ASSERT_EQ(predicate, 2);
    ASSERT_EQ(object, 3);
  }
  ASSERT_OK(model.RefreshStatistics());
  ASSERT_EQ(model.GetStatistics()->rows, 1);
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(client_->GetTablets(model.GetTable(0)->name(), 0, &tablets, nullptr));
  ASSERT_EQ(tablets.size(), 1);
  ASSERT_OK(model.LoadTablet(0, tablets.Get(0).tablet_id()));
  ASSERT_EQ(model.GetTabletCache(0, tablets.Get(0).tablet_id()).Size(), size_t{1});
  model.EndReadSnapshot();
  ASSERT_OK(model.BeginReadSnapshot());
  ASSERT_OK(model.LoadPartition(0, 1));
  ASSERT_EQ(model.GetPartitionCache(0, 1).Size(), size_t{2});
  ASSERT_OK(model.RefreshStatistics());
  ASSERT_EQ(model.GetStatistics()->rows, 2);
  model.EndReadSnapshot();
}


TEST_F(MdbImportIntegrationTest, DictionaryEntriesHonorReadSnapshot) {
  const auto old = FLAGS_mdb_dictionary_v2;
  auto restore = ScopeExit([&] { FLAGS_mdb_dictionary_v2 = old; });
  FLAGS_mdb_dictionary_v2 = true;
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  auto& model = RdfYBModel::Instance();
  auto* dictionary = model.Dictionary();
  const RdfTerm first{TripleTermType::URI, "urn:example:first", "", ""};
  const RdfTerm second{TripleTermType::LITERAL, "new value", "en", ""};
  const auto first_id = ASSERT_RESULT(dictionary->GetOrCreate(first));
  ASSERT_OK(model.BeginReadSnapshot());
  auto second_client = ASSERT_RESULT(cluster_->CreateClient());
  PersistentIdMap writer;
  ASSERT_OK(writer.Init(second_client.get(), kNamespace));
  const auto second_id = ASSERT_RESULT(writer.GetOrCreate(second));
  ASSERT_NE(first_id, second_id);
  auto entries = ASSERT_RESULT(dictionary->Entries(model.QueryReadTime()));
  ASSERT_EQ(entries.size(), size_t{1});
  ASSERT_EQ(entries[0].first, first_id);
  ASSERT_EQ(ASSERT_RESULT(entries[0].second.Key()), ASSERT_RESULT(first.Key()));
  model.EndReadSnapshot();
  ASSERT_OK(model.BeginReadSnapshot());
  entries = ASSERT_RESULT(dictionary->Entries(model.QueryReadTime()));
  std::set<uint64_t> ids;
  for (const auto& entry : entries) ASSERT_TRUE(ids.insert(entry.first).second);
  ASSERT_EQ(ids, (std::set<uint64_t>{first_id, second_id}));
  model.EndReadSnapshot();
}

TEST_F(MdbImportIntegrationTest, ReadSnapshotRejectsNestingWithoutLosingOwner) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  auto& model = RdfYBModel::Instance();
  ASSERT_OK(model.BeginReadSnapshot());
  const auto pinned = model.QueryReadTime();
  ASSERT_TRUE(static_cast<bool>(pinned));
  ASSERT_TRUE(model.BeginReadSnapshot().IsIllegalState());
  ASSERT_TRUE(model.BeginTransaction().IsIllegalState());
  ASSERT_EQ(model.QueryReadTime(), pinned);
  model.EndReadSnapshot();
  ASSERT_FALSE(static_cast<bool>(model.QueryReadTime()));
  ASSERT_OK(model.BeginTransaction());
  ASSERT_TRUE(model.BeginReadSnapshot().IsIllegalState());
  ASSERT_TRUE(model.HasTransaction());
  ASSERT_OK(model.AbortTransaction());
  ASSERT_OK(model.BeginReadSnapshot());
  model.EndReadSnapshot();
}



TEST_F(MdbImportIntegrationTest, V2PipelineBatchFlushPreservesCallerTransaction) {
  const auto old = FLAGS_mdb_dictionary_v2;
  auto restore = ScopeExit([&] { FLAGS_mdb_dictionary_v2 = old; });
  FLAGS_mdb_dictionary_v2 = true;
  ASSERT_OK(client_->CreateNamespaceIfNotExists(kNamespace, YQL_DATABASE_CQL));
  for (int perm = 0; perm < 6; ++perm) {
    client::YBSchemaBuilder builder;
    builder.AddColumn("c0")->Type(DataType::INT64)->HashPrimaryKey()->NotNull();
    builder.AddColumn("c1")->Type(DataType::INT64)->PrimaryKey()->NotNull();
    builder.AddColumn("c2")->Type(DataType::INT64)->PrimaryKey()->NotNull();
    TableProperties properties;
    properties.SetTransactional(true);
    builder.SetTableProperties(properties);
    client::TableHandle table;
    ASSERT_OK(table.Create(client::YBTableName(
        YQL_DATABASE_CQL, kNamespace, RdfYBModel::kTableNames[perm]),
        kNumTablets, client_.get(), &builder));
  }
  ASSERT_NO_FATALS(InitModel());
  auto& model = RdfYBModel::Instance();
  std::vector<Triple> triples;
  for (int i = 0; i < 128; ++i) {
    triples.push_back({"urn:example:subject", "urn:example:predicate", std::to_string(i),
        TripleTermType::URI, TripleTermType::URI, TripleTermType::LITERAL, ""});
  }
  ASSERT_OK(model.BeginTransaction());
  ImportPipeline pipeline;
  ASSERT_EQ(ASSERT_RESULT(pipeline.ImportTriples(triples)).triples_written, 128);
  ASSERT_TRUE(model.HasTransaction());
  client::TableIteratorOptions options;
  Status status;
  options.columns = {"c0", "c1", "c2"};
  options.error_handler = [&](const Status& error) { status = error; };
  for (const auto& row : client::TableRange(*model.GetTable(0), options)) {
    (void)row;
    FAIL() << "Pipeline committed a transaction owned by its caller";
  }
  ASSERT_OK(status);
  ASSERT_OK(model.CommitTransaction());
  const auto entries = ASSERT_RESULT(model.Dictionary()->Entries());
  IdMap dictionary;
  for (const auto& [id, term] : entries) ASSERT_TRUE(dictionary.Restore(id, term));
  std::multiset<std::array<uint64_t, 3>> expected;
  const auto subject = dictionary.Get("urn:example:subject", TripleTermType::URI);
  const auto predicate = dictionary.Get("urn:example:predicate", TripleTermType::URI);
  ASSERT_NE(subject, 0);
  ASSERT_NE(predicate, 0);
  for (int i = 0; i < 128; ++i) {
    const auto object = dictionary.Get(std::to_string(i), TripleTermType::LITERAL);
    ASSERT_NE(object, 0);
    expected.insert({subject, predicate, object});
  }
  ASSERT_OK(model.LoadAllFromDocDB());
  for (int perm = 0; perm < 6; ++perm) {
    std::multiset<std::array<uint64_t, 3>> actual;
    const auto& cache = model.GetCache(perm);
    for (size_t i = 0; i < cache.Size(); ++i) {
      uint64_t c0, c1, c2, subject_id, predicate_id, object_id;
      cache.GetRowAt(i, &c0, &c1, &c2);
      DocKeyEncoder::ReversePermute(
          perm, c0, c1, c2, &subject_id, &predicate_id, &object_id);
      actual.insert({subject_id, predicate_id, object_id});
    }
    ASSERT_EQ(actual, expected);
  }
}

class MdbImportMultiServerTest : public MdbImportIntegrationTest {
 protected:
  int TabletServerCount() const override { return 3; }
};

TEST_F(MdbImportMultiServerTest, PagedSnapshotAcrossTabletServers) {
  ASSERT_OK(client_->CreateNamespaceIfNotExists(kNamespace, YQL_DATABASE_CQL));
  client::YBSchemaBuilder builder;
  builder.AddColumn("c0")->Type(DataType::INT64)->HashPrimaryKey()->NotNull();
  builder.AddColumn("c1")->Type(DataType::INT64)->PrimaryKey()->NotNull();
  builder.AddColumn("c2")->Type(DataType::INT64)->PrimaryKey()->NotNull();
  client::TableHandle table;
  ASSERT_OK(table.Create(client::YBTableName(
      YQL_DATABASE_CQL, kNamespace, "snapshot_rows"), 8, client_.get(), &builder));
  const auto write_rows = [&](int begin, int end) -> Status {
    auto session = client_->NewSession(MonoDelta::FromSeconds(30));
    for (int i = begin; i < end; ++i) {
      auto op = table.NewInsertOp(session->arena());
      QLAddInt64HashValue(op->mutable_request(), i);
      QLAddInt64RangeValue(op->mutable_request(), i + 1000);
      QLAddInt64RangeValue(op->mutable_request(), i + 2000);
      session->Apply(op);
    }
    return session->TEST_Flush();
  };
  ASSERT_OK(write_rows(0, 192));
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  auto& model = RdfYBModel::Instance();
  ASSERT_OK(model.BeginReadSnapshot());
  ASSERT_OK(write_rows(192, 384));
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(client_->GetTablets(table.name(), 0, &tablets, nullptr));
  ASSERT_EQ(tablets.size(), 8);
  std::set<std::string> leaders;
  for (const auto& tablet : tablets) {
    for (const auto& replica : tablet.replicas()) {
      if (replica.role() == PeerRole::LEADER) leaders.insert(replica.ts_info().permanent_uuid());
    }
  }
  ASSERT_GE(leaders.size(), size_t{2});
  const auto check_rows = [&](int count) {
    Status status;
    client::TableIteratorOptions options;
    options.columns = {"c0", "c1", "c2"};
    options.page_size = 7;
    options.read_time = model.QueryReadTime();
    options.error_handler = [&](const Status& error) { status = error; };
    std::multiset<std::array<int64_t, 3>> actual, expected;
    for (const auto& row : client::TableRange(table, options)) {
      actual.insert({row.column(0).int64_value(), row.column(1).int64_value(),
                     row.column(2).int64_value()});
    }
    ASSERT_OK(status);
    for (int i = 0; i < count; ++i) expected.insert({i, i + 1000, i + 2000});
    ASSERT_EQ(actual, expected);
  };
  ASSERT_NO_FATALS(check_rows(192));
  model.EndReadSnapshot();
  ASSERT_OK(model.BeginReadSnapshot());
  ASSERT_NO_FATALS(check_rows(384));
  model.EndReadSnapshot();
}


TEST_F(MdbImportIntegrationTest, LegacyPipelineRejectsMissingPersistenceBeforeWriting) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  ImportPipeline pipeline;
  const Triple triple{"urn:example:subject", "urn:example:predicate", "value",
      TripleTermType::URI, TripleTermType::URI, TripleTermType::LITERAL, ""};
  const auto result = pipeline.ImportTriples({triple});
  ASSERT_FALSE(result.ok());
  ASSERT_TRUE(result.status().IsNotSupported()) << result.status();
  ASSERT_EQ(pipeline.stats().triples_written, 0);
  ASSERT_NO_FATALS(AssertNoStoredTriples());
}

TEST_F(MdbImportIntegrationTest, LegacyInsertRejectsMissingPersistenceEvenWithRestoredIds) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  auto& model = RdfYBModel::Instance();
  const auto old = model.UseTransactions();
  auto restore = ScopeExit([&] { model.SetUseTransactions(old); });
  model.SetUseTransactions(false);
  for (bool restored : {false, true}) {
    IdMap ids;
    if (restored) {
      ASSERT_TRUE(ids.Restore(41, "urn:example:subject"));
      ASSERT_TRUE(ids.Restore(43, "urn:example:predicate"));
      ASSERT_TRUE(ids.Restore(47, "value", TripleTermType::LITERAL));
    }
    ImportStats stats;
    const auto status = InsertDataExecutor::Execute(
        "INSERT DATA { <urn:example:subject> <urn:example:predicate> \"value\" . }", ids, stats);
    ASSERT_TRUE(status.IsNotSupported()) << status;
    ASSERT_EQ(stats.triples_written, 0);
    ASSERT_NO_FATALS(AssertNoStoredTriples());
  }
}

TEST_F(MdbImportIntegrationTest, InsertRejectsCallerTransactionWithoutAbortingIt) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  auto& model = RdfYBModel::Instance();
  const auto old = model.UseTransactions();
  auto restore = ScopeExit([&] { model.SetUseTransactions(old); });
  for (bool transactions : {true, false}) {
    model.SetUseTransactions(transactions);
    ASSERT_OK(model.BeginTransaction());
    IdMap ids;
    ASSERT_TRUE(ids.Restore(IdMap::kMaxId, "urn:example:reserved"));
    ImportStats stats;
    const auto status = InsertDataExecutor::Execute(
        "INSERT DATA { <urn:example:s> <urn:example:p> \"value\" . }", ids, stats,
        [](uint64_t, const RdfTerm&) { return Status::OK(); });
    ASSERT_TRUE(status.IsIllegalState()) << status;
    ASSERT_TRUE(model.HasTransaction());
    ASSERT_EQ(stats.triples_written, 0);
    ASSERT_OK(model.AbortTransaction());
  }
}


TEST_F(MdbImportIntegrationTest, LegacyFilesRejectMissingPersistenceBeforeWriting) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitModel());
  const std::vector<std::pair<std::string, std::string>> files{
      {".nt", "<urn:example:s> <urn:example:p> \"value\" .\n"},
      {".csv", "subject,predicate,object\nurn:example:s,urn:example:p,value\n"},
      {".rdf", R"(<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
          xmlns:ex="urn:example:"><rdf:Description rdf:about="urn:example:s">
          <ex:p>value</ex:p></rdf:Description></rdf:RDF>)"}};
  for (const auto& [extension, contents] : files) {
    const auto path = GetTestPath("legacy-unsafe" + extension);
    std::ofstream file(path);
    file << contents;
    file.close();
    ASSERT_FALSE(file.fail());
    ImportPipeline pipeline;
    const auto result = pipeline.ImportFile(path);
    ASSERT_FALSE(result.ok());
    ASSERT_TRUE(result.status().IsNotSupported()) << result.status();
    ASSERT_EQ(pipeline.stats().triples_parsed, 1);
    ASSERT_EQ(pipeline.stats().triples_written, 0);
    ASSERT_NO_FATALS(AssertNoStoredTriples());
  }
}

TEST_F(MdbImportIntegrationTest, ImportEntriesRejectActiveReadSnapshot) {
  ASSERT_NO_FATALS(CreateRdfTables());
  ASSERT_NO_FATALS(InitPersistentModel());
  auto& model = RdfYBModel::Instance();
  ASSERT_OK(model.BeginReadSnapshot());
  const auto pinned = model.QueryReadTime();
  ImportPipeline pipeline;
  const Triple triple{"urn:example:subject", "urn:example:predicate", "value",
      TripleTermType::URI, TripleTermType::URI, TripleTermType::LITERAL, ""};
  const auto imported = pipeline.ImportTriples({triple});
  ASSERT_FALSE(imported.ok());
  ASSERT_TRUE(imported.status().IsIllegalState()) << imported.status();
  IdMap ids;
  ImportStats stats;
  const auto inserted = InsertDataExecutor::Execute(
      "INSERT DATA { <urn:example:s> <urn:example:p> \"value\" . }", ids, stats);
  ASSERT_TRUE(inserted.IsIllegalState()) << inserted;
  ASSERT_EQ(model.QueryReadTime(), pinned);
  ASSERT_EQ(ASSERT_RESULT(model.Dictionary()->Entries()).size(), size_t{0});
  model.EndReadSnapshot();
  ASSERT_NO_FATALS(AssertNoStoredTriples());
}

}  // namespace yb::mdb

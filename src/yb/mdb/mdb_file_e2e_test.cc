// Portions Copyright (c) YugabyteDB, Inc.


#include <unistd.h>

#include <algorithm>
#include <cstdlib>
#include <fstream>
#include <set>
#include <sstream>
#include <string>
#include <tuple>
#include <vector>


#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include "yb/client/schema.h"
#include "yb/client/session.h"
#include "yb/client/table_handle.h"
#include "yb/client/yb_table_name.h"
#include "yb/client/yb_op.h"
#include "yb/integration-tests/mini_cluster.h"
#include "yb/integration-tests/yb_mini_cluster_test_base.h"
#include "yb/mdb/adapter/mdb_adapter.h"
#include "yb/mdb/model/rdf_yb_model.h"
#include "query/query_context.h"
#include "graph_models/rdf_model/conversions.h"
#include "yb/mdb/import/import_pipeline.h"
#include "yb/mdb/import/insert_executor.h"
#include "yb/util/flags.h"
#include "yb/util/scope_exit.h"
#include "yb/util/path_util.h"
#include "yb/util/subprocess.h"
#include "yb/util/test_macros.h"

DECLARE_bool(mdb_dictionary_v2);

namespace yb::mdb {

struct CapturedTerm {
  YBCMdbTermKind kind;
  std::string lexical, language, datatype;
};

struct CapturedResults {
  size_t schemas = 0;
  bool cancel_schema = false;
  bool cancel_row = false;
  std::vector<std::string> columns;
  std::vector<std::vector<CapturedTerm>> rows;

  static int Schema(const YBCMdbString* names, size_t count, void* ctx) {
    auto& result = *static_cast<CapturedResults*>(ctx);
    ++result.schemas;
    for (size_t i = 0; i < count; ++i) {
      result.columns.emplace_back(names[i].data, names[i].size);
    }
    return result.cancel_schema ? 1 : 0;
  }

  static int Row(const YBCMdbValue* values, size_t count, void* ctx) {
    auto& result = *static_cast<CapturedResults*>(ctx);
    std::vector<CapturedTerm> row;
    for (size_t i = 0; i < count; ++i) {
      row.push_back({values[i].kind,
          std::string(values[i].lexical.data, values[i].lexical.size),
          std::string(values[i].language.data, values[i].language.size),
          std::string(values[i].datatype.data, values[i].datatype.size)});
    }
    result.rows.push_back(std::move(row));
    return result.cancel_row ? 1 : 0;
  }
};

class MdbFileE2ETest : public MiniClusterTestWithClient<MiniCluster> {
 protected:
  using Row = std::vector<std::string>;
  using Rows = std::multiset<Row>;
  static constexpr const char* kNamespace = "mdb_file_e2e";

  void SetUp() override {
    YBMiniClusterTestBase<MiniCluster>::SetUp();
    MiniClusterOptions options;
    options.num_tablet_servers = 1;
    cluster_ = std::make_unique<MiniCluster>(options);
    ASSERT_OK(cluster_->Start());
    ASSERT_OK(CreateClient());
    cli_ = ASSERT_RESULT(path_utils::GetToolPath("mdb_cli"));
  }

  Status RunCli(const std::vector<std::string>& args, std::string* output,
                bool transactions = false) {
    std::vector<std::string> command{
        cli_, "--mdb_masters=" + cluster_->GetMasterAddresses(),
        "--mdb_keyspace=" + std::string(kNamespace),
        std::string("--mdb_use_transactions=") + (transactions ? "true" : "false")};
    command.insert(command.end(), args.begin(), args.end());
    std::string error;
    const auto status = Subprocess::Call(command, output, &error);
    last_cli_error_ = error;
    const auto trace = error.find("MDB tablet-aware query: fragments=");
    if (trace != std::string::npos) {
      LOG(INFO) << error.substr(trace, error.find('\n', trace) - trace);
    }
    if (!status.ok()) {
      LOG(WARNING) << "MDB CLI failed: " << status << "\n" << error;
    }
    return status;
  }


  void CheckPersistedNumeric() {
    const std::string flag = "--mdb_dictionary_v2=true";
    const std::string xsd = "http://www.w3.org/2001/XMLSchema#";
    const auto path = GetTestPath("numeric-terms.nt");
    std::ofstream file(path);
    const std::vector<std::pair<std::string, std::string>> terms{
        {"01", "integer"}, {"1", "integer"}, {"-2", "int"}, {"10", "integer"},
        {"2.5", "decimal"}, {"3E0", "double"}, {"4.0", "float"}, {"oops", "integer"}};
    for (size_t i = 0; i < terms.size(); ++i) {
      file << "<urn:example:n" << i << "> <urn:example:value> \"" << terms[i].first
           << "\"^^<" << xsd << terms[i].second << "> .\n";
    }
    file.close();
    ASSERT_TRUE(file.good());
    std::string output;
    ASSERT_OK(RunCli({flag, "import", path}, &output));
    // Each query is a fresh process and must restore metadata from DocDB.
    ASSERT_OK(RunCli({flag, "query",
        "SELECT ?s ?n WHERE { ?s <urn:example:value> ?n . FILTER(?n > 1) }"}, &output));
    ASSERT_NO_FATALS(CheckOutput(output, {"s", "n"},
        {{"urn:example:n3", "10"}, {"urn:example:n4", "2.5"},
         {"urn:example:n5", "3E0"}, {"urn:example:n6", "4.0"}}));
    ASSERT_OK(RunCli({flag, "query",
        "SELECT ?n ?v WHERE { ?s <urn:example:value> ?n . "
        "FILTER(?n = 1) BIND(?n + 2 AS ?v) }"}, &output));
    ASSERT_NO_FATALS(CheckOutput(output, {"n", "v"}, {{"01", "3"}, {"1", "3"}}));
    ASSERT_OK(RunCli({flag, "query",
        "SELECT ?n WHERE { ?s <urn:example:value> ?n . FILTER(isNumeric(?n)) }"},
        &output));
    ASSERT_NO_FATALS(CheckOutput(output, {"n"},
        {{"01"}, {"1"}, {"-2"}, {"10"}, {"2.5"}, {"3E0"}, {"4.0"}}));
    ASSERT_OK(RunCli({flag, "query",
        "SELECT DISTINCT ?n WHERE { ?s <urn:example:value> ?n . FILTER(?n = 1) }"},
        &output));
    ASSERT_NO_FATALS(CheckOutput(output, {"n"}, {{"01"}, {"1"}}));
    ASSERT_OK(RunCli({flag, "query",
        "SELECT ?s WHERE { ?s <urn:example:value> \"01\"^^<" + xsd + "integer> }"},
        &output));
    ASSERT_NO_FATALS(CheckOutput(output, {"s"}, {{"urn:example:n0"}}));
    ASSERT_OK(RunCli({flag, "query",
        "SELECT ?n WHERE { ?s <urn:example:value> ?n . "
        "FILTER(sameTerm(?n, \"01\"^^<" + xsd + "integer>)) }"}, &output));
    ASSERT_NO_FATALS(CheckOutput(output, {"n"}, {{"01"}}));
    ASSERT_OK(RunCli({flag, "query",
        "SELECT ?n WHERE { ?s <urn:example:value> ?n . FILTER(isNumeric(?n)) } "
        "ORDER BY ?n LIMIT 3 OFFSET 3"}, &output));
    ASSERT_NO_FATALS(CheckOutput(output, {"n"}, {{"2.5"}, {"3E0"}, {"4.0"}}));
    ASSERT_OK(RunCli({flag, "query",
        "SELECT ?s WHERE { ?s <urn:example:value> 1 }"}, &output));
    ASSERT_NO_FATALS(CheckOutput(output, {"s"}, {{"urn:example:n1"}}));
    ASSERT_OK(RunCli({flag, "query",
        "SELECT ?s WHERE { BIND(1 + 0 AS ?n) ?s <urn:example:value> ?n }"}, &output));
    ASSERT_NO_FATALS(CheckOutput(output, {"s"}, {{"urn:example:n1"}}));
    ASSERT_OK(RunCli({flag, "query",
        "SELECT ?s WHERE { { SELECT (1 + 0 AS ?n) WHERE {} } "
        "?s <urn:example:value> ?n }"}, &output));
    ASSERT_NO_FATALS(CheckOutput(output, {"s"}, {{"urn:example:n1"}}));
    ASSERT_NO_FATALS(CheckPhysicalRows(terms.size()));
  }

  void CheckResidentRows(const std::string& query, const std::vector<std::string>& columns,
                         const Rows& expected) {
    std::vector<std::pair<std::string, std::string>> values;
    const auto callback = [](const char* name, const char* value, void* context) {
      auto* result = static_cast<decltype(values)*>(context);
      result->emplace_back(name, value);
    };
    get_query_ctx() = QueryContext{};
    ASSERT_EQ(YBCMdbExecuteSparql(query.c_str(), callback, &values), 0);
    std::ostringstream output;
    for (const auto& [name, value] : values) output << name << '\t' << value << '\n';
    ASSERT_NO_FATALS(CheckOutput(output.str(), columns, expected));
  }

  void QueryJson(const std::string& query, rapidjson::Document* document, bool v2 = false) {
    std::string output;
    std::vector<std::string> args{"--mdb_output_format=json", "query", query};
    if (v2) args.insert(args.begin(), "--mdb_dictionary_v2=true");
    ASSERT_OK(RunCli(args, &output));
    document->Parse(output.data(), output.size());
    ASSERT_FALSE(document->HasParseError()) << output;
    ASSERT_TRUE(document->IsObject());
    ASSERT_TRUE(document->HasMember("head"));
    ASSERT_TRUE((*document)["head"].HasMember("vars"));
    ASSERT_TRUE((*document)["head"]["vars"].IsArray());
    ASSERT_TRUE(document->HasMember("results"));
    ASSERT_TRUE((*document)["results"].HasMember("bindings"));
    ASSERT_TRUE((*document)["results"]["bindings"].IsArray());
  }

  static std::string Node(int index) {
    return "https://example.org/file/person/" + std::to_string(index);
  }

  static std::string Predicate(const std::string& name) {
    return "https://example.org/file/" + name;
  }

  void ImportFixture(int people = 32, bool transactions = false) {
    Rows data;
    const auto path = GetTestPath("people.nt");
    std::ofstream file(path);
    ASSERT_TRUE(file.is_open());
    file << "# Synthetic N-Triples fixture\n\n";
    for (int i = 0; i < people; ++i) {
      const std::string name = "Person " + std::to_string(i);
      file << '<' << Node(i) << "> <" << Predicate("name") << "> \"" << name << "\" .\n";
      data.insert({Node(i), Predicate("name"), name});
    }
    for (const auto& edge : std::vector<std::pair<int, int>>{{0, 1}, {0, 2}, {1, 2}}) {
      file << '<' << Node(edge.first) << "> <" << Predicate("knows") << "> <"
           << Node(edge.second) << "> .\n";
      data.insert({Node(edge.first), Predicate("knows"), Node(edge.second)});
    }
    const std::string description = "A long literal restored in a different process";
    file << '<' << Node(0) << "> <" << Predicate("description") << "> \""
         << description << "\" .\n";
    data.insert({Node(0), Predicate("description"), description});
    // Repeated input triples must not multiply RDF graph solutions.
    file << '<' << Node(0) << "> <" << Predicate("knows") << "> <" << Node(1) << "> .\n";
    file.close();
    ASSERT_FALSE(file.fail());
    std::string output;
    ASSERT_OK(RunCli({"import", path}, &output, transactions));
    expected_ = std::move(data);
  }

  void CheckRows(const std::string& query, const std::vector<std::string>& columns,
                 const Rows& expected, bool transactions = false, bool statistics = false) {
    SCOPED_TRACE(query);
    std::string output;
    std::vector<std::string> args{"query", query};
    if (statistics) args.insert(args.begin(), "--mdb_collect_statistics=true");
    ASSERT_OK(RunCli(args, &output, transactions));
    ASSERT_NE(last_cli_error_.find("MDB tablet-aware query: fragments="),
              std::string::npos) << last_cli_error_;
    ASSERT_NO_FATALS(CheckOutput(output, columns, expected));
  }

  void CheckOrderedRows(const std::string& query, const std::vector<std::string>& columns,
                        const std::vector<Row>& expected) {
    SCOPED_TRACE(query);
    std::string output;
    ASSERT_OK(RunCli({"query", query}, &output));
    std::vector<Row> actual;
    Row row;
    std::istringstream stream(output);
    std::string line;
    while (std::getline(stream, line)) {
      if (line == "---" || line.empty()) continue;
      const auto tab = line.find('\t');
      ASSERT_NE(tab, std::string::npos) << output;
      ASSERT_LT(row.size(), columns.size());
      ASSERT_EQ(line.substr(0, tab), columns[row.size()]) << output;
      row.push_back(line.substr(tab + 1));
      if (row.size() == columns.size()) {
        actual.push_back(row);
        row.clear();
      }
    }
    ASSERT_TRUE(row.empty());
    ASSERT_EQ(actual, expected);
  }

  void CheckOutput(const std::string& output, const std::vector<std::string>& columns,
                   const Rows& expected) {
    Rows actual;
    Row row;
    std::istringstream stream(output);
    std::string line;
    while (std::getline(stream, line)) {
      if (line == "---" || line.empty()) {
        continue;
      }
      const auto tab = line.find('\t');
      ASSERT_NE(tab, std::string::npos) << output;
      ASSERT_LT(row.size(), columns.size());
      ASSERT_EQ(line.substr(0, tab), columns[row.size()]) << output;
      row.push_back(line.substr(tab + 1));
      if (row.size() == columns.size()) {
        actual.insert(row);
        row.clear();
      }
    }
    ASSERT_TRUE(row.empty()) << output;
    ASSERT_EQ(actual, expected);
  }

  void SeedDictionaryEntry(int64_t packed, int64_t graph, const std::string& text) {
    client::TableHandle table;
    ASSERT_OK(table.Open(client::YBTableName(
        YQL_DATABASE_CQL, kNamespace, "rdf_idmap"), client_.get()));
    auto session = client_->NewSession(MonoDelta::FromSeconds(30));
    auto op = table.NewInsertOp(session->arena());
    QLAddInt64HashValue(op->mutable_request(), packed);
    table.AddInt64ColumnValue(op->mutable_request(), "gmap_id", graph);
    table.AddStringColumnValue(op->mutable_request(), "uri", text);
    session->Apply(op);
    ASSERT_OK(session->TEST_Flush());
  }

  void CheckPhysicalRows(size_t expected) {
    for (int perm = 0; perm < 6; ++perm) {
      client::TableHandle table;
      ASSERT_OK(table.Open(client::YBTableName(
          YQL_DATABASE_CQL, kNamespace, RdfYBModel::kTableNames[perm]), client_.get()));
      client::TableIteratorOptions options;
      options.columns = {"c0", "c1", "c2"};
      size_t count = 0;
      for (const auto& row : client::TableRange(table, options)) {
        ASSERT_NE(row.column(0).int64_value(), 0);
        ++count;
      }
      ASSERT_EQ(count, expected) << "permutation " << perm;
    }
  }

  void CheckBlankDocumentScope(bool v2) {
    const auto path = GetTestPath("blank-scope.nt");
    std::ofstream file(path);
    file << "_:b <urn:example:self> _:b .\n_:b <urn:example:name> \"value\" .\n";
    file.close();
    const std::string flag = std::string("--mdb_dictionary_v2=") + (v2 ? "true" : "false");
    std::string output;
    ASSERT_OK(RunCli({flag, "import", path}, &output, true));
    ASSERT_OK(RunCli({flag, "import", path}, &output, true));
    ASSERT_OK(RunCli({flag, "query",
        "SELECT ?name WHERE { ?b <urn:example:self> ?b . ?b <urn:example:name> ?name }"},
        &output, true));
    ASSERT_NO_FATALS(CheckOutput(output, {"name"}, {{"value"}, {"value"}}));
    ASSERT_NO_FATALS(CheckPhysicalRows(4));
    const std::string insert = "INSERT DATA { _:b <urn:example:self> _:b . "
        "_:b <urn:example:name> \"inserted\" . }";
    ASSERT_OK(RunCli({flag, "query", insert}, &output, true));
    ASSERT_OK(RunCli({flag, "query", insert}, &output, true));
    ASSERT_OK(RunCli({flag, "query",
        "SELECT ?name WHERE { ?b <urn:example:self> ?b . ?b <urn:example:name> ?name }"},
        &output, true));
    ASSERT_NO_FATALS(CheckOutput(output, {"name"},
        {{"value"}, {"value"}, {"inserted"}, {"inserted"}}));
    ASSERT_NO_FATALS(CheckPhysicalRows(8));
  }

  void RunScaleBaseline(bool transactions) {
    const auto* python = std::getenv("MDB_SCALE_PYTHON");
    const auto* driver = std::getenv("MDB_SCALE_DRIVER");
    const auto* dataset = std::getenv("MDB_SCALE_DATASET");
    const auto* output_dir = std::getenv("MDB_SCALE_OUTPUT_DIR");
    ASSERT_NE(python, nullptr);
    ASSERT_NE(driver, nullptr);
    ASSERT_NE(dataset, nullptr);
    ASSERT_NE(output_dir, nullptr);
    std::string output, error;
    const auto status = Subprocess::Call({
        python, driver, "--cli", cli_, "--masters", cluster_->GetMasterAddresses(),
        "--namespace", kNamespace, "--dataset", dataset, "--output", output_dir,
        "--parent-pid", std::to_string(getpid()), "--transactions",
        transactions ? "true" : "false"}, &output, &error);
    LOG(INFO) << "Scale baseline results:\n" << output;
    ASSERT_TRUE(status.ok()) << status << error;
    ASSERT_NO_FATALS(CheckPhysicalRows(25000));
  }

  void RunLeapfrogScale(const std::string& shape) {
    const auto* python = std::getenv("MDB_SCALE_PYTHON");
    const auto* driver = std::getenv("MDB_LEAPFROG_SCALE_DRIVER");
    const auto* directory = std::getenv("MDB_LEAPFROG_SCALE_OUTPUT");
    ASSERT_NE(python, nullptr);
    ASSERT_NE(driver, nullptr);
    ASSERT_NE(directory, nullptr);
    ASSERT_OK(client_->CreateNamespaceIfNotExists(kNamespace, YQL_DATABASE_CQL));
    for (const auto* name : RdfYBModel::kTableNames) {
      client::YBSchemaBuilder schema;
      schema.AddColumn("c0")->Type(DataType::INT64)->HashPrimaryKey()->NotNull();
      schema.AddColumn("c1")->Type(DataType::INT64)->PrimaryKey()->NotNull();
      schema.AddColumn("c2")->Type(DataType::INT64)->PrimaryKey()->NotNull();
      client::TableHandle table;
      ASSERT_OK(table.Create(
          client::YBTableName(YQL_DATABASE_CQL, kNamespace, name), 4, client_.get(), &schema));
    }
    std::string output, error;
    const auto status = Subprocess::Call(
        {python, driver, "--cli", cli_, "--masters", cluster_->GetMasterAddresses(), "--namespace",
         kNamespace, "--shape", shape, "--output", std::string(directory) + "/" + shape},
        &output, &error);
    LOG(INFO) << "Leapfrog scale and memory results:\n" << output;
    ASSERT_TRUE(status.ok()) << status << error;
    ASSERT_NO_FATALS(CheckPhysicalRows(25000));
  }

  std::string cli_;
  std::string last_cli_error_;
  Rows expected_;
};

TEST_F(MdbFileE2ETest, CliHashJoinOptInPreservesBagAndRouting) {
  const auto path = GetTestPath("hash-chain.nt");
  std::ofstream file(path);
  const auto key = [](int i) { return "https://example.org/hash/key/" + std::to_string(i); };
  Rows expected;
  for (int i = 0; i < 512; ++i) {
    file << '<' << Node(i) << "> <" << Predicate("left") << "> <" << key(i / 2) << "> .\n";
    const auto value = "value-" + std::to_string(i);
    file << '<' << key(i / 2) << "> <" << Predicate("right") << "> \"" << value << "\" .\n";
    expected.insert({value});
    expected.insert({value});
  }
  for (int i = 0; i < 1024; ++i) {
    file << "<https://example.org/hash/noise/" << i << "> <" << Predicate("noise")
         << "> <https://example.org/hash/irrelevant> .\n";
  }
  file.close();
  ASSERT_FALSE(file.fail());
  std::string output;
  ASSERT_OK(RunCli({"import", path}, &output));
  const std::string left = "?s <" + Predicate("left") + "> ?m";
  const std::string right = "?m <" + Predicate("right") + "> ?o";
  for (int mode = 0; mode < 6; ++mode) {
    SCOPED_TRACE(mode);
    const bool reversed = mode == 4;
    const std::string query = "SELECT ?o WHERE { " +
        (reversed ? right + " . " + left : left + " . " + right) + " }";
    std::vector<std::string> args{
        mode == 0 ? "--mdb_enable_hash_join=false" : "--mdb_enable_hash_join=true",
        mode == 2 ? "--mdb_collect_statistics=false" : "--mdb_collect_statistics=true"};
    if (mode == 3) args.push_back("--mdb_hash_join_max_estimated_build_bytes=1");
    if (mode == 5) args.push_back("--mdb_hash_join_max_build_bytes=1");
    const auto flags = args;
    args.insert(args.end(), {"query", query});
    if (mode == 5) {
      ASSERT_FALSE(RunCli(args, &output).ok());
      ASSERT_TRUE(output.empty()) << output;
      ASSERT_NE(last_cli_error_.find("MDB hash join selected:"), std::string::npos);
      ASSERT_NE(last_cli_error_.find("HashJoin build byte limit exceeded"), std::string::npos);
      const auto query_file = GetTestPath("hash-byte-limit.rq");
      std::ofstream query_output(query_file);
      query_output << query;
      query_output.close();
      ASSERT_FALSE(query_output.fail());
      auto batch_args = flags;
      batch_args.insert(batch_args.end(), {"query-batch", query_file, query_file});
      ASSERT_FALSE(RunCli(batch_args, &output).ok());
      ASSERT_TRUE(output.empty()) << output;
      ASSERT_NE(last_cli_error_.find("HashJoin build byte limit exceeded"), std::string::npos);
      ASSERT_NE(last_cli_error_.find("Batch query 1 failed"), std::string::npos);
      LOG(INFO) << "MDB_CLI_HASH byte_limit=1 query_and_batch_failed_without_rows";
      continue;
    }
    ASSERT_OK(RunCli(args, &output));
    ASSERT_NO_FATALS(CheckOutput(output, {"o"}, expected));
    const bool selected = last_cli_error_.find("MDB hash join selected:") != std::string::npos;
    ASSERT_EQ(selected, mode == 1) << last_cli_error_;
    const auto routed_one = last_cli_error_.find("MDB tablet-aware query: fragments=1");
    if (mode == 1) {
      ASSERT_NE(routed_one, std::string::npos) << last_cli_error_;
    } else {
      ASSERT_EQ(routed_one, std::string::npos) << last_cli_error_;
    }
    LOG(INFO) << "MDB_CLI_HASH mode=" << mode << " selected=" << selected
              << " rows=" << expected.size();
  }
}

TEST_F(MdbFileE2ETest, CliHashColdReadGuardExecutesZeroOverlap) {
  const auto path = GetTestPath("cold-zero-overlap.nt");
  const auto key = [](int i) { return "https://example.org/cold/key/" + std::to_string(i); };
  std::ofstream file(path);
  for (int i = 0; i < 768; ++i) {
    file << '<' << Node(i) << "> <" << Predicate("left") << "> <" << key(i % 96) << "> .\n";
    file << '<' << key(i % 96) << "> <" << Predicate("noise")
         << "> <https://example.org/cold/noise/" << i << "> .\n";
  }
  for (int i = 0; i < 1280; ++i) {
    file << "<https://example.org/cold/unrelated/" << i << "> <" << Predicate("right")
         << "> <https://example.org/cold/value/" << i << "> .\n";
  }
  file.close();
  ASSERT_FALSE(file.fail());
  std::string output;
  ASSERT_OK(RunCli({"import", path}, &output));
  const auto query = "SELECT ?o WHERE { ?s <" + Predicate("left") + "> ?m . ?m <" +
      Predicate("right") + "> ?o }";
  for (int mode = 0; mode < 3; ++mode) {
    SCOPED_TRACE(mode);
    std::vector<std::string> args{
        mode == 0 ? "--mdb_enable_hash_join=false" : "--mdb_enable_hash_join=true",
        "--mdb_collect_statistics=true"};
    if (mode == 2) args.push_back("--mdb_hash_join_max_build_bytes=1");
    args.insert(args.end(), {"query", query});
    const auto status = RunCli(args, &output);
    if (mode == 2) {
      ASSERT_FALSE(status.ok());
      ASSERT_TRUE(output.empty());
      ASSERT_NE(last_cli_error_.find("HashJoin build byte limit exceeded"), std::string::npos);
    } else {
      ASSERT_OK(status);
      ASSERT_NO_FATALS(CheckOutput(output, {"o"}, {}));
      const auto trace = last_cli_error_.find("MDB tablet-aware query: fragments=1 rows=0");
      if (mode == 1) {
        ASSERT_NE(trace, std::string::npos) << last_cli_error_;
      } else {
        ASSERT_EQ(trace, std::string::npos) << last_cli_error_;
      }
    }
    ASSERT_EQ(last_cli_error_.find("MDB hash join selected:") != std::string::npos, mode != 0);
  }
}

TEST_F(MdbFileE2ETest, CliKnownInputStatisticsPreserveLocalAndTabletBags) {
  const auto path = GetTestPath("known-input.nt");
  const auto key = [](int i) { return "https://example.org/input/key/" + std::to_string(i); };
  std::ofstream file(path);
  for (int i = 0; i < 768; ++i) {
    file << '<' << Node(i) << "> <" << Predicate("left") << "> <" << key(i % 96) << "> .\n";
    file << '<' << key(i % 96) << "> <" << Predicate("noise")
         << "> <https://example.org/input/noise/" << i << "> .\n";
  }
  Rows expected;
  for (int i = 0; i < 1280; ++i) {
    const auto subject =
        i < 48 ? key(i / 2) : "https://example.org/input/unrelated/" + std::to_string(i);
    const auto value = "value-" + std::to_string(i);
    file << '<' << subject << "> <" << Predicate("right") << "> \"" << value << "\" .\n";
    if (i < 48) {
      for (int repeat = 0; repeat < 8; ++repeat) {
        expected.insert({value});
      }
    }
  }
  file.close();
  ASSERT_FALSE(file.fail());
  std::string output;
  ASSERT_OK(RunCli({"import", path}, &output));
  const auto left = "?s <" + Predicate("left") + "> ?m";
  const auto right = "?m <" + Predicate("right") + "> ?o";
  for (bool statistics : {false, true}) {
    for (bool reversed : {false, true}) {
      const auto query = "SELECT ?o WHERE { " +
                         (reversed ? right + " . " + left : left + " . " + right) +
                         " } LIMIT 100000";
      ASSERT_OK(RunCli({"--mdb_enable_hash_join=false",
          statistics ? "--mdb_collect_statistics=true" : "--mdb_collect_statistics=false",
          "query", query}, &output));
      ASSERT_NO_FATALS(CheckOutput(output, {"o"}, expected));
      ASSERT_EQ(last_cli_error_.find("MDB tablet-aware query:"), std::string::npos);
    }
  }
  ASSERT_OK(RunCli({"--mdb_enable_hash_join=false", "--mdb_collect_statistics=true", "query",
      "SELECT ?o WHERE { " + left + " . " + right + " }"}, &output));
  ASSERT_NO_FATALS(CheckOutput(output, {"o"}, expected));
  ASSERT_NE(last_cli_error_.find("MDB tablet-aware query: fragments="), std::string::npos);
}

TEST_F(MdbFileE2ETest, CliBatchReusesStatisticsWithFreshQueryState) {
  ASSERT_NO_FATALS(ImportFixture(8));
  const std::vector<std::string> queries{
      "PREFIX f: <https://example.org/file/>\nSELECT ?person WHERE { "
          "?person f:knows ?friend . ?friend f:name ?name }",
      "SELECT * WHERE { <" + Node(0) + "> <" + Predicate("knows") + "> ?next }",
      "SELECT ?missing WHERE { <" + Node(7) + "> <" + Predicate("knows") + "> ?missing }",
      "SELECT ?literal WHERE { ?literal <" + Predicate("name") + "> \"Person 3\" }",
      "SELECT DISTINCT ?person WHERE { ?person <" + Predicate("knows") + "> ?friend }"};
  std::vector<std::string> paths;
  for (size_t i = 0; i < queries.size(); ++i) {
    const auto path = GetTestPath("batch-" + std::to_string(i) + ".rq");
    std::ofstream file(path);
    file << queries[i];
    file.close();
    ASSERT_FALSE(file.fail());
    paths.push_back(path);
  }
  const std::vector<std::vector<std::string>> columns{
      {"person"}, {"next"}, {"missing"}, {"literal"}, {"person"}};
  const std::vector<Rows> expected{
      {{Node(0)}, {Node(0)}, {Node(1)}}, {{Node(1)}, {Node(2)}}, {},
      {{Node(3)}}, {{Node(0)}, {Node(1)}}};
  for (int phase = 0; phase < 3; ++phase) {
    std::vector<std::string> args{
        phase == 0 ? "--mdb_collect_statistics=false" : "--mdb_collect_statistics=true",
        "query-batch"};
    args.insert(args.end(), paths.begin(), paths.end());
    std::string output;
    ASSERT_OK(RunCli(args, &output, phase == 2));
    const std::string collected = "MDB statistics collected:";
    const auto first = last_cli_error_.find(collected);
    if (phase == 0) {
      ASSERT_EQ(first, std::string::npos);
    } else {
      ASSERT_NE(first, std::string::npos);
      ASSERT_EQ(last_cli_error_.find(collected, first + 1), std::string::npos);
      ASSERT_NE(last_cli_error_.find("MDB statistics collected: rows=12"), std::string::npos);
    }
    size_t offset = 0;
    for (size_t i = 0; i < queries.size(); ++i) {
      const auto begin = "=== query " + std::to_string(i + 1) + " ===\n";
      const auto end = "=== end query " + std::to_string(i + 1) + " ===\n";
      ASSERT_EQ(output.find(begin, offset), offset) << output;
      const auto finish = output.find(end, offset + begin.size());
      ASSERT_NE(finish, std::string::npos) << output;
      ASSERT_NO_FATALS(CheckOutput(output.substr(offset + begin.size(),
          finish - offset - begin.size()), columns[i], expected[i]));
      offset = finish + end.size();
    }
    ASSERT_EQ(offset, output.size());
  }
}

TEST_F(MdbFileE2ETest, CliBatchRejectsInvalidInputAndStopsOnFailure) {
  ASSERT_NO_FATALS(ImportFixture(8));
  const auto valid = GetTestPath("valid.rq");
  const auto invalid = GetTestPath("invalid.rq");
  {
    std::ofstream file(valid);
    file << "SELECT ?p WHERE { <" << Node(0) << "> ?p ?o }";
  }
  for (const auto& query : std::vector<std::string>{
      "INSERT DATA { <https://example.org/s> <https://example.org/p> <https://example.org/o> }",
      "SELECT WHERE {", "", "ASK { ?s ?p ?o }"}) {
    std::ofstream file(invalid);
    file << query;
    file.close();
    std::string output;
    ASSERT_FALSE(RunCli({"query-batch", valid, invalid}, &output).ok());
    ASSERT_TRUE(output.empty());
    ASSERT_EQ(last_cli_error_.find("Connecting to masters"), std::string::npos);
  }
  std::string output;
  ASSERT_FALSE(RunCli({"query-batch", GetTestPath("absent.rq")}, &output).ok());
  ASSERT_TRUE(output.empty());
  ASSERT_NE(last_cli_error_.find("Cannot read query file"), std::string::npos);
  ASSERT_FALSE(RunCli({"query-batch"}, &output).ok());
  ASSERT_TRUE(output.empty());
  {
    std::ofstream file(invalid);
    file << "SELECT ?s WHERE { ?s undeclared:predicate ?o }";
  }
  ASSERT_FALSE(RunCli({"--mdb_collect_statistics=true", "query-batch", valid, invalid, valid},
                     &output).ok());
  ASSERT_NE(output.find("=== end query 1 ==="), std::string::npos);
  ASSERT_EQ(output.find("=== query 2 ==="), std::string::npos);
  ASSERT_EQ(output.find("=== query 3 ==="), std::string::npos);
  ASSERT_NE(last_cli_error_.find("Batch query 2 failed"), std::string::npos);
  ASSERT_NO_FATALS(CheckPhysicalRows(12));
}

TEST_F(MdbFileE2ETest, CliStatisticsPreserveJoinAcrossProcesses) {
  ASSERT_NO_FATALS(ImportFixture(8));
  const std::string query = "SELECT ?s ?name WHERE { ?s <" + Predicate("knows") +
      "> ?friend . ?friend <" + Predicate("name") + "> ?name }";
  const Rows expected{{Node(0), "Person 1"}, {Node(0), "Person 2"}, {Node(1), "Person 2"}};
  ASSERT_NO_FATALS(CheckRows(query, {"s", "name"}, expected));
  ASSERT_NO_FATALS(CheckRows(query, {"s", "name"}, expected, false, true));
  ASSERT_NE(last_cli_error_.find("MDB statistics collected: rows=12"), std::string::npos)
      << last_cli_error_;
}


TEST_F(MdbFileE2ETest, CliSameLexemeIriAndLiteralSurviveRestart) {
  const auto path = GetTestPath("same-lexeme.nt");
  std::ofstream file(path);
  file << "<https://example.org/subject> <https://example.org/predicate> "
          "<https://example.org/value> .\n"
          "<https://example.org/subject> <https://example.org/predicate> "
          "\"https://example.org/value\" .\n";
  file.close();
  ASSERT_FALSE(file.fail());
  std::string output;
  ASSERT_OK(RunCli({"import", path}, &output));
  ASSERT_NO_FATALS(CheckPhysicalRows(2));
  ASSERT_NO_FATALS(CheckRows(
      "SELECT ?o WHERE { <https://example.org/subject> <https://example.org/predicate> ?o }",
      {"o"}, {{"https://example.org/value"}, {"https://example.org/value"}}));
  ASSERT_NO_FATALS(CheckRows(
      "SELECT DISTINCT ?o WHERE { ?s <https://example.org/predicate> ?o }",
      {"o"}, {{"https://example.org/value"}, {"https://example.org/value"}}));
  ASSERT_NO_FATALS(CheckRows(
      "SELECT ?s WHERE { ?s <https://example.org/predicate> <https://example.org/value> }",
      {"s"}, {{"https://example.org/subject"}}));
  ASSERT_NO_FATALS(CheckRows(
      "SELECT ?s WHERE { ?s <https://example.org/predicate> \"https://example.org/value\" }",
      {"s"}, {{"https://example.org/subject"}}));
  ASSERT_OK(RunCli({"import", path}, &output));
  ASSERT_NO_FATALS(CheckPhysicalRows(2));
}


TEST_F(MdbFileE2ETest, CliEmptyLiteralSurvivesRestartAndAppend) {
  const auto path = GetTestPath("empty-literal.nt");
  std::ofstream file(path);
  file << "<https://example.org/subject> <https://example.org/predicate> \"\" .\n";
  file.close();
  ASSERT_FALSE(file.fail());
  std::string output;
  ASSERT_OK(RunCli({"import", path}, &output));
  ASSERT_NO_FATALS(CheckRows("SELECT ?o WHERE { ?s ?p ?o }", {"o"}, {{""}}));
  ASSERT_NO_FATALS(CheckRows(
      "SELECT ?s WHERE { ?s <https://example.org/predicate> \"\" }",
      {"s"}, {{"https://example.org/subject"}}));
  ASSERT_OK(RunCli({"import", path}, &output));
  ASSERT_NO_FATALS(CheckPhysicalRows(1));
}

TEST_F(MdbFileE2ETest, CliRejectsAliasedLegacyDictionary) {
  std::string output;
  const auto path = GetTestPath("legacy-alias.nt");
  std::ofstream file(path);
  file << "<https://example.org/s> <https://example.org/p> "
          "<https://example.org/aliased> .\n"
          "<https://example.org/s> <https://example.org/p> "
          "\"https://example.org/aliased\" .\n";
  file.close();
  ASSERT_FALSE(file.fail());
  ASSERT_OK(RunCli({"import", path}, &output));
  client::TableHandle table;
  ASSERT_OK(table.Open(client::YBTableName(
      YQL_DATABASE_CQL, kNamespace, "rdf_idmap"), client_.get()));
  const std::string text = "https://example.org/aliased";
  auto session = client_->NewSession(MonoDelta::FromSeconds(30));
  client::TableIteratorOptions options;
  options.columns = {"pack_id", "uri"};
  std::vector<int64_t> packed_ids;
  for (const auto& row : client::TableRange(table, options)) {
    if (row.column(1).string_value() == text) {
      packed_ids.push_back(row.column(0).int64_value());
    }
  }
  ASSERT_EQ(packed_ids.size(), 2);
  for (const auto packed : packed_ids) {
    auto op = table.NewInsertOp(session->arena());
    QLAddInt64HashValue(op->mutable_request(), packed);
    table.AddInt64ColumnValue(op->mutable_request(), "gmap_id", 3);
    table.AddStringColumnValue(op->mutable_request(), "uri", text);
    session->Apply(op);
  }
  ASSERT_OK(session->TEST_Flush());
  ASSERT_FALSE(RunCli({"query", "SELECT ?s WHERE { ?s ?p ?o }"}, &output).ok());
  ASSERT_NE(last_cli_error_.find("Conflicting persisted RDF dictionary entries"),
            std::string::npos) << last_cli_error_;
}


TEST_F(MdbFileE2ETest, CliIriOrderByAfterRestartAndPagination) {
  const auto path = GetTestPath("iri-order.nt");
  std::ofstream file(path);
  for (const auto& pair : std::vector<std::pair<int, std::string>>{
           {0, "z"}, {1, "a"}, {2, "m"}, {3, "z"}}) {
    file << '<' << Node(pair.first) << "> <" << Predicate("key") << "> "
         << "<https://example.org/" << pair.second << "> .\n";
  }
  file.close();
  ASSERT_FALSE(file.fail());
  std::string output;
  ASSERT_OK(RunCli({"import", path}, &output));
  const std::string body = " WHERE { ?s <" + Predicate("key") + "> ?o }";
  ASSERT_NO_FATALS(CheckOrderedRows("SELECT ?o" + body + " ORDER BY ?o", {"o"},
      {{"https://example.org/a"}, {"https://example.org/m"},
       {"https://example.org/z"}, {"https://example.org/z"}}));
  ASSERT_NO_FATALS(CheckOrderedRows("SELECT ?o" + body + " ORDER BY DESC(?o)", {"o"},
      {{"https://example.org/z"}, {"https://example.org/z"},
       {"https://example.org/m"}, {"https://example.org/a"}}));
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT ?o" + body + " ORDER BY ?o LIMIT 2 OFFSET 1", {"o"},
      {{"https://example.org/m"}, {"https://example.org/z"}}));
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT ?s ?o" + body + " ORDER BY ?o DESC(?s)", {"s", "o"},
      {{Node(1), "https://example.org/a"}, {Node(2), "https://example.org/m"},
       {Node(3), "https://example.org/z"}, {Node(0), "https://example.org/z"}}));
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT DISTINCT ?o" + body + " ORDER BY ?o", {"o"},
      {{"https://example.org/a"}, {"https://example.org/m"}, {"https://example.org/z"}}));
  ASSERT_NO_FATALS(CheckOrderedRows("SELECT ?o" + body + " ORDER BY ?o LIMIT 0", {"o"}, {}));
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT ?o" + body + " ORDER BY ?o OFFSET 20", {"o"}, {}));
}

TEST_F(MdbFileE2ETest, CliValuesIriOrderingAndUnboundKeys) {
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT ?v WHERE { VALUES ?v { <https://example.org/z> <https://example.org/a> "
      "<https://example.org/m> } } ORDER BY ?v", {"v"},
      {{"https://example.org/a"}, {"https://example.org/m"}, {"https://example.org/z"}}));
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT ?label WHERE { VALUES (?key ?label) { (UNDEF \"none\") "
      "(<https://example.org/z> \"last\") (<https://example.org/a> \"first\") } } "
      "ORDER BY ?key", {"label"}, {{"none"}, {"first"}, {"last"}}));
}


TEST_F(MdbFileE2ETest, CliLongLiteralOrderingUsesLexicalValues) {
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT ?v WHERE { VALUES ?v { \"z-long-literal\" \"a-long-literal\" "
      "\"m-long-literal\" } } ORDER BY ?v", {"v"},
      {{"a-long-literal"}, {"m-long-literal"}, {"z-long-literal"}}));
}

TEST_F(MdbFileE2ETest, CliUnsupportedOrderExpressionReturnsError) {
  std::string output;
  ASSERT_FALSE(RunCli({"query",
      "SELECT ?v WHERE { VALUES ?v { <https://example.org/z> <https://example.org/a> } } "
      "ORDER BY STR(?v)"}, &output).ok());
  ASSERT_NE(last_cli_error_.find("ORDER BY expressions are not supported"),
            std::string::npos) << last_cli_error_;
}


TEST_F(MdbFileE2ETest, CliValuesJoinStoredIrisAndLiteralsAfterRestart) {
  ASSERT_NO_FATALS(ImportFixture(4));
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT ?name WHERE { VALUES ?person { <" + Node(1) + "> <" + Node(2) +
      "> } ?person <" + Predicate("name") + "> ?name } ORDER BY ?name",
      {"name"}, {{"Person 1"}, {"Person 2"}}));
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT ?person WHERE { VALUES ?name { \"Person 2\" \"Person 1\" } "
      "?person <" + Predicate("name") + "> ?name } ORDER BY ?person",
      {"person"}, {{Node(1)}, {Node(2)}}));
}


TEST_F(MdbFileE2ETest, CliValuesJoinPreservesUnboundDuplicateAndEmptyBags) {
  ASSERT_NO_FATALS(ImportFixture(4));
  const std::string triple = "?person <" + Predicate("name") + "> ?name";
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT ?name WHERE { VALUES ?person { UNDEF <" + Node(1) + "> } " +
      triple + " } ORDER BY ?name", {"name"},
      {{"Person 0"}, {"Person 1"}, {"Person 1"}, {"Person 2"}, {"Person 3"}}));
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT ?person WHERE { " + triple +
      " VALUES ?name { \"Person 1\" \"Person 1\" } } ORDER BY ?person",
      {"person"}, {{Node(1)}, {Node(1)}}));
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT ?name WHERE { VALUES ?person { } " + triple + " } ORDER BY ?name", {"name"}, {}));
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT ?name WHERE { VALUES ?person { <https://example.org/missing> } " +
      triple + " } ORDER BY ?name", {"name"}, {}));
}


TEST_F(MdbFileE2ETest, CliSparseLegacyDictionaryCanAppend) {
  std::string output;
  ASSERT_OK(RunCli({"query", "SELECT ?s WHERE { ?s ?p ?o }"}, &output));
  // Seed the existing dictionary wire format with a valid sparse graph ID.
  ASSERT_NO_FATALS(SeedDictionaryEntry(0x3000eff649b329cdLL, int64_t{1} << 40,
                                      "https://example.org/sparse"));
  const auto path = GetTestPath("sparse-append.nt");
  std::ofstream file(path);
  file << "<https://example.org/sparse> <https://example.org/predicate> \"ok\" .\n";
  file.close();
  ASSERT_FALSE(file.fail());
  ASSERT_OK(RunCli({"import", path}, &output));
  ASSERT_NO_FATALS(CheckPhysicalRows(1));
  ASSERT_NO_FATALS(CheckRows(
      "SELECT ?s ?p ?o WHERE { ?s ?p ?o }", {"s", "p", "o"},
      {{"https://example.org/sparse", "https://example.org/predicate", "ok"}}));
}

TEST_F(MdbFileE2ETest, CliOutOfRangeDictionaryIdReturnsCorruption) {
  std::string output;
  ASSERT_OK(RunCli({"query", "SELECT ?s WHERE { ?s ?p ?o }"}, &output));
  ASSERT_NO_FATALS(SeedDictionaryEntry(0x3000eff649b329cdLL, -1,
                                      "https://example.org/sparse"));
  ASSERT_FALSE(RunCli({"query", "SELECT ?s WHERE { ?s ?p ?o }"}, &output).ok());
  ASSERT_NE(last_cli_error_.find("Conflicting persisted RDF dictionary entries"),
            std::string::npos) << last_cli_error_;
}

TEST_F(MdbFileE2ETest, CliImportAndFullScan) {
  ASSERT_NO_FATALS(ImportFixture());
  ASSERT_NO_FATALS(CheckPhysicalRows(36));
  ASSERT_NO_FATALS(CheckRows("SELECT ?s ?p ?o WHERE { ?s ?p ?o }", {"s", "p", "o"}, expected_));
}

TEST_F(MdbFileE2ETest, CliFixedIriAndMissing) {
  ASSERT_NO_FATALS(ImportFixture());
  for (bool statistics : {false, true}) {
    ASSERT_NO_FATALS(CheckRows("SELECT ?friend WHERE { <" + Node(0) + "> <" +
        Predicate("knows") + "> ?friend }", {"friend"}, {{Node(1)}, {Node(2)}}, false, statistics));
    ASSERT_NO_FATALS(CheckRows("SELECT ?friend WHERE { <" + Node(31) + "> <" +
        Predicate("knows") + "> ?friend }", {"friend"}, {}, false, statistics));
  }
}

TEST_F(MdbFileE2ETest, CliCrossTabletJoinAndProjection) {
  ASSERT_NO_FATALS(ImportFixture());
  const auto body = " WHERE { ?person <" + Predicate("knows") + "> ?friend . ?friend <" +
      Predicate("name") + "> ?name }";
  ASSERT_NO_FATALS(CheckRows("SELECT ?person ?name" + body, {"person", "name"},
      {{Node(0), "Person 1"}, {Node(0), "Person 2"}, {Node(1), "Person 2"}}));
  ASSERT_NO_FATALS(CheckRows("SELECT DISTINCT ?name" + body, {"name"},
      {{"Person 1"}, {"Person 2"}}));
}

TEST_F(MdbFileE2ETest, CliRestartAndAppend) {
  ASSERT_NO_FATALS(ImportFixture());
  const auto path = GetTestPath("append.nt");
  std::ofstream file(path);
  file << '<' << Node(32) << "> <" << Predicate("name") << "> \"Person 32\" .\n"
       << '<' << Node(0) << "> <" << Predicate("knows") << "> <" << Node(32) << "> .\n";
  file.close();
  ASSERT_FALSE(file.fail());
  std::string output;
  ASSERT_OK(RunCli({"import", path}, &output));
  ASSERT_NO_FATALS(CheckPhysicalRows(38));
  expected_.insert({Node(32), Predicate("name"), "Person 32"});
  expected_.insert({Node(0), Predicate("knows"), Node(32)});
  ASSERT_NO_FATALS(CheckRows("SELECT ?s ?p ?o WHERE { ?s ?p ?o }", {"s", "p", "o"}, expected_));
}

TEST_F(MdbFileE2ETest, CliReimportIsIdempotent) {
  ASSERT_NO_FATALS(ImportFixture());
  std::string output;
  ASSERT_OK(RunCli({"import", GetTestPath("people.nt")}, &output));
  ASSERT_NO_FATALS(CheckPhysicalRows(36));
  ASSERT_NO_FATALS(CheckRows("SELECT ?s ?p ?o WHERE { ?s ?p ?o }", {"s", "p", "o"}, expected_));
}

TEST_F(MdbFileE2ETest, CliLiteralLookupAfterRestart) {
  ASSERT_NO_FATALS(ImportFixture());
  ASSERT_NO_FATALS(CheckRows("SELECT ?person WHERE { ?person <" + Predicate("name") +
      "> \"Person 17\" }", {"person"}, {{Node(17)}}));
  ASSERT_NO_FATALS(CheckRows("SELECT ?person WHERE { ?person <" + Predicate("description") +
      "> \"A long literal restored in a different process\" }", {"person"}, {{Node(0)}}));
}

TEST_F(MdbFileE2ETest, CliTransactionalImportAcrossBatches) {
  ASSERT_NO_FATALS(ImportFixture(140, true));
  ASSERT_NO_FATALS(CheckPhysicalRows(144));
  ASSERT_NO_FATALS(
      CheckRows("SELECT ?s ?p ?o WHERE { ?s ?p ?o }", {"s", "p", "o"}, expected_, true));
}

TEST_F(MdbFileE2ETest, CliMissingFileReturnsError) {
  std::string output;
  ASSERT_FALSE(RunCli({"import", GetTestPath("missing.nt")}, &output).ok());
}

TEST_F(MdbFileE2ETest, CliDs10000NonTransactional) {
  ASSERT_NO_FATALS(RunScaleBaseline(false));
}

TEST_F(MdbFileE2ETest, CliDs10000Transactional) {
  ASSERT_NO_FATALS(RunScaleBaseline(true));
}

TEST_F(MdbFileE2ETest, CliLeapfrogTriangleOptInPreservesBagAndFallback) {
  const auto path = GetTestPath("leapfrog-triangle.nt");
  std::ofstream file(path);
  ASSERT_TRUE(file.is_open());
  for (int x = 0; x < 12; ++x) {
    for (int y = 100; y < 102; ++y) {
      file << '<' << Node(x) << "> <" << Predicate("triangle-p") << "> <" << Node(y) << "> .\n";
    }
    for (int z = 200; z < 203; ++z) {
      file << '<' << Node(x) << "> <" << Predicate("triangle-r") << "> <" << Node(z) << "> .\n";
    }
  }
  for (int y = 100; y < 102; ++y) {
    for (int z = 200; z < 203; ++z) {
      file << '<' << Node(y) << "> <" << Predicate("triangle-q") << "> <" << Node(z) << "> .\n";
    }
  }
  file << '<' << Node(0) << "> <" << Predicate("triangle-p") << "> <" << Node(100) << "> .\n";
  file.close();
  ASSERT_FALSE(file.fail());
  std::string output;
  ASSERT_OK(RunCli({"import", path}, &output));
  ASSERT_NO_FATALS(CheckPhysicalRows(66));
  const auto body = "?x <" + Predicate("triangle-p") + "> ?y . ?y <" + Predicate("triangle-q") +
                    "> ?z . ?x <" + Predicate("triangle-r") + "> ?z";
  for (const bool enabled : {false, true}) {
    const auto flag = std::string("--mdb_enable_leapfrog_join=") + (enabled ? "true" : "false");
    for (const bool projected : {false, true}) {
      Rows expected;
      for (int x = 0; x < 12; ++x) {
        for (int y = 100; y < 102; ++y) {
          for (int z = 200; z < 203; ++z) {
            expected.insert(projected ? Row{Node(x), Node(z)} : Row{Node(x), Node(y), Node(z)});
          }
        }
      }
      const auto query =
          std::string(projected ? "SELECT ?x ?z WHERE { " : "SELECT ?x ?y ?z WHERE { ") + body +
          " }";
      ASSERT_OK(RunCli({flag, "query", query}, &output));
      ASSERT_NO_FATALS(
          CheckOutput(output, projected ? Row{"x", "z"} : Row{"x", "y", "z"}, expected));
      ASSERT_EQ(last_cli_error_.find("MDB Leapfrog triangle:") != std::string::npos, enabled);
      ASSERT_NE(last_cli_error_.find("MDB tablet-aware query: fragments=2"), std::string::npos);
    }
    const auto empty = "SELECT ?x ?z WHERE { ?x <" + Predicate("absent") + "> ?y . ?y <" +
                       Predicate("triangle-q") + "> ?z . ?x <" + Predicate("triangle-r") + "> ?z }";
    ASSERT_OK(RunCli({flag, "query", empty}, &output));
    ASSERT_NO_FATALS(CheckOutput(output, {"x", "z"}, {}));
    ASSERT_EQ(last_cli_error_.find("MDB Leapfrog triangle:") != std::string::npos, enabled);
    const auto chain = "SELECT ?x ?z WHERE { ?x <" + Predicate("triangle-p") + "> ?y . ?y <" +
                       Predicate("triangle-q") + "> ?z }";
    Rows expected;
    for (int x = 0; x < 12; ++x) {
      for (int y = 100; y < 102; ++y) {
        for (int z = 200; z < 203; ++z) expected.insert({Node(x), Node(z)});
      }
    }
    ASSERT_OK(RunCli({flag, "query", chain}, &output));
    ASSERT_NO_FATALS(CheckOutput(output, {"x", "z"}, expected));
    ASSERT_EQ(last_cli_error_.find("MDB Leapfrog triangle:"), std::string::npos);
  }
}

TEST_F(MdbFileE2ETest, YB_DEBUG_ONLY_TEST(CliLeapfrog25kSparseMemoryComparison)) {
  if (!std::getenv("MDB_LEAPFROG_SCALE_RUN")) GTEST_SKIP() << "Explicit scale run required";
  ASSERT_NO_FATALS(RunLeapfrogScale("sparse"));
}

TEST_F(MdbFileE2ETest, YB_DEBUG_ONLY_TEST(CliLeapfrog25kSkewMemoryComparison)) {
  if (!std::getenv("MDB_LEAPFROG_SCALE_RUN")) GTEST_SKIP() << "Explicit scale run required";
  ASSERT_NO_FATALS(RunLeapfrogScale("skew"));
}


TEST_F(MdbFileE2ETest, CliV2MetadataSurvivesRestartAndAppend) {
  const auto path = GetTestPath("metadata-v2.nt");
  std::ofstream file(path);
  file << "<urn:example:en> <urn:example:p> \"same\"@EN .\n"
          "<urn:example:en> <urn:example:p> \"same\"@en .\n"
          "<urn:example:fr> <urn:example:p> \"same\"@fr .\n"
          "<urn:example:typeA> <urn:example:p> \"same\"^^<urn:example:type:a> .\n"
          "<urn:example:typeB> <urn:example:p> \"same\"^^<urn:example:type:b> .\n"
          "<urn:example:plain> <urn:example:p> \"same\" .\n"
          "<urn:example:plain> <urn:example:p> \"same\""
          "^^<http://www.w3.org/2001/XMLSchema#string> .\n"
          "<urn:example:pipe> <urn:example:p> \"same|en\" .\n";
  file.close();
  ASSERT_TRUE(file.good());
  std::string output;
  ASSERT_OK(RunCli({"--mdb_dictionary_v2=true", "import", path}, &output));
  const std::vector<std::pair<std::string, std::string>> constants{
      {"\"same\"@en", "urn:example:en"}, {"\"same\"@FR", "urn:example:fr"},
      {"\"same\"^^<urn:example:type:a>", "urn:example:typeA"},
      {"\"same\"^^<urn:example:type:b>", "urn:example:typeB"},
      {"\"same\"", "urn:example:plain"}, {"\"same|en\"", "urn:example:pipe"}};
  for (const auto& [literal, subject] : constants) {
    ASSERT_OK(RunCli({"--mdb_dictionary_v2=true", "query",
        "SELECT ?s WHERE { ?s <urn:example:p> " + literal + " }"}, &output));
    ASSERT_NO_FATALS(CheckOutput(output, {"s"}, {{subject}}));
  }
  ASSERT_OK(RunCli({"--mdb_dictionary_v2=true", "query",
      "SELECT DISTINCT ?o WHERE { ?s <urn:example:p> ?o }"}, &output));
  ASSERT_NO_FATALS(CheckOutput(output, {"o"},
      {{"same"}, {"same"}, {"same"}, {"same"}, {"same"}, {"same|en"}}));
  const auto append_path = GetTestPath("metadata-append.nt");
  std::ofstream appended(append_path);
  appended << "<urn:example:new> <urn:example:p> \"same\"@en .\n";
  appended.close();
  ASSERT_OK(RunCli({"--mdb_dictionary_v2=true", "import", append_path}, &output));
  ASSERT_OK(RunCli({"--mdb_dictionary_v2=true", "query",
      "SELECT ?s WHERE { VALUES ?o { \"same\"@EN } ?s <urn:example:p> ?o }"}, &output));
  ASSERT_NO_FATALS(CheckOutput(output, {"s"}, {{"urn:example:en"}, {"urn:example:new"}}));
  ASSERT_NO_FATALS(CheckPhysicalRows(7));
  ASSERT_FALSE(RunCli({"query", "SELECT ?s WHERE { ?s ?p ?o }"}, &output).ok());
  ASSERT_NE(last_cli_error_.find("Namespace requires mdb_dictionary_v2"), std::string::npos);
}


TEST_F(MdbFileE2ETest, V2CliPipelineAndInsertShareCommittedDictionary) {
  const auto path = GetTestPath("shared-entrypoints.nt");
  std::ofstream file(path);
  file << "<urn:example:original> <urn:example:p> \"value\"@en .\n";
  file.close();
  ASSERT_TRUE(file.good());
  std::string output;
  ASSERT_OK(RunCli({"--mdb_dictionary_v2=true", "import", path}, &output, true));
  auto& model = RdfYBModel::Instance();
  const auto old_flag = FLAGS_mdb_dictionary_v2;
  const auto old_transactions = model.UseTransactions();
  auto cleanup = ScopeExit([&] {
    model.ClearAll();
    model.SetUseTransactions(old_transactions);
    FLAGS_mdb_dictionary_v2 = old_flag;
  });
  FLAGS_mdb_dictionary_v2 = true;
  model.SetUseTransactions(true);
  ASSERT_OK(model.Init(client_.get(), kNamespace));
  const Triple appended{"urn:example:appended", "urn:example:p", "value",
      TripleTermType::URI, TripleTermType::URI, TripleTermType::LITERAL, "", "en"};
  ImportPipeline first_pipeline;
  ASSERT_OK(model.BeginTransaction());
  ASSERT_RESULT(first_pipeline.ImportTriples({appended}));
  ASSERT_TRUE(model.HasTransaction());
  auto before = ASSERT_RESULT(model.Dictionary()->Entries());
  uint64_t append_id = 0;
  for (const auto& [id, term] : before) {
    if (term.lexical == appended.sub) append_id = id;
  }
  ASSERT_NE(append_id, uint64_t{0});
  ASSERT_OK(model.AbortTransaction());
  ASSERT_OK(RunCli({"--mdb_dictionary_v2=true", "query",
      "SELECT ?s WHERE { ?s <urn:example:p> \"value\"@en }"}, &output, true));
  ASSERT_NO_FATALS(CheckOutput(output, {"s"}, {{"urn:example:original"}}));
  // Reopen both the model and pipeline after failure, then reuse the committed identity.
  model.ClearAll();
  ASSERT_OK(model.Init(client_.get(), kNamespace));
  ImportPipeline second_pipeline;
  ASSERT_OK(model.BeginTransaction());
  ASSERT_RESULT(second_pipeline.ImportTriples({appended}));
  ASSERT_OK(model.CommitTransaction());
  for (const auto& [id, term] : ASSERT_RESULT(model.Dictionary()->Entries())) {
    if (term.lexical == appended.sub) ASSERT_EQ(id, append_id);
  }
  IdMap insert_cache;
  ImportStats stats;
  ASSERT_OK(InsertDataExecutor::Execute(
      "INSERT DATA { <urn:example:inserted> <urn:example:p> \"value\"@EN . }",
      insert_cache, stats));
  ASSERT_EQ(stats.triples_written, 1);
  ASSERT_OK(RunCli({"--mdb_dictionary_v2=true", "query",
      "SELECT ?s WHERE { ?s <urn:example:p> \"value\"@en }"}, &output, true));
  ASSERT_NO_FATALS(CheckOutput(output, {"s"},
      {{"urn:example:original"}, {"urn:example:appended"}, {"urn:example:inserted"}}));
  ASSERT_NO_FATALS(CheckPhysicalRows(3));
}


TEST_F(MdbFileE2ETest, CliInsertDotsSurviveRestartAndAppend) {
  std::string output;
  const std::string insert = R"(INSERT DATA {
    <https://example.org/insert/s> <https://example.org/insert/p> "version 1.2" .
    <https://example.org/insert/s> <https://example.org/insert/link>
      <https://example.org/insert/o> .
  })";
  ASSERT_OK(RunCli({"query", insert}, &output, true));
  ASSERT_NO_FATALS(CheckRows(
      "SELECT ?o WHERE { <https://example.org/insert/s> <https://example.org/insert/p> ?o }",
      {"o"}, {{"version 1.2"}}, true));
  ASSERT_NO_FATALS(CheckRows(
      "SELECT ?s WHERE { ?s <https://example.org/insert/p> \"version 1.2\" }",
      {"s"}, {{"https://example.org/insert/s"}}, true));
  ASSERT_OK(RunCli({"query", R"(INSERT DATA {
    <https://example.org/insert/new> <https://example.org/insert/p> "version 2.3" .
  })"}, &output, true));
  ASSERT_NO_FATALS(CheckRows(
      "SELECT ?s ?o WHERE { ?s <https://example.org/insert/p> ?o }",
      {"s", "o"}, {{"https://example.org/insert/s", "version 1.2"},
                      {"https://example.org/insert/new", "version 2.3"}}, true));
  ASSERT_NO_FATALS(CheckPhysicalRows(3));
}
TEST_F(MdbFileE2ETest, CliInsertRejectsMalformedTailBeforeWriting) {
  std::string output;
  const auto status = RunCli({"query", R"(INSERT DATA {
    <urn:example:s> <urn:example:p> "valid" .
    this is invalid .
  })"}, &output, true);
  ASSERT_FALSE(status.ok());
  ASSERT_NO_FATALS(CheckPhysicalRows(0));
}


TEST_F(MdbFileE2ETest, CliBlankLabelsHaveDocumentScopeLegacy) {
  ASSERT_NO_FATALS(CheckBlankDocumentScope(false));
}

TEST_F(MdbFileE2ETest, CliBlankLabelsHaveDocumentScopeV2) {
  ASSERT_NO_FATALS(CheckBlankDocumentScope(true));
}

TEST_F(MdbFileE2ETest, CliInsertCaseCommentsEscapesAndEnvelope) {
  std::string output;
  const auto query = R"(# A comment with an irrelevant brace }
    insert
    data { <urn:example:s> <urn:example:p> "a. } # quote\"" } # tail }
  )";
  ASSERT_OK(RunCli({"query", query}, &output, false));
  ASSERT_NO_FATALS(CheckRows(
      R"(SELECT ?s WHERE { ?s <urn:example:p> "a. } # quote\"" })",
      {"s"}, {{"urn:example:s"}}));
  for (const auto& invalid : {
      "INSERT DATA { <urn:example:bad> <urn:example:p> \"bad\" . } garbage",
      "INSERT DATA { <urn:example:bad> <urn:example:p> \"bad\" . } INSERT DATA {}",
      "INSERT DATA { <urn:example:bad> <urn:example:p> \"bad\"@ . }",
      "INSERT DATABASE { <urn:example:bad> <urn:example:p> \"bad\" . }"}) {
    ASSERT_FALSE(RunCli({"query", invalid}, &output).ok()) << invalid;
  }
  ASSERT_OK(RunCli({"query", "INSERT DATA {}"}, &output));
  ASSERT_NO_FATALS(CheckPhysicalRows(1));
}

TEST_F(MdbFileE2ETest, XmlLanguageAndDatatypeAreQueryableAfterRestart) {
  std::string output;
  ASSERT_OK(RunCli({"--mdb_dictionary_v2=true", "query",
      "SELECT ?s WHERE { ?s ?p ?o }"}, &output));
  const auto old_flag = FLAGS_mdb_dictionary_v2;
  auto& model = RdfYBModel::Instance();
  auto cleanup = ScopeExit([&] { model.ClearAll(); FLAGS_mdb_dictionary_v2 = old_flag; });
  FLAGS_mdb_dictionary_v2 = true;
  ASSERT_OK(model.Init(client_.get(), kNamespace));
  const auto path = GetTestPath("language.rdf");
  std::ofstream file(path);
  file << R"(<r:RDF xmlns:r="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                 xmlns:ex="https://example.org/#" xml:lang="en">
    <r:Description r:about="urn:example:s">
      <ex:p>  A &amp; B  </ex:p><ex:p xml:lang="fr">  A &amp; B  </ex:p>
      <ex:p r:datatype="urn:example:type">same</ex:p><ex:p/>
    </r:Description>
  </r:RDF>)";
  file.close();
  ImportPipeline pipeline;
  const auto stats = ASSERT_RESULT(pipeline.ImportFile(path));
  ASSERT_EQ(stats.triples_written, 4);
  for (const auto& literal : {"\"  A & B  \"@en", "\"  A & B  \"@fr",
                              "\"same\"^^<urn:example:type>", "\"\"@en"}) {
    ASSERT_OK(RunCli({"--mdb_dictionary_v2=true", "query",
        "SELECT ?s WHERE { ?s <https://example.org/#p> " + std::string(literal) + " }"}, &output));
    ASSERT_NO_FATALS(CheckOutput(output, {"s"}, {{"urn:example:s"}}));
  }
  ASSERT_NO_FATALS(CheckPhysicalRows(4));
}

TEST_F(MdbFileE2ETest, CliMalformedNtFileDoesNotWritePrefix) {
  const auto path = GetTestPath("malformed-cli.nt");
  std::ofstream file(path);
  file << "<urn:example:s> <urn:example:p> \"valid\" .\ninvalid .\n";
  file.close();
  std::string output;
  ASSERT_FALSE(RunCli({"import", path}, &output, false).ok());
  ASSERT_NO_FATALS(CheckPhysicalRows(0));
}


TEST_F(MdbFileE2ETest, ResidentV2ReaderSeesExternalCommittedTerms) {
  const auto old = FLAGS_mdb_dictionary_v2;
  auto& model = RdfYBModel::Instance();
  const auto old_transactions = model.UseTransactions();
  auto cleanup = ScopeExit([&] {
    model.ClearAll();
    FLAGS_mdb_dictionary_v2 = old;
    model.SetUseTransactions(old_transactions);
  });
  FLAGS_mdb_dictionary_v2 = true;
  model.SetUseTransactions(true);
  ASSERT_EQ(YBCMdbInitCluster(cluster_->GetMasterAddresses().c_str(), kNamespace), 0);
  ASSERT_NO_FATALS(CheckResidentRows(
      "SELECT ?s WHERE { ?s ?p ?o }", {"s"}, {}));
  std::string output;
  ASSERT_OK(RunCli({"--mdb_dictionary_v2=true", "query", R"(INSERT DATA {
      <urn:example:new> <urn:example:predicate> "new long value"@en .
    })"}, &output, true));
  ASSERT_NO_FATALS(CheckResidentRows(
      "SELECT ?s WHERE { ?s <urn:example:predicate> \"new long value\"@EN }",
      {"s"}, {{"urn:example:new"}}));
  ASSERT_NO_FATALS(CheckResidentRows(
      "SELECT ?s ?p ?o WHERE { ?s ?p ?o }", {"s", "p", "o"},
      {{"urn:example:new", "urn:example:predicate", "new long value"}}));
  model.SetUseTransactions(false);
  ASSERT_OK(RunCli({"--mdb_dictionary_v2=true", "query", R"(INSERT DATA {
      <urn:example:second> <urn:example:predicate> "second long value"@fr .
    })"}, &output, true));
  ASSERT_NO_FATALS(CheckResidentRows(
      "SELECT ?s ?o WHERE { ?s <urn:example:predicate> ?o } ORDER BY ?s",
      {"s", "o"}, {{"urn:example:new", "new long value"},
                     {"urn:example:second", "second long value"}}));
  ASSERT_NO_FATALS(CheckResidentRows(
      "SELECT ?s WHERE { VALUES ?o { \"second long value\"@fr } "
      "?s <urn:example:predicate> ?o }", {"s"}, {{"urn:example:second"}}));
}


TEST_F(MdbFileE2ETest, ResidentLegacyReaderSeesExternalCommittedTerms) {
  const auto old = FLAGS_mdb_dictionary_v2;
  auto& model = RdfYBModel::Instance();
  auto cleanup = ScopeExit([&] {
    model.ClearAll();
    FLAGS_mdb_dictionary_v2 = old;
  });
  FLAGS_mdb_dictionary_v2 = false;
  ASSERT_EQ(YBCMdbInitCluster(cluster_->GetMasterAddresses().c_str(), kNamespace), 0);
  ASSERT_NO_FATALS(CheckResidentRows("SELECT ?s WHERE { ?s ?p ?o }", {"s"}, {}));
  std::string output;
  ASSERT_OK(RunCli({"query", R"(INSERT DATA {
      <urn:example:first> <urn:example:predicate> "first long value" .
    })"}, &output));
  ASSERT_NO_FATALS(CheckResidentRows(
      "SELECT ?s WHERE { ?s <urn:example:predicate> \"first long value\" }",
      {"s"}, {{"urn:example:first"}}));
  ASSERT_OK(RunCli({"query", R"(INSERT DATA {
      <urn:example:second> <urn:example:predicate> "second long value" .
    })"}, &output));
  ASSERT_NO_FATALS(CheckResidentRows(
      "SELECT ?s ?o WHERE { ?s <urn:example:predicate> ?o } ORDER BY ?s",
      {"s", "o"}, {{"urn:example:first", "first long value"},
                    {"urn:example:second", "second long value"}}));
}

TEST_F(MdbFileE2ETest, ResidentDictionaryFailureReleasesSnapshot) {
  const auto old = FLAGS_mdb_dictionary_v2;
  auto& model = RdfYBModel::Instance();
  auto cleanup = ScopeExit([&] {
    model.ClearAll();
    FLAGS_mdb_dictionary_v2 = old;
  });
  FLAGS_mdb_dictionary_v2 = true;
  std::string output;
  ASSERT_OK(RunCli({"--mdb_dictionary_v2=true", "query", R"(INSERT DATA {
      <urn:example:subject> <urn:example:predicate> "long value" .
    })"}, &output, true));
  ASSERT_EQ(YBCMdbInitCluster(cluster_->GetMasterAddresses().c_str(), kNamespace), 0);
  auto entries = ASSERT_RESULT(model.Dictionary()->Entries());
  uint64_t maximum = 0;
  for (const auto& entry : entries) maximum = std::max(maximum, entry.first);
  client::TableHandle table;
  ASSERT_OK(table.Open(client::YBTableName(
      YQL_DATABASE_CQL, kNamespace, PersistentIdMap::kTableName), client_.get()));
  const auto set_counter = [&](int64_t value) -> Status {
    auto session = client_->NewSession(MonoDelta::FromSeconds(30));
    auto op = table.NewInsertOp(session->arena());
    QLAddStringHashValue(op->mutable_request(), "counter");
    table.AddInt64ColumnValue(op->mutable_request(), "term_id", value);
    return session->TEST_ApplyAndFlush(op);
  };
  ASSERT_OK(set_counter(0));
  int callbacks = 0;
  const auto callback = [](const char*, const char*, void* context) {
    ++*static_cast<int*>(context);
  };
  get_query_ctx() = QueryContext{};
  ASSERT_NE(YBCMdbExecuteSparql("SELECT ?s WHERE { ?s ?p ?o }", callback, &callbacks), 0);
  ASSERT_EQ(callbacks, 0);
  ASSERT_FALSE(static_cast<bool>(model.QueryReadTime()));
  ASSERT_OK(set_counter(static_cast<int64_t>(maximum)));
  ASSERT_NO_FATALS(CheckResidentRows(
      "SELECT ?s WHERE { ?s <urn:example:predicate> \"long value\" }",
      {"s"}, {{"urn:example:subject"}}));
}


TEST_F(MdbFileE2ETest, ImportFileRejectsCallerTransactionWithoutAbortingIt) {
  auto& model = RdfYBModel::Instance();
  const auto previous = model.UseTransactions();
  auto cleanup = ScopeExit([&] {
    model.ClearAll();
    model.SetUseTransactions(previous);
  });
  ASSERT_EQ(YBCMdbInitCluster(cluster_->GetMasterAddresses().c_str(), kNamespace), 0);
  const auto path = GetTestPath("caller-transaction.nt");
  std::ofstream file(path);
  file << "<urn:example:s> <urn:example:p> \"value\" .\n";
  file.close();
  ASSERT_FALSE(file.fail());
  for (bool transactions : {true, false}) {
    model.SetUseTransactions(transactions);
    ASSERT_OK(model.BeginTransaction());
    ASSERT_NE(YBCMdbImportFile(path.c_str()), 0);
    ASSERT_TRUE(model.HasTransaction());
    ASSERT_OK(model.AbortTransaction());
  }
  ASSERT_OK(model.BeginReadSnapshot());
  const auto pinned = model.QueryReadTime();
  ASSERT_NE(YBCMdbImportFile(path.c_str()), 0);
  ASSERT_EQ(model.QueryReadTime(), pinned);
  model.EndReadSnapshot();
  std::string output;
  ASSERT_OK(RunCli({"query", "SELECT ?s WHERE { ?s ?p ?o }"}, &output));
  ASSERT_NO_FATALS(CheckOutput(output, {"s"}, {}));
  ASSERT_NO_FATALS(CheckPhysicalRows(0));
}


TEST_F(MdbFileE2ETest, CliNumericValuesRetainSignedAndWideIntegers) {
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT ?n WHERE { VALUES ?n { -1 0 1 281474976710657 "
      "-9223372036854775808 9223372036854775807 } } ORDER BY ?n", {"n"},
      {{"-9223372036854775808"}, {"-1"}, {"0"}, {"1"},
       {"281474976710657"}, {"9223372036854775807"}}));
}

TEST_F(MdbFileE2ETest, CliDoubleValuesOrderByNumericValue) {
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT ?n WHERE { VALUES ?n { 2e0 -1e0 10e0 0e0 } } ORDER BY ?n", {"n"},
      {{"-1e0"}, {"0e0"}, {"2e0"}, {"10e0"}}));
}

TEST_F(MdbFileE2ETest, CliDecimalValuesKeepPrecisionAndZero) {
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT ?n WHERE { VALUES ?n { 1.000000000000000001 1.0 0.0 -12.75 } } ORDER BY ?n",
      {"n"}, {{"-12.75"}, {"0.0"}, {"1.0"}, {"1.000000000000000001"}}));
}


TEST_F(MdbFileE2ETest, CliNumericFiltersAndMixedOrderPreserveValues) {
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT ?n WHERE { VALUES ?n { -2 -1 0 1 2 } FILTER(?n < 0) } ORDER BY ?n",
      {"n"}, {{"-2"}, {"-1"}}));
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT ?n WHERE { VALUES ?n { -2 -1 0 1 2 3 } "
      "FILTER(?n >= 0 && ?n <= 2) } ORDER BY DESC(?n)",
      {"n"}, {{"2"}, {"1"}, {"0"}}));
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT ?n WHERE { VALUES ?n { 10 -1e0 0.5 2e0 } } ORDER BY ?n LIMIT 2 OFFSET 1",
      {"n"}, {{"0.5"}, {"2e0"}}));
}


TEST_F(MdbFileE2ETest, CliPersistedNumericV2) {
  ASSERT_NO_FATALS(CheckPersistedNumeric());
}

TEST_F(MdbFileE2ETest, CliInvalidNumericArithmeticDoesNotCreateValues) {
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT ?s WHERE { VALUES (?s ?n) { (\"bad\" \"oops\""
      "^^<http://www.w3.org/2001/XMLSchema#integer>) (\"plain\" \"2\") (\"valid\" 2) } "
      "BIND(?n + 1 AS ?v) FILTER(BOUND(?v)) }", {"s"}, {{"valid"}}));
}


TEST_F(MdbFileE2ETest, CliNumericSyntaxPreservesTermIdentity) {
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT DISTINCT ?n WHERE { VALUES ?n { 01 1 \"1\""
      "^^<http://www.w3.org/2001/XMLSchema#integer> \"+1\""
      "^^<http://www.w3.org/2001/XMLSchema#integer> } BIND(STR(?n) AS ?key) } ORDER BY ?key",
      {"n"}, {{"+1"}, {"01"}, {"1"}}));
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT ?n WHERE { VALUES ?n { 01 1 } FILTER(sameTerm(?n, \"1\""
      "^^<http://www.w3.org/2001/XMLSchema#integer>)) }", {"n"}, {{"1"}}));
}


TEST_F(MdbFileE2ETest, CliComputedNumericIdentityMatchesTerms) {
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT ?n WHERE { VALUES ?n { 1 01 } FILTER(sameTerm(1 + 0, ?n)) }",
      {"n"}, {{"1"}}));
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT DISTINCT ?n WHERE { { VALUES ?n { 1 01 } } UNION "
      "{ BIND(1 + 0 AS ?n) } BIND(STR(?n) AS ?key) } ORDER BY ?key", {"n"}, {{"01"}, {"1"}}));
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT ?n WHERE { BIND(1 + 0 AS ?n) VALUES ?n { 1 01 } }", {"n"}, {{"1"}}));
}

TEST_F(MdbFileE2ETest, CliNumericErrorsStayUnbound) {
  const std::vector<std::string> errors{
      "9223372036854775807 + 1", "-9223372036854775808 - 1",
      "9223372036854775807 * 2", "-(-9223372036854775808)",
      "ABS(-9223372036854775808)", "1 / 0", "1.0 / 0.0",
      "<http://www.w3.org/2001/XMLSchema#integer>(\"NaN\""
      "^^<http://www.w3.org/2001/XMLSchema#double>)",
      "<http://www.w3.org/2001/XMLSchema#integer>(\"9223372036854775808\""
      "^^<http://www.w3.org/2001/XMLSchema#double>)"};
  for (const auto& expression : errors) {
    ASSERT_NO_FATALS(CheckOrderedRows(
        "SELECT ?v WHERE { BIND(" + expression + " AS ?v) FILTER(BOUND(?v)) }",
        {"v"}, {}));
    ASSERT_NO_FATALS(CheckOrderedRows(
        "SELECT ?v WHERE { BIND(COALESCE(" + expression + ", 7) AS ?v) }",
        {"v"}, {{"7"}}));
  }
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT ?v WHERE { BIND(1e0 / 0e0 AS ?v) }", {"v"}, {{"INF"}}));
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT ?v WHERE { BIND(0e0 / 0e0 AS ?v) }", {"v"}, {{"NaN"}}));
}

TEST_F(MdbFileE2ETest, CliTypedNumericFunctionsAndCasts) {
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT ?s WHERE { BIND(<http://www.w3.org/2001/XMLSchema#string>(01) AS ?s) }",
      {"s"}, {{"1"}}));
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT ?n ?a ?f ?c ?r ?i WHERE { VALUES ?n { "
      "\"-2.5\"^^<http://www.w3.org/2001/XMLSchema#decimal> } "
      "BIND(ABS(?n) AS ?a) BIND(FLOOR(?n) AS ?f) BIND(CEIL(?n) AS ?c) "
      "BIND(ROUND(?n) AS ?r) "
      "BIND(<http://www.w3.org/2001/XMLSchema#integer>(?n) AS ?i) }",
      {"n", "a", "f", "c", "r", "i"},
      {{"-2.5", "2.5", "-3.0", "-2.0", "-2.0", "-2"}}));
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT ?n ?v WHERE { VALUES ?n { 1 -1 0 } FILTER(?n) "
      "BIND(-?n AS ?v) } ORDER BY ?n", {"n", "v"}, {{"-1", "1"}, {"1", "-1"}}));
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT ?n WHERE { VALUES ?n { \"NaN\""
      "^^<http://www.w3.org/2001/XMLSchema#double> 0 -0.0 0e0 "
      "\"oops\"^^<http://www.w3.org/2001/XMLSchema#integer> } FILTER(?n) }",
      {"n"}, {}));
}


TEST_F(MdbFileE2ETest, CliSelectNumericExpressionsAndDistinctSubquery) {
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT (?n + 1 AS ?v) WHERE { VALUES ?n { 0 1 } } ORDER BY ?v",
      {"v"}, {{"1"}, {"2"}}));
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT ?v WHERE { { SELECT DISTINCT (?n + 0 AS ?v) "
      "WHERE { VALUES ?n { 1 01 } } } }", {"v"}, {{"1"}}));
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT ?v WHERE { { SELECT DISTINCT ?v WHERE { VALUES ?v { 01 1 1 } } } "
      "BIND(STR(?v) AS ?key) } ORDER BY ?key", {"v"}, {{"01"}, {"1"}}));
}


TEST_F(MdbFileE2ETest, CliDecimalArithmeticRetainsPrecision) {
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT ?v WHERE { BIND((1.0 + 0.000000000000000001) - 1.0 AS ?v) }",
      {"v"}, {{"0.000000000000000001"}}));
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT ?v WHERE { BIND(2.0 / 3.0 AS ?v) }",
      {"v"}, {{"0.6666666666666666667"}}));
}

TEST_F(MdbFileE2ETest, CliStringNumericCastsRejectInvalidLexical) {
  const std::string xsd = "<http://www.w3.org/2001/XMLSchema#";
  for (const auto& [type, value] : std::vector<std::pair<std::string, std::string>>{
      {"decimal", "1.2.3"}, {"decimal", "1.1234567890123456789"},
      {"double", "nan"}, {"double", "0x1p0"}, {"float", "infinity"}, {"integer", "1.0"}}) {
    ASSERT_NO_FATALS(CheckOrderedRows(
        "SELECT ?v WHERE { BIND(COALESCE(" + xsd + type + ">(\"" + value +
        "\"), 7) AS ?v) }", {"v"}, {{"7"}}));
  }
}

TEST_F(MdbFileE2ETest, CliNumericMembershipUsesEqualityAndErrors) {
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT ?v WHERE { BIND(\"NaN\"^^<http://www.w3.org/2001/XMLSchema#double> "
      "IN (\"NaN\"^^<http://www.w3.org/2001/XMLSchema#double>) AS ?v) }",
      {"v"}, {{"false"}}));
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT ?v WHERE { BIND(COALESCE(2 NOT IN (3, 1/0), 7) AS ?v) }",
      {"v"}, {{"7"}}));
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT ?v WHERE { BIND(2 IN (1/0, 2) AS ?v) }", {"v"}, {{"true"}}));
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT ?v WHERE { BIND(2 NOT IN (1/0, 2) AS ?v) }", {"v"}, {{"false"}}));
}


TEST_F(MdbFileE2ETest, CliDecimalRangeFailuresStayExpressionErrors) {
  const std::string maximum = "1" + std::string(127, '0') + ".0";
  const std::string minimum = "0." + std::string(127, '0') + "1";
  const std::vector<std::string> errors{
      "9999999999999999999.0 * 2.0", "1.0 + 0.0000000000000000001",
      maximum + " * 10.0", minimum + " / 10.0",
      "<http://www.w3.org/2001/XMLSchema#decimal>(1e200)",
      "<http://www.w3.org/2001/XMLSchema#decimal>(1e-200)"};
  for (const auto& expression : errors) {
    ASSERT_NO_FATALS(CheckOrderedRows(
        "SELECT ?v WHERE { BIND(COALESCE(" + expression + ", 7) AS ?v) }",
        {"v"}, {{"7"}}));
    ASSERT_NO_FATALS(CheckOrderedRows(
        "SELECT ?v WHERE { BIND(" + expression + " AS ?v) FILTER(BOUND(?v)) }",
        {"v"}, {}));
  }
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT ?v WHERE { BIND(0.0 / " + minimum + " AS ?v) }", {"v"}, {{"0.0"}}));
}

TEST_F(MdbFileE2ETest, CliNumericCastLexicalConsistency) {
  const std::string xsd = "<http://www.w3.org/2001/XMLSchema#";
  for (const auto& [type, value, expected] :
       std::vector<std::tuple<std::string, std::string, std::string>>{
           {"integer", " \\t+001\\n", "1"}, {"decimal", " +0001.2300 ", "1.23"},
           {"double", "INF", "INF"}, {"double", "-INF", "-INF"}, {"double", "NaN", "NaN"},
           {"float", "1e-40", "1e-40"}}) {
    ASSERT_NO_FATALS(CheckOrderedRows(
        "SELECT ?v WHERE { BIND(" + xsd + type + ">(\"" + value + "\") AS ?v) }",
        {"v"}, {{expected}}));
  }
}

TEST_F(MdbFileE2ETest, CliNumericBooleanErrorsPropagate) {
  const std::vector<std::pair<std::string, std::string>> expressions{
      {"COALESCE(!(1/0), 7)", "7"}, {"COALESCE(IF(1/0, 1, 2), 7)", "7"},
      {"COALESCE((1/0) && true, 7)", "7"}, {"COALESCE((1/0) || false, 7)", "7"},
      {"(1/0) && false", "false"}, {"(1/0) || true", "true"},
      {"2 IN ()", "false"}, {"2 NOT IN ()", "true"},
      {"COALESCE(2 IN (3, 1/0), 7)", "7"}};
  for (const auto& [expression, expected] : expressions) {
    ASSERT_NO_FATALS(CheckOrderedRows(
        "SELECT ?v WHERE { BIND(" + expression + " AS ?v) }", {"v"}, {{expected}}));
  }
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT ?n WHERE { VALUES ?n { 1 } FILTER(<urn:example:ebv-error>) }",
      {"n"}, {}));
}

TEST_F(MdbFileE2ETest, CliUnboundRowsKeepBoundaries) {
  std::string output;
  ASSERT_OK(RunCli({"query", "SELECT ?x WHERE { VALUES ?x { UNDEF UNDEF } }"}, &output));
  ASSERT_EQ(output, "x\tUNDEF\n---\nx\tUNDEF\n");
}

TEST_F(MdbFileE2ETest, CliSparseProjectionKeepsColumns) {
  std::string output;
  ASSERT_OK(RunCli({"query", R"(
      SELECT ?y ?x WHERE { VALUES (?x ?y) { (UNDEF "a") ("b" UNDEF) } }
    )"}, &output));
  ASSERT_EQ(output, "y\ta\nx\tUNDEF\n---\ny\tUNDEF\nx\tb\n");
}

TEST_F(MdbFileE2ETest, CliEmptyLiteralIsNotUnbound) {
  std::string output;
  ASSERT_OK(RunCli({"query", R"(SELECT ?x WHERE { VALUES ?x { "" } })"}, &output));
  ASSERT_EQ(output, "x\t\n");
}

TEST_F(MdbFileE2ETest, CliJsonUnboundAndEmptyResults) {
  rapidjson::Document result;
  ASSERT_NO_FATALS(QueryJson("SELECT ?x WHERE { VALUES ?x { UNDEF UNDEF } }", &result));
  ASSERT_EQ(result["head"]["vars"].Size(), 1);
  ASSERT_STREQ(result["head"]["vars"][0].GetString(), "x");
  ASSERT_EQ(result["results"]["bindings"].Size(), 2);
  for (const auto& row : result["results"]["bindings"].GetArray()) {
    ASSERT_TRUE(row.IsObject());
    ASSERT_EQ(row.MemberCount(), 0);
  }
  ASSERT_NO_FATALS(QueryJson("SELECT ?x WHERE { VALUES ?x {} }", &result));
  ASSERT_EQ(result["head"]["vars"].Size(), 1);
  ASSERT_EQ(result["results"]["bindings"].Size(), 0);
  ASSERT_NO_FATALS(QueryJson("SELECT * WHERE {}", &result));
  ASSERT_EQ(result["head"]["vars"].Size(), 0);
  ASSERT_EQ(result["results"]["bindings"].Size(), 1);
  ASSERT_EQ(result["results"]["bindings"][0].MemberCount(), 0);
}

TEST_F(MdbFileE2ETest, CliJsonSparseProjectionAndPaging) {
  rapidjson::Document result;
  const std::string body = R"( WHERE { VALUES (?x ?y) {
      (UNDEF "a") ("b" UNDEF) (UNDEF "a") } })";
  ASSERT_NO_FATALS(QueryJson("SELECT ?y ?x" + body, &result));
  ASSERT_EQ(result["head"]["vars"].Size(), 2);
  ASSERT_STREQ(result["head"]["vars"][0].GetString(), "y");
  ASSERT_STREQ(result["head"]["vars"][1].GetString(), "x");
  const auto& rows = result["results"]["bindings"];
  ASSERT_EQ(rows.Size(), 3);
  ASSERT_EQ(rows[0].MemberCount(), 1);
  ASSERT_STREQ(rows[0]["y"]["value"].GetString(), "a");
  ASSERT_EQ(rows[1].MemberCount(), 1);
  ASSERT_STREQ(rows[1]["x"]["value"].GetString(), "b");
  ASSERT_EQ(rows[2].MemberCount(), 1);
  ASSERT_STREQ(rows[2]["y"]["value"].GetString(), "a");
  ASSERT_NO_FATALS(QueryJson("SELECT ?y ?x" + body + " LIMIT 2 OFFSET 1", &result));
  ASSERT_EQ(result["results"]["bindings"].Size(), 2);
  ASSERT_STREQ(result["results"]["bindings"][0]["x"]["value"].GetString(), "b");
  ASSERT_NO_FATALS(QueryJson("SELECT DISTINCT ?y ?x" + body, &result));
  ASSERT_EQ(result["results"]["bindings"].Size(), 2);
  ASSERT_NO_FATALS(QueryJson("SELECT ?x" + body + " LIMIT 0", &result));
  ASSERT_EQ(result["head"]["vars"].Size(), 1);
  ASSERT_EQ(result["results"]["bindings"].Size(), 0);
}

TEST_F(MdbFileE2ETest, CliJsonMetadataAndBinaryLexicalSurviveRestart) {
  const auto path = GetTestPath("result-metadata.nt");
  std::ofstream file(path);
  file << R"(<urn:example:iri> <urn:example:p> <urn:example:value> .
<urn:example:plain> <urn:example:p> "urn:example:value" .
<urn:example:lang> <urn:example:p> "urn:example:value"@EN .
<urn:example:type> <urn:example:p> "urn:example:value"^^<urn:example:datatype> .
<urn:example:empty> <urn:example:p> "" .
<urn:example:escape> <urn:example:p> "line\n\t\"\\\u0000\u4E2D" .
<urn:example:b1> <urn:example:p> _:b .
<urn:example:b2> <urn:example:p> _:b .
)";
  file.close();
  ASSERT_TRUE(file.good());
  std::string output;
  ASSERT_OK(RunCli({"--mdb_dictionary_v2=true", "import", path}, &output));
  rapidjson::Document result;
  ASSERT_NO_FATALS(QueryJson(
      "SELECT ?s ?o ?missing WHERE { ?s <urn:example:p> ?o }", &result, true));
  ASSERT_NE(last_cli_error_.find("MDB tablet-aware query: fragments="), std::string::npos);
  ASSERT_EQ(result["head"]["vars"].Size(), 3);
  ASSERT_STREQ(result["head"]["vars"][2].GetString(), "missing");
  const auto& rows = result["results"]["bindings"];
  ASSERT_EQ(rows.Size(), 8);
  std::map<std::string, const rapidjson::Value*> objects;
  for (const auto& row : rows.GetArray()) {
    ASSERT_EQ(row.MemberCount(), 2);
    ASSERT_STREQ(row["s"]["type"].GetString(), "uri");
    objects.emplace(row["s"]["value"].GetString(), &row["o"]);
  }
  ASSERT_EQ(objects.size(), 8);
  ASSERT_STREQ(objects.at("urn:example:iri")->operator[]("type").GetString(), "uri");
  ASSERT_STREQ(objects.at("urn:example:plain")->operator[]("type").GetString(), "literal");
  ASSERT_STREQ(objects.at("urn:example:lang")->operator[]("xml:lang").GetString(), "en");
  ASSERT_STREQ(objects.at("urn:example:type")->operator[]("datatype").GetString(),
               "urn:example:datatype");
  ASSERT_EQ(objects.at("urn:example:empty")->operator[]("value").GetStringLength(), 0);
  std::string expected = "line\n\t\"\\";
  expected.push_back('\0');
  expected += "\xe4\xb8\xad";
  const auto& binary = objects.at("urn:example:escape")->operator[]("value");
  ASSERT_EQ(std::string(binary.GetString(), binary.GetStringLength()), expected);
  ASSERT_STREQ(objects.at("urn:example:b1")->operator[]("type").GetString(), "bnode");
  ASSERT_STREQ(objects.at("urn:example:b1")->operator[]("value").GetString(),
               objects.at("urn:example:b2")->operator[]("value").GetString());
  ASSERT_NO_FATALS(QueryJson(
      "SELECT ?o WHERE { ?s <urn:example:p> ?o } ORDER BY ?s", &result, true));
  ASSERT_EQ(result["results"]["bindings"].Size(), 8);
  ASSERT_NO_FATALS(CheckPhysicalRows(8));
}

TEST_F(MdbFileE2ETest, CliJsonComputedScalarTypesAndFailure) {
  rapidjson::Document result;
  ASSERT_NO_FATALS(QueryJson(R"(SELECT ?i ?d ?b ?s WHERE {
      BIND(1 + 2 AS ?i) BIND(1.5 + 1.0 AS ?d) BIND(1 = 1 AS ?b) BIND("" AS ?s) })",
      &result));
  const auto& rows = result["results"]["bindings"];
  ASSERT_EQ(rows.Size(), 1);
  const auto& row = rows[0];
  ASSERT_EQ(row.MemberCount(), 4);
  ASSERT_STREQ(row["i"]["value"].GetString(), "3");
  ASSERT_STREQ(row["i"]["datatype"].GetString(), "http://www.w3.org/2001/XMLSchema#integer");
  ASSERT_STREQ(row["d"]["value"].GetString(), "2.5");
  ASSERT_STREQ(row["d"]["datatype"].GetString(), "http://www.w3.org/2001/XMLSchema#decimal");
  ASSERT_STREQ(row["b"]["value"].GetString(), "true");
  ASSERT_STREQ(row["b"]["datatype"].GetString(), "http://www.w3.org/2001/XMLSchema#boolean");
  ASSERT_EQ(row["s"]["value"].GetStringLength(), 0);
  std::string output;
  ASSERT_FALSE(RunCli({"--mdb_output_format=json", "query",
      "SELECT ?x WHERE { VALUES ?x {1 2} } ORDER BY STR(?x)"}, &output).ok());
  ASSERT_TRUE(output.empty());
  ASSERT_FALSE(RunCli({"--mdb_output_format=bad", "query", "SELECT * WHERE {}"}, &output).ok());
  ASSERT_TRUE(output.empty());
}

TEST_F(MdbFileE2ETest, ResultRowsSchemaCancellationAndEmptyBindings) {
  auto& model = RdfYBModel::Instance();
  const auto cleanup = ScopeExit([&] { model.ClearAll(); });
  ASSERT_EQ(YBCMdbInitCluster(cluster_->GetMasterAddresses().c_str(), kNamespace), 0);
  const auto run = [&](const char* query, CapturedResults* result) {
    get_query_ctx() = QueryContext{};
    return YBCMdbExecuteSparqlRows(query, CapturedResults::Schema, CapturedResults::Row, result);
  };
  CapturedResults empty;
  ASSERT_EQ(run("SELECT ?x WHERE { VALUES ?x {} }", &empty), 0);
  ASSERT_EQ(empty.schemas, 1);
  ASSERT_EQ(empty.columns, std::vector<std::string>{"x"});
  ASSERT_TRUE(empty.rows.empty());
  CapturedResults unbound;
  ASSERT_EQ(run("SELECT ?x WHERE { VALUES ?x { UNDEF UNDEF } }", &unbound), 0);
  ASSERT_EQ(unbound.rows.size(), 2);
  for (const auto& row : unbound.rows) {
    ASSERT_EQ(row.size(), 1);
    ASSERT_EQ(row[0].kind, YBC_MDB_UNBOUND);
  }
  CapturedResults no_columns;
  ASSERT_EQ(run("SELECT * WHERE {}", &no_columns), 0);
  ASSERT_TRUE(no_columns.columns.empty());
  ASSERT_EQ(no_columns.rows.size(), 1);
  ASSERT_TRUE(no_columns.rows[0].empty());
  CapturedResults cancel_schema;
  cancel_schema.cancel_schema = true;
  ASSERT_NE(run("SELECT ?x WHERE { VALUES ?x {1 2} }", &cancel_schema), 0);
  ASSERT_EQ(cancel_schema.schemas, 1);
  ASSERT_TRUE(cancel_schema.rows.empty());
  ASSERT_FALSE(model.QueryReadTime());
  CapturedResults cancel_row;
  cancel_row.cancel_row = true;
  ASSERT_NE(run("SELECT ?x WHERE { VALUES ?x {1 2} }", &cancel_row), 0);
  ASSERT_EQ(cancel_row.rows.size(), 1);
  ASSERT_FALSE(model.QueryReadTime());
  CapturedResults recovered;
  ASSERT_EQ(run("SELECT ?x WHERE { VALUES ?x {1 2} }", &recovered), 0);
  ASSERT_EQ(recovered.rows.size(), 2);
  ASSERT_NE(YBCMdbExecuteSparqlRows(nullptr, CapturedResults::Schema,
      CapturedResults::Row, &recovered), 0);
  ASSERT_NE(YBCMdbExecuteSparqlRows("SELECT * WHERE {}", nullptr,
      CapturedResults::Row, &recovered), 0);
  ASSERT_NE(YBCMdbExecuteSparqlRows("SELECT * WHERE {}", CapturedResults::Schema,
      nullptr, &recovered), 0);
}

TEST_F(MdbFileE2ETest, ResultRowsLateDecodeFailureAndLegacyCompatibility) {
  auto& model = RdfYBModel::Instance();
  const auto key = SPARQL::Conversions::pack_string_simple("bad").id;
  const auto cleanup = ScopeExit([&] {
    g_gmap_to_pack.erase(key);
    model.ClearAll();
  });
  ASSERT_EQ(YBCMdbInitCluster(cluster_->GetMasterAddresses().c_str(), kNamespace), 0);
  CapturedResults result;
  const auto inject = [](const YBCMdbString* names, size_t count, void* ctx) {
    g_gmap_to_pack[SPARQL::Conversions::pack_string_simple("bad").id] =
        ::ObjectId(::ObjectId::MASK_IRI_EXTERN | 0x1234567890ULL);
    return CapturedResults::Schema(names, count, ctx);
  };
  get_query_ctx() = QueryContext{};
  ASSERT_NE(YBCMdbExecuteSparqlRows(R"(SELECT ?x WHERE { VALUES ?x {"good" "bad"} })",
      inject, CapturedResults::Row, &result), 0);
  ASSERT_EQ(result.schemas, 1);
  ASSERT_EQ(result.rows.size(), 1);
  ASSERT_EQ(result.rows[0][0].lexical, "good");
  ASSERT_FALSE(model.QueryReadTime());
  g_gmap_to_pack.erase(key);
  ASSERT_NO_FATALS(CheckResidentRows(
      R"(SELECT ?x WHERE { VALUES ?x {"kept" UNDEF} })", {"x"}, {{"kept"}}));
  get_query_ctx() = QueryContext{};
  CapturedResults recovered;
  ASSERT_EQ(YBCMdbExecuteSparqlRows(R"(SELECT ?x WHERE { VALUES ?x {"good" "bad"} })",
      CapturedResults::Schema, CapturedResults::Row, &recovered), 0);
  ASSERT_EQ(recovered.rows.size(), 2);
}

TEST_F(MdbFileE2ETest, CliDescribeDistinguishesSameLexemeTerms) {
  const auto path = GetTestPath("describe-identity.nt");
  std::ofstream file(path);
  file << R"(<urn:example:s> <urn:example:p> <urn:example:v> .
<urn:example:s> <urn:example:p> "urn:example:v" .
<urn:example:s> <urn:example:p> <urn:example:s> .
)";
  file.close();
  ASSERT_TRUE(file.good());
  std::string output;
  ASSERT_OK(RunCli({"--mdb_dictionary_v2=true", "import", path}, &output));
  ASSERT_OK(RunCli({"--mdb_dictionary_v2=true", "query", "DESCRIBE <urn:example:s>"}, &output));
  ASSERT_NO_FATALS(CheckOutput(output, {"subject", "predicate", "object"},
      {{"urn:example:s", "urn:example:p", "urn:example:v"},
       {"urn:example:s", "urn:example:p", "urn:example:v"},
       {"urn:example:s", "urn:example:p", "urn:example:s"}}));
  rapidjson::Document result;
  ASSERT_NO_FATALS(QueryJson("DESCRIBE <urn:example:s>", &result, true));
  const auto& rows = result["results"]["bindings"];
  ASSERT_EQ(rows.Size(), 3);
  std::multiset<std::string> kinds;
  for (const auto& row : rows.GetArray()) kinds.insert(row["object"]["type"].GetString());
  ASSERT_EQ(kinds, (std::multiset<std::string>{"uri", "uri", "literal"}));
}


TEST_F(MdbFileE2ETest, CliCountDistinctAggregate) {
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT (COUNT(DISTINCT ?x) AS ?n) WHERE { VALUES ?x { 1 1 2 UNDEF } }",
      {"n"}, {{"2"}}));
}

TEST_F(MdbFileE2ETest, CliSumDistinctAggregate) {
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT (SUM(DISTINCT ?x) AS ?n) WHERE { VALUES ?x { 1 1 2 } }",
      {"n"}, {{"3"}}));
}

TEST_F(MdbFileE2ETest, CliAvgDistinctAggregate) {
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT (AVG(DISTINCT ?x) AS ?n) WHERE { VALUES ?x { 1 1 2 } }",
      {"n"}, {{"1.5"}}));
}

TEST_F(MdbFileE2ETest, CliCountAllDistinctAggregate) {
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT (COUNT(DISTINCT *) AS ?n) WHERE { VALUES (?x ?y) {"
      " (1 UNDEF) (1 UNDEF) (1 2) (UNDEF UNDEF) (UNDEF UNDEF) } }",
      {"n"}, {{"3"}}));
}

TEST_F(MdbFileE2ETest, CliGroupConcatDistinctAggregate) {
  std::string output;
  ASSERT_OK(RunCli({"query", R"(SELECT (GROUP_CONCAT(DISTINCT ?x; separator="|") AS ?n)
      WHERE { VALUES ?x { "a" "b" "a" } })"}, &output));
  ASSERT_TRUE(output == "n\ta|b\n" || output == "n\tb|a\n") << output;
}

TEST_F(MdbFileE2ETest, CliAggregateComputedProjection) {
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT (2 * SUM(?x) AS ?twice) (COUNT(?x) + 1 AS ?n) "
      "WHERE { VALUES ?x { 1 2 3 } }", {"twice", "n"}, {{"12", "4"}}));
}

TEST_F(MdbFileE2ETest, CliAggregateGroupsHavingAndOrder) {
  ASSERT_NO_FATALS(CheckOrderedRows(R"(SELECT ?g (COUNT(DISTINCT ?x) AS ?n)
      WHERE { VALUES (?g ?x) { ("a" 1) ("b" 2) ("a" 1) ("a" 3) } }
      GROUP BY ?g HAVING(COUNT(?x) > 1) ORDER BY DESC(?n))", {"g", "n"}, {{"a", "2"}}));
}

TEST_F(MdbFileE2ETest, CliPersistedBooleanEbvAndCasts) {
  const auto path = GetTestPath("typed-boolean.nt");
  std::ofstream file(path);
  const std::string type = "http://www.w3.org/2001/XMLSchema#boolean";
  for (const auto& text : {"true", "1", "false", "0", "bad"}) {
    file << "<urn:example:" << text << "> <urn:example:flag> \"" << text
         << "\"^^<" << type << "> .\n";
  }
  file.close();
  ASSERT_TRUE(file.good());
  std::string output;
  ASSERT_OK(RunCli({"--mdb_dictionary_v2=true", "import", path}, &output));
  ASSERT_OK(RunCli({"--mdb_dictionary_v2=true", "query",
      "SELECT ?s WHERE { ?s <urn:example:flag> ?b FILTER(?b) }"}, &output));
  ASSERT_NO_FATALS(CheckOutput(output, {"s"}, {{"urn:example:true"}, {"urn:example:1"}}));
  ASSERT_OK(RunCli({"--mdb_dictionary_v2=true", "query",
      "SELECT ?s WHERE { BIND(true AS ?b) ?s <urn:example:flag> ?b }"}, &output));
  ASSERT_NO_FATALS(CheckOutput(output, {"s"}, {{"urn:example:true"}}));
  ASSERT_OK(RunCli({"--mdb_dictionary_v2=true", "query",
      "SELECT (COUNT(DISTINCT ?b) AS ?n) WHERE { ?s <urn:example:flag> ?b }"}, &output));
  ASSERT_NO_FATALS(CheckOutput(output, {"n"}, {{"5"}}));
  ASSERT_OK(RunCli({"--mdb_dictionary_v2=true", "query",
      "SELECT ?s (<http://www.w3.org/2001/XMLSchema#integer>(?b) AS ?n) "
      "WHERE { ?s <urn:example:flag> ?b FILTER(?b) }"}, &output));
  ASSERT_NO_FATALS(CheckOutput(output, {"s", "n"},
      {{"urn:example:true", "1"}, {"urn:example:1", "1"}}));
}



TEST_F(MdbFileE2ETest, CliAggregateEmptyAndErrorBindings) {
  ASSERT_NO_FATALS(CheckOrderedRows(R"(SELECT (COUNT(?x) AS ?c) (SUM(DISTINCT ?x) AS ?s)
      (AVG(DISTINCT ?x) AS ?a) (GROUP_CONCAT(DISTINCT ?x) AS ?g)
      WHERE { VALUES ?x {} })", {"c", "s", "a", "g"}, {{"0", "0", "0", ""}}));
  ASSERT_NO_FATALS(CheckOrderedRows(R"(SELECT ?g (COUNT(*) AS ?n)
      WHERE { VALUES ?g {} } GROUP BY ?g)", {"g", "n"}, {}));
  ASSERT_NO_FATALS(CheckOrderedRows(R"(SELECT (COUNT(?x) AS ?c) (SUM(?x) AS ?s)
      WHERE { VALUES ?x { 1 UNDEF } })", {"c", "s"}, {{"1", "UNDEF"}}));
  ASSERT_NO_FATALS(CheckOrderedRows(R"(SELECT (COALESCE(SUM(?x), 7) AS ?s)
      WHERE { VALUES ?x { 1 "bad" } })", {"s"}, {{"7"}}));
}

TEST_F(MdbFileE2ETest, CliAggregateInputScopeAndSubqueries) {
  ASSERT_NO_FATALS(CheckOrderedRows(R"(SELECT (COUNT(DISTINCT *) AS ?n) WHERE {
      { SELECT ?x WHERE { VALUES (?x ?hidden) { (1 "a") (1 "b") (2 "a") } } }
      })", {"n"}, {{"2"}}));
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT (COUNT(DISTINCT *) AS ?n) WHERE { VALUES () { () () } }", {"n"}, {{"1"}}));
  const std::string inner = R"({ SELECT ?g (COUNT(DISTINCT ?x) AS ?n)
      WHERE { VALUES (?g ?x) { ("a" 1) ("a" 1) ("a" 2) ("b" 3) } } GROUP BY ?g })";
  ASSERT_NO_FATALS(CheckOrderedRows("SELECT (SUM(?n) AS ?total) WHERE {" + inner + "}",
      {"total"}, {{"3"}}));
  ASSERT_NO_FATALS(CheckOrderedRows("SELECT ?g ?n WHERE { VALUES ?g {\"a\" \"b\"} " +
      inner + "} ORDER BY ?g", {"g", "n"}, {{"a", "2"}, {"b", "1"}}));
}

TEST_F(MdbFileE2ETest, CliAggregateGroupExpressionsAndPagination) {
  ASSERT_NO_FATALS(CheckOrderedRows(R"(SELECT ?g (COUNT(*) AS ?n)
      WHERE { VALUES ?x {1 2 3 4} } GROUP BY (IF(?x < 3, "small", "large") AS ?g)
      ORDER BY DESC(?g) LIMIT 1 OFFSET 1)", {"g", "n"}, {{"large", "2"}}));
  ASSERT_NO_FATALS(CheckOrderedRows(R"(SELECT ?g WHERE { VALUES ?g { "b" "a" "b" } }
      GROUP BY ?g ORDER BY ?g)", {"g"}, {{"a"}, {"b"}}));
  ASSERT_NO_FATALS(CheckOrderedRows(R"(SELECT ?g (COUNT(*) AS ?n) WHERE {
      VALUES ?x {1 2} BIND(1 / 0 AS ?g) } GROUP BY ?g)", {"g", "n"}, {{"UNDEF", "2"}}));
}

TEST_F(MdbFileE2ETest, CliAggregateIdentityAndNumericViews) {
  ASSERT_NO_FATALS(CheckOrderedRows(R"(PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
      SELECT (COUNT(DISTINCT ?x) AS ?n) WHERE {
      VALUES ?x {1 01 true "true"^^xsd:boolean "1"^^xsd:boolean "true"} })",
      {"n"}, {{"5"}}));
  ASSERT_NO_FATALS(CheckOrderedRows(R"(SELECT (COUNT(DISTINCT ?x) AS ?n)
      (SUM(DISTINCT ?x) AS ?s) (AVG(DISTINCT ?x) AS ?a)
      WHERE { VALUES ?x { 01 1 1 } })", {"n", "s", "a"}, {{"2", "2", "1.0"}}));
  ASSERT_NO_FATALS(CheckOrderedRows(R"(SELECT (COUNT(DISTINCT ?x) AS ?n) WHERE {
      VALUES ?x {"a" "a"@en "a"@fr "a"^^<urn:example:type>} })", {"n"}, {{"4"}}));
}

TEST_F(MdbFileE2ETest, CliTypedBooleanLogicCastsAndIdentity) {
  const std::string prefix = "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> ";
  const std::vector<std::pair<std::string, std::string>> cases{
      {R"("1"^^xsd:boolean = true)", "true"},
      {R"(sameTerm("1"^^xsd:boolean, "true"^^xsd:boolean))", "false"},
      {R"("0"^^xsd:boolean IN (true, false))", "true"},
      {R"(xsd:integer("1"^^xsd:boolean))", "1"},
      {R"(xsd:string("0"^^xsd:boolean))", "false"},
      {R"(STR("1"^^xsd:boolean))", "1"},
      {R"(xsd:boolean(" true "))", "true"},
      {R"(IF("bad"^^xsd:boolean, 1, 2))", "2"},
      {R"(COALESCE(xsd:boolean("bad"), 7))", "7"},
      {R"(COALESCE("bad"^^xsd:boolean = true, 7))", "7"},
      {R"(COALESCE("true"^^xsd:boolean + 1, 7))", "7"}};
  for (const auto& [expr, expected] : cases) {
    ASSERT_NO_FATALS(CheckOrderedRows(prefix + "SELECT ?v WHERE { BIND(" + expr + " AS ?v) }",
        {"v"}, {{expected}}));
  }
  ASSERT_NO_FATALS(CheckOrderedRows(prefix + R"(SELECT ?b WHERE {
      VALUES ?b {"1"^^xsd:boolean "0"^^xsd:boolean} } ORDER BY ?b)",
      {"b"}, {{"0"}, {"1"}}));
}

TEST_F(MdbFileE2ETest, CliBooleanGroupingUsesCanonicalTermIdentity) {
  ASSERT_NO_FATALS(CheckOrderedRows(R"(PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
      SELECT ?b (COUNT(*) AS ?n) WHERE { VALUES ?b {true "true"^^xsd:boolean} } GROUP BY ?b)",
      {"b", "n"}, {{"true", "2"}}));
}

TEST_F(MdbFileE2ETest, CliAggregateMinMaxUsesValueOrder) {
  ASSERT_NO_FATALS(CheckOrderedRows(R"(SELECT (MIN(?x) AS ?lo) (MAX(?x) AS ?hi)
      WHERE { VALUES ?x { <urn:example:z> <urn:example:a> <urn:example:m> } })",
      {"lo", "hi"}, {{"urn:example:a", "urn:example:z"}}));
  ASSERT_NO_FATALS(CheckOrderedRows(R"(SELECT (MIN(?x) AS ?lo) (MAX(?x) AS ?hi)
      WHERE { VALUES ?x { "z-long" "a-long" "m-long" } })",
      {"lo", "hi"}, {{"a-long", "z-long"}}));
  ASSERT_NO_FATALS(CheckOrderedRows(R"(PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
      SELECT (MIN(?x) AS ?lo) (MAX(?x) AS ?hi)
      WHERE { VALUES ?x { "10"^^xsd:integer "02"^^xsd:integer } })",
      {"lo", "hi"}, {{"02", "10"}}));
}


TEST_F(MdbFileE2ETest, CliZeroColumnValuesPreserveEmptyMappingBag) {
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT (COUNT(*) AS ?n) WHERE { VALUES () {} }", {"n"}, {{"0"}}));
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT (COUNT(*) AS ?n) WHERE { VALUES () { () } }", {"n"}, {{"1"}}));
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT (COUNT(*) AS ?n) WHERE { VALUES () { () () } }", {"n"}, {{"2"}}));
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT ?x WHERE { VALUES ?x {1 2} VALUES () { () () } } ORDER BY ?x",
      {"x"}, {{"1"}, {"1"}, {"2"}, {"2"}}));
  ASSERT_NO_FATALS(CheckOrderedRows(
      "SELECT ?x WHERE { VALUES ?x {1 2} VALUES () {} }", {"x"}, {}));
}

}  // namespace yb::mdb

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

#include <exception>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "query/parser/sparql_query_parser.h"
#include "query/query_context.h"

#include "yb/mdb/adapter/mdb_adapter.h"
#include "yb/mdb/model/rdf_yb_model.h"
#include "yb/util/flags.h"
#include "yb/util/jsonwriter.h"

DEFINE_NON_RUNTIME_string(mdb_masters, "127.0.0.1:7100", "Master addresses");
DEFINE_NON_RUNTIME_string(mdb_keyspace, "mdb_test", "Keyspace name");
DEFINE_NON_RUNTIME_string(mdb_output_format, "text", "Query output: text or json");
DEFINE_NON_RUNTIME_bool(mdb_collect_statistics, false,
            "Collect RDF cardinality statistics before a query or read-only batch (one SPO scan)");
DEFINE_NON_RUNTIME_bool(mdb_use_partition_load, true, "Use partition loading for queries");
DEFINE_NON_RUNTIME_bool(mdb_use_transactions, true,
    "Use distributed transactions for writes; queries always use a fixed read time");

static int RunInit(int argc, char* argv[]) {
  if (argc < 2) {
    std::cerr << "Usage: mdb_cli init <master_addrs> <keyspace>" << std::endl;
    return 1;
  }
  return YBCMdbInitCluster(argv[0], argv[1]);
}

static int RunImport(int argc, char* argv[]) {
  if (argc < 1) {
    std::cerr << "Usage: mdb_cli import <file_path>" << std::endl;
    return 1;
  }
  yb::mdb::RdfYBModel::Instance().SetUseTransactions(FLAGS_mdb_use_transactions);
  int init_ret = YBCMdbInitCluster(
      FLAGS_mdb_masters.c_str(), FLAGS_mdb_keyspace.c_str());
  if (init_ret != 0) {
    return init_ret;
  }
  return YBCMdbImportFile(argv[0]);
}

static int InitializeQueries() {
  auto& model = yb::mdb::RdfYBModel::Instance();
  model.SetUsePartitionLoad(FLAGS_mdb_use_partition_load);
  model.SetUseTransactions(FLAGS_mdb_use_transactions);
  const int ret = YBCMdbInitCluster(FLAGS_mdb_masters.c_str(), FLAGS_mdb_keyspace.c_str());
  if (ret != 0) return ret;
  if (FLAGS_mdb_collect_statistics) {
    const auto status = model.RefreshStatistics();
    if (!status.ok()) {
      std::cerr << "RDF statistics collection failed: " << status << std::endl;
      return 1;
    }
  }
  return 0;
}

static int ExecuteQuery(const std::string& query, std::ostream& output = std::cout) {
  get_query_ctx() = QueryContext{};
  yb::mdb::RdfYBModel::Instance().ClearQueryCaches();
  if (FLAGS_mdb_output_format != "text" && FLAGS_mdb_output_format != "json") {
    std::cerr << "mdb_output_format must be text or json" << std::endl;
    return 1;
  }
  struct Cell {
    YBCMdbTermKind kind;
    std::string lexical, language, datatype;
  };
  struct Context {
    bool has_schema = false;
    std::vector<std::string> columns;
    std::vector<std::vector<Cell>> rows;
  } context;
  const auto schema = [](const YBCMdbString* columns, size_t count, void* ctx) {
    auto& result = *static_cast<Context*>(ctx);
    if (result.has_schema) return 1;
    result.has_schema = true;
    for (size_t i = 0; i < count; ++i) {
      result.columns.emplace_back(columns[i].data, columns[i].size);
    }
    return 0;
  };
  const auto row = [](const YBCMdbValue* values, size_t count, void* ctx) {
    auto& result = *static_cast<Context*>(ctx);
    if (!result.has_schema || count != result.columns.size()) return 1;
    std::vector<Cell> cells;
    for (size_t i = 0; i < count; ++i) {
      cells.push_back({values[i].kind,
          std::string(values[i].lexical.data, values[i].lexical.size),
          std::string(values[i].language.data, values[i].language.size),
          std::string(values[i].datatype.data, values[i].datatype.size)});
    }
    result.rows.push_back(std::move(cells));
    return 0;
  };
  const int ret = YBCMdbExecuteSparqlRows(query.c_str(), schema, row, &context);
  // A late iterator/decoder error must not publish a successful-looking result prefix.
  if (ret != 0 || !context.has_schema) return ret;
  if (FLAGS_mdb_output_format == "json") {
    std::stringstream json;
    yb::JsonWriter writer(&json, yb::JsonWriter::COMPACT);
    writer.StartObject();
    writer.String("head");
    writer.StartObject();
    writer.String("vars");
    writer.StartArray();
    for (const auto& name : context.columns) writer.String(name);
    writer.EndArray();
    writer.EndObject();
    writer.String("results");
    writer.StartObject();
    writer.String("bindings");
    writer.StartArray();
    for (const auto& cells : context.rows) {
      writer.StartObject();
      for (size_t i = 0; i < cells.size(); ++i) {
        const auto& cell = cells[i];
        if (cell.kind == YBC_MDB_UNBOUND) continue;
        writer.String(context.columns[i]);
        writer.StartObject();
        writer.String("type");
        writer.String(cell.kind == YBC_MDB_IRI ? "uri" :
                      cell.kind == YBC_MDB_BLANK ? "bnode" : "literal");
        writer.String("value");
        writer.String(cell.lexical);
        if (!cell.language.empty()) {
          writer.String("xml:lang");
          writer.String(cell.language);
        } else if (!cell.datatype.empty()) {
          writer.String("datatype");
          writer.String(cell.datatype);
        }
        writer.EndObject();
      }
      writer.EndObject();
    }
    writer.EndArray();
    writer.EndObject();
    writer.EndObject();
    output << json.str() << '\n';
  } else {
    for (size_t i = 0; i < context.rows.size(); ++i) {
      if (i) output << "---\n";
      if (context.columns.empty()) output << "# empty binding\n";
      for (size_t j = 0; j < context.columns.size(); ++j) {
        const auto& cell = context.rows[i][j];
        output << context.columns[j] << '\t'
               << (cell.kind == YBC_MDB_UNBOUND ? "UNDEF" : cell.lexical) << '\n';
      }
    }
  }
  return output.good() ? 0 : 1;
}

static int RunQuery(int argc, char* argv[]) {
  if (argc < 1) {
    std::cerr << "Usage: mdb_cli query <sparql_query>" << std::endl;
    return 1;
  }
  std::string query = argv[0];
  for (int i = 1; i < argc; ++i) query += " " + std::string(argv[i]);
  const int ret = InitializeQueries();
  return ret == 0 ? ExecuteQuery(query) : ret;
}

static int RunQueryBatch(int argc, char* argv[]) {
  if (argc < 1) {
    std::cerr << "Usage: mdb_cli query-batch <sparql_file> [sparql_file ...]" << std::endl;
    return 1;
  }
  std::vector<std::string> queries;
  for (int i = 0; i < argc; ++i) {
    std::ifstream file(argv[i]);
    std::ostringstream content;
    content << file.rdbuf();
    if (!file.is_open() || file.bad()) {
      std::cerr << "Cannot read query file: " << argv[i] << std::endl;
      return 1;
    }
    try {
      SPARQL::QueryParser parser(content.str());
      if (parser.parser.getNumberOfSyntaxErrors() != 0 || !parser.root->selectQuery()) {
        std::cerr << "Batch accepts SELECT queries only: " << argv[i] << std::endl;
        return 1;
      }
    } catch (const std::exception& error) {
      std::cerr << "Invalid query file " << argv[i] << ": " << error.what() << std::endl;
      return 1;
    }
    queries.push_back(content.str());
  }
  const int ret = InitializeQueries();
  if (ret != 0) return ret;
  // Statistics and dictionary reuse require an unchanged dataset for this read-only batch.
  for (size_t i = 0; i < queries.size(); ++i) {
    std::ostringstream output;
    int query_ret;
    try {
      query_ret = ExecuteQuery(queries[i], output);
    } catch (const std::exception& error) {
      std::cerr << "Batch query " << i + 1 << " failed: " << error.what() << std::endl;
      return 1;
    }
    if (query_ret != 0) {
      std::cerr << "Batch query " << i + 1 << " failed" << std::endl;
      return query_ret;
    }
    std::cout << "=== query " << i + 1 << " ===\n" << output.str()
              << "=== end query " << i + 1 << " ===" << std::endl;
  }
  return 0;
}

int main(int argc, char* argv[]) {
  yb::ParseCommandLineFlags(&argc, &argv, true);

  if (argc < 2) {
    std::cerr << "Usage: mdb_cli <command> [args...]" << std::endl;
    std::cerr << "Commands:" << std::endl;
    std::cerr << "  init   <master_addrs> <keyspace>  Connect to cluster" << std::endl;
    std::cerr << "  import <file_path>                 Import RDF data file" << std::endl;
    std::cerr << "  query  <sparql>                    Execute SPARQL query" << std::endl;
    std::cerr << "  query-batch <file> [file ...]      SELECT files on an unchanged dataset"
              << std::endl;
    return 1;
  }

  std::string command = argv[1];

  if (command == "init") {
    return RunInit(argc - 2, argv + 2);
  } else if (command == "import") {
    return RunImport(argc - 2, argv + 2);
  } else if (command == "query") {
    return RunQuery(argc - 2, argv + 2);
  } else if (command == "query-batch") {
    return RunQueryBatch(argc - 2, argv + 2);
  } else {
    std::cerr << "Unknown command: " << command << std::endl;
    return 1;
  }
}

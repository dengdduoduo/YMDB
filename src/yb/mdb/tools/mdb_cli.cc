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

#include <iostream>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "yb/mdb/adapter/mdb_adapter.h"
#include "yb/mdb/model/rdf_yb_model.h"
#include "yb/util/flags.h"

DEFINE_string(mdb_masters, "127.0.0.1:7100", "Master addresses");
DEFINE_string(mdb_keyspace, "mdb_test", "Keyspace name");
DEFINE_bool(mdb_use_partition_load, true, "Use partition loading for queries");
DEFINE_bool(mdb_use_transactions, true, "Use distributed transactions for import consistency and query snapshots");

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

static int RunQuery(int argc, char* argv[]) {
  if (argc < 1) {
    std::cerr << "Usage: mdb_cli query <sparql_query>" << std::endl;
    return 1;
  }

  yb::mdb::RdfYBModel::Instance().SetUsePartitionLoad(FLAGS_mdb_use_partition_load);
  yb::mdb::RdfYBModel::Instance().SetUseTransactions(FLAGS_mdb_use_transactions);

  int init_ret = YBCMdbInitCluster(
      FLAGS_mdb_masters.c_str(), FLAGS_mdb_keyspace.c_str());
  if (init_ret != 0) {
    return init_ret;
  }

  std::string query = argv[0];
  for (int i = 1; i < argc; i++) {
    query += " ";
    query += argv[i];
  }

  struct Ctx {
    std::vector<std::pair<std::string, std::string>> bindings;
  };

  Ctx ctx;

  auto callback = [](const char* name, const char* value, void* ctx_ptr) {
    auto* c = static_cast<Ctx*>(ctx_ptr);
    c->bindings.emplace_back(name, value);
  };

  int ret = YBCMdbExecuteSparql(query.c_str(), callback, &ctx);

  if (ctx.bindings.empty()) {
    return ret;
  }

  std::set<std::string> unique_vars;
  for (auto& [n, v] : ctx.bindings) {
    if (unique_vars.find(n) != unique_vars.end()) {
      break;
    }
    unique_vars.insert(n);
  }
  size_t vars_per_row = unique_vars.size();

  if (vars_per_row == 0) {
    return ret;
  }

  for (size_t i = 0; i < ctx.bindings.size(); i++) {
    if (i > 0 && i % vars_per_row == 0) {
      std::cout << "---" << std::endl;
    }
    std::cout << ctx.bindings[i].first << "\t" << ctx.bindings[i].second << std::endl;
  }

  return ret;
}

int main(int argc, char* argv[]) {
  yb::ParseCommandLineFlags(&argc, &argv, true);

  if (argc < 2) {
    std::cerr << "Usage: mdb_cli <command> [args...]" << std::endl;
    std::cerr << "Commands:" << std::endl;
    std::cerr << "  init   <master_addrs> <keyspace>  Connect to cluster" << std::endl;
    std::cerr << "  import <file_path>                 Import RDF data file" << std::endl;
    std::cerr << "  query  <sparql>                    Execute SPARQL query" << std::endl;
    return 1;
  }

  std::string command = argv[1];

  if (command == "init") {
    return RunInit(argc - 2, argv + 2);
  } else if (command == "import") {
    return RunImport(argc - 2, argv + 2);
  } else if (command == "query") {
    return RunQuery(argc - 2, argv + 2);
  } else {
    std::cerr << "Unknown command: " << command << std::endl;
    return 1;
  }
}

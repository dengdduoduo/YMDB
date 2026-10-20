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
#include <algorithm>
#include <cstdio>
#include <cstring>
#include <set>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "yb/mdb/adapter/mdb_adapter.h"
#include "yb/mdb/model/rdf_yb_model.h"
#include "yb/mdb/storage/docdb_read_context.h"
#include "yb/util/flags.h"
#include "yb/util/scope_exit.h"

DEFINE_string(mdb_masters, "127.0.0.1:7100", "Master addresses");
DEFINE_string(mdb_keyspace, "mdb_test", "Keyspace name");
DEFINE_bool(mdb_use_partition_load, true, "Use partition loading for queries");
DEFINE_bool(mdb_use_transactions, true, "Use distributed transactions for import consistency and query snapshots");
DEFINE_bool(mdb_streaming_reads, false, "Use query-owned transactional paged MDB reads (experimental)");
DEFINE_uint32(mdb_scan_page_rows, 256, "Maximum rows per MDB streaming storage page");
DEFINE_string(mdb_join_policy, "index", "Streaming BGP join policy: index, hash, auto");
DEFINE_bool(mdb_analyze_statistics, false, "Collect bounded metadata with a full snapshot scan before query");
DEFINE_uint32(mdb_hash_query_memory_bytes, 67108864, "Shared buffered-state budget for hash joins in a query");
DEFINE_bool(mdb_batch_index_join, true, "Batch supported streaming index joins");
DEFINE_uint32(mdb_join_input_rows, 64, "Maximum outer rows per index-join batch");
DEFINE_uint32(mdb_join_output_rows, 128, "Maximum output rows per index-join batch");
DEFINE_uint32(mdb_join_batch_bytes, 65536, "Binding batch buffer byte budget");
DEFINE_uint32(mdb_join_memory_bytes, 4194304, "Per-join buffered-state budget, excluding shared client/dictionary storage");
DEFINE_uint32(mdb_join_max_in_flight, 8, "Maximum index probe page operations per flush");
DEFINE_uint32(mdb_join_negative_cache_entries, 256, "Maximum cached empty index probes per join");
DEFINE_bool(mdb_query_profile, false, "Print streaming query operation/payload/buffer counts to stderr");
DEFINE_string(mdb_worker_targets, "", "Comma-separated SSH destinations, or local for isolated local processes");
DEFINE_string(mdb_worker_binary, "", "Absolute mdb_fragment_worker path on every destination");
DEFINE_string(mdb_worker_flagfile, "", "Optional absolute worker-local flagfile for YB client TLS configuration");
DEFINE_string(mdb_worker_ssh_config, "", "Optional absolute coordinator-local SSH configuration path");
DEFINE_uint32(mdb_worker_max_processes, 3, "Maximum concurrently active MDB fragment processes");
DEFINE_uint32(mdb_worker_batch_rows, 128, "Maximum rows per worker pull response");
DEFINE_uint32(mdb_worker_page_rows, 16, "Maximum storage rows per worker page");
DEFINE_uint32(mdb_worker_memory_bytes, 8388608, "Per-worker buffered-state budget; excludes shared YB client infrastructure");
DEFINE_uint32(mdb_global_memory_bytes, 67108864, "Global Gather input reservation budget; excludes shared infrastructure");
DEFINE_uint64(mdb_worker_output_spool_bytes, 268435456,
              "Maximum temporary CLI result bytes before publication; failure discards staged rows");

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
  yb::mdb::RdfYBModel::Instance().SetUseStreamingReads(FLAGS_mdb_streaming_reads);
  yb::mdb::RdfYBModel::Instance().SetStreamingPageRows(FLAGS_mdb_scan_page_rows);
  yb::mdb::BatchJoinOptions options;
  if (FLAGS_mdb_join_policy == "hash") options.policy = yb::mdb::JoinPolicy::Hash;
  else if (FLAGS_mdb_join_policy == "auto") options.policy = yb::mdb::JoinPolicy::Auto;
  else if (FLAGS_mdb_join_policy != "index") {
    std::cerr << "Invalid MDB join policy" << std::endl; return 1;
  }
  if (options.policy != yb::mdb::JoinPolicy::Index && !FLAGS_mdb_streaming_reads) {
    std::cerr << "Hash/auto policy requires streaming reads" << std::endl; return 1;
  }
  options.hash_query_memory_bytes = FLAGS_mdb_hash_query_memory_bytes;
  options.enabled = FLAGS_mdb_batch_index_join;
  options.input_rows = FLAGS_mdb_join_input_rows;
  options.output_rows = FLAGS_mdb_join_output_rows;
  options.batch_bytes = FLAGS_mdb_join_batch_bytes;
  options.memory_bytes = FLAGS_mdb_join_memory_bytes;
  options.max_in_flight = FLAGS_mdb_join_max_in_flight;
  options.negative_cache_entries = FLAGS_mdb_join_negative_cache_entries;
  yb::mdb::RdfYBModel::Instance().SetBatchJoinOptions(options);
  yb::mdb::WorkerOptions workers;
  if (!FLAGS_mdb_worker_targets.empty()) {
    if (!FLAGS_mdb_streaming_reads) {
      std::cerr << "MDB workers require streaming reads" << std::endl;
      return 1;
    }
    std::istringstream destinations(FLAGS_mdb_worker_targets);
    std::string target;
    while (std::getline(destinations, target, ',')) workers.targets.push_back(target);
    workers.binary = FLAGS_mdb_worker_binary;
    workers.flagfile = FLAGS_mdb_worker_flagfile;
    workers.ssh_config = FLAGS_mdb_worker_ssh_config;
    workers.max_workers = FLAGS_mdb_worker_max_processes;
    workers.batch_rows = FLAGS_mdb_worker_batch_rows;
    workers.page_rows = FLAGS_mdb_worker_page_rows;
    workers.memory_bytes = FLAGS_mdb_worker_memory_bytes;
    workers.coordinator_memory_bytes = FLAGS_mdb_global_memory_bytes;
  }
  yb::mdb::RdfYBModel::Instance().SetWorkerOptions(std::move(workers));
  yb::mdb::RdfYBModel::Instance().SetUseTransactions(FLAGS_mdb_use_transactions);

  int init_ret = YBCMdbInitCluster(
      FLAGS_mdb_masters.c_str(), FLAGS_mdb_keyspace.c_str());
  if (init_ret != 0) {
    return init_ret;
  }

  if (FLAGS_mdb_analyze_statistics) {
    auto status = yb::mdb::RdfYBModel::Instance().AnalyzeStatistics();
    if (!status.ok()) { std::cerr << status.ToString() << std::endl; return 1; }
  }
  if (FLAGS_mdb_query_profile)
    std::cerr << "MDB_STATISTICS " << yb::mdb::RdfYBModel::Instance().StatisticsJson() << std::endl;
  std::string query = argv[0];
  for (int i = 1; i < argc; i++) {
    query += " ";
    query += argv[i];
  }

  struct Ctx {
    std::vector<std::pair<std::string, std::string>> bindings;
    FILE* spool = nullptr;
    uint64_t bytes = 0;
    uint64_t records = 0;
    bool failed = false;
    bool first_row_complete = false;
    std::set<std::string> first_row_vars;
  };

  Ctx ctx;
  auto spool_cleanup = yb::ScopeExit([&] { if (ctx.spool) std::fclose(ctx.spool); });
  if (!FLAGS_mdb_worker_targets.empty()) {
    ctx.spool = std::tmpfile();
    if (!ctx.spool) { std::cerr << "Cannot create MDB result staging file" << std::endl; return 1; }
  }

  auto callback = [](const char* name, const char* value, void* ctx_ptr) {
    auto* c = static_cast<Ctx*>(ctx_ptr);
    if (c->spool) {
      if (c->failed) return;
      const auto name_size = std::strlen(name), value_size = std::strlen(value);
      const uint64_t size = name_size + value_size + 2;
      auto write = [&](const void* bytes, size_t length) {
        if (std::fwrite(bytes, 1, length, c->spool) != length) c->failed = true;
      };
      if (size > FLAGS_mdb_worker_output_spool_bytes ||
          c->bytes > FLAGS_mdb_worker_output_spool_bytes - size ||
          FLAGS_mdb_worker_output_spool_bytes - size - c->bytes < sizeof(size)) c->failed = true;
      if (!c->failed) {
        write(&size, sizeof(size)); write(name, name_size); write("\t", 1);
        write(value, value_size); write("\n", 1);
        c->bytes += size + sizeof(size);
        ++c->records;
        if (!c->first_row_complete && !c->first_row_vars.insert(name).second)
          c->first_row_complete = true;
      }
      if (c->failed) {
        if (auto* context = yb::mdb::DocDBReadContextScope::Current()) context->cancelled = true;
      }
      return;
    }
    c->bindings.emplace_back(name, value);
  };

  int ret = YBCMdbExecuteSparql(query.c_str(), callback, &ctx);
  if (FLAGS_mdb_query_profile && *YBCMdbGetLastQueryProfile()) {
    std::cerr << "MDB_QUERY_PROFILE " << YBCMdbGetLastQueryProfile() << std::endl;
  }
  // A later page can fail after callbacks have delivered earlier rows.
  // Do not print that incomplete result as a successful query answer.
  if (ctx.failed) {
    std::cerr << "MDB result staging failed or exceeded its byte limit" << std::endl;
    return 1;
  }
  if (ret != 0) return ret;

  if (ctx.spool) {
    if (ctx.failed || std::fflush(ctx.spool) || std::fseek(ctx.spool, 0, SEEK_SET)) {
      std::cerr << "MDB result staging failed or exceeded its byte limit" << std::endl;
      return 1;
    }
    const auto width = ctx.first_row_vars.size();
    char buffer[8192];
    for (uint64_t record = 0; record < ctx.records; ++record) {
      if (record && width && record % width == 0) std::cout << "---\n";
      uint64_t size;
      if (std::fread(&size, sizeof(size), 1, ctx.spool) != 1) return 1;
      while (size) {
        const auto part = static_cast<size_t>(std::min<uint64_t>(size, sizeof(buffer)));
        if (std::fread(buffer, 1, part, ctx.spool) != part) return 1;
        std::cout.write(buffer, part);
        size -= part;
      }
    }
    return std::cout.good() ? 0 : 1;
  }

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
  // Drain transaction RPCs before process-static response buffers are destroyed.
  auto cleanup = yb::ScopeExit([] { yb::mdb::RdfYBModel::Instance().ClearAll(); });

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

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
#include "yb/mdb/tools/cli_result_writer.h"
#include "yb/mdb/model/rdf_yb_model.h"
#include "yb/mdb/storage/docdb_read_context.h"
#include "yb/util/flags.h"
#include "yb/util/scope_exit.h"
#include "yb/mdb/query/executor/wall_time_scope.h"

DEFINE_string(mdb_result_format, "text", "Query output: text or json (typed SELECT/ASK; graph triples JSON)");
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
DEFINE_string(mdb_exchange, "none", "Opt-in two-triple exchange: none, hash, broadcast");
DEFINE_uint32(mdb_exchange_partitions, 4, "Number of local exchange partitions (2..64)");
DEFINE_uint32(mdb_exchange_memory_bytes, 8388608, "Exchange consumer and metadata reservation budget");
DEFINE_uint64(mdb_exchange_spill_bytes, 1073741824, "Per-query exchange disk budget including broadcast copies");
DEFINE_string(mdb_exchange_spill_directory, "/tmp", "Existing root for private query spill directories");
DEFINE_uint32(mdb_exchange_skew_factor, 2, "Partition skew threshold relative to average build partition rows");
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
  const auto command_started = yb::mdb::WallMicros();
  uint64_t init_us = 0, analyze_us = 0, execute_us = 0, publish_us = 0;
  int command_status = 1;
  auto timing_cleanup = yb::ScopeExit([&] {
    if (FLAGS_mdb_query_profile)
      std::cerr << "MDB_CLI_TIMING {\"init_us\":" << init_us
                << ",\"analyze_us\":" << analyze_us << ",\"execute_us\":" << execute_us
                << ",\"publish_us\":" << publish_us << ",\"total_us\":"
                << yb::mdb::WallMicros() - command_started
                << ",\"status\":" << command_status << "}" << std::endl;
  });
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
  if (FLAGS_mdb_exchange == "hash") workers.exchange = yb::mdb::ExchangeMode::Hash;
  else if (FLAGS_mdb_exchange == "broadcast") workers.exchange = yb::mdb::ExchangeMode::Broadcast;
  else if (FLAGS_mdb_exchange != "none") {
    std::cerr << "Invalid --mdb_exchange" << std::endl; return 1;
  }
  if (workers.exchange != yb::mdb::ExchangeMode::None && FLAGS_mdb_worker_targets.empty()) {
    std::cerr << "MDB exchange requires worker targets" << std::endl; return 1;
  }
  workers.exchange_partitions = FLAGS_mdb_exchange_partitions;
  workers.exchange_memory_bytes = FLAGS_mdb_exchange_memory_bytes;
  workers.exchange_spill_bytes = FLAGS_mdb_exchange_spill_bytes;
  workers.exchange_spill_directory = FLAGS_mdb_exchange_spill_directory;
  workers.exchange_skew_factor = FLAGS_mdb_exchange_skew_factor;
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

  int init_ret;
  {
    yb::mdb::WallTimeScope timer(&init_us);
    init_ret = YBCMdbInitCluster(FLAGS_mdb_masters.c_str(), FLAGS_mdb_keyspace.c_str());
  }
  if (init_ret != 0) {
    return init_ret;
  }

  if (FLAGS_mdb_analyze_statistics) {
    yb::mdb::WallTimeScope timer(&analyze_us);
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

  if (FLAGS_mdb_result_format != "text" && FLAGS_mdb_result_format != "json") {
    std::cerr << "Invalid --mdb_result_format" << std::endl;
    return 1;
  }
  try {
    MdbCliResultWriter writer(FLAGS_mdb_result_format == "json", FLAGS_mdb_worker_output_spool_bytes);
    int ret;
    {
      yb::mdb::WallTimeScope timer(&execute_us);
      ret = YBCMdbExecuteSparqlWithResults(
          query.c_str(), MdbCliResultWriter::Callbacks(), &writer);
    }
    if (FLAGS_mdb_query_profile && *YBCMdbGetLastQueryProfile())
      std::cerr << "MDB_QUERY_PROFILE " << YBCMdbGetLastQueryProfile() << std::endl;
    if (ret != 0) { command_status = ret; return ret; }
    {
      yb::mdb::WallTimeScope timer(&publish_us);
      command_status = writer.Publish();
    }
    return command_status;
  } catch (const std::exception& error) {
    std::cerr << error.what() << std::endl;
    return 1;
  }
}

int main(int argc, char* argv[]) {
  yb::ParseCommandLineFlags(&argc, &argv, true);
  // Drain transaction RPCs before process-static response buffers are destroyed.
  auto cleanup = yb::ScopeExit([] { (void)YBCMdbShutdown(); });

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

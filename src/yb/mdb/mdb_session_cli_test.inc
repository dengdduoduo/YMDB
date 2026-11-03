#include <chrono>
// Copyright (c) YugabyteDB, Inc.
// Included after the fragment fixtures to keep lifecycle cases in their own file.
namespace yb::mdb {
namespace {
TEST_F(MdbDictionaryRecoveryTest, SessionCliReusesInitializationAndRecoversFromBadRequest) {
  std::vector<std::string> queries = {
      "SELECT ?x WHERE { VALUES ?x { <urn:session:a> <urn:session:a> } }", "SELECT WHERE {",
      "INSERT DATA { <urn:session:new> <urn:session:p> \"fresh\"@en . }",
      "SELECT ?value WHERE { <urn:session:new> <urn:session:p> ?value }",
      "ASK { <urn:session:new> <urn:session:p> ?value }"};
  auto args = Base();
  args.insert(
      args.end(), {"--mdb_result_format=json", "--mdb_query_profile=true",
                   "--mdb_init_profile=true", "session"});
  for (size_t i = 0; i < queries.size(); ++i) {
    auto path = GetTestPath("session-" + std::to_string(i) + ".rq");
    {
      std::ofstream file(path);
      file << queries[i];
    }
    args.push_back(path);
  }
  std::string out, error;
  ASSERT_NOK(Subprocess::Call(args, &out, &error));
  std::istringstream lines(out);
  std::vector<std::string> docs;
  for (std::string line; std::getline(lines, line);) {
    if (!line.empty()) {
      docs.push_back(line);
    }
  }
  ASSERT_EQ(docs.size(), 3);
  rapidjson::Document doc;
  doc.Parse(docs[0].c_str());
  ASSERT_FALSE(doc.HasParseError());
  ASSERT_STREQ(doc["head"]["vars"][0].GetString(), "x");
  ASSERT_EQ(doc["results"]["bindings"].Size(), 2);
  for (const auto& row : doc["results"]["bindings"].GetArray()) {
    ASSERT_STREQ(row["x"]["type"].GetString(), "uri");
    ASSERT_STREQ(row["x"]["value"].GetString(), "urn:session:a");
  }
  doc.Parse(docs[1].c_str());
  ASSERT_FALSE(doc.HasParseError());
  ASSERT_STREQ(doc["head"]["vars"][0].GetString(), "value");
  ASSERT_EQ(doc["results"]["bindings"].Size(), 1);
  const auto& term = doc["results"]["bindings"][0]["value"];
  ASSERT_STREQ(term["type"].GetString(), "literal");
  ASSERT_STREQ(term["value"].GetString(), "fresh");
  ASSERT_STREQ(term["xml:lang"].GetString(), "en");
  doc.Parse(docs[2].c_str());
  ASSERT_FALSE(doc.HasParseError());
  ASSERT_TRUE(doc["boolean"].GetBool());
  size_t init_count = 0;
  for (size_t pos = 0; (pos = error.find("MDB_INIT_PROFILE ", pos)) != std::string::npos;
       pos += 17) {
    ++init_count;
  }
  ASSERT_EQ(init_count, 1);
  ASSERT_NE(error.find("MDB_SESSION_REQUEST {\"index\":1,\"status\":1}"), std::string::npos);
  ASSERT_NE(error.find("MDB_SESSION_REQUEST {\"index\":4,\"status\":0}"), std::string::npos);
}

TEST_F(MdbDictionaryRecoveryTest, SessionReadViewSurvivesExternalCommitAndRefreshesTerms) {
  const auto initial = GetTestPath("session-initial.nt");
  {
    std::ofstream f(initial);
    f << "<urn:session:s1> <urn:session:p> \"old\"@en .\n"
      << "<urn:session:s2> <urn:session:p> \"old\"@en .\n";
  }
  auto args = Base();
  args.insert(args.end(), {"import", initial});
  ASSERT_OK(Subprocess::Call(args));
  ASSERT_EQ(YBCMdbInitCluster(cluster_->GetMasterAddresses().c_str(), "mdb_p6_test"), 0);
  auto cleanup = ScopeExit([] { (void)YBCMdbShutdown(); });
  const auto additional = GetTestPath("session-additional.nt");
  {
    std::ofstream f(additional);
    f << "<urn:session:s3> <urn:session:p> \"new-term\"@fr .\n";
  }
  Status committed = STATUS(IllegalState, "External writer was not invoked");
  FirstBatchTypedOutput active;
  active.first_row = [&] {
    auto load = Base();
    load.insert(load.end(), {"import", additional});
    committed = Subprocess::Call(load);
  };
  const char* query = "SELECT ?s ?o WHERE { ?s <urn:session:p> ?o }";
  ASSERT_EQ(YBCMdbExecuteSparqlWithResults(query, active.Callbacks(), &active), 0);
  ASSERT_OK(committed);
  ASSERT_TRUE(active.ended);
  ASSERT_EQ(active.names, std::vector<std::string>({"s", "o"}));
  ASSERT_EQ(active.rows.size(), 2);
  for (const auto& row : active.rows) {
    ASSERT_EQ(std::get<1>(row[1]), "old");
    ASSERT_EQ(std::get<3>(row[1]), "en");
  }
  FirstBatchTypedOutput fresh;
  ASSERT_EQ(YBCMdbExecuteSparqlWithResults(query, fresh.Callbacks(), &fresh), 0);
  ASSERT_TRUE(fresh.ended);
  ASSERT_EQ(fresh.names, active.names);
  ASSERT_EQ(fresh.rows.size(), 3);
  size_t found = 0;
  for (const auto& row : fresh.rows) {
    if (std::get<1>(row[0]) == "urn:session:s3") {
      ASSERT_EQ(std::get<0>(row[1]), YBC_MDB_LITERAL);
      ASSERT_EQ(std::get<1>(row[1]), "new-term");
      ASSERT_EQ(std::get<3>(row[1]), "fr");
      ++found;
    }
  }
  ASSERT_EQ(found, 1);
}

TEST_F(MdbDictionaryRecoveryTest, SessionCliBoundsAndSpoolFailureKeepFollowingRequestClean) {
  const auto oversized = GetTestPath("session-large.rq");
  {
    std::ofstream f(oversized);
    f << std::string(1024 * 1024 + 1, ' ');
  }
  const auto nul = GetTestPath("session-nul.rq");
  {
    std::ofstream f(nul);
    f << "ASK {}";
    f.put('\0');
  }
  const auto large_result = GetTestPath("session-output-large.rq");
  {
    std::ofstream f(large_result);
    f << "SELECT ?x WHERE { VALUES ?x { <urn:session:" << std::string(512, 'a') << "> } }";
  }
  const auto valid = GetTestPath("session-ask.rq");
  {
    std::ofstream f(valid);
    f << "ASK {}";
  }
  auto args = Base();
  args.insert(
      args.end(), {"--mdb_result_format=json", "--mdb_worker_output_spool_bytes=128", "session",
                   oversized, nul, GetTestPath("absent.rq"), large_result, valid});
  std::string out, error;
  ASSERT_NOK(Subprocess::Call(args, &out, &error));
  rapidjson::Document doc;
  doc.Parse(out.c_str());
  ASSERT_FALSE(doc.HasParseError()) << out;
  ASSERT_TRUE(doc["boolean"].GetBool());
  ASSERT_EQ(out.find("urn:session:"), std::string::npos);
  for (int i = 0; i < 4; ++i) {
    ASSERT_NE(
        error.find("MDB_SESSION_REQUEST {\"index\":" + std::to_string(i) + ",\"status\":1}"),
        std::string::npos);
  }
  ASSERT_NE(error.find("MDB_SESSION_REQUEST {\"index\":4,\"status\":0}"), std::string::npos);
}

TEST_F(MdbDictionaryRecoveryTest, SessionCliGraphModesKeepCompleteBagsAcrossRequests) {
  const auto data = GetTestPath("session-graph.nt");
  {
    std::ofstream f(data);
    for (int i = 0; i < 16; ++i) {
      f << "<urn:session:s" << i << "> <urn:session:seed> <urn:session:k> .\n";
    }
    f << "<urn:session:k> <urn:session:edge> <urn:session:end> .\n";
  }
  auto load = Base();
  load.insert(load.end(), {"import", data});
  ASSERT_OK(Subprocess::Call(load));
  const auto query = GetTestPath("session-graph.rq");
  {
    std::ofstream f(query);
    f << "SELECT ?s ?o WHERE { ?s <urn:session:seed> ?k . ?k <urn:session:edge> ?o }";
  }
  for (const std::string mode : {"old", "new", "auto"}) {
    auto args = Base();
    args.insert(
        args.end(), {"--mdb_result_format=json", "--mdb_query_profile=true",
                     "--mdb_worker_targets=local", "--mdb_worker_binary=" + workers_.binary,
                     "--mdb_graph_mode=" + mode, "session", query, query});
    std::string out, error;
    ASSERT_OK(Subprocess::Call(args, &out, &error));
    std::istringstream lines(out);
    size_t count = 0;
    for (std::string line; std::getline(lines, line);) {
      rapidjson::Document doc;
      doc.Parse(line.c_str());
      ASSERT_FALSE(doc.HasParseError());
      ASSERT_EQ(doc["head"]["vars"].Size(), 2);
      ASSERT_STREQ(doc["head"]["vars"][0].GetString(), "s");
      ASSERT_STREQ(doc["head"]["vars"][1].GetString(), "o");
      std::multiset<std::string> actual, expected;
      for (const auto& row : doc["results"]["bindings"].GetArray()) {
        ASSERT_STREQ(row["s"]["type"].GetString(), "uri");
        ASSERT_STREQ(row["o"]["type"].GetString(), "uri");
        ASSERT_STREQ(row["o"]["value"].GetString(), "urn:session:end");
        actual.insert(row["s"]["value"].GetString());
      }
      for (int i = 0; i < 16; ++i) {
        expected.insert("urn:session:s" + std::to_string(i));
      }
      ASSERT_EQ(actual, expected);
      ++count;
    }
    ASSERT_EQ(count, 2);
    size_t profiles = 0;
    std::istringstream diagnostics(error);
    for (std::string line; std::getline(diagnostics, line);) {
      const auto pos = line.find("MDB_QUERY_PROFILE ");
      if (pos == std::string::npos) {
        continue;
      }
      rapidjson::Document profile;
      profile.Parse(line.substr(pos + 18).c_str());
      ASSERT_FALSE(profile.HasParseError());
      ASSERT_EQ(profile["graph_native_workers"].GetUint64(), mode == "old" ? 0 : 4);
      ASSERT_EQ(profile["remote_coordinator_fallbacks"].GetUint64(), 0);
      ++profiles;
    }
    ASSERT_EQ(profiles, 2);
  }
}

TEST_F(MdbDictionaryRecoveryTest, SessionLifecycleWorkerPoolComparison) {
  const auto* output = std::getenv("MDB_SESSION_BENCH_OUTPUT");
  if (!output) GTEST_SKIP() << "Set MDB_SESSION_BENCH_OUTPUT for the opt-in diagnostic";
  auto artifact = [&](const std::string& name) { return std::string(output) + "/" + name; };

  const auto data = GetTestPath("lifecycle-baseline.nt");
  {
    std::ofstream f(data);
    for (int i = 0; i < 16; ++i) {
      f << "<urn:lifecycle:s" << i << "> <urn:lifecycle:p> <urn:lifecycle:k> .\n";
    }
    f << "<urn:lifecycle:k> <urn:lifecycle:e> <urn:lifecycle:o> .\n";
  }
  auto load = Base();
  load.insert(load.end(), {"import", data});
  ASSERT_OK(Subprocess::Call(load));
  const std::string query =
      "SELECT ?s ?o WHERE { ?s <urn:lifecycle:p> ?k . ?k <urn:lifecycle:e> ?o }";
  const auto query_file = GetTestPath("lifecycle-baseline.rq");
  {
    std::ofstream f(query_file);
    f << query;
  }
  auto run = [&](int layout, int trial) {
    const bool session = layout != 0, reuse = layout == 2;
    const std::string name = reuse ? "pool" : session ? "session" : "oneshot";
    const std::string label = name + "-" + std::to_string(trial);
    uint64_t wall = 0, init = 0, open = 0, spawn = 0, execute = 0;
    size_t init_calls = 0, profiles = 0, documents = 0;
    for (int process = 0; process < (session ? 1 : 3); ++process) {
      auto args = Base();
      args.insert(
          args.end(),
          {"--mdb_result_format=json", "--mdb_query_profile=true", "--mdb_init_profile=true",
           "--mdb_graph_mode=new", "--mdb_worker_targets=local",
           "--mdb_worker_binary=" + workers_.binary, "--mdb_worker_max_processes=2"});
      if (reuse) args.push_back("--mdb_worker_reuse=true");
      if (session) {
        args.insert(args.end(), {"session", query_file, query_file, query_file});
      } else {
        args.insert(args.end(), {"query", query});
      }
      std::string out, error;
      const auto started = std::chrono::steady_clock::now();
      const auto status = Subprocess::Call(args, &out, &error);
      wall += std::chrono::duration_cast<std::chrono::microseconds>(
                  std::chrono::steady_clock::now() - started)
                  .count();
      {
        std::ofstream f(artifact(label + "-" + std::to_string(process) + ".stdout"));
        f << out;
      }
      {
        std::ofstream f(artifact(label + "-" + std::to_string(process) + ".stderr"));
        f << error;
      }
      ASSERT_OK(status);
      if (reuse) {
        const auto pos = error.find("MDB_WORKER_POOL ");
        ASSERT_NE(pos, std::string::npos);
        rapidjson::Document stats;
        stats.Parse(error.substr(pos + 16, error.find('\n', pos) - pos - 16).c_str());
        ASSERT_FALSE(stats.HasParseError());
        ASSERT_EQ(stats["process_starts"].GetUint64(), 2);
        ASSERT_EQ(stats["open_acks"].GetUint64(), 12);
        ASSERT_EQ(stats["runtime_reuse_acks"].GetUint64(), 10);
      }
      std::istringstream lines(out);
      for (std::string line; std::getline(lines, line);) {
        rapidjson::Document doc;
        doc.Parse(line.c_str());
        ASSERT_FALSE(doc.HasParseError());
        ASSERT_EQ(doc["head"]["vars"].Size(), 2);
        ASSERT_STREQ(doc["head"]["vars"][0].GetString(), "s");
        ASSERT_STREQ(doc["head"]["vars"][1].GetString(), "o");
        std::multiset<std::string> actual, expected;
        for (const auto& row : doc["results"]["bindings"].GetArray()) {
          ASSERT_STREQ(row["s"]["type"].GetString(), "uri");
          ASSERT_STREQ(row["o"]["type"].GetString(), "uri");
          ASSERT_STREQ(row["o"]["value"].GetString(), "urn:lifecycle:o");
          actual.insert(row["s"]["value"].GetString());
        }
        for (int i = 0; i < 16; ++i) {
          expected.insert("urn:lifecycle:s" + std::to_string(i));
        }
        ASSERT_EQ(actual, expected);
        ++documents;
      }
      std::istringstream diagnostics(error);
      for (std::string line; std::getline(diagnostics, line);) {
        for (const std::string marker :
             {"MDB_CLI_TIMING ", "MDB_QUERY_PROFILE ", "MDB_INIT_PROFILE "}) {
          const auto pos = line.find(marker);
          if (pos == std::string::npos) {
            continue;
          }
          rapidjson::Document doc;
          doc.Parse(line.substr(pos + marker.size()).c_str());
          ASSERT_FALSE(doc.HasParseError());
          if (marker == "MDB_CLI_TIMING ") {
            init += doc["init_us"].GetUint64();
          } else if (marker == "MDB_INIT_PROFILE ") {
            ++init_calls;
          } else {
            ASSERT_EQ(doc["graph_native_workers"].GetUint64(), 4);
            ASSERT_EQ(doc["remote_coordinator_fallbacks"].GetUint64(), 0);
            open += doc["remote_open_wait_us"].GetUint64();
            spawn += doc["remote_spawn_us"].GetUint64();
            execute += doc["adapter_execute_us"].GetUint64();
            ++profiles;
          }
        }
      }
    }
    ASSERT_EQ(documents, 3);
    ASSERT_EQ(profiles, 3);
    ASSERT_EQ(init_calls, session ? 1 : 3);
    LOG(INFO) << "MDB_SESSION_BASELINE {\"layout\":\"" << name
              << "\",\"trial\":" << trial << ",\"requests\":3,\"wall_us\":" << wall
              << ",\"init_us\":" << init << ",\"open_us\":" << open << ",\"spawn_us\":" << spawn
              << ",\"execute_us\":" << execute << ",\"init_calls\":" << init_calls << "}";
  };
  for (int trial = -1; trial < 5; ++trial) {
    for (int offset = 0; offset < 3; ++offset) {
      run((trial + 3 + offset) % 3, trial);
      ASSERT_FALSE(HasFatalFailure());
    }
  }
  LOG(INFO) << "MDB_SESSION_BASELINE_ARTIFACTS " << artifact("lifecycle-baseline.rq");
}

TEST_F(MdbDictionaryRecoveryTest, PublicSessionPoolReusesInfrastructureAndPreservesResults) {
  const auto data = GetTestPath("public-pool.nt");
  {
    std::ofstream f(data);
    for (int i = 0; i < 16; ++i) {
      f << "<urn:pool:s" << i << "> <urn:pool:p> <urn:pool:k> .\n";
    }
    f << "<urn:pool:k> <urn:pool:e> <urn:pool:o> .\n";
  }
  auto load = Base();
  load.insert(load.end(), {"import", data});
  ASSERT_OK(Subprocess::Call(load));
  const auto query = GetTestPath("public-pool.rq");
  {
    std::ofstream f(query);
    f << "SELECT ?s ?o WHERE { ?s <urn:pool:p> ?k . ?k <urn:pool:e> ?o }";
  }
  const auto bad = GetTestPath("public-pool-bad.rq");
  {
    std::ofstream f(bad);
    f << "SELECT WHERE {";
  }
  auto args = Base();
  args.insert(
      args.end(),
      {"--mdb_result_format=json", "--mdb_query_profile=true", "--mdb_graph_mode=new",
       "--mdb_worker_targets=local", "--mdb_worker_binary=" + workers_.binary,
       "--mdb_worker_max_processes=2", "--mdb_worker_reuse=true", "session", query, bad, query});
  std::string out, error;
  ASSERT_NOK(Subprocess::Call(args, &out, &error));
  std::istringstream lines(out);
  size_t documents = 0;
  for (std::string line; std::getline(lines, line);) {
    rapidjson::Document doc;
    doc.Parse(line.c_str());
    ASSERT_FALSE(doc.HasParseError());
    ASSERT_EQ(doc["head"]["vars"].Size(), 2);
    ASSERT_STREQ(doc["head"]["vars"][0].GetString(), "s");
    ASSERT_STREQ(doc["head"]["vars"][1].GetString(), "o");
    std::multiset<std::string> actual, expected;
    for (const auto& row : doc["results"]["bindings"].GetArray()) {
      ASSERT_STREQ(row["s"]["type"].GetString(), "uri");
      ASSERT_STREQ(row["o"]["type"].GetString(), "uri");
      ASSERT_STREQ(row["o"]["value"].GetString(), "urn:pool:o");
      actual.insert(row["s"]["value"].GetString());
    }
    for (int i = 0; i < 16; ++i) {
      expected.insert("urn:pool:s" + std::to_string(i));
    }
    ASSERT_EQ(actual, expected);
    ++documents;
  }
  ASSERT_EQ(documents, 2);
  const auto pos = error.find("MDB_WORKER_POOL ");
  ASSERT_NE(pos, std::string::npos);
  const auto end = error.find('\n', pos);
  rapidjson::Document stats;
  stats.Parse(error.substr(pos + 16, end - pos - 16).c_str());
  ASSERT_FALSE(stats.HasParseError());
  ASSERT_EQ(stats["process_starts"].GetUint64(), 2);
  ASSERT_EQ(stats["open_acks"].GetUint64(), 8);
  ASSERT_EQ(stats["runtime_reuse_acks"].GetUint64(), 6);
}

TEST_F(MdbDictionaryRecoveryTest, PublicSessionPoolOldAndAutoKeepCompleteBags) {
  const auto data = GetTestPath("public-pool.nt");
  {
    std::ofstream f(data);
    for (int i = 0; i < 16; ++i) {
      f << "<urn:pool:s" << i << "> <urn:pool:p> <urn:pool:k> .\n";
    }
    f << "<urn:pool:k> <urn:pool:e> <urn:pool:o> .\n";
  }
  auto load = Base();
  load.insert(load.end(), {"import", data});
  ASSERT_OK(Subprocess::Call(load));
  const auto query = GetTestPath("public-pool.rq");
  {
    std::ofstream f(query);
    f << "SELECT ?s ?o WHERE { ?s <urn:pool:p> ?k . ?k <urn:pool:e> ?o }";
  }
  const auto bad = GetTestPath("public-pool-bad.rq");
  {
    std::ofstream f(bad);
    f << "SELECT WHERE {";
  }
  for (const std::string mode : {"old", "auto"}) {
    auto args = Base();
    args.insert(
        args.end(),
        {"--mdb_result_format=json", "--mdb_query_profile=true", "--mdb_graph_mode=" + mode,
         "--mdb_worker_targets=local", "--mdb_worker_binary=" + workers_.binary,
         "--mdb_worker_max_processes=2", "--mdb_worker_reuse=true", "session", query, bad, query});
    std::string out, error;
    ASSERT_NOK(Subprocess::Call(args, &out, &error));
    std::istringstream lines(out);
    size_t documents = 0;
    for (std::string line; std::getline(lines, line);) {
      rapidjson::Document doc;
      doc.Parse(line.c_str());
      ASSERT_FALSE(doc.HasParseError());
      ASSERT_EQ(doc["head"]["vars"].Size(), 2);
      ASSERT_STREQ(doc["head"]["vars"][0].GetString(), "s");
      ASSERT_STREQ(doc["head"]["vars"][1].GetString(), "o");
      std::multiset<std::string> actual, expected;
      for (const auto& row : doc["results"]["bindings"].GetArray()) {
        ASSERT_STREQ(row["s"]["type"].GetString(), "uri");
        ASSERT_STREQ(row["o"]["type"].GetString(), "uri");
        ASSERT_STREQ(row["o"]["value"].GetString(), "urn:pool:o");
        actual.insert(row["s"]["value"].GetString());
      }
      for (int i = 0; i < 16; ++i) {
        expected.insert("urn:pool:s" + std::to_string(i));
      }
      ASSERT_EQ(actual, expected);
      ++documents;
    }
    ASSERT_EQ(documents, 2);
    const auto pos = error.find("MDB_WORKER_POOL ");
    ASSERT_NE(pos, std::string::npos);
    const auto end = error.find('\n', pos);
    rapidjson::Document stats;
    stats.Parse(error.substr(pos + 16, end - pos - 16).c_str());
    ASSERT_FALSE(stats.HasParseError());
    ASSERT_EQ(stats["process_starts"].GetUint64(), 2);
    ASSERT_EQ(stats["open_acks"].GetUint64(), 8);
    ASSERT_EQ(stats["runtime_reuse_acks"].GetUint64(), 6);
  }
}

TEST_F(MdbDictionaryRecoveryTest, PublicSessionLateReleaseFailurePreservesSuccessfulQueries) {
  const auto data = GetTestPath("release-gate.nt");
  {
    std::ofstream f(data);
    for (int i = 0; i < 16; ++i)
      f << "<urn:gate:s" << i << "> <urn:gate:p> <urn:gate:k> .\n";
    f << "<urn:gate:k> <urn:gate:e> <urn:gate:o> .\n";
  }
  auto load = Base();
  load.insert(load.end(), {"import", data});
  ASSERT_OK(Subprocess::Call(load));
  const auto query = GetTestPath("release-gate.rq");
  {
    std::ofstream f(query);
    f << "SELECT ?s ?o WHERE { ?s <urn:gate:p> ?k . ?k <urn:gate:e> ?o }";
  }
  const auto proxy = GetTestPath("release-gate-proxy");
  {
    std::ofstream f(proxy);
    f << "#!/usr/bin/python3\nimport subprocess,sys,struct\n"
      << "p=subprocess.Popen(['" << workers_.binary
      << "']+sys.argv[1:],stdin=subprocess.PIPE,stdout=subprocess.PIPE)\n"
      << R"PY(
def read(f,n):
 b=b''
 while len(b)<n:
  x=f.read(n-len(b))
  if not x: raise EOFError()
  b+=x
 return b
def frame(f):
 return read(f,struct.unpack('!I',read(f,4))[0])
def send(f,b):
 f.write(struct.pack('!I',len(b))+b);f.flush()
def varint(b,i):
 value=shift=0
 while True:
  x=b[i];i+=1;value|=(x&127)<<shift
  if x<128: return value,i
  shift+=7
def fields(b):
 i=0;result={}
 while i<len(b):
  tag,i=varint(b,i);wire=tag&7;key=tag>>3
  if wire==0: result[key],i=varint(b,i)
  elif wire==1: i+=8
  elif wire==2:
   size,i=varint(b,i);result[key]=b[i:i+size];i+=size
  elif wire==5: i+=4
  else: raise ValueError('Unsupported test wire type')
 return result
try:
 rows={}
 while True:
  req=frame(sys.stdin.buffer);q=fields(req);send(p.stdin,req)
  ack=frame(p.stdout);a=fields(ack);key=q.get(2,b'')
  rows[key]=rows.get(key,0)+a.get(8,0)
  if q.get(32)==1 and q.get(30)==8:
   print('MDB_TEST_LATE_RELEASE rows='+str(rows[key])+' lease=8',file=sys.stderr,flush=True)
   ack+=b'\x88\x02\x01'
  send(sys.stdout.buffer,ack)
except (EOFError,BrokenPipeError): pass
finally:
 p.stdin.close();p.wait(timeout=5)
)PY";
  }
  ASSERT_EQ(chmod(proxy.c_str(), 0700), 0);
  for (const std::string mode : {"old", "new", "auto"}) {
    auto args = Base();
    args.insert(args.end(), {"--mdb_result_format=json", "--mdb_query_profile=true",
        "--mdb_graph_mode=" + mode, "--mdb_worker_targets=local",
        "--mdb_worker_binary=" + proxy, "--mdb_worker_max_processes=1",
        "--mdb_worker_reuse=true", "session", query, query, query});
    std::string out, error;
    ASSERT_NOK(Subprocess::Call(args, &out, &error));
    ASSERT_NE(error.find("MDB_TEST_LATE_RELEASE rows=16 lease=8"), std::string::npos) << error;
    ASSERT_NE(error.find("Invalid MDB worker release acknowledgement"), std::string::npos);
    for (int i = 0; i < 3; ++i) {
      ASSERT_NE(error.find("MDB_SESSION_REQUEST {\"index\":" + std::to_string(i) +
          ",\"status\":" + (i == 1 ? "1}" : "0}")), std::string::npos) << error;
    }
    std::istringstream lines(out);
    size_t documents = 0;
    for (std::string line; std::getline(lines, line);) {
      rapidjson::Document doc;
      doc.Parse(line.c_str());
      ASSERT_FALSE(doc.HasParseError());
      ASSERT_EQ(doc["head"]["vars"].Size(), 2);
      ASSERT_STREQ(doc["head"]["vars"][0].GetString(), "s");
      ASSERT_STREQ(doc["head"]["vars"][1].GetString(), "o");
      std::multiset<std::string> actual, expected;
      for (const auto& row : doc["results"]["bindings"].GetArray()) {
        ASSERT_STREQ(row["s"]["type"].GetString(), "uri");
        ASSERT_STREQ(row["o"]["type"].GetString(), "uri");
        ASSERT_STREQ(row["o"]["value"].GetString(), "urn:gate:o");
        actual.insert(row["s"]["value"].GetString());
      }
      for (int i = 0; i < 16; ++i) expected.insert("urn:gate:s" + std::to_string(i));
      ASSERT_EQ(actual, expected);
      ++documents;
    }
    ASSERT_EQ(documents, 2);
    const auto pos = error.find("MDB_WORKER_POOL ");
    ASSERT_NE(pos, std::string::npos);
    rapidjson::Document stats;
    stats.Parse(error.substr(pos + 16, error.find('\n', pos) - pos - 16).c_str());
    ASSERT_FALSE(stats.HasParseError());
    ASSERT_EQ(stats["process_starts"].GetUint64(), 2);
    ASSERT_EQ(stats["open_acks"].GetUint64(), 12);
    ASSERT_EQ(stats["runtime_reuse_acks"].GetUint64(), 10);
  }
}

TEST_F(MdbDictionaryRecoveryTest, PoolTypedResultsRefreshTermsAfterExternalWriter) {
  const auto initial = GetTestPath("pool-terms-initial.nt");
  {
    std::ofstream f(initial);
    for (int mode = 0; mode < 3; ++mode) {
      const auto prefix = "urn:poolterms:" + std::to_string(mode) + ":";
      for (int s = 1; s <= 2; ++s)
        f << "<" << prefix << "s" << s << "> <" << prefix << "p> <" << prefix << "k> .\n";
      f << "<" << prefix << "k> <" << prefix << "e> \"old\"@en .\n";
    }
  }
  auto load = Base();
  load.insert(load.end(), {"import", initial});
  ASSERT_OK(Subprocess::Call(load));
  ASSERT_EQ(YBCMdbInitCluster(cluster_->GetMasterAddresses().c_str(), "mdb_p6_test"), 0);
  auto cleanup = ScopeExit([] { (void)YBCMdbShutdown(); });
  int index = 0;
  for (const auto mode : {GraphMode::Old, GraphMode::New, GraphMode::Auto}) {
    const auto prefix = "urn:poolterms:" + std::to_string(index++) + ":";
    const auto additional = GetTestPath("pool-terms-more-" + std::to_string(index) + ".nt");
    const auto new_language = "new-term-" + std::to_string(index);
    const auto new_integer = std::to_string(700 + index);
    {
      std::ofstream f(additional);
      f << "<" << prefix << "s3> <" << prefix << "p> <" << prefix << "k> .\n"
        << "<" << prefix << "k> <" << prefix << "e> \"" << new_language << "\"@fr .\n"
        << "<" << prefix << "k> <" << prefix
        << "e> \"" << new_integer << "\"^^<http://www.w3.org/2001/XMLSchema#integer> .\n";
    }
    auto options = workers_;
    options.reuse_processes = true;
    options.max_workers = 1;
    options.graph_mode = mode;
    RdfYBModel::Instance().SetWorkerOptions(options);
    FragmentWorkerPool pool(1);
    const auto query = "SELECT ?s ?o WHERE { ?s <" + prefix + "p> ?k . ?k <" + prefix + "e> ?o }";
    Status committed = STATUS(IllegalState, "External writer was not invoked");
    FirstBatchTypedOutput active;
    active.first_row = [&] {
      auto writer = Base();
      writer.insert(writer.end(), {"import", additional});
      committed = Subprocess::Call(writer);
    };
    ASSERT_EQ(YBCMdbExecuteSparqlWithResults(query.c_str(), active.Callbacks(), &active), 0);
    ASSERT_OK(committed);
    FirstBatchTypedOutput fresh;
    ASSERT_EQ(YBCMdbExecuteSparqlWithResults(query.c_str(), fresh.Callbacks(), &fresh), 0);
    using Term = FirstBatchTypedOutput::Term;
    const Term old{YBC_MDB_LITERAL, "old", "", "en"};
    const Term language{YBC_MDB_LITERAL, new_language, "", "fr"};
    const Term integer{YBC_MDB_LITERAL, new_integer, "http://www.w3.org/2001/XMLSchema#integer", ""};
    std::multiset<std::vector<Term>> expected_old, expected_new;
    for (int s = 1; s <= 3; ++s) {
      const Term subject{YBC_MDB_IRI, prefix + "s" + std::to_string(s), "", ""};
      if (s <= 2) expected_old.insert({subject, old});
      for (const auto& object : {old, language, integer}) expected_new.insert({subject, object});
    }
    ASSERT_TRUE(active.ended);
    ASSERT_TRUE(fresh.ended);
    ASSERT_EQ(active.names, std::vector<std::string>({"s", "o"}));
    ASSERT_EQ(fresh.names, active.names);
    ASSERT_EQ(std::multiset<std::vector<Term>>(active.rows.begin(), active.rows.end()), expected_old);
    ASSERT_EQ(std::multiset<std::vector<Term>>(fresh.rows.begin(), fresh.rows.end()), expected_new);
    rapidjson::Document stats;
    stats.Parse(pool.StatsJson().c_str());
    ASSERT_EQ(stats["process_starts"].GetUint64(), 1);
    ASSERT_EQ(stats["open_acks"].GetUint64(), 8);
    ASSERT_EQ(stats["runtime_reuse_acks"].GetUint64(), 7);
    ASSERT_OK(pool.Close());
  }
}

TEST_F(MdbDictionaryRecoveryTest, PoolBudgetsPublishNoPartialRowsAndRecover) {
  const auto data = GetTestPath("pool-budget.nt");
  {
    std::ofstream f(data);
    for (int i = 0; i < 256; ++i)
      f << "<urn:budget:s" << i << "> <urn:budget:p> <urn:budget:k> .\n";
    f << "<urn:budget:k> <urn:budget:e> <urn:budget:o> .\n"
      << "<urn:budget:one> <urn:budget:smallp> <urn:budget:smallk> .\n"
      << "<urn:budget:smallk> <urn:budget:smalle> <urn:budget:ok> .\n";
  }
  auto load = Base();
  load.insert(load.end(), {"import", data});
  ASSERT_OK(Subprocess::Call(load));
  const auto small = GetTestPath("pool-budget-small.rq");
  const auto large = GetTestPath("pool-budget-large.rq");
  { std::ofstream f(small); f << "SELECT ?s ?o WHERE { ?s <urn:budget:smallp> ?k . ?k <urn:budget:smalle> ?o }"; }
  { std::ofstream f(large); f << "SELECT DISTINCT ?s ?o WHERE { ?s <urn:budget:p> ?k . ?k <urn:budget:e> ?o }"; }
  for (const std::string mode : {"old", "new", "auto"}) {
    auto control = Base();
    control.insert(control.end(), {"--mdb_result_format=json", "--mdb_graph_mode=" + mode,
        "--mdb_worker_targets=local", "--mdb_worker_binary=" + workers_.binary,
        "--mdb_worker_max_processes=1", "--mdb_worker_reuse=true", "session", large});
    std::string full, diagnostic;
    ASSERT_OK(Subprocess::Call(control, &full, &diagnostic));
    rapidjson::Document complete;
    complete.Parse(full.c_str());
    ASSERT_FALSE(complete.HasParseError());
    ASSERT_EQ(complete["head"]["vars"].Size(), 2);
    ASSERT_STREQ(complete["head"]["vars"][0].GetString(), "s");
    ASSERT_STREQ(complete["head"]["vars"][1].GetString(), "o");
    std::multiset<std::string> actual, expected;
    for (const auto& row : complete["results"]["bindings"].GetArray()) {
      ASSERT_STREQ(row["s"]["type"].GetString(), "uri");
      ASSERT_STREQ(row["o"]["type"].GetString(), "uri");
      ASSERT_STREQ(row["o"]["value"].GetString(), "urn:budget:o");
      actual.insert(row["s"]["value"].GetString());
    }
    for (int i = 0; i < 256; ++i) expected.insert("urn:budget:s" + std::to_string(i));
    ASSERT_EQ(actual, expected);

    for (const std::string budget : {"--mdb_global_memory_bytes=4096",
                                   "--mdb_worker_output_spool_bytes=256"}) {
      auto args = Base();
      args.insert(args.end(), {"--mdb_result_format=json", "--mdb_query_profile=true",
          "--mdb_graph_mode=" + mode, "--mdb_worker_targets=local",
          "--mdb_worker_binary=" + workers_.binary, "--mdb_worker_max_processes=1",
          "--mdb_worker_reuse=true", budget, "session", small, large, small});
      std::string out, error;
      ASSERT_NOK(Subprocess::Call(args, &out, &error));
      const bool global = budget.find("global") != std::string::npos;
      const bool expected_error = global
          ? error.find("MDB global DISTINCT input budget exceeded") != std::string::npos ||
                error.find("MDB global Gather input budget exceeded") != std::string::npos
          : error.find("MDB result staging failed or exceeded its byte limit") != std::string::npos;
      ASSERT_TRUE(expected_error) << error;
      for (int i = 0; i < 3; ++i)
        ASSERT_NE(error.find("MDB_SESSION_REQUEST {\"index\":" + std::to_string(i) +
            ",\"status\":" + (i == 1 ? "1}" : "0}")), std::string::npos) << error;
      std::istringstream lines(out);
      size_t documents = 0;
      for (std::string line; std::getline(lines, line);) {
        rapidjson::Document doc;
        doc.Parse(line.c_str());
        ASSERT_FALSE(doc.HasParseError());
        ASSERT_EQ(doc["head"]["vars"].Size(), 2);
        ASSERT_STREQ(doc["head"]["vars"][0].GetString(), "s");
        ASSERT_STREQ(doc["head"]["vars"][1].GetString(), "o");
        ASSERT_EQ(doc["results"]["bindings"].Size(), 1);
        const auto& term = doc["results"]["bindings"][0]["o"];
        ASSERT_STREQ(term["type"].GetString(), "uri");
        ASSERT_STREQ(term["value"].GetString(), "urn:budget:ok");
        const auto& subject = doc["results"]["bindings"][0]["s"];
        ASSERT_STREQ(subject["type"].GetString(), "uri");
        ASSERT_STREQ(subject["value"].GetString(), "urn:budget:one");
        ++documents;
      }
      ASSERT_EQ(documents, 2);
      std::istringstream diagnostics(error);
      size_t profile_count = 0;
      for (std::string line; std::getline(diagnostics, line);) {
        const auto marker = line.find("MDB_QUERY_PROFILE ");
        if (marker == std::string::npos) continue;
        rapidjson::Document profile;
        profile.Parse(line.substr(marker + 18).c_str());
        ASSERT_FALSE(profile.HasParseError());
        if (profile_count != 1) {
          ASSERT_EQ(profile["remote_fragments"].GetUint64(), 4);
          ASSERT_EQ(profile["remote_coordinator_fallbacks"].GetUint64(), 0);
        }
        ++profile_count;
      }
      ASSERT_EQ(profile_count, 3);
      const auto pos = error.find("MDB_WORKER_POOL ");
      ASSERT_NE(pos, std::string::npos);
      rapidjson::Document stats;
      stats.Parse(error.substr(pos + 16, error.find('\n', pos) - pos - 16).c_str());
      ASSERT_FALSE(stats.HasParseError());
      if (global) ASSERT_GE(stats["process_starts"].GetUint64(), 2);
      else ASSERT_EQ(stats["process_starts"].GetUint64(), 1);
      ASSERT_GT(stats["runtime_reuse_acks"].GetUint64(), 0);
      LOG(INFO) << "MDB_POOL_BUDGET mode=" << mode << " budget=" << budget
                << " successful_documents=" << documents << " stats=" << stats["open_acks"].GetUint64();
    }
  }
}

}  // namespace
}  // namespace yb::mdb

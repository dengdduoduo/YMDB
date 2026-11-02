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

}  // namespace
}  // namespace yb::mdb

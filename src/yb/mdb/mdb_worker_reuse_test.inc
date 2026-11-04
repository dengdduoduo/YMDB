// Copyright (c) YugabyteDB, Inc.
namespace yb::mdb {
namespace {
class MdbWorkerReuseTest : public MdbWorkerProbeTest {
 protected:
  FragmentRequestPB Envelope(uint64_t lease, uint64_t sequence = 0) {
    FragmentRequestPB request;
    request.set_version(3);
    request.set_reuse_version(1);
    request.set_lease_id(lease);
    request.set_query_id("reuse-probe");
    request.set_attempt_id(1);
    request.set_fragment_id(0);
    request.set_stream_id(0);
    request.set_batch_seq(sequence);
    return request;
  }
  std::unique_ptr<Subprocess> Start(bool enabled) {
    std::vector<std::string> args = {workers_.binary, "--logtostderr=true"};
    if (enabled) {
      args.push_back("--mdb_worker_reuse_protocol=true");
    }
    auto process = std::make_unique<Subprocess>(args.front(), args);
    process->PipeParentStdin();
    process->PipeParentStdout();
    EXPECT_OK(process->Start());
    EXPECT_OK(SetFragmentNonblocking(process->to_child_stdin_fd()));
    EXPECT_OK(SetFragmentNonblocking(process->from_child_stdout_fd()));
    return process;
  }
  Status Exchange(
      Subprocess& process, const FragmentRequestPB& request, FragmentResponsePB* response) {
    auto deadline = CoarseMonoClock::Now() + MonoDelta::FromSeconds(20);
    RETURN_NOT_OK(WriteFragmentFrame(process.to_child_stdin_fd(), request, deadline));
    response->Clear();
    RETURN_NOT_OK(ReadFragmentFrame(process.from_child_stdout_fd(), response, deadline));
    if (response->version() != request.version() || response->query_id() != request.query_id() ||
        response->lease_id() != request.lease_id() ||
        response->batch_seq() != request.batch_seq()) {
      return STATUS(Corruption, "Reuse test response identity mismatch");
    }
    return Status::OK();
  }
  void Open(FragmentRequestPB* request, const FragmentPlan& plan, DocDBReadContext& view) {
    auto child = view.ExportChild();
    ASSERT_OK(child);
    *request->mutable_open() = plan.physical;
    *request->mutable_open()->mutable_transaction() = *child;
    request->mutable_open()->set_timeout_ms(20000);
  }
};

TEST_F(MdbWorkerReuseTest, ReleaseAckAllowsFreshGraphChildOnSameProcess) {
  Seed(8);
  auto plan = Plan({{-1, 9000, -2}, {-2, 9001, -3}}, 3, {0, 2}, 2, 1);
  ASSERT_OK(plan);
  plan->physical.set_graph_version(1);
  auto process = Start(true);
  uint64_t pid = 0;
  for (uint64_t lease = 1; lease <= 2; ++lease) {
    auto view = RdfYBModel::Instance().NewQueryReadContext();
    ASSERT_OK(view);
    auto request = Envelope(lease);
    Open(&request, *plan, **view);
    ASSERT_FALSE(HasFatalFailure());
    FragmentResponsePB response;
    ASSERT_OK(Exchange(*process, request, &response));
    ASSERT_FALSE(response.has_error()) << response.error();
    ASSERT_EQ(response.reuse_version(), 1);
    ASSERT_EQ(response.runtime_reused(), lease > 1);
    ASSERT_EQ(response.graph_version(), 1);
    ASSERT_FALSE(response.eof());
    ASSERT_FALSE(response.released());
    if (!pid) {
      pid = response.worker_pid();
    }
    ASSERT_EQ(response.worker_pid(), pid);
    request.clear_open();
    Rows actual, expected;
    for (uint64_t s = 101; s < 109; ++s) {
      expected.insert({s, 1001});
      expected.insert({s, 1002});
      if (lease == 2) {
        expected.insert({s, 1003});
      }
    }
    ChildTransactionResultPB child;
    do {
      request.set_batch_seq(request.batch_seq() + 1);
      ASSERT_OK(Exchange(*process, request, &response));
      ASSERT_FALSE(response.has_error()) << response.error();
      ASSERT_EQ(response.worker_pid(), pid);
      ASSERT_EQ(response.values_size(), response.rows() * 2);
      for (uint32_t row = 0; row < response.rows(); ++row) {
        actual.insert({response.values(row * 2), response.values(row * 2 + 1)});
      }
      if (response.eof()) {
        ASSERT_TRUE(response.has_transaction_result());
        child = response.transaction_result();
      }
    } while (!response.eof());
    ASSERT_EQ(actual, expected);
    request.set_batch_seq(request.batch_seq() + 1);
    request.set_release(true);
    ASSERT_OK(Exchange(*process, request, &response));
    ASSERT_FALSE(response.has_error()) << response.error();
    ASSERT_TRUE(response.released());
    ASSERT_EQ(response.reuse_version(), 1);
    ASSERT_FALSE(response.has_transaction_result());
    ASSERT_EQ(response.values_size(), 0);
    ASSERT_OK((*view)->ApplyChild(child));
    if (lease == 1) {
      Write(1, 9001, 1003);
      ASSERT_OK(RdfYBModel::Instance().Flush());
    }
  }
  auto close = Envelope(3);
  close.set_shutdown(true);
  FragmentResponsePB response;
  ASSERT_OK(Exchange(*process, close, &response));
  ASSERT_FALSE(response.has_error());
  ASSERT_TRUE(response.shutdown_ack());
  ASSERT_EQ(response.worker_pid(), pid);
  ASSERT_OK(process->Wait());
}

TEST_F(MdbWorkerReuseTest, DisabledCapabilityAndPrematureReleaseAreRejected) {
  Seed(1);
  auto plan = Plan({{-1, 9000, -2}}, 2, {0, 1}, 2, 1);
  ASSERT_OK(plan);
  for (bool enabled : {false, true}) {
    auto process = Start(enabled);
    auto view = RdfYBModel::Instance().NewQueryReadContext();
    ASSERT_OK(view);
    auto request = Envelope(1);
    Open(&request, *plan, **view);
    ASSERT_FALSE(HasFatalFailure());
    FragmentResponsePB response;
    ASSERT_OK(Exchange(*process, request, &response));
    if (enabled) {
      ASSERT_FALSE(response.has_error());
      ASSERT_EQ(response.reuse_version(), 1);
      request.clear_open();
      request.set_batch_seq(1);
      request.set_release(true);
      ASSERT_OK(Exchange(*process, request, &response));
    }
    ASSERT_TRUE(response.has_error());
    ASSERT_TRUE(response.eof());
    ASSERT_EQ(response.values_size(), 0);
    ASSERT_FALSE(response.has_transaction_result());
    ASSERT_FALSE(response.released());
    ASSERT_NOK(process->Wait());
  }
}

TEST_F(MdbWorkerReuseTest, LateLeaseFrameCannotBecomeNextQuery) {
  Seed(1);
  auto plan = Plan({{-1, 9000, -2}}, 2, {0, 1}, 2, 1);
  ASSERT_OK(plan);
  auto process = Start(true);
  auto view = RdfYBModel::Instance().NewQueryReadContext();
  ASSERT_OK(view);
  auto request = Envelope(1);
  Open(&request, *plan, **view);
  ASSERT_FALSE(HasFatalFailure());
  FragmentResponsePB response;
  ASSERT_OK(Exchange(*process, request, &response));
  ASSERT_FALSE(response.has_error());
  request.clear_open();
  do {
    request.set_batch_seq(request.batch_seq() + 1);
    ASSERT_OK(Exchange(*process, request, &response));
    ASSERT_FALSE(response.has_error());
  } while (!response.eof());
  request.set_batch_seq(request.batch_seq() + 1);
  request.set_release(true);
  ASSERT_OK(Exchange(*process, request, &response));
  ASSERT_TRUE(response.released());
  auto replay = Envelope(1);
  Open(&replay, *plan, **view);
  ASSERT_FALSE(HasFatalFailure());
  ASSERT_OK(Exchange(*process, replay, &response));
  ASSERT_TRUE(response.has_error());
  ASSERT_TRUE(response.eof());
  ASSERT_EQ(response.values_size(), 0);
  ASSERT_FALSE(response.has_transaction_result());
  ASSERT_NOK(process->Wait());
}

TEST_F(MdbWorkerReuseTest, RefillsEveryAvailableSlotAfterTargetWait) {
  Seed(16);
  auto plan = Plan({{-1, 9000, -2}}, 2, {0, 1}, 16, 128);
  ASSERT_OK(plan);
  const auto relay = GetTestPath("pool-refill-relay");
  const auto warm_flags = GetTestPath("pool-refill-warm.flags");
  const auto active_flags = GetTestPath("pool-refill-active.flags");
  const auto opened = GetTestPath("pool-refill-open-");
  const auto expired = GetTestPath("pool-refill-expired");
  { std::ofstream flags(warm_flags); }
  { std::ofstream flags(active_flags); }
  {
    std::ofstream f(relay);
    f << "#!/usr/bin/python3\nimport os,struct,subprocess,sys,time\n"
      << "worker='" << workers_.binary << "'\nopened='" << opened
      << "'\nexpired='" << expired << "'\nactive='--flagfile=" << active_flags
      << "' in sys.argv\n" << R"PY(
def exact(stream,n):
 out=b''
 while len(out)<n:
  part=stream.read(n-len(out))
  if not part: raise EOFError('truncated frame')
  out+=part
 return out
def frame(stream):
 header=exact(stream,4);size=struct.unpack('!I',header)[0]
 if not 0<size<=1048576: raise ValueError('frame limit')
 return header+exact(stream,size)
def varint(data,pos):
 value=shift=0
 while True:
  byte=data[pos];pos+=1;value|=(byte&127)<<shift
  if byte<128:return value,pos
  shift+=7
def number(data,field):
 pos=0
 while pos<len(data):
  tag,pos=varint(data,pos);wire=tag&7
  if wire==0:
   value,pos=varint(data,pos)
   if tag>>3==field:return value
  elif wire==1:pos+=8
  elif wire==2:size,pos=varint(data,pos);pos+=size
  elif wire==5:pos+=4
  else:raise ValueError('wire type')
 return 0
p=subprocess.Popen([worker]+sys.argv[1:],stdin=subprocess.PIPE,stdout=subprocess.PIPE)
try:
 while True:
  request=frame(sys.stdin.buffer);body=request[4:]
  fragment=number(body,3);sequence=number(body,6);shutdown=number(body,33)
  if active and sequence==0 and not shutdown:
   with open(opened+str(fragment),'w') as marker:marker.write(str(os.getpid()))
  p.stdin.write(request);p.stdin.flush();response=frame(p.stdout)
  if active and fragment==0 and sequence==0 and not shutdown:
   # Let the different-key idle lease expire while this worker owns the only
   # active slot. No SSH or scheduler-speed assumption is needed.
   time.sleep(21)
  if active and fragment==1 and sequence==1:
   until=time.monotonic()+5
   while not os.path.exists(opened+'2') and time.monotonic()<until:time.sleep(.001)
   if not os.path.exists(opened+'2'):open(expired,'w').close()
  sys.stdout.buffer.write(response);sys.stdout.buffer.flush()
  if number(response[4:],33):break
 p.stdin.close();sys.exit(p.wait())
finally:
 if p.poll() is None:p.kill();p.wait()
)PY";
  }
  std::filesystem::permissions(relay, std::filesystem::perms::owner_all);
  auto options = workers_;
  options.binary = relay;
  options.targets = {"local"};
  options.reuse_processes = true;
  options.max_workers = 2;
  options.page_rows = 16;
  options.batch_rows = 128;
  FragmentWorkerPool pool(2);
  Rows expected;
  for (uint64_t s = 101; s < 117; ++s) expected.insert({s, 1});
  std::string pids;
  for (const auto& flags : {warm_flags, active_flags}) {
    options.flagfile = flags;
    auto view = RdfYBModel::Instance().NewQueryReadContext();
    ASSERT_OK(view);
    (*view)->deadline = CoarseMonoClock::Now() + MonoDelta::FromSeconds(60);
    Rows rows;
    ASSERT_OK(GatherFragments(*plan, options, **view, [&](const auto& row) {
      rows.insert(row);
      return Status::OK();
    }));
    ASSERT_EQ(rows, expected);
    ASSERT_EQ((*view)->stats.remote_fragments, 4);
    ASSERT_EQ((*view)->stats.remote_max_workers, 2);
    if (!pids.empty()) pids += ',';
    pids += (*view)->stats.remote_worker_pids;
  }
  rapidjson::Document stats;
  stats.Parse(pool.StatsJson().c_str());
  ASSERT_EQ(stats["process_starts"].GetUint64(), 4);
  ASSERT_EQ(stats["open_acks"].GetUint64(), 8);
  ASSERT_OK(pool.Close());
  std::istringstream processes(pids);
  for (std::string pid; std::getline(processes, pid, ',');) {
    ASSERT_OK(WaitFor([&] {
      errno = 0;
      return kill(static_cast<pid_t>(std::stoll(pid)), 0) == -1 && errno == ESRCH;
    }, MonoDelta::FromSeconds(5), "Refill test worker must exit"));
  }
  ASSERT_TRUE(std::filesystem::exists(opened + "2"));
  ASSERT_FALSE(std::filesystem::exists(expired))
      << "Fragment 2 must OPEN before fragment 1 is allowed to finish";
}

TEST_F(MdbWorkerReuseTest, SessionPoolBoundsStartsAndKeepsFreshViews) {
  Seed(8);
  auto plan = Plan({{-1, 9000, -2}, {-2, 9001, -3}}, 3, {0, 2}, 2, 1);
  ASSERT_OK(plan);
  plan->physical.set_graph_version(1);
  workers_.reuse_processes = true;
  FragmentWorkerPool pool(1);
  for (int round = 0; round < 2; ++round) {
    auto view = RdfYBModel::Instance().NewQueryReadContext();
    ASSERT_OK(view);
    Rows rows, expected;
    uint64_t reads = 0;
    ASSERT_OK(ReadView(*plan, **view, &rows, &reads, 1));
    for (uint64_t s = 101; s < 109; ++s) {
      expected.insert({s, 1001});
      expected.insert({s, 1002});
      if (round) {
        expected.insert({s, 1003});
      }
    }
    ASSERT_EQ(rows, expected);
    if (!round) {
      Write(1, 9001, 1003);
      ASSERT_OK(RdfYBModel::Instance().Flush());
    }
  }
  rapidjson::Document stats;
  stats.Parse(pool.StatsJson().c_str());
  ASSERT_EQ(stats["process_starts"].GetUint64(), 1);
  ASSERT_EQ(stats["open_acks"].GetUint64(), 8);
  ASSERT_EQ(stats["runtime_reuse_acks"].GetUint64(), 7);
  ASSERT_OK(pool.Close());
}

TEST_F(MdbWorkerReuseTest, FailedGatherDiscardsLeaseAndNextQueryRecovers) {
  Seed(8);
  auto plan = Plan({{-1, 9000, -2}, {-2, 9001, -3}}, 3, {0, 2}, 2, 1);
  ASSERT_OK(plan);
  plan->physical.set_graph_version(1);
  auto options = workers_;
  options.reuse_processes = true;
  options.max_workers = 1;
  FragmentWorkerPool pool(1);
  auto view = RdfYBModel::Instance().NewQueryReadContext();
  ASSERT_OK(view);
  const auto status = GatherFragments(*plan, options, **view, [](const auto&) {
    return STATUS(Aborted, "Injected consumer failure");
  });
  ASSERT_TRUE(status.IsAborted());
  auto fresh = RdfYBModel::Instance().NewQueryReadContext();
  ASSERT_OK(fresh);
  Rows rows, expected;
  ASSERT_OK(GatherFragments(*plan, options, **fresh, [&](const auto& row) {
    rows.insert(row);
    return Status::OK();
  }));
  for (uint64_t s = 101; s < 109; ++s) {
    expected.insert({s, 1001});
    expected.insert({s, 1002});
  }
  ASSERT_EQ(rows, expected);
  rapidjson::Document stats;
  stats.Parse(pool.StatsJson().c_str());
  ASSERT_GE(stats["process_starts"].GetUint64(), 2);
  ASSERT_OK(pool.Close());
}

TEST_F(MdbWorkerReuseTest, CancelAndDeadlineDiscardActiveLeaseAndRecover) {
  Seed(8);
  auto plan = Plan({{-1, 9000, -2}, {-2, 9001, -3}}, 3, {0, 2}, 2, 1);
  ASSERT_OK(plan);
  plan->physical.set_graph_version(1);
  auto options = workers_;
  options.reuse_processes = true;
  options.max_workers = 1;
  FragmentWorkerPool pool(1);
  for (bool expire : {false, true}) {
    auto view = RdfYBModel::Instance().NewQueryReadContext();
    ASSERT_OK(view);
    size_t delivered = 0;
    const auto status = GatherFragments(*plan, options, **view, [&](const auto&) {
      ++delivered;
      if (expire) {
        (*view)->deadline = CoarseMonoClock::Now();
      } else {
        (*view)->cancelled.store(true);
      }
      return Status::OK();
    });
    ASSERT_EQ(delivered, 1);
    ASSERT_TRUE(expire ? status.IsTimedOut() : status.IsAborted()) << status;
    auto fresh = RdfYBModel::Instance().NewQueryReadContext();
    ASSERT_OK(fresh);
    Rows actual, expected;
    ASSERT_OK(GatherFragments(*plan, options, **fresh, [&](const auto& row) {
      actual.insert(row);
      return Status::OK();
    }));
    for (uint64_t s = 101; s < 109; ++s) {
      expected.insert({s, 1001});
      expected.insert({s, 1002});
    }
    ASSERT_EQ(actual, expected);
  }
  rapidjson::Document stats;
  stats.Parse(pool.StatsJson().c_str());
  ASSERT_GE(stats["process_starts"].GetUint64(), 3);
  ASSERT_OK(pool.Close());
}

TEST_F(MdbWorkerReuseTest, LeaseBudgetRecyclesProcessBeforeLease257) {
  Seed(1);
  auto plan = Plan({{-1, 9000, -2}}, 2, {0, 1}, 8, 8);
  ASSERT_OK(plan);
  workers_.reuse_processes = true;
  FragmentWorkerPool pool(1);
  for (int round = 0; round < 65; ++round) {
    auto view = RdfYBModel::Instance().NewQueryReadContext();
    ASSERT_OK(view);
    Rows actual;
    uint64_t reads = 0;
    ASSERT_OK(ReadView(*plan, **view, &actual, &reads, 1));
    ASSERT_EQ(actual, Rows({{101, 1}}));
    rapidjson::Document stats;
    stats.Parse(pool.StatsJson().c_str());
    ASSERT_EQ(stats["process_starts"].GetUint64(), round < 64 ? 1 : 2);
    ASSERT_EQ(stats["open_acks"].GetUint64(), (round + 1) * 4);
  }
  ASSERT_OK(pool.Close());
}

TEST_F(MdbWorkerReuseTest, ChangedLaunchConfigurationEvictsIdleInfrastructure) {
  Seed(1);
  auto plan = Plan({{-1, 9000, -2}}, 2, {0, 1}, 8, 8);
  ASSERT_OK(plan);
  workers_.reuse_processes = true;
  FragmentWorkerPool pool(1);
  for (int round = 0; round < 2; ++round) {
    workers_.flagfile = GetTestPath("pool-config-" + std::to_string(round));
    { std::ofstream config(workers_.flagfile); }
    auto view = RdfYBModel::Instance().NewQueryReadContext();
    ASSERT_OK(view);
    Rows actual;
    uint64_t reads = 0;
    ASSERT_OK(ReadView(*plan, **view, &actual, &reads, 1));
    ASSERT_EQ(actual, Rows({{101, 1}}));
    rapidjson::Document stats;
    stats.Parse(pool.StatsJson().c_str());
    ASSERT_EQ(stats["process_starts"].GetUint64(), round + 1);
    ASSERT_EQ(stats["runtime_reuse_acks"].GetUint64(), (round + 1) * 3);
  }
  ASSERT_OK(pool.Close());
}

TEST_F(MdbWorkerReuseTest, LegacyRuntimeRejectsActualV3EnvelopeWithoutRows) {
  Seed(1);
  const auto* legacy = std::getenv("MDB_GRAPH_LEGACY_WORKER");
  ASSERT_NE(legacy, nullptr);
  ASSERT_TRUE(std::filesystem::exists(legacy));
  auto plan = Plan({{-1, 9000, -2}}, 2, {0, 1}, 2, 1);
  ASSERT_OK(plan);
  Subprocess process(legacy, {legacy, "--logtostderr=true"});
  process.PipeParentStdin();
  process.PipeParentStdout();
  ASSERT_OK(process.Start());
  ASSERT_OK(SetFragmentNonblocking(process.to_child_stdin_fd()));
  ASSERT_OK(SetFragmentNonblocking(process.from_child_stdout_fd()));
  auto view = RdfYBModel::Instance().NewQueryReadContext();
  ASSERT_OK(view);
  auto request = Envelope(1);
  Open(&request, *plan, **view);
  ASSERT_FALSE(HasFatalFailure());
  const auto deadline = CoarseMonoClock::Now() + MonoDelta::FromSeconds(20);
  ASSERT_OK(WriteFragmentFrame(process.to_child_stdin_fd(), request, deadline));
  FragmentResponsePB response;
  ASSERT_OK(ReadFragmentFrame(process.from_child_stdout_fd(), &response, deadline));
  ASSERT_TRUE(response.has_error());
  ASSERT_NE(response.error().find("envelope/version/sequence mismatch"), std::string::npos);
  ASSERT_TRUE(response.eof());
  ASSERT_EQ(response.rows(), 0);
  ASSERT_EQ(response.values_size(), 0);
  ASSERT_FALSE(response.has_transaction_result());
  ASSERT_NE(response.reuse_version(), 1);
  ASSERT_NOK(process.Wait());
}

TEST_F(MdbWorkerReuseTest, UnexpectedPhaseAcknowledgementDiscardsLease) {
  Seed(8);
  auto plan = Plan({{-1, 9000, -2}}, 2, {0, 1}, 2, 1);
  ASSERT_OK(plan);
  auto options = workers_;
  options.reuse_processes = true;
  options.max_workers = 1;
  FragmentWorkerPool pool(1);
  for (const std::string phase : {"open", "pull", "release"}) {
    const auto proxy = GetTestPath("phase-proxy-" + phase);
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
def release(b):
 i=0
 while i<len(b):
  tag,i=varint(b,i);wire=tag&7
  if wire==0:
   value,i=varint(b,i)
   if tag>>3==32: return value!=0
  elif wire==1: i+=8
  elif wire==2:
   size,i=varint(b,i);i+=size
  elif wire==5: i+=4
  else: raise ValueError('Unsupported test wire type')
 return False
try:
 i=0
 while True:
  req=frame(sys.stdin.buffer);send(p.stdin,req);ack=frame(p.stdout)
)PY";
      if (phase == "open") {
        f << "  if i==0: ack+=b'\\x80\\x02\\x01'\n";
      } else if (phase == "pull") {
        f << "  if i==1: ack+=b'\\x88\\x02\\x01'\n";
      } else {
        f << "  if release(req): ack+=b'\\x88\\x02\\x01'\n";
      }
      f << "  send(sys.stdout.buffer,ack);i+=1\n"
           "except (EOFError,BrokenPipeError): pass\n"
           "finally:\n p.stdin.close();p.wait(timeout=5)\n";
    }
    ASSERT_EQ(chmod(proxy.c_str(), 0700), 0);
    options.binary = proxy;
    auto view = RdfYBModel::Instance().NewQueryReadContext();
    ASSERT_OK(view);
    const auto failed = GatherFragments(*plan, options, **view, [](const auto&) {
      return Status::OK();
    });
    ASSERT_TRUE(failed.IsCorruption()) << phase << ": " << failed;
    options.binary = workers_.binary;
    auto fresh = RdfYBModel::Instance().NewQueryReadContext();
    ASSERT_OK(fresh);
    Rows actual, expected;
    ASSERT_OK(GatherFragments(*plan, options, **fresh, [&](const auto& row) {
      actual.insert(row);
      return Status::OK();
    }));
    for (uint64_t s = 101; s < 109; ++s) expected.insert({s, 1});
    ASSERT_EQ(actual, expected);
  }
  ASSERT_OK(pool.Close());
}

TEST_F(MdbWorkerReuseTest, IdleExpirationRecyclesBeforeWorkerSelfExit) {
  Seed(1);
  auto plan = Plan({{-1, 9000, -2}}, 2, {0, 1}, 8, 8);
  ASSERT_OK(plan);
  workers_.reuse_processes = true;
  FragmentWorkerPool pool(1);
  for (int round = 0; round < 2; ++round) {
    if (round) SleepFor(MonoDelta::FromSeconds(21));
    auto view = RdfYBModel::Instance().NewQueryReadContext();
    ASSERT_OK(view);
    Rows actual;
    uint64_t reads = 0;
    ASSERT_OK(ReadView(*plan, **view, &actual, &reads, 1));
    ASSERT_EQ(actual, Rows({{101, 1}}));
    rapidjson::Document stats;
    stats.Parse(pool.StatsJson().c_str());
    ASSERT_EQ(stats["process_starts"].GetUint64(), round + 1);
    ASSERT_EQ(stats["runtime_reuse_acks"].GetUint64(), (round + 1) * 3);
  }
  ASSERT_OK(pool.Close());
}

TEST_F(MdbWorkerReuseTest, DisconnectedIdleProcessFailsWithoutReplayAndThenRecovers) {
  Seed(1);
  auto plan = Plan({{-1, 9000, -2}}, 2, {0, 1}, 8, 8);
  ASSERT_OK(plan);
  workers_.reuse_processes = true;
  FragmentWorkerPool pool(1);
  for (int round = 0; round < 3; ++round) {
    auto view = RdfYBModel::Instance().NewQueryReadContext();
    ASSERT_OK(view);
    Rows actual;
    uint64_t reads = 0;
    const auto status = ReadView(*plan, **view, &actual, &reads, 1);
    if (round == 1) {
      ASSERT_NOK(status);
      ASSERT_TRUE(actual.empty());
    } else {
      ASSERT_OK(status);
      ASSERT_EQ(actual, Rows({{101, 1}}));
    }
    if (!round) {
      const auto pid = std::stoul((*view)->stats.remote_worker_pids);
      ASSERT_GT(pid, 0);
      ASSERT_EQ(kill(static_cast<pid_t>(pid), SIGKILL), 0);
      SleepFor(MonoDelta::FromMilliseconds(50));
    }
  }
  rapidjson::Document stats;
  stats.Parse(pool.StatsJson().c_str());
  ASSERT_EQ(stats["process_starts"].GetUint64(), 2);
  ASSERT_OK(pool.Close());
}

TEST_F(MdbWorkerReuseTest, ActiveOldSnapshotSurvivesExternalImportAndNewerLease) {
  for (bool graph : {false, true}) {
    const auto prefix = std::string("urn:poolview:") + (graph ? "graph:" : "old:");
    const auto cli = workers_.binary.substr(0, workers_.binary.find_last_of('/')) + "/mdb_cli";
    auto base = std::vector<std::string>{cli, "--mdb_masters=" + cluster_->GetMasterAddresses(),
        "--mdb_keyspace=mdb_p6_test", "--mdb_streaming_reads=true"};
    const auto initial = GetTestPath("pool-view-" + std::to_string(graph) + ".nt");
    {
      std::ofstream f(initial);
      for (int s = 1; s <= 16; ++s)
        f << "<" << prefix << "s" << s << "> <" << prefix << "p> <" << prefix << "k> .\n";
      f << "<" << prefix << "k> <" << prefix << "e> \"old\"@en .\n";
    }
    auto load = base;
    load.insert(load.end(), {"import", initial});
    ASSERT_OK(Subprocess::Call(load));
    PersistentIdMap dictionary;
    ASSERT_OK(dictionary.Init(client_.get(), "mdb_p6_test"));
    const auto predicate = dictionary.GetOrCreate(prefix + "p");
    const auto edge = dictionary.GetOrCreate(prefix + "e");
    const auto old = dictionary.GetOrCreate(RdfDictionaryKey(RdfIdentity(
        TripleTermType::LITERAL, "old", "@en")));
    ASSERT_OK(predicate);
    ASSERT_OK(edge);
    ASSERT_OK(old);
    auto plan = Plan({{-1, static_cast<int64_t>(*predicate), -2},
        {-2, static_cast<int64_t>(*edge), -3}}, 3, {0, 2}, 2, 1);
    ASSERT_OK(plan);
    if (graph) plan->physical.set_graph_version(1);
    auto options = workers_;
    options.reuse_processes = true;
    options.max_workers = 1;
    FragmentWorkerPool pool(1);
    auto old_view = RdfYBModel::Instance().NewQueryReadContext();
    ASSERT_OK(old_view);
    const auto additional = GetTestPath("pool-view-more-" + std::to_string(graph) + ".nt");
    {
      std::ofstream f(additional);
      f << "<" << prefix << "s17> <" << prefix << "p> <" << prefix << "k> .\n"
        << "<" << prefix << "k> <" << prefix << "e> \"new-term\"@fr .\n"
        << "<" << prefix << "k> <" << prefix
        << "e> \"7\"^^<http://www.w3.org/2001/XMLSchema#integer> .\n";
    }
    bool wrote = false;
    Rows active;
    ASSERT_OK(GatherFragments(*plan, options, **old_view, [&](const auto& row) -> Status {
      active.insert(row);
      if (!wrote) {
        if ((*old_view)->stats.remote_fragments >= 4)
          return STATUS(IllegalState, "Writer must commit before all fragments start");
        auto writer = base;
        writer.insert(writer.end(), {"import", additional});
        RETURN_NOT_OK(Subprocess::Call(writer));
        wrote = true;
      }
      return Status::OK();
    }));
    ASSERT_TRUE(wrote);
    Rows expected_old, expected_new;
    const auto language = dictionary.GetOrCreate(RdfDictionaryKey(RdfIdentity(
        TripleTermType::LITERAL, "new-term", "@fr")));
    const auto integer = dictionary.GetOrCreate(RdfDictionaryKey(RdfIdentity(
        TripleTermType::LITERAL, "7", "http://www.w3.org/2001/XMLSchema#integer")));
    ASSERT_OK(language);
    ASSERT_OK(integer);
    for (int s = 1; s <= 17; ++s) {
      const auto subject = dictionary.GetOrCreate(prefix + "s" + std::to_string(s));
      ASSERT_OK(subject);
      if (s <= 16) expected_old.insert({*subject, *old});
      for (auto object : {*old, *language, *integer}) expected_new.insert({*subject, object});
    }
    ASSERT_EQ(active, expected_old);
    auto fresh = RdfYBModel::Instance().NewQueryReadContext();
    ASSERT_OK(fresh);
    Rows current;
    ASSERT_OK(GatherFragments(*plan, options, **fresh, [&](const auto& row) {
      current.insert(row);
      return Status::OK();
    }));
    ASSERT_EQ(current, expected_new);
    Rows historical;
    ASSERT_OK(GatherFragments(*plan, options, **old_view, [&](const auto& row) -> Status {
      historical.insert(row);
      return Status::OK();
    }));
    ASSERT_EQ(historical, expected_old);
    rapidjson::Document stats;
    stats.Parse(pool.StatsJson().c_str());
    ASSERT_EQ(stats["process_starts"].GetUint64(), 1);
    ASSERT_EQ(stats["open_acks"].GetUint64(), 12);
    ASSERT_EQ(stats["runtime_reuse_acks"].GetUint64(), 11);
    ASSERT_OK(pool.Close());
  }
}

TEST_F(MdbWorkerReuseTest, BoundedReuseKeepsProcessAndDescriptorCountsStable) {
  Seed(8);
  auto plan = Plan({{-1, 9000, -2}, {-2, 9001, -3}}, 3, {0, 2}, 2, 1);
  ASSERT_OK(plan);
  plan->physical.set_graph_version(1);
  auto options = workers_;
  options.reuse_processes = true;
  options.max_workers = 2;
  FragmentWorkerPool pool(2);
  std::set<uint64_t> all_pids;
  std::map<uint64_t, std::pair<size_t, uint64_t>> baseline;
  auto sample = [&](uint64_t pid, int round) {
    const auto root = "/proc/" + std::to_string(pid);
    size_t descriptors = 0;
    for (const auto& item : std::filesystem::directory_iterator(root + "/fd")) {
      (void)item;
      ++descriptors;
    }
    uint64_t rss = 0;
    std::ifstream status(root + "/status");
    for (std::string line; std::getline(status, line);) {
      if (line.rfind("VmRSS:", 0) == 0) rss = std::stoull(line.substr(6));
    }
    EXPECT_GT(rss, 0);
    if (round == 16) baseline[pid] = {descriptors, rss};
    if (round > 16) {
      EXPECT_LE(descriptors, baseline.at(pid).first + 8);
      EXPECT_LE(rss, baseline.at(pid).second + 32 * 1024);
    }
    LOG(INFO) << "MDB_POOL_RESOURCE {\"round\":" << round << ",\"pid\":" << pid
              << ",\"fds\":" << descriptors << ",\"rss_kib\":" << rss << "}";
  };
  Rows expected;
  for (uint64_t subject = 101; subject < 109; ++subject)
    for (auto object : {1001, 1002}) expected.insert({subject, static_cast<uint64_t>(object)});
  for (int round = 1; round <= 64; ++round) {
    {
      auto view = RdfYBModel::Instance().NewQueryReadContext();
      ASSERT_OK(view);
      Rows actual;
      ASSERT_OK(GatherFragments(*plan, options, **view, [&](const auto& row) {
        actual.insert(row);
        return Status::OK();
      }));
      ASSERT_EQ(actual, expected);
      std::istringstream pids((*view)->stats.remote_worker_pids);
      for (std::string pid; std::getline(pids, pid, ',');) all_pids.insert(std::stoull(pid));
    }
    ASSERT_EQ(all_pids.size(), 2);
    if (round >= 16 && round % 4 == 0) {
      for (auto pid : all_pids) sample(pid, round);
      sample(getpid(), round);
      ASSERT_FALSE(HasFailure());
    }
  }
  rapidjson::Document stats;
  stats.Parse(pool.StatsJson().c_str());
  ASSERT_EQ(stats["process_starts"].GetUint64(), 2);
  ASSERT_EQ(stats["open_acks"].GetUint64(), 256);
  ASSERT_EQ(stats["runtime_reuse_acks"].GetUint64(), 254);
  ASSERT_OK(pool.Close());
  for (auto pid : all_pids) ASSERT_FALSE(std::filesystem::exists("/proc/" + std::to_string(pid)));
}

TEST_F(MdbWorkerReuseTest, RealStorageIdentitySwitchEvictsHandlesAndRejectsMismatchedIds) {
  Seed(1);
  auto original = Plan({{-1, 9000, -2}}, 2, {0, 1}, 8, 8);
  ASSERT_OK(original);
  const std::string other = "mdb_pool_other";
  ASSERT_OK(client_->CreateNamespaceIfNotExists(other, YQL_DATABASE_CQL));
  std::array<client::TableHandle, 6> tables;
  constexpr int permutation[6][3] = {{0,1,2},{1,2,0},{2,0,1},{1,0,2},{0,2,1},{2,1,0}};
  const int64_t triple[] = {701, 9000, 7001};
  for (int perm = 0; perm < 6; ++perm) {
    client::YBSchemaBuilder schema;
    ConfigureRdfTableSchema(&schema);
    ASSERT_OK(tables[perm].Create(client::YBTableName(
        YQL_DATABASE_CQL, other, RdfYBModel::kTableNames[perm]), 4, client_.get(), &schema));
    auto session = client_->NewSession(MonoDelta::FromSeconds(30));
    auto op = NewMdbInsertOp(tables[perm], session);
    QLAddInt64HashValue(op->mutable_request(), triple[permutation[perm][0]]);
    QLAddInt64RangeValue(op->mutable_request(), triple[permutation[perm][1]]);
    QLAddInt64RangeValue(op->mutable_request(), triple[permutation[perm][2]]);
    ASSERT_OK(session->TEST_ApplyAndFlush(op));
    ASSERT_EQ(op->response().status(), QLResponsePB::YQL_STATUS_OK);
  }
  auto alternate = *original;
  alternate.physical.set_keyspace(other);
  alternate.physical.clear_table_ids();
  for (const auto& table : tables) alternate.physical.add_table_ids(table.table()->id());
  TabletRouter router;
  ASSERT_OK(router.Init(client_.get(), other));
  alternate.tasks = router.GetTablets(0);
  auto mismatch = alternate;
  mismatch.physical.set_keyspace(original->physical.keyspace());
  auto options = workers_;
  options.reuse_processes = true;
  options.max_workers = 1;
  FragmentWorkerPool pool(1);
  for (int round = 0; round < 5; ++round) {
    const auto& plan = round == 1 ? alternate : round == 3 ? mismatch : *original;
    auto view = RdfYBModel::Instance().NewQueryReadContext();
    ASSERT_OK(view);
    Rows actual;
    const auto status = GatherFragments(plan, options, **view, [&](const auto& row) {
      actual.insert(row);
      return Status::OK();
    });
    if (round == 3) {
      ASSERT_NOK(status);
      ASSERT_NE(status.ToString().find("table generation changed"), std::string::npos);
      ASSERT_TRUE(actual.empty());
    } else {
      ASSERT_OK(status);
      ASSERT_EQ(actual, round == 1 ? Rows({{701,7001}}) : Rows({{101,1}}));
    }
  }
  rapidjson::Document stats;
  stats.Parse(pool.StatsJson().c_str());
  ASSERT_EQ(stats["process_starts"].GetUint64(), 5);
  ASSERT_EQ(stats["open_acks"].GetUint64(), 16);
  ASSERT_EQ(stats["runtime_reuse_acks"].GetUint64(), 12);
  ASSERT_OK(pool.Close());
}

// Opt-in because this exercises a separately managed, owner-scoped SSH cluster.
TEST(MdbPoolSshTest, ActiveCancelDeadlineAndRecovery) {
  const auto* path = std::getenv("MDB_POOL_CROSS_CONFIG");
  if (!path) GTEST_SKIP() << "Set MDB_POOL_CROSS_CONFIG to an isolated cross-host fixture";
  std::ifstream file(path);
  ASSERT_TRUE(file.good());
  const std::string text((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
  rapidjson::Document config;
  config.Parse(text.c_str());
  ASSERT_FALSE(config.HasParseError());
  ASSERT_EQ(YBCMdbInitCluster(config["masters"].GetString(), config["keyspace"].GetString()), 0);
  auto cleanup = ScopeExit([] { (void)YBCMdbShutdown(); });
  auto& model = RdfYBModel::Instance();
  IdMap terms;
  ASSERT_OK(model.RestoreTermMap(terms));
  const auto p = model.ResolveTerm(terms, config["predicate"].GetString());
  const auto e = model.ResolveTerm(terms, config["edge"].GetString());
  ASSERT_OK(p);
  ASSERT_OK(e);
  WorkerOptions options;
  options.targets = {"local", config["target"].GetString()};
  options.binary = config["binary"].GetString();
  options.ssh_config = config["ssh_config"].GetString();
  options.reuse_processes = true;
  options.max_workers = 2;
  options.batch_rows = 128;
  options.page_rows = 16;
  FragmentPlan plan;
  auto& open = plan.physical;
  open.set_master_addresses(config["masters"].GetString());
  open.set_keyspace(config["keyspace"].GetString());
  for (int i = 0; i < 6; ++i) open.add_table_ids(model.GetTable(i)->table()->id());
  open.set_variable_count(3);
  open.add_projection(0);
  open.add_projection(2);
  open.set_graph_version(1);
  open.set_batch_rows(options.batch_rows);
  open.set_page_rows(options.page_rows);
  open.set_memory_bytes(options.memory_bytes);
  for (const auto& pattern : std::vector<std::array<int64_t, 3>>{
      {-1, static_cast<int64_t>(*p), -2}, {-2, static_cast<int64_t>(*e), -3}}) {
    auto* triple = open.add_triples();
    for (auto value : pattern) {
      auto* term = triple->add_terms();
      if (value < 0) term->set_variable(-value - 1); else term->set_constant(value);
    }
  }
  TabletRouter router;
  ASSERT_OK(router.Init(model.Client(), config["keyspace"].GetString()));
  plan.tasks = router.GetTablets(0);
  ASSERT_EQ(plan.tasks.size(), 4);
  using Rows = std::multiset<std::vector<uint64_t>>;
  Rows expected;
  std::vector<uint64_t> objects;
  for (int i = 0; i < config["objects"].GetInt(); ++i) {
    std::ostringstream value;
    value << config["object_prefix"].GetString() << std::setw(4) << std::setfill('0') << i;
    auto id = model.ResolveTerm(terms, value.str());
    ASSERT_OK(id);
    objects.push_back(*id);
  }
  for (int i = 0; i < config["subjects"].GetInt(); ++i) {
    std::ostringstream value;
    value << config["subject_prefix"].GetString() << std::setw(4) << std::setfill('0') << i;
    auto id = model.ResolveTerm(terms, value.str());
    ASSERT_OK(id);
    for (auto object : objects) expected.insert({*id, object});
  }
  FragmentWorkerPool pool(2);
  auto success = [&]() {
    auto view = model.NewQueryReadContext();
    ASSERT_OK(view);
    Rows actual;
    ASSERT_OK(GatherFragments(plan, options, **view, [&](const auto& row) {
      actual.insert(row);
      return Status::OK();
    }));
    ASSERT_EQ(actual, expected);
    ASSERT_EQ((*view)->stats.remote_fragments, 4);
  };
  success();
  ASSERT_FALSE(::testing::Test::HasFatalFailure());
  for (bool expire : {false, true}) {
    auto view = model.NewQueryReadContext();
    ASSERT_OK(view);
    size_t rows = 0;
    bool injected = false;
    const auto status = GatherFragments(plan, options, **view, [&](const auto&) {
      ++rows;
      std::set<uint64_t> pids;
      std::istringstream values((*view)->stats.remote_worker_pids);
      for (std::string value; std::getline(values, value, ',');) pids.insert(std::stoull(value));
      if (pids.size() >= 2) {
        injected = true;
        if (expire) (*view)->deadline = CoarseMonoClock::Now(); else (*view)->cancelled = true;
      }
      return Status::OK();
    });
    ASSERT_TRUE(injected);
    ASSERT_GT(rows, 0);
    ASSERT_LT(rows, expected.size());
    ASSERT_TRUE(expire ? status.IsTimedOut() : status.IsAborted()) << status;
    LOG(INFO) << "MDB_POOL_SSH_STOP expire=" << expire << " partial_internal_rows=" << rows
              << " acknowledged_pids=" << (*view)->stats.remote_worker_pids;
    success();
    ASSERT_FALSE(::testing::Test::HasFatalFailure());
  }
  rapidjson::Document stats;
  stats.Parse(pool.StatsJson().c_str());
  ASSERT_GE(stats["process_starts"].GetUint64(), 6);
  ASSERT_OK(pool.Close());
}

}  // namespace
}  // namespace yb::mdb

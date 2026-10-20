// Copyright (c) YugabyteDB, Inc.
#include <arpa/inet.h>
#include <unistd.h>
#include <gtest/gtest.h>

#include "yb/mdb/query/distributed/fragment_plan.h"
#include "yb/mdb/query/distributed/fragment_transport.h"
#include "yb/util/env.h"
#include "yb/util/scope_exit.h"
#include "yb/util/subprocess.h"
#include "yb/util/test_util.h"
#include "yb/util/test_macros.h"

namespace yb::mdb {
namespace {

TEST(MdbFragmentTest, OwnershipRejectsGapsOverlapAndDuplicateTasks) {
  TabletInfo first, second;
  first.tablet_id = "a";
  first.partition_end = std::string("\x80\x00", 2);
  second.tablet_id = "b";
  second.partition_start = first.partition_end;
  ASSERT_OK(FragmentPlan::ValidateCoverage({first, second}));
  second.partition_start = std::string("\x90\x00", 2);
  ASSERT_NOK(FragmentPlan::ValidateCoverage({first, second}));
  second.partition_start = std::string("\x70\x00", 2);
  ASSERT_NOK(FragmentPlan::ValidateCoverage({first, second}));
  second.partition_start = first.partition_end;
  second.tablet_id = "a";
  ASSERT_NOK(FragmentPlan::ValidateCoverage({first, second}));
  ASSERT_NOK(FragmentPlan::ValidateCoverage({}));
}

TEST(MdbFragmentTest, FramesRejectOversizeTruncationAndExpiredDeadline) {
  int pipes[2];
  ASSERT_EQ(pipe(pipes), 0);
  auto cleanup = ScopeExit([&] { close(pipes[0]); close(pipes[1]); });
  ASSERT_OK(SetFragmentNonblocking(pipes[0]));
  ASSERT_OK(SetFragmentNonblocking(pipes[1]));
  uint32_t length = htonl(kFragmentMaxFrame + 1);
  ASSERT_EQ(write(pipes[1], &length, 4), 4);
  FragmentRequestPB request;
  auto deadline = CoarseMonoClock::Now() + MonoDelta::FromSeconds(2);
  ASSERT_NOK(ReadFragmentFrame(pipes[0], &request, deadline));
  length = htonl(10);
  ASSERT_EQ(write(pipes[1], &length, 4), 4);
  close(pipes[1]); pipes[1] = -1;
  ASSERT_NOK(ReadFragmentFrame(pipes[0], &request, deadline));
  ASSERT_NOK(ReadFragmentFrame(pipes[0], &request, CoarseMonoClock::Now()));
}

TEST(MdbFragmentTest, CancellationInterruptsPendingRead) {
  int pipes[2];
  ASSERT_EQ(pipe(pipes), 0);
  auto cleanup = ScopeExit([&] { close(pipes[0]); close(pipes[1]); });
  std::atomic<bool> cancelled{true};
  FragmentRequestPB request;
  ASSERT_OK(SetFragmentNonblocking(pipes[0]));
  auto status = ReadFragmentFrame(pipes[0], &request,
      CoarseMonoClock::Now() + MonoDelta::FromSeconds(10), &cancelled);
  ASSERT_TRUE(status.IsAborted()) << status;
}

TEST(MdbFragmentTest, WorkerRejectsUnknownProtocolBeforeOpeningStorage) {
  std::string executable;
  ASSERT_OK(Env::Default()->GetExecutablePath(&executable));
  const auto binary = executable.substr(0, executable.find_last_of('/')) + "/../bin/mdb_fragment_worker";
  Subprocess process(binary, {binary, "--logtostderr=true"});
  process.PipeParentStdin(); process.PipeParentStdout();
  ASSERT_OK(process.Start());
  ASSERT_OK(SetFragmentNonblocking(process.to_child_stdin_fd()));
  ASSERT_OK(SetFragmentNonblocking(process.from_child_stdout_fd()));
  FragmentRequestPB request;
  request.set_version(999); request.set_query_id("bad-version");
  request.set_attempt_id(1); request.set_fragment_id(0); request.set_stream_id(0);
  request.set_batch_seq(0);
  auto deadline = CoarseMonoClock::Now() + MonoDelta::FromSeconds(5);
  ASSERT_OK(WriteFragmentFrame(process.to_child_stdin_fd(), request, deadline));
  FragmentResponsePB response;
  ASSERT_OK(ReadFragmentFrame(process.from_child_stdout_fd(), &response, deadline));
  ASSERT_TRUE(response.eof());
  ASSERT_TRUE(response.has_error());
  ASSERT_EQ(response.values_size(), 0);
}

}  // namespace
}  // namespace yb::mdb

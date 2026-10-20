// Copyright (c) YugabyteDB, Inc.
#include <arpa/inet.h>
#include <unistd.h>
#include <gtest/gtest.h>
#include <limits>

#include "graph_models/rdf_model/conversions.h"
#include "query/executor/binding.h"
#include "query/executor/binding_iter/binding_expr/binding_expr_term.h"
#include "query/executor/binding_iter/binding_expr/sparql/binding_expr_addition.h"
#include "query/executor/binding_iter/binding_expr/sparql/binding_expr_subtraction.h"

#include "yb/mdb/query/distributed/fragment_plan.h"
#include "yb/mdb/query/distributed/fragment_transport.h"
#include "yb/util/env.h"
#include "yb/util/scope_exit.h"
#include "yb/util/subprocess.h"
#include "yb/util/test_util.h"
#include "yb/util/test_macros.h"

namespace yb::mdb {
namespace {

TEST(MdbFragmentTest, ProjectionIntegerBoundariesAndErrorPropagation) {
  const int64_t high = std::numeric_limits<int64_t>::max();
  const int64_t low = std::numeric_limits<int64_t>::min();
  Binding binding(0);
  auto evaluate = [&](bool subtract, int64_t lhs, int64_t rhs) -> ::ObjectId {
    auto left = std::make_unique<BindingExprTerm>(SPARQL::Conversions::pack_int(lhs));
    auto right = std::make_unique<BindingExprTerm>(SPARQL::Conversions::pack_int(rhs));
    if (subtract) {
      SPARQL::BindingExprSubtraction expr(std::move(left), std::move(right));
      return expr.eval(binding);
    }
    SPARQL::BindingExprAddition expr(std::move(left), std::move(right));
    return expr.eval(binding);
  };
  ASSERT_TRUE(evaluate(false, high, 1).is_null());
  ASSERT_TRUE(evaluate(false, low, -1).is_null());
  ASSERT_TRUE(evaluate(true, low, 1).is_null());
  ASSERT_TRUE(evaluate(true, high, -1).is_null());
  ASSERT_EQ(SPARQL::Conversions::to_integer(evaluate(false, high, low)), -1);
  ASSERT_EQ(SPARQL::Conversions::to_integer(evaluate(false, high, -1)), high - 1);
  ASSERT_EQ(SPARQL::Conversions::to_integer(evaluate(true, low, -1)), low + 1);
  ASSERT_EQ(SPARQL::Conversions::to_integer(evaluate(true, high, high)), 0);
  SPARQL::BindingExprAddition unbound(
      std::make_unique<BindingExprTerm>(::ObjectId::get_null()),
      std::make_unique<BindingExprTerm>(SPARQL::Conversions::pack_int(1)));
  ASSERT_TRUE(unbound.eval(binding).is_null());
  SPARQL::BindingExprSubtraction invalid(
      std::make_unique<BindingExprTerm>(SPARQL::Conversions::pack_iri("urn:projection:invalid")),
      std::make_unique<BindingExprTerm>(SPARQL::Conversions::pack_int(1)));
  ASSERT_TRUE(invalid.eval(binding).is_null());
}

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

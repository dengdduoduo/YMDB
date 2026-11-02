// Copyright (c) YugabyteDB, Inc.
#include <gtest/gtest.h>

#include "yb/mdb/model/rdf_yb_model.h"
#include "yb/mdb/query/distributed/graph_fragment.h"
#include "yb/mdb/storage/docdb_read_context.h"

namespace yb::mdb {
namespace {

// Exercise the pre-read control gate without a connected client or a valid
// physical plan. Checking ok() before status() is required in Debug Result<T>.
TEST(MdbRound3PolicyTest, CancellationPrecedesPlanValidationAndStorage) {
  auto context = DocDBReadContext::BorrowChildSession(nullptr);
  context->cancelled = true;
  auto result = ChooseGraphExecution(
      FragmentPlan{}, RdfYBModel::Instance(), *context, WorkerOptions{});
  ASSERT_FALSE(result.ok());
  ASSERT_TRUE(result.status().IsAborted());
  ASSERT_EQ(context->scan_rpc_calls, 0);
  ASSERT_EQ(context->stats.read_operations, 0);
}

TEST(MdbRound3PolicyTest, DeadlinePrecedesMissingSessionAndStorage) {
  auto context = DocDBReadContext::BorrowChildSession(nullptr);
  context->deadline = CoarseMonoClock::Now();
  auto result = ChooseGraphExecution(
      FragmentPlan{}, RdfYBModel::Instance(), *context, WorkerOptions{});
  ASSERT_FALSE(result.ok());
  ASSERT_TRUE(result.status().IsTimedOut());
  ASSERT_EQ(context->scan_rpc_calls, 0);
  ASSERT_EQ(context->stats.read_operations, 0);
}

TEST(MdbRound3PolicyTest, MissingReadViewCannotSelectOrReplayAnExecution) {
  auto context = DocDBReadContext::BorrowChildSession(nullptr);
  auto result = ChooseGraphExecution(
      FragmentPlan{}, RdfYBModel::Instance(), *context, WorkerOptions{});
  ASSERT_FALSE(result.ok());
  ASSERT_TRUE(result.status().IsTryAgain());
  ASSERT_EQ(context->scan_rpc_calls, 0);
  ASSERT_EQ(context->stats.read_operations, 0);
}

}  // namespace
}  // namespace yb::mdb

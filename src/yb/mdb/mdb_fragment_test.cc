// Copyright (c) YugabyteDB, Inc.
#include <arpa/inet.h>
#include <unistd.h>
#include <gtest/gtest.h>
#include <limits>

#include "graph_models/rdf_model/conversions.h"
#include "query/executor/binding.h"
#include "query/executor/binding_iter/aggregation/sparql/aggs.h"
#include "query/executor/binding_iter/binding_expr/binding_expr_var.h"
#include "query/executor/binding_iter/binding_expr/sparql/binding_expr_greater_or_equal.h"
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

TEST(MdbFragmentTest, NumericAggregateTypePromotionAndBoundaries) {
  using SPARQL::Conversions;
  using State = SPARQL::NumericAggregateState;
  State empty;
  ASSERT_EQ(Conversions::to_integer(empty.get(false)), 0);
  ASSERT_EQ(Conversions::to_integer(empty.get(true)), 0);
  State total;
  total.add(Conversions::pack_int(std::numeric_limits<int64_t>::max()));
  total.add(Conversions::pack_int(1));
  ASSERT_TRUE(total.get(false).is_null());
  total.add(Conversions::pack_int(-std::numeric_limits<int64_t>::max()));
  ASSERT_EQ(Conversions::to_integer(total.get(false)), 1);
  State low;
  low.add(Conversions::pack_int(std::numeric_limits<int64_t>::min()));
  ASSERT_EQ(Conversions::to_integer(low.get(false)), std::numeric_limits<int64_t>::min());
  ASSERT_EQ(Conversions::calculate_optype(low.get(true)), Conversions::OpType::DECIMAL);
  State mixed;
  mixed.add(Conversions::pack_int(1));
  mixed.add(Conversions::try_pack_string_datatype(std::string(SPARQL::RdfTermIdentity::kXsd) + "decimal", "2.5"));
  mixed.add(Conversions::pack_float(3.5));
  ASSERT_EQ(Conversions::calculate_optype(mixed.get(false)), Conversions::OpType::FLOAT);
  mixed.add(Conversions::pack_double(1));
  ASSERT_EQ(Conversions::calculate_optype(mixed.get(false)), Conversions::OpType::DOUBLE);
  ASSERT_EQ(Conversions::to_double(mixed.get(false)), 8);
  ASSERT_EQ(Conversions::to_double(mixed.get(true)), 2);
  State missing;
  missing.add(::ObjectId::get_null());
  ASSERT_TRUE(missing.get(false).is_null());
  ASSERT_EQ(Conversions::to_integer(missing.get(true)), 0);
  missing.add(Conversions::pack_int(1));
  ASSERT_TRUE(missing.get(true).is_null());
  State invalid;
  invalid.add(Conversions::pack_iri("urn:not:numeric"));
  ASSERT_TRUE(invalid.get(false).is_null());
  ASSERT_TRUE(invalid.get(true).is_null());
  State special;
  special.add(Conversions::pack_double(std::numeric_limits<double>::infinity()));
  ASSERT_EQ(Conversions::to_lexical_str(special.get(false)), "INF");
  special.add(Conversions::pack_double(-std::numeric_limits<double>::infinity()));
  ASSERT_TRUE(std::isnan(Conversions::to_double(special.get(false))));
  ASSERT_EQ(Conversions::to_lexical_str(special.get(true)), "NaN");
  Binding comparison_binding(0);
  SPARQL::BindingExprGreaterOrEqual unordered(
      std::make_unique<BindingExprTerm>(special.get(false)),
      std::make_unique<BindingExprTerm>(Conversions::pack_int(0)));
  ASSERT_EQ(unordered.eval(comparison_binding).id, ::ObjectId::BOOL_FALSE);
  ASSERT_LT(SPARQL::Comparisons::compare_for_order(special.get(false), Conversions::pack_int(0)), 0);
  ASSERT_LT(SPARQL::Comparisons::compare_for_order(Conversions::pack_int(9007199254740992LL),
      Conversions::try_pack_string_datatype(std::string(SPARQL::RdfTermIdentity::kXsd) + "decimal",
          "9007199254740993.0")), 0);
  State precise;
  precise.add(Conversions::try_pack_string_datatype(std::string(SPARQL::RdfTermIdentity::kXsd) + "decimal", "1.234567890123456789"));
  ASSERT_TRUE(precise.get(false).is_null());
  State cancellation;
  const std::string datatype = std::string(SPARQL::RdfTermIdentity::kXsd) + "decimal";
  const std::string large = "1" + std::string(100, '0') + ".0";
  const std::string small = "0." + std::string(99, '0') + "1";
  for (const auto& text : {large, small, "-" + large})
    cancellation.add(Conversions::try_pack_string_datatype(datatype, text));
  ASSERT_FALSE(cancellation.get(false).is_null());
  ASSERT_NEAR(Conversions::to_double(cancellation.get(false)), 1e-100, 1e-115);
}

TEST(MdbFragmentTest, AggregateDistinctResetAndValueExtrema) {
  using SPARQL::Conversions;
  Binding binding(1);
  SPARQL::AggSumDistinct sum(VarId(0), std::make_unique<BindingExprVar>(VarId(0)));
  SPARQL::AggAvgDistinct average(VarId(0), std::make_unique<BindingExprVar>(VarId(0)));
  SPARQL::AggMin minimum(VarId(0), std::make_unique<BindingExprVar>(VarId(0)));
  SPARQL::AggMax maximum(VarId(0), std::make_unique<BindingExprVar>(VarId(0)));
  for (Agg* agg : std::vector<Agg*>{&sum, &average, &minimum, &maximum}) {
    agg->set_binding(binding); agg->begin();
    for (int64_t value : {2, 2, 10, -4}) {
      binding.add(VarId(0), Conversions::pack_int(value)); agg->process();
    }
  }
  ASSERT_EQ(Conversions::to_integer(sum.get()), 8);
  ASSERT_NEAR(Conversions::to_double(average.get()), 8.0 / 3, 1e-15);
  ASSERT_EQ(Conversions::to_integer(minimum.get()), -4);
  ASSERT_EQ(Conversions::to_integer(maximum.get()), 10);
  sum.begin(); average.begin(); minimum.begin(); maximum.begin();
  ASSERT_EQ(Conversions::to_integer(sum.get()), 0);
  ASSERT_EQ(Conversions::to_integer(average.get()), 0);
  ASSERT_TRUE(minimum.get().is_null()); ASSERT_TRUE(maximum.get().is_null());
  for (const auto& text : {"urn:z", "urn:a"}) {
    binding.add(VarId(0), Conversions::pack_iri(text));
    minimum.process(); maximum.process();
  }
  ASSERT_EQ(Conversions::to_lexical_str(minimum.get()), "urn:a");
  ASSERT_EQ(Conversions::to_lexical_str(maximum.get()), "urn:z");
}

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

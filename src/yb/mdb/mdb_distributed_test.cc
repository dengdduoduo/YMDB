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

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "graph_models/object_id.h"
#include "query/executor/binding.h"

#include "yb/mdb/query/distributed/distributed_join.h"
#include "yb/mdb/query/distributed/distributed_plan.h"
#include "yb/mdb/query/distributed/gather_operator.h"
#include "yb/mdb/query/distributed/scatter_executor.h"
#include "yb/mdb/query/distributed/tablet_router.h"
#include "yb/util/test_macros.h"

namespace yb::mdb {

TEST(TabletRouterTest, DefaultState) {
  TabletRouter router;
  ASSERT_FALSE(router.IsInitialized());
  ASSERT_EQ(router.GetNumTablets(0), 0);
}

TEST(DistributedPlanTest, EmptyPlan) {
  DistributedPlan plan;
  ASSERT_FALSE(plan.is_distributed());
  ASSERT_EQ(plan.num_fragments(), 0);
  ASSERT_EQ(plan.gather_strategy(), GatherStrategy::UNION_ALL);
}

TEST(DistributedPlanTest, AddFragment) {
  DistributedPlan plan;
  TabletFragment fragment;
  fragment.tablet_id = "test_tablet_1";
  fragment.perm_index = 0;
  plan.AddFragment(std::move(fragment));
  ASSERT_EQ(plan.num_fragments(), 1);
  ASSERT_FALSE(plan.is_distributed());
  ASSERT_EQ(plan.fragments()[0].tablet_id, "test_tablet_1");
}

TEST(DistributedPlanTest, MultipleFragmentsMakesDistributed) {
  DistributedPlan plan;
  for (int i = 0; i < 3; i++) {
    TabletFragment f;
    f.tablet_id = "tablet_" + std::to_string(i);
    f.perm_index = 0;
    plan.AddFragment(std::move(f));
  }
  ASSERT_EQ(plan.num_fragments(), 3);
  ASSERT_TRUE(plan.is_distributed());
}

TEST(DistributedPlanTest, GatherStrategyDefaults) {
  DistributedPlan plan;
  ASSERT_EQ(plan.gather_strategy(), GatherStrategy::UNION_ALL);
  plan.set_gather_strategy(GatherStrategy::UNION_DISTINCT);
  ASSERT_EQ(plan.gather_strategy(), GatherStrategy::UNION_DISTINCT);
}

TEST(GatherOperatorTest, MergeUnionAllEmpty) {
  GatherOperator gather;
  std::vector<ScatterExecutor::FragmentResultSet> input;
  GatherResult result;
  ASSERT_OK(gather.Merge(input, GatherStrategy::UNION_ALL, &result));
  ASSERT_EQ(result.total_input_rows, 0);
  ASSERT_EQ(result.total_output_rows, 0);
}

TEST(GatherOperatorTest, MergeUnionAllSingleFragment) {
  GatherOperator gather;
  std::vector<ScatterExecutor::FragmentResultSet> input;

  ScatterExecutor::FragmentResultSet frag;
  frag.fragment_index = 0;
  frag.status = Status::OK();
  auto b1 = std::make_unique<Binding>(std::size_t{3});
  b1->add(VarId(0), ObjectId(1));
  b1->add(VarId(1), ObjectId(2));
  b1->add(VarId(2), ObjectId(3));
  frag.bindings.push_back(std::move(b1));

  input.push_back(std::move(frag));

  GatherResult result;
  ASSERT_OK(gather.Merge(input, GatherStrategy::UNION_ALL, &result));
  ASSERT_EQ(result.total_input_rows, 1);
  ASSERT_EQ(result.total_output_rows, 1);
}

TEST(GatherOperatorTest, MergeUnionAllMultipleFragments) {
  GatherOperator gather;
  std::vector<ScatterExecutor::FragmentResultSet> input;

  for (int f = 0; f < 3; f++) {
    ScatterExecutor::FragmentResultSet frag;
    frag.fragment_index = static_cast<uint32_t>(f);
    frag.status = Status::OK();
    for (int r = 0; r < 2; r++) {
      auto b = std::make_unique<Binding>(std::size_t{3});
      for (uint_fast32_t v = 0; v < 3; v++) {
        b->add(VarId(v), ObjectId(static_cast<uint64_t>(f * 10 + r + v)));
      }
      frag.bindings.push_back(std::move(b));
    }
    input.push_back(std::move(frag));
  }

  GatherResult result;
  ASSERT_OK(gather.Merge(input, GatherStrategy::UNION_ALL, &result));
  ASSERT_EQ(result.total_input_rows, 6);
  ASSERT_EQ(result.total_output_rows, 6);
}

TEST(GatherOperatorTest, MergeUnionDistinct) {
  GatherOperator gather;
  std::vector<ScatterExecutor::FragmentResultSet> input;

  for (int f = 0; f < 2; f++) {
    ScatterExecutor::FragmentResultSet frag;
    frag.fragment_index = static_cast<uint32_t>(f);
    frag.status = Status::OK();
    for (int r = 0; r < 2; r++) {
      auto b = std::make_unique<Binding>(std::size_t{3});
      for (uint_fast32_t v = 0; v < 3; v++) {
        b->add(VarId(v), ObjectId(static_cast<uint64_t>(v + 1)));
      }
      frag.bindings.push_back(std::move(b));
    }
    input.push_back(std::move(frag));
  }

  GatherResult result;
  ASSERT_OK(gather.Merge(input, GatherStrategy::UNION_DISTINCT, &result));
  ASSERT_EQ(result.total_input_rows, 4);
  ASSERT_EQ(result.total_output_rows, 1);
}

TEST(ScatterExecutorTest, EmptyPlan) {
  ScatterExecutor executor;
  DistributedPlan plan;
  std::vector<ScatterExecutor::FragmentResultSet> results;
  ASSERT_OK(executor.Execute(plan, &results));
  ASSERT_EQ(results.size(), 0);
}

TEST(ScatterExecutorTest, RejectTabletBeforeExecutingAnyFragment) {
  ScatterExecutor executor;
  DistributedPlan plan;
  // A local fragment first ensures validation happens before any execution.
  plan.AddFragment(TabletFragment{});
  TabletFragment fragment{};
  fragment.tablet_id = "tablet_a";
  plan.AddFragment(std::move(fragment));
  std::vector<ScatterExecutor::FragmentResultSet> results(1);
  const auto status = executor.Execute(plan, &results);
  ASSERT_TRUE(status.IsNotSupported()) << status;
  ASSERT_TRUE(results.empty());
}

TEST(ScatterExecutorTest, RejectUnprovenInputOwnership) {
  ScatterExecutor executor;
  DistributedPlan plan;
  plan.AddFragment(TabletFragment{});
  plan.AddFragment(TabletFragment{});
  std::vector<ScatterExecutor::FragmentResultSet> results;
  const auto status = executor.Execute(plan, &results);
  ASSERT_TRUE(status.IsNotSupported()) << status;
  ASSERT_TRUE(results.empty());
}

TEST(ScatterExecutorTest, RejectPartitionBoundsWithoutTabletId) {
  ScatterExecutor executor;
  for (bool start_bound : {false, true}) {
    DistributedPlan plan;
    TabletFragment fragment{};
    (start_bound ? fragment.partition_start : fragment.partition_end) = "boundary";
    plan.AddFragment(std::move(fragment));
    std::vector<ScatterExecutor::FragmentResultSet> results;
    const auto status = executor.Execute(plan, &results);
    ASSERT_TRUE(status.IsNotSupported()) << status;
    ASSERT_TRUE(results.empty());
  }
}

TEST(DistributedJoinTest, AnalyzeJoinSingleFragment) {
  TabletRouter router;
  DistributedJoinOptimizer optimizer(&router);

  std::vector<QueryFragment> left_frags(1);
  std::vector<QueryFragment> right_frags(1);

  auto dist = ASSERT_RESULT(optimizer.AnalyzeJoin("s", left_frags, right_frags));
  ASSERT_EQ(dist.type, JoinDistributionType::CO_LOCATED);
  ASSERT_EQ(dist.join_var, "s");
}

TEST(DistributedJoinTest, AnalyzeJoinBroadcast) {
  TabletRouter router;
  DistributedJoinOptimizer optimizer(&router);

  std::vector<QueryFragment> left_frags(1);
  std::vector<QueryFragment> right_frags(3);

  auto dist = ASSERT_RESULT(optimizer.AnalyzeJoin("s", left_frags, right_frags));
  ASSERT_EQ(dist.type, JoinDistributionType::BROADCAST);
}

TEST(DistributedJoinTest, AnalyzeJoinShuffle) {
  TabletRouter router;
  DistributedJoinOptimizer optimizer(&router);

  std::vector<QueryFragment> left_frags(2);
  std::vector<QueryFragment> right_frags(3);

  auto dist = ASSERT_RESULT(optimizer.AnalyzeJoin("s", left_frags, right_frags));
  ASSERT_EQ(dist.type, JoinDistributionType::SHUFFLE);
}

TEST(DistributedJoinTest, MapCoLocatedSingle) {
  TabletRouter router;
  DistributedJoinOptimizer optimizer(&router);

  TabletFragment left;
  left.tablet_id = "tablet_a";
  TabletFragment right;
  right.tablet_id = "tablet_a";

  std::vector<TabletFragment> left_frags;
  left_frags.push_back(std::move(left));
  std::vector<TabletFragment> right_frags;
  right_frags.push_back(std::move(right));

  auto pairs = ASSERT_RESULT(optimizer.MapCoLocatedFragments(left_frags, right_frags));
  ASSERT_EQ(pairs.size(), 1);
  ASSERT_EQ(pairs[0].first, 0);
  ASSERT_EQ(pairs[0].second, 0);
}

TEST(DistributedJoinTest, MapCoLocatedMultiple) {
  TabletRouter router;
  DistributedJoinOptimizer optimizer(&router);

  std::vector<TabletFragment> left_frags(3);
  left_frags[0].tablet_id = "tablet_0";
  left_frags[1].tablet_id = "tablet_1";
  left_frags[2].tablet_id = "tablet_2";

  std::vector<TabletFragment> right_frags(3);
  right_frags[0].tablet_id = "tablet_0";
  right_frags[1].tablet_id = "tablet_1";
  right_frags[2].tablet_id = "tablet_2";

  auto pairs = ASSERT_RESULT(optimizer.MapCoLocatedFragments(left_frags, right_frags));
  ASSERT_EQ(pairs.size(), 3);
}

}  // namespace yb::mdb

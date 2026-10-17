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
#include <set>
#include <vector>

#include "graph_models/object_id.h"
#include "query/executor/binding.h"
#include "query/executor/binding_iter/leapfrog_join.h"
#include "yb/mdb/storage/leapfrog_docdb_iter.h"

namespace yb::mdb {

class LeapfrogJoinIntegrationTest : public ::testing::Test {
 protected:
  bool interruption_ = false;
};

TEST_F(LeapfrogJoinIntegrationTest, TwoIterJoinOnSharedVar) {
  bool* intr = &interruption_;

  auto iter1 = std::make_unique<LeapfrogDocDBIter>(
      intr,
      std::vector<std::unique_ptr<ScanRange>>{},
      std::vector<VarId>{VarId(0)},
      std::vector<VarId>{VarId(1)});

  auto iter2 = std::make_unique<LeapfrogDocDBIter>(
      intr,
      std::vector<std::unique_ptr<ScanRange>>{},
      std::vector<VarId>{VarId(0)},
      std::vector<VarId>{VarId(2)});

  iter1->range_cache().InsertRow(1001, 100, 10);
  iter1->range_cache().InsertRow(1002, 100, 20);
  iter1->range_cache().InsertRow(1003, 100, 30);

  iter2->range_cache().InsertRow(1001, 200, 11);
  iter2->range_cache().InsertRow(1002, 200, 21);
  iter2->range_cache().InsertRow(1004, 200, 31);

  std::vector<std::unique_ptr<LeapfrogIter>> iters;
  iters.push_back(std::move(iter1));
  iters.push_back(std::move(iter2));

  LeapfrogJoin join(
      std::move(iters),
      std::vector<VarId>{VarId(0), VarId(1), VarId(2)},
      1);

  Binding binding(10);
  join.begin(binding);

  std::set<uint64_t> join_keys;
  int num_results = 0;
  while (join.next()) {
    num_results++;
    join_keys.insert(binding[VarId(0)].id);
  }

  EXPECT_EQ(num_results, 2);
  EXPECT_TRUE(join_keys.count(1001));
  EXPECT_TRUE(join_keys.count(1002));
}

TEST_F(LeapfrogJoinIntegrationTest, SingleIterScan) {
  bool* intr = &interruption_;

  auto iter1 = std::make_unique<LeapfrogDocDBIter>(
      intr,
      std::vector<std::unique_ptr<ScanRange>>{},
      std::vector<VarId>{VarId(0)},
      std::vector<VarId>{VarId(1)});

  iter1->range_cache().InsertRow(10, 1, 0);
  iter1->range_cache().InsertRow(20, 2, 0);
  iter1->range_cache().InsertRow(30, 3, 0);

  std::vector<std::unique_ptr<LeapfrogIter>> iters;
  iters.push_back(std::move(iter1));

  LeapfrogJoin join(
      std::move(iters),
      std::vector<VarId>{VarId(0), VarId(1)},
      1);

  Binding binding(10);
  join.begin(binding);

  int num_results = 0;
  while (join.next()) {
    num_results++;
    EXPECT_GT(binding[VarId(0)].id, 0u);
  }

  EXPECT_EQ(num_results, 3);
}

TEST_F(LeapfrogJoinIntegrationTest, TwoIterNoCommonIntersection) {
  bool* intr = &interruption_;

  auto iter1 = std::make_unique<LeapfrogDocDBIter>(
      intr,
      std::vector<std::unique_ptr<ScanRange>>{},
      std::vector<VarId>{VarId(0)},
      std::vector<VarId>{VarId(1)});

  auto iter2 = std::make_unique<LeapfrogDocDBIter>(
      intr,
      std::vector<std::unique_ptr<ScanRange>>{},
      std::vector<VarId>{VarId(0)},
      std::vector<VarId>{VarId(2)});

  iter1->range_cache().InsertRow(10, 1, 0);
  iter2->range_cache().InsertRow(99, 2, 0);

  std::vector<std::unique_ptr<LeapfrogIter>> iters;
  iters.push_back(std::move(iter1));
  iters.push_back(std::move(iter2));

  LeapfrogJoin join(
      std::move(iters),
      std::vector<VarId>{VarId(0), VarId(1), VarId(2)},
      1);

  Binding binding(10);
  join.begin(binding);

  int num_results = 0;
  while (join.next()) {
    num_results++;
  }

  EXPECT_EQ(num_results, 0);
}

TEST_F(LeapfrogJoinIntegrationTest, CountSeeks) {
  bool* intr = &interruption_;

  auto iter1 = std::make_unique<LeapfrogDocDBIter>(
      intr,
      std::vector<std::unique_ptr<ScanRange>>{},
      std::vector<VarId>{VarId(0)},
      std::vector<VarId>{VarId(1)});

  auto iter2 = std::make_unique<LeapfrogDocDBIter>(
      intr,
      std::vector<std::unique_ptr<ScanRange>>{},
      std::vector<VarId>{VarId(0)},
      std::vector<VarId>{VarId(2)});

  iter1->range_cache().InsertRow(10, 1, 0);
  iter1->range_cache().InsertRow(20, 2, 0);
  iter1->range_cache().InsertRow(30, 3, 0);

  iter2->range_cache().InsertRow(10, 4, 0);
  iter2->range_cache().InsertRow(20, 5, 0);
  iter2->range_cache().InsertRow(30, 6, 0);

  std::vector<std::unique_ptr<LeapfrogIter>> iters;
  iters.push_back(std::move(iter1));
  iters.push_back(std::move(iter2));

  LeapfrogJoin join(
      std::move(iters),
      std::vector<VarId>{VarId(0), VarId(1), VarId(2)},
      1);

  Binding binding(10);
  join.begin(binding);

  int num_results = 0;
  while (join.next()) {
    num_results++;
  }

  EXPECT_EQ(num_results, 3);
}

TEST_F(LeapfrogJoinIntegrationTest, MultiVarResultIsValid) {
  bool* intr = &interruption_;

  auto iter1 = std::make_unique<LeapfrogDocDBIter>(
      intr,
      std::vector<std::unique_ptr<ScanRange>>{},
      std::vector<VarId>{VarId(0)},
      std::vector<VarId>{VarId(1), VarId(2)});

  iter1->range_cache().InsertRow(42, 77, 88);

  std::vector<std::unique_ptr<LeapfrogIter>> iters;
  iters.push_back(std::move(iter1));

  LeapfrogJoin join(
      std::move(iters),
      std::vector<VarId>{VarId(0), VarId(1), VarId(2)},
      1);

  Binding binding(10);
  join.begin(binding);

  ASSERT_TRUE(join.next());
  EXPECT_EQ(binding[VarId(0)].id, 42u);
  EXPECT_EQ(binding[VarId(1)].id, 77u);
  EXPECT_EQ(binding[VarId(2)].id, 88u);

  EXPECT_FALSE(join.next());
}

}  // namespace yb::mdb

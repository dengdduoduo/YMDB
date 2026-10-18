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

#include <algorithm>
#include <array>
#include <functional>
#include <limits>
#include <memory>
#include <set>
#include <vector>

#include <gtest/gtest.h>

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
      intr, std::vector<std::unique_ptr<ScanRange>>{}, std::vector<VarId>{VarId(0)},
      std::vector<VarId>{VarId(1)});

  auto iter2 = std::make_unique<LeapfrogDocDBIter>(
      intr, std::vector<std::unique_ptr<ScanRange>>{}, std::vector<VarId>{VarId(0)},
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

  LeapfrogJoin join(std::move(iters), std::vector<VarId>{VarId(0), VarId(1), VarId(2)}, 1);

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
      intr, std::vector<std::unique_ptr<ScanRange>>{}, std::vector<VarId>{VarId(0)},
      std::vector<VarId>{VarId(1)});

  iter1->range_cache().InsertRow(10, 1, 0);
  iter1->range_cache().InsertRow(20, 2, 0);
  iter1->range_cache().InsertRow(30, 3, 0);

  std::vector<std::unique_ptr<LeapfrogIter>> iters;
  iters.push_back(std::move(iter1));

  LeapfrogJoin join(std::move(iters), std::vector<VarId>{VarId(0), VarId(1)}, 1);

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
      intr, std::vector<std::unique_ptr<ScanRange>>{}, std::vector<VarId>{VarId(0)},
      std::vector<VarId>{VarId(1)});

  auto iter2 = std::make_unique<LeapfrogDocDBIter>(
      intr, std::vector<std::unique_ptr<ScanRange>>{}, std::vector<VarId>{VarId(0)},
      std::vector<VarId>{VarId(2)});

  iter1->range_cache().InsertRow(10, 1, 0);
  iter2->range_cache().InsertRow(99, 2, 0);

  std::vector<std::unique_ptr<LeapfrogIter>> iters;
  iters.push_back(std::move(iter1));
  iters.push_back(std::move(iter2));

  LeapfrogJoin join(std::move(iters), std::vector<VarId>{VarId(0), VarId(1), VarId(2)}, 1);

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
      intr, std::vector<std::unique_ptr<ScanRange>>{}, std::vector<VarId>{VarId(0)},
      std::vector<VarId>{VarId(1)});

  auto iter2 = std::make_unique<LeapfrogDocDBIter>(
      intr, std::vector<std::unique_ptr<ScanRange>>{}, std::vector<VarId>{VarId(0)},
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

  LeapfrogJoin join(std::move(iters), std::vector<VarId>{VarId(0), VarId(1), VarId(2)}, 1);

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
      intr, std::vector<std::unique_ptr<ScanRange>>{}, std::vector<VarId>{VarId(0)},
      std::vector<VarId>{VarId(1), VarId(2)});

  iter1->range_cache().InsertRow(42, 77, 88);

  std::vector<std::unique_ptr<LeapfrogIter>> iters;
  iters.push_back(std::move(iter1));

  LeapfrogJoin join(std::move(iters), std::vector<VarId>{VarId(0), VarId(1), VarId(2)}, 1);

  Binding binding(10);
  join.begin(binding);

  ASSERT_TRUE(join.next());
  EXPECT_EQ(binding[VarId(0)].id, 42u);
  EXPECT_EQ(binding[VarId(1)].id, 77u);
  EXPECT_EQ(binding[VarId(2)].id, 88u);

  EXPECT_FALSE(join.next());
}

TEST_F(LeapfrogJoinIntegrationTest, TailFanoutPreservesBagOracleAndRepeatedBegin) {
  auto left = std::make_unique<LeapfrogDocDBIter>(
      &interruption_, std::vector<std::unique_ptr<ScanRange>>{}, std::vector<VarId>{VarId(0)},
      std::vector<VarId>{VarId(1), VarId(2)});
  auto right = std::make_unique<LeapfrogDocDBIter>(
      &interruption_, std::vector<std::unique_ptr<ScanRange>>{}, std::vector<VarId>{VarId(0)},
      std::vector<VarId>{VarId(3), VarId(4)});
  left->range_cache().InsertRow(7, 10, 100);
  left->range_cache().InsertRow(7, 10, 101);
  right->range_cache().InsertRow(7, 20, 200);
  right->range_cache().InsertRow(7, 21, 201);
  std::vector<std::unique_ptr<LeapfrogIter>> inputs;
  inputs.push_back(std::move(left));
  inputs.push_back(std::move(right));
  LeapfrogJoin join(std::move(inputs), {VarId(0), VarId(1), VarId(2), VarId(3), VarId(4)}, 1);
  Binding binding(5);
  const std::multiset<std::array<uint64_t, 3>> expected{
      {7, 10, 20}, {7, 10, 20}, {7, 10, 21}, {7, 10, 21}};
  const auto collect = [&] {
    std::multiset<std::array<uint64_t, 3>> actual;
    for (size_t n = 0; n < 20 && join.next(); ++n) {
      actual.insert({binding[VarId(0)].id, binding[VarId(1)].id, binding[VarId(3)].id});
    }
    return actual;
  };
  join.begin(binding);
  ASSERT_EQ(collect(), expected);
  join.reset();
  ASSERT_EQ(collect(), expected);
  join.begin(binding);
  ASSERT_EQ(collect(), expected);
}

TEST_F(LeapfrogJoinIntegrationTest, TriangleMatchesIndependentOracleAtIntegerBounds) {
  constexpr uint64_t max = std::numeric_limits<uint64_t>::max();
  using Pair = std::array<uint64_t, 2>;
  const std::vector<Pair> r{{0, 1}, {0, 2}, {1, 2}, {1, max}, {max, 0}};
  const std::vector<Pair> s{{1, 2}, {2, 1}, {2, max}, {max, 0}, {0, max}};
  const std::vector<Pair> t{{0, 2}, {0, 1}, {0, max}, {1, 1}, {1, max}, {1, 0}, {max, max}};
  std::multiset<std::array<uint64_t, 3>> expected;
  for (const auto& a : r)
    for (const auto& b : s)
      for (const auto& c : t) {
        if (a[1] == b[0] && a[0] == c[0] && b[1] == c[1]) {
          expected.insert({a[0], a[1], b[1]});
        }
      }
  const auto make_iter = [&](const std::vector<Pair>& rows, std::vector<VarId> keys) {
    auto iter = std::make_unique<LeapfrogDocDBIter>(
        &interruption_, std::vector<std::unique_ptr<ScanRange>>{}, keys, std::vector<VarId>{});
    for (const auto& row : rows) iter->range_cache().InsertRow(row[0], row[1], 0);
    return iter;
  };
  std::vector<std::unique_ptr<LeapfrogIter>> inputs;
  inputs.push_back(make_iter(r, {VarId(0), VarId(1)}));
  inputs.push_back(make_iter(s, {VarId(1), VarId(2)}));
  inputs.push_back(make_iter(t, {VarId(0), VarId(2)}));
  LeapfrogJoin join(std::move(inputs), {VarId(0), VarId(1), VarId(2)}, 3);
  Binding binding(3);
  for (int round = 0; round < 3; ++round) {
    if (round == 0)
      join.begin(binding);
    else
      join.reset();
    std::multiset<std::array<uint64_t, 3>> actual;
    for (size_t n = 0; n < 100 && join.next(); ++n) {
      actual.insert({binding[VarId(0)].id, binding[VarId(1)].id, binding[VarId(2)].id});
    }
    ASSERT_EQ(actual, expected);
  }
}

TEST_F(LeapfrogJoinIntegrationTest, NoIntersectionEnumeratesFullCartesianBag) {
  std::vector<std::unique_ptr<LeapfrogIter>> inputs;
  for (int side = 0; side < 2; ++side) {
    auto iter = std::make_unique<LeapfrogDocDBIter>(
        nullptr, std::vector<std::unique_ptr<ScanRange>>{}, std::vector<VarId>{},
        std::vector<VarId>{VarId(side * 3), VarId(side * 3 + 1), VarId(side * 3 + 2)});
    for (int i = 0; i < side + 2; ++i) iter->range_cache().InsertRow(i, i + 10, i + 20);
    inputs.push_back(std::move(iter));
  }
  LeapfrogJoin join(
      std::move(inputs), {VarId(0), VarId(1), VarId(2), VarId(3), VarId(4), VarId(5)}, 0);
  Binding binding(6);
  join.begin(binding);
  std::multiset<std::array<uint64_t, 2>> actual;
  for (size_t n = 0; n < 20 && join.next(); ++n) {
    actual.insert({binding[VarId(0)].id, binding[VarId(3)].id});
  }
  ASSERT_TRUE(join.status().ok()) << join.status();
  ASSERT_EQ(
      actual,
      (std::multiset<std::array<uint64_t, 2>>{{0, 0}, {0, 1}, {0, 2}, {1, 0}, {1, 1}, {1, 2}}));
}

TEST_F(LeapfrogJoinIntegrationTest, EmptyRebeginAndErrorsAreDistinct) {
  auto iter = std::make_unique<LeapfrogDocDBIter>(
      &interruption_, std::vector<std::unique_ptr<ScanRange>>{}, std::vector<VarId>{VarId(0)},
      std::vector<VarId>{VarId(1), VarId(2)});
  auto* cache = &iter->range_cache();
  cache->InsertRow(1, 2, 3);
  std::vector<std::unique_ptr<LeapfrogIter>> inputs;
  inputs.push_back(std::move(iter));
  LeapfrogJoin join(std::move(inputs), {VarId(0), VarId(1), VarId(2)}, 1);
  Binding binding(3);
  join.begin(binding);
  ASSERT_TRUE(join.next());
  cache->Clear();
  join.begin(binding);
  ASSERT_FALSE(join.next());
  ASSERT_TRUE(join.status().ok()) << join.status();

  inputs.clear();
  inputs.push_back(
      std::make_unique<LeapfrogDocDBIter>(
          &interruption_, std::vector<std::unique_ptr<ScanRange>>{}, std::vector<VarId>{VarId(0)},
          std::vector<VarId>{VarId(1)}));
  inputs.push_back(
      std::make_unique<LeapfrogDocDBIter>(
          &interruption_, std::vector<std::unique_ptr<ScanRange>>{}, std::vector<VarId>{VarId(0)},
          std::vector<VarId>{VarId(2)}, STATUS(IOError, "injected later child failure")));
  LeapfrogJoin failed(std::move(inputs), {VarId(0), VarId(1), VarId(2)}, 1);
  failed.begin(binding);
  ASSERT_FALSE(failed.next());
  ASSERT_TRUE(failed.status().IsIOError());
  failed.reset();
  ASSERT_FALSE(failed.next());
  ASSERT_TRUE(failed.status().IsIOError());
}

TEST_F(LeapfrogJoinIntegrationTest, InvalidLayoutAndCancellationAreVisible) {
  Binding binding(3);
  LeapfrogJoin empty({}, {}, 0);
  empty.begin(binding);
  ASSERT_FALSE(empty.next());
  ASSERT_TRUE(empty.status().IsInvalidArgument());
  auto invalid_child = std::make_unique<LeapfrogDocDBIter>(
      nullptr, std::vector<std::unique_ptr<ScanRange>>{}, std::vector<VarId>{VarId(9)},
      std::vector<VarId>{});
  std::vector<std::unique_ptr<LeapfrogIter>> invalid_inputs;
  invalid_inputs.push_back(std::move(invalid_child));
  LeapfrogJoin invalid(std::move(invalid_inputs), {VarId(9)}, 1);
  binding.add(VarId(0), ObjectId(uint64_t{42}));
  invalid.begin(binding);
  ASSERT_FALSE(invalid.next());
  ASSERT_TRUE(invalid.status().IsInvalidArgument());
  invalid.assign_nulls();
  ASSERT_EQ(binding[VarId(0)].id, 42);

  auto iter = std::make_unique<LeapfrogDocDBIter>(
      &interruption_, std::vector<std::unique_ptr<ScanRange>>{}, std::vector<VarId>{VarId(0)},
      std::vector<VarId>{VarId(1), VarId(2)});
  iter->range_cache().InsertRow(1, 2, 3);
  std::vector<std::unique_ptr<LeapfrogIter>> inputs;
  inputs.push_back(std::move(iter));
  LeapfrogJoin join(std::move(inputs), {VarId(0), VarId(1), VarId(2)}, 1);
  join.begin(binding);
  ASSERT_TRUE(join.next());
  interruption_ = true;
  ASSERT_FALSE(join.next());
  ASSERT_TRUE(join.status().IsAborted());
  interruption_ = false;
  join.reset();
  ASSERT_FALSE(join.next());
  join.begin(binding);
  ASSERT_FALSE(join.next());
  ASSERT_TRUE(join.status().IsAborted());
}

TEST_F(LeapfrogJoinIntegrationTest, LateEnumerationErrorReachesBindingStatus) {
  class FailingIter : public LeapfrogDocDBIter {
   public:
    using LeapfrogDocDBIter::LeapfrogDocDBIter;
    bool next_enumeration(Binding& binding) override {
      if (++calls_ == 2) {
        set_status(STATUS(IOError, "injected enumeration failure"));
        return false;
      }
      return LeapfrogDocDBIter::next_enumeration(binding);
    }

   private:
    size_t calls_ = 0;
  };
  auto iter = std::make_unique<FailingIter>(
      &interruption_, std::vector<std::unique_ptr<ScanRange>>{}, std::vector<VarId>{VarId(0)},
      std::vector<VarId>{VarId(1), VarId(2)});
  iter->range_cache().InsertRow(1, 2, 3);
  iter->range_cache().InsertRow(1, 4, 5);
  std::vector<std::unique_ptr<LeapfrogIter>> inputs;
  inputs.push_back(std::move(iter));
  LeapfrogJoin join(std::move(inputs), {VarId(0), VarId(1), VarId(2)}, 1);
  Binding binding(3);
  join.begin(binding);
  ASSERT_TRUE(join.next());
  ASSERT_FALSE(join.next());
  ASSERT_TRUE(join.status().IsIOError());
  join.reset();
  ASSERT_FALSE(join.next());
  ASSERT_TRUE(join.status().IsIOError());
}

TEST_F(LeapfrogJoinIntegrationTest, TriangleOrdersAndDensitiesMatchOracle) {
  using Pair = std::array<uint64_t, 2>;
  using Row = std::array<uint64_t, 3>;
  const uint64_t values[] = {0, 1, 2, std::numeric_limits<uint64_t>::max()};
  for (int seed = 0; seed < 8; ++seed) {
    std::array<std::vector<Pair>, 3> relations;
    for (size_t r = 0; r < relations.size(); ++r) {
      for (int a = 0; a < 4; ++a)
        for (int b = 0; b < 4; ++b) {
          if ((seed == 0 && r == 0) || (seed != 1 && (a * 7 + b * 3 + seed + r) % 4 == 0)) continue;
          relations[r].push_back({values[a], values[b]});
        }
    }
    std::multiset<Row> expected;
    for (const auto& a : relations[0])
      for (const auto& b : relations[1]) {
        for (const auto& c : relations[2]) {
          if (a[1] == b[0] && a[0] == c[0] && b[1] == c[1]) expected.insert({a[0], a[1], b[1]});
        }
      }
    std::array<int, 3> order{0, 1, 2};
    do {
      std::vector<std::unique_ptr<LeapfrogIter>> inputs;
      const int columns[3][2] = {{0, 1}, {1, 2}, {0, 2}};
      for (int r = 0; r < 3; ++r) {
        const bool reversed = std::find(order.begin(), order.end(), columns[r][0]) >
                              std::find(order.begin(), order.end(), columns[r][1]);
        auto iter = std::make_unique<LeapfrogDocDBIter>(
            &interruption_, std::vector<std::unique_ptr<ScanRange>>{},
            std::vector<VarId>{
                VarId(columns[r][reversed ? 1 : 0]), VarId(columns[r][reversed ? 0 : 1])},
            std::vector<VarId>{});
        for (const auto& row : relations[r]) {
          iter->range_cache().InsertRow(row[reversed ? 1 : 0], row[reversed ? 0 : 1], 0);
        }
        inputs.push_back(std::move(iter));
      }
      LeapfrogJoin join(std::move(inputs), {VarId(order[0]), VarId(order[1]), VarId(order[2])}, 3);
      Binding binding(3);
      for (int round = 0; round < 2; ++round) {
        if (round == 0)
          join.begin(binding);
        else
          join.reset();
        std::multiset<Row> actual;
        for (size_t n = 0; n <= expected.size() && join.next(); ++n) {
          actual.insert({binding[VarId(0)].id, binding[VarId(1)].id, binding[VarId(2)].id});
        }
        ASSERT_TRUE(join.status().ok()) << join.status();
        ASSERT_EQ(actual, expected) << seed << '/' << order[0] << order[1] << order[2];
      }
    } while (std::next_permutation(order.begin(), order.end()));
  }
}

}  // namespace yb::mdb

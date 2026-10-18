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

#include <array>
#include <functional>
#include <limits>
#include <memory>
#include <set>

#include <gtest/gtest.h>

#include "graph_models/object_id.h"
#include "query/executor/binding.h"
#include "query/executor/binding_iter/scan_ranges/term.h"
#include "yb/mdb/storage/leapfrog_docdb_iter.h"

namespace yb::mdb {

class LeapfrogDocDBIterTest : public ::testing::Test {
 protected:
  bool interruption_ = false;
};

TEST_F(LeapfrogDocDBIterTest, SingleRangeScan3Rows) {
  bool* intr = &interruption_;
  auto iter = std::make_unique<LeapfrogDocDBIter>(
      intr, std::vector<std::unique_ptr<ScanRange>>{}, std::vector<VarId>{VarId(0)},
      std::vector<VarId>{VarId(1)});

  iter->range_cache().InsertRow(100, 10, 1);
  iter->range_cache().InsertRow(200, 20, 2);
  iter->range_cache().InsertRow(300, 30, 3);

  Binding binding(2);
  ASSERT_TRUE(iter->open_terms(binding));

  iter->down();
  EXPECT_EQ(iter->get_key(), 100);

  EXPECT_TRUE(iter->next());
  EXPECT_EQ(iter->get_key(), 200);

  EXPECT_TRUE(iter->next());
  EXPECT_EQ(iter->get_key(), 300);

  EXPECT_FALSE(iter->next());
}

TEST_F(LeapfrogDocDBIterTest, SeekExactHit) {
  bool* intr = &interruption_;
  auto iter = std::make_unique<LeapfrogDocDBIter>(
      intr, std::vector<std::unique_ptr<ScanRange>>{}, std::vector<VarId>{VarId(0)},
      std::vector<VarId>{VarId(1)});

  iter->range_cache().InsertRow(100, 10, 1);
  iter->range_cache().InsertRow(200, 20, 2);
  iter->range_cache().InsertRow(300, 30, 3);

  Binding binding(2);
  ASSERT_TRUE(iter->open_terms(binding));
  iter->down();

  ASSERT_TRUE(iter->seek(200));
  EXPECT_EQ(iter->get_key(), 200);
}

TEST_F(LeapfrogDocDBIterTest, SeekBeyondRange) {
  bool* intr = &interruption_;
  auto iter = std::make_unique<LeapfrogDocDBIter>(
      intr, std::vector<std::unique_ptr<ScanRange>>{}, std::vector<VarId>{VarId(0)},
      std::vector<VarId>{VarId(1)});

  iter->range_cache().InsertRow(100, 10, 1);
  iter->range_cache().InsertRow(200, 20, 2);

  Binding binding(2);
  ASSERT_TRUE(iter->open_terms(binding));
  iter->down();

  ASSERT_FALSE(iter->seek(500));
}

TEST_F(LeapfrogDocDBIterTest, GetKeyConsistent) {
  bool* intr = &interruption_;
  auto iter = std::make_unique<LeapfrogDocDBIter>(
      intr, std::vector<std::unique_ptr<ScanRange>>{}, std::vector<VarId>{VarId(0)},
      std::vector<VarId>{VarId(1)});

  iter->range_cache().InsertRow(42, 10, 1);

  Binding binding(2);
  ASSERT_TRUE(iter->open_terms(binding));
  iter->down();
  EXPECT_EQ(iter->get_key(), 42);
}

TEST_F(LeapfrogDocDBIterTest, EmptyCacheOpenTerms) {
  bool* intr = &interruption_;
  auto iter = std::make_unique<LeapfrogDocDBIter>(
      intr, std::vector<std::unique_ptr<ScanRange>>{}, std::vector<VarId>{VarId(0)},
      std::vector<VarId>{VarId(1)});

  Binding binding(2);
  ASSERT_FALSE(iter->open_terms(binding));
}

TEST_F(LeapfrogDocDBIterTest, SeekMiddleValue) {
  bool* intr = &interruption_;
  auto iter = std::make_unique<LeapfrogDocDBIter>(
      intr, std::vector<std::unique_ptr<ScanRange>>{}, std::vector<VarId>{VarId(0)},
      std::vector<VarId>{VarId(1)});

  iter->range_cache().InsertRow(100, 10, 1);
  iter->range_cache().InsertRow(200, 20, 2);
  iter->range_cache().InsertRow(300, 30, 3);

  Binding binding(2);
  ASSERT_TRUE(iter->open_terms(binding));
  iter->down();

  ASSERT_TRUE(iter->seek(150));
  EXPECT_GE(iter->get_key(), 150);
}

TEST_F(LeapfrogDocDBIterTest, MultipleNextExhausts) {
  bool* intr = &interruption_;
  auto iter = std::make_unique<LeapfrogDocDBIter>(
      intr, std::vector<std::unique_ptr<ScanRange>>{}, std::vector<VarId>{VarId(0)},
      std::vector<VarId>{VarId(1)});

  iter->range_cache().InsertRow(10, 1, 100);
  iter->range_cache().InsertRow(20, 1, 200);

  Binding binding(2);
  ASSERT_TRUE(iter->open_terms(binding));
  iter->down();

  EXPECT_EQ(iter->get_key(), 10);
  ASSERT_TRUE(iter->next());
  EXPECT_EQ(iter->get_key(), 20);
  ASSERT_FALSE(iter->next());
  ASSERT_FALSE(iter->next());
}

TEST_F(LeapfrogDocDBIterTest, GetIterName) {
  bool* intr = &interruption_;
  auto iter = std::make_unique<LeapfrogDocDBIter>(
      intr, std::vector<std::unique_ptr<ScanRange>>{}, std::vector<VarId>{}, std::vector<VarId>{});

  EXPECT_EQ(iter->get_iter_name(), "LeapfrogDocDBIter");
}

TEST_F(LeapfrogDocDBIterTest, EnumeratesAllSuffixRowsAndResets) {
  LeapfrogDocDBIter iter(&interruption_, {}, {VarId(0)}, {VarId(1), VarId(2)});
  for (const auto& row : std::vector<std::array<uint64_t, 3>>{
           {7, 10, 100}, {7, 10, 101}, {7, 11, 102}, {8, 20, 200}}) {
    iter.range_cache().InsertRow(row[0], row[1], row[2]);
  }
  Binding binding(3);
  ASSERT_TRUE(iter.open_terms(binding));
  iter.down();
  ASSERT_EQ(iter.get_key(), 7);
  const auto collect = [&] {
    std::multiset<std::array<uint64_t, 2>> rows;
    for (size_t i = 0; i < 10 && iter.next_enumeration(binding); ++i) {
      rows.insert({binding[VarId(1)].id, binding[VarId(2)].id});
    }
    return rows;
  };
  const std::multiset<std::array<uint64_t, 2>> expected{{10, 100}, {10, 101}, {11, 102}};
  iter.begin_enumeration();
  ASSERT_EQ(collect(), expected);
  iter.reset_enumeration();
  ASSERT_EQ(collect(), expected);
  ASSERT_TRUE(iter.next());
  ASSERT_EQ(iter.get_key(), 8);
  iter.begin_enumeration();
  ASSERT_EQ(collect(), (std::multiset<std::array<uint64_t, 2>>{{20, 200}}));
}

TEST_F(LeapfrogDocDBIterTest, MaximumKeyDoesNotWrapAndSeekIsMonotone) {
  constexpr uint64_t max = std::numeric_limits<uint64_t>::max();
  LeapfrogDocDBIter iter(&interruption_, {}, {VarId(0)}, {VarId(1), VarId(2)});
  iter.range_cache().InsertRow(0, 1, 2);
  iter.range_cache().InsertRow(max, 3, 4);
  Binding binding(3);
  ASSERT_TRUE(iter.open_terms(binding));
  iter.down();
  ASSERT_TRUE(iter.seek(max));
  ASSERT_EQ(iter.get_key(), max);
  ASSERT_FALSE(iter.next());
  ASSERT_FALSE(iter.next());
  ASSERT_FALSE(iter.seek(0));
  iter.up();
  iter.down();
  ASSERT_EQ(iter.get_key(), 0);
  ASSERT_TRUE(iter.seek(max));
  ASSERT_TRUE(iter.seek(0));
  ASSERT_EQ(iter.get_key(), max);
}

TEST_F(LeapfrogDocDBIterTest, FailedOpenDoesNotEnumerateStaleRows) {
  LeapfrogDocDBIter iter(&interruption_, {}, {}, {VarId(0), VarId(1), VarId(2)});
  iter.range_cache().InsertRow(1, 2, 3);
  Binding binding(3);
  ASSERT_TRUE(iter.open_terms(binding));
  iter.begin_enumeration();
  ASSERT_TRUE(iter.next_enumeration(binding));
  iter.range_cache().Clear();
  ASSERT_FALSE(iter.open_terms(binding));
  iter.begin_enumeration();
  ASSERT_FALSE(iter.next_enumeration(binding));
  iter.reset_enumeration();
  ASSERT_FALSE(iter.next_enumeration(binding));
}

TEST_F(LeapfrogDocDBIterTest, AllPrefixSplitsMatchIndependentStoredRows) {
  constexpr uint64_t max = std::numeric_limits<uint64_t>::max();
  using Row = std::array<uint64_t, 3>;
  const std::vector<Row> stored{{0, 1, 2}, {0, 1, 3},   {0, 2, max},
                                {1, 0, 4}, {1, max, 5}, {max, 0, max}};
  for (size_t initial = 0; initial <= 3; ++initial) {
    for (size_t intersections = 0; intersections <= 3 - initial; ++intersections) {
      SCOPED_TRACE(::testing::Message() << initial << '/' << intersections);
      std::vector<std::unique_ptr<ScanRange>> terms;
      for (size_t i = 0; i < initial; ++i) {
        terms.push_back(std::make_unique<Term>(ObjectId(stored.front()[i])));
      }
      std::vector<VarId> keys, outputs;
      for (size_t i = 0; i < 3 - initial; ++i) {
        (i < intersections ? keys : outputs).emplace_back(i);
      }
      LeapfrogDocDBIter iter(&interruption_, std::move(terms), keys, outputs);
      for (auto row = stored.rbegin(); row != stored.rend(); ++row) {
        iter.range_cache().InsertRow((*row)[0], (*row)[1], (*row)[2]);
      }
      Binding binding(3);
      std::multiset<Row> expected, actual;
      for (const auto& row : stored) {
        bool matches = true;
        for (size_t i = 0; i < initial; ++i) matches &= row[i] == stored.front()[i];
        if (matches) expected.insert(row);
      }
      ASSERT_TRUE(iter.open_terms(binding));
      std::function<void(size_t)> visit = [&](size_t depth) {
        if (depth == intersections) {
          iter.begin_enumeration();
          for (size_t n = 0; n <= stored.size() && iter.next_enumeration(binding); ++n) {
            Row row = stored.front();
            for (size_t i = initial; i < 3; ++i) row[i] = binding[VarId(i - initial)].id;
            actual.insert(row);
          }
          return;
        }
        iter.down();
        size_t n = 0;
        do {
          binding.add(keys[depth], ObjectId(iter.get_key()));
          visit(depth + 1);
          ASSERT_LE(++n, stored.size());
        } while (iter.next());
        iter.up();
      };
      ASSERT_NO_FATAL_FAILURE(visit(0));
      ASSERT_EQ(actual, expected);
    }
  }
}

TEST_F(LeapfrogDocDBIterTest, BoundInitialTermReopensWithChangedInput) {
  std::vector<std::unique_ptr<ScanRange>> ranges;
  ranges.push_back(ScanRange::get(Id(VarId(0)), true));
  LeapfrogDocDBIter iter(&interruption_, std::move(ranges), {VarId(1)}, {VarId(2)});
  iter.range_cache().InsertRow(5, 10, 100);
  iter.range_cache().InsertRow(7, 20, 200);
  Binding binding(3);
  for (uint64_t key : {5, 7}) {
    binding.add(VarId(0), ObjectId(key));
    ASSERT_TRUE(iter.open_terms(binding));
    iter.down();
    iter.begin_enumeration();
    ASSERT_TRUE(iter.next_enumeration(binding));
    ASSERT_EQ(binding[VarId(2)].id, key == 5 ? 100 : 200);
    ASSERT_FALSE(iter.next_enumeration(binding));
  }
  binding.add(VarId(0), ObjectId(uint64_t{999}));
  ASSERT_FALSE(iter.open_terms(binding));
  iter.begin_enumeration();
  ASSERT_FALSE(iter.next_enumeration(binding));
  ASSERT_TRUE(iter.status().ok()) << iter.status();
}

TEST_F(LeapfrogDocDBIterTest, InitialLoadErrorAndCancellationRemainSticky) {
  Binding binding(3);
  LeapfrogDocDBIter failed(
      &interruption_, {}, {}, {VarId(0), VarId(1), VarId(2)},
      STATUS(IOError, "injected Leapfrog load failure"));
  failed.range_cache().InsertRow(1, 2, 3);
  ASSERT_FALSE(failed.open_terms(binding));
  ASSERT_TRUE(failed.status().IsIOError());
  failed.begin_enumeration();
  ASSERT_FALSE(failed.next_enumeration(binding));
  ASSERT_FALSE(failed.open_terms(binding));
  ASSERT_TRUE(failed.status().IsIOError());

  LeapfrogDocDBIter cancelled(&interruption_, {}, {}, {VarId(0), VarId(1), VarId(2)});
  cancelled.range_cache().InsertRow(1, 2, 3);
  cancelled.range_cache().InsertRow(2, 3, 4);
  ASSERT_TRUE(cancelled.open_terms(binding));
  cancelled.begin_enumeration();
  ASSERT_TRUE(cancelled.next_enumeration(binding));
  interruption_ = true;
  ASSERT_FALSE(cancelled.next_enumeration(binding));
  ASSERT_TRUE(cancelled.status().IsAborted());
  interruption_ = false;
  ASSERT_FALSE(cancelled.open_terms(binding));
  ASSERT_TRUE(cancelled.status().IsAborted());
}

TEST_F(LeapfrogDocDBIterTest, UnsupportedLayoutsFailBeforeAccessingColumns) {
  Binding binding(3);
  LeapfrogDocDBIter too_wide(&interruption_, {}, {VarId(0), VarId(1), VarId(2)}, {VarId(3)});
  ASSERT_FALSE(too_wide.open_terms(binding));
  ASSERT_TRUE(too_wide.status().IsInvalidArgument());
  LeapfrogDocDBIter repeated(&interruption_, {}, {VarId(0)}, {VarId(0)});
  ASSERT_FALSE(repeated.open_terms(binding));
  ASSERT_TRUE(repeated.status().IsInvalidArgument());
  LeapfrogDocDBIter out_of_bounds(&interruption_, {}, {}, {VarId(9)});
  ASSERT_FALSE(out_of_bounds.open_terms(binding));
  ASSERT_TRUE(out_of_bounds.status().IsInvalidArgument());
  std::vector<std::unique_ptr<ScanRange>> ranges;
  ranges.push_back(ScanRange::get(Id(VarId(0)), false));
  LeapfrogDocDBIter not_fixed(&interruption_, std::move(ranges), {}, {VarId(1)});
  ASSERT_FALSE(not_fixed.open_terms(binding));
  ASSERT_TRUE(not_fixed.status().IsInvalidArgument());
}

TEST_F(LeapfrogDocDBIterTest, EstimatesMatchIndependentPrefixSuffixBags) {
  using Row = std::array<uint64_t, 3>;
  constexpr uint64_t max = std::numeric_limits<uint64_t>::max();
  const std::vector<Row> stored{{0, 0, 0},     {0, 0, 0},   {0, 0, 1},       {0, 1, 0},
                                {0, max, max}, {1, 0, 2},   {1, 0, 2},       {1, 1, 3},
                                {1, max, 4},   {max, 0, 0}, {max, max, max}, {max, max, max}};
  for (size_t initial = 0; initial < 3; ++initial) {
    std::set<Row> prefixes;
    for (const auto& row : stored) {
      Row prefix{};
      for (size_t i = 0; i < initial; ++i) prefix[i] = row[i];
      prefixes.insert(prefix);
    }
    for (size_t intersections = 1; intersections <= 3 - initial; ++intersections) {
      for (const auto& prefix : prefixes) {
        SCOPED_TRACE(
            ::testing::Message() << initial << '/' << intersections << '/' << prefix[0] << '/'
                                 << prefix[1]);
        std::vector<std::unique_ptr<ScanRange>> terms;
        for (size_t i = 0; i < initial; ++i)
          terms.push_back(std::make_unique<Term>(ObjectId(prefix[i])));
        std::vector<VarId> keys, outputs;
        for (size_t i = 0; i < 3 - initial; ++i)
          (i < intersections ? keys : outputs).emplace_back(i);
        LeapfrogDocDBIter iter(&interruption_, std::move(terms), keys, outputs);
        for (auto row = stored.rbegin(); row != stored.rend(); ++row)
          iter.range_cache().InsertRow((*row)[0], (*row)[1], (*row)[2]);
        Binding binding(3);
        ASSERT_TRUE(iter.open_terms(binding));
        Row path = prefix;
        std::multiset<Row> actual, expected;
        for (const auto& row : stored) {
          bool matches = true;
          for (size_t i = 0; i < initial; ++i) matches &= row[i] == prefix[i];
          if (matches) expected.insert(row);
        }
        const auto check = [&](size_t depth) {
          const size_t column = initial + depth;
          size_t all = 0, remaining = 0;
          for (const auto& row : stored) {
            bool matches = true;
            for (size_t i = 0; i < column; ++i) matches &= row[i] == path[i];
            if (matches) {
              ++all;
              if (row[column] >= path[column]) ++remaining;
            }
          }
          std::vector<double> before{123}, after{45};
          const auto key = iter.get_key();
          ASSERT_TRUE(iter.try_estimate(before, after));
          ASSERT_TRUE(iter.try_estimate(before, after));
          ASSERT_EQ(
              before,
              (std::vector<double>{123, static_cast<double>(all), static_cast<double>(all)}));
          ASSERT_EQ(
              after, (std::vector<double>{
                         45, static_cast<double>(remaining), static_cast<double>(remaining)}));
          ASSERT_EQ(iter.get_key(), key);
          ASSERT_TRUE(iter.status().ok()) << iter.status();
        };
        std::function<void(size_t)> visit = [&](size_t depth) {
          if (depth == intersections) {
            iter.begin_enumeration();
            for (size_t n = 0; n <= stored.size() && iter.next_enumeration(binding); ++n) {
              Row row = path;
              for (size_t i = initial + intersections; i < 3; ++i)
                row[i] = binding[VarId(i - initial)].id;
              actual.insert(row);
              ASSERT_NO_FATAL_FAILURE(check(depth - 1));
            }
            return;
          }
          iter.down();
          size_t count = 0;
          do {
            path[initial + depth] = iter.get_key();
            ASSERT_NO_FATAL_FAILURE(check(depth));
            ASSERT_NO_FATAL_FAILURE(visit(depth + 1));
            ASSERT_NO_FATAL_FAILURE(check(depth));
            ASSERT_LE(++count, stored.size());
          } while (iter.next());
          std::vector<double> before{7}, after{3};
          ASSERT_FALSE(iter.try_estimate(before, after));
          ASSERT_EQ(before, (std::vector<double>{7}));
          ASSERT_EQ(after, (std::vector<double>{3}));
          iter.up();
        };
        ASSERT_NO_FATAL_FAILURE(visit(0));
        ASSERT_EQ(actual, expected);
      }
    }
  }
}

TEST_F(LeapfrogDocDBIterTest, UnavailableEstimatesPreserveCallerState) {
  Binding binding(3);
  const auto unavailable = [](const LeapfrogDocDBIter& iter) {
    std::vector<double> before{7, 9}, after{3, 4};
    ASSERT_FALSE(iter.try_estimate(before, after));
    ASSERT_EQ(before, (std::vector<double>{7, 9}));
    ASSERT_EQ(after, (std::vector<double>{3, 4}));
  };
  LeapfrogDocDBIter iter(&interruption_, {}, {VarId(0)}, {VarId(1), VarId(2)});
  iter.range_cache().InsertRow(1, 2, 3);
  iter.range_cache().InsertRow(2, 4, 5);
  ASSERT_NO_FATAL_FAILURE(unavailable(iter));
  ASSERT_TRUE(iter.open_terms(binding));
  ASSERT_NO_FATAL_FAILURE(unavailable(iter));
  iter.down();
  std::vector<double> same{8};
  ASSERT_FALSE(iter.try_estimate(same, same));
  ASSERT_EQ(same, (std::vector<double>{8}));
  std::vector<double> before{7}, after;
  ASSERT_FALSE(iter.try_estimate(before, after));
  ASSERT_EQ(before, (std::vector<double>{7}));
  ASSERT_TRUE(after.empty());
  interruption_ = true;
  ASSERT_NO_FATAL_FAILURE(unavailable(iter));
  ASSERT_TRUE(iter.status().ok());
  interruption_ = false;
  before.clear();
  ASSERT_TRUE(iter.try_estimate(before, after));
  ASSERT_EQ(before, (std::vector<double>{2}));
  ASSERT_EQ(after, (std::vector<double>{2}));
  ASSERT_EQ(iter.get_key(), 1);
  ASSERT_TRUE(iter.next());
  ASSERT_FALSE(iter.next());
  ASSERT_NO_FATAL_FAILURE(unavailable(iter));
  iter.up();
  iter.down();
  ASSERT_EQ(iter.get_key(), 1);
  iter.begin_enumeration();
  ASSERT_TRUE(iter.next_enumeration(binding));
  before.clear();
  after.clear();
  ASSERT_TRUE(iter.try_estimate(before, after));
  ASSERT_FALSE(iter.next_enumeration(binding));
  LeapfrogDocDBIter empty(&interruption_, {}, {VarId(0)}, {VarId(1)});
  ASSERT_FALSE(empty.open_terms(binding));
  ASSERT_NO_FATAL_FAILURE(unavailable(empty));
  LeapfrogDocDBIter failed(
      &interruption_, {}, {VarId(0)}, {VarId(1)}, STATUS(IOError, "estimate load failure"));
  failed.range_cache().InsertRow(1, 2, 3);
  ASSERT_FALSE(failed.open_terms(binding));
  ASSERT_NO_FATAL_FAILURE(unavailable(failed));
  ASSERT_TRUE(failed.status().IsIOError());
  LeapfrogDocDBIter invalid(&interruption_, {}, {VarId(0)}, {VarId(0)});
  ASSERT_FALSE(invalid.open_terms(binding));
  ASSERT_NO_FATAL_FAILURE(unavailable(invalid));
  ASSERT_TRUE(invalid.status().IsInvalidArgument());
  LeapfrogDocDBIter enumeration(&interruption_, {}, {}, {VarId(0), VarId(1), VarId(2)});
  enumeration.range_cache().InsertRow(1, 2, 3);
  ASSERT_TRUE(enumeration.open_terms(binding));
  ASSERT_NO_FATAL_FAILURE(unavailable(enumeration));
}

TEST_F(LeapfrogDocDBIterTest, EstimateInvalidationTracksContentAndOwnership) {
  Binding binding(3);
  const auto open = [&](LeapfrogDocDBIter& iter) {
    ASSERT_TRUE(iter.open_terms(binding));
    iter.down();
  };
  const auto estimate = [](const LeapfrogDocDBIter& iter, double expected) {
    std::vector<double> before, after;
    ASSERT_TRUE(iter.try_estimate(before, after));
    ASSERT_EQ(before, (std::vector<double>{expected}));
    ASSERT_EQ(after, before);
  };
  const auto unavailable = [](const LeapfrogDocDBIter& iter) {
    std::vector<double> before{7}, after{3};
    ASSERT_FALSE(iter.try_estimate(before, after));
    ASSERT_EQ(before, (std::vector<double>{7}));
    ASSERT_EQ(after, (std::vector<double>{3}));
  };
  LeapfrogDocDBIter iter(&interruption_, {}, {VarId(0)}, {VarId(1), VarId(2)});
  auto& cache = iter.range_cache();
  cache.InsertRow(1, 2, 3);
  ASSERT_NO_FATAL_FAILURE(open(iter));
  ASSERT_NO_FATAL_FAILURE(estimate(iter, 1));
  uint64_t c0, c1, c2;
  ASSERT_TRUE(cache.SeekRange(0, 9, &c0, &c1, &c2));
  cache.Reset();
  ASSERT_NO_FATAL_FAILURE(estimate(iter, 1));
  cache.InsertRow(1, 2, 3);
  ASSERT_NO_FATAL_FAILURE(unavailable(iter));
  ASSERT_NO_FATAL_FAILURE(open(iter));
  ASSERT_NO_FATAL_FAILURE(estimate(iter, 2));
  cache.Clear();
  cache.InsertRow(1, 2, 3);
  cache.InsertRow(1, 2, 3);
  ASSERT_NO_FATAL_FAILURE(unavailable(iter));
  ASSERT_NO_FATAL_FAILURE(open(iter));
  DocDBRangeCache replacement;
  replacement.InsertRow(1, 2, 3);
  replacement.InsertRow(1, 2, 4);
  cache = replacement;
  ASSERT_NO_FATAL_FAILURE(unavailable(iter));
  ASSERT_NO_FATAL_FAILURE(open(iter));
  ASSERT_NO_FATAL_FAILURE(estimate(iter, 2));
  cache = std::move(replacement);
  ASSERT_NO_FATAL_FAILURE(unavailable(iter));
  ASSERT_NO_FATAL_FAILURE(open(iter));
  DocDBRangeCache moved(std::move(cache));
  ASSERT_NO_FATAL_FAILURE(unavailable(iter));
  cache = std::move(moved);
  ASSERT_NO_FATAL_FAILURE(open(iter));
  ASSERT_NO_FATAL_FAILURE(estimate(iter, 2));
  class SharedIter : public LeapfrogDocDBIter {
   public:
    using LeapfrogDocDBIter::LeapfrogDocDBIter;
    using LeapfrogDocDBIter::UseSharedCache;
  };
  SharedIter shared(&interruption_, {}, {VarId(0)}, {VarId(1), VarId(2)});
  auto owner = std::make_shared<DocDBRangeCache>(cache);
  shared.UseSharedCache(owner);
  ASSERT_NO_FATAL_FAILURE(unavailable(shared));
  ASSERT_NO_FATAL_FAILURE(open(shared));
  ASSERT_NO_FATAL_FAILURE(estimate(shared, 2));
  auto alternate = std::make_shared<DocDBRangeCache>(*owner);
  shared.UseSharedCache(alternate);
  ASSERT_NO_FATAL_FAILURE(unavailable(shared));
  shared.UseSharedCache(owner);
  ASSERT_NO_FATAL_FAILURE(unavailable(shared));
  ASSERT_NO_FATAL_FAILURE(open(shared));
  owner.reset();
  ASSERT_NO_FATAL_FAILURE(estimate(shared, 2));
  // Copying content does not invalidate the source; moving it does.
  ASSERT_NO_FATAL_FAILURE(open(iter));
  replacement = cache;
  ASSERT_NO_FATAL_FAILURE(estimate(iter, 2));
  replacement = std::move(cache);
  ASSERT_NO_FATAL_FAILURE(unavailable(iter));
}

TEST_F(LeapfrogDocDBIterTest, EstimatesRebindInitialTermsWithoutStaleCounts) {
  std::vector<std::unique_ptr<ScanRange>> ranges;
  ranges.push_back(ScanRange::get(Id(VarId(0)), true));
  LeapfrogDocDBIter iter(&interruption_, std::move(ranges), {VarId(1)}, {VarId(2)});
  for (const auto& row :
       std::vector<std::array<uint64_t, 3>>{{5, 10, 100}, {5, 10, 101}, {5, 20, 200}, {7, 30, 300}})
    iter.range_cache().InsertRow(row[0], row[1], row[2]);
  Binding binding(3);
  for (const uint64_t key : {5U, 7U, 5U}) {
    binding.add(VarId(0), ObjectId(key));
    ASSERT_TRUE(iter.open_terms(binding));
    iter.down();
    std::vector<double> before, after;
    ASSERT_TRUE(iter.try_estimate(before, after));
    ASSERT_EQ(before, (std::vector<double>{key == 5 ? 3.0 : 1.0}));
    ASSERT_EQ(after, before);
    if (key == 5) {
      ASSERT_TRUE(iter.seek(20));
      before.clear();
      after.clear();
      ASSERT_TRUE(iter.try_estimate(before, after));
      ASSERT_EQ(before, (std::vector<double>{3}));
      ASSERT_EQ(after, (std::vector<double>{1}));
    }
  }
  binding.add(VarId(0), ObjectId(uint64_t{99}));
  ASSERT_FALSE(iter.open_terms(binding));
  std::vector<double> before{7}, after{3};
  ASSERT_FALSE(iter.try_estimate(before, after));
  ASSERT_EQ(before, (std::vector<double>{7}));
  ASSERT_EQ(after, (std::vector<double>{3}));
}

}  // namespace yb::mdb

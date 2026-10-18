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
#include <limits>
#include <random>
#include <optional>
#include <vector>

#include <gtest/gtest.h>

#include "graph_models/rdf_model/rdf_catalog.h"
#include "query/exceptions.h"
#include "query/optimizer/rdf_model/plan/triple_plan.h"
#include "storage/docdb_range_cache.h"
#include "storage/index/leapfrog/leapfrog_iter.h"
#include "yb/mdb/model/rdf_yb_model.h"

namespace yb::mdb {


TEST(StorageOptTest, CacheInsertionOrdersPreserveUnsignedTupleBag) {
  using Tuple = std::array<uint64_t, 3>;
  const auto max = std::numeric_limits<uint64_t>::max();
  const uint64_t sign_bit = uint64_t{1} << 63;
  std::vector<Tuple> data;
  for (const uint64_t c0 :
       {uint64_t{0}, uint64_t{7}, sign_bit - 1, sign_bit, max}) {
    for (const uint64_t c1 : {uint64_t{0}, uint64_t{17}, max}) {
      for (const uint64_t c2 : {uint64_t{0}, uint64_t{17}, max})
        data.push_back({c0, c1, c2});
    }
  }
  data.insert(data.end(),
              {{0, 0, 0}, {7, 17, 17}, {7, 17, 17}, {max, max, max}});
  std::sort(data.begin(), data.end());
  for (int order = 0; order < 4; ++order) {
    SCOPED_TRACE(order);
    auto input = data;
    if (order == 1)
      std::reverse(input.begin(), input.end());
    if (order == 2) {
      std::mt19937_64 random(7);
      std::shuffle(input.begin(), input.end(), random);
    }
    if (order == 3)
      std::rotate(input.begin(), input.begin() + input.size() / 2, input.end());
    DocDBRangeCache cache;
    std::vector<Tuple> expected;
    for (const auto &row : input) {
      cache.InsertRow(row[0], row[1], row[2]);
      expected.push_back(row);
      std::sort(expected.begin(), expected.end());
      ASSERT_EQ(cache.Size(), expected.size());
      // Search users require sorted data immediately, without a finalize step.
      for (size_t i = 0; i < expected.size(); ++i) {
        Tuple actual;
        cache.GetRowAt(i, &actual[0], &actual[1], &actual[2]);
        ASSERT_EQ(actual, expected[i]);
      }
    }
    ASSERT_EQ(expected, data);
  }
}

TEST(StorageOptTest, CacheTailDuplicatesAndFallbackPreserveScanAndClear) {
  using Tuple = std::array<uint64_t, 3>;
  const auto max = std::numeric_limits<uint64_t>::max();
  DocDBRangeCache cache;
  std::vector<Tuple> expected;
  const auto insert = [&](Tuple row) {
    cache.InsertRow(row[0], row[1], row[2]);
    expected.push_back(row);
  };
  for (uint64_t c1 = 0; c1 < 256; ++c1) {
    insert({7, c1, 0});
    insert({7, c1, max});
    insert({7, c1, max});
  }
  insert({7, 128, 1});
  insert({0, max, max});
  insert({max, max, max});
  insert({max, max, max});
  insert({0, 0, 0});
  std::sort(expected.begin(), expected.end());
  size_t a = 0, b = 0;
  Tuple row;
  ASSERT_TRUE(cache.SeekC0PrefixWithIndex(7, 128, 128, 0, max, &row[0], &row[1],
                                          &row[2], &a));
  ASSERT_EQ(row, (Tuple{7, 128, 0}));
  ASSERT_TRUE(cache.SeekC0PrefixWithIndex(7, 128, 128, max, max, &row[0],
                                          &row[1], &row[2], &b));
  ASSERT_EQ(row, (Tuple{7, 128, max}));
  ASSERT_TRUE(cache.NextC0PrefixWithIndex(7, 128, 0, max, &row[0], &row[1],
                                          &row[2], &a));
  ASSERT_EQ(row, (Tuple{7, 128, 1}));
  ASSERT_TRUE(cache.NextC0PrefixWithIndex(7, 128, max, max, &row[0], &row[1],
                                          &row[2], &b));
  ASSERT_EQ(row, (Tuple{7, 128, max}));
  ASSERT_FALSE(cache.NextC0PrefixWithIndex(7, 128, max, max, &row[0], &row[1],
                                           &row[2], &b));
  for (int repeat = 0; repeat < 2; ++repeat) {
    cache.Reset();
    ASSERT_TRUE(cache.SeekRange(0, max, &row[0], &row[1], &row[2]));
    std::vector<Tuple> actual;
    do {
      actual.push_back(row);
    } while (cache.Next(max, &row[0], &row[1], &row[2]));
    ASSERT_EQ(actual, expected);
  }
  cache.Clear();
  ASSERT_EQ(cache.Size(), 0);
  cache.InsertRow(max, max, max);
  cache.InsertRow(0, 0, 0);
  cache.InsertRow(max, max, max);
  ASSERT_EQ(cache.Size(), 3);
  cache.Reset();
  ASSERT_TRUE(cache.SeekRange(0, max, &row[0], &row[1], &row[2]));
  ASSERT_EQ(row, (Tuple{0, 0, 0}));
  ASSERT_TRUE(cache.Next(max, &row[0], &row[1], &row[2]));
  ASSERT_EQ(row, (Tuple{max, max, max}));
  ASSERT_TRUE(cache.Next(max, &row[0], &row[1], &row[2]));
  ASSERT_EQ(row, (Tuple{max, max, max}));
  ASSERT_FALSE(cache.Next(max, &row[0], &row[1], &row[2]));
}
TEST(StorageOptTest, CatalogUnknownDoesNotImplyEmptyQuery) {
  auto& model = RdfYBModel::Instance();
  model.ClearAll();
  ASSERT_EQ(model.GetStatistics(), nullptr);
  const auto var = [](int value) { return Id(VarId(value)); };
  const Id fixed(::ObjectId(uint64_t{10}));
  for (const auto& terms : std::vector<std::array<Id, 3>>{
           {var(0), var(1), var(2)}, {var(0), fixed, var(2)},
           {var(0), var(0), var(2)}, {var(0), var(1), var(0)},
           {var(0), var(1), var(1)}, {var(0), var(0), var(0)}}) {
    SPARQL::TriplePlan plan(terms[0], terms[1], terms[2]);
    ASSERT_GT(plan.estimate_output_size(), 0);
    auto clone = plan.clone();
    ASSERT_DOUBLE_EQ(clone->estimate_output_size(), plan.estimate_output_size());
    clone->set_input_vars({VarId(0)});
    ASSERT_GT(clone->estimate_output_size(), 0);
  }
  const RdfCatalog catalog;
  const std::array<std::optional<uint64_t>, 11> counts{
      catalog.get_triples_count(), catalog.get_predicate_count(10),
      catalog.get_subject_count(10), catalog.get_object_count(10),
      catalog.get_equal_sp_count(), catalog.get_equal_so_count(),
      catalog.get_equal_po_count(), catalog.get_equal_spo_count(),
      catalog.get_distinct_subjects(), catalog.get_distinct_predicates(),
      catalog.get_distinct_objects()};
  for (const auto& count : counts) ASSERT_FALSE(count.has_value());
  for (int perm = 0; perm < 6; ++perm) ASSERT_FALSE(model.HasQueryCaches(perm));
}

TEST(StorageOptTest, TriplePlanGetVars_AllVars) {
    Id subj(VarId(1));
    Id pred(VarId(2));
    Id obj(VarId(3));

    SPARQL::TriplePlan plan(subj, pred, obj);
    auto vars = plan.get_vars();
    EXPECT_EQ(vars.size(), 3);
    EXPECT_TRUE(vars.count(VarId(1)));
    EXPECT_TRUE(vars.count(VarId(2)));
    EXPECT_TRUE(vars.count(VarId(3)));
}

TEST(StorageOptTest, TriplePlanGetVars_SubjAssigned) {
    Id subj(::ObjectId(1));
    Id pred(VarId(2));
    Id obj(VarId(3));

    SPARQL::TriplePlan plan(subj, pred, obj);
    auto vars = plan.get_vars();
    EXPECT_EQ(vars.size(), 2);
    EXPECT_TRUE(vars.count(VarId(2)));
    EXPECT_TRUE(vars.count(VarId(3)));
}

TEST(StorageOptTest, TriplePlanGetVars_AllAssigned) {
    Id subj(::ObjectId(1));
    Id pred(::ObjectId(2));
    Id obj(::ObjectId(3));

    SPARQL::TriplePlan plan(subj, pred, obj);
    auto vars = plan.get_vars();
    EXPECT_EQ(vars.size(), 0);
}

TEST(StorageOptTest, TriplePlanSetInputVars) {
    Id subj(VarId(1));
    Id pred(VarId(2));
    Id obj(VarId(3));

    SPARQL::TriplePlan plan(subj, pred, obj);

    EXPECT_FALSE(plan.subject_assigned);
    EXPECT_FALSE(plan.predicate_assigned);
    EXPECT_FALSE(plan.object_assigned);

    std::set<VarId> input_vars = { VarId(1) };
    plan.set_input_vars(input_vars);

    EXPECT_TRUE(plan.subject_assigned);
    EXPECT_FALSE(plan.predicate_assigned);
    EXPECT_FALSE(plan.object_assigned);

    auto vars = plan.get_vars();
    EXPECT_EQ(vars.size(), 2);
}

TEST(StorageOptTest, TriplePlanRelationSize) {
    Id subj(VarId(1));
    Id pred(VarId(2));
    Id obj(VarId(3));

    SPARQL::TriplePlan plan(subj, pred, obj);
    EXPECT_EQ(plan.relation_size(), 3);
}

TEST(StorageOptTest, TriplePlanEstimateAllTerms) {
    Id subj(::ObjectId(1));
    Id pred(::ObjectId(2));
    Id obj(::ObjectId(3));

    SPARQL::TriplePlan plan(subj, pred, obj);
    EXPECT_EQ(plan.estimate_output_size(), 1.0);
}

TEST(StorageOptTest, TriplePlanEstimateCost) {
    Id subj(VarId(1));
    Id pred(VarId(2));
    Id obj(VarId(3));

    SPARQL::TriplePlan plan(subj, pred, obj);
    double size = plan.estimate_output_size();
    double cost = plan.estimate_cost();
    EXPECT_EQ(cost, size);
}

TEST(StorageOptTest, TriplePlanGetBindingIterThrows) {
    Id subj(VarId(1));
    Id pred(VarId(2));
    Id obj(VarId(3));

    SPARQL::TriplePlan plan(subj, pred, obj);
    EXPECT_THROW(plan.get_binding_iter(), NotSupportedException);
}

TEST(StorageOptTest, TriplePlanGetLeapfrogIterReturnsFalse) {
    Id subj(VarId(1));
    Id pred(VarId(2));
    Id obj(VarId(3));

    SPARQL::TriplePlan plan(subj, pred, obj);
    std::vector<std::unique_ptr<LeapfrogIter>> leapfrog_iters;
    std::vector<VarId> var_order;
    uint_fast32_t enumeration_level = 0;
    EXPECT_FALSE(plan.get_leapfrog_iter(leapfrog_iters, var_order, enumeration_level));
}

TEST(StorageOptTest, C0PrefixSeekHonorsSortedBounds) {
    DocDBRangeCache cache;
    for (uint64_t c1 = 10; c1 <= 20; ++c1) {
        for (uint64_t c2 = 0; c2 < 3; ++c2) {
            cache.InsertRow(7, c1, c2);
        }
    }
    cache.InsertRow(8, 12, 1);

    size_t index = 0;
    size_t prefix_rows_examined = 0;
    uint64_t c0 = 0, c1 = 0, c2 = 0;
    ASSERT_TRUE(cache.SeekC0PrefixWithIndex(
        7, 12, 14, 1, 2, &c0, &c1, &c2, &index, &prefix_rows_examined));
    // c1=10 and c1=11 contribute six rows before the lower-bound match.
    EXPECT_EQ(index, 7U);
    EXPECT_EQ(prefix_rows_examined, 1U);

    size_t legacy_index = 0;
    size_t legacy_rows_examined = 0;
    uint64_t legacy_c0 = 0, legacy_c1 = 0, legacy_c2 = 0;
    ASSERT_TRUE(cache.SeekRangeWithIndex(
        7, 7, &legacy_c0, &legacy_c1, &legacy_c2, &legacy_index,
        &legacy_rows_examined));
    while (legacy_c1 != 12 || legacy_c2 != 1) {
        ASSERT_TRUE(cache.NextWithIndex(
            7, &legacy_c0, &legacy_c1, &legacy_c2, &legacy_index));
        ++legacy_rows_examined;
    }
    EXPECT_EQ(legacy_rows_examined, 8U);
    EXPECT_GT(legacy_rows_examined, prefix_rows_examined);

    int rows = 0;
    do {
        EXPECT_EQ(c0, 7);
        EXPECT_GE(c1, 12);
        EXPECT_LE(c1, 14);
        EXPECT_GE(c2, 1);
        EXPECT_LE(c2, 2);
        ++rows;
    } while (cache.NextC0PrefixWithIndex(
        7, 14, 1, 2, &c0, &c1, &c2, &index));
    EXPECT_EQ(rows, 6);

    EXPECT_FALSE(cache.SeekC0PrefixWithIndex(
        7, 30, 31, 0, 2, &c0, &c1, &c2, &index));
    EXPECT_FALSE(cache.SeekC0PrefixWithIndex(
        9, 0, 100, 0, 2, &c0, &c1, &c2, &index));
}

TEST(StorageOptTest, C0PrefixSeekCandidateRowsScaleWithSelectivity) {
    DocDBRangeCache cache;
    for (uint64_t c1 = 0; c1 < 256; ++c1) {
        for (uint64_t c2 = 0; c2 < 4; ++c2) {
            cache.InsertRow(7, c1, c2);
        }
    }

    for (uint64_t min_c1 : {0ULL, 64ULL, 192ULL}) {
        size_t prefix_index = 0;
        size_t prefix_rows_examined = 0;
        uint64_t c0 = 0, c1 = 0, c2 = 0;
        ASSERT_TRUE(cache.SeekC0PrefixWithIndex(
            7, min_c1, min_c1, 0, 0, &c0, &c1, &c2,
            &prefix_index, &prefix_rows_examined));
        EXPECT_EQ(c0, 7);
        EXPECT_EQ(c1, min_c1);
        EXPECT_EQ(c2, 0);
        EXPECT_EQ(prefix_rows_examined, 1U);

        size_t legacy_index = 0;
        size_t legacy_rows_examined = 0;
        ASSERT_TRUE(cache.SeekRangeWithIndex(
            7, 7, &c0, &c1, &c2, &legacy_index, &legacy_rows_examined));
        while (c1 != min_c1 || c2 != 0) {
            ASSERT_TRUE(cache.NextWithIndex(7, &c0, &c1, &c2, &legacy_index));
            ++legacy_rows_examined;
        }
        EXPECT_EQ(legacy_rows_examined, min_c1 * 4 + 1);
    }
}

TEST(StorageOptTest, TriplePlanClone) {
    Id subj(VarId(1));
    Id pred(::ObjectId(2));
    Id obj(VarId(3));

    SPARQL::TriplePlan plan(subj, pred, obj);
    auto clone = plan.clone();

    auto orig_vars = plan.get_vars();
    auto clone_vars = clone->get_vars();
    EXPECT_EQ(orig_vars, clone_vars);

    EXPECT_EQ(plan.estimate_output_size(), clone->estimate_output_size());
}

} // namespace yb::mdb

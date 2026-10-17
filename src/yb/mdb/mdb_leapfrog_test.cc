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
      intr,
      std::vector<std::unique_ptr<ScanRange>>{},
      std::vector<VarId>{VarId(0)},
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
      intr,
      std::vector<std::unique_ptr<ScanRange>>{},
      std::vector<VarId>{VarId(0)},
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
      intr,
      std::vector<std::unique_ptr<ScanRange>>{},
      std::vector<VarId>{VarId(0)},
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
      intr,
      std::vector<std::unique_ptr<ScanRange>>{},
      std::vector<VarId>{VarId(0)},
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
      intr,
      std::vector<std::unique_ptr<ScanRange>>{},
      std::vector<VarId>{VarId(0)},
      std::vector<VarId>{VarId(1)});

  Binding binding(2);
  ASSERT_FALSE(iter->open_terms(binding));
}

TEST_F(LeapfrogDocDBIterTest, SeekMiddleValue) {
  bool* intr = &interruption_;
  auto iter = std::make_unique<LeapfrogDocDBIter>(
      intr,
      std::vector<std::unique_ptr<ScanRange>>{},
      std::vector<VarId>{VarId(0)},
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
      intr,
      std::vector<std::unique_ptr<ScanRange>>{},
      std::vector<VarId>{VarId(0)},
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
      intr,
      std::vector<std::unique_ptr<ScanRange>>{},
      std::vector<VarId>{},
      std::vector<VarId>{});

  EXPECT_EQ(iter->get_iter_name(), "LeapfrogDocDBIter");
}

}  // namespace yb::mdb

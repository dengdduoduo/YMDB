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
#include <string>
#include <vector>

#include "query/executor/binding.h"
#include "query/executor/binding_iter/leapfrog_join.h"
#include "yb/mdb/adapter/mdb_adapter.h"
#include "yb/mdb/storage/leapfrog_docdb_iter.h"

namespace yb::mdb {

TEST(MdbPipelineTest, YBCMdbInitReturnsZero) {
  ASSERT_EQ(YBCMdbInit(), 0);
}

TEST(MdbPipelineTest, ExecuteSparqlNullSafety) {
  ASSERT_EQ(YBCMdbExecuteSparql(nullptr, nullptr, nullptr), 1);
  ASSERT_EQ(YBCMdbExecuteSparql(
      "SELECT ?s WHERE { ?s ?p ?o }", nullptr, nullptr), 1);
}

TEST(MdbPipelineTest, ParseSyntaxError) {
  std::vector<std::pair<std::string, std::string>> captured;
  auto cb = [](const char* n, const char* v, void* ctx) {
    static_cast<decltype(&captured)>(ctx)->emplace_back(n, v);
  };
  int rc = YBCMdbExecuteSparql("SELECT ?s WHERE { ?s ?p", cb, &captured);
  EXPECT_NE(rc, 0);
}

TEST(MdbPipelineTest, FullPipelineSingleIterWithData) {
  bool interrupted = false;
  auto iter = std::make_unique<LeapfrogDocDBIter>(
      &interrupted,
      std::vector<std::unique_ptr<ScanRange>>{},
      std::vector<VarId>{VarId(0)},
      std::vector<VarId>{VarId(1)});

  iter->range_cache().InsertRow(100, 200, 300);
  iter->range_cache().InsertRow(101, 201, 301);
  iter->range_cache().InsertRow(102, 202, 302);

  std::vector<std::string> vals;
  auto cb = [](const char* name, const char* value, void* ctx) {
    static_cast<std::vector<std::string>*>(ctx)->push_back(value);
  };

  Binding binding(10);
  ASSERT_TRUE(iter->open_terms(binding));
  iter->down();
  do {
    cb("?s", std::to_string(iter->get_key()).c_str(), &vals);
  } while (iter->next());

  ASSERT_EQ(vals.size(), 3);
  EXPECT_EQ(vals[0], "100");
  EXPECT_EQ(vals[1], "101");
  EXPECT_EQ(vals[2], "102");
}

TEST(MdbPipelineTest, FullPipelineTwoIterJoin) {
  bool interrupted = false;

  auto iter1 = std::make_unique<LeapfrogDocDBIter>(
      &interrupted,
      std::vector<std::unique_ptr<ScanRange>>{},
      std::vector<VarId>{VarId(0)},
      std::vector<VarId>{VarId(1)});

  auto iter2 = std::make_unique<LeapfrogDocDBIter>(
      &interrupted,
      std::vector<std::unique_ptr<ScanRange>>{},
      std::vector<VarId>{VarId(0)},
      std::vector<VarId>{VarId(2)});

  iter1->range_cache().InsertRow(10, 100, 1);
  iter1->range_cache().InsertRow(20, 100, 2);
  iter1->range_cache().InsertRow(30, 100, 3);

  iter2->range_cache().InsertRow(10, 200, 11);
  iter2->range_cache().InsertRow(20, 200, 21);
  iter2->range_cache().InsertRow(40, 200, 31);

  std::vector<std::unique_ptr<LeapfrogIter>> iters;
  iters.push_back(std::move(iter1));
  iters.push_back(std::move(iter2));

  LeapfrogJoin join(
      std::move(iters),
      std::vector<VarId>{VarId(0), VarId(1), VarId(2)},
      1);

  Binding binding(10);
  join.begin(binding);

  std::vector<std::string> vals;
  auto cb = [](const char* name, const char* value, void* ctx) {
    static_cast<std::vector<std::string>*>(ctx)->push_back(value);
  };

  while (join.next()) {
    cb("?s", std::to_string(binding[VarId(0)].id).c_str(), &vals);
  }

  ASSERT_EQ(vals.size(), 2);
  EXPECT_EQ(vals[0], "10");
  EXPECT_EQ(vals[1], "20");
}

}  // namespace yb::mdb

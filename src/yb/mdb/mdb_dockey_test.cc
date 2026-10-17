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

#include "yb/mdb/storage/docdb_key_encoder.h"

namespace yb::mdb {

TEST(DocKeyEncoderTest, PermuteSPO) {
  uint64_t c0, c1, c2;
  DocKeyEncoder::Permute(DocKeyEncoder::kSPO, 100, 200, 300, &c0, &c1, &c2);
  ASSERT_EQ(c0, 100);
  ASSERT_EQ(c1, 200);
  ASSERT_EQ(c2, 300);
}

TEST(DocKeyEncoderTest, PermutePOS) {
  uint64_t c0, c1, c2;
  DocKeyEncoder::Permute(DocKeyEncoder::kPOS, 100, 200, 300, &c0, &c1, &c2);
  ASSERT_EQ(c0, 200);
  ASSERT_EQ(c1, 300);
  ASSERT_EQ(c2, 100);
}

TEST(DocKeyEncoderTest, PermuteOSP) {
  uint64_t c0, c1, c2;
  DocKeyEncoder::Permute(DocKeyEncoder::kOSP, 100, 200, 300, &c0, &c1, &c2);
  ASSERT_EQ(c0, 300);
  ASSERT_EQ(c1, 100);
  ASSERT_EQ(c2, 200);
}

TEST(DocKeyEncoderTest, PermutePSO) {
  uint64_t c0, c1, c2;
  DocKeyEncoder::Permute(DocKeyEncoder::kPSO, 100, 200, 300, &c0, &c1, &c2);
  ASSERT_EQ(c0, 200);
  ASSERT_EQ(c1, 100);
  ASSERT_EQ(c2, 300);
}

TEST(DocKeyEncoderTest, PermuteSOP) {
  uint64_t c0, c1, c2;
  DocKeyEncoder::Permute(DocKeyEncoder::kSOP, 100, 200, 300, &c0, &c1, &c2);
  ASSERT_EQ(c0, 100);
  ASSERT_EQ(c1, 300);
  ASSERT_EQ(c2, 200);
}

TEST(DocKeyEncoderTest, PermuteOPS) {
  uint64_t c0, c1, c2;
  DocKeyEncoder::Permute(DocKeyEncoder::kOPS, 100, 200, 300, &c0, &c1, &c2);
  ASSERT_EQ(c0, 300);
  ASSERT_EQ(c1, 200);
  ASSERT_EQ(c2, 100);
}

TEST(DocKeyEncoderTest, TableNames) {
  ASSERT_STREQ(DocKeyEncoder::TableName(DocKeyEncoder::kSPO), "rdf_spo");
  ASSERT_STREQ(DocKeyEncoder::TableName(DocKeyEncoder::kPOS), "rdf_pos");
  ASSERT_STREQ(DocKeyEncoder::TableName(DocKeyEncoder::kOSP), "rdf_osp");
  ASSERT_STREQ(DocKeyEncoder::TableName(DocKeyEncoder::kPSO), "rdf_pso");
  ASSERT_STREQ(DocKeyEncoder::TableName(DocKeyEncoder::kSOP), "rdf_sop");
  ASSERT_STREQ(DocKeyEncoder::TableName(DocKeyEncoder::kOPS), "rdf_ops");
}

}  // namespace yb::mdb

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

#pragma once

#include <cstdint>

namespace yb::mdb {

// Maps RDF Hexastore permutations to the column order in each table.
// perm: 0=SPO, 1=POS, 2=OSP, 3=PSO, 4=SOP, 5=OPS
class DocKeyEncoder {
 public:
  static constexpr int kSPO = 0;
  static constexpr int kPOS = 1;
  static constexpr int kOSP = 2;
  static constexpr int kPSO = 3;
  static constexpr int kSOP = 4;
  static constexpr int kOPS = 5;

  // Permute (s, p, o) according to the hexastore permutation index.
  // Result is (c0, c1, c2) representing the primary key columns in order.
  static inline void Permute(int perm,
                              uint64_t s, uint64_t p, uint64_t o,
                              uint64_t* c0, uint64_t* c1, uint64_t* c2) {
    switch (perm) {
      case kSPO: *c0 = s; *c1 = p; *c2 = o; break;
      case kPOS: *c0 = p; *c1 = o; *c2 = s; break;
      case kOSP: *c0 = o; *c1 = s; *c2 = p; break;
      case kPSO: *c0 = p; *c1 = s; *c2 = o; break;
      case kSOP: *c0 = s; *c1 = o; *c2 = p; break;
      case kOPS: *c0 = o; *c1 = p; *c2 = s; break;
    }
  }

  // Reverse permute: map (c0, c1, c2) back to (s, p, o) according to perm.
  static inline void ReversePermute(int perm,
                                    uint64_t c0, uint64_t c1, uint64_t c2,
                                    uint64_t* s, uint64_t* p, uint64_t* o) {
    switch (perm) {
      case kSPO: *s = c0; *p = c1; *o = c2; break;
      case kPOS: *s = c2; *p = c0; *o = c1; break;
      case kOSP: *s = c1; *p = c2; *o = c0; break;
      case kPSO: *s = c1; *p = c0; *o = c2; break;
      case kSOP: *s = c0; *p = c2; *o = c1; break;
      case kOPS: *s = c2; *p = c1; *o = c0; break;
    }
  }

  // Returns the table name for a given permutation.
  static const char* TableName(int perm);
};

}  // namespace yb::mdb

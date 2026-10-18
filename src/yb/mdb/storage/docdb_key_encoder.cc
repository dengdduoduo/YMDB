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

#include "yb/mdb/storage/docdb_key_encoder.h"

namespace yb::mdb {

const char* DocKeyEncoder::TableName(int perm) {
  switch (perm) {
    case kSPO: return "rdf_spo";
    case kPOS: return "rdf_pos";
    case kOSP: return "rdf_osp";
    case kPSO: return "rdf_pso";
    case kSOP: return "rdf_sop";
    case kOPS: return "rdf_ops";
  }
  return "rdf_spo";
}

}  // namespace yb::mdb

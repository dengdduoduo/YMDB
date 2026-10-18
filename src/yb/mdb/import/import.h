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
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "yb/util/result.h"

namespace yb::mdb {

enum class TripleTermType {
  URI,
  BLANK,
  LITERAL,
};

struct Triple {
  std::string sub;
  std::string pred;
  std::string obj;
  TripleTermType sub_type;
  TripleTermType pred_type;
  TripleTermType obj_type;
  std::string object_type;
  std::string object_language = {};

  bool operator==(const Triple& other) const {
    return sub == other.sub && pred == other.pred && obj == other.obj &&
           sub_type == other.sub_type && pred_type == other.pred_type &&
           obj_type == other.obj_type && object_type == other.object_type &&
           object_language == other.object_language;
  }
};

struct ImportStats {
  int64_t lines_read = 0;
  int64_t triples_parsed = 0;
  int64_t triples_written = 0;
  int64_t errors = 0;
  int64_t elapsed_ms = 0;
};

struct RdfTerm;

class IdMap {
 public:
  // Graph IDs must not overlap the packed ObjectId type byte.
  static constexpr uint64_t kMaxId = (uint64_t{1} << 56) - 1;
  Result<uint64_t> GetOrCreate(std::string_view uri, TripleTermType kind = TripleTermType::URI);
  Result<uint64_t> GetOrCreate(const RdfTerm& term);
  bool Restore(uint64_t id, const RdfTerm& term);
  // Restore persisted IDs before allocating IDs for an appended file.
  bool Restore(uint64_t id, std::string_view uri, TripleTermType kind = TripleTermType::URI);
  const std::string& Lookup(uint64_t id) const;
  bool Contains(std::string_view uri, TripleTermType kind = TripleTermType::URI) const;
  uint64_t Get(std::string_view uri, TripleTermType kind = TripleTermType::URI) const;
  size_t Size() const;
  const std::unordered_map<uint64_t, std::string>& Entries() const { return id_to_uri_; }

 private:
  std::unordered_map<std::string, uint64_t> uri_to_id_;
  std::unordered_map<uint64_t, std::string> id_to_uri_;
  uint64_t high_water_ = 0;
  std::unordered_map<uint64_t, std::string> id_to_key_;
};

}  // namespace yb::mdb

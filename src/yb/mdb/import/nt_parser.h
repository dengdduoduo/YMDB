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

#include <string>
#include <string_view>
#include <vector>

#include "yb/mdb/import/import.h"
#include "yb/util/result.h"

namespace yb::mdb {

class NtParser {
 public:
  static Status ParseFile(
      const std::string& file_path, std::vector<Triple>& triples, ImportStats& stats);
  static Result<Triple> ParseLine(const std::string& line);
  // Absolute IRIs, labelled blank nodes and double-quoted literals only.
  // Used by INSERT DATA; unsupported Turtle abbreviations return an error.
  static Result<std::vector<Triple>> ParseSequence(std::string_view text);
};

}  // namespace yb::mdb

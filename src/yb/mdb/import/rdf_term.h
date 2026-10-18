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

#include "yb/mdb/import/import.h"

namespace yb::mdb {

// Versioned identity, independent of display text and packed query IDs.
struct RdfTerm {
  TripleTermType kind = TripleTermType::URI;
  std::string lexical;
  std::string language;
  std::string datatype;

  Result<std::string> Key() const;
  static Result<RdfTerm> FromKey(std::string_view key);
};

RdfTerm ObjectTerm(const Triple& triple);
void ScopeBlankNodes(Triple* triple, const std::string& document_scope);

}  // namespace yb::mdb

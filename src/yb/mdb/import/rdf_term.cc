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

#include "yb/mdb/import/rdf_term.h"

#include <limits>

namespace yb::mdb {
namespace {
constexpr const char* kXsdString = "http://www.w3.org/2001/XMLSchema#string";

void Append(std::string_view value, std::string* key) {
  *key += std::to_string(value.size()) + ":";
  key->append(value);
}
}  // namespace

Result<std::string> RdfTerm::Key() const {
  if (kind != TripleTermType::URI && kind != TripleTermType::BLANK &&
      kind != TripleTermType::LITERAL) {
    return STATUS(InvalidArgument, "Invalid RDF term kind");
  }
  if (kind != TripleTermType::LITERAL &&
      (lexical.empty() || !language.empty() || !datatype.empty())) {
    return STATUS(InvalidArgument, "Invalid RDF resource term");
  }
  if (!language.empty() && !datatype.empty()) {
    return STATUS(InvalidArgument, "Literal has both language and datatype");
  }
  std::string canonical_language = language;
  for (auto& c : canonical_language) {
    if (c >= 'A' && c <= 'Z') c += 'a' - 'A';
  }
  const auto canonical_datatype = datatype == kXsdString ? std::string() : datatype;
  std::string key = "T2";
  key += static_cast<char>('0' + static_cast<int>(kind));
  Append(lexical, &key);
  Append(canonical_language, &key);
  Append(canonical_datatype, &key);
  return key;
}

Result<RdfTerm> RdfTerm::FromKey(std::string_view key) {
  if (key.size() < 3 || key.substr(0, 2) != "T2" || key[2] < '0' || key[2] > '2') {
    return STATUS(Corruption, "Unsupported RDF term key version or kind");
  }
  const std::string original(key);
  RdfTerm term;
  term.kind = static_cast<TripleTermType>(key[2] - '0');
  key.remove_prefix(3);
  for (auto* field : {&term.lexical, &term.language, &term.datatype}) {
    size_t size = 0;
    size_t digits = 0;
    while (digits < key.size() && key[digits] >= '0' && key[digits] <= '9') {
      const size_t next = key[digits] - '0';
      if (size > (std::numeric_limits<size_t>::max() - next) / 10) {
        return STATUS(Corruption, "RDF term field length overflow");
      }
      size = size * 10 + next;
      ++digits;
    }
    if (!digits || digits >= key.size() || key[digits] != ':') {
      return STATUS(Corruption, "Invalid RDF term field length");
    }
    key.remove_prefix(digits + 1);
    if (size > key.size()) return STATUS(Corruption, "Truncated RDF term field");
    field->assign(key.substr(0, size));
    key.remove_prefix(size);
  }
  if (!key.empty()) return STATUS(Corruption, "Trailing RDF term data");
  const auto canonical = term.Key();
  if (!canonical.ok() || *canonical != original) {
    return STATUS(Corruption, "Invalid or noncanonical RDF term metadata");
  }
  return term;
}

RdfTerm ObjectTerm(const Triple& triple) {
  return {triple.obj_type, triple.obj, triple.object_language, triple.object_type};
}

void ScopeBlankNodes(Triple* triple, const std::string& document_scope) {
  if (triple->sub_type == TripleTermType::BLANK) {
    triple->sub = "_:mdb:" + document_scope + ":" + triple->sub;
  }
  if (triple->obj_type == TripleTermType::BLANK) {
    triple->obj = "_:mdb:" + document_scope + ":" + triple->obj;
  }
}

}  // namespace yb::mdb

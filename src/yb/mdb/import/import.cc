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

#include "yb/mdb/import/import.h"

#include "yb/mdb/import/rdf_term.h"

namespace yb::mdb {

Result<uint64_t> IdMap::GetOrCreate(std::string_view uri, TripleTermType kind) {
  return GetOrCreate(RdfTerm{kind, std::string(uri), "", ""});
}

Result<uint64_t> IdMap::GetOrCreate(const RdfTerm& term) {
  const auto key = VERIFY_RESULT(term.Key());
  const auto found = uri_to_id_.find(key);
  if (found != uri_to_id_.end()) return found->second;
  if (high_water_ == kMaxId) return STATUS(IllegalState, "RDF graph ID space exhausted");
  const uint64_t id = high_water_ + 1;
  if (!Restore(id, term)) return STATUS(Corruption, "Cannot cache allocated RDF term");
  return id;
}

bool IdMap::Restore(uint64_t id, std::string_view uri, TripleTermType kind) {
  return Restore(id, RdfTerm{kind, std::string(uri), "", ""});
}

bool IdMap::Restore(uint64_t id, const RdfTerm& term) {
  if (id == 0 || id > kMaxId) return false;
  const auto key = term.Key();
  if (!key.ok()) return false;
  const auto found = uri_to_id_.find(*key);
  if (found != uri_to_id_.end() && found->second != id) return false;
  const auto occupied = id_to_key_.find(id);
  if (occupied != id_to_key_.end() && occupied->second != *key) return false;
  id_to_uri_[id] = term.lexical;
  id_to_key_[id] = *key;
  uri_to_id_[*key] = id;
  if (id > high_water_) high_water_ = id;
  return true;
}

const std::string& IdMap::Lookup(uint64_t id) const {
  const auto found = id_to_uri_.find(id);
  if (found == id_to_uri_.end()) {
    static const std::string k_empty;
    return k_empty;
  }
  return found->second;
}

bool IdMap::Contains(std::string_view uri, TripleTermType kind) const {
  return Get(uri, kind) != 0;
}

uint64_t IdMap::Get(std::string_view uri, TripleTermType kind) const {
  const auto key = RdfTerm{kind, std::string(uri), "", ""}.Key();
  if (!key.ok()) return 0;
  auto it = uri_to_id_.find(*key);
  if (it != uri_to_id_.end()) {
    return it->second;
  }
  return 0;
}

size_t IdMap::Size() const {
  return id_to_uri_.size();
}

}  // namespace yb::mdb

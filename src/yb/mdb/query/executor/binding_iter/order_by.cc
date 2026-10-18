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

#include "yb/mdb/query/executor/binding_iter/order_by.h"

#include <string>

#include "graph_models/rdf_model/comparisons.h"
#include "graph_models/rdf_model/conversions.h"
#include "graph_models/rdf_model/rdf_object_id.h"

namespace {

ObjectId Packed(ObjectId id) {
  const auto found = g_gmap_to_pack.find(id.id);
  return found == g_gmap_to_pack.end() ? id : found->second;
}

}  // namespace

yb::Status OrderBy::ValidateKey(ObjectId key) {
  key = Packed(key);
  if (RDF_OID::get_generic_type(key) == RDF_OID::GenericType::IRI) {
    std::string text;
    if (!SPARQL::Conversions::try_get_iri(key, &text)) {
      return STATUS(Corruption, "ORDER BY IRI is missing its lexical dictionary entry");
    }
  }
  return yb::Status::OK();
}

int64_t OrderBy::rdf_compare(ObjectId lhs, ObjectId rhs) {
  lhs = Packed(lhs);
  rhs = Packed(rhs);
  if (lhs.id == rhs.id) return 0;
  if (lhs.is_null()) return -1;
  if (rhs.is_null()) return 1;
  const auto left_value = SPARQL::Conversions::value_operand(lhs);
  const auto right_value = SPARQL::Conversions::value_operand(rhs);
  if (!left_value.is_null()) lhs = left_value;
  if (!right_value.is_null()) rhs = right_value;
  const auto left_type = RDF_OID::get_generic_type(lhs);
  const auto right_type = RDF_OID::get_generic_type(rhs);
  if (left_type == right_type &&
      (left_type == RDF_OID::GenericType::IRI || left_type == RDF_OID::GenericType::STRING)) {
    const auto left = SPARQL::Conversions::to_lexical_str(lhs);
    const auto right = SPARQL::Conversions::to_lexical_str(rhs);
    return (left > right) - (left < right);
  }
  if (left_type == RDF_OID::GenericType::NUMERIC && right_type == left_type) {
    return SPARQL::Comparisons::order_compare(lhs, rhs);
  }
  return internal_compare(lhs, rhs);
}

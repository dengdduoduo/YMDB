#include "yb/mdb/import/import.h"

#include <limits>
#include <stdexcept>
#include <utility>

namespace yb::mdb {

uint64_t IdMap::GetOrCreate(std::string_view uri) {
  auto it = uri_to_id_.find(std::string(uri));
  if (it != uri_to_id_.end()) {
    return it->second;
  }
  if (max_id_ == std::numeric_limits<uint64_t>::max()) {
    throw std::overflow_error("RDF term ID space exhausted");
  }
  uint64_t id = max_id_ + 1;
  std::string uri_str(uri);
  uri_to_id_[uri_str] = id;
  id_to_uri_.emplace(id, std::move(uri_str));
  max_id_ = id;
  return id;
}

const std::string& IdMap::Lookup(uint64_t id) const {
  auto it = id_to_uri_.find(id);
  if (it == id_to_uri_.end()) {
    static const std::string k_empty;
    return k_empty;
  }
  return it->second;
}

bool IdMap::Contains(std::string_view uri) const {
  return uri_to_id_.find(std::string(uri)) != uri_to_id_.end();
}

uint64_t IdMap::Get(std::string_view uri) const {
  auto it = uri_to_id_.find(std::string(uri));
  if (it != uri_to_id_.end()) {
    return it->second;
  }
  return 0;
}

size_t IdMap::Size() const {
  return id_to_uri_.size();
}

bool IdMap::Restore(uint64_t id, std::string_view uri) {
  if (id == 0) return false;
  std::string term(uri);
  auto by_term = uri_to_id_.find(term);
  if (by_term != uri_to_id_.end() && by_term->second != id) return false;
  auto by_id = id_to_uri_.find(id);
  if (by_id != id_to_uri_.end() && by_id->second != term) return false;
  uri_to_id_.emplace(term, id);
  id_to_uri_.emplace(id, std::move(term));
  if (id > max_id_) max_id_ = id;
  return true;
}

}  // namespace yb::mdb

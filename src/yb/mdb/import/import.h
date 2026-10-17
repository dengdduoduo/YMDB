#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

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

  bool operator==(const Triple& other) const {
    return sub == other.sub && pred == other.pred && obj == other.obj &&
           sub_type == other.sub_type && pred_type == other.pred_type &&
           obj_type == other.obj_type && object_type == other.object_type;
  }
};

struct ImportStats {
  int64_t lines_read = 0;
  int64_t triples_parsed = 0;
  int64_t triples_written = 0;
  int64_t errors = 0;
  int64_t elapsed_ms = 0;
};

class IdMap {
 public:
  uint64_t GetOrCreate(std::string_view uri);
  const std::string& Lookup(uint64_t id) const;
  bool Contains(std::string_view uri) const;
  uint64_t Get(std::string_view uri) const;
  size_t Size() const;
  // Restore an explicit persisted ID without renumbering existing terms.
  // Conflicting mappings and ID zero are rejected without changing the map.
  bool Restore(uint64_t id, std::string_view uri);
  uint64_t MaxId() const { return max_id_; }
  const std::unordered_map<uint64_t, std::string>& Entries() const { return id_to_uri_; }

 private:
  std::unordered_map<std::string, uint64_t> uri_to_id_;
  std::unordered_map<uint64_t, std::string> id_to_uri_;
  uint64_t max_id_ = 0;
};

}  // namespace yb::mdb

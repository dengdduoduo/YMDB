#pragma once

#include <array>
#include <cmath>
#include <cstdint>
#include <map>
#include <sstream>
#include <string>

namespace yb::mdb {
// Full snapshot scan counts with bounded metadata. Distinct counts use a
// fixed 8192-bit linear-counting sketch; a saturated sketch is unavailable.
struct QueryStatistics {
  struct Distinct {
    std::array<uint64_t, 128> bits{};
    void Add(uint64_t value) {
      value ^= value >> 30; value *= 0xbf58476d1ce4e5b9ULL;
      value ^= value >> 27; value *= 0x94d049bb133111ebULL;
      value ^= value >> 31;
      const auto bit = value % 8192;
      bits[bit / 64] |= uint64_t(1) << (bit % 64);
    }
    double Estimate() const {
      size_t occupied = 0;
      for (auto word : bits) while (word) { word &= word - 1; ++occupied; }
      return occupied == 8192 ? -1 : -8192.0 * std::log(1.0 - occupied / 8192.0);
    }
  } subjects, objects;
  uint64_t version = 0, collected_unix_ms = 0, triples = 0;
  uint64_t read_operations = 0, payload_bytes = 0;
  bool predicates_complete = true, degrees_complete = true;
  std::map<uint64_t, uint64_t> predicates, subject_degrees;
  static constexpr size_t kMaxKeys = 4096;
  void Add(uint64_t s, uint64_t p, uint64_t o) {
    ++triples; subjects.Add(s); objects.Add(o);
    auto add = [](auto* map, uint64_t key, bool* complete) {
      auto it = map->find(key);
      if (it != map->end()) ++it->second;
      else if (map->size() < kMaxKeys) map->emplace(key, 1);
      else *complete = false;
    };
    add(&predicates, p, &predicates_complete);
    add(&subject_degrees, s, &degrees_complete);
  }
  std::string ToJson() const {
    std::ostringstream out;
    out << "{\"available\":true,\"version\":" << version
        << ",\"collected_unix_ms\":" << collected_unix_ms
        << ",\"collection\":\"full_snapshot_scan\",\"triples_exact\":" << triples
        << ",\"distinct_method\":\"linear_counting_8192_bits\",\"distinct_subjects_estimate\":"
        << subjects.Estimate() << ",\"distinct_objects_estimate\":" << objects.Estimate()
        << ",\"average_logical_row_bytes\":24,\"predicates_complete\":"
        << (predicates_complete ? "true" : "false") << ",\"predicate_counts\":{";
    bool comma = false;
    for (auto pair : predicates) {
      if (comma) out << ',';
      comma = true; out << '"' << pair.first << "\":" << pair.second;
    }
    out << "},\"subject_degrees_complete\":" << (degrees_complete ? "true" : "false")
        << ",\"degree_population\":\"tracked_subjects\",\"degree_histogram\":{";
    std::map<uint64_t, uint64_t> histogram;
    uint64_t heavy_id = 0, heavy_degree = 0;
    for (auto pair : subject_degrees) {
      uint64_t bucket = 1; while (bucket <= pair.second / 2) bucket *= 2;
      ++histogram[bucket];
      if (pair.second > heavy_degree) { heavy_id = pair.first; heavy_degree = pair.second; }
    }
    comma = false;
    for (auto pair : histogram) {
      if (comma) out << ',';
      comma = true; out << '"' << pair.first << "\":" << pair.second;
    }
    out << "},\"heaviest_tracked_subject\":" << heavy_id
        << ",\"heaviest_tracked_degree\":" << heavy_degree
        << ",\"read_operations\":" << read_operations << ",\"payload_bytes\":" << payload_bytes
        << ",\"freshness\":\"invalidated_by_local_writes_only; reanalyze_after_external_writes\"}";
    return out.str();
  }
};
}  // namespace yb::mdb

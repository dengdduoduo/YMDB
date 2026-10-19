#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

namespace yb::mdb {

struct BatchJoinOptions {
  bool enabled = true;
  size_t input_rows = 64;
  size_t output_rows = 128;
  size_t batch_bytes = 64 * 1024;
  size_t memory_bytes = 4 * 1024 * 1024;
  size_t max_in_flight = 8;
  size_t negative_cache_entries = 256;
};

struct BatchQueryStats {
  uint64_t read_operations = 0;
  uint64_t flush_rounds = 0;
  uint64_t response_payload_bytes = 0;
  uint64_t decoded_rows = 0;
  uint64_t join_operators = 0;
  uint64_t outer_rows = 0;
  uint64_t unique_probes = 0;
  uint64_t duplicate_rows = 0;
  uint64_t negative_cache_hits = 0;
  uint64_t tablet_groups = 0;
  uint64_t output_rows = 0;
  uint64_t peak_buffered_bytes = 0;
  uint64_t max_in_flight = 0;
  std::string ToJson() const;
};

}  // namespace yb::mdb

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

namespace yb::mdb {

enum class JoinPolicy { Index, Hash, Auto };

struct BatchJoinOptions {
  bool enabled = true;
  JoinPolicy policy = JoinPolicy::Index;
  size_t hash_query_memory_bytes = 64 * 1024 * 1024;
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
  uint64_t hash_join_operators = 0;
  uint64_t hash_build_rows = 0;
  uint64_t hash_probe_rows = 0;
  uint64_t hash_output_rows = 0;
  uint64_t hash_peak_reserved_bytes = 0;
  uint64_t optimizer_hash_choices = 0;
  uint64_t optimizer_index_choices = 0;
  uint64_t optimizer_missing_stats = 0;
  uint64_t hash_build_right_operators = 0;
  uint64_t estimated_hash_build_rows = 0;
  uint64_t estimated_hash_build_bytes = 0;
  std::string join_decisions;
  uint64_t remote_fragments = 0;
  uint64_t remote_max_workers = 0;
  uint64_t remote_batches = 0;
  uint64_t remote_rows = 0;
  uint64_t remote_response_bytes = 0;
  uint64_t remote_peak_frame_bytes = 0;
  uint64_t remote_coordinator_fallbacks = 0;
  std::string remote_worker_pids;
  std::string ToJson() const;
};

}  // namespace yb::mdb

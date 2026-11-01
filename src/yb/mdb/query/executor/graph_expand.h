// Copyright (c) YugabyteDB, Inc.
#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "yb/mdb/storage/docdb_read_context.h"
#include "yb/mdb/storage/docdb_scan_cursor.h"

namespace yb::mdb {

// Physical bindings use persistent RDF IDs; zero denotes an unbound slot.
struct GraphBinding {
  uint64_t origin = 0;
  std::vector<uint64_t> values;
};

enum class GraphDirection { Out, In };

struct GraphExpandSpec {
  GraphDirection direction = GraphDirection::Out;
  uint64_t predicate = 0;
  size_t start_slot = 0;
  size_t neighbor_slot = 1;
  size_t page_rows = 16;
  size_t memory_bytes = 8 * 1024 * 1024;
  size_t probe_batch_size = 8;
};

struct GraphExpandStats {
  uint64_t input_rows = 0;
  uint64_t output_rows = 0;
  uint64_t probes = 0;
  uint64_t shared_inputs = 0;
  uint64_t pages = 0;
  uint64_t read_operations = 0;
  uint64_t flush_rounds = 0;
  size_t max_in_flight = 0;
  size_t reserved_bytes = 0;
};

// One-hop physical operator over a bounded seed batch. The owner keeps tables
// and query context alive and completes any child transaction after draining it.
// IDs are opaque here: typed decoding and RDF ordering remain at the query boundary.
class GraphExpand {
 public:
  GraphExpand(std::array<const client::TableHandle*, 6> tables,
              DocDBReadContext& context, GraphExpandSpec spec)
      : tables_(tables), context_(context), spec_(spec) {}
  ~GraphExpand() { Close(); }
  GraphExpand(const GraphExpand&) = delete;
  GraphExpand& operator=(const GraphExpand&) = delete;

  Status Open(const std::vector<GraphBinding>& input);
  // False is successful exhaustion; execution errors stay sticky until Close/Open.
  // Output ownership transfers to the caller and must fit its downstream budget.
  Result<bool> Next(GraphBinding* output);
  void Close();
  const GraphExpandStats& stats() const { return stats_; }

 private:
  struct Group {
    uint64_t start = 0;
    uint64_t neighbor = 0;
    std::vector<size_t> seeds;
  };
  struct Probe {
    size_t group = 0;
    DocDBScanCursor cursor;
    DocDBScanBatch page;
    size_t edge = 0;
    size_t source = 0;
  };
  Status Check() const;
  Status OpenWave();
  Status RefillPages();
  Result<bool> NextImpl(GraphBinding* output);
  std::array<const client::TableHandle*, 6> tables_;
  DocDBReadContext& context_;
  GraphExpandSpec spec_;
  std::vector<GraphBinding> seeds_;
  std::vector<Group> groups_;
  std::vector<std::unique_ptr<Probe>> wave_;
  size_t next_group_ = 0;
  size_t current_ = 0;
  int permutation_ = 0;
  bool opened_ = false;
  Status failure_;
  GraphExpandStats stats_;
};

}  // namespace yb::mdb

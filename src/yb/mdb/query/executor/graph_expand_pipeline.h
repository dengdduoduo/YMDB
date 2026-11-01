// Copyright (c) YugabyteDB, Inc.
#pragma once

#include <functional>
#include <memory>
#include <vector>

#include "yb/mdb/query/executor/graph_expand.h"

namespace yb::mdb {

struct GraphPipelineOptions {
  size_t seed_batch_rows = 128;
  size_t memory_bytes = 8 * 1024 * 1024;
  // Runs over full persistent-ID bindings before projection. RDF expression
  // evaluation belongs to the owner; errors must be returned, not treated as false.
  std::function<Result<bool>(const GraphBinding&)> filter;
  std::vector<size_t> projection;
};

// Bounded two-hop inner expansion. Full bindings carry compatibility constraints
// between hops; projection is last so hidden join/filter slots remain available.
class GraphExpandPipeline {
 public:
  GraphExpandPipeline(std::array<const client::TableHandle*, 6> tables,
                      DocDBReadContext& context, GraphExpandSpec first,
                      GraphExpandSpec second, GraphPipelineOptions options)
      : tables_(tables), context_(context), first_spec_(first), second_spec_(second),
        options_(std::move(options)) {}
  ~GraphExpandPipeline() { Close(); }
  Status Open(const std::vector<GraphBinding>& input);
  Result<bool> Next(GraphBinding* output);
  void Close();
  size_t peak_seed_rows() const { return peak_seed_rows_; }
  size_t reserved_bytes() const { return reserved_bytes_; }
  uint64_t second_hop_batches() const { return second_hop_batches_; }
  uint64_t read_operations() const {
    return completed_reads_ + (first_ ? first_->stats().read_operations : 0) +
        (second_ ? second_->stats().read_operations : 0);
  }

 private:
  Result<bool> NextImpl(GraphBinding* output);
  std::array<const client::TableHandle*, 6> tables_;
  DocDBReadContext& context_;
  GraphExpandSpec first_spec_, second_spec_;
  GraphPipelineOptions options_;
  std::unique_ptr<GraphExpand> first_, second_;
  std::vector<GraphBinding> pending_;
  size_t peak_seed_rows_ = 0;
  size_t reserved_bytes_ = 0;
  uint64_t second_hop_batches_ = 0, completed_reads_ = 0;
  bool second_open_ = false;
  bool exhausted_ = false;
  Status failure_;
};

}  // namespace yb::mdb

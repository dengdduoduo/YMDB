// Copyright (c) YugabyteDB, Inc.
#include "yb/mdb/query/executor/graph_expand_pipeline.h"

#include <algorithm>
#include <exception>

namespace yb::mdb {

Status GraphExpandPipeline::Open(const std::vector<GraphBinding>& input) {
  Close();
  auto open = [&]() -> Status {
    if (!options_.seed_batch_rows || options_.seed_batch_rows > 256)
      return STATUS(InvalidArgument, "Invalid graph pipeline batch size");
    const size_t width = input.empty() ? 0 : input.front().values.size();
    if (width > 128 || input.size() > 256 || options_.projection.size() > 128)
      return STATUS(InvalidArgument, "Graph pipeline dimensions exceeded");
    if (!input.empty()) {
      if (second_spec_.start_slot >= width || second_spec_.neighbor_slot >= width)
        return STATUS(InvalidArgument, "Invalid second-hop graph slots");
      for (auto slot : options_.projection) if (slot >= width)
        return STATUS(InvalidArgument, "Invalid graph projection slot");
      for (const auto& row : input) {
        if (row.values.size() != width ||
            (second_spec_.start_slot != first_spec_.neighbor_slot &&
             !row.values[second_spec_.start_slot]))
          return STATUS(InvalidArgument, "Second graph hop has no bound start");
      }
    }
    const size_t pending_bytes = 4096 + options_.seed_batch_rows *
        (sizeof(GraphBinding) + width * 8) + width * 16;
    if (pending_bytes >= options_.memory_bytes)
      return STATUS(RuntimeError, "Graph pipeline seed budget exceeded");
    auto first = first_spec_, second = second_spec_;
    first.memory_bytes = std::min(first.memory_bytes,
        (options_.memory_bytes - pending_bytes) / 2);
    second.memory_bytes = std::min(second.memory_bytes,
        (options_.memory_bytes - pending_bytes) / 2);
    first_ = std::make_unique<GraphExpand>(tables_, context_, first);
    second_ = std::make_unique<GraphExpand>(tables_, context_, second);
    RETURN_NOT_OK(first_->Open(input));
    // Validate second-hop predicate, table, direction and page/batch limits
    // before any first-hop output is consumed.
    RETURN_NOT_OK(second_->Open({}));
    second_->Close();
    reserved_bytes_ = pending_bytes + first.memory_bytes + second.memory_bytes;
    pending_.reserve(options_.seed_batch_rows);
    return Status::OK();
  };
  failure_ = open();
  if (!failure_.ok()) { first_.reset(); second_.reset(); }
  return failure_;
}

Result<bool> GraphExpandPipeline::Next(GraphBinding* output) {
  if (!output) return STATUS(InvalidArgument, "Null graph pipeline output");
  *output = {};
  RETURN_NOT_OK(failure_);
  auto result = NextImpl(output);
  if (!result.ok()) {
    failure_ = result.status();
    first_.reset(); second_.reset(); pending_.clear();
    *output = {};
  }
  return result;
}

Result<bool> GraphExpandPipeline::NextImpl(GraphBinding* output) {
  if (!first_ || !second_) return STATUS(IllegalState, "Graph pipeline is closed");
  if (context_.cancelled.load()) return STATUS(Aborted, "Graph pipeline cancelled");
  if (CoarseMonoClock::Now() >= context_.deadline)
    return STATUS(TimedOut, "Graph pipeline deadline expired");
  while (true) {
    if (second_open_) {
      GraphBinding row;
      const auto more = VERIFY_RESULT(second_->Next(&row));
      if (more) {
        if (options_.filter) {
          try {
            if (!VERIFY_RESULT(options_.filter(row))) continue;
          } catch (const std::exception& error) {
            return STATUS(RuntimeError, error.what());
          }
        }
        if (context_.cancelled.load()) return STATUS(Aborted, "Graph pipeline cancelled");
        if (CoarseMonoClock::Now() >= context_.deadline)
          return STATUS(TimedOut, "Graph pipeline deadline expired");
        if (!options_.projection.empty()) {
          output->origin = row.origin;
          for (auto slot : options_.projection) output->values.push_back(row.values[slot]);
        } else {
          *output = std::move(row);
        }
        return true;
      }
      completed_reads_ += second_->stats().read_operations;
      second_->Close(); second_open_ = false;
    }
    if (exhausted_) return false;
    pending_.clear();
    while (pending_.size() < options_.seed_batch_rows) {
      GraphBinding row;
      if (!VERIFY_RESULT(first_->Next(&row))) { exhausted_ = true; break; }
      pending_.push_back(std::move(row));
    }
    if (pending_.empty()) return false;
    peak_seed_rows_ = std::max(peak_seed_rows_, pending_.size());
    RETURN_NOT_OK(second_->Open(pending_));
    // The second hop now owns its bounded seed copy; retain no binding payload twice.
    pending_.clear();
    second_open_ = true;
    ++second_hop_batches_;
  }
}

void GraphExpandPipeline::Close() {
  first_.reset(); second_.reset();
  std::vector<GraphBinding>().swap(pending_);
  peak_seed_rows_ = reserved_bytes_ = 0;
  second_hop_batches_ = completed_reads_ = 0;
  second_open_ = exhausted_ = false;
  failure_ = Status::OK();
}

}  // namespace yb::mdb

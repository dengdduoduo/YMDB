// Copyright (c) YugabyteDB, Inc.
#include "yb/mdb/query/executor/graph_expand_pipeline.h"

#include <algorithm>
#include <exception>
#include <map>

#include "yb/client/table.h"
#include "yb/client/table_handle.h"
#include "yb/common/ql_protocol.pb.h"
#include "yb/dockv/partition.h"
#include "yb/mdb/storage/docdb_key_encoder.h"
#include "yb/util/logging.h"

namespace yb::mdb {

void GraphExpandPipeline::LogSecondHopPlacement() const {
  using Probe = std::pair<uint64_t, uint64_t>;
  std::map<Probe, bool> all_local;
  size_t local_rows = 0;
  size_t unknown_rows = 0;
  const bool same_index = first_spec_.direction == second_spec_.direction;
  const auto index = first_spec_.direction == GraphDirection::Out
      ? DocKeyEncoder::kSPO : DocKeyEncoder::kOPS;
  const auto& table = tables_[index]->table();
  const auto partitions = table->GetVersionedPartitions();
  std::map<uint64_t, PartitionKeyPtr> locations;
  auto location = [&](uint64_t id) -> PartitionKeyPtr {
    auto [it, inserted] = locations.try_emplace(id);
    if (!inserted || !id || !partitions || partitions->keys.empty()) return it->second;
    google::protobuf::RepeatedPtrField<QLExpressionPB> values;
    values.Add()->mutable_value()->set_int64_value(static_cast<int64_t>(id));
    std::string key;
    if (table->partition_schema().EncodeKey(values, &key).ok()) {
      it->second = client::FindPartitionStart(partitions, key);
    }
    return it->second;
  };
  for (const auto& row : pending_) {
    const auto start = row.values[second_spec_.start_slot];
    bool local = false;
    if (same_index) {
      const auto first = location(row.values[first_spec_.start_slot]);
      const auto second = location(start);
      unknown_rows += !first || !second;
      local = first && second && *first == *second;
    }
    local_rows += local;
    auto it = all_local.try_emplace({start, row.values[second_spec_.neighbor_slot]}, true).first;
    it->second &= local;
  }
  const auto local_probes = std::count_if(all_local.begin(), all_local.end(),
      [](const auto& entry) { return entry.second; });
  LOG(INFO) << "MDB_SECOND_HOP_PLACEMENT rows=" << pending_.size()
            << " same_tablet_rows=" << local_rows << " unknown_rows=" << unknown_rows
            << " probes=" << all_local.size() << " fully_local_probes=" << local_probes;
}

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
    // Pair level 2 with async_rpc=3 to see the actual tablet Read RPC membership.
    second_ = std::make_unique<GraphExpand>(
        tables_, context_, second, VLOG_IS_ON(2) ? &first_spec_ : nullptr);
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
      const bool diagnose = VLOG_IS_ON(1);
      bool more;
      if (diagnose) {
        DocDBReadContextScope scope(context_);
        more = VERIFY_RESULT(second_->Next(&row));
      } else {
        more = VERIFY_RESULT(second_->Next(&row));
      }
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
      if (diagnose) {
        LOG(INFO) << "MDB_SECOND_HOP_READ read_operations=" << second_->stats().read_operations
                  << " flush_us=" << context_.stats.storage_flush_us - second_flush_start_us_;
      }
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
    if (VLOG_IS_ON(1)) {
      LogSecondHopPlacement();
      second_flush_start_us_ = context_.stats.storage_flush_us;
    }
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
  second_flush_start_us_ = 0;
  second_open_ = exhausted_ = false;
  failure_ = Status::OK();
}

}  // namespace yb::mdb

// Copyright (c) YugabyteDB, Inc.
#include "yb/mdb/query/executor/graph_expand.h"

#include <algorithm>

#include "yb/client/session.h"
#include "yb/mdb/storage/docdb_key_encoder.h"

namespace yb::mdb {

Status GraphExpand::Check() const {
  if (context_.cancelled.load(std::memory_order_relaxed))
    return STATUS(Aborted, "GraphExpand cancelled");
  if (CoarseMonoClock::Now() >= context_.deadline)
    return STATUS(TimedOut, "GraphExpand deadline expired");
  if (!context_.session) return STATUS(IllegalState, "GraphExpand has no query session");
  if (context_.session->IsRestartRequired())
    return STATUS(TryAgain, "GraphExpand read view requires whole-query restart");
  return Status::OK();
}

Status GraphExpand::Open(const std::vector<GraphBinding>& input) {
  Close();
  auto open = [&]() -> Status {
    RETURN_NOT_OK(Check());
    if (spec_.direction != GraphDirection::Out && spec_.direction != GraphDirection::In)
      return STATUS(InvalidArgument, "Invalid GraphExpand direction");
    permutation_ = spec_.direction == GraphDirection::Out
        ? DocKeyEncoder::kSPO : DocKeyEncoder::kOPS;
    if (!tables_[permutation_] || !spec_.predicate || !spec_.page_rows ||
        spec_.page_rows > 256 || input.size() > 256 || !spec_.probe_batch_size ||
        spec_.probe_batch_size > 16)
      return STATUS(InvalidArgument, "Invalid GraphExpand predicate, table or dimensions");
    const size_t width = input.empty() ? 0 : input.front().values.size();
    if (!input.empty() && (!width || width > 128 || spec_.start_slot >= width ||
                           spec_.neighbor_slot >= width))
      return STATUS(InvalidArgument, "Invalid GraphExpand binding slots");
    for (const auto& row : input) {
      if (row.values.size() != width || !row.values[spec_.start_slot])
        return STATUS(InvalidArgument, "GraphExpand requires uniform bindings and bound starts");
    }
    // Reserve worst-case groups and active pages before allocating. Shared
    // client/transaction infrastructure and downstream-retained output are
    // separate budgets, as with existing DocDB operators.
    const size_t active = std::min(input.size(), spec_.probe_batch_size);
    const size_t reservation = 4096 + input.size() *
        (sizeof(GraphBinding) + width * 8 + sizeof(Group) + 64) + width * 8 +
        active * (sizeof(Probe) + 65536 + spec_.page_rows * 512);
    if (reservation > spec_.memory_bytes)
      return STATUS(RuntimeError, "GraphExpand buffered-state budget exceeded");
    stats_.reserved_bytes = reservation;
    stats_.input_rows = input.size();
    seeds_.reserve(input.size());
    groups_.reserve(input.size());
    for (const auto& row : input) {
      const auto start = row.values[spec_.start_slot];
      const auto neighbor = row.values[spec_.neighbor_slot];
      auto group = std::find_if(groups_.begin(), groups_.end(), [&](const Group& item) {
        return item.start == start && item.neighbor == neighbor;
      });
      if (group == groups_.end()) {
        groups_.push_back({start, neighbor, {}});
        group = groups_.end() - 1;
      }
      group->seeds.push_back(seeds_.size());
      seeds_.push_back(row);
    }
    stats_.shared_inputs = input.size() - groups_.size();
    opened_ = true;
    return Status::OK();
  };
  failure_ = open();
  return failure_;
}

Status GraphExpand::OpenWave() {
  wave_.clear();
  current_ = 0;
  while (next_group_ < groups_.size() && wave_.size() < spec_.probe_batch_size) {
    const auto& group = groups_[next_group_];
    auto probe = std::make_unique<Probe>();
    probe->group = next_group_++;
    DocDBScanSpec scan;
    scan.hash_key = group.start;
    scan.columns[0] = {group.start, group.start};
    scan.columns[1] = {spec_.predicate, spec_.predicate};
    if (group.neighbor) scan.columns[2] = {group.neighbor, group.neighbor};
    scan.page_rows = spec_.page_rows;
    scan.page_bytes = spec_.page_rows * sizeof(DocDBScanBatch::Row);
    scan.deadline = context_.deadline;
    scan.cancelled = &context_.cancelled;
    scan.push_exact_filters = true;
    RETURN_NOT_OK(probe->cursor.Open(tables_[permutation_], context_.session, scan));
    wave_.push_back(std::move(probe));
    ++stats_.probes;
  }
  stats_.max_in_flight = std::max(stats_.max_in_flight, wave_.size());
  return Status::OK();
}

Status GraphExpand::RefillPages() {
  std::vector<DocDBScanCursor*> cursors;
  std::vector<Probe*> probes;
  uint64_t before = 0;
  for (auto& probe : wave_) {
    if (probe->edge != probe->page.rows.size() || probe->page.eof) continue;
    probe->page = {};
    probe->edge = 0;
    before += probe->cursor.rpc_calls();
    cursors.push_back(&probe->cursor);
    probes.push_back(probe.get());
  }
  auto pages = DocDBScanCursor::NextBatches(cursors);
  uint64_t after = 0;
  for (auto* cursor : cursors) after += cursor->rpc_calls();
  stats_.read_operations += after - before;
  if (after != before) ++stats_.flush_rounds;
  if (!pages.ok()) return pages.status();
  for (size_t i = 0; i < probes.size(); ++i) {
    probes[i]->page = std::move((*pages)[i]);
    ++stats_.pages;
    if (probes[i]->cursor.retained_payload_bytes() > 65536)
      return STATUS(RuntimeError, "GraphExpand cursor payload budget exceeded");
  }
  return Status::OK();
}

Result<bool> GraphExpand::Next(GraphBinding* output) {
  if (!output) return STATUS(InvalidArgument, "Null GraphExpand output");
  *output = {};
  RETURN_NOT_OK(failure_);
  auto result = NextImpl(output);
  if (!result.ok()) {
    failure_ = result.status();
    wave_.clear();
    *output = {};
  }
  return result;
}

Result<bool> GraphExpand::NextImpl(GraphBinding* output) {
  if (!opened_) return STATUS(IllegalState, "GraphExpand is closed");
  RETURN_NOT_OK(Check());
  while (true) {
    RETURN_NOT_OK(Check());
    if (current_ == wave_.size()) {
      if (next_group_ == groups_.size()) { wave_.clear(); current_ = 0; return false; }
      RETURN_NOT_OK(OpenWave());
    }
    auto& probe = *wave_[current_];
    if (probe.edge == probe.page.rows.size()) {
      if (probe.page.eof) { ++current_; continue; }
      RETURN_NOT_OK(RefillPages());
      if (probe.page.rows.empty()) continue;
    }
    const auto& group = groups_[probe.group];
    const auto& stored = probe.page.rows[probe.edge];
    if (!stored[0] || !stored[1] || !stored[2])
      return STATUS(Corruption, "GraphExpand read a zero persistent RDF ID");
    if (stored[0] != group.start || stored[1] != spec_.predicate)
      return STATUS(Corruption, "GraphExpand cursor returned an incompatible edge");
    const auto& input = seeds_[group.seeds[probe.source++]];
    if (probe.source == group.seeds.size()) { probe.source = 0; ++probe.edge; }
    const auto bound = input.values[spec_.neighbor_slot];
    if (bound && bound != stored[2]) continue;
    *output = input;
    output->values[spec_.neighbor_slot] = stored[2];
    ++stats_.output_rows;
    return true;
  }
}

void GraphExpand::Close() {
  std::vector<std::unique_ptr<Probe>>().swap(wave_);
  std::vector<Group>().swap(groups_);
  std::vector<GraphBinding>().swap(seeds_);
  next_group_ = current_ = 0;
  opened_ = false;
  failure_ = Status::OK();
  stats_ = {};
}

}  // namespace yb::mdb

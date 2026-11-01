// Copyright (c) YugabyteDB, Inc.
#include "yb/mdb/query/distributed/graph_fragment.h"

#include <set>

#include "yb/client/session.h"
#include "yb/client/yb_op.h"
#include "yb/client/table_handle.h"
#include "yb/common/ql_protocol_util.h"
#include "yb/mdb/storage/yb_client_compat.h"

namespace yb::mdb {
namespace {
bool Bound(const FragmentTermPB& term, const std::set<uint32_t>& bound) {
  return term.has_constant() || bound.count(term.variable());
}
}  // namespace

Status ValidateGraphProgram(const FragmentOpenPB& open) {
  for (const auto& triple : open.triples()) {
    if (triple.terms_size() != 3) return STATUS(InvalidArgument, "Invalid graph triple arity");
    for (const auto& term : triple.terms()) {
      if (term.has_variable() == term.has_constant() ||
          (term.has_variable() && term.variable() >= open.variable_count()) ||
          (term.has_constant() && !term.constant()))
        return STATUS(InvalidArgument, "Invalid graph triple term");
    }
  }
  if (open.graph_version() != 1 || open.variable_count() > 124 ||
      open.triples_size() < 1 || open.triples_size() > (open.graph_seeded() ? 2 : 3) ||
      (!open.graph_seeded() && open.triples_size() < 2) || open.graph_seed_rows() > 256 ||
      uint64_t(open.graph_seed_values_size()) !=
          uint64_t(open.graph_seed_rows()) * open.variable_count() ||
      (!open.graph_seeded() && (open.graph_seed_rows() || open.graph_seed_values_size())))
    return STATUS(NotSupported, "Unsupported graph fragment dimensions or version");
  std::set<uint32_t> bound;
  if (open.graph_seeded()) {
    for (uint32_t col = 0; col < open.variable_count(); ++col) {
      bool every = open.graph_seed_rows() != 0;
      for (uint32_t row = 0; row < open.graph_seed_rows(); ++row)
        every &= open.graph_seed_values(row * open.variable_count() + col) != 0;
      if (every) bound.insert(col);
    }
  } else {
    for (const auto& term : open.triples(0).terms())
      if (term.has_variable()) bound.insert(term.variable());
  }
  for (int i = 0; i < open.triples_size(); ++i) {
    const auto& triple = open.triples(i);
    if (!triple.terms(1).has_constant())
      return STATUS(NotSupported, "Graph expansion requires a fixed predicate");
    if (!open.graph_seeded() && i == 0) continue;
    // Zero VALUES rows is a valid empty relation; no expansion will execute.
    if (open.graph_seeded() && !open.graph_seed_rows()) continue;
    if (!Bound(triple.terms(0), bound) && !Bound(triple.terms(2), bound))
      return STATUS(NotSupported, "Graph expansion has no bound endpoint");
    for (const auto& term : triple.terms()) if (term.has_variable()) bound.insert(term.variable());
  }
  return Status::OK();
}

std::vector<GraphExpandSpec> GraphFragment::Specs(GraphBinding* seed) const {
  std::vector<GraphExpandSpec> result;
  std::set<size_t> bound;
  for (size_t i = 0; i < seed->values.size(); ++i) {
    if (seed->values[i]) bound.insert(i);
  }
  size_t scratch = open_.variable_count();
  for (int i = open_.graph_seeded() ? 0 : 1; i < open_.triples_size(); ++i) {
    const auto& triple = open_.triples(i);
    auto slot = [&](const FragmentTermPB& term) -> size_t {
      if (term.has_variable()) return term.variable();
      const auto index = scratch++;
      seed->values[index] = term.constant(); bound.insert(index);
      return index;
    };
    const auto subject = slot(triple.terms(0)), object = slot(triple.terms(2));
    GraphExpandSpec spec;
    spec.predicate = triple.terms(1).constant();
    spec.direction = bound.count(subject) ? GraphDirection::Out : GraphDirection::In;
    spec.start_slot = spec.direction == GraphDirection::Out ? subject : object;
    spec.neighbor_slot = spec.direction == GraphDirection::Out ? object : subject;
    spec.page_rows = open_.page_rows();
    spec.memory_bytes = open_.memory_bytes() - FragmentBufferedReservation(open_);
    result.push_back(spec); bound.insert(subject); bound.insert(object);
  }
  return result;
}

Status GraphFragment::Open(const FragmentOpenPB& open,
    std::array<const client::TableHandle*, 6> tables, client::YBSessionPtr session,
    CoarseTimePoint deadline) {
  RETURN_NOT_OK(ValidateGraphProgram(open));
  open_ = open; tables_ = tables;
  context_ = DocDBReadContext::BorrowChildSession(std::move(session));
  context_->deadline = deadline;
  if (!open.graph_seeded()) {
    DocDBScanSpec spec;
    spec.partition_start = open.partition_start(); spec.partition_end = open.partition_end();
    spec.page_rows = open.page_rows(); spec.page_bytes = open.page_rows() * 24;
    spec.deadline = deadline; spec.cancelled = &context_->cancelled; spec.push_exact_filters = true;
    for (int i = 0; i < 3; ++i) if (open.triples(0).terms(i).has_constant()) {
      auto id = open.triples(0).terms(i).constant(); spec.columns[i] = {id, id};
      if (i == 0) spec.hash_key = id;
    }
    RETURN_NOT_OK(anchor_.Open(tables_[0], context_->session, spec));
  }
  return Status::OK();
}

Result<bool> GraphFragment::NextSeed(GraphBinding* seed) {
  while (true) {
    seed->origin = origin_++;
    seed->values.assign(open_.variable_count() + 4, 0);
    if (open_.graph_seeded()) {
      if (seed_position_ == open_.graph_seed_rows()) return false;
      for (uint32_t col = 0; col < open_.variable_count(); ++col)
        seed->values[col] = open_.graph_seed_values(seed_position_ * open_.variable_count() + col);
      ++seed_position_;
      const auto specs = Specs(seed);
      const int perm = specs.front().direction == GraphDirection::Out ? 0 : 5;
      auto read = NewMdbReadOp(*tables_[perm], context_->session);
      QLAddInt64HashValue(read->mutable_request(),
          static_cast<int64_t>(seed->values[specs.front().start_slot]));
      std::string key; RETURN_NOT_OK(read->GetPartitionKey(&key));
      if (key < open_.partition_start() ||
          (!open_.partition_end().empty() && key >= open_.partition_end())) continue;
      return true;
    }
    if (position_ == page_.rows.size()) {
      if (page_.eof) return false;
      page_ = {}; auto next = VERIFY_RESULT(anchor_.NextBatch());
      page_ = std::move(next); position_ = 0;
      if (page_.rows.empty()) continue;
    }
    const auto& stored = page_.rows[position_++];
    bool compatible = true;
    for (int i = 0; i < 3; ++i) {
      if (!stored[i]) return STATUS(Corruption, "Zero graph anchor term ID");
      const auto& term = open_.triples(0).terms(i);
      if (term.has_constant()) {
        compatible &= term.constant() == stored[i];
      } else {
        auto& value = seed->values[term.variable()];
        compatible &= !value || value == stored[i]; value = stored[i];
      }
    }
    if (compatible) { Specs(seed); return true; }
  }
}

Result<bool> GraphFragment::Next(GraphBinding* output) {
  while (true) {
    if (expand_ || pipeline_) {
      const bool more = expand_ ? VERIFY_RESULT(expand_->Next(output))
                                : VERIFY_RESULT(pipeline_->Next(output));
      if (more) return true;
      completed_reads_ += expand_ ? expand_->stats().read_operations : pipeline_->read_operations();
      expand_.reset(); pipeline_.reset();
    }
    std::vector<GraphBinding> seeds;
    std::vector<GraphExpandSpec> specs;
    for (size_t i = 0; i < 128; ++i) {
      GraphBinding seed;
      if (!VERIFY_RESULT(NextSeed(&seed))) break;
      if (specs.empty()) specs = Specs(&seed);
      seeds.push_back(std::move(seed));
    }
    if (seeds.empty()) { *output = {}; return false; }
    if (specs.size() == 1) {
      expand_ = std::make_unique<GraphExpand>(tables_, *context_, specs[0]);
      RETURN_NOT_OK(expand_->Open(seeds));
    } else {
      GraphPipelineOptions options; options.memory_bytes = specs[0].memory_bytes;
      pipeline_ = std::make_unique<GraphExpandPipeline>(
          tables_, *context_, specs[0], specs[1], options);
      RETURN_NOT_OK(pipeline_->Open(seeds));
    }
  }
}

uint64_t GraphFragment::read_operations() const {
  return anchor_.rpc_calls() + completed_reads_ + (expand_ ? expand_->stats().read_operations : 0) +
      (pipeline_ ? pipeline_->read_operations() : 0);
}
}  // namespace yb::mdb

#include "query/executor/batch_index_join.h"

#include <algorithm>
#include <exception>
#include <map>
#include <utility>

#include "query/executor/binding_iter/index_nested_loop_join.h"
#include "yb/client/table.h"
#include "yb/client/table_handle.h"
#include "yb/client/yb_op.h"
#include "yb/common/ql_protocol_util.h"
#include "yb/mdb/storage/yb_client_compat.h"

namespace yb::mdb {

BatchIndexJoin::BatchIndexJoin(std::unique_ptr<BindingIter> lhs, std::unique_ptr<DocDBScan> rhs)
    : rhs_(std::move(rhs)), context_(rhs_->read_context()), options_(context_->batch_options),
      source_(std::move(lhs), options_.input_rows, options_.batch_bytes) {}

Status BatchIndexJoin::Check() const {
  if (!work_) return STATUS(IllegalState, "MDB batch join is closed");
  RETURN_NOT_OK(failure_);
  if (context_->cancelled.load()) return STATUS(Aborted, "MDB batch join cancelled");
  if (CoarseMonoClock::Now() >= context_->deadline) return STATUS(TimedOut, "MDB batch join deadline exceeded");
  return Status::OK();
}

Status BatchIndexJoin::Open(const Binding& input) {
  Close();
  if (!options_.input_rows || !options_.output_rows || !options_.max_in_flight ||
      options_.max_in_flight > 256 || options_.memory_bytes < 4096) {
    return STATUS(InvalidArgument, "Invalid MDB batch join row/concurrency/memory budget");
  }
  max_input_rows_ = std::min(options_.input_rows, options_.memory_bytes / (8 * sizeof(Group) + 256));
  max_in_flight_ = std::min(options_.max_in_flight, options_.memory_bytes / 8192);
  negative_limit_ = std::min(options_.negative_cache_entries, options_.memory_bytes / (16 * sizeof(Key)));
  if (!max_input_rows_ || !max_in_flight_) return STATUS(InvalidArgument, "MDB join memory budget too small");
  source_.SetLimits(max_input_rows_, std::min(options_.batch_bytes, options_.memory_bytes / 4));
  work_ = std::make_unique<Binding>(input.size);
  work_->add_all(input);
  RETURN_NOT_OK(source_.Open(input));
  ++context_->stats.join_operators;
  return CheckMemory();
}

size_t BatchIndexJoin::MemoryBytes() const {
  size_t bytes = sizeof(*this) + outer_.Bytes() + output_bytes_ +
      (work_ ? work_->size * sizeof(::ObjectId) * 2 : 0) +
      groups_.capacity() * sizeof(Group) + wave_.capacity() * sizeof(Probe) +
      negative_.size() * (sizeof(Key) + 4 * sizeof(void*));
  for (const auto& group : groups_) bytes += group.rows.capacity() * sizeof(size_t) + group.partition.capacity();
  for (const auto& probe : wave_) {
    bytes += probe.page.rows.capacity() * sizeof(DocDBScanBatch::Row);
    if (probe.cursor) bytes += sizeof(DocDBScanCursor) + 2048 + probe.cursor->retained_payload_bytes();
  }
  return bytes;
}

Status BatchIndexJoin::CheckMemory() {
  auto bytes = MemoryBytes();
  context_->stats.peak_buffered_bytes = std::max<uint64_t>(context_->stats.peak_buffered_bytes, bytes);
  if (bytes > options_.memory_bytes) return STATUS(RuntimeError, "MDB batch join memory budget exceeded");
  return Status::OK();
}

Status BatchIndexJoin::BuildGroups() {
  groups_.clear();
  next_group_ = 0;
  std::map<Key, size_t> positions;
  groups_.reserve(outer_.rows());
  for (size_t row = 0; row < outer_.rows(); ++row) {
    RETURN_NOT_OK(outer_.CopyRow(row, work_.get()));
    auto spec = VERIFY_RESULT(rhs_->ProbeSpec(*work_));
    ++context_->stats.outer_rows;
    if (!spec) continue;
    if (!spec->hash_key) return STATUS(NotSupported, "Batch index join needs a bound hash key");
    Key key;
    for (int col = 0; col < 3; ++col) {
      key[col * 2] = spec->columns[col].min;
      key[col * 2 + 1] = spec->columns[col].max;
    }
    if (negative_.count(key)) { ++context_->stats.negative_cache_hits; continue; }
    auto found = positions.find(key);
    if (found != positions.end()) {
      groups_[found->second].rows.push_back(row);
      ++context_->stats.duplicate_rows;
      continue;
    }
    auto op = NewMdbReadOp(*rhs_->table(), context_->session);
    QLAddInt64HashValue(op->mutable_request(), static_cast<int64_t>(*spec->hash_key));
    std::string partition_key;
    RETURN_NOT_OK(op->GetPartitionKey(&partition_key));
    auto start = rhs_->table()->table()->FindPartitionStart(partition_key);
    if (!start) return STATUS(Corruption, "Missing MDB batch probe partition");
    positions.emplace(key, groups_.size());
    groups_.push_back(Group{key, std::move(*spec), *start, {row}});
  }
  std::sort(groups_.begin(), groups_.end(), [](const Group& left, const Group& right) {
    return left.partition < right.partition ||
           (left.partition == right.partition && left.key < right.key);
  });
  for (size_t i = 0; i < groups_.size(); ++i) {
    if (!i || groups_[i].partition != groups_[i-1].partition) ++context_->stats.tablet_groups;
  }
  return CheckMemory();
}

Result<bool> BatchIndexJoin::Refill() {
  RETURN_NOT_OK(Check());
  std::vector<DocDBScanCursor*> cursors;
  std::vector<size_t> indexes;
  for (size_t i = 0; i < wave_.size(); ++i) {
    auto& probe = wave_[i];
    if (!probe.cursor) continue;
    if (probe.page.eof) {
      if (!probe.matched && negative_.size() < negative_limit_) negative_.insert(probe.group->key);
      probe.cursor.reset();
      probe.page = DocDBScanBatch();
    } else {
      probe.page = DocDBScanBatch();
      probe.row = probe.outer = 0;
      cursors.push_back(probe.cursor.get());
      indexes.push_back(i);
    }
  }
  if (cursors.empty()) {
    wave_.clear();
    while (next_group_ == groups_.size()) {
      if (outer_eof_) return false;
      outer_ = BindingBatch();
      auto input = VERIFY_RESULT(source_.NextBatch());
      outer_eof_ = input.eof;
      outer_ = std::move(input);
      RETURN_NOT_OK(BuildGroups());
    }
    const auto count = std::min(max_in_flight_, groups_.size() - next_group_);
    // Allow decoded rows, their retained sidecars, and pending protobuf state.
    auto available = options_.memory_bytes > MemoryBytes() ? options_.memory_bytes - MemoryBytes() : 0;
    auto rows = std::min(context_->page_rows, available / (count * 256));
    if (!rows) return STATUS(RuntimeError, "MDB batch probe page exceeds join memory budget");
    wave_.reserve(count);
    for (size_t i = 0; i < count; ++i) {
      auto& group = groups_[next_group_++];
      auto spec = group.spec;
      spec.page_rows = rows;
      // Filtering is a storage optimization only; decoded ranges and repeated
      // variable constraints are checked again before emitting bindings.
      spec.push_exact_filters = true;
      Probe probe;
      probe.group = &group;
      probe.cursor = std::make_unique<DocDBScanCursor>();
      RETURN_NOT_OK(probe.cursor->Open(rhs_->table(), context_->session, std::move(spec)));
      wave_.push_back(std::move(probe));
      cursors.push_back(wave_.back().cursor.get());
      indexes.push_back(i);
      ++context_->stats.unique_probes;
    }
    context_->stats.max_in_flight = std::max<uint64_t>(context_->stats.max_in_flight, count);
  }
  uint64_t operations = 0, bytes = 0, decoded = 0;
  for (auto* cursor : cursors) {
    operations += cursor->rpc_calls(); bytes += cursor->payload_bytes(); decoded += cursor->decoded_rows();
  }
  auto pages = DocDBScanCursor::NextBatches(cursors);
  uint64_t after = 0, after_bytes = 0, after_decoded = 0;
  for (auto* cursor : cursors) {
    after += cursor->rpc_calls(); after_bytes += cursor->payload_bytes(); after_decoded += cursor->decoded_rows();
  }
  context_->stats.read_operations += after - operations;
  context_->scan_rpc_calls += after - operations;
  context_->stats.flush_rounds += after != operations;
  context_->stats.response_payload_bytes += after_bytes - bytes;
  context_->stats.decoded_rows += after_decoded - decoded;
  if (!pages.ok()) return pages.status();
  for (size_t i = 0; i < cursors.size(); ++i) wave_[indexes[i]].page = std::move((*pages)[i]);
  wave_position_ = 0;
  RETURN_NOT_OK(CheckMemory());
  return true;
}

Result<bool> BatchIndexJoin::NextRow(uint64_t* origin) {
  for (;;) {
    RETURN_NOT_OK(Check());
    if (wave_position_ == wave_.size()) {
      if (!VERIFY_RESULT(Refill())) return false;
    }
    auto& probe = wave_[wave_position_];
    if (!probe.cursor || probe.row == probe.page.rows.size()) { ++wave_position_; continue; }
    const auto& row = probe.page.rows[probe.row];
    if (!rhs_->Matches(row)) { ++probe.row; continue; }
    const auto outer_row = probe.group->rows[probe.outer];
    RETURN_NOT_OK(outer_.CopyRow(outer_row, work_.get()));
    RETURN_NOT_OK(rhs_->DecodeRow(row, work_.get()));
    probe.matched = true;
    *origin = outer_.Origin(outer_row);
    if (++probe.outer == probe.group->rows.size()) { probe.outer = 0; ++probe.row; }
    return true;
  }
}

Result<BindingBatch> BatchIndexJoin::NextBatch() {
  RETURN_NOT_OK(Check());
  auto execute = [&]() -> Result<BindingBatch> {
    BindingBatch output;
    RETURN_NOT_OK(output.Init(BindingBatch::DenseLayout(work_->size), options_.output_rows,
                             std::min(options_.batch_bytes, options_.memory_bytes / 4), true));
    output_bytes_ = output.Bytes();
    RETURN_NOT_OK(CheckMemory());
    while (!output.full()) {
      uint64_t origin;
      if (!VERIFY_RESULT(NextRow(&origin))) { output.eof = true; break; }
      RETURN_NOT_OK(output.Append(*work_, origin));
      ++context_->stats.output_rows;
    }
    return output;
  };
  auto result = execute();
  if (!result.ok()) failure_ = result.status();
  return result;
}

void BatchIndexJoin::Close() {
  wave_.clear(); groups_.clear(); negative_.clear();
  outer_ = BindingBatch(); work_.reset(); source_.Close();
  next_group_ = wave_position_ = output_bytes_ = 0;
  outer_eof_ = false; failure_ = Status::OK();
}

std::unique_ptr<BindingIter> MakeIndexJoin(
    std::unique_ptr<BindingIter> lhs, std::unique_ptr<BindingIter> rhs,
    std::vector<VarId> output_vars) {
  auto* scan = dynamic_cast<DocDBScan*>(rhs.get());
  if (scan && scan->CanBatchBoundJoin() && scan->read_context()->batch_options.enabled) {
    auto owned = std::unique_ptr<DocDBScan>(static_cast<DocDBScan*>(rhs.release()));
    return std::make_unique<BatchBindingIter>(
        std::make_unique<BatchIndexJoin>(std::move(lhs), std::move(owned)), std::move(output_vars));
  }
  return std::make_unique<IndexNestedLoopJoin>(std::move(lhs), std::move(rhs));
}

}  // namespace yb::mdb

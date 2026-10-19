#include "query/executor/hash_join.h"

#include <algorithm>
#include <exception>
#include "yb/mdb/storage/docdb_read_context.h"

namespace yb::mdb {
HashJoin::HashJoin(std::unique_ptr<BindingIter> build, std::unique_ptr<BindingIter> probe,
                   std::vector<VarId> keys, std::vector<VarId> build_vars,
                   DocDBReadContext* context, BatchJoinOptions options)
    : build_(std::move(build), options.input_rows, options.batch_bytes),
      probe_(std::move(probe), options.input_rows, options.batch_bytes),
      keys_(std::move(keys)), build_vars_(std::move(build_vars)),
      context_(context), options_(options) {}

Status HashJoin::Check() const {
  if (context_) {
    if (context_->cancelled.load()) return STATUS(Aborted, "MDB hash join cancelled");
    if (CoarseMonoClock::Now() >= context_->deadline)
      return STATUS(TimedOut, "MDB hash join deadline exceeded");
  }
  return Status::OK();
}

Status HashJoin::Open(const Binding& input) {
  Close();
  RETURN_NOT_OK(Check());
  if (keys_.empty() || !options_.input_rows || !options_.output_rows)
    return STATUS(InvalidArgument, "Hash join requires bound keys and positive row limits");
  width_ = input.size;
  for (auto var : keys_) if (var.id >= width_)
    return STATUS(InvalidArgument, "Hash join key outside binding");
  for (auto var : build_vars_) if (var.id >= width_)
    return STATUS(InvalidArgument, "Hash join output outside binding");
  // Reserve before allocating. Includes batches, bindings, bucket/row arrays,
  // and bounded child scan page allowances. Shared dictionaries, RPC arenas
  // and unrelated legacy operators are outside this buffered-state budget.
  if (width_ > options_.memory_bytes / 64 ||
      options_.batch_bytes > options_.memory_bytes / 4)
    return STATUS(RuntimeError, "MDB hash join memory budget too small");
  size_t fixed = sizeof(*this) + 4 * options_.batch_bytes + 64 * width_ + 4096;
  if (context_) {
    if (context_->page_bytes > options_.memory_bytes / 8 ||
        context_->page_rows > options_.memory_bytes / 192)
      return STATUS(RuntimeError, "MDB hash scan page exceeds memory budget");
    fixed += 2 * (context_->page_bytes + context_->page_rows * 24 + 1024);
  }
  key_columns_.clear();
  for (auto var : keys_) {
    const auto found = std::find(build_vars_.begin(), build_vars_.end(), var);
    if (found == build_vars_.end()) return STATUS(InvalidArgument, "Hash key absent from build layout");
    key_columns_.push_back(found - build_vars_.begin());
  }
  stride_ = build_vars_.size() + 1;
  const size_t unit = stride_ * sizeof(uint64_t) + 2 * sizeof(size_t);
  if (fixed >= options_.memory_bytes || options_.memory_bytes - fixed < unit * 2)
    return STATUS(RuntimeError, "MDB hash join memory budget too small");
  capacity_ = 1;
  const size_t maximum = (options_.memory_bytes - fixed) / unit;
  while (capacity_ <= maximum / 2) capacity_ *= 2;
  const size_t required = fixed + capacity_ * unit;
  if (context_) {
    if (context_->hash_reserved_bytes > options_.hash_query_memory_bytes ||
        required > options_.hash_query_memory_bytes - context_->hash_reserved_bytes)
      return STATUS(RuntimeError, "MDB query hash join memory budget exceeded");
    context_->hash_reserved_bytes += required;
    context_->stats.hash_peak_reserved_bytes = std::max<uint64_t>(
        context_->stats.hash_peak_reserved_bytes, context_->hash_reserved_bytes);
    ++context_->stats.hash_join_operators;
  }
  reservation_ = required;
  try {
    rows_.resize(capacity_ * stride_);
    buckets_.assign(capacity_ * 2, SIZE_MAX);
    work_ = std::make_unique<Binding>(width_);
    work_->add_all(input);
    RETURN_NOT_OK(build_.Open(input));
    RETURN_NOT_OK(probe_.Open(input));
  } catch (const std::exception& e) {
    failure_ = STATUS(RuntimeError, e.what());
    return failure_;
  }
  return Status::OK();
}

Result<bool> HashJoin::ReadRow(BindingIterBatchSource* source, BindingBatch* batch,
                              size_t* position) {
  RETURN_NOT_OK(Check());
  while (*position == batch->rows()) {
    if (batch->eof) return false;
    *batch = BindingBatch();
    *batch = VERIFY_RESULT(source->NextBatch());
    *position = 0;
  }
  RETURN_NOT_OK(batch->CopyRow((*position)++, work_.get()));
  return true;
}

Result<size_t> HashJoin::Bucket() const {
  uint64_t hash = 1469598103934665603ULL;
  for (auto var : keys_) {
    const auto value = (*work_)[var];
    if (value.is_null()) return STATUS(NotSupported, "Hash join key is unbound");
    hash = (hash ^ value.id) * 1099511628211ULL;
  }
  return hash & (buckets_.size() - 1);
}

bool HashJoin::Matches(size_t index) const {
  for (size_t i = 0; i < keys_.size(); ++i)
    if (rows_[index * stride_ + key_columns_[i]] != (*work_)[keys_[i]].id) return false;
  return true;
}

Status HashJoin::Build() {
  while (VERIFY_RESULT(ReadRow(&build_, &build_batch_, &build_position_))) {
    if (count_ == capacity_)
      return STATUS(RuntimeError, "MDB hash build memory limit exceeded; spill unsupported");
    const auto bucket = VERIFY_RESULT(Bucket());
    for (size_t i = 0; i < build_vars_.size(); ++i)
      rows_[count_ * stride_ + i] = (*work_)[build_vars_[i]].id;
    rows_[count_ * stride_ + build_vars_.size()] = buckets_[bucket];
    buckets_[bucket] = count_++;
    if (context_) ++context_->stats.hash_build_rows;
  }
  build_batch_ = BindingBatch();
  built_ = true;
  return Status::OK();
}

Result<BindingBatch> HashJoin::NextBatch() {
  if (!work_) return STATUS(IllegalState, "Hash join is closed");
  RETURN_NOT_OK(failure_);
  auto next = [&]() -> Result<BindingBatch> {
    RETURN_NOT_OK(Check());
    if (!built_) RETURN_NOT_OK(Build());
    BindingBatch output;
    RETURN_NOT_OK(output.Init(BindingBatch::DenseLayout(width_), options_.output_rows,
                             options_.batch_bytes));
    while (!output.full()) {
      RETURN_NOT_OK(Check());
      if (match_ == SIZE_MAX) {
        if (!VERIFY_RESULT(ReadRow(&probe_, &probe_batch_, &probe_position_))) {
          output.eof = true;
          break;
        }
        if (context_) ++context_->stats.hash_probe_rows;
        match_ = buckets_[VERIFY_RESULT(Bucket())];
      }
      // Retain the probe row across output batches and every duplicate match.
      while (match_ != SIZE_MAX) {
        const auto row = match_;
        match_ = rows_[row * stride_ + build_vars_.size()];
        if (!Matches(row)) continue;
        for (size_t i = 0; i < build_vars_.size(); ++i)
          work_->add(build_vars_[i], ::ObjectId(rows_[row * stride_ + i]));
        RETURN_NOT_OK(output.Append(*work_));
        if (context_) ++context_->stats.hash_output_rows;
        if (output.full()) break;
      }
    }
    return output;
  };
  auto result = next();
  if (!result.ok()) failure_ = result.status();
  return result;
}

void HashJoin::Close() {
  build_.Close(); probe_.Close();
  build_batch_ = BindingBatch(); probe_batch_ = BindingBatch(); work_.reset();
  std::vector<uint64_t>().swap(rows_); std::vector<size_t>().swap(buckets_);
  if (context_ && reservation_) context_->hash_reserved_bytes -= reservation_;
  reservation_ = 0; count_ = 0; build_position_ = 0; probe_position_ = 0;
  match_ = SIZE_MAX; built_ = false; failure_ = Status::OK();
}
}  // namespace yb::mdb

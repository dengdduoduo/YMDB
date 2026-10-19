#pragma once

#include <algorithm>
#include <cstdint>
#include <limits>
#include <vector>

#include "query/executor/binding.h"
#include "yb/util/status.h"

namespace yb::mdb {

// Fixed column layout, column-major IDs, and a packed bound/unbound bitmap.
// The budget counts this object's buffers, not external term/dictionary storage.
class BindingBatch {
 public:
  BindingBatch() = default;
  BindingBatch(BindingBatch&&) = default;
  BindingBatch& operator=(BindingBatch&&) = default;
  BindingBatch(const BindingBatch&) = delete;
  BindingBatch& operator=(const BindingBatch&) = delete;

  Status Init(std::vector<VarId> layout, size_t max_rows, size_t max_bytes,
              bool track_origin = false) {
    *this = BindingBatch();
    if (!max_rows) return STATUS(InvalidArgument, "Zero binding batch row budget");
    for (size_t i = 0; i < layout.size(); ++i) {
      for (size_t j = 0; j < i; ++j) {
        if (layout[i] == layout[j]) return STATUS(InvalidArgument, "Duplicate batch column");
      }
    }
    layout_ = std::move(layout);
    const size_t fixed = sizeof(*this) + layout_.capacity() * sizeof(VarId);
    if (max_bytes <= fixed) return STATUS(InvalidArgument, "Binding batch budget too small");
    if (layout_.size() > max_bytes / sizeof(::ObjectId)) {
      return STATUS(InvalidArgument, "Binding batch row exceeds budget");
    }
    auto fits = [&](size_t rows) {
      if (rows > (max_bytes - fixed) / std::max<size_t>(1, layout_.size() * 8 +
                                                     (track_origin ? 8 : 0))) return false;
      size_t cells = rows * layout_.size();
      return fixed + cells * sizeof(::ObjectId) + ((cells + 63) / 64) * 8 +
             (track_origin ? rows * 8 : 0) <= max_bytes;
    };
    size_t lo = 0, hi = std::min(max_rows, max_bytes);
    while (lo < hi) {
      size_t mid = lo + (hi - lo + 1) / 2;
      if (fits(mid)) lo = mid; else hi = mid - 1;
    }
    if (!lo) return STATUS(InvalidArgument, "Binding batch cannot hold one row");
    capacity_ = lo;
    values_.resize(capacity_ * layout_.size());
    bound_.resize((values_.size() + 63) / 64);
    if (track_origin) origins_.resize(capacity_);
    if (Bytes() > max_bytes) return STATUS(InvalidArgument, "Batch allocation exceeds budget");
    return Status::OK();
  }

  static std::vector<VarId> DenseLayout(size_t width) {
    std::vector<VarId> layout;
    layout.reserve(width);
    for (size_t i = 0; i < width; ++i) layout.emplace_back(i);
    return layout;
  }

  Status Append(const Binding& row, uint64_t origin = 0) {
    if (full()) return STATUS(IllegalState, "Binding batch is full");
    for (size_t col = 0; col < layout_.size(); ++col) {
      if (layout_[col].id >= row.size) return STATUS(InvalidArgument, "Batch column outside binding");
      const auto value = row[layout_[col]];
      const size_t cell = col * capacity_ + rows_;
      values_[cell] = value;
      if (!value.is_null()) bound_[cell / 64] |= uint64_t(1) << (cell % 64);
      else bound_[cell / 64] &= ~(uint64_t(1) << (cell % 64));
    }
    if (!origins_.empty()) origins_[rows_] = origin;
    ++rows_;
    return Status::OK();
  }

  Status CopyRow(size_t row, Binding* output) const {
    if (row >= rows_ || !output) return STATUS(InvalidArgument, "Invalid binding batch row");
    for (size_t col = 0; col < layout_.size(); ++col) {
      if (layout_[col].id >= output->size) return STATUS(InvalidArgument, "Batch output layout mismatch");
      output->add(layout_[col], IsBound(row, col) ? Value(row, col) : ::ObjectId::get_null());
    }
    return Status::OK();
  }

  ::ObjectId Value(size_t row, size_t col) const { return values_.at(col * capacity_ + row); }
  bool IsBound(size_t row, size_t col) const {
    const size_t cell = col * capacity_ + row;
    return (bound_.at(cell / 64) >> (cell % 64)) & 1;
  }
  uint64_t Origin(size_t row) const { return origins_.at(row); }
  size_t rows() const { return rows_; }
  size_t capacity() const { return capacity_; }
  bool full() const { return rows_ == capacity_; }
  const std::vector<VarId>& layout() const { return layout_; }
  size_t Bytes() const {
    return sizeof(*this) + layout_.capacity() * sizeof(VarId) +
           values_.capacity() * sizeof(::ObjectId) + bound_.capacity() * sizeof(uint64_t) +
           origins_.capacity() * sizeof(uint64_t);
  }
  bool eof = false;

 private:
  std::vector<VarId> layout_;
  std::vector<::ObjectId> values_;
  std::vector<uint64_t> bound_;
  std::vector<uint64_t> origins_;
  size_t capacity_ = 0;
  size_t rows_ = 0;
};

}  // namespace yb::mdb

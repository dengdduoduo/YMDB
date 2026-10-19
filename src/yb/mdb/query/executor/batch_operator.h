#pragma once

#include <memory>
#include <vector>

#include "query/executor/binding_batch.h"
#include "query/executor/binding_iter.h"
#include "yb/util/result.h"

namespace yb::mdb {

class BatchOperator {
 public:
  virtual ~BatchOperator() = default;
  virtual Status Open(const Binding& input) = 0;
  virtual Result<BindingBatch> NextBatch() = 0;
  virtual void Close() = 0;
};

class BindingIterBatchSource : public BatchOperator {
 public:
  BindingIterBatchSource(std::unique_ptr<BindingIter> iter, size_t rows, size_t bytes)
      : iter_(std::move(iter)), row_limit_(rows), byte_limit_(bytes) {}
  Status Open(const Binding& input) override;
  Result<BindingBatch> NextBatch() override;
  void Close() override;
  void AssignNulls() { iter_->assign_nulls(); }
  size_t width() const { return binding_ ? binding_->size : 0; }
  uint64_t emitted_rows() const { return emitted_; }
  void SetLimits(size_t rows, size_t bytes) { row_limit_ = rows; byte_limit_ = bytes; }
 private:
  std::unique_ptr<BindingIter> iter_;
  std::unique_ptr<Binding> binding_;
  size_t row_limit_, byte_limit_;
  uint64_t emitted_ = 0;
  bool eof_ = false;
  Status failure_;
};

class BatchBindingIter : public BindingIter {
 public:
  BatchBindingIter(std::unique_ptr<BatchOperator> source, std::vector<VarId> output_vars)
      : source_(std::move(source)), output_vars_(std::move(output_vars)) {}
  ~BatchBindingIter() override { source_->Close(); }
  void _begin(Binding& input) override;
  bool _next() override;
  void _reset() override;
  void assign_nulls() override;
  void print(std::ostream& os, int indent, bool stats) const override;
 private:
  std::unique_ptr<BatchOperator> source_;
  std::vector<VarId> output_vars_;
  Binding* binding_ = nullptr;
  BindingBatch batch_;
  size_t position_ = 0;
  Status failure_;
};

}  // namespace yb::mdb

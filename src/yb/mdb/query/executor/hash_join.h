#pragma once

#include "query/executor/batch_operator.h"
#include "query/executor/batch_options.h"

namespace yb::mdb {
class DocDBReadContext;

// Equi inner join for plans whose shared variables are guaranteed bound.
// Fixed-capacity chained buckets retain every build row, including duplicates.
class HashJoin : public BatchOperator {
 public:
  HashJoin(std::unique_ptr<BindingIter> build, std::unique_ptr<BindingIter> probe,
           std::vector<VarId> keys, std::vector<VarId> build_vars,
           DocDBReadContext* context, BatchJoinOptions options);
  ~HashJoin() override { Close(); }
  Status Open(const Binding& input) override;
  Result<BindingBatch> NextBatch() override;
  void Close() override;
 private:
  Status Check() const;
  Status Build();
  Result<bool> ReadRow(BindingIterBatchSource* source, BindingBatch* batch,
                       size_t* position);
  Result<size_t> Bucket() const;
  bool Matches(size_t index) const;
  BindingIterBatchSource build_, probe_;
  std::vector<VarId> keys_, build_vars_;
  std::vector<size_t> key_columns_;
  DocDBReadContext* context_;
  BatchJoinOptions options_;
  std::unique_ptr<Binding> work_;
  BindingBatch build_batch_, probe_batch_;
  std::vector<uint64_t> rows_;
  std::vector<size_t> buckets_;
  size_t width_ = 0, stride_ = 0, count_ = 0, capacity_ = 0;
  size_t build_position_ = 0, probe_position_ = 0, match_ = SIZE_MAX;
  size_t reservation_ = 0;
  bool built_ = false;
  Status failure_;
};
}  // namespace yb::mdb

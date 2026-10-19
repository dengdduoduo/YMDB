#pragma once

#include <array>
#include <memory>
#include <set>
#include <vector>

#include "query/executor/batch_operator.h"
#include "query/executor/batch_options.h"
#include "query/executor/binding_iter/docdb_scan.h"

namespace yb::mdb {

class BatchIndexJoin : public BatchOperator {
 public:
  BatchIndexJoin(std::unique_ptr<BindingIter> lhs, std::unique_ptr<DocDBScan> rhs);
  ~BatchIndexJoin() override { Close(); }
  Status Open(const Binding& input) override;
  Result<BindingBatch> NextBatch() override;
  void Close() override;
 private:
  using Key = std::array<uint64_t, 6>;
  struct Group {
    Key key;
    DocDBScanSpec spec;
    std::string partition;
    std::vector<size_t> rows;
  };
  struct Probe {
    Group* group = nullptr;
    std::unique_ptr<DocDBScanCursor> cursor;
    DocDBScanBatch page;
    size_t row = 0, outer = 0;
    bool matched = false;
  };
  Status Check() const;
  Status BuildGroups();
  Result<bool> Refill();
  Result<bool> NextRow(uint64_t* origin);
  Status CheckMemory();
  size_t MemoryBytes() const;
  std::unique_ptr<DocDBScan> rhs_;
  DocDBReadContext* context_;
  BatchJoinOptions options_;
  BindingIterBatchSource source_;
  std::unique_ptr<Binding> work_;
  BindingBatch outer_;
  std::vector<Group> groups_;
  std::vector<Probe> wave_;
  std::set<Key> negative_;
  size_t next_group_ = 0, wave_position_ = 0, output_bytes_ = 0;
  size_t max_input_rows_ = 0, max_in_flight_ = 0, negative_limit_ = 0;
  bool outer_eof_ = false;
  Status failure_;
};

std::unique_ptr<BindingIter> MakeIndexJoin(
    std::unique_ptr<BindingIter> lhs, std::unique_ptr<BindingIter> rhs,
    std::vector<VarId> output_vars);

}  // namespace yb::mdb

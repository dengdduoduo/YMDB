// Portions Copyright (c) YugabyteDB, Inc.

#pragma once

#include <array>
#include <memory>
#include <unordered_set>

#include "graph_models/object_id.h"
#include "query/executor/binding.h"
#include "query/executor/binding_iter.h"
#include "query/executor/binding_iter/scan_ranges/scan_range.h"
#include "query/id.h"
#include "yb/mdb/storage/docdb_range_cache.h"

namespace yb::mdb { class RdfYBModel; }

class DocDBScan : public BindingIter {
 public:
  static constexpr size_t kMaxDynamicPartitions = 64;

  DocDBScan(
      yb::mdb::DocDBRangeCache& cache, int perm_index,
      std::array<std::unique_ptr<ScanRange>, 3> ranges, int num_ranges, Id subject, Id predicate,
      Id object, bool subject_assigned, bool predicate_assigned, bool object_assigned,
      bool check_c0_eq_c1 = false, bool check_c1_eq_c2 = false, bool check_c0_eq_c2 = false,
      yb::mdb::RdfYBModel* model = nullptr, int c0_var_id = -1,
      yb::Status initial_status = yb::Status::OK());

  void _begin(Binding& parent_binding) override;
  bool _next() override;
  void _reset() override;
  void assign_nulls() override;
  void print(std::ostream& os, int indent, bool stats) const override;

 private:
  yb::mdb::DocDBRangeCache* cache_;
  yb::mdb::DocDBRangeCache& fallback_cache_;
  yb::mdb::RdfYBModel* model_;
  int c0_var_id_;
  size_t dynamic_partition_count_ = 0;
  std::unordered_set<uint64_t> dynamic_partitions_;
  bool use_full_cache_ = false;
  void SelectPartition();
  int perm_index_;
  std::array<std::unique_ptr<ScanRange>, 3> ranges_;
  int num_ranges_;
  Id subject_, predicate_, object_;
  bool subject_assigned_, predicate_assigned_, object_assigned_;
  bool check_c0_eq_c1_, check_c1_eq_c2_, check_c0_eq_c2_;
  Binding* parent_binding_ = nullptr;
  bool first_call_ = true;
  bool use_c0_prefix_seek_ = false;
  uint64_t prefix_c0_ = 0;
  uint64_t prefix_max_c1_ = 0;
  uint64_t prefix_min_c2_ = 0;
  uint64_t prefix_max_c2_ = 0;
  size_t row_index_ = 0;
  uint64_t current_c0_, current_c1_, current_c2_;

  static constexpr uint64_t kMax = 0xFFFFFFFFFFFFFFULL;
};

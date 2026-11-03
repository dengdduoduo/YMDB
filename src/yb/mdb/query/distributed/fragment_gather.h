// Copyright (c) YugabyteDB, Inc.
#pragma once

#include <array>
#include <functional>
#include <memory>
#include "yb/mdb/query/distributed/fragment_plan.h"
#include "yb/mdb/storage/docdb_read_context.h"

namespace yb::mdb {

// One sequential caller explicitly owns process leases across its queries.
class FragmentWorkerPool {
 public:
  struct Impl;
  explicit FragmentWorkerPool(size_t capacity);
  ~FragmentWorkerPool();
  Status Close();
  std::string StatsJson() const;
  FragmentWorkerPool(const FragmentWorkerPool&) = delete;
  FragmentWorkerPool& operator=(const FragmentWorkerPool&) = delete;

 private:
  std::unique_ptr<Impl> impl_;
};

using FragmentRowSink = std::function<Status(const std::vector<uint64_t>&)>;

using FragmentInputSink = std::function<Status(size_t, const std::vector<uint64_t>&)>;

// Pull one bounded frame per active process. No result-sized vector or retry.
Status GatherFragments(
    const FragmentPlan& plan, const WorkerOptions& options, DocDBReadContext& context,
    const FragmentRowSink& sink);

// Both inputs share one child read view and one aggregate max_workers limit.
Status GatherFragmentPair(
    const std::array<FragmentPlan, 2>& plans, const WorkerOptions& options,
    DocDBReadContext& context, const FragmentInputSink& sink);

}  // namespace yb::mdb

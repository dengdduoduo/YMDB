// Copyright (c) YugabyteDB, Inc.
#pragma once

#include "yb/mdb/query/distributed/fragment_plan.h"
#include "yb/mdb/query/executor/graph_expand_pipeline.h"

namespace yb::mdb {

struct GraphDecision {
  std::string mode = "old", reason = "explicit_old";
  bool graph = false, sample_complete = false;
  uint64_t seed_rows = 0, sample_rows = 0, unique_probes = 0;
  uint64_t read_operations = 0, payload_bytes = 0;
  std::string ToJson() const;
};
Result<GraphDecision> ChooseGraphExecution(const FragmentPlan& plan, RdfYBModel& model,
    DocDBReadContext& context, const WorkerOptions& options);

Status ValidateGraphProgram(const FragmentOpenPB& open);
Result<FragmentPlan> BuildGraphFragment(const SPARQL::Op& root, RdfYBModel& model,
                                       const WorkerOptions& options);

class GraphFragment {
 public:
  Status Open(const FragmentOpenPB& open, std::array<const client::TableHandle*, 6> tables,
              client::YBSessionPtr session, CoarseTimePoint deadline);
  Result<bool> Next(GraphBinding* output);
  uint64_t read_operations() const;
 private:
  Result<bool> NextSeed(GraphBinding* seed);
  std::vector<GraphExpandSpec> Specs(GraphBinding* seed) const;
  FragmentOpenPB open_;
  std::array<const client::TableHandle*, 6> tables_;
  std::unique_ptr<DocDBReadContext> context_;
  DocDBScanCursor anchor_;
  DocDBScanBatch page_;
  size_t position_ = 0, seed_position_ = 0;
  uint64_t origin_ = 0, completed_reads_ = 0;
  std::unique_ptr<GraphExpand> expand_;
  std::unique_ptr<GraphExpandPipeline> pipeline_;
};
}  // namespace yb::mdb

// Copyright (c) YugabyteDB, Inc.
#pragma once

#include <optional>

#include "query/parser/op/sparql/op.h"
#include "yb/mdb/query/distributed/fragment.pb.h"
#include "yb/mdb/query/distributed/tablet_router.h"
#include "yb/mdb/query/distributed/worker_options.h"

namespace yb::mdb {

class RdfYBModel;
class DocDBReadContext;

struct PhysicalProperties {
  enum class Partitioning { Unknown, YcqlSubjectHash };
  Partitioning partitioning = Partitioning::Unknown;
  std::string partition_function;
  std::string anchor_table_id;
  std::optional<uint32_t> partition_variable;
  std::optional<double> estimated_rows;
  std::optional<double> estimated_bytes;
  std::vector<uint32_t> output_variables;
  std::vector<uint32_t> bound_variables;
  // Hash partition order is not a global SPARQL value ordering.
  bool globally_ordered = false;
};

// Executable storage-ID graph, separate from the parser's process-local Op tree.
struct FragmentPlan {
  PhysicalProperties properties;
  FragmentOpenPB physical;
  std::vector<TabletInfo> tasks;
  bool empty = false;
  bool coordinator_finalize = false;
  uint64_t global_row_reservation = 0;

  // Called only after Build has accepted the complete coordinator envelope.
  static std::unique_ptr<SPARQL::Op>* InputSlot(SPARQL::Op& root);

  static Result<FragmentPlan> Build(const SPARQL::Op& root, RdfYBModel& model,
                                    const WorkerOptions& options);
  static Status ValidateCoverage(const std::vector<TabletInfo>& tasks);
};

// Validation also runs at the worker boundary before allocation or storage reads.
Status ValidateFragmentOpen(const FragmentOpenPB& open);

}  // namespace yb::mdb

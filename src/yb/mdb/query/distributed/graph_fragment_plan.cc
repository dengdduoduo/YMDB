// Copyright (c) YugabyteDB, Inc.
#include "yb/mdb/query/distributed/graph_fragment.h"

#include <algorithm>
#include <set>
#include <sstream>

#include "yb/client/session.h"
#include "yb/client/yb_op.h"
#include "yb/common/ql_protocol_util.h"
#include "yb/mdb/storage/yb_client_compat.h"

#include "query/parser/op/sparql/op_basic_graph_pattern.h"
#include "query/parser/op/sparql/op_filter.h"
#include "query/parser/op/sparql/op_join.h"
#include "query/parser/op/sparql/op_order_by.h"
#include "query/parser/op/sparql/op_select.h"
#include "query/parser/op/sparql/op_sequence.h"
#include "query/parser/op/sparql/op_values.h"
#include "yb/mdb/import/import.h"
#include "graph_models/rdf_model/rdf_object_id.h"
#include "yb/mdb/model/rdf_yb_model.h"

namespace yb::mdb {
namespace {
Status RemoveSeeds(std::unique_ptr<SPARQL::Op>* slot, std::unique_ptr<SPARQL::OpValues>* seeds) {
  if (auto* values = dynamic_cast<SPARQL::OpValues*>(slot->get())) {
    if (*seeds) {
      return STATUS(NotSupported, "Multiple graph VALUES inputs are unsupported");
    }
    seeds->reset(static_cast<SPARQL::OpValues*>(slot->release()));
    *slot = std::make_unique<SPARQL::OpBasicGraphPattern>(
        std::vector<SPARQL::OpTriple>{}, std::vector<SPARQL::OpPath>{});
    if (values->row_count > 256) {
      return STATUS(NotSupported, "Graph VALUES budget exceeded");
    }
  } else if (auto* join = dynamic_cast<SPARQL::OpJoin*>(slot->get())) {
    RETURN_NOT_OK(RemoveSeeds(&join->lhs, seeds));
    RETURN_NOT_OK(RemoveSeeds(&join->rhs, seeds));
  } else if (auto* sequence = dynamic_cast<SPARQL::OpSequence*>(slot->get())) {
    for (auto& child : sequence->ops) {
      RETURN_NOT_OK(RemoveSeeds(&child, seeds));
    }
  } else if (auto* filter = dynamic_cast<SPARQL::OpFilter*>(slot->get())) {
    (void)filter;
    return STATUS(NotSupported, "Nested graph filters retain coordinator execution");
  }
  return Status::OK();
}
}  // namespace

Result<FragmentPlan> BuildGraphFragment(
    const SPARQL::Op& root, RdfYBModel& model, const WorkerOptions& options) {
  if (options.exchange != ExchangeMode::None || options.sample_join_order) {
    return STATUS(NotSupported, "Explicit exchange/sampling retains its original execution");
  }
  auto clone = root.clone();
  auto* select = dynamic_cast<SPARQL::OpSelect*>(clone.get());
  if (!select || select->is_sub_select) {
    return STATUS(NotSupported, "Graph input requires SELECT");
  }
  for (const auto& expr : select->vars_exprs) {
    if (expr) {
      return STATUS(NotSupported, "Graph SELECT expressions retain coordinator execution");
    }
  }
  auto* slot = &select->op;
  while (true) {
    if (auto* order = dynamic_cast<SPARQL::OpOrderBy*>(slot->get())) {
      slot = &order->op;
    } else if (auto* filter = dynamic_cast<SPARQL::OpFilter*>(slot->get())) {
      auto child = std::move(filter->op);
      *slot = std::move(child);
    } else {
      break;
    }
  }
  std::unique_ptr<SPARQL::OpValues> seeds;
  RETURN_NOT_OK(RemoveSeeds(slot, &seeds));
  LOG(INFO) << "Graph lowered input: " << *clone;
  auto legacy = options;
  legacy.graph_native = false;
  legacy.graph_mode = GraphMode::Old;
  auto plan = VERIFY_RESULT(FragmentPlan::Build(*clone, model, legacy));
  if (plan.count) {
    return STATUS(NotSupported, "Graph aggregates retain coordinator execution");
  }
  auto& open = plan.physical;
  open.set_graph_version(1);
  if (seeds) {
    open.set_graph_seeded(true);
    open.set_graph_seed_rows(seeds->row_count);
    std::set<uint32_t> bgp(
        plan.properties.bound_variables.begin(), plan.properties.bound_variables.end());
    for (auto var : seeds->vars) {
      if (!bgp.count(var.id)) {
        return STATUS(NotSupported, "Graph VALUES variables must occur in its BGP");
      }
    }
    for (size_t row = 0; row < seeds->row_count; ++row) {
      std::vector<uint64_t> binding(open.variable_count(), 0);
      for (size_t col = 0; col < seeds->vars.size(); ++col) {
        const auto packed = seeds->values[row * seeds->vars.size() + col];
        const auto found = g_pack_to_gmap.find(packed.id);
        if (packed.is_null() || found == g_pack_to_gmap.end()) {
          return STATUS(NotSupported, "Unbound/unknown graph VALUES retains coordinator execution");
        }
        binding[seeds->vars[col].id] = found->second;
      }
      for (auto id : binding) {
        open.add_graph_seed_values(id);
      }
    }
  }
  RETURN_NOT_OK(ValidateGraphProgram(open));
  // Stage all typed input before public output: late or mixed-version workers
  // must fail without exposing earlier fragments as a completed result.
  plan.coordinator_finalize = true;
  plan.topk.reset();
  plan.distinct.reset();
  plan.count.reset();
  open.clear_projection();
  plan.properties.output_variables.clear();
  for (auto variable : plan.properties.bound_variables) {
    open.add_projection(variable);
    plan.properties.output_variables.push_back(variable);
  }
  if (open.graph_seeded() && open.graph_seed_rows()) {
    const auto& subject = open.triples(0).terms(0);
    const bool out = subject.has_constant() || open.graph_seed_values(subject.variable()) != 0;
    if (!out) {
      TabletRouter router;
      RETURN_NOT_OK(router.Init(model.Client(), model.Namespace()));
      plan.tasks = router.GetTablets(5);
      std::sort(plan.tasks.begin(), plan.tasks.end(), [](const auto& a, const auto& b) {
        return a.partition_start < b.partition_start;
      });
      RETURN_NOT_OK(FragmentPlan::ValidateCoverage(plan.tasks));
      plan.properties.partitioning = PhysicalProperties::Partitioning::Unknown;
      plan.properties.partition_variable.reset();
      plan.properties.partition_function = "YCQL-object-hash-v1";
      plan.properties.anchor_table_id = open.table_ids(5);
    }
  }
  return plan;
}

std::string GraphDecision::ToJson() const {
  std::ostringstream out;
  out << "{\"mode\":\"" << mode << "\",\"selected\":\"" << (graph ? "graph" : "old")
      << "\",\"reason\":\"" << reason << "\",\"seed_rows\":" << seed_rows
      << ",\"sample_rows\":" << sample_rows << ",\"unique_first_hop_probes\":" << unique_probes
      << ",\"probe_read_operations\":" << read_operations
      << ",\"probe_payload_bytes\":" << payload_bytes
      << ",\"sample_complete\":" << (sample_complete ? "true" : "false")
      << ",\"historical_statistics_used\":false,\"probe_read_operation_limit\":1,\"probe_page_"
         "rows\":16"
         ",\"probe_buffer_limit_bytes\":524288}";
  return out.str();
}

Result<GraphDecision> ChooseGraphExecution(
    const FragmentPlan& plan, RdfYBModel& model, DocDBReadContext& context,
    const WorkerOptions& options) {
  GraphDecision decision;
  decision.mode = "auto";
  auto check = [&]() -> Status {
    if (context.cancelled.load()) {
      return STATUS(Aborted, "Graph decision cancelled");
    }
    if (CoarseMonoClock::Now() >= context.deadline) {
      return STATUS(TimedOut, "Graph decision deadline expired");
    }
    if (!context.session || context.session->IsRestartRequired()) {
      return STATUS(TryAgain, "Graph decision requires the original query read view");
    }
    return Status::OK();
  };
  RETURN_NOT_OK(check());
  const auto& open = plan.physical;
  RETURN_NOT_OK(ValidateGraphProgram(open));
  const auto reserved = FragmentBufferedReservation(open);
  if (options.coordinator_memory_bytes < 512 * 1024 || options.memory_bytes < reserved ||
      options.memory_bytes - reserved < 2 * 1024 * 1024) {
    decision.reason = "conservative_buffer_admission";
    return decision;
  }
  using Key = std::pair<uint64_t, uint64_t>;
  std::set<Key> keys;
  const auto& expand = open.triples(open.graph_seeded() ? 0 : 1);
  auto key = [&](const std::vector<uint64_t>& values) -> Key {
    auto value = [&](const FragmentTermPB& term) {
      return term.has_constant() ? term.constant() : values[term.variable()];
    };
    const auto subject = value(expand.terms(0)), object = value(expand.terms(2));
    return subject ? Key{subject, object} : Key{object, subject};
  };
  if (open.graph_seeded()) {
    decision.seed_rows = open.graph_seed_rows();
    // One worker batch bounds sharing evidence without assuming a cross-batch cache.
    if (open.graph_seed_rows() > 128) {
      decision.reason = "values_exceed_one_seed_batch";
      return decision;
    }
    for (uint32_t row = 0; row < open.graph_seed_rows(); ++row) {
      std::vector<uint64_t> values(open.variable_count());
      for (uint32_t col = 0; col < open.variable_count(); ++col) {
        values[col] = open.graph_seed_values(row * open.variable_count() + col);
      }
      keys.insert(key(values));
    }
    decision.unique_probes = keys.size();
    decision.sample_complete = true;
    decision.graph = decision.seed_rows >= 8 && 2 * keys.size() <= decision.seed_rows;
    decision.reason = decision.graph ? "exact_values_share_first_hop_probes"
                                     : "values_without_sufficient_probe_reuse";
    return decision;
  }
  const auto& anchor = open.triples(0);
  if (plan.tasks.empty() || plan.tasks.size() > 4 || anchor.terms(0).has_constant() ||
      anchor.terms(2).has_constant()) {
    decision.reason = "conservative_probe_coverage_limit";
    return decision;
  }
  // A predicate hash lookup in POS returns at most 16 unfiltered index rows.
  // A filtered SPO request could examine an entire tablet despite a row limit.
  DocDBScanSpec spec;
  spec.hash_key = anchor.terms(1).constant();
  spec.columns[0] = {*spec.hash_key, *spec.hash_key};
  spec.page_rows = 16;
  spec.page_bytes = 16 * 24;
  spec.deadline = context.deadline;
  spec.cancelled = &context.cancelled;
  DocDBScanCursor cursor;
  RETURN_NOT_OK(cursor.Open(model.GetTable(1), context.session, spec));
  const auto page = cursor.NextBatch();
  decision.read_operations = cursor.rpc_calls();
  decision.payload_bytes = cursor.payload_bytes();
  if (!page.ok()) {
    decision.reason = "probe_error_no_execution_fallback";
    context.stats.graph_execution_decision = decision.ToJson();
    return page.status();
  }
  RETURN_NOT_OK(check());
  decision.sample_complete = page->eof;
  std::array<std::set<Key>, 4> fragment_keys;
  for (const auto& stored : page->rows) {
    const DocDBScanBatch::Row row = {stored[2], stored[0], stored[1]};
    std::vector<uint64_t> values(open.variable_count());
    bool compatible = true;
    for (int col = 0; col < 3; ++col) {
      if (!row[col]) {
        return STATUS(Corruption, "Zero graph decision term ID");
      }
      const auto& term = anchor.terms(col);
      if (term.has_constant()) {
        compatible &= row[col] == term.constant();
      } else {
        auto& previous = values[term.variable()];
        compatible &= !previous || previous == row[col];
        previous = row[col];
      }
    }
    if (!compatible) {
      continue;
    }
    // Group reuse by the actual SPO anchor owner, never across worker fragments.
    auto read = NewMdbReadOp(*model.GetTable(0), context.session);
    QLAddInt64HashValue(read->mutable_request(), static_cast<int64_t>(row[0]));
    std::string hash;
    RETURN_NOT_OK(read->GetPartitionKey(&hash));
    size_t owner = 0;
    for (; owner < plan.tasks.size(); ++owner) {
      const auto& task = plan.tasks[owner];
      if (hash >= task.partition_start &&
          (task.partition_end.empty() || hash < task.partition_end)) {
        break;
      }
    }
    if (owner == plan.tasks.size()) {
      return STATUS(Corruption, "Graph probe owner missing");
    }
    ++decision.sample_rows;
    fragment_keys[owner].insert(key(values));
  }
  for (const auto& owned : fragment_keys) {
    decision.unique_probes += owned.size();
  }
  decision.graph = decision.sample_rows >= 8 && 2 * decision.unique_probes <= decision.sample_rows;
  decision.reason = decision.graph ? "snapshot_sample_shares_first_hop_probes"
                                   : "snapshot_sample_inconclusive_keep_old";
  return decision;
}
}  // namespace yb::mdb

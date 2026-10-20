// Copyright (c) YugabyteDB, Inc.
#include "yb/mdb/query/distributed/fragment_plan.h"

#include <algorithm>
#include <set>
#include "yb/client/table.h"
#include "yb/util/net/net_util.h"

#include "query/parser/op/sparql/op_basic_graph_pattern.h"
#include "query/parser/op/sparql/op_join.h"
#include "query/parser/op/sparql/op_sequence.h"
#include "query/parser/op/sparql/op_select.h"
#include "query/parser/op/sparql/op_order_by.h"
#include "query/parser/op/sparql/op_group_by.h"
#include "query/parser/expr/sparql/atom/expr_var.h"
#include "query/parser/expr/sparql/builtin_call/aggregate/expr_agg_count.h"
#include "query/parser/expr/sparql/builtin_call/aggregate/expr_agg_count_all.h"
#include "yb/mdb/model/rdf_yb_model.h"
#include "yb/mdb/query/distributed/fragment_transport.h"

namespace yb::mdb {
namespace {
Status Collect(const SPARQL::Op& op, std::vector<SPARQL::OpTriple>* triples) {
  if (auto* bgp = dynamic_cast<const SPARQL::OpBasicGraphPattern*>(&op)) {
    if (!bgp->paths.empty()) return STATUS(NotSupported, "Worker property paths are not supported");
    for (const auto& triple : bgp->triples) triples->emplace_back(triple);
    return Status::OK();
  }
  if (auto* join = dynamic_cast<const SPARQL::OpJoin*>(&op)) {
    RETURN_NOT_OK(Collect(*join->lhs, triples));
    return Collect(*join->rhs, triples);
  }
  if (auto* sequence = dynamic_cast<const SPARQL::OpSequence*>(&op)) {
    for (const auto& child : sequence->ops) RETURN_NOT_OK(Collect(*child, triples));
    return Status::OK();
  }
  return STATUS(NotSupported, "Operator remains at MDB coordinator");
}
}  // namespace

Status FragmentPlan::ValidateCoverage(const std::vector<TabletInfo>& tasks) {
  if (tasks.empty() || !tasks.front().partition_start.empty() || !tasks.back().partition_end.empty())
    return STATUS(Corruption, "MDB anchor tasks do not cover the table");
  std::set<std::string> ids;
  for (size_t i = 0; i < tasks.size(); ++i) {
    const auto& task = tasks[i];
    if (task.tablet_id.empty() || !ids.insert(task.tablet_id).second)
      return STATUS(Corruption, "MDB anchor task has missing or duplicate identity");
    if ((!task.partition_start.empty() && task.partition_start.size() != 2) ||
        (!task.partition_end.empty() && task.partition_end.size() != 2) ||
        (!task.partition_end.empty() && task.partition_start >= task.partition_end))
      return STATUS(Corruption, "MDB anchor task has invalid range");
    if (i && (tasks[i-1].partition_end.empty() || tasks[i-1].partition_end != task.partition_start))
      return STATUS(Corruption, "MDB anchor tasks overlap or have a gap");
  }
  return Status::OK();
}

Status ValidateFragmentOpen(const FragmentOpenPB& open) {
  if (open.id_namespace() != "rdf_idmap-v1" || open.master_addresses().empty() ||
      open.keyspace().empty() || open.table_ids_size() != 6 || !open.has_transaction() ||
      !open.transaction().has_metadata() || !open.transaction().has_read_time())
    return STATUS(InvalidArgument, "Incomplete MDB fragment storage or snapshot identity");
  if (!open.variable_count() || open.variable_count() > 128 || open.triples_size() < 1 ||
      open.triples_size() > 32 || open.projection_size() < 1 || open.projection_size() > 128 ||
      !open.batch_rows() || open.batch_rows() > 256 || !open.page_rows() || open.page_rows() > 256 ||
      !open.timeout_ms() || open.timeout_ms() > 30000)
    return STATUS(InvalidArgument, "MDB fragment dimensions exceed supported limits");
  // Include all frame copies, depth-first bindings, decoded pages and protocol slack.
  const uint64_t reserved = 4 * kFragmentMaxFrame + uint64_t(open.triples_size()) *
      (uint64_t(open.variable_count()) * 8 + uint64_t(open.page_rows()) * 96 + 4096);
  if (open.memory_bytes() < reserved || open.memory_bytes() > 64 * 1024 * 1024)
    return STATUS(RuntimeError, "MDB fragment buffered-state budget exceeded");
  for (const auto& id : open.table_ids()) if (id.empty())
    return STATUS(InvalidArgument, "Missing MDB table identity");
  const auto& start = open.partition_start();
  const auto& end = open.partition_end();
  if ((!start.empty() && start.size() != 2) || (!end.empty() && end.size() != 2) ||
      (!end.empty() && start >= end)) return STATUS(InvalidArgument, "Invalid MDB anchor bounds");
  std::set<uint32_t> bound;
  for (const auto& triple : open.triples()) {
    if (triple.terms_size() != 3) return STATUS(InvalidArgument, "MDB triple requires three terms");
    for (const auto& term : triple.terms()) {
      if (term.has_variable() == term.has_constant() ||
          (term.has_variable() && term.variable() >= open.variable_count()) ||
          (term.has_constant() && term.constant() == 0))
        return STATUS(InvalidArgument, "Invalid MDB fragment term");
      if (term.has_variable()) bound.insert(term.variable());
    }
  }
  for (auto variable : open.projection()) if (!bound.count(variable))
    return STATUS(NotSupported, "Worker projection requires bound BGP variables");
  if (uint64_t(open.batch_rows()) * open.projection_size() * 8 + 8192 > kFragmentMaxFrame)
    return STATUS(InvalidArgument, "MDB output batch exceeds frame budget");
  return Status::OK();
}

Result<FragmentPlan> FragmentPlan::Build(
    const SPARQL::Op& root, RdfYBModel& model, const WorkerOptions& options) {
  auto* select = dynamic_cast<const SPARQL::OpSelect*>(&root);
  if (!select || !select->op || select->is_sub_select)
    return STATUS(NotSupported, "Worker input requires a root SELECT");
  bool aggregate = false;
  uint64_t distinct_tuple_sets = 0;
  for (const auto& expression : select->vars_exprs) if (expression) {
    // Other aggregates/expressions retain the existing coordinator path until
    // their value, overflow and resource semantics have independent coverage.
    auto* count = dynamic_cast<const SPARQL::ExprAggCount*>(expression.get());
    auto* all = dynamic_cast<const SPARQL::ExprAggCountAll*>(expression.get());
    if ((!count || !dynamic_cast<const SPARQL::ExprVar*>(count->expr.get())) && !all)
      return STATUS(NotSupported, "SELECT expression is outside global fragment capabilities");
    aggregate = true;
    if (all && all->distinct) ++distinct_tuple_sets;
  }
  const SPARQL::Op* input = select->op.get();
  bool ordered = false, grouped = false;
  uint64_t order_keys = 0;
  std::set<VarId> final_variables, order_variables;
  for (size_t i = 0; i < select->vars_exprs.size(); ++i)
    if (select->vars_exprs[i]) final_variables.insert(select->vars[i]);
  while (true) {
    if (auto* order = dynamic_cast<const SPARQL::OpOrderBy*>(input)) {
      if (ordered) return STATUS(NotSupported, "Nested global ordering is unsupported");
      for (const auto& item : order->items) if (!std::holds_alternative<VarId>(item))
        return STATUS(NotSupported, "ORDER BY expressions remain at coordinator");
      order_keys = order->items.size();
      if (order_keys > 128)
        return STATUS(NotSupported, "Global ORDER BY key limit exceeded");
      for (const auto& item : order->items) order_variables.insert(std::get<VarId>(item));
      ordered = true;
      input = order->op.get();
    } else if (auto* group = dynamic_cast<const SPARQL::OpGroupBy*>(input)) {
      if (grouped) return STATUS(NotSupported, "Nested grouping is unsupported");
      for (const auto& item : group->items)
        if (item.second || !dynamic_cast<const SPARQL::ExprVar*>(item.first.get()))
          return STATUS(NotSupported, "GROUP BY expressions remain at coordinator");
      for (const auto& item : group->items)
        final_variables.insert(static_cast<const SPARQL::ExprVar&>(*item.first).var);
      grouped = true;
      input = group->op.get();
    } else break;
  }
  if (aggregate || grouped) {
    for (auto var : order_variables) if (!final_variables.count(var))
      return STATUS(NotSupported, "Non-grouped ORDER BY variable remains at coordinator");
  }
  std::vector<SPARQL::OpTriple> triples;
  RETURN_NOT_OK(Collect(*input, &triples));
  if (triples.empty() || triples.size() > 32 || select->vars.empty())
    return STATUS(NotSupported, "Unsupported MDB BGP size");
  FragmentPlan plan;
  const uint64_t variables = get_query_ctx().get_var_size();
  if (variables > 128) return STATUS(NotSupported, "Worker variable limit exceeded");
  // Each COUNT(DISTINCT *) retains a full binding tuple. Account for every
  // such set, not just the number of returned columns or aggregate outputs.
  // Group sorting retains its input while final sorting materializes the group
  // outputs (at most one per input row). Charge explicit keys as well, including
  // repeated keys that do not increase the query's variable count.
  plan.global_row_reservation = 1024 + 128 * variables +
      16 * distinct_tuple_sets * variables + 16 * order_keys;
  plan.coordinator_finalize = aggregate || grouped || ordered || select->distinct ||
      select->limit != SPARQL::Op::DEFAULT_LIMIT || select->offset != 0;
  auto& physical = plan.physical;
  physical.set_master_addresses(model.Client()->GetMasterLeaderAddress().ToString());
  physical.set_keyspace(model.Namespace());
  for (int perm = 0; perm < 6; ++perm) physical.add_table_ids(model.GetTable(perm)->table()->id());
  physical.set_variable_count(static_cast<uint32_t>(get_query_ctx().get_var_size()));
  physical.set_batch_rows(static_cast<uint32_t>(options.batch_rows));
  physical.set_page_rows(static_cast<uint32_t>(options.page_rows));
  physical.set_memory_bytes(options.memory_bytes);
  for (auto var : select->vars) {
    if (plan.coordinator_finalize) break;
    physical.add_projection(static_cast<uint32_t>(var.id));
    plan.properties.output_variables.push_back(static_cast<uint32_t>(var.id));
  }
  std::set<uint32_t> bound;
  for (const auto& triple : triples) {
    auto* out = physical.add_triples();
    for (const auto& id : {triple.subject, triple.predicate, triple.object}) {
      auto* term = out->add_terms();
      if (id.is_var()) {
        term->set_variable(static_cast<uint32_t>(id.get_var().id));
        bound.insert(static_cast<uint32_t>(id.get_var().id));
      } else {
        if (id.get_OID().is_not_found()) plan.empty = true;
        term->set_constant(id.get_OID().id);
      }
    }
  }
  plan.properties.bound_variables.assign(bound.begin(), bound.end());
  if (bound.empty()) return STATUS(NotSupported, "Constant-only BGP remains at coordinator");
  if (plan.coordinator_finalize) for (auto var : bound) {
    physical.add_projection(var);
    plan.properties.output_variables.push_back(var);
  }
  for (auto var : physical.projection()) if (!bound.count(var))
    return STATUS(NotSupported, "Unbound projection remains at MDB coordinator");
  plan.properties.partitioning = PhysicalProperties::Partitioning::YcqlSubjectHash;
  const auto& anchor_subject = physical.triples(0).terms(0);
  if (anchor_subject.has_variable()) plan.properties.partition_variable = anchor_subject.variable();
  plan.properties.partition_function = "YCQL-hash-v1";
  plan.properties.anchor_table_id = physical.table_ids(0);
  TabletRouter router;
  RETURN_NOT_OK(router.Init(model.Client(), model.Namespace()));
  plan.tasks = router.GetTablets(0);
  if (plan.tasks.size() > 4096) return STATUS(NotSupported, "MDB task metadata limit exceeded");
  std::sort(plan.tasks.begin(), plan.tasks.end(), [](const auto& a, const auto& b) {
    return a.partition_start < b.partition_start;
  });
  RETURN_NOT_OK(ValidateCoverage(plan.tasks));
  return plan;
}

std::unique_ptr<SPARQL::Op>* FragmentPlan::InputSlot(SPARQL::Op& root) {
  auto* slot = &dynamic_cast<SPARQL::OpSelect&>(root).op;
  while (true) {
    if (auto* order = dynamic_cast<SPARQL::OpOrderBy*>(slot->get())) slot = &order->op;
    else if (auto* group = dynamic_cast<SPARQL::OpGroupBy*>(slot->get())) slot = &group->op;
    else return slot;
  }
}

}  // namespace yb::mdb

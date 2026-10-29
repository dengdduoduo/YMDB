// Copyright (c) YugabyteDB, Inc.
#include "yb/mdb/query/distributed/fragment_plan.h"

#include <algorithm>
#include <limits>
#include <set>
#include "graph_models/object_id.h"
#include "yb/client/table.h"
#include "yb/util/net/net_util.h"

#include "query/parser/op/sparql/op_basic_graph_pattern.h"
#include "query/parser/op/sparql/op_join.h"
#include "query/parser/op/sparql/op_sequence.h"
#include "query/parser/op/sparql/op_select.h"
#include "query/parser/op/sparql/op_order_by.h"
#include "query/parser/op/sparql/op_group_by.h"
#include "query/parser/op/sparql/op_having.h"
#include "query/parser/expr/sparql/atom/expr_term.h"
#include "query/parser/expr/sparql/atom/expr_var.h"
#include "query/parser/expr/sparql/binary/expr_and.h"
#include "query/parser/expr/sparql/binary/expr_addition.h"
#include "query/parser/expr/sparql/binary/expr_subtraction.h"
#include "query/parser/expr/sparql/binary/expr_or.h"
#include "query/parser/expr/sparql/binary/expr_equal.h"
#include "query/parser/expr/sparql/binary/expr_not_equal.h"
#include "query/parser/expr/sparql/binary/expr_greater.h"
#include "query/parser/expr/sparql/binary/expr_greater_or_equal.h"
#include "query/parser/expr/sparql/binary/expr_less.h"
#include "query/parser/expr/sparql/binary/expr_less_or_equal.h"
#include "query/parser/expr/sparql/unary/expr_not.h"
#include "query/parser/expr/sparql/builtin_call/aggregate/expr_agg_count.h"
#include "query/parser/expr/sparql/builtin_call/aggregate/expr_agg_count_all.h"
#include "query/parser/expr/sparql/builtin_call/aggregate/expr_agg_sum.h"
#include "query/parser/expr/sparql/builtin_call/aggregate/expr_agg_avg.h"
#include "query/parser/expr/sparql/builtin_call/aggregate/expr_agg_min.h"
#include "query/parser/expr/sparql/builtin_call/aggregate/expr_agg_max.h"
#include "yb/mdb/model/rdf_yb_model.h"
#include "yb/mdb/query/distributed/fragment_transport.h"
#include "yb/mdb/query/distributed/exchange.h"

namespace yb::mdb {
namespace {
struct HavingBudget {
  uint64_t nodes = 0;
  uint64_t aggregates = 0;
  uint64_t distinct_tuple_sets = 0;
  uint64_t numeric_aggregates = 0;
};

// Only variable arguments have bounded per-row state in this capability.
const SPARQL::Expr* ValueAggregate(const SPARQL::Expr& expr, bool* numeric) {
  *numeric = true;
  if (auto* agg = dynamic_cast<const SPARQL::ExprAggSum*>(&expr)) return agg->expr.get();
  if (auto* agg = dynamic_cast<const SPARQL::ExprAggAvg*>(&expr)) return agg->expr.get();
  if (auto* agg = dynamic_cast<const SPARQL::ExprAggMin*>(&expr)) {
    *numeric = false; return agg->expr.get();
  }
  if (auto* agg = dynamic_cast<const SPARQL::ExprAggMax*>(&expr)) {
    *numeric = false; return agg->expr.get();
  }
  return nullptr;
}

bool HavingInteger(::ObjectId value) {
  if (value.get_type() == ::ObjectId::MASK_POSITIVE_INT) return true;
  if (value.get_type() != ::ObjectId::MASK_INT64_EXTERN) return false;
  // Typed RDF integers use dictionary identities even for small constants.
  // Their mask is assigned only after signed-64-bit parsing succeeds. Bound
  // lexical storage as well; arbitrarily zero-padded terms stay on fallback.
  const auto term = SPARQL::RdfTermRegistry::Find(value.id);
  return term && term->kind == 'L' && term->lexical.size() <= 21 &&
      term->qualifier == std::string(SPARQL::RdfTermIdentity::kXsd) + "integer";
}

template<class T>
bool HavingOperands(const SPARQL::Expr& expr, const SPARQL::Expr** lhs, const SPARQL::Expr** rhs) {
  auto* binary = dynamic_cast<const T*>(&expr);
  if (!binary) return false;
  *lhs = binary->lhs.get();
  *rhs = binary->rhs.get();
  return true;
}

// Deliberately bounded COUNT predicates, not a general expression whitelist.
// SELECT aliases are excluded: HAVING is evaluated before SELECT extensions.
Status ValidateHaving(const SPARQL::Expr& expr, const std::set<VarId>& group_vars,
                      HavingBudget* budget, bool condition = true, size_t depth = 0) {
  if (++budget->nodes > 128 || depth > 16)
    return STATUS(NotSupported, "Global HAVING expression limit exceeded");
  bool numeric;
  if (const auto* argument = ValueAggregate(expr, &numeric)) {
    if (!dynamic_cast<const SPARQL::ExprVar*>(argument))
      return STATUS(NotSupported, "Global value aggregate requires a variable argument");
    ++budget->aggregates;
    ++budget->numeric_aggregates;
    return Status::OK();
  }
  if (auto* count = dynamic_cast<const SPARQL::ExprAggCount*>(&expr)) {
    if (!dynamic_cast<const SPARQL::ExprVar*>(count->expr.get()))
      return STATUS(NotSupported, "HAVING COUNT requires a variable argument");
    ++budget->aggregates;
    return Status::OK();
  }
  if (auto* all = dynamic_cast<const SPARQL::ExprAggCountAll*>(&expr)) {
    ++budget->aggregates;
    budget->distinct_tuple_sets += all->distinct;
    return Status::OK();
  }
  if (auto* var = dynamic_cast<const SPARQL::ExprVar*>(&expr)) {
    if (!condition && group_vars.count(var->var)) return Status::OK();
    return STATUS(NotSupported, "HAVING requires a grouped comparison operand");
  }
  if (auto* term = dynamic_cast<const SPARQL::ExprTerm*>(&expr)) {
    if (HavingInteger(term->term) ||
        term->term.id == ::ObjectId::BOOL_TRUE || term->term.id == ::ObjectId::BOOL_FALSE)
      return Status::OK();
    return STATUS(NotSupported, "HAVING literal remains at coordinator");
  }
  if (auto* negation = dynamic_cast<const SPARQL::ExprNot*>(&expr))
    return ValidateHaving(*negation->expr, group_vars, budget, true, depth + 1);
  const SPARQL::Expr *lhs = nullptr, *rhs = nullptr;
  const bool logical = HavingOperands<SPARQL::ExprAnd>(expr, &lhs, &rhs) ||
      HavingOperands<SPARQL::ExprOr>(expr, &lhs, &rhs);
  const bool equality = !logical && (HavingOperands<SPARQL::ExprEqual>(expr, &lhs, &rhs) ||
      HavingOperands<SPARQL::ExprNotEqual>(expr, &lhs, &rhs));
  const bool relational = !logical && !equality && (
      HavingOperands<SPARQL::ExprGreater>(expr, &lhs, &rhs) ||
      HavingOperands<SPARQL::ExprGreaterOrEqual>(expr, &lhs, &rhs) ||
      HavingOperands<SPARQL::ExprLess>(expr, &lhs, &rhs) ||
      HavingOperands<SPARQL::ExprLessOrEqual>(expr, &lhs, &rhs));
  if (relational) {
    // General RDF relational comparison (including NaN/boolean operands) is a
    // separate capability. This extension orders only COUNTs/int64 constants.
    for (auto* operand : {lhs, rhs}) {
      auto* term = dynamic_cast<const SPARQL::ExprTerm*>(operand);
      bool numeric_operand;
      const bool value_aggregate = ValueAggregate(*operand, &numeric_operand) != nullptr;
      if (!value_aggregate && !dynamic_cast<const SPARQL::ExprAggCount*>(operand) &&
          !dynamic_cast<const SPARQL::ExprAggCountAll*>(operand) &&
          (!term || !HavingInteger(term->term)))
        return STATUS(NotSupported, "HAVING relational operands require COUNT or int64 constants");
    }
  }
  if (logical || equality || relational) {
    RETURN_NOT_OK(ValidateHaving(*lhs, group_vars, budget, logical, depth + 1));
    return ValidateHaving(*rhs, group_vars, budget, logical, depth + 1);
  }
  return STATUS(NotSupported, "HAVING expression remains at coordinator");
}

struct ProjectionBudget {
  uint64_t nodes = 0;
  uint64_t hidden_aggregates = 0;
  uint64_t distinct_tuple_sets = 0;
};

Status ValidateProjection(const SPARQL::Expr& expr, const std::set<VarId>& available,
                          const std::set<VarId>& numeric_vars, ProjectionBudget* budget,
                          bool* numeric, bool root = true, bool charge = true, size_t depth = 0) {
  if ((charge && ++budget->nodes > 128) || depth > 16)
    return STATUS(NotSupported, "Global SELECT expression limit exceeded");
  *numeric = true;
  if (const auto* argument = ValueAggregate(expr, numeric)) {
    if (!dynamic_cast<const SPARQL::ExprVar*>(argument))
      return STATUS(NotSupported, "Global value aggregate requires a variable argument");
    budget->hidden_aggregates += !root;
    return Status::OK();
  }
  if (auto* count = dynamic_cast<const SPARQL::ExprAggCount*>(&expr)) {
    if (!dynamic_cast<const SPARQL::ExprVar*>(count->expr.get()))
      return STATUS(NotSupported, "SELECT COUNT requires a variable argument");
    budget->hidden_aggregates += !root;
    return Status::OK();
  }
  if (auto* all = dynamic_cast<const SPARQL::ExprAggCountAll*>(&expr)) {
    budget->hidden_aggregates += !root;
    budget->distinct_tuple_sets += all->distinct;
    return Status::OK();
  }
  if (auto* var = dynamic_cast<const SPARQL::ExprVar*>(&expr)) {
    if (!available.count(var->var))
      return STATUS(NotSupported, "SELECT expression references an unavailable alias or group variable");
    *numeric = numeric_vars.count(var->var);
    return Status::OK();
  }
  if (auto* term = dynamic_cast<const SPARQL::ExprTerm*>(&expr)) {
    if (HavingInteger(term->term)) return Status::OK();
    return STATUS(NotSupported, "SELECT literal remains at coordinator");
  }
  const SPARQL::Expr *lhs = nullptr, *rhs = nullptr;
  if (HavingOperands<SPARQL::ExprAddition>(expr, &lhs, &rhs) ||
      HavingOperands<SPARQL::ExprSubtraction>(expr, &lhs, &rhs)) {
    bool left_numeric, right_numeric;
    RETURN_NOT_OK(ValidateProjection(*lhs, available, numeric_vars, budget,
                                    &left_numeric, false, charge, depth + 1));
    RETURN_NOT_OK(ValidateProjection(*rhs, available, numeric_vars, budget,
                                    &right_numeric, false, charge, depth + 1));
    if (!left_numeric || !right_numeric)
      return STATUS(NotSupported, "SELECT arithmetic requires COUNT/int64 operands");
    return Status::OK();
  }
  return STATUS(NotSupported, "SELECT expression remains at coordinator");
}

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

uint64_t FragmentBufferedReservation(const FragmentOpenPB& open) {
  return 4 * kFragmentMaxFrame + uint64_t(open.triples_size()) *
      (uint64_t(open.variable_count()) * 8 + uint64_t(open.page_rows()) * 96 + 4096);
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
  const uint64_t reserved = FragmentBufferedReservation(open);
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
  for (const auto& expression : select->vars_exprs)
    if (expression) aggregate = aggregate || expression->has_aggregation();
  const SPARQL::Op* input = select->op.get();
  bool ordered = false, grouped = false;
  uint64_t order_keys = 0;
  std::set<VarId> final_variables, order_variables, group_variables;
  const SPARQL::OpHaving* having = nullptr;
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
    } else if (auto* filter = dynamic_cast<const SPARQL::OpHaving*>(input)) {
      if (having) return STATUS(NotSupported, "Nested HAVING is unsupported");
      having = filter;
      input = filter->op.get();
    } else if (auto* group = dynamic_cast<const SPARQL::OpGroupBy*>(input)) {
      if (grouped) return STATUS(NotSupported, "Nested grouping is unsupported");
      for (const auto& item : group->items)
        if (item.second || !dynamic_cast<const SPARQL::ExprVar*>(item.first.get()))
          return STATUS(NotSupported, "GROUP BY expressions remain at coordinator");
      for (const auto& item : group->items) {
        const auto var = static_cast<const SPARQL::ExprVar&>(*item.first).var;
        final_variables.insert(var);
        group_variables.insert(var);
      }
      grouped = true;
      input = group->op.get();
    } else break;
  }
  HavingBudget having_budget;
  if (having) {
    for (const auto& expr : having->exprs)
      RETURN_NOT_OK(ValidateHaving(*expr, group_variables, &having_budget));
    if (having_budget.aggregates > 16)
      return STATUS(NotSupported, "Global HAVING aggregate limit exceeded");
    aggregate = aggregate || having_budget.aggregates != 0;
    if (!grouped && !aggregate)
      return STATUS(NotSupported, "Ungrouped non-aggregate HAVING remains at coordinator");
    for (auto var : select->vars) if (!final_variables.count(var))
      return STATUS(NotSupported, "HAVING projection requires grouped or aggregate variables");
  }
  ProjectionBudget projection_budget;
  std::set<VarId> available = group_variables, numeric_vars;
  for (size_t i = 0; i < select->vars_exprs.size(); ++i) {
    const auto& expr = select->vars_exprs[i];
    if (!expr) {
      if ((aggregate || grouped) && !group_variables.count(select->vars[i]))
        return STATUS(NotSupported, "Grouped SELECT projection requires a group variable");
      continue;
    }
    const bool direct_count = dynamic_cast<const SPARQL::ExprAggCount*>(expr.get()) ||
        dynamic_cast<const SPARQL::ExprAggCountAll*>(expr.get());
    if (!direct_count && !aggregate && !grouped)
      return STATUS(NotSupported, "Non-aggregate SELECT expression remains at coordinator");
    if (available.count(select->vars[i]))
      return STATUS(NotSupported, "SELECT expression alias is already in scope");
    bool numeric;
    RETURN_NOT_OK(ValidateProjection(*expr, available, numeric_vars, &projection_budget,
                                    &numeric, true, !direct_count));
    available.insert(select->vars[i]);
    if (numeric) numeric_vars.insert(select->vars[i]);
  }
  if (having_budget.aggregates + projection_budget.hidden_aggregates > 16)
    return STATUS(NotSupported, "Global private aggregate limit exceeded");
  if (aggregate || grouped) {
    for (auto var : order_variables) if (!final_variables.count(var))
      return STATUS(NotSupported, "Non-grouped ORDER BY variable remains at coordinator");
  }
  std::vector<SPARQL::OpTriple> triples;
  RETURN_NOT_OK(Collect(*input, &triples));
  if (triples.empty() || triples.size() > 32 || select->vars.empty())
    return STATUS(NotSupported, "Unsupported MDB BGP size");
  FragmentPlan plan;
  // The constructor creates one private binding slot for each HAVING COUNT.
  // Account for them before starting workers, without mutating the parser tree.
  const uint64_t variables = get_query_ctx().get_var_size() + having_budget.aggregates +
      projection_budget.hidden_aggregates;
  if (variables > 128) return STATUS(NotSupported, "Worker variable limit exceeded");
  // Each COUNT(DISTINCT *) retains a full binding tuple. Account for every
  // such set, not just the number of returned columns or aggregate outputs.
  // Group sorting retains its input while final sorting materializes the group
  // outputs (at most one per input row). Charge explicit keys as well, including
  // repeated keys that do not increase the query's variable count.
  plan.global_row_reservation = 1024 + 128 * variables +
      16 * (projection_budget.distinct_tuple_sets + having_budget.distinct_tuple_sets) * variables +
      16 * order_keys + 64 * having_budget.nodes + 512 * projection_budget.nodes + 512 * having_budget.numeric_aggregates;
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
  // Only plain bound-variable ordering may prune before the original finalizer.
  // DISTINCT/groups/expressions must still see the full multiset.
  const auto* top_order = dynamic_cast<const SPARQL::OpOrderBy*>(select->op.get());
  if (top_order && !top_order->items.empty() && !select->distinct && !aggregate && !grouped &&
      !having && select->limit != SPARQL::Op::DEFAULT_LIMIT &&
      std::none_of(select->vars_exprs.begin(), select->vars_exprs.end(),
                   [](const auto& expr) { return bool(expr); }) &&
      (!select->limit || select->offset <= std::numeric_limits<uint64_t>::max() - select->limit)) {
    GlobalTopKSpec spec;
    spec.rows = select->limit ? select->limit + select->offset : 0;
    // Add heap row storage to the unchanged downstream finalizer reservation.
    // One additional slot admits an incoming candidate while the heap is full.
    spec.row_reservation = plan.global_row_reservation +
        2 * sizeof(::ObjectId) * physical.projection_size() + 2 * sizeof(std::vector<::ObjectId>);
    for (size_t i = 0; i < top_order->items.size(); ++i) {
      const auto var = std::get<VarId>(top_order->items[i]).id;
      const auto column = std::find(physical.projection().begin(), physical.projection().end(), var);
      if (column == physical.projection().end()) break;
      spec.key_columns.push_back(static_cast<uint32_t>(column - physical.projection().begin()));
      spec.ascending.push_back(i < top_order->ascending_order.size() ? top_order->ascending_order[i] : true);
    }
    if (spec.key_columns.size() == top_order->items.size() &&
        spec.rows < options.coordinator_memory_bytes / spec.row_reservation)
      plan.topk = std::move(spec);
  }
  // Removing duplicate projected tuples is safe only when every downstream
  // ordering key depends on that tuple; hidden keys need all representatives.
  if (select->distinct && !aggregate && !grouped && !having &&
      std::all_of(select->vars.begin(), select->vars.end(),
                  [&](auto var) { return bound.count(var.id) != 0; }) &&
      std::none_of(select->vars_exprs.begin(), select->vars_exprs.end(),
                   [](const auto& expr) { return bool(expr); }) &&
      std::all_of(order_variables.begin(), order_variables.end(), [&](auto var) {
        return std::find(select->vars.begin(), select->vars.end(), var) != select->vars.end();
      })) {
    GlobalDistinctSpec spec;
    for (auto var : select->vars) {
      const auto column = std::find(physical.projection().begin(), physical.projection().end(), var.id);
      spec.key_columns.push_back(static_cast<uint32_t>(column - physical.projection().begin()));
    }
    // Charge map keys/full representatives, capacity slack, an incoming tuple,
    // and the unchanged original finalizer's per-retained-row reservation.
    spec.row_reservation = plan.global_row_reservation + 128 +
        2 * sizeof(std::vector<::ObjectId>) +
        2 * sizeof(::ObjectId) * (physical.projection_size() + spec.key_columns.size());
    plan.distinct = std::move(spec);
  }
  // Only direct, non-DISTINCT counts may replace the original aggregate tree.
  // HAVING and computed extensions must still receive the complete input bag.
  if (aggregate && !having) {
    GlobalCountSpec spec;
    spec.grouped = grouped;
    bool eligible = true;
    const SPARQL::Op* envelope = select->op.get();
    if (auto* order = dynamic_cast<const SPARQL::OpOrderBy*>(envelope)) envelope = order->op.get();
    if (grouped) {
      auto* group = dynamic_cast<const SPARQL::OpGroupBy*>(envelope);
      eligible = group && group->op.get() == input;
    } else eligible = envelope == input;
    auto column = [&](VarId var) -> int32_t {
      const auto found = std::find(physical.projection().begin(), physical.projection().end(), var.id);
      return found == physical.projection().end() ? -1 : found - physical.projection().begin();
    };
    for (auto var : group_variables) {
      spec.group_variables.push_back(var.id);
      spec.group_columns.push_back(column(var));
    }
    for (size_t i = 0; i < select->vars_exprs.size(); ++i) {
      const auto* expr = select->vars_exprs[i].get();
      if (!expr) continue;
      int32_t arg = -1;
      if (auto* all = dynamic_cast<const SPARQL::ExprAggCountAll*>(expr)) {
        if (all->distinct) { eligible = false; break; }
        arg = -2;
      } else if (auto* count = dynamic_cast<const SPARQL::ExprAggCount*>(expr)) {
        const auto* var = dynamic_cast<const SPARQL::ExprVar*>(count->expr.get());
        if (count->distinct || !var) { eligible = false; break; }
        arg = column(var->var);
      } else { eligible = false; break; }
      spec.count_variables.push_back(select->vars[i].id);
      spec.count_columns.push_back(arg);
    }
    // Map keys/counters, incoming key and final Values/order/distinct storage.
    spec.row_reservation = plan.global_row_reservation + 256 +
        4 * sizeof(std::vector<::ObjectId>) +
        2 * sizeof(::ObjectId) * (physical.projection_size() +
            spec.group_variables.size() + spec.count_variables.size());
    if (eligible && !spec.count_variables.empty()) plan.count = std::move(spec);
  }
  if (options.exchange != ExchangeMode::None) RETURN_NOT_OK(ValidateExchange(physical, options));
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
    else if (auto* having = dynamic_cast<SPARQL::OpHaving*>(slot->get())) slot = &having->op;
    else if (auto* group = dynamic_cast<SPARQL::OpGroupBy*>(slot->get())) slot = &group->op;
    else return slot;
  }
}

}  // namespace yb::mdb

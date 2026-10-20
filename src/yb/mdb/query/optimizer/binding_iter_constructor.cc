#include <cassert>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <variant>
#include <vector>

#include "graph_models/rdf_model/rdf_model.h"
#include "graph_models/rdf_model/comparisons.h"
#include "query/executor/binding_iter.h"
#include "query/executor/binding_iter/aggregation/agg.h"
#include "query/executor/binding_iter/aggregation/aggregation.h"
#include "query/executor/binding_iter/aggregation/hybrid_aggregation.h"
#include "query/executor/binding_iter/bind.h"
#include "query/executor/binding_iter/binding_expr/binding_expr.h"
#include "query/executor/binding_iter/binding_expr/binding_expr_var.h"
#include "query/executor/binding_iter/cross_product.h"
#include "query/executor/binding_iter/distinct_hash.h"
#include "query/executor/binding_iter/empty_binding_iter.h"
#include "query/executor/binding_iter/expr_evaluator.h"
#include "query/executor/binding_iter/filter.h"
#include "query/executor/binding_iter/index_left_outer_join.h"
#include "query/executor/binding_iter/index_nested_loop_join.h"
#include "query/executor/binding_iter/leapfrog_join.h"
#include "query/executor/binding_iter/minus.h"
#include "query/executor/binding_iter/nested_loop_join.h"
#include "query/executor/binding_iter/nested_loop_left_join.h"
#include "query/executor/binding_iter/nested_loop_semi_join.h"
#include "query/executor/binding_iter/no_free_variable_minus.h"
#include "query/executor/binding_iter/not_exists.h"
#include "query/executor/binding_iter/order_by.h"
#include "query/executor/binding_iter/procedure/hnsw_scan.h"
#include "query/executor/binding_iter/procedure/hnsw_top_k.h"
#include "query/executor/binding_iter/procedure/text_search_multiscan.h"
#include "query/executor/binding_iter/set_end_boundary_variable.h"
#include "query/executor/binding_iter/single_result_binding_iter.h"
#include "query/executor/binding_iter/slice.h"
#include "query/executor/binding_iter/sparql_service.h"
#include "query/executor/binding_iter/sub_select.h"
#include "query/executor/binding_iter/union.h"
#include "query/executor/binding_iter/values.h"
#include "query/optimizer/plan/join_order/greedy_optimizer.h"
#include "query/optimizer/rdf_model/expr_to_binding_expr.h"
#include "query/optimizer/rdf_model/plan/path_plan.h"
#include "query/optimizer/rdf_model/plan/triple_plan.h"
#include "query/optimizer/binding_iter_constructor.h"
#include "query/parser/expr/sparql/atom/expr_var.h"
#include "query/parser/expr/sparql/expr.h"
#include "query/parser/op/sparql/op.h"
#include "query/parser/op/sparql/op_ask.h"
#include "query/parser/op/sparql/op_basic_graph_pattern.h"
#include "query/parser/op/sparql/op_bind.h"
#include "query/parser/op/sparql/op_construct.h"
#include "query/parser/op/sparql/op_describe.h"
#include "query/parser/op/sparql/op_empty.h"
#include "query/parser/op/sparql/op_filter.h"
#include "query/parser/op/sparql/op_group_by.h"
#include "query/parser/op/sparql/op_having.h"
#include "query/parser/op/sparql/op_minus.h"
#include "query/parser/op/sparql/op_not_exists.h"
#include "query/parser/op/sparql/op_optional.h"
#include "query/parser/op/sparql/op_order_by.h"
#include "query/parser/op/sparql/op_procedure.h"
#include "query/parser/op/sparql/op_select.h"
#include "query/parser/op/sparql/op_semi_join.h"
#include "query/parser/op/sparql/op_sequence.h"
#include "query/parser/op/sparql/op_service.h"
#include "query/parser/op/sparql/op_union.h"
#include "query/parser/op/sparql/op_unit_table.h"
#include "query/parser/op/sparql/op_values.h"
#include "query/var_id.h"

using namespace SPARQL;

namespace {

ObjectId to_boolean(ObjectId oid) {
    if (oid.is_true()) return ObjectId(ObjectId::BOOL_TRUE);
    return ObjectId(ObjectId::BOOL_FALSE);
}

}

BindingIterConstructor::BindingIterConstructor() = default;

BindingIterConstructor::~BindingIterConstructor() = default;

void BindingIterConstructor::print_set(const std::set<VarId>& set) const {
    std::cerr << "{";
    for (auto it = set.begin(); it != set.end(); ++it) {
        if (it != set.begin()) std::cerr << ", ";
        std::cerr << *it;
    }
    std::cerr << "}";
}

bool BindingIterConstructor::is_aggregation_or_group_var(VarId var_id) const {
    return group_vars.count(var_id) || aggregations.count(var_id);
}

void BindingIterConstructor::visit(OpSelect& op_select) {
    for (auto& expr : op_select.vars_exprs) {
        if (expr != nullptr && expr->has_aggregation()) {
            grouping = true;
            break;
        }
    }
    op_select.op->accept_visitor(*this);
    handle_select(op_select);
}

void BindingIterConstructor::visit(OpAsk& op_ask) {
    op_ask.op->accept_visitor(*this);
}

void BindingIterConstructor::visit(OpDescribe& op_describe) {
    if (op_describe.op) {
        op_describe.op->accept_visitor(*this);
    } else {
        tmp = std::make_unique<EmptyBindingIter>();
    }

    std::set<VarId> describe_vars(op_describe.vars.begin(), op_describe.vars.end());
    std::set<VarId> scope_vars;
    if (op_describe.op) {
        scope_vars = op_describe.op->get_scope_vars();
    }

    projection_vars.clear();
    projection_vars.reserve(describe_vars.size());
    for (auto& var : describe_vars) {
        projection_vars.push_back(var);
    }

    safe_assigned_vars = scope_vars;
    for (auto& var : describe_vars) {
        safe_assigned_vars.erase(var);
    }

    uint64_t describe_offset = op_describe.offset;
    uint64_t describe_limit = op_describe.limit;

    if (describe_offset != 0 || describe_limit != 0) {
        tmp = std::make_unique<Slice>(std::move(tmp), describe_offset, describe_limit);
    }

    tmp = std::make_unique<SubSelect>(std::move(tmp), std::move(projection_vars), std::vector<VarId>());
}

void BindingIterConstructor::visit(OpConstruct& op_construct) {
    op_construct.op->accept_visitor(*this);

    uint64_t construct_offset = op_construct.offset;
    uint64_t construct_limit = op_construct.limit;

    if (construct_offset != 0 || construct_limit != 0) {
        tmp = std::make_unique<Slice>(std::move(tmp), construct_offset, construct_limit);
    }
}

void BindingIterConstructor::visit(OpBasicGraphPattern& op_bgp) {
    std::vector<std::unique_ptr<Plan>> base_plans;
    for (auto& triple : op_bgp.triples) {
        base_plans.push_back(std::make_unique<TriplePlan>(triple.subject, triple.predicate, triple.object));
    }
    for (auto& path : op_bgp.paths) {
        base_plans.push_back(std::make_unique<PathPlan>(path.var, path.subject, path.object, path.path->clone()));
    }

    assert(!base_plans.empty());

    if (!safe_assigned_vars.empty()) {
        for (auto& plan : base_plans) {
            plan->set_input_vars(safe_assigned_vars);
        }
    }

    auto plan = GreedyOptimizer::get_plan(base_plans);
    auto plan_vars = plan->get_vars();
    tmp = plan->get_binding_iter();
    safe_assigned_vars.insert(plan_vars.begin(), plan_vars.end());
}

void BindingIterConstructor::visit(OpProcedure& op_procedure) {
    std::vector<std::unique_ptr<BindingExpr>> arg_binding_exprs;
    arg_binding_exprs.reserve(op_procedure.argument_exprs.size());
    for (auto& arg : op_procedure.argument_exprs) {
        ExprToBindingExpr visitor;
        arg->accept_visitor(visitor);
        arg_binding_exprs.push_back(std::move(visitor.tmp));
    }

    std::unique_ptr<BindingIter> proc_iter;
    static HNSW::HNSWIndexManager dummy_hnsw_mgr;
    static TextSearch::TextIndexManager dummy_text_mgr;

    std::vector<VarId> proc_binding_vars = op_procedure.binding_vars;

    switch (op_procedure.procedure_type) {
    case OpProcedure::ProcedureType::HNSW_TOP_K:
        proc_iter = std::make_unique<Procedure::HNSWTopK>(
            std::move(arg_binding_exprs),
            std::move(proc_binding_vars),
            dummy_hnsw_mgr,
            nullptr,
            nullptr);
        break;
    case OpProcedure::ProcedureType::HNSW_SCAN:
        proc_iter = std::make_unique<Procedure::HNSWScan>(
            std::move(arg_binding_exprs),
            std::move(proc_binding_vars),
            dummy_hnsw_mgr,
            nullptr,
            nullptr);
        break;
    case OpProcedure::ProcedureType::TEXT_SEARCH:
        proc_iter = std::make_unique<Procedure::TextSearchMultiScan>(
            std::move(arg_binding_exprs),
            std::move(proc_binding_vars),
            dummy_text_mgr,
            nullptr);
        break;
    default:
        throw std::runtime_error("Unknown procedure type");
    }

    safe_assigned_vars.insert(op_procedure.binding_vars.begin(), op_procedure.binding_vars.end());

    if (tmp != nullptr) {
        tmp = std::make_unique<CrossProduct>(std::move(tmp), std::move(proc_iter));
    } else {
        tmp = std::move(proc_iter);
    }
}

void BindingIterConstructor::visit(OpFilter& op_filter) {
    op_filter.op->accept_visitor(*this);

    for (auto& filter_expr : op_filter.filters) {
        ExprToBindingExpr visitor;
        filter_expr->accept_visitor(visitor);
        std::vector<std::unique_ptr<BindingExpr>> filters_vec;
        filters_vec.push_back(std::move(visitor.tmp));
        tmp = std::make_unique<Filter>(to_boolean, std::move(tmp), std::move(filters_vec));
    }
}

void BindingIterConstructor::visit(OpSequence& op_sequence) {
    for (auto& op : op_sequence.ops) {
        op->accept_visitor(*this);
    }
}

void BindingIterConstructor::visit(OpUnion& op_union) {
    std::vector<std::unique_ptr<BindingIter>> union_iters;

    for (auto& child_op : op_union.unions) {
        BindingIterConstructor child_constructor;
        child_constructor.safe_assigned_vars = safe_assigned_vars;
        child_op->accept_visitor(child_constructor);
        union_iters.push_back(std::move(child_constructor.tmp));
    }

    auto scope_vars = op_union.unions[0]->get_scope_vars();
    safe_assigned_vars.insert(scope_vars.begin(), scope_vars.end());

    tmp = std::make_unique<Union>(std::move(union_iters));
}

void BindingIterConstructor::visit(OpOptional& op_optional) {
    BindingIterConstructor lhs_constructor;
    lhs_constructor.safe_assigned_vars = safe_assigned_vars;
    op_optional.lhs->accept_visitor(lhs_constructor);

    auto join_vars = calculate_join_vars(*op_optional.lhs, *op_optional.rhs);

    BindingIterConstructor rhs_constructor;
    rhs_constructor.safe_assigned_vars = join_vars.parent_safe_vars;
    for (auto& var : join_vars.safe_join_vars) {
        rhs_constructor.safe_assigned_vars.insert(var);
    }
    for (auto& var : join_vars.rhs_fixable_vars) {
        rhs_constructor.safe_assigned_vars.insert(var);
    }
    op_optional.rhs->accept_visitor(rhs_constructor);

    safe_assigned_vars = join_vars.after_left_safe_vars;
    safe_assigned_vars.insert(join_vars.rhs_only_vars.begin(), join_vars.rhs_only_vars.end());

    std::vector<VarId> safe_join_vars_vec(join_vars.safe_join_vars.begin(), join_vars.safe_join_vars.end());
    std::vector<VarId> unsafe_join_vars_vec(join_vars.unsafe_join_vars.begin(), join_vars.unsafe_join_vars.end());
    std::vector<VarId> parent_safe_vars_vec(join_vars.parent_safe_vars.begin(), join_vars.parent_safe_vars.end());
    std::vector<VarId> lhs_only_vars_vec(join_vars.lhs_only_vars.begin(), join_vars.lhs_only_vars.end());
    std::vector<VarId> rhs_only_vars_vec(join_vars.rhs_only_vars.begin(), join_vars.rhs_only_vars.end());

    if (join_vars.safe_join_vars.empty() && join_vars.unsafe_join_vars.empty()) {
        tmp = std::make_unique<CrossProduct>(std::move(lhs_constructor.tmp), std::move(rhs_constructor.tmp));
    } else if (join_vars.unsafe_join_vars.empty()) {
        tmp = std::make_unique<IndexLeftOuterJoin>(
            std::move(lhs_constructor.tmp),
            std::move(rhs_constructor.tmp),
            std::move(rhs_only_vars_vec));
    } else {
        tmp = std::make_unique<NestedLoopLeftJoin>(
            std::move(lhs_constructor.tmp),
            std::move(rhs_constructor.tmp),
            std::move(safe_join_vars_vec),
            std::move(unsafe_join_vars_vec),
            std::move(parent_safe_vars_vec),
            std::move(lhs_only_vars_vec),
            std::move(rhs_only_vars_vec));
    }
}

void BindingIterConstructor::visit(OpMinus& op_minus) {
    BindingIterConstructor lhs_constructor;
    lhs_constructor.safe_assigned_vars = safe_assigned_vars;
    op_minus.lhs->accept_visitor(lhs_constructor);

    auto join_vars = calculate_join_vars(*op_minus.lhs, *op_minus.rhs);

    BindingIterConstructor rhs_constructor;
    rhs_constructor.safe_assigned_vars = join_vars.parent_safe_vars;
    for (auto& var : join_vars.safe_join_vars) {
        rhs_constructor.safe_assigned_vars.insert(var);
    }
    for (auto& var : join_vars.rhs_fixable_vars) {
        rhs_constructor.safe_assigned_vars.insert(var);
    }
    op_minus.rhs->accept_visitor(rhs_constructor);

    safe_assigned_vars = join_vars.parent_safe_vars;
    safe_assigned_vars.insert(join_vars.lhs_only_vars.begin(), join_vars.lhs_only_vars.end());

    std::vector<VarId> safe_join_vars_vec(join_vars.safe_join_vars.begin(), join_vars.safe_join_vars.end());
    std::vector<VarId> unsafe_join_vars_vec(join_vars.unsafe_join_vars.begin(), join_vars.unsafe_join_vars.end());
    std::vector<VarId> parent_safe_vars_vec(join_vars.parent_safe_vars.begin(), join_vars.parent_safe_vars.end());

    if (join_vars.unsafe_join_vars.empty()) {
        tmp = std::make_unique<NoFreeVariableMinus>(
            std::move(lhs_constructor.tmp),
            std::move(rhs_constructor.tmp));
    } else {
        tmp = std::make_unique<Minus>(
            std::move(lhs_constructor.tmp),
            std::move(rhs_constructor.tmp),
            std::move(safe_join_vars_vec),
            std::move(unsafe_join_vars_vec),
            std::move(parent_safe_vars_vec));
    }
}

void BindingIterConstructor::visit(OpNotExists& op_not_exists) {
    BindingIterConstructor lhs_constructor;
    lhs_constructor.safe_assigned_vars = safe_assigned_vars;
    op_not_exists.lhs->accept_visitor(lhs_constructor);

    auto join_vars = calculate_join_vars(*op_not_exists.lhs, *op_not_exists.rhs);

    BindingIterConstructor rhs_constructor;
    rhs_constructor.safe_assigned_vars = join_vars.parent_safe_vars;
    for (auto& var : join_vars.safe_join_vars) {
        rhs_constructor.safe_assigned_vars.insert(var);
    }
    op_not_exists.rhs->accept_visitor(rhs_constructor);

    safe_assigned_vars = join_vars.parent_safe_vars;
    safe_assigned_vars.insert(join_vars.lhs_only_vars.begin(), join_vars.lhs_only_vars.end());

    std::vector<VarId> unsafe_join_vars_vec(join_vars.unsafe_join_vars.begin(), join_vars.unsafe_join_vars.end());

    tmp = std::make_unique<NotExists>(
        std::move(lhs_constructor.tmp),
        std::move(rhs_constructor.tmp),
        std::move(unsafe_join_vars_vec));
}

void BindingIterConstructor::visit(OpSemiJoin& op_semi_join) {
    BindingIterConstructor lhs_constructor;
    lhs_constructor.safe_assigned_vars = safe_assigned_vars;
    op_semi_join.lhs->accept_visitor(lhs_constructor);

    auto join_vars = calculate_join_vars(*op_semi_join.lhs, *op_semi_join.rhs);

    BindingIterConstructor rhs_constructor;
    rhs_constructor.safe_assigned_vars = join_vars.parent_safe_vars;
    for (auto& var : join_vars.safe_join_vars) {
        rhs_constructor.safe_assigned_vars.insert(var);
    }
    op_semi_join.rhs->accept_visitor(rhs_constructor);

    safe_assigned_vars = join_vars.parent_safe_vars;
    safe_assigned_vars.insert(join_vars.lhs_only_vars.begin(), join_vars.lhs_only_vars.end());

    std::vector<VarId> unsafe_join_vars_vec(join_vars.unsafe_join_vars.begin(), join_vars.unsafe_join_vars.end());

    tmp = std::make_unique<NestedLoopSemiJoin>(
        std::move(lhs_constructor.tmp),
        std::move(rhs_constructor.tmp),
        std::move(unsafe_join_vars_vec));
}

void BindingIterConstructor::visit(OpBind& op_bind) {
    op_bind.op->accept_visitor(*this);

    ExprToBindingExpr visitor;
    op_bind.expr->accept_visitor(visitor);
    auto binding_expr = std::move(visitor.tmp);

    tmp = std::make_unique<Bind>(std::move(tmp), std::move(binding_expr), op_bind.var);
    safe_assigned_vars.insert(op_bind.var);
}

void BindingIterConstructor::visit(OpUnitTable&) {
    tmp = std::make_unique<SingleResultBindingIter>();
}

void BindingIterConstructor::visit(OpEmpty&) {
    tmp = std::make_unique<EmptyBindingIter>();
}

void BindingIterConstructor::visit(OpService& op_service) {
    op_service.op->accept_visitor(*this);

    auto scope_vars = op_service.op->get_scope_vars();
    auto fixable_vars = op_service.op->get_fixable_vars();

    std::set<VarId> fixed_join_vars;
    for (auto& var : fixable_vars) {
        if (safe_assigned_vars.count(var)) {
            fixed_join_vars.insert(var);
        }
    }

    tmp = std::make_unique<SparqlService>(
        op_service.silent,
        std::move(op_service.query),
        std::move(op_service.prefixes),
        op_service.var_or_iri,
        std::move(scope_vars),
        std::move(fixed_join_vars),
        std::set<VarId>());
}

void BindingIterConstructor::visit(OpValues& op_values) {
    std::vector<std::pair<VarId, bool>> vars_with_undef;
    vars_with_undef.reserve(op_values.vars.size());
    for (size_t i = 0; i < op_values.vars.size(); i++) {
        vars_with_undef.emplace_back(op_values.vars[i], op_values.has_undef[i]);
    }

    tmp = std::make_unique<Values>(std::move(vars_with_undef), std::move(op_values.values));

    for (size_t i = 0; i < op_values.vars.size(); i++) {
        if (!op_values.has_undef[i]) {
            safe_assigned_vars.insert(op_values.vars[i]);
        }
    }
}

void BindingIterConstructor::visit(OpGroupBy& op_group_by) {
    op_group_by.op->accept_visitor(*this);

    grouping = true;
    this->op_group_by = &op_group_by;

    for (auto& [expr, alias] : op_group_by.items) {
        if (alias.has_value()) {
            ExprToBindingExpr visitor(this, alias);
            expr->accept_visitor(visitor);
        } else {
            auto var_expr = dynamic_cast<ExprVar*>(expr.get());
            if (var_expr) {
                group_vars.insert(var_expr->var);
            }
        }
    }

    grouping = false;
}

void BindingIterConstructor::visit(OpOrderBy& op_order_by) {
    op_order_by.op->accept_visitor(*this);

    this->op_order_by = &op_order_by;

    // SELECT registers its aggregates in handle_select, after visiting its
    // child. Defer ordering until those aggregates have produced their values.
    // Non-grouped query forms retain their existing ordering location.
    if (!grouping && !op_group_by) {
        make_order_by_operator();
    }
}

void BindingIterConstructor::make_order_by_operator() {
    if (!op_order_by) return;
    auto& op_order_by = *this->op_order_by;

    std::vector<VarId> order_vars;
    std::vector<bool> ascending;

    for (std::size_t i = 0; i < op_order_by.items.size(); ++i) {
        if (i < op_order_by.ascending_order.size()) {
            ascending.push_back(op_order_by.ascending_order[i]);
        } else {
            ascending.push_back(true);
        }

        std::visit([&](auto&& item) {
            using T = std::decay_t<decltype(item)>;
            if constexpr (std::is_same_v<T, VarId>) {
                order_vars.push_back(item);
            } else {
                order_vars.push_back(VarId());
            }
        }, op_order_by.items[i]);
    }

    auto saved_vars = op_order_by.op->get_all_vars();
    std::set<VarId> saved_vars_set(saved_vars.begin(), saved_vars.end());

    tmp = std::make_unique<OrderBy>(
        std::move(tmp),
        std::move(saved_vars_set),
        std::move(order_vars),
        std::move(ascending),
        Comparisons::compare_for_order);
    this->op_order_by = nullptr;
}

void BindingIterConstructor::visit(OpHaving& op_having) {
    op_having.op->accept_visitor(*this);

    this->op_having = &op_having;

    for (auto& having_expr : op_having.exprs) {
        ExprToBindingExpr visitor;
        having_expr->accept_visitor(visitor);
        std::vector<std::unique_ptr<BindingExpr>> filters_vec;
        filters_vec.push_back(std::move(visitor.tmp));
        tmp = std::make_unique<Filter>(to_boolean, std::move(tmp), std::move(filters_vec));
    }
}

static void get_non_redundant_exprs(
    BindingIterConstructor& bic,
    const std::vector<std::pair<VarId, std::unique_ptr<BindingExpr>>>& proj_order_exprs,
    const std::set<VarId>& split_vars)
{
    for (auto& [var, expr] : proj_order_exprs) {
        bool redundant = false;

        auto var_expr = dynamic_cast<BindingExprVar*>(expr.get());
        if (var_expr && var_expr->var == var && !bic.aggregations.count(var)) {
            redundant = true;
        }

        if (redundant) {
            bic.group_vars.insert(var);
        }
    }
}

JoinVars BindingIterConstructor::calculate_join_vars(Op& lhs, Op& rhs) {
    JoinVars result;

    auto lhs_scope = lhs.get_scope_vars();
    auto rhs_scope = rhs.get_scope_vars();
    auto lhs_safe = lhs.get_safe_vars();
    auto rhs_safe = rhs.get_safe_vars();
    auto lhs_fixable = lhs.get_fixable_vars();
    auto rhs_fixable = rhs.get_fixable_vars();

    result.parent_safe_vars = safe_assigned_vars;

    for (auto& var : lhs_scope) {
        if (rhs_scope.count(var)) {
            result.common_vars.insert(var);
        }
    }

    for (auto& var : result.common_vars) {
        if (!result.parent_safe_vars.count(var)) {
            result.join_vars.insert(var);
        }
    }

    for (auto& var : lhs_scope) {
        if (!rhs_scope.count(var) && !result.parent_safe_vars.count(var)) {
            result.lhs_only_vars.insert(var);
        }
    }

    for (auto& var : rhs_scope) {
        if (!lhs_scope.count(var) && !result.parent_safe_vars.count(var)) {
            result.rhs_only_vars.insert(var);
        }
    }

    result.after_left_safe_vars = result.parent_safe_vars;
    result.after_left_safe_vars.insert(lhs_safe.begin(), lhs_safe.end());

    result.lhs_fixable_vars.clear();
    for (auto& var : lhs_fixable) {
        if (result.parent_safe_vars.count(var)) {
            result.lhs_fixable_vars.insert(var);
        }
    }

    result.rhs_fixable_vars.clear();
    for (auto& var : rhs_fixable) {
        if (result.after_left_safe_vars.count(var)) {
            result.rhs_fixable_vars.insert(var);
        }
    }

    for (auto& var : result.join_vars) {
        if (lhs_safe.count(var) && result.rhs_fixable_vars.count(var)) {
            result.safe_join_vars.insert(var);
        } else {
            result.unsafe_join_vars.insert(var);
        }
    }

    return result;
}

void BindingIterConstructor::make_solution_modifier_operators(
    bool is_root_query, bool distinct, uint64_t offset, uint64_t limit)
{
    if (op_group_by) {
        group_saved_vars.insert(group_vars.begin(), group_vars.end());
        if (op_having) {
            for (auto& expr : op_having->exprs) {
                auto vars = expr->get_all_vars();
                group_saved_vars.insert(vars.begin(), vars.end());
            }
        }
        if (op_order_by) {
            for (auto& item : op_order_by->items) {
                std::visit([&](auto&& item_var) {
                    using T = std::decay_t<decltype(item_var)>;
                    if constexpr (std::is_same_v<T, VarId>) {
                        group_saved_vars.insert(item_var);
                    }
                }, item);
            }
        }

        std::vector<VarId> group_vars_vector;
        for (auto& var : group_vars) {
            group_vars_vector.push_back(var);
        }
        std::vector<bool> ascending(group_vars_vector.size(), true);

        tmp = std::make_unique<OrderBy>(
            std::move(tmp),
            std::move(group_saved_vars),
            std::move(group_vars_vector),
            std::move(ascending),
            &OrderBy::internal_compare
        );

        op_group_by = nullptr;
    }

    if (!aggregations.empty() || !group_vars.empty()) {
        tmp = std::make_unique<Aggregation>(
            std::move(tmp),
            std::move(aggregations),
            std::move(group_vars));
    }

    // ORDER BY sees the complete groups and COUNT aliases. DISTINCT must then
    // retain their order, and slicing applies to that final solution sequence.
    make_order_by_operator();

    if (distinct && !projection_vars.empty()) {
        tmp = std::make_unique<DistinctHash>(std::move(tmp), std::move(projection_vars));
    }

    if (offset > 0 || limit != Op::DEFAULT_LIMIT) {
        tmp = std::make_unique<Slice>(std::move(tmp), offset, limit);
    }

    if (!is_root_query) {
        std::vector<VarId> safe_v;
        safe_v.reserve(safe_assigned_vars.size());
        for (auto& var : safe_assigned_vars) {
            safe_v.push_back(var);
        }
        tmp = std::make_unique<SubSelect>(std::move(tmp), std::move(projection_vars), std::move(safe_v));
    }
}

void BindingIterConstructor::handle_select(OpSelect& op_select) {
    projection_vars.clear();
    if (!op_group_by) {
        group_vars.clear();
    }
    group_exprs.clear();
    projection_order_exprs.clear();

    for (auto& var : op_select.vars) {
        projection_vars.push_back(var);
    }

    bool has_group_by = (op_group_by != nullptr);

    if (!has_group_by) {
        for (auto& expr : op_select.vars_exprs) {
            if (expr != nullptr && expr->has_aggregation()) {
                has_group_by = true;
                break;
            }
        }
    }

    std::vector<std::pair<VarId, std::unique_ptr<BindingExpr>>> select_exprs;
    for (std::size_t i = 0; i < op_select.vars_exprs.size(); i++) {
        if (op_select.vars_exprs[i] != nullptr) {
            if (has_group_by && op_select.vars_exprs[i]->has_aggregation()) {
                ExprToBindingExpr visitor(this, op_select.vars[i]);
                grouping = true;
                op_select.vars_exprs[i]->accept_visitor(visitor);
                grouping = false;
                select_exprs.emplace_back(op_select.vars[i], std::move(visitor.tmp));
            } else {
                ExprToBindingExpr visitor;
                op_select.vars_exprs[i]->accept_visitor(visitor);
                select_exprs.emplace_back(op_select.vars[i], std::move(visitor.tmp));
            }
        }
    }

    if (has_group_by) {
        std::set<VarId> split_vars;
        for (auto& var : projection_vars) {
            if (var.id != 0) {
                split_vars.insert(var);
            }
        }
        projection_order_exprs = std::move(select_exprs);
        if (op_group_by) {
            get_non_redundant_exprs(*this, projection_order_exprs, split_vars);
        }

        safe_assigned_vars.insert(projection_vars.begin(), projection_vars.end());
    } else {
        auto scope_vars = op_select.op->get_scope_vars();
        safe_assigned_vars.insert(scope_vars.begin(), scope_vars.end());
        safe_assigned_vars.insert(projection_vars.begin(), projection_vars.end());
    }

    make_solution_modifier_operators(
        !op_select.is_sub_select,
        op_select.distinct,
        op_select.offset,
        op_select.limit);
}

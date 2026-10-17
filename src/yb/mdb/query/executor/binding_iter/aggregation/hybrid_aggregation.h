#pragma once
#include <map>
#include <memory>
#include <set>
#include "query/executor/binding_iter.h"
#include "query/executor/binding_iter/aggregation/agg.h"
class HybridAggregation : public BindingIter {
public:
    HybridAggregation(std::unique_ptr<BindingIter> child, std::map<VarId, std::unique_ptr<Agg>>&& aggregations, std::set<VarId>&& group_vars, std::set<VarId>&& order_saved_vars) :
        child(std::move(child)), aggregations(std::move(aggregations)), group_vars(std::move(group_vars)) { }
    void _begin(Binding& parent_binding) override { child->begin(parent_binding); }
    bool _next() override { return false; }
    void _reset() override { child->reset(); }
    void assign_nulls() override { child->assign_nulls(); }
    void print(std::ostream& os, int indent, bool stats) const override { os << std::string(indent, ' ') << "HybridAggregation\\n"; }
    std::unique_ptr<BindingIter> child;
    std::map<VarId, std::unique_ptr<Agg>> aggregations;
    std::set<VarId> group_vars;
};

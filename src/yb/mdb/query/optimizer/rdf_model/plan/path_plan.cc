#include "path_plan.h"

#include <algorithm>
#include <map>
#include <queue>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "graph_models/object_id.h"
#include "graph_models/rdf_model/conversions.h"
#include "graph_models/rdf_model/rdf_object_id.h"
#include "query/executor/binding.h"
#include "query/executor/binding_iter.h"
#include "query/executor/binding_iter/empty_binding_iter.h"
#include "query/executor/binding_iter/values.h"
#include "query/exceptions.h"
#include "query/parser/paths/path_atom.h"
#include "query/parser/paths/path_kleene_plus.h"
#include "query/parser/paths/path_kleene_star.h"
#include "yb/mdb/model/rdf_yb_model.h"

using namespace SPARQL;

namespace {

std::string extract_predicate(const RegularPathExpr* path) {
    switch (path->type()) {
    case PathType::PATH_ATOM: {
        auto* atom = static_cast<const PathAtom*>(path);
        return atom->atom;
    }
    case PathType::PATH_KLEENE_STAR: {
        auto* star = static_cast<const PathKleeneStar*>(path);
        return extract_predicate(star->path.get());
    }
    case PathType::PATH_KLEENE_PLUS: {
        auto* plus = static_cast<const PathKleenePlus*>(path);
        return extract_predicate(plus->path.get());
    }
    default:
        return "";
    }
}

bool is_kleene_star(const RegularPathExpr* path) {
    return path->type() == PathType::PATH_KLEENE_STAR;
}

class PathBindingIter : public BindingIter {
public:
    PathBindingIter(std::vector<std::pair<VarId, bool>>&& vars,
                    std::vector<ObjectId>&& values)
        : vars_(std::move(vars)), values_(std::move(values)) {
        num_cols_ = vars_.size();
    }

    void _begin(Binding& parent_binding) override {
        parent_binding_ = &parent_binding;
        current_row_ = 0;
    }

    bool _next() override {
        if (current_row_ * num_cols_ >= values_.size()) {
            return false;
        }
        size_t base = current_row_ * num_cols_;
        for (size_t i = 0; i < num_cols_; i++) {
            parent_binding_->add(vars_[i].first, values_[base + i]);
        }
        current_row_++;
        return true;
    }

    void _reset() override {
        current_row_ = 0;
    }

    void assign_nulls() override {
        for (size_t i = 0; i < num_cols_; i++) {
            parent_binding_->add(vars_[i].first, ObjectId::get_null());
        }
    }

    void print(std::ostream& os, int indent, bool) const override {
        os << std::string(indent, ' ') << "PathBindingIter("
           << (num_cols_ > 0 ? values_.size() / num_cols_ : 0) << " rows)\n";
    }

private:
    std::vector<std::pair<VarId, bool>> vars_;
    std::vector<ObjectId> values_;
    size_t num_cols_ = 0;
    size_t current_row_ = 0;
    Binding* parent_binding_ = nullptr;
};

} // namespace

PathPlan::PathPlan(VarId var, Id start, Id end, std::unique_ptr<RegularPathExpr> path) :
    var(var),
    start(start),
    end(end),
    path(std::move(path)),
    start_assigned(start.is_OID()),
    end_assigned(end.is_OID())
{ }

std::unique_ptr<Plan> PathPlan::clone() const {
    return std::make_unique<PathPlan>(var, start, end, path->clone());
}

double PathPlan::estimate_cost() const {
    return estimate_output_size();
}

double PathPlan::estimate_output_size() const {
    return 100.0;
}

std::set<VarId> PathPlan::get_vars() const {
    std::set<VarId> res;
    if (start.is_var() && !start_assigned) {
        res.insert(start.get_var());
    }
    if (end.is_var() && !end_assigned) {
        res.insert(end.get_var());
    }
    return res;
}

void PathPlan::set_input_vars(const std::set<VarId>& input_vars) {
    if (start.is_var()) {
        if (input_vars.find(start.get_var()) != input_vars.end()) {
            start_assigned = true;
        }
    }
    if (end.is_var()) {
        if (input_vars.find(end.get_var()) != input_vars.end()) {
            end_assigned = true;
        }
    }
}

static uint64_t to_gmap_id(uint64_t oid_val) {
    auto it = g_pack_to_gmap.find(oid_val);
    if (it != g_pack_to_gmap.end()) {
        return it->second;
    }
    return oid_val;
}

std::unique_ptr<BindingIter> PathPlan::get_binding_iter() const {
    std::string pred_str = extract_predicate(path.get());
    if (pred_str.empty()) {
        return std::make_unique<EmptyBindingIter>();
    }

    ObjectId pred_oid = SPARQL::Conversions::pack_iri(pred_str);
    auto pred_it = g_pack_to_gmap.find(pred_oid.id);
    if (pred_it == g_pack_to_gmap.end()) {
        return std::make_unique<EmptyBindingIter>();
    }
    uint64_t pred_gmap = pred_it->second;

    uint64_t start_gmap = 0;
    if (start_assigned && start.is_OID()) {
        start_gmap = to_gmap_id(start.get_OID().id);
    }

    uint64_t end_gmap = 0;
    if (end_assigned && end.is_OID()) {
        end_gmap = to_gmap_id(end.get_OID().id);
    }

    auto& model = yb::mdb::RdfYBModel::Instance();
    auto& cache = model.GetCache(0);

    std::unordered_map<uint64_t, std::vector<uint64_t>> adj;
    std::unordered_set<uint64_t> all_nodes;

    for (size_t i = 0; i < cache.Size(); i++) {
        uint64_t c0, c1, c2;
        cache.GetRowAt(i, &c0, &c1, &c2);
        if (c1 == pred_gmap) {
            adj[c0].push_back(c2);
            all_nodes.insert(c0);
            all_nodes.insert(c2);
        }
    }

    bool is_star = is_kleene_star(path.get());

    std::vector<uint64_t> start_nodes;
    if (start_assigned && start.is_OID()) {
        start_nodes.push_back(start_gmap);
        all_nodes.insert(start_gmap);
    } else {
        for (auto n : all_nodes) {
            if (adj.find(n) != adj.end()) {
                start_nodes.push_back(n);
            }
        }
        if (all_nodes.empty()) {
            return std::make_unique<EmptyBindingIter>();
        }
    }

    if (end_assigned && end.is_OID()) {
        all_nodes.insert(end_gmap);
    }

    struct PathResult {
        uint64_t s;
        uint64_t o;
    };
    std::vector<PathResult> results;

    for (auto src : start_nodes) {
        std::unordered_set<uint64_t> visited;
        std::queue<uint64_t> q;
        visited.insert(src);
        if (is_star) {
            results.push_back({src, src});
        }
        q.push(src);

        while (!q.empty()) {
            auto u = q.front();
            q.pop();
            auto it = adj.find(u);
            if (it == adj.end()) continue;
            for (auto v : it->second) {
                if (visited.find(v) == visited.end()) {
                    visited.insert(v);
                    results.push_back({src, v});
                    q.push(v);
                }
            }
        }
    }

    if (end_assigned && end.is_OID()) {
        std::vector<PathResult> filtered;
        for (auto& r : results) {
            if (r.o == end_gmap) {
                filtered.push_back(r);
            }
        }
        results = std::move(filtered);
    }

    std::vector<VarId> output_vars;
    if (start.is_var() && !start_assigned) {
        output_vars.push_back(start.get_var());
    }
    if (end.is_var() && !end_assigned) {
        output_vars.push_back(end.get_var());
    }

    if (output_vars.empty()) {
        if (!results.empty()) {
            return std::make_unique<Values>(
                std::vector<std::pair<VarId, bool>>(),
                std::vector<ObjectId>());
        }
        return std::make_unique<EmptyBindingIter>();
    }

    std::vector<ObjectId> result_values;
    for (auto& r : results) {
        for (auto& v : output_vars) {
            if (start.is_var() && v == start.get_var()) {
                auto it = g_gmap_to_pack.find(r.s);
                result_values.push_back(it != g_gmap_to_pack.end() ? it->second : ObjectId(r.s));
            } else if (end.is_var() && v == end.get_var()) {
                auto it = g_gmap_to_pack.find(r.o);
                result_values.push_back(it != g_gmap_to_pack.end() ? it->second : ObjectId(r.o));
            }
        }
    }

    std::vector<std::pair<VarId, bool>> vars_with_undef;
    for (auto& v : output_vars) {
        vars_with_undef.emplace_back(v, false);
    }

    if (result_values.empty()) {
        return std::make_unique<EmptyBindingIter>();
    }

    return std::make_unique<Values>(std::move(vars_with_undef), std::move(result_values));
}

bool PathPlan::get_leapfrog_iter(std::vector<std::unique_ptr<::LeapfrogIter>>&,
                                  std::vector<VarId>&,
                                  uint_fast32_t&) const {
    return false;
}

void PathPlan::print(std::ostream& os, int indent) const {
    os << std::string(indent, ' ');
    os << "PathPlan(" << var << ", " << start << ", " << end << ")\n";
}

int PathPlan::relation_size() const {
    int size = 0;
    if (!start_assigned) size++;
    if (!end_assigned) size++;
    return size;
}

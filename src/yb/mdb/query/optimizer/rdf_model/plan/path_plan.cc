#include "path_plan.h"

#include <algorithm>
#include <limits>
#include <set>
#include <vector>

#include "graph_models/rdf_model/conversions.h"
#include "graph_models/rdf_model/rdf_object_id.h"
#include "query/executor/binding_iter.h"
#include "query/exceptions.h"
#include "query/parser/paths/path_alternatives.h"
#include "query/parser/paths/path_atom.h"
#include "query/parser/paths/path_kleene_plus.h"
#include "query/parser/paths/path_kleene_star.h"
#include "query/parser/paths/path_negated_set.h"
#include "query/parser/paths/path_optional.h"
#include "query/parser/paths/path_sequence.h"
#include "yb/mdb/model/rdf_yb_model.h"
#include "yb/mdb/storage/docdb_read_context.h"
#include "yb/mdb/storage/docdb_scan_cursor.h"

using namespace SPARQL;

namespace {

class PathBindingIter : public BindingIter {
 public:
  PathBindingIter(Id start, Id end, bool start_assigned, bool end_assigned,
                  std::unique_ptr<RegularPathExpr> path)
      : start_(start), end_(end), start_assigned_(start_assigned), end_assigned_(end_assigned),
        path_(std::move(path)) {}

  void _begin(Binding& binding) override { binding_ = &binding; _reset(); }

  void _reset() override {
    context_ = yb::mdb::DocDBReadContextScope::Current();
    if (yb::mdb::RdfYBModel::Instance().IsConnected() && !context_)
      throw QueryException("Property path requires a query-owned read snapshot");
    budget_ = context_ ? context_->batch_options.hash_query_memory_bytes : 64 * 1024 * 1024;
    work_ = 0;
    starts_.clear(); ends_.clear(); source_ = position_ = 0;
    start_value_ = Endpoint(start_, start_assigned_);
    end_value_ = Endpoint(end_, end_assigned_);
    if (!start_value_.is_null()) starts_.push_back(start_value_);
    else {
      std::set<uint64_t> nodes;
      Scan(0, std::nullopt, std::nullopt, [&](const auto& row) {
        for (auto id : {row[0], row[2]}) {
          const auto packed = Decode(id);
          if (nodes.insert(packed.id).second) Charge();
        }
      });
      // An explicit target is also a candidate for a zero-length path,
      // even when that RDF term does not occur in the active graph.
      if (!end_value_.is_null() && nodes.insert(end_value_.id).second) Charge();
      for (auto id : nodes) starts_.emplace_back(id);
    }
  }

  bool _next() override {
    Check();
    for (;;) {
      while (position_ < ends_.size()) {
        const auto end = ends_[position_++];
        const auto start = starts_[source_ - 1];
        if (!end_value_.is_null() && end != end_value_) continue;
        if (start_.is_var() && end_.is_var() && start_.get_var() == end_.get_var() && start != end)
          continue;
        if (start_.is_var() && start_value_.is_null()) binding_->add(start_.get_var(), start);
        if (end_.is_var() && end_value_.is_null()) binding_->add(end_.get_var(), end);
        return true;
      }
      if (source_ == starts_.size()) return false;
      work_ = starts_.size();
      ends_ = Evaluate(*path_, starts_[source_++], 0);
      position_ = 0;
    }
  }

  void assign_nulls() override {
    if (start_.is_var() && !start_assigned_) binding_->add(start_.get_var(), ObjectId::get_null());
    if (end_.is_var() && !end_assigned_) binding_->add(end_.get_var(), ObjectId::get_null());
  }

  void print(std::ostream& os, int indent, bool stats) const override {
    if (stats) print_generic_stats(os, indent);
    os << std::string(indent, ' ') << "PathBindingIter(" << path_->to_string() << ")\n";
  }

 private:
  ObjectId Endpoint(Id id, bool assigned) const {
    if (id.is_var()) return assigned ? (*binding_)[id.get_var()] : ObjectId::get_null();
    auto oid = id.get_OID();
    auto found = g_gmap_to_pack.find(oid.id);
    return found == g_gmap_to_pack.end() ? oid : found->second;
  }

  ObjectId Decode(uint64_t id) const {
    const auto found = g_gmap_to_pack.find(id);
    if (found != g_gmap_to_pack.end()) return found->second;
    if (context_) throw QueryException("Missing property path dictionary identity");
    return ObjectId(id);
  }

  void Check() const {
    if (get_query_ctx().cancellation_requested() || (context_ &&
        (context_->cancelled.load() || yb::CoarseMonoClock::Now() >= context_->deadline)))
      throw QueryException("Property path cancelled or expired");
  }

  void Charge(size_t count = 1) {
    Check();
    // A conservative per-source work/state bound covers vectors, sets and
    // recursive intermediates without retaining a process-global graph cache.
    if (count > budget_ / 128 || work_ > budget_ / 128 - count)
      throw QueryException("Property path work/state budget exceeded");
    work_ += count;
  }

  template<class Consumer>
  void Scan(int perm, std::optional<uint64_t> node, std::optional<uint64_t> predicate,
            Consumer consume) {
    auto& model = yb::mdb::RdfYBModel::Instance();
    if (!context_) {
      auto& cache = model.GetCache(perm);
      for (size_t i = 0; i < cache.Size(); ++i) {
        Check();
        uint64_t a, b, c;
        cache.GetRowAt(i, &a, &b, &c);
        if ((!node || a == *node) && (!predicate || b == *predicate))
          consume(yb::mdb::DocDBScanBatch::Row{a, b, c});
      }
      return;
    }
    yb::mdb::DocDBScanSpec spec;
    if (node) { spec.hash_key = *node; spec.columns[0] = {*node, *node}; }
    if (predicate) spec.columns[1] = {*predicate, *predicate};
    spec.page_rows = context_->page_rows;
    spec.page_bytes = context_->page_bytes;
    spec.deadline = context_->deadline;
    spec.cancelled = &context_->cancelled;
    yb::mdb::DocDBScanCursor cursor;
    auto status = cursor.Open(model.GetTable(perm), context_->session, std::move(spec));
    if (!status.ok()) throw QueryException(status.ToString());
    for (;;) {
      Check();
      const auto before = cursor.rpc_calls();
      const auto bytes = cursor.payload_bytes();
      const auto decoded = cursor.decoded_rows();
      auto batch = cursor.NextBatch();
      const auto operations = cursor.rpc_calls() - before;
      context_->scan_rpc_calls += operations;
      context_->stats.read_operations += operations;
      context_->stats.flush_rounds += operations != 0;
      context_->stats.response_payload_bytes += cursor.payload_bytes() - bytes;
      context_->stats.decoded_rows += cursor.decoded_rows() - decoded;
      if (!batch.ok()) throw QueryException(batch.status().ToString());
      for (const auto& row : batch->rows) consume(row);
      if (batch->eof) break;
    }
  }

  std::vector<ObjectId> Edges(ObjectId source, bool inverse, const std::string* iri,
                              const std::set<uint64_t>& excluded = {}) {
    std::vector<ObjectId> result;
    auto logical = g_pack_to_gmap.find(source.id);
    if (logical == g_pack_to_gmap.end()) return result;
    std::optional<uint64_t> predicate;
    if (iri) {
      auto found = g_pack_to_gmap.find(Conversions::pack_iri(*iri).id);
      if (found == g_pack_to_gmap.end()) return result;
      predicate = found->second;
    }
    Scan(inverse ? 5 : 0, logical->second, predicate, [&](const auto& row) {
      if (!excluded.count(row[1])) { Charge(); result.push_back(Decode(row[2])); }
    });
    return result;
  }

  std::vector<ObjectId> Evaluate(const RegularPathExpr& path, ObjectId source, size_t depth) {
    Charge();
    if (depth > 64) throw QueryException("Property path nesting limit exceeded");
    switch (path.type()) {
      case PathType::PATH_ATOM: {
        const auto& atom = static_cast<const PathAtom&>(path);
        if (!atom.property_checks.empty()) throw NotSupportedException("Path property checks");
        return Edges(source, atom.inverse, &atom.atom);
      }
      case PathType::PATH_ALTERNATIVES: {
        std::vector<ObjectId> result;
        for (const auto& child : static_cast<const PathAlternatives&>(path).alternatives) {
          auto part = Evaluate(*child, source, depth + 1);
          Charge(part.size());
          result.insert(result.end(), part.begin(), part.end());
        }
        return result;
      }
      case PathType::PATH_SEQUENCE: {
        std::vector<ObjectId> result{source};
        for (const auto& child : static_cast<const PathSequence&>(path).sequence) {
          std::vector<ObjectId> next;
          for (auto node : result) {
            auto part = Evaluate(*child, node, depth + 1);
            Charge(part.size());
            next.insert(next.end(), part.begin(), part.end());
          }
          result = std::move(next);
        }
        return result;
      }
      case PathType::PATH_OPTIONAL: {
        auto result = Evaluate(*static_cast<const PathOptional&>(path).path, source, depth + 1);
        Charge(); result.push_back(source);
        std::sort(result.begin(), result.end(), [](auto a, auto b) { return a.id < b.id; });
        result.erase(std::unique(result.begin(), result.end()), result.end());
        return result;
      }
      case PathType::PATH_KLEENE_STAR:
      case PathType::PATH_KLEENE_PLUS: {
        const bool star = path.type() == PathType::PATH_KLEENE_STAR;
        const auto& child = star ? *static_cast<const PathKleeneStar&>(path).path :
                                  *static_cast<const PathKleenePlus&>(path).path;
        std::set<uint64_t> expanded{source.id}, reached;
        std::vector<ObjectId> queue{source}, result;
        if (star) { Charge(); reached.insert(source.id); result.push_back(source); }
        for (size_t i = 0; i < queue.size(); ++i) {
          for (auto node : Evaluate(child, queue[i], depth + 1)) {
            if (reached.insert(node.id).second) { Charge(); result.push_back(node); }
            if (expanded.insert(node.id).second) { Charge(); queue.push_back(node); }
          }
        }
        return result;
      }
      case PathType::PATH_NEGATED_SET: {
        const auto& atoms = static_cast<const PathNegatedSet&>(path).negated_set;
        std::vector<ObjectId> result;
        for (bool inverse : {false, true}) {
          bool enabled = false;
          std::set<uint64_t> excluded;
          for (const auto& atom : atoms) if (atom.inverse == inverse) {
            enabled = true;
            auto found = g_pack_to_gmap.find(Conversions::pack_iri(atom.atom).id);
            if (found != g_pack_to_gmap.end()) excluded.insert(found->second);
          }
          if (enabled) {
            auto part = Edges(source, inverse, nullptr, excluded);
            Charge(part.size()); result.insert(result.end(), part.begin(), part.end());
          }
        }
        std::sort(result.begin(), result.end(), [](auto a, auto b) { return a.id < b.id; });
        result.erase(std::unique(result.begin(), result.end()), result.end());
        return result;
      }
      default: throw NotSupportedException("Property path expression");
    }
  }

  Id start_, end_;
  bool start_assigned_, end_assigned_;
  std::unique_ptr<RegularPathExpr> path_;
  Binding* binding_ = nullptr;
  yb::mdb::DocDBReadContext* context_ = nullptr;
  ObjectId start_value_, end_value_;
  std::vector<ObjectId> starts_, ends_;
  size_t source_ = 0, position_ = 0, budget_ = 0, work_ = 0;
};

}  // namespace

PathPlan::PathPlan(VarId var, Id start, Id end, std::unique_ptr<RegularPathExpr> path)
    : var(var), start(start), end(end), path(std::move(path)),
      start_assigned(start.is_OID()), end_assigned(end.is_OID()) {}

std::unique_ptr<Plan> PathPlan::clone() const {
  auto result = std::make_unique<PathPlan>(var, start, end, path->clone());
  result->start_assigned = start_assigned;
  result->end_assigned = end_assigned;
  return result;
}
double PathPlan::estimate_cost() const { return estimate_output_size(); }
double PathPlan::estimate_output_size() const { return 100.0; }
std::set<VarId> PathPlan::get_vars() const {
  std::set<VarId> result;
  if (start.is_var() && !start_assigned) result.insert(start.get_var());
  if (end.is_var() && !end_assigned) result.insert(end.get_var());
  return result;
}
void PathPlan::set_input_vars(const std::set<VarId>& inputs) {
  if (start.is_var() && inputs.count(start.get_var())) start_assigned = true;
  if (end.is_var() && inputs.count(end.get_var())) end_assigned = true;
}
std::unique_ptr<BindingIter> PathPlan::get_binding_iter() const {
  return std::make_unique<PathBindingIter>(start, end, start_assigned, end_assigned, path->clone());
}
bool PathPlan::get_leapfrog_iter(std::vector<std::unique_ptr<::LeapfrogIter>>&,
                                std::vector<VarId>&, uint_fast32_t&) const { return false; }
void PathPlan::print(std::ostream& os, int indent) const {
  os << std::string(indent, ' ') << "PathPlan(" << var << ", " << start << ", " << end << ")\n";
}
int PathPlan::relation_size() const { return !start_assigned + !end_assigned; }

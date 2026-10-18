// Portions Copyright (c) YugabyteDB, Inc.

#include "yb/mdb/query/optimizer/rdf_model/plan/docdb_leapfrog_plan.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <map>
#include <string>
#include <tuple>
#include <utility>

#include "graph_models/rdf_model/rdf_object_id.h"
#include "query/executor/binding_iter/leapfrog_join.h"
#include "query/executor/binding_iter/scan_ranges/term.h"
#include "query/optimizer/rdf_model/plan/triple_plan.h"
#include "yb/client/table.h"
#include "yb/client/table_handle.h"
#include "yb/common/ql_protocol_util.h"
#include "yb/mdb/model/rdf_yb_model.h"
#include "yb/mdb/storage/docdb_key_encoder.h"
#include "yb/mdb/storage/leapfrog_docdb_iter.h"
#include "yb/util/flags.h"
#include "yb/util/logging.h"
#include "yb/util/sync_point.h"

DEFINE_NON_RUNTIME_bool(mdb_enable_leapfrog_join, false,
    "Use DocDB Leapfrog for supported unbound three-variable predicate triangles");

DEFINE_NON_RUNTIME_bool(mdb_profile_leapfrog, false,
    "Collect opt-in Leapfrog loading, build, join and output phase timings");

DEFINE_NON_RUNTIME_uint32(mdb_leapfrog_read_page_rows, 1024,
    "Rows per fixed-predicate Leapfrog read page, including tablet-owned scans (1..65536)");

DEFINE_NON_RUNTIME_uint64(mdb_leapfrog_reuse_bytes, 8 * 1024 * 1024,
    "Maximum retained tuple-vector bytes for Leapfrog predicate reuse per query; zero disables");

namespace SPARQL {
namespace {

using ProfileClock = std::chrono::steady_clock;
int64_t ElapsedNs(ProfileClock::time_point start) {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(ProfileClock::now() - start).count();
}

using yb::Status;
using yb::mdb::DocKeyEncoder;
using yb::mdb::LeapfrogDocDBIter;
using yb::mdb::RdfYBModel;

// Data is immutable after admission; each iterator owns its traversal positions.
class QueryReuseCache {
 public:
  using Key = std::tuple<std::string, int, uint64_t>;

  std::shared_ptr<const yb::mdb::DocDBRangeCache> Find(const Key& key) {
    const auto found = entries_.find(key);
    if (found == entries_.end()) {
      ++stats.misses;
      return nullptr;
    }
    ++stats.hits;
    return found->second;
  }

  std::shared_ptr<const yb::mdb::DocDBRangeCache> Admit(
      const Key& key, yb::mdb::DocDBRangeCache* rows) {
    const auto bytes = rows->AllocatedRowBytes();
    // Bound metadata even for empty relations. This limits retention, not loading/RSS.
    if (budget_ == 0 || entries_.size() >= 64 || bytes > budget_ - stats.retained_bytes) {
      ++stats.rejections;
      return nullptr;
    }
    auto data = std::make_shared<const yb::mdb::DocDBRangeCache>(std::move(*rows));
    entries_.emplace(key, data);
    ++stats.admissions;
    stats.retained_rows += data->Size();
    stats.retained_bytes += bytes;
    return data;
  }

  DocDBLeapfrogReuseStats stats;

 private:
  const uint64_t budget_ = FLAGS_mdb_leapfrog_reuse_bytes;
  std::map<Key, std::shared_ptr<const yb::mdb::DocDBRangeCache>> entries_;
};

thread_local QueryReuseCache* active_reuse_cache = nullptr;

bool SupportedScan(const TriplePlan& plan) {
  return plan.predicate.is_OID() && plan.subject.is_var() && plan.object.is_var() &&
         !plan.subject_assigned && !plan.object_assigned && plan.subject != plan.object &&
         (plan.tablet_id_.empty() || (plan.tablet_perm_index_ >= 0 && plan.tablet_perm_index_ < 6));
}

std::vector<std::unique_ptr<ScanRange>> FixedPredicate(uint64_t predicate) {
  std::vector<std::unique_ptr<ScanRange>> ranges;
  ranges.push_back(std::make_unique<Term>(ObjectId(predicate)));
  return ranges;
}

class DocDBLeapfrogScan final : public LeapfrogDocDBIter {
 public:
  DocDBLeapfrogScan(const TriplePlan& plan, bool subject_first, std::vector<VarId> keys)
      : LeapfrogDocDBIter(
            &get_query_ctx().thread_info.interruption_requested,
            FixedPredicate(StoragePredicate(plan)), std::move(keys), {}),
        predicate_(StoragePredicate(plan)),
        target_perm_(subject_first ? DocKeyEncoder::kPSO : DocKeyEncoder::kPOS),
        source_perm_(plan.tablet_id_.empty() ? target_perm_ : plan.tablet_perm_index_),
        tablet_id_(plan.tablet_id_) {}

  const DocDBLeapfrogProfile& profile() const { return profile_; }

  bool open_terms(Binding& input) override {
    profile_ = {};
    if (interruption_requested && *interruption_requested) {
      set_status(STATUS(Aborted, "DocDB Leapfrog loading interrupted"));
    }
    if (status().ok() && !loaded_) {
      range_cache().Clear();
      const auto start = profiling_ ? ProfileClock::now() : ProfileClock::time_point{};
      set_status(Load());
      if (profiling_) {
        profile_.load_ns = ElapsedNs(start);
        profile_.loads = cache_hit_ ? 0 : 1;
        profile_.loaded_rows = cache_hit_ ? 0 : read_cache().Size();
      }
      loaded_ = status().ok();
      if (!loaded_) range_cache().Clear();
    }
    return LeapfrogDocDBIter::open_terms(input);
  }

  yb::Result<bool> open_for_estimation(Binding& input) override {
    RETURN_NOT_OK(status());
    if (!loaded_) return STATUS(NotSupported, "DocDB Leapfrog cache has not been loaded");
    return LeapfrogDocDBIter::open_for_estimation(input);
  }

  std::string get_iter_name() const override { return "DocDBLeapfrogScan"; }

 private:
  Status AddRow(const yb::qlexpr::QLRow& row) {
    if (!profiling_) return AddRowImpl(row);
    const auto start = ProfileClock::now();
    const auto status = AddRowImpl(row);
    profile_.build_ns += ElapsedNs(start);
    ++profile_.scanned_rows;
    return status;
  }

  Status AddRowImpl(const yb::qlexpr::QLRow& row) {
    if (interruption_requested && *interruption_requested) {
      return STATUS(Aborted, "DocDB Leapfrog loading interrupted");
    }
    uint64_t s, p, o, c0, c1, c2;
    DocKeyEncoder::ReversePermute(
        source_perm_, row.column(0).int64_value(), row.column(1).int64_value(),
        row.column(2).int64_value(), &s, &p, &o);
    if (p == predicate_) {
      DocKeyEncoder::Permute(target_perm_, s, p, o, &c0, &c1, &c2);
      range_cache().InsertRow(c0, c1, c2);
    }
    return Status::OK();
  }

  Status Load() {
    const auto page_rows = FLAGS_mdb_leapfrog_read_page_rows;
    if (page_rows == 0 || page_rows > 65536) {
      return STATUS(InvalidArgument, "mdb_leapfrog_read_page_rows must be in 1..65536");
    }
    auto& model = RdfYBModel::Instance();
    if (!model.IsConnected() || model.HasTransaction()) {
      return STATUS(IllegalState, "DocDB Leapfrog requires a connected read-only model");
    }
    const auto& table = *model.GetTable(source_perm_);
    if (!tablet_id_.empty()) {
      yb::client::TableIteratorOptions options;
      options.read_time = model.QueryReadTime();
      options.columns = {"c0", "c1", "c2"};
      options.tablet = tablet_id_;
      options.page_size = page_rows;
      // P occupies a different physical column in each source permutation.
      uint64_t c0, c1, c2;
      DocKeyEncoder::Permute(source_perm_, 0, 1, 0, &c0, &c1, &c2);
      const std::string predicate_column = c0 ? "c0" : (c1 ? "c1" : "c2");
      options.filter = [this, predicate_column](
                           const yb::client::TableHandle& handle, yb::LWQLConditionPB* condition) {
        handle.SetInt64Condition(
            condition, predicate_column, yb::QL_OP_EQUAL, static_cast<int64_t>(predicate_));
      };
      options.check_status = [this] {
        return interruption_requested && *interruption_requested
                   ? STATUS(Aborted, "DocDB Leapfrog loading interrupted")
                   : Status::OK();
      };
      options.error_handler = [this](const Status& error) { set_status(error); };
      for (const auto& row : yb::client::TableRange(table, options)) {
        RETURN_NOT_OK(AddRow(row));
      }
      return status();
    }
    const QueryReuseCache::Key key{table.table()->id(), target_perm_, predicate_};
    if (active_reuse_cache) {
      auto cached = active_reuse_cache->Find(key);
      if (cached) {
        UseSharedCache(std::move(cached));
        cache_hit_ = true;
        return Status::OK();
      }
    }
    // A fixed P hash key stays in its partition while every continuation is read.
    auto session = table.client()->NewSession(yb::MonoDelta::FromSeconds(60));
    auto op = table.NewReadOp(session->arena());
    if (model.QueryReadTime()) op->SetReadTime(model.QueryReadTime());
    auto* request = op->mutable_request();
    yb::QLAddInt64HashValue(request, static_cast<int64_t>(predicate_));
    request->set_return_paging_state(true);
    request->set_limit(page_rows);
    table.AddColumns({"c0", "c1", "c2"}, request);
    for (;;) {
      if (interruption_requested && *interruption_requested) {
        return STATUS(Aborted, "DocDB Leapfrog loading interrupted");
      }
      RETURN_NOT_OK(session->TEST_ApplyAndFlush(op));
      if (op->response().status() != yb::QLResponsePB::YQL_STATUS_OK) {
        return STATUS_FORMAT(RuntimeError, "DocDB Leapfrog read failed: $0", op->response());
      }
      const auto rows = VERIFY_RESULT(op->MakeRowBlock());
      for (const auto& row : rows.rows()) RETURN_NOT_OK(AddRow(row));
      const auto& response = op->response();
      if (!response.has_paging_state() || (response.paging_state().next_row_key().empty() &&
                                           response.paging_state().next_partition_key().empty()))
        break;
      *request->mutable_paging_state() = response.paging_state();
    }
    // Publish only complete successful loads, never a failed page or a partial cache.
    if (active_reuse_cache) {
      auto cached = active_reuse_cache->Admit(key, &range_cache());
      if (cached) UseSharedCache(std::move(cached));
    }
    return Status::OK();
  }

  static uint64_t StoragePredicate(const TriplePlan& plan) {
    const auto packed = plan.predicate.get_OID().id;
    const auto mapped = g_pack_to_gmap.find(packed);
    return mapped == g_pack_to_gmap.end() ? packed : mapped->second;
  }

  uint64_t predicate_;
  int target_perm_, source_perm_;
  std::string tablet_id_;
  bool loaded_ = false;
  bool cache_hit_ = false;
  const bool profiling_ = FLAGS_mdb_profile_leapfrog;
  DocDBLeapfrogProfile profile_;
};

// The trie compares storage IDs. Translate only at the boundary, preserving its binding.
class PackedLeapfrogTriangle final : public BindingIter {
 public:
  PackedLeapfrogTriangle(std::unique_ptr<LeapfrogJoin> child, std::vector<VarId> variables)
      : child_(std::move(child)), variables_(std::move(variables)) {}

  void _begin(Binding& input) override {
    parent_ = &input;
    if (!status().ok()) return;
    storage_binding_ = std::make_unique<Binding>(input.size);
    profile_ = {};
    profile_reported_ = false;
    const auto start = profiling_ ? ProfileClock::now() : ProfileClock::time_point{};
    child_->begin(*storage_binding_);
    set_status_from(*child_);
    if (profiling_) CaptureProfile(start);
    DocDBLeapfrogMemory memory;
    for (const auto& scan : child_->leapfrog_iters) {
      const auto& iter = static_cast<LeapfrogDocDBIter&>(*scan);
      if (!iter.uses_shared_cache()) {
        memory.private_rows += iter.read_cache().Size();
        memory.private_bytes += iter.read_cache().AllocatedRowBytes();
      }
    }
    if (active_reuse_cache) {
      memory.shared_rows = active_reuse_cache->stats.retained_rows;
      memory.shared_bytes = active_reuse_cache->stats.retained_bytes;
    }
    TEST_SYNC_POINT_CALLBACK("MDBLeapfrogTriangle::Materialized", &memory);
    LOG(INFO) << "MDB Leapfrog triangle: relations=3 status=" << status()
              << " cache_rows=" << memory.private_rows + memory.shared_rows
              << " row_capacity_bytes=" << memory.private_bytes + memory.shared_bytes;
  }

  bool _next() override {
    if (!status().ok() || parent_ == nullptr) return false;
    const auto start = profiling_ ? ProfileClock::now() : ProfileClock::time_point{};
    const bool found = child_->next();
    if (profiling_) profile_.join_ns += ElapsedNs(start);
    set_status_from(*child_);
    if (!found || !status().ok()) {
      ReportProfile();
      return false;
    }
    const auto output_start = profiling_ ? ProfileClock::now() : ProfileClock::time_point{};
    for (const auto var : variables_) {
      const auto id = (*storage_binding_)[var].id;
      const auto mapped = g_gmap_to_pack.find(id);
      parent_->add(var, mapped == g_gmap_to_pack.end() ? ObjectId(id) : mapped->second);
    }
    if (profiling_) profile_.output_ns += ElapsedNs(output_start);
    return true;
  }

  void _reset() override {
    if (!status().ok()) return;
    if (parent_ == nullptr) {
      set_status(STATUS(IllegalState, "DocDB Leapfrog reset before begin"));
      return;
    }
    profile_ = {};
    profile_reported_ = false;
    const auto start = profiling_ ? ProfileClock::now() : ProfileClock::time_point{};
    child_->reset();
    set_status_from(*child_);
    if (profiling_) CaptureProfile(start);
  }

  void assign_nulls() override {
    if (parent_ && status().ok()) {
      for (const auto var : variables_) parent_->add(var, ObjectId::get_null());
    }
  }

  void print(std::ostream& os, int indent, bool stats) const override {
    os << std::string(indent, ' ') << "DocDBLeapfrogTriangle\n";
    child_->print(os, indent + 2, stats);
  }

 private:
  void CaptureProfile(ProfileClock::time_point start) {
    const auto begin_ns = ElapsedNs(start);
    for (const auto& scan : child_->leapfrog_iters) {
      const auto& part = static_cast<DocDBLeapfrogScan&>(*scan).profile();
      profile_.load_ns += part.load_ns;
      profile_.build_ns += part.build_ns;
      profile_.loads += part.loads;
      profile_.scanned_rows += part.scanned_rows;
      profile_.loaded_rows += part.loaded_rows;
    }
    profile_.join_ns = begin_ns - profile_.load_ns;
  }

  void ReportProfile() {
    if (!profiling_ || profile_reported_) return;
    profile_reported_ = true;
    TEST_SYNC_POINT_CALLBACK("MDBLeapfrogTriangle::Phases", &profile_);
  }

  const bool profiling_ = FLAGS_mdb_profile_leapfrog;
  bool profile_reported_ = false;
  DocDBLeapfrogProfile profile_;
  std::unique_ptr<LeapfrogJoin> child_;
  std::vector<VarId> variables_;
  std::unique_ptr<Binding> storage_binding_;
  Binding* parent_ = nullptr;
};

}  // namespace

struct DocDBLeapfrogQueryScope::Impl {
  Impl() : previous(active_reuse_cache) { active_reuse_cache = &cache; }
  ~Impl() {
    active_reuse_cache = previous;
    TEST_SYNC_POINT_CALLBACK("MDBLeapfrogQuery::Reuse", &cache.stats);
  }

  QueryReuseCache cache;
  QueryReuseCache* previous;
};

DocDBLeapfrogQueryScope::DocDBLeapfrogQueryScope() : impl_(std::make_unique<Impl>()) {}
DocDBLeapfrogQueryScope::~DocDBLeapfrogQueryScope() = default;

bool AppendDocDBLeapfrogScan(
    const TriplePlan& plan, std::vector<std::unique_ptr<LeapfrogIter>>* scans,
    const std::vector<VarId>& order, uint_fast32_t enumeration_level) {
  if (!FLAGS_mdb_enable_leapfrog_join || !SupportedScan(plan) ||
      RdfYBModel::Instance().HasTransaction() || order.size() != 3 || enumeration_level != 3 ||
      std::set<VarId>(order.begin(), order.end()).size() != 3)
    return false;
  std::vector<VarId> keys;
  for (const auto var : order) {
    if (var == plan.subject.get_var() || var == plan.object.get_var()) keys.push_back(var);
  }
  if (keys.size() != 2) return false;
  scans->push_back(
      std::make_unique<DocDBLeapfrogScan>(plan, keys.front() == plan.subject.get_var(), keys));
  return true;
}

std::unique_ptr<BindingIter> TryBuildDocDBLeapfrogTriangle(
    const std::vector<std::unique_ptr<Plan>>& plans, const std::set<VarId>& input_vars) {
  if (!FLAGS_mdb_enable_leapfrog_join || plans.size() != 3 || !input_vars.empty() ||
      RdfYBModel::Instance().HasTransaction())
    return nullptr;
  std::map<VarId, size_t> degrees;
  std::set<std::set<VarId>> pairs;
  for (size_t i = 0; i < plans.size(); ++i) {
    const auto* triple = dynamic_cast<const TriplePlan*>(plans[i].get());
    if (!triple || !SupportedScan(*triple) || (i != 0 && !triple->tablet_id_.empty()))
      return nullptr;
    const auto vars = triple->get_vars();
    if (!pairs.insert(vars).second) return nullptr;
    for (const auto var : vars) ++degrees[var];
  }
  if (degrees.size() != 3) return nullptr;
  std::vector<VarId> order;
  for (const auto& [var, degree] : degrees) {
    if (degree != 2) return nullptr;
    order.push_back(var);
  }
  uint_fast32_t enumeration_level = order.size();
  std::vector<std::unique_ptr<LeapfrogIter>> scans;
  for (const auto& plan : plans) {
    if (!plan->get_leapfrog_iter(scans, order, enumeration_level)) return nullptr;
  }
  auto child = std::make_unique<LeapfrogJoin>(std::move(scans), std::vector<VarId>(order), 3);
  return std::make_unique<PackedLeapfrogTriangle>(std::move(child), std::move(order));
}

}  // namespace SPARQL

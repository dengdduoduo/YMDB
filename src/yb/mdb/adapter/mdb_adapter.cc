// Copyright (c) YugabyteDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.
//

#include "yb/mdb/adapter/mdb_adapter.h"
#include "yb/mdb/adapter/result_sink.h"
#include "yb/mdb/query/executor/wall_time_scope.h"
#include "query/parser/op/sparql/op_ask.h"
#include "query/parser/op/sparql/op_construct.h"

#include <algorithm>
#include <array>
#include <set>
#include <fstream>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "graph_models/rdf_model/conversions.h"
#include "graph_models/rdf_model/rdf_model.h"
#include "query/executor/binding.h"
#include "query/id.h"
#include "query/optimizer/binding_iter_constructor.h"
#include "query/parser/grammar/sparql/query_visitor.h"
#include "query/parser/sparql_query_parser.h"
#include "query/parser/op/sparql/op_basic_graph_pattern.h"
#include "query/parser/op/sparql/op_filter.h"
#include "query/parser/op/sparql/op_group_by.h"
#include "query/parser/op/sparql/op_join.h"
#include "query/parser/op/sparql/op_minus.h"
#include "query/parser/op/sparql/op_optional.h"
#include "query/parser/op/sparql/op_order_by.h"
#include "query/parser/op/sparql/op_select.h"
#include "query/parser/op/sparql/op_union.h"
#include "query/parser/op/sparql/op_values.h"
#include "query/parser/op/sparql/op_bind.h"
#include "query/parser/op/sparql/op_not_exists.h"
#include "query/parser/op/sparql/op_semi_join.h"
#include "query/parser/op/sparql/op_sequence.h"
#include "query/parser/op/sparql/op_having.h"
#include "query/parser/op/sparql/op_graph.h"
#include "query/parser/op/sparql/op_path.h"
#include "query/optimizer/rdf_model/expr_to_binding_expr.h"
#include "query/optimizer/rdf_model/plan/triple_plan.h"
#include "query/parser/op/sparql/op_describe.h"
#include "query/query_context.h"

#include "yb/client/client.h"
#include "yb/client/schema.h"
#include "yb/client/session.h"
#include "yb/client/table_handle.h"
#include "yb/client/yb_op.h"
#include "yb/client/yb_table_name.h"
#include "yb/common/ql_type.h"
#include "yb/mdb/import/import_pipeline.h"
#include "yb/mdb/import/insert_executor.h"
#include "yb/mdb/import/nt_parser.h"
#include "yb/mdb/import/rdf_term.h"
#include "yb/util/uuid.h"
#include "yb/mdb/model/rdf_yb_model.h"
#include "yb/mdb/storage/yb_client_compat.h"
#include "yb/mdb/storage/rdf_table_schema.h"
#include "yb/mdb/storage/docdb_read_context.h"
#include "yb/mdb/query/distributed/fragment_gather.h"
#include "yb/mdb/query/distributed/global_topk.h"
#include "yb/mdb/query/distributed/global_distinct.h"
#include "yb/mdb/query/distributed/global_count.h"
#include "yb/mdb/query/distributed/exchange.h"
#include "yb/server/hybrid_clock.h"
#include "yb/util/logging.h"
#include "yb/util/flags.h"
#include "yb/util/monotime.h"
#include "yb/util/status.h"
#include "yb/util/scope_exit.h"

using yb::MonoDelta;
using yb::Status;
using yb::YQL_DATABASE_CQL;
using namespace SPARQL;

DEFINE_test_flag(int32, mdb_pause_before_dictionary_mirror_ms, 0,
                 "Pause after imported data commits and before its legacy dictionary mirror");

static const char* kIdMapTableName = "rdf_idmap";

std::unordered_map<uint64_t, std::string> g_gmap_to_uri;
std::unordered_map<uint64_t, ObjectId> g_gmap_to_pack;
std::unordered_map<uint64_t, uint64_t> g_pack_to_gmap;

namespace {

// Parsing still touches shared term maps. Serialize the public adapter until
// those maps are query-owned too; callbacks must not re-enter this adapter.
std::mutex adapter_mutex;
thread_local bool adapter_entered = false;
thread_local std::string last_query_profile;

class AdapterScope {
 public:
  AdapterScope() : lock_(adapter_mutex, std::defer_lock) {
    if (!adapter_entered) {
      lock_.lock();
      adapter_entered = true;
    }
  }
  ~AdapterScope() {
    if (lock_.owns_lock()) adapter_entered = false;
  }
  bool entered() const { return lock_.owns_lock(); }

 private:
  std::unique_lock<std::mutex> lock_;
};

std::unique_ptr<yb::mdb::IdMap> g_id_map;

std::unique_ptr<yb::client::YBClient> g_yb_client;

scoped_refptr<yb::server::HybridClock> g_clock;

bool g_cluster_initialized = false;

std::string g_keyspace;

// Called only while AdapterScope owns the public adapter lock.
void ResetAdapter() {
  yb::mdb::RdfYBModel::Instance().ClearAll();
  g_cluster_initialized = false;
  g_id_map.reset();
  g_yb_client.reset();
  g_clock.reset();
  g_keyspace.clear();
  last_query_profile.clear();
}

std::string IdToString(uint64_t id) {
  auto graph = g_gmap_to_pack.find(id);
  if (graph != g_gmap_to_pack.end()) {
    id = graph->second.id;
  }
  const auto type = RDF_OID::get_generic_type(ObjectId(id));
  if (type == RDF_OID::GenericType::BLANK) return "_:mdb" + std::to_string(id);
  if (type == RDF_OID::GenericType::IRI) {
    std::string iri;
    if (!Conversions::try_unpack_iri(ObjectId(id), &iri)) {
      throw QueryException("Cannot resolve RDF result IRI");
    }
    return iri;
  }
  if (type != RDF_OID::GenericType::NULL_ID) {
    // An empty lexical form is a valid literal, not a missing dictionary entry.
    return Conversions::to_lexical_str(ObjectId(id));
  }
  return std::to_string(id);
}

Status CreateIdMapTable() {
  yb::client::YBTableName table_name(YQL_DATABASE_CQL, g_keyspace, kIdMapTableName);

  yb::client::YBSchemaBuilder builder;
  builder.AddColumn("pack_id")->Type(yb::DataType::INT64)->HashPrimaryKey()->NotNull();
  builder.AddColumn("gmap_id")->Type(yb::DataType::INT64)->NotNull();
  builder.AddColumn("uri")->Type(yb::DataType::STRING)->NotNull();

  yb::TableProperties table_properties;
  table_properties.SetTransactional(true);
  builder.SetTableProperties(table_properties);

  yb::client::TableHandle tmp;
  auto status = tmp.Create(table_name, 1, g_yb_client.get(), &builder);
  if (status.IsAlreadyPresent()) return Status::OK();
  return status;
}

Status SaveIdMapToDocDB() {
  if (g_id_map == nullptr) return Status::OK();

  yb::client::YBTableName table_name(YQL_DATABASE_CQL, g_keyspace, kIdMapTableName);
  yb::client::TableHandle table;
  RETURN_NOT_OK(table.Open(table_name, g_yb_client.get()));

  auto session = g_yb_client->NewSession(MonoDelta::FromSeconds(30));

  for (auto& [pack_id, gmap_id] : g_pack_to_gmap) {
    auto insert = yb::mdb::NewMdbInsertOp(table, session);
    auto* req = insert->mutable_request();
    QLAddInt64HashValue(req, static_cast<int64_t>(pack_id));
    table.AddInt64ColumnValue(req, "gmap_id", static_cast<int64_t>(gmap_id));
    auto term = g_id_map->Entries().find(gmap_id);
    if (term == g_id_map->Entries().end()) {
      return STATUS(Corruption, "Missing RDF term for persisted ID");
    }
    table.AddStringColumnValue(req, "uri", term->second);
    session->Apply(insert);
  }

  RETURN_NOT_OK(session->TEST_Flush());

  LOG(INFO) << "Saved " << g_pack_to_gmap.size() << " idmap entries";
  return Status::OK();
}

Status LoadIdMapFromDocDB() {
  yb::client::YBTableName table_name(YQL_DATABASE_CQL, g_keyspace, kIdMapTableName);
  yb::client::TableHandle table;
  auto open_status = table.Open(table_name, g_yb_client.get());
  if (!open_status.ok()) {
    return open_status;
  }

  g_pack_to_gmap.clear();
  g_gmap_to_uri.clear();
  g_gmap_to_pack.clear();

  yb::client::TableIteratorOptions options;
  options.columns = std::vector<std::string>{"pack_id", "gmap_id", "uri"};
  Status scan_status;
  options.error_handler = [&scan_status](const Status& s) { scan_status = s; };
  for (const auto& row : yb::client::TableRange(table, options)) {
    int64_t pack_id = row.column(0).int64_value();
    int64_t gmap_id = row.column(1).int64_value();
    if (row.column(2).IsNull()) {
      return STATUS(Corruption, "Missing persisted RDF term");
    }
    auto uri = row.column(2).string_value();
    if (gmap_id <= 0 || !g_id_map->Restore(static_cast<uint64_t>(gmap_id), uri)) {
      return STATUS(Corruption, "Conflicting persisted RDF term mapping");
    }
    try {
      auto term = yb::mdb::DecodeRdfDictionaryEntry(uri, ObjectId(static_cast<uint64_t>(pack_id)));
      yb::mdb::RegisterRdfIdentity(static_cast<uint64_t>(gmap_id), term);
    } catch (const std::exception& error) {
      return STATUS(Corruption, error.what());
    }
  }

  RETURN_NOT_OK(scan_status);
  RETURN_NOT_OK(yb::mdb::RdfYBModel::Instance().RestoreTermMap(*g_id_map));
  LOG(INFO) << "Loaded " << g_pack_to_gmap.size() << " idmap entries";
  return Status::OK();
}

void RebindOpId(Id& id) {
  if (!id.is_OID()) return;
  uint64_t oid = id.get_OID().id;
  auto it = g_pack_to_gmap.find(oid);
  if (it != g_pack_to_gmap.end()) {
    id = ObjectId(it->second);
  } else if (g_cluster_initialized || yb::mdb::RdfYBModel::Instance().UseStreamingReads()) {
    id = ObjectId::get_not_found();
  }
}

void RebindOpIds(std::unique_ptr<Op>& op);

void RebindOpTriple(OpTriple& triple) {
  RebindOpId(triple.subject);
  RebindOpId(triple.predicate);
  RebindOpId(triple.object);
}

void RebindOpPath(OpPath& path) {
  // Unknown endpoint terms still participate in zero-length paths.
  for (auto* id : {&path.subject, &path.object}) {
    if (!id->is_OID()) continue;
    const auto found = g_pack_to_gmap.find(id->get_OID().id);
    if (found != g_pack_to_gmap.end()) *id = ObjectId(found->second);
  }
}

void RebindOpBasicGraphPattern(OpBasicGraphPattern& bgp) {
  for (auto& triple : bgp.triples) {
    RebindOpTriple(triple);
  }
  for (auto& path : bgp.paths) {
    RebindOpPath(path);
  }
}

template <typename T>
void RebindOptionalChild(std::unique_ptr<Op>& op) {
  if (auto* typed = dynamic_cast<T*>(op.get())) {
    if constexpr (std::is_same_v<T, OpSelect>) {
      if (typed->op) RebindOpIds(typed->op);
    } else if constexpr (std::is_same_v<T, OpFilter>) {
      if (typed->op) RebindOpIds(typed->op);
    } else if constexpr (std::is_same_v<T, OpGroupBy>) {
      if (typed->op) RebindOpIds(typed->op);
    } else if constexpr (std::is_same_v<T, OpHaving>) {
      if (typed->op) RebindOpIds(typed->op);
    } else if constexpr (std::is_same_v<T, OpOrderBy>) {
      if (typed->op) RebindOpIds(typed->op);
    } else if constexpr (std::is_same_v<T, OpBind>) {
      if (typed->op) RebindOpIds(typed->op);
    } else if constexpr (std::is_same_v<T, OpNotExists>) {
      if (typed->op) RebindOpIds(typed->op);
    } else if constexpr (std::is_same_v<T, OpGraph>) {
      if (typed->op) RebindOpIds(typed->op);
    } else if constexpr (std::is_same_v<T, OpBasicGraphPattern>) {
      RebindOpBasicGraphPattern(*typed);
    }
  }
}

void RebindOpIds(std::unique_ptr<Op>& op) {
  if (op == nullptr) return;

  if (auto* ask = dynamic_cast<OpAsk*>(op.get())) {
    RebindOpIds(ask->op);
  } else if (auto* construct = dynamic_cast<OpConstruct*>(op.get())) {
    // Template terms are output identities and may not exist in storage.
    RebindOpIds(construct->op);
  } else if (auto* sel = dynamic_cast<OpSelect*>(op.get())) {
    if (sel->op) RebindOpIds(sel->op);
  } else if (auto* filter = dynamic_cast<OpFilter*>(op.get())) {
    if (filter->op) RebindOpIds(filter->op);
  } else if (auto* join = dynamic_cast<OpJoin*>(op.get())) {
    RebindOpIds(join->lhs);
    RebindOpIds(join->rhs);
  } else if (auto* opt = dynamic_cast<OpOptional*>(op.get())) {
    RebindOpIds(opt->lhs);
    RebindOpIds(opt->rhs);
  } else if (auto* uni = dynamic_cast<OpUnion*>(op.get())) {
    for (auto& u_op : uni->unions) {
      RebindOpIds(u_op);
    }
  } else if (auto* seq = dynamic_cast<OpSequence*>(op.get())) {
    for (auto& child : seq->ops) {
      RebindOpIds(child);
    }
  } else if (auto* semi = dynamic_cast<OpSemiJoin*>(op.get())) {
    RebindOpIds(semi->lhs);
    RebindOpIds(semi->rhs);
  } else if (auto* minus = dynamic_cast<OpMinus*>(op.get())) {
    RebindOpIds(minus->lhs);
    RebindOpIds(minus->rhs);
  } else if (auto* bgp = dynamic_cast<OpBasicGraphPattern*>(op.get())) {
    RebindOpBasicGraphPattern(*bgp);
  } else if (auto* gby = dynamic_cast<OpGroupBy*>(op.get())) {
    if (gby->op) RebindOpIds(gby->op);
  } else if (auto* having = dynamic_cast<OpHaving*>(op.get())) {
    if (having->op) RebindOpIds(having->op);
  } else if (auto* ob = dynamic_cast<OpOrderBy*>(op.get())) {
    if (ob->op) RebindOpIds(ob->op);
  } else if (auto* bind = dynamic_cast<OpBind*>(op.get())) {
    if (bind->op) RebindOpIds(bind->op);
  } else if (auto* ne = dynamic_cast<OpNotExists*>(op.get())) {
    RebindOpIds(ne->lhs);
    RebindOpIds(ne->rhs);
  } else if (auto* graph = dynamic_cast<OpGraph*>(op.get())) {
    if (graph->op) RebindOpIds(graph->op);
  } else if (auto* desc = dynamic_cast<OpDescribe*>(op.get())) {
    if (desc->op) RebindOpIds(desc->op);
  } else if (auto* vals = dynamic_cast<OpValues*>(op.get())) {
    (void) vals;
  }
}

bool ContainsPaths(const Op* op) {
  if (!op) return false;
  if (auto* bgp = dynamic_cast<const OpBasicGraphPattern*>(op)) return !bgp->paths.empty();
  if (auto* join = dynamic_cast<const OpJoin*>(op))
    return ContainsPaths(join->lhs.get()) || ContainsPaths(join->rhs.get());
  if (auto* opt = dynamic_cast<const OpOptional*>(op))
    return ContainsPaths(opt->lhs.get()) || ContainsPaths(opt->rhs.get());
  if (auto* minus = dynamic_cast<const OpMinus*>(op))
    return ContainsPaths(minus->lhs.get()) || ContainsPaths(minus->rhs.get());
  if (auto* ne = dynamic_cast<const OpNotExists*>(op))
    return ContainsPaths(ne->lhs.get()) || ContainsPaths(ne->rhs.get());
  if (auto* semi = dynamic_cast<const OpSemiJoin*>(op))
    return ContainsPaths(semi->lhs.get()) || ContainsPaths(semi->rhs.get());
  if (auto* uni = dynamic_cast<const OpUnion*>(op)) {
    for (const auto& child : uni->unions) if (ContainsPaths(child.get())) return true;
  }
  if (auto* seq = dynamic_cast<const OpSequence*>(op)) {
    for (const auto& child : seq->ops) if (ContainsPaths(child.get())) return true;
  }
  if (auto* node = dynamic_cast<const OpSelect*>(op)) return ContainsPaths(node->op.get());
  if (auto* node = dynamic_cast<const OpFilter*>(op)) return ContainsPaths(node->op.get());
  if (auto* node = dynamic_cast<const OpGroupBy*>(op)) return ContainsPaths(node->op.get());
  if (auto* node = dynamic_cast<const OpHaving*>(op)) return ContainsPaths(node->op.get());
  if (auto* node = dynamic_cast<const OpOrderBy*>(op)) return ContainsPaths(node->op.get());
  if (auto* node = dynamic_cast<const OpBind*>(op)) return ContainsPaths(node->op.get());
  if (auto* node = dynamic_cast<const OpGraph*>(op)) return ContainsPaths(node->op.get());
  if (auto* node = dynamic_cast<const OpDescribe*>(op)) return ContainsPaths(node->op.get());
  if (auto* node = dynamic_cast<const OpAsk*>(op)) return ContainsPaths(node->op.get());
  if (auto* node = dynamic_cast<const OpConstruct*>(op)) return ContainsPaths(node->op.get());
  return false;
}

int ExecuteInsertData(const std::string& query_text) {
  if (g_id_map == nullptr) {
    g_id_map = std::make_unique<yb::mdb::IdMap>();
  }

  yb::mdb::ImportStats stats;
  auto status = yb::mdb::InsertDataExecutor::Execute(query_text, *g_id_map, stats);

  LOG(INFO) << "INSERT DATA: parsed=" << stats.triples_parsed
            << " written=" << stats.triples_written
            << " errors=" << stats.errors;

  if (!status.ok()) {
    LOG(WARNING) << "INSERT DATA failed: " << status;
    return 1;
  }
  auto save_status = SaveIdMapToDocDB();
  if (!save_status.ok()) {
    LOG(WARNING) << "Failed to persist INSERT DATA dictionary: " << save_status;
    return 1;
  }
  return 0;
}

int ExecuteSparql(
    const std::string& query_text, ResultSink& sink)
{
  const auto query_started = yb::mdb::WallMicros();
  QueryContext query_context;
  QueryContextScope query_scope(query_context);
  QueryParser qp(query_text);
  if (qp.parser.getNumberOfSyntaxErrors() > 0) {
    return 1;
  }

  QueryVisitor::GlobalInfo global_info;
  global_info.base_iri = "";

  for (auto& [prefix, iri] : rdf_model.default_query_prefixes) {
    global_info.iri_prefix_map[prefix] = iri;
  }

  QueryVisitor visitor(global_info);
  visitor.visitQuery(qp.root);
  auto op = std::move(visitor.current_op);

  if (op == nullptr) {
    return 0;
  }

  op = QueryParser::rewrite(std::move(op));
  const auto parse_finished = yb::mdb::WallMicros();

  auto& model = yb::mdb::RdfYBModel::Instance();
  std::unique_ptr<yb::mdb::DocDBReadContext> read_context;
  std::optional<yb::mdb::DocDBReadContextScope> read_scope;
  // Public transactional queries must not reuse caches from an earlier query
  // after another importer commits. Join policy selection remains unchanged.
  const bool use_stream = model.IsConnected() && (model.UseStreamingReads() ||
      ContainsPaths(op.get()) || (g_cluster_initialized && model.UseTransactions()));
  const bool use_snapshot = model.IsConnected() && model.UseTransactions() && !use_stream;
  if (use_stream) {
    auto result = model.NewQueryReadContext();
    if (!result.ok()) {
      LOG(WARNING) << "Cannot start MDB streaming read: " << result.status();
      return 1;
    }
    read_context = std::move(*result);
    read_scope.emplace(*read_context);
  } else if (use_snapshot) {
    auto status = model.BeginReadSnapshot();
    if (!status.ok()) {
      LOG(WARNING) << "Cannot start MDB read: " << status;
      return 1;
    }
  }
  if (read_context) {
    read_context->stats.adapter_parse_us = parse_finished - query_started;
    read_context->stats.adapter_read_view_us = yb::mdb::WallMicros() - parse_finished;
  }
  auto snapshot_cleanup = yb::ScopeExit([&] {
    if (read_context) {
      read_context->stats.adapter_total_us = yb::mdb::WallMicros() - query_started;
      last_query_profile = read_context->stats.ToJson();
    }
    if (use_snapshot) model.EndReadSnapshot();
  });

  // Pin the data read point before refreshing immutable term claims. Every
  // term referenced by that view was persisted before its data committed.
  if (g_cluster_initialized && g_id_map) {
    yb::mdb::WallTimeScope timer(read_context ? &read_context->stats.adapter_dictionary_us : nullptr);
    const auto status = model.RestoreTermMap(*g_id_map);
    if (!status.ok()) {
      LOG(WARNING) << "Cannot refresh MDB term dictionary: " << status;
      return 1;
    }
  }
  if (g_cluster_initialized || !g_pack_to_gmap.empty() || model.UseStreamingReads())
    RebindOpIds(op);

  const auto worker_options = model.GetWorkerOptions();
  if (worker_options.exchange != yb::mdb::ExchangeMode::None && worker_options.targets.empty()) {
    LOG(WARNING) << "MDB exchange requires worker targets";
    return 1;
  }
  if (!worker_options.targets.empty()) {
    if (!use_stream) {
      LOG(WARNING) << "MDB workers require query-owned streaming reads";
      return 1;
    }
    const auto plan_started = yb::mdb::WallMicros();
    auto physical = read_context->batch_options.policy == yb::mdb::JoinPolicy::Index
        ? yb::mdb::FragmentPlan::Build(*op, model, worker_options)
        : yb::Result<yb::mdb::FragmentPlan>(STATUS(NotSupported,
            "Worker hash/auto join policies remain at MDB coordinator"));
    read_context->stats.adapter_plan_us += yb::mdb::WallMicros() - plan_started;
    if (!physical.ok()) {
      if (!physical.status().IsNotSupported()) {
        LOG(WARNING) << "MDB fragment planning failed: " << physical.status();
        return 1;
      }
      // Capability fallback happens only before remote work or output starts.
      ++read_context->stats.remote_coordinator_fallbacks;
    } else {
      yb::mdb::WallTimeScope timer(&read_context->stats.adapter_execute_us);
      const auto* select = dynamic_cast<const OpSelect*>(op.get());
      std::vector<ObjectId> gathered;
      std::vector<VarId> gathered_vars;
      uint64_t reserved = 0;
      std::optional<yb::mdb::GlobalTopK> topk;
      if (physical->topk) {
        topk.emplace(*physical->topk);
        ++read_context->stats.global_topk_operators;
        read_context->stats.global_topk_capacity = physical->topk->rows;
      }
      std::optional<yb::mdb::GlobalDistinct> distinct;
      if (physical->distinct) {
        distinct.emplace(*physical->distinct);
        ++read_context->stats.global_distinct_operators;
      }
      std::optional<yb::mdb::GlobalCount> count;
      if (physical->count) {
        count.emplace(*physical->count);
        const auto initialized = count->Initialize(worker_options.coordinator_memory_bytes);
        if (!initialized.ok()) { LOG(WARNING) << initialized; return 1; }
        ++read_context->stats.global_count_operators;
        if (physical->count->has_distinct) {
          ++read_context->stats.global_count_distinct_operators;
          read_context->stats.global_count_distinct_sets = physical->count->distinct_columns.size();
        }
        if (physical->count->having) {
          ++read_context->stats.global_count_having_operators;
          read_context->stats.global_count_having_hidden_counts = physical->count->having_variables.size();
        }
        read_context->stats.global_count_groups = count->size();
        reserved = count->reserved_bytes();
        read_context->stats.global_reserved_bytes = reserved;
      }
      if (physical->coordinator_finalize) {
        for (auto var : physical->physical.projection()) gathered_vars.emplace_back(var);
      }
      if (!physical->coordinator_finalize) sink.Begin(YBC_MDB_BINDINGS, select->vars);
      const auto gather = worker_options.exchange == yb::mdb::ExchangeMode::None
          ? yb::mdb::GatherFragments : yb::mdb::GatherExchangeFragments;
      auto status = gather(*physical, worker_options, *read_context,
          [&](const std::vector<uint64_t>& row) -> Status {
            if (physical->coordinator_finalize) {
              // Bound input plus conservative space for Values, sorting full
              // bindings/keys, DISTINCT trees and COUNT(DISTINCT) sets. This
              // reservation excludes shared dictionaries and client infrastructure.
              const uint64_t row_bytes = topk ? physical->topk->row_reservation :
                  distinct ? physical->distinct->row_reservation : physical->global_row_reservation;
              if (count) {
                RETURN_NOT_OK(count->AdmitCandidate(worker_options.coordinator_memory_bytes));
              } else if (topk || distinct) {
                const uint64_t slots = (topk ? topk->size() : distinct->size()) + 1;
                if (slots > worker_options.coordinator_memory_bytes / row_bytes)
                  return STATUS(RuntimeError, "MDB global Gather input budget exceeded");
                reserved = std::max(reserved, slots * row_bytes);
              } else {
                if (reserved > worker_options.coordinator_memory_bytes ||
                    row_bytes > worker_options.coordinator_memory_bytes - reserved)
                  return STATUS(RuntimeError, "MDB global Gather input budget exceeded");
                reserved += row_bytes;
              }
              std::vector<ObjectId> candidate;
              if (topk || distinct || count) candidate.reserve(row.size());
              for (auto id : row) {
                auto found = g_gmap_to_pack.find(id);
                if (found == g_gmap_to_pack.end())
                  return STATUS(Corruption, "Missing typed dictionary identity for global input");
                if (topk || distinct || count) candidate.push_back(found->second);
                else gathered.push_back(found->second);
              }
              if (count) {
                RETURN_NOT_OK(count->Add(candidate, worker_options.coordinator_memory_bytes));
                reserved = count->reserved_bytes();
                read_context->stats.global_count_groups = count->size();
                read_context->stats.global_count_distinct_entries = count->distinct_entries();
              }
              if (distinct) {
                RETURN_NOT_OK(distinct->Add(std::move(candidate), worker_options.coordinator_memory_bytes));
                reserved = distinct->reserved_bytes();
                read_context->stats.global_distinct_retained_rows = distinct->size();
              }
              if (topk) {
                topk->Add(std::move(candidate));
                read_context->stats.global_topk_retained_rows = topk->size();
              }
              ++read_context->stats.global_input_rows;
              read_context->stats.global_reserved_bytes = reserved;
              return Status::OK();
            }
            std::vector<ObjectId> typed;
            for (auto id : row) {
              const auto found = g_gmap_to_pack.find(id);
              if (found == g_gmap_to_pack.end())
                return STATUS(Corruption, "Missing typed dictionary identity for worker output");
              typed.push_back(found->second);
            }
            sink.Row(typed);
            return Status::OK();
          });
      if (!status.ok()) LOG(WARNING) << "MDB fragment query failed: " << status;
      if (!status.ok()) return 1;
      if (!physical->coordinator_finalize) { sink.End(); return 0; }
      // Gather all child transaction results before exposing global output.
      // The original modifiers execute once over full input or safe representatives.
      if (topk) gathered = topk->ReleaseRows();
      if (distinct) gathered = distinct->ReleaseRows();
      if (count) {
        const auto finalized = yb::mdb::FragmentPlan::FinalizeCount(
            *op, *physical->count, count->ReleaseRows());
        if (!finalized.ok()) { LOG(WARNING) << finalized; return 1; }
      } else {
        *yb::mdb::FragmentPlan::InputSlot(*op) = std::make_unique<OpValues>(
            std::move(gathered_vars), std::move(gathered));
      }
      ++read_context->stats.global_finalize_operators;
    }
  }

  auto iter = [&] {
    yb::mdb::WallTimeScope timer(read_context ? &read_context->stats.adapter_plan_us : nullptr);
    BindingIterConstructor optimizer;
    op->accept_visitor(optimizer);
    return std::move(optimizer.tmp);
  }();
  yb::mdb::WallTimeScope execution_timer(
      read_context ? &read_context->stats.adapter_execute_us : nullptr);

  if (auto* describe = dynamic_cast<OpDescribe*>(op.get())) {
    if (!describe->op && !describe->iris.empty()) {
      sink.Begin(YBC_MDB_GRAPH);
      std::unordered_set<std::string> seen_triples;

      for (const auto& iri_str : describe->iris) {
        auto iri_oid = Conversions::pack_iri(iri_str);
        auto it = g_pack_to_gmap.find(iri_oid.id);
        if (it == g_pack_to_gmap.end()) continue;
        ObjectId graph_id(it->second);
        auto iri_uri = IdToString(graph_id.id);

        {
          auto plan = std::make_unique<TriplePlan>(
              Id(graph_id), Id(VarId(0)), Id(VarId(1)));
          auto desc_iter = plan->get_binding_iter();
          Binding desc_binding(2);
          desc_iter->begin(desc_binding);
          while (desc_iter->next()) {
            auto pred_oid = desc_binding[VarId(0)];
            auto obj_oid = desc_binding[VarId(1)];
            auto pred_str = IdToString(pred_oid.id);
            auto obj_str = IdToString(obj_oid.id);

            std::string triple_key = iri_uri + " " + pred_str + " " + obj_str;
            if (seen_triples.insert(triple_key).second) {
              sink.Triple({iri_oid, pred_oid, obj_oid});
            }
          }
        }

        {
          auto plan = std::make_unique<TriplePlan>(
              Id(VarId(0)), Id(VarId(1)), Id(graph_id));
          auto desc_iter = plan->get_binding_iter();
          Binding desc_binding(2);
          desc_iter->begin(desc_binding);
          while (desc_iter->next()) {
            auto subj_oid = desc_binding[VarId(0)];
            auto pred_oid = desc_binding[VarId(1)];
            auto subj_str = IdToString(subj_oid.id);
            auto pred_str = IdToString(pred_oid.id);

            std::string triple_key = subj_str + " " + pred_str + " " + iri_uri;
            if (seen_triples.insert(triple_key).second) {
              sink.Triple({subj_oid, pred_oid, iri_oid});
            }
          }
        }
      }

      sink.End();
      return 0;
    }
  }

  if (iter == nullptr) throw QueryException("MDB query produced no executable iterator");

  Binding binding(get_query_ctx().get_var_size());
  auto check_global_state = [&] {
    if (read_context && read_context->stats.global_finalize_operators &&
        (read_context->cancelled.load() || yb::CoarseMonoClock::Now() >= read_context->deadline))
      throw QueryException("MDB global finalization cancelled or expired");
  };
  check_global_state();
  iter->begin(binding);
  check_global_state();

  if (dynamic_cast<const OpAsk*>(op.get())) {
    sink.Begin(YBC_MDB_BOOLEAN);
    const bool answer = iter->next();
    check_global_state();
    sink.Boolean(answer);
    sink.End();
    return 0;
  }

  if (auto* construct = dynamic_cast<const OpConstruct*>(op.get())) {
    sink.Begin(YBC_MDB_GRAPH);
    std::set<std::array<uint64_t, 3>> seen;
    const auto blank_scope = yb::Uuid::Generate().ToString();
    uint64_t solution = 0;
    while (iter->next()) {
      check_global_state();
      ++solution;
      std::map<VarId, ObjectId> blanks;
      auto term = [&](Id id) -> ObjectId {
        if (id.is_OID()) return id.get_OID();
        const auto var = id.get_var();
        const auto name = get_query_ctx().get_original_var_name(var);
        if (name.rfind("_:", 0) == 0) {
          auto found = blanks.find(var);
          if (found != blanks.end()) return found->second;
          auto value = yb::mdb::PackRdfIdentity({'B', std::to_string(solution) + ":" + name, blank_scope});
          blanks.emplace(var, value);
          return value;
        }
        return binding[var];
      };
      for (const auto& triple : construct->triples) {
        const auto s = term(triple.subject), p = term(triple.predicate), o = term(triple.object);
        if (s.is_null() || p.is_null() || o.is_null()) continue;
        const auto st = RDF_OID::get_generic_type(s), pt = RDF_OID::get_generic_type(p);
        if ((st != RDF_OID::GenericType::IRI && st != RDF_OID::GenericType::BLANK) ||
            pt != RDF_OID::GenericType::IRI) continue;
        if (seen.size() >= worker_options.coordinator_memory_bytes / 128)
          throw QueryException("MDB CONSTRUCT result deduplication budget exceeded");
        if (seen.insert({s.id, p.id, o.id}).second) sink.Triple({s, p, o});
      }
    }
    check_global_state();
    sink.End();
    return 0;
  }

  std::vector<VarId> output_vars;
  if (const auto* select = dynamic_cast<const OpSelect*>(op.get())) output_vars = select->vars;
  else for (size_t i = 0; i < binding.size; ++i) output_vars.emplace_back(i);
  sink.Begin(YBC_MDB_BINDINGS, output_vars);
  while (iter->next()) {
    check_global_state();
    std::vector<ObjectId> row;
    for (const auto var : output_vars) row.push_back(binding[var]);
    sink.Row(row);
  }
  check_global_state();
  sink.End();
  return 0;
}

Status ImportNtFileWithIdMap(const std::string& file_path,
                             yb::mdb::IdMap& id_map,
                             int64_t& parsed, int64_t& written, int64_t& errors) {
  std::ifstream file(file_path);
  if (!file.is_open()) {
    return STATUS(IOError, "Cannot open file: " + file_path);
  }

  const auto blank_scope = yb::Uuid::Generate().ToString();
  auto& model = yb::mdb::RdfYBModel::Instance();
  bool use_txn = model.UseTransactions();
  if (use_txn) {
    RETURN_NOT_OK(model.BeginTransaction());
  }
  auto abort_on_exit = yb::ScopeExit([&model, use_txn] {
    if (use_txn) (void)model.AbortTransaction();
  });

  std::string line;
  int line_num = 0;

  while (std::getline(file, line)) {
    line_num++;
    if (line.empty() || line[0] == '#') {
      continue;
    }

    auto result = yb::mdb::NtParser::ParseLine(line);
    if (!result.ok()) {
      errors++;
      LOG(WARNING) << "Line " << line_num << ": " << result.status();
      continue;
    }

    auto& triple = *result;

    uint64_t subj_id = VERIFY_RESULT(model.ResolveRdfTerm(
        id_map, triple.sub_type, triple.sub, "", blank_scope));
    uint64_t pred_id = VERIFY_RESULT(model.ResolveRdfTerm(id_map, triple.pred_type, triple.pred));
    uint64_t obj_id = VERIFY_RESULT(model.ResolveRdfTerm(
        id_map, triple.obj_type, triple.obj, triple.object_type, blank_scope));

    auto write_status = model.WriteTripleToDocDB(subj_id, pred_id, obj_id);
    if (!write_status.ok()) {
      if (use_txn) {
        (void)model.AbortTransaction();
      }
      return write_status;
    }
    parsed++;
    written++;

    if (use_txn && written % 50 == 0) {
      RETURN_NOT_OK(model.CommitTransaction());
      RETURN_NOT_OK(model.BeginTransaction());
    } else if (!use_txn && written % 5000 == 0) {
      RETURN_NOT_OK(model.Flush());
    }
  }

  if (use_txn) {
    RETURN_NOT_OK(model.CommitTransaction());
  } else {
    RETURN_NOT_OK(model.Flush());
  }

  return Status::OK();
}

}  // namespace

int YBCMdbInit() {
  return 0;
}

int YBCMdbExecuteSparql(
    const char* query_text,
    YBCMdbRowCallback row_callback,
    void* ctx)
{
  AdapterScope scope;
  if (!scope.entered()) return 1;
  last_query_profile.clear();
  if (query_text == nullptr || row_callback == nullptr) {
    return 1;
  }

  std::string query_str(query_text);

  size_t first_non_space = query_str.find_first_not_of(" \t\r\n");
  if (first_non_space != std::string::npos) {
    std::string_view start = std::string_view(query_str).substr(first_non_space);
    if (start.size() >= 11 && start.substr(0, 11) == "INSERT DATA") {
      return ExecuteInsertData(query_str);
    }
  }

  try {
    ResultSink sink(row_callback, nullptr, ctx);
    return ExecuteSparql(query_str, sink);
  } catch (const std::exception& error) {
    LOG(WARNING) << "MDB query failed: " << error.what();
    return 1;
  } catch (...) {
    LOG(WARNING) << "MDB query failed with an unknown exception";
    return 1;
  }
}

int YBCMdbExecuteSparqlWithResults(
    const char* query_text, const YBCMdbResultCallbacks* callbacks, void* ctx) {
  AdapterScope scope;
  if (!scope.entered()) return 1;
  last_query_profile.clear();
  if (!query_text || !callbacks || !callbacks->begin || !callbacks->row ||
      !callbacks->boolean_result || !callbacks->triple || !callbacks->end) return 1;
  try {
    const std::string query(query_text);
    const auto first = query.find_first_not_of(" \t\r\n");
    if (first != std::string::npos && query.compare(first, 11, "INSERT DATA") == 0)
      return ExecuteInsertData(query);
    ResultSink sink(nullptr, callbacks, ctx);
    return ExecuteSparql(query, sink);
  } catch (const std::exception& error) {
    LOG(WARNING) << "MDB query failed: " << error.what();
    return 1;
  } catch (...) {
    LOG(WARNING) << "MDB query failed with an unknown exception";
    return 1;
  }
}

const char* YBCMdbGetLastQueryProfile() { return last_query_profile.c_str(); }

int YBCMdbShutdown() {
  AdapterScope scope;
  if (!scope.entered()) return 1;
  ResetAdapter();
  return 0;
}

int YBCMdbInitCluster(const char* master_addrs, const char* keyspace) {
  AdapterScope scope;
  if (!scope.entered()) return 1;
  if (master_addrs == nullptr || keyspace == nullptr) {
    return 1;
  }

  const std::string master_addrs_str(master_addrs);
  const std::string keyspace_str(keyspace);
  auto& model = yb::mdb::RdfYBModel::Instance();
  const auto worker_options = model.GetWorkerOptions();
  // The model stores a borrowed client pointer. Drop it before replacing an
  // owned client, also when switching namespaces or retrying a failed Init.
  ResetAdapter();
  model.SetWorkerOptions(worker_options);
  g_keyspace = keyspace_str;
  bool initialized = false;
  auto initialization_cleanup = yb::ScopeExit([&] {
    if (!initialized) ResetAdapter();
  });

  LOG(INFO) << "Connecting to masters at " << master_addrs_str;

  g_clock = make_scoped_refptr<yb::server::HybridClock>();
  auto clock_status = g_clock->Init();
  if (!clock_status.ok()) {
    LOG(WARNING) << "Failed to init HybridClock: " << clock_status;
    return 1;
  }

  auto client_result = yb::client::YBClientBuilder()
      .add_master_server_addr(master_addrs_str)
      .default_admin_operation_timeout(MonoDelta::FromSeconds(30))
      .Build(nullptr, g_clock);

  if (!client_result.ok()) {
    LOG(WARNING) << "Failed to create YBClient: " << client_result.status();
    return 1;
  }

  g_yb_client = std::move(*client_result);

  auto ns_status = g_yb_client->CreateNamespaceIfNotExists(
      g_keyspace, YQL_DATABASE_CQL);
  if (!ns_status.ok()) {
    LOG(WARNING) << "Failed to create namespace: " << ns_status;
    return 1;
  }

  for (int i = 0; i < 6; i++) {
    yb::client::YBTableName table_name(
        YQL_DATABASE_CQL, g_keyspace,
        yb::mdb::RdfYBModel::kTableNames[i]);

    yb::client::YBSchemaBuilder builder;
    yb::mdb::ConfigureRdfTableSchema(&builder);

    yb::client::TableHandle tmp;
    auto status = tmp.Create(table_name, 1, g_yb_client.get(), &builder);
    if (!status.ok()) {
      LOG(INFO) << "Table " << yb::mdb::RdfYBModel::kTableNames[i]
                << ": " << status;
    }
  }

  auto create_idmap_status = CreateIdMapTable();
  if (!create_idmap_status.ok()) {
    LOG(WARNING) << "Failed to create RDF dictionary: " << create_idmap_status;
    return 1;
  }

  auto init_status = yb::mdb::RdfYBModel::Instance().Init(
      g_yb_client.get(), g_keyspace);
  if (!init_status.ok()) {
    LOG(WARNING) << "Failed to init RdfYBModel: " << init_status;
    return 1;
  }

  g_id_map = std::make_unique<yb::mdb::IdMap>();

  auto load_idmap_status = LoadIdMapFromDocDB();
  if (!load_idmap_status.ok()) {
    LOG(WARNING) << "Failed to restore RDF dictionary: " << load_idmap_status;
    return 1;
  }

  g_cluster_initialized = true;
  initialized = true;
  LOG(INFO) << "Cluster initialized successfully, keyspace=" << g_keyspace;
  return 0;
}

int YBCMdbImportFile(const char* file_path) {
  AdapterScope scope;
  if (!scope.entered()) return 1;
  if (file_path == nullptr) {
    return 1;
  }

  if (!g_cluster_initialized) {
    LOG(WARNING) << "Cluster not initialized, call YBCMdbInitCluster first";
    return 1;
  }

  if (g_id_map == nullptr) {
    g_id_map = std::make_unique<yb::mdb::IdMap>();
  }

  int64_t parsed = 0, written = 0, errors = 0;
  auto status = ImportNtFileWithIdMap(std::string(file_path), *g_id_map,
                                      parsed, written, errors);
  if (!status.ok()) {
    LOG(WARNING) << "Import failed: " << status;
    return 1;
  }

  LOG(INFO) << "Import: parsed=" << parsed
            << " written=" << written
            << " errors=" << errors;

  if (FLAGS_TEST_mdb_pause_before_dictionary_mirror_ms > 0)
    yb::SleepFor(MonoDelta::FromMilliseconds(FLAGS_TEST_mdb_pause_before_dictionary_mirror_ms));

  auto save_idmap_status = SaveIdMapToDocDB();
  if (!save_idmap_status.ok()) {
    LOG(WARNING) << "Failed to save RDF dictionary: " << save_idmap_status;
    return 1;
  }

  g_gmap_to_pack.clear();
  for (auto& [pack_id, gmap_id] : g_pack_to_gmap) {
    g_gmap_to_pack[gmap_id] = ObjectId(pack_id);
  }

  auto load_status = yb::mdb::RdfYBModel::Instance().LoadAllFromDocDB();
  if (!load_status.ok()) {
    LOG(WARNING) << "LoadAllFromDocDB failed: " << load_status;
    return 1;
  }

  return 0;
}

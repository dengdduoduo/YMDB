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

#include <algorithm>
#include <fstream>
#include <memory>
#include <set>
#include <string>
#include <tuple>
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
#include "yb/mdb/query/distributed/distributed_plan.h"
#include "yb/mdb/query/distributed/gather_operator.h"
#include "yb/mdb/query/distributed/scatter_executor.h"
#include "yb/mdb/query/distributed/tablet_router.h"
#include "yb/mdb/import/import_pipeline.h"
#include "yb/mdb/import/insert_executor.h"
#include "yb/mdb/import/nt_parser.h"
#include "yb/mdb/model/rdf_yb_model.h"
#include "yb/server/hybrid_clock.h"
#include "yb/util/logging.h"
#include "yb/util/status.h"
#include "yb/util/scope_exit.h"
#include "yb/util/uuid.h"

using yb::MonoDelta;
using yb::Status;
using yb::YQL_DATABASE_CQL;
using SPARQL::BindingIterConstructor;
using SPARQL::Conversions;
using SPARQL::Op;
using SPARQL::OpBasicGraphPattern;
using SPARQL::OpBind;
using SPARQL::OpDescribe;
using SPARQL::OpFilter;
using SPARQL::OpGraph;
using SPARQL::OpGroupBy;
using SPARQL::OpHaving;
using SPARQL::OpJoin;
using SPARQL::OpMinus;
using SPARQL::OpNotExists;
using SPARQL::OpOptional;
using SPARQL::OpOrderBy;
using SPARQL::OpPath;
using SPARQL::OpSelect;
using SPARQL::OpSemiJoin;
using SPARQL::OpSequence;
using SPARQL::OpTriple;
using SPARQL::OpUnion;
using SPARQL::OpValues;
using SPARQL::QueryParser;
using SPARQL::QueryVisitor;
using SPARQL::TriplePlan;

static const char* kIdMapTableName = "rdf_idmap";

std::unordered_map<uint64_t, std::string> g_gmap_to_uri;
std::unordered_map<uint64_t, ObjectId> g_gmap_to_pack;
std::unordered_map<uint64_t, uint64_t> g_pack_to_gmap;

namespace {

std::unique_ptr<yb::mdb::IdMap> g_id_map;

std::unique_ptr<yb::client::YBClient> g_yb_client;

scoped_refptr<yb::server::HybridClock> g_clock;

bool g_cluster_initialized = false;

std::string g_keyspace;

constexpr int kRdfNumTablets = 2;

std::string IdToString(uint64_t id) {
  if (!g_pack_to_gmap.empty()) {
    auto pack_it = g_pack_to_gmap.find(id);
    if (pack_it != g_pack_to_gmap.end()) {
      id = pack_it->second;
    }
  }
  auto it = g_gmap_to_uri.find(id);
  if (it != g_gmap_to_uri.end()) {
    return it->second;
  }
  if (g_id_map != nullptr) {
    auto& name = g_id_map->Lookup(id);
    if (!name.empty()) {
      return name;
    }
  }
  auto str = Conversions::to_lexical_str(ObjectId(id));
  if (!str.empty()) {
    return str;
  }
  return std::to_string(id);
}

struct ResultTerm {
  YBCMdbTermKind kind = YBC_MDB_UNBOUND;
  std::string lexical;
  std::string language;
  std::string datatype;
};

YBCMdbString Borrow(const std::string& value) {
  return {value.data(), value.size()};
}

yb::Result<ResultTerm> DecodeResultTerm(ObjectId value) {
  ResultTerm term;
  if (value.is_null()) return term;
  const auto mapped = g_gmap_to_pack.find(value.id);
  if (mapped != g_gmap_to_pack.end()) value = mapped->second;
  constexpr const char* kXsd = "http://www.w3.org/2001/XMLSchema#";
  switch (RDF_OID::get_generic_type(value)) {
    case RDF_OID::GenericType::IRI:
      term.kind = YBC_MDB_IRI;
      if (!Conversions::try_get_iri(value, &term.lexical)) {
        return STATUS(Corruption, "Result IRI is missing its lexical dictionary entry");
      }
      break;
    case RDF_OID::GenericType::BLANK:
      term.kind = YBC_MDB_BLANK;
      term.lexical = "b" + std::to_string(value.id);
      break;
    case RDF_OID::GenericType::NUMERIC:
      value = Conversions::term_identity(value);
      [[fallthrough]];
    case RDF_OID::GenericType::STRING: {
      term.kind = YBC_MDB_LITERAL;
      const auto subtype = RDF_OID::get_generic_subtype(value);
      if (subtype == RDF_OID::GenericSubType::STRING_LANG) {
        auto [language, lexical] = Conversions::unpack_string_lang(value);
        term.language = std::move(language);
        term.lexical = std::move(lexical);
      } else if (subtype == RDF_OID::GenericSubType::STRING_DATATYPE) {
        auto [datatype, lexical] = Conversions::unpack_string_datatype(value);
        term.datatype = std::move(datatype);
        term.lexical = std::move(lexical);
      } else {
        term.lexical = Conversions::unpack_string(value);
        term.datatype = std::string(kXsd) + "string";
      }
      break;
    }
    case RDF_OID::GenericType::BOOL:
      term.kind = YBC_MDB_LITERAL;
      term.lexical = value.is_true() ? "true" : "false";
      term.datatype = std::string(kXsd) + "boolean";
      break;
    default:
      return STATUS(NotSupported, "Result value has no supported RDF term representation");
  }
  return term;
}

class ResultSink {
 public:
  ResultSink(YBCMdbRowCallback legacy, void* context) : legacy_(legacy), context_(context) {}
  ResultSink(YBCMdbSchemaCallback schema, YBCMdbResultRowCallback row, void* context)
      : schema_(schema), row_(row), context_(context) {}

  bool structured() const { return row_ != nullptr; }

  Status Schema(std::vector<std::string> columns) {
    columns_ = std::move(columns);
    if (!schema_) return Status::OK();
    std::vector<YBCMdbString> names;
    for (const auto& name : columns_) names.push_back(Borrow(name));
    return schema_(names.data(), names.size(), context_) == 0 ? Status::OK()
        : STATUS(Aborted, "Result schema callback cancelled the query");
  }

  Status Row(const std::vector<ObjectId>& values) {
    if (values.size() != columns_.size()) {
      return STATUS(IllegalState, "Result row does not match its schema");
    }
    if (legacy_) {
      for (size_t i = 0; i < values.size(); ++i) {
        if (values[i].is_null() || columns_[i].empty()) continue;
        const auto text = IdToString(values[i].id);
        legacy_(columns_[i].c_str(), text.c_str(), context_);
      }
      return Status::OK();
    }
    std::vector<ResultTerm> terms;
    terms.reserve(values.size());
    for (const auto value : values) terms.push_back(VERIFY_RESULT(DecodeResultTerm(value)));
    std::vector<YBCMdbValue> views;
    for (const auto& term : terms) {
      views.push_back(
          {term.kind, Borrow(term.lexical), Borrow(term.language), Borrow(term.datatype)});
    }
    return row_(views.data(), views.size(), context_) == 0 ? Status::OK()
        : STATUS(Aborted, "Result row callback cancelled the query");
  }

 private:
  YBCMdbRowCallback legacy_ = nullptr;
  YBCMdbSchemaCallback schema_ = nullptr;
  YBCMdbResultRowCallback row_ = nullptr;
  void* context_;
  std::vector<std::string> columns_;
};

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
  if (!status.ok() && !status.IsAlreadyPresent()) return status;
  return Status::OK();
}

Status RegisterTerm(uint64_t id, const yb::mdb::RdfTerm& term) {
  VERIFY_RESULT(term.Key());
  ObjectId packed;
  if (term.kind == yb::mdb::TripleTermType::LITERAL) {
    if (!term.language.empty()) {
      packed = Conversions::pack_string_lang(term.language, term.lexical);
    } else if (!term.datatype.empty()) {
      packed = Conversions::pack_string_datatype(term.datatype, term.lexical);
    } else {
      packed = Conversions::pack_string_simple(term.lexical);
    }
  } else if (term.kind == yb::mdb::TripleTermType::BLANK) {
    // Blank labels are not IRIs; document-scoped label allocation is still separate work.
    packed = ObjectId(ObjectId::MASK_BLANK_TMP |
        (Conversions::pack_iri(term.lexical).id & ObjectId::MASK_EXTERNAL_ID));
  } else {
    packed = Conversions::pack_iri(term.lexical);
  }
  const auto old = g_pack_to_gmap.find(packed.id);
  if (old != g_pack_to_gmap.end() && old->second != id) {
    return STATUS(Corruption, "Packed RDF term maps to conflicting graph IDs");
  }
  const auto graph = g_gmap_to_pack.find(id);
  if (graph != g_gmap_to_pack.end() && graph->second.id != packed.id) {
    return STATUS(Corruption, "Graph ID maps to conflicting RDF terms");
  }
  g_pack_to_gmap[packed.id] = id;
  g_gmap_to_pack[id] = packed;
  g_gmap_to_uri[id] = term.lexical;
  return Status::OK();
}

Status PersistImportedTerm(uint64_t id, const yb::mdb::RdfTerm& term) {
  RETURN_NOT_OK(RegisterTerm(id, term));
  if (yb::mdb::RdfYBModel::Instance().Dictionary()) return Status::OK();
  yb::client::TableHandle table;
  RETURN_NOT_OK(table.Open(yb::client::YBTableName(
      YQL_DATABASE_CQL, g_keyspace, kIdMapTableName), g_yb_client.get()));
  auto session = g_yb_client->NewSession(MonoDelta::FromSeconds(30));
  auto op = table.NewInsertOp(session->arena());
  QLAddInt64HashValue(op->mutable_request(), static_cast<int64_t>(g_gmap_to_pack.at(id).id));
  table.AddInt64ColumnValue(op->mutable_request(), "gmap_id", static_cast<int64_t>(id));
  table.AddStringColumnValue(op->mutable_request(), "uri", term.lexical);
  RETURN_NOT_OK(session->TEST_ApplyAndFlush(op));
  if (op->response().status() != yb::QLResponsePB::YQL_STATUS_OK) {
    return STATUS(RuntimeError, "Cannot persist RDF dictionary: " +
        std::string(op->response().error_message()));
  }
  return Status::OK();
}

Status SaveIdMapToDocDB() {
  if (yb::mdb::RdfYBModel::Instance().Dictionary()) return Status::OK();
  if (g_id_map == nullptr) return Status::OK();

  yb::client::YBTableName table_name(YQL_DATABASE_CQL, g_keyspace, kIdMapTableName);
  yb::client::TableHandle table;
  RETURN_NOT_OK(table.Open(table_name, g_yb_client.get()));

  auto session = g_yb_client->NewSession(MonoDelta::FromSeconds(30));

  for (auto& [pack_id, gmap_id] : g_pack_to_gmap) {
    auto insert = table.NewInsertOp(session->arena());
    auto* req = insert->mutable_request();
    QLAddInt64HashValue(req, static_cast<int64_t>(pack_id));
    table.AddInt64ColumnValue(req, "gmap_id", static_cast<int64_t>(gmap_id));
    auto uri_it = g_gmap_to_uri.find(gmap_id);
    if (uri_it != g_gmap_to_uri.end()) {
      table.AddStringColumnValue(req, "uri", uri_it->second);
    } else {
      ObjectId pack_oid(static_cast<uint64_t>(pack_id));
      auto oid_type = pack_oid.get_type();
      if (oid_type == ObjectId::MASK_STRING_SIMPLE_EXTERN ||
          oid_type == ObjectId::MASK_STRING_XSD_EXTERN ||
          oid_type == ObjectId::MASK_STRING_LANG_EXTERN) {
        std::string str_val = Conversions::to_lexical_str(pack_oid);
        if (!str_val.empty()) {
          table.AddStringColumnValue(req, "uri", str_val);
        }
      }
    }
    session->Apply(insert);
  }

  RETURN_NOT_OK(session->TEST_Flush());

  LOG(INFO) << "Saved " << g_pack_to_gmap.size() << " idmap entries";
  return Status::OK();
}

Status LoadIdMapFromDocDB() {
  auto& model = yb::mdb::RdfYBModel::Instance();
  g_id_map = std::make_unique<yb::mdb::IdMap>();
  g_pack_to_gmap.clear();
  g_gmap_to_uri.clear();
  g_gmap_to_pack.clear();
  if (auto* dictionary = yb::mdb::RdfYBModel::Instance().Dictionary()) {
    for (const auto& [id, term] : VERIFY_RESULT(dictionary->Entries(model.QueryReadTime()))) {
      if (!g_id_map->Restore(id, term)) {
        return STATUS(Corruption, "Conflicting versioned RDF dictionary entries");
      }
      RETURN_NOT_OK(RegisterTerm(id, term));
    }
    return Status::OK();
  }
  yb::client::YBTableName table_name(YQL_DATABASE_CQL, g_keyspace, kIdMapTableName);
  yb::client::TableHandle table;
  auto open_status = table.Open(table_name, g_yb_client.get());
  if (!open_status.ok()) {
    return open_status;
  }

  yb::client::TableIteratorOptions options;
  options.read_time = model.QueryReadTime();
  options.columns = {"pack_id", "gmap_id", "uri"};
  Status read_status = Status::OK();
  options.error_handler = [&read_status](const Status& status) { read_status = status; };
  for (const auto& row : yb::client::TableRange(table, options)) {
    int64_t pack_id = row.column(0).int64_value();
    int64_t gmap_id = row.column(1).int64_value();
    auto uri = row.column(2).string_value();
    g_pack_to_gmap[static_cast<uint64_t>(pack_id)] = static_cast<uint64_t>(gmap_id);
    ObjectId pack_oid(static_cast<uint64_t>(pack_id));
    if (pack_oid.get_sub_type() == ObjectId::MASK_STRING_LANG_INLINED ||
        pack_oid.get_sub_type() == ObjectId::MASK_STRING_DATATYPE_INLINED ||
        pack_oid.get_sub_type() == ObjectId::MASK_STRING_XSD_INLINED) {
      return STATUS(NotSupported,
          "Legacy literal metadata is ambiguous; reimport in a fresh v2 namespace");
    }
    g_gmap_to_pack[static_cast<uint64_t>(gmap_id)] = pack_oid;
    {
      const auto kind = pack_oid.get_type() == ObjectId::MASK_BLANK_TMP
          ? yb::mdb::TripleTermType::BLANK : (pack_oid.id & ObjectId::GENERIC_TYPE_MASK) ==
              (ObjectId::MASK_STRING_SIMPLE_INLINED & ObjectId::GENERIC_TYPE_MASK)
          ? yb::mdb::TripleTermType::LITERAL : yb::mdb::TripleTermType::URI;
      // Both inline-hashed IRIs and literal lexemes need their dictionary after
      // the import process exits. Restore allocation state before later imports.
      if (!g_id_map->Restore(static_cast<uint64_t>(gmap_id), uri, kind)) {
        return STATUS(Corruption, "Conflicting persisted RDF dictionary entries");
      }
      g_gmap_to_uri[static_cast<uint64_t>(gmap_id)] = std::string(uri);
      auto oid_type = pack_oid.get_type();
      if (oid_type == ObjectId::MASK_STRING_SIMPLE_EXTERN ||
                 oid_type == ObjectId::MASK_STRING_XSD_EXTERN ||
                 oid_type == ObjectId::MASK_STRING_LANG_EXTERN) {
        uint64_t external_id = static_cast<uint64_t>(pack_id) & ObjectId::MASK_EXTERNAL_ID;
        Conversions::set_extern_string(external_id, std::string(uri));
      }
    }
  }

  RETURN_NOT_OK(read_status);
  LOG(INFO) << "Loaded " << g_pack_to_gmap.size() << " idmap entries";
  return Status::OK();
}

void RebindOpId(Id& id) {
  if (!id.is_OID()) return;
  // Constant folding can introduce numeric value handles after parsing.
  const auto term = SPARQL::Conversions::term_identity(id.get_OID());
  const auto found = g_pack_to_gmap.find(term.id);
  id = Id(found == g_pack_to_gmap.end() ? term : ::ObjectId(found->second));
}

void RebindOpIds(std::unique_ptr<Op>& op);

void RebindOpTriple(OpTriple& triple) {
  RebindOpId(triple.subject);
  RebindOpId(triple.predicate);
  RebindOpId(triple.object);
}

void RebindOpPath(OpPath& path) {
  RebindOpId(path.subject);
  RebindOpId(path.object);
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

  if (auto* sel = dynamic_cast<OpSelect*>(op.get())) {
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

int ExecuteInsertData(const std::string& query_text) {
  if (g_id_map == nullptr) {
    g_id_map = std::make_unique<yb::mdb::IdMap>();
  }

  yb::mdb::ImportStats stats;
  auto status = yb::mdb::InsertDataExecutor::Execute(
      query_text, *g_id_map, stats, PersistImportedTerm);

  LOG(INFO) << "INSERT DATA: parsed=" << stats.triples_parsed
            << " written=" << stats.triples_written
            << " errors=" << stats.errors;

  if (!status.ok()) {
    LOG(WARNING) << "INSERT DATA failed: " << status;
    return 1;
  }
  if (yb::mdb::RdfYBModel::Instance().Dictionary()) {
    const auto loaded = LoadIdMapFromDocDB();
    if (!loaded.ok()) {
      LOG(WARNING) << loaded;
      return 1;
    }
  }
  return 0;
}

int ExecuteSparql(
    const std::string& query_text,
    ResultSink* sink) {
  auto& model = yb::mdb::RdfYBModel::Instance();
  const bool use_snapshot = model.IsConnected();
  if (use_snapshot) {
    const auto status = model.BeginReadSnapshot();
    if (!status.ok()) {
      LOG(WARNING) << "Read snapshot failed: " << status;
      return 1;
    }
  }
  auto snapshot_cleanup = yb::ScopeExit([&] {
    if (use_snapshot) model.EndReadSnapshot();
  });
  if (g_cluster_initialized) {
    const auto status = LoadIdMapFromDocDB();
    if (!status.ok()) {
      LOG(WARNING) << "Query dictionary refresh failed: " << status;
      return 1;
    }
  }

  std::unique_ptr<Op> op;
  {
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
    op = std::move(visitor.current_op);

    if (op == nullptr) {
      return sink->structured() ? 1 : 0;
    }

    op = QueryParser::rewrite(std::move(op));

    if (!g_pack_to_gmap.empty()) {
      RebindOpIds(op);
    }

  }


  const auto* select = dynamic_cast<const OpSelect*>(op.get());
  std::vector<VarId> output_vars;
  std::vector<std::string> columns;
  if (select) {
    output_vars = select->vars;
    for (const auto var : output_vars) columns.push_back(get_query_ctx().get_var_name(var));
  } else if (const auto* describe = dynamic_cast<const OpDescribe*>(op.get());
             describe && !describe->op && !describe->iris.empty()) {
    columns = {"subject", "predicate", "object"};
  } else if (sink->structured()) {
    LOG(WARNING) << "Structured results support SELECT and direct-IRI DESCRIBE only";
    return 1;
  } else {
    for (size_t i = 0; i < get_query_ctx().get_var_size(); ++i) {
      output_vars.emplace_back(i);
      columns.push_back(get_query_ctx().get_var_name(output_vars.back()));
    }
  }
  if (!sink->Schema(std::move(columns)).ok()) return 1;
  const auto* bgp = select ? dynamic_cast<const OpBasicGraphPattern*>(select->op.get()) : nullptr;
  const bool tablet_query = g_cluster_initialized && bgp && !bgp->triples.empty() &&
      bgp->paths.empty() && select->limit == Op::DEFAULT_LIMIT &&
      select->offset == Op::DEFAULT_OFFSET &&
      std::all_of(select->vars_exprs.begin(), select->vars_exprs.end(),
                  [](const auto& expr) { return expr == nullptr; });
  if (tablet_query) {
    yb::mdb::TabletRouter router;
    yb::mdb::DistributedPlan plan;
    Status status;
    {
      status = router.Init(g_yb_client.get(), g_keyspace);
      if (status.ok()) {
        yb::mdb::DistributedPlanGenerator generator(&router);
        status = generator.Generate(*op, &plan);
      }
    }
    std::vector<yb::mdb::ScatterExecutor::FragmentResultSet> fragments;
    yb::mdb::GatherResult result;
    if (status.ok()) {
      yb::mdb::ScatterExecutor executor;
      status = executor.Execute(plan, &fragments);
    }
    if (status.ok()) {
      yb::mdb::GatherOperator gather;
      status = gather.Merge(fragments, plan.gather_strategy(), &result);
    }
    if (!status.ok()) {
      LOG(WARNING) << "Tablet-aware query failed: " << status;
      return 1;
    }
    LOG(INFO) << "MDB tablet-aware query: fragments=" << plan.num_fragments()
              << " rows=" << result.bindings.size();
    for (const auto& binding : result.bindings) {
      std::vector<ObjectId> values;
      for (const auto var : output_vars) values.push_back((*binding)[var]);
      RETURN_NOT_OK_RET(sink->Row(values), 1);
    }
    return 0;
  }

  BindingIterConstructor optimizer;
  op->accept_visitor(optimizer);
  auto iter = std::move(optimizer.tmp);

  if (auto* describe = dynamic_cast<OpDescribe*>(op.get())) {
    if (!describe->op && !describe->iris.empty()) {
      std::set<std::tuple<uint64_t, uint64_t, uint64_t>> seen_triples;
      const auto identity = [](ObjectId value) {
        const auto found = g_gmap_to_pack.find(value.id);
        return found == g_gmap_to_pack.end() ? value.id : found->second.id;
      };

      for (const auto& iri_str : describe->iris) {
        auto iri_oid = Conversions::pack_iri(iri_str);
        auto it = g_pack_to_gmap.find(iri_oid.id);
        if (it == g_pack_to_gmap.end()) continue;
        ObjectId graph_id(it->second);

        {
          auto plan = std::make_unique<TriplePlan>(
              Id(graph_id), Id(VarId(0)), Id(VarId(1)));
          auto desc_iter = plan->get_binding_iter();
          Binding desc_binding(2);
          desc_iter->begin(desc_binding);
          while (desc_iter->next()) {
            auto pred_oid = desc_binding[VarId(0)];
            auto obj_oid = desc_binding[VarId(1)];
            if (seen_triples.emplace(
                    identity(graph_id), identity(pred_oid), identity(obj_oid)).second) {
              RETURN_NOT_OK_RET(sink->Row({graph_id, pred_oid, obj_oid}), 1);
            }
          }
          if (!desc_iter->status().ok()) return 1;
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
            if (seen_triples.emplace(
                    identity(subj_oid), identity(pred_oid), identity(graph_id)).second) {
              RETURN_NOT_OK_RET(sink->Row({subj_oid, pred_oid, graph_id}), 1);
            }
          }
          if (!desc_iter->status().ok()) return 1;
        }
      }

      return 0;
    }
  }

  if (iter == nullptr) {
    return 0;
  }

  Binding binding(get_query_ctx().get_var_size());
  iter->begin(binding);

  while (iter->next()) {
    std::vector<ObjectId> values;
    for (const auto var : output_vars) values.push_back(binding[var]);
    RETURN_NOT_OK_RET(sink->Row(values), 1);
  }

  if (!iter->status().ok()) {
    LOG(WARNING) << "Query iterator failed: " << iter->status();
    return 1;
  }
  return 0;
}

Status ImportNtFileWithIdMap(const std::string& file_path,
                             yb::mdb::IdMap& id_map,
                             int64_t& parsed, int64_t& written, int64_t& errors) {
  std::vector<yb::mdb::Triple> triples;
  yb::mdb::ImportStats parse_stats;
  const auto parse_status = yb::mdb::NtParser::ParseFile(file_path, triples, parse_stats);
  parsed = parse_stats.triples_parsed;
  errors = parse_stats.errors;
  RETURN_NOT_OK(parse_status);
  const auto scope = yb::Uuid::Generate().ToHexString();
  for (auto& triple : triples) yb::mdb::ScopeBlankNodes(&triple, scope);

  auto& model = yb::mdb::RdfYBModel::Instance();
  bool use_txn = model.UseTransactions();
  auto transaction_cleanup = yb::ScopeExit([&] {
    if (use_txn && model.HasTransaction()) (void)model.AbortTransaction();
  });
  if (use_txn) {
    RETURN_NOT_OK(model.BeginTransaction());
  }

  for (const auto& triple : triples) {
    uint64_t subj_id = VERIFY_RESULT(model.ResolveTerm(
        id_map, {triple.sub_type, triple.sub, "", ""}));
    uint64_t pred_id = VERIFY_RESULT(model.ResolveTerm(
        id_map, {triple.pred_type, triple.pred, "", ""}));

    const auto object = yb::mdb::ObjectTerm(triple);
    uint64_t obj_id = VERIFY_RESULT(model.ResolveTerm(id_map, object));
    RETURN_NOT_OK(PersistImportedTerm(subj_id, {triple.sub_type, triple.sub, "", ""}));
    RETURN_NOT_OK(PersistImportedTerm(pred_id, {triple.pred_type, triple.pred, "", ""}));
    RETURN_NOT_OK(PersistImportedTerm(obj_id, object));

    auto write_status = model.WriteTripleToDocDB(subj_id, pred_id, obj_id);
    if (!write_status.ok()) {
      if (use_txn) {
        (void)model.AbortTransaction();
      }
      return write_status;
    }
    written++;

    if (use_txn && written % 50 == 0) {
      RETURN_NOT_OK(model.CommitTransaction());
      RETURN_NOT_OK(model.BeginTransaction());
    } else if (!use_txn && written % 50 == 0) {
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

static int DispatchQuery(const char* query_text, ResultSink* sink) {
  try {
    std::string query_str(query_text);

    size_t first = 0;
    while (first < query_str.size()) {
      if (query_str[first] == '#') {
        const auto newline = query_str.find('\n', first);
        first = newline == std::string::npos ? query_str.size() : newline + 1;
      } else if (std::string_view(" \t\r\n").find(query_str[first]) != std::string_view::npos) {
        ++first;
      } else {
        break;
      }
    }
    std::string keyword = query_str.substr(first, 6);
    for (auto& c : keyword) if (c >= 'a' && c <= 'z') c -= 'a' - 'A';
    if (keyword == "INSERT") return ExecuteInsertData(query_str);

    return ExecuteSparql(query_str, sink);
  } catch (const std::exception& error) {
    LOG(WARNING) << "MDB query failed: " << error.what();
    return 1;
  }
}

int YBCMdbExecuteSparql(
    const char* query_text, YBCMdbRowCallback row_callback, void* ctx) {
  if (!query_text || !row_callback) return 1;
  ResultSink sink(row_callback, ctx);
  return DispatchQuery(query_text, &sink);
}

int YBCMdbExecuteSparqlRows(
    const char* query_text, YBCMdbSchemaCallback schema_callback,
    YBCMdbResultRowCallback row_callback, void* ctx) {
  if (!query_text || !schema_callback || !row_callback) return 1;
  ResultSink sink(schema_callback, row_callback, ctx);
  return DispatchQuery(query_text, &sink);
}

int YBCMdbInitCluster(const char* master_addrs, const char* keyspace) {
  if (master_addrs == nullptr || keyspace == nullptr) {
    return 1;
  }

  std::string master_addrs_str(master_addrs);
  g_keyspace = std::string(keyspace);

  LOG(INFO) << "Connecting to masters at " << master_addrs_str;

  {
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

  }
  {
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
      builder.AddColumn("c0")->Type(yb::DataType::INT64)->HashPrimaryKey()->NotNull();
      builder.AddColumn("c1")->Type(yb::DataType::INT64)->PrimaryKey()->NotNull();
      builder.AddColumn("c2")->Type(yb::DataType::INT64)->PrimaryKey()->NotNull();

      yb::TableProperties table_properties;
      table_properties.SetTransactional(true);
      builder.SetTableProperties(table_properties);

      yb::client::TableHandle tmp;
      auto status = tmp.Create(
      table_name, kRdfNumTablets, g_yb_client.get(), &builder);
      if (!status.ok() && !status.IsAlreadyPresent()) {
        LOG(WARNING) << "Create RDF table failed: " << status;
        return 1;
      }
    }

    const auto idmap_status = CreateIdMapTable();
    if (!idmap_status.ok()) {
      LOG(WARNING) << "Create RDF dictionary failed: " << idmap_status;
      return 1;
    }

  }
  auto init_status = yb::mdb::RdfYBModel::Instance().Init(
      g_yb_client.get(), g_keyspace);
  if (!init_status.ok()) {
    LOG(WARNING) << "Failed to init RdfYBModel: " << init_status;
    return 1;
  }

  g_id_map = std::make_unique<yb::mdb::IdMap>();

  const auto load_idmap_status = LoadIdMapFromDocDB();
  if (!load_idmap_status.ok()) {
    LOG(WARNING) << "Load RDF dictionary failed: " << load_idmap_status;
    return 1;
  }

  g_cluster_initialized = true;
  LOG(INFO) << "Cluster initialized successfully, keyspace=" << g_keyspace;
  return 0;
}

int YBCMdbImportFile(const char* file_path) try {
  if (file_path == nullptr) {
    return 1;
  }

  if (!g_cluster_initialized) {
    LOG(WARNING) << "Cluster not initialized, call YBCMdbInitCluster first";
    return 1;
  }

  auto& model = yb::mdb::RdfYBModel::Instance();
  if (model.HasTransaction() || model.QueryReadTime()) {
    LOG(WARNING) << "File import cannot replace an active read or write scope";
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

  g_gmap_to_uri.clear();
  for (const auto& [id, uri] : g_id_map->Entries()) {
    g_gmap_to_uri[id] = uri;
  }

  const auto save_status = SaveIdMapToDocDB();
  if (!save_status.ok()) {
    LOG(WARNING) << "Save RDF dictionary failed: " << save_status;
    return 1;
  }

  g_gmap_to_pack.clear();
  for (auto& [pack_id, gmap_id] : g_pack_to_gmap) {
    g_gmap_to_pack[gmap_id] = ObjectId(pack_id);
  }

  return 0;
} catch (const std::exception& error) {
  LOG(WARNING) << "MDB import failed: " << error.what();
  return 1;
}

#pragma once

#include <algorithm>
#include <stdexcept>
#include <string>
#include <vector>

#include "yb/mdb/adapter/mdb_adapter.h"
#include "graph_models/rdf_model/conversions.h"
#include "query/query_context.h"

namespace {

struct ResultTerm {
  YBCMdbTermKind kind = YBC_MDB_UNBOUND;
  std::string value, datatype, language;

  explicit ResultTerm(ObjectId oid) {
    if (oid.is_null()) return;
    if (auto found = g_gmap_to_pack.find(oid.id); found != g_gmap_to_pack.end())
      oid = found->second;
    const auto generic = RDF_OID::get_generic_type(oid);
    if (generic == RDF_OID::GenericType::IRI) {
      kind = YBC_MDB_IRI;
      if (!SPARQL::Conversions::try_unpack_iri(oid, &value))
        throw std::runtime_error("Missing RDF result IRI");
      return;
    }
    if (generic == RDF_OID::GenericType::BLANK) {
      kind = YBC_MDB_BLANK;
      value = "mdb" + std::to_string(oid.id);
      return;
    }
    kind = YBC_MDB_LITERAL;
    value = SPARQL::Conversions::to_lexical_str(oid);
    const std::string xsd = SPARQL::RdfTermIdentity::kXsd;
    if (auto identity = SPARQL::RdfTermRegistry::Find(oid.id)) {
      if (identity->kind == 'G') language = identity->qualifier;
      else if (identity->kind == 'L') datatype = identity->qualifier;
      else throw std::runtime_error("Unexpected RDF result identity");
    } else {
      switch (RDF_OID::get_generic_sub_type(oid)) {
        case RDF_OID::GenericSubType::INTEGER: datatype = xsd + "integer"; break;
        case RDF_OID::GenericSubType::FLOAT: datatype = xsd + "float"; break;
        case RDF_OID::GenericSubType::DOUBLE: datatype = xsd + "double"; break;
        case RDF_OID::GenericSubType::DECIMAL: datatype = xsd + "decimal"; break;
        default: datatype = generic == RDF_OID::GenericType::BOOL ?
            xsd + "boolean" : xsd + "string";
      }
    }
  }
  YBCMdbTerm view() const {
    return {kind, value.c_str(), datatype.c_str(), language.c_str()};
  }
};

class ResultSink {
 public:
  ResultSink(YBCMdbRowCallback legacy, const YBCMdbResultCallbacks* callbacks, void* ctx)
      : legacy_(legacy), callbacks_(callbacks), ctx_(ctx) {}

  void Begin(YBCMdbResultKind kind, const std::vector<VarId>& vars = {}) {
    vars_ = vars;
    if (!callbacks_) return;
    names_.clear(); internal_.clear();
    for (auto var : vars) {
      names_.push_back(get_query_ctx().get_original_var_name(var));
      internal_.push_back(get_query_ctx().get_var_name(var));
    }
    std::vector<YBCMdbColumn> columns;
    for (size_t i = 0; i < vars.size(); ++i)
      columns.push_back({names_[i].c_str(), internal_[i].c_str()});
    callbacks_->begin(kind, columns.data(), columns.size(), ctx_);
  }

  void Row(const std::vector<ObjectId>& ids) {
    if (ids.size() != vars_.size()) throw std::runtime_error("RDF result row width mismatch");
    if (!callbacks_ && std::all_of(ids.begin(), ids.end(), [](auto id) { return id.is_null(); }))
      throw std::runtime_error("Legacy RDF callback cannot represent an empty solution; use typed results");
    std::vector<ResultTerm> storage;
    for (auto id : ids) storage.emplace_back(id);
    std::vector<YBCMdbTerm> terms;
    for (const auto& item : storage) terms.push_back(item.view());
    if (callbacks_) callbacks_->row(terms.data(), terms.size(), ctx_);
    else for (size_t i = 0; i < terms.size(); ++i) {
      if (terms[i].kind == YBC_MDB_UNBOUND) continue;
      const auto name = get_query_ctx().get_var_name(vars_[i]);
      auto value = storage[i].value;
      if (terms[i].kind == YBC_MDB_BLANK) value = "_:" + value;
      legacy_(name.c_str(), value.c_str(), ctx_);
    }
  }

  void Boolean(bool value) {
    if (callbacks_) callbacks_->boolean_result(value, ctx_);
    else legacy_("boolean", value ? "true" : "false", ctx_);
  }

  void Triple(const std::vector<ObjectId>& ids) {
    std::vector<ResultTerm> storage;
    for (auto id : ids) storage.emplace_back(id);
    std::vector<YBCMdbTerm> terms;
    for (const auto& item : storage) terms.push_back(item.view());
    if (callbacks_) callbacks_->triple(terms.data(), ctx_);
    else {
      const char* names[] = {"subject", "predicate", "object"};
      for (size_t i = 0; i < 3; ++i) {
        auto value = storage[i].value;
        if (terms[i].kind == YBC_MDB_BLANK) value = "_:" + value;
        legacy_(names[i], value.c_str(), ctx_);
      }
    }
  }

  void End() { if (callbacks_) callbacks_->end(ctx_); }

 private:
  YBCMdbRowCallback legacy_;
  const YBCMdbResultCallbacks* callbacks_;
  void* ctx_;
  std::vector<VarId> vars_;
  std::vector<std::string> names_, internal_;
};

}  // namespace

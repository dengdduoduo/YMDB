#pragma once

#include "graph_models/rdf_model/conversions.h"
#include "yb/mdb/import/import.h"

namespace yb::mdb {

inline SPARQL::RdfTermIdentity RdfIdentity(
    TripleTermType type, const std::string& lexical, const std::string& qualifier = "",
    const std::string& blank_scope = "") {
  if (type == TripleTermType::URI) return {'I', lexical, ""};
  if (type == TripleTermType::BLANK) {
    if (blank_scope.empty()) throw std::runtime_error("Blank node needs an import scope");
    return {'B', lexical, blank_scope};
  }
  if (!qualifier.empty() && qualifier[0] == '@') {
    return {'G', lexical, SPARQL::RdfTermIdentity::NormalizeLanguage(qualifier.substr(1))};
  }
  // Compatibility for callers of the pre-existing Triple struct: language
  // names had no '@'. Datatype IRIs are absolute and contain a colon.
  if (!qualifier.empty() && qualifier.find(':') == std::string::npos) {
    return {'G', lexical, SPARQL::RdfTermIdentity::NormalizeLanguage(qualifier)};
  }
  return {'L', lexical, qualifier.empty() ?
      std::string(SPARQL::RdfTermIdentity::kXsd) + "string" : qualifier};
}

inline std::string RdfDictionaryKey(const SPARQL::RdfTermIdentity& term) {
  // Preserve existing IRI IDs; all new non-IRI terms have unambiguous keys.
  return term.kind == 'I' ? term.lexical : term.Key();
}

inline ::ObjectId PackRdfIdentity(const SPARQL::RdfTermIdentity& term) {
  using SPARQL::Conversions;
  switch (term.kind) {
    case 'I': return Conversions::pack_iri(term.lexical);
    case 'L': return Conversions::try_pack_string_datatype(term.qualifier, term.lexical);
    case 'G': return Conversions::pack_string_lang(term.qualifier, term.lexical);
    case 'B': return ::ObjectId(SPARQL::RdfTermRegistry::Pack(::ObjectId::MASK_BLANK_INLINED, term));
    default: throw std::runtime_error("Unknown RDF identity");
  }
}

inline void RegisterRdfIdentity(uint64_t graph_id, const SPARQL::RdfTermIdentity& term) {
  const auto packed = PackRdfIdentity(term);
  auto by_packed = g_pack_to_gmap.find(packed.id);
  auto by_graph = g_gmap_to_pack.find(graph_id);
  if ((by_packed != g_pack_to_gmap.end() && by_packed->second != graph_id) ||
      (by_graph != g_gmap_to_pack.end() && by_graph->second != packed)) {
    throw std::runtime_error("Conflicting typed RDF dictionary mapping");
  }
  g_pack_to_gmap[packed.id] = graph_id;
  g_gmap_to_pack[graph_id] = packed;
  if (term.kind == 'I') g_gmap_to_uri[graph_id] = term.lexical;
}

inline SPARQL::RdfTermIdentity DecodeRdfDictionaryEntry(std::string_view key, ::ObjectId packed) {
  if (SPARQL::RdfTermIdentity::IsKey(key)) {
    auto term = SPARQL::RdfTermIdentity::Decode(key);
    if (PackRdfIdentity(term) != packed) throw std::runtime_error("RDF identity checksum mismatch");
    return term;
  }
  if (packed.get_type() == ::ObjectId::MASK_IRI_INLINED ||
      packed.get_type() == ::ObjectId::MASK_IRI_EXTERN) {
    return {'I', std::string(key), ""};
  }
  throw std::runtime_error("Legacy literal identity is ambiguous; reimport into a new keyspace");
}

}  // namespace yb::mdb

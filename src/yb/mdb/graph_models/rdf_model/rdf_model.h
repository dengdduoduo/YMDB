#pragma once
#include <cstdint>
#include <map>
#include <string>
#include <vector>
#include "graph_models/rdf_model/rdf_catalog.h"
#include "query/parser/grammar/sparql/mdb_extensions.h"
#include "query/parser/op/sparql/op.h"
#include "query/parser/paths/regular_path_expr.h"
class SparqlElement;
struct RdfModel {
    static RdfModel& instance() { static RdfModel m; return m; }

    // list of {alias, prefix}
    // These are common prefixes used for queries, in case the user has not
    // defined them in the query.
    // This way IRIs such as xsd:string can be used in queries even if xsd
    // is not explicitly defined.
    std::map<std::string, std::string> default_query_prefixes = {
        { "bd",      "http://www.bigdata.com/rdf#" },
        { "cc",      "http://creativecommons.org/ns#" },
        { "dct",     "http://purl.org/dc/terms/" },
        { "geo",     "http://www.opengis.net/ont/geosparql#" },
        { "hint",    "http://www.bigdata.com/queryHints#" },
        { "mdbfn",   std::string(SPARQL::MDBExtensions::Function::FUNCTION_PREFIX_IRI) },
        { "mdbtype", std::string(SPARQL::MDBExtensions::Type::TYPE_PREFIX_IRI) },
        { "ontolex", "http://www.w3.org/ns/lemon/ontolex#" },
        { "owl",     "http://www.w3.org/2002/07/owl#" },
        { "prov",    "http://www.w3.org/ns/prov#" },
        { "rdf",     "http://www.w3.org/1999/02/22-rdf-syntax-ns#" },
        { "rdfs",    "http://www.w3.org/2000/01/rdf-schema#" },
        { "schema",  "http://schema.org/" },
        { "skos",    "http://www.w3.org/2004/02/skos/core#" },
        { "xsd",     "http://www.w3.org/2001/XMLSchema#" },
        { "p",       "http://www.wikidata.org/prop/" },
        { "pq",      "http://www.wikidata.org/prop/qualifier/" },
        { "pqn",     "http://www.wikidata.org/prop/qualifier/value-normalized/" },
        { "pqv",     "http://www.wikidata.org/prop/qualifier/value/" },
        { "pr",      "http://www.wikidata.org/prop/reference/" },
        { "prn",     "http://www.wikidata.org/prop/reference/value-normalized/" },
        { "prv",     "http://www.wikidata.org/prop/reference/value/" },
        { "psv",     "http://www.wikidata.org/prop/statement/value/" },
        { "ps",      "http://www.wikidata.org/prop/statement/" },
        { "psn",     "http://www.wikidata.org/prop/statement/value-normalized/" },
        { "wd",      "http://www.wikidata.org/entity/" },
        { "wdata",   "http://www.wikidata.org/wiki/Special:EntityData/" },
        { "wdno",    "http://www.wikidata.org/prop/novalue/" },
        { "wdref",   "http://www.wikidata.org/reference/" },
        { "wds",     "http://www.wikidata.org/entity/statement/" },
        { "wdt",     "http://www.wikidata.org/prop/direct/" },
        { "wdtn",    "http://www.wikidata.org/prop/direct-normalized/" },
        { "wdv",     "http://www.wikidata.org/value/" },
        { "wikibase","http://wikiba.se/ontology#" },
    };

    RdfCatalog catalog;
    uint64_t MAX_LIMIT = SPARQL::Op::DEFAULT_LIMIT;
    PathSearchMode path_mode = PathSearchMode::BFS;
};
static RdfModel& rdf_model = RdfModel::instance();

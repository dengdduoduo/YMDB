#!/usr/bin/env python3
"""Generate one SPARQL query per syntax type, covering all 22 templates."""

import random
import json
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from generate_random_queries import (
    parse_nt, SPARQL_PREFIX_BLOCK,
    gen_simple_bgp, gen_join, gen_filter_isiri, gen_filter_bound,
    gen_filter_regex, gen_filter_strstarts, gen_filter_contains, gen_filter_eq,
    gen_filter_sameterm, gen_optional, gen_union,
    gen_path_star, gen_path_plus, gen_path_inverse,
    gen_order_by, gen_distinct, gen_limit, gen_limit_offset, gen_rdf_type,
)

UNIQUE_GENERATORS = [
    ("RND_BGP", gen_simple_bgp),
    ("RND_JOIN", gen_join),
    ("RND_FILTER_isIRI", gen_filter_isiri),
    ("RND_FILTER_BOUND", gen_filter_bound),
    ("RND_FILTER_REGEX", gen_filter_regex),
    ("RND_FILTER_STRSTARTS", gen_filter_strstarts),
    ("RND_FILTER_CONTAINS", gen_filter_contains),
    ("RND_FILTER_eq", gen_filter_eq),
    ("RND_FILTER_sameTerm", gen_filter_sameterm),
    ("RND_OPTIONAL", gen_optional),
    ("RND_UNION", gen_union),
    ("RND_PATH_star", gen_path_star),
    ("RND_PATH_plus", gen_path_plus),
    ("RND_PATH_inverse", gen_path_inverse),
    ("RND_ORDER_ASC", lambda m, i: {"id": i, "template": "RND_ORDER_ASC", "category": "Random ORDER",
                                     "query": f"SELECT ?s ?o WHERE {{ ?s <{random.choice(m['predicates'])}> ?o }} ORDER BY ASC(?s)"}),
    ("RND_ORDER_DESC", lambda m, i: {"id": i, "template": "RND_ORDER_DESC", "category": "Random ORDER",
                                      "query": f"SELECT ?s ?o WHERE {{ ?s <{random.choice(m['predicates'])}> ?o }} ORDER BY DESC(?s)"}),
    ("RND_DISTINCT", gen_distinct),
    ("RND_LIMIT", gen_limit),
    ("RND_LIMIT_OFFSET", gen_limit_offset),
    ("RND_RDF_TYPE", gen_rdf_type),
    ("RND_ASK", lambda m, i: {"id": i, "template": "RND_ASK", "category": "Random ASK",
                               "query": "ASK WHERE { ?s ?p ?o }"}),
    ("RND_SELECT_ALL", lambda m, i: {"id": i, "template": "RND_SELECT_ALL", "category": "Random SELECT",
                                      "query": "SELECT * WHERE { ?s ?p ?o } LIMIT 10"}),
]


def main():
    if len(sys.argv) >= 2:
        dataset_name = sys.argv[1]
    else:
        dataset_name = "ds10000"
    nt_file = os.path.join(os.path.dirname(__file__), f"dataset_{dataset_name}.nt")
    if not os.path.exists(nt_file):
        print(f"ERROR: dataset not found: {nt_file}")
        sys.exit(1)

    random.seed(42)
    meta = parse_nt(nt_file)
    print(f"Parsed {dataset_name}: {len(meta['subjects'])} subjects, "
          f"{len(meta['predicates'])} predicates")

    queries = []
    for idx, (name, gen_fn) in enumerate(UNIQUE_GENERATORS, 1):
        q = gen_fn(meta, idx)
        q["id"] = idx
        queries.append(q)

    out_dir = os.path.join(os.path.dirname(__file__), "queries")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"cover_{dataset_name}.json")
    with open(out_path, "w") as f:
        json.dump(queries, f, indent=2)
    print(f"Generated {len(queries)} syntax-covering queries → {out_path}")


if __name__ == "__main__":
    main()

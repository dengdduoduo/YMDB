#!/usr/bin/env python3
"""
Generate random SPARQL queries based on dataset NT files.
Reproducible via seed=42.

Usage:
  python3 generate_random_queries.py <dataset_name> <pool_size>
  python3 generate_random_queries.py ds100 2000
"""

import random
import re
import json
import os
import sys

PREFIX = "http://example.org/"
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"

SPARQL_PREFIX_BLOCK = """PREFIX ex: <http://example.org/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"""


def parse_nt(filepath):
    """Parse NT file and extract predicates, subjects, objects, types, labels."""
    predicates = set()
    subjects = set()
    objects = set()
    subject_types = {}
    labels = {}

    nt_re = re.compile(
        r'<([^>]+)>\s+<([^>]+)>\s+(?:(<[^>]+>)|("(?:[^"\\]|\\.)*")(?:@(\w+))?(?:\^\^<([^>]+)>)?)\s*\.'
    )

    with open(filepath) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            m = nt_re.match(line)
            if not m:
                continue
            s = m.group(1)
            p = m.group(2)
            o_iri = m.group(3)
            o_lit = m.group(4)
            o_lang = m.group(5)

            predicates.add(p)
            subjects.add(s)

            if o_iri:
                objects.add(o_iri)
            elif o_lit:
                pass

            if p == RDF_TYPE and o_iri:
                subject_types[s] = o_iri

            if p == RDFS_LABEL and o_lit:
                labels[s] = o_lit

    return {
        "predicates": sorted(predicates),
        "subjects": sorted(subjects),
        "objects": sorted(objects),
        "subject_types": subject_types,
        "labels": labels,
    }


def gen_simple_bgp(meta, idx):
    pred = random.choice(meta["predicates"])
    return {
        "id": idx,
        "template": "RND_BGP",
        "category": "Random BGP",
        "query": f"SELECT ?s ?o WHERE {{ ?s <{pred}> ?o }}",
    }


def gen_join(meta, idx):
    p1 = random.choice(meta["predicates"])
    p2 = random.choice(meta["predicates"])
    return {
        "id": idx,
        "template": "RND_JOIN",
        "category": "Random Join",
        "query": f"SELECT ?s ?o ?z WHERE {{ ?s <{p1}> ?o . ?o <{p2}> ?z }}",
    }


def gen_filter_isiri(meta, idx):
    pred = random.choice(meta["predicates"])
    return {
        "id": idx,
        "template": "RND_FILTER_isIRI",
        "category": "Random FILTER",
        "query": f"SELECT ?s ?o WHERE {{ ?s <{pred}> ?o . FILTER(isIRI(?s)) }}",
    }


def gen_filter_bound(meta, idx):
    pred = random.choice(meta["predicates"])
    return {
        "id": idx,
        "template": "RND_FILTER_BOUND",
        "category": "Random FILTER",
        "query": f"SELECT ?s ?o WHERE {{ ?s <{pred}> ?o . FILTER(BOUND(?o)) }}",
    }


def gen_filter_regex(meta, idx):
    patterns = ["Person", "Company", "City", "Product", "Department",
                "Project", "Technology", "Event", "Organization", "Service"]
    pat = random.choice(patterns)
    return {
        "id": idx,
        "template": "RND_FILTER_REGEX",
        "category": "Random FILTER",
        "query": f'SELECT ?s ?l WHERE {{ ?s rdfs:label ?l . FILTER(REGEX(?l, "{pat}")) }}',
    }


def gen_filter_strstarts(meta, idx):
    prefixes = ["Person", "Company", "City", "Product", "Department",
                "Project", "Technology", "Event", "Organization", "Service"]
    pfx = random.choice(prefixes)
    return {
        "id": idx,
        "template": "RND_FILTER_STRSTARTS",
        "category": "Random FILTER",
        "query": f'SELECT ?s ?l WHERE {{ ?s rdfs:label ?l . FILTER(STRSTARTS(?l, "{pfx}")) }}',
    }


def gen_filter_contains(meta, idx):
    parts = ["er", "on", "an", "ct", "en", "ce", "em", "De", "Pr", "Te"]
    sub = random.choice(parts)
    return {
        "id": idx,
        "template": "RND_FILTER_CONTAINS",
        "category": "Random FILTER",
        "query": f'SELECT ?s ?l WHERE {{ ?s rdfs:label ?l . FILTER(CONTAINS(?l, "{sub}")) }}',
    }


def gen_filter_eq(meta, idx):
    pred = random.choice(meta["predicates"])
    return {
        "id": idx,
        "template": "RND_FILTER_eq",
        "category": "Random FILTER",
        "query": f"SELECT ?s ?o WHERE {{ ?s <{pred}> ?o . FILTER(?s != ?o) }}",
    }


def gen_filter_sameterm(meta, idx):
    pred = random.choice(meta["predicates"])
    return {
        "id": idx,
        "template": "RND_FILTER_sameTerm",
        "category": "Random FILTER",
        "query": f"SELECT ?s ?o WHERE {{ ?s <{pred}> ?o . FILTER(sameTerm(?s, ?s)) }}",
    }


def gen_optional(meta, idx):
    p1 = random.choice(meta["predicates"])
    p2 = random.choice(meta["predicates"])
    return {
        "id": idx,
        "template": "RND_OPTIONAL",
        "category": "Random OPTIONAL",
        "query": f"SELECT ?s ?o ?z WHERE {{ ?s <{p1}> ?o . OPTIONAL {{ ?o <{p2}> ?z }} }}",
    }


def gen_union(meta, idx):
    p1 = random.choice(meta["predicates"])
    p2 = random.choice(meta["predicates"])
    return {
        "id": idx,
        "template": "RND_UNION",
        "category": "Random UNION",
        "query": f"SELECT ?s ?o WHERE {{ {{ ?s <{p1}> ?o }} UNION {{ ?s <{p2}> ?o }} }}",
    }


def gen_path_star(meta, idx):
    pred = random.choice(meta["predicates"])
    return {
        "id": idx,
        "template": "RND_PATH_star",
        "category": "Random Path",
        "query": f"SELECT ?s ?o WHERE {{ ?s <{pred}>* ?o }}",
    }


def gen_path_plus(meta, idx):
    pred = random.choice(meta["predicates"])
    return {
        "id": idx,
        "template": "RND_PATH_plus",
        "category": "Random Path",
        "query": f"SELECT ?s ?o WHERE {{ ?s <{pred}>+ ?o }}",
    }


def gen_path_inverse(meta, idx):
    pred = random.choice(meta["predicates"])
    return {
        "id": idx,
        "template": "RND_PATH_inverse",
        "category": "Random Path",
        "query": f"SELECT ?s ?o WHERE {{ ?s ^<{pred}> ?o }}",
    }


def gen_order_by(meta, idx):
    pred = random.choice(meta["predicates"])
    direction = random.choice(["ASC", "DESC"])
    return {
        "id": idx,
        "template": f"RND_ORDER_{direction}",
        "category": "Random ORDER",
        "query": f"SELECT ?s ?o WHERE {{ ?s <{pred}> ?o }} ORDER BY {direction}(?s)",
    }


def gen_distinct(meta, idx):
    pred = random.choice(meta["predicates"])
    return {
        "id": idx,
        "template": "RND_DISTINCT",
        "category": "Random DISTINCT",
        "query": f"SELECT DISTINCT ?s ?o WHERE {{ ?s <{pred}> ?o }}",
    }


def gen_limit(meta, idx):
    pred = random.choice(meta["predicates"])
    lim = random.choice([5, 10, 20, 50])
    return {
        "id": idx,
        "template": "RND_LIMIT",
        "category": "Random LIMIT",
        "query": f"SELECT ?s ?o WHERE {{ ?s <{pred}> ?o }} LIMIT {lim}",
    }


def gen_limit_offset(meta, idx):
    pred = random.choice(meta["predicates"])
    lim = random.choice([5, 10, 20])
    return {
        "id": idx,
        "template": "RND_LIMIT_OFFSET",
        "category": "Random LIMIT",
        "query": f"SELECT ?s ?o WHERE {{ ?s <{pred}> ?o }} LIMIT {lim} OFFSET {lim}",
    }


def gen_rdf_type(meta, idx):
    return {
        "id": idx,
        "template": "RND_RDF_TYPE",
        "category": "Random BGP",
        "query": "SELECT ?s ?t WHERE { ?s rdf:type ?t }",
    }


GENERATORS = [
    gen_simple_bgp, gen_simple_bgp,
    gen_join, gen_join,
    gen_filter_isiri, gen_filter_isiri,
    gen_filter_bound, gen_filter_bound,
    gen_filter_regex, gen_filter_regex,
    gen_filter_strstarts, gen_filter_strstarts,
    gen_filter_contains, gen_filter_contains,
    gen_filter_eq, gen_filter_eq,
    gen_filter_sameterm, gen_filter_sameterm,
    gen_optional, gen_optional,
    gen_union, gen_union,
    gen_path_star, gen_path_star,
    gen_path_plus, gen_path_plus,
    gen_path_inverse, gen_path_inverse,
    gen_order_by, gen_order_by,
    gen_distinct, gen_distinct,
    gen_limit, gen_limit,
    gen_limit_offset, gen_limit_offset,
    gen_rdf_type, gen_rdf_type,
    gen_rdf_type,
]


def main():
    if len(sys.argv) < 3:
        print("Usage: generate_random_queries.py <dataset_name> <pool_size>")
        sys.exit(1)

    dataset_name = sys.argv[1]
    pool_size = int(sys.argv[2])

    nt_file = os.path.join(os.path.dirname(__file__), f"dataset_{dataset_name}.nt")
    if not os.path.exists(nt_file):
        print(f"ERROR: dataset not found: {nt_file}")
        sys.exit(1)

    random.seed(42)

    meta = parse_nt(nt_file)
    print(f"Parsed {dataset_name}: {len(meta['subjects'])} subjects, "
          f"{len(meta['predicates'])} predicates, {len(meta['objects'])} objects")

    queries_list = []
    current_queries = set()
    max_attempts = pool_size * 5
    attempts = 0
    while len(queries_list) < pool_size and attempts < max_attempts:
        gen = random.choice(GENERATORS)
        q = gen(meta, len(queries_list) + 1)
        if q["query"] not in current_queries:
            current_queries.add(q["query"])
            q["id"] = len(queries_list) + 1
            queries_list.append(q)
        attempts += 1

    out_dir = os.path.join(os.path.dirname(__file__), "queries")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"random_{dataset_name}.json")
    with open(out_path, "w") as f:
        json.dump(queries_list, f, indent=2)
    print(f"Generated {len(queries_list)} unique random queries → {out_path}")


if __name__ == "__main__":
    main()

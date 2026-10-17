#!/usr/bin/env python3
"""
Generate full + incremental datasets for YB critical point testing.
Args: <base_name> <new_name>
Example: ds2000 → ds4000
  2000 base nodes, 1000 base edges
  + 2000 new nodes, + 1000 new edges
  Full ds4000: 4000 nodes, 2000 edges (for MDB fresh import)
  Incremental: only 2000 new nodes + 1000 new edges (for YB)
"""

import random
import os
import sys

random.seed(42)

PREFIX = "http://example.org/"
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"

ENTITY_TYPES = [
    "Person", "Company", "Product", "City", "Department",
    "Project", "Technology", "Event", "Organization", "Service",
]

PREDICATE_NAMES = [
    "worksFor", "locatedIn", "manages", "knows", "developedBy",
    "contains", "reportsTo", "memberOf", "fundedBy", "collaboratesWith",
    "produces", "sells", "buys", "owns", "partnersWith",
    "reports", "supervises", "advises", "mentors", "sponsors",
    "competesWith", "dependsOn", "implements", "designs", "tests",
    "deploys", "maintains", "operates", "supports", "integratesWith",
    "regulates", "audits", "certifies", "trains", "evaluates",
    "plans", "executes", "monitors", "reviews", "approves",
    "submits", "validates", "documents", "publishes", "distributes",
    "requires", "provides", "consumes", "generates", "processes",
    "transforms", "stores", "retrieves", "archives", "secures",
    "authenticates", "authorizes", "encrypts", "decrypts", "hashes",
    "encodes", "decodes", "compresses", "decompresses", "serializes",
    "deserializes", "formats", "parses", "renders", "visualizes",
    "compares", "aligns", "merges", "splits", "filters",
    "aggregates", "sorts", "indexes", "ranks", "scores",
    "predicts", "classifies", "clusters", "recommends", "optimizes",
    "schedules", "dispatches", "notifies", "escalates", "resolves",
    "measures", "analyzes", "reportsOn", "governs", "oversees",
    "investsIn", "acquires", "divests", "licenses", "outsources",
]


def escape_nt(s):
    return '"' + s.replace('\\', '\\\\').replace('"', '\\"') + '"'


def gen_nodes(start_id, count):
    uris, types = [], []
    for i in range(start_id, start_id + count):
        etype = ENTITY_TYPES[i % len(ENTITY_TYPES)]
        uris.append(f"{PREFIX}{etype}{i}")
        types.append(etype)
    return uris, types


def node_triples(uris, types):
    lines = []
    for uri, etype in zip(uris, types):
        lines.append(f"<{uri}> <{RDF_TYPE}> <{PREFIX}{etype}> .")
        label = f"{etype} #{uris.index(uri) + 1}"  # hacky but works
        lines.append(f"<{uri}> <{RDFS_LABEL}> {escape_nt(label)} .")
    return lines


def edge_triples(uris, count, relations):
    preds = PREDICATE_NAMES[:relations]
    lines, n = [], len(uris)
    for _ in range(count):
        s = uris[random.randint(0, n - 1)]
        o = uris[random.randint(0, n - 1)]
        p = preds[random.randint(0, relations - 1)]
        lines.append(f"<{s}> <{PREFIX}{p}> <{o}> .")
    return lines


def main():
    if len(sys.argv) < 3:
        print("Usage: generate_incremental.py <base_name> <new_name>")
        sys.exit(1)

    base_name = sys.argv[1]
    new_name = sys.argv[2]
    import re
    base_n = int(re.search(r'(\d+)', base_name).group(1))
    new_n = int(re.search(r'(\d+)', new_name).group(1))
    delta_n = new_n - base_n
    relations = 100
    sdir = os.path.dirname(os.path.abspath(__file__))

    random.seed(42)

    # --- Full file (for MDB) ---
    full_uris, full_types = gen_nodes(1, new_n)
    full_lines = node_triples(full_uris, full_types)
    full_edges = new_n // 2
    full_lines += edge_triples(full_uris, full_edges, relations)
    full_path = os.path.join(sdir, f"dataset_{new_name}.nt")
    with open(full_path, "w") as f:
        f.write("\n".join(full_lines) + "\n")
    print(f"Full {new_name}: {new_n} nodes, {full_edges} edges → {new_n*2 + full_edges} triples")

    # --- Incremental file (for YB) ---
    random.seed(42)
    # Skip state used by full file generation to get independent edges
    for _ in range(new_n * 2 + full_edges):
        random.random()

    new_start = base_n + 1
    inc_uris, inc_types = gen_nodes(new_start, delta_n)
    inc_lines = node_triples(inc_uris, inc_types)
    # Edges across ALL nodes
    all_uris = [f"{PREFIX}{ENTITY_TYPES[i % len(ENTITY_TYPES)]}{i}" for i in range(1, new_n + 1)]
    inc_edges = delta_n // 2
    inc_lines += edge_triples(all_uris, inc_edges, relations)
    inc_path = os.path.join(sdir, f"dataset_{base_name}_to_{new_name}_inc.nt")
    with open(inc_path, "w") as f:
        f.write("\n".join(inc_lines) + "\n")
    print(f"Incremental {base_name}→{new_name}: +{delta_n} nodes, +{inc_edges} edges → {delta_n*2 + inc_edges} triples")


if __name__ == "__main__":
    main()

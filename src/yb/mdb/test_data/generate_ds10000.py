#!/usr/bin/env python3
"""Generate ds10000 dataset: 10000 nodes, 5000 edges, 100 relations."""

import random
import os

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
    s = s.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r')
    return f'"{s}"'


def main():
    nodes = 10000
    edges = 5000
    relations = min(100, len(PREDICATE_NAMES))
    pred_names = PREDICATE_NAMES[:relations]

    out_path = os.path.join(os.path.dirname(__file__), "dataset_ds10000.nt")
    lines = []

    node_uris = []
    node_types = []
    for i in range(1, nodes + 1):
        etype = ENTITY_TYPES[i % len(ENTITY_TYPES)]
        uri = f"{PREFIX}{etype}{i}"
        node_uris.append(uri)
        node_types.append(etype)

    for i, uri in enumerate(node_uris):
        etype = node_types[i]
        lines.append(f"<{uri}> <{RDF_TYPE}> <{PREFIX}{etype}> .")
        lines.append(f"<{uri}> <{RDFS_LABEL}> {escape_nt(f'{etype} #{i+1}')} .")

    for _ in range(edges):
        s_idx = random.randint(0, nodes - 1)
        o_idx = random.randint(0, nodes - 1)
        pred = pred_names[random.randint(0, relations - 1)]
        lines.append(f"<{node_uris[s_idx]}> <{PREFIX}{pred}> <{node_uris[o_idx]}> .")

    lines.append("")
    with open(out_path, "w") as f:
        f.write("\n".join(lines))

    n_triples = nodes * 2 + edges
    print(f"Generated ds10000: {nodes} nodes, {edges} edges, "
          f"{relations} relations → {n_triples} triples → {out_path}")


if __name__ == "__main__":
    main()

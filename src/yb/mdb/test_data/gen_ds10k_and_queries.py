#!/usr/bin/env python3
"""Generate ds10000 and 22 syntax-covering queries using existing templates.

By default, write dataset_ds10000.nt and queries/cover_ds10000.json beside
this script. Use --output-dir to generate fixtures in a separate directory.
"""

import argparse
import json
from pathlib import Path
import random

from generate_cover_queries import UNIQUE_GENERATORS
from generate_ds10000 import ENTITY_TYPES, PREDICATE_NAMES, PREFIX, RDF_TYPE, RDFS_LABEL, escape_nt
from generate_random_queries import parse_nt


def generate_dataset(output_dir, seed):
    """Write 10,000 nodes, 5,000 edges and 100 available edge predicates."""
    nodes = 10000
    edges = 5000
    predicates = PREDICATE_NAMES[:100]
    rng = random.Random(seed)
    output_dir.mkdir(parents=True, exist_ok=True)
    dataset_path = output_dir / "dataset_ds10000.nt"

    node_types = [ENTITY_TYPES[i % len(ENTITY_TYPES)] for i in range(1, nodes + 1)]
    node_uris = [f"{PREFIX}{kind}{i}" for i, kind in enumerate(node_types, 1)]
    with dataset_path.open("w", encoding="utf-8", newline="\n") as output:
        for i, (uri, kind) in enumerate(zip(node_uris, node_types), 1):
            output.write(f"<{uri}> <{RDF_TYPE}> <{PREFIX}{kind}> .\n")
            output.write(f"<{uri}> <{RDFS_LABEL}> {escape_nt(f'{kind} #{i}')} .\n")
        for _ in range(edges):
            subject = rng.choice(node_uris)
            obj = rng.choice(node_uris)
            predicate = rng.choice(predicates)
            output.write(f"<{subject}> <{PREFIX}{predicate}> <{obj}> .\n")

    print(f"Generated {nodes * 2 + edges} triples: {dataset_path}")
    return dataset_path


def generate_queries(dataset_path, output_dir, seed):
    """Preserve the JSON format consumed by the existing benchmark runners."""
    metadata = parse_nt(str(dataset_path))
    queries = []
    # Existing templates use the module-level random generator. Restore its
    # state afterwards so calling this helper does not consume the caller's RNG.
    previous_state = random.getstate()
    try:
        random.seed(seed)
        for index, (_, generator) in enumerate(UNIQUE_GENERATORS, 1):
            query = generator(metadata, index)
            query["id"] = index
            queries.append(query)
    finally:
        random.setstate(previous_state)

    query_dir = output_dir / "queries"
    query_dir.mkdir(parents=True, exist_ok=True)
    query_path = query_dir / "cover_ds10000.json"
    with query_path.open("w", encoding="utf-8", newline="\n") as output:
        json.dump(queries, output, indent=2)
        output.write("\n")
    print(f"Generated {len(queries)} syntax-covering queries: {query_path}")
    return query_path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir", type=Path, default=Path(__file__).resolve().parent,
        help="Directory for the dataset and queries subdirectory",
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed (default: 42)")
    args = parser.parse_args()
    dataset_path = generate_dataset(args.output_dir, args.seed)
    generate_queries(dataset_path, args.output_dir, args.seed)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# Copyright (c) YugabyteDB, Inc.
"""Generate deterministic two-hop baseline inputs, preserving existing files."""
import argparse
import hashlib
import json
from pathlib import Path


def generate(shape, count):
    triples = []
    for index in range(count):
        key = (100000 + index if shape == "unique" else
               100000 if shape == "shared" else 100000 + index % 64)
        triples.append((100 + index, 9000, key))
        if shape == "unique":
            triples.append((key, 9001, 200000 + index))
    if shape == "shared":
        triples.extend([(100000, 9001, 200000), (100000, 9001, 200001)])
    elif shape == "fanout":
        triples.extend((100000 + key, 9001, 200000 + key * 16 + obj)
                       for key in range(64) for obj in range(16))
    payload = "".join(f"<urn:p6:{s}> <urn:p6:{p}> <urn:p6:{o}> .\n"
                      for s, p, o in triples).encode("ascii")
    return payload, len(triples), len({term for row in triples for term in row})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--sizes", type=int, nargs="+", default=[128, 4096])
    args = parser.parse_args()
    if any(count < 1 or count > 1000000 for count in args.sizes):
        parser.error("sizes must be between 1 and 1000000")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    manifest = []
    for count in args.sizes:
        for shape in ["unique", "shared", "fanout"]:
            payload, triples, terms = generate(shape, count)
            path = output / f"{shape}-{count}.nt"
            if path.exists():
                if path.read_bytes() != payload:
                    raise RuntimeError(f"Refusing to replace different data: {path}")
            else:
                path.write_bytes(payload)
            manifest.append(dict(shape=shape, n=count, path=str(path), triples=triples,
                                 rows=count * (1 if shape == "unique" else
                                               2 if shape == "shared" else 16),
                                 distinct_terms=terms,
                                 sha256=hashlib.sha256(payload).hexdigest()))
    manifest_path = output / "datasets.json"
    text = json.dumps(manifest, indent=2) + "\n"
    if manifest_path.exists() and json.loads(manifest_path.read_text()) != manifest:
        raise RuntimeError(f"Refusing to replace a different manifest: {manifest_path}")
    if not manifest_path.exists():
        manifest_path.write_text(text)
    print(f"Generated or verified {len(manifest)} datasets in {output}")


if __name__ == "__main__":
    main()

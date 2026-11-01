# Copyright (c) YugabyteDB, Inc.
"""Generate the frozen graph fixture or validate a complete SPARQL JSON result."""

import argparse
from collections import Counter
import gzip
import hashlib
import json
from pathlib import Path


def iri(case, kind, index):
    return 'urn:r209:' + case + ':' + kind + str(index).zfill(4)


def oracle(case):
    if case == 'shared1024':
        return [(iri(case, 's', i), iri(case, 'o', j))
                for i in range(1024) for j in range(8)]
    if case == 'unique1024':
        return [(iri(case, 's', i), iri(case, 'o', i)) for i in range(1024)]
    if case != 'twohop512':
        raise ValueError('Unknown fixture')
    return [(iri(case, 's', i), iri(case, 'l', (i % 16 * 4 + j) % 7))
            for i in range(512) for j in range(4)]


def generate(output):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        raise ValueError('Output directory must be empty')
    triples = set()
    cases = []
    for name in ['shared1024', 'unique1024', 'twohop512']:
        predicate = 'urn:r209:' + name + ':seed'
        edge = 'urn:r209:' + name + ':edge'
        following = 'urn:r209:' + name + ':next'
        count = 512 if name == 'twohop512' else 1024
        for index in range(count):
            key = 0 if name == 'shared1024' else index if name == 'unique1024' else index % 16
            triples.add((iri(name, 's', index), predicate, iri(name, 'k', key)))
        keys = 1 if name == 'shared1024' else 1024 if name == 'unique1024' else 16
        neighbors = 8 if name == 'shared1024' else 1 if name == 'unique1024' else 4
        for key in range(keys):
            for neighbor in range(neighbors):
                obj = neighbor if name == 'shared1024' else key if name == 'unique1024' else (
                    key * 4 + neighbor)
                triples.add((iri(name, 'k', key), edge, iri(name, 'o', obj)))
        if name == 'twohop512':
            for obj in range(64):
                triples.add((iri(name, 'o', obj), following, iri(name, 'l', obj % 7)))
        tail = '?m . ?m <' + following + '> ?o' if name == 'twohop512' else '?o'
        query = 'SELECT ?s ?o WHERE { ?s <' + predicate + '> ?k . ?k <' + edge + '> ' + tail + ' }'
        expected = gzip.compress(json.dumps(oracle(name), separators=(',', ':')).encode(), mtime=0)
        (output / (name + '.expected.json.gz')).write_bytes(expected)
        cases.append(dict(
            name=name, query=query, query_sha256=hashlib.sha256(query.encode()).hexdigest(),
            columns=['s', 'o'], expected_rows=len(oracle(name)),
            expected_file=name + '.expected.json.gz',
            expected_sha256=hashlib.sha256(expected).hexdigest()))
    assert len(triples) == 3720
    data = ('\n'.join('<' + s + '> <' + p + '> <' + o + '> .'
                      for s, p, o in sorted(triples)) + '\n').encode()
    (output / 'dataset.nt').write_bytes(data)
    manifest = dict(triples=3720, sha256=hashlib.sha256(data).hexdigest(), cases=cases)
    (output / 'dataset-manifest.json').write_text(json.dumps(manifest, indent=2))


def validate(manifest_path, name, result_path):
    manifest_path = Path(manifest_path)
    manifest = json.loads(manifest_path.read_text())
    case = next(case for case in manifest['cases'] if case['name'] == name)
    raw = (manifest_path.parent / case['expected_file']).read_bytes()
    if hashlib.sha256(raw).hexdigest() != case['expected_sha256']:
        raise ValueError('Expected-result digest mismatch')
    expected = Counter(tuple(row) for row in json.loads(gzip.decompress(raw)))
    document = json.loads(Path(result_path).read_text())
    if document['head']['vars'] != case['columns']:
        raise ValueError('Projection order mismatch')
    actual = []
    for row in document['results']['bindings']:
        if set(row) != set(case['columns']):
            raise ValueError('Result columns mismatch')
        if any(term.get('type') != 'uri' or set(term) != {'type', 'value'}
               for term in row.values()):
            raise ValueError('RDF term identity mismatch')
        actual.append(tuple(row[column]['value'] for column in case['columns']))
    if Counter(actual) != expected:
        raise ValueError('Complete result multiset mismatch')
    return {'complete_typed_bag': True, 'rows': len(actual), 'case': name}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    gen = sub.add_parser('generate')
    gen.add_argument('--output', required=True)
    check = sub.add_parser('validate')
    check.add_argument('--manifest', required=True)
    check.add_argument('--case', required=True, choices=['shared1024', 'unique1024', 'twohop512'])
    check.add_argument('--result', required=True)
    args = parser.parse_args()
    if args.command == 'generate':
        generate(args.output)
    else:
        print(json.dumps(validate(args.manifest, args.case, args.result)))


if __name__ == '__main__':
    main()

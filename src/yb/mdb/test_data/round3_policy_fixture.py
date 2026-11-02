# Copyright (c) YugabyteDB, Inc.
"""Independent synthetic RDF fixtures and complete typed/ordered result oracle.

Only Python's standard library is required. No MDB result generates an oracle.
Expected large bags follow the documented index formulas, independently of the
set of emitted triples. Existing round-two fixtures are never overwritten.
"""
import argparse
from collections import Counter
import gzip
import hashlib
import json
from pathlib import Path


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def uri(value):
    return {'type': 'uri', 'value': value}


def iri(case, kind, index=0):
    return 'urn:r3:' + case + ':' + kind + str(index).zfill(6)


def generate(output):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        raise ValueError('Refusing to overwrite a nonempty fixture directory')
    triples = set()
    cases = []

    def add(s, p, o):
        triples.add('<' + s + '> <' + p + '> ' + o + ' .')

    def edge(s, p, o):
        add(s, p, '<' + o + '>')

    def freeze(name, query, columns, rows, ordered=False, performance=False):
        raw = gzip.compress(json.dumps(rows, separators=(',', ':'), sort_keys=True).encode(), mtime=0)
        expected = name + '.expected.json.gz'
        (output / expected).write_bytes(raw)
        (output / (name + '.rq')).write_text(query + '\n', encoding='utf8')
        cases.append(dict(name=name, query=query, query_sha256=digest(query.encode()),
                          columns=columns, expected_rows=len(rows), expected_file=expected,
                          expected_sha256=digest(raw), ordered=ordered, performance=performance,
                          timeout_seconds=120, buffered_worker_bytes=8388608,
                          gather_bytes=67108864))

    for n in (1024, 4096):
        for shape in ('shared', 'unique', 'twohop', 'incoming', 'skew'):
            name = shape + str(n)
            def at(kind, i=0):
                return iri(name, kind, i)
            p, q, r = at('seed'), at('edge'), at('next')
            key_count = n if shape == 'unique' else 16 if shape == 'twohop' else 1
            if shape == 'skew':
                key_count = 1 + n // 4
            for i in range(n):
                k = i if shape == 'unique' else i % 16 if shape == 'twohop' else (
                    0 if i < 3 * n // 4 else i - 3 * n // 4 + 1) if shape == 'skew' else 0
                edge(at('s', i), p, at('k', k))
            for k in range(key_count):
                fanout = 1 if shape == 'unique' or (shape == 'skew' and k) else 4 if shape == 'twohop' else 8
                for j in range(fanout):
                    obj = k if shape == 'unique' else 4 * k + j if shape == 'twohop' else (
                        k + 7 if k else j) if shape == 'skew' else j
                    if shape == 'incoming':
                        edge(at('o', obj), q, at('k', k))
                    else:
                        edge(at('k', k), q, at('o', obj))
            if shape == 'twohop':
                for i in range(64):
                    edge(at('o', i), r, at('l', i % 7))
            tail = '?o <' + q + '> ?k' if shape == 'incoming' else '?k <' + q + '> ?o'
            if shape == 'twohop':
                tail = '?k <' + q + '> ?m . ?m <' + r + '> ?o'
            query = 'SELECT ?s ?o WHERE { ?s <' + p + '> ?k . ' + tail + ' }'
            # Closed-form oracle: independent from adjacency traversal/emission.
            pairs = []
            for i in range(n):
                if shape == 'unique':
                    dest = [('o', i)]
                elif shape == 'twohop':
                    dest = [('l', ((i % 16) * 4 + j) % 7) for j in range(4)]
                elif shape == 'skew' and i >= 3 * n // 4:
                    dest = [('o', i - 3 * n // 4 + 8)]
                else:
                    dest = [('o', j) for j in range(8)]
                pairs.extend([uri(at('s', i)), uri(at(kind, j))] for kind, j in dest)
            freeze(name, query, ['s', 'o'], pairs, performance=True)

    p, q = 'urn:r3:small:p', 'urn:r3:small:q'
    s, k, m = ['urn:r3:small:' + v for v in ('s', 'k', 'm')]
    terms = [uri('urn:r3:small:o'),
             {'type': 'literal', 'value': 'hello', 'xml:lang': 'en'},
             {'type': 'literal', 'value': '7', 'datatype': 'http://www.w3.org/2001/XMLSchema#integer'},
             {'type': 'literal', 'value': '7', 'datatype': 'http://www.w3.org/2001/XMLSchema#string'}]
    edge(s, p, k)
    edge(s, p, m)
    for witness in (k, m):
        for term in terms:
            obj = '<' + term['value'] + '>' if term['type'] == 'uri' else (
                json.dumps(term['value']) + ('@en' if 'xml:lang' in term else
                '^^<' + term['datatype'] + '>' if 'datatype' in term and
                term['datatype'] != 'http://www.w3.org/2001/XMLSchema#string' else ''))
            add(witness, q, obj)
    body = '?s <' + p + '> ?k . ?k <' + q + '> ?o'
    freeze('typed_witnesses', 'SELECT ?o ?s WHERE { ' + body + ' }', ['o', 's'],
           [[t, uri(s)] for _ in range(2) for t in terms])
    values = 'VALUES ?s { ' + ('<' + s + '> ') * 8 + '} '
    freeze('duplicate_values', 'SELECT ?s ?o WHERE { ' + values + body + ' }', ['s', 'o'],
           [[uri(s), t] for _ in range(16) for t in terms])
    freeze('bound_neighbor', 'SELECT ?s WHERE { ?s <' + p + '> ?k . ?k <' + q +
           '> <urn:r3:small:o> }', ['s'], [[uri(s)], [uri(s)]])
    freeze('empty', 'SELECT ?s ?o WHERE { ?s <' + p + '> ?k . ?k <' + q +
           '> ?o FILTER(?s = <urn:r3:absent>) }', ['s', 'o'], [])
    freeze('distinct', 'SELECT DISTINCT ?o WHERE { ' + body + ' }', ['o'], [[t] for t in terms])
    name = 'shared1024'
    ordered = [[uri(iri(name, 's', i)), uri(iri(name, 'o', j))]
               for i in range(1024) for j in range(8)][5:18]
    base = next(c['query'] for c in cases if c['name'] == name)
    freeze('ordered_slice', base + ' ORDER BY ?s ?o OFFSET 5 LIMIT 13', ['s', 'o'], ordered, ordered=True)
    incoming = ('SELECT ?k ?o WHERE { VALUES ?o { ' + ('<urn:r3:small:o> ' * 8) +
                '} ?k <' + q + '> ?o }')
    freeze('reverse_values', incoming, ['k', 'o'], [[uri(w), terms[0]] for _ in range(8) for w in (k, m)])
    data = ('\n'.join(sorted(triples)) + '\n').encode()
    (output / 'dataset.nt').write_bytes(data)
    manifest = dict(format_version=1, generator_sha256=digest(Path(__file__).read_bytes()),
                    triples=len(triples), sha256=digest(data), scales=[1024, 4096],
                    oracle='closed-form index formulas; explicitly enumerated small typed bags', cases=cases)
    (output / 'dataset-manifest.json').write_text(json.dumps(manifest, indent=2), encoding='utf8')
    return manifest


def load_case(manifest_path, name):
    manifest_path = Path(manifest_path)
    manifest = json.loads(manifest_path.read_text())
    if digest((manifest_path.parent / 'dataset.nt').read_bytes()) != manifest['sha256']:
        raise ValueError('Dataset digest mismatch')
    case = next(c for c in manifest['cases'] if c['name'] == name)
    if digest(case['query'].encode()) != case['query_sha256']:
        raise ValueError('Query digest mismatch')
    raw = (manifest_path.parent / case['expected_file']).read_bytes()
    if digest(raw) != case['expected_sha256']:
        raise ValueError('Expected digest mismatch')
    return manifest, case, json.loads(gzip.decompress(raw))


def validate(manifest_path, name, result_path):
    _, case, expected = load_case(manifest_path, name)
    document = json.loads(Path(result_path).read_text())
    if document['head']['vars'] != case['columns']:
        raise ValueError('Projection order mismatch')
    rows = document['results']['bindings']
    if any(set(row) != set(case['columns']) for row in rows):
        raise ValueError('Missing or unexpected binding')
    actual = [[row[col] for col in case['columns']] for row in rows]
    def canonical(row):
        return json.dumps(row, sort_keys=True, separators=(',', ':'))
    if Counter(map(canonical, actual)) != Counter(map(canonical, expected)):
        raise ValueError('Complete typed multiset mismatch')
    if case['ordered'] and list(map(canonical, actual)) != list(map(canonical, expected)):
        raise ValueError('ORDER BY/LIMIT/OFFSET sequence mismatch')
    return dict(case=name, complete_typed_bag=True, projection_order=True,
                ordered_sequence=case['ordered'], rows=len(actual))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    gen = sub.add_parser('generate')
    gen.add_argument('--output', required=True)
    check = sub.add_parser('validate')
    check.add_argument('--manifest', required=True)
    check.add_argument('--case', required=True)
    check.add_argument('--result', required=True)
    args = parser.parse_args()
    if args.command == 'generate':
        result = generate(args.output)
        print(json.dumps({k:v for k,v in result.items() if k != 'cases'}))
    else:
        print(json.dumps(validate(args.manifest, args.case, args.result)))

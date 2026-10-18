#!/usr/bin/env python3
"""Deterministic 25k-triple fixture exposing skew and cross-predicate correlation."""

import argparse
from pathlib import Path

from validate_scale_bgp import iri


def make_triples():
    triples = [(iri(f'Entity{i}'), iri('common'), iri(f'Value{i}')) for i in range(10000)]
    triples += [(iri(f'Entity{i}'), iri('member'), iri('GroupA')) for i in range(5000)]
    triples += [(iri(f'Entity{i}'), iri('label'), f'"Label {i}"') for i in range(5000, 10000)]
    triples += [(iri(f'Entity{i}'), iri('noise'), iri('Noise')) for i in range(4990)]
    triples += [(iri(f'Entity{i}'), iri('rare'), iri('Rare')) for i in range(10)]
    return triples


def make_cases():
    def case(name, variables, patterns):
        return {'name': name, 'variables': variables, 'patterns': patterns, 'distinct': False,
                'query': 'SELECT ' + ' '.join(variables) + ' WHERE { ' +
                         ' . '.join(' '.join(pattern) for pattern in patterns) + ' }'}
    return [
        case('skew_selective_join', ['?s', '?value'],
             [('?s', iri('common'), '?value'), ('?s', iri('rare'), iri('Rare'))]),
        case('positive_correlation', ['?s', '?value'],
             [('?s', iri('member'), iri('GroupA')), ('?s', iri('noise'), '?value')]),
        case('negative_correlation', ['?s', '?label'],
             [('?s', iri('member'), iri('GroupA')), ('?s', iri('label'), '?label')]),
        case('correlated_constants', ['?s'], [('?s', iri('common'), iri('Value42'))]),
    ]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    triples = make_triples()
    assert len(triples) == len(set(triples)) == 25000
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('x') as output:
        for triple in triples:
            output.write(' '.join(triple) + ' .\n')


if __name__ == '__main__':
    main()

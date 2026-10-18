#!/usr/bin/env python3
"""Validate the restricted IRI/plain-literal BGP suite through real CLI processes."""

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import signal
import subprocess
import threading
import time

NT = re.compile(r'^\s*(<[^<>\s]+>)\s+(<[^<>\s]+>)\s+(<[^<>\s]+>|"[^"\\\r\n]*")\s+\.\s*$')
BASE = 'http://example.org/'
TYPE = '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>'
LABEL = '<http://www.w3.org/2000/01/rdf-schema#label>'


def iri(value):
    return '<' + BASE + value + '>'


def load_graph(path):
    triples = []
    for number, line in enumerate(path.read_text().splitlines(), 1):
        if not line.strip() or line.lstrip().startswith('#'):
            continue
        match = NT.fullmatch(line)
        if match is None:
            raise ValueError(f'Unsupported term syntax at input line {number}')
        triples.append(match.groups())
    graph = sorted(set(triples))
    return graph, {
        'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
        'input_triples': len(triples), 'unique_triples': len(graph),
        'duplicate_triples': len(triples) - len(graph),
        'subjects': len({s for s, _, _ in graph}),
        'iri_objects': sum(o.startswith('<') for _, _, o in graph),
        'plain_literal_objects': sum(o.startswith('"') for _, _, o in graph),
    }


def make_cases():
    def case(name, variables, patterns, distinct=False, query=None):
        body = ' . '.join(' '.join(pattern) for pattern in patterns)
        return {'name': name, 'variables': variables, 'patterns': patterns,
                'distinct': distinct, 'query': query or
                'SELECT ' + ('DISTINCT ' if distinct else '') +
                ' '.join(variables) + ' WHERE { ' + body + ' }'}
    chain = [('?s', '?p', '?mid'), ('?mid', LABEL, '?label')]
    return [
        case('all_triples', ['?s', '?p', '?o'], [('?s', '?p', '?o')]),
        case('fixed_subject', ['?p', '?o'], [(iri('Company1'), '?p', '?o')]),
        case('legacy_reviews', ['?s', '?o'], [('?s', iri('reviews'), '?o')]),
        case('legacy_rdf_type', ['?s', '?t'], [('?s', TYPE, '?t')],
             query='SELECT ?s ?t WHERE { ?s rdf:type ?t }'),
        case('all_labels', ['?s', '?label'], [('?s', LABEL, '?label')]),
        case('fixed_object', ['?s', '?p'], [('?s', '?p', iri('Company'))]),
        case('fixed_subject_predicate', ['?label'], [(iri('Company1'), LABEL, '?label')]),
        case('fixed_subject_object', ['?p'], [(iri('Company1'), '?p', iri('Company'))]),
        case('literal_lookup', ['?s'], [('?s', LABEL, '"Company #1"')]),
        case('missing_object', ['?s'], [('?s', TYPE, iri('MissingClassForBaseline'))]),
        case('legacy_chain', ['?s', '?o', '?z'],
             [('?s', iri('collaboratesWith'), '?o'), ('?o', iri('aligns'), '?z')]),
        case('chain_labels', ['?s', '?friend', '?label'],
             [('?s', iri('knows'), '?friend'), ('?friend', LABEL, '?label')]),
        case('star_type_labels', ['?s', '?label'],
             [('?s', TYPE, iri('Company')), ('?s', LABEL, '?label')]),
        case('variable_predicate_chain', ['?s', '?p', '?label'], chain),
        case('bag_projection', ['?s'], chain),
        case('distinct_projection', ['?s'], chain, distinct=True),
        case('three_pattern_chain', ['?s', '?friend', '?t', '?label'],
             [('?s', iri('knows'), '?friend'), ('?friend', TYPE, '?t'), ('?friend', LABEL, '?label')]),
        case('repeated_variable', ['?s', '?p'], [('?s', '?p', '?s')]),
        case('legacy_distinct', ['?s', '?o'], [('?s', iri('developedBy'), '?o')], distinct=True),
        case('distinct_classes', ['?t'], [('?s', TYPE, '?t')], distinct=True),
    ]


class ReferenceGraph:
    def __init__(self, triples):
        self.triples = triples
        self.indices = [defaultdict(list) for _ in range(3)]
        for triple in triples:
            for position in range(3):
                self.indices[position][triple[position]].append(triple)

    def evaluate(self, spec):
        bindings = [{}]
        for pattern in spec['patterns']:
            joined = []
            for binding in bindings:
                fixed = [(index, binding.get(term) if term.startswith('?') else term)
                         for index, term in enumerate(pattern)]
                candidates = [self.indices[index].get(term, [])
                              for index, term in fixed if term is not None]
                candidates = min(candidates, key=len) if candidates else self.triples
                for triple in candidates:
                    result = dict(binding)
                    for term, value in zip(pattern, triple):
                        if term.startswith('?'):
                            if result.setdefault(term, value) != value:
                                break
                        elif term != value:
                            break
                    else:
                        joined.append(result)
            bindings = joined
            if len(bindings) > 1000000:
                raise RuntimeError('Reference intermediate exceeded baseline size budget')
        rows = Counter(tuple(binding[var][1:-1] for var in spec['variables']) for binding in bindings)
        return Counter({row: 1 for row in rows}) if spec['distinct'] else rows


def parse_cli(path, variables):
    names = [var[1:] for var in variables]
    rows, row = Counter(), []
    for line in path.read_text().splitlines():
        if line == '---' or not line:
            if row:
                raise ValueError('CLI row separator before all projected variables')
            continue
        name, value = line.split('\t', 1)
        if name != names[len(row)]:
            raise ValueError(f'Unexpected projected variable: {name}')
        row.append(value)
        if len(row) == len(names):
            rows[tuple(row)] += 1
            row = []
    if row:
        raise ValueError('Incomplete CLI row')
    return rows


def process_rss_kib(pid):
    try:
        match = re.search(r'^VmRSS:\s+(\d+) kB', Path(f'/proc/{pid}/status').read_text(), re.M)
        return int(match.group(1)) if match else 0
    except FileNotFoundError:
        return 0


def system_memory():
    text = Path('/proc/meminfo').read_text()
    return {key: int(re.search(r'^' + key + r':\s+(\d+)', text, re.M).group(1))
            for key in ('MemTotal', 'MemAvailable', 'SwapTotal', 'SwapFree')}


def self_check_reference():
    graph = ReferenceGraph([('<a>', '<p>', '<b>'), ('<a>', '<p>', '<c>'),
                            ('<b>', '<q>', '"end"'), ('<c>', '<q>', '"end"')])
    spec = {'variables': ['?s', '?end'], 'patterns': [('?s', '<p>', '?mid'), ('?mid', '<q>', '?end')],
            'distinct': False}
    assert graph.evaluate(spec) == Counter({('a', 'end'): 2})
    assert graph.evaluate(dict(spec, distinct=True)) == Counter({('a', 'end'): 1})
    spec['patterns'] = [('?s', '<p>', '?s')]
    spec['variables'] = ['?s']
    assert not graph.evaluate(spec)


def main():
    parser = argparse.ArgumentParser()
    for name in ('cli', 'masters', 'namespace', 'dataset', 'output', 'parent-pid'):
        parser.add_argument('--' + name, required=True)
    parser.add_argument('--transactions', choices=('true', 'false'), required=True)
    parser.add_argument('--statistics', choices=('off', 'on', 'compare'),
                        default=os.environ.get('MDB_SCALE_STATISTICS', 'off'))
    parser.add_argument('--workload', choices=('ds10000', 'skew-correlated'),
                        default=os.environ.get('MDB_SCALE_WORKLOAD', 'ds10000'))
    parser.add_argument('--repetitions', type=int,
                        default=int(os.environ.get('MDB_SCALE_REPETITIONS', '1')))
    args = parser.parse_args()
    if not 1 <= args.repetitions <= 10:
        parser.error('repetitions must be between 1 and 10')
    directory = Path(args.output)
    directory.mkdir(parents=True, exist_ok=True)
    if (directory / 'results.json').exists():
        raise RuntimeError('Use a new baseline output directory to preserve old results')
    self_check_reference()
    triples, audit = load_graph(Path(args.dataset))
    if audit['unique_triples'] != 25000 or audit['subjects'] != 10000:
        raise RuntimeError('Dataset content does not match the audited ds10000 input')
    reference = ReferenceGraph(triples)
    if args.workload == 'skew-correlated':
        from statistics_workload import make_cases as make_statistics_cases, make_triples
        if triples != sorted(make_triples()):
            raise RuntimeError('Statistics workload does not match the deterministic fixture')
        cases = make_statistics_cases()
    else:
        cases = make_cases()
    selected = os.environ.get('MDB_SCALE_CASES')
    if selected:
        names = set(selected.split(','))
        cases = [case for case in cases if case['name'] in names]
        if {case['name'] for case in cases} != names:
            raise ValueError('Unknown selected baseline case')
    (directory / 'queries.json').write_text(json.dumps(cases, indent=2))
    state = {'timestamp_utc': datetime.now(timezone.utc).isoformat(), 'dataset': audit,
             'transactions': args.transactions == 'true', 'queries': [],
             'workload': args.workload, 'statistics': args.statistics,
             'repetitions': args.repetitions,
             'kernel': platform.release(), 'system_memory_before_kib': system_memory(),
             'measurement': 'GNU time: fresh CLI process wall time, CPU and MaxRSS; '
                            'includes connection/dictionary/loading/execution/output, not engine-only latency. '
                            'Parent RSS samples include in-process master/tablet server/test fixture; '
                            'OS page cache and server storage caches are not flushed. '
                            'Statistics-on includes a new full SPO collection for every query; '
                            'compare alternates off/on then on/off across repetitions.'}
    report_path = directory / 'results.json'

    def checkpoint():
        report_path.write_text(json.dumps(state, indent=2))

    base = [args.cli, '--mdb_masters=' + args.masters, '--mdb_keyspace=' + args.namespace,
            '--mdb_use_transactions=' + args.transactions]

    def run_step(name, arguments):
        state['active_step'] = name
        checkpoint()
        output_path, error_path = directory / (name + '.stdout'), directory / (name + '.stderr')
        timing = directory / (name + '.time.json')
        command = ['/usr/bin/time', '-f',
                   '{"wall_seconds":%e,"user_seconds":%U,"system_seconds":%S,"cli_peak_rss_kib":%M}',
                   '-o', str(timing)] + base + arguments
        peak = [process_rss_kib(args.parent_pid)]
        stopped = threading.Event()

        def sample():
            while not stopped.wait(0.1):
                peak[0] = max(peak[0], process_rss_kib(args.parent_pid))

        sampler = threading.Thread(target=sample, daemon=True)
        sampler.start()
        try:
            with output_path.open('w') as output, error_path.open('w') as error:
                process = subprocess.Popen(command, stdout=output, stderr=error, start_new_session=True)
                try:
                    returncode = process.wait(timeout=240)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
                    raise RuntimeError(f'{name} exceeded 240 seconds')
        finally:
            stopped.set()
            sampler.join()
        if returncode != 0:
            raise RuntimeError(f'{name}: CLI exit {returncode}; inspect {error_path}')
        stats = json.loads(timing.read_text())
        stats['cluster_test_process_peak_rss_kib_sampled'] = peak[0]
        stats['stdout'] = str(output_path)
        stats['stderr'] = str(error_path)
        return output_path, error_path.read_text(), stats

    try:
        _, log, stats = run_step('import', ['import', args.dataset])
        imported = re.search(r'Import: parsed=(\d+) written=(\d+) errors=(\d+)', log)
        if not imported or tuple(map(int, imported.groups())) != (audit['input_triples'], audit['input_triples'], 0):
            raise RuntimeError('Import count mismatch or parse errors')
        state['import'] = stats
        checkpoint()
        for spec in cases:
            expected = reference.evaluate(spec)
            expected_path = directory / (spec['name'] + '.expected.json')
            expected_path.write_text(json.dumps(sorted((list(row), count) for row, count in expected.items())))
            for repetition in range(args.repetitions):
                modes = ['off', 'on'] if args.statistics == 'compare' else [args.statistics]
                if repetition % 2:
                    modes.reverse()
                for mode in modes:
                    name = spec['name']
                    if args.statistics != 'off' or args.repetitions > 1:
                        name += f'.{mode}.r{repetition + 1}'
                    flag = '--mdb_collect_statistics=' + ('true' if mode == 'on' else 'false')
                    output, error, stats = run_step(name, [flag, 'query', spec['query']])
                    actual = parse_cli(output, spec['variables'])
                    trace = re.search(r'MDB tablet-aware query: fragments=(\d+) rows=(\d+)', error)
                    collected = re.search(r'MDB statistics collected: rows=(\d+)', error)
                    collection_ok = (collected is not None and
                                     int(collected.group(1)) == audit['unique_triples']) if mode == 'on' else collected is None
                    passed = actual == expected and trace is not None and collection_ok
                    if trace:
                        passed = passed and int(trace.group(2)) == actual.total()
                    result = dict(stats, name=spec['name'], query=spec['query'],
                                  statistics=mode, repetition=repetition + 1,
                                  collected_rows=int(collected.group(1)) if collected else None,
                                  expected_rows=expected.total(), actual_rows=actual.total(),
                                  expected_unique_rows=len(expected), actual_unique_rows=len(actual),
                                  fragments=int(trace.group(1)) if trace else None, passed=passed,
                                  missing_sample=list((expected - actual).items())[:5],
                                  extra_sample=list((actual - expected).items())[:5])
                    state['queries'].append(result)
                    checkpoint()
                    print(f"{name}: {'PASS' if passed else 'FAIL'} rows={actual.total()}/{expected.total()} "
                          f"wall={stats['wall_seconds']}s rss={stats['cli_peak_rss_kib']}KiB", flush=True)
        state['passed'] = all(result['passed'] for result in state['queries'])
        state['active_step'] = 'complete'
        state['system_memory_after_kib'] = system_memory()
        checkpoint()
        return 0 if state['passed'] else 1
    except Exception as error:
        state['passed'] = False
        state['error'] = str(error)
        checkpoint()
        raise


if __name__ == '__main__':
    raise SystemExit(main())

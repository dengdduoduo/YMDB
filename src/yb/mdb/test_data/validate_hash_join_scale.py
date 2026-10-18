#!/usr/bin/env python3
"""Measure opt-in hash selection and batch amortization through real CLI processes."""

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import platform
import re
import signal
import subprocess
import threading

from validate_scale_bgp import (
    iri, load_graph, parse_cli, process_rss_kib, ReferenceGraph, system_memory,
)


def make_triples():
    triples = [(iri(f'Entity{i}'), iri('left_distinct'), iri(f'Entity{i}'))
               for i in range(5000)]
    triples += [(iri(f'Entity{5000 + i}'), iri('left_hot'), iri(f'Entity{i % 8}'))
                for i in range(5000)]
    triples += [(iri(f'Entity{i}'), iri('right'), f'"Value {i}"') for i in range(5000)]
    triples += [(iri(f'Entity{i}'), iri('noise'), iri(f'Noise{i}')) for i in range(10000)]
    return triples


def make_cases():
    cases = []
    for shape in ('distinct', 'hot'):
        patterns = [('?s', iri('left_' + shape), '?m'), ('?m', iri('right'), '?o')]
        for projection, variables in (('full', ['?s', '?o']), ('bag', ['?o'])):
            cases.append({'name': shape + '_' + projection, 'variables': variables,
                          'patterns': patterns, 'distinct': False,
                          'query': 'SELECT ' + ' '.join(variables) + ' WHERE { ' +
                          ' . '.join(' '.join(row) for row in patterns) + ' }'})
    return cases


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--generate', type=Path)
    for name in ('cli', 'masters', 'namespace', 'dataset', 'output', 'parent-pid'):
        parser.add_argument('--' + name)
    parser.add_argument('--transactions', choices=('true', 'false'))
    parser.add_argument('--repetitions', type=int, default=2)
    args = parser.parse_args()
    if args.generate:
        args.generate.parent.mkdir(parents=True, exist_ok=True)
        with args.generate.open('x') as output:
            for row in make_triples():
                output.write(' '.join(row) + ' .\n')
        return 0
    for name in ('cli', 'masters', 'namespace', 'dataset', 'output', 'parent_pid',
                 'transactions'):
        if getattr(args, name) is None:
            parser.error('missing --' + name.replace('_', '-'))
    if args.transactions != 'false':
        parser.error('hash selection requires non-transactional reads')
    if not 1 <= args.repetitions <= 10:
        parser.error('repetitions must be between 1 and 10')
    directory = Path(args.output)
    directory.mkdir(parents=True, exist_ok=True)
    result_path = directory / 'results.json'
    if result_path.exists():
        raise RuntimeError('Use a new output directory to preserve old results')
    triples, audit = load_graph(Path(args.dataset))
    if triples != sorted(make_triples()) or audit['unique_triples'] != 25000:
        raise RuntimeError('Dataset differs from the deterministic hash-scale fixture')
    reference = ReferenceGraph(triples)
    cases = make_cases()
    expected = {case['name']: reference.evaluate(case) for case in cases}
    for case in cases:
        path = directory / (case['name'] + '.rq')
        path.write_text(case['query'] + '\n')
        case['file'] = str(path)
    base = [args.cli, '--mdb_masters=' + args.masters, '--mdb_keyspace=' + args.namespace,
            '--mdb_use_transactions=false']
    state = {'timestamp_utc': datetime.now(timezone.utc).isoformat(), 'dataset': audit,
             'kernel': platform.release(), 'cases': cases, 'runs': [],
             'repetitions': args.repetitions, 'system_memory_before_kib': system_memory(),
             'measurement': 'GNU time CLI process wall/CPU/MaxRSS, including initialization, '
             'dictionary, optional statistics, execution and output. Four fresh processes are '
             'compared with one four-query batch. Query caches clear per query; statistics '
             'collect once per process. Parent RSS is sampled every 100ms and includes the '
             'in-process cluster. Server/OS caches are not flushed. Mode and query order '
             'reverse on alternate repetitions. No concurrent writers; timings are diagnostic '
             'with no pass/fail speed threshold. End-to-end hash mode also changes routing; '
             'it does not isolate join algorithm cost or hash-only memory.'}

    def checkpoint():
        result_path.write_text(json.dumps(state, indent=2))

    def run_step(name, arguments):
        state['active_step'] = name
        checkpoint()
        stdout = directory / (name + '.stdout')
        stderr = directory / (name + '.stderr')
        timing = directory / (name + '.time.json')
        command = ['/usr/bin/time', '-f',
                   '{"wall_seconds":%e,"user_seconds":%U,"system_seconds":%S,'
                   '"cli_peak_rss_kib":%M}', '-o', str(timing)] + base + arguments
        peak = [process_rss_kib(args.parent_pid)]
        stopped = threading.Event()

        def sample():
            while not stopped.wait(0.1):
                peak[0] = max(peak[0], process_rss_kib(args.parent_pid))

        sampler = threading.Thread(target=sample, daemon=True)
        sampler.start()
        try:
            with stdout.open('w') as output, stderr.open('w') as error:
                process = subprocess.Popen(command, stdout=output, stderr=error,
                                           start_new_session=True)
                try:
                    code = process.wait(timeout=240)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
                    raise RuntimeError(name + ': exceeded 240 seconds')
        finally:
            stopped.set()
            sampler.join()
        if code:
            raise RuntimeError(f'{name}: exit {code}; inspect {stderr}')
        measured = json.loads(timing.read_text())
        measured.update(stdout=str(stdout), stderr=str(stderr),
                        cluster_test_peak_rss_kib_sampled=peak[0])
        return stdout, stderr.read_text(), measured

    def check(case, output):
        actual = parse_cli(output, case['variables'])
        if actual != expected[case['name']]:
            raise RuntimeError(case['name'] + ': wrong rows or duplicate counts')
        return {'name': case['name'], 'rows': actual.total(), 'unique_rows': len(actual),
                'passed': True}

    try:
        _, log, state['import'] = run_step('import', ['import', args.dataset])
        imported = re.search(r'Import: parsed=(\d+) written=(\d+) errors=(\d+)', log)
        if not imported or tuple(map(int, imported.groups())) != (25000, 25000, 0):
            raise RuntimeError('Import count mismatch')
        for repetition in range(args.repetitions):
            modes = [(kind, batch) for kind in ('baseline', 'statistics', 'hash')
                     for batch in (False, True)]
            selected = cases
            if repetition % 2:
                modes.reverse()
                selected = list(reversed(cases))
            for kind, batch in modes:
                prefix = f'{kind}.{"batch" if batch else "single"}.r{repetition + 1}'
                flags = ['--mdb_collect_statistics=' + str(kind != 'baseline').lower(),
                         '--mdb_enable_hash_join=' + str(kind == 'hash').lower()]
                answers, measurements, logs = [], [], []
                if batch:
                    output, log, measured = run_step(
                        prefix, flags + ['query-batch'] + [case['file'] for case in selected])
                    raw, offset = output.read_text(), 0
                    for index, case in enumerate(selected, 1):
                        begin, end = f'=== query {index} ===\n', f'=== end query {index} ===\n'
                        if not raw.startswith(begin, offset):
                            raise RuntimeError(prefix + ': wrong batch framing')
                        finish = raw.index(end, offset + len(begin))
                        part = directory / (prefix + '.' + case['name'] + '.rows')
                        part.write_text(raw[offset + len(begin):finish])
                        answers.append(check(case, part))
                        offset = finish + len(end)
                    if offset != len(raw):
                        raise RuntimeError(prefix + ': trailing batch data')
                    measurements.append(measured)
                    logs.append(log)
                else:
                    for case in selected:
                        output, log, measured = run_step(prefix + '.' + case['name'],
                                                        flags + ['query', case['query']])
                        answers.append(check(case, output))
                        measurements.append(measured)
                        logs.append(log)
                collections, traces = 0, []
                for log in logs:
                    collected = re.findall(r'MDB statistics collected: rows=(\d+)', log)
                    if any(int(count) != 25000 for count in collected):
                        raise RuntimeError(prefix + ': wrong statistics row count')
                    collections += len(collected)
                    offset = 0
                    for trace in re.finditer(
                            r'MDB tablet-aware query: fragments=(\d+) rows=(\d+)', log):
                        hashes = log[offset:trace.start()].count('MDB hash join selected:')
                        traces.append((int(trace[1]), int(trace[2]), hashes))
                        offset = trace.end()
                if collections != (len(measurements) if kind != 'baseline' else 0):
                    raise RuntimeError(prefix + ': statistics were not collected once per process')
                expected_traces = [(1 if kind == 'hash' and case['name'].startswith('distinct')
                                    else 2, answer['rows'],
                                    int(kind == 'hash' and case['name'].startswith('distinct')))
                                   for case, answer in zip(selected, answers)]
                if traces != expected_traces:
                    raise RuntimeError(f'{prefix}: wrong routing/hash selection: {traces}')
                for answer, trace in zip(answers, traces):
                    answer.update(fragments=trace[0], hash_selected=bool(trace[2]))
                result = {'mode': kind, 'batch': batch, 'repetition': repetition + 1,
                          'wall_seconds': sum(row['wall_seconds'] for row in measurements),
                          'max_cli_peak_rss_kib': max(row['cli_peak_rss_kib']
                                                    for row in measurements),
                          'statistics_collections': collections, 'queries': answers,
                          'measurements': measurements, 'passed': True}
                state['runs'].append(result)
                checkpoint()
                print(f'{prefix}: PASS wall={result["wall_seconds"]:.2f}s '
                      f'collections={collections}', flush=True)
        state.update(passed=True, active_step='complete',
                     system_memory_after_kib=system_memory())
        checkpoint()
        return 0
    except Exception as error:
        state.update(passed=False, error=str(error))
        checkpoint()
        raise


if __name__ == '__main__':
    raise SystemExit(main())

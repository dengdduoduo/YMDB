#!/usr/bin/env python3
"""Measure fresh CLI processes separately from the server and in-process counters."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import statistics
import subprocess
import time

from validate_scale_bgp import ReferenceGraph, load_graph, parse_cli, system_memory


def make_triples(shape):
    base = 'https://example.org/leapfrog-scale/'

    def node(role, index):
        return '<' + base + role + '/' + str(index) + '>'

    p, q, r = ['<' + base + name + '>' for name in ('p', 'q', 'r')]
    triples = []
    for x in range(8192):
        triples.append((node('x', x), p, node('y', x if shape == 'sparse' else x % 8)))
        triples.append((node('x', x), r, node('z', x if shape == 'sparse' else x % 32)))
        if shape == 'sparse':
            triples.append((node('y', x), q, node('z', x)))
        else:
            triples.append((node('x', x), r, node('z', (x + 1) % 32)))
    if shape == 'skew':
        for y in range(8):
            for z in range(32):
                triples.append((node('y', y), q, node('z', z)))
    for index in range(25000 - len(triples)):
        triples.append((node('noise-s', index), '<' + base + 'noise>', node('noise-o', index)))
    assert len(triples) == len(set(triples)) == 25000
    spec = dict(variables=['?x', '?y', '?z'], distinct=False,
                patterns=[('?x', p, '?y'), ('?y', q, '?z'), ('?x', r, '?z')])
    body = ' . '.join(' '.join(pattern) for pattern in spec['patterns'])
    spec['query'] = 'SELECT ?x ?y ?z WHERE { ' + body + ' }'
    return sorted(triples), spec


def bag_digest(bag):
    serial = json.dumps(sorted(bag.items()), separators=(',', ':')).encode()
    return hashlib.sha256(serial).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('cli', 'masters', 'namespace', 'output'):
        parser.add_argument('--' + name, required=True)
    parser.add_argument('--shape', choices=('sparse', 'skew'), required=True)
    args = parser.parse_args()
    directory = Path(args.output)
    directory.mkdir(parents=True, exist_ok=True)
    result_path = directory / 'results.json'
    if result_path.exists():
        raise RuntimeError('Use a new output directory to preserve previous measurements')
    triples, spec = make_triples(args.shape)
    dataset = directory / 'triangle-25000.nt'
    dataset.write_text(''.join(' '.join(t) + ' .\n' for t in triples))
    graph, audit = load_graph(dataset)
    assert len(graph) == audit['unique_triples'] == 25000
    expected = ReferenceGraph(graph).evaluate(spec)
    expected_count = 8192 if args.shape == 'sparse' else 16384
    assert sum(expected.values()) == len(expected) == expected_count
    digest = bag_digest(expected)
    (directory / 'triangle.rq').write_text(spec['query'] + '\n')
    state = dict(shape=args.shape, dataset=audit, expected_rows=expected_count,
                 expected_bag_sha256=digest, records=[], system_memory_before_kib=system_memory(),
                 measurement='GNU time MaxRSS of each fresh CLI only, excluding server and driver; '
                 'wall includes startup, connection, dictionary restore, query and stdout; '
                 'import excluded from query summaries; no OS/server cache flushing; '
                 'profiling clocks off in CLI; in-process phase/counter experiment is separate')
    base = [args.cli, '--mdb_masters=' + args.masters, '--mdb_keyspace=' + args.namespace,
            '--mdb_use_transactions=false', '--mdb_collect_statistics=false',
            '--mdb_use_partition_load=true', '--mdb_enable_hash_join=false',
            '--mdb_enable_leapfrog_join=true', '--mdb_profile_leapfrog=false',
            '--mdb_leapfrog_read_page_rows=1024']

    def checkpoint():
        result_path.write_text(json.dumps(state, indent=2))

    def run(name, arguments):
        state['active_step'] = name
        checkpoint()
        stdout, stderr = directory / (name + '.stdout'), directory / (name + '.stderr')
        timing = directory / (name + '.time.json')
        command = ['/usr/bin/time', '-f',
                   '{"wall_seconds":%e,"user_seconds":%U,"system_seconds":%S,'
                   '"cli_peak_rss_kib":%M}',
                   '-o', str(timing), *base, *arguments]
        start = time.monotonic()
        with stdout.open('w') as output, stderr.open('w') as error:
            process = subprocess.Popen(command, stdout=output, stderr=error, start_new_session=True)
            try:
                code = process.wait(timeout=240)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
                state['failure'] = name + ': child process timed out'
                checkpoint()
                raise
        if code:
            state['failure'] = dict(step=name, exit_code=code, stderr=str(stderr))
            checkpoint()
            raise RuntimeError(f'{name} failed with {code}; inspect {stderr}')
        metrics = json.loads(timing.read_text())
        assert metrics['cli_peak_rss_kib'] > 0
        metrics.update(elapsed_driver_seconds=time.monotonic() - start,
                       stdout=str(stdout), stderr=str(stderr))
        return metrics

    state['import'] = run('import', ['import', str(dataset)])
    checkpoint()
    modes = [('private', 0), ('reuse', 8 * 1024 * 1024), ('limited', 192 * 1024)]
    for trial in range(4):
        for position in range(3):
            mode, budget = modes[(trial + position) % 3]
            name = f'query-{trial}-{mode}'
            metrics = run(name, ['--mdb_leapfrog_reuse_bytes=' + str(budget),
                                 'query', spec['query']])
            actual = parse_cli(Path(metrics['stdout']), spec['variables'])
            if actual != expected:
                state['failure'] = name + ': full result bag differs'
                checkpoint()
                raise AssertionError(state['failure'])
            error = Path(metrics['stderr']).read_text()
            assert 'MDB tablet-aware query: fragments=4' in error
            assert error.count('MDB Leapfrog triangle: relations=3') == 4
            record = dict(trial=trial, mode=mode, budget_bytes=budget, rows=sum(actual.values()),
                          bag_sha256=bag_digest(actual), **metrics)
            state['records'].append(record)
            checkpoint()
            print(json.dumps(dict(shape=args.shape, **record)), flush=True)
    state.pop('active_step', None)
    state['summary'] = {}
    for mode, _ in modes:
        group = [r for r in state['records'] if r['mode'] == mode]
        state['summary'][mode] = {
            'samples': len(group),
            'median_wall_seconds': statistics.median(r['wall_seconds'] for r in group),
            'range_wall_seconds': [min(r['wall_seconds'] for r in group),
                                   max(r['wall_seconds'] for r in group)],
            'median_peak_rss_kib': statistics.median(r['cli_peak_rss_kib'] for r in group),
            'range_peak_rss_kib': [min(r['cli_peak_rss_kib'] for r in group),
                                  max(r['cli_peak_rss_kib'] for r in group)]}
    state['full_bags_passed'] = len(state['records'])
    state['system_memory_after_kib'] = system_memory()
    checkpoint()
    print(json.dumps(state['summary'], indent=2), flush=True)


if __name__ == '__main__':
    main()

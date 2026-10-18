#!/usr/bin/env python3
"""Compare fresh single-query processes with read-only batches on the same graph."""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import signal
import subprocess

from statistics_workload import make_cases, make_triples
from validate_scale_bgp import load_graph, parse_cli, ReferenceGraph, system_memory


def main():
    parser = argparse.ArgumentParser()
    for name in ('cli', 'masters', 'namespace', 'dataset', 'output', 'parent-pid'):
        parser.add_argument('--' + name, required=True)
    parser.add_argument('--transactions', choices=('true', 'false'), required=True)
    args = parser.parse_args()
    directory = Path(args.output)
    directory.mkdir(parents=True, exist_ok=True)
    result_path = directory / 'results.json'
    if result_path.exists():
        raise RuntimeError('Use a new output directory to preserve previous results')
    triples, audit = load_graph(Path(args.dataset))
    assert triples == sorted(make_triples())
    assert audit['unique_triples'] == 25000 and audit['subjects'] == 10000
    reference = ReferenceGraph(triples)
    cases = make_cases()
    expected = {case['name']: reference.evaluate(case) for case in cases}
    for case in cases:
        path = directory / (case['name'] + '.rq')
        path.write_text(case['query'] + '\n')
        case['file'] = str(path)
    base = [args.cli, '--mdb_masters=' + args.masters, '--mdb_keyspace=' + args.namespace,
            '--mdb_use_transactions=' + args.transactions]
    state = {'timestamp_utc': datetime.now(timezone.utc).isoformat(),
             'dataset': audit, 'transactions': args.transactions == 'true',
             'cases': cases, 'runs': [], 'system_memory_before_kib': system_memory(),
             'measurement': 'GNU time fresh-process wall/CPU/MaxRSS. Single groups sum four '
                            'process wall times; batches use one process for four queries. '
                            'Includes connection, dictionary, collection, parsing, execution and output. '
                            'Result caches clear before every query; connection/statistics are reused '
                            'only in batches. Server/OS caches are not flushed. No concurrent writers. '
                            'Two reversed-order repetitions; not a statistically powered benchmark.'}

    def checkpoint():
        result_path.write_text(json.dumps(state, indent=2))

    def run_step(name, arguments):
        state['active_step'] = name
        checkpoint()
        stdout = directory / (name + '.stdout')
        stderr = directory / (name + '.stderr')
        timing = directory / (name + '.time.json')
        command = ['/usr/bin/time', '-f',
                   '{"wall_seconds":%e,"user_seconds":%U,"system_seconds":%S,"cli_peak_rss_kib":%M}',
                   '-o', str(timing)] + base + arguments
        with stdout.open('w') as output, stderr.open('w') as error:
            process = subprocess.Popen(command, stdout=output, stderr=error, start_new_session=True)
            try:
                code = process.wait(timeout=240)
            except subprocess.TimeoutExpired:
                import os
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
                raise RuntimeError(f'{name}: timeout')
        if code:
            raise RuntimeError(f'{name}: exit {code}; inspect {stderr}')
        stats = json.loads(timing.read_text())
        stats.update(stdout=str(stdout), stderr=str(stderr))
        return stdout, stderr.read_text(), stats

    def check(case, output):
        actual = parse_cli(output, case['variables'])
        answer = expected[case['name']]
        if actual != answer:
            raise RuntimeError(f"{case['name']}: incorrect rows or duplicate counts")
        return {'name': case['name'], 'actual_rows': actual.total(),
                'actual_unique_rows': len(actual), 'passed': True}

    try:
        _, log, state['import'] = run_step('import', ['import', args.dataset])
        imported = re.search(r'Import: parsed=(\d+) written=(\d+) errors=(\d+)', log)
        assert imported and tuple(map(int, imported.groups())) == (25000, 25000, 0)
        for repetition in range(2):
            modes = ['single_off', 'batch_off', 'single_on', 'batch_on']
            selected = cases
            if repetition:
                modes.reverse()
                selected = list(reversed(cases))
            for mode in modes:
                enabled = mode.endswith('_on')
                prefix = f'{mode}.r{repetition + 1}'
                flag = '--mdb_collect_statistics=' + ('true' if enabled else 'false')
                answers, measurements, logs = [], [], []
                if mode.startswith('batch'):
                    output, log, measured = run_step(prefix, [flag, 'query-batch'] +
                                                    [case['file'] for case in selected])
                    raw = output.read_text()
                    offset = 0
                    for i, case in enumerate(selected, 1):
                        begin, end = f'=== query {i} ===\n', f'=== end query {i} ===\n'
                        assert raw.startswith(begin, offset)
                        finish = raw.index(end, offset + len(begin))
                        part = directory / (prefix + '.' + case['name'] + '.rows')
                        part.write_text(raw[offset + len(begin):finish])
                        answers.append(check(case, part))
                        offset = finish + len(end)
                    assert offset == len(raw)
                    measurements.append(measured)
                    logs.append(log)
                else:
                    for case in selected:
                        output, log, measured = run_step(prefix + '.' + case['name'],
                                                        [flag, 'query', case['query']])
                        answers.append(check(case, output))
                        measurements.append(measured)
                        logs.append(log)
                collection_count = 0
                for log in logs:
                    collected = re.findall(r'MDB statistics collected: rows=(\d+) predicate_object_pairs=(\d+)', log)
                    assert all(tuple(map(int, row)) == (25000, 15003) for row in collected)
                    collection_count += len(collected)
                assert collection_count == (len(measurements) if enabled else 0)
                traces = [int(rows) for log in logs for rows in re.findall(
                    r'MDB tablet-aware query: fragments=\d+ rows=(\d+)', log)]
                assert traces == [answer['actual_rows'] for answer in answers]
                result = {'mode': mode, 'repetition': repetition + 1,
                          'query_order': [case['name'] for case in selected],
                          'wall_seconds': sum(row['wall_seconds'] for row in measurements),
                          'user_seconds': sum(row['user_seconds'] for row in measurements),
                          'system_seconds': sum(row['system_seconds'] for row in measurements),
                          'max_cli_peak_rss_kib': max(row['cli_peak_rss_kib'] for row in measurements),
                          'statistics_collections': collection_count,
                          'measurements': measurements, 'queries': answers, 'passed': True}
                state['runs'].append(result)
                checkpoint()
                print(f'{prefix}: PASS four queries wall={result["wall_seconds"]:.2f}s '
                      f'collections={collection_count}', flush=True)
        state['active_step'] = 'complete'
        state['passed'] = True
        state['system_memory_after_kib'] = system_memory()
        checkpoint()
        return 0
    except Exception as error:
        state['passed'] = False
        state['error'] = str(error)
        checkpoint()
        raise


if __name__ == '__main__':
    raise SystemExit(main())

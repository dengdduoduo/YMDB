# Copyright (c) YugabyteDB, Inc.
"""Reproduce twohop1024 and measure two matched, controlled locality workloads."""

import argparse
import gzip
import hashlib
import json
from pathlib import Path
import re
import shlex
import socket
import subprocess
import time
import uuid

from round3_policy_fixture import load_case, uri
from round4_policy_session_measure import command, validate_process
from second_hop_rpc_trace import analyze, tablet_ranges


BASELINE_SHA256 = '96df1b1181fac7f7a1c8e9e4d33abbd4d6edf73c2e6b6b0daa7086af1650819b'


def digest(data):
    return hashlib.sha256(data).hexdigest()


def save(path, value):
    path.write_text(json.dumps(value, indent=2), encoding='utf8')


def controls(directory):
    """Both cases have the same anchors, loops, bindings, degree and page size."""
    directory.mkdir()
    subjects = [f'urn:hop2:seed:{n}' for n in range(1024)]
    centers = [f'urn:hop2:center:{n}' for n in range(16)]
    objects = [[f'urn:hop2:object:{n}:{i}' for i in range(4)] for n in range(16)]
    triples = [(subject, 'seed', centers[i % len(centers)])
               for i, subject in enumerate(subjects)]
    for center, neighbors in zip(centers, objects):
        triples.append((center, 'loop', center))
        for neighbor in neighbors:
            triples.extend(((center, 'forward', neighbor), (neighbor, 'reverse', center)))
    data = ''.join(f'<{s}> <urn:hop2:{p}> <{o}> .\n' for s, p, o in triples).encode()
    (directory / 'dataset.nt').write_bytes(data)

    expected = [[uri(subject), uri(neighbor)] for i, subject in enumerate(subjects)
                for neighbor in objects[i % len(centers)]]
    raw = gzip.compress(json.dumps(expected).encode(), mtime=0)
    (directory / 'expected.json.gz').write_bytes(raw)
    cases = []
    tail = {'high1024': '?m <urn:hop2:forward> ?o',
            'low1024': '?o <urn:hop2:reverse> ?m'}
    for name, expansion in tail.items():
        query = ('SELECT ?s ?o WHERE { ?s <urn:hop2:seed> ?k . '
                 '?k <urn:hop2:loop> ?m . ' + expansion + ' . }')
        cases.append(dict(name=name, query=query, query_sha256=digest(query.encode()),
                          columns=['s', 'o'], expected_rows=len(expected),
                          expected_file='expected.json.gz', expected_sha256=digest(raw),
                          ordered=False, timeout_seconds=120))
    manifest = directory / 'dataset-manifest.json'
    save(manifest, dict(sha256=digest(data), cases=cases))
    return manifest, cases


def run(args, output, timeout):
    print(shlex.join(map(str, args)), flush=True)
    with output.open('wb') as stream:
        subprocess.run(args, stdout=stream, stderr=subprocess.STDOUT, timeout=timeout, check=True)


def open_port(port):
    with socket.socket() as connection:
        return connection.connect_ex(('127.0.0.1', port)) == 0


def snapshot(build, masters, keyspace, output):
    ranges = {}
    for index in ('spo', 'ops'):
        path = output / ('rdf_' + index + '.log')
        run([build / 'yb-admin', '--master_addresses=' + masters, 'list_tablets',
             'ycql.' + keyspace, 'rdf_' + index, '0'], path, 30)
        ranges[index] = tablet_ranges(path.read_text())
        if len(ranges[index]) != 4:
            raise ValueError(f'{keyspace}: expected four {index} tablets')
    return ranges


def measure(build, masters, keyspace, manifest, case, output, page_rows):
    output.mkdir()
    worker = output / 'worker'
    worker.write_text('#!/bin/bash\nexec ' + shlex.quote(str(build / 'mdb_fragment_worker')) +
                      ' --vmodule=graph_expand_pipeline=2,async_rpc=3 "$@"'
                      ' 2>"$(dirname "$0")/worker-$$.log"\n', encoding='utf8')
    worker.chmod(0o755)
    config = dict(cli=str(build / 'mdb_cli'), worker_binary=str(worker),
                  masters=masters, keyspace=keyspace)
    args = [arg if not arg.startswith('--mdb_worker_page_rows=') else
            f'--mdb_worker_page_rows={page_rows}' for arg in command(config, 'new', False)]
    args += ['query', case['query']]
    save(output / 'command.json', args)
    stdout, stderr = output / 'stdout.json', output / 'stderr.log'
    before = snapshot(build, masters, keyspace, output)
    print(shlex.join(args), flush=True)
    with stdout.open('wb') as out, stderr.open('wb') as err:
        code = subprocess.run(args, stdout=out, stderr=err, timeout=160).returncode
    record = validate_process(config, manifest, [case], 'new', False, code, stdout, stderr,
                              output)[0][0]
    if (record['status'] != 'passed' or record['profile']['graph_native_workers'] != 4
            or record['profile']['remote_coordinator_fallbacks'] != 0):
        raise ValueError('Expected four fragment workers without coordinator fallback')
    after = output / 'after'
    after.mkdir()
    if before != snapshot(build, masters, keyspace, after):
        raise ValueError('Tablet map changed during the trace')
    logs = list(output.glob('worker-*.log'))
    if len(logs) != 4:
        raise ValueError('Expected four worker logs')
    trace = analyze(logs, before)
    save(output / 'second-hop-rpcs.json', trace)
    totals = trace['totals']
    if totals['unknown_page_source_bindings'] or totals['source_bindings'] <= 0:
        raise ValueError('Incomplete probe source tablet mapping')
    reported_pages = sum(int(count) for log in logs
                         for count in re.findall(r'MDB_SECOND_HOP_READ read_operations=(\d+)',
                                                 log.read_text(encoding='utf8')))
    if totals['page_requests'] != reported_pages:
        raise ValueError('Per-RPC membership does not reconcile with second-hop read operations')
    return dict(case=case['name'], oracle=record['oracle'], totals=totals)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build-bin', required=True, type=Path)
    parser.add_argument('--cluster-config', required=True, type=Path)
    parser.add_argument('--baseline-manifest', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    options = parser.parse_args()
    import resource

    output = options.output.resolve()
    output.mkdir()
    build = options.build_bin.resolve()
    masters = '127.0.0.1:17100'
    cluster = json.loads(options.cluster_config.read_text())
    baseline_manifest = options.baseline_manifest
    baseline_metadata, baseline, _ = load_case(baseline_manifest, 'twohop1024')
    if baseline_metadata['sha256'] != BASELINE_SHA256:
        raise ValueError('Baseline data differs from the archived twohop1024 fixture')
    resource.setrlimit(resource.RLIMIT_NOFILE, (65536, resource.getrlimit(resource.RLIMIT_NOFILE)[1]))
    if any(open_port(port) for port in cluster['ports']):
        raise ValueError('The archived test cluster ports must be idle')
    save(output / 'runtimes.json', {name: digest((build / name).read_bytes())
                                   for name in ('mdb_cli', 'mdb_fragment_worker')})
    daemons = []
    summary = []
    try:
        for i, args in enumerate(cluster['commands']):
            print(shlex.join(args), flush=True)
            with (output / f'daemon-{i}.log').open('wb') as stream:
                daemons.append(subprocess.Popen(args, stdout=stream, stderr=stream,
                                                start_new_session=True))
        save(output / 'processes.json', dict(pids=[p.pid for p in daemons], config=cluster))
        # The existing fixture needs time for its tablets to replay on the original Debug VM.
        time.sleep(65)
        if any(process.poll() is not None for process in daemons):
            raise ValueError('An isolated test daemon exited')
        summary.append(measure(build, masters, 'r3_policy_txn_20261102', baseline_manifest,
                               baseline, output / baseline['name'], 16))
        save(output / 'summary.json', summary)
        manifest, cases = controls(output / 'controls')
        keyspace = 'hop2_trace_' + uuid.uuid4().hex[:12]
        save(output / 'control-keyspace.json', dict(keyspace=keyspace))
        run([build / 'mdb_cli', '--mdb_masters=' + masters, '--mdb_keyspace=' + keyspace,
             'import', manifest.parent / 'dataset.nt'], output / 'import.log', 300)
        for case in cases:
            result = measure(build, masters, keyspace, manifest, case, output / case['name'], 2)
            counts = result['totals']
            expected_local = counts['source_bindings'] if case['name'] == 'high1024' else 0
            if (counts['source_bindings'] != 1024 or counts['local_source_bindings'] != expected_local
                    or counts['continuation_pages'] <= 0):
                raise ValueError('The control did not exercise its intended locality and paging')
            summary.append(result)
            save(output / 'summary.json', summary)
        print(json.dumps(summary, indent=2), flush=True)
    finally:
        for process in reversed(daemons):
            if process.poll() is None:
                process.terminate()
        for process in daemons:
            process.wait(timeout=45)
        save(output / 'cleanup.json', {port: open_port(port) for port in cluster['ports']})


if __name__ == '__main__':
    main()

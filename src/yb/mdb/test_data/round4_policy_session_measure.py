# Copyright (c) YugabyteDB, Inc.
"""Compare independent queries, sequential sessions and bounded worker reuse.

Uses the round-three B fixture/oracle without changing production behavior.
Every request and complete group is retained, including declared deadlines.
"""
import argparse
from collections import defaultdict
import json
import os
from pathlib import Path
import shlex
import signal
import statistics
import subprocess
import threading
import time

from round3_policy_fixture import digest, load_case, validate
from round3_policy_measure import binary_identity, process_rss
from round3_policy_measure import declared_deadline_failure as one_shot_deadline_failure


LAYOUTS = ('independent', 'session', 'pool')
MODES = ('old', 'new', 'auto')


def declared_deadline_failure(config, record, stdout_path, stderr):
    """The same query deadline can expire in Gather or its frame reader."""
    if one_shot_deadline_failure(config, record, stdout_path, stderr):
        return True
    allowed = {(item['query_id'], item['mode'])
               for item in config.get('deadline_failure_contracts', [])}
    status = record.get('cli_timing', {}).get('status')
    return ((record['query_id'], record['mode']) in allowed and
            record['status'] == 'cli_failed' and record.get('exit_code') == 1 and
            type(status) is int and status == 1 and
            Path(stdout_path).stat().st_size == 0 and
            'MDB fragment query failed: Timed out' in stderr and
            'fragment_transport.cc:' in stderr and
            'MDB fragment deadline expired' in stderr)


def request_markers(stderr, count, session):
    """A status terminates its request, so profiles cannot leak into the next one."""
    requests, pending, pools = [], [], []
    for line in stderr.splitlines():
        if line.startswith('MDB_WORKER_POOL '):
            pools.append(json.loads(line[len('MDB_WORKER_POOL '):]))
        elif line.startswith('MDB_SESSION_REQUEST '):
            marker = json.loads(line[len('MDB_SESSION_REQUEST '):])
            if (not session or type(marker.get('index')) is not int or
                    marker['index'] != len(requests) or
                    type(marker.get('status')) is not int or marker['status'] not in (0, 1)):
                raise ValueError('Invalid or reordered request status')
            requests.append((marker['status'], pending))
            pending = []
        else:
            pending.append(line)
    if not session:
        requests = [(None, pending)]
    elif any(line.startswith(('MDB_CLI_TIMING ', 'MDB_QUERY_PROFILE ')) for line in pending):
        raise ValueError('Uncorrelated request diagnostics')
    if len(requests) != count:
        raise ValueError('Missing request status')
    parsed = []
    for status, lines in requests:
        values = {}
        for marker, name in [('MDB_QUERY_PROFILE ', 'profile'), ('MDB_CLI_TIMING ', 'cli_timing')]:
            matches = [json.loads(line[len(marker):]) for line in lines if line.startswith(marker)]
            if len(matches) > 1 or (name == 'cli_timing' and len(matches) != 1):
                raise ValueError('Missing or duplicated request diagnostics')
            if matches:
                values[name] = matches[0]
        timing_status = values['cli_timing'].get('status')
        if type(timing_status) is not int or timing_status not in (0, 1):
            raise ValueError('Invalid timing status')
        if status is not None and timing_status != status:
            raise ValueError('Timing and request status disagree')
        values.update(status=timing_status, stderr='\n'.join(lines))
        if timing_status == 0 and 'profile' not in values:
            raise ValueError('Successful request has no profile')
        parsed.append(values)
    return parsed, pools


def documents(text):
    result, decoder = [], json.JSONDecoder()
    while text.strip():
        text = text.lstrip()
        item, end = decoder.raw_decode(text)
        result.append(item)
        text = text[end:]
    return result


def validate_process(config, manifest, cases, mode, session, code, out, err, directory):
    requests, pools = request_markers(Path(err).read_text(errors='replace'), len(cases), session)
    docs = documents(Path(out).read_text())
    if len(docs) != sum(r['status'] == 0 for r in requests):
        raise ValueError('Public documents do not match successful requests')
    expected_exit = int(any(r['status'] for r in requests))
    if code != expected_exit:
        raise ValueError('Process exit disagrees with request outcomes or final cleanup')
    position, records = 0, []
    for index, (case, request) in enumerate(zip(cases, requests)):
        record = dict(query_id=case['name'], mode=mode, request_index=index,
                      cli_timing=request['cli_timing'], profile=request.get('profile'),
                      status='passed' if request['status'] == 0 else 'cli_failed',
                      failure_contract_verified=False)
        if request['status'] == 0:
            result = directory / ('request-' + str(index) + '.json')
            result.write_text(json.dumps(docs[position]))
            position += 1
            record['oracle'] = validate(manifest, case['name'], result)
            record['result_path'] = str(result)
        else:
            empty = directory / ('failed-request-' + str(index) + '.json')
            empty.write_bytes(b'')
            record['exit_code'] = 1
            if not declared_deadline_failure(config, record, empty, request['stderr']):
                raise ValueError('Undeclared request failure: ' + case['name'])
            record['failure_contract_verified'] = True
            record['oracle'] = dict(complete_typed_bag=False, no_public_rows=True)
        records.append(record)
    return records, pools


def start_counts(config):
    if not config.get('spawn_log'):
        return None
    path = Path(config['spawn_log'])
    counts = {'local': len(path.read_text().splitlines()) if path.exists() else 0}
    if config.get('worker_ssh_target'):
        code = ('from pathlib import Path; p=Path(' + repr(str(path)) +
                '); print(len(p.read_text().splitlines()) if p.exists() else 0)')
        args = ['ssh', '-F', config['worker_ssh_config'], '-o', 'BatchMode=yes',
                '-o', 'ConnectTimeout=8', config['worker_ssh_target'],
                shlex.join(['python3', '-c', code])]
        counts['remote'] = int(subprocess.check_output(args, text=True, timeout=15))
    return counts


def command(config, mode, pool):
    args = [*config.get('command_prefix', []), config['cli'],
            '--mdb_masters=' + config['masters'], '--mdb_keyspace=' + config['keyspace'],
            '--mdb_streaming_reads=true', '--mdb_use_partition_load=false',
            '--mdb_scan_page_rows=16', '--mdb_join_policy=index',
            '--mdb_result_format=json', '--mdb_query_profile=true',
            '--mdb_graph_mode=' + mode,
            '--mdb_worker_targets=' + config.get('worker_targets', 'local'),
            '--mdb_worker_binary=' + config['worker_binary'],
            '--mdb_worker_max_processes=2', '--mdb_worker_batch_rows=128',
            '--mdb_worker_page_rows=16', '--mdb_worker_memory_bytes=8388608',
            '--mdb_global_memory_bytes=67108864',
            '--mdb_worker_reuse=' + ('true' if pool else 'false')]
    if config.get('worker_ssh_config'):
        args.append('--mdb_worker_ssh_config=' + config['worker_ssh_config'])
    return args


def invoke(args, cases, config, manifest, mode, session, directory):
    directory.mkdir()
    out, err = directory / 'stdout.jsonl', directory / 'stderr.log'
    record = dict(command=args, status='started', stdout=str(out), stderr=str(err))
    stop, peaks, simultaneous = threading.Event(), {}, [0]
    started = time.perf_counter()
    try:
        with out.open('wb') as fo, err.open('wb') as fe:
            proc = subprocess.Popen(args, stdout=fo, stderr=fe, start_new_session=True)
            watcher = threading.Thread(
                target=process_rss, args=(stop, proc.pid, peaks, simultaneous))
            watcher.start()
            try:
                proc.wait(timeout=min(config.get('process_timeout_seconds', 360),
                                      sum(c['timeout_seconds'] for c in cases) + 30))
            except BaseException:
                os.killpg(proc.pid, signal.SIGTERM)
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(proc.pid, signal.SIGKILL)
                    proc.wait()
                raise
            finally:
                record['wall_seconds'] = time.perf_counter() - started
                stop.set()
                watcher.join()
        record['exit_code'] = proc.returncode
        record['requests'], record['pool'] = validate_process(
            config, manifest, cases, mode, session, proc.returncode, out, err, directory)
        if '--mdb_worker_reuse=true' in args and len(record['pool']) != 1:
            raise ValueError('Pooled session has no unique final pool diagnostic')
        record['status'] = ('passed' if all(r['status'] == 'passed' for r in record['requests'])
                            else 'declared_failure')
        return record
    except BaseException as error:
        record.update(status='gate_failed', error=str(error))
        raise
    finally:
        record.update(observed_process_peak_rss_kib=peaks,
                      observed_local_query_tree_peak_rss_kib=simultaneous[0])
        (directory / 'process.json').write_text(json.dumps(record, indent=2))


def summarize(groups):
    cells = defaultdict(list)
    for group in groups:
        if not group['warmup']:
            cells[(group['mode'], group['layout'])].append(group)
    result = []
    for (mode, layout), rows in sorted(cells.items()):
        complete = all(r['status'] == 'passed' for r in rows)
        wall = [r['wall_seconds'] for r in rows if 'wall_seconds' in r]
        median = statistics.median(wall) if complete else None
        result.append(dict(mode=mode, layout=layout, groups=len(rows),
            complete_groups=sum(r['status'] == 'passed' for r in rows),
            failed_groups=[r['run_id'] for r in rows if r['status'] != 'passed'],
            wall_median_seconds=median, wall_min_seconds=min(wall) if complete else None,
            wall_max_seconds=max(wall) if complete else None,
            wall_mad_seconds=statistics.median(abs(x-median) for x in wall) if complete else None,
            performance_acceptance=complete and len(rows) >= 5))
    return result


def campaign(config_path, manifest_path, output, selected, repeats, warmups,
             layouts=LAYOUTS, modes=MODES):
    config_path, manifest_path, output = Path(config_path), Path(manifest_path), Path(output)
    config = json.loads(config_path.read_text())
    for field in ['cli', 'worker_binary', 'masters', 'keyspace', 'BASE_SHA',
                  'source_digest', 'build_version']:
        if not config.get(field):
            raise ValueError('Missing configuration field: ' + field)
    if repeats < 1 or warmups < 0 or not layouts or not modes:
        raise ValueError('Invalid campaign dimensions')
    if set(layouts) - set(LAYOUTS) or set(modes) - set(MODES):
        raise ValueError('Unknown layout or mode')
    manifest = json.loads(manifest_path.read_text())
    names = selected or [c['name'] for c in manifest['cases']]
    if not 1 <= len(names) <= 1024 or len(set(names)) != len(names):
        raise ValueError('Invalid or duplicated cases')
    cases = [load_case(manifest_path, name)[1] for name in names]
    output.mkdir(parents=True, exist_ok=False)
    worker_identity = config.get('worker_identity_binary', config['worker_binary'])
    identity = dict(config=config, config_sha256=digest(config_path.read_bytes()),
        cli=binary_identity(config['cli']), worker=binary_identity(worker_identity),
        manifest_sha256=digest(manifest_path.read_bytes()), dataset_sha256=manifest['sha256'],
        measurement_tool_sha256=digest(Path(__file__).read_bytes()),
        oracle_sha256=digest((Path(__file__).parent / 'round3_policy_fixture.py').read_bytes()),
        dependency_sha256=digest((Path(__file__).parent / 'round3_policy_measure.py').read_bytes()),
        cases=names, repeats=repeats, warmups=warmups, layouts=layouts, modes=modes,
        wall_boundary=('sum of complete child-process wall times for one fixed query group; '
                       'includes initialization, output and cleanup'),
        missing_metrics=['remote_native_worker_rss', 'server_rss_attribution',
                         'physical_network_rpc_count', 'isolated_auto_probe_time'])
    (output / 'identity.json').write_text(json.dumps(identity, indent=2))
    query_files = {}
    for case in cases:
        path = output / (case['name'] + '.rq')
        path.write_text(case['query'])
        query_files[case['name']] = str(path)
    groups, cells = [], [(m, l) for m in modes for l in layouts]
    for repeat in range(warmups + repeats):
        order = cells[repeat % len(cells):] + cells[:repeat % len(cells)]
        for mode, layout in order:
            ordered = cases[repeat % len(cases):] + cases[:repeat % len(cases)]
            run_id = mode + '-' + layout + '-' + str(repeat).zfill(2)
            directory = output / run_id
            directory.mkdir()
            group = dict(run_id=run_id, mode=mode, layout=layout, repeat=repeat,
                         warmup=repeat < warmups, case_order=[c['name'] for c in ordered],
                         status='started', processes=[])
            before = start_counts(config)
            try:
                batches = [[c] for c in ordered] if layout == 'independent' else [ordered]
                for index, batch in enumerate(batches):
                    args = command(config, mode, layout == 'pool')
                    args += (['query', batch[0]['query']] if layout == 'independent' else
                             ['session'] + [query_files[c['name']] for c in batch])
                    group['processes'].append(invoke(args, batch, config, manifest_path,
                        mode, layout != 'independent', directory / ('process-' + str(index))))
                # Do not include Python parsing, oracle verification, RSS watcher joins,
                # or inter-query orchestration in the user-process wall measurement.
                group['wall_seconds'] = sum(p['wall_seconds'] for p in group['processes'])
                group['status'] = (
                    'passed' if all(p['status'] == 'passed' for p in group['processes'])
                    else 'declared_failure')
                after = start_counts(config)
                group['native_starts'] = ({key: after[key]-before[key] for key in before}
                                          if before is not None else None)
            except BaseException as error:
                group.update(status='gate_failed', error=str(error))
                raise
            finally:
                groups.append(group)
                (directory / 'group.json').write_text(json.dumps(group, indent=2))
                with (output / 'groups.jsonl').open('a') as f:
                    f.write(json.dumps(group) + '\n')
                (output / 'summary.json').write_text(json.dumps(summarize(groups), indent=2))
                print(json.dumps({k: group.get(k) for k in
                    ['run_id', 'status', 'wall_seconds', 'native_starts']}), flush=True)
    if (identity['cli'] != binary_identity(config['cli']) or
            identity['worker'] != binary_identity(worker_identity)):
        raise RuntimeError('Runtime binary/library identity changed')
    requests = [r for g in groups for p in g['processes'] for r in p['requests']]
    done = dict(completed=True, passed=all(g['status'] == 'passed' for g in groups),
                groups=len(groups), requests=len(requests),
                full_result_passes=sum(r['status'] == 'passed' for r in requests),
                verified_deadline_failures=sum(r['failure_contract_verified'] for r in requests))
    (output / 'complete.json').write_text(json.dumps(done, indent=2))
    return done


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True)
    parser.add_argument('--manifest', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--cases', nargs='+')
    parser.add_argument('--layouts', nargs='+', choices=LAYOUTS, default=list(LAYOUTS))
    parser.add_argument('--modes', nargs='+', choices=MODES, default=list(MODES))
    parser.add_argument('--repeats', type=int, default=5)
    parser.add_argument('--warmups', type=int, default=1)
    args = parser.parse_args()
    campaign(args.config, args.manifest, args.output, args.cases, args.repeats,
             args.warmups, args.layouts, args.modes)

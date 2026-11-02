# Copyright (c) YugabyteDB, Inc.
"""Rotate one-shot CLI modes, validate full results, retain every failed sample.

Configuration requires cli, worker_binary, masters, keyspace, BASE_SHA,
source_digest and build_version. Paths, server topology and source/binary identity
are frozen per campaign. This tool never starts/stops a database service.
"""
import argparse
from collections import defaultdict
import hashlib
import json
import os
from pathlib import Path
import signal
import statistics
import subprocess
import threading
import time

from round3_policy_fixture import digest, validate


def process_rss(stop, root, peaks, simultaneous):
    """Sample only this command's process tree; peaks are observations, not limits."""
    while not stop.is_set():
        table = {}
        for path in Path('/proc').glob('[0-9]*/status'):
            try:
                fields = dict(line.split(':', 1) for line in path.read_text().splitlines() if ':' in line)
                table[int(path.parent.name)] = (int(fields['PPid']), int(fields.get('VmRSS', '0 kB').split()[0]))
            except (OSError, ValueError, KeyError):
                pass
        family = {root}
        for _ in range(16):
            more = {pid for pid, (parent, _) in table.items() if parent in family}
            if more <= family:
                break
            family |= more
        current = 0
        for pid in family:
            rss = table.get(pid, (0, 0))[1]
            peaks[str(pid)] = max(peaks.get(str(pid), 0), rss)
            current += rss
        simultaneous[0] = max(simultaneous[0], current)
        stop.wait(0.05)


def binary_identity(binary):
    path = Path(binary).resolve()
    result = dict(path=str(path), sha256=digest(path.read_bytes()))
    linked = subprocess.run(['ldd', str(path)], capture_output=True, text=True, check=True)
    libraries = {}
    for line in linked.stdout.splitlines():
        if '=>' in line:
            token = line.split('=>', 1)[1].strip().split()[0]
            p = Path(token)
            if p.is_absolute() and p.is_file():
                libraries[str(p.resolve())] = digest(p.read_bytes())
    result['linked_libraries'] = libraries
    return result


def markers(stderr):
    fields = {}
    for line in stderr.splitlines():
        for marker, name in [('MDB_QUERY_PROFILE ', 'profile'), ('MDB_CLI_TIMING ', 'cli_timing')]:
            if line.startswith(marker):
                fields[name] = json.loads(line[len(marker):])
    return fields


def summarize(records):
    groups = defaultdict(list)
    for row in records:
        if not row['warmup']:
            groups[(row['query_id'], row['mode'])].append(row)
    result = []
    for (query, mode), rows in sorted(groups.items()):
        good = [r for r in rows if r['status'] == 'passed']
        wall = [r['wall_seconds'] for r in good]
        metrics = {}
        for field in ['adapter_plan_us','adapter_execute_us','adapter_total_us','storage_flush_us',
                      'remote_spawn_us','remote_open_wait_us','remote_pull_wait_us','remote_cleanup_us',
                      'read_operations','remote_read_operations','peak_buffered_bytes',
                      'graph_native_workers','remote_coordinator_fallbacks']:
            values = [r.get('profile',{}).get(field) for r in good]
            metrics[field] = (statistics.median(values) if values and
                              all(isinstance(x,(int,float)) for x in values) else None)
        result.append(dict(query_id=query, mode=mode, samples=len(rows), passed=len(good),
                           failures=[r['run_id'] for r in rows if r['status'] != 'passed'],
                           wall_median_seconds=statistics.median(wall) if wall else None,
                           wall_min_seconds=min(wall) if wall else None,
                           wall_max_seconds=max(wall) if wall else None,
                           wall_mad_seconds=statistics.median(abs(x-statistics.median(wall)) for x in wall) if wall else None,
                           selected=[r.get('profile', {}).get('graph_execution_decision') for r in rows],
                           profile_medians=metrics,
                           performance_acceptance=len(rows)>=5 and len(good)==len(rows)))
    return result


def campaign(config_path, manifest_path, output, selected, repeats, warmups):
    config_path, manifest_path, output = Path(config_path), Path(manifest_path), Path(output)
    output.mkdir(parents=True, exist_ok=False)
    config = json.loads(config_path.read_text())
    for field in ['cli', 'worker_binary', 'masters', 'keyspace', 'BASE_SHA', 'source_digest', 'build_version']:
        if not config.get(field):
            raise ValueError('Missing configuration field: ' + field)
    manifest = json.loads(manifest_path.read_text())
    cases = [c for c in manifest['cases'] if not selected or c['name'] in selected]
    if selected and set(selected) != {c['name'] for c in cases}:
        raise ValueError('Unknown case')
    identity = dict(config=config, config_sha256=digest(config_path.read_bytes()),
                    measurement_tool_sha256=digest(Path(__file__).read_bytes()),
                    oracle_tool_sha256=digest((Path(__file__).parent/'round3_policy_fixture.py').read_bytes()),
                    manifest_sha256=digest(manifest_path.read_bytes()),
                    dataset_sha256=manifest['sha256'], cli=binary_identity(config['cli']),
                    worker=binary_identity(config['worker_binary']), cases=[c['name'] for c in cases],
                    repeats=repeats, warmups=warmups, rss_sampling_seconds=0.05,
                    wall_boundary='before Popen to process exit including startup/init/workers/output/cleanup',
                    missing_metrics=['isolated_auto_decision_time', 'physical_network_rpc_count', 'server_rss_attribution'])
    (output/'identity.json').write_text(json.dumps(identity, indent=2))
    rows = []
    for repeat in range(warmups+repeats):
        order = cases[repeat % len(cases):] + cases[:repeat % len(cases)]
        for case in order:
            modes = ['old','new','auto']
            modes = modes[repeat % 3:] + modes[:repeat % 3]
            for mode in modes:
                run_id = case['name']+'-'+mode+'-'+str(repeat).zfill(2)
                stdout, stderr = output/(run_id+'.json'), output/(run_id+'.stderr')
                args = [config['cli'], '--mdb_masters='+config['masters'],
                        '--mdb_keyspace='+config['keyspace'], '--mdb_streaming_reads=true',
                        '--mdb_result_format=json','--mdb_query_profile=true',
                        '--mdb_graph_mode='+mode, '--mdb_worker_targets=local',
                        '--mdb_worker_binary='+config['worker_binary'], '--mdb_worker_max_processes=2',
                        '--mdb_worker_batch_rows=128','--mdb_worker_page_rows=16',
                        '--mdb_worker_memory_bytes=8388608','--mdb_global_memory_bytes=67108864',
                        *config.get('extra_flags', []), 'query', case['query']]
                record = dict(run_id=run_id, query_id=case['name'], mode=mode, repeat=repeat,
                              warmup=repeat<warmups, BASE_SHA=config['BASE_SHA'],
                              build_version=config['build_version'], source_digest=config['source_digest'],
                              config_sha256=identity['config_sha256'], input_sha256=manifest['sha256'],
                              query_sha256=case['query_sha256'], result_path=str(stdout),
                              stderr_path=str(stderr), command=args, status='started')
                stop, peaks, simultaneous = threading.Event(), {}, [0]
                started = time.perf_counter()
                with stdout.open('wb') as out, stderr.open('wb') as err:
                    proc = subprocess.Popen(args, stdout=out, stderr=err, start_new_session=True)
                    watcher = threading.Thread(target=process_rss, args=(stop,proc.pid,peaks,simultaneous))
                    watcher.start()
                    try:
                        code = proc.wait(timeout=case['timeout_seconds'])
                        record['exit_code'] = code
                        record['status'] = 'exited' if code==0 else 'cli_failed'
                    except subprocess.TimeoutExpired:
                        os.killpg(proc.pid, signal.SIGKILL)
                        proc.wait()
                        record['status'] = 'timeout'
                    finally:
                        record['wall_seconds'] = time.perf_counter()-started
                        stop.set()
                        watcher.join()
                record['observed_process_peak_rss_kib'] = peaks
                record['observed_simultaneous_query_tree_rss_peak_kib'] = simultaneous[0]
                try:
                    record.update(markers(stderr.read_text(errors='replace')))
                    if record['status']=='exited':
                        if 'profile' not in record or 'cli_timing' not in record:
                            raise ValueError('Missing profile or CLI timing; reject measurement')
                        if record['cli_timing'].get('status')!=0:
                            raise ValueError('CLI timing reports failure')
                        record['oracle'] = validate(manifest_path,case['name'],stdout)
                        record['status'] = 'passed'
                except Exception as error:
                    record['status'] = 'oracle_or_profile_failed'
                    record['error'] = str(error)
                rows.append(record)
                with (output/'runs.jsonl').open('a') as f:
                    f.write(json.dumps(record)+'\n')
                (output/'summary.json').write_text(json.dumps(summarize(rows),indent=2))
                print(json.dumps({k:record[k] for k in ['run_id','status','wall_seconds']}),flush=True)
                if record['status']!='passed':
                    raise RuntimeError('Correctness/timeout gate stopped campaign; artifacts retained: '+run_id)
    # Catch changed code/libraries, including accidental builds during measurement.
    if identity['cli']!=binary_identity(config['cli']) or identity['worker']!=binary_identity(config['worker_binary']):
        (output/'IDENTITY_CHANGED').write_text('Reject this campaign: binary/library identity changed')
        raise RuntimeError('Binary/library changed during campaign')
    (output/'complete.json').write_text(json.dumps(dict(passed=True,runs=len(rows))))


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--config',required=True)
    p.add_argument('--manifest',required=True)
    p.add_argument('--output',required=True)
    p.add_argument('--cases',nargs='*')
    p.add_argument('--repeats',type=int,default=5)
    p.add_argument('--warmups',type=int,default=1)
    a=p.parse_args()
    if a.repeats<1 or a.warmups<0: p.error('Invalid sample counts')
    campaign(a.config,a.manifest,a.output,a.cases,a.repeats,a.warmups)

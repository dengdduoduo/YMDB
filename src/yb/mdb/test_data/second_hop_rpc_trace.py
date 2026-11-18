# Copyright (c) YugabyteDB, Inc.
"""Join opt-in second-hop probe/page logs to the client's actual Read RPC batches."""

import argparse
import ast
from collections import Counter
import json
from pathlib import Path
import re


TABLET_RANGE = re.compile(
    r'^([0-9a-f]{32})\s+partition_key_start: ("(?:\\.|[^"\\])*")'
    r' partition_key_end: ("(?:\\.|[^"\\])*")', re.MULTILINE)
BATCH_HEADER = re.compile(r'Created batch for ([0-9a-f]{32}):$')
PROTO_TOKEN = re.compile(r'"(?:\\.|[^"\\])*"|\bql_batch\s*\{|[{}]')
TRACE_MARKER = re.compile(r'MDB_HOP2_(BEGIN|PROBE|PAGE|END) (.*)$')


def tablet_ranges(text):
    def bound(value, default):
        raw = ast.literal_eval('b' + value)
        if raw and len(raw) != 2:
            raise ValueError('Expected a two-byte YCQL hash bound')
        return int.from_bytes(raw, 'big') if raw else default

    ranges = sorted((bound(start, 0), bound(end, 65536), tablet)
                    for tablet, start, end in TABLET_RANGE.findall(text))
    if (not ranges or ranges[0][0] != 0 or ranges[-1][1] != 65536
            or any(start >= end for start, end, _ in ranges)
            or any(left[1] != right[0] for left, right in zip(ranges, ranges[1:]))
            or len({tablet for _, _, tablet in ranges}) != len(ranges)):
        raise ValueError('Tablet ranges do not uniquely cover the hash space')
    return ranges


def ql_members(message):
    """Extract direct ql_batch entries without splitting on nested braces or strings."""
    depth = 0
    start = None
    for token in PROTO_TOKEN.finditer(message):
        value = token.group()
        if value == '}':
            depth -= 1
            if depth == 0 and start is not None:
                entry = message[start:token.start()]
                query_id = re.search(r'\bquery_id: (-?\d+)\b', entry)
                if not query_id:
                    raise ValueError('Read RPC member has no query_id')
                yield int(query_id[1]), bool(re.search(r'\bpaging_state\s*\{', entry))
                start = None
        elif value.endswith('{'):
            if depth == 0 and value != '{':
                start = token.end()
            depth += 1
    if depth != 0:
        raise ValueError('Unbalanced RPC protobuf log')


def analyze_worker(lines, partitions, worker):
    # The client can reuse an operation address (query_id) across flushes.
    totals = Counter()
    flushes = []
    active = None
    lines = iter(lines)
    for line in lines:
        marker = TRACE_MARKER.search(line)
        if marker:
            kind, payload = marker.groups()
            fields = dict(item.split('=', 1) for item in payload.split())
            if kind == 'BEGIN':
                if active is not None:
                    raise ValueError('Nested second-hop reads')
                active = dict(index=fields['target_index'], probes={}, pages={}, rpcs=[])
                continue
            if active is None:
                raise ValueError('Second-hop marker outside a read scope')
            if kind == 'PROBE':
                slot = int(fields['slot'])
                if slot in active['probes']:
                    raise ValueError('Duplicate probe slot')
                sources = []
                for item in fields['sources'].split(','):
                    origin, first_start, hash_text = item.split(':')
                    first_hash = None if hash_text == '-' else int(hash_text)
                    source_tablet = None
                    if first_hash is not None:
                        source_tablet = next(tablet for lo, hi, tablet
                                             in partitions[fields['source_index']]
                                             if lo <= first_hash < hi)
                    sources.append(dict(origin=int(origin), first_start=int(first_start),
                                        source_hash=first_hash, source_tablet=source_tablet))
                active['probes'][slot] = dict(
                    sources=sources, source_index=fields['source_index'], start=int(fields['start']),
                    predicate=int(fields['predicate']), neighbor=int(fields['neighbor']),
                    group=int(fields['group']))
            elif kind == 'PAGE':
                ident = int(fields['query_id'])
                slot = int(fields['slot'])
                page = int(fields['page'])
                continuation = fields['continuation'] == '1'
                if ident in active['pages'] or not ident or page < 1 or continuation != (page > 1):
                    raise ValueError('Invalid or duplicate page identity')
                if any(probe['slot'] == slot for probe in active['pages'].values()):
                    raise ValueError('Duplicate page slot')
                active['pages'][ident] = dict(active['probes'][slot], slot=slot,
                                              page=page, continuation=continuation)
            else:
                if fields['ok'] != '1':
                    raise ValueError('Second-hop read failed')
                seen = Counter(member['query_id'] for rpc in active['rpcs']
                               for member in rpc['members'])
                if (set(seen) != set(active['pages']) or any(count != 1 for count in seen.values())
                        or len(active['pages']) != len(active['probes']) or not seen):
                    raise ValueError('Second-hop probe, page and RPC membership mismatch')
                flushes.append(dict(worker=worker, flush=len(flushes) + 1, index=active['index'],
                                    traced_refill_us=int(fields['read_us']), rpcs=active['rpcs']))
                active = None
            continue

        header = BATCH_HEADER.search(line.rstrip())
        if not header or active is None:
            continue
        tablet = header[1]
        message = next(lines, '')
        if f'tablet_id: "{tablet}"' not in message:
            raise ValueError('Missing or inconsistent Read RPC protobuf')
        target_tablets = {item[2] for item in partitions[active['index']]}
        if tablet not in target_tablets:
            raise ValueError('Read routed outside the recorded table partition snapshot')
        members = []
        for ident, continuation in ql_members(message):
            page = active['pages'][ident]
            if continuation != page['continuation']:
                raise ValueError('RPC paging state disagrees with cursor')
            local = sum(source['source_tablet'] == tablet for source in page['sources'])
            unknown = sum(source['source_tablet'] is None for source in page['sources'])
            fully_local = local == len(page['sources'])
            totals.update(page_requests=1, continuation_pages=int(continuation),
                          fully_local_pages=int(fully_local), page_source_bindings=len(page['sources']),
                          local_page_source_bindings=local, unknown_page_source_bindings=unknown)
            if page['page'] == 1:
                totals.update(probes=1, fully_local_probes=int(fully_local),
                              source_bindings=len(page['sources']), local_source_bindings=local)
            members.append(dict(page, query_id=ident, local_sources=local, unknown_sources=unknown,
                                fully_local=fully_local))
        if not members:
            raise ValueError('Expected a YCQL Read RPC within the second-hop scope')
        local_pages = sum(member['fully_local'] for member in members)
        category = ('fully_local' if local_pages == len(members) else
                    'mixed' if local_pages else 'no_fully_local_member')
        totals.update(rpcs=1)
        totals[category + '_rpcs'] += 1
        active['rpcs'].append(dict(tablet=tablet, category=category, members=members))

    if active is not None or not flushes:
        raise ValueError(f'{worker}: missing or incomplete second-hop trace')
    return totals, flushes


def analyze(logs, partitions):
    totals = Counter()
    all_flushes = []
    for log in sorted(logs):
        with log.open(encoding='utf8') as stream:
            counts, flushes = analyze_worker(stream, partitions, log.name)
        totals.update(counts)
        all_flushes.extend(flushes)
    if not all_flushes:
        raise ValueError('No worker traces')
    for category in ('fully_local', 'mixed', 'no_fully_local_member'):
        totals.setdefault(category + '_rpcs', 0)
    return dict(totals=dict(totals), flushes=all_flushes)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--spo', required=True, type=Path)
    parser.add_argument('--ops', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('logs', nargs='+', type=Path)
    args = parser.parse_args()
    partitions = {index: tablet_ranges(getattr(args, index).read_text()) for index in ('spo', 'ops')}
    result = analyze(args.logs, partitions)
    with args.output.open('x', encoding='utf8') as output:
        json.dump(result, output, indent=2)
    print(json.dumps(result['totals'], sort_keys=True))

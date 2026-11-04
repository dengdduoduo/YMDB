# Copyright (c) YugabyteDB, Inc.
"""Reject uncorrelated sessions, partial failed output and biased summaries."""
import contextlib
import copy
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import round3_policy_fixture as fixture
import round4_policy_session_measure as measure


def diagnostics(statuses, session=True):
    lines = []
    for index, status in enumerate(statuses):
        lines.append('MDB_QUERY_PROFILE ' + json.dumps({'request': index}))
        if status:
            lines.append('MDB fragment query failed: Timed out: MDB Gather deadline expired')
        lines.append('MDB_CLI_TIMING ' + json.dumps({'status': status, 'init_us': int(index == 0)}))
        if session:
            lines.append('MDB_SESSION_REQUEST ' + json.dumps({'index': index, 'status': status}))
    return '\n'.join(lines)


class SessionGateTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.manifest = self.root / 'fixture/dataset-manifest.json'
        fixture.generate(self.manifest.parent)
        self.case = fixture.load_case(self.manifest, 'empty')[1]
        self.out, self.err = self.root / 'out.json', self.root / 'err.log'
        self.empty = {'head': {'vars': ['s', 'o']}, 'results': {'bindings': []}}

    def tearDown(self):
        self.temp.cleanup()

    def validate(self, statuses, docs, config=None, cause='MDB Gather deadline expired'):
        self.out.write_text('\n'.join(json.dumps(d) for d in docs))
        self.err.write_text(diagnostics(statuses).replace('MDB Gather deadline expired', cause))
        return measure.validate_process(config or {}, self.manifest,
            [self.case] * len(statuses), 'old', True, int(any(statuses)),
            self.out, self.err, self.root)

    def test_successes_use_their_own_profiles_and_order(self):
        rows, _ = self.validate([0, 0], [self.empty, self.empty])
        self.assertEqual([r['profile']['request'] for r in rows], [0, 1])
        self.assertEqual([r['cli_timing']['init_us'] for r in rows], [1, 0])
        self.assertTrue(all(r['oracle']['complete_typed_bag'] for r in rows))

    def test_declared_failed_middle_request_is_not_a_pass(self):
        config = {'deadline_failure_contracts': [{'query_id': 'empty', 'mode': 'old'}]}
        rows, _ = self.validate([0, 1, 0], [self.empty, self.empty], config)
        self.assertEqual([r['status'] for r in rows], ['passed', 'cli_failed', 'passed'])
        self.assertTrue(rows[1]['failure_contract_verified'])
        self.assertFalse(rows[1]['oracle']['complete_typed_bag'])

    def test_frame_read_deadline_retains_failure_and_validates_recovery(self):
        config = {'deadline_failure_contracts': [{'query_id': 'empty', 'mode': 'old'}]}
        cause = '(yb/mdb/query/distributed/fragment_transport.cc:62): MDB fragment deadline expired'
        rows, _ = self.validate([0, 1, 0], [self.empty, self.empty], config, cause)
        self.assertEqual([r['status'] for r in rows], ['passed', 'cli_failed', 'passed'])
        self.assertTrue(rows[1]['failure_contract_verified'])
        self.assertFalse(rows[1]['oracle']['complete_typed_bag'])

    def test_other_transport_deadlines_and_undeclared_causes_are_rejected(self):
        config = {'deadline_failure_contracts': [{'query_id': 'empty', 'mode': 'old'}]}
        causes = ['MDB worker exit deadline expired', 'Network error: Connection refused',
                  'MDB fragment deadline expired',
                  '(fragment_transport.cc:62): MDB fragment cancelled']
        for cause in causes:
            with self.subTest(cause=cause), self.assertRaisesRegex(ValueError, 'Undeclared'):
                self.validate([0, 1, 0], [self.empty, self.empty], config, cause)

    def test_failed_request_cannot_publish_a_document(self):
        config = {'deadline_failure_contracts': [{'query_id': 'empty', 'mode': 'old'}]}
        with self.assertRaisesRegex(ValueError, 'Public documents'):
            self.validate([0, 1, 0], [self.empty] * 3, config)

    def test_wrong_projection_after_failure_still_stops(self):
        config = {'deadline_failure_contracts': [{'query_id': 'empty', 'mode': 'old'}]}
        bad = copy.deepcopy(self.empty)
        bad['head']['vars'].reverse()
        with self.assertRaisesRegex(ValueError, 'Projection'):
            self.validate([0, 1, 0], [self.empty, bad], config)

    def test_missing_reordered_or_duplicate_markers_are_rejected(self):
        text = diagnostics([0, 0])
        variants = [text.replace('"index": 1', '"index": 0'),
                    text.replace('MDB_SESSION_REQUEST', 'IGNORED', 1),
                    text + '\nMDB_CLI_TIMING {"status":0}',
                    text.replace('"index": 0', '"index": false')]
        for variant in variants:
            with self.subTest(variant=variant), self.assertRaises(ValueError):
                measure.request_markers(variant, 2, True)

    def test_disagreeing_timing_or_final_cleanup_failure_is_rejected(self):
        text = diagnostics([0]).replace('"status": 0, "init_us"', '"status": 1, "init_us"')
        with self.assertRaisesRegex(ValueError, 'disagree'):
            measure.request_markers(text, 1, True)
        self.out.write_text(json.dumps(self.empty))
        self.err.write_text(diagnostics([0]))
        with self.assertRaisesRegex(ValueError, 'Process exit'):
            measure.validate_process({}, self.manifest, [self.case], 'old', True,
                                     1, self.out, self.err, self.root)

    def test_failed_group_has_no_success_only_median(self):
        common = dict(mode='old', layout='pool', warmup=False)
        rows = [dict(common, run_id='ok', status='passed', wall_seconds=1),
                dict(common, run_id='failed', status='declared_failure', wall_seconds=30)]
        result = measure.summarize(rows)[0]
        self.assertIsNone(result['wall_median_seconds'])
        self.assertFalse(result['performance_acceptance'])
        result = measure.summarize([dict(common, run_id='gate', status='gate_failed')])[0]
        self.assertIsNone(result['wall_median_seconds'])


@unittest.skipUnless(os.name == 'posix', 'CLI simulation requires POSIX process control')
class CampaignTest(unittest.TestCase):
    def test_three_layouts_preserve_complete_results_and_explicit_flags(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fixture.generate(root / 'input')
            cli = root / 'fake-cli'
            cli.write_text('''#!/usr/bin/env python3
import json,sys
session='session' in sys.argv
count=len(sys.argv)-sys.argv.index('session')-1 if session else 1
for i in range(count):
 print(json.dumps({'head':{'vars':['s','o']},'results':{'bindings':[]}}))
 print('MDB_QUERY_PROFILE '+json.dumps({'request':i}),file=sys.stderr)
 print('MDB_CLI_TIMING '+json.dumps({'status':0,'init_us':int(i==0)}),file=sys.stderr)
 if session: print('MDB_SESSION_REQUEST '+json.dumps({'index':i,'status':0}),file=sys.stderr)
if '--mdb_worker_reuse=true' in sys.argv:
 print('MDB_WORKER_POOL '+json.dumps({'process_starts':0}),file=sys.stderr)
''')
            cli.chmod(0o755)
            config = dict(cli=str(cli), worker_binary=str(cli), masters='127.0.0.1:1',
                          keyspace='synthetic', BASE_SHA='synthetic',
                          source_digest='synthetic', build_version='synthetic')
            path = root / 'config.json'
            path.write_text(json.dumps(config))
            with patch.object(measure, 'binary_identity', return_value={'synthetic': True}), \
                    contextlib.redirect_stdout(io.StringIO()):
                done = measure.campaign(path, root / 'input/dataset-manifest.json',
                    root / 'output', ['empty'], 1, 0, measure.LAYOUTS, ['new'])
            self.assertEqual(done['full_result_passes'], 3)
            self.assertTrue(done['passed'])
            groups = [json.loads(line) for line in
                      (root / 'output/groups.jsonl').read_text().splitlines()]
            for group in groups:
                command = group['processes'][0]['command']
                self.assertIn('--mdb_worker_reuse=' +
                              ('true' if group['layout'] == 'pool' else 'false'), command)
            self.assertTrue(all(not cell['performance_acceptance'] for cell in
                               json.loads((root / 'output/summary.json').read_text())))


if __name__ == '__main__':
    unittest.main()

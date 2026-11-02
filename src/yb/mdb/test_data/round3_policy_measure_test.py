# Copyright (c) YugabyteDB, Inc.
"""Check that deadline contracts never become successful query measurements."""
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
import round3_policy_measure as measure


class DeadlineContractTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.output = Path(self.temp.name)/'result.json'
        self.output.write_bytes(b'')
        self.config = {'deadline_failure_contracts': [{'query_id':'large','mode':'old'}]}
        self.record = dict(query_id='large',mode='old',status='cli_failed',exit_code=1,
                           cli_timing={'status':1})
        self.error = 'MDB fragment query failed: Timed out (synthetic.cc:1): MDB Gather deadline expired'

    def tearDown(self):
        self.temp.cleanup()

    def check(self, record=None, error=None):
        return measure.declared_deadline_failure(self.config,record or self.record,
                                                 self.output,self.error if error is None else error)

    def test_verified_contract_retains_failed_status(self):
        before=copy.deepcopy(self.record)
        self.assertTrue(self.check())
        self.assertEqual(self.record,before)
        self.assertEqual(self.record['status'],'cli_failed')

    def test_partial_public_output_is_never_accepted(self):
        self.output.write_bytes(b'{}')
        self.assertFalse(self.check())

    def test_undeclared_query_or_mode_is_never_accepted(self):
        for field,value in [('query_id','different'),('mode','new')]:
            self.assertFalse(self.check({**self.record,field:value}))

    def test_other_errors_and_successes_are_never_accepted(self):
        for record in [{**self.record,'status':'passed'}, {**self.record,'exit_code':0},
                       {**self.record,'exit_code':-6}, {**self.record,'exit_code':2},
                       {**self.record,'cli_timing':{'status':0}},
                       {**self.record,'cli_timing':{'status':True}},
                       {**self.record,'cli_timing':{}}]:
            self.assertFalse(self.check(record))
        self.assertFalse(self.check(error='Corruption: invalid result frame'))

    def test_failed_cell_has_no_success_only_headline_median(self):
        common=dict(query_id='large',mode='old',warmup=False,campaign_class='deadline_contract')
        rows=[dict(common,run_id='ok',status='passed',wall_seconds=1),
              dict(common,run_id='failed',status='cli_failed',wall_seconds=30,failure_contract_verified=True)]
        summary=measure.summarize(rows)[0]
        self.assertIsNone(summary['wall_median_seconds'])
        self.assertFalse(summary['performance_acceptance'])
        self.assertEqual(summary['failures'],['failed'])


@unittest.skipUnless(os.name=='posix','Executable CLI simulation uses POSIX process control')
class CampaignGateTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp=tempfile.TemporaryDirectory()
        cls.root=Path(cls.temp.name)
        fixture.generate(cls.root/'input')
        cls.cli=cls.root/'fake-cli'
        cls.cli.write_text('''#!/usr/bin/env python3
import json,sys
mode=next(a.split('=',1)[1] for a in sys.argv if a.startswith('--mdb_graph_mode='))
failed=mode=='old'
print('MDB_QUERY_PROFILE '+json.dumps({'graph_execution_decision':{'selected':'old'}}),file=sys.stderr)
print('MDB_CLI_TIMING '+json.dumps({'status':int(failed),'init_us':0,'execute_us':0,'publish_us':0,'total_us':0}),file=sys.stderr)
if failed:
    print('MDB fragment query failed: Timed out (synthetic.cc:1): MDB Gather deadline expired',file=sys.stderr)
    sys.exit(1)
columns=['o','s'] if mode=='auto' and '--fake_bad_auto' in sys.argv else ['s','o']
print(json.dumps({'head':{'vars':columns},'results':{'bindings':[]}}))
''')
        cls.cli.chmod(0o755)

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def run_campaign(self,name,bad=False):
        config=dict(cli=str(self.cli),worker_binary=str(self.cli),masters='127.0.0.1:1',
            keyspace='synthetic',BASE_SHA='synthetic',source_digest='synthetic',build_version='synthetic',
            deadline_failure_contracts=[{'query_id':'empty','mode':'old'}],
            case_classes={'empty':'deadline_contract'},extra_flags=['--fake_bad_auto'] if bad else [])
        path=self.root/(name+'.config.json');path.write_text(json.dumps(config))
        with patch.object(measure,'binary_identity',return_value={'synthetic':True}),contextlib.redirect_stdout(io.StringIO()):
            measure.campaign(path,self.root/'input/dataset-manifest.json',self.root/name,['empty'],1,0)

    def test_complete_campaign_does_not_claim_failed_query_passed(self):
        self.run_campaign('complete')
        done=json.loads((self.root/'complete/complete.json').read_text())
        self.assertTrue(done['completed'])
        self.assertFalse(done['passed'])
        self.assertEqual(done['failed_queries'],1)
        self.assertEqual(done['verified_deadline_failures'],1)
        rows=[json.loads(line) for line in (self.root/'complete/runs.jsonl').read_text().splitlines()]
        self.assertEqual(rows[0]['status'],'cli_failed')
        self.assertFalse(rows[0]['oracle']['complete_typed_bag'])
        self.assertEqual(rows[1]['status'],'passed')

    def test_bad_complete_result_still_stops_after_declared_deadline(self):
        with self.assertRaisesRegex(RuntimeError,'Correctness'):
            self.run_campaign('bad-result',bad=True)
        rows=[json.loads(line) for line in (self.root/'bad-result/runs.jsonl').read_text().splitlines()]
        self.assertEqual([row['status'] for row in rows],['cli_failed','passed','oracle_or_profile_failed'])
        self.assertFalse((self.root/'bad-result/complete.json').exists())


if __name__=='__main__':
    unittest.main()

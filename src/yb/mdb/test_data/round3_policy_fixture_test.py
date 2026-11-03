# Copyright (c) YugabyteDB, Inc.
"""Cross-check formula oracles using a separate indexed BGP interpreter."""
from collections import Counter, defaultdict
import gzip
import json
from pathlib import Path
import re
import tempfile
import unittest

import round3_policy_fixture as fixture


class FixtureTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.root = Path(cls.temp.name)/'input'
        cls.manifest = fixture.generate(cls.root)
        cls.manifest_path = cls.root/'dataset-manifest.json'

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def test_closed_form_matches_independent_indexed_join(self):
        # The interpreter reads emitted data and joins triple patterns. It does
        # not use fixture.iri(), fixture generation formulas, or MDB output.
        by_predicate = defaultdict(list)
        by_subject = defaultdict(list)
        by_object = defaultdict(list)
        for line in (self.root/'dataset.nt').read_text().splitlines():
            match = re.fullmatch(r'<([^>]+)> <([^>]+)> <([^>]+)> \.', line)
            if match:
                s,p,o = match.groups()
                by_predicate[p].append((s,o))
                by_subject[p,s].append(o)
                by_object[p,o].append(s)
        for case in self.manifest['cases']:
            if not case['performance']:
                continue
            patterns = re.findall(r'(\?\w+) <([^>]+)> (\?\w+)',case['query'])
            bindings = [{}]
            for s,p,o in patterns:
                next_rows = []
                for row in bindings:
                    if s in row:
                        candidates = ((row[s], dest) for dest in by_subject[p,row[s]])
                    elif o in row:
                        candidates = ((src,row[o]) for src in by_object[p,row[o]])
                    else:
                        candidates = by_predicate[p]
                    for src,dest in candidates:
                        if s in row and row[s]!=src or o in row and row[o]!=dest:
                            continue
                        next_rows.append({**row,s:src,o:dest})
                bindings=next_rows
            actual=Counter(tuple(row['?'+col] for col in case['columns']) for row in bindings)
            expected=json.loads(gzip.decompress((self.root/case['expected_file']).read_bytes()))
            self.assertEqual(actual,Counter(tuple(t['value'] for t in row) for row in expected),case['name'])

    def document(self, name):
        _,case,rows=fixture.load_case(self.manifest_path,name)
        return {'head':{'vars':case['columns']},'results':{'bindings':[
            dict(zip(case['columns'],row)) for row in rows]}}

    def check(self, name, document):
        path=Path(self.temp.name)/'result.json'
        path.write_text(json.dumps(document))
        return fixture.validate(self.manifest_path,name,path)

    def test_ordered_pagination_and_multiplicity_rejections(self):
        doc=self.document('ordered_slice')
        self.assertTrue(self.check('ordered_slice',doc)['ordered_sequence'])
        doc['results']['bindings'].reverse()
        with self.assertRaisesRegex(ValueError,'sequence'):
            self.check('ordered_slice',doc)
        doc=self.document('duplicate_values')
        self.check('duplicate_values',doc)
        doc['results']['bindings'].pop()
        with self.assertRaisesRegex(ValueError,'multiset'):
            self.check('duplicate_values',doc)

    def test_typed_identity_and_projection_rejections(self):
        doc=self.document('typed_witnesses')
        self.check('typed_witnesses',doc)
        doc['results']['bindings'][2]['o'].pop('datatype')
        with self.assertRaisesRegex(ValueError,'multiset'):
            self.check('typed_witnesses',doc)
        doc=self.document('typed_witnesses')
        doc['head']['vars'].reverse()
        with self.assertRaisesRegex(ValueError,'Projection'):
            self.check('typed_witnesses',doc)

    def test_empty_and_refuse_overwrite(self):
        self.assertEqual(self.check('empty',self.document('empty'))['rows'],0)
        with self.assertRaisesRegex(ValueError,'overwrite'):
            fixture.generate(self.root)

    def test_regeneration_is_byte_identical(self):
        other=Path(self.temp.name)/'second'
        fixture.generate(other)
        for path in self.root.iterdir():
            self.assertEqual(path.read_bytes(),(other/path.name).read_bytes(),path.name)


if __name__=='__main__':
    unittest.main()

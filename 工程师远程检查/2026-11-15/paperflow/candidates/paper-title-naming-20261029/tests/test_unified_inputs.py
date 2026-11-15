import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from paperflow_prefect.settings import Settings
from paperflow_prefect.records import read_json,write_json
from paperflow_prefect.unified_inputs import receive_inputs
from paperflow_prefect.unified import UnifiedBackend
from test_group_classification import FixtureJudge,readable_fixture
from test_receipt_h21 import handoff


class UnifiedInputTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name).resolve();self.library=self.root/'library';self.library.mkdir()
        self.data=self.root/'data';self.data.mkdir()
        self.settings=Settings(dict(Settings.load().values,execution_enabled=True,library_root=str(self.library),
            data_root=str(self.data),receipt_root=str(self.root/'receipts'),classification_root=str(self.root/'classification'),
            history_roots=[]))
    def classify(self):
        person=self.library/'person';person.mkdir();(person/'论文.docx').write_bytes(b'fixture body')
        with patch('paperflow_prefect.nimble_group.NimbleJudge',return_value=FixtureJudge()), \
             patch('paperflow_prefect.group_sort_cli._read_for_sort',new=readable_fixture), \
             patch('paperflow_prefect.group_sort_cli._title_from_body',return_value=(None,None,None,False)):
            return receive_inputs(self.settings,person_folders=[str(person)],verified_new_groups=[str(person)])
    def test_new_directory_receipt_can_confirm_first_request_without_self_history(self):
        inputs=self.classify();self.assertEqual(len(inputs),1);self.assertIn('receipt_path',inputs[0],inputs)
        backend=UnifiedBackend(self.settings);paper=backend.receive(inputs[0]['receipt_path'])
        backend.confirm_history(paper,paper['body'])
        proof=read_json(backend.body_work(paper)/'first-request.json')
        self.assertFalse(proof['request_history_found'])
        backend.confirm_history(paper,paper['body'])
        self.assertEqual(proof,read_json(backend.body_work(paper)/'first-request.json'))
    def test_bad_input_does_not_prevent_valid_group_receipt(self):
        h=handoff(self.library,self.library/'person');path=self.root/'handoff.json';write_json(path,h)
        rows=receive_inputs(self.settings,handoff_paths=[str(self.root/'missing.json'),str(path)])
        self.assertEqual(len(rows),2);self.assertEqual(sum(bool(r.get('receipt_path')) for r in rows),1)
        self.assertEqual(sum(r.get('status')=='incomplete' for r in rows),1)
    def test_explicit_sidecars_reach_existing_receipt_consumer(self):
        h=handoff(self.library,self.library/'person');path=self.root/'handoff.json';write_json(path,h)
        with patch('paperflow_prefect.intake_handoff.receive_handoff',return_value={'record_path':'receipt','receipt_id':'r','paper_id':'p'}) as receive:
            receive_inputs(self.settings,handoff_paths=[str(path)],handoff_evidence={str(path):['retained-sidecar.json']})
        self.assertEqual(receive.call_args.kwargs['evidence_records'],['retained-sidecar.json'])
    def test_input_task_uses_drain_thread_with_keyword_parameters(self):
        from paperflow_prefect import unified_flows as unified
        @asynccontextmanager
        async def gate(*a,**k):yield
        with patch.object(unified.Settings,'load',return_value=self.settings), \
             patch.object(unified.flows,'async_concurrency',new=gate), \
             patch.object(unified,'receive_inputs',return_value=['accepted']) as receive:
            result=asyncio.run(unified.input_stage.fn({'receipt_paths':['receipt']}))
        self.assertEqual(result,['accepted']);self.assertEqual(receive.call_args.kwargs['receipt_paths'],['receipt'])

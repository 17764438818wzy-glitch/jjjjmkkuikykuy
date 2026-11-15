from contextlib import redirect_stdout
from io import StringIO
import json
from pathlib import Path
import unittest

from test_receipt_h21 import ReceiptFixture, save
from paperflow_prefect.receipt_cli import main


class CliTests(ReceiptFixture):
    def run_cli(self,*args):
        stream=StringIO()
        with redirect_stdout(stream):code=main(list(args))
        return code,json.loads(stream.getvalue())

    def test_cli_receives_only_and_returns_immutable_record_path(self):
        h=self.make(attachments=[('附件/a.zip',b'a')])
        handoff=self.base/'handoff.json';save(handoff,h)
        code,result=self.run_cli('--handoff',str(handoff),'--receipt-root',str(self.base/'cli-ledger'))
        self.assertEqual(code,0)
        self.assertEqual(result['summary'],dict(total=1,received=1,remaining=0))
        r=json.loads(Path(result['groups'][0]['record_path']).read_text(encoding='utf8'))
        self.assertEqual(r['state'],'已接收')
        self.assertFalse(r['items'][0]['history']['allow_new_request'])
        self.assertFalse(r['items'][1]['history']['allow_new_request'])

    def test_cli_reports_pending_group_but_keeps_accepted_body(self):
        h=self.make(attachments=[('附件/a.zip',b'a')])
        Path(h['attachments'][0]['path']).unlink()
        handoff=self.base/'handoff.json';save(handoff,h)
        code,result=self.run_cli('--handoff',str(handoff),'--receipt-root',str(self.base/'cli-ledger'))
        self.assertEqual(code,2)
        self.assertEqual(result['summary'],dict(total=1,received=0,remaining=1))
        record=json.loads(Path(result['groups'][0]['record_path']).read_text(encoding='utf8'))
        self.assertIsNotNone(record['inventory']['body'])
        self.assertEqual(record['inventory']['attachments'],[])

    def test_cli_rejects_original_library_as_output_without_creating_database(self):
        h=self.make();handoff=self.base/'handoff.json';save(handoff,h)
        code,result=self.run_cli('--handoff',str(handoff),'--receipt-root',str(self.library/'unsafe'))
        self.assertEqual(code,2)
        self.assertEqual(result['groups'][0]['state'],'未接收')
        self.assertFalse((self.library/'unsafe').exists())


if __name__=='__main__':unittest.main()

import hashlib
import json
from pathlib import Path
import sqlite3
import sys

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path[:0] = [str(HERE), str(ROOT / 'candidates/postprocess-retry-prefect-20261101')]
from test_folder_tickets import example, State
import folder_tickets


def test_folder_shows_failed_attachment_while_position_waits_for_rag(example):
    failure = {'attachment_id': 'attachment-1', 'status': 'failed',
               'code': 'attachment_format_error', 'message': '附件返回格式错误'}
    raw = json.dumps({'failed_attachments': [failure]}, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    digest = hashlib.sha256(raw.encode()).hexdigest()
    with sqlite3.connect(example.control) as db:
        db.execute('ALTER TABLE postprocessing_results ADD COLUMN claim_id TEXT')
        db.execute('ALTER TABLE postprocessing_results ADD COLUMN stage_receipt_sha256 TEXT')
        db.execute('CREATE TABLE postprocessing_receipts(claim_id TEXT, receipt_json TEXT, receipt_sha256 TEXT)')
        db.execute("UPDATE llm_folder_queue SET state='waiting_postprocessing'")
        db.execute("UPDATE llm_request_admissions SET state='raw_complete'")
        db.execute('INSERT INTO paperflow_postprocess_stage_jobs VALUES(?,?,?,?)',
                   (example.task, example.group, example.post, 'finished'))
        db.execute('INSERT INTO postprocessing_results VALUES(?,?,?,?,?,?)',
                   (example.task, example.group, 'passed', 'passed', 'claim', digest))
        db.execute('INSERT INTO postprocessing_receipts VALUES(?,?,?)', ('claim', raw, digest))
    example.states[example.post] = State('completed')
    row = folder_tickets.observe(example.projector)['folders'][0]
    assert row['stage'] == 'waiting_rag_upload'
    assert row['llm_result'] == 'passed'
    assert row['postprocessing_result'] == 'failed'
    assert row['attachment_failures'] == [failure]
    assert 'attachment-1' in row['postprocessing_detail']

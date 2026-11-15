"""Synthetic downstream checks for body delivery with a failed attachment."""
import sys
from pathlib import Path
import time

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
RAG = ROOT / 'candidates/rag-nas-prefect-20261031'
sys.path[:0] = [str(HERE), str(RAG / 'tests'), str(RAG)]
from test_isolated_group_rag import case
from test_folder_native_client import admission
from group_rag_evidence import read_verified_group
from isolated_group_rag import publish_group


def test_body_proceeds_while_failed_attachment_is_recorded_for_later(case):
    item = case['item']
    failed = [{'attachment_id': item['attachment_id'], 'status': 'failed',
               'source_sha256': item['source_sha256'], 'code': 'attachment_format_error',
               'message': '附件返回格式错误'}]
    plan = dict(case['plan'], attachments_required=False)
    result = publish_group(case['stage'], case['copied']['receipt_path'], case['prepared'],
        case['packet'], plan, case['folder'], [], 'd' * 64, case['raw'], case['journal'],
        lambda *_: admission(True), lambda: {'ready': True, 'ready_valid_until': time.time() + 10},
        case['config'], None, case['tmp_path'] / 'partial', failed_attachments=failed)
    assert result['status'] == 'verified'
    assert result['attachments'] == []
    assert result['binding']['failed_attachments'] == failed
    assert result['publication_status'] == 'partial'
    assert len(case['raw'].native) == 2
    checked, copied = read_verified_group(result['receipt_path'], case['copied']['receipt_path'])
    assert checked['binding']['failed_attachments'] == failed
    assert any(row['file_id'] == item['attachment_id'] for row in copied['files'])

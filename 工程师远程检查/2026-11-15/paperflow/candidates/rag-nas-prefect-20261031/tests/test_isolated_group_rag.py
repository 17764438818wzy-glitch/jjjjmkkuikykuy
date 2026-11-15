import hashlib
import json
from pathlib import Path
import time
from types import SimpleNamespace

import pytest

from test_folder_native_attachment import FakeNativeAttachment, Counter, handoff
from test_folder_native_client import admission
from paperflow.native_delivery import NativeDeliveryPublisher
from preextract_publish import _Budget
from effect_journal import EffectJournal
from isolated_copy import create_test_copy


@pytest.fixture
def case(tmp_path):
    from isolated_group_rag import publish_group
    item = handoff(str(tmp_path))
    body_path = tmp_path / 'body.docx'
    body_path.write_bytes(b'synthetic paper')
    body_sha = hashlib.sha256(body_path.read_bytes()).hexdigest()
    ticket = {'task_id': 'big-task', 'group_id': 'group-1', 'paper_id': 'PF_sample',
              'receipt_id': 'receipt-1', 'attempt_id': 'attempt-1',
              'parent_run_id': 'parent-1', 'task_run_id': 'folder-1'}
    stage = {**ticket, 'claim_id': 'claim', 'postprocess_job_id': 'job',
             'postprocess_receipt_sha256': 'e' * 64, 'upstream_manifest_sha256': 'f' * 64, 'source_generation': 4}
    inventory = {'group_id': ticket['group_id'], 'paper_id': ticket['paper_id'],
                 'receipt_id': ticket['receipt_id'], 'files': [
        {'file_id': 'body', 'kind': 'body', 'relative': 'body.docx', 'path': str(body_path),
         'sha256': body_sha, 'bytes': body_path.stat().st_size},
        {'file_id': item['attachment_id'], 'kind': 'attachment', 'relative': 'code.txt',
         'path': item['source_path'], 'sha256': item['source_sha256'], 'bytes': 8}]}
    copied = create_test_copy(inventory, tmp_path / 'copies')
    task = {'task_id': 'prepared-body', 'source_sha256': body_sha, 'revision': 1}
    packet = {'manifest_sha256': 'b' * 64, 'manifest': {'files': [{'path': 'profile.json', 'sha256': 'd' * 64}]},
              'profile': {'article': {'title': 'Synthetic paper'}},
              'tree': {'nodes': []}, 'assets': [], 'structure_plan': None,
              'chunks': [{'id': 'body-1', 'content': 'Existing body', 'kind': 'body'}],
              'routes': [{'code': '081301', 'paper_dataset_id': 'formal-1', 'image_dataset_id': 'formal-i1'},
                         {'code': '080206', 'paper_dataset_id': 'formal-2', 'image_dataset_id': 'formal-i2'}]}
    plan = {'schema': 'paperflow-isolated-test-route-plan/1', 'run_id': 'a' * 32,
            'manifest_sha256': packet['manifest_sha256'], 'attachments_required': True, 'body_images_required': False,
            'routes': [{'code': '081301', 'paper_dataset_id': 'ds-body-1', 'image_dataset_id': 'ds-main'},
                       {'code': '080206', 'paper_dataset_id': 'ds-body-2', 'image_dataset_id': 'ds-ref'}]}
    config = SimpleNamespace(native_pipeline_id='pipeline', native_ingest_timeout=5)
    check = NativeDeliveryPublisher(None, lambda _: None, config)
    records = [{'role': 'body', 'record_id': row[0], 'model_tokens': 1, 'cl100k_tokens': 1,
                'input_sha256': hashlib.sha256(check.render(task, packet, row).encode()).hexdigest()}
               for row in check.records(packet)['body']]
    prepared = {'task': task, 'manifest_sha256': packet['manifest_sha256'],
                'validation': {'embedding': {'admission_verified': True,
                 'cl_limit': 2038, 'model_budget': 2000, 'records': records}}}
    raw = FakeNativeAttachment()
    journal = EffectJournal(tmp_path / 'effects.sqlite3')
    journal.initialize()
    folder = {'schema': 'paperflow-isolated-files-folder/1', 'status': 'verified', 'folder_id': 'folder-main',
              'binding': {'run_id': plan['run_id'], 'journal_id': journal.identity()}}
    def execute():
        return publish_group(stage, copied['receipt_path'], prepared, packet, plan, folder, [item],
            'd' * 64, raw, journal, lambda *_: admission(True),
            lambda: {'ready': True, 'ready_valid_until': time.time() + 10},
            config, _Budget(Counter()), tmp_path / 'group-work')
    return locals()


def test_whole_group_waits_for_body_and_all_attachment_targets(case):
    result = case['execute']()
    assert result['status'] == 'verified'
    assert result['group_count'] == 1
    assert len(case['raw'].native) == 4
    assert len(result['attachments']) == 1
    assert json.loads(Path(result['receipt_path']).read_bytes())['status'] == 'verified'
    assert all(Path(row['path']).exists() for row in case['copied']['files'])


def test_group_cannot_ignore_a_delivered_attachment(case):
    from isolated_group_rag import publish_group
    with pytest.raises(ValueError, match='attachments'):
        publish_group(case['stage'], case['copied']['receipt_path'], case['prepared'], case['packet'],
            case['plan'], case['folder'], [], 'd' * 64, case['raw'], case['journal'],
            lambda *_: admission(True), lambda: {'ready': True, 'ready_valid_until': time.time() + 10},
            case['config'], _Budget(Counter()), case['tmp_path'] / 'other-work')
    assert case['raw'].native == []


def test_group_resume_reads_back_instead_of_resending(case):
    case['execute']()
    case['execute']()
    assert len(case['raw'].native) == 4


def test_attachment_original_is_uploaded_from_created_test_copy(case):
    Path(case['item']['source_path']).write_bytes(b'modified')
    assert case['execute']()['status'] == 'verified'
    assert next(iter(case['raw'].files.values()))['_bytes'] == b'print(1)'


def test_attachment_profile_identity_must_come_from_sealed_manifest(case):
    case['packet']['manifest']['files'][0]['sha256'] = 'e' * 64
    with pytest.raises(ValueError, match='profile'):
        case['execute']()
    assert case['raw'].native == []


def test_group_packet_must_match_the_bound_postprocessing_manifest(case):
    case['prepared']['manifest_sha256'] = 'f' * 64
    with pytest.raises(ValueError, match='postprocessing'):
        case['execute']()
    assert case['raw'].native == []


@pytest.mark.parametrize('kind', ['body', 'attachment'])
def test_group_receipt_rejects_evidence_from_a_different_journal(case, kind):
    from group_rag_evidence import read_verified_group
    from paperflow_prefect.records import sha_file
    result = case['execute']()
    rag_path = Path(result['receipt_path'])
    rag = json.loads(rag_path.read_bytes())
    path = Path(rag['body_receipt_path'] if kind == 'body' else rag['attachments'][0]['receipt_path'])
    evidence = json.loads(path.read_bytes())
    if kind == 'body':
        evidence['binding']['journal_id'] = 'another-journal'
    else:
        evidence['journal_id'] = 'another-journal'
    path.write_text(json.dumps(evidence), encoding='utf8')
    if kind == 'body':
        rag['body_receipt_sha256'] = sha_file(path)
    else:
        rag['attachments'][0]['receipt_sha256'] = sha_file(path)
    rag_path.write_text(json.dumps(rag), encoding='utf8')
    with pytest.raises(ValueError, match='RAG'):
        read_verified_group(rag_path, case['copied']['receipt_path'])

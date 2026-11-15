import base64
import hashlib
import io
import json
from pathlib import Path
import sqlite3
import sys
import time
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest

WORKSPACE = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(WORKSPACE / 'prefect-paperflow'))
from paperflow_prefect.legacy import bind
from paperflow_prefect.settings import Settings
bind(Settings.load())
from paperflow.native_delivery import NativeDeliveryPublisher

from effect_journal import EffectJournal, EffectUnknown
from guarded_rag_client import _json, _sha
from test_folder_native_client import admission


class FakeNative:
    base_url = 'http://synthetic'
    api_key = 'synthetic'
    opener = None

    def __init__(self):
        self.docs, self.chunks = {}, {}
        self.images = {}
        self.opener = self
        self.ingests = []
        self.finish_on_poll = None
        self.polls = 0
        self.lose_retrieval = False
        self.drop_after_enable = False

    def open(self, request, timeout):
        response = io.BytesIO(self.images[request.full_url.rsplit('/', 1)[-1]])
        response.headers = {'Content-Type': 'image/png'}
        return response

    def upload_bytes(self, dataset, name, content):
        doc_id = f'doc-{len(self.docs) + 1}'
        self.docs[doc_id] = {'id': doc_id, 'dataset': dataset, 'name': name, 'status': '0',
                             'run': '0', 'progress': 0, 'meta_fields': {}, 'raw': content}
        self.chunks[doc_id] = []
        return doc_id

    def request(self, method, endpoint, body=None):
        if endpoint == '/documents/ingest':
            did = body['doc_ids'][0]
            doc = self.docs[did]
            self.ingests.append(doc['dataset'])
            payload = json.loads(doc['raw'])
            assert payload['schema'] == 'pf-native-bulk-proof/1'
            self.chunks[did] = [{'id': f'{did}-{n}', 'document_id': did,
                                 'content': row['text'], 'available': False}
                                for n, row in enumerate(payload['chunks'])]
            doc.update(run='3', progress=1)
            return {'accepted': True}
        if endpoint == '/retrieval':
            if self.lose_retrieval:
                return {'chunks': []}
            return {'chunks': list(self.chunks[body['document_ids'][0]])}
        url = urlsplit(endpoint)
        parts = url.path.strip('/').split('/')
        if len(parts) == 3 and parts[2] == 'documents':
            query = parse_qs(url.query)
            if query.get('id') == [self.finish_on_poll]:
                self.polls += 1
                if self.polls == 2:
                    self.docs[self.finish_on_poll].update(run='3', progress=1)
            docs = [d for d in self.docs.values() if d['dataset'] == parts[1]]
            for key in ('id', 'name'):
                if key in query:
                    docs = [d for d in docs if d[key] == query[key][0]]
            return {'docs': [{k: v for k, v in d.items() if k != 'raw'} for d in docs]}
        did = parts[3]
        if method == 'POST' and parts[-1] == 'chunks':
            image_id = f'image-{len(self.images) + 1}'
            self.images[image_id] = base64.b64decode(body['image_base64'])
            chunk = {'id': f'{did}-{len(self.chunks[did])}', 'image_id': image_id,
                     'document_id': did, 'content': body['content'], 'available': False}
            self.chunks[did].append(chunk)
            return {'chunk': chunk}
        if method == 'PATCH' and len(parts) == 4:
            self.docs[did].update(body)
            if 'enabled' in body:
                self.docs[did]['status'] = str(body['enabled'])
                for chunk in self.chunks[did]:
                    chunk['available'] = bool(body['enabled'])
                if self.drop_after_enable and body['enabled'] == 1:
                    self.chunks[did].pop()
            return {'id': did}
        if method == 'GET' and parts[-1] == 'chunks':
            rows = self.chunks[did]
            return {'chunks': rows, 'total': len(rows)}
        raise AssertionError((method, endpoint))


@pytest.fixture
def case(tmp_path):
    from folder_native_body import publish_body
    task = {'task_id': 'postprocess-task', 'source_sha256': 'a' * 64, 'revision': 1}
    packet = {'manifest_sha256': 'b' * 64, 'profile': {'article': {'title': 'Synthetic paper'}},
              'tree': {'nodes': []}, 'assets': [], 'structure_plan': None,
              'chunks': [{'id': 'body-1', 'content': 'Existing processed text', 'kind': 'body'}],
              'routes': [{'code': '100001', 'paper_dataset_id': 'formal-1'},
                         {'code': '100002', 'paper_dataset_id': 'formal-2'}]}
    plan = {'schema': 'paperflow-isolated-test-route-plan/1', 'run_id': 'c' * 32,
            'manifest_sha256': packet['manifest_sha256'], 'body_images_required': False,
            'routes': [{'code': '100001', 'paper_dataset_id': 'test-1'},
                       {'code': '100002', 'paper_dataset_id': 'test-2'}]}
    config = SimpleNamespace(native_pipeline_id='pipeline', native_ingest_timeout=5)
    check = NativeDeliveryPublisher(None, lambda _: None, config)
    rows = [{'role': 'body', 'record_id': row[0], 'model_tokens': 1, 'cl100k_tokens': 1,
             'input_sha256': hashlib.sha256(check.render(task, packet, row).encode()).hexdigest()}
            for row in check.records(packet)['body']]
    prepared = {'task': task, 'validation': {'embedding': {'admission_verified': True,
                'cl_limit': 2038, 'model_budget': 2000, 'records': rows}}}
    raw = FakeNative()
    journal = EffectJournal(tmp_path / 'effects.sqlite3')
    journal.initialize()
    receipt = tmp_path / 'body.json'
    def execute():
        return publish_body(prepared, packet, plan, raw, journal,
                            lambda *_: admission(True),
                            lambda: {'ready': True, 'ready_valid_until': time.time() + 10},
                            config, receipt)
    return locals()


def test_all_specialties_use_native_isolated_targets_and_read_back(case):
    result = case['execute']()
    assert case['raw'].ingests == ['test-1', 'test-2']
    assert result['body_verified'] is True
    assert result['group_rag_complete'] is False
    assert all(row['verified'] and row['retrieval_verified'] for row in result['targets'].values())
    assert json.loads(case['receipt'].read_text(encoding='utf8'))['body_verified'] is True


def test_completed_body_resume_does_not_reingest(case):
    case['execute']()
    case['execute']()
    assert case['raw'].ingests == ['test-1', 'test-2']


def test_native_health_recovery_reuses_the_already_uploaded_document(case):
    from folder_native_body import publish_body
    from folder_native_client import NativeNotReady
    ready = False
    def execute():
        return publish_body(case['prepared'], case['packet'], case['plan'], case['raw'], case['journal'],
                            lambda *_: admission(True),
                            lambda: {'ready': ready, 'ready_valid_until': time.time() + 10},
                            case['config'], case['receipt'])
    with pytest.raises(NativeNotReady):
        execute()
    assert len(case['raw'].docs) == 1 and case['raw'].ingests == []
    ready = True
    assert execute()['body_verified'] is True
    assert len(case['raw'].docs) == 2
    assert case['raw'].ingests == ['test-1', 'test-2']


def test_changed_seal_or_formal_target_stops_before_upload(case):
    case['plan']['routes'][0]['paper_dataset_id'] = 'formal-2'
    with pytest.raises(ValueError, match='target'):
        case['execute']()
    assert case['raw'].docs == {}


def test_unqualified_existing_text_is_not_reprocessed_or_sent(case):
    case['prepared']['validation']['embedding']['records'][0]['model_tokens'] = 3000
    with pytest.raises(ValueError, match='qualified'):
        case['execute']()
    assert case['raw'].docs == {}


def test_started_native_job_is_polled_on_resume_without_resubmitting(case):
    result = case['execute']()
    target = result['targets']['100001']
    target['verified'] = False
    target['retrieval_verified'] = False
    target['body']['native_batch']['state'] = 'intent'
    case['receipt'].write_text(json.dumps(result), encoding='utf8')
    did = target['body']['document_id']
    case['raw'].docs[did].update(run='1', progress=0, status='0')
    for chunk in case['raw'].chunks[did]:
        chunk['available'] = False
    enable = {'method': 'PATCH', 'endpoint': f'/datasets/test-1/documents/{did}', 'body': {'enabled': 1}}
    with sqlite3.connect(case['journal'].path) as db:
        db.execute('DELETE FROM remote_effects WHERE effect_id=?', (_sha(_json(enable)),))
    case['raw'].finish_on_poll = did
    assert case['execute']()['body_verified'] is True
    assert case['raw'].ingests == ['test-1', 'test-2']


def test_unresolved_native_response_never_becomes_group_success(case):
    result = case['execute']()
    target = result['targets']['100001']
    target['verified'] = False
    target['retrieval_verified'] = False
    target['body']['native_batch']['state'] = 'intent'
    case['receipt'].write_text(json.dumps(result), encoding='utf8')
    with sqlite3.connect(case['journal'].path) as db:
        db.execute("UPDATE remote_effects SET state='intent',response_json=NULL WHERE response_json LIKE '%accepted%'")
    with pytest.raises(EffectUnknown):
        case['execute']()
    assert case['raw'].ingests == ['test-1', 'test-2']


def test_failed_reverification_does_not_leave_body_complete(case):
    case['execute']()
    case['raw'].lose_retrieval = True
    with pytest.raises(ConnectionError):
        case['execute']()
    assert json.loads(case['receipt'].read_text(encoding='utf8'))['body_verified'] is False


def test_body_chunk_lost_during_enable_does_not_count_as_complete(case):
    case['raw'].drop_after_enable = True
    with pytest.raises(ConnectionError, match='readback'):
        case['execute']()
    assert json.loads(case['receipt'].read_text(encoding='utf8'))['body_verified'] is False


def test_body_no_longer_terminal_is_not_reverified(case):
    result = case['execute']()
    did = result['targets']['100001']['body']['document_id']
    case['raw'].docs[did].update(run='1', progress=0)
    with pytest.raises(EffectUnknown):
        case['execute']()


def test_body_receipt_cannot_resume_with_another_empty_journal(case):
    from folder_native_body import publish_body
    case['execute']()
    other = EffectJournal(case['tmp_path'] / 'other.sqlite3')
    other.initialize()
    with pytest.raises(ValueError, match='receipt'):
        publish_body(case['prepared'], case['packet'], case['plan'], case['raw'], other,
            lambda *_: admission(True),
            lambda: {'ready': True, 'ready_valid_until': time.time() + 10},
            case['config'], case['receipt'])
    assert case['raw'].ingests == ['test-1', 'test-2']


def test_body_images_are_stored_and_read_back_in_each_test_specialty(case):
    picture = b'synthetic image bytes'
    (case['tmp_path'] / 'image.png').write_bytes(picture)
    case['packet']['root'] = str(case['tmp_path'])
    case['packet']['assets'] = [{'id': 'picture-1', 'kind': 'image', 'storage': 'image',
                                 'file': 'image.png', 'sha256': hashlib.sha256(picture).hexdigest()}]
    for index, route in enumerate(case['packet']['routes'], 1):
        route['image_dataset_id'] = f'formal-image-{index}'
        case['plan']['routes'][index - 1]['image_dataset_id'] = f'test-image-{index}'
    case['plan']['body_images_required'] = True
    result = case['execute']()
    assert result['body_verified'] is True
    assert len(case['raw'].images) == 2
    assert set(case['raw'].images.values()) == {picture}
    for index, target in enumerate(result['targets'].values(), 1):
        assert target['images']['dataset_id'] == f'test-image-{index}'
        assert target['images']['chunks']['asset_picture-1']['image_id']


def test_image_byte_mismatch_cannot_complete_the_group(case):
    picture = b'original image'
    (case['tmp_path'] / 'image.png').write_bytes(picture)
    case['packet']['root'] = str(case['tmp_path'])
    case['packet']['assets'] = [{'id': 'picture-1', 'kind': 'image', 'storage': 'image',
                                 'file': 'image.png', 'sha256': hashlib.sha256(b'different').hexdigest()}]
    for index, route in enumerate(case['packet']['routes'], 1):
        route['image_dataset_id'] = f'formal-image-{index}'
        case['plan']['routes'][index - 1]['image_dataset_id'] = f'test-image-{index}'
    case['plan']['body_images_required'] = True
    with pytest.raises(Exception, match='image bytes readback mismatch'):
        case['execute']()
    assert json.loads(case['receipt'].read_text(encoding='utf8'))['body_verified'] is False

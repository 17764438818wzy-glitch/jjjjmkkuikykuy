import json
import io
from pathlib import Path
import sys
import time

import pytest

WORKSPACE = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(WORKSPACE / 'prefect-paperflow'), str(WORKSPACE / 'prefect-paperflow/tests')]
from paperflow_prefect.legacy import bind
from paperflow_prefect.settings import Settings
bind(Settings.load())
from preextract_publish import _Budget
from test_attachment_ingest import handoff, edit_card
from test_attachment_publish import Counter, FakeStore, route
from test_folder_native_client import admission
from effect_journal import EffectJournal, EffectUnknown


class FakeNativeAttachment(FakeStore):
    api_key = 'synthetic'
    opener = None

    def __init__(self):
        super().__init__()
        self.native = []
        self.lose_native = False
        self.lose_retrieval = False
        self.drop_after_enable = False

    def request(self, method, endpoint, body=None):
        if endpoint == '/retrieval' and self.lose_retrieval:
            return {'chunks': []}
        if endpoint == '/documents/ingest':
            did = body['doc_ids'][0]
            doc = self.docs[did]
            payload = json.loads(doc['_bytes'])
            assert payload['schema'] == 'pf-native-bulk-proof/1'
            self.native.append(did)
            self.chunks[did] = [{'id': f'{did}-{n}', 'document_id': did, 'content': chunk['text'],
                                 'available': False} for n, chunk in enumerate(payload['chunks'])]
            doc.update(run='3', progress=1)
            if self.lose_native:
                raise TimeoutError('response lost after acceptance')
            return {'accepted': True}
        result = super().request(method, endpoint, body)
        if method == 'PATCH' and '/documents/' in endpoint:
            did = endpoint.split('/')[4]
            if 'enabled' in body:
                for chunk in self.chunks[did]:
                    chunk['available'] = bool(body['enabled'])
                if (self.drop_after_enable and body['enabled'] == 1
                        and self.docs[did]['meta_fields'].get('paperflow_role') == 'attachment-card'):
                    self.chunks[did].pop()
            return {'id': did}
        return result


@pytest.fixture
def case(tmp_path):
    from folder_native_attachment import publish_attachment
    item = handoff(str(tmp_path), archive=True, total=80,
                   rows=[('code/file'+str(i)+'.py', 'file', '8') for i in range(80)])
    raw = FakeNativeAttachment()
    journal = EffectJournal(tmp_path / 'effects.sqlite3')
    journal.initialize()
    isolated_route = route()
    def execute():
        return publish_attachment(item, isolated_route, raw, journal,
            lambda *_: admission(True), lambda: {'ready': True, 'ready_valid_until': time.time() + 10},
            'pipeline', _Budget(Counter()), tmp_path / 'attachment-receipt.json')
    return locals()


def test_original_once_and_each_specialty_uses_native_pipeline(case):
    result = case['execute']()
    assert result['status'] == 'verified'
    assert len(case['raw'].files) == 1
    assert len(case['raw'].native) == 2
    assert not any(entry[0] == 'chunk' for entry in case['raw'].sent)
    primary = result['publications'][case['item']['md_sha256']]['081301']
    chunks = case['raw'].chunks[primary['document_id']]
    reassembled = ''.join(row['content'].split('\n', 1)[1] for row in chunks)
    assert len(chunks) > 1
    assert reassembled.startswith(Path(case['item']['md_path']).read_bytes().decode('utf8'))
    assert all(row['available'] for row in chunks)
    assert result['raw']['status'] == 'verified'


def test_verified_attachment_resume_performs_no_further_writes(case):
    case['execute']()
    sent = list(case['raw'].sent)
    native = list(case['raw'].native)
    case['execute']()
    assert case['raw'].sent == sent
    assert case['raw'].native == native


def test_unknown_native_response_is_not_resent(case):
    case['raw'].lose_native = True
    with pytest.raises(TimeoutError):
        case['execute']()
    with pytest.raises(EffectUnknown):
        case['execute']()
    assert len(case['raw'].native) == 1


def test_paused_parent_sends_no_attachment_writes(case):
    from folder_native_attachment import publish_attachment
    with pytest.raises(InterruptedError):
        publish_attachment(case['item'], case['isolated_route'], case['raw'], case['journal'],
            lambda *_: admission(False), lambda: {'ready': True, 'ready_valid_until': time.time() + 10},
            'pipeline', _Budget(Counter()), case['tmp_path'] / 'paused.json')
    assert case['raw'].sent == []
    result = publish_attachment(case['item'], case['isolated_route'], case['raw'], case['journal'],
        lambda *_: admission(True), lambda: {'ready': True, 'ready_valid_until': time.time() + 10},
        'pipeline', _Budget(Counter()), case['tmp_path'] / 'paused.json')
    assert result['status'] == 'verified'


def test_new_attachment_version_cannot_bypass_unknown_old_native_request(case):
    case['raw'].lose_native = True
    with pytest.raises(TimeoutError):
        case['execute']()
    edit_card(case['item'], '包含一段程序。', '更新后的说明。')
    case['raw'].lose_native = False
    with pytest.raises(RuntimeError, match='旧版'):
        case['execute']()
    assert len(case['raw'].native) == 1


def test_failed_reverification_does_not_leave_complete_attachment_receipt(case):
    from paperflow_prefect.attachment_publish import PendingDelivery
    case['execute']()
    case['raw'].lose_retrieval = True
    with pytest.raises(PendingDelivery):
        case['execute']()
    state = json.loads((case['tmp_path'] / 'attachment-receipt.json').read_bytes())
    assert state['status'] == 'pending_verification'
    assert state['publications'][case['item']['md_sha256']]['081301']['retrieval_verified'] is False


def test_string_zero_is_not_an_available_attachment_chunk(case):
    result = case['execute']()
    did = result['publications'][case['item']['md_sha256']]['081301']['document_id']
    case['raw'].chunks[did][0]['available'] = '0'
    with pytest.raises(ValueError, match='enabled and readable'):
        case['execute']()
    state = json.loads((case['tmp_path'] / 'attachment-receipt.json').read_bytes())
    assert state['status'] == 'pending_verification'


def test_existing_http_client_without_download_bytes_is_supported(case):
    from folder_native_attachment import publish_attachment
    from urllib.parse import urlsplit
    class Opener:
        def open(self, request, timeout):
            did = urlsplit(request.full_url).path.split('/')[-1]
            return io.BytesIO(case['raw'].docs[did]['_bytes'])
    class ActualShape:
        opener = Opener()
        def __getattr__(self, name):
            if name == 'download_bytes':
                raise AttributeError(name)
            return getattr(case['raw'], name)
    result = publish_attachment(case['item'], case['isolated_route'], ActualShape(), case['journal'],
        lambda *_: admission(True), lambda: {'ready': True, 'ready_valid_until': time.time() + 10},
        'pipeline', _Budget(Counter()), case['tmp_path'] / 'actual-shape.json')
    assert result['status'] == 'verified'


def test_attachment_chunk_lost_during_enable_does_not_count_as_complete(case):
    case['raw'].drop_after_enable = True
    with pytest.raises(ValueError, match='enabled and readable'):
        case['execute']()
    state = json.loads((case['tmp_path'] / 'attachment-receipt.json').read_bytes())
    assert state['status'] == 'pending_verification'


def test_attachment_no_longer_terminal_is_not_reverified(case):
    result = case['execute']()
    did = result['publications'][case['item']['md_sha256']]['081301']['document_id']
    case['raw'].docs[did].update(run='1', progress=0)
    with pytest.raises(EffectUnknown):
        case['execute']()


def test_attachment_receipt_cannot_resume_against_another_empty_journal(case):
    from folder_native_attachment import publish_attachment
    case['raw'].lose_native = True
    with pytest.raises(TimeoutError):
        case['execute']()
    other = EffectJournal(case['tmp_path'] / 'other.sqlite3')
    other.initialize()
    with pytest.raises(ValueError, match='journal'):
        publish_attachment(case['item'], case['isolated_route'], case['raw'], other,
            lambda *_: admission(True), lambda: {'ready': True, 'ready_valid_until': time.time() + 10},
            'pipeline', _Budget(Counter()), case['tmp_path'] / 'attachment-receipt.json')
    assert len(case['raw'].native) == 1

"""The manual test transport stays inside its targets and records unknown sends."""
import pytest

import sample_native_upload as sample
from effect_journal import EffectJournal, EffectUnknown


class FakeRag:
    base_url = 'http://synthetic'
    api_key = 'synthetic'
    opener = None

    def __init__(self):
        self.uploads = 0

    def request(self, method, endpoint, body=None):
        if method == 'GET':
            return {'docs': [{'run': '3', 'progress': 1}]}
        return {'accepted': True}

    def upload_bytes(self, dataset, name, content):
        self.uploads += 1
        raise TimeoutError('synthetic timeout after send')


@pytest.fixture
def client(tmp_path):
    raw = FakeRag()
    journal = EffectJournal(tmp_path/'effects.sqlite3')
    journal.initialize()
    return sample.TestClient(raw, journal, {'test-dataset'}, 'pipeline'), raw


def test_wrong_dataset_is_not_a_write_target(client):
    transport, raw = client
    with pytest.raises(ValueError, match='test dataset'):
        transport.upload_bytes('formal-dataset', 'sample', b'synthetic')
    with pytest.raises(ValueError, match='outside'):
        transport.request('PATCH', '/datasets/formal-dataset/documents/1', {'enabled': 1})
    assert raw.uploads == 0


def test_unknown_upload_is_recorded_and_not_resent(client):
    transport, raw = client
    with pytest.raises(TimeoutError):
        transport.upload_bytes('test-dataset', 'sample', b'synthetic')
    with pytest.raises(EffectUnknown):
        transport.upload_bytes('test-dataset', 'sample', b'synthetic')
    assert raw.uploads == 1


def test_native_ingestion_uses_selected_pipeline(client):
    transport, _ = client
    result = transport.ingest('test-dataset', 'document', 'pipeline', timeout=1)
    assert result['run'] == '3'

from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import threading

import pytest

from effect_journal import EffectJournal, EffectUnknown
from isolated_targets import ensure_test_targets


RUN = 'a' * 32
FORMAL = 'b' * 32


class Permit:
    def check_current(self): pass
    def start_transport(self, mark): mark()


@contextmanager
def admit(*_):
    yield Permit()


class Client:
    def __init__(self):
        self.rows = [{'id': FORMAL, 'name': 'formal', 'permission': 'me', 'embedding_model': 'model',
                      'tenant_id': 'tenant', 'created_by': 'creator'}]
        self.calls = 0
        self.timeout = False

    def datasets(self):
        return list(self.rows)

    def request(self, method, endpoint, body):
        assert (method, endpoint) == ('POST', '/datasets')
        self.calls += 1
        if self.timeout:
            raise TimeoutError('outcome unknown')
        row = {'id': ('c' if self.calls == 1 else 'd') * 32, **body, 'tenant_id': 'tenant', 'created_by': 'creator',
               'document_count': 0, 'chunk_count': 0}
        self.rows.append(row)
        return row


@pytest.fixture
def inputs(tmp_path):
    catalog = {'majors': [{'code': '080901', 'paper_dataset_id': FORMAL}]}
    routes = [{'code': '080901', 'paper_dataset_id': FORMAL}]
    journal = EffectJournal(tmp_path / 'effects.sqlite3')
    journal.initialize()
    return Client(), catalog, routes, journal


def test_same_specialty_reuses_one_recorded_test_library(tmp_path, inputs):
    client, catalog, routes, journal = inputs
    first = ensure_test_targets(routes, False, False, RUN, catalog, client, journal, admit, tmp_path)
    second = ensure_test_targets(routes, False, False, RUN, catalog, client, journal, admit, tmp_path)
    assert first == second == {'080901': {'paper_dataset_id': 'c' * 32}}
    assert client.calls == 1
    assert (tmp_path / 'rag-dataset-080901.json').is_file()


def test_unknown_create_is_not_repeated(tmp_path, inputs):
    client, catalog, routes, journal = inputs
    client.timeout = True
    with pytest.raises(TimeoutError):
        ensure_test_targets(routes, False, False, RUN, catalog, client, journal, admit, tmp_path)
    with pytest.raises(EffectUnknown):
        ensure_test_targets(routes, False, False, RUN, catalog, client, journal, admit, tmp_path)
    assert client.calls == 1


def test_unrecorded_matching_name_is_not_adopted(tmp_path, inputs):
    client, catalog, routes, journal = inputs
    client.rows.append({'id': 'd' * 32, 'name': f'PaperFlow SELFTEST {RUN} 080901 paper'})
    with pytest.raises(ValueError, match='unrecorded'):
        ensure_test_targets(routes, False, False, RUN, catalog, client, journal, admit, tmp_path)
    assert client.calls == 0


def test_created_library_is_verified_before_saving_receipt(tmp_path, inputs):
    client, catalog, routes, journal = inputs
    request = client.request
    def create_with_wrong_permission(*args):
        row = request(*args)
        row['permission'] = 'team'
        return row
    client.request = create_with_wrong_permission
    with pytest.raises(ValueError, match='isolation'):
        ensure_test_targets(routes, False, False, RUN, catalog, client, journal, admit, tmp_path)
    assert not (tmp_path / 'rag-dataset-080901.json').exists()


def test_existing_receipt_must_match_the_original_creation_result(tmp_path, inputs):
    client, catalog, routes, journal = inputs
    ensure_test_targets(routes, False, False, RUN, catalog, client, journal, admit, tmp_path)
    path = tmp_path / 'rag-dataset-080901.json'
    receipt = json.loads(path.read_text(encoding='utf8'))
    receipt['dataset_id'] = 'd' * 32
    path.write_text(json.dumps(receipt), encoding='utf8')
    client.rows[-1]['id'] = 'd' * 32
    with pytest.raises(ValueError, match='creation'):
        ensure_test_targets(routes, False, False, RUN, catalog, client, journal, admit, tmp_path)
    assert client.calls == 1


def test_existing_receipt_without_creation_journal_is_not_adopted(tmp_path, inputs):
    client, catalog, routes, journal = inputs
    ensure_test_targets(routes, False, False, RUN, catalog, client, journal, admit, tmp_path)
    other = EffectJournal(tmp_path / 'missing-history.sqlite3')
    other.initialize()
    with pytest.raises(EffectUnknown):
        ensure_test_targets(routes, False, False, RUN, catalog, client, other, admit, tmp_path)
    assert client.calls == 1


def test_attachment_requires_its_own_isolated_media_target(tmp_path, inputs):
    client, catalog, routes, journal = inputs
    media = {'id': 'e' * 32, 'name': 'formal media', 'permission': 'me', 'embedding_model': 'model',
             'tenant_id': 'tenant', 'created_by': 'creator'}
    client.rows.append(media)
    catalog['majors'][0]['image_dataset_id'] = media['id']
    routes[0]['image_dataset_id'] = media['id']
    result = ensure_test_targets(routes, False, True, RUN, catalog, client, journal, admit, tmp_path)
    assert result == {'080901': {'paper_dataset_id': 'c' * 32, 'image_dataset_id': 'd' * 32}}
    assert client.calls == 2


def test_missing_batch_receipt_directory_cannot_create_library(tmp_path, inputs):
    client, catalog, routes, journal = inputs
    with pytest.raises(FileNotFoundError):
        ensure_test_targets(routes, False, False, RUN, catalog, client, journal, admit, tmp_path / 'absent')
    assert client.calls == 0


def test_only_identical_test_library_creation_is_shared_between_groups(tmp_path, inputs):
    from isolated_targets import TargetCreations
    client, catalog, routes, journal = inputs
    second_formal = 'e' * 32
    client.rows.append({'id': second_formal, 'name': 'formal-2', 'permission': 'me',
                        'embedding_model': 'model', 'tenant_id': 'tenant', 'created_by': 'creator'})
    catalog['majors'].append({'code': '080902', 'paper_dataset_id': second_formal})
    entered, release = threading.Event(), threading.Event()
    request = client.request
    def blocked(method, endpoint, body):
        if '080901' in body['name']:
            entered.set()
            assert release.wait(10)
        return request(method, endpoint, body)
    client.request = blocked
    coordinate = TargetCreations()
    def prepare(selected):
        return ensure_test_targets(selected, False, False, RUN, catalog, client, journal, admit, tmp_path,
                                   coordinate=coordinate)
    with ThreadPoolExecutor(max_workers=3) as pool:
        first = pool.submit(prepare, routes)
        assert entered.wait(5)
        shared = pool.submit(prepare, routes)
        other = pool.submit(prepare, [{'code': '080902', 'paper_dataset_id': second_formal}])
        assert other.result(timeout=5)['080902']['paper_dataset_id']
        assert client.calls == 1
        release.set()
        assert first.result(timeout=5) == shared.result(timeout=5)
    assert client.calls == 2


def test_shared_unknown_library_creation_is_not_repeated(tmp_path, inputs):
    from isolated_targets import TargetCreations
    client, catalog, routes, journal = inputs
    entered, release = threading.Event(), threading.Event()
    request = client.request
    client.timeout = True
    def blocked(*args):
        entered.set()
        assert release.wait(5)
        return request(*args)
    client.request = blocked
    coordinate = TargetCreations()
    def prepare():
        return ensure_test_targets(routes, False, False, RUN, catalog, client, journal, admit, tmp_path,
                                   coordinate=coordinate)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(prepare)
        assert entered.wait(5)
        shared = pool.submit(prepare)
        release.set()
        with pytest.raises(TimeoutError):
            first.result(timeout=5)
        with pytest.raises((TimeoutError, EffectUnknown)):
            shared.result(timeout=5)
    with pytest.raises(EffectUnknown):
        prepare()
    assert client.calls == 1

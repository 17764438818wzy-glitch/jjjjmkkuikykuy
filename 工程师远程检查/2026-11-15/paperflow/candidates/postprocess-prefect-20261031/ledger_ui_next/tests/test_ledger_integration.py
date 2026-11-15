"""Wire the real candidate projector to the isolated API using only synthetic rows."""
import functools
import importlib.util
from pathlib import Path
import time

from fastapi.testclient import TestClient


HERE = Path(__file__).resolve().parents[1]
ROOT = HERE.parent
BASE = '/api/v1/local-intake/preprocessing/summary'


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def poll(client, predicate, params):
    limit = time.monotonic() + 3
    while time.monotonic() < limit:
        response = client.get(BASE, params=params)
        assert response.status_code == 200
        value = response.json()['postprocessing_registered_ledger']
        if predicate(value):
            return value
        time.sleep(.01)
    raise AssertionError('synthetic ledger projection did not complete')


def test_api_uses_one_snapshot_and_rejects_later_page_after_a_change(tmp_path):
    fixtures = load('synthetic_ledger_fixtures', ROOT / 'ledger_projection_next/tests/test_ledger_projection.py')
    projector = fixtures.projection
    api = load('ledger_ui_api_integration', HERE / 'waiting_api.py')
    db = fixtures.db.__wrapped__(tmp_path)
    fixtures.add_folder(db, 1, job='finished', outcome=('passed', 'passed'), claim='waiting_rag')
    fixtures.add_folder(db, 2, job='finished', outcome=('passed', 'passed'), claim='waiting_rag')

    class Queue:
        def snapshot(self, **_):
            return {'receiver_status': 'unknown', 'waiting_available': False}

    projection = functools.partial(projector.snapshot, db.path, component_id_fn=fixtures.upstream.component_id)
    with TestClient(api.create_app(Queue(), registered_projection=projection)) as client:
        first_params = {'postprocessing_result_filter': 'passed', 'postprocessing_result_page_size': 1}
        first = poll(client, lambda value: value['available'], first_params)
        assert first['results']['counts']['passed'] == 2
        assert len(first['results']['list']['items']) == 1
        assert first['results']['list']['has_next'] is True
        assert first['stage']['counts']['postprocessing_in_progress'] is None
        fixtures.add_folder(db, 3)
        later_params = {**first_params, 'postprocessing_result_page': 2,
                        'postprocessing_result_revision': first['revision']}
        changed = poll(client, lambda value: value.get('reason') == 'snapshot_changed', later_params)
        assert changed['available'] is False
        assert changed['results']['counts']['passed'] is None

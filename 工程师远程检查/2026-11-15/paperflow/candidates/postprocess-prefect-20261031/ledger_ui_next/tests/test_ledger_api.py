import importlib.util
from pathlib import Path
from threading import Event
import time

from fastapi.testclient import TestClient


API_PATH = Path(__file__).resolve().parents[1] / 'waiting_api.py'
BASE = '/api/v1/local-intake/preprocessing/summary'
BASIS = 'postprocessing-registered-ledger/1'
STAMP = '2026-11-01T00:00:00+00:00'
REVISION = 'a' * 64


class Queue:
    def snapshot(self, **_):
        return {'receiver_status': 'unknown', 'waiting_available': False}


def load_api():
    spec = importlib.util.spec_from_file_location('ledger_waiting_api_test', API_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def registered(filter=None, page=1, page_size=20, expected_revision=None):
    results = {'available': True, 'basis': BASIS, 'as_of': STAMP,
               'counts': {'passed': 1, 'failed': 0, 'unrecognized': None}, 'partial': True}
    if filter is not None:
        results['list'] = {'filter': filter, 'page': page, 'page_size': page_size,
                           'total': 1, 'items': [], 'has_next': False}
    return {'available': True, 'basis': BASIS, 'as_of': STAMP, 'revision': REVISION,
            'current_delivery_verified': False,
            'stage': {'available': True, 'basis': BASIS, 'as_of': STAMP, 'partial': False,
                      'counts': {'waiting_postprocessing': 2, 'postprocessing_in_progress': None,
                                 'waiting_rag_upload': 1}},
            'results': results}


def poll(client, predicate, params=None):
    end = time.monotonic() + 2
    while time.monotonic() < end:
        value = client.get(BASE, params=params).json()
        if predicate(value):
            return value
        time.sleep(.01)
    raise AssertionError('registered projection did not become available')


def test_registration_projection_is_background_and_keeps_its_distinct_basis():
    started, finish = Event(), Event()
    def projection(**args):
        started.set()
        assert finish.wait(2)
        return registered(args['result_filter'], args['page'], args['page_size'], args['expected_revision'])
    api = load_api()
    with TestClient(api.create_app(Queue(), registered_projection=projection)) as client:
        before = time.monotonic()
        first = client.get(BASE).json()
        assert time.monotonic() - before < 1
        assert first['postprocessing_registered_ledger']['available'] is False
        assert started.wait(1)
        finish.set()
        value = poll(client, lambda x: x['postprocessing_registered_ledger']['available'] is True)
        assert value['postprocessing_registered_ledger']['revision'] == REVISION
        assert value['postprocessing_registered_ledger']['stage']['counts']['postprocessing_in_progress'] is None
        assert value['postprocessing_results']['available'] is False


def test_page_revision_is_forwarded_and_missing_revision_never_starts_a_read():
    calls = []
    def projection(**args):
        calls.append(args)
        return registered(args['result_filter'], args['page'], args['page_size'], args['expected_revision'])
    api = load_api()
    with TestClient(api.create_app(Queue(), registered_projection=projection)) as client:
        missing = {'postprocessing_result_filter': 'passed', 'postprocessing_result_page': 2}
        result = client.get(BASE, params=missing).json()['postprocessing_registered_ledger']
        assert result['available'] is False
        assert result['reason'] == 'snapshot_revision_required'
        assert calls == []
        params = {**missing, 'postprocessing_result_revision': REVISION}
        poll(client, lambda x: x['postprocessing_registered_ledger']['available'] is True, params)
        assert calls[0]['expected_revision'] == REVISION
        assert calls[0]['page'] == 2


def test_malformed_or_inconsistent_registered_views_are_unavailable():
    api = load_api()
    for change in ('basis', 'revision', 'progress', 'as_of'):
        def projection(**args):
            out = registered(args['result_filter'], args['page'], args['page_size'], args['expected_revision'])
            if change == 'basis': out['stage']['basis'] = 'folder-stage-receipts/1'
            if change == 'revision': out['revision'] = 'z' * 64
            if change == 'progress': out['stage']['counts']['postprocessing_in_progress'] = 5
            if change == 'as_of': out['results']['as_of'] = '2020-01-01T00:00:00Z'
            return out
        with TestClient(api.create_app(Queue(), registered_projection=projection)) as client:
            value = client.get(BASE).json()
            time.sleep(.05)
            value = client.get(BASE).json()
            assert value['postprocessing_registered_ledger']['available'] is False


def test_without_registered_projection_existing_contract_is_unchanged():
    api = load_api()
    with TestClient(api.create_app(Queue())) as client:
        value = client.get(BASE).json()
        assert 'postprocessing_registered_ledger' not in value
        assert value['postprocessing_results']['available'] is False


def test_unknown_unrecognized_total_cannot_be_published_as_zero():
    api = load_api()
    def projection(**args):
        out = registered(args['result_filter'], args['page'], args['page_size'], args['expected_revision'])
        out['results']['list']['total'] = 0
        return out
    with TestClient(api.create_app(Queue(), registered_projection=projection)) as client:
        params = {'postprocessing_result_filter': 'unrecognized'}
        client.get(BASE, params=params)
        time.sleep(.05)
        result = client.get(BASE, params=params).json()['postprocessing_registered_ledger']
        assert result['available'] is False


def test_registered_rows_reuse_identity_checked_titles_and_public_failure_reasons():
    api = load_api()
    def projection(**args):
        out = registered(args['result_filter'], args['page'], args['page_size'], args['expected_revision'])
        out['results']['counts']['failed'] = 1
        out['results']['list']['items'] = [{'task_id': 'task', 'group_id': 'group', 'paper_id': 'paper',
            'receipt_id': 'receipt', 'code': 'body_validation', 'reason': '已登记未通过'}]
        return out
    def titles(items):
        items[0].update(title_status='已提取', paper_title='合成论文标题')
    with TestClient(api.create_app(Queue(), registered_projection=projection, registered_title_enricher=titles,
                       registered_failure_messages={'body_validation': '正文画像校验未通过'})) as client:
        params = {'postprocessing_result_filter': 'failed'}
        value = poll(client, lambda x: x['postprocessing_registered_ledger']['available'] is True, params)
        item = value['postprocessing_registered_ledger']['results']['list']['items'][0]
        assert item['paper_title'] == '合成论文标题'
        assert '正文画像校验未通过' in item['reason']
        assert item['receipt_id'] == 'receipt'


def test_title_enricher_cannot_change_identity_or_associate_a_title_to_another_receipt():
    api = load_api()
    def projection(**args):
        out = registered(args['result_filter'], args['page'], args['page_size'], args['expected_revision'])
        out['results']['list']['items'] = [{'task_id': 'task', 'group_id': 'group', 'paper_id': 'paper',
            'receipt_id': 'receipt', 'code': 'passed', 'reason': '已登记通过'}]
        return out
    def titles(items):
        items[0].update(receipt_id='other', title_status='已提取', paper_title='错误的标题')
    with TestClient(api.create_app(Queue(), registered_projection=projection, registered_title_enricher=titles)) as client:
        params = {'postprocessing_result_filter': 'passed'}
        value = poll(client, lambda x: x['postprocessing_registered_ledger']['available'] is True, params)
        item = value['postprocessing_registered_ledger']['results']['list']['items'][0]
        assert item['receipt_id'] == 'receipt'
        assert item.get('paper_title') is None
        assert item['title_status'] == '暂不可用'

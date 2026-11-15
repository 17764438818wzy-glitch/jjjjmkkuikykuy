"""Candidate API/UI checks using synthetic SQLite and a minimal DOM, no server."""
import copy
import importlib.util
import json
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from test_folder_state import sample, receipt, control

ROOT = Path(__file__).resolve().parents[1]
BASELINE = ROOT.parent / 'llm-new-prefect-20261031'
sys.path.insert(0, str(BASELINE))


def api():
    path = ROOT / 'api_ui/flow_waiting_api.py'
    assert path.is_file(), 'candidate waiting API is not wired'
    spec = importlib.util.spec_from_file_location('candidate_waiting_api', path)
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    return module


@pytest.fixture
def sources(sample, tmp_path):
    database, identity, store = sample
    control(database, 'CREATE TABLE llm_request_admissions(task_id TEXT,task_run_id TEXT,receipt_id TEXT,kind TEXT,state TEXT)')
    queue_database = tmp_path / 'queue.sqlite3'
    control(queue_database, 'CREATE TABLE changes(value TEXT)')
    class Queue:
        path = queue_database
        def snapshot(self, **kwargs):
            return dict(receiver_status='running', waiting_available=True, in_progress_count=0, as_of='2026-10-31T12:00:00Z')
        def list_waiting_llm(self):
            return dict(items=[identity], unavailable_count=0)
    local_queue = Queue()
    class Tickets:
        index = database
        queue = local_queue
        def summary(self, snapshot):
            return dict(source='prefect', scope='all_tasks', available=True,
                counts=dict(waiting_rag_upload=999, postprocessing_in_progress=999))
        def preprocessing_results(self, *args):
            return dict(available=True, counts={})
    return local_queue, Tickets(), store, [database, queue_database], identity


def summary(sources, **kwargs):
    queue, tickets, store, databases, _ = sources
    options = dict(stage_observer=store, stage_snapshot_databases=databases)
    options.update(kwargs)
    app = api().create_app(queue, ticket_observer=tickets, **options)
    with TestClient(app) as client:
        response = client.get('/api/v1/local-intake/preprocessing/summary')
    assert response.status_code == 200
    assert response.headers['cache-control'] == 'no-store'
    return response.json()['prefect_ticket_summary']


def trio(value):
    counts = value['counts']
    return tuple(counts.get(k) for k in ('waiting_postprocessing', 'postprocessing_in_progress', 'waiting_rag_upload'))


def test_stage_transitions_replace_cumulative_upstream_waiting_count(sources, tmp_path):
    _, _, store, _, identity = sources
    assert trio(summary(sources)) == (1, 0, 0)
    claimed = store.claim(identity, 2, 'c' * 64)
    assert trio(summary(sources)) == (0, 1, 0)
    store.complete(identity, claimed['claim_id'], receipt(tmp_path, identity, claimed['claim_id']),
                   validate_receipt=lambda value: True)
    assert trio(summary(sources)) == (0, 0, 1)


@pytest.mark.parametrize('missing', ['observer', 'databases'])
def test_no_stage_observer_or_source_guard_keeps_three_cards_unknown(sources, missing):
    options = {'stage_observer': None} if missing == 'observer' else {'stage_snapshot_databases': None}
    assert trio(summary(sources, **options)) == (None, None, None)


def test_processing_without_liveness_is_unknown_but_other_counts_remain_verified(sources):
    _, _, store, _, identity = sources
    store.claim(identity, 2, 'c' * 64); store.observe_activity = None
    assert trio(summary(sources)) == (0, None, 0)


@pytest.mark.parametrize('target', ['control', 'queue'])
def test_write_during_stage_snapshot_invalidates_all_related_cards(sources, target):
    _, _, store, databases, _ = sources
    original = store.summary
    def racing():
        result = original()
        if target == 'control': control(databases[0], 'UPDATE tasks SET start_generation=start_generation+1')
        else: control(databases[1], "INSERT INTO changes VALUES('concurrent upstream update')")
        return result
    store.summary = racing
    assert trio(summary(sources)) == (None, None, None)


def test_unavailable_queue_snapshot_or_partial_llm_source_fails_closed(sources):
    queue, _, _, _, identity = sources
    queue.list_waiting_llm = lambda: dict(items=[identity], unavailable_count=1)
    assert trio(summary(sources)) == (None, None, None)
    queue.snapshot = lambda **kwargs: dict(receiver_status='unknown', waiting_available=False)
    assert trio(summary(sources)) == (None, None, None)


def test_missing_database_is_never_created_and_counts_stay_unknown(sources, tmp_path):
    missing = tmp_path / 'missing.sqlite3'
    assert trio(summary(sources, stage_snapshot_databases=[*sources[3], missing])) == (None, None, None)
    assert not missing.exists()


def test_stage_and_upstream_must_use_same_control_database(sources, tmp_path):
    other = tmp_path / 'other.sqlite3'
    control(other, 'CREATE TABLE marker(value TEXT)')
    sources[2].database = other
    assert trio(summary(sources)) == (None, None, None)


def test_omitted_actual_queue_database_keeps_stage_counts_unknown(sources):
    assert trio(summary(sources, stage_snapshot_databases=[sources[3][0]])) == (None, None, None)


def test_missing_queue_path_cannot_claim_verified_source_coverage(sources):
    del type(sources[0]).path
    assert trio(summary(sources)) == (None, None, None)


@pytest.mark.parametrize('bad', [None, '', 'missing.sqlite3'])
def test_invalid_or_unavailable_queue_path_fails_closed(sources, bad):
    sources[0].path = bad
    assert trio(summary(sources)) == (None, None, None)


@pytest.mark.parametrize('missing', ['queue', 'path'])
def test_ticket_observer_must_expose_its_real_queue_source(sources, missing):
    if missing == 'queue':
        del type(sources[1]).queue
    else:
        sources[1].queue = SimpleNamespace()
    assert trio(summary(sources)) == (None, None, None)


def test_other_ticket_observer_queue_is_rejected_even_when_both_are_guarded(sources, tmp_path):
    other = tmp_path / 'observer-queue.sqlite3'
    control(other, 'CREATE TABLE other(value TEXT)')
    sources[1].queue = SimpleNamespace(path=other)
    assert trio(summary(sources, stage_snapshot_databases=[*sources[3], other])) == (None, None, None)


def test_distinct_queue_objects_with_same_real_database_are_accepted(sources):
    sources[1].queue = SimpleNamespace(path=sources[0].path)
    assert trio(summary(sources)) == (1, 0, 0)


@pytest.mark.parametrize('state', ['held', 'unresolved'])
def test_diagnostic_stage_counts_are_retained_without_inflating_main_counts(sources, tmp_path, state):
    _, _, store, databases, identity = sources
    claimed = store.claim(identity, 2, 'c' * 64)
    if state == 'held':
        control(databases[0], "UPDATE tasks SET desired_state='paused'")
        store.complete(identity, claimed['claim_id'], receipt(tmp_path, identity, claimed['claim_id']),
                       validate_receipt=lambda value: True)
    else:
        store.mark_unknown(identity, claimed['claim_id'], 'synthetic worker disconnected')
    result = summary(sources)
    assert trio(result) == (0, 0, 0)
    assert result['counts']['postprocessing_held'] == (1 if state == 'held' else 0)
    assert result['counts']['postprocessing_unresolved'] == (1 if state == 'unresolved' else 0)


def test_diagnostic_counts_are_explicitly_unknown_without_stage_source(sources):
    result = summary(sources, stage_observer=None)
    assert 'postprocessing_held' in result['counts'] and result['counts']['postprocessing_held'] is None
    assert 'postprocessing_unresolved' in result['counts'] and result['counts']['postprocessing_unresolved'] is None


def test_inconsistent_stage_partition_cannot_count_one_folder_twice(sources):
    sources[2].summary = lambda: dict(waiting_postprocessing=1, postprocessing=1, waiting_rag=0, held=0, unresolved=0)
    assert trio(summary(sources)) == (None, None, None)


def render(payload):
    path = ROOT / 'api_ui/prefect-ticket-counts.js'
    assert path.is_file(), 'candidate card script is not wired'
    assert shutil.which('node'), 'Node is required for the actual card script test'
    script = r'''
const elements = new Map();
global.document = { getElementById(id) { if (!elements.has(id)) elements.set(id, { textContent:'',setAttribute(){} }); return elements.get(id); } };
global.window = {dispatchEvent(){}};
global.CustomEvent = function(){};
global.setInterval = () => 0;
global.fetch = async () => ({ok:true,json:async()=>JSON.parse(process.argv[2])});
require(process.argv[1]);
setImmediate(() => process.stdout.write(JSON.stringify(Object.fromEntries([...elements].map(([key,value])=>[key,value.textContent])))));
'''
    result = subprocess.run(['node', '-e', script, str(path), json.dumps(payload)], capture_output=True,
                            text=True, encoding='utf8', check=True)
    return json.loads(result.stdout)


def test_ui_displays_stage_counts_in_existing_three_cards(sources):
    result = summary(sources)
    result['counts'].update(waiting_postprocessing=2, postprocessing_in_progress=3, waiting_rag_upload=4)
    ui = render(dict(prefect_ticket_summary=result))
    assert ui['waiting-postprocessing-count'] == '2'
    assert ui['postprocessing-count'] == '3'
    assert ui['waiting-rag-upload-count'] == '4'
    assert '暂不显示' not in ui['waiting-postprocessing-hint']


@pytest.mark.parametrize('bad', [None, -1, True, '0', 0.5, 9007199254740992])
def test_ui_unknown_missing_or_invalid_is_dash_never_zero(bad):
    counts = dict(waiting_postprocessing=bad, postprocessing_in_progress=bad, waiting_rag_upload=bad)
    ui = render(dict(prefect_ticket_summary=dict(source='prefect',scope='all_tasks',available=True,counts=counts,
                                               postprocessing_basis='folder-stage-receipts/1')))
    assert ui['waiting-postprocessing-count'] == ui['postprocessing-count'] == ui['waiting-rag-upload-count'] == '—'


def test_ui_zero_is_shown_only_for_verified_available_counts():
    counts = dict(waiting_postprocessing=0, postprocessing_in_progress=0, waiting_rag_upload=0)
    payload = dict(prefect_ticket_summary=dict(source='prefect',scope='all_tasks',available=True,counts=counts,
                                              postprocessing_basis='folder-stage-receipts/1'))
    ui = render(payload)
    assert ui['waiting-postprocessing-count'] == ui['postprocessing-count'] == ui['waiting-rag-upload-count'] == '0'
    payload['prefect_ticket_summary']['available'] = False
    ui = render(payload)
    assert ui['waiting-postprocessing-count'] == ui['postprocessing-count'] == ui['waiting-rag-upload-count'] == '—'


def test_ui_does_not_accept_legacy_counts_without_stage_observer_provenance():
    counts = dict(waiting_postprocessing=12, postprocessing_in_progress=7, waiting_rag_upload=9)
    ui = render(dict(prefect_ticket_summary=dict(source='prefect',scope='all_tasks',available=True,counts=counts)))
    assert ui['waiting-postprocessing-count'] == ui['postprocessing-count'] == ui['waiting-rag-upload-count'] == '—'


def test_source_observer_failure_is_unavailable_not_a_fabricated_count(sources):
    queue, tickets, store, databases, _ = sources
    def unavailable(snapshot): raise ValueError('synthetic observer unavailable')
    tickets.summary = unavailable
    app = api().create_app(queue, ticket_observer=tickets, stage_observer=store, stage_snapshot_databases=databases)
    with TestClient(app) as client:
        result = client.get('/api/v1/local-intake/preprocessing/summary')
    assert result.status_code == 503
    assert result.json()['waiting_count'] is None


def test_ui_nonzero_held_and_unresolved_are_hints_not_main_card_counts():
    counts = dict(waiting_postprocessing=0, postprocessing_in_progress=0, waiting_rag_upload=0,
                  postprocessing_held=2, postprocessing_unresolved=3)
    ui = render(dict(prefect_ticket_summary=dict(source='prefect',scope='all_tasks',available=True,
                counts=counts,postprocessing_basis='folder-stage-receipts/1')))
    assert ui['waiting-postprocessing-count'] == ui['postprocessing-count'] == ui['waiting-rag-upload-count'] == '0'
    for key in ('waiting-postprocessing-hint', 'postprocessing-hint', 'waiting-rag-upload-hint'):
        assert '暂存待核对 2 组' in ui[key]
        assert '状态待核对 3 组' in ui[key]


@pytest.mark.parametrize('valid_basis', [False, True])
def test_ui_unknown_diagnostics_are_dashes_and_do_not_claim_no_unresolved_work(valid_basis):
    counts = dict(waiting_postprocessing=None, postprocessing_in_progress=None, waiting_rag_upload=None,
                  postprocessing_held=None, postprocessing_unresolved=None)
    value = dict(source='prefect',scope='all_tasks',available=True,counts=counts)
    if valid_basis: value['postprocessing_basis'] = 'folder-stage-receipts/1'
    else: counts.update(postprocessing_held=88, postprocessing_unresolved=99)
    ui = render(dict(prefect_ticket_summary=value))
    for key in ('waiting-postprocessing-hint', 'postprocessing-hint', 'waiting-rag-upload-hint'):
        assert '暂存待核对 —' in ui[key]
        assert '状态待核对 —' in ui[key]
        assert '88' not in ui[key] and '99' not in ui[key]

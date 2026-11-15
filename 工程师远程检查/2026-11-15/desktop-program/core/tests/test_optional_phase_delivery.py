import json
import threading

import pytest

from paperflow.package import load, optional_json, save, sha
from paperflow.phase_cache import content_token
from test_managed_entry import entry_case
from test_processing_graph import graph_case, model_calls, run_worker


BROKEN_ROUTES = '''{"operations": [{"op": "replace"}
}
], "final_routing_candidates": [{"code": "120201", "name": "工商管理"}]}'''


def test_optional_phase_token_tracks_unreadable_bytes_without_blocking(tmp_path):
    report = tmp_path/'corrections.json'
    report.write_text('{incomplete optional report', encoding='utf-8')
    first = content_token(tmp_path, 'round2')
    report.write_text('{different incomplete report', encoding='utf-8')
    assert content_token(tmp_path, 'round2') != first
    save(report, {'operations': []})
    canonical = content_token(tmp_path, 'round2')
    report.write_text('{ "operations" : [ ] }', encoding='utf-8')
    assert content_token(tmp_path, 'round2') == canonical
    (tmp_path/'profile.json').write_text('{broken core file', encoding='utf-8')
    with pytest.raises(json.JSONDecodeError): content_token(tmp_path, 'round2')


def test_optional_json_recovers_only_one_isolated_extra_closer_without_writing(tmp_path):
    report = tmp_path/'corrections.json'
    report.write_text(BROKEN_ROUTES, encoding='utf-8')
    before = sha(report)
    assert optional_json(report, {})['final_routing_candidates'][0]['code'] == '120201'
    assert sha(report) == before
    with pytest.raises(json.JSONDecodeError): load(report)
    report.write_text(BROKEN_ROUTES.replace('"op": "replace"', '"op": "replace" trailing'), encoding='utf-8')
    assert optional_json(report, {}) == {}


def test_normal_graph_finishes_with_optional_report_and_missing_profile_routes(graph_case):
    config, store, task, manager, _ = graph_case
    material = config.root/'fixture-material'
    profile = load(material/'profile.json'); profile.pop('routing_candidates', None)
    save(material/'profile.json', profile)
    (material/'corrections.json').write_text(BROKEN_ROUTES, encoding='utf-8')
    run_worker(graph_case)
    assert load(config.root/'worker-result.json')['stage'] == 'handed_off'
    assert model_calls(config) == ['round1','round2','round3_assets','round3_redact','round3_chunk']
    assert len(manager.ledger.rows('SELECT * FROM delivery_packages')) == 1
    assert not manager.ledger.rows('SELECT * FROM correction_attempts')


def test_explicit_retry_reopens_only_selected_held_work_without_resetting_history(entry_case, monkeypatch):
    from paperflow.managed import run_managed
    from paperflow.scheduler import Scheduler
    from paperflow.store import Store
    root, old, source, config = entry_case
    first = run_managed(config, root, [source], legacy_root=old)
    task_id = first['tasks'][0]['task_id']
    store = Store(config.root/'state.sqlite3')
    with store.db() as db:
        db.execute("UPDATE scheduler_jobs SET state='held' WHERE kind='graph'")
    with pytest.raises(ValueError, match='execute'):
        run_managed(config, root, [source], legacy_root=old, retry_tasks=[task_id])
    stop = threading.Event()
    def pause_tick(scheduler):
        scheduler.manager.pause('test observation only'); stop.set()
    monkeypatch.setattr(Scheduler, 'tick', pause_tick)
    result = run_managed(config, root, [source], legacy_root=old, execute=True,
                         retry_tasks=[task_id], stop=stop, seconds=2)
    assert result['manager']['pause_persisted'] and result['manager']['live_units'] == 0
    with store.db() as db:
        assert db.execute('SELECT state FROM scheduler_jobs').fetchone()[0] == 'queued'
        assert db.execute('SELECT COUNT(*) FROM control_executions').fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM events WHERE kind='operator_graph_retry'").fetchone()[0] == 1

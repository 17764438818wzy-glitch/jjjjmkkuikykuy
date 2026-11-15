"""Cleanup/repair ordering through real SQLite, Job, IPC and queue boundaries."""
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import sys
import threading

import pytest

from paperflow.cleanup_paths import track_artifact, delete_inventory
from paperflow.cleanup_store import CleanupLedger
from paperflow.handoff_store import canonical
from paperflow.package import save, sha
from paperflow.repair_store import RepairLedger
from paperflow.scheduler import Scheduler
from paperflow.scheduling_policy import ActionDeferred, fault
from test_repair_store import repair_case, report
from test_repair_control import repair_manager
from test_repair_review import publish_repair
from test_control import manager
from test_delivery_state import delivery_case
from test_cleanup_store import retired
from test_cleanup_flow import DeletableRag, prepared
from test_cleanup_acceptance import CleanupHandler
from test_scheduling_acceptance import start_server
from test_winjob import wait_until


def temporary(case):
    config, store, task, _, _ = case
    config.processing_ledger_path = str(store.path)
    path = config.root / 'work' / task['task_id'] / 'attempts' / 'unreferenced'
    path.mkdir(parents=True)
    (path / 'output.txt').write_text('synthetic unused attempt')
    track_artifact(config, task, path, 'attempt')
    ledger = CleanupLedger(store, config)
    ledger.close_run(task['task_id'], task['task_id'], 'complete')
    return ledger, ledger.plan_artifact(path), path


def queue_cleanup(manager, config, item, tmp_path, monkeypatch):
    config.min_free_gib = 0
    config.max_memory_percent = 100
    scheduler = Scheduler(manager, config, isolation_root=tmp_path)
    monkeypatch.setattr(scheduler, 'discover', lambda: None)
    service = scheduler.local_service if item['kind'] == 'local' else scheduler.service.services['rag']
    job = scheduler.ledger.enqueue('cleanup', item['source_id'], 'cleanup:' + manager.cleanup.key(item),
        dict(item_id=item['item_id'], resource_kind=item['kind']), service['service_id'])
    return scheduler, job


def attempt(scheduler, job):
    # Advance only this fixture's due time; exercise the actual claim/worker/reap path.
    scheduler.ledger._write(lambda db: db.execute('UPDATE scheduler_jobs SET due=0 WHERE job_id=?', (job,)))
    scheduler.tick()
    wait_until(lambda: scheduler.manager.status()['live_units'] == 0, seconds=25)
    scheduler.reap()
    return scheduler.ledger.job(job)


@pytest.mark.parametrize('state', ['deleting', 'pending_reconcile'])
def test_local_delete_first_defers_repair_without_spending_then_report_replays(
        repair_case, repair_manager, tmp_path, monkeypatch, state):
    config, store, task, repairs, identity = repair_case
    ledger, item, path = temporary(repair_case)
    ledger._write(lambda db: ledger.begin_sql(db, item['item_id']))
    if state == 'pending_reconcile':
        ledger._write(lambda db: ledger.result_sql(db, item['item_id'], 'unknown', {'error': 'local restart fixture'}))
    with pytest.raises(ActionDeferred): report(repair_case)
    assert repairs.used(identity) == 0
    assert not repairs.rows('SELECT * FROM repair_requests')
    assert not repairs.rows('SELECT * FROM correction_attempts')
    assert not repairs.rows("SELECT * FROM package_references WHERE owner_kind='repair'")
    scheduler, job = queue_cleanup(repair_manager, config, item, tmp_path, monkeypatch)
    assert attempt(scheduler, job)['state'] == 'complete'
    assert not path.exists()
    request = report(repair_case)
    run = repairs._write(lambda db: repairs.start_sql(db, request['repair_request_id']))
    publish_repair(repair_case, tmp_path, run, 'repair-after-cleanup')
    assert repairs.get(request['repair_request_id'])['state'] == 'complete'
    assert report(repair_case)['repair_request_id'] == request['repair_request_id']
    assert repairs.used(identity) == 1
    assert sha(task['source_path']) == task['source_sha256']


@pytest.mark.parametrize('pause', [False, True])
def test_repair_first_requeues_cleanup_until_real_handoff_releases_protection(
        repair_case, repair_manager, tmp_path, monkeypatch, pause):
    config, _, task, repairs, identity = repair_case
    ledger, item, path = temporary(repair_case)
    request = report(repair_case)
    scheduler, job = queue_cleanup(repair_manager, config, item, tmp_path, monkeypatch)
    assert attempt(scheduler, job)['state'] == 'retry'
    assert path.exists() and ledger.item(item['item_id'])['state'] == 'waiting'
    assert scheduler.ledger.budget(job)['rounds'] == 0
    run = repairs._write(lambda db: repairs.start_sql(db, request['repair_request_id']))
    result = publish_repair(repair_case, tmp_path, run, 'repair-before-cleanup')
    if pause:
        repair_manager.pause('fixture pause before automatic cleanup')
        count = len(scheduler.ledger.rows('SELECT * FROM scheduler_attempts'))
        assert attempt(scheduler, job)['state'] == 'retry'
        assert len(scheduler.ledger.rows('SELECT * FROM scheduler_attempts')) == count
        assert path.exists()
        wait_until(lambda: repair_manager.status()['pause_persisted'])
        repair_manager.resume()
    assert attempt(scheduler, job)['state'] == 'complete'
    assert not path.exists()
    assert Path(result['package_dir']).exists()
    assert Path(repairs.registration(identity['manifest_sha256'])['package_dir']).exists()
    assert repairs.used(identity) == 1


def test_concurrent_delete_and_new_repair_have_only_one_winner(repair_case):
    ledger, item, _ = temporary(repair_case)
    barrier = threading.Barrier(2)
    def begin():
        barrier.wait()
        try: ledger._write(lambda db: ledger.begin_sql(db, item['item_id'])); return 'delete'
        except ActionDeferred: return 'deferred'
    def register():
        barrier.wait()
        try: report(repair_case); return 'repair'
        except ActionDeferred: return 'deferred'
    with ThreadPoolExecutor(2) as pool:
        a, b = pool.submit(begin), pool.submit(register)
        results = [a.result(), b.result()]
    assert results.count('deferred') == 1
    assert (ledger.item(item['item_id'])['state'] == 'deleting') == ('delete' in results)
    assert repair_case[3].used(repair_case[4]) == int('repair' in results)


def test_completed_report_replay_during_cleanup_is_free_but_new_followup_waits(repair_case, tmp_path):
    _, _, _, repairs, identity = repair_case
    rid = report(repair_case)['repair_request_id']
    run = repairs._write(lambda db: repairs.start_sql(db, rid))
    publish_repair(repair_case, tmp_path, run, 'first-repair')
    ledger, item, _ = temporary(repair_case)
    ledger._write(lambda db: ledger.begin_sql(db, item['item_id']))
    assert report(repair_case)['repair_request_id'] == rid
    with pytest.raises(ActionDeferred): repairs._write(lambda db: repairs.start_sql(db, rid))
    before = {table: repairs.rows('SELECT * FROM ' + table) for table in
              ('repair_requests', 'repair_reports', 'repair_report_followups', 'repair_targets',
               'package_references', 'correction_attempts', 'correction_budgets')}
    with pytest.raises(ActionDeferred):
        repairs._write(lambda db: repairs.request_sql(db, identity, 'round3_chunk', 'missing_chunks',
            '120203', {'missing': 'new evidence after cleanup intent'}))
    assert before == {table: repairs.rows('SELECT * FROM ' + table) for table in before}
    result = delete_inventory(json.loads(item['resource']), ledger.roots(), ledger.originals(), threading.Event())
    ledger._write(lambda db: ledger.result_sql(db, item['item_id'], 'deleted', result))
    resumed = repairs._write(lambda db: repairs.request_sql(db, identity, 'round3_chunk', 'missing_chunks',
        '120203', {'missing': 'new evidence after cleanup intent'}))
    assert repairs.get(resumed['followup_request_id'])['state'] == 'queued'
    assert repairs.used(identity) == 2


def test_followup_without_target_specialty_also_waits_for_local_cleanup(repair_case, tmp_path):
    _, _, _, repairs, _ = repair_case
    rid = report(repair_case)['repair_request_id']
    run = repairs._write(lambda db: repairs.start_sql(db, rid))
    publish_repair(repair_case, tmp_path, run, 'settled-repair')
    ledger, item, _ = temporary(repair_case)
    ledger._write(lambda db: ledger.begin_sql(db, item['item_id']))
    # Exercise the child=None responsibility path directly: there is no child
    # request whose request_sql gate could otherwise protect this new followup.
    with pytest.raises(ActionDeferred):
        repairs._write(lambda db: repairs.forward_late_sql(db, repairs.get(rid),
            'specialty-not-in-result', {'missing': 'uncovered obligation'}))
    assert not repairs.rows('SELECT * FROM repair_report_followups')


def test_direct_graph_launch_waits_behind_local_delete(repair_case, repair_manager):
    _, _, task, _, _ = repair_case
    ledger, item, _ = temporary(repair_case)
    ledger._write(lambda db: ledger.begin_sql(db, item['item_id']))
    with pytest.raises(ActionDeferred):
        repair_manager.launch('graph', task['task_id'], [sys.executable, '-c', 'pass'],
            cwd=Path.cwd(), run_id='another-unclosed-run')
    assert not repair_manager.executions


def test_remote_repair_protection_requeues_then_deletes_once_after_real_release(delivery_case, tmp_path, monkeypatch):
    case, original = retired(delivery_case, tmp_path)
    m, config, task, current, commits, _, _ = case
    ledger = CleanupLedger(m.store, config)
    ledger.discover_remote()
    item = ledger.items()[0]; old = item['manifest_sha256']
    identity = dict(json.loads(ledger.registration(old)['identity_json']), manifest_sha256=old)
    repairs = RepairLedger(m.store)
    # The already published current version is independent of the reported old
    # phase. Real complete_sql can discharge its responsibility without changing it.
    for digest, token in ((old, 'old-phase'), (current, 'independent-phase')):
        ledger._write(lambda db, digest=digest, token=token: db.execute(
            'INSERT INTO package_stage_dependencies VALUES(?,?,?)',
            (digest, 'round3_chunk', canonical(dict(token=token, lineage_complete=True, lineage=[token])))))
    request = repairs._write(lambda db: repairs.request_sql(db, identity, 'round3_chunk', 'chunks_missing',
        '120202', {'kind': 'old phase missing chunk'}))
    repair_fixture = config, m.store, task, repairs, identity
    run = repairs._write(lambda db: repairs.start_sql(db, request['repair_request_id']))
    ledger._write(lambda db: db.execute('UPDATE cleanup_items SET eligible_at=0'))
    server, thread = start_server(CleanupHandler)
    server.rag = DeletableRag(original); server.uploads = {}; server.http = []; server.fail_b = False
    server.inflight = server.peak = 0; server.lock = threading.Lock()
    server.identity = dict(deployment='stage7-fix-loopback', version='fixture-v1')
    thread.start()
    config.rag_url = f'http://127.0.0.1:{server.server_port}'
    proof = tmp_path / 'acceptance.json'
    save(proof, dict(passed=True, endpoint=config.rag_url, identity=server.identity))
    config.delivery_acceptance_report = str(proof)
    actual = m.launch
    def launch(kind, key, argv, **kwargs):
        argv = list(argv)
        if 'paperflow.cleanup_worker' in argv:
            i = argv.index('-m'); argv[i:i+2] = [str(Path('tests/fixtures/cleanup_worker.py').resolve())]
        return actual(kind, key, argv, **kwargs)
    monkeypatch.setattr(m, 'launch', launch)
    scheduler, job = queue_cleanup(m, config, item, tmp_path, monkeypatch)
    active = commits.ledger.active(task['source_id'], '120201')
    try:
        assert attempt(scheduler, job)['state'] == 'retry'
        assert not [row for row in server.http if row[0] == 'DELETE']
        assert scheduler.ledger.budget(job)['rounds'] == 0
        publish_repair(repair_fixture, tmp_path, run, 'obsolete-phase-fixed')
        assert ledger._write(lambda db: ledger.repair_settled(db, request['repair_request_id']))
        assert attempt(scheduler, job)['state'] == 'complete'
        assert ledger.item(item['item_id'])['state'] == 'deleted'
        assert len([row for row in server.http if row[0] == 'DELETE']) == 1
        assert commits.ledger.active(task['source_id'], '120201') == active
        scheduler.tick()
        assert len([row for row in server.http if row[0] == 'DELETE']) == 1
    finally:
        server.shutdown(); thread.join(2); server.server_close()


@pytest.mark.parametrize('receipt_saved', [False, True])
def test_unknown_remote_write_is_not_demoted_by_temporary_repair_protection(delivery_case, tmp_path, receipt_saved):
    workflow, client, case = prepared(delivery_case, tmp_path)
    client.lose_response = True
    with pytest.raises(TimeoutError): workflow.run()
    item = workflow.ledger.item(workflow.item_id)
    assert item['state'] == 'pending_reconcile'
    if not receipt_saved:
        # Lost worker result: the action's unresolved fact alone is authoritative.
        workflow.ledger._write(lambda db: db.execute("UPDATE cleanup_items SET state='deleting' WHERE item_id=?", (workflow.item_id,)))
        item = workflow.ledger.item(workflow.item_id)
    # A retained protection fact plus an unknown write must keep the stronger
    # barrier. The insert represents a persisted pre-existing reference.
    workflow.ledger._write(lambda db: db.execute('INSERT INTO package_references VALUES(?,?,?,?,?)',
        (item['manifest_sha256'], 'safety_specialty', canonical([item['source_id'], item['specialty_code']]),
         'package', item['manifest_sha256'])))
    with pytest.raises(ValueError) as error:
        workflow.ledger._write(lambda db: workflow.ledger.remote_guard(db, item))
    assert fault(error.value)['category'] != 'deferred'
    assert workflow.run()['state'] == 'pending_reconcile'
    assert len([row for row in client.calls if row[0] == 'DELETE']) == 1
    assert workflow.ledger.unresolved(case[2]['source_id'], item['specialty_code'])

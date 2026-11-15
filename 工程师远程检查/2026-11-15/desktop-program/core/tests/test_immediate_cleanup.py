"""Replacement acceptance, retained recovery material and background scheduling."""
import json
from pathlib import Path

import pytest

from paperflow.cleanup_paths import inventory
from paperflow.cleanup_store import CleanupLedger
from paperflow.handoff import PackageHandoff
from paperflow.handoff_store import canonical
from paperflow.package import load, save, sha
from test_control import manager
from test_delivery_state import delivery_case
from test_cleanup_store import retired
from test_delivery_flow import advance, flow, release
from test_scheduling_store import queue, add, TRANSIENT


def test_verified_replacement_is_immediately_eligible_without_changing_legacy_clock(delivery_case, tmp_path):
    case, _ = retired(delivery_case, tmp_path)
    m, config, task, digest, *_ = case
    ledger = CleanupLedger(m.store, config)
    ledger.discover_remote()
    item = ledger.items()[0]
    assert config.old_version_retention_seconds == 7 * 86400  # Legacy records retain their old policy.
    assert item['eligible_at'] == item['created']
    resource = json.loads(item['resource'])
    assert resource['replacement']['identity']['manifest_sha256'] == digest
    assert ledger._write(lambda db: ledger.begin_sql(db, item['item_id']))['state'] == 'deleting'
    assert ledger.status(task['task_id'])['closeout_pending'] == 0


@pytest.mark.parametrize('damage', ['readback', 'retrieval', 'switch', 'frozen', 'qualification'])
def test_replacement_proof_is_rechecked_before_deletion(delivery_case, tmp_path, damage):
    case, _ = retired(delivery_case, tmp_path)
    m, config, task, digest, *_ = case
    ledger = CleanupLedger(m.store, config)
    ledger.discover_remote()
    item = ledger.items()[0]
    # Only the fixture clock is accelerated: the regression must expose old guard gaps too.
    with m.store.db() as db:
        db.execute('UPDATE cleanup_items SET eligible_at=0')
        if damage in ('readback', 'retrieval'):
            row = db.execute('SELECT receipts_json FROM specialty_progress WHERE manifest_sha256=? AND specialty_code=?', (digest, '120201')).fetchone()
            receipts = json.loads(row[0])
            receipts['120201']['verified' if damage == 'readback' else 'retrieval_verified'] = False
            db.execute('UPDATE specialty_progress SET receipts_json=? WHERE manifest_sha256=? AND specialty_code=?', (canonical(receipts), digest, '120201'))
        elif damage == 'switch':
            db.execute("UPDATE specialty_switches SET phase='old_disabled'")
        elif damage == 'frozen':
            db.execute("UPDATE specialty_tasks SET status='frozen' WHERE manifest_sha256=?", (digest,))
        else:
            resource = json.loads(item['resource'])
            if 'replacement' in resource:
                resource['replacement']['receipts_sha256'] = 'f' * 64
            db.execute('UPDATE cleanup_items SET resource=?', (canonical(resource),))
    with pytest.raises(ValueError):
        ledger._write(lambda db: ledger.begin_sql(db, item['item_id']))
    assert ledger.item(item['item_id'])['state'] == 'waiting'


def test_queued_local_delete_cannot_remove_sealed_package_or_original(delivery_case, tmp_path):
    case, _ = retired(delivery_case, tmp_path)
    m, config, task, _, commits, *_ = case
    ledger = CleanupLedger(m.store, config)
    ledger.close_run(task['task_id'], task['task_id'], 'complete')
    old_digest = delivery_case[3]
    old_path = Path(commits.ledger.registration(old_digest)['package_dir'])
    before = {str(p): sha(p) for p in old_path.rglob('*') if p.is_file()}
    original = sha(task['source_path'])
    # Simulate a pre-policy cleanup item mislabeled as a generic artifact.
    resource = dict(inventory(old_path, ledger.roots(), ledger.originals()), type='artifact',
                    thread_id=task['task_id'], task_id=task['task_id'], artifact_kind='attempt')
    identity = dict(task, manifest_sha256=old_digest)
    item = ledger._write(lambda db: ledger.add_sql(db, 'local', 'delayed', identity, resource, 0, 0))
    # Drop ordinary package references to exercise the new permanent preservation rule.
    with m.store.db() as db:
        db.execute('DELETE FROM package_references WHERE package_sha256=?', (old_digest,))
    with pytest.raises(ValueError, match='sealed|封包'):
        ledger._write(lambda db: ledger.begin_sql(db, item['item_id']))
    assert {str(p): sha(p) for p in old_path.rglob('*') if p.is_file()} == before
    assert sha(task['source_path']) == original


def test_abandoned_published_package_stays_retained_by_default(delivery_case):
    m, config, task, _, _, material, catalog = delivery_case
    handoff = PackageHandoff(m.store, config.handoff_root)
    intent = handoff.prepare('retained-orphan', dict(task, revision=3), material, catalog, mapping_version='retained-orphan')
    package = handoff.publish('retained-orphan', intent['input_fingerprint'])
    ledger = CleanupLedger(m.store, config)
    ledger.close_run('retained-orphan', task['task_id'], 'abandoned', reason='isolated explicit abandonment')
    config.orphan_package_retention_seconds = 0
    with pytest.raises(ValueError, match='sealed'):
        ledger.plan_publication('retained-orphan', intent['input_fingerprint'])
    assert package.exists()


def test_ready_delivery_precedes_cleanup_and_cleanup_failure_does_not_block_next(queue):
    ledger, service = queue
    cleaning = add(ledger, service, 'old', kind='cleanup')
    first = add(ledger, service, 'new-one')
    second = add(ledger, service, 'new-two')
    for expected in (first, second):
        claimed = ledger.claim(now=1)
        assert claimed['job_id'] == expected
        ledger.finish(claimed['attempt_id'], 'complete', {}, now=2)
    claimed = ledger.claim(now=3)
    assert claimed['job_id'] == cleaning
    ledger.finish(claimed['attempt_id'], 'failed', {'fault': TRANSIENT}, now=4)
    third = add(ledger, service, 'new-three', now=5)
    assert ledger.claim(now=1000)['job_id'] == third


def test_later_replacement_does_not_erase_first_retirement_qualification(delivery_case, tmp_path):
    case, client = retired(delivery_case, tmp_path)
    m, config, task, digest, commits, material, catalog = case
    ledger = CleanupLedger(m.store, config)
    ledger.discover_remote()
    item = ledger.items()[0]
    proof = json.loads(item['resource'])['replacement']
    handoff = PackageHandoff(m.store, config.handoff_root)
    profile = load(material / 'profile.json')
    profile['article']['title'] += '再次更新'
    save(material / 'profile.json', profile)
    intent = handoff.prepare('third-version', dict(task, revision=3), material, catalog,
        mapping_version=sha(config.catalog_path), expected_targets=handoff.ledger.expectations(task['task_id'], ['120201', '120202']))
    result = handoff.resume('third-version', intent['input_fingerprint'])
    third_case = (m, config, task, result['manifest_sha256'], commits, material, catalog)
    third = flow(third_case, tmp_path, client)
    third.run(); release(third)
    assert ledger.switching(task['source_id'], '120201')['switch_id'] != proof['switch_id']
    assert ledger._write(lambda db: ledger.begin_sql(db, item['item_id']))['state'] == 'deleting'
    assert json.loads(ledger.item(item['item_id'])['resource'])['replacement'] == proof


def test_replacement_damage_after_ticket_still_denies_final_delete_permit(delivery_case, tmp_path):
    from test_cleanup_control import controlled, request
    case, _ = retired(delivery_case, tmp_path)
    commits, item, ex = controlled(case, tmp_path)
    m = commits.manager
    commits.ledger._write(lambda db: commits.ledger.begin_sql(db, item['item_id']))
    obj = json.loads(item['resource'])['object']
    ticket = request(commits, item, ex, 'cleanup_write', 'http', method='DELETE',
        endpoint=f"/datasets/{obj['dataset_id']}/documents", ids=[obj['document_id']])
    with m.store.db() as db:
        db.execute("UPDATE specialty_tasks SET status='frozen' WHERE manifest_sha256=?", (case[3],))
    with pytest.raises(ValueError):
        snapshot = m.check_begin(ex.execution_id, ticket)
        m.grant_begin(ex.execution_id, ticket, snapshot)
    assert not m.tickets[ticket]['consumed']


def test_legacy_retirement_without_acceptance_is_not_silently_accelerated(delivery_case, tmp_path):
    case, _ = retired(delivery_case, tmp_path)
    m, config, *_ = case
    ledger = CleanupLedger(m.store, config)
    with m.store.db() as db:
        row = db.execute('SELECT data FROM specialty_retirements').fetchone()
        data = json.loads(row['data']); data.pop('replacement')
        db.execute('UPDATE specialty_retirements SET data=?', (canonical(data),))
    ledger.discover_remote()
    item = ledger.items()[0]
    assert item['eligible_at'] - item['created'] == 7 * 86400
    with pytest.raises(ValueError, match='retention'):
        ledger._write(lambda db: ledger.begin_sql(db, item['item_id']))
    with pytest.raises(ValueError, match='historical replacement acceptance'):
        ledger._write(lambda db: ledger.begin_sql(db, item['item_id'], now=item['eligible_at'] + 1))
    assert ledger.item(item['item_id'])['state'] == 'waiting'
    assert Path(ledger.registration(item['manifest_sha256'])['package_dir']).exists()
    assert not ledger.rows("SELECT 1 FROM control_actions WHERE kind='cleanup_write'")


def test_new_expectation_temporarily_defers_cleanup_then_retries_without_budget(delivery_case, tmp_path):
    from paperflow.scheduling_policy import ActionDeferred, fault
    case, client = retired(delivery_case, tmp_path)
    m, config, *_ = case
    ledger = CleanupLedger(m.store, config); ledger.discover_remote()
    item = ledger.items()[0]
    newer = advance(case)
    with pytest.raises(ActionDeferred) as error:
        ledger._write(lambda db: ledger.begin_sql(db, item['item_id']))
    assert fault(error.value)['category'] == 'deferred'
    delivery = flow(newer, tmp_path, client); delivery.run(); release(delivery)
    assert ledger._write(lambda db: ledger.begin_sql(db, item['item_id']))['state'] == 'deleting'


def test_real_queue_resumes_cleanup_after_newer_target_commits(delivery_case, tmp_path, monkeypatch):
    import threading
    from test_cleanup_acceptance import CleanupHandler
    from test_cleanup_flow import DeletableRag
    from test_cleanup_repair_order import queue_cleanup, attempt
    from test_scheduling_acceptance import start_server
    case, original = retired(delivery_case, tmp_path)
    m, config, *_ = case
    ledger = CleanupLedger(m.store, config); ledger.discover_remote()
    item = ledger.items()[0]
    newer = advance(case)
    server, thread = start_server(CleanupHandler)
    server.rag = DeletableRag(original); server.uploads = {}; server.http = []; server.fail_b = False
    server.inflight = server.peak = 0; server.lock = threading.Lock()
    server.identity = dict(deployment='immediate-gc-loopback', version='fixture-v1')
    thread.start(); config.rag_url = f'http://127.0.0.1:{server.server_port}'
    proof = tmp_path / 'gc-acceptance.json'
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
    try:
        assert attempt(scheduler, job)['state'] == 'retry'
        assert scheduler.ledger.budget(job)['rounds'] == 0
        assert not [row for row in server.http if row[0] == 'DELETE']
        delivery = flow(newer, tmp_path, server.rag); delivery.run(); release(delivery)
        assert attempt(scheduler, job)['state'] == 'complete'
        assert ledger.item(item['item_id'])['state'] == 'deleted'
        assert len([row for row in server.http if row[0] == 'DELETE']) == 1
    finally:
        server.shutdown(); thread.join(2); server.server_close()


def test_explicit_closeout_keeps_existing_owned_delete_contract(delivery_case, tmp_path):
    from test_cleanup_flow import prepared
    workflow, client, case = prepared(delivery_case, tmp_path)
    item = workflow.ledger.item(workflow.item_id)
    resource = json.loads(item['resource']); resource.pop('replacement')
    with case[0].store.db() as db:
        db.execute("UPDATE cleanup_items SET category='closeout',resource=? WHERE item_id=?", (canonical(resource), item['item_id']))
    active = case[4].ledger.active(case[2]['source_id'], '120201')
    assert workflow.run()['state'] == 'deleted'
    assert len([call for call in client.calls if call[0] == 'DELETE']) == 1
    assert case[4].ledger.active(case[2]['source_id'], '120201') == active

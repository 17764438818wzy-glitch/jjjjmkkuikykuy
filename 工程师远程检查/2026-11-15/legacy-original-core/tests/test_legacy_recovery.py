"""Historical maintenance uses current ownership without reviving old writes."""
import copy
import json

import pytest

from paperflow.delivery_control import ControlledRagClient
from paperflow.handoff_store import canonical, HandoffLedger
from paperflow.legacy_readback import readback
from paperflow.package import save, sha, load
from paperflow.transition import archive_dispatcher_receipt
from test_control import manager
from test_delivery_state import delivery_case, owner, begin
from test_delivery_flow import flow, advance, ManagedActions, release
from test_legacy_readback import legacy
from test_transition_review import stop_owner


def adopted(case, tmp_path):
    actions, remote = legacy(case, tmp_path)
    result = readback(actions.config, actions.commits.ledger, actions,
                      ControlledRagClient(remote, actions), actions.identity)
    assert result['state'] == 'active'
    stop_owner(actions)
    return remote


def archive_pending(case, tmp_path):
    m, c, task, digest, commits, *_ = case
    path = tmp_path / 'old-receipt.json'
    save(path, dict(schema='rag-mid-ingestion-receipt/1.0', task_id='previous-task',
                   source_id=task['source_id'], source_sha256=task['source_sha256'],
                   revision='r01', manifest_sha256='b' * 64, status='submitted_to_parse',
                   targets={'120201': {'document_id': 'prior-doc'}}))
    return archive_dispatcher_receipt(m.store, path, tmp_path / 'archive')


def test_historical_freeze_disables_without_upload_or_content_write(delivery_case, tmp_path):
    remote = adopted(delivery_case, tmp_path)
    m, c, task, digest, commits, *_ = delivery_case
    commits.freeze_active(task['source_id'], '120201', 'confirmed problem')
    worker = flow(delivery_case, tmp_path, remote)
    remote.calls = []
    assert worker.run()['state'] == 'frozen'
    assert commits.ledger.active(task['source_id'], '120201') is None
    assert all(doc['status'] == '0' for doc in remote.docs.values())
    assert all(ch['available'] is False for chunks in remote.chunks.values() for ch in chunks)
    writes = [(method, body) for method, path, body in remote.calls if method != 'GET' and path != '/retrieval']
    assert writes and all(method == 'PATCH' and body in ({'enabled': 0}, {'available': False}) for method, body in writes)
    remote.calls = []
    assert worker.run()['state'] == 'frozen'
    assert not any(method != 'GET' and path != '/retrieval' for method, path, _ in remote.calls)


@pytest.mark.parametrize('bad', ['enable', 'upload', 'content', 'foreign_document', 'foreign_chunk'])
def test_historical_safety_never_grants_other_writes(delivery_case, tmp_path, bad):
    remote = adopted(delivery_case, tmp_path)
    m, c, task, digest, commits, *_ = delivery_case
    commits.freeze_active(task['source_id'], '120201', 'problem')
    worker = flow(delivery_case, tmp_path, remote)
    ds, did = next(iter(remote.docs)); remote.calls = []
    with pytest.raises(ValueError):
        with worker.client.version(digest, recovery=True):
            if bad == 'enable': worker.client.request('PATCH', f'/datasets/{ds}/documents/{did}', {'enabled': 1})
            elif bad == 'upload': worker.client.upload_bytes(ds, 'new.md', b'bad')
            elif bad == 'content': worker.client.request('PATCH', f'/datasets/{ds}/documents/{did}', {'enabled': 0, 'name': 'changed'})
            elif bad == 'foreign_document': worker.client.request('PATCH', f'/datasets/{ds}/documents/foreign', {'enabled': 0})
            else: worker.client.request('PATCH', f'/datasets/{ds}/documents/{did}/chunks/foreign', {'available': False})
    assert not remote.calls


def test_historical_disable_unknown_response_keeps_barrier(delivery_case, tmp_path):
    remote = adopted(delivery_case, tmp_path)
    m, c, task, digest, commits, *_ = delivery_case
    commits.freeze_active(task['source_id'], '120201', 'problem')
    worker = flow(delivery_case, tmp_path, remote); native = remote.request
    def lose(method, endpoint, body=None, content_type=None):
        value = native(method, endpoint, body, content_type)
        if method == 'PATCH': raise TimeoutError('lost response')
        return value
    remote.request = lose
    assert worker.run()['state'] == 'frozen_pending'
    assert commits.ledger.unresolved(task['source_id'], '120201')
    remote.request = native; remote.calls = []
    assert worker.run()['state'] == 'frozen_pending'
    assert not any(method != 'GET' and path != '/retrieval' for method, path, _ in remote.calls)
    assert commits.ledger.safety(task['source_id'], '120201', digest)['state'] == 'pending'


@pytest.mark.parametrize('closed', [True, False])
def test_old_readback_uses_current_owner_and_preserves_expected(delivery_case, tmp_path, closed):
    old, remote = legacy(delivery_case, tmp_path, closed=closed)
    stop_owner(old)
    newer = advance(delivery_case)
    actions = ManagedActions(newer, tmp_path, '120201')
    actions.ex.run_id = 'legacy-readback:' + delivery_case[3]
    remote.calls = []
    result = readback(actions.config, actions.commits.ledger, actions,
                      ControlledRagClient(remote, actions), actions.identity, source_manifest=delivery_case[3])
    assert result['state'] == ('active' if closed else 'observed_pending')
    assert actions.commits.ledger.context(newer[3], '120201')['identity'] == actions.identity
    active = actions.commits.ledger.active(actions.identity['source_id'], '120201')
    assert (active['identity']['manifest_sha256'] if active else None) == (delivery_case[3] if closed else None)
    assert all(method == 'GET' or path == '/retrieval' for method, path, _ in remote.calls)
    with pytest.raises(ValueError):
        begin(actions.m, actions.ex, actions.identity, kind='rag_write', operation='http', method='POST',
              endpoint='/datasets/paper-ds/documents', object_manifest=newer[3])


def test_late_history_read_does_not_replace_new_active(delivery_case, tmp_path):
    remote = adopted(delivery_case, tmp_path)
    newer = advance(delivery_case); worker = flow(newer, tmp_path, remote)
    assert worker.run()['state'] == 'active'; release(worker)
    before = copy.deepcopy(worker.ledger.active(worker.identity['source_id'], '120201'))
    actions = ManagedActions(newer, tmp_path, '120201')
    actions.ex.run_id = 'legacy-readback:' + delivery_case[3]
    remote.calls = []
    readback(actions.config, actions.commits.ledger, actions, ControlledRagClient(remote, actions),
             actions.identity, source_manifest=delivery_case[3])
    assert worker.ledger.active(worker.identity['source_id'], '120201') == before
    with actions.m.store.db() as conn:
        assert not conn.execute('SELECT 1 FROM legacy_pending_scopes').fetchone()
    assert all(method == 'GET' or path == '/retrieval' for method, path, _ in remote.calls)


def test_archived_pending_blocks_new_write_and_cleanup(delivery_case, tmp_path):
    from paperflow.cleanup_store import CleanupLedger
    m, c, task, digest, commits, *_ = delivery_case
    assert archive_pending(delivery_case, tmp_path)['state'] == 'pending_reconcile'
    ex, identity = owner(delivery_case, tmp_path)
    with pytest.raises(ValueError, match='legacy.*pending'):
        begin(m, ex, identity, kind='rag_write', operation='http', method='POST',
              endpoint='/datasets/paper-ds/documents', object_manifest=digest)
    cleanup = CleanupLedger(m.store, c)
    with m.store.db() as conn:
        with pytest.raises(ValueError, match='legacy.*pending'):
            cleanup.remote_guard(conn, dict(manifest_sha256=digest, specialty_code='120201', source_id=task['source_id']))


def test_existing_archive_gains_barrier_when_old_view_is_upgraded(delivery_case, tmp_path):
    m, c, task, digest, commits, *_ = delivery_case
    result = archive_pending(delivery_case, tmp_path)
    with m.store.db() as conn:
        conn.execute('DROP VIEW legacy_pending_scopes')
        conn.execute("CREATE VIEW legacy_pending_scopes AS SELECT json_extract(p.identity_json,'$.source_id') AS source_id, "
                     "l.specialty_code,l.manifest_sha256 FROM legacy_targets l JOIN delivery_packages p "
                     "ON p.manifest_sha256=l.manifest_sha256 WHERE l.state!='active'")
    HandoffLedger(m.store)
    with m.store.db() as conn:
        row = conn.execute('SELECT source_id,specialty_code FROM legacy_pending_scopes').fetchone()
        assert row and tuple(row) == (task['source_id'], '120201')
    assert archive_pending(delivery_case, tmp_path) == result


@pytest.mark.parametrize('change', ['pause', 'new_expected'])
def test_old_readback_late_commit_loses_qualification(delivery_case, tmp_path, change):
    old, remote = legacy(delivery_case, tmp_path)
    stop_owner(old); newer = advance(delivery_case)
    actions = ManagedActions(newer, tmp_path, '120201')
    actions.ex.run_id = 'legacy-readback:' + delivery_case[3]
    commit = actions.commit
    def invalidate(operation, payload):
        if change == 'pause': actions.m.pause('late readback')
        else: advance(newer)
        return commit(operation, payload)
    actions.commit = invalidate
    with pytest.raises(ValueError):
        readback(actions.config, actions.commits.ledger, actions, ControlledRagClient(remote, actions),
                 actions.identity, source_manifest=delivery_case[3])
    assert actions.commits.ledger.active(actions.identity['source_id'], '120201') is None
    with actions.m.store.db() as conn:
        assert conn.execute('SELECT state FROM legacy_targets').fetchone()[0] == 'pending_reconcile'


def test_changed_source_task_can_read_old_history_without_old_write_authority(delivery_case, tmp_path, package_factory):
    from pathlib import Path
    from paperflow.handoff import PackageHandoff
    m, c, task, digest, commits, *_ = delivery_case
    old, remote = legacy(delivery_case, tmp_path); stop_owner(old)
    Path(task['source_path']).write_text('changed source bytes')
    new = m.store.register_source(task['source_path'], sha(task['source_path']))
    material, _, catalog = package_factory('changed', task_id=new['task_id'], source_sha=new['source_sha256'])
    for name in ('profile.json', 'tree.json', 'assets.json', 'chunks.json', 'review.json', 'corrections.json'):
        data = load(material / name); data['source_id'] = new['source_id']; save(material / name, data)
    (material / 'manifest.json').unlink(); (material / 'READY.json').unlink()
    handoff = PackageHandoff(m.store, c.handoff_root)
    intent = handoff.prepare(new['task_id'], new, material, catalog, mapping_version='new-task')
    result = handoff.resume(new['task_id'], intent['input_fingerprint'])
    case = m, c, new, result['manifest_sha256'], commits, material, catalog
    actions = ManagedActions(case, tmp_path, '120201'); actions.ex.run_id = 'legacy-readback:' + digest
    remote.calls = []
    result = readback(c, commits.ledger, actions, ControlledRagClient(remote, actions), actions.identity, source_manifest=digest)
    assert result['state'] == 'active'
    active = commits.ledger.active(task['source_id'], '120201')
    assert active['identity']['task_id'] == task['task_id'] != actions.identity['task_id']
    assert all(method == 'GET' or path == '/retrieval' for method, path, _ in remote.calls)


def test_old_readback_rejects_foreign_source_before_network(delivery_case, tmp_path):
    old, remote = legacy(delivery_case, tmp_path); stop_owner(old)
    newer = advance(delivery_case); actions = ManagedActions(newer, tmp_path, '120201')
    actions.ex.run_id = 'legacy-readback:' + delivery_case[3]
    with actions.m.store.db() as conn:
        identity = json.loads(conn.execute('SELECT identity_json FROM delivery_packages WHERE manifest_sha256=?', (delivery_case[3],)).fetchone()[0])
        identity['source_id'] = 'another-source'
        conn.execute('UPDATE delivery_packages SET identity_json=? WHERE manifest_sha256=?', (canonical(identity), delivery_case[3]))
    remote.calls = []
    with pytest.raises(ValueError, match='another source'):
        readback(actions.config, actions.commits.ledger, actions, ControlledRagClient(remote, actions),
                 actions.identity, source_manifest=delivery_case[3])
    assert not remote.calls


def test_archived_scope_excludes_only_affected_queue_and_status(delivery_case, tmp_path):
    from paperflow.scheduler import Scheduler
    from paperflow.legacy_readback import article_status
    m, c, task, digest, commits, *_ = delivery_case
    archive_pending(delivery_case, tmp_path); c.cleanup = False
    scheduler = Scheduler(m, c, isolation_root=tmp_path); scheduler.discover()
    codes = {json.loads(r['data'])['specialty_code'] for r in scheduler.ledger.rows("SELECT data FROM scheduler_jobs WHERE kind='delivery'")}
    assert codes == {'120202'}
    status = article_status(m.store, task['task_id'], dict(closeout_pending=0))
    assert status['pending_reconcile'] == 1 and not status['complete']

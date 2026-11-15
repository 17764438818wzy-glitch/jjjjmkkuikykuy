"""Isolated acceptance and deletion protection; all documents are synthetic."""
from pathlib import Path
import hashlib
import json
import sqlite3
import threading
import uuid

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from paperflow_prefect.first_step_control_api import install_control_routes
from paperflow_prefect.first_step_registration import register_delivery
from paperflow_prefect.group_classification import process_one_group
from paperflow_prefect.group_task_summary import make_group_summary_row
from test_control_admission import Scheduler, ready_control
from test_first_step_registration import FixtureJudge, evidence


def prepare_registered(tmp_path, *, groups=1, publish=True, attachment=False):
    control, scheduler, task_id = ready_control(tmp_path, groups=groups)
    control.legacy_conversion_root = tmp_path / 'isolated-legacy-cache'
    generation = scheduler.calls[-1][1]
    assert control.begin_run(task_id, generation, 'synthetic-only')
    seq, source = control.claim_next(task_id, generation)
    if attachment:
        (Path(source) / 'data.csv').write_text('date,value\n1,2', encoding='utf8')
    with control.summary._connect() as db:
        task = control._task(db, task_id)
        library, output = control.source_library(task), control.artifact_root(task)
    assert all(Path(value).resolve().is_relative_to(tmp_path.resolve())
               for value in (control.data, control.output, control.receipt_root,
                             control.legacy_conversion_root, source, library, output))
    item = process_one_group(Path(source), library, output, FixtureJudge, evidence, verified_new=True)
    assert item.get('delivery_complete') is True
    receipt = register_delivery(
        item['delivery_dir'], receipt_root=control.receipt_root, history_roots=(),
        lock_path=control.data / 'control-locks' / 'first-step-registration.lock')
    assert receipt['state'] == '已接收'
    item.update(registration_status='registered', receipt_path=receipt['record_path'],
                receipt_id=receipt['receipt_id'], paper_id=receipt['paper_id'])
    row, status, _ = make_group_summary_row(item, library)
    assert status == 'passed'
    if publish:
        control.finish_group(task_id, seq, generation, row)

    app = FastAPI()
    api_control = install_control_routes(app, control.output, control.data,
                                         receipt_root=control.receipt_root)
    api_control.legacy_conversion_root = control.legacy_conversion_root
    app.state.test_control = api_control
    client = TestClient(app)
    request = {'group_id': row['group_id'], 'paper_id': row['paper_id'],
               'receipt_id': row['receipt_id'], 'consumer_id': 'local-intake',
               'request_id': str(uuid.uuid4())}
    return client, control, task_id, row, receipt, request, seq


def base(task_id):
    return f'/api/v1/first-step/tasks/{task_id}/local-retentions'


def confirm_with_local_queue(client, tmp_path, task_id, row, request, retention_id):
    queue = tmp_path / 'local-queue.sqlite3'
    app_control = client.app.state.test_control
    app_control.local_intake_queue_db = queue
    receipt = json.loads(Path(row['receipt_path']).read_text(encoding='utf8'))
    body = next(item for item in receipt['items'] if item['kind'] == 'body')
    attachments = [item for item in receipt['items'] if item['kind'] == 'attachment']
    record = {key: row[key] for key in ('group_id', 'paper_id', 'receipt_id', 'receipt_path', 'delivery_dir')}
    record.update(task_id=task_id, origin_group_id=receipt.get('origin_group_id'),
                  result_version=receipt.get('result_version'),
                  receipt_sha256=hashlib.sha256(Path(row['receipt_path']).read_bytes()).hexdigest(),
                  body=body, attachments=attachments)
    delivery = Path(row['delivery_dir'])
    manifest = json.loads((delivery / 'delivery.json').read_text(encoding='utf8'))
    metadata = [Path(row['receipt_path']), delivery / 'delivery.json', delivery / 'handoff.json']
    metadata += [delivery / entry['path'] for entry in manifest['evidence_records']]
    files = [delivery / 'files' / entry['relative'] for entry in manifest['files']]
    meta_paths = {str(path.resolve()) for path in metadata}
    record['_fingerprint'] = []
    for path in sorted({path.resolve() for path in metadata + files}, key=str):
        stat = path.stat()
        digest = hashlib.sha256(path.read_bytes()).hexdigest() if str(path) in meta_paths else None
        record['_fingerprint'].append([str(path), stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns,
                                       stat.st_dev, stat.st_ino, digest])
    with sqlite3.connect(queue) as db, db:
        db.executescript('''
            CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS groups(task_id TEXT,group_id TEXT,paper_id TEXT,receipt_id TEXT,
                state TEXT,record TEXT,PRIMARY KEY(task_id,group_id));
            CREATE TABLE IF NOT EXISTS retention_requests(task_id TEXT,group_id TEXT,paper_id TEXT,
                receipt_id TEXT,consumer_id TEXT,request_id TEXT PRIMARY KEY,retention_id TEXT,
                state TEXT,last_error TEXT);
            CREATE TABLE IF NOT EXISTS claims(task_id TEXT,group_id TEXT,receipt_id TEXT,state TEXT);
        ''')
        db.execute("INSERT OR REPLACE INTO metadata VALUES('queue_schema','paperflow-local-waiting/1')")
        db.execute('INSERT OR REPLACE INTO groups VALUES(?,?,?,?,?,?)',
                   (task_id, row['group_id'], row['paper_id'], row['receipt_id'], 'waiting', json.dumps(record)))
        db.execute('INSERT OR REPLACE INTO retention_requests VALUES(?,?,?,?,?,?,?,?,?)',
                   (task_id, row['group_id'], row['paper_id'], row['receipt_id'], request['consumer_id'],
                    request['request_id'], retention_id, 'reserved', None))
    identity = {'consumer_id': request['consumer_id'], 'receipt_id': request['receipt_id']}
    return client.post(base(task_id) + f'/{retention_id}/confirm', json=identity)


def reserve_and_confirm(client, tmp_path, task_id, row, request):
    response = client.post(base(task_id), json=request)
    assert response.status_code == 200, response.text
    retention_id = response.json()['retention_id']
    response = confirm_with_local_queue(client, tmp_path, task_id, row, request, retention_id)
    assert response.status_code == 200, response.text
    return retention_id


def test_registered_group_is_protected_before_local_polls(tmp_path):
    client, control, task_id, row, receipt, _, _ = prepare_registered(tmp_path)
    delivery = Path(row['delivery_dir'])
    receipt_path = Path(receipt['record_path'])
    assert delivery.is_dir() and receipt_path.is_file()

    response = client.delete(f'/api/v1/first-step/tasks/{task_id}')

    assert response.status_code == 409, response.text
    assert delivery.is_dir() and receipt_path.is_file()
    assert control.summary.read(task_id, 'passed', 1, 10)['items'][0]['receipt_id'] == row['receipt_id']


def test_running_task_can_reserve_published_group_and_replay(tmp_path):
    client, control, task_id, row, _, request, _ = prepare_registered(tmp_path, groups=2)
    assert control.summary.read(task_id, 'passed', 1, 10)['task_status'] == 'running'
    assert row['title_status'] == '未提取'

    first = client.post(base(task_id), json=request)
    assert first.status_code == 200, first.text
    replay = client.post(base(task_id), json=request)
    assert replay.status_code == 200, replay.text
    assert first.json()['retention_id'] == replay.json()['retention_id']
    assert first.json()['state'] == replay.json()['state'] == 'reserved'
    assert first.json()['created'] is True and replay.json()['created'] is False


def test_verified_local_confirmation_does_not_allow_release(tmp_path):
    client, _, task_id, row, _, request, _ = prepare_registered(tmp_path)
    reserved = client.post(base(task_id), json=request)
    assert reserved.status_code == 200, reserved.text
    retention_id = reserved.json()['retention_id']
    identity = {'consumer_id': request['consumer_id'], 'receipt_id': request['receipt_id']}
    release_body = {**identity, 'reason': 'processed', 'confirm_not_in_use': True}

    assert client.post(base(task_id) + f'/{retention_id}/release', json=release_body).status_code == 409
    confirmed = confirm_with_local_queue(client, tmp_path, task_id, row, request, retention_id)
    assert confirmed.status_code == 200 and confirmed.json()['state'] == 'accepted'
    replay = client.post(base(task_id) + f'/{retention_id}/confirm', json=identity)
    assert replay.status_code == 200 and replay.json()['state'] == 'accepted'
    released = client.post(base(task_id) + f'/{retention_id}/release', json=release_body)
    assert released.status_code == 409
    assert client.post(base(task_id), json=request).status_code == 200
    assert client.delete(f'/api/v1/first-step/tasks/{task_id}').status_code == 409
    assert Path(row['delivery_dir']).is_dir()


def test_reconciliation_lists_explicit_references_without_scanning_receipts(tmp_path, monkeypatch):
    from paperflow_prefect import first_step_retention
    client, _, task_id, row, _, request, _ = prepare_registered(tmp_path)
    listed = client.get(base(task_id))
    assert listed.status_code == 200, listed.text
    assert listed.json()['items'] == []
    assert listed.json()['list_total'] == 0

    response = client.post(base(task_id), json=request)
    assert response.status_code == 200
    monkeypatch.setattr(first_step_retention, 'task_receipt_plan',
                        lambda *_: (_ for _ in ()).throw(AssertionError('unexpected ledger scan')))
    listed = client.get(base(task_id))
    assert listed.status_code == 200
    assert listed.json()['items'][0]['retention_id'] == response.json()['retention_id']
    assert listed.json()['items'][0]['state'] == 'reserved'
    assert listed.json()['list_total'] == 1 and listed.json()['has_next'] is False


def test_crash_after_registration_before_group_publication_is_protected(tmp_path):
    client, control, task_id, row, receipt, _, seq = prepare_registered(tmp_path, publish=False)
    with control.summary._connect() as db, db:
        db.execute("UPDATE tasks SET task_status='paused',desired_state='paused',start_pending=0 WHERE task_id=?",
                   (task_id,))
        db.execute("UPDATE control_groups SET state='pending',worker_pid=NULL WHERE task_id=? AND seq=?",
                   (task_id, seq))
    response = client.delete(f'/api/v1/first-step/tasks/{task_id}')
    assert response.status_code == 409, response.text
    assert Path(row['delivery_dir']).is_dir() and Path(receipt['record_path']).is_file()


def test_receipt_event_is_protected_even_if_materialization_was_interrupted(tmp_path):
    client, control, task_id, row, receipt, _, seq = prepare_registered(tmp_path, publish=False)
    Path(receipt['record_path']).unlink()  # only the synthetic temporary receipt
    with control.summary._connect() as db, db:
        db.execute("UPDATE tasks SET task_status='paused',desired_state='paused',start_pending=0 WHERE task_id=?",
                   (task_id,))
        db.execute("UPDATE control_groups SET state='pending',worker_pid=NULL WHERE task_id=? AND seq=?",
                   (task_id, seq))
    response = client.delete(f'/api/v1/first-step/tasks/{task_id}')
    assert response.status_code == 409, response.text
    assert Path(row['delivery_dir']).is_dir()


@pytest.mark.parametrize('break_database', ['missing', 'corrupt'])
def test_unavailable_receipt_ledger_does_not_look_empty(tmp_path, break_database):
    client, control, task_id, row, _, _, _ = prepare_registered(tmp_path)
    ledger = control.receipt_root / 'receipts.sqlite3'
    if break_database == 'missing':
        ledger.rename(ledger.with_suffix('.unavailable'))
    else:
        ledger.write_bytes(b'not-a-sqlite-database')
    response = client.delete(f'/api/v1/first-step/tasks/{task_id}')
    assert response.status_code == 503, response.text
    assert Path(row['delivery_dir']).is_dir()


def test_legacy_shared_ownership_is_not_guessed(tmp_path):
    client, control, task_id, row, _, request, _ = prepare_registered(tmp_path)
    with control.summary._connect() as db, db:
        db.execute('UPDATE tasks SET artifact_scoped=0 WHERE task_id=?', (task_id,))
    assert client.post(base(task_id), json=request).status_code == 409
    assert client.delete(f'/api/v1/first-step/tasks/{task_id}').status_code == 409
    assert Path(row['delivery_dir']).is_dir()


def test_missing_retention_registry_fails_closed_with_503(tmp_path):
    client, control, task_id, row, _, request, _ = prepare_registered(tmp_path)
    with control.summary._connect() as db, db:
        db.execute('DROP TABLE local_retention_references')
    client = TestClient(client.app, raise_server_exceptions=False)
    assert client.post(base(task_id), json=request).status_code == 503
    assert client.delete(f'/api/v1/first-step/tasks/{task_id}').status_code == 503
    assert Path(row['delivery_dir']).is_dir()


def test_missing_registry_cannot_partially_clean_a_draft_task(tmp_path):
    from paperflow_prefect.first_step_control import FirstStepControl
    control = FirstStepControl(tmp_path / 'output', tmp_path / 'data', receipt_root=tmp_path / 'receipts')
    control.legacy_conversion_root = tmp_path / 'isolated-legacy-cache'
    task_id = control.create_task('合成草稿')['task_id']
    artifact = control.output / 'tasks' / task_id / 'intermediate.txt'
    artifact.parent.mkdir(parents=True)
    artifact.write_text('仅隔离测试', encoding='utf8')
    app = FastAPI()
    api_control = install_control_routes(app, control.output, control.data, receipt_root=control.receipt_root)
    api_control.legacy_conversion_root = control.legacy_conversion_root
    with control.summary._connect() as db, db:
        db.execute('DROP TABLE local_retention_references')
    client = TestClient(app, raise_server_exceptions=False)
    assert client.delete(f'/api/v1/first-step/tasks/{task_id}').status_code == 503
    assert artifact.is_file()


@pytest.mark.parametrize('break_database', ['missing', 'corrupt'])
def test_reservation_rejects_unavailable_authoritative_receipt_ledger(tmp_path, break_database):
    client, control, task_id, row, _, request, _ = prepare_registered(tmp_path)
    ledger = control.receipt_root / 'receipts.sqlite3'
    if break_database == 'missing':
        ledger.rename(ledger.with_suffix('.unavailable'))
    else:
        ledger.write_bytes(b'not-a-sqlite-database')
    response = client.post(base(task_id), json=request)
    assert response.status_code == 503, response.text
    assert Path(row['delivery_dir']).is_dir()


def test_rolled_back_empty_ledger_after_unpublished_receipt_is_not_empty_proof(tmp_path):
    client, control, task_id, row, _, _, seq = prepare_registered(tmp_path, publish=False)
    with control.summary._connect() as db, db:
        db.execute("UPDATE tasks SET task_status='paused',desired_state='paused',start_pending=0 WHERE task_id=?",
                   (task_id,))
        db.execute("UPDATE control_groups SET state='pending',worker_pid=NULL WHERE task_id=? AND seq=?",
                   (task_id, seq))
    with sqlite3.connect(control.receipt_root / 'receipts.sqlite3') as ledger, ledger:
        ledger.execute('DELETE FROM receipt_events')
    response = client.delete(f'/api/v1/first-step/tasks/{task_id}')
    assert response.status_code == 503, response.text
    assert Path(row['delivery_dir']).is_dir()


def test_rolled_back_ledger_after_failed_registration_publication_is_not_empty_proof(tmp_path):
    client, control, task_id, row, _, _, seq = prepare_registered(tmp_path, publish=False)
    with control.summary._connect() as db:
        generation = control._task(db, task_id)['start_generation']
    row['registration_status'] = 'failed'
    control.finish_group(task_id, seq, generation, row)
    with sqlite3.connect(control.receipt_root / 'receipts.sqlite3') as ledger, ledger:
        ledger.execute('DELETE FROM receipt_events')
    response = client.delete(f'/api/v1/first-step/tasks/{task_id}')
    assert response.status_code == 503, response.text
    assert Path(row['delivery_dir']).is_dir()


def test_corrupt_published_status_cannot_bypass_receipt_protection(tmp_path):
    client, control, task_id, row, _, _, _ = prepare_registered(tmp_path)
    with control.summary._connect() as db, db:
        stored = db.execute('SELECT payload FROM group_results WHERE task_id=?', (task_id,)).fetchone()
        payload = json.loads(stored['payload'])
        payload['status'] = 'failed'
        payload['registration_status'] = 'failed'
        db.execute('UPDATE group_results SET payload=? WHERE task_id=?', (json.dumps(payload), task_id))
    with sqlite3.connect(control.receipt_root / 'receipts.sqlite3') as ledger, ledger:
        ledger.execute('DELETE FROM receipt_events')
    response = client.delete(f'/api/v1/first-step/tasks/{task_id}')
    assert response.status_code == 503, response.text
    assert Path(row['delivery_dir']).is_dir()


def test_failed_group_with_complete_delivery_and_missing_ledger_is_not_deletable(tmp_path):
    client, control, task_id, row, _, _, seq = prepare_registered(tmp_path, publish=False)
    with control.summary._connect() as db:
        generation = control._task(db, task_id)['start_generation']
    row.update(status='failed', registration_status='failed')
    control.finish_group(task_id, seq, generation, row)
    with sqlite3.connect(control.receipt_root / 'receipts.sqlite3') as ledger, ledger:
        ledger.execute('DELETE FROM receipt_events')
    response = client.delete(f'/api/v1/first-step/tasks/{task_id}')
    assert response.status_code == 503, response.text
    assert Path(row['delivery_dir']).is_dir()


def test_confirmation_requires_the_configured_authoritative_queue(tmp_path):
    client, _, task_id, _, _, request, _ = prepare_registered(tmp_path)
    response = client.post(base(task_id), json=request)
    assert response.status_code == 200
    retention_id = response.json()['retention_id']
    identity = {'consumer_id': request['consumer_id'], 'receipt_id': request['receipt_id']}
    confirmed = client.post(base(task_id) + f'/{retention_id}/confirm', json=identity)
    assert confirmed.status_code == 503, confirmed.text
    assert client.get(base(task_id)).json()['items'][0]['state'] == 'reserved'


@pytest.mark.parametrize('change,expected', [
    ('missing_group', 409), ('review', 409), ('wrong_paper', 409), ('wrong_hold', 409),
    ('wrong_receipt_hash', 409), ('wrong_body', 409), ('wrong_body_relative', 409),
    ('bad_schema', 503), ('corrupt', 503),
])
def test_confirmation_rechecks_authoritative_queue(tmp_path, change, expected):
    client, _, task_id, row, _, request, _ = prepare_registered(tmp_path)
    retention_id = reserve_and_confirm(client, tmp_path, task_id, row, request)
    queue = tmp_path / 'local-queue.sqlite3'
    if change == 'corrupt':
        queue.write_bytes(b'not-a-sqlite-database')
    else:
        with sqlite3.connect(queue) as db, db:
            if change == 'missing_group':
                db.execute('DELETE FROM groups')
            elif change == 'review':
                db.execute("UPDATE groups SET state='needs_review'")
            elif change == 'wrong_paper':
                db.execute("UPDATE groups SET paper_id='PF_wrong'")
            elif change == 'wrong_hold':
                db.execute("UPDATE retention_requests SET retention_id='other'")
            elif change == 'bad_schema':
                db.execute("UPDATE metadata SET value='other' WHERE key='queue_schema'")
            else:
                raw = db.execute('SELECT record FROM groups').fetchone()[0]
                record = json.loads(raw)
                if change == 'wrong_receipt_hash':
                    record['receipt_sha256'] = 'a' * 64
                elif change == 'wrong_body':
                    record['body']['upstream_file_id'] = 'wrong'
                else:
                    record['body']['relative'] = 'other.txt'
                db.execute('UPDATE groups SET record=?', (json.dumps(record),))
    identity = {'consumer_id': request['consumer_id'], 'receipt_id': request['receipt_id']}
    confirmed = client.post(base(task_id) + f'/{retention_id}/confirm', json=identity)
    assert confirmed.status_code == expected, confirmed.text
    assert client.delete(f'/api/v1/first-step/tasks/{task_id}').status_code == 409
    assert Path(row['delivery_dir']).is_dir()


def test_confirmation_processing_state_requires_matching_claim(tmp_path):
    client, _, task_id, row, _, request, _ = prepare_registered(tmp_path)
    retention_id = reserve_and_confirm(client, tmp_path, task_id, row, request)
    queue = tmp_path / 'local-queue.sqlite3'
    with sqlite3.connect(queue) as db, db:
        db.execute("UPDATE groups SET state='processing'")
    identity = {'consumer_id': request['consumer_id'], 'receipt_id': request['receipt_id']}
    url = base(task_id) + f'/{retention_id}/confirm'
    assert client.post(url, json=identity).status_code == 409
    with sqlite3.connect(queue) as db, db:
        db.execute('INSERT INTO claims VALUES(?,?,?,?)', (task_id, row['group_id'], row['receipt_id'], 'processing'))
    assert client.post(url, json=identity).status_code == 200


def test_confirmation_refuses_file_changed_after_local_acceptance(tmp_path):
    client, _, task_id, row, _, request, _ = prepare_registered(tmp_path)
    retention_id = reserve_and_confirm(client, tmp_path, task_id, row, request)
    receipt = json.loads(Path(row['receipt_path']).read_text(encoding='utf8'))
    body_path = Path(next(item for item in receipt['items'] if item['kind'] == 'body')['delivered_path'])
    body_path.write_bytes(b'different synthetic data')
    identity = {'consumer_id': request['consumer_id'], 'receipt_id': request['receipt_id']}
    confirmed = client.post(base(task_id) + f'/{retention_id}/confirm', json=identity)
    assert confirmed.status_code == 409, confirmed.text


def test_confirmation_rechecks_fingerprint_immediately_before_commit(tmp_path, monkeypatch):
    from paperflow_prefect import first_step_retention
    client, _, task_id, row, _, request, _ = prepare_registered(tmp_path)
    retention_id = reserve_and_confirm(client, tmp_path, task_id, row, request)
    receipt = json.loads(Path(row['receipt_path']).read_text(encoding='utf8'))
    body_path = Path(next(item for item in receipt['items'] if item['kind'] == 'body')['delivered_path'])
    original = first_step_retention._assert_local_acceptance
    def change_after_check(*args):
        snapshot = original(*args)
        body_path.write_bytes(b'changed after first check')
        return snapshot
    monkeypatch.setattr(first_step_retention, '_assert_local_acceptance', change_after_check)
    identity = {'consumer_id': request['consumer_id'], 'receipt_id': request['receipt_id']}
    response = client.post(base(task_id) + f'/{retention_id}/confirm', json=identity)
    assert response.status_code == 409, response.text


@pytest.mark.parametrize('damage', ['missing_metadata_hash', 'missing_attachment_evidence'])
def test_confirmation_requires_complete_metadata_fingerprints(tmp_path, damage):
    client, _, task_id, row, _, request, _ = prepare_registered(tmp_path, attachment=True)
    retention_id = reserve_and_confirm(client, tmp_path, task_id, row, request)
    queue = tmp_path / 'local-queue.sqlite3'
    with sqlite3.connect(queue) as db, db:
        record = json.loads(db.execute('SELECT record FROM groups').fetchone()[0])
        if damage == 'missing_metadata_hash':
            next(entry for entry in record['_fingerprint'] if entry[0].endswith('handoff.json'))[6] = None
        else:
            record['_fingerprint'] = [entry for entry in record['_fingerprint']
                                      if 'attachment-evidence' not in entry[0]]
        db.execute('UPDATE groups SET record=?', (json.dumps(record),))
    identity = {'consumer_id': request['consumer_id'], 'receipt_id': request['receipt_id']}
    response = client.post(base(task_id) + f'/{retention_id}/confirm', json=identity)
    assert response.status_code == 409, response.text


def test_file_verification_does_not_hold_the_upstream_task_lock(tmp_path, monkeypatch):
    from filelock import Timeout
    from paperflow_prefect import first_step_retention
    client, control, task_id, row, _, request, _ = prepare_registered(tmp_path)
    retention_id = reserve_and_confirm(client, tmp_path, task_id, row, request)
    original = first_step_retention._assert_local_acceptance
    inside, proceed = threading.Event(), threading.Event()
    def slow_check(*args):
        inside.set()
        assert proceed.wait(5)
        return original(*args)
    monkeypatch.setattr(first_step_retention, '_assert_local_acceptance', slow_check)
    identity = {'consumer_id': request['consumer_id'], 'receipt_id': request['receipt_id']}
    result = {}
    thread = threading.Thread(target=lambda: result.setdefault('response',
        client.post(base(task_id) + f'/{retention_id}/confirm', json=identity)))
    thread.start()
    assert inside.wait(5)
    lock = control._lock('task', task_id)
    try:
        try:
            lock.acquire(timeout=.3)
            acquired = True
        except Timeout:
            acquired = False
    finally:
        if lock.is_locked:
            lock.release()
        proceed.set()
        thread.join(10)
    assert acquired, 'full verification held the task lock'
    assert result['response'].status_code == 200


def test_reference_listing_pages_stably_without_receipt_scan(tmp_path, monkeypatch):
    from paperflow_prefect import first_step_retention
    client, control, task_id, row, _, request, _ = prepare_registered(tmp_path)
    assert client.post(base(task_id), json=request).status_code == 200
    with control.summary._connect() as db, db:
        for index in range(204):
            db.execute('INSERT INTO local_retention_references '
                       '(retention_id,request_id,task_id,group_id,paper_id,receipt_id,consumer_id,state,'
                       'created_at_ns,updated_at_ns) VALUES(?,?,?,?,?,?,?,?,?,?)',
                       (str(uuid.uuid4()), str(uuid.uuid4()), task_id, row['group_id'], row['paper_id'],
                        row['receipt_id'], 'local-intake', 'reserved', index, index))
    monkeypatch.setattr(first_step_retention, 'task_receipt_plan',
                        lambda *_: (_ for _ in ()).throw(AssertionError('unexpected ledger scan')))
    first = client.get(base(task_id), params={'page': 1, 'page_size': 200}).json()
    second = client.get(base(task_id), params={'page': 2, 'page_size': 200}).json()
    assert first['list_total'] == second['list_total'] == 205
    assert len(first['items']) == 200 and len(second['items']) == 5
    assert first['has_next'] is True and second['has_next'] is False
    assert first['revision'] == second['revision']
    assert len({item['retention_id'] for item in first['items'] + second['items']}) == 205
    with control.summary._connect() as db, db:
        db.execute("UPDATE local_retention_references SET state='accepted' WHERE retention_id=?",
                   (second['items'][0]['retention_id'],))
    assert client.get(base(task_id), params={'page': 2}).json()['revision'] > first['revision']
    assert client.get(base(task_id), params={'page_size': 201}).status_code == 422


def test_missing_receipt_event_does_not_override_an_active_reference(tmp_path):
    client, control, task_id, row, _, request, _ = prepare_registered(tmp_path)
    assert client.post(base(task_id), json=request).status_code == 200
    with sqlite3.connect(control.receipt_root / 'receipts.sqlite3') as ledger, ledger:
        ledger.execute('DELETE FROM receipt_events')
    with control.summary._connect() as db, db:
        db.execute('DELETE FROM group_results WHERE task_id=?', (task_id,))
    assert client.delete(f'/api/v1/first-step/tasks/{task_id}').status_code == 409
    assert Path(row['delivery_dir']).is_dir()


def test_caller_cannot_release_waiting_data_by_claiming_it_is_processed(tmp_path):
    client, _, task_id, row, _, request, _ = prepare_registered(tmp_path)
    response = client.post(base(task_id), json=request)
    retention_id = response.json()['retention_id']
    identity = {'consumer_id': request['consumer_id'], 'receipt_id': request['receipt_id']}
    assert confirm_with_local_queue(client, tmp_path, task_id, row, request, retention_id).status_code == 200
    release = {**identity, 'reason': 'processed', 'confirm_not_in_use': True}
    response = client.post(base(task_id) + f'/{retention_id}/release', json=release)
    assert response.status_code == 409, response.text
    assert client.delete(f'/api/v1/first-step/tasks/{task_id}').status_code == 409


def test_inconsistent_accepted_ledger_event_cannot_be_silently_deleted(tmp_path):
    from paperflow_prefect.records import identity
    client, control, task_id, row, receipt, _, seq = prepare_registered(tmp_path, publish=False)
    with control.summary._connect() as db, db:
        db.execute("UPDATE tasks SET task_status='paused',desired_state='paused',start_pending=0 WHERE task_id=?",
                   (task_id,))
        db.execute("UPDATE control_groups SET state='pending',worker_pid=NULL WHERE task_id=? AND seq=?",
                   (task_id, seq))
    # Synthetic, internally keyed event whose accepted state has no received body.
    payload = {key: value for key, value in receipt.items() if key not in {'receipt_id', 'record_path'}}
    payload['items'] = []
    new_id = identity(payload)
    payload.update(receipt_id=new_id, record_path=str(control.receipt_root / 'receipts' / (new_id + '.json')))
    with sqlite3.connect(control.receipt_root / 'receipts.sqlite3') as ledger, ledger:
        ledger.execute('UPDATE receipt_events SET receipt_id=?,payload=? WHERE receipt_id=?',
                       (new_id, json.dumps(payload, ensure_ascii=False), receipt['receipt_id']))
    client = TestClient(client.app, raise_server_exceptions=False)
    response = client.delete(f'/api/v1/first-step/tasks/{task_id}')
    assert response.status_code == 503, response.text
    assert Path(row['delivery_dir']).is_dir()


def test_conflicting_request_or_incomplete_identity_is_not_reserved(tmp_path):
    client, _, task_id, row, _, request, _ = prepare_registered(tmp_path)
    assert client.post(base(task_id), json={**request, 'paper_id': 'PF_WRONG'}).status_code == 409
    assert client.post(base(task_id), json={**request, 'receipt_id': 'a' * 64}).status_code == 409
    assert client.post(base(task_id), json=request).status_code == 200
    response = client.post(base(task_id), json={**request, 'group_id': 'b' * 32})
    assert response.status_code == 409
    assert client.delete(f'/api/v1/first-step/tasks/{task_id}').status_code == 409
    assert Path(row['delivery_dir']).is_dir()


def test_all_active_references_remain_protected(tmp_path):
    client, _, task_id, row, _, first, _ = prepare_registered(tmp_path)
    second = {**first, 'request_id': str(uuid.uuid4())}
    first_id = reserve_and_confirm(client, tmp_path, task_id, row, first)
    second_id = reserve_and_confirm(client, tmp_path, task_id, row, second)
    assert client.delete(f'/api/v1/first-step/tasks/{task_id}').status_code == 409
    assert Path(row['delivery_dir']).is_dir()
    for retention_id in (first_id, second_id):
        release = {'consumer_id': 'local-intake', 'receipt_id': row['receipt_id'],
                   'reason': 'processed', 'confirm_not_in_use': True}
        assert client.post(base(task_id) + f'/{retention_id}/release', json=release).status_code == 409
    assert client.delete(f'/api/v1/first-step/tasks/{task_id}').status_code == 409


def test_shared_paper_identity_does_not_release_another_tasks_copy(tmp_path):
    from paperflow_prefect.first_step_control import FirstStepControl
    source = tmp_path / 'original' / 'person'
    source.mkdir(parents=True)
    (source / 'paper.txt').write_text('合成正文', encoding='utf8')
    scheduler = Scheduler()
    control = FirstStepControl(tmp_path / 'output', tmp_path / 'data', scheduler,
                               receipt_root=tmp_path / 'receipts')
    control.legacy_conversion_root = tmp_path / 'isolated-legacy-cache'

    def produce(name):
        task_id = control.create_task(name)['task_id']
        ready = control.select_local_source(task_id, str(source.parent))
        control.start(task_id, ready['control_revision'])
        generation = scheduler.calls[-1][1]
        assert control.begin_run(task_id, generation, 'synthetic-only')
        seq, group = control.claim_next(task_id, generation)
        with control.summary._connect() as db:
            task = control._task(db, task_id)
            library, output = control.source_library(task), control.artifact_root(task)
        item = process_one_group(Path(group), library, output, FixtureJudge, evidence, verified_new=True)
        receipt = register_delivery(item['delivery_dir'], receipt_root=control.receipt_root, history_roots=(),
                                    lock_path=control.data / 'control-locks' / 'first-step-registration.lock')
        item.update(registration_status='registered', receipt_path=receipt['record_path'],
                    receipt_id=receipt['receipt_id'], paper_id=receipt['paper_id'])
        row, status, _ = make_group_summary_row(item, library)
        assert status == 'passed'
        control.finish_group(task_id, seq, generation, row)
        return task_id, row

    first_task, first = produce('甲')
    second_task, second = produce('乙')
    assert first['paper_id'] == second['paper_id'] and first['receipt_id'] != second['receipt_id']
    app = FastAPI()
    api_control = install_control_routes(app, control.output, control.data, receipt_root=control.receipt_root)
    api_control.legacy_conversion_root = control.legacy_conversion_root
    app.state.test_control = api_control
    client = TestClient(app)
    request = {'group_id': second['group_id'], 'paper_id': second['paper_id'],
               'receipt_id': second['receipt_id'], 'consumer_id': 'local-intake',
               'request_id': str(uuid.uuid4())}
    reserve_and_confirm(client, tmp_path, second_task, second, request)
    assert client.delete(f'/api/v1/first-step/tasks/{first_task}').status_code == 409
    assert client.delete(f'/api/v1/first-step/tasks/{second_task}').status_code == 409
    assert Path(first['delivery_dir']).is_dir() and (source / 'paper.txt').is_file()


def test_reservation_survives_reconstructed_control(tmp_path):
    from paperflow_prefect.first_step_control import FirstStepControl
    client, control, task_id, row, _, request, _ = prepare_registered(tmp_path)
    initial = client.post(base(task_id), json=request)
    assert initial.status_code == 200
    restarted = FirstStepControl(control.output, control.data, receipt_root=control.receipt_root)
    restarted.legacy_conversion_root = tmp_path / 'isolated-legacy-cache'
    app = FastAPI()
    api_control = install_control_routes(app, restarted.output, restarted.data, receipt_root=restarted.receipt_root)
    api_control.legacy_conversion_root = restarted.legacy_conversion_root
    client_after = TestClient(app)
    replay = client_after.post(base(task_id), json=request)
    assert replay.status_code == 200 and replay.json()['retention_id'] == initial.json()['retention_id']
    assert client_after.delete(f'/api/v1/first-step/tasks/{task_id}').status_code == 409
    assert Path(row['delivery_dir']).is_dir()


def test_interrupted_authorized_delete_can_be_safely_retried(tmp_path, monkeypatch):
    from paperflow_prefect import first_step_delete
    from paperflow_prefect.first_step_control import FirstStepControl
    control = FirstStepControl(tmp_path / 'output', tmp_path / 'data', receipt_root=tmp_path / 'receipts')
    control.legacy_conversion_root = tmp_path / 'isolated-legacy-cache'
    task_id = control.create_task('合成草稿')['task_id']
    artifact = control.output / 'tasks' / task_id / 'synthetic.txt'
    artifact.parent.mkdir(parents=True)
    artifact.write_text('synthetic', encoding='utf8')
    app = FastAPI()
    api_control = install_control_routes(app, control.output, control.data, receipt_root=control.receipt_root)
    api_control.legacy_conversion_root = control.legacy_conversion_root
    client = TestClient(app)
    original_remove = first_step_delete._remove_path
    calls = []
    def fail_once(*args):
        calls.append(True)
        if len(calls) == 1:
            raise OSError('synthetic interruption during draft cleanup')
        return original_remove(*args)
    monkeypatch.setattr(first_step_delete, '_remove_path', fail_once)
    failed = client.delete(f'/api/v1/first-step/tasks/{task_id}')
    assert failed.status_code == 500 and artifact.is_file()
    assert control.summary.read(task_id, None, 1, 10)['task_status'] == 'delete_failed'
    retried = client.delete(f'/api/v1/first-step/tasks/{task_id}')
    assert retried.status_code == 200, retried.text
    assert not artifact.exists()


def test_reserve_first_prevents_a_simultaneous_delete(tmp_path, monkeypatch):
    from paperflow_prefect.first_step_control import ControlError
    from paperflow_prefect.first_step_delete import delete_task
    from paperflow_prefect.first_step_retention import reserve
    client, control, task_id, row, _, first, _ = prepare_registered(tmp_path)
    second = first
    locked, proceed, delete_started = threading.Event(), threading.Event(), threading.Event()
    original_lock = control._lock

    def instrumented_lock(prefix, identifier):
        inner = original_lock(prefix, identifier)
        if prefix != 'task' or threading.current_thread().name != 'synthetic-reserve':
            return inner
        class WaitInsideLock:
            def __enter__(self):
                inner.acquire()
                locked.set()
                assert proceed.wait(5)
                return inner
            def __exit__(self, *args):
                inner.release()
        return WaitInsideLock()

    monkeypatch.setattr(control, '_lock', instrumented_lock)
    outcome = {}
    def receive():
        try:
            outcome['receive'] = reserve(control, task_id, second['group_id'], second['paper_id'],
                                          second['receipt_id'], second['consumer_id'], second['request_id'])
        except Exception as exc:
            outcome['receive'] = exc
    def remove():
        delete_started.set()
        try: outcome['delete'] = delete_task(control, task_id)
        except Exception as exc: outcome['delete'] = exc
    a = threading.Thread(target=receive, name='synthetic-reserve')
    b = threading.Thread(target=remove, name='synthetic-delete')
    a.start()
    assert locked.wait(5)
    b.start()
    assert delete_started.wait(5)
    proceed.set()
    a.join(10); b.join(10)
    assert not a.is_alive() and not b.is_alive()
    assert isinstance(outcome['receive'], dict) and outcome['receive']['state'] == 'reserved'
    assert isinstance(outcome['delete'], ControlError) and outcome['delete'].status == 409
    assert Path(row['delivery_dir']).is_dir()


def test_delete_first_cannot_be_followed_by_stale_reservation(tmp_path, monkeypatch):
    from paperflow_prefect.first_step_control import ControlError, FirstStepControl
    from paperflow_prefect.first_step_delete import delete_task
    from paperflow_prefect.first_step_retention import reserve
    from paperflow_prefect import first_step_delete
    control = FirstStepControl(tmp_path / 'output', tmp_path / 'data', receipt_root=tmp_path / 'receipts')
    control.legacy_conversion_root = tmp_path / 'isolated-legacy-cache'
    task_id = control.create_task('合成草稿')['task_id']
    artifact = control.output / 'tasks' / task_id / 'synthetic.txt'
    artifact.parent.mkdir(parents=True)
    artifact.write_text('synthetic', encoding='utf8')
    second = {'group_id': 'a' * 32, 'paper_id': 'PF_synthetic', 'receipt_id': 'b' * 64,
              'consumer_id': 'local-intake', 'request_id': str(uuid.uuid4())}
    locked, proceed, receive_started = threading.Event(), threading.Event(), threading.Event()
    original_remove = first_step_delete._remove_path

    def wait_before_remove(*args):
        if not locked.is_set():
            locked.set()
            assert proceed.wait(5)
        return original_remove(*args)
    monkeypatch.setattr(first_step_delete, '_remove_path', wait_before_remove)
    outcome = {}
    def remove():
        try: outcome['delete'] = delete_task(control, task_id)
        except Exception as exc: outcome['delete'] = exc
    def receive():
        receive_started.set()
        try:
            outcome['receive'] = reserve(control, task_id, second['group_id'], second['paper_id'],
                                          second['receipt_id'], second['consumer_id'], second['request_id'])
        except Exception as exc: outcome['receive'] = exc
    a = threading.Thread(target=remove, name='synthetic-delete')
    b = threading.Thread(target=receive, name='synthetic-reserve')
    a.start()
    assert locked.wait(5)
    b.start()
    assert receive_started.wait(5)
    proceed.set()
    a.join(10); b.join(10)
    assert not a.is_alive() and not b.is_alive()
    assert isinstance(outcome['delete'], dict) and outcome['delete']['deleted'] is True
    assert isinstance(outcome['receive'], ControlError) and outcome['receive'].status == 404

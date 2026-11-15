"""Verify frozen folder handoff and selected Prefect identity without dispatch."""
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3
import sys
from types import SimpleNamespace
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import pytest


HERE = Path(__file__).resolve().parents[1]
ROOT = HERE.parents[1]
sys.path[:0] = [str(HERE), str(ROOT / 'candidates/local-intake-live-link-20261031')]
spec = importlib.util.spec_from_file_location('upstream_binding', HERE / 'upstream_binding.py')
binding = importlib.util.module_from_spec(spec)
spec.loader.exec_module(binding)


def ref(path, data):
    raw = json.dumps(data, ensure_ascii=False).encode('utf8')
    path.write_bytes(raw)
    return {'path': str(path), 'sha256': hashlib.sha256(raw).hexdigest()}


class State:
    def __init__(self, running=True): self.running = running
    def is_final(self): return False
    def is_cancelling(self): return False
    def is_running(self): return self.running


@pytest.fixture
def bound(tmp_path):
    identity = {'task_id': str(uuid4()), 'group_id': uuid4().hex, 'paper_id': 'paper-1',
                'receipt_id': 'e' * 64, 'attempt_id': 'attempt-1',
                'parent_run_id': str(uuid4()), 'task_run_id': str(uuid4())}
    identity['task_run_id'] = str(uuid5(NAMESPACE_URL, 'paperflow-native-folder-ticket/1/' +
                                        identity['parent_run_id'] + '/' + identity['group_id']))
    body = {'file_id': 'body-1', 'sha256': 'a' * 64}
    attachments = [{'file_id': 'attachment-1', 'sha256': 'b' * 64}]
    paper = {'paper_id': identity['paper_id'], 'revision': 'rev-1',
             'body': body, 'attachments': attachments}
    receipt = ref(tmp_path / 'receipt.json', {'receipt': 'synthetic'})
    material = ref(tmp_path / 'material.json', {'source': {'source_sha256': body['sha256']}})
    reading = ref(tmp_path / 'reading.json', {'source': {'source_sha256': body['sha256']}, 'blocks': []})
    manifest = {'schema': 'paperflow-local-preprocessing-handoff/1',
                **{key: identity[key] for key in ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id')},
                'consumer_id': 'prefect-local-preprocessing', 'revision': paper['revision'],
                'body': body, 'attachments': attachments, 'receipt': receipt, 'material': material,
                'reading_material': reading, 'body_work': str(tmp_path), 'body_text': None, 'index': None, 'gaps': []}
    manifest['capacity'] = {'status': 'planned', 'mode': 'single', 'parts': 1,
        'plan_identity': {'schema': 'paperflow-body-input-plan/1', 'source_sha256': body['sha256'],
          'reading_sha256': reading['sha256'], 'plan_sha256': '5' * 64, 'coverage_sha256': '6' * 64,
          'mode': 'single', 'part_count': 1, 'parts': [{'id': 'part-001', 'request_sha256': '1' * 64}]}}
    handoff = ref(tmp_path / 'handoff.json', manifest)
    handoff_record = {'manifest_path': handoff['path'], 'manifest_sha256': handoff['sha256']}
    verified_receipt = {**identity, 'receipt_path': receipt['path'], 'receipt_sha256': receipt['sha256'], 'paper': paper}
    db_path = tmp_path / 'queue.sqlite3'
    with closing(sqlite3.connect(db_path)) as db, db:
        db.executescript('''
            CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT);
            CREATE TABLE cancelled_tasks(task_id TEXT PRIMARY KEY);
            CREATE TABLE tasks(task_id TEXT PRIMARY KEY,prefect_run_id TEXT,prefect_order_model TEXT,
                desired_state TEXT,start_generation INTEGER,start_pending INTEGER,task_status TEXT);
            CREATE TABLE task_deletions(task_id TEXT PRIMARY KEY);
            CREATE TABLE llm_request_admissions(task_id TEXT,task_run_id TEXT,kind TEXT,state TEXT);
            CREATE TABLE groups(task_id TEXT,group_id TEXT,paper_id TEXT,receipt_id TEXT,state TEXT,checked_session TEXT);
            CREATE TABLE claims(consumer_id TEXT,attempt_id TEXT,task_id TEXT,group_id TEXT,receipt_id TEXT,state TEXT);
            CREATE TABLE preprocessing_handoffs(receipt_id TEXT,task_id TEXT,group_id TEXT,paper_id TEXT,
                consumer_id TEXT,attempt_id TEXT,state TEXT,handoff TEXT,created_at TEXT);
            CREATE TABLE native_folder_tickets(task_id TEXT,group_id TEXT,paper_id TEXT,receipt_id TEXT,
                attempt_id TEXT,parent_run_id TEXT,task_run_id TEXT,bound INTEGER);
            CREATE TABLE retention_requests(task_id TEXT,group_id TEXT,paper_id TEXT,receipt_id TEXT,
                consumer_id TEXT,state TEXT,retention_id TEXT,last_error TEXT,checked_session TEXT);
        ''')
        db.executemany('INSERT INTO metadata VALUES(?,?)', [('queue_schema','paperflow-local-waiting/1'),
                       ('receiver_status','running'), ('receiver_session_id','session-1'),
                       ('receiver_heartbeat_at', datetime.now(timezone.utc).isoformat())])
        db.execute('INSERT INTO tasks VALUES(?,?,?,?,?,?,?)',
                   (identity['task_id'],identity['parent_run_id'],'v2','running',1,0,'complete'))
        db.execute('INSERT INTO groups VALUES(?,?,?,?,?,?)',
                   (identity['task_id'],identity['group_id'],identity['paper_id'],identity['receipt_id'],
                    'ready_for_llm','session-1'))
        db.execute('INSERT INTO claims VALUES(?,?,?,?,?,?)', ('prefect-local-preprocessing',identity['attempt_id'],
                    identity['task_id'],identity['group_id'],identity['receipt_id'],'completed'))
        db.execute('INSERT INTO preprocessing_handoffs VALUES(?,?,?,?,?,?,?,?,?)',
                   (identity['receipt_id'],identity['task_id'],identity['group_id'],identity['paper_id'],
                    'prefect-local-preprocessing',identity['attempt_id'],'waiting',json.dumps(handoff_record),'now'))
        db.execute('INSERT INTO native_folder_tickets VALUES(?,?,?,?,?,?,?,?)',
                   tuple(identity[k] for k in ('task_id','group_id','paper_id','receipt_id','attempt_id',
                                                'parent_run_id','task_run_id')) + (1,))
        db.execute('INSERT INTO retention_requests VALUES(?,?,?,?,?,?,?,?,?)',
                   (identity['task_id'],identity['group_id'],identity['paper_id'],identity['receipt_id'],
                    'local-intake','accepted','retained',None,'session-1'))
    parent = SimpleNamespace(id=UUID(identity['parent_run_id']), deployment_id=uuid4(), parent_task_run_id=None,
            state=State(), idempotency_key='first-step-order-v2-' + identity['task_id'],
            parameters={'task_id': identity['task_id']})
    child = SimpleNamespace(id=UUID(identity['task_run_id']), flow_run_id=parent.id,
            dynamic_key=identity['group_id'], task_key='folder-executable', state=State())
    deployment = SimpleNamespace(tags=['paperflow-order-v2-live-20261031'])
    client = SimpleNamespace(api_url='http://127.0.0.1:4200/api/',
            read_flow_run=lambda _: parent, read_task_run=lambda _: child, read_deployment=lambda _: deployment)
    verifier = binding.UpstreamBinding(db_path, db_path, client, 'http://127.0.0.1:4200/api', 'folder-executable',
                                        'local-intake', lambda *_: verified_receipt)
    return verifier, db_path, identity, manifest, client, parent, child, deployment


def test_reads_only_bound_handoff_from_new_parent(bound):
    verifier, _, identity, manifest, *_ = bound
    assert verifier(identity) == manifest


@pytest.mark.parametrize('statement', [
    "UPDATE groups SET checked_session='old'",
    "UPDATE retention_requests SET state='reserved'",
    "UPDATE claims SET state='processing'",
    "UPDATE native_folder_tickets SET bound=0",
    "UPDATE preprocessing_handoffs SET state='other'",
    "INSERT INTO cancelled_tasks SELECT task_id FROM groups",
    "UPDATE metadata SET value='2000-01-01T00:00:00+00:00' WHERE key='receiver_heartbeat_at'",
])
def test_unavailable_source_or_unbound_ticket_fails_closed(bound, statement):
    verifier, database, identity, *_ = bound
    with closing(sqlite3.connect(database)) as db, db:
        db.execute(statement)
    with pytest.raises(ValueError):
        verifier(identity)


def test_wrong_prefect_service_parent_or_task_never_accepted(bound):
    verifier, _, identity, _, client, parent, child, deployment = bound
    client.api_url = 'http://old-prefect/api'
    with pytest.raises(ValueError, match='Prefect'):
        verifier(identity)
    client.api_url = 'http://127.0.0.1:4200/api/'
    deployment.tags = ['old-deployment']
    with pytest.raises(ValueError, match='Prefect'):
        verifier(identity)
    deployment.tags = ['paperflow-order-v2-live-20261031']
    child.task_key = 'paperflow-folder-waiting-v2'
    with pytest.raises(ValueError, match='工单'):
        verifier(identity)


def test_changed_handoff_is_rejected(bound):
    verifier, _, identity, manifest, *_ = bound
    Path(manifest['reading_material']['path']).write_text('{}', encoding='utf8')
    with pytest.raises(ValueError, match='阅读材料'):
        verifier(identity)


def test_real_binding_checks_again_before_folder_can_begin(bound, tmp_path):
    from folder_ledger import FolderLedger
    verifier, database, identity, _, *_ = bound
    locks = tmp_path / 'locks'
    locks.mkdir()
    ledger = FolderLedger(database, locks)
    ledger.initialize()
    assert ledger.register(identity, verifier)['state'] == 'waiting_llm'
    with closing(sqlite3.connect(database)) as db, db:
        db.execute("UPDATE retention_requests SET state='invalid'")
    with pytest.raises(ValueError, match='交接'):
        ledger.begin(identity, 1, verifier)
    assert ledger.counts(identity['task_id'])['waiting_llm'] == 1


@pytest.mark.parametrize('which', ['parent', 'child'])
def test_paused_prefect_order_never_begins_a_folder(bound, tmp_path, which):
    from folder_ledger import FolderLedger
    verifier, database, identity, _, _, parent, child, _ = bound
    locks = tmp_path / 'locks'
    locks.mkdir()
    ledger = FolderLedger(database, locks)
    ledger.initialize()
    ledger.register(identity, verifier)
    (parent if which == 'parent' else child).state = State(running=False)
    with pytest.raises(InterruptedError):
        ledger.begin(identity, 1, verifier)
    assert ledger.counts(identity['task_id'])['waiting_llm'] == 1

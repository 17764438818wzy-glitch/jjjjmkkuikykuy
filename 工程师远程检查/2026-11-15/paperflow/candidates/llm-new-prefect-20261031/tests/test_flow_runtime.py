"""Bridge the real folder identity into the LLM executor, without a model."""
from contextlib import closing
import sqlite3
import sys
from types import ModuleType, SimpleNamespace
from uuid import uuid4

import pytest

from test_flow_executor import HERE
sys.path[:0] = [str(HERE), str(HERE.parents[1] / 'prefect-paperflow')]
import flow_runtime


@pytest.mark.parametrize('running', [True, False])
def test_runtime_resolves_existing_folder_and_honors_task_pause(tmp_path, monkeypatch, running):
    from prefect.client import orchestration
    from paperflow_prefect import attachment_queue, legacy, registered_input, resources
    task_id, group_id, run_id = str(uuid4()), uuid4().hex, str(uuid4())
    parent_id = str(uuid4())
    database, lock_root = tmp_path / 'control.db', tmp_path / 'locks'
    lock_root.mkdir()
    with closing(sqlite3.connect(database)) as db, db:
        db.executescript('CREATE TABLE tasks(task_id TEXT,prefect_run_id TEXT,prefect_order_model TEXT,'
                         'desired_state TEXT,start_generation INTEGER,start_pending INTEGER,task_status TEXT);'
                         'CREATE TABLE task_deletions(task_id TEXT);')
        db.execute('INSERT INTO tasks VALUES(?,?,?,?,?,?,?)',
                   (task_id, parent_id, 'v2', 'running' if running else 'paused', 4, 0, 'complete'))
    queue_path = tmp_path / 'queue.db'
    queue_path.touch()
    class Queue:
        path, consumer_id = queue_path, 'local-intake'
        def completed_preprocessing(self, _, attempt):
            return {'task_id':task_id, 'group_id':group_id, 'receipt_id':'e' * 64,
                    'attempt_id':attempt, 'paper_id':'paper-1'}
    namespace = '_synthetic_flow_runtime'
    local_queue = ModuleType(namespace + '.local_queue')
    local_queue._current_receiver_session = lambda _: 'session'
    local_preprocessing = ModuleType('paperflow_prefect.local_preprocessing')
    local_preprocessing._control_paths = lambda: (database, lock_root)
    local_preprocessing._queue = lambda: Queue()
    local_preprocessing.NAMESPACE = namespace
    monkeypatch.setitem(sys.modules, namespace + '.local_queue', local_queue)
    monkeypatch.setitem(sys.modules, 'paperflow_prefect.local_preprocessing', local_preprocessing)
    calls = []
    class Client:
        def __init__(self, url, **_): calls.append(('client',url))
        def __enter__(self): return self
        def __exit__(self, *_): return False
    monkeypatch.setattr(orchestration, 'SyncPrefectClient', Client)
    monkeypatch.setattr(attachment_queue, 'settings_for_verified_receipt', lambda settings, _: settings)
    monkeypatch.setattr(registered_input, 'verify_registered_receipt', lambda *_: None)
    monkeypatch.setattr(resources, 'cancellable_concurrency', lambda *_args, **_kw: None)
    monkeypatch.setattr(legacy, 'connection', lambda _: calls.append('credentials') or {'model':'synthetic'})
    class Binding:
        def __init__(self, *_args, **_kwargs): pass
        def load(self, given, *, require_running):
            calls.append(('verify',given['task_run_id'],require_running))
            return {'registered':{}}
    class Guard:
        def __init__(self, *_): pass
        def initialize(self): pass
    class Ledger(Guard): pass
    class Operations:
        def __init__(self, identity, *_args): calls.append(('operations',identity['task_run_id']))
    monkeypatch.setattr(flow_runtime, 'UpstreamFlowBinding', Binding)
    monkeypatch.setattr(flow_runtime, 'AdmissionGuard', Guard)
    monkeypatch.setattr(flow_runtime, 'FolderLedger', Ledger)
    monkeypatch.setattr(flow_runtime, 'FolderModelOperations', Operations)
    monkeypatch.setattr(flow_runtime, 'execute_folder',
                        lambda identity, *_: calls.append(('execute',identity)) or {'state':'waiting_postprocessing'})
    fake_unified = ModuleType('paperflow_prefect.unified')
    fake_unified.UnifiedBackend = lambda *_args, **_kwargs: object()
    monkeypatch.setitem(sys.modules, 'paperflow_prefect.unified', fake_unified)
    result = flow_runtime.run_live_llm_folder({'prefect_api_url':'http://new-prefect/api'},
        task_id, group_id, 'e' * 64, 'attempt-1', run_id)
    if not running:
        assert result['status'] == 'paused' and calls == []
    else:
        assert result['status'] == 'waiting_postprocessing'
        execute = next(row for row in calls if isinstance(row, tuple) and row[0] == 'execute')
        assert execute[1]['task_run_id'] == run_id and execute[1]['parent_run_id'] == parent_id
        assert execute[1]['group_id'] == group_id

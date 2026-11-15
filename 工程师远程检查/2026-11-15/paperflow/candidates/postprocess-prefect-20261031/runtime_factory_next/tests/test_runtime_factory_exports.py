"""Build both candidates using fake dependencies; no real settings or network."""
import importlib.util
from pathlib import Path
import sqlite3
import sys
from types import ModuleType, SimpleNamespace
from uuid import uuid4

import pytest

ROOT = Path(__file__).resolve().parents[2]
for path in (ROOT, ROOT / 'runtime_next', ROOT / 'results_next'):
    sys.path.insert(0, str(path))


def stub(monkeypatch, name, **attrs):
    module = ModuleType(name)
    for key, value in attrs.items():
        setattr(module, key, value)
    monkeypatch.setitem(sys.modules, name, module)
    return module


@pytest.mark.parametrize('variant', ['runtime_next', 'generation_next'])
def test_build_runtime_exports_original_factories_and_fresh_observer(variant, tmp_path, monkeypatch):
    import prefect.client.orchestration as orchestration

    path = ROOT / variant / 'live_runtime.py'
    spec = importlib.util.spec_from_file_location('isolated_' + variant, path)
    runtime = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runtime)
    monkeypatch.setattr(runtime, '_prepare_imports', lambda: None)
    db, queue = tmp_path / 'control.sqlite3', tmp_path / 'queue.sqlite3'
    db.touch(); queue.touch()
    fake_api = 'http://synthetic-prefect/api'
    events = []
    identity = dict(task_id=str(uuid4()), group_id='a' * 32, paper_id='synthetic', receipt_id='b' * 64,
                    attempt_id='attempt-1', parent_run_id=str(uuid4()), task_run_id=str(uuid4()))
    with sqlite3.connect(db) as conn:
        conn.execute('CREATE TABLE llm_folder_queue(task_id TEXT,group_id TEXT,paper_id TEXT,receipt_id TEXT,'
                     'attempt_id TEXT,parent_run_id TEXT,task_run_id TEXT,state TEXT,generation INTEGER)')
        conn.execute('INSERT INTO llm_folder_queue VALUES(?,?,?,?,?,?,?,?,?)',
                     (*identity.values(), 'waiting_postprocessing', 1))

    class Client:
        def __init__(self, api, **kwargs):
            self.api_url, self.closed = api, False
            events.append(('created', self, kwargs))
        def __enter__(self):
            assert not self.closed
            return self
        def __exit__(self, *_):
            self.closed = True
            events.append(('closed', self))

    class Binding:
        def __init__(self, q, control, client, api, consumer, verifier, *, receiver_session):
            assert q == queue.resolve() and control == db and api == fake_api and consumer == 'local-intake'
            assert callable(verifier) and receiver_session is session
            self.client = client
            self.queue_database, self.control_database, self.api_url = q, control, api
            self.retention_consumer, self.verify_receipt, self.receiver_session = consumer, verifier, receiver_session
        def load(self, actual, *, require_running):
            assert not self.client.closed
            events.append(('load', actual, require_running, self.client))
            if actual != identity:
                raise ValueError('synthetic identity mismatch')
            return {'registered': {'synthetic': 'receipt'}}

    class Settings:
        def __init__(self, values): self.values = values
        @staticmethod
        def load(): return SimpleNamespace(values={'prefect_api_url': fake_api})
        def __getitem__(self, key): return self.values[key]
        def require_execution(self): assert self.values['execution_enabled'] is True
        def path(self, _): return tmp_path

    class Backend:
        def __init__(self, scoped, *, invoke):
            self.scoped, self.invoke = scoped, invoke
            events.append(('backend', scoped, invoke))

    class Store:
        def __init__(self, database, **kwargs): self.database, self.kwargs = database, kwargs
        @staticmethod
        def _inspect_receipt(*_): return None

    class Ledger:
        def __init__(self, database, **kwargs): self.database, self.kwargs = database, kwargs

    class Barriers:
        def __init__(self, database): self.database = database
        def postprocess_is_open(self): return False
        def rag_is_held(self): return True

    def session(*_): return 'synthetic-session'
    parent = stub(monkeypatch, 'paperflow_prefect')
    parent.__path__ = []
    parent.legacy = stub(monkeypatch, 'paperflow_prefect.legacy', bind=lambda *_: None)
    llm = stub(monkeypatch, 'paperflow_prefect.llm_queue'); llm.__path__ = []
    stub(monkeypatch, 'paperflow_prefect.attachment_queue',
         settings_for_verified_receipt=lambda settings, registered: (settings, registered))
    stub(monkeypatch, 'paperflow_prefect.llm_queue.folder_completion',
         expected_components=lambda *_: None, inspect_folder=lambda *_: None)
    stub(monkeypatch, 'paperflow_prefect.llm_queue.pause_admission', ControlStop=object)
    stub(monkeypatch, 'paperflow_prefect.llm_queue.upstream_flow_binding', UpstreamFlowBinding=Binding)
    stub(monkeypatch, 'paperflow_prefect.local_preprocessing', _control_paths=lambda: (db, tmp_path),
         FIRST_STEP_CONFIG='synthetic-first', NAMESPACE='synthetic_receiver_for_' + variant,
         QUEUE_CONFIG='synthetic-queue', QUEUE_DIR=tmp_path)
    stub(monkeypatch, 'paperflow_prefect.records', read_json=lambda name:
         {'prefect_api_url': fake_api} if name == 'synthetic-first' else
         {'queue_db': str(queue), 'receipt_root': 'synthetic', 'journal_root': 'synthetic'})
    stub(monkeypatch, 'paperflow_prefect.registered_input', verify_registered_receipt=lambda *_: None)
    stub(monkeypatch, 'paperflow_prefect.settings', Settings=Settings)
    stub(monkeypatch, 'paperflow_prefect.unified', UnifiedBackend=Backend)
    receiver = stub(monkeypatch, 'synthetic_receiver_for_' + variant); receiver.__path__ = []
    stub(monkeypatch, receiver.__name__ + '.local_queue', _current_receiver_session=session)
    stub(monkeypatch, 'body_adapter', frozen_plan_evidence=lambda *_: None)
    stub(monkeypatch, 'folder_state', FolderStageStore=Store)
    stub(monkeypatch, 'handoff_inputs', build_inputs=lambda **_: None)
    stub(monkeypatch, 'generation_mainline_bridge', _snapshot=lambda *_: None,
         process_mainline_folder=lambda **_: None)
    stub(monkeypatch, 'postprocessing_results', ResultsStore=Ledger)
    stub(monkeypatch, 'prefect_slot', postprocessing_slot=lambda *_: None)
    stub(monkeypatch, 'stage_gates', QueueBarriers=Barriers)
    monkeypatch.setattr(orchestration, 'SyncPrefectClient', Client)

    built = runtime.build_runtime()
    assert events == [], 'building the runtime must not open a client or process a folder'
    assert built['stage_store'].kwargs['admission_gate'] is built['coordinator'].task_gate
    assert built['results'].kwargs['admission_gate'] is built['coordinator'].task_gate
    assert built['barriers'].postprocess_is_open() is False
    with built['client']() as client:
        binding = built['upstream_factory'](client)
        assert getattr(binding, 'upstream', None) is not None, 'the exported factory must verify retry sources'
        verified = binding.load(identity, require_running=False)
        backend = built['backend_factory'](verified)
        assert backend.invoke is None
        assert backend.scoped[1] == verified['registered']
    assert client.closed
    assert built['observe_run'](identity) is True
    assert built['observe_run'](dict(identity, group_id='wrong')) is False
    with sqlite3.connect(db) as conn:
        conn.execute('CREATE TABLE paperflow_postprocess_stage_jobs(task_id TEXT,group_id TEXT,paper_id TEXT,'
            'receipt_id TEXT,attempt_id TEXT,parent_run_id TEXT,task_run_id TEXT,source_llm_generation INTEGER,'
            'source_retry_sha256 TEXT,source_retry_prefect_sha256 TEXT,manifest_sha256 TEXT)')
        conn.execute('INSERT INTO paperflow_postprocess_stage_jobs VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                     (*identity.values(), 1, 'a' * 64, 'd' * 64, 'b' * 64))
    assert built['observe_run'](identity) is False
    loads = [event for event in events if event[0] == 'load']
    assert all(event[2] is False for event in loads)
    clients = [event[1] for event in events if event[0] == 'created']
    assert len(clients) == 4 and len({id(client) for client in clients}) == 4
    assert all(client.closed for client in clients)

"""Composition checks: temporary ledgers, synthetic inputs and fake transport only."""
import asyncio
import copy
from contextlib import asynccontextmanager, nullcontext
import hashlib
import importlib
import importlib.util
import json
import os
from pathlib import Path
import shlex
import sqlite3
import subprocess
import sys
from types import ModuleType, SimpleNamespace
from uuid import UUID, uuid4

import pytest

ROOT = Path(__file__).resolve().parents[1]
UPSTREAM = ROOT.parent / 'postprocess-prefect-20261031'
for path in (ROOT, ROOT / 'tests', UPSTREAM, UPSTREAM / 'tests', UPSTREAM / 'runtime_next',
             UPSTREAM / 'results_next', UPSTREAM.parent.parent / 'prefect-paperflow'):
    sys.path.insert(0, str(path))

from test_remote_runtime_adapter import sample


def module():
    try:
        return importlib.import_module('remote_live_runtime')
    except ModuleNotFoundError as exc:
        if exc.name == 'remote_live_runtime':
            pytest.fail('the explicit remote live composition does not exist')
        raise


def digest(value):
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)
    return hashlib.sha256(raw.encode('utf8')).hexdigest()


@pytest.fixture
def configured(sample, tmp_path):
    from live_runtime import ResultsHooks
    from folder_state import FolderStageStore
    from postprocessing_results import ResultsStore
    from snapshot_wire import snapshot_sha

    base = copy.copy(sample['base'])
    base.results = ResultsHooks(ResultsStore(sample['database'], admission_gate=base.task_gate,
                                             inspect_receipt=FolderStageStore._inspect_receipt))
    client = SimpleNamespace(api_url='http://synthetic-prefect/api')
    original = dict(coordinator=base, database=sample['database'], api=client.api_url,
                    client=lambda: nullcontext(client))
    server = copy.deepcopy(sample['server'])
    catalog_sha = hashlib.sha256(sample['catalog'].read_bytes()).hexdigest()
    config = dict(schema='paperflow-server-transport/1', server_identity=server,
        connection=dict(host='synthetic.invalid', port=22, username='synthetic',
            private_key=str(tmp_path / 'never-read-private-key'), server_key_sha256='1' * 64),
        server_python='/synthetic/python', staging_script='/synthetic/stage.py',
        rpc_script='/synthetic/rpc.py', service_config='/synthetic/service.json',
        service_config_sha256='2' * 64, staging_root='/synthetic/staging',
        allowed_roots=[str(tmp_path)], trusted_files=[dict(path=str(sample['catalog']), sha256=catalog_sha)],
        staging_timeout_seconds=60, metadata_timeout_seconds=5, admit_timeout_seconds=2,
        wait_timeout_seconds=20, poll_seconds=.001)
    options = copy.deepcopy(sample['options']['snapshot_options'])
    options['allowed_roots'] = [str(tmp_path)]
    tokenizer = dict(embedding_model_budget=1777, embedding_admission=dict(policy=dict(
        model_sha256='1' * 64, libllama_sha256='2' * 64, cl100k_sha256='3' * 64,
        pipeline_limit=1600, adapter_limit=2100)))
    events = []
    backend = SimpleNamespace(invoke=None, runtime=copy.deepcopy(tokenizer),
        settings=SimpleNamespace(path=lambda key: sample['catalog'] if key == 'catalog_path' else None),
        attachment_work=lambda *_: None)
    def stage(_connection, _command, manifest, **kwargs):
        events.append('stage')
        job = {key: manifest[key] for key in ('job_id', 'claim_id', 'identity', 'generation',
                                               'manifest_sha256', 'code_sha256')}
        job['snapshot_sha256'] = snapshot_sha(manifest)
        return dict(schema='paperflow-remote-snapshot-verified/1', **job, files=len(manifest['files']),
                    evidence_scope='declared_files_only', dependency_closure_verified=False)

    def rpc(_connection, command, payload, **kwargs):
        op = shlex.split(command)[-1]
        events.append(op)
        if op == 'inspect':
            return [sample['attestation'](req) for req in payload['requests']]
        job = payload['job']
        bound = dict(job=copy.deepcopy(job), server_identity=copy.deepcopy(server))
        if op == 'descriptor': return dict(**bound, descriptor_sha256='3' * 64)
        if op == 'preflight': return dict(**bound, preflight_id=str(uuid4()), preflight_sha256='4' * 64)
        if op == 'admit': return dict(**bound, accepted=True)
        if op == 'status': return dict(**bound, state='finished', receipt_ready=True)
        if op == 'result':
            result = sample['remote_only_result'](job)
            request = dict(**bound, receipt_json=result['receipt_json'], receipt_sha256=result['receipt_sha256'])
            result['server_receipt'] = sample['attestation'](request)
            return result
        raise AssertionError(op)

    original.update(upstream_factory=lambda _client: object(), backend_factory=lambda _verified: backend,
                    observe_run=sample['stage_store'].observe_run)
    args = dict(original_runtime=original,
        journal_database=sample['journal'].database, journal_id=sample['journal'].journal_id,
        expected_prefect_api_url=client.api_url,
        server_identity=server, transport_config=config, transport_config_sha256=digest(config),
        snapshot_options=options, expected_catalog_path=sample['catalog'],
        expected_catalog_sha256=catalog_sha, tokenizer_runtime=tokenizer,
        max_summary_receipts=50, stage_call=stage, rpc_call=rpc)
    return SimpleNamespace(args=args, original=original, base=base, backend=backend, events=events,
                           sample=sample, config=config, options=options, tokenizer=tokenizer)


def test_explicit_capacity_wait_builds_busy_aware_transport_without_starting_work(configured):
    from capacity_aware_transport import CapacityAwareTransport
    pause = lambda: None
    value = module().build_remote_live_runtime(**configured.args,
        admission_wait={'wait': pause, 'max_attempts': 3})
    assert isinstance(value.transport, CapacityAwareTransport)
    assert value.factory()._admission_wait == {'wait': pause, 'max_attempts': 3}
    assert configured.events == []


def test_composes_existing_runtime_and_reopens_journal_without_writes(configured):
    x = configured
    original_results = x.base.results
    journal_before = x.sample['journal'].database.read_bytes()
    control_before = x.sample['database'].read_bytes()
    runtime = module().build_remote_live_runtime(**x.args)
    coordinator = runtime.factory()
    assert x.sample['journal'].database.read_bytes() == journal_before
    assert x.sample['database'].read_bytes() == control_before
    assert x.base.results is original_results
    assert coordinator.results.ledger is runtime.results
    assert coordinator.task_gate is x.base.task_gate and coordinator.slot is x.base.slot
    assert runtime.stage_store.inspect_receipt is runtime.results.inspect_receipt
    assert runtime.journal.journal_id == x.sample['journal'].journal_id
    assert runtime.entry.factory is runtime.factory
    assert x.events == []


@pytest.mark.parametrize('key', ['upstream_factory', 'backend_factory', 'observe_run', 'server_identity',
    'journal_id', 'journal_database', 'transport_config', 'transport_config_sha256', 'snapshot_options',
    'expected_catalog_path', 'expected_catalog_sha256', 'tokenizer_runtime', 'max_summary_receipts',
    'expected_prefect_api_url'])
def test_missing_explicit_dependencies_are_rejected_without_dispatch(configured, key):
    x = configured
    if key in ('upstream_factory', 'backend_factory', 'observe_run'):
        args = dict(x.args, original_runtime=dict(x.original, **{key: None}))
    else:
        args = dict(x.args, **{key: None})
    with pytest.raises((ValueError, TypeError)):
        module().build_remote_live_runtime(**args)
    assert x.events == []


def test_separate_factories_cannot_override_the_original_runtime_bindings(configured):
    x = configured
    with pytest.raises(TypeError):
        module().build_remote_live_runtime(**dict(x.args, upstream_factory=lambda _: object()))
    assert x.events == []


def test_different_roots_catalog_server_or_code_are_rejected_before_any_dispatch(configured, tmp_path):
    x = configured
    cases = []
    mismatch = copy.deepcopy(x.config); mismatch['server_identity']['server_id'] = 'other'
    cases.append(dict(transport_config=mismatch, transport_config_sha256=digest(mismatch)))
    mismatch = copy.deepcopy(x.config); mismatch['allowed_roots'] = [str(tmp_path / 'different')]
    cases.append(dict(transport_config=mismatch, transport_config_sha256=digest(mismatch)))
    mismatch = copy.deepcopy(x.config); mismatch['trusted_files'][0]['sha256'] = '9' * 64
    cases.append(dict(transport_config=mismatch, transport_config_sha256=digest(mismatch)))
    options = copy.deepcopy(x.options); options['code_sha256'] = ''
    cases.append(dict(snapshot_options=options))
    for updates in cases:
        with pytest.raises(ValueError):
            module().build_remote_live_runtime(**dict(x.args, **updates))
    assert x.events == []


@pytest.mark.parametrize('key', ['paperflow_root_windows', 'business_workspace_windows',
    'output_root_windows', 'max_json_file_bytes', 'max_total_json_bytes', 'catalog',
    'code_sha256', 'runtime_template_sha256', 'allowed_roots'])
def test_incomplete_snapshot_fields_fail_during_construction(configured, key):
    x = configured
    options = copy.deepcopy(x.options)
    options[key] = None
    with pytest.raises(ValueError):
        module().build_remote_live_runtime(**dict(x.args, snapshot_options=options))
    assert x.events == []


def test_rejects_an_unmatched_prefect_identity(configured):
    x = configured
    with pytest.raises(ValueError):
        module().build_remote_live_runtime(**dict(x.args, expected_prefect_api_url='http://other/api'))
    assert x.events == []


@pytest.mark.parametrize('key,value', [
    ('paperflow_root_windows', r'C:\release\core'),
    ('paperflow_root_windows', r'F:\release\wrong'),
    ('output_root_windows', r'F:\workspace\inside'),
])
def test_invalid_server_paths_fail_before_any_claim(configured, key, value):
    x = configured
    options = copy.deepcopy(x.options)
    options[key] = value
    with pytest.raises(ValueError):
        module().build_remote_live_runtime(**dict(x.args, snapshot_options=options))
    assert x.events == []


def test_catalog_bytes_and_content_must_match_config_before_any_claim(configured):
    x = configured
    options = copy.deepcopy(x.options)
    options['catalog'] = {'different': True}
    with pytest.raises(ValueError):
        module().build_remote_live_runtime(**dict(x.args, snapshot_options=options))
    x.sample['catalog'].write_text('{"different": true}', encoding='utf8')
    with pytest.raises(ValueError):
        module().build_remote_live_runtime(**x.args)
    assert x.events == []


def test_missing_or_replaced_original_journal_never_initializes_one(configured, tmp_path):
    x = configured
    target = tmp_path / 'missing-journal.sqlite3'
    with pytest.raises(ValueError):
        module().build_remote_live_runtime(**dict(x.args, journal_database=target))
    assert not target.exists() and x.events == []
    with pytest.raises(ValueError):
        module().build_remote_live_runtime(**dict(x.args, journal_id=str(uuid4())))
    assert x.events == []


def test_full_prefect_task_routes_through_the_composed_real_adapter(configured, monkeypatch):
    x = configured
    import remote_runtime_adapter as adapter
    import generation_mainline_bridge as bridge
    import handoff_inputs
    from prefect.context import TaskRunContext
    from stage_runtime import build_task

    identity = x.sample['evidence'][0]['identity']
    evidence = x.sample['evidence'][0]
    monkeypatch.setattr(adapter, 'StageBoundHandoff', lambda *_: SimpleNamespace(
        load=lambda actual, require_running: {'synthetic': True}))
    monkeypatch.setattr(bridge, '_snapshot', lambda *_: (
        dict(manifest_sha256=evidence['manifest_sha256'], generation=1), []))
    monkeypatch.setattr(handoff_inputs, 'build_inputs', lambda **_: copy.deepcopy(evidence))
    x.base.verify_execution = lambda item, context: str(context.id) == item['job_id']
    runtime = module().build_remote_live_runtime(**x.args)
    job = runtime.factory().reserve(identity, 1)
    with sqlite3.connect(x.sample['database']) as db:
        db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='submitted' WHERE job_id=?", (job['job_id'],))
    monkeypatch.setattr(TaskRunContext, 'get', classmethod(lambda cls: SimpleNamespace(
        task_run=SimpleNamespace(id=UUID(job['job_id'])))))
    result = asyncio.run(build_task(runtime.factory, job['job_id']).fn())
    assert result['state'] == 'waiting_rag'
    assert x.events[:6] == ['stage', 'descriptor', 'preflight', 'admit', 'status', 'result']
    assert 'inspect' in x.events
    with sqlite3.connect(x.sample['database']) as db:
        assert db.execute('SELECT state FROM postprocessing_results').fetchone()[0] == 'passed'


def test_backend_catalog_and_tokenizer_are_checked_per_folder(configured, monkeypatch, tmp_path):
    x = configured
    import remote_runtime_adapter as adapter
    import generation_mainline_bridge as bridge
    import handoff_inputs
    identity = x.sample['evidence'][0]['identity']
    evidence = x.sample['evidence'][0]
    item = dict(identity, generation=1, source_llm_generation=1, manifest_sha256=evidence['manifest_sha256'])
    monkeypatch.setattr(adapter, 'StageBoundHandoff', lambda *_: SimpleNamespace(
        load=lambda actual, require_running: {'synthetic': True}))
    monkeypatch.setattr(bridge, '_snapshot', lambda *_: (
        dict(manifest_sha256=evidence['manifest_sha256'], generation=1), []))
    monkeypatch.setattr(handoff_inputs, 'build_inputs', lambda **_: copy.deepcopy(evidence))
    runtime = module().build_remote_live_runtime(**x.args)
    load = runtime.factory()._load_inputs
    assert load(identity, 1, item, object())['inputs'] == evidence
    x.backend.runtime['embedding_model_budget'] += 1
    with pytest.raises(ValueError): load(identity, 1, item, object())
    x.backend.runtime = copy.deepcopy(x.tokenizer)
    x.backend.settings.path = lambda key: tmp_path / 'different-catalog.json'
    with pytest.raises(ValueError): load(identity, 1, item, object())
    assert x.events == []


@pytest.mark.parametrize('variant', ['runtime_next', 'generation_next'])
def test_both_original_build_runtimes_feed_the_remote_prefect_task(configured, tmp_path, monkeypatch, variant):
    """Run each actual build_runtime with only explicitly synthetic dependencies."""
    x = configured
    remote_module = module()  # Freeze the shared canonical stage_runtime before loading either source.
    import prefect.client.orchestration as orchestration
    from prefect.context import TaskRunContext
    from stage_runtime import Coordinator, TAG, build_task

    def stub(name, **attrs):
        instance = ModuleType(name)
        for key, value in attrs.items(): setattr(instance, key, value)
        monkeypatch.setitem(sys.modules, name, instance)
        return instance

    evidence = x.sample['evidence'][0]
    identity = evidence['identity']
    fake_api = x.original['api']
    queue = tmp_path / 'synthetic-queue.sqlite3'
    queue.touch()
    (tmp_path / 'locks').mkdir()
    events, selected, bound = [], {}, []

    class Client:
        def __init__(self, api, **_): self.api_url = api
        def __enter__(self): return self
        def __exit__(self, *_): return None
        def read_task_run(self, _): return selected['stage']
        def read_flow_run(self, _):
            return SimpleNamespace(id=identity['task_run_id'], name='合成工单等待后处理',
                state=SimpleNamespace(is_paused=lambda: True,
                    state_details=SimpleNamespace(pause_reschedule=True)))

    class Binding:
        def __init__(self, _queue, _database, client, api, _consumer, _verifier, *, receiver_session):
            assert client.api_url == api == fake_api and callable(receiver_session)
            self.queue_database = _queue
            self.control_database = _database
            self.client = client
            self.api_url = api
            self.retention_consumer = _consumer
            self.verify_receipt = _verifier
            self.receiver_session = receiver_session
        def load(self, actual, *, require_running):
            assert actual == identity and require_running is False
            events.append('upstream')
            return {'registered': {'synthetic': True}}

    class Settings:
        def __init__(self, values): self.values = values
        @staticmethod
        def load(): return SimpleNamespace(values={'prefect_api_url': fake_api})
        def __getitem__(self, key): return self.values[key]
        def require_execution(self): assert self.values['execution_enabled'] is True
        def path(self, _): return tmp_path

    class Backend:
        def __new__(cls, scoped, *, invoke):
            assert invoke is None and scoped[1] == {'synthetic': True}
            events.append('backend')
            return x.backend

    class Barriers:
        def __init__(self, _database): pass
        def postprocess_is_open(self): return True
        def rag_is_held(self): return True

    class Stop:
        def __init__(self, *_): pass
        def is_set(self): return False

    @asynccontextmanager
    async def slot(api):
        assert api == fake_api
        events.append('slot')
        yield

    def bind(_settings):
        bound.append(True)
        events.append('legacy-bound')

    parent = stub('paperflow_prefect'); parent.__path__ = []
    parent.legacy = stub('paperflow_prefect.legacy', bind=bind)
    llm = stub('paperflow_prefect.llm_queue'); llm.__path__ = []
    stub('paperflow_prefect.attachment_queue',
         settings_for_verified_receipt=lambda settings, registered: (settings, registered))
    stub('paperflow_prefect.llm_queue.folder_completion',
         expected_components=lambda *_: None, inspect_folder=lambda *_: None)
    stub('paperflow_prefect.llm_queue.pause_admission', ControlStop=Stop)
    stub('paperflow_prefect.llm_queue.upstream_flow_binding', UpstreamFlowBinding=Binding)
    namespace = 'synthetic_receiver_for_remote_' + variant
    receiver = stub(namespace); receiver.__path__ = []
    stub(namespace + '.local_queue', _current_receiver_session=lambda *_: 'synthetic')
    stub('paperflow_prefect.local_preprocessing', _control_paths=lambda: (x.sample['database'], tmp_path),
         FIRST_STEP_CONFIG='synthetic-first', NAMESPACE=namespace, QUEUE_CONFIG='synthetic-queue',
         QUEUE_DIR=tmp_path)
    stub('paperflow_prefect.records', read_json=lambda name:
         {'prefect_api_url': fake_api} if name == 'synthetic-first' else
         {'queue_db': str(queue), 'receipt_root': 'synthetic', 'journal_root': 'synthetic'})
    stub('paperflow_prefect.registered_input', verify_registered_receipt=lambda *_: None)
    stub('paperflow_prefect.settings', Settings=Settings)
    stub('paperflow_prefect.unified', UnifiedBackend=Backend)
    stub('paperflow_prefect.resources', drain_thread=lambda fn, *args: asyncio.to_thread(fn, *args))
    stub('body_adapter', frozen_plan_evidence=lambda *_: None)
    stub('handoff_inputs', build_inputs=lambda **_: copy.deepcopy(evidence))
    stub('generation_mainline_bridge',
         _snapshot=lambda *_: (dict(manifest_sha256=evidence['manifest_sha256'], generation=1), []),
         process_mainline_folder=lambda **_: pytest.fail('local processor must not run'))
    stub('prefect_slot', postprocessing_slot=slot)
    stub('stage_gates', QueueBarriers=Barriers)

    scheduler = stub('preextract_scheduler_adapter')
    def scheduler_attr(name):
        if name == 'request_observation':
            assert bound, 'legacy source binding must happen before the scheduler adapter is imported'
            events.append('scheduler-import')
            return lambda *_: None
        raise AttributeError(name)
    scheduler.__getattr__ = scheduler_attr
    monkeypatch.setattr(orchestration, 'SyncPrefectClient', Client)

    path = UPSTREAM / variant / 'live_runtime.py'
    spec = importlib.util.spec_from_file_location('synthetic_live_' + variant, path)
    original = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(original)
    monkeypatch.setattr(original, '_prepare_imports', lambda: None)
    built = original.build_runtime()
    assert isinstance(built['coordinator'], Coordinator)
    assert not events and not x.events
    remote = remote_module.build_remote_live_runtime(**dict(x.args, original_runtime=built))
    coordinator = remote.factory()
    assert remote.stage_store.observe_run is built['observe_run']
    assert coordinator.task_gate is built['coordinator'].task_gate
    assert coordinator.slot is built['coordinator'].slot
    job = coordinator.reserve(identity, 1)
    assert events.index('legacy-bound') < events.index('scheduler-import')
    with sqlite3.connect(x.sample['database']) as db:
        db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='submitted' WHERE job_id=?", (job['job_id'],))
    stage = SimpleNamespace(id=UUID(job['job_id']), flow_run_id=None, task_key=job['task_key'],
        dynamic_key='paperflow-postprocess-' + job['job_id'], tags=[TAG],
        state=SimpleNamespace(is_running=lambda: True))
    selected['stage'] = stage
    monkeypatch.setattr(TaskRunContext, 'get', classmethod(lambda cls: SimpleNamespace(task_run=stage)))
    result = asyncio.run(build_task(remote.factory, job['job_id']).fn())
    assert result['state'] == 'waiting_rag'
    assert 'slot' in events and 'backend' in events and 'upstream' in events
    assert x.events[:6] == ['stage', 'descriptor', 'preflight', 'admit', 'status', 'result']
    with sqlite3.connect(x.sample['database']) as db:
        assert db.execute('SELECT state FROM postprocessing_results').fetchone()[0] == 'passed'


@pytest.mark.parametrize('variant', ['runtime_next', 'generation_next'])
def test_each_clean_import_path_shares_original_prefect_class_identity(variant, tmp_path):
    # A separate interpreter is essential: one interpreter caches the first
    # imported "stage_runtime" even when another candidate path is prepended.
    paths = [str(UPSTREAM / variant), str(ROOT), str(UPSTREAM), str(UPSTREAM / 'results_next'),
             str(UPSTREAM.parent.parent / 'prefect-paperflow')]
    env = dict(os.environ, PYTHONPATH=os.pathsep.join(paths), PYTHONDONTWRITEBYTECODE='1')
    script = '''
import importlib.util
from pathlib import Path
import sys
import stage_runtime, live_runtime, remote_live_runtime, remote_prefect_entry, remote_runtime_adapter
expected = Path(sys.argv[1]).resolve()
assert Path(stage_runtime.__file__).resolve() == expected / 'stage_runtime.py'
assert Path(live_runtime.__file__).resolve() == expected / 'live_runtime.py'
assert remote_live_runtime.Coordinator is stage_runtime.Coordinator
assert remote_prefect_entry.Coordinator is stage_runtime.Coordinator
assert remote_runtime_adapter.Coordinator is stage_runtime.Coordinator
assert remote_live_runtime.ResultsHooks is live_runtime.ResultsHooks
assert remote_runtime_adapter.ResultsHooks is live_runtime.ResultsHooks
spec = importlib.util.spec_from_file_location('duplicate_stage_runtime', expected / 'stage_runtime.py')
duplicate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(duplicate)
assert duplicate.Coordinator is not stage_runtime.Coordinator
'''
    result = subprocess.run([sys.executable, '-B', '-c', script, paths[0]], cwd=tmp_path,
                            env=env, capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr

"""Synthetic original Prefect/frozen-source bindings; no network or model work."""
import importlib.util
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
from types import SimpleNamespace
from types import ModuleType
from uuid import UUID, uuid4

import pytest

HERE = Path(__file__).resolve().parents[1]
ROOT = HERE.parent
sys.path.insert(0, str(ROOT / 'generation_next' / 'tests'))
from test_retry_source import install_prior_failed_retry, install_retry_records, install_stopped_retry


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


old = load('manual_retry_scope_fixtures', ROOT / 'remote_projection_next' / 'tests' / 'test_scope_verifier.py')
modules, sample, scope = old.modules, old.sample, old.scope
runtime = load('manual_retry_generation_runtime', ROOT / 'generation_next' / 'stage_runtime.py')


@pytest.fixture
def completed_retry(scope):
    sample, _, request = scope
    request['folders'], _ = old.populate_real_frozen_bindings(sample, 1)
    _, identity, retry_key, retry_job, old_state = install_retry_records(
        sample.control, sample.item, existing_admissions=True)
    folder = sample.client.flows[identity['task_run_id']]
    folder.state.kind = 'FAILED'
    folder.state.id = UUID(old_state)
    folder.state.is_failed = lambda: folder.state.kind == 'FAILED'
    folder.state.is_crashed = lambda: folder.state.kind == 'CRASHED'
    sample.client.stages[retry_job] = SimpleNamespace(id=UUID(retry_job), flow_run_id=None,
        task_key='paperflow-folder-llm-stage-v1', dynamic_key='paperflow-llm-' + retry_job,
        tags=['paperflow-llm-stage-v1'], state=old.previous.State('COMPLETED'))
    sample.client.stages[retry_job].state.id = uuid4()
    sample.client.read_flow_run = lambda key: sample.client.flows[str(key)]
    sample.client.read_task_run = lambda key: sample.client.stages[str(key)]
    sample.client.read_deployment = lambda key: (sample.client.root if key == sample.client.root.id
                                                 else sample.client.folder_deployment)
    return sample, request, sample.projector.binding_factory(sample.client), retry_key, retry_job


def complete_raw_fixture(sample, request):
    """Add one independently hashed synthetic answer to the real frozen binding."""
    from paperflow_prefect.llm_queue.folder_completion import expected_components
    manifest = json.loads(sample.path.read_text(encoding='utf8'))
    model = sample.tmp_path / 'body-work' / 'model'
    model.mkdir(parents=True)
    prompt = {'messages': [{'role': 'user', 'content': 'synthetic'}]}
    request_sha = hashlib.sha256(json.dumps(prompt, ensure_ascii=False, separators=(',', ':')).encode('utf8')).hexdigest()
    response = b'{"synthetic":true}'
    response_sha = hashlib.sha256(response).hexdigest()
    (model / 'request.json').write_text(json.dumps(prompt, ensure_ascii=False), encoding='utf8')
    (model / 'response.txt').write_bytes(response)
    (model / 'response-events.jsonl').write_text('{}\n', encoding='utf8')
    (model / 'request-state.json').write_text(json.dumps(dict(status='response_complete', finish_reason='stop',
        request_sha256=request_sha, response_sha256=response_sha)), encoding='utf8')
    plan = dict(schema='paperflow-body-input-plan/1', source_sha256=manifest['body']['sha256'],
        reading_sha256=manifest['reading_material']['sha256'], plan_sha256='d' * 64,
        coverage_sha256='e' * 64, mode='single', parts=[dict(id='part-001', request_sha256=request_sha)], part_count=1)
    manifest['capacity'] = dict(status='planned', plan_identity=plan, parts=1, mode='single')
    raw = json.dumps(manifest, ensure_ascii=False).encode('utf8')
    sample.path.write_bytes(raw)
    reference = dict(manifest_path=str(sample.path), manifest_sha256=hashlib.sha256(raw).hexdigest())
    digest = hashlib.sha256(json.dumps(manifest, ensure_ascii=False, sort_keys=True,
                            separators=(',', ':')).encode('utf8')).hexdigest()
    component = expected_components(manifest, sample.item['task_run_id'])[0]
    with sqlite3.connect(sample.control) as db:
        db.execute('UPDATE llm_folder_queue SET manifest_sha256=?', (digest,))
        db.execute('UPDATE llm_request_admissions SET component_id=?,source_sha256=?,request_sha256=?,'
            'response_sha256=?,work_dir=?,model_dir=?', (component['component_id'], component['source_sha256'],
             request_sha, response_sha, str(model.parent), str(model)))
    with sqlite3.connect(sample.queue.path) as db:
        db.execute('UPDATE preprocessing_handoffs SET handoff=?', (json.dumps(reference),))
    item = {**{key: sample.item[key] for key in ('task_id', 'group_id', 'paper_id', 'receipt_id')}, **reference}
    sample.queue.list_waiting_llm = lambda: dict(waiting_count=1, unavailable_count=0, items=[item])
    sample.digest = digest
    request['folders'][0]['manifest_sha256'] = digest
    return model


def test_original_upstream_binding_accepts_only_the_verified_retry(completed_retry):
    sample, _, base, _, _ = completed_retry
    with pytest.raises(ValueError):
        base.load(sample.item, require_running=False)
    binding_type = getattr(runtime, 'RetryAwareUpstream', None)
    assert callable(binding_type), 'the runtime must connect the existing retry verifier'
    verified = binding_type(base).load(sample.item, require_running=False)
    assert verified['manifest']['paper_id'] == sample.item['paper_id']
    assert len(verified['source_retry_sha256']) == 64
    assert sample.client.flows[sample.item['task_run_id']].state.kind == 'FAILED'


def test_prior_failed_retry_is_verified_against_prefect(completed_retry):
    sample, _, base, request_id, _ = completed_retry
    prior_id, prior_state = install_prior_failed_retry(sample.control, sample.item, request_id)
    state = old.previous.State('FAILED')
    state.id = UUID(prior_state)
    state.is_failed = lambda: state.kind == 'FAILED'
    state.is_crashed = lambda: state.kind == 'CRASHED'
    sample.client.stages[prior_id] = SimpleNamespace(id=UUID(prior_id), flow_run_id=None,
        task_key='paperflow-folder-llm-stage-v1', dynamic_key='paperflow-llm-' + prior_id,
        tags=['paperflow-llm-stage-v1'], state=state)
    assert runtime.RetryAwareUpstream(base).load(sample.item)['source_retry_sha256']
    state.id = uuid4()
    with pytest.raises(runtime.StageConflict):
        runtime.RetryAwareUpstream(base).load(sample.item)


def test_page_scope_accepts_success_after_a_verified_failed_retry(completed_retry):
    sample, request, _, request_id, _ = completed_retry
    complete_raw_fixture(sample, request)
    prior_id, prior_state = install_prior_failed_retry(sample.control, sample.item, request_id)
    state = old.previous.State('FAILED')
    state.id = UUID(prior_state)
    state.is_failed = lambda: state.kind == 'FAILED'
    state.is_crashed = lambda: state.kind == 'CRASHED'
    sample.client.stages[prior_id] = SimpleNamespace(id=UUID(prior_id), flow_run_id=None,
        task_key='paperflow-folder-llm-stage-v1', dynamic_key='paperflow-llm-' + prior_id,
        tags=['paperflow-llm-stage-v1'], state=state)
    old_factory = sample.projector.binding_factory
    sample.projector.binding_factory = lambda client: runtime.RetryAwareUpstream(old_factory(client))
    source = load('manual_retry_page_with_previous_failure', HERE / 'scope_verifier.py')
    verifier = source.ScopeVerifier(sample.projector, server_identity=request['server_identity'],
                                    check_original_files=lambda: None)
    assert verifier.verify(request) is True


def test_paused_retry_must_have_a_finished_prefect_stage(completed_retry):
    sample, _, base, request_id, _ = completed_retry
    stopped_id = install_stopped_retry(sample.control, sample.item, request_id)
    state = old.previous.State('COMPLETED')
    state.id = uuid4()
    sample.client.stages[stopped_id] = SimpleNamespace(id=UUID(stopped_id), flow_run_id=None,
        task_key='paperflow-folder-llm-stage-v1', dynamic_key='paperflow-llm-' + stopped_id,
        tags=['paperflow-llm-stage-v1'], state=state)
    assert runtime.RetryAwareUpstream(base).load(sample.item)['source_retry_sha256']
    state.kind = 'RUNNING'
    with pytest.raises(runtime.StageConflict):
        runtime.RetryAwareUpstream(base).load(sample.item)


@pytest.mark.parametrize('which', ['completed_retry', 'stopped_retry'])
def test_retry_prefect_completion_state_is_frozen_across_observations(completed_retry, which):
    sample, _, base, request_id, retry_id = completed_retry
    target_id = retry_id
    if which == 'stopped_retry':
        target_id = install_stopped_retry(sample.control, sample.item, request_id)
        state = old.previous.State('COMPLETED')
        state.id = uuid4()
        sample.client.stages[target_id] = SimpleNamespace(id=UUID(target_id), flow_run_id=None,
            task_key='paperflow-folder-llm-stage-v1', dynamic_key='paperflow-llm-' + target_id,
            tags=['paperflow-llm-stage-v1'], state=state)
    binder = runtime.RetryAwareUpstream(base)
    first = binder.load(sample.item)['source_retry_prefect_sha256']
    sample.client.stages[target_id].state.id = uuid4()
    second = binder.load(sample.item)['source_retry_prefect_sha256']
    assert first != second


@pytest.mark.parametrize('change', ['old_flow_state', 'retry_task_key', 'retry_task_state', 'retry_dynamic_key'])
def test_original_binding_rejects_changed_prefect_retry_evidence(completed_retry, change):
    sample, _, base, _, retry_job = completed_retry
    if change == 'old_flow_state':
        sample.client.flows[sample.item['task_run_id']].state.id = uuid4()
    elif change == 'retry_task_key':
        sample.client.stages[retry_job].task_key = 'another-stage'
    elif change == 'retry_task_state':
        sample.client.stages[retry_job].state.kind = 'FAILED'
    else:
        sample.client.stages[retry_job].dynamic_key = 'another-stage'
    with pytest.raises((ValueError, runtime.StageConflict)):
        runtime.RetryAwareUpstream(base).load(sample.item, require_running=False)


def test_retry_answer_changed_while_prefect_is_reading_is_rejected(completed_retry):
    sample, _, base, _, _ = completed_retry
    original = sample.client.read_task_run
    def changed(key):
        with sqlite3.connect(sample.control) as db:
            db.execute('UPDATE llm_request_admissions SET response_sha256=?', ('f' * 64,))
        return original(key)
    sample.client.read_task_run = changed
    with pytest.raises(runtime.StageConflict):
        runtime.RetryAwareUpstream(base).load(sample.item, require_running=False)


def test_server_page_scope_accepts_completed_retry_with_original_folder(completed_retry):
    sample, request, _, _, _ = completed_retry
    complete_raw_fixture(sample, request)
    old_factory = sample.projector.binding_factory
    sample.projector.binding_factory = lambda client: runtime.RetryAwareUpstream(old_factory(client))
    source = load('manual_retry_real_page_scope', HERE / 'scope_verifier.py')
    verifier = source.ScopeVerifier(sample.projector, server_identity=request['server_identity'],
                                    check_original_files=lambda: None)
    assert verifier.verify(request) is True
    assert sample.client.flows[sample.item['task_run_id']].state.kind == 'FAILED'


def test_page_scope_keeps_completed_retry_visible_after_its_postprocess_claim(completed_retry):
    sample, request, _, _, retry_job = completed_retry
    complete_raw_fixture(sample, request)
    old_factory = sample.projector.binding_factory
    sample.projector.binding_factory = lambda client: runtime.RetryAwareUpstream(old_factory(client))
    retry_proof = sample.projector.binding_factory(sample.client).load(sample.item, require_running=False)
    post_id = old.previous.insert_job(sample, 'finished', task_state='COMPLETED')
    sample.client.stages[post_id].state.id = uuid4()
    with sqlite3.connect(sample.control) as db:
        db.execute('ALTER TABLE paperflow_postprocess_stage_jobs ADD COLUMN source_retry_sha256 TEXT')
        db.execute('ALTER TABLE paperflow_postprocess_stage_jobs ADD COLUMN source_retry_prefect_sha256 TEXT')
        db.execute('UPDATE paperflow_postprocess_stage_jobs SET source_retry_sha256=?,source_retry_prefect_sha256=?',
                   (retry_proof['source_retry_sha256'], retry_proof['source_retry_prefect_sha256']))
    old.previous.save_pass(sample)
    source = load('manual_retry_page_after_claim', HERE / 'scope_verifier.py')
    verifier = source.ScopeVerifier(sample.projector, server_identity=request['server_identity'],
                                    check_original_files=lambda: None)
    assert verifier.verify(request) is True
    sample.client.stages[retry_job].state.id = uuid4()
    with pytest.raises(ValueError):
        verifier.verify(request)
    with sqlite3.connect(sample.control) as db:
        db.execute("UPDATE paperflow_postprocess_stage_jobs SET source_retry_sha256=?", ('0' * 64,))
    assert verifier(request) is False


def test_page_scope_rejects_changed_retry_response_bytes(completed_retry):
    sample, request, _, _, _ = completed_retry
    model = complete_raw_fixture(sample, request)
    old_factory = sample.projector.binding_factory
    sample.projector.binding_factory = lambda client: runtime.RetryAwareUpstream(old_factory(client))
    source = load('manual_retry_page_changed_answer', HERE / 'scope_verifier.py')
    verifier = source.ScopeVerifier(sample.projector, server_identity=request['server_identity'],
                                    check_original_files=lambda: None)
    assert verifier.verify(request) is True
    (model / 'response.txt').write_text('changed', encoding='utf8')
    assert verifier(request) is False


def test_server_policy_loader_uses_completed_retry_binding(completed_retry, monkeypatch):
    sample, _, base, _, _ = completed_retry
    sys.path.insert(0, str(ROOT.parent / 'postprocess-server-20261101'))
    sys.path.insert(0, str(ROOT))
    probe = load('synthetic_retry_policy_probe', ROOT.parent / 'postprocess-server-20261101' / 'selected_ticket_probe.py')
    import prefect.client.orchestration as orchestration
    import paperflow_prefect.llm_queue.upstream_flow_binding as upstream
    import paperflow_prefect.attachment_queue as attachments
    import paperflow_prefect.settings as settings
    import paperflow_prefect.unified as unified
    import handoff_inputs
    monkeypatch.setattr(orchestration, 'SyncPrefectClient', lambda *_a, **_kw: sample.client)
    monkeypatch.setattr(upstream, 'UpstreamFlowBinding', lambda *_a, **_kw: base)
    monkeypatch.setattr(attachments, 'settings_for_verified_receipt', lambda *_: object())
    monkeypatch.setattr(settings, 'Settings', lambda *_: object())
    monkeypatch.setattr(unified, 'UnifiedBackend', lambda *_a, **_kw: SimpleNamespace(attachment_work=lambda *_: None))
    monkeypatch.setattr(handoff_inputs, 'build_inputs', lambda **kw: {'identity': kw['identity'],
        'source_retry_sha256': kw['verified'].get('source_retry_sha256')})
    scheduler = ModuleType('preextract_scheduler_adapter')
    scheduler.request_observation = lambda *_: None
    monkeypatch.setitem(sys.modules, 'preextract_scheduler_adapter', scheduler)
    receiver = ModuleType('_paperflow_selected_probe_local_intake.local_queue')
    receiver._current_receiver_session = lambda *_: 'receiver-test'
    monkeypatch.setitem(sys.modules, receiver.__name__, receiver)
    sources = SimpleNamespace(workspace=ROOT.parents[1], control_database=sample.control,
        queue={'queue_db': str(sample.queue.path)}, settings={'prefect_api_url': sample.client.api_url})
    verified, inputs, _ = probe._live_loader(sources)(sample.item, [])
    assert verified['source_retry_sha256'] == inputs['source_retry_sha256']
    assert len(verified['source_retry_sha256']) == 64
    def changed_during_build(**kw):
        with sqlite3.connect(sample.control) as db:
            db.execute('UPDATE llm_request_admissions SET response_sha256=?', ('f' * 64,))
        return {'identity': kw['identity'], 'source_retry_sha256': kw['verified'].get('source_retry_sha256')}
    monkeypatch.setattr(handoff_inputs, 'build_inputs', changed_during_build)
    with pytest.raises((ValueError, runtime.StageConflict)):
        probe._live_loader(sources)(sample.item, [])

"""A failed synthetic attachment stops this paper and preserves its body."""
import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[2]
WORKSPACE = ROOT.parents[1]
sys.path[:0] = [str(ROOT), str(WORKSPACE / 'prefect-paperflow'),
                str(WORKSPACE / 'candidates/postprocess-prefect-20261031/tests')]
from test_synthetic_mainline_integration import synthetic_folder
from test_body_adapter import FixtureCounter, RELEASE


@pytest.fixture
def offline_loop(monkeypatch):
    # Windows creates a loopback socket pair when the event loop is created.
    loop = asyncio.new_event_loop()
    import socket
    monkeypatch.setattr(socket.socket, 'connect', lambda *a, **k: pytest.fail('network forbidden'))
    yield loop.run_until_complete
    loop.close()


def test_attachment_failure_stops_folder_and_manual_retry_reuses_body(tmp_path, monkeypatch, offline_loop, managed_counter_factory):
    try:
        from paperflow_v3.stages import llm_handoff, postprocessing
    except ImportError:
        pytest.fail('The candidate folder postprocessing adapter is missing')
    identity, manifest, paper, admissions, catalog, attachment_work = synthetic_folder(tmp_path)
    events = [{'task_id': identity['task_id'], 'folder_id': identity['group_id'],
        'kind': 'llm_response_saved', 'data': {'component_id': row['component_id'],
            'request_id': row['component_id'], 'kind': row['kind'],
            'request_sha256': row['request_sha256'], 'response_sha256': row['response_sha256'],
            'response_text': (Path(row['model_dir']) / 'response.txt').read_bytes().decode('utf8'),
            'response_path': str(Path(row['model_dir']) / 'response.txt')}} for row in admissions]

    async def read_receipts(_child):
        return list(reversed(events))

    async def save_receipt(**row):
        events.append(row)

    class Backend:
        def attachment_work(self, _paper, _row):
            return Path(attachment_work)

    class Admission:
        async def running(self):
            return True
        async def wait(self):
            return None

    @asynccontextmanager
    async def isolated_slot(_admission, _limits):
        if hasattr(_admission, 'admit'):
            assert await _admission.admit()
        yield

    for module in (llm_handoff, postprocessing):
        monkeypatch.setattr(module, 'read_receipts', read_receipts)
        monkeypatch.setattr(module, 'save_receipt', save_receipt)
    monkeypatch.setattr(postprocessing, 'admitted_slot', isolated_slot)
    inputs = offline_loop(llm_handoff.build_postprocessing_inputs(
        identity=identity, manifest=manifest, paper=paper, backend=Backend()))
    counters = []
    def counter_factory():
        counters.append('created')
        return managed_counter_factory()

    options = dict(inputs=inputs, catalog=catalog, paperflow_root=RELEASE / 'core',
                   counter_factory=counter_factory, output_root=tmp_path / 'output',
                   admission=Admission(), limit_names=['isolated-postprocessing'])
    real_attachment = postprocessing.process_attachment

    def failed_attachment(*_args):
        raise ValueError('synthetic attachment failure')

    monkeypatch.setattr(postprocessing, 'process_attachment', failed_attachment)
    with pytest.raises(ValueError, match='synthetic attachment failure'):
        offline_loop(postprocessing.process_folder(**options))
    assert any(event['kind'] == 'postprocessing_component_complete'
               and event['data']['component_id'] == 'body' for event in events)
    assert counters == ['created']
    assert not any(event['kind'] == 'postprocessing_complete' for event in events)
    body_result = next((tmp_path / 'output').rglob('body-result.json'))
    before = body_result.read_bytes()

    def must_not_redo(*_args, **_kwargs):
        pytest.fail('successful body must not be processed again')

    monkeypatch.setattr(postprocessing, 'process_body', must_not_redo)
    monkeypatch.setattr(postprocessing, 'process_attachment', real_attachment)
    with pytest.raises(RuntimeError, match='人工重试'):
        offline_loop(postprocessing.process_folder(**options))
    result = offline_loop(postprocessing.process_folder(**options, retry_id='manual-retry-1'))
    assert result['validation_status'] == 'passed'
    assert len(result['attachments']) == 1
    assert counters == ['created']
    assert body_result.read_bytes() == before
    assert events[-1]['kind'] == 'postprocessing_complete'


def test_paused_parent_does_not_start_postprocessing():
    from paperflow_v3.stages import postprocessing

    class Paused:
        async def wait(self):
            raise InterruptedError('parent is paused')

    with pytest.raises(InterruptedError, match='parent is paused'):
        asyncio.run(postprocessing.process_folder(inputs={}, catalog={}, paperflow_root='unused', counter_factory=None,
            output_root='unused', admission=Paused(), limit_names=['synthetic']))


def test_invalid_body_preserves_diagnostic_and_never_emits_completion(tmp_path,monkeypatch,offline_loop):
    from paperflow_v3.stages import llm_handoff,postprocessing
    from paperflow_v3.stages.body_adapter import BodyValidationError
    from test_body_adapter import save
    identity,manifest,paper,admissions,catalog,attachment_work=synthetic_folder(tmp_path)
    events=[{'task_id':identity['task_id'],'folder_id':identity['group_id'],'kind':'llm_response_saved',
        'data':{'component_id':r['component_id'],'request_id':r['component_id'],'kind':r['kind'],
          'request_sha256':r['request_sha256'],'response_sha256':r['response_sha256'],
          'response_text':(Path(r['model_dir'])/'response.txt').read_text(encoding='utf-8'),
          'response_path':str(Path(r['model_dir'])/'response.txt')}} for r in admissions]
    async def records(_child):return list(reversed(events))
    async def saved(**row):events.append(row)
    @asynccontextmanager
    async def slot(admission,_limits):
        if hasattr(admission,'admit'):assert await admission.admit()
        yield
    class Backend:
        def attachment_work(self,*args):return Path(attachment_work)
    class Admission:
        async def running(self):return True
        async def wait(self):pass
    for module in (llm_handoff,postprocessing):
        monkeypatch.setattr(module,'read_receipts',records);monkeypatch.setattr(module,'save_receipt',saved)
    monkeypatch.setattr(postprocessing,'admitted_slot',slot)
    inputs=offline_loop(llm_handoff.build_postprocessing_inputs(identity=identity,manifest=manifest,paper=paper,backend=Backend()))
    diagnostic=save(tmp_path/'private/validation.json',{'valid':False,'errors':[{'code':'synthetic-invalid'}]})
    calls=[]
    def invalid(*args,**kwargs):calls.append(1);raise BodyValidationError('candidate_validation_failed',diagnostic)
    monkeypatch.setattr(postprocessing,'_process_body',invalid)
    options=dict(inputs=inputs,catalog=catalog,paperflow_root=RELEASE/'core',counter_factory=None,
        output_root=tmp_path/'output',admission=Admission(),limit_names=['synthetic'])
    with pytest.raises(BodyValidationError):offline_loop(postprocessing.process_folder(**options))
    failure=next(e for e in events if e['kind']=='postprocessing_component_failed')
    assert failure['data']['diagnostic']==diagnostic
    assert not any(e['kind']=='postprocessing_complete' for e in events)
    with pytest.raises(RuntimeError,match='人工重试'):offline_loop(postprocessing.process_folder(**options))
    assert calls==[1] and Path(diagnostic['path']).is_file()


@pytest.mark.parametrize('final_failure', [None, 'receipt', 'inventory'])
def test_pause_after_body_postprocessing_still_completes_the_folder_attachment(
        tmp_path, monkeypatch, stage_store, managed_counter_factory, final_failure):
    from paperflow_v3.stages import llm_handoff, postprocessing
    ids, manifest, paper, admissions, catalog, attachment_work = synthetic_folder(tmp_path)
    events = [{'task_id': ids['task_id'], 'folder_id': ids['group_id'], 'kind': 'llm_response_saved',
        'data': {'component_id': row['component_id'], 'request_id': row['component_id'], 'kind': row['kind'],
            'request_sha256': row['request_sha256'], 'response_sha256': row['response_sha256'],
            'response_text': (Path(row['model_dir']) / 'response.txt').read_bytes().decode('utf8'),
            'response_path': str(Path(row['model_dir']) / 'response.txt')}} for row in admissions]
    async def records(_run):
        return list(reversed(events))
    async def saved(**row):
        if row['kind'] == 'postprocessing_complete' and final_failure == 'receipt':
            raise OSError('synthetic final receipt failure')
        events.append(row)
    @asynccontextmanager
    async def slot(_admission, _limits):
        if hasattr(_admission, 'admit'):
            assert await _admission.admit()
        yield
    class Backend:
        def attachment_work(self, _paper, _row):
            return Path(attachment_work)
    for module in (llm_handoff, postprocessing):
        monkeypatch.setattr(module, 'read_receipts', records)
        monkeypatch.setattr(module, 'save_receipt', saved)
    monkeypatch.setattr(postprocessing, 'admitted_slot', slot)
    parent = stage_store.admission.Admission(ids['task_id'])
    real_body, real_attachment = postprocessing._process_body, postprocessing.process_attachment
    calls = []

    async def exercise():
        await parent.start()
        inputs = await llm_handoff.build_postprocessing_inputs(
            identity=ids, manifest=manifest, paper=paper, backend=Backend())
        loop = asyncio.get_running_loop()
        def body(*args, **kwargs):
            result = real_body(*args, **kwargs)
            calls.append('body')
            asyncio.run_coroutine_threadsafe(parent.pause(), loop).result(3)
            return result
        def attachment(*args, **kwargs):
            assert not asyncio.run_coroutine_threadsafe(parent.running(), loop).result(3)
            calls.append('attachment')
            return real_attachment(*args, **kwargs)
        monkeypatch.setattr(postprocessing, '_process_body', body)
        monkeypatch.setattr(postprocessing, 'process_attachment', attachment)
        async def inventory():
            if final_failure == 'inventory':
                raise OSError('synthetic inventory receipt failure')
        operation = postprocessing.process_folder(inputs=inputs, catalog=catalog,
            paperflow_root=RELEASE / 'core', counter_factory=managed_counter_factory,
            output_root=tmp_path / 'output', admission=parent, limit_names=['synthetic'],
            before_complete=inventory)
        if final_failure:
            with pytest.raises(OSError):
                await operation
        else:
            result = await operation
            assert result['validation_status'] == 'passed' and len(result['attachments']) == 1
        assert calls == ['body', 'attachment']
        assert (await stage_store.stages.pause_progress(ids['task_id']))['state'] == (
            'unresolved' if final_failure else 'paused')
    asyncio.run(exercise())

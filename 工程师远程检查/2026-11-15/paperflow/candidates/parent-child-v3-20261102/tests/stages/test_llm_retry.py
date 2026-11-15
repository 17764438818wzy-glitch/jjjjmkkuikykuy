"""Explicitly retry only proven failures and retain every prior request."""
import asyncio
import io
import json
from pathlib import Path
import sys
from urllib.error import HTTPError
from uuid import uuid4

import pytest

from test_llm_request import Admission, Response, WORKSPACE, dependencies
sys.path.insert(0, str(WORKSPACE / 'candidates/postprocess-prefect-20261031/tests'))


@pytest.mark.parametrize('status', [401])
def test_manual_retry_preserves_failed_original_and_reuses_success(tmp_path, monkeypatch, status):
    module, receipts = dependencies(monkeypatch)
    admission = Admission()
    identity = {'task_id': str(uuid4()), 'group_id': 'folder', 'task_run_id': str(uuid4())}
    work = tmp_path / 'body'
    body = {'model': 'synthetic', 'messages': []}
    connection = {'base_url': 'http://synthetic.invalid/v1', 'api_key': 'synthetic', 'model': 'synthetic'}
    calls = []

    class Failed:
        def open(self, request, timeout):
            calls.append('failed')
            raise HTTPError(request.full_url, status, 'synthetic', {}, io.BytesIO(b'complete failure'))

    class Success:
        def open(self, _request, timeout):
            calls.append('retry')
            return Response(json.dumps({'choices': [{'message': {'content': 'retry succeeded'},
                'finish_reason': 'stop'}]}).encode())

    options = dict(identity=identity, component_id='body-1', request_id='body-1', kind='body',
        request_body_factory=lambda: body, connection=connection, work=work, admission=admission,
        limit_names=['synthetic'], check_history=lambda: None, check_source=lambda: None)
    with pytest.raises(HTTPError):
        asyncio.run(module.run_raw_component(**options, opener=Failed()))
    failed = receipts[-1]['data']
    assert receipts[-1]['kind'] == 'llm_request_failed'
    connection['api_key'] = 'corrected-synthetic-key'
    original_model = work / 'model'
    original_files = {p: p.read_bytes() for p in original_model.iterdir() if p.is_file()}

    retry = dict(identity=identity, component_id='body-1', retry_id='confirmed-retry-1', kind='body',
        original_work=work, original_model=original_model, expected_request_sha256=failed['request_sha256'],
        connection=connection, admission=admission, limit_names=['synthetic'],
        check_source=lambda: None, opener=Success())
    with pytest.raises(RuntimeError, match='人工'):
        asyncio.run(module.retry_raw_component(**retry))
    receipts.append({'kind': 'manual_retry', 'data': {'retry_id': 'confirmed-retry-1', 'state': 'scheduled'}})
    result = asyncio.run(module.retry_raw_component(**retry))
    assert Path(result['response_path']).read_text(encoding='utf8') == 'retry succeeded'
    assert all(path.read_bytes() == value for path, value in original_files.items())
    assert calls == ['failed', 'retry']
    started = [row for row in receipts if row['kind'] == 'llm_request_started']
    assert started[-1]['data']['retry_of'] == 'body-1'
    assert started[-1]['data']['retry_id'] == 'confirmed-retry-1'
    again = asyncio.run(module.retry_raw_component(**retry))
    assert again == result
    assert calls == ['failed', 'retry']


def test_manual_retry_cannot_send_an_unknown_request(tmp_path, monkeypatch):
    module, receipts = dependencies(monkeypatch)
    admission = Admission()
    identity = {'task_id': str(uuid4()), 'group_id': 'folder', 'task_run_id': str(uuid4())}
    calls = []

    class Timeout:
        def open(self, request, timeout):
            calls.append('sent')
            raise TimeoutError('synthetic timeout')

    work = tmp_path / 'body'
    connection = {'base_url': 'http://synthetic.invalid/v1', 'api_key': 'synthetic'}
    with pytest.raises(TimeoutError):
        asyncio.run(module.run_raw_component(identity=identity, component_id='body-1', request_id='body-1',
            kind='body', request_body_factory=lambda: {'model': 'synthetic', 'messages': []},
            connection=connection, work=work, admission=admission, limit_names=['synthetic'],
            check_history=lambda: None, check_source=lambda: None, opener=Timeout()))
    receipts.append({'kind': 'manual_retry', 'data': {'retry_id': 'retry-1', 'state': 'scheduled'}})
    with pytest.raises(RuntimeError, match='待核对'):
        asyncio.run(module.retry_raw_component(identity=identity, component_id='body-1', retry_id='retry-1',
            kind='body', original_work=work, original_model=work / 'model',
            expected_request_sha256=receipts[0]['data']['request_sha256'], connection=connection,
            admission=admission, limit_names=['synthetic'], check_source=lambda: None, opener=Timeout()))
    assert calls == ['sent']


def test_not_sent_resume_never_clears_older_unknown_even_with_local_journal(tmp_path, monkeypatch):
    module, receipts = dependencies(monkeypatch)
    identity = {'task_id': str(uuid4()), 'group_id': 'folder', 'task_run_id': str(uuid4())}
    model = tmp_path / 'body' / 'model'
    model.mkdir(parents=True)
    (model / 'request-state.json').write_text('{}', encoding='utf8')
    data = {'component_id': 'body-1', 'request_id': 'original', 'model_dir': str(model),
            'request_sha256': 'synthetic'}
    receipts.extend([{'kind': 'llm_request_unknown', 'data': data},
                     {'kind': 'llm_request_not_sent', 'data': data}])
    with pytest.raises(RuntimeError, match='待核对'):
        asyncio.run(module.resume_not_sent_component(identity=identity, component_id='body-1',
            kind='body', original_work=model.parent, original_model=model,
            expected_request_sha256='synthetic', connection={}, admission=Admission(),
            limit_names=['synthetic'], check_source=lambda: None))
    with pytest.raises(RuntimeError, match='待核对'):
        module.verify_request_history(list(reversed(receipts)), 'body-1', 'new-request',
            tmp_path / 'retry/model', retry_of='original', resume_not_sent=True)
    assert len(receipts) == 2


@pytest.mark.parametrize('failed_kind', ['body', 'attachment'])
@pytest.mark.parametrize('mode', ['failure', 'pause'])
def test_paper_retry_reuses_other_component_and_only_resends_proven_failure(tmp_path, monkeypatch, failed_kind, mode, managed_counter_factory):
    from contextlib import asynccontextmanager, contextmanager
    import preextract_request
    from paperflow_prefect.body_clean_handoff import save_projection
    from paperflow_prefect.records import read_json
    from paperflow_prefect.attachments import has_request_history
    from paperflow_prefect.request_admission import confirm_first_request
    from paperflow_v3.stages import pipeline, llm_operations, llm_handoff, postprocessing
    from test_synthetic_mainline_integration import synthetic_folder, save
    from test_body_adapter import FixtureCounter, RELEASE

    identity, manifest, paper, admissions, catalog, attachment_work = synthetic_folder(tmp_path)
    save_projection(manifest['body_work'], read_json(manifest['material']['path']),
                    read_json(manifest['reading_material']['path']))
    failed_row = next(row for row in admissions if row['kind'] == failed_kind)
    failed_model = Path(failed_row['model_dir'])
    answer = (failed_model / 'response.txt').read_bytes().decode('utf8')
    for name in ('request.json', 'request-state.json', 'response.txt', 'response-events.jsonl'):
        (failed_model / name).unlink()
    manifest_ref = save(tmp_path / 'handoff.json', manifest)
    events = [{'kind': 'llm_response_saved', 'data': {
        'component_id': row['component_id'], 'request_id': row['component_id'],
        'status': 'raw_response_saved', 'kind': row['kind'], 'request_sha256': row['request_sha256'],
        'response_sha256': row['response_sha256'],
        'response_text': (Path(row['model_dir']) / 'response.txt').read_bytes().decode('utf8'),
        'response_path': str(Path(row['model_dir']) / 'response.txt')}}
        for row in admissions if row is not failed_row]

    async def read_receipts(_child):
        return list(reversed(events))

    async def save_receipt(**row):
        events.append(row)

    @asynccontextmanager
    async def slot(_admission, _limits):
        if hasattr(_admission, 'admit'):
            assert await _admission.admit()
        yield

    class Settings:
        def path(self, key):
            assert key == 'template_path'
            return WORKSPACE.parent / '程序/resources/论文预提取_空白模板_v1.md'

    class Parent(Admission):
        async def wait(self):
            if not self.active:
                raise InterruptedError('folder is waiting at the next stage')

    parent = Parent()
    original_window = preextract_request._request_start_window
    pause_once = [mode == 'pause']

    @contextmanager
    def pause_before_admission(config, stop):
        with original_window(config, stop) as mark:
            def marked():
                mark()
                if pause_once[0]:
                    pause_once[0] = False
                    parent.active = False
            yield marked

    monkeypatch.setattr(preextract_request, '_request_start_window', pause_before_admission)

    class Backend:
        root = tmp_path / 'backend'
        settings = Settings()
        def body_work(self, _paper):
            return Path(manifest['body_work'])
        def attachment_work(self, _paper, _row):
            return Path(attachment_work)
        def paper_root(self, _paper):
            return tmp_path
        def confirm_history(self, _paper, row):
            work = self.body_work(_paper) if row['kind'] == 'body' else self.attachment_work(_paper, row)
            if not has_request_history(work) and not (work / 'first-request.json').exists():
                self.root.mkdir(parents=True, exist_ok=True)
                confirm_first_request(_paper, row, work, history_roots=[self.root])

    calls = []
    class Transport:
        def open(self, request, timeout):
            calls.append(json.loads(request.data))
            if mode == 'failure' and len(calls) == 1:
                raise HTTPError(request.full_url, 401, 'synthetic', {}, io.BytesIO(b'complete failure'))
            return Response(json.dumps({'choices': [{'message': {'content': answer},
                'finish_reason': 'stop'}]}, ensure_ascii=False).encode('utf8'))

    module, _unused = dependencies(monkeypatch)
    for current in (module, pipeline, llm_operations, llm_handoff, postprocessing):
        if hasattr(current, 'read_receipts'):
            monkeypatch.setattr(current, 'read_receipts', read_receipts)
        if hasattr(current, 'save_receipt'):
            monkeypatch.setattr(current, 'save_receipt', save_receipt)
        if hasattr(current, 'admitted_slot'):
            monkeypatch.setattr(current, 'admitted_slot', slot)
    monkeypatch.setattr(pipeline, 'stage_workspace', lambda *_args: tmp_path)
    monkeypatch.setattr(pipeline, 'stage_work_files', lambda _root, _paths: [])
    monkeypatch.setattr(pipeline, 'prepare_registered_folder', lambda *_args, **_kwargs: {
        'status': 'preprocessing_complete', **{key: identity[key] for key in
            ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id')},
        'manifest_path': manifest_ref['path'], 'manifest_sha256': manifest_ref['sha256']})
    monkeypatch.setattr(pipeline, 'settings_for_verified_receipt', lambda settings, _record: settings)
    monkeypatch.setattr(pipeline, 'UnifiedBackend', lambda *_args, **_kwargs: Backend())
    registered = {'paper': paper, **{key: identity[key] for key in
                                   ('task_id', 'group_id', 'paper_id', 'receipt_id')},
                  'receipt_path': manifest['receipt']['path'], 'receipt_sha256': manifest['receipt']['sha256']}
    options = dict(identity=identity, registered=registered, settings=None,
        connection={'model': 'synthetic-never-sent', 'base_url': 'http://synthetic.invalid',
                    'api_key': 'synthetic', 'max_tokens': 2048},
        admission=parent, limits={name: ['synthetic'] for name in
            ('preprocessing', 'body', 'attachment', 'postprocessing')}, catalog=catalog,
        paperflow_root=RELEASE / 'core', counter_factory=managed_counter_factory, output_root=tmp_path / 'result',
        opener=Transport())
    with pytest.raises(HTTPError if mode == 'failure' else InterruptedError):
        asyncio.run(pipeline.process_paper(**options))
    # The final transport gate observes a pause even after local preparation.
    assert len(calls) == (1 if mode == 'failure' else 0)
    if mode == 'failure':
        before = {path: path.read_bytes() for path in failed_model.iterdir() if path.is_file()}
    else:
        assert any(e['kind']=='llm_request_not_sent' for e in events)
        kept=next(r for r in admissions if r is not failed_row)
        before={p:p.read_bytes() for p in Path(kept['model_dir']).iterdir() if p.is_file()}
    if mode == 'failure':
        events.append({'kind': 'manual_retry', 'data': {'state': 'scheduled', 'retry_id': 'manual-1'}})
        result = asyncio.run(pipeline.process_paper(**options, retry_id='manual-1'))
    else:
        parent.active = True
        result = asyncio.run(pipeline.process_paper(**options))
    assert result['validation_status'] == 'passed'
    assert len(calls) == (2 if mode == 'failure' else 1)
    if mode == 'failure':
        assert calls[0] == calls[1]
    assert all(path.read_bytes() == value for path, value in before.items())
    assert asyncio.run(pipeline.process_paper(**options))['validation_status'] == 'passed'
    assert len(calls) == (2 if mode == 'failure' else 1)


def test_each_additional_attempt_requires_a_new_manual_retry_id(tmp_path, monkeypatch):
    module, receipts = dependencies(monkeypatch)
    admission = Admission()
    identity = {'task_id': str(uuid4()), 'group_id': 'folder', 'task_run_id': str(uuid4())}
    work = tmp_path / 'body'
    connection = {'base_url': 'http://synthetic.invalid/v1', 'api_key': 'synthetic', 'model': 'synthetic'}
    calls = []

    class Transport:
        def open(self, request, timeout):
            calls.append(request)
            if len(calls) < 3:
                raise HTTPError(request.full_url, 401, 'synthetic', {}, io.BytesIO(b'complete failure'))
            return Response(json.dumps({'choices': [{'message': {'content': 'completed'},
                'finish_reason': 'stop'}]}).encode())

    common = dict(identity=identity, component_id='body-1', kind='body', connection=connection,
                  admission=admission, limit_names=['synthetic'], check_source=lambda: None, opener=Transport())
    with pytest.raises(HTTPError):
        asyncio.run(module.run_raw_component(**common, request_id='body-1', work=work,
            request_body_factory=lambda: {'model': 'synthetic', 'messages': []}, check_history=lambda: None))
    first = next(row['data'] for row in reversed(receipts) if row['kind'] == 'llm_request_failed')
    retry = dict(**common, original_work=work, original_model=work / 'model',
                 expected_request_sha256=first['request_sha256'])
    receipts.append({'kind': 'manual_retry', 'data': {'retry_id': 'manual-1', 'state': 'scheduled'}})
    with pytest.raises(HTTPError):
        asyncio.run(module.retry_raw_component(**retry, retry_id='manual-1'))
    with pytest.raises(RuntimeError):
        asyncio.run(module.retry_raw_component(**retry, retry_id='manual-1'))
    assert len(calls) == 2
    receipts.append({'kind': 'manual_retry', 'data': {'retry_id': 'manual-2', 'state': 'scheduled'}})
    result = asyncio.run(module.retry_raw_component(**retry, retry_id='manual-2'))
    assert Path(result['response_path']).read_text(encoding='utf8') == 'completed'
    assert len(calls) == 3
    failures = [row['data'] for row in receipts if row['kind'] == 'llm_request_failed']
    assert len(failures) == 2 and all(Path(row['model_dir'], 'http-error.txt').is_file() for row in failures)


def test_pause_after_local_intent_but_before_final_admission_resumes_without_overwriting(tmp_path, monkeypatch):
    from contextlib import contextmanager
    import preextract_request
    module, receipts = dependencies(monkeypatch)
    admission = Admission()
    identity = {'task_id': str(uuid4()), 'group_id': 'folder', 'task_run_id': str(uuid4())}
    work = tmp_path / 'body'
    connection = {'base_url': 'http://synthetic.invalid/v1', 'api_key': 'synthetic', 'model': 'synthetic'}
    calls = []
    original_window = preextract_request._request_start_window
    pause_once = [True]

    @contextmanager
    def paused_window(config, stop):
        with original_window(config, stop) as mark:
            def paused_mark():
                mark()
                if pause_once[0]:
                    pause_once[0] = False
                    admission.active = False
            yield paused_mark

    class Opener:
        def open(self, _request, timeout):
            calls.append('sent')
            return Response(json.dumps({'choices': [{'message': {'content': 'completed'},
                'finish_reason': 'stop'}]}).encode())

    monkeypatch.setattr(preextract_request, '_request_start_window', paused_window)
    with pytest.raises(InterruptedError):
        asyncio.run(module.run_raw_component(identity=identity, component_id='body-1', request_id='body-1',
            kind='body', request_body_factory=lambda: {'model': 'synthetic', 'messages': []},
            connection=connection, work=work, admission=admission, limit_names=['synthetic'],
            check_history=lambda: None, check_source=lambda: None, opener=Opener()))
    assert receipts[-1]['kind'] == 'llm_request_not_sent'
    assert not any(row['kind'] == 'llm_request_started' for row in receipts)
    original = {path: path.read_bytes() for path in (work / 'model').iterdir() if path.is_file()}
    assert original and not calls
    admission.active = True
    result = asyncio.run(module.resume_not_sent_component(identity=identity, component_id='body-1', kind='body',
        original_work=work, original_model=work / 'model',
        expected_request_sha256=receipts[-1]['data']['request_sha256'], connection=connection,
        admission=admission, limit_names=['synthetic'], check_source=lambda: None, opener=Opener()))
    assert Path(result['response_path']).read_text(encoding='utf8') == 'completed'
    assert calls == ['sent']
    assert all(path.read_bytes() == value for path, value in original.items())


def test_changed_failure_evidence_never_authorizes_a_retry(tmp_path, monkeypatch):
    module, receipts = dependencies(monkeypatch)
    identity = {'task_id': str(uuid4()), 'group_id': 'folder', 'task_run_id': str(uuid4())}
    work = tmp_path / 'body'
    connection = {'base_url': 'http://synthetic.invalid/v1', 'api_key': 'synthetic', 'model': 'synthetic'}
    calls = []

    class Rejected:
        def open(self, request, timeout):
            calls.append(request)
            raise HTTPError(request.full_url, 401, 'synthetic', {}, io.BytesIO(b'original failure'))

    with pytest.raises(HTTPError):
        asyncio.run(module.run_raw_component(identity=identity, component_id='body-1', request_id='body-1',
            kind='body', request_body_factory=lambda: {'model': 'synthetic', 'messages': []},
            connection=connection, work=work, admission=Admission(), limit_names=['synthetic'],
            check_history=lambda: None, check_source=lambda: None, opener=Rejected()))
    failed = receipts[-1]['data']
    (work / 'model/http-error.txt').write_bytes(b'changed failure')
    receipts.append({'kind': 'manual_retry', 'data': {'retry_id': 'manual-1', 'state': 'scheduled'}})
    with pytest.raises(ValueError, match='证据发生变化'):
        asyncio.run(module.retry_raw_component(identity=identity, component_id='body-1', retry_id='manual-1',
            kind='body', original_work=work, original_model=work / 'model',
            expected_request_sha256=failed['request_sha256'], connection=connection,
            admission=Admission(), limit_names=['synthetic'], check_source=lambda: None, opener=Rejected()))
    assert len(calls) == 1

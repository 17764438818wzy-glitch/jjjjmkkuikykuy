"""Synthetic folder through real saved-response and postprocessing adapters."""
import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[2]
WORKSPACE = ROOT.parents[1]
sys.path[:0] = [str(ROOT), str(WORKSPACE / 'prefect-paperflow'),
                str(WORKSPACE / 'candidates/postprocess-prefect-20261031/tests')]
from test_synthetic_mainline_integration import synthetic_folder, save
from test_body_adapter import FixtureCounter, RELEASE
from paperflow_prefect.body_clean_handoff import save_projection
from paperflow_prefect.records import read_json
sys.path.insert(0, str(ROOT / 'tests/control'))
from test_folder_pause import stage_store


def test_single_paper_pipeline_reuses_real_replies_and_makes_complete_receipt(tmp_path, monkeypatch, managed_counter_factory):
    try:
        from paperflow_v3.stages import pipeline, llm_request, llm_operations, llm_handoff, postprocessing
    except ImportError:
        pytest.fail('The candidate single-paper pipeline is missing')
    identity, manifest, paper, admissions, catalog, attachment_work = synthetic_folder(tmp_path)
    save_projection(manifest['body_work'], read_json(manifest['material']['path']),
                    read_json(manifest['reading_material']['path']))
    manifest_ref = save(tmp_path / 'preprocessing-handoff.json', manifest)
    events = [{'task_id': identity['task_id'], 'folder_id': identity['group_id'],
        'kind': 'llm_response_saved', 'data': {'component_id': row['component_id'],
            'request_id': row['component_id'], 'kind': row['kind'], 'status': 'raw_response_saved',
            'request_sha256': row['request_sha256'], 'response_sha256': row['response_sha256'],
            'response_text': (Path(row['model_dir']) / 'response.txt').read_bytes().decode('utf8'),
            'response_path': str(Path(row['model_dir']) / 'response.txt')}} for row in admissions]
    original_responses = list(events)
    llm_slots = []

    async def read_receipts(_child):
        return list(reversed(events))

    async def save_receipt(**row):
        events.append(row)

    class Admission:
        async def running(self):
            return True
        def running_sync(self):
            return True
        async def wait(self):
            return None

    @asynccontextmanager
    async def isolated_slot(_admission, _limits):
        if hasattr(_admission, 'admit'):
            assert await _admission.admit()
        yield

    class Backend:
        root = tmp_path / 'backend'
        def paper_root(self, _paper):
            return tmp_path
        def body_work(self, _paper):
            return Path(manifest['body_work'])
        def attachment_work(self, _paper, _row):
            return Path(attachment_work)
        def confirm_history(self, _paper, _row):
            pass

    class NoTransport:
        def open(self, *_args, **_kwargs):
            pytest.fail('successful saved responses must never be sent again')

    for module in (pipeline, llm_request, llm_operations, llm_handoff, postprocessing):
        monkeypatch.setattr(module, 'save_receipt', save_receipt)
        if hasattr(module, 'read_receipts'):
            monkeypatch.setattr(module, 'read_receipts', read_receipts)
        if hasattr(module, 'admitted_slot'):
            monkeypatch.setattr(module, 'admitted_slot', isolated_slot)
    @asynccontextmanager
    async def tracked_llm_slot(_admission, _limits):
        if hasattr(_admission, 'admit'):
            assert await _admission.admit()
        llm_slots.append(1)
        yield
    monkeypatch.setattr(llm_request, 'admitted_slot', tracked_llm_slot)
    def prepare(*_args, **_kwargs):
        assert events[-1]['kind'] == 'preprocessing_started'
        return {'status': 'preprocessing_complete', **{key: identity[key] for key in
            ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id')},
            'manifest_path': manifest_ref['path'], 'manifest_sha256': manifest_ref['sha256']}
    monkeypatch.setattr(pipeline, 'prepare_registered_folder', prepare)
    monkeypatch.setattr(pipeline, 'settings_for_verified_receipt', lambda settings, record: settings)
    monkeypatch.setattr(pipeline, 'UnifiedBackend', lambda *_args, **_kwargs: Backend())
    monkeypatch.setattr(pipeline, 'stage_workspace', lambda *_args: tmp_path)
    stage_file = str(Path(manifest['body_work']) / 'model' / 'request-state.json')
    monkeypatch.setattr(pipeline, 'stage_work_files', lambda _root, _paths: [stage_file])
    registered = {'paper': paper, **{key: identity[key] for key in
                                   ('task_id', 'group_id', 'paper_id', 'receipt_id')},
                  'receipt_path': manifest['receipt']['path'], 'receipt_sha256': manifest['receipt']['sha256']}
    result = asyncio.run(pipeline.process_paper(identity=identity, registered=registered, settings=None,
        connection={'model': 'synthetic-never-sent', 'base_url': 'http://synthetic.invalid',
                    'api_key': 'synthetic'}, admission=Admission(),
        limits={name: ['synthetic'] for name in ('preprocessing', 'body', 'attachment', 'postprocessing')},
        catalog=catalog, paperflow_root=RELEASE / 'core', counter_factory=managed_counter_factory,
        output_root=tmp_path / 'postprocessing', opener=NoTransport()))
    assert result['validation_status'] == 'passed'
    assert not llm_slots
    assert [row for row in events if row['kind'] == 'llm_response_saved'] == original_responses
    assert len(result['attachments']) == 1
    assert any(row['kind'] == 'llm_folder_complete' for row in events)
    assert any(row['kind'] == 'postprocessing_complete' for row in events)
    assert result['stage_work_files'] == [stage_file]
    assert any(row['kind'] == 'stage_work_inventory' and row['data']['files'] == [stage_file]
               for row in events)
    from paperflow_v3.delivery.server_handoff import read_complete_handoff
    delivered_input = read_complete_handoff(identity, paper, list(reversed(events)))
    assert delivered_input['handoff']['identity'] == identity
    assert delivered_input['stage_work_files'] == [stage_file]


def test_one_attachment_failure_stops_later_items_and_allows_started_body_to_finish(tmp_path, monkeypatch):
    import copy
    from paperflow_v3.stages import pipeline

    identity, manifest, paper, _admissions, _catalog, _attachment_work = synthetic_folder(tmp_path)
    paper = copy.deepcopy(paper)
    manifest = copy.deepcopy(manifest)
    second = dict(paper['attachments'][0], file_id='another-attachment')
    paper['attachments'].append(second)
    manifest['attachments'].append(second)
    manifest_ref = save(tmp_path / 'handoff.json', manifest)
    started = asyncio.Event()
    release = asyncio.Event()
    calls = []

    class Admission:
        async def wait(self):
            pass
        async def running(self):
            return True
        def running_sync(self):
            return True

    @asynccontextmanager
    async def slot(_admission, _limits):
        if hasattr(_admission, 'admit'):
            assert await _admission.admit()
        yield

    async def saved(**_row):
        pass

    async def no_records(_child):
        return []

    async def body(**_kwargs):
        assert await _kwargs['admission'].admit()
        calls.append('body-started')
        started.set()
        await release.wait()
        calls.append('body-finished')

    async def attachment(**kwargs):
        await started.wait()
        calls.append(kwargs['row']['file_id'])
        release.set()
        raise ValueError('synthetic attachment failure')

    class Backend:
        pass

    monkeypatch.setattr(pipeline, 'stage_workspace', lambda *_args: tmp_path)
    monkeypatch.setattr(pipeline, 'admitted_slot', slot)
    monkeypatch.setattr(pipeline, 'save_receipt', saved)
    monkeypatch.setattr(pipeline, 'read_receipts', no_records)
    monkeypatch.setattr(pipeline, 'prepare_registered_folder', lambda *_args, **_kwargs: {
        'status': 'preprocessing_complete', **{key: identity[key] for key in
            ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id')},
        'manifest_path': manifest_ref['path'], 'manifest_sha256': manifest_ref['sha256']})
    monkeypatch.setattr(pipeline, 'settings_for_verified_receipt', lambda settings, _record: settings)
    monkeypatch.setattr(pipeline, 'UnifiedBackend', lambda *_args, **_kwargs: Backend())
    monkeypatch.setattr(pipeline, 'run_single_body', body)
    monkeypatch.setattr(pipeline, 'run_attachment', attachment)
    registered = {'paper': paper, **{key: identity[key] for key in
                                   ('task_id', 'group_id', 'paper_id', 'receipt_id')},
                  'receipt_path': manifest['receipt']['path'], 'receipt_sha256': manifest['receipt']['sha256']}
    with pytest.raises(ValueError, match='synthetic attachment failure'):
        asyncio.run(pipeline.process_paper(identity=identity, registered=registered, settings=None,
            connection={}, admission=Admission(),
            limits={name: ['synthetic'] for name in ('preprocessing', 'body', 'attachment', 'postprocessing')},
            catalog={}, paperflow_root=tmp_path, counter_factory=None, output_root=tmp_path / 'output'))
    assert calls == ['body-started', paper['attachments'][0]['file_id'], 'body-finished']


@pytest.mark.parametrize('pause_at_resource', [False, True])
def test_pause_drains_whole_llm_folder_and_leaves_postprocessing_waiting(
        tmp_path, monkeypatch, stage_store, pause_at_resource):
    from copy import deepcopy
    from paperflow_v3.stages import pipeline
    ids, manifest, paper, _admissions, _catalog, _work = synthetic_folder(tmp_path)
    manifest, paper = deepcopy(manifest), deepcopy(paper)
    paper['attachments'].append(dict(paper['attachments'][0], file_id='second-attachment'))
    manifest['attachments'] = paper['attachments']
    manifest_ref = save(tmp_path / 'handoff.json', manifest)
    parent = stage_store.admission.Admission(ids['task_id'])
    events, calls = [], []
    released = asyncio.Event()
    held, interrupted_entry = 0, False

    async def records(_run):
        return list(reversed(events))
    async def saved(**row):
        events.append(row)
    @asynccontextmanager
    async def slot(_parent, _limits):
        nonlocal held, interrupted_entry
        while True:
            await _parent.wait()
            held += 1
            try:
                if pause_at_resource and not interrupted_entry and _limits == ['preprocessing']:
                    interrupted_entry = True
                    await parent.pause()
                if not await _parent.admit():
                    continue
                yield
                return
            finally:
                held -= 1
                if interrupted_entry:
                    released.set()
    async def body(**kwargs):
        assert await kwargs['admission'].admit()
        calls.append('body')
        await parent.pause()
        assert await kwargs['admission'].running(), 'pause cut the folder after its body'
    async def attachment(**kwargs):
        await kwargs['admission'].wait()
        calls.append(kwargs['row']['file_id'])
    drained = asyncio.Event()
    async def inputs(**kwargs):
        drained.set()
        return {'identity': ids}
    async def post(**kwargs):
        await kwargs['admission'].wait()
        calls.append('postprocessing')
        return {'validation_status': 'passed'}
    class Backend:
        def paper_root(self, _paper):
            return tmp_path
        def attachment_work(self, _paper, _row):
            return tmp_path
    monkeypatch.setattr(pipeline, 'stage_workspace', lambda *_args: tmp_path)
    monkeypatch.setattr(pipeline, 'stage_work_files', lambda *_args: [])
    monkeypatch.setattr(pipeline, 'admitted_slot', slot)
    monkeypatch.setattr(pipeline, 'save_receipt', saved)
    monkeypatch.setattr(pipeline, 'read_receipts', records)
    monkeypatch.setattr(pipeline, 'settings_for_verified_receipt', lambda settings, _record: settings)
    monkeypatch.setattr(pipeline, 'UnifiedBackend', lambda *_args, **_kwargs: Backend())
    monkeypatch.setattr(pipeline, 'run_single_body', body)
    monkeypatch.setattr(pipeline, 'run_attachment', attachment)
    monkeypatch.setattr(pipeline, 'build_postprocessing_inputs', inputs)
    monkeypatch.setattr(pipeline, 'process_folder', post)
    monkeypatch.setattr(pipeline, 'prepare_registered_folder', lambda *_args, **_kwargs: {
        'status': 'preprocessing_complete', **{key: ids[key] for key in
            ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id')},
        'manifest_path': manifest_ref['path'], 'manifest_sha256': manifest_ref['sha256']})
    registered = {'paper': paper, **{key: ids[key] for key in ('task_id', 'group_id', 'paper_id', 'receipt_id')},
        'receipt_path': manifest['receipt']['path'], 'receipt_sha256': manifest['receipt']['sha256']}

    async def exercise():
        await parent.start()
        task = asyncio.create_task(pipeline.process_paper(identity=ids, registered=registered, settings=None,
            connection={}, admission=parent,
            limits={name: [name] for name in ('preprocessing', 'body', 'attachment', 'postprocessing')},
            catalog={}, paperflow_root=tmp_path, counter_factory=None, output_root=tmp_path / 'output'))
        try:
            if pause_at_resource:
                await asyncio.wait_for(released.wait(), 1)
                assert held == 0, 'paused folder retained a shared preprocessing slot'
                assert not stage_store.history, 'folder started after pause closed its gate'
                await parent.start()
            async with asyncio.timeout(2):
                while not drained.is_set() and not task.done():
                    await asyncio.sleep(.01)
            if task.done():
                await task
            assert drained.is_set()
            assert calls == ['body', paper['attachments'][0]['file_id'], 'second-attachment']
            assert not task.done()
            assert (await stage_store.stages.pause_progress(ids['task_id']))['state'] == 'paused'
        finally:
            await parent.start()
            await task
    asyncio.run(exercise())

"""One synthetic partitioned paper through the actual local stage chain."""
import asyncio
from contextlib import asynccontextmanager, contextmanager
import io
import json
from pathlib import Path
import sys
from urllib.error import HTTPError

import pytest

ROOT = Path(__file__).resolve().parents[2]
WORKSPACE = ROOT.parents[1]
sys.path[:0] = [str(ROOT), str(WORKSPACE / 'prefect-paperflow'),
                str(WORKSPACE / 'candidates/postprocess-prefect-20261031/tests')]
from test_synthetic_mainline_integration import synthetic_folder, save
from test_body_adapter import FixtureCounter, RELEASE
from paperflow_prefect.body_clean_handoff import save_projection
from paperflow_prefect.records import read_json
from preextract_contract import load_template
from preextract_partition_run import create_plan


@pytest.mark.parametrize('failed_part', [0, 1, 2, 3, 4, 5])
def test_partitioned_pipeline_submits_all_parts_then_one_global_ranking(tmp_path, monkeypatch, failed_part, managed_counter_factory):
    from paperflow_v3.stages import (pipeline, llm_operations, llm_request, llm_handoff,
                                     postprocessing, whole_paper_ranking, body_adapter)

    identity, manifest, paper, admissions, catalog, attachment_work = synthetic_folder(tmp_path, 2)
    work = Path(manifest['body_work'])
    evidence = body_adapter.frozen_plan_evidence(work, manifest['capacity']['plan_identity'])
    plan = body_adapter._load_plan(evidence, manifest['capacity']['plan_identity'])
    body_rows = [row for row in admissions if row['kind'] == 'body']
    body_replies = [(Path(row['model_dir']) / 'response.txt').read_bytes().decode('utf8')
                    for row in body_rows]
    # Rebuild only synthetic fixture files as a native, initially unsubmitted plan.
    for name in ('partition-state.json', 'partition-plan.json', 'partition-material.json'):
        (work / 'model' / name).unlink()
    for row in body_rows:
        model = Path(row['model_dir'])
        for name in ('planned-request.json', 'material.json', 'request.json', 'request-state.json',
                     'response.txt', 'response-events.jsonl'):
            (model / name).unlink()
    original = read_json(manifest['material']['path'])
    reading = read_json(manifest['reading_material']['path'])
    save_projection(work, original, reading)
    template, _ = load_template()
    connection = {'model': 'synthetic-never-sent', 'base_url': 'http://synthetic.invalid',
                  'api_key': 'synthetic', 'max_tokens': 8192, 'context_window': 300000}
    create_plan(plan, original, reading, work, template, connection)
    manifest_ref = save(tmp_path / 'preprocessing-handoff.json', manifest)

    events = [{'task_id': identity['task_id'], 'folder_id': identity['group_id'],
        'kind': 'llm_response_saved', 'data': {
        'component_id': row['component_id'], 'request_id': row['component_id'],
        'kind': row['kind'], 'status': 'raw_response_saved', 'request_sha256': row['request_sha256'],
        'response_sha256': row['response_sha256'],
        'response_text': (Path(row['model_dir']) / 'response.txt').read_bytes().decode('utf8'),
        'response_path': str(Path(row['model_dir']) / 'response.txt')}}
        for row in admissions if row['kind'] == 'attachment']

    async def read_receipts(_child):
        return list(reversed(events))

    async def save_receipt(**row):
        events.append(row)

    def save_receipt_sync(**row):
        events.append(row)

    class Admission:
        active = True
        async def running(self):
            return self.active
        def running_sync(self):
            return self.active
        async def wait(self):
            if not self.active:
                raise InterruptedError('folder is waiting at the next stage')

    admission = Admission()
    if failed_part in (4, 5):
        import preextract_request
        original_window = preextract_request._request_start_window
        starts = []

        @contextmanager
        def paused_window(config, stop):
            with original_window(config, stop) as mark:
                def marked():
                    mark()
                    starts.append(1)
                    if len(starts) == (1 if failed_part == 4 else 3):
                        admission.active = False
                yield marked

        monkeypatch.setattr(preextract_request, '_request_start_window', paused_window)

    llm_slots = []

    @asynccontextmanager
    async def async_slot(_admission, _limits):
        if hasattr(_admission, 'admit'):
            assert await _admission.admit()
        yield

    @asynccontextmanager
    async def async_llm_slot(_admission, _limits):
        if hasattr(_admission, 'admit'):
            assert await _admission.admit()
        llm_slots.append('whole-ranking')
        yield

    @contextmanager
    def sync_slot(_admission, _limits):
        if hasattr(_admission, 'admit_sync'):
            assert _admission.admit_sync()
        llm_slots.append('body-part')
        yield

    class Settings:
        def path(self, key):
            assert key == 'template_path'
            return WORKSPACE.parent / '程序/resources/论文预提取_空白模板_v1.md'

    class Backend:
        root = tmp_path / 'backend'
        settings = Settings()
        def paper_root(self, _paper):
            return tmp_path
        def body_work(self, _paper):
            return work
        def attachment_work(self, _paper, _row):
            return Path(attachment_work)
        def confirm_history(self, _paper, _row):
            from paperflow_prefect.attachments import has_request_history
            from paperflow_prefect.request_admission import confirm_first_request
            selected = work if _row['kind'] == 'body' else Path(attachment_work)
            if not has_request_history(selected) and not (selected / 'first-request.json').exists():
                self.root.mkdir(parents=True, exist_ok=True)
                confirm_first_request(_paper, _row, selected, history_roots=[self.root])

    class Response(io.BytesIO):
        headers = {'Content-Type': 'application/json'}

    calls = []
    class SyntheticOpener:
        def open(self, request, timeout):
            payload = json.loads(request.data)
            system = payload['messages'][0]['content']
            if system == 'fixture':
                index = next(i for i, part in enumerate(plan['parts']) if part['body'] == payload)
                calls.append(f'body-{index + 1}')
                if failed_part in (1, 2) and index + 1 == failed_part and calls.count(f'body-{failed_part}') == 1:
                    raise HTTPError(request.full_url, 401, 'synthetic failure', {}, io.BytesIO(b'complete failure'))
                raw = body_replies[index]
            else:
                assert '最终判断' in system
                projected = json.loads(payload['messages'][1]['content'][0]['text'])
                rows = projected['material']['reading_context']['ranking_inputs']
                assert [row['content'] for row in rows] == body_replies
                units = [row['unit_id_mapping'][0]['merged_id'] for row in rows]
                assert units == ['P001_U001', 'P002_U001']
                raw = json.dumps({'routing_candidates': [
                    {'code': '080901', 'name': '计算机科学与技术', 'reason': '全部分段的全文依据',
                     'unit_ids': units, 'source_refs': []}], 'limitations': []}, ensure_ascii=False)
                calls.append('whole-ranking')
                if failed_part == 3 and calls.count('whole-ranking') == 1:
                    raise HTTPError(request.full_url, 401, 'synthetic failure', {}, io.BytesIO(b'complete failure'))
            return Response(json.dumps({'choices': [{'message': {'content': raw},
                'finish_reason': 'stop'}]}, ensure_ascii=False).encode('utf8'))

    for module in (pipeline, llm_operations, llm_request, llm_handoff, postprocessing, whole_paper_ranking):
        if hasattr(module, 'save_receipt'):
            monkeypatch.setattr(module, 'save_receipt', save_receipt)
        if hasattr(module, 'read_receipts'):
            monkeypatch.setattr(module, 'read_receipts', read_receipts)
            if hasattr(module,'read_receipts_sync'):
                monkeypatch.setattr(module,'read_receipts_sync',lambda _child:list(reversed(events)))
        if hasattr(module, 'admitted_slot'):
            monkeypatch.setattr(module, 'admitted_slot', async_slot)
    monkeypatch.setattr(llm_operations, 'save_receipt_sync', save_receipt_sync)
    monkeypatch.setattr(llm_operations, 'admitted_sync_slot', sync_slot)
    monkeypatch.setattr(llm_request, 'admitted_slot', async_llm_slot)
    monkeypatch.setattr(pipeline, 'prepare_registered_folder', lambda *_args, **_kwargs: {
        'status': 'preprocessing_complete', **{key: identity[key] for key in
            ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id')},
        'manifest_path': manifest_ref['path'], 'manifest_sha256': manifest_ref['sha256']})
    monkeypatch.setattr(pipeline, 'settings_for_verified_receipt', lambda settings, _record: settings)
    monkeypatch.setattr(pipeline, 'UnifiedBackend', lambda *_args, **_kwargs: Backend())
    monkeypatch.setattr(pipeline, 'stage_workspace', lambda *_args: tmp_path)
    stage_file = str(work / 'model' / 'partition-state.json')
    monkeypatch.setattr(pipeline, 'stage_work_files', lambda _root, _paths: [stage_file])
    registered = {'paper': paper, **{key: identity[key] for key in
                                   ('task_id', 'group_id', 'paper_id', 'receipt_id')},
                  'receipt_path': manifest['receipt']['path'], 'receipt_sha256': manifest['receipt']['sha256']}
    options = dict(identity=identity, registered=registered, settings=None, connection=connection,
        admission=admission, limits={name: ['synthetic'] for name in
            ('preprocessing', 'body', 'attachment', 'postprocessing')}, catalog=catalog,
        paperflow_root=RELEASE / 'core', counter_factory=managed_counter_factory, output_root=tmp_path / 'output',
        opener=SyntheticOpener())
    before = None
    if failed_part in (1, 2, 3):
        with pytest.raises(HTTPError):
            asyncio.run(pipeline.process_paper(**options))
        assert calls == (['body-1', 'body-2', 'whole-ranking'] if failed_part == 3 else
                         [f'body-{index}' for index in range(1, failed_part + 1)])
        failed_model = Path(next(row['data']['model_dir'] for row in reversed(events)
                                 if row.get('kind') == 'llm_request_failed'))
        before = {path: path.read_bytes() for path in failed_model.iterdir() if path.is_file()}
        events.append({'kind': 'manual_retry', 'data': {'state': 'scheduled', 'retry_id': 'manual-part-2'}})
        result = asyncio.run(pipeline.process_paper(**options, retry_id='manual-part-2'))
    elif failed_part in (4, 5):
        with pytest.raises(InterruptedError):
            asyncio.run(pipeline.process_paper(**options))
        assert calls == ([] if failed_part == 4 else ['body-1','body-2'])
        assert not any(row['kind'] == 'llm_folder_complete' for row in events)
        before={Path(row['data']['response_path']):Path(row['data']['response_path']).read_bytes()
                for row in events if row['kind']=='llm_response_saved'}
        assert any(row.get('kind') == 'llm_request_not_sent' for row in events)
        admission.active = True
        result = asyncio.run(pipeline.process_paper(**options))
    else:
        result = asyncio.run(pipeline.process_paper(**options))
    assert result['validation_status'] == 'passed'
    expected_calls = [f'body-{index}' for index in range(1, 3)]
    if failed_part in (1, 2):
        expected_calls.insert(failed_part, f'body-{failed_part}')
    expected_calls.append('whole-ranking')
    if failed_part == 3:
        expected_calls.append('whole-ranking')
    assert calls == expected_calls
    if before:
        assert all(path.read_bytes() == raw for path, raw in before.items())
    profile = json.loads((Path(result['body']['package_path']) / 'profile.json').read_text(encoding='utf8'))
    assert profile['routing_candidates'][0]['unit_ids'] == ['P001_U001', 'P002_U001']
    assert any(row['kind'] == 'llm_folder_complete' and row['data']['ranking_complete'] for row in events)
    from paperflow_v3.delivery.server_handoff import read_complete_handoff
    assert read_complete_handoff(identity, paper, list(reversed(events)))['stage_work_files'] == [stage_file]
    saved_before = [row for row in events if row['kind'] == 'llm_response_saved']
    slots_before = list(llm_slots)
    if failed_part == 0:
        # Simulate interruption after the ranking reply was saved but before its completion receipt.
        events[:] = [row for row in events if row['kind'] != 'llm_whole_ranking_complete']
    again = asyncio.run(pipeline.process_paper(**options))
    assert again['validation_status'] == 'passed'
    assert calls == expected_calls
    assert llm_slots == slots_before
    assert [row for row in events if row['kind'] == 'llm_response_saved'] == saved_before

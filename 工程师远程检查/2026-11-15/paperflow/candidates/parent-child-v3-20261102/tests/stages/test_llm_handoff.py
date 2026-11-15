"""Build an actual postprocessing input from synthetic server response receipts."""
import asyncio
from contextlib import asynccontextmanager
import hashlib
import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[2]
WORKSPACE = ROOT.parents[1]
sys.path[:0] = [str(ROOT), str(WORKSPACE / 'prefect-paperflow'),
                str(WORKSPACE / 'candidates/postprocess-prefect-20261031/tests')]
from test_synthetic_mainline_integration import file_ref, synthetic_folder, save
from test_handoff_inputs import compact
from paperflow_prefect.llm_queue.folder_completion import expected_components


@pytest.mark.parametrize('parts', [1, 2])
def test_all_original_responses_are_bound_before_llm_passes(tmp_path, monkeypatch, parts, managed_counter_factory):
    try:
        from paperflow_v3.stages import llm_handoff
    except ImportError:
        pytest.fail('The candidate raw-response handoff is missing')
    identity, manifest, paper, admissions, catalog, attachment_work = synthetic_folder(tmp_path, parts)
    events = []
    for row in admissions:
        events.append({'kind': 'llm_response_saved', 'data': {
            'component_id': row['component_id'], 'request_id': row['component_id'],
            'kind': row['kind'], 'request_sha256': row['request_sha256'],
            'response_sha256': row['response_sha256'],
            'response_text': (Path(row['model_dir']) / 'response.txt').read_bytes().decode('utf8'),
            'response_path': str(Path(row['model_dir']) / 'response.txt')}})
    saved = []

    async def read_receipts(child):
        assert child == identity['task_run_id']
        return list(events)

    async def save_receipt(**row):
        saved.append(row)

    monkeypatch.setattr(llm_handoff, 'read_receipts', read_receipts)
    monkeypatch.setattr(llm_handoff, 'save_receipt', save_receipt)

    class Backend:
        def attachment_work(self, _paper, _row):
            return Path(attachment_work)

    if parts > 1:
        with pytest.raises(ValueError, match='全文专业'):
            asyncio.run(llm_handoff.build_postprocessing_inputs(
                identity=identity, manifest=manifest, paper=paper, backend=Backend()))
        assert not saved
        model = tmp_path / 'whole-ranking/model'
        request = {'model': 'synthetic-ranking', 'messages': []}
        raw = json.dumps({'routing_candidates': [{'code': '080901', 'name': '计算机科学与技术',
                            'reason': '全文依据', 'unit_ids': [], 'source_refs': []}], 'limitations': []},
                         ensure_ascii=False)
        save(model / 'request.json', request)
        (model / 'response.txt').write_text(raw, encoding='utf8')
        response = file_ref(model / 'response.txt')
        save(model / 'response-events.jsonl', {'choices': [{'message': {'content': raw}, 'finish_reason': 'stop'}]})
        save(model / 'request-state.json', {'status': 'response_complete', 'finish_reason': 'stop',
            'request_sha256': compact(request), 'response_sha256': response['sha256']})
        rank = {'component_id': 'whole-ranking', 'request_id': 'whole-ranking', 'kind': 'body',
                'request_sha256': compact(request), 'response_sha256': response['sha256'],
                'response_path': response['path'], 'response_text': raw}
        events.append({'kind': 'llm_response_saved', 'data': rank})
        components = [item for item in expected_components(manifest, identity['task_run_id']) if item['kind'] == 'body']
        events.append({'kind': 'llm_whole_ranking_complete', 'data': {**rank, 'status': 'raw_complete',
            'coverage': [item['part_id'] for item in components],
            'part_responses': [{'part_id': item['part_id'], 'response_sha256': next(
                row['response_sha256'] for row in admissions if row['component_id'] == item['component_id'])}
                for item in components]}})

    inputs = asyncio.run(llm_handoff.build_postprocessing_inputs(
        identity=identity, manifest=manifest, paper=paper, backend=Backend()))
    assert inputs['identity'] == identity
    assert len(inputs['body']['responses']) == parts
    assert len(inputs['attachments']) == 1
    assert saved[-1]['kind'] == 'llm_folder_complete'
    assert saved[-1]['data']['status'] == 'raw_complete'
    assert saved[-1]['data']['ranking_complete'] is True
    from paperflow_v3.control.folder_status import folder_status
    assert folder_status([saved[-1]])['llm_result'] == 'passed'

    # The original response must be preserved on the server before local cleanup.
    saved.clear()
    missing = next(row for row in events if row['kind'] == 'llm_response_saved')
    response_text = missing['data'].pop('response_text')
    with pytest.raises(ValueError, match='服务器'):
        asyncio.run(llm_handoff.build_postprocessing_inputs(
            identity=identity, manifest=manifest, paper=paper, backend=Backend()))
    assert saved == []
    missing['data']['response_text'] = response_text

    # A complete response with invalid JSON passes the LLM stage, then fails
    # in the real postprocessor. An incomplete stream cannot pass LLM.
    from paperflow_v3.stages import postprocessing
    from test_body_adapter import FixtureCounter, RELEASE
    target = next(row['data'] for row in events if row['kind'] == 'llm_response_saved'
                  and (row['data']['component_id'] == 'whole-ranking' if parts > 1
                       else row['data']['kind'] == 'body'))
    ranking = next((row['data'] for row in events if row['kind'] == 'llm_whole_ranking_complete'), None)
    target_before = dict(target)
    ranking_before = dict(ranking) if ranking else None
    model = Path(target['response_path']).parent
    files = [model / name for name in ('response.txt', 'request-state.json', 'response-events.jsonl')]
    original_files = {path: path.read_bytes() for path in files}
    raw = '完整保存但不是 JSON'
    files[0].write_bytes(raw.encode('utf8'))
    digest = hashlib.sha256(raw.encode('utf8')).hexdigest()
    target.update(response_text=raw, response_sha256=digest)
    if ranking:
        ranking.update(response_text=raw, response_sha256=digest)
    state = json.loads(files[1].read_text(encoding='utf8'))
    state['response_sha256'] = digest
    save(files[1], state)
    save(files[2], {'choices': [{'message': {'content': raw}, 'finish_reason': 'stop'}]})
    invalid_inputs = asyncio.run(llm_handoff.build_postprocessing_inputs(
        identity=identity, manifest=manifest, paper=paper, backend=Backend()))
    assert folder_status(list(reversed(saved)))['llm_result'] == 'passed'

    class Admission:
        async def wait(self):
            pass
        async def running(self):
            return True

    @asynccontextmanager
    async def isolated_slot(_admission, _limits):
        if hasattr(_admission, 'admit'):
            assert await _admission.admit()
        yield

    async def all_records(_child):
        return list(reversed(events + saved))

    monkeypatch.setattr(postprocessing, 'read_receipts', all_records)
    monkeypatch.setattr(postprocessing, 'save_receipt', save_receipt)
    monkeypatch.setattr(postprocessing, 'admitted_slot', isolated_slot)
    with pytest.raises(ValueError):
        asyncio.run(postprocessing.process_folder(inputs=invalid_inputs, catalog=catalog,
            paperflow_root=RELEASE / 'core', counter_factory=managed_counter_factory, output_root=tmp_path / 'invalid-output',
            admission=Admission(), limit_names=['synthetic']))
    status = folder_status(list(reversed(saved)))
    assert status['llm_result'] == 'passed' and status['postprocessing_result'] == 'failed'

    for path, content in original_files.items():
        path.write_bytes(content)
    target.clear()
    target.update(target_before)
    if ranking:
        ranking.clear()
        ranking.update(ranking_before)
    state = json.loads(files[1].read_text(encoding='utf8'))
    state.update(status='partial_response', finish_reason='length')
    save(files[1], state)
    saved.clear()
    with pytest.raises(ValueError):
        asyncio.run(llm_handoff.build_postprocessing_inputs(
            identity=identity, manifest=manifest, paper=paper, backend=Backend()))
    assert saved == []
    files[1].write_bytes(original_files[files[1]])

    events[:] = [event for event in events if not (event['kind'] == 'llm_response_saved'
                 and event['data']['kind'] == 'attachment')]
    saved.clear()
    with pytest.raises(ValueError, match='incomplete'):
        asyncio.run(llm_handoff.build_postprocessing_inputs(
            identity=identity, manifest=manifest, paper=paper, backend=Backend()))
    assert saved == []


@pytest.mark.parametrize('retried_kind', ['body', 'attachment'])
def test_handoff_accepts_only_the_bound_successful_retry_journal(tmp_path, monkeypatch, retried_kind, managed_counter_factory):
    import shutil
    from paperflow_prefect.records import identity as fingerprint
    from paperflow_v3.stages import llm_handoff, postprocessing
    from test_body_adapter import FixtureCounter, RELEASE

    identity, manifest, paper, admissions, catalog, attachment_work = synthetic_folder(tmp_path)
    retried = next(row for row in admissions if row['kind'] == retried_kind)
    retry_request_id = 'manual-retry-request'
    original_work = Path(manifest['body_work']) if retried_kind == 'body' else Path(attachment_work)
    new_model = original_work / 'retries' / fingerprint(retried['component_id']) / retry_request_id / 'model'
    shutil.copytree(retried['model_dir'], new_model)
    events = []
    for row in admissions:
        model = new_model if row is retried else Path(row['model_dir'])
        events.append({'kind': 'llm_response_saved', 'data': {
            'component_id': row['component_id'],
            'request_id': retry_request_id if row is retried else row['component_id'],
            'kind': row['kind'], 'request_sha256': row['request_sha256'],
            'response_sha256': row['response_sha256'], 'response_text': (model / 'response.txt').read_bytes().decode('utf8'),
            'response_path': str(model / 'response.txt')}})

    async def read_receipts(_child):
        return list(reversed(events))

    async def save_receipt(**row):
        events.append(row)

    class Backend:
        def attachment_work(self, _paper, _row):
            return Path(attachment_work)

    class Admission:
        async def wait(self):
            pass
        async def running(self):
            return True

    @asynccontextmanager
    async def slot(_admission, _limits):
        if hasattr(_admission, 'admit'):
            assert await _admission.admit()
        yield

    for module in (llm_handoff, postprocessing):
        monkeypatch.setattr(module, 'read_receipts', read_receipts)
        monkeypatch.setattr(module, 'save_receipt', save_receipt)
    monkeypatch.setattr(postprocessing, 'admitted_slot', slot)
    inputs = asyncio.run(llm_handoff.build_postprocessing_inputs(
        identity=identity, manifest=manifest, paper=paper, backend=Backend()))
    if retried_kind == 'body':
        assert Path(inputs['body']['responses'][0]['response']['path']).parent == new_model
    else:
        assert Path(inputs['attachments'][0]['model_refs']['response']['path']).parent == new_model
    result = asyncio.run(postprocessing.process_folder(inputs=inputs, catalog=catalog,
        paperflow_root=RELEASE / 'core', counter_factory=managed_counter_factory, output_root=tmp_path / 'postprocessed',
        admission=Admission(), limit_names=['synthetic']))
    assert result['validation_status'] == 'passed'

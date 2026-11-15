"""Verify whole-paper ranking requests and saved part coverage with synthetic data."""
import asyncio
import hashlib
import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[2]
WORKSPACE = ROOT.parents[1]
sys.path[:0] = [str(ROOT), str(WORKSPACE / 'prefect-paperflow'),
                str(WORKSPACE / 'candidates/postprocess-prefect-20261031/tests')]
from test_synthetic_mainline_integration import synthetic_folder


@pytest.mark.parametrize('layout', ['legacy', 'token-saving/1'])
@pytest.mark.parametrize('final', [False, True])
def test_ranking_request_has_one_consistent_task_and_exact_capacity(layout, final):
    from paperflow_v3.stages.whole_paper_ranking import build_ranking_request
    from preextract_request import RULES

    catalog = {'schema': 'synthetic', 'majors': [
        dict(code='080901', name='计算机科学与技术', discipline_code='08',
             discipline='工学', group_code='0809', group='计算机类')]}
    rows = [{'coverage': ['part-001'], 'content': '第一部分结果'},
            {'coverage': ['part-002'], 'content': '第二部分结果'}]
    config = dict(model='synthetic', max_tokens=512, context_window=50000,
                  request_layout=layout, text_token_counter=len)
    body, capacity = build_ranking_request(rows=rows, source={}, catalog=catalog,
                                           config=config, final=final)
    system = body['messages'][0]['content']
    projected = json.loads(body['messages'][1]['content'][0]['text'])
    assert RULES not in system
    assert '论文完整预提取代理' not in system
    assert '完整既有MD模板' not in system
    assert '080901' in system
    assert projected['material']['reading_context']['ranking_inputs'] == rows
    assert ('最终' in system) if final else ('整理' in system)
    expected_count = len(system) + sum(len(item['text']) for item in body['messages'][1]['content']
                                       if item['type'] == 'text')
    assert capacity['text_count'] == expected_count
    assert capacity['text_token_upper_bound'] == expected_count + 4096
    assert capacity['request_bytes'] == len(json.dumps(body, ensure_ascii=False,
                                               separators=(',', ':')).encode())
    assert not capacity['violations']

    config['max_request_bytes'] = capacity['request_bytes'] - 1
    _, bounded = build_ranking_request(rows=rows, source={}, catalog=catalog,
                                        config=config, final=final)
    assert 'http_bytes' in bounded['violations']


@pytest.mark.parametrize('parts', [2, 3])
def test_all_saved_parts_are_supplied_to_one_final_whole_paper_ranking(tmp_path, monkeypatch, parts):
    from paperflow_v3.stages import whole_paper_ranking as ranking
    identity, manifest, _paper, admissions, catalog, _attachment_work = synthetic_folder(tmp_path, parts)
    events = [{'kind': 'llm_response_saved', 'data': {
        'component_id': row['component_id'], 'request_id': row['component_id'],
        'kind': row['kind'], 'request_sha256': row['request_sha256'],
        'response_sha256': row['response_sha256'],
        'response_path': str(Path(row['model_dir']) / 'response.txt')}} for row in admissions]
    calls = []

    async def read_receipts(_child):
        return list(reversed(events))

    async def save_receipt(**row):
        events.append(row)

    async def run_raw_component(**kwargs):
        body = kwargs['request_body_factory']()
        calls.append(body)
        work = Path(kwargs['work']) / 'model'
        work.mkdir(parents=True)
        raw = json.dumps({'routing_candidates': [
            {'code': '080901', 'name': '计算机科学与技术', 'reason': '全文依据',
             'unit_ids': [], 'source_refs': []}], 'limitations': []}, ensure_ascii=False)
        response = work / 'response.txt'
        response.write_text(raw, encoding='utf8')
        return {'request_sha256': hashlib.sha256(json.dumps(body, ensure_ascii=False,
                    separators=(',', ':')).encode()).hexdigest(),
                'response_sha256': hashlib.sha256(raw.encode()).hexdigest(),
                'response_path': str(response)}

    monkeypatch.setattr(ranking, 'read_receipts', read_receipts)
    monkeypatch.setattr(ranking, 'save_receipt', save_receipt)
    monkeypatch.setattr(ranking, 'run_raw_component', run_raw_component)

    class Settings:
        def path(self, key):
            return WORKSPACE.parent / '程序/resources/论文预提取_空白模板_v1.md'

    class Backend:
        settings = Settings()

    result = asyncio.run(ranking.rank_whole_paper(identity=identity, manifest=manifest,
        backend=Backend(), catalog=catalog,
        connection={'model': 'synthetic', 'max_tokens': 512, 'context_window': 300000,
                    'text_token_counter': len},
        admission=None, limit_names=['synthetic']))
    assert len(calls) == 1
    user = json.loads(calls[0]['messages'][1]['content'][0]['text'])
    rows = user['material']['reading_context']['ranking_inputs']
    body_admissions = [row for row in admissions if row['kind'] == 'body']
    assert len(rows) == parts
    assert [row['content'] for row in rows] == [
        (Path(item['model_dir']) / 'response.txt').read_text(encoding='utf8')
        for item in body_admissions]
    assert [row['unit_id_mapping'] for row in rows] == [
        [{'local_id': 'U001', 'merged_id': f'P{index:03}_U001'}]
        for index in range(1, parts + 1)]
    assert result['coverage'] == [row['coverage'][0] for row in rows]
    assert result['part_responses'] == [
        {'part_id': part_id, 'response_sha256': row['response_sha256']}
        for part_id, row in zip(result['coverage'], body_admissions)]
    assert events[-1]['kind'] == 'llm_whole_ranking_complete'

def test_large_parts_with_different_local_specialties_are_all_sent_to_final_ranking(tmp_path, monkeypatch):
    from paperflow_v3.stages import whole_paper_ranking as ranking
    from test_synthetic_mainline_integration import save

    identity, manifest, _paper, admissions, catalog, _attachment_work = synthetic_folder(tmp_path, 3)
    markers = ['第一段独有研究依据', '第二段独有研究依据', '第三段独有研究依据']
    body_admissions = [row for row in admissions if row['kind'] == 'body']
    for index, row in enumerate(body_admissions):
        model = Path(row['model_dir'])
        candidate = json.loads((model / 'response.txt').read_text(encoding='utf8'))
        candidate['profile']['routing_candidates'][0].update(
            code=f'08090{index + 1}', name=f'局部专业{index + 1}')
        candidate['profile']['test_long_evidence'] = markers[index] + ('测试材料' * 1600)
        raw = json.dumps(candidate, ensure_ascii=False)
        (model / 'response.txt').write_text(raw, encoding='utf8')
        row['response_sha256'] = hashlib.sha256(raw.encode()).hexdigest()
        state = json.loads((model / 'request-state.json').read_text(encoding='utf8'))
        state['response_sha256'] = row['response_sha256']
        save(model / 'request-state.json', state)
        save(model / 'response-events.jsonl',
             {'choices': [{'message': {'content': raw}, 'finish_reason': 'stop'}]})

    events = [{'kind': 'llm_response_saved', 'data': {
        'component_id': row['component_id'], 'request_id': row['component_id'],
        'kind': row['kind'], 'request_sha256': row['request_sha256'],
        'response_sha256': row['response_sha256'],
        'response_path': str(Path(row['model_dir']) / 'response.txt')}} for row in admissions]
    calls = []

    async def read_receipts(_child):
        return list(reversed(events))

    async def save_receipt(**row):
        events.append(row)

    async def run_raw_component(**kwargs):
        body = kwargs['request_body_factory']()
        projected = json.loads(body['messages'][1]['content'][0]['text'])
        inputs = projected['material']['reading_context']['ranking_inputs']
        present = [marker for marker in markers if any(marker in item['content'] for item in inputs)]
        final = '最终判断' in body['messages'][0]['content']
        mappings = [mapping for item in inputs for mapping in item.get('unit_id_mapping', [])]
        calls.append((final, present, mappings))
        raw = json.dumps({'routing_candidates': [], 'limitations': present} if final
                         else {'evidence': present}, ensure_ascii=False)
        work = Path(kwargs['work']) / 'model'
        work.mkdir(parents=True)
        response = work / 'response.txt'
        response.write_text(raw, encoding='utf8')
        return {'request_sha256': hashlib.sha256(json.dumps(body, ensure_ascii=False,
                    separators=(',', ':')).encode()).hexdigest(),
                'response_sha256': hashlib.sha256(raw.encode()).hexdigest(),
                'response_path': str(response)}

    monkeypatch.setattr(ranking, 'read_receipts', read_receipts)
    monkeypatch.setattr(ranking, 'save_receipt', save_receipt)
    monkeypatch.setattr(ranking, 'run_raw_component', run_raw_component)
    source = json.loads(Path(manifest['reading_material']['path']).read_text(encoding='utf8'))['source']
    rows = [{'coverage': [f'part-{index + 1:03}'],
             'content': (Path(row['model_dir']) / 'response.txt').read_text(encoding='utf8')}
            for index, row in enumerate(body_admissions)]
    base = {'model': 'synthetic', 'max_tokens': 512, 'context_window': 300000,
            'text_token_counter': len}
    singles = [ranking.build_ranking_request(rows=[row], source=source, catalog=catalog,
                                            config=base, final=False)[1] for row in rows]
    all_rows = ranking.build_ranking_request(rows=rows, source=source, catalog=catalog,
                                             config=base, final=True)[1]
    budget = max(item['text_token_upper_bound'] + item['output_budget'] for item in singles) + 200
    assert budget < all_rows['text_token_upper_bound'] + all_rows['output_budget']
    base['context_window'] = budget
    result = asyncio.run(ranking.rank_whole_paper(identity=identity, manifest=manifest,
        backend=None, catalog=catalog, connection=base, admission=None, limit_names=['synthetic']))
    assert len(calls) > 1
    assert all(not final for final, _present, _mapping in calls[:-1])
    assert calls[-1] == (True, markers,
        [{'local_id': 'U001', 'merged_id': f'P{index:03}_U001'} for index in range(1, 4)])
    assert result['coverage'] == [row['coverage'][0] for row in rows]


def test_unknown_prior_whole_ranking_blocks_a_new_plan(tmp_path, monkeypatch):
    from paperflow_v3.stages import whole_paper_ranking as ranking
    identity, manifest, _paper, admissions, catalog, _attachment_work = synthetic_folder(tmp_path, 2)
    events = [{'kind': 'llm_response_saved', 'data': {
        'component_id': row['component_id'], 'request_id': row['component_id'],
        'kind': row['kind'], 'request_sha256': row['request_sha256'],
        'response_sha256': row['response_sha256'],
        'response_path': str(Path(row['model_dir']) / 'response.txt')}} for row in admissions]
    previous_model = Path(manifest['body_work']) / 'whole-paper-ranking' / 'old-plan' / 'model'
    events.append({'kind': 'llm_request_unknown', 'data': {'component_id': 'old-component',
        'request_id': 'old-request', 'model_dir': str(previous_model)}})

    async def read_receipts(_child):
        return list(reversed(events))

    async def forbidden(**_kwargs):
        pytest.fail('unknown prior ranking must not cause a new model request')

    monkeypatch.setattr(ranking, 'read_receipts', read_receipts)
    monkeypatch.setattr(ranking, 'run_raw_component', forbidden)
    with pytest.raises(RuntimeError, match='待核对'):
        asyncio.run(ranking.rank_whole_paper(identity=identity, manifest=manifest, backend=None,
            catalog=catalog, connection={'model': 'changed-model', 'max_tokens': 512,
                                         'context_window': 300000},
            admission=None, limit_names=['synthetic']))


def test_completed_whole_ranking_is_reused_even_when_config_changes(tmp_path, monkeypatch):
    from paperflow_v3.stages import whole_paper_ranking as ranking
    from paperflow_prefect.llm_queue.folder_completion import expected_components
    from test_synthetic_mainline_integration import save, file_ref
    from test_handoff_inputs import compact

    identity, manifest, _paper, admissions, catalog, _attachment_work = synthetic_folder(tmp_path, 2)
    events = [{'kind': 'llm_response_saved', 'data': {
        'component_id': row['component_id'], 'request_id': row['component_id'],
        'kind': row['kind'], 'request_sha256': row['request_sha256'],
        'response_sha256': row['response_sha256'],
        'response_path': str(Path(row['model_dir']) / 'response.txt')}} for row in admissions]
    model = Path(manifest['body_work']) / 'whole-paper-ranking' / 'existing' / 'model'
    request = {'model': 'original-model', 'messages': []}
    raw = json.dumps({'routing_candidates': [], 'limitations': []})
    save(model / 'request.json', request)
    (model / 'response.txt').write_text(raw, encoding='utf8')
    response = file_ref(model / 'response.txt')
    save(model / 'response-events.jsonl', {'choices': [{'message': {'content': raw}, 'finish_reason': 'stop'}]})
    save(model / 'request-state.json', {'status': 'response_complete', 'finish_reason': 'stop',
        'request_sha256': compact(request), 'response_sha256': response['sha256']})
    stored = {'component_id': 'existing', 'request_id': 'existing', 'kind': 'body',
        'request_sha256': compact(request), 'response_sha256': response['sha256'],
        'response_path': response['path'], 'response_text': raw}
    parts = [row for row in expected_components(manifest, identity['task_run_id']) if row['kind'] == 'body']
    completed = {'status': 'raw_complete', **{key: stored[key] for key in
        ('component_id', 'request_id', 'request_sha256', 'response_sha256', 'response_path')},
        'coverage': [row['part_id'] for row in parts],
        'part_responses': [{'part_id': part['part_id'], 'response_sha256': next(
            row['response_sha256'] for row in admissions if row['component_id'] == part['component_id'])}
            for part in parts]}
    events.extend([{'kind': 'llm_response_saved', 'data': stored},
                   {'kind': 'llm_whole_ranking_complete', 'data': completed}])

    async def read_receipts(_child):
        return list(reversed(events))

    async def forbidden(**_kwargs):
        pytest.fail('completed global ranking must not be repeated')

    monkeypatch.setattr(ranking, 'read_receipts', read_receipts)
    monkeypatch.setattr(ranking, 'run_raw_component', forbidden)
    result = asyncio.run(ranking.rank_whole_paper(identity=identity, manifest=manifest, backend=None,
        catalog=catalog, connection={'model': 'changed-model', 'max_tokens': 512, 'context_window': 300000},
        admission=None, limit_names=['synthetic']))
    assert result == completed


def test_failed_intermediate_ranking_retries_only_that_request(tmp_path, monkeypatch):
    import io
    from contextlib import asynccontextmanager
    from urllib.error import HTTPError
    from paperflow_v3.stages import whole_paper_ranking as ranking, llm_request
    from test_synthetic_mainline_integration import save

    identity, manifest, _paper, admissions, catalog, _attachment_work = synthetic_folder(tmp_path, 3)
    body_rows = [row for row in admissions if row['kind'] == 'body']
    markers = [f'完整依据{index}' for index in range(1, 4)]
    for index, row in enumerate(body_rows):
        model = Path(row['model_dir'])
        candidate = json.loads((model / 'response.txt').read_text(encoding='utf8'))
        candidate['profile']['synthetic_evidence'] = markers[index] + ('测试依据' * 1600)
        raw = json.dumps(candidate, ensure_ascii=False)
        (model / 'response.txt').write_text(raw, encoding='utf8')
        row['response_sha256'] = hashlib.sha256(raw.encode()).hexdigest()
        state = json.loads((model / 'request-state.json').read_text(encoding='utf8'))
        state['response_sha256'] = row['response_sha256']
        save(model / 'request-state.json', state)
        save(model / 'response-events.jsonl',
             {'choices': [{'message': {'content': raw}, 'finish_reason': 'stop'}]})
    events = [{'kind': 'llm_response_saved', 'data': {
        'component_id': row['component_id'], 'request_id': row['component_id'], 'kind': row['kind'],
        'request_sha256': row['request_sha256'], 'response_sha256': row['response_sha256'],
        'response_text': (Path(row['model_dir']) / 'response.txt').read_bytes().decode('utf8'),
        'response_path': str(Path(row['model_dir']) / 'response.txt')}} for row in admissions]

    async def read_receipts(_child):
        return list(reversed(events))

    async def save_receipt(**row):
        events.append(row)

    @asynccontextmanager
    async def slot(_admission, _limits):
        if hasattr(_admission, 'admit'):
            assert await _admission.admit()
        yield

    class Admission:
        def running_sync(self):
            return True

        async def wait(self):
            pass

    class Response(io.BytesIO):
        headers = {'Content-Type': 'application/json'}

    calls = []
    class SyntheticOpener:
        def open(self, request, timeout):
            body = json.loads(request.data)
            system = body['messages'][0]['content']
            projected = json.loads(body['messages'][1]['content'][0]['text'])
            inputs = projected['material']['reading_context']['ranking_inputs']
            covered = list(dict.fromkeys(part for row in inputs for part in row['coverage']))
            is_final = '最终判断' in system
            calls.append(('final' if is_final else 'intermediate', covered))
            if not is_final and len(calls) == 2:
                raise HTTPError(request.full_url, 401, 'synthetic', {}, io.BytesIO(b'complete failure'))
            present = [marker for marker in markers if any(marker in row['content'] for row in inputs)]
            raw = json.dumps({'routing_candidates': [], 'limitations': present} if is_final
                             else {'evidence': present}, ensure_ascii=False)
            return Response(json.dumps({'choices': [{'message': {'content': raw},
                'finish_reason': 'stop'}]}, ensure_ascii=False).encode('utf8'))

    for module in (ranking, llm_request):
        monkeypatch.setattr(module, 'read_receipts', read_receipts)
        if hasattr(module,'read_receipts_sync'):
            monkeypatch.setattr(module,'read_receipts_sync',lambda _child:list(reversed(events)))
        monkeypatch.setattr(module, 'save_receipt', save_receipt)
    monkeypatch.setattr(llm_request, 'admitted_slot', slot)
    source = json.loads(Path(manifest['reading_material']['path']).read_text(encoding='utf8'))['source']
    source['reading_scope'] = {}
    inputs = [{'coverage': [f'part-{index + 1:03}'],
               'unit_id_mapping': [{'local_id': 'U001', 'merged_id': f'P{index + 1:03}_U001'}],
               'content': (Path(row['model_dir']) / 'response.txt').read_text(encoding='utf8')}
              for index, row in enumerate(body_rows)]
    connection = {'model': 'synthetic', 'base_url': 'http://synthetic.invalid', 'api_key': 'synthetic',
                  'max_tokens': 512, 'context_window': 300000, 'text_token_counter': len}
    capacities = [ranking.build_ranking_request(rows=[row], source=source, catalog=catalog,
                                                config=connection, final=False)[1] for row in inputs]
    connection['context_window'] = max(cap['text_token_upper_bound'] + cap['output_budget']
                                       for cap in capacities) + 200
    options = dict(identity=identity, manifest=manifest, backend=None, catalog=catalog, connection=connection,
                   admission=Admission(), limit_names=['synthetic'], opener=SyntheticOpener())
    with pytest.raises(HTTPError):
        asyncio.run(ranking.rank_whole_paper(**options))
    first_calls = list(calls)
    assert len(first_calls) == 2 and all(kind == 'intermediate' for kind, _coverage in first_calls)
    failed = next(row['data'] for row in reversed(events) if row['kind'] == 'llm_request_failed')
    original_model = Path(failed['model_dir'])
    before = {path: path.read_bytes() for path in original_model.iterdir() if path.is_file()}
    events.append({'kind': 'manual_retry', 'data': {'state': 'scheduled', 'retry_id': 'manual-ranking'}})
    result = asyncio.run(ranking.rank_whole_paper(**options, retry_id='manual-ranking'))
    assert calls[2] == first_calls[1]
    assert calls[-1][0] == 'final'
    assert calls.count(first_calls[0]) == 1
    assert all(path.read_bytes() == value for path, value in before.items())
    assert result['coverage'] == [row['coverage'][0] for row in inputs]

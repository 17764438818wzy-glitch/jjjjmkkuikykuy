"""Synthetic saved replies; real legacy validation/rendering/sealing, no network."""
import copy
import base64
import hashlib
import importlib
import json
import os
from pathlib import Path
import sys

import pytest

CANDIDATE = Path(__file__).resolve().parents[1]
WORKSPACE = CANDIDATE.parents[1]
PROGRAM = WORKSPACE.parent / '程序'
RELEASE = PROGRAM / 'updates/unified-prefect-20261029-material-r02'
os.environ['PAPERFLOW_SELECTED_CORE'] = str(RELEASE / 'core')
sys.path[:0] = [str(CANDIDATE), str(WORKSPACE / 'prefect-paperflow/paperflow_prefect/clean_overlay'),
               str(RELEASE / 'desktop'), str(PROGRAM / 'desktop'), str(RELEASE / 'core'),
               str(WORKSPACE / 'prefect-paperflow/tests')]


def digest(value, sort=True):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=sort,
                                    separators=(',', ':')).encode()).hexdigest()


def reference(path):
    return {'path': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding='utf8')
    return reference(path)


class FixtureCounter:
    """Only external tokenizer is replaced. These are NOT exact model token counts."""
    identity = {'fixture': 'character-count-only'}
    model_budget = 2000
    cl_limit = 2038

    def count_many(self, texts):
        return [{'model_tokens': len(t), 'cl100k_tokens': len(t)} for t in texts]


@pytest.fixture(autouse=True)
def forbid_external_calls(monkeypatch):
    import socket
    import urllib.request
    def forbidden(*args, **kwargs):
        raise AssertionError('Synthetic test must never connect to external services')
    monkeypatch.setattr(socket.socket, 'connect', forbidden)
    monkeypatch.setattr(socket.socket, 'connect_ex', forbidden)
    monkeypatch.setattr(urllib.request.OpenerDirector, 'open', forbidden)


def fixture(root, parts=1):
    from preextract_contract import load_template, _blank
    from fixture_profile import _candidate
    _, skeleton = load_template()
    identity = dict(task_id='existing-order', group_id='existing-group', paper_id='existing-paper',
                    receipt_id='a' * 64, attempt_id='existing-attempt',
                    parent_run_id='existing-parent', task_run_id='existing-folder-flow-run')
    catalog = {'schema': 'specialty-routing/1.0', 'majors': [dict(code='080901', name='计算机科学与技术',
        discipline_code='08', discipline='工学', group_code='0809', group='计算机类',
        paper_dataset_id='synthetic-paper', image_dataset_id='synthetic-images')]}
    source_file = root / 'upstream/body.txt'
    source_file.parent.mkdir(parents=True)
    source_file.write_text('合成输入；绝非真实论文。', encoding='utf8')
    source = _blank(skeleton['source'])
    source.update(source_id='synthetic-source', file=str(source_file), source_sha256=reference(source_file)['sha256'])
    blocks = [dict(id=f'B{i:03d}', section_id=f'S{i:03d}', type='paragraph', order=i,
        text=f'第{i}项材料为三个作业的到达时刻和持续时间。先按两种规则排序，再记录等待时间，比较平均值。') for i in range(1, parts + 1)]
    sections = [dict(id=f'S{i:03d}', title=f'{i} 排程研究', level=1, order=i, parent_id=None,
                     block_ids=[f'B{i:03d}']) for i in range(1, parts + 1)]
    bundle = dict(version='preextract-material/1.0', file_ref='main', source=source,
                  blocks=blocks, sections=sections, assets=[], metadata=[], attachments=[])
    plan_parts, responses = [], []
    for i in range(1, parts + 1):
        subset = copy.deepcopy(bundle)
        subset['blocks'] = [copy.deepcopy(blocks[i - 1])]
        subset['sections'] = [copy.deepcopy(sections[i - 1])]
        spans = [dict(block_id=blocks[i - 1]['id'], start=0, end=len(blocks[i - 1]['text']))]
        request = {'model': 'synthetic-never-sent', 'messages': [{'role': 'system', 'content': 'fixture'},
            {'role': 'user', 'content': [{'type': 'text', 'text': json.dumps({'material': subset}, ensure_ascii=False)}]}]}
        part_id = f'part-{i:03d}'
        plan_parts.append(dict(id=part_id, body=request, bundle=subset, ranges=spans, asset_ids=[], capacity={}))
        response = _candidate({'template': PROGRAM / 'resources/论文预提取_空白模板_v1.md'}, subset)
        model = root / 'upstream' / part_id
        raw = json.dumps(response, ensure_ascii=False)
        model.mkdir()
        (model / 'response.txt').write_text(raw, encoding='utf8')
        req_ref = save(model / 'request.json', request)
        events_ref = save(model / 'response-events.jsonl', {'choices': [{'message': {'content': raw}, 'finish_reason': 'stop'}]})
        state_ref = save(model / 'request-state.json', dict(status='response_complete', finish_reason='stop',
            request_sha256=digest(request, sort=False), response_sha256=reference(model / 'response.txt')['sha256']))
        responses.append(dict(identity=copy.deepcopy(identity), part_id=part_id, request=req_ref,
            state=state_ref, events=events_ref, response=reference(model / 'response.txt'),
            images_work=str(model), request_images=None))
    plan = dict(schema='preextract-input-plan/1', mode='single' if parts == 1 else 'partitioned', parts=plan_parts,
        coverage=dict(complete=True, ranges=[r for p in plan_parts for r in p['ranges']], asset_ids=[],
                      block_count=parts, character_count=sum(len(b['text']) for b in blocks)))
    reading_ref = save(root / 'upstream/reading.json', bundle)
    plan_identity = dict(schema='paperflow-body-input-plan/1', source_sha256=source['source_sha256'],
        reading_sha256=reading_ref['sha256'], mode=plan['mode'], part_count=parts,
        parts=[dict(id=p['id'], request_sha256=digest(p['body'], False), material_sha256=digest(p['bundle']),
                    scope_sha256=digest(dict(ranges=p['ranges'], asset_ids=p['asset_ids']))) for p in plan_parts],
        coverage_sha256=digest(plan['coverage']), plan_sha256=digest(plan))
    evidence = dict(schema='body-postprocess-input/1', identity=identity,
        receipt=save(root / 'upstream/receipt.json', dict(schema='paperflow-receipt/1', identity=identity,
            group_id=identity['group_id'], paper_id=identity['paper_id'], group_root=str(root / 'upstream'))), source=reference(source_file),
        material=save(root / 'upstream/material.json', bundle), reading_material=reading_ref,
        plan=save(root / 'upstream/frozen-plan.json', plan), plan_identity=plan_identity, responses=responses)
    return evidence, catalog


def run(evidence, output, catalog):
    try:
        adapter = importlib.import_module('body_adapter')
    except ModuleNotFoundError:
        pytest.fail('Missing body_adapter: saved responses cannot yet produce a sealed body package')
    return adapter.process_body(evidence, output, expected_identity=evidence['identity'],
        catalog=catalog, paperflow_root=RELEASE / 'core', counter=FixtureCounter())


def test_complete_saved_single_reply_creates_real_package_and_reuses_identity(tmp_path):
    evidence, catalog = fixture(tmp_path)
    before = {p: p.read_bytes() for p in (tmp_path / 'upstream').rglob('*') if p.is_file()}
    result = run(evidence, tmp_path / 'result', catalog)
    assert result['status'] == 'body_package_verified'
    assert result['identity'] == evidence['identity']
    assert result['model_requests_added'] == 0
    assert result['whole_paper_semantic_synthesis'] is False
    assert result['validation']['body_exact_reassembly'] is True
    from paperflow.package import validate_package
    packet = validate_package(result['package_path'], result['task'], catalog, strict_seal=True)
    assert packet['manifest_sha256'] == result['manifest_sha256']
    assert len(packet['profile']['learning_units']) == 1
    assert run(evidence, tmp_path / 'result', catalog) == result
    assert all(p.read_bytes() == raw for p, raw in before.items())


def test_partitioned_saved_replies_preserve_units_and_explicit_program_merge(tmp_path):
    evidence, catalog = fixture(tmp_path, parts=2)
    result = run(evidence, tmp_path / 'result', catalog)
    assert result['assembly']['mode'] == 'program_merge'
    assert result['whole_paper_semantic_synthesis'] is False
    assert result['validation']['learning_units'] == 2
    assert result['limitations']


@pytest.mark.parametrize('change', ['raw', 'source', 'receipt', 'request', 'events'])
def test_changed_evidence_rejects_and_preserves_raw(tmp_path, change):
    evidence, catalog = fixture(tmp_path)
    ref = (evidence['responses'][0]['response' if change == 'raw' else change]
           if change in {'raw', 'request', 'events'} else evidence[change])
    path = Path(ref['path']); path.write_bytes(path.read_bytes() + b' ')
    before = Path(evidence['responses'][0]['response']['path']).read_bytes()
    with pytest.raises(ValueError):
        run(evidence, tmp_path / 'result', catalog)
    assert Path(evidence['responses'][0]['response']['path']).read_bytes() == before
    assert not (tmp_path / 'result/body-result.json').exists()


def test_missing_part_never_packages_partial_order(tmp_path):
    evidence, catalog = fixture(tmp_path, parts=2)
    evidence['responses'].pop()
    with pytest.raises(ValueError):
        run(evidence, tmp_path / 'result', catalog)


def test_unknown_state_and_wrong_folder_are_rejected(tmp_path):
    evidence, catalog = fixture(tmp_path)
    row = evidence['responses'][0]
    state = json.loads(Path(row['state']['path']).read_text(encoding='utf8'))
    state['status'] = 'request_failed_or_uncertain'
    row['state'] = save(Path(row['state']['path']), state)
    with pytest.raises(ValueError):
        run(evidence, tmp_path / 'unknown', catalog)
    evidence, catalog = fixture(tmp_path / 'second')
    evidence['responses'][0]['identity']['task_run_id'] = 'another-folder-flow-run'
    with pytest.raises(ValueError):
        run(evidence, tmp_path / 'wrong-folder', catalog)


def test_ready_without_registered_result_never_mints_second_identity(tmp_path):
    evidence, catalog = fixture(tmp_path)
    output = tmp_path / 'result'; output.mkdir()
    (output / 'READY.json').write_text('{}')
    with pytest.raises(ValueError):
        run(evidence, output, catalog)
    assert sorted(p.name for p in output.iterdir()) == ['READY.json']


def change_response(row, mutate):
    path = Path(row['response']['path'])
    candidate = json.loads(path.read_text(encoding='utf8'))
    mutate(candidate)
    raw = json.dumps(candidate, ensure_ascii=False)
    path.write_text(raw, encoding='utf8')
    row['response'] = reference(path)
    row['events'] = save(Path(row['events']['path']), {'choices': [{'message': {'content': raw}, 'finish_reason': 'stop'}]})
    state = json.loads(Path(row['state']['path']).read_text(encoding='utf8'))
    state['response_sha256'] = row['response']['sha256']
    row['state'] = save(Path(row['state']['path']), state)


def test_invalid_locator_cannot_be_softened_into_ready_by_learning_policy(tmp_path):
    evidence, catalog = fixture(tmp_path)
    change_response(evidence['responses'][0], lambda c: c['profile']['learning_units'][0]['source_refs'][0].update(block_id='INVENTED'))
    with pytest.raises(ValueError, match='unknown_source_ref'):
        run(evidence, tmp_path / 'result', catalog)
    assert (tmp_path / 'result/diagnostic.json').is_file()
    assert not (tmp_path / 'result/publication').exists()


def test_invalid_profile_keeps_full_private_validation_diagnostic(tmp_path, monkeypatch):
    evidence, catalog = fixture(tmp_path)
    before = Path(evidence['responses'][0]['response']['path']).read_bytes()
    import preextract_contract
    report = {'valid': False, 'errors': [{'code': 'synthetic_invalid_profile', 'message': 'synthetic diagnostic'}]}
    monkeypatch.setattr(preextract_contract, 'validate_candidate', lambda *args, **kwargs: copy.deepcopy(report))
    from body_adapter import BodyValidationError
    with pytest.raises(BodyValidationError, match='candidate_validation_failed') as failure:
        run(evidence, tmp_path / 'result', catalog)
    path = tmp_path / 'result/private/part-001-validation.json'
    assert json.loads(path.read_text(encoding='utf8')) == report
    assert failure.value.diagnostic == reference(path)
    assert Path(evidence['responses'][0]['response']['path']).read_bytes() == before
    assert not (tmp_path / 'result/publication').exists()


def test_cross_part_title_conflict_does_not_invent_global_answer(tmp_path):
    evidence, catalog = fixture(tmp_path, parts=2)
    change_response(evidence['responses'][1], lambda c: c['profile']['article'].update(title='互相矛盾的另一篇论文'))
    with pytest.raises(ValueError, match='cross_part_conflict'):
        run(evidence, tmp_path / 'result', catalog)
    assert not (tmp_path / 'result/publication').exists()


@pytest.mark.parametrize('unread_ref', ['B001', 'S001', 'main'])
def test_unread_body_remains_a_public_learning_limit_without_inventing_reading(tmp_path, unread_ref):
    evidence, catalog = fixture(tmp_path)
    change_response(evidence['responses'][0], lambda c: c['source']['reading_scope'].update(read_refs=[], unread_refs=[unread_ref]))
    raw_path = Path(evidence['responses'][0]['response']['path'])
    before = raw_path.read_bytes()
    result = run(evidence, tmp_path / 'result', catalog)
    report = json.loads((tmp_path / 'result/private/part-001-validation.json').read_text(encoding='utf8'))
    scope = report['candidate']['source']['reading_scope']
    assert unread_ref in scope['unread_refs'] and unread_ref not in scope['read_refs']
    profile = json.loads((Path(result['package_path']) / 'profile.json').read_text(encoding='utf8'))
    assert any('未读' in str(gap.get('use_limit', '')) for gap in profile['gaps'])
    assert result['validation']['body_exact_reassembly'] is True
    assert raw_path.read_bytes() == before


def test_unread_does_not_bypass_the_original_usable_learning_unit_check(tmp_path):
    evidence, catalog = fixture(tmp_path)
    def incomplete(candidate):
        candidate['source']['reading_scope'].update(read_refs=[], unread_refs=['B001'])
        candidate['profile']['learning_units'][0]['question'] = ''
    change_response(evidence['responses'][0], incomplete)
    raw_path = Path(evidence['responses'][0]['response']['path'])
    before = raw_path.read_bytes()
    with pytest.raises(ValueError, match='candidate_validation_failed'):
        run(evidence, tmp_path / 'result', catalog)
    report = json.loads((tmp_path / 'result/private/part-001-validation.json').read_text(encoding='utf8'))
    assert report['valid'] is False
    assert any(issue['code'] == 'learning_unusable' for issue in report['errors'])
    assert raw_path.read_bytes() == before
    assert not (tmp_path / 'result/publication').exists()


@pytest.mark.parametrize('parts', [1, 2])
def test_unlocated_unread_declaration_is_a_public_limit_without_a_guessed_location(tmp_path, parts):
    evidence, catalog = fixture(tmp_path, parts=parts)
    change_response(evidence['responses'][0], lambda c: c['source']['reading_scope'].update(
        read_refs=['B001'], unread_refs=['main:unlocated-example']))
    raw_path = Path(evidence['responses'][0]['response']['path'])
    before = raw_path.read_bytes()
    result = run(evidence, tmp_path / 'result', catalog)
    profile = json.loads((Path(result['package_path']) / 'profile.json').read_text(encoding='utf8'))
    limits = profile['migration_boundary']['global_limits']
    assert any('未读' in str(limit) and ('定位' in str(limit) or '对应' in str(limit)) for limit in limits)
    assert raw_path.read_bytes() == before
    assert result['validation']['body_exact_reassembly'] is True


def test_changed_auxiliary_text_invalidates_material_even_when_json_is_unchanged(tmp_path):
    evidence, catalog = fixture(tmp_path)
    # Make an independently hash-bound text file part of all saved material.
    text_path = tmp_path / 'upstream/extracted.txt'; text_path.write_text('frozen extracted text', encoding='utf8')
    plan = json.loads(Path(evidence['plan']['path']).read_text(encoding='utf8'))
    for key in ('material', 'reading_material'):
        material = json.loads(Path(evidence[key]['path']).read_text(encoding='utf8'))
        material['source'].update(text_file=str(text_path), text_sha256=reference(text_path)['sha256'])
        evidence[key] = save(Path(evidence[key]['path']), material)
    source = material['source']
    for part, row in zip(plan['parts'], evidence['responses']):
        part['bundle']['source'] = copy.deepcopy(source)
        sent = json.loads(part['body']['messages'][1]['content'][0]['text'])
        sent['material']['source'] = copy.deepcopy(source)
        part['body']['messages'][1]['content'][0]['text'] = json.dumps(sent, ensure_ascii=False)
        row['request'] = save(Path(row['request']['path']), part['body'])
        change_response(row, lambda c: c.update(source=copy.deepcopy(source)))
        state = json.loads(Path(row['state']['path']).read_text(encoding='utf8'))
        state['request_sha256'] = digest(part['body'], False)
        row['state'] = save(Path(row['state']['path']), state)
    evidence['plan'] = save(Path(evidence['plan']['path']), plan)
    evidence['plan_identity'].update(reading_sha256=evidence['reading_material']['sha256'], plan_sha256=digest(plan))
    evidence['plan_identity']['parts'][0].update(request_sha256=digest(plan['parts'][0]['body'], False),
                                               material_sha256=digest(plan['parts'][0]['bundle']))
    text_path.write_text('changed behind unchanged material json', encoding='utf8')
    with pytest.raises(ValueError, match='input_changed'):
        run(evidence, tmp_path / 'result', catalog)


def test_cached_package_corruption_is_rejected(tmp_path):
    evidence, catalog = fixture(tmp_path)
    result = run(evidence, tmp_path / 'result', catalog)
    public = Path(result['package_path']) / 'profile.json'
    public.write_bytes(public.read_bytes() + b' ')
    with pytest.raises(ValueError):
        run(evidence, tmp_path / 'result', catalog)


def test_post_seal_crash_retains_one_package_and_cannot_generate_new_identity(tmp_path, monkeypatch):
    evidence, catalog = fixture(tmp_path)
    adapter = importlib.import_module('body_adapter')
    real_save = adapter._save
    def crash_before_result(path, value):
        if Path(path).name == 'body-result.json':
            raise OSError('synthetic crash after seal before registration')
        return real_save(path, value)
    monkeypatch.setattr(adapter, '_save', crash_before_result)
    with pytest.raises(OSError, match='synthetic crash'):
        run(evidence, tmp_path / 'result', catalog)
    package = tmp_path / 'result/publication'
    assert (package / 'READY.json').is_file()
    sealed = {p: p.read_bytes() for p in package.rglob('*') if p.is_file()}
    with pytest.raises(ValueError, match='unregistered_output_preserved'):
        run(evidence, tmp_path / 'result', catalog)
    assert all(p.read_bytes() == raw for p, raw in sealed.items())


def image_fixture(root, wrong_origin=False, root_rendition=False):
    from PIL import Image
    evidence, catalog = fixture(root)
    original = root / 'upstream/original.png'
    Image.new('RGB', (20, 20), 'blue').save(original)
    adapted = root / ('upstream/body-work/private/request-images/adapted.png' if root_rendition
                      else 'upstream/part-001/private/request-images/adapted.png')
    adapted.parent.mkdir(parents=True)
    Image.new('RGB', (10, 10), 'blue').save(adapted)
    source_asset = dict(id='A001', image_path=str(original), sha256=reference(original)['sha256'],
                        width=20, height=20, block_ids=['B001'], section_id='S001')
    sent_asset = dict(source_asset, image_path=str(adapted), sha256=reference(adapted)['sha256'], width=10, height=10,
                     image_adaptation=dict(source_sha256=source_asset['sha256'], source_width=20, source_height=20))
    if wrong_origin:
        sent_asset['image_adaptation']['source_sha256'] = '0' * 64
    for key in ('material', 'reading_material'):
        value = json.loads(Path(evidence[key]['path']).read_text(encoding='utf8'))
        value['assets'] = [source_asset]
        evidence[key] = save(Path(evidence[key]['path']), value)
    plan = json.loads(Path(evidence['plan']['path']).read_text(encoding='utf8'))
    part = plan['parts'][0]; part['bundle']['assets'] = [sent_asset]; part['asset_ids'] = ['A001']
    part['body']['messages'][1]['content'] = [
        {'type': 'text', 'text': json.dumps({'material': part['bundle']}, ensure_ascii=False)},
        {'type': 'image_url', 'image_url': {'url': 'data:image/png;base64,' + base64.b64encode(adapted.read_bytes()).decode()}}]
    plan['coverage']['asset_ids'] = ['A001']
    evidence['plan'] = save(Path(evidence['plan']['path']), plan)
    receipt = evidence['plan_identity']
    receipt.update(reading_sha256=evidence['reading_material']['sha256'], plan_sha256=digest(plan),
                   coverage_sha256=digest(plan['coverage']))
    receipt['parts'][0].update(request_sha256=digest(part['body'], False), material_sha256=digest(part['bundle']),
                              scope_sha256=digest(dict(ranges=part['ranges'], asset_ids=part['asset_ids'])))
    row = evidence['responses'][0]
    row['request'] = save(Path(row['request']['path']), part['body'])
    state = json.loads(Path(row['state']['path']).read_text(encoding='utf8'))
    state['request_sha256'] = digest(part['body'], False)
    row['state'] = save(Path(row['state']['path']), state)
    return evidence, original, adapted


def test_original_image_change_cannot_hide_behind_unchanged_sent_rendition(tmp_path):
    evidence, original, adapted = image_fixture(tmp_path)
    adapter = importlib.import_module('body_adapter')
    plan, bundle, parts, aliases = adapter._inputs(evidence, evidence['identity'])
    assert bundle['assets'][0]['sha256'] == reference(adapted)['sha256']
    original.write_bytes(b'changed source image')
    with pytest.raises(ValueError):
        adapter._inputs(evidence, evidence['identity'])


def test_changed_sent_image_is_rejected(tmp_path):
    evidence, original, adapted = image_fixture(tmp_path)
    adapted.write_bytes(b'changed sent image')
    adapter = importlib.import_module('body_adapter')
    with pytest.raises(ValueError):
        adapter._inputs(evidence, evidence['identity'])


def test_adapted_image_must_derive_from_the_bound_original(tmp_path):
    evidence, original, adapted = image_fixture(tmp_path, wrong_origin=True)
    adapter = importlib.import_module('body_adapter')
    with pytest.raises(ValueError, match='image_origin_mismatch'):
        adapter._inputs(evidence, evidence['identity'])


def save_upstream_plan_layout(root, evidence):
    plan = json.loads(Path(evidence['plan']['path']).read_text(encoding='utf8'))
    work = root / 'upstream/body-work'
    if plan['mode'] == 'single':
        save(work / 'private/llm-frozen-single-plan.json', dict(schema='paperflow-raw-frozen-single-plan/1',
            plan_sha256=evidence['plan_identity']['plan_sha256'], plan=plan))
        return work
    reading = json.loads(Path(evidence['reading_material']['path']).read_text(encoding='utf8'))
    source = json.loads(Path(evidence['material']['path']).read_text(encoding='utf8'))
    material_ref = save(work / 'model/partition-material.json', reading)
    rows = []
    for part in plan['parts']:
        material = save(work / 'model' / part['id'] / 'material.json', part['bundle'])
        save(work / 'model' / part['id'] / 'planned-request.json', part['body'])
        rows.append({k: copy.deepcopy(part[k]) for k in ('id', 'ranges', 'asset_ids', 'capacity')})
        rows[-1].update(request_sha256=digest(part['body'], False), material_sha256=material['sha256'])
    frozen_ref = save(work / 'model/partition-plan.json', dict(schema='preextract-partition-run/1', parts=rows,
        coverage=plan['coverage'], source_material_sha256=digest(source), material_sha256=material_ref['sha256']))
    save(work / 'model/partition-state.json', dict(schema='preextract-partition-run/1', status='parts_pending',
        plan_sha256=frozen_ref['sha256'], recoveries=[],
        requests=[dict(id=p['id'], request_sha256=p['request_sha256']) for p in rows]))
    save(work / 'private/llm-frozen-plan-metadata.json', dict(schema='paperflow-raw-frozen-plan-metadata/1',
        plan_sha256=evidence['plan_identity']['plan_sha256'], top={k: v for k, v in plan.items() if k != 'parts'},
        part_extras=[dict(id=p['id'], extra={k: v for k, v in p.items() if k not in
                     {'id', 'ranges', 'asset_ids', 'capacity', 'body', 'bundle'}}) for p in plan['parts']]))
    return work


@pytest.mark.parametrize('parts', [1, 2])
def test_actual_upstream_frozen_layout_is_read_only_and_packages(parts, tmp_path):
    evidence, catalog = fixture(tmp_path, parts=parts)
    work = save_upstream_plan_layout(tmp_path, evidence)
    adapter = importlib.import_module('body_adapter')
    assert hasattr(adapter, 'frozen_plan_evidence'), 'Missing read-only reader for actual upstream saved plans'
    before = {p: p.read_bytes() for p in work.rglob('*') if p.is_file()}
    evidence['plan'] = adapter.frozen_plan_evidence(work, evidence['plan_identity'])
    result = run(evidence, tmp_path / 'result', catalog)
    assert result['validation']['learning_units'] == parts
    assert all(p.read_bytes() == raw for p, raw in before.items())
    next(iter(before)).write_bytes(b'changed frozen plan component')
    with pytest.raises(ValueError):
        run(evidence, tmp_path / 'result', catalog)


def test_saved_plan_rendition_can_live_in_original_body_work(tmp_path):
    evidence, original, adapted = image_fixture(tmp_path, root_rendition=True)
    work = save_upstream_plan_layout(tmp_path, evidence)
    adapter = importlib.import_module('body_adapter')
    evidence['plan'] = adapter.frozen_plan_evidence(work, evidence['plan_identity'])
    plan, bundle, verified, aliases = adapter._inputs(evidence, evidence['identity'])
    assert bundle['assets'][0]['image_path'] == str(adapted)


@pytest.mark.parametrize('location', ['source', 'group', 'delivery', 'model', 'body_work'])
def test_outputs_cannot_enter_protected_upstream_trees(tmp_path, location):
    evidence, catalog = fixture(tmp_path)
    work = save_upstream_plan_layout(tmp_path, evidence)
    adapter = importlib.import_module('body_adapter')
    evidence['plan'] = adapter.frozen_plan_evidence(work, evidence['plan_identity'])
    group = tmp_path / 'original-group'; group.mkdir()
    delivery = tmp_path / 'delivery-version'; delivery.mkdir()
    delivery_ref = save(delivery / 'delivery.json', {'synthetic': True})
    receipt = dict(schema='paperflow-receipt/1', group_root=str(group), delivery_ref=delivery_ref,
                   group_id=evidence['identity']['group_id'], paper_id=evidence['identity']['paper_id'])
    evidence['receipt'] = save(Path(evidence['receipt']['path']), receipt)
    base = {'source': Path(evidence['source']['path']).parent, 'group': group, 'delivery': delivery,
            'model': Path(evidence['responses'][0]['request']['path']).parent, 'body_work': work}[location]
    output = base / 'must-not-be-created'
    with pytest.raises(ValueError, match='protected_output_overlap'):
        run(evidence, output, catalog)
    assert not output.exists()
    # A sibling outside the exact group/delivery/work roots is legitimate.
    assert run(evidence, tmp_path / 'allowed-result', catalog)['status'] == 'body_package_verified'


def test_output_through_directory_symlink_is_rejected_without_writes(tmp_path):
    evidence, catalog = fixture(tmp_path)
    target = tmp_path / 'safe-external'; target.mkdir()
    alias = tmp_path / 'linked-output'
    try:
        alias.symlink_to(target, target_is_directory=True)
    except OSError:
        import subprocess
        subprocess.run(['cmd', '/c', 'mklink', '/J', str(alias), str(target)], check=True, capture_output=True)
    with pytest.raises(ValueError, match='output_reparse_point'):
        run(evidence, alias / 'package', catalog)
    assert not (target / 'package').exists()


@pytest.mark.parametrize('change', ['schema', 'status', 'empty_artifacts', 'wrong_path', 'candidate', 'bundle',
                                  'validation', 'assembly', 'part_1_validation', 'claims'])
def test_completed_reuse_rederives_artifacts_instead_of_trusting_rehashed_result(tmp_path, change):
    evidence, catalog = fixture(tmp_path)
    output = tmp_path / 'result'
    result = run(evidence, output, catalog)
    if change in {'schema', 'status'}:
        result[change] = 'forged'
    elif change == 'empty_artifacts':
        result['private_artifacts'] = {}
    elif change == 'wrong_path':
        original = Path(result['private_artifacts']['candidate']['path'])
        alternate = tmp_path / 'external-candidate.json'; alternate.write_bytes(original.read_bytes())
        result['private_artifacts']['candidate'] = reference(alternate)
    elif change == 'claims':
        result['whole_paper_semantic_synthesis'] = True
        result['model_requests_added'] = 1
    else:
        ref = result['private_artifacts'][change]
        value = json.loads(Path(ref['path']).read_text(encoding='utf8'))
        if change == 'candidate':
            value['profile']['article']['title'] = '伪造但重新计算哈希'
        elif change == 'bundle':
            value['blocks'][0]['text'] = '替换原材料'
        elif change == 'assembly':
            value['mode'] = 'semantic_synthesis'
        else:
            value['valid'] = False
        result['private_artifacts'][change] = save(Path(ref['path']), value)
    save(output / 'body-result.json', result)
    with pytest.raises(ValueError):
        run(evidence, output, catalog)


@pytest.mark.parametrize('change', ['status', 'body_exact_reassembly', 'learning_units', 'manifest_sha256'])
def test_cached_publication_report_claims_are_checked_against_actual_package(tmp_path, change):
    evidence, catalog = fixture(tmp_path)
    output = tmp_path / 'result'; result = run(evidence, output, catalog)
    result['validation'][change] = {'status': 'failed', 'body_exact_reassembly': False,
        'learning_units': 500, 'manifest_sha256': '0' * 64}[change]
    save(output / 'body-result.json', result)
    with pytest.raises(ValueError):
        run(evidence, output, catalog)


def test_output_cannot_be_ancestor_of_any_upstream_data(tmp_path):
    evidence, catalog = fixture(tmp_path)
    before = {p: p.read_bytes() for p in tmp_path.rglob('*') if p.is_file()}
    with pytest.raises(ValueError, match='protected_output_overlap'):
        run(evidence, tmp_path, catalog)
    assert all(p.read_bytes() == raw for p, raw in before.items())


def test_cached_private_artifact_symlink_is_rejected(tmp_path):
    evidence, catalog = fixture(tmp_path)
    output = tmp_path / 'result'; result = run(evidence, output, catalog)
    private = output / 'private'; moved = tmp_path / 'moved-private'; private.rename(moved)
    try:
        private.symlink_to(moved, target_is_directory=True)
    except OSError:
        import subprocess
        subprocess.run(['cmd', '/c', 'mklink', '/J', str(private), str(moved)], check=True, capture_output=True)
    with pytest.raises(ValueError, match='output_reparse_point'):
        run(evidence, output, catalog)


def test_missing_original_group_provenance_is_rejected(tmp_path):
    evidence, catalog = fixture(tmp_path)
    evidence['receipt'] = save(Path(evidence['receipt']['path']), {'identity': evidence['identity']})
    with pytest.raises(ValueError, match='invalid_receipt_paths'):
        run(evidence, tmp_path / 'result', catalog)

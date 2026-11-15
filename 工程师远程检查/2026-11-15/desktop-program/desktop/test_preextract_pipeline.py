"""Single-paper orchestration, with real DOCX/files and no external requests."""
import copy
import hashlib
import io
import json
from pathlib import Path

import pytest

import preextract_pipeline as pipeline
# Load the receipt adapter before per-test delivery guards are installed;
# activation reuses its receipt predicate and must not retain a patched alias.
import preextract_handoff


def _read(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def _hash(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _no_external(*args, **kwargs):
    raise AssertionError('this scenario must not access model connection or external delivery')


@pytest.fixture
def isolated(tmp_path, monkeypatch):
    from docx import Document
    import preextract_publish
    import urllib.request
    real_prepare = preextract_publish.prepare_publication

    root = tmp_path / 'isolated-app'
    root.mkdir()
    runtime = tmp_path / 'runtime'
    runtime.mkdir()
    catalog = {'schema':'specialty-routing/1.0', 'majors':[
        dict(code='080901', name='计算机科学与技术', discipline_code='08', discipline='工学',
             group_code='0809', group='计算机类', paper_dataset_id='fake-paper', image_dataset_id='fake-images')]}
    (runtime / 'config.json').write_text('{}', encoding='utf-8')
    (runtime / 'catalog.json').write_text(json.dumps(catalog, ensure_ascii=False), encoding='utf-8')
    template = tmp_path / 'template.md'
    template.write_bytes(pipeline.DEFAULT_TEMPLATE.read_bytes())
    source = tmp_path / 'paper.docx'
    document = Document()
    document.add_heading('1 排程研究', 1)
    document.add_paragraph('本研究比较两种排程方案，目标是减少作业等待时间。')
    document.add_paragraph('材料为三个作业的到达时刻和持续时间。先按两种规则排序，再记录各作业等待时间，比较平均等待时间。')
    document.add_paragraph('比较得到两组等待时间。本例仅说明比较过程，新问题需要重新采集作业数据。')
    document.save(source)
    monkeypatch.setattr(pipeline, 'ROOT', root)
    monkeypatch.setattr(pipeline, 'load_model_connection', _no_external)
    monkeypatch.setattr(pipeline, 'request_once', _no_external)
    monkeypatch.setattr(pipeline, 'offline_counter', _no_external)
    monkeypatch.setattr(preextract_publish, 'prepare_publication', _no_external)
    monkeypatch.setattr(preextract_publish, 'deliver_publication', _no_external)
    monkeypatch.setattr(urllib.request.OpenerDirector, 'open', _no_external)
    return dict(root=root, runtime=runtime, template=template, source=source, catalog=catalog,
                real_prepare=real_prepare)


def _run(env, stage='prepare', work_dir=None):
    return pipeline.run_single(env['source'], work_dir=work_dir, template_path=env['template'],
        runtime_dir=env['runtime'], paperflow_root=env['root'] / 'unused-paperflow', stage=stage)


def _empty(value):
    if isinstance(value, dict):
        return {key:_empty(item) for key,item in value.items()}
    return [] if isinstance(value, list) else copy.deepcopy(value)


def _candidate(env, bundle):
    _, skeleton = pipeline.load_template(env['template'])
    candidate = _empty(skeleton)
    candidate.update(record_kind='filled_candidate', source=copy.deepcopy(bundle['source']))
    block = next(row for row in bundle['blocks'] if '先按两种规则排序' in row['text'])
    ref = dict(file_ref='main', block_id=block['id'], section_id=block['section_id'], asset_id=None,
               start=None, end=None, source_kind='author_text', locator_hint=None)
    candidate['source']['reading_scope'].update(read_refs=['main', block['id']], notes='根据提供的完整文本提取')
    profile = candidate['profile']
    profile.update(material_type='论文正文')
    profile['material_disposition'].update(decision='keep', reason='有明确的比较研究过程', source_refs=[ref])
    profile['article'].update(title='排程研究', keywords=['排程', '等待时间'], summary='比较两个排程方案的等待时间', source_refs=[ref])
    profile['research_profile'].update(problem='如何减少作业等待时间', object='作业排程', overall_path='整理作业数据后比较两种方案', unit_sequence=['U001'], source_refs=[ref])
    unit = _empty(skeleton['profile']['learning_units'][0])
    unit.update(id='U001', title='比较排程方案', question='两种方案的等待时间如何比较', what_to_learn='组织输入并计算比较结果',
        method='对比实验', is_core=True, core_reason='承载比较研究过程', work_attribution='author_work',
        interpretation='比较得到两组等待时间', conditions=[], limitations=['新问题需重新采集作业数据'],
        section_ids=[block['section_id']], source_refs=[ref])
    unit['method_evidence'].update(status='documented', detail='原文明确列出排序、记录及比较步骤', source_refs=[ref])
    unit['materials'] = [dict(id='M001', type='作业数据', description='三个作业的到达时刻和持续时间', key_fields=['到达时刻', '持续时间'],
        time_scope=None, comparison_objects=[], organization=None, source_refs=[ref])]
    unit['steps'] = [dict(id='T001', operation='按两种规则排序，记录作业等待时间并比较平均值', input_refs=['M001'], output_refs=['O001'], source_refs=[ref])]
    unit['outputs'] = [dict(id='O001', type='比较结果', content='两组等待时间', source_refs=[ref])]
    profile['learning_units'] = [unit]
    profile['routing_candidates'] = [dict(code='080901', name='计算机科学与技术', reason='排程算法比较', unit_ids=['U001'], source_refs=[ref])]
    nodes = []
    for section in bundle['sections']:
        row = _empty(skeleton['tree']['nodes'][0])
        row.update(id=section['id'], title=section['title'], parent_id=section.get('parent_id'), order=section['order'],
                   source_refs=[dict(ref, block_id=None, section_id=section['id'])], unit_ids=['U001'])
        nodes.append(row)
    candidate['tree']['nodes'] = nodes
    return candidate


def _save_completed_response(env, work, candidate):
    """Exercise the real request journal using a deterministic external reply."""
    from preextract_request import build_request, request_once
    bundle = _read(work / 'material.json')
    template = env['template'].read_text(encoding='utf-8')
    connection = dict(model='test-model', base_url='http://127.0.0.1:9/v1', api_key='test-only',
        context_window=300000, max_tokens=1000, max_output_tokens=1000)
    request, _ = build_request(bundle, template, env['catalog'], connection)
    text = json.dumps(candidate, ensure_ascii=False)
    raw = json.dumps(dict(choices=[dict(message=dict(content=text), finish_reason='stop')],
                          usage=dict(prompt_tokens=10, completion_tokens=10)), ensure_ascii=False).encode('utf-8')

    class Response(io.BytesIO):
        headers = {'Content-Type':'application/json'}

    class LocalReply:
        def open(self, *args, **kwargs):
            return Response(raw)

    request_once(request, connection, work / 'model', opener=LocalReply())
    return text


def test_original_pipeline_binds_resized_images_and_reuses_saved_response(isolated, monkeypatch):
    from docx import Document
    from PIL import Image
    from preextract_request import request_once
    from preextract_images import restore_request_images
    env = isolated
    image = env['source'].with_suffix('.png')
    Image.new('RGB', (2362, 743), 'white').save(image)
    doc = Document(env['source'])
    doc.add_picture(str(image))
    doc.save(env['source'])
    work = Path(_run(env)['work_dir'])
    material_bytes = (work/'material.json').read_bytes()
    original = _read(work/'material.json')
    calls, stages = [], []
    connection = dict(model='test-model', base_url='http://127.0.0.1:9/v1', api_key='test-only',
        context_window=1000000, max_tokens=1000, max_output_tokens=1000,
        max_image_side=2000, max_single_image_bytes=5*1024*1024)

    def invoke(body, cfg, directory, **kwargs):
        calls.append(body)
        bound = restore_request_images(original, work, body)
        candidate = _candidate(env, bound)
        source = next(a for a in bound['assets'] if a.get('image_path'))
        assert source['width'] == 2000
        _, skeleton = pipeline.load_template(env['template'])
        asset = _empty(skeleton['assets']['items'][0])
        asset.update(id=source['id'], source_image_id=source['id'], kind='figure', storage='image',
            evidence_basis='pixels', caption='实验图', meaning='研究流程', section_ids=[bound['sections'][0]['id']], unit_ids=['U001'])
        candidate['assets']['items'] = [asset]
        row = _empty(skeleton['redaction_plan'][0])
        row.update(id='R1', entity_key='signature', scope='image', action='mask', location_status='located',
            image_ref=dict(asset_id=source['id'], rendition_sha256=source['sha256'], width=source['width'], height=source['height']),
            redactions=[dict(x0=0, y0=0, x1=20, y1=20)])
        candidate['redaction_plan'] = [row]
        raw = json.dumps(dict(choices=[dict(message=dict(content=json.dumps(candidate, ensure_ascii=False)),
                    finish_reason='stop')]), ensure_ascii=False).encode()
        class Response(io.BytesIO):
            headers = {'Content-Type':'application/json'}
        class Reply:
            def open(self, *args, **kwargs):
                return Response(raw)
        return request_once(body, cfg, directory, opener=Reply(), **kwargs)

    state = pipeline.run_single(env['source'], work_dir=work, template_path=env['template'],
        runtime_dir=env['runtime'], stage='extract', connection=connection, invoke=invoke, progress=stages.append)
    assert state['stage'] == 'candidate_validated'
    assert stages.index('adapting_images') < stages.index('extracting_once')
    before = {p:_hash(p) for p in [work/'material.json', work/'model/request.json', work/'model/response.txt',
                                  work/'private/request-images.json']}
    monkeypatch.setattr(pipeline, 'prepare_request_images', _no_external)
    assert _run(env, stage='extract', work_dir=work)['stage'] == 'candidate_validated'
    assert len(calls) == 1 and (work/'material.json').read_bytes() == material_bytes
    assert all(_hash(p) == digest for p, digest in before.items())
    child = pipeline.create_revision(env['source'], work, 'image-bound-import', mode='candidate_import', reason='test')
    pipeline.save_candidate_import(child, _read(work/'private/candidate.json'), evidence=[work/'model/response.txt'])
    assert _run(env, stage='extract', work_dir=child)['stage'] == 'candidate_validated'
    assert not (child/'model').exists()


def test_material_preparation_checks_every_image_without_model_configuration(isolated):
    from docx import Document
    from PIL import Image
    image = isolated['source'].with_suffix('.png')
    Image.new('RGB', (2362, 743), 'white').save(image)
    doc = Document(isolated['source'])
    doc.add_picture(str(image))
    doc.save(isolated['source'])
    result = _run(isolated)
    work = Path(result['work_dir'])
    assert result['stage'] == 'material_prepared'
    report = _read(work/'private/request-images.json')
    assert report['image_count'] == 1 and report['adjusted_count'] == 1
    assert report['images'][0]['binding']['width'] == 2000
    assert _read(work/'material.json')['assets'][0]['width'] == 2362
    assert not (work/'model').exists()


@pytest.mark.parametrize('delivery_state', ['binding', 'pending', 'complete'])
def test_image_policy_upgrade_keeps_package_with_existing_delivery(isolated, monkeypatch, delivery_state):
    import preextract_publish
    from test_preextract_publish import Counter, PAPERFLOW
    work = Path(_run(isolated)['work_dir'])
    _save_completed_response(isolated, work, _candidate(isolated, _read(work/'material.json')))
    counter = Counter()
    counter.close = lambda: None
    monkeypatch.setattr(pipeline, 'offline_counter', lambda *args: counter)
    monkeypatch.setattr(preextract_publish, 'prepare_publication', isolated['real_prepare'])
    kwargs = dict(work_dir=work, template_path=isolated['template'], runtime_dir=isolated['runtime'],
                  paperflow_root=PAPERFLOW, stage='package')
    pipeline.run_single(isolated['source'], **kwargs)
    path = work/'private/prepared-publication.json'
    old = _read(path)
    old['validation']['publication_version'] = 'preextract-publication/1.4'
    pipeline.write_json(path, old)
    marker = work/('delivery-binding.json' if delivery_state == 'binding' else 'delivery-receipt.json')
    pipeline.write_json(marker, dict(status=delivery_state, manifest_sha256=old['validation']['manifest_sha256']))
    protected = {p:_hash(p) for p in Path(old['package_path']).rglob('*') if p.is_file()}
    protected.update({path:_hash(path), marker:_hash(marker)})
    monkeypatch.setattr(pipeline, 'offline_counter', _no_external)
    monkeypatch.setattr(preextract_publish, 'prepare_publication', _no_external)
    result = pipeline.run_single(isolated['source'], **kwargs)
    assert result['stage'] == 'package_verified'
    assert result['package_path'] == old['package_path']
    assert all(_hash(p) == digest for p, digest in protected.items())


def test_queued_seal_without_delivery_files_is_not_replaced_by_policy_upgrade(isolated, monkeypatch):
    import preextract_publish
    from preextract_delivery_queue import DeliveryQueue, queue_path
    from test_preextract_publish import Counter, PAPERFLOW
    work = Path(_run(isolated)['work_dir'])
    _save_completed_response(isolated, work, _candidate(isolated, _read(work/'material.json')))
    counter = Counter()
    counter.close = lambda: None
    monkeypatch.setattr(pipeline, 'offline_counter', lambda *args: counter)
    monkeypatch.setattr(preextract_publish, 'prepare_publication', isolated['real_prepare'])
    kwargs = dict(work_dir=work, template_path=isolated['template'], runtime_dir=isolated['runtime'],
                  paperflow_root=PAPERFLOW, stage='package')
    pipeline.run_single(isolated['source'], **kwargs)
    path = work/'private/prepared-publication.json'
    old = _read(path)
    old['validation']['publication_version'] = 'preextract-publication/1.4'
    pipeline.write_json(path, old)
    data_root = isolated['root']/'data'
    queue = DeliveryQueue(queue_path(data_root))
    payload = dict(id='queued-original', service='http://invalid', store_root=str(data_root), run_id='run1',
                   task=old['task'], manifest_sha256=old['validation']['manifest_sha256'],
                   item=dict(id='one', package_path=old['package_path'], preextract_work_dir=str(work), source_hash='source'))
    queue.enqueue(payload)
    before_job = queue.get('queued-original')
    protected = {p:_hash(p) for p in Path(old['package_path']).rglob('*') if p.is_file()}
    protected[path] = _hash(path)
    assert not (work/'delivery-binding.json').exists() and not (work/'delivery-receipt.json').exists()
    monkeypatch.setattr(pipeline, 'offline_counter', _no_external)
    monkeypatch.setattr(preextract_publish, 'prepare_publication', _no_external)
    result = pipeline.run_single(isolated['source'], **kwargs)
    assert result['package_path'] == old['package_path']
    assert queue.get('queued-original') == before_job
    assert all(_hash(p) == digest for p, digest in protected.items())


def test_unbound_learning_seal_reuses_rows_without_model_or_image_processing(isolated, monkeypatch):
    import preextract_publish
    from test_preextract_publish import Counter, PAPERFLOW
    work = Path(_run(isolated)['work_dir'])
    _save_completed_response(isolated, work, _candidate(isolated, _read(work/'material.json')))
    counter = Counter()
    counter.close = lambda: None
    monkeypatch.setattr(pipeline, 'offline_counter', lambda *args: counter)
    monkeypatch.setattr(preextract_publish, 'prepare_publication', isolated['real_prepare'])
    kwargs = dict(work_dir=work, template_path=isolated['template'], runtime_dir=isolated['runtime'],
                  paperflow_root=PAPERFLOW, stage='package')
    pipeline.run_single(isolated['source'], **kwargs)
    path = work/'private/prepared-publication.json'
    old = _read(path)
    old['validation']['publication_version'] = 'preextract-publication/1.4'
    pipeline.write_json(path, old)
    original_rows = _read(Path(old['package_path'])/'chunks.json')['chunks']
    frozen = {'body', 'learning', 'research', 'continuity'}
    protected = {p:_hash(p) for folder in [Path(old['package_path']), work/'model']
                 for p in folder.rglob('*') if p.is_file()}
    monkeypatch.setattr(preextract_publish, 'prepare_publication', _no_external)
    monkeypatch.setattr(preextract_publish, '_mask_images', _no_external)
    result = pipeline.run_single(isolated['source'], **kwargs)
    fresh = _read(path)
    assert result['stage'] == 'package_verified'
    assert fresh['task']['task_id'] != old['task']['task_id']
    assert fresh['validation']['reuse']['source_manifest_sha256'] == old['validation']['manifest_sha256']
    fresh_rows = _read(Path(fresh['package_path'])/'chunks.json')['chunks']
    assert [row for row in fresh_rows if row['kind'] in frozen] == [row for row in original_rows if row['kind'] in frozen]
    assert all(_hash(p) == digest for p, digest in protected.items())
    assert not (work/'delivery-binding.json').exists() and not (work/'delivery-receipt.json').exists()


def test_explicit_revision_import_preserves_parent_and_registry(isolated):
    env = isolated
    parent = Path(_run(env)['work_dir'])
    candidate = _candidate(env, _read(parent / 'material.json'))
    _save_completed_response(env, parent, candidate)
    protected = {p: _hash(p) for p in parent.rglob('*') if p.is_file()}
    registry = env['root'] / 'data/single-request/source-index.json'
    original_registry = registry.read_bytes()
    work = pipeline.create_revision(env['source'], parent, 'authorized-import',
        mode='candidate_import', reason='user authorized a syntax-only correction')
    pipeline.save_candidate_import(work, candidate, evidence=[parent / 'model/response.txt'])
    result = _run(env, stage='extract', work_dir=work)
    assert result['stage'] == 'candidate_validated'
    assert not (work / 'model/request-state.json').exists()
    assert _read(work / 'normalization.json')['candidate_origin'] == 'candidate_import'
    assert all(_hash(p) == sha for p, sha in protected.items())
    assert registry.read_bytes() == original_registry
    # Candidate provenance is checked on every reuse, not only when imported.
    (parent / 'model/response.txt').write_text('changed', encoding='utf8')
    with pytest.raises(ValueError, match='revision'):
        _run(env, stage='extract', work_dir=work)


def test_authorized_material_refresh_rebuilds_only_revision_and_binds_new_material(isolated):
    env = isolated
    parent = Path(_run(env)['work_dir'])
    candidate = _candidate(env, _read(parent / 'material.json'))
    _save_completed_response(env, parent, candidate)
    protected = {p: _hash(p) for p in parent.rglob('*') if p.is_file()}
    registry = env['root'] / 'data/single-request/source-index.json'
    original_registry = registry.read_bytes()
    child = pipeline.create_revision(env['source'], parent, 'authorized-material-refresh',
        mode='rerun', reason='user authorized retest after source page renderer repair',
        refresh_material=True, catalog_path=env['runtime'] / 'catalog.json')
    material = _read(child / 'material.json')
    assert material['source']['source_sha256'] == _read(parent / 'material.json')['source']['source_sha256']
    assert Path(material['source']['text_file']).is_relative_to(child)
    assert _hash(child / 'material.json') != _hash(parent / 'material.json')
    assert _run(env, stage='prepare', work_dir=child)['stage'] == 'material_prepared'
    assert not (child / 'model').exists()
    assert all(_hash(p) == sha for p, sha in protected.items())
    assert registry.read_bytes() == original_registry
    with pytest.raises(ValueError, match='refresh'):
        pipeline.create_revision(env['source'], parent, 'invalid-refresh-import',
            mode='candidate_import', reason='invalid', refresh_material=True,
            catalog_path=env['runtime'] / 'catalog.json')
    (child / 'material.json').write_text('{}', encoding='utf8')
    with pytest.raises(ValueError, match='material'):
        pipeline._revision(env['source'], child)


def test_revision_rejects_uncertain_parent_and_unbound_candidate(isolated):
    env = isolated
    parent = Path(_run(env)['work_dir'])
    candidate = _candidate(env, _read(parent / 'material.json'))
    _save_completed_response(env, parent, candidate)
    state_path = parent / 'model/request-state.json'
    state = _read(state_path)
    state['status'] = 'request_failed_or_uncertain'
    state_path.write_text(json.dumps(state), encoding='utf8')
    with pytest.raises(ValueError, match='revision'):
        pipeline.create_revision(env['source'], parent, 'no-automatic-retry', mode='rerun', reason='test')
    state['status'] = 'response_complete'
    state_path.write_text(json.dumps(state), encoding='utf8')
    work = pipeline.create_revision(env['source'], parent, 'authorized-import', mode='candidate_import', reason='test')
    with pytest.raises(ValueError, match='import'):
        _run(env, stage='extract', work_dir=work)


def _save_failed_response(env, parent, candidate=None):
    from preextract_request import build_request, request_once
    from urllib.error import HTTPError
    connection = dict(model='test-model', base_url='http://127.0.0.1:9/v1', api_key='test-only',
                      context_window=300000, max_tokens=1000, max_output_tokens=1000)
    body, _ = build_request(_read(parent/'material.json'), env['template'].read_text(encoding='utf8'),
                            env['catalog'], connection)
    class Response(io.BytesIO):
        headers = {'Content-Type':'text/event-stream'}
    class FailedReply:
        def open(self, *args, **kwargs):
            if candidate is None:
                raise HTTPError(connection['base_url'], 400, 'upstream rejected request', {}, io.BytesIO(b'{"error":"invalid request"}'))
            chunk = dict(choices=[dict(delta=dict(content=json.dumps(candidate, ensure_ascii=False)))])
            return Response(('data: '+json.dumps(chunk)+'\n\ndata: {"error":{"message":"stream interrupted"}}\n\n').encode())
    with pytest.raises((RuntimeError, HTTPError)):
        request_once(body, connection, parent/'model', opener=FailedReply())


def test_authorized_failed_response_reuses_complete_answer_without_request(isolated):
    env=isolated;parent=Path(_run(env)['work_dir'])
    candidate=_candidate(env,_read(parent/'material.json'))
    _save_failed_response(env,parent,candidate)
    protected={p:_hash(p) for p in parent.rglob('*') if p.is_file()}
    work=pipeline.create_revision(env['source'],parent,'explicit-failed-reuse',mode='candidate_import',
                                  reason='user authorized recovery after the ended stream',recover_failed=True)
    pipeline.save_candidate_import(work,candidate,evidence=[parent/'model/response-events.jsonl'])
    assert _run(env,stage='extract',work_dir=work)['stage']=='candidate_validated'
    assert not (work/'model').exists()
    assert all(_hash(p)==sha for p,sha in protected.items())
    (parent/'model/response-events.jsonl').write_text('changed',encoding='utf8')
    with pytest.raises(ValueError,match='evidence changed'):
        _run(env,stage='extract',work_dir=work)


def test_authorized_http_failure_retains_absent_response_and_never_auto_retries(isolated):
    env=isolated;parent=Path(_run(env)['work_dir'])
    _save_failed_response(env,parent)
    protected={p:_hash(p) for p in parent.rglob('*') if p.is_file()}
    work=pipeline.create_revision(env['source'],parent,'explicit-failed-rerun',mode='rerun',
                                  reason='user authorized one new attempt',recover_failed=True)
    assert not (parent/'model/response.txt').exists()
    assert pipeline._revision(env['source'],work)['parent_hashes']['model/response.txt'] is None
    calls=[]
    def fail_once(body,connection,directory,**kwargs):
        from preextract_request import request_once
        class Unavailable:
            def open(self,*args,**kwargs):
                calls.append(1)
                raise OSError('still unavailable')
        return request_once(body,connection,directory,opener=Unavailable(),**kwargs)
    connection=dict(model='test-model',base_url='http://127.0.0.1:9/v1',api_key='test-only',
                    context_window=300000,max_tokens=1000,max_output_tokens=1000)
    for _ in range(2):
        with pytest.raises((OSError,RuntimeError)):
            pipeline.run_single(env['source'],work_dir=work,template_path=env['template'],runtime_dir=env['runtime'],
                                stage='extract',connection=connection,invoke=fail_once)
    assert len(calls)==1
    assert all(_hash(p)==sha for p,sha in protected.items())
    (parent/'model/response.txt').write_text('late response',encoding='utf8')
    with pytest.raises(ValueError,match='evidence changed'):
        pipeline._revision(env['source'],work)


@pytest.mark.parametrize('state_change',[{'status':'request_in_flight'},{'finished_at':None}])
def test_failed_recovery_rejects_unfinished_parent(isolated,state_change):
    env=isolated;parent=Path(_run(env)['work_dir'])
    _save_failed_response(env,parent)
    path=parent/'model/request-state.json';state=_read(path);state.update(state_change)
    pipeline.write_json(path,state)
    with pytest.raises(ValueError,match='revision'):
        pipeline.create_revision(env['source'],parent,'unsafe-rerun',mode='rerun',reason='explicit',recover_failed=True)
    assert not (parent/'revisions/unsafe-rerun').exists()


def test_reselect_source_reuses_verified_revision_and_preserves_original_registry(isolated,monkeypatch):
    import preextract_publish
    from test_preextract_publish import Counter,PAPERFLOW
    env=isolated;parent=Path(_run(env)['work_dir'])
    candidate=_candidate(env,_read(parent/'material.json'))
    _save_completed_response(env,parent,candidate)
    registry=env['root']/'data/single-request/source-index.json';before=registry.read_bytes()
    original=_hash(parent/'model/response.txt')
    work=pipeline.create_revision(env['source'],parent,'verified-recovery',mode='candidate_import',reason='authorized recovery')
    pipeline.save_candidate_import(work,candidate,evidence=[parent/'model/response.txt'])
    counter=Counter();counter.close=lambda:None
    monkeypatch.setattr(pipeline,'offline_counter',lambda *a:counter)
    monkeypatch.setattr(preextract_publish,'prepare_publication',env['real_prepare'])
    result=pipeline.run_single(env['source'],work_dir=work,template_path=env['template'],runtime_dir=env['runtime'],paperflow_root=PAPERFLOW,stage='package')
    assert result['stage']=='package_verified'
    with pytest.raises(ValueError,match='verified delivery'):
        pipeline.activate_revision(env['source'],work)
    prepared=_read(work/'private/prepared-publication.json')
    package=preextract_publish._paperflow(PAPERFLOW)
    packet=package.validate_package(prepared['package_path'],prepared['task'],prepared['catalog'],strict_seal=True)
    from paperflow.rag import RagPublisher
    targets={}
    for route in packet['routes']:
        target=dict(verified=True,retrieval_verified=True)
        for role,records in RagPublisher(None).records(packet).items():
            if records:target[role]=dict(dataset_id=route['paper_dataset_id' if role=='body' else 'image_dataset_id'],document_id='verified-'+role)
        targets[route['code']]=target
    pipeline.write_json(work/'delivery-receipt.json',dict(status='complete',task=prepared['task'],manifest_sha256=packet['manifest_sha256'],targets=targets))
    pipeline.activate_revision(env['source'],work)
    assert pipeline.default_work_dir(env['source'],env['template'])==work
    assert registry.read_bytes()==before and _hash(parent/'model/response.txt')==original
    assert pipeline.run_single(env['source'],template_path=env['template'],runtime_dir=env['runtime'],stage='extract')['stage']=='candidate_validated'
    assert not (work/'model').exists()
    pointer=parent/'active-revision.json';data=_read(pointer);data['work_dir']=str(parent.parent/'elsewhere')
    pipeline.write_json(pointer,data)
    with pytest.raises(ValueError,match='active revision'):
        pipeline.default_work_dir(env['source'],env['template'])


@pytest.mark.parametrize('change_learning', [False, True])
def test_explicit_rerun_uses_new_journal_once_and_keeps_prior_learning_context(isolated, change_learning):
    from preextract_request import request_once
    env = isolated
    parent = Path(_run(env)['work_dir'])
    candidate = _candidate(env, _read(parent / 'material.json'))
    _save_completed_response(env, parent, candidate)
    old_response = _hash(parent / 'model/response.txt')
    work = pipeline.create_revision(env['source'], parent, 'authorized-rerun', mode='rerun', reason='larger output budget',
        request_context=dict(instruction='Retain the completed learning content.', content=dict(profile=candidate['profile'])))
    calls = []
    returned = copy.deepcopy(candidate)
    if change_learning:
        returned['profile']['learning_units'][0]['steps'][0]['operation'] = '模型改写了既有步骤'
    class Response(io.BytesIO):
        headers = {'Content-Type':'application/json'}
    class LocalReply:
        def open(self, request, **kwargs):
            calls.append(json.loads(request.data))
            return Response(json.dumps(dict(choices=[dict(message=dict(content=json.dumps(returned)), finish_reason='stop')])).encode())
    connection = dict(model='test-model', base_url='http://127.0.0.1:9/v1', api_key='test-only',
                      context_window=500000, max_tokens=128000, max_output_tokens=128000)
    def invoke(body, config, directory, **kwargs):
        return request_once(body, config, directory, opener=LocalReply(), **kwargs)
    for _ in range(2):
        result = pipeline.run_single(env['source'], work_dir=work, template_path=env['template'], runtime_dir=env['runtime'],
                                     stage='extract', connection=connection, invoke=invoke)
        assert result['stage'] == ('candidate_requires_attention' if change_learning else 'candidate_validated')
    assert len(calls) == 1
    assert calls[0]['max_tokens'] == 128000
    assert json.loads(calls[0]['messages'][1]['content'][-1]['text'])['profile'] == candidate['profile']
    assert _hash(parent / 'model/response.txt') == old_response
    if change_learning:
        assert 'preserved_content_changed' in {e['code'] for e in _read(work/'validation.json')['errors']}


def test_prepare_only_writes_real_docx_material_without_model_or_upload(isolated):
    before = _hash(isolated['source'])
    result = _run(isolated)
    work = Path(result['work_dir'])
    material = _read(work / 'material.json')
    assert result['stage'] == 'material_prepared'
    assert _read(work / 'status.json')['stage'] == 'material_prepared'
    assert material['source']['source_sha256'] == before == _hash(isolated['source'])
    assert _hash(work / 'private' / 'original.docx') == before
    assert any('三个作业的到达时刻和持续时间' in row['text'] for row in material['blocks'])
    assert not (work / 'model').exists()
    assert not (work / 'publication').exists()
    assert not (work / 'delivery-receipt.json').exists()


def test_parser_upgrade_keeps_failed_source_workspace_and_never_resends(isolated, monkeypatch):
    import preextract_material
    first = _run(isolated)
    work = Path(first['work_dir'])
    model = work / 'model'
    model.mkdir()
    state = model / 'request-state.json'
    state.write_text(json.dumps(dict(status='request_failed_or_uncertain', client_http_requests=1,
        request_sha256='f' * 64)), encoding='utf-8')
    original_state = state.read_bytes()
    original_material = (work / 'material.json').read_bytes()
    monkeypatch.setattr(pipeline, 'PARSER_VERSION', 'unit-test-next-parser')
    monkeypatch.setattr(preextract_material, 'PARSER_VERSION', 'unit-test-next-parser')
    assert pipeline.default_work_dir(isolated['source'], isolated['template']) == work
    with pytest.raises((ValueError, RuntimeError)):
        _run(isolated, stage='extract')
    assert state.read_bytes() == original_state
    assert (work / 'material.json').read_bytes() == original_material
    assert list((isolated['root'] / 'data' / 'single-request').rglob('request-state.json')) == [state]
    assert not (model / 'response.txt').exists()
    assert _read(work / 'status.json')['stage'] == 'stopped'


def test_valid_saved_response_is_reused_without_current_model_connection(isolated):
    prepared = _run(isolated)
    work = Path(prepared['work_dir'])
    bundle = _read(work / 'material.json')
    candidate = _candidate(isolated, bundle)
    text = _save_completed_response(isolated, work, candidate)
    response_hash = _hash(work / 'model' / 'response.txt')
    original_state = (work / 'model' / 'request-state.json').read_bytes()
    result = _run(isolated, stage='extract')
    assert result['stage'] == 'candidate_validated'
    assert _read(work / 'validation.json')['valid'] is True
    assert _read(work / 'private' / 'candidate.json')['profile']['learning_units'][0]['steps'][0]['operation'] == '按两种规则排序，记录作业等待时间并比较平均值'
    assert _hash(work / 'model' / 'response.txt') == response_hash
    assert (work / 'model' / 'response.txt').read_text(encoding='utf-8') == text
    assert (work / 'model' / 'request-state.json').read_bytes() == original_state
    assert pipeline.parse_candidate((work / 'private' / '论文预提取.md').read_text(encoding='utf-8'))['schema_version'] == 'paper-preextract/1.0'
    assert not (work / 'publication').exists()


def test_saved_partial_with_only_privacy_limits_reaches_real_package_without_model_or_upload(isolated, monkeypatch):
    import preextract_publish
    from test_preextract_publish import Counter, PAPERFLOW
    work = Path(_run(isolated)['work_dir'])
    bundle = _read(work / 'material.json')
    candidate = _candidate(isolated, bundle)
    candidate['record_kind'] = 'partial_candidate'
    candidate['redaction_plan'] = [dict(id='R1', scope='metadata', action='replace',
        location_status='unavailable', reason='原文件元数据未定位')]
    _save_completed_response(isolated, work, candidate)
    preserved = {p: _hash(p) for p in (work / 'model').iterdir() if p.is_file()}
    counter = Counter()
    counter.close = lambda: None
    monkeypatch.setattr(pipeline, 'offline_counter', lambda *args: counter)
    monkeypatch.setattr(preextract_publish, 'prepare_publication', isolated['real_prepare'])
    result = pipeline.run_single(isolated['source'], work_dir=work, template_path=isolated['template'],
        runtime_dir=isolated['runtime'], paperflow_root=PAPERFLOW, stage='package')
    assert result['stage'] == 'package_verified'
    assert _read(work / 'private/candidate.json')['record_kind'] == 'partial_candidate'
    report = _read(work / 'package-report.json')
    assert report['model_record_kind'] == 'partial_candidate'
    assert report['redaction']['policy'] == 'best_effort'
    assert report['redaction']['omissions']
    assert report['body_exact_reassembly']
    assert {p: _hash(p) for p in preserved} == preserved
    assert not (work / 'delivery-receipt.json').exists()


@pytest.mark.parametrize('stale', ['policy', 'version'])
def test_stale_publication_rebuilds_locally_and_preserves_old_seal_and_response(isolated, monkeypatch, stale):
    from docx import Document
    import preextract_publish
    from test_preextract_publish import Counter, PAPERFLOW
    document = Document(isolated['source'])
    document.paragraphs[0].insert_paragraph_before('Author: Alice Smith')
    document.save(isolated['source'])
    work = Path(_run(isolated)['work_dir'])
    bundle = _read(work / 'material.json')
    candidate = _candidate(isolated, bundle)
    _save_completed_response(isolated, work, candidate)
    original_model = {p: _hash(p) for p in (work / 'model').iterdir() if p.is_file()}
    old_candidate = pipeline.validate_candidate(candidate, bundle, isolated['catalog'])['candidate']
    pipeline.write_json(work / 'private/candidate.json', old_candidate)
    old = isolated['real_prepare'](old_candidate, bundle, work / 'publication',
        paperflow_root=PAPERFLOW, catalog=isolated['catalog'], counter=Counter())
    if stale == 'version':
        old['validation']['redaction']['policy'] = 'best_effort'
        old['validation']['publication_version'] = 'preextract-publication/legacy'
    saved = {k:v for k,v in old.items() if k != 'packet'}
    saved['candidate_sha256'] = _hash(work / 'private/candidate.json')
    pipeline.write_json(work / 'private/prepared-publication.json', saved)
    old_files = {p: _hash(p) for p in (work / 'publication').rglob('*') if p.is_file()}
    assert 'Alice Smith' in (work / 'publication/article.redacted.md').read_text(encoding='utf8')
    counter = Counter()
    counter.close = lambda: None
    monkeypatch.setattr(pipeline, 'offline_counter', lambda *args: counter)
    monkeypatch.setattr(preextract_publish, 'prepare_publication', isolated['real_prepare'])
    result = pipeline.run_single(isolated['source'], work_dir=work, template_path=isolated['template'],
        runtime_dir=isolated['runtime'], paperflow_root=PAPERFLOW, stage='package')
    assert result['stage'] == 'package_verified'
    fresh = Path(result['package_path'])
    assert fresh != work / 'publication'
    assert 'Alice Smith' not in (fresh / 'article.redacted.md').read_text(encoding='utf8')
    assert _read(work / 'package-report.json')['redaction']['policy'] == 'best_effort'
    assert _read(work / 'package-report.json')['redaction']['frontmatter_blocks'] == 1
    assert list((work / 'private/publication-history').glob('*.json'))
    assert {p: _hash(p) for p in old_files} == old_files
    assert {p: _hash(p) for p in original_model} == original_model


def test_saved_response_hash_drift_stops_without_replacing_or_resending(isolated):
    prepared = _run(isolated)
    work = Path(prepared['work_dir'])
    _save_completed_response(isolated, work, _candidate(isolated, _read(work / 'material.json')))
    state = (work / 'model' / 'request-state.json').read_bytes()
    response = work / 'model' / 'response.txt'
    response.write_text(response.read_text(encoding='utf-8') + '\nchanged', encoding='utf-8')
    altered = response.read_bytes()
    with pytest.raises(ValueError, match='响应'):
        _run(isolated, stage='extract')
    assert response.read_bytes() == altered
    assert (work / 'model' / 'request-state.json').read_bytes() == state
    assert _read(work / 'status.json')['stage'] == 'stopped'
    assert not (work / 'private' / 'candidate.json').exists()


def test_real_prepared_material_and_full_md_candidate_share_source_schema(isolated):
    prepared = _run(isolated)
    bundle = _read(Path(prepared['work_dir']) / 'material.json')
    candidate = _candidate(isolated, bundle)
    candidate['source']['source_id'] = None
    candidate['source']['source_sha256'] = None
    result = pipeline.validate_candidate(candidate, bundle, isolated['catalog'])
    assert result['valid'], result['errors']
    final = result['candidate']
    assert final['source']['source_sha256'] == _hash(isolated['source'])
    assert final['source']['source_id'] == bundle['source']['source_id']
    assert {row['id'] for row in final['tree']['nodes']} == {row['id'] for row in bundle['sections']}
    assert final['profile']['learning_units'][0]['source_refs'][0]['block_id'] in {row['id'] for row in bundle['blocks']}
    assert candidate['source']['source_sha256'] is None

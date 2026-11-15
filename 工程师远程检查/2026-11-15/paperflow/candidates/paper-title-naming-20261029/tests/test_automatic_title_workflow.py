import asyncio
import hashlib
import json
import threading
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace

import pytest

from paperflow_prefect.unified import execute_unified


def _paper(display=None):
    return {'paper_id': 'PF_test', 'group_id': 'group', 'body': {
        'file_id': 'BODY_test', 'sha256': 'a' * 64, 'display_name': display},
        'attachments': [{'file_id': 'ATT_test'}]}


def _receipt():
    return {'state': '已接收', 'unresolved': [], 'items': [{'state': '已接收'}]}


def test_missing_title_is_requested_between_body_preparation_and_profile():
    calls = []
    async def call(stage, paper, row=None):
        calls.append(stage)
        return {'status': 'generated'} if stage == 'generate_title' else {'status': 'complete'}
    result = asyncio.run(execute_unified(_paper(), _receipt(), call))
    assert calls.count('generate_title') == 1
    assert calls.index('prepare') < calls.index('generate_title') < calls.index('profile')
    assert calls.index('generate_title') < calls.index('attachment')
    assert result['status'] == 'complete'


def test_existing_title_needs_no_supplementary_model_request():
    calls = []
    async def call(stage, paper, row=None):
        calls.append(stage)
        return {'status': 'complete'}
    asyncio.run(execute_unified(_paper('已有题目.docx'), _receipt(), call))
    assert 'generate_title' not in calls


def test_failed_title_request_does_not_block_body_or_attachment():
    calls = []
    async def call(stage, paper, row=None):
        calls.append(stage)
        if stage == 'generate_title':
            raise RuntimeError('unknown title request outcome')
        return {'status': 'complete'}
    result = asyncio.run(execute_unified(_paper(), _receipt(), call))
    assert 'profile' in calls and 'attachment' in calls and 'finish' in calls
    assert result['status'] == 'complete'
    assert result['title_generation']['status'] == 'unavailable'


def test_title_waiters_use_a_separate_batch_gate_from_body_profile():
    from paperflow_prefect.flows import RESOURCES, make_gates
    gates = make_gates({'limits': {'body_llm': 100, 'local': 1}})
    assert RESOURCES['generate_title'] == 'title_llm'
    assert gates['title_llm'] is not gates['body_llm']
    assert gates['title_llm']._value == 16 and gates['body_llm']._value == 100


@pytest.mark.parametrize('kind', ['stage3', 'connected'])
def test_other_prefect_entries_also_schedule_title_once(kind):
    from paperflow_prefect.workflow import execute_paper, execute_stage3
    calls = []
    async def call(stage, paper, row=None):
        calls.append(stage)
        return {'status':'generated'} if stage == 'generate_title' else {'status':'complete'}
    if kind == 'stage3':
        result = asyncio.run(execute_stage3(_paper(), call))
        assert result['status'] == 'stage3_complete'
    else:
        result = asyncio.run(execute_paper(_paper(), call))
        assert result['status'] == 'complete'
    assert calls.count('generate_title') == 1
    assert calls.index('prepare') < calls.index('generate_title') < calls.index('profile')
    assert calls.index('generate_title') < calls.index('attachment')


def test_old_body_request_is_not_reissued_as_a_title_request(tmp_path, monkeypatch):
    from paperflow_prefect.backend import Backend
    from paperflow_prefect import legacy
    body_work = tmp_path / 'body'
    (body_work / 'model').mkdir(parents=True)
    (body_work / 'model' / 'request-state.json').write_text('{"status":"request_in_flight"}', encoding='utf8')
    backend = object.__new__(Backend)
    backend.root = tmp_path / 'data'
    backend.body_work = lambda paper: body_work
    backend.settings = SimpleNamespace(require_execution=lambda: None)
    monkeypatch.setattr(legacy, 'connection', lambda *args: pytest.fail('must not make another request'))
    result = backend.generate_title(_paper())
    assert result['status'] == 'existing_body_request_preserved'


@pytest.mark.parametrize('old_artifact', ['profile-result.json', 'private/candidate.json',
                                          'publication', 'delivery-receipt.json'])
def test_previous_body_result_without_model_directory_is_not_replayed(tmp_path, monkeypatch, old_artifact):
    from paperflow_prefect.backend import Backend
    from paperflow_prefect import legacy
    body_work = tmp_path / 'body'; body_work.mkdir()
    old = body_work / old_artifact
    if old_artifact == 'publication': old.mkdir()
    else:
        old.parent.mkdir(parents=True, exist_ok=True)
        old.write_text('{}', encoding='utf8')
    backend = object.__new__(Backend); backend.root = tmp_path / 'data'
    backend.body_work = lambda paper: body_work
    backend.settings = SimpleNamespace(require_execution=lambda: None)
    monkeypatch.setattr(legacy, 'connection', lambda *args: pytest.fail('no historical replay'))
    assert backend.generate_title(_paper())['status'] == 'existing_body_request_preserved'


def test_profile_name_requires_a_bound_generated_title_result(tmp_path):
    from paperflow_prefect.named_delivery import apply_generated_profile_name, NamedDeliveryError
    source = tmp_path / '原文.docx'; source.write_bytes(b'body')
    named = tmp_path / '拟定题目.docx'; named.write_bytes(b'body')
    digest = hashlib.sha256(b'body').hexdigest()
    material = {'source': {'source_sha256': digest}, 'original_filename': source.name}
    mp = tmp_path / 'material.json'; mp.write_text(json.dumps(material, ensure_ascii=False), encoding='utf8')
    proof = {'paper_id':'PF_test', 'file_id':'BODY_test', 'kind':'body',
             'source_sha256':digest, 'source_path':str(source), 'display_name':named.name,
             'delivery_path':str(named)}
    pp = tmp_path / 'named-input-proof.json'; pp.write_text(json.dumps(proof, ensure_ascii=False), encoding='utf8')
    mh = hashlib.sha256(mp.read_bytes()).hexdigest()
    ph = hashlib.sha256(pp.read_bytes()).hexdigest()
    marker = {'schema':'generated-profile-name/1', 'paper_id':'PF_test', 'body_file_id':'BODY_test',
              'source_sha256':digest, 'display_name':named.name, 'material_sha256':mh, 'proof_sha256':ph}
    (tmp_path/'generated-profile-name.json').write_text(json.dumps(marker, ensure_ascii=False), encoding='utf8')
    with pytest.raises(NamedDeliveryError):
        apply_generated_profile_name(material, tmp_path)
    assert material['original_filename'] == source.name
    assert hashlib.sha256(mp.read_bytes()).hexdigest() == mh


@pytest.mark.parametrize('profile_starts_during_title', [False, True])
def test_generated_title_stage_reuses_native_journal_and_names_body(tmp_path, monkeypatch, profile_starts_during_title):
    from paperflow_prefect.backend import Backend
    from paperflow_prefect import legacy
    from paperflow_prefect.named_delivery import apply_generated_profile_name
    source = tmp_path / '学号姓名.docx'; source.write_bytes(b'paper body')
    original_hash = hashlib.sha256(source.read_bytes()).hexdigest()
    work = tmp_path / 'bodywork'; (work / 'private').mkdir(parents=True)
    text = work / 'private/body.txt'; text.write_text('这篇论文研究校园图书借阅管理。', encoding='utf8')
    text_hash = hashlib.sha256(text.read_bytes()).hexdigest()
    material = {'source': {'source_sha256':original_hash, 'text_sha256':text_hash},
                'original_filename':source.name, 'file_hashes':{str(text.resolve()):text_hash}}
    (work / 'material.json').write_text(json.dumps(material, ensure_ascii=False), encoding='utf8')
    paper = {'paper_id':'PF_test', 'group_id':'group', 'body':{'file_id':'BODY_test',
             'sha256':original_hash, 'path':str(source), 'bytes':source.stat().st_size,
             'kind':'body', 'display_name':None}, 'attachments':[]}
    backend = object.__new__(Backend)
    backend.root = tmp_path / 'data'; backend.body_work = lambda paper: work
    backend.settings = SimpleNamespace(require_execution=lambda: None)
    backend.stops = {'PF_test':threading.Event()}
    backend.local_scope = lambda: pytest.fail('title needs its bounded local scope')
    entries = []
    class OneShotLocal:
        def __enter__(self):
            entries.append(1)
            if len(entries) > 1: raise AssertionError('must not reacquire after model reply')
        def __exit__(self, *args): pass
    backend.title_local_scope = OneShotLocal
    monkeypatch.setattr(legacy, 'connection', lambda _: {'model':'test', 'context_window':1000000,
        'max_output_tokens':128000, 'supports_json_object':False, 'timeout':60})
    calls = []
    def invoke(kind, body, connection, directory, **kwargs):
        calls.append((kind, kwargs['admission_timeout']))
        directory = Path(directory); directory.mkdir(parents=True, exist_ok=True)
        formatted = json.dumps(body, ensure_ascii=False, indent=2).encode()
        compact = json.dumps(body, ensure_ascii=False, separators=(',', ':')).encode()
        response = '{"title":"校园图书借阅管理系统设计"}'
        (directory/'request.json').write_bytes(formatted)
        (directory/'response.txt').write_text(response, encoding='utf8')
        (directory/'request-state.json').write_text(json.dumps({'status':'response_complete',
            'finish_reason':'stop', 'http_invoked':True, 'client_http_requests':1,
            'request_sha256':hashlib.sha256(compact).hexdigest(),
            'stored_request_sha256':hashlib.sha256(formatted).hexdigest(),
            'response_sha256':hashlib.sha256(response.encode()).hexdigest()}), encoding='utf8')
        if profile_starts_during_title:
            (work/'model').mkdir(exist_ok=True)
            (work/'model'/'request-state.json').write_text('{"status":"request_in_flight"}', encoding='utf8')
        return response
    backend.invoke = invoke
    first = backend.generate_title(paper)
    if profile_starts_during_title:
        assert first['status'] == 'generated_after_profile_started'
        assert not (work/'generated-profile-name.json').exists()
        from paperflow_prefect.named_delivery import resolved_delivery_row
        assert resolved_delivery_row(paper, paper['body'], backend.root)['display_name'] is None
        assert backend.generate_title(paper)['status'] == 'existing_body_request_preserved'
        assert calls == [('body', 5)]
        return
    assert first['status'] == 'generated'
    assert backend.generate_title(paper)['status'] == 'available'
    assert calls == [('body', 5)]
    assert len(entries) == 1
    result = apply_generated_profile_name(material, work)
    assert result['original_filename'] == '校园图书借阅管理系统设计.docx'
    assert material['original_filename'] == source.name
    assert source.read_bytes() == b'paper body'
    marker = json.loads((work/'generated-profile-name.json').read_text(encoding='utf8'))
    title_file = Path(marker['title_path']); saved = title_file.read_bytes()
    title_file.write_text('{}', encoding='utf8')
    with pytest.raises(ValueError): apply_generated_profile_name(material, work)
    title_file.write_bytes(saved)

    # Force the native partition path with the generated filename projected in
    # memory; a later native revision must still recognize the disk material.
    from paperflow_prefect import body as body_adapter
    from paperflow_prefect.settings import Settings
    legacy.bind(Settings.load())
    import preextract_contract
    import preextract_images
    import preextract_request
    import preextract_partition_run as native_partition
    from preextract_partition import digest as material_digest
    from paperflow_prefect.records import read_json, write_json
    monkeypatch.setattr(preextract_contract, 'load_template', lambda _: ('fixture template', {}))
    monkeypatch.setattr(preextract_images, 'prepare_request_images', lambda original, *_: (original, {}))
    def plan(adapted, *_):
        request = {'model':'fixture', 'messages':[{'role':'system','content':'fixture'},
                    {'role':'user','content':[{'type':'text','text':'{}'}]}]}
        return {'mode':'partition', 'coverage':{}, 'parts':[
            {'id':name, 'body':request, 'bundle':adapted, 'ranges':[], 'asset_ids':[], 'capacity':{}}
            for name in ('part-001', 'part-002')]}
    monkeypatch.setattr(preextract_request, 'plan_requests', plan)
    class CheckedOffline(Exception): pass
    def verify_partition(original, path, template, catalog, conn, invoke, **kwargs):
        path = Path(path)
        stored = read_json(path/'model/partition-plan.json')
        assert original['original_filename'] == source.name
        assert read_json(path/'model/partition-material.json')['original_filename'] == '校园图书借阅管理系统设计.docx'
        assert stored['source_material_sha256'] == material_digest(read_json(path/'material.json'))
        state = read_json(path/'model/partition-state.json')
        state['requests'] = [{'id':'part-001'}, {'id':'part-002'}]
        write_json(path/'model/partition-state.json', state)
        first_dir = path/'model/part-001'
        request = read_json(first_dir/'planned-request.json')
        write_json(first_dir/'request.json', request)
        reply = '已完成的原分段'; (first_dir/'response.txt').write_text(reply, encoding='utf8')
        request_sha = native_partition._request_hash(request)
        write_json(first_dir/'request-state.json', {'status':'response_complete',
            'request_sha256':request_sha, 'response_sha256':hashlib.sha256(reply.encode()).hexdigest()})
        previous = (first_dir/'response.txt').read_bytes()
        failed_dir = path/'model/part-002'
        write_json(failed_dir/'request-state.json', {'status':'request_failed_or_uncertain',
            'started_at':1, 'finished_at':2, 'http_invoked':True})
        native_partition.create_partition_revision(source, path, 'offline-check', 'part-002',
                                                    reason='offline fixture', recover_failed=True)
        assert native_partition._saved_reply(first_dir, request_sha) == reply
        assert (first_dir/'response.txt').read_bytes() == previous
        assert read_json(path/'model/partition-state.json')['recoveries'][0]['id'] == 'part-002'
        raise CheckedOffline()
    monkeypatch.setattr(native_partition, 'run_partition', verify_partition)
    with pytest.raises(CheckedOffline):
        body_adapter._extract_profile(work, tmp_path/'template.md', {}, {'model':'fixture'},
                                      lambda *args: pytest.fail('no model request in this check'))

"""Approved delivery-only gates; external model/server calls use local substitutes."""
import shutil
from pathlib import Path
from types import SimpleNamespace

import pytest

from paperflow.config import Config
from paperflow.graph import Workflow
from paperflow.package import PackageError, load, save, seal_package, sha, validate_package
from paperflow.producer import HarnessProducer, ModelServiceError, SyntaxRepairExhausted
from paperflow.rag import RagPublisher, RemoteConflict
from paperflow.store import Store
from test_rag import MemoryRag


def set_routes(root, routes):
    profile = load(root / 'profile.json')
    profile['routing_candidates'] = routes
    save(root / 'profile.json', profile)
    save(root / 'corrections.json', dict(final_routing_candidates=routes))


def test_auxiliary_files_and_model_concerns_are_not_delivery_gates(package_factory):
    root, task, catalog = package_factory()
    (root / 'corrections.json').write_text('{broken optional note', encoding='utf-8')
    save(root / 'review.json', dict(processing_blockers=['作者数据有疑点', '可能还存在姓名']))
    assets = load(root / 'assets.json')
    assets['items'][0].pop('description')
    assets['items'][0]['caption']='说明指标之间的分解关系。'
    save(root / 'assets.json', assets)
    seal_package(root, task, 1)
    assert validate_package(root, task, catalog)['routes'][0]['code'] == '120201'


def test_missing_optional_reports_do_not_block_usable_core_files(package_factory):
    root, task, catalog = package_factory()
    (root / 'review.json').unlink()
    (root / 'corrections.json').unlink()
    seal_package(root, task, 1)
    assert validate_package(root, task, catalog)['chunks'][0]['kind'] == 'body'


def test_mechanical_id_and_tree_repairs_keep_all_text(package_factory):
    root, task, catalog = package_factory()
    tree = load(root / 'tree.json')
    tree['nodes'] = [dict(id='s1', title='第一章', parent_id='missing'),
                     dict(id='s1', title='第二章', parent_id='s1')]
    save(root / 'tree.json', tree)
    chunks = load(root / 'chunks.json')
    chunks['chunks'][1]['id'] = chunks['chunks'][0]['id']
    original_texts = [r['content'] for r in chunks['chunks']]
    save(root / 'chunks.json', chunks)
    seal_package(root, task, 1)
    packet = validate_package(root, task, catalog)
    assert [r['content'] for r in packet['chunks']] == original_texts
    assert len({r['id'] for r in packet['chunks']}) == 2
    assert [n['title'] for n in packet['tree']['nodes']] == ['第一章', '第二章']
    assert len({n['id'] for n in packet['tree']['nodes']}) == 2


def test_valid_code_tolerates_name_difference_and_needs_no_unused_image_mapping(package_factory):
    root, task, catalog = package_factory()
    set_routes(root, [dict(code='120201', name='工商管理专业')])
    catalog['majors'][0].pop('image_dataset_id')
    seal_package(root, task, 1)
    packet = validate_package(root, task, catalog)
    assert packet['routes'][0]['name'] == '工商管理'
    assert packet.get('pending_routes', []) == []


def test_unknown_specialty_keeps_valid_targets_and_explicit_pending_target(package_factory):
    root, task, catalog = package_factory()
    set_routes(root, [dict(code='120201', name='工商管理'), dict(code='unknown', name='模型选择')])
    seal_package(root, task, 1)
    packet = validate_package(root, task, catalog)
    assert [r['code'] for r in packet['routes']] == ['120201']
    assert [r['code'] for r in packet['pending_routes']] == ['unknown']


@pytest.mark.parametrize('stage', ['round3_redact', 'round3_chunk'])
def test_producer_accepts_core_delivery_without_optional_maps_or_reports(tmp_path, package_factory, stage):
    source = tmp_path / 'source.txt'
    source.write_text('paper', encoding='utf-8')
    root, task, _ = package_factory(source_sha=sha(source))
    (root / 'corrections.json').write_text('{not JSON', encoding='utf-8')
    save(root / 'review.json', dict(processing_blockers=['只作备注']))
    task.update(source_path=str(source), revision=1)

    class Harness:
        calls = 0
        def __init__(self, **kwargs): self.output = Path(kwargs['cwd'])
        def start(self): pass
        def close(self): pass
        def run(self, *args, **kwargs):
            type(self).calls += 1
            for name in ('article.redacted.md', 'profile.json', 'tree.json', 'assets.json', 'chunks.json',
                         'review.json', 'corrections.json'):
                shutil.copy2(root / name, self.output / name)
            return SimpleNamespace(finish_reason='completed', events=[])

    result = HarnessProducer(Config(home=str(tmp_path / 'runtime')), factory=Harness).run_stage(task, stage)
    assert result['disposition'] == 'keep'
    assert Harness.calls == 1


def test_json_correction_has_two_shared_extra_attempts(tmp_path):
    source = tmp_path / 'source.txt'
    source.write_text('paper', encoding='utf-8')
    task = dict(task_id='t', source_id='s', source_sha256=sha(source), source_path=str(source), revision=1)

    class Harness:
        calls = 0
        def __init__(self, **kwargs): self.output = Path(kwargs['cwd'])
        def start(self): pass
        def close(self): pass
        def run(self, *args, **kwargs):
            type(self).calls += 1
            (self.output / 'profile.json').write_text('{"value": "unterminated', encoding='utf-8')
            return SimpleNamespace(finish_reason='completed', events=[])

    producer = HarnessProducer(Config(home=str(tmp_path / 'runtime')), factory=Harness)
    with pytest.raises(SyntaxRepairExhausted): producer.run_stage(task, 'round1')
    assert Harness.calls == 3


def test_small_json_wrappers_are_normalized_without_another_model_call(tmp_path, package_factory):
    source = tmp_path / 'source.txt'
    source.write_text('paper', encoding='utf-8')
    root, task, _ = package_factory(source_sha=sha(source))
    task.update(source_path=str(source), revision=1)

    class Harness:
        calls = 0
        def __init__(self, **kwargs): self.output = Path(kwargs['cwd'])
        def start(self): pass
        def close(self): pass
        def run(self, *args, **kwargs):
            type(self).calls += 1
            for name in ('article.redacted.md', 'profile.json', 'tree.json'):
                shutil.copy2(root / name, self.output / name)
            data = (self.output / 'profile.json').read_text(encoding='utf-8').rstrip()
            (self.output / 'profile.json').write_text('```json\n' + data[:-1] + ',}\n```', encoding='utf-8')
            return SimpleNamespace(finish_reason='completed', events=[])

    result = HarnessProducer(Config(home=str(tmp_path / 'runtime')), factory=Harness).run_stage(task, 'round1')
    assert result['disposition'] == 'keep'
    assert Harness.calls == 1


def test_two_failed_corrections_park_instead_of_creating_more_revisions(tmp_path):
    config = Config(home=str(tmp_path / 'runtime'), handoff_root=str(tmp_path / 'handoff'), source_roots=[])
    store = Store(config.root / 'state.sqlite3')
    source = tmp_path / 'source.txt'
    source.write_text('paper', encoding='utf-8')
    task = store.register_source(source, sha(source))

    class Producer:
        calls = []
        def run_stage(self, task, stage):
            self.calls.append(stage)
            raise PackageError('missing required delivery file: ' + str(len(self.calls)))

    producer = Producer()
    flow = Workflow(config, store, producer=producer, catalog={'majors': []})
    try:
        for _ in range(8):
            task = flow.step(task)
            if task['stage'] == 'parked': break
        assert task['stage'] == 'parked'
        assert task['revision'] == 3
        assert producer.calls == ['round1', 'repair', 'repair']
    finally:
        flow.close()


def test_remote_readback_failure_retries_delivery_without_content_repair(tmp_path, package_factory):
    config = Config(home=str(tmp_path / 'runtime'), handoff_root=str(tmp_path / 'handoff'),
                    source_roots=[], retry_seconds=0, short_retry_interval=0)
    store = Store(config.root / 'state.sqlite3')
    source = tmp_path / 'source.txt'
    source.write_text('paper', encoding='utf-8')
    task = store.register_source(source, sha(source))
    root, _, catalog = package_factory(task_id=task['task_id'], source_sha=task['source_sha256'])

    class Publisher(RagPublisher):
        def verify(self, *args): raise RemoteConflict('exact chunk readback mismatch')
    class Producer:
        def run_stage(self, *args): pytest.fail('remote faults must not call the content model')

    task = store.update(task['task_id'], stage='ingest', package_dir=str(root), manifest_sha256=sha(root / 'manifest.json'))
    flow = Workflow(config, store, producer=Producer(), publisher=Publisher(MemoryRag()), catalog=catalog)
    try:
        for _ in range(6):
            task = flow.step(task)
            if task.get('error_kind'): break
        assert task['stage'] == 'ingest_target'
        assert task['revision'] == 1 and task['repair_count'] == 0
        assert store.receipt(task['task_id'], 1, '120201')['body']['document_id']
    finally:
        flow.close()


def test_unmapped_target_does_not_prevent_valid_write_or_allow_false_completion(tmp_path, package_factory):
    config = Config(home=str(tmp_path / 'runtime'), handoff_root=str(tmp_path / 'handoff'), source_roots=[], cleanup=False)
    store = Store(config.root / 'state.sqlite3')
    source = tmp_path / 'source.txt'
    source.write_text('paper', encoding='utf-8')
    task = store.register_source(source, sha(source))
    root, _, catalog = package_factory(task_id=task['task_id'], source_sha=task['source_sha256'])
    set_routes(root, [dict(code='120201', name='工商管理'), dict(code='unknown', name='模型选择')])
    seal_package(root, task, 1)
    task = store.update(task['task_id'], stage='ingest', package_dir=str(root), manifest_sha256=sha(root / 'manifest.json'))
    client = MemoryRag()
    flow = Workflow(config, store, publisher=RagPublisher(client), catalog=catalog)
    try:
        for _ in range(10):
            task = flow.step(task)
            if task['stage'] in ('parked', 'complete', 'repair', 'code_error'): break
        assert store.receipt(task['task_id'], 1, '120201').get('verified')
        assert task['stage'] == 'parked' and task['revision'] == 1
        assert task['delivery_check'] == 'wrong_library'
        assert task['pending_routes'][0]['code'] == 'unknown'
        assert len(client.docs) == 1
        assert not store.active_versions_for(task['task_id'])
    finally:
        flow.close()


@pytest.mark.parametrize('raw', ['{"code":0120201}', '{"text":"first\nsecond"}'])
def test_format_compatibility_does_not_reinterpret_json_values(tmp_path, raw):
    from paperflow.package import normalize_json
    path = tmp_path / 'profile.json'
    path.write_text(raw, encoding='utf-8')
    assert normalize_json(path) is False
    assert path.read_text(encoding='utf-8') == raw


def test_omit_with_whitespace_does_not_turn_into_retained_image(package_factory):
    root, task, catalog = package_factory()
    assets = load(root / 'assets.json')
    assets['items'][0].update(storage='omit ', source_image_id='source-id', file='assets/not-retained.png')
    save(root / 'assets.json', assets)
    seal_package(root, task, 1)
    packet = validate_package(root, task, catalog)
    assert packet['assets'][0]['storage'] == 'omit'
    assert RagPublisher(MemoryRag()).records(packet)['images'] == []


def test_auxiliary_report_lost_after_sealing_does_not_block_core_delivery(package_factory):
    root, task, catalog = package_factory()
    (root / 'review.json').unlink()
    assert validate_package(root, task, catalog)['routes']


def test_partial_routes_cannot_activate_through_direct_resume(tmp_path, package_factory):
    config = Config(home=str(tmp_path / 'runtime'), source_roots=[])
    store = Store(config.root / 'state.sqlite3')
    source = tmp_path / 'source.txt'; source.write_text('paper', encoding='utf-8')
    task = store.register_source(source, sha(source))
    root, _, catalog = package_factory(task_id=task['task_id'], source_sha=task['source_sha256'])
    set_routes(root, [dict(code='120201'), dict(code='unknown')]); seal_package(root, task, 1)
    task = store.update(task['task_id'], stage='activate', package_dir=str(root), manifest_sha256=sha(root/'manifest.json'))
    client = MemoryRag()
    flow = Workflow(config, store, publisher=RagPublisher(client), catalog=catalog)
    try:
        result = flow.step(task)
        assert result['stage'] == 'parked' and result['delivery_check'] == 'wrong_library'
        assert not store.active_versions_for(task['task_id'])
        assert client.docs == {}
        flow.close()
        flow = Workflow(config, store, publisher=RagPublisher(client), catalog=catalog)
        saved = flow.graph.get_state(flow.graph_config(task)).values['task']
        assert saved['pending_routes'][0]['code'] == 'unknown'
    finally:
        flow.close()


def test_syntax_correction_budget_survives_service_failure_and_producer_restart(tmp_path):
    source = tmp_path / 'source.txt'; source.write_text('paper', encoding='utf-8')
    task = dict(task_id='t', source_id='s', source_sha256=sha(source), source_path=str(source), revision=1)
    class Harness:
        calls = 0
        def __init__(self, **kwargs): self.output = Path(kwargs['cwd'])
        def start(self): pass
        def close(self): pass
        def run(self, *args, **kwargs):
            type(self).calls += 1
            (self.output/'profile.json').write_text('{"text":"unterminated', encoding='utf-8')
            if self.calls == 2: return SimpleNamespace(finish_reason='error', events=[])
            return SimpleNamespace(finish_reason='completed', events=[])
    config = Config(home=str(tmp_path/'runtime'))
    with pytest.raises(ModelServiceError): HarnessProducer(config, factory=Harness).run_stage(task, 'round1')
    with pytest.raises(SyntaxRepairExhausted): HarnessProducer(config, factory=Harness).run_stage(task, 'round1')
    assert Harness.calls == 4


def test_targeted_correction_replaces_stale_predecessor_output(tmp_path):
    config = Config(home=str(tmp_path/'runtime'), source_roots=[])
    store = Store(config.root/'state.sqlite3')
    source = tmp_path/'source.txt'; source.write_text('paper', encoding='utf-8')
    task = store.register_source(source, sha(source))
    class Producer:
        def run_stage(self, task, stage): return dict(draft_dir='NEW_REDACTION', disposition='keep')
    flow = Workflow(config, store, producer=Producer(), catalog={'majors': []})
    try:
        task.update(failed_stage='round3_redact', stage_outputs={'round3_redact':dict(directory='OLD_REDACTION'),
                    'round3_chunk':dict(directory='OLD_CHUNKS')})
        result = flow._execute('repair', task)
        assert result['stage'] == 'round3_chunk'
        assert result['stage_outputs']['round3_redact']['directory'] == 'NEW_REDACTION'
        assert 'round3_chunk' not in result['stage_outputs']
    finally:
        flow.close()


@pytest.mark.parametrize('placeholder',[None,''])
def test_text_field_alias_keeps_learning_content_instead_of_silently_dropping_it(package_factory,placeholder):
    root, task, catalog = package_factory()
    chunks = load(root/'chunks.json')
    chunks['chunks'].append(dict(id='m2',kind='learning',content=placeholder,text='先获取新材料，再分析指标之间的关系。'))
    save(root/'chunks.json', chunks); seal_package(root, task, 1)
    packet = validate_package(root, task, catalog)
    assert next(r for r in packet['chunks'] if r['id']=='m2')['content']=='先获取新材料，再分析指标之间的关系。'
    assert len(packet['chunks']) == 3


def test_wrong_library_receipt_is_rejected_before_any_external_write(package_factory):
    root, task, catalog = package_factory(); task['revision']=1
    packet = validate_package(root, task, catalog)
    client=MemoryRag(); publisher=RagPublisher(client)
    receipts={'120201':{'body':{'dataset_id':'wrong-library','name':'wrong','chunks':{}}}}
    with pytest.raises(RemoteConflict,match='target library'):
        publisher.publish(task,packet,receipts)
    assert client.docs == {} and client.writes == 0


def test_changed_mapping_holds_only_unmapped_target_without_model_repair(tmp_path,package_factory):
    config=Config(home=str(tmp_path/'runtime'),handoff_root=str(tmp_path/'handoff'),source_roots=[],retry_seconds=0,short_retry_interval=0)
    store=Store(config.root/'state.sqlite3')
    source=tmp_path/'source.txt';source.write_text('paper',encoding='utf-8')
    task=store.register_source(source,sha(source))
    root,_,catalog=package_factory(task_id=task['task_id'],source_sha=task['source_sha256'])
    set_routes(root,[dict(code='120201'),dict(code='120202')]);seal_package(root,task,1)
    catalog['majors'].append(dict(code='120202',name='市场营销',paper_dataset_id='other-ds',image_dataset_id='other-images'))
    task=store.update(task['task_id'],stage='ingest',package_dir=str(root),manifest_sha256=sha(root/'manifest.json'))
    class Producer:
        def run_stage(self,*args):pytest.fail('mapping faults must not invoke DeepSeek')
    flow=Workflow(config,store,producer=Producer(),publisher=RagPublisher(MemoryRag()),catalog=catalog)
    try:
        task=flow.step(task)
        catalog['majors'].pop()
        for _ in range(6):
            task=flow.step(task)
            if task['stage'] in ('parked','repair','code_error'):break
        assert task['stage']=='parked' and task['delivery_check']=='wrong_library'
        assert task['revision']==1 and task['repair_count']==0
        assert store.receipt(task['task_id'],1,'120201').get('verified')
    finally:
        flow.close()


@pytest.mark.parametrize('refs',[None,'s1',{'section':'s1'},[{'section':'s1'}]])
def test_auxiliary_reference_shapes_cannot_become_missing_file_errors(package_factory,refs):
    root,task,catalog=package_factory()
    chunks=load(root/'chunks.json');chunks['chunks'][1].update(section_ids=refs,asset_ids=refs)
    save(root/'chunks.json',chunks);seal_package(root,task,1)
    assert validate_package(root,task,catalog)['routes']


def test_old_sealed_asset_without_id_is_compatible_metadata(package_factory):
    root,task,catalog=package_factory()
    assets=load(root/'assets.json');assets['items'][0].pop('id');save(root/'assets.json',assets)
    manifest=load(root/'manifest.json')
    for row in manifest['files']:
        if row['path']=='assets.json':row.update(bytes=(root/'assets.json').stat().st_size,sha256=sha(root/'assets.json'))
    save(root/'manifest.json',manifest)
    ready=load(root/'READY.json');ready['manifest_sha256']=sha(root/'manifest.json');save(root/'READY.json',ready)
    assert validate_package(root,task,catalog)['assets'][0]['id']

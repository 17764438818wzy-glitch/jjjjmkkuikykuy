"""Approved A/B/C rules: real package/graph/ledger logic, local inputs only."""
from pathlib import Path

import pytest

from paperflow.config import Config
from paperflow.graph import Workflow
from paperflow.package import PackageError, load, normalize_delivery, save, seal_package, sha, validate_package
from paperflow.producer import delivery_correction_count, reserve_delivery_correction, SyntaxRepairExhausted
from paperflow.service import Service
from paperflow.store import Store


def test_two_corrections_are_cumulative_across_versions_and_error_stages(tmp_path):
    config=Config(home=str(tmp_path/'runtime'))
    task=dict(task_id='paper',source_sha256='a'*64,revision=1)
    assert reserve_delivery_correction(config,task,'round1')==1
    task.update(revision=2)
    assert reserve_delivery_correction(config,task,'round3_chunk')==2
    assert delivery_correction_count(Config(home=config.home),dict(task,revision=9))==2
    with pytest.raises(SyntaxRepairExhausted):reserve_delivery_correction(config,task,'publish')


def test_empty_chapter_tree_is_restored_from_existing_explicit_headings(package_factory):
    root,task,catalog=package_factory()
    save(root/'tree.json',dict(nodes=[]))
    normalize_delivery(root)
    assert load(root/'tree.json')['nodes'][0]['title']=='研究方法'
    seal_package(root,task,1)
    assert validate_package(root,task,catalog)['tree']['nodes']


def test_missing_tree_file_can_be_restored_without_another_model(package_factory):
    root,task,catalog=package_factory();(root/'tree.json').unlink()
    normalize_delivery(root)
    assert load(root/'tree.json')['nodes'][0]['title']=='研究方法'
    seal_package(root,task,1)
    assert validate_package(root,task,catalog)['routes']


@pytest.mark.parametrize('missing',['profile','tree'])
def test_completely_absent_required_content_without_recovery_is_blocked(package_factory,missing):
    root,task,catalog=package_factory()
    body='只有连续正文，没有任何明确章节标题。'
    (root/'article.redacted.md').write_text(body,encoding='utf-8')
    chunks=load(root/'chunks.json');chunks['chunks'][0]['content']=body;save(root/'chunks.json',chunks)
    if missing=='profile':
        profile=load(root/'profile.json')
        profile={k:v for k,v in profile.items() if k in ('task_id','source_id','source_sha256','routing_candidates','material_disposition')}
        save(root/'profile.json',profile)
    else:save(root/'tree.json',dict(nodes=[]))
    seal_package(root,task,1)
    with pytest.raises(PackageError,match='profile|chapter'):validate_package(root,task,catalog)


def test_explicit_nonchaptered_attachment_is_not_forced_into_a_paper_tree(package_factory):
    root,task,catalog=package_factory()
    profile=load(root/'profile.json');profile.update(material_type='数据附件',structure_applicable=False)
    save(root/'profile.json',profile);save(root/'tree.json',dict(nodes=[]))
    (root/'article.redacted.md').write_text('字段和数据使用说明。',encoding='utf-8')
    chunks=load(root/'chunks.json');chunks['chunks'][0]['content']='字段和数据使用说明。';save(root/'chunks.json',chunks)
    seal_package(root,task,1)
    assert validate_package(root,task,catalog)['tree']['nodes']==[]


def test_empty_description_reuses_only_the_same_assets_existing_explanation(package_factory):
    root,task,catalog=package_factory()
    assets=load(root/'assets.json');asset=assets['items'][0]
    asset.update(description='',note='展示指标之间的分解关系，需要重新取得财务数据。')
    save(root/'assets.json',assets);normalize_delivery(root)
    assert load(root/'assets.json')['items'][0]['description']==asset['note']
    seal_package(root,task,1);assert validate_package(root,task,catalog)['routes']


def test_description_with_no_existing_explanation_is_an_actual_missing_deliverable(package_factory):
    root,task,catalog=package_factory()
    assets=load(root/'assets.json');assets['items'][0].pop('description');save(root/'assets.json',assets)
    seal_package(root,task,1)
    with pytest.raises(PackageError,match='description'):validate_package(root,task,catalog)


def test_explicit_author_identifiers_are_masked_without_changing_public_contacts(package_factory):
    root,task,catalog=package_factory()
    profile=load(root/'profile.json')
    profile.update(author_phone='13800138000',student_id='202020201040',company_phone='13900139000',
                   references=[dict(author_phone='13700137000')],research_object='小米公司',
                   note='202020201040 和 public@example.com 只是研究记录')
    save(root/'profile.json',profile);normalize_delivery(root)
    result=load(root/'profile.json')
    assert result['author_phone']=='[已脱敏]' and result['student_id']=='[已脱敏]'
    assert result['company_phone']=='13900139000'
    assert result['references'][0]['author_phone']=='13700137000'
    assert result['note']==profile['note'] and result['research_object']=='小米公司'
    seal_package(root,task,1);assert validate_package(root,task,catalog)['routes']


def test_labeled_private_body_value_is_fixed_in_body_and_existing_slices(package_factory):
    root,task,catalog=package_factory()
    body='# 研究方法\n作者手机号：13800138000\n通过指标分解解释盈利能力。'
    (root/'article.redacted.md').write_text(body,encoding='utf-8')
    chunks=load(root/'chunks.json');chunks['chunks'][0]['content']=body;save(root/'chunks.json',chunks)
    normalize_delivery(root)
    assert '13800138000' not in (root/'article.redacted.md').read_text(encoding='utf-8')
    assert '13800138000' not in load(root/'chunks.json')['chunks'][0]['content']
    seal_package(root,task,1);assert validate_package(root,task,catalog)['routes']


def test_precisely_located_mapping_does_not_replace_a_same_named_reference(tmp_path,package_factory):
    root,task,catalog=package_factory()
    profile=load(root/'profile.json');profile.update(author=dict(name='张三'),references=[dict(name='张三')])
    save(root/'profile.json',profile)
    mapping=tmp_path/'private/redaction_map.json'
    save(mapping,dict(values=[dict(file='profile.json',json_path=['author','name'],original='张三',replacement='作者A',kind='author')]))
    task['redaction_map_private']=str(mapping)
    normalize_delivery(root,task=task)
    profile=load(root/'profile.json')
    assert profile['author']['name']=='作者A' and profile['references'][0]['name']=='张三'
    seal_package(root,task,1);assert validate_package(root,task,catalog)['routes']


def test_validate_does_not_rehash_or_rewrite_sealed_private_content(package_factory):
    root,task,catalog=package_factory()
    profile=load(root/'profile.json');profile['author_phone']='13800138000';save(root/'profile.json',profile)
    # Model a sealed package from the previous advisory-only rules.
    manifest=load(root/'manifest.json')
    for row in manifest['files']:
        if row['path']=='profile.json':row.update(bytes=(root/'profile.json').stat().st_size,sha256=sha(root/'profile.json'))
    save(root/'manifest.json',manifest)
    ready=load(root/'READY.json');ready['manifest_sha256']=sha(root/'manifest.json');save(root/'READY.json',ready)
    before={name:sha(root/name) for name in ('profile.json','manifest.json','READY.json')}
    with pytest.raises(PackageError) as error:validate_package(root,task,catalog)
    assert error.value.check=='privacy'
    assert '13800138000' not in str(error.value)
    assert all(sha(root/name)==value for name,value in before.items())


def test_existing_identity_conflict_is_not_overwritten_by_normalization(package_factory):
    root,task,_=package_factory()
    profile=load(root/'profile.json');profile['task_id']='different-paper';save(root/'profile.json',profile)
    before=sha(root/'profile.json')
    with pytest.raises(PackageError) as error:normalize_delivery(root,task=task)
    assert error.value.check=='identity'
    assert sha(root/'profile.json')==before


def test_partial_status_uses_current_revision_and_keeps_dispatch_stages(tmp_path):
    config=Config(home=str(tmp_path/'runtime'),handoff_root=str(tmp_path/'handoff'),source_roots=[])
    class IdleWorkflow:
        def close(self):pass
    service=Service(config,workflow_factory=lambda c,s:IdleWorkflow())
    try:
        task=service.store.register_source(tmp_path/'source','a'*64)
        service.store.update(task['task_id'],stage='parked',revision=2,route_codes=['120201','120202'],
                             pending_routes=[dict(code='unknown',reason='unknown specialty')])
        service.store.receipt(task['task_id'],1,'old',dict(verified=True,retrieval_verified=True,body=dict(document_id='old')))
        service.store.receipt(task['task_id'],2,'120201',dict(verified=True,body=dict(document_id='written')))
        service.store.receipt(task['task_id'],2,'120202',dict(body=dict(document_id='pending',chunks={'c1':dict(status='intent')})))
        status=service.status()
        assert status['tasks']=={'parked':1}
        assert status['display_counts']=={'partial_complete':1}
        progress=status['metrics']['partial'][0]
        assert progress['written_targets']==['120201'] and progress['searchable_targets']==[]
        assert set(progress['pending_targets'])=={'120202','unknown'}
        assert service.store.get(task['task_id'])['stage']=='parked'
        assert service.status()['metrics']['success_24h']==0
        # Exercise the actual status consumer without a window or a worker.
        from paperflow.ui import Workbench
        from types import SimpleNamespace
        import threading
        class Widget:
            def __init__(self):self.rows=[];self.value=''
            def get_children(self):return []
            def insert(self,*args,values):self.rows.append(values)
            def set(self,value):self.value=value
            def config(self,**kwargs):pass
        view=Workbench.__new__(Workbench)
        view.service=service;view.thread=None;view.stop_event=threading.Event()
        view.root=SimpleNamespace(after=lambda *args:None)
        for name in ('table','timings','metrics_text','exceptions','alert','message'):setattr(view,name,Widget())
        view.refresh()
        assert view.table.rows==[('部分完成',1)]
        assert '120201' in view.exceptions.value and '120202,unknown' in view.exceptions.value
        assert '可检索确认 待确认' in view.exceptions.value
        assert not service.running
    finally:service.close()


def set_body(root,text,split=None):
    (root/'article.redacted.md').write_text(text,encoding='utf-8')
    obj=load(root/'chunks.json')
    pieces=[text] if split is None else [text[:split],text[split:]]
    obj['chunks']=[dict(id='body'+str(i),kind='body',content=value) for i,value in enumerate(pieces)]
    save(root/'chunks.json',obj)


def test_generic_student_variables_and_fenced_examples_are_not_author_metadata(package_factory):
    root,task,catalog=package_factory()
    profile=load(root/'profile.json')
    profile.update(study_variables=dict(student_id='202020201040'),author_info=dict(student_id='202020201041'))
    save(root/'profile.json',profile)
    body='# 研究方法\n```text\n作者手机号：13800138000\n```\n学号：202020201040\n研究数据中的字段示例。'
    set_body(root,body);normalize_delivery(root)
    assert load(root/'profile.json')['study_variables']['student_id']=='202020201040'
    assert load(root/'profile.json')['author_info']['student_id']=='[已脱敏]'
    assert (root/'article.redacted.md').read_text(encoding='utf-8')==body
    seal_package(root,task,1);assert validate_package(root,task,catalog)['routes']


def test_annotated_author_contact_masks_the_identifier_without_losing_the_annotation(package_factory):
    root,task,catalog=package_factory()
    profile=load(root/'profile.json');profile['author_phone']='13800138000（微信同号）';save(root/'profile.json',profile)
    normalize_delivery(root)
    assert load(root/'profile.json')['author_phone']=='[已脱敏]（微信同号）'
    seal_package(root,task,1);assert validate_package(root,task,catalog)['routes']


def test_precise_mapping_takes_precedence_over_mechanical_placeholder(tmp_path,package_factory):
    root,task,catalog=package_factory()
    profile=load(root/'profile.json');profile['author_phone']='13800138000';save(root/'profile.json',profile)
    mapping=tmp_path/'map.json'
    save(mapping,dict(values=[dict(file='profile.json',json_path=['author_phone'],kind='phone',
                                 original='13800138000',replacement='作者电话已隐藏')]))
    task['redaction_map_private']=str(mapping);normalize_delivery(root,task=task)
    assert load(root/'profile.json')['author_phone']=='作者电话已隐藏'
    seal_package(root,task,1);assert validate_package(root,task,catalog)['routes']


def test_body_identity_edit_can_cross_a_chunk_boundary(package_factory):
    root,task,catalog=package_factory()
    body='# 研究方法\n作者手机号：13800138000\n后面的正常研究正文必须保留。'
    set_body(root,body,body.index('13800138000')+4);normalize_delivery(root)
    final=(root/'article.redacted.md').read_text(encoding='utf-8')
    assert '13800138000' not in final and final.endswith('后面的正常研究正文必须保留。')
    assert ''.join(r['content'] for r in load(root/'chunks.json')['chunks'])==final
    seal_package(root,task,1);assert validate_package(root,task,catalog)['routes']


def test_overlapping_body_mapping_never_cuts_off_normal_research_text(tmp_path,package_factory):
    root,task,catalog=package_factory()
    body='# 研究方法\n作者手机号：13800138000\n后面的正常研究正文必须完整保留。'
    set_body(root,body)
    mapping=tmp_path/'map.json'
    save(mapping,dict(values=[dict(file='article.redacted.md',line=2,kind='phone',
                                 original='作者手机号：13800138000',replacement='作者联系方式已处理')]))
    task['redaction_map_private']=str(mapping);normalize_delivery(root,task=task)
    final=(root/'article.redacted.md').read_text(encoding='utf-8')
    assert final.endswith('后面的正常研究正文必须完整保留。')
    assert ''.join(r['content'] for r in load(root/'chunks.json')['chunks'])==final
    seal_package(root,task,1);assert validate_package(root,task,catalog)['routes']


def test_conflicting_precise_mappings_remain_unmodified_and_blocked(tmp_path,package_factory):
    root,task,catalog=package_factory()
    profile=load(root/'profile.json');profile['author']=dict(name='张三');save(root/'profile.json',profile)
    mapping=tmp_path/'map.json'
    save(mapping,dict(values=[dict(file='profile.json',json_path=['author','name'],kind='author',
                                 original='张三',replacement=value) for value in ('作者A','作者B')]))
    task['redaction_map_private']=str(mapping);normalize_delivery(root,task=task)
    assert load(root/'profile.json')['author']['name']=='张三'
    seal_package(root,task,1)
    with pytest.raises(PackageError) as error:validate_package(root,task,catalog)
    assert error.value.check=='privacy'


def test_sealed_mutation_is_parked_before_resealing_or_model_correction(tmp_path,package_factory):
    config=Config(home=str(tmp_path/'runtime'),handoff_root=str(tmp_path/'handoff'),source_roots=[])
    store=Store(config.root/'state.sqlite3')
    source=tmp_path/'source.txt';source.write_text('paper',encoding='utf-8');task=store.register_source(source,sha(source))
    root,_,catalog=package_factory(task_id=task['task_id'],source_sha=task['source_sha256'])
    profile=load(root/'profile.json');profile['article']['title']='变更后的稿件';save(root/'profile.json',profile)
    before=sha(root/'manifest.json')
    class NoModel:
        def run_stage(self,*args):pytest.fail('sealed identity conflicts do not invoke a model')
    flow=Workflow(config,store,producer=NoModel(),catalog=catalog)
    try:
        task=store.update(task['task_id'],stage='publish',draft_dir=str(root))
        for _ in range(3):
            task=flow.step(task)
            if task['stage']=='parked':break
        assert task['stage']=='parked' and task['delivery_check']=='identity'
        assert task['revision']==1 and task['correction_count']==0
        assert sha(root/'manifest.json')==before
        assert not (Path(config.handoff_root)/'Updated manuscript').exists()
    finally:flow.close()


def test_top_level_package_identity_mismatch_is_not_a_missing_file(package_factory):
    root,task,catalog=package_factory()
    with pytest.raises(PackageError) as error:validate_package(root,dict(task,task_id='another'),catalog)
    assert error.value.check=='identity'


def test_code_fence_context_is_preserved_across_body_chunks(package_factory):
    root,task,catalog=package_factory()
    body='# 研究方法\n```text\n作者手机号：13800138000\n```\n程序字段示例。'
    set_body(root,body,body.index('作者手机号'))
    normalize_delivery(root)
    assert (root/'article.redacted.md').read_text(encoding='utf-8')==body
    assert ''.join(r['content'] for r in load(root/'chunks.json')['chunks'])==body
    seal_package(root,task,1);assert validate_package(root,task,catalog)['routes']


@pytest.mark.parametrize('fault',['missing_ready','unreadable_manifest','invalid_ready_shape'])
def test_broken_existing_seal_is_an_identity_problem(package_factory,fault):
    root,task,catalog=package_factory()
    if fault=='missing_ready':(root/'READY.json').unlink()
    elif fault=='unreadable_manifest':(root/'manifest.json').write_text('{truncated',encoding='utf-8')
    else:save(root/'READY.json',[])
    with pytest.raises(PackageError) as error:validate_package(root,task,catalog)
    assert error.value.check=='identity'

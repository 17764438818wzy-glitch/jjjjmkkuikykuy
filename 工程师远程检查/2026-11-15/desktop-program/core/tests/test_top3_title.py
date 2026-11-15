"""Scoped contract acceptance; isolated files, SQLite, IPC and RAG substitutes."""
import copy
import json
from pathlib import Path

import pytest

from paperflow.config import Config
from paperflow.handoff import PackageHandoff
from paperflow.package import load, save, seal_package, sha, synchronize_routes, validate_package
from paperflow.producer import HarnessProducer
from paperflow.rag import RagPublisher, RemoteConflict
from paperflow.store import Store
from test_rag import MemoryRag
from test_processing_graph import graph_case, run_worker, model_calls
from test_delivery_state import delivery_case
from test_control import manager


def routes(count):
    # Deliberately not specialty-code order: this order is the model's relevance decision.
    return [dict(code=code, name=name, reason='正文中的研究方法适用') for code, name in
            [('120703T', '质量管理工程'), ('120701', '工业工程'), ('080201', '机械工程'),
             ('080801', '自动化'), ('080905', '物联网工程'), ('120702T', '标准化工程'),
             ('082702', '食品质量与安全'), ('080910T', '数据科学与大数据技术')]][:count]


def extend_catalog(catalog):
    return dict(majors=catalog['majors'] + [dict(row, paper_dataset_id='paper-'+row['code'],
                image_dataset_id='image-'+row['code']) for row in routes(8)])


def test_final_profile_keeps_non_delivery_specialties_in_search_payload(package_factory):
    root, task, catalog = package_factory(); task['revision'] = 1
    correction=load(root/'corrections.json')
    correction.update(final_routing_candidates=routes(3), additional_related_specialties=routes(8)[3:])
    save(root/'corrections.json', correction); synchronize_routes(root)
    profile=load(root/'profile.json')
    assert profile['routing_candidates'] == routes(3)
    assert profile['additional_related_specialties'] == routes(8)[3:]
    seal_package(root, task, 1)
    packet=validate_package(root, task, extend_catalog(catalog), strict_seal=True)
    assert {r['code'] for r in packet['routes']} == {r['code'] for r in routes(3)}
    text=next(content for rid, content, _ in RagPublisher(None).records(packet)['body'] if rid=='__profile')
    assert all(row['name'] in text for row in routes(8)[3:])
    from paperflow.native_delivery import NativeDeliveryPublisher
    assert '食品质量与安全' in NativeDeliveryPublisher(None, None, None).document_bytes(task, packet, 'body', {}).decode()


@pytest.mark.parametrize('count', [0, 1, 2, 3])
def test_no_padding_and_model_order_is_preserved(package_factory, count):
    from paperflow.package import validate_routing_selection
    root, _, catalog=package_factory()
    profile=load(root/'profile.json');profile['routing_candidates']=routes(count)
    validate_routing_selection(profile, extend_catalog(catalog))
    assert profile['routing_candidates'] == routes(count)


def test_round2_rejects_four_and_does_not_silently_truncate(tmp_path, package_factory):
    source=tmp_path/'original.txt';source.write_text('原稿')
    root, task, catalog=package_factory(source_sha=sha(source));task.update(source_path=str(source),revision=1)
    correction=load(root/'corrections.json');correction['final_routing_candidates']=routes(4)
    save(root/'corrections.json',correction)
    config=Config(home=str(tmp_path/'runtime'),catalog_path=str(tmp_path/'catalog.json'))
    save(config.catalog_path,extend_catalog(catalog))
    with pytest.raises(ValueError,match='at most 3'):
        HarnessProducer(config)._finish_output(task,'round2',root,[])
    assert len(load(root/'corrections.json')['final_routing_candidates'])==4


def test_new_handoff_rejects_unmarked_legacy_overflow_but_old_package_is_readable(tmp_path,package_factory):
    root,task,catalog=package_factory();task['revision']=1;catalog=extend_catalog(catalog)
    profile=load(root/'profile.json');profile['routing_candidates']=routes(8);save(root/'profile.json',profile)
    seal_package(root,task,1)
    packet=validate_package(root,task,catalog,strict_seal=True)
    assert len(packet['routes'])==8  # Historical readback remains compatible.
    handoff=PackageHandoff(Store(tmp_path/'ledger.sqlite3'),tmp_path/'handoff')
    with pytest.raises(ValueError,match='at most 3'):
        handoff.prepare('new',task,root,catalog,mapping_version='catalog-v1')
    assert not handoff.ledger.deliveries(packet['manifest_sha256'])


def test_preexisting_eight_target_intent_can_resume_unchanged(tmp_path,package_factory,monkeypatch):
    from paperflow.handoff_store import canonical
    root,task,catalog=package_factory();task['revision']=1;catalog=extend_catalog(catalog)
    profile=load(root/'profile.json');profile['routing_candidates']=routes(8);save(root/'profile.json',profile)
    seal_package(root,task,1);original=sha(root/'manifest.json')
    h=PackageHandoff(Store(tmp_path/'ledger.sqlite3'),tmp_path/'handoff')
    # Seed an actual persisted legacy intent; do not permit a new overflow in production.
    prepare=h.ledger.prepare
    def legacy(data):
        h.ledger._write(lambda c:c.execute('INSERT INTO publication_intents(thread_id,input_fingerprint,task_id,data) VALUES(?,?,?,?)',
            (data['thread_id'],data['input_fingerprint'],task['task_id'],canonical(data))))
        return h.ledger.intent(data['thread_id'],data['input_fingerprint'])
    monkeypatch.setattr(h.ledger,'prepare',legacy)
    intent=h.prepare('old-run',task,root,catalog,mapping_version='catalog-v1')
    monkeypatch.setattr(h.ledger,'prepare',prepare)
    assert h.prepare('old-run',task,root,catalog,mapping_version='catalog-v1')==intent
    result=h.resume('old-run',intent['input_fingerprint'])
    assert result['manifest_sha256']==original
    assert len(h.ledger.deliveries(original))==8


def named_packet(package_factory):
    root,task,catalog=package_factory();task['revision']=1
    profile=load(root/'profile.json')
    profile['article'].update(title='论文的真实题目',public_file_name='论文的真实题目.docx',document_name_policy='paper-title/1')
    save(root/'profile.json',profile);seal_package(root,task,1)
    return task,validate_package(root,task,catalog,strict_seal=True)


def test_rag_uses_title_while_retry_keeps_one_document(package_factory):
    task,packet=named_packet(package_factory)
    client=MemoryRag();publisher=RagPublisher(client);receipts={}
    publisher.publish(task,packet,receipts)
    assert next(iter(client.docs.values()))['name'].startswith('论文的真实题目__')
    assert receipts['120201']['body']['upload_name'].startswith('PF_')
    assert receipts['120201']['body']['name']==next(iter(client.docs.values()))['name']
    publisher.publish(task,packet,receipts)
    assert len(client.docs)==1
    publisher.verify(task,packet,receipts)


def test_lost_rename_response_reuses_persisted_id(package_factory):
    task,packet=named_packet(package_factory)
    class Lost(MemoryRag):
        lose=True
        def request(self,method,endpoint,body=None,content_type=None):
            result=super().request(method,endpoint,body,content_type)
            if method=='PATCH' and body.get('name') and self.lose:
                self.lose=False;raise TimeoutError('name written, response lost')
            return result
    client=Lost();publisher=RagPublisher(client);receipts={}
    with pytest.raises(TimeoutError):publisher.publish(task,packet,receipts)
    assert receipts['120201']['body']['document_id']
    publisher.publish(task,packet,receipts)
    assert len(client.docs)==1
    assert next(iter(client.docs.values()))['name'].startswith('论文的真实题目__')


def test_same_title_versions_have_distinct_names_and_retired_receipt_still_matches(package_factory):
    from paperflow.maintenance import verify_retired_object
    task,packet=named_packet(package_factory)
    client=MemoryRag();publisher=RagPublisher(client);receipts={}
    publisher.publish(task,packet,receipts)
    old=copy.deepcopy(receipts['120201']['body'])
    # A new manifest of the same paper/title must never collide with the old public name.
    newer=dict(packet,manifest_sha256='f'*64)
    publisher.publish(task,newer,{})
    assert len(client.docs)==2 and len({d['name'] for d in client.docs.values()})==2
    doc=client.docs[(old['dataset_id'],old['document_id'])]
    assert doc['name']==old['name']
    verify_retired_object(publisher,task,1,'120201','body',old,[doc])


def test_inventory_recognizes_renamed_document_and_ignores_unrelated_same_title(tmp_path,package_factory):
    from types import SimpleNamespace
    from contextlib import nullcontext
    from paperflow.delivery import DeliveryWorkflow
    task,packet=named_packet(package_factory)
    client=MemoryRag();publisher=RagPublisher(client);receipts={}
    publisher.publish(task,packet,receipts)
    client.version=lambda *args,**kwargs:nullcontext()
    doc=copy.deepcopy(next(iter(client.docs.values())))
    doc.update(id='unrelated',meta_fields={'paperflow_task':'another-paper'})
    client.docs[('paper-ds','unrelated')]=doc
    subject=object.__new__(DeliveryWorkflow)
    subject.digest=packet['manifest_sha256'];subject.code='120201';subject.identity=task
    subject.ledger=SimpleNamespace(unresolved=lambda *args:[],switching=lambda *args:None,active=lambda *args:None)
    subject.client=client;subject.publisher=publisher;subject.packet=lambda digest:(task,packet)
    inventory=subject.inventory()
    assert [d['id'] for item in inventory for d in item['documents']]==[receipts['120201']['body']['document_id']]


def test_first_round_cache_stays_reusable_but_changed_round2_prompt_invalidates_it(tmp_path,package_factory,monkeypatch):
    import paperflow.phase_cache as phase
    import shutil
    root,task,catalog=package_factory();task.update(source_path=str(tmp_path/'source.docx'))
    config=Config(home=str(tmp_path/'runtime'),catalog_path=str(tmp_path/'catalog.json'))
    save(config.catalog_path,catalog)
    current=phase.PROMPTS
    baseline=tmp_path/'old-prompts';shutil.copytree(current,baseline)
    (baseline/'round2.original.txt').write_text('旧约定：适合多个专业就全部列出，不设固定数量上限。',encoding='utf-8')
    monkeypatch.setattr(phase,'PROMPTS',baseline)
    before={stage:phase.input_fingerprint(config,task,stage) for stage in ('round1','round2')}
    monkeypatch.setattr(phase,'PROMPTS',current)
    after={stage:phase.input_fingerprint(config,task,stage) for stage in ('round1','round2')}
    assert before['round1']==after['round1']
    assert before['round2']!=after['round2']


def test_duplicate_selected_or_overlap_in_extra_list_is_rejected():
    from paperflow.package import validate_routing_selection
    with pytest.raises(ValueError,match='duplicate'):
        validate_routing_selection(dict(routing_candidates=[routes(1)[0],routes(1)[0]]))
    with pytest.raises(ValueError,match='non-delivery'):
        validate_routing_selection(dict(routing_candidates=routes(1),additional_related_specialties=routes(1)))


def test_round1_title_projects_to_ledger_including_cache_resume(graph_case):
    c,s,t,m,_=graph_case
    profile=load(c.root/'fixture-material/profile.json')
    profile['article'].update(title='按论文题目显示',public_file_name='旧名.txt')
    save(c.root/'fixture-material/profile.json',profile)
    s.update(t['task_id'],display_name='上传的原始名字.txt')
    # Qualified round1 cache exists, but graph checkpoint has not accepted its result yet.
    run_worker(graph_case,'after-cache-round1')
    run_worker(graph_case)
    current=s.get(t['task_id'])
    assert current['display_name']=='按论文题目显示.txt'
    assert current['original_display_name']=='上传的原始名字.txt'
    assert current['source_path']==t['source_path'] and current['source_sha256']==t['source_sha256']
    assert Path(t['source_path']).read_text(encoding='utf-8')=='原稿保留'
    assert model_calls(c).count('round1')==1
    package=Path(m.ledger.rows('SELECT package_dir FROM delivery_packages')[0]['package_dir'])
    assert load(package/'profile.json')['article']['public_file_name']=='按论文题目显示.txt'


def test_real_graph_registers_only_three_and_preserves_extra_names(graph_case):
    c,s,t,m,_=graph_case
    save(c.catalog_path,extend_catalog(c.catalog))
    p=c.root/'fixture-material/corrections.json';correction=load(p)
    correction.update(final_routing_candidates=routes(3),additional_related_specialties=routes(8)[3:])
    save(p,correction)
    run_worker(graph_case)
    assert load(c.root/'worker-result.json')['stage']=='handed_off'
    assert len(m.ledger.rows('SELECT * FROM specialty_tasks'))==3
    package=Path(m.ledger.rows('SELECT package_dir FROM delivery_packages')[0]['package_dir'])
    profile=load(package/'profile.json')
    assert [row['code'] for row in profile['routing_candidates']]==[row['code'] for row in routes(3)]
    assert profile['additional_related_specialties']==routes(8)[3:]
    assert not m.ledger.rows("SELECT * FROM control_actions WHERE kind LIKE 'rag_%'")


def test_old_checkpoint_keeps_original_name_when_only_round2_cache_is_invalidated(graph_case):
    from paperflow.processing_graph import ProcessingWorkflow
    c,s,t,m,_=graph_case
    p=c.root/'fixture-material/profile.json';profile=load(p);profile['article']['title']='新的显示题目';save(p,profile)
    s.update(t['task_id'],display_name='旧上传名字.txt')
    run_worker(graph_case,'after-cache-round3_chunk')
    flow=ProcessingWorkflow(c,s,object())
    try:
        config=flow.graph_config(t);legacy=dict(flow.graph.get_state(config).values['task'])
        legacy.pop('original_display_name',None);legacy.pop('article_title',None);legacy['display_name']='旧上传名字.txt'
        flow.graph.update_state(config,dict(task=legacy),as_node='control')
    finally:flow.close()
    s.update(t['task_id'],display_name='旧上传名字.txt',drop_fields=('original_display_name','article_title'))
    next((c.root/'work'/t['task_id']/'phase-cache/round2').glob('*.json')).unlink()
    run_worker(graph_case)
    current=s.get(t['task_id'])
    assert current['display_name']=='新的显示题目.txt'
    assert current['original_display_name']=='旧上传名字.txt'
    assert model_calls(c).count('round1')==1 and model_calls(c).count('round2')==2


def test_native_pipeline_preserves_new_names_extra_profile_and_controlled_readback(delivery_case,tmp_path):
    from test_native_delivery import NativeServer,native_flow
    from test_delivery_flow import advance
    m,c,t,d,commits,material,catalog=delivery_case
    profile=load(material/'profile.json')
    profile['article'].update(title='真实标题接线验收',document_name_policy='paper-title/1',public_file_name='真实标题接线验收.txt')
    profile['additional_related_specialties']=routes(1)
    save(material/'profile.json',profile)
    server=NativeServer();workflow=native_flow(advance(delivery_case),tmp_path,server)
    assert workflow.run()['state']=='active'
    assert len(server.ingests)==1
    doc=server.docs[('paper-ds',server.ingests[0])]
    assert doc['name'].startswith('真实标题接线验收新版__')
    assert '质量管理工程' in doc['meta_fields']['two_pass_profile_json']
    assert any('质量管理工程' in row['content'] for row in server.chunks[doc['id']])
    receipt=workflow.receipts['120201']['body']
    assert receipt['upload_name'].startswith('PF_') and receipt['name']==doc['name']
    assert m.ledger.rows("SELECT * FROM control_actions WHERE kind='rag_write'")

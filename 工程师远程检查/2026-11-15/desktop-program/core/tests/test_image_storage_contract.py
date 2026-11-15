"""Prevent explicit model image selections from disappearing before upload."""
import copy
import hashlib
from pathlib import Path

import pytest
from PIL import Image

from paperflow.config import Config
from paperflow.package import PackageError,load,save,sha,seal_package,validate_package,normalize_asset_storage
from paperflow.producer import HarnessProducer,ProductionError
from paperflow.rag import RagPublisher,RemoteConflict
from test_rag import MemoryRag


def image_draft(tmp_path,package_factory,*,wrapper='type',redactions=None):
    source=tmp_path/'source.txt';source.write_text('original source')
    draft,task,catalog=package_factory(source_sha=sha(source));task.update(source_path=str(source),revision=1)
    view=tmp_path/'source-view';(view/'images').mkdir(parents=True)
    raw=view/'images/original.png';Image.new('RGB',(12,12),'white').save(raw)
    save(view/'source-index.json',dict(images=[dict(id='original-image',file='images/original.png',sha256=sha(raw))]))
    selected=dict(file='assets/selected.png',source_image_id='original-image',content='关键图的结构和方法说明。')
    if redactions:selected['redactions']=redactions
    selected[wrapper]='image'
    assets=load(draft/'assets.json')
    assets['items']=[dict(id='fig1',kind='image',section_id='s1',body_anchor='s1:figure',storage=selected),
        dict(id='desc1',kind='table',section_id='s1',storage={wrapper:'description','content':'文字即可说明的关系。'}),
        dict(id='omit1',kind='image',storage={wrapper:'omit','reason':'装饰图，不保留。'})]
    save(draft/'assets.json',assets)
    config=Config(home=str(tmp_path/'runtime'),catalog_path=str(tmp_path/'catalog.json'),source_roots=[])
    save(config.catalog_path,catalog);task['reading_dir']=str(view)
    return config,task,draft,catalog,raw


@pytest.mark.parametrize('wrapper',['type','mode'])
def test_real_producer_materializes_selected_image_and_preserves_text_decisions(tmp_path,package_factory,wrapper):
    c,task,draft,catalog,raw=image_draft(tmp_path,package_factory,wrapper=wrapper)
    before=(draft/'article.redacted.md').read_bytes()
    HarnessProducer(c)._finish_output(task,'round3_assets',draft,[])
    assets=load(draft/'assets.json')['items']
    assert [a['storage'] for a in assets]==['image','description','omit']
    assert assets[0]['description']=='关键图的结构和方法说明。'
    assert assets[1]['description']=='文字即可说明的关系。'
    assert assets[0]['section_id']=='s1' and assets[0]['body_anchor']=='s1:figure'
    assert assets[0]['source_image_id']=='original-image'
    assert (draft/'assets/selected.png').read_bytes()==raw.read_bytes()
    assert assets[0]['sha256']==sha(raw)
    normalize_asset_storage(draft);once=(draft/'assets.json').read_bytes();normalize_asset_storage(draft)
    assert (draft/'assets.json').read_bytes()==once
    seal_package(draft,task,1);packet=validate_package(draft,task,catalog,strict_seal=True)
    assert 'assets/selected.png' in packet['manifest']['publication_files']
    records=RagPublisher(None).records(packet)
    assert [r[0] for r in records['images']]==['asset_fig1']
    assert 'asset_desc1' in {r[0] for r in records['body']} and 'asset_omit1' not in {r[0] for r in records['body']}
    assert (draft/'article.redacted.md').read_bytes()==before


def test_nested_redactions_are_applied_before_image_hash_and_readback(tmp_path,package_factory):
    c,task,draft,catalog,raw=image_draft(tmp_path,package_factory,redactions=[dict(x0=0,y0=0,x1=4,y1=4)])
    HarnessProducer(c)._finish_output(task,'round3_redact',draft,[])
    target=draft/'assets/selected.png'
    with Image.open(target) as image:
        assert image.getpixel((1,1))==(0,0,0) and image.getpixel((8,8))==(255,255,255)
    assert sha(target)!=sha(raw)
    assert load(draft/'assets.json')['items'][0]['sha256']==sha(target)


def test_missing_selected_original_is_not_silently_treated_as_text(tmp_path,package_factory):
    c,task,draft,_,raw=image_draft(tmp_path,package_factory)
    raw.unlink()
    with pytest.raises((PackageError,OSError),match='image|system|系统'):
        HarnessProducer(c)._finish_output(task,'round3_assets',draft,[])


def test_sealed_legacy_image_declaration_must_not_verify_as_text(package_factory):
    root,task,catalog=package_factory();task['revision']=1
    # This exact accepted historical shape carries no materialized image.
    assets=load(root/'assets.json');assets['items'][0]['storage']={'type':'image','file':'assets/missing.png','source_image_id':'original-image'}
    save(root/'assets.json',assets)
    manifest=load(root/'manifest.json')
    for entry in manifest['files']:
        if entry['path']=='assets.json':entry.update(bytes=(root/'assets.json').stat().st_size,sha256=sha(root/'assets.json'))
    save(root/'manifest.json',manifest);ready=load(root/'READY.json');ready['manifest_sha256']=sha(root/'manifest.json');save(root/'READY.json',ready)
    before={p.name:sha(p) for p in (root/'manifest.json',root/'READY.json',root/'assets.json')}
    with pytest.raises(PackageError,match='storage|materializ'):
        validate_package(root,task,catalog,strict_seal=True)
    assert {p.name:sha(p) for p in (root/'manifest.json',root/'READY.json',root/'assets.json')}==before


def test_publisher_rejects_unmaterialized_image_plan_before_any_remote_write(package_factory):
    root,task,catalog=package_factory();task['revision']=1
    packet=validate_package(root,task,catalog)
    packet['assets'][0]['storage']={'type':'image','file':'assets/missing.png'}
    client=MemoryRag()
    with pytest.raises(PackageError,match='storage|materializ'):
        RagPublisher(client).publish(task,packet,{})
    assert client.writes==0 and not client.docs


def test_required_image_readback_rejects_missing_receipt_or_wrong_bytes(tmp_path,package_factory):
    c,task,draft,catalog,raw=image_draft(tmp_path,package_factory)
    HarnessProducer(c)._finish_output(task,'round3_assets',draft,[]);seal_package(draft,task,1)
    packet=validate_package(draft,task,catalog,strict_seal=True)
    client=MemoryRag();client.download_image=lambda image_id:raw.read_bytes()
    publisher=RagPublisher(client);receipts={};publisher.publish(task,packet,receipts);publisher.verify(task,packet,receipts)
    body=receipts['120201']['body'];image=receipts['120201']['images']
    fields=client.docs[(body['dataset_id'],body['document_id'])]['meta_fields']
    import json
    assert json.loads(fields['paperflow_image_associations_json'])==image
    assert image['chunks']['asset_fig1']['image_id']=='stored-image'
    missing=copy.deepcopy(receipts);missing['120201'].pop('images')
    with pytest.raises(RemoteConflict):publisher.verify(task,packet,missing)
    client.download_image=lambda image_id:b'wrong bytes'
    with pytest.raises(RemoteConflict,match='image bytes'):publisher.verify(task,packet,receipts)


def test_asset_protocol_invalidates_only_third_round_cache(tmp_path,monkeypatch):
    import paperflow.phase_cache as cache
    c=Config(home=str(tmp_path/'runtime'),catalog_path=str(tmp_path/'catalog.json'));save(c.catalog_path,{'majors':[]})
    source=tmp_path/'paper.txt';source.write_text('source')
    task=dict(task_id='t',source_id='s',source_sha256=sha(source),source_path=str(source),draft_dir=str(tmp_path/'draft'))
    stages=('round1','round2','round3_assets','round3_redact','round3_chunk')
    monkeypatch.setattr(cache,'ASSET_STORAGE_PROTOCOL','legacy',raising=False)
    old={s:cache.input_fingerprint(c,task,s) for s in stages}
    monkeypatch.setattr(cache,'ASSET_STORAGE_PROTOCOL','type-content-fixed')
    current={s:cache.input_fingerprint(c,task,s) for s in stages}
    assert [s for s in stages if old[s]!=current[s]]==['round3_assets','round3_redact','round3_chunk']


def test_empty_mode_does_not_hide_explicit_type_image(tmp_path,package_factory):
    c,task,draft,catalog,raw=image_draft(tmp_path,package_factory)
    obj=load(draft/'assets.json');obj['items'][0]['storage']['mode']=None;save(draft/'assets.json',obj)
    HarnessProducer(c)._finish_output(task,'round3_assets',draft,[])
    assert load(draft/'assets.json')['items'][0]['storage']=='image'
    assert (draft/'assets/selected.png').read_bytes()==raw.read_bytes()


@pytest.mark.parametrize('field,outer',[('redactions',[]),('source_image_id','wrong-image'),('file','assets/wrong.png')])
def test_conflicting_image_instruction_is_rejected_not_hidden(tmp_path,package_factory,field,outer):
    c,task,draft,_,_=image_draft(tmp_path,package_factory,redactions=[dict(x0=0,y0=0,x1=4,y1=4)])
    obj=load(draft/'assets.json');obj['items'][0][field]=outer;save(draft/'assets.json',obj)
    with pytest.raises(PackageError,match='conflict'):
        normalize_asset_storage(draft)


def test_historical_hidden_redaction_conflict_is_rejected_by_publisher(package_factory):
    root,task,catalog=package_factory();packet=validate_package(root,task,catalog)
    packet['assets'][0].update(storage='image',redactions=[],storage_details={'redactions':[dict(x0=0,y0=0,x1=4,y1=4)]})
    with pytest.raises(PackageError,match='conflict'):
        RagPublisher(None).records(packet)


def test_image_delivery_handles_deployment_missing_public_name_response(tmp_path,package_factory):
    from urllib.parse import urlparse,parse_qs
    c,task,draft,catalog,raw=image_draft(tmp_path,package_factory)
    profile=load(draft/'profile.json');profile['article']['document_name_policy']='paper-title/1';save(draft/'profile.json',profile)
    HarnessProducer(c)._finish_output(task,'round3_assets',draft,[]);seal_package(draft,task,1)
    packet=validate_package(draft,task,catalog,strict_seal=True)
    class DeploymentRag(MemoryRag):
        def request(self,method,endpoint,body=None,content_type=None):
            value=super().request(method,endpoint,body,content_type)
            params=parse_qs(urlparse(endpoint).query)
            if method=='GET' and params.get('name') and not value['docs']:
                raise RuntimeError("RAGFlow：you don't own the document "+params['name'][0])
            return value
    client=DeploymentRag();client.download_image=lambda image_id:raw.read_bytes();publisher=RagPublisher(client);receipts={}
    publisher.publish(task,packet,receipts);publisher.verify(task,packet,receipts)
    assert len(client.docs)==2

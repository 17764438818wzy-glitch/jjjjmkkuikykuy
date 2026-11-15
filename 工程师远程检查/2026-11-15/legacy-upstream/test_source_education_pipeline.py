"""Exercise existing offline HTTP fixture through real task/package boundaries."""
import copy
import json
from pathlib import Path

import pytest
from docx import Document

from auto_pipeline import AutoProcessor
from engine import Store, digest
from test_stage_resume import CONFIG, KEY, Transport, document, response


def run_cover(tmp_path, reject=False):
    source=tmp_path/'paper.docx';document(source)
    doc=Document(source);first=doc.add_paragraph('成人本科毕业论文');doc._element.body.insert(0,first._p);doc.save(source)
    cfg=copy.deepcopy(CONFIG);store=Store(tmp_path/'data');processor=AutoProcessor(store,cfg,KEY)
    processor.agent.opener=Transport(lambda message: {'decision':'reject','reason':'synthetic retry'}
                                    if reject and message['task']=='privacy_text' else response(message['task']))
    result=processor.run([{'path':str(source),'name':source.name}])
    return store,cfg,result


def fields(node):
    assert 'source_education' in node, 'missing source education at persistence boundary'
    from source_education import source_education_fields
    actual={key:node[key] for key in ('source_education','source_education_revision')}
    assert actual==source_education_fields(actual['source_education'])
    return actual


def handoff(text):
    return json.loads(text.split('[PAPER_PIPELINE_METADATA_V1]\n',1)[1].split('\n[/PAPER_PIPELINE_METADATA_V1]',1)[0])


def test_fresh_task_checkpoint_package_and_both_handoffs_match(tmp_path):
    store,cfg,result=run_cover(tmp_path)
    item=result['items'][0];assert item['status']=='pass',item
    expected=fields(item);assert expected['source_education']['education_level']['value']=='bachelor'
    cp=json.loads(Path(item['prepared_checkpoint']['path']).read_text('utf8'))
    assert fields(cp)==fields(cp['preparation']['identity'])==fields(cp['preparation']['meta'])==expected
    package=Path(item['package_path']);meta=json.loads((package/'metadata.json').read_text('utf8'))
    assert fields(meta)==fields(handoff((package/'ingest.md').read_text('utf8')))==expected
    from evidence import ragflow_handoff_header
    assert fields(handoff(ragflow_handoff_header(meta)))==expected
    assert fields(handoff((package/'ragflow_assets.md').read_text('utf8')))==expected


@pytest.mark.parametrize('stale_native',[False,True])
def test_legacy_checkpoint_adds_source_metadata_in_new_immutable_snapshot(tmp_path,monkeypatch,stale_native):
    store,cfg,result=run_cover(tmp_path,reject=True)
    item=result['items'][0];cp=item['prepared_checkpoint'];path=Path(cp['path'])
    saved=json.loads(path.read_text('utf8'))
    for node in (saved,saved['preparation']['identity'],saved['preparation']['meta']):
        node.pop('source_education',None);node.pop('source_education_revision',None)
    if stale_native:saved.pop('native_rules_version',None)
    path.write_text(json.dumps(saved,ensure_ascii=False),encoding='utf8');cp['sha256']=digest(path)
    before={str(p):p.read_bytes() for p in path.parent.rglob('*') if p.is_file()}
    if not stale_native:
        import evidence,auto_pipeline
        def forbidden(*args,**kwargs):raise AssertionError('metadata migration reprocessed figures')
        monkeypatch.setattr(evidence,'build_package',forbidden);monkeypatch.setattr(auto_pipeline,'build_package',forbidden)
    processor=AutoProcessor(store,cfg,KEY);processor.agent.opener=Transport()
    resumed=processor.run([{'path':item['source_path'],'name':'paper.docx','prepared_checkpoint':cp}],operation='retry_ai')
    updated=resumed['items'][0];assert updated['status']=='pass',updated
    expected=fields(updated)
    assert updated['prepared_checkpoint']['path']!=str(path)
    new=json.loads(Path(updated['prepared_checkpoint']['path']).read_text('utf8'))
    assert fields(new)==fields(new['preparation']['identity'])==fields(new['preparation']['meta'])==expected
    assert before=={str(p):p.read_bytes() for p in path.parent.rglob('*') if p.is_file()}


@pytest.mark.parametrize('extension,relation',[('docx','normalized'),('doc','converted')])
def test_derived_input_preserves_original_identity_at_all_boundaries(tmp_path,monkeypatch,extension,relation):
    import office_input,auto_pipeline
    original=tmp_path/('paper.'+extension);original.write_bytes(b'synthetic original binary')
    converted=tmp_path/'converted.docx';document(converted)
    doc=Document(converted);p=doc.add_paragraph('函授本科毕业论文');doc._element.body.insert(0,p._p);doc.save(converted)
    if relation=='normalized':monkeypatch.setattr(office_input,'normalize_docx_container',lambda source, output:converted)
    else:monkeypatch.setattr(auto_pipeline,'convert_office',lambda source, output:converted)
    processor=AutoProcessor(Store(tmp_path/'data'),copy.deepcopy(CONFIG),KEY);processor.agent.opener=Transport()
    result=processor.run([{'path':str(original),'name':original.name}]);item=result['items'][0]
    assert item['status']=='pass',item
    value=fields(item)['source_education']
    assert value['source_relation']==relation
    assert value['original_source_sha256']==digest(original)
    assert value['evidence_source_sha256']==digest(converted)
    assert value['derivation']['input_sha256']==digest(original)
    assert value['derivation']['output_sha256']==digest(converted)
    cp=json.loads(Path(item['prepared_checkpoint']['path']).read_text('utf8'))
    assert fields(cp)==fields(item)


def test_conflicting_source_degrees_do_not_reject_whole_paper(tmp_path):
    source=tmp_path/'paper.docx';document(source)
    doc=Document(source)
    for line in reversed(['本科毕业论文','学历：硕士']):
        p=doc.add_paragraph(line);doc._element.body.insert(0,p._p)
    doc.save(source);processor=AutoProcessor(Store(tmp_path/'data'),copy.deepcopy(CONFIG),KEY);processor.agent.opener=Transport()
    item=processor.run([{'path':str(source),'name':source.name}])['items'][0]
    assert item['status']=='pass',item
    assert fields(item)['source_education']['education_level']['status']=='conflict'


def test_full_source_evidence_inventory_fits_bounded_native_handoff(tmp_path):
    from source_education import extract_source_education,source_education_fields
    from test_knowledge_package import sample
    from evidence import export_package
    from knowledge_package import knowledge_files
    source=tmp_path/'cover.docx';doc=Document()
    for line in ['   '.join('非全日制硕士研究生学位论文')]*32+['摘要']:doc.add_paragraph(line)
    doc.save(source)
    payload=extract_source_education(source)
    assert len(payload['evidence'])==1, 'duplicate normalized identity evidence is merged'
    # The frozen handoff schema still permits 32 exact receipts even though the
    # deterministic extractor now selects bounded representative evidence.
    first=payload['evidence'][0];payload['evidence']=[]
    for index in range(1,33):
        row=copy.deepcopy(first);row['id']='ev-'+str(index)
        row['locator']['paragraph_index']=index;payload['evidence'].append(row)
    for field in ('education_level','study_mode'):
        payload[field]['evidence_ids']=[row['id'] for row in payload['evidence']]
    from test_source_education import check
    check(payload)
    meta=sample(tmp_path);meta.update(source_education_fields(payload))
    meta.update(original_source_sha256=digest(source),source_sha256=digest(source))
    export_package(tmp_path,meta,canonical_paper_id=True)
    text=(tmp_path/'ingest.md').read_text('utf8')
    header=text.split('[PAPER_PIPELINE_METADATA_V1]',1)[1].split('[/PAPER_PIPELINE_METADATA_V1]',1)[0]
    assert len(header)>8192
    assert 'ingest.md' in knowledge_files(tmp_path,require_stable_paper_id=True)


def test_crc_recovery_after_conversion_retains_conversion_relation(tmp_path,monkeypatch):
    import auto_pipeline,office_integrity,shutil,zipfile
    original=tmp_path/'paper.doc';original.write_bytes(b'synthetic legacy Word binary')
    converted=tmp_path/'converted.docx';document(converted)
    doc=Document(converted);p=doc.add_paragraph('本科毕业论文');doc._element.body.insert(0,p._p);doc.save(converted)
    monkeypatch.setattr(auto_pipeline,'convert_office',lambda source,output:converted)
    real_build=auto_pipeline.build_package;calls=[]
    def build(source,*args,**kwargs):
        calls.append(source)
        if len(calls)==1:raise zipfile.BadZipFile('synthetic media CRC failure')
        return real_build(source,*args,**kwargs)
    def recover(source,target):
        shutil.copy2(source,target);return ['word/media/synthetic.png']
    monkeypatch.setattr(auto_pipeline,'build_package',build)
    monkeypatch.setattr(office_integrity,'recover_zero_crc_media',recover)
    processor=AutoProcessor(Store(tmp_path/'data'),copy.deepcopy(CONFIG),KEY);processor.agent.opener=Transport()
    item=processor.run([{'path':str(original),'name':original.name}])['items'][0]
    assert item['status']=='pass',item
    value=fields(item)['source_education']
    assert value['source_relation']=='converted'
    assert value['original_source_sha256']==digest(original)
    assert value['derivation']['method']=='convert_office+recover_zero_crc_media'

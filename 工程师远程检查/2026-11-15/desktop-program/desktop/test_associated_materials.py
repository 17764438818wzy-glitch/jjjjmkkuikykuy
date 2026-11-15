"""Real isolated files and batch runner; external model transport is fake only."""
import json
from pathlib import Path
import zipfile

import pytest

from test_preextract_workbench_flow import flow


def record(path):
    return dict(path=str(path), name=path.name)


def test_source_and_unreadable_drawing_keep_originals_without_model(flow, tmp_path):
    from engine import digest
    from auto_view import outcome, batch_summary
    from processing_outcomes import ai_outcome, execution_outcome
    from script_task import task_snapshot, task_rows
    processor, opener, _ = flow
    code=tmp_path/'controller.py'
    marker=tmp_path/'MUST_NOT_EXIST'
    code.write_text(f'from pathlib import Path\nPath({str(marker)!r}).touch()\n', encoding='utf8')
    drawing=tmp_path/'assembly.dwg';drawing.write_bytes(b'AC1032\x00unreadable native design')
    processor.config['concurrency']['papers']=2
    result=processor.run([record(code),record(drawing)])
    assert len(result['items'])==2
    assert not opener.calls and not marker.exists()
    assert {i['status'] for i in result['items']}=={'associated','pending'}
    assert not result.get('human_view',{}).get('issues')
    for item in result['items']:
        info=item['associated_material']
        assert digest(Path(info['original_path']))==item['source_hash']==digest(Path(item['source_path']))
        assert info['remote_original_uploaded'] is False
        assert item['automatic_approval'] is False and not item['uploads']
        assert ai_outcome(item,result['status'])[0]=='未进入 AI'
        assert outcome(item,result['status'])[0] in {'关联资料','待解析'}
        assert execution_outcome(item,result)[0] in {'已保存','待解析'}
    restored=task_snapshot(processor.store,processor.store.load(result['run_id']))
    assert {r['script_status'] for r in task_rows(restored)}=={'已保存','待解析'}
    assert '待解析 1' in batch_summary(result) and '关联资料 1' in batch_summary(result)


def test_design_explanation_uses_current_flow_and_associated_inventory(flow, tmp_path):
    from engine import digest
    processor,opener,sources=flow
    code=tmp_path/'controller.py';code.write_text('def schedule():\n    return 42\n',encoding='utf8')
    drawing=tmp_path/'assembly.dwg';drawing.write_bytes(b'AC1032\0native')
    def design(candidate):candidate['profile']['material_type']='设计说明书'
    opener.transform=design
    result=processor.run([sources[0],record(code),record(drawing)])
    paper=next(i for i in result['items'] if i.get('content_format')=='paper_preextract')
    assert paper['status']=='pass',paper.get('reason')
    assert len(opener.calls)==1
    material=json.loads(opener.calls[0]['messages'][1]['content'][0]['text'])['material']
    assert len(material['attachments'])==2
    encoded=json.dumps(material['attachments'])
    assert str(tmp_path) not in encoded and 'original_path' not in encoded
    bundle=json.loads((Path(paper['preextract_work_dir'])/'material.json').read_text(encoding='utf8'))
    assert bundle['associated_materials_sha256']
    for a in bundle['attachments']:
        assert digest(Path(a['original_path']))==a['sha256']
        assert a['association_status']=='candidate'
    profile=json.loads((Path(paper['package_path'])/'profile.json').read_text(encoding='utf8'))
    assert len(profile['attachments'])==2
    assert all(a['decision']=='uncertain' for a in profile['attachments'])
    public='\n'.join(p.read_text(encoding='utf8') for p in Path(paper['package_path']).rglob('*') if p.is_file() and p.suffix in {'.md','.json','.jsonl'})
    assert str(tmp_path) not in public and 'controller.py' not in public


def test_ambiguous_projects_never_guess_a_primary(flow, tmp_path):
    processor,opener,sources=flow
    code=tmp_path/'shared.py';code.write_text('x=1',encoding='utf8')
    result=processor.run([*sources,record(code)])
    item=next(i for i in result['items'] if i.get('content_format')=='associated_material')
    assert item['associated_material']['association_status']=='unassigned'
    assert not item['associated_material']['related_record_keys']
    assert all(not json.loads(c['messages'][1]['content'][0]['text'])['material']['attachments'] for c in opener.calls)


def test_same_source_bytes_in_two_project_paths_keep_both_relations(flow,tmp_path):
    processor,opener,_=flow
    records=[]
    for name in ('projectA','projectB'):
        folder=tmp_path/name;folder.mkdir();p=folder/'main.c';p.write_text('int main(){return 0;}',encoding='utf8');records.append(record(p))
    processor.config['concurrency']['papers']=2
    result=processor.run(records)
    assert len(result['items'])==2 and not opener.calls
    assert {i['associated_material']['source_key'] for i in result['items']}=={str(Path(r['path'])).replace('\\','/').casefold() for r in records}


def test_archive_preserves_project_members_and_pending_state(flow,tmp_path):
    processor,opener,sources=flow
    archive=tmp_path/'design.zip'
    with zipfile.ZipFile(archive,'w') as z:
        z.write(sources[0]['path'],'project/设计说明书.docx')
        z.writestr('project/src/main.py','def schedule(): return 1')
        z.writestr('project/drawings/assembly.dwg',b'AC1032\0native')
    result=processor.run([record(archive)])
    assert len(result['items'])==3,[i.get('reason') for i in result['items']]
    assert len(opener.calls)==1
    assert {i['status'] for i in result['items']}=={'pass','associated','pending'}
    paper=next(i for i in result['items'] if i['status']=='pass')
    bundle=json.loads((Path(paper['preextract_work_dir'])/'material.json').read_text(encoding='utf8'))
    assert len(bundle['attachments'])==2
    assert all(a['archive_member_path'].startswith('project/') for a in bundle['attachments'])
    assert all(a['association_status']=='candidate' for a in bundle['attachments'])


def test_loose_explanation_and_selected_archive_keep_the_project_together(flow,tmp_path):
    processor,opener,sources=flow
    archive=tmp_path/'drawings.zip'
    with zipfile.ZipFile(archive,'w') as z:
        z.writestr('drawing.dwg',b'AC1032\0native')
        z.writestr('src/main.py','x=1')
    result=processor.run([sources[0],record(archive)])
    paper=next(i for i in result['items'] if i['status']=='pass')
    bundle=json.loads((Path(paper['preextract_work_dir'])/'material.json').read_text(encoding='utf8'))
    assert len(bundle['attachments'])==1
    container=bundle['attachments'][0]
    assert container['kind']=='archive' and container['association_status']=='candidate'
    assert len(container['members'])==2
    assert {m['parse_status'] for m in container['members']}=={'pending','readable'}
    assert all(Path(m['original_path']).is_file() for m in container['members'])
    assert all(m['related_record_keys'] for m in container['members'])
    assert len(opener.calls)==1


def test_identical_archives_keep_both_project_links_in_parallel(flow,tmp_path):
    import io
    processor,opener,sources=flow
    payload=io.BytesIO()
    with zipfile.ZipFile(payload,'w') as z:z.writestr('src/main.py','x=1')
    selected=[]
    for index,name in enumerate(('A','B')):
        folder=tmp_path/name;folder.mkdir()
        doc=folder/'explanation.docx';doc.write_bytes(Path(sources[index]['path']).read_bytes())
        archive=folder/'source.zip';archive.write_bytes(payload.getvalue())
        selected.extend([record(doc),record(archive)])
    processor.config['concurrency']['papers']=2
    result=processor.run(selected)
    saved=[i['associated_material'] for i in result['items'] if i.get('associated_material',{}).get('kind')=='archive']
    assert len(saved)==2
    assert len({s['source_key'] for s in saved})==2
    assert all(s['members'][0]['related_record_keys']==s['related_record_keys'] for s in saved)


def test_complete_project_beside_loose_explanation_still_processes_its_document(flow,tmp_path):
    processor,opener,sources=flow
    archive=tmp_path/'other-project.zip'
    with zipfile.ZipFile(archive,'w') as z:
        z.write(sources[1]['path'],'project/explanation.docx')
        z.writestr('project/main.py','x=1')
    result=processor.run([sources[0],record(archive)])
    assert len([i for i in result['items'] if i['status']=='pass'])==2
    assert len(opener.calls)==2


def test_interrupted_container_registration_can_resume_without_overwrite(tmp_path,monkeypatch):
    import associated_materials as module
    import threading
    archive=tmp_path/'source.zip'
    with zipfile.ZipFile(archive,'w') as z:
        z.writestr('a.py','a=1');z.writestr('b.py','b=2')
    stop=threading.Event();original=module.preserve_material
    def interrupt(record,destination,records,**kwargs):
        value=original(record,destination,records,**kwargs)
        if Path(record['path']).suffix=='.py':stop.set()
        return value
    monkeypatch.setattr(module,'preserve_material',interrupt)
    with pytest.raises(InterruptedError):original(record(archive),tmp_path/'private',[],stop=stop)
    stop.clear();monkeypatch.setattr(module,'preserve_material',original)
    resumed=original(record(archive),tmp_path/'private',[],stop=stop)
    assert len(resumed['members'])==2 and resumed['parse_status']=='readable'


def test_existing_material_binding_never_claims_new_attachment_read(flow,tmp_path):
    processor,opener,sources=flow
    first=processor.run(sources[:1]);paper=first['items'][0]
    before=(Path(paper['preextract_work_dir'])/'material.json').read_bytes()
    code=tmp_path/'new.py';code.write_text('x=2',encoding='utf8')
    record_with_checkpoint=dict(sources[0],prepared_checkpoint=paper['prepared_checkpoint'])
    second=processor.run([record_with_checkpoint,record(code)])
    saved=next(i for i in second['items'] if i.get('content_format')=='paper_preextract')
    assert saved['status']=='pass',saved.get('reason')
    assert len(opener.calls)==1
    assert (Path(saved['preextract_work_dir'])/'material.json').read_bytes()==before
    assert saved['associated_inventory_binding']=='historical'


def test_large_source_preview_is_bounded_but_original_complete(tmp_path):
    from associated_materials import preserve_material,PREVIEW_BYTES
    source=tmp_path/'large.py';source.write_bytes(b'x=1\n'*(PREVIEW_BYTES//4+20))
    info=preserve_material(record(source),tmp_path/'private',[])
    assert Path(info['original_path']).read_bytes()==source.read_bytes()
    assert info['preview_truncated'] is True
    assert Path(info['preview_path']).stat().st_size<=PREVIEW_BYTES+4


def test_corrupt_document_is_pending_without_a_quality_rejection(flow,tmp_path):
    processor,opener,_=flow
    source=tmp_path/'broken-design.docx';source.write_bytes(b'not a readable docx')
    result=processor.run([record(source)]);item=result['items'][0]
    assert item['status']=='pending',item.get('reason')
    assert not opener.calls and not item['automatic_approval']
    assert Path(item['associated_material']['original_path']).read_bytes()==source.read_bytes()


def test_selected_context_does_not_enumerate_unselected_neighbors(flow,tmp_path):
    processor,opener,sources=flow
    neighbor=tmp_path/'secret.py';neighbor.write_text('SECRET=42',encoding='utf8')
    result=processor.run(sources[:1])
    material=json.loads(opener.calls[0]['messages'][1]['content'][0]['text'])['material']
    assert not material['attachments'] and result['items'][0]['status']=='pass'


def test_archive_traversal_remains_blocked_and_empty_archive_pending(flow,tmp_path):
    processor,opener,_=flow
    bad=tmp_path/'bad.zip';empty=tmp_path/'empty.zip'
    with zipfile.ZipFile(bad,'w') as z:z.writestr('../outside.py','raise RuntimeError()')
    with zipfile.ZipFile(empty,'w'):pass
    result=processor.run([record(bad),record(empty)])
    assert {i['status'] for i in result['items']}=={'reject','pending'}
    assert not opener.calls and not (tmp_path/'outside.py').exists()


def test_dxf_static_preview_retained_with_native_reading_limits(tmp_path):
    ezdxf=pytest.importorskip('ezdxf')
    from associated_materials import preserve_material
    source=tmp_path/'drawing.dxf';drawing=ezdxf.new();drawing.modelspace().add_line((0,0),(10,5));drawing.saveas(source)
    info=preserve_material(record(source),tmp_path/'private',[])
    assert Path(info['original_path']).read_bytes()==source.read_bytes()
    assert info['parse_status']=='readable' and Path(info['preview_path']).is_file()
    assert any('未核实' in limit for limit in info['limits'])


def test_preview_reader_limit_retains_image_as_pending(tmp_path,monkeypatch):
    from PIL import Image
    from associated_materials import preserve_material
    source=tmp_path/'drawing.png';Image.new('RGB',(100,100),'white').save(source)
    monkeypatch.setattr(Image,'MAX_IMAGE_PIXELS',10)
    info=preserve_material(record(source),tmp_path/'private',[])
    assert info['parse_status']=='pending'
    assert Path(info['original_path']).read_bytes()==source.read_bytes()


def test_twenty_thousand_selected_inputs_use_one_context_index(tmp_path,monkeypatch):
    import associated_materials as module
    docs=[record(tmp_path/'project'/'设计说明书.docx')]
    records=docs+[record(tmp_path/'project'/'src'/f'unit-{i}.py') for i in range(19999)]
    real=module.material_kind;calls=[]
    def observe(r):calls.append(1);return real(r)
    monkeypatch.setattr(module,'material_kind',observe)
    context=module.MaterialContext(records)
    assert len(module.related_records(docs[0],context))==19999
    for r in records[1::100]:assert module.association(r,context)['association_status']=='candidate'
    assert len(calls)<40000


def test_repeated_retention_has_valid_hashes_without_self_hash(tmp_path):
    from associated_materials import preserve_material
    from engine import digest
    source=tmp_path/'code.py';source.write_text('x=1',encoding='utf8')
    for _ in range(2):
        info=preserve_material(record(source),tmp_path/'private',[])
        assert all(digest(Path(p))==sha for p,sha in info['retained_file_hashes'].items())
        assert all(Path(p).name!='associated-material.json' for p in info['retained_file_hashes'])


def test_model_decision_keeps_program_unread_limits_and_bounded_inventory(tmp_path):
    from associated_materials import preserve_material,add_inventory_decisions,reading_inventory,inventory_summary,METADATA_LIMIT
    source=tmp_path/'main.py';source.write_text('x=1',encoding='utf8')
    info=preserve_material(record(source),tmp_path/'private',[])
    candidate={'attachments':[dict(file_ref=info['file_ref'],decision='keep',description='原回答说明',limits=[])]}
    add_inventory_decisions(candidate,[info])
    assert candidate['attachments'][0]['decision']=='keep'
    assert candidate['attachments'][0]['description']=='原回答说明'
    assert all(limit in candidate['attachments'][0]['limits'] for limit in info['limits'])
    rows=[dict(info,file_ref=f'associated_{i}') for i in range(1000)]
    assert len(reading_inventory(rows))==METADATA_LIMIT
    assert inventory_summary(rows)['retained_count']==1000
    public=add_inventory_decisions({'attachments':[]},rows)['attachments']
    assert len(public)==METADATA_LIMIT+1
    assert public[-1]['file_ref'] is None and '936' in public[-1]['description']


def test_saved_inventory_change_cannot_reuse_an_old_model_answer(flow,tmp_path):
    from associated_materials import inventory_hash
    processor,opener,sources=flow
    code=tmp_path/'main.py';code.write_text('x=1',encoding='utf8')
    result=processor.run([sources[0],record(code)])
    paper=next(i for i in result['items'] if i['status']=='pass')
    material=Path(paper['preextract_work_dir'])/'material.json'
    bundle=json.loads(material.read_text(encoding='utf8'))
    bundle['attachments'][0]['limits'].append('changed private inventory')
    # Even a self-consistent new material inventory cannot borrow the old answer.
    bundle['associated_materials_sha256']=inventory_hash(bundle['attachments'])
    material.write_text(json.dumps(bundle,ensure_ascii=False),encoding='utf8')
    later=processor.run([sources[0],record(code)])
    retry=next(i for i in later['items'] if i.get('content_format')=='paper_preextract')
    assert retry['status']=='reject' and len(opener.calls)==1
    assert '不同关联资料版本' in retry['reason']


def test_attachment_reference_cannot_borrow_main_document_coordinates():
    from test_preextract_contract import fixture
    from preextract_contract import validate_candidate
    candidate,bundle,catalog=fixture()
    bundle['attachments']=[dict(file_ref='associated_file')]
    ref=dict(candidate['profile']['learning_units'][0]['source_refs'][0],file_ref='associated_file')
    candidate['profile']['learning_units'][0]['source_refs']=[ref]
    result=validate_candidate(candidate,bundle,catalog,redaction_policy='best_effort',admission_policy='learning')
    assert not result['valid']
    assert any('attachment reference' in e['message'] for e in result['errors'])


def test_same_body_in_two_projects_preserves_both_project_references(flow,tmp_path):
    processor,opener,sources=flow
    records=[]
    for name in ('A','B'):
        folder=tmp_path/name;folder.mkdir()
        doc=folder/'explanation.docx';doc.write_bytes(Path(sources[0]['path']).read_bytes());records.append(record(doc))
        code=folder/'main.py';code.write_text('x='+repr(name),encoding='utf8');records.append(record(code))
    processor.config['concurrency']['papers']=2
    result=processor.run(records)
    assert len(opener.calls)==1
    primary=next(i for i in result['items'] if i['status']=='pass')
    duplicate=next(e for e in result['events'] if e.get('status')=='duplicate')
    relation=duplicate['project_reference']
    assert relation['status']=='linked' and relation['reused_item_id']==primary['id']
    assert relation['source_record_key'].endswith('/b/explanation.docx')
    assert len([i for i in result['items'] if i['status']=='associated'])==2


def test_nested_archives_retain_project_prefixes_and_prepare_slots(flow,tmp_path,monkeypatch):
    import io
    import auto_pipeline
    processor,opener,sources=flow
    outer=tmp_path/'outer.zip'
    with zipfile.ZipFile(outer,'w') as output:
        for index,name in enumerate(('A','B')):
            payload=io.BytesIO()
            with zipfile.ZipFile(payload,'w') as inner:
                inner.writestr('design/explanation.docx',Path(sources[index]['path']).read_bytes())
                inner.writestr('design/src/main.py','x='+repr(name))
            output.writestr(name+'.zip',payload.getvalue())
    other=tmp_path/'second.py';other.write_text('other=1',encoding='utf8')
    processor.config['concurrency']['papers']=2
    original=auto_pipeline.process_associated
    def check_prepare(proc,*args,**kwargs):
        enter=proc._preextract_enter_prepare;calls=[]
        def observe():calls.append(True);return enter()
        proc._preextract_enter_prepare=observe
        try:
            result=original(proc,*args,**kwargs)
            assert calls,'associated file must reclaim original preparation slot'
            return result
        finally:proc._preextract_enter_prepare=enter
    monkeypatch.setattr(auto_pipeline,'process_associated',check_prepare)
    result=processor.run([record(outer),record(other)])
    papers=[i for i in result['items'] if i['status']=='pass']
    assert len(papers)==2,[i.get('reason') for i in result['items']]
    associated=[i['associated_material'] for i in result['items'] if i.get('associated_material') and i.get('archive_member_path')]
    assert len({i['source_key'] for i in associated})==2
    assert {i['archive_member_path'] for i in associated}=={'A.zip/design/src/main.py','B.zip/design/src/main.py'}
    for item in papers:
        bundle=json.loads((Path(item['preextract_work_dir'])/'material.json').read_text(encoding='utf8'))
        assert len(bundle['attachments'])==1
        assert bundle['attachments'][0]['archive_member_path'].startswith(item['archive_member_path'].split('/')[0]+'/')


def test_desktop_can_filter_saved_and_pending_and_show_private_location(flow,tmp_path,monkeypatch):
    import tkinter as tk
    from app import Workbench
    processor,opener,_=flow
    code=tmp_path/'main.py';code.write_text('x=1',encoding='utf8')
    cad=tmp_path/'drawing.dwg';cad.write_bytes(b'AC1032\0native')
    result=processor.run([record(code),record(cad)])
    root=tk.Tk();root.withdraw()
    app=Workbench(root,processor.store.root,recover=False)
    try:
        app.show_script_results(result)
        assert {'已保存','待解析'} <= set(app.script_filter_combo.cget('values'))
        app.script_filter.set('待解析')
        visible=app.source_tree.get_children()
        assert len(visible)==1
        app.source_tree.selection_set(visible[0])
        previews=[]
        monkeypatch.setattr(app,'text_window',lambda title,text:previews.append(text))
        app.preview_script_row()
        assert '私有原件：' in previews[0] and '源代码从未执行' in previews[0]
        assert '待解析' in app.script_summary.get() and '已保存' in app.script_summary.get()
    finally:
        panel=getattr(app,'_model_activity_panel',None)
        if panel is not None:panel.destroy()
        for timer in root.tk.call('after','info'):root.after_cancel(timer)
        root.destroy()

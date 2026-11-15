import json
from pathlib import Path
from types import SimpleNamespace
import zipfile

import pytest

from paperflow.attachments import AttachmentLedger, attachment_note, prepare_package
from paperflow.config import Config
from paperflow.intake import EXTENSIONS
from paperflow.package import load, save, sha
from paperflow.reading import prepare_reading
from paperflow.store import Store
from test_workbench import web_case
from test_managed_entry import entry_case


@pytest.mark.parametrize('name,kind', [('figure.png','image'),('data.xlsx','spreadsheet'),('paper.docx.json','data')])
def test_materials_take_profile_only_path_and_retain_exact_original_bytes(tmp_path,name,kind):
    from paperflow.producer import HarnessProducer
    from paperflow.attachments import is_candidate
    source=tmp_path/name;source.write_bytes(b'original attachment bytes')
    assert source.suffix in EXTENSIONS and is_candidate(source)
    c=Config(home=str(tmp_path/'runtime'));store=Store(tmp_path/'state.sqlite3')
    task=store.register_source(source,sha(source));draft=c.root/'work'/task['task_id'];draft.mkdir(parents=True)
    save(draft/'profile.json',dict(material_disposition='keep',attachment=dict(kind=kind,description='配套资料，原件保留，正文关联待确认。')))
    result=HarnessProducer(c)._finish_output(task,'round1',draft,[])
    assert result['attachment']['kind']==kind
    task['draft_dir']=str(draft);root=prepare_package(c,task)
    assert (root/('original'+source.suffix)).read_bytes()==source.read_bytes()
    assert not (root/'tree.json').exists()


def test_picture_attachment_has_a_real_readable_image_for_the_model(tmp_path):
    from PIL import Image
    source=tmp_path/'figure.png';Image.new('RGB',(7,5),'white').save(source)
    original=source.read_bytes();out=tmp_path/'reading';result=prepare_reading(source,out)
    assert result['input_kind']=='attachment_candidate' and len(result['images'])==1
    image=result['images'][0];assert image['readable_image'] is True
    assert (out/image['file']).read_bytes()==original
    assert result['source_sha256']==sha(source)


def test_spreadsheet_lists_real_sheet_names_without_claiming_cell_parsing(tmp_path):
    source=tmp_path/'data.xlsx'
    with zipfile.ZipFile(source,'w') as z:
        z.writestr('xl/workbook.xml','<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheets><sheet name="Survey data" sheetId="1"/></sheets></workbook>')
    before=source.read_bytes();out=tmp_path/'reading';result=prepare_reading(source,out)
    assert result['input_kind']=='attachment_candidate'
    text=(out/'source.txt').read_text(encoding='utf-8')
    assert 'Survey data' in text and '未解析单元格' in text
    assert source.read_bytes()==before


def test_json_companion_is_read_as_data_and_never_executed(tmp_path):
    source=tmp_path/'paper.docx.json';source.write_text('{"caption":"Original survey"}',encoding='utf-8')
    out=tmp_path/'reading';result=prepare_reading(source,out)
    assert result['input_kind']=='attachment_candidate'
    assert 'Original survey' in (out/'source.txt').read_text(encoding='utf-8')


@pytest.mark.parametrize('decision',['exclude','uncertain'])
def test_agent_can_exclude_or_hold_material_without_publishing_or_deleting_original(tmp_path,decision):
    from paperflow.producer import HarnessProducer
    c=Config(home=str(tmp_path/'runtime'));source=tmp_path/'extra.json';source.write_text('{"notice":"no study information"}')
    before=source.read_bytes();task=Store(tmp_path/'state.sqlite3').register_source(source,sha(source))
    draft=c.root/'work'/task['task_id'];draft.mkdir(parents=True)
    save(draft/'profile.json',dict(material_disposition=dict(decision=decision,reason='Agent actual-content decision')))
    result=HarnessProducer(c)._finish_output(task,'round1',draft,[])
    assert result['disposition']==decision and not result.get('attachment')
    assert not (c.root/'retained-attachments').exists()
    assert source.read_bytes()==before


def test_material_reuses_only_an_identical_already_configured_common_archive(tmp_path):
    from paperflow.attachments import attachment_target
    from paperflow.attachment_delivery import storage_acceptance
    target={'parent_id':'approved-folder','dataset_id':'approved-materials'}
    targets={'drawing':target,'software':dict(target)}
    assert attachment_target(targets,'image')==('drawing',target)
    assert attachment_target({'drawing':target,'software':{'parent_id':'other','dataset_id':'other'}},'image')==(None,{})
    assert attachment_target({'drawing':target},'data')==(None,{})
    report=tmp_path/'accepted.json'
    save(report,dict(passed=True,endpoint='http://127.0.0.1:19991/api/v1',identity={'dataset_ids':['approved-materials']},attachment_storage=dict(passed=True,targets=targets)))
    cfg=SimpleNamespace(delivery_acceptance_report=report,rag_url='http://127.0.0.1:19991/api/v1',attachment_targets=targets)
    assert storage_acceptance(cfg,'spreadsheet')['passed'] is True
    cfg.attachment_targets={'drawing':target}
    with pytest.raises(ValueError):storage_acceptance(cfg,'image')


def test_explicit_continue_reopens_newly_supported_skips_without_touching_ready_sources(web_case):
    from test_workbench import login, command
    from test_workbench_streaming import begin
    w,client,_=web_case;headers=login(client)
    entries=[dict(id='paper',name='paper.txt',relative_path='folder/paper.txt',bytes=5),
             dict(id='image',name='figure.png',relative_path='folder/figure.png',bytes=8),
             dict(id='unknown',name='unknown.nope',relative_path='folder/unknown.nope',bytes=2)]
    batch=begin(w,client,headers,entries)['batch_id']
    with w.store.db() as db:
        original=dict(db.execute("select * from web_intake_items where batch_id=? and item_id='paper'",(batch,)).fetchone())
        db.execute("update web_intake_items set state='skipped',error='格式暂不支持' where batch_id=? and item_id='image'",(batch,))
    command(client,headers,'/api/pause')
    from test_winjob import wait_until
    wait_until(lambda:w.manager.status()['state']=='paused' and not w.manager.status()['live_units'])
    continued=command(client,headers,'/api/continue')
    assert continued.status_code==200,continued.text
    assert continued.json()['reopened_items']==['image']
    with w.store.db() as db:
        rows={r['item_id']:dict(r) for r in db.execute('select * from web_intake_items where batch_id=?',(batch,))}
    assert rows['paper']==original
    assert rows['image']['state']=='pending' and rows['image']['error']==''
    assert rows['unknown']['state']=='skipped'

import json
from pathlib import Path
import sys
from types import SimpleNamespace
import zipfile

import pytest

from paperflow.config import Config
from paperflow.intake import Intake
from paperflow.package import load, save, sha
from paperflow.reading import prepare_reading
from paperflow.store import Store


def test_software_zip_exposes_member_inventory_without_executing_or_dropping_code(tmp_path):
    source=tmp_path/'project.zip'
    with zipfile.ZipFile(source,'w') as z:
        z.writestr('project/main.py','raise RuntimeError("must never execute")')
        z.writestr('project/design.dwg',b'AC1032\x00original-drawing')
    before=source.read_bytes()
    result=prepare_reading(source,tmp_path/'view')
    assert result.get('input_kind')=='attachment_candidate'
    assert {r['path'] for r in result['archive_members']}=={'project/main.py','project/design.dwg'}
    assert 'main.py' in (tmp_path/'view/source.txt').read_text(encoding='utf-8')
    assert source.read_bytes()==before
    assert not list((tmp_path/'view').rglob('main.py'))


def test_dwg_is_preserved_as_binary_without_office_conversion(tmp_path,monkeypatch):
    source=tmp_path/'drawing.dwg';source.write_bytes(b'AC1032\x00\xff\x00original')
    def forbidden(*args):pytest.fail('A drawing must not enter the Office converter')
    monkeypatch.setitem(sys.modules,'engine',SimpleNamespace(convert_office=forbidden))
    result=prepare_reading(source,tmp_path/'view')
    assert result.get('input_kind')=='attachment_candidate'
    assert result['source_sha256']==sha(source)
    assert result['images']==[]
    assert '未解析' in (tmp_path/'view/source.txt').read_text(encoding='utf-8')


def test_intake_keeps_individual_paper_drawing_and_code_tasks(tmp_path):
    root=tmp_path/'sources';root.mkdir()
    for name in ('paper.txt','drawing.dwg','main.py'):(root/name).write_bytes(b'original '+name.encode())
    c=Config(source_roots=[str(root)],stable_seconds=0)
    s=Store(tmp_path/'state.sqlite3');i=Intake(c,s)
    i.scan(now=0);i.scan(now=1)
    assert {Path(t['source_path']).name for t in s.all_tasks()}=={'paper.txt','drawing.dwg','main.py'}
    assert len({t['task_id'] for t in s.all_tasks()})==3


def test_paper_zip_keeps_readable_body_and_adds_inventory(tmp_path):
    source=tmp_path/'paper.zip'
    with zipfile.ZipFile(source,'w') as z:z.writestr('paper.md','# Research\nPaper body remains readable.')
    result=prepare_reading(source,tmp_path/'view')
    assert 'Paper body remains readable.' in (tmp_path/'view/source.txt').read_text(encoding='utf-8')
    assert result.get('archive_members')==[{'path':'paper.md','bytes':len(b'# Research\nPaper body remains readable.')}]


def test_attachment_description_retains_uncertain_title_without_fabricating_association():
    from paperflow.attachments import attachment_note
    note=attachment_note({'attachment':{'kind':'drawing','description':'减速器装配图','related_paper_title':None}})
    assert note['kind']=='drawing'
    assert note['description']=='减速器装配图'
    assert note['related_paper_title'] is None
    assert note['association_status']=='pending'
    assert attachment_note({'article':{'title':'普通论文'}}) is None


def test_round1_attachment_needs_only_real_profile_and_can_be_cached(tmp_path):
    from paperflow.producer import HarnessProducer
    from paperflow.phase_cache import input_fingerprint,prepare_cache,verify_entry
    c=Config(home=str(tmp_path/'runtime'),catalog_path=str(tmp_path/'catalog.json'))
    save(c.catalog_path,{'majors':[]})
    source=tmp_path/'drawing.dwg';source.write_bytes(b'AC1032\x00original')
    task=Store(tmp_path/'state.sqlite3').register_source(source,sha(source))
    out=c.root/'work'/task['task_id']/'attempts'/'one'/'round1';out.mkdir(parents=True)
    save(out/'profile.json',dict(task_id=task['task_id'],source_id=task['source_id'],source_sha256=task['source_sha256'],
        material_disposition='keep',attachment={'kind':'drawing','description':'装配图原件，未解析CAD内容。'}))
    result=HarnessProducer(c)._finish_output(task,'round1',out,[])
    assert result.get('attachment',{}).get('kind')=='drawing'
    assert not (out/'article.redacted.md').exists()
    assert not (out/'tree.json').exists()
    fingerprint=input_fingerprint(c,task,'round1');result['stage_input_sha256']=fingerprint
    manifest=prepare_cache(c,task,'round1',fingerprint,result)
    assert verify_entry(c,task,'round1',fingerprint,load(manifest))['result']['attachment']['kind']=='drawing'


def test_paper_with_attachment_field_still_requires_its_body_and_tree(tmp_path):
    from paperflow.producer import HarnessProducer,ProductionError
    c=Config(home=str(tmp_path/'runtime'))
    source=tmp_path/'paper.txt';source.write_text('ordinary paper',encoding='utf-8')
    task=Store(tmp_path/'state.sqlite3').register_source(source,sha(source))
    out=c.root/'work'/task['task_id'];out.mkdir(parents=True)
    save(out/'profile.json',dict(material_disposition='keep',attachment={'kind':'drawing','description':'论文提到了装配图'}))
    with pytest.raises(ProductionError,match='missing model artifacts'):
        HarnessProducer(c)._finish_output(task,'round1',out,[])


def test_interrupted_original_copy_is_not_published_and_retry_can_seal(tmp_path,monkeypatch):
    import paperflow.attachments as module
    c=Config(home=str(tmp_path/'runtime'));source=tmp_path/'drawing.dwg';source.write_bytes(b'original drawing')
    task=Store(tmp_path/'state.sqlite3').register_source(source,sha(source))
    out=c.root/'work'/task['task_id'];out.mkdir(parents=True);task['draft_dir']=str(out)
    save(out/'profile.json',dict(attachment={'kind':'drawing','description':'原图'}))
    real=module.shutil.copy2
    def interrupt(src,dst):
        Path(dst).write_bytes(b'partial');raise OSError('simulated interruption')
    monkeypatch.setattr(module.shutil,'copy2',interrupt)
    with pytest.raises(OSError,match='interruption'):module.prepare_package(c,task)
    monkeypatch.setattr(module.shutil,'copy2',real)
    root=module.prepare_package(c,task)
    assert (root/'original.dwg').read_bytes()==source.read_bytes()
    assert load(root/'manifest.json')['original_sha256']==sha(source)

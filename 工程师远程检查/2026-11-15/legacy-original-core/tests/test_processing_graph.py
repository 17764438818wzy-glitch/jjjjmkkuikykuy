import json
import os
from pathlib import Path
import shutil
import sys

import pytest

from paperflow.config import Config
from paperflow.control import Manager
from paperflow.package import load,save,sha,validate_package
from paperflow.store import Store
from test_winjob import wait_until


@pytest.fixture
def graph_case(tmp_path,package_factory,request):
    from paperflow.processing_control import ProcessingCommits
    root=tmp_path/'runtime';root.mkdir()
    c=Config(home=str(root),handoff_root=str(tmp_path/'handoff'),source_roots=[],
             catalog_path=str(tmp_path/'catalog.json'),rag_url='http://127.0.0.1:1')
    source=tmp_path/'source.txt';source.write_text('原稿保留',encoding='utf-8')
    image_bytes=None
    if getattr(request,'param',None)=='docx-image':
        import io,zipfile
        from PIL import Image
        stream=io.BytesIO();Image.new('RGB',(16,16),'white').save(stream,format='PNG');image_bytes=stream.getvalue()
        source=tmp_path/'source.docx'
        with zipfile.ZipFile(source,'w') as archive:
            archive.writestr('word/document.xml','<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"><w:body><w:p><w:r><w:t>研究方法</w:t></w:r></w:p></w:body></w:document>')
            archive.writestr('word/media/chart.png',image_bytes)
    store=Store(root/'state.sqlite3');task=store.register_source(source,sha(source))
    material,_,catalog=package_factory(task_id=task['task_id'],source_sha=task['source_sha256'])
    for name in ('profile.json','tree.json','assets.json','chunks.json','review.json','corrections.json'):
        value=load(material/name);value['source_id']=task['source_id'];save(material/name,value)
    if image_bytes:
        import hashlib
        asset=load(material/'assets.json')
        asset['items'][0].update(storage='image',file='assets/fig1.png',
            source_image_id='im_'+hashlib.sha256(image_bytes).hexdigest()[:20],
            redactions=[dict(x0=0,y0=0,x1=4,y1=4)])
        save(material/'assets.json',asset)
    shutil.copytree(material,root/'fixture-material')
    save(c.catalog_path,catalog);c.save(tmp_path/'config.json')
    manager=Manager(store,tmp_path/'manager',heartbeat_interval=.2,lease_seconds=10).start()
    ProcessingCommits(manager,c);manager.resume()
    yield c,store,task,manager,tmp_path/'config.json'
    manager.shutdown()


def run_worker(case,mode='normal',wait=True):
    c,s,t,m,config=case
    execution=m.launch('graph',t['task_id'],[sys.executable,'-X','utf8',
        str(Path('tests/fixtures/processing_worker.py').resolve()),str(config),str(s.path),t['task_id'],mode],
        cwd=Path.cwd(),env={'PYTHONPATH':os.pathsep.join([os.environ.get('PYTHONPATH',''),str(Path.cwd())])},total_timeout=120)
    if wait:wait_until(lambda:execution.exit_saved,seconds=30)
    return execution


def model_calls(c):
    return [json.loads(line)['stage'] for line in (c.root/'fixture-events.jsonl').read_text(encoding='utf-8').splitlines()
            if json.loads(line)['event']=='model']


def test_three_rounds_end_at_registered_package_without_delivery(graph_case):
    c,s,t,m,_=graph_case
    run_worker(graph_case)
    assert load(c.root/'worker-result.json')['stage']=='handed_off'
    assert model_calls(c)==['round1','round2','round3_assets','round3_redact','round3_chunk']
    packages=m.ledger.rows('SELECT * FROM delivery_packages')
    assert len(packages)==1
    validate_package(packages[0]['package_dir'],t,c.catalog,strict_seal=True)
    assert [r['status'] for r in m.ledger.rows('SELECT * FROM specialty_tasks')]==['pending']
    assert not m.ledger.rows('SELECT * FROM receipts')
    assert not m.ledger.rows("SELECT * FROM control_actions WHERE kind LIKE 'rag_%'")
    assert Path(t['source_path']).is_file()
    assert not (Path(c.handoff_root)/'Updated manuscript').exists()
    # Exercise the shipped launcher/CLI on this completed native checkpoint:
    # it must project its terminal fact without starting a provider or a delivery.
    from paperflow.graph_worker import launch_graph
    s.update(t['task_id'],stage='round1')
    execution=launch_graph(m,c,t['task_id'],total_timeout=30)
    wait_until(lambda:execution.exit_saved,seconds=15)
    assert len(model_calls(c))==5
    assert s.get(t['task_id'])['stage']=='handed_off'


@pytest.mark.parametrize('fault',['after-cache-round1','after-cache-round2','after-cache-round3_assets',
                                'after-cache-round3_redact','after-cache-round3_chunk','after-seal','after-handoff'])
def test_restart_reuses_completed_files_and_replays_handoff(graph_case,fault):
    c,s,t,m,_=graph_case
    run_worker(graph_case,fault)
    first_packages=m.ledger.rows('SELECT * FROM delivery_packages')
    sealed=list(Path(c.handoff_root).glob('sealed/*/*/manifest.json'))
    bytes_before={str(p):p.read_bytes() for p in sealed}
    run_worker(graph_case,fault)
    assert load(c.root/'worker-result.json')['stage']=='handed_off'
    assert model_calls(c)==['round1','round2','round3_assets','round3_redact','round3_chunk']
    assert len(m.ledger.rows('SELECT * FROM delivery_packages'))==1
    assert len(m.ledger.rows('SELECT * FROM specialty_tasks'))==1
    assert m.ledger.rows('SELECT generation FROM target_versions')[0]['generation']==1
    for p,content in bytes_before.items():assert Path(p).read_bytes()==content
    if fault=='after-handoff':assert first_packages


@pytest.mark.parametrize('mode,stages',[('exclude',['round1']),('no-specialty',['round1','round2'])])
def test_held_material_is_not_delivery_success(graph_case,mode,stages):
    c,s,t,m,_=graph_case
    run_worker(graph_case,mode)
    assert load(c.root/'worker-result.json')['stage']=='held'
    assert model_calls(c)==stages
    assert not m.ledger.rows('SELECT * FROM delivery_packages')
    assert Path(t['source_path']).is_file()


def test_pause_during_round2_reuses_round1_and_discards_late_output(graph_case):
    c,s,t,m,_=graph_case
    ex=run_worker(graph_case,'pause-round2',wait=False)
    wait_until(lambda:(c.root/'model-entered').exists(),seconds=20)
    m.pause('real graph pause')
    wait_until(lambda:ex.exit_saved)
    assert not list((c.root/'work'/t['task_id']/'phase-cache'/'round2').glob('*.json'))
    assert not m.ledger.rows('SELECT * FROM delivery_packages')
    m.resume();run_worker(graph_case)
    assert load(c.root/'worker-result.json')['stage']=='handed_off'
    assert model_calls(c)==['round1','round2','round2','round3_assets','round3_redact','round3_chunk']


def test_resume_validates_checkpoint_dependencies_and_rebuilds_only_damaged_stage(graph_case):
    c,s,t,m,_=graph_case
    run_worker(graph_case,'after-cache-round3_chunk')
    marker=next((c.root/'work'/t['task_id']/'phase-cache'/'round2').glob('*.json'))
    entry=load(marker)
    (Path(entry['result']['draft_dir'])/'article.redacted.md').write_text('damaged',encoding='utf-8')
    run_worker(graph_case)
    assert load(c.root/'worker-result.json')['stage']=='handed_off'
    calls=model_calls(c)
    assert calls.count('round1')==1 and calls.count('round2')==2
    assert calls.count('round3_assets')==calls.count('round3_redact')==calls.count('round3_chunk')==1


@pytest.mark.parametrize('bad_marker',[[],None],ids=['array','null'])
def test_non_object_cache_marker_recovers_only_affected_stage(graph_case,bad_marker):
    c,s,t,m,_=graph_case
    run_worker(graph_case,'after-cache-round3_chunk')
    marker=next((c.root/'work'/t['task_id']/'phase-cache'/'round2').glob('*.json'))
    old_draft=load(marker)['result']['draft_dir']
    save(marker,bad_marker)
    run_worker(graph_case)
    result=c.root/'worker-result.json'
    assert result.is_file(), (c.root/'fixture-events.jsonl').read_text(encoding='utf-8')
    assert load(result)['stage']=='handed_off'
    calls=model_calls(c)
    assert calls.count('round1')==1 and calls.count('round2')==2
    assert calls.count('round3_assets')==calls.count('round3_redact')==calls.count('round3_chunk')==1
    repaired=load(marker)
    assert repaired['complete'] is True and repaired['result']['draft_dir']!=old_draft
    assert len(m.ledger.rows('SELECT * FROM delivery_packages'))==1
    assert len(m.ledger.rows('SELECT * FROM specialty_tasks'))==1
    assert m.ledger.rows('SELECT generation FROM target_versions')[0]['generation']==1
    run_worker(graph_case)
    assert model_calls(c)==calls
    assert len(m.ledger.rows('SELECT * FROM delivery_packages'))==1
    assert m.ledger.rows('SELECT generation FROM target_versions')[0]['generation']==1


def test_changed_original_before_sealing_is_not_handed_off(graph_case):
    c,s,t,m,_=graph_case
    run_worker(graph_case,'after-seal')
    Path(t['source_path']).write_text('new source bytes',encoding='utf-8')
    run_worker(graph_case)
    assert not m.ledger.rows('SELECT * FROM delivery_packages')


@pytest.mark.parametrize('fault',['after-recovery-clear','after-recovery-route'])
def test_recovery_checkpoint_write_crash_cannot_turn_unfinished_graph_into_success(graph_case,fault):
    c,s,t,m,_=graph_case
    run_worker(graph_case,'after-cache-round3_chunk')
    marker=next((c.root/'work'/t['task_id']/'phase-cache'/'round2').glob('*.json'))
    draft=Path(load(marker)['result']['draft_dir'])
    (draft/'article.redacted.md').write_text('damaged',encoding='utf-8')
    run_worker(graph_case,fault)
    assert (c.root/fault).is_file()
    assert not (c.root/'worker-result.json').exists()
    assert not m.ledger.rows('SELECT * FROM delivery_packages')
    # Recovery must read the native checkpoint, even with a contradictory mirror.
    s.update(t['task_id'],stage='held',external_note='keep this note')
    run_worker(graph_case)
    assert load(c.root/'worker-result.json')['stage']=='handed_off'
    assert len(m.ledger.rows('SELECT * FROM delivery_packages'))==1
    assert len(m.ledger.rows('SELECT * FROM specialty_tasks'))==1
    assert m.ledger.rows('SELECT generation FROM target_versions')[0]['generation']==1
    calls=model_calls(c)
    assert calls.count('round1')==1 and calls.count('round2')==2
    assert calls.count('round3_assets')==calls.count('round3_redact')==calls.count('round3_chunk')==1
    assert s.get(t['task_id'])['external_note']=='keep this note'
    run_worker(graph_case)
    assert model_calls(c)==calls


def test_persisted_mirror_cannot_rewind_native_graph_position(graph_case):
    c,s,t,m,_=graph_case
    run_worker(graph_case,'after-cache-round3_redact')
    s.update(t['task_id'],stage='round1',external_note='preserve')
    run_worker(graph_case)
    assert load(c.root/'worker-result.json')['stage']=='handed_off'
    assert len(model_calls(c))==5
    assert s.get(t['task_id'])['external_note']=='preserve'


@pytest.mark.parametrize('graph_case',['docx-image'],indirect=True)
def test_actual_docx_reader_and_image_redaction_inside_managed_graph(graph_case):
    from PIL import Image
    c,s,t,m,_=graph_case
    run_worker(graph_case)
    assert load(c.root/'worker-result.json')['stage']=='handed_off'
    package=Path(m.ledger.rows('SELECT package_dir FROM delivery_packages')[0]['package_dir'])
    with Image.open(package/'assets/fig1.png') as img:
        assert img.getpixel((1,1))==(0,0,0)
        assert img.getpixel((10,10))==(255,255,255)
    assert sha(t['source_path'])==t['source_sha256']
    view=load(c.root/'work'/t['task_id']/'private/source-view/source-index.json')
    assert len(view['images'])==1

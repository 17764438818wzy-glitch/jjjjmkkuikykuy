"""NAS is an input source, never an implicit task/run command."""
import concurrent.futures
import hashlib
import os
from pathlib import Path
import threading
import time

import pytest

from paperflow.package import save
from test_managed_entry import entry_case
from test_workbench import web_case, login, select


@pytest.fixture
def nas_case(web_case,tmp_path):
    workbench,client,_=web_case
    root=tmp_path/'NAS论文';root.mkdir()
    (root/'管理学').mkdir();(root/'管理学/论文.docx').write_bytes(b'original-paper-bytes')
    (root/'附件.zip').write_bytes(b'original-archive-bytes')
    (root/'跳过.unhandled').write_bytes(b'not-a-paper')
    save(workbench.root/'nas-roots.json',{'roots':[{'id':'papers','name':'论文原稿','path':str(root)}]})
    return workbench,client,root,login(client)


def browse(client,path='',**params):
    response=client.get('/api/nas/browse',params=dict(root='papers',path=path,**params))
    assert response.status_code==200,response.text
    return response.json()


def import_file(case,path='管理学/论文.docx'):
    w,c,root,headers=case
    listing=browse(c,str(Path(path).parent).replace('\\','/'))
    row=next(r for r in listing['entries'] if r['path']==path)
    return c.post('/api/inputs/nas',headers=headers,json={'root':'papers','path':path,'version':row['version']})


def make_link(path,target):
    try:os.symlink(target,path,target_is_directory=True)
    except OSError:
        if os.name!='nt':raise
        import subprocess
        result=subprocess.run(['cmd','/d','/c','mklink','/J',str(path),str(target)],capture_output=True)
        assert result.returncode==0,result.stderr


def test_nas_roots_and_browse_reuse_auth_and_do_not_run(nas_case):
    w,c,root,headers=nas_case
    c.cookies.clear()
    assert c.get('/api/nas/roots').status_code==401
    login(c)
    roots=c.get('/api/nas/roots')
    assert roots.status_code==200
    assert roots.json()['roots'][0]['id']=='papers'
    listing=browse(c)
    assert {r['name'] for r in listing['entries']}=={'管理学','附件.zip'}
    assert c.get('/api/status').json()['manager']['state']=='paused'
    assert not w.scheduler.ledger.rows('SELECT * FROM scheduler_jobs')
    assert not w.store.all_tasks()


def test_import_is_verified_local_copy_and_reuses_upload_dedup(nas_case):
    w,c,root,headers=nas_case;original=root/'管理学/论文.docx'
    limits=(w.config.llm_workers,w.config.rag_workers)
    before=original.read_bytes();response=import_file(nas_case)
    assert response.status_code==200,response.text
    first=response.json()['id']
    assert import_file(nas_case).json()['id']==first
    assert select(c,headers,'same.docx',before)==first
    with w.store.db() as db:row=dict(db.execute('SELECT * FROM web_inputs').fetchone())
    copy=Path(row['path'])
    assert copy.is_relative_to(w.root/'uploads') and copy.read_bytes()==before
    assert original.read_bytes()==before and len(list(c.get('/api/inputs').json()))==1
    assert not w.store.all_tasks() and not w.scheduler.ledger.rows('SELECT * FROM scheduler_jobs')
    assert c.get('/api/status').json()['manager']['admission_open'] is False
    assert w.config.source_roots==[] and (w.config.llm_workers,w.config.rag_workers)==limits


@pytest.mark.parametrize('path',['../outside.txt','/etc/passwd','C:/secret.txt',r'管理学\..\outside.txt',
    '管理学/论文.docx:secret','管理学/./论文.docx','管理学//论文.docx','管理学/论文.docx.'])
def test_browse_rejects_windows_aliases_and_outside_paths(nas_case,path):
    w,c,root,headers=nas_case
    assert c.get('/api/nas/browse',params={'root':'papers','path':path}).status_code==400
    assert not w.store.all_tasks()


def test_import_reuses_csrf_and_rejects_stale_file_version(nas_case):
    w,c,root,headers=nas_case
    row=browse(c,'管理学')['entries'][0]
    body={'root':'papers','path':row['path'],'version':row['version']}
    assert c.post('/api/inputs/nas',json=body).status_code==403
    (root/'管理学/论文.docx').write_bytes(b'changed-after-selection')
    response=c.post('/api/inputs/nas',json=body,headers=headers)
    assert response.status_code==400 and '改变' in response.json()['detail']
    assert c.get('/api/inputs').json()==[]
    assert not list((w.root/'uploads').glob('*')) if (w.root/'uploads').exists() else True


def test_real_directory_link_cannot_escape_allowed_root(nas_case,tmp_path):
    w,c,root,headers=nas_case
    outside=tmp_path/'outside';outside.mkdir();(outside/'secret.txt').write_bytes(b'not selected')
    make_link(root/'linked',outside)
    assert 'linked' not in {r['name'] for r in browse(c)['entries']}
    assert c.get('/api/nas/browse',params={'root':'papers','path':'linked'}).status_code==400
    assert c.post('/api/inputs/nas',json={'root':'papers','path':'linked/secret.txt','version':'a'*64},headers=headers).status_code==400


def test_browse_is_paged_and_does_not_recurse(nas_case):
    w,c,root,headers=nas_case
    for i in range(6):(root/f'paper{i}.txt').write_text('paper',encoding='utf-8')
    first=browse(c,limit=3)
    assert len(first['entries'])<=3 and first['next_offset'] is not None
    second=browse(c,limit=3,offset=first['next_offset'])
    assert not {r['path'] for r in first['entries']}&{r['path'] for r in second['entries']}
    assert all('/' not in r['path'] for r in first['entries']+second['entries'])


def test_configured_root_cannot_hide_linked_parent(nas_case,tmp_path):
    w,c,root,headers=nas_case
    outside=tmp_path/'outside-root';(outside/'leaf').mkdir(parents=True)
    (outside/'leaf/secret.txt').write_text('outside',encoding='utf-8')
    make_link(root/'parent-link',outside)
    save(w.root/'nas-roots.json',{'roots':[{'id':'papers','name':'linked-parent','path':str(root/'parent-link/leaf')}]})
    assert c.get('/api/nas/browse',params={'root':'papers'}).status_code==400


def test_directory_swapped_after_validation_does_not_leak_outside_listing(nas_case,tmp_path,monkeypatch):
    import paperflow.workbench_nas as module
    w,c,root,headers=nas_case
    outside=tmp_path/'outside-list';outside.mkdir();(outside/'SECRET_OUTSIDE.txt').write_text('outside')
    target=root/'管理学';original=module.os.scandir;swapped=False
    def swapping(path):
        nonlocal swapped
        if Path(path)==target and not swapped:
            swapped=True
            try:target.rename(root/'old-directory')
            except OSError:pass  # A held Windows directory handle can prevent the swap.
            else:make_link(target,outside)
        return original(path)
    monkeypatch.setattr(module.os,'scandir',swapping)
    response=c.get('/api/nas/browse',params={'root':'papers','path':'管理学'})
    assert swapped and 'SECRET_OUTSIDE' not in response.text


def test_blocked_nas_copy_does_not_block_pause_or_queue_more_nas_threads(nas_case,monkeypatch):
    import paperflow.workbench_nas as module
    w,c,root,headers=nas_case
    entered=threading.Event();release=threading.Event();original=module.open_source
    def slow(path):
        entered.set();assert release.wait(8)
        return original(path)
    monkeypatch.setattr(module,'open_source',slow)
    row=browse(c,'管理学')['entries'][0]
    body={'root':'papers','path':row['path'],'version':row['version']}
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        pending=pool.submit(lambda:c.post('/api/inputs/nas',json=body,headers=headers))
        try:
            assert entered.wait(3)
            busy=c.get('/api/nas/browse',params={'root':'papers'})
            assert busy.status_code==503
            started=time.monotonic()
            paused=c.post('/api/pause',headers=headers,json={'epoch':w.manager.epoch})
            assert paused.status_code==200 and time.monotonic()-started<1
            assert not paused.json()['manager']['admission_open']
        finally:release.set()
        assert pending.result(timeout=5).status_code==200
    assert not w.store.all_tasks()


def test_source_change_after_copy_is_rejected_and_local_partial_removed(nas_case,monkeypatch):
    import paperflow.workbench_nas as module
    w,c,root,headers=nas_case
    original=module.file_version;calls=0
    def changed(stat):
        nonlocal calls
        calls+=1
        return ('f'*64) if calls>=3 else original(stat)
    row=browse(c,'管理学')['entries'][0]
    monkeypatch.setattr(module,'file_version',changed)
    response=c.post('/api/inputs/nas',headers=headers,json={'root':'papers','path':row['path'],'version':row['version']})
    assert response.status_code==400
    assert c.get('/api/inputs').json()==[]
    assert not list((w.root/'uploads').glob('*'))


def test_simultaneous_same_bytes_upload_registration_is_atomic(nas_case):
    w,c,root,headers=nas_case;paths=[]
    for i in range(2):
        folder=w.root/'uploads'/str(i);folder.mkdir(parents=True)
        p=folder/'same.txt';p.write_bytes(b'same-content');paths.append(p)
    gate=threading.Barrier(2)
    def register(path):gate.wait();return w.record_upload(path.name,path)['id']
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        ids=list(pool.map(register,paths))
    assert ids[0]==ids[1]
    assert len(c.get('/api/inputs').json())==1


def test_opened_file_outside_root_is_rejected_before_copy(nas_case,tmp_path,monkeypatch):
    import paperflow.workbench_nas as module
    w,c,root,headers=nas_case
    outside=tmp_path/'not-selected.docx';outside.write_bytes(b'outside')
    monkeypatch.setattr(module,'open_source',lambda path:outside.open('rb'))
    response=import_file(nas_case)
    assert response.status_code==400 and c.get('/api/inputs').json()==[]


@pytest.mark.parametrize('failure',['connection','disk','too_large'])
def test_failed_copy_never_becomes_an_input(nas_case,monkeypatch,failure):
    import paperflow.workbench_nas as module
    w,c,root,headers=nas_case
    if failure=='connection':
        def unavailable(path):raise ConnectionError('offline fixture')
        monkeypatch.setattr(module,'open_source',unavailable)
    elif failure=='disk':
        from types import SimpleNamespace
        monkeypatch.setattr(module.shutil,'disk_usage',lambda path:SimpleNamespace(free=0))
    else:
        import paperflow.workbench as workbench
        monkeypatch.setattr(workbench,'MAX_UPLOAD',10)
    response=import_file(nas_case)
    assert response.status_code==(400 if failure=='too_large' else 503)
    assert c.get('/api/inputs').json()==[] and not w.store.all_tasks()


@pytest.mark.skipif(os.name!='nt',reason='Native Windows source sharing contract')
def test_read_handle_denies_concurrent_write_and_delete(tmp_path):
    from paperflow.workbench_nas import open_source
    path=tmp_path/'original.txt';path.write_bytes(b'original')
    with open_source(path) as source:
        with pytest.raises(OSError):path.write_bytes(b'changed')
        with pytest.raises(OSError):path.unlink()
        assert source.read()==b'original'
    assert path.read_bytes()==b'original'


@pytest.mark.skipif(os.environ.get('PAPERFLOW_TEST_LIVE_NAS')!='1',reason='Explicit read-only NAS acceptance only')
def test_real_nas_copy_into_isolated_workbench_without_tasks(web_case):
    w,c,_=web_case;headers=login(c)
    root=Path(r'\\zwjy-nas\home\总稿子')
    relative='25年武汉科技大学/城市建设学院/城乡规划/城乡规划2020级1班/何性如_202008406032/主修_城市建设学院_城乡规划_202008406032_何性如.pdf'
    save(w.root/'nas-roots.json',{'roots':[{'id':'papers','name':'NAS 总稿子','path':str(root)}]})
    parent=relative.rpartition('/')[0]
    row=next(r for r in browse(c,parent)['entries'] if r['path']==relative)
    source=root/relative;digest=hashlib.sha256(source.read_bytes()).hexdigest()
    response=c.post('/api/inputs/nas',headers=headers,json={'root':'papers','path':relative,'version':row['version']})
    assert response.status_code==200,response.text
    with w.store.db() as db:local=dict(db.execute('SELECT * FROM web_inputs').fetchone())
    assert local['sha256']==digest and hashlib.sha256(source.read_bytes()).hexdigest()==digest
    assert Path(local['path']).read_bytes()==source.read_bytes()
    assert not w.store.all_tasks() and not w.scheduler.ledger.rows('SELECT * FROM scheduler_jobs')
    assert c.get('/api/status').json()['manager']['admission_open'] is False

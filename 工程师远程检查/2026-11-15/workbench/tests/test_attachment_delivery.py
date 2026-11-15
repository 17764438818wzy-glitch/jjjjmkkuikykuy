import base64
import importlib
import json
import os
from pathlib import Path
import sys
import time

import pytest

from paperflow.attachments import AttachmentLedger,prepare_package
from paperflow.config import Config
from paperflow.control import Manager
from paperflow.delivery_control import DeliveryCommits
from paperflow.package import load,save,sha
from paperflow.store import Store
from test_winjob import wait_until


def case(tmp_path,*,category='drawing',**backend_options):
    importlib.import_module('paperflow.attachment_delivery')
    c=Config(home=str(tmp_path/'runtime'),handoff_root=str(tmp_path/'handoff'),source_roots=[],
        catalog_path=str(tmp_path/'catalog.json'),rag_url='http://127.0.0.1:1',
        attachment_targets={category:{'parent_id':'raw-'+category,'dataset_id':'attachment-notes'}})
    c.prepare();save(c.catalog_path,{'majors':[]})
    c.delivery_acceptance_report=str(tmp_path/'acceptance.json')
    identity={'dataset_ids':['attachment-notes'],'mode':'offline-synthetic'}
    save(c.delivery_acceptance_report,dict(passed=True,endpoint=c.rag_url,identity=identity,
         attachment_storage=dict(passed=True,targets=c.attachment_targets)))
    c.save(tmp_path/'config.json')
    source=tmp_path/('source.zip' if category=='software' else 'source.dwg')
    if category=='software':
        import zipfile
        with zipfile.ZipFile(source,'w') as z:z.writestr('main.py','raise Exception("never execute")')
    else:source.write_bytes(b'AC1032\x00\xff\x80original CAD')
    s=Store(c.root/'state.sqlite3');task=s.register_source(source,sha(source))
    out=c.root/'work'/task['task_id']/'round1';out.mkdir(parents=True)
    save(out/'profile.json',dict(task_id=task['task_id'],source_id=task['source_id'],source_sha256=task['source_sha256'],
         material_disposition='keep',attachment={'kind':category,'description':'减速器附件，保留原件。','related_paper_title':'减速器设计'}))
    task['draft_dir']=str(out);root=prepare_package(c,task);ledger=AttachmentLedger(s)
    manifest=load(root/'manifest.json');manifest_sha=sha(root/'manifest.json')
    item=ledger._write(lambda db:ledger.register_sql(db,task,root,manifest,c.attachment_targets,manifest_sha,c.rag_url))
    save(c.root/'backend.json',dict(files={},docs={},calls={},deployment_identity=identity,**backend_options))
    m=Manager(s,tmp_path/'manager',heartbeat_interval=.2,lease_seconds=10).start()
    DeliveryCommits(m,c);m.resume()
    return c,s,item['attachment_id'],m


def worker(c,s,aid,m,mode='normal',wait=True):
    ex=m.launch('delivery','attachment:'+aid,[sys.executable,'-X','utf8','-B',
        str(Path('tests/fixtures/attachment_delivery_worker.py').resolve()),str(c.root.parent/'config.json'),aid,mode],
        cwd=Path.cwd(),env={'PYTHONPATH':str(Path.cwd())},total_timeout=60,expected_generation=0)
    if wait:wait_until(lambda:ex.exit_saved,seconds=15)
    return ex


@pytest.mark.parametrize('category',['drawing','software'])
def test_managed_attachment_keeps_exact_bytes_and_readable_note_and_file_id(tmp_path,category):
    c,s,aid,m=case(tmp_path,category=category)
    try:
        worker(c,s,aid,m)
        row=AttachmentLedger(s).item(aid);data=json.loads(row['data']);backend=load(c.root/'backend.json')
        assert row['state']=='complete',load(c.root/'delivery-error.json') if (c.root/'delivery-error.json').exists() else row
        source=s.get(row['task_id'])['source_path']
        assert base64.b64decode(backend['files'][data['remote']['file_id']]['blob'])==Path(source).read_bytes()
        doc=backend['docs'][data['remote']['document_id']]
        assert len(doc['chunks'])==1 and data['remote']['file_id'] in doc['chunks'][0]['content']
        assert '减速器设计' in doc['chunks'][0]['content']
        assert data['association']['status']=='pending' and data['association']['body_document_id'] is None
        assert backend['calls']['POST /files']==1
        assert len(m.ledger.rows("SELECT * FROM control_actions WHERE kind='rag_write' AND state='complete'"))==3
        worker(c,s,aid,m)
        assert load(c.root/'backend.json')['calls']['POST /files']==1
    finally:m.shutdown()


def test_crash_after_successful_post_before_file_id_commit_never_reuploads(tmp_path):
    c,s,aid,m=case(tmp_path)
    try:
        worker(c,s,aid,m,'crash')
        assert load(c.root/'backend.json')['calls']['POST /files']==1
        worker(c,s,aid,m)
        assert AttachmentLedger(s).item(aid)['state']=='complete'
        assert load(c.root/'backend.json')['calls']['POST /files']==1
    finally:m.shutdown()


def test_abandoned_unstarted_upload_may_receive_its_first_real_permit_on_resume(tmp_path):
    c,s,aid,m=case(tmp_path)
    try:
        worker(c,s,aid,m,'prepared')
        assert m.ledger.rows("SELECT * FROM control_actions WHERE kind='rag_write' AND state='abandoned'")
        assert load(c.root/'backend.json')['calls'].get('POST /files',0)==0
        worker(c,s,aid,m)
        assert AttachmentLedger(s).item(aid)['state']=='complete'
        assert load(c.root/'backend.json')['calls']['POST /files']==1
    finally:m.shutdown()


@pytest.mark.parametrize('drop',[False,True])
def test_lost_response_reconciles_only_positive_matching_bytes_and_does_not_resend(tmp_path,drop):
    c,s,aid,m=case(tmp_path,lose_response=True,drop_upload=drop)
    try:
        worker(c,s,aid,m)
        assert m.ledger.rows("SELECT * FROM control_actions WHERE state='unresolved'")
        worker(c,s,aid,m)
        assert load(c.root/'backend.json')['calls']['POST /files']==1
        if drop:
            assert AttachmentLedger(s).item(aid)['state']!='complete'
            assert m.ledger.rows("SELECT * FROM control_actions WHERE state='unresolved'")
        else:
            assert AttachmentLedger(s).item(aid)['state']=='complete'
            assert not m.ledger.rows("SELECT * FROM control_actions WHERE state='unresolved'")
    finally:m.shutdown()


def test_wrong_download_bytes_are_not_marked_saved_or_described(tmp_path):
    c,s,aid,m=case(tmp_path,tamper=True)
    try:
        worker(c,s,aid,m)
        row=AttachmentLedger(s).item(aid)
        assert row['state']!='complete'
        assert not json.loads(row['data'])['remote'].get('file_id')
        assert not load(c.root/'backend.json')['docs']
        assert sha(Path(row['package_dir'])/'original.dwg')==row['source_sha256']
    finally:m.shutdown()


def test_manager_restart_reconciles_unknown_upload_without_reusing_old_epoch(tmp_path):
    c,s,aid,m=case(tmp_path,lose_response=True)
    worker(c,s,aid,m);old=m.epoch;root=m.root;m.shutdown()
    new=Manager(s,root,heartbeat_interval=.2,lease_seconds=10).start()
    try:
        DeliveryCommits(new,c);assert new.epoch!=old and new.stopped.is_set()
        assert new.ledger.rows("SELECT * FROM control_actions WHERE state='unresolved'")
        new.resume();worker(c,s,aid,new)
        assert AttachmentLedger(s).item(aid)['state']=='complete'
        assert load(c.root/'backend.json')['calls']['POST /files']==1
    finally:new.shutdown()


@pytest.mark.parametrize('option',['lose_note_response','lose_chunk_response'])
def test_note_failure_resumes_without_reuploading_original_or_duplicate_note(tmp_path,option):
    c,s,aid,m=case(tmp_path,**{option:True})
    try:
        worker(c,s,aid,m)
        assert AttachmentLedger(s).item(aid)['state']!='complete'
        worker(c,s,aid,m)
        assert AttachmentLedger(s).item(aid)['state']=='complete'
        calls=load(c.root/'backend.json')['calls']
        assert calls['POST /files']==1
        assert calls['POST /datasets/attachment-notes/documents']==1
        assert sum(v for k,v in calls.items() if k.startswith('POST ') and k.endswith('/chunks'))==1
    finally:m.shutdown()


@pytest.mark.parametrize('option',['omit_available','disable_chunk','partial_chunks'])
def test_missing_availability_or_incomplete_slice_set_cannot_complete(tmp_path,option):
    c,s,aid,m=case(tmp_path,**{option:True})
    try:
        worker(c,s,aid,m)
        assert AttachmentLedger(s).item(aid)['state']=='note_saved'
        assert not json.loads(AttachmentLedger(s).item(aid)['data'])['remote'].get('chunk_id')
    finally:m.shutdown()


@pytest.mark.parametrize('change',['wrong_parent','duplicate'])
def test_recovery_rejects_wrong_parent_or_ambiguous_file_identity(tmp_path,change):
    c,s,aid,m=case(tmp_path,lose_response=True)
    try:
        worker(c,s,aid,m)
        data=load(c.root/'backend.json');raw=data['files']['file-1']
        if change=='wrong_parent':raw['parent_id']='foreign'
        else:data['files']['other-file']=dict(raw,id='other-file')
        save(c.root/'backend.json',data)
        worker(c,s,aid,m)
        assert AttachmentLedger(s).item(aid)['state']=='pending'
        assert load(c.root/'backend.json')['calls']['POST /files']==1
        assert not load(c.root/'backend.json')['docs']
    finally:m.shutdown()


def test_superseded_file_cannot_start_attachment_delivery(tmp_path):
    c,s,aid,m=case(tmp_path)
    try:
        row=AttachmentLedger(s).item(aid);source=Path(s.get(row['task_id'])['source_path'])
        source.write_bytes(b'changed original');s.register_source(source,sha(source))
        worker(c,s,aid,m)
        assert load(c.root/'backend.json')['calls'].get('POST /files',0)==0
        assert AttachmentLedger(s).item(aid)['state']=='pending'
    finally:m.shutdown()


def test_pause_before_pending_upload_denies_action_even_after_database_wait(tmp_path):
    c,s,aid,m=case(tmp_path)
    try:
        ex=worker(c,s,aid,m,'wait',wait=False)
        wait_until(lambda:(c.root/'waiting').exists(),seconds=10)
        import sqlite3
        lock=sqlite3.connect(s.path);lock.execute('BEGIN IMMEDIATE')
        (c.root/'continue').touch();time.sleep(.1)
        m.pause('offline acceptance pause')
        assert m.stopped.is_set()
        lock.rollback();lock.close()
        wait_until(lambda:ex.exit_saved,seconds=10)
        assert load(c.root/'backend.json')['calls'].get('POST /files',0)==0
        m.resume();worker(c,s,aid,m)
        assert AttachmentLedger(s).item(aid)['state']=='complete'
    finally:m.shutdown()

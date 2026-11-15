import json
from pathlib import Path
import sys

import pytest

from paperflow.attachments import AttachmentLedger
from paperflow.package import load,save
from paperflow.scheduler import Scheduler
from test_attachment_delivery import case,worker
from test_managed_entry import entry_case
from test_winjob import wait_until


def acceptance(c,path):
    c.delivery_acceptance_report=str(path)
    save(path,dict(passed=True,endpoint=c.rag_url,identity={'dataset_ids':['attachment-notes']},
         attachment_storage=dict(passed=True,targets=c.attachment_targets)))


def test_attachment_launcher_requires_explicit_storage_acceptance_and_reuses_delivery_job(tmp_path,monkeypatch):
    from paperflow.attachment_worker import launch_attachment
    c,s,aid,m=case(tmp_path)
    try:
        c.delivery_acceptance_report=None
        with pytest.raises(ValueError,match='acceptance'):launch_attachment(m,c,aid)
        acceptance(c,tmp_path/'acceptance.json')
        report=load(c.delivery_acceptance_report);report.pop('attachment_storage');save(c.delivery_acceptance_report,report)
        with pytest.raises(ValueError,match='attachment'):launch_attachment(m,c,aid)
        acceptance(c,tmp_path/'acceptance.json')
        calls=[]
        monkeypatch.setattr(m,'launch',lambda *a,**kw:calls.append((a,kw)) or 'launched')
        assert launch_attachment(m,c,aid)=='launched'
        args,kwargs=calls[0]
        assert args[0:2]==('delivery','attachment:'+aid)
        assert 'paperflow.attachment_worker' in args[2] and kwargs['expected_generation']==0
    finally:m.shutdown()


def test_attachment_uses_existing_queue_capacity_and_verified_completion(tmp_path):
    c,s,aid,m=case(tmp_path)
    try:
        row=AttachmentLedger(s).item(aid);s.update(row['task_id'],stage='attachment_ready')
        c.min_free_gib=0;c.max_memory_percent=100;c.max_ingest_backlog=1;c.cleanup=False
        scheduler=Scheduler(m,c,isolation_root=tmp_path)
        def launch(manager,config,key,*,schedule_attempt):
            return manager.launch('delivery','attachment:'+key,[sys.executable,'-X','utf8','-B',
                str(Path('tests/fixtures/attachment_scheduled_worker.py').resolve()),
                '--config',str(tmp_path/'config.json'),'--attachment',key],
                cwd=Path.cwd(),env={'PYTHONPATH':str(Path.cwd())},total_timeout=60,
                schedule_attempt=schedule_attempt,expected_generation=0)
        scheduler.launchers['attachment']=launch
        scheduler.discover();jobs=scheduler.ledger.rows('SELECT * FROM scheduler_jobs')
        assert len(jobs)==1 and jobs[0]['kind']=='delivery' and jobs[0]['unit_key']=='attachment:'+aid
        assert scheduler.pressure()['pending_package_bytes']==json.loads(row['data'])['bytes']
        assert 'target_backlog' in scheduler.pressure()['reasons']
        assert scheduler.tick()['launched']==1
        wait_until(lambda:all(ex.exit_saved for ex in m.executions.values()),seconds=15)
        scheduler.tick()
        assert scheduler.ledger.rows('SELECT * FROM scheduler_jobs')[0]['state']=='complete'
        assert scheduler.pressure()['pending_targets']==0
        assert load(c.root/'backend.json')['calls']['POST /files']==1
    finally:m.shutdown()


def test_configuration_pending_continues_without_reprocessing_or_rebinding_written_object(tmp_path):
    c,s,aid,m=case(tmp_path)
    try:
        ledger=AttachmentLedger(s);row=ledger.item(aid);data=json.loads(row['data'])
        data['target']={}
        ledger._write(lambda db:db.execute("UPDATE attachment_tasks SET state='configuration_pending',data=? WHERE attachment_id=?",(json.dumps(data),aid)))
        worker(c,s,aid,m)
        assert ledger.item(aid)['state']=='complete'
        assert json.loads(ledger.item(aid)['data'])['target']==c.attachment_targets['drawing']
        c.attachment_targets['drawing']['parent_id']='foreign-folder'
        worker(c,s,aid,m)
        assert load(c.root/'backend.json')['calls']['POST /files']==1
    finally:m.shutdown()


def test_operator_status_reports_attachment_readback_separately_from_paper_and_search(tmp_path):
    from paperflow.attachments import attachment_status
    c,s,aid,m=case(tmp_path)
    try:
        task_id=AttachmentLedger(s).item(aid)['task_id']
        assert attachment_status(s,task_id)['complete'] is False
        worker(c,s,aid,m)
        status=attachment_status(s,task_id)
        assert status['complete'] is True
        assert status['verification']=='original_and_note_readback'
        assert status['retrieval']=='not_checked' and status['association']=='pending'
    finally:m.shutdown()


def test_bounded_entry_requires_acceptance_for_additional_attachment_targets(entry_case):
    from paperflow.managed import validate_scope
    root,old,source,c=entry_case
    c.attachment_targets={'drawing':{'parent_id':'drawings','dataset_id':'notes'}}
    with pytest.raises(ValueError,match='attachment'):validate_scope(c,root,[source],old)
    report=load(c.delivery_acceptance_report)
    report['identity']['dataset_ids'].append('notes')
    report['attachment_storage']=dict(passed=True,targets=c.attachment_targets)
    save(c.delivery_acceptance_report,report)
    assert validate_scope(c,root,[source],old)[0]==root


def test_missing_targets_leave_retained_attachment_unqueued(tmp_path):
    c,s,aid,m=case(tmp_path)
    try:
        ledger=AttachmentLedger(s);row=ledger.item(aid);data=json.loads(row['data']);data['target']={}
        ledger._write(lambda db:db.execute("UPDATE attachment_tasks SET state='configuration_pending',data=? WHERE attachment_id=?",(json.dumps(data),aid)))
        s.update(row['task_id'],stage='attachment_ready')
        c.attachment_targets={};c.min_free_gib=0;c.max_memory_percent=100;c.cleanup=False
        scheduler=Scheduler(m,c,isolation_root=tmp_path);scheduler.tick()
        assert not m.executions and not scheduler.ledger.rows('SELECT * FROM scheduler_jobs')
        assert scheduler.pressure()['mapping_pending_targets']==1
        assert Path(row['package_dir']).is_dir()
    finally:m.shutdown()

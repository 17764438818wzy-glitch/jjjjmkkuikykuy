import json
import time
from pathlib import Path

import pytest

from paperflow.config import Config
from paperflow.graph import Workflow
from paperflow.intake import Intake
from paperflow.package import sha, seal_package
from paperflow.store import Store, StaleSourceVersion


def test_rag_and_local_dispatches_cannot_consume_repair_turn(tmp_path):
    s=Store(tmp_path/'state.sqlite3')
    for i in range(15): s.register_source(tmp_path/f'{i}.pdf',f'{i:064x}')
    tasks=s.all_tasks(); repair=tasks[-1]
    s.update(repair['task_id'],stage='repair',kind='repair')
    got=[]
    for turn in range(3):
        task=s.claim(['round1','repair'],'llm'); got.append(task['task_id'])
        for stage in ('verify','validate'):
            candidate=next(t for t in s.all_tasks() if t['task_id'] not in got and t['task_id']!=repair['task_id'] and not t.get('owner'))
            s.update(candidate['task_id'],stage=stage)
            s.claim([stage],stage)
    assert repair['task_id'] in got


def test_stale_middle_version_cannot_be_activated(tmp_path):
    s=Store(tmp_path/'state.sqlite3'); p=tmp_path/'paper.pdf'
    a=s.register_source(p,'a'*64); s.mark_active(a['task_id'])
    b=s.register_source(p,'b'*64); c=s.register_source(p,'c'*64)
    assert s.previous_active(b['task_id'])==[a['task_id']]
    assert not s.is_current(b['task_id'])
    with pytest.raises(StaleSourceVersion): s.mark_active(b['task_id'])
    assert s.active_for(p)==a['task_id']
    s.mark_active(c['task_id']); assert s.active_for(p)==c['task_id']


def test_new_native_repaired_ready_enters_validation_without_another_llm(tmp_path,package_factory):
    config=Config(home=str(tmp_path/'runtime'),handoff_root=str(tmp_path/'handoff'),source_roots=[])
    s=Store(config.root/'state.sqlite3'); original=tmp_path/'a.pdf'; original.write_bytes(b'paper')
    task=s.register_source(original,sha(original)); s.update(task['task_id'],stage='parked',error_kind='content')
    root,_,catalog=package_factory('r002',task_id=task['task_id'],source_sha=task['source_sha256'])
    destination=Path(config.handoff_root)/'Updated manuscript'/task['task_id']/'r002'
    destination.parent.mkdir(parents=True); root.rename(destination)
    Intake(config,s).import_handoffs()
    flow=Workflow(config,s,catalog=catalog);flow.apply_pending_requests();flow.close()
    updated=s.get(task['task_id'])
    assert updated['stage']=='validate'
    assert updated['revision']==2
    assert updated['package_dir']==str(destination)


def test_bad_model_json_is_content_repair_not_infrastructure_retry(tmp_path):
    from paperflow.producer import HarnessProducer
    from types import SimpleNamespace
    class Harness:
        def __init__(self,**kwargs): self.path=Path(kwargs['cwd'])
        def start(self): pass
        def close(self): pass
        def run(self,*args,**kwargs):
            (self.path/'profile.json').write_text('{bad JSON',encoding='utf-8')
            return SimpleNamespace(finish_reason='completed',final_response='',session_id='s')
    config=Config(home=str(tmp_path/'runtime'),handoff_root=str(tmp_path/'handoff'),source_roots=[])
    s=Store(config.root/'state.sqlite3'); original=tmp_path/'a.pdf'; original.write_bytes(b'paper')
    task=s.register_source(original,sha(original))
    flow=Workflow(config,s,producer=HarnessProducer(config,factory=Harness),catalog={'majors':[]})
    result=flow.step(task)
    assert result['stage']=='parked'
    assert result['error_kind']=='content'
    assert result['revision']==1 and result['repair_count']==0
    assert not result.get('failure_packet')
    flow.close()


def test_expired_active_lease_cannot_be_claimed_twice(tmp_path):
    s=Store(tmp_path/'state.sqlite3'); task=s.register_source(tmp_path/'a.pdf','a'*64)
    claimed=s.claim(['round1'],'worker',now=10,lease_seconds=1)
    s.heartbeat([task['task_id']],'worker',now=12,lease_seconds=10)
    assert s.claim(['round1'],'other',now=13,exclude_ids=[task['task_id']]) is None
    assert s.claim(['round1'],'other',now=13) is None


def test_changed_repair_rules_reactivate_parked_content(tmp_path):
    config=Config(home=str(tmp_path/'runtime'),handoff_root=str(tmp_path/'handoff'),source_roots=[])
    s=Store(config.root/'state.sqlite3'); task=s.register_source(tmp_path/'a.pdf','a'*64)
    s.update(task['task_id'],stage='parked',kind='repair',error_kind='content',
             rules_fingerprint='previous-rules',stagnant_count=3)
    Intake(config,s).import_handoffs()
    flow=Workflow(config,s,catalog={'majors':[]});flow.apply_pending_requests();flow.close()
    updated=s.get(task['task_id'])
    assert updated['stage']=='repair'
    assert updated['stagnant_count']==0


def test_scanning_own_failure_cannot_unpark_no_progress_task(tmp_path):
    from paperflow.package import save
    config=Config(home=str(tmp_path/'runtime'),handoff_root=str(tmp_path/'handoff'),source_roots=[])
    s=Store(config.root/'state.sqlite3'); task=s.register_source(tmp_path/'a.pdf','a'*64)
    problem=Path(config.handoff_root)/'failed manuscript'/task['task_id']/'r001-error'
    save(problem/'failure.json',dict(task_id=task['task_id'],source_sha256=task['source_sha256'],stage='repair'))
    s.update(task['task_id'],stage='parked',kind='repair',error_kind='content',failure_packet=str(problem),stagnant_count=3)
    Intake(config,s).import_handoffs()
    assert s.get(task['task_id'])['stage']=='parked'


def test_source_change_during_old_deactivation_rolls_back_availability(tmp_path,package_factory):
    config=Config(home=str(tmp_path/'runtime'),handoff_root=str(tmp_path/'handoff'),source_roots=[],cleanup=False)
    s=Store(config.root/'state.sqlite3'); original=tmp_path/'a.pdf'; original.write_bytes(b'old')
    old=s.register_source(original,sha(original));s.mark_active(old['task_id'])
    s.receipt(old['task_id'],1,'all',{'which':'old'})
    original.write_bytes(b'new'); task=s.register_source(original,sha(original))
    root,_,catalog=package_factory(task_id=task['task_id'],source_sha=task['source_sha256'])
    task=s.update(task['task_id'],stage='activate',package_dir=str(root),manifest_sha256=sha(root/'manifest.json'))
    s.receipt(task['task_id'],1,'all',{'which':'new'})
    states={'old':True,'new':False}
    class Publisher:
        def set_enabled(self,receipts,enabled):
            states[receipts['which']]=enabled
            if receipts['which']=='old' and not enabled:
                s.register_source(original,'c'*64)
        def verify_retrieval(self,*args):pass
    flow=Workflow(config,s,publisher=Publisher(),catalog=catalog)
    updated=flow.step(task)
    assert updated['stage']=='superseded'
    assert states=={'old':True,'new':False}
    assert s.active_for(original)==old['task_id']
    flow.close()


@pytest.mark.parametrize('successor_first',[False,True])
def test_crash_during_switch_is_compensated_before_stale_task_exits(tmp_path,package_factory,successor_first):
    config=Config(home=str(tmp_path/'runtime'),handoff_root=str(tmp_path/'handoff'),source_roots=[],cleanup=False)
    s=Store(config.root/'state.sqlite3');original=tmp_path/'paper.pdf';original.write_bytes(b'old')
    old=s.register_source(original,sha(original));s.mark_active(old['task_id'])
    s.receipt(old['task_id'],1,'all',{'which':'old'})
    original.write_bytes(b'new');task=s.register_source(original,sha(original))
    root,_,catalog=package_factory(task_id=task['task_id'],source_sha=task['source_sha256'])
    task=s.update(task['task_id'],stage='activate',package_dir=str(root),manifest_sha256=sha(root/'manifest.json'))
    s.receipt(task['task_id'],1,'all',{'which':'new'})
    states={'old':True,'new':False}
    class Crash(BaseException):pass
    class Publisher:
        crash=True
        def set_enabled(self,receipts,enabled):
            states[receipts['which']]=enabled
            if receipts['which']=='old' and not enabled and self.crash:
                self.crash=False
                raise Crash()
        def verify_retrieval(self,*args):pass
    publisher=Publisher();flow=Workflow(config,s,publisher=publisher,catalog=catalog)
    with pytest.raises(Crash):flow.step(task)
    flow.close();successor=s.register_source(original,'c'*64)
    flow=Workflow(config,s,publisher=publisher,catalog=catalog)
    if successor_first:
        successor_root,_,_=package_factory('successor',task_id=successor['task_id'],source_sha=successor['source_sha256'])
        successor=s.update(successor['task_id'],stage='activate',package_dir=str(successor_root),manifest_sha256=sha(successor_root/'manifest.json'))
        s.receipt(successor['task_id'],1,'all',{'which':'successor'})
        states['successor']=False
        assert flow.step(successor)['stage']=='cleanup'
    updated=flow.step(s.get(task['task_id']))
    assert updated['stage']=='superseded'
    assert states==({'old':False,'new':False,'successor':True} if successor_first else {'old':True,'new':False})
    assert s.active_for(original)==(successor['task_id'] if successor_first else old['task_id'])
    flow.close()


def test_bad_external_failure_does_not_abort_other_intake(tmp_path):
    config=Config(home=str(tmp_path/'runtime'),handoff_root=str(tmp_path/'handoff'),source_roots=[])
    s=Store(config.root/'state.sqlite3');task=s.register_source(tmp_path/'a.pdf','a'*64)
    failure=Path(config.handoff_root)/'failed manuscript'/task['task_id']/'external'/'failure.json'
    failure.parent.mkdir(parents=True);failure.write_text('{unfinished',encoding='utf-8')
    result=Intake(config,s).import_handoffs()
    assert result['errors']==1
    assert s.get(task['task_id'])['stage']=='round1'


def test_concurrent_new_version_cannot_race_stale_rollback(tmp_path,package_factory):
    import threading
    from concurrent.futures import ThreadPoolExecutor
    config=Config(home=str(tmp_path/'runtime'),handoff_root=str(tmp_path/'handoff'),source_roots=[],cleanup=False)
    s=Store(config.root/'state.sqlite3');original=tmp_path/'paper.pdf';original.write_bytes(b'a')
    a=s.register_source(original,sha(original));s.mark_active(a['task_id']);s.receipt(a['task_id'],1,'all',{'which':'a'})
    original.write_bytes(b'b');b=s.register_source(original,sha(original))
    root,_,catalog=package_factory('b',task_id=b['task_id'],source_sha=b['source_sha256'])
    b=s.update(b['task_id'],stage='activate',package_dir=str(root),manifest_sha256=sha(root/'manifest.json'))
    s.receipt(b['task_id'],1,'all',{'which':'b'})
    entered=threading.Event();release=threading.Event();c_started=threading.Event()
    states={'a':True,'b':False,'c':False}
    class Publisher:
        def set_enabled(self,receipt,enabled):
            key=receipt['which'];states[key]=enabled
            if key=='a' and not enabled and not entered.is_set():
                entered.set();assert release.wait(5)
            if key=='c' and enabled:c_started.set()
        def verify_retrieval(self,*args):pass
    flow=Workflow(config,s,publisher=Publisher(),catalog=catalog)
    with ThreadPoolExecutor(max_workers=2) as pool:
        b_job=pool.submit(flow.step,b);assert entered.wait(5)
        original.write_bytes(b'c');c=s.register_source(original,sha(original))
        root,_,_=package_factory('c',task_id=c['task_id'],source_sha=c['source_sha256'])
        c=s.update(c['task_id'],stage='activate',package_dir=str(root),manifest_sha256=sha(root/'manifest.json'))
        s.receipt(c['task_id'],1,'all',{'which':'c'})
        c_job=pool.submit(flow.step,c)
        started_early=c_started.wait(.15)
        release.set()
        b_result=b_job.result();c_result=c_job.result()
    assert not started_early
    assert b_result['stage']=='superseded' and c_result['stage']=='cleanup'
    assert states=={'a':False,'b':False,'c':True}
    assert s.active_for(original)==c['task_id']
    flow.close()


def test_failed_activation_never_enables_unverified_previous_revision(tmp_path,package_factory):
    config=Config(home=str(tmp_path/'runtime'),handoff_root=str(tmp_path/'handoff'),source_roots=[],cleanup=False)
    s=Store(config.root/'state.sqlite3');original=tmp_path/'a.pdf';original.write_bytes(b'paper')
    task=s.register_source(original,sha(original))
    root,_,catalog=package_factory(task_id=task['task_id'],source_sha=task['source_sha256'])
    task=s.update(task['task_id'],stage='activate',revision=2,package_dir=str(root),manifest_sha256=sha(root/'manifest.json'))
    s.receipt(task['task_id'],1,'all',{'which':'partial'})
    s.receipt(task['task_id'],2,'all',{'which':'new'})
    states={'partial':False,'new':False}
    class Publisher:
        def set_enabled(self,receipt,enabled):states[receipt['which']]=enabled
        def verify_retrieval(self,*args):raise TimeoutError('retrieval not ready')
    flow=Workflow(config,s,publisher=Publisher(),catalog=catalog)
    updated=flow.step(task)
    assert updated['stage']=='activate' and updated['error_kind']=='infrastructure'
    assert states=={'partial':False,'new':False}
    assert s.active_for(original) is None
    flow.close()

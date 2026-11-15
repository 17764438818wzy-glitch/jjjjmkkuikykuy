from pathlib import Path
import pytest

from paperflow.config import Config
from paperflow.graph import Workflow
from paperflow.package import PackageError,load,save,sha
from paperflow.store import Store


def flow_at(tmp_path,producer,**options):
    c=Config(home=str(tmp_path/'runtime'),handoff_root=str(tmp_path/'handoff'),source_roots=[],retry_seconds=0,**options)
    s=Store(c.root/'state.sqlite3');original=tmp_path/'original.txt';original.write_text('paper',encoding='utf-8')
    task=s.register_source(original,sha(original))
    return Workflow(c,s,producer=producer,catalog={'majors':[]}),s,task


def test_round_one_repair_failure_preserves_original_business_stage(tmp_path):
    class Producer:
        calls=[]
        def run_stage(self,task,stage):
            self.calls.append(stage)
            if len(self.calls)<3:raise PackageError('bad artifact')
            return dict(disposition='keep',draft_dir=str(tmp_path/'draft'))
    p=Producer();flow,s,task=flow_at(tmp_path,p)
    task=flow.step(task);task=flow.step(task)
    # 旧台账字段不改变业务原点；第二次定点补交成功后走原点的后继。
    task=s.update(task['task_id'],failed_stage='repair')
    task=flow.step(task)
    assert task['stage']=='round2'
    assert load(Path(task['failure_packet'])/'failure.json')['business_stage']=='round1'
    assert p.calls==['round1','repair','repair']
    flow.close()


def test_unknown_legacy_repair_origin_conservatively_returns_to_round_two(tmp_path):
    class Producer:
        def run_stage(self,*args):return dict(disposition='keep',draft_dir=str(tmp_path/'draft'))
    flow,s,task=flow_at(tmp_path,Producer())
    task=s.update(task['task_id'],stage='repair',failed_stage='repair')
    task=flow.step(task);assert task['stage']=='round2'
    assert flow.step(task)['stage']=='round3'
    flow.close()


@pytest.mark.parametrize('error',[KeyError('bug'),TypeError('bug'),AttributeError('bug'),ValueError('config')])
def test_controller_errors_stop_one_task_and_write_private_trace(tmp_path,error):
    class Producer:
        def run_stage(self,*args):raise error
    flow,s,task=flow_at(tmp_path,Producer())
    result=flow.step(task)
    assert result['stage']=='code_error' and result['error_kind']=='controller'
    assert result['repair_count']==0
    assert Path(result['traceback_private']).is_file()
    assert 'bug' not in result['error']
    flow.close()


def test_infrastructure_retries_are_bounded(tmp_path):
    class Producer:
        def run_stage(self,*args):raise TimeoutError('offline')
    flow,s,task=flow_at(tmp_path,Producer(),max_infrastructure_retries=2)
    task=flow.step(task);assert task['stage']=='round1'
    task=flow.step(task);assert task['stage']=='parked'
    assert task['error_kind']=='infrastructure' and task['repair_count']==0
    flow.close()


def test_concurrent_intake_change_is_not_overwritten_by_model_result(tmp_path):
    class Producer:
        def run_stage(self,task,stage):
            s.update(task['task_id'],external_note='new intake decision')
            s.request(task['task_id'],stage='held')
            return dict(disposition='keep',draft_dir=str(tmp_path/'draft'))
    flow,s,task=flow_at(tmp_path,Producer())
    updated=flow.step(task)
    updated=flow.step(updated)
    assert updated['stage']=='held' and updated['external_note']=='new intake decision'
    # A graph control request pauses without discarding the committed result or the new note.
    assert flow.step(updated)['stage']=='held'
    flow.close()


def test_acceptance_migration_is_idempotent_and_preserves_completed_round_two(tmp_path):
    from paperflow.migrations import migrate_acceptance
    class Producer:pass
    flow,s,task=flow_at(tmp_path,Producer())
    draft=flow.config.root/'work'/task['task_id']/'r005/round2';draft.mkdir(parents=True)
    marker=draft/'corrections.json';marker.write_text('{"operations":[]}',encoding='utf-8')
    task=s.update(task['task_id'],stage='repair',failed_stage='repair',revision=5,
                  error='unresolved model processing issues',draft_dir=str(draft))
    result=migrate_acceptance(flow.config,s,task['task_id'])
    assert result['stage']=='round2' and result['failed_stage']=='round1'
    assert marker.read_text(encoding='utf-8')=='{"operations":[]}'
    assert migrate_acceptance(flow.config,s,task['task_id'])['version']==result['version']
    flow.close()


def test_token_usage_counts_assistant_usage_once_without_stream_duplicates():
    from paperflow.provenance import usage_totals
    usage=dict(inputTokens=10,outputTokens=2,totalTokens=12)
    events=[dict(type='assistant/message',data=dict(usage=usage,stream=[dict(chunk=dict(usage=usage))])),
            dict(type='tool/result',data=dict(usage=usage))]
    assert usage_totals(events)['totalTokens']==12
    assert usage_totals([])['inputTokens'] is None


def test_manifest_contains_runtime_processing_history(tmp_path,package_factory):
    from paperflow.package import seal_package
    root,task,catalog=package_factory()
    task['processing_history']=[dict(stage='round1',model='test',provider='test',prompt_sha256='a'*64)]
    seal_package(root,task,1)
    assert load(root/'manifest.json')['processing_history']==task['processing_history']


def test_service_outer_code_failure_is_terminal_and_has_private_trace(tmp_path):
    import time
    from paperflow.service import Service
    class BrokenWorkflow:
        calls=0
        def step(self,task):self.calls+=1;raise KeyError('bug outside graph')
        def close(self):pass
    c=Config(home=str(tmp_path/'runtime'),handoff_root=str(tmp_path/'handoff'),source_roots=[],min_free_gib=0,max_memory_percent=101)
    broken=BrokenWorkflow();service=Service(c,workflow_factory=lambda *_:broken)
    task=service.store.register_source(tmp_path/'a.txt','a'*64)
    service.tick()
    for future in list(service.futures):
        try:future.result(timeout=3)
        except KeyError:pass
    service.tick();service.tick()
    result=service.store.get(task['task_id'])
    assert result['stage']=='code_error' and broken.calls==1
    assert Path(result['traceback_private']).is_file()
    service.close()


def test_conflicting_worker_releases_only_its_own_lease(tmp_path):
    class Producer:
        def run_stage(self,task,stage):
            s.update(task['task_id'],external_note='new note')
            return dict(disposition='keep',draft_dir=str(tmp_path/'draft'))
    flow,s,task=flow_at(tmp_path,Producer())
    claimed=s.claim(['round1'],'worker')
    result=flow.step(claimed)
    assert result['owner'] is None and result['external_note']=='new note'
    assert s.claim(['round1'],'other') is not None
    flow.close()


def test_harness_credential_homes_are_fixed_worker_slots(tmp_path,monkeypatch):
    from types import SimpleNamespace
    import paperflow.producer as module
    c=Config(home=str(tmp_path/'runtime'),llm_workers=2)
    home=tmp_path/'user';(home/'.dsh').mkdir(parents=True)
    (home/'.dsh/.credentials.yaml').write_text('test-only-copy',encoding='utf-8')
    monkeypatch.setattr(Path,'home',classmethod(lambda cls:home))
    producer=module.HarnessProducer(c)
    monkeypatch.setattr(module.threading,'current_thread',lambda:SimpleNamespace(name='paperflow-llm_0'))
    first=producer._home()
    monkeypatch.setattr(module.threading,'current_thread',lambda:SimpleNamespace(name='paperflow-llm_1'))
    second=producer._home()
    assert first.name=='harness-0' and second.name=='harness-1'
    assert len(list((c.root/'private').glob('harness-*')))==2


def test_cleanup_removes_only_inactive_owned_worker_homes(tmp_path):
    from paperflow.producer import cleanup_worker_homes
    c=Config(home=str(tmp_path/'runtime'),llm_workers=1)
    old=c.root/'private/harness-999';old.mkdir(parents=True)
    (old/'copy').write_text('test',encoding='utf-8')
    active=c.root/'private/harness-0';active.mkdir()
    cleanup_worker_homes(c)
    assert not old.exists() and active.is_dir()


def test_old_database_sources_backfill_version_links(tmp_path):
    path=tmp_path/'state.sqlite3';s=Store(path)
    task=s.register_source(tmp_path/'a.txt','a'*64);s.mark_active(task['task_id'])
    with s.db() as db:db.execute('DELETE FROM source_versions')
    restored=Store(path)
    assert restored.active_versions_for(task['task_id'])==[dict(task_id=task['task_id'],revision=1)]

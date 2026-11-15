"""Durable final outcomes and precise action recovery decide queued retries."""
from copy import deepcopy
import json

import pytest

from test_scheduler import scheduler_case,source
from test_scheduling_store import TRANSIENT


RESOURCE=dict(operation='http',method='GET',endpoint='/datasets/a/documents/b',
    object_manifest='v1',body_sha256='hash1',identity=dict(source_id='article',specialty_code='A',generation=1))


def action(state='diagnostic',failure=True,*,kind='rag_read',resource=None,generation=1):
    return dict(kind=kind,state=state,request=dict(kind=kind,resource=deepcopy(RESOURCE if resource is None else resource),
        expected_generation=generation),result=dict(fault=TRANSIENT) if failure else dict(response_received=True))


def reap_case(case,tmp_path,facts,reported=None,report_data=None,exit_evidence=None,unknown_mode=None):
    m,c,s=case;task=source(case,tmp_path,'article');s.discover()
    attempt=s.ledger.claim();assert attempt
    data=report_data or dict(reason='final decision')
    with m.store.db() as db:
        db.execute("INSERT INTO control_executions(execution_id,epoch,unit_kind,unit_key,job_name,state,lease_until,last_heartbeat,deadline,exit_evidence) VALUES(?,?,?,?,?,'exited',0,0,100,?)",
            ('ex',m.epoch,'graph',task['task_id'],'synthetic',json.dumps(exit_evidence or {})))
        db.execute("UPDATE scheduler_attempts SET execution_id='ex',state='running' WHERE attempt_id=?",(attempt['attempt_id'],))
        for i,fact in enumerate(facts):
            db.execute('INSERT INTO control_actions VALUES(?,?,?,?,?,?,?,?,?,?)',
                (str(i),m.epoch,str(i),'ex',fact['kind'],json.dumps(fact['request']),'{}',fact['state'],100,
                 json.dumps(fact['result']) if fact['result'] is not None else None))
        if reported:db.execute('INSERT INTO control_execution_results VALUES(?,?,?,?,10)',('ex',m.epoch,reported,json.dumps(data)))
    if unknown_mode=='report':m.record_result('ex','1',m.epoch,'unknown',dict(fault=TRANSIENT))
    if unknown_mode=='exit':m.ledger.confirm_exit('ex',dict(all_recorded_processes_exited=True))
    if unknown_mode:
        fact=m.ledger.rows("SELECT state,result FROM control_actions WHERE action_id='1'")[0]
        assert fact['state']=='unresolved'
        assert (fact['result'] is None)==(unknown_mode=='exit')
    s.reap()
    result=s.ledger.rows('SELECT * FROM scheduler_attempts')[0]
    return s.ledger.job(attempt['job_id']),s.ledger.budget(attempt['job_id']),result


@pytest.mark.parametrize('reported',['held','blocked','stopped','complete'])
@pytest.mark.parametrize('recovered',[False,True])
def test_explicit_final_decision_is_not_overwritten(scheduler_case,tmp_path,reported,recovered):
    facts=[action()]+([action('complete',False)] if recovered else [])
    final=dict(fault=dict(category='controller'),reason='final content, code or unresolved decision')
    job,budget,result=reap_case(scheduler_case,tmp_path,facts,reported,final)
    assert result['outcome']==('held' if reported=='complete' else reported)
    assert job['state']==('retry' if reported=='stopped' else 'held' if reported=='complete' else reported)
    assert budget['rounds']==0
    if reported!='complete':assert json.loads(result['data'])==final


@pytest.mark.parametrize('reported',[None,'deferred'])
def test_exact_action_success_clears_earlier_failure_for_inference(scheduler_case,tmp_path,reported):
    job,budget,result=reap_case(scheduler_case,tmp_path,[action(),action('complete',False)],reported)
    assert result['outcome']==('held' if reported is None else 'deferred')
    assert budget['rounds']==0


@pytest.mark.parametrize('reported',[None,'deferred'])
def test_unrecovered_failure_still_counts_one_infrastructure_round(scheduler_case,tmp_path,reported):
    job,budget,result=reap_case(scheduler_case,tmp_path,[action()],reported,
        dict(fault=dict(category='deferred',reason='resource_busy')))
    assert job['state']=='retry' and result['outcome']=='failed' and budget['rounds']==1


@pytest.mark.parametrize('changed',['kind','operation','endpoint','object_manifest','body_sha256','specialty','generation','admission_generation'])
def test_other_action_success_does_not_clear_failed_operation(scheduler_case,tmp_path,changed):
    success=action('complete',False)
    if changed=='kind':success['kind']=success['request']['kind']='rag_write'
    elif changed=='admission_generation':success['request']['expected_generation']=2
    elif changed in ('specialty','generation'):
        success['request']['resource']['identity']['specialty_code' if changed=='specialty' else 'generation']='different'
    else:success['request']['resource'][changed]='different'
    job,budget,result=reap_case(scheduler_case,tmp_path,[action(),success])
    assert result['outcome']=='failed' and budget['rounds']==1


def test_ambiguous_historical_resource_cannot_prove_recovery(scheduler_case,tmp_path):
    old=action();old['request']={}
    success=action('complete',False);success['request']={}
    job,budget,result=reap_case(scheduler_case,tmp_path,[old,success])
    assert result['outcome']=='failed' and budget['rounds']==1


@pytest.mark.parametrize('unknown_mode',['report','exit'])
@pytest.mark.parametrize('reported',[None,'deferred'])
@pytest.mark.parametrize('later_success',[False,True])
def test_unresolved_write_blocks_failure_inference(scheduler_case,tmp_path,unknown_mode,reported,later_success):
    pending=action('possible',kind='rag_write');pending['result']=None
    facts=[action(),pending]+([action('complete',False,kind='rag_write')] if later_success else [])
    job,budget,result=reap_case(scheduler_case,tmp_path,facts,reported,unknown_mode=unknown_mode)
    assert result['outcome']==('held' if reported is None else 'deferred') and budget['rounds']==0
    assert scheduler_case[0].ledger.rows("SELECT state FROM control_actions WHERE action_id='1'")[0]['state']=='unresolved'


def test_later_program_error_without_final_report_stays_held(scheduler_case,tmp_path):
    later=action();later['result']=dict(fault=dict(category='controller'),error='TypeError')
    job,budget,result=reap_case(scheduler_case,tmp_path,[action(),later])
    assert result['outcome']=='held' and budget['rounds']==0


def test_control_stop_without_final_report_does_not_charge_old_failure(scheduler_case,tmp_path):
    job,budget,result=reap_case(scheduler_case,tmp_path,[action()],exit_evidence=dict(stop_at=10))
    assert result['outcome']=='stopped' and budget['rounds']==0


@pytest.mark.parametrize('later_stage',['round1:0','round2:0'])
def test_model_recovery_is_scoped_to_the_same_stage(scheduler_case,tmp_path,later_stage):
    facts=[action(kind='llm',resource=dict(operation='round1:0'),generation=0),
        action('complete',False,kind='llm',resource=dict(operation=later_stage),generation=0)]
    job,budget,result=reap_case(scheduler_case,tmp_path,facts)
    assert budget['rounds']==(0 if later_stage=='round1:0' else 1)

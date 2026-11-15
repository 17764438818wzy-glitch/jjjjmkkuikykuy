"""Real producer error extraction and durable service admission, without an upstream call."""
import json
import time

import pytest

from paperflow.producer import model_failure
from paperflow.scheduling_policy import fault
from test_service_control import service_control,permit
from test_control import start_worker


def rejection(code=11128):
    body={'error':{'message':'WorkBuddy 渠道校验未通过（11128）',
                   'code':'workbuddy_channel_rejected','upstream_code':code,
                   'upstream_request_id':'45722682-4260-4b33-8ce7-389f1502c3d4',
                   'retryable':False,'accessToken':'<REDACTED_SECRET>','prompt':'PRIVATE_MANUSCRIPT'}}
    event={'type':'turn/end','data':{'reason':{'error':{
        'code':'INVALID_REQUEST','message':'400 '+json.dumps(body,ensure_ascii=False)}}}}
    return fault(model_failure([event]))


def submit(m,ex,label,failure):
    ticket=permit(m,ex,label)
    m.record_result(ex.execution_id,ticket,m.epoch,'diagnostic',{'fault':failure})
    return ticket


def test_vendor_reason_survives_the_actual_sdk_message_without_private_content():
    result=rejection()
    assert result['status']==400
    assert result['category']=='held'  # Never send a deterministic refusal to content repair / network retry.
    assert result['model_rejection'] is True
    assert result['upstream_code']==11128 and result['retryable'] is False
    assert result['reason']=='channel_verification_failed'
    assert result['upstream_request_id']=='45722682-4260-4b33-8ce7-389f1502c3d4'
    assert 'SECRET_FIXTURE' not in json.dumps(result) and 'PRIVATE_MANUSCRIPT' not in json.dumps(result)


@pytest.mark.parametrize('payload',[
    {'code':11128,'msg':'Illegal API invocation from an unapproved channel'},
    {'error':{'code':'workbuddy_channel_rejected','upstream_code':11128,'message':'channel verification failed'}}
])
def test_original_and_bridge_error_envelopes_have_the_same_nonretryable_reason(payload):
    event={'type':'turn/end','data':{'reason':{'error':{'message':'HTTP 400: '+json.dumps(payload)}}}}
    result=fault(model_failure([event]))
    assert result['upstream_code']==11128 and result['reason']=='channel_verification_failed'
    assert result['retryable'] is False


@pytest.mark.parametrize('wrapped',[False,True])
def test_sdk_message_only_errors_keep_the_channel_reason(wrapped):
    message='WorkBuddy 渠道校验未通过（11128）：请使用供应商支持的客户端或接口。'
    text=json.dumps({'error':{'message':message,'type':'invalid_request_error'}},ensure_ascii=False) if wrapped else message
    event={'type':'turn/end','data':{'reason':{'error':{'message':'400 '+text}}}}
    result=fault(model_failure([event]))
    assert result['upstream_code']==11128
    assert result['reason']=='channel_verification_failed' and result['retryable'] is False


def test_three_rejected_model_actions_close_only_model_admission_and_require_operator_resume(service_control,tmp_path):
    m,service=service_control
    workers=[start_worker(m,tmp_path,name=name)[0] for name in ('first','second','third','fourth')]
    for i,worker in enumerate(workers[:3]):
        submit(m,worker,'rejected-'+str(i),rejection())
        assert service.services['llm']['state']==('open' if i==2 else 'closed')
    assert service.services['rag']['state']=='closed'
    assert not m.stopped.is_set()  # Already-issued RAG writes are not cancelled by this guard.
    with pytest.raises(ValueError):
        m.request_action(workers[3].execution_id,'must-not-send','llm',{},0,30)
    assert service.open_due(now=time.time()+100000)==[]
    assert len(service.ledger.rows('SELECT * FROM scheduler_observations'))==3
    assert len(service.ledger.rows('SELECT * FROM scheduler_events'))==1
    assert m.ledger.rows("SELECT * FROM control_actions WHERE kind='rag_write'")==[]


def test_success_breaks_the_rejection_streak_and_duplicate_report_does_not_increment_it(service_control,tmp_path):
    m,service=service_control
    worker,_=start_worker(m,tmp_path,name='sequence')
    failure=rejection()
    ticket=submit(m,worker,'first',failure)
    m.record_result(worker.execution_id,ticket,m.epoch,'diagnostic',{'fault':failure})
    submit(m,worker,'second',failure)
    success=permit(m,worker,'success')
    m.record_result(worker.execution_id,success,m.epoch,'complete',{})
    submit(m,worker,'after-success-1',failure)
    submit(m,worker,'after-success-2',failure)
    assert service.services['llm']['state']=='closed'
    submit(m,worker,'after-success-3',failure)
    assert service.services['llm']['state']=='open'


def test_generic_http_400_is_counted_but_local_validation_failure_is_not(service_control,tmp_path):
    m,service=service_control
    worker,_=start_worker(m,tmp_path,name='plain')
    for i in range(4):submit(m,worker,'local-'+str(i),fault(ValueError('local manuscript validation')))
    assert service.services['llm']['state']=='closed'
    event={'type':'turn/end','data':{'reason':{'error':{'message':'400 Upstream rejected the request'}}}}
    for i in range(3):submit(m,worker,'http-'+str(i),fault(model_failure([event])))
    assert service.services['llm']['state']=='open'


def test_reboot_does_not_resume_a_rejected_service_but_explicit_resume_allows_one_probe(service_control,tmp_path):
    from paperflow.control import Manager
    from paperflow.service_control import ServiceControl
    m,service=service_control
    worker,_=start_worker(m,tmp_path,name='restart')
    for i in range(3):submit(m,worker,'http-'+str(i),rejection())
    incident=service.services['llm']['event_id']
    m.shutdown()
    reopened=Manager(m.store,m.root).start()
    try:
        new=ServiceControl(reopened,service.config)
        assert reopened.stopped.is_set()
        assert new.services['llm']['event_id']==incident
        assert new.open_due(now=time.time()+100000)==[]
        reopened.resume()
        assert new.open_due()==['llm']
        assert new.services['llm']['state']=='half_open'
        assert new.open_due()==[]
    finally:reopened.shutdown()


def test_late_success_from_an_inflight_call_does_not_reopen_the_blocked_service(service_control,tmp_path):
    m,service=service_control
    early,_=start_worker(m,tmp_path,name='early')
    late,_=start_worker(m,tmp_path,name='late')
    late_ticket=permit(m,late,'in-flight')
    for i in range(3):submit(m,early,'http-'+str(i),rejection())
    m.record_result(late.execution_id,late_ticket,m.epoch,'complete',{})
    assert service.services['llm']['state']=='open'
    assert service.open_due(now=time.time()+100000)==[]


def test_execution_failure_after_a_successful_action_is_still_recorded(service_control,tmp_path):
    from test_scheduling_store import TRANSIENT
    m,service=service_control
    worker,_=start_worker(m,tmp_path,name='successful-then-transport')
    worker.scheduler_job={'kind':'produce','article':'article-transport','attempt_id':None,'job_id':None}
    ticket=permit(m,worker,'successful-model')
    m.record_result(worker.execution_id,ticket,m.epoch,'complete',{})
    saved=[]
    service.record_execution(worker.execution_id,{'fault':TRANSIENT},lambda _c:saved.append(True))
    observations=service.ledger.rows('SELECT category FROM scheduler_observations ORDER BY rowid')
    assert [row['category'] for row in observations]==['success','transient']
    assert saved==[True]

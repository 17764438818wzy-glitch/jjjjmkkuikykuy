from concurrent.futures import ThreadPoolExecutor

import pytest

from paperflow.store import Store


@pytest.fixture
def queue(tmp_path):
    from paperflow.scheduling_store import SchedulingLedger
    ledger=SchedulingLedger(Store(tmp_path/'ledger.sqlite3'))
    service=ledger.ensure_service('rag',{'endpoint':'http://127.0.0.1:19876','revision':'fixture'})
    ledger.session('epoch',True,now=0)
    return ledger,service['service_id']


def add(ledger,service,article,code='one',kind='delivery',now=0):
    return ledger.enqueue(kind,article,article+':'+code,{'manifest':article,'code':code},service,now=now)


TRANSIENT=dict(category='transient',family='unavailable',status=503,retry_after=0)


def test_two_independent_articles_form_one_event_and_reattribute_early_debt(queue):
    ledger,service=queue;a=add(ledger,service,'a');add(ledger,service,'b')
    attempt=ledger.claim(now=1);ledger.session('epoch',True,now=10)
    ledger.observe(service,'a1','a',TRANSIENT,now=10,attempt_id=attempt['attempt_id'],job_id=a)
    ledger.finish(attempt['attempt_id'],'failed',{'fault':TRANSIENT},now=10,max_rounds=1)
    assert ledger.job(a)['state']=='held_budget' and ledger.budget(a,now=10)['rounds']==1
    ledger.observe(service,'a2','a',TRANSIENT,now=10.5)
    assert ledger.service(service)['state']=='closed'
    ledger.observe(service,'b1','b',TRANSIENT,now=11)
    assert ledger.service(service)['state']=='open'
    assert ledger.budget(a,now=11)=={'rounds':0,'seconds':0}
    assert ledger.job(a)['state']=='blocked_service'
    ledger.observe(service,'b1','b',TRANSIENT,now=12)
    assert len(ledger.rows('SELECT * FROM scheduler_events'))==1


def test_outage_24_hours_single_probe_then_fixed_recovery(queue):
    ledger,service=queue
    for article in ('a','b','c'):add(ledger,service,article)
    for index,article in enumerate(('a','b')):ledger.observe(service,article,article,TRANSIENT,now=10+index)
    assert ledger.claim(now=10000) is None
    ledger.open_probe(service,now=90000)
    with ThreadPoolExecutor(3) as pool:claimed=list(pool.map(lambda _:ledger.claim(now=90000),range(3)))
    assert sum(item is not None for item in claimed)==1
    probe=next(item for item in claimed if item)
    ledger.observe(service,'old-success','other',None,now=90000.5,job_id='unrelated')
    assert ledger.service(service)['state']=='half_open'
    ledger.observe(service,'probe-ok',probe['article'],None,now=90001,job_id=probe['job_id'])
    assert ledger.service(service)['state']=='closed'
    assert all(ledger.budget(row['job_id'],now=90001)['rounds']==0 for row in ledger.rows('SELECT job_id FROM scheduler_jobs'))
    assert ledger.claim(now=90002) is not None


def test_probe_failure_extends_same_event_and_configuration_is_service_scoped(queue):
    ledger,service=queue
    add(ledger,service,'a')
    ledger.observe(service,'auth','a',dict(TRANSIENT,category='configuration',status=401),now=5)
    first=ledger.service(service);assert first['state']=='open'
    ledger.open_probe(service,now=first['next_probe']);probe=ledger.claim(now=first['next_probe'])
    ledger.observe(service,'probe-fail','a',TRANSIENT,now=30,job_id=probe['job_id'])
    second=ledger.service(service)
    assert second['event_id']==first['event_id'] and second['next_probe']>=62


def test_content_and_identity_faults_never_open_service(queue):
    ledger,service=queue
    for category in ('conflict','held','controller','deferred','stopped'):
        ledger.observe(service,category,category,dict(TRANSIENT,category=category),now=3)
    assert ledger.service(service)['state']=='closed'
    assert not ledger.rows('SELECT * FROM scheduler_events')


def test_pause_offline_and_service_time_are_excluded_from_effective_failure_clock(queue):
    ledger,service=queue;job=add(ledger,service,'a');attempt=ledger.claim(now=1)
    ledger.session('epoch',True,now=10)
    ledger.finish(attempt['attempt_id'],'failed',{'fault':TRANSIENT},now=10)
    ledger.session('epoch',False,now=20)
    ledger.session('epoch',True,now=100000);ledger.session('epoch',True,now=100010)
    assert ledger.budget(job,now=100010)==dict(rounds=1,seconds=20)
    ledger.boot('new-epoch',now=200000)
    ledger.session('new-epoch',True,now=200010);ledger.session('new-epoch',True,now=200020)
    assert ledger.budget(job,now=200020)==dict(rounds=1,seconds=30)


def test_retry_rounds_are_durable_idempotent_and_do_not_touch_corrections(queue):
    ledger,service=queue;job=add(ledger,service,'a')
    for n in range(20):
        attempt=ledger.claim(now=n*1000+1)
        assert attempt is not None
        details={'fault':dict(TRANSIENT,retry_after=40)}
        ledger.finish(attempt['attempt_id'],'failed',details,now=n*1000+2)
        ledger.finish(attempt['attempt_id'],'failed',details,now=n*1000+3)
    assert ledger.job(job)['state']=='held_budget'
    assert ledger.budget(job,now=20000)['rounds']==20
    assert ledger.claim(now=9999999) is None


def test_articles_and_kinds_rotate_and_new_arrivals_cannot_starve_existing(queue):
    ledger,service=queue
    for code in range(5):add(ledger,service,'a',str(code))
    add(ledger,service,'b');add(ledger,service,'r',kind='repair');add(ledger,service,'n',kind='graph')
    seen=[]
    for n in range(8):
        attempt=ledger.claim(now=n+1);seen.append((attempt['kind'],attempt['article']))
        ledger.finish(attempt['attempt_id'],'complete',{},now=n+1)
        add(ledger,service,'incoming'+str(n),now=n+1)
    assert {kind for kind,article in seen[:3]}=={'delivery','repair','graph'}
    delivery=[a for k,a in seen if k=='delivery']
    assert delivery[:3]==['a','b','a']


def test_queue_claim_is_unique_by_unit_across_threads_and_replay(queue):
    ledger,service=queue;job=add(ledger,service,'a')
    assert add(ledger,service,'a')==job
    with ThreadPoolExecutor(4) as pool:attempts=list(pool.map(lambda _:ledger.claim(now=1),range(4)))
    assert sum(attempt is not None for attempt in attempts)==1
    ledger.enqueue('repair','a','a:one',{'request':'new'},service,now=1)
    assert ledger.claim(now=2) is None


def test_effective_time_limit_stops_before_next_execution(queue):
    ledger,service=queue;job=add(ledger,service,'a');attempt=ledger.claim(now=1)
    ledger.finish(attempt['attempt_id'],'failed',{'fault':TRANSIENT},now=2,max_seconds=100)
    ledger.session('epoch',True,now=1000)
    assert ledger.claim(now=1000) is None
    assert ledger.job(job)['state']=='held_budget'


def test_success_stops_failure_clock_across_new_versions(queue):
    ledger,service=queue;job=add(ledger,service,'a');attempt=ledger.claim(now=1)
    ledger.finish(attempt['attempt_id'],'failed',{'fault':TRANSIENT},now=2)
    attempt=ledger.claim(now=50);ledger.finish(attempt['attempt_id'],'complete',{},now=60)
    ledger.session('epoch',True,now=200000)
    next_job=ledger.enqueue('delivery','a','a:one',{'manifest':'new'},service,now=200000)
    assert ledger.budget(next_job,now=200000)==dict(rounds=1,seconds=58)
    assert ledger.claim(now=200000)['job_id']==next_job


def test_closed_incident_observations_cannot_confirm_a_new_incident(queue):
    ledger,service=queue
    ledger.observe(service,'a1','a',TRANSIENT,now=1)
    ledger.observe(service,'b1','b',TRANSIENT,now=2)
    first=ledger.service(service)['event_id']
    ledger.open_probe(service,now=30)
    job=add(ledger,service,'a');ledger.claim(now=30)
    ledger.observe(service,'ok','a',None,now=31,job_id=job)
    ledger.observe(service,'a2','a',TRANSIENT,now=32)
    assert ledger.service(service)['state']=='closed'
    ledger.observe(service,'c1','c',TRANSIENT,now=33)
    second=ledger.service(service)['event_id'];assert first!=second
    assert ledger.rows('SELECT started FROM scheduler_events WHERE event_id=?',(second,))[0]['started']==32
    assert ledger.rows("SELECT event_id FROM scheduler_observations WHERE action_id='a1'")[0]['event_id']==first


def test_configuration_takeover_preserves_personal_network_rounds(queue):
    ledger,service=queue;job=add(ledger,service,'a');attempt=ledger.claim(now=1)
    ledger.finish(attempt['attempt_id'],'failed',{'fault':TRANSIENT},now=2)
    new=ledger.ensure_service('rag',{'endpoint':'http://127.0.0.1:19877'})
    llm=ledger.ensure_service('llm',{'model':'synthetic'})
    ledger.rebind_services(dict(rag=new,llm=llm))
    assert ledger.job(job)['service_id']==new['service_id']
    assert ledger.budget(job)['rounds']==1

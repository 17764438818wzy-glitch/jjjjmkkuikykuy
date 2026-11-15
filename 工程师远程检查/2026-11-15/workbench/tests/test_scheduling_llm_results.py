"""Returned model errors must reach service accounting before action success."""
import json
import os
from pathlib import Path
import sys
import threading
import time
from types import SimpleNamespace

import pytest

from paperflow.package import load
from paperflow.producer import HarnessProducer
from test_scheduler import scheduler_case,source
from test_winjob import wait_until


def run_model(case,tmp_path,task,status):
    m,c,s=case;c.save(tmp_path/'model-config.json');s.discover()
    claim=s.ledger.claim();assert claim['unit_key']==task['task_id']
    s.service.refresh()
    ex=m.launch('graph',task['task_id'],[sys.executable,'-B','-X','utf8',
        str(Path('tests/fixtures/llm_service_worker.py').resolve()),str(tmp_path/'model-config.json'),
        str(m.store.path),task['task_id'],str(status)],cwd=Path.cwd(),
        env={'PYTHONPATH':os.pathsep.join([os.environ.get('PYTHONPATH',''),str(Path.cwd())])},
        schedule_attempt=claim['attempt_id'])
    wait_until(lambda:ex.exit_saved,seconds=15)
    result=load(c.root/(task['task_id']+'.result.json'))
    assert (c.root/(task['task_id']+'.closed')).exists()
    attempts=[load(p) for p in (c.root/'private/attempts').glob('*.json') if load(p)['task_id']==task['task_id']]
    assert len(attempts)==1 and attempts[0]['finish_reason']==('completed' if status==200 else 'error')
    return ex,claim,result


@pytest.mark.parametrize('status',[429,503])
def test_two_articles_model_error_opens_shared_service(scheduler_case,tmp_path,status):
    m,c,s=scheduler_case;m.resume()
    for name in ('first','second'):
        task=source(scheduler_case,tmp_path,name)
        ex,claim,result=run_model(scheduler_case,tmp_path,task,status)
        assert result['fault']['category']=='transient'
        s.reap()
    facts=s.ledger.rows("SELECT state,result FROM control_actions WHERE kind='llm'")
    assert len(facts)==2 and all(r['state']=='diagnostic' for r in facts)
    assert all(json.loads(r['result'])['fault']['status']==status for r in facts)
    assert len(s.ledger.rows('SELECT * FROM scheduler_observations'))==2
    assert s.service.services['llm']['state']=='open' and s.service.services['rag']['state']=='closed'
    assert all(s.ledger.budget(j['job_id'])['rounds']==0 for j in s.ledger.rows('SELECT * FROM scheduler_jobs'))


@pytest.mark.parametrize('status',[429,503,200])
def test_half_open_probe_uses_model_return_status(scheduler_case,tmp_path,status):
    m,c,s=scheduler_case;m.resume();sid=s.service.services['llm']['service_id']
    opened=s.ledger.observe(sid,'seed','seed',dict(category='configuration',family='unavailable'),now=time.time()-120)
    s.service.refresh();assert s.service.open_due()==['llm']
    task=source(scheduler_case,tmp_path,'probe')
    ex,claim,result=run_model(scheduler_case,tmp_path,task,status)
    row=s.service.services['llm']
    assert row['state']==('closed' if status==200 else 'open')
    assert row['event_id']==(None if status==200 else opened['event_id'])
    facts=s.ledger.rows("SELECT state FROM control_actions WHERE kind='llm'")
    assert [f['state'] for f in facts]==(['complete'] if status==200 else ['diagnostic'])


@pytest.mark.parametrize('status,category,state',[(401,'configuration','open'),(400,'held','closed')])
def test_model_request_rejection_keeps_its_category(scheduler_case,tmp_path,status,category,state):
    m,c,s=scheduler_case;m.resume();task=source(scheduler_case,tmp_path,'rejected')
    ex,claim,result=run_model(scheduler_case,tmp_path,task,status)
    assert result['fault']['category']==category
    facts=s.ledger.rows("SELECT state,result FROM control_actions WHERE kind='llm'")
    assert len(facts)==1 and facts[0]['state']=='diagnostic'
    assert json.loads(facts[0]['result'])['fault']['category']==category
    assert s.service.services['llm']['state']==state


def test_timeout_return_does_not_complete_model_action(tmp_path):
    from paperflow.config import Config
    from paperflow.package import sha
    closed=threading.Event();outcomes=[]
    class Harness:
        def __init__(self,**options):pass
        def start(self):pass
        def close(self):closed.set()
        def run(self,*args,**kwargs):
            assert closed.wait(3)
            return SimpleNamespace(finish_reason='completed',events=[])
    class RecordingActions:
        def bind(self,cancellation):pass
        def unbind(self,cancellation):pass
        def run(self,kind,resource,fn,**kwargs):
            try:result=fn(threading.Event())
            except Exception:
                if kind=='llm':outcomes.append('diagnostic')
                raise
            if kind=='llm':outcomes.append('complete')
            return result
    original=tmp_path/'a.txt';original.write_text('synthetic source')
    task=dict(task_id='t',source_id='s',source_path=str(original),source_sha256=sha(original),revision=1)
    c=Config(home=str(tmp_path/'runtime'),model_timeout=.1)
    with pytest.raises(TimeoutError):
        HarnessProducer(c,factory=Harness,control=RecordingActions())._run_stage(task,'round1')
    assert closed.is_set() and outcomes==['diagnostic']
    assert len(list((c.root/'private/attempts').glob('*.json')))==1

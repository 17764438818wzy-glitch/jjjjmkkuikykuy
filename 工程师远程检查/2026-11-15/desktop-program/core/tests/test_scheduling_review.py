import json
from pathlib import Path
from types import SimpleNamespace
from urllib.error import URLError,HTTPError
import ssl

import pytest

from paperflow.control import Manager
from paperflow.delivery import DeliveryWorkflow
from paperflow.scheduling_policy import ActionDeferred,fault
from paperflow.scheduler import Scheduler
from test_scheduler import scheduler_case,source
from test_delivery_state import delivery_case
from test_delivery_flow import flow,IndexedRag,release
from test_control import manager


@pytest.mark.parametrize('error,category',[(URLError(ConnectionRefusedError('synthetic')),'transient'),
    (URLError(ssl.SSLError('synthetic')),'configuration')])
def test_raw_download_transport_is_classified(error,category):
    assert fault(error)['category']==category


def test_new_configuration_rebinds_nonrunning_queue_but_keeps_history(scheduler_case,tmp_path):
    m,c,s=scheduler_case;source(scheduler_case,tmp_path,'a');s.discover()
    sid=s.service.services['llm']['service_id'];job=s.ledger.rows('SELECT * FROM scheduler_jobs')[0]
    s.ledger.observe(sid,'config','a',dict(category='configuration',status=401))
    m.shutdown();c.model_access_fingerprint='explicit-new-access-revision'
    new=Manager(m.store,m.root).start()
    try:
        next_=Scheduler(new,c,isolation_root=tmp_path);next_.tick()
        assert new.stopped.is_set() and not new.executions
        assert next_.ledger.job(job['job_id'])['service_id']==next_.service.services['llm']['service_id']!=sid
        assert next_.ledger.service(sid)['state']=='open'
        assert next_.ledger.claim() is not None
    finally:new.shutdown()


def test_freeze_after_delivery_complete_creates_safety_job_and_is_not_complete(delivery_case,tmp_path):
    m,c,t,d,dc,*_=delivery_case;client=IndexedRag();a=flow(delivery_case,tmp_path,client);a.run();release(a)
    c.min_free_gib=0;c.max_memory_percent=100;s=Scheduler(m,c,isolation_root=tmp_path)
    identity=dc.ledger.context(d,'120201')['identity']
    from paperflow.delivery_store import target_key
    jid=s.ledger.enqueue('delivery',t['source_id'],target_key(identity),dict(manifest_sha256=d,specialty_code='120201',generation=1),s.service.services['rag']['service_id'])
    attempt=s.ledger.claim();s.ledger.finish(attempt['attempt_id'],'complete',{})
    assert s.verified_complete(s.ledger.job(jid))
    dc.freeze_active(t['source_id'],'120201','synthetic freeze')
    assert not s.verified_complete(s.ledger.job(jid))
    s.discover()
    jobs=s.ledger.rows("SELECT * FROM scheduler_jobs WHERE json_extract(data,'$.specialty_code')='120201'")
    assert len(jobs)==2 and any(row['state']=='queued' for row in jobs)
    s.discover();assert len(s.ledger.rows("SELECT * FROM scheduler_jobs WHERE json_extract(data,'$.specialty_code')='120201'"))==2
    job=next(row for row in jobs if row['state']=='queued');assert not s.verified_complete(job)
    disable=flow(delivery_case,tmp_path,client);assert disable.run()['state']=='frozen';release(disable)
    assert s.verified_complete(job) and dc.ledger.active(t['source_id'],'120201') is None


def test_frozen_safety_resource_busy_stays_deferred(monkeypatch):
    from contextlib import nullcontext
    from paperflow.delivery import DeliveryPublisher
    monkeypatch.setattr(DeliveryPublisher,'set_enabled',lambda *a:(_ for _ in ()).throw(ActionDeferred('resource_busy')))
    workflow=SimpleNamespace(client=SimpleNamespace(version=lambda *a,**kw:nullcontext()),
        actions=SimpleNamespace(commit=lambda *a,**kw:{}),fault=lambda *a:None)
    with pytest.raises(ActionDeferred):DeliveryWorkflow.disable_frozen(workflow,dict(manifest_sha256='a'*64,data={'receipts':{}}))


def test_read_windows_are_retired_only_after_exit_and_publication(tmp_path):
    from paperflow.store import Store
    m=Manager(Store(tmp_path/'state.sqlite3'),tmp_path/'manager',history_limit=2)
    m.delivery=SimpleNamespace(read_windows={})
    for n in range(100):
        key=str(n);m.executions[key]=SimpleNamespace(execution_id=key,scope='unit:'+key,exit_saved=True,peer=None,active_action=None)
        m.delivery.read_windows[key]={'fixture':True}
    m.publications.add('99');m._trim_history()
    assert m.delivery.read_windows=={'99':{'fixture':True}}
    m.publications.clear();m._trim_history();assert m.delivery.read_windows=={}


def test_missing_local_model_credentials_are_configuration(tmp_path,monkeypatch):
    from paperflow.config import Config
    from paperflow.producer import HarnessProducer
    monkeypatch.setattr(Path,'home',classmethod(lambda cls:tmp_path/'no-user-credentials'))
    producer=HarnessProducer(Config(home=str(tmp_path/'runtime')))
    with pytest.raises(Exception) as error:producer._home()
    assert fault(error.value)['category']=='configuration'

import time
import threading
import pytest
from paperflow.config import Config
from paperflow.store import Store
from paperflow.service import Service
from paperflow.package import save


def test_status_reports_success_failures_backlog_and_attention(tmp_path):
    c=Config(home=str(tmp_path/'run'),source_roots=[],min_free_gib=0)
    service=Service(c);s=service.store
    t=s.register_source(tmp_path/'a','a'*64)
    s.event(t['task_id'],'stage',dict(stage='cleanup',next_stage='complete',revision=1,seconds=2,error_kind=None))
    s.event(t['task_id'],'stage',dict(stage='round1',next_stage='classify_error',revision=1,seconds=10,error_kind='infrastructure'))
    s.update(t['task_id'],stage='code_error',error_kind='controller')
    status=service.status();m=status['metrics']
    assert m['success_1h']==1 and m['success_24h']==1
    assert m['stages']['round1']['failures']['infrastructure']==1
    assert m['stages']['round1']['average_seconds']==10
    assert m['code_error'][0]['task_id']==t['task_id'] and status['alerts']
    service.close()


def test_run_cannot_start_until_all_real_acceptance_checks_pass(tmp_path):
    from paperflow.readiness import NotReady,fingerprint
    c=Config(home=str(tmp_path/'run'),source_roots=[],acceptance_report=str(tmp_path/'report.json'))
    s=Service(c);stop=threading.Event();stop.set()
    with pytest.raises(NotReady):s.run(stop)
    save(c.acceptance_report,dict(contract_version=c.contract_version,rules_sha256=fingerprint(c),checks={'production_paper':True}))
    with pytest.raises(NotReady):s.run(stop)
    s.close()


def test_retired_version_waits_for_stable_current_activation_and_keeps_removed_routes(tmp_path):
    s=Store(tmp_path/'state.sqlite3');p=tmp_path/'source'
    a=s.register_source(p,'a'*64)
    s.receipt(a['task_id'],1,'activation',dict(status='pending',previous=[],previous_versions=[],current={}))
    s.mark_active(a['task_id'],1)
    b=s.register_source(p,'b'*64)
    old=dict(task_id=a['task_id'],revision=1,targets={'removed-code':{'body':{'dataset_id':'old-ds','document_id':'old-doc'}}})
    s.receipt(b['task_id'],1,'activation',dict(status='pending',previous=[],previous_versions=[old],current={}))
    s.mark_active(b['task_id'],1)
    assert s.retired_versions(now=time.time(),stable_seconds=7*86400)==[]
    retired=s.retired_versions(now=time.time()+8*86400,stable_seconds=7*86400)
    assert len(retired)==1 and retired[0]['targets']==old['targets']
    assert retired[0]['task_id']==a['task_id']


def test_gc_checks_ownership_disabled_state_and_records_absence(tmp_path):
    from paperflow.maintenance import collect_version
    from paperflow.rag import RagPublisher,RemoteConflict
    from test_rag import MemoryRag
    class Client(MemoryRag):
        def request(self,method,endpoint,body=None,content_type=None):
            if method=='DELETE':
                ds=endpoint.split('/')[2]
                for did in body['ids']:del self.docs[(ds,did)];del self.chunks[did]
                return {}
            return super().request(method,endpoint,body,content_type)
    s=Store(tmp_path/'state');t=s.register_source(tmp_path/'s','a'*64)
    client=Client();publisher=RagPublisher(client)
    doc=dict(id='d',name='PF_a_hash_code_body.md',status='0',meta_fields={
        'paperflow_task':t['task_id'],'paperflow_revision':'1','paperflow_source_sha256':t['source_sha256'],
        'paperflow_specialty':'code','paperflow_role':'body','paperflow_manifest_sha256':'f'*64})
    client.docs[('ds','d')]=doc;client.chunks['d']=[]
    retired=dict(task_id=t['task_id'],revision=1,targets={'code':{'body':dict(dataset_id='ds',document_id='d',name=doc['name'],chunks={},identity=dict(doc['meta_fields']))}})
    doc['status']='1'
    with pytest.raises(RemoteConflict):collect_version(s,publisher,retired)
    assert client.docs
    doc['status']='0';doc['meta_fields']=dict(doc['meta_fields'],paperflow_manifest_sha256='e'*64)
    with pytest.raises(RemoteConflict):collect_version(s,publisher,retired)
    assert client.docs
    doc['meta_fields']['paperflow_manifest_sha256']='f'*64
    collect_version(s,publisher,retired)
    assert client.docs=={}
    assert s.receipt(t['task_id'],1,'gc')['ds/d']['status']=='deleted'
    collect_version(s,publisher,retired) # Durable completion makes a second run harmless.

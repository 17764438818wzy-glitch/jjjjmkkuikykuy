import copy
import hashlib
import threading

import pytest

from paperflow.package import save,load,sha
from paperflow.handoff import PackageHandoff
from test_delivery_state import delivery_case,owner,begin
from test_control import manager
from test_rag import MemoryRag


class IndexedRag(MemoryRag):
    """Fault server with explicit creation visibility and synchronous terminal writes."""
    base_url='http://127.0.0.1:18088'
    def __init__(self):super().__init__();self.calls=[];self.fail=None;self.images={}
    def request(self,method,endpoint,body=None,content_type=None):
        self.calls.append((method,endpoint,copy.deepcopy(body)))
        if self.fail:self.fail(method,endpoint,body)
        if endpoint=='/retrieval':
            return dict(chunks=[copy.deepcopy(ch) for (ds,did),doc in self.docs.items()
                if ds in body['dataset_ids'] and (not body.get('document_ids') or did in body['document_ids'])
                for ch in self.chunks[did] if ch.get('available') is True])
        result=super().request(method,endpoint,body,content_type)
        parts=endpoint.split('?')[0].strip('/').split('/')
        if method=='PATCH' and len(parts)==4 and 'enabled' in body:
            for ch in self.chunks[parts[3]]:ch['available']=bool(body['enabled'])
        if method=='POST' and parts[-1]=='chunks':
            ch=self.chunks[parts[3]][-1];ch['available']=self.docs[(parts[1],parts[3])]['status']=='1'
            result={'chunk':copy.deepcopy(ch)}
        return result


class ManagedActions:
    def __init__(self,case,tmp_path,code):
        self.m,self.config,_,_,self.commits,*_=case
        self.ex,self.identity=owner(case,tmp_path,code)
        self.execution_id=self.ex.execution_id
    def run(self,kind,resource,call,**kwargs):
        tid=begin(self.m,self.ex,self.identity,kind=kind,**resource)
        try:result=call(threading.Event())
        except BaseException as exc:
            self.m.record_result(self.ex.execution_id,tid,self.m.epoch,'unknown' if kind=='rag_write' else 'diagnostic',dict(error=type(exc).__name__))
            raise
        self.m.record_result(self.ex.execution_id,tid,self.m.epoch,'complete',dict(terminal=True))
        return result
    def commit(self,operation,payload):
        return self.run('local',dict(operation=operation),lambda cancel:self.commits.handle(self.ex.execution_id,operation,
            self.commits.stage(self.ex.execution_id,dict(identity=self.identity,**payload))))


def flow(case,tmp_path,client,code='120201'):
    from paperflow.delivery import DeliveryWorkflow
    from paperflow.delivery_control import ControlledRagClient
    actions=ManagedActions(case,tmp_path,code)
    result=DeliveryWorkflow(actions.config,actions.commits.ledger,actions,ControlledRagClient(client,actions),actions.identity)
    result.check_deployment=lambda:None  # Fault tests only; live acceptance uses the real deployment gate.
    return result


def advance(case):
    m,c,t,d,commits,material,catalog=case
    profile=load(material/'profile.json');profile['article']['title']+='新版';save(material/'profile.json',profile)
    handoff=PackageHandoff(m.store,c.handoff_root)
    expected=handoff.ledger.expectations(t['task_id'],['120201','120202'])
    intent=handoff.prepare(t['task_id'],dict(t,revision=2),material,catalog,mapping_version=sha(c.catalog_path),expected_targets=expected)
    result=handoff.resume(t['task_id'],intent['input_fingerprint'])
    return (m,c,t,result['manifest_sha256'],commits,material,catalog)


def release(flow):
    from test_winjob import wait_until
    flow.actions.m.cancel_unit(flow.actions.ex.execution_id,'test attempt complete')
    wait_until(lambda:flow.actions.ex.exit_saved)


def test_a_success_b_failure_retry_does_not_rewrite_a(delivery_case,tmp_path):
    client=IndexedRag();a=flow(delivery_case,tmp_path,client);assert a.run()['state']=='active'
    before=copy.deepcopy({k:v for k,v in client.docs.items() if k[0]=='paper-ds'})
    client.fail=lambda method,path,body:(_ for _ in ()).throw(ConnectionError('B unavailable')) if method=='GET' and '/paper-two/' in path else None
    b=flow(delivery_case,tmp_path,client,'120202')
    with pytest.raises(ConnectionError):b.run()
    assert before=={k:v for k,v in client.docs.items() if k[0]=='paper-ds'}
    client.fail=None;client.calls=[]
    assert b.run()['state']=='active'
    assert all('/paper-ds/' not in path for _,path,_ in client.calls)


def test_already_active_only_reads_and_never_initializes_disabled(delivery_case,tmp_path):
    client=IndexedRag();a=flow(delivery_case,tmp_path,client);a.run();client.calls=[]
    assert a.run()['state']=='active'
    assert all(method=='GET' or path=='/retrieval' for method,path,_ in client.calls)


@pytest.mark.parametrize('fault',['switch_intent','enabled','retrieved','old_disabled'])
def test_switch_crash_resumes_without_uploading_again(delivery_case,tmp_path,fault):
    client=IndexedRag();a=flow(delivery_case,tmp_path,client)
    def crash(point):
        if point==fault:raise RuntimeError('simulated process exit')
    a.fault=crash
    with pytest.raises(RuntimeError):a.run()
    writes=client.writes;a.fault=lambda point:None
    assert a.run()['state']=='active'
    assert client.writes==writes


def test_lost_write_response_stays_barred_even_when_readback_matches(delivery_case,tmp_path):
    client=IndexedRag();a=flow(delivery_case,tmp_path,client);client.lose_chunk_response=True
    with pytest.raises(TimeoutError):a.run()
    writes=client.writes
    assert a.run()['state']=='blocked'
    assert client.writes==writes
    assert a.ledger.active(a.identity['source_id'],'120201') is None


def test_image_association_metadata_is_part_of_exact_readback(delivery_case,tmp_path):
    client=IndexedRag();a=flow(delivery_case,tmp_path,client);a.run()
    next(iter(client.docs.values()))['meta_fields']['paperflow_image_associations_json']='wrong'
    with pytest.raises(ConnectionError,match='association'):a.run()


def test_new_version_keeps_old_active_until_new_retrieval(delivery_case,tmp_path):
    client=IndexedRag();a=flow(delivery_case,tmp_path,client);a.run();release(a)
    old_ids=set(client.docs);new_case=advance(delivery_case);b=flow(new_case,tmp_path,client)
    def check(point):
        if point=='enabled':assert all(client.docs[k]['status']=='1' for k in old_ids)
    b.fault=check
    assert b.run()['state']=='active'
    assert all(client.docs[k]['status']=='0' for k in old_ids)
    assert b.ledger.active(b.identity['source_id'],b.code)['receipts'][b.code]['retrieval_verified'] is True


def test_corrupted_candidate_after_old_disabled_restores_verified_old(delivery_case,tmp_path):
    client=IndexedRag();a=flow(delivery_case,tmp_path,client);a.run();release(a)
    old_ids=set(client.docs);b=flow(advance(delivery_case),tmp_path,client)
    b.fault=lambda point:(_ for _ in ()).throw(RuntimeError('crash')) if point=='old_disabled' else None
    with pytest.raises(RuntimeError):b.run()
    new_id=next(did for ds,did in client.docs if (ds,did) not in old_ids)
    client.chunks[new_id][0]['content']+='corruption'
    b.fault=lambda point:None
    with pytest.raises(ConnectionError):b.run()
    assert all(v['status']==('1' if k in old_ids else '0') for k,v in client.docs.items())


def test_not_yet_visible_retrieval_retries_candidate_and_preserves_old(delivery_case,tmp_path):
    client=IndexedRag();a=flow(delivery_case,tmp_path,client);a.run();release(a)
    old_ids=set(client.docs);b=flow(advance(delivery_case),tmp_path,client)
    native=client.request
    def no_new(method,path,body=None,content_type=None):
        result=native(method,path,body,content_type)
        if path=='/retrieval' and not any(did in body['document_ids'] for _,did in old_ids):return {'chunks':[]}
        return result
    client.request=no_new
    with pytest.raises(ConnectionError):b.run()
    assert all(client.docs[k]['status']=='1' for k in old_ids)
    assert b.ledger.switching(b.identity['source_id'],b.code)['phase']=='enabled'
    assert b.ledger.active(b.identity['source_id'],b.code)['identity']['manifest_sha256']==a.digest
    writes=client.writes;client.request=native
    assert b.run()['state']=='active'
    assert client.writes==writes
    assert all(client.docs[k]['status']=='0' for k in old_ids)


def test_obsolete_enabled_candidate_is_withdrawn_before_new_target(delivery_case,tmp_path):
    client=IndexedRag();a=flow(delivery_case,tmp_path,client)
    a.fault=lambda point:(_ for _ in ()).throw(RuntimeError('crash')) if point=='enabled' else None
    with pytest.raises(RuntimeError):a.run()
    old_ids=set(client.docs);release(a)
    b=flow(advance(delivery_case),tmp_path,client);b.run()
    assert all(client.docs[k]['status']=='0' for k in old_ids)


def test_freeze_after_enable_allows_only_candidate_withdrawal(delivery_case,tmp_path):
    client=IndexedRag();a=flow(delivery_case,tmp_path,client)
    a.fault=lambda point:(_ for _ in ()).throw(RuntimeError('crash')) if point=='enabled' else None
    with pytest.raises(RuntimeError):a.run()
    m=a.actions.m;m.change_admission_state(a.actions.ex.scope,'freeze',dict(frozen=True))
    a.fault=lambda point:None
    assert a.run()['state']=='frozen'
    assert all(v['status']=='0' for v in client.docs.values())


def test_pause_after_enable_never_performs_automatic_withdrawal(delivery_case,tmp_path):
    client=IndexedRag();a=flow(delivery_case,tmp_path,client)
    def pause(point):
        if point=='enabled':a.actions.m.pause('pause switching')
    a.fault=pause
    with pytest.raises(ValueError):a.run()
    assert all(v['status']=='1' for v in client.docs.values())


def test_wrong_dataset_and_old_version_enable_rejected_before_network(delivery_case,tmp_path):
    client=IndexedRag();a=flow(delivery_case,tmp_path,client)
    with pytest.raises(ValueError,match='dataset|endpoint'):
        a.client.request('POST','/datasets/not-ours/documents/d/chunks',dict(content='x'))
    assert not client.calls

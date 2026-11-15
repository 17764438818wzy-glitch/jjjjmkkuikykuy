import json
import threading

import pytest

from test_control import manager
from test_delivery_state import delivery_case
from test_cleanup_store import retired
from test_cleanup_control import controlled,request
from test_delivery_flow import IndexedRag


class DeletableRag(IndexedRag):
    def __init__(self,original):
        super().__init__();self.docs=original.docs;self.chunks=original.chunks;self.lose_response=False
    def request(self,method,endpoint,body=None,content_type=None):
        if method=='DELETE':
            self.calls.append((method,endpoint,body))
            ds=endpoint.split('/')[2]
            for did in body['ids']:
                self.docs.pop((ds,did),None);self.chunks.pop(did,None)
            if self.lose_response:raise TimeoutError('delete sent, response lost')
            return {}
        return super().request(method,endpoint,body,content_type)


class ManagedCleanup:
    def __init__(self,commits,item,ex):self.commits,self.item,self.ex=commits,item,ex;self.m=commits.manager
    def run(self,kind,resource,call,**kwargs):
        tid=request(self.commits,self.item,self.ex,kind,**resource)
        snapshot=self.m.check_begin(self.ex.execution_id,tid);self.m.grant_begin(self.ex.execution_id,tid,snapshot)
        try:result=call(threading.Event())
        except Exception as exc:
            self.m.record_result(self.ex.execution_id,tid,self.m.epoch,'unknown' if kind=='cleanup_write' else 'diagnostic',dict(error=type(exc).__name__))
            raise
        self.m.record_result(self.ex.execution_id,tid,self.m.epoch,'complete',{})
        return result
    def commit(self,operation,payload):
        return self.run('local',dict(operation=operation),lambda cancel:self.commits.handle(self.ex.execution_id,operation,
            self.commits.stage(self.ex.execution_id,dict(item_id=self.item['item_id'],**payload))))


def prepared(delivery_case,tmp_path):
    from paperflow.cleanup import CleanupWorkflow
    from paperflow.cleanup_control import CleanupClient
    case,original=retired(delivery_case,tmp_path);client=DeletableRag(original)
    commits,item,ex=controlled(case,tmp_path);actions=ManagedCleanup(commits,item,ex)
    flow=CleanupWorkflow(commits.config,commits.ledger,actions,CleanupClient(client,actions),item['item_id'])
    flow.check_deployment=lambda:None
    return flow,client,case


def test_exact_retired_document_removed_readback_and_replay_no_second_delete(delivery_case,tmp_path):
    flow,client,case=prepared(delivery_case,tmp_path)
    active=case[4].ledger.active(case[2]['source_id'],'120201')
    assert flow.run()['state']=='deleted'
    assert flow.run()['state']=='deleted'
    assert len([c for c in client.calls if c[0]=='DELETE'])==1
    assert case[4].ledger.active(case[2]['source_id'],'120201')==active


@pytest.mark.parametrize('damage',['enabled','chunk_enabled','metadata','content','missing'])
def test_ownership_or_availability_mismatch_preserves_remote(delivery_case,tmp_path,damage):
    flow,client,_=prepared(delivery_case,tmp_path)
    obj=json.loads(flow.ledger.item(flow.item_id)['resource'])['object'];key=obj['dataset_id'],obj['document_id']
    if damage=='enabled':client.docs[key]['status']='1'
    if damage=='chunk_enabled':client.chunks[key[1]][0]['available']=True
    if damage=='metadata':client.docs[key]['meta_fields']['paperflow_manifest_sha256']='foreign'
    if damage=='content':client.chunks[key[1]][0]['content']+='changed'
    if damage=='missing':client.docs.pop(key)
    with pytest.raises(Exception):flow.run()
    assert not [c for c in client.calls if c[0]=='DELETE']
    assert flow.ledger.item(flow.item_id)['state']!='deleted'


def test_lost_delete_response_never_counts_as_complete_or_retries_write(delivery_case,tmp_path):
    flow,client,case=prepared(delivery_case,tmp_path);client.lose_response=True
    with pytest.raises(TimeoutError):flow.run()
    assert flow.ledger.item(flow.item_id)['state']=='pending_reconcile'
    assert flow.run()['state']=='pending_reconcile'
    assert len([c for c in client.calls if c[0]=='DELETE'])==1
    assert case[4].ledger.unresolved(case[2]['source_id'],'120201')


def test_unknown_action_blocks_deleted_commit_even_if_readback_is_absent(delivery_case,tmp_path):
    flow,client,case=prepared(delivery_case,tmp_path);client.lose_response=True
    with pytest.raises(TimeoutError):flow.run()
    with pytest.raises(ValueError):flow.actions.commit('result',dict(outcome='deleted',evidence=dict(absent=True)))

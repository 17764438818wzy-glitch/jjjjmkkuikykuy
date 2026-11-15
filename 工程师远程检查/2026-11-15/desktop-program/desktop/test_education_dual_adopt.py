import json
from dataclasses import replace
from unittest.mock import patch
import pytest
from ingest_queue import QueueStore
from test_ingest_education import prepared
from ingest_education import education_specs
from education_dual_adopt import audit_batch, adopt_batch

def setup(tmp):
    data=tmp/'data'; run='AUTO_test'; batch=data/'runs'/run; batch.mkdir(parents=True)
    item,package,dep=prepared(tmp)
    item['package_path']=str(package)
    item['path']=str(package/'ingest.md')
    dep['full_ingest_pipeline_id']=dep['pipeline_id']
    private=data/'private'/'education_pilot'; private.mkdir(parents=True)
    (private/'deployment.json').write_text(json.dumps(dep))
    legacy=data/'private'/'education_full_ingest'/run; legacy.mkdir(parents=True)
    import shutil
    shutil.copytree(package,legacy/'canonical_packages'/item['id'])
    (batch/'manifest.json').write_text(json.dumps({'run_id':run,'items':[item]}))
    spec=education_specs(item,dep,package,'http://localhost:9380',run)[0]
    name=__import__('education_stream_ingest').education_remote_name(package/'ingest.md')
    (legacy/'receipts.json').write_text(json.dumps({'primary':{item['paper_id']:{'document_id':item['id'],'upload_name':name,'ragflow_document_id':'r1','status':'fail'}},'images':{}}))
    class Client:
        base_url='http://localhost:9380'
        def request(self,method,endpoint):
            assert method=='GET'
            return {'docs':[{'id':'r1','name':name,'pipeline_id':dep['pipeline_id'],'run':'DONE','meta_fields':{'education_paper_id':item['paper_id'],'education_admission':'入库'}}] if '/education-md/' in endpoint else []}
    return data,run,Client(),QueueStore(data/'q.sqlite3'),item

def test_running_legacy_worker_blocks_adoption(tmp_path):
    data,run,client,q,_=setup(tmp_path)
    with patch('education_dual_adopt.legacy_locked',return_value=True):
        audit=audit_batch(data,run,client,q)
        with pytest.raises(ValueError,match='worker'): adopt_batch(data,run,client,q,audit)
    assert not q.summary()

def test_remote_done_beats_stale_fail_and_adoption_is_idempotent(tmp_path):
    data,run,client,q,item=setup(tmp_path)
    audit=audit_batch(data,run,client,q)
    adopt_batch(data,run,client,q,audit)
    assert q.summary().get('accepted')==1
    assert not q.summary().get('parse_waiting')
    adopt_batch(data,run,client,q,audit_batch(data,run,client,q))
    assert q.summary().get('accepted')==1
    assert q.find_identity(client.base_url,'education-md',item['paper_id'],'paper_primary')['remote_id']=='r1'

def test_duplicate_remote_name_is_conflict_and_not_uploaded(tmp_path):
    data,run,client,q,_=setup(tmp_path)
    original=client.request
    client.request=lambda *a: {'docs': original(*a)['docs']*2}
    audit=audit_batch(data,run,client,q)
    assert audit['conflicts']
    with pytest.raises(ValueError): adopt_batch(data,run,client,q,audit)
    assert not q.summary()

def test_unknown_parse_is_preserved(tmp_path):
    data,run,client,q,_=setup(tmp_path)
    original=client.request
    def request(*a):
        payload=original(*a)
        for doc in payload['docs']: doc.update(run='UNSTART',meta_fields={})
        return payload
    client.request=request
    receipt=data/'private'/'education_full_ingest'/run/'receipts.json'
    obj=json.loads(receipt.read_text()); next(iter(obj['primary'].values()))['parse_requested_at']='2026-09-26T00:00:00'; receipt.write_text(json.dumps(obj))
    adopt_batch(data,run,client,q,audit_batch(data,run,client,q))
    assert q.summary().get('parse_unknown')==1
    assert not q.summary().get('parse_waiting')

def test_missing_assets_are_queued_only_after_audit_confirms_no_legacy_doc(tmp_path):
    data,run,client,q,item=setup(tmp_path)
    adopt_batch(data,run,client,q,audit_batch(data,run,client,q))
    assert q.summary().get('upload_waiting',0)>=1
    primary=q.find_identity(client.base_url,'education-md',item['paper_id'],'paper_primary')
    assert primary['status']=='accepted'

def test_live_queue_worker_blocks_uncoordinated_adoption(tmp_path):
    data,run,client,q,_=setup(tmp_path)
    audit=audit_batch(data,run,client,q)
    def locked(path): return path.name.endswith('.worker.lock')
    with patch('education_dual_adopt.legacy_locked',side_effect=locked):
        with pytest.raises(ValueError,match='worker'): adopt_batch(data,run,client,q,audit)
    assert not q.summary()

def test_legacy_tick_refuses_batch_owned_by_universal_queue(tmp_path):
    from education_stream_ingest import Stream
    stream=Stream.__new__(Stream);stream.private_dir=tmp_path
    (tmp_path/'queue_owner.json').write_text('{}')
    with pytest.raises(RuntimeError,match='queue'): stream.tick()

def test_adoption_cannot_replace_another_queues_owner(tmp_path):
    data,run,client,q,_=setup(tmp_path)
    marker=data/'private'/'education_full_ingest'/run/'queue_owner.json'
    marker.write_text(json.dumps({'run_id':run,'queue':str(data/'other.sqlite3'),'service_key':client.base_url}))
    audit=audit_batch(data,run,client,q)
    with pytest.raises(ValueError,match='owner'): adopt_batch(data,run,client,q,audit)
    assert not q.summary()

def test_legacy_recovery_refuses_owned_batch(tmp_path):
    from test_education_stream_ingest import ReadyItemsTest
    fixture=ReadyItemsTest()
    stream=fixture.recovery_stream()
    stream.private_dir=tmp_path
    (tmp_path/'queue_owner.json').write_text('{}')
    with pytest.raises(RuntimeError,match='queue'): stream.recover_arrearage_tick()

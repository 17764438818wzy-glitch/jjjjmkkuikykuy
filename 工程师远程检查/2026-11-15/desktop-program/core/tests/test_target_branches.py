import threading
from pathlib import Path
from paperflow.config import Config
from paperflow.graph import Workflow
from paperflow.package import load,save,sha,seal_package
from paperflow.rag import RagPublisher
from paperflow.store import Store
from test_rag import MemoryRag


def test_failed_specialty_retries_without_replaying_confirmed_specialty(tmp_path,package_factory):
    c=Config(home=str(tmp_path/'runtime'),handoff_root=str(tmp_path/'handoff'),source_roots=[],retry_seconds=0,short_retry_interval=0,rag_workers=2)
    s=Store(c.root/'state.sqlite3');source=tmp_path/'a.txt';source.write_text('paper',encoding='utf-8')
    t=s.register_source(source,sha(source));root,_,catalog=package_factory(task_id=t['task_id'],source_sha=t['source_sha256'])
    profile=load(root/'profile.json');profile['routing_candidates'].append(dict(code='120202',name='市场营销',reason='方法适用'))
    save(root/'profile.json',profile);seal_package(root,t,1)
    catalog['majors'].append(dict(code='120202',name='市场营销',paper_dataset_id='other-ds',image_dataset_id='other-images'))
    class Client(MemoryRag):
        lock=threading.RLock()
        remaining_failures=3
        def request(self,method,endpoint,body=None,content_type=None):
            with self.lock:
                if method=='POST' and '/other-ds/' in endpoint and endpoint.endswith('/chunks') and self.remaining_failures:
                    self.remaining_failures-=1;raise TimeoutError('transient specialty failure')
                return super().request(method,endpoint,body,content_type)
        def upload_bytes(self,*args):
            with self.lock:return super().upload_bytes(*args)
    class Publisher(RagPublisher):
        calls={}
        def publish(self,task,packet,receipts):
            code=packet['routes'][0]['code'];self.calls[code]=self.calls.get(code,0)+1
            return super().publish(task,packet,receipts)
    client=Client();publisher=Publisher(client)
    t=s.update(t['task_id'],stage='ingest',package_dir=str(root),manifest_sha256=sha(root/'manifest.json'))
    flow=Workflow(c,s,publisher=publisher,catalog=catalog)
    for _ in range(20):
        t=flow.step(t)
        if t.get('error_kind')=='infrastructure':break
    assert t['stage']=='ingest_target' and t['revision']==1
    confirmed=s.receipt(t['task_id'],1,'120201')
    assert confirmed['verified'] and publisher.calls['120201']==1
    for _ in range(20):
        t=flow.step(t)
        if t['stage']=='complete':break
    assert t['stage']=='complete',t
    assert publisher.calls=={'120201':1,'120202':4}
    assert s.receipt(t['task_id'],1,'all')=={}
    assert s.receipt(t['task_id'],1,'120201')['body']==confirmed['body']
    assert len(client.docs)==2
    flow.close()

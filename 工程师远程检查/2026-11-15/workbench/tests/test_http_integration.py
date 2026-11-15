"""真实HTTP客户端和完整阶段图的故障/恢复验收，不访问生产服务器。"""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import sys
import threading
from pathlib import Path

from paperflow.config import Config
from paperflow.graph import Workflow
from paperflow.package import sha
from paperflow.rag import RagPublisher
from paperflow.store import Store
from test_rag import MemoryRag


def test_full_graph_uses_native_http_and_completes_only_after_readback(tmp_path, package_factory):
    app=Path(r'E:\uv\ragflow_upstream')
    sys.path.insert(0,str(app))
    from ragflow_client import RagflowClient
    state=MemoryRag()
    raw_uploads={}
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args): pass
        def do_GET(self):
            parts=self.path.split('?',1)[0].strip('/').split('/')
            if len(parts)==6 and parts[2]=='datasets' and parts[4]=='documents':
                data=raw_uploads[(parts[3],parts[5])]
                self.send_response(200); self.end_headers(); self.wfile.write(data); return
            self.dispatch('GET')
        def do_PATCH(self): self.dispatch('PATCH')
        def do_POST(self): self.dispatch('POST')
        def dispatch(self,method):
            endpoint=self.path[len('/api/v1'):]
            raw=self.rfile.read(int(self.headers.get('Content-Length',0)))
            if 'multipart/form-data' in self.headers.get('Content-Type',''):
                from email.parser import BytesParser
                message=BytesParser().parsebytes(('Content-Type: '+self.headers['Content-Type']+'\r\n\r\n').encode()+raw)
                part=message.get_payload()[0]; name=part.get_filename(); data=part.get_payload(decode=True)
                ds=endpoint.split('/')[2]
                did=state.upload_bytes(ds,name,data); raw_uploads[(ds,did)]=data
                result=[state.docs[(ds,did)]]
            elif endpoint=='/retrieval':
                body=json.loads(raw)
                result={'chunks':[c for chunks in state.chunks.values() for c in chunks if c['document_id'] in body['document_ids']]}
            else:
                result=state.request(method,endpoint,json.loads(raw) if raw else None)
            response=json.dumps({'code':0,'data':result}).encode()
            self.send_response(200); self.send_header('Content-Type','application/json'); self.end_headers(); self.wfile.write(response)
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
    thread=threading.Thread(target=server.serve_forever,daemon=True); thread.start()
    try:
        original=tmp_path/'original.pdf'; original.write_bytes(b'original')
        config=Config(home=str(tmp_path/'runtime'),handoff_root=str(tmp_path/'handoff'),min_free_gib=0)
        store=Store(config.root/'state.sqlite3')
        task=store.register_source(original,sha(original))
        draft,_,catalog=package_factory(task_id=task['task_id'],source_sha=task['source_sha256'])
        class Producer:
            def run_stage(self,task,stage): return dict(draft_dir=str(draft),disposition='keep')
        client=RagflowClient(f'http://127.0.0.1:{server.server_address[1]}','local-test-only')
        flow=Workflow(config,store,producer=Producer(),publisher=RagPublisher(client),catalog=catalog)
        stages=[]
        for _ in range(20):
            task=flow.step(task); stages.append(task['stage'])
            if task['stage']=='complete': break
        assert task['stage']=='complete',task
        assert stages==['round2','round3','round3_assets','round3_redact','round3_chunk','publish','validate','ingest','ingest_target','join_targets','activate','cleanup','complete']
        assert store.active_for(original)==task['task_id']
        receipt=store.receipt(task['task_id'],1,'120201')
        assert receipt['retrieval_verified']
        assert store.receipt(task['task_id'],1,'all')=={}
        assert len(state.docs)==1
        assert all(d['status']=='1' for d in state.docs.values())
        assert not (config.root/'work'/task['task_id']).exists()
        flow.close()
    finally:
        server.shutdown(); server.server_close(); thread.join()

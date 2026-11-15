"""Real delivery worker, Windows Job, IPC, SQLite and native HTTP client; loopback only."""
import base64
import copy
from email.parser import BytesParser
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import io
import json
from pathlib import Path
import shutil
import threading
import pytest

from PIL import Image

from paperflow.delivery_worker import launch_delivery
from paperflow.package import load,save,sha
from test_control import manager
from test_delivery_state import delivery_case
from test_delivery_flow import IndexedRag,advance
from test_winjob import wait_until


@pytest.mark.parametrize('history_mode',[None,'same_expected','new_expected'])
def test_real_worker_freeze_disables_body_and_image_without_touching_b(delivery_case,tmp_path,history_mode):
    m,c,t,_,commits,material,_=delivery_case
    stream=io.BytesIO();Image.new('RGB',(16,16),'blue').save(stream,format='PNG')
    (material/'image.png').write_bytes(stream.getvalue())
    assets=load(material/'assets.json');assets['items'][0].update(storage='image',file='image.png',sha256=sha(material/'image.png'))
    save(material/'assets.json',assets);case=advance(delivery_case);digest=case[3]
    # Keep the existing HTTP client unchanged, but supply only a synthetic test credential.
    legacy=tmp_path/'http-client';legacy.mkdir()
    for name in ('ragflow_client.py','engine.py','content_rules.py'):
        shutil.copy2(Path(r'E:\uv\ragflow_upstream')/name,legacy/name)
    (legacy/'auto_config.py').write_text("def load_key(*args): return 'loopback-test-only'\n",encoding='utf-8')
    c.legacy_app=str(legacy)
    backend=IndexedRag();raw_uploads={};images={};requests=[]
    identity=dict(candidate='loopback-safety-test',dataset_ids=['paper-ds','images-ds','paper-two','images-two'])
    # Use the catalog's exact image dataset identity.
    identity['dataset_ids']=sorted({ds for item in c.catalog['majors'] for ds in (item['paper_dataset_id'],item['image_dataset_id'])})
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args):pass
        def reply(self,value,content_type='application/json'):
            data=value if isinstance(value,bytes) else json.dumps(value).encode()
            self.send_response(200);self.send_header('Content-Type',content_type);self.send_header('Content-Length',str(len(data)))
            self.end_headers();self.wfile.write(data)
        def do_GET(self):
            if self.path=='/pf-stage4/identity':return self.reply(identity)
            if self.path.startswith('/api/v1/documents/images/'):
                return self.reply(images[self.path.rsplit('/',1)[-1]],'image/png')
            parts=self.path.split('?',1)[0].strip('/').split('/')
            if len(parts)==6 and parts[2]=='datasets' and parts[4]=='documents':
                return self.reply(raw_uploads[(parts[3],parts[5])],'application/octet-stream')
            self.dispatch('GET')
        def do_POST(self):self.dispatch('POST')
        def do_PATCH(self):self.dispatch('PATCH')
        def dispatch(self,method):
            endpoint=self.path.removeprefix('/api/v1');raw=self.rfile.read(int(self.headers.get('Content-Length',0)))
            requests.append((method,endpoint))
            if 'multipart/form-data' in self.headers.get('Content-Type',''):
                message=BytesParser().parsebytes(('Content-Type: '+self.headers['Content-Type']+'\r\n\r\n').encode()+raw)
                part=message.get_payload()[0];data=part.get_payload(decode=True);ds=endpoint.split('/')[2]
                did=backend.upload_bytes(ds,part.get_filename(),data);raw_uploads[(ds,did)]=data
                value=[backend.docs[(ds,did)]]
            else:
                body=json.loads(raw) if raw else None
                value=backend.request(method,endpoint,body)
                if method=='POST' and endpoint.endswith('/chunks') and body.get('image_base64'):
                    parts=endpoint.split('/');ds,did=parts[2],parts[4]
                    row=backend.chunks[did][-1];image_id=ds+'-'+row['id'];row['image_id']=image_id
                    images[image_id]=base64.b64decode(body['image_base64']);value={'chunk':copy.deepcopy(row)}
            self.reply(dict(code=0,data=value))
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler);server.daemon_threads=True
    thread=threading.Thread(target=lambda:server.serve_forever(poll_interval=.05),daemon=True);thread.start()
    runs=[]
    try:
        c.rag_url=f'http://127.0.0.1:{server.server_port}'
        probe=dict(passed=True,index=dict(available_int=0,vectors=[1.,0.]),query=dict(dataset_ids=['fixture']),disabled_reads=[{},{}])
        report=tmp_path/'loopback-acceptance.json';save(report,dict(passed=True,endpoint=c.rag_url,identity=identity,probes=dict(a_body=probe,a_images=probe)))
        c.delivery_acceptance_report=str(report)
        def run(code):
            ex=launch_delivery(m,c,digest,code,total_timeout=60)
            wait_until(lambda:ex.exit_saved,seconds=45)
            result=load(c.root/'delivery-results'/(ex.execution_id+'.json'))
            runs.append(dict(result=result,exit=ex.exit_evidence))
            assert ex.exit_evidence['all_recorded_processes_exited']
            return result
        assert run('120201')['state']=='active'
        assert run('120202')['state']=='active'
        if history_mode:
            from paperflow.delivery_worker import launch_legacy_readback
            from paperflow.transition import TransitionGuard
            from paperflow.handoff_store import canonical
            active=commits.ledger.active(t['source_id'],'120201')
            with m.store.db() as conn:
                conn.execute("DELETE FROM specialty_active WHERE specialty_code='120201'")
                conn.execute("DELETE FROM specialty_progress WHERE specialty_code='120201'")
                conn.execute("DELETE FROM specialty_switches WHERE specialty_code='120201'")
                conn.execute('INSERT INTO legacy_targets VALUES(?,?,?,?,?,?)',
                    (digest,'120201','http-history',canonical(active['receipts']['120201']),'pending_reconcile','{}'))
                conn.execute('INSERT INTO legacy_runs VALUES(?,?,?,?,?)',
                    ('http-history',t['task_id'],canonical(dict(task=dict(t,stage='complete'))),'historical','{}'))
            expected=advance(case)[3] if history_mode=='new_expected' else digest
            stop=tmp_path/'legacy-STOP';stop.write_text('paused');before=len(requests)
            with TransitionGuard([tmp_path/'legacy-controller.lock'],[stop]) as guard:
                m.transition_guard=guard
                ex=launch_legacy_readback(m,c,digest,'120201',total_timeout=60)
                wait_until(lambda:ex.exit_saved,seconds=45)
                result=load(c.root/'delivery-results'/(ex.execution_id+'.json'))
                runs.append(dict(result=result,exit=ex.exit_evidence))
            assert result['state']=='active',result
            assert result['identity']['manifest_sha256']==expected
            assert commits.ledger.active(t['source_id'],'120201')['identity']['manifest_sha256']==digest
            assert all(method=='GET' or path=='/retrieval' for method,path in requests[before:])
            if history_mode=='new_expected':
                # The old object was read by the new owner, never made expected.
                assert commits.ledger.context(expected,'120201')['identity']==result['identity']
                return
        b_docs=copy.deepcopy({key:doc for key,doc in backend.docs.items() if key[0] in ('paper-two','images-two')})
        m.pause('pause before freeze command');before=len(requests)
        commits.freeze_active(t['source_id'],'120201','invalid body and image')
        assert len(requests)==before
        assert commits.ledger.safety(t['source_id'],'120201',digest)['state']=='pending'
        wait_until(lambda:not m.pause_pending)
        m.resume()
        assert run('120201')['state']=='frozen'
        a_docs={key:doc for key,doc in backend.docs.items() if key not in b_docs}
        assert len(a_docs)==2 and len(images)==2
        assert all(doc['status']=='0' for doc in a_docs.values())
        assert all(ch['available'] is False for _,did in a_docs for ch in backend.chunks[did])
        assert b_docs=={key:doc for key,doc in backend.docs.items() if key in b_docs}
        before=len(requests)
        assert run('120201')['state']=='frozen'
        assert run('120202')['already_active'] is True
        assert all(method=='GET' or path=='/retrieval' for method,path in requests[before:])
        assert commits.ledger.active(t['source_id'],'120201') is None
        assert not m.errors
    finally:
        server.shutdown();server.server_close();thread.join()
        save(Path('evidence/safety-http'+('-'+history_mode if history_mode else '')+'.json'),dict(endpoint='loopback only',runs=runs,
            document_states=[dict(dataset_id=ds,document_id=did,status=doc['status']) for (ds,did),doc in backend.docs.items()],
            process_identities=[identity for run in runs for identity in run['exit']['identities']]))

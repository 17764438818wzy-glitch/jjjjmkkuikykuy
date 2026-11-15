from email.parser import BytesParser
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import json
import os
from pathlib import Path
import sys
import threading
import time

from paperflow.package import load,save
from paperflow.scheduler import Scheduler
from test_processing_graph import graph_case
from test_processing_harness import Handler as ModelHandler
from test_delivery_flow import IndexedRag
from test_repair_graph import request as repair_request
from test_winjob import wait_until


class RagHandler(BaseHTTPRequestHandler):
    def log_message(self,*a):pass
    def do_GET(self):self.dispatch('GET')
    def do_POST(self):self.dispatch('POST')
    def do_PATCH(self):self.dispatch('PATCH')
    def dispatch(self,method):
        server=self.server
        with server.lock:server.inflight+=1;server.peak=max(server.peak,server.inflight)
        try:
            time.sleep(.003)
            endpoint=self.path.removeprefix('/api/v1');parts=endpoint.split('?')[0].strip('/').split('/')
            server.http.append((method,endpoint))
            if endpoint=='/pf-stage4/identity':
                self.send_response(200);self.end_headers();self.wfile.write(json.dumps(server.identity).encode());return
            if server.fail_b and method=='GET' and '/paper-two/' in endpoint:
                self.send_response(503);self.end_headers();return
            raw=self.rfile.read(int(self.headers.get('Content-Length',0)))
            if method=='GET' and len(parts)==4 and parts[0]=='datasets':
                self.send_response(200);self.end_headers();self.wfile.write(server.uploads[(parts[1],parts[3])]);return
            if 'multipart/form-data' in self.headers.get('Content-Type',''):
                message=BytesParser().parsebytes(('Content-Type: '+self.headers['Content-Type']+'\r\n\r\n').encode()+raw)
                part=message.get_payload()[0];data=part.get_payload(decode=True)
                did=server.rag.upload_bytes(parts[1],part.get_filename(),data);server.uploads[(parts[1],did)]=data
                value=[server.rag.docs[(parts[1],did)]]
            else:value=server.rag.request(method,endpoint,json.loads(raw) if raw else None)
            self.send_response(200);self.end_headers();self.wfile.write(json.dumps(dict(code=0,data=value)).encode())
        finally:
            with server.lock:server.inflight-=1


def start_server(handler):
    server=ThreadingHTTPServer(('127.0.0.1',0),handler);server.daemon_threads=True
    thread=threading.Thread(target=lambda:server.serve_forever(poll_interval=.05),daemon=True)
    return server,thread


def drive(scheduler,predicate,seconds=80):
    deadline=time.monotonic()+seconds
    while time.monotonic()<deadline:
        scheduler.tick()
        if predicate():return
        time.sleep(.05)
    raise AssertionError(scheduler.ledger.rows('SELECT kind,state,data FROM scheduler_jobs'))


def test_real_scheduler_sdk_graph_http_specialties_and_queued_repair(graph_case,tmp_path,monkeypatch):
    c,store,task,m,config_path=graph_case
    c.min_free_gib=0;c.max_memory_percent=100;c.max_ingest_backlog=1
    catalog=c.catalog;catalog['majors'].append(dict(code='120202',name='B',paper_dataset_id='paper-two',image_dataset_id='images-two'))
    save(c.catalog_path,catalog)
    for filename,key in [('profile.json','routing_candidates'),('corrections.json','final_routing_candidates')]:
        path=c.root/'fixture-material'/filename;value=load(path);value[key].append(dict(code='120202',name='B'));save(path,value)
    model,mt=start_server(ModelHandler);model.mode='actual';model.requests_seen=0;model.stop_stream=threading.Event()
    model.streaming=threading.Event();mt.start()
    rag,rt=start_server(RagHandler);rag.rag=IndexedRag();rag.uploads={};rag.http=[];rag.fail_b=True
    rag.inflight=0;rag.peak=0;rag.lock=threading.Lock()
    rag.identity=dict(deployment='synthetic-stage6-http',dataset_ids=['paper-ds','image-ds','paper-two','images-two'])
    rt.start();c.rag_url=f'http://127.0.0.1:{rag.server_port}'
    report=tmp_path/'visibility.json'
    # Local fixture deployment only, not evidence for an installed RAGFlow deployment.
    probe=dict(passed=True,index=dict(available_int=0,vectors=True),query={},disabled_reads=[{},{}])
    save(report,dict(passed=True,endpoint=c.rag_url,identity=rag.identity,probes=dict(a_body=probe,a_images=probe)))
    c.delivery_acceptance_report=str(report);c.save(config_path)
    scheduler=Scheduler(m,c,isolation_root=tmp_path)
    actual_launch=m.launch
    def launch(kind,key,argv,**kwargs):
        argv=list(argv)
        if 'paperflow.delivery_worker' in argv:
            offset=argv.index('-m');argv[offset:offset+2]=[str(Path('tests/fixtures/scheduling_delivery_worker.py').resolve())]
        return actual_launch(kind,key,argv,**kwargs)
    monkeypatch.setattr(m,'launch',launch)
    def graph_launch(manager,config,key,*,schedule_attempt):
        return manager.launch('graph',key,[sys.executable,'-X','utf8',str(Path('tests/fixtures/processing_worker.py').resolve()),
            str(config_path),str(store.path),key,'actual',f'http://127.0.0.1:{model.server_port}/v1'],
            cwd=Path.cwd(),env={'PYTHONPATH':str(Path.cwd())},total_timeout=150,schedule_attempt=schedule_attempt)
    scheduler.launchers['graph']=graph_launch
    try:
        drive(scheduler,lambda:bool(m.delivery.ledger.active(task['source_id'],'120201')) and bool(scheduler.ledger.rows(
            "SELECT 1 FROM scheduler_jobs j JOIN scheduler_rounds r ON r.job_id=j.job_id WHERE j.kind='delivery' "
            "AND j.state='retry' AND json_extract(j.data,'$.specialty_code')='120202'")))
        assert model.requests_seen==10 and rag.peak==1
        a_before=dict(m.delivery.ledger.active(task['source_id'],'120201'))
        # Other specialty actions may take the shared slot between short attempts.
        # Every sent 503 is saved and charged once; yielding early must be explained by resource_busy.
        attempts=scheduler.ledger.rows("SELECT a.* FROM scheduler_attempts a JOIN scheduler_jobs j ON j.job_id=a.job_id "
            "WHERE json_extract(j.data,'$.specialty_code')='120202'")
        sent=0
        for attempt in attempts:
            results=scheduler.ledger.rows('SELECT result FROM control_actions WHERE execution_id=? AND result IS NOT NULL',
                (attempt['execution_id'],))
            failures=[row for row in results if json.loads(row['result']).get('fault',{}).get('status')==503]
            assert len(failures)<=3
            if not failures:continue
            sent+=len(failures)
            assert len(scheduler.ledger.rows('SELECT 1 FROM scheduler_rounds WHERE attempt_id=?',(attempt['attempt_id'],)))==1
            if len(failures)<3:
                result=scheduler.ledger.rows('SELECT outcome,data FROM control_execution_results WHERE execution_id=?',
                    (attempt['execution_id'],))[0]
                assert result['outcome']=='deferred' and json.loads(result['data'])['fault']['reason']=='resource_busy'
        assert sent>=1 and len([x for x in rag.http if x[0]=='GET' and '/paper-two/' in x[1]])==sent
        assert len(m.ledger.rows('SELECT * FROM delivery_packages'))==1
        assert scheduler.service.services['rag']['state']=='closed'  # One article is not a service-wide diagnosis.
        rag.fail_b=False;rag.http.clear()
        scheduler.ledger._write(lambda db:db.execute("UPDATE scheduler_jobs SET due=0 WHERE state='retry'"))
        drive(scheduler,lambda:bool(m.delivery.ledger.active(task['source_id'],'120202')))
        assert m.delivery.ledger.active(task['source_id'],'120201')==a_before
        assert all('/paper-ds/' not in path for method,path in rag.http)
        drive(scheduler,lambda:not scheduler.ledger.rows("SELECT 1 FROM scheduler_attempts WHERE state!='finished'"))
        rid=repair_request(graph_case)['repair_request_id']
        from paperflow.repair_control import launch_repair
        def repair_launch(manager,config,request_id,*,schedule_attempt):
            # Reuse the stage-5 synthetic repair tool; ownership/start/budget use the shipped launcher.
            original=manager.launch
            def with_env(kind,key,argv,**kw):kw['env']={'PF_TEST_REPAIR':request_id,'PYTHONPATH':str(Path.cwd())};return original(kind,key,argv,**kw)
            manager.launch=with_env
            try:return launch_repair(manager,config,request_id,schedule_attempt=schedule_attempt,worker_argv=[sys.executable,'-X','utf8',
                str(Path('tests/fixtures/processing_worker.py').resolve()),str(config_path),str(store.path),task['task_id'],'normal'])
            finally:manager.launch=original
        scheduler.launchers['repair']=repair_launch
        drive(scheduler,lambda:m.processing.repairs.ledger.get(rid)['state']=='complete')
        digest=json.loads(m.processing.repairs.ledger.get(rid)['result'])['manifest_sha256']
        drive(scheduler,lambda:all(m.delivery.ledger.active(task['source_id'],code)['identity']['manifest_sha256']==digest
                                   for code in ('120201','120202')))
        drive(scheduler,lambda:not scheduler.ledger.rows("SELECT 1 FROM scheduler_attempts WHERE state!='finished'"))
        assert m.processing.repairs.ledger.used(task)==1 and model.requests_seen==10 and rag.peak==1
        assert len(m.ledger.rows('SELECT * FROM delivery_packages'))==2
        active_before_freeze=m.ledger.rows('SELECT source_id,specialty_code,manifest_sha256 FROM specialty_active')
        b_before=m.delivery.ledger.active(task['source_id'],'120202')
        m.delivery.freeze_active(task['source_id'],'120201','synthetic post-completion freeze')
        drive(scheduler,lambda:m.delivery.ledger.active(task['source_id'],'120201') is None)
        drive(scheduler,lambda:not scheduler.ledger.rows("SELECT 1 FROM scheduler_attempts WHERE state!='finished'"))
        assert m.delivery.ledger.active(task['source_id'],'120202')==b_before
        assert m.delivery.ledger.safety(task['source_id'],'120201',digest)['state']=='disabled'
        save(Path('evidence/scheduling-flow.json'),dict(model='installed SDK/dsh.cmd via loopback SSE; synthetic content',
            rag='native HTTP client against local synthetic RAG; fixture visibility evidence only',model_requests=model.requests_seen,
            peak_rag_http=rag.peak,b_failed_requests=sent,repair_request=rid,correction_budget_used=1,
            jobs=scheduler.ledger.rows('SELECT kind,state,data FROM scheduler_jobs'),resources=m.status()['resources'],
            active_before_freeze=active_before_freeze,frozen_a_disabled=True,b_unaffected=True,
            active=m.ledger.rows('SELECT source_id,specialty_code,manifest_sha256 FROM specialty_active')))
    finally:
        m.pause('acceptance complete');wait_until(lambda:m.status()['live_units']==0,seconds=35)
        model.stop_stream.set();model.shutdown();model.server_close();mt.join()
        rag.shutdown();rag.server_close();rt.join()

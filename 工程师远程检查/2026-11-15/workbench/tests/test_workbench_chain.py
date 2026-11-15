"""HTTP controls -> real SDK/graph/Job/IPC/ledger -> loopback RAG fixture.

The model output and RAG storage are synthetic. This is UI integration evidence,
not an acceptance report for the installed server or article quality.
"""
import json
import os
from pathlib import Path
import shutil
import sys
import threading
import time

from paperflow.package import load,save
from test_delivery_flow import IndexedRag
from test_processing_harness import Handler as ModelHandler
from test_scheduling_acceptance import RagHandler,start_server
from test_workbench import web_case,login,select,command
from test_managed_entry import entry_case
from test_winjob import wait_until


class PausableModel(ModelHandler):
    def do_POST(self):
        self.server.mode = 'actual-pause' if self.server.requests_seen >= 2 and not self.server.release_round2 else 'actual'
        super().do_POST()


def test_web_controls_resume_cached_round1_and_deliver_through_real_graph(web_case,package_factory):
    workbench,client,_ = web_case
    config = workbench.config
    headers = login(client)
    material,_,catalog = package_factory('web-fixture')
    save(config.catalog_path,catalog)
    model,mt = start_server(PausableModel)
    model.requests_seen=0;model.release_round2=False
    model.stop_stream=threading.Event();model.streaming=threading.Event();mt.start()
    rag,rt=start_server(RagHandler)
    rag.rag=IndexedRag();rag.uploads={};rag.http=[];rag.fail_b=False
    rag.inflight=0;rag.peak=0;rag.lock=threading.Lock()
    rag.identity={'deployment':'synthetic-workbench-http','dataset_ids':['paper-ds','image-ds']}
    rt.start()
    config.rag_url=f'http://127.0.0.1:{rag.server_port}'
    probe={'passed':True,'index':{'available_int':0,'vectors':True},'query':{},'disabled_reads':[{},{}]}
    save(config.delivery_acceptance_report,{'passed':True,'endpoint':config.rag_url,
        'identity':rag.identity,'probes':{'a_body':probe,'a_images':probe},'synthetic':True})
    saved=config.root/'web-test-config.json';config.save(saved)
    original_launch=workbench.manager.launch
    def managed_launch(kind,key,argv,**kwargs):
        argv=list(argv)
        if 'paperflow.delivery_worker' in argv:
            i=argv.index('-m');argv[i:i+2]=[str(Path('tests/fixtures/scheduling_delivery_worker.py').resolve())]
        return original_launch(kind,key,argv,**kwargs)
    workbench.manager.launch=managed_launch
    def graph_launch(manager,c,key,*,schedule_attempt):
        task=workbench.store.get(key)
        fixture=config.root/'fixture-material'
        if not fixture.exists():
            shutil.copytree(material,fixture)
            for name in ('profile.json','tree.json','assets.json','chunks.json','review.json','corrections.json'):
                value=load(fixture/name)
                value.update({k:task[k] for k in ('task_id','source_id','source_sha256')})
                save(fixture/name,value)
        return manager.launch('graph',key,[sys.executable,'-B','-X','utf8',
            str(Path('tests/fixtures/processing_worker.py').resolve()),str(saved),str(workbench.store.path),key,
            'actual',f'http://127.0.0.1:{model.server_port}/v1'],cwd=Path.cwd(),
            env={'PYTHONPATH':os.pathsep.join([str(Path.cwd()),os.environ.get('PYTHONPATH','')])},
            total_timeout=150,schedule_attempt=schedule_attempt)
    workbench.scheduler.launchers['graph']=graph_launch
    started=time.monotonic()
    try:
        selected=select(client,headers,name='隔离链路验收.txt',data='研究方法：指标分解。'.encode())
        response=command(client,headers,'/api/start',inputs=[selected],request_id='sdk-graph-delivery')
        assert response.status_code==200,response.text
        wait_until(model.streaming.is_set,seconds=50)
        before_pause=client.get('/api/status').json()
        assert before_pause['articles'][0]['stage']=='round2'
        paused=client.post('/api/pause',headers=headers,json={'epoch':workbench.manager.epoch})
        assert paused.status_code==200 and not paused.json()['manager']['admission_open']
        wait_until(lambda:workbench.manager.status()['state']=='paused',seconds=15)
        assert not rag.http
        phase_calls_before=[json.loads(x)['stage'] for x in (config.root/'actual-tools.jsonl').read_text().splitlines()]
        assert phase_calls_before==['round1']
        model.release_round2=True;model.stop_stream.set()
        resumed=command(client,headers,'/api/continue')
        assert resumed.status_code==200,resumed.text
        wait_until(lambda:bool(client.get('/api/status').json()['articles'][0]['complete']),seconds=80)
        wait_until(lambda:workbench.manager.status()['state']=='paused',seconds=10)
        final=client.get('/api/status').json()
        article=final['articles'][0]
        assert article['complete'] and article['stage']=='handed_off'
        assert article['detail']['expected_complete']==1
        assert final['manager']['live_units']==0
        assert all(p['exit_confirmed'] for p in final['processes'])
        actual=[json.loads(x)['stage'] for x in (config.root/'actual-tools.jsonl').read_text().splitlines()]
        assert actual==['round1','round2','round3_assets','round3_redact','round3_chunk']
        assert model.requests_seen==11
        assert rag.peak==1 and article['delivery_seconds']>0 and article['graph_seconds']>0
        assert len(workbench.scheduler.ledger.rows('SELECT * FROM delivery_packages'))==1
        save(Path('evidence')/'web-chain.json',dict(passed=True,kind='synthetic-model-and-storage',
            wall_seconds=time.monotonic()-started,model_requests=model.requests_seen,completed_phases=actual,
            round1_reused=True,peak_rag_requests=rag.peak,final=final,
            actions=[dict(kind=r['kind'],state=r['state']) for r in workbench.manager.ledger.rows('SELECT kind,state FROM control_actions')]))
    finally:
        model.release_round2=True;model.stop_stream.set()
        workbench.manager.pause('acceptance cleanup')
        wait_until(lambda:workbench.manager.status()['live_units']==0,seconds=15)
        model.shutdown();model.server_close();rag.shutdown();rag.server_close()

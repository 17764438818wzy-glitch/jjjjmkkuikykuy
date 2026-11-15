import json
import os
from pathlib import Path
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer

import pytest

from paperflow.package import load,save
from test_processing_graph import graph_case
from test_winjob import wait_until


class Handler(BaseHTTPRequestHandler):
    protocol_version='HTTP/1.1'
    def log_message(self,*args):pass
    def do_POST(self):
        if self.path!='/v1/chat/completions':self.send_error(404);return
        body=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        self.server.requests_seen+=1
        self.send_response(200);self.send_header('Content-Type','text/event-stream')
        self.send_header('Connection','close');self.end_headers();self.close_connection=True
        def emit(delta,finish=None):
            chunk=dict(id='synthetic',object='chat.completion.chunk',created=int(time.time()),model='deepseek-v4.1-flash',
                choices=[dict(index=0,delta=delta,finish_reason=finish)])
            self.wfile.write(b'data: '+json.dumps(chunk).encode()+b'\n\n');self.wfile.flush()
        try:
            emit({'role':'assistant','content':''})
            if self.server.mode=='actual-pause':
                while not self.server.stop_stream.wait(.05):
                    emit({'content':'synthetic '});self.server.streaming.set()
                emit({},'stop')
            elif any(m.get('role')=='tool' for m in body['messages']):
                emit({'content':'synthetic artifacts complete'});emit({},'stop')
            else:
                emit({'tool_calls':[dict(index=0,id='call_write_fixture',type='function',
                    function=dict(name='write_fixture',arguments='{}'))]});emit({},'tool_calls')
            self.wfile.write(b'data: [DONE]\n\n');self.wfile.flush()
        except (BrokenPipeError,ConnectionResetError,ConnectionAbortedError):pass


@pytest.mark.parametrize('mode',['actual','actual-pause'])
def test_real_harness_through_processing_graph(graph_case,mode):
    c,s,t,m,config=graph_case
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler);server.daemon_threads=True
    server.mode=mode;server.requests_seen=0;server.stop_stream=threading.Event();server.streaming=threading.Event()
    threading.Thread(target=lambda:server.serve_forever(poll_interval=.1),daemon=True).start()
    ex=None
    try:
        ex=m.launch('graph',t['task_id'],[sys.executable,'-X','utf8',str(Path('tests/fixtures/processing_worker.py').resolve()),
            str(config),str(s.path),t['task_id'],mode,f'http://127.0.0.1:{server.server_port}/v1'],
            cwd=Path.cwd(),env={'PYTHONPATH':os.pathsep.join([os.environ.get('PYTHONPATH',''),str(Path.cwd())])},total_timeout=150)
        if mode=='actual-pause':
            wait_until(server.streaming.is_set,seconds=45)
            m.pause('actual producer SDK generation cancellation')
        wait_until(lambda:ex.exit_saved,seconds=90 if mode=='actual' else 13)
        events=[json.loads(line) for line in (c.root/'fixture-events.jsonl').read_text(encoding='utf-8').splitlines()]
        if mode=='actual':
            assert load(c.root/'worker-result.json')['stage']=='handed_off',events
            actual=[json.loads(line)['stage'] for line in (c.root/'actual-tools.jsonl').read_text(encoding='utf-8').splitlines()]
            assert actual==['round1','round2','round3_assets','round3_redact','round3_chunk']
            assert server.requests_seen==10
            assert [r['status'] for r in m.ledger.rows('SELECT status FROM specialty_tasks')]==['pending']
        else:
            assert len([e for e in events if e['event']=='actual_close'])==1
            assert not list((c.root/'work').glob('*/phase-cache/*/*.json'))
            assert not m.ledger.rows('SELECT * FROM delivery_packages')
            assert ex.exit_evidence['observed_at']-ex.stop_at<10.5
        assert ex.exit_evidence['all_recorded_processes_exited']
    finally:
        if ex and not ex.exit_saved:m.pause('test cleanup');wait_until(lambda:ex.exit_saved,seconds=13)
        server.stop_stream.set();server.shutdown();server.server_close()
        save(Path('evidence')/('processing-harness-'+mode+'.json'),dict(
            mode=mode,endpoint='loopback controlled SSE; synthetic content, no production upstream',
            requests_seen=server.requests_seen,events=m.events,exit=ex.exit_evidence if ex else None))

import json
import os
from pathlib import Path
import shutil
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from paperflow.control import Manager
from paperflow.store import Store
from test_winjob import wait_until


class ProbeHandler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    def log_message(self, *args): pass
    def do_POST(self):
        if self.path != '/v1/chat/completions':
            self.send_error(404); return
        body=json.loads(self.rfile.read(int(self.headers.get('Content-Length','0'))))
        self.server.requests_seen += 1
        self.send_response(200)
        self.send_header('Content-Type','text/event-stream')
        self.send_header('Connection','close'); self.end_headers(); self.close_connection=True
        def emit(delta, finish=None):
            data=dict(id='chatcmpl-probe',object='chat.completion.chunk',created=int(time.time()),
                model='deepseek-v4.1-flash',choices=[dict(index=0,delta=delta,finish_reason=finish)])
            self.wfile.write(b'data: '+json.dumps(data).encode()+b'\n\n');self.wfile.flush()
        try:
            emit({'role':'assistant','content':''})
            if self.server.mode in ('generation','simultaneous'):
                while not self.server.stop_probe.wait(.05):
                    emit({'content':'controlled-probe '}); self.server.first_chunk.set()
                emit({},'stop')
            elif any(item.get('role')=='tool' for item in body.get('messages',[])):
                emit({'content':'probe completed'});emit({},'stop')
            else:
                emit({'tool_calls':[dict(index=0,id='call_stage2_hold',type='function',
                    function=dict(name='stage2_hold',arguments='{}'))]})
                emit({},'tool_calls')
            self.wfile.write(b'data: [DONE]\n\n');self.wfile.flush()
        except (BrokenPipeError,ConnectionResetError,ConnectionAbortedError):
            self.server.disconnected_at=time.monotonic()


def records(path):
    if not path.exists(): return []
    result=[]
    for line in path.read_text(encoding='utf-8').splitlines():
        try: result.append(json.loads(line))
        except ValueError: pass
    return result


@pytest.mark.parametrize('mode',['generation','tool','ignore','unresponsive','simultaneous'])
def test_actual_harness_cancellation(tmp_path, mode):
    root=tmp_path/mode;root.mkdir()
    server=ThreadingHTTPServer(('127.0.0.1',0),ProbeHandler)
    server.daemon_threads=True
    server.mode=mode;server.requests_seen=0;server.first_chunk=threading.Event()
    server.stop_probe=threading.Event();server.disconnected_at=None
    threading.Thread(target=lambda:server.serve_forever(poll_interval=.1),daemon=True).start()
    manager=Manager(Store(root/'ledger.sqlite3'),root/'manager').start();manager.resume()
    execution=None
    try:
        execution=manager.launch('graph',mode,[sys.executable,str(Path('tests/fixtures/harness_worker.py').resolve()),
            str(root),f'http://127.0.0.1:{server.server_port}/v1',mode],cwd=Path.cwd(),
            env={'PYTHONPATH':os.pathsep.join([os.environ.get('PYTHONPATH',''),str(Path.cwd())])},total_timeout=80)
        if mode in ('generation','simultaneous'):
            wait_until(server.first_chunk.is_set,seconds=50)
        else:
            wait_until(lambda:any(item['event']=='entered' for item in records(root/'tool.jsonl')),seconds=50)
        manager.pause('Harness acceptance '+mode)
        wait_until(lambda:execution.exit_saved,seconds=13)
        worker=records(root/'worker.jsonl');tool=records(root/'tool.jsonl')
        assert sum(item['event']=='close_started' for item in worker)==1
        assert execution.exit_evidence['all_recorded_processes_exited']
        assert len(execution.exit_evidence['process_exits'])==len(execution.exit_evidence['identities'])
        sdk_root=next(item['identity'] for item in worker if item['event']=='sdk_root')
        assert any(item['pid']==sdk_root['pid'] and item['created_at']==sdk_root['created_at']
                   for item in execution.exit_evidence['process_exits'])
        if mode=='tool':
            assert any(item['event']=='aborted' for item in tool)
        if mode=='ignore':
            assert not any(item['event']=='aborted' for item in tool)
            assert execution.exit_evidence['observed_at']-execution.stop_at<10.5
        elif mode=='unresponsive':
            assert execution.kill_sent is not None
            assert 9.9<=execution.kill_sent-execution.stop_at<10.5
        else:
            assert any(item['event']=='shutdown_ack' for item in worker)
        if mode in ('generation','simultaneous'):
            wait_until(lambda:server.disconnected_at is not None)
    finally:
        manager.shutdown(timeout=35)
        server.stop_probe.set();server.shutdown();server.server_close()
        report=dict(mode=mode,endpoint='loopback controlled SSE; no production upstream',errors=manager.errors,
            requests_seen=server.requests_seen,disconnected_at=server.disconnected_at,
            events=manager.events,exit=execution.exit_evidence if execution else None,
            worker=records(root/'worker.jsonl'),tool=records(root/'tool.jsonl'))
        Path('evidence/harness-'+mode+'.json').write_text(json.dumps(report,indent=2),encoding='utf-8')

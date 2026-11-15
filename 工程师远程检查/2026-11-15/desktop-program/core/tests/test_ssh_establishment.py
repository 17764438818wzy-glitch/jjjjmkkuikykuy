"""Cross-process connection establishment is bounded; live sessions are not."""
import io
import json
from pathlib import Path
import subprocess
import sys
import threading
import time

import pytest

from paperflow.remote_gateway import Gateway
from test_remote_gateway import header
from test_winjob import wait_until


SSH = '''
import json,sys,time
from pathlib import Path
root=Path(sys.argv[1]);name=sys.argv[2]
h=json.loads(sys.stdin.buffer.readline())
(root/(name+'.auth')).touch()
while not (root/'begin').exists():time.sleep(.005)
time.sleep(.2)
(root/(name+'.established')).touch()
if h.get('connection_protocol')=='queued-ssh/1':
 print(json.dumps(dict(connection_protocol='queued-ssh/1',ready=True)),flush=True)
while not (root/'release').exists():time.sleep(.005)
'''

CLIENT = '''
import json,sys,subprocess
from pathlib import Path
import paperflow.remote_transport as transport
root=Path(sys.argv[1]);name=sys.argv[2]
real=transport.subprocess.Popen
transport.ssh_args=lambda settings:[]
transport.subprocess.Popen=lambda _args,**kw:real([sys.executable,'-B',str(root/'ssh.py'),str(root),name],**kw)
(root/(name+'.attempt')).touch()
p=transport.spawn(dict(host='isolated',username='test',key_file=str(root/'key')),
                  dict(op='run',namespace='a'*64,execution_id='b'*32,epoch='c'*32,job_id='d'*32))
p.stdin.close();p.wait(timeout=15);p.stdout.close()
'''


def test_many_processes_queue_authentication_but_keep_all_sessions_running(tmp_path):
    (tmp_path/'ssh.py').write_text(SSH);(tmp_path/'key').write_text('test fixture, not a credential')
    children=[];peak_auth=0;peak_live=0;errors=[]
    try:
        for n in range(12):
            children.append(subprocess.Popen([sys.executable,'-B','-c',CLIENT,str(tmp_path),str(n)],
                stdout=subprocess.PIPE,stderr=subprocess.PIPE,creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0)))
        wait_until(lambda:len(list(tmp_path.glob('*.attempt')))==12,seconds=8)
        time.sleep(.3);(tmp_path/'begin').touch()
        end=time.monotonic()+12
        while time.monotonic()<end:
            auth=len(list(tmp_path.glob('*.auth')));live=len(list(tmp_path.glob('*.established')))
            peak_auth=max(peak_auth,auth-live);peak_live=max(peak_live,live)
            if live==12:break
            time.sleep(.005)
        assert peak_auth<=3, f'{peak_auth} SSH handshakes started together'
        assert peak_live==12, 'a connection slot must be released before its long session finishes'
    finally:
        (tmp_path/'begin').touch();(tmp_path/'release').touch()
        for child in children:
            try:_,error=child.communicate(timeout=15)
            except subprocess.TimeoutExpired:child.kill();_,error=child.communicate()
            if error:errors.append(error.decode(errors='replace'))
    assert not errors,errors


def test_gateway_ack_is_opt_in_and_precedes_the_normal_response(tmp_path):
    gateway=Gateway(tmp_path,dict(image=header()['image'],version='fixture'))
    h=dict(header(),op='health')
    old=io.BytesIO();gateway.handle(h,io.BytesIO(),old)
    assert len(old.getvalue().splitlines())==1
    new=io.BytesIO();gateway.handle(dict(h,connection_protocol='queued-ssh/1'),io.BytesIO(),new)
    lines=[json.loads(line) for line in new.getvalue().splitlines()]
    assert len(lines)==2,'authentication acknowledgement was not emitted'
    assert lines[0]==dict(connection_protocol='queued-ssh/1',ready=True)
    assert lines[1]['ready'] and lines[1]['image']==h['image']


def test_waiting_for_connection_honors_cancel_before_spawning(tmp_path,monkeypatch):
    from paperflow import remote_transport as transport
    from paperflow.instance_lock import InstanceLock
    settings=dict(host='isolated',username='test',key_file=str(tmp_path/'key'))
    monkeypatch.setattr(transport,'ssh_args',lambda _:[])
    stop=threading.Event();stop.set()
    monkeypatch.setattr(transport.subprocess,'Popen',lambda *a,**k:pytest.fail('a stopped request spawned SSH'))
    with pytest.raises(ValueError,match='stopped'):
        transport.spawn(settings,dict(op='run'),cancelled=stop.is_set)


def test_control_connections_have_a_reserved_establishment_slot(tmp_path):
    from contextlib import ExitStack
    from paperflow.remote_transport import connection_slot
    settings=dict(host='isolated',username='test',key_file=str(tmp_path/'key'))
    with ExitStack() as stack:
        for _ in range(3):stack.enter_context(connection_slot(settings,False,time.monotonic()+1,None))
        with pytest.raises(TimeoutError):
            with connection_slot(settings,False,time.monotonic()+.1,None):pytest.fail('normal lane exceeded capacity')
        with connection_slot(settings,True,time.monotonic()+1,None):pass


def test_pause_cancels_an_already_waiting_request_without_opening_ssh(tmp_path,monkeypatch):
    from contextlib import ExitStack
    from paperflow import remote_transport as transport
    settings=dict(host='isolated',username='test',key_file=str(tmp_path/'key'))
    monkeypatch.setattr(transport,'ssh_args',lambda _:[])
    monkeypatch.setattr(transport.subprocess,'Popen',lambda *a,**k:pytest.fail('queued cancellation opened SSH'))
    stopped=threading.Event();result=[]
    def waiting():
        try:transport.spawn(settings,dict(op='run'),cancelled=stopped.is_set)
        except ValueError as error:result.append(str(error))
    with ExitStack() as stack:
        for _ in range(3):stack.enter_context(transport.connection_slot(settings,False,time.monotonic()+1,None))
        thread=threading.Thread(target=waiting);thread.start();time.sleep(.1);stopped.set();thread.join(1)
        assert not thread.is_alive() and result==['remote SSH connection stopped']


def test_stalled_authentication_times_out_and_releases_its_slot(tmp_path,monkeypatch):
    from contextlib import ExitStack
    from paperflow import remote_transport as transport
    settings=dict(host='isolated',username='test',key_file=str(tmp_path/'key'))
    monkeypatch.setattr(transport,'ssh_args',lambda _:[sys.executable,'-B','-c','import time;time.sleep(10)'])
    started=time.monotonic()
    with pytest.raises(TimeoutError,match='authentication'):
        transport.spawn(settings,dict(op='run'),timeout=.2)
    assert time.monotonic()-started<2
    with ExitStack() as stack:
        for _ in range(3):stack.enter_context(transport.connection_slot(settings,False,time.monotonic()+1,None))

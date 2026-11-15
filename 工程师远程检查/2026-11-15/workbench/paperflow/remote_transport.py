"""Restricted SSH transport. Payloads use stdin; no credentials or commands in arguments."""
import json
import math
import hashlib
import os
from pathlib import Path
import subprocess
import threading
import time
import uuid
from contextlib import contextmanager

from .instance_lock import InstanceLock
from .remote_diagnostics import safe_context,safe_detail,stage,ssh_detail,RemoteQueueBusy

from .remote_files import write_archive,receive_archive

MAX_LINE=2*1024**2
CONNECTION_PROTOCOL='queued-ssh/1'


@contextmanager
def connection_slot(settings,control,deadline,cancelled):
    """Only connection establishment is scarce. Locks release on process exit.

    All workers use the same existing key directory, never their per-Job TEMP.
    Three startup slots plus one reserved for health/exit confirmation keep a
    burst below sshd's unauthenticated-connection threshold.
    """
    identity=json.dumps([settings['host'],settings.get('port',22),settings['username']])
    root=Path(settings['key_file']).resolve().parent/'.paperflow-ssh'/hashlib.sha256(identity.encode()).hexdigest()[:24]
    slots=(3,0,1,2) if control else (0,1,2)
    while True:
        if cancelled and cancelled():raise ValueError('remote SSH connection stopped')
        if time.monotonic()>=deadline:raise RemoteQueueBusy('remote SSH connection queue timed out')
        for number in slots:
            lock=InstanceLock(root/(str(number)+'.lock'))
            try:lock.__enter__()
            except RuntimeError:continue
            try:
                if cancelled and cancelled():raise ValueError('remote SSH connection stopped')
                if time.monotonic()>=deadline:raise RemoteQueueBusy('remote SSH connection queue timed out')
                yield
            finally:lock.__exit__()
            return
        time.sleep(.05)


def read_json(stream):
    line=stream.readline(MAX_LINE+1)
    if not line or len(line)>MAX_LINE:raise ConnectionError('remote Harness response absent or oversized')
    value=json.loads(line)
    if not isinstance(value,dict):raise ValueError('remote Harness response is not an object')
    return value


def write_json(stream,value):
    data=(json.dumps(value,ensure_ascii=False,allow_nan=False)+'\n').encode('utf-8')
    if len(data)>MAX_LINE:raise ValueError('remote Harness request oversized')
    stream.write(data);stream.flush()


def ssh_args(settings):
    allowed={'host','port','username','key_file','known_hosts','image','version','deployment_id'}
    if (not isinstance(settings,dict) or set(settings)-allowed or not settings.get('host')
            or not settings.get('username') or any(c in settings['host']+settings['username'] for c in '\n\r @')):
        raise ValueError('invalid remote Harness endpoint')
    key=Path(settings['key_file']).resolve()
    known=Path(settings.get('known_hosts',str(Path.home()/'.ssh/known_hosts'))).resolve()
    if not key.is_file() or not known.is_file():raise ValueError('remote Harness key or host identity unavailable')
    executable=str(Path(os.environ.get('SystemRoot',r'C:\Windows'))/'System32/OpenSSH/ssh.exe') if os.name=='nt' else 'ssh'
    return [executable,'-T','-i',str(key),'-p',str(settings.get('port',22)),
        '-o','BatchMode=yes','-o','IdentitiesOnly=yes','-o','StrictHostKeyChecking=yes',
        '-o','UserKnownHostsFile='+str(known),'-o','ConnectTimeout=3',
        '-o','ServerAliveInterval=2','-o','ServerAliveCountMax=2',settings['username']+'@'+settings['host']]


def endpoint_identity(settings):
    ssh=Path(ssh_args(settings)[0]);executable=str(ssh.with_name('ssh-keygen.exe')) if os.name=='nt' else 'ssh-keygen'
    host=settings['host'] if settings.get('port',22)==22 else '['+settings['host']+']:'+str(settings['port'])
    known=str(Path(settings.get('known_hosts',str(Path.home()/'.ssh/known_hosts'))).resolve())
    result=subprocess.run([executable,'-F',host,'-f',known],capture_output=True,text=True,
        creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0),timeout=3)
    keys=sorted(' '.join(line.split()[1:3]) for line in result.stdout.splitlines() if line and not line.startswith('#'))
    if result.returncode or not keys:raise ValueError('remote SSH host identity is not pinned')
    return dict(host=settings['host'],port=settings.get('port',22),username=settings['username'],
        host_keys_sha256=hashlib.sha256('\n'.join(keys).encode()).hexdigest(),deployment_id=settings['deployment_id'])


def spawn(settings,header,*,timeout=120,cancelled=None,capture_stderr=False):
    arguments=ssh_args(settings)
    started=time.monotonic();deadline=started+timeout
    header=dict(header,request_id=header.get('request_id') or uuid.uuid4().hex)
    context=dict(operation=header.get('op'),request_id=header['request_id'],timeout_seconds=timeout)
    control=header.get('op') in ('health','reap_job','reap_execution','status_job')
    budget=header.get('control_budget')
    connection_deadline=min(deadline,started+budget['queue_seconds']) if budget else deadline
    with stage('ssh_queue',**context):
        with connection_slot(settings,control,connection_deadline,cancelled):
            context['queue_seconds']=time.monotonic()-started
            if budget:
                header=dict(header,control_budget=dict(budget,queue_seconds=max(0,budget['queue_seconds']-context['queue_seconds'])))
            with stage('ssh_auth',**context):
                process=subprocess.Popen(arguments,stdin=subprocess.PIPE,stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE if capture_stderr else subprocess.DEVNULL,
                    creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
                established=threading.Event();expired=threading.Event()
                auth_deadline=min(deadline,time.monotonic()+8);auth_started=time.monotonic()
                def watch():
                    while not established.wait(.025):
                        if time.monotonic()>=auth_deadline or (cancelled and cancelled()):
                            expired.set()
                            try:process.kill()
                            except OSError:pass
                            return
                watcher=threading.Thread(target=watch,daemon=True);watcher.start()
                try:
                    write_json(process.stdin,dict(header,connection_protocol=CONNECTION_PROTOCOL))
                    ready=read_json(process.stdout)
                    if ready!=dict(connection_protocol=CONNECTION_PROTOCOL,ready=True):
                        raise ConnectionError('remote SSH authentication acknowledgement absent')
                    if cancelled and cancelled():raise ValueError('remote SSH connection stopped')
                except BaseException as exc:
                    if process.poll() is None:process.kill()
                    process.wait()
                    # SSH diagnostics are classified, not copied into control logs.
                    error=process.stderr.read(4096) if process.stderr else b''
                    for stream in (process.stdin,process.stdout,process.stderr):
                        if stream and not stream.closed:stream.close()
                    if cancelled and cancelled():exc=ValueError('remote SSH connection stopped')
                    elif expired.is_set():exc=TimeoutError('remote SSH authentication timed out')
                    exc.remote_context=safe_context(dict(context,auth_seconds=time.monotonic()-auth_started))
                    exc.remote_error=ssh_detail(error,process.returncode)
                    raise exc
                finally:
                    established.set();watcher.join(.1)
                context['auth_seconds']=time.monotonic()-auth_started
                process.remote_context=safe_context(context)
    # The authenticated operation/session outlives the establishment slot.
    return process


def request(settings,header,*,files=None,destination=None,timeout=12,cancelled=None,queue_timeout=0):
    if (type(queue_timeout) not in (int,float) or not math.isfinite(queue_timeout) or not 0<=queue_timeout<=120
            or type(timeout) not in (int,float) or not math.isfinite(timeout) or timeout<=0):
        raise ValueError('invalid remote request budget')
    if queue_timeout:
        if header.get('op') not in ('status_job','reap_job','reap_execution','prepare','update','collect','update_child','collect_child'):
            raise ValueError('queue budget requires a bounded remote request')
        header=dict(header,control_budget=dict(queue_seconds=queue_timeout,operation_seconds=timeout))
    transfer=header.get('op') in ('prepare','update','collect','update_child','collect_child')
    if transfer:header=dict(header,transfer_protocol='queued-transfer/1')
    started=time.monotonic()
    process=spawn(settings,header,timeout=timeout+queue_timeout+(8 if queue_timeout else 0),cancelled=cancelled,capture_stderr=True)
    context=process.remote_context;operation_started=time.monotonic();phase='request_upload';expired=threading.Event()
    # Authentication has its own existing <=8s bound; connection queue time
    # debits the same finite queue budget sent to the server. Heartbeats never
    # change this deadline, and Docker I/O retains its original timeout.
    deadline=started+timeout+queue_timeout+(context.get('auth_seconds',0) if queue_timeout else 0)
    stopped=threading.Event();finished=threading.Event()
    def expire():
        expired.set()
        try:process.kill()
        except OSError:pass
    timer=threading.Timer(max(.001,deadline-time.monotonic()),expire);timer.daemon=True;timer.start()
    watcher=None
    if cancelled:
        def watch():
            while not finished.wait(.025):
                if cancelled():
                    stopped.set()
                    try:process.kill()
                    except OSError:pass
                    return
        watcher=threading.Thread(target=watch,daemon=True);watcher.start()
    def check():
        if stopped.is_set() or (cancelled and cancelled()):raise ValueError('remote Harness request cancelled')
        if expired.is_set() or time.monotonic()>=deadline:raise TimeoutError('remote Harness operation timed out')
    def response():
        while True:
            check();value=read_json(process.stdout);check()
            if value.get('event')!='heartbeat':break
            if (value.get('phase') not in ('control_queue','transfer_queue') or value.get('request_id')!=context.get('request_id')):
                raise ValueError('remote control progress identity mismatch')
        if value.get('error'):
            known_queue=(value.get('error')=='RemoteQueueBusy' and value.get('request_id')==context.get('request_id')
                and value.get('detail',{}).get('reason')=='resource_queue_busy')
            error=RemoteQueueBusy('remote resource queue timed out') if known_queue else ConnectionError('remote Harness operation failed')
            error.remote_error=safe_detail(value.get('detail',dict(type=value['error'])))
            raise error
        return value
    try:
        if transfer:
            phase='transfer_queue';ready=response()
            if ready!=dict(event='transfer_ready',request_id=context.get('request_id')):
                raise ConnectionError('remote transfer acknowledgement absent')
        check();phase='request_upload'
        if files is not None:write_archive(process.stdin,files)
        check();process.stdin.close()
        phase='request_response'
        value=response()
        if destination is not None:
            phase='request_download';receive_archive(process.stdout,destination,value['files'])
        check();phase='request_exit'
        if process.wait(timeout=max(.001,deadline-time.monotonic()))!=0:raise ConnectionError('remote Harness transport failed')
        check()
        return value
    except Exception as exc:
        if process.poll() is None:process.kill();process.wait()
        error=process.stderr.read(4096) if process.stderr else b''
        if stopped.is_set() or (cancelled and cancelled()):exc=ValueError('remote Harness request cancelled')
        elif expired.is_set() or time.monotonic()>=deadline:exc=TimeoutError('remote Harness operation timed out')
        if not getattr(exc,'remote_error',None):exc.remote_error=ssh_detail(error,process.returncode)
        exc.remote_context=safe_context(dict(context,phase=phase,elapsed_seconds=time.monotonic()-started,
            operation_seconds=time.monotonic()-operation_started))
        raise exc
    finally:
        timer.cancel();finished.set()
        if watcher:watcher.join(.1)
        if process.poll() is None:process.kill();process.wait()
        for stream in (process.stdin,process.stdout,process.stderr):
            if stream and not stream.closed:stream.close()

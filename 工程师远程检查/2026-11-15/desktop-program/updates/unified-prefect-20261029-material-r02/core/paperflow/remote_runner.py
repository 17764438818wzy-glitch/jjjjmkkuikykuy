"""Container PID 1 child: existing SDK, one stage/session family, bounded owner lease."""
import json
import os
from pathlib import Path
import sys
import threading
import time


class Lease:
    heartbeat_seconds=3
    hard_exit_seconds=5
    def __init__(self,now=None):
        self.last=time.monotonic() if now is None else now;self.actions=set();self.deadline=None
    def heartbeat(self,now=None):self.last=time.monotonic() if now is None else now
    def expired(self,now=None):
        now=time.monotonic() if now is None else now
        return now-self.last>=self.heartbeat_seconds or (self.deadline is not None and now>=self.deadline)
    def action(self,identity,now,seconds):
        if not isinstance(identity,str) or not identity or identity in self.actions or not 0<seconds<=86400:
            raise ValueError('remote model action reused or expired')
        self.actions.add(identity);self.deadline=now+seconds
    def idle(self):self.deadline=None


def compact_result(result):
    events=[]
    for event in result.events:
        if event.get('type')=='assistant/message' and isinstance(event.get('data',{}).get('usage'),dict):
            events.append(dict(type='assistant/message',data=dict(usage=event['data']['usage'])))
        elif event.get('type')=='turn/end':events.append(event)
    return dict(finish_reason=result.finish_reason,events=events)


class Runner:
    def __init__(self,root):
        self.root=Path(root);self.lease=Lease();self.harness=None
        self.stopping=threading.Event();self.active=threading.Lock();self.write_lock=threading.Lock()
    def emit(self,value):
        with self.write_lock:
            sys.stdout.write(json.dumps(value,ensure_ascii=False,allow_nan=False)+'\n');sys.stdout.flush()
    def stop(self):
        if self.stopping.is_set():return
        self.stopping.set()
        # Process-space exit terminates every remaining process in this owned container.
        timer=threading.Timer(self.lease.hard_exit_seconds,lambda:os._exit(137));timer.daemon=True;timer.start()
        def close():
            try:
                if self.harness:self.harness.close()
                self.emit(dict(event='sdk_closed'))
                os._exit(0)
            except BaseException:os._exit(1)
        threading.Thread(target=close,daemon=True).start()
    def watch(self):
        while not self.stopping.wait(.05):
            if self.lease.expired():self.stop()
    def call(self,request):
        identity=request['id']
        if not self.active.acquire(blocking=False):
            self.emit(dict(id=identity,error='remote operation already active'));return
        try:
            if self.stopping.is_set():raise ValueError('remote owner stopped')
            method=request['method']
            if method=='initialize':
                from deepseek_harness import DeepSeekHarness
                cfg=json.loads((self.root/'job.json').read_text())
                self.harness=DeepSeekHarness(cwd='/job/output',dsh_home='/job/home',
                    dsh_bin='/opt/paperflow/dsh',profile='sdk',
                    patches=('/job/config/route.yml','/job/config/tools.yml'),
                    provider=cfg['provider'],model=cfg['model'],max_tokens=49152,
                    initialize_timeout_seconds=45,request_timeout_seconds=cfg['model_timeout'],shutdown_timeout_seconds=1,
                    env=dict(cfg.get('env',{}),PKG_NATIVE_CACHE_PATH='/job/home/native-cache',
                        NARB_NATIVE_CACHE_DIR='/job/home/native-cache',TMPDIR='/tmp',
                        LANGSMITH_TRACING='false',LANGCHAIN_TRACING_V2='false',**cfg.get('group_env',{})))
                self.harness.start();result=dict(initialized=True,runner_pid=os.getpid())
            elif method=='run':
                if not self.harness:raise ValueError('remote SDK not initialized')
                self.lease.action(request['action_id'],time.monotonic(),request['seconds'])
                result=compact_result(self.harness.run(request['prompt'],session_id=request['session_id']))
                self.lease.idle()
            else:raise ValueError('unsupported remote method')
            self.emit(dict(id=identity,result=result))
        except BaseException as exc:
            # No manuscripts, model traces or credentials in control-plane errors.
            self.emit(dict(id=identity,error=type(exc).__name__))
        finally:self.active.release()
    def run(self):
        threading.Thread(target=self.watch,daemon=True).start()
        try:
            while line:=sys.stdin.buffer.readline(2*1024**2+1):
                if len(line)>2*1024**2:raise ValueError('remote request oversized')
                request=json.loads(line)
                if request.get('method')=='heartbeat':
                    self.lease.heartbeat();self.emit(dict(event='heartbeat'));continue
                if request.get('method')=='close':self.stop();break
                threading.Thread(target=self.call,args=(request,),daemon=True).start()
        finally:self.stop()
        while True:time.sleep(.1)


class GroupRunner(Runner):
    """One existing SDK transport, independent native Agent inboxes."""
    def call(self,request):
        if request['method']=='initialize':return super().call(request)
        identity=request['id']
        try:
            if self.stopping.is_set() or not self.harness:raise ValueError('group owner unavailable')
            if request['method'] not in ('group/start','group/continue','group/wait','group/close','group/status'):
                raise ValueError('unsupported group method')
            from pydantic import BaseModel
            class Value(BaseModel):value:dict
            # Native child deadlines plus the existing whole-parent heartbeat.
            result=self.harness.client.request(request['method'],
                {k:v for k,v in request.items() if k not in ('id','method')},response_model=Value).value
            if request['method']=='group/status':
                result.update(memory_bytes=int(Path('/sys/fs/cgroup/memory.current').read_text()),load=os.getloadavg()[0])
            self.emit(dict(id=identity,result=result))
        except BaseException as exc:self.emit(dict(id=identity,error=type(exc).__name__))


if __name__=='__main__':
    cfg=json.loads(Path('/job/job.json').read_text())
    (GroupRunner if cfg.get('mode')=='group' else Runner)('/job').run()

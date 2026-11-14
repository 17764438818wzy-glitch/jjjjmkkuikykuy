"""start/run/close adapter; only Harness executes remotely, files and workflow stay local."""
import hashlib
import json
import os
from pathlib import Path
import queue
import re
import shutil
import threading
import time
from types import SimpleNamespace
import uuid

from .package import save,sha
from .remote_files import file_map,manifest,map_text,publish_output
from .remote_transport import request,spawn,read_json,write_json
from .remote_diagnostics import describe,RemoteQueueBusy
from .instrumentation import timed,measured


def namespace(ledger):return hashlib.sha256(str(Path(ledger).resolve()).casefold().encode()).hexdigest()


def current_permit(client):
    permit=getattr(client,'active_permit',None)
    if (client.stopped.is_set() or client.closed.is_set() or client.active_kind!='llm'
            or not permit or permit['epoch']!=client.epoch or time.monotonic()>=permit['deadline']):
        raise ValueError('remote Harness requires a current immediate LLM permit')
    return permit


class RemoteHarness:
    shutdown_wait_seconds=10
    def __new__(cls,config,*args,**kwargs):
        if cls is RemoteHarness and getattr(config,'remote_harness_groups',0):
            from .remote_groups import GroupProxy
            return GroupProxy(config,*args,**kwargs)
        return super().__new__(cls)
    def __init__(self,config,control,output,home,tools):
        if not control or not hasattr(control,'client'):raise ValueError('remote Harness requires managed controls')
        self.config,self.control,self.output,self.home,self.tools=config,control,Path(output),Path(home),tools
        self.settings=config.remote_harness;self.client=control.client
        self.job_id=uuid.uuid4().hex
        self.identity=dict(namespace=namespace(config.processing_ledger_path),execution_id=self.client.execution_id,
            epoch=self.client.epoch,job_id=self.job_id)
        self.folder=config.root/'private/remote-executions'/self.job_id
        self.folder.mkdir(parents=True,exist_ok=False)
        self.path=self.folder/'status.json';self.record_lock=threading.Lock()
        self.record=dict(self.identity,host=self.settings['host'],version=self.settings.get('version'),
            image=self.settings['image'],state='prepared_local',exit_confirmed=False,started_at=time.time())
        self._record()
        self.process=None;self.pending={};self.pending_lock=threading.Lock();self.write_lock=threading.Lock()
        self.stopped=threading.Event();self.exited=threading.Event();self.close_lock=threading.Lock()
        self.proof=None;self.started=False;self.task=None
        self.launch_ready=threading.Event();self.channel_ended=threading.Event()
        self.launch_observation=None;self.launch_queue_busy=False
    def _record(self,**values):
        with self.record_lock:
            self.record.update(values);save(self.path,self.record)
    def configure(self,task):
        if not re.fullmatch('[A-Za-z0-9_-]{1,128}',task['task_id']):raise ValueError('invalid remote task identity')
        self.task=task
        self.mapping={str(self.output):'/job/output',str(self.config.catalog_path):'/job/input/catalog.json',
            str(task['source_path']):'/job/input/original/'+Path(task['source_path']).name}
        if task.get('reading_dir'):self.mapping[str(task['reading_dir'])]='/job/input/reading'
        self._record(task_id=task['task_id'])
        # Only a new stage may replace the pointer; older reader threads update
        # their own status file, never the next stage's process identity.
        save(self.config.root/'private/remote-current'/(task['task_id']+'.json'),self.record)
    def _header(self,op,**extra):return dict(self.identity,op=op,image=self.settings['image'],deployment_id=self.settings['deployment_id'],**extra)
    def _request(self,op,**kwargs):
        try:
            with timed('upload' if op in ('prepare','update','update_child') else 'download' if op in ('collect','collect_child') else 'other_processing'):
                return request(self.settings,self._header(op,**kwargs.pop('header',{})),**kwargs)
        except RemoteQueueBusy:
            if op not in ('prepare','update'):raise
            from .scheduling_policy import ActionDeferred
            raise ActionDeferred('resource_busy') from None
    def _cancelled(self):return self.stopped.is_set() or self.client.stopped.is_set() or self.client.closed.is_set()
    def _files(self):
        import yaml
        if not self.task:raise ValueError('remote task context absent')
        if sha(Path(self.task['source_path']))!=self.task['source_sha256']:raise ValueError('remote original identity changed')
        job=self.folder/'job.json'
        # Only the existing model route's explicit environment can cross machines.
        env={k:os.environ[k] for k in ('FREE_API_KEY','DEEPSEEK_API_KEY','DEEPSEEK_BASE_URL') if k in os.environ}
        save(job,dict(provider=self.config.provider,model=self.config.model,model_timeout=self.config.model_timeout,env=env))
        tools=self.folder/'tools.yml';tools.write_text(yaml.safe_dump(self.tools,allow_unicode=True),encoding='utf-8')
        files={'job.json':job,'config/tools.yml':tools,
            'config/route.yml':Path(__file__).with_name('prompts')/'free-route.yml',
            'input/catalog.json':Path(self.config.catalog_path),
            'input/original/'+Path(self.task['source_path']).name:Path(self.task['source_path'])}
        credentials=self.home/'.credentials.yaml'
        if credentials.is_file():files['home/.credentials.yaml']=credentials
        if self.task.get('reading_dir'):files.update(file_map(self.task['reading_dir'],'input/reading'))
        files.update(file_map(self.output,'output'))
        return files
    def start(self):
        if self.started:return
        current_permit(self.client)
        files=self._files();items=manifest(files)
        self._record(state='creating',manifest_sha256=hashlib.sha256(json.dumps(items,sort_keys=True).encode()).hexdigest())
        try:self._request('prepare',header=dict(files=items),files=files,timeout=120,queue_timeout=60,cancelled=self._cancelled)
        finally:(self.folder/'job.json').unlink(missing_ok=True)
        current_permit(self.client)
        if self.stopped.is_set():raise ValueError('remote Harness start cancelled')
        permit=current_permit(self.client)
        startup_seconds=min(self.config.model_timeout,permit['deadline']-time.monotonic())
        try:self.process=spawn(self.settings,self._header('run',startup_seconds=startup_seconds),cancelled=self._cancelled)
        except RemoteQueueBusy:
            from .scheduling_policy import ActionDeferred
            raise ActionDeferred('resource_busy') from None
        threading.Thread(target=self._read,daemon=True).start()
        threading.Thread(target=self._heartbeat,daemon=True).start()
        deadline=min(current_permit(self.client)['deadline'],time.monotonic()+self.config.model_timeout)
        while not self.launch_ready.wait(.05):
            current_permit(self.client)
            if self._cancelled():raise ValueError('remote Harness start cancelled')
            if self.channel_ended.is_set() or self.process.poll() is not None:
                if self.launch_queue_busy:
                    from .scheduling_policy import ActionDeferred
                    raise ActionDeferred('resource_busy')
                raise ConnectionError('remote Harness startup channel ended')
            if time.monotonic()>=deadline:raise TimeoutError('remote Harness startup queue timed out')
        current_permit(self.client)
        if self._cancelled() or self.channel_ended.is_set():raise ValueError('remote Harness start cancelled')
        self._rpc('initialize',timeout=60)
        current_permit(self.client)
        if self._cancelled() or self.channel_ended.is_set():raise ConnectionError('remote Harness channel ended during initialization')
        # The gateway already inspected actual Running/PID and bound the ready
        # event to this exact job/image. Do not create a second SSH/Docker burst.
        observation=self.launch_observation
        self.started=True;self._record(state='idle',remote_pid=observation['pid'],container=observation['container'])
    def _send(self,value):
        with self.write_lock:
            if not self.process or self.process.poll() is not None:raise ConnectionError('remote Harness channel closed')
            write_json(self.process.stdin,value)
    def _heartbeat(self):
        while not self.stopped.wait(.5):
            if self.client.stopped.is_set() or self.client.closed.is_set():
                self.stopped.set()
                try:self._send(dict(method='close'))
                except Exception:pass
                return
            permit=getattr(self.client,'active_permit',None)
            # active_kind is set before the Manager finishes admission. Keep the
            # existing idle session alive while waiting; run still requires a grant.
            if self.client.active_kind=='llm' and permit and time.monotonic()>=permit['deadline']:
                self.stopped.set();return
            try:self._send(dict(method='heartbeat'))
            except Exception:self.stopped.set();return
    def _read(self):
        observed=0
        try:
            while self.process and self.process.stdout:
                value=read_json(self.process.stdout)
                if (not self.launch_ready.is_set() and value.get('error')=='RemoteQueueBusy'
                        and value.get('detail',{}).get('reason')=='resource_queue_busy'
                        and value.get('request_id')==getattr(self.process,'remote_context',{}).get('request_id')
                        and value.get('request_id')):
                    self.launch_queue_busy=True;break
                if value.get('event')=='remote_started':
                    expected='paperflow-harness-'+self.identity['namespace'][:12]+'-'+self.job_id
                    if (value.get('job_id')!=self.job_id or value.get('image')!=self.settings['image']
                            or type(value.get('pid')) is not int or value['pid']<=0 or value.get('container')!=expected):
                        raise ValueError('remote startup identity mismatch')
                    self.launch_observation=dict(pid=value['pid'],container=value['container'])
                    self.launch_ready.set();continue
                if value.get('event')=='remote_exit':
                    self._accept_proof(value['proof']);break
                if value.get('event')=='heartbeat':
                    if time.monotonic()-observed>2:
                        self._record(last_remote_heartbeat=time.time());observed=time.monotonic()
                    continue
                if value.get('id'):
                    with self.pending_lock:q=self.pending.get(value['id'])
                    if q:q.put_nowait(value)
        except Exception:pass
        finally:
            self.channel_ended.set()
            with self.pending_lock:
                for q in self.pending.values():
                    try:q.put_nowait(dict(error='remote channel ended'))
                    except queue.Full:pass
    @measured('remote_runtime_or_wait')
    def _rpc(self,method,timeout=None,**params):
        ident=uuid.uuid4().hex;q=queue.Queue(1)
        with self.pending_lock:self.pending[ident]=q
        deadline=time.monotonic()+(timeout or self.config.model_timeout)
        try:
            self._send(dict(id=ident,method=method,**params))
            while True:
                if self.client.stopped.is_set() or self.client.closed.is_set() or self.stopped.is_set():raise ValueError('remote Harness cancelled')
                if time.monotonic()>=deadline:raise TimeoutError('remote Harness response timed out')
                try:value=q.get(timeout=.1);break
                except queue.Empty:pass
            if value.get('error'):
                from .scheduling_policy import ServiceConfigurationError
                if method=='initialize':raise ServiceConfigurationError('remote Harness initialization: '+str(value['error'])[:100])
                raise ConnectionError('remote Harness operation failed: '+str(value['error'])[:100])
            return value['result']
        finally:
            with self.pending_lock:self.pending.pop(ident,None)
    def run(self,prompt,*,session_id):
        permit=current_permit(self.client)
        self.start()
        files=file_map(self.output)
        self._request('update',header=dict(files=manifest(files)),files=files,timeout=120,queue_timeout=60,cancelled=self._cancelled)
        permit=current_permit(self.client)
        mapped=map_text(prompt,self.mapping)
        self.prompt_sha256=hashlib.sha256(mapped.encode()).hexdigest()
        self._record(state='running',session_id=session_id,action_id=permit['action_id'],prompt_sha256=self.prompt_sha256)
        result=self._rpc('run',prompt=mapped,session_id=session_id,action_id=permit['action_id'],
            seconds=min(self.config.model_timeout,permit['deadline']-time.monotonic()))
        stage=self.output.with_name(self.output.name+'.remote-'+uuid.uuid4().hex)
        try:
            self._collect(stage)
            current_permit(self.client)
            publish_output(stage,self.output)
        finally:
            if stage.exists():shutil.rmtree(stage)
        self._record(state='idle',collection_pending=False)
        return SimpleNamespace(**result)
    @measured('download')
    def _collect(self,stage):
        # The model has already finished. Retry only the same read-only result
        # transfer; never re-enter _rpc('run') for local resource contention.
        for attempt in range(3):
            current_permit(self.client)
            try:return request(self.settings,self._header('collect'),destination=stage,timeout=120,
                queue_timeout=60,cancelled=self._cancelled)
            except RemoteQueueBusy:
                self._record(state='collection_pending',collection_pending=True)
                if attempt==2:raise ValueError('remote result retained; transfer capacity unavailable') from None
                if self.stopped.wait(.25):raise ValueError('remote Harness collection stopped') from None
    def _accept_proof(self,proof):
        if not proof.get('all_exited') or not proof.get('job_closed') or proof.get('job_id')!=self.job_id or proof.get('image')!=self.settings['image']:
            raise ValueError('remote exit proof mismatch')
        self._record(state='exited',exit_confirmed=True,proof=proof,ended_at=time.time())
        self.proof=proof;self.exited.set()
    def close(self):
        with self.close_lock:
            self.stopped.set()
            try:
                if self.process and not self.exited.is_set():
                    def send_close():
                        try:self._send(dict(method='close'))
                        except Exception:pass
                    sender=threading.Thread(target=send_close,daemon=True);sender.start();sender.join(.05)
                    self.exited.wait(1.5)
                if not self.exited.is_set():
                    # Local cooperation keeps its <=10s window. Longer queue
                    # waits belong to the existing asynchronous Manager guard.
                    proof=request(self.settings,self._header('reap_job'),timeout=6)
                    self._accept_proof(proof)
            except Exception as exc:
                self._record(state='exit_pending',exit_error=describe(exc))
            finally:
                if self.process:
                    try:self.process.wait(timeout=.5)
                    except Exception:
                        self.process.kill()
                        try:self.process.wait(timeout=.5)
                        except Exception:pass
                    for name in ('stdin','stdout','stderr'):
                        stream=getattr(self.process,name,None)
                        if stream and not stream.closed:stream.close()
                if self.proof is not None and self.exited.is_set():self._record(state='exited',exit_confirmed=True)

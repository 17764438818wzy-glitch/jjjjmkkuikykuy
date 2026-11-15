"""Fixed SSH command on Linux; can only manage this application's owned containers."""
import contextlib
from contextvars import ContextVar
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
import uuid

if __package__ in (None,''):
    sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from paperflow.remote_files import file_map,manifest,receive_archive,write_archive
from paperflow.remote_diagnostics import stage,describe,safe_context,RemoteQueueBusy


def save(path,value):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    temp=path.with_name(path.name+'.'+uuid.uuid4().hex)
    with temp.open('x',encoding='utf-8') as stream:
        json.dump(value,stream,ensure_ascii=False);stream.flush();os.fsync(stream.fileno())
    temp.replace(path)


def emit(stream,value):
    stream.write((json.dumps(value,ensure_ascii=False)+'\n').encode());stream.flush()


def host_memory(path=Path('/proc/meminfo')):
    """Host RAM; this fixed gateway runs on Linux, outside Harness containers."""
    try:
        fields={}
        for line in path.read_text(encoding='ascii').splitlines():
            key,*parts=line.split()
            if key in ('MemTotal:','MemAvailable:'):
                if len(parts)!=2 or parts[1]!='kB':return None
                fields[key]=int(parts[0])*1024
        total,available=fields['MemTotal:'],fields['MemAvailable:']
        if not 0<=available<=total or total<=0:return None
        return dict(total_bytes=total,used_bytes=total-available,available_bytes=available)
    except (OSError,ValueError,KeyError):return None


class ControlBudget:
    """One finite wall budget; lock waits cannot consume the Docker I/O budget."""
    def __init__(self,raw=None,*,operation_seconds=12,output=None,request_id=None,check=None,role='query'):
        value=raw if raw is not None else dict(queue_seconds=60,operation_seconds=operation_seconds)
        if (not isinstance(value,dict) or set(value)!={'queue_seconds','operation_seconds'}
                or any(type(v) not in (int,float) or not math.isfinite(v) for v in value.values())
                or not 0<=value['queue_seconds']<=120 or not 0<value['operation_seconds']<=operation_seconds):
            raise ValueError('invalid remote control budget')
        self.queue,self.operation=value['queue_seconds'],value['operation_seconds']
        self.started=time.monotonic();self.waited=0;self.waiting=None;self.last_progress=0
        self.output,self.request_id,self.check_open,self.role=output,request_id,check,role
    def remaining(self):
        now=time.monotonic();waiting=now-self.waiting if self.waiting is not None else 0
        return self.operation-(now-self.started-self.waited-waiting),self.queue-self.waited-waiting
    def check(self):
        active,queued=self.remaining()
        if active<=0:raise TimeoutError('remote control operation timed out')
        if queued<0:raise RemoteQueueBusy('remote control queue timed out')
        if self.check_open:self.check_open()
        return active
    def begin_wait(self):
        if self.waiting is None:self.waiting=time.monotonic()
    def end_wait(self):
        if self.waiting is not None:self.waited+=time.monotonic()-self.waiting;self.waiting=None
    def wait(self,phase='control_queue'):
        self.check()
        if self.output is not None and time.monotonic()-self.last_progress>=.5:
            try:emit(self.output,dict(event='heartbeat',phase=phase,request_id=self.request_id))
            except (OSError,ValueError):
                if self.role!='exit':raise ConnectionError('remote query channel closed') from None
                self.output=None  # Cleanup continues after its caller disconnects.
            self.last_progress=time.monotonic()
        time.sleep(min(.15,.04+.01*(os.getpid()%5),max(.001,self.remaining()[1])))


class Gateway:
    def __init__(self,root,config):
        self.root=Path(root).resolve();self.config=config
        self.root.mkdir(parents=True,exist_ok=True)
        self._control=ContextVar('paperflow-control-'+str(id(self)),default=None)
    def validate(self,h):
        for key,length in (('namespace',64),('execution_id',32),('epoch',32),('job_id',32)):
            if not re.fullmatch('[0-9a-f]{'+str(length)+'}',h.get(key,'')):raise ValueError('invalid remote identity')
        if h.get('image')!=self.config['image'] or h.get('deployment_id')!=self.config.get('deployment_id'):
            raise ValueError('remote image or deployment mismatch')
        return self.root/h['namespace']/h['execution_id']
    @contextlib.contextmanager
    def locked(self,path=None,*,blocking=True):
        import fcntl
        with (Path(path) if path is not None else self.root/'.lock').open('a') as stream:
            fcntl.flock(stream,fcntl.LOCK_EX|(0 if blocking else fcntl.LOCK_NB))
            try:yield
            finally:fcntl.flock(stream,fcntl.LOCK_UN)
    @contextlib.contextmanager
    def control_scope(self,h=None,*,role='query',output=None,operation_seconds=12,check=None):
        current=self._control.get()
        if current is not None:
            yield current;return
        if check is None:check=(lambda:self.check_open(h)) if h is not None and role=='query' else None
        budget=ControlBudget((h or {}).get('control_budget'),operation_seconds=operation_seconds,
            output=output,request_id=(h or {}).get('request_id'),check=check,role=role)
        token=self._control.set(budget)
        try:yield budget
        finally:self._control.reset(token)
    @contextlib.contextmanager
    def wait_lock(self,path):
        budget=self._control.get();assert budget is not None
        try:
            while True:
                budget.check()
                with contextlib.ExitStack() as stack:
                    try:stack.enter_context(self.locked(path,blocking=False))
                    except BlockingIOError:
                        budget.begin_wait();budget.wait();continue
                    budget.end_wait();budget.check();yield;return
        finally:budget.end_wait()
    @contextlib.contextmanager
    def control_slot(self):
        budget=self._control.get();assert budget is not None
        slots=self.config.get('control_slots',2)
        if type(slots) is not int or not 2<=slots<=4:raise ValueError('invalid Docker control capacity')
        numbers=(*range(slots-1),) if budget.role!='exit' else (slots-1,*range(slots-1))
        with self.capacity_slot('.control',numbers) as budget:yield budget
    @contextlib.contextmanager
    def capacity_slot(self,directory,numbers):
        budget=self._control.get();assert budget is not None
        phase='control_queue' if directory=='.control' else 'transfer_queue'
        directory=self.root/directory;directory.mkdir(exist_ok=True)
        try:
            while True:
                budget.check()
                for number in numbers:
                    with contextlib.ExitStack() as stack:
                        try:stack.enter_context(self.locked(directory/(str(number)+'.lock'),blocking=False))
                        except BlockingIOError:continue
                        budget.end_wait();budget.check();yield budget;return
                budget.begin_wait()
                with stage(phase):budget.wait(phase)
        finally:budget.end_wait()
    def docker(self,args,*,timeout,before_dispatch=None,**kwargs):
        # Every inspect/rm/ps shares these cross-process slots. Neither an SSH
        # session nor a long model call holds one; exits have a reserved slot.
        with self.control_slot() as budget:
            budget.check();undo=before_dispatch() if before_dispatch else None
            try:seconds=min(timeout,budget.check())
            except BaseException:
                if undo:undo()  # No command was sent; do not leave a sent marker.
                raise
            result=subprocess.run(args,timeout=seconds,**kwargs)
            budget.check();return result
    def startup_deadline(self,h):
        seconds=h.get('startup_seconds',60)
        if type(seconds) not in (int,float) or not math.isfinite(seconds) or not 0<seconds<=86400:
            raise ValueError('invalid remote startup lifetime')
        return time.monotonic()+seconds
    def check_startup(self,h,input_stream,deadline):
        self.check_open(h)
        if time.monotonic()>=deadline:raise TimeoutError('remote startup queue timed out')
        if input_stream is not None and os.name!='nt':
            import select
            # POLLHUP is visible even when unread RPC bytes remain. Do not peek
            # by reading a byte: Docker must receive the original stream intact.
            try:
                poller=select.poll();poller.register(input_stream.fileno(),select.POLLHUP|select.POLLERR|select.POLLNVAL)
                closed=any(flags&(select.POLLHUP|select.POLLERR|select.POLLNVAL) for _,flags in poller.poll(0))
            except (OSError,ValueError):raise ConnectionError('remote startup input state unknown') from None
            if closed:raise ConnectionError('remote startup input closed')
    def startup_inspect(self,h,input_stream,deadline,name):
        self.check_startup(h,input_stream,deadline)
        remaining=max(.001,deadline-time.monotonic())
        limited=dict(h,control_budget=dict(queue_seconds=min(120,remaining),operation_seconds=min(5,remaining)))
        with self.control_scope(limited,check=lambda:self.check_startup(h,input_stream,deadline)):
            result=self.inspect(name)
            self.check_startup(h,input_stream,deadline);return result
    @contextlib.contextmanager
    def startup_slot(self,h,output_stream,input_stream=None,*,deadline=None):
        # An SSH connection is already established here. Limit Docker startup,
        # not authenticated connections or the lifetime of an LLM session.
        slots=self.config.get('startup_slots',2)
        if type(slots) is not int or not 1<=slots<=4:raise ValueError('invalid remote startup capacity')
        directory=self.root/'.startup';directory.mkdir(exist_ok=True);observed=0
        if deadline is None:deadline=self.startup_deadline(h)
        with stage('startup_queue',job_id=h['job_id']):
            while True:
                self.check_startup(h,input_stream,deadline)
                for number in range(slots):
                    with contextlib.ExitStack() as stack:
                        try:stack.enter_context(self.locked(directory/(str(number)+'.lock'),blocking=False))
                        except BlockingIOError:continue
                        self.check_startup(h,input_stream,deadline)
                        yield
                        return
                if time.monotonic()-observed>=.5:
                    emit(output_stream,dict(event='heartbeat',phase='startup_queue',job_id=h['job_id']))
                    observed=time.monotonic()
                time.sleep(.05)
    def check_open(self,h):
        path=self.validate(h)
        if (path/'closed.json').exists() or (path/h['job_id']/'closed.json').exists():raise ValueError('remote execution closed')
        return path/h['job_id']
    def name(self,h):return 'paperflow-harness-'+h['namespace'][:12]+'-'+h['job_id']
    def inspect(self,name):
        with self.control_scope():
            result=self.docker(['docker','inspect',name],capture_output=True,text=True,timeout=5)
        if result.returncode:
            if 'no such object' in result.stderr.lower() or 'no such container' in result.stderr.lower():return None
            error=ConnectionError('remote Docker state unknown')
            error.remote_error=describe(subprocess.CalledProcessError(result.returncode,result.args,stderr=result.stderr))
            raise error
        return json.loads(result.stdout)[0]
    def owned(self,d,h):
        labels=d['Config'].get('Labels',{})
        if (d['Image']!=self.config['image'] or any(labels.get('paperflow.'+k)!=h[k] for k in ('namespace','execution_id','epoch','job_id'))):
            raise ValueError('remote container ownership mismatch')
    def prepare(self,h,stream):
        with self.locked():
            target=self.check_open(h)
            if target.exists():raise ValueError('remote job intent already exists; reconcile instead of recreating')
            target.mkdir(parents=True)
            save(target/'intent.json',{k:h[k] for k in ('namespace','execution_id','epoch','job_id')})
        stage=self.root/('upload-'+uuid.uuid4().hex)
        try:
            receive_archive(stream,stage,h['files'])
            required=['job.json','config/route.yml','config/tools.yml']
            if any(not (stage/name).is_file() for name in required):raise ValueError('remote launch files missing')
            for part in ('home','output','input'):(stage/part).mkdir(exist_ok=True)
            for path in [stage,*stage.rglob('*')]:
                os.chown(path,1000,1000);os.chmod(path,0o700 if path.is_dir() else 0o600)
            with self.locked():
                self.check_open(h);stage.rename(target/'data')
                save(target/'manifest.json',h['files'])
            return dict(prepared=True,job_id=h['job_id'])
        finally:
            if stage.exists():shutil.rmtree(stage)
    def update(self,h,stream):
        with self.locked():target=self.check_open(h)
        stage=self.root/('upload-'+uuid.uuid4().hex)
        try:
            receive_archive(stream,stage,h['files'])
            with self.control_scope(h,operation_seconds=120),self.wait_lock(target/'.reap.lock'):
                self.check_open(h);output=target/'data/output'
                # Keep the bind-mounted directory inode; atomically replace individual files.
                # Scanning/hashing is per job and never holds the global close barrier.
                wanted=set(file_map(stage))
                before={item['path']:item['sha256'] for item in manifest(file_map(output))}
                incoming={item['path']:item['sha256'] for item in h['files']}
                for name,path in file_map(output).items():
                    if name not in wanted:
                        with self.locked():self.check_open(h);path.unlink()
                for name,path in file_map(stage).items():
                    if before.get(name)==incoming[name]:continue
                    with self.locked():
                        self.check_open(h);dest=output/name;dest.parent.mkdir(parents=True,exist_ok=True)
                        os.chown(dest.parent,1000,1000);os.chown(path,1000,1000);path.replace(dest)
                self.check_open(h)
            return dict(updated=True)
        finally:
            if stage.exists():shutil.rmtree(stage)
    def run(self,h,input_stream,output_stream):
        startup_deadline=self.startup_deadline(h)
        with self.locked():
            target=self.check_open(h)
            if not (target/'manifest.json').is_file():raise ValueError('remote job not prepared')
            name=self.name(h)
            if (target/'started.json').exists():raise ValueError('remote runtime already consumed')
        data=target/'data'
        job_path=data/'job.json'
        grouped=job_path.is_file() and json.loads(job_path.read_bytes()).get('mode')=='group'
        # A group contains eight SDK children. Its server-owned budget must not
        # inherit the single-session 1 GiB cap; swap is still excluded.
        memory_mb=self.config.get('group_memory_mb',3072) if grouped else 1024
        if type(memory_mb) is not int or not 512<=memory_mb<=8192:
            raise ValueError('invalid Harness group memory budget')
        memory=str(memory_mb)+'m'
        command=['docker','create','--name',name,'--init','--interactive','--user','1000:1000',
            '--network','host','--read-only','--cap-drop','ALL','--security-opt','no-new-privileges',
            '--pids-limit','128','--memory',memory,'--memory-swap',memory,'--tmpfs','/tmp:rw,size=256m',
            '--env','PYTHONDONTWRITEBYTECODE=1','--env','PYTHONPATH=/opt/paperflow/app',
            '--env','HOME=/job/home','--env','DSH_HOME=/job/home','--workdir','/job/output',
            '--mount','type=bind,src='+str(data)+',dst=/job,readonly',
            '--mount','type=bind,src='+str(data/'output')+',dst=/job/output',
            '--mount','type=bind,src='+str(data/'home')+',dst=/job/home']
        for key in ('namespace','execution_id','epoch','job_id'):command+=['--label','paperflow.'+key+'='+h[key]]
        command+=['--entrypoint','/opt/paperflow/python/bin/python',self.config['image'],'-B','-m','paperflow.remote_runner']
        reserved=False
        try:
            with self.startup_slot(h,output_stream,input_stream,deadline=startup_deadline):
                with self.locked(target/'.reap.lock'):
                    with self.locked():
                        self.check_startup(h,input_stream,startup_deadline)
                        if (target/'started.json').exists():raise ValueError('remote runtime already consumed')
                        active=[p for p in self.root.glob('*/*/*/started.json') if not (p.parent/'exit.json').exists()]
                        if len(active)>=self.config.get('max_live',1):raise ValueError('remote Harness capacity reserved')
                        save(target/'started.json',dict(name=name,image=self.config['image'],at=time.time(),protocol='serialized-start/1',memory_limit_bytes=memory_mb*1024**2))
                        save(target/'creation.json',dict(state='creating',at=time.time(),protocol='serialized-start/1'))
                        reserved=True
                    try:
                        with stage('container_create',job_id=h['job_id']):
                            subprocess.run(command,capture_output=True,check=True,timeout=8)
                    except BaseException:
                        save(target/'creation.json',dict(state='unknown',at=time.time(),protocol='serialized-start/1'))
                        raise
                    save(target/'creation.json',dict(state='done',at=time.time(),protocol='serialized-start/1'))
                    with stage('container_start',job_id=h['job_id']):
                        self.check_startup(h,input_stream,startup_deadline)
                        d=self.startup_inspect(h,input_stream,startup_deadline,name);self.owned(d,h)
                        with self.locked():
                            self.check_startup(h,input_stream,startup_deadline)
                            process=subprocess.Popen(['docker','start','--attach','--interactive',d['Id']],stdin=input_stream,
                                stdout=output_stream,stderr=subprocess.DEVNULL)
                        deadline=min(startup_deadline,time.monotonic()+8)
                        while True:
                            self.check_startup(h,input_stream,deadline)
                            current=self.startup_inspect(h,input_stream,deadline,d['Id'])
                            self.check_startup(h,input_stream,deadline)
                            if current is None:raise ConnectionError('remote container disappeared during startup')
                            self.owned(current,h)
                            if current['State']['Running'] and current['State']['Pid']>0:
                                emit(output_stream,dict(event='remote_started',job_id=h['job_id'],image=h['image'],
                                    pid=current['State']['Pid'],container=name))
                                break
                            if process.poll() is not None or time.monotonic()>=deadline:
                                raise TimeoutError('remote container did not reach running state')
                            time.sleep(.025)
        except BaseException:
            # Preserve the original error. An uncertain create still needs a
            # real container or the existing bound terminal failure evidence.
            if reserved:
                try:self.reap_job(h)
                except Exception:pass
            raise
        process.wait()
        proof=self.reap_job(h)
        emit(output_stream,dict(event='remote_exit',proof=proof))
    def boot_id(self):return Path('/proc/sys/kernel/random/boot_id').read_text().strip().replace('-','')
    def creator_audit(self,h,target,creation,container_id):
        path=target/'creator-audit.json';raw=path.read_bytes();value=json.loads(raw)
        identity={k:h.get(k) for k in ('namespace','execution_id','epoch','job_id','image','deployment_id')}
        if (value.get('schema')!='paperflow-legacy-creator-audit/1' or value.get('identity')!=identity
                or value.get('container_id')!=container_id or value.get('boot_id')!=self.boot_id()
                or value.get('daemon_git_commit')!='3ce5872'
                or any(value.get(k) is not True for k in ('run_gateway_absent','creator_absent','local_tree_exited'))
                or not isinstance(value.get('checked_at'),(int,float)) or value['checked_at']<json.loads(creation.read_bytes())['at']
                or any(value.get(k+'_sha256')!=hashlib.sha256((target/(k+'.json')).read_bytes()).hexdigest()
                    for k in ('creation','started','intent'))
                or not (target.parent/'closed.json').is_file()
                or json.loads((target.parent/'closed.json').read_text()).get('epoch')!=h['epoch']):
            raise ValueError('remote legacy creator audit mismatch')
        return hashlib.sha256(raw).hexdigest()
    def creation_observation(self,h,target,creation,d):
        """Bind an actual registered object; never infer creation from absence.

        On the deployed Moby daemon, inspect locks the registered container;
        creation has no second registration after that point. New creates hold
        this job lock. Legacy creators additionally require an operator audit.
        """
        identity={k:h.get(k) for k in ('namespace','execution_id','epoch','job_id','image','deployment_id')}
        intent=json.loads((target/'intent.json').read_text());started=json.loads((target/'started.json').read_text())
        marker=json.loads(creation.read_bytes())
        if (intent!={k:h[k] for k in ('namespace','execution_id','epoch','job_id')}
                or started.get('name')!=self.name(h) or started.get('image')!=h['image'] or marker.get('state')!='unknown'):
            raise ValueError('remote creation observation identity mismatch')
        path=target/'creation-observed.json';record=json.loads(path.read_bytes()) if path.exists() else None
        container_id=record.get('container_id') if record else d.get('Id') if d else None
        if not isinstance(container_id,str) or not re.fullmatch('[0-9a-f]{64}',container_id):
            raise ConnectionError('remote container creation is not confirmed complete')
        if d is not None:
            self.owned(d,h)
            if (d.get('Id')!=container_id or d.get('Name')!='/'+self.name(h)
                    or d.get('State',{}).get('Status') not in ('created','running','exited','dead','paused')):
                raise ValueError('remote observed container identity or state mismatch')
        serialized=marker.get('protocol')==started.get('protocol')=='serialized-start/1'
        try:audit=None if serialized else self.creator_audit(h,target,creation,container_id)
        except FileNotFoundError:raise ConnectionError('remote legacy creator exit is unconfirmed') from None
        binding=dict(schema='paperflow-create-observed/1',identity=identity,container_id=container_id,
            container=self.name(h),creator_audit=audit,
            **{k+'_sha256':hashlib.sha256((target/(k+'.json')).read_bytes()).hexdigest() for k in ('creation','started','intent')})
        if record:
            if any(record.get(k)!=v for k,v in binding.items()):raise ValueError('remote creation observation evidence mismatch')
        else:
            if d is None:raise ConnectionError('remote container creation is not confirmed complete')
            save(path,dict(binding,at=time.time(),observed_state=d['State']['Status']))
        return hashlib.sha256(path.read_bytes()).hexdigest(),container_id
    def creation_failure(self,h,target,creation):
        """Accept an operator-verified daemon failure, never mere absence/timeout.

        The root-only audit record binds the original unknown marker and full
        owner. It does not overwrite that marker or permit another create.
        Actual container absence is checked again by the normal exit path.
        """
        record=target/'creation-failure.json'
        if not record.is_file():return None
        raw=record.read_bytes();value=json.loads(raw)
        identity={k:h.get(k) for k in ('namespace','execution_id','epoch','job_id','image','deployment_id')}
        started=json.loads((target/'started.json').read_text())
        intent=json.loads((target/'intent.json').read_text())
        terminal=value['terminal'];message=terminal['MESSAGE'];verified=value.get('verified',{})
        url=r'request-url="/v[0-9]+\.[0-9]+/containers/create\?name='+re.escape(self.name(h))+r'"'
        if (value.get('schema')!='paperflow-create-failure/1' or value.get('identity')!=identity
                or value.get('container')!=self.name(h) or intent!={k:h[k] for k in ('namespace','execution_id','epoch','job_id')}
                or value.get('creation_sha256')!=hashlib.sha256(creation.read_bytes()).hexdigest()
                or json.loads(creation.read_text()).get('state')!='unknown'
                or started.get('name')!=self.name(h) or started.get('image')!=h['image']
                or not (target.parent/'closed.json').is_file()
                or json.loads((target.parent/'closed.json').read_text()).get('epoch')!=h['epoch']
                or terminal.get('_SYSTEMD_UNIT')!='docker.service' or terminal.get('_EXE')!='/usr/bin/dockerd'
                or terminal.get('_UID')!='0' or not re.search(url,message)
                or not re.fullmatch('[0-9a-f]{32}',terminal.get('_BOOT_ID',''))
                or not re.fullmatch('[0-9]+',terminal.get('_PID','')) or not terminal.get('__CURSOR')
                or verified.get('boot_id')!=terminal['_BOOT_ID']
                or verified.get('creator_absent') is not True or verified.get('inventory_empty') is not True
                or not re.search(r'(?:^| )method=POST(?: |$)',message)
                or not re.search(r'(?:^| )status=499(?: |$)',message)
                or 'msg="request cancelled by client"' not in message
                or 'error="failed to lookup digest: Canceled: context canceled"' not in message
                or int(terminal['__REALTIME_TIMESTAMP'])/1e6<json.loads(creation.read_text())['at']):
            raise ValueError('remote creation failure evidence mismatch')
        return hashlib.sha256(raw).hexdigest()
    def reap_job(self,h):
        with self.control_scope(h,role='exit'):return self._reap_job(h)
    def identity(self,h):return {k:h.get(k) for k in ('namespace','execution_id','epoch','job_id','image','deployment_id')}
    def close_path(self,path,h):
        if path.exists():
            if json.loads(path.read_bytes()).get('epoch')!=h['epoch']:raise ValueError('remote close identity mismatch')
        else:save(path,dict(at=time.time(),epoch=h['epoch']))
    def removal(self,h,target):
        path=target/'removal.json'
        if not path.exists():return None
        value=json.loads(path.read_bytes())
        if (value.get('schema')!='paperflow-removal/1' or value.get('identity')!=self.identity(h)
                or value.get('container')!=self.name(h) or not re.fullmatch('[0-9a-f]{64}',str(value.get('container_id','')))):
            raise ValueError('remote removal identity mismatch')
        return value
    def terminal_proof(self,h,target):
        path=target/'exit.json'
        if not path.exists():return None
        value=json.loads(path.read_bytes())
        if value.get('schema')!='paperflow-exit/2':return None
        removal=self.removal(h,target)
        if (value.get('identity')!=self.identity(h) or value.get('container')!=self.name(h)
                or value.get('job_id')!=h['job_id'] or value.get('image')!=h['image']
                or value.get('all_exited') is not True or value.get('job_closed') is not True
                or not removal or value.get('container_id')!=removal['container_id']
                or value.get('removal_sha256')!=hashlib.sha256((target/'removal.json').read_bytes()).hexdigest()):
            raise ValueError('remote terminal proof identity mismatch')
        return value
    def owned_id(self,d,h,cid=None):
        self.owned(d,h)
        if (not re.fullmatch('[0-9a-f]{64}',str(d.get('Id',''))) or d.get('Name')!='/'+self.name(h)
                or (cid is not None and d['Id']!=cid)):
            raise ValueError('remote container ownership mismatch')
        return d['Id']
    def cleanup_credentials(self,target):
        with stage('credential_cleanup',job_id=target.name):
            home=target/'data/home'
            if home.exists():shutil.rmtree(home)
            job=target/'data/job.json'
            if job.is_file():
                config=json.loads(job.read_text());config.pop('env',None);save(job,config)
    def observe_removal(self,h,cid):
        budget=self._control.get()
        with stage('container_observe',job_id=h['job_id']):
            while True:
                actual=self.inspect(cid);named=self.inspect(self.name(h))
                if actual is not None:self.owned_id(actual,h,cid)
                if named is not None:self.owned_id(named,h,cid)
                budget.check()
                if actual is None and named is None:return
                time.sleep(min(.05,budget.check()))
    @contextlib.contextmanager
    def cleanup_slot(self,h,cid):
        # An rm timeout does not end the daemon's work. Persistent ownership
        # survives both SSH exit and process restart; no TTL can free it.
        directory=self.root/'.cleanup';directory.mkdir(exist_ok=True)
        budget=self._control.get();next_probe={}
        try:
            while True:
                budget.check()
                for number in (0,1):
                    if time.monotonic()<next_probe.get(number,0):continue
                    with contextlib.ExitStack() as stack:
                        try:stack.enter_context(self.locked(directory/(str(number)+'.lock'),blocking=False))
                        except BlockingIOError:continue
                        budget.end_wait();budget.check();owner_path=directory/(str(number)+'.json')
                        if owner_path.exists():
                            owner=json.loads(owner_path.read_bytes());identity=owner.get('identity',{})
                            self.validate(identity)
                            if (owner.get('schema')!='paperflow-cleanup/1' or owner.get('container')!=self.name(identity)
                                    or not re.fullmatch('[0-9a-f]{64}',str(owner.get('container_id','')))):
                                raise ValueError('remote cleanup ownership mismatch')
                            if identity!=self.identity(h) or owner['container_id']!=cid:
                                actual=self.inspect(owner['container_id']);named=self.inspect(owner['container'])
                                if actual:self.owned_id(actual,identity,owner['container_id'])
                                if named:self.owned_id(named,identity,owner['container_id'])
                                if actual is not None or named is not None:
                                    next_probe[number]=time.monotonic()+.5;continue
                                owner_path.unlink()
                        yield owner_path;return
                budget.begin_wait();budget.wait()
        finally:budget.end_wait()
    def remove_confirm(self,h,target,cid,send):
        with self.cleanup_slot(h,cid) as owner_path:
            owner=dict(schema='paperflow-cleanup/1',identity=self.identity(h),container=self.name(h),container_id=cid)
            if send:
                def record_dispatch():
                    # Both markers are written only after command capacity and
                    # the last deadline check, immediately before Popen.
                    previous=owner_path.read_bytes() if owner_path.exists() else None
                    save(owner_path,owner)
                    save(target/'removal.json',dict(schema='paperflow-removal/1',identity=self.identity(h),
                        container=self.name(h),container_id=cid,at=time.time()))
                    def undo():
                        (target/'removal.json').unlink()
                        if previous is None:owner_path.unlink()
                        else:save(owner_path,json.loads(previous))
                    return undo
                with stage('container_remove',job_id=h['job_id']):
                    try:self.docker(['docker','rm','--force',cid],capture_output=True,check=True,timeout=6,before_dispatch=record_dispatch)
                    except subprocess.TimeoutExpired:pass
                    except subprocess.CalledProcessError as exc:
                        if describe(exc).get('reason') not in ('removal_in_progress','container_absent'):raise
            elif not owner_path.exists():save(owner_path,owner)
            self.observe_removal(h,cid)
            owner_path.unlink(missing_ok=True)
    def _reap_job(self,h):
        failure=None;observed=None;observed_id=None
        with stage('job_metadata',job_id=h['job_id']):
            with self.wait_lock(self.root/'.lock'):
                target=self.validate(h)/h['job_id'];target.mkdir(parents=True,exist_ok=True)
                self.close_path(target/'closed.json',h)
        # run(), worker.close() and the manager may converge on this same job.
        # Serialize its actual exit and cleanup, never other jobs or SSH sessions.
        with stage('job_lock',job_id=h['job_id']):
            with self.wait_lock(target/'.reap.lock'):
                name=self.name(h)
                cached=self.terminal_proof(h,target)
                if cached:
                    self.cleanup_credentials(target);self._control.get().check();return cached
                removal=self.removal(h,target)
                with stage('job_metadata',job_id=h['job_id']):
                    creation=target/'creation.json'
                    if creation.is_file() and json.loads(creation.read_text()).get('state')!='done':
                        if (target/'creation-failure.json').exists():
                            failure=self.creation_failure(h,target,creation)
                            if not failure:raise ConnectionError('remote container creation is not confirmed complete')
                        elif json.loads(creation.read_text()).get('state')=='unknown':
                            observed,observed_id=self.creation_observation(h,target,creation,self.inspect(name))
                        else:raise ConnectionError('remote container creation is not confirmed complete')
                with stage('container_inspect',job_id=h['job_id']):
                    d=self.inspect(name)
                    if failure and d is not None:raise ConnectionError('failed creation conflicts with actual container')
                    if d:self.owned_id(d,h,removal['container_id'] if removal else None)
                    if observed and d is not None and d['Id']!=observed_id:raise ValueError('remote observed container changed')
                cid=removal['container_id'] if removal else (d['Id'] if d else observed_id)
                with stage('exit_confirm',job_id=h['job_id']):
                    if cid:self.remove_confirm(h,target,cid,bool(d and not removal))
                    elif self.inspect(name) is not None:raise ConnectionError('remote process space exit unconfirmed')
                    self._control.get().check()
                    proof=dict(job_id=h['job_id'],container=name,image=self.config['image'],all_exited=True,job_closed=True,at=time.time())
                    if cid and (target/'removal.json').exists():
                        proof.update(schema='paperflow-exit/2',identity=self.identity(h),container_id=cid,
                            removal_sha256=hashlib.sha256((target/'removal.json').read_bytes()).hexdigest())
                    if failure:proof['creation_failure']=failure
                    if observed:proof['creation_observed']=observed
                    save(target/'exit.json',proof)
                self.cleanup_credentials(target)
                self._control.get().check()
                return proof
    def reap_execution(self,h):
        with self.control_scope(h,role='exit'):return self._reap_execution(h)
    def _reap_execution(self,h):
        path=self.validate(h)
        with stage('execution_metadata'):
            with self.wait_lock(self.root/'.lock'):
                path.mkdir(parents=True,exist_ok=True)
                self.close_path(path/'closed.json',h)
                intents=list(path.glob('*/intent.json'))
        proofs=[]
        for intent in intents:
            identity=json.loads(intent.read_text())
            if any(identity[k]!=h[k] for k in ('namespace','execution_id','epoch')):raise ValueError('remote owner mismatch')
            proofs.append(self.reap_job(dict(identity,image=h['image'],deployment_id=h.get('deployment_id'))))
        # Actual inventory is authoritative; intent files alone cannot prove exit.
        with stage('execution_inventory'):
            if self.inventory(h):raise ConnectionError('remote owned containers remain after reconciliation')
        return dict(execution_id=h['execution_id'],epoch=h['epoch'],execution_closed=True,all_exited=True,jobs=proofs,at=time.time())
    def inventory(self,h):
        command=['docker','ps','--all','--quiet']
        for key in ('namespace','execution_id','epoch'):command+=['--filter','label=paperflow.'+key+'='+h[key]]
        with self.control_scope(role='exit'):
            result=self.docker(command,capture_output=True,text=True,timeout=5)
        if result.returncode:
            error=ConnectionError('remote inventory unknown')
            error.remote_error=describe(subprocess.CalledProcessError(result.returncode,result.args,stderr=result.stderr))
            raise error
        return result.stdout.split()
    def collect(self,h,stream):
        path=self.validate(h)/h['job_id']/'data/output'
        files=file_map(path);items=manifest(files)
        emit(stream,dict(job_id=h['job_id'],files=items));write_archive(stream,files);stream.flush()
    def status_job(self,h):
        with self.control_scope(h):
            self.check_open(h);d=self.inspect(self.name(h))
            if d is None or not d['State']['Running']:raise ConnectionError('remote runtime is not running')
            if type(d['State'].get('Pid')) is not int or d['State']['Pid']<=0:raise ConnectionError('remote runtime PID unknown')
            self.owned_id(d,h);self.check_open(h)
            return dict(pid=d['State']['Pid'],container=self.name(h),image=d['Image'],state='running')
    def child_root(self,h,*,allow_closed=False):
        identity=h.get('actor_id','')
        if not isinstance(identity,str) or not re.fullmatch('[a-f0-9]{32}',identity):raise ValueError('invalid child identity')
        if allow_closed:
            path=self.validate(h);target=path/h['job_id']
            closed=[p for p in (path/'closed.json',target/'closed.json') if p.exists()]
            if closed:
                if any(json.loads(p.read_bytes()).get('epoch')!=h['epoch'] for p in closed):
                    raise ValueError('remote retained close identity mismatch')
                intent=target/'intent.json'
                if (not intent.is_file() or json.loads(intent.read_bytes())!={k:h[k] for k in ('namespace','execution_id','epoch','job_id')}
                        or not self.terminal_proof(h,target)):
                    raise ValueError('remote retained result exit unconfirmed')
                return target,identity
        return self.check_open(h),identity
    def update_child(self,h,stream):
        target,identity=self.child_root(h)
        if any(not item['path'].startswith(('input/','work/')) for item in h['files']):raise ValueError('child exchange scope invalid')
        stage=self.root/('upload-'+uuid.uuid4().hex)
        try:
            receive_archive(stream,stage,h['files'])
            with self.control_scope(h,operation_seconds=120),self.wait_lock(target/'.reap.lock'):
                self.child_root(h)
                for part,destination in (('input',target/'data/input/actors'/identity),('work',target/'data/output/actors'/identity/'work')):
                    source=stage/part;destination.mkdir(parents=True,exist_ok=True)
                    for directory in (destination.parent,destination):os.chown(directory,1000,1000)
                    wanted=file_map(source) if source.is_dir() else {}
                    before={item['path']:item['sha256'] for item in manifest(file_map(destination))}
                    incoming={item['path'][len(part)+1:]:item['sha256'] for item in h['files'] if item['path'].startswith(part+'/')}
                    for name,path in file_map(destination).items():
                        if name not in wanted:path.unlink()
                    for name,path in wanted.items():
                        if before.get(name)==incoming.get(name):continue
                        dest=destination/name;dest.parent.mkdir(parents=True,exist_ok=True)
                        directory=dest.parent
                        while directory!=destination.parent:
                            os.chown(directory,1000,1000);directory=directory.parent
                        os.chown(path,1000,1000);path.replace(dest)
                self.child_root(h)
            return dict(updated=True,actor_id=identity)
        finally:
            if stage.exists():shutil.rmtree(stage)
    def collect_child(self,h,stream):
        target,identity=self.child_root(h,allow_closed=True)
        files=file_map(target/'data/output/actors'/identity/'work');items=manifest(files)
        emit(stream,dict(job_id=h['job_id'],actor_id=identity,files=items));write_archive(stream,files);stream.flush()
    def record_exit_error(self,h,exc):
        detail=describe(exc)
        if h.get('op') not in ('reap_job','reap_execution','run'):return detail
        try:
            path=self.validate(h)
            if h['op']!='reap_execution':path=path/h['job_id']
            if path.is_dir():
                save(path/'exit-error.json',dict(at=time.time(),error=detail,**safe_context(dict(
                    execution_id=h['execution_id'],epoch=h['epoch'],job_id=h['job_id'],
                    operation=h['op'],request_id=h.get('request_id')))))
        except Exception:detail['diagnostic_write_failed']=True
        return detail
    def handle(self,h,input_stream,output_stream):
        self.validate(h);op=h['op']
        if 'connection_protocol' in h:
            if h['connection_protocol']!='queued-ssh/1':raise ValueError('unsupported connection protocol')
            # Machine-readable proof that SSH authenticated and reached the fixed
            # gateway. Legacy clients still receive exactly their original reply.
            emit(output_stream,dict(connection_protocol='queued-ssh/1',ready=True))
        if op in ('status_job','reap_job','reap_execution'):
            with self.control_scope(h,role='query' if op=='status_job' else 'exit',output=output_stream):
                return self.dispatch(h,input_stream,output_stream)
        if op in ('prepare','update','collect','update_child','collect_child'):
            protocol=h.get('transfer_protocol')
            if protocol is not None and protocol!='queued-transfer/1':raise ValueError('unsupported transfer protocol')
            # A legacy uploader writes its whole archive before reading stdout.
            # Only acknowledged clients may receive queue progress before input.
            check=(lambda:self.child_root(h,allow_closed=True)) if op=='collect_child' else None
            with self.control_scope(h,operation_seconds=120,output=output_stream if protocol else None,check=check) as budget,self.capacity_slot('.transfer',(0,1)):
                if protocol:emit(output_stream,dict(event='transfer_ready',request_id=h.get('request_id')))
                result=self.dispatch(h,input_stream,output_stream);budget.check();return result
        return self.dispatch(h,input_stream,output_stream)
    def dispatch(self,h,input_stream,output_stream):
        op=h['op']
        if op=='prepare':emit(output_stream,self.prepare(h,input_stream))
        elif op=='update':emit(output_stream,self.update(h,input_stream))
        elif op=='run':self.run(h,input_stream,output_stream)
        elif op=='collect':self.collect(h,output_stream)
        elif op=='update_child':emit(output_stream,self.update_child(h,input_stream))
        elif op=='collect_child':self.collect_child(h,output_stream)
        elif op=='status_job':emit(output_stream,self.status_job(h))
        elif op=='reap_job':emit(output_stream,self.reap_job(h))
        elif op=='reap_execution':emit(output_stream,self.reap_execution(h))
        elif op=='health':emit(output_stream,dict(ready=True,image=self.config['image'],version=self.config['version'],host_memory=host_memory()))
        else:raise ValueError('unsupported remote operation')


def main():
    import signal
    signal.alarm(15)
    signal.signal(signal.SIGHUP,signal.SIG_IGN)
    gateway=None;h={}
    try:
        # Do not prefetch bytes that the attached container must read from fd 0.
        input_stream=os.fdopen(os.dup(0),'rb',buffering=0)
        header=input_stream.readline(2*1024**2+1)
        if not header or len(header)>2*1024**2:raise ValueError('invalid gateway header')
        h=json.loads(header);signal.alarm(0)
        config=json.loads(Path('/opt/paperflow-harness/config.json').read_text())
        gateway=Gateway('/opt/paperflow-harness/jobs',config)
        gateway.handle(h,input_stream,sys.stdout.buffer)
    except BaseException as exc:
        detail=gateway.record_exit_error(h,exc) if gateway and isinstance(h,dict) else describe(exc)
        emit(sys.stdout.buffer,dict(error=type(exc).__name__,detail=detail,request_id=h.get('request_id')))
        return 1
    return 0


if __name__=='__main__':raise SystemExit(main())

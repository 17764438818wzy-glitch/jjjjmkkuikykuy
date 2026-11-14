"""Persistent admission stops, independent of upload capacity and transport."""
import json
import hashlib
import os
import sqlite3
import tempfile
import threading
import time
import uuid
from contextlib import closing
from pathlib import Path


class LinkedStop(threading.Event):
    def __init__(self,root,run_id):
        super().__init__();self.root,self.run_id=root,run_id
        self._intent_lock=threading.Lock()
        self._intent=None

    def registration(self):
        with self._intent_lock:
            return ({key:value for key,value in self._intent.items() if key!='registering'}
                    if self._intent else dict(state='not_bound'))

    @property
    def reason(self):
        return self.registration().get('reason','body_user_stop')

    def clear(self):
        # Clearing memory never resumes a durable upload scope.
        with self._intent_lock:
            super().clear()
            self._intent=None

    def set(self,reason=None):
        super().set()
        run_id=self.run_id()
        if not run_id:return
        with self._intent_lock:
            if self._intent is None or self._intent['run_id']!=run_id:
                self._intent=dict(run_id=run_id,request_id=uuid.uuid4().hex,
                    reason=reason or 'body_user_stop',state='pending',registering=False)
            intent=self._intent
            if intent['state']=='confirmed' or intent['registering']:return
            intent.update(state='in_progress',registering=True)
        try:
            pause_delivery(self.root,run_id=run_id,reason=intent['reason'],
                           request_id=intent['request_id'])
        except Exception as exc:
            # Event.set is a cancellation notification, never a replacement
            # exception for the processing failure already being unwound.
            from processing_stop import record_failure
            exc.request_id=intent['request_id'];exc.stop_reason=intent['reason']
            record=record_failure(self.root,exc,stage='delivery_stop_registration',run_id=run_id)
            with self._intent_lock:
                intent.update(state='failed_pending',error_type=type(exc).__name__,record=record)
        else:
            with self._intent_lock:
                intent.update(state='confirmed')
                intent.pop('error_type',None)
        finally:
            with self._intent_lock:intent['registering']=False


def control_scope(run_id):
    if run_id is not None and (not isinstance(run_id,str) or not run_id):raise ValueError('invalid upload run identity')
    return '*' if run_id is None else 'run:'+run_id


def fence_path(path,scope):
    return Path(path).parent/'control-stops'/(hashlib.sha256(scope.encode()).hexdigest()+'.json')


def intent_lock_path(path,scope):
    return str(fence_path(path,scope))+'.intent.lock'


def recorded_stop(path,request_id):
    if not Path(path).is_file():return None
    with closing(sqlite3.connect(Path(path).as_uri()+'?mode=ro',uri=True,timeout=5)) as db:
        db.row_factory=sqlite3.Row
        if not db.execute("SELECT 1 FROM sqlite_master WHERE name='delivery_stop_commands'").fetchone():return None
        row=db.execute('SELECT * FROM delivery_stop_commands WHERE request_id=?',(request_id,)).fetchone()
        return dict(row) if row else None


def stop_identity_path(path,request_id):
    return Path(path).parent/'stop-identities'/(hashlib.sha256(request_id.encode()).hexdigest()+'.json')


def recorded_control_command(path,request_id,scope,action,expected_generation):
    if not Path(path).is_file():return None
    from preextract_delivery_queue import _control_command
    with closing(sqlite3.connect(Path(path).as_uri()+'?mode=ro',uri=True,timeout=5)) as db:
        db.execute('PRAGMA query_only=ON');db.row_factory=sqlite3.Row
        if not db.execute("SELECT 1 FROM sqlite_master WHERE name='delivery_control_commands'").fetchone():return None
        return _control_command(db,request_id,scope,action,expected_generation)


def write_stop_identity(target,value):
    fd,temporary=tempfile.mkstemp(dir=target.parent,prefix='.stop-',suffix='.tmp')
    try:
        with os.fdopen(fd,'wb') as output:
            output.write(json.dumps(value,ensure_ascii=False).encode('utf-8'));output.flush();os.fsync(output.fileno())
        from atomic_json import replace_generated
        replace_generated(Path(temporary),target)
    finally:
        try:Path(temporary).unlink(missing_ok=True)
        except OSError:pass


def read_fence(path,scope):
    selected=fence_path(path,scope)
    if not selected.exists():return None
    value=json.loads(selected.read_text(encoding='utf-8'))
    if value.get('scope')!=scope or not value.get('fence_id'):raise ValueError('upload stop fence identity changed')
    return value


def blocked_scopes(path):
    scopes=[]
    for selected in (Path(path).parent/'control-stops').glob('*.json'):
        value=json.loads(selected.read_text(encoding='utf-8'))
        scope=value.get('scope')
        if not isinstance(scope,str) or fence_path(path,scope)!=selected or not value.get('fence_id'):
            raise ValueError('upload stop fence identity changed')
        scopes.append(scope)
    return scopes


def pause_delivery(root,*,run_id=None,reason='operator_stop',queue=None,request_id=None,expected_generation=None):
    from filelock import FileLock
    from preextract_delivery_queue import DeliveryQueue,queue_path
    path=queue_path(root);scope=control_scope(run_id)
    request_id=request_id or uuid.uuid4().hex
    if not isinstance(request_id,str) or not request_id or len(request_id)>128:
        raise ValueError('invalid delivery stop request identity')
    if expected_generation is not None and (type(expected_generation) is not int or expected_generation<0):
        raise ValueError('invalid delivery control generation')
    target=fence_path(path,scope);target.parent.mkdir(parents=True,exist_ok=True)
    identities=path.parent/'stop-identities';identities.mkdir(parents=True,exist_ok=True)
    identity_path=stop_identity_path(path,request_id)
    # Resume holds this same scope lock through validation and fence deletion.
    # Publish before waiting for the busy queue admission lock/SQLite writer.
    with FileLock(str(identity_path)+'.lock',timeout=5), FileLock(intent_lock_path(path,scope),timeout=5):
        if expected_generation is not None:
            command=recorded_control_command(path,request_id,scope,'pause',expected_generation)
            if command is not None:return command
        fresh=not identity_path.is_file()
        read_failure=None
        try:known=recorded_stop(path,request_id)
        except (OSError,sqlite3.Error,ValueError,TypeError) as exc:
            # An old identity must not reapply an uncertain command. For a
            # first new identity, persist the hold before exposing this read
            # failure; queue recovery must not silently reopen admission.
            if not fresh:raise
            known=None;read_failure=exc
        if known:
            if known['scope']!=scope:raise ValueError('delivery stop identity belongs to another scope')
            if expected_generation is not None:raise ValueError('delivery stop identity has no matching versioned command')
            return DeliveryControls(root).control(run_id=run_id)
        fence=read_fence(path,scope)
        if identity_path.is_file():
            identity=json.loads(identity_path.read_text(encoding='utf8'))
            if identity.get('request_id')!=request_id or identity.get('scope')!=scope:
                raise ValueError('delivery stop identity belongs to another scope')
            if identity.get('expected_generation')!=expected_generation:
                raise ValueError('delivery stop identity belongs to different generation')
            current=DeliveryControls(root).control(run_id=run_id)
            if ((fence and fence['fence_id']!=identity['fence_id'])
                    or (not fence and current['generation']!=identity['base_generation'])):
                raise ValueError('delivery stop intent superseded; do not reapply an older pending stop')
        else:
            try:base_generation=DeliveryControls(root).control(run_id=run_id)['generation']
            except (OSError,sqlite3.Error,ValueError,TypeError) as exc:
                base_generation=None
                if read_failure is None:read_failure=exc
            if expected_generation is not None and base_generation!=expected_generation:
                raise ValueError('delivery control generation changed; no new stop was applied')
            identity=dict(schema='delivery-stop-identity/1',scope=scope,request_id=request_id,
                          fence_id=uuid.uuid4().hex,
                          base_generation=base_generation,expected_generation=expected_generation)
            write_stop_identity(identity_path,identity)
        if not fence or fence.get('request_id')!=request_id:
            fence=dict(schema='delivery-stop-pending/1',scope=scope,fence_id=identity['fence_id'],
                       request_id=request_id,reason=reason,created_at=time.time())
            write_stop_identity(target,fence)
        if read_failure is not None:raise read_failure
        with FileLock(str(path)+'.admission.lock',timeout=5):
            queue=queue or DeliveryQueue(path)
            result=queue._pause_record(scope,reason,fence['fence_id'],request_id=request_id,expected_generation=expected_generation)
            return result if result is not None else queue.control(run_id=run_id)


class QueueStop:
    def __init__(self,queue,job,upstream):
        # Ordinary worker -> handoff must keep the same frozen authority and
        # original global generation, rather than recapture creator-only state.
        item=job['payload'].get('item') or {}
        upstream_item=upstream.job['payload'].get('item') or {} if isinstance(upstream,QueueStop) else {}
        if (isinstance(upstream,QueueStop) and Path(upstream.queue.path)==Path(queue.path)
                and upstream.job['payload']['run_id']==job['payload']['run_id']
                and item.get('id') and item.get('source_hash')
                and all(item.get(key)==upstream_item.get(key) for key in ('id','source_hash'))
                and (job.get('id') is None or job['id']==upstream.job.get('id'))):
            self.queue,self.job,self.upstream=queue,upstream.job,upstream.upstream
            self.generations,self.authority=upstream.generations,upstream.authority
            return
        self.queue,self.job,self.upstream=queue,job,upstream
        self.generations=job.get('control_generations') or queue.generations(job['payload']['run_id'])
        self.authority=job.get('delivery_authority')
        if self.authority is None:
            from preextract_task_delivery import capture_delivery_authority
            try:
                self.authority=capture_delivery_authority(queue.path,job['payload'],delivery_id=job.get('id'),
                    dispatch_scope=job.get('dispatch_scope'))
                # Direct continuation must still honor explicitly supplied G/Q
                # generations; a fresh task mapping never refreshes old grants.
                expected=job.get('control_generations')
                if expected is not None:
                    if self.authority['global_generation']!=expected.get('*'):
                        self.authority['references']=[]
                    else:
                        self.authority['references']=[reference for reference in self.authority['references']
                            if all(reference['generations'].get(scope)==generation for scope,generation in expected.items())]
            except (OSError,ValueError,TypeError,KeyError,sqlite3.Error):
                self.authority=None

    def readback_is_set(self):
        """Task/run holds stop new writes; global/worker hard stops retain veto."""
        if self.upstream.is_set():return True
        try:
            control=self.queue.control()
            generation=(self.authority['global_generation'] if self.authority is not None else self.generations['*'])
            return control['paused'] or control['generation']!=generation
        except (OSError,ValueError,TypeError,KeyError,sqlite3.Error):return True

    def is_set(self):
        if self.upstream.is_set():return True
        try:
            if self.authority is None:return True
            from preextract_task_delivery import delivery_authority_open
            return not delivery_authority_open(self.queue.path,self.authority)
        except (OSError,ValueError,TypeError,KeyError,sqlite3.Error):return True

    def wait(self,timeout=None):
        deadline=None if timeout is None else time.monotonic()+timeout
        while not self.is_set():
            if deadline is not None and time.monotonic()>=deadline:return False
            self.upstream.wait(.05 if deadline is None else min(.05,max(0,deadline-time.monotonic())))
        return True


class DeliveryControls:
    """Read direct-handoff controls without enrolling it in the disk queue."""
    def __init__(self,root):
        from preextract_delivery_queue import queue_path
        self.root=Path(root);self.path=queue_path(root)

    def control(self,*,run_id=None):
        scope=control_scope(run_id);fence=read_fence(self.path,scope);row=None
        if self.path.is_file():
            with closing(sqlite3.connect(self.path.as_uri()+'?mode=ro',uri=True,timeout=5)) as db:
                db.row_factory=sqlite3.Row
                if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='delivery_controls'").fetchone():
                    row=db.execute('SELECT * FROM delivery_controls WHERE scope=?',(scope,)).fetchone()
        return dict(scope=scope,paused=bool(fence) or bool(row and row['paused']),
            generation=row['generation'] if row else 0,reason=row['reason'] if row else '',
            updated_at=row['updated_at'] if row else None,fence_id=fence['fence_id'] if fence else None,
            stop_pending=bool(fence and (not row or row['fence_id']!=fence['fence_id'] or not row['paused'])))

    def generations(self,run_id):
        from preextract_delivery_queue import DeliveryQueue
        return DeliveryQueue.generations(self,run_id)

    def pause(self,*,run_id=None,reason='operator_stop'):
        return pause_delivery(self.root,run_id=run_id,reason=reason)


def delivery_control_snapshot(root,*,run_id=None):
    """Expose independent stage/upload holds; reading never resumes either."""
    from global_task_control import GlobalTaskControl
    controls=DeliveryControls(root)
    try:
        upload_global=controls.control();upload_run=controls.control(run_id=run_id) if run_id is not None else None
    except (OSError,ValueError,sqlite3.Error) as exc:
        upload_global=dict(scope='*',paused=None,generation=None,reason='control_unavailable',error_type=type(exc).__name__)
        upload_run=None
    stage=GlobalTaskControl(root).snapshot()
    reasons=[]
    if not stage['admission_open']:reasons.append('global_admission_paused')
    for row in (upload_global,upload_run):
        if row and row['paused'] is not False:reasons.append('upload_control_unknown' if row['paused'] is None else 'upload_paused')
    return dict(schema='backend-delivery-control/1',data_root=str(Path(root).resolve()),run_id=run_id,
        global_stage=stage,upload_global=upload_global,upload_run=upload_run,
        admission_open=not reasons,reason_codes=list(dict.fromkeys(reasons)),observed_at=time.time())


def sync_operator_pause(queue):
    path=queue.path.with_name('native-batch.json')
    if not path.is_file():return
    policy=json.loads(path.read_text(encoding='utf-8'))
    if policy.get('operator_upload_paused') is True or policy.get('native_workers')==0:
        if not queue.control()['paused']:queue.pause(reason='operator_upload_paused')

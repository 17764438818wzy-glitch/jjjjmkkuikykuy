"""Durable global admission, independent of a running stage's cancellation token.

The short SQLite transaction serializes stop with claim/launch. Stage records
survive crashes and are retired only after the native result was persisted.
"""
from contextlib import contextmanager, closing
from contextvars import ContextVar
from pathlib import Path
import json
import os
import sqlite3
import tempfile
import threading
import time
import uuid

import psutil

SCHEMA = 'global-task-control/1'
STAGES = {'preparation', 'model', 'postprocess', 'delivery', 'cleanup', 'archive'}
_current_stage=ContextVar('global_admitted_stage',default=None)
_admission_deadline=ContextVar('global_admission_deadline',default=None)
_process_locks={}
_process_locks_guard=threading.Lock()


class GlobalPaused(RuntimeError):
    """No authority to start another stage (completed results remain pending)."""


def _owner(pid, created):
    result = dict(pid=pid, create_time=created, status='unknown')
    try:
        process = psutil.Process(pid)
        result['status'] = ('alive' if abs(process.create_time()-created) < .01
                            and process.is_running() and process.status() != psutil.STATUS_ZOMBIE else 'dead')
    except psutil.NoSuchProcess:
        result['status'] = 'dead'
    except (psutil.Error, OSError, ValueError):
        pass
    return result


class _Admission:
    def __init__(self, connection):
        self.connection = connection

    def begin(self, stage, *, batch_id='', paper_id='', name='', token=None):
        if stage not in STAGES:
            raise ValueError('invalid global stage')
        if self.connection is None:
            return None
        token = token or uuid.uuid4().hex
        self.connection.execute('INSERT INTO stages(token,stage,batch_id,paper_id,name,started_at,pid,create_time) '
            'VALUES(?,?,?,?,?,?,?,?)', (token, stage, str(batch_id), str(paper_id), str(name), time.time(),
                                      os.getpid(), psutil.Process().create_time()))
        return token


class GlobalTaskControl:
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.path = self.root/'private/global-task-control.sqlite3'
        self.marker = self.path.with_suffix('.initialized')
        self.fence = self.path.with_suffix('.stop.json')
        self.lock_timeout=30

    @contextmanager
    def _admission_lock(self):
        from filelock import FileLock,Timeout
        self.path.parent.mkdir(parents=True,exist_ok=True)
        started=time.monotonic();deadline=started+self.lock_timeout
        marker=_admission_deadline.set(deadline)
        key=os.path.normcase(str(self.path))
        with _process_locks_guard:
            local=_process_locks.setdefault(key,threading.RLock())
        acquired=False;location='process_gate'
        try:
            acquired=local.acquire(timeout=max(0,deadline-time.monotonic()))
            if not acquired:raise Timeout(str(self.path)+'.admission.lock')
            location='file_lock'
            with FileLock(str(self.path)+'.admission.lock',
                          timeout=max(0,deadline-time.monotonic())):
                location='sql_or_admitted_operation'
                yield
        except Timeout as exc:
            if str(exc.lock_file)==str(self.path)+'.admission.lock':
                exc.lock_kind='global_stage_admission'
                exc.lock_wait_seconds=round(time.monotonic()-started,6)
                exc.lock_location=location
            raise
        finally:
            if acquired:local.release()
            _admission_deadline.reset(marker)

    def _read_fence(self):
        if not self.fence.exists():return None
        value=json.loads(self.fence.read_text(encoding='utf8'))
        if (not isinstance(value,dict) or value.get('schema')!='global-stage-stop/1'
                or not value.get('fence_id') or not value.get('request_id')):
            raise ValueError('global stop fence identity invalid')
        return value

    def _write_fence(self,request_id):
        previous=self._read_fence()
        if previous and previous['request_id']==request_id:return previous
        fence=dict(schema='global-stage-stop/1',request_id=request_id,fence_id=uuid.uuid4().hex,created_at=time.time())
        fd,temporary=tempfile.mkstemp(dir=self.path.parent,prefix='.global-stop-',suffix='.tmp')
        try:
            with os.fdopen(fd,'wb') as stream:
                stream.write(json.dumps(fence).encode('utf8'));stream.flush();os.fsync(stream.fileno())
            os.replace(temporary,self.fence)
        finally:Path(temporary).unlink(missing_ok=True)
        return fence

    def _connect(self):
        deadline=_admission_deadline.get()
        timeout=30 if deadline is None else max(0,deadline-time.monotonic())
        connection = sqlite3.connect(self.path, timeout=timeout, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute('PRAGMA synchronous=FULL')
        return connection

    @contextmanager
    def _transaction(self):
        with closing(self._connect()) as connection:
            self._begin_write(connection)
            try:
                yield connection
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def _begin_write(self,connection):
        started=time.monotonic()
        try:connection.execute('BEGIN IMMEDIATE')
        except sqlite3.OperationalError as exc:
            if getattr(exc,'sqlite_errorcode',0)&255 in {sqlite3.SQLITE_BUSY,sqlite3.SQLITE_LOCKED}:
                exc.lock_kind='global_stage_sql'
                exc.lock_file=str(self.path)
                exc.lock_wait_seconds=round(time.monotonic()-started,6)
                exc.lock_location='begin_immediate'
            raise

    def initialize_paused(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        from filelock import FileLock
        with FileLock(str(self.path)+'.initialize.lock'):
            if self.marker.exists() and not self.path.exists():
                raise ValueError('initialized global control database is missing; reconciliation required')
            if not self.marker.exists():
                with self.marker.open('x',encoding='utf8') as stream:
                    stream.write(SCHEMA);stream.flush();os.fsync(stream.fileno())
            self._initialize_database()
        return self.snapshot()

    def _initialize_database(self):
        with self._transaction() as connection:
            connection.execute('CREATE TABLE IF NOT EXISTS control(id INTEGER PRIMARY KEY CHECK(id=1), '
                'generation INTEGER NOT NULL, intent TEXT NOT NULL)')
            connection.execute("INSERT OR IGNORE INTO control(id,generation,intent) VALUES(1,0,'paused')")
            connection.execute('CREATE TABLE IF NOT EXISTS commands(request_id TEXT PRIMARY KEY, '
                'operation TEXT NOT NULL,generation INTEGER NOT NULL)')
            connection.execute('CREATE TABLE IF NOT EXISTS stages(token TEXT PRIMARY KEY, stage TEXT NOT NULL, '
                'batch_id TEXT,paper_id TEXT,name TEXT,started_at REAL,pid INTEGER,create_time REAL, '
                'finished_at REAL,outcome TEXT,error TEXT,drain_generation INTEGER)')

    def _state_rows(self,connection):
        row=connection.execute('SELECT * FROM control WHERE id=1').fetchone()
        if row is None or row['intent'] not in {'running','paused'}:
            raise ValueError('global control state missing or invalid')
        stages=[dict(record) for record in connection.execute(
            'SELECT * FROM stages WHERE finished_at IS NULL ORDER BY started_at')]
        drains=[dict(record) for record in connection.execute(
            'SELECT stage,COUNT(*) AS total,COUNT(finished_at) AS finished '
            'FROM stages WHERE drain_generation=? GROUP BY stage',(row['generation'],))]
        return dict(row=dict(row),stages=stages,drains=drains)

    def _snapshot(self, connection=None, *, data=None):
        base = dict(schema=SCHEMA, state='running', generation=0, admission_open=True,
                    observed_at=time.time(), active=[], unknown=[],
                    drain=dict(total=0, finished=0, remaining=0, by_stage={}))
        if connection is None and data is None:
            return base
        data=data if data is not None else self._state_rows(connection)
        row=data['row'];owners={}
        base['generation'] = row['generation']
        for record in data['stages']:
            item = dict(record)
            identity=(item.pop('pid'),item.pop('create_time'))
            if identity not in owners:owners[identity]=_owner(*identity)
            item['owner'] = dict(owners[identity])
            (base['active'] if item['owner']['status']=='alive' else base['unknown']).append(item)
        for record in data['drains']:
            counts = dict(total=record['total'], finished=record['finished'], remaining=record['total']-record['finished'])
            base['drain']['by_stage'][record['stage']] = counts
            for key in ('total', 'finished', 'remaining'):
                base['drain'][key] += counts[key]
        base['state'] = ('unknown' if base['unknown'] else 'running' if row['intent']=='running'
                         else 'draining' if base['active'] else 'paused')
        fence=self._read_fence()
        if fence and (row['intent']!='paused' or 'fence_id' not in row.keys() or row['fence_id']!=fence['fence_id']):
            base['unknown'].append(dict(reason='stop_pending',request_id=fence['request_id']))
            base['state']='unknown'
        base['admission_open'] = base['state']=='running'
        return base

    def snapshot(self):
        if not self.path.exists():
            if self.marker.exists() or self.fence.exists():
                state=self._snapshot()
                state.update(state='unknown',admission_open=False,unknown=[dict(reason='control_database_missing')])
                return state
            return self._snapshot()
        try:
            with closing(self._connect()) as connection:
                connection.execute('BEGIN')
                data=self._state_rows(connection)
            # Process inspection is observation work. It must not retain a
            # SQLite read lock while writers are trying to admit/finish stages.
            return self._snapshot(data=data)
        except (OSError, sqlite3.Error, ValueError, TypeError) as exc:
            state = self._snapshot()
            state.update(state='unknown', admission_open=False,
                         unknown=[dict(reason='control_unavailable', error_type=type(exc).__name__)])
            return state

    def _command(self, operation, request_id, expected_generation=None):
        if not isinstance(request_id, str) or not request_id.strip():
            raise ValueError('request_id required')
        if operation=='resume' and (type(expected_generation) is not int or expected_generation<0):
            raise ValueError('expected_generation required for resume')
        with self._admission_lock():
            # Known command IDs are read before changing the fence: replaying an
            # old stop must not stop a later explicitly resumed generation.
            read_error=None;previous=None
            if self.path.exists():
                try:
                    with closing(self._connect()) as connection:
                        previous=connection.execute('SELECT * FROM commands WHERE request_id=?',(request_id,)).fetchone()
                except (OSError,sqlite3.Error) as exc:read_error=exc
            if previous:
                if previous['operation']!=operation:raise ValueError('request_id already used for another operation')
                return dict(self.snapshot(),duplicate=True)
            fence=self._write_fence(request_id) if operation=='stop' else None
            if read_error is not None:raise read_error
            if not self.path.exists():self.initialize_paused()
            with self._transaction() as connection:
                state=self._snapshot(connection)
                if operation=='resume':
                    if expected_generation!=state['generation']:raise GlobalPaused('global control generation changed')
                    if state['state'] not in {'paused','running'}:
                        raise GlobalPaused('active or unknown stages must settle before resume')
                    # A healthy snapshot proved that any fence is this SQL
                    # generation's confirmed stop. Remove it under the same
                    # admission lock; SQL rollback still leaves intent paused.
                    confirmed=self._read_fence()
                    if confirmed:self.fence.unlink()
                    old=connection.execute('SELECT intent FROM control WHERE id=1').fetchone()[0]
                    generation=state['generation']+(old!='running')
                    connection.execute("UPDATE control SET intent='running',generation=? WHERE id=1",(generation,))
                else:
                    columns={row[1] for row in connection.execute('PRAGMA table_info(control)')}
                    if 'fence_id' not in columns:connection.execute('ALTER TABLE control ADD COLUMN fence_id TEXT')
                    generation=state['generation']+1
                    connection.execute("UPDATE control SET intent='paused',generation=?,fence_id=? WHERE id=1",
                                       (generation,fence['fence_id']))
                    # Each new stop identity is a new boundary even if a prior
                    # stop had already closed admission.
                    connection.execute('UPDATE stages SET drain_generation=? WHERE finished_at IS NULL', (generation,))
                connection.execute('INSERT INTO commands VALUES(?,?,?)', (request_id, operation, generation))
                return self._snapshot(connection)

    def request_stop(self, request_id):
        return self._command('stop', request_id)

    def resume(self, request_id, expected_generation=None):
        return self._command('resume', request_id, expected_generation)

    @contextmanager
    def admission(self):
        if not self.path.exists():
            if self.marker.exists() or self.fence.exists():raise GlobalPaused('global control database missing; reconciliation required')
            yield _Admission(None)
            return
        with self._admission_lock():
            with closing(self._connect()) as connection:
                self._begin_write(connection)
                try:
                    self._assert_admission(connection)
                    yield _Admission(connection)
                finally:
                    # A launcher can throw after creating a child. Preserve its
                    # ownership until native reap confirms exit and durable result.
                    connection.commit()

    def _assert_admission(self,connection):
        # Keep the authority check in the same writer transaction as begin /
        # launch, without building display items or the historical drain view.
        row=connection.execute('SELECT * FROM control WHERE id=1').fetchone()
        if row is None or row['intent'] not in {'running','paused'}:
            raise ValueError('global control state missing or invalid')
        fence=self._read_fence()
        if row['intent']!='running' or fence:
            raise GlobalPaused('全局已停止准入；本阶段结果保留待继续')
        for owner in connection.execute(
                'SELECT DISTINCT pid,create_time FROM stages WHERE finished_at IS NULL'):
            if _owner(owner['pid'],owner['create_time'])['status']!='alive':
                raise GlobalPaused('阶段持有者未知；保留结果等待核对')

    def begin(self, stage, **identity):
        with self.admission() as admission:
            return admission.begin(stage, **identity)

    def finish(self, token, outcome='complete', error=''):
        if token is None:
            return
        with self._transaction() as connection:
            row = connection.execute('SELECT pid,create_time FROM stages WHERE token=?', (token,)).fetchone()
            if not row or row['pid'] != os.getpid() or abs(row['create_time']-psutil.Process().create_time()) >= .01:
                raise ValueError('global stage owner changed')
            connection.execute('UPDATE stages SET finished_at=?,outcome=?,error=? WHERE token=? AND finished_at IS NULL',
                               (time.time(), outcome, error, token))

    def inherited(self, stage):
        """Prepared children finish the scheduler's already admitted stage."""
        current=_current_stage.get()
        if current and current[:2]==(str(self.root),stage):return True
        token=os.environ.get('PAPERFLOW_GLOBAL_STAGE_TOKEN')
        if not token or not self.path.exists():return False
        # A child can start before its parent's claim/launch transaction commits.
        # Wait for that transaction so an admitted stage cannot look unowned.
        with self._transaction() as connection:
            row=connection.execute('SELECT * FROM stages WHERE token=? AND stage=? AND finished_at IS NULL',
                                   (token,stage)).fetchone()
        if not row or _owner(row['pid'],row['create_time'])['status']!='alive':return False
        try:return row['pid'] in {p.pid for p in psutil.Process().parents()}
        except psutil.Error:return False

    @contextmanager
    def using(self, token, stage):
        marker=_current_stage.set((str(self.root),stage,token))
        try:yield
        finally:_current_stage.reset(marker)

    @contextmanager
    def stage(self, stage, **identity):
        if self.inherited(stage):
            yield None
            return
        token = self.begin(stage, **identity)
        try:
            with self.using(token,stage):yield token
        except BaseException as exc:
            try:self.finish(token, 'failed', type(exc).__name__)
            except Exception as secondary:
                from processing_stop import record_failure
                record_failure(self.root,secondary,stage='global_stage_finish')
            raise
        else:
            self.finish(token)


class StageSequence:
    """One paper advances only after the previous durable phase was finished."""
    def __init__(self, control, **identity):
        self.control, self.identity = control, identity
        self.token = self.current = None

    def start(self, stage):
        if self.current == stage:
            return
        self.finish()
        self.token = self.control.begin(stage, **self.identity) if self.control is not None else None
        self.current = stage

    def finish(self, outcome='complete', error=''):
        if self.control is not None:
            self.control.finish(self.token, outcome, error)
        self.token = self.current = None

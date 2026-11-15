"""Local durable ingest queue; network calls never run inside transactions."""
from __future__ import annotations

import sqlite3
import math
import json
import time
import uuid
from contextlib import closing
from pathlib import Path

from ingest_contracts import ArtifactSpec, task_identity
from ingest_execution import is_database_status_diagnostic
from upload_tracking import service_key


class VersionConflict(ValueError):
    """One logical artifact was submitted with incompatible content or routing."""


_UPDATABLE = {'remote_id', 'failure_kind', 'outcome', 'error',
              'lease_until', 'owner', 'remote_run', 'parse_started_at',
              'post_status', 'execution_begin_at', 'post_retry_at', 'post_retry_count'}
_UPDATABLE.update({'source_metadata_status', 'source_metadata_retry_at'})
_REVIEWABLE = {'quality_hold', 'quality_reject', 'technical_review',
               'outcome_unresolved'}
_RECEIPT_FIELDS = {'remote_id','remote_run','post_status','failure_kind',
    'execution_previous_pipeline_id','execution_pipeline_id','execution_channel_key',
    'execution_model_id','execution_provider','execution_instance','execution_contract_version'}
_RECEIPT_STATES = {'uploaded','parse_waiting','submitted','running','upload_unknown',
    'parse_unknown','technical_review','accepted','quality_hold','quality_reject','outcome_unresolved'}


def _as_dict(row: sqlite3.Row | None) -> dict | None:
    if row is None:
        return None
    value = dict(row)
    value['parse_required'] = bool(value['parse_required'])
    value['requested_pipeline_id'] = value['pipeline_id']
    value['requested_channel_key'] = value['channel_key']
    if value.get('execution_pipeline_id'):
        value['pipeline_id'] = value['execution_pipeline_id']
        value['channel_key'] = value['execution_channel_key'] or value['channel_key']
    return value


class QueueStore:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as db:
            db.execute('PRAGMA journal_mode=WAL')
            db.executescript('''
                CREATE TABLE IF NOT EXISTS tasks (
                    id INTEGER PRIMARY KEY,
                    service_key TEXT NOT NULL,
                    dataset_id TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    role TEXT NOT NULL,
                    lane TEXT NOT NULL,
                    content_sha256 TEXT NOT NULL,
                    path TEXT NOT NULL,
                    remote_name TEXT NOT NULL,
                    pipeline_id TEXT,
                    parse_required INTEGER NOT NULL,
                    priority INTEGER NOT NULL,
                    batch_id TEXT NOT NULL,
                    policy_key TEXT NOT NULL,
                    channel_key TEXT NOT NULL,
                    status TEXT NOT NULL,
                    remote_id TEXT,
                    owner TEXT,
                    lease_until REAL,
                    failure_kind TEXT,
                    outcome TEXT,
                    error TEXT,
                    remote_run TEXT,
                    parse_started_at REAL,
                    post_status TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    UNIQUE(service_key, dataset_id, source_id, role)
                );
                CREATE INDEX IF NOT EXISTS tasks_ready ON tasks(status, priority, created_at, id);
                CREATE INDEX IF NOT EXISTS tasks_batch ON tasks(batch_id, id);
                CREATE TABLE IF NOT EXISTS attempts (
                    id INTEGER PRIMARY KEY,
                    task_id INTEGER NOT NULL REFERENCES tasks(id),
                    operation TEXT NOT NULL,
                    result TEXT NOT NULL,
                    at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS claim_cycle (
                    stage TEXT PRIMARY KEY,
                    counter INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS parse_runs (
                    task_id INTEGER NOT NULL REFERENCES tasks(id),
                    attempt_no INTEGER NOT NULL,
                    channel_key TEXT NOT NULL,
                    pipeline_id TEXT NOT NULL,
                    model_id TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    instance TEXT NOT NULL,
                    contract_version TEXT NOT NULL,
                    started_at REAL NOT NULL,
                    finished_at REAL,
                    outcome TEXT,
                    failure_kind TEXT,
                    input_tokens INTEGER,
                    output_tokens INTEGER,
                    cost_cny REAL,
                    cost_basis TEXT,
                    PRIMARY KEY(task_id,attempt_no)
                );
                CREATE INDEX IF NOT EXISTS parse_runs_channel ON parse_runs(channel_key,started_at);
                CREATE TABLE IF NOT EXISTS route_states (
                    channel_key TEXT PRIMARY KEY,
                    state_json TEXT NOT NULL
                );
            ''')
            columns = {row['name'] for row in db.execute('PRAGMA table_info(tasks)')}
            if 'post_status' not in columns:
                db.execute('ALTER TABLE tasks ADD COLUMN post_status TEXT')
            if 'post_retry_at' not in columns:
                db.execute('ALTER TABLE tasks ADD COLUMN post_retry_at REAL')
            if 'post_retry_count' not in columns:
                db.execute('ALTER TABLE tasks ADD COLUMN post_retry_count INTEGER NOT NULL DEFAULT 0')
            for name in ('handoff_id','package_path','package_sha256'):
                if name not in columns:
                    db.execute(f'ALTER TABLE tasks ADD COLUMN {name} TEXT')
            for name in ('execution_pipeline_id','execution_channel_key','execution_model_id',
                         'execution_provider','execution_instance','execution_contract_version',
                         'preferred_channel_key','execution_previous_pipeline_id',
                         'parse_baseline','execution_begin_at'):
                if name not in columns:
                    db.execute(f'ALTER TABLE tasks ADD COLUMN {name} TEXT')
            if 'execution_attempt' not in columns:
                db.execute('ALTER TABLE tasks ADD COLUMN execution_attempt INTEGER NOT NULL DEFAULT 0')
            for name in ('source_metadata_payload', 'source_metadata_status',
                         'source_metadata_request_id', 'source_metadata_verification',
                         'source_metadata_receipt', 'source_metadata_reason'):
                if name not in columns:
                    db.execute(f'ALTER TABLE tasks ADD COLUMN {name} TEXT')
            for name in ('source_metadata_retry_at', 'source_metadata_lease_until'):
                if name not in columns:
                    db.execute(f'ALTER TABLE tasks ADD COLUMN {name} REAL')
            if 'source_metadata_attempts' not in columns:
                db.execute('ALTER TABLE tasks ADD COLUMN source_metadata_attempts INTEGER NOT NULL DEFAULT 0')
            db.commit()

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.path, timeout=30)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA busy_timeout=30000')
        db.execute('PRAGMA foreign_keys=ON')
        return db

    def enqueue(self, spec: ArtifactSpec,initial_state: dict | None = None) -> int:
        from ingest_source_metadata import source_payload
        payload = source_payload(spec)
        service, dataset, source, role = task_identity(spec)
        now = time.time()
        if initial_state and (set(initial_state)-(_RECEIPT_FIELDS | {'status'})
                or initial_state.get('status') not in _RECEIPT_STATES):
            raise ValueError('Invalid imported receipt state')
        with closing(self._connect()) as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('''SELECT * FROM tasks WHERE service_key=? AND dataset_id=?
                                AND source_id=? AND role=?''',
                             (service, dataset, source, role)).fetchone()
            if row is None:
                cursor = db.execute('''INSERT INTO tasks (
                    service_key,dataset_id,source_id,role,lane,content_sha256,path,
                    remote_name,pipeline_id,parse_required,priority,batch_id,policy_key,
                    channel_key,status,created_at,updated_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                    (service, dataset, source, role, spec.lane,
                     spec.content_sha256.lower(), str(spec.path), spec.remote_name,
                     spec.pipeline_id, int(spec.parse_required), spec.priority,
                     spec.batch_id, spec.policy_key, spec.channel_key,
                     'upload_waiting', now, now))
                task_id = cursor.lastrowid
                db.execute('UPDATE tasks SET handoff_id=?,package_path=?,package_sha256=? WHERE id=?',
                           (spec.handoff_id,str(spec.package_path) if spec.package_path else None,
                            spec.package_sha256,task_id))
                if initial_state:
                    clause=','.join(f'{key}=?' for key in initial_state)
                    db.execute(f'UPDATE tasks SET {clause} WHERE id=?',
                               (*initial_state.values(),task_id))
            else:
                if (row['content_sha256'] != spec.content_sha256.lower()
                        or row['lane'] != spec.lane
                        or row['remote_name'] != spec.remote_name
                        or row['policy_key'] != spec.policy_key
                        or row['handoff_id'] != spec.handoff_id
                        or row['package_sha256'] != spec.package_sha256
                        or row['channel_key'] != spec.channel_key
                        or (row['pipeline_id'] and spec.pipeline_id
                            and row['pipeline_id'] != spec.pipeline_id)):
                    raise VersionConflict('Artifact content or route conflicts with existing task')
                task_id = row['id']
                if initial_state:
                    remote_id = initial_state.get('remote_id')
                    if row['remote_id'] not in (None, remote_id):
                        raise VersionConflict('Existing remote ID differs')
                    if row['status'] in ('upload_waiting','upload_unknown'):
                        self._apply_imported_receipt(db,row,initial_state['status'],
                            {key:value for key,value in initial_state.items() if key!='status'},now)
                        row = db.execute('SELECT * FROM tasks WHERE id=?',(task_id,)).fetchone()
                    elif row['remote_id'] is None:
                        raise VersionConflict('Existing task is not safely adoptable')
                parse_required = bool(row['parse_required'] or spec.parse_required)
                status = row['status']
                if status == 'uploaded' and parse_required:
                    status = 'parse_waiting'
                priority = max(row['priority'], spec.priority) if status == 'upload_waiting' else row['priority']
                pipeline_id = row['pipeline_id'] or spec.pipeline_id
                if (int(parse_required), pipeline_id, priority, status) != (
                        row['parse_required'], row['pipeline_id'], row['priority'], row['status']):
                    db.execute('''UPDATE tasks SET parse_required=?,pipeline_id=?,priority=?,
                                  status=?,updated_at=? WHERE id=?''',
                               (int(parse_required), pipeline_id, priority, status, now, task_id))
            if payload is not None:
                existing_payload = db.execute('SELECT source_metadata_payload FROM tasks WHERE id=?',
                                              (task_id,)).fetchone()[0]
                if existing_payload not in (None, payload):
                    raise VersionConflict('Source metadata conflicts with existing task')
                if existing_payload is None:
                    db.execute("UPDATE tasks SET source_metadata_payload=?,source_metadata_status='pending',updated_at=? WHERE id=?",
                               (payload, now, task_id))
            db.commit()
            return task_id

    def get(self, task_id: int) -> dict | None:
        with closing(self._connect()) as db:
            return _as_dict(db.execute('SELECT * FROM tasks WHERE id=?', (task_id,)).fetchone())

    def get_many(self, task_ids) -> dict[int,dict]:
        ids=list(dict.fromkeys(task_ids))
        result={}
        with closing(self._connect()) as db:
            for offset in range(0,len(ids),500):
                group=ids[offset:offset+500]
                rows=db.execute('SELECT * FROM tasks WHERE id IN ('+
                                ','.join('?' for _ in group)+')',group).fetchall()
                result.update((row['id'],_as_dict(row)) for row in rows)
        return result

    def find_identity(self, service_url: str, dataset_id: str, source_id: str,
                      role: str) -> dict | None:
        with closing(self._connect()) as db:
            row = db.execute('''SELECT * FROM tasks WHERE service_key=? AND dataset_id=?
                                AND source_id=? AND role=?''',
                             (service_key(service_url), dataset_id, source_id, role)).fetchone()
            return _as_dict(row)

    def claim(self, stage: str, owner: str, limit: int, now: float,
              lease_seconds: int, exclude_ids: set[int] | None = None,
              exclude_routes: dict[str,set[str]] | None = None) -> list[dict]:
        if stage not in {'upload', 'parse', 'image'}:
            raise ValueError('Invalid claim stage')
        if not owner or limit < 1 or lease_seconds < 1:
            raise ValueError('Invalid claim parameters')
        waiting = 'parse_waiting' if stage == 'parse' else 'upload_waiting'
        intent = 'parse_intent' if stage == 'parse' else 'upload_intent'
        lane = 'image' if stage == 'image' else ('paper' if stage == 'upload' else None)
        excluded = set(exclude_ids or ())
        claimed = []
        with closing(self._connect()) as db:
            db.execute('BEGIN IMMEDIATE')
            if excluded:
                db.execute('CREATE TEMP TABLE excluded_claims(id INTEGER PRIMARY KEY)')
                db.executemany('INSERT INTO excluded_claims(id) VALUES (?)',
                               ((value,) for value in excluded))
            routes=exclude_routes or {}
            if set(routes)-{'service_key','dataset_id','pipeline_id','channel_key'}:
                raise ValueError('Unsupported excluded route')
            cycle = db.execute('SELECT counter FROM claim_cycle WHERE stage=?', (stage,)).fetchone()
            counter = cycle['counter'] if cycle else 0
            for _ in range(limit):
                where = 'status=?' + (' AND lane=?' if lane else '')
                if stage == 'parse':
                    where += " AND (source_metadata_status IS NULL OR source_metadata_status='confirmed')"
                args = (waiting, lane) if lane else (waiting,)
                if stage in ('upload','image'):
                    where += ''' AND (policy_key NOT IN ('research_profile','research_material') OR role='paper_primary' OR EXISTS (
                        SELECT 1 FROM tasks AS parent
                        WHERE parent.service_key=tasks.service_key
                          AND parent.source_id=tasks.source_id
                          AND parent.policy_key=tasks.policy_key
                          AND parent.channel_key=tasks.channel_key
                          AND parent.role='paper_primary'
                          AND ((parent.status='accepted' AND parent.post_status='done') OR
                               (parent.parse_required=0 AND parent.status='uploaded'))))'''
                if stage == 'image':
                    where += ''' AND (policy_key!='education' OR EXISTS (
                        SELECT 1 FROM tasks AS parent
                        WHERE parent.service_key=tasks.service_key
                          AND parent.source_id=tasks.source_id
                          AND parent.role='paper_primary'
                          AND (parent.status='accepted' OR
                               (parent.parse_required=0 AND parent.status='uploaded'
                                AND (parent.source_metadata_status IS NULL OR parent.source_metadata_status='confirmed')))))'''
                if excluded:
                    where += ' AND id NOT IN (SELECT id FROM excluded_claims)'
                for field,values in routes.items():
                    if values:
                        where+=' AND ('+field+' IS NULL OR '+field+' NOT IN ('+','.join('?' for _ in values)+'))'
                        args+=tuple(values)
                manual = db.execute(f'''SELECT * FROM tasks WHERE {where} AND priority>0
                    ORDER BY created_at,id LIMIT 1''', args).fetchone()
                automatic = db.execute(f'''SELECT * FROM tasks WHERE {where} AND priority=0
                    ORDER BY created_at,id LIMIT 1''', args).fetchone()
                row = (automatic if counter % 4 == 3 else manual) if manual and automatic else (manual or automatic)
                if row is None:
                    break
                counter += 1
                db.execute('''UPDATE tasks SET status=?,owner=?,lease_until=?,updated_at=?
                              WHERE id=?''',
                           (intent, owner, now + lease_seconds, now, row['id']))
                if stage == 'parse':
                    db.execute('''UPDATE tasks SET execution_attempt=execution_attempt+1,
                        parse_started_at=?,parse_baseline=NULL,execution_begin_at=NULL,
                        remote_run=NULL,outcome=NULL,error=NULL,failure_kind=NULL,post_status=NULL,
                        post_retry_at=NULL,post_retry_count=0
                        WHERE id=?''', (now, row['id']))
                claimed.append(_as_dict(db.execute('SELECT * FROM tasks WHERE id=?',
                                                   (row['id'],)).fetchone()))
            db.execute('''INSERT INTO claim_cycle(stage,counter) VALUES (?,?)
                          ON CONFLICT(stage) DO UPDATE SET counter=excluded.counter''',
                       (stage, counter))
            db.commit()
        return claimed

    def transition(self, task_id: int, expected: str, target: str,
                   expected_updated_at: float | None = None, **fields) -> bool:
        if set(fields) - _UPDATABLE:
            raise ValueError('Unsupported task state field')
        if ('source_metadata_status' in fields
                and fields['source_metadata_status'] not in ('pending','blocked')):
            raise ValueError('Metadata confirmation requires a bound receiver receipt')
        if not target or not expected:
            raise ValueError('Task states are required')
        if fields.get('source_metadata_status') == 'pending':
            current = self.get(task_id)
            if current and current.get('source_metadata_status') == 'blocked':
                if (expected != target or current['status'] != expected
                        or set(fields)-{'source_metadata_status','source_metadata_retry_at'}):
                    raise ValueError('Explicit metadata retry cannot change upload or parse state')
                if expected_updated_at is not None and current['updated_at'] != expected_updated_at:
                    return False
                return self.request_source_metadata_retry(task_id,current['updated_at'],
                    current['remote_id'],reason='operator_transition')
        data = {'status': target, 'updated_at': time.time(), **fields}
        clause = ','.join(f'{key}=?' for key in data)
        with closing(self._connect()) as db:
            db.execute('BEGIN IMMEDIATE')
            guard=' AND updated_at=?' if expected_updated_at is not None else ''
            args=(*data.values(),task_id,expected)
            if expected_updated_at is not None:args+= (expected_updated_at,)
            cursor = db.execute(f'UPDATE tasks SET {clause} WHERE id=? AND status=?'+guard,args)
            if cursor.rowcount and target in (_REVIEWABLE | {'accepted'}):
                db.execute('''UPDATE parse_runs SET finished_at=?,outcome=?,failure_kind=?
                    WHERE task_id=? AND finished_at IS NULL''',
                    (time.time(),target,fields.get('failure_kind'),task_id))
            db.commit()
            return cursor.rowcount == 1

    def claim_routed_parse(self, owner: str, now: float, lease_seconds: int,
                           route: dict, total_limit: int, channel_limit: int,
                           external_counts: dict) -> dict | None:
        from ingest_model_routes import _no_credentials
        _no_credentials(route)
        required=('service_key','dataset_id','source_pipeline_id','pipeline_id',
                  'channel_key','model_id','provider','instance','contract_version')
        if (not owner or lease_seconds < 1 or total_limit < 1 or channel_limit < 1
                or any(not isinstance(route.get(key),str) or not route[key] for key in required)):
            raise ValueError('Invalid routed parse claim')
        if any(type(value) is not int or value < 0 for value in external_counts.values()):
            raise ValueError('External in-flight counts must be nonnegative integers')
        active="status IN ('parse_intent','submitted','running','parse_unknown')"
        with closing(self._connect()) as db:
            db.execute('BEGIN IMMEDIATE')
            total=db.execute(f'SELECT COUNT(*) FROM tasks WHERE service_key=? AND {active}',
                             (route['service_key'],)).fetchone()[0]
            channel=db.execute(f'''SELECT COUNT(*) FROM tasks WHERE service_key=? AND {active}
                AND (execution_channel_key=? OR COALESCE(execution_pipeline_id,pipeline_id)=?)''',
                (route['service_key'],route['channel_key'],route['pipeline_id'])).fetchone()[0]
            if (total+external_counts.get('total',0)>=total_limit
                    or channel+external_counts.get(route['channel_key'],0)>=channel_limit):
                return None
            where='''status='parse_waiting' AND role='paper_primary' AND parse_required=1
                AND remote_id IS NOT NULL AND policy_key='education' AND service_key=?
                AND dataset_id=? AND pipeline_id=?
                AND (source_metadata_status IS NULL OR source_metadata_status='confirmed')
                AND (preferred_channel_key IS NULL OR preferred_channel_key=?)'''
            args=(route['service_key'],route['dataset_id'],route['source_pipeline_id'],route['channel_key'])
            manual=db.execute(f'SELECT * FROM tasks WHERE {where} AND priority>0 ORDER BY created_at,id LIMIT 1',args).fetchone()
            automatic=db.execute(f'SELECT * FROM tasks WHERE {where} AND priority=0 ORDER BY created_at,id LIMIT 1',args).fetchone()
            cycle=db.execute("SELECT counter FROM claim_cycle WHERE stage='parse'").fetchone()
            counter=cycle['counter'] if cycle else 0
            row=(automatic if counter%4==3 else manual) if manual and automatic else (manual or automatic)
            if row is None:
                return None
            attempt=row['execution_attempt']+1
            budget=route.get('daily_cny_limit')
            reserve=route.get('reservation_cny')
            if budget is not None:
                if (type(budget) not in (int,float) or not math.isfinite(budget) or budget<0
                        or type(reserve) not in (int,float) or not math.isfinite(reserve) or reserve<=0):
                    return None
                day_start=((now+28800)//86400)*86400-28800
                used=db.execute('''SELECT COALESCE(SUM(cost_cny),0) FROM parse_runs
                    WHERE channel_key=? AND started_at>=?''',(route['channel_key'],day_start)).fetchone()[0]
                if used+reserve>budget:
                    return None
            db.execute('''UPDATE tasks SET status='parse_intent',owner=?,lease_until=?,updated_at=?,
                execution_pipeline_id=?,execution_channel_key=?,execution_model_id=?,execution_provider=?,
                execution_instance=?,execution_contract_version=?,execution_attempt=?,parse_started_at=?,
                remote_run=NULL,outcome=NULL,failure_kind=NULL,error=NULL,post_status=NULL,
                post_retry_at=NULL,post_retry_count=0,
                parse_baseline=NULL,execution_begin_at=NULL,
                execution_previous_pipeline_id=? WHERE id=?''',
                (owner,now+lease_seconds,now,route['pipeline_id'],route['channel_key'],route['model_id'],
                 route['provider'],route['instance'],route['contract_version'],attempt,now,
                 row['execution_pipeline_id'] or row['execution_previous_pipeline_id'] or row['pipeline_id'],row['id']))
            db.execute('''INSERT INTO parse_runs(task_id,attempt_no,channel_key,pipeline_id,model_id,
                provider,instance,contract_version,started_at,cost_basis,cost_cny) VALUES (?,?,?,?,?,?,?,?,?,?,?)''',
                (row['id'],attempt,route['channel_key'],route['pipeline_id'],route['model_id'],
                 route['provider'],route['instance'],route['contract_version'],now,
                 'reserved_upper_bound' if budget is not None else 'unknown',reserve if budget is not None else None))
            db.execute('''INSERT INTO claim_cycle(stage,counter) VALUES ('parse',?)
                ON CONFLICT(stage) DO UPDATE SET counter=excluded.counter''',(counter+1,))
            result=_as_dict(db.execute('SELECT * FROM tasks WHERE id=?',(row['id'],)).fetchone())
            db.commit()
            return result

    def record_parse_baseline(self, task: dict, remote: dict) -> None:
        """Commit the last remote generation before the non-idempotent parse POST."""
        if (remote.get('id') != task.get('remote_id')
                or remote.get('name') != task.get('remote_name')
                or remote.get('pipeline_id') != task.get('pipeline_id')):
            raise ValueError('Parse baseline identity mismatch')
        encoded = json.dumps({'remote_id': remote['id'],
            'pipeline_id': remote.get('pipeline_id'), 'process_begin_at': remote.get('process_begin_at'),
            'run': remote.get('run')}, sort_keys=True)
        with closing(self._connect()) as db:
            db.execute('BEGIN IMMEDIATE')
            changed = db.execute('''UPDATE tasks SET parse_baseline=?,updated_at=?
                WHERE id=? AND status='parse_intent' AND execution_attempt=?
                AND remote_id=? AND COALESCE(execution_pipeline_id,pipeline_id) IS ?
                AND parse_baseline IS NULL''', (encoded, time.time(), task['id'],
                task['execution_attempt'], task['remote_id'], task['pipeline_id'])).rowcount
            if changed != 1:
                raise ValueError('Parse attempt changed before submission')
            db.execute('INSERT INTO attempts(task_id,operation,result,at) VALUES (?,?,?,?)',
                       (task['id'], 'parse_baseline', json.dumps({'attempt': task['execution_attempt'],
                        'remote': json.loads(encoded)}, sort_keys=True), time.time()))
            db.commit()

    def parse_attempts(self, task_id: int) -> list[dict]:
        with closing(self._connect()) as db:
            return [dict(row) for row in db.execute(
                'SELECT * FROM parse_runs WHERE task_id=? ORDER BY attempt_no',(task_id,))]

    def routed_waiting_count(self, route: dict) -> int:
        with closing(self._connect()) as db:
            return db.execute('''SELECT COUNT(*) FROM tasks WHERE status='parse_waiting'
                AND policy_key='education' AND role='paper_primary' AND service_key=?
                AND dataset_id=? AND pipeline_id=?
                AND (source_metadata_status IS NULL OR source_metadata_status='confirmed')''',
                (route['service_key'],route['dataset_id'],route['source_pipeline_id'])).fetchone()[0]

    def record_parse_usage(self, task_id: int, attempt_no: int, usage: dict) -> None:
        if set(usage)-{'input_tokens','output_tokens','cost_cny'} or not usage:
            raise ValueError('Invalid verified usage fields')
        for key in ('input_tokens','output_tokens'):
            if key in usage and (type(usage[key]) is not int or usage[key]<0): raise ValueError('Invalid token usage')
        if 'cost_cny' in usage and (type(usage['cost_cny']) not in (int,float) or not math.isfinite(usage['cost_cny']) or usage['cost_cny']<0):
            raise ValueError('Invalid usage cost')
        fields=dict(usage)
        if 'cost_cny' in fields: fields['cost_basis']='actual'
        with closing(self._connect()) as db:
            clause=','.join(f'{key}=?' for key in fields)
            if db.execute(f'UPDATE parse_runs SET {clause} WHERE task_id=? AND attempt_no=?',(*fields.values(),task_id,attempt_no)).rowcount!=1:
                raise ValueError('Unknown parse attempt')
            db.commit()

    def usage_summary(self, channel_key: str, now: float) -> dict:
        day_start=((now+28800)//86400)*86400-28800
        with closing(self._connect()) as db:
            row=db.execute('''SELECT COUNT(*) AS requests,COALESCE(SUM(input_tokens),0) AS input_tokens,
                COALESCE(SUM(output_tokens),0) AS output_tokens,COALESCE(SUM(cost_cny),0) AS cost_cny,
                SUM(CASE WHEN cost_basis='unknown' THEN 1 ELSE 0 END) AS unpriced_requests,
                SUM(CASE WHEN cost_basis='reserved_upper_bound' THEN 1 ELSE 0 END) AS reserved_requests
                FROM parse_runs WHERE channel_key=? AND started_at>=?''',(channel_key,day_start)).fetchone()
            return dict(row)

    def retry_on_channel(self, task_id: int, expected: str, route: dict, reason: str) -> bool:
        if expected!='technical_review' or reason not in {
                'llm:rate_limit','llm:authentication','llm:arrearage','llm:unavailable','llm:timeout'}:
            return False
        with closing(self._connect()) as db:
            db.execute('BEGIN IMMEDIATE')
            row=db.execute('SELECT * FROM tasks WHERE id=?',(task_id,)).fetchone()
            if (row is None or row['status']!=expected or row['remote_run']!='FAIL'
                    or not row['remote_id'] or not row['parse_required'] or row['role']!='paper_primary'
                    or row['policy_key']!='education' or row['execution_attempt']<1
                    or row['service_key']!=route['service_key'] or row['dataset_id']!=route['dataset_id']
                    or row['pipeline_id']!=route['source_pipeline_id']
                    or row['execution_channel_key']==route['channel_key']
                    or db.execute("SELECT 1 FROM attempts WHERE task_id=? AND operation='automatic_model_fallback'",
                                  (task_id,)).fetchone()):
                return False
            db.execute('''UPDATE tasks SET status='parse_waiting',preferred_channel_key=?,owner=NULL,
                lease_until=NULL,outcome=NULL,failure_kind=NULL,updated_at=? WHERE id=?''',
                (route['channel_key'],time.time(),task_id))
            db.execute('INSERT INTO attempts(task_id,operation,result,at) VALUES (?,?,?,?)',
                       (task_id,'automatic_model_fallback',reason,time.time()))
            db.commit()
            return True

    def route_state(self, channel_key: str) -> dict:
        with closing(self._connect()) as db:
            row=db.execute('SELECT state_json FROM route_states WHERE channel_key=?',(channel_key,)).fetchone()
            return json.loads(row[0]) if row else {}

    def save_route_state(self, channel_key: str, state: dict) -> None:
        from ingest_model_routes import _no_credentials
        _no_credentials(state)
        if not channel_key or not isinstance(state,dict):
            raise ValueError('Invalid route state')
        encoded=json.dumps(state,allow_nan=False)
        with closing(self._connect()) as db:
            db.execute('''INSERT INTO route_states(channel_key,state_json) VALUES (?,?)
                ON CONFLICT(channel_key) DO UPDATE SET state_json=excluded.state_json''',(channel_key,encoded))
            db.commit()

    def append_attempt(self, task_id: int, operation: str, result: str,
                       at: float) -> None:
        if not operation or not result:
            raise ValueError('Attempt operation and result are required')
        with closing(self._connect()) as db:
            db.execute('INSERT INTO attempts(task_id,operation,result,at) VALUES (?,?,?,?)',
                       (task_id, operation, result, at))
            db.commit()

    def record_diagnostic(self, task_id: int, diagnostic: dict, outcome: str) -> bool:
        allowed={'source','component_id','dependency','kind','outcome','reason','log_id',
                 'execution_id','remote_id','pipeline_id','attempt','process_begin_at'}
        valid_source=(diagnostic.get('source')=='execution_node' or
                      (is_database_status_diagnostic(diagnostic) and outcome=='technical_review'))
        if (set(diagnostic)-allowed or not valid_source
                or outcome not in {'quality_hold','quality_reject','technical_review','outcome_unresolved'}):
            raise ValueError('Invalid execution diagnostic')
        encoded=json.dumps(diagnostic,ensure_ascii=False,sort_keys=True)
        if len(encoded)>5000:raise ValueError('Execution diagnostic too large')
        with closing(self._connect()) as db:
            db.execute('BEGIN IMMEDIATE')
            row=db.execute('SELECT * FROM tasks WHERE id=?',(task_id,)).fetchone()
            if (not row or row['status']!=outcome or row['execution_attempt']!=diagnostic.get('attempt',0)
                    or row['remote_id']!=diagnostic.get('remote_id')
                    or (row['execution_pipeline_id'] or row['pipeline_id'])!=diagnostic.get('pipeline_id')):
                return False
            if not db.execute("SELECT 1 FROM attempts WHERE task_id=? AND operation='diagnostic' AND result=?",
                              (task_id,encoded)).fetchone():
                db.execute('INSERT INTO attempts(task_id,operation,result,at) VALUES (?,?,?,?)',
                           (task_id,'diagnostic',encoded,time.time()))
            db.execute('UPDATE parse_runs SET outcome=?,failure_kind=? WHERE task_id=? AND attempt_no=?',
                       (outcome,diagnostic.get('kind'),task_id,row['execution_attempt']))
            db.commit()
            return True

    def attempts(self, task_id: int) -> list[dict]:
        with closing(self._connect()) as db:
            rows = db.execute('SELECT * FROM attempts WHERE task_id=? ORDER BY id',
                              (task_id,)).fetchall()
            return [dict(row) for row in rows]

    def _apply_imported_receipt(self, db, row, status, updates, now):
        if set(updates)-_RECEIPT_FIELDS or status not in _RECEIPT_STATES:
            raise ValueError('Invalid verified receipt')
        if (not row or row['status'] not in ('upload_waiting','upload_unknown','technical_review','outcome_unresolved')
                or (row['lease_until'] is not None and row['lease_until']>now)):
            raise ValueError('Existing task is not safely adoptable')
        if row['remote_id'] not in (None,updates.get('remote_id')):
            raise ValueError('Existing remote ID differs')
        fields=dict(updates,status=status,owner=None,lease_until=None,updated_at=now)
        clause=','.join(f'{key}=?' for key in fields)
        changed=db.execute(f'UPDATE tasks SET {clause} WHERE id=? AND status=? AND updated_at=? AND remote_id IS ?',
            (*fields.values(),row['id'],row['status'],row['updated_at'],row['remote_id'])).rowcount
        if changed!=1:
            raise ValueError('Receipt adoption lost queue identity')

    def import_verified_receipt(self, task_id, expected, status, updates):
        with closing(self._connect()) as db:
            db.execute('BEGIN IMMEDIATE')
            row=db.execute('SELECT * FROM tasks WHERE id=?',(task_id,)).fetchone()
            if not row or row['status']!=expected:
                raise ValueError('Existing task is not safely adoptable')
            self._apply_imported_receipt(db,row,status,updates,time.time())
            db.commit()

    def request_review(self, task_id: int, reason: str) -> None:
        if not reason or not reason.strip():
            raise ValueError('A review reason is required')
        with closing(self._connect()) as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT * FROM tasks WHERE id=?', (task_id,)).fetchone()
            if (row is None or row['status'] not in _REVIEWABLE
                    or not row['remote_id'] or not row['parse_required']):
                raise ValueError('Task is not eligible for a second parse attempt')
            db.execute('''INSERT INTO attempts(task_id,operation,result,at)
                          VALUES (?,?,?,?)''',
                       (task_id, 'review_requested', reason.strip()[:200], time.time()))
            db.execute('''UPDATE tasks SET status='parse_waiting',outcome=NULL,
                          failure_kind=NULL,error=NULL,owner=NULL,lease_until=NULL,
                          updated_at=? WHERE id=?''', (time.time(), task_id))
            db.commit()

    def expired_intents(self, now: float) -> list[dict]:
        with closing(self._connect()) as db:
            rows = db.execute('''SELECT * FROM tasks WHERE status IN
                ('upload_intent','parse_intent') AND lease_until<? ORDER BY id''',
                (now,)).fetchall()
            return [_as_dict(row) for row in rows]

    def reconciliation_ready(self,now: float,limit: int) -> list[dict]:
        with closing(self._connect()) as db:
            rows=db.execute('''SELECT * FROM tasks WHERE
                (status IN ('upload_intent','parse_intent') AND lease_until<?)
                OR (status IN ('upload_unknown','parse_unknown') AND updated_at<?)
                ORDER BY updated_at,id LIMIT ?''',(now,now-15,limit)).fetchall()
            return [_as_dict(row) for row in rows]

    def diagnostics_ready(self, limit: int = 1000) -> list[dict]:
        with closing(self._connect()) as db:
            rows=db.execute('''SELECT * FROM tasks WHERE status='outcome_unresolved'
                AND failure_kind='diagnostic_pending' ORDER BY updated_at,id LIMIT ?''',(limit,))
            return [_as_dict(row) for row in rows]

    def list_statuses(self, statuses: tuple[str, ...], limit: int = 1000) -> list[dict]:
        if not statuses or limit < 1:
            return []
        placeholders = ','.join('?' for _ in statuses)
        with closing(self._connect()) as db:
            rows = db.execute(f'''SELECT * FROM tasks WHERE status IN ({placeholders})
                                  ORDER BY created_at,id LIMIT ?''',
                              (*statuses, limit)).fetchall()
            return [_as_dict(row) for row in rows]

    def summary(self) -> dict[str, int]:
        with closing(self._connect()) as db:
            return {row['status']: row['count'] for row in db.execute(
                'SELECT status,COUNT(*) AS count FROM tasks GROUP BY status')}

    def claim_source_metadata(self, now: float, limit: int, exclude_ids=()) -> list[dict]:
        """Claim only enrolled, upload-confirmed tasks; leases survive process death."""
        claimed = []
        with closing(self._connect()) as db:
            db.execute('BEGIN IMMEDIATE')
            rows = db.execute('''SELECT * FROM tasks WHERE remote_id IS NOT NULL
                AND status NOT IN ('upload_waiting','upload_intent','upload_unknown')
                AND ((source_metadata_status='pending' AND COALESCE(source_metadata_retry_at,0)<=?)
                  OR (source_metadata_status='running' AND source_metadata_lease_until<=?))
                ORDER BY COALESCE(source_metadata_retry_at,0),id LIMIT ?''',
                (now, now, limit+len(exclude_ids)+4)).fetchall()
            for row in rows:
                if row['id'] in exclude_ids:
                    continue
                if row['source_metadata_attempts'] >= 4:
                    db.execute("UPDATE tasks SET source_metadata_status='blocked',source_metadata_reason='retry_exhausted',updated_at=? WHERE id=?",
                               (now, row['id']))
                    continue
                if len(claimed) >= limit:
                    break
                db.execute("""UPDATE tasks SET source_metadata_status='running',
                    source_metadata_request_id=?,source_metadata_attempts=source_metadata_attempts+1,
                    source_metadata_lease_until=?,updated_at=? WHERE id=?""",
                    (uuid.uuid4().hex, now+150, now, row['id']))
                claimed.append(_as_dict(db.execute('SELECT * FROM tasks WHERE id=?', (row['id'],)).fetchone()))
            db.commit()
        return claimed

    def request_source_metadata_retry(self, task_id: int, expected_updated_at: float,
                                      remote_id: str, reason='operator_retry') -> bool:
        """Explicit repair action starts one bounded cycle on the same document."""
        if not remote_id or not reason or len(reason)>80:
            raise ValueError('Metadata retry requires identity and a safe reason')
        now = time.time()
        with closing(self._connect()) as db:
            db.execute('BEGIN IMMEDIATE')
            changed = db.execute("""UPDATE tasks SET source_metadata_status='pending',
                source_metadata_attempts=0,source_metadata_retry_at=NULL,
                source_metadata_lease_until=NULL,source_metadata_request_id=?,
                source_metadata_receipt=NULL,source_metadata_verification=NULL,
                source_metadata_reason=?,updated_at=?
                WHERE id=? AND updated_at=? AND remote_id=? AND role='paper_primary'
                AND source_metadata_status='blocked' AND source_metadata_payload IS NOT NULL
                AND status NOT IN ('upload_waiting','upload_intent','upload_unknown')""",
                (uuid.uuid4().hex,reason,now,task_id,expected_updated_at,remote_id)).rowcount
            if changed:
                db.execute('INSERT INTO attempts(task_id,operation,result,at) VALUES (?,?,?,?)',
                           (task_id,'source_metadata_retry',reason,now))
            db.commit()
            return changed==1

    def finish_source_metadata(self, task: dict, *, status: str, reason: str,
                               receipt: dict | None = None) -> bool:
        """Independent CAS: never change upload, model or quality state."""
        if status not in ('confirmed', 'pending', 'blocked'):
            raise ValueError('Invalid metadata completion')
        if status == 'confirmed':
            from ingest_source_metadata import validate_receipt
            receipt = validate_receipt(task, receipt)
            if receipt['status'] != 'confirmed':
                raise ValueError('Receiver did not confirm metadata')
        now = time.time()
        retries = task['source_metadata_attempts']
        if status == 'pending' and retries >= 4:
            status, reason = 'blocked', 'retry_exhausted'
        retry_at = now+(15,45,120)[retries-1] if status == 'pending' else None
        encoded = json.dumps(receipt, ensure_ascii=False) if receipt else None
        with closing(self._connect()) as db:
            cursor = db.execute('''UPDATE tasks SET source_metadata_status=?,
                source_metadata_reason=?,source_metadata_retry_at=?,source_metadata_lease_until=NULL,
                source_metadata_receipt=?,source_metadata_verification=?,updated_at=?
                WHERE id=? AND updated_at=? AND status=? AND remote_id=?
                AND source_metadata_request_id=? AND source_metadata_status='running'
                AND source_metadata_payload=?''',
                (status, reason, retry_at, encoded, receipt.get('verification') if receipt else None,
                 now, task['id'], task['updated_at'], task['status'], task['remote_id'],
                 task['source_metadata_request_id'], task['source_metadata_payload']))
            if cursor.rowcount:
                db.execute('INSERT INTO attempts(task_id,operation,result,at) VALUES (?,?,?,?)',
                           (task['id'], 'source_metadata', status+':'+reason, now))
            db.commit()
            return cursor.rowcount == 1

    def source_metadata_summary(self) -> dict:
        with closing(self._connect()) as db:
            counts = dict(db.execute('''SELECT source_metadata_status,COUNT(*) FROM tasks
                WHERE source_metadata_status IS NOT NULL GROUP BY source_metadata_status'''))
            completed = db.execute("""SELECT COUNT(*) FROM tasks WHERE role='paper_primary'
                AND parse_required=0 AND status='uploaded'
                AND (source_metadata_status IS NULL OR source_metadata_status='confirmed')""").fetchone()[0]
            reasons = dict(db.execute("""SELECT source_metadata_reason,COUNT(*) FROM tasks
                WHERE source_metadata_status='blocked' GROUP BY source_metadata_reason"""))
            return {'pending': counts.get('pending',0)+counts.get('running',0),
                    'confirmed': counts.get('confirmed',0), 'blocked': counts.get('blocked',0),
                    'blocked_reasons': reasons,
                    'upload_only_complete': completed}

    def artifact_summary(self) -> dict[str, int]:
        with closing(self._connect()) as db:
            rows = db.execute('''SELECT post_status,COUNT(*) FROM tasks
                WHERE status='accepted' AND role='paper_primary' GROUP BY post_status''')
            counts = dict(rows)
            return {'complete': counts.get('done',0), 'needs_review': counts.get('needs_review',0),
                    'pending': counts.get('pending',0)+counts.get('running',0)}

    def post_ready(self,limit: int) -> list[dict]:
        with closing(self._connect()) as db:
            rows=db.execute('''SELECT * FROM tasks WHERE status='accepted'
                AND role='paper_primary' AND post_status IN ('pending','running')
                AND (post_retry_at IS NULL OR post_retry_at<=?)
                ORDER BY created_at,id LIMIT ?''',(time.time(),limit)).fetchall()
            return [_as_dict(row) for row in rows]

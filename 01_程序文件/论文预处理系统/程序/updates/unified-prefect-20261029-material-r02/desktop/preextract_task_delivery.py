"""Task metadata and reference authority over the existing delivery queue.

No worker, scheduler, model call or remote operation is started here. Register
the WEB identity before creating its native run; bind each real run before it
can produce a queue reference. Dispatch also requires a finite, explicitly
recorded grant; legacy control compatibility alone never grants dispatch.
"""
from __future__ import annotations

import json
import hashlib
import re
import sqlite3
import time
from contextlib import closing, contextmanager
from pathlib import Path


def _identity(value):
    if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,127}', value):
        raise ValueError('a specific task/run identity is required')
    return value


def _sources(values):
    if isinstance(values, (str, bytes)):
        raise ValueError('source hashes must be a nonempty collection')
    try:
        result = list(values)
    except TypeError as exc:
        raise ValueError('source hashes must be a nonempty collection') from exc
    if not result or any(not isinstance(value, str) or not re.fullmatch(r'[0-9a-f]{64}', value) for value in result):
        raise ValueError('exact source SHA256 identities are required')
    return sorted(set(result))


@contextmanager
def _readonly(path):
    with closing(sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro', uri=True, timeout=5)) as db:
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA query_only=ON')
        db.execute('BEGIN')
        yield db


def _tables(db):
    return {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def _schema(db):
    # Metadata in the existing queue, with no second delivery/job store.
    db.execute('''CREATE TABLE IF NOT EXISTS delivery_tasks (
        batch_id TEXT PRIMARY KEY, source_hashes TEXT NOT NULL,
        mapping_revision INTEGER NOT NULL, created_at REAL NOT NULL)''')
    db.execute('''CREATE TABLE IF NOT EXISTS delivery_task_runs (
        batch_id TEXT NOT NULL, run_id TEXT NOT NULL, source_hash TEXT NOT NULL,
        PRIMARY KEY(batch_id,run_id,source_hash))''')
    db.execute('CREATE INDEX IF NOT EXISTS delivery_task_run_sources ON delivery_task_runs(run_id,source_hash)')
    db.execute('''CREATE TABLE IF NOT EXISTS delivery_task_source_bindings (
        batch_id TEXT NOT NULL, run_id TEXT NOT NULL, source_hash TEXT NOT NULL,
        input_source_hash TEXT NOT NULL, evidence TEXT NOT NULL,
        PRIMARY KEY(batch_id,run_id,source_hash))''')


def _task(db, batch_id):
    if 'delivery_tasks' not in _tables(db):
        raise ValueError('task delivery has not been registered')
    row = db.execute('SELECT * FROM delivery_tasks WHERE batch_id=?', (batch_id,)).fetchone()
    if row is None:
        raise ValueError('task delivery has not been registered')
    return dict(row)


def _bind(db, batch_id, run_id, sources, source_bindings=()):
    task = _task(db, batch_id)
    frozen=set(json.loads(task['source_hashes']))
    derived={proof['source_sha256']:proof for proof in source_bindings}
    if not set(sources) <= frozen | set(derived) or any(proof['input_source_sha256'] not in frozen for proof in derived.values()):
        raise ValueError('run sources are outside the registered task')
    changed = False
    for source,proof in derived.items():
        previous=db.execute('SELECT * FROM delivery_task_source_bindings WHERE batch_id=? AND run_id=? AND source_hash=?',
                            (batch_id,run_id,source)).fetchone()
        if previous:
            original=json.loads(previous['evidence'])
            identity=('source_sha256','input_source_sha256','relation','producer_run_id','source_path',
                      'input_source_path','parent_source_sha256','parent_source_path','archive_member_path','method','record_id')
            if any(original.get(key)!=proof.get(key) for key in identity):
                raise ValueError('registered native source lineage changed')
        else:
            db.execute('INSERT INTO delivery_task_source_bindings VALUES(?,?,?,?,?)',
                       (batch_id,run_id,source,proof['input_source_sha256'],json.dumps(proof,ensure_ascii=False,sort_keys=True)))
            changed=True
    for source in sources:
        changed |= bool(db.execute('INSERT OR IGNORE INTO delivery_task_runs VALUES(?,?,?)',
                                  (batch_id, run_id, source)).rowcount)
    if changed:
        db.execute('UPDATE delivery_tasks SET mapping_revision=mapping_revision+1 WHERE batch_id=?', (batch_id,))


def register_task_delivery(root, *, batch_id, source_hashes, run_ids=()):
    from filelock import FileLock
    from preextract_delivery_queue import DeliveryQueue, queue_path
    from preextract_delivery_control import fence_path, intent_lock_path
    batch_id, sources = _identity(batch_id), _sources(source_hashes)
    if isinstance(run_ids, (str, bytes)):
        raise ValueError('run identities must be a collection')
    run_ids = sorted({_identity(run_id) for run_id in run_ids})
    queue = DeliveryQueue(queue_path(root))
    scope = 'run:' + batch_id
    fence_path(queue.path, scope).parent.mkdir(parents=True, exist_ok=True)
    with FileLock(intent_lock_path(queue.path, scope), timeout=5), FileLock(str(queue.path) + '.admission.lock', timeout=5):
        with closing(queue._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            _schema(db)
            old = db.execute('SELECT * FROM delivery_tasks WHERE batch_id=?', (batch_id,)).fetchone()
            if old:
                if json.loads(old['source_hashes']) != sources:
                    raise ValueError('task source identities are immutable')
            else:
                if db.execute('SELECT 1 FROM delivery_controls WHERE scope=?', (scope,)).fetchone():
                    raise ValueError('unregistered task identity already has delivery control; explicit migration required')
                db.execute('INSERT INTO delivery_tasks VALUES(?,?,?,?)',
                           (batch_id, json.dumps(sources), 1, time.time()))
                # Creation and the initial native hold become visible in the
                # same transaction. Re-registering never pauses a started task.
                db.execute('INSERT INTO delivery_controls VALUES(?,?,?,?,?,?)',
                           (scope, 1, 1, 'task_pending_start', time.time(), ''))
            for run_id in run_ids:
                _bind(db, batch_id, run_id, sources)
    return task_delivery_snapshot(root, batch_id=batch_id)


def bind_task_delivery_run(root, *, batch_id, run_id, source_hashes, source_evidence=(), binding_token=None):
    from filelock import FileLock
    from preextract_delivery_queue import DeliveryQueue, queue_path
    batch_id, run_id, sources = _identity(batch_id), _identity(run_id), _sources(source_hashes)
    path = queue_path(root)
    # Unknown task/action reads must not create a fresh production database.
    with _readonly(path) as db:
        frozen=json.loads(_task(db, batch_id)['source_hashes'])
    # Evidence reading/hash validation happens before Q; the native caller has
    # released G after persisting pending, and rechecks G only after our return.
    from preextract_task_sources import validate_source_evidence
    proofs,native=validate_source_evidence(root,batch_id=batch_id,run_id=run_id,
        frozen_sources=frozen,source_hashes=sources,source_evidence=source_evidence,binding_token=binding_token)
    queue = DeliveryQueue(path)
    with FileLock(str(path) + '.admission.lock', timeout=5), closing(queue._connect()) as db, db:
        db.execute('BEGIN IMMEDIATE')
        _schema(db)
        _bind(db, batch_id, run_id, sources, proofs)
        task=_task(db,batch_id)
        control=_control(db,path,'run:'+batch_id)
        global_upload=_control(db,path,'*')
        run_control=_control(db,path,'run:'+run_id)
        # Return the exact persisted source set, including earlier same-run
        # members; never disguise an additive run mapping as a replacement.
        registered=[row[0] for row in db.execute('SELECT source_hash FROM delivery_task_runs WHERE batch_id=? AND run_id=? ORDER BY source_hash',
                                                (batch_id,run_id))]
        lineage=[json.loads(row[0]) for row in db.execute('SELECT evidence FROM delivery_task_source_bindings WHERE batch_id=? AND run_id=? ORDER BY source_hash',
                                                         (batch_id,run_id))]
    def digest(value):
        return hashlib.sha256(json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode('utf-8')).hexdigest()
    return dict(schema='task-delivery-binding/2',binding_state='registered',batch_id=batch_id,run_id=run_id,
        requested_source_hashes=sources,source_hashes=registered,input_source_hashes=sorted(set(frozen)&set(registered)
            | {proof['input_source_sha256'] for proof in lineage}),
        source_bindings=lineage,
        source_set_sha256=digest(registered),source_bindings_sha256=digest(lineage),
        mapping_revision=task['mapping_revision'],control_generation=control['generation'],control=control,
        global_upload=global_upload,run_control=run_control,native_observation=native,binding_token=binding_token,
        global_stage=None,admission_open=None,readiness_scope='mapping_acknowledgement_only',observed_at=time.time())


def _task_action(root, *, batch_id, request_id, expected_generation, action):
    from preextract_delivery_queue import DeliveryQueue, queue_path
    batch_id = _identity(batch_id)
    if not isinstance(request_id, str) or not request_id or len(request_id) > 128:
        raise ValueError('a stable command identity is required')
    if type(expected_generation) is not int or expected_generation < 0:
        raise ValueError('the task delivery control generation is required')
    path = queue_path(root)
    with _readonly(path) as db:
        _task(db, batch_id)
    queue = DeliveryQueue(path)
    if action == 'pause':
        result = queue.pause(run_id=batch_id, reason='task_pause', request_id=request_id,
                             expected_generation=expected_generation)
    else:
        result = queue.resume(run_id=batch_id, request_id=request_id, expected_generation=expected_generation)
    # The saved command result is immutable. Live status is deliberately
    # separate: repeating an old pause after resume cannot masquerade as a new
    # effective pause, or silently apply that old pause again.
    return dict(schema='task-delivery-command/1', batch_id=batch_id, request_id=request_id,
                action=action, expected_generation=expected_generation, result=result,
                current=task_delivery_snapshot(root, batch_id=batch_id))


def pause_task_delivery(root, *, batch_id, request_id, expected_generation):
    return _task_action(root, batch_id=batch_id, request_id=request_id,
                        expected_generation=expected_generation, action='pause')


def resume_task_delivery(root, *, batch_id, request_id, expected_generation):
    return _task_action(root, batch_id=batch_id, request_id=request_id,
                        expected_generation=expected_generation, action='resume')


def _control(db, path, scope, *, rows=None):
    from preextract_delivery_control import read_fence
    fence = read_fence(path, scope)
    row = (rows.get(scope) if rows is not None else
        db.execute('SELECT * FROM delivery_controls WHERE scope=?', (scope,)).fetchone() if db else None)
    if row and (type(row['generation']) is not int or row['generation'] < 0 or row['paused'] not in (0, 1)):
        raise ValueError('delivery control is invalid')
    return dict(paused=bool(fence) or bool(row and row['paused']), generation=row['generation'] if row else 0,
                reason=row['reason'] if row else '', updated_at=row['updated_at'] if row else None,
                stop_pending=bool(fence and (not row or row['fence_id']!=fence['fence_id'] or not row['paused'])))


def _owners(db, tables, run_id, source):
    if not db or 'delivery_tasks' not in tables:
        return [None]
    if 'delivery_task_runs' not in tables:
        raise ValueError('task run mapping is incomplete')
    owners = {row[0] for row in db.execute('SELECT batch_id FROM delivery_task_runs WHERE run_id=? AND source_hash=?',
                                          (run_id, source))}
    task = db.execute('SELECT source_hashes FROM delivery_tasks WHERE batch_id=?', (run_id,)).fetchone()
    if task and source in json.loads(task['source_hashes']):
        owners.add(run_id)
    if owners:
        return sorted(owners)
    # A known task run with a missing source mapping is unknown, not legacy.
    known_run = task or db.execute('SELECT 1 FROM delivery_task_runs WHERE run_id=? LIMIT 1', (run_id,)).fetchone()
    return [] if known_run else [None]


def _capture_control_authority(path, payload, *, delivery_id=None, db=None):
    """Interpret controls only; this is not dispatch permission."""
    path = Path(path).resolve()
    if db is None and path.is_file():
        with _readonly(path) as connection:
            return _capture_control_authority(path, payload, delivery_id=delivery_id, db=connection)
    tables = _tables(db) if db else set()
    global_control = _control(db, path, '*')
    result = dict(schema='delivery-reference-authority/1', delivery_id=delivery_id,
                  payload=payload, global_generation=global_control['generation'], references=[])
    if global_control['paused']:
        return result
    item = payload.get('item') or {}
    source = item.get('source_hash', '')
    if payload.get('task', {}).get('source_sha256', source) != source:
        raise ValueError('canonical delivery source identity differs from its sealed task')
    if delivery_id is not None:
        if not db or 'references_to_deliveries' not in tables:
            return result
        references = [dict(row) for row in db.execute(
            'SELECT run_id,item_id,source_hash FROM references_to_deliveries WHERE delivery_id=?', (delivery_id,))]
    else:
        references = [dict(run_id=payload['run_id'], item_id=item['id'], source_hash=source)]
    for reference in references:
        if reference['source_hash'] != source:
            raise ValueError('shared reference source identity changed')
        run_scope = 'run:' + reference['run_id']
        native = _control(db, path, run_scope)
        if native['paused']:
            continue
        for owner in _owners(db, tables, reference['run_id'], source):
            generations = {'*': global_control['generation'], run_scope: native['generation']}
            if owner is not None:
                owner_scope = 'run:' + owner
                control = _control(db, path, owner_scope)
                # A dangling mapping cannot grant open generation-zero access.
                if not db.execute('SELECT 1 FROM delivery_controls WHERE scope=?', (owner_scope,)).fetchone():
                    raise ValueError('task delivery control is missing')
                if control['paused']:
                    continue
                generations[owner_scope] = control['generation']
            result['references'].append(dict(reference, batch_id=owner, generations=generations))
    return result


def _scope_handle(path, service, scope_ids=()):
    from upload_tracking import service_key
    return dict(schema='delivery-dispatch-scope/1', queue_path=str(Path(path).resolve()),
                service=service_key(service), scope_ids=sorted(set(scope_ids)))


def _dispatch_context(path, service, dispatch_scope):
    """Read G before Q, never from inside a Q writer/admission transaction."""
    empty = dict(scope=_scope_handle(path, service), global_task_generation=None)
    if dispatch_scope is None:
        return empty
    if (not isinstance(dispatch_scope, dict)
            or dispatch_scope.get('schema') != 'delivery-dispatch-scope/1'
            or Path(dispatch_scope.get('queue_path', '')).resolve() != Path(path).resolve()
            or dispatch_scope.get('service') != empty['scope']['service']):
        raise ValueError('dispatch scope belongs to another queue or service')
    ids = dispatch_scope.get('scope_ids')
    if not isinstance(ids, list):
        raise ValueError('dispatch scope identities must be a list')
    ids = sorted({_identity(value) for value in ids})
    if not ids:
        return empty
    stage = _global_snapshot(Path(path).resolve().parents[2])
    if stage.get('admission_open') is not True or type(stage.get('generation')) is not int:
        return empty
    return dict(scope=_scope_handle(path, service, ids), global_task_generation=stage['generation'])


def _grant(db, path, scope_id):
    if 'delivery_control_commands' not in _tables(db):
        return None
    row = db.execute('SELECT * FROM delivery_control_commands WHERE request_id=?', (scope_id,)).fetchone()
    if row is None:
        return None
    value = json.loads(row['result'])
    if (row['action'] != 'authorize_dispatch' or row['scope'] != 'dispatch:' + scope_id
            or value.get('schema') != 'delivery-dispatch-grant/1' or value.get('scope_id') != scope_id
            or value.get('queue_path') != str(Path(path).resolve())):
        raise ValueError('dispatch command identity belongs to another operation')
    return value


def _open_generations(db, path, generations):
    for scope, generation in generations.items():
        current = _control(db, path, scope)
        if type(generation) is not int or generation < 0 or current['paused'] or current['generation'] != generation:
            return False
    return True


def _scope_matches(grant, reference, delivery_id):
    for allowed in grant['references']:
        keys = ('run_id', 'source_hash', 'batch_id') if grant['mode'] == 'task' else (
            'run_id', 'item_id', 'source_hash', 'batch_id')
        if all(allowed.get(key) == reference.get(key) for key in keys):
            if grant['mode'] == 'task' or allowed['delivery_id'] == delivery_id:
                return True
    return False


def capture_delivery_authority(path, payload, *, delivery_id=None, db=None, dispatch_scope=None, _context=None):
    """Freeze the intersection of an explicit grant and real eligible references."""
    path = Path(path).resolve()
    if db is None:
        context = _dispatch_context(path, payload['service'], dispatch_scope)
        with _readonly(path) as connection:
            return capture_delivery_authority(path, payload, delivery_id=delivery_id, db=connection,
                                              dispatch_scope=dispatch_scope, _context=context)
    result = _capture_control_authority(path, payload, delivery_id=delivery_id, db=db)
    result['dispatch_scope'] = (_context or {}).get('scope', _scope_handle(path, payload['service']))
    result['global_task_generation'] = (_context or {}).get('global_task_generation')
    permitted = []
    if _context and result['global_task_generation'] is not None:
        if '_grants' not in _context:
            _context['_grants'] = {scope_id: _grant(db, path, scope_id) for scope_id in _context['scope']['scope_ids']}
        for scope_id, grant in _context['_grants'].items():
            if (grant is None or grant['service'] != result['dispatch_scope']['service']
                    or grant['global_task_generation'] != result['global_task_generation']
                    or grant['global_upload_generation'] != result['global_generation']):
                continue
            for reference in result['references']:
                if (_scope_matches(grant, reference, delivery_id)
                        and all(grant['generations'].get(key) == generation
                                for key, generation in reference['generations'].items())):
                    permitted.append(dict(reference, scope_id=scope_id))
    result['references'] = permitted
    return result


def _issue_dispatch(root, *, service, request_id, parameters, build):
    from preextract_delivery_queue import DeliveryQueue, queue_path, _save_control_command
    from upload_tracking import service_key
    from filelock import FileLock
    path = queue_path(root).resolve()
    request_id = _identity(request_id)
    service = service_key(service)
    if not path.is_file():
        raise ValueError('dispatch authorization requires the existing queue')
    def saved(db):
        old = _grant(db, path, request_id)
        if old is not None and old['parameters'] != parameters:
            raise ValueError('dispatch command identity belongs to different parameters')
        return _scope_handle(path, service, [request_id]) if old else None
    with _readonly(path) as db:
        old = saved(db)
        if old is not None:
            return old
    # The G observation is outside every Q lock. Old grants cannot be renewed
    # if G changes between this read and commit: their frozen G generation dies.
    stage = _global_snapshot(root)
    if stage.get('admission_open') is not True or type(stage.get('generation')) is not int:
        raise ValueError('global admission is not open for a new dispatch scope')
    queue = DeliveryQueue(path)
    with FileLock(str(path) + '.admission.lock', timeout=5), closing(queue._connect()) as db, db:
        db.execute('BEGIN IMMEDIATE')
        old = saved(db)
        if old is not None:
            return old
        references, generations = build(db, path)
        if not references or not _open_generations(db, path, generations):
            raise ValueError('dispatch references or control generations are not open')
        grant = dict(schema='delivery-dispatch-grant/1', scope_id=request_id, queue_path=str(path),
                     service=service, mode=parameters['mode'], parameters=parameters,
                     global_task_generation=stage['generation'], global_upload_generation=generations['*'],
                     generations=generations, references=references, created_at=time.time())
        _save_control_command(db, request_id, 'dispatch:' + request_id, 'authorize_dispatch',
                              parameters['expected_generation'], grant)
    return _scope_handle(path, service, [request_id])


def authorize_task_dispatch(root, *, service, batch_id, run_ids, request_id, expected_generation):
    from upload_tracking import service_key
    batch_id = _identity(batch_id)
    if isinstance(run_ids, (str, bytes)):
        raise ValueError('specific bound run identities are required')
    runs = sorted({_identity(run_id) for run_id in run_ids})
    if not runs or type(expected_generation) is not int or expected_generation < 0:
        raise ValueError('specific runs and task control generation are required')
    parameters = dict(mode='task', service=service_key(service), batch_id=batch_id,
                      run_ids=runs, expected_generation=expected_generation)
    def build(db, path):
        _task(db, batch_id)
        owner = 'run:' + batch_id
        if not db.execute('SELECT 1 FROM delivery_controls WHERE scope=?', (owner,)).fetchone():
            raise ValueError('task delivery control is missing')
        generations = {'*': _control(db, path, '*')['generation'], owner: expected_generation}
        references = []
        for run_id in runs:
            sources = [row[0] for row in db.execute(
                'SELECT source_hash FROM delivery_task_runs WHERE batch_id=? AND run_id=? ORDER BY source_hash',
                (batch_id, run_id))]
            if not sources:
                raise ValueError('task dispatch run has no validated source binding')
            generations.setdefault('run:' + run_id, _control(db, path, 'run:' + run_id)['generation'])
            references.extend(dict(run_id=run_id, source_hash=source, batch_id=batch_id) for source in sources)
        return references, generations
    return _issue_dispatch(root, service=service, request_id=request_id, parameters=parameters, build=build)


def authorize_legacy_dispatch(root, *, service, references, request_id, expected_generations):
    from upload_tracking import service_key
    if not isinstance(references, (list, tuple)) or not references or not isinstance(expected_generations, dict):
        raise ValueError('finite original references and exact control generations are required')
    selected = []
    for reference in references:
        if not isinstance(reference, dict) or set(reference) != {'delivery_id', 'run_id', 'item_id', 'source_hash'}:
            raise ValueError('legacy dispatch requires exact original reference identities')
        selected.append(dict(delivery_id=_identity(reference['delivery_id']), run_id=_identity(reference['run_id']),
                             item_id=_identity(reference['item_id']), source_hash=_sources([reference['source_hash']])[0]))
    selected = sorted({json.dumps(row, sort_keys=True): row for row in selected}.values(),
                      key=lambda row: (row['delivery_id'], row['run_id'], row['item_id'], row['source_hash']))
    if '*' not in expected_generations or any(type(value) is not int or value < 0 for value in expected_generations.values()):
        raise ValueError('exact global and original reference generations are required')
    parameters = dict(mode='legacy', service=service_key(service), references=selected,
                      expected_generations=expected_generations, expected_generation=expected_generations['*'])
    def build(db, path):
        result, scopes = [], {'*'}
        tables = _tables(db)
        for reference in selected:
            row = db.execute('''SELECT d.payload,d.service,r.source_hash FROM references_to_deliveries r
                JOIN deliveries d ON d.id=r.delivery_id WHERE r.delivery_id=? AND r.run_id=? AND r.item_id=?''',
                (reference['delivery_id'], reference['run_id'], reference['item_id'])).fetchone()
            if row is None or row['source_hash'] != reference['source_hash'] or row['service'] != parameters['service']:
                raise ValueError('legacy reference differs from the retained queue identity')
            payload = json.loads(row['payload'])
            if (payload['item'].get('source_hash') != reference['source_hash']
                    or payload.get('task', {}).get('source_sha256', reference['source_hash']) != reference['source_hash']):
                raise ValueError('legacy sealed source identity differs from its reference')
            scopes.add('run:' + reference['run_id'])
            owners = _owners(db, tables, reference['run_id'], reference['source_hash'])
            if not owners:
                raise ValueError('known task source binding is incomplete')
            for owner in owners:
                if owner is not None:
                    owner_scope = 'run:' + owner
                    if not db.execute('SELECT 1 FROM delivery_controls WHERE scope=?', (owner_scope,)).fetchone():
                        raise ValueError('task delivery control is missing')
                    scopes.add(owner_scope)
                result.append(dict(reference, batch_id=owner))
        if set(expected_generations) != scopes:
            raise ValueError('legacy authorization generations must cover exactly its original scopes')
        return result, dict(expected_generations)
    return _issue_dispatch(root, service=service, request_id=request_id, parameters=parameters, build=build)


def active_dispatch_scope(root, service):
    from preextract_delivery_queue import queue_path
    path = queue_path(root).resolve()
    empty = _scope_handle(path, service)
    stage = _global_snapshot(root)
    if stage.get('admission_open') is not True or type(stage.get('generation')) is not int or not path.is_file():
        return empty
    with _readonly(path) as db:
        if 'delivery_control_commands' not in _tables(db):
            return empty
        global_upload = _control(db, path, '*')
        if global_upload['paused']:
            return empty
        ids = []
        for row in db.execute("SELECT request_id FROM delivery_control_commands WHERE action='authorize_dispatch'"):
            grant = _grant(db, path, row['request_id'])
            if (grant['service'] == empty['service'] and grant['global_task_generation'] == stage['generation']
                    and grant['global_upload_generation'] == global_upload['generation']):
                for reference in grant['references']:
                    scopes = {'*', 'run:' + reference['run_id']}
                    if reference['batch_id'] is not None:
                        scopes.add('run:' + reference['batch_id'])
                    if _open_generations(db, path, {key: grant['generations'][key] for key in scopes}):
                        ids.append(grant['scope_id'])
                        break
    return _scope_handle(path, service, ids)


def _reference_key(reference):
    return tuple(reference.get(key) for key in ('run_id', 'item_id', 'source_hash', 'batch_id', 'scope_id'))


def delivery_authority_open(path, authority):
    current = capture_delivery_authority(path, authority['payload'], delivery_id=authority['delivery_id'],
                                         dispatch_scope=authority.get('dispatch_scope'))
    if (current['global_generation'] != authority['global_generation']
            or current['global_task_generation'] != authority.get('global_task_generation')):
        return False
    eligible = {_reference_key(reference): reference['generations'] for reference in current['references']}
    return any(eligible.get(_reference_key(reference)) == reference['generations'] for reference in authority['references'])


def _global_snapshot(root):
    # Reuse native state/owner interpretation with a strictly read-only handle.
    from global_task_control import GlobalTaskControl
    control = GlobalTaskControl(root)
    if not control.path.is_file():
        state = control._snapshot()
        # Task creation still succeeds. New task execution never interprets a
        # missing G database as authority to initialize/resume global work.
        state.update(state='unknown', admission_open=False)
        return state
    with _readonly(control.path) as db:
        data = control._state_rows(db)
    return control._snapshot(data=data)


def task_delivery_snapshot(root, *, batch_id):
    from preextract_delivery_queue import queue_path
    batch_id = _identity(batch_id)
    path = queue_path(root)
    result = dict(schema='task-delivery-state/1', batch_id=batch_id, observed_at=time.time(),
                  state='unknown', admission_open=False, control_generation=None, mapping_revision=None,
                  run_ids=[], run_bindings=[], run_controls=[], source_count=None, delivery_counts={}, in_flight=None, shared_continues=None,
                  execution_ready=None, local_submission_state='unknown', readback_state='unknown', reason_codes=[])
    try:
        with _readonly(path) as db:
            task = _task(db, batch_id)
            control_rows={row['scope']:row for row in db.execute('SELECT * FROM delivery_controls')}
            control = _control(db, path, 'run:' + batch_id,rows=control_rows)
            if 'run:'+batch_id not in control_rows:
                raise ValueError('task delivery control is missing')
            global_upload = _control(db, path, '*',rows=control_rows)
            by_run={}
            for row in db.execute('SELECT run_id,source_hash FROM delivery_task_runs WHERE batch_id=? ORDER BY run_id,source_hash',(batch_id,)):
                by_run.setdefault(row['run_id'],dict(run_id=row['run_id'],source_hashes=[]))['source_hashes'].append(row['source_hash'])
            run_ids=list(by_run)
            bindings=list(by_run.values())
            if 'delivery_task_source_bindings' in _tables(db):
                for binding in bindings:binding['source_bindings']=[]
                for row in db.execute('SELECT run_id,evidence FROM delivery_task_source_bindings WHERE batch_id=? ORDER BY run_id,source_hash',(batch_id,)):
                    if row['run_id'] in by_run:by_run[row['run_id']]['source_bindings'].append(json.loads(row['evidence']))
            run_controls=[dict(run_id=run_id,**_control(db,path,'run:'+run_id,rows=control_rows)) for run_id in run_ids]
            rows = db.execute('''SELECT DISTINCT d.id,d.status,d.payload FROM deliveries d JOIN references_to_deliveries r ON r.delivery_id=d.id
                WHERE EXISTS(SELECT 1 FROM delivery_task_runs m WHERE m.batch_id=? AND m.run_id=r.run_id AND m.source_hash=r.source_hash)
                OR (r.run_id=? AND r.source_hash IN (SELECT value FROM json_each(?)))''',
                              (batch_id, batch_id, task['source_hashes'])).fetchall()
            counts, shared = {}, False
            for row in rows:
                counts[row['status']] = counts.get(row['status'], 0) + 1
                if row['status'] != 'complete':
                    payload=json.loads(row['payload'])
                    # This same SQL read transaction fixes paused=1, so the
                    # authority reader would immediately return no references.
                    # Keep parsing every payload: malformed evidence is unknown.
                    confirmed_global_pause=control_rows.get('*') is not None and control_rows['*']['paused']==1
                    if confirmed_global_pause:
                        # Keep the original per-delivery fence validation: a
                        # damaged stop file must still make this read unknown.
                        _control(db,path,'*',rows=control_rows)
                    else:
                        authority = _capture_control_authority(path,payload,delivery_id=row['id'],db=db)
                        shared |= any(reference['batch_id'] != batch_id for reference in authority['references'])
            global_stage = _global_snapshot(root)
            reasons = []
            if control['paused']:reasons.append('task_delivery_paused')
            if global_upload['paused']:reasons.append('global_upload_paused')
            if not global_stage['admission_open']:reasons.append('global_admission_closed')
            if not run_ids:reasons.append('native_run_not_bound')
            elif all(row['paused'] for row in run_controls):reasons.append('native_runs_paused')
            # Queue rows do not cover direct handoff or prove remote inactivity,
            # even when empty or all complete. Only original receipt readback
            # can close that gap; never turn absence of rows into zero activity.
            in_flight = None
            state = ('draining' if control['paused'] and any(counts.get(status) for status in ('uploading', 'needs_review'))
                     else 'paused' if control['paused'] else 'open')
            if state == 'open' and reasons:state = 'waiting'
            result.update(state=state, admission_open=not reasons, control_generation=control['generation'],
                          control=control,
                          mapping_revision=task['mapping_revision'], run_ids=run_ids,
                          run_bindings=bindings, run_controls=run_controls,
                          source_count=len(json.loads(task['source_hashes'])), delivery_counts=counts,
                          in_flight=in_flight, shared_continues=bool(shared and global_stage['admission_open']),
                          local_submission_state='paused' if control['paused'] else 'open',
                          readback_state='unknown',
                          global_stage=global_stage, reason_codes=reasons,
                          readiness_scope='local_task_permission_only')
    except (OSError, sqlite3.Error, ValueError, TypeError, KeyError) as exc:
        result.update(state='unknown', admission_open=False, reason_codes=['task_delivery_unknown'],
                      error_type=type(exc).__name__)
    return result

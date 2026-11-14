"""Read existing WEB/native/queue identities for a finite storage operation.

No registration, permission, scheduler or policy is created here. A shared or
unbound delivery is retained. WEB, native, G and Q revisions remain distinct.
"""
from contextlib import closing, contextmanager, ExitStack
import copy
import hashlib
import json
from pathlib import Path
import sqlite3

from filelock import FileLock


def _digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                    separators=(',', ':')).encode('utf8')).hexdigest()


@contextmanager
def _database(path, *, reserve=False):
    # Never create a missing task/control database while inspecting a scope.
    mode = 'rw' if reserve else 'ro'
    with closing(sqlite3.connect(Path(path).resolve().as_uri() + '?mode=' + mode,
                                uri=True, timeout=0)) as db:
        db.row_factory = sqlite3.Row
        db.execute('BEGIN IMMEDIATE' if reserve else 'BEGIN')
        try:
            yield db
        finally:
            db.rollback()


def _stamp(path):
    stat = Path(path).stat()
    return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns


def _web(db, batch_id, *, verify_inputs=True):
    row = db.execute('SELECT * FROM task_controls WHERE batch_id=?', (batch_id,)).fetchone()
    batch = db.execute('SELECT * FROM batches WHERE batch_id=?', (batch_id,)).fetchone()
    if (row is None or batch is None or row['managed'] != 1 or row['desired_state'] != 'running'
            or row['registration_state'] != 'ready' or batch['state'] in {'created', 'paused', 'archived'}
            or db.execute("SELECT 1 FROM task_commands WHERE batch_id=? AND status='pending' LIMIT 1",
                          (batch_id,)).fetchone()):
        raise ValueError('storage_task_intent_not_open')
    commands = db.execute('SELECT action,status FROM task_commands WHERE batch_id=? AND result_revision=?',
                          (batch_id, row['revision'])).fetchall()
    if len(commands) != 1 or commands[0]['action'] not in {'start', 'resume'} or commands[0]['status'] != 'applied':
        raise ValueError('storage_task_command_not_applied')
    workspace = db.execute("SELECT value FROM metadata WHERE key='workspace'").fetchone()
    # An initial WB legitimately has no workspace metadata and uses id=''.
    if (json.loads(workspace[0]).get('id', '') if workspace is not None else '') != row['workspace_id']:
        raise ValueError('storage_workspace_changed')
    keys = json.loads(batch['inputs'])
    if not isinstance(keys, list) or not keys or len(set(keys)) != len(keys):
        raise ValueError('storage_frozen_inputs_missing')
    meta=dict(row,input_ids=keys)
    if not verify_inputs:return meta,None
    inputs = [db.execute('SELECT id,relative_path,sha256,bytes,path FROM inputs WHERE id=?', (key,)).fetchone()
              for key in keys]
    if any(value is None for value in inputs):
        raise ValueError('storage_frozen_input_missing')
    frozen = [{key: value[key] for key in ('id', 'relative_path', 'sha256', 'bytes')} for value in inputs]
    if _digest(frozen) != row['manifest_sha256']:
        raise ValueError('storage_frozen_input_identity_changed')
    return meta, [dict(value) for value in inputs]


def _item_identity(item):
    return {key: item.get(key) for key in ('id', 'source_hash', 'content_format',
        'preextract_work_dir', 'package_path', 'status', 'automatic_approval')}


def _native_item_identity(item):
    return dict(_item_identity(item),source_path=item.get('source_path'),
                input_source_path=item.get('input_source_path'))


class StorageItem:
    """One real, exclusive task/run/item/source and its frozen current activation."""
    def __init__(self, root, job, dispatch_scope, batch_id, *, stop=None):
        from native_batch_control import NativeBatchControl
        from preextract_task_delivery import capture_delivery_authority
        from preextract_delivery_queue import queue_path
        from preextract_cleanup import _safe_path
        self.root = Path(root).resolve()
        self.path = queue_path(self.root).resolve()
        self.web_path = self.root / 'private/workbench/intake.sqlite3'
        self.job = copy.deepcopy(job)
        self.id, self.batch_id, self.stop = job['id'], batch_id, stop
        self.payload = job['payload']
        item = self.payload['item']
        if job['status'] != 'complete' or self.payload.get('kind') == 'associated_original' or item.get('prepared_scope'):
            raise ValueError('storage_requires_normal_complete_paper')
        if item.get('status') != 'pass' or item.get('automatic_approval') is not True:
            raise ValueError('storage_paper_not_approved')
        self.run_id, self.item_id, self.source = self.payload['run_id'], item['id'], item['source_hash']
        self.work = _safe_path(item['preextract_work_dir'], self.root)
        self.package = _safe_path(item['package_path'], self.root)
        self.authority = capture_delivery_authority(self.path, self.payload, delivery_id=self.id,
                                                    dispatch_scope=dispatch_scope)
        refs = self.authority['references']
        if not refs or any(ref['batch_id'] != batch_id or ref['run_id'] != self.run_id
                           or ref['item_id'] != self.item_id or ref['source_hash'] != self.source for ref in refs):
            raise ValueError('storage_exact_task_reference_missing')
        self.authority = copy.deepcopy(self.authority)
        self.control = NativeBatchControl(self.root)
        native = self.control.assert_running(batch_id)
        self.epoch = native['control_revision']
        binding = self.control._ready_binding(native, self.run_id, self.epoch)
        self.binding_digest = _digest(binding)
        self.native_stamp=None
        self.control._check_delivery(self.control._delivery_snapshot(batch_id), binding, require_open=True)
        store = _safe_path(binding['store_root'], self.root) if Path(binding['store_root']).resolve() != self.root else self.root
        self.manifest = _safe_path(store / 'runs' / self.run_id / 'manifest.json', self.root)
        before = _stamp(self.manifest)
        manifest = json.loads(self.manifest.read_text(encoding='utf8'))
        rows = [entry for entry in manifest.get('items', []) if entry.get('id') == self.item_id]
        if manifest.get('run_id') != self.run_id or len(rows) != 1 or _item_identity(rows[0]) != _item_identity(item):
            raise ValueError('storage_run_item_identity_missing_or_changed')
        # Q intentionally omits source paths. Only the unique native manifest
        # item may supply them; never enrich the saved Q payload by a hash alias.
        self.native_item=copy.deepcopy(rows[0])
        self.manifest_item_identity=_native_item_identity(self.native_item)
        self.manifest_stamp = _stamp(self.manifest)
        if before != self.manifest_stamp:
            raise ValueError('storage_run_manifest_changed_while_reading')
        with _database(self.web_path) as db:
            self.web, inputs = _web(db, batch_id)
        self.input_digest = _digest(inputs)
        if (native['source_manifest_sha256'] != self.web['manifest_sha256']
                or sorted(native['source_hashes']) != sorted({entry['sha256'] for entry in inputs})
                or self.web['native_revision'] != self.epoch):
            raise ValueError('storage_web_native_selection_differs')
        with _database(self.path) as db:
            task = db.execute('SELECT source_hashes FROM delivery_tasks WHERE batch_id=?', (batch_id,)).fetchone()
            sources = [row[0] for row in db.execute('SELECT source_hash FROM delivery_task_runs WHERE batch_id=? AND run_id=? ORDER BY source_hash',
                                                   (batch_id, self.run_id))]
            proof = db.execute('SELECT evidence FROM delivery_task_source_bindings WHERE batch_id=? AND run_id=? AND source_hash=?',
                               (batch_id, self.run_id, self.source)).fetchone()
            if (task is None or sorted(json.loads(task[0])) != sorted(native['source_hashes'])
                    or sources != binding['source_hashes'] or self.source not in sources):
                raise ValueError('storage_native_queue_source_binding_missing')
            self._queue_check(db)
        lineage = json.loads(proof[0]) if proof else None
        input_path = Path((lineage or {}).get('input_source_path') or self.native_item.get('input_source_path') or self.native_item['source_path']).resolve()
        input_hash = (lineage or {}).get('input_source_sha256', self.source)
        selected = [entry for entry in inputs if entry['sha256'] == input_hash and Path(entry['path']).resolve() == input_path]
        if len(selected) != 1 or (lineage is not None and
                (lineage.get('source_sha256') != self.source or Path(lineage['source_path']).resolve() != Path(self.native_item['source_path']).resolve())):
            raise ValueError('storage_frozen_input_lineage_missing_or_ambiguous')
        self.identity = dict(schema='native-task-storage-item/1', batch_id=batch_id, run_id=self.run_id,
            item_id=self.item_id, source_sha256=self.source, work_dir=str(self.work), package_path=str(self.package),
            package_manifest_sha256=self.payload['manifest_sha256'], input_id=selected[0]['id'],
            input_path=str(input_path),input_sha256=input_hash,source_path=self.native_item['source_path'],
            frozen_manifest_sha256=self.web['manifest_sha256'], lineage_sha256=_digest(lineage))
        self.assert_open()

    def _queue_check(self, db, *, path=None,shared=True):
        from preextract_task_delivery import _control, _grant
        row = db.execute('SELECT status,payload FROM deliveries WHERE id=?', (self.id,)).fetchone()
        refs = db.execute('SELECT run_id,item_id,source_hash,package_path FROM references_to_deliveries WHERE delivery_id=?', (self.id,)).fetchall()
        owners = db.execute('SELECT batch_id FROM delivery_task_runs WHERE run_id=? AND source_hash=?',
                            (self.run_id, self.source)).fetchall()
        if (row is None or row['status'] != 'complete' or json.loads(row['payload']) != self.payload
                or len(refs) != 1 or tuple(refs[0]) != (self.run_id, self.item_id, self.source, str(self.package))
                or [value[0] for value in owners] != [self.batch_id]):
            raise ValueError('storage_shared_or_changed_delivery_reference')
        for reference in self.authority['references']:
            for key, expected in reference['generations'].items():
                control = _control(db, self.path, key)
                if control['paused'] or control['generation'] != expected:
                    raise ValueError('storage_delivery_activation_changed')
            grant = _grant(db, self.path, reference['scope_id'])
            if (grant is None or grant['mode'] != 'task' or grant['global_task_generation'] != self.authority['global_task_generation']):
                raise ValueError('storage_task_dispatch_grant_changed')
        # Shared workspaces/packages are never owned just because bytes match.
        if not shared:return
        for other in db.execute("SELECT id,json_extract(payload,'$.item.preextract_work_dir'),json_extract(payload,'$.item.package_path'),json_extract(payload,'$.item.source_path'),json_extract(payload,'$.item.input_source_path') FROM deliveries WHERE id!=?", (self.id,)):
            for position,raw in enumerate(other[1:]):
                if not raw:
                    if position<2:raise ValueError('storage_other_delivery_path_unknown')
                    continue
                foreign = Path(raw).resolve()
                targets = [Path(path).resolve()] if path is not None else [self.work, self.package]
                if any(target == foreign or target.is_relative_to(foreign) or foreign.is_relative_to(target) for target in targets):
                    raise ValueError('storage_shared_path_retained')

    def _current(self, web_db, queue_db, *, full=False):
        if self.stop is not None and self.stop.is_set():
            raise InterruptedError('storage_stopped')
        web, inputs = _web(web_db, self.batch_id,verify_inputs=full)
        if (web['revision'] != self.web['revision'] or web['workspace_id'] != self.web['workspace_id']
                or web['manifest_sha256'] != self.web['manifest_sha256'] or web['input_ids']!=self.web['input_ids']
                or full and _digest(inputs) != self.input_digest
                or web['native_revision'] != self.epoch or web['delivery_generation'] != self.web['delivery_generation']):
            raise ValueError('storage_web_activation_changed')
        current=_stamp(self.control._path(self.batch_id))
        if self.native_stamp!=current:
            native = self.control.assert_running(self.batch_id, epoch=self.epoch)
            binding = self.control._ready_binding(native, self.run_id, self.epoch)
            after=_stamp(self.control._path(self.batch_id))
            if after!=current or _digest(binding)!=self.binding_digest:
                raise ValueError('storage_native_binding_changed')
            context=getattr(self,'release_context',None)
            if context is not None and (_digest(native.get('run_bindings',[]))!=context['bindings_sha256']
                    or (native.get('execution_owner') or {}).get('token')!=context['producer_owner_token']):
                raise ValueError('storage_release_producer_or_binding_changed')
            self.native_stamp=after
        if _stamp(self.manifest) != self.manifest_stamp:
            raise ValueError('storage_native_binding_or_manifest_changed')
        self._queue_check(queue_db,shared=False)
        if getattr(self,'release_context',None) is not None:
            from preextract_task_storage import validate_context
            validate_context(web_db,self.root,self.batch_id,self.release_context,input_id=self.identity['input_id'])

    def assert_open(self):
        from global_task_control import GlobalTaskControl
        stamp=_stamp(self.manifest)
        if stamp!=self.manifest_stamp:
            manifest=json.loads(self.manifest.read_text(encoding='utf8'))
            rows=[entry for entry in manifest.get('items',[]) if entry.get('id')==self.item_id]
            after=_stamp(self.manifest)
            if (stamp!=after or manifest.get('run_id')!=self.run_id or len(rows)!=1
                    or _native_item_identity(rows[0])!=self.manifest_item_identity):
                raise ValueError('storage_exact_manifest_item_changed')
            # Other papers' progress is not this item's identity. Rechecking
            # only this exact row avoids starving copies in a long-running batch.
            self.manifest_stamp=after
        gate = GlobalTaskControl(self.root)
        with _database(gate.path) as db:
            row = db.execute('SELECT generation,intent FROM control WHERE id=1').fetchone()
            if (row is None or row['intent'] != 'running' or gate._read_fence()
                    or row['generation'] != self.authority['global_task_generation']):
                raise ValueError('storage_global_activation_changed')
        with _database(self.web_path) as web, _database(self.path) as queue:
            self._current(web, queue)
        return True

    @contextmanager
    def _guard(self, *, stage=None, path=None, release_sha256=None):
        from global_task_control import GlobalTaskControl
        from preextract_delivery_control import intent_lock_path
        gate = GlobalTaskControl(self.root)
        gate.lock_timeout = 0
        self.assert_open()
        # Actual existing SQL/file locks, not the WB process's RLock. Slow NAS
        # reads, hashing and inventory generation all precede this short guard.
        with _database(self.web_path, reserve=True) as web, gate.admission() as admitted, ExitStack() as locks:
            if admitted.connection is None:
                raise ValueError('storage_global_control_missing')
            generations = self.authority['references'][0]['generations']
            for key in sorted(generations):
                locks.enter_context(FileLock(intent_lock_path(self.path, key), timeout=0))
            locks.enter_context(FileLock(str(self.path) + '.admission.lock', timeout=0))
            with _database(self.path) as queue:
                self._current(web, queue,full=True)
                current = admitted.connection.execute('SELECT generation FROM control WHERE id=1').fetchone()
                if current[0] != self.authority['global_task_generation']:
                    raise ValueError('storage_global_activation_changed')
                if path is not None:
                    self._queue_check(queue, path=path)
                if release_sha256 is not None:
                    if getattr(self,'release_context',None) is None:raise ValueError('storage_release_permission_missing')
                    from preextract_task_storage import validate_context
                    validate_context(web,self.root,self.batch_id,self.release_context,
                                     input_id=self.identity['input_id'],path=path,sha256=release_sha256)
                token = admitted.begin(stage, batch_id=self.run_id, paper_id=self.item_id) if stage else None
                yield token

    @contextmanager
    def stage(self, stage):
        from global_task_control import GlobalTaskControl
        with self._guard(stage=stage) as token:
            pass
        gate = GlobalTaskControl(self.root)
        try:
            with gate.using(token, stage):
                yield
        except BaseException as exc:
            gate.finish(token, 'failed', type(exc).__name__)
            raise
        else:
            gate.finish(token)

    def commit_guard(self, path=None):
        return self._guard(path=path)

    def release_guard(self,path,sha256):
        return self._guard(path=path,release_sha256=sha256)

    def authorize_original(self,path,sha256):
        if getattr(self,'release_context',None) is None:raise ValueError('storage_release_permission_missing')
        from preextract_task_storage import validate_context
        with _database(self.web_path) as db:
            validate_context(db,self.root,self.batch_id,self.release_context,
                             input_id=self.identity['input_id'],path=path,sha256=sha256)

    def owns_cleanup(self, path):
        path = Path(path).resolve()
        return path.is_relative_to(self.work) and not (path == self.work / 'model' or path.is_relative_to(self.work / 'model'))


def selected_items(root, *, delivery_ids=None, stop=None,dispatch_scope=None,expected_batch=None):
    """Existing active task grants only; an empty/unknown scope never means all."""
    from preextract_task_delivery import active_dispatch_scope, _grant, _readonly
    from preextract_delivery_queue import queue_path
    root = Path(root).resolve()
    result, held = {}, {}
    if delivery_ids is None:
        return result,{'scope':'storage_explicit_item_selection_required'}
    if isinstance(delivery_ids,(str,bytes)):
        return result,{'scope':'storage_finite_delivery_id_collection_required'}
    if delivery_ids is not None and not delivery_ids:
        return result, held
    try:
        settings = json.loads((root / 'settings.json').read_text(encoding='utf8'))
        # A grant confirms permission, never chooses items. The finite actual
        # references above are always provided by an existing task/NAS inventory.
        handle = dispatch_scope or active_dispatch_scope(root, settings['ragflow_url'])
        if not handle['scope_ids']:
            return result, {'scope': 'storage_no_active_task_scope'}
        with _readonly(queue_path(root)) as db:
            candidates = {}
            for key in handle['scope_ids']:
                grant = _grant(db, queue_path(root), key)
                if grant is None or grant['mode'] != 'task':
                    continue
                for reference in grant['references']:
                    for row in db.execute('''SELECT d.id,d.status,d.payload,d.projection FROM deliveries d
                        JOIN references_to_deliveries r ON r.delivery_id=d.id
                        WHERE r.run_id=? AND r.source_hash=? AND d.status='complete' ''',
                        (reference['run_id'], reference['source_hash'])):
                        if delivery_ids is not None and row['id'] not in delivery_ids:
                            continue
                        candidates.setdefault(row['id'], (dict(id=row['id'], status=row['status'],
                            payload=json.loads(row['payload']), projection=json.loads(row['projection'])),
                            dict(handle, scope_ids=[key]), reference['batch_id']))
        for ident, (job, scope, batch_id) in candidates.items():
            try:
                if expected_batch is not None and batch_id!=expected_batch:
                    raise ValueError('storage_reference_belongs_to_another_selected_task')
                result[ident] = StorageItem(root, job, scope, batch_id, stop=stop)
            except (OSError, ValueError, KeyError, TypeError, RuntimeError, sqlite3.Error) as exc:
                held[ident] = str(exc)[:180]
        if delivery_ids is not None:
            for ident in set(delivery_ids) - set(result) - set(held):
                held[ident] = 'storage_exact_task_binding_missing'
    except (OSError, ValueError, KeyError, TypeError, RuntimeError, sqlite3.Error) as exc:
        held['scope'] = 'storage_scope_unavailable:' + type(exc).__name__
    return result, held


def selected_task_items(root, *, batch_id, delivery_ids=None, stop=None,dispatch_scope=None):
    """Resolve a named existing selected WEB task; this grants no deletion."""
    from preextract_task_delivery import _identity,_readonly
    from preextract_delivery_queue import queue_path
    batch_id=_identity(batch_id)
    if (not isinstance(delivery_ids,(list,tuple)) or not delivery_ids
            or any(not isinstance(i,str) or not i for i in delivery_ids) or len(set(delivery_ids))!=len(delivery_ids)):
        return {},{'scope':'storage_explicit_unique_delivery_selection_required'}
    with _readonly(queue_path(root)) as db:
        rows=db.execute('''SELECT DISTINCT r.delivery_id FROM references_to_deliveries r
            JOIN delivery_task_runs m ON m.run_id=r.run_id AND m.source_hash=r.source_hash
            WHERE m.batch_id=?''',(batch_id,)).fetchall()
    ids=[row[0] for row in rows if delivery_ids is None or row[0] in delivery_ids]
    missing=set(delivery_ids)-set(ids)
    if missing:return {},{i:'storage_selected_delivery_not_bound_to_task' for i in missing}
    items,held=selected_items(root,delivery_ids=ids,stop=stop,dispatch_scope=dispatch_scope,expected_batch=batch_id)
    if held or set(items)!=set(delivery_ids):return {},dict(held,scope='storage_complete_selection_not_ready')
    return items,held


def registered_items(root, *, stop=None):
    """Only original Q storage requests and registered NAS task inventories."""
    from preextract_nas import policy
    from preextract_delivery_queue import queue_path
    root=Path(root).resolve();selections={};identities={};released=set();held={}
    try:
        value=policy(root)
        if not value.get('enabled'):return {},{'scope':'storage_nas_disabled'}
        database=Path(value['database'])
        if database.is_file():
            with _database(database) as db:
                for row in db.execute("SELECT id,payload,state FROM nas_jobs WHERE json_extract(payload,'$.task_identity.schema')='native-task-storage-item/1'"):
                    identity=json.loads(row['payload'])['task_identity']
                    if not Path(identity['work_dir']).resolve().is_relative_to(root):continue
                    if row['state']=='released':released.add(row['id']);continue
                    selections[row['id']]=identity['batch_id'];identities[row['id']]=identity
        with _database(queue_path(root)) as db:
            for row in db.execute("SELECT id,payload,projection FROM deliveries WHERE status='complete' AND json_extract(projection,'$.preextract_cleanup.task_storage_request.schema')='task-storage-selection/1'"):
                if row['id'] in released:continue
                payload=json.loads(row['payload']);request=json.loads(row['projection'])['preextract_cleanup']['task_storage_request'];item=payload['item']
                if (request['run_id']!=payload['run_id'] or request['item_id']!=item['id']
                        or request['source_hash']!=item['source_hash'] or request['manifest_sha256']!=payload['manifest_sha256']):
                    held[row['id']]='storage_saved_selection_identity_changed';continue
                if row['id'] in selections and selections[row['id']]!=request['batch_id']:
                    held[row['id']]='storage_saved_selection_owner_changed';selections.pop(row['id']);continue
                selections[row['id']]=request['batch_id']
        scopes={}
        for ident,batch in selections.items():
            selected,failed=selected_task_items(root,batch_id=batch,delivery_ids=[ident],stop=stop)
            held.update(failed)
            scope=selected.get(ident)
            if scope is not None:
                if ident in identities and scope.identity!=identities[ident]:held[ident]='storage_registered_task_identity_changed'
                else:scopes[ident]=scope
        return scopes,held
    except (OSError,ValueError,KeyError,TypeError,RuntimeError,sqlite3.Error) as exc:
        held['scope']='storage_registered_scope_unavailable:'+type(exc).__name__
        return {},held


def describe_items(items):
    """Worker parameters only, not a grant; all original controls must match."""
    return [dict(identity=scope.identity,delivery_id=scope.id,web_revision=scope.web['revision'],
                 native_epoch=scope.epoch,authority=scope.authority,release_context=getattr(scope,'release_context',None))
            for _,scope in sorted(items.items())]


def restore_items(root, descriptors, *, stop=None):
    result,held={},{}
    if not isinstance(descriptors,list) or not descriptors:return {},{'scope':'storage_explicit_worker_selection_missing'}
    ids=[v.get('delivery_id') for v in descriptors if isinstance(v,dict)]
    if len(ids)!=len(descriptors) or any(not isinstance(i,str) or not i for i in ids) or len(set(ids))!=len(ids):
        return {},{'scope':'storage_worker_selection_ambiguous'}
    for value in descriptors:
        ident=value['delivery_id'];identity=value['identity']
        scopes,failed=selected_items(root,delivery_ids=[ident],stop=stop,
            dispatch_scope=value['authority']['dispatch_scope'],expected_batch=identity['batch_id'])
        held.update(failed)
        scope=scopes.get(ident)
        if scope is not None:
            if value.get('release_context') is not None:
                try:
                    from preextract_task_storage import bind
                    bind(scope,value['release_context'])
                except (OSError,ValueError,KeyError,TypeError,RuntimeError,sqlite3.Error) as exc:
                    held[ident]=str(exc)[:180];continue
            current=describe_items({ident:scope})[0]
            if current!=value:held[ident]='storage_original_worker_activation_changed'
            else:result[ident]=scope
    if held or set(result)!=set(ids):return {},dict(held,scope='storage_complete_worker_selection_not_ready')
    return result,held

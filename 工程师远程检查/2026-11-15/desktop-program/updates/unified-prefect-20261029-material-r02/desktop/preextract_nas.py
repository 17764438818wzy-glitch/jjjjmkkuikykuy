"""Storage-only tail for the native preextractor. No dispatch/resume imports."""
from contextlib import contextmanager, ExitStack, closing
import copy
import functools
import hashlib
import importlib.util
import json
import logging
from pathlib import Path
import sqlite3
import threading
import time

from filelock import FileLock, Timeout
from paperflow.original_archive.lifecycle import ArchiveLifecycle, safe_file, path_key
from paperflow.original_archive.admission import admission_turn

_log=logging.getLogger(__name__)
TASK_STORAGE_SCOPE_VERSION=2


def policy(root):
    path=Path(root)/'private/preextract_delivery/nas-policy.json'
    if not path.is_file():return {'enabled':False}
    value=json.loads(path.read_text(encoding='utf8'))
    if value.get('schema')!='paper-original-archive-policy/2':raise ValueError('NAS 归档策略版本无效')
    return value


def lifecycle(root,*,task_items=None):
    value=policy(root)
    if task_items:
        # Restore the actual selected intake/work paths, not an unrestricted
        # new workspace root. The same original policy/flags remain in force.
        value=copy.deepcopy(value);roots=set(value['source_roots'])
        for scope in task_items.values():
            if scope.root!=Path(root).resolve():raise ValueError('storage_scope_workspace_differs')
            scope.assert_open()
            roots.update((str(scope.work),str(Path(scope.identity['input_path']).parent)))
        value['source_roots']=sorted(roots)
    return ArchiveLifecycle(root,value) if value.get('enabled') else None


def action_enabled(root, action):
    value=policy(root)
    return bool(value.get('enabled') and value.get(action+'_enabled',True))


def archive_binding(job_id, manifest_sha256):
    # Older loaded releasers compare this entire mapping. The versioned binding
    # prevents them from releasing a v24-cleaned original with v22 lock rules.
    return dict(job_id=job_id,manifest_sha256=manifest_sha256,release_authority='native-v24')


def input_paths(records):
    """Exact selected paths, including independent attachments and their packages."""
    result=set()
    def visit(value):
        if isinstance(value,dict):
            for key,item in value.items():
                if key in {'path','source_path','input_source_path','original_path','file'} and isinstance(item,str) and Path(item).is_absolute():
                    result.add(str(Path(item).absolute()))
                elif isinstance(item,(list,dict)):visit(item)
        elif isinstance(value,(list,tuple)):
            for item in value:visit(item)
    visit(records)
    return sorted(result)


@contextmanager
def use_inputs(root,records,*,stop=None,progress=None):
    from processing_stop import check_stop,current_stop
    stop=stop if stop is not None else current_stop()
    paths=input_paths(records);life=None;reported=False
    def waiting():
        nonlocal reported
        if not reported and progress is not None:progress('waiting_for_local_lock')
        reported=True
    # Queue only registration. The turn ends before yielding any business work,
    # so preparation/model/postprocessing capacities remain independently parallel.
    with admission_turn(root,stop=stop,on_wait=waiting):
        while True:
            check_stop(stop)
            try:
                if life is None:life=lifecycle(root)
                if life is None:break
                token=life.begin_use(paths)
                break
            except (Timeout,sqlite3.OperationalError) as exc:
                if isinstance(exc,sqlite3.OperationalError) and (
                        getattr(exc,'sqlite_errorcode',0)&255) not in (sqlite3.SQLITE_BUSY,sqlite3.SQLITE_LOCKED):
                    raise
                waiting()
                # Only the queue head retries external FileLock/SQLite writers;
                # successors wake directly on handoff, without exponential delay.
                if stop is not None:
                    if stop.wait(.005):check_stop(stop)
                else:time.sleep(.005)
    if life is None:
        yield lambda result:None
        return
    result={}
    def save(value):
        if not isinstance(value,dict):return
        # Never persist model/config/credentials from a return object.
        result.update({k:copy.deepcopy(value[k]) for k in ('status','source_count','processed_sources','run_id',
            'work_dir','source_sha256','stage') if k in value})
        result['items']=[{k:i[k] for k in ('id','status','source_hash','source_path','input_source_path',
            'preextract_work_dir','content_format','automatic_approval') if k in i} for i in value.get('items',[])]
        result['has_unresolved_events']=bool(value.get('events'))
    try:yield save
    finally:
        # Save a durable receipt first. A busy archive database cannot block a
        # completed body or overwrite its normal return with a storage failure.
        try:
            life.defer_end_use(token,result)
        except Exception as exc:
            _log.warning('NAS exit receipt pending: token=%s error_type=%s',token,type(exc).__name__)
        try:
            life.end_use(token,closed=False,result=result,timeout=0)
        except Exception as exc:
            _log.warning('NAS end_use deferred: token=%s error_type=%s',token,type(exc).__name__)
        # An unresolved/active lease stays protected until native reconciliation.
        try:
            kick(root,stop=stop)
        except Exception as exc:
            _log.warning('NAS storage wakeup deferred: error_type=%s',type(exc).__name__)



def uses_batch(function):
    @functools.wraps(function)
    def wrapped(self,selected,*args,**kwargs):
        with use_inputs(self.store.root,selected,stop=getattr(self,'stop',None)) as save:
            result=function(self,selected,*args,**kwargs);save(result);return result
    return wrapped


def uses_single(function):
    @functools.wraps(function)
    def wrapped(source,*args,**kwargs):
        if kwargs.get('local_only'):
            return function(source,*args,**kwargs)
        DATA_DIR=kwargs.get('data_root') or function.__globals__['DATA_DIR']
        records=[{'path':str(source)},kwargs.get('associated_records') or [],kwargs.get('associated_document') or {}]
        with use_inputs(DATA_DIR,records,stop=kwargs.get('stop'),progress=kwargs.get('progress')) as save:
            result=function(source,*args,**kwargs);save(result);return result
    return wrapped


def _originals_only(record,bundle,plan):
    """Keep user originals; an intact, matching ZIP represents its members."""
    import hashlib,zipfile
    from preextract_cleanup_scope import sha
    originals=dict(record['originals']);covered=set()
    source=path_key(record['source_path'])
    def cover(value,digest):
        if not value:return
        key=path_key(value)
        for path,expected in originals.items():
            if path_key(path)==key and key!=source:
                if expected!=digest:raise ValueError('ZIP 附件原件身份不符，保留本地文件')
                covered.add(path)
    def visit(rows,parent=None):
        for row in rows:
            archive=parent
            container=row.get('input_source_path')
            if container and Path(container).is_absolute() and Path(container).suffix.lower()=='.zip':
                archive=Path(container)
                expected=row.get('archive_source_hash')
                if not archive.is_file() or (expected and sha(archive)!=expected):
                    raise ValueError('原始附件 ZIP 已改变，保留本地文件')
                originals.setdefault(str(archive),sha(archive))
            raw=row.get('source_path')
            if raw and Path(raw).is_absolute() and Path(raw).suffix.lower()=='.zip' and Path(raw).is_file():
                if sha(Path(raw))==row.get('sha256'):
                    archive=Path(raw);originals.setdefault(str(archive),row['sha256'])
            member=row.get('archive_member_path')
            if not member and raw and not Path(raw).is_absolute():member=raw
            digest=row.get('sha256') or row.get('source_sha256')
            if archive and member:
                try:
                    with zipfile.ZipFile(archive) as z,z.open(member) as stream:
                        matches=hashlib.file_digest(stream,'sha256').hexdigest()==digest
                except (OSError,ValueError,KeyError,zipfile.BadZipFile):matches=False
                if not matches:raise ValueError('ZIP 内附件未能核对，保留本地文件')
                cover(raw,digest);cover(row.get('original_path'),digest)
            elif archive and digest==sha(archive):
                # The raw ZIP already covers the processing copy of that ZIP.
                cover(row.get('original_path'),digest)
            visit(row.get('members') or [],archive)
    visit(bundle.get('attachments') or [])
    visit((plan or {}).get('files') or [])
    return {p:digest for p,digest in originals.items() if p not in covered}


def register_before_cleanup(root,job,record,bundle,plan,*,task_scope=None):
    if task_scope is None or task_scope.id!=job['id']:
        raise ValueError('storage_exact_task_scope_missing')
    task_scope.assert_open()
    life=lifecycle(root,task_items={task_scope.id:task_scope})
    if life is None:return True
    from preextract_retirement import identity
    old=life.get(job['id'])
    if old:
        if old['payload'].get('task_identity')!=task_scope.identity:
            raise ValueError('storage_registered_task_identity_missing_or_changed')
        if old['payload']['identity']!=identity(job['payload']):raise ValueError('NAS 交付身份改变')
        if old['payload'].get('retention_scope')=='originals-only/1':
            # Resume the same immutable original inventory, without rewriting
            # historical registrations or bringing extraction copies back in.
            record['originals']=copy.deepcopy(old['payload']['native']['record']['originals'])
        from preextract_cleanup_scope import sha
        if any(not Path(e['path']).is_file() or sha(Path(e['path']))!=e['sha256'] for e in old['payload']['source_evidence']):
            raise ValueError('归档后的原件清单发生变化，保留本地材料')
        ready=life.ready(job['id'],identity(job['payload']))
        if ready:record['original_archive']=archive_binding(job['id'],old['result']['manifest_sha256'])
        return ready
    from preextract_cleanup_scope import sha
    files={};work=Path(record['work_dir'])
    def add(path,digest,kind,*,held=None,release=True):
        path=safe_file(path,life.roots)
        if not path.is_file():raise ValueError('归档原件缺失：'+path.name)
        if sha(path)!=digest:raise ValueError('归档原件身份不符：'+path.name)
        if kind=='原始上传包' or path.suffix.lower()=='.zip' and path_key(path)!=path_key(record['source_path']):
            held='shared_archive_container_members_not_closed';release=False
        if path.is_relative_to(task_scope.work/'private'):
            held='retained_for_saved_result_reuse';release=False
        current=files.setdefault(str(path),dict(path=str(path),name=path.name,kind=kind,sha256=digest,
            bytes=path.stat().st_size,hold_reason=held,release=release))
        if held:current['hold_reason']=held
    record['originals']=_originals_only(record,bundle,plan)
    for path,digest in record['originals'].items():
        add(path,digest,'原始来源' if Path(path)==Path(record['source_path']) else '附件')
    # Include the original user upload container, which the delivery payload does
    # not retain. Read its exact matching manifest item before native retirement.
    manifest=Path(root)/'runs'/job['payload']['run_id']/'manifest.json'
    full=json.loads(manifest.read_text(encoding='utf8'))
    source_item=next((i for i in full.get('items',[]) if i.get('id')==job['payload']['item']['id']),None)
    if source_item:
        container=source_item.get('input_source_path')
        if container and path_key(container)!=path_key(record['source_path']):
            add(container,sha(Path(container)),'原始上传包')
    evidence=[dict(path=str(p),sha256=sha(p)) for p in (work/'material.json',work/'private/associated-upload-plan.json') if p.is_file()]
    captured=dict(id=job['id'],retention_scope='originals-only/1',identity=identity(job['payload']),source_sha256=record['source_sha256'],
        task_identity=copy.deepcopy(task_scope.identity),
        title=record['title'] or record['source_name'],files=list(files.values()),source_evidence=evidence,
        native=dict(record=copy.deepcopy(record),material=copy.deepcopy(bundle),plan=copy.deepcopy(plan)))
    life.register(captured)
    return False


def _reconcile_uses(life,jobs):
    """Only native terminal proofs resolve a saved consumer; normal exit is insufficient."""
    from preextract_retirement import terminal,FIELD
    terminal_jobs=[j for j in jobs if terminal(j) and j['projection'][FIELD]['status']=='complete']
    for use in life.uses():
        if use['state']=='active':continue
        result=use['result'];items=result.get('items',[])
        if result.get('work_dir') and result.get('source_sha256'):
            items=[dict(preextract_work_dir=result['work_dir'],source_hash=result['source_sha256'])]
        elif (result.get('status') not in {'complete','completed'} or result.get('has_unresolved_events')
                or result.get('source_count')!=result.get('processed_sources')):continue
        matches=[]
        for item in items:
            found=[j for j in terminal_jobs if j['payload']['item'].get('source_hash')==item.get('source_hash')
                and j['payload']['item'].get('preextract_work_dir')==item.get('preextract_work_dir')]
            if len(found)!=1:break
            matches+=found
        else:
            proven={path_key(j['projection'][FIELD]['source_path']) for j in matches}
            for job in matches:
                archived=life.get(job['id'])
                if archived:
                    proven.update(path_key(f['path']) for f in archived['payload']['files'] if not f.get('hold_reason'))
            remaining=sorted(set(use['paths'])-proven)
            resolved=sorted(set(use['paths'])&proven)
            if resolved:
                # Resolve individual paths; a shared package keeps its consumer
                # without preventing the completed paper from being released.
                result['resolved_paths']=sorted(set(result.get('resolved_paths',[]))|set(resolved))
                with life.db() as db:db.execute('UPDATE nas_uses SET state=?,paths=?,result=? WHERE id=?',
                    ('unresolved' if remaining else 'closed',json.dumps(remaining),json.dumps(result),use['id']))


@contextmanager
def reference_guard(root,captured,stop=None,*,task_scope=None):
    """Existing queue/frozen-scope checks plus new original-file admission gate."""
    from preextract_delivery_queue import DeliveryQueue,queue_path
    from preextract_retirement import jobs,terminal,FIELD,parent_evidence,_references,_consumer_documents
    from preextract_cleanup_scope import family_workspaces,dependency_keys,Dependencies
    from preextract_scope_guard import retirement_admission
    from preextract_cleanup import _references as paths,legacy_processes_alive
    from preextract_coordinator import control_path
    life=lifecycle(root,task_items={task_scope.id:task_scope}) if task_scope is not None else None
    if (task_scope is None or task_scope.root!=Path(root).resolve()
            or captured.get('task_identity')!=task_scope.identity or task_scope.id!=captured['id']):
        raise ValueError('storage_exact_task_binding_missing')
    task_scope.assert_open()
    if life is None or not action_enabled(root,'release'):raise ValueError('归档释放已停止')
    if stop is not None and stop.is_set():raise ValueError('已停止释放')
    if legacy_processes_alive(root):raise ValueError('旧版本任务仍在使用文件')
    root=Path(root).resolve();queue=DeliveryQueue(queue_path(root))
    with ExitStack() as stack:
        stack.enter_context(FileLock(str(queue.path.with_name('cleanup.lock')),timeout=0))
        job=queue.get(captured['id'])
        if not job or job['status']!='complete':
            raise ValueError('原生交付尚未结清')
        from preextract_retirement import identity,_gate
        if identity(job['payload'])!=captured['identity']:raise ValueError('归档与交付版本不符')
        archived=life.get(captured['id'])
        record=copy.deepcopy(captured['native']['record']);work=Path(record['work_dir'])
        for lock in (work/'operation.lock',root/'single-request/source-index.json.lock',root/'private/purpose-review/lifecycle.lock'):
            if lock.parent.exists():stack.enter_context(FileLock(str(lock),timeout=0))
        # The preserve-results lane keeps these local receipts/packages. A
        # missing retained receipt is unknown; no old retirement is imported.
        verified,_,_,_=_gate(root,job,verify_originals=False)
        if any(verified.get(key)!=record.get(key) for key in
                ('identity','routes','targets','attachments','associated_plan_sha256')):
            raise ValueError('storage_complete_rag_evidence_changed')
        cleanup=json.loads((work/'cleanup-receipt.json').read_text(encoding='utf8'))
        expected=dict(identity=task_scope.identity,nas_manifest_sha256=archived['result']['manifest_sha256'])
        if cleanup.get('status')!='complete' or cleanup.get('task_storage')!=expected:
            raise ValueError('storage_scoped_cleanup_not_complete')
        from preextract_cleanup_scope import PROOF,verify_reuse
        from paperflow.original_archive.lifecycle import locked_file,verify_stream
        # Keep the proof and the same-byte local body substitute readable and
        # undeletable through this release, outside the short G/Q commit guard.
        stack.enter_context(locked_file(work/PROOF))
        reuse=verify_reuse(work)
        if (reuse['identity']!={key:captured['identity'][key] for key in ('task','manifest_sha256')}
                or reuse['service']!=captured['identity']['service']):
            raise ValueError('storage_saved_result_identity_changed')
        substitutes=set()
        for original,value in (reuse.get('source_substitutions') or {}).items():
            if (Path(original).resolve()!=Path(record['source_path']).resolve()
                    or value.get('storage_identity')!=task_scope.identity
                    or value.get('nas_manifest_sha256')!=expected['nas_manifest_sha256']
                    or value.get('sha256')!=record['source_sha256']):
                raise ValueError('storage_saved_original_substitute_binding_changed')
            replacement=safe_file(value['path'],[work.resolve()])
            if (not replacement.is_relative_to(work/'private') or not replacement.name.startswith('original.')
                    or reuse['retained'].get(str(replacement))!=value['sha256']):
                raise ValueError('storage_saved_original_substitute_path_changed')
            stream,_=stack.enter_context(locked_file(replacement))
            verify_stream(stream,value['sha256'],replacement.stat().st_size)
            substitutes.add(path_key(original))
        database=control_path(root)
        db=None;version=None
        if database.is_file():
            db=stack.enter_context(closing(sqlite3.connect(database,timeout=0)))
            version=db.execute('PRAGMA data_version').fetchone()[0]
        admit=retirement_admission((record.get('scope_proof') or {}).get('binding'))
        if not admit():raise ValueError('范围已停止或存在未决请求')
        all_jobs=jobs(root,compact=True)
        # Reconcile before taking the uses snapshot in ArchiveLifecycle.release.
        _reconcile_selected_uses(life,task_scope,captured)
        record.update(originals={},family=[str(p) for p in family_workspaces(root,work,record['source_sha256'])],
                      owned_workspaces=[str(work)])
        protected=_references(root,record,all_jobs)
        extra=set()
        for other in all_jobs:
            if other['id']==job['id']:continue
            if terminal(other) and other['projection'][FIELD]['status']=='complete':continue
            item=other['payload']['item'];extra.update(paths(item))
            for document in _consumer_documents(Path(item['preextract_work_dir'])):
                extra.update(dependency_keys(document,'references'))
        extra=Dependencies(extra)
        if db is not None:
            # Native registry constructors write idempotent schema. Complete
            # their checks first, then freeze this exact observed generation.
            db.execute('BEGIN IMMEDIATE')
            if db.execute('PRAGMA data_version').fetchone()[0]!=version:
                raise ValueError('任务引用在核对期间改变，稍后重新核对')
        def check(path,digest):
            if stop is not None and stop.is_set() or not action_enabled(root,'release') or not admit():return '已停止释放'
            own=next(f for f in captured['files'] if Path(f['path'])==path)
            if own.get('hold_reason'):return own['hold_reason']
            try:task_scope.authorize_original(path,digest)
            except ValueError as exc:return str(exc)[:180]
            if str(path) in reuse.get('retained',{}):return 'saved_result_still_depends_on_local_file'
            if path_key(path)==path_key(record['source_path']) and path_key(path) not in substitutes:
                return 'saved_result_original_substitute_not_verified'
            if protected.overlaps(path) or extra.overlaps(path):return '其他待办仍引用这个文件'
            return None
        check.commit_guard=lambda path:task_scope.release_guard(path,next(f['sha256'] for f in captured['files'] if Path(f['path'])==path))
        from paperflow.original_archive.lifecycle import isolation_record
        check.isolation_record=lambda path,digest,size:isolation_record(root,
            dict(task_identity=task_scope.identity,release_context=task_scope.release_context),path,digest,size)
        yield check


def release_once(root,limit=4,stop=None,*,task_items=None):
    if not action_enabled(root,'release'):return []
    if task_items is None:
        return [dict(status='waiting',released_files=0,released_bytes=0,
                     reason='storage_explicit_selected_task_items_required')]
    try:
        from preextract_task_storage import release_set,require_recoverable
        release_set(root,task_items)
        with ExitStack() as proofs:
            for scope in task_items.values():
                proof=proofs.enter_context(verified_for_cleanup(root,scope))
                cleanup=json.loads((scope.work/'cleanup-receipt.json').read_text(encoding='utf8'))
                if cleanup.get('status')!='complete' or cleanup.get('task_storage')!=proof:
                    raise ValueError('storage_complete_group_cleanup_not_confirmed')
            require_recoverable(root)
            return _release_selected(root,limit,stop,task_items=task_items)
    except Exception as exc:
        return [dict(status='waiting',reason=str(exc)[:180],released_files=0,released_bytes=0)]


def _release_selected(root,limit,stop,*,task_items):
    eligible=set(task_items)
    if not eligible:return []
    life=lifecycle(root,task_items=task_items)
    if life is None:return []
    # Admission only: release still rechecks NAS bytes, identity, input leases,
    # references and native locks immediately before each original is removed.
    with life.db() as db:
        ids=[r[0] for r in db.execute(
            "SELECT id FROM nas_jobs WHERE state IN ('verified','waiting') ORDER BY updated") if r[0] in eligible][:limit]
    result=[]
    for ident in ids:
        scope=task_items[ident]
        try:
            if scope.root!=Path(root).resolve():raise ValueError('storage_scope_workspace_differs')
            scope.assert_open()
            with scope.stage('cleanup'):
                result.append(life.release(ident,lambda job:reference_guard(root,job,stop,task_scope=scope)))
        except Exception as exc:
            result.append(dict(status='waiting',reason=str(exc)[:180],released_files=0,released_bytes=0))
    return result


def _reconcile_selected_uses(life,scope,captured):
    """Resolve only inactive leases with this exact saved item/run proof."""
    native=scope.control.assert_running(scope.batch_id,epoch=scope.epoch)
    bindings={row['run_id']:row for row in native['run_bindings']}
    ancestors=set();current=scope.run_id
    while current:
        if current in ancestors or current not in bindings:raise ValueError('storage_lease_run_lineage_unknown')
        ancestors.add(current);current=bindings[current].get('parent_run_id')
    proven={path_key(f['path']) for f in captured['files'] if not f.get('hold_reason')}
    def matches_saved(saved):
        if saved.get('work_dir')==str(scope.work) and saved.get('source_sha256')==scope.source:return True
        items=[item for item in saved.get('items',[]) if item.get('id')==scope.item_id
            and item.get('source_hash')==scope.source and item.get('preextract_work_dir')==str(scope.work)]
        return saved.get('run_id') in ancestors and len(items)==1
    # Replay only the target's actual durable exit receipt. An active use with
    # no matching receipt remains active, and all non-target paths stay protected.
    pending=[use['id'] for use in life.uses() if use['state']=='active' and set(use['paths'])&proven]
    if pending:
        life.reconcile_end_uses(use_ids=pending,authorize=lambda value:matches_saved(value['result']))
    for use in life.uses():
        if use['state']!='unresolved':continue
        saved=use['result']
        # Single preparation receipts have no invented run/item. Their exact
        # work/source is already bound to this exclusive native manifest item.
        if not matches_saved(saved):continue
        resolved=set(use['paths'])&proven
        if not resolved:continue
        remaining=sorted(set(use['paths'])-resolved)
        saved=copy.deepcopy(saved)
        saved['resolved_paths']=sorted(set(saved.get('resolved_paths',[]))|resolved)
        scope.assert_open()
        with life.db() as db:
            db.execute("UPDATE nas_uses SET state=?,paths=?,result=? WHERE id=? AND state='unresolved' AND json(paths)=json(?)",
                ('unresolved' if remaining else 'closed',json.dumps(remaining),json.dumps(saved),use['id'],json.dumps(use['paths'])))


@contextmanager
def verified_for_cleanup(root,scope):
    """Copy is earlier; cleanup needs actual complete, readable NAS bytes."""
    scope.assert_open()
    life=lifecycle(root,task_items={scope.id:scope})
    if life is None:raise ValueError('storage_nas_not_enabled')
    archived=life.get(scope.id)
    if not archived or archived['payload'].get('task_identity')!=scope.identity:
        raise ValueError('storage_nas_task_inventory_missing')
    with life.verified_files(scope.id,archived['payload']['identity']) as verified:
        scope.assert_open()
        from preextract_retirement import _gate
        current,_,bundle,plan=_gate(Path(root).resolve(),scope.job,verify_originals=False)
        saved=verified['payload']['native']
        if any(current.get(key)!=saved['record'].get(key) for key in
                ('identity','routes','targets','attachments','associated_plan_sha256')) or plan!=saved['plan']:
            raise ValueError('storage_required_rag_or_attachment_scope_changed')
        from preextract_cleanup_scope import sha
        if any(not Path(entry['path']).is_file() or sha(Path(entry['path']))!=entry['sha256']
               for entry in verified['payload']['source_evidence']):
            raise ValueError('storage_registered_material_scope_changed')
        yield dict(identity=copy.deepcopy(scope.identity),nas_manifest_sha256=verified['result']['manifest_sha256'])


def storage_progress(queue,identity,**changes):
    """Retain NAS selection in the existing whitelisted cleanup projection."""
    current=queue.get(identity)
    if not current or current['status']!='complete':return False
    value=copy.deepcopy(current['projection'].get('preextract_cleanup') or {})
    value.update(changes)
    return queue.update(identity,projection={'preextract_cleanup':value},expected_status='complete')


def register_task_storage(root,job,*,dispatch_scope,stop=None):
    """Existing Q completion tail: register this real paper, without NAS I/O.

    Caller supplies its original claimed job/scope after Q commits complete.
    Upload permission is not deletion permission. No copy/release switch is set.
    """
    from preextract_task_delivery import _readonly,_grant,_owners,_tables
    from preextract_delivery_queue import queue_path,DeliveryQueue
    from preextract_nas_scope import selected_task_items
    from preextract_retirement import _gate
    root=Path(root).resolve()
    try:
        # Retain the existing completion's selection even if a pause landed
        # after upload. This progress marker never grants a storage side effect.
        path=queue_path(root);payload=job['payload'];item=payload['item']
        with _readonly(path) as db:
            actual=db.execute('SELECT status,payload FROM deliveries WHERE id=?',(job['id'],)).fetchone()
            if not actual or actual['status']!='complete' or json.loads(actual['payload'])!=payload:
                raise ValueError('storage_queue_completion_not_committed')
            owners=_owners(db,_tables(db),payload['run_id'],item['source_hash'])
            if len(owners)!=1 or owners[0] is None:raise ValueError('storage_exact_task_reference_missing')
            batch=owners[0]
            grants=[_grant(db,path,key) for key in (dispatch_scope or {}).get('scope_ids',[])]
            if not any(grant and grant['mode']=='task' and grant['service']==payload['service']
                and any(ref['batch_id']==batch and ref['run_id']==payload['run_id'] and ref['source_hash']==item['source_hash']
                        for ref in grant['references']) for grant in grants):
                raise ValueError('storage_original_selected_task_grant_missing')
        selection=dict(schema='task-storage-selection/1',batch_id=batch,run_id=payload['run_id'],
            item_id=item['id'],source_hash=item['source_hash'],manifest_sha256=payload['manifest_sha256'])
        queue=DeliveryQueue(path)
        if not storage_progress(queue,job['id'],task_storage_request=dict(selection,status='pending')):
            return dict(status='held',reason='storage_queue_completion_changed')
        scopes,held=selected_task_items(root,batch_id=batch,delivery_ids=[job['id']],
                                       stop=stop,dispatch_scope=dispatch_scope)
        if job['id'] not in scopes:return dict(status='held',held=held)
        scope=scopes[job['id']]
        if not policy(root).get('enabled'):return dict(status='disabled')
        with FileLock(str(queue_path(root).with_name('cleanup.lock')),timeout=0),FileLock(str(scope.work/'operation.lock'),timeout=0):
            scope.assert_open()
            record,prepared,bundle,plan=_gate(root,scope.job)
            register_before_cleanup(root,scope.job,record,bundle,plan,task_scope=scope)
        storage_progress(queue,job['id'],task_storage_request=dict(selection,status='registered'))
        return dict(status='registered',delivery_id=scope.id,identity=scope.identity)
    except Exception as exc:
        return dict(status='held',reason=str(exc)[:180],error_type=type(exc).__name__)


def background_task_items(root,stop=None):
    from preextract_nas_scope import registered_items
    return registered_items(root,stop=stop)


def restore_task_items(root,selection,stop=None):
    from preextract_nas_scope import restore_items
    return restore_items(root,selection,stop=stop)


def submitted_task_selections(root):
    from preextract_task_storage import submissions
    return submissions(root)


def cleanup_once(root,*,task_items,stop=None):
    from preextract_cleanup import cleanup_queue_once
    return cleanup_queue_once(root,stop=stop,limit=max(1,len(task_items)),
                              delivery_ids=list(task_items),storage_items=task_items)


def copy_once(root,limit=1,stop=None,*,task_items=None):
    """Register/copy selected complete papers without enabling cleanup policy."""
    if not action_enabled(root,'copy'):return dict(state='disabled',held={})
    from preextract_nas_scope import registered_items
    from preextract_delivery_queue import queue_path
    from preextract_retirement import _gate
    if task_items is None:task_items,held=registered_items(root,stop=stop)
    else:held={}
    if not task_items:return dict(state='held',held=held)
    life=lifecycle(root,task_items=task_items)
    if life is None:return dict(state='disabled',held=held)
    ready={}
    for ident,scope in task_items.items():
        if len(ready)>=limit:break
        try:
            scope.assert_open()
            old=life.get(ident)
            if old:
                if old['payload'].get('task_identity')!=scope.identity:
                    raise ValueError('storage_registered_task_identity_missing_or_changed')
                if old['state'] in {'verified','waiting','released'}:continue
                if old['state'] not in {'queued','retry'} or old['next_attempt']>time.time():continue
            # Registration is local inventory only; NAS I/O follows after the
            # native cleanup/operation locks are released.
            with FileLock(str(queue_path(root).with_name('cleanup.lock')),timeout=0), FileLock(str(scope.work/'operation.lock'),timeout=0):
                scope.assert_open()
                record,prepared,bundle,plan=_gate(Path(root).resolve(),scope.job)
                register_before_cleanup(root,scope.job,record,bundle,plan,task_scope=scope)
            ready[ident]=scope
        except Exception as exc:held[ident]=str(exc)[:180]
    if ready:life.copy_once(limit=limit,task_items=ready)
    return dict(state='checked',selected=len(task_items),held=held)


def _wake_archive_service(root,*,task_items=None):
    """Reuse the storage-only launcher with this exact local data root."""
    for base in Path(__file__).resolve().parents:
        script=base/'scripts/original_archive_worker.py'
        if script.is_file() and (base/'config/paths.json').is_file():
            spec=importlib.util.spec_from_file_location('_paperflow_archive_service_launcher',script)
            module=importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            from preextract_nas_scope import describe_items
            return module.ensure_started(base,data_root=Path(root),
                task_selection=describe_items(task_items) if task_items is not None else None)
    raise RuntimeError('未找到原生NAS后台入口；待办保留')


_tails={};_tail_lock=threading.Lock()


def kick(root,stop=None,*,task_items=None):
    """Coalesce asynchronous wakes; never execute NAS I/O on a body worker."""
    root=str(Path(root).resolve())
    from global_task_control import GlobalTaskControl
    if not GlobalTaskControl(root).snapshot()['admission_open']:return dict(status='held',reason='storage_global_admission_closed')
    if stop is not None and stop.is_set():return dict(status='held',reason='storage_stopped')
    value=policy(root)
    if not value.get('enabled') or not (value.get('copy_enabled',True) or value.get('release_enabled',True)):
        return dict(status='held',reason='storage_policy_disabled')
    if task_items is not None:
        from preextract_task_storage import submit
        submit(root,task_items)
        # The existing worker reads these exact parameters from the same
        # original command. An already running ordinary worker can consume them
        # without replacing its owner or opening the whole active scope.
        result=_wake_archive_service(root)
        return dict(status=result['state'],reason=result.get('reason',''))
    with _tail_lock:
        current=_tails.get(root)
        if current is not None:
            current['pending']=True
            if stop is not None:current['stop']=stop
            if task_items is not None:current['task_items']=task_items
            return
        current=dict(pending=False,stop=stop,task_items=task_items)
        _tails[root]=current
    def run():
        while True:
            try:
                current_stop=current['stop']
                if current_stop is None or not current_stop.is_set():
                    value=policy(root)
                    if value.get('enabled') and (value.get('copy_enabled',True) or value.get('release_enabled',True)):
                        result=_wake_archive_service(root,task_items=current['task_items'])
                        if result.get('state')=='held':
                            _log.warning('NAS selected wake held: reason=%s',result.get('reason'))
            except Exception as exc:
                _log.warning('NAS service wakeup deferred: error_type=%s',type(exc).__name__)
            with _tail_lock:
                if current['pending']:
                    current['pending']=False
                    continue
                _tails.pop(root,None)
                return
    threading.Thread(target=run,name='originals-nas-wakeup',daemon=True).start()

"""Local cleanup tail: verified uploads retain a table row, not local products.

Unuploaded work and originals stay protected by the existing native locks.
"""
from __future__ import annotations

from contextlib import closing, nullcontext
from concurrent.futures import ThreadPoolExecutor, as_completed
import copy
import json
import math
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import threading
import time

if __name__=='__main__':
    for project in Path(__file__).resolve().parents:
        if (project/'config/paths.json').is_file():
            selected=os.environ.get('PAPERFLOW_SELECTED_CORE')
            core=Path(selected).resolve() if selected else project/'core'
            if selected and core.parent!=Path(__file__).resolve().parent.parent:
                raise RuntimeError('后台清理与选中控制内核版本不一致，保持停止')
            sys.path[:0]=[str(Path(__file__).resolve().parent),str(project/'desktop'),str(core)]
            break
    if '--inspect-source' in sys.argv:
        import preextract_scope_guard
        print(json.dumps(dict(worker=__file__,scope_guard=preextract_scope_guard.__file__)))
        raise SystemExit(0)

from filelock import FileLock, Timeout

from engine import write_json
from preextract_delivery_queue import queue_path

SCHEMA = 'preextract-cleanup/3'


def _load(path):
    return json.loads(Path(path).read_text(encoding='utf8'))


def _sha(path):
    from preextract_material import file_sha
    return file_sha(Path(path))


def policy(root):
    path = queue_path(root).with_name('cleanup-policy.json')
    return _load(path) if path.is_file() else {'enabled': False}


def legacy_processes_alive(root):
    import psutil
    owners=policy(root).get('wait_for_processes', [])
    if not isinstance(owners,list):
        return True
    for owner in owners:
        try:
            if (not isinstance(owner,dict) or type(owner.get('pid')) is not int or owner['pid']<=0
                    or not isinstance(owner.get('create_time'),(int,float))
                    or not math.isfinite(owner['create_time']) or owner['create_time']<=0):
                return True
            process = psutil.Process(owner['pid'])
            if process.is_running() and abs(process.create_time()-owner['create_time']) < .001:
                return True
        except psutil.NoSuchProcess:
            continue
        except (psutil.Error, KeyError, TypeError, ValueError):
            return True  # Unknown is never evidence that old code has exited.
    return False


def _safe_path(path, root):
    path = Path(path).absolute()
    for part in (path, *path.parents):
        if part.is_symlink() or part.is_junction():
            raise ValueError('cleanup links forbidden')
    from paperflow.private_files import resolve_private_path
    resolved = resolve_private_path(path)
    root = resolve_private_path(root)
    if resolved == root or not resolved.is_relative_to(root):
        raise ValueError('cleanup outside original application data')
    return resolved


def _references(value):
    """Conservatively preserve absolute local paths in immutable evidence."""
    values=set()
    def collect(value):
        if isinstance(value,dict):
            for key,child in value.items():collect(key);collect(child)
        elif isinstance(value,list):
            for child in value:collect(child)
        elif isinstance(value,str) and (value.startswith(('\\\\','/')) or len(value)>2 and value[1]==':' and value[2] in '/\\'):
            values.add(value)
    collect(value)
    for value in values:
        try:
            path=Path(value)
            if not path.is_absolute():continue
            from paperflow.private_files import resolve_private_path
            yield Path(os.path.normpath(value)) if value.startswith('\\\\') and not value.startswith('\\\\?\\') else resolve_private_path(path)
        except (OSError,ValueError):
            pass


def _summary(report):
    return {key: report[key] for key in ('schema', 'status', 'removed_files', 'removed_bytes',
        'completed_at', 'reason', 'error_type', 'rules_version', 'eligible_residual_files', 'purpose_cache') if key in report}


def _incomplete_reference(root, work):
    path = queue_path(root)
    if not path.is_file():
        return False
    with closing(sqlite3.connect(path.as_uri()+'?mode=ro', uri=True, timeout=5)) as db:
        rows = db.execute("SELECT json_extract(payload,'$.item.preextract_work_dir') "
                          "FROM deliveries WHERE status!='complete'")
        return any(value and Path(value).resolve() == work for (value,) in rows)


def _mark_receipt(work, report, identity):
    receipt = _load(work/'delivery-receipt.json')
    if receipt.get('status') != 'complete' or any(receipt.get(k) != v for k, v in identity.items()):
        raise ValueError('delivery identity changed before cleanup receipt')
    receipt.update(cleanup='complete' if report['status'] == 'complete' else 'pending',
                   cleanup_receipt=str(work/'cleanup-receipt.json'), cleanup_result=_summary(report))
    write_json(work/'delivery-receipt.json', receipt)


def _cleanup_locked(root, work, item, stop, admit, *, defer_purpose=False, storage_scope=None, storage_proof=None):
    from preextract_publish import _paperflow
    from preextract_handoff import _receipt_complete

    if item.get('status') != 'pass' or not item.get('automatic_approval'):
        return dict(status='deferred', reason='paper_not_approved')
    if _incomplete_reference(root, work):
        return dict(status='deferred', reason='another_delivery_still_uses_workspace')
    prepared = _load(work/'private/prepared-publication.json')
    published = _safe_path(item['package_path'], root)
    packet = _paperflow(prepared['paperflow_root']).validate_package(
        published, prepared['task'], prepared['catalog'], strict_seal=True)
    identity = dict(task=prepared['task'], manifest_sha256=packet['manifest_sha256'])
    receipt = _load(work/'delivery-receipt.json')
    binding = _load(work/'delivery-binding.json')
    if (not _receipt_complete(receipt, packet) or receipt.get('stage') != 'verified'
            or set(receipt.get('targets', {})) != {r['code'] for r in packet['routes']}
            or any(receipt.get(k) != v or binding.get(k) != v for k, v in identity.items())
            or binding.get('routes') != packet['routes']
            or prepared['validation']['manifest_sha256'] != packet['manifest_sha256']):
        return dict(status='deferred', reason='all_specialty_readback_not_confirmed')
    if prepared.get('associated_upload_plan_sha256'):
        from associated_delivery import load_plan,receipt_summary
        prepared['packet']=packet
        plan=load_plan(prepared,work)
        associated=receipt_summary(work)
        if plan['files'] and (not associated or associated['status']!='complete'
                or associated['plan_sha256']!=prepared['associated_upload_plan_sha256']
                or associated['service']!=binding.get('service')
                or associated['verified_count']!=len(plan['files'])):
            return dict(status='deferred',reason='associated_originals_not_confirmed')
        from purpose_cache_cleanup import verify_associated_receipt
        try:
            verify_associated_receipt(prepared, work, dict(receipt, service=binding['service']), plan)
        except (OSError, ValueError, KeyError, TypeError):
            return dict(status='deferred', reason='associated_exact_readback_not_confirmed')
    delivery = item.get('preextract_delivery') or {}
    if delivery and (delivery.get('service') != binding.get('service')
                     or delivery.get('manifest_sha256') != packet['manifest_sha256']):
        return dict(status='deferred', reason='delivery_service_or_version_changed')
    bundle = _load(work/'material.json')
    candidate = _load(work/'private/candidate.json')
    if (item.get('source_hash') != prepared['task']['source_sha256']
            or bundle['source']['source_sha256'] != prepared['task']['source_sha256']
            or prepared.get('candidate_sha256') != _sha(work/'private/candidate.json')):
        return dict(status='deferred', reason='retained_learning_identity_changed')
    checkpoint = item.get('prepared_checkpoint') or {}
    if checkpoint and (_sha(work/'material.json') != checkpoint.get('sha256')
                       or Path(checkpoint['path']).resolve() != work/'material.json'):
        return dict(status='deferred', reason='material_checkpoint_changed')
    for path, digest in bundle.get('file_hashes', {}).items():
        if not Path(path).is_file() or _sha(path) != digest:
            return dict(status='deferred', reason='retained_material_dependency_changed')
    from preextract_cleanup_scope import finish_cleanup
    report = finish_cleanup(root, work, item, prepared, published, bundle, candidate, identity, stop, admit,
                            storage_scope=storage_scope,storage_proof=storage_proof)
    if report.get('status') == 'complete':
        if storage_scope is not None:
            # Shared purpose/model caches are not owned by this item. Retain
            # them rather than expanding this task's disposable-file inventory.
            report['shared_cache']='retained_outside_item_scope'
            write_json(work/'cleanup-receipt.json',report)
            _mark_receipt(work,report,identity)
            return report
        if defer_purpose:
            from purpose_review import production_settings
            if production_settings(root).get('enabled'):
                # All parallel family locks must be released before shared
                # consumers can lock each other. The serial pass revalidates
                # the full original admission; this is not a deletion permit.
                report.update(status='pending', reason='shared_cleanup_pending')
                write_json(work/'cleanup-receipt.json', report)
                _mark_receipt(work, report, identity)
                return report
        from purpose_cache_cleanup import cleanup_purpose_cache
        purpose = cleanup_purpose_cache(root, work, item, prepared, bundle, candidate, stop=stop, admit=admit)
        report['purpose_cache'] = purpose
        if purpose['status'] not in {'complete', 'not_applicable'}:
            report.update(status='pending', reason=purpose.get('reason', 'purpose_cleanup_deferred'))
        write_json(work/'cleanup-receipt.json', report)
        _mark_receipt(work, report, identity)
    return report


def cleanup_item(root, run_id, item, *, already_locked=False, stop=None, admit=None):
    """Never block a live paper or change a remote delivery's success status."""
    from global_task_control import GlobalTaskControl
    if not GlobalTaskControl(root).snapshot()['admission_open']:
        return dict(status='deferred',reason='global_paused')
    # The uploader must commit complete before a durable record can replace
    # its local receipt. The existing queue tail performs full retirement.
    return dict(status='deferred', reason='awaiting_durable_delivery') if policy(root).get('enabled') else dict(status='disabled')

def _legacy_cleanup_item(root, run_id, item, *, already_locked=False, stop=None, admit=None,
                  _batch_lock=None, _defer_purpose=False, storage_scope=None, storage_proof=None):
    from global_task_control import GlobalTaskControl,GlobalPaused
    try:
        with GlobalTaskControl(root).stage('cleanup',batch_id=run_id,paper_id=item.get('id',''),name=item.get('title','')):
            return _legacy_cleanup_admitted(root,run_id,item,already_locked=already_locked,stop=stop,admit=admit,
                _batch_lock=_batch_lock,_defer_purpose=_defer_purpose,
                storage_scope=storage_scope,storage_proof=storage_proof)
    except GlobalPaused:return dict(status='deferred',reason='global_paused')


def _legacy_cleanup_admitted(root, run_id, item, *, already_locked=False, stop=None, admit=None,
                  _batch_lock=None, _defer_purpose=False, storage_scope=None, storage_proof=None):
    """Never block a live paper or change a remote delivery's success status."""
    root = Path(root).resolve()
    if storage_scope is None or not storage_proof:
        return dict(status='deferred',reason='storage_exact_task_or_nas_proof_missing')
    if not policy(root).get('enabled'):
        return dict(status='disabled')
    if legacy_processes_alive(root):
        return dict(status='deferred', reason='waiting_for_legacy_process_exit')
    stop = stop or threading.Event()
    if stop.is_set():
        return dict(status='deferred', reason='stopped')
    try:
        work = _safe_path(item['preextract_work_dir'], root)
        from preextract_scope_guard import cleanup_admission,save_tombstone
        if policy(root).get('preserve_results',True):
            from preextract_scope_guard import binding,retirement_scope_proof,retirement_admission
            marker=binding(work)
            if marker:
                retirement_scope_proof(work,_load(work/'delivery-receipt.json'))
            scope_admit=retirement_admission(marker)
        else:
            scope_admit=cleanup_admission(work)
        previous_admit=admit
        if scope_admit is not None:
            admit=lambda:scope_admit() and (previous_admit is None or previous_admit())
            if not admit():return dict(status='deferred',reason='scope_delivery_not_complete')
        if not work.is_dir():
            return dict(status='deferred', reason='workspace_missing')
        global_path = queue_path(root).with_name('cleanup.lock')
        # The one native worker owns the legacy global gate while its bounded
        # pool handles independent families. Old uploaders still defer here.
        if _batch_lock is not None and (not _batch_lock.is_locked
                or Path(_batch_lock.lock_file).resolve() != global_path):
            raise ValueError('cleanup batch compatibility lock is not held')
        global_scope = nullcontext() if _batch_lock is not None else FileLock(str(global_path), timeout=0)
        with global_scope:
            scope = nullcontext() if already_locked else FileLock(str(work/'operation.lock'), timeout=0)
            with scope:
                if admit is not None and not admit():
                    return dict(status='deferred', reason='delivery_not_complete')
                report=_cleanup_locked(root, work, item, stop, admit, defer_purpose=_defer_purpose,
                    storage_scope=storage_scope,storage_proof=storage_proof)
                if not policy(root).get('preserve_results',True):
                    save_tombstone(work,report)
                return report
    except Timeout:
        return dict(status='deferred', reason='paper_or_cleanup_busy')
    except Exception as exc:
        return dict(status='error', error_type=type(exc).__name__)


def cleanup_queue_once(root, *, stop=None, limit=16, force=False, delivery_ids=None,storage_items=None):
    """Read only complete jobs from the same queue; no model/RAG client exists."""
    from global_task_control import GlobalTaskControl
    gate=GlobalTaskControl(root)
    if not gate.snapshot()['admission_open'] and not gate.inherited('cleanup'):
        return dict(completed=0,deferred=0,global_paused=True)
    from preextract_nas_scope import selected_items,registered_items
    scopes,held=((storage_items,{}) if storage_items is not None else
                 registered_items(root,stop=stop) if delivery_ids is None
                 else selected_items(root,delivery_ids=delivery_ids,stop=stop))
    if not scopes:return dict(completed=0,deferred=0,held=held)
    delivery_ids=list(scopes)
    if policy(root).get('preserve_results',True):
        result=_legacy_cleanup_queue_once(root,stop=stop,limit=limit,force=force,delivery_ids=delivery_ids,
                                          storage_items=scopes)
        result['held']=held
        return result
    from preextract_retirement import cleanup_queue
    try:
        return cleanup_queue(root,stop=stop,limit=limit,force=force,delivery_ids=delivery_ids)
    finally:
        if stop is None or not stop.is_set():
            from preextract_nas import kick
            kick(root,stop=stop)  # All native cleanup locks have now been released.

def _legacy_cleanup_queue_once(root, *, stop=None, limit=16, force=False, delivery_ids=None,storage_items=None):
    """Read only complete jobs from the same queue; no model/RAG client exists."""
    root = Path(root).resolve()
    if storage_items is None:
        from preextract_nas_scope import selected_items,registered_items
        storage_items,_=(registered_items(root,stop=stop) if delivery_ids is None
                         else selected_items(root,delivery_ids=delivery_ids,stop=stop))
    delivery_ids=list(storage_items)
    if not delivery_ids:return dict(completed=0,deferred=0)
    if not policy(root).get('enabled'):
        return dict(completed=0, deferred=0)
    if legacy_processes_alive(root):
        return dict(completed=0, deferred=0, waiting_for_legacy=True)
    path = queue_path(root)
    if not path.is_file():
        return dict(completed=0, deferred=0)
    with closing(sqlite3.connect(path.as_uri()+'?mode=ro', uri=True, timeout=5)) as db:
        db.row_factory = sqlite3.Row
        query=("SELECT id,payload,projection FROM deliveries WHERE status='complete' "
               "AND COALESCE(json_extract(payload,'$.kind'),'')!='associated_original' ")
        args=[]
        if delivery_ids is not None:
            if not delivery_ids: return dict(completed=0,deferred=0)
            query+='AND id IN ('+','.join('?' for _ in delivery_ids)+') '
            args+=list(delivery_ids)
        if not force:
            query+=("AND (COALESCE(json_extract(projection,'$.preextract_delivery.cleanup'),'')!='complete' "
                "OR COALESCE(json_extract(projection,'$.preextract_cleanup.schema'),'')!='preextract-cleanup/3') "
                "AND COALESCE(json_extract(projection,'$.preextract_cleanup.next_check_at'),0)<=? ")
            args.append(time.time())
        # First admission of a verified paper must not sit behind thousands of
        # already diagnosed cleanup exceptions. Those retain their own backoff.
        rows=db.execute(query+"ORDER BY CASE WHEN json_extract(projection,'$.preextract_cleanup.checked_at') IS NULL "
                        "THEN 0 ELSE 1 END, updated_at LIMIT ?",(*args,limit)).fetchall()
    from preextract_delivery_queue import DeliveryQueue
    queue = DeliveryQueue(path)
    result = dict(completed=0, deferred=0)
    if not rows or stop is not None and stop.is_set():
        return result
    configured = policy(root).get('cleanup_workers', 2)
    workers = min(4, max(1, configured)) if type(configured) is int else 2
    families = {}
    for row in rows:
        try:
            value = json.loads(row['payload'])
            if not isinstance(value, dict) or not isinstance(value.get('task', {}), dict) or not isinstance(value.get('item', {}), dict):
                raise ValueError('malformed cleanup family')
            source = value.get('task', {}).get('source_sha256') or value.get('item', {}).get('source_hash')
            if not isinstance(source, str):
                source = None
        except (ValueError, KeyError, TypeError):
            source = None
        # Same bytes can have separate provenance; grouping only serializes
        # them conservatively. It does not merge their identities or receipts.
        families.setdefault(('source', source) if source else ('row', row['id']), []).append(row)

    def clean_row(row, batch_lock, *, defer_purpose=False):
        from global_task_control import GlobalTaskControl,GlobalPaused
        payload=json.loads(row['payload']);item=payload['item']
        try:
            with GlobalTaskControl(root).stage('cleanup',batch_id=payload['run_id'],paper_id=item.get('id',row['id'])):
                return clean_row_admitted(row,batch_lock,defer_purpose=defer_purpose)
        except GlobalPaused:return 'deferred'

    def clean_row_admitted(row, batch_lock, *, defer_purpose=False):
        if stop is not None and stop.is_set() or not policy(root).get('enabled'):
            return 'deferred'
        payload = json.loads(row['payload'])
        item = dict(payload['item'], **json.loads(row['projection']))
        def admit():
            if stop is not None and stop.is_set() or not policy(root).get('enabled'):
                return False
            current = queue.get(row['id'])
            return (current['status'] == 'complete' and current['payload']['manifest_sha256'] == payload['manifest_sha256']
                    and storage_items[row['id']].assert_open())
        try:
            work = _safe_path(item['preextract_work_dir'], root)
            from preextract_nas import verified_for_cleanup
            with verified_for_cleanup(root,storage_items[row['id']]) as proof, FileLock(str(work/'operation.lock'), timeout=0):
                report = _legacy_cleanup_item(root, payload['run_id'], item, already_locked=True, stop=stop,
                                      admit=admit, _batch_lock=batch_lock, _defer_purpose=defer_purpose,
                                      storage_scope=storage_items[row['id']],storage_proof=proof)
                progress=copy.deepcopy(queue.get(row['id'])['projection'].get('preextract_cleanup') or {})
                progress.update(_summary(report))
                projection = dict(preextract_cleanup=dict(progress, checked_at=time.time(),
                    next_check_at=time.time()+60))
                if report['status'] in {'complete', 'pending'}:
                    receipt = _load(work/'delivery-receipt.json')
                    delivery = copy.deepcopy(queue.get(row['id'])['projection'].get('preextract_delivery', {}))
                    delivery.update(cleanup=receipt['cleanup'], cleanup_receipt=receipt['cleanup_receipt'],
                                    cleanup_result=receipt['cleanup_result'])
                    projection['preextract_delivery'] = delivery
                saved = queue.update(row['id'], projection=projection, expected_status='complete')
                if saved and report.get('reason') == 'shared_cleanup_pending':
                    return 'shared_pending'
                return 'completed' if saved and report['status']=='complete' else 'deferred'
        except (Timeout, OSError, ValueError):
            from preextract_nas import storage_progress
            storage_progress(queue,row['id'],status='deferred',reason='paper_or_file_busy',
                             checked_at=time.time(),next_check_at=time.time()+60)
            return 'deferred'

    def clean_family(group, batch_lock, *, defer_purpose=False):
        counts = dict(completed=0, deferred=0)
        for row in group:
            try:
                outcome = clean_row(row, batch_lock, defer_purpose=defer_purpose)
            except Exception as exc:
                outcome = 'deferred'
                try:
                    from preextract_nas import storage_progress
                    storage_progress(queue,row['id'],status='error',reason='cleanup_item_error',
                        error_type=type(exc).__name__,checked_at=time.time(),next_check_at=time.time()+60)
                except Exception as save_error:
                    counts.setdefault('errors', []).append(dict(id=row['id'], error_type=type(save_error).__name__))
            if outcome == 'shared_pending':
                counts.setdefault('shared_pending', []).append(row)
            else:
                counts[outcome] += 1
        return counts
    try:
        # Thread-shared ownership is checked by cleanup_item; the coordinator
        # keeps the OS lock until every admitted task has released its locks.
        with FileLock(str(path.with_name('cleanup.lock')), timeout=0, thread_local=False) as batch_lock:
            shared_pending = []
            with ThreadPoolExecutor(max_workers=workers, thread_name_prefix='verified-cleanup') as pool:
                futures = [pool.submit(clean_family, group, batch_lock, defer_purpose=True) for group in families.values()]
                for future in as_completed(futures):
                    counts = future.result()
                    for key in ('completed', 'deferred'):
                        result[key] += counts[key]
                    if counts.get('errors'):
                        result.setdefault('errors', []).extend(counts['errors'])
                    shared_pending.extend(counts.get('shared_pending', []))
            # No other parallel cleanup owns an operation.lock at this point.
            # Shared purpose/model resources remain on their native serial path.
            counts = clean_family(shared_pending, batch_lock)
            for key in ('completed', 'deferred'):
                result[key] += counts[key]
            if counts.get('errors'):
                result.setdefault('errors', []).extend(counts['errors'])
    except Timeout:
        result['deferred'] += len(rows)
    return result


def cleanup_batch_once(root, run_id, *, stop=None):
    """Recheck a closed producer and its complete reference set, exactly once."""
    import hashlib
    from preextract_cleanup_scope import RULES
    root=Path(root).resolve()
    if not policy(root).get('enabled'): return dict(status='disabled')
    if legacy_processes_alive(root): return dict(status='deferred',reason='waiting_for_legacy_process_exit')
    directory=_safe_path(root/'runs'/run_id,root)
    manifest=_load(directory/'manifest.json')
    if manifest.get('run_id')!=run_id or manifest.get('status')!='completed' or not manifest.get('finished_at'):
        return dict(status='deferred',reason='producer_not_finished')
    expected={item['id']:item for item in manifest.get('items',[]) if item.get('status')=='pass'
        and item.get('automatic_approval') and item.get('content_format')=='paper_preextract'}
    with closing(sqlite3.connect(queue_path(root).as_uri()+'?mode=ro',uri=True,timeout=5)) as db:
        db.row_factory=sqlite3.Row
        rows=db.execute('''SELECT d.id,d.status,d.payload,r.item_id,r.package_path,r.source_hash
            FROM references_to_deliveries r JOIN deliveries d ON d.id=r.delivery_id WHERE r.run_id=?
            AND COALESCE(json_extract(d.payload,'$.kind'),'')!='associated_original' ''',(run_id,)).fetchall()
    by_item={row['item_id']:row for row in rows}
    if not expected or not set(expected)<=set(by_item):
        return dict(status='deferred',reason='batch_registration_not_complete')
    for item_id,item in expected.items():
        row=by_item[item_id]
        if (Path(row['package_path']).resolve()!=Path(item['package_path']).resolve()
                or row['source_hash']!=item['source_hash']):
            return dict(status='deferred',reason='batch_reference_changed')
    if any(row['status']!='complete' for row in rows):
        return dict(status='deferred',reason='batch_delivery_not_complete')
    frozen=[(r['id'],r['item_id'],r['package_path'],r['source_hash'],json.loads(r['payload'])['manifest_sha256']) for r in rows]
    from purpose_cache_cleanup import batch_scope_stamp
    def current_fingerprint():
        scope = batch_scope_stamp(root, [json.loads(r['payload']) for r in rows])
        return hashlib.sha256(json.dumps([RULES,'purpose-cache-cleanup/1',manifest['finished_at'],sorted(frozen),scope],sort_keys=True).encode()).hexdigest()
    fingerprint=current_fingerprint()
    marker=directory/'cleanup-batch-receipt.json'
    if marker.is_file():
        previous=_load(marker)
        if previous.get('status')=='complete' and previous.get('fingerprint')==fingerprint:
            return dict(status='already_complete',checked=previous['checked'])
    ids=sorted({row['id'] for row in rows}); result=dict(completed=0,deferred=0)
    for offset in range(0,len(ids),16):
        if stop is not None and stop.is_set(): return dict(status='deferred',reason='stopped')
        part=cleanup_queue_once(root,stop=stop,limit=16,force=True,delivery_ids=ids[offset:offset+16])
        for key in result: result[key]+=part[key]
    report=dict(schema='preextract-batch-cleanup/1',rules_version=RULES,run_id=run_id,
        status='complete' if result['completed']==len(ids) and not result['deferred'] else 'pending',
        fingerprint=current_fingerprint(),checked=len(ids),completed_at=time.time(),**result)
    write_json(marker,report)
    return report


def cleanup_batches_once(root, *, stop=None):
    # Pending rows already retain the exact whole-product plan and retry through
    # the same queue. A batch must not recreate per-workspace JSON receipts.
    return dict(batches_checked=0)

def _legacy_cleanup_batches_once(root, *, stop=None):
    path=queue_path(root)
    if not path.is_file(): return dict(checked=0)
    with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=5)) as db:
        runs=[row[0] for row in db.execute('SELECT DISTINCT run_id FROM references_to_deliveries ORDER BY run_id DESC')]
    for run_id in runs:
        if stop is not None and stop.is_set(): break
        try:
            result=cleanup_batch_once(root,run_id,stop=stop)
        except (OSError,ValueError,KeyError):
            continue
        if result.get('status') in {'complete','pending'}: return dict(result,batches_checked=1)
    return dict(batches_checked=0)


def ensure_cleanup_worker(root):
    """One lightweight local tail for the existing queue, including old executors."""
    from global_task_control import GlobalTaskControl
    if not GlobalTaskControl(root).snapshot()['admission_open']:return False
    # Managed workers already have a finite cleanup job in PreparedAdapter.
    # A permanent child would keep their Windows Job alive after delivery ends.
    if os.environ.get('PAPERFLOW_EXECUTION'):
        return False
    root = Path(root).resolve()
    if not policy(root).get('enabled') or not queue_path(root).is_file():
        return False
    path = queue_path(root).with_name('cleanup.worker.lock')
    try:
        with FileLock(str(path)+'.launcher', timeout=0):
            guard = FileLock(str(path), timeout=0)
            guard.acquire();guard.release()
            with path.with_name('cleanup.stderr.log').open('ab') as err:
                subprocess.Popen([sys.executable, '-B', str(Path(__file__).resolve()), '--data-root', str(root)],
                    cwd=str(Path(__file__).resolve().parent), stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL, stderr=err,
                    creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0), close_fds=True)
            return True
    except Timeout:
        return False


def main():
    import argparse
    import signal
    import psutil
    parser = argparse.ArgumentParser()
    parser.add_argument('--data-root', type=Path, required=True)
    parser.add_argument('--handover-file',type=Path)
    parser.add_argument('--initial-delivery',action='append',default=[])
    args = parser.parse_args()
    path = queue_path(args.data_root).with_name('cleanup.worker.lock')
    stop=threading.Event()
    for name in ('SIGINT','SIGTERM','SIGBREAK'):
        if hasattr(signal,name):signal.signal(getattr(signal,name),lambda *_:stop.set())
    control=path.with_name('cleanup-stop.json')
    created=psutil.Process().create_time()
    def stopping():
        if stop.is_set():return True
        try:
            request=json.loads(control.read_text(encoding='utf8'))
            if request.get('pid')==os.getpid() and abs(request.get('create_time',0)-created)<.01:
                stop.set()
        except (OSError,ValueError,TypeError):pass
        return stop.is_set()
    version=Path(__file__).resolve().parent.parent.name
    def publish(state,**fields):
        write_json(path.with_name('cleanup-status.json'),dict(observed_at=time.time(),pid=os.getpid(),
            create_time=created,version=version,state=state,**fields))
    try:
        with FileLock(str(path), timeout=0):
            publish('ready')
            def watch_control():
                while not stop.wait(1):
                    if stopping():return
            threading.Thread(target=watch_control,name='cleanup-control',daemon=True).start()
            initial=args.initial_delivery
            next_run=0
            while policy(args.data_root).get('enabled') and not stopping():
                from global_task_control import GlobalTaskControl
                if not GlobalTaskControl(args.data_root).snapshot()['admission_open']:
                    publish('global_paused');break
                if args.handover_file is not None:
                    try:activated=json.loads(args.handover_file.read_text(encoding='utf8')).get('enabled') is True
                    except (OSError,ValueError,AttributeError):activated=False
                    if not activated:
                        stop.wait(1)
                        continue
                if time.monotonic()<next_run:
                    stop.wait(min(1,next_run-time.monotonic()))
                    continue
                try:
                    configured=policy(args.data_root)
                    interval=configured.get('interval_seconds',1800)
                    interval=max(10,int(interval))
                    limit=max(1,min(512,int(configured.get('batch_limit',256))))
                    publish('cleaning')
                    if initial:
                        result=cleanup_queue_once(args.data_root,stop=stop,limit=len(initial),force=True,delivery_ids=initial)
                    else:
                        result = cleanup_queue_once(args.data_root,stop=stop,limit=limit)
                    result['batch_review']=dict(batches_checked=0,reason='exact_task_items_only')
                    publish('idle',next_check_at=time.time()+interval,**result)
                except Exception as exc:
                    interval=60
                    publish('retry',next_check_at=time.time()+interval,error_type=type(exc).__name__)
                next_run=time.monotonic()+interval
            publish('stopped')
    except Timeout:
        pass


if __name__ == '__main__':
    main()

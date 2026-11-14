"""Verified-delivery cleanup in the existing workspaces and delivery queue.

Only disposable render directories and redundant complete stream logs qualify.
Originals, material/checkpoint dependencies, complete learning and seals stay put.
"""
from __future__ import annotations

from contextlib import closing, nullcontext
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

from filelock import FileLock, Timeout

from engine import write_json
from preextract_delivery_queue import queue_path

SCHEMA = 'preextract-cleanup/1'


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
    resolved = path.resolve()
    if resolved == root or not resolved.is_relative_to(root):
        raise ValueError('cleanup outside original application data')
    return resolved


def _references(value):
    """Conservatively preserve absolute local paths in immutable evidence."""
    if isinstance(value, dict):
        for key, child in value.items():
            yield from _references(key)
            yield from _references(child)
    elif isinstance(value, list):
        for child in value:
            yield from _references(child)
    elif isinstance(value, str):
        try:
            path = Path(value)
            if path.is_absolute():
                yield path.resolve()
        except (OSError, ValueError):
            pass


def _summary(report):
    return {key: report[key] for key in ('schema', 'status', 'removed_files', 'removed_bytes',
        'completed_at', 'reason', 'error_type') if key in report}


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


def _cleanup_locked(root, work, item, stop, admit):
    from preextract_publish import _paperflow
    from preextract_handoff import _receipt_complete

    if item.get('status') != 'pass' or not item.get('automatic_approval'):
        return dict(status='deferred', reason='paper_not_approved')
    if _incomplete_reference(root, work):
        return dict(status='deferred', reason='another_delivery_still_uses_workspace')
    if any((work/name).exists() for name in ('revision.json', 'revisions', 'model/partition-state.json')):
        return dict(status='deferred', reason='revision_or_partition_evidence_retained')
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
    originals = [Path(bundle['source']['file']).resolve()]
    copies = list((work/'private').glob('original.*'))
    if not copies or not any(_sha(p) == bundle['source']['source_sha256'] for p in copies):
        return dict(status='deferred', reason='original_copy_not_verified')
    originals += copies
    state = _load(work/'model/request-state.json')
    if state.get('status') != 'response_complete' or state.get('response_sha256') != _sha(work/'model/response.txt'):
        return dict(status='deferred', reason='complete_model_answer_not_verified')
    from paperflow.cleanup_paths import inventory, delete_inventory
    # Inventory only candidate directories. Windows deliberately denies reads
    # of the operation.lock held by this caller, so never inventory the work root.
    protected = set(_references(bundle)) | set(_references(candidate))
    report_path = work/'cleanup-receipt.json'
    preserved = dict(material_sha256=_sha(work/'material.json'), candidate_sha256=_sha(work/'private/candidate.json'),
                     response_sha256=state['response_sha256'])
    previous = _load(report_path) if report_path.is_file() else None
    if previous:
        if (previous.get('schema') != SCHEMA or previous.get('identity') != identity
                or previous.get('work_dir') != str(work) or previous.get('preserved') != preserved):
            return dict(status='deferred', reason='cleanup_intent_changed')
        report = previous
        if report['status'] == 'complete':
            _mark_receipt(work, report, identity)
            return report
    else:
        resources = []
        rendered = work/'private/rendered'
        if rendered.is_dir() and not any(p == rendered or p.is_relative_to(rendered) for p in protected):
            resources.append(dict(kind='directory', inventory=inventory(rendered, [root], originals)))
        stream = work/'model/response-events.jsonl'
        if stream.is_file() and stream not in protected:
            entry = inventory(stream.parent, [root], originals)['files'][stream.name]
            resources.append(dict(kind='file', path=str(stream), **entry))
        archived = work/('delivery-receipt.before-cleanup.'+str(time.time_ns())+'.json')
        archived.write_bytes((work/'delivery-receipt.json').read_bytes())
        report = dict(schema=SCHEMA, status='planned', identity=identity, work_dir=str(work), preserved=preserved,
            resources=resources, verified_delivery_receipt=str(archived), started_at=time.time(),
            removed_files=0, removed_bytes=0)
        write_json(report_path, report)
    try:
        report.update(status='deleting', updated_at=time.time())
        write_json(report_path, report)
        for resource in report['resources']:
            if stop.is_set() or admit is not None and not admit():
                raise InterruptedError('cleanup deferred; work or delivery state changed')
            if resource['kind'] == 'directory':
                record = resource['inventory']
                if Path(record['path']) != work/'private/rendered':
                    raise ValueError('cleanup directory is not allowlisted')
                if any(p == Path(record['path']) or p.is_relative_to(Path(record['path'])) for p in protected):
                    raise ValueError('cleanup directory became a material dependency')
                delete_inventory(record, [root], originals, stop)
            elif resource['kind'] == 'file':
                path = Path(resource['path'])
                if path != work/'model/response-events.jsonl' or path in protected:
                    raise ValueError('cleanup file is not allowlisted')
                if path.exists():
                    actual = inventory(path.parent, [root], originals)['files'][path.name]
                    if actual != {k:resource[k] for k in ('sha256', 'bytes')}:
                        raise ValueError('cleanup file changed')
                    path.unlink()
            else:
                raise ValueError('unsupported cleanup resource')
        missing = []
        for resource in report['resources']:
            if resource['kind'] == 'directory':
                record = resource['inventory']
                missing += [v for name, v in record['files'].items() if not (Path(record['path'])/name).exists()]
                if Path(record['path']).exists():
                    raise ValueError('cleanup directory removal not confirmed')
            else:
                if Path(resource['path']).exists():
                    raise ValueError('cleanup file removal not confirmed')
                missing.append(resource)
        report.update(status='complete', removed_files=len(missing), removed_bytes=sum(p['bytes'] for p in missing),
                      completed_at=time.time())
        report.pop('error_type', None)
    except Exception as exc:
        report.update(status='pending', error_type=type(exc).__name__, updated_at=time.time())
    write_json(report_path, report)
    _mark_receipt(work, report, identity)
    return report


def cleanup_item(root, run_id, item, *, already_locked=False, stop=None, admit=None):
    """Never block a live paper or change a remote delivery's success status."""
    root = Path(root).resolve()
    if not policy(root).get('enabled'):
        return dict(status='disabled')
    if legacy_processes_alive(root):
        return dict(status='deferred', reason='waiting_for_legacy_process_exit')
    stop = stop or threading.Event()
    if stop.is_set():
        return dict(status='deferred', reason='stopped')
    try:
        work = _safe_path(item['preextract_work_dir'], root)
        if not work.is_dir():
            return dict(status='deferred', reason='workspace_missing')
        global_path = queue_path(root).with_name('cleanup.lock')
        with FileLock(str(global_path), timeout=0):
            scope = nullcontext() if already_locked else FileLock(str(work/'operation.lock'), timeout=0)
            with scope:
                if admit is not None and not admit():
                    return dict(status='deferred', reason='delivery_not_complete')
                return _cleanup_locked(root, work, item, stop, admit)
    except Timeout:
        return dict(status='deferred', reason='paper_or_cleanup_busy')
    except Exception as exc:
        return dict(status='error', error_type=type(exc).__name__)


def cleanup_queue_once(root, *, stop=None, limit=16):
    """Read only complete jobs from the same queue; no model/RAG client exists."""
    root = Path(root).resolve()
    if not policy(root).get('enabled'):
        return dict(completed=0, deferred=0)
    if legacy_processes_alive(root):
        return dict(completed=0, deferred=0, waiting_for_legacy=True)
    path = queue_path(root)
    if not path.is_file():
        return dict(completed=0, deferred=0)
    with closing(sqlite3.connect(path.as_uri()+'?mode=ro', uri=True, timeout=5)) as db:
        db.row_factory = sqlite3.Row
        rows = db.execute("SELECT id,payload,projection FROM deliveries WHERE status='complete' "
            "AND COALESCE(json_extract(projection,'$.preextract_delivery.cleanup'),'')!='complete' "
            "AND COALESCE(json_extract(projection,'$.preextract_cleanup.next_check_at'),0)<=? "
            'ORDER BY updated_at LIMIT ?', (time.time(), limit)).fetchall()
    result = dict(completed=0, deferred=0)
    from preextract_delivery_queue import DeliveryQueue
    queue = DeliveryQueue(path)
    for row in rows:
        if stop is not None and stop.is_set():
            break
        payload = json.loads(row['payload'])
        item = dict(payload['item'], **json.loads(row['projection']))
        def admit():
            current = queue.get(row['id'])
            return current['status'] == 'complete' and current['payload']['manifest_sha256'] == payload['manifest_sha256']
        try:
            work = _safe_path(item['preextract_work_dir'], root)
            with FileLock(str(work/'operation.lock'), timeout=0):
                report = cleanup_item(root, payload['run_id'], item, already_locked=True, stop=stop, admit=admit)
                projection = dict(preextract_cleanup=dict(_summary(report), checked_at=time.time(),
                    next_check_at=time.time()+60))
                if report['status'] in {'complete', 'pending'}:
                    receipt = _load(work/'delivery-receipt.json')
                    delivery = copy.deepcopy(queue.get(row['id'])['projection'].get('preextract_delivery', {}))
                    delivery.update(cleanup=receipt['cleanup'], cleanup_receipt=receipt['cleanup_receipt'],
                                    cleanup_result=receipt['cleanup_result'])
                    projection['preextract_delivery'] = delivery
                saved = queue.update(row['id'], projection=projection, expected_status='complete')
                result['completed' if saved and report['status']=='complete' else 'deferred'] += 1
        except (Timeout, OSError, ValueError):
            result['deferred'] += 1
            queue.update(row['id'], projection=dict(preextract_cleanup=dict(status='deferred',
                reason='paper_or_file_busy', next_check_at=time.time()+60)), expected_status='complete')
    return result


def ensure_cleanup_worker(root):
    """One lightweight local tail for the existing queue, including old executors."""
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
    parser = argparse.ArgumentParser()
    parser.add_argument('--data-root', type=Path, required=True)
    args = parser.parse_args()
    path = queue_path(args.data_root).with_name('cleanup.worker.lock')
    try:
        with FileLock(str(path), timeout=0):
            while policy(args.data_root).get('enabled'):
                try:
                    result = cleanup_queue_once(args.data_root)
                    write_json(path.with_name('cleanup-status.json'), dict(observed_at=time.time(), pid=os.getpid(), **result))
                except Exception as exc:
                    write_json(path.with_name('cleanup-status.json'), dict(observed_at=time.time(), pid=os.getpid(), error_type=type(exc).__name__))
                time.sleep(10)
    except Timeout:
        pass


if __name__ == '__main__':
    main()

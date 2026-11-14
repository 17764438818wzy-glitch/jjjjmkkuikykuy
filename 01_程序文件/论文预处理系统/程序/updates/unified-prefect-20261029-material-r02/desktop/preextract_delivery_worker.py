"""Rolling delivery slots, using the original sealed-package publisher."""
from __future__ import annotations

import copy
import json
import os
import subprocess
import sys
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from pathlib import Path

# Only newly launched workers use this immutable overlay. An already running
# uploader retains its original imports and receives no reload or stop signal.
if __name__=='__main__':
    for _project in Path(__file__).resolve().parents:
        if (_project/'config/paths.json').is_file():
            _selected=os.environ.get('PAPERFLOW_SELECTED_CORE')
            _core=Path(_selected).resolve() if _selected else _project/'core'
            if _selected and _core.parent!=Path(__file__).resolve().parent.parent:
                raise RuntimeError('后台上传与选中控制内核版本不一致，保持停止')
            sys.path[:0]=[str(Path(__file__).resolve().parent),str(_project/'desktop'),str(_core)]
            break
    if '--inspect-source' in sys.argv:
        import associated_delivery
        print(json.dumps(dict(worker=__file__,associated_delivery=associated_delivery.__file__)))
        raise SystemExit(0)

from filelock import FileLock, Timeout
from preextract_delivery_queue import DeliveryQueue, DELIVERY_FIELDS, queue_path
from upload_tracking import service_key


DELIVERY_WORKERS = 6
NATIVE_DELIVERY_WORKERS = 4

_runtime_last={}


def publish_runtime(root,active_ids,limit,*,state='running',force=False,worker_exited=False,control=None,admission=None):
    """Best-effort telemetry only; it never admits, retries or changes a job."""
    import psutil
    from engine import write_json
    path=queue_path(root).with_name('upload-runtime.json');now=time.monotonic()
    if not force and now-_runtime_last.get(str(path),0)<1:return
    try:
        create_time=psutil.Process().create_time()
        if state in {'stopped','paused','error'} and path.is_file():
            previous=json.loads(path.read_text(encoding='utf8'))
            if previous.get('pid')!=os.getpid() or previous.get('create_time')!=create_time:return
        ids=sorted(set(active_ids))
        write_json(path,dict(schema='native-upload-observation/1',queue_path=str(queue_path(root)),
            pid=os.getpid(),create_time=create_time,observed_at=time.time(),state=state,
            active=len(ids),limit=limit,active_ids=ids,configured_capacity=limit,
            effective_admission=0 if state in {'draining','paused','stopped','error'} else limit if admission is None else admission,
            admission_state='paused' if state in {'draining','paused'} else state,
            control_generation=(control or {}).get('generation'),control_reason=(control or {}).get('reason'),
            worker_exited=worker_exited))
        _runtime_last[str(path)]=now
    except (OSError,ValueError,TypeError,psutil.Error):pass


def read_runtime(root,*,max_age=5):
    """Read telemetry with freshness and PID identity; absence is not activity."""
    import psutil
    path=queue_path(root).with_name('upload-runtime.json')
    try:
        row=json.loads(path.read_text(encoding='utf-8'))
        fresh=0<=time.time()-row['observed_at']<=max_age
        alive=row.get('worker_exited') is True or psutil.Process(row['pid']).create_time()==row['create_time']
        if fresh and alive:return dict(row,stale=False)
        return dict(row,observed_state=row.get('state'),observed_active=row.get('active'),
            observed_active_ids=row.get('active_ids'),observed_admission_state=row.get('admission_state'),
            observed_effective_admission=row.get('effective_admission'),observed_worker_exited=row.get('worker_exited'),
            state='unknown',active=None,active_ids=None,admission_state='unknown',effective_admission=None,
            worker_exited=None,stale=True)
    except (OSError,ValueError,KeyError,TypeError,psutil.Error):
        return dict(state='unknown',active=None,active_ids=None,admission_state='unknown',effective_admission=None,
                    worker_exited=None,stale=True)


class _DeliveryStore:
    """The existing handoff writes its projection here, never to the producer."""
    def __init__(self, queue, job):
        self.queue, self.job = queue, job
        self.root = Path(job['payload']['store_root'])
        self.runs = self.root / 'runs'
        self.lock = threading.RLock()

    def load(self, run_id):
        if run_id != self.job['payload']['run_id']:
            raise ValueError('交付批次身份不一致')
        item = copy.deepcopy(self.job['payload']['item'])
        item.update(self.queue.get(self.job['id'])['projection'])
        return dict(run_id=run_id, items=[item])

    def save(self, manifest):
        item = manifest['items'][0]
        self.queue.update(self.job['id'], projection={key: item[key] for key in DELIVERY_FIELDS if key in item})


def deliver_job(queue, job, client, stop):
    from preextract_delivery_control import QueueStop
    stop=QueueStop(queue,job,stop)
    if stop.is_set():
        queue.deferred(job,reason='upload_paused',delay=0)
        return
    if job['payload'].get('kind')=='associated_original':
        from associated_pending import deliver_job as deliver_attachment
        return deliver_attachment(queue,job,client,stop)
    from preextract_handoff import handoff_preextract_item
    from preextract_delivery_queue import package_reference
    payload = job['payload']
    store = _DeliveryStore(queue, job)
    try:
        if service_key(client.base_url) != payload['service']:
            raise ValueError('当前服务与成品排队时的投递服务不一致')
        current = package_reference(store, payload['run_id'], payload['item']['id'], client.base_url)
        if current['id'] != job['id']:
            raise ValueError('排队后封包或任务身份发生变化，未投递')
        # Candidate follow-up is opt-in only for a fresh queue-owned native
        # job. Existing bindings, receipts and historical jobs are not adopted.
        from preextract_native_followup import FollowClient, arm, wait_projection, observed_terminal
        saved = queue.get(job['id'])['projection'].get('preextract_native_followup')
        marker = saved
        if (not payload['item'].get('prepared_scope') and not getattr(client, 'prepared_scope', None)
                and getattr(client, 'native_batch_config', None) is not None):
            marker = arm(queue, job)
        if marker and marker.get('requests') and getattr(client, 'native_batch_config', None) is None:
            queue.update(job['id'], status='queued', expected_status='uploading',
                         projection=wait_projection(queue, job, 'native_configuration_unavailable'))
            return
        active_client = FollowClient(client, queue, job, stop) if marker else client
        following = bool(marker and marker.get('requests'))
        outcome=handoff_preextract_item(store, payload['run_id'], payload['item']['id'], active_client,
            stop=stop, dispatch_scope=job.get('dispatch_scope'), reconcile=following,
            continue_delivery=following,
            expected_generations=job.get('control_generations') if following else None)
        if (outcome or {}).get('status')=='waiting_workspace':
            queue.deferred(job)
            return
        if marker and (outcome or {}).get('status') in {'native_followup_pending', 'reconciliation_required', 'handoff_error', 'paused'}:
            status = outcome.get('status')
            terminal = observed_terminal(outcome) if status == 'reconciliation_required' else None
            retry = (status in {'native_followup_pending', 'reconciliation_required', 'paused'}
                     or outcome.get('transient') is True or stop.is_set())
            if not terminal and retry:
                try:
                    observed_at = ((outcome.get('execution') or {}).get('observed_at')
                                   if status == 'native_followup_pending' else outcome.get('observed_at')
                                   if status == 'reconciliation_required' else None)
                    projection = wait_projection(queue, job, status, observed_at=observed_at)
                except (OSError, ValueError, KeyError, TypeError, OverflowError):
                    terminal = 'native_identity_or_tracking_limit'
                else:
                    queue.update(job['id'], status='queued', expected_status='uploading', projection=projection)
                    return
            queue.update(job['id'], status='needs_review', expected_status='uploading', projection=dict(
                handoff_status='pending', preextract_handoff_blocked=True,
                handoff_reason='原文档结果失败、冲突或无法安全确认；保留原回执并隔离，未重复上传',
                preextract_delivery_wait=dict(reason=terminal or outcome.get('error_type') or 'native_unrecoverable',
                                              next_attempt_at=0, owner='isolated')))
            return
        projected = queue.get(job['id'])['projection']
        complete = (projected.get('handoff_status') == 'uploaded'
                    and (projected.get('preextract_delivery') or {}).get('status') == 'complete')
        work = Path(payload['item']['preextract_work_dir'])
        state = 'complete' if complete else 'needs_review' if (
            (work / 'delivery-binding.json').exists() or (work / 'delivery-receipt.json').exists()) else 'failed'
        if stop.is_set() and state=='failed':
            queue.deferred(job,reason='upload_paused',delay=0)
            return
        tail = ({'preextract_cleanup': dict(status='deferred', reason='task_scoped_nas_release_unconfirmed')}
                if complete and marker else None)
        committed = queue.update(job['id'], status=state, projection=tail, expected_status='uploading')
        if complete and committed is True:
            # A failed CAS owns no storage wakeup. Local tail failures must not
            # turn the already committed RAG delivery into a failed upload.
            try:
                from preextract_delivery_queue import MODE
                saved = queue.get(job['id'])
                if (MODE != 'disk_queue_v1' or saved is None or saved['status'] != 'complete'
                        or saved['payload'] != payload or payload['item'].get('prepared_scope')
                        or getattr(client, 'prepared_scope', None)
                        or payload['item'].get('content_format') != 'paper_preextract'):
                    return
                from preextract_nas import register_task_storage, storage_progress, kick
                storage_root = Path(queue.path).resolve().parents[2]
                result = register_task_storage(storage_root, job,
                    dispatch_scope=job.get('dispatch_scope'), stop=stop)
                storage_progress(queue,job['id'],storage_status=result.get('status','held'),
                    storage_reason=result.get('reason') or result.get('held') or '')
                # Upload completion only wakes the ordinary copy/cleanup tail;
                # it supplies no explicit release selection or deletion grant.
                kick(storage_root,stop=stop)
                from preextract_cleanup import ensure_cleanup_worker
                from global_task_control import GlobalTaskControl
                if GlobalTaskControl(storage_root).snapshot()['admission_open']:ensure_cleanup_worker(storage_root)
            except Exception as exc:
                try:
                    from preextract_nas import storage_progress
                    storage_progress(queue,job['id'],storage_status='pending',
                        storage_reason='storage_tail_pending',storage_error_type=type(exc).__name__)
                except Exception:
                    pass
    except Exception as exc:
        reason = str(exc).replace(getattr(client, 'api_key', '') or '\0', '[密钥]')[:240]
        if getattr(client,'prepared_scope',None):
            from paperflow.scheduling_policy import ActionDeferred
            if isinstance(exc,(ActionDeferred,InterruptedError)):
                queue.update(job['id'],status='pending',projection=dict(handoff_status='pending',handoff_reason=reason))
                raise
        queue.update(job['id'], status='needs_review', projection=dict(
            handoff_status='pending', handoff_reason=reason, preextract_handoff_blocked=True))


def drain_queue(root, client, *, stop=None, idle_seconds=0, deliver=None, native_batch=False,
                dispatch_scope=None, followup_only=False):
    """Own one shared delivery pool across batches; never submit the whole backlog."""
    if not dispatch_scope or not dispatch_scope.get('scope_ids'):
        return False
    from preextract_task_delivery import active_dispatch_scope
    path = queue_path(root)
    if not path.is_file():return False
    initial_queue = DeliveryQueue(path)
    has_initial = (initial_queue.has_followup_work(client.base_url, dispatch_scope=dispatch_scope, due_only=True)
                   if followup_only else initial_queue.has_work(client.base_url, include_interrupted=True, dispatch_scope=dispatch_scope))
    if not has_initial:
        return False
    stop = stop or threading.Event()
    deliver = deliver or deliver_job
    lock = FileLock(str(path) + '.worker.lock', timeout=0)
    try:
        lock.acquire()
    except Timeout:
        return False
    transition_guard = expansion_guard = config = None
    unconfirmed_launches=[]
    final_state='stopped';workers=DELIVERY_WORKERS;configured_capacity=workers;published_final=False
    queue=DeliveryQueue(path)
    from preextract_delivery_control import sync_operator_pause,QueueStop
    from global_task_control import GlobalTaskControl,GlobalPaused
    global_control=GlobalTaskControl(root)
    try:
        publish_runtime(root,[],workers,state='starting',force=True)
        sync_operator_pause(queue)
        if queue.control()['paused'] or not global_control.snapshot()['admission_open']:
            final_state='paused'
            policy=path.with_name('native-batch.json')
            raw=json.loads(policy.read_text(encoding='utf-8')) if policy.is_file() else {}
            capacity=raw.get('native_workers_target',raw.get('native_workers',workers))
            if capacity==0 and raw.get('enabled') is True and raw.get('operator_upload_paused') is True:
                from preextract_batch_delivery import load_native_batch_config,native_worker_capacity
                capacity=native_worker_capacity(load_native_batch_config(root,client))
            workers=capacity if type(capacity) is int and capacity>0 else None
            configured_capacity=workers
            return True
        if native_batch:
            expansion_guard = FileLock(str(path) + '.expansion.lock', timeout=0)
            try:
                expansion_guard.acquire()
            except Timeout:
                expansion_guard = None
                return False
            transition_guard = FileLock(str(path) + '.transition.lock', timeout=0)
            try:
                transition_guard.acquire()
            except Timeout:
                transition_guard = None
                return False
            from preextract_batch_delivery import load_native_batch_config
            client.native_batch_config = load_native_batch_config(root, client)
        config = getattr(client, 'native_batch_config', None)
        gate = None
        workers = DELIVERY_WORKERS
        if config is not None:
            from preextract_batch_delivery import NativeMemoryGate, native_worker_target,native_worker_capacity
            gate = NativeMemoryGate(config)
            workers = getattr(config, 'max_native_workers', 8)
            configured_capacity=native_worker_capacity(config)
        # The durable grants are shared with an already running worker. Read
        # them outside G admission; a new grant affects only future claims.
        dispatch_scope=active_dispatch_scope(root,client.base_url)
        queue.recover(client.base_url,dispatch_scope=dispatch_scope,followup_only=followup_only)
        recovered_scope_ids=set(dispatch_scope.get('scope_ids',()))
        from associated_pending import refresh_waiting
        if not followup_only:
            refresh_waiting(queue,client.base_url,stop=stop,dispatch_scope=dispatch_scope)
        idle_since = time.monotonic()
        def deliver_stage(job,token,started):
            started.set()
            try:
                with global_control.using(token,'delivery'):deliver(queue,job,client,stop)
            except BaseException as exc:
                global_control.finish(token,'failed',type(exc).__name__)
                raise
            else:global_control.finish(token)
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix='sealed-delivery') as pool:
            pending = set();pending_ids={}
            while True:
                dispatch_scope=active_dispatch_scope(root,client.base_url)
                sync_operator_pause(queue)
                paused=queue.control()['paused'] or not global_control.snapshot()['admission_open']
                target = 0 if paused else native_worker_target(config) if gate is not None else workers
                configured_capacity=native_worker_capacity(config) if gate is not None else workers
                if target==0:paused=True
                new_scope_ids=set(dispatch_scope.get('scope_ids',()))-recovered_scope_ids
                if new_scope_ids and not pending:
                    # Never recover uploading rows while this pool owns an
                    # in-flight job, including one shared by a newer grant.
                    queue.recover(client.base_url,dispatch_scope=dispatch_scope,followup_only=followup_only)
                    if not followup_only:
                        refresh_waiting(queue,client.base_url,stop=stop,dispatch_scope=dispatch_scope)
                    recovered_scope_ids.update(new_scope_ids)
                while not stop.is_set() and len(pending) < target:
                    if gate is not None and not gate.can_admit(len(pending)):
                        break
                    try:
                        with global_control.admission() as admission:
                            job = queue.claim(client.base_url,dispatch_scope=dispatch_scope,followup_only=followup_only)
                            if job is None:break
                            if QueueStop(queue,job,stop).is_set():
                                queue.deferred(job,reason='upload_paused',delay=0)
                                continue
                            payload=job['payload'];item=payload.get('item',{})
                            token=admission.begin('delivery',batch_id=payload['run_id'],
                                paper_id=item.get('id',job['id']),name=item.get('title',''))
                            started=threading.Event()
                            try:future=pool.submit(deliver_stage,job,token,started)
                            except BaseException:
                                # submit can enqueue work before thread startup
                                # fails. Settle the executor outside G's SQL
                                # transaction before deciding it never started.
                                unconfirmed_launches.append((job,token,started))
                                final_state='error'
                                raise
                    except GlobalPaused:
                        paused=True
                        break
                    pending.add(future);pending_ids[future]=job['id']
                    idle_since = time.monotonic()
                phase='draining' if (paused or stop.is_set()) and pending else 'paused' if paused else 'running'
                publish_runtime(root,[pending_ids[f] for f in pending],configured_capacity,state=phase,control=queue.control(),admission=target)
                if pending:
                    finished, pending = wait(pending, timeout=.2, return_when=FIRST_COMPLETED)
                    for future in finished:
                        pending_ids.pop(future,None)
                        future.result()
                    if finished and not followup_only:refresh_waiting(queue,client.base_url,stop=stop,dispatch_scope=dispatch_scope)
                    idle_since = time.monotonic()
                elif stop.is_set() or paused:
                    if paused:final_state='paused'
                    break
                elif time.monotonic() - idle_since >= idle_seconds:
                    eligible = (queue.has_followup_work(client.base_url, dispatch_scope=dispatch_scope, due_only=True)
                                if followup_only else queue.has_work(client.base_url, dispatch_scope=dispatch_scope))
                    if gate is not None and eligible:
                        stop.wait(1)
                        continue
                    # Serialize the final empty check and release with launch.
                    # A producer registering at idle exit either keeps this
                    # worker alive or observes a released lock and starts one.
                    try:
                        with FileLock(str(path) + '.launcher.lock', timeout=0):
                            idle_scope=active_dispatch_scope(root,client.base_url)
                            eligible = (queue.has_followup_work(client.base_url, dispatch_scope=idle_scope, due_only=True)
                                        if followup_only else queue.has_work(client.base_url, include_interrupted=True, dispatch_scope=idle_scope))
                            if eligible:
                                stop.wait(.05)
                                continue
                            if not followup_only:
                                from preextract_batch_delivery import restore_temporary_native_policy
                                restore_temporary_native_policy(config)
                            if any(row['paused'] for row in [queue.control(run_id=j['payload']['run_id']) for j in queue.scoped_list(client.base_url,status='queued',dispatch_scope=idle_scope)]):
                                final_state='paused'
                            publish_runtime(root,[],configured_capacity,state=final_state,force=True,worker_exited=True,control=queue.control())
                            published_final=True
                            lock.release()
                            return True
                    except Timeout:
                        stop.wait(.05)
                else:
                    stop.wait(.2)
        return True
    finally:
        # ThreadPoolExecutor.__exit__ has now settled every started thread.
        # A lost submit result is never permission to replay possible effects.
        for job,token,started in unconfirmed_launches:
            try:
                if not started.is_set():
                    queue.deferred(job,reason='executor_submit_failed',delay=0)
                    global_control.finish(token,'not_started','executor_submit_failed')
                elif queue.get(job['id'])['status']=='uploading':
                    queue.update(job['id'],status='needs_review',projection=dict(handoff_status='pending',
                        preextract_handoff_blocked=True,handoff_reason='工作线程已启动但提交结果未确认；保留意图等待核对'))
            except Exception:
                final_state='error'  # Retain an unsettled G owner, never invent completion.
        if not published_final:
            try:publish_runtime(root,[],configured_capacity,state=final_state,force=True,worker_exited=True,control=queue.control())
            except Exception:pass  # Telemetry cannot prevent releasing the real worker locks.
        try:
            if not followup_only:
                from preextract_batch_delivery import restore_temporary_native_policy
                restore_temporary_native_policy(config)
        finally:
            lock.release()
            if transition_guard is not None:
                transition_guard.release()
            if expansion_guard is not None:
                expansion_guard.release()


_launching = {}
_launch_guard = threading.Lock()


def _ensure_worker(root, path, service, *, followup_only=False):
    from global_task_control import GlobalTaskControl
    if not GlobalTaskControl(root).snapshot()['admission_open']:return
    from ingest_service import validate_worker_service
    validate_worker_service(root, service)
    # Only this background launcher waits. Waiting for the final empty-check
    # handshake prevents a newly persisted package losing its wake at exit.
    with FileLock(str(path) + '.launcher.lock'):
        from preextract_task_delivery import active_dispatch_scope
        dispatch_scope=active_dispatch_scope(root,service)
        queue = DeliveryQueue(path)
        eligible = (queue.has_followup_work(service, dispatch_scope=dispatch_scope, due_only=True)
                    if followup_only else queue.has_work(service,include_interrupted=True,dispatch_scope=dispatch_scope))
        if not eligible:return 'empty'
        guard = FileLock(str(path) + '.worker.lock', timeout=0)
        try:
            guard.acquire()
        except Timeout:
            return 'busy'
        guard.release()
        with (path.parent / 'worker.stdout.log').open('ab') as out, (path.parent / 'worker.stderr.log').open('ab') as err:
            args = [sys.executable, str(Path(__file__).resolve()), '--data-root', str(root)]
            if followup_only:args.append('--followup-only')
            child = subprocess.Popen(args,
                cwd=str(Path(__file__).resolve().parent), stdin=subprocess.DEVNULL, stdout=out, stderr=err,
                creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0), close_fds=True)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                guard.acquire()
            except Timeout:
                return 'started'
            guard.release()
            if child.poll() is not None:
                raise RuntimeError('成品上传进程未能启动，包保留在队列')
            time.sleep(.1)
        raise RuntimeError('成品上传进程未取得队列锁，包保留在队列')


def wake_worker(root, service, *, dispatch_scope=None, allow_cleanup_start=True, followup_only=False):
    """Launch outside the producer; neither network nor worker startup blocks LLM."""
    if not dispatch_scope or not dispatch_scope.get('scope_ids'):return
    root = Path(root).resolve()
    from global_task_control import GlobalTaskControl
    if not GlobalTaskControl(root).snapshot()['admission_open']:return
    path = queue_path(root)
    if not path.is_file():return
    queue=DeliveryQueue(path)
    from preextract_delivery_control import sync_operator_pause
    sync_operator_pause(queue)
    eligible = (queue.has_followup_work(service, dispatch_scope=dispatch_scope, due_only=True)
                if followup_only else queue.has_work(service, include_interrupted=True, dispatch_scope=dispatch_scope))
    if queue.control()['paused'] or not eligible:return
    if allow_cleanup_start and not followup_only:
        from preextract_cleanup import ensure_cleanup_worker
        try:ensure_cleanup_worker(root)
        except (OSError,ValueError):pass  # A cleanup launcher never blocks delivery.
    path = queue_path(root)
    if not path.exists():
        return
    key = str(path)
    with _launch_guard:
        if key in _launching:
            entry = _launching[key]
            entry['normal'] = entry['normal'] or not followup_only
            entry['requested'].set()
            return
        entry = _launching[key] = dict(requested=threading.Event(), normal=not followup_only)

    def start():
        while True:
            with _launch_guard:
                entry['requested'].clear()
                restricted = not entry['normal']
            result = None
            try:
                result = _ensure_worker(root, path, service, followup_only=restricted)
            except Exception as exc:
                from engine import write_json
                try:
                    write_json(path.parent / 'worker-error.json', dict(error_type=type(exc).__name__,
                        message='后台上传暂未启动；成品已保存，可由原交接入口继续', at=time.time()))
                except OSError:
                    pass
            with _launch_guard:
                if entry['requested'].is_set():
                    continue
                # A normal producer wake arriving during a restricted follow-up
                # worker must not be lost when that worker owns the pool lock.
                retry_busy = result == 'busy' and entry['normal']
                if not retry_busy:
                    del _launching[key]
                    return
            entry['requested'].wait(2)

    threading.Thread(target=start, name='sealed-delivery-launcher', daemon=True).start()


def resume_queue(root, service, *, dispatch_scope=None):
    if not dispatch_scope or not dispatch_scope.get('scope_ids'):return
    from global_task_control import GlobalTaskControl
    if not GlobalTaskControl(root).snapshot()['admission_open']:return
    path=queue_path(root)
    if path.is_file():
        queue=DeliveryQueue(path)
        from preextract_delivery_control import sync_operator_pause
        sync_operator_pause(queue)
        if queue.control()['paused'] or not queue.has_work(service,include_interrupted=True,dispatch_scope=dispatch_scope):return
    from preextract_cleanup import ensure_cleanup_worker
    try:ensure_cleanup_worker(root)
    except (OSError,ValueError):pass
    path = queue_path(root)
    if path.is_file():
        from associated_pending import refresh_waiting
        refresh_waiting(DeliveryQueue(path),service,dispatch_scope=dispatch_scope)
    if path.is_file() and DeliveryQueue(path).has_work(service, include_interrupted=True,dispatch_scope=dispatch_scope):
        wake_worker(root, service,dispatch_scope=dispatch_scope)


def resume_uploads(root,service,*,expected_generation,run_id=None,dispatch_scope=None):
    queue=DeliveryQueue(queue_path(root))
    control=queue.resume(run_id=run_id,expected_generation=expected_generation)
    # Resuming a control never creates or renews a dispatch grant. An expired
    # scope remains closed; finite maintenance must authorize its exact refs.
    resume_queue(root,service,dispatch_scope=dispatch_scope)
    return control


def main():
    import argparse
    from auto_config import load_key
    from ragflow_client import RagflowClient
    parser = argparse.ArgumentParser()
    parser.add_argument('--data-root', type=Path, required=True)
    parser.add_argument('--followup-only', action='store_true')
    args = parser.parse_args()
    root = args.data_root.resolve()
    settings = json.loads((root / 'settings.json').read_text(encoding='utf8'))
    from preextract_task_delivery import active_dispatch_scope
    dispatch_scope=active_dispatch_scope(root,settings['ragflow_url'])
    if not dispatch_scope.get('scope_ids'):return
    key = os.environ.get('RAGFLOW_API_KEY', '') or load_key(root, 'ragflow')
    if not key:
        raise ValueError('当前 Windows 账户未取得原 RAGFlow 凭据，成品保留待上传')
    drain_queue(root, RagflowClient(settings['ragflow_url'], key),
                idle_seconds=0 if args.followup_only else 30, native_batch=True,
                dispatch_scope=dispatch_scope, followup_only=args.followup_only)


if __name__ == '__main__':
    main()

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

from filelock import FileLock, Timeout
from preextract_delivery_queue import DeliveryQueue, DELIVERY_FIELDS, queue_path
from upload_tracking import service_key


DELIVERY_WORKERS = 6
NATIVE_DELIVERY_WORKERS = 4


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
        handoff_preextract_item(store, payload['run_id'], payload['item']['id'], client, stop=stop)
        projected = queue.get(job['id'])['projection']
        complete = (projected.get('handoff_status') == 'uploaded'
                    and (projected.get('preextract_delivery') or {}).get('status') == 'complete')
        work = Path(payload['item']['preextract_work_dir'])
        state = 'complete' if complete else 'needs_review' if (
            (work / 'delivery-binding.json').exists() or (work / 'delivery-receipt.json').exists()) else 'failed'
        queue.update(job['id'], status=state)
    except Exception as exc:
        reason = str(exc).replace(getattr(client, 'api_key', '') or '\0', '[密钥]')[:240]
        queue.update(job['id'], status='needs_review', projection=dict(
            handoff_status='pending', handoff_reason=reason, preextract_handoff_blocked=True))


def drain_queue(root, client, *, stop=None, idle_seconds=0, deliver=None, native_batch=False):
    """Own one shared delivery pool across batches; never submit the whole backlog."""
    path = queue_path(root)
    stop = stop or threading.Event()
    deliver = deliver or deliver_job
    lock = FileLock(str(path) + '.worker.lock', timeout=0)
    try:
        lock.acquire()
    except Timeout:
        return False
    transition_guard = expansion_guard = None
    try:
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
            from preextract_batch_delivery import NativeMemoryGate, native_worker_target
            gate = NativeMemoryGate(config)
            workers = 8
        queue = DeliveryQueue(path)
        queue.recover(client.base_url)
        idle_since = time.monotonic()
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix='sealed-delivery') as pool:
            pending = set()
            while True:
                target = native_worker_target(config) if gate is not None else workers
                while not stop.is_set() and len(pending) < target:
                    if gate is not None and not gate.can_admit(len(pending)):
                        break
                    job = queue.claim(client.base_url)
                    if job is None:
                        break
                    pending.add(pool.submit(deliver, queue, job, client, stop))
                    idle_since = time.monotonic()
                if pending:
                    finished, pending = wait(pending, timeout=.2, return_when=FIRST_COMPLETED)
                    for future in finished:
                        future.result()
                    idle_since = time.monotonic()
                elif stop.is_set():
                    break
                elif time.monotonic() - idle_since >= idle_seconds:
                    if gate is not None and queue.has_work(client.base_url):
                        stop.wait(1)
                        continue
                    # Serialize the final empty check and release with launch.
                    # A producer registering at idle exit either keeps this
                    # worker alive or observes a released lock and starts one.
                    try:
                        with FileLock(str(path) + '.launcher.lock', timeout=0):
                            if queue.has_work(client.base_url):
                                continue
                            lock.release()
                            return True
                    except Timeout:
                        stop.wait(.05)
                else:
                    stop.wait(.2)
        return True
    finally:
        lock.release()
        if transition_guard is not None:
            transition_guard.release()
        if expansion_guard is not None:
            expansion_guard.release()


_launching = {}
_launch_guard = threading.Lock()


def _ensure_worker(root, path, service):
    from ingest_service import validate_worker_service
    validate_worker_service(root, service)
    # Only this background launcher waits. Waiting for the final empty-check
    # handshake prevents a newly persisted package losing its wake at exit.
    with FileLock(str(path) + '.launcher.lock'):
        guard = FileLock(str(path) + '.worker.lock', timeout=0)
        try:
            guard.acquire()
        except Timeout:
            return
        guard.release()
        with (path.parent / 'worker.stdout.log').open('ab') as out, (path.parent / 'worker.stderr.log').open('ab') as err:
            child = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), '--data-root', str(root)],
                cwd=str(Path(__file__).resolve().parent), stdin=subprocess.DEVNULL, stdout=out, stderr=err,
                creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0), close_fds=True)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                guard.acquire()
            except Timeout:
                return
            guard.release()
            if child.poll() is not None:
                raise RuntimeError('成品上传进程未能启动，包保留在队列')
            time.sleep(.1)
        raise RuntimeError('成品上传进程未取得队列锁，包保留在队列')


def wake_worker(root, service):
    """Launch outside the producer; neither network nor worker startup blocks LLM."""
    root = Path(root).resolve()
    from preextract_cleanup import ensure_cleanup_worker
    try:ensure_cleanup_worker(root)
    except (OSError,ValueError):pass  # A cleanup launcher never blocks delivery.
    path = queue_path(root)
    if not path.exists():
        return
    key = str(path)
    with _launch_guard:
        if key in _launching:
            _launching[key].set()
            return
        requested = _launching[key] = threading.Event()

    def start():
        while True:
            with _launch_guard:
                requested.clear()
            try:
                _ensure_worker(root, path, service)
            except Exception as exc:
                from engine import write_json
                try:
                    write_json(path.parent / 'worker-error.json', dict(error_type=type(exc).__name__,
                        message='后台上传暂未启动；成品已保存，可由原交接入口继续', at=time.time()))
                except OSError:
                    pass
            with _launch_guard:
                if requested.is_set():
                    continue
                del _launching[key]
                return

    threading.Thread(target=start, name='sealed-delivery-launcher', daemon=True).start()


def resume_queue(root, service):
    from preextract_cleanup import ensure_cleanup_worker
    try:ensure_cleanup_worker(root)
    except (OSError,ValueError):pass
    path = queue_path(root)
    if path.is_file() and DeliveryQueue(path).has_work(service, include_interrupted=True):
        wake_worker(root, service)


def main():
    import argparse
    from auto_config import load_key
    from ragflow_client import RagflowClient
    parser = argparse.ArgumentParser()
    parser.add_argument('--data-root', type=Path, required=True)
    args = parser.parse_args()
    root = args.data_root.resolve()
    settings = json.loads((root / 'settings.json').read_text(encoding='utf8'))
    key = os.environ.get('RAGFLOW_API_KEY', '') or load_key(root, 'ragflow')
    if not key:
        raise ValueError('当前 Windows 账户未取得原 RAGFlow 凭据，成品保留待上传')
    drain_queue(root, RagflowClient(settings['ragflow_url'], key), idle_seconds=30, native_batch=True)


if __name__ == '__main__':
    main()

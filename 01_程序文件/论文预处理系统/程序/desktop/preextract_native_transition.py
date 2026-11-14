"""Drain only legacy admissions; keep all existing uploads running to completion."""
from __future__ import annotations

import json
import sqlite3
import time
import uuid
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import closing
from pathlib import Path
import threading

from filelock import FileLock, Timeout
from preextract_delivery_queue import DeliveryQueue, queue_path, _row
from upload_tracking import service_key


class NativeQueueGate:
    NAME = 'preextract_native_admission_gate'

    def __init__(self, path, service):
        self.path, self.service = Path(path), service_key(service)

    def install(self):
        with closing(sqlite3.connect(self.path, timeout=5)) as db, db:
            db.execute('BEGIN IMMEDIATE')
            service = db.execute('SELECT quote(?)', (self.service,)).fetchone()[0]
            sql = f'''CREATE TRIGGER {self.NAME}
                BEFORE UPDATE OF status ON deliveries
                WHEN OLD.status='queued' AND NEW.status='uploading' AND NEW.service={service}
                  AND json_extract(NEW.payload,'$._native_claim') IS
                      json_extract(OLD.payload,'$._native_claim')
                BEGIN SELECT RAISE(ABORT,'native replacement: legacy admission closed'); END'''
            current = db.execute("SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?",
                                 (self.NAME,)).fetchone()
            if current is not None and ' '.join(current[0].split()) != ' '.join(sql.split()):
                raise ValueError('existing native admission gate belongs to another policy')
            if current is None:
                db.execute(sql)
        # Installation is committed first, so no older admission can be missed.
        with closing(sqlite3.connect(self.path, timeout=5)) as db:
            return [row[0] for row in db.execute(
                "SELECT id FROM deliveries WHERE service=? AND status='uploading'", (self.service,))]

    def claim(self, queue, *, generation=None, guard_ids=()):
        with closing(sqlite3.connect(self.path, timeout=5)) as db, db:
            db.row_factory = sqlite3.Row
            db.execute('BEGIN IMMEDIATE')
            if guard_ids:
                ids = list(dict.fromkeys(guard_ids))
                placeholders = ','.join('?' for _ in ids)
                states = dict(db.execute('SELECT id,status FROM deliveries WHERE id IN ('+placeholders+')', ids))
                if len(states) != len(ids) or any(s not in {'complete','uploading'} for s in states.values()):
                    raise RuntimeError('prior delivery remains uncertain; no new admission')
            row = db.execute("SELECT * FROM deliveries WHERE service=? AND status='queued' "
                             "ORDER BY sequence LIMIT 1", (self.service,)).fetchone()
            if row is None:
                return None
            result = _row(row)
            result['payload']['_native_claim'] = uuid.uuid4().hex
            if generation is not None:
                result['payload']['_native_generation'] = generation
                result['payload']['_native_claim'] = generation + uuid.uuid4().hex
            now = time.time()
            db.execute("UPDATE deliveries SET payload=?,status='uploading',updated_at=? WHERE id=?",
                (json.dumps(result['payload'], ensure_ascii=False, sort_keys=True), now, row['id']))
            result.update(status='uploading', updated_at=now)
        return result

    def remove(self):
        with closing(sqlite3.connect(self.path, timeout=5)) as db, db:
            db.execute(f'DROP TRIGGER IF EXISTS {self.NAME}')


class NativeGenerationGate:
    """Transfer future admission only; earlier executors finish their own futures."""
    NAME = 'preextract_native_generation_gate'

    def __init__(self, path, service, generation):
        self.path, self.service, self.generation = Path(path), service_key(service), generation

    def install(self):
        with closing(sqlite3.connect(self.path, timeout=5)) as db, db:
            db.execute('BEGIN IMMEDIATE')
            service, generation = db.execute('SELECT quote(?),quote(?)',
                (self.service, self.generation)).fetchone()
            sql = f'''CREATE TRIGGER {self.NAME} BEFORE UPDATE OF status ON deliveries
                WHEN OLD.status='queued' AND NEW.status='uploading' AND NEW.service={service}
                  AND (json_extract(NEW.payload,'$._native_generation') IS NOT {generation}
                       OR substr(json_extract(NEW.payload,'$._native_claim'),1,{len(self.generation)}) IS NOT {generation})
                BEGIN SELECT RAISE(ABORT,'native admission transferred to capacity controller'); END'''
            existing = db.execute("SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?",
                                  (self.NAME,)).fetchone()
            if existing is not None and ' '.join(existing[0].split()) != ' '.join(sql.split()):
                raise ValueError('another native capacity generation already owns admission')
            if existing is None:
                db.execute(sql)
        with closing(sqlite3.connect(self.path, timeout=5)) as db:
            return [row[0] for row in db.execute(
                "SELECT id FROM deliveries WHERE service=? AND status='uploading'", (self.service,))]

    def remove(self):
        with closing(sqlite3.connect(self.path, timeout=5)) as db, db:
            db.execute(f'DROP TRIGGER IF EXISTS {self.NAME}')


def expand_native_queue(root, client, *, idle_seconds=30):
    from preextract_batch_delivery import NativeMemoryGate, load_native_batch_config, native_worker_target
    from preextract_delivery_worker import deliver_job
    root, generation = Path(root), uuid.uuid4().hex
    path = queue_path(root)
    report_path = path.with_name('native-capacity.json')
    expansion_lock = FileLock(str(path)+'.expansion.lock', timeout=0)
    transition_lock = FileLock(str(path)+'.transition.lock', timeout=0)
    worker_lock = FileLock(str(path)+'.worker.lock', timeout=0)
    gate = NativeQueueGate(path, client.base_url)
    ownership = NativeGenerationGate(path, client.base_url, generation)
    report = dict(schema='preextract-native-capacity/1', started_at=time.time(),
                  generation=generation, status='starting', completed=[])

    def save():
        report['observed_at'] = time.time()
        temporary = report_path.with_suffix('.tmp')
        temporary.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf8')
        temporary.replace(report_path)

    with expansion_lock:
        config = client.native_batch_config = load_native_batch_config(root, client)
        if config is None:
            raise ValueError('explicit verified native policy is required')
        target = native_worker_target(config)
        memory = NativeMemoryGate(config)
        queue = DeliveryQueue(path)
        gate.install()
        report['external_ids'] = external = ownership.install()
        pending = {}
        stop = threading.Event()
        transition_owned = worker_owned = False
        idle_since, last_save = time.monotonic(), 0
        def finish(futures):
            for future in futures:
                item = pending.pop(future)
                future.result()
                item.update(status=queue.get(item['id'])['status'],
                    elapsed_seconds=time.time()-item['started_at'], finished_at=time.time())
                report['completed'].append(item)
                if item['status'] != 'complete':
                    raise RuntimeError('native delivery requires review; new admission stopped')
        try:
            with ThreadPoolExecutor(max_workers=8, thread_name_prefix='native-capacity') as pool:
                while True:
                    finish([future for future in pending if future.done()])
                    target = native_worker_target(config)
                    if external:
                        with closing(sqlite3.connect(path, timeout=5)) as db:
                            placeholders = ','.join('?' for _ in external)
                            external_states = dict(db.execute(
                                'SELECT id,status FROM deliveries WHERE id IN ('+placeholders+')', external))
                        if (len(external_states) != len(external)
                                or any(value not in {'uploading','complete'} for value in external_states.values())):
                            raise RuntimeError('prior delivery remains uncertain; new admission stopped')
                    active = queue.list(client.base_url, status='uploading')
                    active_ids = {job['id'] for job in active}
                    native_count = sum(bool(job['payload'].get('_native_claim')) for job in active)
                    legacy_count = len(active)-native_count
                    outside = len(active_ids.intersection(external))
                    memory.can_admit(native_count)
                    if not transition_owned:
                        try:
                            transition_lock.acquire()
                            transition_owned = True
                        except Timeout:
                            pass
                    if transition_owned and not worker_owned:
                        try:
                            worker_lock.acquire()
                            worker_owned = True
                            if outside:
                                raise RuntimeError('prior uploads survived their ownership locks; no recovery attempted')
                            ownership.remove()
                            gate.remove()
                        except Timeout:
                            pass
                    # Count all earlier native futures, not only this executor's.
                    # Up to eight deliveries overlap during the measured expansion.
                    while (native_count < target and len(active_ids) < 8
                           and memory.can_admit(native_count)):
                        job = gate.claim(queue, generation=generation,
                            guard_ids=external+[item['id'] for item in pending.values()])
                        if job is None:
                            break
                        future = pool.submit(deliver_job, queue, job, client, stop)
                        pending[future] = dict(id=job['id'], started_at=time.time())
                        active_ids.add(job['id'])
                        native_count += 1
                        idle_since = time.monotonic()
                    if pending:
                        finished, _ = wait(pending, timeout=.5, return_when=FIRST_COMPLETED)
                        finish(finished)
                        idle_since = time.monotonic()
                    else:
                        time.sleep(.5)
                    sample = memory.last_sample
                    available = sample.get('available_gib')
                    if available is not None:
                        report['minimum_available_gib'] = min(report.get('minimum_available_gib', available), available)
                    report.update(status='native'+str(target) if worker_owned else 'rolling',
                        target_native_slots=target, native_active=native_count, legacy_active=legacy_count,
                        external_active=outside, owned_native=len(pending), worker_lock_owned=worker_owned,
                        memory=sample)
                    if time.monotonic()-last_save >= 2:
                        save()
                        last_save = time.monotonic()
                    if (worker_owned and not pending and not queue.has_work(client.base_url)
                            and time.monotonic()-idle_since >= idle_seconds):
                        break
            report.update(status='complete', finished_at=time.time())
            save()
        except BaseException as exc:
            gate.install()
            ownership.install()
            report.update(status='blocked', error_type=type(exc).__name__, finished_at=time.time())
            save()
            raise
        finally:
            if worker_owned:
                worker_lock.release()
            if transition_owned:
                transition_lock.release()
    return report


def transition(root, client, *, idle_seconds=30):
    from preextract_batch_delivery import NativeMemoryGate, load_native_batch_config
    from preextract_delivery_worker import deliver_job
    root = Path(root)
    path = queue_path(root)
    output = path.with_name('native-transition.json')
    transition_lock = FileLock(str(path)+'.transition.lock', timeout=0)
    worker_lock = FileLock(str(path)+'.worker.lock', timeout=0)
    gate = NativeQueueGate(path, client.base_url)
    report = dict(schema='preextract-native-transition/1', started_at=time.time(),
                  target_native_slots=4, status='starting', completed=[], legacy_ids=[])

    def save():
        report['observed_at'] = time.time()
        temp = output.with_suffix('.tmp')
        temp.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf8')
        temp.replace(output)

    with transition_lock:
        client.native_batch_config = load_native_batch_config(root, client)
        if client.native_batch_config is None:
            raise ValueError('explicit verified native policy is required')
        memory = NativeMemoryGate(client.native_batch_config)
        memory.can_admit()
        # Closing old admission lowers load even when memory currently admits no native work.
        report['memory'] = memory.last_sample
        queue = DeliveryQueue(path)
        report['legacy_ids'] = gate.install()
        pending = {}
        stop = threading.Event()
        idle_since = time.monotonic()
        last_save = 0
        owned = False
        try:
            with ThreadPoolExecutor(max_workers=4, thread_name_prefix='native-transition') as pool:
                while True:
                    memory.can_admit(len(pending))
                    active = {job['id'] for job in queue.list(client.base_url, status='uploading')}
                    legacy = len(active.intersection(report['legacy_ids']))
                    if not owned:
                        try:
                            worker_lock.acquire()
                            owned = True
                            if legacy:
                                raise RuntimeError('legacy uploads survived their worker lock; no recovery attempted')
                            gate.remove()
                        except Timeout:
                            pass
                    capacity = min(4, max(0, 6-legacy))
                    while len(pending) < capacity and memory.can_admit(len(pending)):
                        job = gate.claim(queue)
                        if job is None:
                            break
                        future = pool.submit(deliver_job, queue, job, client, stop)
                        pending[future] = dict(id=job['id'], title=job['payload']['item'].get('title'),
                                               started_at=time.time())
                        idle_since = time.monotonic()
                    if pending:
                        finished, _ = wait(pending, timeout=.5, return_when=FIRST_COMPLETED)
                        for future in finished:
                            item = pending.pop(future)
                            future.result()
                            item.update(status=queue.get(item['id'])['status'],
                                        elapsed_seconds=time.time()-item['started_at'], finished_at=time.time())
                            report['completed'].append(item)
                            if item['status'] != 'complete':
                                raise RuntimeError('native delivery requires review; new admission stopped')
                        idle_since = time.monotonic()
                    else:
                        time.sleep(.5)
                    report.update(status='native4' if owned else 'legacy_draining', legacy_active=legacy,
                        native_active=len(pending), admitted_native_slots=capacity,
                        worker_lock_owned=owned, memory=memory.last_sample)
                    if time.monotonic()-last_save >= 2:
                        save()
                        last_save = time.monotonic()
                    if (owned and not pending and not queue.has_work(client.base_url)
                            and time.monotonic()-idle_since >= idle_seconds):
                        break
            report.update(status='complete', finished_at=time.time(), worker_lock_owned=owned)
            save()
        except BaseException as exc:
            gate.install()
            report.update(status='blocked', error_type=type(exc).__name__, finished_at=time.time())
            save()
            # Keep the admission gate on a failed migration; never reopen the old six.
            raise
        finally:
            if owned:
                worker_lock.release()
    return report


def main():
    import argparse
    import os
    from auto_config import load_key
    from ragflow_client import RagflowClient
    parser = argparse.ArgumentParser()
    parser.add_argument('--data-root', type=Path, required=True)
    parser.add_argument('--adaptive', action='store_true')
    args = parser.parse_args()
    root = args.data_root.resolve()
    settings = json.loads((root/'settings.json').read_text(encoding='utf8'))
    key = os.environ.get('RAGFLOW_API_KEY') or load_key(root, 'ragflow')
    runner = expand_native_queue if args.adaptive else transition
    runner(root, RagflowClient(settings['ragflow_url'], key))


if __name__ == '__main__':
    main()

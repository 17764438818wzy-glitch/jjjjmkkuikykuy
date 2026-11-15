"""Inert candidate adapter for verified manual-retry notes in the LLM list.

Importing or constructing starts no threads or business work. A list read can
start bounded background observers; it never changes task state or calls models.
The caller must supply the existing runtime's full ``verify_upstream`` callback;
ordinary result pages remain owned by the original observer. This module is
not connected to the live application.
"""
from contextlib import closing
from collections import deque
from concurrent.futures import Future
import copy
from pathlib import Path
import re
import sqlite3
import threading
import time

from stage_runtime import FIELDS, _identity, _retry_source_snapshot_db


class RetryNoteObserver:
    """Add a note only after the exact original folder and retry are verified."""
    def __init__(self, observer, database, *, verify_upstream):
        if not callable(getattr(observer, 'llm_results', None)) or not callable(verify_upstream):
            raise ValueError('original LLM observer and source verifier are required')
        self.observer = observer
        self.database = Path(database).resolve(strict=True)
        self.verify_upstream = verify_upstream
        stat = self.database.stat()
        self._file_identity = (stat.st_dev, stat.st_ino)
        self._lock = threading.Lock()
        self._revision_db = sqlite3.connect(self.database.as_uri() + '?mode=ro', uri=True,
                                            timeout=.05, check_same_thread=False)
        self._condition = threading.Condition(self._lock)
        self._queue = deque()
        self._workers_started = False
        self._cursor = 0
        self._pending = {}
        self._closed = False

    def __getattr__(self, name):
        return getattr(self.observer, name)

    def close(self):
        """Stop scheduling observations; callers close this at page shutdown."""
        with self._condition:
            if self._closed:
                return
            self._closed = True
            for future, _, _ in self._queue:
                future.cancel()
            self._queue.clear()
            self._revision_db.close()
            self._condition.notify_all()

    def _worker(self):
        while True:
            with self._condition:
                while not self._queue and not self._closed:
                    self._condition.wait()
                if self._closed:
                    return
                future, _, item = self._queue.popleft()
                if not future.set_running_or_notify_cancel():
                    continue
            try:
                revision = self._revision()
                value = self._note(item)
                # A stable negative observation may be cached briefly too, so
                # ordinary non-retry rows cannot monopolize the observer.
                if value is None and revision is not None and self._revision() == revision:
                    value = (None, revision, time.monotonic())
                future.set_result(value)
            except BaseException as exc:
                future.set_exception(exc)

    def _start_workers(self):
        # Called under _condition. Hung filesystem I/O must not keep the page
        # service process alive after shutdown.
        if not self._workers_started:
            self._workers_started = True
            for number in range(2):
                threading.Thread(target=self._worker, daemon=True,
                                 name='retry-note-observer-' + str(number)).start()

    def _revision(self):
        try:
            stat = self.database.stat()
            if (stat.st_dev, stat.st_ino) != self._file_identity:
                return None
            with self._lock:
                if self._closed:
                    return None
                return self._revision_db.execute('PRAGMA data_version').fetchone()[0]
        except (OSError, sqlite3.Error):
            return None

    def _snapshot(self, item):
        if not isinstance(item, dict):
            return None
        key = (item.get('task_id'), item.get('group_id'))
        if not all(isinstance(part, str) and part for part in key):
            return None
        with closing(sqlite3.connect(self.database.as_uri() + '?mode=ro', uri=True, timeout=5)) as db:
            db.row_factory = sqlite3.Row
            db.execute('BEGIN')
            row = db.execute('SELECT * FROM llm_folder_queue WHERE task_id=? AND group_id=?', key).fetchone()
            task = db.execute('SELECT * FROM tasks WHERE task_id=?', (key[0],)).fetchone()
            if (not row or not task or db.execute('SELECT 1 FROM task_deletions WHERE task_id=?', (key[0],)).fetchone()
                    or row['state'] != 'waiting_postprocessing'
                    or any(item.get(name) != row[name]
                           for name in ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id'))
                    or task['prefect_order_model'] != 'v2' or task['prefect_run_id'] != row['parent_run_id']
                    or task['desired_state'] not in {'running', 'paused'}
                    or task['task_status'] not in {'ready', 'running', 'pausing', 'paused', 'complete', 'needs_review'}
                    or task['start_pending'] or type(task['start_generation']) is not int
                    or type(row['generation']) is not int or not 1 <= row['generation'] <= task['start_generation']
                    or not re.fullmatch('[0-9a-f]{64}', str(row['manifest_sha256']))):
                return None
            for table in ('paperflow_postprocess_stage_jobs', 'postprocessing_claims', 'postprocessing_results'):
                # Missing tables are not proof that no postprocessing exists.
                if db.execute('SELECT 1 FROM ' + table + ' WHERE task_id=? AND group_id=? LIMIT 1', key).fetchone():
                    return None
            identity = _identity({name: row[name] for name in FIELDS})
            selected = _retry_source_snapshot_db(db, identity, row['generation'])
            if selected is None:
                return None
            return dict(identity=identity, generation=task['start_generation'],
                        source_generation=row['generation'], manifest_sha256=row['manifest_sha256'],
                        retry_sha256=selected['sha256'], desired_state=task['desired_state'],
                        task_status=task['task_status'])

    def _note(self, item):
        try:
            revision = self._revision()
            if revision is None:
                return None
            before = self._snapshot(item)
            if before is None:
                return None
            evidence = self.verify_upstream(dict(before['identity']), before['generation'])
            prefect_proof = evidence.get('source_retry_prefect_sha256') if isinstance(evidence, dict) else None
            if (not isinstance(evidence, dict) or evidence.get('identity') != before['identity']
                    or type(evidence.get('generation')) is not int
                    or evidence.get('generation') != before['generation']
                    or type(evidence.get('source_llm_generation')) is not int
                    or evidence.get('source_llm_generation') != before['source_generation']
                    or evidence.get('manifest_sha256') != before['manifest_sha256']
                    or evidence.get('source_retry_sha256') != before['retry_sha256']
                    or not isinstance(prefect_proof, str) or not re.fullmatch('[0-9a-f]{64}', prefect_proof)
                    or evidence.get('raw_complete') is not True or evidence.get('session_current') is not True
                    or evidence.get('not_deleted') is not True or self._snapshot(item) != before
                    or self._revision() != revision):
                return None
            if before['desired_state'] == 'paused' and before['task_status'] == 'paused':
                note = '人工重试已完成，核验时任务已暂停；原失败记录已保留'
            elif before['desired_state'] == 'paused' or before['task_status'] in {'pausing', 'paused'}:
                note = '人工重试已完成，核验时任务状态正在切换；原失败记录已保留'
            else:
                note = '人工重试已完成，核验时等待后处理；原失败记录已保留'
            return note, revision, time.monotonic()
        except Exception:
            return None

    def _cached_note(self, item, revision):
        key = tuple(item.get(name) for name in ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id'))
        if revision is None or not all(isinstance(value, str) and value for value in key):
            return None
        with self._lock:
            future = self._pending.get(key)
        if future is not None and future.done():
            try:
                value = future.result()
            except Exception:
                value = None
            if value is not None and value[1] == revision and 0 <= time.monotonic() - value[2] <= 15:
                return value[0]
            with self._lock:
                if self._pending.get(key) is future:
                    del self._pending[key]
        with self._condition:
            # Bound both threads and pending work; a slow verifier never holds
            # up the original LLM result page or grows an unbounded queue.
            if not self._closed and key not in self._pending:
                if len(self._pending) >= 100:
                    for previous, finished in list(self._pending.items()):
                        if finished.done():
                            del self._pending[previous]
                            break
                if len(self._pending) < 100:
                    future = Future()
                    self._pending[key] = future
                    self._queue.append((future, key, copy.deepcopy(item)))
                    self._start_workers()
                    self._condition.notify()
        return None

    def llm_results(self, snapshot, result_filter=None, page=1, page_size=20):
        result = self.observer.llm_results(snapshot, result_filter, page, page_size)
        if (not isinstance(result, dict) or result.get('available') is not True
                or result_filter != 'passed' or not isinstance(result.get('list'), dict)
                or not isinstance(result['list'].get('items'), list)):
            return result
        original = result
        result = copy.deepcopy(result)
        items = result['list']['items']
        if len(items) > 100:
            return original
        revision = self._revision()
        changed = False
        with self._condition:
            # A page does not identify a browser session. Keep other pages'
            # queued observations so separate clients cannot cancel them.
            offset = self._cursor % len(items) if items else 0
            self._cursor = (offset + 20) % len(items) if items else 0
        for index in list(range(offset, len(items))) + list(range(offset)):
            item = items[index]
            note = self._cached_note(item, revision) if isinstance(item, dict) else None
            if note is not None:
                previous = item.get('order_note')
                if previous is not None and not isinstance(previous, str):
                    continue
                item['order_note'] = (previous + '；' + note) if previous else note
                changed = True
        if changed and self._revision() != revision:
            # Do not combine notes observed across a changed local snapshot.
            return original
        return result

"""Explicit first-step discovery; importing this module starts no worker."""

import ipaddress
import json
import math
from pathlib import Path
import sqlite3
from threading import Event, Thread, get_ident
from uuid import UUID
from urllib.error import HTTPError
from urllib.parse import quote, urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from filelock import FileLock, Timeout
from local_queue import RetentionIdentityConflict


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, msg, headers, newurl):
        return None


class _ScanStopped(Exception):
    """A cooperative stop requested between pages or groups."""


def _check_stop(stop_event):
    if stop_event is not None and stop_event.is_set():
        raise _ScanStopped()


class FirstStepHTTPSource:
    """GET the first-step local API when the caller explicitly asks to scan."""

    def __init__(self, base_url='http://127.0.0.1:14203', *, page_size=200, timeout=5):
        parsed = urlsplit(base_url)
        try:
            loopback = parsed.hostname == 'localhost' or ipaddress.ip_address(parsed.hostname).is_loopback
        except ValueError:
            loopback = False
        if (parsed.scheme != 'http' or not loopback or parsed.username or parsed.password
                or parsed.path not in ('', '/') or parsed.query or parsed.fragment):
            raise ValueError('第一步接口必须是本机 HTTP 地址')
        if type(page_size) is not int or not 1 <= page_size <= 200 or timeout <= 0:
            raise ValueError('第一步接口分页或超时无效')
        self.base_url = base_url.rstrip('/')
        self.page_size = page_size
        self.timeout = timeout
        self._opener = build_opener(ProxyHandler({}), _NoRedirect())
        self.expected_index = None
        self.expected_queue = None
        self.expected_prefect_api = None

    def verified_binding(self, receipt_root):
        """Verify the running API is the configured source for this receiver."""
        if not all((self.expected_index, self.expected_queue, self.expected_prefect_api)):
            return None
        payload = self._request(self.base_url + '/health')
        instance = payload.get('instance_id')
        try:
            if str(UUID(instance)) != instance:
                return None
            if (payload.get('status') != 'ok' or payload.get('scope') != 'first-step-summary'
                    or payload.get('first_step_order_model') != 'v2'
                    or payload.get('prefect_api_url', '').rstrip('/') != self.expected_prefect_api.rstrip('/')
                    or not Path(payload['receipt_root']).samefile(Path(receipt_root).resolve(strict=True))
                    or not Path(payload['summary_index']).samefile(self.expected_index)
                    or not Path(payload['local_intake_queue_db']).samefile(self.expected_queue)):
                return None
        except (OSError, ValueError, TypeError, KeyError, AttributeError):
            return None
        return instance

    def _get(self, path, query):
        url = self.base_url + path + '?' + urlencode(query)
        return self._request(url)

    def _post(self, path, payload):
        body = json.dumps(payload, ensure_ascii=False).encode('utf-8')
        request = Request(self.base_url + path, data=body,
                          headers={'Content-Type': 'application/json'}, method='POST')
        return self._request(request)

    def _request(self, request):
        with self._opener.open(request, timeout=self.timeout) as response:
            raw = response.read(8 * 1024 * 1024 + 1)
        if len(raw) > 8 * 1024 * 1024:
            raise ValueError('第一步接口响应过大')
        payload = json.loads(raw.decode('utf-8'))
        if not isinstance(payload, dict):
            raise ValueError('第一步接口响应无效')
        return payload

    def list_tasks(self, page):
        return self._get('/api/v1/first-step/tasks', {'page': page, 'page_size': self.page_size})

    def list_groups(self, task_id, page):
        path = '/api/v1/first-step/tasks/' + quote(task_id, safe='')
        return self._get(path, {'filter': 'passed', 'page': page, 'page_size': self.page_size})

    def reserve_retention(self, task_id, item, *, consumer_id, request_id):
        path = '/api/v1/first-step/tasks/' + quote(task_id, safe='') + '/local-retentions'
        return self._post(path, {'group_id': item['group_id'], 'paper_id': item['paper_id'],
                                 'receipt_id': item['receipt_id'], 'consumer_id': consumer_id,
                                 'request_id': request_id})

    def list_retentions(self, task_id, *, stop_event=None):
        path = '/api/v1/first-step/tasks/' + quote(task_id, safe='') + '/local-retentions'

        def collect():
            page, expected = 1, None
            items, ids = [], set()
            while True:
                _check_stop(stop_event)
                payload = self._get(path, {'page': page, 'page_size': self.page_size})
                _check_stop(stop_event)
                total, size, revision = (payload.get('list_total'), payload.get('page_size'),
                                         payload.get('revision'))
                rows = payload.get('items')
                if (payload.get('task_id') != task_id or type(payload.get('page')) is not int
                        or payload['page'] != page or type(total) is not int or total < 0
                        or type(size) is not int or not 1 <= size <= self.page_size
                        or type(revision) is not int or revision < 0
                        or type(payload.get('has_next')) is not bool or not isinstance(rows, list)):
                    raise ValueError('上游接收保护分页无效')
                current = (total, size, revision)
                if expected is None:
                    expected = current
                elif current != expected:
                    raise ValueError('上游接收保护分页期间发生变化')
                count = max(0, min(size, total - (page - 1) * size))
                if len(rows) != count or payload['has_next'] != (page * size < total):
                    raise ValueError('上游接收保护分页不完整')
                for row in rows:
                    ident = row.get('retention_id') if isinstance(row, dict) else None
                    if not isinstance(ident, str) or not ident or ident in ids:
                        raise ValueError('上游接收保护编号缺失或重复')
                    ids.add(ident)
                    items.append(row)
                if not payload['has_next']:
                    return revision, items
                if page >= 100000:
                    raise ValueError('上游接收保护分页超限')
                page += 1

        first_revision, first = collect()
        second_revision, second = collect()
        if first_revision != second_revision or first != second:
            raise ValueError('上游接收保护两次读取不一致')
        return {'task_id': task_id, 'items': second, 'revision': second_revision}

    def confirm_retention(self, task_id, retention_id, *, consumer_id, receipt_id):
        path = ('/api/v1/first-step/tasks/' + quote(task_id, safe='') + '/local-retentions/'
                + quote(retention_id, safe='') + '/confirm')
        return self._post(path, {'consumer_id': consumer_id, 'receipt_id': receipt_id})


def _scan(source, *, on_group=None, stop_event=None):
    """Enumerate one set of API pages; duplicate or inconsistent pages fail."""
    tasks = {}
    inconsistent = False
    page = 1
    expected_total = expected_size = None
    while True:
        _check_stop(stop_event)
        try:
            payload = source.list_tasks(page)
            if not isinstance(payload, dict) or not isinstance(payload.get('items'), list):
                raise ValueError('第一步任务列表无效')
            if type(payload.get('has_next')) is not bool:
                raise ValueError('第一步任务分页状态无效')
            total, size = payload.get('total'), payload.get('page_size')
            if (payload.get('page') != page or type(total) is not int or total < 0
                    or type(size) is not int or size <= 0):
                raise ValueError('第一步任务分页数量无效')
            if page == 1:
                expected_total, expected_size = total, size
            elif (total, size) != (expected_total, expected_size):
                inconsistent = True
            expected = max(0, min(size, total - (page - 1) * size))
            if len(payload['items']) != expected or payload['has_next'] != (page * size < total):
                raise ValueError('第一步任务分页不完整')
            for task in payload['items']:
                task_id = task.get('task_id') if isinstance(task, dict) else None
                if not isinstance(task_id, str) or not task_id or task_id in tasks:
                    inconsistent = True
                    continue
                tasks[task_id] = task.get('control_revision')
            if not payload['has_next']:
                break
            if not payload['items'] or page >= 100000:
                raise ValueError('第一步任务分页不完整')
            page += 1
        except (OSError, ValueError, KeyError, TypeError):
            # Keep the already discovered tasks, but never report an exhaustive scan.
            inconsistent = True
            break

    groups = {}
    for task_id in sorted(tasks):
        _check_stop(stop_event)
        try:
            page = 1
            expected_total = expected_size = expected_revision = None
            while True:
                _check_stop(stop_event)
                payload = source.list_groups(task_id, page)
                if (not isinstance(payload, dict) or payload.get('task_id') != task_id
                        or payload.get('page') != page or not isinstance(payload.get('items'), list)):
                    raise ValueError('第一步资料组分页无效')
                total, size, revision = (payload.get('list_total'), payload.get('page_size'),
                                         payload.get('control_revision'))
                if type(total) is not int or total < 0 or type(size) is not int or size <= 0:
                    raise ValueError('第一步资料组分页数量无效')
                if page == 1:
                    expected_total, expected_size, expected_revision = total, size, revision
                elif (total, size, revision) != (expected_total, expected_size, expected_revision):
                    # Each returned row can still be verified, but this is not a complete snapshot.
                    inconsistent = True
                rows = payload['items']
                expected = max(0, min(size, total - (page - 1) * size))
                if len(rows) != expected:
                    raise ValueError('第一步资料组分页不完整')
                for row in rows:
                    group_id = row.get('group_id') if isinstance(row, dict) else None
                    key = (task_id, group_id)
                    if not isinstance(group_id, str) or not group_id:
                        raise ValueError('第一步资料组身份缺失')
                    if key in groups:
                        inconsistent = True
                        continue
                    groups[key] = row
                    if on_group is not None:
                        on_group(key, row)
                if page * size >= total:
                    break
                if page >= 100000:
                    raise ValueError('第一步资料组分页超限')
                page += 1
        except (OSError, ValueError, KeyError, TypeError):
            # Other tasks can still provide individually verifiable groups.
            inconsistent = True
    if inconsistent:
        raise ValueError('第一步资料组分页不完整或期间发生变化')
    return tasks, groups


def _signature(scan):
    tasks, groups = scan
    fields = ('group_id', 'status', 'registration_status', 'delivery_complete',
              'paper_id', 'receipt_id', 'receipt_path', 'delivery_dir')
    return (tuple(sorted(tasks.items())),
            tuple((key, tuple(groups[key].get(field) for field in fields)) for key in sorted(groups)))


def _eligible(scan):
    return {key: row for key, row in scan[1].items()
            if (row.get('status') == 'passed' and row.get('registration_status') == 'registered'
                and row.get('delivery_complete') is True)}


def _is_eligible(row):
    return (row.get('status') == 'passed' and row.get('registration_status') == 'registered'
            and row.get('delivery_complete') is True)


def _refresh_retentions(queue, retention_client, *, stop_event=None):
    if retention_client is None or not hasattr(queue, 'retained_tasks'):
        return 0
    failures = 0
    for task_id in queue.retained_tasks():
        _check_stop(stop_event)
        try:
            response = retention_client.list_retentions(task_id, stop_event=stop_event)
            _check_stop(stop_event)
            failures += queue.reconcile_retention(task_id, response)
        except (OSError, ValueError, KeyError, TypeError, AttributeError):
            queue.retention_unavailable(task_id)
            failures += 1
    return failures


def _receive_rows(groups, queue, receipt_root, *, stop_event=None, retention_client=None):
    failures = 0
    for (task_id, _), item in groups.items():
        _check_stop(stop_event)
        intent = None
        if hasattr(queue, 'check_source_identity') and not queue.check_source_identity(task_id, item):
            failures += 1
            continue
        if retention_client is not None:
            try:
                intent = queue.prepare_retention(task_id, item)
                if intent['state'] == 'intent':
                    response = retention_client.reserve_retention(
                        task_id, item, consumer_id=intent['consumer_id'], request_id=intent['request_id'])
                    intent = queue.record_retention(intent, response)
            except (OSError, ValueError, KeyError, TypeError) as exc:
                if intent is not None:
                    # A 404 may be an older upstream without this route; keep the idempotency key.
                    invalid = isinstance(exc, RetentionIdentityConflict)
                    queue.retention_error(intent, exc, invalid=invalid)
                failures += 1
                continue
        try:
            result = queue.receive(task_id, item, receipt_root=receipt_root)
        except (OSError, ValueError, KeyError, TypeError):
            failures += 1
            continue
        if isinstance(result, dict) and result.get('state') == 'needs_review':
            failures += 1
            continue
        if intent is not None and intent['state'] != 'accepted':
            _check_stop(stop_event)
            try:
                response = retention_client.confirm_retention(
                    task_id, intent['retention_id'], consumer_id=intent['consumer_id'],
                    receipt_id=intent['receipt_id'])
                queue.record_retention(intent, response, confirmed=True)
            except (OSError, ValueError, KeyError, TypeError) as exc:
                invalid = isinstance(exc, RetentionIdentityConflict)
                queue.retention_error(intent, exc, invalid=invalid, phase='confirm')
                failures += 1
                continue
        on_ready = getattr(queue, 'on_receipt_ready', None)
        if on_ready is not None and result.get('state') == 'waiting':
            try:
                on_ready(task_id, item['group_id'])
            except Exception:
                # Keep the verified receipt and retry the same durable identity
                # on a later scan; never turn a submit ambiguity into a new order.
                failures += 1
    return failures


def discover_once(source, queue, *, receipt_root, stop_event=None, retention_client=None, _watch_held=False):
    """Admit verified groups incrementally; reconcile only a stable scan."""
    one_shot_guard = None
    if not _watch_held:
        one_shot_guard = FileLock(str(queue.path) + '.watch.lock', timeout=0)
        try:
            one_shot_guard.acquire()
        except Timeout:
            return {'complete': False, 'reason': 'watch_in_progress'}
    try:
        _check_stop(stop_event)
        queue.record_scan(complete=False, reason='扫描进行中')
        retention_failures = _refresh_retentions(queue, retention_client, stop_event=stop_event)
        baseline = {(row['task_id'], row['group_id'], row['receipt_id'])
                    for row in queue.list_waiting()}
        try:
            root = Path(receipt_root).resolve(strict=True)
            if not root.is_dir():
                raise ValueError('登记目录不可用')
            binding_before = None
            if hasattr(source, 'verified_binding'):
                try:
                    binding_before = source.verified_binding(root)
                except (OSError, ValueError, KeyError, TypeError, HTTPError):
                    pass
            if binding_before:
                queue.record_scan(complete=False, reason='扫描进行中', source_instance=binding_before)
            collected = {}
            failures = 0

            def collect_discovered(key, item):
                nonlocal failures
                if _is_eligible(item):
                    collected[key] = item
                    failures += _receive_rows({key: item}, queue, root, stop_event=stop_event,
                                              retention_client=retention_client)

            # Accept each verified folder as soon as it is discovered; a later
            # page or incomplete scan must not hold up earlier folders.
            try:
                first = _scan(source, on_group=collect_discovered, stop_event=stop_event)
            except (OSError, ValueError, KeyError, TypeError):
                retention_failures += _refresh_retentions(queue, retention_client, stop_event=stop_event)
                raise
            retention_failures += _refresh_retentions(queue, retention_client, stop_event=stop_event)
            eligible = _eligible(first)
            def check_second(key, item):
                previous = first[1].get(key)
                if previous is None or not _is_eligible(previous):
                    return
                if hasattr(queue, 'check_source_identity'):
                    queue.check_source_identity(key[0], item)
                if not _is_eligible(item) and isinstance(previous.get('receipt_id'), str):
                    queue.reconcile_visible(set(), candidates={(key[0], key[1], previous['receipt_id'])})

            second = _scan(source, on_group=check_second, stop_event=stop_event)
            if hasattr(queue, 'reconcile_visible'):
                still_eligible = _eligible(second)
                absent = {(task_id, group_id, item['receipt_id'])
                          for (task_id, group_id), item in eligible.items()
                          if (task_id, group_id) not in still_eligible and isinstance(item.get('receipt_id'), str)}
                if absent:
                    queue.reconcile_visible(set(), candidates=absent)
            if _signature(first) != _signature(second):
                newly_seen = {key: row for key, row in _eligible(second).items() if key not in eligible}
                _receive_rows(newly_seen, queue, root, stop_event=stop_event, retention_client=retention_client)
                if newly_seen:
                    retention_failures += _refresh_retentions(queue, retention_client, stop_event=stop_event)
                raise ValueError('核验期间上游结果发生变化')
            visible = {(task_id, group_id, item['receipt_id'])
                       for (task_id, group_id), item in eligible.items()
                       if isinstance(item.get('receipt_id'), str)}
            _check_stop(stop_event)
            missing = queue.reconcile_visible(visible, candidates=baseline)
            if failures or missing or retention_failures:
                queue.record_scan(complete=False, reason='部分接收核验失败或已登记资料不可用')
                return {'complete': False, 'eligible_count': len(eligible),
                        'verification_failures': failures, 'missing_count': len(missing),
                        'retention_failures': retention_failures}
            binding_after = None
            if hasattr(source, 'verified_binding'):
                try:
                    binding_after = source.verified_binding(root)
                except (OSError, ValueError, KeyError, TypeError, HTTPError):
                    pass
            if binding_before and binding_before == binding_after:
                _check_stop(stop_event)
                queue.record_scan(complete=True, source_instance=binding_before)
                return {'complete': True, 'eligible_count': len(eligible),
                        'verification_failures': 0, 'missing_count': 0}
            if not eligible or getattr(source, 'expected_index', None) is not None:
                queue.record_scan(complete=False, reason='上游与登记目录绑定未确认')
                return {'complete': False, 'reason': 'source_binding_unconfirmed',
                        'eligible_count': len(eligible)}
            _check_stop(stop_event)
            queue.record_scan(complete=True)
            return {'complete': True, 'eligible_count': len(eligible),
                    'verification_failures': 0, 'missing_count': 0}
        except (OSError, ValueError, KeyError, TypeError, HTTPError) as exc:
            queue.record_scan(complete=False, reason=type(exc).__name__)
            return {'complete': False, 'reason': type(exc).__name__}
    finally:
        if one_shot_guard is not None:
            one_shot_guard.release()


def poll_until_stopped(source, queue, *, receipt_root, stop_event, interval_seconds=30,
                       on_result=None, retention_client=None):
    """Repeated discovery only when explicitly started by a caller."""
    if (isinstance(interval_seconds, bool) or not isinstance(interval_seconds, (int, float))
            or not math.isfinite(interval_seconds) or interval_seconds <= 0):
        raise ValueError('扫描间隔无效')
    worker_lock = FileLock(str(queue.path) + '.watch.lock', timeout=0) if hasattr(queue, 'path') else None
    if worker_lock is not None:
        try:
            worker_lock.acquire()
        except Timeout as exc:
            raise RuntimeError('本地接收进程已经在运行') from exc
    done = Event()
    heartbeat_thread = None
    heartbeat_started = False
    receiver_session_id = None
    def record_receiver(state):
        if hasattr(queue, 'receiver_session_id'):
            queue.record_receiver(state, expected_session=receiver_session_id)
        else:
            queue.record_receiver(state)

    def heartbeat():
        while not done.is_set():
            try:
                record_receiver('running')
            except (sqlite3.Error, OSError, ValueError):
                pass
            if done.wait(10):
                break

    normal_stop = False
    try:
        if hasattr(queue, 'watch_owner_thread'):
            queue.watch_owner_thread = get_ident()
        if hasattr(queue, 'start_receiver'):
            queue.start_receiver()
            receiver_session_id = queue.receiver_session_id
        if hasattr(queue, 'record_receiver'):
            heartbeat_thread = Thread(target=heartbeat, daemon=True, name='local-intake-heartbeat')
            heartbeat_thread.start()
            heartbeat_started = True
        while not stop_event.is_set():
            try:
                result = discover_once(source, queue, receipt_root=receipt_root, stop_event=stop_event,
                                       retention_client=retention_client, _watch_held=worker_lock is not None)
            except _ScanStopped:
                break
            except sqlite3.Error:
                result = {'complete': False, 'reason': 'local_state_unavailable'}
            if on_result is not None:
                on_result(result)
            if stop_event.wait(interval_seconds):
                break
        normal_stop = True
    finally:
        done.set()
        try:
            if heartbeat_started:
                try:
                    heartbeat_thread.join(timeout=2)
                    record_receiver('stopped' if normal_stop else 'interrupted')
                except (sqlite3.Error, OSError, RuntimeError, ValueError):
                    pass
        finally:
            if hasattr(queue, 'receiver_session_id'):
                queue.receiver_session_id = None
            if hasattr(queue, 'watch_owner_thread'):
                queue.watch_owner_thread = None
            if worker_lock is not None:
                worker_lock.release()

"""Explicit first-step discovery; importing this module starts no worker."""

import ipaddress
import json
import math
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import quote, urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, build_opener

from filelock import FileLock, Timeout


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
        self._opener = build_opener(_NoRedirect())

    def _get(self, path, query):
        url = self.base_url + path + '?' + urlencode(query)
        with self._opener.open(url, timeout=self.timeout) as response:
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


def _receive_rows(groups, queue, receipt_root, *, stop_event=None):
    failures = 0
    for (task_id, _), item in groups.items():
        _check_stop(stop_event)
        try:
            queue.receive(task_id, item, receipt_root=receipt_root)
        except (OSError, ValueError, KeyError, TypeError):
            failures += 1
    return failures


def discover_once(source, queue, *, receipt_root, stop_event=None):
    """Admit verified groups incrementally; reconcile only a stable scan."""
    lock = FileLock(str(queue.path) + '.scan.lock', timeout=0)
    try:
        lock.acquire()
    except Timeout:
        return {'complete': False, 'reason': 'scan_in_progress'}
    try:
        _check_stop(stop_event)
        queue.record_scan(complete=False, reason='扫描进行中')
        baseline = {(row['task_id'], row['group_id'], row['receipt_id'])
                    for row in queue.list_waiting()}
        try:
            root = Path(receipt_root).resolve(strict=True)
            if not root.is_dir():
                raise ValueError('登记目录不可用')
            collected = {}

            def collect_discovered(key, item):
                if _is_eligible(item):
                    collected[key] = item

            # Enumerate before hashing files so expensive verification does not delay later pages.
            # Even an incomplete scan can safely offer its individually verified rows for reception.
            try:
                first = _scan(source, on_group=collect_discovered, stop_event=stop_event)
            except (OSError, ValueError, KeyError, TypeError):
                _receive_rows(collected, queue, root, stop_event=stop_event)
                raise
            failures = _receive_rows(collected, queue, root, stop_event=stop_event)
            eligible = _eligible(first)
            second = _scan(source, stop_event=stop_event)
            if _signature(first) != _signature(second):
                newly_seen = {key: row for key, row in _eligible(second).items() if key not in eligible}
                _receive_rows(newly_seen, queue, root, stop_event=stop_event)
                raise ValueError('核验期间上游结果发生变化')
            visible = {(task_id, group_id, item['receipt_id'])
                       for (task_id, group_id), item in eligible.items()
                       if isinstance(item.get('receipt_id'), str)}
            _check_stop(stop_event)
            missing = queue.reconcile_visible(visible, candidates=baseline)
            if failures or missing:
                queue.record_scan(complete=False, reason='部分接收核验失败或已登记资料不可用')
                return {'complete': False, 'eligible_count': len(eligible),
                        'verification_failures': failures, 'missing_count': len(missing)}
            if not eligible:
                queue.record_scan(complete=False, reason='空来源无法核实上游与登记目录的绑定')
                return {'complete': False, 'reason': 'source_binding_unconfirmed'}
            _check_stop(stop_event)
            queue.record_scan(complete=True)
            return {'complete': True, 'eligible_count': len(eligible),
                    'verification_failures': 0, 'missing_count': 0}
        except (OSError, ValueError, KeyError, TypeError, HTTPError) as exc:
            queue.record_scan(complete=False, reason=type(exc).__name__)
            return {'complete': False, 'reason': type(exc).__name__}
    finally:
        lock.release()


def poll_until_stopped(source, queue, *, receipt_root, stop_event, interval_seconds=30,
                       on_result=None):
    """Repeated discovery only when explicitly started by a caller."""
    if (isinstance(interval_seconds, bool) or not isinstance(interval_seconds, (int, float))
            or not math.isfinite(interval_seconds) or interval_seconds <= 0):
        raise ValueError('扫描间隔无效')
    while not stop_event.is_set():
        try:
            result = discover_once(source, queue, receipt_root=receipt_root, stop_event=stop_event)
        except _ScanStopped:
            break
        if on_result is not None:
            on_result(result)
        if stop_event.wait(interval_seconds):
            break

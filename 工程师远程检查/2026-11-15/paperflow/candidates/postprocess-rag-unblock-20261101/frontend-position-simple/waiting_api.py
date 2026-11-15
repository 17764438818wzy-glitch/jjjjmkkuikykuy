"""Frontend-facing waiting-count API; constructing it starts no intake work."""

from collections import OrderedDict
import copy
from datetime import datetime, timezone
import hashlib
import json
import math
import re
import sqlite3
from threading import Event, Lock, Thread
import time
from typing import Literal

from fastapi import FastAPI, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse


STAGE_KEYS = ('waiting_postprocessing', 'postprocessing_in_progress', 'waiting_rag_upload')
STAGE_DIAGNOSTICS = ('postprocessing_held', 'postprocessing_unresolved')
REGISTERED_BASIS = 'postprocessing-registered-ledger/1'
PROJECTION_REFRESH_SECONDS = 4
PROJECTION_MAX_AGE_SECONDS = 8
GUARDED_PROJECTION_REFRESH_SECONDS = 15
GUARDED_PROJECTION_MAX_AGE_SECONDS = 45
GUARDED_LOCAL_REFRESH_SECONDS = 2
GUARDED_LOCAL_MAX_AGE_SECONDS = 8
GUARDED_REMOTE_REFRESH_SECONDS = 3
GUARDED_REMOTE_MAX_AGE_SECONDS = 15
projection_clock = time.monotonic


def _snapshot_key(snapshot):
    return tuple(snapshot.get(name) for name in (
        'receiver_status', 'receiver_session_id', 'scan_source_instance',
        'waiting_available', 'upstream_scan_verified', 'upstream_scan_completed_at', 'waiting_llm_total'))


def _usable_source(snapshot):
    return (snapshot.get('receiver_status') == 'running'
            and isinstance(snapshot.get('receiver_session_id'), str) and bool(snapshot['receiver_session_id'])
            and isinstance(snapshot.get('scan_source_instance'), str) and bool(snapshot['scan_source_instance'])
            and snapshot.get('waiting_available') is True
            and snapshot.get('upstream_scan_verified') is True
            and _valid_verification_time(snapshot.get('upstream_scan_completed_at'))
            and type(snapshot.get('waiting_llm_total')) is int and snapshot['waiting_llm_total'] >= 0)


def _usable_subset_source(snapshot):
    return (isinstance(snapshot, dict) and snapshot.get('receiver_status') == 'running'
            and isinstance(snapshot.get('receiver_session_id'), str) and bool(snapshot['receiver_session_id'])
            and isinstance(snapshot.get('scan_source_instance'), str) and bool(snapshot['scan_source_instance']))


def _valid_count(value):
    return value is None or (type(value) is int and value >= 0)


def _valid_verification_time(value):
    if not isinstance(value, str):
        return False
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        return parsed.utcoffset() is not None
    except (TypeError, ValueError, OverflowError):
        return False


def _valid_stage(value):
    if (not isinstance(value, dict) or value.get('available') is not True
            or value.get('basis') != 'folder-stage-receipts/1'
            or not _valid_verification_time(value.get('as_of'))
            or not isinstance(value.get('counts'), dict)):
        return False
    counts = value['counts']
    return (all(key in counts and _valid_count(counts[key]) for key in STAGE_KEYS)
            and all(_valid_count(counts[key]) for key in STAGE_DIAGNOSTICS if key in counts))


def _valid_results(value, result_filter, page, page_size):
    if (not isinstance(value, dict) or value.get('available') is not True
            or not _valid_verification_time(value.get('as_of'))
            or not isinstance(value.get('counts'), dict) or type(value.get('partial')) is not bool):
        return False
    counts = value['counts']
    if not all(key in counts and _valid_count(counts[key]) for key in ('passed', 'failed', 'unrecognized')):
        return False
    if result_filter is None:
        return True
    rows = value.get('list')
    return (isinstance(rows, dict) and rows.get('filter') == result_filter
            and type(rows.get('page')) is int and rows['page'] == page
            and type(rows.get('page_size')) is int and rows['page_size'] == page_size
            and _valid_count(rows.get('total')) and 'total' in rows
            and isinstance(rows.get('items'), list) and len(rows['items']) <= page_size
            and all(isinstance(item, dict) for item in rows['items']))


def _unavailable_results(result_filter, page, page_size):
    value = {'available': False, 'counts': dict.fromkeys(('passed', 'failed', 'unrecognized')),
             'partial': True, 'message': '后处理结果暂不可核验'}
    if result_filter is not None:
        value['list'] = dict(filter=result_filter, page=page, page_size=page_size,
                             total=None, items=[], message=value['message'])
    return value


def _unavailable_registered(reason='ledger_unavailable'):
    return {'available': False, 'basis': REGISTERED_BASIS, 'reason': reason,
            'current_delivery_verified': False,
            'results': {'available': False, 'message': '登记结果暂不可读取'}}


def _valid_registered(value, result_filter, page, page_size, revision, *, allow_partial_listing=False):
    if (not isinstance(value, dict) or value.get('basis') != REGISTERED_BASIS
            or value.get('current_delivery_verified') is not False
            or type(value.get('available')) is not bool):
        return False
    if not value['available']:
        return value.get('reason') in {'ledger_unavailable', 'snapshot_changed', 'snapshot_revision_required'}
    observed, stage, result = value.get('as_of'), value.get('stage'), value.get('results')
    actual_revision = value.get('revision')
    if (not isinstance(actual_revision, str) or not re.fullmatch(r'[0-9a-f]{64}', actual_revision)
            or (revision is not None and revision != actual_revision)
            or not _valid_verification_time(observed)
            or not isinstance(stage, dict) or stage.get('available') is not True
            or stage.get('basis') != REGISTERED_BASIS or stage.get('as_of') != observed
            or type(stage.get('partial')) is not bool or not isinstance(stage.get('counts'), dict)
            or not all(key in stage['counts'] and _valid_count(stage['counts'][key]) for key in STAGE_KEYS)
            or stage['counts']['postprocessing_in_progress'] is not None
            or not isinstance(result, dict) or result.get('basis') != REGISTERED_BASIS
            or result.get('as_of') != observed or not _valid_results(result, result_filter, page, page_size)
            or result['counts']['unrecognized'] is not None):
        return False
    if result_filter is not None:
        listing = result['list']
        subset_listing = (allow_partial_listing and result_filter == 'passed' and result['partial'] is True
                          and stage['partial'] is True and listing['total'] is None
                          and value.get('server_observation', {}).get('scope') == 'verified_registered_subset')
        if (type(listing.get('has_next')) is not bool
                or (result_filter == 'unrecognized' and listing['total'] is not None)
                or (result_filter != 'unrecognized' and not subset_listing
                    and listing['total'] != result['counts'][result_filter])):
            return False
    return True


def _normalize_server_observation(value, *, expected_revision=None, now=None):
    """Fail closed when a remote observation is missing or too old to display."""
    if not isinstance(value, dict) or value.get('available') is not True:
        return value
    now = projection_clock() if now is None else now
    started = getattr(value, '_server_observed_monotonic', None)
    valid_for = getattr(value, '_server_valid_for_seconds', None)
    observed = value.get('server_observation')
    count = value.get('stage', {}).get('counts', {}).get('waiting_rag_upload')
    fresh = (isinstance(observed, dict) and observed.get('available') is True
             and observed.get('basis') == 'paperflow-server-receipt-observation/1'
             and observed.get('observation_revision') == value.get('revision')
             and observed.get('as_of') == value.get('as_of')
             and observed.get('waiting_rag_upload') == count and _valid_count(count)
             and type(started) in (int, float) and math.isfinite(started)
             and type(valid_for) in (int, float) and 0 < valid_for <= 30
             and type(now) in (int, float) and math.isfinite(now)
             and 0 <= now - started <= valid_for)
    result = copy.deepcopy(value)
    result['stage']['counts']['postprocessing_in_progress'] = None
    result['results']['counts']['failed'] = None
    result['results']['counts']['unrecognized'] = None
    listing = result['results'].get('list')
    if listing and listing.get('filter') in {'failed', 'unrecognized'}:
        listing['total'] = None
        for item in listing.get('items', []):
            if '待核验' not in str(item.get('reason', '')):
                item['reason'] = (item.get('reason') or '已登记') + ' · 证据待核验'
    if not fresh:
        source_revision = observed.get('source_revision') if isinstance(observed, dict) else None
        reason = observed.get('reason') if isinstance(observed, dict) else None
        if not isinstance(reason, str) or not re.fullmatch(r'[a-z_]{1,64}', reason):
            reason = 'server_observation_expired_or_unavailable'
        result['stage']['counts']['waiting_postprocessing'] = None
        result['stage']['counts']['waiting_rag_upload'] = None
        result['results']['counts']['passed'] = None
        result['results']['partial'] = True
        if listing and listing.get('filter') == 'passed':
            listing['total'] = None
            for item in listing.get('items', []):
                item['reason'] = '已登记通过 · 服务器回执待核验'
        result['server_observation'] = {'available': False,
            'basis': 'paperflow-server-receipt-observation/1', 'as_of': result.get('as_of'),
            'waiting_rag_upload': None, 'source_revision': source_revision, 'reason': reason}
        if isinstance(source_revision, str) and re.fullmatch(r'[0-9a-f]{64}', source_revision):
            canonical = {'ledger': source_revision, 'server_available': False, 'maps': []}
            raw = json.dumps(canonical, sort_keys=True, ensure_ascii=False, separators=(',', ':')).encode('utf8')
            result['revision'] = hashlib.sha256(raw).hexdigest()
        else:
            result['revision'] = hashlib.sha256(('server-unavailable:' + result['revision']).encode('ascii')).hexdigest()
        result['server_observation']['observation_revision'] = result['revision']
    if expected_revision is not None and expected_revision != result['revision']:
        return _unavailable_registered('snapshot_changed')
    return result


def _enrich_registered(value, result_filter, title_enricher, failure_messages):
    """Optional page labels; never let a title lookup replace ledger identities."""
    if value.get('available') is not True or result_filter is None:
        return value
    value = copy.deepcopy(value)
    items = value['results']['list']['items']
    for item in items:
        item.pop('paper_title', None)
        item.pop('paper_title_candidate', None)
        item['title_status'] = '暂不可用'
        if result_filter == 'failed' and isinstance(failure_messages, dict):
            message = failure_messages.get(item.get('code'))
            if isinstance(message, str) and 0 < len(message) <= 256:
                item['reason'] = '已登记未通过：' + message
    if not callable(title_enricher) or not items:
        return value
    candidate = copy.deepcopy(items)
    try:
        title_enricher(candidate)
        if not isinstance(candidate, list) or len(candidate) != len(items):
            return value
        for original, enriched in zip(items, candidate):
            if (not isinstance(enriched, dict) or any(not original.get(key) or enriched.get(key) != original[key]
                    for key in ('task_id', 'group_id', 'paper_id', 'receipt_id'))):
                continue
            status = enriched.get('title_status')
            if status == '已提取' and isinstance(enriched.get('paper_title'), str) and enriched['paper_title'].strip():
                original.update(title_status=status, paper_title=enriched['paper_title'].strip())
            elif status == '待核对' and isinstance(enriched.get('paper_title_candidate'), str) and enriched['paper_title_candidate'].strip():
                original.update(title_status=status, paper_title_candidate=enriched['paper_title_candidate'].strip())
            elif status == '未提取':
                original['title_status'] = status
    except Exception:
        pass
    return value


class _SourceSnapshotCapture:
    """Coalesce quick source captures; bound waiting if a capture is slow."""
    MAX_WAIT_SECONDS = .2

    def __init__(self):
        self.lock = Lock()
        self.active = None
        self.sequence = 0

    def take(self, queue, max_age_seconds):
        with self.lock:
            leader = self.active is None
            if leader:
                self.active = {'done': Event(), 'snapshot': None, 'sequence': None}
            capture = self.active
        if not leader:
            if capture['done'].wait(self.MAX_WAIT_SECONDS) and capture['sequence'] is not None:
                return copy.deepcopy(capture['snapshot']), capture['sequence']
            # Preserve other summary cards without allowing an unordered
            # fallback observation to rebind the postprocessing source.
            return queue.snapshot(max_age_seconds=max_age_seconds), None
        try:
            snapshot = queue.snapshot(max_age_seconds=max_age_seconds)
            frozen = copy.deepcopy(snapshot)
        except BaseException:
            with self.lock:
                self.active = None
                capture['done'].set()
            raise
        with self.lock:
            self.sequence += 1
            capture['snapshot'] = frozen
            capture['sequence'] = self.sequence
            self.active = None
            capture['done'].set()
            return snapshot, capture['sequence']


class _ProjectionCache:
    """Bounded, passive cache; reads and hashing never block an HTTP response."""
    def __init__(self, *, max_entries=32, max_inflight=2, invalidate_server_failures=False):
        self.entries = OrderedDict()
        self.inflight = set()
        self.local_inflight = set()
        self.remote_inflight = set()
        self.lock = Lock()
        self.max_entries = max_entries
        self.max_inflight = max_inflight
        self.source = None
        self.source_epoch = 0
        self.source_sequence = 0
        self.invalidate_server_failures = invalidate_server_failures
        self.latest_server_started = float('-inf')
        self.latest_server_state = None
        self.server_epoch = 0

    def current_revision(self):
        """Expose cache invalidation without opening a database or starting work."""
        with self.lock:
            return self.source_epoch, self.server_epoch

    @staticmethod
    def _server_state(value):
        if not isinstance(value, dict) or value.get('available') is not True:
            return ('unavailable',)
        observation = value.get('server_observation')
        if not isinstance(observation, dict) or observation.get('available') is not True:
            return ('unavailable', observation.get('source_revision') if isinstance(observation, dict) else None)
        return ('verified', observation.get('source_revision'), observation.get('observation_revision'))

    def bind_source(self, source, request_sequence):
        """Do not allow an older request to undo a newer source observation."""
        with self.lock:
            if request_sequence < self.source_sequence:
                return None
            self.source_sequence = request_sequence
            if self.source != source:
                self.source = source
                self.source_epoch += 1
                self.entries.clear()
            return self.source_epoch

    def get(self, key, compute, *, snapshot=None, expected_epoch=None, with_deadline=False):
        now = projection_clock()
        with self.lock:
            if expected_epoch is not None and expected_epoch != self.source_epoch:
                return (None, None) if with_deadline else None
            entry = self.entries.get(key)
            if entry is not None:
                self.entries.move_to_end(key)
            witness = entry.get('witness') if entry is not None else None
            age = now - entry['at'] if entry is not None else None
            max_age = GUARDED_PROJECTION_MAX_AGE_SECONDS if witness is not None else PROJECTION_MAX_AGE_SECONDS
            refresh_age = GUARDED_PROJECTION_REFRESH_SECONDS if witness is not None else PROJECTION_REFRESH_SECONDS
            local_age = now - entry['local_at'] if witness is not None else None
            remote_age = now - entry['remote_at'] if witness is not None else None
            local_ok = witness is None or 0 <= local_age <= GUARDED_LOCAL_MAX_AGE_SECONDS
            remote_ok = witness is None or 0 <= remote_age <= GUARDED_REMOTE_MAX_AGE_SECONDS
            cached = entry['value'] if age is not None and 0 <= age <= max_age and local_ok and remote_ok else None
            local_launch = (witness is not None and 0 <= age <= max_age
                            and (local_age < 0 or local_age >= GUARDED_LOCAL_REFRESH_SECONDS)
                            and key not in self.local_inflight
                            and len(self.local_inflight) < self.max_inflight)
            if local_launch:
                self.local_inflight.add(key)
            remote_launch = (witness is not None and 0 <= age <= max_age
                             and (remote_age < 0 or remote_age >= GUARDED_REMOTE_REFRESH_SECONDS)
                             and key not in self.remote_inflight
                             and len(self.remote_inflight) < self.max_inflight)
            if remote_launch:
                self.remote_inflight.add(key)
            launch = ((age is None or age < 0 or age >= refresh_age)
                      and key not in self.inflight and len(self.inflight) < self.max_inflight)
            if launch:
                self.inflight.add(key)
                launch_epoch = self.source_epoch
                launched_at = now
        if local_launch:
            def refresh_local():
                try:
                    valid = witness.local(snapshot) is True
                except Exception:
                    valid = False
                finally:
                    with self.lock:
                        if self.entries.get(key) is entry:
                            if valid:
                                # Use the check's start, not its completion.
                                entry['local_at'] = now
                            else:
                                self.entries.pop(key, None)
                        self.local_inflight.discard(key)
            try:
                Thread(target=refresh_local, name='postprocessing-local-witness', daemon=True).start()
            except Exception:
                with self.lock:
                    if self.entries.get(key) is entry:
                        self.entries.pop(key, None)
                    self.local_inflight.discard(key)
                cached = None
        if remote_launch:
            def refresh_remote():
                try:
                    valid = witness.remote() is True
                except Exception:
                    valid = False
                finally:
                    with self.lock:
                        if self.entries.get(key) is entry:
                            if valid:
                                # A slow remote read must not make the start
                                # of that observation appear newly verified.
                                entry['remote_at'] = now
                            else:
                                self.entries.pop(key, None)
                        self.remote_inflight.discard(key)
            try:
                Thread(target=refresh_remote, name='postprocessing-prefect-witness', daemon=True).start()
            except Exception:
                with self.lock:
                    if self.entries.get(key) is entry:
                        self.entries.pop(key, None)
                    self.remote_inflight.discard(key)
                cached = None
        if launch:
            def refresh():
                value = None
                try:
                    value = copy.deepcopy(compute())
                    guard = getattr(value, '_cache_witness', None)
                    local_observed_at = None
                    if guard is not None and (not callable(getattr(guard, 'local', None))
                                              or not callable(getattr(guard, 'remote', None))):
                        value, guard = None, None
                    if guard is not None:
                        full_at = getattr(guard, 'full_observed_at', None)
                        observed_at = getattr(guard, 'remote_observed_at', None)
                        local_observed_at = projection_clock()
                        if (type(full_at) not in (int, float) or not math.isfinite(full_at)
                                or type(observed_at) not in (int, float) or not math.isfinite(observed_at)
                                or not 0 <= full_at <= observed_at <= projection_clock()
                                or guard.local(snapshot) is not True):
                            value, guard = None, None
                except Exception:
                    value, guard = None, None
                finally:
                    with self.lock:
                        now = projection_clock()
                        same_source = self.source_epoch == launch_epoch
                        can_store = same_source and self.entries.get(key) is entry
                        if same_source and self.invalidate_server_failures:
                            # A stale page revision is a query error, not a fresh
                            # observation that the server or source is offline.
                            query_error = (isinstance(value, dict) and value.get('available') is False
                                           and value.get('reason') in {'snapshot_changed', 'snapshot_revision_required'})
                            if not query_error:
                                state = self._server_state(value)
                                # A newer result may have evicted this filter's
                                # entry while its read was still in flight. A
                                # later failure still invalidates other filters;
                                # a displaced success may never replace them.
                                can_order = can_store or state[0] == 'unavailable'
                                observed = getattr(value, '_server_observed_monotonic', None)
                                if type(observed) not in (int, float) or not math.isfinite(observed):
                                    # An unmarked failure may have happened after
                                    # another filter published a success. Fail
                                    # closed at completion instead of treating
                                    # the request's earlier start as the failure.
                                    observed = projection_clock() if state == ('unavailable',) else launched_at
                                cache_order = getattr(value, '_server_cache_order_monotonic', None)
                                if cache_order is not None:
                                    if (type(cache_order) not in (int, float) or not math.isfinite(cache_order)
                                            or cache_order < observed or cache_order > projection_clock()):
                                        can_store = False
                                        if state[0] == 'unavailable':
                                            observed = projection_clock()
                                        else:
                                            can_order = False
                                    else:
                                        observed = cache_order
                                if observed < self.latest_server_started and state != self.latest_server_state:
                                    can_store = False
                                elif can_order and observed >= self.latest_server_started:
                                    if state != self.latest_server_state:
                                        self.server_epoch += 1
                                        for old_key, old_entry in list(self.entries.items()):
                                            old_value = old_entry.get('value')
                                            old_started = getattr(old_value, '_server_cache_order_monotonic',
                                                getattr(old_value, '_server_observed_monotonic', old_entry['at']))
                                            if ((type(old_started) not in (int, float) or not math.isfinite(old_started)
                                                 or old_started <= observed)
                                                    and self._server_state(old_value) != state):
                                                self.entries.pop(old_key, None)
                                    self.latest_server_started, self.latest_server_state = observed, state
                        if can_store:
                            self.entries[key] = dict(at=guard.full_observed_at if guard else now,
                                                     value=value, witness=guard,
                                                     local_at=local_observed_at if guard else now,
                                                     remote_at=guard.remote_observed_at if guard else now)
                            self.entries.move_to_end(key)
                            while len(self.entries) > self.max_entries:
                                self.entries.popitem(last=False)
                        self.inflight.discard(key)
            try:
                Thread(target=refresh, name='postprocessing-projection', daemon=True).start()
            except Exception:
                with self.lock:
                    self.inflight.discard(key)
        if cached is not None:
            with self.lock:
                if self.entries.get(key) is not entry:
                    return (None, None) if with_deadline else None
                if with_deadline:
                    deadline = entry['at'] + max_age
                    if witness is not None:
                        deadline = min(deadline, entry['local_at'] + GUARDED_LOCAL_MAX_AGE_SECONDS,
                                       entry['remote_at'] + GUARDED_REMOTE_MAX_AGE_SECONDS)
                    return copy.deepcopy(cached), deadline
        return (None, None) if with_deadline else copy.deepcopy(cached)


def create_app(queue, *, max_age_seconds=60, enable_claims=False, ticket_observer=None,
               stage_projection=None, results_projection=None, registered_projection=None,
               registered_title_enricher=None, registered_failure_messages=None, postprocess_only=False,
               partial_observation=False, registered_revision=None, local_waiting_projection=None):
    if registered_revision is not None and not callable(registered_revision):
        raise ValueError('control revision witness must be callable')
    app = FastAPI(title='本地接收等待统计', version='1.0')
    app.add_middleware(
        CORSMiddleware,
        allow_origin_regex=r'^https?://(localhost|127\.0\.0\.1)(:\d+)?$',
        allow_methods=['GET'], allow_headers=['*'])

    @app.get('/api/v1/local-intake/postprocessing/waiting')
    def local_postprocessing_waiting(response: Response):
        """The local folder queue does not depend on a remote delivery receipt."""
        response.headers['Cache-Control'] = 'no-store'
        try:
            value = local_waiting_projection() if callable(local_waiting_projection) else None
            stage = value.get('stage', {}) if isinstance(value, dict) else {}
            count = stage.get('counts', {}).get('waiting_postprocessing') if isinstance(stage, dict) else None
            if (value.get('available') is True and value.get('basis') == 'postprocessing-registered-ledger/1'
                    and stage.get('available') is True and type(stage.get('partial')) is bool
                    and type(count) is int and count >= 0 and isinstance(value.get('as_of'), str)):
                return {'available': True, 'count': count, 'partial': stage['partial'], 'as_of': value['as_of']}
        except Exception:
            pass
        return {'available': False}

    llm_cache = {'key': None, 'at': 0.0, 'value': None, 'inflight': False}
    llm_lock = Lock()
    stage_cache = _ProjectionCache(max_inflight=1)
    results_cache = _ProjectionCache()
    registered_cache = _ProjectionCache(max_inflight=2, invalidate_server_failures=True)
    app.state.postprocessing_cache_revision = registered_cache.current_revision
    source_capture = _SourceSnapshotCapture()

    def projection_nonblocking(snapshot, ticket_summary, projection, cache, args, validator, request_sequence):
        if request_sequence is None:
            return None
        if not _usable_source(snapshot):
            cache.bind_source(None, request_sequence)
            return None
        source = _snapshot_key(snapshot)
        epoch = cache.bind_source(source, request_sequence)
        if epoch is None:
            return None
        if (not callable(projection) or ticket_summary.get('source') != 'prefect'
                or ticket_summary.get('scope') != 'all_tasks'
                or ticket_summary.get('available') is not True):
            return None
        captured = copy.deepcopy(snapshot)
        key = (source, *args)

        def read():
            value = projection(*args) if cache is results_cache else projection(copy.deepcopy(captured))
            if not validator(value):
                return None
            current = queue.snapshot(max_age_seconds=max_age_seconds)
            if not _usable_source(current) or _snapshot_key(current) != source:
                return None
            return value

        return cache.get(key, read, snapshot=captured, expected_epoch=epoch)

    def llm_snapshot_key(snapshot):
        return (snapshot.get('receiver_status'), snapshot.get('receiver_session_id'),
                snapshot.get('scan_source_instance'), snapshot.get('waiting_available'),
                snapshot.get('upstream_scan_verified'), snapshot.get('waiting_llm_total'))

    def llm_summary_nonblocking(snapshot):
        """Keep Prefect latency from blanking unrelated preprocessing cards."""
        if ticket_observer is None or snapshot.get('receiver_status') != 'running':
            return None
        key = llm_snapshot_key(snapshot)
        started = time.monotonic()
        with llm_lock:
            age = started - llm_cache['at'] if llm_cache['key'] == key else None
            cached = llm_cache['value'] if age is not None and age <= 8 else None
            launch = (age is None or age >= 4) and not llm_cache['inflight']
            if launch:
                llm_cache['inflight'] = True
        if launch:
            def refresh():
                value = None
                try:
                    from llm_queue_summary import summarize_llm
                    value = summarize_llm(queue, ticket_observer.index, ticket_observer, snapshot)
                    current = queue.snapshot(max_age_seconds=max_age_seconds)
                    if llm_snapshot_key(current) != key:
                        value = None
                except Exception:
                    value = None
                finally:
                    with llm_lock:
                        llm_cache.update(key=key, at=time.monotonic(), value=value, inflight=False)
            Thread(target=refresh, name='prefect-llm-summary', daemon=True).start()
        return cached

    @app.get('/api/v1/local-intake/preprocessing/summary')
    def waiting_summary(response: Response, result_filter: Literal['passed', 'failed', 'unrecognized'] | None = None,
                        result_page: int = Query(1, ge=1, le=1000000),
                        result_page_size: int = Query(20, ge=1, le=100),
                        llm_result_filter: Literal['passed', 'failed', 'unrecognized'] | None = None,
                        llm_result_page: int = Query(1, ge=1, le=1000000),
                        llm_result_page_size: int = Query(20, ge=1, le=100),
                        postprocessing_result_filter: Literal['passed', 'failed', 'unrecognized'] | None = None,
                        postprocessing_result_page: int = Query(1, ge=1, le=1000000),
                        postprocessing_result_page_size: int = Query(20, ge=1, le=100),
                        postprocessing_result_revision: str | None = Query(None, max_length=64)):
        response.headers['Cache-Control'] = 'no-store'
        try:
            snapshot, request_sequence = source_capture.take(queue, max_age_seconds)
            snapshot['prefect_ticket_summary'] = (
                ticket_observer.summary(snapshot) if ticket_observer is not None else
                {'source': 'prefect', 'scope': 'all_tasks', 'available': False, 'counts': {}})
            results = (ticket_observer.preprocessing_results(snapshot, result_filter, result_page, result_page_size)
                       if ticket_observer is not None and not postprocess_only else {'available': False, 'counts': {}})
            snapshot['preprocessing_results'] = results
            # An unavailable LLM results projection must not break the existing
            # preprocessing counts or other queue cards.
            try:
                snapshot['llm_results'] = (
                    ticket_observer.llm_results(snapshot, llm_result_filter, llm_result_page, llm_result_page_size)
                    if ticket_observer is not None and not postprocess_only else {'available': False, 'counts': {}})
            except Exception:
                snapshot['llm_results'] = {'available': False, 'counts': {}, 'message': 'LLM 结果暂不可用'}
            ticket_summary = snapshot['prefect_ticket_summary']
            # Never reuse upstream cumulative LLM completions as current stage counts.
            ticket_summary['counts'] = dict(ticket_summary.get('counts', {}))
            for name in (*STAGE_KEYS, *STAGE_DIAGNOSTICS):
                ticket_summary['counts'].pop(name, None)
            ticket_summary.pop('postprocessing_basis', None)
            ticket_summary.pop('postprocessing_as_of', None)
            ticket_summary.pop('waiting_postprocessing_partial', None)
            if ticket_summary.get('available') and not postprocess_only:
                # The live snapshot checks which local preprocessing attempts
                # are actually active. Do not discard it merely because an
                # older cached ticket snapshot has a different count.
                in_progress = snapshot.get('in_progress_count')
                if type(in_progress) is int and in_progress >= 0:
                    ticket_summary['counts']['preprocessing_in_progress'] = in_progress
                    ticket_summary['preprocessing_in_progress_as_of'] = snapshot.get('as_of')
                else:
                    ticket_summary['counts'].pop('preprocessing_in_progress', None)
                # Passed preprocessing is cumulative, not the current LLM
                # waiting queue. Derive LLM phases from the folder ledger.
                names = ('waiting_llm', 'llm_body_in_progress', 'llm_attachment_in_progress')
                try:
                    llm = llm_summary_nonblocking(snapshot)
                    if llm is None:
                        raise ValueError('LLM 工单快照暂不可用')
                    for name in names:
                        value = llm.get(name)
                        if type(value) is int and value >= 0:
                            ticket_summary['counts'][name] = value
                        else:
                            ticket_summary['counts'].pop(name, None)
                    ticket_summary['waiting_llm_as_of'] = llm.get('as_of')
                    ticket_summary['waiting_llm_partial'] = llm['waiting_llm_partial']
                    ticket_summary['llm_processing_as_of'] = llm.get('as_of')
                    ticket_summary['llm_body_in_progress_partial'] = llm['llm_body_in_progress_partial']
                    ticket_summary['llm_attachment_in_progress_partial'] = llm['llm_attachment_in_progress_partial']
                except Exception:
                    for name in names:
                        ticket_summary['counts'].pop(name, None)
            args = (postprocessing_result_filter, postprocessing_result_page, postprocessing_result_page_size)
            if registered_projection is None:
                # This server-only candidate must not fall back to a local
                # process/path projection when remote wiring is absent.
                snapshot['postprocessing_results'] = _unavailable_results(*args)
                snapshot['postprocessing_registered_ledger'] = _unavailable_registered()
            else:
                snapshot['postprocessing_results'] = _unavailable_results(*args)
                revision = postprocessing_result_revision
                eligible = ((_usable_subset_source(snapshot) and ticket_observer is not None)
                            if partial_observation else
                            (_usable_source(snapshot) and ticket_summary.get('source') == 'prefect'
                             and ticket_summary.get('scope') == 'all_tasks'
                             and ticket_summary.get('available') is True))
                source_snapshot = _snapshot_key(snapshot) if eligible else None
                control_revision = None
                if eligible and registered_revision is not None:
                    try:
                        control_revision = registered_revision()
                        if control_revision is None: raise ValueError('control revision unavailable')
                    except Exception:
                        eligible = False
                source = (source_snapshot, control_revision) if eligible else None
                epoch = (registered_cache.bind_source(source, request_sequence)
                         if request_sequence is not None else None)
                if not eligible or epoch is None:
                    registered, cache_deadline = _unavailable_registered(), None
                elif (postprocessing_result_filter is not None and postprocessing_result_page > 1
                        and (not isinstance(revision, str) or not re.fullmatch(r'[0-9a-f]{64}', revision))):
                    registered = _unavailable_registered('snapshot_revision_required')
                    cache_deadline = None
                else:
                    def read_registered():
                        try:
                            value = registered_projection(result_filter=postprocessing_result_filter,
                                page=postprocessing_result_page, page_size=postprocessing_result_page_size,
                                expected_revision=revision)
                            if not _valid_registered(value, *args, revision, allow_partial_listing=partial_observation):
                                return _unavailable_registered()
                            if partial_observation and value.get('available') is True:
                                stage = value['stage']
                                observed = value.get('server_observation', {})
                                if (stage.get('partial') is not True or value['results'].get('partial') is not True
                                        or any(stage['counts'][key] is not None for key in STAGE_KEYS)
                                        or observed.get('scope') != 'verified_registered_subset'):
                                    return _unavailable_registered()
                            current = queue.snapshot(max_age_seconds=max_age_seconds)
                            source_ok = _usable_subset_source(current) if partial_observation else _usable_source(current)
                            if (not source_ok or _snapshot_key(current) != source_snapshot
                                    or (registered_revision is not None and registered_revision() != control_revision)):
                                return _unavailable_registered()
                            return _enrich_registered(value, postprocessing_result_filter,
                                                      registered_title_enricher, registered_failure_messages)
                        except Exception:
                            return _unavailable_registered()
                    registered, cache_deadline = registered_cache.get((source, *args, revision), read_registered,
                        expected_epoch=epoch, with_deadline=True)
                snapshot['postprocessing_registered_ledger'] = _normalize_server_observation(
                    registered or _unavailable_registered(), expected_revision=revision)
                # The outer ASGI adapter must never extend the lifetime of an
                # observation that was already aging in this inner cache.
                observed = snapshot['postprocessing_registered_ledger'].get('server_observation')
                started = getattr(registered, '_server_observed_monotonic', None)
                valid_for = getattr(registered, '_server_valid_for_seconds', None)
                if (isinstance(observed, dict) and observed.get('available') is True
                        and type(cache_deadline) in (int, float) and math.isfinite(cache_deadline)
                        and type(started) in (int, float) and math.isfinite(started)
                        and type(valid_for) in (int, float) and 0 < valid_for <= 30):
                    response.headers['X-Paperflow-Observation-Deadline'] = repr(
                        min(cache_deadline, started + valid_for))
            return snapshot
        except (OSError, sqlite3.Error):
            return JSONResponse(
                status_code=503,
                headers={'Cache-Control': 'no-store'},
                content={'scope': 'all_tasks', 'waiting_count': None,
                         'waiting_available': False,
                         'as_of': datetime.now(timezone.utc).isoformat(),
                         'upstream_scan_completed_at': None,
                         'upstream_scan_status': 'unavailable',
                         'receiver_status': 'unknown', 'receiver_heartbeat_at': None,
                         'oldest_verified_at': None, 'needs_review_count': None,
                         'preprocessing_handoff_connected': False,
                         'error': 'local_state_unavailable'})

    @app.get('/api/v1/local-intake/llm/waiting')
    def llm_waiting(response: Response):
        response.headers['Cache-Control'] = 'no-store'
        try:
            if ticket_observer is None:
                raise ValueError('文件夹工单观察器不可用')
            from llm_queue_summary import summarize_llm
            snapshot = queue.snapshot(max_age_seconds=max_age_seconds)
            llm = summarize_llm(queue, ticket_observer.index, ticket_observer, snapshot)
            return {'available': llm['waiting_llm'] is not None, 'waiting_count': llm['waiting_llm'],
                    'unavailable_count': llm['unavailable_count'], 'items': llm['waiting_items'],
                    'partial': llm['waiting_llm_partial']}
        except Exception:
            return JSONResponse(status_code=503, headers={'Cache-Control': 'no-store'},
                                content={'available': False, 'waiting_count': None, 'items': []})

    return app

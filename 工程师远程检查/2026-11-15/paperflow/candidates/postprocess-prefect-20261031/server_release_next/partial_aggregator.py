"""Bounded background observations of automatically registered folder results.

This is a passive projection, not a Prefect scheduler or a work-order lock.
Only registered passes enter the original one-folder receipt verifier. A slow
or unavailable folder cannot turn another receipt into a global queue count.
"""
from concurrent.futures import ThreadPoolExecutor
import copy
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
import time


class PartialAggregator:
    def __init__(self, database, *, ledger_snapshot, component_id_fn, inspector, stage_store,
                 scope, server_helpers, partial, max_inflight=2, retry_seconds=10, check_inputs=None,
                 refresh_before_seconds=15, revision_witness=None):
        if (not callable(ledger_snapshot) or type(max_inflight) is not int or not 1 <= max_inflight <= 2
                or type(retry_seconds) not in (int, float) or not 1 <= retry_seconds <= 60
                or type(refresh_before_seconds) not in (int, float) or not 0 < refresh_before_seconds <= 20):
            raise ValueError('bounded read-only observation dependencies are required')
        self.database = Path(database).resolve(strict=True)
        self.ledger_snapshot, self.component_id_fn = ledger_snapshot, component_id_fn
        self.inspector, self.stage_store, self.scope = inspector, stage_store, scope
        self.server, self.partial = server_helpers, partial
        if check_inputs is not None and not callable(check_inputs):
            raise ValueError('release input check must be callable')
        if revision_witness is not None and not callable(revision_witness):
            raise ValueError('control revision witness must be callable')
        self._owned_revision = None
        if revision_witness is None:
            from revision_witness import ReadOnlyRevision
            self._owned_revision = ReadOnlyRevision(self.database)
            revision_witness = self._owned_revision.current
        self.revision_witness = revision_witness
        self.check_inputs = check_inputs or (lambda: None)
        self.max_inflight, self.retry_seconds = max_inflight, retry_seconds
        self.refresh_before_seconds = refresh_before_seconds
        self._pool = ThreadPoolExecutor(max_workers=max_inflight, thread_name_prefix='receipt-observe')
        self._lock = Lock()  # This protects only this process's observation cache.
        self._inflight, self._done, self._retry = {}, {}, {}
        self._last_scheduled, self._schedule_sequence = {}, 0
        self._control_revision, self._epoch = None, 0
        self._closed = False

    def close(self):
        with self._lock:
            self._closed = True
            self._done.clear()
        self._pool.shutdown(wait=False, cancel_futures=True)
        if self._owned_revision is not None:
            self._owned_revision.close()

    @staticmethod
    def _key(item):
        return item.get('task_id'), item.get('group_id')

    def _observe(self, key):
        return self.partial.observe_one(self.database, key, ledger_snapshot=self.ledger_snapshot,
            component_id_fn=self.component_id_fn, inspector=self.inspector, stage_store=self.stage_store,
            scope=self.scope, server_helpers=self.server)

    def snapshot(self, *, result_filter=None, page=1, page_size=20, expected_revision=None):
        try:
            with self._lock:
                if self._closed:
                    raise ValueError('the read-only observer is closed')
            control_revision = self.revision_witness()
            if control_revision is None:
                raise ValueError('control revision is unavailable')
            self.check_inputs()
            base, items = self.partial.registered_candidates(self.database, ledger_snapshot=self.ledger_snapshot,
                component_id_fn=self.component_id_fn, server_helpers=self.server)
            revision = base['revision']
            if len(items) > self.inspector.max_summary_receipts:
                raise ValueError('registered observations exceed the configured bound')
            value = self.ledger_snapshot(self.database, result_filter=result_filter, page=page,
                page_size=page_size, expected_revision=revision, component_id_fn=self.component_id_fn)
            if value.get('available') is not True or value.get('revision') != revision:
                raise ValueError('registered observations changed')
            now = time.monotonic()
            by_key = {self._key(item): item for item in items}
            if len(by_key) != len(items):
                raise ValueError('duplicate registered folder')
            with self._lock:
                if self._closed:
                    raise ValueError('the read-only observer is closed')
                if self.revision_witness() != control_revision:
                    raise ValueError('control revision changed during observation')
                if self._control_revision != control_revision:
                    self._control_revision = control_revision
                    self._epoch += 1
                    self._done.clear()
                    self._retry.clear()
                epoch = self._epoch
                for key, (future, started_epoch) in list(self._inflight.items()):
                    if not future.done():
                        continue
                    self._inflight.pop(key)
                    try:
                        answer = future.result()
                    except Exception:
                        answer = None
                    if (isinstance(answer, dict) and answer.get('available') is True
                            and started_epoch == epoch
                            and answer.get('scope') == 'verified_registered_subset'
                            and answer.get('ledger_revision') == revision and key in by_key
                            and self._key(answer.get('item', {})) == key
                            and type(answer.get('observed_monotonic')) in (int, float)
                            and type(answer.get('valid_until_monotonic')) in (int, float)
                            and answer['observed_monotonic'] <= now < answer['valid_until_monotonic']
                            and answer['valid_until_monotonic'] - answer['observed_monotonic'] <= 30):
                        self._done[key] = answer
                        self._retry.pop(key, None)
                    elif key in by_key and started_epoch == epoch:
                        self._done.pop(key, None)
                        self._retry[key] = now + self.retry_seconds
                self._done = {key: answer for key, answer in self._done.items()
                              if key in by_key and answer['ledger_revision'] == revision
                              and now < answer['valid_until_monotonic']}
                self._retry = {key: until for key, until in self._retry.items() if key in by_key and until > now}
                self._last_scheduled = {key: order for key, order in self._last_scheduled.items() if key in by_key}
                eligible = [key for key in by_key if key not in self._inflight and key not in self._retry
                            and (key not in self._done or
                                 self._done[key]['valid_until_monotonic'] - now <= self.refresh_before_seconds)]
                # Unobserved folders first; afterward rotate by last attempt so
                # a frequently refreshed early row cannot starve later rows.
                eligible.sort(key=lambda key: self._last_scheduled.get(key, -1))
                for key in eligible[:self.max_inflight - len(self._inflight)]:
                    self._schedule_sequence += 1
                    self._last_scheduled[key] = self._schedule_sequence
                    self._inflight[key] = (self._pool.submit(self._observe, key), epoch)
                verified = [copy.deepcopy(self._done[key]) for key in by_key if key in self._done]
                cache_order = time.monotonic()
            result = copy.deepcopy(value)
            self.server._unknown_results(result)
            result['stage']['partial'] = result['results']['partial'] = True
            observed_at = (min((item['as_of'] for item in verified), default=None)
                           or datetime.now(timezone.utc).isoformat())
            result['as_of'] = result['stage']['as_of'] = result['results']['as_of'] = observed_at
            count = len(verified) if verified else None
            result['results']['counts']['passed'] = count
            if result_filter == 'passed':
                listing = result['results']['list']
                verified_by_key = {self._key(item['item']): item['item'] for item in verified}
                rows = []
                for key, item in by_key.items():
                    if key in verified_by_key:
                        rows.append(verified_by_key[key])
                    else:
                        pending = copy.deepcopy(item)
                        pending.update(verification_pending=True, reason='已登记通过 · 证据待核对')
                        rows.append(pending)
                start = (page - 1) * page_size
                listing.update(total=None, has_next=start + page_size < len(rows),
                               items=rows[start:start + page_size])
            if result_filter in {'failed', 'unrecognized'}:
                result['results']['list']['total'] = None
            source_revision = revision
            observed_revision = self.server._hash(
                {'ledger': revision, 'scope': 'verified_registered_subset',
                 'verified': sorted([list(self._key(item['item'])) for item in verified])}
                if verified else {'ledger': revision, 'server_available': False, 'maps': []})
            result['revision'] = observed_revision
            result['server_observation'] = {'available': bool(verified), 'basis': self.server.SERVER_BASIS,
                'scope': 'verified_registered_subset', 'as_of': observed_at, 'source_revision': source_revision,
                'observation_revision': observed_revision, 'waiting_rag_upload': None}
            self.check_inputs()
            after = self.ledger_snapshot(self.database, component_id_fn=self.component_id_fn)
            if after.get('available') is not True or after.get('revision') != revision:
                raise ValueError('registered ledger changed before publication')
            with self._lock:
                if (self._closed or self._epoch != epoch or self._control_revision != control_revision
                        or self.revision_witness() != control_revision):
                    raise ValueError('control revision changed before publication')
            if expected_revision is not None and expected_revision != observed_revision:
                return self.server._unavailable('snapshot_changed', result_filter, page, page_size)
            if verified:
                started = min(item['observed_monotonic'] for item in verified)
                deadline = min(item['valid_until_monotonic'] for item in verified)
                observation = self.server.Observation(result, started, deadline - started)
            else:
                observation = self.server.Observation(result, now, 30)
            # The result's TTL still starts at the oldest individual check.
            # Cache ordering instead follows when this subset was collected,
            # so a pending poll cannot outrank an observation that finishes later.
            observation._server_cache_order_monotonic = cache_order
            return observation
        except Exception:
            with self._lock:
                self._done.clear()
                self._retry.clear()
                self._control_revision = None
                self._epoch += 1
                failed_at = time.monotonic()
            failure = self.server.Observation(
                self.server._unavailable('ledger_unavailable', result_filter, page, page_size), failed_at, 30)
            failure._server_cache_order_monotonic = failed_at
            return failure

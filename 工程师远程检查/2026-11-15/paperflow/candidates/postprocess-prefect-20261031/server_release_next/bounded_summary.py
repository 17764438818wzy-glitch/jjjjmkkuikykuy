"""Bound slow summary observations without blocking the 5 s browser request.

The wrapped app remains authoritative. This adapter only keeps a short dated
observation, never dispatches work, and returns 503 while evidence is pending.
At most two observations may continue in the background; cancelling an HTTP
request does not pretend to cancel blocking SQLite/SSH/Prefect work.
"""
import asyncio
from collections import OrderedDict
import json
import math
import re
import time


SUMMARY = '/api/v1/local-intake/preprocessing/summary'
ORIGIN = re.compile(r'^https?://(?:localhost|127\.0\.0\.1)(?::\d+)?$')
MAX_BODY = 8 * 1024 * 1024


class BoundedSummary:
    def __init__(self, app, *, max_inflight=2, wait_seconds=.2, max_age_seconds=15, max_entries=32,
                 revision_witness=None):
        if (not callable(app) or type(max_inflight) is not int or not 1 <= max_inflight <= 2
                or type(max_entries) is not int or not 1 <= max_entries <= 32
                or not isinstance(wait_seconds, (int, float)) or not 0 < wait_seconds <= 1
                or not isinstance(max_age_seconds, (int, float)) or not 0 < max_age_seconds <= 15
                or (revision_witness is not None and not callable(revision_witness))):
            raise ValueError('bounded summary policy is invalid')
        self.app, self.max_inflight = app, max_inflight
        self.wait_seconds, self.max_age_seconds, self.max_entries = wait_seconds, max_age_seconds, max_entries
        self.active, self.entries = {}, OrderedDict()
        self.started, self.latest_completed, self.latest_basis, self.epoch = 0, 0, None, 0
        self.revision_witness, self.witness_revision, self.has_witness_revision = revision_witness, None, False

    def _sync_revision(self):
        if self.revision_witness is None:
            return True
        try:
            value = self.revision_witness()
            if value is None:
                raise ValueError('control revision unavailable')
        except Exception:
            self.entries.clear()
            self.latest_basis = None
            self.has_witness_revision = False
            self.epoch += 1
            return False
        if self.has_witness_revision and value != self.witness_revision:
            self.entries.clear()
            self.latest_basis = None
            self.epoch += 1
        self.witness_revision, self.has_witness_revision = value, True
        return True

    @staticmethod
    def _basis(body):
        value = json.loads(body)
        if not isinstance(value, dict):
            raise ValueError('summary is not an object')
        ledger = value.get('postprocessing_registered_ledger')
        remote = ledger.get('server_observation') if isinstance(ledger, dict) else None
        available = bool(isinstance(ledger, dict) and ledger.get('available') is True
                         and isinstance(remote, dict) and remote.get('available') is True)
        ticket = value.get('prefect_ticket_summary')
        if not isinstance(ticket, dict):
            ticket = {}
        return (value.get('receiver_status'), value.get('receiver_session_id'),
                value.get('scan_source_instance'), value.get('waiting_available'),
                value.get('upstream_scan_verified'), value.get('upstream_scan_completed_at'),
                value.get('waiting_llm_total'), ticket.get('source'), ticket.get('scope'), ticket.get('available'),
                available, ledger.get('revision') if available else None)

    async def _observe(self, key, scope, sequence, started, epoch):
        response = {'status': None, 'headers': [], 'body': bytearray(), 'ended': False}
        first_request = True
        never_disconnect = asyncio.Event()
        async def receive():
            nonlocal first_request
            if first_request:
                first_request = False
                return {'type': 'http.request', 'body': b'', 'more_body': False}
            await never_disconnect.wait()
            return {'type': 'http.disconnect'}
        async def capture(message):
            kind = message.get('type')
            if kind == 'http.response.start':
                if response['status'] is not None:
                    raise ValueError('duplicate summary response')
                response['status'] = message['status']
                response['headers'] = list(message.get('headers', []))
            elif kind == 'http.response.body':
                response['body'].extend(message.get('body', b''))
                if len(response['body']) > MAX_BODY:
                    raise ValueError('summary response is too large')
                if not message.get('more_body', False):
                    response['ended'] = True
        try:
            await self.app(scope, receive, capture)
            if response['status'] != 200 or not response['ended']:
                raise ValueError('summary is unavailable')
            body = bytes(response['body'])
            basis = self._basis(body)
            if not self._sync_revision():
                return
            if epoch != self.epoch:
                return
            if sequence < self.latest_completed:
                # A slower list may complete after a polling summary. Permit
                # it only if both have the same verified source and ledger
                # revision. Request start order does not establish which
                # contradictory observation happened later: invalidate both
                # instead of trusting either until a fresh request completes.
                if basis != self.latest_basis:
                    self.entries.clear()
                    self.latest_basis = None
                    self.epoch += 1
                    return
                if not basis[-2]:
                    return
            else:
                self.latest_completed = sequence
                if self.latest_basis is not None and basis != self.latest_basis:
                    self.entries.clear()
                    self.latest_basis = None
                    self.epoch += 1
                    return
                self.latest_basis = basis
            if 0 <= time.monotonic() - started <= self.max_age_seconds:
                deadlines = [value for name, value in response['headers']
                             if name.lower() == b'x-paperflow-observation-deadline']
                deadline = None
                if len(deadlines) == 1:
                    try:
                        value = float(deadlines[0].decode('ascii'))
                        if math.isfinite(value) and value <= time.monotonic() + 30:
                            deadline = value
                    except (UnicodeDecodeError, ValueError, OverflowError):
                        pass
                self.entries[key] = (started, response['status'], response['headers'], body, deadline)
                self.entries.move_to_end(key)
                while len(self.entries) > self.max_entries:
                    self.entries.popitem(last=False)
        except asyncio.CancelledError:
            raise
        except Exception:
            if epoch == self.epoch:
                self.latest_completed, self.latest_basis = max(sequence, self.latest_completed), None
                self.entries.clear()
                self.epoch += 1
        finally:
            current = self.active.get(key)
            if current and current[0] == sequence:
                self.active.pop(key, None)

    async def _pending(self, scope, send):
        headers = [(b'content-type', b'application/json; charset=utf-8'), (b'cache-control', b'no-store'),
                   (b'retry-after', b'1'), (b'x-paperflow-pending', b'1')]
        origin = next((value.decode('ascii', 'ignore') for key, value in scope.get('headers', [])
                       if key.lower() == b'origin'), '')
        if ORIGIN.fullmatch(origin):
            headers.extend([(b'access-control-allow-origin', origin.encode('ascii')),
                            (b'vary', b'Origin')])
        await send({'type': 'http.response.start', 'status': 503, 'headers': headers})
        await send({'type': 'http.response.body', 'body': b'{"status":"observation_pending"}'})

    async def __call__(self, scope, receive, send):
        if scope.get('type') != 'http' or scope.get('method') != 'GET' or scope.get('path') != SUMMARY:
            return await self.app(scope, receive, send)
        query = scope.get('query_string', b'')
        if not isinstance(query, bytes) or len(query) > 2048:
            return await self._pending(scope, send)
        if not self._sync_revision():
            return await self._pending(scope, send)
        # The underlying app chooses CORS response headers for the requesting
        # origin. Never serve those headers to a different origin from cache.
        origin = next((value for name, value in scope.get('headers', [])
                       if name.lower() == b'origin'), b'')
        key = (query, origin)
        now = time.monotonic()
        cached = self.entries.get(key)
        if cached and not 0 <= now - cached[0] <= self.max_age_seconds:
            self.entries.pop(key, None)
            cached = None
        active = self.active.get(key)
        if active is None and len(self.active) < self.max_inflight:
            self.started += 1
            sequence = self.started
            copied_scope = dict(scope)
            copied_scope['headers'] = list(scope.get('headers', []))
            task = asyncio.create_task(self._observe(key, copied_scope, sequence, now, self.epoch))
            self.active[key] = (sequence, task)
            active = self.active[key]
        if cached is None and active is not None:
            try:
                await asyncio.wait_for(asyncio.shield(active[1]), timeout=self.wait_seconds)
            except asyncio.TimeoutError:
                pass
            cached = self.entries.get(key)
            if cached and not 0 <= time.monotonic() - cached[0] <= self.max_age_seconds:
                self.entries.pop(key, None)
                cached = None
        if cached is None:
            return await self._pending(scope, send)
        if not self._sync_revision() or self.entries.get(key) is not cached:
            return await self._pending(scope, send)
        started, status, headers, body, deadline = cached
        now = time.monotonic()
        verified = self._basis(body)[-2]
        remaining_ms = max(0, math.floor(1000 * (started + self.max_age_seconds - now)))
        if verified:
            remaining_ms = (min(remaining_ms, max(0, math.floor(1000 * (deadline - now))))
                            if deadline is not None else 0)
        value = json.loads(body)
        if verified and not remaining_ms:
            # Keep the independent preprocessing/LLM results, but never turn
            # an expired server observation into a fresh successful result.
            value['postprocessing_registered_ledger'] = {
                'available': False, 'basis': 'postprocessing-registered-ledger/1',
                'reason': 'ledger_unavailable', 'current_delivery_verified': False,
                'results': {'available': False, 'message': '服务器回执已过期，等待重新核验'}}
        # Browser lifetimes can only consume the remaining server/cache age.
        # The client starts its own timer at request start, not at receipt.
        value['postprocessing_observation_remaining_ms'] = remaining_ms
        body = json.dumps(value, ensure_ascii=False, separators=(',', ':')).encode('utf8')
        headers = [(key, value) for key, value in headers if key.lower() not in
                   {b'content-length', b'transfer-encoding', b'cache-control', b'x-paperflow-observation-deadline'}]
        headers.append((b'cache-control', b'no-store'))
        await send({'type': 'http.response.start', 'status': status, 'headers': headers})
        await send({'type': 'http.response.body', 'body': body})

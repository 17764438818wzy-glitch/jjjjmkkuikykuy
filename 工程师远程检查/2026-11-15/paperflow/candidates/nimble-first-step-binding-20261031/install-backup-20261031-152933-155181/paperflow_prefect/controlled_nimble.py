"""Use the existing Nimble decisions with a durable pause and response journal."""
import base64
from contextlib import closing
import hashlib
import http.client
import time
from urllib.error import HTTPError
from urllib.parse import urlsplit

from .nimble_group import NimbleJudge, _nimble_request_gate
from .first_step_control import PauseRequested, RequestNotSent
from .records import identity


class ControlledNimbleJudge(NimbleJudge):
    def __init__(self, control, task_id, seq, snapshot_id, generation, group_scope=None, **kwargs):
        super().__init__(**kwargs)
        self.control, self.task_id, self.seq = control, task_id, seq
        self.snapshot_id, self.generation = snapshot_id, generation
        self._step = None
        self._scope = group_scope.casefold() if group_scope else None
        self._request_identity = None

    @staticmethod
    def _file_identity(record):
        if not record or not record.get('relative') or not record.get('sha256'):
            return None
        return identity([record['relative'].casefold(), record.get('bytes'), record['sha256']])

    def _acquire_request_gate(self, timeout):
        while True:
            with closing(self.control.summary._connect()) as db:
                if not self.control._running(db, self.task_id, self.generation):
                    raise PauseRequested()
            if _nimble_request_gate.acquire(timeout=.05):
                return

    def _remaining_after_queue(self, start):
        return float(self.timeout)

    def _queue_timed_out(self):
        with closing(self.control.summary._connect()) as db:
            if not self.control._running(db, self.task_id, self.generation):
                raise PauseRequested()
        raise TimeoutError('Nimble 请求排队超时')

    def _cached_response(self, request):
        if not self._step:
            raise ValueError('模型判断步骤未确定')
        result = self.control.cached_model(self.task_id, self.seq, self._step, self.snapshot_id,
                                           hashlib.sha256(request.data).hexdigest(), self.generation)
        return None if result is None else self._response_body(request, result)

    @staticmethod
    def _response_body(request, result):
        if not 200 <= result['status'] < 300:
            raise HTTPError(request.full_url, result['status'], 'Nimble 请求失败', None, None)
        return base64.b64decode(result['body'], validate=True)

    def choose_body(self, records):
        self._step = 'body'
        members = [self._file_identity(row) for row in records]
        self._request_identity = ({'scope': self._scope, 'kind': 'body', 'members': sorted(members)}
                                  if self._scope and members and all(members) else None)
        return super().choose_body(records)

    def record_confirmed_body(self, digest):
        self.control.record_confirmed_body(self.task_id, self.seq, self.generation, digest)

    def choose_other(self, record, body_id):
        self._step = 'other:' + record['file_id']
        members = [self._file_identity(self.body_candidates.get(body_id)), self._file_identity(record)]
        self._request_identity = ({'scope': self._scope, 'kind': 'other', 'members': members}
                                  if self._scope and all(members) else None)
        return super().choose_other(record, body_id)

    def choose_filename_title(self, filename):
        self._step = 'filename-title'
        self._request_identity = ({'scope': self._scope, 'kind': 'filename-title',
                                   'members': [identity(filename)]} if self._scope else None)
        return super().choose_filename_title(filename)

    def _transmit(self, request, remaining):
        if not self._step:
            raise ValueError('模型判断步骤未确定')
        data = request.data
        payload_hash = hashlib.sha256(data).hexdigest()
        url = urlsplit(request.full_url)
        deadline = None

        def begin():
            nonlocal deadline
            deadline = time.monotonic() + remaining
            connection = http.client.HTTPConnection(url.hostname, url.port or 80,
                                                    timeout=max(.01, min(2.0, deadline - time.monotonic())))
            path = url.path or '/'
            if url.query:
                path += '?' + url.query
            try:
                connection.connect()
            except Exception as exc:
                connection.close()
                raise RequestNotSent('无法连接本机 Nimble，模型请求未发送') from exc
            try:
                connection.request('POST', path, body=data, headers=dict(request.header_items()))
                if connection.sock is not None:
                    connection.sock.settimeout(max(.01, deadline - time.monotonic()))
                return connection
            except Exception:
                connection.close()
                raise

        def receive(connection):
            try:
                response = connection.getresponse()
                raw = response.read(256 * 1024 + 1)
                return {'status': response.status, 'body': base64.b64encode(raw).decode('ascii')}
            finally:
                connection.close()

        result = self.control.invoke_model(self.task_id, self.seq, self._step, self.snapshot_id,
                                           payload_hash, begin, receive, timeout=remaining,
                                           generation=self.generation, request_identity=self._request_identity)
        return self._response_body(request, result)

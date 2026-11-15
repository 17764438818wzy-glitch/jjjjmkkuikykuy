"""Candidate raw LLM request boundary, with no parsing or postprocessing.

The caller must provide the real Prefect lease and an admission context that
records in-flight work under the same pause control before yielding. Importing
this module does not start a worker, load credentials, or send a request.
"""
from contextlib import ExitStack
import hashlib
import json
from pathlib import Path
import threading
import time

from filelock import FileLock


def _sha_file(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def _body_hash(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, separators=(',', ':')).encode()).hexdigest()


class _DrainAfterStart:
    """Pause cancels admission only; an invoked request is allowed to finish."""
    def __init__(self, before_start):
        self.before_start = before_start
        self.started = threading.Event()

    def mark_started(self):
        if self.started.is_set():
            raise ValueError('模型发送边界被重复启动')
        self.started.set()

    def is_set(self):
        return not self.started.is_set() and self.before_start.is_set()

    def wait(self, seconds=None):
        deadline = None if seconds is None else time.monotonic() + max(0, seconds)
        while True:
            if self.is_set():
                return True
            if deadline is not None and time.monotonic() >= deadline:
                return False
            time.sleep(.05 if deadline is None else min(.05, max(0, deadline - time.monotonic())))


def _perform_one(*, kind, request_body, connection, model, stop, request_once,
                 lease, admit, check_source, opener=None):
    """Send or recover one journaled response; the caller holds work locks."""
    transport_stop = _DrainAfterStart(stop)
    active_admission = {}

    def on_request_started():
        token = active_admission.get('token')
        if not callable(getattr(token, 'start_transport', None)):
            raise ValueError('缺少与暂停协调的模型发送准入')
        token.start_transport(transport_stop.mark_started)

    def dispatch(send, transport):
        with lease(kind, stop):
            if stop.is_set():
                raise InterruptedError('任务已暂停，未发送模型请求')
            with admit(transport) as token:
                if not callable(getattr(token, 'start_transport', None)):
                    raise ValueError('缺少与暂停协调的模型发送准入')
                active_admission['token'] = token
                check_source()
                if stop.is_set():
                    raise InterruptedError('任务已暂停，未发送模型请求')
                return send()

    kwargs = {'stop': transport_stop, 'dispatch': dispatch, 'on_request_started': on_request_started}
    if opener is not None:
        kwargs['opener'] = opener
    request_once(request_body, connection, model, **kwargs)
    state_path, request_path, response_path = (model / name for name in
        ('request-state.json', 'request.json', 'response.txt'))
    try:
        state = json.loads(state_path.read_text(encoding='utf8'))
        actual_request = json.loads(request_path.read_text(encoding='utf8'))
    except (OSError, ValueError) as exc:
        raise ValueError('模型请求证据不可核验') from exc
    expected_request = _body_hash(request_body)
    from preextract_scheduler_adapter import request_observation
    try:
        phase, next_step, evidence = request_observation(model)
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise ValueError('模型响应证据不可核验') from exc
    if (state.get('status') != 'response_complete' or state.get('finish_reason') != 'stop'
            or state.get('request_sha256') != expected_request
            or _body_hash(actual_request) != expected_request
            or not response_path.is_file()
            or state.get('response_sha256') != _sha_file(response_path)
            or not (model / 'response-events.jsonl').is_file()
            or phase != 'response_complete' or next_step != 'postprocess'
            or evidence.get('request_sha256') != expected_request
            or evidence.get('response_sha256') != state['response_sha256']):
        raise ValueError('模型响应证据不完整或请求身份不一致')
    return {'status': 'raw_response_saved', 'kind': kind,
            'request_sha256': expected_request, 'response_path': str(response_path),
            'response_sha256': state['response_sha256']}


def run_raw_request(*, kind, request_body_factory, connection, work, stop, request_once,
                    lease, admit, check_history, check_source, opener=None):
    """Retain one raw result, leaving queue publication to the caller.

    `lease(kind, stop)` must hold both the shared total and the lane limit for
    the whole HTTP request. `admit(transport)` must atomically register the
    request as in-flight against the current task generation before yielding;
    it must not keep the pause-control mutex locked while waiting for HTTP.
    """
    if kind not in {'body', 'attachment'}:
        raise ValueError('LLM 请求类型无效')
    if stop is None or not all(callable(x) for x in (request_body_factory, request_once, lease, admit, check_history, check_source)):
        raise ValueError('LLM 请求缺少暂停、准入或历史核验')
    if stop.is_set():
        raise InterruptedError('任务已暂停，未发送模型请求')
    work = Path(work).resolve()
    work.mkdir(parents=True, exist_ok=True)
    with ExitStack() as stack:
        if kind == 'attachment':
            stack.enter_context(FileLock(str(work / 'attachment-identification.lock'), timeout=0))
        # Existing recovery and desktop entry points use these different locks.
        # Acquire both on the same work directory to prevent cross-entry races.
        stack.enter_context(FileLock(str(work / 'profile-recovery.lock'), timeout=0))
        stack.enter_context(FileLock(str(work / 'operation.lock'), timeout=0))
        check_history()
        check_source()
        model = work / 'model'
        if kind == 'body' and model.is_dir() and any(
                path.name.startswith(('partition-', 'part-', 'global-', 'initialization-evidence'))
                for path in model.iterdir()):
            raise ValueError('已有分段请求或计划，需按原分段记录核对，不能重新整篇发送')
        # Image adaptation and request planning must run while the same work
        # locks are held; callers must not prebuild a mutating plan outside.
        request_body = request_body_factory()
        if not isinstance(request_body, dict):
            raise ValueError('LLM 请求内容无效')
        check_source()
        if stop.is_set():
            raise InterruptedError('任务已暂停，未发送模型请求')

        # One saved component is not sufficient to place a whole folder or a
        # multi-part body into the waiting-for-postprocessing queue.
        return _perform_one(kind=kind, request_body=request_body, connection=connection,
                            model=work / 'model', stop=stop, request_once=request_once,
                            lease=lease, admit=admit, check_source=check_source, opener=opener)
